"""hcom plugin for Hermes Agent: connects an `hcom hermes` launch to hcom.

Written by hcom (`hcom hooks add hermes`, or the first `hcom hermes` for a
profile); edits are overwritten. Inert unless this process is the Hermes that
hcom launched: the plugin loads in every process of the profile (gateway,
one-off subcommands, nested runs), so `register()` only scrubs foreign agent
markers there and binds nothing.

Delivery, one batch outstanding per participant at a time:
- mid-turn: appended to a tool result (`transform_tool_result`);
- turn start: added to the turn's context (`pre_llm_call`);
- idle: queued as the next user turn, verified in selected/persisted history,
  released unacked if Hermes drops it.
Idle text goes on the classic CLI's `_pending_input` queue directly:
`ctx.inject_message` picks the interrupt queue whenever a turn is running, and
a turn can start between any busy check and the call. The TUI injector queues
behind a running turn on its own.

Bridges (Phase 2): in a messaging gateway the plugin serves chats that started
hcom with `/hcom start` (see `GatewayHost`). It delivers to them through
Hermes's public `ctx.inject_message`, which re-checks the chat's allowlist.

Every batch read becomes an `Outstanding` record owned by one identity. A
read that finishes after the participant's generation moved on (every bind,
session end and detach bumps `gen`) is dropped. A record's state only moves
forward: handed_off -> offered -> admitted -> acked. Text the model has seen
is never withheld because an ack reply went missing; the ack is retried until
it settles, and nothing else is read meanwhile.
"""

import asyncio
import hashlib
import json
import os
import queue
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

HCOM_TIMEOUT = 5.0
# Starting writes the profile's injection grant through Hermes's CLI.
JOIN_TIMEOUT = 60.0
# A CLI delivery Hermes no longer holds and has not admitted within this
# window is released and delivered again later.
ADMIT_GRACE = 10.0
RECONCILE = 5.0
# Poll for unread mail this often when the notify port works (wakes cover the
# rest), and this often when it doesn't.
PENDING_POLL = 60.0
FALLBACK_PENDING_POLL = 5.0
# The CLI's running flag stays set for a moment after on_session_end; idle
# delivery retries at this pace for this long after a turn ends.
END_RETRY_STEP = 0.1
END_RETRY_FOR = 2.0
# A bridge hand-off the gateway has not run within this window while the chat
# is idle is released (the gateway's queue is not visible to the plugin).
BRIDGE_ADMIT_GRACE = 300.0
# Bridges have no process for hcom to watch: the gateway heartbeats them.
HEARTBEAT = 20.0
# The loaded copy, reported on sync: hcom flags a gateway still running an
# older plugin after an upgrade (the file on disk may already be newer).
SOURCE_HASH = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:16]
# How a gateway-capable process waits for the gateway injector: a CLI gives up
# as soon as its REPL attaches, a TUI after a grace, anything else eventually.
GATEWAY_POLL = 1.0
TERMINAL_GRACE = 30.0
GATEWAY_WAIT = 600.0
# Sync retry while hcom is unreachable at gateway start.
SYNC_RETRY = 5.0
# How long the CLI watcher waits for the REPL to attach.
CLI_READY_TIMEOUT = 120.0
# Set by the claiming Hermes process: nested Hermes runs and subcommands the
# agent starts inherit a foreign PID and stay inert.
HOST_PID_ENV = "HCOM_PLUGIN_HOST_PID"
# Hermes runs plugins in a separate host process under `plugins.isolation: host`.
HERMES_HOST_PROCESS_ENV = "HERMES_PLUGIN_HOST_PROCESS"
NOT_ATTACHED = "is not attached to this process"

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load_settings():
    """hcom.json, written next to this file by hcom's installer."""
    try:
        with open(os.path.join(_HERE, "hcom.json"), encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def log(env, level, event, instance=None, **extra):
    """One JSONL line in hcom's log, the same shape as the Pi plugin's."""
    hcom_dir = env.get("HCOM_DIR") or os.path.join(os.path.expanduser("~"), ".hcom")
    path = os.path.join(hcom_dir, ".tmp", "logs", "hcom.log")
    entry = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "level": level,
        "subsystem": "plugin",
        "event": event,
    }
    if instance:
        entry["instance"] = instance
    entry.update(extra)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except OSError:
        pass


def scrub_env(env, names):
    """Drop inherited agent markers so this Hermes's terminal children are not
    mistaken for the agent that started Hermes."""
    for name in names:
        env.pop(name, None)


def claim_host(env, pid):
    """True when this process may act for hcom."""
    owner = env.get(HOST_PID_ENV)
    if owner and owner != str(pid):
        return False
    env[HOST_PID_ENV] = str(pid)
    return True


def launched_by_hcom(env):
    return (
        env.get("HCOM_LAUNCHED") == "1"
        and bool(env.get("HCOM_PROCESS_ID"))
        and env.get("HCOM_TOOL") == "hermes"
    )


def format_messages(messages, recipient):
    """Same shape as the Pi plugin's formatMessagesForInjection."""
    parts = []
    for m in messages:
        event_id = m.get("event_id")
        intent, thread = m.get("intent"), m.get("thread")
        if intent:
            prefix = f"[{intent}:{thread} #{event_id}]" if thread else f"[{intent} #{event_id}]"
        else:
            prefix = f"[thread:{thread} #{event_id}]" if thread else f"[new message #{event_id}]"
        parts.append(f"{prefix} {m.get('from')} -> {recipient}: {m.get('message')}")
    if len(parts) == 1:
        return f"<hcom>{parts[0]}</hcom>"
    return f"<hcom>[{len(parts)} new messages] | {' | '.join(parts)}</hcom>"


def message_body(message, recipient):
    """One message as it appears inside any delivered batch: the evidence
    that it reached the model."""
    return format_messages([message], recipient)[len("<hcom>"):-len("</hcom>")]


# Appended to a bridge delivery when the chat does not show incoming messages,
# so the reply the person sees is not out of context.
UNSEEN_HINT = ("(The person in this chat did not see this message. If you reply in "
               "the chat, say briefly that it is about a message from the sender.)")


def carries(user_message, text):
    """True when the turn's user message is `text`, or holds it as a whole
    blank-line-separated part: the TUI and the gateway merge prompts queued
    behind a busy turn as `earlier + "\\n\\n" + later`. A prompt that merely
    quotes it does not count; the text's `#<event id>` markers keep a whole
    part unique. A multimodal turn (a photo merged with queued text) is a
    list of parts: any text part may carry it."""
    if isinstance(user_message, list):
        return any(carries(part.get("text"), text) for part in user_message
                   if isinstance(part, dict) and part.get("type") == "text")
    if not isinstance(user_message, str):
        return False
    sep = "\n\n"
    return (
        user_message == text
        or user_message.startswith(text + sep)
        or user_message.endswith(sep + text)
        or (sep + text + sep) in user_message
    )


def _valid_batch(messages):
    if not isinstance(messages, list):
        return False
    last = 0
    for m in messages:
        event_id = m.get("event_id") if isinstance(m, dict) else None
        if not isinstance(event_id, int) or isinstance(event_id, bool) or event_id <= last:
            return False
        last = event_id
    return True


def content_text(value):
    """Text-only evidence from scalar or multimodal Hermes message content."""
    if isinstance(value, str):
        # Multimodal content is JSON-encoded in state.db.
        if value.startswith("["):
            try:
                decoded = json.loads(value)
                if isinstance(decoded, list):
                    return content_text(decoded)
            except ValueError:
                pass
        return value
    if isinstance(value, list):
        return "\n".join(part["text"] for part in value if isinstance(part, dict)
                         and part.get("type") == "text" and isinstance(part.get("text"), str))
    return ""


class Outstanding:
    """One batch read from `owner`'s mailbox."""

    HANDED_OFF, OFFERED, ADMITTED = "handed_off", "offered", "admitted"

    def __init__(self, owner, target, messages, ack_id, has_more, state):
        self.owner = owner
        self.target = target  # the session it was handed to
        self.messages = messages
        self.text = format_messages(messages, owner)
        self.sender = str(messages[0].get("from") or "")
        self.count = len(messages)
        self.ack_id = ack_id
        self.has_more = has_more
        self.state = state
        self.sent_at = time.monotonic()
        self.noticed = set()


class Participant:
    """One hcom identity's delivery in this process."""

    def __init__(self, host, kind):
        self.host = host
        self.kind = kind  # "cli" | "tui"
        self.lock = threading.RLock()
        self.bind_lock = threading.Lock()
        self.ack_lock = threading.Lock()
        self.name = None
        self.session_id = None
        self.bootstrap = None
        self.bootstrapped_session = None
        self.gen = 0
        # Permanent: hcom gave this identity to another launch (or killed it).
        self.detached = False
        self.busy = False
        self.pending = True  # mail may be waiting at bind
        self.delivering = False
        self.out = None  # the Outstanding batch, if any
        self.last_poll = 0.0
        self.retry_until = 0.0
        self.last_status = None

    # ── plumbing ─────────────────────────────────────────────────────────

    def log(self, level, event, **extra):
        log(self.host.env, level, event, self.name, **extra)

    def rpc(self, args):
        return self.host.rpc(args, self)

    def admit_grace(self):
        """How long an unadmitted hand-off Hermes no longer holds may wait for
        its turn; None: until its session ends."""
        return ADMIT_GRACE if self.kind == "cli" else None

    def detach(self):
        """Killed, or resumed by another launch: this process never acts for
        the identity again."""
        with self.lock:
            if self.detached:
                return
            self.detached = True
            self.gen += 1
            self.out = None
            self.delivering = False
        self.log("INFO", "plugin.detached")

    def inactive(self):
        """Detached for good, or fenced: the host was unloaded or replaced."""
        return self.detached or self.host.stopped

    def already_run(self, rec):
        """Whether Hermes already ran `rec` (only bridges can tell)."""
        return False

    def persisted_events(self, rec):
        """Event markers in accepted user/tool rows, including API sidecars.
        Bridges search the route across session rotation; terminal participants
        search the session that received this batch."""
        if not self.host.home:
            return set()
        try:
            path = Path(self.host.home, "state.db").resolve().as_uri() + "?mode=ro"
            with closing(sqlite3.connect(path, uri=True, timeout=1)) as db:
                columns = {row[1] for row in db.execute("PRAGMA table_info(messages)")}
                content = "(instr(m.content, ?) > 0"
                if "api_content" in columns:
                    content += " OR instr(m.api_content, ?) > 0"
                content += ")"
                bridge = self.kind == "bridge"
                where = ("s.session_key = ? AND COALESCE(s.source, '') != 'subagent'"
                         if bridge else "m.session_id = ?")
                route = self.session_key if bridge else rec.target
                found = set()
                for message in rec.messages:
                    body = message_body(message, rec.owner)
                    marker = f"#{message['event_id']}]"
                    params = [route, marker] + ([marker] if "api_content" in columns else [])
                    sidecar = "m.api_content" if "api_content" in columns else "NULL"
                    rows = db.execute(
                        f"SELECT m.content, {sidecar} FROM messages m JOIN sessions s ON s.id = m.session_id "
                        "WHERE " + where + " AND m.role IN ('user', 'tool') AND " + content, params)
                    if any(body in content_text(api or raw) for raw, api in rows):
                        found.add(message["event_id"])
                return found
        except (sqlite3.Error, OSError, ValueError):
            return set()

    def confirm(self, history=None):
        """Confirm the final context, never the transform's proposed output.
        The completed-turn hook supplies selected history; the worker can also
        confirm durable rows earlier, while a long tool loop is still running.
        A hand-off whose turn start was not recognized is confirmed from the
        persisted rows too, rather than held (TUI, bridge) or re-sent (CLI)."""
        with self.lock:
            rec = self.out
            if not rec or rec.state not in (rec.HANDED_OFF, rec.OFFERED) or self.inactive():
                return
        found = self.persisted_events(rec)
        if isinstance(history, list) and rec.state == rec.OFFERED:
            for message in rec.messages:
                body = message_body(message, rec.owner)
                for row in history:
                    if not isinstance(row, dict) or row.get("role") not in ("user", "tool"):
                        continue
                    # api_content holds the exact accepted bytes when present.
                    value = row.get("api_content") or row.get("content")
                    if body in content_text(value):
                        found.add(message["event_id"])
                        break
        if not all(m["event_id"] in found for m in rec.messages):
            return
        with self.lock:
            if self.out is not rec or rec.state == rec.ADMITTED or self.inactive():
                return
            rec.state = rec.ADMITTED
        self.notice(rec)
        self.settle(rec)

    def notice(self, rec):
        pass  # only messaging bridges have a human-visible route

    def cli(self):
        return getattr(self.host.manager, "_cli_ref", None)

    def is_busy(self):
        return self.busy or bool(getattr(self.cli(), "_agent_running", False))

    def is_main(self, session_id):
        """Hooks also fire for delegate_task subagents, under their own session
        ids; only the bound session is this participant."""
        return bool(session_id) and session_id == self.session_id

    def notify(self):
        with self.lock:
            self.pending = True

    def status(self, status, context="", detail=""):
        # Counted from when it is queued: the reconcile tick must not queue
        # another `listening` while the write is still on its way.
        self.last_status = status
        self.host.status_queue.put(
            (self, ["hermes-status", "--status", status, "--context", context, "--detail", detail],
             status))

    def status_failed(self, status):
        """A write did not land: unless a newer one is queued, forget it, so
        the tick restores listening."""
        if self.last_status == status:
            self.last_status = None

    def before_tool(self, tool, args):
        try:
            input_json = json.dumps(args if isinstance(args, dict) else {})
        except (TypeError, ValueError):
            input_json = "{}"
        self.last_status = "active"
        self.host.status_queue.put(
            (self, ["hermes-beforetool", "--tool", tool, "--input-json", input_json], "active"))

    # ── binding ──────────────────────────────────────────────────────────

    def bind(self, session_id):
        """Bind (or rebind after /new) this identity to `session_id`. The first
        bind is launch readiness for hcom."""
        if not session_id:
            return False
        with self.bind_lock:
            if self.detached:
                return False
            if self.name and session_id == self.session_id:
                return True
            port = self.host.notify_port()
            args = ["hermes-start", "--session-id", session_id, "--cwd", os.getcwd()]
            if self.host.home:
                args += ["--transcript-path", os.path.join(self.host.home, "state.db")]
            if port:
                args += ["--notify-port", str(port)]
            reply = self.rpc(args)
            name = reply.get("name") if reply else None
            if reply is None or not isinstance(name, str) or not name:
                self.log("WARN", "plugin.bind_failed", reply=reply)
                return False
            with self.lock:
                if self.detached:
                    return False
                rebind = self.name is not None
                self.name = name
                self.session_id = session_id
                bootstrap = reply.get("bootstrap")
                self.bootstrap = bootstrap if isinstance(bootstrap, str) and bootstrap else None
                self.gen += 1
                self.delivering = False
                out = self.out
                # The CLI keeps queued input across /new: a batch it holds becomes
                # the new session's first turn, so it stays outstanding (grace
                # restarted). Never across a change of identity: it would ack
                # another mailbox. The TUI drops queued input with the session.
                if out and out.owner == name and (self.kind == "cli" or out.state == out.ADMITTED):
                    out.target = session_id
                    out.sent_at = time.monotonic()
                else:
                    self.out = None
                self.pending = True
                self.last_status = None
            self.log("INFO", "plugin.bound", session_id=session_id, surface=self.kind,
                     notify_port=port, rebind=rebind)
            self.host.start_threads()
            self.host.wake.set()
            return True

    def watch_cli(self):
        """Classic CLI readiness: the REPL attaches itself to the plugin manager
        a few seconds after register(). Quiet one-shot runs attach too, after
        setting `_single_query_mode`."""
        deadline = time.monotonic() + CLI_READY_TIMEOUT
        while time.monotonic() < deadline and not self.detached:
            cli = self.cli()
            if cli is not None:
                if getattr(cli, "_single_query_mode", False):
                    self.log("INFO", "plugin.single_query_skip")
                    return
                if self.bind(getattr(cli, "session_id", None)):
                    return
                time.sleep(1.0)  # hcom refused or failed: retry, slowly
            if self.name:
                return  # bound another way (TUI)
            time.sleep(0.05)

    # ── delivery ─────────────────────────────────────────────────────────

    def begin(self):
        """Claim the delivery slot and consume the wake. Returns the
        (name, gen, session) the read runs under, or None."""
        with self.lock:
            if (not self.name or self.inactive() or not self.pending or self.delivering
                    or self.out):
                return None
            self.pending = False
            self.delivering = True
            return self.name, self.gen, self.session_id

    def end(self, op, more):
        """Release the slot; `more` re-arms the wake (unread left, or a failed
        read). A wake that arrived meanwhile has already set `pending`."""
        with self.lock:
            if op[1] != self.gen:
                return
            self.delivering = False
            if more:
                self.pending = True

    def read(self, op, state):
        """Read the oldest unread batch for `op` and record it as outstanding.
        Returns the record, None when empty or stale, or False on failure."""
        name, gen, session = op
        reply = self.rpc(["hermes-read", "--name", name])
        if reply is None:
            return False
        messages = reply.get("messages")
        if not _valid_batch(messages):
            self.log("WARN", "plugin.rpc_failed", command="hermes-read", error="malformed batch")
            return False
        has_more = reply.get("has_more") is True
        with self.lock:
            if gen != self.gen or self.inactive():
                return None
            if not messages:
                if has_more:
                    self.pending = True
                return None
            rec = Outstanding(name, session, messages, messages[-1]["event_id"], has_more, state)
            self.out = rec
            return rec

    def settle(self, rec):
        """Ack an admitted record (serialized). On success it is done: the
        slot frees and leftover unread re-arms the wake. On failure it stays,
        so nothing else is read until the ack settles."""
        if not self.ack_lock.acquire(blocking=False):
            return False
        try:
            with self.lock:
                if self.out is not rec or rec.state != rec.ADMITTED or self.inactive():
                    return False
                if rec.owner != self.name:
                    self.out = None  # never ack another identity's mailbox
                    return False
            reply = self.rpc(["hermes-read", "--name", rec.owner, "--ack", "--up-to",
                              str(rec.ack_id)])
            if reply is None or reply.get("acked_to") != rec.ack_id:
                self.log("WARN", "plugin.ack_failed", ack_id=rec.ack_id)
                return False
            with self.lock:
                if self.out is rec:
                    self.out = None
                if rec.has_more:
                    self.pending = True
            self.log("INFO", "plugin.delivery_acked", acked_to=rec.ack_id)
            self.host.wake.set()
            return True
        finally:
            self.ack_lock.release()

    def take_batch(self, source):
        """Offer a batch as hook output. It is confirmed only after Hermes
        selects/applies it, not while this callback is proposing it."""
        op = self.begin()
        if op is None:
            return None
        rec = None
        try:
            rec = self.read(op, Outstanding.OFFERED)
            if not rec:
                return None
            if self.already_run(rec):
                rec.state = rec.ADMITTED
                self.settle(rec)
                return None
            self.log("INFO", "plugin.delivery_sent", count=rec.count, ack_id=rec.ack_id, via=source)
            return rec.text
        finally:
            self.end(op, rec is False)

    def hand_off(self, rec):
        """Queue `rec` as the next user turn of the session it was read for.
        Never interrupts: the CLI's `_pending_input` is its between-turns
        queue, and the TUI injector queues behind a running turn."""
        if self.kind == "cli":
            pending_input = getattr(self.cli(), "_pending_input", None)
            if pending_input is None:
                self.log("ERROR", "plugin.no_cli_queue")
                return False
            pending_input.put(rec.text)
            return True
        manager = self.host.manager
        if not getattr(manager, "has_tui_message_injector", False):
            self.log("ERROR", "plugin.no_tui_injector")
            return False
        return bool(manager.inject_tui_message(
            session_key=rec.target, content=rec.text, plugin_id=self.host.ctx.plugin_id))

    def deliver_idle(self):
        op = self.begin()
        if op is None:
            return
        rec = None
        try:
            if self.is_busy():
                rec = False  # mail stays pending for after the turn
                return
            rec = self.read(op, Outstanding.HANDED_OFF)
            if not rec:
                return
            if self.already_run(rec):
                # Ran before a gateway restart lost the record: only the ack is
                # missing.
                with self.lock:
                    rec.state = rec.ADMITTED
                self.log("INFO", "plugin.delivery_already_run", ack_id=rec.ack_id)
                self.settle(rec)
                return
            with self.lock:
                if self.out is not rec or self.is_busy() or self.inactive():
                    if self.out is rec:
                        self.out = None  # a turn started meanwhile: read again later
                    rec = False
                    return
                # A reload waits for this lock before adopting the record.
                # It must not take over between the fence and queue admission.
                try:
                    accepted = self.hand_off(rec)
                except Exception as e:
                    accepted = False
                    self.log("ERROR", "plugin.inject_error", error=repr(e))
            if not accepted:
                with self.lock:
                    if self.out is rec:
                        self.out = None
                    # Retry on the next wake or poll, not every reconcile tick.
                    self.last_poll = time.monotonic()
                self.log("WARN", "plugin.inject_rejected", ack_id=rec.ack_id)
                return
            self.log("INFO", "plugin.delivery_sent", count=rec.count, ack_id=rec.ack_id,
                     via="inject")
            self.notice(rec)
        finally:
            self.end(op, rec is False)

    def held(self, rec):
        """True while the CLI still has a handed-off batch queued. Other
        queues are not visible; `admit_grace` covers them."""
        if self.kind != "cli":
            return False
        pending_input = getattr(self.cli(), "_pending_input", None)
        try:
            return rec.text in list(pending_input.queue)
        except AttributeError:
            return False

    def release_unadmitted(self, reason="grace", force=False):
        with self.lock:
            rec = self.out
            if not rec or rec.state not in (rec.HANDED_OFF, rec.OFFERED):
                return
            if not force:
                if rec.state == rec.HANDED_OFF and self.held(rec):
                    # The grace runs from when Hermes lets go of it, not from
                    # the hand-off: a long /new confirmation must not eat it.
                    rec.sent_at = time.monotonic()
                    return
                grace = ADMIT_GRACE if rec.state == rec.OFFERED else self.admit_grace()
                if grace is None or self.is_busy() or time.monotonic() - rec.sent_at < grace:
                    return
            self.out = None
            # Redeliver on a later wake or poll rather than at once, so a turn
            # Hermes keeps refusing is not re-queued in a tight loop.
            self.last_poll = time.monotonic()
        self.log("WARN", "plugin.delivery_not_admitted", ack_id=rec.ack_id, reason=reason)

    def admit(self, user_message):
        """The handed-off batch this turn's user message carries, now admitted."""
        with self.lock:
            rec = self.out
            if rec and rec.state == rec.HANDED_OFF and carries(user_message, rec.text):
                rec.state = rec.OFFERED
                return rec
        return None

    def tick(self):
        """Reconcile: retry an unsettled ack, release a dropped hand-off,
        restore listening, and deliver pending mail while idle."""
        if not self.name or self.inactive():
            return
        self.confirm()
        rec = self.out
        if rec and rec.state == rec.ADMITTED:
            self.settle(rec)
        self.release_unadmitted()
        if self.is_busy():
            return
        if self.last_status != "listening":
            self.status("listening")
        now = time.monotonic()
        interval = PENDING_POLL if self.host.port else FALLBACK_PENDING_POLL
        with self.lock:
            if not self.pending and now - self.last_poll >= interval:
                self.pending = True
            if self.pending:
                self.last_poll = now
        self.deliver_idle()

    # ── turn events (routed by Host) ─────────────────────────────────────

    def turn_start(self, session_id, user_message):
        parts = []
        with self.lock:
            self.busy = True
            if self.bootstrap and self.bootstrapped_session != session_id:
                self.bootstrapped_session = session_id
                parts.append(self.bootstrap)
        rec = self.admit(user_message)
        if rec:
            self.log("DEBUG", "plugin.delivery_admitted", ack_id=rec.ack_id)
            self.status("active", f"deliver:{rec.sender}" if rec.sender else "deliver")
        else:
            self.status("active", "prompt")
            text = self.take_batch("turn_start")
            if text:
                parts.append(text)
        return {"context": "\n\n".join(parts)} if parts else None

    def turn_end(self):
        self.confirm()
        with self.lock:
            self.busy = False
            if self.out and self.out.state == self.out.OFFERED:
                self.out.sent_at = time.monotonic()
        self.status("listening")
        self.retry_until = time.monotonic() + END_RETRY_FOR
        self.host.wake.set()

    def session_finalized(self, reason):
        with self.lock:
            self.busy = False
            self.gen += 1
            self.delivering = False
        if self.kind == "tui":
            self.release_unadmitted("session_finalize", force=True)
        # Only a CLI shutdown ends the process here. CLI /new (session_boundary)
        # and every TUI finalize (/new included) are followed by a reset that
        # rebinds; a TUI exit is cleaned up by hcom when the process exits.
        if reason == "shutdown" and self.name and not self.detached:
            self.rpc(["hermes-stop", "--name", self.name, "--reason", "shutdown"])
            self.detach()


class Host:
    """The plugin in one Hermes process: hcom RPC, the notify port, the
    delivery and status threads, and hook routing."""

    def __init__(self, ctx, env, home):
        self.ctx = ctx
        self.env = env
        self.home = home
        self.hcom = "hcom"
        self.manager = getattr(ctx, "_manager", None)
        self.wake = threading.Event()
        self.status_queue = queue.Queue()
        self.lock = threading.Lock()
        self.port = None
        self.notify_socket = None
        self.port_started = False
        self.threads_started = False
        self.stopped = False
        self.me = Participant(self, "cli")

    def participants(self):
        return [self.me]

    def rpc(self, args, participant=None, timeout=HCOM_TIMEOUT):
        """Run `hcom <args>`; every plugin RPC exits 0 with a JSON payload or
        `{"error": ...}`. Returns the payload, or None (logged)."""
        name = participant.name if participant else None

        def fail(**extra):
            log(self.env, "WARN", "plugin.rpc_failed", name, command=args[0], **extra)

        env = getattr(participant, "env", None) or self.env
        try:
            proc = subprocess.run(
                [self.hcom, *args],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                # hcom writes UTF-8 whatever the locale (Windows: cp1252).
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
                env=env,
            )
        except subprocess.TimeoutExpired:
            return fail(error=f"timed out after {timeout}s")
        except OSError as e:
            return fail(error=str(e))
        if proc.returncode != 0:
            return fail(exit_code=proc.returncode, stderr=proc.stderr[:300])
        try:
            reply = json.loads(proc.stdout)
        except ValueError:
            return fail(raw=proc.stdout[:300])
        if not isinstance(reply, dict):
            return fail(raw=proc.stdout[:300])
        if "error" in reply:
            if participant and NOT_ATTACHED in str(reply["error"]):
                participant.detach()
            return fail(error=reply["error"])
        return reply

    # ── threads ──────────────────────────────────────────────────────────

    def notify_port(self):
        with self.lock:
            if not self.port_started:
                self.port_started = True
                self.port = self.start_notify_server()
            return self.port

    def start_notify_server(self):
        try:
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.bind(("127.0.0.1", 0))
            srv.listen(16)
            srv.settimeout(0.5)
        except OSError as e:
            log(self.env, "ERROR", "notify_server.start_failed", error=str(e))
            return None
        self.notify_socket = srv

        def serve():
            try:
                while not self.stopped:
                    try:
                        conn, _ = srv.accept()
                        conn.close()
                    except socket.timeout:
                        continue
                    except OSError:
                        break
                    for p in self.participants():
                        p.notify()
                    self.wake.set()
            finally:
                srv.close()

        threading.Thread(target=serve, name="hcom-notify", daemon=True).start()
        return srv.getsockname()[1]

    def start_threads(self):
        with self.lock:
            if self.threads_started:
                return
            self.threads_started = True
        threading.Thread(target=self.drive, name="hcom-deliver", daemon=True).start()
        threading.Thread(target=self.report_statuses, name="hcom-status", daemon=True).start()

    def stop(self):
        """Unloaded (plugin reload, or replaced by a newer copy): stop acting.
        Every in-flight step re-checks the generation or `inactive()` before
        it touches Hermes or hcom, so nothing lands after this returns."""
        self.stopped = True
        for p in self.all_participants():
            with p.lock:
                p.gen += 1
                p.delivering = False
        self.wake.set()
        self.status_queue.put((None, None, None))
        if self.notify_socket is not None:
            self.notify_socket.close()

    def all_participants(self):
        return [self.me] if self.me else []

    def drive(self):
        while not self.stopped:
            now = time.monotonic()
            fast = any(now < p.retry_until for p in self.participants())
            self.wake.wait(END_RETRY_STEP if fast else RECONCILE)
            self.wake.clear()
            for p in self.participants():
                if self.stopped:
                    break
                try:
                    p.tick()
                except Exception as e:  # never let the thread die
                    p.log("ERROR", "plugin.tick_error", error=repr(e))

    def report_statuses(self):
        """Status writes run one at a time so they land in call order."""
        while not self.stopped:
            p, args, status = self.status_queue.get()
            if p is None:
                return
            name = p.name
            if not name or p.detached or self.stopped:
                continue
            try:
                if args[0] == "notice":
                    self.show_notice(p, args[1])
                elif p.rpc([args[0], "--name", name, *args[1:]]) is None:
                    p.status_failed(status)
            except Exception as e:  # never let the thread die
                p.status_failed(status)
                p.log("ERROR", "plugin.status_error", error=repr(e))

    # ── Hermes hooks ─────────────────────────────────────────────────────

    def on_session_reset(self, session_id=None, platform=None, **_):
        # TUI: fired at session build (readiness) and on /new. CLI: on /new.
        me = self.me
        if me.cli() is None:
            me.kind = "tui"
        if me.name or me.kind == "tui":
            me.bind(session_id)

    def on_session_start(self, session_id=None, **_):
        me = self.me
        if me.name and session_id != me.session_id and session_id == getattr(
            me.cli(), "session_id", None
        ):
            me.bind(session_id)  # the CLI moved to a new session

    def pre_llm_call(self, session_id=None, user_message=None, **_):
        me = self.me
        if not me.name or me.detached or self.stopped:
            return None
        if not me.is_main(session_id):
            if not (session_id and session_id == getattr(me.cli(), "session_id", None)):
                return None
            me.bind(session_id)  # compression or /new continuation
        return me.turn_start(session_id, user_message)

    # Shared by both hosts: `route` finds the participant a session belongs to.

    def route(self, session_id):
        me = self.me
        return me if me and not self.stopped and me.is_main(session_id) else None

    def transform_tool_result(self, result=None, session_id=None, **_):
        p = self.route(session_id)
        if p is None or not p.pending or not isinstance(result, str):
            return None
        text = p.take_batch("tool_result")
        return f"{result}\n\n{text}" if text else None

    def post_llm_call(self, session_id=None, conversation_history=None, **_):
        p = self.route(session_id)
        if p:
            p.confirm(conversation_history)

    def pre_tool_call(self, tool_name=None, args=None, session_id=None, **_):
        # Fails closed on timeout: only enqueue, never block, never decide.
        p = self.route(session_id)
        if p and tool_name:
            p.before_tool(tool_name, args)
        return None

    def on_human_input_request(self, kind=None, session_id=None, session_key=None, **_):
        # Hermes takes these ids from the approval context, which may carry only
        # the session key.
        p = self.route(session_id) or self.route(session_key)
        log(self.env, "DEBUG", "plugin.human_input_request", p.name if p else None, kind=kind,
            session_id=session_id, session_key=session_key, mine=p is not None)
        if p:
            p.status("blocked", "approval", kind or "")

    def on_human_input_resolved(self, session_id=None, session_key=None, **_):
        p = self.route(session_id) or self.route(session_key)
        if p:
            p.status("active", "agent")

    def on_session_end(self, session_id=None, **_):
        p = self.route(session_id)
        if p:
            p.turn_end()

    def on_session_finalize(self, session_id=None, reason=None, **_):
        p = self.route(session_id)
        if p:
            p.session_finalized(reason)


# ── bridges (messaging gateway) ──────────────────────────────────────────

SESSION_ENV = ("HERMES_SESSION_KEY", "HERMES_SESSION_ID", "HERMES_SESSION_PLATFORM",
               "HERMES_SESSION_CHAT_ID", "HERMES_SESSION_CHAT_NAME", "HERMES_SESSION_CHAT_TYPE",
               "HERMES_SESSION_THREAD_ID", "HERMES_SESSION_USER_ID", "HERMES_SESSION_USER_NAME")
# Session surfaces that are not a messaging chat (gateway/session_context.py).
NOT_A_CHAT = {"", "api_server", "cli", "codex", "desktop", "gateway", "kanban", "local",
              "msgraph_webhook", "tool", "tui", "webhook"}


def session_env():
    """The current chat's session facts. Command handlers and hooks run inside
    the gateway's per-chat context; outside a gateway they read empty."""
    try:
        from gateway.session_context import get_session_env
    except Exception:
        return {}
    return {name: get_session_env(name) or "" for name in SESSION_ENV}


class Bridge(Participant):
    """A gateway chat on hcom. Hermes queues turns for a busy chat
    itself, so delivery only waits on this chat's own turn."""

    def __init__(self, host, entry):
        super().__init__(host, "bridge")
        self.name = entry["name"]
        self.token = entry["token"]
        self.session_key = entry["session_key"]
        self.recorded_session = None  # the session hcom last recorded
        self.origin = None
        # hcom stopped serving it (stale cleanup, a refused RPC): wait for the
        # next sync instead of acting. Only the registry removes a bridge.
        self.paused = False
        # Every RPC presents the bridge's process token.
        self.env = dict(host.env, HCOM_PROCESS_ID=self.token)
        self.update(entry)

    def update(self, entry):
        """Apply a sync entry (the registry's view)."""
        with self.lock:
            bootstrap = entry.get("bootstrap")
            if isinstance(bootstrap, str) and bootstrap:
                self.bootstrap = bootstrap
            origin = entry.get("origin")
            self.origin = origin if isinstance(origin, dict) and origin else None
            mode = entry.get("visibility", "preview")
            self.visibility = mode if mode in ("off", "preview", "full") else "preview"
            if self.paused or entry.get("recreated"):
                self.paused = False
                self.gen += 1
                self.delivering = False
                self.pending = True
                self.last_status = None

    def adopt(self, old):
        """Take over a bridge from a replaced copy of the plugin in this
        process, outstanding batch included: the gateway may still hold it.
        The old copy may be an older plugin version: read its fields with
        defaults, and keep its batch only if it has what this version uses."""
        with old.lock, self.lock:
            if getattr(old, "token", None) != self.token:
                return
            rec, old.out = getattr(old, "out", None), None
            self.session_id = getattr(old, "session_id", None)
            self.recorded_session = getattr(old, "recorded_session", None)
            self.bootstrapped_session = getattr(old, "bootstrapped_session", None)
            self.busy = getattr(old, "busy", False)
            fields = ("owner", "target", "messages", "text", "sender", "count", "ack_id",
                      "has_more", "state", "sent_at", "noticed")
            if rec and all(hasattr(rec, f) for f in fields) and rec.owner == self.name:
                self.out = rec

    def inactive(self):
        return self.paused or super().inactive()

    def is_busy(self):
        return self.busy

    def admit_grace(self):
        return BRIDGE_ADMIT_GRACE

    def detach(self):
        """hcom refused this bridge's token: it was stopped in hcom (stale
        cleanup, kill). Pause until the next sync says whether it is back."""
        with self.lock:
            if self.paused:
                return
            self.paused = True
            self.gen += 1
            self.delivering = False
        self.log("INFO", "plugin.bridge_paused")
        self.host.wake.set()

    def session_finalized(self, reason):
        # A gateway session ending is a rotation, not the bridge stopping: the
        # next turn in the chat follows the new session. Any hand-off stays
        # outstanding (the route may run it in the new session).
        with self.lock:
            self.busy = False

    def follow(self, session_id):
        """The chat's gateway session rotated (/new, idle reset, a session the
        route started): bootstrap the new one. Routing follows at once; hcom's
        record of it is retried on the next turn until it lands."""
        with self.lock:
            if not session_id or session_id == self.recorded_session:
                return
            self.session_id = session_id
        reply = self.rpc(["hermes-bridge", "session", "--name", self.name,
                          "--session-id", session_id])
        if reply is not None:
            with self.lock:
                self.recorded_session = session_id

    def already_run(self, rec):
        """Drop messages the chat already ran before a gateway restart.
        New mail can grow or split a batch, so compare each event rather than
        the batch envelope. Keep the original ack_id to settle the whole read."""
        found = self.persisted_events(rec)
        unseen = [m for m in rec.messages if m["event_id"] not in found]
        if not unseen:
            return True
        rec.messages = unseen
        rec.text = format_messages(unseen, rec.owner)
        rec.sender = str(unseen[0].get("from") or "")
        rec.count = len(unseen)
        return False

    def notice(self, rec):
        with self.lock:
            if self.visibility == "off" or self.inactive():
                return
            for message in rec.messages:
                eid = message["event_id"]
                if eid in rec.noticed:
                    continue
                rec.noticed.add(eid)
                body = str(message.get("message") or "")
                if self.visibility == "preview" and len(body) > 240:
                    body = body[:240].rstrip() + "…"
                text = f"📨 hcom · @{message.get('from')} · #{eid}\n{body}"
                self.host.status_queue.put((self, ["notice", text], None))

    def hand_off(self, rec):
        """Public inject_message, by the chat's route: Hermes checks the route
        and its allowlist before accepting, and continues the chat's session or
        starts a new one if it expired. A session key is only checked after
        the call returns (an expired one is dropped with a log line), so it is
        the fallback for a bridge with no stored route."""
        rec.text = format_messages(rec.messages, rec.owner)
        if self.visibility == "off":
            rec.text += "\n" + UNSEEN_HINT
        ctx = self.host.ctx
        if self.origin:
            return bool(ctx.inject_message(rec.text, origin=self.origin))
        return bool(ctx.inject_message(rec.text, session_key=self.session_key))


class GatewayHost(Host):
    """The plugin in a Hermes process hcom did not launch. Inert unless this
    process runs the messaging gateway and the profile has bridges; it always
    offers `/hcom` so a chat can start hcom.

    hcom's registry is the only authority: `sync` at gateway readiness and on
    every heartbeat says which bridges to serve."""

    def __init__(self, ctx, env, home, settings):
        super().__init__(ctx, env, home)
        self.me = None
        self.bridges = {}  # name -> Bridge
        self.previous = None  # the copy this one replaced in this process
        self.synced = False
        self.reload_requested = False  # once per loaded copy
        self.sessions = {}  # session_id -> (session_key, delegated), bounded
        self.env = dict(env, **(settings.get("env") or {}))
        self.hcom = shutil.which("hcom", path=self.env.get("PATH")) or settings.get("hcom") or "hcom"

    def participants(self):
        return [] if self.stopped else list(self.bridges.values())

    def all_participants(self):
        return list(self.bridges.values())

    def serving(self):
        return bool(getattr(self.manager, "has_gateway_message_injector", False))

    def start(self):
        threading.Thread(target=self.wait_for_gateway, name="hcom-gateway",
                         daemon=True).start()

    def wait_for_gateway(self):
        """Bridges are served by the gateway process only. A CLI or TUI
        process of the profile gives up rather than polling forever."""
        started = time.monotonic()
        while not self.serving():
            elapsed = time.monotonic() - started
            if (self.stopped or self.manager is None or elapsed > GATEWAY_WAIT
                    or getattr(self.manager, "_cli_ref", None) is not None
                    or (elapsed > TERMINAL_GRACE
                        and getattr(self.manager, "has_tui_message_injector", False))):
                return
            time.sleep(GATEWAY_POLL)
        if not self.home:
            log(self.env, "ERROR", "plugin.no_profile_home", None)
            return
        self.take_over()
        self.start_threads()
        self.wake.set()

    def take_over(self):
        """One gateway host per profile per process: a reload stops the older
        copy, whose outstanding batches this one adopts."""
        hosts = sys.__dict__.setdefault("_hcom_gateway_hosts", {})
        old = hosts.get(self.home)
        if old is not None and old is not self:
            old.stop()
            self.previous = old
        hosts[self.home] = self

    def sync(self):
        """Serve exactly the bridges hcom's registry lists for this profile."""
        args = ["hermes-bridge", "sync", "--home", self.home, "--plugin-hash", SOURCE_HASH]
        port = self.notify_port()
        if port:
            args += ["--notify-port", str(port)]
        first = not self.synced
        if first:
            args.append("--ensure-grant")
        reply = self.rpc(args, timeout=JOIN_TIMEOUT if first else HCOM_TIMEOUT)
        entries = (reply or {}).get("bridges")
        if not isinstance(entries, list) or self.stopped:
            return False  # hcom unreachable: keep serving what we have, retry
        self.synced = True
        if reply.get("reload_plugin") and not self.reload_requested:
            # hcom was upgraded and refreshed the code on disk: have the gateway
            # load it. Off this thread: the reload replaces this very copy.
            self.reload_requested = True
            threading.Thread(target=self.reload_plugin, name="hcom-reload", daemon=True).start()
        listed = {e["name"]: e for e in entries
                  if isinstance(e, dict) and e.get("name") and e.get("token")}
        with self.lock:
            for name, entry in listed.items():
                bridge = self.bridges.get(name)
                if bridge is None or bridge.token != entry["token"]:
                    bridge = Bridge(self, entry)
                    old = self.previous and self.previous.bridges.get(name)
                    if old:
                        try:
                            bridge.adopt(old)
                        except Exception as e:  # serve it fresh rather than not at all
                            bridge.log("ERROR", "plugin.adopt_failed", error=repr(e))
                    self.bridges[name] = bridge
                    bridge.log("INFO", "plugin.bridge_attached", session_key=bridge.session_key)
                else:
                    bridge.update(entry)
            for name in [n for n in self.bridges if n not in listed]:
                gone = self.bridges.pop(name)
                with gone.lock:
                    gone.detached = True
                    gone.gen += 1
                gone.log("INFO", "plugin.bridge_gone")
            self.previous = None  # all outstanding records have been adopted
        return True

    def start_threads(self):
        with self.lock:
            first = not self.threads_started
        super().start_threads()
        if first:
            threading.Thread(target=self.heartbeat, name="hcom-heartbeat", daemon=True).start()

    def heartbeat(self):
        """Sync at once, then on every beat: keeps the bridges live in hcom,
        and picks up starts, stops and recoveries."""
        while not self.stopped:
            try:
                self.sync()
            except Exception as e:  # never let the thread die
                log(self.env, "ERROR", "plugin.sync_error", None, error=repr(e))
            self.wake.set()
            time.sleep(HEARTBEAT if self.synced else SYNC_RETRY)

    def reload_plugin(self):
        """Ask the gateway to reload plugins, through Hermes's own control-socket
        client (what `hermes plugins enable` does). A gateway too old for the
        verb answers None: it keeps this copy, and `hcom status` says restart."""
        try:
            from gateway.control_socket import reload_gateway_plugins
            from hermes_constants import get_default_hermes_root
            home = Path(self.home)
            answer = reload_gateway_plugins(home)
            root = Path(get_default_hermes_root())
            if answer is None and root.resolve() != home.resolve():
                answer = reload_gateway_plugins(root, profile_home=home)
            reloaded = bool(answer and answer.get("reloaded"))
        except Exception as e:
            reloaded = False
            log(self.env, "WARN", "plugin.reload_failed", None, error=repr(e))
        log(self.env, "INFO", "plugin.reload_requested", None, reloaded=reloaded)

    def show_notice(self, bridge, text):
        """A best-effort platform notice, separate from mailbox admission.
        Upstream has no plugin send facade yet. Guard its gateway route helpers
        and use the same profile/allowlist checks as public injection. Never
        fall back to another profile, shell sending, or a new agent turn."""
        try:
            from gateway.run import _gateway_runner_ref
            runner = _gateway_runner_ref()
            loop = getattr(runner, "_gateway_loop", None)
            if loop is None or not loop.is_running():
                raise RuntimeError("gateway loop unavailable")

            async def send():
                if bridge.inactive() or bridge.visibility == "off" or not bridge.origin:
                    return
                if not self.ctx._gateway_injection_allowed():
                    raise RuntimeError("gateway injection grant removed")
                source = runner._plugin_origin_source(
                    bridge.origin, plugin_home=Path(self.home), plugin_id=self.ctx.plugin_id)
                if source is None or not runner._plugin_injection_authorized(
                        source, plugin_id=self.ctx.plugin_id, target="hcom notice"):
                    raise RuntimeError("bridge route refused")
                adapter = runner._delivery_adapter_for(source)
                if adapter is None:
                    raise RuntimeError("bridge adapter unavailable")
                result = await adapter.send(
                    chat_id=source.chat_id, content=text,
                    metadata=runner._thread_metadata_for_source(source))
                if not getattr(result, "success", False):
                    raise RuntimeError("platform send failed")

            future = asyncio.run_coroutine_threadsafe(send(), loop)
            # Do not cancel on timeout or retry: the platform may still send it.
            future.result(timeout=HCOM_TIMEOUT)
        except Exception as e:
            bridge.log("WARN", "plugin.notice_failed", error=repr(e))

    # ── routing ──────────────────────────────────────────────────────────

    def session_facts(self, session_id):
        """(session key, delegated) for `session_id`. A delegate_task child
        inherits its parent chat's session key, so it must never be taken for
        the chat: Hermes marks it in the hook's context, and its session row
        says `subagent`."""
        if not session_id:
            return None, False
        try:
            from agent.delegation_context import is_delegated_child_context
            if is_delegated_child_context():
                return None, True
        except Exception:
            pass
        if session_id in self.sessions:
            return self.sessions[session_id]
        key, source = session_env().get("HERMES_SESSION_KEY") or None, None
        path = os.path.join(self.home, "state.db") if self.home else None
        # No state.db yet: only the chat's own context can say. One that cannot
        # be read fails closed: a delegate must never be taken for the chat.
        if path and os.path.exists(path):
            try:
                with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1)) as db:
                    row = db.execute("SELECT session_key, source FROM sessions WHERE id = ?",
                                     (session_id,)).fetchone()
            except sqlite3.Error as e:
                log(self.env, "WARN", "plugin.session_lookup_failed", None, error=repr(e))
                return None, False
            if row:
                key, source = row[0] or key, row[1]
        facts = (key, source == "subagent")
        if key:
            if len(self.sessions) >= 1024:
                self.sessions.clear()
            self.sessions[session_id] = facts
        return facts

    def route(self, session_id):
        return next((b for b in self.participants() if b.is_main(session_id)), None)

    def route_chat(self, session_id):
        """`route`, else the chat whose key the session carries: a turn can end
        under a session id the bridge never saw (compression rotates it mid
        turn), and that end must still clear the chat's busy flag."""
        bridge = self.route(session_id)
        if bridge is None and self.participants():
            key, delegated = self.session_facts(session_id)
            bridge = None if delegated else self.by_key(key)
        return bridge

    def by_key(self, key):
        return next((b for b in self.participants() if key and b.session_key == key), None)

    # ── Hermes hooks ─────────────────────────────────────────────────────

    def on_session_reset(self, **_):
        pass

    def on_session_start(self, **_):
        pass

    def pre_llm_call(self, session_id=None, user_message=None, **_):
        if not self.participants():
            return None
        key, delegated = self.session_facts(session_id)
        if delegated:
            return None
        bridge = self.by_key(key)
        if bridge is None or bridge.inactive():
            return None
        bridge.follow(session_id)
        return bridge.turn_start(session_id, user_message)

    def on_session_end(self, session_id=None, **_):
        bridge = self.route_chat(session_id)
        if bridge:
            bridge.turn_end()

    def on_session_finalize(self, session_id=None, reason=None, **_):
        bridge = self.route_chat(session_id)
        if bridge:
            bridge.session_finalized(reason)

    # ── /hcom ────────────────────────────────────────────────────────────

    async def command(self, raw_args=""):
        words = (raw_args or "").split()
        action = words[0] if words else ""
        chat = session_env()
        if chat.get("HERMES_SESSION_PLATFORM", "") in NOT_A_CHAT:
            return ("hcom bridges are for messaging chats. To run this Hermes as an hcom "
                    "agent, launch it with `hcom hermes`.")
        if not self.home:
            return "hcom: this Hermes does not report its profile home; update Hermes."
        key = chat.get("HERMES_SESSION_KEY", "")
        if not key:
            return "hcom: this chat has no session key yet; send a message first."
        bridge = self.by_key(key)
        if action == "visibility":
            if not bridge:
                return "This chat is not on hcom. /hcom start first."
            if len(words) != 2 or words[1] not in ("off", "preview", "full"):
                return "Usage: /hcom visibility off|preview|full"
            reply = await asyncio.to_thread(bridge.rpc, ["hermes-bridge", "visibility",
                                          "--name", bridge.name, "--mode", words[1]])
            if not reply or reply.get("visibility") != words[1]:
                return "hcom: could not save visibility; try again."
            bridge.update(reply)
            return f"Incoming hcom message visibility: {bridge.visibility}."
        if action == "start":
            if bridge:
                return f"This chat is already the hcom participant {bridge.name}."
            return await asyncio.to_thread(self.start_chat, chat, words[1] if len(words) > 1 else "")
        if action == "stop":
            if not bridge:
                return "This chat is not on hcom."
            return await asyncio.to_thread(self.stop_chat, bridge)
        if action:
            return "Usage: /hcom [start [name] | stop | visibility off|preview|full]"
        if bridge:
            return (f"This chat is the hcom participant {bridge.name}. Agents reach it with "
                    f"`hcom send @{bridge.name}`. Incoming visibility: {bridge.visibility}. "
                    "/hcom visibility off|preview|full to change; /hcom stop to disconnect.")
        return "This chat is not on hcom. /hcom start [name] lets other agents message it."

    def start_chat(self, chat, name):
        platform = chat["HERMES_SESSION_PLATFORM"]
        origin = {"platform": platform}
        for field in ("chat_id", "chat_name", "chat_type", "thread_id", "user_id", "user_name"):
            value = chat.get("HERMES_SESSION_" + field.upper())
            if value:
                origin[field] = value
        label = chat.get("HERMES_SESSION_CHAT_NAME") or chat.get("HERMES_SESSION_USER_NAME") or ""
        args = ["hermes-bridge", "add", "--home", self.home, "--session-key",
                chat["HERMES_SESSION_KEY"], "--platform", platform, "--origin", json.dumps(origin),
                "--chat", label, "--session-id", chat.get("HERMES_SESSION_ID", "")]
        if name:
            args += ["--name", name]
        reply = self.rpc(args, timeout=JOIN_TIMEOUT)
        if reply is None or not reply.get("name"):
            return "hcom: could not start (see hcom's log: hcom status --logs)."
        self.take_over()
        self.synced = True  # the start granted injection already
        self.sync()
        bridge = self.bridges.get(reply["name"])
        if bridge is None:
            return "hcom: started, but this gateway could not serve it yet (hcom status --logs)."
        bridge.follow(chat.get("HERMES_SESSION_ID", ""))
        self.start_threads()
        self.wake.set()
        return (f"Started hcom as {bridge.name}. Other agents can message this chat with "
                f"`hcom send @{bridge.name}`; replies to them go out through hcom.")

    def stop_chat(self, bridge):
        # Stop acting first: a delivery tick racing the removal would present
        # a token hcom has just unbound. Whether or not hcom confirms, the next
        # sync settles it (it drops the bridge once it is gone).
        with bridge.lock:
            bridge.paused = True
            bridge.gen += 1
            bridge.delivering = False
        reply = bridge.rpc(["hermes-bridge", "rm", "--name", bridge.name])
        if not reply or reply.get("ok") is not True:
            return f"hcom: could not confirm stopping ({bridge.name}); try /hcom stop again."
        self.sync()
        return (f"Stopped hcom for {bridge.name}: other agents can no longer message this chat. "
                f"/hcom start {bridge.name} to reconnect.")


HOOKS = (
    "on_session_reset",
    "on_session_start",
    "pre_llm_call",
    "transform_tool_result",
    "post_llm_call",
    "pre_tool_call",
    "on_human_input_request",
    "on_human_input_resolved",
    "on_session_end",
    "on_session_finalize",
)


def setup(ctx, env, pid, settings):
    """Everything register() does, with the process facts passed in (tests)."""
    scrub_env(env, settings.get("scrub_vars") or ())
    if env.get(HERMES_HOST_PROCESS_ENV):
        # The scrub and the PID claim would only reach this host process, never
        # Hermes's terminal children, so a nested Hermes could take the identity.
        if launched_by_hcom(env):
            log(env, "ERROR", "plugin.host_isolation",
                error="hcom needs plugins.isolation: in_process for this profile")
        return None
    if not claim_host(env, pid):
        return None
    # Read now: from a background thread in a multiplexed process this reports
    # the ambient profile, not this plugin's.
    home = getattr(getattr(ctx, "_manager", None), "home_path", None)
    home = str(home) if home is not None else None
    if launched_by_hcom(env):
        host = Host(ctx, env, home)
    else:
        host = GatewayHost(ctx, env, home, settings)
        ctx.register_command("hcom", host.command, description="Message other AI agents via hcom",
                             args_hint="[start [name] | stop | visibility off|preview|full]")
    for hook in HOOKS:
        ctx.register_hook(hook, getattr(host, hook))
    return host


def register(ctx):
    # Never bind here: register() also runs in one-off `hermes` subcommands.
    host = setup(ctx, os.environ, os.getpid(), _load_settings())
    if host is not None and callable(getattr(ctx, "on_unload", None)):
        ctx.on_unload(host.stop)
    if isinstance(host, GatewayHost):
        host.start()
    elif host is not None:
        threading.Thread(target=host.me.watch_cli, name="hcom-cli-watch", daemon=True).start()
