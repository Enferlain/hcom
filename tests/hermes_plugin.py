"""Hermes plugin (src/hermes_plugin/__init__.py) against a fake Hermes and a
fake hcom. Stdlib only; run with `python3 -I tests/hermes_plugin.py` (part of
scripts/typecheck.sh, so `just ci`)."""

import asyncio
import importlib.util
import os
import queue
import sys
import tempfile
import threading
import time
import unittest

sys.dont_write_bytecode = True  # no __pycache__ in src/hermes_plugin
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location(
    "hcom_hermes_plugin", os.path.join(ROOT, "src", "hermes_plugin", "__init__.py")
)
assert _spec and _spec.loader
plugin = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(plugin)

LAUNCH_ENV = {"HCOM_LAUNCHED": "1", "HCOM_PROCESS_ID": "pid-1", "HCOM_TOOL": "hermes"}


class FakeCli:
    """The classic REPL: `_pending_input` is the between-turns queue."""

    def __init__(self, session_id):
        self.session_id = session_id
        self._agent_running = False
        self._single_query_mode = False
        self._pending_input = queue.Queue()

    def take_turn(self):
        """The REPL pulls the next queued input, as its loop does."""
        return self._pending_input.get_nowait()


class FakeManager:
    def __init__(self):
        self._cli_ref = None
        self.home_path = "/h/.hermes/profiles/work"
        self.has_tui_message_injector = True
        self.tui = []
        self.accept = True

    def inject_tui_message(self, session_key, content, plugin_id):
        self.tui.append((session_key, content, plugin_id))
        return self.accept


class FakeCtx:
    plugin_id = "hcom"

    def __init__(self):
        self._manager = FakeManager()
        self.hooks = {}

    def register_hook(self, name, fn):
        self.hooks[name] = fn

    def register_command(self, name, handler, **_):
        self.hooks["/" + name] = handler

    def inject_message(self, *a, **k):
        raise AssertionError("idle delivery must not use ctx.inject_message (it interrupts)")


class FakeHcom:
    """hcom's side of the plugin RPC: a mailbox with a cursor. `on_read`
    runs inside a read, to model events landing while one is in flight."""

    def __init__(self):
        self.calls = []
        self.mail = []  # (event_id, sender, text)
        self.cursors = {}
        self.next_id = 100
        self.on_read = None
        self.names = ["kiri"]  # hermes-start answers these in turn, then the last
        self.lose_ack_replies = 0  # acks that commit but whose reply is lost
        self.ack_delay = 0.0
        self.lock = threading.Lock()

    def send(self, text, sender="luna", to="kiri"):
        with self.lock:
            self.next_id += 1
            self.mail.append((self.next_id, sender, text, to))
            return self.next_id

    @property
    def cursor(self):
        return self.cursors.get("kiri", 0)

    def unread(self, name="kiri"):
        with self.lock:
            at = self.cursors.get(name, 0)
            return [m[:3] for m in self.mail if m[3] == name and m[0] > at]

    def __call__(self, args):
        self.calls.append(args)
        cmd = args[0]
        if cmd == "hermes-start":
            name = self.names.pop(0) if len(self.names) > 1 else self.names[0]
            return {"name": name, "bootstrap": "<hcom_system_context>boot</hcom_system_context>"}
        if cmd == "hermes-read":
            if "--ack" in args:
                up_to = int(args[args.index("--up-to") + 1])
                time.sleep(self.ack_delay)
                name = args[args.index("--name") + 1]
                with self.lock:
                    self.cursors[name] = max(self.cursors.get(name, 0), up_to)
                    if self.lose_ack_replies:
                        self.lose_ack_replies -= 1
                        return None
                return {"acked_to": up_to}
            name = args[args.index("--name") + 1]
            batch = self.unread(name)[:2]
            reply = {
                "messages": [{"event_id": i, "from": s, "message": t} for i, s, t in batch],
                "has_more": len(self.unread(name)) > len(batch),
            }
            if self.on_read:
                hook, self.on_read = self.on_read, None
                hook()
            return reply
        return {"ok": True}

    def commands(self, name):
        return [c for c in self.calls if c[0] == name]


def apply_hooks(host):
    """Fake the host selecting/applying a callback, then reporting its final
    accepted history. Production hooks must never ACK inside the callback."""
    history = []
    for name in ("pre_llm_call", "transform_tool_result"):
        original = getattr(host, name)

        def applied(_original=original, _name=name, **kwargs):
            out = _original(**kwargs)
            if _name == "pre_llm_call":
                text = kwargs.get("user_message") or ""
                context = (out or {}).get("context")
                if context:
                    text += "\n\n" + context
                history.append({"role": "user", "content": text})
            else:
                history.append({"role": "tool", "content": out or kwargs.get("result")})
            host.post_llm_call(session_id=kwargs.get("session_id"), conversation_history=history)
            return out

        setattr(host, name, applied)


def make(bind=None, env=None, receipts=True):
    ctx = FakeCtx()
    p = plugin.setup(ctx, dict(LAUNCH_ENV, **(env or {})), 4242, {"scrub_vars": ["CLAUDECODE"]})
    assert p is not None
    hcom = FakeHcom()
    p.rpc = lambda args, participant=None, **_: hcom(args)
    p.start_notify_server = lambda: None
    p.start_threads = lambda: None
    if receipts:
        apply_hooks(p)
    if bind:
        sid, surface = bind
        p.me.kind = surface
        if surface == "cli":
            ctx._manager._cli_ref = FakeCli(sid)
        assert p.me.bind(sid)
    return p, ctx, hcom


class hcom_binary:
    """Patch subprocess.run so the plugin's real RPC wrapper sees `stdout`
    (or `exc`) from the hcom binary."""

    def __init__(self, stdout="", exc=None):
        self.stdout, self.exc = stdout, exc

    def __enter__(self):
        self.real = plugin.subprocess.run
        outer = self

        class Proc:
            returncode = 0
            stdout = outer.stdout
            stderr = ""

        def run(*a, **k):
            if outer.exc:
                raise outer.exc
            return Proc()

        plugin.subprocess.run = run

    def __exit__(self, *exc):
        plugin.subprocess.run = self.real


def statuses(p):
    out = []
    while True:
        try:
            _, args, _ = p.status_queue.get_nowait()
        except queue.Empty:
            return out
        out.append(args)


class Gate(unittest.TestCase):
    def test_scrubs_markers_even_when_inert(self):
        env = {"CLAUDECODE": "1", "CLAUDE_CODE_SESSION_ID": "x"}
        ctx = FakeCtx()
        settings = {"scrub_vars": ["CLAUDECODE", "CLAUDE_CODE_SESSION_ID"]}
        host = plugin.setup(ctx, env, 1, settings)
        self.assertEqual(env, {"HCOM_PLUGIN_HOST_PID": "1"})
        # Not launched by hcom: only a possible gateway, idle without bridges.
        self.assertIsInstance(host, plugin.GatewayHost)
        self.assertIn("/hcom", ctx.hooks)
        host.rpc = lambda *a: self.fail("no hcom call without bridges")
        self.assertIsNone(host.pre_llm_call(session_id="s", user_message="hi"))
        self.assertIsNone(host.transform_tool_result(result="r", session_id="s"))

    def test_only_an_hcom_hermes_launch_binds(self):
        for env in (
            {"HCOM_LAUNCHED": "1", "HCOM_PROCESS_ID": "p"},
            {"HCOM_LAUNCHED": "1", "HCOM_PROCESS_ID": "p", "HCOM_TOOL": "claude"},
            {"HCOM_PROCESS_ID": "p", "HCOM_TOOL": "hermes"},
            {"HCOM_LAUNCHED": "1", "HCOM_TOOL": "hermes"},
        ):
            self.assertIsInstance(plugin.setup(FakeCtx(), dict(env), 1, {}), plugin.GatewayHost, env)
        ctx = FakeCtx()
        self.assertIsNotNone(plugin.setup(ctx, dict(LAUNCH_ENV), 1, {}))
        self.assertIn("pre_tool_call", ctx.hooks)
        self.assertNotIn("post_tool_call", ctx.hooks)

    def test_pid_claim_keeps_nested_processes_inert(self):
        env = dict(LAUNCH_ENV)
        self.assertTrue(plugin.claim_host(env, 10))
        self.assertEqual(env["HCOM_PLUGIN_HOST_PID"], "10")
        self.assertTrue(plugin.claim_host(env, 10), "same process may re-register")
        # A child (nested hermes, `hermes plugins ...`) inherits the claim.
        self.assertIsNone(plugin.setup(FakeCtx(), dict(env), 11, {}))

    def test_host_isolation_stays_inert_and_says_why(self):
        env = dict(LAUNCH_ENV, HERMES_PLUGIN_HOST_PROCESS="1")
        ctx = FakeCtx()
        self.assertIsNone(plugin.setup(ctx, env, 50, {}))
        self.assertEqual(ctx.hooks, {})
        self.assertNotIn("HCOM_PLUGIN_HOST_PID", env)
        with open(os.path.join(env["HCOM_DIR"], ".tmp", "logs", "hcom.log")) as f:
            self.assertIn("plugins.isolation: in_process", f.read())


class Binding(unittest.TestCase):
    def test_cli_binds_when_the_repl_attaches(self):
        p, ctx, hcom = make()
        ctx._manager._cli_ref = FakeCli("s1")
        p.me.watch_cli()
        self.assertEqual((p.me.name, p.me.session_id, p.me.kind), ("kiri", "s1", "cli"))
        start = hcom.commands("hermes-start")[0]
        self.assertEqual(start[start.index("--session-id") + 1], "s1")
        self.assertEqual(
            start[start.index("--transcript-path") + 1], "/h/.hermes/profiles/work/state.db"
        )

    def test_quiet_one_shot_never_binds(self):
        p, ctx, hcom = make()
        cli = FakeCli("s1")
        cli._single_query_mode = True
        ctx._manager._cli_ref = cli
        p.me.watch_cli()
        self.assertIsNone(p.me.name)
        self.assertEqual(hcom.calls, [])

    def test_tui_binds_on_session_build_and_queues_through_its_injector(self):
        p, ctx, hcom = make()
        p.on_session_reset(session_id="t1", platform="tui", reason=None)
        self.assertEqual((p.me.name, p.me.kind), ("kiri", "tui"))
        hcom.send("hello")
        p.me.tick()
        key, text, plugin_id = ctx._manager.tui[0]
        self.assertEqual((key, plugin_id), ("t1", "hcom"))
        self.assertIn("hello", text)

    def test_cli_new_session_rebinds_and_bootstraps_again(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        self.assertIn("boot", p.pre_llm_call(session_id="s1", user_message="hi")["context"])
        self.assertIsNone(p.pre_llm_call(session_id="s1", user_message="again"))
        p.on_session_end(session_id="s1")
        p.on_session_finalize(session_id="s1", reason="session_boundary")
        self.assertEqual(hcom.commands("hermes-stop"), [])
        ctx._manager._cli_ref.session_id = "s2"
        p.on_session_reset(session_id="s2", platform="cli", reason="new_session")
        self.assertEqual(p.me.session_id, "s2")
        self.assertEqual(len(hcom.commands("hermes-start")), 2)
        self.assertIn("boot", p.pre_llm_call(session_id="s2", user_message="x")["context"])


class Detach(unittest.TestCase):
    def detached(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        fake = p.rpc
        del p.rpc  # the real wrapper, over a fake hcom binary
        with hcom_binary('{"error": "kiri is not attached to this process"}'):
            self.assertIsNone(p.rpc(["hermes-read", "--name", "kiri"], p.me))
        p.rpc = fake
        return p, ctx, hcom

    def test_a_killed_or_resumed_identity_never_binds_again(self):
        p, ctx, hcom = self.detached()
        self.assertTrue(p.me.detached)
        starts = len(hcom.commands("hermes-start"))
        ctx._manager._cli_ref.session_id = "s2"
        p.on_session_reset(session_id="s2", platform="cli")
        p.on_session_start(session_id="s2")
        p.pre_llm_call(session_id="s2", user_message="x")
        self.assertFalse(p.me.bind("s3"))
        self.assertEqual(len(hcom.commands("hermes-start")), starts)
        hcom.send("late")
        p.me.tick()
        self.assertIsNone(p.transform_tool_result(result="r", session_id="s1"))
        self.assertTrue(ctx._manager._cli_ref._pending_input.empty())
        self.assertEqual(hcom.cursor, 0)

    def test_a_failed_first_bind_is_retried(self):
        p, ctx, hcom = make()
        fake = p.rpc
        p.rpc = lambda args, participant=None, **_: None
        self.assertFalse(p.me.bind("s1"))
        self.assertFalse(p.me.detached)
        p.rpc = fake
        self.assertTrue(p.me.bind("s1"))


class Delivery(unittest.TestCase):
    def test_idle_delivery_queues_for_the_next_turn_and_acks_on_admission(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        cli = ctx._manager._cli_ref
        eid = hcom.send("ping")
        p.me.tick()
        text = cli.take_turn()
        self.assertEqual(text, f"<hcom>[new message #{eid}] luna -> kiri: ping</hcom>")
        self.assertEqual(hcom.cursor, 0, "not acked on hand-off")
        p.me.tick()
        self.assertTrue(cli._pending_input.empty(), "one delivery in flight")
        out = p.pre_llm_call(session_id="s1", user_message=text)
        self.assertEqual(hcom.cursor, eid)
        self.assertIsNone(p.me.out)
        self.assertNotIn("ping", out["context"], "already the user message")
        self.assertIn(["hermes-status", "--status", "active", "--context", "deliver:luna",
                       "--detail", ""], statuses(p))

    def test_admission_is_the_exact_text(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        hcom.send("ping")
        p.me.tick()
        text = ctx._manager._cli_ref.take_turn()
        for prompt in (f"x{text}", f"{text} ", f"quote: '{text}'",
                       [{"type": "text", "text": f"see {text}"}]):
            p.pre_llm_call(session_id="s1", user_message=prompt)
            p.on_session_end(session_id="s1")
            self.assertEqual(hcom.cursor, 0, repr(prompt))
        p.pre_llm_call(session_id="s1", user_message=text)
        self.assertGreater(hcom.cursor, 0)

    def test_a_photo_turn_carrying_the_batch_admits_it(self):
        # Hermes passes multimodal turns through as a list of parts.
        p, ctx, hcom = make()
        p.on_session_reset(session_id="t1", platform="tui")
        eid = hcom.send("ping")
        p.me.tick()
        text = ctx._manager.tui[0][1]
        photo = [{"type": "text", "text": f"look\n\n{text}"},
                 {"type": "image_url", "image_url": {"url": "x"}}]
        p.pre_llm_call(session_id="t1", user_message=photo)
        self.assertEqual(hcom.cursor, eid)
        self.assertIsNone(p.me.out)

    def test_an_unrecognized_turn_start_is_confirmed_from_persisted_rows(self):
        home = tempfile.mkdtemp(dir=LAUNCH_ENV["HCOM_DIR"])
        p, ctx, hcom = make()
        p.home = home
        p.on_session_reset(session_id="t1", platform="tui")
        eid = hcom.send("ping")
        p.me.tick()
        state_db(home, sessions=[("t1", None, "tui")], user_rows=[("t1", ctx._manager.tui[0][1])])
        p.me.tick()
        self.assertEqual(hcom.cursor, eid, "acked, not held until the session ends")
        self.assertIsNone(p.me.out)

    def test_delayed_admission_past_grace_delivers_one_copy(self):
        # The /new confirmation (or a clarify) holds the CLI queue.
        p, ctx, hcom = make(bind=("s1", "cli"))
        cli = ctx._manager._cli_ref
        hcom.send("ping")
        p.me.tick()
        p.me.out.sent_at -= plugin.ADMIT_GRACE + plugin.PENDING_POLL + 1
        p.me.last_poll -= plugin.PENDING_POLL + 1
        p.me.notify()
        p.me.tick()
        p.me.tick()
        self.assertEqual(cli._pending_input.qsize(), 1, "still held: never released")
        # The modal resolves into a new session whose first turn is the delivery.
        p.on_session_finalize(session_id="s1", reason="session_boundary")
        cli.session_id = "s2"
        p.on_session_reset(session_id="s2", platform="cli", reason="new_session")
        p.me.tick()
        text = cli.take_turn()
        self.assertTrue(cli._pending_input.empty())
        p.me.tick()  # between the REPL dequeuing it and the turn starting
        out = p.pre_llm_call(session_id="s2", user_message=text)
        self.assertNotIn("ping", out["context"], "not also delivered as turn context")
        self.assertEqual(hcom.cursor, 101)
        p.on_session_end(session_id="s2")
        p.me.tick()
        self.assertTrue(cli._pending_input.empty(), "no second copy")

    def test_delivery_dequeued_across_new_session_is_admitted_once(self):
        # Live order: the REPL takes the queued text, the /new confirmation
        # switches sessions, then the text runs as the new session's first turn.
        p, ctx, hcom = make(bind=("s1", "cli"))
        cli = ctx._manager._cli_ref
        hcom.send("ping")
        p.me.tick()
        p.me.out.sent_at -= plugin.ADMIT_GRACE + 1
        text = cli.take_turn()
        p.on_session_finalize(session_id="s1", reason="session_boundary")
        cli.session_id = "s2"
        p.on_session_reset(session_id="s2", platform="cli", reason="new_session")
        p.me.tick()
        out = p.pre_llm_call(session_id="s2", user_message=text)
        self.assertNotIn("ping", out["context"], "not also delivered as turn context")
        self.assertEqual(hcom.cursor, 101)
        self.assertIsNone(p.me.out)
        self.assertTrue(cli._pending_input.empty(), "no second copy")

    def test_unadmitted_delivery_hermes_dropped_is_released_not_acked(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        hcom.send("ping")
        p.me.tick()
        ctx._manager._cli_ref.take_turn()  # the REPL took it, then no turn ran
        p.me.out.sent_at -= plugin.ADMIT_GRACE + 1
        p.me.tick()
        self.assertIsNone(p.me.out)
        self.assertEqual(hcom.cursor, 0)
        p.me.last_poll -= plugin.PENDING_POLL + 1
        p.me.tick()
        self.assertIsNotNone(p.me.out, "redelivered on the next poll")

    def test_never_queues_while_a_turn_runs(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        cli = ctx._manager._cli_ref
        hcom.send("ping")
        cli._agent_running = True
        p.me.tick()
        self.assertTrue(cli._pending_input.empty())
        p.pre_llm_call(session_id="s1", user_message="user prompt")  # takes the mail
        hcom.send("pong")
        p.me.notify()
        cli._agent_running = False
        p.me.tick()
        self.assertTrue(cli._pending_input.empty(), "own busy flag holds until on_session_end")
        p.on_session_end(session_id="s1")
        cli._agent_running = True  # lags on_session_end
        p.me.tick()
        self.assertTrue(cli._pending_input.empty())
        cli._agent_running = False
        p.me.tick()
        self.assertIn("pong", cli.take_turn())

    def test_turn_start_and_mid_turn_deliveries_ack_after_host_applies_them(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        first = hcom.send("one")
        out = p.pre_llm_call(session_id="s1", user_message="do work")
        self.assertIn("luna -> kiri: one", out["context"])
        self.assertEqual(hcom.cursor, first)
        reads = len(hcom.commands("hermes-read"))
        self.assertIsNone(p.transform_tool_result(result="ok", session_id="s1"))
        self.assertEqual(len(hcom.commands("hermes-read")), reads, "no RPC without a wake")
        second = hcom.send("two")
        p.me.notify()
        self.assertIsNone(p.transform_tool_result(result="ok", session_id="sub-1"),
                          "subagent tool results never carry mail")
        out = p.transform_tool_result(result="ok", session_id="s1", tool_name="terminal")
        self.assertTrue(out.startswith("ok\n\n<hcom>"))
        self.assertEqual(hcom.cursor, second)
        self.assertFalse(p.me.pending)

    def test_wake_during_read_is_not_lost(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        hcom.send("one")
        p.me.notify()

        def arrive():  # a second message and its notify, mid-read
            hcom.send("two")
            t = threading.Thread(target=p.me.notify)
            t.start()
            t.join()

        hcom.on_read = arrive
        p.pre_llm_call(session_id="s1", user_message="go")  # drains one, sees has_more=False
        self.assertTrue(p.me.pending, "the wake that landed mid-read survives")
        out = p.transform_tool_result(result="r", session_id="s1")
        self.assertIn("two", out)

    def test_threaded_wakes_drain_every_message(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        p.pre_llm_call(session_id="s1", user_message="go")
        done = threading.Event()

        def sender():
            for i in range(20):
                hcom.send(f"m{i}")
                p.me.notify()
                time.sleep(0.001)
            done.set()

        seen = []
        t = threading.Thread(target=sender)
        t.start()
        while not done.is_set() or p.me.pending:
            out = p.transform_tool_result(result="r", session_id="s1")
            if out:
                seen.append(out)
        t.join()
        self.assertEqual(hcom.unread(), [])
        joined = " ".join(seen)
        for i in range(20):
            self.assertEqual(joined.count(f"kiri: m{i}<") + joined.count(f"kiri: m{i} |"), 1, i)

    def test_stale_reads_are_dropped_after_a_session_change(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        cli = ctx._manager._cli_ref
        hcom.send("ping")

        def new_session():  # /new lands while the idle read is in flight
            p.on_session_finalize(session_id="s1", reason="session_boundary")
            cli.session_id = "s2"
            p.on_session_reset(session_id="s2", platform="cli")

        hcom.on_read = new_session
        p.me.tick()
        self.assertTrue(cli._pending_input.empty(), "the old session's read is dropped")
        self.assertEqual(hcom.cursor, 0)
        p.me.tick()
        self.assertIn("ping", cli.take_turn())

    def test_rejected_hand_off_is_retried_on_the_next_wake_or_poll(self):
        p, ctx, hcom = make()
        p.on_session_reset(session_id="t1", platform="tui")
        ctx._manager.accept = False
        hcom.send("ping")
        p.me.tick()
        self.assertIsNone(p.me.out)
        ctx._manager.accept = True
        p.me.tick()
        self.assertIsNone(p.me.out, "not re-sent every reconcile tick")
        self.assertEqual(hcom.cursor, 0, "still unread")
        p.me.notify()
        p.me.tick()
        self.assertIsNotNone(p.me.out)

    def test_batches_drain_in_order(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        ids = [hcom.send(f"m{i}") for i in range(3)]
        out = p.transform_tool_result(result="r", session_id="s1")
        self.assertIn("[2 new messages]", out)
        self.assertTrue(p.me.pending, "has_more keeps it pending")
        out = p.transform_tool_result(result="r", session_id="s1")
        self.assertIn("m2", out)
        self.assertEqual(hcom.cursor, ids[-1])

    def test_subagent_hooks_are_ignored(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        self.assertIsNone(p.pre_llm_call(session_id="sub-1", user_message="task"))
        self.assertFalse(p.me.busy)
        p.pre_llm_call(session_id="s1", user_message="go")
        p.on_session_end(session_id="sub-1")
        self.assertTrue(p.me.busy, "a subagent finishing does not end the parent's turn")
        self.assertEqual(len(hcom.commands("hermes-start")), 1)


class Outstanding(unittest.TestCase):
    def test_callbacks_wait_for_selected_context_before_ack(self):
        p, ctx, hcom = make(bind=("s1", "cli"), receipts=False)
        first = hcom.send("context mail")
        out = p.pre_llm_call(session_id="s1", user_message="human")
        self.assertEqual(hcom.cursor, 0)
        p.post_llm_call(session_id="s1", conversation_history=[
            {"role": "user", "content": "human", "api_content": "human\n\n" + out["context"]}])
        self.assertEqual(hcom.cursor, first)
        second = hcom.send("tool mail")
        p.me.notify()
        transformed = p.transform_tool_result(session_id="s1", result="original")
        self.assertEqual(hcom.cursor, first, "proposing a result is not admission")
        p.post_llm_call(session_id="s1", conversation_history=[
            {"role": "tool", "content": "another plugin won"}])
        self.assertEqual(hcom.cursor, first, "discarded transforms must remain unread")
        p.post_llm_call(session_id="subagent", conversation_history=[
            {"role": "tool", "content": transformed}])
        self.assertEqual(hcom.cursor, first)
        p.post_llm_call(session_id="s1", conversation_history=[
            {"role": "tool", "content": transformed}])
        self.assertEqual(hcom.cursor, second)

    def test_dropped_output_is_released_after_turn_without_ack(self):
        p, ctx, hcom = make(bind=("s1", "cli"), receipts=False)
        eid = hcom.send("not accepted")
        p.pre_llm_call(session_id="s1", user_message="human")
        p.on_session_end(session_id="s1")
        p.me.out.sent_at -= plugin.ADMIT_GRACE + 1
        p.me.release_unadmitted()
        self.assertIsNone(p.me.out)
        self.assertEqual(hcom.cursor, 0)
        p.me.notify()
        p.me.tick()
        self.assertIn("not accepted", ctx._manager._cli_ref.take_turn())
        self.assertEqual(p.me.out.ack_id, eid)

    def test_a_lost_ack_reply_never_loses_the_mail(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        p.pre_llm_call(session_id="s1", user_message="go")
        eid = hcom.send("one")
        p.me.notify()
        hcom.lose_ack_replies = 1  # the cursor commits, the reply is lost
        out = p.transform_tool_result(result="r", session_id="s1")
        self.assertIn("one", out, "the model still gets the batch")
        self.assertEqual(p.me.out.state, "admitted")
        hcom.send("two")
        p.me.notify()
        self.assertIsNone(p.transform_tool_result(result="r", session_id="s1"),
                          "nothing else is read while an ack is unsettled")
        p.me.tick()
        self.assertIsNone(p.me.out)
        self.assertEqual(hcom.cursor, eid)
        out = p.transform_tool_result(result="r", session_id="s1")
        self.assertIn("two", out)

    def test_a_lost_ack_after_admission_is_retried(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        eid = hcom.send("ping")
        p.me.tick()
        text = ctx._manager._cli_ref.take_turn()
        hcom.lose_ack_replies = 1
        p.pre_llm_call(session_id="s1", user_message=text)
        self.assertEqual(p.me.out.state, "admitted")
        p.on_session_end(session_id="s1")
        p.me.tick()
        self.assertIsNone(p.me.out)
        self.assertEqual(hcom.cursor, eid)
        self.assertTrue(ctx._manager._cli_ref._pending_input.empty(), "not handed off again")

    def test_rebind_to_another_identity_never_acks_its_mailbox(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        cli = ctx._manager._cli_ref
        hcom.names = ["sora"]
        old = hcom.send("for kiri")
        p.me.tick()
        text = cli.take_turn()
        # In-process /resume into a session another identity owns.
        cli.session_id = "s9"
        p.on_session_reset(session_id="s9", platform="cli")
        self.assertEqual(p.me.name, "sora")
        self.assertIsNone(p.me.out, "kiri's batch is not carried into sora")
        p.pre_llm_call(session_id="s9", user_message=text)
        acks = [c for c in hcom.commands("hermes-read") if "--ack" in c]
        self.assertFalse(any(c[2] == "sora" and c[-1] == str(old) for c in acks), acks)

    def test_same_identity_new_session_keeps_the_batch(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        cli = ctx._manager._cli_ref
        hcom.send("ping")
        p.me.tick()
        rec = p.me.out
        cli.session_id = "s2"
        p.on_session_reset(session_id="s2", platform="cli")
        self.assertIs(p.me.out, rec)
        self.assertEqual((rec.owner, rec.target), ("kiri", "s2"))

    def test_hand_off_routes_to_the_session_it_was_read_for(self):
        p, ctx, hcom = make()
        p.on_session_reset(session_id="t1", platform="tui")
        hcom.send("ping")
        real_read = p.me.read

        def read_then_reset(op, state):  # a TUI reset lands right after the read
            rec = real_read(op, state)
            p.me.session_id = "t2"
            return rec

        p.me.read = read_then_reset
        p.me.tick()
        self.assertEqual(ctx._manager.tui[0][0], "t1")

    def test_acks_are_serialized(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        hcom.send("ping")
        p.me.tick()
        rec = p.me.out
        rec.state = rec.ADMITTED
        hcom.ack_delay = 0.05
        threads = [threading.Thread(target=p.me.settle, args=(rec,)) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len([c for c in hcom.commands("hermes-read") if "--ack" in c]), 1)


class TuiQueue(unittest.TestCase):
    """The TUI merges text queued behind a busy turn: earlier + "\n\n" + later
    (tui_gateway/session_auto_continue.py)."""

    def test_a_merged_prompt_admits_the_batch_once(self):
        p, ctx, hcom = make()
        p.on_session_reset(session_id="t1", platform="tui")
        eid = hcom.send("ping")
        p.me.tick()
        text = ctx._manager.tui[0][1]
        for merged in (f"human first\n\n{text}", f"{text}\n\nhuman after",
                       f"a\n\n{text}\n\nb"):
            self.assertTrue(plugin.carries(merged, text), merged)
        for quoted in (f"see: {text}", f"{text}!", f"'{text}'\n\nx", text[:-1]):
            self.assertFalse(plugin.carries(quoted, text), quoted)
        out = p.pre_llm_call(session_id="t1", user_message=f"human first\n\n{text}")
        self.assertEqual(hcom.cursor, eid)
        self.assertNotIn("ping", out["context"] if out else "")

    def test_a_tui_hand_off_waits_for_its_turn(self):
        p, ctx, hcom = make()
        p.on_session_reset(session_id="t1", platform="tui")
        hcom.send("ping")
        p.me.tick()
        p.me.out.sent_at -= plugin.ADMIT_GRACE * 10
        p.me.tick()
        self.assertIsNotNone(p.me.out, "not released while the TUI may still hold it")
        self.assertEqual(len(ctx._manager.tui), 1)
        p.on_session_finalize(session_id="t1", reason=None)
        self.assertIsNone(p.me.out)


class FakeGatewayCtx(FakeCtx):
    """A multiplexed gateway: public inject_message by session key or origin."""

    def __init__(self):
        super().__init__()
        self._manager.has_gateway_message_injector = True
        self._manager.has_tui_message_injector = False
        self.injected = []
        self.accept_key = True

    def inject_message(self, text, role="user", session_key=None, origin=None):
        self.injected.append((text, session_key, origin))
        return self.accept_key if session_key else origin is not None


CHAT = {"HERMES_SESSION_KEY": "agent:work:irc:dm:anno", "HERMES_SESSION_ID": "g1",
        "HERMES_SESSION_PLATFORM": "irc", "HERMES_SESSION_CHAT_ID": "anno",
        "HERMES_SESSION_CHAT_TYPE": "dm", "HERMES_SESSION_USER_ID": "anno",
        "HERMES_SESSION_USER_NAME": "anno"}
ROUTE = {"platform": "irc", "chat_id": "anno"}


class FakeBridgeHcom(FakeHcom):
    """hcom with a bridge registry (the only authority)."""

    def __init__(self):
        super().__init__()
        self.names = ["chat1"]
        self.envs = []
        self.registry = {}
        self.recreated = set()  # names the next sync reports as re-created
        self.stale = set()  # names whose token hcom refuses until the next sync
        self.fail = set()  # bridge subcommands that fail (no reply)
        self.reload = False  # sync says the gateway runs an older plugin
        self.hashes = []  # --plugin-hash of every sync

    def join(self, name="chat1", key=CHAT["HERMES_SESSION_KEY"]):
        self.registry[name] = {"name": name, "token": f"tok-{name}", "session_key": key,
                               "origin": dict(ROUTE)}

    def call(self, args, env, participant=None):
        self.envs.append((args[:2], env.get("HCOM_PROCESS_ID")))
        if args[0] == "hermes-bridge":
            sub = args[1]
            if sub in self.fail:
                return None
            if sub == "add":
                name = args[args.index("--name") + 1] if "--name" in args else "chat1"
                self.join(name, args[args.index("--session-key") + 1])
                return dict(self.registry[name])
            if sub == "sync":
                self.hashes.append(args[args.index("--plugin-hash") + 1])
                entries = [dict(e, bootstrap="BRIDGE-BOOT", recreated=n in self.recreated)
                           for n, e in self.registry.items()]
                self.recreated.clear()
                self.stale.clear()
                return {"bridges": entries, "reload_plugin": self.reload}
            if sub == "rm":
                self.registry.pop(args[args.index("--name") + 1], None)
                return {"ok": True}
            if sub == "visibility":
                name = args[args.index("--name") + 1]
                self.registry[name]["visibility"] = args[args.index("--mode") + 1]
                return dict(self.registry[name])
            return {"ok": True}
        name = args[args.index("--name") + 1] if "--name" in args else None
        if name in self.stale:
            participant.detach()
            return None
        return self(args)


class FakeDelegation:
    """agent.delegation_context: a ContextVar set while a delegate runs."""

    flag = __import__("contextvars").ContextVar("delegated", default=False)

    @classmethod
    def is_delegated_child_context(cls):
        return cls.flag.get()


def gateway(chat=None, hcom=None, joined=True, home=None, receipts=True):
    ctx = FakeGatewayCtx()
    env = {"HCOM_DIR": LAUNCH_ENV["HCOM_DIR"]}
    host = plugin.setup(ctx, env, 4242, {})
    assert isinstance(host, plugin.GatewayHost)
    hcom = hcom or FakeBridgeHcom()
    host.rpc = lambda args, participant=None, **_: hcom.call(
        args, getattr(participant, "env", None) or host.env, participant)
    host.start_notify_server = lambda: None
    host.start_threads = lambda: None
    if receipts:
        apply_hooks(host)
    host.home = home or "/h/.hermes/profiles/work"
    current = dict(chat or CHAT)
    plugin.session_env = lambda: dict(current)
    if joined:
        hcom.join()
        assert host.sync()
    return host, ctx, hcom, current


def state_db(home, sessions=(), user_rows=()):
    """A profile state.db with the columns the plugin reads."""
    import sqlite3
    os.makedirs(home, exist_ok=True)
    db = sqlite3.connect(os.path.join(home, "state.db"))
    db.executescript("CREATE TABLE sessions (id TEXT PRIMARY KEY, session_key TEXT, source TEXT);"
                     "CREATE TABLE messages (session_id TEXT, role TEXT, content TEXT);")
    db.executemany("INSERT INTO sessions VALUES (?, ?, ?)", sessions)
    db.executemany("INSERT INTO messages VALUES (?, 'user', ?)", user_rows)
    db.commit()
    db.close()


class Bridges(unittest.TestCase):
    def setUp(self):
        self.real_session_env = plugin.session_env
        sys.modules["agent"] = type(sys)("agent")
        sys.modules["agent.delegation_context"] = FakeDelegation
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        plugin.session_env = self.real_session_env
        sys.modules.pop("agent.delegation_context", None)
        sys.modules.pop("agent", None)
        sys.__dict__.pop("_hcom_gateway_hosts", None)

    def test_start_from_a_chat_and_reply_through_hcom(self):
        host, ctx, hcom, chat = gateway(joined=False)
        out = asyncio.run(host.command("start"))
        self.assertIn("Started hcom as chat1", out)
        self.assertEqual(host.bridges["chat1"].session_id, "g1")
        self.assertIn("already", asyncio.run(host.command("start")))

    def test_visibility_is_per_chat_persisted_and_reported(self):
        host, ctx, hcom, chat = gateway()
        hcom.join("chat2", key="another-chat")
        host.sync()
        self.assertIn("visibility: preview", asyncio.run(host.command()))
        for mode in ("off", "preview", "full"):
            self.assertIn(mode, asyncio.run(host.command("visibility " + mode)))
            self.assertEqual(hcom.registry["chat1"]["visibility"], mode)
            self.assertEqual(host.bridges["chat2"].visibility, "preview")
            host.sync()
            self.assertEqual(host.bridges["chat1"].visibility, mode)
        hcom.fail.add("visibility")
        self.assertIn("could not save", asyncio.run(host.command("visibility off")))
        self.assertEqual(host.bridges["chat1"].visibility, "full")
        self.assertIn("Usage", asyncio.run(host.command("visibility full extra")))
        self.assertIn("Usage", asyncio.run(host.command("visibility wrong")))

    def test_visibility_notices_are_bounded_and_once_per_batch(self):
        host, ctx, hcom, chat = gateway(receipts=False)
        b = host.bridges["chat1"]
        b.update(dict(hcom.registry["chat1"], visibility="off"))
        hcom.send("A" * 300, to="chat1")
        b.tick()
        self.assertFalse(any(a[1][0] == "notice" for a in list(host.status_queue.queue)))
        b.update(dict(hcom.registry["chat1"], visibility="preview"))
        b.notice(b.out)
        notices = [a[1][1] for a in list(host.status_queue.queue) if a[1][0] == "notice"]
        self.assertEqual(len(notices), 1)
        self.assertIn("@luna", notices[0])
        self.assertEqual(notices[0].split("\n", 1)[1], "A" * 240 + "…")
        b.notice(b.out)
        self.assertEqual(sum(a[1][0] == "notice" for a in list(host.status_queue.queue)), 1)
        rec = b.out
        rec.noticed.clear()
        b.visibility = "full"
        b.notice(rec)
        self.assertEqual(list(host.status_queue.queue)[-1][1][1].split("\n", 1)[1], "A" * 300)

    def test_unseen_hint_only_when_visibility_off(self):
        host, ctx, hcom, chat = gateway(receipts=False)
        b = host.bridges["chat1"]
        b.update(dict(hcom.registry["chat1"], visibility="off"))
        hcom.send("hi", to="chat1")
        b.tick()
        text = ctx.injected[-1][0]
        self.assertTrue(text.endswith("</hcom>\n" + plugin.UNSEEN_HINT), text)
        self.assertEqual(b.out.text, text)
        b.visibility = "full"
        b.hand_off(b.out)
        self.assertTrue(ctx.injected[-1][0].endswith("</hcom>"))
        self.assertEqual(b.out.text, ctx.injected[-1][0])

    def test_notice_transport_keeps_profile_topic_and_allowlist(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever)
        thread.start()
        host, ctx, hcom, chat = gateway()
        bridge = host.bridges["chat1"]
        bridge.visibility = "full"
        bridge.origin = dict(ROUTE, thread_id="topic1")
        sent = []
        routes = []
        authorized = [True]

        async def send(**kwargs):
            sent.append(kwargs)
            return SimpleNamespace(success=True)

        def source(origin, **kwargs):
            routes.append((origin, kwargs))
            return SimpleNamespace(chat_id=origin["chat_id"], thread_id=origin["thread_id"])

        runner = SimpleNamespace(
            _gateway_loop=loop, _plugin_origin_source=source,
            _plugin_injection_authorized=lambda *a, **k: authorized[0],
            _delivery_adapter_for=lambda s: SimpleNamespace(send=send),
            _thread_metadata_for_source=lambda s: {"thread_id": s.thread_id})
        module = SimpleNamespace(_gateway_runner_ref=lambda: runner)
        ctx._gateway_injection_allowed = lambda: True
        try:
            with patch.dict(sys.modules, {"gateway.run": module}):
                host.show_notice(bridge, "notice")
                self.assertEqual(sent, [{"chat_id": "anno", "content": "notice",
                                         "metadata": {"thread_id": "topic1"}}])
                self.assertEqual(str(routes[0][1]["plugin_home"]), host.home)
                self.assertEqual(routes[0][1]["plugin_id"], "hcom")
                authorized[0] = False
                host.show_notice(bridge, "denied")
                self.assertEqual(len(sent), 1)
                # A failed notice does not alter admission or block the mailbox.
                eid = hcom.send("mail", to="chat1")
                bridge.tick()
                text = ctx.injected[0][0]
                host.pre_llm_call(session_id="g1", user_message=text)
                self.assertEqual(hcom.cursors.get("chat1"), eid)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            loop.close()

    def test_recovery_finds_api_sidecars_and_tools_but_not_other_routes(self):
        import sqlite3
        for role, column in (("user", "api_content"), ("tool", "content")):
            with self.subTest(role=role, column=column):
                home = tempfile.mkdtemp(dir=self.tmp)
                host, ctx, hcom, chat = gateway(home=home, receipts=False)
                eid = hcom.send("accepted", to="chat1")
                message = {"event_id": eid, "from": "luna", "message": "accepted"}
                text = plugin.format_messages([message], "chat1")
                state_db(home, sessions=[("g1", CHAT["HERMES_SESSION_KEY"], "irc"),
                                         ("g2", "another-chat", "irc")])
                with sqlite3.connect(os.path.join(home, "state.db")) as db:
                    db.execute("ALTER TABLE messages ADD COLUMN api_content TEXT")
                    db.execute(f"INSERT INTO messages (session_id, role, {column}) VALUES (?, ?, ?)",
                               ("g2", role, text))
                b = host.bridges["chat1"]
                self.assertEqual(b.persisted_events(plugin.Outstanding(
                    b.name, "g1", [message], eid, False, "offered")), set())
                with sqlite3.connect(os.path.join(home, "state.db")) as db:
                    db.execute(f"INSERT INTO messages (session_id, role, {column}) VALUES (?, ?, ?)",
                               ("g1", role, text))
                b.tick()
                self.assertEqual(ctx.injected, [])
                self.assertEqual(hcom.cursors.get("chat1"), eid)

    def test_worker_confirms_persisted_multimodal_context_during_a_turn(self):
        import sqlite3
        import json
        home = tempfile.mkdtemp(dir=self.tmp)
        host, ctx, hcom, chat = gateway(home=home, receipts=False)
        eid = hcom.send('accepted "quotes"\nand newlines', to="chat1")
        out = host.pre_llm_call(session_id="g1", user_message="human photo")
        self.assertIsNone(hcom.cursors.get("chat1"), "callback has only offered output")
        state_db(home, sessions=[("g1", CHAT["HERMES_SESSION_KEY"], "irc")])
        with sqlite3.connect(os.path.join(home, "state.db")) as db:
            db.execute("INSERT INTO messages VALUES (?, 'user', ?)",
                       ("g1", json.dumps([{"type": "text", "text": out["context"]},
                                          {"type": "image_url", "image_url": {"url": "ignored"}}])))
        bridge = host.bridges["chat1"]
        self.assertTrue(bridge.busy)
        bridge.tick()
        self.assertEqual(hcom.cursors.get("chat1"), eid)
        self.assertIsNone(bridge.out)

    def test_threaded_inform_and_unknown_thread_do_not_block_later_mail(self):
        host, ctx, hcom, chat = gateway(receipts=False)
        b = host.bridges["chat1"]
        first = hcom.send("reply", to="chat1")
        last = hcom.send("next", to="chat1")
        raw_rpc = host.rpc

        def rpc(args, participant=None, **kwargs):
            reply = raw_rpc(args, participant=participant, **kwargs)
            if args[0] == "hermes-read" and "--ack" not in args and reply:
                for m in reply.get("messages", []):
                    if m["event_id"] == first:
                        m.update(intent="inform", thread="unknown-topic", reply_to=77)
            return reply

        host.rpc = rpc
        b.tick()
        text, key, origin = ctx.injected[0]
        self.assertEqual(origin, ROUTE)
        self.assertIn("inform:unknown-topic", text)
        self.assertIn("next", text)
        host.pre_llm_call(session_id="g1", user_message=text)
        self.assertIsNone(hcom.cursors.get("chat1"))
        host.post_llm_call(session_id="g1", conversation_history=[{"role": "user", "content": text}])
        self.assertEqual(hcom.cursors.get("chat1"), last)

    def test_terminal_sessions_cannot_start(self):
        host, ctx, hcom, chat = gateway(chat=dict(CHAT, HERMES_SESSION_PLATFORM="tui"),
                                        joined=False)
        self.assertIn("hcom hermes", asyncio.run(host.command("start")))
        self.assertEqual(host.bridges, {})

    def test_delivery_uses_the_public_api_and_acks_on_admission(self):
        host, ctx, hcom, chat = gateway()
        bridge = host.bridges["chat1"]
        eid = hcom.send("ping", to="chat1")
        bridge.tick()
        text, key, origin = ctx.injected[0]
        self.assertEqual((key, origin), (None, ROUTE))
        out = host.pre_llm_call(session_id="g1", user_message=text)
        self.assertIn("BRIDGE-BOOT", out["context"])
        self.assertEqual(hcom.cursors.get("chat1"), eid)
        reads = [tok for a, tok in hcom.envs if a[0] == "hermes-read"]
        self.assertTrue(reads and all(tok == "tok-chat1" for tok in reads), reads)

    def test_a_bridge_without_a_route_uses_its_session_key(self):
        host, ctx, hcom, chat = gateway()
        bridge = host.bridges["chat1"]
        bridge.origin = None
        hcom.send("ping", to="chat1")
        bridge.tick()
        self.assertEqual(ctx.injected[-1][1:], (CHAT["HERMES_SESSION_KEY"], None))

    def test_a_new_chat_session_is_followed_and_bootstrapped(self):
        host, ctx, hcom, chat = gateway()
        bridge = host.bridges["chat1"]
        host.pre_llm_call(session_id="g1", user_message="hi")
        host.on_session_end(session_id="g1")
        chat["HERMES_SESSION_ID"] = "g2"
        out = host.pre_llm_call(session_id="g2", user_message="after /new")
        self.assertIn("BRIDGE-BOOT", out["context"])
        self.assertEqual(bridge.session_id, "g2")
        self.assertIn((["hermes-bridge", "session"], "tok-chat1"), hcom.envs)

    def test_a_failed_session_record_is_retried_on_the_next_turn(self):
        host, ctx, hcom, chat = gateway()
        bridge = host.bridges["chat1"]
        hcom.fail.add("session")
        host.pre_llm_call(session_id="g1", user_message="hi")
        self.assertEqual(bridge.session_id, "g1", "routing follows at once")
        hcom.fail.clear()
        host.on_session_end(session_id="g1")
        host.pre_llm_call(session_id="g1", user_message="again")
        self.assertEqual(bridge.recorded_session, "g1")

    def test_a_turn_ending_under_a_rotated_session_clears_busy(self):
        home = os.path.join(self.tmp, "work")
        state_db(home, sessions=[("g1", CHAT["HERMES_SESSION_KEY"], "irc"),
                                 ("g2", CHAT["HERMES_SESSION_KEY"], "irc")])
        host, ctx, hcom, chat = gateway(home=home)
        bridge = host.bridges["chat1"]
        host.pre_llm_call(session_id="g1", user_message="long task")
        self.assertTrue(bridge.busy)
        host.on_session_end(session_id="g2")  # compression rotated it mid turn
        self.assertFalse(bridge.busy)

    def test_other_chats_are_ignored(self):
        host, ctx, hcom, chat = gateway()
        chat["HERMES_SESSION_KEY"] = "agent:work:irc:dm:someone"
        hcom.send("ping", to="chat1")
        self.assertIsNone(host.pre_llm_call(session_id="x9", user_message="hello"))
        self.assertIsNone(host.transform_tool_result(result="r", session_id="x9"))
        self.assertEqual(hcom.cursors.get("chat1", 0), 0)

    def test_delegates_never_take_over_the_chat(self):
        home = os.path.join(self.tmp, "work")
        # The context flag covers in-process children; the session row covers
        # any child the flag does not reach.
        state_db(home, sessions=[("g1", CHAT["HERMES_SESSION_KEY"], "irc"),
                                 ("c2", CHAT["HERMES_SESSION_KEY"], "subagent")])
        host, ctx, hcom, chat = gateway(home=home)
        bridge = host.bridges["chat1"]
        host.pre_llm_call(session_id="g1", user_message="research this")
        follows = hcom.envs.count((["hermes-bridge", "session"], "tok-chat1"))

        def delegate(session_id, flagged):
            token = FakeDelegation.flag.set(flagged)
            try:
                self.assertIsNone(host.pre_llm_call(session_id=session_id, user_message="task"))
                hcom.send("for the chat", to="chat1")
                bridge.notify()
                self.assertIsNone(host.transform_tool_result(result="r", session_id=session_id))
                host.on_session_end(session_id=session_id)
            finally:
                FakeDelegation.flag.reset(token)

        threads = [threading.Thread(target=delegate, args=("c1", True)),
                   threading.Thread(target=delegate, args=("c2", False))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(bridge.session_id, "g1", "the binding stays with the chat")
        self.assertTrue(bridge.busy, "a delegate ending is not the chat's turn ending")
        self.assertEqual(hcom.envs.count((["hermes-bridge", "session"], "tok-chat1")), follows)
        out = host.transform_tool_result(result="r", session_id="g1")
        self.assertIn("for the chat", out, "the chat's own tool results get its mail")

    def test_stop_kill_and_a_failed_stop(self):
        host, ctx, hcom, chat = gateway()
        self.assertIn("Stopped hcom for", asyncio.run(host.command("stop")))
        self.assertEqual(host.bridges, {})
        # Killed or stopped from the CLI: the next sync drops it.
        host, ctx, hcom, chat = gateway()
        hcom.registry.clear()
        host.sync()
        self.assertEqual(host.bridges, {})
        # An unconfirmed stop says so and stops acting, but keeps the record.
        host, ctx, hcom, chat = gateway()
        hcom.fail.add("rm")
        out = asyncio.run(host.command("stop"))
        self.assertIn("could not confirm", out)
        bridge = host.bridges["chat1"]
        self.assertTrue(bridge.inactive())
        hcom.send("ping", to="chat1")
        bridge.notify()
        bridge.tick()
        self.assertEqual(ctx.injected, [])
        hcom.fail.clear()
        self.assertIn("Stopped hcom for", asyncio.run(host.command("stop")))
        self.assertEqual(host.bridges, {})

    def test_stale_cleanup_pauses_then_sync_recovers_without_a_rejoin(self):
        host, ctx, hcom, chat = gateway()
        bridge = host.bridges["chat1"]
        hcom.stale.add("chat1")  # hcom reaped the row: the token is refused
        hcom.send("ping", to="chat1")
        bridge.notify()
        bridge.tick()  # the read comes before the next beat
        self.assertTrue(bridge.paused)
        self.assertIs(host.bridges["chat1"], bridge, "not dropped")
        hcom.recreated.add("chat1")
        host.sync()
        self.assertFalse(bridge.paused)
        bridge.tick()
        self.assertEqual(len(ctx.injected), 1)

    def test_an_outdated_plugin_has_the_gateway_reload_it_once(self):
        host, ctx, hcom, chat = gateway()
        self.assertEqual(hcom.hashes[-1], plugin.SOURCE_HASH)
        calls = []
        host.reload_plugin = lambda: calls.append(1)
        hcom.reload = True
        host.sync()
        host.sync()
        deadline = time.time() + 2
        while not calls and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(calls, [1])
        self.assertIn("chat1", host.bridges, "keeps serving until the reload")

    def test_a_failed_first_sync_is_retried(self):
        hcom = FakeBridgeHcom()
        hcom.fail.add("sync")
        host, ctx, hcom, chat = gateway(hcom=hcom, joined=False)
        hcom.join()
        self.assertFalse(host.sync())
        self.assertEqual(host.bridges, {})
        hcom.fail.clear()
        self.assertTrue(host.sync())
        self.assertIn("chat1", host.bridges)
        syncs = [a for a, _ in hcom.envs if a == ["hermes-bridge", "sync"]]
        self.assertEqual(len(syncs), 2)

    def test_a_reload_hands_over_a_queued_batch(self):
        old, ctx, hcom, chat = gateway()
        old.take_over()
        hcom.send("ping", to="chat1")
        old.bridges["chat1"].tick()
        text = ctx.injected[0][0]
        new, ctx2, _, _ = gateway(hcom=hcom, joined=False)
        new.ctx = ctx
        new.take_over()
        new.sync()
        self.assertTrue(old.stopped)
        new.bridges["chat1"].tick()
        self.assertEqual(len(ctx.injected), 1, "not injected again")
        new.pre_llm_call(session_id="g1", user_message=text)
        self.assertGreater(hcom.cursors.get("chat1", 0), 0)
        self.assertIsNone(new.bridges["chat1"].out)

    def test_a_reload_adopts_from_an_older_plugin_version(self):
        # The replaced copy predates fields this version added.
        old, ctx, hcom = gateway()[:3]
        old.take_over()
        hcom.send("ping", to="chat1")
        old.bridges["chat1"].tick()
        del old.bridges["chat1"].recorded_session
        new = gateway(hcom=hcom, joined=False)[0]
        new.ctx = ctx
        new.take_over()
        self.assertTrue(new.sync())
        self.assertIsNone(new.previous, "the handover completed")
        self.assertIsNotNone(new.bridges["chat1"].out, "the queued batch came along")

    def test_a_reused_name_does_not_adopt_a_different_bridge_identity(self):
        old, ctx, hcom, chat = gateway()
        bridge = old.bridges["chat1"]
        bridge.session_id = "old-session"
        bridge.busy = True
        new = plugin.Bridge(old, dict(hcom.registry["chat1"], token="replacement-token"))
        new.adopt(bridge)
        self.assertIsNone(new.session_id)
        self.assertFalse(new.busy)

    def test_a_reload_during_a_read_is_fenced(self):
        old, ctx, hcom, chat = gateway()
        old.take_over()
        hcom.send("ping", to="chat1")
        holder = {}

        def reload():
            new, _, _, _ = gateway(hcom=hcom, joined=False)
            new.ctx = ctx
            new.take_over()
            new.sync()
            holder["new"] = new

        hcom.on_read = reload
        old.bridges["chat1"].tick()
        self.assertEqual(ctx.injected, [], "the replaced copy acts on nothing")
        holder["new"].bridges["chat1"].tick()
        self.assertEqual(len(ctx.injected), 1)

    def test_a_reload_after_admission_settles_once(self):
        old, ctx, hcom, chat = gateway()
        old.take_over()
        eid = hcom.send("ping", to="chat1")
        old.bridges["chat1"].tick()
        text = ctx.injected[0][0]
        hcom.lose_ack_replies = 1
        old.pre_llm_call(session_id="g1", user_message=text)  # admitted, ack unsettled
        new, _, _, _ = gateway(hcom=hcom, joined=False)
        new.ctx = ctx
        new.take_over()
        new.sync()
        bridge = new.bridges["chat1"]
        self.assertEqual(bridge.out.state, "admitted")
        bridge.tick()
        self.assertIsNone(bridge.out)
        self.assertEqual(hcom.cursors.get("chat1"), eid)
        self.assertEqual(len(ctx.injected), 1)

    def test_reload_waits_for_an_inflight_queue_handoff(self):
        old, ctx, hcom, chat = gateway()
        old.take_over()
        hcom.send("ping", to="chat1")
        entering, release, replaced = threading.Event(), threading.Event(), threading.Event()
        inject = ctx.inject_message

        def blocked_inject(*args, **kwargs):
            entering.set()
            if not release.wait(2):
                raise RuntimeError("test handoff was not released")
            return inject(*args, **kwargs)

        ctx.inject_message = blocked_inject
        delivery = threading.Thread(target=old.bridges["chat1"].tick)
        delivery.start()
        self.assertTrue(entering.wait(1))
        new, _, _, _ = gateway(hcom=hcom, joined=False)
        new.ctx = ctx

        def reload():
            new.take_over()
            new.sync()
            replaced.set()

        replacement = threading.Thread(target=reload)
        replacement.start()
        try:
            self.assertFalse(replaced.wait(0.05), "takeover waits until queue admission settles")
        finally:
            release.set()
            delivery.join(2)
            replacement.join(2)
        self.assertTrue(replaced.is_set())
        new.bridges["chat1"].tick()
        self.assertEqual(len(ctx.injected), 1)

    def test_after_a_gateway_restart_messages_that_already_ran_are_only_acked(self):
        # Idle delivery or a human turn; the whole batch ran, or new mail grew it.
        for via, grown in (("idle", False), ("idle", True), ("turn", True)):
            with self.subTest(via=via, grown=grown):
                home = tempfile.mkdtemp(dir=self.tmp)
                host, ctx, hcom, chat = gateway(home=home)
                first = hcom.send("already acted on", to="chat1")
                text = plugin.format_messages(
                    [{"event_id": first, "from": "luna", "message": "already acted on"}], "chat1")
                state_db(home, sessions=[("g1", CHAT["HERMES_SESSION_KEY"], "irc")],
                         user_rows=[("g1", text)])
                last = hcom.send("new work", to="chat1") if grown else first
                if via == "turn":
                    delivered = host.pre_llm_call(session_id="g1", user_message="human")["context"]
                else:
                    host.bridges["chat1"].tick()
                    delivered = ctx.injected[0][0] if grown else None
                    self.assertEqual(ctx.injected != [], grown)
                if grown:
                    self.assertNotIn("already acted on", delivered)
                    self.assertIn("new work", delivered)
                    if via == "idle":
                        host.pre_llm_call(session_id="g1", user_message=delivered)
                self.assertEqual(hcom.cursors.get("chat1"), last)

    def test_bridges_wait_for_the_gateway_process_only(self):
        host, ctx, hcom, chat = gateway(joined=False)
        hcom.join()
        plugin.GATEWAY_POLL = 0.01
        ctx._manager.has_gateway_message_injector = False
        ctx._manager._cli_ref = FakeCli("s1")  # a CLI process of the profile
        host.start()
        time.sleep(0.05)
        self.assertEqual(host.bridges, {})
        self.assertFalse(any(t.name == "hcom-gateway" for t in threading.enumerate()),
                         "a CLI process stops looking at once")
        ctx._manager._cli_ref = None
        started = []
        host.start_threads = lambda: started.append(True)
        host.start()
        time.sleep(0.03)
        ctx._manager.has_gateway_message_injector = True
        for _ in range(100):
            if started:
                break
            time.sleep(0.01)
        self.assertTrue(started)

    def test_an_unloaded_copy_stops_delivering(self):
        old, ctx, hcom, chat = gateway()
        old.stop()
        hcom.send("ping", to="chat1")
        old.bridges["chat1"].notify()
        old.bridges["chat1"].tick()
        self.assertIsNone(old.pre_llm_call(session_id="g1", user_message="hi"))
        self.assertIsNone(old.transform_tool_result(result="r", session_id="g1"))
        self.assertEqual(ctx.injected, [])
        self.assertEqual(hcom.cursors.get("chat1", 0), 0)
        # A launched host stops on unload too.
        p, ctx, hcom = make(bind=("s1", "cli"))
        p.stop()
        hcom.send("ping")
        p.me.notify()
        self.assertIsNone(p.pre_llm_call(session_id="s1", user_message="go"))
        self.assertIsNone(p.transform_tool_result(result="r", session_id="s1"))


class Status(unittest.TestCase):
    def test_unload_releases_notify_socket_and_status_worker(self):
        host = plugin.Host(FakeCtx(), dict(LAUNCH_ENV), None)
        port = host.notify_port()
        self.assertIsInstance(port, int)
        worker = threading.Thread(target=host.report_statuses, daemon=True)
        worker.start()
        host.stop()
        worker.join(1)
        self.assertFalse(worker.is_alive(), "an idle status worker must wake on unload")
        self.assertEqual(host.notify_socket.fileno(), -1)

    def test_the_status_worker_survives_a_failing_write(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        calls = []

        def rpc(args, participant=None, **_):
            calls.append(args[0])
            if len(calls) == 1:
                raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad")
            return {"ok": True}

        p.rpc = rpc
        worker = threading.Thread(target=p.report_statuses, daemon=True)
        worker.start()
        p.me.status("active", "prompt")
        p.me.status("listening")
        for _ in range(100):
            if p.me.last_status == "listening":
                break
            time.sleep(0.01)
        self.assertEqual(p.me.last_status, "listening")
        p.stop()
        worker.join(1)

    def test_a_turn_end_reports_listening_once(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        p.pre_llm_call(session_id="s1", user_message="go")
        statuses(p)
        p.on_session_end(session_id="s1")
        for _ in range(5):  # the fast reconcile ticks after a turn ends
            p.me.tick()
        listening = [a for a in statuses(p) if a[:3] == ["hermes-status", "--status", "listening"]]
        self.assertEqual(len(listening), 1)
        # A write that did not land is restored by the next tick.
        p.me.status_failed("listening")
        p.me.tick()
        self.assertEqual(len(statuses(p)), 1)

    def test_status_mapping_and_shutdown(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        statuses(p)
        self.assertIsNone(p.pre_tool_call(tool_name="terminal", args={"command": "ls"},
                                           session_id="s1"))
        p.on_human_input_request(kind="clarify", session_id="s1", session_key="s1")
        p.on_human_input_resolved(session_id="s1", outcome="submitted")
        p.on_session_end(session_id="s1")
        got = statuses(p)
        self.assertEqual(got[0], ["hermes-beforetool", "--tool", "terminal", "--input-json",
                                  '{"command": "ls"}'])
        self.assertEqual(got[1], ["hermes-status", "--status", "blocked", "--context",
                                  "approval", "--detail", "clarify"])
        self.assertEqual(got[2][2], "active")
        self.assertEqual(got[3][2], "listening")
        p.on_session_finalize(session_id="s1", reason=None)  # TUI /new or exit
        self.assertEqual(hcom.commands("hermes-stop"), [])
        p.on_session_finalize(session_id="s1", reason="shutdown")
        self.assertEqual(hcom.commands("hermes-stop"),
                         [["hermes-stop", "--name", "kiri", "--reason", "shutdown"]])

    def test_human_input_matched_by_session_key_too(self):
        p, ctx, hcom = make(bind=("s1", "cli"))
        statuses(p)
        p.on_human_input_request(kind="approval", session_id="", session_key="s1")
        p.on_human_input_request(kind="approval", session_id="sub", session_key="sub")
        got = statuses(p)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0][2], "blocked")


class Format(unittest.TestCase):
    def test_matches_pi_format(self):
        one = [{"event_id": 7, "from": "luna", "message": "hi", "intent": "request", "thread": "t"}]
        self.assertEqual(plugin.format_messages(one, "kiri"),
                         "<hcom>[request:t #7] luna -> kiri: hi</hcom>")
        two = [{"event_id": 7, "from": "a", "message": "x"},
               {"event_id": 8, "from": "b", "message": "y", "thread": "t"}]
        self.assertEqual(
            plugin.format_messages(two, "kiri"),
            "<hcom>[2 new messages] | [new message #7] a -> kiri: x | [thread:t #8] b -> kiri: y</hcom>",
        )

    def test_rejects_malformed_batches(self):
        self.assertTrue(plugin._valid_batch([]))
        self.assertFalse(plugin._valid_batch([{"event_id": 2}, {"event_id": 1}]))
        self.assertFalse(plugin._valid_batch([{"event_id": True}]))
        self.assertFalse(plugin._valid_batch(None))


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["HCOM_DIR"] = tmp  # plugin logs land here, not in ~/.hcom
        LAUNCH_ENV["HCOM_DIR"] = tmp
        result = unittest.main(exit=False, verbosity=1).result
    sys.exit(0 if result.wasSuccessful() else 1)
