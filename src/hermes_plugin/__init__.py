"""hcom integration plugin for Hermes Agent (validated against 0.21.3).

Bridges a running Hermes session into an hcom mesh: binds a session identity,
drains pending hcom messages into the model context at turn assembly
(pre_llm_call), reports status/tool activity back to the hcom event log, and
keeps a localhost TCP port registered so hcom can prove the plugin is loaded
(launch readiness) and poke the session's wake endpoints.

Delivery semantics (honest ones): messages are retrieved only when the plugin
runs — i.e. at turn assembly. The notify poke is a readiness/heartbeat signal;
it cannot wake a session that sits idle at its prompt. Agents that must keep
receiving while idle wait with `hcom listen` (plain shell), which the
bootstrap text teaches.

The hcom side is a set of argv-based RPC subcommands (`hcom hermes-…`),
mirroring the Pi/Omp plugin contract:

    hcom hermes-sessionstart --session-id ID [--cwd DIR] [--notify-port N]
        -> {"name": ..., "session_id": ..., "bootstrap": ...}
    hcom hermes-status --name N --status active|listening [--context C] [--detail D]
    hcom hermes-read --name N [--format | --check | --ack [--up-to ID]]

    Every hermes-read mode prints exactly one JSON document on stdout:
      (default) -> [message, ...]
      --check   -> {"has_unread": bool}
      --format  -> {"context": "<hcom>…</hcom>", "up_to": int, "count": int}
                   `up_to` is the cursor of the DELIVERED batch (the batch is
                   capped, so it can be below the newest unread event) and is
                   the only value the caller may acknowledge.
      --ack     -> {"acked": n} | {"acked_to": id}
    Errors are reported as {"error": "..."} with exit code 0 so the plugin
    can react to setup failures without Hermes seeing a failed subprocess.

    hcom hermes-beforetool --name N --tool T [--input-json JSON]
    hcom hermes-stop --name N [--reason R] [--soft]

All IPC is one-shot subprocess calls against the hcom CLI on PATH (or
$HCOM_BIN), so nothing here imports Rust code or holds the hcom DB open.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import subprocess
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_STATE: Dict[str, Any] = {"name": None, "session_id": None, "bootstrap": None}
# The notify listener intentionally outlives one session binding: on session
# reset we drop the binding but keep the socket so a rebind in the same
# process reuses the port instead of leaking a listener per session.
_NOTIFY_LOCK = threading.Lock()
_NOTIFY_SERVER: Optional[socket.socket] = None
_NOTIFY_PORT: Optional[int] = None
_BOUND = threading.Event()

_LOCK = threading.Lock()


def _hcom_bin() -> str:
    return os.environ.get("HCOM_BIN", "hcom")


def _log(level: str, event: str, **extra: Any) -> None:
    # Standard logging: hermes configures the root logger and captures
    # plugin records (bundled plugins do the same). No hermes import needed.
    try:
        logger.log(
            logging.getLevelName(level.upper()) if level else logging.INFO,
            "hcom.%s %s",
            event,
            " ".join(f"{k}={v}" for k, v in extra.items()),
        )
    except Exception:
        pass


def _hcom(args: List[str], timeout: float = 10.0) -> Optional[Dict[str, Any]]:
    """Run one hcom RPC subcommand; return parsed JSON stdout or None.

    All RPC outputs are JSON documents (see module docstring); a non-JSON
    stdout is a contract violation and is treated as a failure, never as
    usable data.
    """
    try:
        proc = subprocess.run(
            [_hcom_bin(), *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        _log("warning", "rpc_failed", args=args[:2], error=str(exc))
        return None
    if proc.returncode != 0:
        _log("warning", "rpc_error", args=args[:2], code=proc.returncode,
             stderr=(proc.stderr or "")[:300])
        return None
    out = (proc.stdout or "").strip()
    if not out:
        return {}
    try:
        parsed = json.loads(out)
    except json.JSONDecodeError:
        _log("warning", "rpc_parse_failed", args=args[:2], raw=out[:200])
        return None
    return parsed if isinstance(parsed, dict) else {"value": parsed}


def _start_notify_listener() -> Optional[int]:
    """Bind a localhost TCP port; hcom connect-drops it as a wake signal.

    The poke carries no data and does NOT deliver messages: delivery always
    goes through `hermes-read` at turn assembly. The port's real jobs are
    proving the plugin is alive (hcom launch readiness keys off the
    registration) and participating in hcom's wake fan-out.
    """
    global _NOTIFY_SERVER, _NOTIFY_PORT
    with _NOTIFY_LOCK:
        if _NOTIFY_SERVER is not None and _NOTIFY_PORT:
            return _NOTIFY_PORT
        try:
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("127.0.0.1", 0))
            srv.listen(8)
            port = srv.getsockname()[1]
        except OSError as exc:
            _log("warning", "notify_bind_failed", error=str(exc))
            return None

        def _serve() -> None:
            srv.settimeout(30.0)
            while True:
                try:
                    conn, _ = srv.accept()
                    with conn:
                        conn.recv(64)
                except socket.timeout:
                    continue
                except OSError:
                    return

        threading.Thread(target=_serve, daemon=True, name="hcom-notify").start()
        _NOTIFY_SERVER = srv
        _NOTIFY_PORT = port
        return port


def _ensure_bound(session_id: str) -> Optional[str]:
    """Bind this session to an hcom instance name (idempotent per session)."""
    with _LOCK:
        if _STATE.get("session_id") == session_id and _STATE.get("name"):
            return _STATE["name"]
        if not os.environ.get("HCOM_LAUNCHED"):
            # Not launched by hcom: joining would hijack an unrelated instance.
            return None
        port = _start_notify_listener()
        args = ["hermes-sessionstart", "--session-id", session_id,
                "--cwd", os.getcwd()]
        if port:
            args += ["--notify-port", str(port)]
        result = _hcom(args, timeout=20.0)
        if not result or result.get("error") or not result.get("name"):
            _log("warning", "bind_failed",
                 detail=str(result)[:200] if result else "none")
            return None
        _STATE["name"] = result["name"]
        _STATE["session_id"] = session_id
        _STATE["bootstrap"] = result.get("bootstrap")
        _BOUND.set()
        _log("info", "bound", name=result["name"], session_id=session_id,
             port=port)
        return result["name"]


def _clear_state() -> None:
    """Drop the session binding (name, cursor-free state, bootstrap flag)."""
    with _LOCK:
        _STATE["name"] = None
        _STATE["session_id"] = None
        _STATE["bootstrap"] = None
        _BOUND.clear()


def _set_status(name: str, status: str, context: str) -> None:
    _hcom(["hermes-status", "--name", name, "--status", status,
           "--context", context], timeout=5.0)


def _drain_messages(name: str) -> Optional[str]:
    """Fetch + ack unread messages; return formatted model context or None.

    Only the DELIVERED batch is acknowledged, via the explicit `up_to`
    cursor `--format` reports — the batch is capped, so acknowledging
    "everything unread" here would silently drop the overflow. The ack
    happens before returning because the plugin has no post-injection
    callback; a failed ack is logged and the text is still returned so a
    broken RPC degrades to a possible redelivery, never to message loss.
    """
    fetched = _hcom(["hermes-read", "--name", name, "--format"])
    if not isinstance(fetched, dict) or fetched.get("error"):
        _log("warning", "read_failed", name=name,
             detail=str(fetched)[:200] if fetched else "none")
        return None
    text = fetched.get("context")
    up_to = fetched.get("up_to") or 0
    if not text or not up_to:
        return None
    acked = _hcom(["hermes-read", "--name", name, "--ack",
                   "--up-to", str(up_to)], timeout=5.0)
    if not isinstance(acked, dict) or acked.get("error"):
        _log("warning", "ack_failed", name=name, up_to=up_to)
    return text if isinstance(text, str) else None


# ── Hermes hook callbacks (all names in hermes 0.21 VALID_HOOKS) ─────────

def on_session_start(**kwargs: Any) -> None:
    """Bind the hcom instance when the first turn of a session is created.

    Deliberately does NOT touch the bootstrap here: the primer is handed to
    the model exactly once, by pre_llm_call, alongside the first delivery.
    """
    sid = kwargs.get("session_id") or ""
    if sid:
        _ensure_bound(sid)


def pre_llm_call(**kwargs: Any) -> Optional[Dict[str, str]]:
    """Drain hcom messages into the turn context at assembly time."""
    name = _STATE.get("name")
    if not name:
        sid = kwargs.get("session_id") or ""
        if sid:
            name = _ensure_bound(sid)
        if not name:
            return None
    # One-shot bootstrap plus anything pending — both ride the same
    # one-shot injection channel so a bound session never waits a turn
    # just because the primer hadn't been spent yet.
    parts: List[str] = []
    boot = _STATE.get("bootstrap")
    if boot:
        _STATE["bootstrap"] = None
        parts.append(boot)
    text = _drain_messages(name)
    if text:
        parts.append(text)
        _set_status(name, "active", "deliver")
    if parts:
        return {"context": "\n\n".join(parts)}
    return None


def post_llm_call(**kwargs: Any) -> None:
    """Turn finished — report listening again (egress bookkeeping)."""
    name = _STATE.get("name")
    if name:
        _set_status(name, "listening", "turn_end")


def post_tool_call(**kwargs: Any) -> None:
    """Report tool activity into the hcom event stream (egress)."""
    name = _STATE.get("name")
    if not name:
        return
    tool = kwargs.get("tool_name") or ""
    if not tool:
        return
    input_payload = {}
    args = kwargs.get("args")
    if isinstance(args, dict):
        input_payload = args
    _hcom(["hermes-beforetool", "--name", name, "--tool", tool,
           "--input-json", json.dumps(input_payload)[:4000]], timeout=5.0)


def on_session_end(**kwargs: Any) -> None:
    """Finalize the hcom instance when the session ends."""
    name = _STATE.get("name")
    _clear_state()
    if name:
        _hcom(["hermes-stop", "--name", name,
               "--reason", "session_end"], timeout=5.0)


def on_session_reset(**kwargs: Any) -> None:
    """Session reset (/new): soft-finalize; this process may rebind."""
    name = _STATE.get("name")
    _clear_state()
    if name:
        _hcom(["hermes-stop", "--name", name, "--reason", "reset",
               "--soft"], timeout=5.0)


def register(ctx) -> None:
    ctx.register_hook("on_session_start", on_session_start)
    ctx.register_hook("pre_llm_call", pre_llm_call)
    ctx.register_hook("post_llm_call", post_llm_call)
    ctx.register_hook("post_tool_call", post_tool_call)
    ctx.register_hook("on_session_end", on_session_end)
    ctx.register_hook("on_session_reset", on_session_reset)
