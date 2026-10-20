"""hcom backend extension for Hermes Agent.

Provides existing-only identity enrollment, per-session notify listeners,
visible user-turn message injection via PluginContext.inject_message,
and lifecycle synchronization.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger("hermes.plugin.hcom")

# Global session registry: session_id -> SessionState
_sessions: Dict[str, SessionState] = {}
_sessions_lock = threading.RLock()
_session_locks: Dict[str, threading.Lock] = {}
_finalized_sessions: Set[str] = set()
_discovery_throttles: Dict[str, float] = {}
_unloaded: bool = False
_current_ctx: Any = None

# Pluggable runner for testing; None means use default subprocess execution.
_runner_override: Optional[Callable[[List[str]], tuple[int, Dict[str, Any]]]] = None


@dataclass
class SessionState:
    session_id: str
    name: Optional[str] = None
    port: Optional[int] = None
    server_sock: Optional[socket.socket] = None
    listener_thread: Optional[threading.Thread] = None
    active: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_discovery_time: float = 0.0
    backoff_until: float = 0.0
    backoff_delay: float = 2.0
    retry_needed: bool = False
    last_opportunistic_drain: float = 0.0


def _get_hcom_bin() -> str:
    return os.environ.get("HCOM_BIN", "hcom")


def _run_hcom(args: List[str]) -> tuple[int, Dict[str, Any]]:
    """Execute an hcom CLI subcommand and parse JSON output."""
    if _runner_override is not None:
        return _runner_override(args)

    hcom_bin = _get_hcom_bin()
    try:
        res = subprocess.run(
            [hcom_bin] + args,
            capture_output=True,
            text=True,
            timeout=10,
        )
        out = res.stdout.strip()
        if not out:
            return res.returncode, {}
        try:
            return res.returncode, json.loads(out)
        except json.JSONDecodeError:
            return res.returncode, {"raw": out}
    except Exception as exc:
        logger.debug("hcom %s failed: %s", args[0] if args else "", exc)
        return -1, {"error": str(exc)}


def _drain_messages(session_state: SessionState, ctx: Any) -> None:
    """Fetch unread messages, inject them into Hermes, and ack only after host acceptance."""
    with session_state.lock:
        if not session_state.active or not session_state.name:
            return
        if time.time() < session_state.backoff_until:
            return
        session_state.last_opportunistic_drain = time.time()

        iterations = 0
        max_iterations = 50
        cap_reached = False
        while session_state.active and iterations < max_iterations:
            iterations += 1
            with _sessions_lock:
                if not session_state.active or session_state.session_id in _finalized_sessions or _unloaded:
                    break

            code, data = _run_hcom([
                "hermes-read",
                "--format",
                "--name",
                session_state.name,
                "--session-id",
                session_state.session_id,
            ])
            if code != 0 or not isinstance(data, dict) or data.get("error"):
                err_msg = str(data.get("error", "")) if isinstance(data, dict) else ""
                logger.debug(
                    "hermes-read format refused or failed for session %s: %s",
                    session_state.session_id,
                    err_msg or data,
                )
                # Stale attribution check: tear down old state and allow discovery/re-enrollment
                if (
                    "no longer attributed" in err_msg
                    or "Unknown Hermes instance" in err_msg
                    or "Not a Hermes instance" in err_msg
                ):
                    logger.warning(
                        "Tearing down stale session %s (%s): %s",
                        session_state.session_id,
                        session_state.name,
                        err_msg,
                    )
                    session_state.active = False
                    session_state.retry_needed = False
                    session_state.backoff_until = 0.0
                    if session_state.server_sock:
                        try:
                            session_state.server_sock.close()
                        except Exception:
                            pass
                    with _sessions_lock:
                        if _sessions.get(session_state.session_id) is session_state:
                            _sessions.pop(session_state.session_id, None)
                        _discovery_throttles.pop(session_state.session_id, None)
                else:
                    # Transient format failure: set bounded backoff and retry_needed so mail is not stranded
                    session_state.retry_needed = True
                    session_state.backoff_until = time.time() + session_state.backoff_delay
                    session_state.backoff_delay = min(session_state.backoff_delay * 2.0, 30.0)
                break

            count = data.get("count")
            up_to = data.get("up_to")
            context = data.get("context", "")

            # Empty mailbox: clean exit and reset retry state
            if count == 0 and up_to == 0 and not context:
                session_state.backoff_until = 0.0
                session_state.backoff_delay = 2.0
                session_state.retry_needed = False
                break

            # Validate count and up_to as positive numeric integers before injecting/ack; fail closed
            if (
                not isinstance(count, int)
                or isinstance(count, bool)
                or count <= 0
                or not isinstance(up_to, int)
                or isinstance(up_to, bool)
                or up_to <= 0
                or not isinstance(context, str)
                or not context
            ):
                logger.warning(
                    "Malformed hermes-read format response for session %s: count=%r, up_to=%r, context_len=%d",
                    session_state.session_id,
                    count,
                    up_to,
                    len(context) if isinstance(context, str) else -1,
                )
                session_state.retry_needed = True
                session_state.backoff_until = time.time() + session_state.backoff_delay
                session_state.backoff_delay = min(session_state.backoff_delay * 2.0, 30.0)
                break

            with _sessions_lock:
                if not session_state.active or session_state.session_id in _finalized_sessions or _unloaded:
                    break

            accepted = False
            try:
                # Injected as role="user" with session_key=session_id (visible user turn)
                accepted = bool(ctx.inject_message(
                    context,
                    role="user",
                    session_key=session_state.session_id,
                ))
            except Exception as exc:
                logger.warning("inject_message raised for session %s: %s", session_state.session_id, exc)
                accepted = False

            with _sessions_lock:
                if not session_state.active or session_state.session_id in _finalized_sessions or _unloaded:
                    break

            if accepted:
                # Monotonic ack only after host acceptance
                ack_code, ack_data = _run_hcom([
                    "hermes-read",
                    "--ack",
                    "--up-to",
                    str(up_to),
                    "--name",
                    session_state.name,
                    "--session-id",
                    session_state.session_id,
                ])
                acked_to = ack_data.get("acked_to") if isinstance(ack_data, dict) else None
                ack_failed = (
                    ack_code != 0
                    or not isinstance(ack_data, dict)
                    or bool(ack_data.get("error"))
                    or not isinstance(acked_to, (int, float))
                    or isinstance(acked_to, bool)
                    or acked_to < up_to
                )
                if ack_failed:
                    logger.warning(
                        "hermes-read ack failed or made no progress for session %s: %s",
                        session_state.session_id,
                        ack_data,
                    )
                    session_state.retry_needed = True
                    session_state.backoff_until = time.time() + session_state.backoff_delay
                    session_state.backoff_delay = min(session_state.backoff_delay * 2.0, 30.0)
                    break
                session_state.backoff_until = 0.0
                session_state.backoff_delay = 2.0
                session_state.retry_needed = False
            else:
                # Denied injection remains unread! Back off so we don't spin.
                session_state.retry_needed = True
                session_state.backoff_until = time.time() + session_state.backoff_delay
                session_state.backoff_delay = min(session_state.backoff_delay * 2.0, 30.0)
                break

            if iterations >= max_iterations:
                cap_reached = True

        if cap_reached and session_state.active:
            logger.debug(
                "Reached maximum drain iterations (%d) for session %s; scheduling continuation",
                max_iterations,
                session_state.session_id,
            )
            session_state.retry_needed = True
            session_state.backoff_until = time.time() + 0.05
            session_state.backoff_delay = max(session_state.backoff_delay, 2.0)


def _listener_loop(session_state: SessionState, ctx: Any) -> None:
    """TCP listener loop handling connect-drop wake notifications for one session."""
    sock = session_state.server_sock
    if not sock:
        return

    while session_state.active:
        try:
            conn, _ = sock.accept()
            conn.close()
        except socket.timeout:
            # Bounded retry after backoff expires without busy polling
            now = time.time()
            if session_state.active and session_state.retry_needed and now >= session_state.backoff_until:
                _drain_messages(session_state, ctx)
            continue
        except OSError as exc:
            if not session_state.active or sock.fileno() == -1 or getattr(exc, "errno", None) == errno.EBADF:
                break
            logger.debug(
                "Transient accept OSError for session %s: %s; retrying with backoff",
                session_state.session_id,
                exc,
            )
            time.sleep(0.05)
            continue
        except Exception as exc:
            if not session_state.active:
                break
            logger.debug(
                "Listener accept error for session %s: %s; retrying with backoff",
                session_state.session_id,
                exc,
            )
            time.sleep(0.05)
            continue

        _drain_messages(session_state, ctx)


def _enroll_session(session_id: str, ctx: Any) -> Optional[SessionState]:
    """Enroll a session using hermes-sessionstart --existing-only."""
    with _sessions_lock:
        if _unloaded or session_id in _finalized_sessions:
            return None
        existing = _sessions.get(session_id)
        if existing and existing.active and existing.name:
            return existing
        if session_id not in _session_locks:
            _session_locks[session_id] = threading.Lock()
        sess_lock = _session_locks[session_id]

    with sess_lock:
        with _sessions_lock:
            if _unloaded or session_id in _finalized_sessions:
                return None
            existing = _sessions.get(session_id)
            if existing and existing.active and existing.name:
                return existing

        # Bind ephemeral TCP socket for wake notifications
        server_sock = None
        try:
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind(("127.0.0.1", 0))
            server_sock.listen(128)
            server_sock.settimeout(1.0)
            port = server_sock.getsockname()[1]
        except Exception as exc:
            logger.warning("Failed to bind notify socket for session %s: %s", session_id, exc)
            if server_sock:
                try:
                    server_sock.close()
                except Exception:
                    pass
            return None

        # Check if finalized or unloaded while binding socket
        with _sessions_lock:
            if _unloaded or session_id in _finalized_sessions:
                server_sock.close()
                return None

        code, data = _run_hcom([
            "hermes-sessionstart",
            "--existing-only",
            "--session-id",
            session_id,
            "--notify-port",
            str(port),
        ])

        name = data.get("name") if (code == 0 and isinstance(data, dict)) else None
        if not name:
            # Not enrolled (ordinary session without prior hcom start, or serve session): stay silent
            try:
                server_sock.close()
            except Exception:
                pass
            return None

        with _sessions_lock:
            if _unloaded or session_id in _finalized_sessions:
                # Finalized or unloaded while sessionstart was in flight: clean up immediately and do not publish
                try:
                    server_sock.close()
                except Exception:
                    pass
                _run_hcom([
                    "hermes-stop",
                    "--soft",
                    "--name",
                    name,
                    "--session-id",
                    session_id,
                    "--reason",
                    "session finalize or unload during enroll",
                ])
                return None

            state = SessionState(
                session_id=session_id,
                name=name,
                port=port,
                server_sock=server_sock,
                active=True,
                last_opportunistic_drain=time.time(),
            )
            thread = threading.Thread(
                target=_listener_loop,
                args=(state, ctx),
                daemon=True,
                name=f"hcom-notify-{session_id}",
            )
            state.listener_thread = thread
            thread.start()
            _sessions[session_id] = state

        # Drain any messages already waiting
        _drain_messages(state, ctx)
        return state


def _on_session_start(session_id: str, **kwargs: Any) -> None:
    """Session start hook: attempt existing-only enrollment."""
    global _current_ctx
    if not session_id or not _current_ctx:
        return
    with _sessions_lock:
        if _unloaded:
            return
        _finalized_sessions.discard(session_id)
    _enroll_session(session_id, _current_ctx)


def _is_hcom_start_call(tool_name: str, kwargs: Dict[str, Any]) -> bool:
    """Detect if a tool call explicitly invokes hcom start."""
    if "hcom start" in tool_name.lower():
        return True

    def check_val(v: Any) -> bool:
        if isinstance(v, str):
            return "hcom start" in v
        elif isinstance(v, dict):
            return any(check_val(item) for item in v.values())
        elif isinstance(v, (list, tuple)):
            return any(check_val(item) for item in v)
        return False

    return check_val(kwargs)


def _post_tool_call(session_id: str, tool_name: str = "", **kwargs: Any) -> None:
    """Post tool call hook: discover mid-session hcom start."""
    global _current_ctx
    if not session_id or not _current_ctx:
        return

    now = time.time()
    force = _is_hcom_start_call(tool_name, kwargs)
    drain_target = None

    with _sessions_lock:
        if session_id in _finalized_sessions or _unloaded:
            return
        state = _sessions.get(session_id)
        if state and state.active and state.name:
            if state.retry_needed and now >= state.backoff_until:
                drain_target = state
                state.last_opportunistic_drain = now
            elif (now - state.last_opportunistic_drain) >= 3.0:
                state.last_opportunistic_drain = now
                drain_target = state
            if not drain_target:
                return
        if not drain_target:
            last_time = _discovery_throttles.get(session_id, 0.0)
            if not force and (now - last_time < 1.0):
                return
            _discovery_throttles[session_id] = now

    if drain_target:
        _drain_messages(drain_target, _current_ctx)
        return

    # Cheap read-only resolve check before invoking full sessionstart and TCP socket
    code, data = _run_hcom(["hermes-read", "--session-id", session_id])
    if code != 0 or not isinstance(data, dict):
        return
    name = data.get("name")
    if not name:
        # Still unenrolled; negative cache throttles further attempts
        return

    _enroll_session(session_id, _current_ctx)


def _on_session_reset(session_id: str, reason: str = "", **kwargs: Any) -> None:
    """Session reset hook: clear transient backoff without stopping the session."""
    with _sessions_lock:
        _finalized_sessions.discard(session_id)
        state = _sessions.get(session_id)
        if state:
            state.backoff_until = 0.0
            state.backoff_delay = 2.0
            state.retry_needed = False


def _on_session_finalize(session_id: str, **kwargs: Any) -> None:
    """Session finalize hook: soft-stop the matching session and clean up its resources."""
    with _sessions_lock:
        _finalized_sessions.add(session_id)
        _discovery_throttles.pop(session_id, None)
        if session_id not in _session_locks:
            _session_locks[session_id] = threading.Lock()
        sess_lock = _session_locks[session_id]

    with sess_lock:
        with _sessions_lock:
            state = _sessions.pop(session_id, None)
            _session_locks.pop(session_id, None)

        if state:
            state.active = False
            state.retry_needed = False
            state.backoff_until = 0.0
            if state.name:
                _run_hcom([
                    "hermes-stop",
                    "--soft",
                    "--name",
                    state.name,
                    "--session-id",
                    session_id,
                    "--reason",
                    "session finalize",
                ])
            if state.server_sock:
                try:
                    state.server_sock.close()
                except Exception:
                    pass
            if state.listener_thread and state.listener_thread.is_alive():
                state.listener_thread.join(timeout=0.5)


def _on_unload() -> None:
    """Plugin unload hook: soft-stop active identities, close listeners, and clear state."""
    global _unloaded
    with _sessions_lock:
        _unloaded = True
        active_sessions = list(_sessions.values())
        _sessions.clear()
        _session_locks.clear()
        _discovery_throttles.clear()
        _finalized_sessions.clear()

    for state in active_sessions:
        state.active = False
        state.retry_needed = False
        state.backoff_until = 0.0
        if state.name:
            _run_hcom([
                "hermes-stop",
                "--soft",
                "--name",
                state.name,
                "--session-id",
                state.session_id,
                "--reason",
                "plugin unload",
            ])
        if state.server_sock:
            try:
                state.server_sock.close()
            except Exception:
                pass
        if state.listener_thread and state.listener_thread.is_alive():
            state.listener_thread.join(timeout=0.5)


def register(ctx: Any) -> None:
    """Hermes plugin entry point."""
    global _current_ctx, _unloaded
    _current_ctx = ctx
    with _sessions_lock:
        _unloaded = False

    ctx.register_hook("on_session_start", _on_session_start)
    ctx.register_hook("on_session_finalize", _on_session_finalize)
    ctx.register_hook("on_session_reset", _on_session_reset)
    ctx.register_hook("post_tool_call", _post_tool_call)

    if hasattr(ctx, "on_unload"):
        ctx.on_unload(_on_unload)
