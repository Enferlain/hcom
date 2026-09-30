"""Stdlib-only fake-host tests for hcom Hermes backend extension.

Tests cover:
- Two concurrent interleaved sessions with independent notify listeners and routing.
- Denied injection (ctx.inject_message returns False) leaves mail unread (no ack).
- Ordinary unenrolled sessions (name is null) stay silent with no listener.
- Mid-session hcom start discovery via post_tool_call with throttle bypass.
- Unenrolled negative throttle and prevention of socket creation.
- Malformed hermes-read format validation fails closed.
- Transient accept OSError recovery in listener loop.
- Finalize vs enroll/drain race synchronization.
- on_unload soft-stops all active identities.
- on_session_reset never unconditionally stops.
- on_session_finalize soft-stops only the matching session id.
- All backend writes and formatted reads carry --session-id.
"""

from __future__ import annotations

import errno
import socket
import threading
import time
import unittest
from typing import Any, Dict, List, Optional

from hermes_backend.hcom import (
    SessionState,
    _discovery_throttles,
    _drain_messages,
    _enroll_session,
    _finalized_sessions,
    _listener_loop,
    _on_session_finalize,
    _on_session_reset,
    _on_session_start,
    _on_unload,
    _post_tool_call,
    _runner_override,
    _session_locks,
    _sessions,
    _sessions_lock,
    register,
)
import hermes_backend.hcom as hcom_ext


class FakePluginContext:
    """Mock Hermes PluginContext using standard library only."""

    def __init__(self) -> None:
        self.hooks: Dict[str, Any] = {}
        self.unload_callbacks: List[Any] = []
        self.injected_messages: List[Dict[str, Any]] = []
        # Controls whether inject_message accepts (True) or denies (False).
        # Can be set as a bool or a dict mapping session_key -> bool.
        self.injection_policy: Any = True

    def register_hook(self, hook_name: str, callback: Any) -> None:
        self.hooks[hook_name] = callback

    def on_unload(self, callback: Any) -> None:
        self.unload_callbacks.append(callback)

    def inject_message(
        self, content: str, role: str = "user", *, session_key: Optional[str] = None
    ) -> bool:
        record = {
            "content": content,
            "role": role,
            "session_key": session_key,
        }
        self.injected_messages.append(record)

        if isinstance(self.injection_policy, dict):
            return self.injection_policy.get(session_key, True)
        return bool(self.injection_policy)


class TestHermesBackendExtension(unittest.TestCase):

    def setUp(self) -> None:
        # Reset extension state between tests
        with _sessions_lock:
            for s in list(_sessions.values()):
                s.active = False
                if s.server_sock:
                    try:
                        s.server_sock.close()
                    except Exception:
                        pass
                if s.listener_thread and s.listener_thread.is_alive():
                    s.listener_thread.join(timeout=0.2)
            _sessions.clear()
            _session_locks.clear()
            _finalized_sessions.clear()
            _discovery_throttles.clear()

        hcom_ext._unloaded = False
        hcom_ext._runner_override = None
        self.ctx = FakePluginContext()
        register(self.ctx)
        self.command_log: List[List[str]] = []

    def tearDown(self) -> None:
        _on_unload()
        hcom_ext._runner_override = None

    def _mock_runner(self, responses: Dict[str, Any]):
        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0] if args else ""
            if subcmd in responses:
                resp = responses[subcmd]
                if callable(resp):
                    return resp(args)
                return 0, resp
            return 0, {}

        hcom_ext._runner_override = runner

    def test_ordinary_session_no_auto_enrollment(self) -> None:
        """Ordinary sessions without existing hcom start receive null name and stay silent."""
        self._mock_runner({
            "hermes-sessionstart": {"name": None, "session_id": "ses-ordinary"},
        })

        self.ctx.hooks["on_session_start"]("ses-ordinary")

        with _sessions_lock:
            self.assertNotIn("ses-ordinary", _sessions)

        self.assertEqual(len(self.ctx.injected_messages), 0)
        # Check command log
        start_cmd = next(cmd for cmd in self.command_log if cmd[0] == "hermes-sessionstart")
        self.assertIn("--existing-only", start_cmd)
        self.assertIn("--session-id", start_cmd)
        self.assertIn("ses-ordinary", start_cmd)

    def test_two_interleaved_sessions_and_notify_routing(self) -> None:
        """Two concurrent interleaved sessions have independent ports, mail, and monotonic acks."""
        mailboxes: Dict[str, List[Dict[str, Any]]] = {
            "vega": [{"event_id": 101, "from": "alice", "text": "hello vega"}],
            "sol": [{"event_id": 201, "from": "bob", "text": "hello sol"}],
        }

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                sid_idx = args.index("--session-id") + 1
                sid = args[sid_idx]
                name = "vega" if sid == "ses-1" else "sol"
                return 0, {"name": name, "session_id": sid, "bootstrap": "info"}
            elif subcmd == "hermes-read":
                if "--format" in args:
                    name_idx = args.index("--name") + 1
                    name = args[name_idx]
                    sid_idx = args.index("--session-id") + 1
                    sid = args[sid_idx]
                    msgs = mailboxes.get(name, [])
                    if msgs:
                        return 0, {
                            "context": f"[{name}] {msgs[-1]['text']}",
                            "up_to": msgs[-1]["event_id"],
                            "count": len(msgs),
                        }
                    return 0, {"context": "", "up_to": 0, "count": 0}
                elif "--ack" in args:
                    name_idx = args.index("--name") + 1
                    name = args[name_idx]
                    mailboxes[name] = []
                    up_to_val = int(args[args.index("--up-to") + 1]) if "--up-to" in args else 100
                    return 0, {"acked": 1, "acked_to": up_to_val}
            return 0, {}

        hcom_ext._runner_override = runner

        # 1. Start session 1
        self.ctx.hooks["on_session_start"]("ses-1")
        with _sessions_lock:
            state1 = _sessions["ses-1"]
            self.assertEqual(state1.name, "vega")
            self.assertIsNotNone(state1.port)
            port1 = state1.port

        # 2. Start session 2
        self.ctx.hooks["on_session_start"]("ses-2")
        with _sessions_lock:
            state2 = _sessions["ses-2"]
            self.assertEqual(state2.name, "sol")
            self.assertIsNotNone(state2.port)
            port2 = state2.port

        # Ports must be distinct
        self.assertNotEqual(port1, port2)

        # Session 1 initial drain injected vega's message
        self.assertTrue(
            any(
                m["session_key"] == "ses-1" and "hello vega" in m["content"]
                for m in self.ctx.injected_messages
            )
        )
        # Session 2 initial drain injected sol's message
        self.assertTrue(
            any(
                m["session_key"] == "ses-2" and "hello sol" in m["content"]
                for m in self.ctx.injected_messages
            )
        )

        # 3. Simulate a connect-drop notify ping to Session 1's port
        mailboxes["vega"] = [{"event_id": 102, "from": "alice", "text": "ping for vega"}]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("127.0.0.1", port1))

        # Give listener thread a moment to wake and drain
        time.sleep(0.1)

        self.assertTrue(
            any(
                m["session_key"] == "ses-1" and "ping for vega" in m["content"]
                for m in self.ctx.injected_messages
            )
        )
        # Ensure sol did NOT receive vega's ping
        self.assertFalse(
            any(
                m["session_key"] == "ses-2" and "ping for vega" in m["content"]
                for m in self.ctx.injected_messages
            )
        )

        # Verify all ack commands carried the correct session_id and at least two acks occurred
        acks = [cmd for cmd in self.command_log if cmd[0] == "hermes-read" and "--ack" in cmd]
        self.assertGreaterEqual(len(acks), 2, "Interleaved sessions test must execute at least two acks")
        for ack in acks:
            self.assertIn("--session-id", ack)
            sid_idx = ack.index("--session-id") + 1
            name_idx = ack.index("--name") + 1
            if ack[name_idx] == "vega":
                self.assertEqual(ack[sid_idx], "ses-1")
            elif ack[name_idx] == "sol":
                self.assertEqual(ack[sid_idx], "ses-2")

    def test_denied_injection_leaves_mail_unread(self) -> None:
        """When ctx.inject_message denies (returns False), no ack is sent and mail remains unread."""
        self.ctx.injection_policy = False  # Deny all injections

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                return 0, {"name": "vega", "session_id": "ses-denied"}
            elif subcmd == "hermes-read" and "--format" in args:
                return 0, {
                    "context": "[user] urgent prompt",
                    "up_to": 55,
                    "count": 1,
                }
            elif subcmd == "hermes-read" and "--ack" in args:
                return 0, {"acked": 1, "acked_to": 55}
            return 0, {}

        hcom_ext._runner_override = runner

        self.ctx.hooks["on_session_start"]("ses-denied")

        # Attempted injection once
        self.assertEqual(len(self.ctx.injected_messages), 1)

        # Crucial check: NO hermes-read --ack command was ever executed!
        ack_calls = [cmd for cmd in self.command_log if cmd[0] == "hermes-read" and "--ack" in cmd]
        self.assertEqual(len(ack_calls), 0, "Denied injection must not be acknowledged!")

        # Verify backoff was set
        with _sessions_lock:
            state = _sessions["ses-denied"]
            self.assertGreater(state.backoff_until, time.time())

    def test_post_tool_call_discovers_mid_session_hcom_start(self) -> None:
        """A session that starts unenrolled discovers its identity when hcom start is run mid-session."""
        enrolled = False
        mail_pending = True

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal mail_pending
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-read" and "--name" not in args:
                # Read-only resolve mode
                if enrolled:
                    return 0, {"name": "luna", "session_id": "ses-mid"}
                return 0, {"name": None, "session_id": "ses-mid"}
            elif subcmd == "hermes-sessionstart":
                if enrolled:
                    return 0, {"name": "luna", "session_id": "ses-mid"}
                return 0, {"name": None, "session_id": "ses-mid"}
            elif subcmd == "hermes-read" and "--format" in args:
                if mail_pending:
                    return 0, {"context": "discovered mail", "up_to": 12, "count": 1}
                return 0, {"context": "", "up_to": 0, "count": 0}
            elif subcmd == "hermes-read" and "--ack" in args:
                mail_pending = False
                up_to_val = int(args[args.index("--up-to") + 1]) if "--up-to" in args else 12
                return 0, {"acked": 1, "acked_to": up_to_val}
            return 0, {}

        hcom_ext._runner_override = runner

        # 1. Start initially unenrolled
        self.ctx.hooks["on_session_start"]("ses-mid")
        with _sessions_lock:
            self.assertNotIn("ses-mid", _sessions)

        # 2. Mid-session tool call runs: hcom start is performed (explicit command bypasses throttle)
        enrolled = True
        self.ctx.hooks["post_tool_call"]("ses-mid", tool_name="bash", command="hcom start luna")

        # 3. Session must now be discovered, enrolled, and mail drained
        with _sessions_lock:
            self.assertIn("ses-mid", _sessions)
            self.assertEqual(_sessions["ses-mid"].name, "luna")

        self.assertTrue(any("discovered mail" in m["content"] for m in self.ctx.injected_messages))

    def test_post_tool_call_unenrolled_throttles_and_does_not_spawn_sockets(self) -> None:
        """Unenrolled sessions use cheap resolve, negative-cache throttle, and never spawn sockets."""
        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                return 0, {"name": None, "session_id": "ses-un"}
            elif subcmd == "hermes-read" and "--name" not in args:
                return 0, {"name": None, "session_id": "ses-un"}
            return 0, {}

        hcom_ext._runner_override = runner

        # Initial start
        self.ctx.hooks["on_session_start"]("ses-un")
        sessionstart_count_initial = len([c for c in self.command_log if c[0] == "hermes-sessionstart"])
        self.assertEqual(sessionstart_count_initial, 1)

        # Rapidly fire 5 ordinary tool calls
        for _ in range(5):
            self.ctx.hooks["post_tool_call"]("ses-un", tool_name="read_file", path="/foo/bar")

        # hermes-sessionstart was NOT called by post_tool_call (no sockets created!)
        sessionstart_count_after = len([c for c in self.command_log if c[0] == "hermes-sessionstart"])
        self.assertEqual(sessionstart_count_after, 1)

        # hermes-read in resolve mode was called exactly once due to 1.0s throttle
        resolve_calls = [c for c in self.command_log if c[0] == "hermes-read" and "--session-id" in c and "--name" not in c]
        self.assertEqual(len(resolve_calls), 1)

        # An explicit hcom start command bypasses the throttle immediately
        self.ctx.hooks["post_tool_call"]("ses-un", tool_name="bash", command="hcom start vega")
        resolve_calls_after = [c for c in self.command_log if c[0] == "hermes-read" and "--session-id" in c and "--name" not in c]
        self.assertEqual(len(resolve_calls_after), 2)

    def test_drain_messages_format_validation_fails_closed(self) -> None:
        """Malformed count/up_to/context in hermes-read --format must fail closed and back off."""
        current_format_resp: Dict[str, Any] = {}

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                return 0, {"name": "vega", "session_id": "ses-val"}
            elif subcmd == "hermes-read" and "--format" in args:
                return 0, current_format_resp
            elif subcmd == "hermes-read" and "--ack" in args:
                return 0, {"acked": 1, "acked_to": 10}
            return 0, {}

        hcom_ext._runner_override = runner
        self.ctx.hooks["on_session_start"]("ses-val")
        with _sessions_lock:
            state = _sessions["ses-val"]

        malformed_cases = [
            {"context": "hello", "count": "1", "up_to": 10},       # count is str
            {"context": "hello", "count": True, "up_to": 10},      # count is bool
            {"context": "hello", "count": -1, "up_to": 10},        # count negative
            {"context": "hello", "count": 1, "up_to": "10"},       # up_to is str
            {"context": "hello", "count": 1, "up_to": True},       # up_to is bool
            {"context": "hello", "count": 1, "up_to": 0},          # up_to is 0 with count 1
            {"context": "", "count": 1, "up_to": 10},              # empty context
            {"context": 12345, "count": 1, "up_to": 10},           # context not str
        ]

        for case in malformed_cases:
            self.ctx.injected_messages.clear()
            self.command_log.clear()
            state.backoff_until = 0.0
            current_format_resp = case

            _drain_messages(state, self.ctx)

            # Never injected and never acked
            self.assertEqual(len(self.ctx.injected_messages), 0, f"Must not inject for case {case}")
            ack_calls = [c for c in self.command_log if c[0] == "hermes-read" and "--ack" in c]
            self.assertEqual(len(ack_calls), 0, f"Must not ack for case {case}")
            # Backoff was set
            self.assertGreater(state.backoff_until, time.time(), f"Backoff must be set for case {case}")

    def test_listener_loop_transient_oserror_recovery(self) -> None:
        """Transient accept OSError must not terminate active listener loop."""
        accepted_connections = 0

        class MockSocket:
            def __init__(self):
                self._attempts = 0
                self._closed = False

            def fileno(self) -> int:
                return -1 if self._closed else 5

            def accept(self):
                self._attempts += 1
                if self._attempts == 1:
                    # First attempt raises transient EMFILE
                    raise OSError(errno.EMFILE, "Too many open files")
                elif self._attempts == 2:
                    nonlocal accepted_connections
                    accepted_connections += 1
                    class MockConn:
                        def close(self):
                            pass
                    return MockConn(), ("127.0.0.1", 12345)
                else:
                    # Subsequent attempts sleep until closed
                    time.sleep(0.1)
                    if self._closed:
                        raise OSError(errno.EBADF, "Bad file descriptor")
                    raise socket.timeout()

            def close(self):
                self._closed = True

        mock_sock = MockSocket()
        state = SessionState(session_id="ses-transient", name="vega", active=True, server_sock=mock_sock)

        drained = False
        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal drained
            subcmd = args[0]
            if subcmd == "hermes-read" and "--format" in args:
                drained = True
                return 0, {"context": "", "up_to": 0, "count": 0}
            return 0, {}

        hcom_ext._runner_override = runner

        t = threading.Thread(target=_listener_loop, args=(state, self.ctx))
        t.start()

        # Give listener time to hit the transient error, back off 50ms, and accept the second connection
        time.sleep(0.2)

        state.active = False
        mock_sock.close()
        t.join(timeout=0.5)

        self.assertEqual(accepted_connections, 1)
        self.assertTrue(drained, "Listener should have drained after accepting second connection")

    def test_finalize_vs_enroll_race_synchronization(self) -> None:
        """Concurrently enrolling and finalizing a session leaves no open sockets, no published state, and no leaks."""
        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                time.sleep(0.02)  # Delay to ensure finalize executes during sessionstart
                return 0, {"name": "vega", "session_id": "ses-race"}
            elif subcmd == "hermes-stop":
                return 0, {"ok": True}
            elif subcmd == "hermes-read":
                return 0, {"context": "race mail", "up_to": 10, "count": 1}
            return 0, {}

        hcom_ext._runner_override = runner

        t_enroll = threading.Thread(target=_enroll_session, args=("ses-race", self.ctx))
        t_finalize = threading.Thread(target=_on_session_finalize, args=("ses-race",))

        t_enroll.start()
        time.sleep(0.005)
        t_finalize.start()

        t_enroll.join()
        t_finalize.join()

        # Session must NOT remain in _sessions
        with _sessions_lock:
            self.assertNotIn("ses-race", _sessions)

        # No mail should have been accepted into the host after finalize
        self.assertEqual(len(self.ctx.injected_messages), 0)

    def test_on_unload_soft_stops_active_identities(self) -> None:
        """on_unload soft-stops all active identities and cleans up listeners."""
        self._mock_runner({
            "hermes-sessionstart": lambda args: (
                0,
                {
                    "name": "vega" if "ses-u1" in args else "sol",
                    "session_id": "ses-u1" if "ses-u1" in args else "ses-u2",
                },
            ),
            "hermes-read": {"context": "", "up_to": 0, "count": 0},
            "hermes-stop": {"ok": True},
        })

        self.ctx.hooks["on_session_start"]("ses-u1")
        self.ctx.hooks["on_session_start"]("ses-u2")

        with _sessions_lock:
            self.assertIn("ses-u1", _sessions)
            self.assertIn("ses-u2", _sessions)

        # Call on_unload
        _on_unload()

        # _sessions must be cleared
        with _sessions_lock:
            self.assertEqual(len(_sessions), 0)

        # Both sessions must have received hermes-stop --soft
        stops = [cmd for cmd in self.command_log if cmd[0] == "hermes-stop"]
        self.assertEqual(len(stops), 2)
        stopped_names = {cmd[cmd.index("--name") + 1] for cmd in stops}
        self.assertEqual(stopped_names, {"vega", "sol"})
        for stop in stops:
            self.assertIn("--soft", stop)
            self.assertIn("plugin unload", " ".join(stop))

    def test_on_session_reset_never_stops(self) -> None:
        """on_session_reset clears transient state but never executes hermes-stop."""
        self._mock_runner({
            "hermes-sessionstart": {"name": "vega", "session_id": "ses-reset"},
            "hermes-read": {"context": "", "up_to": 0, "count": 0},
        })

        self.ctx.hooks["on_session_start"]("ses-reset")
        with _sessions_lock:
            _sessions["ses-reset"].backoff_until = 9999999999.0

        # Fire on_session_reset
        self.ctx.hooks["on_session_reset"]("ses-reset", reason="agent resume")

        # Session is still active and in registry
        with _sessions_lock:
            self.assertIn("ses-reset", _sessions)
            self.assertTrue(_sessions["ses-reset"].active)
            self.assertEqual(_sessions["ses-reset"].backoff_until, 0.0)

        # No hermes-stop command was issued
        stop_calls = [cmd for cmd in self.command_log if cmd[0] == "hermes-stop"]
        self.assertEqual(len(stop_calls), 0)

    def test_on_session_finalize_soft_stops_matching_sid_only(self) -> None:
        """on_session_finalize soft-stops only the matching session id."""
        self._mock_runner({
            "hermes-sessionstart": lambda args: (
                0,
                {
                    "name": "vega" if "ses-1" in args else "sol",
                    "session_id": "ses-1" if "ses-1" in args else "ses-2",
                },
            ),
            "hermes-read": {"context": "", "up_to": 0, "count": 0},
            "hermes-stop": {"ok": True},
        })

        self.ctx.hooks["on_session_start"]("ses-1")
        self.ctx.hooks["on_session_start"]("ses-2")

        with _sessions_lock:
            self.assertIn("ses-1", _sessions)
            self.assertIn("ses-2", _sessions)

        # Finalize only ses-1
        self.ctx.hooks["on_session_finalize"]("ses-1")

        with _sessions_lock:
            self.assertNotIn("ses-1", _sessions)
            self.assertIn("ses-2", _sessions)
            self.assertTrue(_sessions["ses-2"].active)

        # Verify hermes-stop was called for ses-1 and vega, with --soft and --session-id
        stop_calls = [cmd for cmd in self.command_log if cmd[0] == "hermes-stop"]
        self.assertEqual(len(stop_calls), 1)
        stop = stop_calls[0]
        self.assertIn("--soft", stop)
        self.assertIn("--name", stop)
        self.assertEqual(stop[stop.index("--name") + 1], "vega")
        self.assertIn("--session-id", stop)
        self.assertEqual(stop[stop.index("--session-id") + 1], "ses-1")

    def test_drain_messages_ack_failure_stops_and_backs_off(self) -> None:
        """When hermes-read --ack fails or returns error/no progress, drain stops and backs off."""
        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                return 0, {"name": "vega", "session_id": "ses-ack-fail"}
            elif subcmd == "hermes-read" and "--format" in args:
                return 0, {
                    "context": "persistent batch",
                    "up_to": 42,
                    "count": 1,
                }
            elif subcmd == "hermes-read" and "--ack" in args:
                # Simulate ack failure / no progress
                return 0, {"error": "database busy", "acked": 0, "acked_to": 0}
            return 0, {}

        hcom_ext._runner_override = runner

        self.ctx.hooks["on_session_start"]("ses-ack-fail")

        # Injected only ONCE: did not loop endlessly on ack failure!
        injections = [
            m for m in self.ctx.injected_messages if m["session_key"] == "ses-ack-fail"
        ]
        self.assertEqual(len(injections), 1)

        # Backoff was set
        with _sessions_lock:
            state = _sessions["ses-ack-fail"]
            self.assertGreater(state.backoff_until, time.time())

        # An immediate subsequent drain attempt during backoff does nothing
        _drain_messages(state, self.ctx)
        self.assertEqual(
            len([m for m in self.ctx.injected_messages if m["session_key"] == "ses-ack-fail"]),
            1,
        )

    def test_drain_messages_empty_ack_payload_fails_closed_and_backs_off(self) -> None:
        """When hermes-read --ack returns code 0 but empty payload {}, drain stops and backs off."""
        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                return 0, {"name": "vega", "session_id": "ses-empty-ack"}
            elif subcmd == "hermes-read" and "--format" in args:
                return 0, {
                    "context": "persistent batch",
                    "up_to": 42,
                    "count": 1,
                }
            elif subcmd == "hermes-read" and "--ack" in args:
                # Code 0 with empty payload {} missing acked_to
                return 0, {}
            return 0, {}

        hcom_ext._runner_override = runner

        self.ctx.hooks["on_session_start"]("ses-empty-ack")

        # Injected only ONCE: did not loop endlessly on empty ack payload!
        injections = [
            m for m in self.ctx.injected_messages if m["session_key"] == "ses-empty-ack"
        ]
        self.assertEqual(len(injections), 1)

        # Backoff was set
        with _sessions_lock:
            state = _sessions["ses-empty-ack"]
            self.assertGreater(state.backoff_until, time.time())

        # An immediate subsequent drain attempt during backoff does nothing
        _drain_messages(state, self.ctx)
        self.assertEqual(
            len([m for m in self.ctx.injected_messages if m["session_key"] == "ses-empty-ack"]),
            1,
        )

    def test_enroll_session_concurrent_idempotent(self) -> None:
        """Concurrent enrollment attempts on the same session_id are serialized and idempotent."""
        sessionstart_calls = 0
        call_lock = threading.Lock()

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal sessionstart_calls
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                with call_lock:
                    sessionstart_calls += 1
                time.sleep(0.02)  # Artificial delay to ensure race window
                return 0, {"name": "luna", "session_id": "ses-conc"}
            elif subcmd == "hermes-read":
                return 0, {"context": "", "up_to": 0, "count": 0}
            return 0, {}

        hcom_ext._runner_override = runner

        results: List[Any] = []
        threads: List[threading.Thread] = []

        def worker():
            res = _enroll_session("ses-conc", self.ctx)
            results.append(res)

        for _ in range(8):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All worker threads should have returned the same SessionState instance
        self.assertEqual(len(results), 8)
        first_state = results[0]
        self.assertIsNotNone(first_state)
        for res in results:
            self.assertIs(res, first_state)

        # hermes-sessionstart was called EXACTLY ONCE
        self.assertEqual(sessionstart_calls, 1)

        # Exactly one session in the registry with name luna
        with _sessions_lock:
            self.assertIn("ses-conc", _sessions)
            self.assertEqual(_sessions["ses-conc"].name, "luna")

    def test_idle_listener_timeout_retries_after_backoff_and_stops_when_empty(self) -> None:
        """Listener loop catches socket.timeout, retries expired backoff, and ceases retries once mailbox is empty."""
        mail_pending = True
        self.ctx.injection_policy = False  # Deny first injection

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal mail_pending
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                return 0, {"name": "vega", "session_id": "ses-idle-retry"}
            elif subcmd == "hermes-read" and "--format" in args:
                if mail_pending:
                    return 0, {"context": "retry message", "up_to": 77, "count": 1}
                return 0, {"context": "", "up_to": 0, "count": 0}
            elif subcmd == "hermes-read" and "--ack" in args:
                mail_pending = False
                up_to_val = int(args[args.index("--up-to") + 1]) if "--up-to" in args else 77
                return 0, {"acked": 1, "acked_to": up_to_val}
            return 0, {}

        hcom_ext._runner_override = runner

        # 1. Start session: initial drain fails injection and sets retry_needed=True and backoff_until
        self.ctx.hooks["on_session_start"]("ses-idle-retry")
        with _sessions_lock:
            state = _sessions["ses-idle-retry"]
            self.assertTrue(state.retry_needed)
            self.assertGreater(state.backoff_until, time.time())

        # No ack yet
        acks = [c for c in self.command_log if c[0] == "hermes-read" and "--ack" in c]
        self.assertEqual(len(acks), 0)

        # 2. Host now accepts injections
        self.ctx.injection_policy = True

        # Fast-forward backoff expiration
        state.backoff_until = time.time() - 0.01

        # Direct test of idle listener timeout branch
        now = time.time()
        if state.active and state.retry_needed and now >= state.backoff_until:
            _drain_messages(state, self.ctx)

        # Injected successfully on retry
        self.assertEqual(len(self.ctx.injected_messages), 2)
        self.assertEqual(self.ctx.injected_messages[-1]["content"], "retry message")

        # Ack was sent
        acks_after = [c for c in self.command_log if c[0] == "hermes-read" and "--ack" in c]
        self.assertEqual(len(acks_after), 1)

        # retry_needed cleared and backoff reset
        self.assertFalse(state.retry_needed)
        self.assertEqual(state.backoff_until, 0.0)

        # 3. Simulate subsequent socket.timeout on empty mailbox: MUST NOT trigger hermes-read subprocesses
        read_count_before = len([c for c in self.command_log if c[0] == "hermes-read"])
        now = time.time()
        if state.active and state.retry_needed and now >= state.backoff_until:
            _drain_messages(state, self.ctx)

        read_count_after = len([c for c in self.command_log if c[0] == "hermes-read"])
        self.assertEqual(read_count_before, read_count_after, "Idle timeout must not invoke hermes-read when empty")

    def test_drain_messages_transient_format_failure_sets_retry_and_backoff(self) -> None:
        """Transient --format failures set retry_needed and bounded backoff without tearing down session."""
        transient_error = True
        mail_pending = True

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal mail_pending
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                return 0, {"name": "vega", "session_id": "ses-trans-fmt"}
            elif subcmd == "hermes-read" and "--format" in args:
                if transient_error:
                    return 0, {"error": "database is locked"}
                if mail_pending:
                    return 0, {"context": "recovered mail", "up_to": 33, "count": 1}
                return 0, {"context": "", "up_to": 0, "count": 0}
            elif subcmd == "hermes-read" and "--ack" in args:
                mail_pending = False
                return 0, {"acked": 1, "acked_to": 33}
            return 0, {}

        hcom_ext._runner_override = runner

        # 1. Start session
        self.ctx.hooks["on_session_start"]("ses-trans-fmt")
        with _sessions_lock:
            state = _sessions["ses-trans-fmt"]
            # Initial drain hit transient format error
            self.assertTrue(state.active, "Session must NOT be torn down on transient error")
            self.assertTrue(state.retry_needed, "retry_needed must be set on transient error")
            self.assertGreater(state.backoff_until, time.time())

        # No messages injected yet
        self.assertEqual(len(self.ctx.injected_messages), 0)

        # 2. Database unlocks, expired backoff retry succeeds
        transient_error = False
        state.backoff_until = time.time() - 0.01

        _drain_messages(state, self.ctx)

        # Successfully recovered and drained
        self.assertEqual(len(self.ctx.injected_messages), 1)
        self.assertEqual(self.ctx.injected_messages[0]["content"], "recovered mail")
        self.assertFalse(state.retry_needed)
        self.assertEqual(state.backoff_until, 0.0)

    def test_claimed_format_transient_db_error_preserves_session_and_recovers(self) -> None:
        """Transient DB read error on claimed --format preserves active session/listener, sets backoff, and recovers without teardown."""
        error_mode: Optional[str] = "db_error"
        mail_items = [{"context": "recovered event", "up_to": 42, "count": 1}]

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                return 0, {"name": "vega", "session_id": "ses-db-retry"}
            elif subcmd == "hermes-read" and "--format" in args:
                if error_mode == "db_error":
                    # Distinguishable retryable DB read failure from Rust backend
                    return 0, {"error": "Failed to read events from database: disk I/O error"}
                elif error_mode == "stale":
                    return 0, {
                        "error": "Hermes instance 'vega' is no longer attributed to session 'ses-db-retry' (current: ses-other)"
                    }
                elif mail_items:
                    item = mail_items[-1]
                    return 0, {"context": item["context"], "up_to": item["up_to"], "count": item["count"]}
                return 0, {"context": "", "up_to": 0, "count": 0}
            elif subcmd == "hermes-read" and "--ack" in args:
                mail_items.clear()
                up_to_val = int(args[args.index("--up-to") + 1]) if "--up-to" in args else 42
                return 0, {"acked": 1, "acked_to": up_to_val}
            return 0, {}

        hcom_ext._runner_override = runner

        # 1. Start session; initial drain encounters the distinguishable DB read error
        now_before = time.time()
        self.ctx.hooks["on_session_start"]("ses-db-retry")

        with _sessions_lock:
            self.assertIn("ses-db-retry", _sessions)
            state = _sessions["ses-db-retry"]
            # Active session, socket, and listener thread must remain intact!
            self.assertTrue(state.active, "Session must remain active on transient DB error")
            self.assertIsNotNone(state.server_sock)
            self.assertNotEqual(state.server_sock.fileno(), -1)
            self.assertIsNotNone(state.listener_thread)
            self.assertTrue(state.listener_thread.is_alive())
            # retry_needed and failure backoff floor >= 2s set
            self.assertTrue(state.retry_needed)
            self.assertGreaterEqual(state.backoff_until, now_before + 1.9)
            self.assertGreaterEqual(state.backoff_delay, 4.0)

        # No messages injected and no ack sent
        self.assertEqual(len(self.ctx.injected_messages), 0)
        acks = [c for c in self.command_log if c[0] == "hermes-read" and "--ack" in c]
        self.assertEqual(len(acks), 0)

        # 2. Transient DB failure clears; expired backoff retry succeeds
        error_mode = None
        state.backoff_until = time.time() - 0.01

        _drain_messages(state, self.ctx)

        # Mail is injected and acked
        self.assertEqual(len(self.ctx.injected_messages), 1)
        self.assertEqual(self.ctx.injected_messages[0]["content"], "recovered event")
        self.assertEqual(self.ctx.injected_messages[0]["session_key"], "ses-db-retry")
        self.assertFalse(state.retry_needed)
        self.assertEqual(state.backoff_until, 0.0)

        acks_after = [c for c in self.command_log if c[0] == "hermes-read" and "--ack" in c]
        self.assertEqual(len(acks_after), 1)
        self.assertIn("--session-id", acks_after[0])
        self.assertEqual(acks_after[0][acks_after[0].index("--session-id") + 1], "ses-db-retry")

        # 3. Contrast: verified stale attribution response STILL tears down state
        error_mode = "stale"
        _drain_messages(state, self.ctx)

        self.assertFalse(state.active, "Stale attribution must tear down active session")
        with _sessions_lock:
            self.assertNotIn("ses-db-retry", _sessions)

    def test_stale_attribution_format_refusal_tears_down_state_and_allows_re_enrollment(self) -> None:
        """When hermes-read --format is refused due to stale attribution, old state is torn down and re-enrollment allowed."""
        stale = False
        rebound = False
        mail_pending = True

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal mail_pending
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                name = "sol" if rebound else "vega"
                return 0, {"name": name, "session_id": "ses-stale"}
            elif subcmd == "hermes-read" and "--format" in args:
                if stale:
                    return 0, {
                        "error": "Hermes instance 'vega' is no longer attributed to session 'ses-stale' (current: ses-other)"
                    }
                if mail_pending:
                    return 0, {"context": "batch", "up_to": 1, "count": 1}
                return 0, {"context": "", "up_to": 0, "count": 0}
            elif subcmd == "hermes-read" and "--ack" in args:
                mail_pending = False
                return 0, {"acked": 1, "acked_to": 1}
            elif subcmd == "hermes-read" and "--name" not in args:
                # Resolve mode
                if rebound:
                    return 0, {"name": "sol", "session_id": "ses-stale"}
                return 0, {"name": None, "session_id": "ses-stale"}
            return 0, {}

        hcom_ext._runner_override = runner

        # 1. Enrolled initially as vega
        self.ctx.hooks["on_session_start"]("ses-stale")
        with _sessions_lock:
            state = _sessions["ses-stale"]
            self.assertTrue(state.active)
            self.assertEqual(state.name, "vega")

        # 2. Next read returns stale attribution error
        stale = True
        _drain_messages(state, self.ctx)

        # Old state must be torn down
        self.assertFalse(state.active)
        with _sessions_lock:
            self.assertNotIn("ses-stale", _sessions)
            self.assertNotIn("ses-stale", _discovery_throttles)

        # 3. Mid-session re-enrollment with new name sol
        rebound = True
        stale = False
        self.ctx.hooks["post_tool_call"]("ses-stale", tool_name="bash", command="hcom start sol")

        with _sessions_lock:
            self.assertIn("ses-stale", _sessions)
            new_state = _sessions["ses-stale"]
            self.assertTrue(new_state.active)
            self.assertEqual(new_state.name, "sol")

    def test_drain_messages_iteration_cap_and_continuation(self) -> None:
        """_drain_messages caps at max_iterations and schedules continuation to avoid stranding mail."""
        iterations = 0
        total_available = 70

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal iterations
            subcmd = args[0]
            if subcmd == "hermes-read" and "--format" in args:
                if iterations < total_available:
                    iterations += 1
                    return 0, {"context": f"batch {iterations}", "up_to": iterations, "count": 1}
                return 0, {"context": "", "up_to": 0, "count": 0}
            elif subcmd == "hermes-read" and "--ack" in args:
                return 0, {"acked": 1, "acked_to": iterations}
            return 0, {}

        hcom_ext._runner_override = runner
        state = SessionState(session_id="ses-cap", name="vega", active=True)

        # First drain pass
        _drain_messages(state, self.ctx)

        # Capped at 50 iterations
        self.assertEqual(iterations, 50)
        self.assertEqual(len(self.ctx.injected_messages), 50)
        # Scheduled continuation: retry_needed is True with short backoff
        self.assertTrue(state.retry_needed)
        self.assertGreater(state.backoff_until, 0.0)
        self.assertGreaterEqual(state.backoff_delay, 2.0, "Failure backoff floor must remain >= 2.0s")

        # Continuation pass after backoff expires drains remaining 20 batches
        state.backoff_until = time.time() - 0.01
        _drain_messages(state, self.ctx)

        self.assertEqual(iterations, 70)
        self.assertEqual(len(self.ctx.injected_messages), 70)
        # Mailbox now empty: retry_needed reset
        self.assertFalse(state.retry_needed)
        self.assertEqual(state.backoff_until, 0.0)

    def test_drain_messages_iteration_cap_failure_on_continuation_respects_backoff_floor(self) -> None:
        """When continuation after reaching the iteration cap fails, failure backoff floor >= 2s is enforced."""
        iterations = 0
        total_available = 70
        fail_continuation = False

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal iterations
            subcmd = args[0]
            if subcmd == "hermes-read" and "--format" in args:
                if fail_continuation:
                    return 0, {"error": "transient db failure"}
                if iterations < total_available:
                    iterations += 1
                    return 0, {"context": f"batch {iterations}", "up_to": iterations, "count": 1}
                return 0, {"context": "", "up_to": 0, "count": 0}
            elif subcmd == "hermes-read" and "--ack" in args:
                return 0, {"acked": 1, "acked_to": iterations}
            return 0, {}

        hcom_ext._runner_override = runner
        state = SessionState(session_id="ses-cap-fail", name="vega", active=True)

        # 1. First drain pass hits 50-iteration cap
        _drain_messages(state, self.ctx)
        self.assertEqual(iterations, 50)
        self.assertTrue(state.retry_needed)
        # Short scheduled continuation
        self.assertLessEqual(state.backoff_until, time.time() + 0.1)
        # Floor must be >= 2.0s
        self.assertGreaterEqual(state.backoff_delay, 2.0)

        # 2. Continuation attempt encounters a transient error
        fail_continuation = True
        state.backoff_until = time.time() - 0.01
        now_before = time.time()
        _drain_messages(state, self.ctx)

        # Failure backoff floor >= 2.0s must be enforced, NOT 0.05s
        self.assertTrue(state.retry_needed)
        self.assertGreaterEqual(state.backoff_until, now_before + 1.9)
        self.assertGreaterEqual(state.backoff_delay, 4.0)

    def test_drain_messages_iteration_cap_failure_on_50th_iteration_preserves_failure_backoff(self) -> None:
        """When the 50th iteration fails, failure backoff floor >= 2s is preserved and no 50ms continuation is scheduled."""
        iterations = 0

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal iterations
            subcmd = args[0]
            if subcmd == "hermes-read" and "--format" in args:
                iterations += 1
                if iterations == 50:
                    # 50th format call fails with transient error
                    return 0, {"error": "transient database lock on 50th"}
                return 0, {"context": f"batch {iterations}", "up_to": iterations, "count": 1}
            elif subcmd == "hermes-read" and "--ack" in args:
                return 0, {"acked": 1, "acked_to": iterations}
            return 0, {}

        hcom_ext._runner_override = runner
        state = SessionState(session_id="ses-50th-fail", name="vega", active=True)

        now_before = time.time()
        _drain_messages(state, self.ctx)

        # First 49 batches were injected and acked
        self.assertEqual(len(self.ctx.injected_messages), 49)
        self.assertEqual(iterations, 50)

        # 50th failed: retry_needed is True, backoff >= 2.0s from now_before, NOT 50ms!
        self.assertTrue(state.retry_needed)
        self.assertGreaterEqual(state.backoff_until, now_before + 1.9)
        self.assertGreater(state.backoff_until, time.time() + 0.5, "Must not schedule short 50ms continuation")
        self.assertGreaterEqual(state.backoff_delay, 4.0)

    def test_drain_messages_iteration_cap_empty_on_50th_iteration_preserves_clean_exit(self) -> None:
        """When the 50th iteration receives an empty mailbox, retry_needed is cleared and no continuation is scheduled."""
        iterations = 0

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            nonlocal iterations
            subcmd = args[0]
            if subcmd == "hermes-read" and "--format" in args:
                iterations += 1
                if iterations == 50:
                    # 50th format call sees empty mailbox
                    return 0, {"context": "", "up_to": 0, "count": 0}
                return 0, {"context": f"batch {iterations}", "up_to": iterations, "count": 1}
            elif subcmd == "hermes-read" and "--ack" in args:
                return 0, {"acked": 1, "acked_to": iterations}
            return 0, {}

        hcom_ext._runner_override = runner
        state = SessionState(session_id="ses-50th-empty", name="vega", active=True)

        _drain_messages(state, self.ctx)

        # First 49 batches injected and acked
        self.assertEqual(len(self.ctx.injected_messages), 49)
        self.assertEqual(iterations, 50)

        # 50th was empty: retry_needed is False, backoff_until is 0.0, no continuation
        self.assertFalse(state.retry_needed)
        self.assertEqual(state.backoff_until, 0.0)
        self.assertEqual(state.backoff_delay, 2.0)

    def test_enroll_session_aborted_if_unloaded(self) -> None:
        """_enroll_session immediately aborts if plugin was unloaded."""
        _on_unload()
        res = _enroll_session("ses-after-unload", self.ctx)
        self.assertIsNone(res)
        with _sessions_lock:
            self.assertNotIn("ses-after-unload", _sessions)

    def test_on_session_start_after_unload_does_not_resurrect_module(self) -> None:
        """A late/in-flight on_session_start after on_unload must not reset _unloaded or publish a listener."""
        _on_unload()
        self.assertTrue(hcom_ext._unloaded)

        # In-flight hook arrives after unload
        self.ctx.hooks["on_session_start"]("ses-late")

        # Must still be unloaded and not enrolled
        self.assertTrue(hcom_ext._unloaded)
        with _sessions_lock:
            self.assertNotIn("ses-late", _sessions)

        # Fresh register resets _unloaded
        new_ctx = FakePluginContext()
        register(new_ctx)
        self.assertFalse(hcom_ext._unloaded)

    def test_stale_attribution_teardown_preserves_session_lock_mutual_exclusion(self) -> None:
        """Stale attribution teardown preserves _session_locks entry so concurrent re-enrollments cannot double publish."""
        stale = False
        rebound = False

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                name = "sol" if rebound else "vega"
                return 0, {"name": name, "session_id": "ses-lock-race"}
            elif subcmd == "hermes-read" and "--format" in args:
                if stale:
                    return 0, {
                        "error": "Hermes instance 'vega' is no longer attributed to session 'ses-lock-race' (current: ses-other)"
                    }
                return 0, {"context": "", "up_to": 0, "count": 0}
            return 0, {}

        hcom_ext._runner_override = runner

        # 1. Enroll initially as vega
        self.ctx.hooks["on_session_start"]("ses-lock-race")
        with _sessions_lock:
            state = _sessions["ses-lock-race"]
            self.assertTrue(state.active)
            self.assertIn("ses-lock-race", _session_locks)
            initial_lock = _session_locks["ses-lock-race"]

        # 2. Trigger stale attribution teardown
        stale = True
        _drain_messages(state, self.ctx)

        # State is torn down, but _session_locks entry MUST be preserved
        self.assertFalse(state.active)
        with _sessions_lock:
            self.assertNotIn("ses-lock-race", _sessions)
            self.assertIn(
                "ses-lock-race",
                _session_locks,
                "_session_locks entry must NOT be removed by stale attribution teardown",
            )
            self.assertIs(
                _session_locks["ses-lock-race"],
                initial_lock,
                "Canonical session lock instance must remain identical across stale teardown",
            )

        # 3. Deterministic mutual exclusion verification:
        # If another thread / caller attempts to re-enroll while the canonical lock is held,
        # it must block and cannot acquire a distinct lock or double-publish.
        rebound = True
        stale = False

        enrollment_started = threading.Event()
        enrollment_done = threading.Event()
        enrolled_state: List[Optional[SessionState]] = []

        # Hold the preserved lock to simulate a concurrent thread in critical section
        with initial_lock:
            def concurrent_enroll():
                enrollment_started.set()
                s = _enroll_session("ses-lock-race", self.ctx)
                enrolled_state.append(s)
                enrollment_done.set()

            t = threading.Thread(target=concurrent_enroll, daemon=True)
            t.start()
            self.assertTrue(enrollment_started.wait(timeout=1.0))

            # Allow some time: worker thread MUST be blocked by initial_lock
            time.sleep(0.05)
            self.assertFalse(
                enrollment_done.is_set(),
                "Concurrent enrollment must be blocked by the existing session lock",
            )
            with _sessions_lock:
                self.assertNotIn(
                    "ses-lock-race",
                    _sessions,
                    "Session must not be published while session lock is held",
                )

        # Once lock is released, concurrent enrollment unblocks and succeeds safely
        self.assertTrue(enrollment_done.wait(timeout=1.0))
        t.join(timeout=1.0)

        with _sessions_lock:
            self.assertIn("ses-lock-race", _sessions)
            new_state = _sessions["ses-lock-race"]
            self.assertTrue(new_state.active)
            self.assertEqual(new_state.name, "sol")
            self.assertIs(
                _session_locks["ses-lock-race"],
                initial_lock,
                "Lock instance must remain consistent after re-enrollment",
            )

    def test_lost_notify_fallback_opportunistic_drain_and_throttle(self) -> None:
        """Enrolled session with retry_needed=False checks mail opportunistically on post_tool_call with 3s throttle."""
        mail_items: List[Dict[str, Any]] = []

        def runner(args: List[str]) -> tuple[int, Dict[str, Any]]:
            self.command_log.append(list(args))
            subcmd = args[0]
            if subcmd == "hermes-sessionstart":
                return 0, {"name": "vega", "session_id": "ses-lost-notify"}
            elif subcmd == "hermes-read" and "--format" in args:
                if mail_items:
                    item = mail_items[-1]
                    return 0, {
                        "context": f"lost notify message: {item['text']}",
                        "up_to": item["id"],
                        "count": len(mail_items),
                    }
                return 0, {"context": "", "up_to": 0, "count": 0}
            elif subcmd == "hermes-read" and "--ack" in args:
                up_to_val = int(args[args.index("--up-to") + 1]) if "--up-to" in args else 1
                mail_items.clear()
                return 0, {"acked": 1, "acked_to": up_to_val}
            return 0, {}

        hcom_ext._runner_override = runner

        # 1. Start session; mailbox starts empty, so retry_needed is False
        self.ctx.hooks["on_session_start"]("ses-lost-notify")
        with _sessions_lock:
            state = _sessions["ses-lost-notify"]
            self.assertTrue(state.active)
            self.assertEqual(state.name, "vega")
            self.assertFalse(state.retry_needed)
            self.assertEqual(state.backoff_until, 0.0)

        # Clear initial enroll command logs and injected messages
        self.command_log.clear()
        self.ctx.injected_messages.clear()

        # 2. Incoming mail arrives, but connect-drop notify ping to TCP port is LOST (never sent/received)
        mail_items.append({"id": 10, "text": "important un-notified event"})

        # 3. A tool call finishes immediately (< 3.0s elapsed)
        self.ctx.hooks["post_tool_call"]("ses-lost-notify", tool_name="bash", command="ls -la")

        # Throttled: no hermes-read --format should be invoked, no injection
        format_calls = [c for c in self.command_log if c[0] == "hermes-read" and "--format" in c]
        self.assertEqual(len(format_calls), 0, "Opportunistic drain must throttle when < 3.0s elapsed")
        self.assertEqual(len(self.ctx.injected_messages), 0)

        # 4. Advance time past the 3.0s cooldown
        state.last_opportunistic_drain = time.time() - 3.1

        # 5. Next tool call finishes: opportunistic check discovers the mail
        self.ctx.hooks["post_tool_call"]("ses-lost-notify", tool_name="bash", command="cat output.txt")

        # hermes-read --format was called and message injected
        format_calls = [c for c in self.command_log if c[0] == "hermes-read" and "--format" in c]
        self.assertGreaterEqual(len(format_calls), 1, "Opportunistic drain must trigger after 3.0s throttle")
        self.assertEqual(len(self.ctx.injected_messages), 1)
        self.assertIn("important un-notified event", self.ctx.injected_messages[0]["content"])
        self.assertEqual(self.ctx.injected_messages[0]["session_key"], "ses-lost-notify")

        # Monotonic ack was sent carrying correct session_id
        ack_calls = [c for c in self.command_log if c[0] == "hermes-read" and "--ack" in c]
        self.assertEqual(len(ack_calls), 1)
        self.assertIn("--session-id", ack_calls[0])
        self.assertEqual(ack_calls[0][ack_calls[0].index("--session-id") + 1], "ses-lost-notify")
        self.assertIn("--name", ack_calls[0])
        self.assertEqual(ack_calls[0][ack_calls[0].index("--name") + 1], "vega")

        # 6. Immediate subsequent tool call (< 3.0s): must be throttled again
        self.command_log.clear()
        self.ctx.hooks["post_tool_call"]("ses-lost-notify", tool_name="bash", command="echo done")
        subsequent_format_calls = [c for c in self.command_log if c[0] == "hermes-read" and "--format" in c]
        self.assertEqual(
            len(subsequent_format_calls),
            0,
            "Immediate subsequent tool call must be throttled by 3.0s cooldown",
        )


if __name__ == "__main__":
    unittest.main()
