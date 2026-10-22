"""Unit and fake-host tests for ordinary Hermes terminal participation
in src/hermes_plugin/__init__.py. Stdlib only; run with `python3 tests/hermes_native_plugin.py`."""

import asyncio
import importlib.util
import os
import queue
import sys
import threading
import time
import unittest

sys.dont_write_bytecode = True
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location(
    "hcom_hermes_plugin", os.path.join(ROOT, "src", "hermes_plugin", "__init__.py")
)
assert _spec and _spec.loader
plugin = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(plugin)


class FakeCli:
    def __init__(self, session_id):
        self.session_id = session_id
        self._pending_input = queue.Queue()


class FakeManager:
    def __init__(self, session_id="s-term-1"):
        self._cli_ref = FakeCli(session_id)
        self.home_path = "/tmp/fake_hermes_home"
        self.has_tui_message_injector = False
        self.has_gateway_message_injector = False


class FakeCtx:
    plugin_id = "hcom"

    def __init__(self, session_id="s-term-1"):
        self._manager = FakeManager(session_id)
        self.hooks = {}
        self.commands = {}

    def register_hook(self, name, fn):
        self.hooks[name] = fn

    def register_command(self, name, handler, **_):
        self.commands[name] = handler


class FakeTerminalHcom:
    def __init__(self, participant_name="kiri"):
        self.participant_name = participant_name
        self.calls = []
        self.mail = []  # (event_id, sender, text, to)
        self.cursors = {}
        self.next_id = 100
        self.enrolled_sessions = {}
        self.lock = threading.Lock()

    def enroll(self, session_id, name):
        with self.lock:
            self.enrolled_sessions[session_id] = name

    def send(self, text, sender="luna", to=None):
        to = to or self.participant_name
        with self.lock:
            self.next_id += 1
            self.mail.append((self.next_id, sender, text, to))
            return self.next_id

    def call(self, args, env=None, participant=None):
        with self.lock:
            self.calls.append(list(args))
        cmd = args[0]
        if cmd == "hermes-read":
            # Session-id read-only resolve mode:
            if "--session-id" in args and "--name" not in args:
                sid_idx = args.index("--session-id") + 1
                sid = args[sid_idx] if sid_idx < len(args) else None
                with self.lock:
                    name = self.enrolled_sessions.get(sid)
                return {"name": name, "session_id": sid}

            name_idx = args.index("--name") + 1 if "--name" in args else None
            name = args[name_idx] if name_idx and name_idx < len(args) else self.participant_name

            # Ack mode
            if "--ack" in args:
                up_to = int(args[args.index("--up-to") + 1]) if "--up-to" in args else self.next_id
                with self.lock:
                    self.cursors[name] = max(self.cursors.get(name, 0), up_to)
                return {"acked_to": up_to, "acked": 1}

            # Batch read mode
            with self.lock:
                cursor = self.cursors.get(name, 0)
                batch = [m for m in self.mail if m[3] == name and m[0] > cursor]
            return {
                "messages": [{"event_id": m[0], "from": m[1], "message": m[2]} for m in batch],
                "has_more": False,
            }
        if cmd == "hermes-start":
            if "--existing-only" in args:
                # Attach-only, exactly the handler's contract: the name
                # hcom already has for this session, or null when nothing
                # is enrolled — never a name regardless of the flag.
                sid_idx = args.index("--session-id") + 1
                sid = args[sid_idx] if sid_idx < len(args) else None
                with self.lock:
                    name = self.enrolled_sessions.get(sid)
                return {"name": name, "bootstrap": None}
            return {"name": self.participant_name, "bootstrap": None}


class HermesNativePluginTests(unittest.TestCase):
    def setUp(self):
        self.ctx = FakeCtx("ses-test-term")
        self.env = {"PATH": os.environ.get("PATH", "")}
        self.fake_hcom = FakeTerminalHcom("kiri")

    def test_ordinary_start_without_hcom_launched_creates_gateway_host(self):
        """Without HCOM_LAUNCHED=1, setup creates a GatewayHost capable of ordinary terminal discovery."""
        host = plugin.setup(self.ctx, dict(self.env), 99991, {})
        self.assertIsInstance(host, plugin.GatewayHost)
        self.assertIsNotNone(host.me)
        self.assertIsNone(host.me.name)

    def test_host_isolation_guard_against_nested_hermes(self):
        """If HERMES_HOST_PROCESS_ENV is already present, setup refuses to bind nested process."""
        env = dict(self.env, **{plugin.HERMES_HOST_PROCESS_ENV: "host-pid-1"})
        host = plugin.setup(self.ctx, env, 99992, {})
        self.assertIsNone(host)

    def test_pid_claim_is_one_shot(self):
        """Host claim succeeds for host PID; child process with different PID cannot claim."""
        env = dict(self.env)
        self.assertTrue(plugin.claim_host(env, 99993))
        self.assertEqual(env.get(plugin.HOST_PID_ENV), "99993")
        # Child process cannot claim host
        self.assertFalse(plugin.claim_host(env, 99994))

    def test_terminal_enrollment_discovery_via_read_only_session_id(self):
        """An ordinary terminal session is discovered via check_enrolled_terminal when enrolled in DB."""
        host = plugin.setup(self.ctx, dict(self.env), 99995, {})
        host.rpc = lambda args, participant=None, **_: self.fake_hcom.call(args, participant=participant)
        host.start_notify_server = lambda: None
        host.start_threads = lambda: None

        # Before enrollment: check returns False
        self.assertFalse(host.check_enrolled_terminal("ses-test-term"))
        self.assertIsNone(host.me.name)

        # Enroll in fake hcom
        self.fake_hcom.enroll("ses-test-term", "kiri")

        # Hook on_session_start triggers check_enrolled_terminal
        host.on_session_start(session_id="ses-test-term")
        self.assertEqual(host.me.name, "kiri")
        self.assertEqual(host.me.session_id, "ses-test-term")
        self.assertIn(host.me, host.participants())

    def test_mid_turn_enrollment_timing_discovery_flow(self):
        """Test hook order: start (no enrollment) -> enroll mid-turn -> transform_tool_result and pre_llm.
        Verifies bounded discovery without any synthetic second session-start."""
        ctx = FakeCtx("ses-mid-turn")
        host = plugin.setup(ctx, dict(self.env), 99988, {})
        host.rpc = lambda args, participant=None, **_: self.fake_hcom.call(args, participant=participant)
        host.start_notify_server = lambda: None
        host.start_threads = lambda: None

        # 1. on_session_start fires when session is NOT yet enrolled
        host.on_session_start(session_id="ses-mid-turn")
        self.assertIsNone(host.me.name)
        self.assertIsNone(host.me.session_id)
        self.assertEqual(host.terminal_session, "ses-mid-turn")

        # 2. Model runs hcom start mid-turn, enrolling the session
        self.fake_hcom.enroll("ses-mid-turn", "kiri")
        self.fake_hcom.send("urgent mid-tool dispatch", sender="luna", to="kiri")

        # 3. Next hook: transform_tool_result (without any second on_session_start!)
        # Bounded discovery recognizes the enrollment and delivers pending mail
        res = host.transform_tool_result(result="tool execution complete", session_id="ses-mid-turn")
        self.assertEqual(host.me.name, "kiri")
        self.assertEqual(host.me.session_id, "ses-mid-turn")
        self.assertIsNotNone(res)
        self.assertIn("tool execution complete", res)
        self.assertIn("urgent mid-tool dispatch", res)

        # Confirm history so outstanding delivery clears
        host.post_llm_call(
            session_id="ses-mid-turn",
            conversation_history=[{"role": "tool", "content": res}],
        )

        # 4. Next hook: pre_llm_call for subsequent prompt turn
        self.fake_hcom.send("turn start message", sender="luna", to="kiri")
        host.me.pending = True
        out_pre = host.pre_llm_call(session_id="ses-mid-turn", user_message="What next?")
        self.assertIsNotNone(out_pre)
        self.assertIn("turn start message", out_pre.get("context", ""))

        # 5. Unknown or delegate session does not auto-adopt or leak
        res_unknown = host.transform_tool_result(result="other output", session_id="ses-unknown-subagent")
        self.assertIsNone(res_unknown)

    def test_visible_delivery_turn_start_pre_llm_call(self):
        """When enrolled, pre_llm_call delivers messages into the prompt context."""
        host = plugin.setup(self.ctx, dict(self.env), 99996, {})
        host.rpc = lambda args, participant=None, **_: self.fake_hcom.call(args, participant=participant)
        host.start_notify_server = lambda: None
        host.start_threads = lambda: None

        self.fake_hcom.enroll("ses-test-term", "kiri")
        host.on_session_start(session_id="ses-test-term")

        # Send a message to kiri
        msg_id = self.fake_hcom.send("Hello from luna!", sender="luna", to="kiri")

        # pre_llm_call fetches unread batch
        out = host.pre_llm_call(session_id="ses-test-term", user_message="What is the plan?")
        self.assertIsNotNone(out)
        self.assertIn("Hello from luna!", out.get("context", ""))
        self.assertIn("luna", out.get("context", ""))

    def test_visible_delivery_mid_turn_transform_tool_result(self):
        """When enrolled, mid-turn incoming messages are appended via transform_tool_result."""
        host = plugin.setup(self.ctx, dict(self.env), 99997, {})
        host.rpc = lambda args, participant=None, **_: self.fake_hcom.call(args, participant=participant)
        host.start_notify_server = lambda: None
        host.start_threads = lambda: None

        self.fake_hcom.enroll("ses-test-term", "kiri")
        host.on_session_start(session_id="ses-test-term")

        # Arm pending batch
        self.fake_hcom.send("Urgent update mid-tool", sender="boss", to="kiri")
        host.me.pending = True

        res = host.transform_tool_result(result="tool finished ok", session_id="ses-test-term")
        self.assertIsNotNone(res)
        self.assertIn("tool finished ok", res)
        self.assertIn("Urgent update mid-tool", res)
        self.assertIn("boss", res)

    def test_write_before_ack_guarantee(self):
        """ACK is only issued when the message is confirmed present in conversation_history."""
        host = plugin.setup(self.ctx, dict(self.env), 99998, {})
        host.rpc = lambda args, participant=None, **_: self.fake_hcom.call(args, participant=participant)
        host.start_notify_server = lambda: None
        host.start_threads = lambda: None

        self.fake_hcom.enroll("ses-test-term", "kiri")
        host.on_session_start(session_id="ses-test-term")

        msg_id = self.fake_hcom.send("Critical instruction", sender="boss", to="kiri")

        # 1. Offer the batch via pre_llm_call
        out = host.pre_llm_call(session_id="ses-test-term", user_message="Hello")
        self.assertIsNotNone(out)
        delivered_text = out["context"]
        self.assertIn("Critical instruction", delivered_text)

        # Before LLM confirms: cursor in fake_hcom must NOT be updated
        self.assertEqual(self.fake_hcom.cursors.get("kiri", 0), 0)

        # 2. Case A: post_llm_call with unrelated history (batch was dropped)
        host.post_llm_call(
            session_id="ses-test-term",
            conversation_history=[{"role": "user", "content": "Just user msg without delivery"}],
        )
        # Settle attempt should not ACK because batch was not confirmed
        if host.me.out:
            host.me.settle(host.me.out)
        self.assertEqual(self.fake_hcom.cursors.get("kiri", 0), 0)

        # 3. Case B: post_llm_call confirms conversation history contains the delivery
        host.post_llm_call(
            session_id="ses-test-term",
            conversation_history=[
                {"role": "user", "content": f"Hello\n\n{delivered_text}"}
            ],
        )
        # Confirm in post_llm_call automatically calls settle, verifying write-before-ACK
        self.assertEqual(self.fake_hcom.cursors.get("kiri"), msg_id)

    def test_gateway_route_isolation_preserved(self):
        """Gateway messaging chat bridges and ordinary terminal coexist without route collisions."""
        host = plugin.setup(self.ctx, dict(self.env), 99999, {})
        host.rpc = lambda args, participant=None, **_: self.fake_hcom.call(args, participant=participant)
        host.start_notify_server = lambda: None
        host.start_threads = lambda: None

        # Add a bridge for chat telegram:123
        bridge = plugin.Bridge(
            host,
            {
                "name": "telegram_chat",
                "token": "tok1",
                "session_key": "telegram:123",
                "visibility": "full",
            },
        )
        bridge.bind("ses-bridge-telegram")
        bridge_starts = [c for c in self.fake_hcom.calls
                         if c[0] == "hermes-start" and "ses-bridge-telegram" in c]
        self.assertEqual(len(bridge_starts), 1)
        self.assertNotIn("--existing-only", bridge_starts[0],
                         "bridge binds keep the full ladder (their row exists)")
        host.bridges["telegram_chat"] = bridge

        # Enroll terminal
        self.fake_hcom.enroll("ses-test-term", "kiri")
        host.on_session_start(session_id="ses-test-term")

        # Routing check
        self.assertIs(host.route("ses-test-term"), host.me)
        self.assertIs(host.route("ses-bridge-telegram"), bridge)
        self.assertIsNone(host.route("ses-unknown-other"))

    def test_on_session_reset_unknown_tui_does_not_auto_adopt(self):
        """A session reset for an unknown TUI must not adopt: only the
        read-only enrollment check may look, exactly like the other
        discovery hooks. Enrolling is `/hcom start`, never a reset."""
        tui_ctx = FakeCtx("ses-tui-1")
        del tui_ctx._manager._cli_ref  # a TUI process: no CLI participant
        host = plugin.setup(tui_ctx, dict(self.env), 99986, {})
        host.rpc = lambda args, participant=None, **_: self.fake_hcom.call(args, participant=participant)
        host.start_notify_server = lambda: None
        host.start_threads = lambda: None
        self.assertIsNone(host.me.cli())

        # Unknown session: no bind, and crucially no hermes-start at all.
        host.on_session_reset(session_id="ses-tui-1")
        self.assertIsNone(host.me.name, "an unknown TUI must not adopt on session reset")
        starts = [c for c in self.fake_hcom.calls if c[0] == "hermes-start"]
        self.assertEqual(starts, [], "no hermes-start may be issued for an unknown TUI")
        reads = [c for c in self.fake_hcom.calls if c[0] == "hermes-read"]
        self.assertTrue(reads, "the read-only enrollment check must still run")

        # Once enrolled (explicit /hcom start elsewhere), a later reset
        # re-attaches the terminal through the same read-only check.
        self.fake_hcom.enroll("ses-tui-1", "kiri")
        host.on_session_reset(session_id="ses-tui-1")
        self.assertEqual(host.me.name, "kiri")
        self.assertEqual(host.me.session_id, "ses-tui-1")

        # An enrolled participant follows /new explicitly — with the FULL
        # ladder (the new session has no enrollment yet, so attach-only
        # would refuse the rebind).
        host.on_session_reset(session_id="ses-tui-new")
        self.assertEqual(host.me.session_id, "ses-tui-new")
        new_starts = [c for c in self.fake_hcom.calls
                      if c[0] == "hermes-start" and "ses-tui-new" in c]
        self.assertEqual(len(new_starts), 1)
        self.assertNotIn("--existing-only", new_starts[0])

    def test_enrollment_removed_between_read_and_start_does_not_mint(self):
        """Enrollment can disappear between the read-only discovery check
        and the bind's hermes-start. The attach RPC carries
        --existing-only, so hcom answers a null name instead of minting:
        no participant name, no adoption, and the start was attach-only."""
        tui_ctx = FakeCtx("ses-race-1")
        del tui_ctx._manager._cli_ref  # a TUI terminal: no CLI session to match
        host = plugin.setup(tui_ctx, dict(self.env), 99982, {})
        host.rpc = lambda args, participant=None, **_: self.fake_hcom.call(args, participant=participant)
        host.start_notify_server = lambda: None
        host.start_threads = lambda: None

        race_session = "ses-race-1"
        self.fake_hcom.enroll(race_session, "kiri")
        original_call = self.fake_hcom.call

        def racing_call(args, env=None, participant=None):
            # The enrollment exists for the read... and is gone before the
            # bind's hermes-start lands.
            if args[0] == "hermes-read" and race_session in args and "--name" not in args:
                with self.fake_hcom.lock:
                    name = self.fake_hcom.enrolled_sessions.pop(race_session, None)
                return {"name": name, "session_id": race_session}
            return original_call(args, env=env, participant=participant)

        host.rpc = lambda args, participant=None, **_: racing_call(args, participant=participant)
        host.on_session_start(session_id=race_session)  # discovery: read -> bind

        self.assertIsNone(host.me.name, "a vanished enrollment must not become a minted name")
        self.assertIsNone(host.me.session_id)
        starts = [c for c in self.fake_hcom.calls if c[0] == "hermes-start"]
        self.assertEqual(len(starts), 1, "the bind ran exactly one start RPC")
        self.assertIn("--existing-only", starts[0])
        self.assertIn(race_session, starts[0])

    def test_host_rpc_forwards_bound_session_id_for_participant_mutators(self):
        """Host.rpc appends the participant's bound session id to --name
        mutators so hcom can correlate the caller with the row's session
        attribution. Calls that already carry a session id, host-level
        resolves, bridge-lane commands, and participant-less calls are
        untouched. Plugin binds are attach-only (--existing-only)."""
        host = plugin.setup(self.ctx, dict(self.env), 99983, {})
        host.start_notify_server = lambda: None
        host.start_threads = lambda: None

        calls = []

        class Reply:
            def __init__(self, stdout):
                self.returncode = 0
                self.stdout = stdout
                self.stderr = ""

        def fake_run(cmd, **_):
            calls.append(list(cmd))
            return Reply('{"name": "kiri", "ok": true}')

        orig_run = plugin.subprocess.run
        plugin.subprocess.run = fake_run
        try:
            host.me.session_id = "ses-fwd-1"
            host.rpc(["hermes-status", "--name", "kiri"], host.me)
            host.rpc(["hermes-read", "--name", "kiri", "--ack", "--up-to", "5"], host.me)
            host.rpc(["hermes-read", "--session-id", "ses-fwd-1"])
            host.rpc(["hermes-bridge", "sync"], host.me)
            host.rpc(["hermes-status", "--name", "kiri"])
        finally:
            plugin.subprocess.run = orig_run

        hcom = calls[0][0]
        self.assertEqual(calls[0], [hcom, "hermes-status", "--name", "kiri",
                                    "--session-id", "ses-fwd-1"])
        self.assertEqual(calls[1], [hcom, "hermes-read", "--name", "kiri",
                                    "--ack", "--up-to", "5", "--session-id", "ses-fwd-1"])
        self.assertEqual(calls[2], [hcom, "hermes-read", "--session-id", "ses-fwd-1"])
        self.assertEqual(calls[3], [hcom, "hermes-bridge", "sync"])
        self.assertEqual(calls[4], [hcom, "hermes-status", "--name", "kiri"])

        # Binds are attach-only: never a minting hermes-start.
        self.fake_hcom.enroll("ses-fwd-1", "kiri")
        host.rpc = lambda args, participant=None, **_: self.fake_hcom.call(
            args, participant=participant)
        self.assertTrue(host.me.bind("ses-fwd-1"))
        starts = [c for c in self.fake_hcom.calls if c[0] == "hermes-start"]
        self.assertTrue(starts, "bind must issue hermes-start")
        self.assertIn("--existing-only", starts[-1])


if __name__ == "__main__":
    unittest.main()
