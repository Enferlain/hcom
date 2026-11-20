"""Stdlib-only unit tests for the hcom hermes plugin.

Run:  ./test.sh   (from this project directory)

The plugin talks to hcom exclusively through argv subprocess RPC, so these
tests stub the `hcom` binary (HCOM_BIN) with a script that records every
invocation to a JSONL file and emits canned JSON per subcommand — the same
contract the Rust side implements in src/hooks/hermes.rs.
"""

import importlib.util
import json
import os
import socket
import sys
import tempfile
import unittest

HERMES_PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("hermes_plugin", os.path.join(HERMES_PLUGIN_DIR, "__init__.py"))
hermes_plugin = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = hermes_plugin
spec.loader.exec_module(hermes_plugin)

STUB = r'''#!/usr/bin/env python3
import json, os, sys

calls_path = os.environ["HCOM_TEST_CALLLOG"]
argv = sys.argv[1:]
with open(calls_path, "a") as f:
    f.write(json.dumps(argv) + "\n")

mode = argv[0] if argv else ""
if mode == "hermes-sessionstart":
    sid = argv[argv.index("--session-id") + 1] if "--session-id" in argv else "?"
    print(json.dumps({"name": "luna", "session_id": sid,
                      "bootstrap": "BOOTSTRAP-PRIMER"}))
elif mode == "hermes-read" and "--format" in argv:
    # Batch smaller than unread: up_to (7) lags the newest unread event (9).
    print(json.dumps({"context": "<hcom>hello from sol</hcom>", "up_to": 7, "count": 1}))
elif mode == "hermes-read" and "--ack" in argv:
    print(json.dumps({"acked_to": 7}))
elif mode == "hermes-read":
    # request-intent by default; inform-only tests set HCOM_TEST_CHECK_REQUEST=0
    import os as _os
    if _os.environ.get("HCOM_TEST_NO_UNREAD") == "1":
        print(json.dumps([]))
    else:
        intent = "request" if _os.environ.get("HCOM_TEST_CHECK_REQUEST", "1") == "1" else "inform"
        print(json.dumps([{"from": "sol", "message": "hello", "intent": intent}]))
elif mode == "hermes-status":
    print(json.dumps({"ok": True}))
elif mode == "hermes-beforetool":
    print(json.dumps({"decision": "allow"}))
elif mode == "hermes-stop":
    print(json.dumps({"ok": True}))
else:
    print(json.dumps({}))
'''


class FakeCtx:
    def __init__(self):
        self.hooks = []

    def register_hook(self, name, fn):
        self.hooks.append((name, fn))


class HcomPluginTest(unittest.TestCase):
    def setUp(self):
        self._saved_env = {
            k: os.environ.get(k) for k in ("HCOM_BIN", "HCOM_LAUNCHED", "HCOM_TEST_CALLLOG")
        }
        self.tmp = tempfile.TemporaryDirectory()
        self.calllog = os.path.join(self.tmp.name, "calls.jsonl")
        stub_path = os.path.join(self.tmp.name, "hcom-stub.py")
        with open(stub_path, "w") as f:
            f.write(STUB)
        os.chmod(stub_path, 0o755)
        os.environ["HCOM_BIN"] = stub_path
        os.environ["HCOM_LAUNCHED"] = "1"
        os.environ["HCOM_TEST_CALLLOG"] = self.calllog
        hermes_plugin._clear_state()
        hermes_plugin._CTX_REF = None
        hermes_plugin._LAST_WAKE_MONOTONIC = 0.0
        self._close_notify()

    def tearDown(self):
        self._close_notify()
        hermes_plugin._clear_state()
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    @staticmethod
    def _close_notify():
        srv = hermes_plugin._NOTIFY_SERVER
        if srv is not None:
            srv.close()
            hermes_plugin._NOTIFY_SERVER = None
            hermes_plugin._NOTIFY_PORT = None

    def calls(self):
        if not os.path.exists(self.calllog):
            return []
        with open(self.calllog) as f:
            return [json.loads(line) for line in f if line.strip()]

    # ── registration / manifest ───────────────────────────────────────────

    def test_register_declares_every_hook_in_the_manifest(self):
        ctx = FakeCtx()
        hermes_plugin.register(ctx)
        registered = {name for name, _ in ctx.hooks}
        self.assertEqual(
            registered,
            {"on_session_start", "pre_llm_call", "post_llm_call",
             "post_tool_call", "on_session_end", "on_session_reset"},
        )
        # Each registered callable must exist on the module (loader resolves by attr).
        for name, fn in ctx.hooks:
            self.assertIs(getattr(hermes_plugin, name), fn)

    # ── binding + one-shot bootstrap ──────────────────────────────────────

    def bind(self):
        hermes_plugin.on_session_start(session_id="ses-1")
        self.assertEqual(hermes_plugin._STATE["name"], "luna")

    def test_bootstrap_is_injected_exactly_once(self):
        self.bind()
        first = hermes_plugin.pre_llm_call(session_id="ses-1")
        self.assertIsNotNone(first)
        self.assertIn("BOOTSTRAP-PRIMER", first["context"])
        # The primer is spent: a second turn must not repeat it.
        self.assertIsNone(hermes_plugin._STATE["bootstrap"])

    def test_bootstrap_and_first_delivery_share_one_injection(self):
        self.bind()
        first = hermes_plugin.pre_llm_call(session_id="ses-1")
        # --format ran and its batch rode the same context as the primer.
        self.assertIn("BOOTSTRAP-PRIMER", first["context"])
        self.assertIn("hello from sol", first["context"])
        formats = [c for c in self.calls()
                   if c and c[0] == "hermes-read" and "--format" in c]
        self.assertEqual(len(formats), 1)

    def test_sessionstart_failure_leaves_state_unbound(self):
        os.environ["HCOM_BIN"] = "/bin/false"
        hermes_plugin.on_session_start(session_id="ses-1")
        self.assertIsNone(hermes_plugin._STATE["name"])
        self.assertIsNone(hermes_plugin.pre_llm_call(session_id="ses-1"))

    def test_ordinary_session_binds_without_hcom_launched(self):
        # A normally started, plugin-enabled session has no HCOM_LAUNCHED:
        # the backend registers it as a vanilla identity (the session id is
        # the identity key — same RPC, same bootstrap).
        del os.environ["HCOM_LAUNCHED"]
        self.bind()
        starts = [c for c in self.calls() if c and c[0] == "hermes-sessionstart"]
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0][starts[0].index("--session-id") + 1], "ses-1")

    def test_ordinary_session_receives_bootstrap_via_pre_llm(self):
        # The primer arrives through pre_llm_call injection (hook-delivery
        # bootstrap), not the ad-hoc manual channel.
        del os.environ["HCOM_LAUNCHED"]
        self.bind()
        first = hermes_plugin.pre_llm_call(session_id="ses-1")
        self.assertIsNotNone(first)
        self.assertIn("BOOTSTRAP-PRIMER", first["context"])

    def test_rebind_after_reset_uses_new_session_identity(self):
        # on_session_reset soft-finalizes; the next session start (fresh sid)
        # must issue a second sessionstart and receive a fresh primer.
        self.bind()
        hermes_plugin.on_session_reset(session_id="ses-1")
        hermes_plugin.on_session_start(session_id="ses-2")
        self.assertEqual(hermes_plugin._STATE["session_id"], "ses-2")
        self.assertEqual(hermes_plugin._STATE["name"], "luna")
        starts = [c for c in self.calls() if c and c[0] == "hermes-sessionstart"]
        self.assertEqual(len(starts), 2)
        self.assertEqual(starts[1][starts[1].index("--session-id") + 1], "ses-2")
        first = hermes_plugin.pre_llm_call(session_id="ses-2")
        self.assertIn("BOOTSTRAP-PRIMER", first["context"])

    # ── delivered-batch-only ack ──────────────────────────────────────────

    def test_ack_uses_the_delivered_batch_cursor(self):
        self.bind()
        text = hermes_plugin._drain_messages("luna")
        self.assertIn("hello from sol", text)
        acks = [c for c in self.calls()
                if c and c[0] == "hermes-read" and "--ack" in c]
        self.assertEqual(len(acks), 1)
        # The stub's --format reports up_to=7 while newer unread events (9)
        # exist: acknowledging anything above 7 would drop the overflow.
        self.assertEqual(acks[0][acks[0].index("--up-to") + 1], "7")

    def test_empty_batch_is_not_acked(self):
        self.bind()
        # Point HCOM_BIN at a stub that returns an empty document.
        stub = os.path.join(self.tmp.name, "hcom-empty.py")
        with open(stub, "w") as f:
            f.write('#!/usr/bin/env python3\nimport json\nprint(json.dumps({"context":"","up_to":0,"count":0}))\n')
        os.chmod(stub, 0o755)
        os.environ["HCOM_BIN"] = stub
        self.assertIsNone(hermes_plugin._drain_messages("luna"))
        acks = [c for c in self.calls() if c and c[:2] == ["hermes-read", "--ack"]]
        self.assertEqual(acks, [])

    def test_non_json_stdout_is_rejected(self):
        stub = os.path.join(self.tmp.name, "hcom-noise.py")
        with open(stub, "w") as f:
            f.write('#!/usr/bin/env python3\nprint("<hcom>not json</hcom>")\n')
        os.chmod(stub, 0o755)
        os.environ["HCOM_BIN"] = stub
        self.assertIsNone(hermes_plugin._hcom(["hermes-read", "--name", "luna"]))

    def test_empty_stdout_parses_to_empty_dict(self):
        stub = os.path.join(self.tmp.name, "hcom-silent.py")
        with open(stub, "w") as f:
            f.write("#!/usr/bin/env python3\n")
        os.chmod(stub, 0o755)
        os.environ["HCOM_BIN"] = stub
        self.assertEqual(hermes_plugin._hcom(["hermes-status", "--name", "luna"]), {})

    # ── status / lifecycle ────────────────────────────────────────────────

    def test_post_llm_call_restores_listening(self):
        self.bind()
        hermes_plugin.post_llm_call(session_id="ses-1")
        status = [c for c in self.calls() if c and c[0] == "hermes-status"]
        self.assertEqual(len(status), 1)
        self.assertEqual(status[0][status[0].index("--status") + 1], "listening")

    def test_delivery_marks_status_active(self):
        self.bind()
        hermes_plugin.pre_llm_call(session_id="ses-1")
        status = [c for c in self.calls() if c and c[0] == "hermes-status"]
        self.assertTrue(status)
        self.assertEqual(status[-1][status[-1].index("--status") + 1], "active")

    def test_post_tool_call_reports_tool_activity(self):
        self.bind()
        hermes_plugin.post_tool_call(tool_name="terminal", args={"command": "ls"})
        calls = [c for c in self.calls() if c and c[0] == "hermes-beforetool"]
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][calls[0].index("--tool") + 1], "terminal")
        payload = json.loads(calls[0][calls[0].index("--input-json") + 1])
        self.assertEqual(payload, {"command": "ls"})

    def test_session_reset_soft_finalizes_and_clears_state(self):
        self.bind()
        hermes_plugin.on_session_reset(session_id="ses-1")
        self.assertIsNone(hermes_plugin._STATE["name"])
        self.assertIsNone(hermes_plugin._STATE["session_id"])
        self.assertIsNone(hermes_plugin._STATE["bootstrap"])
        stops = [c for c in self.calls() if c and c[0] == "hermes-stop"]
        self.assertEqual(len(stops), 1)
        self.assertIn("--soft", stops[0])
        self.assertEqual(stops[0][stops[0].index("--reason") + 1], "reset")

    def test_session_end_keeps_binding_between_turns(self):
        self.bind()
        hermes_plugin.on_session_end(session_id="ses-1", platform="tui")
        stops = [c for c in self.calls() if c and c[0] == "hermes-stop"]
        self.assertEqual(stops, [])
        self.assertEqual(hermes_plugin._STATE["name"], "luna")

    # ── notify listener reuse (no socket leak per rebind) ─────────────────

    def test_notify_listener_is_reused_across_rebinds(self):
        port1 = hermes_plugin._start_notify_listener()
        port2 = hermes_plugin._start_notify_listener()
        self.assertEqual(port1, port2)
        # The port actually accepts the connect-drop poke hcom performs.
        probe = socket.create_connection(("127.0.0.1", port1), timeout=2)
        probe.close()
        self.bind()
        hermes_plugin.on_session_reset(session_id="ses-1")
        hermes_plugin.on_session_start(session_id="ses-2")
        port3 = hermes_plugin._start_notify_listener()
        self.assertEqual(port3, port1, "rebind must not leak a second listener")

    def test_hcom_rpc_missing_binary_returns_none(self):
        os.environ["HCOM_BIN"] = os.path.join(self.tmp.name, "does-not-exist")
        self.assertIsNone(hermes_plugin._hcom(["hermes-status", "--name", "luna"]))

    # ── wake-on-message ────────────────────────────────────────────────────

    def test_wake_ping_injects_when_unread(self):
        class RecordingCtx(FakeCtx):
            def __init__(self):
                super().__init__()
                self.injected = []

            def inject_message(self, content, role="user", *, session_key=None):
                self.injected.append((content, session_key))
                return True

        ctx = RecordingCtx()
        hermes_plugin.register(ctx)
        self.bind()
        hermes_plugin._handle_wake_ping()
        self.assertEqual(len(ctx.injected), 1)
        content, key = ctx.injected[0]
        self.assertEqual(content, "<hcom>")
        self.assertEqual(key, "ses-1")
        # The unread query ran before the inject (filter step).
        check_calls = [
            c for c in self.calls()
            if c[:1] == ["hermes-read"] and "--format" not in c and "--ack" not in c
        ]
        self.assertEqual(len(check_calls), 1, f"calls: {self.calls()}")

    def test_wake_ping_without_unread_injects_nothing(self):
        class NoUnreadCtx(FakeCtx):
            def __init__(self):
                super().__init__()
                self.injected = []

            def inject_message(self, content, role="user", *, session_key=None):
                self.injected.append((content, session_key))
                return True

        # Replace the stub with an empty-unread variant for this test.
        no_unread_stub = STUB.replace(
            'if _os.environ.get("HCOM_TEST_NO_UNREAD") == "1":',
            'if True:')
        stub_path = os.path.join(self.tmp.name, "hcom-nounread.py")
        with open(stub_path, "w") as f:
            f.write(no_unread_stub)
        os.chmod(stub_path, 0o755)
        os.environ["HCOM_BIN"] = stub_path

        ctx = NoUnreadCtx()
        hermes_plugin.register(ctx)
        self.bind()
        hermes_plugin._handle_wake_ping()
        self.assertEqual(ctx.injected, [])

    def test_wake_ping_debounces(self):
        class RecordingCtx(FakeCtx):
            def __init__(self):
                super().__init__()
                self.injected = []

            def inject_message(self, content, role="user", *, session_key=None):
                self.injected.append((content, session_key))
                return True

        ctx = RecordingCtx()
        hermes_plugin.register(ctx)
        self.bind()
        hermes_plugin._handle_wake_ping()
        hermes_plugin._handle_wake_ping()
        hermes_plugin._handle_wake_ping()
        self.assertEqual(len(ctx.injected), 1, "second/third ping within interval must be dropped")

    def test_wake_ping_without_binding_is_a_noop(self):
        class ExplodingCtx(FakeCtx):
            def inject_message(self, content, role="user", *, session_key=None):
                raise AssertionError("inject must not run unbound")

        ctx = ExplodingCtx()
        hermes_plugin.register(ctx)
        hermes_plugin._handle_wake_ping()  # must not raise

    def test_wake_ping_unbound_ctx_is_a_noop(self):
        self.bind()
        hermes_plugin._handle_wake_ping()  # no ctx captured: must not raise

    def test_wake_skipped_for_inform_only_mail(self):
        class RecordingCtx(FakeCtx):
            def __init__(self):
                super().__init__()
                self.injected = []

            def inject_message(self, content, role="user", *, session_key=None):
                self.injected.append((content, session_key))
                return True

        os.environ["HCOM_TEST_CHECK_REQUEST"] = "0"  # inform-only inbox
        try:
            ctx = RecordingCtx()
            hermes_plugin.register(ctx)
            self.bind()
            hermes_plugin._handle_wake_ping()
            self.assertEqual(ctx.injected, [], "inform-only mail must not wake")
        finally:
            os.environ.pop("HCOM_TEST_CHECK_REQUEST", None)

    def test_register_captures_ctx_for_wake(self):
        ctx = FakeCtx()
        hermes_plugin.register(ctx)
        self.assertIs(hermes_plugin._CTX_REF, ctx)


if __name__ == "__main__":
    unittest.main()
