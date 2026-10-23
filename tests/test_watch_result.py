"""Exercise the skill observer as a process, including child cleanup and streaming."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


HELPER = Path(__file__).resolve().parents[1] / "skills/hcom-agent-messaging/references/scripts/watch-result.py"
FAKE_HCOM = r'''#!/usr/bin/env python3
import json, os, sys, time
from pathlib import Path
args = sys.argv[1:]
with open(os.environ['CALLS'], 'a') as f:
    f.write(json.dumps({'pid': os.getpid(), 'args': args}) + '\n')
assert args[0] == 'events', args
mode = os.environ.get('MODE', '')
if args[1] == 'stream':
    if '--compact' in args:
        record = {'schema_version': 1, 'activity': {'type': 'heartbeat', 'phase': 'active'}}
    else:
        record = {'id': 50178, 'type': 'message', 'data': {'from': 'volu', 'intent': 'inform', 'thread': 'progress', 'text': 'Review progress: checking lifetimes'}}
    print(json.dumps(record), flush=True)
    Path(os.environ['READY']).touch()
    if mode == 'stream-error': sys.exit(2)
    if mode == 'stopped': sys.exit(0)
    time.sleep(30)
    sys.exit(0)
assert args[1] == '--wait', args
while not Path(os.environ['READY']).exists(): time.sleep(.01)
time.sleep(.4)
if mode == 'hang': time.sleep(30)
if mode == 'bad-json':
    print('not JSON')
    sys.exit(0)
rc = int(os.environ.get('RESULT_RC', '0'))
payload = {'data': {'text': 'FAILED: review found a bug'}, 'outcome': 'received' if rc == 0 else 'terminal'}
if mode == 'large': payload['data']['text'] += 'x' * 2_000_000
print(json.dumps(payload), flush=True)
sys.exit(rc)
'''


class WatchResultTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="hcom-watch-test-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        binary = self.root / "hcom"
        binary.write_text(FAKE_HCOM)
        binary.chmod(0o755)
        self.calls = self.root / "calls"
        self.env = dict(os.environ, PATH=f"{self.root}:{os.environ['PATH']}",
                        CALLS=str(self.calls), READY=str(self.root / "ready"))
        self.command = [sys.executable, str(HELPER), "--worker", "volu", "--thread", "review-unique",
                        "--after-id", "50177", "--timeout", "1500", "--name", "caller"]

    def recorded(self):
        return [json.loads(line) for line in self.calls.read_text().splitlines()] if self.calls.exists() else []

    def assert_children_reaped(self):
        for call in self.recorded():
            with self.assertRaises(ProcessLookupError):
                os.kill(call["pid"], 0)

    def run_observer(self, mode="", rc=0):
        result = subprocess.run(self.command, env=dict(self.env, MODE=mode, RESULT_RC=str(rc)),
                                capture_output=True, text=True, timeout=8)
        self.assert_children_reaped()
        return result

    def test_one_stream_and_one_wait_with_correlated_arguments(self):
        result = self.run_observer()
        self.assertEqual(result.returncode, 0, result.stderr)
        records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(records[0]["type"], "message")
        self.assertEqual(records[0]["data"]["text"], "Review progress: checking lifetimes")
        self.assertEqual(records[-1]["kind"], "result")
        self.assertEqual(records[-1]["exit_code"], 0)
        self.assertIn("FAILED", records[-1]["payload"]["data"]["text"])
        calls = self.recorded()
        self.assertEqual(len(calls), 2)
        for call in calls:
            args = call["args"]
            self.assertEqual(args[args.index("--after-id") + 1], "50177")
            self.assertEqual(args[args.index("--name") + 1], "caller")
        wait_args = next(c["args"] for c in calls if "--wait" in c["args"])
        stream_args = next(c["args"] for c in calls if "stream" in c["args"])
        self.assertNotIn("--compact", stream_args)
        self.assertNotIn("--heartbeat", stream_args)
        self.assertEqual(wait_args[wait_args.index("--wait") + 1], "1500")
        self.assertEqual(wait_args[wait_args.index("--thread") + 1], "review-unique")
        self.assertEqual(wait_args[wait_args.index("--result-from") + 1], "volu")

    def test_compact_is_explicit_and_keeps_its_heartbeat_option(self):
        self.command.extend(["--compact", "--heartbeat", "2"])
        result = self.run_observer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout.splitlines()[0])["activity"]["type"], "heartbeat")
        args = next(c["args"] for c in self.recorded() if "stream" in c["args"])
        self.assertIn("--compact", args)
        self.assertEqual(args[args.index("--heartbeat") + 1], "2")

    def test_heartbeat_without_compact_is_rejected_before_spawning(self):
        self.command.extend(["--heartbeat", "2"])
        result = self.run_observer()
        self.assertEqual(result.returncode, 2)
        self.assertIn("--heartbeat requires --compact", result.stderr)
        self.assertFalse(self.calls.exists())

    def test_all_terminal_wait_statuses_are_preserved_without_rearming(self):
        for rc in range(1, 6):
            with self.subTest(rc=rc):
                self.calls.unlink(missing_ok=True)
                result = self.run_observer(rc=rc)
                self.assertEqual(result.returncode, rc, result.stderr)
                self.assertEqual(json.loads(result.stdout.splitlines()[-1])["exit_code"], rc)
                self.assertEqual(len(self.recorded()), 2)

    def test_large_report_does_not_block_on_unread_pipe(self):
        result = self.run_observer(mode="large")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreater(len(json.loads(result.stdout.splitlines()[-1])["payload"]["data"]["text"]), 2_000_000)

    def test_stopped_stream_still_collects_missing_result_outcome(self):
        result = self.run_observer(mode="stopped", rc=3)
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertEqual(json.loads(result.stdout.splitlines()[-1])["exit_code"], 3)

    def test_observer_failure_stops_waiter(self):
        result = self.run_observer(mode="stream-error")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("Progress observer failed", result.stderr)

    def test_malformed_result_is_an_error(self):
        result = self.run_observer(mode="bad-json")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("no valid JSON", result.stderr)

    def test_progress_arrives_before_result_and_interrupt_reaps_observers(self):
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signum=signum):
                self.calls.unlink(missing_ok=True)
                process = subprocess.Popen(self.command, env=dict(self.env, MODE="hang"),
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    self.assertEqual(json.loads(process.stdout.readline())["type"], "message")
                    deadline = time.monotonic() + 2
                    while len(self.recorded()) < 2 and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertEqual(len(self.recorded()), 2)
                    self.assertIsNone(process.poll())
                    process.send_signal(signum)
                    process.communicate(timeout=8)
                    self.assertEqual(process.returncode, 128 + signum)
                    self.assert_children_reaped()
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate()

    def test_invalid_deadline_does_not_start_processes(self):
        result = subprocess.run([*self.command, "--timeout", "0"], env=self.env,
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
