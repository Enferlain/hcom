#!/usr/bin/env python3
"""Stream worker progress and collect one correlated result without relaunching waits."""

import argparse
import json
import signal
import subprocess
import sys
import tempfile


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def stop(process):
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
    process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True)
    parser.add_argument("--thread", required=True)
    parser.add_argument("--after-id", required=True, type=int)
    parser.add_argument("--timeout", default=1500, type=positive)
    parser.add_argument("--compact", action="store_true",
                        help="Use the compact activity projection (omits message events)")
    parser.add_argument("--heartbeat", type=positive,
                        help="Quiet heartbeat seconds in --compact mode (default: 15)")
    parser.add_argument("--name")
    args = parser.parse_args()
    if args.after_id < 0:
        parser.error("--after-id must be nonnegative")
    if args.heartbeat is not None and not args.compact:
        parser.error("--heartbeat requires --compact")

    identity = ["--name", args.name] if args.name else []
    stream = waiter = None
    interrupted = None

    def interrupt(signum, _frame):
        # Finish recording child ownership before handling a launch-time signal.
        nonlocal interrupted
        interrupted = signum

    previous = {s: signal.signal(s, interrupt) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        # A file avoids blocking a large final report on an unread pipe while
        # progress is streamed directly to the caller's stdout.
        with tempfile.TemporaryFile() as result_file:
            stream_args = ["hcom", "events", "stream", "--follow", args.worker,
                           "--after-id", str(args.after_id)]
            if args.compact:
                stream_args += ["--compact", "--heartbeat", str(args.heartbeat or 15)]
            stream = subprocess.Popen([*stream_args, *identity])
            if interrupted:
                return 128 + interrupted
            waiter = subprocess.Popen(
                ["hcom", "events", "--wait", str(args.timeout),
                 "--after-id", str(args.after_id), "--thread", args.thread,
                 "--result-from", args.worker, *identity],
                stdout=result_file,
            )
            while not interrupted:
                try:
                    rc = waiter.wait(timeout=0.2)
                    break
                except subprocess.TimeoutExpired:
                    stream_rc = stream.poll()
                    if stream_rc is not None and stream_rc != 0:
                        print(f"Progress observer failed (exit {stream_rc})", file=sys.stderr)
                        return 2
                    # A normal stop boundary can precede result recovery. Let
                    # the correlated waiter classify it; never infer success.
            else:
                return 128 + interrupted

            stop(stream)
            result_file.seek(0)
            try:
                payload = json.load(result_file)
            except (ValueError, UnicodeError) as error:
                print(f"Result wait returned no valid JSON (exit {rc}): {error}", file=sys.stderr)
                return rc if rc > 0 else 2
            print(json.dumps({"kind": "result", "exit_code": rc, "payload": payload}), flush=True)
            return rc if rc >= 0 else 128 - rc
    except (OSError, BrokenPipeError) as error:
        print(f"Observer error: {error}", file=sys.stderr)
        return 2
    finally:
        # Only observation processes belong to this helper. Leave the worker
        # available for questions, fixes, and later assignments.
        stop(waiter)
        stop(stream)
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    sys.exit(main())
