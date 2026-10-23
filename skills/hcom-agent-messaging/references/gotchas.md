# Messaging and workflow gotchas

## Missing or unwanted delivery

Inspect `hcom list`, the relevant launch outcome, and a small event slice.
Check exact recipients, `mentions`/`delivered_to`, worker status, and integration
binding. `hcom term <name> --clean` can expose a trust or approval prompt after
a plausible stall. A wait for idle is a status observation, not proof that a
task completed or a missing integration recovered.

Local `@name` and `@tag-name` targets match exact names. `@tag-` intentionally
addresses a whole tag group. Avoid generic role tags for one worker when
multiple workflows may be running; capture and use its actual name.

Threads seed and reuse routing membership. They do not require a receiver to
subscribe to a matching thread, do not hide other addressed messages, and do
not provide an access-control boundary. Use explicit targets and distinct run
IDs to keep workflows separate.

```bash
hcom send @actual-worker --thread unique-run --intent request -- "Task"
hcom send --thread unique-run --intent inform -- "Update to existing members"
hcom send --broadcast --intent inform -- "Explicit team-wide announcement"
```

A thread with no members must be seeded with targets. An unthreaded message
without targets is rejected unless `--broadcast` is explicit. A later
thread-only send can reach all accumulated members; use an exact target when
only one agent should receive it.

## Missed or false completion signals

An ordinary wait without `--after-id` starts at the current cursor. If the
worker already replied, that wait can time out. Capture the cursor before
launch for a correlated result wait, or before dispatch for an ordinary event
wait. Do not capture it after sending the request.

```bash
cursor=$(hcom events --cursor)
# Launch a worker; capture its exact name and assign the task.
hcom events --wait 180 --after-id "$cursor" --thread "$result_thread" --result-from "$worker"
```

`--result-from` requires `--after-id` and exactly one thread; capture the
cursor before launch for a new worker. It cannot be combined with SQL or other event filters.
It accepts an `inform` from the exact worker generation. Progress `inform`
messages on the same thread can therefore satisfy it early. Reserve that
thread for the final message, and use another for progress and peer feedback.
For supported stopped providers, a recovered result includes provenance;
inspect it as evidence, not automatic validation of the task.

For an already-running worker, capture the cursor before dispatch and use a
fresh result thread for each assignment. Generation correlation identifies
the worker attempt, not the task within that session. Use an ordinary
sender/thread/protocol filter when you need more specific message matching;
that lacks `--result-from`'s generation correlation. Avoid broad `LIKE '%DONE%'`
queries: requests and progress can quote the same marker. SQLite `LIKE` is
case-insensitive for ASCII; it does not understand protocol meaning.

## Preserve wait failures

Do not respond to a quiet reviewer by repeatedly issuing short result waits,
`list`, or transcript commands. Use [watch-result.py](scripts/watch-result.py)
for one task-sized wait with streamed progress. The deadline payload's
re-arm example is recovery guidance, not a polling loop. Keep the same host
execution session when the tool yields; avoid commentary that only repeats
that the review is still running.

For a valid correlated wait, outcomes use 0 result, 1 deadline, 2 error,
3 stopped without result, 4 blocked, 5 launch failure. Plain SQL waits use
0 match, 1 timeout, 2 error. Input/correlation validation can fail before the
wait starts with exit 1, such as combining `--result-from` with SQL. Read the
diagnostics instead of classifying every exit 1 as a timeout. Under `set -e`,
capture a nonzero status in an `if` or `||` clause:

```bash
rc=0
hcom events --wait 180 --after-id "$cursor" --thread "$result_thread" \
  --result-from "$worker" || rc=$?
case "$rc" in
  0) echo "Result received; inspect its content" ;;
  *) echo "Worker wait failed (exit $rc)" >&2; exit "$rc" ;;
esac
```

`command && echo PASS || echo FAIL` ends with a successful `echo`, masking a
failed command from the caller. A received failure report also exits 0: the
message's claims still need review and verification.

## Intent and replies

`request` asks for a response; `inform` invites one only if useful; `ack`
asks for no response. These are bootstrap conventions, not guarantees of
model behavior. An acknowledgement requires a reply link to a request:

```bash
hcom send @actual-sender --reply-to 42 --intent ack -- "Received"
```

Use the real incoming request ID. For a reply to an ordinary information
message, use `inform`; do not invent an ID to satisfy acknowledgement checks.
A final report fulfills a request for a response; an immediate ack is optional.
An `ack` does not satisfy an `inform`-only result wait, so it may acknowledge
the request on its result thread without falsely completing that wait. The
restriction is on extra progress `inform` messages, not all message intents.

## Launch and cleanup

In a script, `--go` skips the launch preview; `--headless` requests a background
worker. Neither removes tool approval or workspace-trust prompts. Claude's
default headless backend is a live PTY; print mode requires explicit `-p` or
`--print`.

Launch recipients before dispatching work to senders. For multi-worker
protocols, first create all workers with waiting prompts, then send the first
request using their actual names. Do not let an intermediate worker stop if
it must handle feedback in later rounds. Bound feedback rounds and deadlines.

Parse the `names` array from `--json` launch output. A single launch can exit 2
after its inline readiness window while still launching or blocked. With a
valid worker name, the examples continue to the bounded result wait, which
reports the final outcome. Other nonzero launch exits abort the workflow.
Retain any returned names even on failure, so partial launches are cleaned up. With
human-readable output, parse the `Names:` line instead of assuming names or
removing whitespace from a batch of names.

Use an `EXIT` cleanup trap and signal traps that exit with the signal status.
An `ERR` trap alone misses explicit `exit` and some conditional failures.
Kill only workers owned by the script. `hcom stop` disconnects participation;
`hcom kill` also terminates the worker and attempts to close its pane. Report
retained workers explicitly when a workflow deliberately keeps them alive.

## Setup and state roots

Per-launch tools get their integration through `hcom <tool>`. For persistent
tools, inspect `hcom hooks status <tool>` before changing settings. Bare
`hooks add` selects the detected current tool, or all outside one; explicit
`hooks add all` skips Hermes, which needs profile-specific setup.

`HCOM_DIR` redirects only hcom state; it does not relocate tool configuration
or persistent hooks.
Participants on different state roots do not share a local database. Reset
archives and clears state; diagnose delivery before choosing that operation.
