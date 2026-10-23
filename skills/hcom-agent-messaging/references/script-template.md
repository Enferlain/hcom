# Writing hcom workflow scripts

Use Bash scripts for shell orchestration or Python's `subprocess.run` with
argument lists for more structured state. Keep command exit statuses, parse
JSON output, and release only the workers the script created.

## Run the examples

The six `.sh` examples require Bash, Python 3, and this checkout's hcom on
`PATH`. They source `workflow-common.bash` from the same directory. Keep that
file alongside them when copying examples into the user script directory:

```bash
script_dir="${HCOM_DIR:-$HOME/.hcom}/scripts"
mkdir -p "$script_dir"
# Run from the skill's directory; copies examples and their shared helper.
cp references/scripts/*.sh references/scripts/workflow-common.bash "$script_dir/"
hcom run basic-messaging "Count from one to five"
```

User `.sh` and `.py` scripts are discovered automatically; line 2 is their
description in `hcom run`. The `.bash` helper is not a workflow entrypoint.
User scripts shadow bundled scripts with the same name.

Examples accept one quoted task and an optional `--name <caller>` (which hcom
can inject). Run from the intended repository: workers receive its absolute
working directory. They use the active caller when available, otherwise the
external `bigboss` sender. An invalid explicit caller fails instead of silently
switching identities. They launch background workers with waiting prompts,
send the assignment only after recipients exist, then print final event JSON.

## Minimal example using the helper

```bash
#!/usr/bin/env bash
# One background worker, one correlated result, cleanup on every exit.
set -euo pipefail
source "${BASH_SOURCE[0]%/*}/workflow-common.bash"
workflow_init "Summarize the repository's test entrypoints" "$@"

launch_worker worker claude
worker=$last_worker
send_request "$worker" "Task: $task. Send exactly one final inform to @$coordinator on thread $result_thread; include findings and limitations. Keep progress on $work_thread."
wait_result "$worker" "$result_thread" 180
```

The helper implements argument-array identity forwarding, unique run IDs,
cursor capture, JSON name capture (including failed launches), and `EXIT`
cleanup with signal exit codes. Inspect [its source](scripts/workflow-common.bash)
when adapting worker ownership or retention.

## Waiting and observing

Capture `hcom events --cursor` before launching the worker for
`events --wait --after-id ... --thread ... --result-from ...`. Do not add SQL
or other filters to that correlated wait. Only the final publisher should
send `inform` on the result thread; otherwise progress can look like a result.

For an existing worker, capture a cursor before dispatch and use a fresh
result thread for each assignment, or an ordinary event wait with explicit
sender/thread/protocol filters when message matching is needed. For interactive
progress and completion together, run the supplied observer once:

```bash
python3 references/scripts/watch-result.py --worker "$worker" \
  --after-id "$cursor" --thread "$result_thread" --timeout 1500
```

It starts exactly one normal generation-following stream and one long result wait. Events flow
as JSON Lines; the last record is `{"kind":"result","exit_code":...,"payload":...}`.
The normal stream includes intermediate worker messages with their text,
thread and intent, plus activity/status events. Events have no `kind` field;
the final helper record carries the authoritative result/recovery payload.
The final report can also appear earlier as an ordinary stream event. Its exit code
is the wait's outcome, not a verdict on the report. Interrupting the helper
stops both observation commands and leaves the worker running. An observer
failure returns 2; a clean stream stop still lets result recovery finish.
Use the installed helper's absolute path when your cwd is the target repo.
Keep it in the skill folder; the example-install command above intentionally
copies only the six workflows and their Bash helper. Copying `watch-result.py`
into hcom's user script directory would also expose it in `hcom run` listings.
Approval-gated hosts may require approval for this Python command even when
direct hcom observation commands are auto-approved.

Run it directly so progress reaches the execution tool; command substitution
or `capture_output=True` hides that progress until completion. When the
execution tool yields, resume its existing session. Do not restart
this helper or repeatedly re-arm 45/60-second waits while a healthy reviewer
is running. An
expired overall deadline calls for assessment, not an automatic restart.
For observation alone, use `events stream --follow <name> --after-id <cursor>`.
Propagate a failed wait's status to the caller.

Compact display is opt-in: add `--compact` to the helper, with optional
`--heartbeat SEC` (default 15). It preserves hcom's typed `activity.type`
projection but omits all message events, so it is unsuitable when intermediate
communication matters. Heartbeats mean the observer is alive, not task progress.

Valid result-wait outcomes use 0 receipt, 1 deadline, 2 error, 3 stopped without
result, 4 blocked, 5 launch failure. Input validation can also exit 1 before
waiting; preserve diagnostics. The examples print the received payload without
calling it a successful task. Review the report, diff, and relevant validation.

## Python adaptations

Use `subprocess.run(["hcom", ...], check=False, capture_output=True, text=True)`.
Decode launch JSON and retain its `names` before checking the launch return
code. Put worker cleanup in `finally`, including on a wait error. Forward
`--name` as two separate arguments. Avoid `shell=True` and string-built command
lines for user tasks. Use a UUID for thread IDs and preserve the hcom wait's
return code when exiting.
