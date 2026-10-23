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
progress, use one `events stream --follow <name> --compact --heartbeat 15
--after-id <cursor>` observer. Use bounded waits instead of sleep-based status
polls, and propagate a failed wait's status to the caller.

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
