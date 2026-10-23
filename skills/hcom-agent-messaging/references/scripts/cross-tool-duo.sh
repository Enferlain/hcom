#!/usr/bin/env bash
# Claude designs and assesses a specification implemented by Codex.
set -euo pipefail
source "${BASH_SOURCE[0]%/*}/workflow-common.bash"
workflow_init "Implement a function that adds two numbers and check it" "$@"

launch_worker engineer codex
engineer=$last_worker
launch_worker architect claude
architect=$last_worker

send_request "$engineer" "Wait for a SPEC from @$architect on thread $work_thread. Implement only that bounded spec and run relevant checks. Send your outcome and evidence to @$architect with intent inform on $work_thread. End your turn to wait for the spec."
send_request "$architect" "Design a concise bounded spec for: $task. Send it via hcom send \"@$engineer\" --thread \"$work_thread\" --intent request -- \"SPEC: <spec>\". Wait for their report by ending your turn. Inspect the resulting diff and evidence; explicitly state any unresolved issue. $final_instruction"
wait_result "$architect" "$result_thread" 240
