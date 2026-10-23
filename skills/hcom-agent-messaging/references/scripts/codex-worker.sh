#!/usr/bin/env bash
# Codex implements a task; Claude checks its changes and reports the outcome.
set -euo pipefail
source "${BASH_SOURCE[0]%/*}/workflow-common.bash"
workflow_init "Summarize this repository's test entrypoints without modifying files" "$@"

launch_worker reviewer claude
reviewer=$last_worker
launch_worker coder codex
coder=$last_worker

send_request "$reviewer" "Review @$coder for task: $task. Wait for their report on $work_thread by ending your turn. Inspect the actual changes and relevant validation; use hcom transcript @$coder --full --detailed for recorded context if available. State pass, fail, or inconclusive with evidence. $final_instruction"
send_request "$coder" "Do this bounded task: $task. Run the relevant checks and report changes, evidence, and limitations via hcom send \"@$reviewer\" --thread \"$work_thread\" --intent inform -- \"CODE REPORT: <outcome and evidence>\". Then end your turn and stay available."
wait_result "$reviewer" "$result_thread" 240
