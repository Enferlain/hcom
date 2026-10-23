#!/usr/bin/env bash
# Worker and reviewer exchange at most three rounds before a final report.
set -euo pipefail
source "${BASH_SOURCE[0]%/*}/workflow-common.bash"
workflow_init "Count from one to ten in Turkish" "$@"

launch_worker reviewer claude
reviewer=$last_worker
launch_worker worker claude
worker=$last_worker

send_request "$reviewer" "Review @$worker on thread $work_thread for task: $task. For each ROUND N report, check the result and relevant evidence. Reply via hcom send \"@$worker\" --thread \"$work_thread\" with inform APPROVED when correct, or request FIX: <issues> when wrong. Stay available through all rounds. After round 3, send inform LIMIT: <remaining issues> if still wrong. End your turn between messages; do not poll."
send_request "$worker" "Task: $task. Send ROUND 1: <result and evidence> to @$reviewer with intent inform on thread $work_thread. On FIX, correct it and send the next round, up to 3 rounds total. End your turn while awaiting feedback. After APPROVED, or LIMIT after round 3, report the actual outcome to the coordinator. $final_instruction"
wait_result "$worker" "$result_thread" 240
