#!/usr/bin/env bash
# Worker sends an answer to a reviewer, who reports to the coordinator.
set -euo pipefail
source "${BASH_SOURCE[0]%/*}/workflow-common.bash"
workflow_init "Count from one to five" "$@"

launch_worker reviewer claude
reviewer=$last_worker
launch_worker worker claude
worker=$last_worker

send_request "$reviewer" "Review the answer that will arrive from @$worker on thread $work_thread for this task: $task. Respond to the worker with inform, not ack (their report is inform). $final_instruction End your turn while waiting; do not stop before the answer arrives."
send_request "$worker" "Do this bounded task: $task. Send your answer via hcom send \"@$reviewer\" --thread \"$work_thread\" --intent inform -- \"ANSWER: <answer and evidence>\". Then end your turn and remain available."
wait_result "$reviewer" "$result_thread" 180
