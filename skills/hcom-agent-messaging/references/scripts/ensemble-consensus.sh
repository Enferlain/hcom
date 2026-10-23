#!/usr/bin/env bash
# Three independent answers are collected by a judge before a final verdict.
set -euo pipefail
source "${BASH_SOURCE[0]%/*}/workflow-common.bash"
workflow_init "What is 17 times 23? Explain the calculation" "$@"

launch_worker judge claude
judge=$last_worker
contestants=()
for i in 1 2 3; do
  launch_worker "c$i" claude
  contestants+=("$last_worker")
done

send_request "$judge" "Collect one independent answer from each of ${contestants[*]} on thread $work_thread for task: $task. Count distinct senders; do not report until all three answered. Assess evidence and disagreements. $final_instruction End your turn while waiting; do not poll."
for contestant in "${contestants[@]}"; do
  send_request "$contestant" "Answer independently without reading peer transcripts: $task. Send your answer and reasoning via hcom send \"@$judge\" --thread \"$work_thread\" --intent inform -- \"ANSWER: <answer and reasoning>\". Then end your turn."
done
wait_result "$judge" "$result_thread" 180
