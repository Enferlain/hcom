#!/usr/bin/env bash
# Receive a plan, then give its full report and transcript to an executor.
set -euo pipefail
source "${BASH_SOURCE[0]%/*}/workflow-common.bash"
workflow_init "List five prime numbers and explain how you checked them" "$@"

plan_thread="plan-$run_id"
launch_worker planner claude
planner=$last_worker
send_request "$planner" "Write a concise plan for: $task. Send one final inform to @$coordinator on thread $plan_thread with the full plan and limitations; keep progress on $work_thread. Then end your turn."
wait_result "$planner" "$plan_thread" 120
# Recovery can return a report whose id belongs to a stop event, not stored text.
plan_report=$last_result

# Each stage uses a boundary from before its own worker creation.
cursor=$(hcom events --cursor ${name_args[@]+"${name_args[@]}"})
launch_worker executor claude
executor=$last_worker
send_request "$executor" "Task: $task. Here is the full planner report, including any recovery provenance: $plan_report. Assess the plan within the task's scope. For further context inspect hcom transcript @$planner --full --detailed if available. Execute the bounded plan when valid and report any blocker or failed planning outcome. Validate the outcome. $final_instruction"
wait_result "$executor" "$result_thread" 180
