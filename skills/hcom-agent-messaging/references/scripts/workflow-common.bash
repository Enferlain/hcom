# Shared helpers for the example workflows; source from the same directory.

workflow_signal_traps() {
  trap 'exit 130' INT
  trap 'exit 143' TERM
}

workflow_cleanup() {
  local rc=$?
  trap - EXIT
  local worker
  for worker in ${launched_names[@]+"${launched_names[@]}"}; do
    hcom kill "$worker" --go ${name_args[@]+"${name_args[@]}"} >/dev/null 2>&1 || true
  done
  exit "$rc"
}

workflow_init() {
  local default_task=$1
  shift
  local caller=""
  task=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --name)
        [[ $# -ge 2 && -n "$2" ]] || { echo "--name requires a value" >&2; exit 2; }
        caller=$2
        shift 2 ;;
      -h|--help)
        echo "Usage: hcom run <workflow> [--name CALLER] [TASK]"
        exit 0 ;;
      --)
        shift
        [[ $# -le 1 && -z "$task" ]] || { echo "Provide one quoted task" >&2; exit 2; }
        task=${1:-}
        break ;;
      -*) echo "Unknown argument: $1" >&2; exit 2 ;;
      *)
        [[ -z "$task" ]] || { echo "Provide one quoted task" >&2; exit 2; }
        task=$1
        shift ;;
    esac
  done
  task=${task:-$default_task}
  command -v python3 >/dev/null || { echo "Python 3 is required" >&2; exit 2; }
  name_args=()
  sender_args=()
  [[ -z "$caller" ]] || name_args=(--name "$caller")
  if coordinator=$(hcom list self name ${name_args[@]+"${name_args[@]}"} 2>/dev/null); then
    [[ -n "$coordinator" ]] || { echo "Empty caller identity" >&2; exit 2; }
    name_args=(--name "$coordinator")
  elif [[ -n "$caller" ]]; then
    echo "Cannot resolve explicit caller: $caller" >&2
    exit 2
  else
    coordinator=bigboss
    sender_args=(-b)
  fi
  run_id=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')
  work_thread="work-$run_id"
  result_thread="result-$run_id"
  working_dir=$(pwd -P)
  launched_names=()
  trap workflow_cleanup EXIT
  workflow_signal_traps
  cursor=$(hcom events --cursor ${name_args[@]+"${name_args[@]}"})
  [[ "$cursor" =~ ^[0-9]+$ ]] || { echo "Invalid event cursor" >&2; exit 2; }
  final_instruction="Send exactly one final report with hcom send \"@$coordinator\" --thread \"$result_thread\" --intent inform -- \"RESULT: <outcome, evidence, limitations>\". State failure or inconclusive explicitly when appropriate. Keep progress on $work_thread."
}

# Call directly, not in $(...), so name tracking survives in the parent shell.
launch_worker() {
  local role=$1 tool=$2 output names worker launch_rc=0 launch_signal=0
  # A pending signal must not skip recording the just-started worker's name.
  trap 'launch_signal=130' INT
  trap 'launch_signal=143' TERM
  output=$(hcom 1 "$tool" --dir "$working_dir" --tag "w${run_id:0:8}$role" \
    --go --headless --json ${name_args[@]+"${name_args[@]}"} \
    --hcom-prompt "You are the $role in a bounded workflow. Wait for the coordinator's hcom request before doing work. That request specifies your report recipient and thread. End your turn now to receive it; do not poll. Stay available for follow-up until the coordinator kills this worker.") || launch_rc=$?
  if [[ -z "$output" ]]; then
    workflow_signal_traps
    echo "Launch command produced no JSON for $role (exit $launch_rc)" >&2
    [[ "$launch_signal" -eq 0 ]] || return "$launch_signal"
    [[ "$launch_rc" -eq 0 ]] || return "$launch_rc"
    return 2
  fi
  # Parse and track names even on nonzero launch status, before returning failure.
  names=$(python3 -c '
import json, sys
data = json.load(sys.stdin)
names = data["names"]
if not isinstance(names, list) or any(not isinstance(n, str) or not n or any(c.isspace() for c in n) for n in names):
    raise ValueError("invalid launch names")
print("\n".join(names))
' <<<"$output") || {
    workflow_signal_traps
    echo "Invalid launch JSON; worker ownership could not be recovered" >&2
    [[ "$launch_signal" -eq 0 ]] || return "$launch_signal"
    return 2
  }
  last_worker=""
  local count=0
  while IFS= read -r worker; do
    [[ -n "$worker" ]] || continue
    launched_names+=("$worker")
    last_worker=$worker
    count=$((count + 1))
  done <<<"$names"
  workflow_signal_traps
  [[ "$launch_signal" -eq 0 ]] || return "$launch_signal"
  if [[ "$launch_rc" -ne 0 && "$launch_rc" -ne 2 ]]; then
    echo "Launch failed for $role (exit $launch_rc)" >&2
    return "$launch_rc"
  fi
  [[ "$count" -eq 1 ]] || { echo "Expected one $role worker" >&2; return 2; }
  if [[ "$launch_rc" -eq 2 ]]; then
    echo "Launch readiness pending or blocked; the result wait will report the outcome." >&2
  fi
  echo "$role: $last_worker" >&2
}

send_request() {
  local target=$1 message=$2
  hcom send "@$target" ${name_args[@]+"${name_args[@]}"} ${sender_args[@]+"${sender_args[@]}"} \
    --thread "$work_thread" --intent request --quiet -- "$message"
}

wait_result() {
  local worker=$1 thread=$2 seconds=$3 rc=0
  last_result=$(hcom events --wait "$seconds" --after-id "$cursor" --thread "$thread" \
    --result-from "$worker" ${name_args[@]+"${name_args[@]}"}) || rc=$?
  printf '%s\n' "$last_result"
  if [[ "$rc" -ne 0 ]]; then
    echo "Result wait failed (exit $rc): $worker" >&2
    return "$rc"
  fi
  echo "Result received; review its contents and evidence." >&2
}
