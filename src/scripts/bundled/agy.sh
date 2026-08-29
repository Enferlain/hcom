#!/usr/bin/env bash
# Launch an Antigravity worker and return one isolated completion report.
set -euo pipefail

readonly DEFAULT_MODEL="gemini-3.7-flash-high"
readonly DEFAULT_EFFORT="high"
readonly DEFAULT_TIMEOUT_SECONDS=1800
readonly DEFAULT_HEARTBEAT_SECONDS=120
readonly IDLE_NUDGE_SECONDS=240

launched_names=()

track_launch() {
  local output="$1"
  local names
  names=$(printf '%s\n' "$output" | sed -n 's/^Names: //p' | head -n 1)
  local name
  for name in $names; do
    launched_names+=("$name")
  done
}

cleanup() {
  if [[ ${#launched_names[@]} -eq 0 ]]; then
    return
  fi
  local name
  for name in "${launched_names[@]}"; do
    hcom kill "$name" >/dev/null 2>&1 || true
  done
}

print_heartbeat() {
  local worker="$1"
  local elapsed="$2"
  local worker_json status status_context status_age transcript commands edits

  worker_json=$(hcom list "$worker" --json 2>/dev/null || true)
  if [[ -z "$worker_json" ]] || ! command -v jq >/dev/null 2>&1; then
    printf 'Antigravity status — %ss elapsed — worker state unavailable.\n' "$elapsed"
    return
  fi

  status=$(jq -r '.status // "unknown"' <<<"$worker_json")
  status_context=$(jq -r '.status_context // ""' <<<"$worker_json")
  status_age=$(jq -r '.stored_status_age_seconds // "unknown"' <<<"$worker_json")
  transcript=$(jq -r '.transcript_path // empty' <<<"$worker_json")
  commands="?"
  edits="?"
  if [[ -n "$transcript" && -r "$transcript" ]]; then
    read -r commands edits < <(jq -rs '
      [
        ([.[]
          | select(.type == "RUN_COMMAND" or .type == "PLANNER_RESPONSE")
          | if .type == "RUN_COMMAND" then 1
            else [(.tool_calls? // [])[]? | objects | select(.name == "run_command")] | length
            end]
          | add // 0),
        ([.[]
          | select(.type == "CODE_ACTION" or .type == "PLANNER_RESPONSE")
          | if .type == "CODE_ACTION" then 1
            else [(.tool_calls? // [])[]? | objects
              | select((.name // "")
                | test("^(write_to_file|replace_file_content|multi_replace_file_content|replace|write|edit|apply_patch|create_file)$"; "i"))]
              | length
            end]
          | add // 0)
      ]
      | @tsv
    ' "$transcript" 2>/dev/null || printf '?\t?\n')
  fi

  if [[ -n "$status_context" ]]; then
    status="${status}/${status_context}"
  fi
  printf 'Antigravity status — %ss elapsed — %s — last change %ss ago — %s commands, %s edits.\n' \
    "$elapsed" "$status" "$status_age" "$commands" "$edits"
}

caller=""
workdir="$PWD"
timeout_seconds=$DEFAULT_TIMEOUT_SECONDS
heartbeat_seconds=$DEFAULT_HEARTBEAT_SECONDS
keep_worker=0
thread=""
model="$DEFAULT_MODEL"
effort="$DEFAULT_EFFORT"
task_parts=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)
      caller="${2:?--name requires an hcom identity}"
      shift 2
      ;;
    --dir)
      workdir="${2:?--dir requires a directory}"
      shift 2
      ;;
    --timeout)
      timeout_seconds="${2:?--timeout requires seconds}"
      shift 2
      ;;
    --heartbeat)
      heartbeat_seconds="${2:?--heartbeat requires seconds}"
      shift 2
      ;;
    --thread)
      thread="${2:?--thread requires a value}"
      shift 2
      ;;
    --model)
      model="${2:?--model requires an Antigravity model identifier}"
      shift 2
      ;;
    --effort)
      effort="${2:?--effort requires low, medium, or high}"
      shift 2
      ;;
    --keep)
      keep_worker=1
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: hcom run agy [OPTIONS] TASK

Launch one Antigravity implementation worker and wait for its completion report.

Options:
  --dir PATH       Worker directory (default: current directory)
  --model MODEL    Antigravity model (default: gemini-3.7-flash-high)
  --effort LEVEL   Antigravity effort: low, medium, or high (default: high)
  --timeout SEC    Overall result wait ceiling (default: 1800)
  --heartbeat SEC  Compact status interval (default: 120)
  --thread NAME    Explicit hcom thread (default: unique generated name)
  --keep           Keep the worker alive after receiving its report
  -h, --help       Show this help

The worker uses Antigravity's accept-edits mode and the configured command/MCP
allowlist. It does not bypass permissions. On timeout it is left running and
the command needed to continue waiting is printed.
EOF
      exit 0
      ;;
    --)
      shift
      task_parts+=("$@")
      break
      ;;
    -*)
      printf 'Unknown option: %s\n' "$1" >&2
      exit 2
      ;;
    *)
      task_parts+=("$1")
      shift
      ;;
  esac
done

if [[ ! "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  printf '%s\n' '--timeout must be a positive integer' >&2
  exit 2
fi
if [[ ! "$heartbeat_seconds" =~ ^[1-9][0-9]*$ ]]; then
  printf '%s\n' '--heartbeat must be a positive integer' >&2
  exit 2
fi
if [[ ! "$effort" =~ ^(low|medium|high)$ ]]; then
  printf '%s\n' '--effort must be low, medium, or high' >&2
  exit 2
fi
if [[ ${#task_parts[@]} -eq 0 ]]; then
  printf '%s\n' 'A task is required. Run: hcom run agy --help' >&2
  exit 2
fi
if ! command -v agy >/dev/null 2>&1; then
  printf '%s\n' 'The Antigravity CLI (agy) is not available' >&2
  exit 127
fi

task="${task_parts[*]}"
requested_workdir="$workdir"
if ! workdir=$(cd -- "$requested_workdir" 2>/dev/null && pwd -P); then
  printf 'Not a directory: %s\n' "$requested_workdir" >&2
  exit 2
fi

if [[ -z "$caller" ]]; then
  caller=$(hcom list | sed -n 's/^Your name: //p' | head -n 1 || true)
fi
if [[ -z "$caller" || "$caller" == "(not participating)" ]]; then
  printf '%s\n' 'Could not resolve the calling hcom identity' >&2
  exit 2
fi

thread="${thread:-agy-$(date +%s)-$$}"

# Arm the result boundary before launch so a fast worker cannot finish during
# readiness handling and disappear behind a newly captured wait cursor.
if ! result_cursor=$(hcom events --cursor 2>&1); then
  printf 'Could not capture the pre-launch hcom event cursor: %s\n' \
    "$result_cursor" >&2
  exit 1
fi
if [[ ! "$result_cursor" =~ ^[0-9]+$ ]]; then
  printf 'Invalid pre-launch hcom event cursor: %s\n' "$result_cursor" >&2
  exit 1
fi

prompt="Work on the following bounded task in ${workdir}. Read and follow repository instructions. Preserve unrelated changes. Do not commit, push, install dependencies, delegate to other agents, or use external services unless the task explicitly authorizes it. Verify the result in proportion to risk.

Task:
${task}

When complete or blocked, send one concise completion report to @${caller} in hcom thread ${thread} with intent inform. Include what changed, files changed, verification and outcome, and remaining risks. Use your hcom identity when sending. Run the completion command with 'hcom send', not 'uvx hcom send', so the report uses the same hcom version as this workflow. Do not finish your turn without sending that report."

trap cleanup ERR
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM
trap - ERR
set +e
launch_output=$(hcom 1 agy \
  --tag agy \
  --go \
  --headless \
  --dir "$workdir" \
  --hcom-prompt "$prompt" \
  --model "$model" \
  --effort "$effort" \
  --mode accept-edits 2>&1)
launch_status=$?
set -e
trap cleanup ERR
track_launch "$launch_output"
if [[ "$launch_status" -ne 0 && "$launch_status" -ne 2 ]]; then
  printf '%s\n' "$launch_output" >&2
  cleanup
  trap - ERR INT TERM
  exit "$launch_status"
fi
printf '%s\n' "$launch_output"

if [[ ${#launched_names[@]} -ne 1 ]]; then
  printf '%s\n' 'Could not determine the launched Antigravity worker name' >&2
  cleanup
  trap - ERR INT TERM
  exit 1
fi

worker="${launched_names[0]}"
if [[ "$launch_status" -eq 2 ]]; then
  batch_id=$(printf '%s\n' "$launch_output" | sed -n 's/^Batch id: //p' | head -n 1)
  if [[ -n "$batch_id" ]]; then
    trap - ERR
    set +e
    hcom events launch "$batch_id" --timeout 60 >/dev/null 2>&1
    launch_wait_status=$?
    set -e
    trap cleanup ERR
    if [[ "$launch_wait_status" -ne 0 ]]; then
      printf '%s\n' \
        'Antigravity readiness was not confirmed; continuing to wait for the isolated completion report.' >&2
    fi
  else
    printf '%s\n' \
      'Antigravity launch is pending without a batch ID; continuing with the resolved worker identity.' >&2
  fi
fi

printf 'Antigravity worker: %s\nThread: %s\nModel: %s; effort: %s; mode: accept-edits.\n' \
  "$worker" "$thread" "$model" "$effort"

started_at=$SECONDS
deadline=$((started_at + timeout_seconds))
nudged_idle_worker=0

while (( SECONDS < deadline )); do
  remaining=$((deadline - SECONDS))
  wait_seconds=$heartbeat_seconds
  if (( remaining < wait_seconds )); then
    wait_seconds=$remaining
  fi

  trap - ERR
  set +e
  event_output=$(hcom events \
    --wait "$wait_seconds" \
    --after-id "$result_cursor" \
    --thread "$thread" \
    --result-from "$worker" \
    --name "$caller" 2>&1)
  wait_status=$?
  set -e
  trap cleanup ERR

  case "$wait_status" in
    0)
      printf '%s\n' "$event_output"
      trap - ERR INT TERM
      if [[ "$keep_worker" -eq 0 ]]; then
        cleanup
        printf 'Stopped Antigravity worker %s after receiving its report.\n' "$worker"
      else
        printf 'Antigravity worker %s remains available on thread %s.\n' "$worker" "$thread"
      fi
      printf 'Requested model and effort: %s, %s.\n' "$model" "$effort"
      exit 0
      ;;
    1)
      if [[ "$event_output" != *'"timed_out":true'* ]]; then
        printf 'Antigravity result wait failed before timeout: %s\n' \
          "$event_output" >&2
        cleanup
        trap - ERR INT TERM
        exit 1
      fi

      elapsed=$((SECONDS - started_at))
      worker_json=$(hcom list "$worker" --json 2>/dev/null || true)
      if [[ -z "$worker_json" ]]; then
        stopped_info=$(hcom list --stopped "$worker" 2>/dev/null || true)
        if [[ "$stopped_info" == Stopped:* ]]; then
          transcript=$(printf '%s\n' "$stopped_info" \
            | sed -n 's/^[[:space:]]*Transcript:[[:space:]]*//p' \
            | head -n 1)
          trap - ERR INT TERM
          printf 'Antigravity worker %s stopped without a completion report or a result recovered by hcom.\n' \
            "$worker" >&2
          [[ -n "$transcript" ]] && printf 'Transcript: %s\n' "$transcript" >&2
          exit 126
        fi
        printf 'Antigravity status — %ss elapsed — worker lookup temporarily unavailable.\n' \
          "$elapsed"
        continue
      fi

      print_heartbeat "$worker" "$elapsed"
      status="unknown"
      status_age=0
      if command -v jq >/dev/null 2>&1; then
        status=$(jq -r '.status // "unknown"' <<<"$worker_json")
        status_age=$(jq -r '.stored_status_age_seconds // 0 | floor' <<<"$worker_json")
      fi
      if [[ "$status" == "listening" && "$status_age" =~ ^[0-9]+$ \
        && "$status_age" -ge "$IDLE_NUDGE_SECONDS" && "$nudged_idle_worker" -eq 0 ]]; then
        trap - ERR
        set +e
        hcom term inject "$worker" \
          "If the assigned task is complete or blocked, send the required completion report to @${caller} on thread ${thread} now using 'hcom send', not 'uvx hcom send'. Otherwise continue the task." \
          --enter >/dev/null
        inject_status=$?
        set -e
        trap cleanup ERR
        if [[ "$inject_status" -eq 0 ]]; then
          nudged_idle_worker=1
          printf 'Woke idle Antigravity worker %s once to request its missing completion report.\n' \
            "$worker"
        else
          printf 'Could not wake idle Antigravity worker %s; continuing to monitor it.\n' \
            "$worker" >&2
        fi
      fi
      ;;
    *)
      printf '%s\n' "$event_output" >&2
      cleanup
      trap - ERR INT TERM
      exit "$wait_status"
      ;;
  esac
done

trap - ERR INT TERM
printf 'No completion report arrived within %s seconds.\n' "$timeout_seconds" >&2
printf 'Antigravity worker %s was left running; continue waiting with:\n' "$worker" >&2
printf '  hcom events --wait 1800 --after-id %s --thread %s --result-from %s --name %s\n' \
  "$result_cursor" "$thread" "$worker" "$caller" >&2
printf 'Inspect it with: hcom list %s --json; hcom term %s; hcom transcript %s --last 5 --full\n' \
  "$worker" "$worker" "$worker" >&2
exit 124
