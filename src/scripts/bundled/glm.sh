#!/usr/bin/env bash
# Launch a GLM worker through hcom's native Claude Code integration.
set -euo pipefail

GLM_CLAUDE_BIN_DIR="${HCOM_GLM_CLAUDE_BIN_DIR:-}"
readonly GLM_CLAUDE_CONFIG_DIR="${HCOM_GLM_CLAUDE_CONFIG_DIR:-${HCOM_DIR:-$HOME/.hcom}/glm-claude}"
readonly GLM_API_BASE_URL="${HCOM_GLM_API_BASE_URL:-https://api.z.ai/api/anthropic}"
# Z.ai currently maps this 1M-context compatibility selector to its current
# coding model. The canonical served model is read from the session transcript.
readonly GLM_MODEL="${HCOM_GLM_MODEL:-glm-5.2[1m]}"
readonly GLM_EFFORT="${HCOM_GLM_EFFORT:-max}"

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

print_runtime_identity() {
  local worker="$1"
  local stopped_info transcript runtime
  stopped_info=$(hcom list --stopped "$worker" 2>/dev/null || true)
  transcript=$(printf '%s\n' "$stopped_info" | sed -n 's/^[[:space:]]*Transcript:[[:space:]]*//p' | head -n 1)
  if [[ -z "$transcript" || ! -r "$transcript" ]] || ! command -v jq >/dev/null 2>&1; then
    printf 'Requested GLM selector: %s; effort: %s.\n' "$GLM_MODEL" "$GLM_EFFORT"
    return
  fi
  runtime=$(jq -r '
    select(.type == "assistant" and .message.model != null)
    | [.message.model, (.effort // "unknown")]
    | @tsv
  ' "$transcript" 2>/dev/null | head -n 1) || runtime=""
  if [[ -n "$runtime" ]]; then
    printf 'Runtime model and effort: %s\n' "$runtime"
  else
    printf 'Requested GLM selector: %s; effort: %s.\n' "$GLM_MODEL" "$GLM_EFFORT"
  fi
}

trap cleanup ERR
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

caller=""
workdir="$PWD"
timeout_seconds=1800
keep_worker=0
thread=""
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
    --thread)
      thread="${2:?--thread requires a value}"
      shift 2
      ;;
    --keep)
      keep_worker=1
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: hcom run glm [OPTIONS] TASK

Launch the current Z.ai GLM coding worker with a 1M context selector and max effort.

Options:
  --dir PATH       Worker directory (default: current directory)
  --timeout SEC    Result wait ceiling (default: 1800)
  --thread NAME    Explicit hcom thread (default: unique generated name)
  --keep           Keep the worker alive after a successful result
  -h, --help       Show this help

Environment:
  AI_API_KEY                  Z.ai Coding Plan API key (required)
  HCOM_GLM_CLAUDE_BIN_DIR     Directory containing the Claude Code executable
  HCOM_GLM_CLAUDE_CONFIG_DIR  Isolated Claude configuration directory
  HCOM_GLM_API_BASE_URL       Anthropic-compatible Z.ai endpoint
  HCOM_GLM_MODEL              Compatibility model selector
  HCOM_GLM_EFFORT             Claude Code effort setting

On success, the worker is killed unless --keep is supplied. On a result-wait
timeout, the worker is intentionally left running and its name/thread are
printed so its eventual result is not lost. A worker whose launch remains
unconfirmed is also left available for inspection.
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

if [[ ${#task_parts[@]} -eq 0 ]]; then
  printf '%s\n' 'A task is required. Run: hcom run glm --help' >&2
  exit 2
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

if [[ -z "${AI_API_KEY:-}" ]]; then
  printf '%s\n' 'AI_API_KEY is required for the Z.ai Coding Plan' >&2
  exit 2
fi
if [[ -z "$GLM_CLAUDE_BIN_DIR" ]]; then
  claude_path=$(command -v claude 2>/dev/null || true)
  if [[ -n "$claude_path" ]]; then
    GLM_CLAUDE_BIN_DIR=$(dirname "$claude_path")
  fi
fi
if [[ ! -x "$GLM_CLAUDE_BIN_DIR/claude" ]]; then
  printf '%s\n' \
    'Claude Code is required. Put claude on PATH or set HCOM_GLM_CLAUDE_BIN_DIR.' >&2
  exit 2
fi

thread="${thread:-glm-$(date +%s)-$$}"

# Capture the attempt boundary before launch so a fast report or a stopped
# worker's transcript recovery remains tied to this exact worker generation.
if ! result_cursor=$(hcom events --cursor 2>&1); then
  printf 'Could not capture the pre-launch hcom event cursor: %s\n' \
    "$result_cursor" >&2
  exit 1
fi
if [[ ! "$result_cursor" =~ ^[0-9]+$ ]]; then
  printf 'Invalid pre-launch hcom event cursor: %s\n' "$result_cursor" >&2
  exit 1
fi

export PATH="$GLM_CLAUDE_BIN_DIR:$PATH"
export CLAUDE_CONFIG_DIR="$GLM_CLAUDE_CONFIG_DIR"
export ANTHROPIC_BASE_URL="$GLM_API_BASE_URL"
export ANTHROPIC_AUTH_TOKEN="$AI_API_KEY"
unset ANTHROPIC_API_KEY

prompt="Work on the following bounded task in ${workdir}. Read and follow repository instructions. Preserve unrelated changes. Do not commit, push, install dependencies, or use external services unless the task explicitly authorizes it. Verify the result in proportion to risk.

Task:
${task}

When complete or blocked, send one concise completion report to @${caller} in hcom thread ${thread} with intent inform. Include what changed, files changed, verification and outcome, and remaining risks. Use your hcom identity when sending. Do not finish your turn without sending that report."

trap - ERR
set +e
launch_output=$(hcom 1 claude \
  --tag glm \
  --go \
  --headless \
  --dir "$workdir" \
  --hcom-prompt "$prompt" \
  --model "$GLM_MODEL" \
  --effort "$GLM_EFFORT" \
  --permission-mode acceptEdits 2>&1)
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
  printf '%s\n' 'Could not determine the launched GLM worker name' >&2
  cleanup
  trap - ERR INT TERM
  exit 1
fi

worker="${launched_names[0]}"
if [[ "$launch_status" -eq 2 ]]; then
  batch_id=$(printf '%s\n' "$launch_output" | sed -n 's/^Batch id: //p' | head -n 1)
  if [[ -z "$batch_id" ]]; then
    printf '%s\n' 'Launch is still pending but its batch ID was not reported' >&2
    cleanup
    trap - ERR INT TERM
    exit 1
  fi
  trap - ERR
  set +e
  launch_wait_output=$(hcom events launch "$batch_id" --timeout 60 2>&1)
  launch_wait_status=$?
  set -e
  trap cleanup ERR
  printf '%s\n' "$launch_wait_output"
  if [[ "$launch_wait_status" -ne 0 ]]; then
    trap - ERR INT TERM
    printf 'GLM worker %s did not become ready and was left available for inspection.\n' \
      "$worker" >&2
    exit 125
  fi
fi
printf 'GLM worker: %s\nThread: %s\n' "$worker" "$thread"

trap - ERR
set +e
event_output=$(hcom events \
  --wait "$timeout_seconds" \
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
      printf 'Stopped GLM worker %s after receiving its report.\n' "$worker"
      print_runtime_identity "$worker"
    else
      printf 'GLM worker %s remains available on thread %s.\n' "$worker" "$thread"
    fi
    ;;
  1)
    if [[ "$event_output" != *'"timed_out":true'* ]]; then
      printf 'GLM result wait or recovery failed before timeout: %s\n' \
        "$event_output" >&2
      cleanup
      trap - ERR INT TERM
      exit 1
    fi
    trap - ERR INT TERM
    printf 'No report arrived within %s seconds.\n' "$timeout_seconds" >&2
    printf 'Worker %s was left running; continue waiting with:\n' "$worker" >&2
    printf '  hcom events --wait 1800 --after-id %s --thread %s --result-from %s --name %s\n' \
      "$result_cursor" "$thread" "$worker" "$caller" >&2
    exit 124
    ;;
  *)
    printf '%s\n' "$event_output" >&2
    cleanup
    trap - ERR INT TERM
    exit "$wait_status"
    ;;
esac
