# Agent observation and sandbox follow-up notes

Recorded 2026-10-23 for this checkout. Recheck working-tree state, installed
skills, binaries, and OpenShell capabilities before resuming implementation.

## Streaming findings from the OMP review trial

A parent repeatedly ran `events --wait 45 --result-from ...` while an OMP
reviewer worked. Each invocation silently waited, returned a deadline, and
was re-armed. Repeated unchanged-status commentary accompanied those calls.
This was not a failure of upstream streaming: neither streaming command was
being invoked.

The retained patch series still includes generation following, compact
progress, and correlated result waits. They were not replaced wholesale by
upstream. See [patch ownership](fork-patches.json) and
[maintenance guidance](fork-patches.md). An isolated probe verified compact
heartbeats in the installed executable, not just in the checkout's build.

| Interface | Current behavior | Suitable use |
| --- | --- | --- |
| `hcom events --follow [filters]` | Continuous matching event feed, including messages; not pinned to one worker incarnation | General observation, multiple agents, or filtered event types |
| `hcom events stream --follow NAME --after-id ID` | Emits events, including messages sent by the exact worker generation; ends at its terminal stop boundary | Observe one delegated attempt |
| The preceding command with `--compact` | Emits typed phases, file paths, command categories, and optional quiet heartbeats; suppresses message events entirely | Optional bounded activity display, not live conversational coordination |
| `hcom events --wait SEC --after-id ID --thread THREAD --result-from NAME` | One silent correlated completion wait with typed terminal outcomes | Collect a report; not a progress stream |

Compact's message suppression is deliberate in the current implementation:
[classifier](../src/core/compact.rs) and [closed progress schema](../src/core/progress.rs).
Messages reset the quiet timer but produce no notification. Do not claim that
compact currently emits even a metadata-only "message received" record.
Normal streaming is the user's preferred default for live coordination.

## Current observer helper and its limits

[watch-result.py](../skills/hcom-agent-messaging/references/scripts/watch-result.py)
is an optional skill helper, not a new native hcom command or streaming backend.
It starts one normal generation-following stream plus one task-sized correlated
result wait. It forwards events, then emits
`{"kind":"result","exit_code":...,"payload":...}` and returns the wait status.
It stops only its two observers; the worker remains available for follow-up.
Compact is opt-in via `--compact`; `--heartbeat` requires that option.

The helper exists because a final report normally does not stop the worker.
Following its generation alone therefore need not end when the task finishes.
A received report is not proof that the review/task succeeded.

Use one resumable host execution; resume the same execution handle instead
of starting more hcom waits. Run directly, without command substitution or
other buffering that hides output until exit. Use a task-sized deadline;
do not automatically re-arm short deadlines or narrate each quiet interval.
The [messaging skill](../skills/hcom-agent-messaging/SKILL.md) contains usage.

Following a worker shows that worker's outbound messages; it is not a feed
of every message addressed to the main agent. Observation leaves inbox
cursors alone. Codex normally receives its own pending mail through hooks
at tool boundaries. A continuously blocking host invocation can delay model
awareness; stream output does not itself guarantee immediate interruption.
Responsiveness to unrelated incoming messages remains an integration topic.

The helper and skill changes accompany this note; locate their commit in Git
history when resuming. The installed Codex
skill was refreshed and verified equal to the repo package (14 files).
Existing sessions must reread it; updating disk does not replace previously
loaded instructions in their conversation.

## Acknowledgements and skill distribution

The attachment also showed the reviewer overthinking whether an ack could
complete the result wait, then issuing an ack without `--reply-to`.
An ack requires the actual incoming request ID. It cannot satisfy an
inform-only result wait. A requested final report fulfills the response
requirement; a separate immediate ack is optional.
[Bootstrap wording](../src/bootstrap.rs) was corrected in the working tree;
the installed executable was not replaced in this session.

Executable installation, runtime hooks, generated bootstrap, and full skill
discovery are separate layers. `hcom hooks add` does not distribute skills.
Automatic skill installation/refresh before launch was discussed but is not
implemented. The repo's full skill folder is the maintained distribution
source; the Codex copy was explicitly refreshed. Do not infer its original
installer or assume other tool profiles received a copy.
See [setup and distribution](../skills/hcom-agent-messaging/references/setup.md),
including effective profile paths and the Claude plugin-name collision.
OMP reviews in this trial should inherit the configured model/effort unless
the user requests overrides.

## Sandbox work to resume

The existing OpenSpec change is
[add-openshell-sandbox-mode](../openspec/changes/add-openshell-sandbox-mode/proposal.md),
with [design and source research](../openspec/changes/add-openshell-sandbox-mode/design.md)
and [implementation/qualification tasks](../openspec/changes/add-openshell-sandbox-mode/tasks.md).
It is a researched proposal, not an implemented or live-qualified feature.
The proposal was committed as `ff9a2b3`; messaging/setup documentation followed
in `7f22416`. Do not consult historical plans in older hcom folders.

The intended OpenShell checkout is
`/home/imi/Projects/openshell-eval/OpenShell-current`. Preserve its existing
work and verify exact CLI/gateway/runtime versions rather than trusting PATH
or old version observations in the design.

The proposed contract is:

- Opt-in named profile, one worker/private hcom database per sandbox, and a
  committed repository snapshot. Guest permissions allow unattended work
  inside the execution boundary; this does not grant host permissions.
- Host-owned team routing and bridge, ordinary conversations across host
  and guests, durable replay/deduplication, bounded traffic/storage, captured
  ownership IDs, and a controller independent of the launching terminal.
  Do not share the host DB, trusted MQTT PSK, ambient home, or credentials.
- Explicit request-correlated final results separated from progress;
  summaries/evidence over hcom, versioned artifacts over Git. Artifact
  readiness is separate from report acceptance; no automatic host merge.
- First qualification is headless Linux VM execution with Claude's PTY path;
  other tools/runtimes need separate qualification. Keep the portable sandbox
  core independent of this fork's observation/result adapters.
- Reprobe exec input allowances, concurrent output draining/backpressure,
  connection rotation, reconnect, final status, failure preservation, and
  three-agent communication/Git handoff. Source research is not live evidence.

A possible follow-up is a native hcom operation combining streamed observation
with correlated task completion and clear message-notification semantics.
That operation does not exist yet. Prefer small upstream integration points,
but do not sacrifice communication or containment correctness to minimize
the patch. Do not use idle, an ack, or the first arbitrary inform as a sandbox
task-completion marker.

## Validation evidence and remaining boundaries

- Observer tests: 10 passing, covering message-preserving default, opt-in
  compact/heartbeat, one stream plus one wait, statuses, large reports,
  cancellation, and child cleanup. Wired into `scripts/typecheck.sh`.
- Isolated real-CLI probes: intermediate message text/thread appears before
  the final result; unrelated progress/acks cannot finish the result wait;
  wrong generations are excluded and inbox cursors remain unchanged.
- Bootstrap tests: 27 passing. Normal check gate passed TypeScript checking,
  25 plugin tests, 71 Hermes tests, and the then-current 8 observer tests.
  The two additional observer tests passed after the normal-stream default
  correction. Skill validation, formatting, and whitespace checks passed.
- Required review completed at `/tmp/review-mcp-run-b3p7qm1y/review.md`;
  actionable findings were addressed. It reviewed the initial compact-default
  helper; the subsequent small normal-default correction was tested separately.
- No live provider trial validated the revised parent's orchestration. No
  sandbox implementation or sandbox live qualification was performed.
