# Agent orchestration and token-efficiency issue register

## Problem statement

hcom provides strong communication, visibility, and terminal-control primitives, but much of the worker lifecycle state machine is currently exposed to the calling model. A main agent may therefore spend a substantial part of its turn polling status, inspecting terminals, acknowledging messages, recovering launches, retrieving transcripts, and deciding whether workers have finished.

The target is not to remove hcom's low-level capabilities. It is to move routine, deterministic supervision below the LLM while preserving:

- direct agent-to-agent communication;
- peer consultation when it is useful;
- terminal inspection and control as diagnostic escape hatches;
- complete transcripts and event history;
- subscriptions, messaging, and interactive workflows;
- the ability for a human or agent to override automation.

This register focuses on behavior that causes tool-call spam, consumes the main agent's context, or encourages agents to communicate continuously without improving task capability.

## Summary

| Priority | Area | Issues | Intended outcome |
| --- | --- | ---: | --- |
| P0 | Correctness and state semantics | 13 | Waiting and completion become reliable |
| P1 | Orchestration abstractions | 17 | Routine supervision moves into deterministic software |
| P1 | Communication policy | 9 | Useful communication remains available without constant interruption |
| P2 | Efficiency and maintainability | 7 | Polling, duplicated logic, and context noise are reduced |

### Status overview

- **Working:** 1, 2, 3, 4, 6, 14, 33, 34, 35, 36, 39, 40, 41, 42, and 46.
- **Partially addressed:** 30, with the remaining native-workflow follow-up
  tracked by issues 8 and 28.
- **Partially mitigated:** 15; provider-native workspace trust is not an hcom
  policy decision.
- **Planned:** 44, the optional Bubblewrap boundary.
- All other entries remain open unless their section says otherwise.

## Field evidence from Codex session `019fbf7b-705a-75d1-81bc-d935ab026c1c`

The session contains two sustained hcom-supervised Antigravity runs on 2026-08-15 and 2026-08-16. Task-specific source and prompt content is intentionally omitted here; the orchestration behavior is the relevant evidence.

Across the interval from `2026-08-15T23:15Z` to `2026-08-16T01:02Z`, 91 tool-call records mentioned hcom. Some calls combined several shell commands. Their payloads contained 31 `hcom events`, 28 `hcom list`, 27 `hcom term`, 18 `hcom listen`, 10 `hcom send`, 7 `hcom transcript`, 2 `hcom kill`, and 2 `hcom r` invocations. These figures are trace evidence, not a benchmark, but they demonstrate the scale of the supervision burden in ordinary use.

### Run A: active worker reported as launch failure

- Launch readiness returned `0/1 ready` after 10 seconds and again after a 30-second event wait, even though the captured terminal tail ended in `Running`.
- Immediate inspection showed the worker actively reading files and using tools. The parent nevertheless had to use verbose list, terminal, and event inspection, then send a second task request.
- The worker itself ran `hcom list`, several `hcom events` calls, two acknowledgement sends, and repeated short `hcom listen` calls while doing the task.
- A filtered event wait later exited successfully with only an `<hcom>[hcom-events]` wrapper and no matching worker result.
- `hcom list -v` reported the worker as `listening` while its terminal still showed a full test suite and repeated listen calls in progress.
- After a valid completion report, the same worker appeared as `blocked: launch_blocked`, while terminal inspection showed `ready=true` and a completed response. `hcom r` then refused to resume it because it was “still active.” A queued follow-up did not visibly start until the parent injected text and Enter into the terminal.

### Run B: approval, addressing, and completion-report failures

- A headless launch using accept-edits and skip-permission-oriented flags still stopped on command approval. The parent had to inspect the terminal and inject Enter, twice during the run.
- A send written as `hcom send moto` first failed with `No input received on stdin`. The heredoc retry reported delivery to both the intended tagged worker and an unrelated stale agent named `zero`.
- `hcom listen --idle moto` repeatedly matched the worker entering its own short `hcom listen` command, even though the delegated task was not complete.
- Several long filtered listens yielded after the surrounding tool's shorter execution window, sending the main model back into list/terminal/listen cycles.
- Transcript output showed `ASSISTANT: (no response)` while the worker was actively editing and communicating through hcom, encouraging further terminal inspection.
- The final free-form completion report claimed Ruff was clean. Immediate parent verification found two Ruff `F401` errors, showing that a reported gate result is not equivalent to captured verification evidence.

### Mixed-provider Claude and GLM incident

The separately captured Claude/GLM run exposed another failure chain:

- Antigravity Claude appeared active but was still behind workspace trust and had not begun the task.
- GLM then completed, and its completion message was accepted as Claude's result. Claude was stopped while it was still starting or reading.
- GLM's substantive final response was not returned through the expected result path, so the parent retrieved it manually from the transcript before relaunching Claude.

The observations above directly motivate issues 33–40 and provide concrete reproductions for several earlier issues.

## P0: correctness and state semantics

### 1. Filtered waits can be satisfied by unrelated unread messages — working

Status: **Working as of 2026-08-27.** Filtered event waits and filtered `listen` calls now ignore unrelated unread inbox messages, while unfiltered waits retain the older-unread interrupt. Filtered listen queries matching events directly and does not advance the normal inbox cursor, so ignored messages remain available to a later ordinary listen. Regression coverage verifies the negative filtered cases, preserved inbox delivery, the preserved unfiltered behavior, and positive matching-event cases. The focused event/listen suites and Clippy pass, focused review approved the changes, and the live CLI smoke test reproduced the original failure before the fix and passed afterward.

Previously, the filtered event wait first checked for matching events but could also return success when the waiting identity merely had unread messages. An unrelated message could therefore wake a wait for a particular worker, thread, or event type.

Evidence: [`events_wait`](../src/commands/events.rs#L797) and its unread-message fallback in [`events.rs`](../src/commands/events.rs#L875).

The implemented fix makes every filtered wait complete only on its declared condition. A future explicit inbox-interrupt option with a distinct return reason may still be useful.

Remaining follow-up: internal `filter-wait:` status bookkeeping is excluded from filtered `listen`, but an `events --wait --type status` call or persistent status subscription can still observe that bookkeeping from another listener. The shared wait engine in issue 28 should suppress internal lifecycle noise consistently without hiding genuine agent status changes.

### 2. Waits use a time lookback instead of a durable cursor — working

Status: **Working as of 2026-08-27.** `hcom events --wait` and filtered `hcom listen` now use an event-ID boundary instead of a ten-second wall-clock lookback. A wait without `--after-id` captures the current last event and observes only future events. A caller that captures a boundary before launching work can pass `--after-id <ID>` to safely consume a result that arrived just before the waiter started. Explicit-cursor waits are strict and do not fall back to older unread inbox messages. Both custom help pages now document the cursor option.

Regression coverage verifies that a completed match is not replayed by a new wait, an event after an explicit cursor is consumed, filtered listen applies the same boundary without consuming inbox messages, both help pages expose the flag, and `events --after-id` requires wait mode. The live CLI smoke test confirms a match after the cursor, silence after advancing it, preserved ordinary inbox delivery, and visible help. The focused event, listen, and help suites pass; broader-gate details are recorded with the implementing commits.

Previously, event waiting used a recent time window, which could replay old events or make repeated waits observe the same completion.

Implementation: the cursor boundary in [`events.rs`](../src/commands/events.rs#L812) and the equivalent filtered-listen boundary in [`listen.rs`](../src/commands/listen.rs#L520).

The implemented CLI spelling is `--after-id`. Waiting and consumption no longer depend on wall-clock overlap.

### 3. An acknowledgement can satisfy a request watch — working

Status: **Working as of 2026-08-27.** Request watches now treat `ack` and nested `request` messages as nonterminal. An `inform` reply, or a legacy reply with no intent, can complete the watch. The same intent rule is enforced in live flow/reply-ID cancellation, reply-existence checks, and the atomic delayed-grace sweep, so an ACK cannot resolve the watch through a secondary path.

Regression coverage verifies end-to-end that ACK preserves a watch and a later result removes it, while the reply-existence path ignores both ACK and nested request intents. Request-watch notices now say that the worker did not return a result rather than incorrectly claiming it did not respond. The focused subscription suite and Clippy pass, and focused review approved the change. The broader-run caveats are the same as for issue 2 above.

Previously, the bootstrap instructions said that an `ACK` confirms receipt but does not complete the task, while request-watch resolution was based on the existence of any reply.

Evidence: the stated protocol in [`bootstrap.rs`](../src/bootstrap.rs#L117) and reply detection in [`subscriptions.rs`](../src/db/subscriptions.rs#L194).

The immediate protocol mismatch is fixed. A future durable workflow model should still represent at least these distinct states:

```text
received -> accepted -> running -> succeeded | failed | blocked | cancelled
```

The current intent-based rule remains a compatibility bridge; explicit terminal task state is tracked separately by the lifecycle and workflow issues below.

### 4. Threaded requests can lose abandonment detection — working

Status: **Working as of 2026-08-29.** Thread-routed requests now create the same
per-recipient durable request watches as explicitly targeted requests. Routing
through an existing thread no longer disables abandonment detection, while the
existing sender-kind, request-intent, mentions-scope, and delivered-recipient
guards remain intact.

Regression coverage verifies that a threaded request creates its watch and that
an `inform` reply on the thread cancels it. A hermetic live CLI run also showed
the `reqwatch-*` subscription after the request and its removal after the reply.

Previously, request watches were not created for thread-routed requests. A
request could therefore have a thread but no durable mechanism that detected
abandonment or terminal completion.

Implementation: request-watch creation in [`send.rs`](../src/commands/send.rs#L470)
and its threaded request/reply regression test in
[`send.rs`](../src/commands/send.rs#L1646).

The immediate routing/watch coupling is fixed. Tracking task completion by a
stable task or workflow ID independently of message routing remains the broader
follow-up described by issue 10.

### 5. Agent activity is conflated with task state

States such as active, listening, and blocked describe process or interaction behavior, but callers often infer task progress from them. A healthy process can be waiting for input, thinking silently, finished, or stuck while presenting similar surface activity.

Keep three separate state dimensions:

```text
process_state:     starting | running | exited
interaction_state: producing_output | awaiting_input | quiet
task_state:        queued | running | succeeded | failed | blocked | cancelled
```

The coordinator should wait on `task_state`, not terminal activity.

### 6. Stable terminal output can be mistaken for idleness

The PTY monitor transitions an active agent to listening after stable output for a short interval, with a source comment already noting possible false positives. Silent reasoning or a long tool call can therefore look idle.

Evidence: the delivery-gate status ownership and quiet-screen diagnostic in
[`delivery.rs`](../src/delivery.rs#L2293).

Status: **Working as of 2026-09-01.** The delivery loop no longer changes a
provider-owned `active` row to `listening`/`pty:recovered` after ten seconds of
quiet terminal output. Provider hooks now exclusively own the task-active to
task-idle transition. Screen quietness remains available as `quiet_ms` in gate
diagnostics but cannot unblock delivery or make a worker eligible for message
injection. Regression coverage keeps an eleven-second-quiet active worker
active, non-idle, and blocked at the delivery gate, and verifies that the
surviving TUI context annotation never changes status or emits a status event.

Use screen stability only as a UI hint. It should not establish task completion, abandonment, or readiness for reassignment.

### 7. Focused regression coverage for wait isolation is missing

The important failure modes are concurrent and semantic, not just parser-level. Add regression tests for:

- unrelated messages arriving during a filtered wait;
- repeated waits after one completion;
- simultaneous waits for different workers;
- cancellation while waiting;
- acknowledgements followed by delayed final results;
- results arriving shortly before or after waiter startup.

### 33. Recipient ambiguity can silently broaden delivery — working

Status: **Working as of 2026-08-27.** Recipient-free CLI sends now fail closed instead of becoming implicit broadcasts. A caller must provide an exact `@name`, reuse a seeded `--thread`, or explicitly acknowledge fan-out with `--broadcast`; large broadcasts from AI tools retain the additional `--go` preview gate. A live CLI smoke test reproduces `hcom send moto`, verifies that it creates no message, and confirms that `@moto` reaches only the intended worker.

In the observed session, a short-name send intended for `records-common-moto` reported delivery to both that worker and an unrelated stale agent, `zero`. The preceding quoted-argument form failed with `No input received on stdin`, making the broad-delivery retry a plausible agent response to unclear syntax.

The send path now fails closed. Exact names and deliberately suffixed tag groups remain available, while broadcast is an explicit operation rather than the fallback for ambiguous syntax.

### 34. An idle wait can be satisfied by the worker's own wait command — working

Status: **Working as of 2026-08-29.** `--idle` is now a task-idle predicate rather than a textual alias for `status=listening`. It excludes `cmd:listen`, internal `filter-wait:*` bookkeeping, provider startup status, and orphan-recovery transport state, while genuine provider idle transitions continue to match. Ordinary and filtered waits invoked by an integrated AI tool no longer overwrite its provider-owned task status merely because the command is waiting on transport; ad-hoc participants retain their operational listening marker. Ambiguous combinations with `--agent`, `--status`, or `--blocked` fail clearly instead of creating an unsatisfiable wait. Unit and CLI regressions cover the negative transport-wait case and a positive provider-idle transition.

Workers repeatedly called `hcom listen 5` or `hcom listen 10`. Those commands changed their status to `listening`, which caused the parent's `hcom listen --idle <worker>` to return even though the task was still active.

Evidence: listen explicitly writes listening status in [`listen.rs`](../src/commands/listen.rs#L244).

Do not implement task-idle waiting in terms of the status side effect of the wait command itself. At minimum, distinguish `transport_waiting` from `task_idle`; preferably wait on the workflow's terminal task state.

### 35. Lifecycle views can remain stale and mutually contradictory — working

One observed worker was reported as `blocked: launch_blocked` after it had completed and sent a result. Terminal inspection simultaneously showed `ready=true`, while resume refused because the process was still active.

Status: **Working as of 2026-09-01.** Provider-owned `active` status from a real
turn or tool hook is now authoritative launch evidence. Session-scoped status
events retain that evidence if a fast first turn returns to `listening` between
delivery-loop reads. It finalizes both pending launches and recovery from an
earlier launch blocker without overwriting the provider's active or listening
task state. Screen readiness remains the pre-execution signal. An approval shown
after provider execution began remains a task-level blocker, but cannot
retroactively redefine that launch as a failure.

Antigravity approval responses also suppress the already-answered screen scrape
for one redraw window. This prevents the observed same-second
`approval_cleared -> approval -> approval_cleared` bounce from leaking a stale
blocked state into a wrapper heartbeat without approving any command itself.

Regression coverage recreates an unready, non-empty, approval-looking terminal
alongside active Antigravity tool status and verifies pending-to-ready,
blocked-to-ready, fast-turn durable-event, and generic-provider transitions,
attempt/session and rebound-name isolation, preserved approval blockers and
provider state, and typed ready lifecycle events. Delivery tests and Clippy
pass. Live `hcom run agy` regressions changed the previously repeated `0/1
ready` result to `1/1 ready` in 8.0–8.5 seconds, then returned the exact
completion report and cleaned up the worker automatically. A direct
`hcom 1 agy` launch also reached `1/1 ready` in 7.0 seconds without a trust or
approval prompt.

Make lifecycle transitions monotonic where appropriate and attach freshness, source, and attempt IDs to every state. A later verified ready/running/completed state must supersede an earlier launch blocker for the same attempt. Commands should return one reconciled state rather than forcing the caller to compare list, PTY, event, and resume interpretations.

### 39. One worker's result can be attributed to another workflow — working

Status: **Working as of 2026-08-27.** `hcom events --cursor` captures an attempt boundary, and `hcom events --result-from <worker>` provides one fail-closed terminal-result wait from that boundary. The latter requires exactly one `--thread` workflow ID, binds the worker's exact registered generation (name plus immutable creation time), and owns the `message`/`inform` terminal condition so caller-supplied OR filters cannot widen it. If the worker reports and stops before the wait starts, correlation recovers the same generation from its post-cursor stopped snapshot. Regression and CLI coverage prove that a GLM report on Claude's thread, a Claude report on another thread, and a later worker reusing Claude's display name are rejected, while the exact worker/workflow/attempt tuple succeeds.

In the mixed Claude/GLM incident, GLM's completion was accepted as Claude's result and the Claude worker was stopped. This is more severe than a spurious wake-up: it can terminate the wrong worker and cause the parent to reason from the wrong model's output.

A terminal result must match an immutable tuple such as:

```text
workflow_id + attempt_id + worker_session_id + result_kind
```

Display names, recent messages, shared caller inboxes, or thread proximity remain insufficient correlation keys. Coordinators should capture the event cursor before launch, wait with `--result-from`, and clean up only the exact worker supplied to that correlated wait.

### 41. Filtered-listen timeout is indistinguishable from success — working

Status: **Working as of 2026-08-29.** A filtered `hcom listen` now returns exit code `1` when no event matches before the timeout, while a match remains `0` and interruption remains `130`. JSON mode emits a structured `matched: false`, `reason: timeout`, notification, and requested/effective timeout durations instead of producing no result. A final event scan runs before the timeout decision so a match arriving during the last polling interval wins. Callers that intentionally rely on the legacy zero-on-timeout behavior can opt into it explicitly with `--timeout-ok`; the structured timeout payload is still emitted. Unit and hermetic CLI regressions cover the strict, compatibility, and final-interval paths.

Filtered `hcom listen` currently exits with status `0` when its timeout expires without a match. A shell coordinator therefore cannot distinguish "condition matched" from "nothing happened" using the process result and may incorrectly advance a workflow.

Use a distinct nonzero timeout exit code and a structured timeout result, aligned with `events --wait`. Preserve a documented compatibility path for interactive callers that intentionally treat an empty timeout as success.

### 42. Filtered-listen JSON does not have a stable compatibility contract — working

The direct event-scan implementation made filtered-listen JSON more consistent by returning `matched`, `notification`, `event_id`, `type`, `instance`, and `data`, but it also changed the previous notification text and output shape. Scripts that parse the old subscription-oriented payload can break even though the underlying match is correct.

Status: **Working as of 2026-09-03.** Filtered `hcom listen --json` output is now an explicitly versioned contract built by one module, [`listen_result.rs`](../src/commands/listen_result.rs), so the match and timeout shapes cannot drift apart. Both outcomes carry `schema_version` (currently `1`), `matched`, and the legacy `notification` prose with its wording preserved; a match adds the typed `event_id`, `type`, `instance`, and `data` fields, a timeout adds `reason`, `timeout_seconds`, and `effective_timeout_seconds`, and no key appears on the wrong outcome. The module documents the compatibility policy: additive keys may land within a version and consumers must ignore unknown keys, while removing, renaming, or retyping a key, moving one between outcomes, or changing `matched` semantics requires a version bump together with the contract tests. `hcom listen --help` documents the schema, unit tests pin the exact key sets and types for both outcomes, and hermetic CLI regressions verify the versioned match and timeout objects end to end. Unfiltered message-mode `--json` lines remain a separate legacy shape outside the contract.

Define and test a stable machine-readable schema. Prefer typed fields over parsing `notification`, document additive versus breaking changes, and provide either a compatibility version or an explicit schema version when fields or meanings must change.

## P1: orchestration abstractions

### 8. There is no native delegate-and-supervise operation

The main model must compose launch, list, events, terminal, transcript, inject, and kill primitives itself. The existing `run` path is primarily an executor rather than a durable workflow abstraction.

Evidence: [`run.rs`](../src/commands/run.rs#L307).

Provide high-level operations such as:

```text
hcom delegate <agent> --task ...
hcom wait <workflow-id>
hcom result <workflow-id>
hcom cancel <workflow-id>
```

Keep the current low-level commands for debugging and unusual interactive work.

### 9. There is no first-class `wait_all`

Parallel delegation forces the caller to poll each worker and reconcile partial completion. Add one operation that waits for a declared worker set and returns all terminal states and results together.

### 10. Completion is a prompt convention rather than a protocol event

Workers are told how to report completion in prose, leaving the coordinator to interpret messages. Add a typed terminal event tied to a workflow and attempt ID. Human-readable prose can remain attached as the result body.

### 11. There is no portable final-result abstraction

Provider-specific scripts extract final answers in different ways, while the repository primarily exposes general transcript rendering. Add a stable command and schema:

```text
hcom result <worker-or-workflow> --json
```

The result should distinguish the final task response from acknowledgements, progress, terminal noise, and peer discussion.

### 12. Human-readable launch output is used as an API

Coordinator scripts may need to parse launch prose to recover identities, thread IDs, or status. Every orchestration-facing launch path should support structured JSON containing stable identifiers and machine-readable state.

### 13. Launch recovery is handed back to the model

When launch readiness fails, guidance points callers toward inspection commands such as verbose listing and launch events.

Evidence: launch troubleshooting in [`launch.rs`](../src/commands/launch.rs#L815).

Classify known failure modes and handle safe, deterministic recovery internally. Escalate only unknown or policy-sensitive conditions.

### 14. Block reasons are not sufficiently typed

Launch-blocked events can include a terminal tail, but the caller still has to infer whether it saw workspace trust, authentication, quota exhaustion, a crash, or another prompt.

Evidence: blocked launch reporting in [`delivery.rs`](../src/delivery.rs#L1351).

Status: **Working as of 2026-08-31.** Launch-blocked events and wait results now
carry a stable `blocked_kind`, the actual matched evidence line, the raw detail,
and the legacy human-readable blocker string. Provider approval signals outrank
incidental auth, quota, or crash words in proposed commands; successful exit
code 0 is not treated as a crash; and only a distinctive workspace-trust prompt
can bypass screen settling. Producer-to-consumer regression coverage verifies
that the event emitted by the delivery loop is returned by `wait_for_launch`
without losing the structured fields.

Add a `blocked_kind`, for example:

```text
workspace_trust | authentication | quota | confirmation | crashed | unknown
```

Include the recognized evidence separately from the raw diagnostic tail.

### 15. Workspace-trust recovery is provider-dependent

The launcher has targeted readiness handling for some providers, but Antigravity can remain behind a workspace-trust prompt while appearing alive.

Evidence: provider-specific readiness handling in [`launcher.rs`](../src/launcher.rs#L1520).

Status: **Partially mitigated locally as of 2026-08-29, not fixed in hcom.**
Antigravity now has native trusted-workspace state and explicit per-workspace file
grants for the current development workspaces. A fresh live launch entered the
workspace and completed its read-only task without external input. Earlier
launches nevertheless stopped at the trust screen twice, so persisted provider
state has not yet established a reliable launcher contract.

Recognize the prompt only to return a typed `workspace_trust` blocker. hcom must
not synthesize approval, inject confirmation keys, or mutate Antigravity trust
state. Trust remains under the provider's native policy and the user's control.

### 16. Timeout recovery is not a resumable workflow

A timeout often returns troubleshooting commands to the main model, which then becomes the retry loop. Persist attempts and retry policy so the same workflow can resume, restart, or fail terminally without repeated reasoning.

### 17. Workflow state is distributed across loosely related records

Threads, launches, request watches, messages, and transcript output are connected by convention rather than one durable workflow record. Introduce a workflow entity containing the task, participating agents, attempts, lifecycle state, result references, deadlines, and recovery history.

### 18. Active-turn waiting is documented but not integrated

The VS Code gap note already recommends a native active-turn wait rather than repeated CLI polling. Implement the same deterministic coordinator core behind both CLI and MCP interfaces.

See [Codex VS Code native-agent gap](codex-vscode-native-agent-gap.md#recommended-next-step-active-turn-mcp-wait).

The observed session also shows why the interface must own the entire wait: long CLI listens repeatedly yielded at the surrounding tool's shorter execution boundary, causing the model to issue another listen or inspect command even though the underlying workflow had not changed.

### 36. A successful send does not mean a blocked worker can act on it — working

An observed follow-up returned `Sent to: blocked-worker`, but the worker did not visibly resume. Resume then failed because the same process was considered active, and the parent resorted to terminal injection.

A live `hcom run glm` session reproduced this on 2026-09-01. After a proposed
command was denied, Claude stopped at `What should Claude do instead?`. An
`hcom send` instruction was persisted successfully but did not schedule a new
turn; the same instruction had to be submitted with `hcom term inject` before
the worker continued.

Status: **Working as of 2026-09-03 for the reproduced Claude denial path.** A
Claude `PermissionDenied` hook changes the worker to listening only when the
current lifecycle row is the matching hook-owned `blocked: approval` state.
Claude Code 2.1.252 does not emit that hook for every interactive `No`, so the
PTY also recognizes its settled, empty `Interrupted · What should Claude do
instead?` prompt as the same falling edge. The screen fallback is constrained
to the current input-box context and refuses to fire while a newer approval
menu is visible. Both paths preserve denial diagnostics and wake delivery;
rule or policy denials without a current interactive approval, and late
denials after a newer provider transition, leave the current state unchanged.

Regression coverage inserts a real targeted message, verifies that it remains
pending while approval is blocked, and proves that it becomes preparable and
passes the delivery gate after denial. Separate tests cover subagent approval
rows and the guard that preserves an active state. The complete Claude-hook
and delivery test suites, formatting, and `cargo check` pass; focused review
approved the guarded implementation.

A live `hcom run glm` regression then entered `blocked: approval`, selected
`No` through `hcom term inject`, and queued a targeted follow-up. Without any
external terminal input or prompt injection, the worker returned to listening,
received the queued message, confirmed it, sent its completion report, and was
cleaned up by the wrapper.

Return separate delivery states such as `persisted`, `delivered_to_process`, `turn_scheduled`, and `actionable`. When a managed worker is blocked or between turns, the coordinator should either schedule a turn through a supported mechanism or report one typed blocker. A successful queue write must not imply that task execution resumed.

Remaining follow-up: the CLI still reports a successful persistence/routing
operation without distinguishing `delivered_to_process`, `turn_scheduled`, and
`actionable`. The concrete stuck-worker failure is fixed, but those explicit
delivery receipts remain desirable protocol work.

### 37. Completion reports do not carry verifiable gate provenance

In the observed session, a worker reported that Ruff passed cleanly; the parent's immediate check found two Ruff errors. The result protocol currently carries a model-authored assertion, not the command, exit code, output digest, working tree, and revision against which the gate ran.

Let integrations attach structured verification records to the result:

```text
gate: ruff
command: uv run ruff check ...
exit_code: 0
finished_at: ...
workspace_revision: ...
output_digest: ...
```

These records do not remove the need for parent verification, but they make stale, fabricated, or mismatched claims detectable without transcript archaeology.

### 40. Final-result recovery differs by provider wrapper — working

The current standalone Antigravity wrapper can recover the last completed `PLANNER_RESPONSE` from a stopped worker's transcript when its hcom report is missing. The GLM wrapper waits for an hcom completion report but does not provide equivalent transcript recovery. This asymmetry recreates the original “worker completed but result did not appear” failure depending on which provider ran the task.

Define a provider adapter contract that returns one normalized result:

```text
extract_final_result(worker_session_id, attempt_id)
```

Each adapter should identify authoritative final-response records, reject partial or cross-attempt content, and return provenance describing whether the result came from the protocol event or transcript recovery. Recovery belongs in repository code with fixtures and tests, not only in personal `~/.hcom/scripts`.

Status: **Working as of 2026-08-29.** Exact `--result-from` waits now watch the
correlated worker generation for both its hcom completion message and a stopped
lifecycle snapshot. If an Antigravity or Claude-format worker, including GLM,
stops without a matching message, a versioned provider adapter recovers only a
result bound to the expected workflow thread and session. Successful completion
sends are preferred; a terminal Claude response is accepted only from the
matching turn. Failed sends, wrong threads, wrong sessions, partial responses,
placeholder stops, and reused display names cannot complete the wait.

Recovered output uses the normal message-shaped result with explicit
`transcript_recovery` provenance, provider, evidence kind, session, transcript,
and attempt cursor. A supported stopped worker with no authoritative result
fails explicitly instead of becoming a heartbeat or timeout loop. The tested
user-created `agy` and `glm` workflows both capture the cursor before launch
and use the same `--result-from` contract; the obsolete Antigravity-only shell
parser was removed.
Provider-adapter, correlation, stopped-worker recovery, syntax, and focused
event tests pass. The workflow also instructs workers to use the coordinator's
`hcom` command rather than a separately resolved `uvx hcom`, preventing an
older package version from being selected during the tested workflow. This is
a compatibility mitigation rather than protocol enforcement. A live
Antigravity run returned its exact generation-tagged report in 12.9 seconds,
cleaned up automatically, and required one wrapper invocation.
The equivalent direct workflow—pre-launch cursor, raw `hcom 1 agy`, and one
blocking `hcom events --result-from` wait—returned the exact generation-tagged
report in 9.5 seconds without polling, terminal inspection, or manual input,
then cleaned up successfully. This verifies the underlying primitives as well
as the user-workflow coordinator; raw callers still own launch parsing and cleanup.
The Claude-hosted GLM end-to-end path remains gated by the separate native
workspace-trust issue, but its transcript format and wrapper contract are
covered hermetically.

### 44. Provider-native sandboxes do not provide one reliable worker boundary

Status: **Planned.** Adopt Bubblewrap as an optional hcom-owned outer sandbox
for interacting workers on Linux. This is a downstream architecture item, not
part of the immediate Antigravity lifecycle fixes.

The outer policy should mount only the declared workspace read/write, expose
provider credentials and configuration with the minimum required access, hide
secret and system paths, make `.git` read-only by default, and control network
access explicitly. hcom communication should use a project-local `HCOM_DIR` or
a narrow broker interface rather than mounting the complete user-level hcom
state into every worker.

Provider permission systems remain useful for approval UX, but they should not
be the sole containment boundary. Sandbox startup must be fail-closed: a worker
must never silently fall back to unrestricted host execution when Bubblewrap is
unavailable or its policy cannot be installed.

### 45. Provider-run cleanup can turn a successful result into wrapper failure

A live `hcom run glm` probe on 2026-09-03 received the worker's valid,
correlated completion report and stopped the worker, but the wrapper then
exited nonzero because removal of its temporary `provider-runs` directory raced
with another writer and returned `Directory not empty`. This makes a successful
task look failed and encourages callers to inspect state or rerun work that has
already completed.

Make provider-run cleanup atomic or retry boundedly after child processes have
closed their files. Cleanup failure should remain a structured warning when the
authoritative result was already recovered, while genuine leaked processes or
credentials should still fail loudly and report their exact path and owner.

### 46. Antigravity file-access approvals are invisible to supervision — working

Antigravity uses a separate terminal dialog for reads outside the workspace:
`File access`, `Allow access to this file?`, and a numbered allow/deny menu.
The PTY detector recognized command approvals only, so hcom continued reporting
the worker as `active/tool:view_file` while an unattended run waited forever for
input. This recreates the expensive terminal-inspection loop even when a caller
uses one blocking result wait.

Status: **Working as of 2026-09-05.** The Antigravity screen detector now
recognizes the complete file-access dialog while rejecting a stale heading
without its question and affirmative menu. `cargo test antigravity_ --bin hcom`
passes all 59 selected tests. A live `hcom run agy` reproduction requested the
same external file, published `blocked/pty:approval`, and let the user-created
wrapper terminate with status 125 without terminal inspection or approval
injection.

The wrapper's fail-fast handling remains a user-workflow policy in
`~/.hcom/scripts/agy.sh`; hcom's repository-owned responsibility is to publish
the blocker accurately. No provider permission was bypassed or automatically
accepted.

## P1: communication policy and context control

### 19. Every task requires a model-generated acknowledgement

Workers are instructed to acknowledge receipt and later send a completion report. The first message consumes inference and wakes the requester despite conveying transport-level information.

Evidence: acknowledgement instructions in [`bootstrap.rs`](../src/bootstrap.rs#L79).

Generate delivery and acceptance receipts automatically. Ask the model to speak only when it has task-relevant content or needs help.

### 20. Ad-hoc agents are taught a manual listen loop

Bootstrap guidance encourages repeated listening, which can turn a persistent wait into repeated model/tool cycles.

Evidence: listen-loop guidance in [`bootstrap.rs`](../src/bootstrap.rs#L160).

Move continuous listening into the agent integration or coordinator process. Wake the model only for actionable input.

### 21. Native subagents announce connection in prose

Connection announcements are useful lifecycle information but do not require model-generated chat.

Evidence: native-subagent guidance in [`bootstrap.rs`](../src/bootstrap.rs#L190).

Emit a structured `worker.connected` event automatically and render it in the UI without injecting it into another model's conversational context.

### 22. System notifications share the ordinary message channel

Lifecycle, collision, delivery, and user-authored messages can all become conversational input. Add notification classes:

```text
model_actionable | coordinator_actionable | ui_only | audit_only
```

Only `model_actionable` notifications should interrupt the agent by default.

### 23. Progress lacks a non-interrupting channel

Agents should be able to expose progress without forcing the main model to read and respond. Store progress as structured workflow events, visible to humans and the coordinator but omitted from model context unless requested.

### 24. Thread membership can outlive the workflow

Subscriptions persist until explicitly removed, so old participants can continue receiving traffic.

Evidence: subscription behavior in [`subscriptions.rs`](../src/db/subscriptions.rs#L436) and thread delivery in [`messages.rs`](../src/messages.rs#L705).

Support workflow-close cleanup, membership expiry, or a configurable TTL.

### 25. Collision detection can create broad cross-agent noise

Integrations enable collision subscriptions by default, which is valuable for safety but can make every agent react to routine overlap.

Evidence: defaults in [`config.rs`](../src/config.rs#L337) and binding behavior in [`instance_binding.rs`](../src/instance_binding.rs#L980).

Keep collision detection, but coalesce duplicate notices and route them to the coordinator unless a worker must change behavior immediately.

### 26. Large message batches can be injected into model context

The delivery path can inject up to 50 messages at once. This preserves delivery but can consume a substantial part of the receiving agent's turn.

Evidence: [`MAX_MESSAGES_PER_DELIVERY`](../src/shared/constants.rs#L19) and truncation in [`hooks/common.rs`](../src/hooks/common.rs#L238).

Retain all messages in storage, but inject a prioritized manifest or compact summary with explicit retrieval for full bodies. Never silently discard messages.

### 27. Launch tips emphasize low-level supervision commands

Tips that advertise list, event, terminal, and transcript primitives encourage models to construct their own polling loop.

Evidence: low-level launch tips in [`tips.rs`](../src/core/tips.rs#L176).

For agent-facing output, recommend the high-level workflow operation first. Present low-level commands only as diagnostics after an escalation or failure.

## P2: efficiency and maintainability

### 28. Waiting behavior is implemented in several places

Event wait, listen, filtered listen, hook polling, launch readiness, and PTY monitoring have different completion and timeout contracts. Consolidate them on one cursor-based event/wait engine so fixes apply consistently.

### 29. A targeted message can wake more agents than necessary

Message delivery may trigger broad wake behavior even when recipients are known.

Evidence: wake behavior in [`send.rs`](../src/commands/send.rs#L470).

Wake only intended recipients and any explicitly registered coordinator.

### 30. Useful provider coordinators live outside the repository

Local scripts can contain important result extraction, retry, and readiness behavior that is neither portable nor tested with hcom. Move the generic state machine into the Rust application and keep provider-specific recognition in small adapters.

Status: **Partially addressed by issue 40.** Result correlation and
provider-specific stopped-transcript recovery live in versioned Rust code. The
deterministic Antigravity and GLM launch/wait/heartbeat/cleanup workflows remain
user-created scripts under `~/.hcom/scripts/`. The remaining step is to move the
generic single-worker state machine from shell into a native workflow command
while retaining provider-specific policy and launch arguments in small adapters.

### 31. Heartbeats can still pollute the parent context

Compact heartbeats are cheaper than full polling but still cost tokens if repeatedly injected into the main conversation. Default delegated runs to final-result-only model delivery; send heartbeats to UI metadata or expose them on demand.

Filtered listen currently caps its local polling interval at 500 ms so local status and lifecycle events are noticed promptly even when no socket wake arrives. Heartbeat persistence should be rate-limited independently (for example, every 5–10 seconds) so a long wait does not turn responsiveness into repeated SQLite writes.

### 32. Workflows lack an explicit communication policy

Different tasks need different collaboration styles. Add a per-workflow policy instead of relying on prompts alone:

```text
communication = open | coordinator_only | result_only
progress_interval = none | 5m
peer_questions = allowed
automatic_receipts = true
```

`result_only` should suppress routine chatter, not prevent a blocked worker from asking a necessary question.

### 38. Command grammar and filter vocabulary cause trial-and-error calls

The observed parent tried plausible but unsupported forms including transcript `--tail`, events `--limit`, an SQL `from_agent` field, and a direct positional send. Each required another help, retry, or inspection call.

Normalize common options across commands—for example `--last`, `--after-id`, `--from`, and `--json`—and validate ambiguous send syntax before doing anything. Structured orchestration APIs should avoid shell parsing entirely. When a command rejects an option, return the exact supported equivalent for that command rather than only generic usage.

### 43. Internal filtered-wait context leaks into user-facing status

Filtered listen uses a unique `filter-wait:<pid>:<epoch>:<sequence>` status context to prevent its own bookkeeping event from satisfying the filter. `hcom list` and the TUI can expose that raw implementation marker, adding noisy identifiers to the exact status surfaces agents inspect during supervision.

Keep the unique internal marker for correctness, but hide or normalize it at presentation boundaries. Human- and model-facing status should say that the agent is waiting on an event filter without exposing coordination IDs unless diagnostic verbosity is explicitly requested.

## Capability-preserving design principles

1. Keep existing low-level commands as manual diagnostics and escape hatches.
2. Retain every message and event in durable storage even when it is not injected into a model context.
3. Automate only typed, recognized states under explicit policy. Unknown states should be surfaced, not guessed through.
4. Preserve peer consultation. Quiet defaults should reduce ceremonial chatter, not prevent useful collaboration.
5. Route human-visible progress through UI or structured progress events rather than conversational interruptions.
6. Make the common coordinator deterministic. An LLM should decide what work to delegate and handle genuinely ambiguous exceptions, not poll processes.
7. Share one workflow engine across CLI, hooks, and MCP so their lifecycle semantics cannot drift.

## Target interaction

The ordinary main-agent experience should be one high-level delegation call and one terminal result, even if the coordinator performs many internal checks:

```text
delegate_many([
  { agent: "glm", task: "..." },
  { agent: "opus", task: "..." }
])

-> both workers launched
-> known startup prompts handled under policy
-> progress retained outside model context
-> failed attempts retried according to policy
-> final structured results returned together
```

Commands such as `hcom list -v`, `hcom term`, `hcom transcript`, and
`hcom term inject` should appear in the main agent's trace only when it
deliberately enters diagnostic mode.

## Recommended implementation order

1. Fix filtered waits, cursor semantics, strict workflow-result correlation, unique recipient resolution, acknowledgement handling, and their concurrency tests.
2. Separate process, interaction, transport-wait, and task state; reconcile stale lifecycle events by attempt.
3. Add a durable workflow record plus `delegate`, `wait`, `wait_all`, `result`, and `cancel` operations, with normalized provider result recovery.
4. Add typed blocking reasons, actionable-delivery state, and policy-controlled startup recovery.
5. Replace model-generated receipts and progress chatter with structured protocol events and captured gate provenance.
6. Expose the coordinator through the active-turn MCP wait described in the VS Code gap note.
7. Normalize CLI addressing and filter vocabulary, then update bootstrap prompts, notification routing, and tips to prefer the high-level workflow while retaining low-level controls.

## Success measures

- A two-worker delegated task normally requires one launch call and one returned result from the main model.
- Unrelated messages cannot satisfy a filtered wait.
- An ambiguous recipient cannot cause implicit fan-out.
- A result from one provider, worker, or attempt cannot complete or clean up another workflow.
- A receipt or acknowledgement cannot be mistaken for completion.
- A worker entering a transport wait cannot be mistaken for task completion.
- Known startup prompts are either handled under explicit policy or returned once as a typed blocker.
- Lifecycle views agree on the current workflow attempt and expose the source and freshness of their state.
- Routine progress and lifecycle events remain visible to humans without entering the main model's context.
- Peer-to-peer questions still work when a worker needs information.
- Reported verification gates carry machine-captured provenance and remain subject to parent verification.
- Every supported provider can return a normalized final result or one explicit typed failure, including after transcript recovery.
- Full event history, messages, transcripts, and manual terminal control remain available for debugging.
