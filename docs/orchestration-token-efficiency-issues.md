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
| P0 | Correctness and state semantics | 11 | Waiting and completion become reliable |
| P1 | Orchestration abstractions | 14 | Routine supervision moves into deterministic software |
| P1 | Communication policy | 9 | Useful communication remains available without constant interruption |
| P2 | Efficiency and maintainability | 6 | Polling, duplicated logic, and context noise are reduced |

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

Status: **Working as of 2026-08-24.** Filtered waits now ignore unrelated unread inbox messages, while unfiltered waits retain the older-unread interrupt. Regression coverage verifies the negative filtered case, the preserved unfiltered behavior, and a positive matching-event case. The events test module and Clippy pass, the focused review approved the change, and the broader suite passes when excluding one pre-existing unrelated transcript PATH-error test.

Previously, the filtered event wait first checked for matching events but could also return success when the waiting identity merely had unread messages. An unrelated message could therefore wake a wait for a particular worker, thread, or event type.

Evidence: [`events_wait`](../src/commands/events.rs#L854) and its unread-message fallback in [`events.rs`](../src/commands/events.rs#L888).

The implemented fix makes a filtered wait complete only on its declared condition. A future explicit inbox-interrupt option with a distinct return reason may still be useful.

### 2. Waits use a time lookback instead of a durable cursor — working

Status: **Working as of 2026-08-27.** `hcom events --wait` and filtered `hcom listen` now use an event-ID boundary instead of a ten-second wall-clock lookback. A wait without `--after-id` captures the current last event and observes only future events. A caller that captures a boundary before launching work can pass `--after-id <ID>` to safely consume a result that arrived just before the waiter started. Explicit-cursor waits are strict and do not fall back to older unread inbox messages.

Regression coverage verifies that a completed match is not replayed by a new wait, an event after an explicit cursor is consumed, filtered listen applies the same boundary, and `events --after-id` requires wait mode. The focused event and listen suites and Clippy pass, and focused review approved the cursor semantics. The full parallel run passed 2,178 tests; two unrelated environment-sensitive hook-install tests failed there and passed immediately when rerun alone. One pre-existing unrelated transcript PATH test remains excluded.

Previously, event waiting used a recent time window, which could replay old events or make repeated waits observe the same completion.

Implementation: the cursor boundary in [`events.rs`](../src/commands/events.rs#L805) and the equivalent filtered-listen boundary in [`listen.rs`](../src/commands/listen.rs#L488).

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

### 4. Threaded requests can lose abandonment detection

Request watches are not created for some thread-routed requests. A request can therefore have a thread but no durable mechanism that detects abandonment or terminal completion.

Evidence: request-watch creation in [`send.rs`](../src/commands/send.rs#L451) and the corresponding behavior documented by its test in [`send.rs`](../src/commands/send.rs#L1571).

Track task completion by a stable task or workflow ID independently of the message-routing thread.

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

Evidence: the stable-output heuristic in [`delivery.rs`](../src/delivery.rs#L1975).

Use screen stability only as a UI hint. It should not establish task completion, abandonment, or readiness for reassignment.

### 7. Focused regression coverage for wait isolation is missing

The important failure modes are concurrent and semantic, not just parser-level. Add regression tests for:

- unrelated messages arriving during a filtered wait;
- repeated waits after one completion;
- simultaneous waits for different workers;
- cancellation while waiting;
- acknowledgements followed by delayed final results;
- results arriving shortly before or after waiter startup.

### 33. Recipient ambiguity can silently broaden delivery

In the observed session, a short-name send intended for `records-common-moto` reported delivery to both that worker and an unrelated stale agent, `zero`. The preceding quoted-argument form failed with `No input received on stdin`, making the broad-delivery retry a plausible agent response to unclear syntax.

This should fail closed. A send with no explicit, uniquely resolved recipient must not fan out implicitly. Return candidate identities and require the caller to select one, preferably using the stable workflow participant ID rather than a display name.

### 34. An idle wait can be satisfied by the worker's own wait command

Workers repeatedly called `hcom listen 5` or `hcom listen 10`. Those commands changed their status to `listening`, which caused the parent's `hcom listen --idle <worker>` to return even though the task was still active.

Evidence: listen explicitly writes listening status in [`listen.rs`](../src/commands/listen.rs#L244).

Do not implement task-idle waiting in terms of the status side effect of the wait command itself. At minimum, distinguish `transport_waiting` from `task_idle`; preferably wait on the workflow's terminal task state.

### 35. Lifecycle views can remain stale and mutually contradictory

One observed worker was reported as `blocked: launch_blocked` after it had completed and sent a result. Terminal inspection simultaneously showed `ready=true`, while resume refused because the process was still active.

Make lifecycle transitions monotonic where appropriate and attach freshness, source, and attempt IDs to every state. A later verified ready/running/completed state must supersede an earlier launch blocker for the same attempt. Commands should return one reconciled state rather than forcing the caller to compare list, PTY, event, and resume interpretations.

### 39. One worker's result can be attributed to another workflow

In the mixed Claude/GLM incident, GLM's completion was accepted as Claude's result and the Claude worker was stopped. This is more severe than a spurious wake-up: it can terminate the wrong worker and cause the parent to reason from the wrong model's output.

A terminal result must match an immutable tuple such as:

```text
workflow_id + attempt_id + worker_session_id + result_kind
```

Display names, recent messages, shared caller inboxes, or thread proximity are insufficient correlation keys. Cleanup must use the worker identity recorded on that same matched attempt. Add a concurrent mixed-provider regression test proving that GLM completion cannot satisfy or clean up a Claude workflow, and vice versa.

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

Add a `blocked_kind`, for example:

```text
workspace_trust | authentication | quota | confirmation | crashed | unknown
```

Include the recognized evidence separately from the raw diagnostic tail.

### 15. Workspace-trust recovery is provider-dependent

The launcher has targeted readiness handling for some providers, but Antigravity can remain behind a workspace-trust prompt while appearing alive.

Evidence: provider-specific readiness handling in [`launcher.rs`](../src/launcher.rs#L1520).

Add exact prompt recognition and an explicit policy controlling whether trust may be accepted automatically. Do not use broad keystroke injection based on fuzzy terminal text.

### 16. Timeout recovery is not a resumable workflow

A timeout often returns troubleshooting commands to the main model, which then becomes the retry loop. Persist attempts and retry policy so the same workflow can resume, restart, or fail terminally without repeated reasoning.

### 17. Workflow state is distributed across loosely related records

Threads, launches, request watches, messages, and transcript output are connected by convention rather than one durable workflow record. Introduce a workflow entity containing the task, participating agents, attempts, lifecycle state, result references, deadlines, and recovery history.

### 18. Active-turn waiting is documented but not integrated

The VS Code gap note already recommends a native active-turn wait rather than repeated CLI polling. Implement the same deterministic coordinator core behind both CLI and MCP interfaces.

See [Codex VS Code native-agent gap](codex-vscode-native-agent-gap.md#recommended-next-step-active-turn-mcp-wait).

The observed session also shows why the interface must own the entire wait: long CLI listens repeatedly yielded at the surrounding tool's shorter execution boundary, causing the model to issue another listen or inspect command even though the underlying workflow had not changed.

### 36. A successful send does not mean a blocked worker can act on it

An observed follow-up returned `Sent to: blocked-worker`, but the worker did not visibly resume. Resume then failed because the same process was considered active, and the parent resorted to terminal injection.

Return separate delivery states such as `persisted`, `delivered_to_process`, `turn_scheduled`, and `actionable`. When a managed worker is blocked or between turns, the coordinator should either schedule a turn through a supported mechanism or report one typed blocker. A successful queue write must not imply that task execution resumed.

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

### 40. Final-result recovery differs by provider wrapper

The current standalone Antigravity wrapper can recover the last completed `PLANNER_RESPONSE` from a stopped worker's transcript when its hcom report is missing. The GLM wrapper waits for an hcom completion report but does not provide equivalent transcript recovery. This asymmetry recreates the original “worker completed but result did not appear” failure depending on which provider ran the task.

Define a provider adapter contract that returns one normalized result:

```text
extract_final_result(worker_session_id, attempt_id)
```

Each adapter should identify authoritative final-response records, reject partial or cross-attempt content, and return provenance describing whether the result came from the protocol event or transcript recovery. Recovery belongs in repository code with fixtures and tests, not only in personal `~/.hcom/scripts`.

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

### 31. Heartbeats can still pollute the parent context

Compact heartbeats are cheaper than full polling but still cost tokens if repeatedly injected into the main conversation. Default delegated runs to final-result-only model delivery; send heartbeats to UI metadata or expose them on demand.

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

Commands such as `hcom list -v`, `hcom term`, `hcom transcript`, and `hcom inject` should appear in the main agent's trace only when it deliberately enters diagnostic mode.

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
