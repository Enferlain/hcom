## Context

See `proposal.md` for motivation and
`specs/correlated-worker-waits/spec.md` for observable behavior.

`events_wait` in `src/commands/events.rs` is intentionally one-shot. It captures
or accepts a durable cursor, registers an `events_wait` TCP notification
endpoint when identity is available, rechecks SQLite, prints the first matching
event, and exits. Correlated `--result-from` additionally owns exact result,
blocker, failure, stopped-result recovery, and deadline outcomes. These are
mature synchronization semantics and are not the streaming integration point.

`events sub` is persistent but routes matches back into hcom as
`[hcom-events]` messages. That is useful for conversational notification, not
for a passive status surface. Event writes already pass through subscription
processing, while the existing wait listener uses targeted TCP wakes plus a
bounded database recheck cadence.

The common filter grammar lives in `src/core/filters.rs`. Current streamlined
event output removes universal bloat and truncates some status detail, but it is
not a security boundary: compact worker status needs a stricter typed
projection. Message events carry `sender_instance_key`; status events currently
carry a display name, status, context, and detail without an immutable
generation key. Exact following must therefore combine generation resolution
with lifecycle ownership boundaries rather than assuming every event carries a
generation field.

`hcom run` executes workflow scripts synchronously, and existing user-created
GLM/Antigravity scripts capture one-shot wait output. This change does not alter
those defaults. A terminal or tool host may independently keep the new stream
process live or yield it into the background.

## Goals / Non-Goals

**Goals:**

- Add a composable continuous event-reading process without changing `wait`.
- Support generic filtered streaming and an optional safe compact worker view.
- Reuse durable cursors, filter SQL, parsing, and listener cleanup where doing
  so does not couple stream lifetime to wait terminal semantics.
- Keep output bounded enough for a long-lived tool call and safe enough for
  model context when compact mode is selected.
- Preserve hcom messaging as the only conversation and reply mechanism.

**Non-Goals:**

- Daemonizing or supervising the stream process inside hcom.
- Changing `events --wait`, `events sub`, request watches, or result recovery.
- Automatically answering, summarizing, or converting worker messages.
- Making compact status the default for existing commands or workflows.
- Replacing terminal, transcript, snapshot, or subscription diagnostics.
- Guaranteeing how every external tool host renders or interrupts live output.

## Decisions

### 1. Add `events stream` as a separate subcommand

Extend `EventsSubcmd` with a stream-specific argument type rather than add
progress flags to `EventsArgs.wait`. The stream owns its cursor, timeout,
filters, compact mode, and optional worker-follow settings. Existing wait parser
constraints and execution stay unchanged.

Alternative: extend `events --wait` to emit multiple records. Rejected because
wait promises one matching synchronization result, existing scripts capture its
single output, and multi-record behavior would blur two distinct purposes.

Alternative: use `events sub`. Rejected because subscriptions deliberately turn
matches into messages and would make passive observation conversational noise.

### 2. Extract only the reusable listener mechanics

Factor cursor-ordered queries, optional TCP endpoint registration, bounded
rechecks, and cleanup into a small internal listener abstraction. `events_wait`
consumes at most one match through it; `events stream` repeatedly consumes
matches. Wait-specific result correlation and terminal scanning remain in the
wait path.

Use a distinct notify-endpoint kind such as `events_stream`, allowing a wait and
stream owned by the same identity to coexist. Generic event writes need not
broadcast to every observer: targeted wakes are an optimization, while a short
deterministic SQLite recheck interval guarantees progress for status, file, and
lifecycle activity. This internal polling does not create model tool calls.

Alternative: modify every event writer to connect to every stream endpoint.
Rejected initially because it increases write-path fan-out and failure surface
for an observation-only feature.

### 3. Preserve the existing filter and output surfaces in generic mode

Generic stream mode reuses `EventFilterArgs`, filter validation, SQL generation,
event parsing, and the existing streamlined/full projections. It starts after
the current event ID unless `--after-id` is supplied, emits matching records in
ascending ID order, and includes the durable event ID needed to resume.

Unlike wait, stream does not exit after a match. It ends on interruption, an
optional overall timeout, output-pipe closure, or a correlated end condition
explicitly selected by the caller.

### 4. Make compact worker status a typed projection

Adapt the staged `core/progress.rs` work into a stream-owned contract. Remove
the `input_required` terminal payload and exit code; they no longer belong in
this change. Compact activity is a closed union of lifecycle phase, file path,
allowlisted command category, and heartbeat. Raw commands, arguments,
environment values, transcript text, and message bodies are unrepresentable.

Every compact NDJSON record carries a schema version, source event time, last
observed durable cursor, and exact worker generation. Phase changes emit
immediately. Equivalent phases are deduplicated, file/command bursts are
coalesced, and quiet heartbeats are optional and rate-limited using injectable
timing for deterministic tests.

Generic full/streamlined mode retains existing event visibility and is not
advertised as secret-safe. Compact mode is the intended model-context surface.

### 5. Bind compact following to a generation lifecycle

Extract the generation discovery portion of `apply_result_correlation` without
reusing its message-only filter injection. A correlated stream resolves exactly
one live or post-cursor generation and records its immutable instance key.

Message events can be checked directly against `sender_instance_key`. Status,
file, command, and ordinary lifecycle events are currently name-scoped, so they
are accepted only while the resolved generation owns that live instance name
and before its matching terminal stopped snapshot. Provider soft stops mark
execution-loop boundaries while retaining the live generation; they remain
observable but do not end the stream, and compact projection reports them as
`listening` rather than the terminal `stopped` phase. The matching terminal
stop boundary ends a generation-follow stream before a later worker can reuse
the name. Ambiguous or missing generations fail closed.

Alternative: filter solely by display name. Rejected because a long-lived
stream could silently cross into a replacement worker.

One-shot `--result-from` waits keep their existing provider-turn semantics:
a soft stop may trigger transcript recovery when a worker did not send its
authoritative result. Continuous streams instead interpret only a terminal
stop as the end of the worker generation. This distinction preserves existing
workflow recovery without making background observation end between turns.

### 6. Keep messaging completely independent

The stream reads stored events and writes records to stdout. It does not create
subscriptions, request watches, hcom messages, or reply state. A worker-authored
`intent=request` continues through normal hcom routing and is answered with
ordinary `hcom send --reply-to`; stream output is neither a substitute nor a
second protocol.

Generic callers may explicitly filter message events just as snapshot queries
can, but compact worker status excludes message bodies and never treats a
message as a control outcome.

### 7. Flush records and let the caller own background execution

Serialize one NDJSON record per line and explicitly flush stdout. Handle broken
pipes as normal stream termination. Install bounded signal/cleanup handling so
the stream removes only its own endpoint and never changes worker state.

Hcom does not daemonize the command. A shell, terminal, workflow, or tool host
decides whether to keep it foreground, background it, or yield the live process
while other work continues.

## Risks / Trade-offs

- **[Status events lack immutable generation keys]** -> Resolve one generation,
  gate name-scoped events by live ownership, distinguish provider soft loop
  stops from terminal stops, and end at the matching terminal snapshot; test
  soft continuation, rapid name reuse, and pre/post-boundary events.
- **[Internal rechecks create database load]** -> Use one cursor query per
  bounded interval, consume batches, and wake early when existing notification
  plumbing can do so.
- **[A stream can become output spam]** -> Keep generic raw visibility explicit;
  compact mode deduplicates, coalesces, and rate-limits heartbeats with tested
  upper bounds.
- **[Tool hosts may buffer stdout]** -> Flush every record and live-test the CLI
  through supported hosts; document host limitations without adding model-side
  polling.
- **[Background process leaks listener state]** -> Use distinct endpoint
  ownership, cleanup guards, signal tests, and broken-pipe tests; stale endpoint
  cleanup remains recoverable.
- **[Refactoring listener code could change wait]** -> Pin current wait output,
  first-match exit, cursor, timeout, blocker, and recovery behavior before and
  after extraction.
- **[Existing workflow scripts capture output]** -> Do not silently modify
  defaults. Document explicit stream adoption and update a workflow only when
  separately selected for opt-in integration.

## Migration Plan

1. Rework the staged compact payload module into a stream-only contract and
   remove the wait-specific `input_required` surface.
2. Pin existing one-shot wait behavior with compatibility tests, then extract
   the minimum shared listener mechanics.
3. Add generic `events stream` with filters, cursor continuation, flushing,
   interruption, timeout, and cleanup.
4. Add exact-generation compact following, coalescing, and optional heartbeats.
5. Document explicit usage and live-test a background stream alongside normal
   hcom request/reply and an unchanged one-shot result wait.
6. Roll back by removing the stream subcommand; no stored schema, wait behavior,
   subscription, message, or workflow migration is required.
