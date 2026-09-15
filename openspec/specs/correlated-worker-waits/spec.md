# Correlated Worker Waits Specification

## Purpose

Provide an optional continuous event stream for background worker visibility
without changing one-shot waits or replacing ordinary hcom conversation.

## Requirements

### Requirement: Event streaming is distinct from one-shot waiting
The system SHALL provide a continuous event-stream mode separate from
`events --wait`. Starting, consuming, or stopping a stream MUST NOT change the
matching, output, exit status, timeout, result recovery, or blocker behavior of
an existing one-shot wait.

#### Scenario: One-shot wait receives a match
- **WHEN** a caller uses `events --wait` and a matching event occurs
- **THEN** the command emits that one match and exits with its existing behavior

#### Scenario: Stream receives multiple matches
- **WHEN** a caller uses event-stream mode and multiple matching events occur
- **THEN** the command emits each match in cursor order and remains active

### Requirement: Streams use durable event boundaries and existing filters
An event stream SHALL start after the current durable cursor by default and
SHALL allow a caller to supply an earlier durable event ID to close the gap
between arming and process startup. It SHALL apply the existing composable event
filter semantics to every emitted event and include enough cursor information
for deterministic continuation.

#### Scenario: Event arrives before stream startup
- **WHEN** a caller captures a cursor, an event occurs, and the caller starts a stream from that cursor
- **THEN** the stream emits the matching event without requiring a snapshot query

#### Scenario: Unrelated event occurs
- **WHEN** an event does not satisfy the stream filters
- **THEN** the stream advances safely without emitting that event as a match

#### Scenario: Stream is resumed
- **WHEN** a caller restarts a stream after the ID of its last emitted event
- **THEN** already-consumed events are not emitted again

### Requirement: Stream output is live and lifecycle-safe
The system SHALL encode stream records as line-delimited structured output and
SHALL flush every emitted record. A stream SHALL run until explicitly stopped,
its optional timeout expires, or an explicitly selected correlated terminal
lifecycle boundary occurs. A provider's soft execution-loop stop MUST remain
observable without ending a follow while that worker generation remains live.
Exiting a stream MUST clean up listener state and MUST NOT stop or otherwise
mutate the observed worker.

#### Scenario: Tool host keeps the command live
- **WHEN** a tool host yields the running stream process into the background
- **THEN** later records become available from the same process without another hcom query

#### Scenario: Stream is interrupted
- **WHEN** the caller terminates the stream
- **THEN** the stream removes its listener registration and leaves observed agents running

#### Scenario: Output consumer closes
- **WHEN** the stream output pipe closes
- **THEN** the stream terminates cleanly without repeated errors or stale listener state

### Requirement: Compact worker status is optional, bounded, and safe
The system SHALL allow an event stream following a worker to select a compact
status projection. Compact output MUST deduplicate unchanged phases, coalesce
repetitive file and command activity, rate-limit quiet heartbeats, and omit raw
command strings, arguments, environment values, transcript text, and message
bodies. Every compact record SHALL identify its schema version, event time,
durable cursor, and correlated worker generation.

#### Scenario: Repetitive activity occurs
- **WHEN** a worker produces repeated equivalent status, file, or command events
- **THEN** compact mode emits at most the configured update cadence rather than one record per event

#### Scenario: Worker becomes quiet
- **WHEN** no new correlated activity occurs for the configured heartbeat interval
- **THEN** compact mode emits one rate-limited heartbeat containing the last known phase and activity time

#### Scenario: Command detail contains credentials
- **WHEN** a command event contains raw arguments or environment values
- **THEN** compact output exposes only an allowlisted command category

### Requirement: Worker-generation following does not cross reuse boundaries
When a stream follows a worker attempt, the system SHALL bind observation to one
immutable worker generation. Name-only status and lifecycle events MAY be used
only while that generation owns the live name and before its matching terminal
stop boundary. Provider soft stops that retain the live generation MUST NOT end
the stream; a later worker reusing the display name MUST NOT enter the stream.

#### Scenario: Provider execution loop stops but worker remains live
- **WHEN** the followed generation emits a soft stop and remains available for another turn
- **THEN** the stream continues following the same generation and compact output reports a nonterminal listening phase rather than stopped

#### Scenario: Worker name is reused
- **WHEN** the followed generation stops and another generation later acquires the same display name
- **THEN** the original correlated stream emits no activity from the later generation

#### Scenario: Generation cannot be resolved uniquely
- **WHEN** the requested worker could refer to zero or multiple generations after the supplied cursor
- **THEN** the stream fails closed with an actionable correlation error

### Requirement: Streaming does not become conversation
An event stream MUST NOT create hcom messages, subscriptions, request watches,
replies, or model-authored summaries as a side effect of observation. Worker
messages SHALL continue through ordinary hcom delivery, and callers SHALL use
ordinary hcom messaging to respond.

#### Scenario: Worker sends a request
- **WHEN** an observed worker sends an `intent=request` message to another agent
- **THEN** normal hcom delivery handles the authored request independently of the status stream

#### Scenario: Status activity is observed
- **WHEN** compact mode emits worker activity
- **THEN** no corresponding `[hcom-events]` message or conversational event is created

### Requirement: Stream adoption is explicit
Existing event queries, waits, subscriptions, and workflows SHALL preserve their
current defaults. A stream SHALL start only through an explicit stream command
or an explicit workflow option.

#### Scenario: Existing workflow runs
- **WHEN** a workflow has not opted into event streaming
- **THEN** it retains its existing output and waiting behavior

#### Scenario: Caller opts into observation
- **WHEN** a caller explicitly enables a stream or compact-follow option
- **THEN** the stream may run alongside other interaction without changing it
