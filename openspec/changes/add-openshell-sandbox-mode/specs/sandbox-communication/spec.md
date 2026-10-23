# Spec Delta

## Purpose

Keep host and sandboxed workers on the same explicitly scoped hcom team, with reliable conversations and observable delivery, without giving guest processes authority over the host.

## ADDED Requirements

### Requirement: Teams address workers independently of execution location

The system SHALL route messages among authorized host and sandboxed workers through ordinary hcom targets, threads, intents, and reply references. A sandbox-to-sandbox conversation MUST NOT require a parent agent to relay messages manually. Display aliases MUST resolve to unambiguous location-aware identities.

#### Scenario: Two sandboxed workers collaborate
- **WHEN** a worker in sandbox A sends a targeted question to a team member in sandbox B
- **THEN** the question and its reply travel through the bridge with the original conversation references preserved

#### Scenario: Display aliases collide
- **WHEN** two available workers share a display alias
- **THEN** hcom exposes distinct canonical targets and rejects an ambiguous target rather than guessing

### Requirement: Routing identities are assigned by the host

The host SHALL bind each connection to a sandbox, worker generation, and team. It MUST derive sender identity and validate conversation references against that binding. Guests MUST NOT register themselves as another worker, a host agent, or a system sender. Only the host can change team membership.

#### Scenario: Guest claims another sender
- **WHEN** a guest submits a frame claiming to be a host agent or another sandbox
- **THEN** the host rejects the frame without creating the claimed sender or delivering the forged message

#### Scenario: Old worker traffic arrives after replacement
- **WHEN** a frame from a retired worker generation arrives after a display name is reused
- **THEN** it is rejected or retained as evidence for that retired generation and cannot be attributed to the replacement

### Requirement: Discovery and subscriptions are scoped to the team

A guest SHALL see only the directory and allowed events for its assigned team. Target resolution, broadcasts, thread membership, and event subscriptions MUST be checked against that scope. Knowledge of a name or thread identifier MUST NOT grant access to another team's messages or history.

#### Scenario: Worker broadcasts to its team
- **WHEN** an enrolled guest sends a permitted broadcast
- **THEN** delivery reaches only the authorized team recipients resolved by the host

#### Scenario: Worker guesses an unrelated thread
- **WHEN** a guest requests another team's directory, event stream, or thread history
- **THEN** access is denied and unrelated host data is not included in the response

#### Scenario: Host broadcasts without a sandbox team
- **WHEN** a host agent broadcasts without selecting an authorized sandbox-team context
- **THEN** the broadcast does not acquire sandbox recipients merely because sandboxes exist

### Requirement: Accepted messages survive bridge interruption

A successful queue-acceptance response SHALL mean the message is durably recorded at that endpoint with a stable ID. Routing MUST retain it until the next endpoint acknowledges durable acceptance or a visible terminal delivery failure is recorded. Queue acceptance MUST NOT be reported as agent delivery or task completion.

#### Scenario: Sender loses connectivity before host acceptance
- **WHEN** a guest queues a message while its host connection is unavailable
- **THEN** the message remains pending in durable guest storage and is retried after reconnection without claiming recipient delivery

#### Scenario: Host restarts before forwarding
- **WHEN** the host accepts a message and restarts before forwarding it to the recipient
- **THEN** the message remains eligible for forwarding after restart

### Requirement: Replay does not create duplicate inbox messages

The bridge SHALL retry unacknowledged traffic and deduplicate by stable origin identity, database incarnation, and message sequence. Acceptance state and inbox insertion MUST commit atomically. Ordering MUST be preserved within each sender stream; cross-sender ordering and exactly-once execution of agent actions are not guaranteed.

#### Scenario: Acknowledgment is lost
- **WHEN** a receiver commits a message but its acceptance acknowledgment is lost
- **THEN** replay acknowledges the existing message without inserting a second inbox event

#### Scenario: Guest database is replaced
- **WHEN** a reconnecting guest presents a different database incarnation
- **THEN** the host rejects automatic cursor reuse and requires explicit reconciliation or a new worker generation

### Requirement: Local tool integration owns agent delivery

Imported messages SHALL enter the recipient environment's ordinary hcom inbox and trigger its local notification and tool-delivery mechanisms. The bridge MUST NOT consume the agent's unread cursor as an observer or inject message text directly into a host terminal on behalf of a guest.

#### Scenario: Recipient is idle inside a sandbox
- **WHEN** a targeted message is accepted into its guest inbox
- **THEN** the local hooks or PTY delivery wake that worker and normal delivery records reflect consumption

#### Scenario: Bridge observes outgoing messages
- **WHEN** the bridge exports messages or status events
- **THEN** unrelated messages remain unread for the worker and its delivery cursor is unchanged

### Requirement: Delivery receipts describe distinct stages

The system SHALL distinguish local queue acceptance, recipient-environment acceptance, observed delivery into agent context, and an explicit answer. Receipt references MUST use stable message identities rather than unsynchronized wall clocks. Missing delivery evidence MUST remain unknown rather than being inferred from transport success.

#### Scenario: Worker is busy after inbox acceptance
- **WHEN** its environment accepts a message but the worker has not consumed it
- **THEN** the sender sees queued or accepted status without a delivered or answered claim

#### Scenario: Worker explicitly answers
- **WHEN** the worker replies to a delivered request
- **THEN** the answer references that request independently of transport acknowledgments

### Requirement: Exec connections are bounded and reconnectable

The bridge SHALL use non-terminal framed transport with negotiated version and size limits. It MUST rotate connections before the supported OpenShell cumulative input budget is exhausted, preserving unacknowledged traffic and ensuring only the current connection owns delivery. Malformed, incompatible, oversized, or saturated input MUST produce visible failure or backpressure without silent truncation.

#### Scenario: Traffic exceeds one exec invocation's input allowance
- **WHEN** cumulative team traffic exceeds the qualified per-invocation input budget
- **THEN** the bridge rotates connections and continues delivery without losing or duplicating inbox events

#### Scenario: Replaced connection continues writing
- **WHEN** an older bridge connection emits frames after a replacement becomes active
- **THEN** those frames cannot alter current routing or lifecycle state

#### Scenario: Queue reaches its bound
- **WHEN** durable queue capacity is exhausted
- **THEN** new sends fail or backpressure explicitly and no accepted message is silently discarded

#### Scenario: Reverse output stalls
- **WHEN** guest-to-host output is sustained or the host reader stalls
- **THEN** transport applies bounded backpressure and recovers or reports a connection failure without claiming delivery of unread frames

### Requirement: Guest traffic has aggregate admission limits

The system SHALL enforce finite message-count and byte budgets per guest run and team, pacing for guest-origin traffic, and a host-wide storage bound before accepting payloads. Limits MUST cover already delivered history and diagnostics, not only pending queues. Exhaustion MUST produce bounded backpressure or a visible failure without growing host inboxes or error logs indefinitely.

#### Scenario: Guest floods valid small messages
- **WHEN** a guest repeatedly submits individually valid frames beyond its rate or cumulative budget
- **THEN** the host withholds admission or closes the violating connection with bounded diagnostics and does not append unbounded inbox or history rows

#### Scenario: Several guests share a budget
- **WHEN** multiple guests together reach the team's or host's aggregate bound
- **THEN** further admission is limited even if each guest remains under its individual bound

### Requirement: Imported content cannot inject terminal controls

The bridge SHALL reject guest message text containing C0 or C1 control characters other than newline and tab, including DEL. Identifier and metadata fields MUST satisfy their stricter schemas. Rejection MUST occur before inbox delivery and remain visible to the sender; ordinary local message behavior MUST remain unchanged.

#### Scenario: Guest submits an escape or key-control sequence
- **WHEN** guest text contains ESC, BEL, carriage return, DEL, or a C1 control
- **THEN** it is rejected without entering a host renderer or PTY input path

#### Scenario: Guest submits ordinary multiline code
- **WHEN** otherwise valid text contains newline and tab for formatting
- **THEN** those characters remain valid and the ordinary local delivery mechanism controls submission to the recipient

### Requirement: Guest senders normalize terminal-derived text before acceptance

A compliant guest sender SHALL escape prohibited text controls into visible notation before canonical digesting and durable enqueue, retaining newline and tab. It MUST report normalization to the caller and reject invalid identifiers rather than rewriting them. Host-side validation MUST remain independent of this convenience behavior.

#### Scenario: Worker quotes colored command output
- **WHEN** the normal guest send adapter receives text containing terminal color escape bytes
- **THEN** it queues a visibly escaped representation with a stable digest and reports the normalization without requiring the worker to resubmit

#### Scenario: Guest bypasses normalization
- **WHEN** a guest submits raw prohibited controls directly over the bridge
- **THEN** host-side rejection still prevents inbox delivery

### Requirement: Messaging does not grant host control authority

Guest frames SHALL be limited to authorized conversations, receipts, bounded status, and result data. They MUST NOT authorize host launch, kill, resume, terminal injection, configuration changes, arbitrary SQL, filesystem reads, or policy expansion. Host-only management requests MUST use a separate authority path. Guests MUST NOT receive the host's trusted-relay credentials.

#### Scenario: Guest submits a control action
- **WHEN** a guest submits a launch, kill, configuration, or terminal-injection action as bridge data
- **THEN** it is rejected without invoking a host control handler

#### Scenario: Guest includes command syntax in message text
- **WHEN** an authorized message contains shell commands or an instruction to a host agent
- **THEN** it is delivered only as attributed message content and is never executed by the bridge

### Requirement: Imported state cannot escape its transport scope

The system SHALL mark sandbox-origin state and events with host-assigned provenance. Guest metadata MUST NOT install host notification endpoints, PIDs, paths, or trusted capabilities. Sandbox traffic MUST NOT be automatically re-exported into the all-trusted MQTT relay or removed by its unrelated peer-cleanup rules.

#### Scenario: Trusted relay is also enabled
- **WHEN** sandbox-origin messages and statuses arrive on a host that participates in an MQTT relay
- **THEN** they remain within their authorized sandbox team and do not become trusted relay control or state traffic

#### Scenario: MQTT peer cleanup runs
- **WHEN** ordinary stale-relay cleanup executes while sandbox workers are connected or temporarily offline
- **THEN** it does not delete or reclassify sandbox-owned identities

### Requirement: Connection state and agent claims remain distinguishable

The system SHALL expose bridge connectivity separately from guest-reported activity and gateway-confirmed sandbox lifecycle. Replayed or stale status MUST NOT overwrite newer observations. A guest's claimed completion, idle state, or heartbeat MUST NOT establish host-verified correctness or override gateway-confirmed termination.

#### Scenario: Worker appears active but connection is stale
- **WHEN** the last activity report says active and the bridge loses contact
- **THEN** status shows the last observed activity together with lost connectivity

#### Scenario: Old status is replayed
- **WHEN** a lower-sequence activity frame arrives after a newer state or confirmed termination
- **THEN** the newer observation remains authoritative
