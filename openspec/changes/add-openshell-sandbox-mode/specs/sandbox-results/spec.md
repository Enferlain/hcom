# Spec Delta

## Purpose

Make sandbox task outcomes useful to other agents by correlating explicit final reports with their requests and by ensuring referenced Git artifacts are available independently of the guest filesystem.

## ADDED Requirements

### Requirement: Final results identify the exact request and worker

A sandbox final result SHALL include a stable result ID, originating request reference, thread, exact worker generation, outcome, summary, and validation claims. Supported outcomes MUST distinguish completed, failed, and cancelled work. Only an authorized result from the assigned worker generation can satisfy its result wait.

#### Scenario: Assigned worker finishes a request
- **WHEN** the expected worker submits a valid final result for its request and thread
- **THEN** the requester receives the correlated outcome and provenance

#### Scenario: Another worker or generation reports completion
- **WHEN** a result refers to the same thread but originates from another worker or a replaced generation
- **THEN** it cannot satisfy the original request's result wait

### Requirement: Progress and receipts cannot complete a result wait

The system SHALL distinguish explicit final results from progress, informational messages, delivery acknowledgments, and idle status. Sandbox-aware result waits MUST require the final-result marker and request correlation. Existing non-sandbox message and result-wait contracts MUST remain compatible.

#### Scenario: Worker reports progress in the request thread
- **WHEN** the worker sends an informational progress update or becomes idle before finishing
- **THEN** the request remains pending and the update is observable without being treated as completion

#### Scenario: Ordinary worker uses the existing result contract
- **WHEN** a non-sandbox worker reports a result using the existing supported message contract
- **THEN** the sandbox feature does not silently require new fields from that worker

### Requirement: Artifact availability is established before successful handoff

A completed result requiring Git artifacts SHALL identify the repository, input base commit, result commit, and an authorized retrieval location or transfer ID. The system MUST verify that those objects are available through the declared route before announcing a successful handoff. A guest-local path alone MUST NOT establish availability.

#### Scenario: Commit exists only in the guest
- **WHEN** a worker reports a result commit that has not reached an authorized retrieval location
- **THEN** the handoff remains pending or reports artifact transfer failure rather than successful completion

#### Scenario: Work produces no code artifact
- **WHEN** a request requires an analysis-only result and declares no Git artifacts
- **THEN** a valid correlated report can complete the request without inventing a commit

### Requirement: Team members can retrieve immutable Git outputs

An authorized teammate SHALL be able to retrieve the result's exact Git objects into a separate workspace through the declared route. The handoff MUST preserve the input base and result commit and MUST NOT depend on a mutable branch name alone. The system MUST reject mismatched repositories, unavailable prerequisites, and invalid object identities.

#### Scenario: Result branch moves after publication
- **WHEN** a branch advances after a result was published
- **THEN** the recipient still retrieves the immutable commit identified by that result

#### Scenario: Another sandbox needs the artifact
- **WHEN** an authorized worker in sandbox B requests the artifact produced in sandbox A
- **THEN** the host routes the Git transfer to B's separate workspace without requiring a parent agent to copy files manually

### Requirement: Collection preserves host workspace and execution authority

Collected Git outputs SHALL enter a bounded staging location under host control. Collection MUST NOT import guest Git configuration or hooks, execute guest-supplied host commands, or automatically check out, merge, or cherry-pick changes into the launcher's worktree. Applying and executing returned work requires the host workflow's existing authorization.

#### Scenario: Collection succeeds while host has local changes
- **WHEN** a result bundle is collected into staging
- **THEN** the host working tree and index remain unchanged and the result can be inspected separately

#### Scenario: Guest submits an unsafe artifact descriptor
- **WHEN** a descriptor contains a path traversal, unsupported URL scheme, oversized artifact, or a command instead of a Git reference
- **THEN** collection fails explicitly without accessing the requested host path or executing the command

### Requirement: Final results survive transport retries and cleanup

A submitted final result SHALL remain durable until accepted by the host. Host acceptance MUST preserve its report and artifact availability state before cleanup. Replaying the same result MUST be idempotent; conflicting terminal results for the same request and generation MUST be surfaced rather than replacing an accepted outcome.

#### Scenario: Result acknowledgment is lost
- **WHEN** the guest replays a final result after the host has accepted it
- **THEN** the host acknowledges the existing result without announcing a second completion

#### Scenario: Cleanup is requested before collection finishes
- **WHEN** required artifacts or the final report are still only in the guest
- **THEN** automatic cleanup is deferred and explicit destructive cleanup reports what would be lost

### Requirement: Assigned requests reserve bounded final-report capacity

Each assigned request SHALL reserve finite terminal-report capacity within aggregate storage limits before execution. Progress traffic MUST NOT consume it. A valid bounded final report MUST remain admissible after ordinary payload credit is exhausted; replay MUST NOT allocate additional capacity or bypass artifact availability checks.

#### Scenario: Progress exhausts the ordinary budget
- **WHEN** an assigned worker exhausts ordinary traffic credit and then submits a final report within its reserved limit
- **THEN** the host can durably accept the report and report its artifact state without budget renewal or forfeiting the result

#### Scenario: No final-report reservation is available
- **WHEN** a new request cannot reserve its terminal-report capacity within aggregate limits
- **THEN** assignment fails before execution rather than launching work whose report has no admission capacity

### Requirement: Missing results remain visibly unresolved

The system SHALL distinguish reported failure, cancellation, lost connectivity, and a worker that stopped without a final result. It MUST NOT infer completed work from a zero exit status, idle state, sandbox termination, or a locally inaccessible transcript. Recovery of a report MUST preserve its source and exact request correlation.

#### Scenario: Worker stops before submitting its report
- **WHEN** the owned worker generation terminates without an accepted final result
- **THEN** its request reports missing result or a confirmed failure and diagnostic evidence is retained

#### Scenario: Optional transcript recovery is unavailable
- **WHEN** the host cannot retrieve or correlate a guest transcript through the owned sandbox
- **THEN** recovery fails visibly and no host filesystem path is substituted

### Requirement: Validation claims retain their provenance

Result reports SHALL distinguish worker-reported validation from independently observed host validation. Validation evidence MUST identify the tested commit and execution environment when present. Artifact transfer or successful sandbox execution MUST NOT be represented as proof that the returned code is correct.

#### Scenario: Worker reports passing tests
- **WHEN** the final report states that guest tests passed
- **THEN** the requester sees the tested commit, guest environment reference, and worker-reported provenance

#### Scenario: Host performs additional checks
- **WHEN** the host independently tests the collected commit
- **THEN** its validation record is attributed separately and does not overwrite the original guest report
