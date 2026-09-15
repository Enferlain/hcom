## Purpose

Provide a provider-neutral, fail-closed workspace boundary in which delegated
hcom workers can perform normal development without gaining ambient host access
or repeatedly requiring routine command approval, while preserving live hcom
conversation and progress observation.

## ADDED Requirements

### Requirement: Isolation is selected by hcom
The system SHALL treat worker isolation as an hcom launch property independent
of the provider's own permission or sandbox flags. It SHALL support `off`,
`workspace`, and `workspace-git` profiles and SHALL consume isolation arguments
without forwarding them to the provider.

#### Scenario: Launch with workspace isolation
- **WHEN** a caller launches a supported worker with `--isolate workspace`
- **THEN** hcom launches it with the effective `workspace` isolation profile and the provider does not receive the hcom isolation argument

#### Scenario: Unknown isolation profile
- **WHEN** a caller requests an unknown isolation profile
- **THEN** hcom rejects the launch before creating a provider process

### Requirement: Isolation plans are inspectable before launch
The system SHALL provide commands that diagnose host support and explain the
effective isolation plan for a canonical workspace without starting a provider.
Diagnostic output SHALL identify the backend, profile, workspace, Git policy,
runtime roots, workload trust, network mode, stable workflow-policy identity,
attempt-plan identity, and failed preflight stage without exposing secret
values.

#### Scenario: Explain a valid plan
- **WHEN** a caller explains a supported profile for a valid workspace
- **THEN** hcom returns the resolved policy and performs no provider launch

#### Scenario: Diagnose an unsupported host
- **WHEN** required isolation facilities or a safe backend version are unavailable
- **THEN** the doctor command fails with an actionable reason and does not claim the host is ready

### Requirement: Isolation fails closed
The system MUST NOT retry an isolated launch without isolation or grant host
execution when preflight, plan construction, namespace startup, mount setup, or
provider startup fails.

#### Scenario: Backend setup fails
- **WHEN** the requested isolation backend cannot install the complete policy
- **THEN** hcom records a typed isolation blocker and no unrestricted provider process is started

#### Scenario: Unsupported launch backend
- **WHEN** isolation is requested for a launch backend that has not implemented the isolation contract
- **THEN** hcom rejects the launch instead of using the existing host-execution path

### Requirement: Routine in-boundary work is non-interactive
The outer hcom isolation boundary SHALL be the primary enforcement mechanism
for isolated workers. Once that boundary is active, provider adapters SHALL let
ordinary reads, workspace edits, formatting, builds, tests, and Git inspection
proceed without per-command approval. A workflow MAY also grant scoped GitHub
capabilities such as reading, creating, commenting on, or reviewing issues and
pull requests; operations inside those declared capabilities SHALL not require
repeated provider approval. External-service grants SHALL be enforced through a
scoped identity or broker rather than inferred from provider auto-approval.

Provider approval or a typed hcom blocker SHALL remain required for attempts to
escape the boundary, access protected host resources, change credentials or
security policy, perform a remote destructive operation outside the workflow's
declared capability, or exercise a capability the workflow was not granted.
Broad non-interactive provider permission MUST NOT be enabled when the outer
isolation plan is absent, incomplete, or failed, or when credentials available
to the child grant undeclared capabilities. A provider-native sandbox MAY be
disabled when it conflicts with the active outer boundary.

#### Scenario: Run a routine development command
- **WHEN** an isolated worker runs a formatter, compiler, test suite, file read, or workspace-scoped edit allowed by its effective plan
- **THEN** the command proceeds without a provider permission prompt

#### Scenario: Use an authorized GitHub capability
- **WHEN** a workflow with a scoped GitHub identity grants issue and pull-request participation and the worker performs an operation within that grant
- **THEN** the scoped identity or service broker authorizes the operation, it proceeds without per-command approval, and it is attributed to that identity

#### Scenario: Attempt an undeclared or destructive capability
- **WHEN** a worker requests boundary escape, protected host access, credential or policy mutation, or a remote destructive action outside its workflow grant
- **THEN** the operation is denied or returned as a typed actionable blocker without silently broadening the plan

#### Scenario: Isolation failed before provider startup
- **WHEN** the outer boundary cannot be installed completely
- **THEN** hcom does not enable the provider's broad non-interactive permission mode and does not launch the worker unrestricted

### Requirement: Workspace profile confines filesystem writes
The `workspace` profile SHALL allow writes only to the canonical declared
workspace and private per-run scratch or build paths. Unrelated workspaces, the
workspace parent, user configuration, common secret locations, and global hcom
state SHALL not be writable or visible unless explicitly declared by the
effective plan.

#### Scenario: Normal workspace edit
- **WHEN** an isolated worker creates, modifies, or removes a file inside its declared workspace
- **THEN** the operation succeeds subject to normal host file permissions

#### Scenario: Write outside the workspace
- **WHEN** an isolated worker attempts to write to the workspace parent, user home, another repository, or global hcom directory
- **THEN** the operation is denied by the outer isolation boundary

#### Scenario: Access a protected secret path
- **WHEN** an isolated worker attempts to read an undeclared credential or secret path
- **THEN** the path is absent or access is denied

#### Scenario: Use private runtime configuration
- **WHEN** an isolated worker uses home, XDG, temporary, cache, DNS, or TLS facilities declared by its effective plan
- **THEN** private writable state and minimum public operating-system data are available without exposing the host user's home or credential stores

### Requirement: Git metadata policy is explicit
The `workspace` profile SHALL expose the canonical Git metadata required for
read-only repository inspection but SHALL deny Git metadata mutation. The
`workspace-git` profile MAY make dedicated disposable Git metadata writable
when explicitly selected. It MUST NOT claim branch-level protection while
mounting a shared repository's common Git metadata read-write.

#### Scenario: Inspect repository state
- **WHEN** a worker in the `workspace` profile runs Git status, diff, log, blame, or equivalent read operations
- **THEN** the operation can read the repository and linked-worktree metadata

#### Scenario: Mutate Git metadata in workspace profile
- **WHEN** a worker in the `workspace` profile attempts to commit, switch branches, reset, rebase, or otherwise change Git metadata
- **THEN** the outer isolation boundary denies the metadata write

#### Scenario: Mutate Git metadata in workspace-git profile
- **WHEN** a worker receives the `workspace-git` profile
- **THEN** its writable Git state is dedicated to that grant and mutation cannot rewrite a shared checkout's common Git metadata

### Requirement: Worker processes are isolated and owned
An isolated worker SHALL not observe or signal unrelated host processes. hcom
SHALL retain ownership of the provider process tree and SHALL terminate it on
kill, cancellation, timeout cleanup, or failed startup.

#### Scenario: Inspect host processes
- **WHEN** an isolated worker enumerates processes
- **THEN** unrelated host processes are not visible

#### Scenario: Use the inherited PTY
- **WHEN** an isolated provider checks terminal identity, size, input, signals, or `/dev/tty`
- **THEN** the outer isolation boundary preserves the PTY behavior required by hcom readiness, injection, and interactive messaging

#### Scenario: Cancel an isolated workflow
- **WHEN** the coordinator cancels an isolated workflow
- **THEN** the provider and its descendants terminate before writable runtime state is removed

### Requirement: hcom state is scoped to the workflow
Isolated participants SHALL communicate through a host-owned endpoint scoped by
explicit workflow, attempt, worker-generation, thread, and message-intent
identities. They SHALL not receive direct read-write access to a global or
private hcom database, configuration, scripts, archives, transcripts, or
unrelated messages. Authorized ordinary messages, requests, blockers, results,
and bounded progress events SHALL remain available to non-isolated participants
through normal hcom commands.

#### Scenario: Worker reports completion
- **WHEN** an isolated worker sends its correlated completion result
- **THEN** the host coordinator records it against the exact workflow attempt and makes it available to ordinary hcom consumers and an optional high-level result wait

#### Scenario: Parent communicates with a live isolated worker
- **WHEN** an authorized parent sends or receives an ordinary hcom request while the isolated worker remains active
- **THEN** the host-owned endpoint routes the message without exposing either participant's database files

#### Scenario: Parent follows isolated progress
- **WHEN** an authorized parent follows the isolated worker with compact event streaming
- **THEN** correlated progress, blocker, lifecycle, and result records remain observable through the normal stream interface

#### Scenario: Worker inspects unrelated hcom state
- **WHEN** an isolated worker attempts to query a different workflow or the global hcom database
- **THEN** the unrelated state is unavailable

#### Scenario: Worker attempts direct state mutation
- **WHEN** an isolated worker attempts to open or rewrite hcom's backing database
- **THEN** no database file is available and only authorized broker operations can change host-owned state

### Requirement: Provider state is explicitly classified
Each supported provider SHALL declare the public configuration, writable
per-run state, authentication mechanism, safe environment values, and brokered
credential or service handles needed for isolated startup. The system MUST NOT
mount or copy the provider's complete user directory as a shortcut, serialize
secret values into an isolation plan, or expose them in process arguments.

#### Scenario: Supported provider starts
- **WHEN** a provider adapter can supply its validated minimum runtime state
- **THEN** the provider starts with only that declared state and private writable session storage

#### Scenario: Provider requires an unhandled durable credential
- **WHEN** safe authentication cannot be supplied under the selected profile
- **THEN** hcom reports the provider as unsupported for that profile before launch

### Requirement: Network limitations are explicit
Every isolated launch SHALL record and report an explicit workload trust class
and effective network mode. A local filesystem path MUST NOT implicitly confer
trust. Host-network preview mode SHALL be limited to explicitly trusted local
workloads, and hcom SHALL NOT represent it as hardened against exfiltration.
Untrusted workloads SHALL remain disabled until brokered egress and credentials
are enforced.

#### Scenario: Trusted preview launch
- **WHEN** an explicitly trusted local workload uses the initial workspace profile with host networking
- **THEN** launch diagnostics visibly report its trust class, `network=host`, and the narrower security boundary

#### Scenario: Untrusted workload requests host networking
- **WHEN** an untrusted repository or GitHub-event workflow requests a profile with host networking
- **THEN** hcom rejects the launch

### Requirement: Isolation identity survives lifecycle operations
The requested profile, canonical workspace, workload trust, workflow ID,
stable workflow-policy identity, attempt ID, and attempt-specific isolation
plan identity SHALL be stored with the launch. Resume and recovery SHALL retain
the stable policy inputs while creating a new attempt identity and private
runtime path, or fail before provider startup.

#### Scenario: Resume an isolated worker
- **WHEN** a caller resumes a worker whose recorded isolation inputs are still valid
- **THEN** hcom preserves the workflow-policy identity while creating and recording a distinct attempt-plan identity

#### Scenario: Resume policy drift
- **WHEN** a resume would change or cannot reconstruct the recorded workspace or isolation profile
- **THEN** hcom refuses the resume and reports the mismatch

### Requirement: Supported environments are explicit
The initial release SHALL support Linux PTY-backed launches only and SHALL
resolve runtime roots for both conventional distributions and NixOS. Other
operating systems and native-print launches SHALL fail as unsupported until an
equivalent backend exists.

#### Scenario: NixOS runtime resolution
- **WHEN** isolation is explained or launched on NixOS
- **THEN** the plan includes the read-only runtime paths needed to execute Nix store binaries

#### Scenario: Unsupported operating system
- **WHEN** a caller requests isolation on an operating system without an isolation backend
- **THEN** hcom rejects the launch without running the provider on the host

### Requirement: Containment is verified before real-provider use
The project SHALL verify filesystem, process, PTY, lifecycle, broker, and
workflow-channel containment with a fake provider before enabling a real
provider adapter.

#### Scenario: Fake-provider containment fails
- **WHEN** any required containment assertion fails
- **THEN** real-provider isolation tests and release enablement remain blocked

#### Scenario: Fake-provider containment passes
- **WHEN** all hermetic containment and cleanup assertions pass
- **THEN** the project may proceed to the ordered Antigravity and Claude-backed GLM live gates
