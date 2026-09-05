## Purpose

Provide a provider-neutral, fail-closed workspace boundary in which unattended
hcom workers can perform normal development without gaining ambient host access
or repeatedly requiring routine command approval.

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
runtime roots, network mode, and failed preflight stage without exposing secret
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

### Requirement: Git metadata policy is explicit
The `workspace` profile SHALL expose the canonical Git metadata required for
read-only repository inspection but SHALL deny Git metadata mutation. The
`workspace-git` profile MAY make the exact repository Git metadata writable
when explicitly selected.

#### Scenario: Inspect repository state
- **WHEN** a worker in the `workspace` profile runs Git status, diff, log, blame, or equivalent read operations
- **THEN** the operation can read the repository and linked-worktree metadata

#### Scenario: Mutate Git metadata in workspace profile
- **WHEN** a worker in the `workspace` profile attempts to commit, switch branches, reset, rebase, or otherwise change Git metadata
- **THEN** the outer isolation boundary denies the metadata write

### Requirement: Worker processes are isolated and owned
An isolated worker SHALL not observe or signal unrelated host processes. hcom
SHALL retain ownership of the provider process tree and SHALL terminate it on
kill, cancellation, timeout cleanup, or failed startup.

#### Scenario: Inspect host processes
- **WHEN** an isolated worker enumerates processes
- **THEN** unrelated host processes are not visible

#### Scenario: Cancel an isolated workflow
- **WHEN** the coordinator cancels an isolated workflow
- **THEN** the provider and its descendants terminate before writable runtime state is removed

### Requirement: hcom state is scoped to the workflow
Isolated participants SHALL communicate through hcom state scoped by explicit
workflow and attempt identities. They SHALL not receive read-write access to the
user's global hcom configuration, scripts, archives, transcripts, or unrelated
messages.

#### Scenario: Worker reports completion
- **WHEN** an isolated worker sends its correlated completion result
- **THEN** the host coordinator receives that result from the workflow-scoped channel and returns it through the high-level workflow

#### Scenario: Worker inspects unrelated hcom state
- **WHEN** an isolated worker attempts to query a different workflow or the global hcom database
- **THEN** the unrelated state is unavailable

### Requirement: Provider state is explicitly classified
Each supported provider SHALL declare the public configuration, writable
per-run state, and authentication material needed for isolated startup. The
system MUST NOT mount or copy the provider's complete user directory as a
shortcut.

#### Scenario: Supported provider starts
- **WHEN** a provider adapter can supply its validated minimum runtime state
- **THEN** the provider starts with only that declared state and private writable session storage

#### Scenario: Provider requires an unhandled durable credential
- **WHEN** safe authentication cannot be supplied under the selected profile
- **THEN** hcom reports the provider as unsupported for that profile before launch

### Requirement: Network limitations are explicit
Every isolated launch SHALL report its effective network mode. Host-network
preview mode SHALL be limited to trusted local repositories, and hcom SHALL NOT
represent it as hardened against exfiltration. Untrusted workloads SHALL remain
disabled until brokered egress and credentials are enforced.

#### Scenario: Trusted preview launch
- **WHEN** a trusted local repository uses the initial workspace profile with host networking
- **THEN** launch diagnostics visibly report `network=host` and the narrower security boundary

#### Scenario: Untrusted workload requests host networking
- **WHEN** an untrusted repository or GitHub-event workflow requests a profile with host networking
- **THEN** hcom rejects the launch

### Requirement: Isolation identity survives lifecycle operations
The requested profile, canonical workspace, workflow ID, attempt ID, and
effective isolation plan identity SHALL be stored with the launch. Resume and
recovery SHALL reuse those values or fail before provider startup.

#### Scenario: Resume an isolated worker
- **WHEN** a caller resumes a worker whose recorded isolation inputs are still valid
- **THEN** hcom recreates the same effective boundary for the new attempt

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
The project SHALL verify filesystem, process, lifecycle, and workflow-channel
containment with a fake provider before enabling a real provider adapter.

#### Scenario: Fake-provider containment fails
- **WHEN** any required containment assertion fails
- **THEN** real-provider isolation tests and release enablement remain blocked

#### Scenario: Fake-provider containment passes
- **WHEN** all hermetic containment and cleanup assertions pass
- **THEN** the project may proceed to the ordered Antigravity and Claude-backed GLM live gates
