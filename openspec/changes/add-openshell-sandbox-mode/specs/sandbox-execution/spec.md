# Spec Delta

## Purpose

Let users launch unattended hcom workers inside an explicitly configured OpenShell boundary while preserving ordinary launches, the host workspace, and inspectable worker lifecycle state.

## ADDED Requirements

### Requirement: Sandbox execution is explicitly selected

The system SHALL select sandbox execution only when a named sandbox profile is requested. Ordinary launches and tool-owned sandbox arguments MUST retain their existing meaning. Unsupported launch combinations MUST fail before provisioning.

#### Scenario: Profile selects the sandbox location
- **WHEN** a user launches a supported CLI with `--hcom-sandbox coding`
- **THEN** the system resolves the trusted `coding` profile and reports the selected execution location before launching

#### Scenario: Tool sandbox flag is preserved
- **WHEN** a user passes a tool-owned `--sandbox` argument without the hcom sandbox option
- **THEN** the argument is forwarded according to the existing tool contract and no OpenShell sandbox is created

#### Scenario: Unsupported combination is rejected
- **WHEN** a launch combines sandbox mode with a non-headless launch, `--run-here`, `--terminal`, an unsupported desktop-backed tool, a nested sandbox launch, or a cross-device option
- **THEN** the system reports the unsupported combination without starting a host worker or provisioning a sandbox

#### Scenario: Batch provisions separate workers
- **WHEN** a supported sandbox launch requests two headless workers
- **THEN** each worker receives its own sandbox and generation in the batch's team and each provisioning outcome is reported separately

### Requirement: Launch profiles define the execution boundary

The system SHALL resolve an operator-controlled profile specifying the OpenShell executable, gateway, workspace, image, policy, provider references, and guest tool mode. It MUST verify required capabilities before launching and MUST NOT silently substitute a different gateway, runtime, or host execution location.

#### Scenario: Incompatible executable is selected
- **WHEN** the configured executable lacks the required exec or transfer capabilities
- **THEN** preflight reports the failed capability and preserves the host workspace without launching a worker

#### Scenario: Gateway cannot enforce the selected profile
- **WHEN** the selected gateway is unavailable or has a compute driver incompatible with the profile
- **THEN** the launch fails explicitly without weakening policy or falling back to another execution location

### Requirement: Guest state and credentials are isolated

Each sandbox worker SHALL have private hcom and tool state. Guest configuration MUST come from explicit profile inputs and provider references. The system MUST NOT implicitly copy host home directories, hcom databases, relay secrets, tool credentials, gateway credentials, terminal sockets, or ambient environment variables into the guest.

#### Scenario: Host has unrelated credentials and configuration
- **WHEN** a sandbox worker is launched from a host with existing hcom, shell, and tool configuration
- **THEN** only explicitly selected non-secret settings and attached provider access are available in the guest

#### Scenario: Worker attempts host filesystem access
- **WHEN** a worker attempts to read host home files or write the launcher's checkout outside its provided snapshot
- **THEN** the configured boundary denies access and the host files remain unchanged

### Requirement: Repository inputs are explicit committed snapshots

The system SHALL record the repository identity and exact input commit. The initial mode MUST transfer only a selected committed repository snapshot and its necessary Git objects, excluding host Git configuration, credentials, hooks, and unrelated refs. Dirty source trees MUST require an explicit committed-only source selection rather than silently omitting local work.

#### Scenario: Clean repository is launched
- **WHEN** a launch selects a clean repository and commit
- **THEN** the guest receives that input commit and the run record identifies it as the base

#### Scenario: Dirty repository has no explicit source selection
- **WHEN** the source includes uncommitted or untracked work and the user has not explicitly selected committed-only input
- **THEN** the launch explains that the local work would be excluded and does not provision a sandbox

#### Scenario: User explicitly chooses a commit despite local work
- **WHEN** the user explicitly selects a committed-only source in a dirty repository
- **THEN** the run records that local changes were excluded and the host working tree and index remain unchanged

### Requirement: Unattended permissions apply only inside the guest

A qualified profile SHALL allow routine guest commands without per-command approval. It MUST NOT change host tool permissions, automatically approve policy expansion, or disable the OpenShell boundary. Requests outside the selected boundary MUST surface as blocked work requiring an explicit operator decision.

#### Scenario: Worker runs routine commands
- **WHEN** a qualified worker edits its snapshot, runs tests, and installs an allowed dependency
- **THEN** it proceeds without per-command user approval and reports progress through hcom

#### Scenario: Worker needs an unapproved endpoint
- **WHEN** a worker needs network access beyond its profile
- **THEN** it reports the denial and requested access without automatically broadening policy or retrying on the host

### Requirement: Readiness identifies the worker rather than only the sandbox

The system SHALL report sandbox readiness, bridge readiness, and agent readiness separately. A successful agent launch MUST require readiness of the exact worker generation, including usable hcom delivery. A provisioned scratch shell or open exec connection alone MUST NOT establish agent readiness.

#### Scenario: Sandbox starts but agent fails
- **WHEN** OpenShell reaches Ready but the tool cannot start, authenticate, or bind to hcom
- **THEN** the launch reports failure or a specific blocker and retains diagnostic evidence

#### Scenario: Worker and bridge become ready
- **WHEN** the expected worker generation binds and the bridge completes its compatible handshake
- **THEN** the worker becomes available under its assigned team identity

### Requirement: Lifecycle operations use captured ownership

The system SHALL bind lifecycle operations to the configured gateway, workspace, captured sandbox ID, and worker generation. Disconnecting a bridge MUST NOT be treated as terminating the worker. Deletion MUST be confirmed by observing the owned resource reach an absent state within a bounded timeout.

#### Scenario: Bridge connection is interrupted
- **WHEN** the bridge disconnects while the guest worker continues running
- **THEN** the system reports lost connectivity, retains queued traffic, and reconnects without launching a second worker

#### Scenario: A display name is reused
- **WHEN** a stopped worker's display name is assigned to a new sandbox
- **THEN** an operation captured for the old generation cannot stop, resume, or delete the new sandbox

#### Scenario: Delete is asynchronous
- **WHEN** OpenShell accepts deletion and reports Deleting
- **THEN** hcom retains ownership information until absence is confirmed or reports a cleanup timeout

### Requirement: Controller ownership survives the launching terminal

Sandbox launch SHALL ensure an enabled detached host controller owns forwarding independently of the launching terminal. Sandbox sends MUST remain durably pending while the controller is absent or explicitly disabled. Eligible host activity SHALL restart a failed controller under exclusive ownership when autostart is enabled; status-only reads MUST NOT override explicit disablement.

#### Scenario: Launching terminal closes
- **WHEN** the terminal that launched a sandbox worker closes
- **THEN** the owned host controller continues forwarding messages without terminating or recreating the guest worker

#### Scenario: Controller crashes before a send
- **WHEN** an authorized host send targets a sandbox while autostart is enabled and the controller is absent
- **THEN** the send is durably queued and an exclusively owned controller is started to resume forwarding

#### Scenario: Operator disables the controller
- **WHEN** the operator explicitly stops the controller and disables autostart
- **THEN** status reads leave it stopped, sandbox sends report pending forwarding, and ordinary local messages still work

### Requirement: Observation resolves guest resources within their location

The system SHALL expose sandbox location and connection state in worker status. Supported log, transcript, and terminal-view operations MUST resolve guest resources through the owned sandbox. Unsupported operations MUST return explicit errors rather than treating guest paths, sockets, or PIDs as host resources.

#### Scenario: A transcript exists only inside the guest
- **WHEN** a user requests that worker's transcript
- **THEN** the system obtains a bounded view from the owned sandbox or reports that the operation is unavailable

#### Scenario: A guest advertises a host-looking path
- **WHEN** guest metadata contains a path matching a sensitive host location
- **THEN** host observation does not open that host path

### Requirement: Unused sandbox mode preserves existing workflows

The system SHALL preserve existing local launch, messaging, trusted-relay, and database behavior when sandbox mode is unused. Sandbox-specific state MUST be additive and MUST NOT consume upstream migration numbers or replace existing tool integrations.

#### Scenario: User continues ordinary hcom work
- **WHEN** no sandbox profile is requested and no sandbox is active
- **THEN** ordinary launches and messages work without an OpenShell installation or a running sandbox controller

#### Scenario: Sandbox support is disabled after use
- **WHEN** the user stops the sandbox controller and returns to ordinary hcom commands
- **THEN** existing host messages, instance state, and cursors remain usable and retained sandbox records do not become local processes
