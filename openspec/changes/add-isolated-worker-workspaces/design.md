## Context

See `proposal.md` for motivation and
`specs/isolated-worker-workspaces/spec.md` for the behavioral contract. The
current PTY launch path is `commands::launch -> launcher::launch -> runner
script -> terminal::launch_terminal -> hcom pty -> pty::Proxy::spawn`. The PTY
proxy owns supervision outside the final provider process, giving hcom a single
provider-neutral point at which to install an outer boundary.

The first supported host is Linux/NixOS. It has working unprivileged user
namespaces, but its current Bubblewrap 0.11.2 is below the required safe version
and its executables resolve through `/run/current-system/sw` into `/nix/store`.
The implementation must therefore discover platform runtime roots and reject
Bubblewrap versions older than 0.12.0.

## Goals / Non-Goals

**Goals:**

- Keep hcom's PTY supervision and process ownership outside the sandbox.
- Apply one isolation contract to PTY-backed providers without encoding the
  policy separately for every CLI.
- Make the effective boundary inspectable, testable, resumable, and fail-closed.
- Support normal trusted-repository editing, Rust build/test workflows, and
  isolated hcom messaging, progress observation, and completion reporting.
- Let routine work inside an active outer boundary proceed without repetitive
  provider approvals while retaining explicit capability gates for dangerous or
  out-of-scope actions.
- Establish workflow and attempt identity as the tenancy key for runtime state.
- Keep workflow identity and workspace ownership independent so later sessions
  can explicitly share a workspace without collapsing their isolation state.

**Non-Goals:**

- Refactoring the full launcher or `Tool`/`LaunchTool` model first.
- Building a general container runtime or adding a required daemon.
- Claiming hostile-code containment while networking remains shared.
- Supporting native-print, macOS, Windows, arbitrary global hcom messaging, or
  untrusted GitHub-event workloads in the first milestone.
- Defining concurrent-edit, locking, conflict-resolution, or branch-ownership
  policy for a future shared-workspace mode.

## Decisions

### Isolation is a composable launch dimension

Represent isolation independently from provider identity and launch mode:

```text
Tool + LaunchBackend + IsolationProfile + WorkflowRole
```

Do not add sandbox-specific `LaunchTool` variants. A future
`LaunchTarget { tool, mode }` cleanup remains possible but is deliberately
separate from this change.

**Alternative considered:** Normalize `Tool` and `LaunchTool` before adding
isolation. Rejected because it changes every launch route and increases the
regression surface before containment exists.

### A dedicated isolation runtime owns policy

Add `src/isolation/` containing profile parsing, plan resolution, preflight,
platform runtime discovery, backend argument construction, and runtime cleanup.
`commands/launch.rs` parses intent and `launcher.rs` coordinates it; neither
constructs Bubblewrap arguments directly.

Core values:

- `IsolationProfile`: `off`, `workspace`, or `workspace-git`.
- `WorkflowId` and `AttemptId`: opaque validated identifiers.
- `WorkloadTrust`: an explicit trust/provenance classification; a local path is
  not itself evidence that its contents or task input are trusted.
- `IsolationPlan`: canonical workspace, Git paths, mounts, environment keys,
  backend, network mode, trust, runtime path, stable workflow-policy identity,
  and attempt-specific plan identity.
- `IsolationRuntime`: owns prepared sidecars and cleanup state.

The plan is serializable, but secret values are never part of it or launch
records.

**Alternative considered:** Add isolation fields directly throughout the
launcher. Rejected because policy ownership would bleed into an already broad
coordination module and make independent containment tests difficult.

### Install the boundary at the PTY provider-child spawn

The runner and `hcom pty` remain on the host. Immediately before
`pty::Proxy::spawn` creates the provider, hcom replaces the child command vector
with `bwrap <resolved plan> -- <provider> <args>`. The PTY slave is inherited by
the sandboxed provider while the master and delivery loop remain outside.

The resolved plan reaches `hcom pty` through a private sidecar or inherited file
descriptor. It is parsed into an argument vector; no shell interpolation is
used for mount paths or provider arguments.

**Alternative considered:** Wrap the whole terminal runner. Rejected because it
would unnecessarily move hcom supervision and global state inside the boundary.

**Alternative considered:** Rely on each provider's native sandbox. Rejected
because provider policies differ and the live Antigravity sandbox failed before
requesting host bypass.

### Bubblewrap is the first backend and fails closed

The Linux backend begins with an empty mount namespace and adds validated
read-only runtime roots, the workspace, Git metadata, private temp/runtime
paths, declared caches, provider state, and a workflow-scoped broker endpoint.
It uses separate user, PID, IPC, UTS, and cgroup namespaces, parent-death
handling, dropped capabilities, and mandatory nested-user-namespace disabling.
The existing PTY proxy already creates a session and controlling terminal, so
the backend does not add Bubblewrap's `--new-session` unless the PTY containment
suite proves that doing so preserves `/dev/tty`, terminal sizing, signals, and
interactive input.

`hcom isolation doctor` verifies:

- supported operating system and architecture;
- resolved Bubblewrap version of at least 0.12.0;
- no unexpected setuid bit or file capabilities;
- usable unprivileged user namespaces;
- platform runtime-root resolution;
- a minimal namespace execution probe.

`hcom isolation explain` resolves and renders the plan without launching a
provider. Any incomplete plan is an error. There is no `workspace -> off`
fallback.

**Alternative considered:** Accept the currently installed Bubblewrap 0.11.2.
Rejected because the 0.12.0 release fixes a sandbox-setup symlink traversal and
mount construction touches potentially attacker-controlled workspaces.

### Runtime mounts are platform-aware

Conventional Linux and NixOS use separate runtime-root resolvers. NixOS exposes
`/nix` and `/run/current-system/sw` read-only; conventional Linux resolves only
the existing system roots required by the provider and development tools.

The workspace is canonicalized before untrusted execution. Linked-worktree Git
indirection is resolved explicitly. The workspace is mounted read-write, then
its `.git` file/directory and common Git directory are over-mounted read-only in
the `workspace` profile. Read-only Git execution sets `GIT_OPTIONAL_LOCKS=0` so
inspection does not fail while attempting an optional index refresh.

`workspace-git` must not imply branch-level protection that ordinary writable
Git metadata cannot enforce. The preview profile uses dedicated disposable Git
state, such as a private clone, when branch mutation is enabled; directly
mounting a shared worktree's common Git directory read-write is not sufficient.

Writable build output remains inside the workspace or a private runtime path.
Nix stores and dependency source caches are read-only. Credential files such as
Cargo's credentials are excluded. Each plan also constructs a private `HOME`,
`XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, `XDG_STATE_HOME`, and `XDG_RUNTIME_DIR`, and
adds only the public operating-system files needed for identity, DNS, and TLS.
Provider and external-service proxy endpoints, CA material, and safe placeholder
environment values are contributed explicitly by an adapter or broker.

**Alternative considered:** Bind the host root read-only and overlay writable
paths. Rejected because it exposes unrelated files and secrets and makes the
allowlist harder to audit.

### hcom state is host-owned and workflow-scoped

The sandbox never receives a writable SQLite database. A per-run broker owned
by the host-side PTY coordinator exposes only the hcom operations authorized for
the workflow and binds each request to the recorded workflow, attempt, worker
generation, thread, and message intent. The broker writes accepted lifecycle,
message, request, blocker, result, and bounded progress events into host-owned
hcom state.

Normal `hcom agy` and `hcom claude` workers therefore remain visible and
reachable through ordinary hcom messaging and the compact event stream. The
high-level `hcom run` wrapper may consume the same correlated result channel,
but it is not the only supported interaction model and does not define the
tenancy boundary.

Runtime and broker identities are keyed by `WorkflowId` and `AttemptId`, not
only an instance name. This introduces the minimum first-class workflow
identity needed for tenancy without mounting global state or redesigning every
existing message and instance table.

**Alternative considered:** Mount global `~/.hcom` read-write. Rejected because
the worker could inspect or corrupt unrelated messages, scripts, transcripts,
configuration, and launches.

**Alternative considered:** Mount a private workflow SQLite database read-write.
Rejected because a worker with shell access could inspect, rewrite, or forge
participant state and because the separate database would disconnect ordinary
hcom messaging and progress streams. A narrow per-run broker is smaller than a
general cross-workflow broker and is required in milestone 1.

### Workflow identity does not own the workspace

Workflow and attempt identities authorize communication and runtime resources;
they are not derived from the canonical workspace path and cleanup never owns
or deletes that workspace. Initial launches may require an exclusive workspace
grant, but plan, broker, and lifecycle records must allow a future explicit
shared grant to reference the same workspace from multiple sessions while
keeping their identities, messages, results, and cleanup independent.

The future sharing mode still needs a deliberate coordination policy for
concurrent edits, branches, locking, and conflict handling. This change only
preserves that design path; it does not enable implicit workspace sharing.

### Provider-specific behavior is a small capability

Keep `IntegrationSpec` declarative. Add a small optional `IsolationAdapter` that
classifies public configuration, writable per-run state, authentication, and
additional validated mounts/environment plus credential or service-broker
handles. Secret values are not serialized into an isolation plan or exposed in
the Bubblewrap argument vector. Do not create one large provider trait covering
launch, hooks, delivery, transcript, session, and isolation.

Antigravity is the first real adapter after fake-provider containment because it
is the current blocker. Claude-backed GLM follows against the same runtime.

**Alternative considered:** Copy or mount each provider's complete user config
directory. Rejected because provider child commands could read durable
credentials and mutate persistent settings.

### The outer boundary owns enforcement; inner permissions are risk-based

An active, validated hcom isolation plan is the prerequisite for unattended
provider permissions. Inside that boundary, adapters configure providers to
proceed automatically for ordinary file reads, workspace edits, formatting,
builds, tests, and Git inspection. Requiring an LLM-mediated approval for
commands such as `cargo fmt` adds supervision cost without strengthening the
filesystem or process boundary.

An adapter may disable a conflicting provider-native sandbox or select the
provider's broad non-interactive mode only after the outer boundary is active
and every external-service credential available to the child is scoped or
brokered. Provider flags named "bypass" are not categorically forbidden: their
safety comes from the enforced outer boundary and capability surface, not their
name. Without those prerequisites the adapter fails before startup.

Remote operations use explicit workflow capabilities enforced by scoped
identities or service brokers; provider auto-approval alone is not
authorization.
For example, a GitHub-participation workflow may grant issue and pull-request
read/write/review operations to its agent account without prompting for each
`gh` invocation. Credential changes, repository administration, secret access,
branch deletion, force operations, merges, or other remote destructive actions
remain denied unless the workflow grants the narrower capability explicitly.

The provider's permission system remains defense in depth for boundary escape,
protected resources, security-policy changes, and undeclared capabilities. If
the hcom boundary is off or fails preflight, adapters must not select broad
non-interactive modes. Provider prompt recognition is still required so an
unexpected guard becomes a typed blocker rather than a silent stall.

**Alternative considered:** Maintain a long allowlist of exact routine command
prefixes. Rejected because normal tools compose commands in many equivalent
forms, the list becomes provider-specific maintenance, and it still asks the
model to mediate harmless work. The isolation plan and workflow capability are
the stable policy units.

### Networking is honest and staged

Milestone 1 records an explicit `trusted-local` workload classification and
shares host networking only for that class, reporting both trust and
`network=host` on every launch. A local path does not establish trust by
itself. Trust may come from an explicit launch choice or a stored policy keyed
to canonical workspace and repository identity, so daily use does not require a
repeated prompt. This provides filesystem/process isolation for immediate use
but is not described as hardened against exfiltration. Untrusted issue text,
pull requests, and repositories are rejected until brokered credentials and
egress exist.

**Alternative considered:** Disable networking. Rejected for real providers
because inference requires network access.

**Alternative considered:** Claim host networking is safe because filesystem
mounts are restricted. Rejected because exposed provider credentials or
repository data could still leave through arbitrary egress.

### Isolation lifecycle is durable and observable

Store requested profile, effective backend, canonical workspace, workload
trust, workflow ID, stable workflow-policy identity, attempt ID, Git policy,
network mode, runtime path, and an attempt-specific non-secret plan digest with
the launch. `hcom list -v` and structured launch results expose them.

Lifecycle stages are:

```text
preflight -> plan_resolved -> namespace_started -> provider_started
          -> ready -> running -> result -> namespace_stopped -> cleaned
```

Setup failures emit a typed `isolation` blocker with the failed stage. Resume
creates a new attempt and runtime path but must reproduce the stable recorded
workflow policy; the attempt plan digest is expected to change. Cancellation
and timeout terminate the provider namespace before deleting writable runtime
state. A cleanup warning after an authoritative result does not erase that
result.

## Risks / Trade-offs

- **[Host networking allows egress]** → Restrict preview use to trusted local
  repositories, display the network mode, and block untrusted workflows until
  egress is brokered.
- **[Provider authentication may be visible to child commands]** → Require an
  explicit provider adapter and reject providers whose credentials cannot yet
  be supplied safely.
- **[Bubblewrap policy mistakes create a false security claim]** → Require
  version/platform preflight, renderable plans, canonical paths, an empty-root
  allowlist, and fake-provider containment tests.
- **[Read-only Git metadata breaks tools that take locks]** → Test common Git
  inspection commands with optional locks disabled and reserve
  `workspace-git` for disposable, dedicated Git state.
- **[NixOS closures require many runtime paths]** → Mount the Nix store and
  system profile read-only and test executable resolution separately from
  conventional Linux.
- **[A writable worker database permits state forgery]** → Keep SQLite on the
  host and expose only an authenticated, workflow-scoped broker protocol.
- **[The broker accidentally becomes a second orchestration framework]** → Keep
  it transport-only: authorize and relay typed hcom operations without model
  reasoning, task scheduling, or a required daemon.
- **[A new workflow identity starts a larger domain migration]** → Introduce
  opaque IDs and launch metadata only; defer broad schema normalization.
- **[Provider-native and hcom sandboxes conflict]** → Provider adapters select
  compatible flags, and the fake-provider gate proves the outer boundary before
  real-provider tuning.
- **[Broad provider permissions are enabled without containment]** → Make an
  active validated isolation plan plus scoped external-service credentials a
  hard prerequisite and fail before provider startup rather than weakening
  either layer.
- **[Process isolation is mistaken for resource limiting]** → Document that a
  cgroup namespace does not impose CPU, memory, disk, or network quotas; rely on
  timeout/kill initially and track quotas separately.

## Migration Plan

1. Add plan/config types, explicit workload trust, stable workflow-policy and
   attempt identities, CLI parsing, `doctor`, and `explain` without changing
   launches.
2. Upgrade the development host to Bubblewrap 0.12.0 or newer and make the
   doctor gate authoritative.
3. Add the Bubblewrap process builder and fake-provider containment suite at the
   PTY child-spawn seam.
4. Add the host-owned workflow broker and prove ordinary messages, requests,
   compact progress, blockers, and results without exposing a database file.
5. Enable the Antigravity adapter behind explicit `--isolate workspace` preview
   selection and run Gemini then Claude live gates through normal interactive
   hcom launches as well as the optional high-level wrapper.
6. Enable Claude-backed GLM in a fresh worktree and run communication,
   cleanup, and resume gates.
7. Make `workspace` the delegated-workflow default only after the full trusted
   local workflow matrix passes.
8. Add constrained egress and hardened credential brokering before enabling
   untrusted workloads; design explicit shared-workspace coordination as a
   separate follow-up.

Rollback is configuration-only until step 7 because isolation is opt-in. After
it becomes the delegated default, users may explicitly select `off`; hcom must
never select `off` automatically after an isolation failure.

## Open Questions

- When explicit workspace sharing is added, what coordination model should
  govern concurrent edits, branch ownership, locking, and conflict handling?
  The current design only guarantees that workflow identity, communication,
  result correlation, and cleanup remain independent from workspace ownership.
