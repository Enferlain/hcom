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
  isolated hcom completion reporting.
- Let routine work inside an active outer boundary proceed without repetitive
  provider approvals while retaining explicit capability gates for dangerous or
  out-of-scope actions.
- Establish workflow and attempt identity as the tenancy key for runtime state.

**Non-Goals:**

- Refactoring the full launcher or `Tool`/`LaunchTool` model first.
- Building a general container runtime or adding a required daemon.
- Claiming hostile-code containment while networking remains shared.
- Supporting native-print, macOS, Windows, arbitrary global hcom messaging, or
  untrusted GitHub-event workloads in the first milestone.

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
- `IsolationPlan`: canonical workspace, Git paths, mounts, environment keys,
  backend, network mode, runtime path, and plan identity.
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
paths, declared caches, provider state, and workflow-scoped hcom state. It uses
separate user, PID, IPC, UTS, and cgroup namespaces, a new session, parent-death
handling, and dropped capabilities. Nested user namespaces are disabled when
the host supports that control.

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
the `workspace` profile. `workspace-git` makes only those exact Git paths
writable.

Writable build output remains inside the workspace or a private runtime path.
Nix stores and dependency source caches are read-only. Credential files such as
Cargo's credentials are excluded.

**Alternative considered:** Bind the host root read-only and overlay writable
paths. Rejected because it exposes unrelated files and secrets and makes the
allowlist harder to audit.

### hcom state is workflow-scoped

Create a private hcom directory for each workflow/attempt. The host coordinator
and sandboxed participants use that database for lifecycle and result events.
High-level `hcom run` prints the correlated result to its caller, so the parent
does not need membership in the private database.

The runtime path is keyed by `WorkflowId` and `AttemptId`, not only an instance
name. This introduces the minimum first-class workflow identity needed for
tenancy without redesigning existing message and instance tables.

**Alternative considered:** Mount global `~/.hcom` read-write. Rejected because
the worker could inspect or corrupt unrelated messages, scripts, transcripts,
configuration, and launches.

**Alternative considered:** Build the full global communication broker first.
Rejected for milestone 1 because high-level result return is sufficient for the
daily worker workflow. The narrow broker follows when arbitrary cross-workflow
messaging is required.

### Provider-specific behavior is a small capability

Keep `IntegrationSpec` declarative. Add a small optional `IsolationAdapter` that
classifies public configuration, writable per-run state, authentication, and
additional validated mounts/environment. Do not create one large provider trait
covering launch, hooks, delivery, transcript, session, and isolation.

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

Remote operations use explicit workflow capabilities and scoped identities.
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

Milestone 1 shares host networking only for explicitly trusted local
repositories and reports `network=host` on every launch. This provides
filesystem/process isolation for immediate use but is not described as hardened
against exfiltration. Untrusted issue text, pull requests, and repositories are
rejected until brokered credentials and egress exist.

**Alternative considered:** Disable networking. Rejected for real providers
because inference requires network access.

**Alternative considered:** Claim host networking is safe because filesystem
mounts are restricted. Rejected because exposed provider credentials or
repository data could still leave through arbitrary egress.

### Isolation lifecycle is durable and observable

Store requested profile, effective backend, canonical workspace, workflow ID,
attempt ID, Git policy, network mode, runtime path, and a non-secret plan digest
with the launch. `hcom list -v` and structured launch results expose them.

Lifecycle stages are:

```text
preflight -> plan_resolved -> namespace_started -> provider_started
          -> ready -> running -> result -> namespace_stopped -> cleaned
```

Setup failures emit a typed `isolation` blocker with the failed stage. Resume
must reproduce the recorded boundary. Cancellation and timeout terminate the
provider namespace before deleting writable runtime state. A cleanup warning
after an authoritative result does not erase that result.

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
  inspection commands and reserve explicit `workspace-git` for workflows that
  genuinely own branch mutation.
- **[NixOS closures require many runtime paths]** → Mount the Nix store and
  system profile read-only and test executable resolution separately from
  conventional Linux.
- **[Private hcom state reduces ad-hoc communication]** → Prioritize reliable
  high-level workflow results; add an authenticated narrow broker later.
- **[A new workflow identity starts a larger domain migration]** → Introduce
  opaque IDs and launch metadata only; defer broad schema normalization.
- **[Provider-native and hcom sandboxes conflict]** → Provider adapters select
  compatible flags, and the fake-provider gate proves the outer boundary before
  real-provider tuning.
- **[Broad provider permissions are enabled without containment]** → Make an
  active validated isolation plan a hard prerequisite and fail before provider
  startup rather than weakening either layer.

## Migration Plan

1. Add plan/config types, workflow/attempt IDs, CLI parsing, `doctor`, and
   `explain` without changing launches.
2. Upgrade the development host to Bubblewrap 0.12.0 or newer and make the
   doctor gate authoritative.
3. Add the Bubblewrap process builder and fake-provider containment suite at the
   PTY child-spawn seam.
4. Add workflow-scoped hcom state and correlated high-level result return.
5. Enable the Antigravity adapter behind explicit `--isolate workspace` preview
   selection and run Gemini then Claude live gates.
6. Enable Claude-backed GLM in a fresh worktree and run cleanup/resume gates.
7. Make `workspace` the delegated-workflow default only after the full trusted
   local workflow matrix passes.
8. Add brokered credentials, egress, and global hcom messaging before enabling
   untrusted workloads.

Rollback is configuration-only until step 7 because isolation is opt-in. After
it becomes the delegated default, users may explicitly select `off`; hcom must
never select `off` automatically after an isolation failure.
