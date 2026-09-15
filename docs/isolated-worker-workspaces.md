# Isolated worker workspaces

Status: **blocking design for daily delegated use**

This document turns issue 44 in the
[orchestration issue register](orchestration-token-efficiency-issues.md#44-provider-native-sandboxes-do-not-provide-one-reliable-worker-boundary)
into an implementable provider-neutral isolation capability.

The immediate goal is not a general container platform. It is a reliable way to
let an hcom-launched coding agent read, edit, build, and test one declared
workspace without repeatedly asking permission or receiving ambient host
access, while preserving ordinary hcom messaging and progress observation.

## Why this is now a blocker

Live delegated runs exposed the complete failure chain:

1. A fresh Claude-backed GLM worktree stopped at provider workspace trust.
   Per-run Claude state can handle that gate without making the trust durable.
2. Antigravity stopped on an ordinary compound read command even though each
   component command was allowed separately.
3. Antigravity's `proceed-in-sandbox` mode correctly stopped asking for commands
   that remained inside its sandbox.
4. The native Antigravity sandbox then failed to execute a local Git command.
   The provider requested permission to retry outside the sandbox, returning the
   unattended workflow to an approval prompt.

This cannot be solved with a longer command allowlist. Real coding agents
compose commands, invoke project tools, create temporary executables, and
discover new test entry points. Exact-command policies either interrupt normal
work or grow into an unsafe approximation of `allow everything`.

The required division of responsibility is:

```text
model/provider decides which development operation to perform
                         |
                         v
             hcom-owned isolation boundary
                         |
                         v
       declared workspace, build tools, scoped hcom channel
```

Provider permission systems may still provide diagnostics and approval UX, but
they are not the containment boundary.

## Security contract

### Protected from the worker

The default workspace profile must prevent the provider process and everything
it launches from:

- writing outside the declared workspace and private per-run scratch paths;
- modifying Git metadata, hcom's global state, provider configuration, shell
  startup files, or unrelated workspaces;
- reading common credential and secret locations unless an explicit provider
  adapter supplies the minimum material required to start;
- observing or signalling unrelated host processes;
- acquiring privileges or creating nested isolation that weakens the boundary;
- silently falling back to host execution when isolation setup fails.

### Available to the worker

The default profile must still support ordinary repository work:

- read and write the declared workspace;
- read Git metadata for status, diff, history, and blame;
- run the repository's existing compiler, formatter, linter, and tests;
- use a private temporary directory and bounded writable build caches;
- communicate through an hcom channel scoped to the current workflow;
- exchange normal requests and replies with authorized non-isolated agents and
  emit bounded progress through the existing event stream;
- receive the provider authentication needed for inference without inheriting
  unrelated credentials;
- use a PTY normally so readiness, prompts, transcripts, injection, and cleanup
  continue to work.

Git metadata is read-only by default. Commit, branch mutation, checkout, merge,
rebase, and push are coordinator operations unless a stronger profile is chosen
explicitly. This matches the current delegated workflow: workers edit and
verify while the parent reviews and lands the result.

### Threat model and boundary

The first milestone protects against accidental destructive commands and
repository-supplied instructions attempting to reach host files or processes.
It must not be described as complete hostile-code containment until credential
delivery and network egress are also brokered.

Bubblewrap constructs a sandbox; its arguments define the actual security
policy. A successful `bwrap` process is insufficient evidence. hcom must
validate its exact mount, namespace, environment, and lifecycle plan and record
the effective plan for inspection.

## User-facing contract

Isolation is an hcom launch property, not a provider flag. Proposed CLI:

```text
hcom agy --isolate workspace ...
hcom claude --isolate workspace ...
hcom isolation doctor
hcom isolation explain --profile workspace --dir /absolute/workspace
```

`--isolate` is consumed by hcom and never forwarded to the provider. The same
resolved policy is stored with the launch so resume, recovery, and diagnostics
cannot silently change it.

Initial profiles:

| Profile | Behavior |
| --- | --- |
| `off` | Existing host execution; explicit opt-out only |
| `workspace` | Workspace writable, Git metadata read-only, private temp/cache, scoped hcom broker |
| `workspace-git` | As above, but dedicated disposable Git metadata is writable for an explicitly authorized workflow |

The first release should not expose a misleading `strict` profile or claim
network isolation before egress and credential paths are enforced. Unknown
profiles, unsupported operating systems, missing Bubblewrap, invalid paths, and
policy construction failures are fatal before provider launch.

Configuration may provide a default without hiding the launch decision:

```toml
[isolation]
default = "workspace"
backend = "bubblewrap"
```

The launch result and `hcom list -v` report the requested profile, effective
backend, workspace, runtime directory, Git policy, and whether host networking
or brokered networking is active.

## Runtime architecture

The current PTY-backed path is:

```text
commands::launch
  -> launcher::launch
  -> create_runner_script
  -> terminal::launch_terminal
  -> hcom pty
  -> pty::Proxy::spawn(provider)
```

The isolation boundary belongs immediately before the final provider spawn:

```text
host
  hcom launcher
  terminal / detached runner
  hcom PTY proxy and delivery loop
           |
           | PTY master/slave
           v
sandbox
  provider CLI
  provider child commands
  workflow-scoped hcom client and hooks
           |
           v
host-owned scoped broker -> ordinary hcom messages/events/results
```

Keeping the PTY proxy outside preserves supervision, terminal injection, status
detection, process ownership, and fail-closed cleanup even when the provider is
unhealthy. The provider executable and its descendants receive the same mount
namespace, so asking the model to bypass its own policy cannot escape hcom's
outer boundary.

Implementation components:

1. `IsolationProfile` represents user intent.
2. `IsolationPlan` contains canonical, validated paths, explicit workload trust,
   stable workflow-policy identity, attempt identity, and safe environment
   metadata. It is serializable for launch records and diagnostics.
3. A Linux Bubblewrap backend converts the plan to an argument vector without a
   shell.
4. The PTY command accepts the resolved plan through a private sidecar or
   inherited file descriptor, then spawns `bwrap ... -- provider`.
5. A host-owned per-run broker exposes only authorized hcom operations without
   mounting a database into the sandbox.
6. Cleanup owns the provider process group and per-run runtime directory, never
   the declared workspace.

Do not build a shell command string containing mount paths. Existing launch
arguments may contain hostile characters and workspaces may contain spaces.

### Architectural guardrails

Isolation must become a runtime capability, not another provider identity. Do
not add variants such as `SandboxedAntigravity` or `IsolatedClaudePty` to the
existing `LaunchTool` enum. The concepts remain independently composable:

```text
Tool + LaunchBackend + IsolationProfile + WorkflowRole
```

The current `Tool`/`LaunchTool` split can be normalized later toward a
`LaunchTarget { tool, mode }` representation, but that refactor is not a
prerequisite for isolation. Combining it with the first containment change
would enlarge the regression surface across every provider and launch mode.

Likewise, do not turn `IntegrationSpec` into a large behavior trait. It remains
the declarative provider description. Provider-specific isolation behavior is a
small optional capability, conceptually:

```text
IsolationAdapter
  -> classify public configuration
  -> classify per-run writable state
  -> prepare authentication
  -> contribute validated mounts and environment
```

Other provider behavior may eventually move into similarly focused launch,
hook, delivery, transcript, and session adapters, but issue 44 introduces only
the isolation capability it needs.

The implementation belongs in a dedicated runtime module such as
`src/isolation/`. `commands/launch.rs` parses user intent, `launcher.rs`
coordinates the launch, and the isolation module owns policy resolution,
preflight, backend command construction, runtime state, and cleanup. This avoids
adding another full subsystem directly to the already broad launcher module.

Finally, per-workflow state is a real tenancy boundary, not merely a temporary
directory naming convention. The isolation records use explicit `WorkflowId`
and `AttemptId` values from the beginning. Phase 1 does not need to redesign all
existing instance/message tables, but it must not create anonymous runtime
directories that cannot later become first-class workflow records.

Workflow identity is independent from workspace ownership. Initial launches may
require an exclusive workspace grant, but the plan and lifecycle must remain
compatible with a later explicit mode where multiple sessions share one
workspace while retaining separate messages, results, and cleanup. Concurrent
edit, locking, branch, and conflict policy for that mode remains undecided.

This remains one Rust binary with local SQLite state. The design does not add a
required daemon, service mesh, or separate orchestration deployment.

### Backend coverage

Milestone 1 covers PTY-backed normal interactive `hcom agy` and `hcom claude`
launches first. High-level `hcom run` workflows consume the same isolation and
communication path as an optional completion-oriented interface. Foreground and
detached PTY paths must both reach the same final provider-spawn boundary.

Claude native print mode currently bypasses `hcom pty` and must fail clearly if
isolation is requested until it is routed through the same process builder.
Windows and macOS likewise report `unsupported` rather than falling back to host
execution. Future backends may implement the same `IsolationPlan` contract with
different operating-system primitives.

## Filesystem plan

The workspace profile begins with an empty mount namespace and adds only:

- platform runtime roots read-only;
- the canonical workspace read-write;
- the canonical Git directory read-only, including linked-worktree Git dirs;
- a new `/proc`, minimal `/dev`, private `/tmp`, and private runtime directory;
- private home and XDG config/cache/state/runtime directories;
- the minimum public identity, DNS, and TLS data needed by declared tools;
- selected build caches, each declared read-only or read-write by policy;
- a workflow-scoped broker endpoint, but no hcom database file;
- provider-specific runtime material selected by an adapter.

Linux layouts are discovered rather than assumed. On NixOS, a `/usr`-only plan
cannot execute basic tools; `/run/current-system/sw` resolves into `/nix/store`,
so both require read-only visibility. Other distributions need their own
runtime-root resolver and tests.

The backend should use the equivalent of:

```text
--unshare-user --unshare-pid --unshare-ipc --unshare-uts --unshare-cgroup
--disable-userns --die-with-parent --cap-drop ALL
```

The hcom PTY proxy already creates a session and controlling terminal. Do not
add Bubblewrap's `--new-session` unless the fake-provider PTY tests prove that a
second session preserves `/dev/tty`, sizing, input, signals, readiness, and
injection. Mount targets are created only beneath hcom-owned runtime roots
before untrusted workspace content is introduced; symlink and canonical-path
validation is a hard precondition.

### Bubblewrap version gate

The current machine has Bubblewrap 0.11.2 and working unprivileged user
namespaces, but it must not be accepted for the security profile. Bubblewrap
0.12.0 fixes a sandbox-setup symlink traversal affecting earlier releases.
`hcom isolation doctor` must require at least 0.12.0 for this design, verify the
resolved executable is not unexpectedly setuid or capability-bearing, and run a
minimal namespace probe using the detected platform runtime roots.

## Scoped hcom communication

Mounting `~/.hcom` read-write would let every worker inspect or corrupt global
messages, configuration, scripts, transcripts, and launch state. It is not an
acceptable shortcut.

Mounting a separate private SQLite database is also insufficient: a worker with
shell access could inspect, rewrite, or forge its contents, and normal hcom
messages and event streams would live in a different database.

Milestone 1 therefore creates a host-owned broker endpoint per workflow. The
endpoint authenticates the workflow, attempt, worker generation, thread, and
message intent and accepts only a small typed protocol for lifecycle records,
ordinary messages and requests, blockers, bounded progress, and results. It
does not proxy arbitrary SQL, filesystem access, task scheduling, or model
reasoning.

The broker writes authorized records into host-owned hcom state, so normal
`hcom send`, replies, and `hcom events stream --compact` continue to work with
isolated workers. A high-level wrapper can wait for the same correlated result,
but is not required for live interaction. The agent instance remains a
participant in a workflow rather than the owner of its security scope.

## Credentials and network

Whole-process isolation creates a credential problem: the provider needs to
authenticate, but a file mounted for the provider may also be readable by its
child shell. Each provider adapter classifies its state into:

- public/read-only configuration;
- writable per-run session state;
- authentication material;
- optional external-service credentials.

Authentication uses a broker or narrowly scoped disposable credential when
available. Proxy endpoints, CA material, and non-secret placeholder environment
values are declared explicitly; secret values never enter the serialized plan
or Bubblewrap argv. Copying a user's complete provider directory into the
sandbox is not permitted. A provider that cannot start without exposing a
durable credential remains unsupported by the hardened profile until that path
is tested.

The provider also needs network access for inference. Sharing the host network
preserves functionality but does not enforce egress isolation. The
implementation therefore reports networking honestly:

- `host`: functional but not hardened against data exfiltration;
- `brokered`: only configured provider and dependency endpoints are reachable;
- `none`: usable only for an already-running provider architecture or offline
  commands.

`workspace` must not be called hardened while using `host` networking. The plan
records workload trust explicitly; selecting a local directory does not confer
trust. An explicit launch choice or stored policy keyed to canonical workspace
and repository identity may grant trusted-local status without a repeated
prompt. Brokered egress is a release gate for untrusted repositories or issue
text. For an explicitly trusted-local workflow, filesystem/process isolation
with visible `host` network status may be released as a preview after the user
accepts that narrower boundary.

## Lifecycle and failure semantics

Isolation setup occurs before the provider process exists. Failures produce a
typed `isolation` launch blocker containing the failed stage and no secret
values.

There is no fallback from `workspace` to `off`. Resume preserves the stable
workflow-policy identity and policy inputs while creating a new attempt ID,
runtime path, and attempt-plan digest; policy drift fails before startup. Kill
and timeout cleanup terminate the namespace/process group before deleting
runtime state. Cleanup warnings after an authoritative task result follow issue
45's result-preserving semantics.

Suggested stages:

```text
preflight -> plan_resolved -> namespace_started -> provider_started
          -> ready -> running -> result -> namespace_stopped -> cleaned
```

## Verification matrix

Unit tests:

- hcom flags are consumed and never forwarded to providers;
- profile/config precedence is deterministic;
- canonical workspace and linked-worktree Git paths are correct;
- unknown profiles and unsupported platforms fail closed;
- NixOS and conventional Linux runtime roots produce valid plans;
- launch records never contain credential values;
- workload trust is explicit and a local path does not imply trust;
- stable policy identity survives resume while attempt identity rotates.

Hermetic integration tests:

- workspace create/edit/delete succeeds;
- writes to the parent directory, home, global hcom directory, and another
  workspace fail;
- Git status/diff/log succeed while Git metadata mutation fails;
- host PIDs and sensitive paths are not visible;
- private home/XDG, temp, caches, DNS, TLS, and declared proxy facilities work
  without host credential visibility;
- sandboxed ordinary messages, requests, compact progress, blockers, and
  completion reach authorized host-side hcom consumers;
- the worker cannot open or forge an hcom database;
- a nested shell cannot escape the mount policy;
- `/dev/tty`, sizing, input, signals, readiness, and injection remain usable;
- missing/old Bubblewrap and invalid mount plans fail before launch;
- SIGINT, timeout, provider crash, and successful result leave no process or
  writable runtime state behind.

Live gates, in order:

1. `hcom isolation doctor` on NixOS.
2. A fake provider under the real PTY proxy.
3. Read/edit/test task with Antigravity Gemini.
4. The same task with Antigravity Claude.
5. Claude-backed GLM in a fresh Git worktree.
6. Worker/reviewer workflow using the host-owned scoped broker.

No live provider test is attempted until the fake-provider containment suite
passes.

## Delivery sequence

### Phase 0: preflight and plan

- Add the config model, CLI parsing, `doctor`, `explain`, launch-record fields,
  version gate, canonicalization, and platform runtime-root detection.
- Add the dedicated isolation module, opaque workflow/attempt identifiers, and
  the small provider isolation-adapter interface without changing provider
  launch semantics.
- No provider process is sandboxed in this phase.

### Phase 1: PTY workspace isolation

- Add Bubblewrap process construction at the PTY child-spawn seam.
- Support fake providers and PTY-backed real providers with filesystem/process
  isolation.
- Create the per-run host-owned hcom broker and correlated live
  messaging/progress/result return.
- Release only as preview while networking is reported as `host`.

### Phase 2: provider adapters

- Split provider configuration, session state, and authentication mounts.
- Prove Antigravity and Claude-backed GLM startup, resume, and cleanup.
- Remove provider workspace-trust and command-approval loops from the normal
  isolated workflow without changing durable provider trust outside it. A
  conflicting provider-native sandbox may be disabled, and a broad provider
  permission mode may be selected only while the validated outer boundary and
  scoped credentials are active.

### Phase 3: hardened egress and cross-workflow brokering

- Add credential-aware and network-aware egress controls.
- Harden the scoped hcom broker for untrusted cross-workflow use.
- Permit untrusted repository and GitHub-event workloads only after these gates
  pass.

### Phase 4: remaining backends and platforms

- Route native print through the isolation process builder.
- Add macOS and Windows backends against the same profile contract.

## Milestone 1 decisions

The first implementation is intentionally narrow:

1. It supports explicitly trusted-local workloads and reports their trust class
   plus `network=host` on every launch. Untrusted repositories and GitHub-event
   content remain blocked until brokered egress and credentials are implemented.
2. Nix runtime and store paths and dependency source caches are read-only.
   Build output stays in the workspace or private per-run storage. Cargo
   credentials and other package-manager secrets are not mounted.
3. Normal interactive hcom messaging and compact progress use the scoped broker
   first. High-level `hcom run` is an optional consumer of the same result path.
4. Antigravity is the first real provider adapter after the fake-provider
   containment gate because it is the current daily-use blocker. Claude-backed
   GLM follows against the same boundary.

None of these decisions permits fallback to unrestricted host execution.

The architecture intentionally does not equate a workflow with exclusive
workspace ownership. A later opt-in sharing feature may authorize multiple
sessions against one workspace while retaining separate broker identities and
cleanup. Its edit coordination, locking, branch, and conflict rules remain to
be designed.

## Non-goals

- rewriting the launcher, PTY subsystem, or provider integrations before the
  containment path works;
- mechanically splitting large files without moving ownership to a coherent
  runtime or application boundary;
- replacing `Tool`/`LaunchTool` as part of the first isolation patch;
- creating one giant provider trait whose implementers must support irrelevant
  capabilities;
- splitting hcom into services or introducing a required background daemon;
- building a general-purpose container orchestrator.
