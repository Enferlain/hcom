## 1. Establish isolation identity and launch configuration

- [x] 1.1 Add `src/isolation/` with validated `IsolationProfile`, `WorkflowId`, `AttemptId`, and serializable non-secret `IsolationPlan` types; verify parsing, validation, serialization, and secret-exclusion unit tests.
- [ ] 1.2 Extend isolation identity with explicit workload trust plus separate stable workflow-policy and attempt-plan identities; verify a resumed attempt changes its attempt identity and runtime path while policy-equivalent inputs retain the same policy identity.
- [ ] 1.3 Add configuration and `--isolate off|workspace|workspace-git` CLI parsing that hcom consumes without forwarding to providers; require explicit trusted-local selection for host-network preview and verify precedence, unknown-profile rejection, trust handling, and provider-argv tests.
- [ ] 1.4 Persist requested/effective isolation metadata with launches, expose it through status output, and enforce recorded policy identity during resume while allocating a fresh attempt; verify round-trip, policy-drift, and attempt-rotation tests.

## 2. Add authoritative preflight and plan inspection

- [ ] 2.1 Implement conventional Linux and NixOS runtime-root resolvers with canonical paths, minimum public identity/DNS/TLS data, private home/XDG paths, and an explicit unsupported-platform result; verify representative filesystem fixtures without admitting host credential stores.
- [ ] 2.2 Implement Bubblewrap discovery and `hcom isolation doctor`, requiring version 0.12.0 or newer and checking setuid/file capabilities, usable unprivileged user namespaces, mandatory nested-user-namespace disabling, runtime roots, and a minimal namespace probe; verify every failure stage and a successful probe deterministically.
- [ ] 2.3 Implement `hcom isolation explain` in human-readable and JSON forms, including backend, profile, workspace, Git policy, workload trust, runtime roots, network mode, policy identity, and attempt-plan identity; verify stable output, no provider launch, and redaction of secret values.

## 3. Build the fail-closed Bubblewrap runtime

- [ ] 3.1 Resolve and validate canonical workspace paths plus linked-worktree Git directories, rejecting unsafe symlink or out-of-scope metadata paths; require dedicated disposable Git state for `workspace-git` and verify normal repositories, linked worktrees, private Git state, and traversal cases.
- [ ] 3.2 Build Bubblewrap argument vectors from an empty root with required namespaces, nested-user-namespace disabling, read-only runtime roots, private home/XDG/tmp paths, workspace/Git policy, constrained caches, clear environment construction, and no secret argv values or shell interpolation; verify exact argv snapshots for both profiles and Linux layouts.
- [ ] 3.3 Add an `IsolationRuntime` that owns private runtime directories and the provider namespace/process tree, terminating descendants before cleanup on success, cancellation, timeout, crash, and failed startup; verify lifecycle and cleanup integration tests.
- [ ] 3.4 Pass the resolved plan privately to `hcom pty` and wrap only the final provider child immediately before `pty::Proxy::spawn`; preserve the existing controlling PTY rather than adding a second session unless proven compatible, reject missing/changed/malformed plans, and verify no unrestricted fallback starts.

## 4. Prove containment with a fake provider

- [ ] 4.1 Add a fake provider fixture that runs through the real terminal, runner, `hcom pty`, and PTY proxy path and can execute controlled filesystem, process, environment, and PTY probes.
- [ ] 4.2 Add hermetic containment tests proving workspace edits succeed while writes outside the workspace, protected secret reads, undeclared caches, and Git metadata mutation fail; verify Git inspection with optional locks disabled and explicit `workspace-git` against dedicated disposable Git state.
- [ ] 4.3 Add process and PTY containment tests proving unrelated host PIDs are hidden, nested user namespaces are denied, descendants remain owned and signalable by hcom, and `/dev/tty`, terminal size, input, signals, readiness, and injection remain functional.
- [ ] 4.4 Add lifecycle tests for cancellation, timeout, provider crash, setup failure, and cleanup ordering; document that namespace isolation does not impose CPU, memory, disk, or network quotas, and keep real-provider enablement blocked unless the complete fake-provider suite passes.

## 5. Add host-owned scoped hcom communication

- [ ] 5.1 Add a per-run host-owned broker endpoint bound to workflow, attempt, worker generation, thread, and intent; keep all SQLite files outside the sandbox and verify direct database access and cross-workflow operations are unavailable.
- [ ] 5.2 Relay authorized ordinary messages, requests, blockers, lifecycle records, results, and bounded compact progress into host-owned hcom state; verify normal `hcom send`/reply and `hcom events stream --compact` work with an isolated live worker without exposing unrelated state.
- [ ] 5.3 Make the optional high-level `hcom run` result wait consume the same correlated broker channel; verify the exact attempt is returned and an authoritative result survives cleanup warnings.
- [ ] 5.4 Add isolated worker-to-reviewer coverage with scoped communication and verify messaging, result correlation, cancellation, and absence of cross-workflow messages; also verify two workflow identities can reference one workspace grant without sharing state or allowing either cleanup to delete the workspace.

## 6. Enable providers in containment-gated order

- [ ] 6.1 Add the small optional `IsolationAdapter` capability for declared public configuration, writable per-run state, authentication, safe environment values, validated mounts, broker handles, and workflow-granted external operations; verify full provider directories, secret plan/argv values, unsupported credentials, and ungranted operations fail preflight.
- [ ] 6.2 Add the Antigravity adapter with isolated settings/auth/session state and compatible launch flags; permit disabling its conflicting native sandbox or selecting broad non-interactive permissions only when the validated outer boundary and scoped credentials are active, and verify adapter plan, capability enforcement, prompt recognition, and fresh-state startup fixtures.
- [ ] 6.3 Run and document the Antigravity Gemini trusted-local live gate through normal interactive `hcom agy`, covering edit/format/build/test, ordinary messaging, compact progress, authorized GitHub participation, completion, cancellation, and cleanup without routine prompts; also test the optional high-level wrapper and keep preview-only if any gate fails.
- [ ] 6.4 Run and document the Antigravity Claude trusted-local live gate through normal interactive hcom for the same permission, communication, and lifecycle matrix, including a fresh worktree with no durable provider workspace-trust state; keep preview-only if any gate fails.
- [ ] 6.5 Add the GLM adapter and run its trusted-local live gate through normal interactive `hcom claude` in a fresh worktree, including non-interactive routine work, messaging, compact progress, scoped GitHub work, blocked escape, result, resume-policy invariance, cancellation, and cleanup; keep preview-only if any gate fails.

## 7. Harden observability and roll out deliberately

- [ ] 7.1 Expose isolation profile, backend, workload trust, workflow/policy/attempt identities, lifecycle stage, Git policy, and `network=host` limitations in status, help, structured output, and user documentation; verify snapshots and ensure no credentials appear.
- [ ] 7.2 Run `cargo fmt --check`, `cargo check --workspace --all-targets`, `cargo clippy --workspace --all-targets -- -D warnings`, the full locked test suite, and all containment gates on supported Linux and NixOS environments.
- [ ] 7.3 Make `workspace` the default only for supported PTY-backed delegated workflows after fake-provider and enabled real-provider matrices pass; verify normal interactive launches and optional wrappers, explicit `off`, and no fallback after isolation failure.
- [ ] 7.4 Record hardened credential/egress brokering, resource limits, explicit shared-workspace coordination, additional operating systems, native-print launches, and untrusted GitHub-event workloads as separate dependent Beads work before claiming those capabilities.
