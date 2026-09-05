## 1. Establish isolation identity and launch configuration

- [ ] 1.1 Add `src/isolation/` with validated `IsolationProfile`, `WorkflowId`, `AttemptId`, and serializable non-secret `IsolationPlan` types; verify parsing, validation, serialization, and secret-exclusion unit tests.
- [ ] 1.2 Add configuration and `--isolate off|workspace|workspace-git` CLI parsing that hcom consumes without forwarding to providers; verify CLI precedence, unknown-profile rejection, and provider-argv tests.
- [ ] 1.3 Persist requested/effective isolation metadata with launches, expose it through status output, and enforce the recorded profile/workspace identity during resume; verify round-trip and policy-drift tests.

## 2. Add authoritative preflight and plan inspection

- [ ] 2.1 Implement conventional Linux and NixOS runtime-root resolvers with canonical paths and an explicit unsupported-platform result; verify resolver tests against representative filesystem fixtures.
- [ ] 2.2 Implement Bubblewrap discovery and `hcom isolation doctor`, requiring version 0.12.0 or newer and checking setuid/file capabilities, user namespaces, runtime roots, and a minimal namespace probe; verify each failure stage and successful probe with deterministic tests.
- [ ] 2.3 Implement `hcom isolation explain` in human-readable and JSON forms, including backend, profile, workspace, Git policy, runtime roots, network mode, and plan identity; verify stable output, no provider launch, and redaction of secret values.

## 3. Build the fail-closed Bubblewrap runtime

- [ ] 3.1 Resolve and validate canonical workspace paths plus linked-worktree Git directories, rejecting unsafe symlink or out-of-scope metadata paths; verify normal repositories, linked worktrees, and traversal cases.
- [ ] 3.2 Build Bubblewrap argument vectors from an empty root with the required namespaces, read-only runtime roots, workspace/Git policy, constrained caches, and private scratch paths, without shell interpolation; verify exact argv snapshots for both isolation profiles and both Linux layouts.
- [ ] 3.3 Add an `IsolationRuntime` that owns private runtime directories and the provider process tree, terminating descendants before cleanup on success, cancellation, timeout, crash, and failed startup; verify lifecycle and cleanup integration tests.
- [ ] 3.4 Pass the resolved plan privately to `hcom pty` and wrap only the final provider child immediately before `pty::Proxy::spawn`; reject missing, changed, or malformed plans and verify that no unrestricted fallback process starts.

## 4. Prove containment with a fake provider

- [ ] 4.1 Add a fake provider fixture that runs through the real terminal, runner, `hcom pty`, and PTY proxy path and can execute controlled filesystem/process probes.
- [ ] 4.2 Add hermetic containment tests proving workspace edits succeed while writes outside the workspace, protected secret reads, undeclared caches, and disallowed Git metadata mutation fail; verify read-only Git inspection and explicit `workspace-git` behavior.
- [ ] 4.3 Add process-containment tests proving unrelated host PIDs are hidden, nested namespace creation is denied where supported, and worker descendants remain owned and signalable by hcom.
- [ ] 4.4 Add lifecycle tests for cancellation, timeout, provider crash, setup failure, authoritative-result preservation, and cleanup ordering; keep real-provider enablement blocked unless the complete fake-provider suite passes.

## 5. Scope hcom communication to each workflow

- [ ] 5.1 Create a private per-workflow/per-attempt `HCOM_DIR` and database accessible only to the host coordinator and declared participants; verify unrelated workflows and global hcom state are unavailable inside the worker.
- [ ] 5.2 Bridge the correlated completion or failure event back to the high-level `hcom run` caller without exposing the private database; verify one blocking result wait returns the correct attempt and preserves an authoritative result across cleanup warnings.
- [ ] 5.3 Add an isolated worker-to-reviewer integration workflow using scoped state; verify bounded participant messaging, result correlation, cancellation, and absence of cross-workflow messages.

## 6. Enable providers in containment-gated order

- [ ] 6.1 Add the small optional `IsolationAdapter` capability for declared public configuration, writable per-run state, authentication material, mounts, and environment; verify that undeclared full provider directories and unsupported credentials fail preflight.
- [ ] 6.2 Add the Antigravity adapter with isolated settings/auth/session state and compatible launch flags, without native sandbox-bypass flags; verify adapter plan fixtures and fresh-state startup behavior.
- [ ] 6.3 Run and document the Antigravity Gemini trusted-local live gate for editing, build/test, completion reporting, cancellation, and cleanup; keep the adapter preview-only if any gate fails.
- [ ] 6.4 Run and document the Antigravity Claude trusted-local live gate for the same matrix, including a previously untrusted fresh worktree; keep the adapter preview-only if any gate fails.
- [ ] 6.5 Add the GLM adapter and run its trusted-local live gate in a fresh worktree, including edit/test/result, resume-invariant, cancellation, and cleanup checks; keep it preview-only if any gate fails.

## 7. Harden observability and roll out deliberately

- [ ] 7.1 Expose isolation profile, backend, workflow/attempt IDs, lifecycle stage, Git policy, and `network=host` limitations in status, help, structured output, and user documentation; verify snapshots and ensure no credentials appear.
- [ ] 7.2 Run `cargo fmt --check`, `cargo check --workspace --all-targets`, `cargo clippy --workspace --all-targets -- -D warnings`, the full locked test suite, and all containment gates on supported Linux and NixOS environments.
- [ ] 7.3 Make `workspace` the default only for high-level delegated workflows after the fake-provider and all enabled real-provider matrices pass; verify explicit `off` remains available and no failed isolated launch silently falls back to it.
- [ ] 7.4 Record brokered credentials, constrained egress, authenticated global hcom messaging, additional operating systems, native-print launches, and untrusted GitHub-event workloads as separate dependent Beads work before claiming hostile-workload support.
