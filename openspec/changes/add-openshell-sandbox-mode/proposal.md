# Proposal

## Why

Routine command approvals and terminal supervision make delegated work harder to leave unattended. An optional OpenShell mode should let a worker act freely inside an explicit execution boundary while remaining a normal hcom teammate: reachable by name, able to exchange questions and results, and able to hand off Git artifacts without modifying the launcher's checkout.

## What Changes

- Add an opt-in, named OpenShell launch profile selected with `--hcom-sandbox <profile>`. Reserve hcom-prefixed flags because several supported tools already own `--sandbox`.
- Run one worker per sandbox initially, with a private hcom database, a selected committed repository snapshot, a qualified tool image, explicit provider access, and a guest-only unattended permission mode. Preserve ordinary local launches and their permission settings.
- Add a host-owned bridge for team discovery, messages, replies, bounded status events, delivery receipts, and explicit final results. Support host-to-sandbox and sandbox-to-sandbox communication without an agent manually forwarding messages.
- Keep routing identity and team membership under host control. Do not enroll guests in the host's fully trusted MQTT relay or expose host process-control, configuration, filesystem, or arbitrary RPC surfaces to guests.
- Make transport reconnectable with durable queues, stable IDs, transactional deduplication, explicit failure reporting, bounded connection rotation, and aggregate guest traffic budgets. Run the host controller independently of the launching terminal.
- Add sandbox-aware lifecycle and observation: distinguish agent readiness, sandbox readiness, and bridge connectivity; retain failed runs and pending results; operate on captured sandbox IDs rather than guest PIDs or reusable names.
- Share versioned artifacts through Git references or validated Git bundles. Carry summaries, outcome, provenance, and validation claims over hcom. Never apply returned changes to the host checkout automatically.
- Prefer dedicated modules and small upstream integration points. Keep integrations with this fork's compact progress and correlated result waits separately identifiable from the portable sandbox core.

The initial qualification target is headless launch on a Linux OpenShell gateway with the VM driver and one CLI worker per sandbox. A multi-worker batch provisions one sandbox for each worker. Claude Code's existing PTY launch path is the reference worker; its provider, model, and credentials are operator-configured. Other runtimes and tools require their own qualification. Visible launch, desktop-backed Antigravity launch, nested sandbox creation, cross-host sandbox control, uncommitted-worktree capture, automatic host merges, and a general task scheduler are outside this first change.

## Capabilities

### New Capabilities

- `sandbox-execution`: Explicit launch profiles, committed snapshot preparation, unattended guest execution, containment, readiness, lifecycle, and sandbox-aware observation.
- `sandbox-communication`: Host-controlled team addressing, scoped discovery and event exchange, local delivery integration, durable routing and receipts, reconnect behavior, and transport authorization.
- `sandbox-results`: Explicit request-correlated terminal results, progress separation, Git artifact availability and transfer, provenance, and preservation before cleanup.

### Modified Capabilities

None. This checkout's OpenSpec capability inventory is empty. Existing implemented local and trusted-relay behavior remains a compatibility constraint, rather than being restated as new specifications.

## Impact

- New host/guest bridge, profile, storage, transport, lifecycle, and result code should live primarily under a dedicated `src/sandbox/` boundary. Small hooks are expected in launch parsing/dispatch, message routing, instance projection/cleanup, and management commands. UI changes should expose location and connection state without duplicating the sandbox controller.
- Reuse upstream hcom tool launch, local hooks, PTY delivery, messages, and events inside each environment. Sandbox connections must be classified before MQTT dispatch and excluded from trusted-relay export and stale-peer cleanup.
- Use the OpenShell CLI as an optional external dependency; do not vendor its Rust crates or change OpenShell in this proposal. Pin and probe CLI/gateway/runtime capabilities because PATH currently selects an older CLI than the evaluated checkout.
- Use additive, sandbox-namespaced storage without consuming upstream migration numbers or requiring the existing approval-owner schema extension. Preserve messages and ordinary launches when the mode is unused.
- During implementation, update this checkout's patch manifest and its existing "No isolation implementation" scope statement narrowly for the new opt-in adapter. Record exact dependencies and focused checks; do not import historical isolation implementations or plans from other hcom folders.
- Require protocol/fault tests, local compatibility gates, a live three-agent communication and Git handoff trial, and independent review. Qualification failures must remain visible; speed of integration is secondary to preserving the boundary.
