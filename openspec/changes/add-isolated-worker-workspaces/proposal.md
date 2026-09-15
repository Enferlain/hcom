## Why

Delegated hcom workers are currently stopped by provider-specific
workspace trust, exact-command approvals, and sandbox failures, forcing the
calling model back into expensive process supervision. hcom needs one
provider-neutral, fail-closed workspace boundary that preserves normal live
agent communication before delegated agents can be used reliably for daily
development.

## What Changes

- Add hcom-level isolation profiles that are resolved independently from the
  selected provider and launch backend.
- Record explicit workload trust and separate stable workflow-policy identity
  from attempt-specific runtime identity.
- Add Linux Bubblewrap preflight and explain commands, including platform
  runtime discovery and a minimum safe-version gate.
- Enforce workspace and process isolation at the PTY provider-child spawn seam
  while leaving hcom's PTY supervisor outside the boundary.
- Keep hcom databases on the host and expose a narrow workflow-scoped
  communication endpoint for ordinary messages, requests, correlated results,
  blockers, and compact progress events instead of exposing database files.
- Add small provider isolation adapters for configuration, session state, and
  authentication requirements.
- Make the hcom boundary the primary enforcement layer so routine in-boundary
  read, edit, build, test, and explicitly authorized GitHub operations run
  without provider approval prompts; reserve intervention for boundary escape,
  protected resources, credential or policy changes, and ungranted destructive
  capabilities.
- Enforce external-service grants through scoped identities or brokers rather
  than treating provider auto-approval as authorization.
- Add containment tests with a fake provider before enabling Antigravity and
  Claude-backed GLM.
- Fail before provider launch on unsupported platforms or invalid isolation
  plans; never fall back silently to host execution.

## Capabilities

### New Capabilities

- `isolated-worker-workspaces`: Resolve, explain, enforce, observe, and clean up
  provider-neutral workspace isolation for hcom-launched workers.

### Modified Capabilities

None.

## Impact

- Adds an isolation runtime and Linux Bubblewrap backend to the Rust binary.
- Extends launch parsing, launch records, PTY child construction, status
  diagnostics, resume validation, and cleanup.
- Adds explicit trust, workflow-policy, and attempt identity for isolated
  runtime state without requiring a wholesale persistence rewrite.
- Requires Bubblewrap 0.12.0 or newer for the Linux security profile and
  platform-aware runtime mounts, including NixOS support.
- Initially supports trusted local repositories through normal interactive and
  high-level PTY-backed workflows. `hcom run` remains an optional consumer of
  the same communication path rather than the required interaction model.
- Keeps workflow identity independent from workspace ownership so a later,
  explicitly authorized shared-workspace mode can be added without redesigning
  isolation identity; sharing and conflict policy are not part of this change.
- Untrusted GitHub-event workloads remain disabled until credential and network
  egress brokering are implemented.
- Tracked by Beads epic `hcom-f6g` and elaborated in
  `docs/isolated-worker-workspaces.md`.
