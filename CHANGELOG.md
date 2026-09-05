# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Rules:

- Use proper sub titles "Added", "Changed", "Removed" and "Fixed"
- Keep proper track of days for where entries should go
- Be concise but mention all changes without necessarily detailing each one

## [2026-09-05]

### Fixed

- Antigravity out-of-workspace file-access prompts are now detected as PTY
  approval blockers, so unattended hcom workflows can stop decisively instead
  of reporting a worker as active indefinitely.

## [2026-09-03]

### Changed

- Filtered `hcom listen --json` results are now an explicitly versioned
  contract: both match and timeout outcomes carry `schema_version` with the
  typed fields documented in `hcom listen --help`, keeping the legacy
  notification prose as a non-parsing surface.

### Fixed

- Claude permission denials now release only the matching hook-owned approval
  blocker, allowing an already queued targeted message to reach the resulting
  `What should Claude do instead?` prompt without external terminal input.
  Releases that omit the `PermissionDenied` hook are covered by a guarded PTY
  check for that settled empty prompt; policy denials and newer provider
  lifecycle states remain non-idle.

## [2026-09-01]

### Fixed

- Quiet terminal output no longer rewrites a provider-owned active worker as
  listening; screen stability is now diagnostic only, so silent reasoning and
  long-running tools remain non-idle and ineligible for message injection.
- Delivery gate diagnostics reuse their existing screen lock instead of
  recursively acquiring it while a writer may be queued.

## [2026-08-31]

### Added

- Launch-blocked results now expose structured blocker records with typed
  `workspace_trust`, `authentication`, `quota`, `confirmation`, `crashed`, or
  `unknown` kinds and separate matched evidence while preserving the existing
  human-readable blocker list.

### Fixed

- Structured approval signals now take precedence over incidental command text,
  successful exit code 0 is not classified as a crash, and only distinctive
  workspace-trust prompts bypass launch-screen settling.
- Antigravity approval responses now suppress the stale prompt for one redraw,
  preventing a cleared approval from briefly returning as blocked.

## [2026-08-29]

### Changed

- `--idle` now ignores transport, startup, and orphan-recovery wait states and
  matches genuine task-idle transitions.
- Direct Antigravity callers can compose a pre-launch cursor, raw launch, and
  one generation-aware blocking result wait without repeated status polling.

### Fixed

- Exact `--result-from` waits now recover thread- and session-scoped final
  results from stopped Antigravity and Claude-format workers, including GLM,
  and label transcript provenance instead of timing out after completed work.
- Named `hcom list <worker> --json` results now expose status context, detail,
  computed age, and stored provider-state age consistently with the full
  listing, enabling deterministic idle-worker recovery without another broad
  status query.
- Launch readiness now treats provider-owned turn and tool activity as
  authoritative without overwriting active task state, preventing workers that
  have begun execution from later being reported as launch-blocked. Later
  approval prompts remain task-level blockers rather than launch failures.
- Thread-routed requests now retain abandonment detection by creating durable
  request watches for their delivered recipients.
- Filtered `hcom listen` now returns a nonzero timeout result with structured
  JSON; `--timeout-ok` preserves the legacy exit code when explicitly needed.
- Filtered waits perform a final event scan before timing out, so events in the
  last polling interval are not reported as false timeouts.

### Removed

- `agy` and `glm` are no longer shipped as built-in workflows. Existing
  `~/.hcom/scripts/agy.sh` and `glm.sh` files continue to run as user-created
  workflows.

## [2026-08-28]

### Added

- Added `hcom events --cursor` and `--result-from <agent>` for durable,
  generation-aware worker-result waits.

### Changed

- Recipient-free `hcom send` calls now require `--broadcast`; targeted and
  seeded-thread sends continue to work without it.

### Fixed

- Prevented result waits from accepting another worker's message, a result
  from the wrong workflow thread, or a stale result from a reused agent name.
- Result correlation now survives report-then-stop ordering and ignores failed
  launch placeholder stops.
- `--result-from` now fails clearly when combined with an events subcommand
  instead of being silently ignored.

### Removed
