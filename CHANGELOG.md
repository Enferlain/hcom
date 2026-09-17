# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Rules:

- Use proper sub titles "Added", "Changed", "Removed" and "Fixed"
- Keep proper track of days for where entries should go
- Be concise but mention all changes without necessarily detailing each one

## [2026-09-16]

### Fixed

- Compact worker followers attached after an already-running worker's latest
  event now seed heartbeat state from the live worker status, avoiding a
  permanently silent stream without replaying events at or before the
  exclusive `--after-id` cursor.

- Claude permission blockers stay dominant across parallel tool completion:
  the instance row now records which tool use id opened an interactive
  approval, so sibling `PostToolUse`, `PostToolUseFailure`,
  `PermissionDenied`, or late-arriving `PreToolUse` hooks for other tools in
  the same parallel batch can no longer clear the blocked state and hide an
  unresolved prompt. Only the approving tool's own completion or denial
  resolves it; approvals recorded without a tool use id fall back to
  formatted tool-detail matching, and Claude `Read` approvals now surface the
  file path as their status detail.

### Changed

- Delegation guidance now defines `--after-id` as an exclusive cursor that is
  captured before launch, forbids short wait/list/log polling loops, and uses
  visible interactive launches by default; `--headless` is reserved for
  explicitly requested background workers.

- Repository delegation guidance (AGENTS.md, CLAUDE.md, mirrored) and the
  hcom-agent-messaging gotchas now require non-empty prompts, observe workers
  through one durable cursor/stream rather than recreated short streams, and
  act on surfaced blockers with at most one targeted terminal inspection
  after a plausible stall.

## [2026-09-15]

### Fixed

- Missing transcript search tools now retain the actionable not-found diagnostic
  when an inaccessible `PATH` entry makes process spawning report permission
  denied, while real non-executable candidates and explicit paths still surface
  their permission errors.

## [2026-09-12]

### Added

- Added explicit continuous `hcom events stream` observation with durable
  cursors, existing event filters, line-flushed generic output, and optional
  exact-generation `--follow NAME --compact` progress records with bounded
  phase, file, command-category, and heartbeat updates.
- Added hermetic coverage for simultaneous wait and stream listeners, cursor
  races, unrelated and reused worker generations, noisy activity, interruption,
  timeout, broken pipes, cleanup, and ordinary request/reply messaging alongside
  compact observation.

### Changed

- Event and workflow guidance now distinguishes snapshot queries, one-shot
  waits, conversational subscriptions, generic streams, and compact worker
  observation. Existing `events`, `events --wait`, `events sub`, and `hcom run`
  behavior remains opt-in and unchanged by streaming.
- Tool hosts may need to poll the existing yielded process handle to display
  newly flushed stream records; `hcom` does not daemonize the stream or issue
  follow-up event queries on the host's behalf.

## [2026-09-09]

### Added

- Added the versioned internal record contract for compact worker observation
  streams, including exact worker generation, durable event cursors, source
  event timestamps, safe activity categories, and serialization coverage that
  prevents raw command details, environment values, transcript text, and message
  bodies from entering stream records. Runtime event streaming is not
  implemented yet.

### Fixed

- Native subagent connections now produce an atomic, exactly-once structured
  lifecycle event instead of requiring a model-generated announcement; the
  event is filterable, subscribable, and visible in the TUI without entering
  the conversational message channel.

## [2026-09-08]

### Added

- Added bounded concurrent regression coverage for correlated waits across
  repeated cursors, multiple workers, cancellation, acknowledgements, and
  results arriving on either side of waiter startup.

### Fixed

- Delegated-task bootstrap guidance no longer requires immediate
  model-generated acknowledgements or routine progress chatter; workers send a
  substantive `inform` result or a necessary `request`, while explicit
  acknowledgement semantics remain available when requested.
- Current Antigravity workspace-trust dialogs are now returned as typed
  `workspace_trust` launch blockers with the exact prompt evidence, without
  approving the workspace or changing provider trust state.
- Common command-grammar retries now resolve directly: transcript accepts
  `--tail`, events accepts `--limit`, invalid `from_agent` SQL points to
  `msg_from`/`--from`, and exact bare agent names work in multi-word direct
  sends without weakening explicit broadcast or thread scoping.

## [2026-09-06]

### Added

- Added validated, immutable isolation profile, workflow/attempt identity, and
  non-secret isolation-plan types with deterministic plan identities and strict
  deserialization checks.
- Correlated `hcom events --result-from` waits are now a single atomic attempt
  wait: besides the authoritative result and stopped-worker transcript
  recovery, they terminate on a typed actionable blocker (`pty:approval`,
  `pty:survey`, `elicitation`, unresolved `launch_blocked`), a launch failure,
  or a stop without a recoverable result. All terminal scans are anchored at
  the pre-launch `--after-id` cursor, so a blocker that fired between launch
  readiness and wait registration cannot be missed, and every non-result
  termination prints one structured outcome preserving the worker generation,
  workflow thread, attempt cursor, blocker evidence, and recovery guidance.
  New exit codes: `4` typed blocker, `5` launch failure (`3` remains
  stopped-without-result and keeps its legacy `result_unavailable`/`timed_out`
  markers for script compatibility).

### Fixed

- Targeted and thread-resolved messages now wake only their resolved local
  recipients; explicit broadcasts retain system-wide wake fan-out.
- Pending Antigravity survey-blocker clears keep the bounded Unix poll cadence,
  while failed publication and clearing warnings are rate-limited to avoid
  repeated log and database work.
- Raw interactive and headless `hcom claude` launches now default to Claude
  Code's `auto` permission mode, while explicit CLI or `claude_args`
  permission modes continue to override that default.
- Antigravity sandbox-bypass approval prompts (`Requesting permission for:`
  paired with `Allow sandbox bypass for command execution?` and its affirmative
  numbered menu) are now recognized as `blocked/pty:approval` blockers,
  handling realistic 80-column wrapping, rejecting stale scrollback or task
  text, and preserving command detail through denial and clearance lifecycles.
- Antigravity feedback surveys are now recognized as non-task prompts and
  safely skipped; failed dismissal becomes a typed `pty:survey` blocker instead
  of leaving unattended result waits silently stuck.
- Antigravity mode banners (`Accept-edits`/`Plan`/`Best-of-N`) on an empty
  prompt are no longer scraped as uncommitted input text, so targeted delivery
  to a ready idle worker injects and submits exactly one turn instead of
  blocking forever on `tui:prompt-has-text`.

## [2026-09-05]

### Changed

- Updated the Antigravity workflow default to Gemini 3.8 Flash High.
- Promoted the provider-neutral isolated-worker boundary from a downstream idea
  to a blocking daily-use design, including its fail-closed launch contract,
  PTY integration seam, scoped hcom state, security limits, test matrix, and
  staged Bubblewrap rollout.

### Fixed

- Windows targets compile again with the `Tool` import required by ConPTY
  delivery-state checks.
- Fixed an issue where auto-thread memberships would outlive workflows, causing
  cross-workflow noise. They now expire atomically when their participant's
  lifecycle closes without affecting unrelated delivery state.
- Antigravity out-of-workspace file-access prompts are now detected as PTY
  approval blockers, so unattended hcom workflows can stop decisively instead
  of reporting a worker as active indefinitely.
- Normal `hcom list` and TUI output now describe internal filtered waits as
  `event filter`; verbose list output retains the raw diagnostic context.

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
