# Worker Model Observations

This document records practical observations from models used through hcom.
It is an evidence log, not a permanent ranking or a promise that a provider
will keep a model identifier available. Update it after substantive runs so
task routing can reflect observed behavior instead of reputation alone.

## How to record a run

Record the model and effort, the kind and approximate difficulty of the task,
whether the result was accepted after parent review, and any operational issue
that came from hcom or the provider rather than the model. Prefer repeated
patterns over conclusions from one run.

Useful outcome dimensions are:

- correctness and completeness after review;
- unnecessary scope or edits to pre-existing work;
- quality and cost of tests or verification;
- time to a useful result;
- need for clarification or parent intervention;
- launch, permissions, delivery, or reporting failures outside model control.

## Current working summary

These summaries are provisional and reflect local hcom development runs through
2026-09-09.

| Model and effort | Observed strengths | Observed limitations | Current task fit | Confidence |
| --- | --- | --- | --- | --- |
| GLM 5.3, max | Deliberate repository work, broad regression analysis, and detailed implementation reports. Has completed focused Rust fixes and test work successfully. | Usually slower than the Flash models and can spend heavily on context and exhaustive reasoning. | Medium-to-hard backend changes where careful coverage is worth the latency. | Medium |
| GLM 5.3 Flash, max | Careful on bounded audits and test additions. Two event-wait coverage tasks found existing coverage before adding only missing scenarios, with no unnecessary production changes. | Speed has varied substantially: a recent four-test compatibility task took about 22 minutes and used a large reasoning context despite fast parent-side verification. Evidence remains concentrated in test-oriented work. | Small-to-medium tests, audits, and well-specified implementation tasks where scope discipline matters more than minimum latency. | Medium |
| Gemini 3.8 Flash High | Fast routine implementation and codebase exploration. In a natural hcom terminal test it followed the repository graph workflow and used the full codebase-memory discovery sequence without being told to invoke the MCP by name. | Some earlier runs needed parent correction around preserving unrelated work. Antigravity approval and launch blockers observed in those runs were integration defects and should not be scored as model failures. | Routine implementation, exploration, and quick iteration with parent review. | Medium |
| Claude Opus 4.6 Thinking | Reserved for difficult reasoning, cross-cutting design, and security-sensitive work where a stronger slow pass is justified. | Local evidence is limited and Antigravity Claude allocation is comparatively scarce. Workspace-trust and permission interruptions were provider-integration problems, not useful evidence about code quality. | Hard correlation, isolation, or architectural work after the task boundary is well specified. | Low |

## Interpretation rules

- Separate model quality from launcher, terminal, permission, message-delivery,
  and completion-report behavior.
- Do not count a worker report as success until the parent inspects the diff and
  reruns verification proportional to risk.
- Do not infer broad capability from a documentation-only or test-only task.
- Record requested changes and the number of repair cycles when reviewing a
  model's output; a passing final tree alone hides supervision cost.
- Prefer the cheapest model with repeated success for the task shape, while
  reserving stronger models for genuinely difficult or high-risk boundaries.

## Run notes

### 2026-09-09 — Gemini 3.8 Flash High, code discovery

- Task: identify callers and signature impact for `events_wait`, read-only.
- Route: normal interactive `hcom agy` terminal.
- Outcome: correctly used `list_projects`, `index_status`, `search_graph`,
  `trace_path`, `get_code_snippet`, and `check_index_coverage`, then sent one
  concise hcom report. Raw transcript inspection confirmed the MCP calls.
- Intervention: none after launch.
- Confounder: Antigravity reads generated JSON tool schemas before its first
  call to each MCP tool; this is bridge overhead, not model reasoning quality.

### 2026-09-10 — Gemini 3.8 Flash High, event-listener extraction

- Task: OpenSpec task 2.1, extract reusable cursor, notification, bounded
  recheck, and cleanup mechanics from the mature one-shot wait loop.
- Outcome: produced the intended single-file listener abstraction and four
  focused tests without implementing the later stream CLI. Independent review
  found the structure sound but caught one medium compatibility regression:
  query bind/step errors had become fatal instead of retryable. Parent restored
  the previous behavior, removed a duplicated endpoint-kind literal, hardened
  one timing test, and fixed one strict-Clippy finding. All focused tests,
  workspace check, strict Clippy, formatting, and diff hygiene then passed.
- Model assessment: useful implementation and good scope control, but it needed
  parent review for an error-path semantic difference that happy-path tests did
  not expose.
- Operational confounder: the parent mistakenly launched Antigravity with
  `accept-edits` instead of `auto`. Harmless formatter commands stopped for
  approval three times, and hcom reported generic command activity rather than
  an attention-required event. The roughly 45-minute wall time therefore must
  not be treated as model latency. Future unattended Antigravity trials use
  `auto`.

### 2026-09-10 — Gemini 3.8 Flash High, stream lifecycle handling

- Task: OpenSpec task 2.3, add per-record flushing, clean broken-pipe and
  signal termination, endpoint cleanup isolation, and worker-state invariance
  coverage to `events stream`.
- Outcome: produced the requested focused implementation using the existing
  cross-platform signal helpers. Parent verification passed 59 event-command
  unit tests, all 11 `events_stream` CLI tests (including real SIGINT, SIGTERM,
  broken pipe, and immediate-flush processes), workspace all-target check,
  strict Clippy, formatting, and diff hygiene.
- Model assessment: the implementation direction and behavioral coverage were
  useful, but the run did not reach its own final report. The worker added a
  comparatively large subprocess-test footprint, which still needs an
  independent review before acceptance. Do not interpret the wall time as
  clean model latency.
- Operational confounder: hcom was invoked with `--mode auto`, but the live
  Antigravity UI showed `accept-edits`. The worker silently blocked three times
  on `target/debug/hcom`, `sqlite3`, and `cargo fmt --all -- --check`; neither
  `hcom events --wait` nor the activity status surfaced an attention-required
  event. The parent granted two narrow session-only prefixes, then stopped the
  worker at the third prompt and completed verification locally. This is
  tracked as P1 bug `hcom-5qe` and must not be scored against Gemini quality.
- Review status: the first independent review attempt timed out after the stale
  900-second client limit. After the limit was corrected to 1500 seconds, the
  review completed and confirmed the core behavior. Parent fixed its actionable
  findings: a Windows-incompatible flush-test termination, signal tests that
  could fall through to timeout, the pre-registration signal window, slow
  identity-backed stream rechecks, and cleanup ownership keyed too broadly.
  Task 2.3 was then accepted after all gates passed again.

### 2026-09-10 — GLM 5.3 Max, generic event stream CLI

- Task: OpenSpec task 2.2, add the explicit generic `events stream` command,
  durable cursor continuation, filtered-tail advancement, wake routing, help,
  and subprocess-level compatibility tests.
- Outcome: correctly implemented the difficult scan-boundary logic and added
  broad unit/CLI coverage. Parent verification passed. Independent review found
  one medium parser gap: filters typed before the subcommand were silently
  ignored, which could produce an unfiltered stream. Parent made parent-level
  flags and filters fail closed, added checked timeout arithmetic and tests,
  and updated notify documentation. All focused gates and strict Clippy passed.
- Cost and latency: completion arrived about 23 minutes after launch; the
  terminal reported roughly 55K context tokens at the 15-minute inspection.
  The worker spent meaningful effort debugging a default-cursor subprocess
  test rather than waiting on permissions.
- Model assessment: strong on stateful backend mechanics and exhaustive tests,
  but expensive and still dependent on independent review for CLI misuse paths.

### 2026-09-08 — GLM 5.3 Flash Max, event-wait coverage audit

- Task: audit six result-wait scenarios and add only missing tests.
- Outcome: recognized two scenarios as already covered, added five focused
  concurrency and lifecycle tests, and reported 43 focused tests passing across
  four runs. Parent verification accepted the test-only scope.
- Intervention: no production repair was required because the new tests exposed
  no defect.

### 2026-09-09 — GLM 5.3 Flash Max, wait compatibility pins

- Task: OpenSpec task 1.2, audit and pin the pre-refactor `events --wait`
  contract without changing production behavior.
- Outcome: found most requested behavior already covered and added four missing
  tests across `src/commands/events.rs` and `tests/cli_smoke.rs`. Parent reran
  formatting, 45 focused event tests, 42 CLI smoke tests, and the workspace
  all-target check successfully.
- Scope discipline: good; it left OpenSpec, Beads, changelog, the model ledger,
  and production code untouched as requested.
- Cost and latency: approximately 22 minutes to the completion report and a
  reported 34.6K-token context partway through the run. Parent verification of
  the resulting diff took about 11 seconds. Treat the model as careful rather
  than predictably low-latency for this task shape.
- hcom behavior: launch and reporting worked without permission intervention.
  One 10-minute result wait expired while the worker continued normally; the
  rearmed wait received the final report at its deadline.

Add future entries when a run materially changes one of the summaries above.
