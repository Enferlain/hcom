## 1. Reconcile the Staged Contract

- [x] 1.1 Adapt `core/progress.rs` into a stream-owned compact record contract, remove `input_required` payloads and exit codes, and restore wait-only visibility in `core/result_wait.rs`; verify exact serialization fields, event cursors, and secret-bearing fixtures.
- [ ] 1.2 Pin existing `events --wait` first-match output, cursor, timeout, correlated result, blocker, failure, and stopped-result recovery behavior with compatibility tests before listener extraction.

## 2. Shared Listener and Stream CLI

- [ ] 2.1 Extract cursor-ordered event querying, optional notify-endpoint registration, bounded rechecks, and cleanup into an internal listener used by wait and stream; verify wait compatibility tests remain unchanged and simultaneous endpoint kinds do not collide.
- [ ] 2.2 Add `events stream` with its own arguments for existing filters, `--after-id`, streamlined/full output, and optional timeout; verify parser conflicts, help text, multiple ordered matches, default current-cursor behavior, and resume without replay.
- [ ] 2.3 Implement explicit flush-on-record output plus clean interruption, timeout, and broken-pipe handling; verify the stream removes only its own endpoint and never changes observed worker state.

## 3. Compact Worker Observation

- [ ] 3.1 Separate worker-generation discovery from result-message filter injection and bind compact following to one live/post-cursor generation; verify missing, ambiguous, stopped, and rapidly reused names fail closed or terminate at the correct boundary.
- [ ] 3.2 Implement the typed compact classifier with phase deduplication, file/command coalescing, allowlisted command categories, optional rate-limited heartbeats, and deterministic injected timing; verify noisy traces have a bounded output count and raw command, argument, environment, transcript, and message text never serialize.
- [ ] 3.3 Keep generic stream output compatible with existing event projections while documenting that only compact mode is safe for model context; verify generic filters retain their current AND/OR and type-validation semantics.

## 4. Messaging and Compatibility Isolation

- [ ] 4.1 Prove streaming has no subscription, request-watch, inbox, or message side effects; verify a worker can send and receive an ordinary correlated `intent=request`/reply while a compact stream remains active.
- [ ] 4.2 Preserve existing `events`, `events --wait`, `events sub`, and `hcom run` defaults; verify no stream starts without an explicit command or workflow option and existing scripts retain terminal-only wait output.
- [ ] 4.3 Update CLI and workflow-authoring guidance to describe snapshot, one-shot wait, conversational subscription, generic stream, and compact worker observation as distinct choices.

## 5. Integration and Live Verification

- [ ] 5.1 Add hermetic integration coverage for simultaneous wait/stream listeners, unrelated workers, cursor races, noisy activity, exact-generation reuse, interruption, timeout, broken pipe, and final cleanup; verify the focused suite is stable across repeated runs.
- [ ] 5.2 Run one live worker flow with the compact stream kept as a yielded background process while the parent performs other work and exchanges one normal hcom request/reply; verify status remains bounded, no diagnostic polling occurs, one-shot result waiting remains unchanged, and the worker survives stream termination.
- [ ] 5.3 Run formatting, focused and workspace tests, `cargo check --workspace --all-targets`, strict Clippy, and independent review; record results and any host-side live-output limitations in Beads and the changelog.
