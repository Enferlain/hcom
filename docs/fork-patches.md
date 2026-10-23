# Maintaining the downstream patch series

The foundation is upstream `de33e31a0cdbba4c5863f7cc050d8b79aa50bcdf` (0.7.31). The accepted previous rebuild is reference material, not a replacement source tree. The machine-readable [manifest](fork-patches.json) assigns each retained contract to a patch or to upstream, records dependencies and focused checks, and defines the intended behavior. Beads tracks implementation progress; this document describes the maintenance contract.

Upstream supplies Hermes provider registration, detection, plugin/backend ownership, installation, launch/resume/configuration and launched-session bootstrap. It also supplies the generic `events --follow` stream. Keep these implementations. Add only missing participation/search and observation/result behavior. Six shared Hermes contracts received no legacy implementation port: pristine-upstream Hermes Rust tests (32 passed, one ignored), its Python plugin suite (71 passed), and two adapted retained conformance tests passed. Four upstream generic-follow integration tests also passed. These checks establish the exercised behavior, not exhaustive equivalence.

## Patch inventory

Feature commits carry a `Fork-Patch: <id>` trailer and list contract IDs in their bodies. Supporting conformance/documentation commits need no feature trailer. Dependencies determine stacking order; a patch may touch the same upstream file as another patch. Substantial local logic belongs in dedicated modules where useful, while database transactions and lifecycle guards stay at their necessary integration points.

| Patch | Contracts | Depends on |
| --- | --- | --- |
| transcript-diagnostics | `transcript-path-diagnostic` | — |
| messaging | `explicit-send-policy`, `targeted-recipient-wake`, `request-ack-result-separation`, `thread-membership-lifetime` | — |
| approval-ownership | `approval-owner-race-safety` | — |
| antigravity-banner | `antigravity-idle-mode-banner` | — |
| antigravity-survey | `antigravity-feedback-survey` | approval-ownership |
| connection-signal | `subagent-connected-signal` | — |
| filtered-observation | `filtered-listen-isolation`, `events-wait-boundaries` | — |
| worker-progress | `durable-events-stream`, `compact-progress-stream` | messaging, filtered-observation |
| worker-results | `correlated-result-outcomes`, `provider-result-recovery`, `typed-launch-blockers` | messaging, approval-ownership, worker-progress |
| native-participation | `claude-native-session-start-adoption`, `codex-wrapperless-vscode-adoption`, `codex-thread-sender-resolution`, `tool-family-authoritative-binding`, `recent-kimi-bind-safety`, `hermes-session-attribution`, `hermes-session-adoption` | approval-ownership, connection-signal |
| hermes-extensions | `hermes-hook-dispatch`, `hermes-transcript` | native-participation |

Corrected retained transcript tests also pass upstream's session-scoped tracked search, while exposing four extension gaps: recorded-profile discovery under `--all`, tracked corrupt-database failure reporting, and two partial-error cases under `--all` (including a reached result limit). The local search extension covers those gaps; blank session attribution never broadens to the whole shared database.

Ordinary Hermes session ownership and explicit `hcom start` share the native participation binding layer. The later Hermes extension adds plugin hook discovery/delivery and recorded-profile transcript search, while retaining upstream gateway and parser implementations. Compact progress omits raw messages and command text; projected file paths remain untrusted input.

Waits without `--after-id` arm at the current durable cursor, including unfiltered waits; they do not retain upstream's 10-second quick lookback. Capture `events --cursor` before launching work and pass that cursor to include an event that arrives before the wait begins. Unfiltered waits retain the one-time unread preview, while filtered or explicit-cursor waits leave unrelated messages out of their output. `events --cursor` suppresses after-command inbox delivery to keep stdout machine-readable, leaving messages unread. One-shot wait and result output may add `data.message` as an alias of `data.text`, including with `--full`; it is not an exact stored-event representation.

## Checking a checkout

```sh
python3 scripts/fork-check.py
python3 scripts/fork-check.py --verify-series
python3 scripts/fork-check.py --test worker-results
```

The manifest's focused checks run against the complete checkout. At series creation, also build and run the relevant checks at each feature commit so a patch does not secretly depend on a later commit. Run serial Rust tests, formatting, strict all-target Clippy, locked release build, and relevant upstream plugin/type checks at the completed series. Record independent review and unrun platform/provider checks in the validation record. Do not claim a live-provider check from a fixture-only test.

## Updating upstream

1. Fetch upstream and choose an exact revision R.
2. Run conformance checks for any behavior upstream may now cover. Adapt tests to the current public behavior; an obsolete internal API or screen glyph is not evidence that upstream lacks the feature.
3. Start the next candidate branch at R. Carry the supporting manifest, checker and useful conformance tests too, adapting their registration to R rather than copying an old entry point. Use the manifest and `--verify-series` output to select retained feature commits in dependency order; replay them oldest first. Repository-local `rerere` is enabled, but review every reused resolution.
4. Resolve conflicts against the behavior contracts and current upstream implementation. If upstream covers a contract, remove its local implementation and keep useful conformance tests. If it covers part, retain only the missing extension. Preserve upstream thread persistence and Hermes plugin/parser/gateway handling.
5. Run focused checks at each replayed patch, then the complete gates and independent review. Update manifest ownership, dependencies, checks, and `last_checked_upstream` values.
6. Qualify a candidate executable before changing the installed release. Preserve a working rollback and current messages. Publication, main-branch migration, installation and removal of reference worktrees follow the user's authorized scope.

This avoids reviewing every upstream commit individually. It cannot eliminate conflicts where both implementations deliberately change the same behavior.

## Database compatibility

Approval-owner state is an additive column managed by `db/fork_schema.rs`; it does not consume upstream migration numbers. Both current upstream databases and the installed older fork must retain rows, messages and cursor state. The older fork used schema version 19 for this column, while upstream used 19 for an FTS repair. A narrow compatibility path reapplies that upstream repair when upgrading the older fork. Keep migration/concurrency regression tests with the approval patch.

## Files and installation

Source, regression tests, the manifest, this procedure and portable validation summaries belong in Git. `.patch-artifacts/` holds ignored reference copies, pristine-source test snapshots, raw review output, logs and executable candidates. `target/` is regenerable Cargo output. Provider/model preferences and user workflow scripts remain external assets.

The candidate is a linked Git worktree. Preserve the original repository's common Git directory until the maintained branch is deliberately consolidated into `/home/imi/Projects/hcom`; remove other worktrees through Git after checking their tracked and untracked contents. Building this candidate does not change the installed release.
