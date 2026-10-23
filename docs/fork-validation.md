# Feature-series validation

Recorded on 2026-10-23 on Linux x86_64 with rustc 1.99.0 (b940084d7 2026-09-28). Upstream foundation: `de33e31a0cdbba4c5863f7cc050d8b79aa50bcdf` (0.7.31). Final feature commit: `40042593c4dc4dbdf5827f2cff14edcfe0c639c7`. Supporting documentation commits do not carry feature trailers.

Implementation, local build/test checks and the final bounded integration review completed successfully. The review returned no blocking correctness findings (result_ok=true). This is not a live-provider or installation qualification.

## Complete checkout

- `cargo test --workspace --all-targets --locked --no-fail-fast -- --test-threads=1`: **3078 passed, 0 failed, 19 ignored**, across 18 test harnesses. Totals count executed tests, including repeated shared support tests in separate harnesses.
- `format`: `cargo fmt --all -- --check` passed.
- `clippy`: `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `release`: `cargo build --release --locked` passed.
- Upstream Hermes Python plugin suite: 71 passed. Downstream native-plugin suite: 12 passed. Both ran with `python3 -I` against the independently archived final feature prefix.
- Manifest ownership, dependency order, pinned upstream revisions, and exactly one feature commit per patch were checked with `scripts/fork-check.py` and `--verify-series`.

Cargo checks unset `HCOM_DEV_ROOT` and `CARGO_TARGET_DIR`. No ignored live-provider tests were enabled. All 11 feature prefixes were independently archived, compiled for all targets, and exercised with their focused checks. CLI error assertions and four legacy inbox-draining tests were adapted to the retained filtered-observation contract, with explicit later-delivery and cursor checks. These test-only corrections were exercised on the filtered-observation prefix and folded into that feature commit; range-diff confirmed no production-code change in the rewritten commits.

Release executable SHA-256: `b1259ee47745b306392b9a206c7e78b6fb2aa634f5788c2c3a274d34a4b6cbcf`. This identifies the locally built candidate; the executable itself is regenerable and is not committed.

## Upstream conformance

Pristine upstream runtime source passed 32 Hermes Rust tests (one provider test ignored), 71 Python plugin tests, two adapted retained Hermes conformance tests, and four generic-follow integration tests. These bounded checks justify upstream ownership of the six shared Hermes contracts; they do not establish exhaustive equivalence.

With corrected Hermes database fixtures, upstream passed the retained session-scoped tracked-search case. Four retained cases failed on the distinct extension gaps: recorded-profile discovery under `--all`, reporting a corrupt tracked database, and reporting partial errors under `--all` both before and after reaching a result limit. The completed candidate passes all nine transcript-harness tests, including its shared support cases.

## Review disposition

Initial review-mcp reviews covered messaging, event observation, and approval ownership. Their actionable findings were corrected and exercised by focused and complete gates. Messaging received a completed follow-up review. The interrupted native/Hermes requests produced no review result and were not counted as approval.

User-authorized OMP GLM 5.3 Flash reviews covered native/Hermes integration and approval/ACK races. Follow-up reports confirmed the requested corrections, with no remaining high or medium findings within their stated scope. The parent inspected the changes and ran the tests. Test-fixture, attribution, and CLI-assertion corrections made during final gates were inspected separately.

The completed final review-mcp integration review used GLM 5.3 Flash at high effort. It checked observation/inbox separation, cursor and generation boundaries, result recovery, ordinary Hermes attribution/gateway preservation, and connected/approval transaction interactions. It returned no blocking findings, with one LOW and three INFO observations. The default wait change from upstream's 10-second lookback to cursor-at-arm semantics, the data.message alias even under --full, and cursor-command delivery suppression are now recorded in the manifest/procedure. A gateway-host kind assignment was noted as inert while serving, without a routing change. An earlier quota-failed request remains preserved as failed evidence; the Oct23 retry completed with result_ok=true. No native fallback was used.

Remaining review observations include a fail-closed interrupted native-hook installation, pre-existing legacy elicitation/status-event behavior, and historical FTS entries not being rebuilt when a derived index was already destroyed. The additive approval-schema compatibility tests cover ordinary upstream/older-fork upgrades and retained rows, messages and cursors, not arbitrary database corruption recovery.

## Feature commits

| Patch | Commit |
| --- | --- |
| `transcript-diagnostics` | `12dbe030283b574eb60d79a2d25e5accd1eb0f04` |
| `messaging` | `733615added88c917b68e7a6af6bcded17338146` |
| `approval-ownership` | `d9475d75c8761f76c289b486dabf90f3655aac63` |
| `antigravity-banner` | `75601aa7a681a8beacc9d2a95ead9702bd185f4a` |
| `antigravity-survey` | `f21b64137c164ff115c0bd3a1016526be02c8518` |
| `connection-signal` | `fb888380a3a32902a24a47473e4b4c0bffa999e5` |
| `filtered-observation` | `26f79c23e47155bed184824ad0bd9edc735677bc` |
| `worker-progress` | `76395c371b024429c3121d1fcb41081c4745b083` |
| `worker-results` | `e1bb390660bcc5866c7080394e5dc69e1dd1bedf` |
| `native-participation` | `9371b6ee4c646530c330b6d9769c4fa8a3d5023b` |
| `hermes-extensions` | `40042593c4dc4dbdf5827f2cff14edcfe0c639c7` |

## Boundaries and retained evidence

Live model completion, desktop interaction, Windows runtime behavior, and migration of the actual installed database were not tested. Python fake-host tests and real local CLI subprocess tests do not substitute for those checks. The hcom workers themselves used the existing installed release.

The installed stable 0.7.28 executable remained unchanged (SHA-256 `7810b1618a3acd04032416a1ab6f63e0d0b71cc7384302012769a7afc4e6504e`). No branch was pushed and no candidate was installed. Reference checkouts were preserved.

Raw prefix/full-gate logs, failed attempts, pristine-source snapshots and review reports remain in ignored `.patch-artifacts/`; `target/` contains regenerable build output. The [manifest](fork-patches.json) and [update procedure](fork-patches.md) are the portable maintenance contract. No credentials, machine profile settings or raw agent transcripts are part of the series.
