# Maintaining the local fork

This rebuild starts at upstream `f687a9cf71d3fdfc5bde2634f2ffcd19520d6a14`. The local delta is defined by behavioral contracts in `fork-features.json`, with regression tests at current upstream interfaces. The manifest records port status; acceptance requires source review and relevant gates.

## Retained behavior

Principal local features are the surviving Hermes integration, deterministic observation/result waits, guarded native Claude/Codex participation, and narrow correctness fixes. Upstream owns base Claude lineage/generations, thread routing, relay replies, per-run hooks and provider/PID lifecycle improvements. Ports must preserve those implementations; replacing shared files with older fork copies loses upstream fixes.

Isolation is excluded, including preliminary types. Its original design/reference materials remain in the reference checkout. Passive transcript-marker identity binding and reverted Hermes mesh/dashboard experiments stay retired. User scripts, provider router, model choices and permission preferences remain separate external assets where upstream supports them. This rebuild does not switch installed binaries, modify provider profiles or activate GitHub automation.

## Updating upstream

Pin upstream before evaluating updates. Rebase or merge grouped downstream feature commits onto it in an isolated checkout; review conflicts against feature contracts. Upstream is the foundation, so there is no need to reclassify every upstream commit. When upstream covers a local contract, remove that local implementation after checking behavior and ownership guarantees. Keep changes grouped by behavior and current integration seam. Small broadly useful corrections can be proposed upstream through a separately authorized action.

Hermes/provider and orchestration state still need maintenance. This approach reduces duplicated mechanisms; it does not promise conflict-free updates.

## Validation

Check cursor boundaries, inbox ownership, generation/thread correlation, approval races, transactional binding/cleanup, provider argv and hook entrypoints with fixtures. Compare changed behavior against the pinned source as well as running tests. Run serial Rust tests, formatting, strict Clippy, locked build, relevant TypeScript/plugin checks and Hermes backend fixtures, followed by independent review. [fork-validation.json](fork-validation.json) records the executed checks, environment, artifact and remaining review or execution limits.

Do not export `HCOM_DEV_ROOT` globally into unit tests: they exercise its precedence. Pin the rebuild root/binary only in spawned CLI fixtures so they cannot silently run the installed fork. Keep DB path safety tests in a normal checkout rather than placing source under `/tmp`. When `CARGO_TARGET_DIR` is external, expose that build through the checkout’s ignored `target` path so dev-root routing finds the same binary; otherwise clean-log fixtures report a missing-build warning.

Record unrun live-provider/native-Windows limits explicitly. Installing or publishing the result is a separate action after validation and review.

## Files intended for version control

Keep the retained implementation under `src/`, regression fixtures under `tests/`, `.gitignore`, and the portable documentation under `docs/` in Git. The feature manifest defines the retained contracts; the validation record and [review conclusions](reviews/upstream-rebuild-2026-10-20.md) record acceptance and its limits.

Generated binaries and Cargo caches live in the ignored `target/` directory. Raw reviews/tool traces, logs, source-guard manifests, inventory drafts and historical worker scratch live in the ignored `.rebuild-artifacts/` directory. `node_modules/`, runtime `.hcom/` state and Python bytecode also stay out of Git. The raw archive is useful local evidence; a fresh clone needs only the source, tests and portable summaries to rebuild and understand the accepted change.

The checkout's `target/` is now a real directory rather than a link into `/tmp`. Build from the repository root with `env -u HCOM_DEV_ROOT -u CARGO_TARGET_DIR cargo build --release --locked`; run the serial suite with `env -u HCOM_DEV_ROOT -u CARGO_TARGET_DIR cargo test --workspace --all-targets --locked -- --test-threads=1`. The recorded gates predate artifact relocation, so original paths inside archived logs are historical provenance.

This checkout remains a linked Git worktree of the original repository. Its source and local artifacts no longer depend on the temporary folders, but its Git metadata still depends on the original repository's common Git directory; preserve that directory.

## External preferences and scripts

Keep the Claude permission preference in user configuration rather than adding a launcher default. Upstream exposes `launch.claude.args` (`claude_args` at the CLI); preserve other model/provider arguments when adding `--permission-mode auto`. The existing router stays in `/home/imi/.config/hcom-model-router`, and the user wrappers stay in `/home/imi/.hcom/scripts`. Their shell syntax passes. `tests/event_observation.rs` exercises their durable cursor/thread/worker wait arguments and result, timeout, blocker, failure and recovery responses against the rebuilt binary; launch name/batch output was checked against the preserved upstream formatter. Full provider wrapper workflows were not run. No profile has been changed by this rebuild.
