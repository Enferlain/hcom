# Upstream rebuild review, 2026-10-20

The candidate starts at upstream `f687a9cf71d3fdfc5bde2634f2ffcd19520d6a14` and implements the 29 retained contracts in [the feature manifest](../fork-features.json). Local workspace isolation and retired experiments are excluded. Review used `review-mcp review_with_context` against `HEAD`, with explicit context for new files.

## Full review

The full candidate review found four actionable issues. The resulting corrections were:

| Finding | Resolution |
| --- | --- |
| Codex/Hermes process recovery could bypass tool-family checks | Use the existing transactional tool-checked binder; foreign sessions cannot switch ownership or retire another row. Remove the obsolete OMP-only recovery fallback from Hermes. |
| Cursor acknowledgement could overwrite a concurrent newer cursor | Use the existing single-statement atomic MAX cursor operation for Hermes and OMP ACK/init paths. |
| Resumable soft stops entered terminal-result recovery | Exclude soft stops from terminal-stop recovery and blocker suppression while preserving hard-stop behavior. |
| Hermes comments described removed isolation machinery | Document the actual HERMES_HOME/HOME profile lookup. |

## Follow-up review

Review confirmed the binding, soft-stop, comment and Hermes cursor fixes. It identified two remaining issues: the stricter thread matcher rejected hcom's own delivered-message envelopes, and OMP's three cursor writes still needed the monotonic primitive.

The matcher now recognizes the exact thread in formatter-generated `request`, `inform`, `ack` and `thread` envelopes, with or without reply IDs and hcom wrapper tags. It retains rejection of punctuation prefix/suffix collisions, unknown tags and ambiguous quoting. Formatter-derived tests exercise both Claude and Antigravity recovery; a CLI fixture exercises recovery from a production envelope. OMP ACK/init writes now use the atomic primitive, with independent-writer contention and stale/duplicate ACK regressions.

## Final review

The final scoped review checked both corrections against the current formatter, wake builder, recovery paths and database primitive. It accepted both with no new actionable findings. Informational observations were that ANSI human-terminal output is outside the matcher protocol, init's second-call test covers its zero-cursor guard while the ACK race test covers atomic MAX, and the pre-existing cursorless fallback and test environment assumptions remain unchanged.

After the corrections, formatting, strict all-target Clippy, all serial Rust targets and the locked release build passed: **2,880 passed, 18 ignored, zero failed**. The unchanged Hermes backend passed 26 Python tests and TypeScript typecheck passed. See [the validation record](../fork-validation.json) for exact counts, commands, environment and artifact hash.

## Limits and raw evidence

Independent review was source-level; it did not run live providers. Final review focused on the corrective delta, using the full initial review as its baseline. Real-provider/terminal, native Windows and Rust 1.88 checks remain unrun as recorded in the validation report. TypeScript passed on Node 24 with the repository's Node 22 engine warning.

Raw review responses/tool traces, test logs, failed attempts, inventory and source guards are preserved locally under `.rebuild-artifacts/`, which is ignored by Git. Their recorded temporary paths describe the original execution. These concise conclusions, the feature manifest and validation record are the portable evidence intended for version control.
