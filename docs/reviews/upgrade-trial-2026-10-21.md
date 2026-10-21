# Live upgrade qualification, 2026-10-21

The published rebuild at `08588f8` was tested as a candidate while the old default remained active. The candidate executable and previous effective executable were stored separately from disposable build caches; the live database was snapshotted before trial.

## Initial live checks

Both real Gemini and GLM sessions launched and completed a local hash/file task, reporting hcom 0.7.28 through ordinary messaging. GLM also received a fresh nonce only through a follow-up hcom message, wrote the expected bytes and satisfied the cursor/thread/worker-correlated result wait with exit 0.

Gemini's follow-up timed out after 90 seconds. A captured ready idle screen contained the non-dim accept-edits banner, which the rebuild misclassified as typed input (`tui:prompt-has-text`). No default switch was made. This exposed the omitted independent original-fork fix hcom-f6g.10; its historical isolation label did not make the fix isolation-specific.

## Banner restoration

Restored only the original three known mode banners, exact/wrapped-prefix recognition, and the input extractor check. Tests cover full/non-dim and wrapped banners, the missing-ready-footer case, mode-head text remaining protected, and delivery-state injection/clear/submit-edge wiring. Production approval and delivery code remains unchanged.

Validation passed: **2,887 Rust tests, 18 ignored, zero failures**, formatting, strict all-target Clippy and locked release build. Focused PTY tests passed 241 cases; the banner filter passed 8 (overlapping counts). Independent review accepted the bounded change and traced the mixed-prompt refusal and fresh approval check before Enter. A rare genuinely typed full banner/prefix can receive one appended wake preview, but the resulting mixed prompt is not submitted. Very narrow panes and changed provider banner wording still fail closed.

A fresh live Gemini retest passed against the saved banner-fixed executable: it completed the hash/file task, then received a newly generated nonce only through ordinary hcom messaging, wrote the exact expected bytes, and satisfied the cursor/thread/worker-correlated result wait with exit 0. No manual Enter was supplied after the follow-up message. Its final ready screen reported an empty prompt. The default installation remains unchanged pending survey qualification.

## Survey restoration and remaining qualification

The neighboring independent feedback-survey safeguard hcom-f6g.9 is also absent from the rebuild. Its original behavior safely skips a non-task survey or publishes an actionable typed blocker, without treating it as permission approval. The installed Antigravity binary still exposes feedback-survey UI, but no live survey was observed in this trial. Restoration and validation are tracked under hcom-4ap.2 before activation; isolation remains excluded.

The safeguard has now been restored in the four PTY files. It sends only ASCII `0` without Enter, bounds retries and elapsed survey time, rejects current command-approval dialogs below quoted survey text, and preserves foreign/replacement blockers. Status mutation and event insertion share one database write scope; subscriptions and wake notifications follow commit. Regression tests force event insertion to abort and verify rollback without changing the shared title. Unix writes reject zero-byte writes and bound would-block retries. Windows uses the same controller on both output and quiet ticks.

The parent full locked suite passed **2,916 Rust tests, 18 ignored, zero failures**, with formatting and strict all-target Clippy clean. The locked release build also passed. Fresh final-candidate Gemini and GLM sessions both passed the exact file-hash/version task and a nonce delivered only through a follow-up message. Both cursor/thread/worker-correlated result waits exited 0, and the expected nonce-plus-newline bytes were independently checked. No manual prompt submission was supplied after either follow-up. Gemini required scoped workspace trust and command approvals during phase 1; no persistent approval rule was added. Independent review remains pending at this checkpoint.

The real Claude trial exercised upstream's migration from legacy hcom-owned global hooks/env/permission entries to per-run settings. Other provider preferences remain separate. Auto-mode classifier denials during the source fix required a temporary manual-approval session; they are integration evidence, not model-quality evidence. No persistent broad approval rules were added.

Raw screens, launch/results and review responses are retained locally under ignored `.rebuild-artifacts/live-trial-2026-10-21/`. The original full rebuild validation remains in [fork-validation.json](../fork-validation.json). Native Windows, live-survey dismissal and Rust 1.88 execution remain unverified.


## Survey review findings

The first independent review verified the complete detector, Skip-only policy, retry/grace bounds, Unix/Windows quiet/data wiring, transaction rollback and post-commit callbacks. It requested three bounded corrections before activation: release the survey blocker to active until the normal ready path signals listening, stop falling-edge cleanup retries when the instance row has vanished, and store actionable survey detail on the row rather than retain stale approval detail. All three corrections passed focused regression tests and an independent follow-up review. The final full suite passed **2,917 Rust tests, 18 ignored, zero failures**, with formatting and strict all-target Clippy clean. The final locked release build also passed. The qualified release is saved independently of the source build cache. Clearing to active retains the existing non-Codex quiet-output recovery path if no subsequent hook marks listening.

The optional tall-pane concern was checked against the pinned `atuin-vt100` 0.19.1 implementation: `Screen::rows(start, width)` restricts columns and iterates all visible rows. Passing the column count is correct; no geometry change is needed for that concern.
