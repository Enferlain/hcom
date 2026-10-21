# Live upgrade qualification, 2026-10-21

The published rebuild at `08588f8` was tested as a candidate while the old default remained active. The candidate executable and previous effective executable were stored separately from disposable build caches; the live database was snapshotted before trial.

## Initial live checks

Both real Gemini and GLM sessions launched and completed a local hash/file task, reporting hcom 0.7.28 through ordinary messaging. GLM also received a fresh nonce only through a follow-up hcom message, wrote the expected bytes and satisfied the cursor/thread/worker-correlated result wait with exit 0.

Gemini's follow-up timed out after 90 seconds. A captured ready idle screen contained the non-dim accept-edits banner, which the rebuild misclassified as typed input (`tui:prompt-has-text`). No default switch was made. This exposed the omitted independent original-fork fix hcom-f6g.10; its historical isolation label did not make the fix isolation-specific.

## Banner restoration

Restored only the original three known mode banners, exact/wrapped-prefix recognition, and the input extractor check. Tests cover full/non-dim and wrapped banners, the missing-ready-footer case, mode-head text remaining protected, and delivery-state injection/clear/submit-edge wiring. Production approval and delivery code remains unchanged.

Validation passed: **2,887 Rust tests, 18 ignored, zero failures**, formatting, strict all-target Clippy and locked release build. Focused PTY tests passed 241 cases; the banner filter passed 8 (overlapping counts). Independent review accepted the bounded change and traced the mixed-prompt refusal and fresh approval check before Enter. A rare genuinely typed full banner/prefix can receive one appended wake preview, but the resulting mixed prompt is not submitted. Very narrow panes and changed provider banner wording still fail closed.

A fresh live Gemini retest is pending. These results qualify the banner correction's source and build; they do not constitute an activation claim.

## Remaining qualification

The neighboring independent feedback-survey safeguard hcom-f6g.9 is also absent from the rebuild. Its original behavior safely skips a non-task survey or publishes an actionable typed blocker, without treating it as permission approval. The installed Antigravity binary still exposes feedback-survey UI, but no live survey was observed in this trial. Restoration and validation are tracked under hcom-4ap.2 before activation; isolation remains excluded.

The real Claude trial exercised upstream's migration from legacy hcom-owned global hooks/env/permission entries to per-run settings. Other provider preferences remain separate. Auto-mode classifier denials during the source fix required a temporary manual-approval session; they are integration evidence, not model-quality evidence. No persistent broad approval rules were added.

Raw screens, launch/results and review responses are retained locally under ignored `.rebuild-artifacts/live-trial-2026-10-21/`. The original full rebuild validation remains in [fork-validation.json](../fork-validation.json). Native Windows, live-survey dismissal and Rust 1.88 execution remain unverified.
