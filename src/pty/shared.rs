//! Portable PTY orchestration shared by the Unix poll loop (`super`) and the
//! Windows ConPTY proxy (`super::win`).
//!
//! These were originally methods on the Unix `Proxy`. They were lifted to free
//! functions taking explicit parameters (every `self.X` became an argument) so
//! the Windows proxy can drive the exact same delivery-thread startup, approval
//! publishing, screen-state refresh, title escaping, and launch-failure
//! finalization. The bodies are byte-for-byte the Unix originals apart from the
//! `self.X` → parameter substitution; the Unix correctness rests on that.

use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::config::Config;
use crate::db::HcomDb;
use crate::delivery::{
    APPROVAL_RESPONSE_GRACE_MS, APPROVAL_SCRAPE_CLEAR_MS, DeliveryState, ScreenState, ToolConfig,
    latch_scraped_approval, run_delivery_loop,
};
use crate::log::{log_error, log_info, log_warn};
use crate::notify::NotifyServer;
use crate::shared::{ST_BLOCKED, ST_LISTENING};
use crate::tool::Tool;

use super::PtyTarget;
use super::screen::ScreenTracker;

/// User-activity cooldown applied uniformly across tools (0.5s). Dim detection
/// enables this for Claude.
pub(super) const USER_ACTIVITY_COOLDOWN_MS: u64 = 500;

fn approval_response_grace_active(answered_at: Option<Instant>) -> bool {
    answered_at.is_some_and(|answered_at| {
        answered_at.elapsed() < Duration::from_millis(APPROVAL_RESPONSE_GRACE_MS)
    })
}

/// Update shared delivery state from screen tracker.
///
/// `publish` is the caller's approval-status publisher (it owns the
/// `instance_name`/`current_status` plumbing); it is invoked on the approval
/// edge exactly as the Unix proxy invoked `self.publish_approval_status`.
pub(super) fn update_delivery_state(
    screen_state: &Arc<RwLock<ScreenState>>,
    screen: &ScreenTracker,
    target: &PtyTarget,
    launch_phase_active: &Arc<AtomicBool>,
    publish: &dyn Fn(bool),
) {
    let mut approval_changed = None;
    let mut claude_denial_followup = false;
    if let Ok(mut state) = screen_state.write() {
        state.ready = screen.is_ready();
        // Cursor and Codex can briefly erase their approval surfaces during
        // redraws (Codex does this on focus changes). Latch positive detection
        // until output settles so a partial frame cannot clear blocked status.
        let scrape_latched_tool = matches!(target.known_tool(), Some(Tool::Codex | Tool::Cursor));
        let scraped_approval = match target.known_tool() {
            Some(Tool::Codex) => screen.is_waiting_approval() || screen.is_codex_approval_visible(),
            Some(Tool::Cursor) => screen.is_cursor_approval_visible(),
            _ => false,
        };
        state.approval_scrape_latched = latch_scraped_approval(
            state.approval_scrape_latched,
            scraped_approval,
            screen.is_output_stable(APPROVAL_SCRAPE_CLEAR_MS),
        );
        let antigravity_response_grace = target.name() == "antigravity"
            && approval_response_grace_active(state.approval_response_at);
        // Antigravity's feedback survey (`is_antigravity_survey_visible`) is
        // a non-task UX prompt handled by the dismissal policy in
        // `run_survey_dismissal` — it must never flow into this `approval`
        // flag / `pty:approval`, which is reserved for permission prompts
        // (hcom-f6g.9).
        let approval = (scrape_latched_tool && state.approval_scrape_latched)
            || (target.name() == "antigravity"
                && !antigravity_response_grace
                && screen.is_antigravity_approval_visible());
        if approval != state.approval {
            approval_changed = Some(approval);
        }
        state.approval = approval;
        let input_text = screen.get_input_box_text(target.name());
        let new_prompt_empty = input_text.as_ref().is_some_and(|t| t.is_empty());
        // Stamp submit-edge cooldown when input transitions from a known
        // non-empty value to empty or briefly undetected. Guards against
        // the race where the delivery gate sees `prompt_empty + listening`
        // in the gap before the tool's UserPromptSubmit hook flips status
        // to active. Requiring a previously-known non-empty input avoids
        // stamping on the initial false->true edge at startup.
        if super::prompt_submit_observed(state.input_text.as_deref(), input_text.as_deref()) {
            state.last_prompt_submit = Some(Instant::now());
        }
        state.prompt_empty = new_prompt_empty;
        state.input_text = input_text;
        // Claude only: defer injection while a nav overlay is focused (subagent
        // navigator or `←` session switcher) — the wake trigger would land in the
        // overlay's shared-PTY input box, not the session's root prompt.
        state.nav_overlay = matches!(target.known_tool(), Some(Tool::Claude))
            && (screen.is_claude_subagent_nav_visible()
                || screen.is_claude_session_switcher_visible());
        // Claude Code v2.1.252 can omit PermissionDenied for the interactive
        // "No" choice. Its settled denial follow-up is a narrow, observable
        // falling edge for the hook-owned approval block.
        claude_denial_followup = matches!(target.known_tool(), Some(Tool::Claude))
            && screen.is_claude_denial_followup_visible()
            && screen.is_output_stable(APPROVAL_SCRAPE_CLEAR_MS);
        // visible_tail is only consumed by the launch-blocked heuristic;
        // skip the screen walk + allocation once launch phase is over.
        state.visible_tail = if launch_phase_active.load(Ordering::Acquire) {
            screen.visible_tail(10, 1_000)
        } else {
            None
        };
        state.last_output = screen.last_output_instant();
        state.cols = screen.cols();
    }

    if let Some(approval) = approval_changed {
        publish(approval);
    }
    if claude_denial_followup {
        publish(false);
    }
}

/// Clear a pending approval answered by an injected keystroke.
///
/// Only acts when approval is currently showing, so routine message
/// injection (approval already false) is a no-op and never falsely stamps
/// user-active state. Cursor's approval is authoritative-by-prompt — it
/// clears only when the prompt leaves the screen — so it is excluded here,
/// matching the interactive stdin handler.
///
/// Returns `true` when the approval was cleared; the caller is then responsible
/// for clearing the tracker's approval (`screen.clear_approval()` on Unix, an
/// atomic request consumed by the reader thread on Windows).
pub(super) fn clear_injected_approval_state(
    target: &PtyTarget,
    screen_state: &Arc<RwLock<ScreenState>>,
    publish: &dyn Fn(bool),
) -> bool {
    if target.name() == "cursor" {
        return false;
    }
    let approval_cleared = match screen_state.write() {
        Ok(mut state) if state.approval => {
            state.approval = false;
            state.approval_response_at = Some(Instant::now());
            true
        }
        _ => false,
    };
    if approval_cleared {
        publish(false);
        return true;
    }
    false
}

/// Record a genuine user keystroke against the shared delivery state.
///
/// Genuine keystrokes answering a title-detected approval clear it immediately.
/// Cursor's approval is screen-scraped and authoritative-by-prompt, so it clears
/// only when the prompt actually leaves the screen.
///
/// Returns `true` only when a standing approval was actually cleared — matching
/// `clear_injected_approval_state`. The Unix caller ignores this and clears its
/// tracker inline on every non-cursor keystroke; the Windows caller uses it to
/// gate the tracker-clear atomic it consumes, so a keystroke with no approval
/// showing does not wipe the OSC scrape buffer (`output_buffer`) and lose an
/// approval edge that arrives in the same window.
pub(super) fn note_user_keystroke(
    target: &PtyTarget,
    screen_state: &Arc<RwLock<ScreenState>>,
    publish: &dyn Fn(bool),
) -> bool {
    let cursor_scrape = target.name() == "cursor";
    let mut approval_cleared = false;
    if let Ok(mut state) = screen_state.write() {
        state.last_user_input = Instant::now();
        if !cursor_scrape {
            approval_cleared = state.approval;
            state.approval = false;
            if approval_cleared {
                state.approval_response_at = Some(Instant::now());
            }
        }
    }
    if approval_cleared {
        publish(false);
    }
    approval_cleared
}

// ---- Antigravity feedback survey (hcom-f6g.9) ----

/// Keystroke that dismisses the Antigravity feedback survey: `0` selects the
/// `[0] Skip` option. Deliberately no Enter/newline — if detection ever raced
/// a live prompt, a stray `0` is inert text while an Enter could submit the
/// user's in-progress input. Skipping answers no permission question and
/// changes no tool setting.
pub(super) const ANTIGRAVITY_SURVEY_SKIP_KEY: &[u8] = b"0";

/// Dismissal attempts before the typed non-task blocker is published.
pub(super) const SURVEY_DISMISS_MAX_ATTEMPTS: u32 = 3;

/// Cooldown between dismissal attempts: long enough for the TUI to consume
/// the key and redraw the survey away (or prove it did not). Also the wait
/// before the blocker may publish after the final attempt, and the retry
/// cadence for a failed/deferred blocker publication.
pub(super) const SURVEY_DISMISS_RETRY_COOLDOWN: Duration = Duration::from_millis(2500);

/// How long the survey may vanish before the sighting is considered over.
/// Redraw flicker hides the survey for at most a frame or two; within this
/// window the sighting's age and attempt budget survive, so flicker cannot
/// reset the budget and starve escalation (hcom-f6g.9 review fix).
pub(super) const SURVEY_REAPPEAR_GRACE: Duration = Duration::from_millis(2000);

/// Wall-clock ceiling on a continuous sighting. A survey whose output never
/// settles (an animated/spinner variant) can never pass the stability gate,
/// so after this duration the controller escalates straight to the actionable
/// blocker. Skip injection stays stability-gated either way.
pub(super) const SURVEY_SIGHTING_MAX: Duration = Duration::from_secs(30);

/// While a sighting is active (or possibly mid-flicker), the Unix poll loop
/// caps its timeout to this so stability windows, retries, and blocker
/// publication fire on time instead of after the 10s idle poll.
pub(super) const SURVEY_POLL_CAP_MS: u16 = 500;

/// Per-sighting state for the Antigravity feedback-survey dismissal policy.
#[derive(Default)]
pub(super) struct SurveyDismissal {
    attempts: u32,
    next_attempt_at: Option<Instant>,
    /// First tick of the current sighting; anchors the wall-clock ceiling.
    first_seen_at: Option<Instant>,
    /// Last tick that saw the survey; anchors the reappearance grace and the
    /// poll-cap hint.
    last_seen_at: Option<Instant>,
    /// True once a blocker publication succeeded. Latched only by
    /// `run_survey_dismissal` on a successful `publish_survey_status` —
    /// never by the pure tick — so a failed or deferred publication retries.
    blocker_published: bool,
}

impl SurveyDismissal {
    /// True while a sighting is active or possibly mid-flicker — the Unix
    /// poll loop caps its timeout so dismissal timing stays tight.
    pub(super) fn caps_poll(&self) -> bool {
        self.last_seen_at
            .is_some_and(|last| last.elapsed() < SURVEY_REAPPEAR_GRACE)
    }

    /// Push the next blocker-publication retry out by one cooldown after a
    /// failed/deferred publish, so retries do not hot-loop.
    fn defer_block_retry(&mut self, now: Instant) {
        self.next_attempt_at = Some(now + SURVEY_DISMISS_RETRY_COOLDOWN);
    }
}

/// Action for the PTY loop after a [`survey_dismissal_tick`].
#[derive(Debug, PartialEq, Eq)]
pub(super) enum SurveyAction {
    /// Inject [`ANTIGRAVITY_SURVEY_SKIP_KEY`] now.
    Dismiss,
    /// Publish the typed non-task blocker. Emitted on every qualifying tick
    /// until the caller latches a successful publication — a deferred or
    /// failed publish must retry.
    Block,
    /// The sighting ended with a published blocker: clear it. Re-emitted
    /// until the caller observes a successful clear.
    Unblock,
    /// Nothing to do.
    None,
}

/// Pure dismissal-policy decision for one PTY-loop tick.
///
/// Deterministic given (sighting history, visibility, output stability, now)
/// so the Unix poll loop and the Windows reader thread behave identically.
/// Blocker publication state is owned by the caller: the tick emits
/// [`SurveyAction::Block`] / [`SurveyAction::Unblock`] and only the caller's
/// successful `publish_survey_status` latches `blocker_published`, so a
/// failed or deferred publication (e.g. another blocker owning the instance
/// row) retries on later ticks.
pub(super) fn survey_dismissal_tick(
    state: &mut SurveyDismissal,
    survey_visible: bool,
    output_stable: bool,
    now: Instant,
) -> SurveyAction {
    if survey_visible {
        let first_seen = *state.first_seen_at.get_or_insert(now);
        state.last_seen_at = Some(now);

        // Bounded wall-clock escalation: an animated survey whose output
        // never settles can never pass the stability gate below, but it must
        // not stall unattended waits forever — at the sighting ceiling the
        // actionable blocker fires even while output churns. Skip injection
        // itself stays stability-gated.
        if now.saturating_duration_since(first_seen) >= SURVEY_SIGHTING_MAX
            && !state.blocker_published
            && state.next_attempt_at.is_none_or(|at| now >= at)
        {
            return SurveyAction::Block;
        }

        // Only act on a settled frame: a partially drawn (or mid-redraw)
        // screen must not receive the dismissal key.
        if !output_stable {
            return SurveyAction::None;
        }

        if state.attempts >= SURVEY_DISMISS_MAX_ATTEMPTS {
            // The final Skip attempt gets its full cooldown before the
            // blocker may publish — the tool may still be mid-redraw
            // consuming the key (hcom-f6g.9 review fix).
            if state.next_attempt_at.is_some_and(|at| now < at) {
                return SurveyAction::None;
            }
            if !state.blocker_published {
                return SurveyAction::Block;
            }
            return SurveyAction::None;
        }

        if state.next_attempt_at.is_some_and(|at| now < at) {
            return SurveyAction::None;
        }
        state.attempts += 1;
        state.next_attempt_at = Some(now + SURVEY_DISMISS_RETRY_COOLDOWN);
        return SurveyAction::Dismiss;
    }

    // Not visible. A disappearance shorter than the reappearance grace is
    // redraw flicker: the sighting (age, attempts, published blocker)
    // survives it, so flicker cannot reset the budget and starve escalation.
    // `next_attempt_at` also survives a reset sighting end, preventing an
    // immediate second Skip key on the next sighting.
    let sighting_over = state
        .last_seen_at
        .is_some_and(|last| now.saturating_duration_since(last) >= SURVEY_REAPPEAR_GRACE);
    if !sighting_over {
        return SurveyAction::None;
    }
    // Sighting over: reset the budget for the next survey. A published
    // blocker is unlatched by the caller on a successful clear; until then
    // keep re-emitting Unblock so a failed clear retries.
    state.first_seen_at = None;
    state.attempts = 0;
    if state.blocker_published {
        return SurveyAction::Unblock;
    }
    SurveyAction::None
}

/// Drive the Antigravity feedback-survey policy for one PTY-loop tick.
///
/// Shared by the Unix poll loop and the Windows reader thread; only the
/// keystroke write differs (`inject`). Non-Antigravity targets are a no-op.
/// The survey is a non-task UX prompt, not a permission approval: dismissal
/// sends only the Skip key, publishes nothing while it succeeds, and never
/// touches the approval path. If dismissal repeatedly fails,
/// [`publish_survey_status`] converts the silent stall into an actionable
/// typed blocker.
pub(super) fn run_survey_dismissal(
    screen: &ScreenTracker,
    survey: &mut SurveyDismissal,
    target: &PtyTarget,
    instance_name_cfg: Option<&str>,
    current_status: &Arc<RwLock<String>>,
    inject: impl FnOnce(&[u8]) -> Result<()>,
) {
    if !matches!(target.known_tool(), Some(Tool::Antigravity)) {
        return;
    }
    match survey_dismissal_tick(
        survey,
        screen.is_antigravity_survey_visible(),
        screen.is_output_stable(APPROVAL_SCRAPE_CLEAR_MS),
        Instant::now(),
    ) {
        SurveyAction::Dismiss => {
            if let Err(error) = inject(ANTIGRAVITY_SURVEY_SKIP_KEY) {
                // The failed write still consumed the attempt; retries and,
                // finally, the blocker bound the damage. Never fail the
                // session over the survey.
                log_warn(
                    "native",
                    "pty.survey_dismiss_failed",
                    &format!("Failed to inject Antigravity survey Skip key: {error}"),
                );
            } else {
                log_info(
                    "native",
                    "pty.survey_dismiss",
                    "Antigravity feedback survey detected; injected Skip (0)",
                );
            }
        }
        SurveyAction::Block => {
            // Latch only on a successful publication. A deferred or failed
            // publish (another blocker owns the row, DB hiccup, instance not
            // yet bound) must retry on later ticks — including after the
            // other blocker clears — so push the retry out by one cooldown.
            if publish_survey_status(true, instance_name_cfg, current_status) {
                survey.blocker_published = true;
                log_warn(
                    "native",
                    "pty.survey_block",
                    "Antigravity feedback survey did not dismiss after repeated Skip \
                     attempts; published pty:survey blocker",
                );
            } else {
                survey.defer_block_retry(Instant::now());
                log_warn(
                    "native",
                    "pty.survey_block_deferred",
                    "pty:survey blocker publication deferred (another blocker owns the \
                     instance row, the instance is not bound yet, or the DB is \
                     unavailable); will retry",
                );
            }
        }
        SurveyAction::Unblock => {
            // A failed clear keeps the latch so the next quiet tick re-emits
            // Unblock and retries the clear.
            if publish_survey_status(false, instance_name_cfg, current_status) {
                survey.blocker_published = false;
            } else {
                log_warn(
                    "native",
                    "pty.survey_unblock_failed",
                    "Failed to clear the pty:survey blocker; will retry",
                );
            }
        }
        SurveyAction::None => {}
    }
}

/// Publish the Antigravity feedback-survey blocker edge (hcom-f6g.9).
///
/// Fires only when auto-dismissal failed repeatedly, converting a silent
/// stall behind the survey into an actionable typed non-task blocker
/// (`pty:survey`) that `hcom list` and result waits can surface. The survey
/// is not a permission approval: this path never uses the `pty:approval`
/// context and never overwrites a hook- or approval-owned block. Clearing
/// releases only rows this path blocked.
///
/// Returns `true` when, after the call, this path owns the published state:
/// the rising edge actually wrote `pty:survey` (or the row already carried
/// it), or the falling edge actually cleared it (or there was nothing of
/// ours to clear). Returns `false` when publication did not happen — DB
/// failure, unknown instance, or another blocker owning the row — so the
/// caller must retry rather than latch. A session with no instance name has
/// no channel to publish on and can never succeed, so it counts as handled.
pub(super) fn publish_survey_status(
    blocked: bool,
    instance_name_cfg: Option<&str>,
    current_status: &Arc<RwLock<String>>,
) -> bool {
    let Ok(db) = HcomDb::open() else {
        log_warn(
            "native",
            "pty.survey_status_open_failed",
            "Failed to open database for PTY survey status",
        );
        return false;
    };

    let config = Config::get();
    let instance_name = config
        .process_id
        .as_deref()
        .and_then(|process_id| db.get_process_binding(process_id).ok().flatten())
        .or_else(|| instance_name_cfg.map(str::to_string))
        .or(config.instance_name);
    let Some(instance_name) = instance_name.filter(|name| !name.is_empty()) else {
        return true;
    };

    let current = match db.get_instance_full(&instance_name) {
        Ok(row) => row,
        Err(error) => {
            log_warn(
                "native",
                "pty.survey_status_failed",
                &format!(
                    "Failed to read status for survey blocked={} on {}: {}",
                    blocked, instance_name, error
                ),
            );
            return false;
        }
    };
    let Some(current) = current else {
        // Instance not bound yet (launch race); it may appear — retry.
        return false;
    };
    let survey_blocked = current.status == ST_BLOCKED && current.status_context == "pty:survey";

    if blocked {
        if survey_blocked {
            // Already ours — idempotent success; keep the shared status
            // consistent for `hcom list`.
            if let Ok(mut shared_status) = current_status.write() {
                *shared_status = ST_BLOCKED.to_string();
            }
            return true;
        }
        if current.status == ST_BLOCKED {
            // Another blocker (hook approval, elicitation, …) owns the row
            // and outranks the survey: defer, and let the caller retry once
            // it clears.
            return false;
        }
    } else if !survey_blocked {
        // No survey block of ours to clear.
        return true;
    }

    let (status, context) = if blocked {
        (ST_BLOCKED, "pty:survey")
    } else {
        (ST_LISTENING, "pty:survey_cleared")
    };
    if let Err(error) = db.set_status(&instance_name, status, context) {
        log_warn(
            "native",
            "pty.survey_status_failed",
            &format!(
                "Failed to publish survey blocked={} for {}: {}",
                blocked, instance_name, error
            ),
        );
        return false;
    }

    let mut data = serde_json::json!({
        "status": status,
        "context": context,
        "position": current.last_event_id,
    });
    if blocked {
        data["detail"] = serde_json::json!(
            "Antigravity feedback survey did not dismiss after repeated Skip attempts; \
             answer it (0 = Skip) to resume the worker"
        );
    }
    if let Err(error) = db.log_event("status", &instance_name, &data) {
        // The row write succeeded, so ownership stands; only the event is
        // missing.
        log_warn(
            "native",
            "pty.survey_status_event_failed",
            &format!(
                "Failed to emit survey status event ({}) for {}: {}",
                context, instance_name, error
            ),
        );
    }

    if let Ok(mut shared_status) = current_status.write() {
        *shared_status = status.to_string();
    }
    true
}

/// Publish PTY approval edges independently of the delivery queue.
///
/// Approval is agent state: `hcom list` must report it even when no message
/// is pending. Clearing is guarded by the PTY-owned context so lifecycle
/// hooks that already moved the agent to active are never overwritten.
pub(super) fn publish_approval_status(
    approval: bool,
    instance_name_cfg: Option<&str>,
    current_status: &Arc<RwLock<String>>,
) {
    let Ok(db) = HcomDb::open() else {
        log_warn(
            "native",
            "pty.approval_status_open_failed",
            "Failed to open database for PTY approval status",
        );
        return;
    };

    let config = Config::get();
    let instance_name = config
        .process_id
        .as_deref()
        .and_then(|process_id| db.get_process_binding(process_id).ok().flatten())
        .or_else(|| instance_name_cfg.map(str::to_string))
        .or(config.instance_name);
    let Some(instance_name) = instance_name.filter(|name| !name.is_empty()) else {
        return;
    };

    let current = match db.get_instance_full(&instance_name) {
        Ok(row) => row,
        Err(error) => {
            log_warn(
                "native",
                "pty.approval_status_failed",
                &format!(
                    "Failed to read status for approval={} on {}: {}",
                    approval, instance_name, error
                ),
            );
            return;
        }
    };
    let pty_blocked = current
        .as_ref()
        .is_some_and(|row| row.status == ST_BLOCKED && row.status_context == "pty:approval");
    let hook_blocked = current
        .as_ref()
        .is_some_and(|row| row.status == ST_BLOCKED && row.status_context == "approval");
    let already_blocked = pty_blocked || hook_blocked;

    // Resolve the approval edge to publish: block on the rising edge, release
    // on the falling edge, and stay silent when the row already matches.
    let edge = if approval {
        (!already_blocked).then_some((ST_BLOCKED, "pty:approval"))
    } else {
        pty_blocked
            .then_some((ST_LISTENING, "pty:approval_cleared"))
            .or_else(|| hook_blocked.then_some((ST_LISTENING, "denied:screen")))
    };
    let Some((status, context)) = edge else {
        // No transition to publish. Still reflect a standing block in the
        // PTY-owned shared status so `hcom list` stays consistent.
        if already_blocked && let Ok(mut shared_status) = current_status.write() {
            *shared_status = ST_BLOCKED.to_string();
        }
        return;
    };

    // Write the instance row, then log a paired status event. The bare
    // `set_status` leaves `status_detail` (the gated-command preview) intact,
    // while the explicit event keeps the block/release visible to the events
    // table, `events sub`, and the TUI — mirroring how the sibling
    // launch_blocked path pairs a row write with its own emitted event.
    // Without the event, the row updates silently and event consumers never
    // see the approval gate (Codex's only PTY-driven block path).
    if let Err(error) = db.set_status(&instance_name, status, context) {
        log_warn(
            "native",
            "pty.approval_status_failed",
            &format!(
                "Failed to publish approval={} for {}: {}",
                approval, instance_name, error
            ),
        );
        return;
    }

    let position = current.as_ref().map(|row| row.last_event_id).unwrap_or(0);
    let detail = current
        .as_ref()
        .map(|row| row.status_detail.as_str())
        .unwrap_or("");
    let mut data = serde_json::json!({
        "status": status,
        "context": context,
        "position": position,
    });
    if !detail.is_empty() {
        data["detail"] = serde_json::json!(detail);
    }
    if let Err(error) = db.log_event("status", &instance_name, &data) {
        log_warn(
            "native",
            "pty.approval_status_event_failed",
            &format!(
                "Failed to emit approval status event ({}) for {}: {}",
                context, instance_name, error
            ),
        );
    }

    if let Ok(mut shared_status) = current_status.write() {
        *shared_status = status.to_string();
    }
}

/// Outcome of [`start_delivery_thread`].
///
/// `Result::Err` (distinct from every variant here) is an up-front init failure:
/// the spawned thread already returned, so the attempt is safely **retryable**.
pub(super) enum DeliveryStart {
    /// Init succeeded (DB opened, notify server created). Join the handle at
    /// shutdown.
    Started(JoinHandle<()>),
    /// No instance name (delivery disabled). No thread was spawned.
    Disabled,
    /// The thread was spawned but its init result was not observed within the
    /// timeout (or the init channel disconnected). It is detached and may still
    /// be initializing, so it MUST be joined at shutdown and MUST NOT be retried
    /// — a retry would spawn a *second* delivery thread alongside it (there is no
    /// singleton guard in `run_delivery_loop`) and double-deliver.
    Pending(JoinHandle<()>, mpsc::Receiver<Result<()>>),
}

/// Start the delivery thread (and transcript watcher for Codex).
///
/// Returns [`DeliveryStart::Started`] when the delivery thread initialized
/// successfully (DB opened, notify server created), [`DeliveryStart::Disabled`]
/// when there is no instance name, and [`DeliveryStart::Pending`] when the thread
/// was spawned but its init result timed out / the channel disconnected (the
/// thread is detached and still live; non-retryable). Returns `Err` only on an
/// up-front init failure, where no thread is left running — that case is
/// retryable and the caller maps it to a launch failure.
#[allow(clippy::too_many_arguments)]
pub(super) fn start_delivery_thread(
    instance_name_cfg: Option<&str>,
    running: Arc<AtomicBool>,
    delivery_state: Arc<RwLock<ScreenState>>,
    launch_phase_active: Arc<AtomicBool>,
    inject_port: u16,
    target: PtyTarget,
    notify_port: Arc<AtomicU16>,
    current_name: Arc<RwLock<String>>,
    current_status: Arc<RwLock<String>>,
    title_wake: Option<crate::delivery::TitleWake>,
) -> Result<DeliveryStart> {
    let instance_name = match instance_name_cfg {
        Some(name) => name.to_string(),
        None => {
            // Try to get from environment (fallback for testing without explicit config)
            Config::get().instance_name.unwrap_or_default()
        }
    };

    if instance_name.is_empty() {
        // No instance name - skip delivery (hybrid mode or testing)
        crate::log::log_warn(
            "native",
            "delivery.skip.no_instance_name",
            "No instance name - delivery disabled. Set config.instance_name or HCOM_INSTANCE_NAME env var.",
        );
        return Ok(DeliveryStart::Disabled);
    }

    // Create oneshot channel for init result
    let (init_tx, init_rx) = mpsc::channel();

    let handle = std::thread::spawn(move || {
        log_info(
            "native",
            "delivery.start",
            &format!("Starting delivery thread for {}", instance_name),
        );

        // Initialize delivery components with dependency injection
        let (mut db, notify) = match super::initialize_delivery_components(
            &instance_name,
            HcomDb::open,
            NotifyServer::new,
        ) {
            Ok((db, notify)) => {
                log_info(
                    "native",
                    "delivery.init.success",
                    &format!("Initialized delivery for {}", instance_name),
                );
                // Store port for shutdown wakeup
                notify_port.store(notify.port(), Ordering::Release);
                log_info(
                    "native",
                    "notify.registered",
                    &format!("Registered notify port {}", notify.port()),
                );
                // Register inject port for screen queries
                if let Err(e) = db.register_inject_port(&instance_name, inject_port) {
                    log_warn(
                        "native",
                        "inject.register_fail",
                        &format!("Failed to register inject port: {}", e),
                    );
                }

                // Signal successful initialization to parent
                let _ = init_tx.send(Ok(()));

                // For Codex: spawn the transcript watcher only after delivery
                // init has succeeded (#5). Spawning it before init meant a failed
                // or timed-out init still left an orphan watcher running against a
                // session that never came up. Init success is reached exactly once
                // per live delivery thread, so the watcher starts exactly once.
                if matches!(target.known_tool(), Some(Tool::Codex)) {
                    let watcher_running = running.clone();
                    let watcher_name = instance_name.clone();
                    std::thread::spawn(move || {
                        crate::hooks::codex_file_edits::run_transcript_watcher(
                            watcher_running,
                            watcher_name,
                            Duration::from_secs(5),
                        );
                    });
                }
                (db, notify)
            }
            Err(e) => {
                log_error(
                    "native",
                    "delivery.init.fail",
                    &format!("Failed to initialize delivery: {}", e),
                );
                let _ = init_tx.send(Err(e));
                return;
            }
        };

        // Create delivery state wrapper
        let state = DeliveryState {
            screen: delivery_state,
            launch_phase_active,
            inject_port,
            user_activity_cooldown_ms: USER_ACTIVITY_COOLDOWN_MS,
        };

        // Get tool config
        let config = ToolConfig::for_tool(target.delivery_tool());

        // Run delivery loop (pass shared state for main loop's OSC override)
        run_delivery_loop(
            running,
            &mut db,
            &notify,
            &state,
            &instance_name,
            &config,
            Some(current_name),
            Some(current_status),
            title_wake,
        );

        log_info(
            "native",
            "delivery.stop",
            &format!("Delivery thread stopped for {}", instance_name),
        );
    });

    // Wait for initialization result (with timeout to avoid blocking forever)
    match init_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => {
            log_info(
                "native",
                "delivery.init.success",
                "Delivery thread initialized successfully",
            );
            Ok(DeliveryStart::Started(handle))
        }
        Ok(Err(e)) => {
            log_error(
                "native",
                "delivery.init.fail",
                &format!("Delivery thread init failed: {}", e),
            );
            Err(e)
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            log_error(
                "native",
                "delivery.init.timeout",
                "Delivery thread init timed out after 5s",
            );
            // Detached thread is still running; keep the handle so shutdown can
            // join it, and flag as non-retryable (Pending).
            Ok(DeliveryStart::Pending(handle, init_rx))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            log_error(
                "native",
                "delivery.init.disconnect",
                "Delivery thread init channel disconnected",
            );
            // Sender dropped without sending: thread returned/panicked, possibly
            // after partial registration. Non-retryable like a timeout; keep the
            // handle so shutdown can join it.
            Ok(DeliveryStart::Pending(handle, init_rx))
        }
    }
}

/// Decide whether the delivery coordinator should start delivery this tick.
///
/// Pure helper so the decision is host-testable (the coordinator itself needs a
/// live ConPTY). Start once the tool is ready, once the start-timeout has
/// elapsed, or when we are shutting down.
///
/// The shutdown trigger only fires after the child has already exited — `run()`
/// flips `running` false only once the reader has hit EOF and been joined — so
/// there is no live child left to inject into and the delivery loop's `while
/// running` body runs zero iterations (its `register_notify_port` never runs).
/// This final start therefore exists so init-time registration (DB open, notify
/// server, inject-port registration) and the loop's post-loop cleanup get a
/// chance to run and settle the final DB status, not to hand an in-flight `hcom
/// deliver` a live consumer.
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn should_start_delivery(
    ready: bool,
    elapsed: Duration,
    timeout: Duration,
    shutting_down: bool,
) -> bool {
    ready || elapsed > timeout || shutting_down
}

/// Leading-edge throttle for the `hcom term` screen snapshot the reader renders.
///
/// Rendering `get_screen_dump` costs ~150µs on a wide screen, so refreshing it on
/// every ConPTY chunk would burn measurable CPU (and steal reader time from
/// draining the pipe) under heavy output. Cap it at one render per `throttle`;
/// any chunk skipped here is marked dirty and picked up by a single trailing-edge
/// refresh once output goes quiet (see the reader loop's debounce). Pure so the
/// decision is host-testable.
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn should_refresh_snapshot(since_last_snapshot: Duration, throttle: Duration) -> bool {
    since_last_snapshot >= throttle
}

/// Finalize a launch failure once the child has exited before binding.
///
/// `tail` is the screen's visible tail (the Unix caller passes
/// `self.screen.visible_tail(8, 1000)`); kept as a parameter so the function
/// stays free of any tracker reference.
pub(super) fn finalize_launch_failure_after_exit(
    instance_name_cfg: Option<&str>,
    tail: Option<&str>,
    launch_phase_active: &Arc<AtomicBool>,
    elapsed: Duration,
    exit_code: i32,
) {
    let Some(instance_name) = instance_name_cfg else {
        return;
    };

    let Ok(db) = HcomDb::open() else {
        return;
    };
    let Ok(Some(instance)) = db.get_instance_full(instance_name) else {
        return;
    };

    if instance.session_id.is_some()
        || instance.status_context != "new"
        || (instance.status != crate::shared::ST_INACTIVE && instance.status != "pending")
    {
        return;
    }

    let elapsed_secs = elapsed.as_secs();
    let mut fallback =
        format!("exited {elapsed_secs}s after spawn before binding (exit code {exit_code})");
    if let Some(tail) = tail {
        fallback.push_str("\nPTY output:\n");
        fallback.push_str(tail);
    }
    let Some(detail) =
        crate::instance_lifecycle::finalize_launch_failure_detail(&db, &instance, Some(&fallback))
    else {
        return;
    };
    let _ = db.emit_launch_failed_event(
        instance_name,
        crate::shared::ST_INACTIVE,
        "launch_failed",
        "exited_before_bind",
        &detail,
    );
    launch_phase_active.store(false, Ordering::Release);

    if let Ok(process_id) = std::env::var("HCOM_PROCESS_ID")
        && !process_id.is_empty()
    {
        let _ = db.delete_process_binding(&process_id);
    }
}

/// Build the OSC 1/2 title-set escape for `name`/`status` under `tool_name`.
///
/// - [`TitleMode::Label`] → `◉ luna [claude]` (hcom's status label only).
/// - [`TitleMode::Combined`] → `◉ luna - ⠋ Working` — hcom's `{icon} name` plus
///   the wrapped tool's live title after ` - ` (dropping the `[tool]` tag). The
///   child text is already sanitized upstream by `ScreenTracker` (control/escape
///   bytes stripped, whitespace collapsed, length bounded) so it cannot break
///   out of the OSC we wrap it in.
///
/// [`TitleMode::Off`] never reaches here — the caller skips writing entirely and
/// lets the tool's own title pass through — so it falls back to the label.
pub(super) fn build_title_escape(
    name: &str,
    status: &str,
    tool_name: &str,
    mode: crate::shared::TitleMode,
    child_title: Option<&str>,
) -> String {
    let title = match mode {
        crate::shared::TitleMode::Combined => {
            crate::shared::format_pane_title_combined(status, name, child_title)
        }
        _ => crate::shared::format_pane_title(status, name, tool_name),
    };
    format!("\x1b]1;{}\x07\x1b]2;{}\x07", title, title)
}

/// Build minimal launch_context JSON from env vars available in the PTY process.
/// Captures process_id and late-bound terminal metadata needed by kill.
/// The start hook captures the full context (git_branch, tty, env snapshot) later.
///
/// Portable: every operation here (`env::var`, `fs::read_to_string`, `thread::sleep`)
/// works identically on Windows. `TMUX_PANE`/`ZELLIJ_PANE_ID`/`KITTY_WINDOW_ID` simply
/// won't be set there — kitty and the multiplexers have no native Windows build — so
/// those fields degrade to absent, same as on Unix outside of tmux/zellij/kitty.
/// `WEZTERM_PANE` is meaningful on Windows too, since WezTerm is natively cross-platform.
pub(super) fn build_early_launch_context() -> String {
    use serde_json::{Map, Value};

    let mut ctx = Map::new();

    if let Ok(pid) = std::env::var("HCOM_PROCESS_ID")
        && !pid.is_empty()
    {
        ctx.insert("process_id".into(), Value::String(pid));
    }

    // Kitty socket path for close-on-kill (needed when launching from outside kitty)
    if let Ok(listen) = std::env::var("KITTY_LISTEN_ON")
        && !listen.is_empty()
    {
        ctx.insert("kitty_listen_on".into(), Value::String(listen));
    }

    // A selected preset defines the pane-ID namespace. Never pair its close
    // command with an ID inherited from another backend.
    let launched_preset = std::env::var("HCOM_LAUNCHED_PRESET")
        .ok()
        .filter(|preset| !preset.is_empty());
    if let Some(preset) = launched_preset.as_deref()
        && let Some(var) = crate::config::get_merged_preset_pane_id_env(preset)
        && let Ok(val) = std::env::var(&var)
        && !val.is_empty()
    {
        ctx.insert("pane_id".into(), Value::String(val));
    }

    // Legacy fallback for launches that predate effective-preset propagation.
    if launched_preset.is_none() {
        let pane_id_vars: &[&str] = &[
            "WEZTERM_PANE",
            "TMUX_PANE",
            "KITTY_WINDOW_ID",
            "ZELLIJ_PANE_ID",
        ];
        for &var in pane_id_vars {
            if let Ok(val) = std::env::var(var)
                && !val.is_empty()
            {
                ctx.insert("pane_id".into(), Value::String(val));
                break;
            }
        }
    }

    // Read terminal_id from temp file written by parent's launch stdout capture.
    // This is the ID returned by `kitten @ launch` (or similar) and serves as
    // fallback for pane_id when the terminal env var isn't available.
    //
    // Race condition: parent writes this file after `kitten @ launch` returns
    // (~500ms after child starts), but we run within ~10-100ms of spawn.
    // Retry with backoff only when pane_id not already captured from env vars
    // (tmux/wezterm set env vars directly, no file needed).
    if let Some(process_id) = ctx.get("process_id").and_then(|v| v.as_str()) {
        let id_file = crate::paths::hcom_dir()
            .join(".tmp")
            .join("terminal_ids")
            .join(process_id);
        let needs_id = !ctx.contains_key("pane_id");
        let max_attempts: usize = if needs_id { 10 } else { 1 };
        let mut terminal_id_value = String::new();

        for attempt in 0..max_attempts {
            if let Ok(contents) = std::fs::read_to_string(&id_file) {
                let trimmed = contents.trim().to_string();
                if !trimmed.is_empty() {
                    terminal_id_value = trimmed;
                    break;
                }
            }
            if attempt + 1 < max_attempts {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }

        if !terminal_id_value.is_empty() {
            ctx.insert(
                "terminal_id".into(),
                Value::String(terminal_id_value.clone()),
            );
            if !ctx.contains_key("pane_id") {
                ctx.insert("pane_id".into(), Value::String(terminal_id_value));
            }
        }
        // Don't delete the file here — capture_context in the SessionStart hook
        // reads it to persist terminal_id into DB launch_context. If we delete
        // early, the hook finds exists=false and terminal_id is lost from DB.
    }

    Value::Object(ctx).to_string()
}

/// True when `seq` is the terminal's cursor-position query (`ESC[6n`, a DSR
/// with parameter 6). In headless mode there is no outer terminal to answer it,
/// so the reader must reply on the child's behalf or startup hangs (#1).
///
/// Deliberately narrow: only the bare `ESC[6n` query matches. A CPR *reply*
/// (`ESC[<r>;<c>R`), a private DSR (`ESC[?6n`), and a parameterless `ESC[n`
/// must not match — we only synthesize a reply to the child's own query.
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn csi_is_dsr_cpr(seq: &[u8]) -> bool {
    seq == b"\x1b[6n"
}

/// Rebuild a DEC private mode-set (`ESC[? … h|l`), dropping only the Win32-input
/// (`9001`) and focus-reporting (`1004`) parameters and keeping every other mode
/// (#15).
///
/// The previous whole-prefix match (`starts_with(b"\x1b[?9001")`) dropped or kept
/// the entire CSI, so `ESC[?9001;25h` lost mode 25 and `ESC[?25;9001h` leaked
/// 9001 to the outer terminal. Filtering per parameter fixes both. Returns an
/// empty `Vec` when every parameter was dropped (emit nothing).
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn filter_dec_private_modes(seq: &[u8]) -> Vec<u8> {
    let final_byte = *seq.last().unwrap();
    let params = &seq[3..seq.len() - 1];
    let kept: Vec<&[u8]> = params
        .split(|&b| b == b';')
        .filter(|p| *p != b"9001" && *p != b"1004")
        .collect();
    if kept.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(seq.len());
    out.extend_from_slice(b"\x1b[?");
    for (i, p) in kept.iter().enumerate() {
        if i > 0 {
            out.push(b';');
        }
        out.extend_from_slice(p);
    }
    out.push(final_byte);
    out
}

/// Rewrites the child's DEC private-mode **sets** so the *outer* terminal is
/// never switched into Win32 input mode (`?9001`) or focus reporting (`?1004`),
/// and notices the child's cursor-position query (`ESC[6n`) so a headless reader
/// can answer it.
///
/// A ConPTY wrapper sits between the child and the real terminal. If the child's
/// `ESC[?9001h` reaches the outer terminal, that terminal starts encoding its
/// input — including its automatic `ESC[6n` (cursor position) reply — as Win32
/// input records. The child, which only understands a plain `ESC[15;1R`, then
/// waits forever for a reply it can parse. Stripping those mode-sets keeps the
/// outer terminal in normal VT mode so the DSR reply round-trips correctly.
///
/// The parser is stateful so sequences split across reads are handled, and every
/// other byte (including all other escape sequences) passes through unchanged.
/// DSR queries are still passed through: in interactive mode the real terminal
/// must see them to answer; the headless reply is gated separately in win.rs.
///
/// Lives here (rather than in `win.rs`) so its correctness-critical parsing runs
/// under the host test gate; `win.rs` owns only the headless DSR-reply write.
#[cfg_attr(not(windows), allow(dead_code))]
#[derive(Default)]
pub(super) struct OutputModeFilter {
    state: FilterState,
    buf: Vec<u8>,
    dsr_seen: bool,
    pending_utf8: u8,
    /// When true (title_mode `off`), the tool's own OSC 0/1/2 titles are passed
    /// through to the terminal instead of stripped. DSR/ground-state tracking is
    /// unaffected. Default false preserves the strip-and-override behavior.
    passthrough_titles: bool,
}

#[derive(Default, PartialEq)]
enum FilterState {
    #[default]
    Ground,
    Esc,
    Csi,
    OscStart,
    OscDigit(u8),
    StringSeq {
        strip: bool,
        saw_esc: bool,
        discarded: usize,
    },
    SingleShift,
    Nf,
}

#[cfg_attr(not(windows), allow(dead_code))]
impl OutputModeFilter {
    /// Pass the tool's own OSC 0/1/2 titles through instead of stripping them
    /// (title_mode `off`). DSR/ground-state tracking is unaffected.
    pub(super) fn set_passthrough_titles(&mut self, passthrough: bool) {
        self.passthrough_titles = passthrough;
    }

    pub(super) fn filter(&mut self, input: &[u8], out: &mut Vec<u8>) {
        let output_start = out.len();
        for &b in input {
            match self.state {
                FilterState::Ground => {
                    if b == 0x1b {
                        self.buf.clear();
                        self.buf.push(b);
                        self.state = FilterState::Esc;
                    } else {
                        out.push(b);
                    }
                }
                FilterState::Esc => {
                    self.buf.push(b);
                    match b {
                        b'[' => self.state = FilterState::Csi,
                        b']' => self.state = FilterState::OscStart,
                        b'P' | b'^' | b'_' | b'X' => {
                            out.extend_from_slice(&self.buf);
                            self.buf.clear();
                            self.state = FilterState::StringSeq {
                                strip: false,
                                saw_esc: false,
                                discarded: 0,
                            };
                        }
                        b'N' | b'O' => {
                            out.extend_from_slice(&self.buf);
                            self.buf.clear();
                            self.state = FilterState::SingleShift;
                        }
                        0x20..=0x2f => {
                            out.extend_from_slice(&self.buf);
                            self.buf.clear();
                            self.state = FilterState::Nf;
                        }
                        _ => {
                            // A complete two-byte escape: pass through untouched.
                            out.extend_from_slice(&self.buf);
                            self.buf.clear();
                            self.state = FilterState::Ground;
                        }
                    }
                }
                FilterState::Csi => {
                    self.buf.push(b);
                    if (0x40..=0x7e).contains(&b) {
                        // Completed CSI. Notice the child's cursor-position query
                        // (still pass it through — interactive needs the real
                        // terminal to answer), and filter DEC private mode-sets
                        // per-parameter.
                        if csi_is_dsr_cpr(&self.buf) {
                            self.dsr_seen = true;
                        }
                        if self.buf.starts_with(b"\x1b[?") && matches!(b, b'h' | b'l') {
                            out.extend_from_slice(&filter_dec_private_modes(&self.buf));
                        } else {
                            out.extend_from_slice(&self.buf);
                        }
                        self.buf.clear();
                        self.state = FilterState::Ground;
                    } else if self.buf.len() > 32 {
                        // Malformed/overlong — give up filtering, emit as-is.
                        // Combined mode-sets are short, so this never trips on a
                        // legitimate `?9001`/`?1004` sequence.
                        out.extend_from_slice(&self.buf);
                        self.buf.clear();
                        self.state = FilterState::Ground;
                    }
                }
                FilterState::OscStart => {
                    self.buf.push(b);
                    if matches!(b, b'0' | b'1' | b'2') {
                        self.state = FilterState::OscDigit(b);
                    } else {
                        // Not a title OSC. Emit its prefix immediately and keep
                        // tracking until BEL/ST so title writes cannot split it.
                        out.extend_from_slice(&self.buf);
                        self.buf.clear();
                        self.state = FilterState::StringSeq {
                            strip: false,
                            saw_esc: b == 0x1b,
                            discarded: 0,
                        };
                        if b == 0x07 {
                            self.state = FilterState::Ground;
                        }
                    }
                }
                FilterState::OscDigit(_digit) => {
                    self.buf.push(b);
                    if b == b';' {
                        // Confirmed OSC 0/1/2: discard the complete title
                        // (including a terminator that may arrive in a later
                        // read) — unless title_mode `off`, where we pass the
                        // tool's own title through untouched.
                        if self.passthrough_titles {
                            out.extend_from_slice(&self.buf);
                        }
                        self.buf.clear();
                        self.state = FilterState::StringSeq {
                            strip: !self.passthrough_titles,
                            saw_esc: false,
                            discarded: 0,
                        };
                    } else {
                        // Multi-digit/non-title OSC. Preserve it while tracking
                        // its boundary for safe insertion of hcom's title.
                        out.extend_from_slice(&self.buf);
                        self.buf.clear();
                        self.state = FilterState::StringSeq {
                            strip: false,
                            saw_esc: b == 0x1b,
                            discarded: 0,
                        };
                        if b == 0x07 {
                            self.state = FilterState::Ground;
                        }
                    }
                }
                FilterState::StringSeq {
                    strip,
                    saw_esc,
                    discarded,
                } => {
                    if !strip {
                        out.push(b);
                    }
                    if b == 0x07 || (saw_esc && b == b'\\') {
                        self.state = FilterState::Ground;
                    } else if strip && discarded >= 256 {
                        // Match the Unix title filter's fail-open bound: a
                        // malformed title must not swallow unbounded real output.
                        self.state = FilterState::Ground;
                    } else {
                        self.state = FilterState::StringSeq {
                            strip,
                            saw_esc: b == 0x1b,
                            discarded: discarded + usize::from(strip),
                        };
                    }
                }
                FilterState::SingleShift => {
                    out.push(b);
                    self.state = FilterState::Ground;
                }
                FilterState::Nf => {
                    out.push(b);
                    if (0x30..=0x7e).contains(&b) {
                        self.state = FilterState::Ground;
                    }
                }
            }
        }
        // If this read contained only a stripped title OSC, retain the previous
        // UTF-8 state: no real output arrived to complete the pending character.
        if out.len() > output_start {
            self.pending_utf8 = advance_pending_utf8(self.pending_utf8, &out[output_start..]);
        }
    }

    /// One-shot: returns `true` once after a cursor-position query (`ESC[6n`)
    /// was seen, then resets. The reader uses it to reply on the child's behalf
    /// when running headless.
    pub(super) fn take_dsr(&mut self) -> bool {
        std::mem::take(&mut self.dsr_seen)
    }

    /// True when an hcom title OSC can be appended without splitting a control
    /// sequence or a multi-byte UTF-8 character.
    pub(super) fn title_write_safe(&self) -> bool {
        self.state == FilterState::Ground && self.pending_utf8 == 0
    }
}

/// Advance the number of expected UTF-8 continuation bytes across arbitrary
/// read boundaries. Invalid input resets the guard and is otherwise untouched.
fn advance_pending_utf8(mut pending: u8, data: &[u8]) -> u8 {
    for &byte in data {
        if pending > 0 && byte & 0xc0 == 0x80 {
            pending -= 1;
            continue;
        }
        pending = if byte & 0xf8 == 0xf0 {
            3
        } else if byte & 0xf0 == 0xe0 {
            2
        } else if byte & 0xe0 == 0xc0 {
            1
        } else {
            0
        };
    }
    pending
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::status_icon;

    #[test]
    fn should_start_delivery_on_ready() {
        // Ready alone starts delivery even well before the timeout.
        assert!(should_start_delivery(
            true,
            Duration::from_millis(1),
            Duration::from_secs(10),
            false,
        ));
    }

    #[test]
    fn should_start_delivery_on_timeout() {
        // #8 regression: not ready, but elapsed exceeded the timeout → start.
        assert!(should_start_delivery(
            false,
            Duration::from_secs(11),
            Duration::from_secs(10),
            false,
        ));
    }

    #[test]
    fn should_start_delivery_on_shutdown() {
        // Shutting down forces a final start (child already exited) so init-time
        // registration and post-loop cleanup run, even if never ready and still
        // inside the timeout.
        assert!(should_start_delivery(
            false,
            Duration::from_millis(1),
            Duration::from_secs(10),
            true,
        ));
    }

    #[test]
    fn should_not_start_delivery_before_ready_or_timeout() {
        // #8 regression: not ready and still inside the timeout window while
        // running → must NOT start yet (the old reader could start too early).
        assert!(!should_start_delivery(
            false,
            Duration::from_secs(1),
            Duration::from_secs(10),
            false,
        ));
    }

    #[test]
    fn should_refresh_snapshot_only_after_throttle_elapsed() {
        let throttle = Duration::from_millis(100);
        // Fresh chunk right after a refresh: defer (mark dirty), don't re-render.
        assert!(!should_refresh_snapshot(Duration::from_millis(0), throttle));
        assert!(!should_refresh_snapshot(
            Duration::from_millis(99),
            throttle
        ));
        // At/after the throttle window: render now (leading edge).
        assert!(should_refresh_snapshot(
            Duration::from_millis(100),
            throttle
        ));
        assert!(should_refresh_snapshot(
            Duration::from_millis(250),
            throttle
        ));
    }

    #[test]
    fn csi_is_dsr_cpr_matches_only_the_cursor_position_query() {
        assert!(csi_is_dsr_cpr(b"\x1b[6n"));
        // A CPR reply, a private DSR, and a bare DSR must not match.
        assert!(!csi_is_dsr_cpr(b"\x1b[6;1R"));
        assert!(!csi_is_dsr_cpr(b"\x1b[?6n"));
        assert!(!csi_is_dsr_cpr(b"\x1b[n"));
    }

    fn filter_modes(chunks: &[&[u8]]) -> Vec<u8> {
        let mut f = OutputModeFilter::default();
        let mut out = Vec::new();
        for c in chunks {
            f.filter(c, &mut out);
        }
        out
    }

    #[test]
    fn filter_dec_private_modes_drops_only_targeted_params() {
        // Whole-prefix regression (#15): a targeted param anywhere in the list
        // must be dropped without losing the others, in either order.
        assert_eq!(filter_dec_private_modes(b"\x1b[?9001;25h"), b"\x1b[?25h");
        assert_eq!(filter_dec_private_modes(b"\x1b[?25;9001h"), b"\x1b[?25h");
        assert_eq!(
            filter_dec_private_modes(b"\x1b[?1004;2004h"),
            b"\x1b[?2004h"
        );
        assert_eq!(filter_dec_private_modes(b"\x1b[?9001;1004h"), b"");
        assert_eq!(
            filter_dec_private_modes(b"\x1b[?9001;1004;25h"),
            b"\x1b[?25h"
        );
        assert_eq!(filter_dec_private_modes(b"\x1b[?25;9001l"), b"\x1b[?25l");
    }

    #[test]
    fn output_mode_filter_drops_win32_and_focus_mode_sets() {
        // ESC[?9001h ESC[?1004h "hi" ESC[6n — mode-sets dropped, DSR passes.
        let input = b"\x1b[?9001h\x1b[?1004h hi \x1b[6n";
        assert_eq!(filter_modes(&[input]), b" hi \x1b[6n");
    }

    #[test]
    fn output_mode_filter_passes_other_sequences_and_text() {
        let input = b"\x1b[31mred\x1b[0m\x1b[2J plain";
        assert_eq!(filter_modes(&[input]), input);
    }

    #[test]
    fn output_mode_filter_keeps_other_modes_in_a_combined_set() {
        // #15: mixed sets keep non-targeted modes and drop targeted ones,
        // regardless of parameter order.
        assert_eq!(filter_modes(&[b"\x1b[?9001;25h"]), b"\x1b[?25h");
        assert_eq!(filter_modes(&[b"\x1b[?25;9001h"]), b"\x1b[?25h");
        assert_eq!(filter_modes(&[b"\x1b[4h\x1b[?25h"]), b"\x1b[4h\x1b[?25h");
    }

    #[test]
    fn output_mode_filter_handles_sequence_split_across_reads() {
        // A combined set split mid-sequence still filters per-parameter.
        assert_eq!(filter_modes(&[b"\x1b[?25;90", b"01h"]), b"\x1b[?25h");
        // The pure Win32-input set split mid-sequence is still fully dropped.
        assert_eq!(filter_modes(&[b"\x1b[?90", b"01h", b"X"]), b"X");
    }

    #[test]
    fn output_mode_filter_drops_mode_reset_too() {
        assert_eq!(filter_modes(&[b"\x1b[?9001l\x1b[?1004lY"]), b"Y");
    }

    #[test]
    fn output_mode_filter_take_dsr_is_one_shot() {
        let mut f = OutputModeFilter::default();
        let mut out = Vec::new();
        f.filter(b"\x1b[6n", &mut out);
        // DSR passes through to the outer terminal...
        assert_eq!(out, b"\x1b[6n");
        // ...and is latched exactly once.
        assert!(f.take_dsr());
        assert!(!f.take_dsr());
    }

    #[test]
    fn output_mode_filter_strips_title_osc_split_across_reads() {
        assert_eq!(
            filter_modes(&[b"before\x1b]2;Clau", b"de Code\x07after"]),
            b"beforeafter"
        );
        assert_eq!(
            filter_modes(&[b"\x1b", b"]1", b";icon\x1b", b"\\text"]),
            b"text"
        );
    }

    #[test]
    fn output_mode_filter_passthrough_keeps_tool_title() {
        // title_mode `off`: the tool's own OSC 0/2 title must reach the terminal
        // intact, including across a read split, while DSR tracking still works.
        let mut f = OutputModeFilter::default();
        f.set_passthrough_titles(true);
        let mut out = Vec::new();
        f.filter(b"before\x1b]2;Clau", &mut out);
        f.filter(b"de Code\x07after", &mut out);
        assert_eq!(out, b"before\x1b]2;Claude Code\x07after".to_vec());
    }

    #[test]
    fn output_mode_filter_preserves_non_title_osc_and_tracks_its_boundary() {
        let chunks: &[&[u8]] = &[b"\x1b]8;;https://exam", b"ple.test\x1b\\link"];
        assert_eq!(filter_modes(chunks), chunks.concat());

        let mut f = OutputModeFilter::default();
        let mut out = Vec::new();
        f.filter(chunks[0], &mut out);
        assert!(!f.title_write_safe());
        f.filter(chunks[1], &mut out);
        assert!(f.title_write_safe());
    }

    #[test]
    fn output_mode_filter_defers_title_across_split_utf8() {
        let mut f = OutputModeFilter::default();
        let mut out = Vec::new();
        f.filter(&[0xe2], &mut out);
        assert!(!f.title_write_safe());
        f.filter(&[0x94], &mut out);
        assert!(!f.title_write_safe());
        f.filter(&[0x80], &mut out);
        assert!(f.title_write_safe());
        assert_eq!(out, "─".as_bytes());
    }

    #[test]
    fn output_mode_filter_title_only_read_preserves_pending_utf8() {
        let mut f = OutputModeFilter::default();
        let mut out = Vec::new();
        f.filter(&[0xe2, 0x94], &mut out);
        assert!(!f.title_write_safe());
        f.filter(b"\x1b]2;Claude Code\x07", &mut out);
        assert!(!f.title_write_safe());
        f.filter(&[0x80], &mut out);
        assert!(f.title_write_safe());
    }

    #[test]
    fn output_mode_filter_tracks_other_split_escape_boundaries() {
        let mut f = OutputModeFilter::default();
        let mut out = Vec::new();

        f.filter(b"\x1bPpayload", &mut out);
        assert!(!f.title_write_safe());
        f.filter(b"\x1b\\", &mut out);
        assert!(f.title_write_safe());

        f.filter(b"\x1bN", &mut out);
        assert!(!f.title_write_safe());
        f.filter(b"x", &mut out);
        assert!(f.title_write_safe());

        f.filter(b"\x1b(", &mut out);
        assert!(!f.title_write_safe());
        f.filter(b"B", &mut out);
        assert!(f.title_write_safe());

        assert_eq!(out, b"\x1bPpayload\x1b\\\x1bNx\x1b(B");
    }

    #[test]
    fn build_title_escape_label_mode_formats_osc_1_and_2() {
        use crate::shared::TitleMode;
        // Label mode keeps the [tool] tag; assert exact OSC framing.
        let esc = build_title_escape("alpha", "listening", "claude", TitleMode::Label, None);
        let icon = status_icon("listening");
        let title = format!("{} alpha [claude]", icon);
        assert_eq!(esc, format!("\x1b]1;{}\x07\x1b]2;{}\x07", title, title));
        assert!(esc.starts_with("\x1b]1;"));
        assert!(esc.contains("\x07\x1b]2;"));
        assert!(esc.ends_with('\x07'));
    }

    #[test]
    fn build_title_escape_uses_status_icon() {
        use crate::shared::TitleMode;
        // Different statuses must change the embedded icon.
        let listening = build_title_escape("a", "listening", "claude", TitleMode::Label, None);
        let blocked = build_title_escape("a", "blocked", "claude", TitleMode::Label, None);
        assert_ne!(listening, blocked);
    }

    #[test]
    fn build_title_escape_combined_appends_child_and_drops_tool() {
        use crate::shared::TitleMode;
        // Combined mode: `{icon} name - {child}`, no `[tool]` tag.
        let icon = status_icon("active");
        let esc = build_title_escape(
            "luna",
            "active",
            "codex",
            TitleMode::Combined,
            Some("⠋ Working"),
        );
        let title = format!("{} luna - ⠋ Working", icon);
        assert_eq!(esc, format!("\x1b]1;{}\x07\x1b]2;{}\x07", title, title));
        assert!(!esc.contains("[codex]"), "combined mode drops the tool tag");
    }

    #[test]
    fn build_title_escape_combined_without_child_is_icon_name() {
        use crate::shared::TitleMode;
        // No child title → just `{icon} name`, no dangling separator, no tag.
        let icon = status_icon("active");
        let esc = build_title_escape("luna", "active", "codex", TitleMode::Combined, None);
        let title = format!("{} luna", icon);
        assert_eq!(esc, format!("\x1b]1;{}\x07\x1b]2;{}\x07", title, title));
    }

    #[test]
    fn note_user_keystroke_cursor_is_noop_and_returns_false() {
        let target = PtyTarget::AdhocCommand("cursor".to_string());
        let state = Arc::new(RwLock::new(ScreenState {
            approval: true,
            ..ScreenState::default()
        }));
        let calls = std::cell::Cell::new(0);
        let publish = |_a: bool| calls.set(calls.get() + 1);
        // cursor name: must not clear approval, must not publish, returns false.
        let cleared = note_user_keystroke(&target, &state, &publish);
        assert!(!cleared);
        assert!(state.read().unwrap().approval, "cursor approval untouched");
        assert_eq!(calls.get(), 0, "cursor keystroke must not publish");
    }

    #[test]
    fn note_user_keystroke_clears_approval_for_non_cursor() {
        let target = PtyTarget::Known(Tool::Claude);
        let state = Arc::new(RwLock::new(ScreenState {
            approval: true,
            ..ScreenState::default()
        }));
        let calls = std::cell::Cell::new(0);
        let publish = |a: bool| {
            assert!(!a, "keystroke publishes the cleared (false) edge");
            calls.set(calls.get() + 1);
        };
        let cleared = note_user_keystroke(&target, &state, &publish);
        assert!(cleared, "a standing approval was cleared");
        assert!(!state.read().unwrap().approval, "approval cleared");
        assert!(state.read().unwrap().approval_response_at.is_some());
        assert_eq!(calls.get(), 1, "cleared edge published once");
    }

    #[test]
    fn note_user_keystroke_no_publish_when_not_blocked() {
        let target = PtyTarget::Known(Tool::Claude);
        let state = Arc::new(RwLock::new(ScreenState::default())); // approval=false
        let calls = std::cell::Cell::new(0);
        let publish = |_a: bool| calls.set(calls.get() + 1);
        // No approval was showing, so nothing is cleared and the Windows caller
        // must not request a tracker-clear (which would wipe the scrape buffer).
        let cleared = note_user_keystroke(&target, &state, &publish);
        assert!(
            !cleared,
            "no standing approval means no tracker clear requested"
        );
        assert_eq!(calls.get(), 0, "no edge to publish when already clear");
    }

    #[test]
    fn clear_injected_approval_state_cursor_returns_false() {
        let target = PtyTarget::AdhocCommand("cursor".to_string());
        let state = Arc::new(RwLock::new(ScreenState {
            approval: true,
            ..ScreenState::default()
        }));
        let publish = |_a: bool| panic!("cursor must not publish");
        assert!(!clear_injected_approval_state(&target, &state, &publish));
        assert!(state.read().unwrap().approval, "cursor approval untouched");
    }

    #[test]
    fn clear_injected_approval_state_clears_when_blocked() {
        let target = PtyTarget::Known(Tool::Claude);
        let state = Arc::new(RwLock::new(ScreenState {
            approval: true,
            ..ScreenState::default()
        }));
        let calls = std::cell::Cell::new(0);
        let publish = |a: bool| {
            assert!(!a);
            calls.set(calls.get() + 1);
        };
        assert!(clear_injected_approval_state(&target, &state, &publish));
        assert!(!state.read().unwrap().approval);
        assert!(state.read().unwrap().approval_response_at.is_some());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn clear_injected_approval_state_noop_when_not_blocked() {
        let target = PtyTarget::Known(Tool::Claude);
        let state = Arc::new(RwLock::new(ScreenState::default()));
        let publish = |_a: bool| panic!("must not publish when nothing to clear");
        assert!(!clear_injected_approval_state(&target, &state, &publish));
    }

    #[test]
    #[serial]
    fn publish_screen_denial_releases_only_hook_owned_approval() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, tool, status, status_context, status_detail, status_time, created_at)
                 VALUES ('nova', 'claude', 'blocked', 'approval', 'cargo fmt', 0, 0)",
                [],
            )
            .unwrap();
        let shared_status = Arc::new(RwLock::new(ST_BLOCKED.to_string()));

        publish_approval_status(false, Some("nova"), &shared_status);

        let released = db.get_instance_full("nova").unwrap().unwrap();
        assert_eq!(released.status, ST_LISTENING);
        assert_eq!(released.status_context, "denied:screen");
        assert_eq!(released.status_detail, "cargo fmt");
        assert_eq!(*shared_status.read().unwrap(), ST_LISTENING);
        let event_data: String = db
            .conn()
            .query_row(
                "SELECT data FROM events WHERE type = 'status' AND instance = 'nova'
                 ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let event: serde_json::Value = serde_json::from_str(&event_data).unwrap();
        assert_eq!(event["status"], ST_LISTENING);
        assert_eq!(event["context"], "denied:screen");

        for (status, context) in [("active", "tool:Bash"), (ST_BLOCKED, "elicitation")] {
            db.conn()
                .execute(
                    "UPDATE instances SET status = ?1, status_context = ?2 WHERE name = 'nova'",
                    rusqlite::params![status, context],
                )
                .unwrap();
            publish_approval_status(false, Some("nova"), &shared_status);
            let unchanged = db.get_instance_full("nova").unwrap().unwrap();
            assert_eq!(unchanged.status, status);
            assert_eq!(unchanged.status_context, context);
        }
    }

    #[test]
    fn approval_response_grace_expires_after_one_redraw_window() {
        assert!(!approval_response_grace_active(None));
        assert!(approval_response_grace_active(Some(Instant::now())));
        assert!(!approval_response_grace_active(Some(
            Instant::now() - Duration::from_millis(APPROVAL_RESPONSE_GRACE_MS + 1)
        )));
    }

    // ---- Antigravity feedback survey dismissal policy (hcom-f6g.9) ----

    #[test]
    fn survey_tick_holds_until_stable_then_dismisses_with_cooldown() {
        let mut s = SurveyDismissal::default();
        let t0 = Instant::now();
        // Sighting mid-draw: hold fire (partial frames must not be keyed).
        assert_eq!(
            survey_dismissal_tick(&mut s, true, false, t0),
            SurveyAction::None
        );
        assert!(s.caps_poll(), "sighting latches the poll-cap hint");
        // Settled: first Skip attempt.
        let t1 = t0 + Duration::from_millis(500);
        assert_eq!(
            survey_dismissal_tick(&mut s, true, true, t1),
            SurveyAction::Dismiss
        );
        // Still visible (key not consumed yet): cooldown holds further tries.
        let t2 = t1 + Duration::from_millis(100);
        assert_eq!(
            survey_dismissal_tick(&mut s, true, true, t2),
            SurveyAction::None
        );
        // After the cooldown the retry fires.
        let t3 = t1 + SURVEY_DISMISS_RETRY_COOLDOWN;
        assert_eq!(
            survey_dismissal_tick(&mut s, true, true, t3),
            SurveyAction::Dismiss
        );
    }

    #[test]
    fn survey_tick_waits_out_final_cooldown_before_blocking() {
        // Regression (hcom-f6g.9 review): the third Skip attempt gets its
        // full cooldown before the blocker may publish — the tool may still
        // be mid-redraw consuming the key.
        let mut s = SurveyDismissal::default();
        let t0 = Instant::now();
        for i in 0..SURVEY_DISMISS_MAX_ATTEMPTS {
            let t = t0 + SURVEY_DISMISS_RETRY_COOLDOWN * i;
            assert_eq!(
                survey_dismissal_tick(&mut s, true, true, t),
                SurveyAction::Dismiss
            );
        }
        let third_attempt = t0 + SURVEY_DISMISS_RETRY_COOLDOWN * (SURVEY_DISMISS_MAX_ATTEMPTS - 1);
        // Inside the final cooldown: no Block yet.
        assert_eq!(
            survey_dismissal_tick(
                &mut s,
                true,
                true,
                third_attempt + SURVEY_DISMISS_RETRY_COOLDOWN - Duration::from_millis(1)
            ),
            SurveyAction::None
        );
        // Full cooldown elapsed: Block.
        assert_eq!(
            survey_dismissal_tick(
                &mut s,
                true,
                true,
                third_attempt + SURVEY_DISMISS_RETRY_COOLDOWN
            ),
            SurveyAction::Block
        );
    }

    #[test]
    fn survey_tick_blocks_once_after_exhausted_attempts() {
        let mut s = SurveyDismissal::default();
        let t0 = Instant::now();
        for i in 0..SURVEY_DISMISS_MAX_ATTEMPTS {
            let t = t0 + SURVEY_DISMISS_RETRY_COOLDOWN * i;
            assert_eq!(
                survey_dismissal_tick(&mut s, true, true, t),
                SurveyAction::Dismiss
            );
        }
        let t_block = t0 + SURVEY_DISMISS_RETRY_COOLDOWN * SURVEY_DISMISS_MAX_ATTEMPTS;
        assert_eq!(
            survey_dismissal_tick(&mut s, true, true, t_block),
            SurveyAction::Block
        );
        // The tick keeps emitting Block until the CALLER latches a
        // successful publication (`run_survey_dismissal` does); simulate it.
        s.blocker_published = true;
        assert_eq!(
            survey_dismissal_tick(&mut s, true, true, t_block),
            SurveyAction::None
        );
    }

    #[test]
    fn survey_tick_animated_survey_escalates_after_bounded_age() {
        // An animated survey whose output never settles can never pass the
        // stability gate, so no Skip key is ever injected…
        let mut s = SurveyDismissal::default();
        let t0 = Instant::now();
        assert_eq!(
            survey_dismissal_tick(&mut s, true, false, t0),
            SurveyAction::None
        );
        assert_eq!(
            survey_dismissal_tick(&mut s, true, false, t0 + Duration::from_secs(10)),
            SurveyAction::None
        );
        // …but escalation cannot be starved forever: at the bounded sighting
        // age the actionable blocker fires even while output churns.
        assert_eq!(
            survey_dismissal_tick(&mut s, true, false, t0 + SURVEY_SIGHTING_MAX),
            SurveyAction::Block
        );
    }

    #[test]
    fn survey_tick_flicker_cannot_starve_escalation() {
        // Half-second tick cadence (the Unix poll cap) with a one-tick
        // disappearance inside every cooldown window. Each flicker is well
        // inside the reappearance grace, so the sighting's age and attempt
        // budget survive and escalation completes on schedule.
        let mut s = SurveyDismissal::default();
        let step = Duration::from_millis(500);
        let t0 = Instant::now();
        let mut dismissals = 0u32;
        let mut blocked_at: Option<u32> = None;
        for i in 0..16u32 {
            let now = t0 + step * i;
            let visible = i % 5 != 1; // one-tick flicker per window
            match survey_dismissal_tick(&mut s, visible, true, now) {
                SurveyAction::Dismiss => dismissals += 1,
                SurveyAction::Block => blocked_at = Some(i),
                SurveyAction::Unblock => panic!("no blocker was published"),
                SurveyAction::None => {}
            }
        }
        assert_eq!(dismissals, SURVEY_DISMISS_MAX_ATTEMPTS);
        // Third attempt at 2×cooldown; the blocker follows only after the
        // full final cooldown (i.e. at 3×cooldown = tick 15).
        assert_eq!(blocked_at, Some(15));
    }

    #[test]
    fn survey_tick_clears_blocker_and_rearms_when_survey_leaves() {
        let mut s = SurveyDismissal::default();
        let t0 = Instant::now();
        for i in 0..SURVEY_DISMISS_MAX_ATTEMPTS {
            let t = t0 + SURVEY_DISMISS_RETRY_COOLDOWN * i;
            assert_eq!(
                survey_dismissal_tick(&mut s, true, true, t),
                SurveyAction::Dismiss
            );
        }
        let t_block = t0 + SURVEY_DISMISS_RETRY_COOLDOWN * SURVEY_DISMISS_MAX_ATTEMPTS;
        assert_eq!(
            survey_dismissal_tick(&mut s, true, true, t_block),
            SurveyAction::Block
        );
        s.blocker_published = true; // caller latched the publication
        // Survey leaves: within the reappearance grace the sighting (and the
        // published blocker) survives a flicker…
        assert_eq!(
            survey_dismissal_tick(&mut s, false, true, t_block + Duration::from_millis(500)),
            SurveyAction::None
        );
        // …past the grace the sighting ends and the blocker clear fires.
        assert_eq!(
            survey_dismissal_tick(&mut s, false, true, t_block + SURVEY_REAPPEAR_GRACE),
            SurveyAction::Unblock
        );
        // Caller observed the successful clear → re-armed: a fresh sighting
        // gets a fresh attempt budget.
        s.blocker_published = false;
        let t2 = t_block + SURVEY_REAPPEAR_GRACE + Duration::from_secs(5);
        assert_eq!(
            survey_dismissal_tick(&mut s, true, true, t2),
            SurveyAction::Dismiss
        );
        // Leaving again without a published blocker is silent.
        let t3 = t2 + Duration::from_secs(10);
        assert_eq!(
            survey_dismissal_tick(&mut s, false, true, t3),
            SurveyAction::None
        );
    }

    #[test]
    fn survey_tick_redraw_flicker_does_not_refire_immediately() {
        // A mid-redraw frame that hides the survey for one tick must not
        // re-arm an immediate second Skip key — the cooldown survives.
        let mut s = SurveyDismissal::default();
        let t0 = Instant::now();
        assert_eq!(
            survey_dismissal_tick(&mut s, true, true, t0),
            SurveyAction::Dismiss
        );
        assert_eq!(
            survey_dismissal_tick(&mut s, false, true, t0 + Duration::from_millis(50)),
            SurveyAction::None
        );
        assert_eq!(
            survey_dismissal_tick(&mut s, true, true, t0 + Duration::from_millis(100)),
            SurveyAction::None,
            "back within the cooldown: no immediate second key"
        );
        assert_eq!(
            survey_dismissal_tick(
                &mut s,
                true,
                true,
                t0 + SURVEY_DISMISS_RETRY_COOLDOWN + Duration::from_millis(100)
            ),
            SurveyAction::Dismiss
        );
    }

    #[test]
    fn survey_tick_idle_state_is_inert() {
        let mut s = SurveyDismissal::default();
        assert_eq!(
            survey_dismissal_tick(&mut s, false, true, Instant::now()),
            SurveyAction::None
        );
        assert!(!s.caps_poll());
    }

    #[test]
    #[serial]
    fn publish_survey_status_ownership_semantics() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let shared_status = Arc::new(RwLock::new(ST_LISTENING.to_string()));

        // Unbound instance: publication deferred (it may bind later), so the
        // caller must retry instead of latching.
        assert!(!publish_survey_status(true, Some("mia"), &shared_status));

        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, tool, status, status_context, status_detail, status_time, created_at)
                 VALUES ('mia', 'antigravity', 'listening', 'idle', '', 0, 0)",
                [],
            )
            .unwrap();

        // Rising edge publishes and reports ownership.
        assert!(publish_survey_status(true, Some("mia"), &shared_status));
        let row = db.get_instance_full("mia").unwrap().unwrap();
        assert_eq!(row.status, ST_BLOCKED);
        assert_eq!(row.status_context, "pty:survey");
        assert_eq!(*shared_status.read().unwrap(), ST_BLOCKED);

        // Already ours: idempotent success.
        assert!(publish_survey_status(true, Some("mia"), &shared_status));

        // Falling edge clears and reports success.
        assert!(publish_survey_status(false, Some("mia"), &shared_status));
        let row = db.get_instance_full("mia").unwrap().unwrap();
        assert_eq!(row.status, ST_LISTENING);
        assert_eq!(row.status_context, "pty:survey_cleared");
        assert_eq!(*shared_status.read().unwrap(), ST_LISTENING);

        // Another blocker owning the row defers the rising edge…
        db.conn()
            .execute(
                "UPDATE instances SET status = 'blocked', status_context = 'approval'
                 WHERE name = 'mia'",
                [],
            )
            .unwrap();
        assert!(!publish_survey_status(true, Some("mia"), &shared_status));
        let row = db.get_instance_full("mia").unwrap().unwrap();
        assert_eq!(row.status_context, "approval", "other blocker untouched");
        // …and once it clears, the retried publish succeeds.
        db.conn()
            .execute(
                "UPDATE instances SET status = 'listening', status_context = 'idle'
                 WHERE name = 'mia'",
                [],
            )
            .unwrap();
        assert!(publish_survey_status(true, Some("mia"), &shared_status));

        // Clearing while another blocker owns the row touches nothing and
        // reports handled (there is no survey block of ours to clear).
        db.conn()
            .execute(
                "UPDATE instances SET status = 'blocked', status_context = 'elicitation'
                 WHERE name = 'mia'",
                [],
            )
            .unwrap();
        assert!(publish_survey_status(false, Some("mia"), &shared_status));
        let row = db.get_instance_full("mia").unwrap().unwrap();
        assert_eq!(row.status, ST_BLOCKED);
        assert_eq!(row.status_context, "elicitation");
    }

    // build_early_launch_context is portable (env::var/fs::read_to_string/thread::sleep
    // all work identically on Windows), so these run on every platform rather than
    // being Unix-only. Each test clears the env vars it touches before asserting so a
    // panic can't leak state into later tests; #[serial] additionally prevents these
    // from interleaving with each other.
    use serial_test::serial;

    fn clear_launch_context_env() {
        // SAFETY: tests are #[serial].
        unsafe {
            std::env::remove_var("HCOM_PROCESS_ID");
            std::env::remove_var("KITTY_LISTEN_ON");
            std::env::remove_var("WEZTERM_PANE");
            std::env::remove_var("TMUX_PANE");
            std::env::remove_var("KITTY_WINDOW_ID");
            std::env::remove_var("ZELLIJ_PANE_ID");
            std::env::remove_var("HCOM_LAUNCHED_PRESET");
            std::env::remove_var("HERDR_PANE_ID");
        }
    }

    #[test]
    #[serial]
    fn build_early_launch_context_empty_when_no_env_vars_set() {
        clear_launch_context_env();
        let json = build_early_launch_context();
        clear_launch_context_env();
        assert_eq!(json, "{}");
    }

    #[test]
    #[serial]
    fn build_early_launch_context_captures_kitty_listen_on() {
        clear_launch_context_env();
        // SAFETY: test is #[serial].
        unsafe {
            std::env::set_var("KITTY_LISTEN_ON", "unix:/tmp/kitty.sock");
        }
        let json = build_early_launch_context();
        clear_launch_context_env();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["kitty_listen_on"], "unix:/tmp/kitty.sock");
        assert!(parsed.get("pane_id").is_none());
    }

    #[test]
    #[serial]
    fn build_early_launch_context_prefers_first_pane_id_var_in_priority_order() {
        clear_launch_context_env();
        // SAFETY: test is #[serial].
        unsafe {
            std::env::set_var("WEZTERM_PANE", "wezterm-pane");
            std::env::set_var("TMUX_PANE", "tmux-pane");
        }
        let json = build_early_launch_context();
        clear_launch_context_env();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["pane_id"], "wezterm-pane");
    }

    #[test]
    #[serial]
    fn build_early_launch_context_ignores_multiplexer_only_vars_when_absent() {
        // TMUX_PANE/ZELLIJ_PANE_ID never being set on Windows must degrade to
        // simply absent fields, not an error — same as on Unix outside a
        // multiplexer. This is the "no platform branching needed" behavior.
        clear_launch_context_env();
        // SAFETY: test is #[serial].
        unsafe {
            std::env::set_var("KITTY_WINDOW_ID", "kitty-window-1");
        }
        let json = build_early_launch_context();
        clear_launch_context_env();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["pane_id"], "kitty-window-1");
    }

    #[test]
    #[serial]
    fn build_early_launch_context_preset_pane_id_env_wins_over_generic_vars() {
        clear_launch_context_env();
        // SAFETY: test is #[serial].
        unsafe {
            std::env::set_var("HCOM_LAUNCHED_PRESET", "herdr");
            std::env::set_var("HERDR_PANE_ID", "w2:p1A");
            std::env::set_var("WEZTERM_PANE", "0");
        }
        let json = build_early_launch_context();
        clear_launch_context_env();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["pane_id"], "w2:p1A");
    }

    #[test]
    #[serial]
    fn build_early_launch_context_known_preset_rejects_foreign_fallback() {
        clear_launch_context_env();
        // SAFETY: test is #[serial].
        unsafe {
            std::env::set_var("HCOM_LAUNCHED_PRESET", "herdr");
            std::env::set_var("WEZTERM_PANE", "4");
        }
        let json = build_early_launch_context();
        clear_launch_context_env();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("pane_id").is_none());
    }

    // ---- Antigravity mode-banner delivery wiring (hcom-f6g.10) ----

    /// Tracker showing a ready, empty agy prompt in accept-edits mode: the
    /// mode banner on the `>` row plus the ready footer. Built with the real
    /// constructor exactly as the PTY loops do (test Config redirects to a
    /// throwaway dir, so no debug flag file exists).
    fn banner_tracker() -> ScreenTracker {
        let mut tracker = ScreenTracker::new_with_instance(24, 80, b"? for shortcuts", None);
        tracker.process(
            "> Accept-edits mode: file edits auto-approved (shift+tab to cycle)\r\n".as_bytes(),
        );
        tracker.process("? for shortcuts\r\n".as_bytes());
        tracker
    }

    #[test]
    fn delivery_state_maps_antigravity_mode_banner_to_empty_prompt() {
        // hcom-f6g.10: a ready agy prompt whose only contents is the visible
        // accept-edits mode banner must reach the delivery gate as an empty
        // prompt (input_text=Some("")), so a targeted send injects, submits
        // exactly one turn, and the submit-edge stamping below still fires on
        // the injected text clearing — instead of blocking forever on
        // prompt_has_text with the banner read back as uncommitted text.
        let screen_state = Arc::new(RwLock::new(ScreenState::default()));
        let tracker = banner_tracker();
        let launch_phase_active = Arc::new(AtomicBool::new(true));
        update_delivery_state(
            &screen_state,
            &tracker,
            &PtyTarget::Known(Tool::Antigravity),
            &launch_phase_active,
            &|_| {},
        );
        {
            let state = screen_state.read().unwrap();
            assert!(state.ready);
            assert!(state.prompt_empty);
            assert_eq!(state.input_text.as_deref(), Some(""));
            assert!(!state.approval);
        }

        // Injected wake text replaces the banner; on Enter it clears back to
        // the banner — still an empty prompt, with the submit edge stamped.
        let mut mid_turn = banner_tracker();
        mid_turn.process("\x1b[1;1H\x1b[2J".as_bytes());
        mid_turn.process("> <hcom>test</hcom>\r\n? for shortcuts\r\n".as_bytes());
        update_delivery_state(
            &screen_state,
            &mid_turn,
            &PtyTarget::Known(Tool::Antigravity),
            &launch_phase_active,
            &|_| {},
        );
        {
            let state = screen_state.read().unwrap();
            assert!(!state.prompt_empty);
            assert_eq!(state.input_text.as_deref(), Some("<hcom>test</hcom>"));
            assert!(state.last_prompt_submit.is_none());
        }

        let cleared = banner_tracker();
        update_delivery_state(
            &screen_state,
            &cleared,
            &PtyTarget::Known(Tool::Antigravity),
            &launch_phase_active,
            &|_| {},
        );
        let state = screen_state.read().unwrap();
        assert!(state.prompt_empty);
        assert_eq!(state.input_text.as_deref(), Some(""));
        assert!(state.last_prompt_submit.is_some());
    }

    #[test]
    #[serial]
    fn update_delivery_state_detects_and_clears_antigravity_sandbox_bypass() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, tool, status, status_context, status_detail, status_time, created_at)
                 VALUES ('hera', 'antigravity', 'active', 'tool:run_command',
                         'bd show hcom-f6g.12', 0, 1000.0)",
                [],
            )
            .unwrap();
        let screen_state = Arc::new(RwLock::new(ScreenState::default()));
        let mut tracker = ScreenTracker::new_with_instance(24, 80, b"? for shortcuts", None);
        let launch_phase_active = Arc::new(AtomicBool::new(false));
        let shared_status = Arc::new(RwLock::new(crate::shared::ST_ACTIVE.to_string()));
        let publish_status = Arc::clone(&shared_status);
        let publish = move |a: bool| {
            publish_approval_status(a, Some("hera"), &publish_status);
        };

        // Render sandbox bypass approval dialog
        tracker.process(
            b"Requesting permission for: bd show hcom-f6g.12\r\nAllow sandbox bypass for command execution?\r\n> 1. Yes\r\n  2. Yes, and always allow sandbox bypass for command execution\r\n  3. No, keep sandboxed\r\n  Up/Down Navigate - tab Amend - ctrl+g edit/expand command\r\nesc to cancel\r\n",
        );

        update_delivery_state(
            &screen_state,
            &tracker,
            &PtyTarget::Known(Tool::Antigravity),
            &launch_phase_active,
            &publish,
        );

        assert!(screen_state.read().unwrap().approval);
        let blocked = db.get_instance_full("hera").unwrap().unwrap();
        assert_eq!(blocked.status, ST_BLOCKED);
        assert_eq!(blocked.status_context, "pty:approval");
        assert_eq!(blocked.status_detail, "bd show hcom-f6g.12");
        assert_eq!(*shared_status.read().unwrap(), ST_BLOCKED);
        let blocked_event: String = db
            .conn()
            .query_row(
                "SELECT data FROM events WHERE type = 'status' AND instance = 'hera'
                 ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let blocked_event: serde_json::Value = serde_json::from_str(&blocked_event).unwrap();
        assert_eq!(blocked_event["status"], ST_BLOCKED);
        assert_eq!(blocked_event["context"], "pty:approval");
        assert_eq!(blocked_event["detail"], "bd show hcom-f6g.12");

        // When denied / cleared, screen transitions to idle prompt
        let mut denied = ScreenTracker::new_with_instance(24, 80, b"? for shortcuts", None);
        denied.process(
            b"Requesting permission for: bd show hcom-f6g.12\r\nAllow sandbox bypass for command execution?\r\n> 1. Yes\r\n  2. Yes, and always allow\r\n  3. No, keep sandboxed\r\n  Up/Down Navigate - tab Amend - ctrl+g edit/expand command\r\nesc to cancel\r\nuser denied permission to run command\r\n> idle\r\n",
        );

        update_delivery_state(
            &screen_state,
            &denied,
            &PtyTarget::Known(Tool::Antigravity),
            &launch_phase_active,
            &publish,
        );

        assert!(!screen_state.read().unwrap().approval);
        let cleared = db.get_instance_full("hera").unwrap().unwrap();
        assert_eq!(cleared.status, ST_LISTENING);
        assert_eq!(cleared.status_context, "pty:approval_cleared");
        assert_eq!(cleared.status_detail, "bd show hcom-f6g.12");
        assert_eq!(*shared_status.read().unwrap(), ST_LISTENING);
    }
}
