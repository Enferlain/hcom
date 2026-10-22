//! Antigravity feedback-survey dismissal policy and typed blocker publishing (hcom-f6g.9).

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::config::Config;
use crate::db::HcomDb;
use crate::delivery::APPROVAL_SCRAPE_CLEAR_MS;
use crate::log::{log_info, log_warn};
use crate::pty::PtyTarget;
use crate::pty::screen::ScreenTracker;
use crate::shared::constants::{ST_ACTIVE, ST_BLOCKED};
use crate::tool::Tool;

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

/// Minimum interval between repeated `pty.survey_block_deferred` warnings
/// while blocker publication remains deferred (e.g. another blocker owns
/// the instance row, or the instance is not bound yet).
pub(super) const SURVEY_BLOCK_DEFERRED_WARN_INTERVAL: Duration = Duration::from_secs(30);

/// Minimum interval between repeated `pty.survey_unblock_failed` warnings.
pub(super) const SURVEY_UNBLOCK_FAILED_WARN_INTERVAL: Duration = Duration::from_secs(30);

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
    /// Last instant a `pty.survey_block_deferred` warning was emitted; rate-limits
    /// repeated warnings while publication remains deferred.
    last_block_deferred_warn_at: Option<Instant>,
    /// Last instant a failed blocker-clear warning was emitted.
    last_unblock_failed_warn_at: Option<Instant>,
}

impl SurveyDismissal {
    /// True while a sighting is active, possibly mid-flicker, or while a
    /// published blocker is pending clear — the Unix poll loop caps its
    /// timeout so dismissal timing and unblock retries stay tight.
    pub(super) fn caps_poll(&self) -> bool {
        self.caps_poll_at(Instant::now())
    }

    pub(super) fn caps_poll_at(&self, now: Instant) -> bool {
        self.blocker_published
            || self
                .last_seen_at
                .is_some_and(|last| now.saturating_duration_since(last) < SURVEY_REAPPEAR_GRACE)
    }

    /// Push the next blocker-publication retry out by one cooldown after a
    /// failed/deferred publish, so retries do not hot-loop.
    ///
    /// Returns `true` if a `pty.survey_block_deferred` warning should be emitted
    /// (the initial deferral and rate-limited repeats every [`SURVEY_BLOCK_DEFERRED_WARN_INTERVAL`]).
    pub(super) fn defer_block_retry(&mut self, now: Instant) -> bool {
        self.next_attempt_at = Some(now + SURVEY_DISMISS_RETRY_COOLDOWN);
        let should_warn = self.last_block_deferred_warn_at.is_none_or(|last| {
            now.saturating_duration_since(last) >= SURVEY_BLOCK_DEFERRED_WARN_INTERVAL
        });
        if should_warn {
            self.last_block_deferred_warn_at = Some(now);
        }
        should_warn
    }

    /// Delay another blocker-clear attempt and rate-limit its warning.
    pub(super) fn defer_unblock_retry(&mut self, now: Instant) -> bool {
        self.next_attempt_at = Some(now + SURVEY_DISMISS_RETRY_COOLDOWN);
        let should_warn = self.last_unblock_failed_warn_at.is_none_or(|last| {
            now.saturating_duration_since(last) >= SURVEY_UNBLOCK_FAILED_WARN_INTERVAL
        });
        if should_warn {
            self.last_unblock_failed_warn_at = Some(now);
        }
        should_warn
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
    state.last_block_deferred_warn_at = None;
    if state.blocker_published {
        if state.next_attempt_at.is_some_and(|at| now < at) {
            return SurveyAction::None;
        }
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
    let action = survey_dismissal_tick(
        survey,
        screen.is_antigravity_survey_visible(),
        screen.is_output_stable(APPROVAL_SCRAPE_CLEAR_MS),
        Instant::now(),
    );
    handle_survey_action(survey, instance_name_cfg, current_status, inject, action);
}

fn handle_survey_action(
    survey: &mut SurveyDismissal,
    instance_name_cfg: Option<&str>,
    current_status: &Arc<RwLock<String>>,
    inject: impl FnOnce(&[u8]) -> Result<()>,
    action: SurveyAction,
) {
    match action {
        SurveyAction::Dismiss => {
            if let Err(error) = inject(ANTIGRAVITY_SURVEY_SKIP_KEY) {
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
            if publish_survey_status(true, instance_name_cfg, current_status) {
                survey.blocker_published = true;
                survey.last_block_deferred_warn_at = None;
                log_warn(
                    "native",
                    "pty.survey_block",
                    "Antigravity feedback survey did not dismiss after repeated Skip \
                     attempts; published pty:survey blocker",
                );
            } else if survey.defer_block_retry(Instant::now()) {
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
            if publish_survey_status(false, instance_name_cfg, current_status) {
                survey.blocker_published = false;
                survey.next_attempt_at = None;
                survey.last_unblock_failed_warn_at = None;
            } else if survey.defer_unblock_retry(Instant::now()) {
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

enum SurveyOutcome {
    NotFound,
    Deferred,
    AlreadyOurs,
    NothingToClear,
    Committed {
        status: &'static str,
        event_id: i64,
        data: serde_json::Value,
    },
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
    let instance_name = match config.process_id.as_deref() {
        Some(process_id) => match db.get_process_binding(process_id) {
            Ok(Some(name)) if !name.is_empty() => name,
            Ok(None) | Ok(Some(_)) => {
                // Missing or unbound process: fail closed!
                // An old PTY whose binding disappeared must never fall back to
                // a configured name that might now belong to a reused-name instance.
                return !blocked;
            }
            Err(error) => {
                log_warn(
                    "native",
                    "pty.survey_status_binding_failed",
                    &format!(
                        "Failed to read process binding for process_id={}: {}",
                        process_id, error
                    ),
                );
                return false;
            }
        },
        None => match instance_name_cfg
            .map(str::to_string)
            .or(config.instance_name)
            .filter(|name| !name.is_empty())
        {
            Some(name) => name,
            None => return true,
        },
    };

    let outcome = db.with_write_scope(|| {
        if let Some(process_id) = config.process_id.as_deref() {
            let current_binding = db.get_process_binding(process_id)?;
            if current_binding.as_deref() != Some(&instance_name) {
                // Stale process: binding disappeared or rebound inside write scope.
                // Fail closed so a stale PTY never mutates a reused-name instance.
                return Ok(if blocked {
                    SurveyOutcome::NotFound
                } else {
                    SurveyOutcome::NothingToClear
                });
            }
        }

        let current = match db.get_instance_full(&instance_name)? {
            Some(row) => row,
            None => {
                return Ok(if blocked {
                    SurveyOutcome::NotFound
                } else {
                    SurveyOutcome::NothingToClear
                });
            }
        };
        let survey_blocked = current.status == ST_BLOCKED && current.status_context == "pty:survey";

        if blocked {
            if survey_blocked {
                return Ok(SurveyOutcome::AlreadyOurs);
            }
            if current.status == ST_BLOCKED {
                return Ok(SurveyOutcome::Deferred);
            }
        } else if !survey_blocked {
            return Ok(SurveyOutcome::NothingToClear);
        }

        let (status, context, detail) = if blocked {
            (
                ST_BLOCKED,
                "pty:survey",
                "Antigravity feedback survey did not dismiss after repeated Skip attempts; \
                 answer it (0 = Skip) to resume the worker",
            )
        } else {
            (ST_ACTIVE, "pty:survey_cleared", "")
        };

        db.set_status(&instance_name, status, context)?;
        db.conn().execute(
            "UPDATE instances SET status_detail = ? WHERE name = ?",
            rusqlite::params![detail, &instance_name],
        )?;

        let mut data = serde_json::json!({
            "status": status,
            "context": context,
            "position": current.last_event_id,
        });
        if !detail.is_empty() {
            data["detail"] = serde_json::json!(detail);
        }

        let event_id = db.insert_event_row("status", &instance_name, &data, None)?;
        Ok(SurveyOutcome::Committed {
            status,
            event_id,
            data,
        })
    });

    match outcome {
        Ok(SurveyOutcome::Committed {
            status,
            event_id,
            data,
        }) => {
            crate::notify::wake(&db, &instance_name, crate::notify::WakeKind::DELIVERY_LOOPS);
            db.after_event_logged(event_id, "status", &instance_name, &data);
            if let Ok(mut shared_status) = current_status.write() {
                *shared_status = status.to_string();
            }
            true
        }
        Ok(SurveyOutcome::AlreadyOurs) => {
            if let Ok(mut shared_status) = current_status.write() {
                *shared_status = ST_BLOCKED.to_string();
            }
            true
        }
        Ok(SurveyOutcome::NothingToClear) => true,
        Ok(SurveyOutcome::Deferred) => false,
        Ok(SurveyOutcome::NotFound) => false,
        Err(error) => {
            log_warn(
                "native",
                "pty.survey_status_failed",
                &format!(
                    "Failed to publish survey blocked={} for {}: {}",
                    blocked, instance_name, error
                ),
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::constants::ST_LISTENING;
    use serial_test::serial;

    #[test]
    fn survey_skip_key_is_byte_zero_without_enter() {
        assert_eq!(ANTIGRAVITY_SURVEY_SKIP_KEY, b"0");
        assert!(!ANTIGRAVITY_SURVEY_SKIP_KEY.contains(&b'\n'));
        assert!(!ANTIGRAVITY_SURVEY_SKIP_KEY.contains(&b'\r'));
    }

    #[test]
    fn survey_tick_sighting_progression() {
        let t0 = Instant::now();
        let mut survey = SurveyDismissal::default();

        // 1. Initial stable sighting -> Dismiss #1
        assert_eq!(
            survey_dismissal_tick(&mut survey, true, true, t0),
            SurveyAction::Dismiss
        );
        assert_eq!(survey.attempts, 1);
        assert!(survey.caps_poll_at(t0));

        // 2. Cooldown active -> None
        let t1 = t0 + Duration::from_millis(500);
        assert_eq!(
            survey_dismissal_tick(&mut survey, true, true, t1),
            SurveyAction::None
        );

        // 3. Output unstable -> None
        let t2 = t0 + SURVEY_DISMISS_RETRY_COOLDOWN;
        assert_eq!(
            survey_dismissal_tick(&mut survey, true, false, t2),
            SurveyAction::None
        );

        // 4. Stable after cooldown -> Dismiss #2
        assert_eq!(
            survey_dismissal_tick(&mut survey, true, true, t2),
            SurveyAction::Dismiss
        );
        assert_eq!(survey.attempts, 2);

        // 5. Dismiss #3
        let t3 = t2 + SURVEY_DISMISS_RETRY_COOLDOWN;
        assert_eq!(
            survey_dismissal_tick(&mut survey, true, true, t3),
            SurveyAction::Dismiss
        );
        assert_eq!(survey.attempts, 3);

        // 6. Max attempts reached; final attempt cooldown active -> None
        let t4 = t3 + Duration::from_millis(500);
        assert_eq!(
            survey_dismissal_tick(&mut survey, true, true, t4),
            SurveyAction::None
        );

        // 7. Max attempts reached and cooldown elapsed -> Block
        let t5 = t3 + SURVEY_DISMISS_RETRY_COOLDOWN;
        assert_eq!(
            survey_dismissal_tick(&mut survey, true, true, t5),
            SurveyAction::Block
        );

        // 8. Caller latches publication -> Block action stops re-emitting
        survey.blocker_published = true;
        assert_eq!(
            survey_dismissal_tick(&mut survey, true, true, t5),
            SurveyAction::None
        );

        // 9. Survey vanishes, but within grace period -> None (flicker protection)
        let t6 = t5 + Duration::from_millis(500);
        assert_eq!(
            survey_dismissal_tick(&mut survey, false, true, t6),
            SurveyAction::None
        );
        assert!(survey.caps_poll_at(t6));

        // 10. Grace period expires -> Unblock
        let t7 = t5 + SURVEY_REAPPEAR_GRACE;
        assert_eq!(
            survey_dismissal_tick(&mut survey, false, true, t7),
            SurveyAction::Unblock
        );
    }

    #[test]
    #[serial]
    fn publish_survey_status_preserves_foreign_and_replacement_approval() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let shared_status = Arc::new(RwLock::new(ST_BLOCKED.to_string()));

        // Seed instance as blocked:approval
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, status_context, status_detail, created_at, last_event_id)
                 VALUES ('zara', 'antigravity', 'blocked', 'approval', 'Run command permission', 0, 0)",
                [],
            )
            .unwrap();

        // 1. Survey publish cannot overwrite hook approval
        assert!(!publish_survey_status(true, Some("zara"), &shared_status));
        let row = db.get_instance_full("zara").unwrap().unwrap();
        assert_eq!(row.status, ST_BLOCKED);
        assert_eq!(row.status_context, "approval");
        assert_eq!(row.status_detail, "Run command permission");

        // 2. Unblock when row is approval (not survey) does not mutate row or clear approval
        assert!(publish_survey_status(false, Some("zara"), &shared_status));
        let row = db.get_instance_full("zara").unwrap().unwrap();
        assert_eq!(row.status, ST_BLOCKED);
        assert_eq!(row.status_context, "approval");
        assert_eq!(row.status_detail, "Run command permission");
        assert_eq!(*shared_status.read().unwrap(), ST_BLOCKED);
    }

    #[test]
    #[serial]
    fn publish_survey_status_event_insert_failure_rolls_back_status_mutation() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let shared_status = Arc::new(RwLock::new(ST_LISTENING.to_string()));

        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, status_context, status_detail, created_at, last_event_id)
                 VALUES ('bora', 'antigravity', 'listening', 'idle', 'original_detail', 0, 0)",
                [],
            )
            .unwrap();

        // Trigger fails status event inserts
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_status_event BEFORE INSERT ON events
                 WHEN NEW.type = 'status'
                 BEGIN
                     SELECT RAISE(ABORT, 'forced status event failure');
                 END;",
            )
            .unwrap();

        // Attempt to publish survey blocker: event insert will fail inside write scope
        assert!(!publish_survey_status(true, Some("bora"), &shared_status));

        // Status and detail mutations rolled back: row remains listening:idle with original_detail
        let row = db.get_instance_full("bora").unwrap().unwrap();
        assert_eq!(row.status, ST_LISTENING);
        assert_eq!(row.status_context, "idle");
        assert_eq!(row.status_detail, "original_detail");
        assert_eq!(*shared_status.read().unwrap(), ST_LISTENING);
    }

    #[test]
    #[serial]
    fn publish_survey_status_unblock_refuses_replacement_approval_and_preserves_shared_title() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        unsafe {
            std::env::remove_var("HCOM_PROCESS_ID");
        }
        crate::config::Config::reset();
        crate::config::Config::init();
        let db = HcomDb::open().unwrap();
        let shared_status = Arc::new(RwLock::new(ST_LISTENING.to_string()));

        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, status_context, status_detail, created_at, last_event_id)
                 VALUES ('kora', 'antigravity', 'listening', 'idle', '', 0, 0)",
                [],
            )
            .unwrap();

        // First, publish survey block
        assert!(publish_survey_status(true, Some("kora"), &shared_status));
        assert_eq!(*shared_status.read().unwrap(), ST_BLOCKED);
        let row = db.get_instance_full("kora").unwrap().unwrap();
        assert!(row.status_detail.contains("Antigravity feedback survey"));

        // A newer approval arrives while survey was blocked (replacement approval)
        db.conn()
            .execute(
                "UPDATE instances SET status = 'blocked', status_context = 'approval',
                 status_detail = 'Run this command?' WHERE name = 'kora'",
                [],
            )
            .unwrap();

        // Now survey dismisser attempts to unblock:
        // Must refuse to clear the replacement approval!
        assert!(publish_survey_status(false, Some("kora"), &shared_status));

        // Verify the approval row was NOT overwritten or cleared, and detail is untouched
        let row = db.get_instance_full("kora").unwrap().unwrap();
        assert_eq!(row.status, ST_BLOCKED);
        assert_eq!(row.status_context, "approval");
        assert_eq!(row.status_detail, "Run this command?");
    }

    #[test]
    #[serial]
    fn publish_survey_status_vanished_row_releases_controller() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let _db = HcomDb::open().unwrap();
        let shared_status = Arc::new(RwLock::new(ST_LISTENING.to_string()));

        // Rising edge on missing instance fails (returns false to retry)
        assert!(!publish_survey_status(
            true,
            Some("vanished"),
            &shared_status
        ));

        // Falling edge on missing instance succeeds (returns true so controller is released)
        assert!(publish_survey_status(
            false,
            Some("vanished"),
            &shared_status
        ));
    }

    #[test]
    #[serial]
    fn publish_survey_status_refuses_stale_process_when_name_reused() {
        struct ProcessIdGuard;
        impl Drop for ProcessIdGuard {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("HCOM_PROCESS_ID");
                }
                crate::config::Config::reset();
                crate::config::Config::init();
            }
        }

        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let shared_status = Arc::new(RwLock::new(ST_LISTENING.to_string()));

        // Simulate an existing instance with reused name in DB
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, tool, status, status_context, status_detail, status_time, created_at)
                 VALUES ('zara', 'antigravity', 'listening', 'idle', '', 0, 0)",
                [],
            )
            .unwrap();

        // 1. Process ID is configured (launched process), but process_bindings has NO binding for it.
        // It must NOT fall back to instance_name_cfg ("zara") and mutate the reused-name row!
        let _pid_guard = ProcessIdGuard;
        crate::config::Config::reset();
        unsafe {
            std::env::set_var("HCOM_PROCESS_ID", "stale-pid-999");
        }
        crate::config::Config::init();

        // Rising edge must fail closed (return false) without mutating the reused row
        assert!(!publish_survey_status(true, Some("zara"), &shared_status));
        let row = db.get_instance_full("zara").unwrap().unwrap();
        assert_eq!(row.status, ST_LISTENING);
        assert_eq!(row.status_context, "idle");
        assert_eq!(*shared_status.read().unwrap(), ST_LISTENING);

        // Falling edge must return true (handled so controller unlatches) without mutating the reused row
        assert!(publish_survey_status(false, Some("zara"), &shared_status));
        let row = db.get_instance_full("zara").unwrap().unwrap();
        assert_eq!(row.status, ST_LISTENING);
        assert_eq!(row.status_context, "idle");
        assert_eq!(*shared_status.read().unwrap(), ST_LISTENING);

        // 2. Now bind stale-pid-999 to "other-instance", not "zara"
        db.conn()
            .execute(
                "INSERT INTO process_bindings (process_id, instance_name, updated_at) VALUES ('stale-pid-999', 'other-instance', 0.0)",
                [],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, tool, status, status_context, status_detail, status_time, created_at)
                 VALUES ('other-instance', 'antigravity', 'listening', 'idle', '', 0, 0)",
                [],
            )
            .unwrap();

        // Calling with instance_name_cfg Some("zara") must NOT touch "zara"
        assert!(publish_survey_status(true, Some("zara"), &shared_status));
        let zara_row = db.get_instance_full("zara").unwrap().unwrap();
        assert_eq!(zara_row.status, ST_LISTENING, "zara must not be touched");
        let other_row = db.get_instance_full("other-instance").unwrap().unwrap();
        assert_eq!(
            other_row.status, ST_BLOCKED,
            "bound other-instance was updated"
        );
    }
}
