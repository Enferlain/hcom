//! Terminal outcome scanning for correlated worker result waits.
//!
//! `hcom events --wait --result-from` owns one atomic wait for a delegated
//! worker attempt. Besides the authoritative result message and the
//! stopped-generation transcript recovery, the wait must also terminate on
//! states in which no result can ever arrive: a typed actionable blocker, a
//! launch failure, or a worker that stopped without delivering its report.
//!
//! Every scan here is anchored at the caller's pre-launch durable cursor
//! (`--after-id`), not at a subscription or at the wait's own start. A
//! transition that fired between launch readiness and wait registration is
//! therefore still discovered on the first poll — the race that motivated this
//! module. Outcome payloads preserve the correlation tuple (worker generation,
//! workflow thread, attempt cursor) plus blocker evidence and recovery
//! guidance so a coordinator can act without re-deriving context.
//!
//! Launch-scoped life events (`launch_failed`, `launch_blocked`) carry no
//! generation snapshot; they are correlated by instance name within the
//! attempt cursor boundary, which the arm-time generation resolution in
//! `commands::events` already pins to a single generation.

use serde_json::{Value, json};

use crate::core::launch_status::BlockedKind;
use crate::db::HcomDb;
use crate::messages::sender_instance_key;
use crate::shared::{ST_ACTIVE, ST_BLOCKED, ST_LISTENING};

/// Wait terminated by a typed actionable blocker (worker needs attention).
pub(crate) const RESULT_BLOCKED_EXIT: i32 = 4;
/// Wait terminated after a worker stops without a recoverable result.
pub(crate) const RESULT_UNAVAILABLE_EXIT: i32 = 3;
/// Wait terminated by a launch failure recorded after the attempt cursor.
pub(crate) const RESULT_LAUNCH_FAILED_EXIT: i32 = 5;

/// Borrowed correlation tuple identifying one worker attempt.
pub(crate) struct OutcomeWait<'a> {
    pub worker: &'a str,
    pub generation: &'a str,
    pub thread: &'a str,
    pub attempt_after_id: i64,
}

/// A terminal non-result outcome: process exit code plus structured payload.
pub(crate) struct TerminalOutcome {
    pub exit_code: i32,
    pub payload: Value,
}

/// Canonical blocker kind for a blocked status context.
///
/// The empty context renders as "permission needed" in status descriptions, so
/// it maps to the same actionable `approval` kind as the explicit approval
/// contexts. Unknown contexts pass through unchanged — new blocker sources
/// stay observable instead of collapsing into `unknown`.
fn status_blocker_kind(context: &str) -> String {
    match context {
        "" | "approval" | "pty:approval" => "approval".to_string(),
        "pty:survey" => "survey".to_string(),
        "elicitation" => "elicitation".to_string(),
        other => other.to_string(),
    }
}

/// Correlation fields embedded in every structured outcome payload.
fn correlation_fields(wait: &OutcomeWait<'_>) -> Value {
    json!({
        "worker": wait.worker,
        "generation": wait.generation,
        "thread": wait.thread,
        "attempt_after_id": wait.attempt_after_id,
    })
}

/// Merge the canonical correlation tuple into a structured payload for wait
/// terminal outcomes.
fn merge_correlation(payload: &mut Value, wait: &OutcomeWait<'_>) {
    if let (Some(target), Some(fields)) = (
        payload.as_object_mut(),
        correlation_fields(wait).as_object(),
    ) {
        for (key, value) in fields {
            target.insert(key.clone(), value.clone());
        }
    }
}

/// Recovery guidance for a blocked worker, keyed by blocker kind.
fn blocked_recovery_guidance(wait: &OutcomeWait<'_>, kind: &str) -> String {
    let rearm = format!(
        "then re-arm this wait with `hcom events --wait <SEC> --after-id {} --thread {} --result-from {}`",
        wait.attempt_after_id, wait.thread, wait.worker
    );
    match kind {
        "approval" | "elicitation" => format!(
            "answer or dismiss the pending prompt with `hcom term {}` (context: `hcom transcript {} --last 5`), {rearm}",
            wait.worker, wait.worker
        ),
        "survey" => format!(
            "dismiss the non-task survey prompt with `hcom term {}`, {rearm}",
            wait.worker
        ),
        _ => format!(
            "unblock worker {} (`hcom term {}` shows its current state), {rearm}",
            wait.worker, wait.worker
        ),
    }
}

/// Deadline payload. Keeps the legacy `timed_out: true` marker (scripts grep
/// for it) and adds the correlation tuple when the wait was correlated.
pub(crate) fn deadline_payload(wait: Option<&OutcomeWait<'_>>) -> Value {
    let Some(wait) = wait else {
        return json!({"timed_out": true});
    };
    let mut payload = json!({
        "timed_out": true,
        "outcome": "deadline",
        "recovery": format!(
            "continue waiting with `hcom events --wait <SEC> --after-id {} --thread {} --result-from {}`; inspect the worker with `hcom list {} --json`, `hcom term {}`, or `hcom transcript {} --last 5 --full`",
            wait.attempt_after_id, wait.thread, wait.worker, wait.worker, wait.worker, wait.worker
        ),
    });
    merge_correlation(&mut payload, wait);
    payload
}

/// Stopped-without-result payload. Keeps the legacy `result_unavailable: true`
/// marker and adds the correlation tuple and recovery guidance.
pub(crate) fn unavailable_payload(wait: &OutcomeWait<'_>, reason: &str) -> Value {
    let mut payload = json!({
        "result_unavailable": true,
        "outcome": "stopped_without_result",
        "reason": reason,
        "recovery": format!(
            "the worker generation stopped without delivering the correlated report; inspect its transcript with `hcom transcript {} --last 5 --full` or launch a new attempt from a fresh pre-launch cursor",
            wait.worker
        ),
    });
    merge_correlation(&mut payload, wait);
    payload
}

struct LaunchLifeEvent {
    id: i64,
    timestamp: String,
    action: String,
    reason: Option<String>,
    detail: Option<String>,
    batch_id: Option<String>,
    blocked_kind: Option<String>,
    evidence: Option<String>,
}

struct BlockedStatusEvent {
    id: i64,
    timestamp: String,
    context: Option<String>,
    detail: Option<String>,
}

/// Return the stop event for the exact correlated generation, if it has
/// already stopped. Once this exists, launch/status observations under the
/// reusable display name are historical; stopped-result recovery owns the
/// terminal decision.
fn correlated_stop_event_id(db: &HcomDb, wait: &OutcomeWait<'_>) -> Result<Option<i64>, String> {
    let mut statement = db
        .conn()
        .prepare(
            "SELECT id, data FROM events
             WHERE id > ?1 AND instance = ?2 AND type = 'life'
               AND json_valid(data)
               AND json_extract(data, '$.action') = 'stopped'
               AND json_extract(data, '$.placeholder') IS NOT TRUE
             ORDER BY id",
        )
        .map_err(|error| format!("failed to inspect correlated stop events: {error}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![wait.attempt_after_id, wait.worker],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|error| format!("failed to inspect correlated stop events: {error}"))?;

    for row in rows {
        let (id, data) =
            row.map_err(|error| format!("failed to inspect correlated stop row: {error}"))?;
        let Ok(data) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        let Some(snapshot) = data.get("snapshot") else {
            continue;
        };
        if sender_instance_key(wait.worker, snapshot).as_deref() == Some(wait.generation) {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

/// Whether a later event demonstrates that the worker progressed beyond a
/// launch failure/blocker. This keeps resolved launch observations resolved
/// even after a later lifecycle transition changes or removes the live row.
fn progressed_after(db: &HcomDb, wait: &OutcomeWait<'_>, event_id: i64) -> Result<bool, String> {
    db.conn()
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM events
                 WHERE id > ?1 AND instance = ?2 AND (
                     (type = 'life' AND json_valid(data)
                       AND json_extract(data, '$.action') = 'ready')
                     OR
                     (type = 'status' AND json_valid(data)
                       AND json_extract(data, '$.status') IN
                           ('active', 'listening', 'launching'))
                 )
             )",
            rusqlite::params![event_id, wait.worker],
            |row| row.get(0),
        )
        .map_err(|error| format!("failed to inspect post-launch progress: {error}"))
}

/// Query launch-scoped terminal life events for the worker after the cursor.
fn launch_life_events(db: &HcomDb, wait: &OutcomeWait<'_>) -> Result<Vec<LaunchLifeEvent>, String> {
    let mut statement = db
        .conn()
        .prepare(
            "SELECT id, timestamp,
                    json_extract(data, '$.action'),
                    json_extract(data, '$.reason'),
                    json_extract(data, '$.detail'),
                    json_extract(data, '$.batch_id'),
                    json_extract(data, '$.blocked_kind'),
                    json_extract(data, '$.evidence')
             FROM events
             WHERE id > ?1 AND instance = ?2 AND type = 'life'
               AND json_valid(data)
               AND json_extract(data, '$.action') IN ('launch_failed', 'launch_blocked')
             ORDER BY id",
        )
        .map_err(|error| format!("failed to inspect launch outcome events: {error}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![wait.attempt_after_id, wait.worker],
            |row| {
                Ok(LaunchLifeEvent {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    action: row.get(2)?,
                    reason: row.get(3).ok(),
                    detail: row.get(4).ok(),
                    batch_id: row.get(5).ok(),
                    blocked_kind: row.get(6).ok(),
                    evidence: row.get(7).ok(),
                })
            },
        )
        .map_err(|error| format!("failed to inspect launch outcome events: {error}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to inspect launch outcome rows: {error}"))
}

/// Query the newest blocked status event for the worker after the cursor.
fn latest_blocked_status_event(
    db: &HcomDb,
    wait: &OutcomeWait<'_>,
) -> Result<Option<BlockedStatusEvent>, String> {
    let mut statement = db
        .conn()
        .prepare(
            "SELECT id, timestamp,
                    json_extract(data, '$.context'),
                    json_extract(data, '$.detail')
             FROM events
             WHERE id > ?1 AND instance = ?2 AND type = 'status'
               AND json_valid(data)
               AND json_extract(data, '$.status') = 'blocked'
             ORDER BY id DESC LIMIT 1",
        )
        .map_err(|error| format!("failed to inspect blocked status events: {error}"))?;
    let mut rows = statement
        .query_map(
            rusqlite::params![wait.attempt_after_id, wait.worker],
            |row| {
                Ok(BlockedStatusEvent {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    context: row.get(2).ok(),
                    detail: row.get(3).ok(),
                })
            },
        )
        .map_err(|error| format!("failed to inspect blocked status events: {error}"))?;
    rows.next()
        .transpose()
        .map_err(|error| format!("failed to inspect blocked status row: {error}"))
}

/// Evidence for one blocker observation, mirroring the source event fields.
struct BlockerEvidence {
    event_id: i64,
    timestamp: String,
    kind: String,
    context: Option<String>,
    evidence: Option<String>,
    detail: Option<String>,
    reason: Option<String>,
}

fn blocker_payload(wait: &OutcomeWait<'_>, blocker: &BlockerEvidence) -> TerminalOutcome {
    let mut payload = json!({
        "result_blocked": true,
        "outcome": "blocked",
        "event_id": blocker.event_id,
        "ts": blocker.timestamp,
        "kind": blocker.kind,
        "recovery": blocked_recovery_guidance(wait, &blocker.kind),
    });
    if let Some(object) = payload.as_object_mut() {
        if let Some(context) = blocker.context.as_deref().filter(|value| !value.is_empty()) {
            object.insert("context".into(), json!(context));
        }
        if let Some(evidence) = blocker
            .evidence
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            object.insert("evidence".into(), json!(evidence));
        }
        if let Some(detail) = blocker.detail.as_deref().filter(|value| !value.is_empty()) {
            object.insert("detail".into(), json!(detail));
        }
        if let Some(reason) = blocker.reason.as_deref().filter(|value| !value.is_empty()) {
            object.insert("reason".into(), json!(reason));
        }
    }
    merge_correlation(&mut payload, wait);
    TerminalOutcome {
        exit_code: RESULT_BLOCKED_EXIT,
        payload,
    }
}

fn launch_failed_payload(wait: &OutcomeWait<'_>, event: &LaunchLifeEvent) -> TerminalOutcome {
    let mut payload = json!({
        "result_launch_failed": true,
        "outcome": "launch_failed",
        "event_id": event.id,
        "ts": event.timestamp,
        "recovery": format!(
            "the attempt failed before producing a result; check ~/.hcom/.tmp/logs/background_*.log or `hcom list {} -v`, fix the cause, and launch a new attempt from a fresh pre-launch cursor",
            wait.worker
        ),
    });
    if let Some(object) = payload.as_object_mut() {
        if let Some(batch_id) = event.batch_id.as_deref().filter(|value| !value.is_empty()) {
            object.insert("batch_id".into(), json!(batch_id));
        }
        if let Some(detail) = event.detail.as_deref().filter(|value| !value.is_empty()) {
            object.insert("detail".into(), json!(detail));
        }
        if let Some(reason) = event.reason.as_deref().filter(|value| !value.is_empty()) {
            object.insert("reason".into(), json!(reason));
        }
    }
    merge_correlation(&mut payload, wait);
    TerminalOutcome {
        exit_code: RESULT_LAUNCH_FAILED_EXIT,
        payload,
    }
}

/// Scan for a terminal non-result outcome for the correlated attempt.
///
/// Precedence:
///
/// 1. a stopped event for the exact correlated generation defers to the
///    stopped-result recovery path; later events may belong to a reused name.
/// 2. `launch_failed` life event — the attempt is dead unless later readiness
///    evidence contradicts it.
/// 3. unresolved `launch_blocked` life event — typed launch blocker, ignored
///    once the worker demonstrably progressed past it (active/listening row).
/// 4. blocked status event that is still current — the live instance row is
///    still blocked and belongs to the correlated generation, so the blocker
///    is actionable rather than historical.
///
/// Returns `Ok(None)` when no terminal state is currently observable; the
/// caller keeps waiting for the result, recovery, or deadline.
pub(crate) fn scan_terminal_outcome(
    db: &HcomDb,
    wait: &OutcomeWait<'_>,
) -> Result<Option<TerminalOutcome>, String> {
    if correlated_stop_event_id(db, wait)?.is_some() {
        return Ok(None);
    }

    let launch_events = launch_life_events(db, wait)?;
    let mut unresolved_launch_blocked: Option<&LaunchLifeEvent> = None;
    for event in &launch_events {
        if event.action == "launch_failed" && !progressed_after(db, wait, event.id)? {
            return Ok(Some(launch_failed_payload(wait, event)));
        }
        if event.action == "launch_blocked" && unresolved_launch_blocked.is_none() {
            unresolved_launch_blocked = Some(event);
        }
    }

    let row = db
        .get_instance(wait.worker)
        .map_err(|error| format!("failed to read the correlated worker row: {error}"))?;
    let generation_matches = row
        .as_ref()
        .and_then(|data| sender_instance_key(wait.worker, data))
        .as_deref()
        == Some(wait.generation);
    let row_is_progressed = generation_matches
        && row.as_ref().is_some_and(|data| {
            matches!(
                data.get("status").and_then(Value::as_str),
                Some(ST_ACTIVE) | Some(ST_LISTENING) | Some("launching")
            )
        });

    if let Some(event) = unresolved_launch_blocked
        && !row_is_progressed
        && !progressed_after(db, wait, event.id)?
    {
        let kind = event
            .blocked_kind
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(BlockedKind::from_str)
            .map(|kind| kind.as_str().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        return Ok(Some(blocker_payload(
            wait,
            &BlockerEvidence {
                event_id: event.id,
                timestamp: event.timestamp.clone(),
                kind,
                context: Some("launch_blocked".to_string()),
                evidence: event.evidence.clone(),
                detail: event.detail.clone(),
                reason: event.reason.clone(),
            },
        )));
    }

    // A blocked status only terminates the wait while it is still current and
    // still belongs to the correlated generation. A blocker that fired and
    // cleared (approval answered, survey dismissed) must not end the attempt,
    // and a reused worker name must not satisfy the previous generation's wait.
    if let Some(event) = latest_blocked_status_event(db, wait)?
        && row
            .as_ref()
            .is_some_and(|data| data.get("status").and_then(Value::as_str) == Some(ST_BLOCKED))
        && generation_matches
    {
        let context = event.context.clone().unwrap_or_default();
        let kind = status_blocker_kind(&context);
        return Ok(Some(blocker_payload(
            wait,
            &BlockerEvidence {
                event_id: event.id,
                timestamp: event.timestamp.clone(),
                kind,
                context: Some(context),
                evidence: event.detail.clone(),
                detail: event.detail.clone(),
                reason: None,
            },
        )));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_blocker_kind_maps_known_contexts() {
        assert_eq!(status_blocker_kind(""), "approval");
        assert_eq!(status_blocker_kind("approval"), "approval");
        assert_eq!(status_blocker_kind("pty:approval"), "approval");
        assert_eq!(status_blocker_kind("pty:survey"), "survey");
        assert_eq!(status_blocker_kind("elicitation"), "elicitation");
        assert_eq!(status_blocker_kind("custom:gate"), "custom:gate");
    }

    #[test]
    fn deadline_payload_keeps_legacy_marker_without_correlation() {
        let payload = deadline_payload(None);
        assert_eq!(payload["timed_out"], true);
        assert!(payload.get("outcome").is_none());
    }

    #[test]
    fn deadline_payload_carries_correlation_and_recovery() {
        let wait = OutcomeWait {
            worker: "kuma",
            generation: "kuma@1000.000000",
            thread: "glm-1788727501-105060",
            attempt_after_id: 42,
        };
        let payload = deadline_payload(Some(&wait));
        assert_eq!(payload["timed_out"], true);
        assert_eq!(payload["outcome"], "deadline");
        assert_eq!(payload["worker"], "kuma");
        assert_eq!(payload["generation"], "kuma@1000.000000");
        assert_eq!(payload["thread"], "glm-1788727501-105060");
        assert_eq!(payload["attempt_after_id"], 42);
        let recovery = payload["recovery"].as_str().unwrap();
        assert!(recovery.contains("--after-id 42"));
        assert!(recovery.contains("--result-from kuma"));
        // Scripts grep for the compact marker; keep it byte-compatible.
        assert!(
            serde_json::to_string(&payload)
                .unwrap()
                .contains("\"timed_out\":true")
        );
    }

    #[test]
    fn unavailable_payload_keeps_legacy_marker_and_adds_structure() {
        let wait = OutcomeWait {
            worker: "kuma",
            generation: "kuma@1000.000000",
            thread: "t",
            attempt_after_id: 7,
        };
        let payload = unavailable_payload(&wait, "provider 'codex' does not support recovery");
        assert_eq!(payload["result_unavailable"], true);
        assert_eq!(payload["outcome"], "stopped_without_result");
        assert_eq!(payload["generation"], "kuma@1000.000000");
        assert_eq!(payload["attempt_after_id"], 7);
        assert_eq!(
            payload["reason"],
            "provider 'codex' does not support recovery"
        );
        assert!(
            payload["recovery"]
                .as_str()
                .unwrap()
                .contains("hcom transcript kuma")
        );
    }
}
