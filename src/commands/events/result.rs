//! Correlated result waits and fail-closed stopped-provider recovery.
use super::*;
use crate::core::filters::validate_type_constraints;
use crate::messages::sender_instance_key;
use std::time::Instant;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::events) struct ResultCorrelation {
    pub(super) exact_worker: String,
    pub(super) instance_key: String,
    pub(super) thread: String,
    pub(super) after_id: i64,
}

const RESULT_RECOVERY_GRACE: Duration = Duration::from_secs(2);
const RESULT_RECOVERY_RETRY: Duration = Duration::from_millis(200);

/// Apply the fail-closed result-correlation contract.
pub(in crate::commands::events) fn apply_result_correlation(
    db: &HcomDb,
    args: &EventsArgs,
    filters: &mut HashMap<String, Vec<String>>,
) -> Result<Option<ResultCorrelation>, String> {
    let Some(worker) = args.result_from.as_deref() else {
        return Ok(None);
    };

    if args.after_id.is_none() {
        return Err("--result-from requires --after-id captured before launch".to_string());
    }
    if args.filters.thread.len() != 1 {
        return Err("--result-from requires exactly one --thread workflow ID".to_string());
    }
    if filters.keys().any(|key| key != "thread") {
        return Err(
            "--result-from cannot be combined with event filters other than --thread".to_string(),
        );
    }
    if args.sql.is_some() {
        return Err("--result-from cannot be combined with --sql".to_string());
    }

    let after_id = args.after_id.expect("checked above");
    let generation = discover_worker_generation(db, "--result-from", worker, after_id)?;
    let exact_worker = generation.exact_worker;
    let instance_key = generation.instance_key;
    filters.insert("type".into(), vec!["message".into()]);
    filters.insert("from".into(), vec![exact_worker.clone()]);
    filters.insert("intent".into(), vec!["inform".into()]);
    filters.insert("sender_instance_key".into(), vec![instance_key.clone()]);
    validate_type_constraints(filters)?;
    Ok(Some(ResultCorrelation {
        exact_worker,
        instance_key,
        thread: args.filters.thread[0].clone(),
        after_id,
    }))
}

/// Recover a terminal provider response only after the exact correlated worker
/// generation has stopped.
pub(super) fn recover_correlated_stopped_result(
    db: &HcomDb,
    correlation: &ResultCorrelation,
) -> Result<Option<Value>, String> {
    let mut statement = db
        .conn()
        .prepare(
            "SELECT id, timestamp, instance, data FROM events
             WHERE id > ?1 AND instance = ?2 AND type = 'life'
               AND json_valid(data)
               AND json_extract(data, '$.action') = 'stopped'
               AND json_extract(data, '$.placeholder') IS NOT TRUE
               AND json_extract(data, '$.soft') IS NOT TRUE
             ORDER BY id",
        )
        .map_err(|error| format!("failed to inspect stopped result worker: {error}"))?;
    let rows = statement
        .query_map(
            rusqlite::params![correlation.after_id, correlation.exact_worker],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .map_err(|error| format!("failed to inspect stopped result worker: {error}"))?;

    for row in rows {
        let (id, timestamp, instance, data) =
            row.map_err(|error| format!("failed to inspect stopped result row: {error}"))?;
        let Ok(data) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        if data.get("placeholder").and_then(Value::as_bool) == Some(true)
            || data.get("soft").and_then(Value::as_bool) == Some(true)
        {
            continue;
        }
        let Some(snapshot) = data.get("snapshot") else {
            continue;
        };
        if sender_instance_key(&instance, snapshot).as_deref()
            != Some(correlation.instance_key.as_str())
        {
            continue;
        }

        let tool = snapshot
            .get("tool")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(tool, "antigravity" | "claude") {
            return Err(format!(
                "provider '{}' does not support stopped-result recovery",
                if tool.is_empty() { "unknown" } else { tool }
            ));
        }
        let transcript_path = snapshot
            .get("transcript_path")
            .and_then(Value::as_str)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| {
                format!(
                    "{} worker stopped without transcript metadata",
                    correlation.exact_worker
                )
            })?;
        let session_id = snapshot
            .get("session_id")
            .and_then(Value::as_str)
            .filter(|session| !session.is_empty());
        let recovered = crate::provider_result::recover_provider_result(
            tool,
            transcript_path,
            session_id,
            &correlation.thread,
        )?;

        return Ok(Some(json!({
            "id": id,
            "ts": timestamp,
            "type": "message",
            "instance": instance,
            "data": {
                "from": instance,
                "intent": "inform",
                "text": recovered.text,
                "message": recovered.text,
                "thread": correlation.thread,
                "sender_instance_key": correlation.instance_key,
                "recovered": true,
                "provenance": {
                    "kind": "transcript_recovery",
                    "provider": recovered.provider,
                    "evidence": recovered.evidence,
                    "session_id": session_id,
                    "transcript_path": transcript_path,
                    "attempt_after_id": correlation.after_id,
                }
            }
        })));
    }

    Ok(None)
}

// ── Event Streamlining ──────────────────────────────────────────────────

/// Remove bloat fields from event for ~35% token reduction.
///
/// Preserves fields used in active filters.
/// Wait mode: block until matching event or timeout.
pub(in crate::commands::events) struct ResultWaitOptions<'a> {
    pub(in crate::commands::events) after_id: Option<i64>,
    pub(in crate::commands::events) full_output: bool,
    pub(in crate::commands::events) filters: &'a HashMap<String, Vec<String>>,
    pub(in crate::commands::events) instance_name: Option<&'a str>,
    pub(in crate::commands::events) result_correlation: Option<&'a ResultCorrelation>,
}

pub(in crate::commands::events) fn wait_for_result(
    db: &HcomDb,
    filter_query: &str,
    wait_timeout: u64,
    options: ResultWaitOptions<'_>,
) -> i32 {
    let ResultWaitOptions {
        after_id,
        full_output,
        filters,
        instance_name,
        result_correlation,
    } = options;

    let mut listener = EventListener::new(
        db,
        EventListenerOptions {
            after_id,
            instance_name,
            endpoint_kind: EVENTS_WAIT_ENDPOINT_KIND,
        },
    );

    let start = Instant::now();
    let mut recovery_observed_event_id =
        result_correlation.map_or(listener.cursor(), |correlation| correlation.after_id);
    let mut recovery_error_since: Option<Instant> = None;
    let mut next_recovery_retry = start;
    let mut recovery_event_pending = false;
    let mut next_recovery_scan = start;
    let mut unread_preview_shown = false;

    let outcome_wait =
        result_correlation.map(|correlation| crate::core::result_wait::OutcomeWait {
            worker: &correlation.exact_worker,
            generation: &correlation.instance_key,
            thread: &correlation.thread,
            attempt_after_id: correlation.after_id,
        });

    loop {
        // Query for new matching events
        match listener.query_next_event(filter_query) {
            Ok(Some(event)) => {
                let mut output = if full_output {
                    event
                } else {
                    streamline_event(&event, filters)
                };
                if output.get("type").and_then(Value::as_str) == Some("message")
                    && let Some(data) = output.get_mut("data").and_then(Value::as_object_mut)
                    && let Some(text) = data.get("text").cloned()
                {
                    data.entry("message").or_insert(text);
                }
                println!("{}", serde_json::to_string(&output).unwrap_or_default());
                break 0;
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("Error in SQL WHERE clause: {e}");
                break 2;
            }
        }

        // Terminal non-result outcomes (actionable blocker, launch failure).
        if let Some(wait) = outcome_wait.as_ref() {
            match crate::core::result_wait::scan_terminal_outcome(db, wait) {
                Ok(Some(outcome)) => {
                    println!(
                        "{}",
                        serde_json::to_string(&outcome.payload).unwrap_or_default()
                    );
                    break outcome.exit_code;
                }
                Ok(None) => {}
                Err(error) => {
                    eprintln!("Warning: worker outcome scan failed: {error}");
                }
            }
        }

        if let Some(correlation) = result_correlation
            && {
                let latest_event_id = db.get_last_event_id();
                let has_new_events = latest_event_id > recovery_observed_event_id;
                if has_new_events {
                    recovery_observed_event_id = latest_event_id;
                    recovery_event_pending = true;
                }
                let now = Instant::now();
                (recovery_event_pending && now >= next_recovery_scan)
                    || (recovery_error_since.is_some() && Instant::now() >= next_recovery_retry)
            }
        {
            recovery_event_pending = false;
            next_recovery_scan = Instant::now() + Duration::from_millis(500);
            match recover_correlated_stopped_result(db, correlation) {
                Ok(Some(event)) => {
                    let output = if full_output {
                        event
                    } else {
                        streamline_event(&event, filters)
                    };
                    println!("{}", serde_json::to_string(&output).unwrap_or_default());
                    break 0;
                }
                Ok(None) => {
                    recovery_error_since = None;
                }
                Err(error) => {
                    let now = Instant::now();
                    let first_error_at = recovery_error_since.unwrap_or(now);
                    recovery_error_since = Some(first_error_at);
                    next_recovery_retry = now + RESULT_RECOVERY_RETRY;
                    if now.duration_since(first_error_at) >= RESULT_RECOVERY_GRACE {
                        eprintln!("Result recovery failed: {error}");
                        println!(
                            "{}",
                            serde_json::to_string(&crate::core::result_wait::unavailable_payload(
                                &crate::core::result_wait::OutcomeWait {
                                    worker: &correlation.exact_worker,
                                    generation: &correlation.instance_key,
                                    thread: &correlation.thread,
                                    attempt_after_id: correlation.after_id,
                                },
                                &error,
                            ),)
                            .unwrap_or_default()
                        );
                        break crate::core::result_wait::RESULT_UNAVAILABLE_EXIT;
                    }
                }
            }
        }

        // Check for unread messages (optional preview notification) — use <hcom> XML tag format.
        // Upstream previews unread mail once and continues waiting; filtered waits and
        // explicit-cursor waits emit no unrelated preview.
        if filter_query.is_empty()
            && !listener.has_explicit_cursor()
            && !unread_preview_shown
            && let Some(name) = instance_name
        {
            let messages = db.get_unread_messages(name);
            if !messages.is_empty() {
                let preview = build_message_preview(db, name);
                println!("{preview}");
                unread_preview_shown = true;
            }
        }

        // Wait for TCP notification or timeout
        let remaining = Duration::from_secs(wait_timeout).saturating_sub(start.elapsed());
        if remaining.is_zero() {
            println!(
                "{}",
                serde_json::to_string(&crate::core::result_wait::deadline_payload(
                    outcome_wait.as_ref()
                ))
                .unwrap_or_default()
            );
            break 1;
        }

        let now = Instant::now();
        let mut wait_duration = listener.bounded_recheck_duration(Some(remaining));
        if recovery_error_since.is_some() {
            wait_duration = wait_duration.min(next_recovery_retry.saturating_duration_since(now));
        }
        listener.wait_tick(wait_duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{ResultWaitOptions as EventsWaitOptions, wait_for_result as events_wait};
    #[test]
    fn test_events_args_result_from_conflicts() {
        use clap::Parser;
        assert!(EventsArgs::try_parse_from(["events", "--result-from", "w"]).is_err());
        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "10",
            "--after-id",
            "5",
            "--result-from",
            "worker",
            "--thread",
            "th1",
        ])
        .unwrap();
        assert_eq!(args.result_from.as_deref(), Some("worker"));
    }

    #[test]
    fn result_correlation_rejects_every_additional_filter() {
        use clap::Parser;
        let db = HcomDb::open_raw(std::path::Path::new(":memory:")).unwrap();
        db.init_db().unwrap();
        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--after-id",
            "0",
            "--result-from",
            "worker",
            "--thread",
            "t",
        ])
        .unwrap();
        for key in [
            "instance",
            "from",
            "type",
            "intent",
            "participant",
            "mention",
            "after",
            "before",
            "status",
            "context",
            "file",
            "cmd",
            "action",
            "reply_to",
            "collision",
            "idle",
        ] {
            let mut filters = args.filters.to_filter_map();
            filters.insert(key.to_string(), vec!["decoy".to_string()]);
            let error = apply_result_correlation(&db, &args, &mut filters).unwrap_err();
            assert!(error.contains("other than --thread"), "{key}: {error}");
        }
    }

    #[test]
    fn test_apply_result_correlation() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("apply-corr.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 1000.0)",
                [],
            )
            .unwrap();

        use clap::Parser;
        // Missing after_id -> error
        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "10",
            "--result-from",
            "worker",
            "--thread",
            "t1",
        ])
        .unwrap();
        let mut filters = args.filters.to_filter_map();
        assert!(apply_result_correlation(&db, &args, &mut filters).is_err());

        // Valid args
        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "10",
            "--after-id",
            "0",
            "--result-from",
            "worker",
            "--thread",
            "t1",
        ])
        .unwrap();
        let mut filters = args.filters.to_filter_map();
        let corr = apply_result_correlation(&db, &args, &mut filters)
            .unwrap()
            .unwrap();
        assert_eq!(corr.exact_worker, "worker");
        assert_eq!(corr.thread, "t1");
        assert_eq!(filters.get("type"), Some(&vec!["message".to_string()]));
        assert_eq!(filters.get("intent"), Some(&vec!["inform".to_string()]));
    }

    #[test]
    fn test_events_wait_terminal_outcome_blocker_and_launch_failed() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("wait-terminal.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('worker', 'blocked', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let generation =
            discover_worker_generation(&db, "--result-from", "worker", cursor).unwrap();

        // Emit launch_blocked
        db.conn()
            .execute(
                "INSERT INTO events (type, instance, data, timestamp)
                 VALUES ('life', 'worker', ?1, '2026-10-20T10:00:00Z')",
                rusqlite::params![
                    json!({
                        "action": "launch_blocked",
                        "status": "blocked",
                        "blocked_kind": "confirmation",
                        "detail": "user approval required",
                    })
                    .to_string()
                ],
            )
            .unwrap();

        let correlation = ResultCorrelation {
            exact_worker: "worker".to_string(),
            instance_key: generation.instance_key.clone(),
            thread: "th-1".to_string(),
            after_id: cursor,
        };

        let filters = HashMap::from([
            ("type".to_string(), vec!["message".to_string()]),
            ("thread".to_string(), vec!["th-1".to_string()]),
        ]);
        let filter_sql = build_sql_from_flags(&filters).unwrap();
        let filter_query = format!(" AND ({filter_sql})");

        let exit = events_wait(
            &db,
            &filter_query,
            1,
            EventsWaitOptions {
                after_id: Some(cursor),
                full_output: false,
                filters: &filters,
                instance_name: None,
                result_correlation: Some(&correlation),
            },
        );
        assert_eq!(exit, crate::core::result_wait::RESULT_BLOCKED_EXIT);
    }

    #[test]
    fn test_recover_correlated_stopped_result_ignores_soft_stop() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("soft-stop-rec.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at, tool)
                 VALUES ('worker', 'stopped', 1000.0, 'hermes')",
                [],
            )
            .unwrap();

        let correlation = ResultCorrelation {
            exact_worker: "worker".to_string(),
            instance_key: "worker@1000.000000".to_string(),
            thread: "th-1".to_string(),
            after_id: 0,
        };

        // Seed soft stop event
        db.conn()
            .execute(
                "INSERT INTO events (type, instance, data, timestamp)
                 VALUES ('life', 'worker', ?1, '2026-10-20T10:00:00Z')",
                rusqlite::params![
                    json!({
                        "action": "stopped",
                        "soft": true,
                        "snapshot": {
                            "name": "worker",
                            "created_at": 1000.0,
                            "tool": "hermes",
                        }
                    })
                    .to_string()
                ],
            )
            .unwrap();

        // Soft stop must be ignored by recovery, returning Ok(None) rather than unsupported-provider Err
        let recovered = recover_correlated_stopped_result(&db, &correlation).unwrap();
        assert_eq!(recovered, None);
    }

    #[test]
    fn test_recover_correlated_stopped_result_fails_closed_on_unsupported_provider_hard_stop() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("hard-stop-rec.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at, tool)
                 VALUES ('worker', 'stopped', 1000.0, 'hermes')",
                [],
            )
            .unwrap();

        let correlation = ResultCorrelation {
            exact_worker: "worker".to_string(),
            instance_key: "worker@1000.000000".to_string(),
            thread: "th-1".to_string(),
            after_id: 0,
        };

        // Seed hard stop event (without soft: true)
        db.conn()
            .execute(
                "INSERT INTO events (type, instance, data, timestamp)
                 VALUES ('life', 'worker', ?1, '2026-10-20T10:00:00Z')",
                rusqlite::params![
                    json!({
                        "action": "stopped",
                        "snapshot": {
                            "name": "worker",
                            "created_at": 1000.0,
                            "tool": "hermes",
                        }
                    })
                    .to_string()
                ],
            )
            .unwrap();

        // Hard stop with unsupported provider must error with descriptive message
        let err = recover_correlated_stopped_result(&db, &correlation).unwrap_err();
        assert!(err.contains("provider 'hermes' does not support stopped-result recovery"));
    }
}
