//! Worker-generation observation and result contracts layered on upstream events.
use super::*;
use crate::core::compact::{CompactClassifier, CompactStreamConfig};
use crate::shared::time::now_iso;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Parse a heartbeat interval in seconds, rejecting anything below one second.
fn parse_heartbeat_secs(value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|secs| *secs >= 1)
        .ok_or_else(|| format!("invalid heartbeat '{value}': must be at least 1 second"))
}

/// Args for `hcom events stream`.
#[derive(clap::Args, Debug)]
pub struct EventsStreamArgs {
    /// Only emit events with an ID greater than this cursor
    /// (default: the current durable cursor when the stream starts)
    #[arg(long)]
    pub after_id: Option<i64>,
    /// Full output (not streamlined)
    #[arg(long)]
    pub full: bool,
    /// Overall lifetime in seconds; the stream exits 0 when it expires
    #[arg(long)]
    pub timeout: Option<u64>,
    /// Follow one exact worker generation: observe only that generation's
    /// events and end the stream at its matching stop boundary
    /// (requires --after-id; cannot be combined with filters)
    #[arg(long, requires = "after_id")]
    pub follow: Option<String>,
    /// Emit the typed compact worker-status projection for the followed
    /// generation instead of raw events, excluding raw message and command
    /// text. File paths remain untrusted (requires --follow; conflicts with --full).
    #[arg(long, requires = "follow", conflicts_with = "full")]
    pub compact: bool,
    /// Quiet-heartbeat interval in seconds (at least 1) for compact mode:
    /// while the followed generation produces no correlated activity, emit
    /// at most one rate-limited heartbeat per interval
    #[arg(long, requires = "compact", value_parser = parse_heartbeat_secs)]
    pub heartbeat: Option<u64>,
    /// Composable event filters
    #[command(flatten)]
    pub filters: EventFilterArgs,
}

// ── Stream Mode ──────────────────────────────────────────────────────────

/// Stream mode: emit every matching event, stay active.
struct EventsStreamOptions<'a> {
    after_id: Option<i64>,
    full_output: bool,
    filters: &'a HashMap<String, Vec<String>>,
    instance_name: Option<&'a str>,
    generation_follow: Option<WorkerGenerationFollow>,
    compact: Option<CompactStreamConfig>,
}

fn write_and_flush_record_to<W: Write>(writer: &mut W, line: &str) -> std::io::Result<()> {
    writeln!(writer, "{line}")?;
    writer.flush()
}

fn write_compact_record_to<W: Write>(
    writer: &mut W,
    record: &crate::core::progress::CompactRecord,
) -> std::io::Result<()> {
    let line = serde_json::to_string(record)
        .expect("compact record is data-only and cannot fail to serialize");
    write_and_flush_record_to(writer, &line)
}

fn events_stream(
    db: &HcomDb,
    filter_query: &str,
    stream_timeout: Option<u64>,
    options: EventsStreamOptions<'_>,
) -> i32 {
    let mut stdout = std::io::stdout().lock();
    events_stream_to(db, filter_query, stream_timeout, &mut stdout, options)
}

fn events_stream_to<W: Write>(
    db: &HcomDb,
    filter_query: &str,
    stream_timeout: Option<u64>,
    writer: &mut W,
    options: EventsStreamOptions<'_>,
) -> i32 {
    let EventsStreamOptions {
        after_id,
        full_output,
        filters,
        instance_name,
        mut generation_follow,
        compact,
    } = options;

    if let Some(follow) = generation_follow.as_ref() {
        if !filter_query.is_empty() {
            eprintln!("Error: generation following cannot be combined with event filters");
            return 1;
        }
        if after_id != Some(follow.generation.after_id) {
            eprintln!("Error: generation following must arm at its discovery cursor");
            return 1;
        }
    }

    let mut compact_classifier = None;
    let mut compact_bootstrap_pending = false;
    if let Some(config) = compact {
        let Some(follow) = generation_follow.as_ref() else {
            eprintln!("Error: compact mode requires generation following");
            return 1;
        };
        compact_classifier = Some(CompactClassifier::new(
            follow.generation.instance_key.clone(),
            config,
        ));
        compact_bootstrap_pending = true;
    }

    let interrupted = Arc::new(AtomicBool::new(false));
    crate::sys::signal::register_int(&interrupted);
    crate::sys::signal::register_term(&interrupted);

    let mut listener = EventListener::new(
        db,
        EventListenerOptions {
            after_id,
            instance_name,
            endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
        },
    );

    let deadline = match stream_timeout {
        Some(seconds) => match Instant::now().checked_add(Duration::from_secs(seconds)) {
            Some(deadline) => Some(deadline),
            None => {
                eprintln!("Error: --timeout exceeds the supported duration");
                return 1;
            }
        },
        None => None,
    };

    loop {
        if interrupted.load(Ordering::Relaxed) {
            return 0;
        }

        if db.is_stale() {
            // Raw streams retain the upstream reset control record. Compact
            // stdout remains a closed progress union: report reset through
            // stderr and the nonzero exit instead of inventing an activity.
            if compact_classifier.is_none()
                && let Err(error) = write_and_flush_record_to(writer, "{\"db_reset\":true}")
            {
                if error.kind() == std::io::ErrorKind::BrokenPipe {
                    return 0;
                }
                eprintln!("Error: Failed writing reset marker: {error}");
            }
            eprintln!(
                "Error: database was reset; capture a new cursor and rediscover the worker generation"
            );
            return 1;
        }

        match listener.drain_new_events(filter_query) {
            Ok(events) => {
                for event in events {
                    if interrupted.load(Ordering::Relaxed) {
                        return 0;
                    }
                    let disposition = generation_follow
                        .as_mut()
                        .map_or(GenerationEventDisposition::Observe, |follow| {
                            follow.classify(&event)
                        });
                    if disposition == GenerationEventDisposition::Ignore {
                        continue;
                    }
                    if disposition == GenerationEventDisposition::Terminate {
                        eprintln!(
                            "Warning: followed worker generation cannot be safely correlated at \
                             event {}; ending the stream",
                            event
                                .get("id")
                                .and_then(Value::as_i64)
                                .map_or_else(|| "unknown".to_string(), |id| id.to_string())
                        );
                        return 0;
                    }
                    if let Some(classifier) = compact_classifier.as_mut() {
                        for record in classifier.observe(&event) {
                            if let Err(err) = write_compact_record_to(writer, &record) {
                                if err.kind() == std::io::ErrorKind::BrokenPipe {
                                    return 0;
                                }
                                eprintln!("Error: Failed writing to stdout: {err}");
                                return 1;
                            }
                        }
                    } else {
                        let output = if full_output {
                            event
                        } else {
                            streamline_event(&event, filters)
                        };
                        let line = serde_json::to_string(&output).unwrap_or_default();
                        if let Err(err) = write_and_flush_record_to(writer, &line) {
                            if err.kind() == std::io::ErrorKind::BrokenPipe {
                                return 0;
                            }
                            eprintln!("Error: Failed writing to stdout: {err}");
                            return 1;
                        }
                    }
                    if disposition == GenerationEventDisposition::ObserveAndStop {
                        return 0;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error in SQL WHERE clause: {e}");
                return 2;
            }
        }

        if compact_bootstrap_pending {
            compact_bootstrap_pending = false;
            if let (Some(classifier), Some(follow), Some(cursor)) = (
                compact_classifier.as_mut(),
                generation_follow.as_ref(),
                after_id,
            ) {
                match db.get_instance_status(&follow.generation.exact_worker) {
                    Ok(Some(status)) => {
                        classifier.seed_live_status(cursor, &now_iso(), &status.status);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!("Error: failed to read followed worker status: {error}");
                        return 2;
                    }
                }
            }
        }

        if interrupted.load(Ordering::Relaxed) {
            return 0;
        }

        if let Some(classifier) = compact_classifier.as_mut()
            && let Some(record) = classifier.poll_heartbeat()
            && let Err(err) = write_compact_record_to(writer, &record)
        {
            if err.kind() == std::io::ErrorKind::BrokenPipe {
                return 0;
            }
            eprintln!("Error: Failed writing to stdout: {err}");
            return 1;
        }

        let Some(deadline) = deadline else {
            listener.wait_tick_interruptible(listener.bounded_recheck_duration(None), &interrupted);
            continue;
        };
        let now = Instant::now();
        if now >= deadline {
            return 0;
        }
        listener.wait_tick_interruptible(
            listener.bounded_recheck_duration(Some(deadline.saturating_duration_since(now))),
            &interrupted,
        );
    }
}

pub(super) fn cmd_events_stream(
    db: &HcomDb,
    args: &EventsStreamArgs,
    instance_name: Option<&str>,
) -> i32 {
    crate::cli_context::claim_inline_delivery();
    let mut filters = args.filters.to_filter_map();

    let mut generation_follow = None;
    let mut compact = None;
    if let Some(worker) = args.follow.as_deref() {
        if !filters.is_empty() {
            eprintln!("Error: --follow cannot be combined with event filters");
            return 1;
        }
        let after_id = args
            .after_id
            .expect("--follow requires --after-id (enforced by the parser)");
        match discover_worker_generation(db, "--follow", worker, after_id) {
            Ok(generation) => {
                generation_follow = Some(WorkerGenerationFollow {
                    generation,
                    stopped: false,
                });
            }
            Err(error) => {
                eprintln!("Error: {error}");
                return 1;
            }
        }
        if args.compact {
            compact = Some(CompactStreamConfig {
                heartbeat: args.heartbeat.map(Duration::from_secs),
                clock: Arc::new(Instant::now),
            });
        }
    }

    resolve_filter_names(&mut filters, db);

    let mut filter_query = String::new();
    if !filters.is_empty() {
        match build_sql_from_flags(&filters) {
            Ok(flag_sql) if !flag_sql.is_empty() => {
                filter_query.push_str(&format!(" AND ({flag_sql})"));
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error: Filter error: {e}");
                return 1;
            }
        }
    }

    if args.follow.is_none() && args.timeout.is_none() {
        return super::events_follow(&filter_query, None, args.after_id, args.full, &filters);
    }

    events_stream(
        db,
        &filter_query,
        args.timeout,
        EventsStreamOptions {
            after_id: args.after_id,
            full_output: args.full,
            filters: &filters,
            instance_name,
            generation_follow,
            compact,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::super::wait::{WaitOptions as EventsWaitOptions, wait_for_event as events_wait};
    use super::*;
    use crate::messages::sender_instance_key;

    #[cfg(unix)]
    #[test]
    fn stream_stops_at_database_replacement_without_crossing_generations() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("stream-reset.db");
        let mut db = HcomDb::open_raw(&path).unwrap();
        db.ensure_schema().unwrap();
        db.log_event("status", "worker", &json!({"status":"active"}))
            .unwrap();
        let replacement = temp.path().join("replacement.db");
        let mut next = HcomDb::open_raw(&replacement).unwrap();
        next.ensure_schema().unwrap();
        // Replacing the inode is the same boundary used by upstream reset.
        std::fs::rename(&replacement, &path).unwrap();
        let mut output = Vec::new();
        let filters = HashMap::new();
        let code = events_stream_to(
            &db,
            "",
            None,
            &mut output,
            EventsStreamOptions {
                after_id: Some(0),
                full_output: false,
                filters: &filters,
                instance_name: None,
                generation_follow: None,
                compact: None,
            },
        );
        assert_eq!(code, 1);
        assert_eq!(String::from_utf8(output).unwrap(), "{\"db_reset\":true}\n");

        let mut compact_output = Vec::new();
        let code = events_stream_to(
            &db,
            "",
            None,
            &mut compact_output,
            EventsStreamOptions {
                after_id: Some(0),
                full_output: false,
                filters: &filters,
                instance_name: None,
                generation_follow: Some(WorkerGenerationFollow {
                    generation: WorkerGeneration {
                        exact_worker: "worker".into(),
                        instance_key: "worker@1000.000000".into(),
                        after_id: 0,
                        stop_event_id: None,
                    },
                    stopped: false,
                }),
                compact: Some(CompactStreamConfig {
                    heartbeat: None,
                    clock: Arc::new(Instant::now),
                }),
            },
        );
        assert_eq!(code, 1);
        assert!(
            compact_output.is_empty(),
            "reset must not forge a compact activity"
        );
    }

    #[test]
    fn test_events_wait_runtime_sql_stepping_error_returns_two() {
        let db = HcomDb::open_raw(std::path::Path::new(":memory:")).unwrap();
        db.init_db().unwrap();
        let filters = HashMap::new();
        // Seed at least one event so the query steps rows
        db.log_event("status", "runtime_sql_agent", &json!({"status": "active"}))
            .unwrap();
        let code = events_wait(
            &db,
            " AND (json_extract('{not json}', '$.foo') = 'bar')",
            1,
            EventsWaitOptions {
                after_id: Some(0),
                full_output: false,
                filters: &filters,
                instance_name: None,
            },
        );
        assert_eq!(
            code, 2,
            "runtime SQL stepping error must return exit code 2"
        );
    }

    #[test]
    fn test_events_stream_runtime_sql_stepping_error_returns_two() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-runtime-sql.db")).unwrap();
        db.ensure_schema().unwrap();
        db.log_event("status", "worker", &json!({"status": "active"}))
            .unwrap();

        let filters = HashMap::new();
        let mut output = Vec::new();
        let exit = events_stream_to(
            &db,
            " AND (json_extract('{not json}', '$.foo') = 'bar')",
            Some(1),
            &mut output,
            EventsStreamOptions {
                after_id: Some(0),
                full_output: false,
                filters: &filters,
                instance_name: None,
                generation_follow: None,
                compact: None,
            },
        );
        assert_eq!(
            exit, 2,
            "runtime SQL stepping error in stream must return exit code 2"
        );
    }

    #[test]
    fn test_events_args_after_id_requires_wait_or_follow() {
        use clap::Parser;
        let args =
            EventsArgs::try_parse_from(["events", "--wait", "30", "--after-id", "42"]).unwrap();
        assert_eq!(args.after_id, Some(42));
        assert!(EventsArgs::try_parse_from(["events", "--after-id", "42"]).is_err());
        assert!(EventsArgs::try_parse_from(["events", "--follow", "--after-id", "42"]).is_ok());
    }

    #[test]
    fn test_events_cursor_is_a_standalone_mode() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from(["events", "--cursor"]).unwrap();
        assert!(args.cursor);
        assert!(EventsArgs::try_parse_from(["events", "--cursor", "--wait", "1"]).is_err());
    }

    #[test]
    fn test_events_stream_args() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from([
            "events",
            "stream",
            "--after-id",
            "10",
            "--timeout",
            "5",
            "--follow",
            "worker",
            "--compact",
            "--heartbeat",
            "2",
        ])
        .unwrap();
        let Some(EventsSubcmd::Stream(stream)) = args.subcmd else {
            panic!("expected stream subcommand");
        };
        assert_eq!(stream.after_id, Some(10));
        assert_eq!(stream.timeout, Some(5));
        assert_eq!(stream.follow.as_deref(), Some("worker"));
        assert!(stream.compact);
        assert_eq!(stream.heartbeat, Some(2));
    }

    #[test]
    fn test_worker_generation_discovery() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("gen-discovery.db")).unwrap();
        db.ensure_schema().unwrap();

        let err = discover_worker_generation(&db, "--result-from", "missing", 0).unwrap_err();
        assert!(err.contains("has no generation after the attempt cursor"));

        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('live_worker', 1000.0)",
                [],
            )
            .unwrap();
        let generation =
            discover_worker_generation(&db, "--result-from", "live_worker", 0).unwrap();
        assert_eq!(generation.exact_worker, "live_worker");
        assert_eq!(generation.stop_event_id, None);
    }

    #[test]
    fn test_generation_follow_classification() {
        let generation = WorkerGeneration {
            exact_worker: "worker".to_string(),
            instance_key: sender_instance_key("worker", &json!({"created_at": 1000.0})).unwrap(),
            after_id: 10,
            stop_event_id: None,
        };
        let mut follow = WorkerGenerationFollow {
            generation,
            stopped: false,
        };

        // Before cursor: ignore
        assert_eq!(
            follow.classify(&json!({"id": 5, "type": "status", "instance": "worker"})),
            GenerationEventDisposition::Ignore
        );

        // Matching activity after cursor: observe
        assert_eq!(
            follow.classify(&json!({"id": 15, "type": "status", "instance": "worker"})),
            GenerationEventDisposition::Observe
        );

        // Unrelated worker: ignore
        assert_eq!(
            follow.classify(&json!({"id": 16, "type": "status", "instance": "other"})),
            GenerationEventDisposition::Ignore
        );

        // Terminal stop with matching snapshot: observe and stop
        assert_eq!(
            follow.classify(&json!({
                "id": 20,
                "type": "life",
                "instance": "worker",
                "data": {
                    "action": "stopped",
                    "snapshot": {"name": "worker", "created_at": 1000.0}
                }
            })),
            GenerationEventDisposition::ObserveAndStop
        );

        // After stopped: ignore
        assert_eq!(
            follow.classify(&json!({"id": 25, "type": "status", "instance": "worker"})),
            GenerationEventDisposition::Ignore
        );
    }

    #[test]
    fn test_filtered_wait_ignores_unrelated_unread_message() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("filter-wait.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0)",
                [],
            )
            .unwrap();
        db.log_event(
            "message",
            "nova",
            &json!({"from": "nova", "scope": "broadcast", "text": "unrelated"}),
        )
        .unwrap();
        assert!(!db.get_unread_messages("luna").is_empty());

        let filters = HashMap::from([("type".to_string(), vec!["status".to_string()])]);
        let filter_sql = build_sql_from_flags(&filters).unwrap();
        let filter_query = format!(" AND ({filter_sql})");

        // Filtered wait ignores unread message and times out (returns 1)
        let code = events_wait(
            &db,
            &filter_query,
            1,
            EventsWaitOptions {
                after_id: None,
                full_output: false,
                filters: &filters,
                instance_name: Some("luna"),
            },
        );
        assert_eq!(code, 1);

        // Matching event arrives after cursor -> returns 0
        let cursor = db.get_last_event_id();
        db.log_event("status", "nova", &json!({"status": "active"}))
            .unwrap();
        let code = events_wait(
            &db,
            &filter_query,
            1,
            EventsWaitOptions {
                after_id: Some(cursor),
                full_output: false,
                filters: &filters,
                instance_name: Some("luna"),
            },
        );
        assert_eq!(code, 0);
    }

    #[test]
    fn test_unfiltered_wait_previews_and_continues() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("unfilter-wait.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0)",
                [],
            )
            .unwrap();
        db.log_event(
            "message",
            "nova",
            &json!({"from": "nova", "scope": "broadcast", "text": "preview me"}),
        )
        .unwrap();
        assert!(!db.get_unread_messages("luna").is_empty());

        // Unfiltered wait previews and continues waiting until timeout (returns 1)
        let code = events_wait(
            &db,
            "",
            1,
            EventsWaitOptions {
                after_id: None,
                full_output: false,
                filters: &HashMap::new(),
                instance_name: Some("luna"),
            },
        );
        assert_eq!(code, 1);
    }

    #[test]
    fn test_listener_drain_advances_past_excluded_rows() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("drain.db")).unwrap();
        db.ensure_schema().unwrap();

        db.log_event("status", "agent", &json!({"status": "active"}))
            .unwrap();
        let id1 = db.get_last_event_id();
        db.log_event("life", "agent", &json!({"action": "started"}))
            .unwrap();
        let id2 = db.get_last_event_id();

        let mut listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: Some(0),
                instance_name: None,
                endpoint_kind: "test",
            },
        );

        // Filter for type = 'status'
        let events = listener.drain_new_events(" AND (type = 'status')").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["id"].as_i64(), Some(id1));
        // Drain must advance cursor past id2 even though id2 did not match
        assert_eq!(listener.cursor(), id2);

        // Subsequent drain yields nothing
        let next_events = listener.drain_new_events("").unwrap();
        assert!(next_events.is_empty());
    }

    #[test]
    fn test_events_stream_to_ordered_and_resumable() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-order.db")).unwrap();
        db.ensure_schema().unwrap();

        db.log_event("message", "worker", &json!({"text": "first"}))
            .unwrap();
        let id1 = db.get_last_event_id();
        db.log_event("message", "worker", &json!({"text": "second"}))
            .unwrap();
        let id2 = db.get_last_event_id();

        let mut output = Vec::new();
        let code = events_stream_to(
            &db,
            "",
            Some(1),
            &mut output,
            EventsStreamOptions {
                after_id: Some(0),
                full_output: false,
                filters: &HashMap::new(),
                instance_name: None,
                generation_follow: None,
                compact: None,
            },
        );
        assert_eq!(code, 0);

        let text = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let e1: Value = serde_json::from_str(lines[0]).unwrap();
        let e2: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(e1["id"].as_i64(), Some(id1));
        assert_eq!(e2["id"].as_i64(), Some(id2));

        // Resume after id1 -> only id2 emitted
        let mut output2 = Vec::new();
        let code2 = events_stream_to(
            &db,
            "",
            Some(1),
            &mut output2,
            EventsStreamOptions {
                after_id: Some(id1),
                full_output: false,
                filters: &HashMap::new(),
                instance_name: None,
                generation_follow: None,
                compact: None,
            },
        );
        assert_eq!(code2, 0);
        let text2 = String::from_utf8(output2).unwrap();
        let lines2: Vec<&str> = text2.lines().collect();
        assert_eq!(lines2.len(), 1);
        let resumed: Value = serde_json::from_str(lines2[0]).unwrap();
        assert_eq!(resumed["id"].as_i64(), Some(id2));
    }

    #[test]
    fn test_generation_follow_stream_emits_through_exact_stop_boundary() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("follow-stream-boundary.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let generation = discover_worker_generation(&db, "followed", "worker", cursor).unwrap();
        assert_eq!(generation.stop_event_id, None);

        db.log_event("status", "worker", &json!({ "status": "active" }))
            .unwrap();
        db.log_event(
            "life",
            "worker",
            &json!({
                "action": "stopped",
                "snapshot": { "name": "worker", "created_at": 1000.0 }
            }),
        )
        .unwrap();
        // Activity from reused worker name after stop
        db.log_event(
            "status",
            "worker",
            &json!({ "status": "active", "created_at": 2000.0 }),
        )
        .unwrap();

        let filters = HashMap::new();
        let mut output = Vec::new();
        let started = Instant::now();
        let exit = events_stream_to(
            &db,
            "",
            Some(5),
            &mut output,
            EventsStreamOptions {
                after_id: Some(cursor),
                full_output: true,
                filters: &filters,
                instance_name: None,
                generation_follow: Some(WorkerGenerationFollow {
                    generation,
                    stopped: false,
                }),
                compact: None,
            },
        );
        assert_eq!(exit, 0);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the matching stop boundary must end the stream before its timeout"
        );
        let records = String::from_utf8(output).unwrap();
        let lines: Vec<Value> = records
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            lines.len(),
            2,
            "only the bound generation's activity and its stop boundary may be emitted"
        );
        assert_eq!(lines[0]["type"], "status");
        assert_eq!(lines[0]["instance"], "worker");
        assert_eq!(lines[1]["type"], "life");
        assert_eq!(lines[1]["data"]["action"], "stopped");
    }

    #[test]
    fn test_generation_follow_stream_continues_through_soft_stop() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("follow-stream-soft-stop.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let generation = discover_worker_generation(&db, "--follow", "worker", cursor).unwrap();

        db.log_event(
            "life",
            "worker",
            &json!({
                "action": "stopped",
                "soft": true,
                "snapshot": { "name": "worker", "created_at": 1000.0 }
            }),
        )
        .unwrap();
        db.log_event("status", "worker", &json!({ "status": "listening" }))
            .unwrap();
        db.log_event(
            "life",
            "worker",
            &json!({
                "action": "stopped",
                "snapshot": { "name": "worker", "created_at": 1000.0 }
            }),
        )
        .unwrap();

        let filters = HashMap::new();
        let mut output = Vec::new();
        let started = Instant::now();
        let exit = events_stream_to(
            &db,
            "",
            Some(5),
            &mut output,
            EventsStreamOptions {
                after_id: Some(cursor),
                full_output: true,
                filters: &filters,
                instance_name: None,
                generation_follow: Some(WorkerGenerationFollow {
                    generation,
                    stopped: false,
                }),
                compact: None,
            },
        );
        assert_eq!(exit, 0);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the terminal stop must still end the stream before timeout"
        );
        let lines: Vec<Value> = String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0]["data"]["soft"], true);
        assert_eq!(lines[1]["data"]["status"], "listening");
        assert_eq!(lines[2]["data"]["action"], "stopped");
    }

    #[test]
    fn test_events_stream_has_no_subscription_request_watch_inbox_or_message_side_effects() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-no-side-effects.db")).unwrap();
        db.ensure_schema().unwrap();

        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('worker', 'active', 1000.0), ('listener', 'active', 1000.0)",
                [],
            )
            .unwrap();

        let sub_count_before: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:sub-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sub_count_before, 0);

        let reqwatch_count_before: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reqwatch_count_before, 0);

        let msg_count_before: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE type = 'message'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(msg_count_before, 0);

        let cursor = db.get_last_event_id();
        db.log_event(
            "status",
            "worker",
            &json!({"status": "active", "context": "tool:Bash", "detail": "git status"}),
        )
        .unwrap();
        db.log_event(
            "message",
            "worker",
            &json!({
                "from": "worker",
                "scope": "broadcast",
                "text": "regular message body",
                "sender_instance_key": "worker@1000.000000"
            }),
        )
        .unwrap();

        let filters = HashMap::new();
        let mut output = Vec::new();
        let exit = events_stream_to(
            &db,
            "",
            Some(1),
            &mut output,
            EventsStreamOptions {
                after_id: Some(cursor),
                full_output: false,
                filters: &filters,
                instance_name: Some("listener"),
                generation_follow: None,
                compact: None,
            },
        );
        assert_eq!(exit, 0);

        let sub_count_after: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:sub-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sub_count_after, 0);

        let reqwatch_count_after: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reqwatch_count_after, 0);

        let msg_count_after: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE type = 'message'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(msg_count_after, 1);
    }
}
