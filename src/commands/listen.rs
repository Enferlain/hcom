//! `hcom listen` command — block and receive messages.
//!
//!
//! Supports: message-wait mode, --timeout, --json, --sql filter mode.
//! Uses TCP notify socket for instant wake on local messages.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::cli_context::{InlineBatch, format_envelope_prefix};
use crate::commands::listen_result::{self, FilterMatchEvent};
use crate::core::filters::{EventFilterArgs, build_sql_from_flags, resolve_filter_names};
use crate::db::HcomDb;
use crate::identity;
use crate::identity::get_display_name;
use crate::instance_lifecycle::{StatusUpdate, set_status};
use crate::instances;
use crate::notify::NotifyServer;
use crate::shared::{CommandContext, ST_ACTIVE, ST_INACTIVE, ST_LISTENING};

/// Parsed arguments for `hcom listen`.
#[derive(clap::Parser, Debug)]
#[command(name = "listen", about = "Wait for events matching filters")]
pub struct ListenArgs {
    /// Timeout in seconds (positional shorthand)
    pub timeout_positional: Option<u64>,
    /// Timeout in seconds (default: 86400 = 24h)
    #[arg(long)]
    pub timeout: Option<u64>,
    /// JSON output
    #[arg(long)]
    pub json: bool,
    /// SQL WHERE filter
    #[arg(long)]
    pub sql: Option<String>,
    /// Only match events with an ID greater than this cursor (filter mode only)
    #[arg(long)]
    pub after_id: Option<i64>,
    /// Preserve the legacy zero exit code when a filtered wait times out
    #[arg(long)]
    pub timeout_ok: bool,
    /// Composable event filters
    #[command(flatten)]
    pub filters: EventFilterArgs,
}

// Filter parsing, SQL generation, and expansion are imported from crate::core::filters

/// Update heartbeat timestamp.
/// Writes last_stop to instances table so stale-cleanup sees the agent as alive.
///
/// Deliberately leaves `wait_timeout` alone: that column is the instance's
/// persistent idle-wait setting (read by the Claude Stop-hook poll and
/// `hcom config -i`), not a per-call value. A listen timeout written there
/// would outlive this call and shorten every later idle wait (#132).
fn update_heartbeat(db: &HcomDb, instance_name: &str) {
    let now = crate::shared::time::now_epoch_i64();

    let mut updates = serde_json::Map::new();
    updates.insert("last_stop".into(), serde_json::json!(now));
    instances::update_instance_position(db, instance_name, &updates);
}

/// Format messages as text for model consumption.
fn format_messages_text(
    db: &HcomDb,
    messages: &[crate::db::Message],
    instance_name: &str,
) -> String {
    let recipient_display = get_display_name(db, instance_name);
    let parts: Vec<String> = messages
        .iter()
        .map(|msg| {
            let sender_display = get_display_name(db, &msg.from);
            let prefix = format_envelope_prefix(
                msg.intent.as_deref(),
                msg.thread.as_deref(),
                msg.reply_id().as_deref(),
            );
            format!(
                "{prefix} {sender_display} -> {recipient_display}: {}",
                msg.text
            )
        })
        .collect();
    if parts.len() == 1 {
        parts.into_iter().next().unwrap_or_default()
    } else {
        format!("[{} new messages] | {}", parts.len(), parts.join(" | "))
    }
}

/// One `--json` line per delivered message. `reply_id` is the value for
/// `hcom send --reply-to`: the local event id, or `<origin_id>:<DEVICE>` for a
/// relayed message. `event_id` is always the local row id.
fn message_json(msg: &crate::db::Message) -> String {
    serde_json::json!({
        "from": msg.from,
        "text": msg.text,
        "event_id": msg.event_id,
        "reply_id": msg.reply_id(),
        "intent": msg.intent,
        "thread": msg.thread,
    })
    .to_string()
}

/// Status after listen returns. Adhoc has no hooks to move it back out of
/// active, so it records what happened as inactive, like every other adhoc
/// command. This is deliberately not an `exit:*` context: a quiet poll is not
/// a process exit, and exit contexts are reaped after 60s regardless of PID.
fn set_listen_done_status(
    db: &HcomDb,
    instance_name: &str,
    instance_data: &serde_json::Value,
    context: &str,
) {
    let status = if is_adhoc(instance_data) {
        ST_INACTIVE
    } else {
        ST_ACTIVE
    };
    set_status(db, instance_name, status, context, Default::default());
}

fn is_adhoc(instance_data: &serde_json::Value) -> bool {
    instance_data.get("tool").and_then(|v| v.as_str()) == Some("adhoc")
}

fn expand_sql_preset(sql: &str) -> Result<String, &'static str> {
    let Some(name) = sql.strip_prefix("stopped:") else {
        return Ok(sql.to_string());
    };
    if name.is_empty() {
        return Err("stopped: preset requires an agent name");
    }
    let escaped = name.replace('\'', "''");
    Ok(format!(
        "type='life' AND instance='{escaped}' AND json_extract(data, '$.action')='stopped'"
    ))
}

/// Main entry point for `hcom listen` command.
///
/// Returns exit code (0 = success, 1 = timeout/error, 130 = interrupted).
pub fn cmd_listen(db: &HcomDb, args: &ListenArgs, ctx: Option<&CommandContext>) -> i32 {
    let explicit_name = ctx.and_then(|c| c.explicit_name.as_deref());

    // Resolve identity
    let resolve_result = if let Some(c) = ctx {
        if let Some(ref id) = c.identity {
            Ok((id.clone(), id.name.clone()))
        } else {
            let name = explicit_name.or(c.explicit_name.as_deref());
            match identity::resolve_identity(db, name, None, None, None, None) {
                Ok(id) => {
                    let n = id.name.clone();
                    Ok((id, n))
                }
                Err(e) => Err(e),
            }
        }
    } else {
        match identity::resolve_identity(db, explicit_name, None, None, None, None) {
            Ok(id) => {
                let n = id.name.clone();
                Ok((id, n))
            }
            Err(e) => Err(e),
        }
    };
    let (identity, instance_name) = match resolve_result {
        Ok(r) => r,
        Err(e) => {
            if explicit_name.is_some() {
                eprintln!("Error: {e}");
            } else {
                eprintln!("Error: --name required (no identity context)");
                eprintln!("Usage: hcom listen --name <name> [--timeout N]");
            }
            return 1;
        }
    };

    // Resolve timeout: --timeout flag > positional > default (24h)
    let mut timeout: f64 = if let Some(t) = args.timeout {
        t as f64
    } else if let Some(t) = args.timeout_positional {
        t as f64
    } else {
        86400.0
    };

    let requested_timeout = timeout;

    // Quick check mode
    if timeout <= 1.0 {
        timeout = 0.1;
    }

    let json_output = args.json;

    // Convert clap filter args to FilterMap
    let mut filters = args.filters.to_filter_map();
    resolve_filter_names(&mut filters, db);

    // Combine filters and --sql (both work together, ANDed)
    let combined_sql = {
        let mut sql_parts = Vec::new();

        if !filters.is_empty() {
            match build_sql_from_flags(&filters) {
                Ok(flag_sql) if !flag_sql.is_empty() => {
                    sql_parts.push(format!("({flag_sql})"));
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    return 1;
                }
                _ => {}
            }
        }

        if let Some(ref sql) = args.sql {
            match expand_sql_preset(sql) {
                Ok(expanded) => sql_parts.push(format!("({expanded})")),
                Err(error) => {
                    eprintln!("Error: {error}");
                    return 1;
                }
            }
        }

        if sql_parts.is_empty() {
            None
        } else {
            Some(sql_parts.join(" AND "))
        }
    };

    if args.after_id.is_some() && combined_sql.is_none() {
        eprintln!("Error: --after-id requires --sql or an event filter");
        return 1;
    }
    if args.timeout_ok && combined_sql.is_none() {
        eprintln!("Error: --timeout-ok requires --sql or an event filter");
        return 1;
    }

    let instance_data = identity.instance_data.as_ref();
    if instance_data.is_none() {
        eprintln!("Error: hcom not started for '{instance_name}'.");
        return 1;
    }

    // Branch: SQL filter mode (combined from flags + --sql). Validate before
    // reserving command output ownership so malformed filters retain the
    // normal error path and do not suppress router delivery.
    if let Some(ref filter) = combined_sql {
        if let Err(e) = validate_filter_sql(db, filter) {
            eprintln!("Invalid SQL filter: {e}");
            return 1;
        }
        crate::cli_context::claim_inline_delivery();

        // Setup SIGTERM handler for filter mode
        let shutdown = Arc::new(AtomicBool::new(false));
        crate::sys::signal::register_term(&shutdown);
        return listen_with_filter(
            db,
            filter,
            &instance_name,
            timeout,
            requested_timeout,
            args.after_id,
            args.timeout_ok,
            json_output,
            instance_data.unwrap(),
            &shutdown,
        );
    }

    // Standard message-wait mode
    // Mark as listening
    set_status(
        db,
        &instance_name,
        ST_LISTENING,
        "ready",
        StatusUpdate {
            detail: "cmd:listen",
            ..Default::default()
        },
    );

    let start_time = std::time::Instant::now();

    // Setup TCP notify server
    let notify_server = NotifyServer::new().ok();
    let notify_port = notify_server.as_ref().map(|s| s.port());

    // Register notify endpoint
    if let Some(port) = notify_port {
        let _ = db.upsert_notify_endpoint(&instance_name, "listen", port);
    }

    update_heartbeat(db, &instance_name);

    // Setup SIGTERM handler for clean shutdown
    let shutdown = Arc::new(AtomicBool::new(false));
    crate::sys::signal::register_term(&shutdown);

    // Check if already disconnected
    if db
        .get_instance_full(&instance_name)
        .ok()
        .flatten()
        .is_none()
    {
        eprintln!("[You have been disconnected from HCOM]");
        return 0;
    }

    if !json_output {
        let display = get_display_name(db, &instance_name);
        eprintln!("[Listening for messages to {display}. Timeout: {timeout}s]");
    }

    let result = listen_loop(
        db,
        &instance_name,
        timeout,
        json_output,
        instance_data.unwrap(),
        start_time,
        notify_server.as_ref(),
        &shutdown,
    );

    // Cleanup: clear cmd:listen detail if still set
    if let Ok(Some(current)) = db.get_instance_full(&instance_name)
        && current.status_detail == "cmd:listen"
    {
        set_status(
            db,
            &instance_name,
            ST_LISTENING,
            "ready",
            Default::default(),
        );
    }

    // Cleanup notify endpoint
    let _ = db.delete_notify_endpoint(&instance_name, "listen");

    result
}

#[allow(clippy::too_many_arguments)]
fn listen_loop(
    db: &HcomDb,
    instance_name: &str,
    timeout: f64,
    json_output: bool,
    instance_data: &serde_json::Value,
    start_time: std::time::Instant,
    notify_server: Option<&NotifyServer>,
    shutdown: &AtomicBool,
) -> i32 {
    loop {
        // Check for SIGTERM
        if shutdown.load(Ordering::Relaxed) {
            if !json_output {
                eprintln!("\n[SIGTERM received, shutting down]");
            }
            return 130;
        }

        // Check if instance was stopped externally
        if db.get_instance_full(instance_name).ok().flatten().is_none() {
            if !json_output {
                eprintln!(
                    "\n[Disconnected: HCOM stopped for {instance_name}. Unless told otherwise, stop work and end your turn now]"
                );
            }
            return 0;
        }

        // Check for unread messages
        if let Some(batch) = InlineBatch::take(db, instance_name) {
            // listen delivers this batch itself (or leaves it unread on a failed
            // write); the router must not deliver another one after it.
            crate::cli_context::claim_inline_delivery();
            let output = if json_output {
                batch
                    .messages
                    .iter()
                    .map(|msg| message_json(msg) + "\n")
                    .collect()
            } else {
                format!(
                    "\n{}\n{}",
                    format_messages_text(db, &batch.messages, instance_name),
                    batch.remaining_note(instance_name)
                )
            };
            if let Err(e) = batch.emit(db, instance_name, &output, false) {
                eprintln!("hcom: {e}");
                set_listen_done_status(db, instance_name, instance_data, "delivery failed");
                return 1;
            }
            let late = batch.arrived_since_note(db, instance_name);
            if json_output {
                // Keep stdout pure JSON; notes go to stderr.
                eprint!("{}{late}", batch.remaining_note(instance_name));
            } else {
                print!("{late}");
            }

            let context = if is_adhoc(instance_data) {
                "message received"
            } else {
                "finished listening"
            };
            set_listen_done_status(db, instance_name, instance_data, context);
            return 0;
        }

        // Always perform at least one unread check before honoring the timeout.
        // Quick-check mode uses a 100 ms budget, and command/setup overhead can
        // consume that budget under load even when a message is already queued.
        let elapsed = start_time.elapsed().as_secs_f64();
        if elapsed >= timeout {
            if is_adhoc(instance_data) {
                set_listen_done_status(db, instance_name, instance_data, "listen timeout");
            }
            if !json_output {
                eprintln!("\n[Timeout: no messages after {timeout}s]");
            }
            return 0;
        }

        // Update heartbeat
        update_heartbeat(db, instance_name);

        // Wait for notification or short poll
        let remaining = timeout - elapsed;
        if remaining <= 0.0 {
            continue;
        }

        // TCP select for local notifications. Relay imports (pull.rs) call
        // `crate::notify::wake_all` after every batch, so the TCP wake fires
        // as soon as remote events land — no separate relay polling needed.
        let wait_time = if notify_server.is_some() {
            remaining.min(30.0)
        } else {
            remaining.min(0.1)
        };

        if let Some(server) = notify_server {
            server.wait(Duration::from_secs_f64(wait_time));
        } else {
            std::thread::sleep(Duration::from_secs_f64(wait_time));
        }
    }
}

type FilterMatch = (i64, String, String, String);
static FILTER_WAIT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn first_filter_match_after(
    db: &HcomDb,
    sql_filter: &str,
    after_id: i64,
) -> Result<Option<FilterMatch>, rusqlite::Error> {
    let cursor_query = format!(
        "SELECT id, type, instance, data FROM events_v \
         WHERE id > ?1 \
           AND NOT (type = 'status' AND COALESCE(status_context, '') LIKE 'filter-wait:%') \
           AND ({sql_filter}) ORDER BY id LIMIT 1"
    );
    let mut stmt = db.conn().prepare(&cursor_query)?;
    let mut rows = stmt.query(rusqlite::params![after_id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    Ok(Some((
        row.get::<_, i64>(0)?,
        row.get::<_, String>(1)?,
        row.get::<_, String>(2)?,
        row.get::<_, String>(3)?,
    )))
}

fn print_filter_match(row: &FilterMatch, json_output: bool) {
    let event = FilterMatchEvent::from_row(row.0, &row.1, &row.2, &row.3);
    if json_output {
        // Versioned contract: see commands/listen_result.rs.
        let output = listen_result::matched_result(&event);
        println!("{}", serde_json::to_string(&output).unwrap_or_default());
    } else {
        println!("{}", listen_result::match_notification(&event));
    }
}

fn complete_filter_match(
    db: &HcomDb,
    row: &FilterMatch,
    json_output: bool,
    instance_name: &str,
    instance_data: &serde_json::Value,
) {
    print_filter_match(row, json_output);
    if instance_data.get("tool").and_then(|v| v.as_str()) == Some("adhoc") {
        set_status(
            db,
            instance_name,
            ST_INACTIVE,
            "filter matched",
            Default::default(),
        );
    }
}

fn validate_filter_sql(db: &HcomDb, sql_filter: &str) -> Result<(), String> {
    // Validate SQL syntax (use events_v view for computed columns).
    let test_query = format!("SELECT 1 FROM events_v WHERE ({sql_filter}) LIMIT 0");
    db.conn()
        .execute_batch(&test_query)
        .map_err(|e| e.to_string())
}

/// Listen directly for matching events without consuming the instance inbox.
#[allow(clippy::too_many_arguments)]
fn listen_with_filter(
    db: &HcomDb,
    sql_filter: &str,
    instance_name: &str,
    timeout: f64,
    requested_timeout: f64,
    after_id: Option<i64>,
    timeout_ok: bool,
    json_output: bool,
    instance_data: &serde_json::Value,
    shutdown: &AtomicBool,
) -> i32 {
    if let Err(e) = validate_filter_sql(db, sql_filter) {
        eprintln!("Invalid SQL filter: {e}");
        return 1;
    }

    // A caller can capture this boundary before launching work and pass it
    // back to consume a result that arrived just before listen started. With
    // no explicit boundary, this invocation only observes future events.
    let start_id = after_id.unwrap_or_else(|| db.get_last_event_id());

    match first_filter_match_after(db, sql_filter, start_id) {
        Ok(Some(row)) => {
            complete_filter_match(db, &row, json_output, instance_name, instance_data);
            return 0;
        }
        Ok(None) => {}
        Err(e) => {
            eprintln!("Error querying filtered events: {e}");
            return 1;
        }
    }

    // Filter-wait status rows are operational bookkeeping, not events callers
    // asked to match. Give this invocation a unique context; direct filtered
    // queries exclude all contexts in this internal namespace.
    let status_context = format!(
        "filter-wait:{}:{}:{}",
        std::process::id(),
        crate::shared::time::now_epoch_i64(),
        FILTER_WAIT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    if instance_data.get("tool").and_then(|v| v.as_str()) == Some("adhoc") {
        set_status(
            db,
            instance_name,
            ST_LISTENING,
            &status_context,
            Default::default(),
        );
    }

    // The loop queries events_v directly, so unrelated inbox messages remain
    // unread and no temporary subscription notification is generated.
    let notify_server = NotifyServer::new().ok();
    if let Some(ref server) = notify_server {
        let _ = db.upsert_notify_endpoint(instance_name, "listen_filter", server.port());
    }

    update_heartbeat(db, instance_name);
    let start_time = std::time::Instant::now();

    if !json_output {
        eprintln!("[Listening for events matching filter. Timeout: {timeout}s]");
    }

    let result = filter_listen_loop(
        db,
        sql_filter,
        start_id,
        instance_name,
        timeout,
        requested_timeout,
        timeout_ok,
        json_output,
        instance_data,
        start_time,
        notify_server.as_ref(),
        shutdown,
    );

    let _ = db.delete_notify_endpoint(instance_name, "listen_filter");
    result
}

#[allow(clippy::too_many_arguments)]
fn filter_listen_loop(
    db: &HcomDb,
    sql_filter: &str,
    mut after_id: i64,
    instance_name: &str,
    timeout: f64,
    requested_timeout: f64,
    timeout_ok: bool,
    json_output: bool,
    instance_data: &serde_json::Value,
    start_time: std::time::Instant,
    notify_server: Option<&NotifyServer>,
    shutdown: &AtomicBool,
) -> i32 {
    loop {
        // Check for SIGTERM
        if shutdown.load(Ordering::Relaxed) {
            if !json_output {
                eprintln!("\n[SIGTERM received, shutting down]");
            }
            if instance_data.get("tool").and_then(|v| v.as_str()) == Some("adhoc") {
                set_status(
                    db,
                    instance_name,
                    ST_INACTIVE,
                    "listen interrupted",
                    Default::default(),
                );
            }
            return 130;
        }

        // Check if stopped
        if db.get_instance_full(instance_name).ok().flatten().is_none() {
            if !json_output {
                eprintln!("\n[Disconnected: HCOM stopped for {instance_name}]");
            }
            return 0;
        }

        let scan_through = db.get_last_event_id();
        match first_filter_match_after(db, sql_filter, after_id) {
            Ok(Some(row)) => {
                complete_filter_match(db, &row, json_output, instance_name, instance_data);
                return 0;
            }
            Ok(None) => after_id = scan_through,
            Err(e) => {
                eprintln!("Error querying filtered events: {e}");
                return 1;
            }
        }

        // Scan before honoring the deadline so a cross-instance event that
        // arrived during the final socket/poll interval cannot become a false
        // timeout.
        let elapsed = start_time.elapsed().as_secs_f64();
        if elapsed >= timeout {
            if json_output {
                // Versioned contract: see commands/listen_result.rs.
                let output = listen_result::timeout_result(requested_timeout, timeout);
                println!("{}", serde_json::to_string(&output).unwrap_or_default());
            } else {
                eprintln!("\n{}", listen_result::timeout_notification(timeout));
            }
            if instance_data.get("tool").and_then(|v| v.as_str()) == Some("adhoc") {
                set_status(
                    db,
                    instance_name,
                    ST_INACTIVE,
                    "listen timeout",
                    Default::default(),
                );
            }
            return i32::from(!timeout_ok);
        }

        update_heartbeat(db, instance_name);

        let remaining = timeout - elapsed;
        if remaining <= 0.0 {
            continue;
        }

        // Relay imports call `wake_all`, while local events for another agent
        // do not necessarily target this endpoint. Cap the socket wait so those
        // cross-instance events are still detected promptly by direct polling.
        let wait_time = if notify_server.is_some() {
            remaining.min(0.5)
        } else {
            remaining.min(0.1)
        };

        if let Some(server) = notify_server {
            server.wait(Duration::from_secs_f64(wait_time));
        } else {
            std::thread::sleep(Duration::from_secs_f64(wait_time));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ListenArgs, cmd_listen, expand_sql_preset, filter_listen_loop, first_filter_match_after,
        listen_with_filter, message_json,
    };
    use crate::db::HcomDb;
    use crate::instance_lifecycle::set_status;
    use crate::shared::{CommandContext, ST_LISTENING};
    use serial_test::serial;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    type TestEnv = (
        tempfile::TempDir,
        PathBuf,
        PathBuf,
        crate::hooks::test_helpers::EnvGuard,
    );

    fn setup_test_db() -> (HcomDb, PathBuf, TestEnv) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let env = crate::hooks::test_helpers::isolated_test_env();
        let db_path = std::env::temp_dir().join(format!(
            "test_hcom_listen_{}_{}.db",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let db = HcomDb::open_at(&db_path).unwrap();
        (db, db_path, env)
    }

    fn cleanup_test_db(path: PathBuf) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    fn listen_args(extra: &[&str]) -> ListenArgs {
        use clap::Parser;
        ListenArgs::try_parse_from(std::iter::once("listen").chain(extra.iter().copied())).unwrap()
    }

    fn row(db: &HcomDb) -> crate::db::InstanceRow {
        db.get_instance_full("luna").unwrap().unwrap()
    }

    /// #132: a short `hcom listen` must not become the instance's persistent
    /// idle-wait timeout, in message mode or filter mode, including NULL.
    #[test]
    #[serial]
    fn listen_timeout_does_not_overwrite_instance_wait_timeout() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, wait_timeout, last_stop) \
                 VALUES ('luna', 1000.0, 86400, 0)",
                [],
            )
            .unwrap();
        let ctx = CommandContext {
            explicit_name: Some("luna".into()),
            identity: None,
            go: false,
            identity_warning: None,
        };

        // Message mode, quiet timeout.
        cmd_listen(&db, &listen_args(&["1"]), Some(&ctx));
        assert_eq!(row(&db).wait_timeout, Some(86400));
        assert!(row(&db).last_stop > 0, "listen must still write heartbeat");

        // Filter mode, quiet timeout.
        let filtered = listen_args(&["--timeout", "1", "--from", "nobody"]);
        cmd_listen(&db, &filtered, Some(&ctx));
        assert_eq!(row(&db).wait_timeout, Some(86400));

        // Message mode, message delivered.
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('nova', 1000.0)",
                [],
            )
            .unwrap();
        crate::commands::send::send_message(
            &db,
            &crate::shared::SenderIdentity {
                kind: crate::shared::SenderKind::Instance,
                name: "nova".into(),
                instance_data: None,
                session_id: None,
            },
            "@luna hi",
            None,
            Some(&["luna".to_string()]),
        )
        .unwrap();
        assert_eq!(cmd_listen(&db, &listen_args(&["20"]), Some(&ctx)), 0);
        assert_eq!(row(&db).wait_timeout, Some(86400));

        // An unset timeout stays unset so the global HCOM_TIMEOUT still applies.
        db.conn()
            .execute(
                "UPDATE instances SET wait_timeout = NULL WHERE name = 'luna'",
                [],
            )
            .unwrap();
        cmd_listen(&db, &listen_args(&["1"]), Some(&ctx));
        assert_eq!(row(&db).wait_timeout, None);
        cmd_listen(
            &db,
            &listen_args(&["--timeout", "1", "--from", "nobody"]),
            Some(&ctx),
        );
        assert_eq!(row(&db).wait_timeout, None);

        cleanup_test_db(path);
    }

    fn ctx(name: &str) -> CommandContext {
        CommandContext {
            explicit_name: Some(name.into()),
            identity: None,
            go: false,
            identity_warning: None,
        }
    }

    fn backdate_status(db: &HcomDb, secs: i64) {
        db.conn()
            .execute(
                "UPDATE instances SET status_time = ? WHERE name = 'luna'",
                [crate::shared::time::now_epoch_i64() - secs],
            )
            .unwrap();
    }

    fn cleanup_keeps_luna(db: &HcomDb) -> bool {
        crate::instance_lifecycle::cleanup_stale_instances(db, 3600, 3600);
        db.get_instance_full("luna").unwrap().is_some()
    }

    /// #118: a quiet poll is not an exit. An adhoc identity that polls with
    /// `listen` must outlive the 60s exit-cleanup tier in both listen modes,
    /// while observed exits and the ordinary inactive tier still reap it.
    #[test]
    #[serial]
    fn adhoc_quiet_listen_is_not_an_exit() {
        let _wake_guard = crate::instance_lifecycle::wake_test_guard();
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, tool) VALUES ('luna', 1000.0, 'adhoc')",
                [],
            )
            .unwrap();

        for (args, expected_code) in [
            (listen_args(&["1"]), 0),
            (listen_args(&["--timeout", "1", "--from", "nobody"]), 1),
        ] {
            assert_eq!(cmd_listen(&db, &args, Some(&ctx("luna"))), expected_code);
            assert_eq!(row(&db).status, crate::shared::ST_INACTIVE);
            assert_eq!(row(&db).status_context, "listen timeout");
            backdate_status(&db, 120);
            assert!(cleanup_keeps_luna(&db), "{args:?} reaped after 120s");
        }

        // Ordinary inactive tier still applies (adhoc rows carry no PID).
        backdate_status(&db, 3700);
        assert!(!cleanup_keeps_luna(&db));

        // An observed exit is still reaped on the short tier.
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, tool, status, status_context) \
                 VALUES ('luna', 1000.0, 'adhoc', 'inactive', 'exit:closed')",
                [],
            )
            .unwrap();
        backdate_status(&db, 120);
        assert!(!cleanup_keeps_luna(&db));

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn filtered_match_does_not_consume_unrelated_inbox_messages() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, tool) VALUES ('sender', 1000.0, 'adhoc'), ('plain', 1000.0, 'adhoc'), ('json', 1000.0, 'adhoc')",
                [],
            )
            .unwrap();

        for (name, json) in [("plain", false), ("json", true)] {
            crate::commands::send::send_message(
                &db,
                &crate::shared::SenderIdentity {
                    kind: crate::shared::SenderKind::Instance,
                    name: "sender".into(),
                    instance_data: None,
                    session_id: None,
                },
                &format!("@{name} unrelated"),
                None,
                Some(&[name.to_string()]),
            )
            .unwrap();
            db.log_event(
                "life",
                "review-other",
                &serde_json::json!({"action": "stopped"}),
            )
            .unwrap();

            let mut filter_args = vec![
                "--sql",
                "type='life' AND instance='review-other'",
                "--after-id",
                "0",
                "--timeout",
                "1",
            ];
            if json {
                filter_args.push("--json");
            }
            assert_eq!(
                cmd_listen(&db, &listen_args(&filter_args), Some(&ctx(name))),
                0
            );
            assert_eq!(
                db.get_instance_full(name).unwrap().unwrap().last_event_id,
                0,
                "filtered {name} listen must not acknowledge inbox messages"
            );
            assert_eq!(db.get_unread_messages(name).len(), 1);
        }

        cleanup_test_db(path);
    }

    #[test]
    fn listen_args_parse_durable_cursor() {
        use clap::Parser;
        let args =
            ListenArgs::try_parse_from(["listen", "--sql", "type='status'", "--after-id", "42"])
                .unwrap();
        assert_eq!(args.after_id, Some(42));
    }

    #[test]
    fn filtered_listen_uses_event_id_boundary() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listen-cursor.db")).unwrap();
        db.ensure_schema().unwrap();
        let event_id = db
            .log_event("status", "nova", &serde_json::json!({"status": "active"}))
            .unwrap();

        assert!(
            first_filter_match_after(&db, "type='status'", event_id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            first_filter_match_after(&db, "type='status'", event_id - 1)
                .unwrap()
                .map(|matched| matched.0),
            Some(event_id)
        );
    }

    #[test]
    fn filtered_listen_excludes_its_own_listening_status_event() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listen-status.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, created_at) VALUES ('alice', 'codex', 1000.0)",
                [],
            )
            .unwrap();
        let start_id = db.get_last_event_id();
        let context = "filter-wait:test:unique";

        set_status(&db, "alice", ST_LISTENING, context, Default::default());
        let status_id = db.get_last_event_id();
        let stored_context: String = db
            .conn()
            .query_row(
                "SELECT status_context FROM events_v WHERE id = ?1",
                rusqlite::params![status_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_context, context);

        assert!(
            first_filter_match_after(&db, "type='status'", start_id)
                .unwrap()
                .is_none(),
            "filtered waits must ignore operational filter-wait status rows"
        );
    }

    #[test]
    fn integrated_filtered_wait_does_not_overwrite_provider_status() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listen-provider-status.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances \
                 (name, tool, status, status_context, status_detail, created_at) \
                 VALUES ('alice', 'codex', 'active', 'tool:shell', 'working', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();

        assert_eq!(
            listen_with_filter(
                &db,
                "type='life'",
                "alice",
                0.01,
                1.0,
                Some(cursor),
                false,
                true,
                &serde_json::json!({"tool": "codex"}),
                &AtomicBool::new(false),
            ),
            1
        );

        let current = db.get_instance_full("alice").unwrap().unwrap();
        assert_eq!(current.status, "active");
        assert_eq!(current.status_context, "tool:shell");
        assert_eq!(current.status_detail, "working");
        assert_eq!(
            db.get_last_event_id(),
            cursor,
            "an integrated transport wait must not emit task-status events"
        );
    }

    #[test]
    fn integrated_filtered_match_does_not_overwrite_provider_status() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listen-provider-match.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances \
                 (name, tool, status, status_context, status_detail, created_at) \
                 VALUES ('alice', 'codex', 'active', 'tool:shell', 'working', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let match_id = db
            .log_event("life", "nova", &serde_json::json!({"action": "stopped"}))
            .unwrap();

        assert_eq!(
            listen_with_filter(
                &db,
                "type='life' AND instance='nova'",
                "alice",
                1.0,
                1.0,
                Some(cursor),
                false,
                true,
                &serde_json::json!({"tool": "codex"}),
                &AtomicBool::new(false),
            ),
            0
        );

        let current = db.get_instance_full("alice").unwrap().unwrap();
        assert_eq!(current.status, "active");
        assert_eq!(current.status_context, "tool:shell");
        assert_eq!(current.status_detail, "working");
        assert_eq!(db.get_last_event_id(), match_id);
    }

    #[test]
    fn filtered_wait_scans_once_more_at_the_deadline() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("listen-final-scan.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, created_at) VALUES ('alice', 'codex', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();

        let writer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(40));
            let mut writer_db = HcomDb::open_raw(&db_path).unwrap();
            writer_db.ensure_schema().unwrap();
            writer_db
                .log_event(
                    "status",
                    "nova",
                    &serde_json::json!({"status": "listening", "context": "turn:end"}),
                )
                .unwrap();
        });

        assert_eq!(
            filter_listen_loop(
                &db,
                "type='status' AND instance='nova'",
                cursor,
                "alice",
                0.1,
                0.1,
                false,
                true,
                &serde_json::json!({"tool": "codex"}),
                std::time::Instant::now(),
                None,
                &AtomicBool::new(false),
            ),
            0,
            "an event inside the final poll interval must beat timeout"
        );
        writer.join().unwrap();
    }

    #[test]
    fn filtered_listen_does_not_consume_unrelated_inbox_messages() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listen-inbox.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances \
                 (name, tool, status, status_context, status_detail, created_at) \
                 VALUES ('alice', 'codex', 'active', 'before-match', 'working', 1000.0)",
                [],
            )
            .unwrap();

        let unrelated_id = db
            .log_event(
                "message",
                "bob",
                &serde_json::json!({
                    "from": "bob",
                    "scope": "mentions",
                    "delivered_to": ["alice"],
                    "text": "unrelated"
                }),
            )
            .unwrap();
        let match_id = db
            .log_event(
                "message",
                "bob",
                &serde_json::json!({
                    "from": "bob",
                    "scope": "mentions",
                    "delivered_to": ["alice"],
                    "text": "wanted"
                }),
            )
            .unwrap();
        let shutdown = AtomicBool::new(false);
        let instance_data = serde_json::json!({"tool": "codex"});
        let sql = "type='message' AND msg_text='wanted'";

        assert_eq!(
            filter_listen_loop(
                &db,
                sql,
                unrelated_id,
                "alice",
                1.0,
                1.0,
                false,
                true,
                &instance_data,
                std::time::Instant::now(),
                None,
                &shutdown,
            ),
            0
        );
        let matched_context: String = db
            .conn()
            .query_row(
                "SELECT status_context FROM instances WHERE name = 'alice'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(matched_context, "before-match");
        set_status(
            &db,
            "alice",
            ST_LISTENING,
            "before-timeout",
            Default::default(),
        );
        assert_eq!(
            filter_listen_loop(
                &db,
                sql,
                match_id,
                "alice",
                0.01,
                0.01,
                false,
                true,
                &instance_data,
                std::time::Instant::now(),
                None,
                &shutdown,
            ),
            1
        );
        let timeout_context: String = db
            .conn()
            .query_row(
                "SELECT status_context FROM instances WHERE name = 'alice'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(timeout_context, "before-timeout");

        let last_event_id: i64 = db
            .conn()
            .query_row(
                "SELECT last_event_id FROM instances WHERE name = 'alice'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last_event_id, 0, "filtered waits must leave inbox unread");
    }

    #[test]
    fn filtered_listen_timeout_ok_preserves_legacy_success() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listen-timeout-ok.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, created_at) VALUES ('alice', 'codex', 1000.0)",
                [],
            )
            .unwrap();

        assert_eq!(
            filter_listen_loop(
                &db,
                "type='life'",
                db.get_last_event_id(),
                "alice",
                0.01,
                0.01,
                true,
                true,
                &serde_json::json!({"tool": "codex"}),
                std::time::Instant::now(),
                None,
                &AtomicBool::new(false),
            ),
            0
        );
    }

    #[test]
    fn stopped_sql_preset_expands_and_escapes_name() {
        let sql = expand_sql_preset("stopped:win'probe").unwrap();
        assert!(sql.contains("instance='win''probe'"));
        assert!(sql.contains("json_extract(data, '$.action')='stopped'"));
    }

    #[test]
    fn stopped_sql_preset_requires_name() {
        assert_eq!(
            expand_sql_preset("stopped:").unwrap_err(),
            "stopped: preset requires an agent name"
        );
    }

    /// #118: `--json` must carry what a reply needs, and `reply_id` must
    /// resolve through `send --reply-to` to the received event, for local and
    /// relay-imported messages alike.
    #[test]
    #[serial]
    fn json_envelope_reply_id_links_reply_to_received_message() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, tool) VALUES \
                 ('luna', 1000.0, 'adhoc'), ('nova', 1000.0, 'adhoc')",
                [],
            )
            .unwrap();

        let envelope = crate::messages::MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Request),
            thread: Some("t1".into()),
            ..Default::default()
        };
        let (local_id, _) = crate::commands::send::send_message(
            &db,
            &crate::shared::SenderIdentity {
                kind: crate::shared::SenderKind::Instance,
                name: "nova".into(),
                instance_data: None,
                session_id: None,
            },
            "@luna local",
            Some(&envelope),
            Some(&["luna".to_string()]),
        )
        .unwrap();
        let expected_relay_local_id: i64 = db
            .conn()
            .query_row("SELECT COALESCE(MAX(id), 0) + 1 FROM events", [], |r| {
                r.get(0)
            })
            .unwrap();

        let relay_local_id = db
            .log_event(
                "message",
                "remo:BOXE",
                &serde_json::json!({
                    "from": "remo:BOXE",
                    "text": "remote",
                    "intent": "request",
                    "thread": "t2",
                    "_relay": {"id": 42, "short": "BOXE", "device": "dev-boxe"},
                }),
            )
            .unwrap();
        assert_eq!(relay_local_id, expected_relay_local_id);
        assert_ne!(
            relay_local_id, 42,
            "local receiver id must not collide with foreign origin id 42"
        );

        let lines: Vec<serde_json::Value> = db
            .get_unread_messages("luna")
            .iter()
            .map(|m| serde_json::from_str(&message_json(m)).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["event_id"], local_id);
        assert_eq!(lines[0]["reply_id"], local_id.to_string());
        assert_eq!(lines[0]["intent"], "request");
        assert_eq!(lines[0]["thread"], "t1");
        assert_eq!(lines[1]["event_id"], relay_local_id);
        assert_eq!(lines[1]["reply_id"], "42:BOXE");
        assert_eq!(lines[1]["intent"], "request");
        assert_eq!(lines[1]["thread"], "t2");

        for (line, expected_local, expected_thread) in [
            (&lines[0], local_id, "t1"),
            (&lines[1], relay_local_id, "t2"),
        ] {
            use clap::Parser;
            let reply_id = line["reply_id"].as_str().unwrap();
            let mut args = crate::commands::send::SendArgs::try_parse_from([
                "send",
                "@nova",
                "--intent",
                "ack",
                "--reply-to",
                reply_id,
                "--",
                "ok",
            ])
            .unwrap();
            args.had_separator = true;
            assert_eq!(
                crate::commands::send::cmd_send(&db, &args, Some(&ctx("luna"))),
                0
            );
            let (reply_to_local, thread): (i64, String) = db
                .conn()
                .query_row(
                    "SELECT json_extract(data, '$.reply_to_local'), json_extract(data, '$.thread') \
                     FROM events WHERE type = 'message' ORDER BY id DESC LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(reply_to_local, expected_local);
            assert_eq!(thread, expected_thread);
        }

        cleanup_test_db(path);
    }
}
