//! Direct filtered observation leaves the caller inbox untouched.
use super::*;
use crate::commands::listen_result::{self, FilterMatchEvent};
use std::sync::atomic::AtomicU64;
type FilterMatch = (i64, String, String, String);
static FILTER_WAIT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) fn first_filter_match_after(
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

pub(super) fn validate_filter_sql(db: &HcomDb, sql_filter: &str) -> Result<(), String> {
    // Validate SQL syntax (use events_v view for computed columns).
    let test_query = format!("SELECT 1 FROM events_v WHERE ({sql_filter}) LIMIT 0");
    db.conn()
        .execute_batch(&test_query)
        .map_err(|e| e.to_string())
}

/// Listen directly for matching events without consuming the instance inbox.
#[allow(clippy::too_many_arguments)]
pub(super) fn listen_with_filter(
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
