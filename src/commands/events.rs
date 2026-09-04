//! `hcom events` command — query events, manage subscriptions.
//!
//!
//! Modes:
//! - Query: `hcom events [--last N] [--all] [--full] [--wait SEC] [--sql EXPR] [filters...]`
//! - Subscribe: `hcom events sub [list | SQL | filters...] [--once] [--for name]`
//! - Unsubscribe: `hcom events unsub <id>`
//! - Launch status: `hcom events launch [batch_id] [--timeout N]`

use std::collections::{BTreeSet, HashMap};
use std::net::TcpListener;
use std::time::Duration;

use serde_json::{Value, json};

use crate::core::filters::{
    EventFilterArgs, build_sql_from_flags, resolve_filter_names, validate_type_constraints,
};
use crate::core::launch_status::wait_for_launch;
use crate::db::HcomDb;
use crate::db::subscriptions::{
    SubCreateOutcome, build_and_insert_sql_subscription, create_filter_subscription,
};
use crate::messages::sender_instance_key;
use crate::shared::CommandContext;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResultCorrelation {
    exact_worker: String,
    instance_key: String,
    thread: String,
    after_id: i64,
}

const RESULT_UNAVAILABLE_EXIT: i32 = 3;
const RESULT_RECOVERY_GRACE: Duration = Duration::from_secs(2);
const RESULT_RECOVERY_RETRY: Duration = Duration::from_millis(200);

/// Parsed arguments for `hcom events`.
#[derive(clap::Parser, Debug)]
#[command(name = "events", about = "Query and subscribe to events")]
pub struct EventsArgs {
    /// Subcommand (sub, unsub, launch) or handled as query mode
    #[command(subcommand)]
    pub subcmd: Option<EventsSubcmd>,
    /// Limit count (default: 20)
    #[arg(long)]
    pub last: Option<usize>,
    #[arg(long, hide = true)]
    pub limit: Option<usize>,
    /// Include archived sessions
    #[arg(long)]
    pub all: bool,
    /// Full output (not streamlined)
    #[arg(long)]
    pub full: bool,
    /// Block until match (default: 60s when flag present without value)
    #[arg(long, num_args(0..=1), default_missing_value = "60")]
    pub wait: Option<u64>,
    /// Only match events with an ID greater than this cursor (requires --wait)
    #[arg(long, requires = "wait")]
    pub after_id: Option<i64>,
    /// Print the current durable event cursor for arming a later wait
    #[arg(
        long,
        conflicts_with_all = ["wait", "last", "all", "full", "sql", "remote_fetch"]
    )]
    pub cursor: bool,
    /// Wait for one exact worker result, recovering supported stopped-provider transcripts
    #[arg(long, requires = "wait", conflicts_with = "remote_fetch")]
    pub result_from: Option<String>,
    /// Raw SQL WHERE clause
    #[arg(long)]
    pub sql: Option<String>,
    /// Composable event filters
    #[command(flatten)]
    pub filters: EventFilterArgs,
    /// Fetch events from a remote device instead of local DB
    #[arg(long)]
    pub remote_fetch: bool,
    /// Target device short_id for --remote-fetch (e.g., NUVA)
    #[arg(long)]
    pub device: Option<String>,
}

#[derive(clap::Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum EventsSubcmd {
    /// Subscribe to events
    Sub(EventsSubArgs),
    /// Remove subscription
    Unsub(EventsUnsubArgs),
    /// Wait for launch to complete
    Launch(EventsLaunchArgs),
}

/// Args for `hcom events sub`.
#[derive(clap::Args, Debug)]
pub struct EventsSubArgs {
    /// Auto-remove after first match
    #[arg(long)]
    pub once: bool,
    /// Subscribe on behalf of another agent
    #[arg(long = "for")]
    pub for_agent: Option<String>,
    /// Target remote device short_id (e.g., NUVA) — installs the sub on that device
    #[arg(long)]
    pub device: Option<String>,
    /// Attach a message (sent from the sub's caller) whenever it fires. Supports @mentions.
    #[arg(long = "on-hit")]
    pub on_hit: Option<String>,
    /// Create the sub as an external sender (same semantics as `hcom send --from`).
    /// Use `-b` as shorthand for `--as bigboss`.
    #[arg(long = "as")]
    pub as_name: Option<String>,
    #[arg(short = 'b', long = "bigboss", default_value_t = false)]
    pub from_bigboss: bool,
    /// Composable event filters
    #[command(flatten)]
    pub filters: EventFilterArgs,
    /// SQL parts or "list" keyword
    pub rest: Vec<String>,
}

/// Args for `hcom events unsub`.
#[derive(clap::Args, Debug)]
pub struct EventsUnsubArgs {
    /// Subscription ID to remove
    pub id: String,
    /// Target remote device short_id (e.g., NUVA) — removes the sub on that device
    #[arg(long)]
    pub device: Option<String>,
}

/// Args for `hcom events launch`.
#[derive(clap::Args, Debug)]
pub struct EventsLaunchArgs {
    /// Batch ID to wait for
    pub batch_id: Option<String>,
    /// Timeout in seconds (default: 30)
    #[arg(long, default_value = "30")]
    pub timeout: u64,
}

/// Apply the fail-closed result-correlation contract.
///
/// A result wait is deliberately stricter than composing independent filters:
/// one immutable thread identifies the workflow, `after_id` identifies the
/// attempt boundary, and the resolved sender identifies the worker. Owning the
/// message type and intent here prevents an accidental OR filter from widening
/// the terminal condition.
fn apply_result_correlation(
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
    if !args.filters.from.is_empty()
        || !args.filters.event_type.is_empty()
        || !args.filters.intent.is_empty()
    {
        return Err(
            "--result-from owns --from, --type, and --intent; remove those filters".to_string(),
        );
    }
    if args.sql.is_some() {
        return Err("--result-from cannot be combined with --sql".to_string());
    }

    let after_id = args.after_id.expect("checked above");
    let mut generations = BTreeSet::new();

    if let Some(exact_worker) = crate::identity::resolve_display_name(db, worker)
        && let Some(instance_data) = db
            .get_instance(&exact_worker)
            .map_err(|error| format!("failed to read --result-from worker: {error}"))?
        && let Some(instance_key) = sender_instance_key(&exact_worker, &instance_data)
    {
        generations.insert((exact_worker, instance_key));
    }

    // A worker commonly reports and then stops before the coordinator arms
    // its wait. Stopping deletes the live instance row, but the lifecycle event
    // retains the immutable pre-delete snapshot. Only consider stops after the
    // caller's pre-launch cursor so an earlier generation cannot be revived.
    let mut statement = db
        .conn()
        .prepare(
            "SELECT instance, data FROM events
             WHERE id > ?1 AND type = 'life'
               AND json_valid(data)
               AND json_extract(data, '$.action') = 'stopped'
               AND json_extract(data, '$.placeholder') IS NOT TRUE
             ORDER BY id",
        )
        .map_err(|error| format!("failed to inspect stopped workers: {error}"))?;
    let stopped = statement
        .query_map(rusqlite::params![after_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| format!("failed to inspect stopped workers: {error}"))?;
    for row in stopped {
        let (name, data) =
            row.map_err(|error| format!("failed to inspect stopped worker row: {error}"))?;
        let Ok(data) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        let Some(snapshot) = data.get("snapshot") else {
            continue;
        };
        let tag = snapshot
            .get("tag")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        let display_name = tag.map_or_else(|| name.clone(), |tag| format!("{tag}-{name}"));
        if !name.eq_ignore_ascii_case(worker) && !display_name.eq_ignore_ascii_case(worker) {
            continue;
        }
        if let Some(instance_key) = sender_instance_key(&name, snapshot) {
            generations.insert((name, instance_key));
        }
    }

    let mut generations = generations.into_iter();
    let Some((exact_worker, instance_key)) = generations.next() else {
        return Err(format!(
            "--result-from worker '{worker}' has no generation after the attempt cursor"
        ));
    };
    if generations.next().is_some() {
        return Err(format!(
            "--result-from worker '{worker}' resolves to multiple generations after the attempt cursor"
        ));
    }
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
/// generation has stopped. The stop snapshot supplies immutable session and
/// transcript metadata; the provider adapter additionally requires the unique
/// workflow thread marker inside that transcript.
fn recover_correlated_stopped_result(
    db: &HcomDb,
    correlation: &ResultCorrelation,
) -> Result<Option<Value>, String> {
    let mut statement = db
        .conn()
        .prepare(
            "SELECT id, timestamp, instance, data FROM events
             WHERE id > ?1 AND type = 'life'
               AND json_valid(data)
               AND json_extract(data, '$.action') = 'stopped'
               AND json_extract(data, '$.placeholder') IS NOT TRUE
             ORDER BY id",
        )
        .map_err(|error| format!("failed to inspect stopped result worker: {error}"))?;
    let rows = statement
        .query_map(rusqlite::params![correlation.after_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|error| format!("failed to inspect stopped result worker: {error}"))?;

    for row in rows {
        let (id, timestamp, instance, data) =
            row.map_err(|error| format!("failed to inspect stopped result row: {error}"))?;
        let Ok(data) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
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
pub fn streamline_event(event: &Value, filters: &HashMap<String, Vec<String>>) -> Value {
    let mut data = event.get("data").cloned().unwrap_or_else(|| json!({}));

    if let Some(obj) = data.as_object_mut() {
        // Drop universal bloat
        obj.remove("sender_kind");
        obj.remove("scope");
        obj.remove("delivered_to");
        if !filters.contains_key("mention") {
            obj.remove("mentions");
        }

        let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match event_type {
            "message" => {
                obj.remove("reply_to");
                if !filters.contains_key("sender_instance_key") {
                    obj.remove("sender_instance_key");
                }
            }
            "status" => {
                // Truncate detail unless --cmd or --file filter active
                if !filters.contains_key("cmd")
                    && !filters.contains_key("file")
                    && let Some(detail) = obj.get("detail").and_then(|v| v.as_str())
                    && detail.len() > 60
                {
                    let end = (0..=60)
                        .rev()
                        .find(|&i| detail.is_char_boundary(i))
                        .unwrap_or(0);
                    let truncated = format!("{}...", &detail[..end]);
                    obj.insert("detail".into(), json!(truncated));
                }
                obj.remove("position");
            }
            "life" => {
                obj.remove("snapshot");
            }
            _ => {}
        }
    }

    // Truncate timestamp to 19 chars (remove microseconds)
    let ts = event.get("ts").and_then(|v| v.as_str()).unwrap_or("");
    let ts_truncated = if ts.len() > 19 { &ts[..19] } else { ts };

    json!({
        "id": event.get("id"),
        "ts": ts_truncated,
        "type": event.get("type"),
        "instance": event.get("instance"),
        "data": data,
    })
}

// ── Query events from DB ─────────────────────────────────────────────────

/// Query events from events_v view. Returns parsed event objects.
fn query_events(
    db: &HcomDb,
    filter_query: &str,
    last_n: usize,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<Vec<Value>, String> {
    let query =
        format!("SELECT * FROM events_v WHERE 1=1{filter_query} ORDER BY id DESC LIMIT {last_n}");

    let mut stmt = db
        .conn()
        .prepare(&query)
        .map_err(|e| format!("Error in SQL WHERE clause: {e}"))?;

    let rows = stmt
        .query_map(params, |row| {
            let id: i64 = row.get("id")?;
            let ts: String = row.get("timestamp")?;
            let etype: String = row.get("type")?;
            let instance: String = row.get("instance")?;
            let data_str: String = row.get("data")?;
            Ok((id, ts, etype, instance, data_str))
        })
        .map_err(|e| format!("Error in SQL WHERE clause: {e}"))?;

    let mut events = Vec::new();
    for row in rows {
        match row {
            Ok((id, ts, etype, instance, data_str)) => {
                let data: Value = serde_json::from_str(&data_str).unwrap_or(json!({}));
                events.push(json!({
                    "id": id,
                    "ts": ts,
                    "type": etype,
                    "instance": instance,
                    "data": data,
                }));
            }
            Err(e) => {
                eprintln!("Warning: Skipping corrupt event: {e}");
            }
        }
    }

    Ok(events)
}

// ── Subscription Management ──────────────────────────────────────────────

/// List all active event subscriptions.
fn events_sub_list(db: &HcomDb) -> i32 {
    let rows: Vec<(String, String)> = db
        .conn()
        .prepare("SELECT key, value FROM kv WHERE key LIKE 'events_sub:%'")
        .ok()
        .map(|mut stmt| {
            stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|r| r.ok())
            .collect()
        })
        .unwrap_or_default();

    if rows.is_empty() {
        println!("No active subscriptions");
        return 0;
    }

    let subs: Vec<Value> = rows
        .iter()
        .filter_map(|(_, v)| serde_json::from_str(v).ok())
        .collect();

    if subs.is_empty() {
        println!("No active subscriptions");
        return 0;
    }

    println!("{:<10} {:<12} {:<10} FILTER", "ID", "FOR", "MODE");
    for sub in &subs {
        let id = sub.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let caller = sub.get("caller").and_then(|v| v.as_str()).unwrap_or("");
        let is_thread_member = sub
            .get("auto_thread_member")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let mode = if is_thread_member {
            "thread"
        } else if sub.get("once").and_then(|v| v.as_bool()).unwrap_or(false) {
            "once"
        } else {
            "continuous"
        };

        let filter_display = if is_thread_member {
            let thread = sub
                .get("thread_name")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            format!("thread-member:{thread}")
        } else if let Some(filters) = sub.get("filters") {
            let s = filters.to_string();
            if s.len() > 35 {
                {
                    let end = (0..=35).rev().find(|&i| s.is_char_boundary(i)).unwrap_or(0);
                    format!("{}...", &s[..end])
                }
            } else {
                s
            }
        } else {
            let sql = sub.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            if sql.len() > 35 {
                {
                    let end = (0..=35)
                        .rev()
                        .find(|&i| sql.is_char_boundary(i))
                        .unwrap_or(0);
                    format!("{}...", &sql[..end])
                }
            } else {
                sql.to_string()
            }
        };

        println!("{id:<10} {caller:<12} {mode:<10} {filter_display}");
        if let Some(on_hit) = sub.get("on_hit_text").and_then(|v| v.as_str()) {
            println!("{:<10} {:<12} {:<10} on-hit: {on_hit:?}", "", "", "");
        }
    }

    0
}

/// Show one-time tip for a command, tracked per-instance via kv.
/// Delegates to centralized core::tips module.
fn maybe_show_tip(db: &HcomDb, instance_name: &str, command: &str) {
    crate::core::tips::maybe_show_tip(db, instance_name, command, false);
}

/// Create a filter-based subscription.
fn events_sub_filter(
    db: &HcomDb,
    filters: &HashMap<String, Vec<String>>,
    sql_parts: &[String],
    caller: &str,
    once: bool,
    on_hit: Option<&str>,
) -> i32 {
    let outcome = match create_filter_subscription(db, filters, sql_parts, caller, once, on_hit) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Error: {e}");
            return 1;
        }
    };

    match outcome {
        SubCreateOutcome::AlreadyExists { id } => {
            println!("Subscription {id} already exists");
        }
        SubCreateOutcome::Created { id, final_sql } => {
            println!("Subscription {id} created");

            if let Ok(count) = db.conn().query_row(
                &format!("SELECT COUNT(*) FROM events_v WHERE ({final_sql})"),
                [],
                |row| row.get::<_, i64>(0),
            ) && count > 0
            {
                println!("  historical matches: {count} events");
                println!("  You will be notified on the next matching event(s)");
            }

            maybe_show_tip(db, caller, "sub:created");
        }
    }

    0
}

/// Create a raw SQL subscription.
fn events_sub_sql(
    db: &HcomDb,
    sql_parts: &[String],
    caller: &str,
    once: bool,
    on_hit: Option<&str>,
) -> i32 {
    let outcome = match build_and_insert_sql_subscription(db, sql_parts, caller, once, on_hit) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{e}");
            return 1;
        }
    };

    let (sub_id, sql) = match outcome {
        SubCreateOutcome::AlreadyExists { id } => {
            println!("Subscription {id} already exists");
            return 0;
        }
        SubCreateOutcome::Created { id, final_sql } => (id, final_sql),
    };

    // Output
    println!("{sub_id}");
    println!("  for: {caller}");
    println!("  filter: {sql}");

    // Historical matches
    if let Ok(count) = db.conn().query_row(
        &format!("SELECT COUNT(*) FROM events_v WHERE ({sql})"),
        [],
        |row| row.get::<_, i64>(0),
    ) {
        if count > 0 {
            println!("  historical matches: {count} events");
            // Show latest match as example
            if let Ok(mut stmt) = db.conn().prepare(
                &format!("SELECT timestamp, type, instance FROM events_v WHERE ({sql}) ORDER BY id DESC LIMIT 1")
            )
                && let Ok(row) = stmt.query_row([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                }) {
                    let ts = if row.0.len() > 19 { &row.0[..19] } else { &row.0 };
                    println!("  latest match: [{}] {} @ {}", row.1, row.2, ts);
                }
            println!("  You will be notified on the next matching event(s)");
        } else {
            println!("  historical matches: 0 (filter will apply to future events only)");
        }
    }

    maybe_show_tip(db, caller, "sub:created");

    0
}

/// Handle `hcom events sub` subcommand.
fn cmd_events_sub(db: &HcomDb, args: &EventsSubArgs, caller_name: Option<&str>) -> i32 {
    let is_list = args.rest.first().map(|s| s.as_str()) == Some("list");

    // Remote dispatch: install/list subscriptions on another device.
    if let Some(device) = args.device.as_deref() {
        if is_list {
            return cmd_events_sub_remote_list(db, device);
        }
        return cmd_events_sub_remote_create(db, args, device);
    }

    if is_list {
        return events_sub_list(db);
    }

    // Convert clap filter args to FilterMap
    let mut filters = args.filters.to_filter_map();
    resolve_filter_names(&mut filters, db);

    let once = args.once;
    let target_instance = args.for_agent.as_deref().map(|name| {
        crate::identity::resolve_display_name(db, name).unwrap_or_else(|| name.to_string())
    });
    let sql_parts: Vec<String> = args.rest.clone();

    // Resolve caller
    let caller = if let Some(target) = &target_instance {
        // Exact match first, then prefix fallback
        let exact: Option<String> = db
            .conn()
            .query_row(
                "SELECT name FROM instances WHERE name = ?",
                rusqlite::params![target],
                |row| row.get::<_, String>(0),
            )
            .ok();
        let resolved = exact.or_else(|| {
            db.conn()
                .query_row(
                    "SELECT name FROM instances WHERE name LIKE ? LIMIT 1",
                    rusqlite::params![format!("{target}%")],
                    |row| row.get::<_, String>(0),
                )
                .ok()
        });
        match resolved {
            Some(name) => name,
            None => {
                eprintln!("Not found: {target}");
                eprintln!("Use 'hcom list' to see available agents");
                return 1;
            }
        }
    } else if args.from_bigboss || args.as_name.is_some() {
        args.as_name
            .clone()
            .unwrap_or_else(|| crate::shared::constants::SENDER.to_string())
    } else if let Some(name) = caller_name {
        name.to_string()
    } else {
        match crate::identity::resolve_identity(db, None, None, None, None, None, None) {
            Ok(id) => id.name,
            Err(_) => {
                eprintln!("Error: Cannot create subscription without identity.");
                eprintln!("Run 'hcom start' first, or use --name.");
                return 1;
            }
        }
    };

    // Filter-based subscription
    if !filters.is_empty() {
        return events_sub_filter(
            db,
            &filters,
            &sql_parts,
            &caller,
            once,
            args.on_hit.as_deref(),
        );
    }

    // No filters and no SQL: show help
    if sql_parts.is_empty() {
        println!(
            "Event subscriptions: get notified via hcom message when a future event matches.\n\n\
             Usage:\n\
             \x20 events sub [filters] [--once]     Subscribe using filter flags\n\
             \x20 events sub \"SQL WHERE\" [--once]   Subscribe using raw SQL\n\
             \x20 events sub list                   List active subscriptions\n\
             \x20 events unsub <id>                 Remove a subscription\n\
             \x20   --once                          Auto-remove after first match\n\
             \x20   --for <name>                    Subscribe on behalf of another agent\n\
             \x20   --on-hit <TEXT>                 Attach message (sent from caller) when sub fires\n\n\
             Filters (same flag repeated = OR, different flags = AND):\n\
             \x20 --agent NAME                      Agent name\n\
             \x20 --type TYPE                       message | status | life\n\
             \x20 --status VAL                      listening | active | blocked\n\
             \x20 --context PATTERN                 tool:Bash | deliver:X (supports * wildcard)\n\
             \x20 --action VAL                      created | started | ready | stopped | batch_launched | launch_failed | launch_blocked\n\
             \x20 --cmd PATTERN                     Shell command (contains, ^prefix, =exact)\n\
             \x20 --file PATH                       File write (*.py for glob, file.py for contains)\n\
             \x20 --collision                        Two agents edit same file within 30s\n\
             \x20 --from NAME                       Sender\n\
             \x20 --mention NAME                    @mention target\n\
             \x20 --intent VAL                      request | inform | ack\n\
             \x20 --thread NAME                     Thread name\n\
             \x20 --after TIME                      After timestamp (ISO-8601)\n\
             \x20 --before TIME                     Before timestamp (ISO-8601)\n\
             \x20 Shortcuts: --idle NAME, --blocked NAME\n\n\
             Examples:\n\
             \x20 events sub --idle peso            Notified when peso goes idle\n\
             \x20 events sub --file '*.py' --once   One-shot: next .py file write\n\
             \x20 events sub --collision            File edit conflict detection"
        );
        return 0;
    }

    // SQL-based subscription
    events_sub_sql(db, &sql_parts, &caller, once, args.on_hit.as_deref())
}

/// Handle `hcom events unsub <id>`.
fn cmd_events_unsub(db: &HcomDb, args: &EventsUnsubArgs) -> i32 {
    let mut sub_id = args.id.clone();
    if !sub_id.starts_with("sub-") {
        sub_id = format!("sub-{sub_id}");
    }

    if let Some(device) = args.device.as_deref() {
        return cmd_events_unsub_remote(db, device, &sub_id);
    }

    let key = format!("events_sub:{sub_id}");

    // Check exists
    if db.kv_get(&key).ok().flatten().is_none() {
        eprintln!("Not found: {sub_id}");
        eprintln!("Use 'hcom events sub list' to list active subscriptions.");
        return 1;
    }

    let _ = db.kv_set(&key, None);
    println!("Removed {sub_id}");
    0
}

/// Install a subscription on a remote device via SUB_CREATE RPC.
fn cmd_events_sub_remote_create(db: &HcomDb, args: &EventsSubArgs, device: &str) -> i32 {
    // Identity selection for the remote sub:
    //   --as NAME / -b → external caller (any name, not required to exist on remote)
    //   --for NAME     → existing remote instance caller
    let (caller, caller_is_external) = if args.from_bigboss || args.as_name.is_some() {
        let name = args
            .as_name
            .clone()
            .unwrap_or_else(|| crate::shared::constants::SENDER.to_string());
        (name, true)
    } else {
        match args.for_agent.as_deref() {
            Some(s) if !s.is_empty() => (s.to_string(), false),
            _ => {
                eprintln!(
                    "Error: --for <name>, --as <name>, or -b is required when using --device"
                );
                return 1;
            }
        }
    };

    // Build filter map from CLI flags (no local name resolution — the remote side owns the namespace)
    let filters = args.filters.to_filter_map();
    let sql_parts: Vec<String> = args.rest.clone();

    // Must have at least filters or sql_parts
    if filters.is_empty() && sql_parts.is_empty() {
        eprintln!("Error: provide at least one filter or SQL WHERE clause");
        return 1;
    }

    let mut params = json!({
        "caller": caller,
        "caller_is_external": caller_is_external,
        "filters": filters,
        "sql_parts": sql_parts,
        "once": args.once,
    });
    if let Some(text) = args.on_hit.as_deref() {
        params["on_hit"] = json!(text);
    }

    match crate::relay::control::dispatch_remote(
        db,
        device,
        None,
        crate::relay::control::rpc_action::SUB_CREATE,
        &params,
        crate::relay::control::RPC_DEFAULT_TIMEOUT,
    ) {
        Ok(result) => {
            let id = result.get("id").and_then(|v| v.as_str()).unwrap_or("?");
            let resolved_caller = result
                .get("caller")
                .and_then(|v| v.as_str())
                .unwrap_or(&caller);
            let already = result
                .get("already_existed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if already {
                println!("Subscription {id} already exists on {device}");
            } else {
                println!("Subscription {id} created on {device} for {resolved_caller}");
            }
            0
        }
        Err(e) => {
            eprintln!("Remote sub_create failed: {e}");
            1
        }
    }
}

/// List subscriptions on a remote device via SUB_LIST RPC.
fn cmd_events_sub_remote_list(db: &HcomDb, device: &str) -> i32 {
    match crate::relay::control::dispatch_remote(
        db,
        device,
        None,
        crate::relay::control::rpc_action::SUB_LIST,
        &json!({}),
        crate::relay::control::RPC_DEFAULT_TIMEOUT,
    ) {
        Ok(result) => {
            let empty = Vec::new();
            let subs = result
                .get("subs")
                .and_then(|v| v.as_array())
                .unwrap_or(&empty);
            if subs.is_empty() {
                println!("No active subscriptions on {device}");
                return 0;
            }
            println!("{:<10} {:<12} {:<10} FILTER", "ID", "FOR", "MODE");
            for sub in subs {
                let id = sub.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let caller = sub.get("caller").and_then(|v| v.as_str()).unwrap_or("");
                let is_thread_member = sub
                    .get("auto_thread_member")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let mode = if is_thread_member {
                    "thread"
                } else if sub.get("once").and_then(|v| v.as_bool()).unwrap_or(false) {
                    "once"
                } else {
                    "continuous"
                };
                let filter_display = if is_thread_member {
                    let thread = sub
                        .get("thread_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?");
                    format!("thread-member:{thread}")
                } else if let Some(f) = sub.get("filters") {
                    let s = f.to_string();
                    if s.len() > 35 {
                        let end = (0..=35).rev().find(|&i| s.is_char_boundary(i)).unwrap_or(0);
                        format!("{}...", &s[..end])
                    } else {
                        s
                    }
                } else {
                    let sql = sub.get("sql").and_then(|v| v.as_str()).unwrap_or("");
                    if sql.len() > 35 {
                        let end = (0..=35)
                            .rev()
                            .find(|&i| sql.is_char_boundary(i))
                            .unwrap_or(0);
                        format!("{}...", &sql[..end])
                    } else {
                        sql.to_string()
                    }
                };
                println!("{id:<10} {caller:<12} {mode:<10} {filter_display}");
            }
            0
        }
        Err(e) => {
            eprintln!("Remote sub_list failed: {e}");
            1
        }
    }
}

/// Remove a subscription on a remote device via SUB_UNSUB RPC.
fn cmd_events_unsub_remote(db: &HcomDb, device: &str, sub_id: &str) -> i32 {
    let params = json!({ "id": sub_id });
    match crate::relay::control::dispatch_remote(
        db,
        device,
        None,
        crate::relay::control::rpc_action::SUB_UNSUB,
        &params,
        crate::relay::control::RPC_DEFAULT_TIMEOUT,
    ) {
        Ok(result) => {
            let removed = result
                .get("removed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if removed {
                println!("Removed {sub_id} on {device}");
                0
            } else {
                eprintln!("Not found on {device}: {sub_id}");
                1
            }
        }
        Err(e) => {
            eprintln!("Remote sub_unsub failed: {e}");
            1
        }
    }
}

/// Handle `hcom events launch [batch_id] [--timeout N]`.
///
/// Exit codes:
/// - `0` — batch reached `ready`
/// - `1` — batch reported `error` or no launches were found (`no_launches`)
/// - `2` — wait timed out (`timeout`) or batch is `blocked` on user attention
///
/// Callers that just want "did it succeed" should check `== 0`. Callers that
/// distinguish "still in progress" from "broken" should branch on `2` vs `1`.
fn cmd_events_launch(db: &HcomDb, args: &EventsLaunchArgs, instance_name: Option<&str>) -> i32 {
    let timeout = args.timeout;

    let batch_id = args.batch_id.as_deref();

    // Resolve launcher
    let launcher = instance_name.map(|s| s.to_string()).or_else(|| {
        if crate::shared::is_inside_ai_tool() {
            crate::identity::resolve_identity(db, None, None, None, None, None, None)
                .ok()
                .map(|id| id.name)
        } else {
            None
        }
    });

    let result = wait_for_launch(db, launcher.as_deref(), batch_id, timeout);
    let result_json = result.to_json();
    println!(
        "{}",
        serde_json::to_string(&result_json).unwrap_or_default()
    );

    match result_json.get("status").and_then(|v| v.as_str()) {
        Some("ready") => 0,
        Some("timeout") | Some("blocked") => 2,
        _ => 1,
    }
}

// ── Wait Mode ────────────────────────────────────────────────────────────

/// Wait mode: block until matching event or timeout.
struct EventsWaitOptions<'a> {
    after_id: Option<i64>,
    full_output: bool,
    filters: &'a HashMap<String, Vec<String>>,
    instance_name: Option<&'a str>,
    result_correlation: Option<&'a ResultCorrelation>,
}

fn events_wait(
    db: &HcomDb,
    filter_query: &str,
    wait_timeout: u64,
    options: EventsWaitOptions<'_>,
) -> i32 {
    use std::time::Instant;

    let EventsWaitOptions {
        after_id,
        full_output,
        filters,
        instance_name,
        result_correlation,
    } = options;

    // Capture the boundary before setting up notification plumbing so events
    // arriving during setup are still observed. An explicit cursor also lets a
    // caller safely arm a wait before launching work and consume an event that
    // arrived just before this process started.
    let has_explicit_cursor = after_id.is_some();
    let mut last_id = after_id.unwrap_or_else(|| db.get_last_event_id());

    // Setup TCP notify server for instant wake — only useful when we can
    // register an `events_wait` wake endpoint for `crate::notify::wake_all`
    // to poke. Anonymous waits (no instance_name) skip the listener and
    // fall through to a short poll.
    let mut notify_server: Option<TcpListener> = None;
    let mut notify_port: Option<u16> = None;
    if let Some(name) = instance_name
        && let Ok(server) = TcpListener::bind("127.0.0.1:0")
        && let Ok(addr) = server.local_addr()
    {
        let port = addr.port();
        server.set_nonblocking(true).ok();
        if db.upsert_notify_endpoint(name, "events_wait", port).is_ok() {
            notify_server = Some(server);
            notify_port = Some(port);
        }
    }

    let start = Instant::now();
    let mut recovery_observed_event_id =
        result_correlation.map_or(last_id, |correlation| correlation.after_id);
    let mut recovery_error_since: Option<Instant> = None;
    let mut next_recovery_retry = start;
    let mut recovery_event_pending = false;
    let mut next_recovery_scan = start;

    let result = loop {
        if start.elapsed() >= Duration::from_secs(wait_timeout) {
            println!("{}", json!({"timed_out": true}));
            break 1;
        }

        // Query for new matching events
        let query = format!("SELECT * FROM events_v WHERE id > ?{filter_query} ORDER BY id");
        let mut found = false;
        match db.conn().prepare(&query) {
            Ok(mut stmt) => {
                if let Ok(mut rows) = stmt.query(rusqlite::params![last_id]) {
                    while let Ok(Some(row)) = rows.next() {
                        // Always advance last_id regardless of parse success
                        if let Ok(id) = row.get::<_, i64>("id") {
                            last_id = id;
                        }
                        if let Ok(event) = parse_event_row(row) {
                            let output = if full_output {
                                event.clone()
                            } else {
                                streamline_event(&event, filters)
                            };
                            println!("{}", serde_json::to_string(&output).unwrap_or_default());
                            found = true;
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error in SQL WHERE clause: {e}");
                break 2;
            }
        }

        if found {
            break 0;
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
                            json!({
                                "result_unavailable": true,
                                "worker": correlation.exact_worker,
                                "thread": correlation.thread,
                                "reason": error,
                            })
                        );
                        break RESULT_UNAVAILABLE_EXIT;
                    }
                }
            }
        }

        // For a legacy unfiltered wait, an older unread inbox message is still a
        // useful interrupt. Filtered waits and explicit-cursor waits must only
        // complete on their declared event boundary: otherwise unrelated or
        // already-consumed messages can produce a false successful match.
        if filter_query.is_empty()
            && !has_explicit_cursor
            && let Some(name) = instance_name
        {
            let messages = db.get_unread_messages(name);
            if !messages.is_empty() {
                // Format as <hcom> XML tag
                let preview = build_message_preview(db, name);
                println!("{preview}");
                break 0;
            }
        }

        // Wait for TCP notification or timeout
        let remaining = wait_timeout.saturating_sub(start.elapsed().as_secs());
        if remaining == 0 {
            println!("{}", json!({"timed_out": true}));
            break 1;
        }

        if let Some(ref server) = notify_server {
            // Use poll-based wait (500ms intervals since TcpListener is non-blocking)
            let wait_time = std::cmp::min(remaining, 5);
            let now = Instant::now();
            let mut wait_duration = Duration::from_secs(wait_time);
            if recovery_error_since.is_some() {
                wait_duration =
                    wait_duration.min(next_recovery_retry.saturating_duration_since(now));
            }
            let poll_end = now + wait_duration;
            while Instant::now() < poll_end {
                // Try accept (non-blocking)
                if let Ok((conn, _)) = server.accept() {
                    let _ = conn.shutdown(std::net::Shutdown::Both);
                    break; // Got notification, re-check events
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        } else {
            // No registered listener (anonymous wait or bind failed) — nothing
            // can TCP-wake us, so re-check the events query on a short tick.
            let now = Instant::now();
            let mut wait_duration = Duration::from_millis(500);
            if recovery_error_since.is_some() {
                wait_duration =
                    wait_duration.min(next_recovery_retry.saturating_duration_since(now));
            }
            std::thread::sleep(wait_duration);
        }
    };

    // Cleanup TCP notify endpoint
    if let (Some(name), Some(_port)) = (instance_name, notify_port) {
        let _ = db.delete_notify_endpoint(name, "events_wait");
    }

    result
}

/// Parse a row from events_v into a JSON value.
fn parse_event_row(row: &rusqlite::Row) -> Result<Value, rusqlite::Error> {
    let id: i64 = row.get("id")?;
    let ts: String = row.get("timestamp")?;
    let etype: String = row.get("type")?;
    let instance: String = row.get("instance")?;
    let data_str: String = row.get("data")?;
    let data: Value = serde_json::from_str(&data_str).unwrap_or(json!({}));
    Ok(json!({
        "id": id,
        "ts": ts,
        "type": etype,
        "instance": instance,
        "data": data,
    }))
}

/// Build <hcom> XML message preview for unread notification.
fn build_message_preview(db: &HcomDb, instance_name: &str) -> String {
    let messages = db.get_unread_messages(instance_name);
    if messages.is_empty() {
        return "<hcom></hcom>".to_string();
    }

    // Build simple "sender → you" format
    let display_name = crate::identity::get_display_name(db, instance_name);
    let senders: Vec<String> = messages
        .iter()
        .map(|m| crate::identity::get_display_name(db, &m.from))
        .collect();

    // Deduplicate senders preserving order
    let mut seen = std::collections::HashSet::new();
    let unique_senders: Vec<&str> = senders
        .iter()
        .filter(|s| seen.insert(s.as_str()))
        .map(|s| s.as_str())
        .collect();

    let preview = if unique_senders.len() == 1 {
        format!("{} → {display_name}", unique_senders[0])
    } else {
        format!("{} → {display_name}", unique_senders.join(", "))
    };

    // Truncate if needed (max ~200 chars)
    let max_content = 200;
    if preview.len() > max_content {
        let end = (0..=(max_content - 3))
            .rev()
            .find(|&i| preview.is_char_boundary(i))
            .unwrap_or(0);
        format!("<hcom>{}...</hcom>", &preview[..end])
    } else {
        format!("<hcom>{preview}</hcom>")
    }
}

// ── Main Entry Point ─────────────────────────────────────────────────────

/// Main entry point for `hcom events` command.
pub fn cmd_events(db: &HcomDb, args: &EventsArgs, ctx: Option<&CommandContext>) -> i32 {
    if args.limit.is_some() {
        eprintln!("Error: Unsupported flag '--limit'. Use '--last' instead.");
        return 1;
    }

    if let Some(sql) = &args.sql {
        if sql.contains("from_agent") {
            eprintln!("Error: Unknown SQL field 'from_agent'. Use 'from' (or 'msg_from' in raw SQL).");
            return 1;
        }
    }

    // Resolve identity context
    let instance_name = ctx
        .and_then(|c| c.identity.as_ref())
        .filter(|id| matches!(id.kind, crate::shared::SenderKind::Instance))
        .map(|id| id.name.clone());
    let caller_name = instance_name.clone();

    if args.cursor {
        if args.subcmd.is_some()
            || !args.filters.to_filter_map().is_empty()
            || args.device.is_some()
            || args.result_from.is_some()
        {
            eprintln!("Error: --cursor cannot be combined with filters or other event modes");
            return 1;
        }
        println!("{}", db.get_last_event_id());
        return 0;
    }

    // Handle subcommands
    if let Some(ref subcmd) = args.subcmd {
        if args.result_from.is_some() {
            eprintln!("Error: --result-from is only supported in query mode");
            return 1;
        }
        if args.remote_fetch {
            eprintln!("Error: --remote-fetch is only supported in query mode");
            return 1;
        }
        match subcmd {
            EventsSubcmd::Launch(launch_args) => {
                return cmd_events_launch(db, launch_args, instance_name.as_deref());
            }
            EventsSubcmd::Sub(sub_args) => {
                return cmd_events_sub(db, sub_args, caller_name.as_deref());
            }
            EventsSubcmd::Unsub(unsub_args) => {
                return cmd_events_unsub(db, unsub_args);
            }
        }
    }

    // Query mode — use typed fields directly
    let search_all = args.all;
    let full_output = args.full;
    let last_n = args.last.unwrap_or(20);
    let sql_where = args.sql.as_ref().map(|s| s.replace("\\!", "!"));
    let wait_timeout = args.wait;

    // Convert clap filter args to FilterMap
    let mut filters = args.filters.to_filter_map();
    let result_correlation = match apply_result_correlation(db, args, &mut filters) {
        Ok(correlation) => correlation,
        Err(error) => {
            eprintln!("Error: {error}");
            return 1;
        }
    };
    resolve_filter_names(&mut filters, db);

    // Remote one-shot fetch
    if args.remote_fetch {
        if wait_timeout.is_some() {
            eprintln!("Error: --wait is not supported with --remote-fetch");
            return 1;
        }
        let device = match args.device.as_deref() {
            Some(d) if !d.is_empty() => d.to_string(),
            _ => {
                eprintln!("Error: --remote-fetch requires --device <SHORT_ID>");
                return 1;
            }
        };
        let filters_json = match serde_json::to_value(&filters) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Error: failed to serialize filters: {e}");
                return 1;
            }
        };
        let mut params = json!({
            "filters": filters_json,
            "last": last_n,
        });
        if let Some(ref s) = sql_where {
            params["sql"] = json!(s);
        }
        match crate::relay::control::dispatch_remote(
            db,
            &device,
            None,
            crate::relay::control::rpc_action::EVENTS,
            &params,
            crate::relay::control::RPC_DEFAULT_TIMEOUT,
        ) {
            Ok(result) => {
                let events_arr = match result.get("events").and_then(|v| v.as_array()) {
                    Some(a) => a,
                    None => {
                        eprintln!(
                            "Remote events fetch: malformed peer response (missing 'events' array)"
                        );
                        return 1;
                    }
                };
                for event in events_arr {
                    let output = if full_output {
                        event.clone()
                    } else {
                        streamline_event(event, &filters)
                    };
                    println!("{}", serde_json::to_string(&output).unwrap_or_default());
                }
                if result
                    .get("truncated")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    println!(
                        "{}",
                        json!({"truncated": true, "note": "response size capped"})
                    );
                }
                return 0;
            }
            Err(e) => {
                eprintln!("Remote events fetch failed: {e}");
                return 1;
            }
        }
    }

    // Build filter SQL
    let mut filter_query = String::new();

    if !filters.is_empty() {
        match build_sql_from_flags(&filters) {
            Ok(flag_sql) if !flag_sql.is_empty() => {
                filter_query.push_str(&format!(" AND ({flag_sql})"));
            }
            Err(e) => {
                eprintln!("Error: Filter error: {e}");
                return 1;
            }
            _ => {}
        }
    }

    // Add user SQL WHERE clause
    if let Some(ref sql) = sql_where {
        filter_query.push_str(&format!(" AND ({sql})"));
    }

    // Wait mode
    if let Some(timeout) = wait_timeout {
        return events_wait(
            db,
            &filter_query,
            timeout,
            EventsWaitOptions {
                after_id: args.after_id,
                full_output,
                filters: &filters,
                instance_name: instance_name.as_deref(),
                result_correlation: result_correlation.as_ref(),
            },
        );
    }

    // Snapshot mode (default)
    let events = match query_events(db, &filter_query, last_n, &[]) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };

    // Optionally search archives
    let mut all_events = events;

    if search_all {
        // Mark current events
        for event in &mut all_events {
            if let Some(obj) = event.as_object_mut() {
                obj.insert("source".into(), json!("current"));
            }
        }

        // Search archives
        let archive_dir = crate::paths::hcom_dir().join("archive");
        if archive_dir.exists()
            && let Ok(entries) = std::fs::read_dir(&archive_dir)
        {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let db_path = path.join("hcom.db");
                if !db_path.exists() {
                    continue;
                }
                let archive_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("archive");

                if let Ok(archive_db) = HcomDb::open_raw(&db_path) {
                    // Build archive query with same filters
                    let archive_filter = filter_query.clone();
                    let query = format!(
                        "SELECT * FROM events_v WHERE 1=1{archive_filter} ORDER BY id DESC LIMIT {last_n}"
                    );
                    if let Ok(mut stmt) = archive_db.conn().prepare(&query)
                        && let Ok(rows) = stmt.query_map([], |row| {
                            let id: i64 = row.get("id")?;
                            let ts: String = row.get("timestamp")?;
                            let etype: String = row.get("type")?;
                            let instance: String = row.get("instance")?;
                            let data_str: String = row.get("data")?;
                            Ok((id, ts, etype, instance, data_str))
                        })
                    {
                        for row in rows.flatten() {
                            let (id, ts, etype, instance, data_str) = row;
                            let data: Value = serde_json::from_str(&data_str).unwrap_or(json!({}));
                            all_events.push(json!({
                                "id": id,
                                "ts": ts,
                                "type": etype,
                                "instance": instance,
                                "data": data,
                                "source": archive_name,
                            }));
                        }
                    }
                }
            }
        }
    }

    // Sort by timestamp and limit
    all_events.sort_by(|a, b| {
        let ts_a = a.get("ts").and_then(|v| v.as_str()).unwrap_or("");
        let ts_b = b.get("ts").and_then(|v| v.as_str()).unwrap_or("");
        ts_a.cmp(ts_b)
    });

    if all_events.len() > last_n {
        let start = all_events.len() - last_n;
        all_events = all_events[start..].to_vec();
    }

    // Output
    for event in &all_events {
        let output = if full_output {
            event.clone()
        } else {
            streamline_event(event, &filters)
        };
        println!("{}", serde_json::to_string(&output).unwrap_or_default());
    }

    0
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streamline_event_message() {
        let event = json!({
            "id": 1,
            "ts": "2025-02-23T15:30:45.123456",
            "type": "message",
            "instance": "luna",
            "data": {
                "from": "nova",
                "text": "hello",
                "sender_kind": "instance",
                "scope": "mentions",
                "delivered_to": ["luna"],
                "mentions": ["luna"],
                "reply_to": "42",
                "reply_to_local": 42,
            }
        });

        let filters = HashMap::new();
        let result = streamline_event(&event, &filters);

        let data = result.get("data").unwrap();
        assert!(data.get("sender_kind").is_none());
        assert!(data.get("scope").is_none());
        assert!(data.get("delivered_to").is_none());
        assert!(data.get("mentions").is_none());
        assert!(data.get("reply_to").is_none());
        assert!(data.get("reply_to_local").is_some());
        assert_eq!(result.get("ts").unwrap().as_str().unwrap().len(), 19);
    }

    #[test]
    fn test_streamline_event_status() {
        let long_detail = "x".repeat(100);
        let event = json!({
            "id": 2,
            "ts": "2025-02-23T15:30:45",
            "type": "status",
            "instance": "luna",
            "data": {
                "detail": long_detail,
                "position": {"last_event_id": 42},
                "status": "active",
            }
        });

        let filters = HashMap::new();
        let result = streamline_event(&event, &filters);
        let data = result.get("data").unwrap();

        // Detail should be truncated
        let detail = data.get("detail").unwrap().as_str().unwrap();
        assert!(detail.len() <= 64); // 60 + "..."
        assert!(detail.ends_with("..."));

        // Position should be removed
        assert!(data.get("position").is_none());
    }

    #[test]
    fn test_streamline_event_status_with_cmd_filter() {
        let long_detail = "x".repeat(100);
        let event = json!({
            "id": 2,
            "ts": "2025-02-23T15:30:45",
            "type": "status",
            "instance": "luna",
            "data": {
                "detail": long_detail,
            }
        });

        let mut filters = HashMap::new();
        filters.insert("cmd".to_string(), vec!["git".to_string()]);
        let result = streamline_event(&event, &filters);
        let data = result.get("data").unwrap();

        // Detail should NOT be truncated when --cmd filter active
        let detail = data.get("detail").unwrap().as_str().unwrap();
        assert_eq!(detail.len(), 100);
    }

    #[test]
    fn test_streamline_event_life() {
        let event = json!({
            "id": 3,
            "ts": "2025-02-23T15:30:45",
            "type": "life",
            "instance": "luna",
            "data": {
                "action": "stopped",
                "snapshot": {"large": "nested", "object": true},
            }
        });

        let filters = HashMap::new();
        let result = streamline_event(&event, &filters);
        let data = result.get("data").unwrap();

        assert!(data.get("snapshot").is_none());
        assert!(data.get("action").is_some());
    }

    #[test]
    fn test_streamline_preserves_mentions_with_filter() {
        let event = json!({
            "id": 1,
            "ts": "2025-02-23T15:30:45",
            "type": "message",
            "instance": "luna",
            "data": {
                "mentions": ["luna", "nova"],
            }
        });

        let mut filters = HashMap::new();
        filters.insert("mention".to_string(), vec!["luna".to_string()]);
        let result = streamline_event(&event, &filters);
        let data = result.get("data").unwrap();

        assert!(data.get("mentions").is_some());
    }

    #[test]
    fn test_events_args_wait_with_value() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from(["events", "--wait", "30", "--full"]).unwrap();
        assert_eq!(args.wait, Some(30));
        assert!(args.full);
    }

    #[test]
    fn test_events_args_after_id_requires_wait() {
        use clap::Parser;
        let args =
            EventsArgs::try_parse_from(["events", "--wait", "30", "--after-id", "42"]).unwrap();
        assert_eq!(args.after_id, Some(42));
        assert!(EventsArgs::try_parse_from(["events", "--after-id", "42"]).is_err());
    }

    #[test]
    fn test_events_cursor_is_a_standalone_mode() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from(["events", "--cursor"]).unwrap();
        assert!(args.cursor);
        assert!(EventsArgs::try_parse_from(["events", "--cursor", "--wait", "1"]).is_err());
    }

    #[test]
    fn result_wait_requires_exact_workflow_attempt_and_worker() {
        use clap::Parser;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("result-correlation.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute_batch(
                "INSERT INTO instances (name, created_at) VALUES
                    ('claude-worker', 1000.0),
                    ('glm-worker', 1000.0);",
            )
            .unwrap();
        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--after-id",
            "0",
            "--thread",
            "claude-workflow",
            "--result-from",
            "claude-worker",
        ])
        .unwrap();
        let mut filters = args.filters.to_filter_map();
        let correlation = apply_result_correlation(&db, &args, &mut filters)
            .unwrap()
            .unwrap();
        assert_eq!(filters.get("type").unwrap(), &["message"]);
        assert_eq!(filters.get("from").unwrap(), &["claude-worker"]);
        assert_eq!(filters.get("intent").unwrap(), &["inform"]);
        let claude_instance_key = filters.get("sender_instance_key").unwrap()[0].clone();
        let cursor = db.get_last_event_id();
        db.log_event(
            "message",
            "glm-worker",
            &json!({
                "from": "glm-worker",
                "scope": "mentions",
                "mentions": ["caller"],
                "intent": "inform",
                "thread": "claude-workflow",
                "sender_instance_key": "glm-worker@1000.000000",
                "text": "wrong provider result"
            }),
        )
        .unwrap();
        db.log_event(
            "message",
            "claude-worker",
            &json!({
                "from": "claude-worker",
                "scope": "mentions",
                "mentions": ["caller"],
                "intent": "inform",
                "thread": "glm-workflow",
                "sender_instance_key": claude_instance_key.clone(),
                "text": "wrong workflow result"
            }),
        )
        .unwrap();
        db.log_event(
            "message",
            "claude-worker",
            &json!({
                "from": "claude-worker",
                "scope": "mentions",
                "mentions": ["caller"],
                "intent": "inform",
                "thread": "claude-workflow",
                "sender_instance_key": "claude-worker@2000.000000",
                "text": "reused display name result"
            }),
        )
        .unwrap();

        let filter_sql = build_sql_from_flags(&filters).unwrap();
        let filter_query = format!(" AND ({filter_sql})");
        assert_eq!(
            events_wait(
                &db,
                &filter_query,
                1,
                EventsWaitOptions {
                    after_id: Some(cursor),
                    full_output: true,
                    filters: &filters,
                    instance_name: None,
                    result_correlation: Some(&correlation),
                },
            ),
            1,
            "another provider, workflow, or reused worker name must not satisfy the result wait"
        );

        db.log_event(
            "message",
            "claude-worker",
            &json!({
                "from": "claude-worker",
                "scope": "mentions",
                "mentions": ["caller"],
                "intent": "inform",
                "thread": "claude-workflow",
                "sender_instance_key": claude_instance_key,
                "text": "correlated result"
            }),
        )
        .unwrap();
        assert_eq!(
            events_wait(
                &db,
                &filter_query,
                1,
                EventsWaitOptions {
                    after_id: Some(cursor),
                    full_output: true,
                    filters: &filters,
                    instance_name: None,
                    result_correlation: Some(&correlation),
                },
            ),
            0,
            "only the exact worker/workflow/attempt tuple may complete the wait"
        );

        let missing_cursor = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--thread",
            "claude-workflow",
            "--result-from",
            "claude-worker",
        ])
        .unwrap();
        let mut invalid_filters = missing_cursor.filters.to_filter_map();
        assert!(
            apply_result_correlation(&db, &missing_cursor, &mut invalid_filters)
                .unwrap_err()
                .contains("--after-id")
        );
        assert!(
            EventsArgs::try_parse_from([
                "events",
                "--after-id",
                "0",
                "--thread",
                "claude-workflow",
                "--result-from",
                "claude-worker",
            ])
            .is_err(),
            "--result-from must require --wait"
        );

        let two_threads = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--after-id",
            "0",
            "--thread",
            "workflow-a",
            "--thread",
            "workflow-b",
            "--result-from",
            "claude-worker",
        ])
        .unwrap();
        let mut invalid_filters = two_threads.filters.to_filter_map();
        assert!(
            apply_result_correlation(&db, &two_threads, &mut invalid_filters)
                .unwrap_err()
                .contains("exactly one --thread")
        );

        let conflicting = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--after-id",
            "0",
            "--thread",
            "claude-workflow",
            "--from",
            "glm-worker",
            "--result-from",
            "claude-worker",
        ])
        .unwrap();
        let mut invalid_filters = conflicting.filters.to_filter_map();
        assert!(
            apply_result_correlation(&db, &conflicting, &mut invalid_filters)
                .unwrap_err()
                .contains("owns --from")
        );

        let sql = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--after-id",
            "0",
            "--thread",
            "claude-workflow",
            "--result-from",
            "claude-worker",
            "--sql",
            "id > 0",
        ])
        .unwrap();
        let mut invalid_filters = sql.filters.to_filter_map();
        assert!(
            apply_result_correlation(&db, &sql, &mut invalid_filters)
                .unwrap_err()
                .contains("cannot be combined with --sql")
        );
    }

    #[test]
    fn result_wait_recovers_generation_after_worker_stops() {
        use clap::Parser;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stopped-result.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, session_id, tag, created_at)
                 VALUES ('claude-worker', 'session-a', 'impl', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "message",
            "claude-worker",
            &json!({
                "from": "claude-worker",
                "scope": "mentions",
                "mentions": ["caller"],
                "intent": "inform",
                "thread": "stopped-workflow",
                "sender_instance_key": "claude-worker@1000.000000",
                "text": "report before stop"
            }),
        )
        .unwrap();
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'claude-worker'", [])
            .unwrap();
        db.log_event(
            "life",
            "claude-worker",
            &json!({
                "action": "stopped",
                "snapshot": {
                    "name": "claude-worker",
                    "tag": "impl",
                    "created_at": 1000.0,
                    "session_id": "session-a"
                }
            }),
        )
        .unwrap();
        db.log_event(
            "life",
            "claude-worker",
            &json!({
                "action": "stopped",
                "placeholder": true,
                "snapshot": {
                    "name": "claude-worker",
                    "tag": "impl",
                    "created_at": 1001.0,
                    "session_id": "failed-launch"
                }
            }),
        )
        .unwrap();

        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--after-id",
            &cursor.to_string(),
            "--thread",
            "stopped-workflow",
            "--result-from",
            "impl-claude-worker",
        ])
        .unwrap();
        let mut filters = args.filters.to_filter_map();
        let correlation = apply_result_correlation(&db, &args, &mut filters)
            .unwrap()
            .unwrap();
        assert_eq!(
            filters.get("sender_instance_key").unwrap(),
            &["claude-worker@1000.000000"]
        );
        let filter_sql = build_sql_from_flags(&filters).unwrap();
        assert_eq!(
            events_wait(
                &db,
                &format!(" AND ({filter_sql})"),
                1,
                EventsWaitOptions {
                    after_id: Some(cursor),
                    full_output: true,
                    filters: &filters,
                    instance_name: None,
                    result_correlation: Some(&correlation),
                },
            ),
            0,
            "a report logged before stop must remain consumable"
        );
    }

    #[test]
    fn result_wait_recovers_claude_transcript_after_correlated_stop() {
        use clap::Parser;

        let temp = tempfile::TempDir::new().unwrap();
        let transcript = temp.path().join("claude-session.jsonl");
        std::fs::write(
            &transcript,
            [
                json!({
                    "type":"user",
                    "sessionId":"session-a",
                    "message":{"content":"complete task on recovery-workflow"}
                }),
                json!({
                    "type":"assistant",
                    "sessionId":"session-a",
                    "message":{
                        "stop_reason":"end_turn",
                        "content":[{"type":"text","text":"recovered completion"}]
                    }
                }),
            ]
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
        )
        .unwrap();

        let mut db = HcomDb::open_raw(&temp.path().join("transcript-result.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                    (name, tool, session_id, transcript_path, created_at)
                 VALUES ('claude-worker', 'claude', 'session-a', ?1, 1000.0)",
                rusqlite::params![transcript.to_str().unwrap()],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--after-id",
            &cursor.to_string(),
            "--thread",
            "recovery-workflow",
            "--result-from",
            "claude-worker",
        ])
        .unwrap();
        let mut filters = args.filters.to_filter_map();
        let correlation = apply_result_correlation(&db, &args, &mut filters)
            .unwrap()
            .unwrap();

        db.conn()
            .execute("DELETE FROM instances WHERE name = 'claude-worker'", [])
            .unwrap();
        db.log_event(
            "life",
            "claude-worker",
            &json!({
                "action": "stopped",
                "snapshot": {
                    "name": "claude-worker",
                    "tool": "claude",
                    "created_at": 1000.0,
                    "session_id": "session-a",
                    "transcript_path": transcript.to_str().unwrap(),
                }
            }),
        )
        .unwrap();

        let recovered = recover_correlated_stopped_result(&db, &correlation)
            .unwrap()
            .unwrap();
        assert_eq!(recovered["data"]["recovered"], true);
        assert_eq!(
            recovered["data"]["provenance"]["kind"],
            "transcript_recovery"
        );
        assert_eq!(recovered["data"]["provenance"]["provider"], "claude");
        assert_eq!(recovered["data"]["provenance"]["session_id"], "session-a");
        assert_eq!(recovered["data"]["provenance"]["attempt_after_id"], cursor);

        let filter_sql = build_sql_from_flags(&filters).unwrap();
        assert_eq!(
            events_wait(
                &db,
                &format!(" AND ({filter_sql})"),
                1,
                EventsWaitOptions {
                    after_id: Some(cursor),
                    full_output: true,
                    filters: &filters,
                    instance_name: None,
                    result_correlation: Some(&correlation),
                },
            ),
            0,
            "a stopped exact generation should return its thread-scoped transcript result"
        );
    }

    #[test]
    fn result_wait_retries_while_a_stopped_transcript_finishes_flushing() {
        use clap::Parser;

        let temp = tempfile::TempDir::new().unwrap();
        let transcript = temp.path().join("delayed-session.jsonl");
        let mut db = HcomDb::open_raw(&temp.path().join("delayed-result.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                    (name, tool, session_id, transcript_path, created_at)
                 VALUES ('claude-worker', 'claude', 'session-a', ?1, 1000.0)",
                rusqlite::params![transcript.to_str().unwrap()],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "3",
            "--after-id",
            &cursor.to_string(),
            "--thread",
            "delayed-workflow",
            "--result-from",
            "claude-worker",
        ])
        .unwrap();
        let mut filters = args.filters.to_filter_map();
        let correlation = apply_result_correlation(&db, &args, &mut filters)
            .unwrap()
            .unwrap();
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'claude-worker'", [])
            .unwrap();
        db.log_event(
            "life",
            "claude-worker",
            &json!({
                "action": "stopped",
                "snapshot": {
                    "name": "claude-worker",
                    "tool": "claude",
                    "created_at": 1000.0,
                    "session_id": "session-a",
                    "transcript_path": transcript.to_str().unwrap(),
                }
            }),
        )
        .unwrap();

        let transcript_writer = transcript.clone();
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            std::fs::write(
                transcript_writer,
                [
                    json!({
                        "type":"user","sessionId":"session-a",
                        "message":{"content":"task delayed-workflow"}
                    }),
                    json!({
                        "type":"assistant","sessionId":"session-a",
                        "message":{"stop_reason":"end_turn","content":[{
                            "type":"text","text":"flushed result"
                        }]}
                    }),
                ]
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
            )
            .unwrap();
        });
        let filter_sql = build_sql_from_flags(&filters).unwrap();
        let status = events_wait(
            &db,
            &format!(" AND ({filter_sql})"),
            3,
            EventsWaitOptions {
                after_id: Some(cursor),
                full_output: true,
                filters: &filters,
                instance_name: None,
                result_correlation: Some(&correlation),
            },
        );
        writer.join().unwrap();
        assert_eq!(status, 0);
    }

    #[test]
    fn unsupported_stopped_provider_returns_result_unavailable() {
        use clap::Parser;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("unsupported-result.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, created_at)
                 VALUES ('codex-worker', 'codex', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "3",
            "--after-id",
            &cursor.to_string(),
            "--thread",
            "unsupported-workflow",
            "--result-from",
            "codex-worker",
        ])
        .unwrap();
        let mut filters = args.filters.to_filter_map();
        let correlation = apply_result_correlation(&db, &args, &mut filters)
            .unwrap()
            .unwrap();
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'codex-worker'", [])
            .unwrap();
        db.log_event(
            "life",
            "codex-worker",
            &json!({
                "action": "stopped",
                "snapshot": {
                    "name": "codex-worker",
                    "tool": "codex",
                    "created_at": 1000.0,
                }
            }),
        )
        .unwrap();
        let filter_sql = build_sql_from_flags(&filters).unwrap();
        assert_eq!(
            events_wait(
                &db,
                &format!(" AND ({filter_sql})"),
                3,
                EventsWaitOptions {
                    after_id: Some(cursor),
                    full_output: true,
                    filters: &filters,
                    instance_name: None,
                    result_correlation: Some(&correlation),
                },
            ),
            RESULT_UNAVAILABLE_EXIT
        );
    }

    #[test]
    fn test_events_args_wait_no_value() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from(["events", "--wait", "--full"]).unwrap();
        assert_eq!(args.wait, Some(60)); // default_missing_value
        assert!(args.full);
    }

    #[test]
    fn filtered_wait_ignores_unrelated_unread_message() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("events-wait.db")).unwrap();
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
        db.conn()
            .execute(
                "UPDATE events SET timestamp = '2000-01-01T00:00:00Z' WHERE type = 'message'",
                [],
            )
            .unwrap();
        assert!(!db.get_unread_messages("luna").is_empty());

        let filters = HashMap::from([("type".to_string(), vec!["status".to_string()])]);
        let filter_sql = build_sql_from_flags(&filters).unwrap();
        let filter_query = format!(" AND ({filter_sql})");
        assert_eq!(
            events_wait(
                &db,
                &filter_query,
                1,
                EventsWaitOptions {
                    after_id: None,
                    full_output: false,
                    filters: &filters,
                    instance_name: Some("luna"),
                    result_correlation: None,
                },
            ),
            1,
            "an unrelated unread message must not satisfy a filtered wait"
        );

        assert_eq!(
            events_wait(
                &db,
                "",
                1,
                EventsWaitOptions {
                    after_id: None,
                    full_output: false,
                    filters: &HashMap::new(),
                    instance_name: Some("luna"),
                    result_correlation: None,
                },
            ),
            0,
            "an unfiltered wait should retain the older-unread inbox interrupt"
        );

        let cursor = db.get_last_event_id();
        db.log_event(
            "status",
            "nova",
            &json!({"status": "active", "context": "test"}),
        )
        .unwrap();
        assert_eq!(
            events_wait(
                &db,
                &filter_query,
                1,
                EventsWaitOptions {
                    after_id: Some(cursor),
                    full_output: false,
                    filters: &filters,
                    instance_name: Some("luna"),
                    result_correlation: None,
                },
            ),
            0,
            "a matching event after the durable cursor must satisfy the filtered wait"
        );

        assert_eq!(
            events_wait(
                &db,
                &filter_query,
                1,
                EventsWaitOptions {
                    after_id: None,
                    full_output: false,
                    filters: &filters,
                    instance_name: Some("luna"),
                    result_correlation: None,
                },
            ),
            1,
            "a new wait must not replay the previously consumed match"
        );
    }

    #[test]
    fn test_events_args_no_wait() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from(["events", "--full"]).unwrap();
        assert_eq!(args.wait, None);
        assert!(args.full);
    }

    #[test]
    fn result_from_is_rejected_with_subcommands() {
        use clap::Parser;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("result-subcommand.db")).unwrap();
        db.ensure_schema().unwrap();
        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--after-id",
            "0",
            "--thread",
            "workflow",
            "--result-from",
            "worker",
            "sub",
            "--once",
        ])
        .unwrap();

        assert_eq!(cmd_events(&db, &args, None), 1);
    }

    #[test]
    fn test_events_args_last() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from(["events", "--last", "50"]).unwrap();
        assert_eq!(args.last, Some(50));
    }

    #[test]
    fn test_events_args_with_filters() {
        use clap::Parser;
        let args =
            EventsArgs::try_parse_from(["events", "--agent", "peso", "--type", "message"]).unwrap();
        assert_eq!(args.filters.agent, vec!["peso"]);
        assert_eq!(args.filters.event_type, vec!["message"]);
        assert!(args.subcmd.is_none());
    }

    #[test]
    fn test_events_sub_args() {
        use clap::Parser;
        let args =
            EventsArgs::try_parse_from(["events", "sub", "--agent", "peso", "--once"]).unwrap();
        match args.subcmd {
            Some(EventsSubcmd::Sub(ref sub)) => {
                assert!(sub.once);
                assert_eq!(sub.filters.agent, vec!["peso"]);
            }
            _ => panic!("Expected Sub subcommand"),
        }
    }

    #[test]
    fn test_events_unsub_args() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from(["events", "unsub", "sub-abc123"]).unwrap();
        match args.subcmd {
            Some(EventsSubcmd::Unsub(ref unsub)) => {
                assert_eq!(unsub.id, "sub-abc123");
            }
            _ => panic!("Expected Unsub subcommand"),
        }
    }

    #[test]
    fn test_events_launch_args() {
        use clap::Parser;
        let args =
            EventsArgs::try_parse_from(["events", "launch", "batch1", "--timeout", "60"]).unwrap();
        match args.subcmd {
            Some(EventsSubcmd::Launch(ref launch)) => {
                assert_eq!(launch.batch_id, Some("batch1".to_string()));
                assert_eq!(launch.timeout, 60);
            }
            _ => panic!("Expected Launch subcommand"),
        }
    }
}
