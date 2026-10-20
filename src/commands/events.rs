//! `hcom events` command — query events, manage subscriptions.
//!
//!
//! Modes:
//! - Query: `hcom events [--last N] [--all] [--full] [--wait SEC] [--sql EXPR] [filters...]`
//! - Subscribe: `hcom events sub [list | SQL | filters...] [--once] [--for name]`
//! - Unsubscribe: `hcom events unsub <id>`
//! - Launch status: `hcom events launch [batch_id] [--timeout N]`

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::core::compact::{CompactClassifier, CompactStreamConfig};
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
use crate::shared::time::now_iso;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResultCorrelation {
    exact_worker: String,
    instance_key: String,
    thread: String,
    after_id: i64,
}

/// One immutable worker generation selected at a durable attempt boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerGeneration {
    exact_worker: String,
    instance_key: String,
    after_id: i64,
    stop_event_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenerationEventDisposition {
    Ignore,
    Observe,
    ObserveAndStop,
    Terminate,
}

#[derive(Debug, Clone)]
struct WorkerGenerationFollow {
    generation: WorkerGeneration,
    stopped: bool,
}

impl WorkerGenerationFollow {
    fn classify(&mut self, event: &Value) -> GenerationEventDisposition {
        if self.stopped {
            return GenerationEventDisposition::Ignore;
        }

        let Some(event_id) = event.get("id").and_then(Value::as_i64) else {
            return GenerationEventDisposition::Ignore;
        };
        if event_id <= self.generation.after_id {
            return GenerationEventDisposition::Ignore;
        }
        if let Some(stop_id) = self.generation.stop_event_id
            && event_id > stop_id
        {
            self.stopped = true;
            return GenerationEventDisposition::Terminate;
        }

        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let instance = event
            .get("instance")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let data = event.get("data").unwrap_or(&Value::Null);

        if event_type == "life" && data.get("action").and_then(Value::as_str) == Some("stopped") {
            if data.get("placeholder").and_then(Value::as_bool) == Some(true) {
                return GenerationEventDisposition::Ignore;
            }
            if data.get("soft").and_then(Value::as_bool) == Some(true) {
                return if data
                    .get("snapshot")
                    .and_then(|snapshot| sender_instance_key(instance, snapshot))
                    .as_deref()
                    == Some(self.generation.instance_key.as_str())
                {
                    GenerationEventDisposition::Observe
                } else {
                    GenerationEventDisposition::Ignore
                };
            }
            let snapshot_key = data
                .get("snapshot")
                .and_then(|snapshot| sender_instance_key(instance, snapshot))
                .map(|key| key.to_string());
            match snapshot_key.as_deref() {
                Some(key) if key == self.generation.instance_key => {
                    self.stopped = true;
                    self.generation.stop_event_id = Some(event_id);
                    return GenerationEventDisposition::ObserveAndStop;
                }
                Some(_) => return GenerationEventDisposition::Ignore,
                None if instance.eq_ignore_ascii_case(&self.generation.exact_worker) => {
                    self.stopped = true;
                    return GenerationEventDisposition::Terminate;
                }
                None => return GenerationEventDisposition::Ignore,
            }
        }

        if event_type == "message" {
            return if data.get("sender_instance_key").and_then(Value::as_str)
                == Some(self.generation.instance_key.as_str())
            {
                GenerationEventDisposition::Observe
            } else {
                GenerationEventDisposition::Ignore
            };
        }

        if instance.eq_ignore_ascii_case(&self.generation.exact_worker) {
            GenerationEventDisposition::Observe
        } else {
            GenerationEventDisposition::Ignore
        }
    }
}

const RESULT_RECOVERY_GRACE: Duration = Duration::from_secs(2);
const RESULT_RECOVERY_RETRY: Duration = Duration::from_millis(200);

fn discover_worker_generation(
    db: &HcomDb,
    label: &str,
    worker: &str,
    after_id: i64,
) -> Result<WorkerGeneration, String> {
    let mut generations: BTreeMap<String, WorkerGeneration> = BTreeMap::new();

    if let Some(exact_worker) = crate::identity::resolve_display_name(db, worker)
        && let Some(instance_data) = db
            .get_instance(&exact_worker)
            .map_err(|error| format!("failed to read {label} worker: {error}"))?
        && let Some(instance_key) = sender_instance_key(&exact_worker, &instance_data)
    {
        generations.insert(
            instance_key.clone(),
            WorkerGeneration {
                exact_worker,
                instance_key,
                after_id,
                stop_event_id: None,
            },
        );
    }

    let mut statement = db
        .conn()
        .prepare(
            "SELECT id, instance, data FROM events
             WHERE id > ?1 AND type = 'life'
               AND json_valid(data)
               AND json_extract(data, '$.action') = 'stopped'
               AND json_extract(data, '$.placeholder') IS NOT TRUE
             ORDER BY id",
        )
        .map_err(|error| format!("failed to inspect stopped workers: {error}"))?;
    let stopped = statement
        .query_map(rusqlite::params![after_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|error| format!("failed to inspect stopped workers: {error}"))?;
    for row in stopped {
        let (event_id, name, data) =
            row.map_err(|error| format!("failed to inspect stopped worker row: {error}"))?;
        let Ok(data) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        let soft = data.get("soft").and_then(Value::as_bool) == Some(true);
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
        let Some(instance_key) = sender_instance_key(&name, snapshot) else {
            continue;
        };
        generations
            .entry(instance_key.clone())
            .and_modify(|generation| {
                if !soft {
                    generation.stop_event_id = Some(
                        generation
                            .stop_event_id
                            .map_or(event_id, |current| current.min(event_id)),
                    );
                }
            })
            .or_insert(WorkerGeneration {
                exact_worker: name,
                instance_key,
                after_id,
                stop_event_id: (!soft).then_some(event_id),
            });
    }

    let mut generations = generations.into_values();
    let Some(generation) = generations.next() else {
        return Err(format!(
            "{label} worker '{worker}' has no generation after the attempt cursor"
        ));
    };
    if generations.next().is_some() {
        return Err(format!(
            "{label} worker '{worker}' resolves to multiple generations after the attempt cursor"
        ));
    }
    Ok(generation)
}

/// Parsed arguments for `hcom events`.
#[derive(clap::Parser, Debug)]
#[command(name = "events", about = "Query and subscribe to events")]
pub struct EventsArgs {
    /// Subcommand (sub, unsub, launch) or handled as query mode
    #[command(subcommand)]
    pub subcmd: Option<EventsSubcmd>,
    /// Limit count (default: 20; --limit is an accepted alias)
    #[arg(long, visible_alias = "limit")]
    pub last: Option<usize>,
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
    /// Wait for one exact worker result; also terminates on a typed actionable
    /// blocker, launch failure, or a stop without a recoverable result
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
    /// Continuously stream matching events
    Stream(EventsStreamArgs),
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
    /// generation instead of raw events; the only output safe for model
    /// context (requires --follow; cannot be combined with --full or filters)
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

/// Apply the fail-closed result-correlation contract.
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
               AND json_extract(data, '$.soft') IS NOT TRUE
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
                if let Some(text) = obj.get("text").cloned() {
                    obj.entry("message").or_insert(text);
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

    let id_width = subs
        .iter()
        .filter_map(|sub| sub.get("id").and_then(|v| v.as_str()))
        .map(|s| s.chars().count())
        .max()
        .unwrap_or(2)
        .max(10);
    let caller_width = subs
        .iter()
        .filter_map(|sub| sub.get("caller").and_then(|v| v.as_str()))
        .map(|s| s.chars().count())
        .max()
        .unwrap_or(3)
        .max(12);

    println!(
        "{:<id_width$} {:<caller_width$} {:<10} FILTER",
        "ID", "FOR", "MODE"
    );
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

        println!("{id:<id_width$} {caller:<caller_width$} {mode:<10} {filter_display}");
        if let Some(on_hit) = sub.get("on_hit_text").and_then(|v| v.as_str()) {
            println!(
                "{:<id_width$} {:<caller_width$} {:<10} on-hit: {on_hit:?}",
                "", "", ""
            );
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
        match crate::identity::resolve_identity(db, None, None, None, None, None) {
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

/// An `--agent` typo otherwise looks exactly like "no events yet".
fn warn_unknown_agent_filters(db: &HcomDb, filters: &crate::core::filters::FilterMap) {
    let Some(names) = filters.get("instance") else {
        return;
    };
    for name in names {
        let seen = db
            .conn()
            .query_row(
                "SELECT 1 FROM events WHERE instance = ? LIMIT 1",
                rusqlite::params![name],
                |_| Ok(()),
            )
            .is_ok()
            || db.get_instance_full(name).ok().flatten().is_some();
        if !seen {
            eprintln!(
                "Warning: {}",
                crate::identity::describe_missing_agent(db, name)
            );
        }
    }
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
            crate::identity::resolve_identity(db, None, None, None, None, None)
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

// ── Event Listener ───────────────────────────────────────────────────────

pub(crate) const EVENTS_WAIT_ENDPOINT_KIND: &str = "events_wait";
pub(crate) const EVENTS_STREAM_ENDPOINT_KIND: &str = "events_stream";

const LISTENER_NOTIFY_POLL_INTERVAL: Duration = Duration::from_secs(5);
const LISTENER_FALLBACK_RECHECK_INTERVAL: Duration = Duration::from_millis(500);
const STREAM_RECHECK_INTERVAL: Duration = Duration::from_millis(500);
const LISTENER_TCP_POLL_INTERVAL: Duration = Duration::from_millis(200);

pub(crate) struct EventListenerOptions<'a> {
    pub after_id: Option<i64>,
    pub instance_name: Option<&'a str>,
    pub endpoint_kind: &'a str,
}

pub(crate) struct EventListener<'a> {
    db: &'a HcomDb,
    cursor: i64,
    has_explicit_cursor: bool,
    instance_name: Option<String>,
    endpoint_kind: String,
    notify_server: Option<TcpListener>,
    notify_port: Option<u16>,
}

impl<'a> EventListener<'a> {
    pub(crate) fn new(db: &'a HcomDb, options: EventListenerOptions<'a>) -> Self {
        let has_explicit_cursor = options.after_id.is_some();
        let cursor = options.after_id.unwrap_or_else(|| db.get_last_event_id());

        let mut notify_server: Option<TcpListener> = None;
        let mut notify_port: Option<u16> = None;
        if let Some(name) = options.instance_name
            && let Ok(server) = TcpListener::bind("127.0.0.1:0")
            && let Ok(addr) = server.local_addr()
        {
            let port = addr.port();
            server.set_nonblocking(true).ok();
            if db
                .upsert_notify_endpoint(name, options.endpoint_kind, port)
                .is_ok()
            {
                notify_server = Some(server);
                notify_port = Some(port);
            }
        }

        Self {
            db,
            cursor,
            has_explicit_cursor,
            instance_name: options.instance_name.map(str::to_string),
            endpoint_kind: options.endpoint_kind.to_string(),
            notify_server,
            notify_port,
        }
    }

    pub(crate) fn cursor(&self) -> i64 {
        self.cursor
    }

    pub(crate) fn has_explicit_cursor(&self) -> bool {
        self.has_explicit_cursor
    }

    pub(crate) fn is_listening(&self) -> bool {
        self.notify_server.is_some()
    }

    pub(crate) fn query_next_events(
        &mut self,
        filter_query: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Value>, rusqlite::Error> {
        let query = format!("SELECT * FROM events_v WHERE id > ?{filter_query} ORDER BY id");
        let mut stmt = self.db.conn().prepare(&query)?;
        let mut events = Vec::new();
        let mut rows = stmt.query(rusqlite::params![self.cursor])?;
        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => break,
            };
            if let Ok(id) = row.get::<_, i64>("id") {
                self.cursor = id;
            }
            if let Ok(event) = parse_event_row(row) {
                events.push(event);
                if let Some(max) = limit
                    && events.len() >= max
                {
                    break;
                }
            }
        }
        Ok(events)
    }

    pub(crate) fn query_next_event(
        &mut self,
        filter_query: &str,
    ) -> Result<Option<Value>, rusqlite::Error> {
        let mut events = self.query_next_events(filter_query, Some(1))?;
        Ok(events.pop())
    }

    pub(crate) fn drain_new_events(
        &mut self,
        filter_query: &str,
    ) -> Result<Vec<Value>, rusqlite::Error> {
        let boundary = self.db.get_last_event_id();
        if boundary <= self.cursor {
            return Ok(Vec::new());
        }
        let query =
            format!("SELECT * FROM events_v WHERE id > ? AND id <= ?{filter_query} ORDER BY id");
        let mut stmt = self.db.conn().prepare(&query)?;
        let mut events = Vec::new();
        let mut rows = stmt.query(rusqlite::params![self.cursor, boundary])?;
        while let Some(row) = rows.next()? {
            if let Ok(event) = parse_event_row(row) {
                events.push(event);
            }
        }
        self.cursor = boundary;
        Ok(events)
    }

    pub(crate) fn bounded_recheck_duration(&self, ceiling: Option<Duration>) -> Duration {
        let base = if self.endpoint_kind == EVENTS_STREAM_ENDPOINT_KIND {
            STREAM_RECHECK_INTERVAL
        } else if self.is_listening() {
            LISTENER_NOTIFY_POLL_INTERVAL
        } else {
            LISTENER_FALLBACK_RECHECK_INTERVAL
        };
        match ceiling {
            Some(limit) => base.min(limit),
            None => base,
        }
    }

    pub(crate) fn wait_tick(&self, max_wait: Duration) -> bool {
        static DUMMY_INTERRUPT: AtomicBool = AtomicBool::new(false);
        self.wait_tick_interruptible(max_wait, &DUMMY_INTERRUPT)
    }

    pub(crate) fn wait_tick_interruptible(
        &self,
        max_wait: Duration,
        interrupted: &AtomicBool,
    ) -> bool {
        let poll_end = Instant::now() + max_wait;
        let slice = Duration::from_millis(50);
        if let Some(ref server) = self.notify_server {
            while Instant::now() < poll_end {
                if interrupted.load(Ordering::Relaxed) {
                    return false;
                }
                if let Ok((conn, _)) = server.accept() {
                    let _ = conn.shutdown(std::net::Shutdown::Both);
                    return true;
                }
                let remaining = poll_end.saturating_duration_since(Instant::now());
                std::thread::sleep(LISTENER_TCP_POLL_INTERVAL.min(remaining).min(slice));
            }
            false
        } else {
            while Instant::now() < poll_end {
                if interrupted.load(Ordering::Relaxed) {
                    return false;
                }
                let remaining = poll_end.saturating_duration_since(Instant::now());
                std::thread::sleep(remaining.min(slice));
            }
            false
        }
    }

    pub(crate) fn cleanup(&mut self) {
        if let (Some(name), Some(_port)) = (&self.instance_name, self.notify_port.take()) {
            let _ = self.db.delete_notify_endpoint(name, &self.endpoint_kind);
        }
        self.notify_server = None;
    }
}

impl Drop for EventListener<'_> {
    fn drop(&mut self) {
        self.cleanup();
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
    let EventsWaitOptions {
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
        let remaining_secs = wait_timeout.saturating_sub(start.elapsed().as_secs());
        if remaining_secs == 0 {
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
        let mut wait_duration =
            listener.bounded_recheck_duration(Some(Duration::from_secs(remaining_secs)));
        if recovery_error_since.is_some() {
            wait_duration = wait_duration.min(next_recovery_retry.saturating_duration_since(now));
        }
        listener.wait_tick(wait_duration);
    }
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

fn cmd_events_stream(db: &HcomDb, args: &EventsStreamArgs, instance_name: Option<&str>) -> i32 {
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
            EventsSubcmd::Stream(stream_args) => {
                if args.wait.is_some()
                    || args.after_id.is_some()
                    || args.full
                    || args.all
                    || args.last.is_some()
                    || args.sql.is_some()
                    || args.device.is_some()
                    || args.filters.has_filters()
                {
                    eprintln!(
                        "Error: query-mode flags and filters before `stream` are not supported; pass stream flags and filters after `events stream`"
                    );
                    return 1;
                }
                return cmd_events_stream(db, stream_args, instance_name.as_deref());
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
    // --all also searches archives, where an agent may exist only historically;
    // --wait may legitimately target an agent that hasn't started yet.
    if !args.remote_fetch && !search_all && wait_timeout.is_none() {
        warn_unknown_agent_filters(db, &filters);
    }

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
        crate::cli_context::claim_inline_delivery();
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

    // Chronological by timestamp (relay-pulled events keep their original
    // time, so insertion order isn't chronological). Timestamps have second
    // resolution; break ties by id to keep same-second events in write order.
    all_events.sort_by_cached_key(|v| {
        (
            v.get("ts")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            v.get("id").and_then(|v| v.as_i64()).unwrap_or(0),
        )
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
    fn test_events_args_wait_no_value() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from(["events", "--wait", "--full"]).unwrap();
        assert_eq!(args.wait, Some(60)); // default_missing_value
        assert!(args.full);
    }

    #[test]
    fn test_events_args_no_wait() {
        use clap::Parser;
        let args = EventsArgs::try_parse_from(["events", "--full"]).unwrap();
        assert_eq!(args.wait, None);
        assert!(args.full);
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

    const UNRELATED_PROBE_TIMEOUT: Duration = Duration::from_millis(300);

    struct WaiterFixture {
        _temp: tempfile::TempDir,
        db_path: std::path::PathBuf,
        writer: HcomDb,
    }

    impl WaiterFixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let db_path = temp.path().join("events_wait_test.db");
            let writer = HcomDb::open_raw(&db_path).unwrap();
            writer.init_db().unwrap();
            Self {
                _temp: temp,
                db_path,
                writer,
            }
        }

        fn open_reader(&self) -> HcomDb {
            HcomDb::open_raw(&self.db_path).unwrap()
        }

        fn register_instance(&self, name: &str) {
            self.writer
                .conn()
                .execute(
                    "INSERT INTO instances (name, tool, status, created_at, last_event_id) VALUES (?1, 'test', 'active', 1000.0, 0)",
                    rusqlite::params![name],
                )
                .unwrap();
        }

        fn wait_for_notify_endpoint(&self, name: &str) -> bool {
            for _ in 0..100 {
                if self.writer.has_notify_endpoint_kind(name, "events_wait") {
                    return true;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            false
        }

        fn send_message(&self, from: &str, text: &str) {
            let msg_data = json!({
                "from": from,
                "text": text,
                "scope": "broadcast",
            });
            self.writer
                .log_event_with_ts("message", from, &msg_data, None)
                .unwrap();
        }

        fn send_status(&self, instance: &str, status: &str, detail: &str) {
            let status_data = json!({
                "status": status,
                "detail": detail,
            });
            self.writer
                .log_event_with_ts("status", instance, &status_data, None)
                .unwrap();
        }

        fn wake(&self, instance: &str) {
            crate::notify::wake(
                &self.writer,
                instance,
                &[crate::notify::WakeKind::EventsWait],
            );
        }
    }

    struct WaiterWorker {
        handle: std::thread::JoinHandle<i32>,
        rx: std::sync::mpsc::Receiver<i32>,
    }

    impl WaiterWorker {
        fn spawn(
            reader: HcomDb,
            filter_query: &'static str,
            timeout_secs: u64,
            instance: &'static str,
        ) -> Self {
            let (tx, rx) = std::sync::mpsc::channel();
            let handle = std::thread::spawn(move || {
                let code = events_wait(
                    &reader,
                    filter_query,
                    timeout_secs,
                    EventsWaitOptions {
                        after_id: None,
                        full_output: false,
                        filters: &HashMap::new(),
                        instance_name: Some(instance),
                        result_correlation: None,
                    },
                );
                let _ = tx.send(code);
                code
            });
            Self { handle, rx }
        }

        fn probe(&self, timeout: Duration) -> Result<i32, std::sync::mpsc::RecvTimeoutError> {
            self.rx.recv_timeout(timeout)
        }

        fn join(self) -> i32 {
            self.handle.join().unwrap_or(-1)
        }
    }

    #[test]
    fn test_events_wait_unrelated_unread_arriving_after_readiness() {
        let f = WaiterFixture::new();
        f.register_instance("waiter_arriving");
        let initial_cursor = f.writer.get_cursor("waiter_arriving");

        let worker = WaiterWorker::spawn(
            f.open_reader(),
            " AND (type = 'status')",
            5,
            "waiter_arriving",
        );
        let ready = f.wait_for_notify_endpoint("waiter_arriving");

        f.send_message("sender", "unrelated arriving message");
        f.wake("waiter_arriving");

        let probe = worker.probe(UNRELATED_PROBE_TIMEOUT);
        if probe.is_err() {
            f.send_status("waiter_arriving", "active", "matched");
            f.wake("waiter_arriving");
        }
        let code = worker.join();
        let endpoint_cleaned = !f
            .writer
            .has_notify_endpoint_kind("waiter_arriving", "events_wait");
        let cursor_after = f.writer.get_cursor("waiter_arriving");

        assert_eq!(cursor_after, initial_cursor);
        assert!(
            endpoint_cleaned,
            "notify endpoint must be cleaned up on exit"
        );
        assert!(
            ready && probe.is_err() && code == 0,
            "waiter must remain pending on unrelated unread: ready={ready}, probe={probe:?}, code={code}"
        );
    }

    #[test]
    fn test_events_wait_preexisting_unrelated_unread_does_not_satisfy_filter() {
        let f = WaiterFixture::new();
        f.register_instance("waiter_pre");
        f.send_message("sender", "preexisting unread message");
        let initial_cursor = f.writer.get_cursor("waiter_pre");

        let worker =
            WaiterWorker::spawn(f.open_reader(), " AND (type = 'status')", 1, "waiter_pre");

        let probe = worker.probe(UNRELATED_PROBE_TIMEOUT);
        let code = worker.join();
        let endpoint_cleaned = !f
            .writer
            .has_notify_endpoint_kind("waiter_pre", "events_wait");
        let cursor_after = f.writer.get_cursor("waiter_pre");

        assert_eq!(cursor_after, initial_cursor);
        assert!(
            endpoint_cleaned,
            "notify endpoint must be cleaned up on exit"
        );
        assert!(
            probe.is_err() && code == 1,
            "events_wait must not break with 0 on preexisting unread message: probe={probe:?}, code={code}"
        );
    }

    #[test]
    fn test_events_wait_matching_status_after_readiness_exits_zero() {
        let f = WaiterFixture::new();
        f.register_instance("waiter_matching");
        let initial_cursor = f.writer.get_cursor("waiter_matching");

        let worker = WaiterWorker::spawn(
            f.open_reader(),
            " AND (type = 'status')",
            5,
            "waiter_matching",
        );
        let ready = f.wait_for_notify_endpoint("waiter_matching");

        f.send_status("waiter_matching", "active", "ready");
        f.wake("waiter_matching");

        let code = worker.join();
        let endpoint_cleaned = !f
            .writer
            .has_notify_endpoint_kind("waiter_matching", "events_wait");
        let cursor_after = f.writer.get_cursor("waiter_matching");

        assert!(ready, "endpoint must be registered");
        assert!(
            endpoint_cleaned,
            "notify endpoint must be cleaned up on exit"
        );
        assert_eq!(cursor_after, initial_cursor);
        assert_eq!(code, 0, "matching status event must wake and exit 0");
    }

    #[test]
    fn test_events_wait_timeout_returns_one() {
        let f = WaiterFixture::new();
        f.register_instance("timeout_agent");
        let filters = HashMap::new();
        let reader = f.open_reader();
        let code = events_wait(
            &reader,
            " AND (type = 'nonexistent')",
            1,
            EventsWaitOptions {
                after_id: None,
                full_output: false,
                filters: &filters,
                instance_name: Some("timeout_agent"),
                result_correlation: None,
            },
        );
        assert_eq!(code, 1);
        assert!(
            !f.writer
                .has_notify_endpoint_kind("timeout_agent", "events_wait")
        );
    }

    #[test]
    fn test_events_wait_invalid_sql_returns_two() {
        let f = WaiterFixture::new();
        f.register_instance("sql_agent");
        let filters = HashMap::new();
        let reader = f.open_reader();
        let code = events_wait(
            &reader,
            " AND (invalid sql %%%)",
            1,
            EventsWaitOptions {
                after_id: None,
                full_output: false,
                filters: &filters,
                instance_name: Some("sql_agent"),
                result_correlation: None,
            },
        );
        assert_eq!(code, 2);
        assert!(
            !f.writer
                .has_notify_endpoint_kind("sql_agent", "events_wait")
        );
    }

    #[test]
    fn test_events_wait_runtime_sql_stepping_error_returns_two() {
        let f = WaiterFixture::new();
        f.register_instance("runtime_sql_agent");
        let filters = HashMap::new();
        let reader = f.open_reader();
        // Seed at least one event so the query steps rows
        f.writer
            .log_event("status", "runtime_sql_agent", &json!({"status": "active"}))
            .unwrap();
        let code = events_wait(
            &reader,
            " AND (json_extract('{not json}', '$.foo') = 'bar')",
            1,
            EventsWaitOptions {
                after_id: None,
                full_output: false,
                filters: &filters,
                instance_name: Some("runtime_sql_agent"),
                result_correlation: None,
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
                result_correlation: None,
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
                result_correlation: None,
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
                result_correlation: None,
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
