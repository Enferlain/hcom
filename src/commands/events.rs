//! `hcom events` command — query events, manage subscriptions.
//!
//!
//! Modes:
//! - Query: `hcom events [--last N] [--all] [--full] [--wait SEC] [--sql EXPR] [filters...]`
//! - Subscribe: `hcom events sub [list | SQL | filters...] [--once] [--for name]`
//! - Unsubscribe: `hcom events unsub <id>`
//! - Launch status: `hcom events launch [batch_id] [--timeout N]`
//! - Stream: `hcom events stream [--after-id N] [--full] [--timeout SEC]
//!   [--follow NAME [--compact [--heartbeat SEC]]] [filters...]`
//!
//! Generic stream output reuses the raw full/streamlined event projections
//! and may contain raw or secret-bearing event data (command text, message
//! bodies, file detail); only the `--follow --compact` projection is
//! intended as model-context-safe.
//!
//! Correlated result waits (`--wait --result-from NAME --thread ID --after-id
//! N`) exit `0` on the authoritative result, `1` on deadline, `2` on SQL
//! error, `3` when the worker stopped without a recoverable result, `4` on a
//! typed actionable blocker, and `5` on a launch failure. Every non-result
//! termination prints one structured outcome preserving the worker
//! generation, workflow thread, attempt cursor, blocker evidence, and
//! recovery guidance.

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResultCorrelation {
    exact_worker: String,
    instance_key: String,
    thread: String,
    after_id: i64,
}

/// One immutable worker generation selected at a durable attempt boundary.
///
/// The stop cursor is known when discovery finds a post-cursor stopped
/// snapshot. A live generation has no stop cursor until its matching lifecycle
/// event is observed by the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerGeneration {
    exact_worker: String,
    instance_key: String,
    after_id: i64,
    stop_event_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenerationEventDisposition {
    /// Not from the followed generation; skip without ending the stream.
    Ignore,
    /// From the followed generation; emit and keep following.
    Observe,
    /// The followed generation's matching stop boundary; emit it and end
    /// the stream.
    ObserveAndStop,
    /// The followed generation already ended before this event; end the
    /// stream without emitting activity from a later generation.
    Terminate,
}

/// Stateful boundary gate for a stream following one worker generation.
///
/// Message events carry an immutable sender key and are matched directly.
/// Other worker activity is name-scoped and is accepted only until the exact
/// generation's terminal stopped snapshot is encountered. Provider soft stops
/// mark execution-loop boundaries while retaining the same generation, so they
/// remain observable without ending the follow. Once terminally stopped, later
/// activity from a worker reusing the name is ignored even if a caller
/// accidentally continues feeding events to the gate. When discovery already
/// located the terminal stop boundary, an event past it means the stream armed
/// too late: the follow terminates instead of silently ignoring a reused name.
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
                    // Every production terminal stop carries an immutable
                    // snapshot. If a malformed event violates that invariant,
                    // fail closed rather than crossing into name reuse.
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

/// Resolve exactly one live or post-cursor stopped generation for `worker`.
///
/// Discovery is intentionally independent from any event filters so result
/// waits and compact streams can share generation identity without sharing
/// message-only terminal semantics. `label` names the caller-facing operation
/// (e.g. `--result-from`) and is embedded in the resolution errors so each
/// caller keeps its existing actionable error text.
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

    // A worker can terminate before a follower arms. The immutable pre-delete
    // snapshot is the only authoritative identity after its live row is gone.
    // Soft execution-loop stops retain the live generation and are generation
    // evidence, but never terminal boundaries. Keeping them in discovery makes
    // a later replacement under the same display name fail closed as ambiguous.
    // Ignore all stops at or before the caller's attempt cursor so historical
    // generations cannot be revived.
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

/// Parse a heartbeat interval in seconds, rejecting anything below one
/// second: a zero-second heartbeat would fire on every 500ms recheck poll
/// and degenerate into output spam, the exact failure the rate limit exists
/// to prevent.
fn parse_heartbeat_secs(value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|secs| *secs >= 1)
        .ok_or_else(|| format!("invalid heartbeat '{value}': must be at least 1 second"))
}

/// Args for `hcom events stream`.
///
/// Generic stream mode: emit every event matching the existing composable
/// filters in ascending durable-ID order and stay active until the optional
/// overall timeout. This is not a compact worker view and not a conversation
/// surface; it reads stored events and writes records to stdout only.
/// Generic output is NOT secret-safe: full and streamlined projections
/// carry raw event data (command text, message bodies, file detail), so
/// only feed it to a model after filtering for exactly what is wanted.
///
/// Worker-following mode (`--follow`) binds the stream to one exact worker
/// generation discovered at `--after-id` and ends at that generation's
/// matching stop boundary; it cannot be combined with filters. `--compact`
/// selects the typed compact status projection for the followed generation,
/// which is the only stream output safe for model context.
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
/// generation has stopped. The stop snapshot supplies immutable session and
/// transcript metadata; the provider adapter additionally requires the unique
/// workflow thread marker inside that transcript.
///
/// Soft stops intentionally count here: provider turn-end without an authored
/// result message is the existing one-shot transcript-recovery trigger. Only a
/// continuous generation follow treats a soft stop as nonterminal.
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
/// Preserves fields used in active filters. This is a token-reduction
/// projection, not a security boundary: streamlined output still carries raw
/// event data such as command text and message bodies, so only the compact
/// projection (`--follow --compact`) is model-context-safe.
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

/// Format a user-SQL prepare error, appending the exact public equivalent when
/// the failure is a recognizable wrong-column attempt (tracker 38).
fn sql_where_error(err: rusqlite::Error) -> String {
    let base = format!("Error in SQL WHERE clause: {err}");
    match crate::core::filters::sql_column_hint(&base) {
        Some(hint) => format!("{base}\nSQL hint: {hint}"),
        None => base,
    }
}

/// Query events from events_v view. Returns parsed event objects.
fn query_events(
    db: &HcomDb,
    filter_query: &str,
    last_n: usize,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<Vec<Value>, String> {
    let query =
        format!("SELECT * FROM events_v WHERE 1=1{filter_query} ORDER BY id DESC LIMIT {last_n}");

    let mut stmt = db.conn().prepare(&query).map_err(sql_where_error)?;

    let rows = stmt
        .query_map(params, |row| {
            let id: i64 = row.get("id")?;
            let ts: String = row.get("timestamp")?;
            let etype: String = row.get("type")?;
            let instance: String = row.get("instance")?;
            let data_str: String = row.get("data")?;
            Ok((id, ts, etype, instance, data_str))
        })
        .map_err(sql_where_error)?;

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
             \x20 --action VAL                      created | started | connected | ready | stopped | batch_launched | launch_failed | launch_blocked\n\
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

// ── Event Listener ───────────────────────────────────────────────────────

pub(crate) const EVENTS_WAIT_ENDPOINT_KIND: &str = crate::notify::WakeKind::EventsWait.as_str();
pub(crate) const EVENTS_STREAM_ENDPOINT_KIND: &str = crate::notify::WakeKind::EventsStream.as_str();

const LISTENER_NOTIFY_POLL_INTERVAL: Duration = Duration::from_secs(5);
const LISTENER_FALLBACK_RECHECK_INTERVAL: Duration = Duration::from_millis(500);
const STREAM_RECHECK_INTERVAL: Duration = Duration::from_millis(500);
const LISTENER_TCP_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Options for configuring an [`EventListener`].
pub(crate) struct EventListenerOptions<'a> {
    pub after_id: Option<i64>,
    pub instance_name: Option<&'a str>,
    pub endpoint_kind: &'a str,
}

/// Internal cursor-ordered event listener with optional TCP wake notification,
/// bounded database rechecks, and automatic cleanup.
///
/// Used by one-shot `events_wait` and continuous `events stream`.
/// The database stores one endpoint per `(instance, endpoint_kind)`, so the
/// newest same-kind listener owns targeted wakes. Port-guarded cleanup prevents
/// an older listener from deleting that replacement endpoint.
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
    /// Create and arm an event listener.
    ///
    /// Captures the starting cursor boundary before registering any notification
    /// plumbing so events arriving during setup are still observed.
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

    /// Current durable event cursor boundary.
    pub(crate) fn cursor(&self) -> i64 {
        self.cursor
    }

    /// Whether this listener was initialized with an explicit `--after-id`.
    pub(crate) fn has_explicit_cursor(&self) -> bool {
        self.has_explicit_cursor
    }

    /// Bound TCP notify port, if successfully registered.
    #[allow(dead_code)]
    pub(crate) fn endpoint_port(&self) -> Option<u16> {
        self.notify_port
    }

    /// Configured endpoint kind in `notify_endpoints`.
    #[allow(dead_code)]
    pub(crate) fn endpoint_kind(&self) -> &str {
        &self.endpoint_kind
    }

    /// Instance name associated with this listener, if any.
    #[allow(dead_code)]
    pub(crate) fn instance_name(&self) -> Option<&str> {
        self.instance_name.as_deref()
    }

    /// Whether this listener has an active TCP notification server.
    pub(crate) fn is_listening(&self) -> bool {
        self.notify_server.is_some()
    }

    /// Query new events arriving after `self.cursor` matching `filter_query`,
    /// ordered by `id ASC`.
    ///
    /// `self.cursor` advances past every examined row regardless of parse success.
    /// If `limit` is specified, stops scanning once `limit` parsed events have been collected.
    pub(crate) fn query_next_events(
        &mut self,
        filter_query: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Value>, rusqlite::Error> {
        let query = format!("SELECT * FROM events_v WHERE id > ?{filter_query} ORDER BY id");
        let mut stmt = self.db.conn().prepare(&query)?;
        let mut events = Vec::new();
        // Preserve `events --wait` compatibility: historically only statement
        // preparation errors produced SQL exit 2. Transient bind/step failures
        // ended this scan and were retried by the outer bounded loop.
        let Ok(mut rows) = stmt.query(rusqlite::params![self.cursor]) else {
            return Ok(events);
        };
        loop {
            let row = match rows.next() {
                Ok(Some(row)) => row,
                Ok(None) | Err(_) => break,
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

    /// Query the single next matching event arriving after `self.cursor`.
    /// Advances `self.cursor` up to that event (or the last unparseable row scanned).
    pub(crate) fn query_next_event(
        &mut self,
        filter_query: &str,
    ) -> Result<Option<Value>, rusqlite::Error> {
        let mut events = self.query_next_events(filter_query, Some(1))?;
        Ok(events.pop())
    }

    /// Query all currently available matching events arriving after `self.cursor`.
    #[allow(dead_code)]
    pub(crate) fn query_events(
        &mut self,
        filter_query: &str,
    ) -> Result<Vec<Value>, rusqlite::Error> {
        self.query_next_events(filter_query, None)
    }

    /// Query every currently available matching event after `self.cursor`,
    /// then advance the cursor past the entire scanned range — including rows
    /// the SQL filter excluded — so a long-lived stream never rescans a
    /// growing tail of non-matching events.
    ///
    /// The boundary jump is safe because event rows are immutable,
    /// `events.id` is AUTOINCREMENT (future rows always receive higher IDs),
    /// and the scan is capped at the durable max ID snapshotted before the
    /// query runs: a row excluded from `(cursor, boundary]` can never match
    /// later. If row iteration fails midway, the cursor only advances to the
    /// last row actually examined, preserving the step-wise guarantee of
    /// [`EventListener::query_next_events`].
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
        // Preserve `events --wait` compatibility: only statement preparation
        // errors surface; transient bind/step failures end this scan and are
        // retried by the outer bounded loop.
        let Ok(mut rows) = stmt.query(rusqlite::params![self.cursor, boundary]) else {
            return Ok(events);
        };
        let mut examined = self.cursor;
        let mut scan_complete = false;
        loop {
            let row = match rows.next() {
                Ok(Some(row)) => row,
                Ok(None) => {
                    scan_complete = true;
                    break;
                }
                Err(_) => break,
            };
            if let Ok(id) = row.get::<_, i64>("id") {
                examined = id;
            }
            if let Ok(event) = parse_event_row(row) {
                events.push(event);
            }
        }
        self.cursor = if scan_complete { boundary } else { examined };
        Ok(events)
    }

    /// Calculate the bounded recheck duration based on listener state and an optional ceiling.
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

    /// Wait for a TCP notification or until `max_wait` elapses.
    ///
    /// When a notification server is present, polls for incoming connections in short
    /// intervals, waking early on arrival. When absent, sleeps for `max_wait`.
    /// Returns `true` if a notification connection was received, or `false` on timeout.
    pub(crate) fn wait_tick(&self, max_wait: Duration) -> bool {
        static DUMMY_INTERRUPT: AtomicBool = AtomicBool::new(false);
        self.wait_tick_interruptible(max_wait, &DUMMY_INTERRUPT)
    }

    /// Wait for a TCP notification or until `max_wait` elapses or `interrupted` is flagged.
    ///
    /// Periodically checks `interrupted` so loops can break cleanly upon cancellation.
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

    /// Remove this listener's registered notify endpoint from the database.
    ///
    /// Idempotent; only removes the specific `(instance, kind)` endpoint owned by this listener.
    pub(crate) fn cleanup(&mut self) {
        if let (Some(name), Some(port)) = (&self.instance_name, self.notify_port.take()) {
            let _ = self
                .db
                .delete_notify_endpoint_if_port(name, &self.endpoint_kind, port);
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
    // Correlated waits terminate on more than the result message: a typed
    // actionable blocker or a launch failure observed after the pre-launch
    // cursor also ends the attempt with a structured outcome. The scan is
    // cursor-anchored (not registration-anchored), so a transition that fired
    // between launch readiness and this wait is found on the first poll.
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
                let output = if full_output {
                    event
                } else {
                    streamline_event(&event, filters)
                };
                println!("{}", serde_json::to_string(&output).unwrap_or_default());
                break 0;
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("{}", sql_where_error(e));
                break 2;
            }
        }

        // Terminal non-result outcomes (actionable blocker, launch failure).
        // Scan failures are non-fatal: a transient read error must not abort a
        // wait that could still complete with the authoritative result.
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

        // For a legacy unfiltered wait, an older unread inbox message is still a
        // useful interrupt. Filtered waits and explicit-cursor waits must only
        // complete on their declared event boundary: otherwise unrelated or
        // already-consumed messages can produce a false successful match.
        if filter_query.is_empty()
            && !listener.has_explicit_cursor()
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

/// Write a record line to a writer and flush it immediately.
fn write_and_flush_record_to<W: Write>(writer: &mut W, line: &str) -> std::io::Result<()> {
    writeln!(writer, "{line}")?;
    writer.flush()
}

/// Serialize and flush one compact record as its own NDJSON line.
fn write_compact_record_to<W: Write>(
    writer: &mut W,
    record: &crate::core::progress::CompactRecord,
) -> std::io::Result<()> {
    let line = serde_json::to_string(record)
        .expect("compact record is data-only and cannot fail to serialize");
    write_and_flush_record_to(writer, &line)
}

/// Continuous stream: emit every matching event in ascending durable-ID order
/// and remain active until interrupted, broken pipe, the overall timeout expires
/// (exit `0`), or — when bound to one worker generation — that generation's
/// matching stop boundary.
///
/// Unlike [`events_wait`], no unread-inbox fallback applies and nothing is
/// synthesized on deadline: a stream that ends at its timeout has already
/// emitted every match that became durable, which is a normal completion.
/// Progress is guaranteed by bounded 500ms SQLite rechecks plus targeted TCP
/// wakes from wake-all senders; a missed wake never loses data, it only delays
/// the next recheck. Records are emitted through the existing streamlined/full
/// projections and explicitly flushed after each emitted record.
/// BrokenPipe on stdout is treated as a clean successful termination (exit `0`).
/// User interruption (Unix SIGINT/SIGTERM, Windows Ctrl-C/break/close) is
/// handled cleanly via [`crate::sys::signal`] helpers (exit `0`).
/// Every termination path removes only this listener's `events_stream` endpoint
/// and never mutates the observed worker's status, state, or generation.
///
/// A generation follow carries two fail-closed arming invariants: the drain
/// must be unfiltered (caller SQL could exclude the exact stop boundary and
/// admit activity from a reused name), and the listener cursor must equal the
/// generation's discovery cursor (arming later could skip past the boundary).
///
/// With a compact config the same follow gating drives the typed
/// [`crate::core::compact`] projection instead of raw event projections: only
/// bounded, secret-safe compact records can reach stdout, and optional quiet
/// heartbeats are polled between drains.
fn events_stream(
    db: &HcomDb,
    filter_query: &str,
    stream_timeout: Option<u64>,
    options: EventsStreamOptions<'_>,
) -> i32 {
    let mut stdout = std::io::stdout().lock();
    events_stream_to(db, filter_query, stream_timeout, &mut stdout, options)
}

/// Writer-general core of [`events_stream`] so the generation-follow binding
/// and compact projection can be exercised end to end without touching
/// process stdout.
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

    // Compact records identify their exact worker generation, so the typed
    // projection exists only inside a generation follow.
    let mut compact_classifier = None;
    if let Some(config) = compact {
        let Some(follow) = generation_follow.as_ref() else {
            eprintln!("Error: compact mode requires generation following");
            return 1;
        };
        compact_classifier = Some(CompactClassifier::new(
            follow.generation.instance_key.clone(),
            config,
        ));
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

        // Query for new matching events and emit the whole ordered batch.
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
                        // Fail closed if an internal caller skipped past a
                        // known boundary or a malformed terminal event cannot
                        // be tied safely to an immutable generation.
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
                        // Compact mode replaces the raw projections: only
                        // bounded, typed records can reach stdout here.
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
                eprintln!("{}", sql_where_error(e));
                return 2;
            }
        }

        if interrupted.load(Ordering::Relaxed) {
            return 0;
        }

        // Quiet heartbeats are polled between drains: promptness is bounded
        // by the listener recheck cadence, and the classifier rate-limits
        // them to one per heartbeat interval.
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

        // Remain active until the optional overall deadline.
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

/// Handle `hcom events stream`.
///
/// Exit codes: `0` when the stream completes cleanly (timeout, broken pipe,
/// signal interruption, or a followed generation's stop boundary), `1` on
/// filter errors, query-mode flag conflicts, correlation errors, or stdout
/// write errors, `2` on SQL errors.
fn cmd_events_stream(db: &HcomDb, args: &EventsStreamArgs, instance_name: Option<&str>) -> i32 {
    // Convert clap filter args to FilterMap
    let mut filters = args.filters.to_filter_map();

    // Worker following owns the whole drain: caller filters could exclude the
    // exact stop boundary and admit activity from a reused name.
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

    // Build filter SQL (validation happens inside build_sql_from_flags)
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
                // `events stream` owns its own flag set. A query-mode flag
                // typed before the subcommand would otherwise parse and then
                // be silently ignored, so reject the combination instead of
                // guessing which surface the caller meant.
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
    use clap::Parser;

    #[test]
    fn events_accepts_limit_alias() {
        // Tracker 38: `hcom events --limit N` must map to --last instead of a
        // clap rejection.
        let args = EventsArgs::try_parse_from(["events", "--limit", "5"]).unwrap();
        assert_eq!(args.last, Some(5));
    }

    #[test]
    fn events_canonical_last_still_parses() {
        let args = EventsArgs::try_parse_from(["events", "--last", "5"]).unwrap();
        assert_eq!(args.last, Some(5));
    }

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
    fn streamline_preserves_sender_instance_key_for_correlated_waits() {
        // Compatibility pin before listener extraction: a correlated result
        // wait always filters on sender_instance_key, so its streamlined
        // output must keep the generation key scripts use to verify which
        // worker generation produced the result.
        let event = json!({
            "id": 7,
            "ts": "2025-02-23T15:30:45.123456",
            "type": "message",
            "instance": "claude-worker",
            "data": {
                "from": "claude-worker",
                "intent": "inform",
                "text": "correlated result",
                "sender_instance_key": "claude-worker@1000.000000",
            }
        });

        let mut filters = HashMap::new();
        filters.insert(
            "sender_instance_key".to_string(),
            vec!["claude-worker@1000.000000".to_string()],
        );
        let result = streamline_event(&event, &filters);

        assert_eq!(
            result["data"]["sender_instance_key"].as_str(),
            Some("claude-worker@1000.000000")
        );
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
    fn worker_generation_discovery_fails_closed_when_missing() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("missing-generation.db")).unwrap();
        db.ensure_schema().unwrap();

        let error =
            discover_worker_generation(&db, "--result-from", "missing-worker", 0).unwrap_err();
        assert_eq!(
            error,
            "--result-from worker 'missing-worker' has no generation after the attempt cursor"
        );
    }

    #[test]
    fn worker_generation_discovery_recovers_stopped_tagged_generation() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stopped-generation.db")).unwrap();
        db.ensure_schema().unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "life",
            "worker",
            &json!({
                "action": "stopped",
                "snapshot": {
                    "name": "worker",
                    "tag": "impl",
                    "created_at": 1000.0
                }
            }),
        )
        .unwrap();
        let stop_event_id = db.get_last_event_id();

        let generation =
            discover_worker_generation(&db, "--result-from", "impl-worker", cursor).unwrap();
        assert_eq!(generation.exact_worker, "worker");
        assert_eq!(generation.instance_key, "worker@1000.000000");
        assert_eq!(generation.after_id, cursor);
        assert_eq!(generation.stop_event_id, Some(stop_event_id));
    }

    #[test]
    fn worker_generation_discovery_resolves_live_generation() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("live-generation.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tag, created_at) VALUES ('worker', 'impl', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();

        let generation =
            discover_worker_generation(&db, "followed", "impl-worker", cursor).unwrap();
        assert_eq!(generation.exact_worker, "worker");
        assert_eq!(generation.instance_key, "worker@1000.000000");
        assert_eq!(generation.after_id, cursor);
        assert_eq!(
            generation.stop_event_id, None,
            "a live generation has no stop boundary yet"
        );
    }

    #[test]
    fn worker_generation_discovery_does_not_treat_soft_stop_as_terminal() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("soft-stop-generation.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
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

        let generation = discover_worker_generation(&db, "--follow", "worker", cursor).unwrap();
        assert_eq!(generation.instance_key, "worker@1000.000000");
        assert_eq!(
            generation.stop_event_id, None,
            "a soft execution-loop stop must not become the generation boundary"
        );
    }

    #[test]
    fn worker_generation_discovery_rejects_replacement_after_soft_stop() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("soft-stop-reuse.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
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
        db.delete_instance("worker").unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 2000.0)",
                [],
            )
            .unwrap();

        let error = discover_worker_generation(&db, "--follow", "worker", cursor).unwrap_err();
        assert_eq!(
            error,
            "--follow worker 'worker' resolves to multiple generations after the attempt cursor"
        );
    }

    #[test]
    fn worker_generation_discovery_rejects_rapid_name_reuse() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("reused-generation.db")).unwrap();
        db.ensure_schema().unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "life",
            "worker",
            &json!({
                "action": "stopped",
                "snapshot": { "name": "worker", "created_at": 1000.0 }
            }),
        )
        .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 2000.0)",
                [],
            )
            .unwrap();

        let error = discover_worker_generation(&db, "--result-from", "worker", cursor).unwrap_err();
        assert_eq!(
            error,
            "--result-from worker 'worker' resolves to multiple generations after the attempt cursor"
        );
    }

    #[test]
    fn generation_follow_stops_at_exact_boundary_and_ignores_reused_name() {
        let generation = WorkerGeneration {
            exact_worker: "worker".to_string(),
            instance_key: "worker@1000.000000".to_string(),
            after_id: 10,
            stop_event_id: None,
        };
        let mut follow = WorkerGenerationFollow {
            generation,
            stopped: false,
        };

        assert_eq!(
            follow.classify(&json!({
                "id": 11,
                "type": "status",
                "instance": "worker",
                "data": { "status": "active" }
            })),
            GenerationEventDisposition::Observe
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 12,
                "type": "message",
                "instance": "worker",
                "data": { "sender_instance_key": "worker@2000.000000" }
            })),
            GenerationEventDisposition::Ignore,
            "messages require the immutable generation key"
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 12,
                "type": "status",
                "instance": "other-worker",
                "data": { "status": "active" }
            })),
            GenerationEventDisposition::Ignore,
            "name-scoped events from a different instance must not enter the follow"
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 12,
                "type": "life",
                "instance": "worker",
                "data": {
                    "action": "stopped",
                    "placeholder": true,
                    "snapshot": { "name": "worker", "created_at": 1000.0 }
                }
            })),
            GenerationEventDisposition::Ignore,
            "placeholder stops are not lifecycle boundaries for the generation"
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 12,
                "type": "life",
                "instance": "worker",
                "data": {
                    "action": "stopped",
                    "snapshot": { "name": "worker", "created_at": 2000.0 }
                }
            })),
            GenerationEventDisposition::Ignore,
            "another generation's stop must not be emitted or terminate this follow"
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 12,
                "type": "life",
                "instance": "worker",
                "data": {
                    "action": "stopped",
                    "soft": true,
                    "snapshot": { "name": "worker", "created_at": 1000.0 }
                }
            })),
            GenerationEventDisposition::Observe,
            "a soft stop remains visible without ending the live generation"
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 12,
                "type": "status",
                "instance": "worker",
                "data": { "status": "listening" }
            })),
            GenerationEventDisposition::Observe,
            "the same generation must remain observable after its soft stop"
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 13,
                "type": "life",
                "instance": "worker",
                "data": {
                    "action": "stopped",
                    "snapshot": { "name": "worker", "created_at": 1000.0 }
                }
            })),
            GenerationEventDisposition::ObserveAndStop
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 14,
                "type": "status",
                "instance": "worker",
                "data": { "status": "active", "created_at": 2000.0 }
            })),
            GenerationEventDisposition::Ignore,
            "later activity from a reused display name must not cross the stop boundary"
        );
    }

    #[test]
    fn generation_follow_replays_stopped_generation_only_through_known_boundary() {
        let mut follow = WorkerGenerationFollow {
            generation: WorkerGeneration {
                exact_worker: "worker".to_string(),
                instance_key: "worker@1000.000000".to_string(),
                after_id: 20,
                stop_event_id: Some(22),
            },
            stopped: false,
        };

        assert_eq!(
            follow.classify(&json!({
                "id": 21,
                "type": "status",
                "instance": "worker",
                "data": { "status": "active" }
            })),
            GenerationEventDisposition::Observe
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 22,
                "type": "life",
                "instance": "worker",
                "data": {
                    "action": "stopped",
                    "snapshot": { "name": "worker", "created_at": 1000.0 }
                }
            })),
            GenerationEventDisposition::ObserveAndStop,
            "replaying a discovered post-cursor stop must end the follow at its boundary"
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 23,
                "type": "status",
                "instance": "worker",
                "data": { "status": "active" }
            })),
            GenerationEventDisposition::Ignore
        );
    }

    #[test]
    fn generation_follow_fails_closed_on_unkeyable_terminal_stop() {
        let mut follow = WorkerGenerationFollow {
            generation: WorkerGeneration {
                exact_worker: "worker".to_string(),
                instance_key: "worker@1000.000000".to_string(),
                after_id: 20,
                stop_event_id: None,
            },
            stopped: false,
        };

        assert_eq!(
            follow.classify(&json!({
                "id": 21,
                "type": "life",
                "instance": "worker",
                "data": { "action": "stopped" }
            })),
            GenerationEventDisposition::Terminate,
            "a malformed terminal stop under the followed name must fail closed"
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 22,
                "type": "status",
                "instance": "worker",
                "data": { "status": "active" }
            })),
            GenerationEventDisposition::Ignore
        );
    }

    #[test]
    fn generation_follow_terminates_when_armed_past_the_known_stop_boundary() {
        let mut follow = WorkerGenerationFollow {
            generation: WorkerGeneration {
                exact_worker: "worker".to_string(),
                instance_key: "worker@1000.000000".to_string(),
                after_id: 20,
                stop_event_id: Some(22),
            },
            stopped: false,
        };

        assert_eq!(
            follow.classify(&json!({
                "id": 23,
                "type": "status",
                "instance": "worker",
                "data": { "status": "active", "created_at": 2000.0 }
            })),
            GenerationEventDisposition::Terminate,
            "a stream armed after the boundary must end instead of trailing a reused name"
        );
        assert_eq!(
            follow.classify(&json!({
                "id": 24,
                "type": "status",
                "instance": "worker",
                "data": { "status": "active", "created_at": 2000.0 }
            })),
            GenerationEventDisposition::Ignore
        );
    }

    #[test]
    fn generation_follow_stream_emits_through_exact_stop_boundary() {
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
    fn generation_follow_stream_continues_through_soft_stop() {
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
        assert!(lines[2]["data"].get("soft").is_none());
    }

    #[test]
    fn generation_follow_stream_skips_unrelated_and_wrong_generation_activity() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("follow-stream-gating.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let generation = discover_worker_generation(&db, "followed", "worker", cursor).unwrap();

        db.log_event("status", "other-worker", &json!({ "status": "active" }))
            .unwrap();
        db.log_event(
            "message",
            "worker",
            &json!({
                "from": "worker",
                "intent": "inform",
                "text": "reused generation message",
                "sender_instance_key": "worker@2000.000000"
            }),
        )
        .unwrap();
        db.log_event("status", "worker", &json!({ "status": "blocked" }))
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
        assert_eq!(exit, 0, "a follow stream that reaches its timeout exits 0");
        let records = String::from_utf8(output).unwrap();
        let lines: Vec<Value> = records
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            lines.len(),
            1,
            "other instances and reused-generation messages must not be emitted"
        );
        assert_eq!(lines[0]["type"], "status");
        assert_eq!(lines[0]["instance"], "worker");
        assert_eq!(lines[0]["data"]["status"], "blocked");
    }

    #[test]
    fn generation_follow_stream_enforces_arming_invariants() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("follow-invariants.db")).unwrap();
        db.ensure_schema().unwrap();
        let follow = WorkerGenerationFollow {
            generation: WorkerGeneration {
                exact_worker: "worker".to_string(),
                instance_key: "worker@1000.000000".to_string(),
                after_id: 7,
                stop_event_id: None,
            },
            stopped: false,
        };
        let filters = HashMap::new();

        // Caller SQL could exclude the exact stop boundary of the followed
        // generation and admit activity from a reused name.
        assert_eq!(
            events_stream_to(
                &db,
                " AND (instance = 'worker')",
                Some(1),
                &mut Vec::new(),
                EventsStreamOptions {
                    after_id: Some(7),
                    full_output: true,
                    filters: &filters,
                    instance_name: None,
                    generation_follow: Some(follow.clone()),
                    compact: None,
                },
            ),
            1,
            "a follow drain combined with caller filters must fail closed"
        );

        // A listener armed away from the discovery cursor could skip past the
        // stop boundary between the two cursors.
        assert_eq!(
            events_stream_to(
                &db,
                "",
                Some(1),
                &mut Vec::new(),
                EventsStreamOptions {
                    after_id: None,
                    full_output: true,
                    filters: &filters,
                    instance_name: None,
                    generation_follow: Some(follow.clone()),
                    compact: None,
                },
            ),
            1,
            "a follow stream must not arm at an implicit current cursor"
        );
        assert_eq!(
            events_stream_to(
                &db,
                "",
                Some(1),
                &mut Vec::new(),
                EventsStreamOptions {
                    after_id: Some(8),
                    full_output: true,
                    filters: &filters,
                    instance_name: None,
                    generation_follow: Some(follow),
                    compact: None,
                },
            ),
            1,
            "a follow stream must arm exactly at its discovery cursor"
        );
    }

    #[test]
    fn compact_stream_projects_typed_records_and_ends_at_stop_boundary() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("compact-stream-projection.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let generation = discover_worker_generation(&db, "--follow", "worker", cursor).unwrap();

        // A noisy, secret-bearing trace: repeated file bursts, repeated
        // command bursts with credentials, one message body, and the stop.
        for id in 0..50 {
            db.log_event(
                "status",
                "worker",
                &json!({
                    "status": "active",
                    "context": "tool:Edit",
                    "detail": format!("src/file{id}.rs"),
                }),
            )
            .unwrap();
        }
        for id in 0..50 {
            db.log_event(
                "status",
                "worker",
                &json!({
                    "status": "active",
                    "context": "tool:Bash",
                    "detail": format!("curl -H 'Authorization: Bearer secret-{id}' https://internal.example"),
                }),
            )
            .unwrap();
        }
        db.log_event(
            "message",
            "worker",
            &json!({
                "from": "worker",
                "intent": "inform",
                "text": "the database password is hunter2",
                "sender_instance_key": "worker@1000.000000",
            }),
        )
        .unwrap();
        db.log_event(
            "life",
            "worker",
            &json!({
                "action": "stopped",
                "snapshot": { "name": "worker", "created_at": 1000.0 },
            }),
        )
        .unwrap();

        // A frozen manual clock keeps every drain inside one cadence window,
        // so the record count is deterministic.
        let clock_state = std::sync::Arc::new(std::sync::Mutex::new(Instant::now()));
        let clock = {
            let state = std::sync::Arc::clone(&clock_state);
            std::sync::Arc::new(move || *state.lock().unwrap())
        };
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
                full_output: false,
                filters: &filters,
                instance_name: None,
                generation_follow: Some(WorkerGenerationFollow {
                    generation,
                    stopped: false,
                }),
                compact: Some(CompactStreamConfig {
                    heartbeat: None,
                    clock,
                }),
            },
        );
        assert_eq!(exit, 0);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the stop boundary must end the compact stream before its timeout"
        );

        let text = String::from_utf8(output).unwrap();
        assert!(
            !text.contains("secret-"),
            "raw command arguments must never serialize: {text}"
        );
        assert!(
            !text.contains("Authorization"),
            "raw command text must never serialize: {text}"
        );
        assert!(
            !text.contains("hunter2"),
            "message bodies must never serialize: {text}"
        );

        let lines: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        // Bounded regardless of the 101 noisy events: one phase, one file
        // record, one command record, and the terminal stopped phase.
        assert_eq!(
            lines.len(),
            4,
            "a noisy trace must produce a bounded compact record count: {text}"
        );
        assert_eq!(
            lines[0]["activity"],
            json!({"type": "phase", "phase": "active"})
        );
        assert_eq!(
            lines[1]["activity"],
            json!({"type": "file", "path": "src/file0.rs"})
        );
        assert_eq!(
            lines[2]["activity"],
            json!({"type": "command", "category": "other"})
        );
        assert_eq!(
            lines[3]["activity"],
            json!({"type": "phase", "phase": "stopped"})
        );
        for line in &lines {
            assert_eq!(line["schema_version"], 1);
            assert_eq!(line["generation"], "worker@1000.000000");
            assert!(line["cursor"].as_i64().is_some());
            assert!(line["ts"].as_str().is_some());
            assert!(line.get("thread").is_none());
        }
    }

    #[test]
    fn compact_stream_heartbeats_are_bounded_while_quiet() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("compact-stream-heartbeat.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let generation = discover_worker_generation(&db, "--follow", "worker", cursor).unwrap();

        let phase_event = db
            .log_event("status", "worker", &json!({ "status": "active" }))
            .unwrap();

        // Deterministic injected timing: the classifier reads the clock
        // exactly once per observe and once per heartbeat poll. One event is
        // pre-logged, so calls 1 (observe) and 2 (first poll) stay frozen,
        // and every later poll jumps an hour ahead: the second poll fires the
        // heartbeat, and the rate limit keeps all further polls quiet no
        // matter how often the 500ms recheck loop runs before the 1s timeout.
        let clock_calls = Arc::new(std::sync::Mutex::new(0u32));
        let clock: crate::core::compact::CompactClock = {
            let calls = Arc::clone(&clock_calls);
            Arc::new(move || {
                let mut count = calls.lock().unwrap();
                *count += 1;
                if *count <= 2 {
                    Instant::now()
                } else {
                    Instant::now() + Duration::from_secs(3600)
                }
            })
        };

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
                instance_name: None,
                generation_follow: Some(WorkerGenerationFollow {
                    generation,
                    stopped: false,
                }),
                compact: Some(CompactStreamConfig {
                    heartbeat: Some(Duration::from_secs(1)),
                    clock,
                }),
            },
        );
        assert_eq!(exit, 0, "a quiet compact stream ends at its timeout");

        let text = String::from_utf8(output).unwrap();
        let lines: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            lines.len(),
            2,
            "exactly the phase record plus one rate-limited heartbeat: {text}"
        );
        assert_eq!(
            lines[0]["activity"],
            json!({"type": "phase", "phase": "active"})
        );
        assert_eq!(
            lines[1]["activity"],
            json!({"type": "heartbeat", "phase": "active"}),
            "heartbeats carry the last known phase"
        );
        assert_eq!(lines[1]["cursor"], json!(phase_event));
        assert_eq!(lines[1]["generation"], "worker@1000.000000");
    }

    #[test]
    fn compact_stream_requires_a_generation_follow() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("compact-without-follow.db")).unwrap();
        db.ensure_schema().unwrap();

        let filters = HashMap::new();
        let clock: crate::core::compact::CompactClock = Arc::new(Instant::now);
        assert_eq!(
            events_stream_to(
                &db,
                "",
                Some(1),
                &mut Vec::new(),
                EventsStreamOptions {
                    after_id: Some(0),
                    full_output: false,
                    filters: &filters,
                    instance_name: None,
                    generation_follow: None,
                    compact: Some(CompactStreamConfig {
                        heartbeat: None,
                        clock,
                    }),
                },
            ),
            1,
            "compact records identify a worker generation, so compact mode without following must fail closed"
        );
    }

    #[test]
    fn compact_stream_flags_parse_and_fail_closed() {
        use clap::Parser;

        let args = EventsArgs::try_parse_from([
            "events",
            "stream",
            "--after-id",
            "7",
            "--follow",
            "kuma",
            "--compact",
            "--heartbeat",
            "30",
            "--timeout",
            "60",
        ])
        .unwrap();
        match args.subcmd {
            Some(EventsSubcmd::Stream(ref stream)) => {
                assert_eq!(stream.follow.as_deref(), Some("kuma"));
                assert!(stream.compact);
                assert_eq!(stream.heartbeat, Some(30));
            }
            _ => panic!("Expected Stream subcommand"),
        }

        // --compact selects the projection only inside a follow.
        assert!(EventsArgs::try_parse_from(["events", "stream", "--compact"]).is_err());
        // Following needs an explicit attempt cursor.
        assert!(EventsArgs::try_parse_from(["events", "stream", "--follow", "kuma"]).is_err());
        // Heartbeats exist only in compact mode.
        assert!(
            EventsArgs::try_parse_from([
                "events",
                "stream",
                "--follow",
                "kuma",
                "--after-id",
                "1",
                "--heartbeat",
                "5",
            ])
            .is_err()
        );
        // Compact owns the output projection, so --full is a conflict.
        assert!(
            EventsArgs::try_parse_from([
                "events",
                "stream",
                "--follow",
                "kuma",
                "--after-id",
                "1",
                "--compact",
                "--full",
            ])
            .is_err()
        );
        // A zero-second heartbeat would degenerate into per-poll output.
        assert!(
            EventsArgs::try_parse_from([
                "events",
                "stream",
                "--follow",
                "kuma",
                "--after-id",
                "1",
                "--compact",
                "--heartbeat",
                "0",
            ])
            .is_err(),
            "--heartbeat 0 must be rejected at parse time"
        );
        // One second is the smallest accepted interval.
        let minimal = EventsArgs::try_parse_from([
            "events",
            "stream",
            "--follow",
            "kuma",
            "--after-id",
            "1",
            "--compact",
            "--heartbeat",
            "1",
        ])
        .unwrap();
        match minimal.subcmd {
            Some(EventsSubcmd::Stream(ref stream)) => assert_eq!(stream.heartbeat, Some(1)),
            _ => panic!("Expected Stream subcommand"),
        }
    }

    #[test]
    fn compact_stream_drops_hostile_file_detail() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("compact-hostile-detail.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('worker', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let generation = discover_worker_generation(&db, "--follow", "worker", cursor).unwrap();

        // File-context detail that is not a displayable path must never enter
        // the compact output, while ordinary paths still project.
        db.log_event(
            "status",
            "worker",
            &json!({
                "status": "active",
                "context": "tool:Write",
                "detail": "src/lib.rs\nrm -rf /",
            }),
        )
        .unwrap();
        db.log_event(
            "status",
            "worker",
            &json!({
                "status": "active",
                "context": "tool:Edit",
                "detail": "password=hunter2 as free-form prose",
            }),
        )
        .unwrap();
        db.log_event(
            "status",
            "worker",
            &json!({
                "status": "active",
                "context": "tool:Write",
                "detail": "src/\u{202E}spoof.rs",
            }),
        )
        .unwrap();
        db.log_event(
            "status",
            "worker",
            &json!({ "status": "active", "context": "tool:Edit", "detail": "src/clean.rs" }),
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
                instance_name: None,
                generation_follow: Some(WorkerGenerationFollow {
                    generation,
                    stopped: false,
                }),
                compact: Some(CompactStreamConfig {
                    heartbeat: None,
                    clock: Arc::new(Instant::now),
                }),
            },
        );
        assert_eq!(exit, 0);

        let text = String::from_utf8(output).unwrap();
        assert!(
            !text.contains("rm -rf") && !text.contains("hunter2") && !text.contains("spoof"),
            "hostile file detail must never serialize: {text}"
        );
        let lines: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            lines.len(),
            2,
            "one phase record plus the single clean file record: {text}"
        );
        assert_eq!(
            lines[1]["activity"],
            json!({"type": "file", "path": "src/clean.rs"})
        );
    }

    fn parse_stream_args(argv: &[&str]) -> EventsStreamArgs {
        use clap::Parser;
        match EventsArgs::try_parse_from(argv).unwrap().subcmd {
            Some(EventsSubcmd::Stream(stream)) => stream,
            _ => panic!("Expected Stream subcommand"),
        }
    }

    #[test]
    fn compact_stream_cmd_rejects_filters_and_unresolvable_generations() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("compact-stream-cmd.db")).unwrap();
        db.ensure_schema().unwrap();

        // Caller filters could exclude the exact stop boundary.
        let filtered = parse_stream_args(&[
            "events",
            "stream",
            "--after-id",
            "1",
            "--follow",
            "kuma",
            "--compact",
            "--type",
            "status",
        ]);
        assert_eq!(cmd_events_stream(&db, &filtered, None), 1);

        // A stream generic follow of an unresolvable worker fails closed.
        let generic_follow =
            parse_stream_args(&["events", "stream", "--after-id", "1", "--follow", "ghost"]);
        assert_eq!(cmd_events_stream(&db, &generic_follow, None), 1);

        // So does its compact projection.
        let compact_follow = parse_stream_args(&[
            "events",
            "stream",
            "--after-id",
            "1",
            "--follow",
            "ghost",
            "--compact",
        ]);
        assert_eq!(cmd_events_stream(&db, &compact_follow, None), 1);
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
            crate::core::result_wait::RESULT_UNAVAILABLE_EXIT
        );
    }

    #[test]
    fn mid_wait_worker_cancellation_returns_result_unavailable() {
        // Tracker hcom-dbj: the exact worker generation being cancelled
        // (stopped without a recoverable transcript) while the wait is live
        // must end the attempt with the structured unavailable exit instead of
        // running to the deadline. The pre-registration variant is covered by
        // unsupported_stopped_provider_returns_result_unavailable.
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("mid-wait-cancel.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, created_at)
                 VALUES ('codex-worker', 'codex', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let (correlation, filters) =
            arm_result_wait(&db, "codex-worker", "cancelled-workflow", cursor);

        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            let mut writer_db = HcomDb::open_raw(&db_path).unwrap();
            writer_db.ensure_schema().unwrap();
            writer_db
                .conn()
                .execute("DELETE FROM instances WHERE name = 'codex-worker'", [])
                .unwrap();
            writer_db
                .log_event(
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
        });

        let status = run_correlated_wait(&db, &filters, &correlation, cursor, 5);
        writer.join().unwrap();
        assert_eq!(
            status,
            crate::core::result_wait::RESULT_UNAVAILABLE_EXIT,
            "a mid-wait cancellation must terminate the attempt, not the deadline"
        );
    }

    #[test]
    fn result_wait_skips_ack_and_completes_on_delayed_final_result() {
        // Tracker hcom-dbj: the worker's acknowledgement on the exact
        // worker/thread/generation tuple carries intent=ack and must never
        // satisfy the result wait; only the later intent=inform report may.
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("ack-then-result.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('claude-worker', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let (correlation, filters) = arm_result_wait(&db, "claude-worker", "ack-workflow", cursor);
        let generation = filters.get("sender_instance_key").unwrap()[0].clone();

        // Deterministic phase: an ack alone never completes the wait, and the
        // delayed final result completes it without replaying the ack.
        db.log_event(
            "message",
            "claude-worker",
            &json!({
                "from": "claude-worker",
                "scope": "mentions",
                "mentions": ["caller"],
                "intent": "ack",
                "thread": "ack-workflow",
                "sender_instance_key": generation,
                "text": "ack: starting work",
            }),
        )
        .unwrap();
        assert_eq!(
            run_correlated_wait(&db, &filters, &correlation, cursor, 1),
            1,
            "an acknowledgement must never complete a result wait"
        );
        log_correlated_result(
            &db,
            "claude-worker",
            "ack-workflow",
            &generation,
            "final result",
        );
        assert_eq!(
            run_correlated_wait(&db, &filters, &correlation, cursor, 1),
            0,
            "the delayed final result completes the wait and skips the ack"
        );

        // Live phase: a wait already registered when the ack lands must stay
        // pending until the later inform result. The writer hands back control
        // after the ack so the assertion window is ordered by events, and the
        // inform is only delivered after the window proves no early exit.
        let fresh_cursor = db.get_last_event_id();
        let live_args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "6",
            "--after-id",
            &fresh_cursor.to_string(),
            "--thread",
            "ack-workflow",
            "--result-from",
            "claude-worker",
        ])
        .unwrap();
        let mut live_filters = live_args.filters.to_filter_map();
        let live_correlation = apply_result_correlation(&db, &live_args, &mut live_filters)
            .unwrap()
            .unwrap();

        let (ack_landed, ack_receiver) = std::sync::mpsc::channel::<()>();
        let writer_db_path = db_path.clone();
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            let mut writer_db = HcomDb::open_raw(&writer_db_path).unwrap();
            writer_db.ensure_schema().unwrap();
            writer_db
                .log_event(
                    "message",
                    "claude-worker",
                    &json!({
                        "from": "claude-worker",
                        "scope": "mentions",
                        "mentions": ["caller"],
                        "intent": "ack",
                        "thread": "ack-workflow",
                        "sender_instance_key": generation,
                        "text": "ack: mid-flight",
                    }),
                )
                .unwrap();
            ack_landed.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(1200));
            log_correlated_result(
                &writer_db,
                "claude-worker",
                "ack-workflow",
                &generation,
                "delayed final result",
            );
        });

        let wait_thread = std::thread::spawn(move || {
            let wait_db = HcomDb::open_raw(&db_path).unwrap();
            run_correlated_wait(&wait_db, &live_filters, &live_correlation, fresh_cursor, 6)
        });

        ack_receiver.recv().unwrap();
        let window = std::time::Instant::now() + Duration::from_millis(700);
        while std::time::Instant::now() < window {
            assert!(
                !wait_thread.is_finished(),
                "the live wait must stay pending while only the ack exists"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
        assert_eq!(
            wait_thread.join().unwrap(),
            0,
            "the live wait must complete once the final result lands"
        );
        writer.join().unwrap();
    }

    #[test]
    fn result_delivered_after_waiter_registration_completes_the_wait() {
        // Tracker hcom-dbj: results arriving immediately after waiter startup
        // (the pre-startup window is covered by the cursor-anchored tests
        // above) must be discovered by the live poll loop.
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("post-registration-result.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('claude-worker', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let (correlation, filters) = arm_result_wait(&db, "claude-worker", "late-workflow", cursor);
        let generation = filters.get("sender_instance_key").unwrap()[0].clone();

        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            let writer_db = HcomDb::open_raw(&db_path).unwrap();
            log_correlated_result(
                &writer_db,
                "claude-worker",
                "late-workflow",
                &generation,
                "result after registration",
            );
        });

        let status = run_correlated_wait(&db, &filters, &correlation, cursor, 4);
        writer.join().unwrap();
        assert_eq!(
            status, 0,
            "a correlated result delivered after registration must complete the wait"
        );
    }

    /// Shared fixture: arm a correlated wait exactly as the CLI would.
    fn arm_result_wait(
        db: &HcomDb,
        worker: &str,
        thread: &str,
        cursor: i64,
    ) -> (ResultCorrelation, HashMap<String, Vec<String>>) {
        use clap::Parser;

        let args = EventsArgs::try_parse_from([
            "events",
            "--wait",
            "1",
            "--after-id",
            &cursor.to_string(),
            "--thread",
            thread,
            "--result-from",
            worker,
        ])
        .unwrap();
        let mut filters = args.filters.to_filter_map();
        let correlation = apply_result_correlation(db, &args, &mut filters)
            .unwrap()
            .unwrap();
        (correlation, filters)
    }

    fn outcome_wait_from(
        correlation: &ResultCorrelation,
    ) -> crate::core::result_wait::OutcomeWait<'_> {
        crate::core::result_wait::OutcomeWait {
            worker: &correlation.exact_worker,
            generation: &correlation.instance_key,
            thread: &correlation.thread,
            attempt_after_id: correlation.after_id,
        }
    }

    /// Run the CLI's correlated wait engine from `after_id` with fixture
    /// filters whose tag-less names are already resolved.
    fn run_correlated_wait(
        db: &HcomDb,
        filters: &HashMap<String, Vec<String>>,
        correlation: &ResultCorrelation,
        after_id: i64,
        timeout: u64,
    ) -> i32 {
        let filter_sql = build_sql_from_flags(filters).unwrap();
        events_wait(
            db,
            &format!(" AND ({filter_sql})"),
            timeout,
            EventsWaitOptions {
                after_id: Some(after_id),
                full_output: true,
                filters,
                instance_name: None,
                result_correlation: Some(correlation),
            },
        )
    }

    /// Log a correlated terminal report from `worker` on `thread`.
    fn log_correlated_result(
        db: &HcomDb,
        worker: &str,
        thread: &str,
        generation: &str,
        text: &str,
    ) -> i64 {
        db.log_event(
            "message",
            worker,
            &json!({
                "from": worker,
                "scope": "mentions",
                "mentions": ["caller"],
                "intent": "inform",
                "thread": thread,
                "sender_instance_key": generation,
                "text": text,
            }),
        )
        .unwrap()
    }

    #[test]
    fn result_wait_rearm_after_completion_replays_only_from_original_cursor() {
        // Tracker hcom-dbj: a coordinator that re-arms the same correlated wait
        // after one completion must deterministically re-consume the exact
        // attempt result from its pre-launch cursor, and a fresh cursor must
        // not re-complete on the already-consumed result.
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("rearm-result.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('claude-worker', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let (correlation, filters) =
            arm_result_wait(&db, "claude-worker", "rearm-workflow", cursor);
        let generation = filters.get("sender_instance_key").unwrap()[0].clone();
        let result_id = log_correlated_result(
            &db,
            "claude-worker",
            "rearm-workflow",
            &generation,
            "attempt one result",
        );

        assert_eq!(
            run_correlated_wait(&db, &filters, &correlation, cursor, 1),
            0,
            "the first wait must complete on the correlated result"
        );
        assert_eq!(
            run_correlated_wait(&db, &filters, &correlation, cursor, 1),
            0,
            "the attempt cursor is durable: a re-armed wait replays the same result"
        );
        assert_eq!(
            run_correlated_wait(&db, &filters, &correlation, result_id, 1),
            1,
            "a fresh post-completion cursor must not re-complete on the consumed result"
        );
    }

    #[test]
    fn simultaneous_result_waits_complete_only_on_their_own_worker() {
        // Tracker hcom-dbj: two live correlated waits for different workers
        // must not interfere — each terminates only on its own worker's result.
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("parallel-result.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at) VALUES
                    ('worker-alpha', 'listening', 1000.0),
                    ('worker-beta', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let (alpha_correlation, alpha_filters) =
            arm_result_wait(&db, "worker-alpha", "workflow-alpha", cursor);
        let (beta_correlation, beta_filters) =
            arm_result_wait(&db, "worker-beta", "workflow-beta", cursor);
        let alpha_generation = alpha_filters.get("sender_instance_key").unwrap()[0].clone();
        let beta_generation = beta_filters.get("sender_instance_key").unwrap()[0].clone();

        let spawn_wait = |correlation: ResultCorrelation, filters: HashMap<String, Vec<String>>| {
            let db_path = db_path.clone();
            std::thread::spawn(move || {
                let db = HcomDb::open_raw(&db_path).unwrap();
                run_correlated_wait(&db, &filters, &correlation, cursor, 6)
            })
        };
        let alpha_wait = spawn_wait(alpha_correlation.clone(), alpha_filters.clone());
        let beta_wait = spawn_wait(beta_correlation.clone(), beta_filters.clone());

        log_correlated_result(
            &db,
            "worker-alpha",
            "workflow-alpha",
            &alpha_generation,
            "alpha result",
        );
        assert_eq!(
            alpha_wait.join().unwrap(),
            0,
            "worker-alpha's wait must complete on its own result"
        );

        // A wrongly-wide beta filter would complete on alpha's message within
        // one 500ms poll tick of its delivery; 700ms here is bounded margin,
        // not timing dependence.
        let window = std::time::Instant::now() + Duration::from_millis(700);
        while std::time::Instant::now() < window {
            assert!(
                !beta_wait.is_finished(),
                "worker-beta's wait must not complete on worker-alpha's result"
            );
            std::thread::sleep(Duration::from_millis(25));
        }

        log_correlated_result(
            &db,
            "worker-beta",
            "workflow-beta",
            &beta_generation,
            "beta result",
        );
        assert_eq!(
            beta_wait.join().unwrap(),
            0,
            "worker-beta's wait must complete on its own delayed result"
        );
    }

    #[test]
    fn result_wait_returns_pre_registration_approval_blocker() {
        // Reproduction of hcom-f6g.11: the worker reported ready, entered
        // pty:approval before the coordinator armed its wait, and the wait
        // must still terminate immediately instead of running to deadline.
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("pre-blocked.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, status_detail, created_at)
                 VALUES ('agy-worker', 'blocked', 'pty:approval',
                         'Bash: cargo test -- --nocapture', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "status",
            "agy-worker",
            &json!({
                "status": "blocked",
                "context": "pty:approval",
                "detail": "Bash: cargo test -- --nocapture",
            }),
        )
        .unwrap();
        let (correlation, filters) = arm_result_wait(&db, "agy-worker", "blocked-workflow", cursor);
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
            crate::core::result_wait::RESULT_BLOCKED_EXIT,
            "a blocker that fired between readiness and wait registration must terminate the wait"
        );
    }

    #[test]
    fn result_wait_returns_sandbox_bypass_approval_blocker_and_preserves_command() {
        // Reproduction of hcom-f6g.12: Antigravity sandbox-bypass prompt
        // publishes blocked/pty:approval with command detail preserved in status_detail,
        // and the correlated --result-from wait terminates promptly with RESULT_BLOCKED_EXIT (4).
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("sandbox-bypass-blocked.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, status_detail, created_at)
                 VALUES ('agy-worker', 'blocked', 'pty:approval',
                         'bd show hcom-f6g.12', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "status",
            "agy-worker",
            &json!({
                "status": "blocked",
                "context": "pty:approval",
                "detail": "bd show hcom-f6g.12",
            }),
        )
        .unwrap();
        let (correlation, filters) =
            arm_result_wait(&db, "agy-worker", "sandbox-bypass-workflow", cursor);
        let outcome =
            crate::core::result_wait::scan_terminal_outcome(&db, &outcome_wait_from(&correlation))
                .unwrap()
                .expect("sandbox-bypass approval must produce a terminal outcome");
        assert_eq!(
            outcome.exit_code,
            crate::core::result_wait::RESULT_BLOCKED_EXIT
        );
        assert_eq!(outcome.payload["evidence"], "bd show hcom-f6g.12");
        assert_eq!(outcome.payload["detail"], "bd show hcom-f6g.12");
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
            crate::core::result_wait::RESULT_BLOCKED_EXIT,
            "sandbox-bypass approval blocker must terminate the correlated wait promptly"
        );
    }

    #[test]
    fn result_wait_blocker_payload_preserves_correlation_and_evidence() {
        use crate::core::result_wait::scan_terminal_outcome;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("blocked-payload.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, status_detail, created_at)
                 VALUES ('agy-worker', 'blocked', 'pty:approval',
                         'Bash: cargo test -- --nocapture', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "status",
            "agy-worker",
            &json!({
                "status": "blocked",
                "context": "pty:approval",
                "detail": "Bash: cargo test -- --nocapture",
            }),
        )
        .unwrap();
        let (correlation, _filters) =
            arm_result_wait(&db, "agy-worker", "blocked-workflow", cursor);

        let outcome = scan_terminal_outcome(&db, &outcome_wait_from(&correlation))
            .unwrap()
            .expect("a current approval blocker must produce a terminal outcome");
        assert_eq!(
            outcome.exit_code,
            crate::core::result_wait::RESULT_BLOCKED_EXIT
        );
        let payload = outcome.payload;
        assert_eq!(payload["result_blocked"], true);
        assert_eq!(payload["outcome"], "blocked");
        assert_eq!(payload["worker"], "agy-worker");
        assert_eq!(payload["generation"], "agy-worker@1000.000000");
        assert_eq!(payload["thread"], "blocked-workflow");
        assert_eq!(payload["attempt_after_id"], cursor);
        assert_eq!(payload["kind"], "approval");
        assert_eq!(payload["context"], "pty:approval");
        assert_eq!(payload["evidence"], "Bash: cargo test -- --nocapture");
        let recovery = payload["recovery"].as_str().unwrap();
        assert!(recovery.contains("hcom term agy-worker"));
        assert!(recovery.contains(&format!("--after-id {cursor}")));
    }

    #[test]
    fn result_wait_returns_mid_wait_blocker_transition() {
        // Race coverage: the approval edge fires while the wait is already
        // registered and polling.
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("mid-blocked.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('agy-worker', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let (correlation, filters) =
            arm_result_wait(&db, "agy-worker", "mid-blocked-workflow", cursor);

        let writer_path = db_path.clone();
        let writer = std::thread::spawn(move || {
            let mut writer_db = HcomDb::open_raw(&writer_path).unwrap();
            writer_db.ensure_schema().unwrap();
            std::thread::sleep(Duration::from_millis(300));
            writer_db
                .conn()
                .execute(
                    "UPDATE instances SET status = 'blocked', status_context = 'pty:approval',
                         status_detail = 'Bash: cargo build'
                     WHERE name = 'agy-worker'",
                    [],
                )
                .unwrap();
            writer_db
                .log_event(
                    "status",
                    "agy-worker",
                    &json!({
                        "status": "blocked",
                        "context": "pty:approval",
                        "detail": "Bash: cargo build",
                    }),
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
        assert_eq!(
            status,
            crate::core::result_wait::RESULT_BLOCKED_EXIT,
            "a blocker transition during the wait must terminate it before the deadline"
        );
    }

    #[test]
    fn resolved_blocker_does_not_terminate_result_wait() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("resolved-blocked.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('agy-worker', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "status",
            "agy-worker",
            &json!({
                "status": "blocked",
                "context": "pty:approval",
                "detail": "Bash: cargo test",
            }),
        )
        .unwrap();
        db.log_event(
            "status",
            "agy-worker",
            &json!({
                "status": "listening",
                "context": "pty:approval_cleared",
            }),
        )
        .unwrap();
        let (correlation, filters) =
            arm_result_wait(&db, "agy-worker", "resolved-workflow", cursor);
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
            1,
            "a blocker that already cleared must not end the attempt"
        );
    }

    #[test]
    fn other_worker_blocker_does_not_terminate_result_wait() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("other-blocked.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at) VALUES
                    ('agy-worker', 'listening', 1000.0),
                    ('other-worker', 'blocked', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "status",
            "other-worker",
            &json!({
                "status": "blocked",
                "context": "pty:approval",
                "detail": "Bash: rm -rf /",
            }),
        )
        .unwrap();
        let (correlation, filters) =
            arm_result_wait(&db, "agy-worker", "isolation-workflow", cursor);
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
            1,
            "another worker's blocker must not terminate this wait"
        );
    }

    #[test]
    fn result_wait_returns_launch_failure_outcome() {
        use crate::core::result_wait::scan_terminal_outcome;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("launch-failed.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('agy-worker', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "life",
            "agy-worker",
            &json!({
                "action": "launch_failed",
                "batch_id": "batch-199",
                "reason": "ready_never_observed",
                "detail": "terminal exited before the worker bound its session",
            }),
        )
        .unwrap();
        let (correlation, filters) = arm_result_wait(&db, "agy-worker", "failed-workflow", cursor);

        let outcome = scan_terminal_outcome(&db, &outcome_wait_from(&correlation))
            .unwrap()
            .expect("a launch failure must produce a terminal outcome");
        assert_eq!(
            outcome.exit_code,
            crate::core::result_wait::RESULT_LAUNCH_FAILED_EXIT
        );
        let payload = outcome.payload;
        assert_eq!(payload["result_launch_failed"], true);
        assert_eq!(payload["outcome"], "launch_failed");
        assert_eq!(payload["generation"], "agy-worker@1000.000000");
        assert_eq!(payload["attempt_after_id"], cursor);
        assert_eq!(payload["batch_id"], "batch-199");
        assert_eq!(
            payload["detail"],
            "terminal exited before the worker bound its session"
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
            crate::core::result_wait::RESULT_LAUNCH_FAILED_EXIT
        );
    }

    #[test]
    fn result_wait_returns_typed_launch_blocker() {
        use crate::core::result_wait::scan_terminal_outcome;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("launch-blocked.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, created_at)
                 VALUES ('agy-worker', 'blocked', 'launch_blocked', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "life",
            "agy-worker",
            &json!({
                "action": "launch_blocked",
                "batch_id": "batch-77",
                "reason": "screen_settled_not_ready",
                "detail": "launch blocked: workspace approval required",
                "blocked_kind": "workspace_trust",
                "evidence": "Do you trust the files in this folder?",
            }),
        )
        .unwrap();
        let (correlation, filters) =
            arm_result_wait(&db, "agy-worker", "blocked-launch-workflow", cursor);

        let outcome = scan_terminal_outcome(&db, &outcome_wait_from(&correlation))
            .unwrap()
            .expect("an unresolved launch blocker must produce a terminal outcome");
        assert_eq!(
            outcome.exit_code,
            crate::core::result_wait::RESULT_BLOCKED_EXIT
        );
        let payload = outcome.payload;
        assert_eq!(payload["kind"], "workspace_trust");
        assert_eq!(
            payload["evidence"],
            "Do you trust the files in this folder?"
        );
        assert_eq!(
            payload["detail"],
            "launch blocked: workspace approval required"
        );
        assert_eq!(payload["reason"], "screen_settled_not_ready");
        assert_eq!(payload["thread"], "blocked-launch-workflow");
        assert_eq!(payload["attempt_after_id"], cursor);

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
            crate::core::result_wait::RESULT_BLOCKED_EXIT
        );
    }

    #[test]
    fn resolved_launch_blocker_does_not_terminate_result_wait() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("launch-blocked-resolved.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('agy-worker', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "life",
            "agy-worker",
            &json!({
                "action": "launch_blocked",
                "batch_id": "batch-78",
                "detail": "launch blocked: workspace approval required",
                "blocked_kind": "workspace_trust",
            }),
        )
        .unwrap();
        let (correlation, filters) = arm_result_wait(&db, "agy-worker", "resolved-launch", cursor);
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
            1,
            "a launch blocker the worker progressed past must not end the attempt"
        );
    }

    #[test]
    fn stopped_generation_keeps_cleared_launch_blocker_resolved() {
        use crate::core::result_wait::scan_terminal_outcome;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("cleared-then-stopped.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, created_at)
                 VALUES ('agy-worker', 'antigravity', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let (correlation, _filters) =
            arm_result_wait(&db, "agy-worker", "cleared-then-stopped", cursor);
        db.log_event(
            "life",
            "agy-worker",
            &json!({
                "action": "launch_blocked",
                "batch_id": "batch-cleared",
                "blocked_kind": "workspace_trust",
            }),
        )
        .unwrap();
        db.log_event(
            "life",
            "agy-worker",
            &json!({"action": "ready", "batch_id": "batch-cleared"}),
        )
        .unwrap();
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'agy-worker'", [])
            .unwrap();
        db.log_event(
            "life",
            "agy-worker",
            &json!({
                "action": "stopped",
                "snapshot": {
                    "name": "agy-worker",
                    "tool": "antigravity",
                    "created_at": 1000.0,
                    "session_id": "session-a",
                }
            }),
        )
        .unwrap();

        assert!(
            scan_terminal_outcome(&db, &outcome_wait_from(&correlation))
                .unwrap()
                .is_none(),
            "a cleared launch blocker must not reappear after the worker stops"
        );
    }

    #[test]
    fn newer_generation_launch_failure_does_not_terminate_original_wait() {
        use crate::core::result_wait::scan_terminal_outcome;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("reused-launch-failure.db")).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, created_at)
                 VALUES ('agy-worker', 'antigravity', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let (correlation, _filters) =
            arm_result_wait(&db, "agy-worker", "original-generation", cursor);
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'agy-worker'", [])
            .unwrap();
        db.log_event(
            "life",
            "agy-worker",
            &json!({
                "action": "stopped",
                "snapshot": {
                    "name": "agy-worker",
                    "tool": "antigravity",
                    "created_at": 1000.0,
                    "session_id": "session-a",
                }
            }),
        )
        .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, created_at)
                 VALUES ('agy-worker', 'antigravity', 'inactive', 2000.0)",
                [],
            )
            .unwrap();
        db.log_event(
            "life",
            "agy-worker",
            &json!({
                "action": "launch_failed",
                "batch_id": "new-generation",
                "reason": "exited_before_bind",
            }),
        )
        .unwrap();

        assert!(
            scan_terminal_outcome(&db, &outcome_wait_from(&correlation))
                .unwrap()
                .is_none(),
            "a reused name's launch failure must not satisfy the original generation's wait"
        );
    }

    #[test]
    fn blocker_in_final_poll_interval_wins_over_deadline() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("final-interval-blocker.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at)
                 VALUES ('agy-worker', 'listening', 1000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        let (correlation, filters) = arm_result_wait(&db, "agy-worker", "final-interval", cursor);

        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(700));
            let mut writer_db = HcomDb::open_raw(&db_path).unwrap();
            writer_db.ensure_schema().unwrap();
            writer_db
                .conn()
                .execute(
                    "UPDATE instances SET status = 'blocked', status_context = 'pty:approval',
                         status_detail = 'Bash: cargo test'
                     WHERE name = 'agy-worker'",
                    [],
                )
                .unwrap();
            writer_db
                .log_event(
                    "status",
                    "agy-worker",
                    &json!({
                        "status": "blocked",
                        "context": "pty:approval",
                        "detail": "Bash: cargo test",
                    }),
                )
                .unwrap();
        });

        let filter_sql = build_sql_from_flags(&filters).unwrap();
        let status = events_wait(
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
        );
        writer.join().unwrap();
        assert_eq!(
            status,
            crate::core::result_wait::RESULT_BLOCKED_EXIT,
            "a blocker observed during the final interval must win over the deadline"
        );
    }

    #[test]
    fn scan_ignores_blocked_status_from_a_reused_worker_generation() {
        use crate::core::result_wait::scan_terminal_outcome;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("reused-name.db")).unwrap();
        db.ensure_schema().unwrap();
        // The name was reused by a newer generation (created_at 2000) while
        // the wait is correlated against the original generation.
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, status_detail, created_at)
                 VALUES ('agy-worker', 'blocked', 'pty:approval', 'Bash: ls /etc', 2000.0)",
                [],
            )
            .unwrap();
        let cursor = db.get_last_event_id();
        db.log_event(
            "status",
            "agy-worker",
            &json!({
                "status": "blocked",
                "context": "pty:approval",
                "detail": "Bash: ls /etc",
            }),
        )
        .unwrap();
        let wait = crate::core::result_wait::OutcomeWait {
            worker: "agy-worker",
            generation: "agy-worker@1000.000000",
            thread: "reused-workflow",
            attempt_after_id: cursor,
        };
        assert!(
            scan_terminal_outcome(&db, &wait).unwrap().is_none(),
            "a reused name's blocker must not satisfy the original generation's wait"
        );
    }

    #[test]
    fn scan_requires_post_cursor_blocked_status_event() {
        use crate::core::result_wait::scan_terminal_outcome;

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("pre-cursor-blocked.db")).unwrap();
        db.ensure_schema().unwrap();
        // Blocked status event predates the attempt cursor: only the row is
        // currently blocked, with no post-cursor evidence event.
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, status_detail, created_at)
                 VALUES ('agy-worker', 'blocked', 'pty:approval', 'Bash: ls /etc', 1000.0)",
                [],
            )
            .unwrap();
        db.log_event(
            "status",
            "agy-worker",
            &json!({
                "status": "blocked",
                "context": "pty:approval",
                "detail": "Bash: ls /etc",
            }),
        )
        .unwrap();
        let cursor = db.get_last_event_id();
        let wait = crate::core::result_wait::OutcomeWait {
            worker: "agy-worker",
            generation: "agy-worker@1000.000000",
            thread: "anchor-workflow",
            attempt_after_id: cursor,
        };
        assert!(
            scan_terminal_outcome(&db, &wait).unwrap().is_none(),
            "the blocker scan is anchored at the attempt cursor, not the live row alone"
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
    fn default_cursor_wait_consumes_only_events_arriving_after_start() {
        // Compatibility pin before listener extraction: a wait armed without
        // --after-id captures the current durable cursor as its boundary. A
        // non-matching event landing mid-wait must not satisfy it, while a
        // matching event landing mid-wait must.
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("default-cursor.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        let filters = HashMap::from([("type".to_string(), vec!["status".to_string()])]);
        let filter_sql = build_sql_from_flags(&filters).unwrap();
        let filter_query = format!(" AND ({filter_sql})");

        let chatter_path = db_path.clone();
        let chatter_started = Instant::now();
        let chatter = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            let mut writer_db = HcomDb::open_raw(&chatter_path).unwrap();
            writer_db.ensure_schema().unwrap();
            writer_db
                .log_event(
                    "message",
                    "nova",
                    &json!({"from": "nova", "scope": "broadcast", "text": "chatter"}),
                )
                .unwrap();
            chatter_started.elapsed()
        });
        let non_match_result = events_wait(
            &db,
            &filter_query,
            1,
            EventsWaitOptions {
                after_id: None,
                full_output: false,
                filters: &filters,
                instance_name: None,
                result_correlation: None,
            },
        );
        let chatter_elapsed = chatter.join().unwrap();
        assert!(
            chatter_elapsed < Duration::from_secs(1),
            "the non-matching event must actually arrive before the wait deadline"
        );
        assert_eq!(
            non_match_result, 1,
            "a mid-wait non-matching event must not satisfy the default-cursor wait"
        );

        let status_path = db_path.clone();
        let status = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            let mut writer_db = HcomDb::open_raw(&status_path).unwrap();
            writer_db.ensure_schema().unwrap();
            writer_db
                .log_event(
                    "status",
                    "nova",
                    &json!({"status": "active", "context": "test"}),
                )
                .unwrap();
        });
        assert_eq!(
            events_wait(
                &db,
                &filter_query,
                3,
                EventsWaitOptions {
                    after_id: None,
                    full_output: false,
                    filters: &filters,
                    instance_name: None,
                    result_correlation: None,
                },
            ),
            0,
            "a mid-wait matching event must complete the default-cursor wait"
        );
        status.join().unwrap();
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

    #[test]
    fn test_listener_cursor_ordering_and_batching() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listener-cursor.db")).unwrap();
        db.ensure_schema().unwrap();

        // Seed 3 initial events
        let id1 = db
            .log_event("status", "nova", &json!({"status": "init"}))
            .unwrap();
        let id2 = db
            .log_event("status", "nova", &json!({"status": "working"}))
            .unwrap();
        let id3 = db
            .log_event(
                "message",
                "nova",
                &json!({"from": "nova", "scope": "broadcast", "text": "hello"}),
            )
            .unwrap();
        assert_eq!(db.get_last_event_id(), id3);

        // 1. Armed with default cursor (after_id: None) starts at db.get_last_event_id()
        let mut default_listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: None,
                endpoint_kind: EVENTS_WAIT_ENDPOINT_KIND,
            },
        );
        assert_eq!(default_listener.cursor(), id3);
        assert!(!default_listener.has_explicit_cursor());

        // 2. Armed with explicit cursor (after_id: Some(id1)) starts at id1
        let mut explicit_listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: Some(id1),
                instance_name: None,
                endpoint_kind: EVENTS_WAIT_ENDPOINT_KIND,
            },
        );
        assert_eq!(explicit_listener.cursor(), id1);
        assert!(explicit_listener.has_explicit_cursor());

        // Querying explicit_listener step-by-step with filter
        let filter = " AND (type = 'status')";
        let next = explicit_listener.query_next_event(filter).unwrap();
        assert!(next.is_some());
        let ev = next.unwrap();
        assert_eq!(ev["id"].as_i64(), Some(id2));
        // id3 is a message, so filtering by status yields None in SQL; cursor remains at id2
        let next_none = explicit_listener.query_next_event(filter).unwrap();
        assert!(next_none.is_none());
        assert_eq!(explicit_listener.cursor(), id2);

        // Querying without filter encounters id3 (message) and advances cursor to id3
        let next_msg = explicit_listener.query_next_event("").unwrap();
        assert!(next_msg.is_some());
        assert_eq!(next_msg.unwrap()["id"].as_i64(), Some(id3));
        assert_eq!(explicit_listener.cursor(), id3);

        // Seed new events
        let id4 = db
            .log_event("status", "nova", &json!({"status": "step 1"}))
            .unwrap();
        let id5 = db
            .log_event("status", "nova", &json!({"status": "step 2"}))
            .unwrap();
        let id6 = db
            .log_event(
                "message",
                "nova",
                &json!({"from": "nova", "scope": "broadcast", "text": "done"}),
            )
            .unwrap();

        // Default listener queries all events in batch
        let all_new = default_listener.query_events("").unwrap();
        assert_eq!(all_new.len(), 3);
        let ids: Vec<i64> = all_new.iter().filter_map(|e| e["id"].as_i64()).collect();
        assert_eq!(
            ids,
            vec![id4, id5, id6],
            "events must be in strictly ascending ID order"
        );
        assert_eq!(default_listener.cursor(), id6);

        // Subsequent query has no events and does not replay
        let empty = default_listener.query_events("").unwrap();
        assert!(
            empty.is_empty(),
            "querying again must not replay already-consumed events"
        );
        assert_eq!(default_listener.cursor(), id6);
    }

    #[test]
    fn test_listener_cleanup_on_drop_and_explicit() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listener-cleanup.db")).unwrap();
        db.ensure_schema().unwrap();

        // 1. Drop-based cleanup
        {
            let listener = EventListener::new(
                &db,
                EventListenerOptions {
                    after_id: None,
                    instance_name: Some("auto_drop_agent"),
                    endpoint_kind: EVENTS_WAIT_ENDPOINT_KIND,
                },
            );
            assert!(listener.is_listening());
            assert!(db.has_notify_endpoint_kind("auto_drop_agent", EVENTS_WAIT_ENDPOINT_KIND));
            let port = listener.endpoint_port().unwrap();
            assert!(port > 0);
        }
        // Dropped -> endpoint deleted
        assert!(
            !db.has_notify_endpoint_kind("auto_drop_agent", EVENTS_WAIT_ENDPOINT_KIND),
            "drop must remove registered notify endpoint"
        );

        // 2. Explicit cleanup and idempotency
        let mut explicit_listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: Some("explicit_cleanup_agent"),
                endpoint_kind: EVENTS_WAIT_ENDPOINT_KIND,
            },
        );
        assert!(db.has_notify_endpoint_kind("explicit_cleanup_agent", EVENTS_WAIT_ENDPOINT_KIND));
        explicit_listener.cleanup();
        assert!(
            !db.has_notify_endpoint_kind("explicit_cleanup_agent", EVENTS_WAIT_ENDPOINT_KIND),
            "explicit cleanup must remove registered notify endpoint"
        );
        assert_eq!(explicit_listener.endpoint_port(), None);
        // Idempotent: second cleanup call should not panic or fail
        explicit_listener.cleanup();
        drop(explicit_listener);

        // 3. Anonymous listener has no endpoint registered
        let mut anon_listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: None,
                endpoint_kind: EVENTS_WAIT_ENDPOINT_KIND,
            },
        );
        assert!(!anon_listener.is_listening());
        assert_eq!(anon_listener.endpoint_port(), None);
        anon_listener.cleanup();
    }

    #[test]
    fn test_listener_cleanup_preserves_same_kind_replacement() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listener-replacement.db")).unwrap();
        db.ensure_schema().unwrap();

        let old_listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: Some("shared_identity"),
                endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
            },
        );
        let replacement = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: Some("shared_identity"),
                endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
            },
        );
        let replacement_port = replacement.endpoint_port().unwrap();

        drop(old_listener);

        let stored_port: u16 = db
            .conn()
            .query_row(
                "SELECT port FROM notify_endpoints WHERE instance = 'shared_identity' AND kind = 'events_stream'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_port, replacement_port);

        drop(replacement);
        assert!(!db.has_notify_endpoint_kind("shared_identity", EVENTS_STREAM_ENDPOINT_KIND));
    }

    #[test]
    fn test_registered_stream_keeps_prompt_fallback_rechecks() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-recheck.db")).unwrap();
        db.ensure_schema().unwrap();

        let listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: Some("stream_observer"),
                endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
            },
        );
        assert!(listener.is_listening());
        assert_eq!(
            listener.bounded_recheck_duration(None),
            STREAM_RECHECK_INTERVAL
        );
    }

    #[test]
    fn test_listener_simultaneous_endpoint_kinds_coexist() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listener-coexist.db")).unwrap();
        db.ensure_schema().unwrap();

        let wait_listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: Some("shared_identity"),
                endpoint_kind: EVENTS_WAIT_ENDPOINT_KIND,
            },
        );
        let stream_listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: Some("shared_identity"),
                endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
            },
        );

        assert!(wait_listener.is_listening());
        assert!(stream_listener.is_listening());

        let wait_port = wait_listener.endpoint_port().unwrap();
        let stream_port = stream_listener.endpoint_port().unwrap();
        assert_ne!(
            wait_port, stream_port,
            "each listener must bind a distinct port"
        );

        assert_eq!(wait_listener.endpoint_kind(), "events_wait");
        assert_eq!(stream_listener.endpoint_kind(), "events_stream");

        // Both endpoints must coexist simultaneously for the same instance
        assert!(db.has_notify_endpoint_kind("shared_identity", EVENTS_WAIT_ENDPOINT_KIND));
        assert!(db.has_notify_endpoint_kind("shared_identity", EVENTS_STREAM_ENDPOINT_KIND));

        // Dropping the wait listener must remove ONLY its own endpoint kind
        drop(wait_listener);
        assert!(
            !db.has_notify_endpoint_kind("shared_identity", EVENTS_WAIT_ENDPOINT_KIND),
            "dropping wait listener must delete events_wait endpoint"
        );
        assert!(
            db.has_notify_endpoint_kind("shared_identity", EVENTS_STREAM_ENDPOINT_KIND),
            "events_stream endpoint must still be intact after wait listener dropped"
        );

        // Dropping the stream listener removes its endpoint
        drop(stream_listener);
        assert!(
            !db.has_notify_endpoint_kind("shared_identity", EVENTS_STREAM_ENDPOINT_KIND),
            "dropping stream listener must delete events_stream endpoint"
        );
    }

    #[test]
    fn test_listener_wait_tick_notification_and_timeout() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("listener-tick.db")).unwrap();
        db.ensure_schema().unwrap();

        let listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: Some("tick_agent"),
                endpoint_kind: EVENTS_WAIT_ENDPOINT_KIND,
            },
        );
        assert!(listener.is_listening());
        let port = listener.endpoint_port().unwrap();

        // 1. TCP notification triggers early wake
        let waker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            let addr = format!("127.0.0.1:{port}");
            let _ = std::net::TcpStream::connect_timeout(
                &addr.parse().unwrap(),
                Duration::from_millis(200),
            );
        });

        let start = Instant::now();
        let woken = listener.wait_tick(Duration::from_secs(3));
        assert!(
            woken,
            "wait_tick must return true when TCP wake is received"
        );
        assert!(
            start.elapsed() < Duration::from_millis(1500),
            "wait_tick must return early on notification rather than waiting full max_wait"
        );
        waker.join().unwrap();

        // 2. Timeout returns false when no notification arrives
        let start = Instant::now();
        let timed_out = listener.wait_tick(Duration::from_millis(60));
        assert!(
            !timed_out,
            "wait_tick must return false when no wake arrives"
        );
        assert!(start.elapsed() >= Duration::from_millis(60));
    }

    #[test]
    fn test_events_stream_args() {
        let args = EventsArgs::try_parse_from([
            "events",
            "stream",
            "--after-id",
            "42",
            "--timeout",
            "5",
            "--full",
            "--type",
            "message",
        ])
        .unwrap();
        match args.subcmd {
            Some(EventsSubcmd::Stream(ref stream)) => {
                assert_eq!(stream.after_id, Some(42));
                assert_eq!(stream.timeout, Some(5));
                assert!(stream.full);
                assert_eq!(stream.filters.event_type, vec!["message"]);
            }
            _ => panic!("Expected Stream subcommand"),
        }

        let defaults = EventsArgs::try_parse_from(["events", "stream"]).unwrap();
        match defaults.subcmd {
            Some(EventsSubcmd::Stream(ref stream)) => {
                assert_eq!(stream.after_id, None);
                assert_eq!(stream.timeout, None);
                assert!(!stream.full);
                assert!(!stream.filters.has_filters());
            }
            _ => panic!("Expected Stream subcommand"),
        }
    }

    #[test]
    fn stream_rejects_query_mode_flags_before_subcommand() {
        // Top-level --after-id still requires --wait, so it cannot arm a stream.
        assert!(EventsArgs::try_parse_from(["events", "--after-id", "42", "stream"]).is_err());

        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-conflict.db")).unwrap();
        db.ensure_schema().unwrap();

        for argv in [
            vec!["events", "--wait", "1", "stream"],
            vec!["events", "--wait", "1", "--after-id", "7", "stream"],
            vec!["events", "--full", "stream"],
            vec!["events", "--last", "5", "stream"],
            vec!["events", "--all", "stream"],
            vec!["events", "--sql", "1=1", "stream"],
            vec!["events", "--device", "ABCD", "stream"],
            vec!["events", "--type", "message", "stream"],
        ] {
            let args = EventsArgs::try_parse_from(argv.clone()).unwrap();
            assert_eq!(
                cmd_events(&db, &args, None),
                1,
                "query-mode flags before `stream` must be rejected: {argv:?}"
            );
        }
    }

    #[test]
    fn stream_listener_drains_ordered_matches_and_advances_past_excluded_tail() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-drain.db")).unwrap();
        db.ensure_schema().unwrap();

        let m1 = db
            .log_event("message", "nova", &json!({"from": "nova", "text": "one"}))
            .unwrap();
        let _noise1 = db
            .log_event("status", "nova", &json!({"status": "working"}))
            .unwrap();
        let m2 = db
            .log_event("message", "nova", &json!({"from": "nova", "text": "two"}))
            .unwrap();
        let noise2 = db
            .log_event("status", "nova", &json!({"status": "done"}))
            .unwrap();

        let mut listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: Some(m1 - 1),
                instance_name: None,
                endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
            },
        );

        let filter = " AND (type = 'message')";
        let batch = listener.drain_new_events(filter).unwrap();
        let ids: Vec<i64> = batch.iter().filter_map(|e| e["id"].as_i64()).collect();
        assert_eq!(
            ids,
            vec![m1, m2],
            "matches must be drained in ascending durable-ID order"
        );
        assert!(
            listener.cursor() >= noise2,
            "the scan boundary must advance past SQL-excluded rows so the excluded tail is not rescanned"
        );

        // Already-scanned rows are never replayed.
        assert!(
            listener.drain_new_events(filter).unwrap().is_empty(),
            "re-draining without new events must not replay consumed rows"
        );

        // The advanced boundary must not skip a later matching ID.
        let m3 = db
            .log_event("message", "nova", &json!({"from": "nova", "text": "three"}))
            .unwrap();
        let next = listener.drain_new_events(filter).unwrap();
        assert_eq!(
            next.first().and_then(|e| e["id"].as_i64()),
            Some(m3),
            "a matching event after the advanced boundary must still be drained"
        );

        // Resuming from the last emitted ID replays nothing before it.
        let mut resumed = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: Some(m2),
                instance_name: None,
                endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
            },
        );
        assert_eq!(
            resumed
                .drain_new_events(filter)
                .unwrap()
                .first()
                .and_then(|e| e["id"].as_i64()),
            Some(m3),
            "resume after the last emitted ID must not replay consumed events"
        );
    }

    #[test]
    fn stream_listener_default_cursor_consumes_only_post_arm_events() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-default-cursor.db")).unwrap();
        db.ensure_schema().unwrap();

        let before = db
            .log_event(
                "message",
                "nova",
                &json!({"from": "nova", "text": "before arm"}),
            )
            .unwrap();
        assert_eq!(db.get_last_event_id(), before);

        let mut listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: None,
                endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
            },
        );
        assert_eq!(
            listener.cursor(),
            before,
            "a default-cursor stream must arm at the current durable cursor"
        );
        assert!(
            listener.drain_new_events("").unwrap().is_empty(),
            "events durable before arming must not be emitted by default"
        );

        let after = db
            .log_event(
                "message",
                "nova",
                &json!({"from": "nova", "text": "after arm"}),
            )
            .unwrap();
        let batch = listener.drain_new_events("").unwrap();
        assert_eq!(
            batch.first().and_then(|e| e["id"].as_i64()),
            Some(after),
            "only events arriving after the arm cursor may be emitted"
        );
    }

    #[test]
    fn stream_stays_active_until_timeout_and_exits_zero() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("stream-timeout.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();
        let cursor = db.get_last_event_id();

        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            let writer_db = HcomDb::open_raw(&db_path).unwrap();
            writer_db
                .log_event("message", "nova", &json!({"from": "nova", "text": "live"}))
                .unwrap();
        });

        let args = EventsArgs::try_parse_from([
            "events",
            "stream",
            "--after-id",
            &cursor.to_string(),
            "--timeout",
            "1",
        ])
        .unwrap();
        let Some(EventsSubcmd::Stream(stream_args)) = args.subcmd else {
            panic!("Expected Stream subcommand");
        };

        let start = Instant::now();
        let code = cmd_events_stream(&db, &stream_args, None);
        let elapsed = start.elapsed();
        writer.join().unwrap();
        assert_eq!(code, 0, "a stream that reaches its timeout must exit 0");
        assert!(
            elapsed >= Duration::from_secs(1),
            "the stream must stay active for the whole timeout window, exited after {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the stream must end at its timeout rather than hang, exited after {elapsed:?}"
        );
    }

    #[test]
    fn stream_filter_errors_exit_one() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-filter-error.db")).unwrap();
        db.ensure_schema().unwrap();

        // Cross-type constraints keep failing closed through the stream:
        // message-type + status-type,
        let message_status = EventsArgs::try_parse_from([
            "events", "stream", "--from", "nova", "--status", "blocked",
        ])
        .unwrap();
        // message-type + life-type,
        let message_life = EventsArgs::try_parse_from([
            "events", "stream", "--intent", "inform", "--action", "ready",
        ])
        .unwrap();
        // and the typed --idle predicate against --agent.
        let idle_agent =
            EventsArgs::try_parse_from(["events", "stream", "--idle", "kuma", "--agent", "kuma"])
                .unwrap();
        for args in [message_status, message_life, idle_agent] {
            let Some(EventsSubcmd::Stream(stream_args)) = args.subcmd else {
                panic!("Expected Stream subcommand");
            };
            assert_eq!(
                cmd_events_stream(&db, &stream_args, None),
                1,
                "stream filter composition must retain type-validation failures"
            );
        }
    }

    /// Build the generic stream filter SQL exactly as `cmd_events_stream`
    /// does, so composition tests exercise the shared filter pipeline.
    fn stream_filter_query(args: &EventsStreamArgs, db: &HcomDb) -> Result<String, String> {
        let mut filters = args.filters.to_filter_map();
        resolve_filter_names(&mut filters, db);
        let flag_sql = build_sql_from_flags(&filters)?;
        if !filters.is_empty() && !flag_sql.is_empty() {
            Ok(format!(" AND ({flag_sql})"))
        } else {
            Ok(String::new())
        }
    }

    /// Run a generic stream to completion (a zero-second timeout drains every
    /// currently durable match and exits immediately) and return the parsed
    /// NDJSON records.
    fn run_generic_stream(
        db: &HcomDb,
        filter_query: &str,
        full_output: bool,
        filters: &HashMap<String, Vec<String>>,
    ) -> Vec<Value> {
        let mut output = Vec::new();
        let exit = events_stream_to(
            db,
            filter_query,
            Some(0),
            &mut output,
            EventsStreamOptions {
                after_id: Some(0),
                full_output,
                filters,
                instance_name: None,
                generation_follow: None,
                compact: None,
            },
        );
        assert_eq!(exit, 0);
        String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn generic_stream_repeated_filter_values_compose_as_or() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-or-filter.db")).unwrap();
        db.ensure_schema().unwrap();

        db.log_event("message", "nova", &json!({"from": "nova", "text": "one"}))
            .unwrap();
        db.log_event("status", "kuma", &json!({ "status": "active" }))
            .unwrap();
        db.log_event("life", "kuma", &json!({"action": "stopped"}))
            .unwrap();

        let args = EventsArgs::try_parse_from([
            "events", "stream", "--type", "status", "--type", "message",
        ])
        .unwrap();
        let Some(EventsSubcmd::Stream(stream_args)) = args.subcmd else {
            panic!("Expected Stream subcommand");
        };
        let lines = run_generic_stream(
            &db,
            &stream_filter_query(&stream_args, &db).unwrap(),
            false,
            &HashMap::new(),
        );

        let mut types: Vec<&str> = lines
            .iter()
            .map(|line| line["type"].as_str().unwrap())
            .collect();
        types.sort_unstable();
        assert_eq!(
            types,
            ["message", "status"],
            "repeated values of one filter keep OR semantics through the stream"
        );
    }

    #[test]
    fn generic_stream_distinct_filters_compose_as_and() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-and-filter.db")).unwrap();
        db.ensure_schema().unwrap();

        db.log_event("message", "nova", &json!({"from": "nova", "text": "match"}))
            .unwrap();
        db.log_event("status", "nova", &json!({ "status": "active" }))
            .unwrap();
        db.log_event("message", "kuma", &json!({"from": "kuma", "text": "no"}))
            .unwrap();

        let args = EventsArgs::try_parse_from([
            "events", "stream", "--agent", "nova", "--type", "message",
        ])
        .unwrap();
        let Some(EventsSubcmd::Stream(stream_args)) = args.subcmd else {
            panic!("Expected Stream subcommand");
        };
        let lines = run_generic_stream(
            &db,
            &stream_filter_query(&stream_args, &db).unwrap(),
            false,
            &HashMap::new(),
        );

        assert_eq!(lines.len(), 1, "distinct filters keep AND semantics");
        assert_eq!(lines[0]["type"], "message");
        assert_eq!(lines[0]["instance"], "nova");
    }

    #[test]
    fn generic_stream_expands_shortcut_filters() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-shortcut-filter.db")).unwrap();
        db.ensure_schema().unwrap();

        db.log_event(
            "status",
            "kuma",
            &json!({"status": "blocked", "context": "approval"}),
        )
        .unwrap();
        db.log_event("status", "kuma", &json!({ "status": "active" }))
            .unwrap();
        db.log_event(
            "status",
            "nova",
            &json!({"status": "blocked", "context": "approval"}),
        )
        .unwrap();

        // --blocked NAME expands to --agent NAME --status blocked.
        let args = EventsArgs::try_parse_from(["events", "stream", "--blocked", "kuma"]).unwrap();
        let Some(EventsSubcmd::Stream(stream_args)) = args.subcmd else {
            panic!("Expected Stream subcommand");
        };
        let lines = run_generic_stream(
            &db,
            &stream_filter_query(&stream_args, &db).unwrap(),
            false,
            &HashMap::new(),
        );

        assert_eq!(lines.len(), 1, "shortcut expansion must survive the stream");
        assert_eq!(lines[0]["instance"], "kuma");
        assert_eq!(lines[0]["data"]["status"], "blocked");
    }

    #[test]
    fn generic_stream_projections_match_existing_event_projections() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-projections.db")).unwrap();
        db.ensure_schema().unwrap();

        let long_detail =
            "cargo test --features 'secret-feature-token=hunter2' --release --all-targets --locked";
        db.log_event(
            "status",
            "nova",
            &json!({
                "status": "active",
                "context": "tool:Bash",
                "detail": long_detail,
                "position": 42,
            }),
        )
        .unwrap();
        db.log_event(
            "life",
            "nova",
            &json!({
                "action": "stopped",
                "snapshot": {"name": "nova", "created_at": 1000.0, "tool": "claude"},
            }),
        )
        .unwrap();
        db.log_event(
            "message",
            "nova",
            &json!({
                "from": "nova",
                "intent": "inform",
                "text": "body with password hunter2",
                "reply_to": "81",
                "sender_instance_key": "nova@1000.000000",
                "mentions": ["kuma"],
            }),
        )
        .unwrap();

        let filters = HashMap::new();

        // Full output is the raw event view: nothing the streamliner would
        // trim is trimmed.
        let full = run_generic_stream(&db, "", true, &filters);
        assert_eq!(full.len(), 3);
        assert_eq!(full[0]["data"]["detail"], json!(long_detail));
        assert_eq!(full[0]["data"]["position"], json!(42));
        assert!(full[1]["data"]["snapshot"].is_object());
        assert_eq!(full[2]["data"]["reply_to"], json!("81"));

        // Streamlined output is exactly the existing snapshot projection:
        // equality with streamline_event pins compatibility with query and
        // wait output rather than a stream-specific shape.
        let streamlined = run_generic_stream(&db, "", false, &filters);
        assert_eq!(streamlined.len(), 3);
        for (streamed, raw) in streamlined.iter().zip(&full) {
            assert_eq!(streamed, &streamline_event(raw, &filters));
        }
        // Spot-check the known trims happened through the stream path.
        let detail = streamlined[0]["data"]["detail"].as_str().unwrap();
        assert!(detail.ends_with("...") && detail.len() < long_detail.len());
        assert!(streamlined[0]["data"].get("position").is_none());
        assert!(streamlined[1]["data"].get("snapshot").is_none());
        assert!(streamlined[2]["data"].get("reply_to").is_none());
        assert!(streamlined[2]["data"].get("mentions").is_none());
        assert_eq!(
            streamlined[0]["ts"].as_str().unwrap().len(),
            full[0]["ts"].as_str().unwrap()[..19].len(),
            "timestamps truncate to the existing 19-char projection"
        );
    }

    #[test]
    fn stream_timeout_overflow_exits_one() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-timeout-overflow.db")).unwrap();
        db.ensure_schema().unwrap();

        let args =
            EventsArgs::try_parse_from(["events", "stream", "--timeout", &u64::MAX.to_string()])
                .unwrap();
        let Some(EventsSubcmd::Stream(stream_args)) = args.subcmd else {
            panic!("Expected Stream subcommand");
        };
        assert_eq!(cmd_events_stream(&db, &stream_args, None), 1);
    }

    struct FailingWriter {
        fail_on_flush: bool,
    }

    impl Write for FailingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if !self.fail_on_flush {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "simulated broken pipe on write",
                ))
            } else {
                Ok(buf.len())
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            if self.fail_on_flush {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "simulated broken pipe on flush",
                ))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn test_write_and_flush_record_detects_broken_pipe() {
        let mut on_write = FailingWriter {
            fail_on_flush: false,
        };
        let err = write_and_flush_record_to(&mut on_write, "{\"test\":true}").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);

        let mut on_flush = FailingWriter {
            fail_on_flush: true,
        };
        let err = write_and_flush_record_to(&mut on_flush, "{\"test\":true}").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn test_listener_wait_tick_interruptible_wakes_early() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("interruptible-wake.db")).unwrap();
        db.ensure_schema().unwrap();

        let listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: None,
                endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
            },
        );

        let interrupted = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&interrupted);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            flag_clone.store(true, Ordering::Relaxed);
        });

        let start = Instant::now();
        let woken = listener.wait_tick_interruptible(Duration::from_secs(5), &interrupted);
        let elapsed = start.elapsed();
        assert!(!woken, "interruption should return false for TCP wake");
        assert!(
            elapsed < Duration::from_secs(2),
            "interrupted wait_tick must return early, took {elapsed:?}"
        );
        assert!(interrupted.load(Ordering::Relaxed));
    }

    #[test]
    fn test_events_stream_cleanup_preserves_other_endpoints_and_instances() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut db = HcomDb::open_raw(&temp.path().join("stream-cleanup-isolation.db")).unwrap();
        db.ensure_schema().unwrap();

        // Seed other endpoints for the same listener and for another instance
        db.upsert_notify_endpoint("listener-inst", "pty", 7777)
            .unwrap();
        db.upsert_notify_endpoint("other-inst", "events_stream", 8888)
            .unwrap();

        // Seed an observed worker in instances table
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, status_detail, session_id, created_at)
                 VALUES ('observed-worker', 'active', 'workflow:step2', 'in progress', 'sess-123', 1000.0)",
                [],
            )
            .unwrap();

        // Arm a stream listener for listener-inst
        let listener = EventListener::new(
            &db,
            EventListenerOptions {
                after_id: None,
                instance_name: Some("listener-inst"),
                endpoint_kind: EVENTS_STREAM_ENDPOINT_KIND,
            },
        );
        assert!(listener.is_listening());

        // Verify the events_stream endpoint was added
        let stream_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM notify_endpoints WHERE instance = 'listener-inst' AND kind = 'events_stream'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stream_count, 1);

        // Explicitly drop/cleanup the listener
        drop(listener);

        // Assert: listener-inst's events_stream endpoint is deleted
        let stream_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM notify_endpoints WHERE instance = 'listener-inst' AND kind = 'events_stream'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stream_count, 0, "events_stream endpoint must be removed");

        // Assert: listener-inst's pty endpoint is preserved
        let pty_port: u16 = db
            .conn()
            .query_row(
                "SELECT port FROM notify_endpoints WHERE instance = 'listener-inst' AND kind = 'pty'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pty_port, 7777, "pty endpoint must remain untouched");

        // Assert: other-inst's events_stream endpoint is preserved
        let other_port: u16 = db
            .conn()
            .query_row(
                "SELECT port FROM notify_endpoints WHERE instance = 'other-inst' AND kind = 'events_stream'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            other_port, 8888,
            "other instances' endpoints must remain untouched"
        );

        // Assert: observed worker's status, context, detail, session_id in instances are completely unchanged
        let (status, context, detail, sess): (String, String, String, String) = db
            .conn()
            .query_row(
                "SELECT status, status_context, status_detail, session_id FROM instances WHERE name = 'observed-worker'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(status, "active");
        assert_eq!(context, "workflow:step2");
        assert_eq!(detail, "in progress");
        assert_eq!(sess, "sess-123");
    }
}
