//! Hermes Agent hook handlers — argv-based lifecycle plus Python plugin.
//!
//! The integration surface is a Python plugin installed into
//! `~/.hermes/plugins/hcom/` (embedded below via `include_str!`, same
//! pattern as the Pi/Omp TypeScript plugins). The plugin shells out to the
//! argv-based RPC subcommands dispatched here:
//!
//! - `hermes-sessionstart` — bind session→process, set listening, return
//!   `{name, session_id, bootstrap}`. Works for hcom-launched sessions
//!   (`HCOM_PROCESS_ID` + launch placeholder) AND for ordinarily started,
//!   plugin-enabled sessions: with no launch binding, the handler recovers
//!   an existing identity by Hermes session id or mints a vanilla one
//!   (analogous to unmanaged Codex adoption), idempotently.
//! - `hermes-status` — set active/listening with context/detail.
//! - `hermes-read` — every mode prints one JSON document on stdout:
//!   raw unread list (default, `[message, …]`), `--format`
//!   (`{"context", "up_to", "count"}` where `up_to` is the cursor of the
//!   DELIVERED batch — the batch is capped, so it can lag the newest
//!   unread event), `--check` (`{"has_unread": bool}`), and
//!   `--ack [--up-to ID]` (`{"acked_to": id}` / `{"acked": n}`; acks are
//!   monotonic — the cursor never moves backwards).
//! - `hermes-beforetool` — record tool activity into the event stream.
//! - `hermes-stop` — finalize (or `--soft`-finalize) the instance.

use std::time::{Duration, Instant};

use serde_json::Value;

use crate::bootstrap;
use crate::db::HcomDb;
use crate::instance_binding;
use crate::instance_lifecycle as lifecycle;
use crate::instances;
use crate::log::{log_error, log_info, log_warn};
use crate::shared::ST_LISTENING;
use crate::shared::context::HcomContext;

use super::common;

fn parse_flag(argv: &[String], flag: &str) -> Option<String> {
    argv.iter()
        .position(|a| a == flag)
        .and_then(|i| argv.get(i + 1))
        .cloned()
}

fn has_flag(argv: &[String], flag: &str) -> bool {
    argv.iter().any(|a| a == flag)
}

fn upsert_plugin_notify_endpoint(db: &HcomDb, instance_name: &str, port: u16) {
    if let Err(e) = db.upsert_notify_endpoint(instance_name, "plugin", port) {
        log_error(
            "native",
            "hermes.register_notify_fail",
            &format!("Failed to register plugin notify port for {instance_name}: {e}"),
        );
        return;
    }
    crate::notify::wake(db, instance_name, crate::notify::WakeKind::DELIVERY_LOOPS);
}

fn initialize_last_event_id(db: &HcomDb, instance_name: &str) {
    if let Ok(Some(existing)) = db.get_instance_full(instance_name)
        && existing.last_event_id == 0
    {
        let launch_event_id: Option<i64> = std::env::var("HCOM_LAUNCH_EVENT_ID")
            .ok()
            .and_then(|s| s.parse().ok());
        let current_max = db.get_last_event_id();
        // Restart safety: a hard stop deletes the instance row but logs a
        // life.stopped snapshot carrying the prior read cursor. When the
        // identity is recovered (vanilla adoption / restore_stopped), carry
        // that cursor forward instead of seeding to current max — messages
        // that arrived while the session was down must survive the rebind,
        // not be silently skipped (observed live: event consumed unseen).
        let prior_cursor: Option<i64> = db
            .conn()
            .query_row(
                "SELECT json_extract(data, '$.snapshot.last_event_id') FROM events \
                 WHERE type = 'life' \
                   AND json_extract(data, '$.action') = 'stopped' \
                   AND instance = ? \
                 ORDER BY id DESC LIMIT 1",
                rusqlite::params![instance_name],
                |row| row.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten();
        let new_id = match (launch_event_id, prior_cursor) {
            (Some(lei), _) if lei <= current_max => lei,
            (_, Some(prior)) if prior > 0 => prior.min(current_max),
            _ => current_max,
        };
        let mut updates = serde_json::Map::new();
        updates.insert("last_event_id".into(), serde_json::json!(new_id));
        instances::update_instance_position(db, instance_name, &updates);
    }
}

fn instance_name_from_env(ctx: &HcomContext) -> Option<String> {
    ctx.raw_env
        .get("HCOM_INSTANCE_NAME")
        .filter(|s| !s.is_empty())
        .cloned()
}

fn bootstrap_for(ctx: &HcomContext, db: &HcomDb, instance_name: &str) -> String {
    let tag = db
        .get_instance_full(instance_name)
        .ok()
        .flatten()
        .and_then(|d| d.tag.clone())
        .unwrap_or_default();
    let hcom_config = crate::config::HcomConfig::load(None).unwrap_or_default();
    let relay_enabled = crate::relay::is_relay_enabled(&hcom_config);
    let effective_tag = if tag.is_empty() {
        &hcom_config.tag
    } else {
        &tag
    };
    bootstrap::get_bootstrap(
        db,
        &ctx.hcom_dir,
        instance_name,
        "hermes",
        ctx.is_background,
        ctx.is_launched,
        &ctx.notes,
        effective_tag,
        relay_enabled,
        ctx.background_name.as_deref(),
    )
}

/// Mint (or, under a concurrent hook, recover) a vanilla Hermes identity for
/// an ordinary plugin-enabled session — no `HCOM_PROCESS_ID`, no launch
/// placeholder. Mirrors `adopt_unmanaged_codex_instance`: the check+create is
/// serialized by a lock file so concurrent plugin hooks (session start and the
/// first pre-LLM call run as separate `hcom` processes) cannot mint two rows
/// for one session, and the write is transactional with rollback + cleanup on
/// failure. Only `hermes-sessionstart` reaches this path; read/status/stop
/// hooks must never create identity as a side effect.
fn adopt_vanilla_hermes_instance(
    db: &HcomDb,
    session_id: &str,
    transcript_path: Option<&str>,
    cwd: Option<&str>,
) -> Option<String> {
    if session_id.is_empty() {
        return None;
    }
    let lock_dir = db.path().parent()?.join(".tmp");
    if let Err(error) = std::fs::create_dir_all(&lock_dir) {
        log_warn(
            "hooks",
            "hermes.adopt.lock_dir_failed",
            &format!("session_id={session_id} err={error}"),
        );
        return None;
    }
    let lock_file = std::fs::File::options()
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_dir.join("hermes_adopt.lock"));
    let lock_file = match lock_file {
        Ok(file) => file,
        Err(error) => {
            log_warn(
                "hooks",
                "hermes.adopt.lock_open_failed",
                &format!("session_id={session_id} err={error}"),
            );
            return None;
        }
    };
    if let Err(error) = crate::sys::fs::lock_exclusive(&lock_file) {
        log_warn(
            "hooks",
            "hermes.adopt.lock_failed",
            &format!("session_id={session_id} err={error}"),
        );
        return None;
    }

    // A concurrent/repeated hook may have completed the binding after the
    // caller's lookups. Recheck while holding the adoption lock — this also
    // covers recovery: a live session binding, or the soft-stopped row found
    // through its `life.stopped` snapshot.
    if let Some(name) = instance_binding::bind_session_to_process(db, session_id, None) {
        return Some(name);
    }

    let name = crate::instance_names::generate_unique_name(db).ok()?;
    if let Err(error) = db.conn().execute_batch("BEGIN IMMEDIATE") {
        log_error(
            "hooks",
            "hermes.adopt.transaction_failed",
            &format!("instance={name} session_id={session_id} err={error}"),
        );
        let _ = db.delete_instance(&name);
        return None;
    }

    let adoption = (|| -> anyhow::Result<()> {
        if !instance_binding::initialize_instance_in_position_file(
            db,
            &name,
            Some(session_id),
            None,
            None,
            None,
            transcript_path,
            Some("hermes"),
            false,
            None,
            None,
            None,
            None,
            cwd,
        ) || db.get_instance_full(&name)?.is_none()
        {
            anyhow::bail!("instance initialization did not produce a row");
        }
        db.rebind_instance_session(&name, session_id)?;
        if db.get_session_binding(session_id)?.as_deref() != Some(name.as_str()) {
            anyhow::bail!("session binding verification failed");
        }
        Ok(())
    })();

    let adoption = adoption.and_then(|()| {
        db.conn()
            .execute_batch("COMMIT")
            .map_err(anyhow::Error::from)
    });
    if let Err(error) = adoption {
        // The binding writes happened inside the transaction, so ROLLBACK
        // already undid them; only the instance row needs explicit cleanup.
        let _ = db.conn().execute_batch("ROLLBACK");
        let _ = db.delete_instance(&name);
        log_error(
            "hooks",
            "hermes.adopt.failed",
            &format!("instance={name} session_id={session_id} err={error}"),
        );
        return None;
    }
    instance_binding::capture_and_store_launch_context(db, &name);
    crate::relay::worker::ensure_worker(true);
    common::notify_hook_instance_with_db(db, &name);
    log_info(
        "hooks",
        "hermes.adopt.success",
        &format!("instance={name} session_id={session_id}"),
    );
    Some(name)
}

fn handle_start(ctx: &HcomContext, db: &HcomDb, argv: &[String]) -> (i32, String) {
    // Plugin RPC returns JSON errors on exit 0 so the plugin can handle
    // setup failures without Hermes treating the subprocess as failed.
    let session_id = match parse_flag(argv, "--session-id") {
        Some(sid) => sid,
        None => return (0, r#"{"error":"Missing --session-id"}"#.to_string()),
    };
    let transcript_path = parse_flag(argv, "--transcript-path");
    let cwd = parse_flag(argv, "--cwd");
    let notify_port: Option<u16> = parse_flag(argv, "--notify-port").and_then(|s| s.parse().ok());
    let cwd_value = cwd
        .as_deref()
        .filter(|p| !p.is_empty())
        .or_else(|| ctx.cwd.to_str());

    // hcom-launched sessions carry HCOM_PROCESS_ID plus a launch placeholder;
    // ordinarily started, plugin-enabled sessions have neither. Resolution
    // order: launch/session bindings (recovers by Hermes session id — live
    // binding, or the soft-stopped row via its life.stopped snapshot), then
    // the HCOM_INSTANCE_NAME omp recovery, then a freshly minted vanilla
    // identity. Every step is idempotent per session id.
    let instance_name =
        instance_binding::bind_session_to_process(db, &session_id, ctx.process_id.as_deref())
            .or_else(|| {
                instance_name_from_env(ctx).and_then(|name| {
                    instance_binding::recover_process_binding_for_instance(
                        db,
                        &name,
                        &session_id,
                        ctx.process_id.as_deref().unwrap_or(""),
                    )
                })
            })
            .or_else(|| {
                adopt_vanilla_hermes_instance(
                    db,
                    &session_id,
                    transcript_path.as_deref(),
                    cwd_value,
                )
            });
    let instance_name = match instance_name {
        Some(name) => name,
        None => {
            return (
                0,
                r#"{"error":"Failed to bind or create hcom identity"}"#.to_string(),
            );
        }
    };

    initialize_last_event_id(db, &instance_name);
    lifecycle::set_status(
        db,
        &instance_name,
        ST_LISTENING,
        "start",
        Default::default(),
    );
    instance_binding::capture_and_store_launch_context(db, &instance_name);

    let mut updates = serde_json::Map::new();
    updates.insert("tool".into(), serde_json::json!("hermes"));
    updates.insert("session_id".into(), serde_json::json!(&session_id));
    if let Some(path) = transcript_path.as_ref().filter(|p| !p.is_empty()) {
        updates.insert("transcript_path".into(), serde_json::json!(path));
    }
    if let Some(cwd) = cwd_value {
        updates.insert("directory".into(), serde_json::json!(cwd));
    }
    instances::update_instance_position(db, &instance_name, &updates);
    if let Some(port) = notify_port {
        upsert_plugin_notify_endpoint(db, &instance_name, port);
    }
    log_info(
        "hooks",
        "hermes-sessionstart.bind",
        &format!("instance={instance_name} session_id={session_id}"),
    );
    crate::relay::worker::ensure_worker(true);

    let response = serde_json::json!({
        "name": instance_name,
        "session_id": session_id,
        "bootstrap": bootstrap_for(ctx, db, &instance_name),
    });
    (0, response.to_string())
}

fn handle_status(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let name = match parse_flag(argv, "--name") {
        Some(n) => n,
        None => return (0, r#"{"error":"Missing --name or --status"}"#.to_string()),
    };
    let status = match parse_flag(argv, "--status") {
        Some(s) => s,
        None => return (0, r#"{"error":"Missing --name or --status"}"#.to_string()),
    };
    let context = parse_flag(argv, "--context").unwrap_or_default();
    let detail = parse_flag(argv, "--detail").unwrap_or_default();
    let was_listening = db
        .get_instance_full(&name)
        .ok()
        .flatten()
        .is_some_and(|inst| inst.status == ST_LISTENING);

    lifecycle::set_status(
        db,
        &name,
        &status,
        &context,
        lifecycle::StatusUpdate {
            detail: &detail,
            ..Default::default()
        },
    );
    if status == ST_LISTENING && !was_listening {
        crate::notify::wake(db, &name, &[]);
    }
    (0, r#"{"ok":true}"#.to_string())
}

fn handle_read(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let name = match parse_flag(argv, "--name") {
        Some(n) => n,
        None => return (0, r#"{"error":"Missing --name"}"#.to_string()),
    };
    let format_mode = has_flag(argv, "--format");
    let check_mode = has_flag(argv, "--check");
    let ack_mode = has_flag(argv, "--ack");

    let raw_messages = db.get_unread_messages(&name);
    let messages: Vec<Value> = raw_messages.iter().map(common::message_to_value).collect();

    if format_mode {
        // Always a JSON document: {"context", "up_to", "count"}. `up_to` is
        // the cursor of the delivered batch AFTER the per-delivery cap, so
        // acknowledging it can never skip messages beyond the batch.
        if messages.is_empty() {
            return (
                0,
                serde_json::json!({"context": "", "up_to": 0, "count": 0}).to_string(),
            );
        }
        let deliver = common::limit_delivery_messages(&messages);
        let up_to = deliver
            .iter()
            .filter_map(|m| m.get("event_id").and_then(|v| v.as_i64()))
            .max()
            .unwrap_or(0);
        let context = common::format_messages_json_for_instance(db, &deliver, &name);
        return (
            0,
            serde_json::json!({
                "context": context,
                "up_to": up_to,
                "count": deliver.len(),
            })
            .to_string(),
        );
    }
    if ack_mode {
        if let Some(up_to) = parse_flag(argv, "--up-to") {
            let Ok(ack_id) = up_to.parse::<i64>() else {
                return (
                    0,
                    serde_json::json!({"error": format!("Invalid --up-to: {up_to}")}).to_string(),
                );
            };
            if ack_id <= 0 {
                return (
                    0,
                    serde_json::json!({"error": format!("Invalid --up-to: {up_to}")}).to_string(),
                );
            }
            // Monotonic: the cursor never moves backwards, so a late/duplicate
            // ack for an older batch cannot re-mark newer messages unread.
            let current = db
                .get_instance_full(&name)
                .ok()
                .flatten()
                .map(|inst| inst.last_event_id)
                .unwrap_or(0);
            if ack_id <= current {
                return (
                    0,
                    serde_json::json!({"acked_to": current, "noop": true}).to_string(),
                );
            }
            let mut updates = serde_json::Map::new();
            updates.insert("last_event_id".into(), serde_json::json!(ack_id));
            instances::update_instance_position(db, &name, &updates);
            return (0, serde_json::json!({"acked_to": ack_id}).to_string());
        }
        // Cursor-less --ack is a manual/debug fallback: ack exactly this
        // instance's unread set, never beyond it (no global-max fallback —
        // that would ack events not yet delivered to this instance).
        if messages.is_empty() {
            return (0, r#"{"acked":0}"#.to_string());
        }
        let Some(ack_id) = messages
            .iter()
            .filter_map(|m| m.get("event_id").and_then(|v| v.as_i64()))
            .max()
            .filter(|id| *id > 0)
        else {
            return (0, r#"{"acked":0}"#.to_string());
        };
        let current = db
            .get_instance_full(&name)
            .ok()
            .flatten()
            .map(|inst| inst.last_event_id)
            .unwrap_or(0);
        let ack_id = ack_id.max(current);
        let mut updates = serde_json::Map::new();
        updates.insert("last_event_id".into(), serde_json::json!(ack_id));
        instances::update_instance_position(db, &name, &updates);
        return (
            0,
            serde_json::json!({"acked": messages.len(), "acked_to": ack_id}).to_string(),
        );
    }
    if check_mode {
        return (
            0,
            serde_json::json!({ "has_unread": !messages.is_empty() }).to_string(),
        );
    }
    (
        0,
        serde_json::to_string(&messages).unwrap_or_else(|_| "[]".to_string()),
    )
}

fn handle_beforetool(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let name = match parse_flag(argv, "--name") {
        Some(n) => n,
        None => return (0, r#"{"decision":"allow"}"#.to_string()),
    };
    let tool_name = parse_flag(argv, "--tool").unwrap_or_default();
    let input = parse_flag(argv, "--input-json")
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    if !tool_name.is_empty() {
        common::update_tool_status(db, &name, "hermes", &tool_name, &input);
    }
    (0, r#"{"decision":"allow"}"#.to_string())
}

fn handle_stop(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let name = match parse_flag(argv, "--name") {
        Some(n) => n,
        None => return (0, r#"{"error":"Missing --name"}"#.to_string()),
    };
    let reason = parse_flag(argv, "--reason").unwrap_or_else(|| "unknown".to_string());
    if has_flag(argv, "--soft") {
        common::soft_finalize_session(db, &name, &reason, None, true);
        (0, r#"{"ok":true,"soft":true}"#.to_string())
    } else {
        common::finalize_session(db, &name, &reason, None);
        (0, r#"{"ok":true}"#.to_string())
    }
}

pub fn dispatch_hermes_hook(hook_name: &str, argv: &[String]) -> (i32, String) {
    let start = Instant::now();
    let ctx = HcomContext::from_os();
    crate::paths::ensure_hcom_directories_at(&ctx.hcom_dir);
    let db = match HcomDb::open() {
        Ok(db) => db,
        Err(e) => {
            log_error(
                "hooks",
                "hook.error",
                &format!("hook={hook_name} op=db_open err={e}"),
            );
            return (
                0,
                serde_json::json!({"error": format!("DB open failed: {e}")}).to_string(),
            );
        }
    };
    let handler_argv: Vec<String> = if !argv.is_empty() && argv[0] == hook_name {
        argv[1..].to_vec()
    } else {
        argv.to_vec()
    };
    // An ordinary plugin-enabled Hermes session can be the first hcom
    // identity on a fresh DB; its session-start RPC must run even when the
    // gate sees no instances yet (mirrors the unmanaged-codex bootstrap
    // exception — only session-start may create identity).
    let can_bootstrap_unmanaged = hook_name == "hermes-sessionstart"
        && parse_flag(&handler_argv, "--session-id").is_some_and(|sid| !sid.is_empty());
    if !common::hook_gate_check(&ctx, &db) && !can_bootstrap_unmanaged {
        return (0, String::new());
    }
    let hook_name_owned = hook_name.to_string();
    let handler_start = Instant::now();
    let fallback = (
        0,
        serde_json::json!({"error": format!("hook handler panicked: {hook_name_owned}")})
            .to_string(),
    );
    let (exit_code, output) =
        common::dispatch_with_panic_guard("hermes", &hook_name_owned, fallback, || {
            match hook_name_owned.as_str() {
                "hermes-sessionstart" => handle_start(&ctx, &db, &handler_argv),
                "hermes-status" => handle_status(&db, &handler_argv),
                "hermes-read" => handle_read(&db, &handler_argv),
                "hermes-beforetool" => handle_beforetool(&db, &handler_argv),
                "hermes-stop" => handle_stop(&db, &handler_argv),
                _ => (
                    0,
                    serde_json::json!({"error": format!("Unknown Hermes hook: {hook_name_owned}")})
                        .to_string(),
                ),
            }
        });
    log_info(
        "hooks",
        "hermes.dispatch.timing",
        &format!(
            "hook={hook_name} handler_ms={:.2} total_ms={:.2} exit_code={exit_code}",
            handler_start.elapsed().as_secs_f64() * 1000.0,
            start.elapsed().as_secs_f64() * 1000.0,
        ),
    );
    (exit_code, output)
}

// ── Plugin artifact management (install/verify/remove) ───────────────────

pub const PLUGIN_INIT_SOURCE: &str = include_str!("../hermes_plugin/__init__.py");
pub const PLUGIN_YAML_SOURCE: &str = include_str!("../hermes_plugin/plugin.yaml");
const PLUGIN_DIR_NAME: &str = "hcom";

fn current_home_dir() -> std::path::PathBuf {
    std::env::var("HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_default())
}

/// User plugins live at `<hermes home>/plugins` (hermes_cli/plugins_discovery).
/// `HERMES_HOME` IS the hermes home itself (like `~/.hermes`), not its parent —
/// honor it verbatim so installs never duplicate `.hermes` inside a custom
/// home. Without it, fall back to the default home, isolating under the tool
/// config root when hcom runs against an isolated workspace (mirroring the
/// launcher's `isolated_tool_config_dir` choice of `<root>/.hermes`).
fn hermes_plugin_root() -> std::path::PathBuf {
    if let Some(home) = std::env::var("HERMES_HOME").ok().filter(|s| !s.is_empty()) {
        return std::path::PathBuf::from(home).join("plugins");
    }
    let tool_root = crate::runtime_env::tool_config_root();
    let home = current_home_dir();
    if tool_root == home {
        home.join(".hermes").join("plugins")
    } else {
        tool_root.join(".hermes").join("plugins")
    }
}

pub fn get_hermes_plugin_path() -> std::path::PathBuf {
    hermes_plugin_root()
        .join(PLUGIN_DIR_NAME)
        .join("plugin.yaml")
}

fn plugin_matches_source() -> bool {
    let init = hermes_plugin_root()
        .join(PLUGIN_DIR_NAME)
        .join("__init__.py");
    let yaml = get_hermes_plugin_path();
    match (
        std::fs::read_to_string(&init),
        std::fs::read_to_string(&yaml),
    ) {
        (Ok(a), Ok(b)) => a == PLUGIN_INIT_SOURCE && b == PLUGIN_YAML_SOURCE,
        _ => false,
    }
}

/// The hermes home itself (`<root>/.hermes` or `$HERMES_HOME`).
fn hermes_home_dir() -> std::path::PathBuf {
    hermes_plugin_root()
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_default()
}

fn hermes_config_path() -> std::path::PathBuf {
    hermes_home_dir().join("config.yaml")
}

/// Normalize a YAML scalar list item: strip one pair of matching surrounding
/// quotes (PyYAML quotes plain-looking names it can't be sure about).
fn normalize_yaml_item(item: &str) -> String {
    let trimmed = item.trim();
    let bytes = trimmed.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            // Both stripped bytes are ASCII, so the slice boundaries are
            // valid char boundaries.
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

/// Normalize a block-sequence item (`- name`), ignoring an unquoted
/// trailing comment — `- hcom  # note` is the name `hcom` in valid YAML, so
/// reading it as `hcom  # note` would misread the enabled state. None when
/// the item cannot be parsed safely (unparseable comment/quoting shapes,
/// empty name): callers treat that as unresolvable rather than guess.
fn normalize_block_item(item: &str) -> Option<String> {
    let trimmed = item.trim();
    let bytes = trimmed.as_bytes();
    let mut in_quote: Option<u8> = None;
    let mut comment: Option<usize> = None;
    for (index, &byte) in bytes.iter().enumerate() {
        match in_quote {
            Some(q) => {
                if byte == q {
                    in_quote = None;
                }
            }
            None => match byte {
                b'\'' | b'"' => in_quote = Some(byte),
                // A `#` at the start or preceded by whitespace begins a
                // comment; one glued to a scalar (`a#b`) leaves a name this
                // narrow reader refuses to interpret (see the check below).
                b'#' if index == 0 || bytes[index - 1].is_ascii_whitespace() => {
                    comment = Some(index);
                    break;
                }
                _ => {}
            },
        }
    }
    let scalar = &trimmed[..comment.unwrap_or(trimmed.len())];
    let name = normalize_yaml_item(scalar);
    if name.is_empty()
        || name.contains(['\'', '"', '#', ','])
        || name.chars().any(|ch| ch.is_whitespace())
    {
        return None;
    }
    Some(name)
}

/// Outcome of scanning Hermes's `plugins.enabled` list.
#[derive(Debug, PartialEq)]
enum EnabledScan {
    /// No enabled list in the content at all.
    NoList,
    /// The list parsed confidently; normalized names in document order.
    Parsed(Vec<String>),
    /// A list exists but its shape is beyond this narrow scanner (unclosed
    /// flow list, unbalanced quotes). Callers must not guess: verification
    /// reads it as not-enabled (the CLI enable step self-heals), removal
    /// refuses to claim a clean state.
    Unresolved,
}

/// Content between a flow list's opening `[` and its matching `]`, plus the
/// raw text after the closing bracket. None when the bracket never closes at
/// the top level or a quote is left open before it.
fn split_flow_span(joined: &str) -> Option<(&str, &str)> {
    let start = joined.find('[')? + 1;
    let bytes = joined.as_bytes();
    let mut quote: Option<u8> = None;
    let mut end = None;
    for (index, &byte) in bytes.iter().enumerate().skip(start) {
        match quote {
            Some(q) => {
                if byte == q {
                    quote = None;
                }
            }
            None => match byte {
                b'\'' | b'"' => quote = Some(byte),
                b']' => {
                    end = Some(index);
                    break;
                }
                _ => {}
            },
        }
    }
    let end = end?;
    // The bracket and quote bytes are ASCII, so the slice boundaries are
    // valid char boundaries.
    Some((&joined[start..end], &joined[end + 1..]))
}

/// Split a flow-list body on top-level commas, honoring single- and
/// double-quoted scalars (a comma inside quotes does not split), and
/// normalizing each item. None when quotes are unbalanced or an unquoted
/// `#` appears (comments are not valid inside flow collections, so such a
/// config is ambiguous) — the caller must treat that as unresolvable, not
/// guess.
fn split_flow_items(inner: &str) -> Option<Vec<String>> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    for ch in inner.chars() {
        match quote {
            Some(q) => {
                current.push(ch);
                if ch == q {
                    quote = None;
                }
            }
            None => match ch {
                '\'' | '"' => {
                    quote = Some(ch);
                    current.push(ch);
                }
                '#' => return None,
                ',' => {
                    items.push(normalize_yaml_item(&current));
                    current.clear();
                }
                _ => current.push(ch),
            },
        }
    }
    if quote.is_some() {
        return None;
    }
    items.push(normalize_yaml_item(&current));
    Some(items.into_iter().filter(|item| !item.is_empty()).collect())
}

/// Whether a parsed flow item can be re-emitted bare without changing
/// meaning — no quotes, commas, brackets, comments, colons, or whitespace.
fn is_safe_flow_item(item: &str) -> bool {
    !item.is_empty()
        && !item.contains(['\'', '"', ',', '[', ']', '#', ':'])
        && item.chars().all(|ch| !ch.is_whitespace())
}

/// Consume a `enabled: […]` occurrence for the scanner: single-line lists
/// parse in place; multi-line ones (opening `[` without `]` on the key line)
/// park in `flow_pending` until the closing bracket arrives.
fn scan_flow_occurrence(
    rest: &str,
    names: &mut Vec<String>,
    saw_list: &mut bool,
    unresolved: &mut bool,
    flow_pending: &mut Option<String>,
) {
    if !rest.contains(']') {
        *flow_pending = Some(rest.to_string());
        return;
    }
    match split_flow_span(rest) {
        Some((body, after)) if after.trim().is_empty() || after.trim_start().starts_with('#') => {
            match split_flow_items(body) {
                Some(items) => {
                    names.extend(items);
                    *saw_list = true;
                }
                None => *unresolved = true,
            }
        }
        // Trailing junk after the closing bracket, or a quote that never
        // closed before it: shapes we refuse to interpret.
        Some(_) | None => *unresolved = true,
    }
}

/// Minimal read-only scan of Hermes's `plugins:` table for the enabled list.
/// Hermes owns config.yaml — hcom never writes it directly; this check only
/// decides whether the CLI enable step still needs to run. Covers the shapes
/// real writers produce: `hermes plugins enable`'s indented block lists,
/// PyYAML's same-indent sequence style (`enabled:` / `- name` at one indent),
/// quoted block items, unquoted inline comments (`- hcom  # note`,
/// `enabled: # note`), and flow lists (`enabled: [a, 'b']`) including
/// multi-line ones and commas inside quoted scalars. There is no YAML
/// dependency in the workspace to reuse, so this stays a narrowly scoped
/// scanner; anything beyond it is [`EnabledScan::Unresolved`] rather than a
/// guessed answer.
fn scan_hermes_enabled_plugins(content: &str) -> EnabledScan {
    let mut in_plugins = false;
    let mut enabled_indent: Option<usize> = None;
    let mut names = Vec::new();
    let mut saw_list = false;
    let mut unresolved = false;
    // Multi-line flow: text after `enabled: [`, accumulated until `]`.
    let mut flow_pending: Option<String> = None;

    for line in content.lines() {
        let text = line.trim_start();
        if text.is_empty() || text.starts_with('#') {
            continue;
        }
        let indent = line.len() - text.len();
        if flow_pending.is_some() {
            // Still inside an unclosed flow list. A top-level key before the
            // closing bracket means the list never closed inside plugins: —
            // not something to guess about.
            if indent == 0 {
                unresolved = true;
                flow_pending = None;
                in_plugins = text == "plugins:";
                enabled_indent = None;
                continue;
            }
            let mut pending = flow_pending.take().unwrap_or_default();
            pending.push(' ');
            pending.push_str(text);
            if pending.contains(']') {
                let (body, after) = match split_flow_span(&pending) {
                    Some(span) => span,
                    None => {
                        unresolved = true;
                        continue;
                    }
                };
                if after.trim().is_empty() || after.trim_start().starts_with('#') {
                    match split_flow_items(body) {
                        Some(items) => {
                            names.extend(items);
                            saw_list = true;
                        }
                        None => unresolved = true,
                    }
                } else {
                    unresolved = true;
                }
            } else {
                flow_pending = Some(pending);
            }
            continue;
        }
        if indent == 0 {
            in_plugins = text == "plugins:";
            enabled_indent = None;
            continue;
        }
        if !in_plugins {
            continue;
        }
        match enabled_indent {
            None => {
                if let Some(rest) = text.strip_prefix("enabled:") {
                    let rest = rest.trim();
                    if rest.starts_with('[') {
                        scan_flow_occurrence(
                            rest,
                            &mut names,
                            &mut saw_list,
                            &mut unresolved,
                            &mut flow_pending,
                        );
                    } else {
                        enabled_indent = Some(indent);
                    }
                }
            }
            Some(list_indent) => {
                if let Some(item) = text.strip_prefix("- ")
                    && indent >= list_indent
                {
                    // Sequence items belong to the enabled list both at the
                    // key's own indent (PyYAML block style) and deeper
                    // (hermes's own writer). An item this reader cannot
                    // normalize (ambiguous comment/quoting shape) marks the
                    // whole scan unresolved instead of guessing a name.
                    match normalize_block_item(item) {
                        Some(name) => {
                            names.push(name);
                            saw_list = true;
                        }
                        None => unresolved = true,
                    }
                } else {
                    // A non-item line at or above the key's indent is a
                    // sibling key under plugins: (e.g. disabled:) and ends
                    // the list; another enabled: key restarts it — block or
                    // flow. A key comment (`enabled: # note`) still opens a
                    // block list, same as a bare `enabled:`.
                    enabled_indent = None;
                    if let Some(rest) = text.strip_prefix("enabled:") {
                        let rest = rest.trim();
                        if rest.starts_with('[') {
                            scan_flow_occurrence(
                                rest,
                                &mut names,
                                &mut saw_list,
                                &mut unresolved,
                                &mut flow_pending,
                            );
                        } else if (rest.is_empty() || rest.starts_with('#'))
                            && indent == list_indent
                        {
                            enabled_indent = Some(indent);
                        }
                    }
                }
            }
        }
    }
    if flow_pending.is_some() {
        // EOF inside an unclosed flow list.
        unresolved = true;
    }
    if unresolved {
        return EnabledScan::Unresolved;
    }
    if saw_list {
        EnabledScan::Parsed(names)
    } else {
        EnabledScan::NoList
    }
}

/// The narrow scanner's answer as the parse-level tests express it:
/// Some(names) for a confidently-parsed list, None otherwise. Production
/// code uses scan_hermes_enabled_plugins/hermes_hcom_enabled_state so
/// Unresolved is never collapsed into "absent".
#[cfg(test)]
fn parse_hermes_enabled_plugins(content: &str) -> Option<Vec<String>> {
    match scan_hermes_enabled_plugins(content) {
        EnabledScan::Parsed(names) => Some(names),
        EnabledScan::NoList | EnabledScan::Unresolved => None,
    }
}

/// hcom's presence in Hermes's `plugins.enabled`, without ever guessing.
#[derive(Debug, PartialEq, Eq)]
enum HermesEnabledState {
    /// No enabled list in the config at all (or no config file).
    NoList,
    /// A list was read confidently; hcom is not in it.
    NotEnabled,
    /// hcom is enabled.
    Enabled,
    /// A list exists but this reader cannot tell whether hcom is in it.
    Unresolved,
}

fn hermes_hcom_enabled_state() -> HermesEnabledState {
    let content = match std::fs::read_to_string(hermes_config_path()) {
        Ok(content) => content,
        // No config file at all: nothing can be enabled.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return HermesEnabledState::NoList;
        }
        // Unreadable for any other reason (permissions, not a file, not valid
        // UTF-8): claiming "no list" here would let removal report success
        // over a config it could not read.
        Err(_) => return HermesEnabledState::Unresolved,
    };
    match scan_hermes_enabled_plugins(&content) {
        EnabledScan::NoList => HermesEnabledState::NoList,
        EnabledScan::Parsed(names) => {
            if names.iter().any(|name| name == "hcom") {
                HermesEnabledState::Enabled
            } else {
                HermesEnabledState::NotEnabled
            }
        }
        EnabledScan::Unresolved => HermesEnabledState::Unresolved,
    }
}

fn hermes_plugin_enabled_in_config() -> bool {
    hermes_hcom_enabled_state() == HermesEnabledState::Enabled
}

fn hermes_cli_command() -> std::path::PathBuf {
    #[cfg(test)]
    if let Some(bin) = std::env::var("HCOM_TEST_HERMES_BIN")
        .ok()
        .filter(|s| !s.is_empty())
    {
        return std::path::PathBuf::from(bin);
    }
    std::path::PathBuf::from("hermes")
}

/// Error from `hermes plugins …`. Carries whether the CLI could be run at
/// all: removal falls back to the surgical config clean only when the CLI is
/// unavailable, never when it ran and failed.
#[derive(Debug)]
enum HermesCliError {
    /// The CLI could not be spawned (typically: not installed / not on PATH).
    NotAvailable(String),
    /// It ran but failed, timed out, or could not be waited on.
    Failed(String),
}

impl HermesCliError {
    fn message(&self) -> &str {
        match self {
            HermesCliError::NotAvailable(message) | HermesCliError::Failed(message) => message,
        }
    }
}

fn cli_timeout() -> Duration {
    // Test hook: keeps the timeout-behavior test fast without exposing a
    // knob in real runs.
    #[cfg(test)]
    if let Some(ms) = std::env::var("HCOM_TEST_HERMES_CLI_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        && ms > 0
    {
        return Duration::from_millis(ms);
    }
    Duration::from_secs(30)
}

/// Run `hermes plugins <subcommand>` through Hermes's own CLI so config.yaml
/// edits, ordering, and future plugin semantics stay Hermes-owned.
///
/// - HERMES_HOME is pinned to `hermes_home_dir()` — the same home the file
///   installation and verification use — so an isolated HCOM_DIR (ambient
///   HERMES_HOME unset) enables the plugin in the config hcom actually reads,
///   not the CLI's default ~/.hermes.
/// - Stdin is null: the enable flow's only prompt (the tool-override grant)
///   reads from stdin and EOF-denies safely when it can never be answered.
/// - The child runs in its own session/process group (`detach_session`), so
///   the group kill on timeout cannot signal hcom or its siblings.
/// - stdout is discarded (never read) and stderr is drained on a reader
///   thread: piping output nobody reads would deadlock a chatty CLI once the
///   pipe buffer (~64KB) fills, turning a healthy run into a bogus timeout.
/// - The wait is bounded (mirrors shell_env's timed_shell_output pattern) so
///   a CLI that blocks anyway cannot hang hooks setup; the timeout and
///   wait-error branches kill, reap, and return their error directly — the
///   loop never re-polls a child it already killed. std-only, no new dep.
///
/// Returns an actionable message on failure (the exact command to run
/// manually).
fn run_hermes_plugins_cli(args: &[&str]) -> Result<(), HermesCliError> {
    let display = format!("hermes {}", args.join(" "));
    let timeout = cli_timeout();
    let mut command = std::process::Command::new(hermes_cli_command());
    command
        .args(args)
        .env("HERMES_HOME", hermes_home_dir())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    crate::sys::process::detach_session(&mut command);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            return Err(HermesCliError::NotAvailable(format!(
                "could not run `{display}`: {e}; is the hermes CLI on PATH?"
            )));
        }
    };
    let stderr_handle = child.stderr.take();
    let stderr_reader = std::thread::spawn(move || {
        let mut buffer = Vec::new();
        if let Some(mut stderr) = stderr_handle {
            use std::io::Read;
            let _ = stderr.read_to_end(&mut buffer);
        }
        buffer
    });

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let stderr = String::from_utf8_lossy(&stderr_reader.join().unwrap_or_default())
                    .trim()
                    .to_string();
                if status.success() {
                    return Ok(());
                }
                let detail = if stderr.is_empty() {
                    format!("exit status {}", status.code().unwrap_or(-1))
                } else {
                    stderr
                };
                return Err(HermesCliError::Failed(format!(
                    "`{display}` failed ({detail}); run it manually"
                )));
            }
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Ok(None) => {
                crate::sys::process::kill_child_group(&mut child);
                let _ = child.wait();
                return Err(HermesCliError::Failed(format!(
                    "`{display}` did not finish within {}s (killed — it may be waiting for input); run it manually",
                    timeout.as_secs_f64()
                )));
            }
            Err(e) => {
                crate::sys::process::kill_child_group(&mut child);
                let _ = child.wait();
                return Err(HermesCliError::Failed(format!(
                    "could not wait for `{display}`: {e}"
                )));
            }
        }
    }
}

/// Installed means files match AND Hermes config has the plugin enabled.
pub fn verify_hermes_plugin_installed() -> bool {
    plugin_matches_source() && hermes_plugin_enabled_in_config()
}

pub fn install_hermes_plugin() -> std::io::Result<bool> {
    let dir = hermes_plugin_root().join(PLUGIN_DIR_NAME);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("__init__.py"), PLUGIN_INIT_SOURCE)?;
    std::fs::write(dir.join("plugin.yaml"), PLUGIN_YAML_SOURCE)?;
    // Hermes gates user plugins behind `plugins.enabled` in config.yaml —
    // writing files alone leaves the plugin discovered-but-skipped (installer
    // bug observed live: "Skipping 'hcom' (not in plugins.enabled)"). Enable
    // through the official CLI so config edits, ordering, and future plugin
    // semantics stay Hermes-owned. A failed enable is surfaced as an error,
    // not logged away: an installed-but-disabled plugin looks like success
    // while the session never joins the mesh.
    //
    // Plain `hermes plugins enable hcom` (no --no-allow-tool-override): the
    // hcom manifest declares no tools or capabilities, so 0.21's enable is
    // non-interactive — the legacy tool-override prompt only reads stdin,
    // which is null here (EOF → safe deny). Pinning the flag bought nothing
    // and would break CLI versions that lack it.
    if let Err(error) = run_hermes_plugins_cli(&["plugins", "enable", "hcom"]) {
        return Err(std::io::Error::other(format!(
            "plugin files installed at {} but not enabled — {}",
            dir.display(),
            error.message()
        )));
    }
    Ok(true)
}

/// Verify + install, surfacing the installer's own error (files-installed
/// path, CLI failure detail) so callers like the launcher can show the real
/// cause instead of a generic rerun hint.
pub fn ensure_hermes_plugin_installed() -> Result<(), String> {
    if verify_hermes_plugin_installed() {
        return Ok(());
    }
    install_hermes_plugin()
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Replace Hermes's config.yaml through a same-directory temp file + atomic
/// rename (same pattern as paths::atomic_write_io): a failure anywhere
/// leaves the original config untouched instead of a truncated file. The
/// temp file adopts the original's permissions where we can (ownership
/// cannot change unprivileged).
fn write_hermes_config_atomically(
    config_path: &std::path::Path,
    content: &str,
) -> std::io::Result<()> {
    let dir = config_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    std::io::Write::write_all(&mut tmp, content.as_bytes())?;
    tmp.as_file().sync_all()?;
    if let Ok(metadata) = std::fs::metadata(config_path) {
        let _ = tmp.as_file().set_permissions(metadata.permissions());
    }
    tmp.persist(config_path).map(|_| ()).map_err(|e| e.error)
}

/// Remove just the hcom entries from `plugins.enabled` — block-style item
/// lines (`- hcom`) and flow-style lists (`enabled: [hcom, …]`, single- or
/// multi-line) alike — leaving every other line byte-identical, including
/// newline style: a CRLF file stays CRLF and a file without a trailing
/// newline does not gain one. Reached when the plugin files are gone (stale
/// entry) or the CLI is unavailable. Surgical by construction: a line
/// changes only when it belongs to an enabled list (same traversal rules as
/// the scanner) and normalizes to exactly `hcom` — an unquoted trailing
/// comment on a dropped item (`- hcom  # note`) goes with it; a list left
/// empty becomes `enabled: []` so the key stays a well-defined list, unless
/// the key line itself carries a trailing comment that collapse would lose.
/// Ambiguity is never guessed at: an item or flow list whose shape cannot
/// be parsed confidently (matching the scanner's Unresolved cases), or flow
/// entries that cannot be re-emitted losslessly (quotes, embedded commas,
/// trailing comments), is refused with an error instead of rewritten —
/// removal must not corrupt unrelated names, and must not claim success
/// while the enabled state is unknowable. Failures propagate; the write is
/// atomic (see [`write_hermes_config_atomically`]).
fn remove_stale_hcom_enabled_entries() -> Result<(), String> {
    let config_path = hermes_config_path();
    let content = std::fs::read_to_string(&config_path)
        .map_err(|e| format!("could not read {}: {e}", config_path.display()))?;
    // Rebuild losslessly for any newline style: `split('\n')` keeps a
    // trailing "" element when the file ends with a newline (so `join`
    // restores it exactly), and one trailing \r per line is stripped here
    // and re-added by the CRLF joiner.
    let eol: &str = if content.contains("\r\n") {
        "\r\n"
    } else {
        "\n"
    };
    let lines: Vec<String> = content
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line).to_string())
        .collect();

    const KEY_COMMENT_LOST: &str = "cannot safely rewrite the enabled list in config.yaml \
         (a comment on the enabled: key would be lost); run `hermes plugins disable hcom` or \
         edit config.yaml manually";

    // Plan edits (nested fns instead of closures: they need &mut to several
    // planning buffers).
    // Flush a finished block list: drop its hcom item lines and, when hcom
    // was the only item, collapse the key to `enabled: []` — unless the key
    // line carries trailing content (a comment) that collapse would lose.
    fn flush_block_list(
        key_line: &mut Option<(usize, usize, bool)>,
        items: &mut Vec<(usize, bool)>,
        drop_lines: &mut Vec<usize>,
        replace_lines: &mut Vec<(usize, String)>,
    ) -> Result<(), String> {
        if let Some((key_idx, indent, key_clean)) = *key_line
            && items.iter().any(|(_, is_hcom)| *is_hcom)
        {
            drop_lines.extend(
                items
                    .iter()
                    .filter(|(_, is_hcom)| *is_hcom)
                    .map(|(idx, _)| *idx),
            );
            if !items.iter().any(|(_, is_hcom)| !*is_hcom) {
                if !key_clean {
                    return Err(KEY_COMMENT_LOST.to_string());
                }
                replace_lines.push((key_idx, format!("{}enabled: []", " ".repeat(indent))));
            }
        }
        *key_line = None;
        items.clear();
        Ok(())
    }

    /// Rewrite a closed flow list (single-line: `joined` is the key line's
    /// text after `enabled:`; multi-line: the accumulated text). Refuses —
    /// never rewrites — anything that cannot round-trip losslessly.
    fn resolve_flow_rewrite(
        joined: &str,
        key_indent: usize,
        start_idx: usize,
        continuation: &[usize],
        drop_lines: &mut Vec<usize>,
        replace_lines: &mut Vec<(usize, String)>,
    ) -> Result<(), String> {
        let refuse = |why: &str| {
            format!(
                "cannot safely rewrite the flow-style enabled list in config.yaml \
                 ({why}); run `hermes plugins disable hcom` or edit config.yaml manually"
            )
        };
        let Some((body, after)) = split_flow_span(joined) else {
            return Err(refuse("unclosed bracket or unbalanced quotes"));
        };
        if !after.trim().is_empty() {
            // Trailing content (e.g. a comment) would be lost by collapsing
            // the list onto one line.
            return Err(refuse("content after the closing bracket"));
        }
        let Some(items) = split_flow_items(body) else {
            return Err(refuse("unbalanced quotes or an unquoted comment"));
        };
        if !items.iter().any(|item| item == "hcom") {
            return Ok(());
        }
        let remaining: Vec<&str> = items
            .iter()
            .filter(|item| item.as_str() != "hcom")
            .map(String::as_str)
            .collect();
        if remaining.iter().any(|item| !is_safe_flow_item(item)) {
            // Emitting such an item bare would corrupt it (quotes, embedded
            // commas, …) — refuse rather than guess.
            return Err(refuse("entries that cannot be re-emitted losslessly"));
        }
        replace_lines.push((
            start_idx,
            format!(
                "{}enabled: [{}]",
                " ".repeat(key_indent),
                remaining.join(", ")
            ),
        ));
        drop_lines.extend_from_slice(continuation);
        Ok(())
    }

    /// An `enabled: [` occurrence: resolve single-line lists immediately,
    /// park multi-line ones (opening bracket, no closing bracket yet).
    fn handle_flow_occurrence(
        idx: usize,
        indent: usize,
        rest: &str,
        flow: &mut Option<(usize, usize, Vec<usize>, String)>,
        drop_lines: &mut Vec<usize>,
        replace_lines: &mut Vec<(usize, String)>,
    ) -> Result<(), String> {
        if rest.contains(']') {
            resolve_flow_rewrite(rest, indent, idx, &[], drop_lines, replace_lines)
        } else {
            *flow = Some((idx, indent, Vec::new(), rest.to_string()));
            Ok(())
        }
    }

    let mut in_plugins = false;
    let mut enabled_indent: Option<usize> = None;
    // (line index, indent, whether the key line is a bare `enabled:`).
    let mut key_line: Option<(usize, usize, bool)> = None;
    let mut items: Vec<(usize, bool)> = Vec::new(); // current list's block items
    // Open multi-line flow list: (start index, key indent, continuation line
    // indices, accumulated text after `enabled:`).
    let mut flow: Option<(usize, usize, Vec<usize>, String)> = None;
    let mut drop_lines: Vec<usize> = Vec::new();
    let mut replace_lines: Vec<(usize, String)> = Vec::new();
    const NEVER_CLOSED: &str = "cannot safely rewrite the flow-style enabled list in config.yaml \
         (never closed); run `hermes plugins disable hcom` or edit config.yaml manually";
    for (idx, line) in lines.iter().enumerate() {
        let text = line.trim_start();
        if text.is_empty() || text.starts_with('#') {
            continue;
        }
        let indent = line.len() - text.len();
        if let Some((start_idx, key_indent, mut continuation, mut joined)) = flow.take() {
            // Still inside an unclosed flow list. A top-level key before the
            // closing bracket means it never closed inside plugins:.
            if indent == 0 {
                return Err(NEVER_CLOSED.to_string());
            }
            continuation.push(idx);
            joined.push(' ');
            joined.push_str(text);
            if joined.contains(']') {
                resolve_flow_rewrite(
                    &joined,
                    key_indent,
                    start_idx,
                    &continuation,
                    &mut drop_lines,
                    &mut replace_lines,
                )?;
            } else {
                flow = Some((start_idx, key_indent, continuation, joined));
            }
            continue;
        }
        if indent == 0 {
            flush_block_list(
                &mut key_line,
                &mut items,
                &mut drop_lines,
                &mut replace_lines,
            )?;
            in_plugins = text == "plugins:";
            enabled_indent = None;
            continue;
        }
        if !in_plugins {
            continue;
        }
        match enabled_indent {
            None => {
                if let Some(rest) = text.strip_prefix("enabled:") {
                    let rest = rest.trim();
                    if rest.starts_with('[') {
                        handle_flow_occurrence(
                            idx,
                            indent,
                            rest,
                            &mut flow,
                            &mut drop_lines,
                            &mut replace_lines,
                        )?;
                    } else {
                        enabled_indent = Some(indent);
                        key_line = Some((idx, indent, rest.is_empty()));
                        items.clear();
                    }
                }
            }
            Some(list_indent) => {
                if let Some(item) = text.strip_prefix("- ")
                    && indent >= list_indent
                {
                    // Same normalization as the scanner: an item whose
                    // comment/quoting shape cannot be parsed confidently
                    // makes the whole list unrewritable, not a guess.
                    match normalize_block_item(item) {
                        Some(name) => items.push((idx, name == "hcom")),
                        None => {
                            return Err(format!(
                                "cannot safely rewrite the enabled list in config.yaml \
                                 (cannot parse item `{item}`); run `hermes plugins disable \
                                 hcom` or edit config.yaml manually"
                            ));
                        }
                    }
                } else {
                    enabled_indent = None;
                    flush_block_list(
                        &mut key_line,
                        &mut items,
                        &mut drop_lines,
                        &mut replace_lines,
                    )?;
                    if let Some(rest) = text.strip_prefix("enabled:") {
                        let rest = rest.trim();
                        if rest.starts_with('[') {
                            handle_flow_occurrence(
                                idx,
                                indent,
                                rest,
                                &mut flow,
                                &mut drop_lines,
                                &mut replace_lines,
                            )?;
                        } else if (rest.is_empty() || rest.starts_with('#'))
                            && indent == list_indent
                        {
                            enabled_indent = Some(indent);
                            key_line = Some((idx, indent, rest.is_empty()));
                            items.clear();
                        }
                    }
                }
            }
        }
    }
    if flow.is_some() {
        // EOF inside an unclosed flow list.
        return Err(NEVER_CLOSED.to_string());
    }
    flush_block_list(
        &mut key_line,
        &mut items,
        &mut drop_lines,
        &mut replace_lines,
    )?;
    if drop_lines.is_empty() && replace_lines.is_empty() {
        return Ok(());
    }

    let mut rewritten: Vec<String> = Vec::with_capacity(lines.len());
    for (idx, line) in lines.iter().enumerate() {
        if drop_lines.contains(&idx) {
            continue;
        }
        if let Some((_, replacement)) = replace_lines.iter().find(|(i, _)| *i == idx) {
            rewritten.push(replacement.clone());
            continue;
        }
        rewritten.push(line.clone());
    }
    write_hermes_config_atomically(&config_path, &rewritten.join(eol))
        .map_err(|e| format!("could not write {}: {e}", config_path.display()))
}

pub fn remove_hermes_plugin() -> std::io::Result<()> {
    let dir = hermes_plugin_root().join(PLUGIN_DIR_NAME);
    let files_present = dir.exists();
    let state = hermes_hcom_enabled_state();

    // Idempotent: nothing installed and provably not enabled → Ok without
    // invoking the CLI, so `hcom hooks remove all` succeeds on machines
    // without hermes on PATH.
    if !files_present
        && matches!(
            state,
            HermesEnabledState::NotEnabled | HermesEnabledState::NoList
        )
    {
        return Ok(());
    }
    if !files_present && state == HermesEnabledState::Unresolved {
        // Never claim a clean removal when the enabled state is unknowable.
        return Err(std::io::Error::other(format!(
            "cannot tell whether hcom is enabled in {}; run `hermes plugins disable \
             hcom` or edit config.yaml manually",
            hermes_config_path().display()
        )));
    }

    if files_present {
        // Disable through the CLI FIRST while the files still exist: Hermes
        // 0.21.4's `plugins disable` resolves the plugin key and fails with
        // unknown-plugin once the directory is gone — deleting first would
        // make the disable unrunnable and leave a stale `plugins.enabled`
        // entry. Only when there is something to disable: a failed enable
        // leaves files with a clean config, and removing those must not
        // require the CLI at all. When the CLI is unavailable, fall back to
        // the surgical config clean (its refusals and errors propagate) and
        // still remove the files.
        if matches!(
            state,
            HermesEnabledState::Enabled | HermesEnabledState::Unresolved
        ) {
            match run_hermes_plugins_cli(&["plugins", "disable", "hcom"]) {
                Ok(()) => {
                    // Exit 0 does not prove the config changed (a stub or a
                    // future CLI no-op would still leave hcom enabled), so
                    // verify before deleting the files: still enabled → clean
                    // surgically; still unreadable → refuse rather than claim
                    // success over an unknown state.
                    match hermes_hcom_enabled_state() {
                        HermesEnabledState::Enabled => {
                            remove_stale_hcom_enabled_entries().map_err(|e| {
                                std::io::Error::other(format!(
                                    "hermes CLI reported success but hcom is still enabled — {e}"
                                ))
                            })?;
                        }
                        HermesEnabledState::Unresolved => {
                            return Err(std::io::Error::other(format!(
                                "cannot verify hcom was disabled in {}; run `hermes \
                                 plugins disable hcom` or edit config.yaml manually",
                                hermes_config_path().display()
                            )));
                        }
                        HermesEnabledState::NotEnabled | HermesEnabledState::NoList => {}
                    }
                }
                Err(HermesCliError::NotAvailable(_)) => {
                    remove_stale_hcom_enabled_entries().map_err(|e| {
                        std::io::Error::other(format!(
                            "plugin files present but config could not be cleaned — {e}"
                        ))
                    })?;
                }
                Err(error) => {
                    return Err(std::io::Error::other(format!(
                        "plugin still installed at {} but not disabled — {}",
                        dir.display(),
                        error.message()
                    )));
                }
            }
        }
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(|e| {
                std::io::Error::other(format!(
                    "plugin disabled in config but files remain at {} — {e}",
                    dir.display()
                ))
            })?;
        }
    } else {
        // Stale enabled entry without files: the CLI cannot resolve the
        // missing directory, so clean the entry surgically. Errors propagate
        // — remove must not claim success while hcom may still be enabled.
        remove_stale_hcom_enabled_entries().map_err(|e| {
            std::io::Error::other(format!(
                "stale hcom enabled entry may remain in {} — {e}",
                hermes_config_path().display()
            ))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_flags() {
        let argv: Vec<String> = vec![
            "hermes-read".to_string(),
            "--name".to_string(),
            "luna".to_string(),
            "--format".to_string(),
        ];
        assert_eq!(parse_flag(&argv, "--name").as_deref(), Some("luna"));
        assert!(has_flag(&argv, "--format"));
        assert!(!has_flag(&argv, "--ack"));
        // hook-name prefix stripping
        let handler_argv: Vec<String> = if !argv.is_empty() && argv[0] == "hermes-read" {
            argv[1..].to_vec()
        } else {
            argv.clone()
        };
        assert_eq!(parse_flag(&handler_argv, "--name").as_deref(), Some("luna"));
    }

    #[test]
    fn plugin_sources_are_nonempty() {
        assert!(PLUGIN_INIT_SOURCE.contains("def register(ctx)"));
        assert!(PLUGIN_YAML_SOURCE.contains("pre_llm_call"));
    }

    // ── handle_read contract tests ────────────────────────────────────────

    fn setup_test_db() -> (HcomDb, std::path::PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let db_path = std::env::temp_dir().join(format!(
            "test_hermes_hooks_{}_{}.db",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let db = HcomDb::open_at(&db_path).unwrap();
        (db, db_path)
    }

    fn cleanup_db(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    fn save_hermes_instance(db: &HcomDb, name: &str) {
        let mut row = serde_json::Map::new();
        row.insert("name".into(), serde_json::json!(name));
        row.insert("tool".into(), serde_json::json!("hermes"));
        row.insert("status".into(), serde_json::json!(ST_LISTENING));
        row.insert("status_context".into(), serde_json::json!(""));
        row.insert("status_detail".into(), serde_json::json!(""));
        row.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named(name, &row).unwrap();
    }

    fn log_broadcast(db: &HcomDb, from: &str, text: &str) -> i64 {
        db.log_event(
            "message",
            from,
            &serde_json::json!({"from": from, "text": text, "scope": "broadcast"}),
        )
        .unwrap()
    }

    fn argv_of(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn read_format_returns_json_document_with_batch_cursor() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "luna");
        let e1 = log_broadcast(&db, "sol", "first");
        let e2 = log_broadcast(&db, "sol", "second");
        let (_, out) = handle_read(&db, &argv_of(&["--name", "luna", "--format"]));
        cleanup_db(&path);
        let doc: Value = serde_json::from_str(&out).expect("output must be one JSON document");
        assert!(doc.get("error").is_none());
        let context = doc["context"].as_str().expect("context is a string");
        assert!(context.contains("<hcom>"), "context wraps the hcom block");
        assert!(context.contains("first") && context.contains("second"));
        assert_eq!(doc["up_to"].as_i64(), Some(e2.max(e1)));
        assert_eq!(doc["count"].as_i64(), Some(2));
    }

    #[test]
    fn read_format_empty_returns_zero_cursor_document() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "luna");
        let (_, out) = handle_read(&db, &argv_of(&["--name", "luna", "--format"]));
        cleanup_db(&path);
        // Never an empty/non-JSON body: the plugin parses every reply as JSON.
        let doc: Value = serde_json::from_str(&out).expect("output must be one JSON document");
        assert_eq!(doc["context"].as_str(), Some(""));
        assert_eq!(doc["up_to"].as_i64(), Some(0));
        assert_eq!(doc["count"].as_i64(), Some(0));
    }

    #[test]
    fn read_check_reports_json_has_unread() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "luna");
        let (_, out) = handle_read(&db, &argv_of(&["--name", "luna", "--check"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(doc["has_unread"].as_bool(), Some(false));
        log_broadcast(&db, "sol", "wake up");
        let (_, out) = handle_read(&db, &argv_of(&["--name", "luna", "--check"]));
        cleanup_db(&path);
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(doc["has_unread"].as_bool(), Some(true));
    }

    #[test]
    fn ack_up_to_is_monotonic() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "luna");
        let _ = log_broadcast(&db, "sol", "one");
        let e2 = log_broadcast(&db, "sol", "two");
        let e3 = log_broadcast(&db, "sol", "three");
        // Ack the newest first (as a plugin that read a fresh batch would).
        let (_, out) = handle_read(
            &db,
            &argv_of(&["--name", "luna", "--ack", "--up-to", &e3.to_string()]),
        );
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(doc["acked_to"].as_i64(), Some(e3));
        // A stale ack for an older batch must not move the cursor backwards.
        let (_, out) = handle_read(
            &db,
            &argv_of(&["--name", "luna", "--ack", "--up-to", &e2.to_string()]),
        );
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(doc["noop"].as_bool(), Some(true));
        let inst = db.get_instance_full("luna").unwrap().unwrap();
        assert_eq!(inst.last_event_id, e3, "cursor never regresses");
        // Non-numeric cursor is a JSON error, not a panic.
        let (_, out) = handle_read(
            &db,
            &argv_of(&["--name", "luna", "--ack", "--up-to", "abc"]),
        );
        cleanup_db(&path);
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(doc.get("error").is_some());
    }

    #[test]
    fn read_format_cursor_stays_with_capped_batch() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "luna");
        let total = crate::shared::constants::MAX_MESSAGES_PER_DELIVERY + 3;
        let mut last_id = 0;
        for i in 0..total {
            last_id = log_broadcast(&db, "sol", &format!("m{i}"));
        }
        let (_, out) = handle_read(&db, &argv_of(&["--name", "luna", "--format"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        // The cursor points at the LAST DELIVERED message, not the newest
        // unread one — acknowledging it must leave the overflow intact.
        assert_eq!(doc["count"].as_i64(), Some(total as i64 - 3));
        assert!(
            doc["up_to"].as_i64().unwrap() < last_id,
            "batch cursor lags the newest event when the batch is capped"
        );
        // Acking the delivered batch then leaves exactly the overflow.
        let up_to = doc["up_to"].as_i64().unwrap();
        handle_read(
            &db,
            &argv_of(&["--name", "luna", "--ack", "--up-to", &up_to.to_string()]),
        );
        let remaining = db.get_unread_messages("luna").len();
        cleanup_db(&path);
        assert_eq!(remaining, 3);
    }

    #[test]
    fn ack_without_cursor_never_uses_global_max() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "luna");
        let hers = log_broadcast(&db, "sol", "for luna");
        // Advance the channel with a NON-message event luna must not consume;
        // a db.get_last_event_id() fallback would ack past it.
        db.log_event("status", "sol", &serde_json::json!({"status": "active"}))
            .unwrap();
        let (_, out) = handle_read(&db, &argv_of(&["--name", "luna", "--ack"]));
        cleanup_db(&path);
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(doc["acked"].as_i64(), Some(1));
        assert_eq!(
            doc["acked_to"].as_i64(),
            Some(hers),
            "cursor stays within luna's unread messages, not the global event max"
        );
    }

    // ── Plugin root / HERMES_HOME tests ──────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn plugin_root_honors_hermes_home_without_duplicating_hermes_dir() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        unsafe { std::env::set_var("HERMES_HOME", "/tmp/custom-hermes-home") };
        let root = hermes_plugin_root();
        unsafe { std::env::remove_var("HERMES_HOME") };
        // HERMES_HOME IS the hermes home: plugins sit directly inside it.
        assert_eq!(
            root,
            std::path::PathBuf::from("/tmp/custom-hermes-home/plugins"),
            "must not append another .hermes segment under HERMES_HOME"
        );
    }

    #[test]
    #[serial_test::serial]
    fn plugin_root_defaults_to_home_hermes_and_isolated_root() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let iso = temp.path().join("workspace").join(".sandbox");
        std::fs::create_dir_all(home.join(".hcom")).unwrap();
        std::fs::create_dir_all(iso.join(".hcom")).unwrap();

        // Default: HERMES_HOME unset, hcom dir under HOME → ~/.hermes/plugins.
        unsafe {
            std::env::remove_var("HERMES_HOME");
            std::env::set_var("HOME", &home);
            std::env::set_var("HCOM_DIR", home.join(".hcom"));
        }
        assert_eq!(hermes_plugin_root(), home.join(".hermes").join("plugins"));

        // Isolated workspace: hcom dir under a separate root → <root>/.hermes.
        unsafe { std::env::set_var("HCOM_DIR", iso.join(".hcom")) };
        let root = hermes_plugin_root();
        unsafe {
            std::env::set_var("HOME", home);
            std::env::remove_var("HCOM_DIR");
        }
        assert_eq!(root, iso.join(".hermes").join("plugins"));
    }

    // ── Embedded plugin source guards (pin behavior, like omp tests) ─────

    #[test]
    fn plugin_bootstrap_is_spent_by_pre_llm_not_session_start() {
        let start_idx = PLUGIN_INIT_SOURCE
            .find("def on_session_start")
            .expect("on_session_start present");
        let pre_idx = PLUGIN_INIT_SOURCE
            .find("def pre_llm_call")
            .expect("pre_llm_call present");
        assert!(
            !PLUGIN_INIT_SOURCE[start_idx..pre_idx].contains("_STATE[\"bootstrap\"] = None"),
            "on_session_start must not discard the bootstrap before it is injected"
        );
        let pre_body = &PLUGIN_INIT_SOURCE[pre_idx..];
        assert!(pre_body.contains("_STATE[\"bootstrap\"] = None"));
    }

    #[test]
    fn plugin_acks_only_the_delivered_batch_cursor() {
        // The only ack the plugin performs carries the explicit up_to cursor
        // reported by --format; a cursor-less ack would over-ack past the cap.
        assert!(PLUGIN_INIT_SOURCE.contains("\"--ack\","));
        assert!(PLUGIN_INIT_SOURCE.contains("\"--up-to\", str(up_to)"));
        assert!(PLUGIN_INIT_SOURCE.contains("fetched.get(\"up_to\")"));
    }

    #[test]
    fn plugin_uses_standard_logging_not_hermes_logging() {
        assert!(PLUGIN_INIT_SOURCE.contains("logging.getLogger(__name__)"));
        assert!(!PLUGIN_INIT_SOURCE.contains("hermes_logging"));
    }

    #[test]
    fn plugin_yaml_matches_hermes_manifest_schema() {
        // hermes_cli/plugin_validate requires name/version/description and
        // hook declarations under provides_hooks (a bare `hooks:` key is not
        // part of the 0.21 schema).
        assert!(PLUGIN_YAML_SOURCE.contains("provides_hooks:"));
        assert!(!PLUGIN_YAML_SOURCE.contains("\nhooks:"));
        for hook in [
            "on_session_start",
            "pre_llm_call",
            "post_llm_call",
            "post_tool_call",
            "on_session_end",
            "on_session_reset",
        ] {
            assert!(PLUGIN_YAML_SOURCE.contains(hook), "missing {hook}");
        }
        // requires_hermes is deliberately absent: upstream builds have
        // shipped broken version strings (the 2026.9.24 nix build reports
        // 0.0.0), which would fail any version gate while the code is
        // current. If the gate ever returns, pin its presence here again.
        assert!(!PLUGIN_YAML_SOURCE.contains("requires_hermes:"));
    }

    #[test]
    fn plugin_does_not_claim_instant_idle_delivery() {
        // The notify port is a readiness/wake signal only; promising instant
        // idle push would be false (pre_llm_call runs at turn assembly).
        assert!(!PLUGIN_INIT_SOURCE.contains("push"));
        assert!(!PLUGIN_YAML_SOURCE.contains("instant delivery"));
    }

    // ── Ordinary (plugin-enabled, not hcom-launched) session lifecycle ─────

    use crate::hooks::test_helpers::isolated_test_env;
    use serial_test::serial;
    use std::collections::HashMap;

    fn ordinary_hermes_ctx(cwd: &std::path::Path) -> HcomContext {
        // Empty env: no HCOM_PROCESS_ID, no HCOM_INSTANCE_NAME — exactly what
        // a normally started Hermes session carries.
        HcomContext::from_env(&HashMap::new(), cwd.to_path_buf())
    }

    #[test]
    #[serial]
    fn sessionstart_without_process_id_adopts_once() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let workspace = home.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let db = HcomDb::open().unwrap();
        let ctx = ordinary_hermes_ctx(&workspace);
        let argv = argv_of(&[
            "--session-id",
            "ses-ordinary",
            "--cwd",
            workspace.to_str().unwrap(),
        ]);

        let (code, out) = handle_start(&ctx, &db, &argv);
        assert_eq!(code, 0);
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(doc.get("error").is_none());
        let name = doc["name"].as_str().unwrap().to_string();
        assert!(!name.is_empty());

        let rows = db.iter_instances_full().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tool, "hermes");
        assert_eq!(rows[0].session_id.as_deref(), Some("ses-ordinary"));
        assert_eq!(rows[0].status, ST_LISTENING);
        assert_eq!(
            db.get_session_binding("ses-ordinary").unwrap().as_deref(),
            Some(name.as_str())
        );

        // Repeated hook (plugin retry / resume) recovers, never duplicates.
        let (_, out2) = handle_start(&ctx, &db, &argv);
        let doc2: Value = serde_json::from_str(&out2).unwrap();
        assert_eq!(doc2["name"].as_str(), Some(name.as_str()));
        assert_eq!(db.iter_instances_full().unwrap().len(), 1);

        // The bootstrap teaches the hook-delivery waiting model, not ad-hoc.
        let bootstrap_text = doc["bootstrap"].as_str().unwrap();
        assert!(bootstrap_text.contains("Messages do NOT arrive instantly"));
        assert!(bootstrap_text.contains("hcom listen"));
        assert!(!bootstrap_text.contains("Messages do NOT arrive automatically"));
    }

    #[test]
    #[serial]
    fn sessionstart_after_soft_stop_recovers_same_identity() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let workspace = home.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let db = HcomDb::open().unwrap();
        let ctx = ordinary_hermes_ctx(&workspace);
        let argv = argv_of(&[
            "--session-id",
            "ses-resume",
            "--cwd",
            workspace.to_str().unwrap(),
        ]);

        let (_, out) = handle_start(&ctx, &db, &argv);
        let name = serde_json::from_str::<Value>(&out).unwrap()["name"]
            .as_str()
            .unwrap()
            .to_string();

        // Session reset soft-finalizes: row stays, bindings drop.
        let (_, stop_out) = handle_stop(
            &db,
            &argv_of(&["--name", &name, "--soft", "--reason", "reset"]),
        );
        assert!(serde_json::from_str::<Value>(&stop_out).unwrap()["soft"].as_bool() == Some(true));
        assert!(db.get_session_binding("ses-resume").unwrap().is_none());

        // Resume with the same Hermes session id recovers THAT identity, not
        // a duplicate.
        let (_, out2) = handle_start(&ctx, &db, &argv);
        let doc2: Value = serde_json::from_str(&out2).unwrap();
        assert_eq!(doc2["name"].as_str(), Some(name.as_str()));
        let rows = db.iter_instances_full().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, ST_LISTENING);
        assert_eq!(
            db.get_session_binding("ses-resume").unwrap().as_deref(),
            Some(name.as_str())
        );
    }

    #[test]
    #[serial]
    fn dispatch_sessionstart_bypasses_gate_on_empty_db() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        // The ambient test process IS an hcom-launched session: strip the
        // launch env or from_os() would satisfy the gate for the wrong
        // reason. EnvGuard does not manage these; restore even on panic.
        struct LaunchEnvRestore(Vec<(String, Option<String>)>);
        impl Drop for LaunchEnvRestore {
            fn drop(&mut self) {
                for (key, value) in self.0.drain(..) {
                    match value {
                        Some(v) => unsafe { std::env::set_var(&key, v) },
                        None => unsafe { std::env::remove_var(&key) },
                    }
                }
            }
        }
        let _launch_env = LaunchEnvRestore(
            [
                "HCOM_PROCESS_ID",
                "HCOM_LAUNCHED",
                "HCOM_INSTANCE_NAME",
                "HCOM_LAUNCH_EVENT_ID",
            ]
            .iter()
            .map(|k| {
                let value = std::env::var(k).ok();
                unsafe { std::env::remove_var(k) };
                (k.to_string(), value)
            })
            .collect(),
        );
        let workspace = home.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let db = HcomDb::open().unwrap();
        assert!(
            !common::hook_gate_check(&HcomContext::from_os(), &db),
            "precondition: fresh DB, no launch env — gate closed"
        );

        // Only session-start may create identity: read stays gated while the
        // DB is empty (it must never adopt as a side effect).
        let read_argv = argv_of(&["hermes-read", "--name", "whoever", "--check"]);
        let (_, read_out) = dispatch_hermes_hook("hermes-read", &read_argv);
        assert!(read_out.is_empty(), "read must stay gated on an empty DB");

        // Session-start is the bootstrap exception: it answers with an
        // identity even though the gate sees no instances.
        let argv = argv_of(&[
            "hermes-sessionstart",
            "--session-id",
            "ses-fresh",
            "--cwd",
            workspace.to_str().unwrap(),
        ]);
        let (_, out) = dispatch_hermes_hook("hermes-sessionstart", &argv);
        let doc: Value =
            serde_json::from_str(&out).expect("session-start must answer past the gate");
        assert!(doc.get("error").is_none());
        assert!(doc["name"].as_str().is_some_and(|n| !n.is_empty()));

        // After adoption the mesh is actively used, so the gate is open and
        // read now answers with a JSON document.
        let (_, read_out) = dispatch_hermes_hook("hermes-read", &read_argv);
        let read_doc: Value = serde_json::from_str(&read_out).expect("read answers once active");
        assert!(read_doc.get("has_unread").is_some());
    }

    // ── Installer: enable/disable through the Hermes CLI ──────────────────

    fn write_fake_hermes_cli(dir: &std::path::Path, fail: bool) -> std::path::PathBuf {
        // A python3 stand-in for the `hermes` CLI: rewrites the enabled list
        // in $HERMES_HOME/config.yaml block-style, preserving sibling keys and
        // unrelated plugins — the ownership contract the real CLI carries.
        let script = if fail {
            "#!/usr/bin/env python3\nimport sys\nsys.stderr.write(\"boom: config locked\\n\")\nsys.exit(1)\n"
        } else {
            r#"#!/usr/bin/env python3
import os, re, sys

home = os.environ["HERMES_HOME"]
config = os.path.join(home, "config.yaml")
argv = sys.argv[1:]
mode, plugin = argv[1], argv[2]

# Mirrors Hermes 0.21.4 cmd_disable/_resolve_plugin_key: the CLI resolves the
# plugin key from the installed directory and fails unknown-plugin when it is
# gone. This pins the ordering contract — disable MUST run before removal.
manifest = os.path.join(home, "plugins", plugin, "plugin.yaml")
if not os.path.exists(manifest):
    sys.stderr.write(f"unknown plugin: {plugin}\n")
    sys.exit(1)

with open(config) as f:
    lines = f.read().splitlines()

# Locate the plugins: table and its enabled:/disabled: lists.
plugins_idx = next(i for i, l in enumerate(lines) if l.strip() == "plugins:")
def list_range(header):
    for i in range(plugins_idx + 1, len(lines)):
        if not lines[i].startswith((" ", "-")):
            break
        if lines[i].strip().startswith(header):
            j = i + 1
            while j < len(lines) and (lines[j].strip().startswith("- ") or lines[j].strip() == ""):
                j += 1
            return i, j
    return None

en_i, en_j = list_range("enabled:")
entries = [l.strip()[2:] for l in lines[en_i + 1:en_j] if l.strip()]
if mode == "enable" and plugin not in entries:
    entries.append(plugin)
if mode == "disable" and plugin in entries:
    entries.remove(plugin)
new_block = [f"  enabled:"] + [f"    - {e}" for e in sorted(entries)]
lines[en_i:en_j] = new_block

with open(config, "w") as f:
    f.write("\n".join(lines) + "\n")
"#
        };
        let path = dir.join(format!(
            "hermes-cli-{}.py",
            if fail { "fail" } else { "ok" }
        ));
        std::fs::write(&path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        path
    }

    fn write_slow_hermes_cli(dir: &std::path::Path) -> std::path::PathBuf {
        // Outlives the test's timeout override and never touches config.
        let script = "#!/usr/bin/env python3\nimport time\ntime.sleep(30)\n";
        let path = dir.join("hermes-cli-slow.py");
        std::fs::write(&path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        path
    }

    fn write_chatty_hermes_cli(dir: &std::path::Path) -> std::path::PathBuf {
        // Writes 256KB to BOTH stdout and stderr, then exits 0: with piped
        // output nobody drains, this would fill the ~64KB pipe buffer and
        // block forever — the deadlock the null-stdout + reader-thread
        // design prevents.
        let script = "#!/usr/bin/env python3\nimport sys\nblob = \"x\" * 65536\nfor _ in range(4):\n    sys.stdout.write(blob)\n    sys.stderr.write(blob)\n";
        let path = dir.join("hermes-cli-chatty.py");
        std::fs::write(&path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        path
    }

    fn write_lying_hermes_cli(dir: &std::path::Path) -> std::path::PathBuf {
        // Reports success (exit 0) without touching config.yaml — pins the
        // post-disable re-check: remove must not trust exit codes alone.
        let script = "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n";
        let path = dir.join("hermes-cli-lying.py");
        std::fs::write(&path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        path
    }

    fn install_plugin_files(home: &std::path::Path) {
        // The file half of install_hermes_plugin without the CLI enable —
        // the exact state a failed enable leaves behind.
        let dir = home.join("plugins").join("hcom");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("__init__.py"), PLUGIN_INIT_SOURCE).unwrap();
        std::fs::write(dir.join("plugin.yaml"), PLUGIN_YAML_SOURCE).unwrap();
    }

    fn seed_hermes_config(home: &std::path::Path) {
        std::fs::create_dir_all(home.join("plugins")).unwrap();
        std::fs::write(
            home.join("config.yaml"),
            "model: sonnet\nplugins:\n  disabled: []\n  enabled:\n    - current-time\n",
        )
        .unwrap();
    }

    fn config_text(home: &std::path::Path) -> String {
        std::fs::read_to_string(home.join("config.yaml")).unwrap()
    }

    #[test]
    #[serial]
    fn install_enables_via_cli_and_preserves_unrelated_config() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let cli = write_fake_hermes_cli(&home, false);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }

        assert!(!verify_hermes_plugin_installed());
        install_hermes_plugin().unwrap();
        assert!(verify_hermes_plugin_installed());
        let text = config_text(&home);
        assert!(text.contains("- hcom"), "hcom enabled in config");
        assert!(text.contains("- current-time"), "unrelated plugin kept");
        assert!(text.contains("model: sonnet"), "unrelated config kept");
        assert!(home.join("plugins/hcom/__init__.py").exists());
    }

    #[test]
    #[serial]
    fn remove_disables_before_removing_files_and_preserves_unrelated_plugins() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let cli = write_fake_hermes_cli(&home, false);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        install_hermes_plugin().unwrap();

        // The fake CLI fails unknown-plugin when the plugin directory is gone
        // (Hermes 0.21.4 _resolve_plugin_key): a files-first implementation
        // makes this unwrap fail, pinning disable-before-remove ordering.
        remove_hermes_plugin().unwrap();
        assert!(!home.join("plugins/hcom").exists());
        let text = config_text(&home);
        assert!(!text.contains("- hcom"), "hcom disabled in config");
        assert!(text.contains("- current-time"), "unrelated plugin kept");

        // Prove the pin has teeth: with the files gone (the state a files-
        // first removal would create), the CLI-side key resolution itself
        // fails — exactly the real-CLI behavior the ordering avoids.
        let err = run_hermes_plugins_cli(&["plugins", "disable", "hcom"])
            .unwrap_err()
            .message()
            .to_string();
        assert!(
            err.contains("unknown plugin"),
            "stub must fail key resolution without the manifest: {err}"
        );

        // Repeated removal is a clean no-op: swap in the fail stub — a CLI
        // invocation would now error — and remove must still succeed without
        // touching the config.
        let fail_cli = write_fake_hermes_cli(&home, true);
        unsafe { std::env::set_var("HCOM_TEST_HERMES_BIN", &fail_cli) };
        remove_hermes_plugin().unwrap();
        let text = config_text(&home);
        assert!(!text.contains("- hcom"), "still disabled");
        assert!(text.contains("- current-time"), "unrelated plugin kept");
    }

    #[test]
    #[serial]
    fn install_cli_failure_is_an_actionable_error() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        let err = install_hermes_plugin().unwrap_err().to_string();
        assert!(
            err.contains("plugin files installed at"),
            "says what happened: {err}"
        );
        assert!(
            err.contains("hermes plugins enable hcom"),
            "says the manual fix: {err}"
        );
        assert!(err.contains("boom"), "surfaces CLI stderr: {err}");
        assert!(!hermes_plugin_enabled_in_config());
    }

    #[test]
    #[serial]
    fn install_missing_cli_is_an_actionable_error() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", home.join("no-such-hermes"));
        }
        let err = install_hermes_plugin().unwrap_err().to_string();
        assert!(
            err.contains("is the hermes CLI on PATH?"),
            "says the likely cause: {err}"
        );
    }

    #[test]
    fn enabled_list_parsing_ignores_sibling_keys() {
        // A following disabled: block at the same indent must terminate the
        // enabled list — otherwise disabled entries count as enabled.
        let content = "plugins:\n  enabled:\n    - hcom\n  disabled:\n    - rogue\n";
        assert_eq!(
            parse_hermes_enabled_plugins(content),
            Some(vec!["hcom".to_string()])
        );
        // Flow style + absent table parse sanely.
        assert_eq!(
            parse_hermes_enabled_plugins("plugins:\n  enabled: [hcom, current-time]\n"),
            Some(vec!["hcom".to_string(), "current-time".to_string()])
        );
        assert_eq!(parse_hermes_enabled_plugins("model: sonnet\n"), None);
    }

    #[test]
    fn enabled_list_parsing_accepts_same_indent_and_quoted_entries() {
        // PyYAML's default block style puts sequence items at the SAME indent
        // as their key; sibling lists (disabled:) at that indent still close
        // the enabled list.
        let same_indent =
            "plugins:\n  enabled:\n  - current-time\n  - hcom\n  disabled:\n  - rogue\n";
        assert_eq!(
            parse_hermes_enabled_plugins(same_indent),
            Some(vec!["current-time".to_string(), "hcom".to_string()])
        );
        // Quoted block items normalize to bare names.
        let quoted_block = "plugins:\n  enabled:\n    - 'hcom'\n    - \"current-time\"\n";
        assert_eq!(
            parse_hermes_enabled_plugins(quoted_block),
            Some(vec!["hcom".to_string(), "current-time".to_string()])
        );
        // Quoted flow items too.
        let quoted_flow = "plugins:\n  enabled: ['hcom', \"current-time\"]\n";
        assert_eq!(
            parse_hermes_enabled_plugins(quoted_flow),
            Some(vec!["hcom".to_string(), "current-time".to_string()])
        );
    }

    #[test]
    #[serial]
    fn remove_is_noop_when_never_installed() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        // No hermes home at all (no config.yaml, no plugins dir) and a CLI
        // stub that FAILS if ever invoked — remove must not shell out.
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::remove_var("HERMES_HOME");
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        remove_hermes_plugin().unwrap();
        assert!(!home.join(".hermes/config.yaml").exists());
        // Repeated removal stays a clean no-op.
        remove_hermes_plugin().unwrap();
        assert!(!home.join(".hermes/config.yaml").exists());
    }

    #[test]
    #[serial]
    fn remove_cleans_stale_enabled_entry_without_files() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        // Config says enabled, but the plugin files are gone; the fail stub
        // proves the CLI is never invoked (it cannot resolve a missing dir).
        let config = home.join("config.yaml");
        std::fs::write(
            &config,
            "model: sonnet\nplugins:\n  disabled: []\n  enabled:\n    - current-time\n    - hcom\n",
        )
        .unwrap();
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        remove_hermes_plugin().unwrap();
        let text = std::fs::read_to_string(&config).unwrap();
        assert!(!text.contains("hcom"), "stale entry removed: {text}");
        assert!(text.contains("- current-time"), "unrelated plugin kept");
        assert!(text.contains("model: sonnet"), "unrelated config kept");

        // An emptied list collapses to `enabled: []` (still a valid list).
        std::fs::write(&config, "plugins:\n  enabled:\n    - hcom\n").unwrap();
        remove_hermes_plugin().unwrap();
        let text = std::fs::read_to_string(&config).unwrap();
        assert_eq!(text, "plugins:\n  enabled: []\n");
    }

    #[test]
    #[serial]
    fn remove_cleans_stale_flow_entry_without_files() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let config = home.join("config.yaml");
        // Flow-style enabled list (the parser recognizes it, so the cleaner
        // must too); files absent and the fail stub proves the CLI is never
        // invoked.
        std::fs::write(
            &config,
            "model: sonnet\nplugins:\n  enabled: [current-time, hcom]\n",
        )
        .unwrap();
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        remove_hermes_plugin().unwrap();
        let text = std::fs::read_to_string(&config).unwrap();
        assert!(!text.contains("hcom"), "stale flow entry removed: {text}");
        assert!(text.contains("current-time"), "unrelated flow entry kept");
        assert!(text.contains("model: sonnet"), "unrelated config kept");

        // Quoted flow entries normalize; a list left empty collapses to [].
        std::fs::write(&config, "plugins:\n  enabled: ['hcom']\n").unwrap();
        remove_hermes_plugin().unwrap();
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "plugins:\n  enabled: []\n"
        );
    }

    #[test]
    fn scan_flow_edge_cases_never_guess() {
        // A comma inside a quoted scalar is ONE item, not two.
        assert_eq!(
            parse_hermes_enabled_plugins("plugins:\n  enabled: ['a,b', hcom]\n"),
            Some(vec!["a,b".to_string(), "hcom".to_string()])
        );
        // Multi-line flow lists join until the closing bracket.
        assert_eq!(
            parse_hermes_enabled_plugins(
                "plugins:\n  enabled: [\n    hcom,\n    current-time\n  ]\n"
            ),
            Some(vec!["hcom".to_string(), "current-time".to_string()])
        );
        // Unclosed flow and junk after the closing bracket are Unresolved —
        // never a false "absent".
        assert_eq!(
            scan_hermes_enabled_plugins("plugins:\n  enabled: [hcom\n"),
            EnabledScan::Unresolved
        );
        assert_eq!(
            scan_hermes_enabled_plugins("plugins:\n  enabled: [hcom]\nother: 1\n"),
            EnabledScan::Parsed(vec!["hcom".to_string()])
        );
        assert_eq!(
            scan_hermes_enabled_plugins("plugins:\n  enabled: ['hcom\n"),
            EnabledScan::Unresolved
        );
        // Scan-level behavior of a clean config stays NoList.
        assert_eq!(
            scan_hermes_enabled_plugins("model: sonnet\n"),
            EnabledScan::NoList
        );
    }

    #[test]
    #[serial]
    fn remove_cleans_multiline_flow_entry() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let config = home.join("config.yaml");
        std::fs::write(
            &config,
            "model: sonnet\nplugins:\n  enabled: [\n    current-time,\n    hcom\n  ]\n",
        )
        .unwrap();
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        remove_hermes_plugin().unwrap();
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "model: sonnet\nplugins:\n  enabled: [current-time]\n"
        );
    }

    #[test]
    #[serial]
    fn remove_refuses_ambiguous_flow_and_preserves_config() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let config = home.join("config.yaml");
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        // Remaining entries that cannot be re-emitted losslessly: refuse,
        // never rewrite (a naive rewrite would corrupt `a,b` into `a`/`b`).
        let ambiguous = "plugins:\n  enabled: ['a,b', hcom]\n";
        std::fs::write(&config, ambiguous).unwrap();
        let err = remove_hermes_plugin().unwrap_err().to_string();
        assert!(
            err.contains("cannot safely rewrite")
                || err.contains("refus")
                || err.contains("cannot tell"),
            "refusal is explained: {err}"
        );
        assert!(
            err.contains("hermes plugins disable hcom"),
            "actionable fix: {err}"
        );
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            ambiguous,
            "the ambiguous config is untouched"
        );

        // Unclosed flow: the enabled state is unknowable — remove errors
        // instead of claiming a clean removal.
        std::fs::write(&config, "plugins:\n  enabled: [hcom\n").unwrap();
        let err = remove_hermes_plugin().unwrap_err().to_string();
        assert!(err.contains("cannot tell"), "what: {err}");
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "plugins:\n  enabled: [hcom\n",
            "still untouched"
        );
    }

    #[test]
    #[serial]
    fn remove_deletes_files_when_enable_never_succeeded() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        install_plugin_files(&home);
        // The failed-enable half-state: files exist, config has no hcom. The
        // CLI bin does not exist — removing these files must not need it.
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", home.join("no-such-hermes"));
        }
        let before = std::fs::read_to_string(home.join("config.yaml")).unwrap();
        remove_hermes_plugin().unwrap();
        assert!(!home.join("plugins/hcom").exists(), "files removed");
        assert_eq!(
            std::fs::read_to_string(home.join("config.yaml")).unwrap(),
            before,
            "config never had hcom — untouched"
        );
    }

    #[test]
    #[serial]
    fn remove_without_cli_cleans_config_and_files() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        install_plugin_files(&home);
        let config = home.join("config.yaml");
        std::fs::write(
            &config,
            "model: sonnet\nplugins:\n  enabled:\n    - current-time\n    - hcom\n",
        )
        .unwrap();
        // Enabled + files present, but no hermes CLI: fall back to the
        // surgical config clean, then remove the files.
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", home.join("no-such-hermes"));
        }
        remove_hermes_plugin().unwrap();
        assert!(!home.join("plugins/hcom").exists(), "files removed");
        let text = std::fs::read_to_string(&config).unwrap();
        assert!(!text.contains("hcom"), "enabled entry cleaned: {text}");
        assert!(text.contains("- current-time"), "unrelated plugin kept");
        assert!(text.contains("model: sonnet"), "unrelated config kept");
    }

    #[test]
    fn scan_inline_comment_shapes() {
        // A trailing comment on a block item is not part of the name: this is
        // valid YAML enabling hcom, and misreading it as `hcom  # note` would
        // report NotEnabled while hcom runs.
        assert_eq!(
            parse_hermes_enabled_plugins("plugins:\n  enabled:\n    - hcom  # note\n"),
            Some(vec!["hcom".to_string()])
        );
        assert_eq!(
            parse_hermes_enabled_plugins(
                "plugins:\n  enabled:\n    - current-time  # c\n    - hcom\n"
            ),
            Some(vec!["current-time".to_string(), "hcom".to_string()])
        );
        // A comment-bearing key still opens a block list.
        assert_eq!(
            parse_hermes_enabled_plugins("plugins:\n  enabled: # note\n    - hcom\n"),
            Some(vec!["hcom".to_string()])
        );
        // Comment shapes this reader cannot parse confidently are Unresolved,
        // never a guessed name and never a false "absent": an unquoted # in a
        // flow list, and a comment where the item name should be.
        assert_eq!(
            scan_hermes_enabled_plugins("plugins:\n  enabled: [hcom # c]\n"),
            EnabledScan::Unresolved
        );
        assert_eq!(
            scan_hermes_enabled_plugins("plugins:\n  enabled:\n    - # note\n"),
            EnabledScan::Unresolved
        );
    }

    #[test]
    #[serial]
    fn remove_drops_commented_block_item() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let config = home.join("config.yaml");
        std::fs::write(
            &config,
            "model: sonnet\nplugins:\n  enabled:\n    - hcom  # note\n    - current-time  # keep\n",
        )
        .unwrap();
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
        }
        // Files absent + hcom enabled (comment notwithstanding) → surgical
        // clean drops the hcom line whole, comment included; the sibling's
        // comment survives byte-identical.
        remove_hermes_plugin().unwrap();
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "model: sonnet\nplugins:\n  enabled:\n    - current-time  # keep\n"
        );
    }

    #[test]
    #[serial]
    fn remove_refuses_losing_key_comment() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let config = home.join("config.yaml");
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
        }
        // hcom was the only item: collapsing to `enabled: []` would silently
        // drop the key's comment — refuse and leave the config untouched.
        let commented_key = "plugins:\n  enabled: # note\n    - hcom\n";
        std::fs::write(&config, commented_key).unwrap();
        let err = remove_hermes_plugin().unwrap_err().to_string();
        assert!(
            err.contains("comment on the enabled: key would be lost"),
            "refusal is explained: {err}"
        );
        assert!(
            err.contains("hermes plugins disable hcom"),
            "actionable fix: {err}"
        );
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            commented_key,
            "the commented key config is untouched"
        );

        // With another plugin left in the list the key line is kept as-is,
        // comment and all — only the hcom item goes.
        std::fs::write(
            &config,
            "plugins:\n  enabled: # note\n    - hcom\n    - current-time\n",
        )
        .unwrap();
        remove_hermes_plugin().unwrap();
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "plugins:\n  enabled: # note\n    - current-time\n"
        );
    }

    #[test]
    #[serial]
    fn remove_rechecks_state_after_cli_reports_success() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        install_plugin_files(&home);
        let config = home.join("config.yaml");
        std::fs::write(
            &config,
            "model: sonnet\nplugins:\n  enabled:\n    - current-time\n    - hcom\n",
        )
        .unwrap();
        // The CLI exits 0 without changing config.yaml: without the post-
        // disable re-check, remove would delete the files and report success
        // with hcom still enabled.
        let cli = write_lying_hermes_cli(&home);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        remove_hermes_plugin().unwrap();
        assert!(!home.join("plugins/hcom").exists(), "files removed");
        let text = std::fs::read_to_string(&config).unwrap();
        assert!(!text.contains("hcom"), "hcom actually disabled: {text}");
        assert!(text.contains("- current-time"), "unrelated plugin kept");
    }

    #[test]
    #[serial]
    fn remove_refuses_when_config_unreadable() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let config = home.join("config.yaml");
        std::fs::write(&config, "plugins:\n  enabled:\n    - hcom\n").unwrap();
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o000)).unwrap();
            // A read the state check cannot perform (EACCES here) is
            // Unresolved, not NoList: remove must refuse, never report
            // success over a config it could not read.
            let err = remove_hermes_plugin().unwrap_err().to_string();
            std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(
                err.contains("cannot tell whether hcom is enabled"),
                "refuses on unreadable config: {err}"
            );
            assert_eq!(
                std::fs::read_to_string(&config).unwrap(),
                "plugins:\n  enabled:\n    - hcom\n",
                "still untouched"
            );
        }
    }

    #[test]
    #[serial]
    fn cli_timeout_kills_and_reports() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let cli = write_slow_hermes_cli(&home);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
            std::env::set_var("HCOM_TEST_HERMES_CLI_TIMEOUT_MS", "300");
        }
        let start = std::time::Instant::now();
        let err = run_hermes_plugins_cli(&["plugins", "enable", "hcom"])
            .unwrap_err()
            .message()
            .to_string();
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "the override applies: returned in {:?}",
            start.elapsed()
        );
        assert!(
            err.contains("did not finish within"),
            "timeout is the reported failure, not a killed-exit masquerade: {err}"
        );
        assert!(err.contains("killed"), "says the CLI was killed: {err}");
    }

    #[test]
    #[serial]
    fn cli_chatty_output_does_not_deadlock() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let cli = write_chatty_hermes_cli(&home);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
            std::env::set_var("HCOM_TEST_HERMES_CLI_TIMEOUT_MS", "10000");
        }
        // 256KB on both streams: with piped-but-undrained output the child
        // would fill the pipe buffer, block, and get timeout-killed.
        run_hermes_plugins_cli(&["plugins", "enable", "hcom"])
            .expect("chatty but successful CLI completes");
    }

    #[test]
    #[serial]
    fn remove_stale_entry_preserves_newline_style_and_trailing_newline() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let config = home.join("config.yaml");
        std::fs::create_dir_all(home.join("plugins")).unwrap();
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        // No trailing newline: the rewrite must not add one.
        std::fs::write(&config, "model: sonnet\nplugins:\n  enabled:\n    - hcom").unwrap();
        remove_hermes_plugin().unwrap();
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "model: sonnet\nplugins:\n  enabled: []"
        );
        // CRLF: line endings survive the rewrite byte-for-byte.
        std::fs::write(&config, "plugins:\r\n  enabled:\r\n    - hcom\r\n").unwrap();
        remove_hermes_plugin().unwrap();
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "plugins:\r\n  enabled: []\r\n"
        );
        // Flow + CRLF, sibling entries preserved.
        std::fs::write(&config, "plugins:\r\n  enabled: [hcom, k9s]\r\n").unwrap();
        remove_hermes_plugin().unwrap();
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "plugins:\r\n  enabled: [k9s]\r\n"
        );
    }

    #[test]
    #[serial]
    #[cfg(unix)]
    fn remove_stale_entry_write_failure_is_an_error() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let config = home.join("config.yaml");
        std::fs::write(&config, "plugins:\n  enabled: [hcom]\n").unwrap();
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        // The write is atomic (temp file + rename), so a read-only FILE no
        // longer blocks it — a read-only DIRECTORY does, by making temp
        // creation impossible. Remove must say so instead of returning Ok
        // with hcom still enabled.
        use std::os::unix::fs::PermissionsExt;
        let original = std::fs::metadata(&home).unwrap().permissions();
        std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o555)).unwrap();
        let result = remove_hermes_plugin();
        std::fs::set_permissions(&home, original).unwrap();
        let err = result.unwrap_err().to_string();
        assert!(err.contains("stale hcom enabled entry"), "what: {err}");
        assert!(err.contains("config.yaml"), "where: {err}");
        assert!(
            std::fs::read_to_string(&config).unwrap().contains("hcom"),
            "hcom really is still enabled — that is why remove had to fail"
        );
        // No temp litter left beside the config.
        let leftovers: Vec<_> = std::fs::read_dir(&home)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().contains("tmp"))
            .collect();
        assert!(leftovers.is_empty(), "no temp files left: {leftovers:?}");
    }

    #[test]
    #[serial]
    fn remove_stale_entry_preserves_config_permissions() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let config = home.join("config.yaml");
        std::fs::create_dir_all(home.join("plugins")).unwrap();
        std::fs::write(&config, "plugins:\n  enabled: [hcom]\n").unwrap();
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o640)).unwrap();
            remove_hermes_plugin().unwrap();
            let mode = std::fs::metadata(&config).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o640, "the rewritten config keeps its mode");
        }
        #[cfg(not(unix))]
        {
            remove_hermes_plugin().unwrap();
        }
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "plugins:\n  enabled: []\n"
        );
    }

    #[test]
    #[serial]
    fn isolated_root_cli_and_verification_share_home() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        // Isolated workspace: HCOM_DIR under a root distinct from HOME, no
        // ambient HERMES_HOME — the CLI must be pointed at the isolated
        // .hermes explicitly or it would enable into the HOME-side config
        // while verification reads the isolated one.
        let iso = home.join("workspace").join(".sandbox");
        std::fs::create_dir_all(iso.join(".hcom")).unwrap();
        seed_hermes_config(&iso.join(".hermes"));
        let cli = write_fake_hermes_cli(&home, false);
        unsafe {
            std::env::remove_var("HERMES_HOME");
            std::env::set_var("HCOM_DIR", iso.join(".hcom"));
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }

        install_hermes_plugin().unwrap();
        assert!(verify_hermes_plugin_installed());
        assert!(iso.join(".hermes/plugins/hcom/__init__.py").exists());
        let text = std::fs::read_to_string(iso.join(".hermes/config.yaml")).unwrap();
        assert!(text.contains("- hcom"), "enabled in the isolated config");
        assert!(text.contains("- current-time"), "unrelated plugin kept");
        assert!(
            !home.join(".hermes/config.yaml").exists(),
            "must not touch the HOME-side hermes config"
        );
    }

    #[test]
    #[serial]
    fn ensure_surfaces_underlying_cli_error() {
        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        seed_hermes_config(&home);
        let cli = write_fake_hermes_cli(&home, true);
        unsafe {
            std::env::set_var("HERMES_HOME", &home);
            std::env::set_var("HCOM_TEST_HERMES_BIN", &cli);
        }
        let err = ensure_hermes_plugin_installed().unwrap_err();
        assert!(err.contains("plugin files installed at"), "what: {err}");
        assert!(err.contains("hermes plugins enable hcom"), "fix: {err}");
        assert!(err.contains("boom"), "underlying CLI stderr: {err}");
    }
    #[test]
    fn recovered_cursor_survives_rebind_after_hard_stop() {
        // Regression: a hard stop deletes the instance row but snapshots
        // last_event_id; the rebind must carry that cursor forward so
        // messages that arrived during the stop window are delivered, not
        // silently skipped by a seed-to-current-max.
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "mira");
        // Delivered+acked up to event 10 before the stop.
        let mut updates = serde_json::Map::new();
        updates.insert("last_event_id".into(), serde_json::json!(10));
        crate::instances::update_instance_position(&db, "mira", &updates);
        // Message arrives at 11 while down, then the row is hard-stopped
        // (finalize path: snapshot with cursor 10, row deleted).
        // A fresh test DB starts its event ids at 1, so seed enough prior
        // events that the stop-window message lands beyond cursor 10.
        while db.get_last_event_id() < 10 {
            log_broadcast(&db, "bigboss", "filler");
        }
        let msg_id = log_broadcast(&db, "bigboss", "arrived while stopped");
        assert!(
            msg_id > 10,
            "test setup: message must land after cursor (id={msg_id})"
        );
        db.log_life_event(
            "mira",
            "stopped",
            "session",
            "exit:session_end",
            Some(serde_json::json!({
                "name": "mira",
                "session_id": "s-1",
                "tool": "hermes",
                "last_event_id": 10,
            })),
        )
        .unwrap();
        // Rebind recreates the row with last_event_id 0 (fresh save).
        save_hermes_instance(&db, "mira");
        // Tests may run in an environment carrying HCOM_LAUNCH_EVENT_ID;
        // clear it so the launch-env branch cannot mask the snapshot path.
        #[allow(unused_unsafe)]
        unsafe {
            std::env::remove_var("HCOM_LAUNCH_EVENT_ID");
        }
        initialize_last_event_id(&db, "mira");
        let row = db.get_instance_full("mira").ok().flatten().unwrap();
        assert_eq!(
            row.last_event_id, 10,
            "rebind must carry the snapshot cursor, not seed to current max"
        );
        // And the stop-window message is still unread.
        let unread = db.get_unread_messages("mira");
        assert!(
            unread
                .iter()
                .any(|m| m.text.contains("arrived while stopped")),
            "stop-window message must survive the rebind"
        );
        cleanup_db(&path);
    }
}
