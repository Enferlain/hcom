//! Hermes Agent hook handlers — argv-based lifecycle plus Python plugin.
//!
//! Hermes-side plugins may use the argv-based RPC subcommands dispatched
//! here. Their installation and lifecycle are owned outside hcom:
//!
//! - `hermes-sessionstart` — bind session→process, set listening, return
//!   `{name, session_id, bootstrap}`.
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

use std::time::Instant;

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
        let new_id = match launch_event_id {
            Some(lei) if lei <= current_max => lei,
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

/// Ordinary Hermes sessions have no hcom launch placeholder. Only this
/// Hermes-specific start RPC may create an identity for one.
fn adopt_ordinary_hermes_instance(
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
        log_warn("hooks", "hermes.adopt.lock_dir_failed", &error.to_string());
        return None;
    }
    let lock_file = std::fs::File::options()
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_dir.join("hermes_adopt.lock"))
        .ok()?;
    if let Err(error) = crate::sys::fs::lock_exclusive(&lock_file) {
        log_warn("hooks", "hermes.adopt.lock_failed", &error.to_string());
        return None;
    }
    // Do not use the shared stopped-snapshot restore path here: without a
    // launch placeholder it can return a name whose instance row is gone.
    if let Ok(Some(name)) = db.get_session_binding(session_id) {
        match db.get_instance_full(&name).ok().flatten() {
            Some(row) if row.tool == "hermes" => return Some(name),
            // A session ID already owned by another tool is not ours to steal.
            Some(_) => return None,
            None => {}
        }
    }
    // A soft reset retains the row but drops the binding. Recover it locally
    // without changing the shared bind_session_to_process behavior.
    if let Ok(Some(name)) = db.find_stopped_instance_by_session_id(session_id)
        && db
            .get_instance_full(&name)
            .ok()
            .flatten()
            .is_some_and(|row| row.tool == "hermes")
        && db.rebind_instance_session(&name, session_id).is_ok()
    {
        return Some(name);
    }
    let name = crate::instance_names::generate_unique_name(db).ok()?;
    if let Err(error) = db.conn().execute_batch("BEGIN IMMEDIATE") {
        let _ = db.delete_instance(&name);
        log_error(
            "hooks",
            "hermes.adopt.transaction_failed",
            &error.to_string(),
        );
        return None;
    }
    let created = (|| -> anyhow::Result<()> {
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
        db.conn().execute_batch("COMMIT")?;
        Ok(())
    })();
    if let Err(error) = created {
        let _ = db.conn().execute_batch("ROLLBACK");
        let _ = db.delete_session_bindings_for_instance(&name);
        let _ = db.delete_instance(&name);
        log_error("hooks", "hermes.adopt.failed", &error.to_string());
        return None;
    }
    common::notify_hook_instance_with_db(db, &name);
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

    let instance_name = if let Some(process_id) = ctx.process_id.as_deref() {
        instance_binding::bind_session_to_process(db, &session_id, Some(process_id)).or_else(|| {
            instance_name_from_env(ctx).and_then(|name| {
                instance_binding::recover_process_binding_for_instance(
                    db,
                    &name,
                    &session_id,
                    process_id,
                )
            })
        })
    } else {
        let existing = db.get_session_binding(&session_id).ok().flatten();
        existing
            .filter(|name| {
                db.get_instance_full(name)
                    .ok()
                    .flatten()
                    .is_some_and(|row| row.tool == "hermes")
            })
            .or_else(|| {
                adopt_ordinary_hermes_instance(
                    db,
                    &session_id,
                    transcript_path.as_deref(),
                    cwd.as_deref().or_else(|| ctx.cwd.to_str()),
                )
            })
    };
    let Some(instance_name) = instance_name else {
        return (
            0,
            r#"{"error":"No Hermes instance could be bound"}"#.to_string(),
        );
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
    let cwd_value = cwd
        .as_deref()
        .filter(|p| !p.is_empty())
        .or_else(|| ctx.cwd.to_str());
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
    if !common::hook_gate_check(&ctx, &db) && hook_name != "hermes-sessionstart" {
        return (0, String::new());
    }
    let handler_argv: Vec<String> = if !argv.is_empty() && argv[0] == hook_name {
        argv[1..].to_vec()
    } else {
        argv.to_vec()
    };
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

// ── External Hermes plugin discovery ─────────────────────────────────────

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

pub fn verify_hermes_plugin_installed() -> bool {
    let dir = hermes_plugin_root().join(PLUGIN_DIR_NAME);
    dir.join("__init__.py").is_file() && dir.join("plugin.yaml").is_file()
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

    #[test]
    #[serial_test::serial]
    fn ordinary_session_binds_idempotently_without_changing_shared_binding() {
        use crate::hooks::test_helpers::isolated_test_env;
        use std::collections::HashMap;

        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let workspace = home.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let db = HcomDb::open().unwrap();
        let ctx = HcomContext::from_env(&HashMap::new(), workspace.clone());
        let argv = argv_of(&[
            "--session-id",
            "ses-ordinary",
            "--cwd",
            workspace.to_str().unwrap(),
        ]);

        let (_, first) = handle_start(&ctx, &db, &argv);
        let first: Value = serde_json::from_str(&first).unwrap();
        let name = first["name"].as_str().expect("ordinary session bound");
        assert_eq!(db.get_instance_full(name).unwrap().unwrap().tool, "hermes");
        assert_eq!(
            db.get_session_binding("ses-ordinary").unwrap().as_deref(),
            Some(name)
        );

        let (_, second) = handle_start(&ctx, &db, &argv);
        let second: Value = serde_json::from_str(&second).unwrap();
        assert_eq!(second["name"].as_str(), Some(name));
        assert_eq!(db.iter_instances_full().unwrap().len(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn ordinary_session_soft_reset_reuses_live_row() {
        use crate::hooks::test_helpers::isolated_test_env;
        use std::collections::HashMap;

        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = HcomContext::from_env(&HashMap::new(), home);
        let argv = argv_of(&["--session-id", "ses-reset"]);
        let (_, first) = handle_start(&ctx, &db, &argv);
        let first: Value = serde_json::from_str(&first).unwrap();
        let name = first["name"].as_str().expect("ordinary session bound");
        let (_, stopped) = handle_stop(&db, &argv_of(&["--name", name, "--soft"]));
        let stopped: Value = serde_json::from_str(&stopped).unwrap();
        assert_eq!(stopped["soft"].as_bool(), Some(true));

        let (_, resumed) = handle_start(&ctx, &db, &argv);
        let resumed: Value = serde_json::from_str(&resumed).unwrap();
        assert_eq!(resumed["name"].as_str(), Some(name));
        assert_eq!(db.iter_instances_full().unwrap().len(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn ordinary_session_does_not_steal_another_tools_binding() {
        use crate::hooks::test_helpers::isolated_test_env;

        let (_tmp, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        assert!(instance_binding::initialize_instance_in_position_file(
            &db,
            "other",
            Some("shared-session"),
            None,
            None,
            None,
            None,
            Some("claude"),
            false,
            None,
            None,
            None,
            None,
            None,
        ));
        db.rebind_instance_session("other", "shared-session")
            .unwrap();

        assert!(adopt_ordinary_hermes_instance(&db, "shared-session", None, None).is_none());
        assert_eq!(
            db.get_session_binding("shared-session").unwrap().as_deref(),
            Some("other")
        );
        assert_eq!(db.iter_instances_full().unwrap().len(), 1);
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
}
