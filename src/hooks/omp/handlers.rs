//! Omp Coding Agent hook handlers — argv-based lifecycle plus TypeScript plugin.

use std::time::Instant;

use serde_json::Value;

use crate::bootstrap;
use crate::db::HcomDb;
use crate::instance_binding;
use crate::instance_lifecycle as lifecycle;
use crate::instances;
use crate::log::{log_error, log_info};
use crate::shared::ST_LISTENING;
use crate::shared::context::HcomContext;

use crate::hooks::common;
use crate::hooks::common::finalize_session;

fn parse_flag(argv: &[String], flag: &str) -> Option<String> {
    argv.iter()
        .position(|a| a == flag)
        .and_then(|i| argv.get(i + 1))
        .cloned()
}

fn has_flag(argv: &[String], flag: &str) -> bool {
    argv.iter().any(|a| a == flag)
}

pub(crate) fn upsert_plugin_notify_endpoint(db: &HcomDb, instance_name: &str, port: u16) {
    if let Err(e) = db.upsert_notify_endpoint(instance_name, "plugin", port) {
        log_error(
            "native",
            "omp.register_notify_fail",
            &format!(
                "Failed to register plugin notify port for {}: {}",
                instance_name, e
            ),
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
        if new_id > 0
            && let Err(e) = db.advance_instance_cursor(instance_name, new_id)
        {
            log_error(
                "hooks",
                "omp.init_cursor",
                &format!("instance={instance_name} err={e}"),
            );
        }
    }
}

fn instance_name_from_env(ctx: &HcomContext) -> Option<String> {
    ctx.raw_env
        .get("HCOM_INSTANCE_NAME")
        .filter(|s| !s.is_empty())
        .cloned()
}

fn bootstrap_for(ctx: &HcomContext, db: &HcomDb, instance_name: &str) -> String {
    bootstrap::get_bootstrap(db, ctx, instance_name, "omp")
}

pub(crate) fn handle_start(ctx: &HcomContext, db: &HcomDb, argv: &[String]) -> (i32, String) {
    // Plugin RPC returns JSON errors on exit 0 so the extension can handle
    // setup failures without Pi treating the hook itself as failed.
    let session_id = match parse_flag(argv, "--session-id") {
        Some(sid) => sid,
        None => return (0, r#"{"error":"Missing --session-id"}"#.to_string()),
    };
    let transcript_path = parse_flag(argv, "--transcript-path");
    let cwd = parse_flag(argv, "--cwd");
    let notify_port: Option<u16> = parse_flag(argv, "--notify-port").and_then(|s| s.parse().ok());

    let process_id = match &ctx.process_id {
        Some(pid) => pid.clone(),
        None => return (0, r#"{"error":"HCOM_PROCESS_ID not set"}"#.to_string()),
    };

    let instance_name =
        match instance_binding::bind_session_to_process(db, &session_id, Some(&process_id)) {
            Some(name) => name,
            None => match instance_name_from_env(ctx).and_then(|name| {
                instance_binding::recover_process_binding_for_instance(
                    db,
                    &name,
                    &session_id,
                    &process_id,
                )
            }) {
                Some(name) => name,
                None => {
                    return (
                        0,
                        r#"{"error":"No instance bound to this process"}"#.to_string(),
                    );
                }
            },
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
    updates.insert("tool".into(), serde_json::json!("omp"));
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
        "omp-start.bind",
        &format!("instance={} session_id={}", instance_name, session_id),
    );
    crate::relay::worker::try_spawn_worker();

    let response = serde_json::json!({
        "name": instance_name,
        "session_id": session_id,
        "bootstrap": bootstrap_for(ctx, db, &instance_name),
    });
    (0, response.to_string())
}

pub(crate) fn handle_status(db: &HcomDb, argv: &[String]) -> (i32, String) {
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
        if messages.is_empty() {
            return (0, String::new());
        }
        let deliver = common::limit_delivery_messages(&messages);
        return (
            0,
            common::format_messages_json_for_instance(db, &deliver, &name),
        );
    }
    if ack_mode {
        if let Some(up_to) = parse_flag(argv, "--up-to") {
            let Ok(ack_id) = up_to.parse::<i64>() else {
                return (
                    0,
                    serde_json::json!({"error": format!("Invalid --up-to: {}", up_to)}).to_string(),
                );
            };
            if ack_id < 0 {
                return (
                    0,
                    serde_json::json!({"error": format!("Invalid --up-to: {}", up_to)}).to_string(),
                );
            }
            if let Err(e) = db.advance_instance_cursor(&name, ack_id) {
                log_error(
                    "hooks",
                    "omp.ack_cursor",
                    &format!("instance={name} err={e}"),
                );
                return (
                    0,
                    serde_json::json!({"error": format!("Failed to advance cursor: {e}")})
                        .to_string(),
                );
            }
            return (0, serde_json::json!({"acked_to": ack_id}).to_string());
        }
        if messages.is_empty() {
            return (0, r#"{"acked":0}"#.to_string());
        }
        let ack_id = messages
            .iter()
            .filter_map(|m| m.get("event_id").and_then(|v| v.as_i64()))
            .max()
            .filter(|id| *id > 0)
            .unwrap_or_else(|| db.get_last_event_id());
        if ack_id > 0
            && let Err(e) = db.advance_instance_cursor(&name, ack_id)
        {
            log_error(
                "hooks",
                "omp.ack_cursor",
                &format!("instance={name} err={e}"),
            );
            return (
                0,
                serde_json::json!({"error": format!("Failed to advance cursor: {e}")}).to_string(),
            );
        }
        return (0, serde_json::json!({"acked": messages.len()}).to_string());
    }
    if check_mode {
        return (
            0,
            if messages.is_empty() { "false" } else { "true" }.to_string(),
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
        common::update_tool_status(db, &name, "omp", &tool_name, &input);
    }
    (0, r#"{"decision":"allow"}"#.to_string())
}

pub(crate) fn handle_stop(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let name = match parse_flag(argv, "--name") {
        Some(n) => n,
        None => return (0, r#"{"error":"Missing --name"}"#.to_string()),
    };
    let reason = parse_flag(argv, "--reason").unwrap_or_else(|| "unknown".to_string());
    if has_flag(argv, "--soft") {
        common::soft_finalize_session(db, &name, &reason, None, true);
        (0, r#"{"ok":true,"soft":true}"#.to_string())
    } else {
        finalize_session(db, &name, &reason, None);
        (0, r#"{"ok":true}"#.to_string())
    }
}

pub fn dispatch_omp_hook(hook_name: &str, argv: &[String]) -> (i32, String) {
    let start = Instant::now();
    let ctx = HcomContext::from_os();
    crate::paths::ensure_hcom_directories_at(&ctx.hcom_dir);
    let db = match HcomDb::open() {
        Ok(db) => db,
        Err(e) => {
            log_error(
                "hooks",
                "hook.error",
                &format!("hook={} op=db_open err={}", hook_name, e),
            );
            return (
                0,
                serde_json::json!({"error": format!("DB open failed: {}", e)}).to_string(),
            );
        }
    };
    if !common::hook_gate_check(&ctx, &db) {
        return (0, String::new());
    }
    let handler_argv: Vec<String> = if !argv.is_empty() && argv[0] == hook_name {
        argv[1..].to_vec()
    } else {
        argv.to_vec()
    };
    let hook_name_owned = hook_name.to_string();
    let handler_start = Instant::now();
    let (exit_code, output) = common::dispatch_with_panic_guard(
        "omp",
        &hook_name_owned,
        (
            0,
            serde_json::json!({"error": "internal panic"}).to_string(),
        ),
        || match hook_name_owned.as_str() {
            "omp-start" => handle_start(&ctx, &db, &handler_argv),
            "omp-status" => handle_status(&db, &handler_argv),
            "omp-read" => handle_read(&db, &handler_argv),
            "omp-beforetool" => handle_beforetool(&db, &handler_argv),
            "omp-stop" => handle_stop(&db, &handler_argv),
            _ => (
                0,
                serde_json::json!({"error": format!("Unknown Omp hook: {}", hook_name_owned)})
                    .to_string(),
            ),
        },
    );
    log_info(
        "hooks",
        "omp.dispatch.timing",
        &format!(
            "hook={} handler_ms={:.2} total_ms={:.2} exit_code={}",
            hook_name,
            handler_start.elapsed().as_secs_f64() * 1000.0,
            start.elapsed().as_secs_f64() * 1000.0,
            exit_code
        ),
    );
    (exit_code, output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_db() -> (HcomDb, std::path::PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let db_path = std::env::temp_dir().join(format!(
            "test_omp_handlers_{}_{}.db",
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

    fn save_omp_instance(db: &HcomDb, name: &str) {
        let mut row = serde_json::Map::new();
        row.insert("name".into(), serde_json::json!(name));
        row.insert("tool".into(), serde_json::json!("omp"));
        row.insert("status".into(), serde_json::json!(ST_LISTENING));
        row.insert("status_context".into(), serde_json::json!(""));
        row.insert("status_detail".into(), serde_json::json!(""));
        row.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named(name, &row).unwrap();
    }

    fn argv_of(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    static BLOCKED_SIGNALED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    fn test_busy_handler(_count: i32) -> bool {
        BLOCKED_SIGNALED.store(true, std::sync::atomic::Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(10));
        true
    }

    #[test]
    fn concurrent_ack_does_not_rewind_cursor() {
        let (_db_orig, path) = setup_test_db();
        let db_main = HcomDb::open_at(&path).unwrap();
        save_omp_instance(&db_main, "tuna");

        // Pre-open the independent thread connection before acquiring writer lock
        let db_thread = HcomDb::open_at(&path).unwrap();

        let ext_conn = rusqlite::Connection::open(&path).unwrap();
        ext_conn
            .busy_timeout(std::time::Duration::from_millis(5000))
            .unwrap();

        BLOCKED_SIGNALED.store(false, std::sync::atomic::Ordering::SeqCst);
        db_thread
            .conn()
            .busy_handler(Some(test_busy_handler))
            .unwrap();

        // Step 1: Writer holds uncommitted transaction setting cursor = 10
        ext_conn
            .execute_batch(
                "BEGIN IMMEDIATE; UPDATE instances SET last_event_id = 10 WHERE name = 'tuna';",
            )
            .unwrap();

        // Step 2: Background thread runs handle_read with --ack --up-to 5
        let handle = std::thread::spawn(move || {
            handle_read(
                &db_thread,
                &argv_of(&["--name", "tuna", "--ack", "--up-to", "5"]),
            )
        });

        // Wait until db_thread is confirmed blocked waiting for ext_conn's write lock
        let start = std::time::Instant::now();
        while !BLOCKED_SIGNALED.load(std::sync::atomic::Ordering::SeqCst) {
            if start.elapsed() > std::time::Duration::from_secs(3) {
                panic!("db_thread did not trigger busy handler within timeout");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Step 3: Commit writer's transaction with cursor = 10
        ext_conn.execute_batch("COMMIT;").unwrap();

        // Step 4: Background thread finishes
        let (_, out) = handle.join().expect("background thread finished");
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(doc.get("error").is_none(), "ack succeeded: {out}");

        // Step 5: Verify last_event_id in DB is 10, proving MAX prevented regression
        let inst = db_main.get_instance_full("tuna").unwrap().unwrap();
        assert_eq!(
            inst.last_event_id, 10,
            "cursor was not rewound by concurrent ack of older event 5"
        );
        cleanup_db(&path);
    }

    #[test]
    fn duplicate_or_stale_ack_does_not_rewind_cursor() {
        let (db, path) = setup_test_db();
        save_omp_instance(&db, "tuna");

        // Advance to 10
        let (_, out) = handle_read(&db, &argv_of(&["--name", "tuna", "--ack", "--up-to", "10"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(doc["acked_to"].as_i64(), Some(10));
        assert_eq!(
            db.get_instance_full("tuna").unwrap().unwrap().last_event_id,
            10
        );

        // Stale / older ack of 5
        let (_, out) = handle_read(&db, &argv_of(&["--name", "tuna", "--ack", "--up-to", "5"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(doc.get("error").is_none());
        assert_eq!(
            db.get_instance_full("tuna").unwrap().unwrap().last_event_id,
            10,
            "cursor never moves backwards on stale ack"
        );

        // Duplicate ack of 10
        let (_, out) = handle_read(&db, &argv_of(&["--name", "tuna", "--ack", "--up-to", "10"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(doc.get("error").is_none());
        assert_eq!(
            db.get_instance_full("tuna").unwrap().unwrap().last_event_id,
            10,
            "cursor unchanged on duplicate ack"
        );

        // Invalid negative cursor
        let (_, out) = handle_read(&db, &argv_of(&["--name", "tuna", "--ack", "--up-to", "-3"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(
            doc.get("error").is_some(),
            "negative cursor must be rejected"
        );

        cleanup_db(&path);
    }

    #[test]
    fn initialize_last_event_id_monotonic() {
        let (db, path) = setup_test_db();
        save_omp_instance(&db, "tuna");

        // Log events so last_event_id > 0
        db.log_event("message", "src", &serde_json::json!({"text": "m1"}))
            .unwrap();
        let e2 = db
            .log_event("message", "src", &serde_json::json!({"text": "m2"}))
            .unwrap();

        initialize_last_event_id(&db, "tuna");
        assert_eq!(
            db.get_instance_full("tuna").unwrap().unwrap().last_event_id,
            e2
        );

        // Advance cursor further
        db.advance_instance_cursor("tuna", e2 + 10).unwrap();
        assert_eq!(
            db.get_instance_full("tuna").unwrap().unwrap().last_event_id,
            e2 + 10
        );

        // Second initialize_last_event_id must not regress the cursor
        initialize_last_event_id(&db, "tuna");
        assert_eq!(
            db.get_instance_full("tuna").unwrap().unwrap().last_event_id,
            e2 + 10
        );
        cleanup_db(&path);
    }
}
