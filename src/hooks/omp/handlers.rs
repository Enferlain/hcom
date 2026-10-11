//! Omp Coding Agent hook handlers — argv-based lifecycle plus TypeScript plugin.

use std::time::Instant;

use crate::bootstrap;
use crate::db::HcomDb;
use crate::instance_binding;
use crate::instance_lifecycle as lifecycle;
use crate::instances;
use crate::log::{log_error, log_info};
use crate::shared::ST_LISTENING;
use crate::shared::context::HcomContext;

use crate::hooks::common;
use crate::hooks::plugin_rpc::{self, parse_flag};

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

    plugin_rpc::initialize_last_event_id(db, &instance_name);
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
            "omp-status" => plugin_rpc::handle_status(&db, &handler_argv),
            "omp-read" => plugin_rpc::handle_read(&db, &handler_argv),
            "omp-beforetool" => plugin_rpc::handle_tool(&db, &handler_argv, "omp"),
            "omp-stop" => plugin_rpc::handle_stop(&db, &handler_argv),
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
