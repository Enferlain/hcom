//! Pi Coding Agent hook handlers — argv-based lifecycle plus TypeScript plugin.

use anyhow::{Context, Result};

use super::pi_family::PI;
use super::runtime::{self, LaunchCtx, PerRunAdapter, RuntimeInjection};

pub fn dispatch_pi_hook(hook_name: &str, argv: &[String]) -> (i32, String) {
    PI.dispatch(hook_name, argv)
}

pub const PLUGIN_SOURCE: &str = include_str!("../pi_plugin/hcom.ts");
const PLUGIN_FILENAME: &str = "hcom.ts";

pub static PER_RUN: PerRunAdapter = PerRunAdapter {
    prepare: prepare_per_run,
    cleanup_legacy: cleanup_legacy_per_run,
    ensure_permissions: None,
    managed_value_flags: &["-e", "--extension"],
    strip_legacy_args: None,
};

/// Ambient plugin path: `$PI_CODING_AGENT_DIR` or `~/.pi/agent`, plus
/// `extensions/hcom.ts`.
pub fn get_pi_plugin_path() -> std::path::PathBuf {
    effective_plugin_path(&LaunchCtx::ambient(crate::tool::Tool::Pi, false))
}

/// Older hcom installed to `<HCOM_DIR parent>/.pi/extensions/` under a
/// project-local HCOM_DIR.
fn project_local_legacy_path() -> Option<std::path::PathBuf> {
    crate::runtime_env::legacy_tool_config_root()
        .map(|root| root.join(".pi").join("extensions").join(PLUGIN_FILENAME))
}

fn effective_plugin_path(ctx: &LaunchCtx) -> std::path::PathBuf {
    let agent_dir = ctx
        .path_var("PI_CODING_AGENT_DIR")
        .unwrap_or_else(|| ctx.home().join(".pi").join("agent"));
    agent_dir.join("extensions").join(PLUGIN_FILENAME)
}

fn prepare_per_run(ctx: &LaunchCtx) -> Result<RuntimeInjection> {
    let path = runtime::publish_file("pi", PLUGIN_FILENAME, PLUGIN_SOURCE.as_bytes())
        .context("Cannot publish Pi runtime extension")?;
    let mut args = ctx.args.clone();
    runtime::insert_before_separator(
        &mut args,
        ["-e".to_string(), path.to_string_lossy().into_owned()],
    );
    Ok(RuntimeInjection {
        args,
        env: Vec::new(),
    })
}

/// Legacy installs went to the launch's agent dir, or to `<HCOM_DIR parent>/.pi/`
/// under a project-local HCOM_DIR; check both.
fn cleanup_legacy_per_run(ctx: &LaunchCtx) -> Result<()> {
    let paths = std::iter::once(effective_plugin_path(ctx)).chain(project_local_legacy_path());
    crate::hooks::runtime::remove_owned_files(paths, is_hcom_owned).map(drop)
}

/// True when `path` holds an hcom Pi plugin: the current source or any earlier
/// version (all carry the `hcom-bootstrap` message type).
pub fn is_hcom_owned(path: &std::path::Path) -> std::io::Result<bool> {
    crate::hooks::runtime::file_is_hcom_owned(path, |content| {
        content == PLUGIN_SOURCE || content.contains("customType: \"hcom-bootstrap\"")
    })
}

/// Remove hcom's plugin file. A user file with the same name is left alone.
pub fn remove_pi_plugin() -> std::io::Result<bool> {
    let mut removed = false;
    for path in std::iter::once(get_pi_plugin_path()).chain(project_local_legacy_path()) {
        if is_hcom_owned(&path)? {
            std::fs::remove_file(path)?;
            removed = true;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::HcomDb;
    use crate::hooks::plugin_rpc::handle_status;
    use crate::instances;
    use crate::shared::context::HcomContext;
    use crate::shared::{ST_ACTIVE, ST_LISTENING};
    use serde_json::Value;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::time::Duration;

    fn setup_test_db() -> (HcomDb, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_pi_hooks_{}_{}.db",
            std::process::id(),
            test_id
        ));

        let db = HcomDb::open_at(&db_path).unwrap();
        (db, db_path)
    }

    fn cleanup(path: PathBuf) {
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    fn save_test_instance(db: &HcomDb, name: &str, status: &str) {
        let mut row = serde_json::Map::new();
        row.insert("name".into(), serde_json::json!(name));
        row.insert("tool".into(), serde_json::json!("pi"));
        row.insert("status".into(), serde_json::json!(status));
        row.insert("status_context".into(), serde_json::json!(""));
        row.insert("status_detail".into(), serde_json::json!(""));
        row.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named(name, &row).unwrap();
    }

    #[test]
    fn plugin_bootstraps_via_hidden_message() {
        assert!(PLUGIN_SOURCE.contains("before_agent_start"));
        assert!(PLUGIN_SOURCE.contains("customType: \"hcom-bootstrap\""));
        assert!(PLUGIN_SOURCE.contains("display: false"));
        assert!(!PLUGIN_SOURCE.contains("text: `${bootstrapText}\\n\\n${event.text}`"));
    }

    #[test]
    fn plugin_reports_idle_on_agent_settled() {
        assert!(PLUGIN_SOURCE.contains("pi.on(\"agent_settled\""));
        assert!(!PLUGIN_SOURCE.contains("pi.on(\"agent_end\""));
        assert!(!PLUGIN_SOURCE.contains("IDLE_DEBOUNCE_MS"));
    }

    #[test]
    fn plugin_stops_only_on_quit() {
        assert!(PLUGIN_SOURCE.contains("event.reason === undefined || event.reason === \"quit\""));
        assert!(PLUGIN_SOURCE.contains("if (name && terminal)"));
    }

    /// Acknowledgement waits for Pi to admit the delivery to the transcript;
    /// sendUserMessage() returning (void) is not a receipt.
    #[test]
    fn plugin_acks_on_admission_not_on_send() {
        assert!(PLUGIN_SOURCE.contains("pi.on(\"message_end\""));
        let send = PLUGIN_SOURCE.find("pi.sendUserMessage(").unwrap();
        let send_block = &PLUGIN_SOURCE[send..send + 600];
        assert!(!send_block.contains("flushAck"));
        assert!(PLUGIN_SOURCE.contains("void flushAck(\"message_end\")"));
        assert!(PLUGIN_SOURCE.contains("releaseUnadmitted"));
    }

    #[test]
    fn plugin_delivery_reports_active_edge() {
        assert!(PLUGIN_SOURCE.contains("reportStatus(ctx, \"active\""));
        assert!(PLUGIN_SOURCE.contains("`deliver:${sender}`"));
    }

    #[test]
    fn status_handler_wakes_plugin_only_when_entering_listening() {
        let (db, path) = setup_test_db();
        save_test_instance(&db, "luna", ST_LISTENING);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        db.upsert_notify_endpoint("luna", "plugin", port).unwrap();

        let argv = vec![
            "--name".to_string(),
            "luna".to_string(),
            "--status".to_string(),
            ST_LISTENING.to_string(),
        ];
        let (code, _) = handle_status(&db, &argv);
        assert_eq!(code, 0);
        std::thread::sleep(Duration::from_millis(20));
        assert!(listener.accept().is_err());

        let mut updates = serde_json::Map::new();
        updates.insert("status".into(), serde_json::json!(ST_ACTIVE));
        instances::update_instance_position(&db, "luna", &updates);

        let (code, _) = handle_status(&db, &argv);
        assert_eq!(code, 0);
        let mut accepted = false;
        for _ in 0..10 {
            if listener.accept().is_ok() {
                accepted = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(accepted);

        cleanup(path);
    }

    /// reload/new keep the process alive and skip pi-stop, so the next pi-start
    /// must rebind the same identity through the process binding.
    #[test]
    fn start_handler_keeps_identity_across_same_process_session_restart() {
        let (db, path) = setup_test_db();
        let temp = tempfile::TempDir::new().unwrap();
        save_test_instance(&db, "soba", ST_LISTENING);
        db.set_process_binding("pid-1", "", "soba").unwrap();

        let env = std::collections::HashMap::from([
            ("HCOM_PROCESS_ID".to_string(), "pid-1".to_string()),
            ("HCOM_LAUNCHED".to_string(), "1".to_string()),
            ("HCOM_TOOL".to_string(), "pi".to_string()),
        ]);
        let ctx = HcomContext::from_env(&env, temp.path().to_path_buf());
        let start = |sid: &str| {
            let (code, output) = PI.handle_start(&ctx, &db, &["--session-id".into(), sid.into()]);
            assert_eq!(code, 0);
            serde_json::from_str::<Value>(&output).unwrap()["name"]
                .as_str()
                .map(str::to_string)
        };

        // initial bind, /reload (same session), /new (fresh session)
        for sid in ["sid-1", "sid-1", "sid-2"] {
            assert_eq!(start(sid).as_deref(), Some("soba"));
            let inst = db.get_instance_full("soba").unwrap().unwrap();
            assert_eq!(inst.session_id.as_deref(), Some(sid));
            assert_eq!(
                db.get_session_binding(sid).unwrap().as_deref(),
                Some("soba")
            );
        }

        cleanup(path);
    }

    #[test]
    fn start_handler_uses_central_binding_for_existing_session() {
        let (db, path) = setup_test_db();
        let temp = tempfile::TempDir::new().unwrap();

        let mut canonical = serde_json::Map::new();
        canonical.insert("name".into(), serde_json::json!("miso"));
        canonical.insert("tool".into(), serde_json::json!("pi"));
        canonical.insert("session_id".into(), serde_json::json!("sid-123"));
        canonical.insert("status".into(), serde_json::json!(ST_LISTENING));
        canonical.insert("status_context".into(), serde_json::json!(""));
        canonical.insert("status_detail".into(), serde_json::json!(""));
        canonical.insert("last_event_id".into(), serde_json::json!(42));
        canonical.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named("miso", &canonical).unwrap();
        db.rebind_session("sid-123", "miso").unwrap();

        let mut placeholder = serde_json::Map::new();
        placeholder.insert("name".into(), serde_json::json!("temp"));
        placeholder.insert("tool".into(), serde_json::json!("pi"));
        placeholder.insert("status".into(), serde_json::json!("pending"));
        placeholder.insert("status_context".into(), serde_json::json!("new"));
        placeholder.insert("status_detail".into(), serde_json::json!(""));
        placeholder.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named("temp", &placeholder).unwrap();
        db.set_process_binding("pid-123", "", "temp").unwrap();

        let env = std::collections::HashMap::from([
            ("HCOM_PROCESS_ID".to_string(), "pid-123".to_string()),
            ("HCOM_LAUNCHED".to_string(), "1".to_string()),
            ("HCOM_TOOL".to_string(), "pi".to_string()),
        ]);
        let ctx = HcomContext::from_env(&env, temp.path().to_path_buf());

        let (code, output) = PI.handle_start(
            &ctx,
            &db,
            &[
                "--session-id".to_string(),
                "sid-123".to_string(),
                "--cwd".to_string(),
                temp.path().to_string_lossy().to_string(),
            ],
        );
        assert_eq!(code, 0);
        let response: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(response.get("name").and_then(|v| v.as_str()), Some("miso"));
        assert!(db.get_instance_full("temp").unwrap().is_none());
        assert_eq!(
            db.get_process_binding("pid-123").unwrap(),
            Some("miso".to_string())
        );

        let rebound = db.get_instance_full("miso").unwrap().unwrap();
        assert_eq!(rebound.last_event_id, 42);
        assert_eq!(rebound.directory, temp.path().to_string_lossy());

        cleanup(path);
    }

    #[test]
    fn test_is_hcom_owned_matches_hcom_plugins_only() {
        let dir = tempfile::tempdir().unwrap();
        let current = dir.path().join("current.ts");
        let older = dir.path().join("older.ts");
        let user = dir.path().join("user.ts");
        std::fs::write(&current, PLUGIN_SOURCE).unwrap();
        std::fs::write(&older, "sendMessage({ customType: \"hcom-bootstrap\" })").unwrap();
        std::fs::write(&user, "export default function mine() {}").unwrap();
        assert!(is_hcom_owned(&current).unwrap());
        assert!(is_hcom_owned(&older).unwrap());
        assert!(!is_hcom_owned(&user).unwrap());
        assert!(!is_hcom_owned(&dir.path().join("missing.ts")).unwrap());
        // Something unreadable in the way is an error, not "not ours".
        assert!(is_hcom_owned(dir.path()).is_err());
    }

    #[test]
    #[serial_test::serial]
    fn per_run_injection_uses_runtime_extension_before_separator() {
        let (_dir, hcom, home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let ctx = LaunchCtx {
            tool: crate::tool::Tool::Pi,
            env: [("HOME".to_string(), home.to_string_lossy().into_owned())]
                .into_iter()
                .collect(),
            cwd: home,
            args: vec!["--model".into(), "x".into(), "--".into(), "prompt".into()],
            auto_approve: false,
        };
        let injection = (PER_RUN.prepare)(&ctx).unwrap();
        assert_eq!(&injection.args[..2], &["--model", "x"]);
        assert_eq!(injection.args[2], "-e");
        let path = std::path::Path::new(&injection.args[3]);
        assert!(path.starts_with(hcom.join("integrations").join("pi")));
        assert_eq!(path.file_name().and_then(|n| n.to_str()), Some("hcom.ts"));
        assert_eq!(&injection.args[4..], &["--", "prompt"]);
    }

    #[test]
    fn per_run_cleanup_uses_launch_env_and_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let cwd = dir.path().join("work");
        let plugin = cwd
            .join("relative-agent")
            .join("extensions")
            .join("hcom.ts");
        std::fs::create_dir_all(plugin.parent().unwrap()).unwrap();
        std::fs::write(&plugin, PLUGIN_SOURCE).unwrap();

        let ctx = LaunchCtx {
            tool: crate::tool::Tool::Pi,
            env: [
                ("HOME".to_string(), home.to_string_lossy().into_owned()),
                (
                    "PI_CODING_AGENT_DIR".to_string(),
                    "relative-agent".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
            cwd,
            args: Vec::new(),
            auto_approve: false,
        };
        (PER_RUN.cleanup_legacy)(&ctx).unwrap();
        assert!(!plugin.exists());
    }
}
