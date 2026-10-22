//! Hook RPC dispatch shared by Pi, its fork Oh My Pi, and Hermes. Their plugins
//! call `hcom <tool>-{start,status,read,beforetool,stop}` with the same argv
//! shape; only the tool name and OMP's soft-stop recovery differ.

use std::time::Instant;

use crate::bootstrap;
use crate::db::HcomDb;
use crate::instance_binding;
use crate::instance_lifecycle as lifecycle;
use crate::instances;
use crate::log::{log_error, log_info, log_warn};
use crate::shared::ST_LISTENING;
use crate::shared::context::HcomContext;
use crate::tool::Tool;

use super::common;
use super::plugin_rpc::{self, parse_flag};

pub(crate) struct PiFamily {
    pub tool: Tool,
    /// OMP soft-stops on session_shutdown (task subagents share the parent
    /// process), which clears the process binding; the next start recovers it
    /// from `HCOM_INSTANCE_NAME`.
    pub recover_soft_stopped: bool,
}

pub(crate) const PI: PiFamily = PiFamily {
    tool: Tool::Pi,
    recover_soft_stopped: false,
};

pub(crate) const OMP: PiFamily = PiFamily {
    tool: Tool::Omp,
    recover_soft_stopped: true,
};

pub(crate) const HERMES: PiFamily = PiFamily {
    tool: Tool::Hermes,
    recover_soft_stopped: false,
};

impl PiFamily {
    fn name(&self) -> &'static str {
        self.tool.as_str()
    }

    /// Register the extension's notify port and wake the PTY loop: the bind is
    /// launch readiness for plugin-driven tools (see `delivery.rs`).
    pub(crate) fn upsert_plugin_notify_endpoint(
        &self,
        db: &HcomDb,
        instance_name: &str,
        port: u16,
    ) {
        if let Err(e) = db.upsert_notify_endpoint(instance_name, "plugin", port) {
            log_error(
                "native",
                &format!("{}.register_notify_fail", self.name()),
                &format!(
                    "Failed to register plugin notify port for {}: {}",
                    instance_name, e
                ),
            );
            return;
        }
        crate::notify::wake(db, instance_name, crate::notify::WakeKind::DELIVERY_LOOPS);
    }

    fn recover_binding(
        &self,
        ctx: &HcomContext,
        session_id: &str,
        process_id: &str,
        db: &HcomDb,
    ) -> Option<String> {
        if !self.recover_soft_stopped {
            return None;
        }
        let name = ctx
            .raw_env
            .get("HCOM_INSTANCE_NAME")
            .filter(|s| !s.is_empty())?;
        instance_binding::recover_process_binding_for_instance(db, name, session_id, process_id)
    }

    pub(crate) fn handle_start(
        &self,
        ctx: &HcomContext,
        db: &HcomDb,
        argv: &[String],
    ) -> (i32, String) {
        // Plugin RPC returns JSON errors on exit 0 so the extension can handle
        // setup failures without the agent treating the hook itself as failed.
        let session_id = match parse_flag(argv, "--session-id") {
            Some(sid) => sid,
            None => return (0, r#"{"error":"Missing --session-id"}"#.to_string()),
        };
        let transcript_path = parse_flag(argv, "--transcript-path");
        let cwd = parse_flag(argv, "--cwd");
        let notify_port: Option<u16> =
            parse_flag(argv, "--notify-port").and_then(|s| s.parse().ok());
        let existing_only = plugin_rpc::has_flag(argv, "--existing-only");

        let instance_name = if self.tool == Tool::Hermes {
            // Hermes branch: handles launched Hermes AND ordinary Hermes terminals.
            // Lock MUST fail closed: abort if lock cannot be acquired.
            let _lock = match crate::hooks::hermes::lock_hermes_adopt(db) {
                Ok(file) => file,
                Err(error) => {
                    log_warn("hooks", "hermes-start.lock_failed", &error.to_string());
                    return (
                        0,
                        serde_json::json!({
                            "error": format!("Hermes adopt lock unavailable: {error}")
                        })
                        .to_string(),
                    );
                }
            };
            if let Some(pid) = &ctx.process_id {
                // Launched or attached Hermes process: strict toolchecked bind
                match instance_binding::bind_session_to_process_for_tool(
                    db,
                    &session_id,
                    Some(pid),
                    "hermes",
                    false,
                ) {
                    instance_binding::ToolCheckedBind::Bound(name) => Some(name),
                    _ => self.recover_binding(ctx, &session_id, pid, db),
                }
            } else {
                // Ordinary Hermes terminal (not launched by hcom)
                // Single lock held across lookup, recovery, and adoption:
                let binding = match db.get_session_binding(&session_id) {
                    Ok(b) => b,
                    Err(e) => {
                        log_warn("hooks", "hermes-start.binding_read_failed", &e.to_string());
                        return (
                            0,
                            serde_json::json!({
                                "error": format!("Database error reading binding: {e}")
                            })
                            .to_string(),
                        );
                    }
                };
                let mut canonical = None;
                if let Some(bound_name) = binding {
                    match db.get_instance_full(&bound_name) {
                        Ok(Some(row)) if row.tool == "hermes" => {
                            if row.session_id.as_deref() == Some(&session_id) {
                                canonical = Some(bound_name);
                            } else {
                                return (
                                    0,
                                    serde_json::json!({
                                        "error": format!(
                                            "Hermes session {session_id} is bound to '{bound_name}', attributed to session {:?}",
                                            row.session_id
                                        )
                                    })
                                    .to_string(),
                                );
                            }
                        }
                        Ok(Some(_)) => {
                            return (
                                0,
                                serde_json::json!({
                                    "error": format!(
                                        "Hermes session {session_id} is bound to '{bound_name}', which is not a Hermes instance"
                                    )
                                })
                                .to_string(),
                            );
                        }
                        Ok(None) => {} // stale binding, fall through to recovery
                        Err(e) => {
                            log_warn("hooks", "hermes-start.instance_read_failed", &e.to_string());
                            return (
                                0,
                                serde_json::json!({
                                    "error": format!("Database error reading instance: {e}")
                                })
                                .to_string(),
                            );
                        }
                    }
                }
                if let Some(name) = canonical {
                    Some(name)
                } else {
                    let recovered =
                        match crate::hooks::hermes::recover_stopped_hermes_row(db, &session_id) {
                            Ok(r) => r,
                            Err(e) => {
                                log_warn("hooks", "hermes-start.recovery_failed", &e.to_string());
                                return (
                                    0,
                                    serde_json::json!({
                                        "error": format!(
                                            "Database error recovering stopped row: {e}"
                                        )
                                    })
                                    .to_string(),
                                );
                            }
                        };
                    if let Some(name) = recovered {
                        Some(name)
                    } else if existing_only {
                        return (
                            0,
                            serde_json::json!({"name": serde_json::Value::Null, "session_id": session_id})
                                .to_string(),
                        );
                    } else {
                        // Check if row already holds session_id before minting
                        match crate::hooks::hermes::hermes_row_holding_session_id(db, &session_id) {
                            Ok(Some(row)) => {
                                return (
                                    0,
                                    serde_json::json!({
                                        "error": format!(
                                            "Hermes session {session_id} already held by instance '{}'",
                                            row.name
                                        )
                                    })
                                    .to_string(),
                                );
                            }
                            Err(e) => {
                                return (
                                    0,
                                    serde_json::json!({
                                        "error": format!(
                                            "Database error checking row holders: {e}"
                                        )
                                    })
                                    .to_string(),
                                );
                            }
                            Ok(None) => {}
                        }
                        // Adopt with locked helper (single lock held!)
                        match crate::hooks::hermes::adopt_ordinary_hermes_instance_locked(
                            db,
                            &session_id,
                            transcript_path.as_deref(),
                            cwd.as_deref().or_else(|| ctx.cwd.to_str()),
                        ) {
                            Ok(name) => Some(name),
                            Err(e) => {
                                log_error("hooks", "hermes-start.adopt_failed", &e.to_string());
                                return (
                                    0,
                                    serde_json::json!({"error": format!("Adoption failed: {e}")})
                                        .to_string(),
                                );
                            }
                        }
                    }
                }
            }
        } else {
            let process_id = match &ctx.process_id {
                Some(pid) => pid.clone(),
                None => return (0, r#"{"error":"HCOM_PROCESS_ID not set"}"#.to_string()),
            };

            instance_binding::bind_session_to_process(db, &session_id, Some(&process_id))
                .or_else(|| self.recover_binding(ctx, &session_id, &process_id, db))
        };

        let Some(instance_name) = instance_name else {
            if existing_only {
                return (
                    0,
                    serde_json::json!({"name": serde_json::Value::Null, "session_id": session_id})
                        .to_string(),
                );
            }
            return (
                0,
                r#"{"error":"No instance bound to this process"}"#.to_string(),
            );
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
        updates.insert("tool".into(), serde_json::json!(self.name()));
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
            self.upsert_plugin_notify_endpoint(db, &instance_name, port);
        }
        log_info(
            "hooks",
            &format!("{}-start.bind", self.name()),
            &format!("instance={} session_id={}", instance_name, session_id),
        );
        crate::relay::worker::try_spawn_worker();

        let response = serde_json::json!({
            "name": instance_name,
            "session_id": session_id,
            "bootstrap": bootstrap::get_bootstrap(db, ctx, &instance_name, self.name()),
        });
        (0, response.to_string())
    }

    pub(crate) fn dispatch(&self, hook_name: &str, argv: &[String]) -> (i32, String) {
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
                return plugin_rpc::error(format!("DB open failed: {}", e));
            }
        };
        // Upstream launch gate: hooks run only for the process hcom launched.
        // The Hermes branches below also serve ordinary (not-launched) Hermes
        // terminals, which have no HCOM_PROCESS_ID by design — their RPCs are
        // authorized per-call instead of by ambient env (the plugin host env
        // may not even carry HERMES_SESSION_ID): a read-only enrolled-session
        // resolve, a `--name` participant checked for tool and session
        // attribution, or the explicit `hermes-start` enrollment ladder.
        // Nothing adopts an unknown session passively. Pi/OMP keep the strict
        // gate unchanged.
        let launched = common::hook_gate_check(&ctx);
        if !launched && self.tool != Tool::Hermes {
            return (0, String::new());
        }
        let handler_argv = argv.strip_prefix(&[hook_name.to_string()]).unwrap_or(argv);
        let hook = hook_name
            .strip_prefix(self.name())
            .and_then(|s| s.strip_prefix('-'))
            .unwrap_or("");
        if matches!(hook, "status" | "read" | "beforetool" | "stop") {
            // Hermes read-only session resolve mode: hermes-read --session-id <sid>
            if self.tool == Tool::Hermes
                && hook == "read"
                && parse_flag(handler_argv, "--name").is_none()
                && parse_flag(handler_argv, "--session-id").is_some()
            {
                let session_id = parse_flag(handler_argv, "--session-id").unwrap();
                let name = crate::hooks::hermes::resolve_enrolled_hermes_session(&db, &session_id);
                return (
                    0,
                    serde_json::json!({
                        "name": name,
                        "session_id": session_id,
                    })
                    .to_string(),
                );
            }
            if self.tool == Tool::Hermes && !launched {
                if let Some(name) = parse_flag(handler_argv, "--name") {
                    // An ordinary terminal must prove its session: correlate
                    // the explicit --session-id with the row's attribution.
                    // The plugin host forwards the participant's bound
                    // session id with every participant RPC, so a bare
                    // --name cannot mutate anything. Same-user CLI is not
                    // authentication — this is fail-closed attribution, not
                    // proof of identity.
                    let Some(claimed_session) = parse_flag(handler_argv, "--session-id") else {
                        return plugin_rpc::error("Missing --session-id");
                    };
                    match db.get_instance_full(&name) {
                        Ok(Some(row)) => {
                            if row.tool != "hermes" {
                                return plugin_rpc::error(format!(
                                    "{name} is not a Hermes instance"
                                ));
                            }
                            if row.session_id.as_deref() != Some(&claimed_session) {
                                return plugin_rpc::error(format!(
                                    "{name} is attributed to a different session"
                                ));
                            }
                        }
                        Ok(None) => {
                            return plugin_rpc::error(format!("Unknown instance: {name}"));
                        }
                        Err(e) => {
                            return plugin_rpc::error(format!("Instance lookup failed: {e}"));
                        }
                    }
                } else {
                    return plugin_rpc::error("Missing --name");
                }
            } else if let Err(reply) = plugin_rpc::require_attached(&ctx, &db, handler_argv) {
                return reply;
            }
        }
        let handler_start = Instant::now();
        let (exit_code, output) = common::dispatch_with_panic_guard(
            self.name(),
            hook_name,
            plugin_rpc::error("internal panic"),
            || match hook {
                "start" => self.handle_start(&ctx, &db, handler_argv),
                "status" => plugin_rpc::handle_status(&db, handler_argv),
                "read" => plugin_rpc::handle_read(&db, handler_argv),
                "beforetool" => plugin_rpc::handle_tool(&db, handler_argv, self.name()),
                "stop" => plugin_rpc::handle_stop(&db, handler_argv),
                _ => plugin_rpc::error(format!("Unknown {} hook: {}", self.name(), hook_name)),
            },
        );
        log_info(
            "hooks",
            &format!("{}.dispatch.timing", self.name()),
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
}
