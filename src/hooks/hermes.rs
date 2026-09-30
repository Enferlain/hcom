//! Hermes Agent hook handlers — argv-based lifecycle plus Python plugin.
//!
//! Hermes-side plugins may use the argv-based RPC subcommands dispatched
//! here. Their installation and lifecycle are owned outside hcom:
//!
//! - `hermes-sessionstart` — bind session→process, set listening, return
//!   `{name, session_id, bootstrap}`. With `--existing-only` (the backend
//!   extension's enrollment route) nothing is ever created: an existing
//!   hermes-owned binding is reused, an explicitly stopped hermes row is
//!   recovered by session id only while the live row is still attributed to
//!   that session (the stopped snapshot is history, not ownership — a row
//!   since rebound to another session is not recoverable, or the old session
//!   would steal the identity and delete the new binding), and the
//!   hcom-launched process placeholder may bind only while no other session
//!   already owns it, the enrolling session carries no stopped-snapshot
//!   history (the fallback must never restore or recreate through that
//!   snapshot), and any stale session binding to a missing row could not be
//!   healed by recovery (the fallback would otherwise mint the missing row
//!   back and retire the placeholder) — hermes serve hosts several sessions
//!   per process; a second must not steal the first's identity. An
//!   unenrolled session answers `{"name": null}` — expected, not an error.
//! - `hermes-status` — set active/listening with context/detail.
//! - `hermes-read` — every mode prints one JSON document on stdout:
//!   raw unread list (default, `[message, …]`), `--format`
//!   (`{"context", "up_to", "count"}` where `up_to` is the cursor of the
//!   DELIVERED batch — the batch is capped, so it can lag the newest
//!   unread event), `--check` (`{"has_unread": bool}`), and
//!   `--ack [--up-to ID]` (`{"acked_to": id}` / `{"acked": n}`; acks are
//!   monotonic — the cursor never moves backwards, and acking the
//!   empty-inbox cursor 0 is a no-op, not an error).
//!   `--session-id <sid>` (without `--name`) is a read-only resolve mode:
//!   `{"name": <enrolled name or null>, "session_id"}` — no binding,
//!   recovery, or creation; used by the backend extension's post-tool-call
//!   discovery for sessions that ran `hcom start` mid-session.
//! - `hermes-beforetool` — record tool activity into the event stream.
//! - `hermes-stop` — finalize (or `--soft`-finalize) the instance.
//!
//! Every write RPC (`hermes-status`, `hermes-read` in ack mode,
//! `hermes-stop`) requires `--name` to resolve to an existing hermes-owned
//! instance row and answers a JSON error otherwise: an unknown name must not
//! emit status events for a nonexistent instance or claim an ack that no row
//! recorded, and a row owned by another tool is not the Hermes plugin's to
//! touch. Writes also accept an optional `--session-id` claim: when present,
//! the row must still be attributed to that session (stale-callback guard —
//! a per-session callback must not status/ack/finalize a name that has since
//! been rebound to a different session). Absent, the legacy contract holds.
//! Reads of an unknown name stay permissive (empty inbox);
//! `hermes-beforetool` always allows — it only skips bookkeeping for names
//! that are not hers.

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

/// The argv RPCs are the Hermes plugin's private channel: a `--name` must
/// resolve to an existing hermes-owned row before any write. `set_status`
/// still emits a status event when its UPDATE touches no row, so an unknown
/// name would otherwise log ghost events into the shared stream, and a name
/// owned by another tool would let the plugin overwrite or finalize it.
fn is_hermes_instance(db: &HcomDb, name: &str) -> bool {
    db.get_instance_full(name)
        .ok()
        .flatten()
        .is_some_and(|row| row.tool == "hermes")
}

/// Stale-callback guard for write RPCs: a caller that carries its Hermes
/// session id (`--session-id`) may only touch a row CURRENTLY attributed to
/// that session. A session whose identity was since rebound elsewhere (serve
/// rebind, `hcom start --as`) must not have its old name statused, acked, or
/// finalized through a stale per-session callback. Absent/empty session id
/// is the legacy contract: no claim, nothing to validate.
fn session_attribution_matches(db: &HcomDb, name: &str, session_id: &str) -> bool {
    if session_id.is_empty() {
        return true;
    }
    db.get_instance_full(name)
        .ok()
        .flatten()
        .is_some_and(|row| row.session_id.as_deref() == Some(session_id))
}

/// Refusal JSON for a stale-attribution write RPC.
fn stale_attribution_error(name: &str, session_id: &str) -> String {
    serde_json::json!({
        "error": format!(
            "Hermes instance '{name}' is no longer attributed to session {session_id}; \
             refusing the stale-callback write"
        )
    })
    .to_string()
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
    let _adopt_lock = acquire_hermes_adopt_lock(db)?;
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
    // without changing the shared bind_session_to_process behavior. Legacy
    // adopt stays error-tolerant: an unreadable snapshot simply means no
    // recovery here (a fresh identity is minted below), as it always did.
    if let Ok(Some(name)) = recover_stopped_hermes_row(db, session_id) {
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

/// Safely recover an explicitly stopped hermes row by session id: the
/// stopped-snapshot evidence plus a live hermes-owned row, rebound in place
/// (the row keeps its cursor and history; rebind_instance_session also heals
/// a stale binding naming the session). `Ok(None)` is a decided "not
/// recoverable"; `Err` means the state was unreadable and no decision was
/// made — callers that must fail closed treat it as a hard stop.
///
/// Fail-closed live ownership guard: the stopped snapshot is immutable
/// life-event history, so it still names a row long after that row was
/// rebound to a different session — and `rebind_instance_session` deletes the
/// row's CURRENT binding. Recovering through a stale snapshot would strip the
/// new session's binding and attribution (the old session stealing the
/// identity). Only a live row that is unattributed or still attributed to
/// this session is recoverable; a row owned by another session is not ours.
fn recover_stopped_hermes_row(db: &HcomDb, session_id: &str) -> anyhow::Result<Option<String>> {
    let Some(name) = db.find_stopped_instance_by_session_id(session_id)? else {
        return Ok(None);
    };
    let Some(row) = db.get_instance_full(&name)? else {
        return Ok(None);
    };
    if row.tool != "hermes" {
        return Ok(None);
    }
    if let Some(owner) = row
        .session_id
        .as_deref()
        .filter(|sid| !sid.is_empty() && *sid != session_id)
    {
        log_warn(
            "hooks",
            "hermes.recover_stopped.refused_row_rebound",
            &format!("instance={name} stopped_session={session_id} live_owner={owner}"),
        );
        return Ok(None);
    }
    db.rebind_instance_session(&name, session_id)?;
    Ok(Some(name))
}

/// Take the cross-process adopt/enroll serialization lock. Session→identity
/// transitions (adopt, stopped-row recovery, placeholder bind) resolve a row
/// and its bindings then rebind them; two of them racing can interleave their
/// reads and rebinds so one session claims an identity out from under the
/// other. Holders serialize on `<db dir>/.tmp/hermes_adopt.lock`; the guard
/// file keeps the lock held until it is dropped.
fn acquire_hermes_adopt_lock(db: &HcomDb) -> Option<std::fs::File> {
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
    Some(lock_file)
}

fn is_hcom_launched_interactive_cli(ctx: &HcomContext, row: &crate::db::InstanceRow) -> bool {
    if !ctx.is_launched {
        return false;
    }
    if row.tool != "hermes" {
        return false;
    }
    let Some(ref raw_args) = row.launch_args else {
        return false;
    };
    let Ok(args) = serde_json::from_str::<Vec<String>>(raw_args) else {
        return false;
    };
    !args.iter().any(|a| a == "serve")
}

fn handle_start(ctx: &HcomContext, db: &HcomDb, argv: &[String]) -> (i32, String) {
    // Plugin RPC returns JSON errors on exit 0 so the plugin can handle
    // setup failures without Hermes treating the subprocess as failed.
    let session_id = match parse_flag(argv, "--session-id").filter(|s| !s.is_empty()) {
        Some(sid) => sid,
        None => return (0, r#"{"error":"Missing --session-id"}"#.to_string()),
    };
    let transcript_path = parse_flag(argv, "--transcript-path")
        .filter(|p| !p.is_empty())
        .or_else(|| {
            crate::transcript::hermes::resolve_hermes_db_path_from_env(&ctx.raw_env)
                .map(|p| p.to_string_lossy().to_string())
        });
    let cwd = parse_flag(argv, "--cwd");
    let notify_port: Option<u16> =
        parse_flag(argv, "--notify-port").and_then(|raw| match raw.parse::<u16>() {
            Ok(port) => Some(port),
            Err(_) => {
                log_warn(
                    "hooks",
                    "hermes-sessionstart.invalid_notify_port",
                    &format!("ignoring unparseable --notify-port {raw:?}"),
                );
                None
            }
        });

    let existing_only = has_flag(argv, "--existing-only");
    // The enrollment lock spans the whole enrollment critical section: name
    // resolution below (binding, stopped-row recovery, placeholder fallback)
    // AND the attribution and notify-endpoint writes further down — a rebind
    // landing between them would be clobbered by enrollment's own
    // session_id/port writes. Released before the bootstrap/relay side
    // effects, which write no per-name session state.
    let mut enroll_lock: Option<std::fs::File> = None;
    let instance_name = if existing_only {
        // Backend-extension enrollment: existing identities only, never a
        // mint (adopt_ordinary_hermes_instance stays on the legacy route).
        // A binding naming a live non-hermes row is a collision, not ours.
        // Enrollment resolves a row and rebinds its bindings exactly like an
        // adopt, so it serializes on the same adopt lock; when the lock
        // cannot be taken, refuse rather than resolve unsynchronized.
        enroll_lock = acquire_hermes_adopt_lock(db);
        if enroll_lock.is_none() {
            return (
                0,
                serde_json::json!({
                    "error": "Hermes enrollment lock unavailable; refusing unsynchronized enrollment"
                })
                .to_string(),
            );
        }
        // Guard reads fail CLOSED on DB errors: a swallowed Err would look
        // exactly like "no binding / no snapshot" and let the placeholder
        // fallback proceed even though a retry could reveal a stale binding
        // or stopped-snapshot history. Each guard read below (binding,
        // bound row, recovery, snapshot gate) answers a JSON error on Err
        // instead of guessing. The placeholder lookup after the guards is
        // optional resolution, not a guard: its read failures fail closed to
        // a null name with no mutation, which is safe but not an error.
        let binding = match db.get_session_binding(&session_id) {
            Ok(binding) => binding,
            Err(error) => {
                log_warn(
                    "hooks",
                    "hermes-sessionstart.enroll_binding_read_failed",
                    &error.to_string(),
                );
                return (
                    0,
                    serde_json::json!({
                        "error": format!(
                            "Hermes enrollment read failed for session {session_id}: {error}"
                        )
                    })
                    .to_string(),
                );
            }
        };
        let mut canonical: Option<String> = None;
        let mut stale_binding = false;
        if let Some(bound) = binding {
            let bound_row = match db.get_instance_full(&bound) {
                Ok(row) => row,
                Err(error) => {
                    log_warn(
                        "hooks",
                        "hermes-sessionstart.enroll_bound_row_read_failed",
                        &error.to_string(),
                    );
                    return (
                        0,
                        serde_json::json!({
                            "error": format!(
                                "Hermes enrollment read failed for instance {bound}: {error}"
                            )
                        })
                        .to_string(),
                    );
                }
            };
            match bound_row {
                Some(row) if row.tool == "hermes" => {
                    if row.session_id.as_deref() != Some(session_id.as_str()) {
                        return (
                            0,
                            serde_json::json!({
                                "error": format!(
                                    "Hermes session {session_id} is bound to '{bound}', which is attributed to session {}",
                                    row.session_id.as_deref().unwrap_or("none")
                                )
                            })
                            .to_string(),
                        );
                    }
                    canonical = Some(bound);
                }
                Some(_) => {
                    return (
                        0,
                        serde_json::json!({
                            "error": format!(
                                "Hermes session {session_id} is bound to '{bound}', which is not a Hermes instance"
                            )
                        })
                        .to_string(),
                    );
                }
                // Stale binding: the stopped-row recovery below rebinds
                // through it, healing the binding to name the recovered row.
                None => stale_binding = true,
            }
        }
        let recovered = match canonical
            .map(|name| Ok(Some(name)))
            .unwrap_or_else(|| recover_stopped_hermes_row(db, &session_id))
        {
            Ok(name) => name,
            Err(error) => {
                log_warn(
                    "hooks",
                    "hermes-sessionstart.enroll_recovery_read_failed",
                    &error.to_string(),
                );
                return (
                    0,
                    serde_json::json!({
                        "error": format!(
                            "Hermes enrollment read failed for session {session_id}: {error}"
                        )
                    })
                    .to_string(),
                );
            }
        };
        match recovered {
            Some(name) => Some(name),
            // Stale-binding gate: bind_session_to_process re-reads the
            // session binding and would treat the stale name as a live
            // canonical (its canonical-exists path) — recreating the
            // deleted row (a mint) and retiring the live placeholder as a
            // session switch. If recovery could not heal the binding,
            // existing-only fails closed instead.
            None if stale_binding => {
                log_info(
                    "hooks",
                    "hermes-sessionstart.enroll_fallback_stale_binding_refused",
                    &format!("session={session_id}"),
                );
                None
            }
            // Snapshot-history gate: the guarded recovery above is the
            // ONLY route by which a session's stopped snapshot may be
            // consumed by enrollment. bind_session_to_process would
            // consult the same immutable snapshot too (its
            // restore-stopped path runs before the placeholder bind) —
            // recreating a deleted row (violating existing-only's
            // never-creates guarantee) or rebinding a row now owned by
            // another session (the stale-snapshot steal the recovery
            // guard refuses). A session with stopped-snapshot history
            // does not enroll through the process placeholder — and an
            // unreadable snapshot cannot prove the session has none.
            None => match db.find_stopped_instance_by_session_id(&session_id) {
                Ok(Some(_)) => {
                    log_info(
                        "hooks",
                        "hermes-sessionstart.enroll_fallback_snapshot_refused",
                        &format!("session={session_id}"),
                    );
                    None
                }
                Err(error) => {
                    log_warn(
                        "hooks",
                        "hermes-sessionstart.enroll_snapshot_read_failed",
                        &error.to_string(),
                    );
                    return (
                        0,
                        serde_json::json!({
                            "error": format!(
                                "Hermes enrollment read failed for session {session_id}: {error}"
                            )
                        })
                        .to_string(),
                    );
                }
                // The hcom-launched interactive CLI placeholder may bind —
                // but hermes serve hosts several sessions in one process, and
                // must never auto-enroll sessions into a serve process placeholder.
                // Furthermore, process-placeholder fallback must be limited to
                // demonstrably hcom-launched interactive CLI.
                Ok(None) => ctx.process_id.as_deref().and_then(|process_id| {
                    let (bound_session, placeholder_name) =
                        db.get_process_binding_full(process_id).ok()??;
                    let row = db.get_instance_full(&placeholder_name).ok()??;
                    if row.tool != "hermes" {
                        return None;
                    }
                    if let Some(other) = row
                        .session_id
                        .as_deref()
                        .filter(|sid| !sid.is_empty() && *sid != session_id.as_str())
                    {
                        log_warn(
                            "hooks",
                            "hermes-sessionstart.placeholder_double_attribution",
                            &format!(
                                "placeholder={placeholder_name} owned_session={other} \
                                 requested_session={session_id}"
                            ),
                        );
                        return None;
                    }
                    let already_owning = row.session_id.as_deref() == Some(session_id.as_str())
                        || bound_session.as_deref() == Some(session_id.as_str());
                    if !already_owning && !is_hcom_launched_interactive_cli(ctx, &row) {
                        log_info(
                            "hooks",
                            "hermes-sessionstart.placeholder_fallback_skipped",
                            &format!(
                                "placeholder={placeholder_name} session={session_id} \
                                 is_launched={} launch_args={:?}",
                                ctx.is_launched, row.launch_args
                            ),
                        );
                        return None;
                    }
                    instance_binding::bind_session_to_process(db, &session_id, Some(process_id))
                }),
            },
        }
    } else if let Some(process_id) = ctx.process_id.as_deref() {
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
        if existing_only {
            // Unenrolled is the expected state for an ordinary session that
            // never ran `hcom start`: a null name, not an error, so the
            // backend extension can stay silent without error noise.
            return (
                0,
                serde_json::json!({"name": null, "session_id": session_id}).to_string(),
            );
        }
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
        // Inside the enrollment critical section: the notify port is routing
        // state for the name — a rebind landing before this upsert would let
        // this enrollment misroute the new owner's mail.
        upsert_plugin_notify_endpoint(db, &instance_name, port);
    }
    // The enrollment critical section ends here: identity, binding,
    // attribution, and the notify endpoint are consistent, and the remaining
    // bootstrap/relay side effects write no per-name session state.
    drop(enroll_lock);
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
    if !is_hermes_instance(db, &name) {
        return (
            0,
            serde_json::json!({"error": format!("Unknown Hermes instance: {name}")}).to_string(),
        );
    }
    let claimed_session = parse_flag(argv, "--session-id").unwrap_or_default();
    if !session_attribution_matches(db, &name, &claimed_session) {
        return (0, stale_attribution_error(&name, &claimed_session));
    }
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
    // Read-only resolve mode: map a Hermes session id to its enrolled hcom
    // name without binding, recovering, or creating anything. The backend
    // extension's post-tool-call discovery uses this for sessions that ran
    // `hcom start` mid-session, where a full sessionstart would be too heavy
    // to run per tool call. `--name` keeps its mailbox semantics and takes
    // precedence when both are given.
    if parse_flag(argv, "--name").is_none()
        && let Some(session_id) = parse_flag(argv, "--session-id").filter(|s| !s.is_empty())
    {
        if has_flag(argv, "--ack") || has_flag(argv, "--format") || has_flag(argv, "--check") {
            return (
                0,
                serde_json::json!({
                    "error": "Cannot use --ack, --format, or --check in session resolve mode without --name"
                })
                .to_string(),
            );
        }
        let name = db
            .get_session_binding(&session_id)
            .ok()
            .flatten()
            .filter(|name| {
                db.get_instance_full(name)
                    .ok()
                    .flatten()
                    .is_some_and(|row| {
                        row.tool == "hermes"
                            && row.session_id.as_deref() == Some(session_id.as_str())
                    })
            });
        return (
            0,
            serde_json::json!({
                "name": name,
                "session_id": session_id,
            })
            .to_string(),
        );
    }
    let name = match parse_flag(argv, "--name") {
        Some(n) => n,
        None => return (0, r#"{"error":"Missing --name"}"#.to_string()),
    };
    let format_mode = has_flag(argv, "--format");
    let check_mode = has_flag(argv, "--check");
    let ack_mode = has_flag(argv, "--ack");

    // An existing row that is not a hermes instance is another tool's
    // mailbox: refuse every mode rather than read or ack it. A missing row
    // stays permissive for reads (documented empty inbox), but acks below
    // additionally require a real row — `update_instance_fields` is
    // UPDATE-only, so acking a missing row would claim a cursor no row kept.
    if db
        .get_instance_full(&name)
        .ok()
        .flatten()
        .is_some_and(|row| row.tool != "hermes")
    {
        return (
            0,
            serde_json::json!({"error": format!("Not a Hermes instance: {name}")}).to_string(),
        );
    }

    let claimed_session = parse_flag(argv, "--session-id").unwrap_or_default();
    if format_mode && !session_attribution_matches(db, &name, &claimed_session) {
        return (0, stale_attribution_error(&name, &claimed_session));
    }

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
        // Acks write a cursor: require a real hermes row. Without one the
        // UPDATE below would touch nothing while the reply claimed success.
        if !is_hermes_instance(db, &name) {
            return (
                0,
                serde_json::json!({"error": format!("Unknown Hermes instance: {name}")})
                    .to_string(),
            );
        }
        // And an ack is a write: a stale per-session callback must not
        // advance the cursor of a name since rebound to another session.
        let claimed_session = parse_flag(argv, "--session-id").unwrap_or_default();
        if !session_attribution_matches(db, &name, &claimed_session) {
            return (0, stale_attribution_error(&name, &claimed_session));
        }
        if let Some(up_to) = parse_flag(argv, "--up-to") {
            let Ok(ack_id) = up_to.parse::<i64>() else {
                return (
                    0,
                    serde_json::json!({"error": format!("Invalid --up-to: {up_to}")}).to_string(),
                );
            };
            if ack_id < 0 {
                return (
                    0,
                    serde_json::json!({"error": format!("Invalid --up-to: {up_to}")}).to_string(),
                );
            }
            // Monotonic: the cursor never moves backwards, so a late/duplicate
            // ack for an older batch cannot re-mark newer messages unread —
            // including the empty-inbox cursor 0 that `--format` reports.
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
    // The allow decision is unconditional; bookkeeping is skipped for names
    // that are not ours so a stray --name cannot ghost-log tool events.
    if !tool_name.is_empty() && is_hermes_instance(db, &name) {
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
    if !is_hermes_instance(db, &name) {
        return (
            0,
            serde_json::json!({"error": format!("Unknown Hermes instance: {name}")}).to_string(),
        );
    }
    // Finalizing is the strongest write: a stale per-session callback must
    // not stop a name that has since been rebound to a different session.
    let claimed_session = parse_flag(argv, "--session-id").unwrap_or_default();
    if !session_attribution_matches(db, &name, &claimed_session) {
        return (0, stale_attribution_error(&name, &claimed_session));
    }
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

/// The hermes home itself (like `~/.hermes`). `HERMES_HOME` IS the home, not
/// its parent — honor it verbatim so a custom home never duplicates `.hermes`
/// inside itself. Without it, fall back to the default home, isolating under
/// the tool config root when hcom runs against an isolated workspace
/// (mirroring the launcher's `isolated_tool_config_dir` choice of
/// `<root>/.hermes`). Hermes resolves `<home>/config.yaml` and
/// `<home>/plugins` from the same root (hermes_cli config.get_config_path /
/// plugins_discovery).
fn hermes_home_dir() -> std::path::PathBuf {
    if let Some(home) = std::env::var("HERMES_HOME").ok().filter(|s| !s.is_empty()) {
        return std::path::PathBuf::from(home);
    }
    let tool_root = crate::runtime_env::tool_config_root();
    let home = current_home_dir();
    if tool_root == home {
        home.join(".hermes")
    } else {
        tool_root.join(".hermes")
    }
}

/// User plugins live at `<hermes home>/plugins` (hermes_cli/plugins_discovery).
fn hermes_plugin_root() -> std::path::PathBuf {
    hermes_home_dir().join("plugins")
}

fn hermes_config_path() -> std::path::PathBuf {
    hermes_home_dir().join("config.yaml")
}

pub fn get_hermes_plugin_path() -> std::path::PathBuf {
    hermes_plugin_root()
        .join(PLUGIN_DIR_NAME)
        .join("plugin.yaml")
}

/// External plugin files present under `<hermes home>/plugins/hcom`.
pub fn verify_hermes_plugin_installed() -> bool {
    let dir = hermes_plugin_root().join(PLUGIN_DIR_NAME);
    dir.join("__init__.py").is_file() && dir.join("plugin.yaml").is_file()
}

/// Hermes `HERMES_SAFE_MODE` truthy values — exactly hermes_cli's
/// `utils.TRUTHY_STRINGS` ({"1", "true", "yes", "on"}, stripped + lowercased,
/// per `env_var_enabled`/`is_truthy_value`); when active Hermes suppresses
/// ALL plugin loading.
fn hermes_safe_mode_active() -> bool {
    std::env::var("HERMES_SAFE_MODE")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Extract the plugin name sets from the top-level `plugins:` block of a
/// Hermes config.yaml.
///
/// Deliberately a subset parser, not a YAML implementation: the installed
/// Hermes CLI answers authoritatively via `hermes plugins list --enabled
/// --user --json` (~1.2s, see the probe below), so this parser only serves
/// as the fallback when the CLI cannot answer (no binary, version skew,
/// timeout) — hcom carries no YAML dependency for that niche. Hermes itself
/// writes this file via yaml dump, so the supported forms are the ones its
/// dumper emits (and the parser accepts both styles):
/// - block lists with items one level deeper than the key (observed config)
///   or at the key's own indent (PyYAML default style);
/// - inline flow lists (`enabled: [a, 'b']`) incl. quoted entries;
/// - comments and nested subblocks (e.g. `plugins.entries.*`) are skipped.
///
/// Unsupported YAML (aliases, multiline scalars) can only ever yield a name
/// that differs from `hcom`, so it cannot false-positive "enabled". Returns
/// None when the block or the lists are missing/malformed — Hermes treats a
/// missing allow-list as "nothing enabled" (opt-in default), so callers fail
/// safe to not-ready, and the suggested remediation
/// (`hermes plugins enable hcom`) is idempotent.
fn parse_hermes_plugin_lists(config_text: &str) -> Option<(Vec<String>, Vec<String>)> {
    fn unquote(item: &str) -> String {
        let item = item.trim();
        let stripped = item.strip_prefix(['"', '\'']).unwrap_or(item);
        stripped
            .strip_suffix(['"', '\''])
            .unwrap_or(stripped)
            .to_string()
    }
    fn flow_list(rest: &str) -> Option<Vec<String>> {
        let inner = rest.strip_prefix('[')?.strip_suffix(']')?;
        Some(
            inner
                .split(',')
                .map(unquote)
                .filter(|s| !s.is_empty())
                .collect(),
        )
    }

    #[derive(Clone, Copy)]
    enum List {
        Enabled,
        Disabled,
    }

    let mut enabled: Option<Vec<String>> = None;
    let mut disabled: Option<Vec<String>> = None;
    let mut in_plugins = false;
    let mut key_indent: Option<usize> = None;
    let mut collecting: Option<List> = None;

    for raw in config_text.lines() {
        if raw.trim().is_empty() || raw.trim_start().starts_with('#') {
            continue;
        }
        let indent = raw.len() - raw.trim_start().len();
        let body = raw.trim_start();

        if !in_plugins {
            if indent == 0 && body == "plugins:" {
                in_plugins = true;
            }
            continue;
        }
        // Leaving the plugins block (a new top-level key).
        if indent == 0 {
            in_plugins = false;
            collecting = None;
            continue;
        }
        // First key inside the block fixes the child indentation level.
        let key_level = *key_indent.get_or_insert(indent);
        if indent < key_level {
            // Deeper-nested subblock (e.g. plugins.entries.hcom) — skip.
            continue;
        }
        // List items may sit at the key's indent (PyYAML default block
        // style) or one level deeper (Hermes's indented dumps).
        if let Some(item) = body.strip_prefix("- ") {
            if let Some(which) = collecting {
                let list = match which {
                    List::Enabled => &mut enabled,
                    List::Disabled => &mut disabled,
                };
                list.get_or_insert_with(Vec::new).push(unquote(item));
            }
            continue;
        }
        if indent > key_level {
            // A deeper NON-item line turns the enclosing key into a mapping,
            // not a name list (e.g. `enabled:` holding `nested: [- hcom]`).
            // Stop collecting so items under a sub-key can never leak in as
            // allow-list names — Hermes itself treats a non-list `enabled`
            // as "nothing enabled".
            collecting = None;
            continue;
        }
        // Key line at child level: ends any list being collected.
        collecting = None;
        let (key, rest) = body.split_once(':')?;
        let rest = rest.trim();
        match key.trim() {
            "enabled" => {
                if rest.is_empty() {
                    collecting = Some(List::Enabled);
                } else {
                    enabled = flow_list(rest);
                }
            }
            "disabled" => {
                if rest.is_empty() {
                    collecting = Some(List::Disabled);
                } else {
                    disabled = flow_list(rest);
                }
            }
            _ => {}
        }
    }

    // A malformed/missing enabled allow-list loads nothing in Hermes.
    let enabled = enabled?;
    let disabled = disabled.unwrap_or_default();
    Some((enabled, disabled))
}

/// One evaluated readiness verdict with its diagnostic, produced by a
/// single pass so a gate prints exactly what it checked: re-probing after
/// a failed verdict could race a timeout and contradict the first answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermesPluginReadiness {
    pub ready: bool,
    /// Empty when ready; otherwise an actionable, state-accurate message.
    pub diagnostic: String,
}

fn not_ready(diagnostic: String) -> HermesPluginReadiness {
    HermesPluginReadiness {
        ready: false,
        diagnostic,
    }
}

/// Evaluate the full Hermes integration readiness ONCE: the external plugin
/// files exist AND Hermes actually enables the plugin. Files alone are NOT
/// ready — a present-but-disabled plugin never loads into Hermes.
///
/// Enabled-state mirrors hermes_cli/plugins.py loader semantics:
/// `plugins.enabled` is an opt-in allow-list, `plugins.disabled` vetoes
/// even when enabled, a missing or malformed allow-list loads nothing, and
/// `HERMES_SAFE_MODE` suppresses all plugin loading. The installed Hermes
/// CLI is the source of truth when available (it parses the real config
/// with a real YAML loader and applies the loader's own semantics, so
/// neither subset-parse false positives nor false negatives can flip the
/// launch gate). The config.yaml subset parse is only a fallback for when
/// the CLI cannot answer — no binary, version skew (no `--json`), or a
/// hung command.
pub fn evaluate_hermes_plugin_readiness() -> HermesPluginReadiness {
    if hermes_safe_mode_active() {
        return not_ready(
            "HERMES_SAFE_MODE is set — Hermes suppresses all plugin loading. Unset it, then ensure the plugin is enabled: hermes plugins enable hcom"
                .to_string(),
        );
    }
    if !verify_hermes_plugin_installed() {
        return not_ready(format!(
            "Hermes plugin files are missing at {}. The plugin is managed outside hcom — install it with Hermes, then run: hermes plugins enable hcom",
            get_hermes_plugin_path().display()
        ));
    }
    match hermes_cli_reports_plugin_enabled() {
        Some(true) => HermesPluginReadiness {
            ready: true,
            diagnostic: String::new(),
        },
        // The CLI answered: ground the text in its verdict (the reason may
        // be the allow-list, a broken manifest, or anything else the loader
        // itself weighs).
        Some(false) => not_ready(
            "Hermes does not report the hcom plugin as enabled (`hermes plugins list --enabled --user`). Run `hermes plugins enable hcom`, then restart Hermes."
                .to_string(),
        ),
        // CLI unavailable (no binary / version skew / timeout): ground the
        // verdict in the config fallback instead.
        None => {
            let parsed = std::fs::read_to_string(hermes_config_path())
                .ok()
                .and_then(|text| parse_hermes_plugin_lists(&text));
            match parsed {
                // Explicit deny-list entry is a positive extraction —
                // trustworthy.
                Some((_, disabled)) if disabled.iter().any(|n| n == PLUGIN_DIR_NAME) => not_ready(format!(
                    "Hermes config has the hcom plugin explicitly disabled (plugins.disabled) in {}. Run `hermes plugins enable hcom`, then restart Hermes.",
                    hermes_config_path().display()
                )),
                Some((enabled, _)) if enabled.iter().any(|n| n == PLUGIN_DIR_NAME) => {
                    HermesPluginReadiness {
                        ready: true,
                        diagnostic: String::new(),
                    }
                }
                Some(_) => not_ready(format!(
                    "Hermes config has the hcom plugin not in the plugins.enabled allow-list in {}. Run `hermes plugins enable hcom`, then restart Hermes.",
                    hermes_config_path().display()
                )),
                None => not_ready(format!(
                    "Hermes config has the hcom plugin not enabled (no parsable plugins.enabled allow-list) in {}. Run `hermes plugins enable hcom`, then restart Hermes.",
                    hermes_config_path().display()
                )),
            }
        }
    }
}

/// Full Hermes integration readiness as a plain bool (status tables and
/// hook-state checks). Callers that need the diagnostic on failure must use
/// [`evaluate_hermes_plugin_readiness`] so the verdict and the text come
/// from ONE evaluation.
pub fn verify_hermes_plugin_ready() -> bool {
    evaluate_hermes_plugin_readiness().ready
}

/// Run `hermes plugins list --enabled --user --json` and capture stdout,
/// killing the child after `timeout` (a hung CLI must never hang an hcom
/// launch). Returns None on spawn failure, non-zero exit, timeout, or read
/// error.
fn run_hermes_json_probe(hermes_bin: &str, timeout: std::time::Duration) -> Option<String> {
    use std::io::Read;
    // Pin the child to the SAME home our config fallback reads: when
    // HERMES_HOME is unset and hcom runs against an isolated workspace,
    // hermes_home_dir() resolves to <root>/.hermes while a bare `hermes`
    // would default to the real ~/.hermes and answer about a different
    // config. (When HERMES_HOME is set, this re-sets the identical value.)
    let home = hermes_home_dir();
    let mut child = std::process::Command::new(hermes_bin)
        .args(["plugins", "list", "--enabled", "--user", "--json"])
        .env("HERMES_HOME", &home)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    // Read concurrently: a JSON payload larger than the pipe buffer would
    // otherwise deadlock against the poll loop below.
    let reader = std::thread::spawn(move || {
        let mut buf = String::new();
        stdout.read_to_string(&mut buf).map(|_| buf).ok()
    });
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return reader.join().ok().flatten(),
            Ok(Some(_)) => return None,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

/// Does the CLI's `plugins list` JSON report the hcom plugin as enabled?
/// Pure so the schema expectations are unit-testable.
fn hermes_plugin_listed_enabled(items: &[Value]) -> bool {
    items.iter().any(|item| {
        item.get("name").and_then(|v| v.as_str()) == Some(PLUGIN_DIR_NAME)
            && item
                .get("removed")
                .is_none_or(|r| r.is_null() || r == &Value::Bool(false))
            && item
                .get("status")
                .and_then(|v| v.as_str())
                .is_none_or(|s| s == "enabled")
    })
}

/// Authoritative enabled-check via the installed Hermes CLI (~1.2s Python
/// startup; only reached after the plugin files exist, from human-paced
/// paths — status display and launch gating). Returns None when the CLI
/// cannot answer (no binary, non-zero exit, timeout, non-JSON stdout) so
/// the caller falls back to the config parse. Version skew (a Hermes
/// without `--json`) surfaces here and degrades gracefully.
fn hermes_cli_reports_plugin_enabled() -> Option<bool> {
    if cfg!(test) {
        // Unit tests pin the config-parse fallback deterministically; the
        // live probe depends on this machine's real Hermes state and is
        // covered by end-to-end verification instead.
        return None;
    }
    // Use the resolved binary path (Windows shim / non-PATH installs)
    // rather than a bare name.
    let hermes_bin = crate::terminal::which_bin("hermes")?;
    let json = run_hermes_json_probe(&hermes_bin, std::time::Duration::from_secs(3))?;
    let Ok(Value::Array(items)) = serde_json::from_str::<Value>(&json) else {
        return None;
    };
    Some(hermes_plugin_listed_enabled(&items))
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
        save_instance_with_tool(db, name, "hermes");
    }

    fn save_instance_with_tool(db: &HcomDb, name: &str, tool: &str) {
        let mut row = serde_json::Map::new();
        row.insert("name".into(), serde_json::json!(name));
        row.insert("tool".into(), serde_json::json!(tool));
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

    // ── Existing-only enrollment / resolve / stale-attribution tests ──────

    fn ctx_without_process(cwd: &str) -> HcomContext {
        HcomContext::from_env(
            &std::collections::HashMap::new(),
            std::path::PathBuf::from(cwd),
        )
    }

    fn ctx_with_process(process_id: &str, cwd: &str) -> HcomContext {
        let mut env = std::collections::HashMap::new();
        env.insert("HCOM_PROCESS_ID".to_string(), process_id.to_string());
        HcomContext::from_env(&env, std::path::PathBuf::from(cwd))
    }

    fn set_row_session(db: &HcomDb, name: &str, session_id: &str) {
        let mut updates = serde_json::Map::new();
        updates.insert("session_id".into(), serde_json::json!(session_id));
        db.update_instance_fields(name, &updates).unwrap();
    }

    fn hermes_row_count(db: &HcomDb) -> i64 {
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM instances WHERE tool = 'hermes'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn json_out(out: &str) -> Value {
        serde_json::from_str(out).expect("handler output must be one JSON document")
    }

    #[test]
    fn existing_only_enrolls_existing_identity_and_never_mints() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-eo-1");
        db.set_session_binding("ses-eo-1", "vega").unwrap();

        let ctx = ctx_without_process("/tmp/project");
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-eo-1"]),
        );
        let doc = json_out(&out);
        assert_eq!(
            doc["name"].as_str(),
            Some("vega"),
            "an enrolled session resolves to its existing identity"
        );
        assert_eq!(hermes_row_count(&db), 1);

        // Unknown session: a null name — expected unenrolled state, and no
        // identity may be minted for it.
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-unknown"]),
        );
        let doc = json_out(&out);
        assert!(doc["name"].is_null(), "unenrolled must answer null: {doc}");
        assert!(doc.get("error").is_none());
        assert_eq!(
            hermes_row_count(&db),
            1,
            "existing-only must never mint an identity"
        );
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_recovers_explicitly_stopped_row() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-eo-2");
        db.set_session_binding("ses-eo-2", "vega").unwrap();
        crate::hooks::common::soft_finalize_session(&db, "vega", "session end", None, true);
        assert!(
            db.get_session_binding("ses-eo-2").unwrap().is_none(),
            "fixture: soft finalize drops the binding, keeps the row"
        );

        let ctx = ctx_without_process("/tmp/project");
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-eo-2"]),
        );
        let doc = json_out(&out);
        assert_eq!(
            doc["name"].as_str(),
            Some("vega"),
            "an explicitly stopped row is recovered by session id"
        );
        assert_eq!(
            db.get_session_binding("ses-eo-2").unwrap().as_deref(),
            Some("vega")
        );
        assert_eq!(hermes_row_count(&db), 1, "recovered, not re-minted");
        cleanup_db(&path);
    }

    #[test]
    #[serial_test::serial]
    fn stale_session_cannot_steal_rebound_row_on_legacy_start() {
        use crate::hooks::test_helpers::isolated_test_env;

        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-a");
        db.set_session_binding("ses-a", "vega").unwrap();
        // vega stops while attributed to ses-a: the snapshot persists, the
        // binding drops, the row survives still naming ses-a.
        crate::hooks::common::soft_finalize_session(&db, "vega", "session end", None, true);
        // vega has since been rebound to ses-b (e.g. `hcom start --as vega`).
        set_row_session(&db, "vega", "ses-b");
        db.set_session_binding("ses-b", "vega").unwrap();

        // A late callback still speaking for ses-a must not reclaim vega: the
        // stopped snapshot is history, not ownership.
        let ctx = HcomContext::from_env(&std::collections::HashMap::new(), home);
        let (_, out) = handle_start(&ctx, &db, &argv_of(&["--session-id", "ses-a"]));
        let doc = json_out(&out);
        let fresh = doc["name"]
            .as_str()
            .expect("legacy start mints a fresh identity");
        assert_ne!(
            fresh, "vega",
            "a stale session must not reclaim vega: {doc}"
        );
        assert_eq!(
            db.get_instance_full("vega").unwrap().unwrap().session_id,
            Some("ses-b".to_string()),
            "vega keeps its current owner's attribution"
        );
        assert_eq!(
            db.get_session_binding("ses-b").unwrap().as_deref(),
            Some("vega"),
            "the current owner's binding is not deleted"
        );
        assert_eq!(
            db.get_session_binding("ses-a").unwrap().as_deref(),
            Some(fresh),
            "the stale session is answered by its fresh identity"
        );

        // The current owner can still write through vega; the stale one can't.
        let (_, out) = handle_status(
            &db,
            &argv_of(&[
                "--name",
                "vega",
                "--status",
                "active",
                "--session-id",
                "ses-b",
            ]),
        );
        assert!(
            json_out(&out).get("error").is_none(),
            "the current owner's write still lands: {out}"
        );
        let (_, out) = handle_status(
            &db,
            &argv_of(&[
                "--name",
                "vega",
                "--status",
                "active",
                "--session-id",
                "ses-a",
            ]),
        );
        assert!(
            json_out(&out)["error"]
                .as_str()
                .unwrap_or_default()
                .contains("no longer attributed"),
            "the stale session's write through vega is refused: {out}"
        );
    }

    #[test]
    fn existing_only_stale_session_cannot_steal_rebound_row() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-a");
        db.set_session_binding("ses-a", "vega").unwrap();
        crate::hooks::common::soft_finalize_session(&db, "vega", "session end", None, true);
        set_row_session(&db, "vega", "ses-b");
        db.set_session_binding("ses-b", "vega").unwrap();

        let ctx = ctx_without_process("/tmp/project");
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-a"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["name"].is_null(),
            "a stale session must not re-enroll into vega: {doc}"
        );
        assert_eq!(
            db.get_instance_full("vega").unwrap().unwrap().session_id,
            Some("ses-b".to_string()),
            "vega's attribution is not overwritten"
        );
        assert_eq!(
            db.get_session_binding("ses-b").unwrap().as_deref(),
            Some("vega"),
            "vega's live binding is not deleted"
        );
        assert!(db.get_session_binding("ses-a").unwrap().is_none());
        assert_eq!(hermes_row_count(&db), 1, "nothing was minted");
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_fallback_refuses_placeholder_for_rebound_snapshot_row() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-a");
        db.set_session_binding("ses-a", "vega").unwrap();
        crate::hooks::common::soft_finalize_session(&db, "vega", "session end", None, true);
        set_row_session(&db, "vega", "ses-b");
        db.set_session_binding("ses-b", "vega").unwrap();

        // A separate hcom-launched interactive CLI placeholder in whose
        // process the stale callback fires: without the snapshot gate,
        // bind_session_to_process would restore the stale snapshot (stealing
        // vega's attribution) before ever binding the placeholder.
        save_hermes_instance(&db, "ph");
        let mut updates = serde_json::Map::new();
        updates.insert("launch_args".into(), serde_json::json!(["chat"]));
        db.update_instance_fields("ph", &updates).unwrap();
        db.set_process_binding("pid-cli", "", "ph").unwrap();
        let mut env = std::collections::HashMap::new();
        env.insert("HCOM_PROCESS_ID".to_string(), "pid-cli".to_string());
        env.insert("HCOM_LAUNCHED".to_string(), "1".to_string());
        let ctx = HcomContext::from_env(&env, std::path::PathBuf::from("/tmp/project"));

        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-a"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["name"].is_null(),
            "placeholder fallback must not restore through a stale snapshot: {doc}"
        );
        assert_eq!(
            db.get_instance_full("vega").unwrap().unwrap().session_id,
            Some("ses-b".to_string()),
            "vega's attribution is not overwritten"
        );
        assert_eq!(
            db.get_session_binding("ses-b").unwrap().as_deref(),
            Some("vega"),
            "vega's live binding is not deleted"
        );
        assert!(
            db.get_instance_full("ph")
                .unwrap()
                .unwrap()
                .session_id
                .is_none(),
            "the placeholder stays unattributed"
        );
        assert!(db.get_session_binding("ses-a").unwrap().is_none());
        assert_eq!(hermes_row_count(&db), 2);
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_stale_binding_to_missing_row_never_mints_or_retires() {
        let (db, path) = setup_test_db();
        // A stale session binding naming a row that no longer exists, with
        // NO stopped snapshot: nothing legitimizes re-creating that row or
        // re-attributing anything to this session. Built the way production
        // gets it — the row vanished without its binding cleanup (the FK
        // blocks constructing it any other way).
        save_hermes_instance(&db, "gone");
        db.set_session_binding("ses-a", "gone").unwrap();
        db.conn()
            .execute_batch(
                "PRAGMA foreign_keys=OFF; DELETE FROM instances WHERE name='gone'; \
                 PRAGMA foreign_keys=ON;",
            )
            .unwrap();
        assert!(db.get_instance_full("gone").unwrap().is_none());
        assert_eq!(
            db.get_session_binding("ses-a").unwrap().as_deref(),
            Some("gone"),
            "fixture: binding outlived its row"
        );

        // A live, unattributed hcom-launched interactive CLI placeholder:
        // without the stale-binding gate, bind_session_to_process would treat
        // 'gone' as a live canonical — minting it back from the placeholder
        // and retiring the placeholder as a session switch.
        save_hermes_instance(&db, "ph");
        let mut updates = serde_json::Map::new();
        updates.insert("launch_args".into(), serde_json::json!(["chat"]));
        db.update_instance_fields("ph", &updates).unwrap();
        db.set_process_binding("pid-cli", "", "ph").unwrap();
        let mut env = std::collections::HashMap::new();
        env.insert("HCOM_PROCESS_ID".to_string(), "pid-cli".to_string());
        env.insert("HCOM_LAUNCHED".to_string(), "1".to_string());
        let ctx = HcomContext::from_env(&env, std::path::PathBuf::from("/tmp/project"));

        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-a"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["name"].is_null(),
            "a stale binding must not enroll through the placeholder: {doc}"
        );
        assert!(
            db.get_instance_full("gone").unwrap().is_none(),
            "the missing row is not recreated"
        );
        let ph = db.get_instance_full("ph").unwrap().unwrap();
        assert_eq!(
            ph.status, "listening",
            "the live placeholder is not retired"
        );
        assert!(
            ph.session_id.is_none(),
            "the placeholder is not attributed to the stale session"
        );
        assert!(
            db.get_session_binding("ses-a").unwrap().is_none()
                || db.get_session_binding("ses-a").unwrap().as_deref() == Some("gone"),
            "the stale binding is not silently rewritten to another row"
        );
        assert_eq!(hermes_row_count(&db), 1, "nothing was minted");
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_fails_closed_when_guard_reads_error() {
        // A placeholder whose every policy guard would pass: only the
        // injected read errors stand between the fallback and a bind.
        let placeholder_fixture = |db: &HcomDb| {
            save_hermes_instance(db, "ph");
            let mut updates = serde_json::Map::new();
            updates.insert("launch_args".into(), serde_json::json!(["chat"]));
            db.update_instance_fields("ph", &updates).unwrap();
            db.set_process_binding("pid-cli", "", "ph").unwrap();
        };
        let mut env = std::collections::HashMap::new();
        env.insert("HCOM_PROCESS_ID".to_string(), "pid-cli".to_string());
        env.insert("HCOM_LAUNCHED".to_string(), "1".to_string());

        // 1. The session binding read errors: an unreadable binding must not
        //    masquerade as an absent one and let enrollment proceed.
        let (db, path) = setup_test_db();
        placeholder_fixture(&db);
        let ctx = HcomContext::from_env(&env, std::path::PathBuf::from("/tmp/project"));
        db.conn()
            .execute_batch("DROP TABLE session_bindings")
            .unwrap();
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-a"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["error"]
                .as_str()
                .unwrap_or_default()
                .contains("enrollment read failed"),
            "an unreadable binding must fail closed with an error: {doc}"
        );
        assert!(
            db.get_instance_full("ph")
                .unwrap()
                .unwrap()
                .session_id
                .is_none(),
            "no bind may happen on a failed guard read"
        );
        cleanup_db(&path);

        // 2. The stopped-snapshot read errors mid-recovery (life events
        //    unreadable): with every other guard green, the placeholder
        //    fallback must still not run.
        let (db, path) = setup_test_db();
        placeholder_fixture(&db);
        let ctx = HcomContext::from_env(&env, std::path::PathBuf::from("/tmp/project"));
        db.conn().execute_batch("DROP TABLE events").unwrap();
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-a"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["error"]
                .as_str()
                .unwrap_or_default()
                .contains("enrollment read failed"),
            "an unreadable snapshot must fail closed with an error: {doc}"
        );
        let ph = db.get_instance_full("ph").unwrap().unwrap();
        assert_eq!(ph.status, "listening", "the placeholder is not touched");
        assert!(
            ph.session_id.is_none(),
            "the placeholder is not bound to the session"
        );
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_fallback_never_recreates_deleted_row_from_snapshot() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "oldid");
        set_row_session(&db, "oldid", "ses-a");
        db.set_session_binding("ses-a", "oldid").unwrap();
        // Hard stop: the row is deleted, but the stopped snapshot naming
        // oldid persists in life events.
        let (_, out) = handle_stop(&db, &argv_of(&["--name", "oldid", "--reason", "done"]));
        assert!(json_out(&out).get("ok").is_some());
        assert!(db.get_instance_full("oldid").unwrap().is_none());

        // A demonstrably hcom-launched interactive CLI placeholder: without
        // the snapshot gate, bind_session_to_process would RECREATE oldid
        // from the snapshot before binding the placeholder — a mint.
        save_hermes_instance(&db, "ph");
        let mut updates = serde_json::Map::new();
        updates.insert("launch_args".into(), serde_json::json!(["chat"]));
        db.update_instance_fields("ph", &updates).unwrap();
        db.set_process_binding("pid-cli", "", "ph").unwrap();
        let mut env = std::collections::HashMap::new();
        env.insert("HCOM_PROCESS_ID".to_string(), "pid-cli".to_string());
        env.insert("HCOM_LAUNCHED".to_string(), "1".to_string());
        let ctx = HcomContext::from_env(&env, std::path::PathBuf::from("/tmp/project"));

        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-a"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["name"].is_null(),
            "existing-only must never recreate a deleted identity: {doc}"
        );
        assert!(
            db.get_instance_full("oldid").unwrap().is_none(),
            "the deleted row stays deleted"
        );
        assert!(db.get_session_binding("ses-a").unwrap().is_none());
        assert_eq!(hermes_row_count(&db), 1, "only the placeholder row exists");
        cleanup_db(&path);
    }

    #[test]
    fn recovery_allows_stopped_row_now_unattributed() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-a");
        db.set_session_binding("ses-a", "vega").unwrap();
        crate::hooks::common::soft_finalize_session(&db, "vega", "session end", None, true);
        // Attribution since cleared (a later rebind of ses-a elsewhere strips
        // it): vega itself is unowned, so recovery may still heal it.
        let mut clear = serde_json::Map::new();
        clear.insert("session_id".into(), serde_json::Value::Null);
        db.update_instance_fields("vega", &clear).unwrap();

        let ctx = ctx_without_process("/tmp/project");
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-a"]),
        );
        let doc = json_out(&out);
        assert_eq!(
            doc["name"].as_str(),
            Some("vega"),
            "an unowned stopped row is still recoverable: {doc}"
        );
        assert_eq!(
            db.get_session_binding("ses-a").unwrap().as_deref(),
            Some("vega")
        );
        assert_eq!(hermes_row_count(&db), 1, "recovered, not re-minted");
        cleanup_db(&path);
    }

    #[test]
    fn hermes_adopt_lock_acquires_and_releases_on_drop() {
        let (db, path) = setup_test_db();
        {
            let lock = acquire_hermes_adopt_lock(&db);
            assert!(lock.is_some(), "lock acquires against a fresh db dir");
        }
        // The dropped guard released the lock: a sequential re-acquisition
        // (the serialization adopt and enrollment both rely on) succeeds.
        assert!(acquire_hermes_adopt_lock(&db).is_some());
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_refuses_foreign_session_binding() {
        let (db, path) = setup_test_db();
        save_instance_with_tool(&db, "luna", "claude");
        set_row_session(&db, "luna", "ses-eo-3");
        db.set_session_binding("ses-eo-3", "luna").unwrap();

        let ctx = ctx_without_process("/tmp/project");
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-eo-3"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["error"]
                .as_str()
                .unwrap_or_default()
                .contains("not a Hermes instance"),
            "a foreign binding must be refused, not adopted: {doc}"
        );
        assert_eq!(
            db.get_session_binding("ses-eo-3").unwrap().as_deref(),
            Some("luna"),
            "the foreign binding is left exactly as it was"
        );
        assert_eq!(hermes_row_count(&db), 0);
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_never_binds_two_serve_sessions_to_one_placeholder() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-a");
        db.set_process_binding("pid-1", "ses-a", "vega").unwrap();

        let ctx = ctx_with_process("pid-1", "/tmp/project");
        // The owning session re-binds idempotently through its placeholder.
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-a"]),
        );
        let doc = json_out(&out);
        assert_eq!(doc["name"].as_str(), Some("vega"));

        // A second serve session in the same process: no attribution, and the
        // placeholder keeps its owning session.
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-b"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["name"].is_null(),
            "a second serve session must not claim the placeholder: {doc}"
        );
        assert_eq!(
            db.get_instance_full("vega")
                .unwrap()
                .unwrap()
                .session_id
                .as_deref(),
            Some("ses-a"),
            "the placeholder keeps its owning session"
        );
        assert!(db.get_session_binding("ses-b").unwrap().is_none());
        cleanup_db(&path);
    }

    #[test]
    fn read_resolve_mode_maps_session_to_name_read_only() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-r");
        db.set_session_binding("ses-r", "vega").unwrap();
        save_instance_with_tool(&db, "luna", "claude");
        set_row_session(&db, "luna", "ses-x");
        db.set_session_binding("ses-x", "luna").unwrap();

        let (_, out) = handle_read(&db, &argv_of(&["--session-id", "ses-r"]));
        let doc = json_out(&out);
        assert_eq!(doc["name"].as_str(), Some("vega"));

        // A foreign binding resolves to null, an unknown session to null.
        let (_, out) = handle_read(&db, &argv_of(&["--session-id", "ses-x"]));
        assert!(json_out(&out)["name"].is_null());
        let (_, out) = handle_read(&db, &argv_of(&["--session-id", "ses-none"]));
        assert!(json_out(&out)["name"].is_null());

        // Read-only: nothing was created, recovered, or rebound.
        assert_eq!(hermes_row_count(&db), 1);
        assert_eq!(
            db.get_session_binding("ses-x").unwrap().as_deref(),
            Some("luna")
        );
        assert_eq!(
            db.get_session_binding("ses-r").unwrap().as_deref(),
            Some("vega")
        );
        cleanup_db(&path);
    }

    #[test]
    fn read_resolve_mode_refuses_ack_format_and_check_without_name() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-r");
        db.set_session_binding("ses-r", "vega").unwrap();

        for flag in ["--ack", "--format", "--check"] {
            let (_, out) = handle_read(&db, &argv_of(&["--session-id", "ses-r", flag]));
            let doc = json_out(&out);
            assert!(
                doc.get("error").is_some(),
                "resolve mode must refuse {flag} without --name: {out}"
            );
        }

        // Clean resolve without --ack/--format/--check still works
        let (_, out) = handle_read(&db, &argv_of(&["--session-id", "ses-r"]));
        let doc = json_out(&out);
        assert_eq!(doc["name"].as_str(), Some("vega"));
        assert!(doc.get("error").is_none());
        cleanup_db(&path);
    }

    #[test]
    fn write_rpcs_refuse_stale_session_attribution() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        // vega has since been rebound to ses-new; a stale callback still
        // speaking for ses-old must not write through it.
        set_row_session(&db, "vega", "ses-new");

        let (_, out) = handle_status(
            &db,
            &argv_of(&[
                "--name",
                "vega",
                "--status",
                "active",
                "--session-id",
                "ses-old",
            ]),
        );
        let doc = json_out(&out);
        assert!(
            doc["error"]
                .as_str()
                .unwrap_or_default()
                .contains("no longer attributed"),
            "stale status must be refused: {doc}"
        );
        assert_eq!(
            db.get_instance_full("vega").unwrap().unwrap().status,
            "listening",
            "refused status must not mutate the row"
        );
        let (_, out) = handle_status(
            &db,
            &argv_of(&[
                "--name",
                "vega",
                "--status",
                "active",
                "--session-id",
                "ses-new",
            ]),
        );
        assert!(json_out(&out).get("error").is_none());

        // Ack: stale claim refused (cursor untouched), current claim advances.
        let e1 = log_broadcast(&db, "sol", "first");
        let e2 = log_broadcast(&db, "sol", "second");
        let (_, out) = handle_read(
            &db,
            &argv_of(&[
                "--name",
                "vega",
                "--ack",
                "--up-to",
                &e2.to_string(),
                "--session-id",
                "ses-old",
            ]),
        );
        let doc = json_out(&out);
        assert!(
            doc["error"]
                .as_str()
                .unwrap_or_default()
                .contains("no longer attributed"),
            "stale ack must be refused: {doc}"
        );
        assert_eq!(
            db.get_instance_full("vega").unwrap().unwrap().last_event_id,
            0,
            "refused ack must not advance the cursor"
        );
        let (_, out) = handle_read(
            &db,
            &argv_of(&[
                "--name",
                "vega",
                "--ack",
                "--up-to",
                &e2.to_string(),
                "--session-id",
                "ses-new",
            ]),
        );
        assert_eq!(json_out(&out)["acked_to"].as_i64(), Some(e2));
        let _ = e1;

        // Read format: stale claim refused BEFORE reading messages.
        let (_, out) = handle_read(
            &db,
            &argv_of(&["--name", "vega", "--format", "--session-id", "ses-old"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["error"]
                .as_str()
                .unwrap_or_default()
                .contains("no longer attributed"),
            "stale read format must be refused: {doc}"
        );

        // Read format with current claim succeeds.
        let (_, out) = handle_read(
            &db,
            &argv_of(&["--name", "vega", "--format", "--session-id", "ses-new"]),
        );
        assert!(json_out(&out).get("error").is_none());

        // Stop: stale claim refused, current claim soft-stops.
        let (_, out) = handle_stop(
            &db,
            &argv_of(&[
                "--name",
                "vega",
                "--reason",
                "stale test",
                "--session-id",
                "ses-old",
            ]),
        );
        let doc = json_out(&out);
        assert!(
            doc["error"]
                .as_str()
                .unwrap_or_default()
                .contains("no longer attributed"),
            "stale stop must be refused: {doc}"
        );
        assert!(
            db.get_instance_full("vega").unwrap().is_some(),
            "refused stop must not finalize the row"
        );
        let (_, out) = handle_stop(
            &db,
            &argv_of(&[
                "--name",
                "vega",
                "--reason",
                "session finalize",
                "--soft",
                "--session-id",
                "ses-new",
            ]),
        );
        assert!(json_out(&out).get("error").is_none());
        assert!(db.get_instance_full("vega").unwrap().is_some());
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_refuses_bound_row_with_different_session_id() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-other");
        db.set_session_binding("ses-req", "vega").unwrap();

        let ctx = ctx_without_process("/tmp/project");
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-req"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["error"]
                .as_str()
                .unwrap_or_default()
                .contains("attributed to session"),
            "mismatched bound session must be refused: {doc}"
        );
        assert_eq!(
            db.get_instance_full("vega")
                .unwrap()
                .unwrap()
                .session_id
                .as_deref(),
            Some("ses-other"),
            "row session_id is not overwritten"
        );
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_never_auto_enrolls_first_session_of_serve() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        // Fresh serve placeholder: no session_id yet, launch_args = ["serve"]
        let mut updates = serde_json::Map::new();
        updates.insert("launch_args".into(), serde_json::json!(["serve"]));
        db.update_instance_fields("vega", &updates).unwrap();
        db.set_process_binding("pid-srv", "", "vega").unwrap();

        let mut env = std::collections::HashMap::new();
        env.insert("HCOM_PROCESS_ID".to_string(), "pid-srv".to_string());
        env.insert("HCOM_LAUNCHED".to_string(), "1".to_string());
        let ctx = HcomContext::from_env(&env, std::path::PathBuf::from("/tmp/project"));

        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-first-serve"]),
        );
        let doc = json_out(&out);
        assert!(
            doc["name"].is_null(),
            "first session of serve must never auto-enroll: {doc}"
        );
        assert!(
            db.get_instance_full("vega")
                .unwrap()
                .unwrap()
                .session_id
                .is_none(),
            "placeholder keeps empty session_id"
        );
        assert!(db.get_session_binding("ses-first-serve").unwrap().is_none());
        cleanup_db(&path);
    }

    #[test]
    fn existing_only_placeholder_fallback_requires_demonstrably_hcom_launched_interactive_cli() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        let mut updates = serde_json::Map::new();
        updates.insert("launch_args".into(), serde_json::json!(["chat"]));
        db.update_instance_fields("vega", &updates).unwrap();
        db.set_process_binding("pid-cli", "", "vega").unwrap();

        // 1. Not launched by hcom: ctx.is_launched is false -> refuses fallback
        let mut env = std::collections::HashMap::new();
        env.insert("HCOM_PROCESS_ID".to_string(), "pid-cli".to_string());
        let unlaunched_ctx = HcomContext::from_env(&env, std::path::PathBuf::from("/tmp/project"));
        let (_, out) = handle_start(
            &unlaunched_ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-cli"]),
        );
        assert!(json_out(&out)["name"].is_null());

        // 2. Launched by hcom, but launch_args is missing (None) -> fails closed
        env.insert("HCOM_LAUNCHED".to_string(), "1".to_string());
        let launched_ctx = HcomContext::from_env(&env, std::path::PathBuf::from("/tmp/project"));
        let mut clear_args = serde_json::Map::new();
        clear_args.insert("launch_args".into(), serde_json::Value::Null);
        db.update_instance_fields("vega", &clear_args).unwrap();
        let (_, out) = handle_start(
            &launched_ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-cli"]),
        );
        assert!(
            json_out(&out)["name"].is_null(),
            "missing launch_args must fail closed"
        );

        // 3. Launched by hcom, but launch_args is malformed JSON -> fails closed
        let mut malformed_args = serde_json::Map::new();
        malformed_args.insert("launch_args".into(), serde_json::json!("{not valid json"));
        db.update_instance_fields("vega", &malformed_args).unwrap();
        let (_, out) = handle_start(
            &launched_ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-cli"]),
        );
        assert!(
            json_out(&out)["name"].is_null(),
            "malformed launch_args must fail closed"
        );

        // 4. Launched by hcom, but launch_args contains serve subcommand -> refuses
        let mut serve_args = serde_json::Map::new();
        serve_args.insert(
            "launch_args".into(),
            serde_json::json!(["serve", "--port", "8000"]),
        );
        db.update_instance_fields("vega", &serve_args).unwrap();
        let (_, out) = handle_start(
            &launched_ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-cli"]),
        );
        assert!(
            json_out(&out)["name"].is_null(),
            "serve launch_args must be refused"
        );

        // 5. Demonstrably launched by hcom interactive CLI: binds successfully
        let mut clean_args = serde_json::Map::new();
        clean_args.insert("launch_args".into(), serde_json::json!(["chat"]));
        db.update_instance_fields("vega", &clean_args).unwrap();
        let (_, out) = handle_start(
            &launched_ctx,
            &db,
            &argv_of(&["--existing-only", "--session-id", "ses-cli"]),
        );
        let doc = json_out(&out);
        assert_eq!(doc["name"].as_str(), Some("vega"));
        assert_eq!(
            db.get_instance_full("vega")
                .unwrap()
                .unwrap()
                .session_id
                .as_deref(),
            Some("ses-cli")
        );
        assert_eq!(
            db.get_session_binding("ses-cli").unwrap().as_deref(),
            Some("vega")
        );
        cleanup_db(&path);
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

    // ── Write-path gating / cursor truthfulness tests ────────────────────

    #[test]
    fn write_paths_refuse_unknown_names_without_emitting_events() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "luna");
        log_broadcast(&db, "sol", "seed");
        let before = db.get_last_event_id();
        assert!(before > 0, "baseline event exists");

        let (_, out) = handle_status(
            &db,
            &argv_of(&["--name", "ghost", "--status", "active", "--context", "work"]),
        );
        assert!(
            serde_json::from_str::<Value>(&out)
                .unwrap()
                .get("error")
                .is_some(),
            "status refuses an unknown name: {out}"
        );
        for args in [vec!["--name", "ghost"], vec!["--name", "ghost", "--soft"]] {
            let (_, out) = handle_stop(&db, &argv_of(&args));
            assert!(
                serde_json::from_str::<Value>(&out)
                    .unwrap()
                    .get("error")
                    .is_some(),
                "stop refuses an unknown name: {out}"
            );
        }
        let (_, out) = handle_read(&db, &argv_of(&["--name", "ghost", "--ack", "--up-to", "9"]));
        assert!(
            serde_json::from_str::<Value>(&out)
                .unwrap()
                .get("error")
                .is_some(),
            "ack refuses an unknown name instead of claiming success: {out}"
        );
        assert!(db.get_instance_full("ghost").unwrap().is_none());

        // beforetool never blocks a tool, but a stray name logs nothing.
        let (_, out) = handle_beforetool(
            &db,
            &argv_of(&["--name", "ghost", "--tool", "Bash", "--input-json", "{}"]),
        );
        assert_eq!(out, r#"{"decision":"allow"}"#);

        // Reads of an unknown name stay permissive (documented empty inbox).
        let (_, out) = handle_read(&db, &argv_of(&["--name", "ghost", "--check"]));
        assert_eq!(
            serde_json::from_str::<Value>(&out).unwrap()["has_unread"].as_bool(),
            Some(false)
        );

        assert_eq!(
            db.get_last_event_id(),
            before,
            "no ghost status/life events may be emitted for unknown names"
        );
        cleanup_db(&path);
    }

    #[test]
    fn write_paths_refuse_names_owned_by_other_tools() {
        let (db, path) = setup_test_db();
        save_instance_with_tool(&db, "other", "claude");
        let e1 = log_broadcast(&db, "sol", "for other");

        let (_, out) = handle_status(&db, &argv_of(&["--name", "other", "--status", "active"]));
        assert!(
            serde_json::from_str::<Value>(&out)
                .unwrap()
                .get("error")
                .is_some(),
            "status refuses a foreign-owned name: {out}"
        );
        let row = db.get_instance_full("other").unwrap().unwrap();
        assert_eq!(row.status, ST_LISTENING, "foreign status untouched");
        assert_eq!(row.status_context, "");

        for args in [vec!["--name", "other"], vec!["--name", "other", "--soft"]] {
            let (_, out) = handle_stop(&db, &argv_of(&args));
            assert!(
                serde_json::from_str::<Value>(&out)
                    .unwrap()
                    .get("error")
                    .is_some(),
                "stop refuses a foreign-owned name: {out}"
            );
        }
        assert!(
            db.get_instance_full("other").unwrap().is_some(),
            "foreign row must not be finalized by the Hermes RPC"
        );

        // Another tool's mailbox is not readable or ackable either.
        let up_to = e1.to_string();
        for args in [
            vec!["--name", "other", "--format"],
            vec!["--name", "other", "--check"],
            vec!["--name", "other", "--ack"],
            vec!["--name", "other", "--ack", "--up-to", up_to.as_str()],
        ] {
            let (_, out) = handle_read(&db, &argv_of(&args));
            assert!(
                serde_json::from_str::<Value>(&out)
                    .unwrap()
                    .get("error")
                    .is_some(),
                "read/ack refuses a foreign-owned name: {out}"
            );
        }
        assert_eq!(
            db.get_instance_full("other")
                .unwrap()
                .unwrap()
                .last_event_id,
            0,
            "foreign cursor untouched"
        );

        let (_, out) = handle_beforetool(&db, &argv_of(&["--name", "other", "--tool", "Bash"]));
        assert_eq!(out, r#"{"decision":"allow"}"#);
        assert_eq!(
            db.get_instance_full("other").unwrap().unwrap().status,
            ST_LISTENING,
            "beforetool skips bookkeeping on foreign names"
        );
        cleanup_db(&path);
    }

    #[test]
    fn status_transition_updates_row_and_emits_one_event() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "luna");
        let before = db.get_last_event_id();
        let (_, out) = handle_status(
            &db,
            &argv_of(&[
                "--name",
                "luna",
                "--status",
                "active",
                "--context",
                "tool:Bash",
                "--detail",
                "running",
            ]),
        );
        assert_eq!(
            serde_json::from_str::<Value>(&out).unwrap()["ok"].as_bool(),
            Some(true)
        );
        let row = db.get_instance_full("luna").unwrap().unwrap();
        assert_eq!(row.status, "active");
        assert_eq!(row.status_context, "tool:Bash");
        assert_eq!(row.status_detail, "running");
        assert_eq!(
            db.get_last_event_id(),
            before + 1,
            "exactly one status event"
        );
        cleanup_db(&path);
    }

    #[test]
    fn ack_up_to_zero_is_monotonic_noop_not_error() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "luna");
        // --format reports up_to 0 for an empty inbox; acking that cursor
        // must be a clean no-op, not an error a plugin sees every idle read.
        let (_, out) = handle_read(&db, &argv_of(&["--name", "luna", "--ack", "--up-to", "0"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(
            doc.get("error").is_none(),
            "zero cursor is not an error: {out}"
        );
        assert_eq!(doc["noop"].as_bool(), Some(true));

        let _ = log_broadcast(&db, "sol", "one");
        let e2 = log_broadcast(&db, "sol", "two");
        let (_, out) = handle_read(
            &db,
            &argv_of(&["--name", "luna", "--ack", "--up-to", &e2.to_string()]),
        );
        assert_eq!(
            serde_json::from_str::<Value>(&out).unwrap()["acked_to"].as_i64(),
            Some(e2)
        );
        // A late ack of the empty cursor cannot regress the advanced one.
        let (_, out) = handle_read(&db, &argv_of(&["--name", "luna", "--ack", "--up-to", "0"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(doc.get("error").is_none());
        assert_eq!(doc["acked_to"].as_i64(), Some(e2));
        assert_eq!(
            db.get_instance_full("luna").unwrap().unwrap().last_event_id,
            e2,
            "cursor never regresses"
        );
        // Negative cursors stay invalid.
        let (_, out) = handle_read(&db, &argv_of(&["--name", "luna", "--ack", "--up-to", "-3"]));
        assert!(
            serde_json::from_str::<Value>(&out)
                .unwrap()
                .get("error")
                .is_some()
        );
        cleanup_db(&path);
    }

    #[test]
    #[serial_test::serial]
    fn start_rejects_empty_session_id() {
        use crate::hooks::test_helpers::isolated_test_env;
        use std::collections::HashMap;

        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = HcomContext::from_env(&HashMap::new(), home);
        let (_, out) = handle_start(&ctx, &db, &argv_of(&["--session-id", ""]));
        assert_eq!(
            serde_json::from_str::<Value>(&out).unwrap()["error"].as_str(),
            Some("Missing --session-id")
        );
        assert_eq!(db.iter_instances_full().unwrap().len(), 0);
    }

    #[test]
    #[serial_test::serial]
    fn ordinary_session_hard_stop_restart_creates_fresh_instance() {
        use crate::hooks::test_helpers::isolated_test_env;
        use std::collections::HashMap;

        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = HcomContext::from_env(&HashMap::new(), home);
        let argv = argv_of(&["--session-id", "ses-hard"]);
        let (_, first) = handle_start(&ctx, &db, &argv);
        let name1 = serde_json::from_str::<Value>(&first).unwrap()["name"]
            .as_str()
            .expect("ordinary session bound")
            .to_string();

        let (_, out) = handle_stop(&db, &argv_of(&["--name", &name1, "--reason", "done"]));
        assert_eq!(
            serde_json::from_str::<Value>(&out).unwrap()["ok"].as_bool(),
            Some(true)
        );
        assert!(
            db.get_instance_full(&name1).unwrap().is_none(),
            "hard stop deletes the row"
        );
        assert!(db.get_session_binding("ses-hard").unwrap().is_none());

        // The stopped snapshot lives on in life events, but the adopt path
        // must not resurrect a deleted row: a restart gets a fresh instance.
        let (_, second) = handle_start(&ctx, &db, &argv);
        let name2 = serde_json::from_str::<Value>(&second).unwrap()["name"]
            .as_str()
            .expect("session rebound")
            .to_string();
        assert_ne!(name2, name1);
        assert_eq!(db.iter_instances_full().unwrap().len(), 1);
        assert_eq!(
            db.get_session_binding("ses-hard").unwrap().as_deref(),
            Some(name2.as_str())
        );
    }

    #[test]
    #[serial_test::serial]
    fn start_persists_transcript_path_and_directory() {
        use crate::hooks::test_helpers::isolated_test_env;
        use std::collections::HashMap;

        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let workspace = home.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let db = HcomDb::open().unwrap();
        let ctx = HcomContext::from_env(&HashMap::new(), workspace.clone());
        let transcript = home.join("transcript.jsonl");
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&[
                "--session-id",
                "ses-meta",
                "--transcript-path",
                transcript.to_str().unwrap(),
                "--cwd",
                workspace.to_str().unwrap(),
            ]),
        );
        let name = serde_json::from_str::<Value>(&out).unwrap()["name"]
            .as_str()
            .expect("ordinary session bound")
            .to_string();
        let row = db.get_instance_full(&name).unwrap().unwrap();
        assert_eq!(row.tool, "hermes");
        assert_eq!(row.session_id.as_deref(), Some("ses-meta"));
        assert_eq!(row.transcript_path, transcript.to_str().unwrap());
        assert_eq!(row.directory, workspace.to_str().unwrap());
    }

    #[test]
    #[serial_test::serial]
    fn start_derives_transcript_path_from_hermes_home_when_unspecified() {
        use crate::hooks::test_helpers::isolated_test_env;
        use std::collections::HashMap;

        let (_tmp, _hcom_dir, home, _guard) = isolated_test_env();
        let profile_dir = home.join("custom_profile");
        std::fs::create_dir_all(&profile_dir).unwrap();
        let state_db = profile_dir.join("state.db");
        std::fs::write(&state_db, b"").unwrap();

        let mut env = HashMap::new();
        env.insert(
            "HERMES_HOME".to_string(),
            profile_dir.to_str().unwrap().to_string(),
        );
        let workspace = home.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();

        let db = HcomDb::open().unwrap();
        let ctx = HcomContext::from_env(&env, workspace.clone());

        // Call handle_start without --transcript-path
        let (_, out) = handle_start(
            &ctx,
            &db,
            &argv_of(&[
                "--session-id",
                "ses-derived",
                "--cwd",
                workspace.to_str().unwrap(),
            ]),
        );
        let name = serde_json::from_str::<Value>(&out).unwrap()["name"]
            .as_str()
            .expect("ordinary session bound")
            .to_string();
        let row = db.get_instance_full(&name).unwrap().unwrap();
        assert_eq!(row.tool, "hermes");
        assert_eq!(row.session_id.as_deref(), Some("ses-derived"));
        assert_eq!(row.transcript_path, state_db.to_str().unwrap());
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

    // ── Plugin readiness / config-enable tests ──────────────────────────

    fn install_fake_hermes_plugin(home: &std::path::Path) {
        let dir = home.join("plugins").join(PLUGIN_DIR_NAME);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("__init__.py"), "# test plugin\n").unwrap();
        std::fs::write(dir.join("plugin.yaml"), "name: hcom\nversion: \"0.2.0\"\n").unwrap();
    }

    fn write_hermes_config(home: &std::path::Path, body: &str) {
        std::fs::write(home.join("config.yaml"), body).unwrap();
    }

    /// Point the hermes home at `home` (custom-home coverage) and clear
    /// safe mode. EnvGuard restores HERMES_HOME on drop.
    fn hermes_home_guard(home: &std::path::Path) -> crate::hooks::test_helpers::EnvGuard {
        let guard = crate::hooks::test_helpers::EnvGuard::new();
        unsafe {
            std::env::set_var("HERMES_HOME", home);
            std::env::remove_var("HERMES_SAFE_MODE");
        }
        guard
    }

    #[test]
    #[serial_test::serial]
    fn plugin_ready_when_enabled_in_config() {
        let temp = tempfile::tempdir().unwrap();
        install_fake_hermes_plugin(temp.path());
        write_hermes_config(
            temp.path(),
            "model:\n  default: glm\nplugins:\n  disabled: []\n  enabled:\n    - current-time\n    - hcom\nagent:\n  max_turns: 60\n",
        );
        let _guard = hermes_home_guard(temp.path());
        assert!(verify_hermes_plugin_installed());
        assert!(evaluate_hermes_plugin_readiness().ready);
        assert!(verify_hermes_plugin_ready());
    }

    #[test]
    #[serial_test::serial]
    fn plugin_not_ready_when_present_but_absent_from_allow_list() {
        let temp = tempfile::tempdir().unwrap();
        install_fake_hermes_plugin(temp.path());
        write_hermes_config(
            temp.path(),
            "plugins:\n  disabled: []\n  enabled:\n    - current-time\n",
        );
        let _guard = hermes_home_guard(temp.path());
        // Hermes plugins are opt-in: present files are NOT ready.
        assert!(verify_hermes_plugin_installed());
        let readiness = evaluate_hermes_plugin_readiness();
        assert!(!readiness.ready);
        assert!(!verify_hermes_plugin_ready());
        let diag = readiness.diagnostic;
        assert!(diag.contains("hermes plugins enable hcom"), "{diag}");
        assert!(
            diag.contains(temp.path().join("config.yaml").to_str().unwrap()),
            "{diag}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn plugin_disabled_veto_beats_enabled_entry() {
        let temp = tempfile::tempdir().unwrap();
        install_fake_hermes_plugin(temp.path());
        write_hermes_config(
            temp.path(),
            "plugins:\n  disabled:\n    - hcom\n  enabled:\n    - hcom\n",
        );
        let _guard = hermes_home_guard(temp.path());
        let readiness = evaluate_hermes_plugin_readiness();
        assert!(!readiness.ready && !verify_hermes_plugin_ready());
        assert!(readiness.diagnostic.contains("disabled"), "{readiness:?}");
    }

    #[test]
    #[serial_test::serial]
    fn plugin_not_ready_when_config_missing_or_without_plugins_block() {
        let temp = tempfile::tempdir().unwrap();
        install_fake_hermes_plugin(temp.path());
        let _guard = hermes_home_guard(temp.path());
        // No config.yaml at all.
        assert!(!evaluate_hermes_plugin_readiness().ready);
        assert!(!verify_hermes_plugin_ready());

        // Config exists but has no plugins block: nothing is enabled.
        write_hermes_config(temp.path(), "model:\n  default: glm\n");
        let readiness = evaluate_hermes_plugin_readiness();
        assert!(!readiness.ready);
        assert!(
            readiness.diagnostic.contains("hermes plugins enable hcom"),
            "{readiness:?}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn plugin_not_ready_when_files_missing_even_if_enabled() {
        let temp = tempfile::tempdir().unwrap();
        write_hermes_config(temp.path(), "plugins:\n  enabled:\n    - hcom\n");
        let _guard = hermes_home_guard(temp.path());
        assert!(!verify_hermes_plugin_installed());
        // The config itself would enable hcom; only the files gate fails.
        let (enabled, _) = parse_hermes_plugin_lists(
            &std::fs::read_to_string(temp.path().join("config.yaml")).unwrap(),
        )
        .unwrap();
        assert!(enabled.iter().any(|n| n == "hcom"));
        let readiness = evaluate_hermes_plugin_readiness();
        assert!(!readiness.ready);
        let diag = readiness.diagnostic;
        assert!(
            diag.contains(
                temp.path()
                    .join("plugins")
                    .join("hcom")
                    .join("plugin.yaml")
                    .to_str()
                    .unwrap()
            ),
            "diagnostic names the missing plugin path: {diag}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn plugin_safe_mode_suppresses_even_when_enabled() {
        let temp = tempfile::tempdir().unwrap();
        install_fake_hermes_plugin(temp.path());
        write_hermes_config(temp.path(), "plugins:\n  enabled:\n    - hcom\n");
        let _guard = hermes_home_guard(temp.path());
        unsafe { std::env::set_var("HERMES_SAFE_MODE", "1") };
        let readiness = evaluate_hermes_plugin_readiness();
        assert!(!readiness.ready && !verify_hermes_plugin_ready());
        assert!(readiness.diagnostic.contains("HERMES_SAFE_MODE"));
        unsafe { std::env::remove_var("HERMES_SAFE_MODE") };
        assert!(verify_hermes_plugin_ready());
    }

    #[test]
    fn plugin_list_parser_handles_yaml_shapes() {
        // Hermes's indented dump (as seen in real config.yaml).
        let (enabled, disabled) = parse_hermes_plugin_lists(
            "plugins:\n  disabled: []\n  enabled:\n    - current-time\n    - hcom\n  entries:\n    hcom:\n      allow_tool_override: false\n",
        )
        .unwrap();
        assert!(enabled.contains(&"hcom".to_string()));
        assert!(disabled.is_empty());

        // PyYAML default style: items at the key's own indent.
        let (enabled, _) = parse_hermes_plugin_lists("plugins:\n  enabled:\n  - hcom\n").unwrap();
        assert_eq!(enabled, vec!["hcom".to_string()]);

        // Flow lists and quoted entries.
        let (enabled, disabled) = parse_hermes_plugin_lists(
            "plugins:\n  enabled: ['hcom', llm-usage]\n  disabled: [\"current-time\"]\n",
        )
        .unwrap();
        assert_eq!(enabled, vec!["hcom".to_string(), "llm-usage".to_string()]);
        assert_eq!(disabled, vec!["current-time".to_string()]);

        // Equal-indent items must not be mistaken for keys; later top-level
        // keys end the block.
        let (enabled, _) = parse_hermes_plugin_lists(
            "other:\n  enabled: noise\nplugins:\n  disabled:\n  - x\n  enabled:\n  - hcom\nknown_plugin_toolsets:\n  cli:\n    - spotify\n",
        )
        .unwrap();
        assert_eq!(enabled, vec!["hcom".to_string()]);

        // Missing block / non-list enabled value: nothing enabled (opt-in).
        assert!(parse_hermes_plugin_lists("model:\n  default: glm\n").is_none());
        assert!(parse_hermes_plugin_lists("plugins:\n  enabled: yes\n").is_none());

        // A nested mapping under `enabled` is NOT a name list: its items
        // must never leak into the allow-list (Hermes treats a non-list
        // enabled as "nothing enabled").
        assert!(
            parse_hermes_plugin_lists("plugins:\n  enabled:\n    nested:\n      - hcom\n")
                .is_none()
        );
        // Symmetric on the veto list: a nested disabled mapping is no veto,
        // so a plainly-enabled hcom stays enabled.
        let (enabled, disabled) = parse_hermes_plugin_lists(
            "plugins:\n  enabled:\n    - hcom\n  disabled:\n    nested:\n      - hcom\n",
        )
        .unwrap();
        assert!(enabled.iter().any(|n| n == "hcom"));
        assert!(disabled.is_empty());
    }

    #[test]
    fn plugin_list_json_matcher_reports_hcom_enabled() {
        let items = vec![
            serde_json::json!({"name": "current-time", "status": "enabled"}),
            serde_json::json!({"name": "hcom", "status": "enabled", "removed": null}),
        ];
        assert!(hermes_plugin_listed_enabled(&items));
        // Missing status is accepted: the probe pre-filters with --enabled.
        assert!(hermes_plugin_listed_enabled(&[
            serde_json::json!({"name": "hcom"})
        ]));
    }

    #[test]
    fn plugin_list_json_matcher_rejects_other_states() {
        // Wrong name, disabled status, or a removal flag never report ready.
        assert!(!hermes_plugin_listed_enabled(&[serde_json::json!({
            "name": "other", "status": "enabled"
        })]));
        assert!(!hermes_plugin_listed_enabled(&[serde_json::json!({
            "name": "hcom", "status": "disabled"
        })]));
        assert!(!hermes_plugin_listed_enabled(&[serde_json::json!({
            "name": "hcom", "status": "enabled", "removed": true
        })]));
    }
}
