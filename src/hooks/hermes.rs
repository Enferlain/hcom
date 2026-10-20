//! Hermes Agent hook handlers — argv-based lifecycle plus Python plugin.
//!
//! The hcom-owned Hermes backend extension uses the argv-based RPC
//! subcommands dispatched here:
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
//! been rebound to a different session). A VERIFIED rebind answers the
//! stale-attribution refusal; a transient read error answers a retryable
//! error instead — the extension's listener teardown keys on the stale text,
//! so an unreadable row must never pose as a verified rebind. Absent, the
//! legacy contract holds.
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
        if new_id > 0
            && let Err(e) = db.advance_instance_cursor(instance_name, new_id)
        {
            log_error(
                "hooks",
                "hermes.init_cursor",
                &format!("instance={instance_name} err={e}"),
            );
        }
    }
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

/// Stale-callback claim check for write RPCs: a caller that carries its
/// Hermes session id (`--session-id`) may only touch a row CURRENTLY
/// attributed to that session. A session whose identity was since rebound
/// elsewhere (serve rebind, `hcom start --as`, or the row's deletion) must
/// not have its old name statused, acked, or finalized through a stale
/// per-session callback. Absent/empty session id is the legacy contract: no
/// claim, nothing to validate.
///
/// The three outcomes stay distinguishable because the backend extension
/// keys permanent listener teardown off the stale-attribution refusal: a
/// transient DB read error must NEVER surface as that text, or the plugin
/// would tear down a still-valid listener over a retryable failure.
enum SessionClaim {
    /// Empty `--session-id`: the legacy no-claim contract.
    Unclaimed,
    /// The row was read; `matches` says whether it is still attributed to
    /// the claiming session (a read of a MISSING row is verified, not an
    /// error — the row's absence is itself the answer).
    Verified { matches: bool },
    /// The row could not be read: attribution is UNKNOWN. Not a verified
    /// rebind — retryable, never a teardown signal.
    Unreadable(anyhow::Error),
}

fn check_session_claim(db: &HcomDb, name: &str, session_id: &str) -> SessionClaim {
    if session_id.is_empty() {
        return SessionClaim::Unclaimed;
    }
    match db.get_instance_full(name) {
        Ok(Some(row)) => SessionClaim::Verified {
            matches: row.session_id.as_deref() == Some(session_id),
        },
        Ok(None) => SessionClaim::Verified { matches: false },
        Err(error) => SessionClaim::Unreadable(error),
    }
}

/// Claim gate for write RPCs: `Some(reply)` refuses the call. A verified
/// rebind (or verified-missing row) answers the stale-attribution refusal;
/// an unreadable row answers a retryable read error instead — the Python
/// listener treats the stale text as a verified rebind and permanently tears
/// down, so conflating Err with stale would destroy a live listener over a
/// transient failure.
fn claimed_write_gate(db: &HcomDb, name: &str, claimed_session: &str) -> Option<String> {
    match check_session_claim(db, name, claimed_session) {
        SessionClaim::Unclaimed | SessionClaim::Verified { matches: true } => None,
        SessionClaim::Verified { matches: false } => {
            Some(stale_attribution_error(name, claimed_session))
        }
        SessionClaim::Unreadable(error) => Some(
            serde_json::json!({
                "error": format!(
                    "Hermes instance '{name}' could not be read ({error}); \
                     transient read failure — retry"
                )
            })
            .to_string(),
        ),
    }
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
    bootstrap::get_bootstrap(db, ctx, instance_name, "hermes")
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
                    match instance_binding::bind_session_to_process_for_tool(
                        db,
                        &session_id,
                        Some(process_id),
                        "hermes",
                        false,
                    ) {
                        instance_binding::ToolCheckedBind::Bound(name) => Some(name),
                        instance_binding::ToolCheckedBind::Unbound
                        | instance_binding::ToolCheckedBind::Rejected => None,
                    }
                }),
            },
        }
    } else if let Some(process_id) = ctx.process_id.as_deref() {
        match instance_binding::bind_session_to_process_for_tool(
            db,
            &session_id,
            Some(process_id),
            "hermes",
            false,
        ) {
            instance_binding::ToolCheckedBind::Bound(name) => Some(name),
            instance_binding::ToolCheckedBind::Unbound
            | instance_binding::ToolCheckedBind::Rejected => None,
        }
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
    crate::relay::worker::ensure_worker();

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
    // Claim gate first: a claimed call must surface a transient read error
    // as retryable, not as Unknown (which is_hermes_instance would mask it
    // into) and never as the teardown-triggering stale text.
    let claimed_session = parse_flag(argv, "--session-id").unwrap_or_default();
    if let Some(reply) = claimed_write_gate(db, &name, &claimed_session) {
        return (0, reply);
    }
    if !is_hermes_instance(db, &name) {
        return (
            0,
            serde_json::json!({"error": format!("Unknown Hermes instance: {name}")}).to_string(),
        );
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
    if format_mode && let Some(reply) = claimed_write_gate(db, &name, &claimed_session) {
        // Claim gate: a transient read error surfaces as retryable, never as
        // the stale-attribution refusal the Python listener teardown keys on.
        return (0, reply);
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
        // Claim gate first (see the format gate above): an ack is a write, a
        // stale per-session callback must not advance the cursor of a name
        // since rebound to another session, and a transient read error must
        // not pose as that verified rebind.
        if let Some(reply) = claimed_write_gate(db, &name, &claimed_session) {
            return (0, reply);
        }
        // Acks write a cursor: require a real hermes row. Without one the
        // UPDATE below would touch nothing while the reply claimed success.
        if !is_hermes_instance(db, &name) {
            return (
                0,
                serde_json::json!({"error": format!("Unknown Hermes instance: {name}")})
                    .to_string(),
            );
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
            if let Err(e) = db.advance_instance_cursor(&name, ack_id) {
                log_error(
                    "hooks",
                    "hermes.ack_cursor",
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
        if let Err(e) = db.advance_instance_cursor(&name, ack_id) {
            log_error(
                "hooks",
                "hermes.ack_cursor",
                &format!("instance={name} err={e}"),
            );
            return (
                0,
                serde_json::json!({"error": format!("Failed to advance cursor: {e}")}).to_string(),
            );
        }
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
    // Claim gate first (see handle_status): finalizing is the strongest
    // write — a stale per-session callback must not stop a name that has
    // since been rebound to a different session, and a transient read error
    // must not pose as that verified rebind.
    let claimed_session = parse_flag(argv, "--session-id").unwrap_or_default();
    if let Some(reply) = claimed_write_gate(db, &name, &claimed_session) {
        return (0, reply);
    }
    if !is_hermes_instance(db, &name) {
        return (
            0,
            serde_json::json!({"error": format!("Unknown Hermes instance: {name}")}).to_string(),
        );
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
/// inside itself. Without it, fall back to the default home (`$HOME/.hermes`).
/// Hermes resolves `<home>/config.yaml` and `<home>/plugins` from the same root
/// (hermes_cli config.get_config_path / plugins_discovery).
fn hermes_home_dir() -> std::path::PathBuf {
    if let Some(home) = std::env::var("HERMES_HOME").ok().filter(|s| !s.is_empty()) {
        return std::path::PathBuf::from(home);
    }
    current_home_dir().join(".hermes")
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

/// Evaluate Hermes plugin readiness ONCE: the hcom-owned backend files exist
/// AND Hermes actually enables the plugin. Files alone are NOT
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
            "Hermes backend files are missing at {}. Run: hcom hooks add hermes",
            get_hermes_plugin_path().display()
        ));
    }
    // hcom owns the backend extension and must never load someone else's
    // plugin in its place: an unmanaged/foreign copy at the plugin path is
    // not ready even when it is enabled — and hcom will not auto-overwrite
    // it. Tell the user exactly how to migrate by hand.
    match backend_ownership() {
        Ok(BackendOwnership::Ours) => {}
        _ => {
            return not_ready(format!(
                "A plugin that is not hcom-managed is installed at {}. hcom will not overwrite \
                 it. Back it up if needed, remove it (e.g. hermes plugins remove hcom), then \
                 run: hcom hooks add hermes",
                backend_plugin_dir().display()
            ));
        }
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

/// Full BACKEND readiness for hook-install and launch gating: the plugin
/// loads (owned, present, enabled — [`evaluate_hermes_plugin_readiness`])
/// AND the gateway-injection grant is in place. Without the grant the
/// extension cannot inject visible user turns into `hermes serve` /
/// non-CLI sessions, so hooks-add must not report "already installed" and
/// launch must not skip setup over a missing grant.
pub fn evaluate_hermes_backend_readiness() -> HermesPluginReadiness {
    let plugin = evaluate_hermes_plugin_readiness();
    if !plugin.ready {
        return plugin;
    }
    match hermes_gateway_injection_enabled() {
        Some(true) => plugin,
        Some(false) => not_ready(
            "Gateway injection is denied for the hcom plugin entry \
             (plugins.entries.hcom.allow_gateway_injection: false); non-CLI/serve sessions \
             cannot receive injected turns. Run: hcom hooks add hermes"
                .to_string(),
        ),
        None => not_ready(
            "Gateway injection is not granted (plugins.entries.hcom.allow_gateway_injection); \
             non-CLI/serve sessions cannot receive injected turns. Run: hcom hooks add hermes"
                .to_string(),
        ),
    }
}

/// Backend readiness as a plain bool — the Hermes arm of the hooks surface
/// (`Tool::verify_hooks_installed`) and the launch gate.
pub fn verify_hermes_backend_ready() -> bool {
    evaluate_hermes_backend_readiness().ready
}

// ── Backend extension installer ──────────────────────────────────────────
//
// hcom owns the Hermes backend extension (`src/hermes_backend/hcom`): it is
// the delivery surface for both hcom-launched CLI sessions and `hermes
// serve`. `hcom hooks add hermes` copies it into `<hermes home>/plugins/hcom`
// and enables it through Hermes config; nothing else is installed — no
// Desktop/UI plugin, no other package, path, manifest, or migration.

/// Embedded sources of the hcom-owned backend extension. Only these two
/// files are ever installed (never the test module or pycache).
const BACKEND_INIT_SOURCE: &str = include_str!("../hermes_backend/hcom/__init__.py");
const BACKEND_MANIFEST_SOURCE: &str = include_str!("../hermes_backend/hcom/plugin.yaml");

/// Marker file our installer writes inside the plugin directory. Only its
/// own writes ever create it, so its presence is the sentinel that a
/// (possibly drifted) copy is hcom-managed and may be updated in place.
/// Without it, only a byte-exact copy of the current sources counts as ours
/// — anything else at the plugin path is foreign and never touched.
const BACKEND_MANAGED_MARKER: &str = ".hcom-managed";
const BACKEND_MARKER_CONTENT: &str = "hcom-managed backend extension (hcom hooks add hermes)\n";

fn backend_plugin_dir() -> std::path::PathBuf {
    hermes_plugin_root().join(PLUGIN_DIR_NAME)
}

/// A plugin path redirected outside its visible directory is never ours to
/// update or remove. On Windows, junctions are reparse points but are not
/// reported by `Path::is_symlink()`.
fn backend_path_is_indirect(path: &std::path::Path) -> bool {
    if path.is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        return std::fs::symlink_metadata(path)
            .is_ok_and(|metadata| metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0);
    }
    #[cfg(not(windows))]
    false
}

/// Ownership state of whatever sits at the backend plugin path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendOwnership {
    /// Nothing installed there.
    Absent,
    /// hcom-managed: our marker is present, or the files are a byte-exact
    /// copy of the current embedded sources (adoption of a fresh copy).
    Ours,
    /// Someone else's plugin (or unknown/unexpected content, incl. symlinked
    /// paths): hcom must never write through or delete it.
    Foreign,
}

/// Decide ownership of the plugin directory. A symlinked directory or a
/// symlinked file inside it is Foreign by definition — hcom does not follow
/// or rewrite symlinks into user-managed locations.
fn backend_ownership() -> Result<BackendOwnership, String> {
    let dir = backend_plugin_dir();
    if backend_path_is_indirect(&dir) {
        return Ok(BackendOwnership::Foreign);
    }
    if !dir.exists() {
        return Ok(BackendOwnership::Absent);
    }
    let manifest = dir.join("plugin.yaml");
    let init = dir.join("__init__.py");
    if backend_path_is_indirect(&manifest) || backend_path_is_indirect(&init) {
        return Ok(BackendOwnership::Foreign);
    }
    // The marker must be a REGULAR file with our exact content: a symlinked
    // or edited marker must never make someone else's directory look
    // hcom-managed. A valid marker also lets us repair a first install that
    // was interrupted before either runtime file was written.
    let marker = dir.join(BACKEND_MANAGED_MARKER);
    if backend_path_is_indirect(&marker) {
        return Ok(BackendOwnership::Foreign);
    }
    if marker.exists() {
        return Ok(
            if std::fs::read_to_string(&marker)
                .is_ok_and(|content| content == BACKEND_MARKER_CONTENT)
            {
                BackendOwnership::Ours
            } else {
                BackendOwnership::Foreign
            },
        );
    }
    if !manifest.exists() && !init.exists() {
        // An interrupted first install may have created only an empty
        // directory. Nothing there can be someone else's plugin, so a retry
        // may safely adopt it. Any unknown content remains foreign.
        return Ok(
            if std::fs::read_dir(&dir)
                .map_err(|e| e.to_string())?
                .next()
                .is_none()
            {
                BackendOwnership::Absent
            } else {
                BackendOwnership::Foreign
            },
        );
    }
    let manifest_exact =
        std::fs::read_to_string(&manifest).is_ok_and(|c| c == BACKEND_MANIFEST_SOURCE);
    let init_exact = std::fs::read_to_string(&init).is_ok_and(|c| c == BACKEND_INIT_SOURCE);
    if manifest_exact && init_exact {
        return Ok(BackendOwnership::Ours);
    }
    Ok(BackendOwnership::Foreign)
}

/// Public ownership probe for status reporting.
pub fn backend_ownership_pub() -> Result<BackendOwnership, String> {
    backend_ownership()
}

fn foreign_plugin_error() -> String {
    format!(
        "a plugin that is not hcom-managed exists at {}; hcom will not overwrite or remove it — \
         back it up and remove it (e.g. hermes plugins remove hcom), then run: hcom hooks add hermes",
        backend_plugin_dir().display()
    )
}

/// Install (or update) the backend extension files. Refuses to touch a
/// foreign plugin at the same path. `Ok(true)` when files were written,
/// `Ok(false)` when already current.
pub fn install_hermes_backend_files() -> Result<bool, String> {
    if backend_ownership()? == BackendOwnership::Foreign {
        return Err(foreign_plugin_error());
    }
    let dir = backend_plugin_dir();
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let manifest_current = std::fs::read_to_string(dir.join("plugin.yaml"))
        .map(|c| c == BACKEND_MANIFEST_SOURCE)
        .unwrap_or(false);
    let init_current = std::fs::read_to_string(dir.join("__init__.py"))
        .map(|c| c == BACKEND_INIT_SOURCE)
        .unwrap_or(false);
    let marker_present = dir.join(BACKEND_MANAGED_MARKER).exists();
    if manifest_current && init_current && marker_present {
        return Ok(false);
    }
    // Claim ownership before changing either runtime file. Each replacement
    // is staged outside the plugin directory and renamed into place, so an
    // interrupted update cannot leave a torn, discoverable Python module.
    // Write the manifest last because Hermes discovers a directory plugin
    // through plugin.yaml.
    if !marker_present {
        atomic_write_backend_file(&dir, BACKEND_MANAGED_MARKER, BACKEND_MARKER_CONTENT)?;
    }
    if !init_current {
        atomic_write_backend_file(&dir, "__init__.py", BACKEND_INIT_SOURCE)?;
    }
    if !manifest_current {
        atomic_write_backend_file(&dir, "plugin.yaml", BACKEND_MANIFEST_SOURCE)?;
    }
    Ok(true)
}

fn atomic_write_backend_file(dir: &std::path::Path, name: &str, body: &str) -> Result<(), String> {
    use std::io::Write;

    // Keep temporary files outside the plugin directory: an interrupted
    // write must not become part of the discovered plugin or its removal.
    let parent = dir
        .parent()
        .ok_or("backend plugin directory has no parent")?;
    let mut staged = tempfile::Builder::new()
        .prefix(".hcom-backend-")
        .tempfile_in(parent)
        .map_err(|e| e.to_string())?;
    staged
        .write_all(body.as_bytes())
        .and_then(|_| staged.as_file().sync_all())
        .map_err(|e| e.to_string())?;
    staged.persist(dir.join(name)).map_err(|e| e.to_string())?;
    Ok(())
}

/// The first file inside the plugin directory that is not one of our known
/// files (`plugin.yaml`, `__init__.py`, the managed marker, and Python's
/// own compiled `__init__` cache), if any. Unreadable entries fail closed.
fn backend_unexpected_file(dir: &std::path::Path) -> Result<Option<String>, String> {
    let entries = std::fs::read_dir(dir).map_err(|e| e.to_string())?;
    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "__pycache__" {
            if !entry.file_type().map_err(|e| e.to_string())?.is_dir() {
                return Ok(Some(name.to_string()));
            }
            for cached in std::fs::read_dir(entry.path()).map_err(|e| e.to_string())? {
                let cached = cached.map_err(|e| e.to_string())?;
                let cached_name = cached.file_name();
                let cached_name = cached_name.to_string_lossy();
                let compiled_init = cached_name.starts_with("__init__.cpython-")
                    && cached_name.ends_with(".pyc")
                    && cached.file_type().map_err(|e| e.to_string())?.is_file();
                if !compiled_init {
                    return Ok(Some(format!("__pycache__/{cached_name}")));
                }
            }
        } else if name != BACKEND_MANAGED_MARKER
            && !matches!(name.as_ref(), "plugin.yaml" | "__init__.py")
        {
            return Ok(Some(name.to_string()));
        }
    }
    Ok(None)
}

fn unexpected_file_error(dir: &std::path::Path, name: &str) -> String {
    format!(
        "unexpected file '{}' inside {}; hcom will not delete the directory — \
         inspect and remove it manually, then retry",
        name,
        dir.display()
    )
}

/// Remove the backend extension files. Refuses to remove a foreign plugin,
/// and refuses to delete a directory holding anything beyond our known
/// files. `Ok(true)` when the directory was removed, `Ok(false)` when
/// absent.
pub fn remove_hermes_backend_files() -> Result<bool, String> {
    if backend_ownership()? == BackendOwnership::Foreign {
        return Err(foreign_plugin_error());
    }
    let dir = backend_plugin_dir();
    if !dir.exists() {
        return Ok(false);
    }
    if let Some(name) = backend_unexpected_file(&dir)? {
        return Err(unexpected_file_error(&dir, &name));
    }
    std::fs::remove_dir_all(&dir).map_err(|e| e.to_string())?;
    Ok(true)
}

/// Files present AND byte-current with the embedded sources (drives the
/// "update available" nuance in status output).
pub fn hermes_backend_files_current() -> bool {
    let dir = backend_plugin_dir();
    let manifest = std::fs::read_to_string(dir.join("plugin.yaml"))
        .map(|c| c == BACKEND_MANIFEST_SOURCE)
        .unwrap_or(false);
    let init = std::fs::read_to_string(dir.join("__init__.py"))
        .map(|c| c == BACKEND_INIT_SOURCE)
        .unwrap_or(false);
    manifest && init
}

/// Run `hermes <args...>` against OUR resolved hermes home. Returns
/// `(success, stdout, stderr)` — callers classify "not set" style non-zero
/// replies themselves (that notice arrives on stderr).
fn run_hermes_cli_raw(args: &[&str]) -> Result<(bool, String, String), String> {
    let hermes_bin = crate::terminal::which_bin("hermes")
        .ok_or_else(|| "the hermes CLI was not found on PATH; install Hermes first".to_string())?;
    run_hermes_command(&hermes_bin, args, std::time::Duration::from_secs(10))
}

/// Capture both output pipes concurrently and bound the entire CLI call.
/// Config reads/writes run on launch and status paths, so a wedged Hermes
/// binary must not hang hcom indefinitely.
fn run_hermes_command(
    hermes_bin: &str,
    args: &[&str],
    timeout: std::time::Duration,
) -> Result<(bool, String, String), String> {
    use std::io::Read;
    use std::sync::mpsc;

    let home = hermes_home_dir();
    let mut child = std::process::Command::new(hermes_bin)
        .args(args)
        .env("HERMES_HOME", &home)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to run hermes {}: {e}", args.join(" ")))?;
    let mut stdout = child.stdout.take().ok_or("missing Hermes stdout pipe")?;
    let mut stderr = child.stderr.take().ok_or("missing Hermes stderr pipe")?;
    let (stdout_tx, stdout_rx) = mpsc::channel();
    let (stderr_tx, stderr_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stdout.read_to_end(&mut bytes).map(|_| bytes);
        let _ = stdout_tx.send(result);
    });
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stderr.read_to_end(&mut bytes).map(|_| bytes);
        let _ = stderr_tx.send(result);
    });
    let deadline = std::time::Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("hermes {} timed out", args.join(" ")));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "failed to wait for hermes {}: {error}",
                    args.join(" ")
                ));
            }
        }
    };
    // A child process inheriting a pipe must not turn a completed Hermes
    // command into an unbounded wait on the reader threads.
    let read_timeout = deadline.saturating_duration_since(std::time::Instant::now())
        + std::time::Duration::from_millis(200);
    let stdout = stdout_rx
        .recv_timeout(read_timeout)
        .map_err(|e| format!("failed to read hermes stdout: {e}"))?
        .map_err(|e| format!("failed to read hermes stdout: {e}"))?;
    let stderr = stderr_rx
        .recv_timeout(read_timeout)
        .map_err(|e| format!("failed to read hermes stderr: {e}"))?
        .map_err(|e| format!("failed to read hermes stderr: {e}"))?;
    Ok((
        status.success(),
        String::from_utf8_lossy(&stdout).trim().to_string(),
        String::from_utf8_lossy(&stderr).trim().to_string(),
    ))
}

/// Run `hermes <args...>` requiring success. Returns `Err(message)` when
/// the CLI is missing or fails — config writes must not be guessed at when
/// the authoritative writer cannot run.
pub fn run_hermes_cli(args: &[&str]) -> Result<String, String> {
    let (success, stdout, stderr) = run_hermes_cli_raw(args)?;
    if !success {
        let detail = if stderr.is_empty() { &stdout } else { &stderr };
        return Err(format!("hermes {} failed: {detail}", args.join(" ")));
    }
    Ok(stdout)
}

/// JSON-array literal for a `hermes config set <list-key> <value>` value
/// (JSON is a YAML subset; the CLI's own dumper emits the block form).
fn config_list_value(items: &[String]) -> String {
    serde_json::to_string(items).unwrap_or_else(|_| "[]".to_string())
}

/// The last line of `hermes config get <key> --json` output, parsed as a
/// JSON string array. The CLI prints its value last; anything else in the
/// output is warnings.
fn json_list_from_get_output(stdout: &str) -> Option<Vec<String>> {
    let last = stdout.lines().last()?.trim();
    serde_json::from_str::<Vec<String>>(last).ok()
}

/// Read the current plugin allow/veto lists through the installed CLI —
/// the authoritative parser (handles every YAML shape, incl. flow style).
/// `hermes config get` exits non-zero with "Config key not set" for an
/// unset list, which means empty.
fn plugin_lists_via_cli() -> Result<(Vec<String>, Vec<String>), String> {
    let mut lists = Vec::new();
    for key in ["plugins.enabled", "plugins.disabled"] {
        let (success, stdout, stderr) = run_hermes_cli_raw(&["config", "get", key, "--json"])?;
        let missing = format!("Config key not set: {key}");
        if success && let Some(list) = json_list_from_get_output(&stdout) {
            lists.push(list);
        } else if !success
            && (stdout.lines().last().map(str::trim) == Some(missing.as_str())
                || stderr.lines().last().map(str::trim) == Some(missing.as_str()))
        {
            lists.push(Vec::new());
        } else {
            let detail = if stderr.is_empty() { &stdout } else { &stderr };
            return Err(format!("hermes config get {key}: {detail}"));
        }
    }
    Ok((lists.remove(0), lists.remove(0)))
}

/// Current `plugins.enabled`/`plugins.disabled` lists for list-rewriting.
/// CLI-first (authoritative); without the CLI, the conservative text parser
/// — and a present-but-unparsable config FAILS CLOSED rather than letting a
/// rewrite drop other plugins' allow-list entries. A missing config file is
/// a fresh home (empty lists).
fn current_plugin_lists() -> Result<(Vec<String>, Vec<String>), String> {
    if !cfg!(test) && crate::terminal::which_bin("hermes").is_some() {
        return plugin_lists_via_cli();
    }
    match std::fs::read_to_string(hermes_config_path()) {
        Err(_) => Ok((Vec::new(), Vec::new())),
        Ok(text) => parse_hermes_plugin_lists(&text).ok_or_else(|| {
            format!(
                "cannot safely parse the plugins block in {}; refusing to rewrite the plugin \
                 allow-lists — fix the config (hermes config check) or make the hermes CLI \
                 available, then retry",
                hermes_config_path().display()
            )
        }),
    }
}

/// Enable the backend extension through Hermes config, preserving every
/// other plugin and setting (the CLI's comment-preserving writer is the only
/// thing that touches config.yaml):
/// - add `hcom` to `plugins.enabled` (keeping all other entries, order kept)
/// - drop `hcom` from `plugins.disabled` if present (the loader vetoes it)
/// - grant `plugins.entries.hcom.allow_gateway_injection: true` — required
///   for the extension to inject visible user turns into `hermes serve` /
///   non-CLI sessions (hermes_cli PluginContext.inject_message gates non-CLI
///   injection on exactly this entry).
///
/// The `run` closure executes each `hermes config …` operation (injected so
/// tests assert the exact operations without the live CLI).
pub fn enable_hermes_backend_config(
    run: &dyn Fn(&[&str]) -> Result<(), String>,
) -> Result<(), String> {
    let (enabled, disabled) = current_plugin_lists()?;
    if !enabled.iter().any(|n| n == PLUGIN_DIR_NAME) {
        let mut desired = enabled.clone();
        desired.push(PLUGIN_DIR_NAME.to_string());
        run(&[
            "config",
            "set",
            "plugins.enabled",
            &config_list_value(&desired),
        ])?;
    }
    if disabled.iter().any(|n| n == PLUGIN_DIR_NAME) {
        let desired: Vec<String> = disabled
            .iter()
            .filter(|n| n.as_str() != PLUGIN_DIR_NAME)
            .cloned()
            .collect();
        run(&[
            "config",
            "set",
            "plugins.disabled",
            &config_list_value(&desired),
        ])?;
    }
    run(&[
        "config",
        "set",
        "plugins.entries.hcom.allow_gateway_injection",
        "true",
    ])?;
    Ok(())
}

/// Undo the config side of the install: remove `hcom` from
/// `plugins.enabled` and drop `plugins.entries.hcom` (a foreign `entries`
/// key is never touched — only ours). `plugins.disabled` is left alone: a
/// user's explicit disable choice survives a reinstall.
pub fn disable_hermes_backend_config(
    run: &dyn Fn(&[&str]) -> Result<(), String>,
) -> Result<(), String> {
    let (enabled, _disabled) = current_plugin_lists()?;
    if enabled.iter().any(|n| n == PLUGIN_DIR_NAME) {
        let desired: Vec<String> = enabled
            .iter()
            .filter(|n| n.as_str() != PLUGIN_DIR_NAME)
            .cloned()
            .collect();
        run(&[
            "config",
            "set",
            "plugins.enabled",
            &config_list_value(&desired),
        ])?;
    }
    // `hermes config unset` exits non-zero with "Config key not set" when
    // the key is already absent — an idempotent no-op, not a failure.
    if let Err(error) = run(&["config", "unset", "plugins.entries.hcom"])
        && !error.ends_with("Config key not set: plugins.entries.hcom")
    {
        return Err(error);
    }
    Ok(())
}

/// Full install used by the hooks surface: files, then config enablement.
/// Idempotent — already-current files and already-enabled config are no-ops.
pub fn try_setup_hermes_backend() -> Result<(), String> {
    if crate::terminal::which_bin("hermes").is_none() {
        return Err("the hermes CLI was not found on PATH; install Hermes first".to_string());
    }
    // Fail before writing plugin files when the existing allow-lists cannot
    // be read safely. A broken or absent Hermes CLI must not leave an
    // unconfigured extension behind.
    current_plugin_lists()?;
    install_hermes_backend_files()?;
    let run = |args: &[&str]| run_hermes_cli(args).map(|_| ());
    enable_hermes_backend_config(&run)
}

/// Full removal used by the hooks surface. An absent backend is a no-op that
/// leaves Hermes config untouched and does not require the Hermes CLI.
/// Ownership AND directory contents are validated FIRST: a foreign plugin
/// is never disabled or deleted, and unexpected files are refused. Config
/// cleanup must succeed before removing files so a failed CLI call leaves
/// an installed backend that can be retried, not stranded config.
pub fn remove_hermes_backend() -> Result<bool, String> {
    match backend_ownership()? {
        BackendOwnership::Absent => {
            // Nothing hcom-owned is installed. Do not touch a user's Hermes
            // config or require the Hermes CLI just to report a no-op.
            return Ok(false);
        }
        BackendOwnership::Foreign => return Err(foreign_plugin_error()),
        BackendOwnership::Ours => {}
    }
    let dir = backend_plugin_dir();
    if dir.exists()
        && let Some(name) = backend_unexpected_file(&dir)?
    {
        return Err(unexpected_file_error(&dir, &name));
    }
    if crate::terminal::which_bin("hermes").is_some() {
        let run = |args: &[&str]| run_hermes_cli(args).map(|_| ());
        disable_hermes_backend_config(&run)
            .map_err(|error| format!("Hermes config cleanup failed: {error}"))?;
    } else if !config_proves_no_hcom_settings()? {
        return Err(
            "Hermes config cleanup needs the hermes CLI, but it was not found on PATH; \
                    restore the CLI and retry (backend files were preserved)"
                .to_string(),
        );
    }
    remove_hermes_backend_files()
}

/// With no Hermes CLI, removal is safe only when no hcom config change is
/// possible. Do not try to edit YAML ourselves. A readable config with no
/// literal `hcom` and either no plugins block or a parseable one is a
/// conservative proof; anything ambiguous waits for the CLI writer.
fn config_proves_no_hcom_settings() -> Result<bool, String> {
    match std::fs::read_to_string(hermes_config_path()) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(format!("cannot inspect Hermes config for removal: {error}")),
        Ok(text) => Ok(!text.contains("hcom")
            && (!text.contains("plugins:") || parse_hermes_plugin_lists(&text).is_some())),
    }
}

/// Is gateway injection granted to our plugin entry? Authoritative answer
/// from the CLI when available; a conservative block-shape text scan of
/// config.yaml otherwise (only the block shape Hermes's own dumper emits).
/// `None` = unset/unknown.
pub fn hermes_gateway_injection_enabled() -> Option<bool> {
    if !cfg!(test)
        && let Ok(stdout) = run_hermes_cli(&[
            "config",
            "get",
            "plugins.entries.hcom.allow_gateway_injection",
        ])
    {
        match stdout.lines().last().map(str::trim) {
            Some("true") => return Some(true),
            Some("false") => return Some(false),
            _ => {}
        }
    }
    let text = std::fs::read_to_string(hermes_config_path()).ok()?;
    gateway_injection_from_config_text(&text)
}

/// Text fallback: recognize `entries:` → `hcom:` → `allow_gateway_injection:
/// true` in Hermes's block-shaped dump. Flow YAML is left to the authoritative
/// CLI parser; this fallback must never infer permission from a different key.
fn gateway_injection_from_config_text(text: &str) -> Option<bool> {
    fn bool_value(rest: &str) -> Option<bool> {
        let value = rest.trim();
        if value == "true" {
            Some(true)
        } else if value == "false" {
            Some(false)
        } else {
            None
        }
    }
    // Only a direct plugins -> entries -> hcom child is authoritative. Keep
    // the indentation path so a similarly named nested key cannot grant
    // permission in the fallback scan.
    let mut path: Vec<(usize, &str)> = Vec::new();
    for raw in text.lines() {
        if raw.trim().is_empty() || raw.trim_start().starts_with('#') {
            continue;
        }
        let indent = raw.len() - raw.trim_start().len();
        let body = raw.trim_start();
        while path.last().is_some_and(|(level, _)| indent <= *level) {
            path.pop();
        }
        let Some((key, rest)) = body.split_once(':') else {
            continue;
        };
        let key = key.trim();
        if path.len() == 3
            && path[0].0 == 0
            && path[0].1 == "plugins"
            && path[1].1 == "entries"
            && path[2].1 == "hcom"
            && key == "allow_gateway_injection"
        {
            return bool_value(rest);
        }
        if rest.trim().is_empty() {
            path.push((indent, key));
        }
    }
    None
}

/// Run `hermes plugins list --enabled --user --json` and capture stdout,
/// killing the child after `timeout` (a hung CLI must never hang an hcom
/// launch). Returns None on spawn failure, non-zero exit, timeout, or read
/// error.
fn run_hermes_json_probe(hermes_bin: &str, timeout: std::time::Duration) -> Option<String> {
    use std::io::Read;
    // Pin the child to the SAME home our config fallback reads: when
    // HERMES_HOME is set, use it verbatim; otherwise fall back to $HOME/.hermes
    // so a bare `hermes` subshell inspects the exact same configuration and
    // plugins directory that hcom evaluates.
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
    fn claimed_writes_answer_retryable_error_not_stale_on_read_failure() {
        let (db, path) = setup_test_db();
        save_hermes_instance(&db, "vega");
        set_row_session(&db, "vega", "ses-new");
        let before = db.get_last_event_id();

        // Transient read failure: the instances table becomes unreadable, so
        // every claim check reads Err — attribution is UNKNOWN, not verified.
        db.conn().execute_batch("DROP TABLE instances").unwrap();

        // Each claimed write must answer the retryable read error — never
        // the stale-attribution text the Python listener treats as a
        // verified rebind (and permanently tears down on).
        for out in [
            handle_status(
                &db,
                &argv_of(&[
                    "--name",
                    "vega",
                    "--status",
                    "active",
                    "--session-id",
                    "ses-new",
                ]),
            )
            .1,
            handle_read(
                &db,
                &argv_of(&["--name", "vega", "--format", "--session-id", "ses-new"]),
            )
            .1,
            handle_read(
                &db,
                &argv_of(&[
                    "--name",
                    "vega",
                    "--ack",
                    "--up-to",
                    "9",
                    "--session-id",
                    "ses-new",
                ]),
            )
            .1,
            handle_stop(
                &db,
                &argv_of(&["--name", "vega", "--reason", "r", "--session-id", "ses-new"]),
            )
            .1,
        ] {
            let doc = json_out(&out);
            let error = doc["error"].as_str().unwrap_or_default();
            assert!(
                !error.contains("no longer attributed"),
                "a read Err must not pose as a verified rebind: {doc}"
            );
            assert!(
                error.contains("could not be read"),
                "a retryable read error must be surfaced: {doc}"
            );
            assert!(
                doc.get("acked_to").is_none() && doc.get("ok").is_none(),
                "no success may be claimed while the row is unreadable: {doc}"
            );
        }
        assert_eq!(
            db.get_last_event_id(),
            before,
            "no ghost status/life events emitted while the row was unreadable"
        );

        // A REAL rebind (row readable, attributed elsewhere) still answers
        // the stale-attribution refusal — teardown stays keyed on truth.
        let (db2, path2) = setup_test_db();
        save_hermes_instance(&db2, "vega");
        set_row_session(&db2, "vega", "ses-new");
        for out in [
            handle_status(
                &db2,
                &argv_of(&[
                    "--name",
                    "vega",
                    "--status",
                    "active",
                    "--session-id",
                    "ses-old",
                ]),
            )
            .1,
            handle_read(
                &db2,
                &argv_of(&["--name", "vega", "--format", "--session-id", "ses-old"]),
            )
            .1,
            handle_read(
                &db2,
                &argv_of(&[
                    "--name",
                    "vega",
                    "--ack",
                    "--up-to",
                    "9",
                    "--session-id",
                    "ses-old",
                ]),
            )
            .1,
            handle_stop(
                &db2,
                &argv_of(&["--name", "vega", "--reason", "r", "--session-id", "ses-old"]),
            )
            .1,
        ] {
            let doc = json_out(&out);
            assert!(
                doc["error"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("no longer attributed"),
                "a verified rebind must still answer the stale refusal: {doc}"
            );
        }
        assert_eq!(
            db2.get_instance_full("vega")
                .unwrap()
                .unwrap()
                .last_event_id,
            0,
            "refused ack must not advance the cursor"
        );
        assert_eq!(
            db2.get_instance_full("vega").unwrap().unwrap().status,
            "listening",
            "refused status must not mutate the row"
        );
        assert!(
            db2.get_instance_full("vega").unwrap().is_some(),
            "refused stop must not finalize the row"
        );
        cleanup_db(&path);
        cleanup_db(&path2);
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

    // ── Backend extension installer tests (isolated HERMES_HOME) ─────────

    /// Isolated hermes home with an optional pre-existing config.yaml.
    fn isolated_hermes_home(
        config: Option<&str>,
    ) -> (tempfile::TempDir, crate::hooks::test_helpers::EnvGuard) {
        let temp = tempfile::tempdir().unwrap();
        if let Some(body) = config {
            std::fs::write(temp.path().join("config.yaml"), body).unwrap();
        }
        let guard = hermes_home_guard(temp.path());
        (temp, guard)
    }

    type RecordedOps = std::sync::Arc<std::sync::Mutex<Vec<Vec<String>>>>;

    fn recorded_ops() -> (RecordedOps, impl Fn(&[&str]) -> Result<(), String>) {
        let ops = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = std::sync::Arc::clone(&ops);
        let run = move |args: &[&str]| -> Result<(), String> {
            sink.lock()
                .unwrap()
                .push(args.iter().map(|s| s.to_string()).collect());
            Ok(())
        };
        (ops, run)
    }

    fn expected_ops(groups: &[&[&str]]) -> Vec<Vec<String>> {
        groups
            .iter()
            .map(|args| args.iter().map(|s| s.to_string()).collect())
            .collect()
    }

    #[test]
    #[serial_test::serial]
    #[cfg(unix)]
    fn hermes_config_runner_times_out_hung_cli() {
        let (_tmp, _guard) = isolated_hermes_home(None);
        let start = std::time::Instant::now();
        let error = run_hermes_command(
            "sh",
            &["-c", "while :; do :; done"],
            std::time::Duration::from_millis(80),
        )
        .expect_err("a hung CLI must be killed");
        assert!(error.contains("timed out"), "{error}");
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
    }

    #[test]
    #[serial_test::serial]
    fn backend_install_writes_then_updates_our_files() {
        let (_tmp, _guard) = isolated_hermes_home(None);
        assert!(!verify_hermes_plugin_installed());

        // Fresh install: exactly our two files, byte-current.
        assert_eq!(install_hermes_backend_files(), Ok(true));
        assert!(verify_hermes_plugin_installed());
        assert!(hermes_backend_files_current());
        assert_eq!(
            std::fs::read_to_string(backend_plugin_dir().join("plugin.yaml")).unwrap(),
            BACKEND_MANIFEST_SOURCE
        );
        assert_eq!(
            std::fs::read_to_string(backend_plugin_dir().join("__init__.py")).unwrap(),
            BACKEND_INIT_SOURCE
        );

        // Idempotent: current files are a no-op.
        assert_eq!(install_hermes_backend_files(), Ok(false));

        // A drifted copy under the hcom-managed marker (written by the
        // install above — an older hcom version's files) updates in place.
        std::fs::write(
            backend_plugin_dir().join("__init__.py"),
            "# older hcom-managed copy\ndef register(ctx):\n    pass\n",
        )
        .unwrap();
        assert!(!hermes_backend_files_current());
        assert_eq!(install_hermes_backend_files(), Ok(true));
        assert!(hermes_backend_files_current());
        assert!(
            std::fs::read_dir(hermes_plugin_root())
                .unwrap()
                .all(|entry| entry.unwrap().file_name() == PLUGIN_DIR_NAME),
            "completed atomic writes leave no staging files"
        );
    }

    #[test]
    #[serial_test::serial]
    fn backend_ownership_requires_marker_or_exact_sources() {
        // Absent when nothing is there.
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Absent));
        }
        // If an install stops immediately after mkdir, the empty directory
        // is adoptable, but arbitrary content is still foreign.
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Absent));
            assert_eq!(install_hermes_backend_files(), Ok(true));
            assert!(hermes_backend_files_current());
        }
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("user-notes.txt"), "data").unwrap();
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Foreign));
        }
        // Byte-exact sources without the marker: adoption (an exact copy of
        // OUR current files is definitionally ours) — install adds marker.
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("plugin.yaml"), BACKEND_MANIFEST_SOURCE).unwrap();
            std::fs::write(dir.join("__init__.py"), BACKEND_INIT_SOURCE).unwrap();
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Ours));
            assert_eq!(install_hermes_backend_files(), Ok(true));
            assert!(dir.join(BACKEND_MANAGED_MARKER).exists());
        }
        // A marker-only or marker-plus-init directory can be left by an
        // interrupted first install; both remain safely recoverable.
        for partial_init in [false, true] {
            let (_tmp, _guard) = isolated_hermes_home(None);
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(BACKEND_MANAGED_MARKER), BACKEND_MARKER_CONTENT).unwrap();
            if partial_init {
                std::fs::write(dir.join("__init__.py"), BACKEND_INIT_SOURCE).unwrap();
            }
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Ours));
            assert_eq!(install_hermes_backend_files(), Ok(true));
            assert!(hermes_backend_files_current());
        }
        // Drifted copy carrying hcom-ish identity but NO marker and no exact
        // match — e.g. the older externally managed plugin — is Foreign:
        // never updated, never deleted.
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("plugin.yaml"), "name: hcom\nversion: 0.0.1\n").unwrap();
            std::fs::write(
                dir.join("__init__.py"),
                "import logging\nlogger = logging.getLogger(\"hermes.plugin.hcom\")\n",
            )
            .unwrap();
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Foreign));
            let error = install_hermes_backend_files().expect_err("unmanaged drift refused");
            assert!(error.contains("not hcom-managed"), "{error}");
            assert!(remove_hermes_backend_files().is_err());
            assert!(dir.join("plugin.yaml").exists(), "foreign files untouched");
        }
    }

    #[test]
    #[serial_test::serial]
    #[cfg(unix)]
    fn backend_ownership_rejects_symlinked_paths() {
        // A symlinked plugin directory is user-managed wiring: Foreign.
        {
            let (tmp, _guard) = isolated_hermes_home(None);
            let real = tmp.path().join("elsewhere");
            std::fs::create_dir_all(&real).unwrap();
            std::fs::write(real.join("plugin.yaml"), BACKEND_MANIFEST_SOURCE).unwrap();
            std::fs::write(real.join("__init__.py"), BACKEND_INIT_SOURCE).unwrap();
            std::fs::create_dir_all(hermes_plugin_root()).unwrap();
            std::os::unix::fs::symlink(&real, backend_plugin_dir()).unwrap();
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Foreign));
            assert!(install_hermes_backend_files().is_err());
            assert!(remove_hermes_backend_files().is_err());
            assert!(
                real.join("plugin.yaml").exists(),
                "symlink target untouched"
            );
        }
        // A symlinked file inside a real directory is equally untouchable.
        {
            let (tmp, _guard) = isolated_hermes_home(None);
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("__init__.py"), BACKEND_INIT_SOURCE).unwrap();
            std::os::unix::fs::symlink(
                tmp.path().join("elsewhere-manifest.yaml"),
                dir.join("plugin.yaml"),
            )
            .unwrap();
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Foreign));
            assert!(install_hermes_backend_files().is_err());
        }
        // A symlinked (or edited) managed marker must NEVER make someone
        // else's directory look hcom-managed.
        {
            let (tmp, _guard) = isolated_hermes_home(None);
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("plugin.yaml"), "name: someone-elses\n").unwrap();
            std::fs::write(dir.join("__init__.py"), "# externally managed\n").unwrap();
            std::fs::write(tmp.path().join("marker-elsewhere"), BACKEND_MARKER_CONTENT).unwrap();
            std::os::unix::fs::symlink(
                tmp.path().join("marker-elsewhere"),
                dir.join(BACKEND_MANAGED_MARKER),
            )
            .unwrap();
            assert_eq!(
                backend_ownership(),
                Ok(BackendOwnership::Foreign),
                "a symlinked marker does not confer ownership"
            );
            assert!(install_hermes_backend_files().is_err());
            assert!(remove_hermes_backend_files().is_err());
            assert!(dir.join("plugin.yaml").exists());
        }
        // An EDITED marker (wrong content, regular file) is equally invalid.
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("plugin.yaml"), "name: someone-elses\n").unwrap();
            std::fs::write(dir.join("__init__.py"), "# externally managed\n").unwrap();
            std::fs::write(dir.join(BACKEND_MANAGED_MARKER), "forged\n").unwrap();
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Foreign));
        }
        // Even byte-current sources cannot make a symlinked marker safe to
        // write through on a later update.
        {
            let (tmp, _guard) = isolated_hermes_home(None);
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("plugin.yaml"), BACKEND_MANIFEST_SOURCE).unwrap();
            std::fs::write(dir.join("__init__.py"), BACKEND_INIT_SOURCE).unwrap();
            std::fs::write(tmp.path().join("marker-elsewhere"), BACKEND_MARKER_CONTENT).unwrap();
            std::os::unix::fs::symlink(
                tmp.path().join("marker-elsewhere"),
                dir.join(BACKEND_MANAGED_MARKER),
            )
            .unwrap();
            assert_eq!(backend_ownership(), Ok(BackendOwnership::Foreign));
            assert!(install_hermes_backend_files().is_err());
        }
    }

    #[test]
    #[serial_test::serial]
    fn backend_remove_absent_is_successful_noop() {
        let (_tmp, _guard) = isolated_hermes_home(Some(
            "plugins:\n  enabled:\n    - hcom\n  entries:\n    hcom:\n      allow_gateway_injection: true\n",
        ));
        let before = std::fs::read_to_string(hermes_config_path()).unwrap();
        assert_eq!(remove_hermes_backend(), Ok(false));
        assert_eq!(crate::tool::Tool::Hermes.remove_hooks(), Ok(true));
        assert_eq!(
            std::fs::read_to_string(hermes_config_path()).unwrap(),
            before
        );
    }

    #[test]
    #[serial_test::serial]
    fn backend_config_noop_proof_is_conservative() {
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            assert_eq!(config_proves_no_hcom_settings(), Ok(true));
        }
        {
            let (_tmp, _guard) = isolated_hermes_home(Some(
                "plugins:\n  enabled:\n    - current-time\n  entries:\n    current-time:\n      allow_tool_override: false\n",
            ));
            assert_eq!(config_proves_no_hcom_settings(), Ok(true));
        }
        {
            let (_tmp, _guard) = isolated_hermes_home(Some("model: some-model\n"));
            assert_eq!(config_proves_no_hcom_settings(), Ok(true));
        }
        {
            let (_tmp, _guard) = isolated_hermes_home(Some(
                "plugins:\n  entries:\n    hcom:\n      allow_gateway_injection: true\n",
            ));
            assert_eq!(config_proves_no_hcom_settings(), Ok(false));
        }
        {
            let (_tmp, _guard) = isolated_hermes_home(Some("plugins: [unclosed\n"));
            assert_eq!(config_proves_no_hcom_settings(), Ok(false));
        }
    }

    #[test]
    #[serial_test::serial]
    fn backend_remove_refuses_unexpected_files_and_foreign_skips_config() {
        // Unexpected file inside our directory: refuse to delete anything.
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            assert_eq!(install_hermes_backend_files(), Ok(true));
            std::fs::write(backend_plugin_dir().join("user-notes.txt"), "data").unwrap();
            let error = remove_hermes_backend_files().expect_err("unexpected file refused");
            assert!(error.contains("user-notes.txt"), "{error}");
            assert!(backend_plugin_dir().exists(), "directory is preserved");
        }
        // Python's own compiled init is expected runtime content, but an
        // unrelated file inside the cache must not be deleted.
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            assert_eq!(install_hermes_backend_files(), Ok(true));
            let cache = backend_plugin_dir().join("__pycache__");
            std::fs::create_dir_all(&cache).unwrap();
            std::fs::write(cache.join("user-notes.txt"), "data").unwrap();
            assert!(remove_hermes_backend_files().is_err());
            assert!(cache.join("user-notes.txt").exists());
            std::fs::remove_file(cache.join("user-notes.txt")).unwrap();
            std::fs::write(cache.join("__init__.cpython-314.pyc"), "cached").unwrap();
            assert_eq!(remove_hermes_backend_files(), Ok(true));
        }
        // A FOREIGN plugin: remove_hermes_backend refuses BEFORE any config
        // mutation — the foreign plugin's enablement is not ours to change.
        {
            let (_tmp, _guard) = isolated_hermes_home(Some(
                "plugins:\n  enabled:\n    - hcom\n  entries:\n    hcom:\n      allow_gateway_injection: true\n",
            ));
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("plugin.yaml"), "name: hcom\nversion: 0.0.1\n").unwrap();
            std::fs::write(dir.join("__init__.py"), "# externally managed\n").unwrap();
            assert!(remove_hermes_backend().is_err());
            assert!(dir.join("plugin.yaml").exists());
            let config = std::fs::read_to_string(hermes_config_path()).unwrap();
            assert!(
                config.contains("- hcom") && config.contains("allow_gateway_injection: true"),
                "config is untouched for a foreign plugin:\n{config}"
            );
        }
    }

    #[test]
    #[serial_test::serial]
    fn enable_config_fails_closed_on_unparsable_config() {
        // A present-but-unparsable plugins block must NEVER be rewritten
        // from an empty-list fallback — that would erase other plugins'
        // allow-list entries.
        let (_tmp, _guard) = isolated_hermes_home(Some("plugins: [unclosed\n  bad yaml :::\n"));
        let (ops, run) = recorded_ops();
        let error = enable_hermes_backend_config(&run).expect_err("must fail closed");
        assert!(
            error.contains("refusing to rewrite"),
            "diagnostic must explain the refusal: {error}"
        );
        assert!(
            ops.lock().unwrap().is_empty(),
            "no config operations may run on an unreadable config"
        );
    }

    #[test]
    #[serial_test::serial]
    fn backend_readiness_requires_grant_and_ownership() {
        // Owned + enabled + granted → ready.
        {
            let (_tmp, _guard) = isolated_hermes_home(Some(
                "plugins:\n  enabled:\n    - hcom\n  entries:\n    hcom:\n      allow_gateway_injection: true\n",
            ));
            assert_eq!(install_hermes_backend_files(), Ok(true));
            assert!(evaluate_hermes_backend_readiness().ready);
            assert!(verify_hermes_backend_ready());
        }
        // Owned + enabled but NO grant: the plugin loads, yet the backend is
        // not ready — hooks-add must not say "already installed" here.
        {
            let (_tmp, _guard) = isolated_hermes_home(Some("plugins:\n  enabled:\n    - hcom\n"));
            assert_eq!(install_hermes_backend_files(), Ok(true));
            let readiness = evaluate_hermes_backend_readiness();
            assert!(!readiness.ready, "{readiness:?}");
            assert!(
                readiness
                    .diagnostic
                    .to_lowercase()
                    .contains("gateway injection"),
                "diagnostic must name the missing grant: {}",
                readiness.diagnostic
            );
            assert!(!verify_hermes_backend_ready());
        }
        // Foreign plugin that IS enabled and granted: still not ready —
        // launch must install ours, never load someone else's plugin.
        {
            let (_tmp, _guard) = isolated_hermes_home(Some(
                "plugins:\n  enabled:\n    - hcom\n  entries:\n    hcom:\n      allow_gateway_injection: true\n",
            ));
            let dir = backend_plugin_dir();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("plugin.yaml"), "name: hcom\nversion: 0.0.1\n").unwrap();
            std::fs::write(dir.join("__init__.py"), "# externally managed\n").unwrap();
            let readiness = evaluate_hermes_plugin_readiness();
            assert!(!readiness.ready, "{readiness:?}");
            assert!(
                readiness.diagnostic.contains("not hcom-managed"),
                "migration diagnostic names the problem: {}",
                readiness.diagnostic
            );
            assert!(!verify_hermes_backend_ready());
        }
    }

    #[test]
    #[serial_test::serial]
    fn backend_install_refuses_foreign_plugin_at_our_path() {
        let (_tmp, _guard) = isolated_hermes_home(None);
        let dir = backend_plugin_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("plugin.yaml"),
            "name: someone-elses\nversion: 9.9\n",
        )
        .unwrap();
        let foreign_manifest = std::fs::read_to_string(dir.join("plugin.yaml")).unwrap();

        let error = install_hermes_backend_files().expect_err("foreign plugin must be refused");
        assert!(error.contains("not hcom-managed"), "{error}");
        assert_eq!(
            std::fs::read_to_string(dir.join("plugin.yaml")).unwrap(),
            foreign_manifest,
            "the foreign file is untouched"
        );
        assert!(
            !dir.join("__init__.py").exists(),
            "nothing else was written"
        );

        // Foreign __init__.py is refused the same way.
        std::fs::write(dir.join("plugin.yaml"), BACKEND_MANIFEST_SOURCE).unwrap();
        std::fs::write(dir.join("__init__.py"), "print('not ours')\n").unwrap();
        assert!(install_hermes_backend_files().is_err());
        assert_eq!(
            std::fs::read_to_string(dir.join("__init__.py")).unwrap(),
            "print('not ours')\n",
            "the foreign module is untouched"
        );
    }

    #[test]
    #[serial_test::serial]
    fn backend_remove_removes_ours_and_refuses_foreign() {
        let (_tmp, _guard) = isolated_hermes_home(None);
        assert_eq!(
            remove_hermes_backend_files(),
            Ok(false),
            "absent is a no-op"
        );

        assert_eq!(install_hermes_backend_files(), Ok(true));
        assert_eq!(remove_hermes_backend_files(), Ok(true));
        assert!(!backend_plugin_dir().exists());

        // Foreign content at our path is never deleted.
        let dir = backend_plugin_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("plugin.yaml"), "name: someone-elses\n").unwrap();
        assert!(remove_hermes_backend_files().is_err());
        assert!(dir.join("plugin.yaml").exists());
    }

    #[test]
    #[serial_test::serial]
    fn enable_config_preserves_foreign_lists_and_grants_injection() {
        let (_tmp, _guard) = isolated_hermes_home(Some(
            "# user comment\nmodel:\n  default: glm\nplugins:\n  disabled:\n    - hcom\n    - other\n  enabled:\n    - current-time\n  entries:\n    current-time:\n      allow_tool_override: false\nagent:\n  max_turns: 60\n",
        ));
        let (ops, run) = recorded_ops();
        enable_hermes_backend_config(&run).unwrap();
        let ops = ops.lock().unwrap().clone();
        assert_eq!(
            ops,
            expected_ops(&[
                &[
                    "config",
                    "set",
                    "plugins.enabled",
                    "[\"current-time\",\"hcom\"]"
                ],
                &["config", "set", "plugins.disabled", "[\"other\"]"],
                &[
                    "config",
                    "set",
                    "plugins.entries.hcom.allow_gateway_injection",
                    "true"
                ],
            ]),
            "foreign enabled/disabled entries survive; the veto is dropped; injection granted"
        );
    }

    #[test]
    #[serial_test::serial]
    fn enable_config_is_idempotent_and_boots_fresh_homes() {
        // Already enabled, not vetoed: only the injection grant is ensured.
        // (Each case in its own block: two live EnvGuards on the env lock in
        // one thread would self-deadlock.)
        {
            let (_tmp, _guard) = isolated_hermes_home(Some(
                "plugins:\n  disabled: []\n  enabled:\n    - current-time\n    - hcom\n",
            ));
            let (ops, run) = recorded_ops();
            enable_hermes_backend_config(&run).unwrap();
            assert_eq!(
                ops.lock().unwrap().clone(),
                expected_ops(&[&[
                    "config",
                    "set",
                    "plugins.entries.hcom.allow_gateway_injection",
                    "true"
                ]])
            );
        }

        // No config at all: enable seeds the allow-list with just hcom.
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            let (ops, run) = recorded_ops();
            enable_hermes_backend_config(&run).unwrap();
            assert_eq!(
                ops.lock().unwrap().clone(),
                expected_ops(&[
                    &["config", "set", "plugins.enabled", "[\"hcom\"]"],
                    &[
                        "config",
                        "set",
                        "plugins.entries.hcom.allow_gateway_injection",
                        "true"
                    ],
                ])
            );
        }
    }

    #[test]
    #[serial_test::serial]
    fn enable_config_propagates_cli_failure() {
        let (_tmp, _guard) = isolated_hermes_home(None);
        let run =
            |_: &[&str]| -> Result<(), String> { Err("hermes config set failed".to_string()) };
        assert!(
            enable_hermes_backend_config(&run)
                .unwrap_err()
                .contains("hermes config set failed")
        );
    }

    #[test]
    #[serial_test::serial]
    fn disable_config_removes_only_our_entries() {
        // (Each case in its own block: two live EnvGuards on the env lock
        // in one thread would self-deadlock.)
        {
            let (_tmp, _guard) = isolated_hermes_home(Some(
                "plugins:\n  enabled:\n    - current-time\n    - hcom\n  entries:\n    current-time:\n      allow_tool_override: false\n    hcom:\n      allow_gateway_injection: true\n",
            ));
            let (ops, run) = recorded_ops();
            disable_hermes_backend_config(&run).unwrap();
            assert_eq!(
                ops.lock().unwrap().clone(),
                expected_ops(&[
                    &["config", "set", "plugins.enabled", "[\"current-time\"]"],
                    &["config", "unset", "plugins.entries.hcom"],
                ]),
                "foreign enabled entries and foreign entries keys are preserved"
            );
        }

        // Not enabled: only the entries cleanup runs — and an already-absent
        // entries key ("Config key not set") is an idempotent no-op.
        {
            let (_tmp, _guard) = isolated_hermes_home(Some("plugins:\n  enabled: []\n"));
            let ops = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Vec<String>>::new()));
            let sink = std::sync::Arc::clone(&ops);
            let run = move |args: &[&str]| -> Result<(), String> {
                let mut guard = sink.lock().unwrap();
                guard.push(args.iter().map(|s| s.to_string()).collect());
                if args.first() == Some(&"unset") {
                    return Err("hermes config unset plugins.entries.hcom failed: Config key not set: plugins.entries.hcom".to_string());
                }
                Ok(())
            };
            disable_hermes_backend_config(&run).unwrap();
            assert_eq!(
                ops.lock().unwrap().clone(),
                expected_ops(&[&["config", "unset", "plugins.entries.hcom"]])
            );
        }
    }

    #[test]
    fn gateway_injection_text_fallback_only_trusts_exact_block_key() {
        // Block shape (Hermes's own dumper output).
        assert_eq!(
            gateway_injection_from_config_text(
                "plugins:\n  enabled:\n    - hcom\n  entries:\n    hcom:\n      allow_gateway_injection: true\n"
            ),
            Some(true)
        );
        assert_eq!(
            gateway_injection_from_config_text(
                "plugins:\n  entries:\n    other:\n      allow_gateway_injection: true\n    hcom:\n      allow_gateway_injection: false\n"
            ),
            Some(false)
        );
        // Flow shape is left to the Hermes CLI's YAML parser. A text scan
        // must not mistake another plugin's similarly named key for hcom.
        assert_eq!(
            gateway_injection_from_config_text(
                "plugins: {entries: {hcom: {allow_gateway_injection: true}}}\n"
            ),
            None
        );
        assert_eq!(
            gateway_injection_from_config_text(
                "plugins: {entries: {otherhcom: {allow_gateway_injection: true}}}\n"
            ),
            None
        );
        // Absent, or granted only to a foreign plugin: unknown/not set.
        assert_eq!(
            gateway_injection_from_config_text("plugins:\n  enabled:\n    - hcom\n"),
            None
        );
        assert_eq!(
            gateway_injection_from_config_text(
                "plugins:\n  entries:\n    other:\n      allow_gateway_injection: true\n"
            ),
            None
        );
        // Only direct children count, not identically named keys nested in
        // another plugin or beneath hcom's own unrelated setting.
        for config in [
            "plugins:\n  other:\n    entries:\n      hcom:\n        allow_gateway_injection: true\n",
            "plugins:\n  entries:\n    other:\n      hcom:\n        allow_gateway_injection: true\n",
            "plugins:\n  entries:\n    hcom:\n      other:\n        allow_gateway_injection: true\n",
            "other:\n  plugins:\n    entries:\n      hcom:\n        allow_gateway_injection: true\n",
        ] {
            assert_eq!(gateway_injection_from_config_text(config), None, "{config}");
        }
    }

    #[test]
    #[serial_test::serial]
    fn gateway_injection_enabled_reads_isolated_home() {
        // (Each case in its own block: two live EnvGuards on the env lock
        // in one thread would self-deadlock.)
        {
            let (_tmp, _guard) = isolated_hermes_home(Some(
                "plugins:\n  enabled:\n    - hcom\n  entries:\n    hcom:\n      allow_gateway_injection: true\n",
            ));
            assert_eq!(hermes_gateway_injection_enabled(), Some(true));
        }
        {
            let (_tmp, _guard) = isolated_hermes_home(None);
            assert_eq!(hermes_gateway_injection_enabled(), None);
        }
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
    fn plugin_root_defaults_to_home_independently_of_hcom_state() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let state = temp.path().join("other-state");
        std::fs::create_dir_all(&home).unwrap();
        unsafe {
            std::env::remove_var("HERMES_HOME");
            std::env::set_var("HOME", &home);
            std::env::set_var("HCOM_DIR", &state);
        }
        assert_eq!(hermes_plugin_root(), home.join(".hermes").join("plugins"));
    }

    // ── Plugin readiness / config-enable tests ──────────────────────────

    fn install_fake_hermes_plugin(home: &std::path::Path) {
        // Readiness rejects foreign plugins, so tests install exactly what
        // `hcom hooks add hermes` writes: the embedded sources plus the
        // hcom-managed marker.
        let dir = home.join("plugins").join(PLUGIN_DIR_NAME);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("__init__.py"), BACKEND_INIT_SOURCE).unwrap();
        std::fs::write(dir.join("plugin.yaml"), BACKEND_MANIFEST_SOURCE).unwrap();
        std::fs::write(dir.join(BACKEND_MANAGED_MARKER), BACKEND_MARKER_CONTENT).unwrap();
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
        save_hermes_instance(&db_main, "luna");

        // Pre-open the independent thread connection before acquiring writer lock.
        // This ensures all schema/WAL initialization on db_thread completes cleanly.
        let db_thread = HcomDb::open_at(&path).unwrap();

        // Independent connection to hold writer lock
        let ext_conn = rusqlite::Connection::open(&path).unwrap();
        ext_conn
            .busy_timeout(std::time::Duration::from_millis(5000))
            .unwrap();

        // Install busy handler on db_thread before the lock
        BLOCKED_SIGNALED.store(false, std::sync::atomic::Ordering::SeqCst);
        db_thread
            .conn()
            .busy_handler(Some(test_busy_handler))
            .unwrap();

        // Step 1: Open an IMMEDIATE transaction on ext_conn and set last_event_id = 10,
        // but DO NOT commit yet.
        ext_conn
            .execute_batch(
                "BEGIN IMMEDIATE; UPDATE instances SET last_event_id = 10 WHERE name = 'luna';",
            )
            .unwrap();

        // Step 2: In a background thread, run handle_read with --ack --up-to 5 on db_thread.
        // In WAL mode, get_instance_full reads the committed state (last_event_id = 0).
        // Since 5 > 0, handle_read proceeds to advance the cursor.
        // advance_instance_cursor executes an UPDATE which blocks on ext_conn's write lock.
        let handle = std::thread::spawn(move || {
            handle_read(
                &db_thread,
                &argv_of(&["--name", "luna", "--ack", "--up-to", "5"]),
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

        // Step 3: Commit ext_conn's transaction, making last_event_id = 10 the committed value.
        ext_conn.execute_batch("COMMIT;").unwrap();

        // Step 4: Wait for handle_read in the background thread to finish.
        let (_, out) = handle.join().expect("background thread finished");
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(doc.get("error").is_none(), "ack succeeded: {out}");

        // Step 5: Verify that last_event_id in DB is 10, proving MAX preserved it and did not rewind.
        let inst = db_main.get_instance_full("luna").unwrap().unwrap();
        assert_eq!(
            inst.last_event_id, 10,
            "cursor was not rewound by the concurrent ack of older event 5"
        );
        cleanup_db(&path);
    }

    #[test]
    fn handle_read_ack_rejects_foreign_and_stale_owners() {
        let (db, path) = setup_test_db();

        // 1. Foreign instance owned by claude
        save_instance_with_tool(&db, "claude-inst", "claude");
        let (_, out) = handle_read(
            &db,
            &argv_of(&["--name", "claude-inst", "--ack", "--up-to", "5"]),
        );
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(
            doc.get("error").is_some(),
            "foreign tool instance must be rejected with error: {doc}"
        );
        assert_eq!(
            db.get_instance_full("claude-inst")
                .unwrap()
                .unwrap()
                .last_event_id,
            0
        );

        // 2. Stale session attribution
        save_hermes_instance(&db, "luna");
        set_row_session(&db, "luna", "ses-current");
        db.set_session_binding("ses-current", "luna").unwrap();
        let (_, out) = handle_read(
            &db,
            &argv_of(&[
                "--name",
                "luna",
                "--session-id",
                "ses-stale",
                "--ack",
                "--up-to",
                "5",
            ]),
        );
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(
            doc.get("error").is_some(),
            "stale session attribution must be rejected with error: {doc}"
        );
        assert_eq!(
            db.get_instance_full("luna").unwrap().unwrap().last_event_id,
            0
        );
        cleanup_db(&path);
    }

    #[test]
    fn handle_start_process_present_fails_closed_on_foreign_session() {
        let (db, path) = setup_test_db();
        // Foreign instance (claude) owning session "ses-foreign"
        save_instance_with_tool(&db, "claude-inst", "claude");
        set_row_session(&db, "claude-inst", "ses-foreign");
        db.set_session_binding("ses-foreign", "claude-inst")
            .unwrap();

        // A process running with HCOM_PROCESS_ID
        let ctx = ctx_with_process("proc-hermes-1", "/tmp/project");
        let (_, out) = handle_start(&ctx, &db, &argv_of(&["--session-id", "ses-foreign"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(
            doc["name"].is_null(),
            "handle_start must fail closed and return null name on foreign session: {doc}"
        );

        // Foreign instance must not be modified or retired
        let foreign = db.get_instance_full("claude-inst").unwrap().unwrap();
        assert_eq!(foreign.tool, "claude");
        assert_ne!(foreign.status, "inactive");
        assert_eq!(
            db.get_session_binding("ses-foreign").unwrap().as_deref(),
            Some("claude-inst")
        );
        cleanup_db(&path);
    }

    #[test]
    fn process_unbound_with_foreign_instance_name_env_does_not_mutate_or_bind() {
        let (db, path) = setup_test_db();
        // Inactive OMP instance
        save_instance_with_tool(&db, "omp-inst", "omp");
        let mut updates = serde_json::Map::new();
        updates.insert("status".into(), serde_json::json!("inactive"));
        db.update_instance_fields("omp-inst", &updates).unwrap();

        let mut env = std::collections::HashMap::new();
        env.insert("HCOM_PROCESS_ID".to_string(), "proc-hermes-2".to_string());
        env.insert("HCOM_INSTANCE_NAME".to_string(), "omp-inst".to_string());
        let ctx = HcomContext::from_env(&env, std::path::PathBuf::from("/tmp/project"));

        let (_, out) = handle_start(&ctx, &db, &argv_of(&["--session-id", "ses-unknown"]));
        let doc: Value = serde_json::from_str(&out).unwrap();
        assert!(
            doc["name"].is_null(),
            "unbound process must return null and never adopt foreign inactive row: {doc}"
        );

        // Foreign instance must not be mutated or bound
        let row = db.get_instance_full("omp-inst").unwrap().unwrap();
        assert_eq!(row.tool, "omp");
        assert_eq!(row.status, "inactive");
        assert!(row.session_id.is_none());
        assert!(db.get_process_binding("proc-hermes-2").unwrap().is_none());
        cleanup_db(&path);
    }
}
