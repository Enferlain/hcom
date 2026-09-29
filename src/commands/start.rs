//! Start command: `hcom start [--name <agent-id>] [--as <name>] [--orphan <name|pid>]`
//!
//! Runs inside an already-running tool session rather than launching a new one.
//! Used for adhoc/manual setup, identity rebinding, and orphan recovery:
//! - Bare start: detect vanilla tool or create adhoc instance
//! - `--name <agent-id>`: register a subagent (a router-level global flag, not
//!   parsed by `StartArgs` — resolved in `run()` via `flags.name`)
//! - `--orphan`: recover orphaned PTY process
//! - `--as`: rebind session identity

use anyhow::{Result, bail};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::bootstrap;
use crate::claude_actor;
use crate::config::HcomConfig;
use crate::db::{HcomDb, InstanceRow};
use crate::identity;
use crate::instance_binding;
use crate::instance_lifecycle as lifecycle;
use crate::instance_names;
use crate::instances;
use crate::log::{log_info, log_warn};
use crate::paths;
use crate::pidtrack;
use crate::relay;
use crate::router::GlobalFlags;
use crate::shared::constants::{ST_ACTIVE, ST_LISTENING};
use crate::shared::context::HcomContext;

/// Parsed arguments for `hcom start`.
#[derive(clap::Parser, Debug)]
#[command(name = "start", about = "Start hcom participation")]
pub struct StartArgs {
    /// Rebind to a different instance name
    #[arg(long = "as")]
    pub as_name: Option<String>,
    /// Recover orphaned PTY process by name or PID
    #[arg(long)]
    pub orphan: Option<String>,
}

/// Run the start command.
pub fn run(argv: &[String], flags: &GlobalFlags) -> Result<i32> {
    // Filter out global flags already consumed by the router (start, --name X, --go)
    let mut filtered = vec!["start".to_string()];
    let mut skip_next = false;
    for arg in argv {
        if skip_next {
            skip_next = false;
            continue;
        }
        match arg.as_str() {
            "start" | "--go" => continue,
            "--name" => {
                skip_next = true;
                continue;
            }
            _ => filtered.push(arg.clone()),
        }
    }

    use clap::Parser;
    let start_args = match StartArgs::try_parse_from(&filtered) {
        Ok(a) => a,
        Err(e) => {
            e.print().ok();
            return Ok(if e.use_stderr() { 1 } else { 0 });
        }
    };

    let orphan_target = start_args.orphan;
    let rebind_target = start_args.as_name;

    let db = HcomDb::open()?;
    let hcom_dir = paths::hcom_dir();

    let ctx = HcomContext::from_os();
    let verified_actor = claude_actor::resolve_env_actor(&db).map_err(anyhow::Error::new)?;
    if let (Some(actor), Some(name)) = (verified_actor.as_ref(), flags.name.as_deref()) {
        claude_actor::ensure_explicit_matches(&db, actor, name).map_err(anyhow::Error::new)?;
    }

    let requested_name = flags
        .name
        .as_deref()
        .map(|name| identity::resolve_display_name(&db, name).unwrap_or_else(|| name.to_string()));

    // A verified child actor can only promote/use its existing row. It cannot
    // rebind or recover another identity, and it does not need --name.
    if let Some(actor) = verified_actor.as_ref()
        && let Some(actor_row) = db.get_instance_full(&actor.name)?
        && instances::is_subagent_instance(&actor_row)
    {
        if rebind_target.is_some() {
            println!("[HCOM] Subagents cannot use --as. End your turn.");
            return Ok(1);
        }
        if orphan_target.is_some() {
            println!("[HCOM] Subagents cannot use --orphan. End your turn.");
            return Ok(1);
        }
        return start_subagent(&db, &actor_row);
    }

    // Without a capability, retain the ordinary manual fallback. A direct
    // indexed child lookup supports the documented --name <agent-id> form
    // without scanning duplicated parent JSON.
    let subagent_via_name = if verified_actor.is_none() {
        requested_name
            .as_deref()
            .and_then(|id| detect_subagent(&db, id))
    } else {
        None
    };
    let subagent_via_as = if verified_actor.is_none() {
        rebind_target
            .as_deref()
            .and_then(|id| detect_subagent(&db, id))
    } else {
        None
    };

    if subagent_via_as.is_some() || (subagent_via_name.is_some() && rebind_target.is_some()) {
        println!("[HCOM] Subagents cannot change identity. End your turn.");
        return Ok(1);
    }

    if let Some(orphan) = orphan_target {
        return start_from_orphan(&db, &hcom_dir, &orphan, &ctx);
    }

    if let Some(rebind) = rebind_target {
        let current_name = verified_actor
            .as_ref()
            .map(|actor| actor.name.as_str())
            .or(requested_name.as_deref());
        return start_rebind(&db, &rebind, &ctx, current_name);
    }

    if let Some(subagent) = subagent_via_name {
        return start_subagent(&db, &subagent);
    }

    // A verified root actor stays the root even while children exist.
    let effective_name = verified_actor
        .as_ref()
        .map(|actor| actor.name.as_str())
        .or(requested_name.as_deref());
    start_bare(&db, &hcom_dir, &ctx, effective_name)
}

/// Resolve a live child row directly by agent_id (or by its exact row name).
fn detect_subagent(db: &HcomDb, check_id: &str) -> Option<InstanceRow> {
    let name = db
        .get_instance_by_agent_id(check_id)
        .ok()
        .flatten()
        .unwrap_or_else(|| check_id.to_string());
    let row = db.get_instance_full(&name).ok().flatten()?;
    row.parent_name.as_ref().filter(|name| !name.is_empty())?;
    Some(row)
}

/// Promote an existing dormant child row into active hcom participation.
fn start_subagent(db: &HcomDb, info: &InstanceRow) -> Result<i32> {
    let parent_name = info.parent_name.as_deref().unwrap_or("");
    if parent_name.is_empty() || info.agent_id.as_deref().unwrap_or("").is_empty() {
        bail!(
            "Subagent row '{}' is missing parent/agent identity",
            info.name
        );
    }

    let was_announced = info.name_announced != 0;
    lifecycle::set_status(db, &info.name, ST_ACTIVE, "tool:start", Default::default());
    instance_binding::capture_and_store_launch_context(db, &info.name);

    log_info(
        "lifecycle",
        "start.subagent",
        &format!(
            "name={} parent={} agent_id={} announced={}",
            info.name,
            parent_name,
            info.agent_id.as_deref().unwrap_or(""),
            was_announced
        ),
    );

    if was_announced {
        println!("hcom already started for {}", info.name);
        return Ok(0);
    }

    let bootstrap = bootstrap::get_subagent_bootstrap(&info.name, parent_name);
    if !bootstrap.is_empty() {
        println!("{bootstrap}");
    }

    // Exactly-once connection gate: one conditional UPDATE claims the row and
    // the `connected` life event commits in the same transaction, so a hook
    // activation ack racing this start (or a duplicate ack) can neither
    // double-emit nor lose the signal. On a DB error the row stays unclaimed,
    // so a later start re-delivers the bootstrap and retries the claim —
    // warn, don't fail the start.
    if let Err(e) = db.claim_subagent_connection(&info.name, parent_name, "cli") {
        eprintln!(
            "[hcom] warn: subagent connection claim failed for {}: {e}",
            info.name
        );
    }

    Ok(0)
}

/// Recover orphaned PTY process by PID or name.
fn start_from_orphan(
    db: &HcomDb,
    hcom_dir: &std::path::Path,
    target: &str,
    _ctx: &HcomContext,
) -> Result<i32> {
    let active_pids: HashSet<u32> = db
        .iter_instances_full()?
        .iter()
        .filter_map(|inst| inst.pid.map(|p| p as u32))
        .collect();
    let orphans = pidtrack::get_orphan_processes(hcom_dir, Some(&active_pids));

    if orphans.is_empty() {
        bail!("No orphan processes found.");
    }

    // Match by PID or name
    let orphan = if let Ok(pid) = target.parse::<u32>() {
        match orphans.iter().find(|o| o.pid == pid) {
            Some(o) => o,
            None => bail!("Orphan PID {} not found.", pid),
        }
    } else {
        let matches: Vec<_> = orphans
            .iter()
            .filter(|o| o.names.contains(&target.to_string()))
            .collect();
        match matches.len() {
            0 => bail!("Orphan '{}' not found.", target),
            1 => matches[0],
            _ => {
                let pids: Vec<String> = matches.iter().map(|m| m.pid.to_string()).collect();
                bail!(
                    "Multiple orphans match '{}' (PIDs: {}). Use --orphan <pid>.",
                    target,
                    pids.join(", ")
                );
            }
        }
    };

    let pid = orphan.pid;

    if orphan.process_id.is_empty() {
        bail!(
            "Orphan PID {} has no process_id and cannot be recovered.",
            pid
        );
    }

    let preferred_name = orphan.names.last().cloned().unwrap_or_default();
    let can_reuse = !preferred_name.is_empty()
        && identity::is_valid_base_name(&preferred_name)
        && db.get_instance_full(&preferred_name)?.is_none();
    let name = if can_reuse {
        preferred_name
    } else {
        instance_names::generate_unique_name(db)?
    };

    // Core DB registration
    let _ = pidtrack::recover_single_orphan_to_db(db, orphan, &name);

    db.log_event(
        "life",
        &name,
        &json!({
            "action": "started",
            "by": "cli",
            "reason": "orphan_recover",
            "orphan_pid": pid,
        }),
    )
    .ok();

    pidtrack::remove_pid(hcom_dir, pid);

    println!("[hcom:{}]", name);
    if can_reuse {
        println!("Recovered orphan PID {} as '{}'.", pid, name);
    } else {
        println!(
            "Recovered orphan PID {} as new identity '{}' (name conflict/unavailable).",
            pid, name
        );
    }

    log_info(
        "start",
        "orphan.recovered",
        &format!("name={} pid={} tool={}", name, pid, orphan.tool),
    );

    Ok(0)
}

#[derive(Debug, Clone)]
struct ChildLink {
    name: String,
    parent_name: Option<String>,
}

fn snapshot_child_links(db: &HcomDb, session_id: Option<&str>) -> Result<Vec<ChildLink>> {
    let Some(session_id) = session_id.filter(|value| !value.is_empty()) else {
        return Ok(Vec::new());
    };
    let mut stmt = db
        .conn()
        .prepare("SELECT name, parent_name FROM instances WHERE parent_session_id = ?")?;
    let rows = stmt.query_map(rusqlite::params![session_id], |row| {
        Ok(ChildLink {
            name: row.get(0)?,
            parent_name: row.get(1)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn restore_child_links_after_root_rebind(
    db: &HcomDb,
    links: &[ChildLink],
    session_id: &str,
    old_root: &str,
    new_root: &str,
) -> Result<()> {
    db.with_immediate_transaction(|txn| {
        for link in links {
            let parent_name = match link.parent_name.as_deref() {
                Some(parent) if parent == old_root => Some(new_root),
                other => other,
            };
            txn.execute(
                "UPDATE instances SET parent_session_id = ?, parent_name = ? WHERE name = ?",
                rusqlite::params![session_id, parent_name, &link.name],
            )?;
        }
        Ok(())
    })
}

/// Rebind session identity (`--as <name>`), preserving last_event_id and any
/// live Claude child hierarchy owned by the current root actor.
fn start_rebind(
    db: &HcomDb,
    rebind_target: &str,
    ctx: &HcomContext,
    explicit_name: Option<&str>,
) -> Result<i32> {
    let hcom_dir = paths::hcom_dir();

    // Resolve the target name
    let target_name = identity::resolve_display_name_or_stopped(db, rebind_target)
        .unwrap_or_else(|| rebind_target.to_string());

    // Guard: refuse to reclaim a subagent slot. Subagents share their parent's
    // session_id, so `hcom start --as <subagent_name>` from inside a subagent
    // bash would rebind session_bindings[parent_sid] to the subagent name,
    // clobbering the parent's identity. `--as` is documented for top-level
    // restartable identities (compaction/resume/clear), not for subagent
    // lifecycle — which has its own SubagentStart bootstrap path.
    if db.was_subagent_name(&target_name) {
        eprintln!(
            "Error: '{target_name}' is a subagent slot; cannot be reclaimed with --as.\n\
             Subagents register via 'hcom start --name <agent-id>' in the SubagentStart context. If your session ended, stop working and end your turn."
        );
        return Ok(1);
    }

    // Hermes reclaim: a FRESH ordinary Hermes session (no binding, no row
    // holding its id) may take over a compatible STOPPED identity with `--as`
    // and must end up bound to it by this session's id, so later commands
    // resolve the reclaimed name. The decision and every mutation below run
    // under the shared Hermes session-binding lock (same file as bare start
    // and the plugin's adopt), failing closed, so a concurrent start/adopt
    // cannot interleave and leave an unbound or double-bound target.
    let hermes_session_id = (ctx.tool == crate::tool::Tool::Hermes)
        .then(|| resolve_hermes_session_id(&ctx.raw_env))
        .flatten();
    let _hermes_binding_lock = hermes_session_id
        .as_ref()
        .map(|_| lock_hermes_session_binding(db))
        .transpose()?;
    // A Hermes context without HERMES_SESSION_ID (e.g. a manual HCOM_TOOL
    // override) has no durable session id to bind a reclaimed identity
    // through. The strict Hermes binding write below would fail only after
    // the target row has already been deleted — or leave the reclaimed
    // identity unresolvable — so fail closed before any mutation.
    if ctx.tool == crate::tool::Tool::Hermes && hermes_session_id.is_none() {
        bail!(
            "This Hermes session exposes no HERMES_SESSION_ID, so a reclaimed identity \
             could not be bound to it. Start from a real Hermes session — or fix the \
             HCOM_TOOL override — and try again."
        );
    }
    let mut hermes_stale_binding = false;
    if let Some(ref session_id) = hermes_session_id {
        if let Some(bound_name) = db.get_session_binding(session_id)? {
            match db.get_instance_full(&bound_name)? {
                Some(row) if row.tool == "hermes" => {
                    bail!(
                        "Hermes session {} already has hcom identity '{bound_name}'. Refusing \
                         to rebind it; end that identity with `hcom kill {bound_name}` and \
                         start again.",
                        session_id.chars().take(8).collect::<String>()
                    );
                }
                Some(_) => {
                    bail!(
                        "Hermes session {} is bound to '{bound_name}', which is not a Hermes \
                         instance. Refusing to adopt or overwrite that binding.",
                        session_id.chars().take(8).collect::<String>()
                    );
                }
                // Stale binding: its row is gone, so it names nothing — but an
                // inconsistent/legacy DB may still have ANOTHER row owning
                // this sid. The holder check below refuses that case; only a
                // sid with no holder may be taken over by the reclaim.
                None => hermes_stale_binding = true,
            }
        }
        // A row still holding this sid is the session's retained identity
        // whether or not a (possibly stale) binding exists: it must be refused,
        // never REPLACE-clobbered by the reclaim below.
        if let Some(row) = hermes_row_holding_session_id(db, session_id)? {
            if row.tool == "hermes" {
                bail!(
                    "Hermes session {} already has hcom identity '{}' (retained by a \
                     soft-finalized session: row kept, binding dropped). A bare \
                     `hcom start` recovers it; discard it with `hcom kill {}` before \
                     reclaiming another name.",
                    session_id.chars().take(8).collect::<String>(),
                    row.name,
                    row.name
                );
            } else {
                bail!(
                    "Hermes session {} is held by '{}' of another tool. Refusing to \
                     overwrite it.",
                    session_id.chars().take(8).collect::<String>(),
                    row.name
                );
            }
        }
    }

    // A remote Hermes mirror is a reflection owned by another device: the
    // delete-skip for remote rows would leave it to be rewritten in place by
    // the recreation below (session_id, tool, directory), so refuse before any
    // mutation. Its lifecycle belongs to the owning device.
    if hermes_session_id.is_some()
        && let Some(row) = db.get_instance_full(&target_name)?
        && row.tool == "hermes"
        && crate::instances::is_remote_instance(&row)
    {
        bail!(
            "Hermes identity '{target_name}' is a remote mirror owned by another device and \
             cannot be reclaimed here. Reclaim it on the owning device."
        );
    }

    // A Hermes reclaim takes over a STOPPED identity. A live one belongs to a
    // session that may still be running elsewhere: taking it over would steal
    // it, and "succeeding" while leaving this session unbound is worse.
    if hermes_session_id.is_some()
        && let Some(row) = db.get_instance_full(&target_name)?
        && row.tool == "hermes"
        && !matches!(row.status.as_str(), "inactive" | "stopped")
    {
        bail!(
            "Hermes identity '{target_name}' is currently {} and cannot be reclaimed. Only \
             stopped identities can be taken over with --as; if it is stale, `hcom kill \
             {target_name}` first.",
            row.status
        );
    }

    let explicit_current_name = explicit_name.unwrap_or("");

    // Candidate session ids: the process binding for this terminal, then the
    // explicit current identity's row.
    let process_binding = ctx
        .process_id
        .as_deref()
        .and_then(|pid| db.get_process_binding_full(pid).ok().flatten());
    let process_sid = process_binding
        .as_ref()
        .and_then(|(sid, _)| sid.clone().filter(|s| !s.is_empty()));
    let explicit_sid = if explicit_current_name.is_empty() {
        None
    } else {
        db.get_instance_full(explicit_current_name)
            .ok()
            .flatten()
            .and_then(|row| row.session_id.filter(|s| !s.is_empty()))
    };

    let session_id = if let Some(ref hermes_sid) = hermes_session_id {
        // HERMES_SESSION_ID is authoritative for a Hermes reclaim. The
        // stale-binding decision above was computed for it, later commands
        // resolve this session by it, and rebind_session is an unconditional
        // upsert — binding through any other sid could clobber a foreign
        // binding. A process binding or explicit identity naming a different
        // session is therefore a conflict: fail closed before any mutation.
        if let Some(process_sid) = process_sid.as_deref()
            && process_sid != hermes_sid.as_str()
        {
            let owner = process_binding
                .as_ref()
                .map(|(_, name)| name.clone())
                .unwrap_or_default();
            let owner_note = if owner.is_empty() {
                String::new()
            } else {
                format!(" (identity '{owner}'; `hcom kill {owner}` clears it)")
            };
            bail!(
                "Hermes session {} refuses to rebind to '{target_name}': this terminal's \
                 process binding still points at session {}{owner_note} — a different \
                 session than HERMES_SESSION_ID. Reclaim from the Hermes session that owns \
                 the identity, or clear the stale process binding first.",
                hermes_sid.chars().take(8).collect::<String>(),
                process_sid.chars().take(8).collect::<String>()
            );
        }
        if let Some(explicit_sid) = explicit_sid.as_deref()
            && explicit_sid != hermes_sid.as_str()
        {
            bail!(
                "Hermes session {} refuses to rebind to '{target_name}': the current \
                 identity '{explicit_current_name}' belongs to session {} — a different \
                 session than HERMES_SESSION_ID, so it cannot be replaced from here. Start \
                 from the session that owns it, or drop it with \
                 `hcom kill {explicit_current_name}` first.",
                hermes_sid.chars().take(8).collect::<String>(),
                explicit_sid.chars().take(8).collect::<String>()
            );
        }
        if explicit_sid.is_none()
            && db
                .get_instance_full(explicit_current_name)
                .ok()
                .flatten()
                .is_some()
        {
            // A claimed current identity with no session id of its own cannot
            // be vetted against HERMES_SESSION_ID: nothing proves it belongs to
            // this session, and the replace-old-identity step below would
            // delete the row on the caller's word alone.
            bail!(
                "Hermes session {} refuses to treat '{explicit_current_name}' as its current \
                 identity: that row has no session id, so hcom cannot prove it belongs to \
                 this session. If it is a leftover identity, drop it with \
                 `hcom kill {explicit_current_name}` first.",
                hermes_sid.chars().take(8).collect::<String>()
            );
        }
        // Same contract for a fresh ordinary Hermes reclaim: the session id is
        // what makes the takeover stick — the recreated row records it and the
        // binding makes later commands resolve the reclaimed name.
        Some(hermes_sid.clone())
    } else {
        let mut sid = process_sid.or(explicit_sid);
        if sid.is_none() && ctx.tool == crate::tool::Tool::Claude {
            // A vanilla Claude session has neither a process binding nor, before its
            // first start, a row to read the id back from. Its own session id is
            // what makes the rebind stick: without it the reclaimed name stays
            // unbound and the identity it replaces is never cleaned up.
            sid = resolve_claude_session_id(&ctx.raw_env);
        }
        sid
    };
    let current_name = if !explicit_current_name.is_empty() {
        explicit_current_name.to_string()
    } else if let Some(ref sid) = session_id {
        // A stale binding names a deleted row; it is not a current identity.
        db.get_session_binding(sid)?
            .filter(|_| !hermes_stale_binding)
            .unwrap_or_default()
    } else {
        String::new()
    };
    let child_links = snapshot_child_links(db, session_id.as_deref())?;

    let target_meta = load_rebind_target_metadata(db, &target_name).ok();
    if let Some(ref meta) = target_meta {
        ensure_rebind_compatible(&target_name, meta, ctx)?;
    }

    // Preserve last_event_id from target (cursor preservation)
    let mut last_event_id = target_meta.as_ref().map(|m| m.last_event_id);
    let target_data = db.get_instance_full(&target_name)?;

    // Final fallback: use current max to avoid re-delivering old messages
    if last_event_id.is_none() {
        last_event_id = Some(db.get_last_event_id());
    }

    // Skip delete for remote instances (origin_device_id)
    if let Some(ref td) = target_data
        && (td.origin_device_id.is_none() || td.origin_device_id.as_deref() == Some(""))
        && let Err(e) = db.delete_instance(&target_name)
    {
        eprintln!("[hcom] warn: delete_instance failed for {target_name}: {e}");
    }

    // Clean up target's bindings
    if let Err(e) = db.delete_process_bindings_for_instance(&target_name) {
        eprintln!("[hcom] warn: delete_process_bindings failed for {target_name}: {e}");
    }
    if let Err(e) = db.delete_session_bindings_for_instance(&target_name) {
        eprintln!("[hcom] warn: delete_session_bindings failed for {target_name}: {e}");
    }

    // Delete old identity if different from target
    if !current_name.is_empty()
        && current_name != target_name
        && let Err(e) = db.delete_instance(&current_name)
    {
        eprintln!("[hcom] warn: delete_instance failed for {current_name}: {e}");
    }

    // Create fresh instance with the target name
    let tool = ctx.tool.as_str();
    let cwd_override = ctx.cwd.to_string_lossy().to_string();
    let hermes_transcript_path = (ctx.tool == crate::tool::Tool::Hermes)
        .then(|| crate::transcript::hermes::resolve_hermes_db_path_from_env(&ctx.raw_env))
        .flatten()
        .map(|p| p.to_string_lossy().to_string());
    instance_binding::initialize_instance_in_position_file(
        db,
        &target_name,
        session_id.as_deref(),
        None, // parent_session_id
        None, // parent_name
        None, // agent_id
        hermes_transcript_path.as_deref(),
        Some(tool),
        false, // background
        None,  // tag
        None,  // wait_timeout
        None,  // subagent_timeout
        None,  // hints
        Some(&cwd_override),
    );

    if let Some(ref sid) = session_id {
        let old_root = if current_name.is_empty() {
            target_name.as_str()
        } else {
            current_name.as_str()
        };
        restore_child_links_after_root_rebind(db, &child_links, sid, old_root, &target_name)?;
        if old_root != target_name {
            db.rebind_claude_root_actor_state(sid, old_root, &target_name)?;
        }
    }

    // Restore cursor position + mark as announced
    {
        let mut updates = serde_json::Map::new();
        if let Some(eid) = last_event_id {
            updates.insert("last_event_id".into(), serde_json::json!(eid));
        }
        updates.insert("name_announced".into(), serde_json::json!(1));
        if let Err(e) = db.update_instance_fields(&target_name, &updates) {
            eprintln!("[hcom] warn: update_instance_fields failed for {target_name}: {e}");
        }
    }

    // Create bindings
    if let Some(ref sid) = session_id {
        if ctx.tool == crate::tool::Tool::Hermes {
            // The reclaimed identity must end up owning this session, or the
            // command has not done its job — fail instead of reporting
            // success with the session unbound. set_session_binding stays
            // strict (an owner that appeared despite the lock is never
            // silently overwritten); a stale binding is reclaimed by force.
            if hermes_stale_binding {
                db.rebind_session(sid, &target_name)?;
            } else {
                db.set_session_binding(sid, &target_name)?;
            }
            // Plugin-less reclaim: nothing else will ever advance the
            // recreated row's status, so mark the live session listening the
            // way the plugin's adopt path does.
            lifecycle::set_status(
                db,
                &target_name,
                ST_LISTENING,
                "start:reclaim",
                Default::default(),
            );
        } else if let Err(e) = db.set_session_binding(sid, &target_name) {
            eprintln!("[hcom] warn: set_session_binding failed for {target_name}: {e}");
        } else if ctx.tool == crate::tool::Tool::Claude
            && let Err(e) = db.mark_claude_session_validated(sid, &target_name)
        {
            // The cache still names the identity being replaced, and it is keyed
            // by session generation, so it does not expire on its own. Left
            // stale, every hook for this session resolves to no_instance: no
            // status, no delivery, and the reclaimed row is flagged
            // launch_failed ~30s later while the session is alive and bound.
            eprintln!("[hcom] warn: mark_claude_session_validated failed for {target_name}: {e}");
        }
    }
    if let Some(ref process_id) = ctx.process_id {
        let sid = session_id.as_deref().unwrap_or("");
        if let Err(e) = db.set_process_binding(process_id, sid, &target_name) {
            eprintln!("[hcom] warn: set_process_binding failed for {target_name}: {e}");
        }

        // Migrate notify endpoints before notify so wake reaches correct port
        if !current_name.is_empty()
            && current_name != target_name
            && let Err(e) = db.migrate_notify_endpoints(&current_name, &target_name)
        {
            eprintln!("[hcom] warn: migrate_notify_endpoints failed: {e}");
        }

        crate::notify::wake(db, &target_name, crate::notify::WakeKind::DELIVERY_LOOPS);
    }

    // Print bootstrap
    let hcom_config = HcomConfig::load(None).unwrap_or_else(|_| {
        let mut c = HcomConfig::default();
        c.normalize();
        c
    });

    let bootstrap_text = bootstrap::get_bootstrap(
        db,
        &hcom_dir,
        &target_name,
        tool,
        false,
        false,
        &ctx.notes,
        &hcom_config.tag,
        relay::is_relay_enabled(&hcom_config),
        None,
    );

    println!("[hcom:{}]", target_name);
    println!("{}", bootstrap_text);
    // Same reason as bare start: keep the new name visible in a tailed snapshot.
    println!("[hcom:{}]", target_name);

    log_info(
        "start",
        "rebind.complete",
        &format!("from={} to={}", current_name, target_name),
    );

    Ok(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RebindTargetMetadata {
    tool: String,
    directory: String,
    last_event_id: i64,
}

fn ensure_rebind_compatible(
    target_name: &str,
    meta: &RebindTargetMetadata,
    ctx: &HcomContext,
) -> Result<()> {
    let current_tool = ctx.tool.as_str();
    if !meta.tool.is_empty() && meta.tool != current_tool {
        bail!(
            "Refusing to reclaim '{target_name}': latest identity used tool '{}' but current session is '{}'",
            meta.tool,
            current_tool
        );
    }

    let current_dir = ctx.cwd.to_string_lossy();
    if !meta.directory.is_empty() && !same_path(&meta.directory, &current_dir) {
        bail!(
            "Refusing to reclaim '{target_name}': latest identity used directory '{}' but current session is '{}'",
            meta.directory,
            current_dir
        );
    }

    Ok(())
}

fn same_path(left: &str, right: &str) -> bool {
    normalize_path_for_compare(left) == normalize_path_for_compare(right)
}

fn normalize_path_for_compare(path: &str) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path))
}

/// Load rebind metadata from the live row first, then the latest stopped snapshot.
fn load_rebind_target_metadata(db: &HcomDb, name: &str) -> Result<RebindTargetMetadata> {
    if let Some(inst) = db.get_instance_full(name)? {
        return Ok(RebindTargetMetadata {
            tool: inst.tool,
            directory: inst.directory,
            last_event_id: inst.last_event_id,
        });
    }

    let mut stmt = db.conn().prepare(
        "SELECT data FROM events WHERE type='life' AND instance=? ORDER BY id DESC LIMIT 10",
    )?;

    let rows: Vec<String> = stmt
        .query_map(rusqlite::params![name], |row| row.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();

    for data_str in &rows {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(data_str)
            && data.get("action").and_then(|v| v.as_str()) == Some("stopped")
            && let Some(snapshot) = data.get("snapshot")
        {
            return Ok(RebindTargetMetadata {
                tool: snapshot
                    .get("tool")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                directory: snapshot
                    .get("directory")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                last_event_id: snapshot
                    .get("last_event_id")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0),
            });
        }
    }

    bail!("No rebind metadata found for '{}'", name)
}

/// Resolve the Claude session id visible to a CLI invocation.
///
/// Two sources, in order:
/// - `HCOM_CLAUDE_UNIX_SESSION_ID`: hcom's own SessionStart hook appends this
///   export to `CLAUDE_ENV_FILE`, which Claude runs before each Bash command.
/// - `CLAUDE_CODE_SESSION_ID`: Claude sets this directly in every Bash and
///   PowerShell subprocess, and it matches the `session_id` hooks receive.
///
/// The env-file round trip is the fragile one: it needs `CLAUDE_ENV_FILE` to
/// exist and our SessionStart to have run in this session generation. Without
/// the second source, a session that misses it cannot be recognized on a repeat
/// `hcom start`, which then mints a SECOND identity — the first stays bound to
/// nothing and later reports as launch_failed. Both values are set by Claude
/// for the session running this command, so either one binds identity.
fn resolve_claude_session_id(env: &HashMap<String, String>) -> Option<String> {
    ["HCOM_CLAUDE_UNIX_SESSION_ID", "CLAUDE_CODE_SESSION_ID"]
        .into_iter()
        .find_map(|key| env.get(key).filter(|value| !value.is_empty()).cloned())
}

/// Ordinary Hermes exports `HERMES_SESSION_ID` into every terminal subprocess
/// (hermes `tools/environments/local.py _inject_session_context_env`), and it
/// is the session's own durable id. It plays for Hermes the role
/// `CLAUDE_CODE_SESSION_ID` plays above: without keying on it, every bare
/// `hcom start` in one Hermes session mints a second identity. An empty export
/// is not identity (it is also not a Hermes marker at all).
fn resolve_hermes_session_id(env: &HashMap<String, String>) -> Option<String> {
    env.get("HERMES_SESSION_ID")
        .filter(|value| !value.is_empty())
        .cloned()
}

/// Take the cross-process Hermes session-binding lock, shared with the
/// plugin's adopt path (hooks/hermes.rs, same lock file), so that concurrent
/// identity decisions for one Hermes session — two bare starts, or a bare
/// start racing `hermes-sessionstart` — cannot both observe "unbound" and
/// mint two identities, and a stale-binding reclaim cannot overwrite a
/// binding another process just created. Fails closed: without the lock the
/// decision-to-write sequence has no protection — the mint's INSERT OR
/// REPLACE lands before set_session_binding's conflict check, and a stale
/// reclaim's rebind is an unconditional upsert — so an unacquirable lock
/// must abort the start, never proceed unlocked.
fn lock_hermes_session_binding(db: &HcomDb) -> Result<std::fs::File> {
    let lock_dir = db
        .path()
        .parent()
        .ok_or_else(|| anyhow::anyhow!("db path has no parent dir"))?
        .join(".tmp");
    std::fs::create_dir_all(&lock_dir).inspect_err(|error| {
        log_warn(
            "start",
            "hermes.binding_lock_dir_failed",
            &error.to_string(),
        );
    })?;
    let lock_path = lock_dir.join("hermes_adopt.lock");
    let lock_file = std::fs::File::options()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|error| {
            log_warn(
                "start",
                "hermes.binding_lock_open_failed",
                &error.to_string(),
            );
            anyhow::anyhow!(
                "cannot open Hermes session-binding lock at {}: {error}",
                lock_path.display()
            )
        })?;
    crate::sys::fs::lock_exclusive(&lock_file)
        .inspect_err(|error| {
            log_warn("start", "hermes.binding_lock_failed", &error.to_string());
        })
        .map_err(|error| {
            anyhow::anyhow!(
                "cannot acquire Hermes session-binding lock at {}: {error}",
                lock_path.display()
            )
        })?;
    Ok(lock_file)
}

/// The instance row currently holding this session id, if any. A
/// soft-finalized Hermes session keeps its row and session_id after
/// `soft_finalize_session` drops only the binding, so a start that finds no
/// binding must look here before minting: a fresh row with the same session_id
/// would hit `UNIQUE(session_id)` and `save_instance_named`'s INSERT OR
/// REPLACE would delete the retained row. Read errors propagate — swallowing
/// one into "no row" would let the caller mint over a row it failed to see.
fn hermes_row_holding_session_id(db: &HcomDb, session_id: &str) -> Result<Option<InstanceRow>> {
    use rusqlite::OptionalExtension;
    let name: Option<String> = db
        .conn()
        .query_row(
            "SELECT name FROM instances WHERE session_id = ?",
            rusqlite::params![session_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(anyhow::Error::new)?;
    match name {
        Some(name) => Ok(db.get_instance_full(&name)?),
        None => Ok(None),
    }
}

/// Live local Claude instances in this directory that no session id points at.
///
/// These are the plausible earlier identities of a session that exposes no id
/// of its own — the only useful thing to say when hcom cannot recognize it.
fn unbound_claude_candidates(db: &HcomDb, ctx: &HcomContext, exclude: &str) -> Vec<String> {
    let cwd = ctx.cwd.to_string_lossy();
    let mut rows: Vec<InstanceRow> = db
        .iter_instances_full()
        .unwrap_or_default()
        .into_iter()
        .filter(|row| {
            row.tool == "claude"
                && row.status != "stopped"
                && row.name != exclude
                && row.directory == cwd
                && row.session_id.is_none()
                && row.parent_name.is_none()
                && !crate::instances::is_remote_instance(row)
        })
        .collect();
    rows.sort_by(|a, b| b.created_at.total_cmp(&a.created_at));
    rows.truncate(4);
    rows.into_iter().map(|row| row.name).collect()
}

/// Path C: Bare start — detect tool or create adhoc instance.
fn start_bare(
    db: &HcomDb,
    hcom_dir: &std::path::Path,
    ctx: &HcomContext,
    explicit_name: Option<&str>,
) -> Result<i32> {
    let explicit_name = explicit_name
        .map(|name| identity::resolve_display_name(db, name).unwrap_or_else(|| name.to_string()));
    let explicit_name = explicit_name.as_deref();

    // Skip vanilla detection if --name is provided with an existing instance
    let has_valid_identity = explicit_name
        .and_then(|n| db.get_instance_full(n).ok().flatten())
        .is_some();

    // Vanilla tool detection: auto-install hooks for unmanaged AI tools.
    // Identity is already canonical on HcomContext, so route every released
    // hook-bearing integration through the typed Tool hook adapter. This keeps
    // bare `hcom start` aligned with `hcom hooks add` as integrations evolve.
    if !has_valid_identity && ctx.detect_vanilla_tool().is_some() {
        let vanilla_tool = ctx.tool;
        if vanilla_tool != crate::tool::Tool::Hermes
            && !vanilla_tool.hooks().is_empty()
            && !vanilla_tool.verify_hooks_installed(false)
        {
            println!("Installing {} hooks...", vanilla_tool.as_str());
            let include_perms = crate::config::load_config_snapshot().core.auto_approve;
            match vanilla_tool.try_setup_hooks(include_perms) {
                Ok(()) => {
                    println!(
                        "\nRestart {} to enable automatic message delivery.",
                        vanilla_tool.spec().label
                    );
                    println!("Then run: hcom start");
                }
                Err(error) if error.is_empty() => {
                    eprintln!(
                        "Failed to install hooks. Run: hcom hooks add {}",
                        vanilla_tool.as_str()
                    );
                }
                Err(error) => {
                    eprintln!(
                        "Failed to install {} hooks: {error}\nRun: hcom hooks add {}",
                        vanilla_tool.as_str(),
                        vanilla_tool.as_str()
                    );
                }
            }
            return Ok(1);
        }

        // Gemini: ensure hooksConfig.enabled is set (self-heal for v0.26.0+)
        if vanilla_tool == crate::tool::Tool::Gemini {
            let _ = crate::hooks::gemini::ensure_hooks_enabled();
        }
    }

    let tool = ctx.tool.as_str();
    let claude_session_id = (ctx.tool == crate::tool::Tool::Claude)
        .then(|| resolve_claude_session_id(&ctx.raw_env))
        .flatten();

    if explicit_name.is_none()
        && let Some(ref session_id) = claude_session_id
        && let Some(bound_name) = db.get_session_binding(session_id)?
    {
        // Only hcom writes session bindings, so a row keyed by this session's
        // own id is trusted identity evidence. Heal bindings created by older
        // versions before returning the existing row.
        db.mark_claude_session_validated(session_id, &bound_name)?;
        println!("hcom already started for {bound_name}");
        return Ok(0);
    }

    // Ordinary Hermes explicit start is idempotent by the same session-id
    // contract as Claude, standing on its own — no plugin required: reuse only
    // a binding whose row is hermes-owned, never touch one owned by another
    // tool, recover the retained row of a soft-finalized session instead of
    // minting over it, and reclaim a binding whose row is gone. Everything
    // from the decision through the binding write below runs under the shared
    // Hermes session-binding lock, so concurrent starts of one session
    // converge on one identity. The lock fails closed: an unacquirable lock
    // aborts the start before any decision or write, because unlocked the
    // mint's INSERT OR REPLACE lands before the binding conflict check and a
    // stale reclaim's rebind is an unconditional upsert.
    let hermes_session_id = (ctx.tool == crate::tool::Tool::Hermes)
        .then(|| resolve_hermes_session_id(&ctx.raw_env))
        .flatten();
    let hermes_transcript_path = (ctx.tool == crate::tool::Tool::Hermes)
        .then(|| crate::transcript::hermes::resolve_hermes_db_path_from_env(&ctx.raw_env))
        .flatten()
        .map(|p| p.to_string_lossy().to_string());
    let _hermes_binding_lock = hermes_session_id
        .as_ref()
        .map(|_| lock_hermes_session_binding(db))
        .transpose()?;
    let mut reclaim_hermes_binding = false;
    if let Some(ref session_id) = hermes_session_id {
        if let Some(bound_name) = db.get_session_binding(session_id)? {
            match db.get_instance_full(&bound_name)? {
                Some(row) if row.tool == "hermes" => {
                    // An established identity is final for this session: a
                    // different explicit name would create a second identity
                    // (or fight the binding), so refuse before any mutation.
                    if explicit_name.is_some_and(|explicit| explicit != bound_name) {
                        bail!(
                            "Hermes session {} already has hcom identity '{bound_name}'. \
                             Refusing to create another identity for the same session.",
                            session_id.chars().take(8).collect::<String>()
                        );
                    }
                    if row.transcript_path.is_empty()
                        && let Some(ref path) = hermes_transcript_path
                    {
                        let mut updates = serde_json::Map::new();
                        updates.insert("transcript_path".into(), serde_json::json!(path));
                        instances::update_instance_position(db, &row.name, &updates);
                    }
                    // A regressed row — inactive/stopped, or still on its
                    // fresh "new" context — with the session factually live:
                    // plugin-less Hermes has no hook to advance it, so heal it
                    // to listening like the recover path. A genuinely
                    // active/blocked/listening/launching row carries live
                    // delivery or launch evidence this path must not overwrite.
                    let regressed = matches!(row.status.as_str(), "inactive" | "stopped")
                        || (row.status_context == "new"
                            && !matches!(
                                row.status.as_str(),
                                "active" | "blocked" | "listening" | "launching"
                            ));
                    if regressed {
                        lifecycle::set_status(
                            db,
                            &row.name,
                            ST_LISTENING,
                            "start:heal",
                            Default::default(),
                        );
                    }
                    println!("hcom already started for {bound_name}");
                    return Ok(0);
                }
                // A Hermes session id resolving to another tool's row is a binding
                // collision — not ours to adopt or overwrite. Reported as a bare
                // refusal: an explicit `--as` can hit the same collision, so it is
                // no remedy to advertise.
                Some(_) => {
                    bail!(
                        "Hermes session {} is already bound to '{bound_name}', which is not a \
                         Hermes instance. Refusing to adopt or overwrite that binding.",
                        session_id.chars().take(8).collect::<String>()
                    );
                }
                // Stale binding: its instance row was deleted, so it points at
                // nothing — but an inconsistent/legacy DB may still have
                // ANOTHER row owning this sid. The holder check below recovers
                // that row; only a sid with no holder may be reclaimed by the
                // mint, whose INSERT OR REPLACE would silently delete one.
                None => reclaim_hermes_binding = true,
            }
        }
        // A row still holding this sid is the session's retained identity
        // whether or not a (possibly stale) binding exists (a soft-finalized
        // session keeps row and session_id after its binding is dropped):
        // recover it — minting instead would INSERT OR REPLACE it away on
        // UNIQUE(session_id). In the stale-binding case rebind_instance_session
        // also heals the binding to name the recovered row.
        if let Some(row) = hermes_row_holding_session_id(db, session_id)? {
            if row.tool != "hermes" {
                bail!(
                    "Hermes session {} is held by '{}', which is not a Hermes \
                     instance. Refusing to adopt or overwrite it.",
                    session_id.chars().take(8).collect::<String>(),
                    row.name
                );
            }
            if explicit_name.is_some_and(|explicit| explicit != row.name) {
                bail!(
                    "Hermes session {} already has hcom identity '{}'. Refusing to \
                     create another identity for the same session.",
                    session_id.chars().take(8).collect::<String>(),
                    row.name
                );
            }
            db.rebind_instance_session(&row.name, session_id)?;
            if row.transcript_path.is_empty()
                && let Some(ref path) = hermes_transcript_path
            {
                let mut updates = serde_json::Map::new();
                updates.insert("transcript_path".into(), serde_json::json!(path));
                instances::update_instance_position(db, &row.name, &updates);
            }
            // Plugin-less recovery: the session is factually live again, and
            // no hook exists to advance the retained row's status — mark it
            // listening the way the plugin's adopt path does.
            lifecycle::set_status(
                db,
                &row.name,
                ST_LISTENING,
                "start:recover",
                Default::default(),
            );
            println!("hcom already started for {}", row.name);
            return Ok(0);
        }

        // An explicit name that is an existing hermes identity belongs to
        // another session and cannot be claimed by bare `--name` regardless of
        // status: a live one would be stolen outright, an inactive one falls
        // through to the already-started return and reports success while
        // leaving this session unbound, and a stopped one would be silently
        // rebound while its old session's binding survives. Stopped/inactive
        // identities are taken over with --as, which clears the previous
        // binding first; live ones need `hcom kill` first. Remote mirrors are
        // excluded here so they reach their own dedicated refusal below —
        // local --as/kill guidance does not apply to another device's mirror.
        if let Some(explicit) = explicit_name
            && let Some(row) = db.get_instance_full(explicit)?
            && row.tool == "hermes"
            && !crate::instances::is_remote_instance(&row)
        {
            if matches!(row.status.as_str(), "inactive" | "stopped") {
                bail!(
                    "Hermes identity '{explicit}' already exists ({}) in another session and \
                     cannot be claimed with --name. Reclaim it with \
                     `hcom start --as {explicit}`, or choose a different name.",
                    row.status
                );
            }
            bail!(
                "Hermes identity '{explicit}' is currently {} in another session and cannot \
                 be claimed here. Only stopped identities can be reclaimed with \
                 `hcom start --as {explicit}`; if it is stale, `hcom kill {explicit}` first.",
                row.status
            );
        }
        // An existing non-Hermes row is another tool's identity, not a name
        // this Hermes session may take: the generic already-started return
        // would report success while leaving this session unbound, and a
        // stopped row would be silently overwritten by the mint below.
        // Remote mirrors keep their own dedicated refusal further down.
        if let Some(explicit) = explicit_name
            && let Some(row) = db.get_instance_full(explicit)?
            && row.tool != "hermes"
            && !crate::instances::is_remote_instance(&row)
        {
            bail!(
                "'{explicit}' is an existing {} identity and cannot be claimed from a \
                 Hermes session with --name. Choose a different name.",
                if row.tool.is_empty() {
                    "other tool"
                } else {
                    row.tool.as_str()
                }
            );
        }
    }

    // Resolve or generate name
    let name = if let Some(n) = explicit_name {
        n.to_string()
    } else {
        instance_names::generate_unique_name(db)?
    };

    // Remote instances are relay mirrors. Starting them remotely is intentionally
    // unsupported because the useful remote lifecycle operations are launch/resume/kill.
    if let Ok(Some(ref existing)) = db.get_instance_full(&name)
        && crate::instances::is_remote_instance(existing)
    {
        bail!("Remote start is not supported for '{name}'. Start it on the owning device instead.");
    }

    // Check if already exists and active (only for explicit names —
    // generate_unique_name creates a placeholder row we must skip past)
    if explicit_name.is_some()
        && let Ok(Some(existing)) = db.get_instance_full(&name)
        && existing.status != "stopped"
    {
        println!("hcom already started for {}", name);
        return Ok(0);
    }

    // A Hermes session binds its identity exactly like a Claude one: the row
    // records the id (stopped-snapshot recovery keys on it) and the session
    // binding makes the next bare start return this identity.
    let created_session_id = claude_session_id
        .as_deref()
        .or(hermes_session_id.as_deref());
    instance_binding::initialize_instance_in_position_file(
        db,
        &name,
        created_session_id,
        None, // parent_session_id
        None, // parent_name
        None, // agent_id
        hermes_transcript_path.as_deref(),
        Some(tool),
        false, // background
        None,  // tag
        None,  // wait_timeout
        None,  // subagent_timeout
        None,  // hints
        None,  // cwd_override
    );

    if let Some(ref session_id) = claude_session_id {
        db.set_session_binding(session_id, &name)?;
        db.mark_claude_session_validated(session_id, &name)?;
    } else if let Some(ref session_id) = hermes_session_id {
        // set_session_binding refuses an id already owned by another row, so
        // neither a foreign binding nor a concurrent binder can be silently
        // overwritten here; a stale one is reclaimed explicitly.
        if reclaim_hermes_binding {
            db.rebind_session(session_id, &name)?;
        } else {
            db.set_session_binding(session_id, &name)?;
        }
        // Plugin-less mint: ordinary Hermes has no hooks, so nothing else will
        // ever advance the fresh row's status — mark the live session listening
        // like recover/reclaim do, or the row sits inactive/new and ages to
        // launch_failed while the session is alive.
        lifecycle::set_status(db, &name, ST_LISTENING, "start:mint", Default::default());
    }

    // Bind process if we have a process_id
    if let Some(ref process_id) = ctx.process_id
        && let Err(e) = db.set_process_binding(process_id, "", &name)
    {
        eprintln!("[hcom] warn: set_process_binding failed for {name}: {e}");
    }

    // Claude builds old enough to expose neither session id leave nothing to
    // recognize this session by, so a later `hcom start` here mints another
    // identity. Say what was just created and name the way back instead of
    // letting the duplicate appear silently.
    if explicit_name.is_none()
        && ctx.tool == crate::tool::Tool::Claude
        && claude_session_id.is_none()
    {
        let candidates = unbound_claude_candidates(db, ctx, &name);
        eprintln!(
            "[hcom] warn: this Claude session exposes no session id, so it was registered \
             as a new identity '{name}'. If it already had one{}, reclaim it with \
             `hcom start --as <name>` and drop this one with `hcom kill {name}`.",
            if candidates.is_empty() {
                String::new()
            } else {
                format!(" (unbound here: {})", candidates.join(", "))
            }
        );
    }

    // Print bootstrap
    let hcom_config = HcomConfig::load(None).unwrap_or_else(|e| {
        eprintln!("[hcom] warn: config load failed, using defaults: {e}");
        let mut c = HcomConfig::default();
        c.normalize();
        c
    });

    let bootstrap_text = bootstrap::get_bootstrap(
        db,
        hcom_dir,
        &name,
        tool,
        false,
        ctx.is_launched,
        &ctx.notes,
        &hcom_config.tag,
        relay::is_relay_enabled(&hcom_config),
        None,
    );

    println!("[hcom:{}]", name);
    println!("{}", bootstrap_text);
    // Repeated deliberately: the header above sits on top of a long bootstrap, so
    // `hcom start | tail -n` shows none of it. A caller that cannot see its own
    // name re-runs start, which is one way duplicate identities appear.
    println!("[hcom:{}]", name);

    // Log
    db.log_event(
        "life",
        &name,
        &json!({
            "action": "started",
            "tool": tool,
            "name": name,
        }),
    )
    .ok();

    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use rusqlite::params;
    use serde_json::json;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn make_ctx(tool_env: &[(&str, &str)], cwd: &str) -> HcomContext {
        let mut env: HashMap<String, String> = std::env::vars().collect();
        for (k, v) in tool_env {
            env.insert((*k).to_string(), (*v).to_string());
        }
        HcomContext::from_env(&env, PathBuf::from(cwd))
    }

    /// Claude context carrying exactly one session-id source, so an ambient
    /// value from the shell running the tests cannot decide the outcome.
    fn make_claude_ctx(session: Option<(&str, &str)>, cwd: &str) -> HcomContext {
        let mut env: HashMap<String, String> = std::env::vars().collect();
        env.remove("HCOM_CLAUDE_UNIX_SESSION_ID");
        env.remove("CLAUDE_CODE_SESSION_ID");
        env.insert("CLAUDECODE".to_string(), "1".to_string());
        if let Some((key, value)) = session {
            env.insert(key.to_string(), value.to_string());
        }
        HcomContext::from_env(&env, PathBuf::from(cwd))
    }

    /// Hermes context whose only tool marker and session-id source is the one
    /// passed in. This suite runs inside AI-tool terminals whose ambient
    /// markers (CLAUDECODE, CODEX_*, …) outrank the Hermes native marker, so
    /// every detection marker is scrubbed and detection is asserted — a stray
    /// ambient marker fails the test loudly instead of silently testing the
    /// wrong tool's path.
    fn make_hermes_ctx(session: Option<&str>, cwd: &str) -> HcomContext {
        let mut env: HashMap<String, String> = std::env::vars().collect();
        for key in crate::shared::tool_detection::tool_marker_vars() {
            env.remove(*key);
        }
        env.retain(|key, _| !key.starts_with("CODEX_"));
        env.remove("CLAUDE_CODE_SESSION_ID");
        env.remove("HCOM_CLAUDE_UNIX_SESSION_ID");
        // The suite's own terminal runs under an hcom PTY: its process marker
        // must not leak into every Hermes test context. Explicit-process tests
        // use make_hermes_ctx_with_process, which sets their own value.
        env.remove("HCOM_PROCESS_ID");
        if let Some(session_id) = session {
            env.insert("HERMES_SESSION_ID".to_string(), session_id.to_string());
        }
        let ctx = HcomContext::from_env(&env, PathBuf::from(cwd));
        if session.is_some() {
            assert_eq!(
                ctx.tool,
                crate::tool::Tool::Hermes,
                "hermes test context must actually detect as Hermes"
            );
        }
        ctx
    }

    /// Same as [`make_hermes_ctx`], plus the hcom process binding marker, so a
    /// test can reproduce a terminal whose PTY already carries a process
    /// binding row. The ambient HCOM_PROCESS_ID of the suite's own terminal is
    /// overridden, not inherited.
    fn make_hermes_ctx_with_process(session: &str, process_id: &str, cwd: &str) -> HcomContext {
        let mut env: HashMap<String, String> = std::env::vars().collect();
        for key in crate::shared::tool_detection::tool_marker_vars() {
            env.remove(*key);
        }
        env.retain(|key, _| !key.starts_with("CODEX_"));
        env.remove("CLAUDE_CODE_SESSION_ID");
        env.remove("HCOM_CLAUDE_UNIX_SESSION_ID");
        env.insert("HERMES_SESSION_ID".to_string(), session.to_string());
        env.insert("HCOM_PROCESS_ID".to_string(), process_id.to_string());
        let ctx = HcomContext::from_env(&env, PathBuf::from(cwd));
        assert_eq!(
            ctx.tool,
            crate::tool::Tool::Hermes,
            "hermes test context must actually detect as Hermes"
        );
        assert_eq!(
            ctx.process_id.as_deref(),
            Some(process_id),
            "process-id test context must carry the process marker"
        );
        ctx
    }

    fn hermes_instance_names(db: &HcomDb) -> Vec<String> {
        let mut names: Vec<String> = db
            .iter_instances_full()
            .unwrap()
            .into_iter()
            .filter(|row| row.tool == "hermes")
            .map(|row| row.name)
            .collect();
        names.sort();
        names
    }

    fn log_stopped_snapshot(
        db: &HcomDb,
        name: &str,
        tool: &str,
        directory: &str,
        session_id: &str,
        last_event_id: i64,
    ) {
        db.log_event(
            "life",
            name,
            &json!({
                "action": "stopped",
                "snapshot": {
                    "tool": tool,
                    "directory": directory,
                    "session_id": session_id,
                    "last_event_id": last_event_id
                }
            }),
        )
        .unwrap();
    }
    #[test]
    fn test_start_args_bare() {
        let args = StartArgs::try_parse_from(["start"]).unwrap();
        assert!(args.orphan.is_none());
        assert!(args.as_name.is_none());
    }

    #[test]
    fn test_start_args_orphan() {
        let args = StartArgs::try_parse_from(["start", "--orphan", "1234"]).unwrap();
        assert_eq!(args.orphan, Some("1234".to_string()));
        assert!(args.as_name.is_none());
    }

    #[test]
    fn test_start_args_rebind() {
        let args = StartArgs::try_parse_from(["start", "--as", "luna"]).unwrap();
        assert!(args.orphan.is_none());
        assert_eq!(args.as_name, Some("luna".to_string()));
    }

    #[test]
    fn test_start_args_bare_as_errors() {
        let err = StartArgs::try_parse_from(["start", "--as"]);
        assert!(err.is_err());
    }

    #[test]
    fn test_start_args_bare_orphan_errors() {
        let err = StartArgs::try_parse_from(["start", "--orphan"]);
        assert!(err.is_err());
    }

    #[test]
    fn test_start_args_unknown_flag_errors() {
        let err = StartArgs::try_parse_from(["start", "--bogus"]);
        assert!(err.is_err());
    }

    #[test]
    #[serial]
    fn test_start_subagent_emits_connected_lifecycle_event_once() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('nova', 'sess-1', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, parent_session_id, parent_name, agent_id, tool, status,
                  status_time, last_seen, created_at)
                 VALUES ('nova_task_1', 'sess-1', 'nova', 'agent-1', 'claude',
                         'inactive', 0, 0, 0)",
                [],
            )
            .unwrap();

        let connected_events = |db: &HcomDb| -> Vec<String> {
            let mut stmt = db
                .conn()
                .prepare(
                    "SELECT data FROM events
                     WHERE type = 'life' AND instance = 'nova_task_1'
                       AND json_extract(data, '$.action') = 'connected'",
                )
                .unwrap();
            stmt.query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect()
        };

        let row = db.get_instance_full("nova_task_1").unwrap().unwrap();
        assert_eq!(start_subagent(&db, &row).unwrap(), 0);

        let events = connected_events(&db);
        assert_eq!(
            events.len(),
            1,
            "first start must emit exactly one connected event"
        );
        let data: serde_json::Value = serde_json::from_str(&events[0]).unwrap();
        assert_eq!(data.get("parent").and_then(|v| v.as_str()), Some("nova"));
        assert_eq!(data.get("by").and_then(|v| v.as_str()), Some("cli"));
        // Task delivery semantics ride on the same one-shot gate: the
        // bootstrap is consumed exactly once, and so is the signal.
        assert_eq!(
            db.get_instance_full("nova_task_1")
                .unwrap()
                .unwrap()
                .name_announced,
            1
        );

        // Idempotent repeat: no second bootstrap, no second signal.
        let row = db.get_instance_full("nova_task_1").unwrap().unwrap();
        assert_eq!(start_subagent(&db, &row).unwrap(), 0);
        assert_eq!(
            connected_events(&db).len(),
            1,
            "repeat start must not re-emit the connected event"
        );

        // Cross-path: a hook activation ack for the same row loses the
        // atomic claim, emits nothing, but still advances the cursor.
        let hook_ack = crate::hooks::DeliveryAck {
            instance_name: "nova_task_1".to_string(),
            last_event_id: 3,
            status_context: "deliver:nova".to_string(),
            msg_ts: "2026-01-01T00:00:01Z".to_string(),
            mark_announced: true,
        };
        crate::hooks::common::commit_delivery_ack(&db, &hook_ack);
        assert_eq!(
            connected_events(&db).len(),
            1,
            "hook activation after CLI start must not re-emit connected"
        );
        assert_eq!(
            db.get_instance_full("nova_task_1")
                .unwrap()
                .unwrap()
                .last_event_id,
            3,
            "the losing ack still advances the delivery cursor"
        );
    }

    #[test]
    #[serial]
    fn test_start_rejects_remote_instances() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, origin_device_id, created_at) VALUES (?1, ?2, ?3)",
                params![
                    "luna:ABCD",
                    "remote-device",
                    crate::shared::time::now_epoch_f64()
                ],
            )
            .unwrap();

        let flags = crate::router::GlobalFlags {
            name: Some("luna:ABCD".to_string()),
            go: false,
        };
        let err = run(&["start".to_string()], &flags).unwrap_err();
        assert!(
            err.to_string().contains("Remote start is not supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    #[serial]
    fn test_vanilla_claude_start_immediately_binds_exported_session() {
        struct RestoreEnv(Option<std::ffi::OsString>);
        impl Drop for RestoreEnv {
            fn drop(&mut self) {
                unsafe {
                    match self.0.take() {
                        Some(value) => std::env::set_var("HCOM_CLAUDE_UNIX_SESSION_ID", value),
                        None => std::env::remove_var("HCOM_CLAUDE_UNIX_SESSION_ID"),
                    }
                }
            }
        }

        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        assert!(crate::hooks::claude::setup_claude_hooks(false));

        let _restore = RestoreEnv(std::env::var_os("HCOM_CLAUDE_UNIX_SESSION_ID"));
        unsafe {
            std::env::set_var("HCOM_CLAUDE_UNIX_SESSION_ID", "sess-vanilla");
        }
        let ctx = make_ctx(&[("CLAUDECODE", "1")], "/tmp/project");

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let name = db
            .get_session_binding("sess-vanilla")
            .unwrap()
            .expect("bare vanilla start must bind immediately");
        let row = db.get_instance_full(&name).unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("sess-vanilla"));
        assert_eq!(row.tool, "claude");
        assert_eq!(
            db.get_validated_claude_session_owner("sess-vanilla")
                .unwrap()
                .as_deref(),
            Some(name.as_str()),
            "CLI-created Claude bindings must be immediately trusted by hooks"
        );

        let transcript = hcom_dir.join("vanilla.jsonl");
        std::fs::write(&transcript, "{\"sessionId\":\"sess-vanilla\"}\n").unwrap();
        let mut hook_ctx = ctx.clone();
        hook_ctx.process_id = None;
        let (resolved, _, _) = crate::hooks::common::init_hook_context(
            &db,
            &hook_ctx,
            "sess-vanilla",
            transcript.to_str().unwrap(),
        );
        assert_eq!(resolved.as_deref(), Some(name.as_str()));

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("sess-vanilla").unwrap().as_deref(),
            Some(name.as_str()),
            "repeated bare start must retain the existing vanilla identity"
        );
    }

    #[test]
    fn test_resolve_claude_session_id_sources() {
        let env = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect()
        };

        assert_eq!(
            resolve_claude_session_id(&env(&[
                ("HCOM_CLAUDE_UNIX_SESSION_ID", "hook-sess"),
                ("CLAUDE_CODE_SESSION_ID", "claude-sess"),
            ])),
            Some("hook-sess".to_string()),
            "our own export stays the first source"
        );
        assert_eq!(
            resolve_claude_session_id(&env(&[("CLAUDE_CODE_SESSION_ID", "claude-sess")])),
            Some("claude-sess".to_string()),
            "Claude's own Bash env carries identity when the env file cannot"
        );
        assert_eq!(
            resolve_claude_session_id(&env(&[
                ("HCOM_CLAUDE_UNIX_SESSION_ID", ""),
                ("CLAUDE_CODE_SESSION_ID", "claude-sess"),
            ])),
            Some("claude-sess".to_string()),
            "an empty export is not identity"
        );
        assert_eq!(resolve_claude_session_id(&env(&[])), None);
    }

    #[test]
    #[serial]
    fn test_vanilla_claude_start_reuses_claude_code_session_id() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        assert!(crate::hooks::claude::setup_claude_hooks(false));

        // No CLAUDE_ENV_FILE round trip, so HCOM_CLAUDE_UNIX_SESSION_ID never
        // arrives — the case that used to mint a second identity per start.
        let ctx = make_claude_ctx(
            Some(("CLAUDE_CODE_SESSION_ID", "sess-claude-env")),
            "/tmp/project",
        );

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let name = db
            .get_session_binding("sess-claude-env")
            .unwrap()
            .expect("CLAUDE_CODE_SESSION_ID must bind identity");
        assert_eq!(
            db.get_validated_claude_session_owner("sess-claude-env")
                .unwrap()
                .as_deref(),
            Some(name.as_str()),
            "hooks must trust the binding the CLI just created"
        );

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("sess-claude-env")
                .unwrap()
                .as_deref(),
            Some(name.as_str()),
            "repeat start must return the first identity, not mint a second"
        );
        let claude_rows: Vec<String> = db
            .iter_instances_full()
            .unwrap()
            .into_iter()
            .filter(|row| row.tool == "claude")
            .map(|row| row.name)
            .collect();
        assert_eq!(claude_rows, vec![name], "exactly one identity per session");
    }

    #[test]
    #[serial]
    fn test_hermes_bare_start_is_idempotent_by_session_id() {
        // The bead's isolated reproduction: two bare starts inside one
        // ordinary Hermes session (same HERMES_SESSION_ID) used to mint two
        // different identities.
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let first = db
            .get_session_binding("audit-session-1")
            .unwrap()
            .expect("first bare start must bind the Hermes session");
        let row = db.get_instance_full(&first).unwrap().unwrap();
        assert_eq!(row.tool, "hermes");
        assert_eq!(row.session_id.as_deref(), Some("audit-session-1"));

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("audit-session-1")
                .unwrap()
                .as_deref(),
            Some(first.as_str()),
            "repeat start must return the first identity, not mint a second"
        );
        assert_eq!(
            hermes_instance_names(&db),
            vec![first],
            "exactly one identity per Hermes session"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_bare_start_mints_listening_identity() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let name = db
            .get_session_binding("audit-session-1")
            .unwrap()
            .expect("bare start must bind the Hermes session");
        let row = db.get_instance_full(&name).unwrap().unwrap();
        // Ordinary Hermes is plugin-less: no hook will ever advance the fresh
        // row's status, so the mint must — or the identity sits inactive/new
        // and ages to launch_failed while the session is alive and bound.
        assert_eq!(
            row.status, "listening",
            "a freshly minted plugin-less Hermes identity must be listening"
        );

        // The stale-binding reclaim mint shares the same binding write, so it
        // lands listening too rather than resurrecting an inactive identity.
        let stale_ctx = make_hermes_ctx(Some("audit-session-2"), "/tmp/project");
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('ghost', 'audit-session-2', 'hermes', 'inactive', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("audit-session-2", "ghost").unwrap();
        db.conn()
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 DELETE FROM instances WHERE name = 'ghost';
                 PRAGMA foreign_keys=ON;",
            )
            .unwrap();
        assert_eq!(start_bare(&db, &hcom_dir, &stale_ctx, None).unwrap(), 0);
        let reclaimed = db.get_session_binding("audit-session-2").unwrap().unwrap();
        assert_ne!(reclaimed, "ghost");
        assert_eq!(
            db.get_instance_full(&reclaimed).unwrap().unwrap().status,
            "listening",
            "a reclaimed stale binding must also land on a listening identity"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_distinct_sessions_get_distinct_identities() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx_a = make_hermes_ctx(Some("hermes-ses-a"), "/tmp/project");
        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");

        assert_eq!(start_bare(&db, &hcom_dir, &ctx_a, None).unwrap(), 0);
        assert_eq!(start_bare(&db, &hcom_dir, &ctx_b, None).unwrap(), 0);

        let name_a = db.get_session_binding("hermes-ses-a").unwrap().unwrap();
        let name_b = db.get_session_binding("hermes-ses-b").unwrap().unwrap();
        assert_ne!(name_a, name_b, "each Hermes session owns its identity");
        assert_eq!(
            db.get_instance_full(&name_a).unwrap().unwrap().session_id,
            Some("hermes-ses-a".to_string())
        );
        assert_eq!(
            db.get_instance_full(&name_b).unwrap().unwrap().session_id,
            Some("hermes-ses-b".to_string())
        );
        assert_eq!(hermes_instance_names(&db).len(), 2);

        // Repeats in either session stay on that session's own identity.
        assert_eq!(start_bare(&db, &hcom_dir, &ctx_a, None).unwrap(), 0);
        assert_eq!(start_bare(&db, &hcom_dir, &ctx_b, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("hermes-ses-a").unwrap().as_deref(),
            Some(name_a.as_str())
        );
        assert_eq!(
            db.get_session_binding("hermes-ses-b").unwrap().as_deref(),
            Some(name_b.as_str())
        );
    }

    #[test]
    #[serial]
    fn test_hermes_bare_start_refuses_foreign_session_binding() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // A live non-Hermes row already owning this session id: a collision
        // hcom must not resolve by adopting or overwriting it.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('luna', 'audit-session-1', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("audit-session-1", "luna").unwrap();

        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");
        let err = start_bare(&db, &hcom_dir, &ctx, None).unwrap_err();
        assert!(
            err.to_string().contains("not a Hermes instance"),
            "unexpected error: {err}"
        );
        assert_eq!(
            db.get_session_binding("audit-session-1")
                .unwrap()
                .as_deref(),
            Some("luna"),
            "the foreign binding must be left exactly as it was"
        );
        assert!(
            hermes_instance_names(&db).is_empty(),
            "a refused start must not mint a Hermes identity either"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_bare_start_reclaims_stale_session_binding() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // Binding whose instance row is gone: the schema cascades deletes, so
        // this state needs a pre-foreign-key database (or external edit) —
        // drop the row with enforcement off to reproduce it.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('ghost', 'audit-session-1', 'hermes', 'inactive', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("audit-session-1", "ghost").unwrap();
        db.conn()
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 DELETE FROM instances WHERE name = 'ghost';
                 PRAGMA foreign_keys=ON;",
            )
            .unwrap();
        assert!(
            db.get_instance_full("ghost").unwrap().is_none(),
            "fixture must leave a binding with no instance row"
        );

        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let bound = db.get_session_binding("audit-session-1").unwrap().unwrap();
        assert_ne!(bound, "ghost");
        let row = db.get_instance_full(&bound).unwrap().unwrap();
        assert_eq!(row.tool, "hermes");
        assert_eq!(row.session_id.as_deref(), Some("audit-session-1"));

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("audit-session-1")
                .unwrap()
                .as_deref(),
            Some(bound.as_str()),
            "the reclaimed identity is now the idempotent one"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_bare_start_recovers_soft_finalized_session_row() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let name = db.get_session_binding("audit-session-1").unwrap().unwrap();
        let created_at = db.get_instance_full(&name).unwrap().unwrap().created_at;

        // The real soft-finalize path: binding dropped, row + session_id kept.
        crate::hooks::common::soft_finalize_session(&db, &name, "session end", None, true);
        assert!(
            db.get_session_binding("audit-session-1").unwrap().is_none(),
            "fixture: soft finalize drops the binding"
        );
        let row = db.get_instance_full(&name).unwrap().unwrap();
        assert_eq!(
            row.session_id.as_deref(),
            Some("audit-session-1"),
            "fixture: soft finalize retains the row's session id"
        );

        // Next bare start must recover that row, not mint over it: a fresh row
        // with the same session_id would INSERT OR REPLACE on UNIQUE(session_id)
        // and destroy the retained row.
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("audit-session-1")
                .unwrap()
                .as_deref(),
            Some(name.as_str()),
            "the soft-finalized identity must be recovered, not replaced"
        );
        assert_eq!(
            hermes_instance_names(&db),
            vec![name.clone()],
            "no second identity may be minted for the session"
        );
        assert_eq!(
            db.get_instance_full(&name).unwrap().unwrap().created_at,
            created_at,
            "the retained row must survive intact, not be deleted and recreated"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_bare_start_stale_binding_recovers_row_holder() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // Inconsistent legacy state: the binding names a deleted row while
        // 'ghost' still owns the session id. The mint used to INSERT OR REPLACE
        // 'ghost' away on UNIQUE(session_id) instead of noticing it.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('ghost', 'hermes-ses-b', 'hermes', 'listening', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, tool, status, status_time, last_seen, created_at)
                 VALUES ('lost', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("hermes-ses-b", "lost").unwrap();
        db.conn()
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 DELETE FROM instances WHERE name = 'lost';
                 PRAGMA foreign_keys=ON;",
            )
            .unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        assert_eq!(start_bare(&db, &hcom_dir, &ctx_b, None).unwrap(), 0);
        // The holder is recovered and the stale binding heals to name it.
        assert_eq!(
            db.get_session_binding("hermes-ses-b").unwrap().as_deref(),
            Some("ghost")
        );
        let row = db.get_instance_full("ghost").unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-b"));
        assert_eq!(row.status, "listening");
        assert_eq!(
            hermes_instance_names(&db),
            vec!["ghost".to_string()],
            "no second identity minted over the holder"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_bare_start_heals_regressed_bound_row() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let name = db.get_session_binding("audit-session-1").unwrap().unwrap();

        // A regressed bound row (stopped in place with the binding kept): the
        // session is factually live, so the idempotent start heals it.
        let set_status_raw = |status: &str| {
            let mut updates = serde_json::Map::new();
            updates.insert("status".into(), serde_json::json!(status));
            db.update_instance_fields(&name, &updates).unwrap();
        };
        set_status_raw("stopped");
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        assert_eq!(
            db.get_instance_full(&name).unwrap().unwrap().status,
            "listening",
            "a regressed bound row must heal to listening on the idempotent return"
        );

        // Explicit start by the bound name heals the same way.
        set_status_raw("inactive");
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, Some(&name)).unwrap(), 0);
        assert_eq!(
            db.get_instance_full(&name).unwrap().unwrap().status,
            "listening"
        );

        // Genuinely active/blocked rows carry delivery evidence and keep it.
        for preserved in ["active", "blocked"] {
            set_status_raw(preserved);
            assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
            assert_eq!(
                db.get_instance_full(&name).unwrap().unwrap().status,
                preserved,
                "a genuinely {preserved} row must not be stomped to listening"
            );
        }
        assert_eq!(
            db.get_session_binding("audit-session-1")
                .unwrap()
                .as_deref(),
            Some(name.as_str()),
            "the healed identity keeps its binding"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_start_as_with_existing_binding_fails_before_mutation() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let name = db.get_session_binding("audit-session-1").unwrap().unwrap();

        let err = start_rebind(&db, "nova", &ctx, None).unwrap_err();
        assert!(
            err.to_string().contains("already has hcom identity"),
            "unexpected error: {err}"
        );
        // Refused before any mutation: no target row, no binding change.
        assert!(db.get_instance_full("nova").unwrap().is_none());
        assert_eq!(
            db.get_session_binding("audit-session-1")
                .unwrap()
                .as_deref(),
            Some(name.as_str())
        );
    }

    #[test]
    #[serial]
    fn test_hermes_start_as_refuses_soft_finalized_session_row() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let name = db.get_session_binding("audit-session-1").unwrap().unwrap();
        // Soft finalize: binding dropped, retained row keeps the session id.
        crate::hooks::common::soft_finalize_session(&db, &name, "session end", None, true);

        let err = start_rebind(&db, "nova", &ctx, None).unwrap_err();
        assert!(
            err.to_string().contains("already has hcom identity"),
            "unexpected error: {err}"
        );
        // Refused before any mutation: no orphan row, retained row intact.
        assert!(
            db.get_instance_full("nova").unwrap().is_none(),
            "no target row may be created for a session with a retained identity"
        );
        let row = db.get_instance_full(&name).unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("audit-session-1"));

        // The retained identity remains the one a bare start recovers.
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("audit-session-1")
                .unwrap()
                .as_deref(),
            Some(name.as_str())
        );
    }

    #[test]
    #[serial]
    fn test_hermes_start_fails_closed_when_binding_lock_unavailable() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // Make the lock file impossible to open: a directory where the file
        // must be. Without the lock the decision-to-write sequence is
        // unprotected, so the start must abort rather than proceed unlocked.
        let lock_path = db
            .path()
            .parent()
            .unwrap()
            .join(".tmp")
            .join("hermes_adopt.lock");
        std::fs::create_dir_all(&lock_path).unwrap();

        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");
        let err = start_bare(&db, &hcom_dir, &ctx, None).unwrap_err();
        assert!(
            err.to_string().contains("session-binding lock"),
            "unexpected error: {err}"
        );
        // Failed closed means failed before any mutation at all: no minted
        // identity, no binding, nothing for a later start to trip over.
        assert!(
            hermes_instance_names(&db).is_empty(),
            "no identity may be minted without the lock"
        );
        assert!(db.get_session_binding("audit-session-1").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_hermes_explicit_name_with_existing_binding_fails_before_mutation() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let name = db.get_session_binding("audit-session-1").unwrap().unwrap();

        let err = start_bare(&db, &hcom_dir, &ctx, Some("nova")).unwrap_err();
        assert!(
            err.to_string().contains("already has hcom identity"),
            "unexpected error: {err}"
        );
        assert!(
            db.get_instance_full("nova").unwrap().is_none(),
            "refusal must happen before any row or position is created"
        );
        assert_eq!(
            db.get_session_binding("audit-session-1")
                .unwrap()
                .as_deref(),
            Some(name.as_str())
        );

        // The bound identity's own name is still accepted and idempotent.
        assert_eq!(
            start_bare(&db, &hcom_dir, &ctx, Some(name.as_str())).unwrap(),
            0
        );
    }

    #[test]
    #[serial]
    fn test_hermes_foreign_row_holding_session_id_is_refused() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // No binding, but a live non-Hermes row already holds the session id:
        // minting would INSERT OR REPLACE that row away on UNIQUE(session_id).
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('luna', 'audit-session-1', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();

        let ctx = make_hermes_ctx(Some("audit-session-1"), "/tmp/project");
        let err = start_bare(&db, &hcom_dir, &ctx, None).unwrap_err();
        assert!(
            err.to_string().contains("not a Hermes"),
            "unexpected error: {err}"
        );
        assert_eq!(
            db.get_instance_full("luna").unwrap().unwrap().name,
            "luna",
            "the foreign row must not be replaced or deleted"
        );
        assert!(
            hermes_instance_names(&db).is_empty(),
            "a refused start must not mint a Hermes identity"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_concurrent_same_session_starts_converge_on_one_identity() {
        use std::sync::{Arc, Barrier};

        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let hcom_dir = Arc::new(hcom_dir.to_path_buf());

        let starter = |barrier: Arc<Barrier>, hcom_dir: Arc<PathBuf>| {
            std::thread::spawn(move || {
                // Own connection and context per starter: the same conditions
                // as two independent `hcom start` processes.
                let db = HcomDb::open().unwrap();
                let ctx = make_hermes_ctx(Some("audit-session-race"), "/tmp/project");
                barrier.wait();
                start_bare(&db, &hcom_dir, &ctx, None)
            })
        };
        let a = starter(Arc::clone(&barrier), Arc::clone(&hcom_dir));
        let b = starter(barrier, hcom_dir);
        let (ra, rb) = (a.join().unwrap(), b.join().unwrap());
        assert_eq!(ra.unwrap(), 0, "both concurrent starts must succeed");
        assert_eq!(rb.unwrap(), 0, "the loser must reuse, not conflict");

        let bound = db
            .get_session_binding("audit-session-race")
            .unwrap()
            .expect("the session must end up bound");
        assert_eq!(
            hermes_instance_names(&db),
            vec![bound],
            "exactly one identity may exist for the session"
        );
    }

    /// Fixture: an identity `name` stopped in Hermes session `old_session`
    /// with delivery cursor `cursor`, either as a soft-finalized retained row
    /// (`keep_row`) or fully deleted like a kill (snapshot only).
    fn stopped_hermes_identity(
        db: &HcomDb,
        old_session: &str,
        name: &str,
        cursor: i64,
        keep_row: bool,
    ) -> String {
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at, last_event_id, directory)
                 VALUES (?1, ?2, 'hermes', 'listening', 0, 0, 0, ?3, '/tmp/project')",
                params![name, old_session, cursor],
            )
            .unwrap();
        db.set_session_binding(old_session, name).unwrap();
        crate::hooks::common::soft_finalize_session(db, name, "session end", None, true);
        if !keep_row {
            db.delete_instance(name).unwrap();
        }
        name.to_string()
    }

    #[test]
    #[serial]
    fn test_hermes_fresh_session_reclaims_stopped_identity_via_as() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // vega stopped in session A with a delivery cursor to preserve.
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);

        // A FRESH session B reclaims it; its own session id must own it after.
        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        assert_eq!(start_rebind(&db, "vega", &ctx_b, None).unwrap(), 0);

        assert_eq!(
            db.get_session_binding("hermes-ses-b").unwrap().as_deref(),
            Some("vega"),
            "the reclaiming session's id must be bound to the reclaimed name"
        );
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.tool, "hermes");
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-b"));
        assert_eq!(
            row.last_event_id, 42,
            "the stopped identity's cursor must be preserved"
        );
        assert_eq!(
            row.status, "listening",
            "a plugin-less reclaimed session is live: no hook will ever advance the status"
        );
        assert!(
            db.get_session_binding("hermes-ses-a").unwrap().is_none(),
            "the old session's binding must be gone with its identity"
        );
        // Later hcom commands resolve the reclaimed name from the session id.
        assert_eq!(
            crate::identity::resolve_hermes_session_identity(&db, "hermes-ses-b")
                .map(|identity| identity.name),
            Some("vega".to_string())
        );
        // Idempotence is preserved through the binding: a bare start in the
        // same session returns the reclaimed identity.
        assert_eq!(start_bare(&db, &hcom_dir, &ctx_b, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("hermes-ses-b").unwrap().as_deref(),
            Some("vega")
        );
    }

    #[test]
    #[serial]
    fn test_hermes_fresh_session_reclaims_killed_identity_via_as() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // Row fully deleted (kill): only the stopped snapshot remains.
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 7, false);

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        assert_eq!(start_rebind(&db, "vega", &ctx_b, None).unwrap(), 0);

        assert_eq!(
            db.get_session_binding("hermes-ses-b").unwrap().as_deref(),
            Some("vega")
        );
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-b"));
        assert_eq!(
            row.last_event_id, 7,
            "the cursor comes from the stopped snapshot when the row was deleted"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_as_refuses_live_identity() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('vega', 'hermes-ses-a', 'hermes', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("hermes-ses-a", "vega").unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_rebind(&db, "vega", &ctx_b, None).unwrap_err();
        assert!(
            err.to_string().contains("cannot be reclaimed"),
            "unexpected error: {err}"
        );
        // Refused before mutation: the live identity and its binding stand.
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.status, "active");
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(
            db.get_session_binding("hermes-ses-a").unwrap().as_deref(),
            Some("vega")
        );
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_hermes_as_reclaims_stale_binding_together_with_identity() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);
        // Session B's id carries a stale binding to a deleted row.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('ghost', 'hermes-ses-b', 'hermes', 'inactive', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("hermes-ses-b", "ghost").unwrap();
        db.conn()
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 DELETE FROM instances WHERE name = 'ghost';
                 PRAGMA foreign_keys=ON;",
            )
            .unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        assert_eq!(start_rebind(&db, "vega", &ctx_b, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("hermes-ses-b").unwrap().as_deref(),
            Some("vega"),
            "a stale binding that names nothing is reclaimed, not obeyed"
        );
        assert_eq!(
            db.get_instance_full("vega").unwrap().unwrap().session_id,
            Some("hermes-ses-b".to_string())
        );
    }

    #[test]
    #[serial]
    fn test_hermes_as_refuses_foreign_row_holding_session_id() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);
        // A non-Hermes row already holds session B's id: binding the reclaim
        // would INSERT OR REPLACE that row away on UNIQUE(session_id).
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('luna', 'hermes-ses-b', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_rebind(&db, "vega", &ctx_b, None).unwrap_err();
        assert!(
            err.to_string().contains("another tool"),
            "unexpected error: {err}"
        );
        assert!(
            db.get_instance_full("luna").unwrap().is_some(),
            "the foreign row must not be replaced or deleted"
        );
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
        assert_eq!(
            db.get_instance_full("vega").unwrap().unwrap().session_id,
            Some("hermes-ses-a".to_string()),
            "the stopped identity must be untouched by the refusal"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_start_name_refuses_live_identity_elsewhere() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // vega is a live identity of another Hermes session.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('vega', 'hermes-ses-a', 'hermes', 'listening', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("hermes-ses-a", "vega").unwrap();

        // A fresh session must not claim it by name and "succeed" unbound.
        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_bare(&db, &hcom_dir, &ctx_b, Some("vega")).unwrap_err();
        assert!(
            err.to_string().contains("cannot be claimed here"),
            "unexpected error: {err}"
        );
        assert!(
            db.get_session_binding("hermes-ses-b").unwrap().is_none(),
            "no success, no binding: the fresh session stays unrecognized"
        );
        assert_eq!(
            db.get_session_binding("hermes-ses-a").unwrap().as_deref(),
            Some("vega"),
            "the live identity is untouched"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_start_name_refuses_inactive_foreign_identity() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // vega is a never-advanced identity minted by another Hermes session.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('vega', 'hermes-ses-a', 'hermes', 'inactive', 0, 0, 0)",
                [],
            )
            .unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_bare(&db, &hcom_dir, &ctx_b, Some("vega")).unwrap_err();
        assert!(
            err.to_string().contains("cannot be claimed with --name"),
            "unexpected error: {err}"
        );
        // Refused instead of "already started": reporting success here would
        // leave this session unbound while another session owns the identity.
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.status, "inactive");
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(
            hermes_instance_names(&db),
            vec!["vega".to_string()],
            "no second identity may be minted by the refusal"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_start_name_refuses_stopped_identity_and_directs_to_as() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // vega stopped with its old session's binding still in place: a bare
        // --name used to mint over the row while the old binding survived, so
        // two sessions resolved to one identity.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at,
                  last_event_id, directory)
                 VALUES ('vega', 'hermes-ses-a', 'hermes', 'stopped', 0, 0, 0, 42,
                         '/tmp/project')",
                [],
            )
            .unwrap();
        db.set_session_binding("hermes-ses-a", "vega").unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_bare(&db, &hcom_dir, &ctx_b, Some("vega")).unwrap_err();
        assert!(
            err.to_string().contains("hcom start --as vega"),
            "unexpected error: {err}"
        );
        // Failed closed: neither binding nor row moved.
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
        assert_eq!(
            db.get_session_binding("hermes-ses-a").unwrap().as_deref(),
            Some("vega")
        );
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(row.last_event_id, 42);

        // The directed path is the one that works: --as takes the stopped
        // identity over and clears the old session's binding.
        assert_eq!(start_rebind(&db, "vega", &ctx_b, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("hermes-ses-b").unwrap().as_deref(),
            Some("vega")
        );
        assert!(
            db.get_session_binding("hermes-ses-a").unwrap().is_none(),
            "--as must remove the previous session's binding, not leave it behind"
        );
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-b"));
        assert_eq!(row.last_event_id, 42, "cursor preserved through --as");
    }

    #[test]
    #[serial]
    fn test_hermes_start_name_refuses_existing_non_hermes_rows() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // A live other-tool identity: the generic already-started return used
        // to report success while leaving this Hermes session unbound.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('luna', 'claude-ses-1', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();
        // A stopped one: the mint used to overwrite the row in place, tool and
        // session id included.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('mars', 'claude-ses-2', 'claude', 'stopped', 0, 0, 0)",
                [],
            )
            .unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        for (foreign, status) in [("luna", "active"), ("mars", "stopped")] {
            let err = start_bare(&db, &hcom_dir, &ctx_b, Some(foreign)).unwrap_err();
            assert!(
                err.to_string()
                    .contains("cannot be claimed from a Hermes session"),
                "unexpected error for {foreign}: {err}"
            );
            let row = db.get_instance_full(foreign).unwrap().unwrap();
            assert_eq!(row.tool, "claude", "{foreign} must keep its tool");
            assert_eq!(row.status, status);
            assert!(
                row.session_id.is_some(),
                "{foreign} must keep its session id"
            );
        }
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
        assert!(
            hermes_instance_names(&db).is_empty(),
            "no Hermes identity may be minted over another tool's name"
        );
    }

    #[test]
    #[serial]
    fn test_hermes_start_name_remote_mirror_reaches_remote_refusal() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // A remote Hermes mirror under --name must hit the remote refusal, not
        // local --as/kill reclaim guidance that cannot apply to another
        // device's mirror.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at,
                  origin_device_id)
                 VALUES ('rhea:ABCD', 'hermes-ses-a', 'hermes', 'stopped', 0, 0, 0,
                         'remote-device')",
                [],
            )
            .unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_bare(&db, &hcom_dir, &ctx_b, Some("rhea:ABCD")).unwrap_err();
        assert!(
            err.to_string().contains("Remote start is not supported"),
            "unexpected error: {err}"
        );
        let row = db.get_instance_full("rhea:ABCD").unwrap().unwrap();
        assert_eq!(row.tool, "hermes");
        assert_eq!(row.status, "stopped");
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(row.origin_device_id.as_deref(), Some("remote-device"));
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_hermes_as_fails_closed_when_binding_lock_unavailable() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);
        let lock_path = db
            .path()
            .parent()
            .unwrap()
            .join(".tmp")
            .join("hermes_adopt.lock");
        std::fs::create_dir_all(&lock_path).unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_rebind(&db, "vega", &ctx_b, None).unwrap_err();
        assert!(
            err.to_string().contains("session-binding lock"),
            "unexpected error: {err}"
        );
        // Failed closed = failed before any mutation: the stopped identity
        // and every binding stand exactly as they were.
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-a"));
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_hermes_as_fails_closed_on_conflicting_process_binding() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);
        // This terminal's process binding belongs to a different, live session:
        // resolving the rebind sid from it would bind (or clobber) that
        // session's identity instead of HERMES_SESSION_ID's.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('luna', 'hermes-ses-x', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("hermes-ses-x", "luna").unwrap();
        db.set_process_binding("pty-1", "hermes-ses-x", "luna")
            .unwrap();

        let ctx_b = make_hermes_ctx_with_process("hermes-ses-b", "pty-1", "/tmp/project");
        let err = start_rebind(&db, "vega", &ctx_b, None).unwrap_err();
        assert!(
            err.to_string().contains("process binding"),
            "unexpected error: {err}"
        );
        // Failed closed before any mutation: the foreign binding, its row, and
        // the stopped identity stand exactly as they were.
        assert_eq!(
            db.get_session_binding("hermes-ses-x").unwrap().as_deref(),
            Some("luna")
        );
        assert!(db.get_instance_full("luna").unwrap().is_some());
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(row.last_event_id, 42);
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_hermes_as_fails_closed_on_conflicting_explicit_identity() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);
        // The claimed current identity lives in yet another session: taking it
        // at face value would rebind under that session's id — or delete a live
        // foreign identity — instead of this session's HERMES_SESSION_ID.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('old', 'hermes-ses-c', 'hermes', 'listening', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("hermes-ses-c", "old").unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_rebind(&db, "vega", &ctx_b, Some("old")).unwrap_err();
        assert!(
            err.to_string().contains("a different session"),
            "unexpected error: {err}"
        );
        // Failed closed before any mutation: 'old', its binding, and the
        // stopped target are untouched.
        let old = db.get_instance_full("old").unwrap().unwrap();
        assert_eq!(old.session_id.as_deref(), Some("hermes-ses-c"));
        assert_eq!(
            db.get_session_binding("hermes-ses-c").unwrap().as_deref(),
            Some("old")
        );
        let vega = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(vega.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(vega.last_event_id, 42);
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_hermes_as_refuses_sidless_explicit_identity() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);
        // 'old' carries no session id: nothing ties it to this Hermes session,
        // so treating it as the replaceable current identity would delete a
        // possibly foreign row on the caller's word alone.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, tool, status, status_time, last_seen, created_at)
                 VALUES ('old', 'hermes', 'listening', 0, 0, 0)",
                [],
            )
            .unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_rebind(&db, "vega", &ctx_b, Some("old")).unwrap_err();
        assert!(
            err.to_string().contains("no session id"),
            "unexpected error: {err}"
        );
        // Failed closed before any mutation: 'old' survives, and the stopped
        // target keeps its session and cursor.
        let old = db.get_instance_full("old").unwrap().unwrap();
        assert_eq!(old.session_id, None);
        assert_eq!(old.status, "listening");
        let vega = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(vega.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(vega.last_event_id, 42);
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_hermes_as_refuses_remote_mirror_target() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        // A remote mirror as the --as target: the remote delete-skip used to
        // leave the row for the recreation to rewrite in place (session_id,
        // tool, directory) instead of refusing.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at,
                  last_event_id, origin_device_id)
                 VALUES ('rhea:ABCD', 'hermes-ses-a', 'hermes', 'stopped', 0, 0, 0, 42,
                         'remote-device')",
                [],
            )
            .unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_rebind(&db, "rhea:ABCD", &ctx_b, None).unwrap_err();
        assert!(
            err.to_string().contains("remote mirror"),
            "unexpected error: {err}"
        );
        // Refused before any mutation: the mirror row is byte-for-byte what it
        // was, and this session stays unbound.
        let row = db.get_instance_full("rhea:ABCD").unwrap().unwrap();
        assert_eq!(row.tool, "hermes");
        assert_eq!(row.status, "stopped");
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(row.last_event_id, 42);
        assert_eq!(row.origin_device_id.as_deref(), Some("remote-device"));
        assert!(db.get_session_binding("hermes-ses-b").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_hermes_as_fails_closed_without_session_id() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);

        // Manual HCOM_TOOL override: Hermes by detection, but no
        // HERMES_SESSION_ID to bind a reclaim through. The strict binding
        // write used to run only after the target row was already deleted.
        let mut env: HashMap<String, String> = std::env::vars().collect();
        for key in crate::shared::tool_detection::tool_marker_vars() {
            env.remove(*key);
        }
        env.retain(|key, _| !key.starts_with("CODEX_"));
        env.remove("CLAUDE_CODE_SESSION_ID");
        env.remove("HCOM_CLAUDE_UNIX_SESSION_ID");
        env.remove("HERMES_SESSION_ID");
        env.remove("HCOM_PROCESS_ID");
        env.insert("HCOM_TOOL".to_string(), "hermes".to_string());
        let ctx = HcomContext::from_env(&env, PathBuf::from("/tmp/project"));
        assert_eq!(
            ctx.tool,
            crate::tool::Tool::Hermes,
            "override context must detect as Hermes"
        );
        assert!(
            ctx.process_id.is_none(),
            "no ambient process marker may leak in"
        );

        let err = start_rebind(&db, "vega", &ctx, None).unwrap_err();
        assert!(
            err.to_string().contains("no HERMES_SESSION_ID"),
            "unexpected error: {err}"
        );
        // Failed closed before any mutation: the stopped target is intact and
        // nothing was bound.
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(row.last_event_id, 42);
    }

    #[test]
    #[serial]
    fn test_hermes_as_reclaims_stale_binding_with_matching_process_pointer() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);
        // Session B's id carries a stale binding to a deleted row, and this
        // terminal's process binding points at the same (stale) session: the
        // pointers agree with HERMES_SESSION_ID, so the stale pair is reclaimed
        // rather than treated as a conflict.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('ghost', 'hermes-ses-b', 'hermes', 'inactive', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("hermes-ses-b", "ghost").unwrap();
        db.set_process_binding("pty-1", "hermes-ses-b", "ghost")
            .unwrap();
        db.conn()
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 DELETE FROM instances WHERE name = 'ghost';
                 PRAGMA foreign_keys=ON;",
            )
            .unwrap();

        let ctx_b = make_hermes_ctx_with_process("hermes-ses-b", "pty-1", "/tmp/project");
        assert_eq!(start_rebind(&db, "vega", &ctx_b, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("hermes-ses-b").unwrap().as_deref(),
            Some("vega"),
            "the stale binding is reclaimed under the authoritative session id"
        );
        let row = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(row.session_id.as_deref(), Some("hermes-ses-b"));
        assert_eq!(row.status, "listening");
        // The process binding follows the reclaimed identity instead of
        // pointing at the deleted row.
        assert_eq!(
            db.get_process_binding_full("pty-1").unwrap(),
            Some((Some("hermes-ses-b".to_string()), "vega".to_string()))
        );
    }

    #[test]
    #[serial]
    fn test_hermes_as_stale_binding_with_row_holder_fails_closed() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        stopped_hermes_identity(&db, "hermes-ses-a", "vega", 42, true);
        // Stale binding plus a surviving row holder: the reclaim used to skip
        // the holder check and its recreation could REPLACE the holder away.
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('ghost', 'hermes-ses-b', 'hermes', 'listening', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, tool, status, status_time, last_seen, created_at)
                 VALUES ('lost', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.set_session_binding("hermes-ses-b", "lost").unwrap();
        db.conn()
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 DELETE FROM instances WHERE name = 'lost';
                 PRAGMA foreign_keys=ON;",
            )
            .unwrap();

        let ctx_b = make_hermes_ctx(Some("hermes-ses-b"), "/tmp/project");
        let err = start_rebind(&db, "vega", &ctx_b, None).unwrap_err();
        assert!(
            err.to_string().contains("already has hcom identity"),
            "unexpected error: {err}"
        );
        // Failed closed before any mutation: the holder keeps its session and
        // the stopped target keeps its own.
        let ghost = db.get_instance_full("ghost").unwrap().unwrap();
        assert_eq!(ghost.session_id.as_deref(), Some("hermes-ses-b"));
        assert_eq!(ghost.status, "listening");
        let vega = db.get_instance_full("vega").unwrap().unwrap();
        assert_eq!(vega.session_id.as_deref(), Some("hermes-ses-a"));
        assert_eq!(vega.last_event_id, 42);
    }

    #[test]
    fn test_resolve_hermes_session_id_sources() {
        let env = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect()
        };

        assert_eq!(
            resolve_hermes_session_id(&env(&[("HERMES_SESSION_ID", "ses-1")])),
            Some("ses-1".to_string())
        );
        assert_eq!(
            resolve_hermes_session_id(&env(&[("HERMES_SESSION_ID", "")])),
            None,
            "an empty export is not identity"
        );
        assert_eq!(resolve_hermes_session_id(&env(&[])), None);
    }

    #[test]
    #[serial]
    fn test_hermes_start_records_profile_db_path() {
        let (dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        let profile_dir = dir.path().join("my_hermes_profile");
        std::fs::create_dir_all(&profile_dir).unwrap();
        let state_db = profile_dir.join("state.db");
        std::fs::write(&state_db, b"").unwrap();

        let mut env: HashMap<String, String> = std::env::vars().collect();
        for key in crate::shared::tool_detection::tool_marker_vars() {
            env.remove(*key);
        }
        env.retain(|key, _| !key.starts_with("CODEX_"));
        env.remove("CLAUDE_CODE_SESSION_ID");
        env.remove("HCOM_CLAUDE_UNIX_SESSION_ID");
        env.insert(
            "HERMES_SESSION_ID".to_string(),
            "ses-profile-test".to_string(),
        );
        env.insert(
            "HERMES_HOME".to_string(),
            profile_dir.to_str().unwrap().to_string(),
        );

        let ctx = HcomContext::from_env(&env, PathBuf::from("/tmp/project"));
        assert_eq!(ctx.tool, crate::tool::Tool::Hermes);

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let name = db.get_session_binding("ses-profile-test").unwrap().unwrap();
        let row = db.get_instance_full(&name).unwrap().unwrap();
        assert_eq!(row.tool, "hermes");
        assert_eq!(row.session_id.as_deref(), Some("ses-profile-test"));
        assert_eq!(row.transcript_path, state_db.to_str().unwrap());
    }

    #[test]
    #[serial]
    fn test_vanilla_claude_rebind_binds_session_and_drops_old_identity() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        assert!(crate::hooks::claude::setup_claude_hooks(false));

        let ctx = make_claude_ctx(
            Some(("CLAUDE_CODE_SESSION_ID", "sess-rebind")),
            "/tmp/project",
        );
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let first = db.get_session_binding("sess-rebind").unwrap().unwrap();

        assert_eq!(start_rebind(&db, "nova", &ctx, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("sess-rebind").unwrap().as_deref(),
            Some("nova"),
            "a reclaimed name must own the session that reclaimed it"
        );
        assert!(
            db.get_instance_full(&first).unwrap().is_none(),
            "the identity being replaced must not be left behind"
        );
        assert_eq!(
            db.get_validated_claude_session_owner("sess-rebind")
                .unwrap()
                .as_deref(),
            Some("nova"),
            "hooks must resolve the reclaimed name, not reject the session"
        );

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        assert_eq!(
            db.get_session_binding("sess-rebind").unwrap().as_deref(),
            Some("nova"),
            "a start after the rebind returns the reclaimed identity"
        );
    }

    #[test]
    #[serial]
    fn test_unidentifiable_claude_start_lists_unbound_candidates() {
        let (_dir, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        assert!(crate::hooks::claude::setup_claude_hooks(false));

        let cwd = std::env::current_dir().unwrap();
        let ctx = make_claude_ctx(None, cwd.to_str().unwrap());

        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let first = db
            .iter_instances_full()
            .unwrap()
            .into_iter()
            .find(|row| row.tool == "claude")
            .expect("first start creates an identity")
            .name;
        assert!(
            db.get_instance_full(&first)
                .unwrap()
                .unwrap()
                .session_id
                .is_none(),
            "a session with no id leaves the row unbound"
        );

        // Without any session id hcom still cannot recognize the session, so the
        // second start mints another identity — the warning names this one back.
        assert_eq!(start_bare(&db, &hcom_dir, &ctx, None).unwrap(), 0);
        let second = db
            .iter_instances_full()
            .unwrap()
            .into_iter()
            .find(|row| row.tool == "claude" && row.name != first)
            .expect("second start mints a second identity")
            .name;
        assert_eq!(
            unbound_claude_candidates(&db, &ctx, &second),
            vec![first],
            "the earlier unbound identity is the reclaim candidate"
        );
    }

    #[test]
    #[serial]
    fn test_root_rebind_preserves_child_hierarchy_and_actor_state() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();

        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('nova', 'sess-1', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, parent_session_id, parent_name, agent_id, tool, status,
                  status_time, last_seen, created_at)
                 VALUES ('nova_task_1', 'sess-1', 'nova', 'agent-1', 'claude',
                         'active', 0, 0, 0)",
                [],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, parent_session_id, parent_name, agent_id, tool, status,
                  status_time, last_seen, created_at)
                 VALUES ('nova_task_2', 'sess-1', 'nova_task_1', 'agent-2', 'claude',
                         'active', 0, 0, 0)",
                [],
            )
            .unwrap();

        let token = db
            .issue_claude_actor_capability("sess-1", "tool-root", None, "nova")
            .unwrap();

        let links = snapshot_child_links(&db, Some("sess-1")).unwrap();
        assert_eq!(links.len(), 2);
        db.delete_instance("nova").unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, status, status_time, last_seen, created_at)
                 VALUES ('sol', 'sess-1', 'claude', 'active', 0, 0, 0)",
                [],
            )
            .unwrap();

        restore_child_links_after_root_rebind(&db, &links, "sess-1", "nova", "sol").unwrap();
        db.rebind_claude_root_actor_state("sess-1", "nova", "sol")
            .unwrap();

        let direct = db.get_instance_full("nova_task_1").unwrap().unwrap();
        assert_eq!(direct.parent_session_id.as_deref(), Some("sess-1"));
        assert_eq!(direct.parent_name.as_deref(), Some("sol"));
        let nested = db.get_instance_full("nova_task_2").unwrap().unwrap();
        assert_eq!(nested.parent_session_id.as_deref(), Some("sess-1"));
        assert_eq!(nested.parent_name.as_deref(), Some("nova_task_1"));
        assert_eq!(
            db.resolve_claude_actor_capability(&token, "sess-1")
                .unwrap(),
            Some("sol".to_string())
        );
    }

    #[test]
    #[serial]
    fn test_same_name_root_rebind_restores_child_session_links() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, session_id, tool, directory, status, status_time, last_seen, created_at)
                 VALUES ('nova', 'sess-1', 'claude', '/tmp/project', 'active', 0, 0, 1)",
                [],
            )
            .unwrap();
        db.set_session_binding("sess-1", "nova").unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances
                 (name, parent_session_id, parent_name, agent_id, tool, status,
                  status_time, last_seen, created_at)
                 VALUES ('nova_task_1', 'sess-1', 'nova', 'agent-1', 'claude',
                         'active', 0, 0, 2)",
                [],
            )
            .unwrap();
        let token = db
            .issue_claude_actor_capability("sess-1", "tool-child", Some("agent-1"), "nova_task_1")
            .unwrap();

        let ctx = make_ctx(&[("CLAUDECODE", "1")], "/tmp/project");
        assert_eq!(start_rebind(&db, "nova", &ctx, Some("nova")).unwrap(), 0);

        let child = db.get_instance_full("nova_task_1").unwrap().unwrap();
        assert_eq!(child.parent_session_id.as_deref(), Some("sess-1"));
        assert_eq!(child.parent_name.as_deref(), Some("nova"));
        assert_eq!(
            db.resolve_claude_actor_capability(&token, "sess-1")
                .unwrap(),
            Some("nova_task_1".to_string())
        );
    }

    #[test]
    #[serial]
    fn test_start_rebind_rejects_cross_tool_stopped_snapshot_hijack() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();

        log_stopped_snapshot(
            &db,
            "fama",
            "codex",
            "/tmp/dasha-code/.worktrees/layer1-basic-conversation-fixes",
            "sid-fama",
            42,
        );

        let ctx = make_ctx(
            &[("CLAUDECODE", "1")],
            "/tmp/hcom-gan-harness/.worktrees/bench-infra",
        );

        let err = start_rebind(&db, "fama", &ctx, None).unwrap_err();
        assert!(
            err.to_string().contains("Refusing to reclaim 'fama'"),
            "unexpected error: {err}"
        );

        assert!(db.get_instance_full("fama").unwrap().is_none());
        assert_eq!(db.get_session_binding("sid-fama").unwrap(), None);
    }

    #[test]
    #[serial]
    fn test_start_rebind_allows_matching_stopped_snapshot_reclaim() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();

        log_stopped_snapshot(
            &db,
            "nova",
            "claude",
            "/tmp/dasha-code/.worktrees/layer1-basic-conversation-fixes",
            "sid-nova",
            77,
        );

        let ctx = make_ctx(
            &[("CLAUDECODE", "1")],
            "/tmp/dasha-code/.worktrees/layer1-basic-conversation-fixes",
        );

        let exit_code = start_rebind(&db, "nova", &ctx, None).unwrap();
        assert_eq!(exit_code, 0);

        let inst = db.get_instance_full("nova").unwrap().unwrap();
        assert_eq!(inst.tool, "claude");
        assert_eq!(
            inst.directory,
            "/tmp/dasha-code/.worktrees/layer1-basic-conversation-fixes"
        );
        assert_eq!(inst.last_event_id, 77);
    }

    #[test]
    #[serial]
    fn test_start_rebind_rejects_cross_directory_stopped_snapshot_hijack() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open().unwrap();

        log_stopped_snapshot(
            &db,
            "mira",
            "claude",
            "/tmp/dasha-code/.worktrees/layer1-basic-conversation-fixes",
            "sid-mira",
            18,
        );

        let ctx = make_ctx(
            &[("CLAUDECODE", "1")],
            "/tmp/hcom-gan-harness/.worktrees/bench-infra",
        );

        let err = start_rebind(&db, "mira", &ctx, None).unwrap_err();
        assert!(
            err.to_string().contains("Refusing to reclaim 'mira'"),
            "unexpected error: {err}"
        );

        assert!(db.get_instance_full("mira").unwrap().is_none());
    }

    #[test]
    #[cfg(unix)]
    fn test_same_path_resolves_symlink_aliases() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        let alias = dir.path().join("alias");
        std::fs::create_dir_all(&real).unwrap();
        std::os::unix::fs::symlink(&real, &alias).unwrap();

        assert!(same_path(
            real.to_string_lossy().as_ref(),
            alias.to_string_lossy().as_ref()
        ));
    }
}
