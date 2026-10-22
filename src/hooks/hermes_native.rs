//! Native participation and adoption helpers for ordinary Hermes sessions.
//!
//! Ordinary Hermes sessions (started outside of `hcom launch hermes`) export
//! `HERMES_SESSION_ID` into terminal subprocesses. This module provides:
//! - Cross-process serialization via `<db_dir>/.tmp/hermes_adopt.lock`.
//! - Safe recovery of soft-stopped/stopped Hermes instances without stealing live sessions.
//! - Idempotent, transactional adoption and enrollment of ordinary Hermes terminals.
//! - Read-only session resolution verifying tool ownership.

use std::collections::HashMap;
use std::fs::File;

use anyhow::{Result, anyhow};
use rusqlite::OptionalExtension;

use crate::db::{HcomDb, InstanceRow};
use crate::instance_binding;
use crate::log::{log_error, log_warn};
use crate::shared::{SenderIdentity, SenderKind};

/// Ordinary Hermes exports `HERMES_SESSION_ID` into every terminal subprocess
/// (hermes `tools/environments/local.py _inject_session_context_env`), and it
/// is the session's own durable id.
pub fn resolve_hermes_session_id(env: &HashMap<String, String>) -> Option<String> {
    env.get("HERMES_SESSION_ID")
        .filter(|value| !value.is_empty())
        .cloned()
}

/// Take the cross-process Hermes adopt / session-binding serialization lock.
/// Holders serialize on `<db dir>/.tmp/hermes_adopt.lock`; the guard
/// file keeps the lock held until it is dropped. Fails closed: an unacquirable
/// lock returns Err so the caller never proceeds unsynchronized.
pub fn lock_hermes_adopt(db: &HcomDb) -> Result<File> {
    let lock_dir = db
        .path()
        .parent()
        .ok_or_else(|| anyhow!("db path has no parent dir"))?
        .join(".tmp");
    std::fs::create_dir_all(&lock_dir).map_err(|error| {
        log_warn("hooks", "hermes.adopt.lock_dir_failed", &error.to_string());
        anyhow!(
            "cannot create Hermes lock dir at {}: {error}",
            lock_dir.display()
        )
    })?;
    let lock_path = lock_dir.join("hermes_adopt.lock");
    let lock_file = File::options()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|error| {
            log_warn("hooks", "hermes.adopt.lock_open_failed", &error.to_string());
            anyhow!(
                "cannot open Hermes adopt lock at {}: {error}",
                lock_path.display()
            )
        })?;
    crate::sys::fs::lock_exclusive(&lock_file).map_err(|error| {
        log_warn("hooks", "hermes.adopt.lock_failed", &error.to_string());
        anyhow!(
            "cannot acquire Hermes adopt lock at {}: {error}",
            lock_path.display()
        )
    })?;
    Ok(lock_file)
}

/// Compatibility aliases for start.rs and hooks coordination.
pub use lock_hermes_adopt as lock_hermes_session_binding;

/// The instance row currently holding this session id, if any. A
/// soft-finalized Hermes session keeps its row and session_id after
/// `soft_finalize_session` drops only the binding, so a start that finds no
/// binding must look here before minting: a fresh row with the same session_id
/// would hit `UNIQUE(session_id)` and `save_instance_named`'s INSERT OR
/// REPLACE would delete the retained row. Read errors propagate — swallowing
/// one into "no row" would let the caller mint over a row it failed to see.
pub fn hermes_row_holding_session_id(db: &HcomDb, session_id: &str) -> Result<Option<InstanceRow>> {
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
pub fn recover_stopped_hermes_row(db: &HcomDb, session_id: &str) -> Result<Option<String>> {
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

/// Ordinary Hermes sessions have no hcom launch placeholder.
/// This helper assumes the caller ALREADY holds `lock_hermes_adopt` (or equivalent
/// cross-process serialization), avoiding double-locking on separate file descriptors.
/// It verifies existing bindings, checks stopped-row recovery, ensures no retained row
/// will be clobbered by INSERT OR REPLACE, and transactionally mints and initializes
/// a fresh instance.
pub fn adopt_ordinary_hermes_instance_locked(
    db: &HcomDb,
    session_id: &str,
    transcript_path: Option<&str>,
    cwd: Option<&str>,
) -> Result<String> {
    if session_id.is_empty() {
        return Err(anyhow!("Missing session_id for Hermes adoption"));
    }

    // Guard 1: Verify existing session binding with fail-closed DB reads.
    if let Some(name) = db.get_session_binding(session_id)? {
        let bound_row = db.get_instance_full(&name)?;
        match bound_row {
            Some(row) if row.tool == "hermes" => {
                if row.session_id.as_deref() == Some(session_id) {
                    return Ok(name);
                }
                return Err(anyhow!(
                    "Hermes session {session_id} is bound to '{name}', which is attributed to session {:?}",
                    row.session_id
                ));
            }
            Some(row) => {
                return Err(anyhow!(
                    "Hermes session {session_id} is bound to '{name}', which belongs to tool '{}'",
                    row.tool
                ));
            }
            None => {} // Stale binding, fall through to recovery
        }
    }

    // Guard 2: Safely recover stopped hermes row if present.
    if let Some(name) = recover_stopped_hermes_row(db, session_id)? {
        return Ok(name);
    }

    // Guard 3: Prevent INSERT OR REPLACE from clobbering any existing row holding this session_id.
    if let Some(row) = hermes_row_holding_session_id(db, session_id)? {
        if row.tool == "hermes" {
            return Err(anyhow!(
                "Hermes session {session_id} already held by instance '{}'",
                row.name
            ));
        }
        return Err(anyhow!(
            "Session {session_id} is already held by non-Hermes instance '{}'",
            row.name
        ));
    }

    // Mint unique name and create row transactionally.
    let name = crate::instance_names::generate_unique_name(db)?;
    db.conn().execute_batch("BEGIN IMMEDIATE")?;
    let created = (|| -> Result<()> {
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
        return Err(error);
    }
    crate::hooks::common::notify_hook_instance_with_db(db, &name);
    Ok(name)
}

/// Resolve an ordinary Hermes terminal command's identity from the
/// `HERMES_SESSION_ID` its host exports into every terminal subprocess.
///
/// Strictly a lookup: the session id must already be bound to an existing,
/// hermes-owned instance row. Stale or unknown ids, deleted rows, rows owned
/// by other tools, and DB errors all return `None`.
/// This never adopts, binds, or creates anything.
pub fn resolve_hermes_session_identity(db: &HcomDb, session_id: &str) -> Option<SenderIdentity> {
    if session_id.is_empty() {
        return None;
    }
    let name = db.get_session_binding(session_id).ok().flatten()?;
    let instance_data = db.get_instance(&name).ok().flatten()?;
    if instance_data.get("tool").and_then(|v| v.as_str()) != Some("hermes") {
        log_warn(
            "identity",
            "resolve.hermes_session_foreign_row",
            &format!(
                "session_id={}, name={}",
                session_id.chars().take(8).collect::<String>(),
                name
            ),
        );
        return None;
    }
    Some(SenderIdentity {
        kind: SenderKind::Instance,
        name,
        session_id: Some(session_id.to_string()),
        instance_data: Some(instance_data),
    })
}

/// Read-only session resolve mode for hermes-read without `--name`.
/// Returns the instance name if the session is enrolled and owned by Hermes.
pub fn resolve_enrolled_hermes_session(db: &HcomDb, session_id: &str) -> Option<String> {
    if session_id.is_empty() {
        return None;
    }
    db.get_session_binding(session_id)
        .ok()
        .flatten()
        .filter(|name| {
            db.get_instance_full(name)
                .ok()
                .flatten()
                .is_some_and(|row| {
                    row.tool == "hermes" && row.session_id.as_deref() == Some(session_id)
                })
        })
}
