//! Track hcom-launched process PIDs for orphan detection and recovery.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::instance_lifecycle as lifecycle;

const PIDFILE_NAME: &str = ".tmp/launched_pids.json";

/// Tracked process entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PidEntry {
    pub tool: String,
    pub names: Vec<String>,
    pub launched_at: f64,
    #[serde(default)]
    pub directory: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub process_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub terminal_preset: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pane_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub terminal_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kitty_listen_on: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub zellij_session_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub notify_port: u16,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub inject_port: u16,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tag: String,
    /// Process incarnation observed when this PID was recorded.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pid_identity: String,
}

fn is_zero(v: &u16) -> bool {
    *v == 0
}

/// Orphan process info (enriched with PID).
#[derive(Debug, Clone)]
pub struct OrphanProcess {
    pub pid: u32,
    pub tool: String,
    pub names: Vec<String>,
    pub directory: String,
    pub process_id: String,
    pub terminal_preset: String,
    pub pane_id: String,
    pub terminal_id: String,
    pub kitty_listen_on: String,
    pub zellij_session_name: String,
    pub session_id: String,
    pub notify_port: u16,
    pub inject_port: u16,
    pub tag: String,
    /// The observed process incarnation, carried unchanged into DB recovery.
    pub pid_identity: String,
}

impl From<(u32, &PidEntry)> for OrphanProcess {
    fn from((pid, entry): (u32, &PidEntry)) -> Self {
        Self {
            pid,
            tool: entry.tool.clone(),
            names: entry.names.clone(),
            directory: entry.directory.clone(),
            process_id: entry.process_id.clone(),
            terminal_preset: entry.terminal_preset.clone(),
            pane_id: entry.pane_id.clone(),
            terminal_id: entry.terminal_id.clone(),
            kitty_listen_on: entry.kitty_listen_on.clone(),
            zellij_session_name: entry.zellij_session_name.clone(),
            session_id: entry.session_id.clone(),
            notify_port: entry.notify_port,
            inject_port: entry.inject_port,
            tag: entry.tag.clone(),
            pid_identity: entry.pid_identity.clone(),
        }
    }
}

/// Resolve the pidfile path from hcom_dir.
fn pidfile_path(hcom_dir: &Path) -> PathBuf {
    hcom_dir.join(PIDFILE_NAME)
}

/// Check if a process is alive. See [`crate::sys::process::is_alive`].
pub fn is_alive(pid: u32) -> bool {
    crate::sys::process::is_alive(pid)
}

/// Read raw pidfile data.
fn read_raw(hcom_dir: &Path) -> HashMap<String, PidEntry> {
    match std::fs::read_to_string(pidfile_path(hcom_dir)) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

/// Write pidfile data atomically (temp + rename).
fn write_raw(hcom_dir: &Path, data: &HashMap<String, PidEntry>) {
    if let Ok(content) = serde_json::to_string(data) {
        crate::paths::atomic_write(&pidfile_path(hcom_dir), &content);
    }
}

/// Parameters for recording a launched process.
#[derive(Debug)]
pub struct PidRecord<'a> {
    pub hcom_dir: &'a Path,
    pub pid: u32,
    pub tool: &'a str,
    pub name: &'a str,
    pub directory: &'a str,
    pub process_id: &'a str,
    pub terminal_preset: &'a str,
    pub pane_id: &'a str,
    pub terminal_id: &'a str,
    pub kitty_listen_on: &'a str,
    pub zellij_session_name: &'a str,
    pub session_id: &'a str,
    pub notify_port: u16,
    pub inject_port: u16,
    pub tag: &'a str,
    pub pid_identity: &'a str,
}

impl<'a> PidRecord<'a> {
    /// Create with required fields, defaulting optional ones.
    #[cfg(test)]
    pub fn new(
        hcom_dir: &'a Path,
        pid: u32,
        tool: &'a str,
        name: &'a str,
        directory: &'a str,
    ) -> Self {
        Self {
            hcom_dir,
            pid,
            tool,
            name,
            directory,
            process_id: "",
            terminal_preset: "",
            pane_id: "",
            terminal_id: "",
            kitty_listen_on: "",
            zellij_session_name: "",
            session_id: "",
            notify_port: 0,
            inject_port: 0,
            tag: "",
            pid_identity: "",
        }
    }
}

/// Record a launched process PID.
pub fn record_pid(rec: &PidRecord<'_>) {
    let PidRecord {
        hcom_dir,
        pid,
        tool,
        name,
        directory,
        process_id,
        terminal_preset,
        pane_id,
        terminal_id,
        kitty_listen_on,
        zellij_session_name,
        session_id,
        notify_port,
        inject_port,
        tag,
        pid_identity,
    } = rec;
    let mut data = read_raw(hcom_dir);
    let key = pid.to_string();

    // A numeric PID can be reused. Once both observations carry an
    // incarnation, discard the old record rather than appending its names or
    // metadata to the unrelated process now holding the same PID.
    let replaced_incarnation = data.get(&key).is_some_and(|entry| {
        !pid_identity.is_empty()
            && !entry.pid_identity.is_empty()
            && entry.pid_identity != *pid_identity
    });
    if replaced_incarnation {
        data.remove(&key);
    }

    if let Some(entry) = data.get_mut(&key) {
        // Append name if not already present
        if !entry.names.contains(&name.to_string()) {
            entry.names.push(name.to_string());
        }
        // Fill in fields that are empty
        if !process_id.is_empty() && entry.process_id.is_empty() {
            entry.process_id = process_id.to_string();
        }
        if !terminal_preset.is_empty() && entry.terminal_preset.is_empty() {
            entry.terminal_preset = terminal_preset.to_string();
        }
        if !pane_id.is_empty() && entry.pane_id.is_empty() {
            entry.pane_id = pane_id.to_string();
        }
        if !terminal_id.is_empty() && entry.terminal_id.is_empty() {
            entry.terminal_id = terminal_id.to_string();
        }
        if !kitty_listen_on.is_empty() && entry.kitty_listen_on.is_empty() {
            entry.kitty_listen_on = kitty_listen_on.to_string();
        }
        if !zellij_session_name.is_empty() && entry.zellij_session_name.is_empty() {
            entry.zellij_session_name = zellij_session_name.to_string();
        }
        if !session_id.is_empty() && entry.session_id.is_empty() {
            entry.session_id = session_id.to_string();
        }
        if *notify_port != 0 && entry.notify_port == 0 {
            entry.notify_port = *notify_port;
        }
        if *inject_port != 0 && entry.inject_port == 0 {
            entry.inject_port = *inject_port;
        }
        if !tag.is_empty() && entry.tag.is_empty() {
            entry.tag = tag.to_string();
        }
        if !pid_identity.is_empty() && entry.pid_identity.is_empty() {
            entry.pid_identity = pid_identity.to_string();
        }
    } else {
        data.insert(
            key,
            PidEntry {
                tool: tool.to_string(),
                names: vec![name.to_string()],
                launched_at: crate::shared::time::now_epoch_f64(),
                directory: directory.to_string(),
                process_id: process_id.to_string(),
                terminal_preset: terminal_preset.to_string(),
                pane_id: pane_id.to_string(),
                terminal_id: terminal_id.to_string(),
                kitty_listen_on: kitty_listen_on.to_string(),
                zellij_session_name: zellij_session_name.to_string(),
                session_id: session_id.to_string(),
                notify_port: *notify_port,
                inject_port: *inject_port,
                tag: tag.to_string(),
                pid_identity: pid_identity.to_string(),
            },
        );
    }

    write_raw(hcom_dir, &data);
}

/// Get running hcom processes not accounted for by active instances.
///
/// Auto-prunes dead PIDs from the file. If `active_pids` is provided,
/// also prunes PIDs that are now active from the file and filters them
/// from the result.
pub fn get_orphan_processes(
    hcom_dir: &Path,
    active_pids: Option<&std::collections::HashSet<u32>>,
) -> Vec<OrphanProcess> {
    get_orphan_processes_with_identity_probe(hcom_dir, active_pids, &crate::sys::process::identity)
}

/// Read tracked processes with an injectable identity probe.
///
/// A live process whose stored identity cannot currently be observed remains in
/// the pidfile for a later retry, but is excluded from recovery candidates. A
/// definite identity mismatch is stale ownership and is pruned immediately.
fn get_orphan_processes_with_identity_probe(
    hcom_dir: &Path,
    active_pids: Option<&std::collections::HashSet<u32>>,
    identity_probe: &dyn Fn(u32) -> Option<String>,
) -> Vec<OrphanProcess> {
    let data = read_raw(hcom_dir);

    // Filter to alive processes only. Keep a PID with a stored identity when
    // the platform probe is temporarily unavailable, but do not make it an
    // eligible recovery candidate until the identity can be verified.
    let mut retained: HashMap<String, PidEntry> = HashMap::new();
    let mut eligible: Vec<OrphanProcess> = Vec::new();
    for (pid_str, entry) in &data {
        if let Ok(pid) = pid_str.parse::<u32>()
            && pid > 0
            && i32::try_from(pid).is_ok()
            && is_alive(pid)
        {
            if !entry.pid_identity.is_empty() {
                match identity_probe(pid) {
                    Some(observed) if observed == entry.pid_identity => {}
                    Some(_) => continue,
                    None => {
                        retained.insert(pid_str.clone(), entry.clone());
                        continue;
                    }
                }
            }
            retained.insert(pid_str.clone(), entry.clone());
            eligible.push(OrphanProcess::from((pid, entry)));
        }
    }

    // Write back pruned data if anything was removed
    if retained.len() != data.len() {
        write_raw(hcom_dir, &retained);
    }

    let mut result = eligible;

    // Prune active PIDs from file and filter from result
    if let Some(active) = active_pids {
        let active_in_file: Vec<String> = result
            .iter()
            .filter(|p| active.contains(&p.pid))
            .map(|p| p.pid.to_string())
            .collect();
        if !active_in_file.is_empty() {
            let mut pruned = retained;
            for k in &active_in_file {
                pruned.remove(k);
            }
            write_raw(hcom_dir, &pruned);
        }
        result.retain(|p| !active.contains(&p.pid));
    }

    result
}

/// Remove a PID from tracking (after kill).
pub fn remove_pid(hcom_dir: &Path, pid: u32) {
    let mut data = read_raw(hcom_dir);
    let key = pid.to_string();
    if data.remove(&key).is_some() {
        write_raw(hcom_dir, &data);
    }
}

/// Re-register a single orphan into the DB.
///
/// Creates instance row, sets PID/directory, creates process/session bindings,
/// and sets status to listening so the PTY delivery gate can inject messages.
/// Does NOT log events, print output, or remove from pidtrack — caller handles those.
///
/// Returns `Err` if the critical instance INSERT fails. Caller must leave the
/// pidtrack entry intact on failure so recovery can be retried later.
pub fn recover_single_orphan_to_db(
    db: &crate::db::HcomDb,
    orphan: &OrphanProcess,
    instance_name: &str,
) -> Result<(), String> {
    use crate::shared::constants::ST_LISTENING;

    // A pidfile entry without an incarnation is legacy/unverified ownership:
    // probe it only to reject invalid or already-dead PIDs, and do not promote
    // the current occupant into a newly trusted identity. A supplied identity
    // came from the original launch observation and is carried unchanged.
    if orphan.pid == 0 || i32::try_from(orphan.pid).is_err() {
        return Err(format!("invalid orphan pid {}", orphan.pid));
    }
    if orphan.pid_identity.is_empty() {
        // A live numeric PID is not evidence that it is still the process
        // which wrote a legacy pidfile: the PID may already have been reused.
        // Without the original incarnation token, fail closed after probing
        // liveness rather than registering an unowned process.
        match crate::db::observe_pid_identity(orphan.pid) {
            Ok(Some(_)) => {
                return Err(format!(
                    "cannot recover legacy orphan pid {} without its recorded identity",
                    orphan.pid
                ));
            }
            Ok(None) => {
                return Err(format!(
                    "cannot recover legacy orphan pid {} without verifiable identity",
                    orphan.pid
                ));
            }
            Err(e) => {
                return Err(format!("cannot validate orphan pid {}: {e}", orphan.pid));
            }
        }
    }

    let now = crate::shared::time::now_epoch_i64();

    db.conn()
        .execute_batch("SAVEPOINT hcom_recover_orphan")
        .map_err(|e| format!("failed to begin orphan recovery: {e}"))?;
    let recovery = (|| -> Result<(), String> {
        // Create instance row — this is the critical step; fail = abort recovery
        db.conn()
            .execute(
                "INSERT OR IGNORE INTO instances (name, tool, status, status_context, created_at) VALUES (?1, ?2, 'inactive', 'new', ?3)",
                rusqlite::params![instance_name, orphan.tool, now],
            )
            .map_err(|e| format!("failed to insert instance '{}': {}", instance_name, e))?;

        // Update PID and directory
        let mut updates = serde_json::Map::new();
        if !orphan.directory.is_empty() {
            updates.insert("directory".into(), serde_json::json!(orphan.directory));
        }
        if !orphan.terminal_preset.is_empty() {
            updates.insert(
                "terminal_preset_effective".into(),
                serde_json::json!(orphan.terminal_preset),
            );
        }
        let mut launch_context = serde_json::Map::new();
        if !orphan.process_id.is_empty() {
            launch_context.insert("process_id".into(), serde_json::json!(orphan.process_id));
        }
        if !orphan.pane_id.is_empty() {
            launch_context.insert("pane_id".into(), serde_json::json!(orphan.pane_id));
        }
        if !orphan.terminal_id.is_empty() {
            launch_context.insert("terminal_id".into(), serde_json::json!(orphan.terminal_id));
        }
        if !orphan.kitty_listen_on.is_empty() {
            launch_context.insert(
                "kitty_listen_on".into(),
                serde_json::json!(orphan.kitty_listen_on),
            );
        }
        if !orphan.zellij_session_name.is_empty() {
            launch_context.insert(
                "env".into(),
                serde_json::json!({ "ZELLIJ_SESSION_NAME": orphan.zellij_session_name }),
            );
        }
        if !orphan.pid_identity.is_empty() {
            launch_context.insert(
                "pid_identity".into(),
                serde_json::json!(orphan.pid_identity),
            );
        }
        if !launch_context.is_empty() {
            updates.insert(
                "launch_context".into(),
                serde_json::json!(
                    serde_json::to_string(&launch_context).unwrap_or_else(|_| "{}".to_string())
                ),
            );
        }

        db.update_instance_pid_with_identity_and_fields(
            instance_name,
            orphan.pid,
            (!orphan.pid_identity.is_empty()).then_some(orphan.pid_identity.as_str()),
            &updates,
        )
        .map_err(|e| format!("failed to set runtime state for orphan: {e}"))?;

        // Create process binding
        if !orphan.process_id.is_empty() {
            let sid = if orphan.session_id.is_empty() {
                None
            } else {
                Some(orphan.session_id.as_str())
            };
            db.set_process_binding(&orphan.process_id, sid.unwrap_or(""), instance_name)
                .map_err(|e| format!("failed to set process binding: {}", e))?;
        }

        // Create session binding
        if !orphan.session_id.is_empty() {
            db.rebind_session(&orphan.session_id, instance_name)
                .map_err(|e| format!("failed to rebind session: {}", e))?;
            let mut sid_update = serde_json::Map::new();
            sid_update.insert("session_id".into(), serde_json::json!(orphan.session_id));
            db.update_instance_fields(instance_name, &sid_update)
                .map_err(|e| format!("failed to set orphan session id: {e}"))?;
        }

        // Restore notify endpoints
        if orphan.notify_port != 0 {
            db.register_notify_port(instance_name, orphan.notify_port)
                .map_err(|e| format!("failed to register notify port: {}", e))?;
        }
        if orphan.inject_port != 0 {
            db.register_inject_port(instance_name, orphan.inject_port)
                .map_err(|e| format!("failed to register inject port: {}", e))?;
        }

        // Set listening so PTY delivery gate allows message injection
        lifecycle::set_status_checked(
            db,
            instance_name,
            ST_LISTENING,
            "recovered",
            Default::default(),
        )
        .map_err(|e| format!("failed to set recovered status: {e}"))?;

        Ok(())
    })();

    if let Err(error) = recovery {
        let _ = db.conn().execute_batch("ROLLBACK TO hcom_recover_orphan");
        let _ = db.conn().execute_batch("RELEASE hcom_recover_orphan");
        return Err(error);
    }
    db.conn()
        .execute_batch("RELEASE hcom_recover_orphan")
        .map_err(|e| format!("failed to commit orphan recovery: {e}"))?;

    // Publish the wake only after the row, bindings, endpoints, and status
    // event are durable. Unlike the old best-effort status helper, a failed
    // status transition cannot leave a wake pointing at rolled-back state.
    crate::notify::wake(db, instance_name, crate::notify::WakeKind::DELIVERY_LOOPS);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn make_temp_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".tmp")).unwrap();
        dir
    }

    fn rec<'a>(dir: &'a Path, pid: u32, tool: &'a str, name: &'a str) -> PidRecord<'a> {
        PidRecord::new(dir, pid, tool, name, "/tmp")
    }

    #[test]
    fn test_record_and_read() {
        let dir = make_temp_dir();
        record_pid(&PidRecord {
            hcom_dir: dir.path(),
            pid: 12345,
            tool: "claude",
            name: "luna",
            directory: "/tmp",
            process_id: "pid-1",
            terminal_preset: "kitty",
            pane_id: "pane-1",
            terminal_id: "term-1",
            kitty_listen_on: "/tmp/kitty.sock",
            zellij_session_name: "wise-kangaroo",
            session_id: "sess-1",
            notify_port: 8080,
            inject_port: 8081,
            tag: "test-tag",
            pid_identity: "test:12345:1",
        });

        let data = read_raw(dir.path());
        assert_eq!(data.len(), 1);
        let entry = data.get("12345").unwrap();
        assert_eq!(entry.tool, "claude");
        assert_eq!(entry.names, vec!["luna"]);
        assert_eq!(entry.process_id, "pid-1");
        assert_eq!(entry.terminal_preset, "kitty");
        assert_eq!(entry.pane_id, "pane-1");
        assert_eq!(entry.terminal_id, "term-1");
        assert_eq!(entry.kitty_listen_on, "/tmp/kitty.sock");
        assert_eq!(entry.zellij_session_name, "wise-kangaroo");
        assert_eq!(entry.session_id, "sess-1");
        assert_eq!(entry.notify_port, 8080);
        assert_eq!(entry.inject_port, 8081);
        assert_eq!(entry.pid_identity, "test:12345:1");
    }

    #[test]
    fn test_record_appends_name() {
        let dir = make_temp_dir();
        record_pid(&rec(dir.path(), 12345, "claude", "luna"));
        record_pid(&rec(dir.path(), 12345, "claude", "nova"));

        let data = read_raw(dir.path());
        let entry = data.get("12345").unwrap();
        assert_eq!(entry.names, vec!["luna", "nova"]);
    }

    #[test]
    fn test_record_fills_empty_fields() {
        let dir = make_temp_dir();
        record_pid(&rec(dir.path(), 12345, "claude", "luna"));
        record_pid(&PidRecord {
            process_id: "pid-1",
            terminal_preset: "kitty",
            zellij_session_name: "wise-kangaroo",
            notify_port: 8080,
            ..rec(dir.path(), 12345, "claude", "luna")
        });

        let data = read_raw(dir.path());
        let entry = data.get("12345").unwrap();
        assert_eq!(entry.process_id, "pid-1");
        assert_eq!(entry.terminal_preset, "kitty");
        assert_eq!(entry.zellij_session_name, "wise-kangaroo");
        assert_eq!(entry.notify_port, 8080);
    }

    #[test]
    fn test_record_does_not_overwrite_existing_fields() {
        let dir = make_temp_dir();
        record_pid(&PidRecord {
            process_id: "pid-1",
            terminal_preset: "kitty",
            ..rec(dir.path(), 12345, "claude", "luna")
        });
        record_pid(&PidRecord {
            process_id: "pid-2",
            terminal_preset: "wezterm",
            ..rec(dir.path(), 12345, "claude", "luna")
        });

        let data = read_raw(dir.path());
        let entry = data.get("12345").unwrap();
        assert_eq!(entry.process_id, "pid-1");
        assert_eq!(entry.terminal_preset, "kitty");
    }

    #[test]
    fn test_record_replaces_reused_pid_incarnation() {
        let dir = make_temp_dir();
        record_pid(&PidRecord {
            pid_identity: "boot:old:1",
            ..rec(dir.path(), 12345, "claude", "old")
        });
        record_pid(&PidRecord {
            pid_identity: "boot:new:2",
            ..rec(dir.path(), 12345, "gemini", "new")
        });

        let entry = read_raw(dir.path()).remove("12345").unwrap();
        assert_eq!(entry.tool, "gemini");
        assert_eq!(entry.names, vec!["new"]);
        assert_eq!(entry.pid_identity, "boot:new:2");
    }

    #[test]
    fn test_remove_pid() {
        let dir = make_temp_dir();
        record_pid(&rec(dir.path(), 12345, "claude", "luna"));
        record_pid(&rec(dir.path(), 67890, "gemini", "nova"));

        remove_pid(dir.path(), 12345);
        let data = read_raw(dir.path());
        assert_eq!(data.len(), 1);
        assert!(data.contains_key("67890"));
        assert!(!data.contains_key("12345"));
    }

    #[test]
    fn test_remove_nonexistent_pid() {
        let dir = make_temp_dir();
        record_pid(&rec(dir.path(), 12345, "claude", "luna"));
        remove_pid(dir.path(), 99999);
        let data = read_raw(dir.path());
        assert_eq!(data.len(), 1);
    }

    #[test]
    fn test_orphan_prunes_dead_pids() {
        let dir = make_temp_dir();
        record_pid(&rec(dir.path(), 99999999, "claude", "dead"));

        let orphans = get_orphan_processes(dir.path(), None);
        let data = read_raw(dir.path());
        assert!(!data.contains_key("99999999"));
        assert!(orphans.iter().all(|o| o.pid != 99999999));
    }

    #[test]
    fn test_orphan_active_pids_pruned() {
        let dir = make_temp_dir();
        let our_pid = std::process::id();
        record_pid(&rec(dir.path(), our_pid, "claude", "luna"));

        let mut active = HashSet::new();
        active.insert(our_pid);

        let orphans = get_orphan_processes(dir.path(), Some(&active));
        // Our PID is active — should be filtered from results AND pruned from file
        assert!(orphans.is_empty());
        let data = read_raw(dir.path());
        assert!(!data.contains_key(&our_pid.to_string()));
    }

    #[test]
    fn test_orphan_rejects_reused_pid_identity() {
        let dir = make_temp_dir();
        let pid = std::process::id();
        record_pid(&PidRecord {
            pid_identity: "boot:unrelated:99",
            ..rec(dir.path(), pid, "claude", "foreign")
        });

        let orphans = get_orphan_processes(dir.path(), None);
        assert!(orphans.is_empty());
        assert!(!read_raw(dir.path()).contains_key(&pid.to_string()));
    }

    #[test]
    fn test_orphan_identity_probe_unavailable_retains_until_retry() {
        let dir = make_temp_dir();
        let pid = std::process::id();
        record_pid(&PidRecord {
            pid_identity: "historical:current",
            ..rec(dir.path(), pid, "claude", "retry")
        });

        let unavailable = get_orphan_processes_with_identity_probe(dir.path(), None, &|_| None);
        assert!(unavailable.is_empty());
        assert!(
            read_raw(dir.path()).contains_key(&pid.to_string()),
            "an unobservable live PID must remain for a later identity retry"
        );

        let eligible = get_orphan_processes_with_identity_probe(dir.path(), None, &|_| {
            Some("historical:current".to_string())
        });
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].pid, pid);
        assert_eq!(eligible[0].pid_identity, "historical:current");
    }

    #[test]
    fn test_empty_pidfile() {
        let dir = make_temp_dir();
        let orphans = get_orphan_processes(dir.path(), None);
        assert!(orphans.is_empty());
    }

    #[test]
    fn test_is_alive_current_process() {
        assert!(is_alive(std::process::id()));
    }

    #[test]
    fn test_is_alive_dead_process() {
        assert!(!is_alive(99999999));
    }

    #[test]
    fn test_recover_single_orphan_returns_error_on_db_failure() {
        // DB without init_db → no instances table → INSERT fails
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = crate::db::HcomDb::open_raw(&db_path).unwrap();
        // Deliberately NOT calling db.init_db()

        let orphan = OrphanProcess {
            pid: std::process::id(),
            tool: "claude".into(),
            names: vec!["luna".into()],
            directory: "/tmp".into(),
            process_id: "pid-1".into(),
            terminal_preset: String::new(),
            pane_id: String::new(),
            terminal_id: String::new(),
            kitty_listen_on: String::new(),
            zellij_session_name: String::new(),
            session_id: String::new(),
            notify_port: 0,
            inject_port: 0,
            tag: String::new(),
            pid_identity: String::new(),
        };

        let result = recover_single_orphan_to_db(&db, &orphan, "luna");
        assert!(
            result.is_err(),
            "expected error when DB has no instances table"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_recover_single_orphan_rolls_back_partial_registration() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = crate::db::HcomDb::open().unwrap();
        let orphan = OrphanProcess {
            pid: u32::MAX,
            tool: "claude".into(),
            names: vec!["luna".into()],
            directory: "/tmp".into(),
            process_id: "proc-retry".into(),
            terminal_preset: String::new(),
            pane_id: String::new(),
            terminal_id: String::new(),
            kitty_listen_on: String::new(),
            zellij_session_name: String::new(),
            session_id: "sess-retry".into(),
            notify_port: 0,
            inject_port: 0,
            tag: String::new(),
            pid_identity: String::new(),
        };

        let result = recover_single_orphan_to_db(&db, &orphan, "luna");
        assert!(result.is_err(), "unobservable PID must fail recovery");
        assert!(
            db.get_instance_full("luna").unwrap().is_none(),
            "failed recovery must not strand an inactive row"
        );
        assert_eq!(
            db.get_process_binding("proc-retry").unwrap(),
            None,
            "failed recovery must roll back process ownership"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_recover_legacy_orphan_requires_original_identity() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = crate::db::HcomDb::open().unwrap();
        let orphan = OrphanProcess {
            pid: std::process::id(),
            tool: "claude".into(),
            names: vec!["luna".into()],
            directory: "/tmp".into(),
            process_id: "proc-legacy".into(),
            terminal_preset: String::new(),
            pane_id: String::new(),
            terminal_id: String::new(),
            kitty_listen_on: String::new(),
            zellij_session_name: String::new(),
            session_id: "sess-legacy".into(),
            notify_port: 12_345,
            inject_port: 12_346,
            tag: String::new(),
            pid_identity: String::new(),
        };

        let result = recover_single_orphan_to_db(&db, &orphan, "luna");
        assert!(
            result.is_err(),
            "legacy PID without an incarnation is unproven"
        );
        assert!(db.get_instance_full("luna").unwrap().is_none());
        assert_eq!(db.get_process_binding("proc-legacy").unwrap(), None);
        assert_eq!(
            db.conn()
                .query_row(
                    "SELECT COUNT(*) FROM notify_endpoints WHERE instance = 'luna'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_recover_orphan_carries_pid_identity_without_reobserving() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = crate::db::HcomDb::open().unwrap();
        let historical_identity = "linux:historical-boot:12345";
        let orphan = OrphanProcess {
            pid: std::process::id(),
            tool: "claude".into(),
            names: vec!["luna".into()],
            directory: "/tmp".into(),
            process_id: String::new(),
            terminal_preset: String::new(),
            pane_id: String::new(),
            terminal_id: String::new(),
            kitty_listen_on: String::new(),
            zellij_session_name: String::new(),
            session_id: String::new(),
            notify_port: 0,
            inject_port: 0,
            tag: String::new(),
            pid_identity: historical_identity.into(),
        };

        recover_single_orphan_to_db(&db, &orphan, "luna").unwrap();
        let row = db.get_instance_full("luna").unwrap().unwrap();
        assert_eq!(row.pid, Some(std::process::id() as i64));
        assert_eq!(row.pid_identity().as_deref(), Some(historical_identity));
        let context: serde_json::Value =
            serde_json::from_str(row.launch_context.as_deref().unwrap()).unwrap();
        assert_eq!(context["pid_identity"], historical_identity);
    }

    #[test]
    #[serial_test::serial]
    fn test_recover_orphan_status_failure_rolls_back_all_registration_state() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = crate::db::HcomDb::open().unwrap();
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_recovery_status
                 BEFORE UPDATE ON instances
                 WHEN NEW.name = 'luna' AND NEW.status = 'listening'
                 BEGIN SELECT RAISE(ABORT, 'injected status failure'); END;",
            )
            .unwrap();
        let orphan = OrphanProcess {
            pid: std::process::id(),
            tool: "claude".into(),
            names: vec!["luna".into()],
            directory: "/tmp".into(),
            process_id: "proc-status-fail".into(),
            terminal_preset: String::new(),
            pane_id: String::new(),
            terminal_id: String::new(),
            kitty_listen_on: String::new(),
            zellij_session_name: String::new(),
            session_id: "sess-status-fail".into(),
            notify_port: 12_345,
            inject_port: 12_346,
            tag: String::new(),
            pid_identity: "historical:status-fail".into(),
        };

        let result = recover_single_orphan_to_db(&db, &orphan, "luna");
        assert!(result.is_err());
        assert_recovery_state_is_empty(&db);
    }

    #[test]
    #[serial_test::serial]
    fn test_recover_orphan_event_failure_rolls_back_all_registration_state() {
        let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = crate::db::HcomDb::open().unwrap();
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_recovery_event
                 BEFORE INSERT ON events
                 WHEN NEW.instance = 'luna' AND NEW.type = 'status'
                 BEGIN SELECT RAISE(ABORT, 'injected event failure'); END;",
            )
            .unwrap();
        let orphan = OrphanProcess {
            pid: std::process::id(),
            tool: "claude".into(),
            names: vec!["luna".into()],
            directory: "/tmp".into(),
            process_id: "proc-event-fail".into(),
            terminal_preset: String::new(),
            pane_id: String::new(),
            terminal_id: String::new(),
            kitty_listen_on: String::new(),
            zellij_session_name: String::new(),
            session_id: "sess-event-fail".into(),
            notify_port: 12_345,
            inject_port: 12_346,
            tag: String::new(),
            pid_identity: "historical:event-fail".into(),
        };

        let result = recover_single_orphan_to_db(&db, &orphan, "luna");
        assert!(result.is_err());
        assert_recovery_state_is_empty(&db);
    }

    fn assert_recovery_state_is_empty(db: &crate::db::HcomDb) {
        assert!(db.get_instance_full("luna").unwrap().is_none());
        assert_eq!(db.get_process_binding("proc-status-fail").unwrap(), None);
        assert_eq!(db.get_process_binding("proc-event-fail").unwrap(), None);
        assert_eq!(db.get_session_binding("sess-status-fail").unwrap(), None);
        assert_eq!(db.get_session_binding("sess-event-fail").unwrap(), None);
        let endpoints: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM notify_endpoints WHERE instance = 'luna'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(endpoints, 0);
        let status_events: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE instance = 'luna'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status_events, 0);
    }
}
