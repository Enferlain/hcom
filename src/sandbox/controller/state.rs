use std::path::Path;

use anyhow::{Result, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::shared::time::monotonic_seconds;
use crate::sys::process;

use super::Store;

pub(in crate::sandbox) const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS sandbox_controller_state (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
        epoch INTEGER,
        pid INTEGER,
        process_start TEXT,
        executable TEXT,
        executable_id TEXT,
        heartbeat INTEGER,
        heartbeat_monotonic INTEGER,
        CHECK (
            (epoch IS NULL AND pid IS NULL AND process_start IS NULL AND executable IS NULL
             AND executable_id IS NULL AND heartbeat IS NULL AND heartbeat_monotonic IS NULL)
            OR (epoch IS NOT NULL AND pid IS NOT NULL AND process_start IS NOT NULL AND executable IS NOT NULL
             AND executable_id IS NOT NULL AND heartbeat IS NOT NULL AND heartbeat_monotonic IS NOT NULL)
        )
    ) STRICT;
    INSERT OR IGNORE INTO sandbox_controller_state (singleton, enabled) VALUES (1, 0);
    CREATE TABLE IF NOT EXISTS sandbox_controller_work (
        run_key TEXT PRIMARY KEY,
        state TEXT NOT NULL CHECK (state IN ('provisioning', 'running', 'unknown', 'stopped', 'terminal')),
        artifact_pending INTEGER NOT NULL CHECK (artifact_pending IN (0, 1))
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_controller_routes (
        generation TEXT PRIMARY KEY REFERENCES sandbox_workers(generation),
        plan TEXT NOT NULL
    ) STRICT;
";

const HEARTBEAT_TTL_SECONDS: i64 = 8;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Process {
    pub epoch: i64,
    pub pid: u32,
    pub process_start: String,
    pub executable: String,
    pub executable_id: String,
    pub heartbeat: i64,
    pub heartbeat_monotonic: i64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ControllerStatus {
    pub enabled: bool,
    pub owner: Option<Process>,
    pub alive: bool,
    pub ready: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum RunState {
    Provisioning,
    Running,
    Unknown,
    Stopped,
    Terminal,
}

impl RunState {
    fn key(self) -> &'static str {
        match self {
            Self::Provisioning => "provisioning",
            Self::Running => "running",
            Self::Unknown => "unknown",
            Self::Stopped => "stopped",
            Self::Terminal => "terminal",
        }
    }
}

pub fn status(db_path: &Path) -> Result<ControllerStatus> {
    let conn = match Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY) {
        Ok(conn) => conn,
        Err(error) => match db_path.try_exists() {
            Ok(false) => return Ok(ControllerStatus::default()),
            _ => return Err(error.into()),
        },
    };
    conn.busy_timeout(std::time::Duration::from_secs(2))?;
    let mut snapshot = read_status(&conn)?;
    if snapshot.owner.is_some() && super::owner_released(db_path)? {
        snapshot.alive = false;
        snapshot.ready = false;
    }
    Ok(snapshot)
}

pub(super) fn read_status(conn: &Connection) -> Result<ControllerStatus> {
    let has_table: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'sandbox_controller_state')",
        [], |row| row.get(0),
    )?;
    if !has_table {
        return Ok(ControllerStatus::default());
    }
    let (enabled, owner) = conn
        .query_row(
            "SELECT enabled, epoch, pid, process_start, executable, executable_id, heartbeat, heartbeat_monotonic
         FROM sandbox_controller_state WHERE singleton = 1",
            [],
            |row| {
                let owner = if let Some(epoch) = row.get(1)? {
                    Some(Process {
                        epoch,
                        pid: row.get(2)?,
                        process_start: row.get(3)?,
                        executable: row.get(4)?,
                        executable_id: row.get(5)?,
                        heartbeat: row.get(6)?,
                        heartbeat_monotonic: row.get(7)?,
                    })
                } else {
                    None
                };
                Ok((row.get::<_, bool>(0)?, owner))
            },
        )
        .optional()?
        .unwrap_or_default();
    let alive = owner
        .as_ref()
        .is_some_and(|owner| process::has_identity(owner.pid, &owner.process_start));
    let now = monotonic_seconds()?;
    let ready = enabled
        && alive
        && owner.as_ref().is_some_and(|owner| {
            now.checked_sub(owner.heartbeat_monotonic)
                .is_some_and(|age| (0..=HEARTBEAT_TTL_SECONDS).contains(&age))
        });
    Ok(ControllerStatus {
        enabled,
        owner,
        alive,
        ready,
    })
}

impl Store<'_> {
    pub fn set_run_work(
        &self,
        run_key: &str,
        state: RunState,
        artifact_pending: bool,
    ) -> Result<()> {
        super::super::protocol::validate_identifier(run_key)?;
        self.write(|tx| {
            let retired: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sandbox_workers WHERE generation = ? AND retired = 1)",
                [run_key],
                |row| row.get(0),
            )?;
            ensure!(
                !retired || matches!(state, RunState::Terminal),
                "retired sandbox worker cannot resume controller work"
            );
            tx.execute(
                "INSERT INTO sandbox_controller_work VALUES (?, ?, ?)
                 ON CONFLICT (run_key) DO UPDATE SET state = excluded.state,
                 artifact_pending = excluded.artifact_pending",
                params![run_key, state.key(), artifact_pending],
            )?;
            Ok(())
        })
    }

    pub(super) fn set_enabled(&self, enabled: bool) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "UPDATE sandbox_controller_state SET enabled = ? WHERE singleton = 1",
                [enabled],
            )?;
            Ok(())
        })
    }

    pub(super) fn claim_controller(&self, owner: &Process) -> Result<bool> {
        self.write(|tx| {
            Ok(tx.execute(
                "UPDATE sandbox_controller_state SET epoch = ?, pid = ?, process_start = ?,
             executable = ?, executable_id = ?, heartbeat = ?, heartbeat_monotonic = ?
             WHERE singleton = 1 AND enabled = 1",
                params![
                    owner.epoch,
                    owner.pid,
                    owner.process_start,
                    owner.executable,
                    owner.executable_id,
                    owner.heartbeat,
                    owner.heartbeat_monotonic
                ],
            )? == 1)
        })
    }

    pub(super) fn clear_controller(&self, epoch: i64) -> Result<()> {
        self.write(|tx| {
            clear_controller(tx, epoch)?;
            Ok(())
        })
    }

    pub(super) fn tick_controller(
        &self,
        epoch: i64,
        now: i64,
        monotonic_now: i64,
        idle_timeout_elapsed: bool,
    ) -> Result<Tick> {
        self.write(|tx| {
            let row = tx.query_row(
                "SELECT enabled FROM sandbox_controller_state WHERE singleton = 1 AND epoch = ?",
                [epoch], |row| row.get::<_, bool>(0),
            ).optional()?;
            let Some(enabled) = row else {
                return Ok(Tick::LostOwnership);
            };
            let pending: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sandbox_outbox WHERE accepted = 0 AND expires_at > ?)
                      OR EXISTS(SELECT 1 FROM sandbox_controller_work WHERE
                          state IN ('provisioning', 'running', 'unknown') OR artifact_pending = 1)",
                [now],
                |row| row.get(0),
            )?;
            if !enabled || (!pending && idle_timeout_elapsed) {
                clear_controller(tx, epoch)?;
                return Ok(Tick::Exit);
            }
            tx.execute(
                "UPDATE sandbox_controller_state SET heartbeat = ?, heartbeat_monotonic = ?
                        WHERE singleton = 1 AND epoch = ? AND heartbeat_monotonic <> ?",
                params![now, monotonic_now, epoch, monotonic_now],
            )?;
            Ok(if pending { Tick::Work } else { Tick::Idle })
        })
    }
}

fn clear_controller(conn: &Connection, epoch: i64) -> Result<()> {
    let rows = conn.execute(
        "UPDATE sandbox_controller_state SET epoch = NULL, pid = NULL, process_start = NULL,
         executable = NULL, executable_id = NULL, heartbeat = NULL, heartbeat_monotonic = NULL
         WHERE singleton = 1 AND epoch = ?",
        [epoch],
    )?;
    ensure!(rows <= 1, "invalid sandbox controller ownership record");
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Tick {
    Exit,
    LostOwnership,
    Work,
    Idle,
}
