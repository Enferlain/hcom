use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::db::HcomDb;
use crate::shared::time::{monotonic_seconds, now_epoch_i64};
use crate::sys::{fs, process};

use super::state::{Tick, read_status};
use super::{ControllerStatus, Owner, Process, Store, lock_file, owner_released, status};

const WAIT: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(50);
const TICK: Duration = Duration::from_millis(250);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const USAGE: &str = "Usage: hcom sandbox controller <start|recover|stop|status>";

#[derive(Debug)]
pub enum Forwarding {
    ControllerReady(Process),
    Disabled,
    Deferred(String),
}

/// Resolve forwarding after the host outbox transaction commits.
pub fn forwarding_after_commit(store: &Store<'_>) -> Forwarding {
    forwarding_with(store, spawn_worker)
}

fn forwarding_with(store: &Store<'_>, spawn: impl Fn(&Path) -> Result<Child>) -> Forwarding {
    match ensure_running(store, false, spawn) {
        Ok(Some(owner)) => Forwarding::ControllerReady(owner),
        Ok(None) => Forwarding::Disabled,
        Err(error) => Forwarding::Deferred(format!("{error:#}")),
    }
}

/// The sandbox launch adapter calls this before publishing a success handle.
pub fn ensure_after_launch(store: &Store<'_>) -> Result<Process> {
    ensure_running(store, true, spawn_worker)?
        .context("sandbox controller was disabled during startup")
}

fn transition_lock(path: &Path) -> Result<std::fs::File> {
    let lock = lock_file(path, "transition.lock")?;
    let deadline = Instant::now() + WAIT;
    while !fs::try_lock_exclusive(&lock)? {
        if Instant::now() >= deadline {
            bail!("timed out waiting for sandbox controller transition");
        }
        thread::sleep(POLL);
    }
    Ok(lock)
}

fn ensure_running(
    store: &Store<'_>,
    enable: bool,
    spawn: impl Fn(&Path) -> Result<Child>,
) -> Result<Option<Process>> {
    if !enable && !read_status(store.connection()?)?.enabled {
        return Ok(None);
    }
    let _transition = transition_lock(store.path())?;
    if enable {
        store.set_enabled(true)?;
    }
    let (executable, executable_id) = current_executable()?;
    let deadline = Instant::now() + WAIT;
    let mut child: Option<Child> = None;
    let result = (|| loop {
        let snapshot = read_status(store.connection()?)?;
        if !snapshot.enabled {
            break Ok(None);
        }
        let released = owner_released(store.path())?;
        if snapshot.ready
            && !released
            && let Some(candidate) = snapshot.owner
        {
            let checked = read_status(store.connection()?)?;
            if checked.ready
                && let Some(owner) = checked.owner
                && owner.epoch == candidate.epoch
                && owner.pid == candidate.pid
                && child.as_ref().is_none_or(|child| owner.pid == child.id())
            {
                anyhow::ensure!(
                    owner.executable == executable && owner.executable_id == executable_id,
                    "running sandbox controller uses a different executable; stop it before starting this build"
                );
                break Ok(Some(owner));
            }
        }
        if let Some(child) = child.as_mut() {
            if let Some(exit) = child.try_wait()? {
                break Err(anyhow::anyhow!(
                    "sandbox controller exited during startup ({exit})"
                ));
            }
        } else if released {
            child = Some(spawn(store.path())?);
        }
        if Instant::now() >= deadline {
            break Err(anyhow::anyhow!(
                "sandbox controller did not publish a fresh heartbeat"
            ));
        }
        thread::sleep(POLL);
    })();
    if let Some(mut child) = child {
        if result.is_err() {
            // Only terminate the child we created, never a PID read from the database.
            let _ = child.kill();
        }
        thread::spawn(move || {
            let _ = child.wait();
        });
    }
    result
}

fn current_executable() -> Result<(String, String)> {
    let executable = std::env::current_exe()?;
    // On Linux this names the running image even if a new build replaced the file.
    #[cfg(target_os = "linux")]
    let image = Path::new("/proc/self/exe");
    #[cfg(not(target_os = "linux"))]
    let image = executable.as_path();
    let id = fs::file_id(image);
    anyhow::ensure!(id != 0, "could not identify sandbox controller executable");
    Ok((executable.to_string_lossy().into_owned(), id.to_string()))
}

fn spawn_worker(db_path: &Path) -> Result<Child> {
    anyhow::ensure!(
        !process::is_sandboxed(),
        "sandbox controller must start outside the local tool sandbox"
    );
    let hcom_dir = db_path
        .parent()
        .context("sandbox database has no state directory")?
        .canonicalize()?;
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("sandbox-worker")
        .current_dir(&hcom_dir)
        .env_clear()
        .env("HCOM_DIR", &hcom_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for key in ["PATH", "SystemRoot", "TMPDIR", "TEMP", "TMP"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    process::spawn_detached(&mut command).context("start detached sandbox controller")
}

fn disable(store: &Store<'_>) -> Result<()> {
    let _transition = transition_lock(store.path())?;
    store.set_enabled(false)?;
    let deadline = Instant::now() + WAIT;
    loop {
        if owner_released(store.path())? {
            if let Some(owner) = read_status(store.connection()?)?.owner {
                store.clear_controller(owner.epoch)?;
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("sandbox controller is disabled but has not released ownership");
        }
        thread::sleep(POLL);
    }
}

pub fn run_worker() -> Result<()> {
    let db = HcomDb::open()?;
    run_worker_in(&db)
}

fn run_worker_in(db: &HcomDb) -> Result<()> {
    let store = Store::initialize(db)?;
    if !read_status(store.connection()?)?.enabled {
        return Ok(());
    }
    let deadline = Instant::now() + WAIT;
    let ownership = loop {
        if let Some(ownership) = Owner::try_acquire(&store)? {
            break ownership;
        }
        if Instant::now() >= deadline || !read_status(store.connection()?)?.enabled {
            return Ok(());
        }
        thread::sleep(POLL);
    };
    let pid = std::process::id();
    let (executable, executable_id) = current_executable()?;
    let owner = Process {
        epoch: ownership.epoch(),
        pid,
        process_start: process::identity(pid).context("identify sandbox controller process")?,
        executable,
        executable_id,
        heartbeat: now_epoch_i64(),
        heartbeat_monotonic: monotonic_seconds()?,
    };
    if !store.claim_controller(&owner)? {
        return Ok(());
    }
    let result = supervise(db, &store, &ownership);
    let cleanup = store.clear_controller(ownership.epoch());
    result.and(cleanup)
}

fn supervise(_db: &HcomDb, store: &Store<'_>, owner: &Owner<'_, '_>) -> Result<()> {
    #[cfg(unix)]
    let mut routes = super::routes::Routes::default();
    let mut idle_since = Instant::now();
    let mut tick_at = idle_since;
    loop {
        if Instant::now() >= tick_at {
            let now = now_epoch_i64();
            match store.tick_controller(
                owner.epoch(),
                now,
                monotonic_seconds()?,
                idle_since.elapsed() >= IDLE_TIMEOUT,
            )? {
                Tick::Exit => {
                    #[cfg(unix)]
                    routes.drain(_db, store);
                    return Ok(());
                }
                Tick::LostOwnership => bail!("sandbox controller lost database ownership"),
                Tick::Work => idle_since = Instant::now(),
                Tick::Idle => {}
            }
            if !store.expire_pending(now)?.is_empty() {
                idle_since = Instant::now();
            }
            #[cfg(unix)]
            routes.refresh(store)?;
            tick_at = Instant::now() + TICK;
        }
        #[cfg(unix)]
        {
            routes.poll(_db, owner);
            if routes.is_active() {
                thread::sleep(super::routes::STEP);
                continue;
            }
        }
        thread::sleep(tick_at.saturating_duration_since(Instant::now()));
    }
}

pub fn run_command(args: &[String]) -> Result<()> {
    let args: Vec<_> = args.iter().map(String::as_str).collect();
    if matches!(&args[..], [] | ["controller"]) || matches!(args.last(), Some(&("--help" | "-h"))) {
        println!("{USAGE}");
        return Ok(());
    }
    let db_path = crate::paths::hcom_dir().join("hcom.db");
    match &args[..] {
        ["controller", "status"] => {}
        ["controller", "start" | "recover" | "stop"] => {
            let db = HcomDb::open()?;
            let store = Store::initialize(&db)?;
            match args[1] {
                "start" => {
                    ensure_after_launch(&store)?;
                }
                "recover" => {
                    ensure_running(&store, false, spawn_worker)?;
                }
                _ => disable(&store)?,
            }
        }
        _ => bail!("{USAGE}"),
    }
    let snapshot: ControllerStatus = status(&db_path)?;
    println!("{}", serde_json::to_string(&snapshot)?);
    Ok(())
}

#[cfg(test)]
mod tests;
