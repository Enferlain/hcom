//! Relay worker process — manages the MQTT relay as a standalone process.
//!
//! Entry point for `hcom relay-worker`. Handles PID file management,
//! signal handling, auto-exit watchdog, and relay lifecycle.
//!
//! Auto-spawn: any hcom activity spawns the worker whenever relay is enabled,
//! including on devices with no agents, so remote commands always land.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::config::HcomConfig;
use crate::db::HcomDb;
use crate::log;
use crate::relay::client::RelayCommand;

/// Set when the worker exits because a new build was installed: the exiting worker spawns its
/// successor after its pidfile is gone. Leaving the restart to the next hcom call would leave a
/// device with no local activity without a relay worker.
static HANDOFF_ON_EXIT: AtomicBool = AtomicBool::new(false);

/// The executable path this worker was started from, captured ONCE at start. The upgrade
/// watchdog fingerprints it and the hand-off spawns it. Asking the OS again at exit is wrong
/// after an upgrade: a rename-then-install swap leaves the running image under its new
/// (old-build) name, and on Linux an atomic replace makes /proc/self/exe read "... (deleted)",
/// so the successor could run the old build or fail to start.
static INSTALL_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();

fn install_path() -> Option<PathBuf> {
    INSTALL_PATH
        .get_or_init(|| std::env::current_exe().ok())
        .clone()
}

/// Ask the worker to start a successor when it exits.
fn request_handoff() {
    HANDOFF_ON_EXIT.store(true, Ordering::SeqCst);
}

// ── PID file helpers ────────────────────────────────────────────────

fn pid_file_path() -> PathBuf {
    crate::paths::hcom_dir().join(".tmp").join("relay.pid")
}

fn spawn_lock_path() -> PathBuf {
    crate::paths::hcom_dir()
        .join(".tmp")
        .join("relay.spawn.lock")
}

fn write_pid_file_for(pid: u32) {
    crate::paths::atomic_write(&pid_file_path(), &pid.to_string());
    // Seed heartbeat alongside the pidfile so readers in the startup window
    // (before the main loop starts ticking) don't see pid-alive + no-heartbeat
    // and falsely declare the worker dead.
    if let Ok(db) = HcomDb::open() {
        let _ = super::write_worker_heartbeat(&db);
    }
}

fn write_pid_file() {
    write_pid_file_for(std::process::id());
}

fn read_pid_file() -> Option<u32> {
    let path = pid_file_path();
    let content = std::fs::read_to_string(&path).ok()?;
    let pid: u32 = content.trim().parse().ok()?;
    if crate::pidtrack::is_alive(pid) {
        // NB: we deliberately do NOT gate this on heartbeat freshness. A
        // heartbeat-stale-but-alive PID may be a wedged worker (e.g. DB
        // contention blocking its tick writes), and forgetting its ownership
        // here would let a second worker spawn on top of the first and would
        // let stop_relay_worker SIGTERM the wrong process once the PID gets
        // reused. Display-layer callers (status, relay) cross-check heartbeat
        // themselves to avoid reporting a false-green "running".
        Some(pid)
    } else {
        // Stale PID file — delegate to remove_pid_file so the cleanup steps
        // (pidfile + heartbeat) stay in one place and can't drift apart.
        remove_pid_file();
        None
    }
}

/// Remove PID file and clear heartbeat KV.
fn remove_pid_file() {
    let _ = std::fs::remove_file(pid_file_path());
    if let Ok(db) = HcomDb::open() {
        super::clear_worker_heartbeat(&db);
    }
}

/// Check if a relay-worker process is currently running.
pub fn is_relay_worker_running() -> bool {
    read_pid_file().is_some()
}

/// Pure pidfile observer for `derive_relay_health`. Unlike `read_pid_file`,
/// never mutates the pidfile — needed because derivation must be side-effect
/// free (every status render would otherwise be a hidden state transition).
///
/// Returns:
///   None           — no pidfile on disk
///   Some(pid,true) — pidfile present, PID is alive
///   Some(pid,false) — pidfile present, PID is dead (stale)
pub fn observe_pid_file() -> Option<(u32, bool)> {
    let content = std::fs::read_to_string(pid_file_path()).ok()?;
    let pid: u32 = content.trim().parse().ok()?;
    Some((pid, crate::pidtrack::is_alive(pid)))
}

// ── Drop guard for PID file cleanup ─────────────────────────────────

struct PidFileGuard;

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        remove_pid_file();
    }
}

// ── Worker entry point ──────────────────────────────────────────────

/// Env the worker must not carry from whoever started it (often an agent's
/// hook): its terminal pane, which remote launches would target long after it
/// closed, and its AI-tool identity, which makes launch errors suggest sandbox
/// escalation.
fn inherited_identity_vars() -> impl Iterator<Item = String> {
    let instance_state = crate::integration_spec::ALL
        .iter()
        .flat_map(|spec| spec.instance_state_env.iter().map(|v| v.to_string()));
    crate::launcher::env_strip_set()
        .into_iter()
        .chain(instance_state)
        .chain(
            [
                "HCOM_INSTANCE_NAME",
                // Session sockets local launches keep on purpose (tab/split CLIs
                // target the launcher's own server), but in the worker they name
                // the spawner's, which may be gone. HERDR_PANE_ID also drives
                // terminal auto-detection.
                "WEZTERM_UNIX_SOCKET",
                "TMUX",
                "ZELLIJ",
                "ZELLIJ_SESSION_NAME",
                "HERDR_PANE_ID",
                "HERDR_SOCKET_PATH",
                "HERDR_ENV",
            ]
            .map(String::from),
        )
}

/// Drop identity the worker inherited from its spawner. The spawner normally
/// strips it, but after an upgrade the spawner is the previous build's worker,
/// which may not. Called from `main` before anything caches the env.
pub fn scrub_inherited_env() {
    // The scrub removes `CODEX_SANDBOX`, one of the sandbox signals; record the
    // verdict first.
    STARTED_SANDBOXED.store(crate::sys::process::is_sandboxed(), Ordering::SeqCst);
    for key in inherited_identity_vars() {
        // SAFETY: called first thing in `main`, before any thread exists.
        unsafe { std::env::remove_var(key) };
    }
}

static STARTED_SANDBOXED: AtomicBool = AtomicBool::new(false);

/// Run the relay-worker process. Called from router dispatch.
pub fn run() -> i32 {
    // The spawner normally refuses this, but after an upgrade the spawner is
    // the previous build's worker, which may not.
    if STARTED_SANDBOXED.load(Ordering::SeqCst) || crate::sys::process::is_sandboxed() {
        log::log_warn(
            "relay",
            "relay_worker.sandboxed_exit",
            "started inside a sandbox; exiting so an unsandboxed caller starts the worker",
        );
        return 1;
    }

    // Check if already running
    if let Some(existing_pid) = read_pid_file() {
        let current_pid = std::process::id();
        if existing_pid != current_pid {
            eprintln!("relay-worker already running (PID {})", existing_pid);
            return 1;
        }
    }

    // Write PID file (guard removes on exit)
    write_pid_file();
    let pid_guard = PidFileGuard;
    // Capture the install path now, before any upgrade can move or replace the file.
    let _ = install_path();

    log::log_info(
        "relay",
        "relay_worker.start",
        &format!("pid={}", std::process::id()),
    );

    // Load config
    let config = HcomConfig::load(None);

    if !super::is_relay_enabled(&config) {
        eprintln!("Error: Relay not configured or disabled");
        return 1;
    }

    // Connect to MQTT
    let (relay, connection, cmd_tx) = match super::client::MqttRelay::connect(&config) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: Failed to connect: {e}");
            return 1;
        }
    };

    // Bind TCP notify listener for CLI → daemon push wake.
    // CLI callers (hcom send, hooks) connect to trigger immediate push.
    let notify_port = setup_notify_listener(&cmd_tx);

    // Install shutdown-signal handlers (set AtomicBool on terminate/interrupt).
    // The watchdog thread checks this flag — no separate signal-polling thread needed.
    let shutdown = Arc::new(AtomicBool::new(false));
    crate::sys::signal::register_term(&shutdown);
    crate::sys::signal::register_int(&shutdown);

    // Spawn auto-exit watchdog thread (also monitors shutdown flag)
    let cmd_tx_watchdog = cmd_tx;
    std::thread::spawn(move || {
        auto_exit_watchdog(cmd_tx_watchdog, shutdown);
    });

    // Run relay event loop (blocks until shutdown)
    relay.run(connection);

    // Clear notify port so CLI callers stop trying to connect
    if notify_port.is_some()
        && let Ok(db) = HcomDb::open()
    {
        super::safe_kv_set(&db, "relay_daemon_port", None);
    }

    log::log_info("relay", "relay_worker.stop", "exited cleanly");
    if HANDOFF_ON_EXIT.load(Ordering::SeqCst) {
        // The successor refuses to start while this worker's pidfile names a live PID,
        // so release it first.
        drop(pid_guard);
        let spawned = do_spawn_with(install_path());
        log::log_info(
            "relay",
            "relay_worker.handoff",
            &format!("successor spawned={spawned}"),
        );
    }
    0
}

/// Bind TCP listener on random port for CLI→daemon push notifications.
/// Stores port in KV `relay_daemon_port`. Returns port on success.
fn setup_notify_listener(cmd_tx: &std::sync::mpsc::Sender<RelayCommand>) -> Option<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").ok()?;
    let port = listener.local_addr().ok()?.port();

    // Store port in DB so CLI callers can find us
    if let Ok(db) = HcomDb::open() {
        super::safe_kv_set(&db, "relay_daemon_port", Some(&port.to_string()));
    }

    log::log_info(
        "relay",
        "relay_worker.notify_listen",
        &format!("port={}", port),
    );

    // Spawn thread to accept connections and send Push commands.
    // Each incoming TCP connection (no data, just connect+close) triggers a push.
    let cmd_tx = cmd_tx.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(conn) => {
                    drop(conn); // Close immediately — connection itself is the signal
                    if cmd_tx.send(RelayCommand::Push).is_err() {
                        break; // Relay shut down
                    }
                }
                Err(_) => break,
            }
        }
    });

    Some(port)
}

/// Auto-exit watchdog: send Shutdown on a shutdown signal, when the executable is
/// replaced, or once relay is disabled for 2 consecutive checks (30s apart).
///
/// Local agent count deliberately plays no part: an enabled relay keeps the
/// worker alive so it can receive remote RPCs (e.g. the first `launch` on a
/// fresh device).
fn auto_exit_watchdog(cmd_tx: std::sync::mpsc::Sender<RelayCommand>, shutdown: Arc<AtomicBool>) {
    const CHECK_INTERVAL: Duration = Duration::from_secs(30);
    const SIGNAL_POLL: Duration = Duration::from_millis(200);
    let mut consecutive_disabled = 0u32;
    // An install replaces the executable file under a running worker. Exit when
    // that happens and the next hcom call (every hook calls ensure_worker)
    // starts a worker from the new build, so an upgrade needs no manual restart
    // and none of the elevation that killing a worker can need.
    let exe_path = install_path();
    let started_with = exe_path.as_deref().and_then(exe_fingerprint);

    loop {
        // Poll the signal flag often: `relay daemon stop` force-kills after 5s,
        // and a kill skips the clean shutdown that clears relay status.
        let next_check = std::time::Instant::now() + CHECK_INTERVAL;
        while std::time::Instant::now() < next_check {
            if shutdown.load(Ordering::Relaxed) {
                let _ = cmd_tx.send(RelayCommand::Shutdown);
                return;
            }
            std::thread::sleep(SIGNAL_POLL);
        }

        if let (Some(path), Some(start)) = (exe_path.as_deref(), started_with)
            && binary_replaced(path, start)
        {
            log::log_info(
                "relay",
                "relay_worker.binary_changed",
                "executable replaced; exiting so a worker from the new build takes over",
            );
            request_handoff();
            let _ = cmd_tx.send(RelayCommand::Shutdown);
            return;
        }

        if relay_enabled_in_config() {
            consecutive_disabled = 0;
            continue;
        }
        consecutive_disabled += 1;
        if consecutive_disabled >= 2 {
            log::log_info(
                "relay",
                "relay_worker.auto_exit",
                "relay disabled for 2 checks",
            );
            let _ = cmd_tx.send(RelayCommand::Shutdown);
            return;
        }
    }
}

type ExeFingerprint = (u64, std::time::SystemTime);

/// Size and modification time of an executable file.
fn exe_fingerprint(path: &std::path::Path) -> Option<ExeFingerprint> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.len(), meta.modified().ok()?))
}

/// True when the file at `path` is no longer the one the worker started from.
/// A missing file (mid-swap) is not a replacement yet.
fn binary_replaced(path: &std::path::Path, started_with: ExeFingerprint) -> bool {
    exe_fingerprint(path).is_some_and(|now| now != started_with)
}

/// Check if relay is enabled in the current config (non-empty relay_id + relay_enabled flag).
fn relay_enabled_in_config() -> bool {
    super::is_relay_enabled(&HcomConfig::load(None))
}

// ── Auto-spawn ──────────────────────────────────────────────────────

/// Spawn the relay-worker process (caller must check preconditions).
/// Detaches via setsid() so the worker survives terminal close.
/// Returns true if spawned successfully, false if already running or spawn failed.
fn do_spawn() -> bool {
    do_spawn_with(std::env::current_exe().ok())
}

/// `do_spawn` for a given executable: the hand-off passes the install path captured at start.
fn do_spawn_with(binary: Option<PathBuf>) -> bool {
    // A worker started from an agent's sandboxed shell stays sandboxed: it
    // still relays, but remote launches can't open terminals. Leave the spawn
    // to the next unsandboxed hook or command.
    if crate::sys::process::is_sandboxed() {
        log::log_warn(
            "relay",
            "relay_worker.spawn_skipped_sandboxed",
            "not starting the relay worker from inside a sandbox",
        );
        return false;
    }

    let lock_path = spawn_lock_path();
    if let Some(parent) = lock_path.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        log::log_warn(
            "relay",
            "relay_worker.spawn_lock_mkdir_err",
            &format!("{e}"),
        );
        return false;
    }

    let lock_file = match std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
    {
        Ok(file) => file,
        Err(e) => {
            log::log_warn("relay", "relay_worker.spawn_lock_open_err", &format!("{e}"));
            return false;
        }
    };

    if let Err(err) = crate::sys::fs::lock_exclusive(&lock_file) {
        log::log_warn("relay", "relay_worker.spawn_lock_err", &format!("{err}"));
        return false;
    }

    if is_relay_worker_running() {
        return false;
    }

    // Whatever runtime status is left belongs to a dead worker (a force-kill
    // skips clean shutdown). Clear it under the spawn lock, so it can't race a
    // concurrent spawn, and readers only ever see what the new worker reports.
    if let Ok(db) = HcomDb::open() {
        super::clear_runtime_relay_kv(&db);
    }

    // Pre-warm device_id in the parent so the spawned worker reads the same
    // UUID we'd report from this process. Without this, the worker and any
    // concurrent CLI (hcom relay status, etc.) can race read_device_uuid on
    // a fresh HCOM_DIR and end up with different UUIDs — causing the worker's
    // published short_id to disagree with what `relay status` displays.
    if super::read_device_uuid().is_none() {
        log::log_warn(
            "relay",
            "relay_worker.device_id_unwritable",
            "could not create device_id file before spawn",
        );
        return false;
    }

    let Some(binary) = binary else {
        return false;
    };

    let mut cmd = Command::new(&binary);
    cmd.arg("relay-worker")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for key in inherited_identity_vars() {
        cmd.env_remove(key);
    }

    // Detach into its own session so it survives parent terminal close (no
    // SIGHUP) and, on Windows, doesn't inherit the parent's stdio handles
    // (which would otherwise keep any caller piping hcom's output from ever
    // observing EOF).
    match crate::sys::process::spawn_detached(&mut cmd) {
        Ok(child) => {
            write_pid_file_for(child.id());
            log::log_info(
                "relay",
                "relay_worker.spawned",
                &format!("pid={}", child.id()),
            );
            true
        }
        Err(e) => {
            log::log_warn("relay", "relay_worker.spawn_err", &format!("{}", e));
            false
        }
    }
}

/// Ensure the relay worker is running and its notify port is live, for explicit
/// commands (relay connect/new/on, daemon start). Polls for readiness (max 500ms)
/// even when the worker was already running, to cover the startup window before
/// port bind. Hooks and `send` use the fire-and-forget [`try_spawn_worker`].
///
/// Returns true if the worker is running and port-ready.
pub fn ensure_worker() -> bool {
    let config = HcomConfig::load(None);

    if !super::is_relay_enabled(&config) {
        return false;
    }

    // Explicit command path: ensure running AND port-ready.
    if is_relay_worker_running() {
        // Process exists but may be in startup window before port bind.
        if poll_until_ready(300) {
            return true;
        }
        // The existing worker may have exited while we were polling (for
        // example, a user just stopped it or it hit watchdog exit). In that
        // case, fall through and try to spawn a fresh worker.
        if is_relay_worker_running() {
            return false;
        }
    }
    if !do_spawn() {
        // TOCTOU: another process may have spawned between our check and do_spawn().
        if is_relay_worker_running() {
            return poll_until_ready(300);
        }
        return false;
    }
    poll_until_ready(500)
}

/// Spawn the relay worker if relay is enabled and not already running.
/// Fire-and-forget, no readiness wait: called from hooks, `send`, the TUI and
/// trigger_push(), so any hcom activity revives a missing worker.
pub fn try_spawn_worker() {
    if is_relay_worker_running() {
        return;
    }
    let config = HcomConfig::load(None);
    if super::is_relay_enabled(&config) {
        do_spawn();
    }
}

/// Poll until the worker's TCP notify port is in KV and accepting connections.
/// Opens DB once before the loop to avoid repeated open overhead.
/// Returns true if ready within timeout_ms, false on timeout.
fn poll_until_ready(timeout_ms: u64) -> bool {
    let start = std::time::Instant::now();
    let deadline = std::time::Duration::from_millis(timeout_ms);
    let db = HcomDb::open().ok();

    while start.elapsed() < deadline {
        if let Some(ref db) = db
            && let Some(port_str) = super::safe_kv_get(db, "relay_daemon_port")
            && let Ok(port) = port_str.trim().parse::<u16>()
        {
            use std::net::{SocketAddr, TcpStream};
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            if TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(50)).is_ok() {
                return true;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(30));
    }
    false
}

/// Return the PID of the running relay worker, or None if not running.
pub fn relay_worker_pid() -> Option<u32> {
    read_pid_file()
}

/// Remove the relay worker PID file (for post-SIGKILL cleanup).
pub fn remove_relay_pid_file() {
    remove_pid_file();
}

/// Stop a running relay-worker by sending SIGTERM to the PID from PID file.
pub fn stop_relay_worker() -> bool {
    if let Some(pid) = read_pid_file()
        && crate::sys::process::terminate(pid)
    {
        log::log_info("relay", "relay_worker.stopped", &format!("pid={}", pid));
        return true;
    }
    false
}

/// Stop the relay worker and block until it exits, escalating to a force-kill
/// if the graceful request does not take effect within ~5s. Removes the PID
/// file once the worker is gone.
///
/// Unlike [`stop_relay_worker`], this *guarantees* termination. Callers with no
/// other backstop must use this: [`terminate`](crate::sys::process::terminate)
/// is best-effort and on Windows may not be delivered when the worker shares no
/// console with the caller, so a bare `stop_relay_worker` could leave the worker
/// running (the auto-exit watchdog keeps it alive while relay is enabled).
pub fn stop_relay_worker_blocking() {
    let Some(pid) = read_pid_file() else {
        return;
    };

    let _ = stop_relay_worker();
    for _ in 0..50 {
        if !crate::pidtrack::is_alive(pid) {
            remove_pid_file();
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Graceful request did not take effect in time; force-kill so a stale
    // worker cannot survive a relay reset/off.
    crate::sys::process::kill(pid);
    remove_pid_file();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_replaced_executable_is_detected_and_a_missing_one_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("hcom.exe");
        std::fs::write(&exe, b"old build").unwrap();
        let start = exe_fingerprint(&exe).unwrap();
        assert!(!binary_replaced(&exe, start), "unchanged file");

        // Mid-swap: the running file was renamed away and the new one is not
        // there yet.
        std::fs::rename(&exe, dir.path().join("hcom.exe.pre-old")).unwrap();
        assert!(
            !binary_replaced(&exe, start),
            "missing file is not a replacement"
        );

        std::fs::write(&exe, b"the new, longer build").unwrap();
        assert!(binary_replaced(&exe, start), "new file in place");
    }

    #[test]
    fn test_pid_file_path() {
        crate::config::Config::init();
        let path = pid_file_path();
        assert!(path.to_string_lossy().contains("relay.pid"));
    }
}
