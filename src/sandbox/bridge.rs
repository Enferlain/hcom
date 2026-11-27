//! Framed, restartable OpenShell exec transport for one owned sandbox.

use std::path::PathBuf;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rusqlite::{Transaction, params};
use serde::Deserialize;

use super::controller::Owner;
use super::openshell::Adapter;
use super::protocol::{Activity, Binding, Message, ObservationKey, Receiver, TeamPeer};
use super::store::Store;
use crate::db::HcomDb;
use crate::shared::time::now_iso;

mod inbox;
mod peer;
mod proof;
mod wire;

pub use inbox::HostInbox;

pub trait Inbound {
    fn insert(&mut self, tx: &Transaction<'_>, message: &Message) -> Result<i64>;
    fn committed(&mut self, _event_id: i64) {}
    fn observe(
        &self,
        _tx: &Transaction<'_>,
        _key: Option<&ObservationKey>,
    ) -> Result<(bool, Option<Activity>)> {
        Ok((false, None))
    }
    fn replace_directory(
        &mut self,
        _tx: &Transaction<'_>,
        peers: Option<&[TeamPeer]>,
    ) -> Result<()> {
        ensure!(
            peers.is_none(),
            "sandbox bridge has no local worker for a team directory"
        );
        Ok(())
    }
}

impl<F> Inbound for F
where
    F: FnMut(&Transaction<'_>, &Message) -> Result<i64>,
{
    fn insert(&mut self, tx: &Transaction<'_>, message: &Message) -> Result<i64> {
        self(tx, message)
    }
}

use peer::{Limits, Peer};

pub(crate) const GUEST_COMMAND: &str = "sandbox-guest-bridge";
const POLL_INTERVAL: Duration = Duration::from_millis(5);
const DIAGNOSTIC_LIMIT: usize = 64 * 1024;
const DIAGNOSTIC_TAIL: usize = 1024;

pub struct ExecTarget<'a> {
    /// The lifecycle layer resolves this name from the owned sandbox ID.
    pub sandbox_name: &'a str,
    pub guest_hcom: &'a str,
    pub guest_manifest: &'a str,
    /// Shared with the specific guest during provisioning. Never passed in argv.
    pub startup_key: Option<&'a [u8; proof::BYTES]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exit {
    Rotate,
    Stopped,
}

/// Drive one exec. A replacement always reads the oldest still-pending
/// message from the store, even when the previous exec exited unsuccessfully.
pub fn exec(
    owner: &Owner<'_, '_>,
    adapter: &Adapter,
    target: ExecTarget<'_>,
    binding: &Binding,
    stop: impl FnMut() -> bool,
    import: impl Inbound,
) -> Result<Exit> {
    ensure!(
        absolute_guest_path(target.guest_manifest),
        "sandbox bridge requires an absolute guest manifest path"
    );
    if let Some(worker) = validate_local_exec(owner.store(), binding, &target)? {
        adapter.verify_owned_target(target.sandbox_name, &worker)?;
    }
    let session = start_verified(owner, adapter, target, binding)?;
    session.run(stop, import)
}

fn validate_local_exec(
    store: &Store<'_>,
    binding: &Binding,
    target: &ExecTarget<'_>,
) -> Result<Option<super::directory::Worker>> {
    let worker = super::directory::current_worker(store.connection()?, &binding.worker.worker)?;
    match &worker {
        Some(worker) => ensure!(
            worker.binding == *binding && target.startup_key.is_some(),
            "sandbox exec route or startup key no longer matches enrollment"
        ),
        // The low-level VM qualification fixture is the only unregistered route.
        None => ensure!(
            cfg!(test) && target.startup_key.is_none(),
            "sandbox exec route is not enrolled"
        ),
    }
    Ok(worker)
}

/// The caller has just verified OpenShell placement. The local binding and
/// epoch are checked again here, before starting the authenticated exec.
pub(super) fn start_verified<'store, 'db>(
    owner: &'store Owner<'_, 'db>,
    adapter: &Adapter,
    target: ExecTarget<'_>,
    binding: &Binding,
) -> Result<Session<'store, 'db>> {
    ensure!(
        absolute_guest_path(target.guest_manifest),
        "sandbox bridge requires an absolute guest manifest path"
    );
    validate_local_exec(owner.store(), binding, &target)?;
    let epoch = owner.allocate_connection_epoch()?;
    let command = adapter.managed_exec(
        target.sandbox_name,
        target.guest_hcom,
        &[
            GUEST_COMMAND.into(),
            target.guest_manifest.into(),
            epoch.to_string().into(),
        ],
    )?;
    let peer = Peer::new(
        owner.store(),
        binding,
        epoch,
        Receiver::Host,
        Some(adapter.measured_stdin_bytes()),
        Limits::default(),
        owner.store().snapshot_directory(binding)?,
    )?;
    Session::new(command, peer, target.startup_key)
}

pub(super) struct Session<'store, 'db> {
    process: Process,
    peer: Peer<'store, 'db>,
}

impl<'store, 'db> Session<'store, 'db> {
    fn new(
        command: Command,
        mut peer: Peer<'store, 'db>,
        startup_key: Option<&[u8; proof::BYTES]>,
    ) -> Result<Self> {
        let (binding, epoch) = peer.binding_and_epoch();
        let proof = startup_key
            .map(|key| proof::Check::new(key, binding, epoch))
            .transpose()?;
        if proof.is_some() {
            peer.reserve_host_input(proof::BYTES)?;
            peer.enable_observation();
        }
        let mut process = Process::spawn(command)?;
        process.proof = proof;
        peer.start_clock();
        Ok(Self { process, peer })
    }

    pub(super) fn poll(&mut self, stop: bool, inbox: &mut impl Inbound) -> Result<Option<Exit>> {
        match poll_host(&mut self.process, &mut self.peer, || stop, inbox)? {
            HostStep::Finished(exit) => Ok(Some(exit)),
            HostStep::Progress | HostStep::Waiting => Ok(None),
        }
    }

    pub(super) fn ready(&self) -> bool {
        self.peer.is_ready()
    }

    fn run(mut self, stop: impl FnMut() -> bool, import: impl Inbound) -> Result<Exit> {
        let result = drive_host(&mut self.process, self.peer, stop, import);
        if result.is_err()
            && self.process.proof.is_none()
            && let Some(diagnostic) = self.process.diagnostic_tail()
        {
            return result.with_context(|| format!("sandbox bridge exec stderr: {diagnostic}"));
        }
        result
    }
}

#[cfg(test)]
fn run_host(
    command: Command,
    peer: Peer<'_, '_>,
    stop: impl FnMut() -> bool,
    import: impl Inbound,
    startup_key: Option<&[u8; proof::BYTES]>,
) -> Result<Exit> {
    Session::new(command, peer, startup_key)?.run(stop, import)
}

fn drive_host(
    process: &mut Process,
    mut peer: Peer<'_, '_>,
    mut stop: impl FnMut() -> bool,
    mut import: impl Inbound,
) -> Result<Exit> {
    loop {
        match poll_host(process, &mut peer, &mut stop, &mut import)? {
            HostStep::Finished(exit) => return Ok(exit),
            HostStep::Progress => {}
            HostStep::Waiting => std::thread::sleep(POLL_INTERVAL),
        }
    }
}

enum HostStep {
    Progress,
    Waiting,
    Finished(Exit),
}

/// Advance one route without waiting on subprocess I/O. The caller can
/// interleave other routes and controller state transitions before polling again.
fn poll_host(
    process: &mut Process,
    peer: &mut Peer<'_, '_>,
    stop: impl FnOnce() -> bool,
    import: &mut impl Inbound,
) -> Result<HostStep> {
    peer.check()?;
    let mut progress = process.read_diagnostics()?;
    if let Some(proof) = &mut process.proof {
        let stdin = process
            .stdin
            .as_mut()
            .context("missing sandbox guest startup pipe")?;
        if !proof.poll(&mut process.stdout, stdin)? {
            return Ok(HostStep::Waiting);
        }
        process.proof = None;
        process.diagnostics.clear();
        progress = true;
    }
    if peer.is_ready() && peer.draining.is_none() && stop() {
        peer.begin_drain(false)?;
    }
    progress |= peer.read_step(&mut process.stdout, import)?;
    if peer.input_eof {
        peer.note_eof();
        process.stdin.take();
    }
    if let Some(stdin) = process.stdin.as_mut() {
        progress |= peer.write_step(stdin)?;
        if peer.close_written {
            process.stdin.take();
        }
    }
    if peer.input_eof
        && process.stderr_eof
        && let Some(status) = process.status()?
    {
        ensure!(status.success(), "sandbox bridge exec failed ({status})");
        ensure!(
            peer.remote_closed && peer.close_written,
            "sandbox bridge exec disconnected before completing the close handshake"
        );
        let exit = if peer.rotating() {
            peer.complete_rotation();
            Exit::Rotate
        } else {
            Exit::Stopped
        };
        return Ok(HostStep::Finished(exit));
    }
    progress |= peer.poll_outbox()?;
    progress |= peer.poll_observation()?;
    Ok(if progress {
        HostStep::Progress
    } else {
        HostStep::Waiting
    })
}

struct Process {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: ChildStdout,
    stderr: ChildStderr,
    stderr_eof: bool,
    diagnostics: Vec<u8>,
    reaped: bool,
    proof: Option<proof::Check>,
}

impl Process {
    fn spawn(mut command: Command) -> Result<Self> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = crate::sys::process::spawn_detached(&mut command)
            .context("start sandbox bridge exec")?;
        let process = Self {
            stdin: child.stdin.take(),
            stdout: child.stdout.take().expect("piped stdout"),
            stderr: child.stderr.take().expect("piped stderr"),
            stderr_eof: false,
            diagnostics: Vec::new(),
            reaped: false,
            proof: None,
            child,
        };
        wire::nonblocking(process.stdin.as_ref().expect("piped stdin"))?;
        wire::nonblocking(&process.stdout)?;
        wire::nonblocking(&process.stderr)?;
        Ok(process)
    }

    fn read_diagnostics(&mut self) -> Result<bool> {
        if self.stderr_eof {
            return Ok(false);
        }
        let mut buffer = [0; 4096];
        let Some(count) = wire::try_read(&mut self.stderr, &mut buffer)? else {
            return Ok(false);
        };
        self.stderr_eof = count == 0;
        ensure!(
            self.diagnostics.len() + count <= DIAGNOSTIC_LIMIT,
            "sandbox bridge exceeded stderr limit"
        );
        self.diagnostics.extend_from_slice(&buffer[..count]);
        Ok(true)
    }

    fn diagnostic_tail(&self) -> Option<String> {
        let start = self.diagnostics.len().saturating_sub(DIAGNOSTIC_TAIL);
        let tail = String::from_utf8_lossy(&self.diagnostics[start..]);
        let tail = tail.trim();
        // Preserve error context while keeping terminal control sequences and
        // line breaks from crossing the guest rendering boundary.
        (!tail.is_empty()).then(|| tail.escape_default().to_string())
    }

    fn status(&mut self) -> Result<Option<ExitStatus>> {
        let status = self.child.try_wait()?;
        self.reaped = status.is_some();
        Ok(status)
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // The child remains unreaped until both output pipes finish, so its
        // process group identity remains reserved even when it left descendants.
        if !self.reaped {
            crate::sys::process::kill_child_group(&mut self.child);
            let _ = self.child.wait();
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GuestManifest {
    binding: Binding,
    database: PathBuf,
    local_worker: Option<LocalWorker>,
    startup_key: Option<[u8; proof::BYTES]>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalWorker {
    name: String,
    created_at: f64,
}

pub(crate) fn guest_main(args: &[String]) -> Result<()> {
    use std::io::{IsTerminal, Read};
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let [manifest, epoch] = args else {
        anyhow::bail!("invalid sandbox guest bridge arguments");
    };
    ensure!(
        absolute_guest_path(manifest),
        "invalid sandbox guest bridge manifest path"
    );
    let epoch = epoch
        .parse::<i64>()
        .context("invalid sandbox bridge epoch")?;
    ensure!(epoch > 0, "invalid sandbox bridge epoch");
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    ensure!(
        !stdin.is_terminal() && !stdout.is_terminal(),
        "sandbox guest bridge requires non-terminal stdio"
    );

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(manifest)?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file(),
        "sandbox guest bridge manifest must be a regular file"
    );
    let mut bytes = Vec::new();
    file.take(16 * 1024 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 16 * 1024,
        "sandbox guest bridge manifest is too large"
    );
    let manifest: GuestManifest =
        serde_json::from_slice(&bytes).context("invalid sandbox guest bridge manifest")?;
    ensure!(
        manifest.startup_key.is_none() || metadata.permissions().mode() & 0o077 == 0,
        "sandbox startup key requires a private guest manifest"
    );
    manifest.binding.validate()?;
    ensure!(
        manifest.database.is_absolute() && manifest.database.is_file(),
        "sandbox guest bridge requires an existing private database"
    );
    let db = HcomDb::open_at(&manifest.database)?;
    let store = Store::initialize(&db)?;
    ensure!(
        store.incarnation() == manifest.binding.worker.incarnation,
        "sandbox guest database incarnation mismatch"
    );

    let mut input = std::fs::File::from(nix::unistd::dup(stdin)?);
    let mut output = std::fs::File::from(nix::unistd::dup(stdout)?);
    wire::nonblocking(&input)?;
    wire::nonblocking(&output)?;
    let mut peer = Peer::new(
        &store,
        &manifest.binding,
        epoch,
        Receiver::Guest,
        None,
        Limits::default(),
        None,
    )?;
    if let Some(local_worker) = &manifest.local_worker {
        let inbox = inbox::GuestInbox::new(&db, &manifest.binding, local_worker)?;
        if let Some(key) = &manifest.startup_key {
            store.write(|tx| inbox.check_local_worker(tx))?;
            proof::answer(&mut input, &mut output, key, &manifest.binding, epoch)?;
        }
        return drive_guest(&mut input, &mut output, &mut peer, inbox);
    }
    ensure!(
        manifest.startup_key.is_none(),
        "sandbox startup key requires a registered guest worker"
    );
    drive_guest(&mut input, &mut output, &mut peer, stage_guest_message)
}

fn drive_guest(
    input: &mut impl std::io::Read,
    output: &mut impl std::io::Write,
    peer: &mut Peer<'_, '_>,
    mut inbound: impl Inbound,
) -> Result<()> {
    loop {
        peer.check()?;
        let mut progress = peer.read_step(input, &mut inbound)?;
        ensure!(
            !peer.input_eof || peer.remote_closed,
            "sandbox bridge host disconnected without closing"
        );
        progress |= peer.write_step(output)?;
        if peer.close_written {
            return Ok(());
        }
        progress |= peer.poll_outbox()?;
        if !progress {
            std::thread::sleep(POLL_INTERVAL);
        }
    }
}

pub(super) fn absolute_guest_path(path: &str) -> bool {
    path.starts_with('/') && !path.chars().any(char::is_control)
}

// This transport-owned event lets the guest persist a payload atomically
// before replying. Ordinary inbox insertion/wakes are a separate adapter.
fn stage_guest_message(tx: &Transaction<'_>, message: &Message) -> Result<i64> {
    tx.execute(
        "INSERT INTO events (timestamp, type, instance, data) VALUES (?, 'sandbox_bridge_ingress', ?, ?)",
        params![now_iso(), message.payload.target, serde_json::to_string(message)?],
    )?;
    Ok(tx.last_insert_rowid())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod live;
