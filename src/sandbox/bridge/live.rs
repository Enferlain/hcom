//! Opt-in fixture for the pinned, disposable OpenShell VM.

use std::cell::Cell;
use std::fs;
use std::time::Instant;

use crate::sandbox::admission;
use crate::sandbox::openshell::{Profile, Qualification};
use crate::sandbox::protocol::{Intent, MessagePayload, Origin};
use crate::sandbox::store::Delivery;
use crate::shared::time::now_epoch_i64;

use super::*;

const MESSAGES: i64 = 22;
const TEXT_BYTES: usize = 200 * 1024;
const REMOTE: &str = "/sandbox/hcom-fixture";
// These routes share the same per-run admission budget because they use
// the same sandbox and generation. Keep the aggregate under its bound.
const ROUTES: [(&str, i64); 3] = [("input", 0), ("output", MESSAGES), ("replay", 1)];

#[derive(Deserialize)]
struct Fixture {
    root: PathBuf,
    sandbox: String,
    sandbox_id: String,
    profile: Profile,
    receipt: Qualification,
}

#[test]
#[ignore = "requires a disposable OpenShell VM with the qualification fixture staged"]
fn vm_qualification() -> Result<()> {
    let config = std::env::var_os("HCOM_BRIDGE_VM_FIXTURE")
        .context("set HCOM_BRIDGE_VM_FIXTURE to the disposable fixture JSON")?;
    let fixture: Fixture = serde_json::from_slice(&fs::read(config)?)?;
    match std::env::var("HCOM_BRIDGE_VM_PHASE")?.as_str() {
        "prepare" => fixture.prepare(),
        "exercise" => fixture.exercise(),
        "interrupt" => fixture.interrupt(),
        "restart" => fixture.restart(),
        _ => anyhow::bail!("unknown OpenShell bridge fixture phase"),
    }
}

impl Fixture {
    fn prepare(&self) -> Result<()> {
        for (route, count) in ROUTES {
            let path = self.root.join(format!("{route}.db"));
            ensure!(!path.exists(), "fixture database already exists");
            let db = HcomDb::open_at(&path)?;
            let store = Store::initialize(&db)?;
            let worker = Origin {
                worker: route.into(),
                generation: "fixture-1".into(),
                incarnation: store.incarnation().into(),
            };
            let binding = Binding {
                team: "fixture".into(),
                sandbox: self.sandbox_id.clone(),
                worker: worker.clone(),
            };
            for _ in 0..count {
                store.enqueue(
                    Receiver::Host,
                    &binding,
                    worker.clone(),
                    payload("host", 'g'),
                    now_epoch_i64(),
                    stage_guest_message,
                )?;
            }
            db.conn().execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
            fs::write(
                self.root.join(format!("{route}.json")),
                serde_json::to_vec(&serde_json::json!({
                    "binding": binding,
                    "database": format!("{REMOTE}/{route}.db"),
                }))?,
            )?;
        }
        Ok(())
    }

    fn binding(&self, route: &str) -> Result<Binding> {
        let manifest: GuestManifest =
            serde_json::from_slice(&fs::read(self.root.join(format!("{route}.json")))?)?;
        Ok(manifest.binding)
    }

    fn with_host<T>(&self, run: impl FnOnce(&Adapter, &Owner<'_, '_>) -> Result<T>) -> Result<T> {
        let adapter = Adapter::preflight(self.profile.clone(), &self.receipt)?;
        let db = HcomDb::open_at(&self.root.join("host.db"))?;
        let mut limits = admission::Limits::default();
        // Exercise >4 MiB through a single reverse-output exec. The regular
        // pacing tests cover the lower production rate.
        limits.pace.bytes = 8 * 1024 * 1024;
        let store = Store::with_limits(&db, limits)?;
        let owner = Owner::try_acquire(&store)?.context("fixture controller lock is held")?;
        run(&adapter, &owner)
    }

    fn exercise(&self) -> Result<()> {
        self.with_host(|adapter, owner| {
            ensure!(adapter.measured_stdin_bytes() == 4 * 1024 * 1024);
            let store = owner.store();
            let input = self.binding("input")?;
            let host = Origin {
                worker: "host".into(),
                generation: "fixture-1".into(),
                incarnation: store.incarnation().into(),
            };
            let mut last = None;
            for _ in 0..MESSAGES {
                last = Some(store.enqueue(
                    Receiver::Guest,
                    &input,
                    host.clone(),
                    payload(&input.worker.worker, 'h'),
                    now_epoch_i64(),
                    stage_guest_message,
                )?);
            }
            let last = last.context("missing fixture payload")?;
            let mut rotations = 0;
            loop {
                let end = self.exec_route(adapter, owner, &input, || {
                    // The host sends one pending item at a time in sequence order.
                    store
                        .delivery(&last.id, now_epoch_i64())
                        .expect("read final input delivery status")
                        == Some(Delivery::Accepted)
                })?;
                if end == Exit::Stopped {
                    break;
                }
                rotations += 1;
                ensure!(rotations <= 3, "too many fixture rotations");
            }
            ensure!(rotations >= 2, "sustained input did not rotate");
            ensure!(store.next_pending(&input, now_epoch_i64())?.is_none());
            println!(
                "input: {} payload bytes, {rotations} rotations",
                MESSAGES * TEXT_BYTES as i64
            );

            let output = self.binding("output")?;
            let accepted = Cell::new(0);
            let mut stalled = false;
            let end = self.exec_route(adapter, owner, &output, || {
                if !stalled {
                    // Stop the host dispatcher after the handshake while the
                    // live guest attempts its first >pipe-sized send.
                    std::thread::sleep(Duration::from_millis(500));
                    stalled = true;
                }
                let count = inbound_count(store, &output).expect("read output inbox count");
                accepted.set(count);
                count == MESSAGES
            })?;
            ensure!(end == Exit::Stopped && accepted.get() == MESSAGES);
            println!(
                "output: {} payload bytes in one exec, including a 500 ms host read stall",
                MESSAGES * TEXT_BYTES as i64
            );

            Ok(())
        })
    }

    fn interrupt(&self) -> Result<()> {
        self.with_host(|adapter, owner| {
            let replay = self.binding("replay")?;
            let (process, _, epoch) = self.await_receipt(adapter, owner, &replay)?;
            drop(process);
            ensure!(inbound_count(owner.store(), &replay)? == 1);
            fs::write(self.root.join("lost-receipt-epoch"), epoch.to_string())?;
            eprintln!(
                "disconnect: committed one event; exited with the database and owner still open"
            );
            // Do not drop the SQLite connection or owner before the runner
            // starts the separate restart phase. The owned OpenShell CLI was
            // already reaped above.
            std::process::exit(75);
        })
    }

    fn restart(&self) -> Result<()> {
        self.with_host(|adapter, owner| {
            let replay = self.binding("replay")?;
            ensure!(inbound_count(owner.store(), &replay)? == 1);
            let (mut process, peer, epoch) = self.await_receipt(adapter, owner, &replay)?;
            let interrupted: i64 =
                fs::read_to_string(self.root.join("lost-receipt-epoch"))?.parse()?;
            ensure!(
                epoch > interrupted,
                "fixture did not take over the old epoch"
            );
            // Stop only after the replay queued its receipt. The normal writer
            // sends that receipt out ahead of Close.
            let end = drive_host(&mut process, peer, || true, stage_guest_message)?;
            ensure!(end == Exit::Stopped && inbound_count(owner.store(), &replay)? == 1);
            println!("restart: one inbox row after replay, awaiting guest receipt verification");
            Ok(())
        })
    }

    fn exec_route(
        &self,
        adapter: &Adapter,
        owner: &Owner<'_, '_>,
        binding: &Binding,
        stop: impl FnMut() -> bool,
    ) -> Result<Exit> {
        let manifest = format!("{REMOTE}/{}.json", binding.worker.worker);
        super::exec(
            owner,
            adapter,
            ExecTarget {
                sandbox_name: &self.sandbox,
                guest_hcom: &format!("{REMOTE}/hcom"),
                guest_manifest: &manifest,
            },
            binding,
            stop,
            stage_guest_message,
        )
    }

    fn await_receipt<'a, 'db>(
        &self,
        adapter: &Adapter,
        owner: &'a Owner<'_, 'db>,
        binding: &'a Binding,
    ) -> Result<(Process, Peer<'a, 'db>, i64)> {
        let epoch = owner.allocate_connection_epoch()?;
        let mut peer = Peer::new(
            owner.store(),
            binding,
            epoch,
            Receiver::Host,
            Some(adapter.measured_stdin_bytes()),
            Limits::default(),
        )?;
        let command = adapter.managed_exec(
            &self.sandbox,
            &format!("{REMOTE}/hcom"),
            &[
                GUEST_COMMAND.into(),
                format!("{REMOTE}/replay.json").into(),
                epoch.to_string().into(),
            ],
        )?;
        let mut process = Process::spawn(command)?;
        peer.start_clock();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            ensure!(
                Instant::now() < deadline,
                "guest did not send the fixture message"
            );
            peer.check()?;
            process.read_diagnostics()?;
            peer.read_step(&mut process.stdout, &mut stage_guest_message)?;
            if peer.receipt_pending() {
                // Return after commit but before another write to stdin.
                return Ok((process, peer, epoch));
            }
            ensure!(!peer.input_eof, "fixture exec ended before durable import");
            peer.write_step(process.stdin.as_mut().context("missing fixture stdin")?)?;
            std::thread::sleep(POLL_INTERVAL);
        }
    }
}

fn payload(target: &str, character: char) -> MessagePayload {
    MessagePayload {
        target: target.into(),
        target_generation: None,
        thread: None,
        reply_to: None,
        intent: Intent::Inform,
        text: std::iter::repeat_n(character, TEXT_BYTES).collect(),
    }
}

fn inbound_count(store: &Store<'_>, binding: &Binding) -> Result<i64> {
    Ok(store.connection()?.query_row(
        "SELECT count(*) FROM sandbox_inbox WHERE worker = ? AND generation = ? AND incarnation = ?",
        params![binding.worker.worker, binding.worker.generation, binding.worker.incarnation],
        |row| row.get(0),
    )?)
}
