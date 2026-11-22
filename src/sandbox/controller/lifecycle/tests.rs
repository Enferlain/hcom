use super::*;
use crate::sandbox::controller::RunState;
use crate::sandbox::protocol::Intent;

#[cfg(unix)]
struct DisableOnDrop<'a, 'db>(&'a Store<'db>);

#[cfg(unix)]
impl Drop for DisableOnDrop<'_, '_> {
    fn drop(&mut self) {
        let _ = disable(self.0);
    }
}

fn route() -> Binding {
    Binding {
        team: "team".into(),
        sandbox: "sandbox-a".into(),
        worker: Origin {
            worker: "worker-a".into(),
            generation: "generation-a".into(),
            incarnation: uuid::Uuid::new_v4().to_string(),
        },
    }
}

fn fixture_owner(epoch: i64) -> Process {
    let pid = std::process::id();
    let (executable, executable_id) = current_executable().unwrap();
    Process {
        epoch,
        pid,
        executable,
        executable_id,
        process_start: process::identity(pid).unwrap(),
        heartbeat: now_epoch_i64(),
        heartbeat_monotonic: monotonic_seconds().unwrap(),
    }
}

fn send(store: &Store<'_>, route: &Binding, spawn: impl Fn(&Path) -> Result<Child>) -> QueuedSend {
    enqueue_host_with(
        store,
        route,
        Origin {
            worker: "host".into(),
            generation: "host-generation".into(),
            incarnation: store.incarnation().into(),
        },
        MessagePayload {
            target: route.worker.worker.clone(),
            target_generation: None,
            sender_routing_generation: None,
            thread: None,
            reply_to: None,
            intent: Intent::Request,
            text: "fixture".into(),
        },
        |tx, _| {
            tx.execute(
                "INSERT INTO events (timestamp, type, instance, data)
                        VALUES ('fixture', 'sandbox-fixture', 'host', '{}')",
                [],
            )?;
            Ok(tx.last_insert_rowid())
        },
        spawn,
    )
    .unwrap()
}

#[test]
fn failed_or_disabled_start_preserves_queued_message() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let store = Store::initialize(&db).unwrap();
    let route = route();
    let first = send(&store, &route, |_| {
        panic!("disabled controller must not spawn")
    });
    assert!(matches!(first.forwarding, Forwarding::Disabled));
    store.set_enabled(true).unwrap();
    let second = send(&store, &route, |_| {
        let delivery = store.delivery(&first.message.id, now_epoch_i64())?;
        assert_eq!(delivery, Some(crate::sandbox::store::Delivery::Pending));
        assert_eq!(
            store.next_pending(&route, now_epoch_i64())?,
            Some(first.message.clone())
        );
        bail!("fixture spawn error")
    });
    assert!(
        matches!(second.forwarding, Forwarding::Deferred(error) if error.contains("fixture spawn error"))
    );
    assert_eq!(
        store.delivery(&second.message.id, now_epoch_i64()).unwrap(),
        Some(crate::sandbox::store::Delivery::Pending)
    );
}

#[cfg(unix)]
#[test]
fn worker_fixture() {
    let Some(path) = std::env::var_os("HCOM_SANDBOX_LIFECYCLE_TEST_DB") else {
        return;
    };
    let db = HcomDb::open_at(Path::new(&path)).unwrap();
    run_worker_in(&db).unwrap();
}

#[cfg(unix)]
#[test]
fn send_after_crash_lazily_starts_successor_and_replays_pending() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let store = Store::initialize(&db).unwrap();
    let _cleanup = DisableOnDrop(&store);
    let spawn_fixture = |path: &Path| {
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--exact",
                "sandbox::controller::lifecycle::tests::worker_fixture",
            ])
            .env("HCOM_SANDBOX_LIFECYCLE_TEST_DB", path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        Ok(process::spawn_detached(&mut command)?)
    };
    let owner = ensure_running(&store, true, spawn_fixture)
        .unwrap()
        .unwrap();
    let route = route();
    let first = send(&store, &route, |_| panic!("must adopt running controller"));
    assert!(
        matches!(first.forwarding, Forwarding::ControllerReady(ref process) if process.epoch == owner.epoch)
    );
    assert!(process::has_identity(owner.pid, &owner.process_start));
    assert!(process::kill(owner.pid));
    let deadline = Instant::now() + WAIT;
    while !owner_released(store.path()).unwrap() {
        assert!(
            Instant::now() < deadline,
            "crashed fixture retained owner lock"
        );
        thread::sleep(POLL);
    }
    let second = send(&store, &route, spawn_fixture);
    assert!(
        matches!(second.forwarding, Forwarding::ControllerReady(ref process) if process.epoch > owner.epoch)
    );
    assert_eq!(
        store.next_pending(&route, now_epoch_i64()).unwrap(),
        Some(first.message)
    );
    disable(&store).unwrap();
    assert!(status(store.path()).unwrap().owner.is_none());
}

#[test]
fn idle_retirement_distinguishes_retained_runs_and_pending_work() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let store = Store::initialize(&db).unwrap();
    let owner = Owner::try_acquire(&store).unwrap().unwrap();
    let record = fixture_owner(owner.epoch());
    store.set_enabled(true).unwrap();
    assert!(store.claim_controller(&record).unwrap());
    assert_eq!(
        store
            .tick_controller(
                owner.epoch() - 1,
                now_epoch_i64(),
                monotonic_seconds().unwrap(),
                true
            )
            .unwrap(),
        Tick::LostOwnership
    );
    let tick = |now| {
        store
            .tick_controller(owner.epoch(), now, monotonic_seconds().unwrap(), true)
            .unwrap()
    };
    for state in [RunState::Provisioning, RunState::Running, RunState::Unknown] {
        store.set_run_work("run", state, false).unwrap();
        assert_eq!(tick(1), Tick::Work);
    }
    store.set_run_work("run", RunState::Stopped, true).unwrap();
    assert_eq!(tick(1), Tick::Work);
    store
        .set_run_work("run", RunState::Terminal, false)
        .unwrap();
    assert_eq!(
        store
            .tick_controller(
                owner.epoch(),
                now_epoch_i64(),
                monotonic_seconds().unwrap(),
                false
            )
            .unwrap(),
        Tick::Idle
    );
    let queued = send(&store, &route(), |_| panic!("test owner should be adopted"));
    assert!(matches!(queued.forwarding, Forwarding::ControllerReady(_)));
    let now = now_epoch_i64();
    assert_eq!(tick(now), Tick::Work);
    store.set_run_work("run", RunState::Stopped, false).unwrap();
    assert_eq!(tick(now + 3601), Tick::Exit);
    assert!(matches!(
        store.delivery(&queued.message.id, now + 3601).unwrap(),
        Some(crate::sandbox::store::Delivery::AwaitingExpiry)
    ));
    assert!(status(store.path()).unwrap().enabled);
    assert!(status(store.path()).unwrap().owner.is_none());
}

#[test]
fn fresh_monotonic_heartbeat_survives_wall_skew_and_mismatched_binary_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let store = Store::initialize(&db).unwrap();
    let owner = Owner::try_acquire(&store).unwrap().unwrap();
    store.set_enabled(true).unwrap();
    let mut record = fixture_owner(owner.epoch());
    record.heartbeat += 3600;
    record.executable_id.push_str("-different-image");
    assert!(store.claim_controller(&record).unwrap());
    assert!(status(store.path()).unwrap().ready);
    let error = ensure_running(&store, false, |_| {
        panic!("must not spawn beside a different binary")
    })
    .unwrap_err();
    assert!(error.to_string().contains("different executable"));
    record.heartbeat_monotonic = monotonic_seconds().unwrap() + 3600;
    assert!(store.claim_controller(&record).unwrap());
    assert!(!status(store.path()).unwrap().ready);
}

#[test]
fn unhealthy_owner_is_not_replaced_or_signalled_by_start_or_stop() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let store = Store::initialize(&db).unwrap();
    let owner = Owner::try_acquire(&store).unwrap().unwrap();
    store.set_enabled(true).unwrap();
    let mut record = fixture_owner(owner.epoch());
    record.heartbeat_monotonic -= 60;
    assert!(store.claim_controller(&record).unwrap());
    let stale = status(store.path()).unwrap();
    assert!(stale.alive && !stale.ready);
    let error = ensure_running(&store, false, |_| {
        panic!("must not spawn beside a stale owner")
    })
    .unwrap_err();
    assert!(error.to_string().contains("fresh heartbeat"));
    assert!(
        disable(&store)
            .unwrap_err()
            .to_string()
            .contains("has not released ownership")
    );
    let stopped = status(store.path()).unwrap();
    assert!(!stopped.enabled && stopped.alive);
    assert_eq!(stopped.owner.unwrap().epoch, owner.epoch());
}
