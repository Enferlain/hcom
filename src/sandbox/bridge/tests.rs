use std::fs;
use std::time::Instant;

use crate::sandbox::openshell::ROTATION_BYTES;
use crate::sandbox::protocol::{
    Frame, FrameBody, Intent, MAX_ENCODED_FRAME, MessageId, MessagePayload, Origin, VERSION,
    write_frame,
};
use crate::sandbox::store::Delivery;
use crate::shared::time::now_epoch_i64;

use super::*;

struct Fixture {
    dir: tempfile::TempDir,
    db: HcomDb,
    binding: Binding,
    origin: Origin,
}

fn origin(worker: &str, incarnation: &str) -> Origin {
    Origin {
        worker: worker.into(),
        generation: "gen".into(),
        incarnation: incarnation.into(),
    }
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&dir.path().join("hcom.db")).unwrap();
        let store = Store::initialize(&db).unwrap();
        let origin = origin("host", store.incarnation());
        Self {
            dir,
            db,
            binding: Binding {
                team: "team".into(),
                sandbox: "guest-vm".into(),
                worker: self::origin("guest", &uuid::Uuid::new_v4().to_string()),
            },
            origin,
        }
    }

    fn enqueue(&self, text: String) -> Message {
        self.store()
            .enqueue(
                Receiver::Guest,
                &self.binding,
                self.origin.clone(),
                payload("guest", text),
                now_epoch_i64(),
                stage_guest_message,
            )
            .unwrap()
    }

    fn store(&self) -> Store<'_> {
        Store::initialize(&self.db).unwrap()
    }

    fn host(&self, epoch: i64, script: &str, stop: impl FnMut() -> bool) -> Result<Exit> {
        let store = self.store();
        let peer = Peer::new(
            &store,
            &self.binding,
            epoch,
            Receiver::Host,
            Some(4 * 1024 * 1024),
            Limits {
                progress: Duration::from_secs(2),
                drain: Duration::from_millis(350),
                ..Limits::default()
            },
            None,
        )?;
        let mut shell = Command::new("/bin/sh");
        shell
            .args(["-eu", "-c", script])
            .current_dir(self.dir.path());
        run_host(shell, peer, stop, stage_guest_message, None)
    }

    fn send(&self, text: String) -> FrameBody {
        let id = MessageId {
            origin: self.binding.worker.clone(),
            sequence: 1,
        };
        FrameBody::Send(Message::new(id, payload("host", text)).unwrap())
    }

    fn frame(&self, name: &str, epoch: i64, body: FrameBody) -> usize {
        let mut out = Vec::new();
        write_frame(
            &mut out,
            &Frame {
                version: VERSION,
                epoch,
                binding: self.binding.clone(),
                body,
            },
        )
        .unwrap();
        fs::write(self.dir.path().join(name), &out).unwrap();
        out.len()
    }

    fn hello(&self, epoch: i64) -> usize {
        self.frame(
            "hello",
            epoch,
            FrameBody::Hello {
                max_frame_bytes: MAX_ENCODED_FRAME,
                directory: None,
            },
        )
    }

    fn delivery(&self, message: &Message) -> Option<Delivery> {
        self.store().delivery(&message.id, now_epoch_i64()).unwrap()
    }
}

fn payload(target: &str, text: String) -> MessagePayload {
    MessagePayload {
        target: target.into(),
        target_generation: None,
        text,
        sender_routing_generation: None,
        thread: None,
        reply_to: None,
        intent: Intent::Inform,
    }
}

fn receipt(message: &Message) -> FrameBody {
    FrameBody::Accepted {
        id: message.id.clone(),
        digest: message.digest,
    }
}

#[test]
fn duplex_split_frames_and_delayed_stdout_survive_half_close() {
    let fixture = Fixture::new();
    let host_msg = fixture.enqueue("h".repeat(128 * 1024));
    let guests = fixture.send("g".repeat(128 * 1024));
    let FrameBody::Send(guest_msg) = &guests else {
        unreachable!();
    };
    let hello_len = fixture.hello(1);
    fixture.frame("send", 1, guests.clone());
    fixture.frame("accepted", 1, receipt(&host_msg));
    let deliver_len = fixture.frame("deliver", 1, FrameBody::Deliver(host_msg.clone()));
    let receipt_len = fixture.frame("receipt", 1, receipt(guest_msg));
    fixture.frame("close", 1, FrameBody::Close);
    let consume = hello_len + deliver_len + receipt_len;
    let script = format!(
        "head -c 2 hello; sleep .03; tail -c +3 hello
         cat send
         head -c {consume} > host_frames
         sleep .03; cat accepted
         cat > host_close
         sleep .03; cat close"
    );
    let store = fixture.store();
    let exit = fixture.host(1, &script, || {
        store.delivery(&host_msg.id, now_epoch_i64()).unwrap() == Some(Delivery::Accepted)
    });
    assert_eq!(exit.unwrap(), Exit::Stopped);
    let read = |name| fs::read(fixture.dir.path().join(name)).unwrap();
    let host_frames = read("host_frames");
    let mut expected = read("hello");
    expected.extend(read("deliver"));
    expected.extend(read("receipt"));
    assert_eq!(host_frames, expected);
    assert_eq!(read("host_close"), read("close"));
}

#[test]
fn rotates_before_two_mib_and_replays_same_pending_payload() {
    let fixture = Fixture::new();
    let messages = (0..9)
        .map(|_| fixture.enqueue("x".repeat(250 * 1024)))
        .collect::<Vec<_>>();
    let hello_len = fixture.hello(1);
    let mut script = format!("cat hello; head -c {hello_len} > hello_read\n");
    let mut consumed = hello_len as u64;
    let mut count = 0;
    for (index, msg) in messages.iter().enumerate() {
        let len = fixture.frame("expected", 1, FrameBody::Deliver(msg.clone()));
        if consumed + len as u64 > ROTATION_BYTES {
            break;
        }
        consumed += len as u64;
        count += 1;
        fixture.frame(&format!("ack{index}"), 1, receipt(msg));
        script.push_str(&format!("head -c {len} > payload{index}; cat ack{index}\n"));
    }
    assert_eq!(count, 8);
    fixture.frame("close", 1, FrameBody::Close);
    script.push_str("cat > remainder; cmp close remainder; cat close");
    assert_eq!(fixture.host(1, &script, || false).unwrap(), Exit::Rotate);
    assert_eq!(
        fixture
            .store()
            .next_pending(&fixture.binding, now_epoch_i64())
            .unwrap()
            .unwrap()
            .id,
        messages[count].id
    );

    let total = fixture.hello(2)
        + fixture.frame("replayed", 2, FrameBody::Deliver(messages[count].clone()));
    fixture.frame("accepted", 2, receipt(&messages[count]));
    fixture.frame("close", 2, FrameBody::Close);
    let script = format!(
        "cat hello; head -c {total} > host_frames; cat accepted; cat >/dev/null; cat close"
    );
    assert_eq!(
        fixture
            .host(2, &script, || {
                fixture.delivery(&messages[count]) == Some(Delivery::Accepted)
            })
            .unwrap(),
        Exit::Stopped
    );
    let read = |name| fs::read(fixture.dir.path().join(name)).unwrap();
    let mut expected = read("hello");
    expected.extend(read("replayed"));
    assert_eq!(read("host_frames"), expected);
}

#[test]
fn enrolled_bridge_sends_only_a_challenge_until_guest_proof_matches() {
    let fixture = Fixture::new();
    let store = fixture.store();
    let peer = Peer::new(
        &store,
        &fixture.binding,
        1,
        Receiver::Host,
        Some(4 * 1024 * 1024),
        Limits::default(),
        None,
    )
    .unwrap();
    let mut shell = Command::new("/bin/sh");
    shell.args([
        "-eu",
        "-c",
        "head -c 32 > challenge; printf 'unverified guest stderr' >&2; printf '%032d' 0; cat > extra",
    ]).current_dir(fixture.dir.path());
    let error = run_host(shell, peer, || false, stage_guest_message, Some(&[7; 32]))
        .unwrap_err()
        .to_string();
    assert!(error.contains("startup proof did not match"), "{error}");
    assert!(!error.contains("unverified guest stderr"));
    assert_eq!(
        fs::metadata(fixture.dir.path().join("challenge"))
            .unwrap()
            .len(),
        32
    );
    assert_eq!(
        fs::read(fixture.dir.path().join("extra")).unwrap_or_default(),
        b""
    );
    assert_eq!(fixture.db.get_last_event_id(), 0);
}

#[test]
fn host_polls_an_active_route_while_another_child_is_stalled() {
    let stalled = Fixture::new();
    let mut active = Fixture::new();
    active.binding.sandbox = "active-vm".into();
    active.binding.worker.worker = "active".into();
    let store = stalled.store();
    let peer = |store, binding| {
        Peer::new(
            store,
            binding,
            1,
            Receiver::Host,
            None,
            Limits::default(),
            None,
        )
        .unwrap()
    };
    let mut stalled_peer = peer(&store, &stalled.binding);
    let mut active_peer = peer(&store, &active.binding);
    active.hello(1);
    active.frame("send", 1, active.send("still responsive".into()));
    active.frame("close", 1, FrameBody::Close);

    let spawn = |fixture: &Fixture, script: &str| {
        let mut shell = Command::new("/bin/sh");
        shell
            .args(["-eu", "-c", script])
            .current_dir(fixture.dir.path());
        Process::spawn(shell).unwrap()
    };
    let mut stalled_child = spawn(&stalled, "exec sleep 30");
    let mut active_child = spawn(&active, "cat hello send; cat >/dev/null; cat close");
    stalled_peer.start_clock();
    active_peer.start_clock();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut inbound = stage_guest_message;
    loop {
        let stalled_step = poll_host(
            &mut stalled_child,
            &mut stalled_peer,
            || false,
            &mut inbound,
        )
        .unwrap();
        assert!(!matches!(stalled_step, HostStep::Finished(_)));
        let step = poll_host(
            &mut active_child,
            &mut active_peer,
            || ingress_count(&stalled) == 1,
            &mut inbound,
        )
        .unwrap();
        if let HostStep::Finished(exit) = step {
            assert_eq!(exit, Exit::Stopped);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "active route stalled behind the other child"
        );
        std::thread::sleep(POLL_INTERVAL);
    }
    assert_eq!(ingress_count(&stalled), 1);
    assert!(!stalled_peer.is_ready());
}

#[test]
fn process_failure_or_eof_never_acknowledge_payload() {
    for (tail, error) in [
        ("exit 7", "exec failed"),
        ("exit 0", "disconnected"),
        ("head -c 2 close", "truncated sandbox frame"),
    ] {
        let fixture = Fixture::new();
        let message = fixture.enqueue("pending".into());
        let total =
            fixture.hello(1) + fixture.frame("deliver", 1, FrameBody::Deliver(message.clone()));
        fixture.frame("close", 1, FrameBody::Close);
        let script = format!("cat hello; head -c {total} >/dev/null; {tail}");
        let status = fixture.host(1, &script, || false).unwrap_err().to_string();
        assert!(status.contains(error), "{status}");
        assert_eq!(fixture.delivery(&message), Some(Delivery::Pending));
    }
}

#[test]
fn stalled_drain_is_bounded_and_reaps_its_process_group() {
    let fixture = Fixture::new();
    let message = fixture.enqueue("pending".into());
    let total = fixture.hello(1) + fixture.frame("deliver", 1, FrameBody::Deliver(message.clone()));
    let script =
        format!("cat hello; head -c {total} > received; touch stop; cat >/dev/null; sleep 30");
    let before = Instant::now();
    let error = fixture
        .host(1, &script, || fixture.dir.path().join("stop").exists())
        .unwrap_err();
    assert!(error.to_string().contains("drain timed out"), "{error}");
    assert!(before.elapsed() < Duration::from_secs(4));
    assert_eq!(fixture.delivery(&message), Some(Delivery::Pending));
}

#[test]
fn stalled_child_reader_does_not_block_stdout_import() {
    let fixture = Fixture::new();
    let message = fixture.enqueue("x".repeat(160 * 1024));
    fixture.hello(1);
    fixture.frame("send", 1, fixture.send("y".repeat(120 * 1024)));
    let script = "cat hello; cat send; sleep 30";
    let error = fixture.host(1, script, || false).unwrap_err().to_string();
    assert!(error.contains("timed out"), "{error}");
    assert_eq!(fixture.delivery(&message), Some(Delivery::Pending));
    assert_eq!(ingress_count(&fixture), 1);
}

#[test]
fn exceeding_payload_credit_prevents_a_second_import() {
    let fixture = Fixture::new();
    fixture.enqueue("x".repeat(160 * 1024));
    fixture.hello(1);
    let first = fixture.send("first".into());
    let FrameBody::Send(mut second) = fixture.send("second".into()) else {
        unreachable!();
    };
    second.id.sequence = 2;
    fixture.frame("first", 1, first);
    fixture.frame("second", 1, FrameBody::Send(second));
    let error = fixture
        .host(1, "cat hello first second; sleep 30", || false)
        .unwrap_err();
    assert!(
        error.to_string().contains("exceeded payload credit"),
        "{error}"
    );
    assert_eq!(ingress_count(&fixture), 1);
}

#[test]
fn failing_final_status_after_close_preserves_unacknowledged_payload() {
    let fixture = Fixture::new();
    let message = fixture.enqueue("pending".into());
    let total = fixture.hello(1) + fixture.frame("deliver", 1, FrameBody::Deliver(message.clone()));
    fixture.frame("close", 1, FrameBody::Close);
    let script = format!(
        "cat hello; head -c {total} >/dev/null; touch stop; cat >/dev/null; cat close
         printf 'fixture\\033[31m failure\\n' >&2
         exit 9"
    );
    let error = fixture
        .host(1, &script, || fixture.dir.path().join("stop").exists())
        .unwrap_err();
    assert!(format!("{error:#}").contains("exit status: 9"), "{error:#}");
    assert!(
        error.to_string().contains(r"fixture\u{1b}[31m failure"),
        "{error:#}"
    );
    assert_eq!(fixture.delivery(&message), Some(Delivery::Pending));
}

fn ingress_count(fixture: &Fixture) -> i64 {
    fixture.db.conn().query_row(
        "SELECT count(*) FROM events WHERE type = 'sandbox_bridge_ingress' AND instance = 'host'",
        [],
        |row| row.get(0),
    ).unwrap()
}

#[test]
fn lost_receipt_replay_and_stale_epochs_preserve_one_event() {
    let fixture = Fixture::new();
    let inbound = fixture.send("one event".into());
    fixture.hello(4);
    fixture.frame("send", 4, inbound.clone());
    let script = "cat hello; cat send; sleep .08; exit 8";
    assert!(fixture.host(4, script, || false).is_err());
    assert_eq!(ingress_count(&fixture), 1);

    let store = fixture.store();
    let old = store.attach_bridge(&fixture.binding, 5).unwrap();
    fixture.hello(6);
    fixture.frame("send", 6, inbound.clone());
    assert!(fixture.host(6, script, || false).is_err());
    assert_eq!(ingress_count(&fixture), 1);
    assert!(store.attach_bridge(&fixture.binding, 6).is_err());
    let FrameBody::Send(message) = inbound else {
        unreachable!();
    };
    let err = old
        .import(
            Receiver::Host,
            &fixture.binding,
            &message,
            now_epoch_i64(),
            |_| panic!("stale import"),
        )
        .unwrap_err();
    assert!(err.to_string().contains("stale sandbox connection epoch"));
    let pending = fixture.enqueue("still pending".into());
    let err = old
        .acknowledge(
            &fixture.binding,
            &pending.id,
            &pending.digest,
            now_epoch_i64(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("stale sandbox connection epoch"));
}
