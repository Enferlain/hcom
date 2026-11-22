use super::*;
use crate::sandbox::directory::{Access, HostIdentity, Placement};
use crate::sandbox::protocol::{
    Frame, FrameBody, Intent, MAX_ENCODED_FRAME, MessagePayload, Origin, Receiver, VERSION,
    write_frame,
};
use crate::sandbox::store::Import;
use crate::tool::Tool;

fn message(origin: Origin, sequence: i64, target: &str) -> Message {
    Message::new(
        MessageId { origin, sequence },
        MessagePayload {
            target: target.into(),
            target_generation: None,
            sender_routing_generation: None,
            thread: Some("thread".into()),
            reply_to: None,
            intent: Intent::Request,
            text: "hello".into(),
        },
    )
    .unwrap()
}

fn addressed(mut message: Message, generation: &str) -> Message {
    message.payload.target_generation = Some(generation.into());
    Message::new(message.id, message.payload).unwrap()
}

fn event(db: &HcomDb, id: i64) -> Value {
    let json: String = db
        .conn()
        .query_row(
            "SELECT data FROM events WHERE id = ? AND type = 'message'",
            [id],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&json).unwrap()
}

#[test]
fn local_guest_inbox_maps_recipient_and_publishes_only_after_commit() {
    let root = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&root.path().join("guest.db")).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO instances (name, created_at, last_event_id)
         VALUES ('guest', 1, 3), ('unrelated', 2, 7);",
        )
        .unwrap();
    let store = Store::initialize(&db).unwrap();
    let binding = Binding {
        team: "team".into(),
        sandbox: "sandbox".into(),
        worker: Origin {
            worker: "guest.realm".into(),
            generation: "one".into(),
            incarnation: store.incarnation().into(),
        },
    };
    let sender = Origin {
        worker: "host".into(),
        generation: "one".into(),
        incarnation: uuid::Uuid::new_v4().to_string(),
    };
    let input = addressed(
        message(sender, 1, &binding.worker.worker),
        &binding.worker.generation,
    );
    let local_worker = LocalWorker {
        name: "guest".into(),
        created_at: 1.0,
    };
    let mut inbox = GuestInbox::new(&db, &binding, &local_worker).unwrap();
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    db.upsert_notify_endpoint("guest", "pty", listener.local_addr().unwrap().port())
        .unwrap();

    db.conn()
        .execute_batch(
            "CREATE TEMP TRIGGER reject_once BEFORE INSERT ON sandbox_inbox
         BEGIN SELECT RAISE(ABORT, 'injected failure'); END;",
        )
        .unwrap();
    let mut rollback_peer = super::super::peer::Peer::new(
        &store,
        &binding,
        1,
        Receiver::Guest,
        None,
        Default::default(),
        None,
    )
    .unwrap();
    let failure = feed(&mut rollback_peer, &binding, 1, &input, &mut inbox);
    assert!(failure.is_err());
    assert_eq!(db.get_last_event_id(), 0);
    assert!(db.get_thread_members("thread").is_empty());
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    db.conn().execute_batch("DROP TRIGGER reject_once").unwrap();

    let mut peer = super::super::peer::Peer::new(
        &store,
        &binding,
        2,
        Receiver::Guest,
        None,
        Default::default(),
        None,
    )
    .unwrap();
    feed(&mut peer, &binding, 2, &input, &mut inbox).unwrap();
    listener
        .accept()
        .expect("committed event wakes the local guest");
    let first = db.get_last_event_id();
    assert_eq!(event(&db, first)["delivered_to"], json!(["guest"]));
    assert_eq!(event(&db, first)["_sandbox"]["id"], json!(input.id));
    assert!(db.get_thread_members("thread").contains(&"guest".into()));

    let mut reply = message(input.id.origin.clone(), 2, &binding.worker.worker);
    reply.payload.reply_to = Some(input.id.clone());
    reply.payload.intent = Intent::Ack;
    let reply = Message::new(reply.id, reply.payload).unwrap();
    let Imported::New(id) = apply(&store, Receiver::Guest, &binding, &reply, &mut inbox) else {
        panic!("expected new reply")
    };
    assert_eq!(event(&db, id)["reply_to_local"], first);
    assert!(matches!(
        apply(&store, Receiver::Guest, &binding, &reply, &mut inbox),
        Imported::Replay
    ));
    let mut next = message(input.id.origin.clone(), 3, &binding.worker.worker);
    next.payload.reply_to = Some(reply.id.clone());
    next.payload.intent = Intent::Ack;
    let next = Message::new(next.id, next.payload).unwrap();
    assert!(
        store
            .import(Receiver::Guest, &binding, &next, now_epoch_i64(), |tx| {
                inbox.insert(tx, &next)
            })
            .is_err()
    );
    assert_eq!(
        db.get_instance_full("unrelated")
            .unwrap()
            .unwrap()
            .last_event_id,
        7
    );
    db.conn()
        .execute(
            "UPDATE instances SET created_at = 3 WHERE name = 'guest'",
            [],
        )
        .unwrap();
    let stale = message(reply.id.origin, 3, &binding.worker.worker);
    assert!(
        store
            .import(Receiver::Guest, &binding, &stale, now_epoch_i64(), |tx| {
                inbox.insert(tx, &stale)
            })
            .is_err()
    );
}

fn feed(
    peer: &mut super::super::peer::Peer<'_, '_>,
    binding: &Binding,
    epoch: i64,
    message: &Message,
    inbox: &mut GuestInbox<'_>,
) -> Result<()> {
    let mut bytes = vec![];
    for body in [
        FrameBody::Hello {
            max_frame_bytes: MAX_ENCODED_FRAME,
            directory: None,
        },
        FrameBody::Deliver(message.clone()),
    ] {
        write_frame(
            &mut bytes,
            &Frame {
                version: VERSION,
                epoch,
                binding: binding.clone(),
                body,
            },
        )?;
    }
    let mut reader = bytes.as_slice();
    while !reader.is_empty() {
        peer.read_step(&mut reader, inbox)?;
    }
    Ok(())
}

enum Imported {
    New(i64),
    Replay,
}

fn apply(
    store: &Store<'_>,
    receiver: Receiver,
    binding: &Binding,
    message: &Message,
    inbox: &mut impl Inbound,
) -> Imported {
    match store
        .import(receiver, binding, message, now_epoch_i64(), |tx| {
            inbox.insert(tx, message)
        })
        .unwrap()
    {
        Import::Inserted(event) => {
            inbox.committed(event);
            Imported::New(event)
        }
        Import::Replay(_) => Imported::Replay,
        Import::Rejected => panic!("fixture message was unexpectedly rejected"),
    }
}

#[test]
fn host_inbox_delivers_to_enrolled_local_or_forwards_to_same_team() {
    let root = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO instances (name, created_at) VALUES ('host', 1), ('outsider', 2), ('member', 3);",
        )
        .unwrap();
    let identity = crate::identity::resolve_from_name(&db, "host").unwrap();
    let member = HostIdentity::capture(&db, &identity).unwrap();
    let store = Store::initialize(&db).unwrap();
    let team = store.create_team(Some(&member)).unwrap();
    let enroll_local = || {
        let local = crate::identity::resolve_from_name(&db, "member").unwrap();
        let local = HostIdentity::capture(&db, &local).unwrap();
        store
            .enroll_host(&team.id, Access::Host(&member), &local)
            .unwrap();
    };
    enroll_local();
    let make_worker = |name| {
        store
            .reserve_worker(
                &team.id,
                Access::Host(&member),
                name,
                Tool::Claude,
                Placement {
                    gateway: "gateway",
                    workspace: "default",
                    sandbox: name,
                    incarnation: &uuid::Uuid::new_v4().to_string(),
                },
            )
            .unwrap()
    };
    let source = make_worker("source").binding;
    let destination = make_worker("destination").binding;
    let other_team = store.create_team(None).unwrap();
    let outside = store
        .reserve_worker(
            &other_team.id,
            Access::Controller(other_team.external_controller.as_ref().unwrap()),
            "outside",
            Tool::Codex,
            Placement {
                gateway: "other-gateway",
                workspace: "outside-workspace",
                sandbox: "outside-sandbox",
                incarnation: &uuid::Uuid::new_v4().to_string(),
            },
        )
        .unwrap();
    let snapshot = store.snapshot_directory(&source).unwrap().unwrap();
    assert_eq!(
        snapshot,
        vec![
            TeamPeer {
                name: destination.worker.worker.clone(),
                alias: Some("destination".into()),
                generation: destination.worker.generation.clone(),
            },
            TeamPeer {
                name: "host".into(),
                alias: None,
                generation: host_recipient_generation(db.conn(), "host", 1.0).unwrap(),
            },
            TeamPeer {
                name: "member".into(),
                alias: None,
                generation: host_recipient_generation(db.conn(), "member", 3.0).unwrap(),
            },
        ]
    );
    assert_ne!(
        store.snapshot_directory(&outside.binding).unwrap().unwrap(),
        snapshot
    );
    let mut inbox = HostInbox::new(&db, &store, &source);

    let local = addressed(
        message(source.worker.clone(), 1, "member"),
        &host_recipient_generation(db.conn(), "member", 3.0).unwrap(),
    );
    let Imported::New(id) = apply(&store, Receiver::Host, &source, &local, &mut inbox) else {
        panic!("expected local import")
    };
    assert_eq!(event(&db, id)["mentions"], json!(["member"]));

    let forwarded = addressed(
        message(source.worker.clone(), 2, &destination.worker.worker),
        &destination.worker.generation,
    );
    let Imported::New(id) = apply(&store, Receiver::Host, &source, &forwarded, &mut inbox) else {
        panic!("expected forwarded import")
    };
    assert_eq!(
        event(&db, id)["queued_to"],
        json!([destination.worker.worker])
    );
    assert_eq!(event(&db, id)["delivered_to"], json!([]));
    assert_eq!(
        store.next_pending(&destination, now_epoch_i64()).unwrap(),
        Some(forwarded.clone())
    );

    let import = |inbox: &mut HostInbox<'_, '_>, message: &Message| {
        store.import(Receiver::Host, &source, message, now_epoch_i64(), |tx| {
            inbox.insert(tx, message)
        })
    };
    let before = db.get_last_event_id();
    let unauthorized = addressed(
        message(source.worker.clone(), 3, "outsider"),
        &host_recipient_generation(db.conn(), "outsider", 2.0).unwrap(),
    );
    assert_eq!(import(&mut inbox, &unauthorized).unwrap(), Import::Rejected);
    let unbound = message(source.worker.clone(), 4, "member");
    assert!(
        import(&mut inbox, &unbound)
            .unwrap_err()
            .to_string()
            .contains("requires a target generation")
    );

    db.conn()
        .execute(
            "UPDATE instances SET created_at = 4 WHERE name = 'member'",
            [],
        )
        .unwrap();
    enroll_local();
    let stale_local = addressed(
        message(source.worker.clone(), 4, "member"),
        &host_recipient_generation(db.conn(), "member", 3.0).unwrap(),
    );
    assert_eq!(import(&mut inbox, &stale_local).unwrap(), Import::Rejected);

    store
        .retire_worker(Access::Host(&member), &destination)
        .unwrap();
    let replacement = make_worker("destination").binding;
    assert_eq!(replacement.worker.worker, destination.worker.worker);
    assert_ne!(replacement.worker.generation, destination.worker.generation);
    let stale_guest = addressed(
        message(source.worker.clone(), 5, &destination.worker.worker),
        &destination.worker.generation,
    );
    assert_eq!(import(&mut inbox, &stale_guest).unwrap(), Import::Rejected);
    assert_eq!(db.get_last_event_id(), before);
    assert!(
        store
            .next_pending(&replacement, now_epoch_i64())
            .unwrap()
            .is_none()
    );
}

#[test]
fn guest_handshake_replaces_its_persisted_directory_on_reconnect() {
    use super::super::peer::Peer;

    let root = tempfile::tempdir().unwrap();
    let guest_db = HcomDb::open_at(&root.path().join("guest.db")).unwrap();
    guest_db
        .conn()
        .execute(
            "INSERT INTO instances (name, created_at) VALUES ('guest', 1)",
            [],
        )
        .unwrap();
    let guest_store = Store::initialize(&guest_db).unwrap();
    let binding = Binding {
        team: "team".into(),
        sandbox: "sandbox".into(),
        worker: Origin {
            worker: "guest.realm".into(),
            generation: "one".into(),
            incarnation: guest_store.incarnation().into(),
        },
    };
    let host_db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    let host_store = Store::initialize(&host_db).unwrap();
    let local = LocalWorker {
        name: "guest".into(),
        created_at: 1.0,
    };
    let mut inbox = GuestInbox::new(&guest_db, &binding, &local).unwrap();
    let peer = TeamPeer {
        name: "host".into(),
        alias: None,
        generation: host_recipient_generation(host_db.conn(), "host", 2.0).unwrap(),
    };
    let retired = TeamPeer {
        name: "other.realm".into(),
        alias: Some("other".into()),
        generation: "two".into(),
    };

    let stored = || {
        let raw: String = guest_db
            .conn()
            .query_row(
                "SELECT peers FROM sandbox_guest_directory WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str::<Vec<TeamPeer>>(&raw).unwrap()
    };
    let handshake =
        |inbox: &mut GuestInbox<'_>, epoch, peers: Option<Vec<TeamPeer>>| -> Result<()> {
            let mut host = Peer::new(
                &host_store,
                &binding,
                epoch,
                Receiver::Host,
                None,
                Default::default(),
                peers,
            )?;
            let mut guest = Peer::new(
                &guest_store,
                &binding,
                epoch,
                Receiver::Guest,
                None,
                Default::default(),
                None,
            )?;
            let mut hello = Vec::new();
            while host.write_step(&mut hello)? {}
            let mut input = hello.as_slice();
            while !input.is_empty() {
                guest.read_step(&mut input, inbox)?;
            }
            ensure!(
                guest.is_ready(),
                "guest did not finish its directory handshake"
            );
            let mut ack = Vec::new();
            while guest.write_step(&mut ack)? {}
            let mut input = ack.as_slice();
            while !input.is_empty() {
                host.read_step(&mut input, &mut super::super::stage_guest_message)?;
            }
            assert!(host.is_ready());
            Ok(())
        };
    handshake(&mut inbox, 1, Some(vec![peer.clone(), retired.clone()])).unwrap();
    assert_eq!(stored(), vec![peer.clone(), retired]);
    handshake(&mut inbox, 2, Some(vec![peer.clone()])).unwrap();
    assert_eq!(stored(), vec![peer.clone()]);
    handshake(&mut inbox, 3, None).unwrap();
    assert!(stored().is_empty());
    handshake(&mut inbox, 4, Some(vec![peer.clone()])).unwrap();

    guest_db
        .conn()
        .execute(
            "UPDATE instances SET created_at = 2 WHERE name = 'guest'",
            [],
        )
        .unwrap();
    let local_replacement = LocalWorker {
        name: "guest".into(),
        created_at: 2.0,
    };
    let mut replacement = GuestInbox::new(&guest_db, &binding, &local_replacement).unwrap();
    assert!(handshake(&mut replacement, 5, None).is_err());
    assert_eq!(stored(), vec![peer]);
}

#[test]
fn a_lost_rejection_receipt_replays_and_releases_the_next_guest_message() {
    use super::super::peer::Peer;
    use crate::sandbox::store::Delivery;

    fn exchange(from: &mut Peer<'_, '_>, to: &mut Peer<'_, '_>, inbox: &mut impl Inbound) {
        let mut bytes = Vec::new();
        while from.write_step(&mut bytes).unwrap() {}
        let mut input = bytes.as_slice();
        while !input.is_empty() {
            to.read_step(&mut input, inbox).unwrap();
        }
    }

    let root = tempfile::tempdir().unwrap();
    let guest_db = HcomDb::open_at(&root.path().join("guest.db")).unwrap();
    let host_db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    let guest_store = Store::initialize(&guest_db).unwrap();
    let host_store = Store::initialize(&host_db).unwrap();
    host_db
        .conn()
        .execute(
            "INSERT INTO instances (name, created_at) VALUES ('host', 1), ('owner', 2)",
            [],
        )
        .unwrap();
    let host = crate::identity::resolve_from_name(&host_db, "host").unwrap();
    let host = HostIdentity::capture(&host_db, &host).unwrap();
    let owner = crate::identity::resolve_from_name(&host_db, "owner").unwrap();
    let owner = HostIdentity::capture(&host_db, &owner).unwrap();
    let team = host_store.create_team(Some(&owner)).unwrap();
    let worker = host_store
        .reserve_worker(
            &team.id,
            Access::Host(&owner),
            "worker",
            Tool::Claude,
            Placement {
                gateway: "gateway",
                workspace: "default",
                sandbox: "sandbox",
                incarnation: guest_store.incarnation(),
            },
        )
        .unwrap();
    let binding = &worker.binding;
    let now = now_epoch_i64();
    let queue = |target: &str, created_at| {
        let generation = host_recipient_generation(host_db.conn(), target, created_at).unwrap();
        let payload = addressed(message(binding.worker.clone(), 1, target), &generation).payload;
        guest_store
            .enqueue(
                Receiver::Host,
                binding,
                binding.worker.clone(),
                payload,
                now,
                super::super::stage_guest_message,
            )
            .unwrap()
    };
    let expired = queue("owner", 2.0);
    guest_db
        .conn()
        .execute(
            "UPDATE sandbox_outbox SET expires_at = ?1 WHERE sequence = ?2",
            params![now - 1, expired.id.sequence],
        )
        .unwrap();
    let rejected = queue("host", 1.0);
    let valid = queue("owner", 2.0);
    let mut host_inbox = HostInbox::new(&host_db, &host_store, binding);
    let mut guest_inbox = super::super::stage_guest_message;
    let connect = |epoch| {
        (
            Peer::new(
                &host_store,
                binding,
                epoch,
                Receiver::Host,
                None,
                Default::default(),
                None,
            )
            .unwrap(),
            Peer::new(
                &guest_store,
                binding,
                epoch,
                Receiver::Guest,
                None,
                Default::default(),
                None,
            )
            .unwrap(),
        )
    };

    {
        let (mut host, mut guest) = connect(1);
        exchange(&mut host, &mut guest, &mut guest_inbox);
        exchange(&mut guest, &mut host, &mut host_inbox);
        assert!(guest.poll_outbox().unwrap());
        exchange(&mut guest, &mut host, &mut host_inbox);
        assert!(host.receipt_pending());
        assert_eq!(host_db.get_last_event_id(), 0);
        // Drop both peers after the durable rejection and before its receipt.
    }
    assert_eq!(
        guest_store.delivery(&rejected.id, now).unwrap(),
        Some(Delivery::Pending)
    );
    assert_eq!(
        guest_store.delivery(&expired.id, now - 2).unwrap(),
        Some(Delivery::Expired)
    );
    assert_eq!(
        guest_store.next_pending(binding, now - 2).unwrap(),
        Some(rejected.clone())
    );
    assert!(
        guest_store
            .reject(binding, &rejected.id, &[0; 32], now)
            .is_err()
    );
    host_store
        .enroll_host(&team.id, Access::Host(&owner), &host)
        .unwrap();
    let (mut host, mut guest) = connect(2);
    exchange(&mut host, &mut guest, &mut guest_inbox);
    exchange(&mut guest, &mut host, &mut host_inbox);
    assert!(guest.poll_outbox().unwrap());
    exchange(&mut guest, &mut host, &mut host_inbox);
    exchange(&mut host, &mut guest, &mut guest_inbox);
    assert_eq!(
        guest_store.delivery(&rejected.id, now).unwrap(),
        Some(Delivery::Rejected)
    );
    assert!(
        guest_store
            .acknowledge(binding, &rejected.id, &rejected.digest, now)
            .is_err()
    );
    assert!(guest.poll_outbox().unwrap());
    exchange(&mut guest, &mut host, &mut host_inbox);
    exchange(&mut host, &mut guest, &mut guest_inbox);
    assert_eq!(
        guest_store.delivery(&valid.id, now).unwrap(),
        Some(Delivery::Accepted)
    );
    assert_eq!(
        event(&host_db, host_db.get_last_event_id())["delivered_to"],
        json!(["owner"])
    );
    let messages: i64 = host_db
        .conn()
        .query_row(
            "SELECT count(*) FROM events WHERE type='message'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(messages, 1);
}

#[test]
fn ordinary_guest_send_uses_the_directory_and_replies_to_the_bound_host() {
    use clap::Parser;

    let root = tempfile::tempdir().unwrap();
    let guest_db = HcomDb::open_at(&root.path().join("guest.db")).unwrap();
    let host_db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    guest_db
        .conn()
        .execute(
            "INSERT INTO instances (name, created_at) VALUES ('host', 1)",
            [],
        )
        .unwrap();
    host_db
        .conn()
        .execute_batch("INSERT INTO instances (name, created_at) VALUES ('host', 2), ('other', 3);")
        .unwrap();
    let guest_store = Store::initialize(&guest_db).unwrap();
    let host_store = Store::initialize(&host_db).unwrap();
    let host = crate::identity::resolve_from_name(&host_db, "host").unwrap();
    let host_member = HostIdentity::capture(&host_db, &host).unwrap();
    let other = crate::identity::resolve_from_name(&host_db, "other").unwrap();
    let owner = HostIdentity::capture(&host_db, &other).unwrap();
    let team = host_store.create_team(Some(&owner)).unwrap();
    host_store
        .enroll_host(&team.id, Access::Host(&owner), &host_member)
        .unwrap();
    let worker = host_store
        .reserve_worker(
            &team.id,
            Access::Host(&owner),
            "guest",
            Tool::Claude,
            Placement {
                gateway: "gateway",
                workspace: "default",
                sandbox: "guest",
                incarnation: guest_store.incarnation(),
            },
        )
        .unwrap();
    let binding = &worker.binding;
    let sibling = host_store
        .reserve_worker(
            &team.id,
            Access::Host(&owner),
            "sibling",
            Tool::Codex,
            Placement {
                gateway: "gateway",
                workspace: "default",
                sandbox: "sibling",
                incarnation: &uuid::Uuid::new_v4().to_string(),
            },
        )
        .unwrap();
    let collision = host_store
        .reserve_worker(
            &team.id,
            Access::Host(&owner),
            "host",
            Tool::Codex,
            Placement {
                gateway: "gateway",
                workspace: "default",
                sandbox: "alias-collision",
                incarnation: &uuid::Uuid::new_v4().to_string(),
            },
        )
        .unwrap();
    let local = LocalWorker {
        name: "host".into(),
        created_at: 1.0,
    };
    let mut guest_inbox = GuestInbox::new(&guest_db, binding, &local).unwrap();
    let snapshot = host_store.snapshot_directory(binding).unwrap().unwrap();
    let attached = guest_store.attach_bridge(binding, 1).unwrap();
    attached
        .write(|tx| guest_inbox.replace_directory(tx, Some(&snapshot)))
        .unwrap();

    let request = crate::sandbox::outgoing::queue(
        &host_db,
        &host,
        &worker,
        "test request",
        &crate::messages::MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Request),
            thread: Some("work".into()),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let now = now_epoch_i64();
    let work = host_store.next_pending(binding, now).unwrap().unwrap();
    let Imported::New(request_event) = apply(
        &guest_store,
        Receiver::Guest,
        binding,
        &work,
        &mut guest_inbox,
    ) else {
        panic!("expected guest inbox request");
    };
    let ctx = crate::shared::CommandContext {
        explicit_name: None,
        identity: Some(crate::identity::resolve_from_name(&guest_db, "host").unwrap()),
        go: true,
        identity_warning: None,
    };
    let send = |argv: &[&str]| {
        let mut args = crate::commands::send::SendArgs::try_parse_from(argv).unwrap();
        args.had_separator = true;
        crate::commands::send::cmd_send(&guest_db, &args, Some(&ctx))
    };
    let reference = request_event.to_string();
    assert_eq!(send(&["send", "--json", "@missing", "--", "wrong"]), 1);
    assert_eq!(
        send(&[
            "send",
            "--json",
            "--reply-to",
            &reference,
            "@other",
            "--",
            "wrong"
        ]),
        1
    );
    assert_eq!(guest_db.get_last_event_id(), request_event);
    assert_eq!(
        send(&[
            "send",
            "--json",
            "--intent",
            "ack",
            "--reply-to",
            &reference,
            "@host",
            "--",
            "received"
        ]),
        0
    );
    let reply = guest_store.next_pending(binding, now).unwrap().unwrap();
    assert_eq!(reply.id.origin, binding.worker);
    assert_eq!(reply.payload.reply_to, Some(work.id.clone()));
    assert_eq!(reply.payload.thread.as_deref(), Some("work"));

    let mut host_inbox = HostInbox::new(&host_db, &host_store, binding);
    let mut wrong = reply.payload.clone();
    wrong.target = "other".into();
    wrong.target_generation =
        Some(host_recipient_generation(host_db.conn(), "other", 3.0).unwrap());
    let wrong = Message::new(reply.id.clone(), wrong).unwrap();
    let rejected = host_store
        .import(Receiver::Host, binding, &wrong, now, |tx| {
            host_inbox.insert(tx, &wrong)
        })
        .unwrap_err();
    assert!(rejected.to_string().contains("between these participants"));
    let Imported::New(reply_event) = apply(
        &host_store,
        Receiver::Host,
        binding,
        &reply,
        &mut host_inbox,
    ) else {
        panic!("expected host inbox reply");
    };
    let data = event(&host_db, reply_event);
    assert_eq!(data["delivered_to"], json!(["host"]));
    assert_eq!(data["reply_to_local"], request.event_id);
    assert!(
        host_store
            .next_pending(&collision.binding, now)
            .unwrap()
            .is_none()
    );

    assert_eq!(send(&["send", "--json", "@sibling", "--", "hello peer"]), 0);
    assert_eq!(
        guest_store.next_pending(binding, now).unwrap(),
        Some(reply.clone())
    );
    guest_store
        .acknowledge(binding, &reply.id, &reply.digest, now)
        .unwrap();
    let direct = guest_store.next_pending(binding, now).unwrap().unwrap();
    assert_eq!(direct.payload.target, sibling.binding.worker.worker);
    let Imported::New(forwarded) = apply(
        &host_store,
        Receiver::Host,
        binding,
        &direct,
        &mut host_inbox,
    ) else {
        panic!("expected sibling forwarding");
    };
    assert_eq!(
        event(&host_db, forwarded)["queued_to"],
        json!([direct.payload.target])
    );
    assert_eq!(
        host_store.next_pending(&sibling.binding, now).unwrap(),
        Some(direct.clone())
    );
    guest_db
        .conn()
        .execute(
            "UPDATE sandbox_outbox SET expires_at = ?1 WHERE sequence = ?2",
            params![now - 1, direct.id.sequence],
        )
        .unwrap();
    assert_eq!(send(&["send", "--json", "@host", "--", "after expiry"]), 0);
    assert_eq!(
        guest_store.delivery(&direct.id, now).unwrap(),
        Some(crate::sandbox::store::Delivery::Expired)
    );
    assert_eq!(
        guest_store
            .next_pending(binding, now)
            .unwrap()
            .unwrap()
            .payload
            .text,
        "after expiry"
    );
    host_db
        .conn()
        .execute(
            "UPDATE instances SET created_at = 4 WHERE name = 'host'",
            [],
        )
        .unwrap();
    let replacement = crate::identity::resolve_from_name(&host_db, "host").unwrap();
    let replacement = HostIdentity::capture(&host_db, &replacement).unwrap();
    host_store
        .enroll_host(&team.id, Access::Host(&owner), &replacement)
        .unwrap();
    let snapshot = host_store.snapshot_directory(binding).unwrap().unwrap();
    let attached = guest_store.attach_bridge(binding, 2).unwrap();
    attached
        .write(|tx| guest_inbox.replace_directory(tx, Some(&snapshot)))
        .unwrap();
    let before = guest_db.get_last_event_id();
    assert_eq!(
        send(&[
            "send",
            "--json",
            "--reply-to",
            &reference,
            "@host",
            "--",
            "stale reply"
        ]),
        1
    );
    assert_eq!(guest_db.get_last_event_id(), before);
}
