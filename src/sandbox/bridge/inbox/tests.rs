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
            conversation: None,
            target: target.into(),
            target_generation: None,
            sender_routing_generation: None,
            broadcast: false,
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
    let queue = |target: &str, generation: &str, text: &str| {
        let mut payload = addressed(message(binding.worker.clone(), 1, target), generation).payload;
        // Use the queue directly to exercise the host boundary independently of guest CLI checks.
        payload.text = text.into();
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
    let owner_generation = host_recipient_generation(host_db.conn(), "owner", 2.0).unwrap();
    let expired = queue("owner", &owner_generation, "expired");
    guest_db
        .conn()
        .execute(
            "UPDATE sandbox_outbox SET expires_at = ?1 WHERE sequence = ?2",
            params![now - 1, expired.id.sequence],
        )
        .unwrap();
    let rejected = queue("owner", &owner_generation, "blocked\u{1b}text");
    let outsider_generation = host_recipient_generation(host_db.conn(), "host", 1.0).unwrap();
    let mut denied = vec![
        queue("host", &outsider_generation, "outside the team"),
        queue("mention", "unassigned", "reserved target"),
        queue(crate::shared::SENDER, "unassigned", "system target"),
    ];
    denied.extend(
        [
            "bel\u{0007}seq",
            "cr\rseq",
            "del\u{007f}seq",
            "c1\u{009b}seq",
            " \n\t ",
        ]
        .map(|text| queue("owner", &owner_generation, text)),
    );
    let literal = "printf '\\n'\n\tselect 1";
    let valid = queue("owner", &owner_generation, literal);
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    host_db
        .upsert_notify_endpoint("owner", "pty", listener.local_addr().unwrap().port())
        .unwrap();
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
    for denied in denied {
        assert!(guest.poll_outbox().unwrap());
        exchange(&mut guest, &mut host, &mut host_inbox);
        exchange(&mut host, &mut guest, &mut guest_inbox);
        assert_eq!(
            guest_store.delivery(&denied.id, now).unwrap(),
            Some(Delivery::Rejected)
        );
    }
    assert_eq!(host_db.get_last_event_id(), 0);
    assert!(host_db.get_thread_members("thread").is_empty());
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert!(guest.poll_outbox().unwrap());
    exchange(&mut guest, &mut host, &mut host_inbox);
    exchange(&mut host, &mut guest, &mut guest_inbox);
    assert_eq!(
        guest_store.delivery(&valid.id, now).unwrap(),
        Some(Delivery::Accepted)
    );
    let delivered = event(&host_db, host_db.get_last_event_id());
    assert_eq!(delivered["delivered_to"], json!(["owner"]));
    assert_eq!(delivered["from"], json!(binding.worker.worker));
    assert_eq!(delivered["sender_kind"], "instance");
    assert_eq!(delivered["text"], literal);
    listener
        .accept()
        .expect("the valid message wakes the owner");
    assert!(host_db.get_unread_messages("host").is_empty());
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

fn broadcast_msg(origin: Origin, sequence: i64, text: &str) -> Message {
    Message::new(
        MessageId { origin, sequence },
        MessagePayload {
            conversation: None,
            target: String::new(),
            target_generation: None,
            sender_routing_generation: None,
            broadcast: true,
            thread: None,
            reply_to: None,
            intent: Intent::Inform,
            text: text.into(),
        },
    )
    .unwrap()
}

fn deliver_all(
    from: &mut super::super::peer::Peer<'_, '_>,
    to: &mut super::super::peer::Peer<'_, '_>,
    inbox: &mut impl Inbound,
) {
    let mut bytes = Vec::new();
    while from.write_step(&mut bytes).unwrap() {}
    let mut input = bytes.as_slice();
    while !input.is_empty() {
        to.read_step(&mut input, inbox).unwrap();
    }
}

fn wake_listener(db: &HcomDb, name: &str, kind: &str) -> std::net::TcpListener {
    let socket = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    socket.set_nonblocking(true).unwrap();
    db.upsert_notify_endpoint(name, kind, socket.local_addr().unwrap().port())
        .unwrap();
    socket
}

fn run_cmd_send(db: &HcomDb, actor: &str, argv: &[&str]) -> i32 {
    use clap::Parser;
    let ctx = crate::shared::CommandContext {
        explicit_name: None,
        identity: Some(crate::identity::resolve_from_name(db, actor).unwrap()),
        go: true,
        identity_warning: None,
    };
    let mut args = crate::commands::send::SendArgs::try_parse_from(argv).unwrap();
    args.had_separator = true;
    crate::commands::send::cmd_send(db, &args, Some(&ctx))
}

struct GuestNode {
    db: HcomDb,
    worker: crate::sandbox::directory::Worker,
    local: LocalWorker,
}

impl GuestNode {
    fn new(
        dir: &std::path::Path,
        file: &str,
        host_store: &Store<'_>,
        team: &str,
        access: Access<'_>,
        alias: &str,
        created_at: f64,
    ) -> Self {
        let db = HcomDb::open_at(&dir.join(file)).unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, last_event_id) VALUES ('guest', ?, 0)",
                [created_at],
            )
            .unwrap();
        let store = Store::initialize(&db).unwrap();
        let worker = host_store
            .reserve_worker(
                team,
                access,
                alias,
                Tool::Claude,
                Placement {
                    gateway: "gateway",
                    workspace: "default",
                    sandbox: alias,
                    incarnation: store.incarnation(),
                },
            )
            .unwrap();
        let local = LocalWorker {
            name: "guest".into(),
            created_at,
        };
        let mut inbox = GuestInbox::new(&db, &worker.binding, &local).unwrap();
        let snapshot = host_store.snapshot_directory(&worker.binding).unwrap();
        store
            .attach_bridge(&worker.binding, 1)
            .unwrap()
            .write(|tx| inbox.replace_directory(tx, snapshot.as_deref()))
            .unwrap();
        Self { db, worker, local }
    }

    fn store(&self) -> Store<'_> {
        Store::initialize(&self.db).unwrap()
    }

    fn inbox(&self) -> GuestInbox<'_> {
        GuestInbox::new(&self.db, &self.worker.binding, &self.local).unwrap()
    }

    fn refresh_directory(&self, host_store: &Store<'_>, epoch: i64) {
        let mut inbox = self.inbox();
        let snapshot = host_store.snapshot_directory(&self.worker.binding).unwrap();
        self.store()
            .attach_bridge(&self.worker.binding, epoch)
            .unwrap()
            .write(|tx| inbox.replace_directory(tx, snapshot.as_deref()))
            .unwrap();
    }
}

fn bridge_step(
    host_store: &Store<'_>,
    guest: &GuestNode,
    epoch: i64,
    host_inbox: &mut HostInbox<'_, '_>,
    drop_after_first: bool,
    from_host: bool,
) {
    use super::super::peer::Peer;
    let mut host_peer = Peer::new(
        host_store,
        &guest.worker.binding,
        epoch,
        Receiver::Host,
        None,
        Default::default(),
        host_store
            .snapshot_directory(&guest.worker.binding)
            .unwrap(),
    )
    .unwrap();
    let guest_store = guest.store();
    let mut guest_peer = Peer::new(
        &guest_store,
        &guest.worker.binding,
        epoch,
        Receiver::Guest,
        None,
        Default::default(),
        None,
    )
    .unwrap();
    let mut guest_inbox = guest.inbox();
    deliver_all(&mut host_peer, &mut guest_peer, &mut guest_inbox);
    deliver_all(&mut guest_peer, &mut host_peer, host_inbox);
    loop {
        if from_host {
            if !host_peer.poll_outbox().unwrap() {
                break;
            }
            deliver_all(&mut host_peer, &mut guest_peer, &mut guest_inbox);
            if drop_after_first {
                assert!(guest_peer.receipt_pending());
                break;
            }
            deliver_all(&mut guest_peer, &mut host_peer, host_inbox);
        } else {
            if !guest_peer.poll_outbox().unwrap() {
                break;
            }
            deliver_all(&mut guest_peer, &mut host_peer, host_inbox);
            if drop_after_first {
                assert!(host_peer.receipt_pending());
                break;
            }
            deliver_all(&mut host_peer, &mut guest_peer, &mut guest_inbox);
        }
    }
}

#[test]
fn accepted_guest_delivery_requires_hook_evidence_and_survives_disconnect() {
    use super::super::peer::Peer;
    use crate::sandbox::receipts::view;

    let root = tempfile::tempdir().unwrap();
    let host_db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    host_db
        .conn()
        .execute(
            "INSERT INTO instances (name, created_at) VALUES ('owner', 1)",
            [],
        )
        .unwrap();
    let sender = crate::identity::resolve_from_name(&host_db, "owner").unwrap();
    let owner = HostIdentity::capture(&host_db, &sender).unwrap();
    let host_store = Store::initialize(&host_db).unwrap();
    let team = host_store.create_team(Some(&owner)).unwrap();
    let guest = GuestNode::new(
        root.path(),
        "guest.db",
        &host_store,
        &team.id,
        Access::Host(&owner),
        "worker",
        2.0,
    );
    let binding = &guest.worker.binding;
    let target = format!("@{}", binding.worker.worker);
    assert_eq!(
        run_cmd_send(
            &host_db,
            "owner",
            &["send", "--json", &target, "--", "test receipt"]
        ),
        0
    );
    let event_id = host_db.get_last_event_id();
    let read = || view(host_db.conn(), event_id).unwrap();
    assert_eq!(read()["routes"][0]["transport"], "pending");

    let guest_store = guest.store();
    let mut host_inbox = HostInbox::new(&host_db, &host_store, binding);
    let mut guest_inbox = guest.inbox();
    let peers = |epoch| {
        (
            Peer::new(
                &host_store,
                binding,
                epoch,
                Receiver::Host,
                None,
                Default::default(),
                host_store.snapshot_directory(binding).unwrap(),
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
    let (mut host, mut peer) = peers(2);
    deliver_all(&mut host, &mut peer, &mut guest_inbox);
    deliver_all(&mut peer, &mut host, &mut host_inbox);
    assert!(host.poll_outbox().unwrap());
    deliver_all(&mut host, &mut peer, &mut guest_inbox);
    deliver_all(&mut peer, &mut host, &mut host_inbox);
    let prepared = crate::hooks::common::prepare_pending_messages(&guest.db, "guest").unwrap();
    guest
        .db
        .advance_instance_cursor("guest", prepared.ack.last_event_id)
        .unwrap();
    guest
        .db
        .conn()
        .execute(
            "UPDATE instances SET status = 'listening' WHERE name = 'guest'",
            [],
        )
        .unwrap();

    assert!(host.observe_now().unwrap());
    deliver_all(&mut host, &mut peer, &mut guest_inbox);
    deliver_all(&mut peer, &mut host, &mut host_inbox);
    let pending = read();
    assert_eq!(pending["routes"][0]["transport"], "accepted");
    assert_eq!(pending["routes"][0]["agent_delivery"], "unknown");
    assert_eq!(pending["routes"][0]["bridge"]["connection"], "connected");
    assert_eq!(pending["routes"][0]["bridge"]["last_activity"], "listening");
    drop(host);
    drop(peer);
    assert_eq!(read()["routes"][0]["bridge"]["connection"], "disconnected");
    assert_eq!(read()["routes"][0]["bridge"]["last_activity"], "listening");

    crate::hooks::common::commit_delivery_ack(&guest.db, &prepared.ack);
    assert_eq!(read()["routes"][0]["agent_delivery"], "unknown");
    let (mut host, mut peer) = peers(3);
    deliver_all(&mut host, &mut peer, &mut guest_inbox);
    deliver_all(&mut peer, &mut host, &mut host_inbox);
    assert!(host.observe_now().unwrap());
    deliver_all(&mut host, &mut peer, &mut guest_inbox);
    deliver_all(&mut peer, &mut host, &mut host_inbox);
    let delivered = read();
    assert_eq!(delivered["id"], pending["id"]);
    assert_eq!(delivered["routes"][0]["transport"], "accepted");
    assert_eq!(delivered["routes"][0]["agent_delivery"], "observed");
    assert_eq!(delivered["routes"][0]["bridge"]["last_activity"], "active");
    assert_eq!(delivered["routes"][0]["bridge"]["epoch"], 3);
    host.begin_drain(true).unwrap();
    host.complete_rotation();
    drop(host);
    assert_eq!(read()["routes"][0]["agent_delivery"], "observed");
    assert_eq!(read()["routes"][0]["bridge"]["connection"], "rotating");
    drop(peer);
    let (mut host, mut peer) = peers(4);
    deliver_all(&mut host, &mut peer, &mut guest_inbox);
    deliver_all(&mut peer, &mut host, &mut host_inbox);
    assert!(host.observe_now().unwrap());
    deliver_all(&mut host, &mut peer, &mut guest_inbox);
    deliver_all(&mut peer, &mut host, &mut host_inbox);
    assert_eq!(read()["routes"][0]["bridge"]["connection"], "connected");
    drop(host);
    assert_eq!(read()["routes"][0]["bridge"]["connection"], "disconnected");
}

#[test]
fn guest_broadcast_routes_to_bound_team_with_stable_ids_and_postcommit_wakes() {
    let root = tempfile::tempdir().unwrap();
    let host_db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    host_db
        .conn()
        .execute_batch(
            "INSERT INTO instances (name, created_at) VALUES
             ('host_a', 1), ('host_b', 2), ('outsider_host', 3), ('other_team_host', 4);
             INSERT INTO instances (name, created_at, status) VALUES ('stopped_host', 5, 'stopped');",
        )
        .unwrap();
    let host_store = Store::initialize(&host_db).unwrap();
    let cap = |name| {
        HostIdentity::capture(
            &host_db,
            &crate::identity::resolve_from_name(&host_db, name).unwrap(),
        )
        .unwrap()
    };
    let (owner, host_a, stopped_host, other_owner) = (
        cap("host_b"),
        cap("host_a"),
        cap("stopped_host"),
        cap("other_team_host"),
    );
    let team = host_store.create_team(Some(&owner)).unwrap();
    host_store
        .enroll_host(&team.id, Access::Host(&owner), &host_a)
        .unwrap();
    host_store
        .enroll_host(&team.id, Access::Host(&owner), &stopped_host)
        .unwrap();
    let other_team = host_store.create_team(Some(&other_owner)).unwrap();

    let guest_a = GuestNode::new(
        root.path(),
        "ga.db",
        &host_store,
        &team.id,
        Access::Host(&owner),
        "alpha",
        10.0,
    );
    let guest_b = GuestNode::new(
        root.path(),
        "gb.db",
        &host_store,
        &team.id,
        Access::Host(&owner),
        "bravo",
        20.0,
    );
    guest_b
        .db
        .conn()
        .execute(
            "INSERT INTO instances (name, created_at, last_event_id) VALUES ('unrelated', 21, 0)",
            [],
        )
        .unwrap();
    let guest_c = GuestNode::new(
        root.path(),
        "gc.db",
        &host_store,
        &team.id,
        Access::Host(&owner),
        "charlie",
        30.0,
    );
    guest_a.refresh_directory(&host_store, 2);
    guest_b.refresh_directory(&host_store, 2);
    guest_c.refresh_directory(&host_store, 2);
    let retired = host_store
        .reserve_worker(
            &team.id,
            Access::Host(&owner),
            "retired",
            Tool::Claude,
            Placement {
                gateway: "gateway",
                workspace: "default",
                sandbox: "vm-retired",
                incarnation: &uuid::Uuid::new_v4().to_string(),
            },
        )
        .unwrap();
    host_store
        .retire_worker(Access::Host(&owner), &retired.binding)
        .unwrap();
    let outside = host_store
        .reserve_worker(
            &other_team.id,
            Access::Host(&other_owner),
            "outsider",
            Tool::Claude,
            Placement {
                gateway: "gateway",
                workspace: "default",
                sandbox: "vm-out",
                incarnation: &uuid::Uuid::new_v4().to_string(),
            },
        )
        .unwrap();

    let host_a_wake = wake_listener(&host_db, "host_a", "pty");
    let host_b_wake = wake_listener(&host_db, "host_b", "pty");
    let host_b_inject = wake_listener(&host_db, "host_b", "inject");
    let outsider_wake = wake_listener(&host_db, "outsider_host", "pty");
    let stopped_wake = wake_listener(&host_db, "stopped_host", "pty");
    let observer_wake = wake_listener(&host_db, "host_a", "events_stream");
    let guest_b_wake = wake_listener(&guest_b.db, "guest", "pty");
    let guest_c_wake = wake_listener(&guest_c.db, "guest", "pty");

    // Enqueue broadcast 1 via guest_send API (verifying Queued feedback & CRLF->LF), then broadcast 2
    // and direct message 3 via ordinary `hcom send` CLI.
    let ga_id = crate::identity::resolve_from_name(&guest_a.db, "guest").unwrap();
    let target =
        crate::sandbox::guest_send::resolve(&guest_a.db, &ga_id, Some(&[]), true, None, false)
            .unwrap()
            .unwrap();
    let queued = crate::sandbox::guest_send::queue(
        &guest_a.db,
        &ga_id,
        &target,
        "team update\r\nline2\t@outsider_host",
        &crate::messages::MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Request),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    assert!(queued.broadcast && queued.queued_to.is_empty());
    assert_eq!(queued.display_target, "team broadcast");
    assert_eq!(queued.forwarding, "pending");
    let journal = event(&guest_a.db, queued.event_id);
    assert_eq!(journal["queued_to"], json!([]));
    assert_eq!(journal["_sandbox"]["broadcast"], json!(true));
    assert_eq!(journal["text"], json!("team update\nline2\t@outsider_host"));
    assert_eq!(
        run_cmd_send(
            &guest_a.db,
            "guest",
            &["send", "--json", "--broadcast", "--", "second broadcast"]
        ),
        0
    );
    assert_eq!(
        run_cmd_send(
            &guest_a.db,
            "guest",
            &["send", "--json", "@bravo", "--", "direct followup"]
        ),
        0
    );

    let now = now_epoch_i64();
    let wire_b1 = guest_a
        .store()
        .next_pending(&guest_a.worker.binding, now)
        .unwrap()
        .unwrap();
    let mut host_inbox_a = HostInbox::new(&host_db, &host_store, &guest_a.worker.binding);

    // Start after the directory refresh at epoch 2. Drop the first receipt to exercise replay.
    bridge_step(&host_store, &guest_a, 3, &mut host_inbox_a, true, false);
    host_a_wake.accept().expect("host_a wakes");
    host_b_wake.accept().expect("host_b wakes via wake_ports");
    observer_wake.accept().expect("observer wakes");
    for sock in [&host_b_inject, &outsider_wake, &stopped_wake] {
        assert_eq!(
            sock.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    let host_event_id = host_db.get_last_event_id();
    let host_ev = event(&host_db, host_event_id);
    assert_eq!(host_ev["mentions"], json!(["host_a", "host_b"]));
    assert_eq!(host_ev["delivered_to"], json!(["host_a", "host_b"]));
    assert_eq!(
        host_ev["queued_to"],
        json!([
            guest_b.worker.binding.worker.worker,
            guest_c.worker.binding.worker.worker
        ])
    );
    assert_eq!(host_ev["_sandbox"]["id"], json!(wire_b1.id));
    assert_eq!(
        host_ev["_sandbox"]["recipient_created_ats"],
        json!({"host_a": 1.0, "host_b": 2.0})
    );
    assert_eq!(host_db.get_unread_messages("host_a").len(), 1);
    assert_eq!(host_db.get_unread_messages("host_b").len(), 1);
    for excluded in ["outsider_host", "other_team_host", "stopped_host"] {
        assert!(host_db.get_unread_messages(excluded).is_empty());
    }
    assert!(
        crate::relay::push::build_push_payload(&host_db, "device")
            .unwrap()
            .1
            .is_empty()
    );
    for none_route in [&guest_a.worker.binding, &retired.binding, &outside.binding] {
        assert!(host_store.next_pending(none_route, now).unwrap().is_none());
    }

    // Reconnect and replay the first broadcast before draining the other two messages.
    bridge_step(&host_store, &guest_a, 4, &mut host_inbox_a, false, false);
    assert_eq!(
        host_db
            .conn()
            .query_row(
                "SELECT count(*) FROM events WHERE type='message'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        3
    );
    for socket in [&host_a_wake, &host_b_wake] {
        socket
            .accept()
            .expect("the second broadcast wakes each host");
        assert_eq!(
            socket.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    // Deliver to guest_b: drop after first to verify guest replay, then drain remaining in FIFO order.
    let mut host_inbox_b = HostInbox::new(&host_db, &host_store, &guest_b.worker.binding);
    bridge_step(&host_store, &guest_b, 3, &mut host_inbox_b, true, true);
    guest_b_wake.accept().expect("guest_b wakes");
    let guest_b_first = guest_b.db.get_last_event_id();
    bridge_step(&host_store, &guest_b, 4, &mut host_inbox_b, false, true);
    let gb_msgs: Vec<String> = guest_b
        .db
        .get_unread_messages("guest")
        .into_iter()
        .map(|m| m.text)
        .collect();
    assert_eq!(
        gb_msgs,
        [
            "team update\nline2\t@outsider_host",
            "second broadcast",
            "direct followup"
        ]
    );
    assert!(guest_b.db.get_unread_messages("unrelated").is_empty());

    // guest_c queue remained intact until delivered independently.
    assert_eq!(
        host_store
            .next_pending(&guest_c.worker.binding, now)
            .unwrap(),
        Some(wire_b1.clone())
    );
    let mut host_inbox_c = HostInbox::new(&host_db, &host_store, &guest_c.worker.binding);
    bridge_step(&host_store, &guest_c, 3, &mut host_inbox_c, false, true);
    guest_c_wake.accept().expect("guest_c wakes");
    assert_eq!(guest_c.db.get_unread_messages("guest").len(), 2);

    // Directed replies to received broadcast correlate for guest_b and host_a, reject outsider/replaced host.
    assert_eq!(
        run_cmd_send(
            &guest_b.db,
            "guest",
            &[
                "send",
                "--json",
                "--intent",
                "ack",
                "--reply-to",
                &guest_b_first.to_string(),
                "@alpha",
                "--",
                "ack broadcast"
            ]
        ),
        0
    );
    let gb_reply = guest_b
        .store()
        .next_pending(&guest_b.worker.binding, now)
        .unwrap()
        .unwrap();
    let Imported::New(host_reply_ev) = apply(
        &host_store,
        Receiver::Host,
        &guest_b.worker.binding,
        &gb_reply,
        &mut host_inbox_b,
    ) else {
        panic!("expected forwarded guest reply");
    };
    assert_eq!(
        event(&host_db, host_reply_ev)["reply_to_local"],
        json!(host_event_id)
    );
    let mut guest_a_inbox = guest_a.inbox();
    let fwd_reply = host_store
        .next_pending(&guest_a.worker.binding, now)
        .unwrap()
        .unwrap();
    let Imported::New(ga_reply_ev) = apply(
        &guest_a.store(),
        Receiver::Guest,
        &guest_a.worker.binding,
        &fwd_reply,
        &mut guest_a_inbox,
    ) else {
        panic!("expected guest_a reply");
    };
    assert_eq!(
        event(&guest_a.db, ga_reply_ev)["reply_to_local"],
        json!(queued.event_id)
    );

    let host_a_id = crate::identity::resolve_from_name(&host_db, "host_a").unwrap();
    let host_reply = crate::sandbox::outgoing::queue(
        &host_db,
        &host_a_id,
        &guest_a.worker,
        "host ack",
        &crate::messages::MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Ack),
            ..Default::default()
        },
        Some(host_event_id),
    )
    .unwrap();
    assert_eq!(
        event(&host_db, host_reply.event_id)["reply_to_local"],
        json!(host_event_id)
    );
    let outsider_id = crate::identity::resolve_from_name(&host_db, "outsider_host").unwrap();
    assert!(
        crate::sandbox::outgoing::queue(
            &host_db,
            &outsider_id,
            &guest_a.worker,
            "outsider",
            &Default::default(),
            Some(host_event_id)
        )
        .is_err()
    );
    host_db
        .conn()
        .execute(
            "UPDATE instances SET created_at = 99 WHERE name = 'host_a'",
            [],
        )
        .unwrap();
    let replaced_a = crate::identity::resolve_from_name(&host_db, "host_a").unwrap();
    host_store
        .enroll_host(
            &team.id,
            Access::Host(&owner),
            &HostIdentity::capture(&host_db, &replaced_a).unwrap(),
        )
        .unwrap();
    assert!(
        crate::sandbox::outgoing::queue(
            &host_db,
            &replaced_a,
            &guest_a.worker,
            "stale",
            &Default::default(),
            Some(host_event_id)
        )
        .is_err()
    );

    // Host global `hcom send --broadcast` still reaches local hosts and omits sandboxes.
    assert_eq!(
        run_cmd_send(
            &host_db,
            "host_b",
            &["send", "--json", "--broadcast", "--", "global"]
        ),
        0
    );
    let global = event(&host_db, host_db.get_last_event_id());
    assert_eq!(global["scope"], json!("broadcast"));
    let delivered = global["delivered_to"].as_array().unwrap();
    assert!(delivered.contains(&json!("outsider_host")));
    assert!(!delivered.contains(&json!(guest_a.worker.binding.worker.worker)));
}

#[test]
fn guest_only_broadcast_forwards_without_waking_host_observers() {
    let root = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    db.conn()
        .execute(
            "INSERT INTO instances (name, created_at) VALUES ('observer', 1)",
            [],
        )
        .unwrap();
    let observer = wake_listener(&db, "observer", "events_stream");
    let store = Store::initialize(&db).unwrap();
    let team = store.create_team(None).unwrap();
    let access = Access::Controller(team.external_controller.as_ref().unwrap());
    let sender = GuestNode::new(root.path(), "a.db", &store, &team.id, access, "alpha", 2.0);
    let peer = GuestNode::new(root.path(), "b.db", &store, &team.id, access, "bravo", 3.0);
    let binding = &sender.worker.binding;
    let message = broadcast_msg(binding.worker.clone(), 1, "guest-only broadcast");
    let mut inbox = HostInbox::new(&db, &store, binding);
    let Imported::New(event_id) = apply(&store, Receiver::Host, binding, &message, &mut inbox)
    else {
        panic!("expected a new forwarding journal event");
    };
    assert_eq!(event(&db, event_id)["delivered_to"], json!([]));
    assert_eq!(
        store
            .next_pending(&peer.worker.binding, now_epoch_i64())
            .unwrap(),
        Some(message)
    );
    assert_eq!(
        observer.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn guest_broadcast_rejects_mixed_scopes_forgery_and_handles_oversized_team() {
    let root = tempfile::tempdir().unwrap();
    let host_db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    host_db
        .conn()
        .execute_batch(
            "INSERT INTO instances (name, created_at) VALUES ('host', 1), ('outsider', 2);",
        )
        .unwrap();
    let host_store = Store::initialize(&host_db).unwrap();
    let host_id = crate::identity::resolve_from_name(&host_db, "host").unwrap();
    let owner = HostIdentity::capture(&host_db, &host_id).unwrap();
    let team = host_store.create_team(Some(&owner)).unwrap();

    let guest = GuestNode::new(
        root.path(),
        "guest.db",
        &host_store,
        &team.id,
        Access::Host(&owner),
        "alpha",
        10.0,
    );
    let sibling = GuestNode::new(
        root.path(),
        "sibling.db",
        &host_store,
        &team.id,
        Access::Host(&owner),
        "bravo",
        20.0,
    );

    // Seed direct + threaded events on guest.db to test reply-to mixed-scope rejection.
    crate::sandbox::outgoing::queue(
        &host_db,
        &host_id,
        &guest.worker,
        "direct",
        &crate::messages::MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Request),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    crate::sandbox::outgoing::queue(
        &host_db,
        &host_id,
        &guest.worker,
        "threaded",
        &crate::messages::MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Request),
            thread: Some("ops".into()),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let now = now_epoch_i64();
    let mut guest_inbox = guest.inbox();
    let w1 = host_store
        .next_pending(&guest.worker.binding, now)
        .unwrap()
        .unwrap();
    host_store
        .acknowledge(&guest.worker.binding, &w1.id, &w1.digest, now)
        .unwrap();
    let w2 = host_store
        .next_pending(&guest.worker.binding, now)
        .unwrap()
        .unwrap();
    let Imported::New(d_ev) = apply(
        &guest.store(),
        Receiver::Guest,
        &guest.worker.binding,
        &w1,
        &mut guest_inbox,
    ) else {
        panic!("direct event")
    };
    let Imported::New(t_ev) = apply(
        &guest.store(),
        Receiver::Guest,
        &guest.worker.binding,
        &w2,
        &mut guest_inbox,
    ) else {
        panic!("threaded event")
    };

    let before_guest = guest.db.get_last_event_id();
    let (d_ref, t_ref) = (d_ev.to_string(), t_ev.to_string());
    for bad in [
        vec!["send", "--json", "--broadcast", "@host", "--", "mixed"],
        vec![
            "send",
            "--json",
            "--broadcast",
            "--thread",
            "ops",
            "--",
            "mixed",
        ],
        vec![
            "send",
            "--json",
            "--broadcast",
            "--reply-to",
            &d_ref,
            "--",
            "mixed",
        ],
        vec![
            "send",
            "--json",
            "--broadcast",
            "--reply-to",
            &t_ref,
            "--",
            "mixed",
        ],
        vec![
            "send",
            "--json",
            "--broadcast",
            "--intent",
            "ack",
            "--reply-to",
            &d_ref,
            "--",
            "mixed",
        ],
        vec![
            "send",
            "--json",
            "--broadcast",
            "--title",
            "b",
            "--description",
            "d",
            "--",
            "bundle",
        ],
        vec!["send", "--json", "--", "implicit"],
        vec![
            "send",
            "--json",
            "--from",
            "bigboss",
            "--broadcast",
            "--",
            "spoof",
        ],
        vec!["send", "--json", "-b", "--broadcast", "--", "spoof"],
        vec!["send", "--json", "--broadcast", "--", "bad\u{1b}[31mtext"],
        vec!["send", "--json", "--broadcast", "--", "bad\u{0007}bell"],
        vec!["send", "--json", "--broadcast", "--", "bad\rreturn"],
        vec!["send", "--json", "--broadcast", "--", "bad\u{007f}del"],
        vec!["send", "--json", "--broadcast", "--", "bad\u{0085}c1"],
    ] {
        assert_eq!(
            run_cmd_send(&guest.db, "guest", &bad),
            1,
            "expected failure for {bad:?}"
        );
    }
    {
        use clap::Parser;
        let spoof_ctx = crate::shared::CommandContext {
            explicit_name: Some("other".into()),
            identity: Some(crate::shared::SenderIdentity {
                kind: crate::shared::SenderKind::Instance,
                name: "other".into(),
                instance_data: Some(json!({"created_at": 10.0})),
                session_id: None,
            }),
            go: true,
            identity_warning: None,
        };
        let mut spoof_args = crate::commands::send::SendArgs::try_parse_from([
            "send",
            "--json",
            "--broadcast",
            "--",
            "spoof",
        ])
        .unwrap();
        spoof_args.had_separator = true;
        assert_eq!(
            crate::commands::send::cmd_send(&guest.db, &spoof_args, Some(&spoof_ctx)),
            1
        );
    }
    assert_eq!(guest.db.get_last_event_id(), before_guest);
    assert!(
        guest
            .store()
            .next_pending(&guest.worker.binding, now)
            .unwrap()
            .is_none()
    );

    // HostInbox & GuestInbox forgery and retired-sender rejection.
    let mut host_inbox = HostInbox::new(&host_db, &host_store, &guest.worker.binding);
    let host_before = host_db.get_last_event_id();
    let mut forged_gen = broadcast_msg(guest.worker.binding.worker.clone(), 1, "gen");
    forged_gen.payload.sender_routing_generation = Some("fake".into());
    let forged_gen = Message::new(forged_gen.id, forged_gen.payload).unwrap();
    for bad_msg in [
        broadcast_msg(sibling.worker.binding.worker.clone(), 1, "forged origin"),
        forged_gen,
    ] {
        assert!(
            host_store
                .import(Receiver::Host, &guest.worker.binding, &bad_msg, now, |tx| {
                    host_inbox.insert(tx, &bad_msg)
                })
                .is_err()
        );
    }
    let self_msg = broadcast_msg(guest.worker.binding.worker.clone(), 3, "loopback");
    assert!(
        guest
            .store()
            .import(
                Receiver::Guest,
                &guest.worker.binding,
                &self_msg,
                now,
                |tx| guest_inbox.insert(tx, &self_msg)
            )
            .is_err()
    );

    // Legacy oversized team (>257 members) rejects broadcasts terminally without breaking directed traffic.
    host_db
        .conn()
        .execute_batch(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 258)
             INSERT INTO instances(name, created_at) SELECT 'legacy_' || i, 50 FROM n;",
        )
        .unwrap();
    host_db
        .conn()
        .execute(
            "INSERT INTO sandbox_team_hosts SELECT ?, name, created_at FROM instances WHERE created_at = 50",
            [&team.id],
        )
        .unwrap();
    assert!(
        host_store
            .snapshot_directory(&guest.worker.binding)
            .unwrap()
            .is_none()
    );
    guest
        .store()
        .attach_bridge(&guest.worker.binding, 2)
        .unwrap()
        .write(|tx| guest_inbox.replace_directory(tx, None))
        .unwrap();
    assert_eq!(
        run_cmd_send(
            &guest.db,
            "guest",
            &["send", "--json", "--broadcast", "--", "oversized team"]
        ),
        0
    );
    let mut host_inbox = HostInbox::new(&host_db, &host_store, &guest.worker.binding);
    bridge_step(&host_store, &guest, 3, &mut host_inbox, false, false);
    assert!(
        guest
            .store()
            .next_pending(&guest.worker.binding, now)
            .unwrap()
            .is_none()
    );
    assert_eq!(host_db.get_last_event_id(), host_before);
    assert!(
        host_store
            .next_pending(&sibling.worker.binding, now)
            .unwrap()
            .is_none()
    );
    let oversized_msg_id: MessageId = serde_json::from_value(
        event(&guest.db, guest.db.get_last_event_id())["_sandbox"]["id"].clone(),
    )
    .unwrap();
    assert_eq!(
        guest.store().delivery(&oversized_msg_id, now).unwrap(),
        Some(crate::sandbox::store::Delivery::Rejected)
    );
    let direct_msg = addressed(
        message(
            guest.worker.binding.worker.clone(),
            2,
            &sibling.worker.binding.worker.worker,
        ),
        &sibling.worker.binding.worker.generation,
    );
    assert!(matches!(
        apply(
            &host_store,
            Receiver::Host,
            &guest.worker.binding,
            &direct_msg,
            &mut host_inbox
        ),
        Imported::New(_)
    ));
    let directed_ev = host_db.get_last_event_id();
    assert_eq!(
        host_store
            .next_pending(&sibling.worker.binding, now)
            .unwrap(),
        Some(direct_msg)
    );

    // Retired sender & stale guest instance rejected.
    host_store
        .retire_worker(Access::Host(&owner), &guest.worker.binding)
        .unwrap();
    let retired_msg = broadcast_msg(guest.worker.binding.worker.clone(), 4, "retired");
    assert!(
        host_store
            .import(
                Receiver::Host,
                &guest.worker.binding,
                &retired_msg,
                now,
                |tx| host_inbox.insert(tx, &retired_msg)
            )
            .is_err()
    );
    assert_eq!(host_db.get_last_event_id(), directed_ev);
    assert_ne!(host_before, directed_ev);

    guest
        .db
        .conn()
        .execute(
            "UPDATE instances SET created_at = 99 WHERE name = 'guest'",
            [],
        )
        .unwrap();
    assert_eq!(
        run_cmd_send(
            &guest.db,
            "guest",
            &["send", "--json", "--broadcast", "--", "stale"]
        ),
        1
    );
}

#[test]
fn guest_broadcast_rolls_back_events_all_queues_and_wakes_on_failure() {
    use crate::sandbox::admission::{Budget, Limits};

    let root = tempfile::tempdir().unwrap();
    let host_db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    host_db
        .conn()
        .execute(
            "INSERT INTO instances (name, created_at) VALUES ('host', 1)",
            [],
        )
        .unwrap();
    let host_id = crate::identity::resolve_from_name(&host_db, "host").unwrap();
    let owner = HostIdentity::capture(&host_db, &host_id).unwrap();

    // Pending budget = 1 lets sibling_b forward_in pass and fails on sibling_c -> rolls back all writes.
    let tight = Store::with_limits(
        &host_db,
        Limits {
            pending: Budget {
                messages: 1,
                bytes: 1024 * 1024,
            },
            ..Limits::default()
        },
    )
    .unwrap();
    let team = tight.create_team(Some(&owner)).unwrap();
    let reserve = |alias| {
        tight
            .reserve_worker(
                &team.id,
                Access::Host(&owner),
                alias,
                Tool::Claude,
                Placement {
                    gateway: "gateway",
                    workspace: "default",
                    sandbox: alias,
                    incarnation: &uuid::Uuid::new_v4().to_string(),
                },
            )
            .unwrap()
            .binding
    };
    let (sender, sibling_b, sibling_c) = (reserve("alpha"), reserve("bravo"), reserve("charlie"));
    let host_wake = wake_listener(&host_db, "host", "pty");
    let observer_wake = wake_listener(&host_db, "host", "events_stream");
    let msg = broadcast_msg(sender.worker.clone(), 1, "atomic rollback");
    let now = now_epoch_i64();

    {
        let mut inbox = HostInbox::new(&host_db, &tight, &sender);
        assert!(
            tight
                .import(Receiver::Host, &sender, &msg, now, |tx| inbox
                    .insert(tx, &msg))
                .is_err()
        );
    }
    assert_eq!(host_db.get_last_event_id(), 0);
    assert!(tight.next_pending(&sibling_b, now).unwrap().is_none());
    assert!(tight.next_pending(&sibling_c, now).unwrap().is_none());
    assert_eq!(
        host_db
            .conn()
            .query_row("SELECT count(*) FROM sandbox_usage", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    for sock in [&host_wake, &observer_wake] {
        assert_eq!(
            sock.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    // Trigger failure on sandbox_inbox after events + both outboxes wrote also rolls back completely.
    let normal = Store::initialize(&host_db).unwrap();
    host_db
        .conn()
        .execute_batch(
            "CREATE TEMP TRIGGER abort_broadcast_inbox BEFORE INSERT ON sandbox_inbox
             BEGIN SELECT RAISE(ABORT, 'injected broadcast inbox failure'); END;",
        )
        .unwrap();
    let mut inbox = HostInbox::new(&host_db, &normal, &sender);
    assert!(
        normal
            .import(Receiver::Host, &sender, &msg, now, |tx| inbox
                .insert(tx, &msg))
            .is_err()
    );
    assert_eq!(host_db.get_last_event_id(), 0);
    assert!(normal.next_pending(&sibling_b, now).unwrap().is_none());
    assert!(normal.next_pending(&sibling_c, now).unwrap().is_none());
    for sock in [&host_wake, &observer_wake] {
        assert_eq!(
            sock.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    host_db
        .conn()
        .execute_batch("DROP TRIGGER abort_broadcast_inbox")
        .unwrap();

    let Imported::New(event_id) = apply(&normal, Receiver::Host, &sender, &msg, &mut inbox) else {
        panic!("expected retry success");
    };
    assert_eq!(event(&host_db, event_id)["delivered_to"], json!(["host"]));
    assert_eq!(
        normal.next_pending(&sibling_b, now).unwrap(),
        Some(msg.clone())
    );
    assert_eq!(normal.next_pending(&sibling_c, now).unwrap(), Some(msg));
    host_wake.accept().expect("host wakes after commit");
    observer_wake.accept().expect("observer wakes after commit");
}

/// Granted host deliveries retain verifiable membership evidence bound to the
/// route and worker generation; conflicting grants fail closed and ungranted
/// traffic retains nothing.
#[test]
fn guest_retains_grant_evidence_and_rejects_conflicting_ids() {
    let root = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&root.path().join("guest.db")).unwrap();
    db.conn()
        .execute_batch("INSERT INTO instances (name, created_at) VALUES ('guest', 1);")
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
        generation: "host-generation".into(),
        incarnation: uuid::Uuid::new_v4().to_string(),
    };
    let local = LocalWorker {
        name: "guest".into(),
        created_at: 1.0,
    };
    let mut inbox = GuestInbox::new(&db, &binding, &local).unwrap();
    let scoped = |sequence: i64, conversation: &str| {
        Message::new(
            MessageId {
                origin: sender.clone(),
                sequence,
            },
            MessagePayload {
                target: binding.worker.worker.clone(),
                target_generation: Some(binding.worker.generation.clone()),
                sender_routing_generation: Some("host-recipient".into()),
                broadcast: false,
                thread: Some("work".into()),
                reply_to: None,
                conversation: Some(conversation.into()),
                intent: Intent::Inform,
                text: "scoped".into(),
            },
        )
        .unwrap()
    };
    let ungranted = Message::new(
        MessageId {
            origin: sender.clone(),
            sequence: 4,
        },
        MessagePayload {
            target: binding.worker.worker.clone(),
            target_generation: Some(binding.worker.generation.clone()),
            sender_routing_generation: Some("host-recipient".into()),
            broadcast: false,
            thread: Some("work".into()),
            reply_to: None,
            conversation: None,
            intent: Intent::Inform,
            text: "plain".into(),
        },
    )
    .unwrap();
    let evidence_rows = || -> i64 {
        db.conn()
            .query_row(
                "SELECT count(*) FROM sandbox_guest_conversations",
                [],
                |row| row.get(0),
            )
            .unwrap()
    };

    // A granted delivery retains exact evidence and stamps the local event.
    let Imported::New(granted_event) = apply(
        &store,
        Receiver::Guest,
        &binding,
        &scoped(1, "conv-1"),
        &mut inbox,
    ) else {
        panic!("expected granted import")
    };
    assert_eq!(
        event(&db, granted_event)["_sandbox"]["conversation"],
        json!("conv-1")
    );
    let (label, generation, retained): (String, String, String) = db
        .conn()
        .query_row(
            "SELECT label, generation, binding FROM sandbox_guest_conversations
             WHERE conversation = 'conv-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(label, "work");
    assert_eq!(generation, "one");
    assert_eq!(retained, serde_json::to_string(&binding).unwrap());

    // A later message repeating the same grant keeps one evidence row.
    apply(
        &store,
        Receiver::Guest,
        &binding,
        &scoped(2, "conv-1"),
        &mut inbox,
    );
    assert_eq!(evidence_rows(), 1);

    // A different conversation id for the same label conflicts: the import
    // fails and leaves no event or evidence behind.
    let conflict = store.import(
        Receiver::Guest,
        &binding,
        &scoped(3, "conv-2"),
        now_epoch_i64(),
        |tx| inbox.insert(tx, &scoped(3, "conv-2")),
    );
    assert!(
        conflict
            .unwrap_err()
            .to_string()
            .contains("conflicts with its retained grant")
    );
    assert_eq!(evidence_rows(), 1);

    // Ungranted directed traffic retains nothing.
    apply(&store, Receiver::Guest, &binding, &ungranted, &mut inbox);
    assert_eq!(evidence_rows(), 1);

    // A replay reuses the committed event without touching evidence.
    let replay = apply(
        &store,
        Receiver::Guest,
        &binding,
        &scoped(1, "conv-1"),
        &mut inbox,
    );
    assert!(matches!(replay, Imported::Replay));
    assert_eq!(evidence_rows(), 1);
}

/// The host rejects a forged scoped marker at frame validation before any
/// grant resolution or import happens.
#[test]
fn host_import_rejects_forged_scoped_markers() {
    let root = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    db.conn()
        .execute_batch("INSERT INTO instances (name, created_at) VALUES ('host', 1);")
        .unwrap();
    let identity = crate::identity::resolve_from_name(&db, "host").unwrap();
    let member = HostIdentity::capture(&db, &identity).unwrap();
    let store = Store::initialize(&db).unwrap();
    let team = store.create_team(Some(&member)).unwrap();
    let source = store
        .reserve_worker(
            &team.id,
            Access::Host(&member),
            "source",
            Tool::Claude,
            Placement {
                gateway: "gateway",
                workspace: "default",
                sandbox: "source",
                incarnation: &uuid::Uuid::new_v4().to_string(),
            },
        )
        .unwrap()
        .binding;
    // Message::new refuses to build a conversation-bearing payload without
    // host routing, so forge past the constructor: the import path must
    // reject the malformed or addressed marker at frame validation (via the
    // size check) before the inbox grant resolution ever sees it.
    let forged = Message {
        digest: [0; 32],
        id: MessageId {
            origin: source.worker.clone(),
            sequence: 1,
        },
        payload: MessagePayload {
            target: "host".into(),
            target_generation: Some(host_recipient_generation(db.conn(), "host", 1.0).unwrap()),
            sender_routing_generation: None,
            broadcast: false,
            thread: Some("work".into()),
            reply_to: None,
            conversation: Some("conv-1".into()),
            intent: Intent::Inform,
            text: "forged scope".into(),
        },
    };
    let mut inbox = HostInbox::new(&db, &store, &source);
    let failure = store
        .import(Receiver::Host, &source, &forged, now_epoch_i64(), |tx| {
            inbox.insert(tx, &forged)
        })
        .unwrap_err()
        .to_string();
    assert!(
        failure.contains("guest thread expansion requests carry no target or generation"),
        "unexpected rejection: {failure}"
    );
    assert_eq!(db.get_last_event_id(), 0);
}

/// Guest thread-only dispatch resolves a retained grant, queues one immutable
/// expansion request, and the host fans it out to the live creating host and
/// the other current guests over preserved wire identities. Ungranted
/// guests, foreign scopes, and stale generations are fenced; ordinary
/// directed sends stay untouched.
#[test]
fn guest_thread_only_dispatch_expands_to_current_members() {
    let root = tempfile::tempdir().unwrap();
    let host_db = HcomDb::open_at(&root.path().join("host.db")).unwrap();
    host_db
        .conn()
        .execute_batch(
            "INSERT INTO instances (name, created_at) VALUES ('host', 1), ('outsider', 2);",
        )
        .unwrap();
    let host_store = Store::initialize(&host_db).unwrap();
    let cap = |name| {
        HostIdentity::capture(
            &host_db,
            &crate::identity::resolve_from_name(&host_db, name).unwrap(),
        )
        .unwrap()
    };
    let creator = cap("host");
    let outsider = cap("outsider");
    let team = host_store.create_team(Some(&creator)).unwrap();
    let other_team = host_store.create_team(Some(&outsider)).unwrap();

    let guest_a = GuestNode::new(
        root.path(),
        "ga.db",
        &host_store,
        &team.id,
        Access::Host(&creator),
        "alpha",
        10.0,
    );
    let guest_b = GuestNode::new(
        root.path(),
        "gb.db",
        &host_store,
        &team.id,
        Access::Host(&creator),
        "bravo",
        20.0,
    );
    let ungranted = GuestNode::new(
        root.path(),
        "gc.db",
        &host_store,
        &team.id,
        Access::Host(&creator),
        "charlie",
        30.0,
    );
    let foreign = host_store
        .reserve_worker(
            &other_team.id,
            Access::Host(&outsider),
            "outside",
            Tool::Claude,
            Placement {
                gateway: "gateway",
                workspace: "default",
                sandbox: "vm-out",
                incarnation: &uuid::Uuid::new_v4().to_string(),
            },
        )
        .unwrap()
        .binding;
    guest_a.refresh_directory(&host_store, 2);
    guest_b.refresh_directory(&host_store, 2);
    ungranted.refresh_directory(&host_store, 2);
    let _ = &foreign;

    // The creating host seeds the conversation with both guests through
    // explicit threaded sends; the deliveries mint guest evidence.
    let host_sender = crate::identity::resolve_from_name(&host_db, "host").unwrap();
    let grant_envelope = crate::messages::MessageEnvelope {
        thread: Some("work".into()),
        ..Default::default()
    };
    for guest in [&guest_a, &guest_b] {
        crate::sandbox::outgoing::queue(
            &host_db,
            &host_sender,
            &guest.worker,
            "seed",
            &grant_envelope,
            None,
        )
        .unwrap();
        let mut inbox = HostInbox::new(&host_db, &host_store, &guest.worker.binding);
        bridge_step(&host_store, guest, 3, &mut inbox, false, true);
    }
    let conversation: String = host_db
        .conn()
        .query_row(
            "SELECT id FROM sandbox_conversations WHERE label = 'work'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let evidence = |db: &HcomDb| -> i64 {
        db.conn()
            .query_row(
                "SELECT count(*) FROM sandbox_guest_conversations",
                [],
                |row| row.get(0),
            )
            .unwrap()
    };
    assert_eq!(evidence(&guest_a.db), 1);
    assert_eq!(evidence(&guest_b.db), 1);
    assert_eq!(evidence(&ungranted.db), 0);

    let host_events = || -> i64 {
        host_db
            .conn()
            .query_row(
                "SELECT count(*) FROM events WHERE type = 'message'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    };

    // An ungranted team guest cannot dispatch or fall through to any
    // ordinary membership.
    assert_eq!(
        run_cmd_send(
            &ungranted.db,
            "guest",
            &["send", "--thread", "work", "--", "guess"]
        ),
        1
    );
    assert_eq!(
        ungranted
            .store()
            .next_pending(&ungranted.worker.binding, now_epoch_i64())
            .unwrap(),
        None
    );

    // Guest bravo dispatches the retained thread; one immutable request is
    // queued for the host.
    assert_eq!(
        run_cmd_send(
            &guest_b.db,
            "guest",
            &["send", "--thread", "work", "--", "expanding"]
        ),
        0
    );
    let now = now_epoch_i64();
    let request = guest_b
        .store()
        .next_pending(&guest_b.worker.binding, now)
        .unwrap()
        .unwrap();
    assert_eq!(
        request.payload.conversation.as_deref(),
        Some(conversation.as_str())
    );
    assert_eq!(request.payload.thread.as_deref(), Some("work"));
    assert!(request.payload.target.is_empty());
    assert_eq!(request.payload.target_generation, None);
    assert_eq!(request.payload.sender_routing_generation, None);
    assert!(!request.payload.broadcast);
    let request_event_id: i64 = guest_b
        .db
        .conn()
        .query_row(
            "SELECT event_id FROM sandbox_outbox WHERE sequence = ?",
            [request.id.sequence],
            |row| row.get(0),
        )
        .unwrap();
    let request_journal = event(&guest_b.db, request_event_id);
    assert_eq!(
        request_journal["_sandbox"]["conversation"],
        json!(conversation)
    );

    // The host import resolves the grant and fans out atomically: the live
    // creator receives the ordinary event and alpha gets the verbatim
    // request over its own route.
    let before = host_events();
    let mut bravo_host_inbox = HostInbox::new(&host_db, &host_store, &guest_b.worker.binding);
    host_db
        .conn()
        .execute_batch(
            "CREATE TRIGGER reject_thread_forward BEFORE INSERT ON sandbox_outbox
         WHEN NEW.forwarded_sequence IS NOT NULL
         BEGIN SELECT RAISE(ABORT, 'thread forward fixture failure'); END;",
        )
        .unwrap();
    let rejected = host_store.import(
        Receiver::Host,
        &guest_b.worker.binding,
        &request,
        now_epoch_i64(),
        |tx| bravo_host_inbox.insert(tx, &request),
    );
    assert!(
        rejected
            .unwrap_err()
            .to_string()
            .contains("thread forward fixture failure")
    );
    assert_eq!(host_events(), before);
    assert!(
        host_store
            .next_pending(&guest_a.worker.binding, now_epoch_i64())
            .unwrap()
            .is_none()
    );
    host_db
        .conn()
        .execute_batch("DROP TRIGGER reject_thread_forward")
        .unwrap();
    bridge_step(
        &host_store,
        &guest_b,
        4,
        &mut bravo_host_inbox,
        false,
        false,
    );
    assert_eq!(host_events(), before + 1);
    let fanout: String = host_db
        .conn()
        .query_row(
            "SELECT data FROM events ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let fanout: Value = serde_json::from_str(&fanout).unwrap();
    assert_eq!(fanout["from"], json!(guest_b.worker.binding.worker.worker));
    assert_eq!(fanout["delivered_to"], json!(["host"]));
    assert_eq!(
        fanout["queued_to"],
        json!([guest_a.worker.binding.worker.worker])
    );
    assert_eq!(fanout["_sandbox"]["conversation"], json!(conversation));
    assert_eq!(fanout["_sandbox"]["id"], json!(request.id));
    let hops: i64 = host_db
        .conn()
        .query_row(
            "SELECT count(*) FROM sandbox_outbox WHERE accepted = 0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(hops, 1, "only the alpha hop is queued");

    // The grant-holding destination accepts the verbatim request; the wire
    // identity and digest survive the hop untouched, and no evidence is
    // minted by an expansion delivery.
    let mut host_inbox_a = HostInbox::new(&host_db, &host_store, &guest_a.worker.binding);
    bridge_step(&host_store, &guest_a, 5, &mut host_inbox_a, false, true);
    let alpha_event_id: i64 = guest_a
        .db
        .conn()
        .query_row(
            "SELECT event_id FROM sandbox_inbox WHERE worker = ? AND sequence = ?",
            rusqlite::params![request.id.origin.worker, request.id.sequence],
            |row| row.get(0),
        )
        .unwrap();
    let alpha_event = event(&guest_a.db, alpha_event_id);
    assert_eq!(alpha_event["_sandbox"]["id"], json!(request.id));
    assert_eq!(alpha_event["_sandbox"]["conversation"], json!(conversation));
    assert_eq!(alpha_event["thread"], json!("work"));
    let alpha_digest: Vec<u8> = guest_a
        .db
        .conn()
        .query_row(
            "SELECT digest FROM sandbox_inbox WHERE worker = ? AND sequence = ?",
            rusqlite::params![request.id.origin.worker, request.id.sequence],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(alpha_digest, request.digest.to_vec());
    assert_eq!(evidence(&guest_a.db), 1, "expansions mint no evidence");

    // Guest charlie holds no grant: the same expansion delivery is refused
    // at its inbox, and a replay on the host stays idempotent.
    let delivered = crate::sandbox::protocol::Message {
        digest: request.digest,
        id: request.id.clone(),
        payload: request.payload.clone(),
    };
    let charlie_store = ungranted.store();
    let charlie_failure = charlie_store
        .import(
            Receiver::Guest,
            &ungranted.worker.binding,
            &delivered,
            now_epoch_i64(),
            |tx| ungranted.inbox().insert(tx, &delivered),
        )
        .unwrap_err()
        .to_string();
    assert!(
        charlie_failure.contains("sandbox expansion delivery requires a retained grant"),
        "unexpected rejection: {charlie_failure}"
    );
    let replay = {
        let mut inbox = HostInbox::new(&host_db, &host_store, &guest_b.worker.binding);
        host_store
            .import(
                Receiver::Host,
                &guest_b.worker.binding,
                &request,
                now_epoch_i64(),
                |tx| inbox.insert(tx, &request),
            )
            .unwrap()
    };
    assert!(matches!(replay, crate::sandbox::store::Import::Replay(_)));
    assert_eq!(host_events(), before + 1);

    // Unknown or foreign scopes are rejected without a journal row, and a
    // guest cannot claim an addressed conversation grant.
    let forged_scope = crate::sandbox::protocol::Message::new(
        request.id.clone(),
        crate::sandbox::protocol::MessagePayload {
            conversation: Some("nonexistent".into()),
            ..request.payload.clone()
        },
    )
    .unwrap();
    let unknown = {
        let mut inbox = HostInbox::new(&host_db, &host_store, &guest_b.worker.binding);
        host_store
            .write(|tx| inbox.insert(tx, &forged_scope))
            .unwrap_err()
            .to_string()
    };
    assert!(
        unknown.contains("sandbox thread expansion is not granted for this source"),
        "unexpected rejection: {unknown}"
    );
    host_db
        .conn()
        .execute(
            "INSERT INTO sandbox_conversations (id, team, label) VALUES ('foreign', ?, 'work')",
            [&other_team.id],
        )
        .unwrap();
    let foreign_scope = crate::sandbox::protocol::Message::new(
        request.id.clone(),
        crate::sandbox::protocol::MessagePayload {
            conversation: Some("foreign".into()),
            ..request.payload.clone()
        },
    )
    .unwrap();
    let foreign = {
        let mut inbox = HostInbox::new(&host_db, &host_store, &guest_b.worker.binding);
        host_store
            .write(|tx| inbox.insert(tx, &foreign_scope))
            .unwrap_err()
            .to_string()
    };
    assert!(
        foreign.contains("sandbox thread expansion is not granted for this source"),
        "unexpected rejection: {foreign}"
    );
    let addressed_claim = crate::sandbox::protocol::Message::new(
        request.id.clone(),
        crate::sandbox::protocol::MessagePayload {
            conversation: Some(conversation.clone()),
            target: guest_a.worker.binding.worker.worker.clone(),
            target_generation: Some(guest_a.worker.binding.worker.generation.clone()),
            sender_routing_generation: Some("host-recipient".into()),
            ..request.payload.clone()
        },
    )
    .unwrap();
    let claimed = {
        let mut inbox = HostInbox::new(&host_db, &host_store, &guest_b.worker.binding);
        host_store
            .write(|tx| inbox.insert(tx, &addressed_claim))
            .unwrap_err()
            .to_string()
    };
    assert!(
        claimed.contains("guest cannot claim an addressed sandbox conversation"),
        "unexpected rejection: {claimed}"
    );
    assert_eq!(host_events(), before + 1);

    // A retired member is excluded from later expansions and its same-name
    // replacement never inherits the membership.
    host_store
        .retire_worker(Access::Host(&creator), &guest_a.worker.binding)
        .unwrap();
    let replacement = host_store
        .reserve_worker(
            &team.id,
            Access::Host(&creator),
            "alpha",
            Tool::Claude,
            Placement {
                gateway: "gateway",
                workspace: "default",
                sandbox: "vm-replacement",
                incarnation: &uuid::Uuid::new_v4().to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        replacement.binding.worker.worker,
        guest_a.worker.binding.worker.worker
    );
    assert_eq!(
        run_cmd_send(
            &guest_b.db,
            "guest",
            &["send", "--thread", "work", "--", "expanding again"]
        ),
        0
    );
    let mut bravo_host_inbox = HostInbox::new(&host_db, &host_store, &guest_b.worker.binding);
    bridge_step(
        &host_store,
        &guest_b,
        6,
        &mut bravo_host_inbox,
        false,
        false,
    );
    let fanout: String = host_db
        .conn()
        .query_row(
            "SELECT data FROM events ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let fanout: Value = serde_json::from_str(&fanout).unwrap();
    assert_eq!(fanout["delivered_to"], json!(["host"]));
    assert!(
        fanout["queued_to"].is_null(),
        "stale and replacement members are excluded: {fanout}"
    );
}
