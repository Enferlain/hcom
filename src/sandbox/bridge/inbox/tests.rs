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

    let reject = |inbox: &mut HostInbox<'_, '_>, message: &Message| {
        store
            .import(Receiver::Host, &source, message, now_epoch_i64(), |tx| {
                inbox.insert(tx, message)
            })
            .unwrap_err()
            .to_string()
    };
    let before = db.get_last_event_id();
    let unauthorized = addressed(
        message(source.worker.clone(), 3, "outsider"),
        &host_recipient_generation(db.conn(), "outsider", 2.0).unwrap(),
    );
    assert!(reject(&mut inbox, &unauthorized).contains("outside the team"));
    let unbound = message(source.worker.clone(), 3, "member");
    assert!(reject(&mut inbox, &unbound).contains("requires a target generation"));

    db.conn()
        .execute(
            "UPDATE instances SET created_at = 4 WHERE name = 'member'",
            [],
        )
        .unwrap();
    enroll_local();
    let stale_local = addressed(
        message(source.worker.clone(), 3, "member"),
        &host_recipient_generation(db.conn(), "member", 3.0).unwrap(),
    );
    assert!(reject(&mut inbox, &stale_local).contains("target generation is stale"));

    store
        .retire_worker(Access::Host(&member), &destination)
        .unwrap();
    let replacement = make_worker("destination").binding;
    assert_eq!(replacement.worker.worker, destination.worker.worker);
    assert_ne!(replacement.worker.generation, destination.worker.generation);
    let stale_guest = addressed(
        message(source.worker.clone(), 3, &destination.worker.worker),
        &destination.worker.generation,
    );
    assert!(reject(&mut inbox, &stale_guest).contains("target generation is stale"));
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
