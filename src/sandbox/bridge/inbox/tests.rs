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
            thread: Some("thread".into()),
            reply_to: None,
            intent: Intent::Request,
            text: "hello".into(),
        },
    )
    .unwrap()
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
    let input = message(sender, 1, &binding.worker.worker);
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
            "INSERT INTO instances (name, created_at) VALUES ('host', 1), ('outsider', 2);",
        )
        .unwrap();
    let identity = crate::identity::resolve_from_name(&db, "host").unwrap();
    let member = HostIdentity::capture(&db, &identity).unwrap();
    let store = Store::initialize(&db).unwrap();
    let team = store.create_team(Some(&member)).unwrap();
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
    let mut inbox = HostInbox::new(&db, &store, &source);

    let local = message(source.worker.clone(), 1, "host");
    let Imported::New(id) = apply(&store, Receiver::Host, &source, &local, &mut inbox) else {
        panic!("expected local import")
    };
    assert_eq!(event(&db, id)["mentions"], json!(["host"]));

    let forwarded = message(source.worker.clone(), 2, &destination.worker.worker);
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
        Some(forwarded)
    );

    let unauthorized = message(source.worker.clone(), 3, "outsider");
    assert!(
        store
            .import(
                Receiver::Host,
                &source,
                &unauthorized,
                now_epoch_i64(),
                |tx| inbox.insert(tx, &unauthorized)
            )
            .is_err()
    );
}
