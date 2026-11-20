use super::*;
use crate::sandbox::protocol::Intent;

fn payload(text: &str) -> MessagePayload {
    MessagePayload {
        target: "worker-b".into(),
        thread: None,
        reply_to: None,
        intent: Intent::Inform,
        text: text.into(),
    }
}

fn origin(worker: &str, incarnation: &str) -> Origin {
    Origin {
        worker: worker.into(),
        generation: "generation-1".into(),
        incarnation: incarnation.into(),
    }
}

#[test]
fn optional_schema_is_idempotent_and_preserves_user_version() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let version = || {
        db.conn()
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap()
    };
    let before = version();
    let store = Store::initialize(&db).unwrap();
    assert_eq!(
        Store::initialize(&db).unwrap().incarnation(),
        store.incarnation()
    );
    assert_eq!(version(), before);
}

fn event_count(db: &HcomDb) -> i64 {
    db.conn()
        .query_row(
            "SELECT count(*) FROM events WHERE type = 'sandbox-test'",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn insert_event(tx: &Transaction<'_>) -> Result<i64> {
    tx.execute(
        "INSERT INTO events (timestamp, type, instance, data)
                VALUES ('2026-11-21T00:00:00Z', 'sandbox-test', 'worker-a', '{}')",
        [],
    )?;
    Ok(tx.last_insert_rowid())
}

#[test]
fn enqueue_and_receipt_are_atomic_and_survive_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("hcom.db");
    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::initialize(&db).unwrap();
    let origin = origin("worker-a", store.incarnation());
    let route = Binding {
        team: "team".into(),
        sandbox: "sandbox-b".into(),
        worker: self::origin("worker-b", &Uuid::new_v4().to_string()),
    };
    let fail = store.enqueue(
        Receiver::Guest,
        &route,
        origin.clone(),
        payload("rollback"),
        |tx, _| {
            insert_event(tx)?;
            anyhow::bail!("simulate failure after event insert")
        },
    );
    assert!(fail.is_err());
    assert_eq!(event_count(&db), 0);
    assert!(store.next_pending(&route).unwrap().is_none());
    let message = store
        .enqueue(
            Receiver::Guest,
            &route,
            origin,
            payload("durable"),
            |tx, _| insert_event(tx),
        )
        .unwrap();
    assert_eq!(message.id.sequence, 1);
    assert_eq!(event_count(&db), 1);
    drop(db);

    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::initialize(&db).unwrap();
    assert_eq!(store.next_pending(&route).unwrap(), Some(message.clone()));
    let mut wrong_route = route.clone();
    wrong_route.team = "other".into();
    assert!(
        store
            .acknowledge(&wrong_route, &message.id, &message.digest)
            .is_err()
    );
    assert!(store.acknowledge(&route, &message.id, &[0; 32]).is_err());
    assert!(store.next_pending(&route).unwrap().is_some());
    store
        .acknowledge(&route, &message.id, &message.digest)
        .unwrap();
    store
        .acknowledge(&route, &message.id, &message.digest)
        .unwrap();
    assert!(store.next_pending(&route).unwrap().is_none());
    assert_eq!(event_count(&db), 1);
}

#[test]
fn imported_event_and_key_rollback_together_and_replay_once() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("hcom.db");
    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::initialize(&db).unwrap();
    let id = MessageId {
        origin: origin("worker-a", &Uuid::new_v4().to_string()),
        sequence: 1,
    };
    let message = Message::new(id.clone(), payload("received")).unwrap();
    assert!(
        store
            .import(&message, |tx| {
                insert_event(tx)?;
                anyhow::bail!("simulate failure before mapping")
            })
            .is_err()
    );
    assert_eq!(event_count(&db), 0);
    let Import::Inserted(event) = store.import(&message, insert_event).unwrap() else {
        panic!("expected new event")
    };
    drop(db);

    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::initialize(&db).unwrap();
    let no_write = |_: &Transaction<'_>| panic!("must never insert a second event");
    assert_eq!(
        store.import(&message, no_write).unwrap(),
        Import::Replay(event)
    );
    let conflict = Message::new(id.clone(), payload("conflict")).unwrap();
    assert!(store.import(&conflict, no_write).is_err());
    let mut replaced = id;
    replaced.origin.incarnation = Uuid::new_v4().to_string();
    let replaced = Message::new(replaced, payload("replacement")).unwrap();
    assert!(
        store
            .import(&replaced, no_write)
            .unwrap_err()
            .to_string()
            .contains("incarnation")
    );
    assert_eq!(event_count(&db), 1);
}

#[cfg(unix)]
#[test]
fn active_store_rejects_replaced_database() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("hcom.db");
    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::initialize(&db).unwrap();
    let message = Message::new(
        MessageId {
            origin: origin("worker-a", &Uuid::new_v4().to_string()),
            sequence: 1,
        },
        payload("stale"),
    )
    .unwrap();
    std::fs::rename(&path, temp.path().join("old.db")).unwrap();
    std::fs::File::create(&path).unwrap();
    assert!(
        store
            .import(&message, |_| panic!(
                "must reject stale database before writing"
            ))
            .unwrap_err()
            .to_string()
            .contains("database was replaced")
    );
}
