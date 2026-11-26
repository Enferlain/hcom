use super::*;
use crate::sandbox::admission::Budget;
use crate::sandbox::protocol::Intent;

const NOW: i64 = 1_000;

fn payload(text: &str) -> MessagePayload {
    MessagePayload {
        target: "worker-b".into(),
        target_generation: None,
        sender_routing_generation: None,
        broadcast: false,
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

fn incoming_binding(message: &Message) -> Binding {
    Binding {
        team: "team".into(),
        sandbox: "sandbox-a".into(),
        worker: message.id.origin.clone(),
    }
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
        NOW,
        |tx, _| {
            insert_event(tx)?;
            anyhow::bail!("simulate failure after event insert")
        },
    );
    assert!(fail.is_err());
    assert_eq!(event_count(&db), 0);
    assert!(store.next_pending(&route, NOW).unwrap().is_none());
    let message = store
        .enqueue(
            Receiver::Guest,
            &route,
            origin,
            payload("durable"),
            NOW,
            |tx, _| insert_event(tx),
        )
        .unwrap();
    assert_eq!(message.id.sequence, 1);
    assert_eq!(event_count(&db), 1);
    // Simulate reopening a queue created before foreign forwarding existed.
    db.conn()
        .execute_batch(
            "DROP INDEX sandbox_outbox_forwarded;
         ALTER TABLE sandbox_outbox DROP COLUMN forwarded_sequence;
         ALTER TABLE sandbox_outbox DROP COLUMN forwarded_incarnation;
         ALTER TABLE sandbox_outbox DROP COLUMN rejected;",
        )
        .unwrap();
    drop(db);

    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::initialize(&db).unwrap();
    let routes = store.pending_routes(NOW).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].as_ref().unwrap(), &route);
    assert_eq!(
        store.next_pending(&route, NOW).unwrap(),
        Some(message.clone())
    );
    let mut wrong_route = route.clone();
    wrong_route.team = "other".into();
    assert!(
        store
            .acknowledge(&wrong_route, &message.id, &message.digest, NOW)
            .is_err()
    );
    assert!(
        store
            .acknowledge(&route, &message.id, &[0; 32], NOW)
            .is_err()
    );
    let mut wrong_id = message.id.clone();
    wrong_id.origin.worker = "other-worker".into();
    assert!(
        store
            .acknowledge(&route, &wrong_id, &message.digest, NOW)
            .is_err()
    );
    assert_eq!(store.delivery(&wrong_id, NOW).unwrap(), None);
    assert!(store.next_pending(&route, NOW).unwrap().is_some());
    store
        .acknowledge(&route, &message.id, &message.digest, NOW)
        .unwrap();
    store
        .acknowledge(&route, &message.id, &message.digest, NOW)
        .unwrap();
    assert!(store.next_pending(&route, NOW).unwrap().is_none());
    assert_eq!(event_count(&db), 1);
}

#[test]
fn foreign_forwarding_keeps_the_wire_id_and_orders_with_local_sends() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("hcom.db");
    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::initialize(&db).unwrap();
    let source = origin("worker-a", &Uuid::new_v4().to_string());
    // Both senders can produce the same sequence number. Their wire origins
    // also differ from the host's queue order.
    let remote = Message::new(
        MessageId {
            origin: source,
            sequence: 1,
        },
        payload("remote"),
    )
    .unwrap();
    let source_binding = incoming_binding(&remote);
    let destination = Binding {
        team: source_binding.team.clone(),
        sandbox: "sandbox-b".into(),
        worker: origin("worker-b", &Uuid::new_v4().to_string()),
    };
    let hop = ForwardRoute {
        source: &source_binding,
        destination: &destination,
    };
    let local = store
        .enqueue(
            Receiver::Guest,
            &destination,
            origin("host", store.incarnation()),
            payload("local"),
            NOW,
            |tx, _| insert_event(tx),
        )
        .unwrap();
    assert_eq!(local.id.sequence, remote.id.sequence);

    let failed = store.import(Receiver::Host, &source_binding, &remote, NOW, |tx| {
        let event = insert_event(tx)?;
        store.forward_in(tx, hop, &remote, event, NOW)?;
        anyhow::bail!("interrupt before commit")
    });
    assert!(failed.is_err());
    assert_eq!(event_count(&db), 1);
    store
        .acknowledge(&destination, &local.id, &local.digest, NOW)
        .unwrap();
    assert!(store.next_pending(&destination, NOW).unwrap().is_none());

    let received = store
        .import(Receiver::Host, &source_binding, &remote, NOW, |tx| {
            let event = insert_event(tx)?;
            let cross_team = Binding {
                team: "other".into(),
                ..destination.clone()
            };
            assert!(
                store
                    .forward_in(
                        tx,
                        ForwardRoute {
                            source: &source_binding,
                            destination: &cross_team,
                        },
                        &remote,
                        event,
                        NOW
                    )
                    .is_err()
            );
            let mut wrong_worker = destination.clone();
            wrong_worker.worker.worker = "not-the-message-target".into();
            assert!(
                store
                    .forward_in(
                        tx,
                        ForwardRoute {
                            source: &source_binding,
                            destination: &wrong_worker,
                        },
                        &remote,
                        event,
                        NOW,
                    )
                    .is_err()
            );
            store.forward_in(tx, hop, &remote, event, NOW)?;
            Ok(event)
        })
        .unwrap();
    let Import::Inserted(event) = received else {
        panic!("first import should insert a local event");
    };
    assert_eq!(
        store
            .import(Receiver::Host, &source_binding, &remote, NOW, |_| {
                panic!("replay must not forward again")
            })
            .unwrap(),
        Import::Replay(event)
    );
    drop(db);

    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::initialize(&db).unwrap();
    let before_ack = store.next_pending(&destination, NOW).unwrap();
    assert_eq!(before_ack, Some(remote.clone()));
    let mut forged = remote.id.clone();
    forged.origin.incarnation = store.incarnation().into();
    assert!(
        store
            .acknowledge(&destination, &forged, &remote.digest, NOW)
            .is_err()
    );
    assert!(
        store
            .acknowledge(&source_binding, &remote.id, &remote.digest, NOW)
            .is_err()
    );
    store
        .acknowledge(&destination, &remote.id, &remote.digest, NOW)
        .unwrap();
    assert!(store.next_pending(&destination, NOW).unwrap().is_none());
    assert_eq!(event_count(&db), 2);
}

#[test]
fn terminal_rejection_rolls_back_callback_writes_and_charges_pacing_once() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let limits = Limits {
        pace: Budget {
            messages: 1,
            bytes: 1024 * 1024,
        },
        ..Limits::default()
    };
    let store = Store::with_limits(&db, limits).unwrap();
    let origin = origin("worker-a", &Uuid::new_v4().to_string());
    let make = |sequence, text| {
        Message::new(
            MessageId {
                origin: origin.clone(),
                sequence,
            },
            payload(text),
        )
        .unwrap()
    };
    let first = make(1, "first");
    let second = make(2, "second");
    let binding = incoming_binding(&first);
    let reject = |tx: &Transaction<'_>| {
        insert_event(tx)?;
        Err(UnavailableRecipient("fixture rejection after a tentative event").into())
    };
    let import = |msg, now| store.import(Receiver::Host, &binding, msg, now, reject);

    assert_eq!(import(&first, NOW).unwrap(), Import::Rejected);
    assert_eq!(event_count(&db), 0);
    assert_eq!(
        store
            .import(Receiver::Host, &binding, &first, NOW, |_| panic!(
                "replay must not recheck the recipient"
            ))
            .unwrap(),
        Import::Rejected,
    );
    assert_error(import(&second, NOW), "guest pacing");
    assert_eq!(event_count(&db), 0);
    assert_error(
        import(&make(1, "different digest"), NOW),
        "conflicting sandbox rejection",
    );
    // A paced-out sequence leaves no rejection. It can be accepted when credit returns.
    assert!(matches!(
        store
            .import(
                Receiver::Host,
                &binding,
                &second,
                NOW + limits.pace_seconds,
                insert_event
            )
            .unwrap(),
        Import::Inserted(_),
    ));
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
            .import(
                Receiver::Host,
                &incoming_binding(&message),
                &message,
                NOW,
                |tx| {
                    insert_event(tx)?;
                    anyhow::bail!("simulate failure before mapping")
                }
            )
            .is_err()
    );
    assert_eq!(event_count(&db), 0);
    let Import::Inserted(event) = store
        .import(
            Receiver::Host,
            &incoming_binding(&message),
            &message,
            NOW,
            insert_event,
        )
        .unwrap()
    else {
        panic!("expected new event")
    };
    drop(db);

    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::initialize(&db).unwrap();
    let no_write = |_: &Transaction<'_>| panic!("must never insert a second event");
    assert_eq!(
        store
            .import(
                Receiver::Host,
                &incoming_binding(&message),
                &message,
                NOW,
                no_write
            )
            .unwrap(),
        Import::Replay(event)
    );
    let conflict = Message::new(id.clone(), payload("conflict")).unwrap();
    assert!(
        store
            .import(
                Receiver::Host,
                &incoming_binding(&conflict),
                &conflict,
                NOW,
                no_write
            )
            .is_err()
    );
    let mut replaced = id;
    replaced.origin.incarnation = Uuid::new_v4().to_string();
    let replaced = Message::new(replaced, payload("replacement")).unwrap();
    assert!(
        store
            .import(
                Receiver::Host,
                &incoming_binding(&replaced),
                &replaced,
                NOW,
                no_write
            )
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
            .import(
                Receiver::Host,
                &incoming_binding(&message),
                &message,
                NOW,
                |_| panic!("must reject stale database before writing")
            )
            .unwrap_err()
            .to_string()
            .contains("database was replaced")
    );
}

#[test]
fn queue_saturation_preserves_sends_until_expiry_and_reopen_keeps_failure() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("hcom.db");
    let db = HcomDb::open_at(&path).unwrap();
    let limits = Limits {
        pending: Budget {
            messages: 1,
            bytes: 4096,
        },
        run: Budget {
            messages: 2,
            bytes: 8192,
        },
        queue_ttl_seconds: 5,
        ..Limits::default()
    };
    let store = Store::with_limits(&db, limits).unwrap();
    let route = Binding {
        team: "team".into(),
        sandbox: "sandbox-b".into(),
        worker: origin("worker-b", &Uuid::new_v4().to_string()),
    };
    let enqueue = |store: &Store<'_>, text: &str, now| {
        store.enqueue(
            Receiver::Guest,
            &route,
            origin("worker-a", store.incarnation()),
            payload(text),
            now,
            |tx, _| insert_event(tx),
        )
    };
    assert_error(enqueue(&store, &"x".repeat(4200), NOW), "pending queue");
    assert_eq!(event_count(&db), 0);
    let first = enqueue(&store, "first", NOW).unwrap();
    assert_error(enqueue(&store, "second", NOW), "pending queue");
    assert_eq!(event_count(&db), 1);
    assert_eq!(
        store.next_pending(&route, NOW).unwrap(),
        Some(first.clone())
    );
    assert!(store.next_pending(&route, NOW + 5).unwrap().is_none());
    assert_eq!(
        store.delivery(&first.id, NOW + 5).unwrap(),
        Some(Delivery::AwaitingExpiry)
    );
    assert_error(
        store.acknowledge(&route, &first.id, &first.digest, NOW + 5),
        "receipt",
    );
    let failures = store.expire_pending(NOW + 5).unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].sequence, first.id.sequence);
    assert!(failures[0].event_id > 0);
    assert!(store.expire_pending(NOW + 5).unwrap().is_empty());
    assert_error(
        store.acknowledge(&route, &first.id, &first.digest, NOW + 6),
        "receipt",
    );
    let second = enqueue(&store, "second", NOW + 5).unwrap();
    assert_eq!(second.id.sequence, 2);
    store
        .acknowledge(&route, &second.id, &second.digest, NOW + 6)
        .unwrap();
    assert_error(enqueue(&store, "third", NOW + 6), "run quota");
    assert_eq!(event_count(&db), 2);
    drop(db);

    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::with_limits(&db, limits).unwrap();
    assert_eq!(
        store.delivery(&first.id, NOW + 30).unwrap(),
        Some(Delivery::Expired)
    );
    assert_eq!(
        store.delivery(&second.id, NOW + 30).unwrap(),
        Some(Delivery::Accepted)
    );
    assert_error(enqueue(&store, "third", NOW + 30), "run quota");
}

fn assert_error<T: std::fmt::Debug>(result: Result<T>, expected: &str) {
    let error = result.unwrap_err().to_string();
    assert!(error.contains(expected), "{error}");
}

#[test]
fn guest_pacing_team_and_host_ceilings_survive_reopen_and_replay() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("hcom.db");
    let db = HcomDb::open_at(&path).unwrap();
    let limits = Limits {
        pace: Budget {
            messages: 1,
            bytes: 6000,
        },
        pace_seconds: 10,
        team: Budget {
            messages: 2,
            bytes: 8192,
        },
        host: Budget {
            messages: 4,
            bytes: 8192,
        },
        ..Limits::default()
    };
    let store = Store::with_limits(&db, limits).unwrap();
    let mut route_a = incoming_binding(
        &Message::new(
            MessageId {
                origin: origin("worker-a", &Uuid::new_v4().to_string()),
                sequence: 1,
            },
            payload("a"),
        )
        .unwrap(),
    );
    let import = |store: &Store<'_>, binding: &Binding, sequence, now, text: &str| {
        let message = Message::new(
            MessageId {
                origin: binding.worker.clone(),
                sequence,
            },
            payload(text),
        )
        .unwrap();
        store.import(Receiver::Host, binding, &message, now, insert_event)
    };
    let Import::Inserted(event_id) = import(&store, &route_a, 1, NOW, "a").unwrap() else {
        panic!("expected first insert")
    };
    assert_error(import(&store, &route_a, 2, NOW, "b"), "guest pacing");
    drop(db);

    let db = HcomDb::open_at(&path).unwrap();
    let store = Store::with_limits(&db, limits).unwrap();
    assert_eq!(
        import(&store, &route_a, 1, NOW + 1, "a").unwrap(),
        Import::Replay(event_id)
    );
    assert_error(import(&store, &route_a, 2, NOW + 1, "b"), "guest pacing");
    assert_error(import(&store, &route_a, 2, NOW - 1, "b"), "guest pacing");
    import(&store, &route_a, 2, NOW + 10, "b").unwrap();
    let mut route_b = route_a.clone();
    route_b.sandbox = "sandbox-b".into();
    route_b.worker.worker = "worker-b".into();
    assert_error(import(&store, &route_b, 1, NOW + 10, "c"), "team quota");
    route_b.team = "team-b".into();
    import(&store, &route_b, 1, NOW + 10, &"z".repeat(3000)).unwrap();
    route_a.team = "team-a2".into();
    assert_error(
        import(&store, &route_a, 3, NOW + 20, &"z".repeat(4000)),
        "host quota",
    );
    assert_eq!(event_count(&db), 3);
}

#[test]
fn nonpositive_budget_does_not_initialize_sandbox_schema() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let mut limits = Limits::default();
    limits.pace.bytes = 0;
    assert!(Store::with_limits(&db, limits).is_err());
    let tables: i64 = db
        .conn()
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name LIKE 'sandbox_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tables, 0);
}

#[test]
fn concurrent_inputs_cannot_both_spend_one_team_slot() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("hcom.db");
    let db = HcomDb::open_at(&path).unwrap();
    let limits = Limits {
        team: Budget {
            messages: 1,
            bytes: 8192,
        },
        ..Limits::default()
    };
    let _store = Store::with_limits(&db, limits).unwrap();
    let barrier = std::sync::Barrier::new(2);
    let insert = |worker: &str| {
        let other_db = HcomDb::open_at(&path);
        barrier.wait();
        let other_db = other_db?;
        let store = Store::with_limits(&other_db, limits)?;
        let message = Message::new(
            MessageId {
                origin: origin(worker, &Uuid::new_v4().to_string()),
                sequence: 1,
            },
            payload("concurrent"),
        )
        .unwrap();
        let mut binding = incoming_binding(&message);
        binding.sandbox = worker.into();
        store.import(Receiver::Host, &binding, &message, NOW, insert_event)
    };
    let (a, b) = std::thread::scope(|scope| {
        let a = scope.spawn(|| insert("worker-a"));
        let b = scope.spawn(|| insert("worker-b"));
        (a.join().unwrap(), b.join().unwrap())
    });
    match (a, b) {
        (Ok(Import::Inserted(_)), duplicate @ Err(_))
        | (duplicate @ Err(_), Ok(Import::Inserted(_))) => assert_error(duplicate, "team quota"),
        outcomes => panic!("expected one accepted input, got {outcomes:?}"),
    }
    assert_eq!(event_count(&db), 1);
}

#[test]
fn unreadable_queue_data_does_not_block_recovery_or_expiry() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let limits = Limits {
        queue_ttl_seconds: 5,
        ..Limits::default()
    };
    let store = Store::with_limits(&db, limits).unwrap();
    let mut route = Binding {
        team: "team".into(),
        sandbox: "sandbox-a".into(),
        worker: origin("worker-b", &Uuid::new_v4().to_string()),
    };
    let sender = origin("worker-a", store.incarnation());
    let queue = |route: &Binding| {
        store
            .enqueue(
                Receiver::Guest,
                route,
                sender.clone(),
                payload("pending"),
                NOW,
                |tx, _| insert_event(tx),
            )
            .unwrap()
    };
    let broken = queue(&route);
    route.sandbox = "sandbox-b".into();
    let intact = queue(&route);
    db.conn().execute(
        "UPDATE sandbox_outbox SET route = 'unreadable', message = 'unreadable' WHERE sequence = ?",
        [broken.id.sequence],
    ).unwrap();
    let routes = store.pending_routes(NOW).unwrap();
    let error = routes
        .iter()
        .find_map(|route| route.as_ref().err())
        .unwrap();
    assert!(
        error
            .to_string()
            .contains(&format!("sequence {}", broken.id.sequence))
    );
    assert_eq!(routes.iter().filter(|route| route.is_err()).count(), 1);
    assert_eq!(
        routes
            .iter()
            .filter_map(|route| route.as_ref().ok())
            .collect::<Vec<_>>(),
        vec![&route]
    );
    assert_eq!(store.next_pending(&route, NOW).unwrap(), Some(intact));
    let mut sequences: Vec<_> = store
        .expire_pending(NOW + 5)
        .unwrap()
        .into_iter()
        .map(|failure| failure.sequence)
        .collect();
    sequences.sort_unstable();
    assert_eq!(sequences, vec![1, 2]);
    assert!(store.pending_routes(NOW).unwrap().is_empty());
    assert_eq!(
        store.delivery(&broken.id, NOW + 5).unwrap(),
        Some(Delivery::Expired)
    );
}

#[test]
fn expiry_drains_bounded_batches() {
    let temp = tempfile::tempdir().unwrap();
    let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
    let limits = Limits {
        queue_ttl_seconds: 5,
        ..Limits::default()
    };
    let store = Store::with_limits(&db, limits).unwrap();
    let route = Binding {
        team: "team".into(),
        sandbox: "sandbox-a".into(),
        worker: origin("worker-b", &Uuid::new_v4().to_string()),
    };
    let sender = origin("worker-a", store.incarnation());
    for _ in 0..65 {
        store
            .enqueue(
                Receiver::Guest,
                &route,
                sender.clone(),
                payload("pending"),
                NOW,
                |tx, _| insert_event(tx),
            )
            .unwrap();
    }
    assert_eq!(store.expire_pending(NOW + 5).unwrap().len(), 64);
    assert_eq!(store.expire_pending(NOW + 5).unwrap().len(), 1);
    assert!(store.expire_pending(NOW + 5).unwrap().is_empty());
}
