use super::*;
use crate::db::HcomDb;
use crate::messages::MessageEnvelope;
use crate::sandbox::directory::{Access, HostIdentity, Placement, Worker};
use crate::sandbox::store::Store;
use crate::shared::SenderIdentity;

const GATEWAY_INCARNATION: &str = "fedc8acb-cc62-422d-9724-54e730e64f6e";

struct Fixture {
    _root: tempfile::TempDir,
    db: HcomDb,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&root.path().join("hcom.db")).unwrap();
        Self { _root: root, db }
    }

    fn host(&self, name: &str, created: f64) -> HostIdentity {
        self.db
            .conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES (?, ?)",
                params![name, created],
            )
            .unwrap();
        HostIdentity::capture(&self.db, &self.sender(name)).unwrap()
    }

    fn sender(&self, name: &str) -> SenderIdentity {
        crate::identity::resolve_from_name(&self.db, name).unwrap()
    }

    fn store(&self) -> Store<'_> {
        Store::initialize(&self.db).unwrap()
    }

    fn reserve(&self, team: &str, host: &HostIdentity, alias: &str, sandbox: &str) -> Worker {
        self.store()
            .reserve_worker(
                team,
                Access::Host(host),
                alias,
                crate::tool::Tool::Claude,
                Placement {
                    gateway: "gateway",
                    workspace: "default",
                    sandbox,
                    incarnation: GATEWAY_INCARNATION,
                },
            )
            .unwrap()
    }
}

fn envelope(thread: Option<&str>) -> MessageEnvelope {
    MessageEnvelope {
        thread: thread.map(str::to_owned),
        ..Default::default()
    }
}

fn conversation_members(db: &HcomDb) -> Vec<(String, String, Option<f64>)> {
    let mut stmt = db
        .conn()
        .prepare(
            "SELECT m.name, m.generation, m.host_created_at
             FROM sandbox_conversation_members m
             JOIN sandbox_conversations c ON c.id = m.conversation
             WHERE c.label = 'work' ORDER BY m.name",
        )
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn conversation_id(db: &HcomDb) -> String {
    db.conn()
        .query_row(
            "SELECT id FROM sandbox_conversations WHERE label = 'work'",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn ordinary_membership(db: &HcomDb, thread: &str, caller: &str) {
    db.kv_set(
        &format!("events_sub:ordinary-{thread}-{caller}"),
        Some(
            &serde_json::json!({
                "id": format!("ordinary-{thread}-{caller}"),
                "caller": caller,
                "thread_name": thread,
                "auto_thread_member": true,
                "delivery_only": true,
                "sql": "0",
                "created": 1.0,
                "last_id": 0,
                "once": false,
            })
            .to_string(),
        ),
    )
    .unwrap();
}

/// Seeding, expansion, thread-only fan-out, and the resulting wire identity.
#[test]
fn thread_sends_seed_expand_and_fan_out_to_current_participants() {
    let fixture = Fixture::new();
    let luna = fixture.host("luna", 1.0);
    let sender = fixture.sender("luna");
    let store = fixture.store();
    let team = store.create_team(Some(&luna)).unwrap();
    let alpha = fixture.reserve(&team.id, &luna, "alpha", "vm-1");
    let beta = fixture.reserve(&team.id, &luna, "beta", "vm-2");

    // The explicit threaded send seeds the host sender and that guest.
    let seeded = crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &alpha,
        "seed",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    assert_eq!(conversation_members(&fixture.db).len(), 2);
    let conversation = conversation_id(&fixture.db);
    let event: String = fixture
        .db
        .conn()
        .query_row(
            "SELECT data FROM events WHERE id = ?",
            [seeded.event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&event).unwrap()["_sandbox"]["conversation"],
        serde_json::json!(conversation)
    );

    // A current participant adds a same-team named peer.
    crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &beta,
        "expand",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    // Settle the two directed sends so the route heads are the fan-out hops.
    let now = crate::shared::time::now_epoch_i64();
    for worker in [&alpha, &beta] {
        let pending = store.next_pending(&worker.binding, now).unwrap().unwrap();
        store
            .acknowledge(&worker.binding, &pending.id, &pending.digest, now)
            .unwrap();
    }
    assert_eq!(conversation_members(&fixture.db).len(), 3);

    // Thread-only fan-out reaches the current guests in one atomic batch.
    let resolved = resolve(&fixture.db, &sender, "work").unwrap().unwrap();
    assert_eq!(resolved.team, team.id);
    let queued = queue(
        &fixture.db,
        &sender,
        &resolved,
        "hello thread",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    let mut targets = queued.queued_to.clone();
    targets.sort();
    let mut expected = vec![
        alpha.binding.worker.worker.clone(),
        beta.binding.worker.worker.clone(),
    ];
    expected.sort();
    assert_eq!(targets, expected);

    let event: String = fixture
        .db
        .conn()
        .query_row(
            "SELECT data FROM events WHERE id = ?",
            [queued.event_id],
            |row| row.get(0),
        )
        .unwrap();
    let data: serde_json::Value = serde_json::from_str(&event).unwrap();
    assert_eq!(data["thread"], json!("work"));
    assert_eq!(data["mentions"], json!([]));
    assert_eq!(data["delivered_to"], json!([]));
    assert_eq!(data["queued_to"], serde_json::json!(queued.queued_to));
    assert_eq!(data["_sandbox"]["team"], json!(team.id));
    assert_eq!(data["_sandbox"]["conversation"], json!(conversation));
    let wire_id = data["_sandbox"]["id"].clone();

    // Both hops share the event's stable wire identity with their own digest.
    let mut identities = Vec::new();
    for worker in [&alpha, &beta] {
        let message = store
            .next_pending(&worker.binding, crate::shared::time::now_epoch_i64())
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(message.id.clone()).unwrap(),
            wire_id,
            "every destination sees the same stable message id"
        );
        assert_eq!(message.payload.thread.as_deref(), Some("work"));
        assert!(message.payload.sender_routing_generation.is_some());
        assert!(
            message
                .payload
                .target_generation
                .as_deref()
                .is_some_and(|generation| { generation == worker.binding.worker.generation })
        );
        assert_eq!(
            store
                .delivery(&message.id, crate::shared::time::now_epoch_i64())
                .unwrap(),
            None,
            "a fan-out wire id has no single-route delivery status"
        );
        identities.push(store.pending_routes(0).unwrap().len());
    }
    assert_eq!(identities, vec![2, 2]);
    let (rows, events): (i64, i64) = fixture
        .db
        .conn()
        .query_row(
            "SELECT (SELECT count(*) FROM sandbox_outbox),
                    (SELECT count(*) FROM events WHERE type = 'message')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    // Two directed single hops plus two fan-out hops.
    assert_eq!(rows, 4);
    assert_eq!(events, 3);

    // Sandbox thread names never enter the ordinary membership index.
    assert!(fixture.db.get_thread_members("work").is_empty());
}

/// Retired generations lose their grant; a reused canonical name must be
/// explicitly re-seeded before it participates again.
#[test]
fn retired_generations_leave_reused_names_ungranted() {
    let fixture = Fixture::new();
    let luna = fixture.host("luna", 1.0);
    let sender = fixture.sender("luna");
    let store = fixture.store();
    let team = store.create_team(Some(&luna)).unwrap();
    let old = fixture.reserve(&team.id, &luna, "alpha", "vm-1");
    crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &old,
        "seed",
        &envelope(Some("work")),
        None,
    )
    .unwrap();

    // The name is reused by a fresh generation of the same canonical name.
    store
        .retire_worker(Access::Host(&luna), &old.binding)
        .unwrap();
    let replacement = fixture.reserve(&team.id, &luna, "alpha", "vm-2");
    assert_eq!(old.binding.worker.worker, replacement.binding.worker.worker);
    assert_ne!(
        old.binding.worker.generation,
        replacement.binding.worker.generation
    );

    // Old history grants the replacement nothing: the only other member is
    // stale, so the fan-out refuses rather than delivering to the dead name.
    let resolved = resolve(&fixture.db, &sender, "work").unwrap().unwrap();
    let before: i64 = fixture
        .db
        .conn()
        .query_row("SELECT count(*) FROM sandbox_outbox", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    let failed = queue(
        &fixture.db,
        &sender,
        &resolved,
        "hello",
        &envelope(Some("work")),
        None,
    );
    assert!(
        failed
            .unwrap_err()
            .to_string()
            .contains("no other current participants")
    );
    let after: i64 = fixture
        .db
        .conn()
        .query_row("SELECT count(*) FROM sandbox_outbox", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    assert_eq!(after, before, "the refused fan-out wrote no queue rows");

    // The explicit send re-seeds the current generation of that name.
    crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &replacement,
        "again",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    let replacement_target = replacement.binding.worker.worker.clone();
    let queued = queue(
        &fixture.db,
        &sender,
        &resolved,
        "hello thread",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    assert_eq!(queued.queued_to, vec![replacement_target]);
    let message = store
        .next_pending(&replacement.binding, crate::shared::time::now_epoch_i64())
        .unwrap()
        .unwrap();
    assert_eq!(
        message.payload.target_generation.as_deref(),
        Some(replacement.binding.worker.generation.as_str())
    );
}

/// A label the sender holds in two of their own teams is refused by the
/// thread-only path; explicit routing never creates the conflicting grant,
/// and a foreign team's label is invisible and never blocks or leaks.
#[test]
fn cross_team_labels_fail_closed_without_foreign_leaks() {
    let fixture = Fixture::new();
    let luna = fixture.host("luna", 1.0);
    let miso = fixture.host("miso", 2.0);
    let sender = fixture.sender("luna");
    let store = fixture.store();
    let first = store.create_team(Some(&luna)).unwrap();
    let second = store.create_team(Some(&luna)).unwrap();
    let foreign = store.create_team(Some(&miso)).unwrap();
    let a = fixture.reserve(&first.id, &luna, "alpha", "vm-1");
    let b = fixture.reserve(&second.id, &luna, "beta", "vm-2");
    let hidden = fixture.reserve(&foreign.id, &miso, "gamma", "vm-3");

    crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &a,
        "seed",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    // Seeding the same label in the second team routes explicitly but
    // creates no conflicting grant, so thread-only stays resolvable.
    let queued = crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &b,
        "seed",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    let event: String = fixture
        .db
        .conn()
        .query_row(
            "SELECT data FROM events WHERE id = ?",
            [queued.event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        serde_json::from_str::<serde_json::Value>(&event).unwrap()["_sandbox"]
            .get("conversation")
            .is_none()
    );
    let resolved = resolve(&fixture.db, &sender, "work").unwrap().unwrap();
    assert_eq!(resolved.team, first.id);

    // The thread-only path still fails closed if two grants ever exist, and
    // the failure carries no team or peer identifiers.
    fixture
        .db
        .conn()
        .execute(
            "INSERT INTO sandbox_conversations (id, team, label)
             SELECT 'forced', ?, 'work'
             WHERE NOT EXISTS (SELECT 1 FROM sandbox_conversations WHERE id = 'forced')",
            [&second.id],
        )
        .unwrap();
    fixture
        .db
        .conn()
        .execute(
            "INSERT OR IGNORE INTO sandbox_conversation_members
             (conversation, name, generation, host_created_at)
             VALUES ('forced', 'luna', ?, 1.0)",
            [host_generation(1.0)],
        )
        .unwrap();
    let failed = resolve(&fixture.db, &sender, "work").unwrap_err();
    let message = failed.to_string();
    assert!(message.contains("more than one of your teams"));
    assert!(!message.contains(&first.id) && !message.contains(&second.id));
    assert!(!message.contains(&a.binding.worker.worker));
    assert!(!message.contains(&hidden.binding.worker.worker));
    fixture
        .db
        .conn()
        .execute(
            "DELETE FROM sandbox_conversation_members WHERE conversation = 'forced'",
            [],
        )
        .unwrap();
    fixture
        .db
        .conn()
        .execute("DELETE FROM sandbox_conversations WHERE id = 'forced'", [])
        .unwrap();

    // The same label in a team luna does not belong to is invisible: it
    // neither blocks her resolution nor leaks into her errors.
    crate::sandbox::outgoing::queue(
        &fixture.db,
        &fixture.sender("miso"),
        &hidden,
        "seed",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    let resolved = resolve(&fixture.db, &sender, "work").unwrap().unwrap();
    assert_eq!(resolved.team, first.id);
    let foreign_view = resolve(&fixture.db, &fixture.sender("miso"), "work")
        .unwrap()
        .unwrap();
    assert_eq!(foreign_view.team, foreign.id);
}

/// An ordinary thread with the same label refuses the sandbox thread-only
/// path while ordinary resolution keeps working, and explicit directed
/// routing never creates a grant that would collide with it.
#[test]
fn ordinary_thread_collision_fails_closed() {
    let fixture = Fixture::new();
    let luna = fixture.host("luna", 1.0);
    let sender = fixture.sender("luna");
    let store = fixture.store();
    let team = store.create_team(Some(&luna)).unwrap();
    let alpha = fixture.reserve(&team.id, &luna, "alpha", "vm-1");
    let _nova = fixture.host("nova", 3.0);

    // Existing conversation, ordinary thread appears afterwards.
    crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &alpha,
        "seed",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    ordinary_membership(&fixture.db, "work", "nova");
    let failed = resolve(&fixture.db, &sender, "work").unwrap_err();
    let message = failed.to_string();
    assert!(message.contains("collides with an ordinary thread"));
    assert!(message.contains("work"));
    assert!(!message.contains(&alpha.binding.worker.worker));
    // Ordinary resolution is untouched by the conversation.
    assert_eq!(fixture.db.get_thread_members("work"), vec!["nova"]);

    // Explicit routing to a fresh label that already lives in the ordinary
    // index proceeds without creating a conflicting grant.
    ordinary_membership(&fixture.db, "ops", "nova");
    let queued = crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &alpha,
        "seed",
        &envelope(Some("ops")),
        None,
    )
    .unwrap();
    let event: String = fixture
        .db
        .conn()
        .query_row(
            "SELECT data FROM events WHERE id = ?",
            [queued.event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        serde_json::from_str::<serde_json::Value>(&event).unwrap()["_sandbox"]
            .get("conversation")
            .is_none()
    );
    let seeded: i64 = fixture
        .db
        .conn()
        .query_row(
            "SELECT count(*) FROM sandbox_conversations WHERE label = 'ops'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(seeded, 0);
    assert_eq!(fixture.db.get_thread_members("ops"), vec!["nova"]);
}

/// A same-team host without the participant grant resolves nothing, cannot
/// fan out, cannot adopt the label by targeting it, and keeps replying.
#[test]
fn non_participants_resolve_and_grant_nothing() {
    let fixture = Fixture::new();
    let luna = fixture.host("luna", 1.0);
    let miso = fixture.host("miso", 2.0);
    let luna_sender = fixture.sender("luna");
    let miso_sender = fixture.sender("miso");
    let store = fixture.store();
    let team = store.create_team(Some(&luna)).unwrap();
    store
        .enroll_host(&team.id, Access::Host(&luna), &miso)
        .unwrap();
    let alpha = fixture.reserve(&team.id, &luna, "alpha", "vm-1");
    crate::sandbox::outgoing::queue(
        &fixture.db,
        &luna_sender,
        &alpha,
        "seed",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    let conversation = conversation_id(&fixture.db);

    // No grant: the hidden thread is indistinguishable from an absent one.
    assert!(
        resolve(&fixture.db, &miso_sender, "work")
            .unwrap()
            .is_none()
    );

    // The guessed label cannot adopt membership; the directed send proceeds.
    let queued = crate::sandbox::outgoing::queue(
        &fixture.db,
        &miso_sender,
        &alpha,
        "guess",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    let event: String = fixture
        .db
        .conn()
        .query_row(
            "SELECT data FROM events WHERE id = ?",
            [queued.event_id],
            |row| row.get(0),
        )
        .unwrap();
    let data: serde_json::Value = serde_json::from_str(&event).unwrap();
    assert!(data["_sandbox"].get("conversation").is_none());
    let members = conversation_members(&fixture.db);
    assert_eq!(members.len(), 2);
    assert!(members.iter().all(|(name, _, _)| name != "miso"));
    assert_eq!(conversation_id(&fixture.db), conversation);

    // Only the participant can still fan out, unaffected by the guess.
    let resolved = resolve(&fixture.db, &luna_sender, "work").unwrap().unwrap();
    let queued = queue(
        &fixture.db,
        &luna_sender,
        &resolved,
        "hello thread",
        &envelope(Some("work")),
        None,
    )
    .unwrap();
    assert_eq!(queued.queued_to, vec![alpha.binding.worker.worker]);

    // A retained grant cannot route or reveal the thread once team enrollment is gone.
    fixture
        .db
        .conn()
        .execute(
            "DELETE FROM sandbox_team_hosts WHERE team = ? AND name = 'luna'",
            [&team.id],
        )
        .unwrap();
    assert!(
        resolve(&fixture.db, &luna_sender, "work")
            .unwrap()
            .is_none()
    );
    assert!(
        queue(
            &fixture.db,
            &luna_sender,
            &resolved,
            "revoked sender",
            &envelope(Some("work")),
            None,
        )
        .is_err()
    );
}

/// A replaced host instance generation loses its participant grant and
/// cannot thread-only send under the same display name.
#[test]
fn replaced_host_generation_loses_the_grant() {
    let fixture = Fixture::new();
    let luna = fixture.host("luna", 1.0);
    let sender = fixture.sender("luna");
    let store = fixture.store();
    let team = store.create_team(Some(&luna)).unwrap();
    let alpha = fixture.reserve(&team.id, &luna, "alpha", "vm-1");
    crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &alpha,
        "seed",
        &envelope(Some("work")),
        None,
    )
    .unwrap();

    // The display name is reused with a new instance generation.
    fixture
        .db
        .conn()
        .execute(
            "UPDATE instances SET created_at = 9 WHERE name = 'luna'",
            [],
        )
        .unwrap();
    // The stale identity resolves nothing: the grant was never re-seeded.
    assert!(resolve(&fixture.db, &sender, "work").unwrap().is_none());
    let failed = crate::sandbox::outgoing::queue(
        &fixture.db,
        &sender,
        &alpha,
        "seed",
        &envelope(Some("work")),
        None,
    );
    assert!(failed.is_err());
    // The replacement generation is not the conversation member either: a
    // directed thread seed fails its participant check and grants nothing.
    let replaced_sender = fixture.sender("luna");
    let failed = crate::sandbox::outgoing::queue(
        &fixture.db,
        &replaced_sender,
        &alpha,
        "seed",
        &envelope(Some("work")),
        None,
    );
    assert!(failed.is_err());
    let members = conversation_members(&fixture.db);
    assert_eq!(members.len(), 2);
    assert!(
        members
            .iter()
            .all(|(name, generation, created_at)| name != "luna"
                || *generation == host_generation(1.0) && *created_at == Some(1.0))
    );
    // The stale generation cannot direct sends into the team at all: team
    // enrollment binds the exact host generation.
    let failed = crate::sandbox::outgoing::queue(
        &fixture.db,
        &replaced_sender,
        &alpha,
        "plain",
        &envelope(None),
        None,
    );
    assert!(failed.is_err());
}
