//! Integration tests for orchestration contracts:
//! - Broadcast grammar guard (--broadcast, refusing implicit broadcast, bare-word rules)
//! - ACK vs result request watches (ACK preserves watch, inform completes/cancels watch)
//! - Thread membership lifetime & transactional cleanup on stop
//! - Targeted wake routing (targeted send pings only recipient, broadcast pings all)

mod support;

use rusqlite::Connection;
use std::net::TcpListener;
use std::time::{Duration, Instant};
use support::Hcom;

fn setup() -> (Hcom, Connection, String, String) {
    let h = Hcom::new();
    let sender = h.start();
    let receiver = h.start();
    let db = Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    (h, db, sender, receiver)
}

fn bind_notify_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    listener.set_nonblocking(true).expect("set nonblocking");
    let port = listener.local_addr().expect("local addr").port();
    (listener, port)
}

fn poll_wake_received(listener: &TcpListener, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        match listener.accept() {
            Ok((_stream, _addr)) => return true,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("accept error: {e}"),
        }
    }
    false
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Explicit --broadcast grammar guard
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn broadcast_flag_allows_delivery_to_all() {
    let (h, db, sender, receiver) = setup();

    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--broadcast",
        "--",
        "hello everyone",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    let (scope, text): (String, String) = db
        .query_row(
            "SELECT json_extract(data, '$.scope'), json_extract(data, '$.text')
             FROM events WHERE type = 'message' ORDER BY id DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(scope, "broadcast");
    assert_eq!(text, "hello everyone");

    // Delivered to receiver
    let delivered_json: String = db
        .query_row(
            "SELECT json_extract(data, '$.delivered_to')
             FROM events WHERE type = 'message' ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(delivered_json.contains(&receiver));
}

#[test]
fn implicit_broadcast_refused_without_flag() {
    let (h, _db, sender, _receiver) = setup();

    let (code, _stdout, stderr) = h.run(["send", "--name", &sender, "--", "hello nobody"]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("Refusing implicit broadcast: no explicit recipient was resolved"),
        "stderr was: {stderr}"
    );
}

#[test]
fn broadcast_flag_cannot_combine_with_target() {
    let (h, _db, sender, receiver) = setup();

    let (code, _stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--broadcast",
        &format!("@{receiver}"),
        "--",
        "invalid combo",
    ]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("--broadcast cannot be combined with agent targets or --thread"),
        "stderr was: {stderr}"
    );
}

#[test]
fn broadcast_flag_cannot_combine_with_thread() {
    let (h, _db, sender, _receiver) = setup();

    let (code, _stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--broadcast",
        "--thread",
        "ops",
        "--",
        "invalid combo",
    ]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("--broadcast cannot be combined with agent targets or --thread"),
        "stderr was: {stderr}"
    );
}

#[test]
fn bare_word_target_shorthand_resolves_known_agent() {
    let (h, db, sender, receiver) = setup();

    // First bare word matches known agent 'receiver', remaining words form message
    let (code, stdout, stderr) =
        h.run(["send", "--name", &sender, &receiver, "shorthand", "message"]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    let (scope, text, delivered): (String, String, String) = db
        .query_row(
            "SELECT json_extract(data, '$.scope'), json_extract(data, '$.text'), json_extract(data, '$.delivered_to')
             FROM events WHERE type = 'message' ORDER BY id DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(scope, "mentions");
    assert_eq!(text, "shorthand message");
    assert!(delivered.contains(&receiver));
}

#[test]
fn ambiguous_bare_words_rejected() {
    let (h, _db, sender, _receiver) = setup();

    // Multiple bare words with first word not matching any known agent
    let (code, _stdout, stderr) = h.run(["send", "--name", &sender, "nonexistentagent", "hello"]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("ambiguous send — multiple bare words cannot form one message"),
        "stderr was: {stderr}"
    );
}

#[test]
fn broadcast_bare_words_passthrough() {
    let (h, db, sender, _receiver) = setup();

    // With --broadcast, words after -- form the message
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--broadcast",
        "--",
        "broadcast",
        "words",
        "together",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    let (scope, text): (String, String) = db
        .query_row(
            "SELECT json_extract(data, '$.scope'), json_extract(data, '$.text')
             FROM events WHERE type = 'message' ORDER BY id DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(scope, "broadcast");
    assert_eq!(text, "broadcast words together");
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. ACK vs result request watches
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn request_creates_watch_and_ack_does_not_cancel_it_but_inform_does() {
    let (h, db, requester, responder) = setup();

    // Requester sends request
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &requester,
        &format!("@{responder}"),
        "--intent",
        "request",
        "--",
        "compute numbers",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    let req_id: i64 = db
        .query_row(
            "SELECT MAX(id) FROM events WHERE type = 'message'",
            [],
            |r| r.get(0),
        )
        .unwrap();

    // Request watch created in kv
    let reqwatch_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reqwatch_count, 1, "request watch row must be created");

    // Responder sends ACK referencing the request
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &responder,
        &format!("@{requester}"),
        "--intent",
        "ack",
        "--reply-to",
        &req_id.to_string(),
        "--",
        "received request",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    // ACK MUST NOT cancel the request watch!
    let reqwatch_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reqwatch_count, 1, "ACK must not cancel request watch");

    // Responder sends inform (the result)
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &responder,
        &format!("@{requester}"),
        "--intent",
        "inform",
        "--reply-to",
        &req_id.to_string(),
        "--",
        "result is 42",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    // Inform completes and cancels the request watch
    let reqwatch_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reqwatch_count, 0, "inform must cancel request watch");
}

#[test]
fn thread_request_creates_watch_and_inform_cancels_it() {
    let (h, db, requester, responder) = setup();

    // Seed thread
    let (code, _, _) = h.run([
        "send",
        "--name",
        &requester,
        "--thread",
        "pipeline",
        &format!("@{responder}"),
        "--",
        "seed thread",
    ]);
    assert_eq!(code, 0);

    // Send threaded request
    let (code, _, _) = h.run([
        "send", "--name", &requester, "--thread", "pipeline", "--intent", "request", "--",
        "status?",
    ]);
    assert_eq!(code, 0);

    let req_id: i64 = db
        .query_row(
            "SELECT MAX(id) FROM events WHERE type = 'message'",
            [],
            |r| r.get(0),
        )
        .unwrap();

    // Watch created
    let reqwatch_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reqwatch_count, 1, "thread request creates request watch");

    // Responder replies in thread with ACK - watch remains
    let (code, _, _) = h.run([
        "send",
        "--name",
        &responder,
        "--thread",
        "pipeline",
        "--intent",
        "ack",
        "--reply-to",
        &req_id.to_string(),
        "--",
        "working on it",
    ]);
    assert_eq!(code, 0);

    let reqwatch_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        reqwatch_count, 1,
        "ACK in thread does not cancel request watch"
    );

    // Responder replies in thread with inform - watch cancelled
    let (code, _, _) = h.run([
        "send", "--name", &responder, "--thread", "pipeline", "--intent", "inform", "--", "done",
    ]);
    assert_eq!(code, 0);

    let reqwatch_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reqwatch_count, 0, "inform in thread cancels request watch");
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Thread membership expiry / cleanup on stop transactional
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn thread_membership_cleaned_up_transactionally_on_instance_stop() {
    let (h, db, sender, receiver) = setup();

    // Sender seeds thread
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--thread",
        "cleanup-test",
        &format!("@{receiver}"),
        "--",
        "seed",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    // Verify both sender and receiver have auto-thread memberships in kv
    let sender_sub_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv
             WHERE key LIKE 'events_sub:%'
               AND json_extract(value, '$.thread_name') = 'cleanup-test'
               AND json_extract(value, '$.caller') = ?
               AND json_extract(value, '$.auto_thread_member') = 1",
            [&sender],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sender_sub_count, 1);

    let receiver_sub_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv
             WHERE key LIKE 'events_sub:%'
               AND json_extract(value, '$.thread_name') = 'cleanup-test'
               AND json_extract(value, '$.caller') = ?
               AND json_extract(value, '$.auto_thread_member') = 1",
            [&receiver],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(receiver_sub_count, 1);

    // Stop sender
    let (code, stdout, stderr) = h.run(["stop", &sender]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    // Transactional cleanup: sender's auto thread membership must be deleted
    let sender_sub_count_after: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv
             WHERE key LIKE 'events_sub:%'
               AND json_extract(value, '$.thread_name') = 'cleanup-test'
               AND json_extract(value, '$.caller') = ?",
            [&sender],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        sender_sub_count_after, 0,
        "auto thread membership must be cleaned up on stop"
    );

    // Receiver's membership is preserved
    let receiver_sub_count_after: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv
             WHERE key LIKE 'events_sub:%'
               AND json_extract(value, '$.thread_name') = 'cleanup-test'
               AND json_extract(value, '$.caller') = ?",
            [&receiver],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(receiver_sub_count_after, 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Targeted wakes
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn targeted_send_wakes_only_recipient() {
    let (h, db, sender, recipient) = setup();
    let bystander = h.start();

    let (recipient_listener, recipient_port) = bind_notify_listener();
    let (bystander_listener, bystander_port) = bind_notify_listener();

    // Register notify endpoints in DB
    db.execute(
        "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
         VALUES (?, 'listen', ?, unixepoch())",
        rusqlite::params![recipient, recipient_port],
    )
    .unwrap();
    db.execute(
        "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
         VALUES (?, 'listen', ?, unixepoch())",
        rusqlite::params![bystander, bystander_port],
    )
    .unwrap();

    // Sender sends targeted message to recipient
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        &format!("@{recipient}"),
        "--intent",
        "inform",
        "--",
        "wake recipient only",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    // Recipient receives wake
    assert!(
        poll_wake_received(&recipient_listener, Duration::from_millis(500)),
        "recipient must receive targeted wake"
    );

    // Bystander does NOT receive wake
    assert!(
        !poll_wake_received(&bystander_listener, Duration::from_millis(200)),
        "bystander must NOT receive targeted wake"
    );
}

#[test]
fn broadcast_send_wakes_all_endpoints() {
    let (h, db, sender, recipient) = setup();
    let bystander = h.start();

    let (recipient_listener, recipient_port) = bind_notify_listener();
    let (bystander_listener, bystander_port) = bind_notify_listener();

    db.execute(
        "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
         VALUES (?, 'listen', ?, unixepoch())",
        rusqlite::params![recipient, recipient_port],
    )
    .unwrap();
    db.execute(
        "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
         VALUES (?, 'listen', ?, unixepoch())",
        rusqlite::params![bystander, bystander_port],
    )
    .unwrap();

    // Sender sends broadcast message
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--broadcast",
        "--",
        "wake everyone",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    // Both receive wake
    assert!(
        poll_wake_received(&recipient_listener, Duration::from_millis(500)),
        "recipient must receive broadcast wake"
    );
    assert!(
        poll_wake_received(&bystander_listener, Duration::from_millis(500)),
        "bystander must receive broadcast wake"
    );
}
