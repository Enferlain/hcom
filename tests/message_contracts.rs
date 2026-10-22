//! Integration tests for orchestration contracts:
//! - Broadcast grammar guard (--broadcast, refusing implicit broadcast, bare-word rules)
//! - ACK vs result request watches (ACK preserves watch, inform completes/cancels watch)
//! - Thread membership persistence across stop/resume (upstream contract)
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

fn run_with_stdin(h: &Hcom, args: &[&str], input: &str) -> (i32, String, String) {
    use std::io::Write;
    use std::process::Stdio;

    let mut cmd = h.cmd();
    cmd.args(args);
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn command with stdin");
    {
        let mut stdin = child.stdin.take().expect("open stdin");
        stdin.write_all(input.as_bytes()).expect("write to stdin");
    }
    let output = child.wait_with_output().expect("wait for output");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
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
fn broadcast_combining_with_target_fails_before_stdin() {
    let (h, _db, sender, receiver) = setup();

    // Even when stdin is empty (null stdin), combining --broadcast with a target fails immediately
    // with the conflict error, not "No input received on stdin".
    let (code, _stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--broadcast",
        &format!("@{receiver}"),
        "--stdin",
    ]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("--broadcast cannot be combined with agent targets or --thread"),
        "stderr was: {stderr}"
    );
}

#[test]
fn piped_broadcast_with_in_text_mention_stays_broadcast() {
    let (h, db, sender, receiver) = setup();

    // echo "hey @receiver and everyone" | hcom send --name sender --broadcast
    let (code, stdout, stderr) = run_with_stdin(
        &h,
        &["send", "--name", &sender, "--broadcast"],
        &format!("hey @{receiver} and everyone\n"),
    );
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
    assert_eq!(text, format!("hey @{receiver} and everyone\n"));
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
fn bare_word_target_shorthand_resolves_tagged_agent() {
    let (h, db, sender, _receiver) = setup();

    // Insert a tagged instance: name 'target', tag 'worker', display name 'worker-target'
    db.execute(
        "INSERT INTO instances (name, tag, created_at) VALUES ('target', 'worker', unixepoch())",
        [],
    )
    .unwrap();

    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        "worker-target",
        "hello",
        "tagged",
    ]);
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
    assert_eq!(text, "hello tagged");
    assert!(delivered.contains("target"));
}

#[test]
fn bare_word_matching_agent_with_thread_flag_treats_all_words_as_message() {
    let (h, db, sender, receiver) = setup();

    // Seed existing thread first
    let (seed_code, _, stderr) = h.run([
        "send",
        "--name",
        &sender,
        &format!("@{receiver}"),
        "--thread",
        "chat",
        "--",
        "seed thread",
    ]);
    assert_eq!(seed_code, 0, "seeding thread failed: {stderr}");

    // Even though first word is `receiver`, because `--thread` is set,
    // allow_bare_target is false, so `receiver` is not stripped as a target.
    // Use one quoted message arg per upstream syntax.
    let msg = format!("{receiver} is the first word");
    let (code, stdout, stderr) = h.run(["send", "--name", &sender, "--thread", "chat", &msg]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    let (scope, text, thread, delivered): (String, String, String, String) = db
        .query_row(
            "SELECT json_extract(data, '$.scope'), json_extract(data, '$.text'), json_extract(data, '$.thread'), json_extract(data, '$.delivered_to')
             FROM events WHERE type = 'message' ORDER BY id DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(scope, "mentions");
    assert_eq!(thread, "chat");
    assert_eq!(text, msg);
    assert!(delivered.contains(&receiver));
}

#[test]
fn bare_word_matching_agent_with_broadcast_flag_treats_all_words_as_message() {
    let (h, db, sender, receiver) = setup();

    // Even though first word is `receiver`, because `--broadcast` is set,
    // allow_bare_target is false, so `receiver` is not stripped as a target.
    // Use one quoted message arg per upstream syntax.
    let msg = format!("{receiver} is the first word");
    let (code, stdout, stderr) = h.run(["send", "--name", &sender, "--broadcast", &msg]);
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
    assert_eq!(text, msg);
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
fn ambiguous_bare_words_rejected_before_stdin() {
    let (h, _db, sender, _receiver) = setup();

    // Passing multiple bare words without @ fails on ambiguity before reading stdin
    let (code, _stdout, stderr) = h.run(["send", "--name", &sender, "nonexistentagent", "hello"]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("ambiguous send — multiple bare words cannot form one message"),
        "stderr was: {stderr}"
    );

    // A lone bare word also fails before stdin can be consumed
    let (code, _stdout, stderr) = h.run(["send", "--name", &sender, "lonebareword"]);
    assert_ne!(code, 0);
    assert!(
        stderr.contains("is ambiguous: a target needs '@', a broadcast needs '--broadcast'"),
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
// 3. Thread membership persistence across stop/rejoin (upstream contract)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn thread_membership_persists_across_instance_stop_and_rejoin() {
    let (h, db, sender, receiver) = setup();

    // Sender seeds thread
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--thread",
        "persist-test",
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
               AND json_extract(value, '$.thread_name') = 'persist-test'
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
               AND json_extract(value, '$.thread_name') = 'persist-test'
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

    // Upstream persistence: thread membership is retained across stop
    let sender_sub_count_after: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv
             WHERE key LIKE 'events_sub:%'
               AND json_extract(value, '$.thread_name') = 'persist-test'
               AND json_extract(value, '$.caller') = ?",
            [&sender],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        sender_sub_count_after, 1,
        "thread membership must persist across stop in upstream"
    );

    // Receiver's membership is preserved
    let receiver_sub_count_after: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv
             WHERE key LIKE 'events_sub:%'
               AND json_extract(value, '$.thread_name') = 'persist-test'
               AND json_extract(value, '$.caller') = ?",
            [&receiver],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(receiver_sub_count_after, 1);

    // Reclaim/resume sender identity under the same name
    let (code, stdout, stderr) = h.run(["start", "--as", &sender]);
    assert_eq!(
        code, 0,
        "start --as failed: stderr={stderr}, stdout={stdout}"
    );

    // Receiver sends post-resume message in thread
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &receiver,
        "--thread",
        "persist-test",
        "--",
        "post-resume message",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    let delivered_json: String = db
        .query_row(
            "SELECT json_extract(data, '$.delivered_to')
             FROM events WHERE type = 'message' ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        delivered_json.contains(&sender),
        "resumed instance must continue receiving thread messages: {delivered_json}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Targeted wakes
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn targeted_send_wakes_only_recipient() {
    let (h, db, sender, recipient) = setup();
    let bystander = h.start();

    let (recipient_listener, recipient_port) = bind_notify_listener();
    let (recipient_pty_listener, recipient_pty_port) = bind_notify_listener();
    let (bystander_listener, bystander_port) = bind_notify_listener();
    let (bystander_hook_listener, bystander_hook_port) = bind_notify_listener();

    // Register notify endpoints in DB:
    // Recipient has both listen and pty delivery loops
    db.execute(
        "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
         VALUES (?, 'listen', ?, unixepoch())",
        rusqlite::params![recipient, recipient_port],
    )
    .unwrap();
    db.execute(
        "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
         VALUES (?, 'pty', ?, unixepoch())",
        rusqlite::params![recipient, recipient_pty_port],
    )
    .unwrap();

    // Bystander has listen and hook delivery loops
    db.execute(
        "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
         VALUES (?, 'listen', ?, unixepoch())",
        rusqlite::params![bystander, bystander_port],
    )
    .unwrap();
    db.execute(
        "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
         VALUES (?, 'hook', ?, unixepoch())",
        rusqlite::params![bystander, bystander_hook_port],
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

    // Recipient receives wake on both listen and pty endpoints
    assert!(
        poll_wake_received(&recipient_listener, Duration::from_millis(500)),
        "recipient must receive targeted wake on listen endpoint"
    );
    assert!(
        poll_wake_received(&recipient_pty_listener, Duration::from_millis(500)),
        "recipient must receive targeted wake on pty endpoint"
    );

    // Bystander does NOT receive wake on either listen or hook endpoints
    assert!(
        !poll_wake_received(&bystander_listener, Duration::from_millis(200)),
        "bystander must NOT receive targeted wake on listen endpoint"
    );
    assert!(
        !poll_wake_received(&bystander_hook_listener, Duration::from_millis(200)),
        "bystander must NOT receive targeted wake on hook endpoint"
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

#[test]
fn targeted_send_wakes_unrelated_raw_observers_but_not_unrelated_pty_or_inject() {
    let (h, db, sender, recipient) = setup();
    let bystander = h.start();

    let (recipient_listener, recipient_port) = bind_notify_listener();
    let (bystander_pty_listener, bystander_pty_port) = bind_notify_listener();
    let (bystander_inject_listener, bystander_inject_port) = bind_notify_listener();
    let (bystander_wait_listener, bystander_wait_port) = bind_notify_listener();
    let (bystander_stream_listener, bystander_stream_port) = bind_notify_listener();
    let (bystander_filter_listener, bystander_filter_port) = bind_notify_listener();

    // Register recipient endpoint
    db.execute(
        "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
         VALUES (?, 'listen', ?, unixepoch())",
        rusqlite::params![recipient, recipient_port],
    )
    .unwrap();

    // Register bystander endpoints: delivery loops, inject, and raw observer endpoints
    for (kind, port) in [
        ("pty", bystander_pty_port),
        ("inject", bystander_inject_port),
        ("events_wait", bystander_wait_port),
        ("events_stream", bystander_stream_port),
        ("listen_filter", bystander_filter_port),
    ] {
        db.execute(
            "INSERT OR REPLACE INTO notify_endpoints (instance, kind, port, updated_at)
             VALUES (?, ?, ?, unixepoch())",
            rusqlite::params![bystander, kind, port],
        )
        .unwrap();
    }

    // Sender sends targeted message to recipient
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        &sender,
        &format!("@{recipient}"),
        "--intent",
        "inform",
        "--",
        "wake observers test",
    ]);
    assert_eq!(code, 0, "stdout: {stdout}, stderr: {stderr}");

    // Targeted recipient is woken
    assert!(
        poll_wake_received(&recipient_listener, Duration::from_millis(500)),
        "recipient must receive targeted wake"
    );

    // Unrelated raw observer endpoints MUST be woken so they see the new event
    assert!(
        poll_wake_received(&bystander_wait_listener, Duration::from_millis(500)),
        "unrelated events_wait observer must be woken on targeted send"
    );
    assert!(
        poll_wake_received(&bystander_stream_listener, Duration::from_millis(500)),
        "unrelated events_stream observer must be woken on targeted send"
    );
    assert!(
        poll_wake_received(&bystander_filter_listener, Duration::from_millis(500)),
        "unrelated listen_filter observer must be woken on targeted send"
    );

    // Unrelated PTY and inject MUST NOT be woken
    assert!(
        !poll_wake_received(&bystander_pty_listener, Duration::from_millis(200)),
        "unrelated bystander PTY endpoint must NOT be woken"
    );
    assert!(
        !poll_wake_received(&bystander_inject_listener, Duration::from_millis(200)),
        "unrelated bystander inject endpoint must NOT be woken"
    );
}
