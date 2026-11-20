//! Inline delivery acknowledges only the chronological output prefix.
mod support;

use rusqlite::{Connection, params};
use support::Hcom;

fn setup(tool: &str) -> (Hcom, Connection, String, String) {
    let h = Hcom::new();
    let sender = h.start();
    let receiver = h.start();
    let db = Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    db.execute(
        "UPDATE instances SET tool=? WHERE name=?",
        params![tool, receiver],
    )
    .unwrap();
    (h, db, sender, receiver)
}

fn queue(db: &Connection, from: &str, to: &str, text: &str) -> i64 {
    let data = serde_json::json!({
        "from": from,
        "text": text,
        "scope": "mentions",
        "mentions": [to],
        "delivered_to": [to],
        "sender_kind": "instance",
        "intent": "inform"
    });
    db.execute(
        "INSERT INTO events(timestamp,type,instance,data) VALUES(strftime('%Y-%m-%dT%H:%M:%f','now'),'message',?,?)",
        params![from, data.to_string()],
    )
    .unwrap();
    db.last_insert_rowid()
}

fn cursor(db: &Connection, name: &str) -> i64 {
    db.query_row(
        "SELECT last_event_id FROM instances WHERE name=?",
        [name],
        |row| row.get(0),
    )
    .unwrap()
}

fn send_reply(h: &Hcom, from: &str, to: &str, body: &str) -> String {
    let (code, stdout, stderr) = h.run([
        "send",
        "--name",
        from,
        &format!("@{to}"),
        "--intent",
        "inform",
        "--",
        body,
    ]);
    assert_eq!(code, 0, "{stderr}");
    stdout
}

#[test]
fn send_uses_one_chronological_cap_for_mixed_senders() {
    let (h, db, sender, receiver) = setup("adhoc");
    let child = h.start();
    db.execute(
        "UPDATE instances SET parent_name=? WHERE name=?",
        params![receiver, child],
    )
    .unwrap();

    let ids: Vec<i64> = (0..51)
        .map(|i| {
            queue(
                &db,
                if i % 2 == 0 { &child } else { &sender },
                &receiver,
                &format!("sentinel-{i:03}"),
            )
        })
        .collect();
    let first = send_reply(&h, &receiver, &sender, "reply");

    assert_eq!(cursor(&db, &receiver), ids[49]);
    assert!(first.contains("sentinel-000"));
    assert!(first.contains("sentinel-049"));
    assert!(!first.contains("sentinel-050"));
    assert!(first.contains("[+1 more unread"));
    assert!(first.contains("[Subagent messages]"));

    let second = send_reply(&h, &receiver, &sender, "reply-2");
    assert!(second.contains("sentinel-050"));
    assert_eq!(cursor(&db, &receiver), ids[50]);
}

#[test]
fn quiet_send_does_not_let_router_drain_the_inbox() {
    let (h, db, sender, receiver) = setup("adhoc");
    let id = queue(&db, &sender, &receiver, "quiet-sentinel");
    let before = cursor(&db, &receiver);

    let (code, stdout, stderr) = h.run([
        "send",
        "--quiet",
        "--name",
        &receiver,
        &format!("@{sender}"),
        "--",
        "reply",
    ]);
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.is_empty(), "{stdout}");
    assert_eq!(cursor(&db, &receiver), before);

    let output = send_reply(&h, &receiver, &sender, "reply-2");
    assert!(output.contains("quiet-sentinel"));
    assert_eq!(cursor(&db, &receiver), id);
}

#[test]
fn listen_emits_only_the_capped_prefix_before_router_returns() {
    let (h, db, sender, receiver) = setup("adhoc");
    let ids: Vec<i64> = (0..51)
        .map(|i| queue(&db, &sender, &receiver, &format!("listen-{i:03}")))
        .collect();

    let (code, stdout, stderr) = h.run(["listen", "--name", &receiver, "--timeout", "1"]);
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.contains("listen-000"));
    assert!(stdout.contains("listen-049"));
    assert!(!stdout.contains("listen-050"));
    assert!(stdout.contains("[+1 more unread"));
    assert_eq!(cursor(&db, &receiver), ids[49]);
}

#[test]
fn event_stream_output_does_not_drain_the_callers_inbox() {
    let (h, db, sender, receiver) = setup("adhoc");
    let before = cursor(&db, &receiver);
    let id = queue(&db, &sender, &receiver, "stream-sentinel");
    let after_id = (id - 1).to_string();

    let (code, stdout, stderr) = h.run([
        "events",
        "stream",
        "--name",
        &receiver,
        "--after-id",
        &after_id,
        "--timeout",
        "1",
        "--type",
        "message",
    ]);
    assert_eq!(code, 0, "{stderr}");
    let records: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).expect("stream stdout must remain JSON"))
        .collect();
    assert_eq!(records.len(), 1, "{stdout}");
    assert_eq!(records[0]["data"]["text"], "stream-sentinel");
    assert_eq!(cursor(&db, &receiver), before);
}

#[cfg(unix)]
#[test]
fn broken_inline_output_keeps_the_batch_unread() {
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    use std::process::Stdio;

    let (h, db, sender, receiver) = setup("adhoc");
    queue(&db, &sender, &receiver, "broken-output-sentinel");
    let before = cursor(&db, &receiver);
    let (writer, reader) = UnixStream::pair().unwrap();
    drop(reader);

    let output = h
        .cmd()
        .args([
            "send",
            "--name",
            &receiver,
            &format!("@{sender}"),
            "--",
            "reply",
        ])
        .stdout(Stdio::from(OwnedFd::from(writer)))
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Message sent, but incoming message output failed")
    );
    assert_eq!(cursor(&db, &receiver), before);
    assert!(send_reply(&h, &receiver, &sender, "reply-2").contains("broken-output-sentinel"));
}
