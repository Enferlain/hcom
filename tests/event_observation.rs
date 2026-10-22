//! Cursor waits and inbox ownership.
#[path = "support/event_observation.rs"]
mod event_fixture;
mod support;
use event_fixture::{db, parse_first_json_line, seed_test_event};
use serde_json::json;
use support::Hcom;

#[test]
fn events_cursor_is_standalone_and_advances() {
    let h = Hcom::new();

    // 1. In a fresh directory, --cursor initializes the DB and returns 0.
    let (code, stdout, stderr) = h.run(["events", "--cursor"]);
    assert_eq!(code, 0, "stderr={stderr}");
    let cursor: i64 = stdout.trim().parse().expect("cursor must be an integer");
    assert_eq!(cursor, 0, "fresh db cursor should be 0");

    // 2. Seeding an event advances the cursor.
    let id1 = seed_test_event(&h, "status", "agent1", json!({"status": "active"}));
    let (code, stdout, stderr) = h.run(["events", "--cursor"]);
    assert_eq!(code, 0, "stderr={stderr}");
    let new_cursor: i64 = stdout.trim().parse().expect("cursor must be an integer");
    assert_eq!(new_cursor, id1, "cursor must match the latest event id");
}

#[test]
fn filtered_wait_isolates_inbox_and_matches_target() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();

    // Capture cursor before sending message
    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    let conn = db(&h);
    let caller_cursor_before: i64 = conn
        .query_row(
            "SELECT COALESCE(last_event_id, 0) FROM instances WHERE name = ?1",
            rusqlite::params![caller],
            |r| r.get(0),
        )
        .unwrap_or(0);
    drop(conn);

    // Send an unrelated message to caller
    let (send_code, _, send_err) = h.run([
        "send",
        &format!("@{caller}"),
        "--name",
        &worker,
        "--",
        "unrelated inbox message",
    ]);
    assert_eq!(send_code, 0, "stderr={send_err}");

    // Filtered wait for status=active from worker with --after-id:
    // should ignore the unread message and time out (exit 1)
    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "1",
        "--after-id",
        &cursor,
        "--agent",
        &worker,
        "--status",
        "active",
    ]);
    assert_eq!(
        code, 1,
        "filtered wait must time out: stdout={stdout} stderr={stderr}"
    );
    let preview_pattern = format!("<hcom>{worker} → {caller}</hcom>");
    assert!(
        !stdout.contains(&preview_pattern),
        "filtered wait must not emit unread message preview: {stdout}"
    );

    // Verify caller read cursor is UNCHANGED (proves inbox message was not consumed/acknowledged)
    let conn = db(&h);
    let caller_cursor_after: i64 = conn
        .query_row(
            "SELECT COALESCE(last_event_id, 0) FROM instances WHERE name = ?1",
            rusqlite::params![caller],
            |r| r.get(0),
        )
        .unwrap_or(0);
    assert_eq!(
        caller_cursor_after, caller_cursor_before,
        "caller cursor must not advance during filtered wait"
    );

    // Verify unread message is STILL unread in the database (id > caller's read cursor)
    let unread_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE type = 'message' AND id > ?1 AND json_extract(data, '$.text') = 'unrelated inbox message'",
            rusqlite::params![caller_cursor_after],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(unread_count, 1, "message must remain unread in inbox");

    // Assert no subscription or watch side-effects in kv
    let sub_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        sub_count, 0,
        "filtered wait must not create subscriptions or request watches in kv"
    );
    drop(conn);

    // Emit matching status event for worker after cursor
    let status_id = seed_test_event(
        &h,
        "status",
        &worker,
        json!({"status": "active", "detail": "ready for work"}),
    );

    // Now filtered wait with the same cursor immediately catches the matching event and exits 0
    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "1",
        "--after-id",
        &cursor,
        "--agent",
        &worker,
        "--status",
        "active",
    ]);
    assert_eq!(
        code, 0,
        "matching status event must exit 0: stderr={stderr}"
    );
    let event = parse_first_json_line(&stdout);
    assert_eq!(event["id"].as_i64(), Some(status_id));
    assert_eq!(event["data"]["status"], "active");
}

#[test]
fn unfiltered_wait_previews_and_continues_to_timeout() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();

    // Send an unread message to caller
    let (send_code, _, send_err) = h.run([
        "send",
        &format!("@{caller}"),
        "--name",
        &worker,
        "--",
        "preview-and-continue message",
    ]);
    assert_eq!(send_code, 0, "stderr={send_err}");

    // Unfiltered wait without explicit cursor: previews unread mail once,
    // but CONTINUES waiting until timeout and exits 1 (does not false-complete).
    let (code, stdout, stderr) = h.run(["events", "--name", &caller, "--wait", "1"]);
    assert_eq!(
        code, 1,
        "unfiltered wait must time out with exit 1 even if preview was shown: stdout={stdout} stderr={stderr}"
    );
    assert!(
        stdout.contains("preview-and-continue message")
            || stdout.contains(&format!("{worker} → {caller}")),
        "unfiltered wait must show unread preview once: {stdout}"
    );
}

#[test]
fn events_wait_runtime_sql_stepping_error_exits_two() {
    let h = Hcom::new();
    let (cursor_code, _, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);

    seed_test_event(&h, "status", "agent1", json!({"status": "active"}));

    // Wait mode with runtime SQL stepping error in --sql, with explicit --after-id 0
    let (code, _stdout, stderr) = h.run([
        "events",
        "--wait",
        "1",
        "--after-id",
        "0",
        "--sql",
        "json_extract('{not json}', '$.foo') = 'bar'",
    ]);
    assert_eq!(
        code, 2,
        "runtime SQL error in wait mode must exit 2: stderr={stderr}"
    );
    assert!(
        stderr.contains("Error in SQL WHERE clause"),
        "stderr must explain error: {stderr}"
    );
}
