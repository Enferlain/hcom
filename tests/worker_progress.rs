//! Exact worker-generation streams and compact progress.
#[path = "support/event_observation.rs"]
mod event_fixture;
mod support;
use event_fixture::{db, seed_test_event};
use serde_json::{Value, json};
use support::Hcom;

#[test]
fn events_stream_ordered_and_resumable() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    let id1 = seed_test_event(&h, "status", "agent1", json!({"status": "starting"}));
    let _id2 = seed_test_event(
        &h,
        "message",
        "agent1",
        json!({"from": "agent1", "text": "skip me"}),
    );
    let id3 = seed_test_event(&h, "status", "agent1", json!({"status": "active"}));

    // Stream with filter --type status
    let (code, stdout, stderr) = h.run([
        "events",
        "stream",
        "--after-id",
        &cursor,
        "--timeout",
        "1",
        "--type",
        "status",
    ]);
    assert_eq!(code, 0, "stream exit 0 on timeout: stderr={stderr}");

    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        lines.len(),
        2,
        "must emit exactly the 2 status events: {stdout}"
    );

    let ev1: Value = serde_json::from_str(lines[0]).unwrap();
    let ev3: Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(ev1["id"].as_i64(), Some(id1));
    assert_eq!(ev3["id"].as_i64(), Some(id3));
    assert_eq!(ev1["data"]["status"], "starting");
    assert_eq!(ev3["data"]["status"], "active");

    // Resuming after id1: only id3 is emitted
    let (code2, stdout2, stderr2) = h.run([
        "events",
        "stream",
        "--after-id",
        &id1.to_string(),
        "--timeout",
        "1",
        "--type",
        "status",
    ]);
    assert_eq!(code2, 0, "stderr={stderr2}");
    let lines2: Vec<&str> = stdout2.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        lines2.len(),
        1,
        "resuming stream must not replay id1: {stdout2}"
    );
    let resumed: Value = serde_json::from_str(lines2[0]).unwrap();
    assert_eq!(resumed["id"].as_i64(), Some(id3));

    // Verify stream left no subscription or request-watch side effects in kv
    let conn = db(&h);
    let sub_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        sub_count, 0,
        "events stream must not leave subscriptions in kv"
    );
}

#[test]
fn events_stream_follow_and_stop_boundaries() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    let worker = h.start();
    let conn = db(&h);
    let created_at: f64 = conn
        .query_row(
            "SELECT created_at FROM instances WHERE name = ?1",
            rusqlite::params![worker],
            |r| r.get(0),
        )
        .unwrap();
    drop(conn);

    // Emit active status
    seed_test_event(
        &h,
        "status",
        &worker,
        json!({"status": "active", "detail": "running task"}),
    );

    // Emit soft stop (status stopped) - stream must NOT terminate on soft stop
    seed_test_event(
        &h,
        "status",
        &worker,
        json!({"status": "stopped", "detail": "soft pause"}),
    );

    // Emit resumed activity after soft stop
    seed_test_event(
        &h,
        "status",
        &worker,
        json!({"status": "active", "detail": "resumed task"}),
    );

    // Emit terminal life event: action: stopped with matching snapshot
    seed_test_event(
        &h,
        "life",
        &worker,
        json!({
            "action": "stopped",
            "status": "stopped",
            "reason": "completed",
            "snapshot": {
                "name": worker,
                "created_at": created_at,
            }
        }),
    );

    // Run stream --follow <worker>
    let (code, stdout, stderr) = h.run([
        "events",
        "stream",
        "--after-id",
        &cursor,
        "--follow",
        &worker,
        "--timeout",
        "3",
    ]);
    assert_eq!(
        code, 0,
        "stream must exit cleanly on terminal stop: stderr={stderr}"
    );

    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        lines.len() >= 4,
        "all events up to terminal stop emitted: {stdout}"
    );

    // The last emitted event is the terminal life event (ObserveAndStop)
    let last_ev: Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert_eq!(last_ev["type"], "life");
    assert_eq!(last_ev["data"]["action"], "stopped");
}

#[test]
fn compact_progress_stream_schema_and_deduplication() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    let worker = h.start();
    let conn = db(&h);
    let created_at: f64 = conn
        .query_row(
            "SELECT created_at FROM instances WHERE name = ?1",
            rusqlite::params![worker],
            |r| r.get(0),
        )
        .unwrap();
    drop(conn);

    // Seed events: active phase, tool Edit on path, then redundant active phase
    seed_test_event(
        &h,
        "status",
        &worker,
        json!({"status": "active", "detail": "starting work"}),
    );
    seed_test_event(
        &h,
        "status",
        &worker,
        json!({"status": "active", "context": "tool:Edit", "detail": "src/main.rs"}),
    );
    seed_test_event(
        &h,
        "status",
        &worker,
        json!({"status": "active", "detail": "still active"}),
    );
    seed_test_event(
        &h,
        "life",
        &worker,
        json!({
            "action": "stopped",
            "status": "stopped",
            "snapshot": {
                "name": worker,
                "created_at": created_at,
            }
        }),
    );

    let (code, stdout, stderr) = h.run([
        "events",
        "stream",
        "--after-id",
        &cursor,
        "--follow",
        &worker,
        "--compact",
        "--timeout",
        "3",
    ]);
    assert_eq!(code, 0, "compact stream must succeed: stderr={stderr}");

    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(!lines.is_empty(), "compact stream should emit records");

    for line in &lines {
        let rec: Value = serde_json::from_str(line).expect("valid compact NDJSON");
        assert_eq!(rec["schema_version"], 1, "schema_version must be 1: {line}");
        assert!(
            rec["generation"].as_str().unwrap().starts_with(&worker),
            "generation must start with worker name: {line}"
        );
        assert!(
            rec.get("activity").is_some(),
            "activity must be present: {line}"
        );
        assert!(rec.get("cursor").is_some());
        assert!(rec.get("ts").is_some());
    }

    // Check that Edit tool emitted file activity record
    let file_rec = lines.iter().find(|l| l.contains(r#""path":"src/main.rs""#));
    assert!(
        file_rec.is_some(),
        "file activity record expected: {stdout}"
    );
}
