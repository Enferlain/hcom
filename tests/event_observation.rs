//! Integration tests for event observation contracts:
//! - cursor management (`events --cursor`)
//! - filtered wait inbox isolation (`events --wait --after-id`)
//! - unfiltered wait preview-then-continue
//! - durable ordered & resumable stream (`events stream`)
//! - generation follow and soft-stop continuity
//! - compact progress stream (`events stream --follow --compact`)
//! - result correlation outcomes & exit codes (`events --wait --result-from`)
//! - stopped-provider result recovery (`src/provider_result.rs` integration)

mod support;

use rusqlite::Connection;
use serde_json::{Value, json};
use support::Hcom;

fn db(h: &Hcom) -> Connection {
    Connection::open(h.path().join("hcom.db")).unwrap()
}

fn seed_test_event(h: &Hcom, event_type: &str, instance: &str, data: Value) -> i64 {
    let conn = db(h);
    conn.execute(
        "INSERT INTO events (timestamp, type, instance, data)
         VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?1, ?2, ?3)",
        rusqlite::params![event_type, instance, data.to_string()],
    )
    .unwrap();
    conn.last_insert_rowid()
}

fn parse_first_json_line(stdout: &str) -> Value {
    let line = stdout
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with('{'))
        .unwrap_or_else(|| panic!("expected JSON line in output:\n{stdout}"));
    serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON line '{line}': {e}"))
}

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

#[test]
fn result_from_correlation_outcomes_and_exit_codes() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();

    // ── 1. Outcome: Actionable Blocker (exit 4) ───────────────────────────
    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    seed_test_event(
        &h,
        "life",
        &worker,
        json!({
            "action": "launch_blocked",
            "status": "blocked",
            "blocked_kind": "confirmation",
            "detail": "user approval required",
        }),
    );

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "2",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-blocker",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 4,
        "blocked worker must exit 4: stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["outcome"], "blocked");
    assert_eq!(outcome["result_blocked"], true);
    assert_eq!(outcome["worker"], worker);
    assert_eq!(outcome["thread"], "workflow-blocker");

    // ── 2. Outcome: Launch Failure (exit 5) ───────────────────────────────
    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    seed_test_event(
        &h,
        "life",
        &worker,
        json!({
            "action": "launch_failed",
            "status": "failed",
            "reason": "exited_before_bind",
            "detail": "process crashed on startup",
        }),
    );

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "2",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-fail",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 5,
        "launch failed worker must exit 5: stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["outcome"], "launch_failed");
    assert_eq!(outcome["result_launch_failed"], true);
    assert_eq!(outcome["worker"], worker);

    // ── 3. Outcome: Deadline Timeout (exit 1) ─────────────────────────────
    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "1",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-timeout",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 1,
        "deadline timeout must exit 1: stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["timed_out"], true);
    assert_eq!(outcome["outcome"], "deadline");
    assert_eq!(outcome["worker"], worker);

    // ── 4. Outcome: Matching Result Message (exit 0) ──────────────────────
    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    // Worker sends result with matching thread
    let (send_code, _, send_err) = h.run([
        "send",
        &format!("@{caller}"),
        "--name",
        &worker,
        "--intent",
        "inform",
        "--thread",
        "workflow-success",
        "--",
        "task completed successfully",
    ]);
    assert_eq!(send_code, 0, "stderr={send_err}");

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "2",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-success",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 0,
        "matching result must exit 0: stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["type"], "message");
    assert_eq!(outcome["data"]["text"], "task completed successfully");
    // External wrappers consume .data.message as well
    assert_eq!(outcome["data"]["message"], "task completed successfully");
}

#[test]
fn result_from_rejects_cross_worker_message() {
    let h = Hcom::new();
    let caller = h.start();
    let expected_worker = h.start();
    let decoy_worker = h.start();

    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    // Decoy sends a message with the same thread
    let (send_code, _, _) = h.run([
        "send",
        &format!("@{caller}"),
        "--name",
        &decoy_worker,
        "--intent",
        "inform",
        "--thread",
        "cross-worker-thread",
        "--",
        "decoy result",
    ]);
    assert_eq!(send_code, 0);

    // Correlated wait for expected_worker must NOT match decoy worker's message, times out (exit 1)
    let (code, stdout, _) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "1",
        "--after-id",
        &cursor,
        "--thread",
        "cross-worker-thread",
        "--result-from",
        &expected_worker,
    ]);
    assert_eq!(code, 1, "must not accept decoy worker result: {stdout}");
    assert!(stdout.contains(r#""timed_out":true"#));

    // Expected worker sends message with same thread
    let (send_code2, _, _) = h.run([
        "send",
        &format!("@{caller}"),
        "--name",
        &expected_worker,
        "--intent",
        "inform",
        "--thread",
        "cross-worker-thread",
        "--",
        "expected result",
    ]);
    assert_eq!(send_code2, 0);

    // Correlated wait now exits 0 with expected result
    let (code2, stdout2, stderr2) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "1",
        "--after-id",
        &cursor,
        "--thread",
        "cross-worker-thread",
        "--result-from",
        &expected_worker,
    ]);
    assert_eq!(code2, 0, "stderr={stderr2}");
    assert!(stdout2.contains("expected result"));
}

#[test]
fn stopped_provider_result_recovery_claude() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();

    let conn = db(&h);
    let created_at: f64 = conn
        .query_row(
            "SELECT created_at FROM instances WHERE name = ?1",
            rusqlite::params![worker],
            |r| r.get(0),
        )
        .unwrap();

    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    let session_id = "test-session-claude-123";
    let transcript_file = h.root.path().join("claude_transcript.jsonl");
    let transcript_path = transcript_file.to_string_lossy().to_string();

    // Create Claude transcript with completion send AND successful tool_result
    let transcript_lines = vec![
        json!({
            "type": "user",
            "sessionId": session_id,
            "message": { "content": "Please complete workflow-recover" }
        }),
        json!({
            "type": "assistant",
            "sessionId": session_id,
            "message": {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-call-1",
                        "name": "Bash",
                        "input": {
                            "command": "hcom send @caller --thread workflow-recover --intent inform -- 'recovered from claude transcript'"
                        }
                    }
                ]
            }
        }),
        json!({
            "type": "user",
            "sessionId": session_id,
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tool-call-1",
                        "is_error": false,
                        "content": "Sent to: caller"
                    }
                ]
            }
        }),
    ];
    let content = transcript_lines
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&transcript_file, content).unwrap();

    // Update worker in DB with tool, session_id, transcript_path, and status = stopped
    conn.execute(
        "UPDATE instances
         SET tool = 'claude', session_id = ?1, transcript_path = ?2, status = 'stopped'
         WHERE name = ?3",
        rusqlite::params![session_id, transcript_path, worker],
    )
    .unwrap();

    // Emit worker stopped life event with complete snapshot
    seed_test_event(
        &h,
        "life",
        &worker,
        json!({
            "action": "stopped",
            "status": "stopped",
            "reason": "clean_exit",
            "snapshot": {
                "name": worker,
                "created_at": created_at,
                "tool": "claude",
                "session_id": session_id,
                "transcript_path": transcript_path,
            }
        }),
    );

    // Correlated wait should recover the result from transcript and exit 0!
    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "2",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-recover",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 0,
        "result must be recovered from stopped worker transcript: stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["type"], "message");
    assert_eq!(outcome["data"]["text"], "recovered from claude transcript");
    assert_eq!(
        outcome["data"]["message"],
        "recovered from claude transcript"
    );
    assert_eq!(outcome["data"]["recovered"], true);
}

#[test]
fn stopped_provider_result_recovery_claude_production_envelope() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();

    let conn = db(&h);
    let created_at: f64 = conn
        .query_row(
            "SELECT created_at FROM instances WHERE name = ?1",
            rusqlite::params![worker],
            |r| r.get(0),
        )
        .unwrap();

    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    let session_id = "test-session-claude-env-1";
    let transcript_file = h.root.path().join("claude_env_transcript.jsonl");
    let transcript_path = transcript_file.to_string_lossy().to_string();

    // User prompt uses standard delivered envelope format [request:workflow-env #101]
    let transcript_lines = vec![
        json!({
            "type": "user",
            "sessionId": session_id,
            "message": { "content": "[request:workflow-env #101] caller → worker: please complete task" }
        }),
        json!({
            "type": "assistant",
            "sessionId": session_id,
            "message": {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-call-env",
                        "name": "Bash",
                        "input": {
                            "command": "hcom send @caller --thread workflow-env --intent inform -- 'recovered envelope result'"
                        }
                    }
                ]
            }
        }),
        json!({
            "type": "user",
            "sessionId": session_id,
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tool-call-env",
                        "is_error": false,
                        "content": "Sent to: caller"
                    }
                ]
            }
        }),
    ];
    let content = transcript_lines
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&transcript_file, content).unwrap();

    conn.execute(
        "UPDATE instances
         SET tool = 'claude', session_id = ?1, transcript_path = ?2, status = 'stopped'
         WHERE name = ?3",
        rusqlite::params![session_id, transcript_path, worker],
    )
    .unwrap();

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
                "tool": "claude",
                "session_id": session_id,
                "transcript_path": transcript_path,
            }
        }),
    );

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "4",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-env",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 0,
        "result must be recovered from production envelope in transcript: stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["type"], "message");
    assert_eq!(outcome["data"]["text"], "recovered envelope result");
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

#[test]
fn result_wait_soft_stop_does_not_hide_actionable_blocker() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();

    let conn = db(&h);
    let created_at: f64 = conn
        .query_row(
            "SELECT created_at FROM instances WHERE name = ?1",
            rusqlite::params![worker],
            |r| r.get(0),
        )
        .unwrap();

    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    conn.execute(
        "UPDATE instances SET tool = 'hermes', status = 'blocked' WHERE name = ?1",
        rusqlite::params![worker],
    )
    .unwrap();

    // Soft stop first
    seed_test_event(
        &h,
        "life",
        &worker,
        json!({
            "action": "stopped",
            "status": "stopped",
            "soft": true,
            "snapshot": {
                "name": worker,
                "created_at": created_at,
                "tool": "hermes",
            }
        }),
    );

    // Later actionable blocker
    seed_test_event(
        &h,
        "status",
        &worker,
        json!({
            "status": "blocked",
            "context": "approval",
            "detail": "user approval required",
        }),
    );

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "2",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-soft-blocker",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 4,
        "soft stop must not hide later actionable blocker (exit 4): stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["outcome"], "blocked");
    assert_eq!(outcome["result_blocked"], true);
    assert_eq!(outcome["worker"], worker);
}

#[test]
fn result_wait_soft_stop_alone_leads_deadline() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();

    let conn = db(&h);
    let created_at: f64 = conn
        .query_row(
            "SELECT created_at FROM instances WHERE name = ?1",
            rusqlite::params![worker],
            |r| r.get(0),
        )
        .unwrap();

    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    conn.execute(
        "UPDATE instances SET tool = 'hermes' WHERE name = ?1",
        rusqlite::params![worker],
    )
    .unwrap();

    // Soft stop alone (resumable). Wait beyond the two-second recovery grace
    // so the old terminal-soft-stop bug would exit unavailable before timeout.
    seed_test_event(
        &h,
        "life",
        &worker,
        json!({
            "action": "stopped",
            "status": "stopped",
            "soft": true,
            "snapshot": {
                "name": worker,
                "created_at": created_at,
                "tool": "hermes",
            }
        }),
    );

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "4",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-soft-deadline",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 1,
        "soft stop alone must lead to deadline timeout (exit 1), not unavailable (exit 3): stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["timed_out"], true);
    assert_eq!(outcome["outcome"], "deadline");
    assert_eq!(outcome["worker"], worker);
}

#[test]
fn result_wait_same_generation_result_after_soft_stop_satisfies_result() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();

    let conn = db(&h);
    let created_at: f64 = conn
        .query_row(
            "SELECT created_at FROM instances WHERE name = ?1",
            rusqlite::params![worker],
            |r| r.get(0),
        )
        .unwrap();

    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    conn.execute(
        "UPDATE instances SET tool = 'hermes' WHERE name = ?1",
        rusqlite::params![worker],
    )
    .unwrap();

    // Worker experiences a resumable soft stop
    seed_test_event(
        &h,
        "life",
        &worker,
        json!({
            "action": "stopped",
            "status": "stopped",
            "soft": true,
            "snapshot": {
                "name": worker,
                "created_at": created_at,
                "tool": "hermes",
            }
        }),
    );

    // Later in the same generation, worker delivers the report
    let (send_code, _, send_err) = h.run([
        "send",
        &format!("@{caller}"),
        "--name",
        &worker,
        "--intent",
        "inform",
        "--thread",
        "workflow-soft-report",
        "--",
        "report delivered after soft stop",
    ]);
    assert_eq!(send_code, 0, "stderr={send_err}");

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "2",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-soft-report",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 0,
        "same-generation result after soft stop must satisfy wait: stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["type"], "message");
    assert_eq!(outcome["data"]["text"], "report delivered after soft stop");
    assert_eq!(
        outcome["data"]["sender_instance_key"],
        format!("{worker}@{created_at:.6}")
    );
}

#[test]
fn result_wait_hard_stop_unsupported_provider_exits_unavailable() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();

    let conn = db(&h);
    let created_at: f64 = conn
        .query_row(
            "SELECT created_at FROM instances WHERE name = ?1",
            rusqlite::params![worker],
            |r| r.get(0),
        )
        .unwrap();

    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();

    conn.execute(
        "UPDATE instances SET tool = 'hermes', status = 'stopped' WHERE name = ?1",
        rusqlite::params![worker],
    )
    .unwrap();

    // Actual terminal hard stop (without soft: true) for unsupported provider
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
                "tool": "hermes",
            }
        }),
    );

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "3",
        "--after-id",
        &cursor,
        "--thread",
        "workflow-hard-unavail",
        "--result-from",
        &worker,
    ]);
    assert_eq!(
        code, 3,
        "hard stop for unsupported provider must exit 3 (unavailable): stdout={stdout} stderr={stderr}"
    );
    let outcome = parse_first_json_line(&stdout);
    assert_eq!(outcome["result_unavailable"], true);
    assert_eq!(outcome["outcome"], "stopped_without_result");
    assert!(
        outcome["reason"]
            .as_str()
            .unwrap()
            .contains("provider 'hermes' does not support")
    );
}
