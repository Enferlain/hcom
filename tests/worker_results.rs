//! Correlated result outcomes and stopped-provider recovery.
#[path = "support/event_observation.rs"]
mod event_fixture;
mod support;
use event_fixture::{db, parse_first_json_line, seed_test_event};
use serde_json::json;
use support::Hcom;

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
