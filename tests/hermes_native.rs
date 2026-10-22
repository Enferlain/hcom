//! Integration tests for Hermes ordinary terminal participation and adoption contract.
//! Tests: repeats, concurrency, foreign ownership, no identifier, unknown readonly,
//! stop recovery, and launched/gateway untouched.
//!
//! Every hook RPC below drives the real current binary through the top-level
//! `hermes-*` hook CLI (`hcom hermes-start …`, what the plugin's Host.rpc
//! subprocess runs) with the ordinary-terminal env: no HCOM_PROCESS_ID, no
//! HCOM_TOOL, no other tool markers.

mod support;

use rusqlite::{Connection, params};
use std::process::Command;
use std::sync::{Arc, Barrier};
use support::Hcom;

fn fixture() -> Hcom {
    let h = Hcom::new();
    let (code, _, stderr) = h.run(["status", "--json"]);
    assert_eq!(code, 0, "{stderr}");
    h
}

fn db(h: &Hcom) -> Connection {
    Connection::open(h.path().join("hcom.db")).unwrap()
}

/// A command with the ordinary-terminal env: no launch marker, no tool
/// markers — exactly what the plugin's RPC subprocess carries.
fn hermes_cmd(h: &Hcom, session_id: Option<&str>) -> Command {
    let mut cmd = h.cmd();
    cmd.env_remove("CLAUDECODE");
    cmd.env_remove("CLAUDE_CODE_SESSION_ID");
    cmd.env_remove("HCOM_CLAUDE_UNIX_SESSION_ID");
    cmd.env_remove("HCOM_PROCESS_ID");
    cmd.env_remove("CODEX_THREAD_ID");
    cmd.env_remove("CODEX_SESSION_ID");
    cmd.env_remove("HCOM_TOOL");
    if let Some(sid) = session_id {
        cmd.env("HERMES_SESSION_ID", sid);
    } else {
        cmd.env_remove("HERMES_SESSION_ID");
    }
    cmd
}

fn capture(mut cmd: Command) -> (i32, String, String) {
    let output = cmd.output().expect("failed to execute hcom");
    let code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (code, stdout, stderr)
}

/// The plugin RPC seam: `hcom hermes-<hook> …` with the ordinary-terminal env.
/// Replies exit 0 with a JSON payload or `{"error": …}`.
fn run_hook(h: &Hcom, args: &[&str]) -> (i32, String, String) {
    let mut cmd = hermes_cmd(h, None);
    cmd.args(args);
    capture(cmd)
}

/// The CLI a user types inside an ordinary Hermes session: `hcom start` with
/// the session exported, as the terminal's own env would have it.
fn run_cli(h: &Hcom, session_id: &str, args: &[&str]) -> (i32, String, String) {
    let mut cmd = hermes_cmd(h, Some(session_id));
    cmd.args(args);
    capture(cmd)
}

fn parse(stdout: &str) -> serde_json::Value {
    serde_json::from_str(stdout).unwrap_or_default()
}

#[test]
fn test_hermes_repeated_start_is_idempotent() {
    let h = fixture();
    // 1. Hook RPC idempotency (the seam the plugin drives).
    let (code1, stdout1, stderr1) =
        run_hook(&h, &["hermes-start", "--session-id", "ses-repeat-rpc"]);
    assert_eq!(code1, 0, "first hermes-start failed: {stderr1}");
    let name1 = parse(&stdout1)["name"]
        .as_str()
        .expect("name must be a string")
        .to_string();
    assert!(!name1.is_empty());

    let (code2, stdout2, stderr2) =
        run_hook(&h, &["hermes-start", "--session-id", "ses-repeat-rpc"]);
    assert_eq!(code2, 0, "repeated hermes-start failed: {stderr2}");
    assert_eq!(parse(&stdout2)["name"].as_str(), Some(name1.as_str()));

    let conn = db(&h);
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM instances WHERE tool = 'hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "exactly one instance row should exist");

    // 2. Direct-session CLI idempotence: `hcom start` inside the session
    // binds, and repeating it must not mint a second identity.
    let (c1, _out1, err1) = run_cli(&h, "ses-repeat-cli", &["start"]);
    assert_eq!(c1, 0, "CLI start failed: {err1}");
    let (c2, out2, err2) = run_cli(&h, "ses-repeat-cli", &["start"]);
    assert_eq!(c2, 0, "repeated CLI start failed: {err2}");
    assert!(
        out2.contains("already started") || out2.contains("[hcom:"),
        "repeated CLI start must be idempotent: {out2}"
    );
}

#[test]
fn test_hermes_concurrent_start_converges_on_one_identity() {
    let h = fixture();
    let barrier = Arc::new(Barrier::new(2));

    let cmd1 = hermes_cmd(&h, None);
    let mut cmd1 = cmd1;
    cmd1.args(["hermes-start", "--session-id", "ses-concurrent-1"]);

    let cmd2 = hermes_cmd(&h, None);
    let mut cmd2 = cmd2;
    cmd2.args(["hermes-start", "--session-id", "ses-concurrent-1"]);

    let b1 = Arc::clone(&barrier);
    let t1 = std::thread::spawn(move || {
        b1.wait();
        capture(cmd1)
    });

    let b2 = barrier;
    let t2 = std::thread::spawn(move || {
        b2.wait();
        capture(cmd2)
    });

    let (c1, out1, err1) = t1.join().unwrap();
    let (c2, out2, err2) = t2.join().unwrap();
    assert_eq!(c1, 0, "thread 1 failed: {err1}");
    assert_eq!(c2, 0, "thread 2 failed: {err2}");

    let v1 = parse(&out1);
    let v2 = parse(&out2);
    assert_eq!(
        v1["name"].as_str(),
        v2["name"].as_str(),
        "concurrent starts must converge on same instance name"
    );

    let conn = db(&h);
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM instances WHERE tool = 'hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 1,
        "concurrent starts must converge on exactly one instance row"
    );
}

#[test]
fn test_hermes_foreign_ownership_rejection() {
    let h = fixture();
    let conn = db(&h);
    conn.execute(
        "INSERT INTO instances(name, tool, status, created_at) VALUES ('luna', 'claude', 'active', 1000.0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO session_bindings(session_id, instance_name, created_at) VALUES ('ses-foreign', 'luna', 0.0)",
        [],
    )
    .unwrap();

    // 1. hermes-start hook must refuse foreign session binding. The RPC
    // contract exits 0 with an error payload — assert that exact shape.
    let (code, stdout, _) = run_hook(&h, &["hermes-start", "--session-id", "ses-foreign"]);
    assert_eq!(code, 0, "hook RPC replies exit 0 even on refusal");
    let v = parse(&stdout);
    assert!(
        v["error"]
            .as_str()
            .is_some_and(|e| e.contains("not a Hermes instance")),
        "start must refuse the foreign session binding with a specific error, got: {stdout}"
    );

    // Luna remains owner
    let owner: String = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'ses-foreign'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(owner, "luna", "foreign binding must remain intact");

    // 2. Foreign ownership by name
    let (c1, out1, _) = run_hook(&h, &["hermes-start", "--session-id", "ses-owner-1"]);
    assert_eq!(c1, 0);
    let name1 = parse(&out1)["name"].as_str().unwrap().to_string();

    // hermes-status with a different session_id must be rejected
    let (c2, out2, _) = run_hook(
        &h,
        &[
            "hermes-status",
            "--name",
            &name1,
            "--status",
            "active",
            "--session-id",
            "ses-other",
        ],
    );
    assert_eq!(c2, 0, "hook RPC replies exit 0 even on refusal");
    let v2 = parse(&out2);
    assert!(
        v2["error"]
            .as_str()
            .is_some_and(|e| e.contains("different session")),
        "status update by wrong session must be rejected: {out2}"
    );
    // The rejected write must not have landed.
    let row_session: Option<String> = conn
        .query_row(
            "SELECT session_id FROM instances WHERE name = ?1",
            params![&name1],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(row_session.as_deref(), Some("ses-owner-1"));

    // A foreign tool's row is refused by name, correctly correlated or not.
    let (c3, out3, _) = run_hook(
        &h,
        &[
            "hermes-status",
            "--name",
            "luna",
            "--status",
            "active",
            "--session-id",
            "ses-foreign",
        ],
    );
    assert_eq!(c3, 0, "hook RPC replies exit 0 even on refusal");
    assert!(
        parse(&out3)["error"]
            .as_str()
            .is_some_and(|e| e.contains("not a Hermes instance")),
        "foreign-tool name must be rejected: {out3}"
    );
}

#[test]
fn test_hermes_fails_closed_without_session_id() {
    let h = fixture();
    // hermes-start requires --session-id
    let (code, stdout, _) = run_hook(&h, &["hermes-start"]);
    assert_eq!(code, 0, "hook RPC replies exit 0 even on refusal");
    let v = parse(&stdout);
    assert!(
        v["error"]
            .as_str()
            .is_some_and(|e| e.contains("Missing --session-id")),
        "must fail closed when missing --session-id: {stdout}"
    );
}

#[test]
fn test_hermes_unknown_readonly_identity_does_not_mint() {
    let h = fixture();
    let conn = db(&h);

    // Read-only session resolve mode for unknown session
    let (code, stdout, stderr) = run_hook(&h, &["hermes-read", "--session-id", "ses-unknown-ro"]);
    assert_eq!(code, 0, "{stderr}");
    let v = parse(&stdout);
    assert_eq!(v["session_id"].as_str(), Some("ses-unknown-ro"));
    assert!(
        v["name"].is_null(),
        "unknown session must resolve to null name"
    );

    // Database must have NO instances minted
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM instances WHERE tool = 'hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();

    // Attach-only start: with --existing-only an unknown session answers a
    // null name instead of minting — the shape plugin binds rely on.
    let (code_eo, out_eo, err_eo) = run_hook(
        &h,
        &[
            "hermes-start",
            "--session-id",
            "ses-no-existing",
            "--existing-only",
        ],
    );
    assert_eq!(code_eo, 0, "{err_eo}");
    let v_eo = parse(&out_eo);
    assert!(
        v_eo["name"].is_null(),
        "attach-only start must not mint: {out_eo}"
    );
    assert_eq!(v_eo["session_id"].as_str(), Some("ses-no-existing"));
    let minted: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM instances WHERE tool = 'hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(minted, 0, "attach-only start must leave no row behind");
    assert_eq!(
        count, 0,
        "read-only resolve must never mint or mutate instances"
    );

    let binding_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM session_bindings WHERE session_id = 'ses-unknown-ro'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        binding_count, 0,
        "read-only resolve must never create session bindings"
    );
}

#[test]
fn test_hermes_stop_recovery_and_rebind() {
    let h = fixture();
    let conn = db(&h);

    // 1. Create an initial hermes instance
    let (c1, out1, _) = run_hook(&h, &["hermes-start", "--session-id", "ses-orig"]);
    assert_eq!(c1, 0);
    let name = parse(&out1)["name"].as_str().unwrap().to_string();

    // 2. Soft-stop the instance
    let (c_stop, out_stop, _) = run_hook(
        &h,
        &[
            "hermes-stop",
            "--name",
            &name,
            "--soft",
            "--session-id",
            "ses-orig",
        ],
    );
    assert_eq!(c_stop, 0, "soft stop failed: {out_stop}");

    // Simulate cursor and stopped event
    conn.execute(
        "UPDATE instances SET status = 'stopped', last_event_id = 42 WHERE name = ?",
        params![&name],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO events(timestamp, type, instance, data) VALUES ('2026-10-13T00:00:00Z', 'life', ?, ?)",
        params![
            &name,
            serde_json::json!({
                "action": "stopped",
                "snapshot": {
                    "tool": "hermes",
                    "directory": h.workspace.to_str().unwrap(),
                    "session_id": "ses-orig",
                    "last_event_id": 42
                }
            }).to_string()
        ],
    )
    .unwrap();

    // 3. Start again with same session id recovers stopped instance
    let (c2, out2, _) = run_hook(&h, &["hermes-start", "--session-id", "ses-orig"]);
    assert_eq!(c2, 0);
    let v2 = parse(&out2);
    assert_eq!(v2["name"].as_str(), Some(name.as_str()));

    let (status, last_event_id): (String, i64) = conn
        .query_row(
            "SELECT status, last_event_id FROM instances WHERE name = ?",
            params![&name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "listening", "recovered instance must be listening");
    assert_eq!(last_event_id, 42, "cursor must be preserved on recovery");
}

#[test]
fn test_hermes_cli_seam_ordinary_read_and_status() {
    let h = fixture();
    let conn = db(&h);

    // Ordinary start through the plugin seam: no launch marker anywhere.
    let (c1, out1, err1) = run_hook(
        &h,
        &[
            "hermes-start",
            "--session-id",
            "ses-seam-1",
            "--cwd",
            h.workspace.to_str().unwrap(),
        ],
    );
    assert_eq!(c1, 0, "ordinary hermes-start failed: {err1}");
    let name = parse(&out1)["name"].as_str().unwrap().to_string();
    assert!(!name.is_empty());

    // Status write for the checked participant lands on the row.
    let (c2, out2, err2) = run_hook(
        &h,
        &[
            "hermes-status",
            "--name",
            &name,
            "--status",
            "active",
            "--context",
            "test",
            "--session-id",
            "ses-seam-1",
        ],
    );
    assert_eq!(c2, 0, "hermes-status failed: {err2}");
    assert_eq!(parse(&out2)["ok"], serde_json::json!(true));
    let status: String = conn
        .query_row(
            "SELECT status FROM instances WHERE name = ?1",
            params![&name],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "active");

    // Read by name returns an empty batch for a fresh mailbox.
    let (c3, out3, err3) = run_hook(
        &h,
        &["hermes-read", "--name", &name, "--session-id", "ses-seam-1"],
    );
    assert_eq!(c3, 0, "hermes-read failed: {err3}");
    let v3 = parse(&out3);
    assert_eq!(v3["messages"].as_array().map(Vec::len), Some(0));

    // Session-id resolve mode maps the enrolled session back to the name.
    let (c4, out4, err4) = run_hook(&h, &["hermes-read", "--session-id", "ses-seam-1"]);
    assert_eq!(c4, 0, "session resolve failed: {err4}");
    assert_eq!(parse(&out4)["name"].as_str(), Some(name.as_str()));

    // A claimed session that is not the row's attribution is refused.
    let (c5, out5, _) = run_hook(
        &h,
        &[
            "hermes-status",
            "--name",
            &name,
            "--status",
            "listening",
            "--session-id",
            "ses-seam-other",
        ],
    );
    assert_eq!(c5, 0, "hook RPC replies exit 0 even on refusal");
    assert!(
        parse(&out5)["error"]
            .as_str()
            .is_some_and(|e| e.contains("different session")),
        "wrong-session status must be rejected: {out5}"
    );

    // And an unknown name is refused outright.
    let (c6, out6, _) = run_hook(
        &h,
        &[
            "hermes-status",
            "--name",
            "nobody-here",
            "--session-id",
            "ses-unknown",
            "--status",
            "active",
        ],
    );
    assert_eq!(c6, 0, "hook RPC replies exit 0 even on refusal");
    assert!(
        parse(&out6)["error"]
            .as_str()
            .is_some_and(|e| e.contains("Unknown instance")),
        "unknown name must be rejected: {out6}"
    );

    // A bare --name mutator without a session id is refused: the ordinary
    // terminal must correlate its session with the row's attribution.
    let (c7, out7, _) = run_hook(
        &h,
        &["hermes-status", "--name", &name, "--status", "active"],
    );
    assert_eq!(c7, 0, "hook RPC replies exit 0 even on refusal");
    assert!(
        parse(&out7)["error"]
            .as_str()
            .is_some_and(|e| e.contains("Missing --session-id")),
        "session-less mutator must be rejected: {out7}"
    );
}

#[test]
fn test_hermes_launched_and_gateway_untouched() {
    let h = fixture();
    let conn = db(&h);

    // 1. Create a launched Hermes placeholder
    conn.execute(
        "INSERT INTO instances(name, tool, status, created_at) VALUES ('placeholder-hermes', 'hermes', 'listening', 1000.0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO process_bindings(process_id, instance_name, session_id, updated_at) VALUES ('proc-hermes-1', 'placeholder-hermes', '', 0.0)",
        [],
    )
    .unwrap();

    // 2. Start with HCOM_PROCESS_ID set (launched Hermes)
    let mut cmd = hermes_cmd(&h, None);
    cmd.env("HCOM_PROCESS_ID", "proc-hermes-1");
    cmd.args(["hermes-start", "--session-id", "ses-launched-1"]);
    let (code, stdout, stderr) = capture(cmd);
    assert_eq!(code, 0, "{stderr}");
    let v = parse(&stdout);
    assert_eq!(v["name"].as_str(), Some("placeholder-hermes"));

    // 3. Verify placeholder was bound properly and not replaced
    let bound_session: String = conn
        .query_row(
            "SELECT session_id FROM instances WHERE name = 'placeholder-hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound_session, "ses-launched-1");

    // 4. Verify gateway bridge hook dispatch is unaffected (its own
    // ungated top-level dispatch).
    let (code_bridge, stdout_bridge, _) = run_hook(&h, &["hermes-bridge", "help"]);
    assert_eq!(
        code_bridge, 0,
        "bridge RPC must retain its JSON reply contract"
    );
    assert_eq!(
        parse(&stdout_bridge)["error"].as_str(),
        Some("Unknown hermes-bridge command: help"),
        "unlaunched bridge command must reach the upstream dispatcher: {stdout_bridge}"
    );
}
