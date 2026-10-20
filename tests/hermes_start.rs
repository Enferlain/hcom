//! Integration tests for Hermes explicit start and rebind adoption contract.

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

fn run_hermes(h: &Hcom, session_id: Option<&str>, args: &[&str]) -> (i32, String, String) {
    let mut cmd = hermes_cmd(h, session_id);
    cmd.args(args);
    let output = cmd.output().expect("failed to execute hcom start");
    let code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (code, stdout, stderr)
}

#[test]
fn test_hermes_repeated_start_is_idempotent() {
    let h = fixture();
    let (code1, stdout1, stderr1) = run_hermes(&h, Some("ses-repeat-1"), &["start"]);
    assert_eq!(code1, 0, "first start failed: {stderr1}");
    assert!(
        stdout1.contains("[hcom:"),
        "expected bootstrap output with instance name"
    );

    let conn = db(&h);
    let name: String = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'ses-repeat-1'",
            [],
            |r| r.get(0),
        )
        .expect("session binding should exist");

    let (code2, stdout2, stderr2) = run_hermes(&h, Some("ses-repeat-1"), &["start"]);
    assert_eq!(code2, 0, "repeated start failed: {stderr2}");
    assert!(
        stdout2.contains(&format!("hcom already started for {name}")),
        "repeated start must report already started for {name}, got: {stdout2}"
    );

    let instances: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM instances WHERE tool = 'hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(instances, 1, "exactly one instance row should exist");

    let (status, session_id): (String, Option<String>) = conn
        .query_row(
            "SELECT status, session_id FROM instances WHERE name = ?",
            params![name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "listening", "Hermes start mints listening identity");
    assert_eq!(session_id.as_deref(), Some("ses-repeat-1"));
}

#[test]
fn test_hermes_concurrent_start_converges_on_one_identity() {
    let h = fixture();
    let mut cmd1 = hermes_cmd(&h, Some("ses-concurrent"));
    cmd1.args(["start"]);
    let mut cmd2 = hermes_cmd(&h, Some("ses-concurrent"));
    cmd2.args(["start"]);

    let barrier = Arc::new(Barrier::new(2));
    let b1 = Arc::clone(&barrier);
    let b2 = barrier;

    let t1 = std::thread::spawn(move || {
        b1.wait();
        cmd1.output().unwrap()
    });
    let t2 = std::thread::spawn(move || {
        b2.wait();
        cmd2.output().unwrap()
    });

    let o1 = t1.join().unwrap();
    let o2 = t2.join().unwrap();
    assert_eq!(o1.status.code(), Some(0));
    assert_eq!(o2.status.code(), Some(0));

    let conn = db(&h);
    let instances: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM instances WHERE tool = 'hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        instances, 1,
        "concurrent start must converge on exactly one instance"
    );

    let bound_name: String = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'ses-concurrent'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!bound_name.is_empty());
}

#[test]
fn test_hermes_profile_persistence() {
    let h = fixture();
    let profile_dir = h.root_path().join("hermes_profile_test");
    std::fs::create_dir_all(&profile_dir).unwrap();
    let state_db = profile_dir.join("state.db");
    std::fs::write(&state_db, b"").unwrap();

    let mut cmd = hermes_cmd(&h, Some("ses-profile"));
    cmd.env("HERMES_HOME", profile_dir.to_str().unwrap());
    cmd.args(["start"]);
    let output = cmd.output().unwrap();
    assert_eq!(output.status.code(), Some(0));

    let conn = db(&h);
    let transcript_path: Option<String> = conn
        .query_row(
            "SELECT transcript_path FROM instances WHERE session_id = 'ses-profile'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        transcript_path.as_deref(),
        Some(state_db.to_str().unwrap()),
        "profile state.db path must be recorded in transcript_path"
    );
}

#[test]
fn test_hermes_safe_rebind_reclaims_stopped_identity() {
    let h = fixture();
    let (c1, _, e1) = run_hermes(&h, Some("ses-orig"), &["start", "--name", "vega"]);
    assert_eq!(c1, 0, "{e1}");

    let conn = db(&h);
    conn.execute(
        "UPDATE instances SET status = 'stopped', last_event_id = 77 WHERE name = 'vega'",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO events(timestamp, type, instance, data) VALUES ('2026-10-13T00:00:00Z', 'life', 'vega', ?)",
        params![serde_json::json!({
            "action": "stopped",
            "snapshot": {
                "tool": "hermes",
                "directory": h.workspace.to_str().unwrap(),
                "session_id": "ses-orig",
                "last_event_id": 77
            }
        }).to_string()],
    )
    .unwrap();

    let (c2, stdout2, e2) = run_hermes(&h, Some("ses-new"), &["start", "--as", "vega"]);
    assert_eq!(c2, 0, "rebind failed: {e2}");
    assert!(stdout2.contains("[hcom:vega]"));

    let (session_id, status, last_event_id): (Option<String>, String, i64) = conn
        .query_row(
            "SELECT session_id, status, last_event_id FROM instances WHERE name = 'vega'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(session_id.as_deref(), Some("ses-new"));
    assert_eq!(status, "listening");
    assert_eq!(last_event_id, 77, "cursor must be preserved on rebind");

    let old_bound: Option<String> = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'ses-orig'",
            [],
            |r| r.get(0),
        )
        .ok();
    assert!(old_bound.is_none(), "old session binding must be dropped");

    let new_bound: String = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'ses-new'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(new_bound, "vega");
}

#[test]
fn test_hermes_rebind_refuses_live_identity() {
    let h = fixture();
    let (c1, _, e1) = run_hermes(&h, Some("ses-live"), &["start", "--name", "vega"]);
    assert_eq!(c1, 0, "{e1}");

    let (c2, _, stderr2) = run_hermes(&h, Some("ses-attacker"), &["start", "--as", "vega"]);
    assert_ne!(c2, 0, "rebind of live session must fail");
    assert!(
        stderr2.contains("cannot be reclaimed"),
        "error must explain that live identity cannot be reclaimed: {stderr2}"
    );

    let conn = db(&h);
    let owner: String = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'ses-live'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(owner, "vega");

    let attacker_bound: Option<String> = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'ses-attacker'",
            [],
            |r| r.get(0),
        )
        .ok();
    assert!(attacker_bound.is_none());
}

#[test]
fn test_hermes_rebind_fails_closed_without_session_id() {
    let h = fixture();
    let conn = db(&h);
    conn.execute(
        "INSERT INTO instances(name, tool, status, last_event_id, directory, created_at)
         VALUES ('mira', 'hermes', 'stopped', 10, ?, 1000.0)",
        params![h.workspace.to_str().unwrap()],
    )
    .unwrap();

    let mut cmd = hermes_cmd(&h, None);
    cmd.env("HCOM_TOOL", "hermes");
    cmd.args(["start", "--as", "mira"]);
    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_ne!(output.status.code(), Some(0));
    assert!(
        stderr.contains("no HERMES_SESSION_ID"),
        "must fail closed when Hermes session exposes no HERMES_SESSION_ID: {stderr}"
    );

    let row_exists: bool = conn
        .query_row("SELECT 1 FROM instances WHERE name = 'mira'", [], |_| {
            Ok(true)
        })
        .unwrap_or(false);
    assert!(
        row_exists,
        "target row must not be deleted on failed validation"
    );
}

#[test]
fn test_hermes_foreign_session_id_rejection() {
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

    let (code, _, stderr) = run_hermes(&h, Some("ses-foreign"), &["start"]);
    assert_ne!(code, 0, "start must refuse foreign session binding");
    assert!(
        stderr.contains("not a Hermes instance"),
        "error must mention foreign binding collision: {stderr}"
    );

    let owner: String = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'ses-foreign'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(owner, "luna");
}

#[test]
fn test_hermes_foreign_ownership_rejection_by_name() {
    let h = fixture();
    let (c1, _, e1) = run_hermes(&h, Some("ses-owner"), &["start", "--name", "kiki"]);
    assert_eq!(c1, 0, "{e1}");

    let (c2, _, stderr2) = run_hermes(&h, Some("ses-other"), &["start", "--name", "kiki"]);
    assert_ne!(
        c2, 0,
        "start --name must not steal an active identity from another session"
    );
    assert!(
        stderr2.contains("cannot be claimed here"),
        "unexpected error: {stderr2}"
    );
}

#[test]
fn test_nested_claude_precedence() {
    let h = fixture();
    let mut cmd = h.cmd();
    cmd.env("HERMES_SESSION_ID", "hermes-ambient");
    cmd.env("CLAUDECODE", "1");
    cmd.env("CLAUDE_CODE_SESSION_ID", "claude-nested");
    cmd.args(["start"]);
    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "start failed: {stderr}");

    let conn = db(&h);
    let claude_bound: Option<String> = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'claude-nested'",
            [],
            |r| r.get(0),
        )
        .ok();
    assert!(
        claude_bound.is_some(),
        "nested Claude session must be bound"
    );

    let hermes_bound: Option<String> = conn
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id = 'hermes-ambient'",
            [],
            |r| r.get(0),
        )
        .ok();
    assert!(
        hermes_bound.is_none(),
        "ambient Hermes session must not be bound when Claude takes precedence"
    );

    let name = claude_bound.unwrap();
    let tool: String = conn
        .query_row(
            "SELECT tool FROM instances WHERE name = ?",
            params![name],
            |r| r.get(0),
        )
        .unwrap();
    assert_ne!(tool, "hermes", "instance must not be registered as hermes");
}

#[test]
fn test_nested_codex_precedence_with_inherited_thread() {
    let h = fixture();
    let mut cmd = h.cmd();
    cmd.env("HERMES_SESSION_ID", "hermes-ambient");
    cmd.env("CODEX_THREAD_ID", "thread-inherited");
    cmd.env("CODEX_SESSION_ID", "sess-codex-root");
    cmd.args(["start"]);
    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "start failed: {stderr}");

    let conn = db(&h);
    let hermes_instances: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM instances WHERE tool = 'hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        hermes_instances, 0,
        "Codex markers must outrank ambient HERMES_SESSION_ID in tool detection"
    );
}
