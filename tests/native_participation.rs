//! Native participation through the real CLI, in isolated profiles.
mod support;

use std::io::Write;
use std::process::{Command, Stdio};

use rusqlite::{Connection, params};
use serde_json::{Value, json};
use support::Hcom;

fn db(h: &Hcom) -> Connection {
    Connection::open(h.path().join("hcom.db")).unwrap()
}
fn fixture() -> Hcom {
    let h = Hcom::new();
    assert_eq!(h.run(["status", "--json"]).0, 0);
    h
}

#[test]
fn ordinary_hermes_cli_start_and_read_only_sender_share_one_session() {
    let h = fixture();
    let recipient = h.start();
    let run = |args: &[&str]| {
        let mut cmd = h.cmd();
        for key in [
            "HCOM_PROCESS_ID",
            "HCOM_LAUNCHED",
            "CLAUDECODE",
            "CLAUDE_CODE_SESSION_ID",
            "HCOM_NATIVE_PARTICIPATION",
            "HCOM_CLAUDE_UNIX_SESSION_ID",
            "CODEX_THREAD_ID",
            "CODEX_SANDBOX",
        ] {
            cmd.env_remove(key);
        }
        cmd.env("HERMES_SESSION_ID", "ordinary-session")
            .args(args)
            .output()
            .unwrap()
    };
    let target = format!("@{recipient}");
    let send = [
        "send",
        target.as_str(),
        "--intent",
        "inform",
        "--",
        "ordinary-hermes-sentinel",
    ];
    assert!(
        !run(&send).status.success(),
        "unknown sender must not enroll itself"
    );
    assert_eq!(
        db(&h)
            .query_row("SELECT count(*) FROM instances", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    for _ in 0..2 {
        let out = run(&["start"]);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let conn = db(&h);
    let (count, name): (i64, String) = conn.query_row(
        "SELECT count(*), min(name) FROM instances WHERE tool='hermes' AND session_id='ordinary-session'", [],
        |r| Ok((r.get(0)?,r.get(1)?)),
    ).unwrap();
    assert_eq!(count, 1, "repeated start must reuse the enrolled session");
    let out = run(&send);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let sender: String = conn.query_row(
        "SELECT json_extract(data,'$.from') FROM events WHERE type='message' AND json_extract(data,'$.text')='ordinary-hermes-sentinel'", [], |r| r.get(0),
    ).unwrap();
    assert_eq!(sender, name);
    conn.execute("UPDATE instances SET tool='claude' WHERE name=?", [&name])
        .unwrap();
    assert!(
        !run(&send).status.success(),
        "a foreign tool row cannot identify a Hermes sender"
    );
}

fn native_command(h: &Hcom) -> Command {
    let mut cmd = h.cmd();
    cmd.env("CLAUDECODE", "1");
    cmd
}
fn hook(h: &Hcom, event: &str, payload: Value, env_file: &std::path::Path) -> std::process::Output {
    let mut cmd = native_command(h);
    cmd.args(["hooks", "native", "run", "claude", event])
        .env("CLAUDE_ENV_FILE", env_file)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(payload.to_string().as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn native_install_is_idempotent_and_preserves_foreign_settings() {
    let h = fixture();
    let settings = h.claude_home.join("settings.json");
    let foreign = json!({"env":{"PROVIDER":"keep"},"hooks":{"SessionStart":[
        {"hooks":[{"type":"command","command":"foreign-hook"}]}]}});
    std::fs::write(&settings, foreign.to_string()).unwrap();
    for operation in ["add", "add", "status"] {
        let (code, stdout, stderr) = h.run(["hooks", "native", operation, "claude", "--json"]);
        assert_eq!(code, 0, "{stderr}");
        let status: Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(status["installed"], true);
        assert_eq!(status["owned_hooks"], status["expected_hooks"]);
    }
    let (code, _, stderr) = h.run(["hooks", "remove", "claude"]);
    assert_eq!(code, 0, "{stderr}");
    let (code, stdout, stderr) = h.run(["hooks", "native", "status", "claude", "--json"]);
    assert_eq!(code, 0, "{stderr}");
    let status: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        status["installed"], true,
        "per-run cleanup must preserve explicit native declarations"
    );
    let (code, _, stderr) = h.run(["hooks", "native", "remove", "claude", "--json"]);
    assert_eq!(code, 0, "{stderr}");
    let after: Value = serde_json::from_slice(&std::fs::read(&settings).unwrap()).unwrap();
    assert_eq!(after, foreign);
}

#[test]
fn session_start_handshake_does_not_adopt_pending_or_foreign_rows() {
    let h = fixture();
    let env_file = h.root_path().join("claude.env");
    db(&h)
        .execute(
            "INSERT INTO instances(name,tool,status,created_at) VALUES ('pending','claude','inactive',0)",
            [],
        )
        .unwrap();
    for _ in 0..2 {
        let out = hook(
            &h,
            "SessionStart",
            json!({"session_id":"unknown-session","source":"startup"}),
            &env_file,
        );
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.stdout.is_empty());
    }
    let exports = std::fs::read_to_string(&env_file).unwrap();
    assert_eq!(
        exports.matches("export HCOM_NATIVE_PARTICIPATION=").count(),
        1
    );
    assert_eq!(
        exports
            .matches("export HCOM_CLAUDE_UNIX_SESSION_ID=")
            .count(),
        1
    );
    let count: i64 = db(&h)
        .query_row("SELECT COUNT(*) FROM instances", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    let bound: i64 = db(&h)
        .query_row(
            "SELECT COUNT(*) FROM session_bindings WHERE session_id='unknown-session'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound, 0);
    let out = hook(
        &h,
        "PreToolUse",
        json!({"session_id":"unknown-session","tool_name":"Bash","tool_input":{"command":"hcom list"}}),
        &env_file,
    );
    assert!(out.status.success());
    assert!(out.stdout.is_empty());
    db(&h).execute_batch("INSERT INTO instances(name,tool,session_id,status,created_at) VALUES ('foreign','codex','foreign-session','active',0);
        INSERT INTO session_bindings(session_id,instance_name,created_at) VALUES ('foreign-session','foreign',0);").unwrap();
    let out = hook(
        &h,
        "SessionStart",
        json!({"session_id":"foreign-session","source":"startup"}),
        &env_file,
    );
    assert!(out.status.success());
    assert!(out.stdout.is_empty());
    let owner: String = db(&h)
        .query_row(
            "SELECT instance_name FROM session_bindings WHERE session_id='foreign-session'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(owner, "foreign");
    let tool: String = db(&h)
        .query_row("SELECT tool FROM instances WHERE name='foreign'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(tool, "codex");
}

#[test]
fn enrolled_native_claude_shell_sends_as_validated_session() {
    let h = fixture();
    let receiver = h.start();
    db(&h)
        .execute(
            "UPDATE instances SET tool='codex',session_id='parent-codex-session' WHERE name=?",
            [&receiver],
        )
        .unwrap();
    db(&h)
        .execute(
            "INSERT INTO session_bindings(session_id,instance_name,created_at) VALUES \
             ('parent-codex-session',?,0)",
            [&receiver],
        )
        .unwrap();
    let env_file = h.root_path().join("claude.env");
    let out = hook(
        &h,
        "SessionStart",
        json!({"session_id":"native-session","source":"startup"}),
        &env_file,
    );
    assert!(out.status.success());
    let out = native_command(&h)
        .args(["start"])
        .env("HCOM_NATIVE_PARTICIPATION", "claude")
        .env("HCOM_CLAUDE_UNIX_SESSION_ID", "stale-inherited-session")
        .env("CLAUDE_CODE_SESSION_ID", "native-session")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let name: String = db(&h)
        .query_row(
            "SELECT name FROM instances WHERE session_id='native-session' AND tool='claude'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let out = hook(
        &h,
        "PreToolUse",
        json!({"session_id":"native-session","tool_name":"Bash","tool_use_id":"shell-1","tool_input":{"command":"hcom send"}}),
        &env_file,
    );
    assert!(out.status.success());
    let out = native_command(&h)
        .args([
            "send",
            &format!("@{receiver}"),
            "--intent",
            "inform",
            "--",
            "native-sender-sentinel",
        ])
        .env("HCOM_NATIVE_PARTICIPATION", "claude")
        .env("HCOM_CLAUDE_UNIX_SESSION_ID", "stale-inherited-session")
        .env("CLAUDE_CODE_SESSION_ID", "native-session")
        .env("CODEX_THREAD_ID", "parent-codex-session")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let sender: String = db(&h)
        .query_row(
            "SELECT json_extract(data,'$.from') FROM events WHERE type='message' AND \
             json_extract(data,'$.text')=?",
            ["native-sender-sentinel"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sender, name);
    let out = native_command(&h)
        .args([
            "send",
            &format!("@{receiver}"),
            "--",
            "unmarked-must-not-send",
        ])
        .env("CLAUDE_CODE_SESSION_ID", "native-session")
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "a bare session ID must not opt in native participation"
    );
    // A session binding without the validated owner is insufficient.
    db(&h)
        .execute(
            "UPDATE instances SET tool='codex' WHERE name=?",
            params![name],
        )
        .unwrap();
    let out = native_command(&h)
        .args(["send", &format!("@{receiver}"), "--", "must-not-send"])
        .env("HCOM_NATIVE_PARTICIPATION", "claude")
        .env("CLAUDE_CODE_SESSION_ID", "native-session")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let count: i64 = db(&h)
        .query_row(
            "SELECT COUNT(*) FROM events WHERE type='message' AND \
             json_extract(data,'$.text')='must-not-send'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn native_codex_dispatch_adopts_only_vscode_and_preserves_sender() {
    let h = fixture();
    let receiver = h.start();
    let invoke = |event: &str, sid: &str, originator: &str| {
        let mut cmd = h.cmd();
        cmd.args(["hooks", "native", "run", "codex", event])
            .env("CODEX_INTERNAL_ORIGINATOR_OVERRIDE", originator)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(
                json!({
                    "session_id": sid,
                    "source": "startup",
                    "cwd": h.workspace,
                    "transcript_path": h.root_path().join("native.jsonl")
                })
                .to_string()
                .as_bytes(),
            )
            .unwrap();
        child.wait_with_output().unwrap()
    };
    for event in ["SessionStart", "UserPromptSubmit"] {
        let out = invoke(event, "vscode-session", "codex_vscode");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let (count, name): (i64, String) = db(&h)
        .query_row(
            "SELECT COUNT(*), MIN(name) FROM instances WHERE tool='codex' AND \
             session_id='vscode-session'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(count, 1);
    let out = invoke("SessionStart", "wrong-originator-session", "codex_cli_rs");
    assert!(out.status.success());
    let count: i64 = db(&h)
        .query_row(
            "SELECT COUNT(*) FROM instances WHERE session_id='wrong-originator-session'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    let out = h
        .cmd()
        .args([
            "send",
            &format!("@{receiver}"),
            "--intent",
            "inform",
            "--",
            "codex-native-sentinel",
        ])
        .env("CODEX_THREAD_ID", "vscode-session")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let sender: String = db(&h)
        .query_row(
            "SELECT json_extract(data,'$.from') FROM events WHERE type='message' AND \
             json_extract(data,'$.text')='codex-native-sentinel'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sender, name);
}
