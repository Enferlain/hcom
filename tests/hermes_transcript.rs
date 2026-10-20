//! CLI coverage for tracked Hermes databases and session attribution.

mod support;

use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};
use serde_json::{Value, json};
use support::Hcom;

fn fixture() -> Hcom {
    let h = Hcom::new();
    let (code, _, stderr) = h.run(["status", "--json"]);
    assert_eq!(code, 0, "{stderr}");
    h
}

fn profile(h: &Hcom, name: &str, messages: &[(&str, &str)]) -> PathBuf {
    let dir = h.root_path().join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL, role TEXT NOT NULL, content TEXT NOT NULL,
            tool_call_id TEXT DEFAULT '', tool_calls TEXT DEFAULT '',
            tool_name TEXT DEFAULT '', timestamp REAL DEFAULT 0,
            token_count INTEGER DEFAULT 0, active INTEGER DEFAULT 1
        );",
    )
    .unwrap();
    for (sid, text) in messages {
        conn.execute(
            "INSERT INTO messages(session_id,role,content,timestamp) VALUES (?,'user',?,1700000000)",
            params![sid, text],
        )
        .unwrap();
    }
    path
}

fn track(h: &Hcom, name: &str, sid: &str, path: &Path) {
    Connection::open(h.path().join("hcom.db"))
        .unwrap()
        .execute(
            "INSERT INTO instances(name,tool,session_id,transcript_path,status,created_at)
             VALUES (?,'hermes',?,?,'active',1000)",
            params![name, sid, path.to_str().unwrap()],
        )
        .unwrap();
}

fn stop_snapshot(h: &Hcom, name: &str, sid: &str, path: &Path) {
    Connection::open(h.path().join("hcom.db"))
        .unwrap()
        .execute(
            "INSERT INTO events(timestamp,type,instance,data) VALUES (?, 'life', ?, ?)",
            params![
                "2026-10-11T00:00:00Z",
                name,
                json!({"action":"stopped", "snapshot":{
                    "tool":"hermes", "session_id":sid, "transcript_path":path
                }})
                .to_string()
            ],
        )
        .unwrap();
}

fn search(h: &Hcom, pattern: &str, extra: &[&str]) -> (i32, Value, String) {
    let mut cmd = h.cmd();
    cmd.env("HERMES_HOME", h.root_path().join("unrelated-profile"))
        .args([
            "transcript",
            "search",
            pattern,
            "--agent",
            "hermes",
            "--json",
        ])
        .args(extra);
    let out = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let code = out.status.code().unwrap_or(-1);
    let value = if code == 0 {
        serde_json::from_str(&stdout).unwrap_or_else(|err| panic!("{err}: {stdout}"))
    } else {
        Value::Null
    };
    (
        code,
        value,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn tracked_search_keeps_session_scope_and_live_boundary() {
    let h = fixture();
    let a = profile(
        &h,
        "profile-a",
        &[("same-sid", "needle active"), ("foreign", "needle private")],
    );
    let b = profile(&h, "profile-b", &[("same-sid", "needle stopped")]);
    track(&h, "hira", "same-sid", &a);
    track(&h, "blank", "", &a);
    stop_snapshot(&h, "mira", "same-sid", &b);

    let (code, tracked, stderr) = search(&h, "needle", &[]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(tracked["count"], 2);
    let results = tracked["results"].as_array().unwrap();
    assert!(
        results
            .iter()
            .all(|r| r["session_id"] == "same-sid" && r.get("label").is_some())
    );
    assert!(
        results
            .iter()
            .any(|r| r["hcom_name"] == "hira" && r["text"].as_str().unwrap().contains("active"))
    );
    assert!(
        results
            .iter()
            .any(|r| r["hcom_name"] == "mira" && r["text"].as_str().unwrap().contains("stopped"))
    );
    assert!(!tracked.to_string().contains("private"));

    let (code, live, stderr) = search(&h, "needle", &["--live"]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(live["count"], 1);
    assert_eq!(live["results"][0]["hcom_name"], "hira");
}

#[test]
fn all_search_finds_recorded_profiles_and_correlates_path_plus_session() {
    let h = fixture();
    let a = profile(&h, "profile-a", &[("same-sid", "needle active")]);
    let b = profile(&h, "profile-b", &[("same-sid", "needle stopped")]);
    track(&h, "hira", "same-sid", &a);
    stop_snapshot(&h, "mira", "same-sid", &b);
    #[cfg(unix)]
    {
        let alias = h.root_path().join("alias.db");
        std::os::unix::fs::symlink(&a, &alias).unwrap();
        track(&h, "alias", "", &alias);
    }
    let (code, result, stderr) = search(&h, "needle", &["--all"]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(result["count"], 2, "{result}");
    let rows = result["results"].as_array().unwrap();
    assert!(
        rows.iter()
            .any(|r| r["hcom_name"] == "hira" && r["path"] == a.to_str().unwrap())
    );
    assert!(
        rows.iter()
            .any(|r| r["hcom_name"] == "mira" && r["path"] == b.to_str().unwrap())
    );
}

#[test]
fn tracked_search_propagates_regex_and_database_errors() {
    let h = fixture();
    let path = profile(&h, "profile-a", &[("sid", "needle")]);
    track(&h, "hira", "sid", &path);
    let (code, _, stderr) = search(&h, "[invalid", &[]);
    assert_eq!(code, 1);
    assert!(stderr.contains("Invalid regex"), "{stderr}");
    std::fs::write(&path, b"not a SQLite database").unwrap();
    let (code, _, stderr) = search(&h, "needle", &[]);
    assert_eq!(code, 1);
    assert!(
        stderr.contains("Query error") || stderr.contains("Cannot open Hermes"),
        "{stderr}"
    );
}
