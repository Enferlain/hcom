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
        "CREATE TABLE sessions (id TEXT PRIMARY KEY, title TEXT, cwd TEXT);
         CREATE TABLE messages (
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
            "INSERT OR IGNORE INTO sessions (id, title, cwd) VALUES (?, ?, '/test')",
            params![sid, sid],
        )
        .unwrap();
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
    let value = serde_json::from_str(&stdout).unwrap_or(Value::Null);
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
    assert!(results.iter().all(|r| r["session_id"] == "same-sid"));
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
        rows.iter().any(|r| {
            let row_path = PathBuf::from(r["path"].as_str().unwrap());
            let canonical_row = std::fs::canonicalize(&row_path).unwrap_or(row_path);
            let canonical_a = std::fs::canonicalize(&a).unwrap_or_else(|_| a.clone());
            r["hcom_name"] == "hira"
                && r["session_id"] == "same-sid"
                && canonical_row == canonical_a
        }),
        "expected result for hira with same-sid pointing to physical database a: {result}"
    );
    assert!(
        rows.iter().any(|r| {
            let row_path = PathBuf::from(r["path"].as_str().unwrap());
            let canonical_row = std::fs::canonicalize(&row_path).unwrap_or(row_path);
            let canonical_b = std::fs::canonicalize(&b).unwrap_or_else(|_| b.clone());
            r["hcom_name"] == "mira"
                && r["session_id"] == "same-sid"
                && canonical_row == canonical_b
        }),
        "expected result for mira with same-sid pointing to physical database b: {result}"
    );
}

#[test]
fn tracked_search_propagates_regex_and_database_errors() {
    let h = fixture();
    let path = profile(&h, "profile-a", &[("sid", "needle")]);
    track(&h, "hira", "sid", &path);
    let (code, _, stderr) = search(&h, "[invalid", &[]);
    assert_eq!(code, 1);
    assert!(stderr.to_lowercase().contains("regex"), "{stderr}");
    std::fs::write(&path, b"not a SQLite database").unwrap();
    let (code, _, stderr) = search(&h, "needle", &[]);
    assert_eq!(code, 1);
    assert!(
        ["sqlite", "hermes", "database"]
            .iter()
            .any(|word| stderr.to_lowercase().contains(word)),
        "{stderr}"
    );
}

#[test]
fn all_search_one_valid_one_corrupt_returns_partial_results_and_exits_one() {
    let h = fixture();
    let a = profile(&h, "profile-a", &[("sid-valid", "needle active")]);
    let dir_b = h.root_path().join("profile-b");
    std::fs::create_dir_all(&dir_b).unwrap();
    let b = dir_b.join("state.db");
    std::fs::write(&b, b"not a valid SQLite database").unwrap();

    track(&h, "hira", "sid-valid", &a);
    track(&h, "mira", "sid-corrupt", &b);

    let (code, result, stderr) = search(&h, "needle", &["--all"]);
    assert_eq!(code, 1, "expected exit 1 on corrupt database, got: {code}");
    assert!(
        ["sqlite", "database", "hermes"]
            .iter()
            .any(|word| stderr.to_lowercase().contains(word)),
        "stderr should explain failure: {stderr}"
    );

    assert_eq!(
        result["partial"], true,
        "result should have partial: true: {result}"
    );
    assert!(
        result["errors"].as_array().map_or(0, |e| e.len()) >= 1,
        "result should have recorded errors: {result}"
    );
    assert_eq!(
        result["count"], 1,
        "valid matches should be retained in result: {result}"
    );
    let rows = result["results"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["hcom_name"], "hira");
    assert_eq!(rows[0]["path"], a.to_str().unwrap());
}

#[test]
fn all_search_limit_does_not_hide_subsequent_corrupt_database() {
    let h = fixture();
    let a = profile(
        &h,
        "profile-a",
        &[("sid-1", "needle match one"), ("sid-2", "needle match two")],
    );
    let dir_b = h.root_path().join("profile-b");
    std::fs::create_dir_all(&dir_b).unwrap();
    let b = dir_b.join("state.db");
    std::fs::write(&b, b"not a valid SQLite database").unwrap();

    track(&h, "hira", "sid-1", &a);
    track(&h, "mira", "sid-corrupt", &b);

    let (code, result, stderr) = search(&h, "needle", &["--all", "--limit", "1"]);
    assert_eq!(
        code, 1,
        "expected exit 1 when corrupt database exists even if limit is reached: {code}"
    );
    assert!(
        ["sqlite", "database", "hermes"]
            .iter()
            .any(|word| stderr.to_lowercase().contains(word)),
        "stderr should explain failure: {stderr}"
    );

    assert_eq!(
        result["partial"], true,
        "result should have partial: true: {result}"
    );
    assert!(
        result["errors"].as_array().map_or(0, |e| e.len()) >= 1,
        "result should have recorded errors: {result}"
    );
    assert_eq!(
        result["count"], 1,
        "only limit number of matches should be retained in result: {result}"
    );
    let rows = result["results"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["hcom_name"], "hira");
    assert_eq!(rows[0]["path"], a.to_str().unwrap());
}
