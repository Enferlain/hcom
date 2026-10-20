//! Hermes Agent transcript parser (state.db SQLite).
//!
//! Hermes stores sessions in `$HERMES_HOME/state.db`, else `~/.hermes/state.db`,
//! table `messages`:
//! id, session_id, role (user|assistant|tool), content, tool_call_id,
//! tool_calls (JSON), tool_name, timestamp (unix float), token_count.
//! A "turn" = user message followed by assistant/tool rows until the next
//! user message. Read-only connection; the DB may be live (WAL mode).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde_json::{Value, json};

use super::TranscriptSearchMatch;
use super::shared::{
    Exchange, ToolUse, dedup_sorted_capped, finalize_action_text, normalize_tool_name, truncate_str,
};

fn normalize_hermes_tool_name(name: &str) -> &str {
    match name {
        "terminal" => "Bash",
        _ => normalize_tool_name(name),
    }
}

fn tool_file(input: &Value) -> Option<String> {
    let obj = input.as_object()?;
    for field in ["path", "file_path", "file"] {
        if let Some(val) = obj.get(field).and_then(Value::as_str)
            && !val.is_empty()
        {
            return Some(
                Path::new(val)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(val)
                    .to_string(),
            );
        }
    }
    None
}

fn tool_command(input: &Value) -> Option<String> {
    input
        .get("command")
        .or_else(|| input.get("cmd"))
        .and_then(Value::as_str)
        .map(|s| truncate_str(s, 80).to_string())
}

fn is_error_content(content: &str) -> bool {
    content.len() < 2000 && (content.contains("error:") || content.contains("Error:"))
}

/// Resolve existing Hermes SQLite state DB path.
/// `HERMES_HOME` IS the hermes home itself (like `~/.hermes`) and is always
/// treated as a directory — even when its basename is `state.db`, Hermes
/// itself reads `$HERMES_HOME/state.db` — so the DB sits directly at
/// `$HERMES_HOME/state.db`. Without it, mirror the plugin root
/// (`hermes_plugin_root`): `~/.hermes`.
pub(crate) fn get_hermes_db_path() -> Option<PathBuf> {
    let explicit = std::env::var("HERMES_HOME")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from);
    let base = match explicit {
        Some(home) => home,
        None => dirs::home_dir().unwrap_or_default().join(".hermes"),
    };
    let direct = base.join("state.db");
    if direct.exists() {
        return Some(direct);
    }
    None
}

/// Resolve Hermes SQLite state DB path from an explicit environment map,
/// falling back to process environment and the default tool config root.
///
/// If `HERMES_HOME` is present in `raw_env` or process environment, the DB
/// sits at `$HERMES_HOME/state.db` — `HERMES_HOME` is always a directory,
/// never the DB file itself. When `HERMES_HOME` is explicitly specified, the
/// resulting path is returned even if the DB file has not been created yet.
/// When falling back to the default hermes home
/// (`~/.hermes`), the path is only returned if `state.db`
/// exists.
pub(crate) fn resolve_hermes_db_path_from_env(
    raw_env: &HashMap<String, String>,
) -> Option<PathBuf> {
    let explicit = raw_env
        .get("HERMES_HOME")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("HERMES_HOME")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(PathBuf::from)
        });
    let base = match explicit {
        Some(ref home) => home.clone(),
        None => dirs::home_dir().unwrap_or_default().join(".hermes"),
    };
    let direct = base.join("state.db");
    if explicit.is_some() || direct.exists() {
        Some(direct)
    } else {
        None
    }
}

/// Search Hermes sessions in the state database for a pattern.
pub(crate) fn search_hermes_sessions(
    db_path: &Path,
    pattern: &str,
    limit: usize,
) -> Result<Vec<TranscriptSearchMatch>, String> {
    let re = Regex::new(pattern).map_err(|e| format!("Invalid regex: {e}"))?;
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| format!("Cannot open Hermes state DB: {e}"))?;

    let mut stmt = conn
        .prepare("SELECT session_id, content FROM messages WHERE active IS NOT 0 ORDER BY id ASC")
        .map_err(|e| format!("Query error: {e}"))?;

    let mut by_session: HashMap<String, TranscriptSearchMatch> = HashMap::new();
    let mut order = Vec::new();
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0).unwrap_or_default(),
                row.get::<_, String>(1).unwrap_or_default(),
            ))
        })
        .map_err(|e| format!("Query error: {e}"))?;

    for row in rows {
        let (session_id, content) = row.map_err(|e| format!("Query error: {e}"))?;
        if session_id.is_empty() || content.is_empty() || !re.is_match(&content) {
            continue;
        }

        let entry = by_session.entry(session_id.clone()).or_insert_with(|| {
            order.push(session_id.clone());
            TranscriptSearchMatch {
                path: db_path.to_string_lossy().to_string(),
                agent: "hermes".to_string(),
                line: 0,
                text: truncate_str(&content.replace('\n', " "), 100).to_string(),
                matches: 0,
                session_id: Some(session_id.clone()),
                label: None,
            }
        });
        entry.matches += 1;
    }

    Ok(order
        .into_iter()
        .filter_map(|session_id| by_session.remove(&session_id))
        .take(limit)
        .collect())
}

/// Search a specific Hermes session in the state database for a pattern.
pub(crate) fn search_hermes_session(
    db_path: &Path,
    session_id: &str,
    pattern: &str,
) -> Result<Option<TranscriptSearchMatch>, String> {
    if session_id.trim().is_empty() {
        return Ok(None);
    }
    let re = Regex::new(pattern).map_err(|e| format!("Invalid regex: {e}"))?;
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| format!("Cannot open Hermes state DB: {e}"))?;

    let mut stmt = conn
        .prepare(
            "SELECT content FROM messages WHERE session_id = ? AND active IS NOT 0 ORDER BY id ASC",
        )
        .map_err(|e| format!("Query error: {e}"))?;

    let mut result: Option<TranscriptSearchMatch> = None;
    let rows = stmt
        .query_map(rusqlite::params![session_id], |row| {
            Ok(row.get::<_, String>(0).unwrap_or_default())
        })
        .map_err(|e| format!("Query error: {e}"))?;

    for row in rows {
        let content = row.map_err(|e| format!("Query error: {e}"))?;
        if content.is_empty() || !re.is_match(&content) {
            continue;
        }

        match result {
            None => {
                result = Some(TranscriptSearchMatch {
                    path: db_path.to_string_lossy().to_string(),
                    agent: "hermes".to_string(),
                    line: 0,
                    text: truncate_str(&content.replace('\n', " "), 100).to_string(),
                    matches: 1,
                    session_id: Some(session_id.to_string()),
                    label: None,
                });
            }
            Some(ref mut m) => {
                m.matches += 1;
            }
        }
    }

    Ok(result)
}

pub(crate) fn parse_hermes_sqlite(
    db_path: &Path,
    session_id: &str,
    last: usize,
) -> Result<Vec<Exchange>, String> {
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| format!("Cannot open Hermes state DB: {e}"))?;

    // One row per message, oldest first. tool_calls carries the outbound
    // call JSON array on assistant rows; tool_name/content on role='tool'
    // rows carry the result.
    let mut stmt = conn
        .prepare(
            "SELECT role, content, tool_calls, tool_name, tool_call_id, timestamp \
             FROM messages WHERE session_id = ? AND active IS NOT 0 ORDER BY id ASC",
        )
        .map_err(|e| format!("Query error: {e}"))?;

    struct Row {
        role: String,
        content: String,
        tool_calls: String,
        tool_name: String,
        tool_call_id: String,
        timestamp: f64,
    }

    let rows: Vec<Row> = stmt
        .query_map(rusqlite::params![session_id], |row| {
            Ok(Row {
                role: row.get::<_, String>(0).unwrap_or_default(),
                content: row.get::<_, String>(1).unwrap_or_default(),
                tool_calls: row.get::<_, String>(2).unwrap_or_default(),
                tool_name: row.get::<_, String>(3).unwrap_or_default(),
                tool_call_id: row.get::<_, String>(4).unwrap_or_default(),
                timestamp: row.get::<_, f64>(5).unwrap_or(0.0),
            })
        })
        .map_err(|e| format!("Row mapping error: {e}"))?
        .filter_map(|r| r.ok())
        .collect();

    if rows.is_empty() {
        return Err(format!("No messages for session {session_id}"));
    }

    // Group into exchanges keyed by leading user message.
    struct Partial {
        user: String,
        action: Vec<String>,
        files: Vec<String>,
        tools: Vec<ToolUse>,
        timestamp: f64,
        errors: Vec<Value>,
    }

    let mut exchanges: Vec<Partial> = Vec::new();
    // Cache of tool_call_id -> (name, input) from assistant tool_calls JSON.
    let mut call_index: HashMap<String, (String, Value)> = HashMap::new();

    for row in rows {
        match row.role.as_str() {
            "user" => {
                // Skip injected ephemeral context blocks.
                let text = row.content.trim().to_string();
                if text.is_empty() {
                    continue;
                }
                exchanges.push(Partial {
                    user: truncate_str(&text, 2000).to_string(),
                    action: Vec::new(),
                    files: Vec::new(),
                    tools: Vec::new(),
                    timestamp: row.timestamp,
                    errors: Vec::new(),
                });
            }
            "assistant" => {
                if exchanges.is_empty() {
                    continue;
                }
                let ex = exchanges.last_mut().expect("nonempty");
                if !row.content.trim().is_empty() {
                    ex.action
                        .push(truncate_str(row.content.trim(), 500).to_string());
                }
                if !row.tool_calls.trim().is_empty()
                    && let Ok(Value::Array(calls)) = serde_json::from_str::<Value>(&row.tool_calls)
                {
                    for call in calls {
                        let id = call
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let name = call
                            .get("function")
                            .and_then(|f| f.get("name"))
                            .and_then(Value::as_str)
                            .unwrap_or("tool")
                            .to_string();
                        let input = call
                            .get("function")
                            .and_then(|f| f.get("arguments"))
                            .cloned()
                            .unwrap_or_else(|| json!({}));
                        let parsed_input = if let Some(args_str) = input.as_str() {
                            serde_json::from_str::<Value>(args_str).unwrap_or(input)
                        } else {
                            input
                        };
                        if !id.is_empty() {
                            call_index.insert(id, (name.clone(), parsed_input.clone()));
                        }
                        let norm_name = normalize_hermes_tool_name(&name).to_string();
                        let file = tool_file(&parsed_input);
                        let command = tool_command(&parsed_input);
                        if let Some(ref f) = file
                            && !ex.files.contains(f)
                        {
                            ex.files.push(f.clone());
                        }
                        ex.tools.push(ToolUse {
                            name: norm_name,
                            is_error: false,
                            file,
                            command,
                            output: None,
                        });
                    }
                }
            }
            "tool" => {
                if exchanges.is_empty() {
                    continue;
                }
                let ex = exchanges.last_mut().expect("nonempty");
                let (name, input) = call_index
                    .get(&row.tool_call_id)
                    .cloned()
                    .unwrap_or_else(|| (row.tool_name.clone(), json!({})));
                let output = truncate_str(row.content.trim(), 400).to_string();
                let is_error = is_error_content(&row.content);
                let norm_name = normalize_hermes_tool_name(&name).to_string();
                let file = tool_file(&input);
                let command = tool_command(&input);
                if let Some(ref f) = file
                    && !ex.files.contains(f)
                {
                    ex.files.push(f.clone());
                }
                if is_error {
                    ex.errors.push(json!({
                        "tool": norm_name,
                        "content": output,
                    }));
                }

                if let Some(existing) = ex
                    .tools
                    .iter_mut()
                    .rev()
                    .find(|t| t.output.is_none() && t.name == norm_name)
                {
                    existing.is_error = is_error;
                    existing.output = Some(output);
                    if existing.file.is_none() {
                        existing.file = file;
                    }
                    if existing.command.is_none() {
                        existing.command = command;
                    }
                } else {
                    ex.tools.push(ToolUse {
                        name: norm_name,
                        is_error,
                        file,
                        command,
                        output: Some(output),
                    });
                }
            }
            _ => {}
        }
    }

    let tail: Vec<Partial> = if last > 0 && exchanges.len() > last {
        exchanges.split_off(exchanges.len() - last)
    } else {
        exchanges
    };

    Ok(tail
        .into_iter()
        .enumerate()
        .map(|(i, p)| {
            let ended_on_error = p.tools.last().map(|t| t.is_error).unwrap_or(false);
            let action =
                finalize_action_text(&p.action.join(" "), &p.tools, &p.errors, ended_on_error);
            Exchange {
                position: i + 1,
                user: p.user,
                action,
                files: dedup_sorted_capped(&p.files, 10),
                timestamp: format_unix_ts(p.timestamp),
                tools: p.tools,
                edits: Vec::new(),
                errors: p.errors,
                ended_on_error,
            }
        })
        .collect())
}

fn format_unix_ts(ts: f64) -> String {
    if ts <= 0.0 {
        return String::new();
    }
    chrono::DateTime::from_timestamp(ts as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_helpers() {
        let input = json!({"path": "/a/b/cool.py"});
        assert_eq!(tool_file(&input).as_deref(), Some("cool.py"));
        let input = json!({"command": "cargo test --workspace"});
        assert!(tool_command(&input).unwrap().contains("cargo test"));
        assert_eq!(normalize_hermes_tool_name("terminal"), "Bash");
        assert_eq!(normalize_hermes_tool_name("write_file"), "Write");
    }

    fn create_test_db() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                tool_call_id TEXT DEFAULT '',
                tool_calls TEXT DEFAULT '',
                tool_name TEXT DEFAULT '',
                timestamp REAL DEFAULT 0.0,
                token_count INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1
            );",
        )
        .unwrap();
        (dir, db_path)
    }

    #[test]
    fn test_parse_hermes_sqlite_multiturn() {
        let (_dir, db_path) = create_test_db();
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        // Session 1: Turn 1 (user -> assistant with terminal call -> tool response -> assistant text)
        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_1', 'user', 'Run tests and check status', '', '', '', 1700000000.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_1', 'assistant', 'Running cargo test', '', '[{\"id\":\"call_1\",\"function\":{\"name\":\"terminal\",\"arguments\":{\"command\":\"cargo test\"}}}]', '', 1700000001.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_1', 'tool', 'test result: ok. 10 passed', 'call_1', '', 'terminal', 1700000002.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_1', 'assistant', 'All tests passed!', '', '', '', 1700000003.0)",
            [],
        ).unwrap();

        // Turn 2 (user -> assistant tool edit -> tool response)
        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_1', 'user', 'Now update file', '', '', '', 1700000010.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_1', 'assistant', '', '', '[{\"id\":\"call_2\",\"function\":{\"name\":\"write_file\",\"arguments\":{\"path\":\"/src/main.rs\"}}}]', '', 1700000011.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_1', 'tool', 'file written', 'call_2', '', 'write_file', 1700000012.0)",
            [],
        ).unwrap();

        let exchanges = parse_hermes_sqlite(&db_path, "ses_1", 10).unwrap();
        assert_eq!(exchanges.len(), 2);

        assert_eq!(exchanges[0].position, 1);
        assert_eq!(exchanges[0].user, "Run tests and check status");
        assert!(exchanges[0].action.contains("Running cargo test"));
        assert!(exchanges[0].action.contains("All tests passed!"));
        assert_eq!(exchanges[0].tools.len(), 1);
        assert_eq!(exchanges[0].tools[0].name, "Bash");
        assert_eq!(exchanges[0].tools[0].command.as_deref(), Some("cargo test"));
        assert_eq!(
            exchanges[0].tools[0].output.as_deref(),
            Some("test result: ok. 10 passed")
        );
        assert!(!exchanges[0].ended_on_error);

        assert_eq!(exchanges[1].position, 2);
        assert_eq!(exchanges[1].user, "Now update file");
        assert_eq!(exchanges[1].files, vec!["main.rs"]);
        assert_eq!(exchanges[1].tools.len(), 1);
        assert_eq!(exchanges[1].tools[0].name, "Write");
        assert!(!exchanges[1].ended_on_error);
    }

    #[test]
    fn test_parse_hermes_sqlite_session_id_isolation() {
        let (_dir, db_path) = create_test_db();
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('session_alpha', 'user', 'Alpha query', '', '', '', 1700000000.0)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('session_alpha', 'assistant', 'Alpha answer', '', '', '', 1700000001.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('session_beta', 'user', 'Beta query', '', '', '', 1700000000.0)",
            [],
        ).unwrap();

        let alpha = parse_hermes_sqlite(&db_path, "session_alpha", 10).unwrap();
        assert_eq!(alpha.len(), 1);
        assert_eq!(alpha[0].user, "Alpha query");

        let beta = parse_hermes_sqlite(&db_path, "session_beta", 10).unwrap();
        assert_eq!(beta.len(), 1);
        assert_eq!(beta[0].user, "Beta query");

        let non_existent = parse_hermes_sqlite(&db_path, "session_gamma", 10);
        assert!(non_existent.is_err());
    }

    #[test]
    fn test_parse_hermes_sqlite_tool_error() {
        let (_dir, db_path) = create_test_db();
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_err', 'user', 'Break something', '', '', '', 1700000000.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_err', 'assistant', '', '', '[{\"id\":\"c_err\",\"function\":{\"name\":\"terminal\",\"arguments\":{\"command\":\"false\"}}}]', '', 1700000001.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_err', 'tool', 'error: command returned exit code 1', 'c_err', '', 'terminal', 1700000002.0)",
            [],
        ).unwrap();

        let exchanges = parse_hermes_sqlite(&db_path, "ses_err", 10).unwrap();
        assert_eq!(exchanges.len(), 1);
        assert!(exchanges[0].ended_on_error);
        assert_eq!(exchanges[0].errors.len(), 1);
        assert!(exchanges[0].tools[0].is_error);
        assert!(exchanges[0].action.contains("ended in error"));
    }

    #[test]
    fn test_search_hermes_sessions() {
        let (_dir, db_path) = create_test_db();
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_a', 'user', 'Deploy to production server', '', '', '', 1700000000.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_b', 'user', 'Debug production issue', '', '', '', 1700000001.0)",
            [],
        ).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp) \
             VALUES ('ses_c', 'user', 'Refactor local tests', '', '', '', 1700000002.0)",
            [],
        ).unwrap();

        let matches = search_hermes_sessions(&db_path, "production", 10).unwrap();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].session_id.as_deref(), Some("ses_a"));
        assert_eq!(matches[0].agent, "hermes");
        assert_eq!(matches[1].session_id.as_deref(), Some("ses_b"));
    }

    #[test]
    #[serial_test::serial]
    fn test_get_hermes_db_path() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let dir = tempfile::tempdir().unwrap();
        let db_file = dir.path().join("state.db");
        std::fs::write(&db_file, b"").unwrap();

        unsafe {
            std::env::set_var("HERMES_HOME", dir.path());
        }

        let found = get_hermes_db_path();
        assert_eq!(found, Some(db_file));
    }

    #[test]
    #[serial_test::serial]
    fn hermes_db_path_defaults_to_home_hermes_when_hcom_dir_is_under_home() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        std::fs::create_dir_all(home.join(".hermes")).unwrap();
        std::fs::write(home.join(".hermes").join("state.db"), b"").unwrap();

        unsafe {
            std::env::remove_var("HERMES_HOME");
            std::env::set_var("HOME", &home);
            std::env::remove_var("HCOM_DIR");
        }

        // With no explicit profile, use the current user's ~/.hermes.
        assert_eq!(
            get_hermes_db_path(),
            Some(home.join(".hermes").join("state.db"))
        );
    }

    #[test]
    fn test_parse_and_search_hermes_excludes_inactive_rewind_rows() {
        let (_dir, db_path) = create_test_db();
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        // Active messages (active = 1)
        conn.execute(
            "INSERT INTO messages (session_id, role, content, active, timestamp) \
             VALUES ('ses_rewind', 'user', 'Active user question', 1, 1700000000.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, active, timestamp) \
             VALUES ('ses_rewind', 'assistant', 'Active assistant response', 1, 1700000001.0)",
            [],
        )
        .unwrap();

        // Inactive rewind messages (active = 0)
        conn.execute(
            "INSERT INTO messages (session_id, role, content, active, timestamp) \
             VALUES ('ses_rewind', 'user', 'Rewound question unique_inactive_pattern', 0, 1700000002.0)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, active, timestamp) \
             VALUES ('ses_rewind', 'assistant', 'Rewound assistant answer', 0, 1700000003.0)",
            [],
        )
        .unwrap();

        // 1. Search must ignore inactive messages
        let inactive_matches =
            search_hermes_sessions(&db_path, "unique_inactive_pattern", 10).unwrap();
        assert!(
            inactive_matches.is_empty(),
            "search_hermes_sessions should not match inactive messages"
        );

        let active_matches = search_hermes_sessions(&db_path, "Active assistant", 10).unwrap();
        assert_eq!(active_matches.len(), 1);
        assert_eq!(active_matches[0].session_id.as_deref(), Some("ses_rewind"));

        // 2. Transcript parsing must ignore inactive messages
        let exchanges = parse_hermes_sqlite(&db_path, "ses_rewind", 10).unwrap();
        assert_eq!(exchanges.len(), 1);
        assert_eq!(exchanges[0].user, "Active user question");
        assert!(exchanges[0].action.contains("Active assistant response"));
        assert!(!exchanges[0].action.contains("Rewound"));

        // 3. NULL active (legacy rows predating the NOT NULL default) counts
        // as active — the real DB carries a partial index on active IS NULL,
        // so `active = 1` would silently drop those rows.
        conn.execute(
            "INSERT INTO messages (session_id, role, content, active, timestamp) \
             VALUES ('ses_rewind', 'user', 'Legacy question', NULL, 1700000004.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, active, timestamp) \
             VALUES ('ses_rewind', 'assistant', 'Legacy answer unique_null_active', NULL, 1700000005.0)",
            [],
        ).unwrap();

        let null_matches = search_hermes_sessions(&db_path, "unique_null_active", 10).unwrap();
        assert_eq!(
            null_matches.len(),
            1,
            "NULL active must be treated as active by search"
        );
        let null_exchanges = parse_hermes_sqlite(&db_path, "ses_rewind", 10).unwrap();
        assert_eq!(
            null_exchanges.len(),
            2,
            "NULL active must be treated as active by parse"
        );
        assert!(null_exchanges[1].action.contains("unique_null_active"));
    }

    #[test]
    fn test_search_hermes_session_filters_by_session_id() {
        let (_dir, db_path) = create_test_db();
        let conn = rusqlite::Connection::open(&db_path).unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, active, timestamp) \
             VALUES ('ses_one', 'user', 'common_term in session one', 1, 1700000000.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, active, timestamp) \
             VALUES ('ses_one', 'assistant', 'common_term second match in session one', 1, 1700000001.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, active, timestamp) \
             VALUES ('ses_two', 'user', 'common_term in session two', 1, 1700000002.0)",
            [],
        )
        .unwrap();

        // Search specific session ses_one
        let match_one = search_hermes_session(&db_path, "ses_one", "common_term").unwrap();
        assert!(match_one.is_some());
        let m = match_one.unwrap();
        assert_eq!(m.matches, 2);
        assert_eq!(m.session_id.as_deref(), Some("ses_one"));
        assert!(m.text.contains("session one"));

        // Search specific session ses_two
        let match_two = search_hermes_session(&db_path, "ses_two", "common_term").unwrap();
        assert!(match_two.is_some());
        let m2 = match_two.unwrap();
        assert_eq!(m2.matches, 1);
        assert_eq!(m2.session_id.as_deref(), Some("ses_two"));

        // Search non-existent session returns None
        let match_none = search_hermes_session(&db_path, "ses_missing", "common_term").unwrap();
        assert!(match_none.is_none());

        // Empty session ID returns None
        let match_empty = search_hermes_session(&db_path, "", "common_term").unwrap();
        assert!(match_empty.is_none());
        let match_whitespace = search_hermes_session(&db_path, "   ", "common_term").unwrap();
        assert!(match_whitespace.is_none());

        // Invalid regex returns Err
        let err_regex = search_hermes_session(&db_path, "ses_one", "[invalid").unwrap_err();
        assert!(err_regex.contains("Invalid regex"));

        // Corrupt/invalid DB file returns Err
        let corrupt_path = _dir.path().join("corrupt.db");
        std::fs::write(&corrupt_path, b"not a valid sqlite database").unwrap();
        let err_db = search_hermes_session(&corrupt_path, "ses_one", "common_term").unwrap_err();
        assert!(err_db.contains("Cannot open Hermes state DB") || err_db.contains("Query error"));
    }

    #[test]
    #[serial_test::serial]
    fn hermes_home_is_always_a_directory_even_when_named_state_db() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        // A hermes home whose basename happens to be state.db is still a
        // directory: the DB sits at $HERMES_HOME/state.db.
        let home = temp.path().join("state.db");
        std::fs::create_dir_all(&home).unwrap();
        let db_file = home.join("state.db");
        std::fs::write(&db_file, b"").unwrap();

        unsafe {
            std::env::set_var("HERMES_HOME", &home);
        }
        assert_eq!(get_hermes_db_path(), Some(db_file));

        let mut raw_env = HashMap::new();
        raw_env.insert(
            "HERMES_HOME".to_string(),
            home.to_string_lossy().to_string(),
        );
        assert_eq!(
            resolve_hermes_db_path_from_env(&raw_env),
            Some(home.join("state.db"))
        );

        // HERMES_HOME naming an actual state.db FILE is not special-cased as
        // the DB either: it is treated as a directory, so the resolution nests
        // state.db under it rather than returning the file.
        let file_home = temp.path().join("file_home.db");
        std::fs::write(&file_home, b"").unwrap();
        let mut raw_env = HashMap::new();
        raw_env.insert(
            "HERMES_HOME".to_string(),
            file_home.to_string_lossy().to_string(),
        );
        assert_eq!(
            resolve_hermes_db_path_from_env(&raw_env),
            Some(file_home.join("state.db"))
        );
    }
}
