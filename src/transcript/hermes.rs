//! Hermes Agent transcripts: the profile's `state.db` (SQLite, WAL), read-only.
//!
//! One row per message: `user`, `assistant` (text and/or `tool_calls` JSON),
//! and `tool` (result, linked by `tool_call_id`). hcom text delivered mid-turn
//! lives inside the `tool` row it was appended to.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde_json::{Value, json};

use super::opencode::TranscriptSearchMatch;
use super::shared::{
    Exchange, SessionUsage, TokenUsage, ToolUse, Transcript, capture_tool_output,
    dedup_sorted_capped, display_path, final_segment, finalize_action_text, normalize_tool_name,
    tool_input_file, truncate_str,
};

fn open(path: &Path) -> Result<rusqlite::Connection, String> {
    rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("Cannot open Hermes state.db: {e}"))
}

/// User hook context is persisted in the exact API-content sidecar on newer
/// Hermes versions, while content retains the human's clean prompt.
fn content_column(conn: &rusqlite::Connection) -> String {
    let has_sidecar = conn
        .prepare("PRAGMA table_info(messages)")
        .and_then(|mut stmt| {
            stmt.query_map([], |r| r.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()
        })
        .is_ok_and(|columns| columns.iter().any(|c| c == "api_content"));
    if has_sidecar {
        "CASE WHEN m.role = 'user' THEN COALESCE(NULLIF(m.api_content, ''), m.content) ELSE m.content END".into()
    } else {
        "m.content".into()
    }
}

struct Row {
    role: String,
    content: String,
    tool_calls: Option<String>,
    tool_name: Option<String>,
    tool_call_id: Option<String>,
    timestamp: f64,
}

fn format_ts(secs: f64) -> String {
    chrono::DateTime::from_timestamp(secs as i64, 0)
        .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default()
}

/// A tool row's content is JSON (`{"output", "exit_code", "error"}`) for most
/// tools, followed by anything a plugin appended.
fn tool_output(content: &str) -> (String, bool) {
    let mut stream = serde_json::Deserializer::from_str(content).into_iter::<Value>();
    let Some(Ok(value)) = stream.next() else {
        return (content.to_string(), false);
    };
    let rest = content[stream.byte_offset()..].trim();
    let error = value.get("error").is_some_and(|e| !e.is_null() && e != "")
        || value
            .get("exit_code")
            .and_then(Value::as_i64)
            .is_some_and(|code| code != 0);
    let output = match value.get("output") {
        Some(Value::String(s)) => s.clone(),
        _ => value.to_string(),
    };
    let output = if rest.is_empty() {
        output
    } else {
        format!("{output}\n{rest}")
    };
    (output, error)
}

/// The API-content sidecar carries hcom's bootstrap after the prompt; the
/// transcript shows only what was said.
fn strip_bootstrap(text: &str) -> &str {
    text.find("<hcom_system_context>")
        .map_or(text, |i| &text[..i])
        .trim()
}

#[cfg(test)]
pub(crate) fn parse_hermes_sqlite(
    path: &Path,
    session_id: &str,
    last: usize,
) -> Result<Vec<Exchange>, String> {
    let mut transcript = parse_hermes_sqlite_with_usage(path, session_id)?;
    transcript.truncate(last);
    Ok(transcript.exchanges)
}

pub(crate) fn parse_hermes_sqlite_with_usage(
    path: &Path,
    session_id: &str,
) -> Result<Transcript, String> {
    let conn = open(path)?;
    let content = content_column(&conn);
    // Read metadata and totals together. Named optional columns also allow
    // transcripts from older schemas which did not persist token totals.
    let (cwd, session_usage) = conn
        .query_row(
            "SELECT * FROM sessions WHERE id = ?1",
            [session_id],
            |row| {
                let n = |key: &str| {
                    row.get::<_, Option<i64>>(key)
                        .ok()
                        .flatten()
                        .and_then(|v| u64::try_from(v).ok())
                };
                let tokens = TokenUsage {
                    input: n("input_tokens").unwrap_or(0),
                    output: n("output_tokens").unwrap_or(0),
                    cache_read: n("cache_read_tokens").unwrap_or(0),
                    cache_write: n("cache_write_tokens").unwrap_or(0),
                    reasoning: n("reasoning_tokens"),
                };
                let recorded = [
                    "input_tokens",
                    "output_tokens",
                    "cache_read_tokens",
                    "cache_write_tokens",
                    "reasoning_tokens",
                ]
                .iter()
                .any(|k| n(k).is_some());
                let status = row.get::<_, Option<String>>("cost_status").ok().flatten();
                let cost = row
                    .get::<_, Option<f64>>("actual_cost_usd")
                    .ok()
                    .flatten()
                    .or_else(|| {
                        if status.as_deref() == Some("included") {
                            None
                        } else {
                            row.get::<_, Option<f64>>("estimated_cost_usd")
                                .ok()
                                .flatten()
                        }
                    });
                let model = row.get::<_, Option<String>>("model").ok().flatten();
                Ok((
                    row.get::<_, Option<String>>("cwd").ok().flatten(),
                    recorded.then_some(SessionUsage {
                        tokens,
                        cost_usd: cost,
                        model,
                        ..Default::default()
                    }),
                ))
            },
        )
        .unwrap_or_default();
    let mut stmt = conn
        .prepare(&format!(
            "SELECT role, {content}, tool_calls, tool_name, tool_call_id, timestamp FROM messages m
             WHERE session_id = ?1 AND active = 1 ORDER BY id"
        ))
        .map_err(|e| format!("Unexpected Hermes state.db schema: {e}"))?;
    let rows: Vec<Row> = stmt
        .query_map([session_id], |r| {
            Ok(Row {
                role: r.get(0)?,
                content: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                tool_calls: r.get(2)?,
                tool_name: r.get(3)?,
                tool_call_id: r.get(4)?,
                timestamp: r.get(5)?,
            })
        })
        .and_then(|rows| rows.collect())
        .map_err(|e| format!("Cannot read Hermes messages: {e}"))?;

    let mut exchanges: Vec<Exchange> = Vec::new();
    let mut current: Option<Exchange> = None;
    let mut call_args: HashMap<String, Value> = HashMap::new();
    // Where the text after the latest tool call starts in the current action.
    let mut final_start = 0usize;
    let mut tool_since_text = false;

    let flush = |exchanges: &mut Vec<Exchange>,
                 current: &mut Option<Exchange>,
                 final_start: &mut usize,
                 tool_since_text: &mut bool| {
        if let Some(mut ex) = current.take() {
            ex.position = exchanges.len() + 1;
            ex.final_text = final_segment(&ex.action, std::mem::take(final_start));
            *tool_since_text = false;
            ex.action = finalize_action_text(&ex.action, &ex.tools, &ex.errors, ex.ended_on_error);
            ex.files = dedup_sorted_capped(&ex.files, 5);
            exchanges.push(ex);
        }
    };

    for row in rows {
        if row.role == "user" {
            flush(
                &mut exchanges,
                &mut current,
                &mut final_start,
                &mut tool_since_text,
            );
            current = Some(Exchange {
                usage: None,
                position: 0,
                user: strip_bootstrap(&row.content).to_string(),
                action: String::new(),
                final_text: String::new(),
                files: Vec::new(),
                timestamp: format_ts(row.timestamp),
                tools: Vec::new(),
                edits: Vec::new(),
                errors: Vec::new(),
                ended_on_error: false,
            });
            continue;
        }
        let ex = current.get_or_insert_with(|| Exchange {
            usage: None,
            position: 0,
            user: String::new(),
            action: String::new(),
            final_text: String::new(),
            files: Vec::new(),
            timestamp: format_ts(row.timestamp),
            tools: Vec::new(),
            edits: Vec::new(),
            errors: Vec::new(),
            ended_on_error: false,
        });
        match row.role.as_str() {
            "assistant" => {
                let calls: Value = row
                    .tool_calls
                    .as_deref()
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or_default();
                for call in calls.as_array().into_iter().flatten() {
                    let id = call.get("id").and_then(Value::as_str).unwrap_or("");
                    let args = call["function"]["arguments"]
                        .as_str()
                        .and_then(|raw| serde_json::from_str(raw).ok())
                        .unwrap_or_default();
                    call_args.insert(id.to_string(), args);
                }
                let text = row.content.trim();
                if !text.is_empty() {
                    if !ex.action.is_empty() {
                        ex.action.push_str("\n\n");
                    }
                    if std::mem::take(&mut tool_since_text) {
                        final_start = ex.action.len();
                    }
                    ex.action.push_str(text);
                }
            }
            "tool" => {
                let name = row.tool_name.unwrap_or_default();
                let args = row
                    .tool_call_id
                    .as_deref()
                    .and_then(|id| call_args.get(id))
                    .cloned()
                    .unwrap_or_default();
                let (output, is_error) = tool_output(&row.content);
                tool_since_text = true;
                let file = tool_input_file(&name, &args).map(|f| display_path(f, cwd.as_deref()));
                if let Some(file) = &file {
                    ex.files.push(file.clone());
                }
                if is_error {
                    ex.errors.push(json!({
                        "tool": name,
                        "content": truncate_str(&output, 300),
                    }));
                }
                ex.ended_on_error = is_error;
                ex.tools.push(ToolUse {
                    name: normalize_tool_name(if name == "terminal" { "bash" } else { &name })
                        .to_string(),
                    is_error,
                    file,
                    command: args
                        .get("command")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    output: capture_tool_output(&output),
                });
            }
            _ => {}
        }
    }
    flush(
        &mut exchanges,
        &mut current,
        &mut final_start,
        &mut tool_since_text,
    );

    Ok(Transcript {
        exchanges,
        usage: session_usage,
    })
}

/// The ambient profile's `state.db`, for `transcript search --all`.
pub(crate) fn get_hermes_db_path() -> Option<PathBuf> {
    let home = crate::hooks::hermes::hermes_home(&crate::hooks::runtime::LaunchCtx::ambient(
        crate::tool::Tool::Hermes,
        false,
    ));
    Some(home.join("state.db")).filter(|p| p.exists())
}

pub(crate) fn search_hermes_sessions(
    db_path: &Path,
    pattern: &str,
    limit: usize,
) -> Result<Vec<TranscriptSearchMatch>, String> {
    let re = Regex::new(pattern).map_err(|e| format!("Invalid regex: {e}"))?;
    let conn = open(db_path)?;
    let content = content_column(&conn);
    let mut stmt = conn
        .prepare(&format!(
            "SELECT m.session_id, COALESCE(s.title, ''), COALESCE({content}, '')
             FROM messages m LEFT JOIN sessions s ON s.id = m.session_id
             WHERE m.active = 1 AND m.role IN ('user', 'assistant') ORDER BY m.id"
        ))
        .map_err(|e| format!("Unexpected Hermes state.db schema: {e}"))?;
    let rows = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .map_err(|e| format!("Cannot read Hermes messages: {e}"))?;
    // Streamed (a profile's state.db holds every session); the first bad row
    // ends the search with an error, as it ends a transcript read.
    let mut failed = None;
    let rows = rows.map_while(|row| row.map_err(|e| failed = Some(e)).ok());
    let found = super::opencode::group_matches(db_path, "hermes", &re, rows, limit);
    match failed {
        Some(e) => Err(format!("Cannot read Hermes messages: {e}")),
        None => Ok(found),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The columns hcom reads, as Hermes v0.21 lays them out.
    fn fixture() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE sessions (id TEXT PRIMARY KEY, title TEXT, cwd TEXT);
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, role TEXT NOT NULL,
                content TEXT, tool_call_id TEXT, tool_calls TEXT, tool_name TEXT,
                timestamp REAL NOT NULL, active INTEGER NOT NULL DEFAULT 1);
            INSERT INTO sessions VALUES ('s1', 'Codewords', '/w');
            INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp, active) VALUES
              ('s1', 'user', 'old turn', NULL, NULL, NULL, 1791910800.0, 0),
              ('s1', 'user', 'Run `echo hi`', NULL, NULL, NULL, 1791910831.5, 1),
              ('s1', 'assistant', NULL, NULL,
               '[{"id": "call_1", "type": "function", "function": {"name": "terminal", "arguments": "{\"command\":\"echo hi\"}"}}]',
               NULL, 1791910835.6, 1),
              ('s1', 'tool', '{"output": "hi", "exit_code": 0, "error": null}
<hcom>from nova: codeword is PELICAN</hcom>', 'call_1', NULL, 'terminal', 1791910835.8, 1),
              ('s1', 'assistant', 'Checking.', NULL,
               '[{"id": "call_s", "type": "function", "function": {"name": "search_files", "arguments": "{\"pattern\":\"x\",\"path\":\"/w/src\"}"}}]',
               NULL, 1791910836.0, 1),
              ('s1', 'tool', '{"output": "", "exit_code": 0, "error": null}', 'call_s', NULL, 'search_files', 1791910836.5, 1),
              ('s1', 'assistant', 'PELICAN', NULL, NULL, NULL, 1791910837.9, 1),
              ('s1', 'user', 'write it', NULL, NULL, NULL, 1791910840.0, 1),
              ('s1', 'assistant', NULL, NULL,
               '[{"id": "call_2", "type": "function", "function": {"name": "write_file", "arguments": "{\"path\":\"/w/notes.md\"}"}}]',
               NULL, 1791910841.0, 1),
              ('s1', 'tool', '{"output": "", "exit_code": 1, "error": "denied"}', 'call_2', NULL, 'write_file', 1791910842.0, 1),
              ('s2', 'user', 'other session', NULL, NULL, NULL, 1791910850.0, 1);
            "#,
        )
        .unwrap();
        (dir, path)
    }

    #[test]
    fn parses_turns_tools_and_mid_turn_hcom_text() {
        let (_dir, path) = fixture();
        let exchanges = parse_hermes_sqlite(&path, "s1", 10).unwrap();
        assert_eq!(
            exchanges.len(),
            2,
            "inactive rows and other sessions skipped"
        );

        let first = &exchanges[0];
        assert_eq!(first.user, "Run `echo hi`");
        assert_eq!(first.action, "Checking.\n\nPELICAN");
        assert_eq!(first.final_text, "PELICAN");
        assert!(first.files.is_empty(), "a search directory is not a file");
        assert_eq!(first.timestamp, "2026-10-13T17:00:31Z");
        assert_eq!(first.tools[0].name, "Bash");
        assert_eq!(first.tools[0].command.as_deref(), Some("echo hi"));
        let output = first.tools[0].output.as_deref().unwrap();
        assert!(output.starts_with("hi\n<hcom>from nova"), "{output}");

        let second = &exchanges[1];
        assert_eq!(
            second.files,
            vec!["notes.md"],
            "relative to the session cwd"
        );
        assert!(second.tools[0].is_error);
        assert!(second.ended_on_error);
        assert!(second.action.contains("turn ended in error"));

        assert_eq!(parse_hermes_sqlite(&path, "s1", 1).unwrap().len(), 1);
    }

    #[test]
    fn search_groups_matches_by_session() {
        let (_dir, path) = fixture();
        let hits = search_hermes_sessions(&path, "PELICAN|session", 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].session_id.as_deref(), Some("s1"));
        assert_eq!(hits[0].label.as_deref(), Some("Codewords"));
        assert_eq!(hits[1].session_id.as_deref(), Some("s2"));
        assert_eq!(hits[1].agent, "hermes");
    }

    #[test]
    fn user_api_sidecar_is_visible_and_searchable() {
        let (_dir, path) = fixture();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("ALTER TABLE messages ADD COLUMN api_content TEXT;")
            .unwrap();
        conn.execute(
            "UPDATE messages SET api_content = ?1 WHERE role = 'user' AND content = 'Run `echo hi`'",
            ["Run `echo hi`\n\n<hcom>SIDECAR-CODEWORD</hcom>\n\n<hcom_system_context>\n[HCOM SESSION]\n</hcom_system_context>"],
        ).unwrap();
        let turns = parse_hermes_sqlite(&path, "s1", 10).unwrap();
        assert!(turns[0].user.contains("SIDECAR-CODEWORD"));
        assert!(
            !turns[0].user.contains("HCOM SESSION"),
            "bootstrap is not shown"
        );
        assert_eq!(turns[1].user, "write it");
        let hits = search_hermes_sessions(&path, "SIDECAR-CODEWORD", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].session_id.as_deref(), Some("s1"));
    }
    #[test]
    fn reads_session_totals_without_inventing_exchange_usage() {
        let (_dir, path) = fixture();
        assert!(
            parse_hermes_sqlite_with_usage(&path, "s1")
                .unwrap()
                .usage
                .is_none()
        );
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch("ALTER TABLE sessions ADD COLUMN input_tokens INTEGER; ALTER TABLE sessions ADD COLUMN output_tokens INTEGER; ALTER TABLE sessions ADD COLUMN cache_read_tokens INTEGER; ALTER TABLE sessions ADD COLUMN cache_write_tokens INTEGER; ALTER TABLE sessions ADD COLUMN reasoning_tokens INTEGER; ALTER TABLE sessions ADD COLUMN actual_cost_usd REAL; ALTER TABLE sessions ADD COLUMN estimated_cost_usd REAL; ALTER TABLE sessions ADD COLUMN cost_status TEXT; ALTER TABLE sessions ADD COLUMN model TEXT; UPDATE sessions SET input_tokens=10, output_tokens=20, cache_read_tokens=30, cache_write_tokens=4, reasoning_tokens=5, estimated_cost_usd=0.5, model='hermes' WHERE id='s1';").unwrap();
        let t = parse_hermes_sqlite_with_usage(&path, "s1").unwrap();
        let u = t.usage.unwrap();
        assert_eq!(u.tokens.input, 10);
        assert_eq!(u.tokens.output, 20);
        assert_eq!(u.tokens.cache_read, 30);
        assert_eq!(u.tokens.reasoning, Some(5));
        assert_eq!(u.cost_usd, Some(0.5));
        assert!(t.exchanges.iter().all(|e| e.usage.is_none()));
        c.execute_batch("UPDATE sessions SET cost_status='included';")
            .unwrap();
        assert_eq!(
            parse_hermes_sqlite_with_usage(&path, "s1")
                .unwrap()
                .usage
                .unwrap()
                .cost_usd,
            None
        );
        c.execute_batch("UPDATE sessions SET input_tokens=0,output_tokens=0,cache_read_tokens=0,cache_write_tokens=0,reasoning_tokens=NULL;").unwrap();
        assert_eq!(
            parse_hermes_sqlite_with_usage(&path, "s1")
                .unwrap()
                .usage
                .unwrap()
                .tokens,
            TokenUsage::default()
        );
        c.execute_batch("UPDATE sessions SET actual_cost_usd=0.2;")
            .unwrap();
        assert_eq!(
            parse_hermes_sqlite_with_usage(&path, "s1")
                .unwrap()
                .usage
                .unwrap()
                .cost_usd,
            Some(0.2)
        );
    }
}
