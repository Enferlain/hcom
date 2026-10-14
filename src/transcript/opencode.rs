//! OpenCode transcript parser (SQLite).

use std::path::{Path, PathBuf};

use regex::Regex;
use serde_json::{Value, json};

use super::shared::{
    Exchange, TokenUsage, ToolUse, Transcript, UsageCollector, capture_tool_output, display_path,
    finalize_action_text, normalize_tool_name, tool_call_detail, tool_input_file, truncate_str,
};
use crate::log::log_warn;

#[derive(Debug, Clone)]
pub(crate) struct TranscriptSearchMatch {
    pub path: String,
    pub agent: String,
    pub line: usize,
    pub text: String,
    pub matches: usize,
    pub session_id: Option<String>,
    pub label: Option<String>,
}

fn get_family_db_path(tool: &str) -> Option<PathBuf> {
    crate::runtime_env::opencode_family_db_path(tool).filter(|p| p.exists())
}

pub(crate) fn get_opencode_db_path() -> Option<PathBuf> {
    get_family_db_path("opencode")
}

pub(crate) fn get_kilo_db_path() -> Option<PathBuf> {
    get_family_db_path("kilo")
}

fn extract_opencode_edits(state: &Value, input: &Value) -> Vec<Value> {
    let Some(metadata) = state.get("metadata") else {
        return Vec::new();
    };
    let mut edits = Vec::new();
    if let Some(files) = metadata.get("files").and_then(Value::as_array) {
        for file in files {
            // Real opencode/kilo `apply_patch` stores the per-file diff under
            // `patch`; `diff` is kept as a fallback for other schema versions.
            let Some(diff) = file
                .get("patch")
                .or_else(|| file.get("diff"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            let path = file
                .get("filePath")
                .or_else(|| file.get("relativePath"))
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            edits.push(json!({"file": path, "diff": truncate_str(diff, 1000)}));
        }
    }
    if edits.is_empty()
        && let Some(diff) = metadata.get("diff").and_then(Value::as_str)
    {
        let path = input
            .get("file_path")
            .or_else(|| input.get("filePath"))
            .or_else(|| input.get("path"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        edits.push(json!({"file": path, "diff": truncate_str(diff, 1000)}));
    }
    edits
}

/// Text rows to search, oldest first: `(session_id, title, text)`.
///
/// OpenCode 1/Kilo text parts for sessions OpenCode 2 hasn't migrated, then
/// OpenCode 2 user/assistant text. Migration copies v1 history into
/// `session_message` and keeps the v1 tables, so v2 is the complete copy.
fn family_text_rows(conn: &rusqlite::Connection) -> Result<Vec<(String, String, String)>, String> {
    let mut out = Vec::new();
    let has_v2 = table_exists(conn, "session_v2") && table_exists(conn, "session_message");
    if table_exists(conn, "session") && table_exists(conn, "part") {
        let v2_filter = if has_v2 {
            "AND s.id NOT IN (SELECT session_id FROM session_message)"
        } else {
            ""
        };
        let mut stmt = conn
            .prepare(&format!(
                "SELECT s.id, s.title, p.data
                 FROM session s
                 JOIN part p ON p.session_id = s.id
                 WHERE json_extract(p.data, '$.type') = 'text' {v2_filter}
                 ORDER BY p.time_created ASC"
            ))
            .map_err(|e| format!("Query error: {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| format!("Query error: {e}"))?;
        for (session_id, title, data_str) in rows.flatten() {
            let Ok(data) = serde_json::from_str::<Value>(&data_str) else {
                continue;
            };
            if data
                .get("synthetic")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                continue;
            }
            if let Some(text) = data.get("text").and_then(Value::as_str) {
                out.push((session_id, title, text.to_string()));
            }
        }
    }

    if has_v2 {
        let mut stmt = conn
            .prepare(
                "SELECT s.id, s.title, m.type, m.data
                 FROM session_v2 s
                 JOIN session_message m ON m.session_id = s.id
                 WHERE m.type IN ('user', 'assistant')
                 ORDER BY m.time_created ASC, m.seq ASC",
            )
            .map_err(|e| format!("Query error: {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|e| format!("Query error: {e}"))?;
        for (session_id, title, kind, data_str) in rows.flatten() {
            let Ok(data) = serde_json::from_str::<Value>(&data_str) else {
                continue;
            };
            if kind == "user" {
                if let Some(text) = data.get("text").and_then(Value::as_str) {
                    out.push((session_id, title, text.to_string()));
                }
                continue;
            }
            for block in data
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if block.get("type").and_then(Value::as_str) == Some("text")
                    && let Some(text) = block.get("text").and_then(Value::as_str)
                {
                    out.push((session_id.clone(), title.clone(), text.to_string()));
                }
            }
        }
    }
    Ok(out)
}

fn search_family_sessions(
    db_path: &Path,
    pattern: &str,
    limit: usize,
    agent: &str,
) -> Result<Vec<TranscriptSearchMatch>, String> {
    let re = Regex::new(pattern).map_err(|e| format!("Invalid regex: {e}"))?;
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| format!("Cannot open OpenCode DB: {e}"))?;
    Ok(group_matches(
        db_path,
        agent,
        &re,
        family_text_rows(&conn)?,
        limit,
    ))
}

/// One search hit per session (first matching text, match count), in the
/// order sessions first matched. Rows are `(session_id, title, text)`.
pub(crate) fn group_matches(
    db_path: &Path,
    agent: &str,
    re: &Regex,
    rows: impl IntoIterator<Item = (String, String, String)>,
    limit: usize,
) -> Vec<TranscriptSearchMatch> {
    let mut by_session: std::collections::HashMap<String, TranscriptSearchMatch> =
        std::collections::HashMap::new();
    let mut order = Vec::new();
    for (session_id, title, text) in rows {
        if text.is_empty() || !re.is_match(&text) {
            continue;
        }

        let entry = by_session.entry(session_id.clone()).or_insert_with(|| {
            order.push(session_id.clone());
            TranscriptSearchMatch {
                path: db_path.to_string_lossy().to_string(),
                agent: agent.to_string(),
                line: 0,
                text: truncate_str(&text.replace('\n', " "), 100).to_string(),
                matches: 0,
                session_id: Some(session_id.clone()),
                label: Some(title.clone()),
            }
        });
        entry.matches += 1;
    }

    order
        .into_iter()
        .filter_map(|session_id| by_session.remove(&session_id))
        .take(limit)
        .collect()
}

pub(crate) fn search_opencode_sessions(
    db_path: &Path,
    pattern: &str,
    limit: usize,
) -> Result<Vec<TranscriptSearchMatch>, String> {
    search_family_sessions(db_path, pattern, limit, "opencode")
}

pub(crate) fn search_kilo_sessions(
    db_path: &Path,
    pattern: &str,
    limit: usize,
) -> Result<Vec<TranscriptSearchMatch>, String> {
    search_family_sessions(db_path, pattern, limit, "kilo")
}

/// One conversation message, normalized to the OpenCode 1 shape: a role plus
/// v1-style parts (`{"type":"text",...}` / `{"type":"tool","tool":...,"state":...}`).
struct MsgRow {
    role: String,
    time_created: i64,
    parts: Vec<Value>,
    data: Value,
}

fn table_exists(conn: &rusqlite::Connection, table: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
        rusqlite::params![table],
        |_| Ok(()),
    )
    .is_ok()
}

/// OpenCode 1 / Kilo: `message` rows (role in JSON `data`) with `part` rows.
fn load_v1_messages(conn: &rusqlite::Connection, session_id: &str) -> Result<Vec<MsgRow>, String> {
    if !table_exists(conn, "message") {
        return Ok(Vec::new());
    }
    let mut stmt = conn
        .prepare(
            "SELECT id, data, time_created FROM message WHERE session_id = ? ORDER BY time_created ASC",
        )
        .map_err(|e| format!("Query error: {e}"))?;
    let rows: Vec<(String, String, i64)> = stmt
        .query_map(rusqlite::params![session_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2).unwrap_or(0)))
        })
        .map_err(|e| format!("Query error: {e}"))?
        .filter_map(|r| r.ok())
        .collect();

    // Query per message_id to avoid dependency on part.session_id column.
    let mut parts_stmt = if rows.is_empty() {
        None
    } else {
        conn.prepare("SELECT data FROM part WHERE message_id = ? ORDER BY id ASC")
            .ok()
    };
    let mut messages = Vec::with_capacity(rows.len());
    for (id, data_str, time_created) in rows {
        let data = match serde_json::from_str::<Value>(&data_str) {
            Ok(data) => data,
            Err(e) => {
                log_warn(
                    "transcript",
                    "opencode_parse",
                    &format!("skipping message {id}: invalid JSON in data column: {e}"),
                );
                continue;
            }
        };
        let role = data
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let mut parts = Vec::new();
        if let Some(stmt) = parts_stmt.as_mut()
            && let Ok(rows) = stmt.query_map(rusqlite::params![id], |row| row.get::<_, String>(0))
        {
            parts.extend(
                rows.flatten()
                    .filter_map(|data| serde_json::from_str::<Value>(&data).ok()),
            );
        }
        messages.push(MsgRow {
            role,
            time_created,
            parts,
            data,
        });
    }
    Ok(messages)
}

/// Text of an OpenCode 2 tool result: its text content blocks, joined.
fn v2_content_text(content: Option<&Value>) -> Option<String> {
    let text = content?
        .as_array()?
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

/// Convert an OpenCode 2 assistant content block to a v1-style part.
fn v2_block_to_part(block: &Value) -> Option<Value> {
    match block.get("type").and_then(Value::as_str)? {
        "text" => {
            Some(json!({"type": "text", "text": block.get("text").cloned().unwrap_or_default()}))
        }
        "tool" => {
            let state = block.get("state").cloned().unwrap_or_else(|| json!({}));
            let status = state.get("status").and_then(Value::as_str).unwrap_or("");
            let mut v1_state = json!({
                "status": status,
                "input": state.get("input").filter(|v| v.is_object()).cloned().unwrap_or_else(|| json!({})),
            });
            if let Some(metadata) = state.get("metadata") {
                v1_state["metadata"] = metadata.clone();
            }
            if let Some(output) = v2_content_text(state.get("content")) {
                v1_state["output"] = Value::String(output);
            }
            if status == "error" {
                let message = state
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| {
                        v1_state
                            .get("output")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
                    .unwrap_or_default();
                v1_state["error"] = Value::String(message);
            }
            Some(json!({
                "type": "tool",
                "tool": block.get("name").cloned().unwrap_or_default(),
                "state": v1_state,
            }))
        }
        _ => None,
    }
}

/// OpenCode 2: `session_message` rows whose `type` is the role and whose JSON
/// `data` holds the body (`text` for user, `content[]` blocks for assistant).
/// Other row types (system, synthetic, idle, switches) are not conversation.
fn load_v2_messages(conn: &rusqlite::Connection, session_id: &str) -> Result<Vec<MsgRow>, String> {
    // OpenCode 1 DBs can carry a pre-split `session_message`; only the
    // `session_v2` layout is OpenCode 2's canonical history.
    if !(table_exists(conn, "session_v2") && table_exists(conn, "session_message")) {
        return Ok(Vec::new());
    }
    let mut stmt = conn
        .prepare(
            "SELECT id, type, data, time_created FROM session_message
             WHERE session_id = ?1 AND type IN ('user', 'assistant') ORDER BY seq ASC",
        )
        .map_err(|e| format!("Query error: {e}"))?;
    let rows = stmt
        .query_map(rusqlite::params![session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3).unwrap_or(0),
            ))
        })
        .map_err(|e| format!("Query error: {e}"))?;
    let mut messages = Vec::new();
    for (id, role, data_str, time_created) in rows.flatten() {
        let data = match serde_json::from_str::<Value>(&data_str) {
            Ok(data) => data,
            Err(e) => {
                log_warn(
                    "transcript",
                    "opencode_parse",
                    &format!("skipping message {id}: invalid JSON in data column: {e}"),
                );
                continue;
            }
        };
        let parts = if role == "user" {
            vec![json!({"type": "text", "text": data.get("text").cloned().unwrap_or_default()})]
        } else {
            data.get("content")
                .and_then(Value::as_array)
                .map(|blocks| blocks.iter().filter_map(v2_block_to_part).collect())
                .unwrap_or_default()
        };
        messages.push(MsgRow {
            role,
            time_created,
            parts,
            data,
        });
    }
    Ok(messages)
}

/// Parse an OpenCode-family SQLite transcript database.
///
/// OpenCode 1 and Kilo store conversations in `message` + `part` tables;
/// OpenCode 2 stores them in `session_message`. Migration copies v1 history
/// into v2 and keeps the v1 tables, so v2 wins and v1 is the fallback.
#[cfg(test)]
pub(crate) fn parse_opencode_sqlite(
    db_path: &Path,
    session_id: &str,
    last: usize,
) -> Result<Vec<Exchange>, String> {
    let mut transcript = parse_opencode_sqlite_with_usage(db_path, session_id)?;
    transcript.truncate(last);
    Ok(transcript.exchanges)
}

pub(crate) fn parse_opencode_sqlite_with_usage(
    db_path: &Path,
    session_id: &str,
) -> Result<Transcript, String> {
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| format!("Cannot open OpenCode DB: {e}"))?;

    let mut messages = load_v2_messages(&conn, session_id)?;
    if messages.is_empty() {
        messages = load_v1_messages(&conn, session_id)?;
    }
    if messages.is_empty() {
        return Ok(Transcript::default());
    }
    // Either schema may be absent, so a failed lookup just means no directory.
    let cwd: Option<String> = ["session_v2", "session"].iter().find_map(|table| {
        conn.query_row(
            &format!("SELECT directory FROM {table} WHERE id = ?1"),
            [session_id],
            |row| row.get(0),
        )
        .ok()
    });

    // Build exchanges: group by user messages
    let mut usage = UsageCollector::default();
    let mut exchanges = Vec::new();
    let mut position = 0;

    // Find user message indices (with actual text)
    let mut user_indices: Vec<usize> = Vec::new();
    for (i, msg) in messages.iter().enumerate() {
        if msg.role == "assistant"
            && let Some(u) = msg.data.get("tokens").filter(|u| u.is_object())
        {
            let n = |key: &str| u.get(key).and_then(Value::as_u64).unwrap_or(0);
            let cache_read = u
                .pointer("/cache/read")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let cache_write = u
                .pointer("/cache/write")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let tokens = TokenUsage {
                input: n("input"),
                // OpenCode stores visible output separately from reasoning.
                output: n("output").saturating_add(n("reasoning")),
                cache_read,
                cache_write,
                reasoning: u.get("reasoning").and_then(Value::as_u64),
            };
            usage.record(
                tokens,
                Some(
                    tokens
                        .input
                        .saturating_add(cache_read)
                        .saturating_add(cache_write),
                ),
                msg.data.get("cost").and_then(Value::as_f64),
                msg.data
                    .get("modelID")
                    .or_else(|| msg.data.get("model"))
                    .and_then(Value::as_str),
                user_indices.len().checked_sub(1),
            );
        }
        if msg.role != "user" {
            continue;
        }
        let has_text = msg.parts.iter().any(|p| {
            p.get("type").and_then(|v| v.as_str()) == Some("text")
                && !p
                    .get("synthetic")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                && !p
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .is_empty()
        });
        if has_text {
            user_indices.push(i);
        }
    }

    for (ui_pos, &user_idx) in user_indices.iter().enumerate() {
        let next_user_idx = user_indices
            .get(ui_pos + 1)
            .copied()
            .unwrap_or(messages.len());
        let user_msg = &messages[user_idx];
        // Extract user text
        let user_text: String = user_msg
            .parts
            .iter()
            .filter(|p| {
                p.get("type").and_then(|v| v.as_str()) == Some("text")
                    && !p
                        .get("synthetic")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
            })
            .filter_map(|p| p.get("text").and_then(|v| v.as_str()))
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
            .join("\n");

        let timestamp = if user_msg.time_created > 0 {
            let secs = user_msg.time_created / 1000;
            chrono::DateTime::from_timestamp(secs, 0)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .unwrap_or_default()
        } else {
            String::new()
        };

        // Process assistant messages between this user msg and next
        let mut action_parts: Vec<String> = Vec::new();
        let mut files: Vec<String> = Vec::new();
        let mut tools: Vec<ToolUse> = Vec::new();
        let mut edits: Vec<Value> = Vec::new();
        let mut errors: Vec<Value> = Vec::new();
        // First text part after the latest tool call.
        let mut final_part = 0;
        let mut tool_since_text = false;

        for msg in &messages[(user_idx + 1)..next_user_idx] {
            if msg.role != "assistant" {
                continue;
            }
            for p in &msg.parts {
                let ptype = p.get("type").and_then(|v| v.as_str()).unwrap_or("");
                match ptype {
                    "text" => {
                        if p.get("synthetic")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                        {
                            continue;
                        }
                        if let Some(text) = p.get("text").and_then(|v| v.as_str())
                            && !text.is_empty()
                        {
                            if std::mem::take(&mut tool_since_text) {
                                final_part = action_parts.len();
                            }
                            action_parts.push(text.to_string());
                        }
                    }
                    "tool" => {
                        let tool_name = p.get("tool").and_then(|v| v.as_str()).unwrap_or("unknown");
                        let normalized = normalize_tool_name(tool_name);
                        let state = p.get("state").cloned().unwrap_or(json!({}));
                        let input = state.get("input").cloned().unwrap_or(json!({}));
                        let is_err = state.get("status").and_then(|v| v.as_str()) == Some("error");
                        let raw_output = if is_err {
                            state.get("error").and_then(Value::as_str)
                        } else {
                            state.get("output").and_then(Value::as_str)
                        };

                        tool_since_text = true;
                        let file = tool_input_file(tool_name, &input)
                            .map(|f| display_path(f, cwd.as_deref()));
                        if let Some(f) = &file
                            && !files.contains(f)
                        {
                            files.push(f.clone());
                        }
                        let command = tool_call_detail(tool_name, &input);

                        if matches!(normalized, "Edit" | "Write") {
                            edits.extend(extract_opencode_edits(&state, &input));
                        }
                        if is_err {
                            errors.push(json!({
                                "tool": normalized,
                                "content": raw_output.map(|s| truncate_str(s, 300)).unwrap_or(""),
                            }));
                        }

                        tools.push(ToolUse {
                            name: normalized.to_string(),
                            is_error: is_err,
                            file,
                            command,
                            output: raw_output.and_then(capture_tool_output),
                        });
                    }
                    _ => {}
                }
            }
        }

        position += 1;
        files.truncate(5);

        let ended_on_error = tools.last().map(|t| t.is_error).unwrap_or(false);
        let action =
            finalize_action_text(&action_parts.join("\n"), &tools, &errors, ended_on_error);

        let final_text = if final_part == 0 {
            String::new()
        } else {
            action_parts[final_part..].join("\n").trim().to_string()
        };

        exchanges.push(Exchange {
            usage: None,
            position,
            user: user_text,
            action,
            final_text,
            files,
            timestamp,
            tools,
            edits,
            errors,
            ended_on_error,
        });
    }

    Ok(usage.attach(exchanges))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_outputs_errors_and_edit_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("opencode.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE message (id TEXT, session_id TEXT, data TEXT, time_created INTEGER);
             CREATE TABLE part (id TEXT, message_id TEXT, data TEXT);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["u1", "ses_1", json!({"role": "user"}).to_string(), 1_000],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO part VALUES (?1, ?2, ?3)",
            rusqlite::params![
                "p1",
                "u1",
                json!({"type": "text", "text": "edit it"}).to_string()
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "a1",
                "ses_1",
                json!({"role": "assistant"}).to_string(),
                2_000
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO part VALUES (?1, ?2, ?3)",
            rusqlite::params![
                "p2",
                "a1",
                json!({
                    "type": "tool",
                    "tool": "apply_patch",
                    "state": {
                        "status": "completed",
                        // Real apply_patch input carries only patchText (no
                        // path), so the file must be resolved from
                        // metadata.files[].filePath and the diff from `patch`.
                        "input": {"patchText": "*** Update File: /work/a.rs"},
                        "output": "updated",
                        "metadata": {"files": [{
                            "filePath": "/work/a.rs",
                            "patch": "-old\n+new"
                        }]}
                    }
                })
                .to_string()
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO part VALUES (?1, ?2, ?3)",
            rusqlite::params![
                "p3",
                "a1",
                json!({
                    "type": "tool",
                    "tool": "bash",
                    "state": {
                        "status": "error",
                        "input": {"command": "false"},
                        "error": "exit status 1"
                    }
                })
                .to_string()
            ],
        )
        .unwrap();
        drop(conn);

        let exchanges = parse_opencode_sqlite(&db_path, "ses_1", 10).unwrap();
        assert_eq!(exchanges.len(), 1);
        assert_eq!(exchanges[0].tools[0].output.as_deref(), Some("updated"));
        assert_eq!(
            exchanges[0].tools[1].output.as_deref(),
            Some("exit status 1")
        );
        assert_eq!(exchanges[0].errors[0]["content"], "exit status 1");
        assert_eq!(exchanges[0].edits[0]["file"], "/work/a.rs");
        assert_eq!(exchanges[0].edits[0]["diff"], "-old\n+new");
    }

    fn v2_db(dir: &Path) -> std::path::PathBuf {
        let db_path = dir.join("opencode.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE session (id TEXT, title TEXT, directory TEXT);
             CREATE TABLE message (id TEXT, session_id TEXT, data TEXT, time_created INTEGER);
             CREATE TABLE part (id TEXT, message_id TEXT, session_id TEXT, data TEXT, time_created INTEGER);
             CREATE TABLE session_v2 (id TEXT, title TEXT, directory TEXT);
             CREATE TABLE session_message (id TEXT, session_id TEXT, type TEXT, seq INTEGER,
                                           time_created INTEGER, data TEXT);
             INSERT INTO session_v2 VALUES ('ses_2', 'v2 work', '/work');",
        )
        .unwrap();
        let rows = [
            ("system", json!({"text": "tools changed"})),
            ("user", json!({"text": "run it", "files": [], "agents": []})),
            (
                "assistant",
                json!({"content": [
                    {"type": "reasoning", "text": "thinking"},
                    {"type": "text", "text": "running now"},
                    {"type": "tool", "id": "c1", "name": "shell", "state": {
                        "status": "completed",
                        "input": {"command": "echo hi"},
                        "content": [{"type": "text", "text": "hi\n"}],
                        "metadata": {"exit": 0}
                    }},
                    {"type": "tool", "id": "c2", "name": "edit", "state": {
                        "status": "completed",
                        "input": {"path": "/work/a.rs"},
                        "content": [{"type": "text", "text": "Edited"}],
                        "metadata": {"diff": "-a\n+b"}
                    }},
                    {"type": "tool", "id": "c3", "name": "shell", "state": {
                        "status": "error",
                        "input": {"command": "false"},
                        "error": {"type": "tool.execution", "message": "exit status 1"}
                    }}
                ]}),
            ),
            ("synthetic", json!({"text": "internal"})),
            ("user", json!({"text": "second"})),
        ];
        for (seq, (kind, data)) in rows.iter().enumerate() {
            conn.execute(
                "INSERT INTO session_message VALUES (?1, 'ses_2', ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    format!("msg_{seq}"),
                    kind,
                    seq as i64,
                    1_000 + seq as i64,
                    data.to_string()
                ],
            )
            .unwrap();
        }
        db_path
    }

    #[test]
    fn reads_opencode2_session_messages() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = v2_db(dir.path());

        let exchanges = parse_opencode_sqlite(&db_path, "ses_2", 10).unwrap();
        assert_eq!(exchanges.len(), 2);
        let first = &exchanges[0];
        assert_eq!(first.user, "run it");
        assert!(first.action.contains("running now"), "{}", first.action);
        assert!(!first.action.contains("thinking"));
        let names: Vec<_> = first.tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["Bash", "Edit", "Bash"]);
        assert_eq!(first.tools[0].command.as_deref(), Some("echo hi"));
        assert_eq!(first.tools[0].output.as_deref(), Some("hi\n"));
        // Relative to the session directory (/work).
        assert_eq!(first.tools[1].file.as_deref(), Some("a.rs"));
        assert_eq!(first.edits[0]["file"], "/work/a.rs");
        assert_eq!(first.edits[0]["diff"], "-a\n+b");
        assert!(first.tools[2].is_error);
        assert_eq!(first.errors[0]["content"], "exit status 1");
        assert!(first.ended_on_error);
        assert_eq!(exchanges[1].user, "second");

        let hits = search_opencode_sessions(&db_path, "running", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].session_id.as_deref(), Some("ses_2"));
        assert_eq!(hits[0].label.as_deref(), Some("v2 work"));
        assert!(
            search_opencode_sessions(&db_path, "internal", 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn v2_wins_for_migrated_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = v2_db(dir.path());
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        // ses_2 was migrated (v1 rows kept, history continues in v2);
        // ses_old exists only in v1.
        conn.execute_batch(
            r#"INSERT INTO session VALUES ('ses_2', 'v1 copy', '/work');
               INSERT INTO message VALUES ('u1', 'ses_2', '{"role":"user"}', 1);
               INSERT INTO part VALUES ('p1', 'u1', 'ses_2', '{"type":"text","text":"from v1"}', 1);
               INSERT INTO session VALUES ('ses_old', 'old', '/work');
               INSERT INTO message VALUES ('u2', 'ses_old', '{"role":"user"}', 1);
               INSERT INTO part VALUES ('p2', 'u2', 'ses_old', '{"type":"text","text":"legacy only"}', 1);"#,
        )
        .unwrap();
        drop(conn);

        let exchanges = parse_opencode_sqlite(&db_path, "ses_2", 10).unwrap();
        assert_eq!(exchanges.len(), 2);
        assert_eq!(exchanges[1].user, "second");
        let old = parse_opencode_sqlite(&db_path, "ses_old", 10).unwrap();
        assert_eq!(old[0].user, "legacy only");

        let hits = search_opencode_sessions(&db_path, "second", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].label.as_deref(), Some("v2 work"));
        assert!(
            search_opencode_sessions(&db_path, "from v1", 10)
                .unwrap()
                .is_empty()
        );
        let hits = search_opencode_sessions(&db_path, "legacy only", 10).unwrap();
        assert_eq!(hits[0].session_id.as_deref(), Some("ses_old"));
    }

    #[test]
    fn v2_only_db_without_v1_tables() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("opencode.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            r#"CREATE TABLE session_v2 (id TEXT, title TEXT);
               CREATE TABLE session_message (id TEXT, session_id TEXT, type TEXT, seq INTEGER,
                                             time_created INTEGER, data TEXT);
               INSERT INTO session_v2 VALUES ('ses_3', NULL);
               INSERT INTO session_message VALUES ('msg_1', 'ses_3', 'user', 1, 5, '{"text":"hello"}');"#,
        )
        .unwrap();
        drop(conn);
        let exchanges = parse_opencode_sqlite(&db_path, "ses_3", 10).unwrap();
        assert_eq!(exchanges[0].user, "hello");
        assert_eq!(
            search_opencode_sessions(&db_path, "hello", 10)
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn reads_usage_from_both_schemas_without_counting_migrated_rows_twice() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let c = rusqlite::Connection::open(f.path()).unwrap();
        c.execute_batch("CREATE TABLE message (id TEXT, session_id TEXT, data TEXT, time_created INTEGER); CREATE TABLE part (id TEXT, message_id TEXT, data TEXT); CREATE TABLE session_v2 (id TEXT, directory TEXT); CREATE TABLE session_message (id TEXT, session_id TEXT, type TEXT, data TEXT, time_created INTEGER, seq INTEGER);").unwrap();
        let assistant = json!({"role":"assistant","modelID":"oc", "tokens":{"input":10,"output":20,"reasoning":3,"cache":{"read":30,"write":4}},"cost":0.25,"content":[]}).to_string();
        c.execute(
            "INSERT INTO message VALUES ('u','s',?1,1)",
            [json!({"role":"user"}).to_string()],
        )
        .unwrap();
        c.execute(
            "INSERT INTO part VALUES ('p','u',?1)",
            [json!({"type":"text","text":"hello"}).to_string()],
        )
        .unwrap();
        c.execute("INSERT INTO message VALUES ('a','s',?1,2)", [&assistant])
            .unwrap();
        let first = parse_opencode_sqlite_with_usage(f.path(), "s").unwrap();
        assert_eq!(first.exchanges[0].usage.unwrap().input, 10);
        let u = first.usage.unwrap();
        assert_eq!(u.context_tokens, Some(44));
        assert_eq!(u.cost_usd, Some(0.25));
        assert_eq!(u.tokens.reasoning, Some(3));
        assert_eq!(u.tokens.output, 23);
        c.execute(
            "INSERT INTO session_message VALUES ('u','s','user',?1,1,0)",
            [json!({"text":"hello"}).to_string()],
        )
        .unwrap();
        c.execute(
            "INSERT INTO session_message VALUES ('a','s','assistant',?1,2,1)",
            [&assistant],
        )
        .unwrap();
        let migrated = parse_opencode_sqlite_with_usage(f.path(), "s").unwrap();
        assert_eq!(migrated.usage.unwrap(), u);
        assert_eq!(migrated.exchanges[0].usage.unwrap().cache_read, 30);
        c.execute(
            "UPDATE session_message SET data=?1 WHERE type='assistant'",
            [json!({"tokens":{"input":0,"output":0},"content":[]}).to_string()],
        )
        .unwrap();
        assert_eq!(
            parse_opencode_sqlite_with_usage(f.path(), "s")
                .unwrap()
                .usage
                .unwrap()
                .tokens,
            TokenUsage::default()
        );
        assert!(
            parse_opencode_sqlite_with_usage(f.path(), "absent")
                .unwrap()
                .usage
                .is_none()
        );
    }
}
