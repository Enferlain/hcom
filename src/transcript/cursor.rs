//! Cursor-agent transcript parser (.jsonl).
//!
//! cursor-agent writes one JSON object per line at
//! `~/.cursor/projects/<slug>/agent-transcripts/<uuid>/<uuid>.jsonl` (the path
//! the hook hands hcom verbatim as `transcript_path`). Each line is
//! Anthropic-shaped:
//!
//! ```jsonc
//! {"role": "user",      "message": {"content": [{"type": "text", "text": "…"}]}}
//! {"role": "assistant", "message": {"content": [
//!     {"type": "text", "text": "…"},
//!     {"type": "tool_use", "name": "Shell", "input": {"command": "…"}}
//! ]}}
//! ```
//!
//! Notable differences from the Claude/Codex transcripts:
//! - **No timestamps, no `sessionId`/`cwd`, no `id` on tool_use blocks.**
//! - **Tool outputs are not recorded** (no `tool_result` blocks), so error
//!   detection has nothing to read — every tool use is reported non-error.
//! - User prompts are wrapped in `<user_query>…</user_query>`; we unwrap it.

use std::path::Path;

use serde_json::Value;

use super::shared::{
    Exchange, ToolUse, dedup_sorted_capped, display_path, final_segment, finalize_action_text,
    normalize_tool_name, read_file_lossy, same_trimmed_text, tool_input_file, truncate_str,
};

/// Map cursor's tool names onto hcom's canonical (Claude) names. cursor uses
/// Claude-style CamelCase for most tools (`Read`, `Write`, `Grep`, `Glob`,
/// `WebFetch`, `TodoWrite`, `Shell`, `Task`), so only its divergent names need
/// remapping before falling through to the shared normalizer:
/// `StrReplace`→`Edit`, `ReadFile`→`Read`, `run_terminal_cmd`→`Shell`,
/// `Subagent`→`Task`.
fn normalize_cursor_tool(name: &str) -> &str {
    match name {
        "StrReplace" => "Edit",
        "ReadFile" => "Read",
        "run_terminal_cmd" => "Shell",
        "Subagent" => "Task",
        other => normalize_tool_name(other),
    }
}

/// Strip cursor's `<user_query>…</user_query>` wrapper from a user prompt,
/// returning the inner text trimmed. Leaves unwrapped text untouched.
fn unwrap_user_query(text: &str) -> String {
    let trimmed = text.trim();
    if let Some(inner) = trimmed
        .strip_prefix("<user_query>")
        .and_then(|s| s.strip_suffix("</user_query>"))
    {
        return inner.trim().to_string();
    }
    trimmed.to_string()
}

/// Join the text blocks of a `message.content` array, skipping non-text blocks
/// (tool_use / any future block types). Tolerates a bare-string `content`.
fn content_text(message: &Value) -> String {
    match message.get("content") {
        Some(Value::String(s)) => s.trim().to_string(),
        Some(Value::Array(blocks)) => {
            let mut parts = Vec::new();
            for block in blocks {
                if block.get("type").and_then(Value::as_str) == Some("text")
                    && let Some(text) = block.get("text").and_then(Value::as_str)
                {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        parts.push(trimmed.to_string());
                    }
                }
            }
            parts.join("\n")
        }
        _ => String::new(),
    }
}

/// Basename of a tool-input path field, if any of `path`/`file_path`/`file`.
/// Parse a cursor-agent JSONL transcript into the shared `Exchange` model.
#[cfg(test)]
pub(crate) fn parse_cursor_jsonl(
    path: &Path,
    last: usize,
    detailed: bool,
) -> Result<Vec<Exchange>, String> {
    let mut transcript = parse_cursor_jsonl_with_usage(path, detailed)?;
    transcript.truncate(last);
    Ok(transcript.exchanges)
}

pub(crate) fn parse_cursor_jsonl_with_usage(
    path: &Path,
    _detailed: bool,
) -> Result<super::Transcript, String> {
    let content = read_file_lossy(path)?;

    let mut exchanges: Vec<Exchange> = Vec::new();
    let mut current_user = String::new();
    let mut current_action = String::new();
    let mut current_tools: Vec<ToolUse> = Vec::new();
    let mut current_files: Vec<String> = Vec::new();
    let mut assistant_chunks: Vec<String> = Vec::new();
    let mut position = 0usize;
    let mut in_exchange = false;
    // Where the text after the latest tool call starts in `current_action`.
    let mut final_start = 0usize;
    let mut tool_since_text = false;

    // Flush the in-progress exchange (if any user/assistant content accrued).
    let flush = |exchanges: &mut Vec<Exchange>,
                 position: &mut usize,
                 in_exchange: &mut bool,
                 user: &mut String,
                 action: &mut String,
                 tools: &mut Vec<ToolUse>,
                 files: &mut Vec<String>,
                 chunks: &mut Vec<String>,
                 final_start: &mut usize,
                 tool_since_text: &mut bool| {
        if !user.is_empty() || !action.is_empty() {
            *position += 1;
            let final_action = finalize_action_text(action, tools, &[], false);
            exchanges.push(Exchange {
                usage: None,
                position: *position,
                user: std::mem::take(user),
                action: final_action,
                final_text: final_segment(action, *final_start),
                files: dedup_sorted_capped(files, 5),
                timestamp: String::new(),
                tools: std::mem::take(tools),
                edits: Vec::new(),
                errors: Vec::new(),
                ended_on_error: false,
            });
            action.clear();
            files.clear();
            chunks.clear();
            *final_start = 0;
            *tool_since_text = false;
        }
        *in_exchange = false;
    };

    for line in content.lines() {
        let entry: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let role = entry.get("role").and_then(Value::as_str).unwrap_or("");
        let Some(message) = entry.get("message") else {
            continue;
        };

        match role {
            "user" => {
                let text = unwrap_user_query(&content_text(message));
                // A user message with no text (e.g. a tool_result-only turn)
                // must not start a new exchange.
                if text.is_empty() {
                    continue;
                }
                // Repeated identical user line before any assistant reply (e.g.
                // a re-submitted prompt): keep the open exchange, don't split.
                if in_exchange
                    && current_action.is_empty()
                    && same_trimmed_text(&current_user, &text)
                {
                    continue;
                }
                flush(
                    &mut exchanges,
                    &mut position,
                    &mut in_exchange,
                    &mut current_user,
                    &mut current_action,
                    &mut current_tools,
                    &mut current_files,
                    &mut assistant_chunks,
                    &mut final_start,
                    &mut tool_since_text,
                );
                current_user = text;
                in_exchange = true;
            }
            "assistant" => {
                let text = content_text(message);
                if !text.is_empty()
                    && !assistant_chunks
                        .iter()
                        .any(|chunk| same_trimmed_text(chunk, &text))
                {
                    if !current_action.is_empty() {
                        current_action.push('\n');
                    }
                    if std::mem::take(&mut tool_since_text) {
                        final_start = current_action.len();
                    }
                    current_action.push_str(&text);
                    assistant_chunks.push(text);
                }
                if let Some(blocks) = message.get("content").and_then(Value::as_array) {
                    for block in blocks {
                        if block.get("type").and_then(Value::as_str) != Some("tool_use") {
                            continue;
                        }
                        let raw_name = block
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown");
                        let name = normalize_cursor_tool(raw_name).to_string();
                        let input = block.get("input").cloned().unwrap_or(Value::Null);
                        tool_since_text = true;
                        let file = tool_input_file(raw_name, &input).map(|f| display_path(f, None));
                        if let Some(ref f) = file
                            && !current_files.contains(f)
                        {
                            current_files.push(f.clone());
                        }
                        let command = input.get("command").and_then(Value::as_str).map(|s| {
                            if s.len() > 80 {
                                format!("{}...", truncate_str(s, 77))
                            } else {
                                s.to_string()
                            }
                        });
                        current_tools.push(ToolUse {
                            name,
                            is_error: false,
                            file,
                            command,
                            output: Some("(output not recorded by cursor)".to_string()),
                        });
                    }
                }
            }
            _ => {}
        }
    }

    flush(
        &mut exchanges,
        &mut position,
        &mut in_exchange,
        &mut current_user,
        &mut current_action,
        &mut current_tools,
        &mut current_files,
        &mut assistant_chunks,
        &mut final_start,
        &mut tool_since_text,
    );

    Ok(super::Transcript {
        exchanges,
        usage: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;

    fn write_jsonl(lines: &[Value]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chat.jsonl");
        fs::write(
            &path,
            lines
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        (dir, path)
    }

    #[test]
    fn parses_user_query_unwrap_and_tool_use() {
        let (_d, path) = write_jsonl(&[
            json!({"role": "user", "message": {"content": [
                {"type": "text", "text": "<user_query>\nfix the parser\n</user_query>"}
            ]}}),
            json!({"role": "assistant", "message": {"content": [
                {"type": "text", "text": "On it."},
                {"type": "tool_use", "name": "Shell", "input": {"command": "cargo build", "description": "build"}}
            ]}}),
            json!({"role": "assistant", "message": {"content": [
                {"type": "tool_use", "name": "StrReplace", "input": {"path": "/repo/src/main.rs", "old_string": "a", "new_string": "b"}}
            ]}}),
        ]);
        let ex = parse_cursor_jsonl(&path, 10, true).unwrap();
        assert_eq!(ex.len(), 1);
        assert_eq!(ex[0].user, "fix the parser");
        assert_eq!(ex[0].action, "On it.");
        // Two tool_use blocks across two assistant lines, one exchange.
        assert_eq!(ex[0].tools.len(), 2);
        assert_eq!(ex[0].tools[0].name, "Shell");
        assert_eq!(ex[0].tools[0].command.as_deref(), Some("cargo build"));
        // StrReplace → canonical Edit; cursor records no cwd, so the path stays whole.
        assert_eq!(ex[0].tools[1].name, "Edit");
        assert_eq!(ex[0].files, vec!["/repo/src/main.rs".to_string()]);
    }

    #[test]
    fn normalizes_cursor_divergent_tool_names() {
        assert_eq!(normalize_cursor_tool("StrReplace"), "Edit");
        assert_eq!(normalize_cursor_tool("ReadFile"), "Read");
        assert_eq!(normalize_cursor_tool("run_terminal_cmd"), "Shell");
        assert_eq!(normalize_cursor_tool("Subagent"), "Task");
        // Already-canonical cursor names pass through unchanged.
        assert_eq!(normalize_cursor_tool("Shell"), "Shell");
        assert_eq!(normalize_cursor_tool("Task"), "Task");
        assert_eq!(normalize_cursor_tool("Read"), "Read");
    }

    #[test]
    fn tool_only_turn_and_empty_user_turn_handled() {
        let (_d, path) = write_jsonl(&[
            // user turn with no text (tool_result-only shape) must not split.
            json!({"role": "user", "message": {"content": [
                {"type": "text", "text": "first"}
            ]}}),
            json!({"role": "assistant", "message": {"content": [
                {"type": "tool_use", "name": "Read", "input": {"path": "a.txt"}}
            ]}}),
            json!({"role": "user", "message": {"content": []}}),
            json!({"role": "assistant", "message": {"content": [
                {"type": "text", "text": "done"}
            ]}}),
        ]);
        let ex = parse_cursor_jsonl(&path, 10, false).unwrap();
        // The empty user turn did not open a new exchange; assistant text
        // accrued onto the first (tool-only) exchange.
        assert_eq!(ex.len(), 1);
        assert_eq!(ex[0].user, "first");
        assert_eq!(ex[0].action, "done");
        assert_eq!(ex[0].tools.len(), 1);
        assert_eq!(ex[0].tools[0].name, "Read");
    }

    #[test]
    fn last_truncates_to_most_recent() {
        let mut lines = Vec::new();
        for i in 0..5 {
            lines.push(json!({"role": "user", "message": {"content": [
                {"type": "text", "text": format!("q{i}")}
            ]}}));
            lines.push(json!({"role": "assistant", "message": {"content": [
                {"type": "text", "text": format!("a{i}")}
            ]}}));
        }
        let (_d, path) = write_jsonl(&lines);
        let ex = parse_cursor_jsonl(&path, 2, false).unwrap();
        assert_eq!(ex.len(), 2);
        assert_eq!(ex[0].user, "q3");
        assert_eq!(ex[1].user, "q4");
        // Positions are 1-based and contiguous after truncation.
        assert_eq!(ex[1].position, 5);
    }
}
