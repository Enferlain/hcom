//! Claude transcript parser (.jsonl).

use std::path::Path;

use serde_json::{Value, json};

use super::shared::{
    Exchange, TokenUsage, ToolUse, Transcript, UsageCollector, capture_tool_output,
    collapse_codex_duplicate_exchanges, dedup_sorted_capped, display_path, extract_content_text,
    extract_edit_info, extract_text_content, final_segment, finalize_action_text, has_user_text,
    is_error_result, normalize_tool_name, read_file_lossy, token_counter, tool_call_detail,
    tool_input_file, truncate_str,
};

fn extract_antigravity_entry_text(entry: &Value) -> String {
    for key in [
        "content",
        "text",
        "message",
        "response",
        "plannerResponse",
        "userInput",
        "input",
    ] {
        let text = extract_content_text(entry.get(key));
        if !text.trim().is_empty() && !text.trim_start().starts_with('{') {
            return text.trim().to_string();
        }
        if let Some(value) = entry.get(key) {
            let nested = extract_text_content(value);
            if !nested.trim().is_empty() {
                return nested.trim().to_string();
            }
        }
    }
    String::new()
}

/// Parse Claude JSONL transcript.
#[cfg(test)]
pub(crate) fn parse_claude_jsonl(
    path: &Path,
    last: usize,
    detailed: bool,
) -> Result<Vec<Exchange>, String> {
    let mut transcript = parse_claude_jsonl_with_usage(path, detailed)?;
    transcript.truncate(last);
    Ok(transcript.exchanges)
}

pub(crate) fn parse_claude_jsonl_with_usage(
    path: &Path,
    detailed: bool,
) -> Result<Transcript, String> {
    let mut usage = UsageCollector::default();
    let content = read_file_lossy(path)?;

    // First pass: parse all entries and build tool_use index for detailed mode
    struct ParsedEntry {
        entry_type: String,
        usage_only: bool,
        ts: String,
        data: Value,
    }

    let mut entries = Vec::new();
    let mut final_usage = std::collections::HashMap::new();
    // Map (session_id, tool_use_id) -> {name, input} for matching tool_results to tool_uses
    let mut tool_use_index: std::collections::HashMap<(String, String), (String, Value)> =
        std::collections::HashMap::new();

    for line in content.lines() {
        let entry: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // Sidechains have separate session files and remain outside this transcript.
        if entry
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            continue;
        }
        let usage_only = entry
            .get("isMeta")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || entry
                .get("isCompactSummary")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        if usage_only && entry.get("type").and_then(Value::as_str) != Some("assistant") {
            continue;
        }

        let entry_type = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let ts = entry
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Skip system-level entry types
        if matches!(
            entry_type.as_str(),
            "summary"
                | "system"
                | "result"
                | "progress"
                | "file-history-snapshot"
                | "saved_hook_context"
        ) {
            continue;
        }

        if entry_type == "assistant"
            && let Some(message) = entry.get("message")
            && let Some(id) = message.get("id").and_then(Value::as_str)
            && let Some(raw) = message.get("usage").filter(|v| v.is_object())
        {
            final_usage.insert(id.to_owned(), raw.clone());
        }

        // Build tool_use index from assistant entries
        if entry_type == "assistant" {
            let session_id = entry
                .get("sessionId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Some(arr) = entry
                .get("message")
                .and_then(|v| v.get("content"))
                .and_then(|v| v.as_array())
            {
                for block in arr {
                    if block.get("type").and_then(|v| v.as_str()) == Some("tool_use") {
                        let tool_id = block
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let name = block
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let input = block.get("input").cloned().unwrap_or(json!({}));
                        tool_use_index.insert((session_id.clone(), tool_id), (name, input));
                    }
                }
            }
        }

        entries.push(ParsedEntry {
            entry_type,
            usage_only,
            ts,
            data: entry,
        });
    }

    // Second pass: build exchanges
    let mut exchanges = Vec::new();
    let mut current_user = String::new();
    let mut current_action = String::new();
    let mut current_tools: Vec<ToolUse> = Vec::new();
    let mut current_files: Vec<String> = Vec::new();
    let mut current_ts = String::new();
    let mut current_edits: Vec<Value> = Vec::new();
    let mut current_errors: Vec<Value> = Vec::new();
    let mut current_last_was_error = false;
    // Where the text after the latest tool call starts in `current_action`.
    let mut final_start = 0;
    // tool_use id → index in `current_tools` of a call still awaiting its result.
    let mut pending_tools: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut tool_since_text = false;
    let mut cwd: Option<String> = None;
    let mut position = 0;

    let mut seen_usage = std::collections::HashSet::new();
    for pe in &entries {
        if pe.entry_type == "assistant" {
            let message = &pe.data["message"];
            let id = message.get("id").and_then(Value::as_str);
            // Content blocks repeat usage and may update output as the call finishes.
            // Use its final snapshot at the call's first location, keeping context order.
            if let Some(raw) = id
                .and_then(|id| final_usage.get(id))
                .or_else(|| message.get("usage"))
                .filter(|v| v.is_object())
                && id.is_none_or(|id| seen_usage.insert(id.to_owned()))
            {
                let tokens = TokenUsage {
                    input: token_counter(raw, "input_tokens"),
                    output: token_counter(raw, "output_tokens"),
                    cache_read: token_counter(raw, "cache_read_input_tokens"),
                    cache_write: token_counter(raw, "cache_creation_input_tokens"),
                    reasoning: raw
                        .pointer("/output_tokens_details/thinking_tokens")
                        .and_then(Value::as_u64),
                };
                let context = tokens
                    .input
                    .saturating_add(tokens.cache_read)
                    .saturating_add(tokens.cache_write);
                let ratio = raw
                    .get("context_usage_ratio")
                    .and_then(Value::as_f64)
                    .filter(|ratio| ratio.is_finite() && (0.0..=1.0).contains(ratio));
                let session = usage.record(
                    tokens,
                    None,
                    None,
                    message.get("model").and_then(Value::as_str),
                    (!pe.usage_only && !current_user.is_empty()).then_some(exchanges.len()),
                );
                // Each call reports its context as tokens or, with zero counters, a ratio.
                session.context_tokens = if context == 0 && ratio.is_some_and(|ratio| ratio > 0.0) {
                    None
                } else {
                    Some(context)
                };
                session.context_usage_ratio = ratio;
            }
        }
        if pe.usage_only {
            continue;
        }
        if let Some(entry_cwd) = pe.data.get("cwd").and_then(Value::as_str) {
            cwd = Some(entry_cwd.to_string());
        }
        match pe.entry_type.as_str() {
            "USER_INPUT" => {
                let user_text = extract_antigravity_entry_text(&pe.data);
                if user_text.is_empty() {
                    continue;
                }

                if !current_user.is_empty() || !current_action.is_empty() {
                    position += 1;
                    exchanges.push(Exchange {
                        usage: None,
                        position,
                        user: current_user.clone(),
                        action: finalize_action_text(
                            &current_action,
                            &current_tools,
                            &current_errors,
                            current_last_was_error,
                        ),
                        final_text: final_segment(&current_action, final_start),
                        files: dedup_sorted_capped(&current_files, 5),
                        timestamp: current_ts.clone(),
                        tools: std::mem::take(&mut current_tools),
                        edits: std::mem::take(&mut current_edits),
                        errors: std::mem::take(&mut current_errors),
                        ended_on_error: current_last_was_error,
                    });
                }

                current_user = user_text;
                current_action = String::new();
                current_tools = Vec::new();
                current_files = Vec::new();
                current_edits = Vec::new();
                current_errors = Vec::new();
                current_last_was_error = false;
                final_start = 0;
                tool_since_text = false;
                pending_tools.clear();
                current_ts = pe.ts.clone();
            }
            "PLANNER_RESPONSE" => {
                let text = extract_antigravity_entry_text(&pe.data);
                if !text.is_empty() {
                    if !current_action.is_empty() {
                        current_action.push('\n');
                    }
                    if std::mem::take(&mut tool_since_text) {
                        final_start = current_action.len();
                    }
                    current_action.push_str(&text);
                }
            }
            "user" => {
                // Check if this user entry has actual user text (not just tool_result blocks)
                let has_text = has_user_text(&pe.data.get("message").cloned().unwrap_or(json!({})));
                if !has_text {
                    // tool_result-only user entry — process for error detection in detailed mode
                    if detailed {
                        let session_id = pe
                            .data
                            .get("sessionId")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let tool_use_result = pe.data.get("toolUseResult").cloned();
                        if let Some(arr) = pe
                            .data
                            .get("message")
                            .and_then(|v| v.get("content"))
                            .and_then(|v| v.as_array())
                        {
                            for block in arr {
                                if block.get("type").and_then(|v| v.as_str()) != Some("tool_result")
                                {
                                    continue;
                                }
                                let tool_use_id = block
                                    .get("tool_use_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let (tool_name, tool_input) = tool_use_index
                                    .get(&(session_id.clone(), tool_use_id.clone()))
                                    .map(|(n, i)| (n.clone(), i.clone()))
                                    .unwrap_or_else(|| ("unknown".to_string(), json!({})));

                                let is_err = is_error_result(block);
                                let normalized = normalize_tool_name(&tool_name);
                                let output = extract_content_text(block.get("content"));

                                let file = tool_use_result
                                    .as_ref()
                                    .filter(|_| normalized == "Edit")
                                    .and_then(|r| r.get("filePath").and_then(Value::as_str))
                                    .or_else(|| tool_input_file(&tool_name, &tool_input))
                                    .map(|f| display_path(f, cwd.as_deref()));
                                let command = tool_call_detail(&tool_name, &tool_input);

                                let tool = ToolUse {
                                    name: normalized.to_string(),
                                    is_error: is_err,
                                    file,
                                    command,
                                    output: capture_tool_output(&output),
                                };
                                match pending_tools.remove(&tool_use_id) {
                                    Some(i) => current_tools[i] = tool,
                                    None => current_tools.push(tool),
                                }

                                // Extract edit info for Edit tools
                                if normalized == "Edit"
                                    && let Some(edit) =
                                        extract_edit_info(&tool_use_result, &tool_input)
                                {
                                    current_edits.push(edit);
                                }

                                if is_err {
                                    let truncated = truncate_str(&output, 300);
                                    current_errors.push(json!({
                                        "tool": normalized,
                                        "content": truncated,
                                    }));
                                    current_last_was_error = true;
                                } else {
                                    current_last_was_error = false;
                                }
                            }
                        }
                    }
                    continue;
                }

                // Save previous exchange
                if !current_user.is_empty() || !current_action.is_empty() {
                    position += 1;
                    exchanges.push(Exchange {
                        usage: None,
                        position,
                        user: current_user.clone(),
                        action: finalize_action_text(
                            &current_action,
                            &current_tools,
                            &current_errors,
                            current_last_was_error,
                        ),
                        final_text: final_segment(&current_action, final_start),
                        files: dedup_sorted_capped(&current_files, 5),
                        timestamp: current_ts.clone(),
                        tools: std::mem::take(&mut current_tools),
                        edits: std::mem::take(&mut current_edits),
                        errors: std::mem::take(&mut current_errors),
                        ended_on_error: current_last_was_error,
                    });
                }

                // Extract user text
                current_user =
                    extract_text_content(&pe.data.get("message").cloned().unwrap_or(json!({})));
                current_action = String::new();
                current_tools = Vec::new();
                current_files = Vec::new();
                current_edits = Vec::new();
                current_errors = Vec::new();
                current_last_was_error = false;
                final_start = 0;
                tool_since_text = false;
                pending_tools.clear();
                current_ts = pe.ts.clone();
            }
            "assistant" => {
                let msg = pe.data.get("message").cloned().unwrap_or(json!({}));

                // Extract text and tool_use blocks
                if let Some(content) = msg.get("content") {
                    if let Some(arr) = content.as_array() {
                        for block in arr {
                            if block.get("type").and_then(|v| v.as_str()) == Some("text") {
                                if let Some(text) = block.get("text").and_then(|v| v.as_str()) {
                                    if !current_action.is_empty() {
                                        current_action.push('\n');
                                    }
                                    if std::mem::take(&mut tool_since_text) {
                                        final_start = current_action.len();
                                    }
                                    current_action.push_str(text);
                                }
                            } else if block.get("type").and_then(|v| v.as_str()) == Some("tool_use")
                            {
                                let tool_name =
                                    block.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                let input = block.get("input").cloned().unwrap_or(json!({}));

                                tool_since_text = true;
                                let file = tool_input_file(tool_name, &input)
                                    .map(|f| display_path(f, cwd.as_deref()));

                                if let Some(ref f) = file
                                    && !current_files.contains(f)
                                {
                                    current_files.push(f.clone());
                                }

                                // Recorded at call time so an in-flight call is visible;
                                // in detailed mode its tool_result fills in the outcome.
                                if detailed
                                    && let Some(id) = block.get("id").and_then(Value::as_str)
                                {
                                    pending_tools.insert(id.to_string(), current_tools.len());
                                }
                                current_tools.push(ToolUse {
                                    name: normalize_tool_name(tool_name).to_string(),
                                    is_error: false,
                                    file,
                                    command: tool_call_detail(tool_name, &input),
                                    output: None,
                                });
                            }
                        }
                    } else if let Some(text) = content.as_str() {
                        current_action = text.to_string();
                        final_start = 0;
                    }
                }
            }
            _ => {}
        }
    }

    // Save last exchange
    if !current_user.is_empty() || !current_action.is_empty() {
        position += 1;
        exchanges.push(Exchange {
            usage: None,
            position,
            user: current_user,
            action: finalize_action_text(
                &current_action,
                &current_tools,
                &current_errors,
                current_last_was_error,
            ),
            final_text: final_segment(&current_action, final_start),
            files: dedup_sorted_capped(&current_files, 5),
            timestamp: current_ts,
            tools: current_tools,
            edits: current_edits,
            errors: current_errors,
            ended_on_error: current_last_was_error,
        });
    }

    // Attach call usage before collapsing duplicate exchanges.
    let mut transcript = usage.attach(exchanges);
    transcript.exchanges = collapse_codex_duplicate_exchanges(transcript.exchanges);
    Ok(transcript)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detailed_captures_tool_result_output() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let lines = [
            json!({
                "type": "user",
                "sessionId": "s1",
                "message": {"role": "user", "content": "run pwd"}
            }),
            json!({
                "type": "assistant",
                "sessionId": "s1",
                "message": {"role": "assistant", "content": [{
                    "type": "tool_use", "id": "t1", "name": "Bash",
                    "input": {"command": "pwd"}
                }]}
            }),
            json!({
                "type": "user",
                "sessionId": "s1",
                "message": {"role": "user", "content": [{
                    "type": "tool_result", "tool_use_id": "t1", "content": "/work"
                }]}
            }),
        ];
        std::fs::write(
            file.path(),
            lines
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let exchanges = parse_claude_jsonl(file.path(), 10, true).unwrap();
        assert_eq!(exchanges[0].tools[0].output.as_deref(), Some("/work"));
    }

    #[test]
    fn final_text_files_and_details_follow_tool_calls() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let lines = [
            json!({"type": "user", "sessionId": "s1", "cwd": "/repo",
                   "message": {"role": "user", "content": "check the parser"}}),
            json!({"type": "assistant", "sessionId": "s1", "message": {"role": "assistant", "content": [
                {"type": "text", "text": "I'll look at the parser first."},
                {"type": "tool_use", "id": "t1", "name": "Grep",
                 "input": {"pattern": "fn parse", "path": "/repo/src"}},
                {"type": "tool_use", "id": "t2", "name": "Read",
                 "input": {"file_path": "/repo/src/parser.rs"}}
            ]}}),
            json!({"type": "user", "sessionId": "s1", "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": "src/parser.rs:1"},
                {"type": "tool_result", "tool_use_id": "t2", "content": "fn parse() {}"}
            ]}}),
            json!({"type": "assistant", "sessionId": "s1", "message": {"role": "assistant", "content": [
                {"type": "text", "text": "The parser is fine."}
            ]}}),
        ];
        std::fs::write(
            file.path(),
            lines
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        for detailed in [false, true] {
            let ex = &parse_claude_jsonl(file.path(), 10, detailed).unwrap()[0];
            assert_eq!(ex.final_text, "The parser is fine.");
            // Grep's directory is not a file; paths are relative to the session cwd.
            assert_eq!(ex.files, ["src/parser.rs"]);
            let details: Vec<_> = ex
                .tools
                .iter()
                .map(|t| (t.name.as_str(), t.file.as_deref(), t.command.as_deref()))
                .collect();
            assert_eq!(
                details,
                [
                    ("Grep", None, Some("fn parse")),
                    ("Read", Some("src/parser.rs"), None)
                ],
                "detailed={detailed}"
            );
        }
    }

    #[test]
    fn detailed_keeps_tool_calls_still_awaiting_results() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let lines = [
            json!({"type": "user", "sessionId": "s1", "message": {"role": "user", "content": "run it"}}),
            json!({"type": "assistant", "sessionId": "s1", "message": {"role": "assistant", "content": [
                {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "echo done"}},
                {"type": "tool_use", "id": "t2", "name": "Bash", "input": {"command": "sleep 100"}}
            ]}}),
            json!({"type": "user", "sessionId": "s1", "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": "done"}
            ]}}),
        ];
        std::fs::write(
            file.path(),
            lines
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let ex = &parse_claude_jsonl(file.path(), 10, true).unwrap()[0];
        let tools: Vec<_> = ex
            .tools
            .iter()
            .map(|t| (t.command.as_deref(), t.output.as_deref()))
            .collect();
        assert_eq!(
            tools,
            [(Some("echo done"), Some("done")), (Some("sleep 100"), None)]
        );
    }
}

#[cfg(test)]
mod usage_snapshot_tests {
    use super::*;
    #[test]
    fn compaction_usage_is_session_only_and_sidechains_are_excluded() {
        let entries = [
            json!({"type":"user","message":{"content":"hello"}}),
            json!({"type":"assistant","isMeta":true,"message":{"id":"compact","content":"hidden","usage":{"input_tokens":10,"output_tokens":2}}}),
            json!({"type":"assistant","isSidechain":true,"message":{"id":"side","usage":{"input_tokens":100,"output_tokens":20}}}),
            json!({"type":"assistant","message":{"id":"answer","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":3,"output_tokens":1}}}),
        ];
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            entries
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let transcript = parse_claude_jsonl_with_usage(file.path(), false).unwrap();
        assert_eq!(transcript.usage.unwrap().tokens.input, 13);
        assert_eq!(transcript.exchanges[0].usage.unwrap().input, 3);
        assert_eq!(transcript.exchanges[0].action, "done");
    }
    #[test]
    fn final_snapshot_counts_once_without_reordering_context() {
        let call = |id, input, output| json!({"type":"assistant","message":{"id":id,"content":[{"type":"text","text":"done"}],"usage":{"input_tokens":input,"output_tokens":output}}});
        let entries = [
            json!({"type":"user","message":{"content":"hello"}}),
            call("first", 10, 1),
            call("second", 20, 2),
            call("first", 10, 5),
            call("first", 10, 5),
        ];
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            entries
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let transcript = parse_claude_jsonl_with_usage(file.path(), false).unwrap();
        let usage = transcript.usage.unwrap();
        assert_eq!(usage.tokens.input, 30);
        assert_eq!(usage.tokens.output, 7);
        assert_eq!(usage.context_tokens, Some(20));
        assert_eq!(transcript.exchanges[0].usage, Some(usage.tokens));
    }
}

#[cfg(test)]
mod context_ratio_tests {
    use super::*;
    #[test]
    fn ratio_only_context_keeps_recorded_zero_tokens() {
        for (ratio, expected_context, expected_label) in [
            (Some(0.33375), None, "33%"),
            (None, Some(0), "0"),
            (Some(0.0), Some(0), "0"),
            (Some(1.1), Some(0), "0"),
        ] {
            let file = tempfile::NamedTempFile::new().unwrap();
            let entries = [
                json!({"type":"user","message":{"content":"hello"}}),
                json!({"type":"assistant","message":{"id":"a","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0,"context_usage_ratio":ratio,"credits":12}}}),
            ];
            std::fs::write(
                file.path(),
                entries
                    .iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
            .unwrap();
            let transcript = parse_claude_jsonl_with_usage(file.path(), false).unwrap();
            let usage = transcript.usage.unwrap();
            assert_eq!(usage.tokens, TokenUsage::default());
            assert_eq!(usage.context_tokens, expected_context);
            assert_eq!(usage.context_window, None);
            assert_eq!(usage.cost_usd, None);
            assert_eq!(
                super::super::shared::format_context(&usage).as_deref(),
                Some(expected_label)
            );
            assert_eq!(
                usage.context_usage_ratio,
                ratio.filter(|ratio| (0.0..=1.0).contains(ratio))
            );
        }
    }
}
