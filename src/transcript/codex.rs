//! Codex transcript parser (.jsonl).

use std::path::Path;

use serde_json::{Value, json};

use super::shared::{
    Exchange, TokenUsage, ToolUse, Transcript, UsageCollector, capture_tool_output, codex_is_error,
    collapse_codex_duplicate_exchanges, dedup_sorted_capped, display_path,
    extract_codex_event_message_text, extract_text_content, final_segment, finalize_action_text,
    is_codex_system_injected_user_text, is_no_response_action, normalize_tool_name,
    read_file_lossy, same_trimmed_text, token_counter, tool_call_detail, tool_input_file,
    truncate_str,
};

/// Extract plain text from a `function_call_output.output` value.
///
/// Codex encodes this field as either a plain string (older format) or an
/// array of content items `[{"type":"input_text","text":...}, ...]` (current
/// format, codex-cli 0.14x). This mirrors codex's own lossy
/// `FunctionCallOutputContentItem` → text conversion: keep `input_text` text,
/// drop image/audio/encrypted items, join with newlines.
fn codex_output_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.get("text").and_then(Value::as_str))
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Forks of forks are followed this deep; a cycle or a runaway chain stops here.
const MAX_FORK_DEPTH: usize = 16;

/// A rollout's entries, preceded by whatever it inherited as a fork.
///
/// Since Codex 0.152 a fork's rollout no longer copies the parent's history: its
/// `session_meta` names the parent (`forked_from_id`) and the inherited prefix
/// (every parent entry with `ordinal < forked_from_ordinal_exclusive`). Without
/// resolving that, a fork's transcript starts at its first own turn.
fn read_codex_rollout_entries(path: &Path) -> Result<Vec<Value>, String> {
    let own = read_rollout_lines(path)?;
    let mut entries = inherited_entries(path, &own, 0);
    entries.extend(own);
    Ok(entries)
}

fn read_rollout_lines(path: &Path) -> Result<Vec<Value>, String> {
    Ok(read_file_lossy(path)?
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect())
}

/// What the rollout at `path` (whose parsed lines are `own`) inherited from its
/// fork parent, recursively. Empty when it is not a fork, or when the parent
/// rollout is gone — the fork's own turns still render.
fn inherited_entries(path: &Path, own: &[Value], depth: usize) -> Vec<Value> {
    let Some((parent_id, bound)) = own.first().and_then(fork_reference) else {
        return Vec::new();
    };
    if depth >= MAX_FORK_DEPTH {
        return Vec::new();
    }
    let Some(parent_path) = find_sibling_rollout(path, &parent_id) else {
        return Vec::new();
    };
    let Ok(parent_own) = read_rollout_lines(&parent_path) else {
        return Vec::new();
    };
    let mut entries = inherited_entries(&parent_path, &parent_own, depth + 1);
    entries.extend(
        parent_own
            .into_iter()
            .enumerate()
            .filter(|(index, entry)| {
                let ordinal = entry.get("ordinal").and_then(Value::as_u64);
                ordinal.unwrap_or(*index as u64) < bound
            })
            .map(|(_, entry)| entry),
    );
    entries
}

/// `(parent id, inherited ordinal bound)` when `entry` is a forked rollout's
/// `session_meta` that references its parent rather than copying it.
fn fork_reference(entry: &Value) -> Option<(String, u64)> {
    if entry.get("type").and_then(Value::as_str) != Some("session_meta") {
        return None;
    }
    let meta = entry.get("payload")?;
    let parent_id = meta.get("forked_from_id")?.as_str()?.to_string();
    let inherited = meta.get("forked_from_ordinal_exclusive")?.as_u64()?;
    Some((parent_id, inherited))
}

/// The rollout for session `id` under the same `sessions` root as `path`
/// (rollouts live in dated subdirectories, so the parent may be days away).
fn find_sibling_rollout(path: &Path, id: &str) -> Option<std::path::PathBuf> {
    let sessions = path
        .ancestors()
        .find(|dir| dir.file_name().is_some_and(|name| name == "sessions"))?;
    let pattern = format!("{}/**/rollout-*-{id}.jsonl", sessions.display());
    glob::glob(&pattern).ok()?.filter_map(Result::ok).next()
}

/// Codex input counts include cached and cache-write tokens; make them disjoint.
fn codex_tokens(raw: &Value) -> TokenUsage {
    let read = token_counter(raw, "cached_input_tokens");
    let write = token_counter(raw, "cache_write_input_tokens");
    TokenUsage {
        input: token_counter(raw, "input_tokens")
            .saturating_sub(read)
            .saturating_sub(write),
        output: token_counter(raw, "output_tokens"),
        cache_read: read,
        cache_write: write,
        reasoning: raw.get("reasoning_output_tokens").and_then(Value::as_u64),
    }
}

/// Parse Codex JSONL transcript.
/// Handles both response_item (older) and event_msg (newer) formats.
#[cfg(test)]
pub(crate) fn parse_codex_jsonl(
    path: &Path,
    last: usize,
    detailed: bool,
) -> Result<Vec<Exchange>, String> {
    let mut transcript = parse_codex_jsonl_with_usage(path, detailed)?;
    transcript.truncate(last);
    Ok(transcript.exchanges)
}

pub(crate) fn parse_codex_jsonl_with_usage(
    path: &Path,
    detailed: bool,
) -> Result<Transcript, String> {
    let mut usage = UsageCollector::default();
    let entries = read_codex_rollout_entries(path)?;

    // First pass: build call_id → output map for error detection
    let mut call_outputs: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut parsed_lines: Vec<Value> = Vec::new();

    for entry in entries {
        let payload = entry.get("payload").cloned().unwrap_or(entry.clone());
        let payload_type = payload.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if payload_type == "function_call_output" {
            let call_id = payload
                .get("call_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let output = codex_output_text(payload.get("output"));
            call_outputs.insert(call_id, output);
        }
        parsed_lines.push(entry);
    }

    // Second pass: build exchanges
    let mut exchanges = Vec::new();
    let mut current_user = String::new();
    let mut current_action = String::new();
    let mut current_tools: Vec<ToolUse> = Vec::new();
    let mut current_files: Vec<String> = Vec::new();
    let mut current_ts = String::new();
    let mut current_errors: Vec<Value> = Vec::new();
    let mut current_last_was_error = false;
    let mut current_assistant_chunks: Vec<String> = Vec::new();
    // Where the text after the latest tool call starts in `current_action`.
    let mut final_start = 0;
    let mut tool_since_text = false;
    let mut cwd: Option<String> = None;
    let mut position = 0;
    let mut in_exchange = false; // track whether we have a real user entry

    let save_exchange = |exchanges: &mut Vec<Exchange>,
                         position: &mut usize,
                         in_exchange: &mut bool,
                         user: &mut String,
                         action: &mut String,
                         files: &mut Vec<String>,
                         ts: &mut String,
                         tools: &mut Vec<ToolUse>,
                         errors: &mut Vec<Value>,
                         assistant_chunks: &mut Vec<String>,
                         last_was_error: &mut bool,
                         final_start: &mut usize,
                         tool_since_text: &mut bool| {
        if !user.is_empty() || !action.is_empty() {
            *position += 1;
            let final_action = finalize_action_text(action, tools, errors, *last_was_error);
            exchanges.push(Exchange {
                usage: None,
                position: *position,
                user: std::mem::take(user),
                action: final_action,
                final_text: final_segment(action, *final_start),
                files: dedup_sorted_capped(files, 5),
                timestamp: std::mem::take(ts),
                tools: std::mem::take(tools),
                edits: Vec::new(),
                errors: std::mem::take(errors),
                ended_on_error: *last_was_error,
            });
            let _ = std::mem::take(action);
            files.clear();
            assistant_chunks.clear();
            *last_was_error = false;
            *final_start = 0;
            *tool_since_text = false;
        }
        *in_exchange = false;
    };

    // Per-response records cover every call, including compaction, which the
    // cumulative `token_count` totals leave out; their `thread_token_usage` also
    // carries earlier rollout segments. Older rollouts only have `token_count`.
    let has_records = parsed_lines
        .iter()
        .any(|entry| entry.get("type").and_then(Value::as_str) == Some("token_usage_record"));
    let mut seen_responses = std::collections::HashSet::new();
    let mut thread_total = None;
    let mut previous_usage = TokenUsage::default();
    let mut latest_model = None;
    for entry in &parsed_lines {
        let entry_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let ts = entry
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let payload = entry.get("payload").cloned().unwrap_or(entry.clone());
        let payload_type = payload.get("type").and_then(|v| v.as_str()).unwrap_or("");

        if matches!(entry_type, "session_meta" | "turn_context")
            && let Some(entry_cwd) = payload.get("cwd").and_then(Value::as_str)
        {
            cwd = Some(entry_cwd.to_string());
        }

        if let Some(model) = payload.get("model").and_then(Value::as_str) {
            latest_model = Some(model.to_owned());
        }
        if entry_type == "token_usage_record"
            && let Some(thread) = payload.get("thread_token_usage").filter(|v| v.is_object())
        {
            thread_total = Some(codex_tokens(thread));
        }
        if entry_type == "token_usage_record"
            && let Some(raw) = payload.get("usage").filter(|v| v.is_object())
            && payload
                .get("response_id")
                .and_then(Value::as_str)
                .is_none_or(|id| seen_responses.insert(id.to_owned()))
        {
            usage.record(
                codex_tokens(raw),
                None,
                None,
                latest_model.as_deref(),
                in_exchange.then_some(exchanges.len()),
            );
        }
        if entry_type == "event_msg" && payload_type == "token_count" {
            let info = &payload["info"];
            if let Some(raw) = info.get("total_token_usage").filter(|v| v.is_object()) {
                let total = codex_tokens(raw);
                let delta = total.saturating_sub(previous_usage);
                previous_usage = total;
                let session = usage.record(
                    if has_records {
                        TokenUsage::default()
                    } else {
                        delta
                    },
                    info.pointer("/last_token_usage/total_tokens")
                        .and_then(Value::as_u64),
                    None,
                    latest_model.as_deref(),
                    in_exchange.then_some(exchanges.len()),
                );
                if let Some(window) = info.get("model_context_window").and_then(Value::as_u64) {
                    session.context_window = Some(window);
                }
            }
        }

        // Skip system entry types
        if matches!(
            entry_type,
            "session_meta" | "turn_context" | "session_start" | "session_end"
        ) {
            continue;
        }

        // Handle response_item format (older Codex)
        if entry_type == "response_item" || (entry_type.is_empty() && payload_type == "message") {
            match payload_type {
                "message" => {
                    let role = payload.get("role").and_then(|v| v.as_str()).unwrap_or("");
                    if role == "user" {
                        let text = extract_text_content(&payload);
                        // Only start exchange if user has actual text
                        if text.is_empty() || is_codex_system_injected_user_text(&text) {
                            continue;
                        }
                        if in_exchange
                            && current_action.is_empty()
                            && same_trimmed_text(&current_user, &text)
                        {
                            current_ts = ts.clone();
                            continue;
                        }
                        save_exchange(
                            &mut exchanges,
                            &mut position,
                            &mut in_exchange,
                            &mut current_user,
                            &mut current_action,
                            &mut current_files,
                            &mut current_ts,
                            &mut current_tools,
                            &mut current_errors,
                            &mut current_assistant_chunks,
                            &mut current_last_was_error,
                            &mut final_start,
                            &mut tool_since_text,
                        );
                        current_user = text;
                        current_ts = ts.clone();
                        in_exchange = true;
                    } else if role == "assistant" {
                        let text = extract_text_content(&payload);
                        if !text.is_empty() {
                            if current_assistant_chunks
                                .iter()
                                .any(|chunk| same_trimmed_text(chunk, &text))
                            {
                                continue;
                            }
                            if !current_action.is_empty() {
                                current_action.push('\n');
                            }
                            if std::mem::take(&mut tool_since_text) {
                                final_start = current_action.len();
                            }
                            current_action.push_str(&text);
                            current_assistant_chunks.push(text);
                        }
                    }
                }
                "function_call" => {
                    let raw_name = payload
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    let tool_name = normalize_tool_name(raw_name);
                    let args_str = payload
                        .get("arguments")
                        .and_then(|v| v.as_str())
                        .unwrap_or("{}");
                    let args: Value = serde_json::from_str(args_str).unwrap_or(json!({}));
                    let call_id = payload
                        .get("call_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    tool_since_text = true;
                    let file =
                        tool_input_file(raw_name, &args).map(|f| display_path(f, cwd.as_deref()));
                    if let Some(f) = &file
                        && !current_files.contains(f)
                    {
                        current_files.push(f.clone());
                    }

                    // Determine is_error from call output
                    let output = call_outputs.get(&call_id).map(|s| s.as_str()).unwrap_or("");
                    let is_err = if detailed {
                        codex_is_error(output)
                    } else {
                        false
                    };

                    let command = tool_call_detail(raw_name, &args);

                    current_tools.push(ToolUse {
                        name: tool_name.to_string(),
                        is_error: is_err,
                        file,
                        command,
                        output: capture_tool_output(output),
                    });

                    if is_err {
                        let truncated = truncate_str(output, 300);
                        current_errors.push(json!({
                            "tool": tool_name,
                            "content": truncated,
                        }));
                        current_last_was_error = true;
                    } else {
                        current_last_was_error = false;
                    }
                }
                _ => {}
            }
        }
        // Handle event_msg format (newer Codex)
        else if entry_type == "event_msg" {
            match payload_type {
                "user_message" => {
                    let text = extract_codex_event_message_text(&payload);
                    if text.is_empty() {
                        continue;
                    }
                    if in_exchange
                        && current_action.is_empty()
                        && same_trimmed_text(&current_user, &text)
                    {
                        continue;
                    }
                    save_exchange(
                        &mut exchanges,
                        &mut position,
                        &mut in_exchange,
                        &mut current_user,
                        &mut current_action,
                        &mut current_files,
                        &mut current_ts,
                        &mut current_tools,
                        &mut current_errors,
                        &mut current_assistant_chunks,
                        &mut current_last_was_error,
                        &mut final_start,
                        &mut tool_since_text,
                    );
                    current_user = text;
                    current_ts = ts.clone();
                    in_exchange = true;
                }
                "agent_message" => {
                    let text = extract_codex_event_message_text(&payload);
                    if !text.is_empty() {
                        if current_assistant_chunks
                            .iter()
                            .any(|chunk| same_trimmed_text(chunk, &text))
                        {
                            continue;
                        }
                        if !current_action.is_empty() {
                            current_action.push('\n');
                        }
                        if std::mem::take(&mut tool_since_text) {
                            final_start = current_action.len();
                        }
                        current_action.push_str(&text);
                        current_assistant_chunks.push(text);
                    }
                }
                _ => {} // token_count, agent_reasoning, etc — skip
            }
        }
    }

    // Last exchange
    save_exchange(
        &mut exchanges,
        &mut position,
        &mut in_exchange,
        &mut current_user,
        &mut current_action,
        &mut current_files,
        &mut current_ts,
        &mut current_tools,
        &mut current_errors,
        &mut current_assistant_chunks,
        &mut current_last_was_error,
        &mut final_start,
        &mut tool_since_text,
    );

    if let Some(total) = thread_total
        && let Some(session) = usage.session.as_mut()
    {
        session.tokens = total;
    }
    let mut transcript = usage.attach(exchanges);
    transcript.exchanges = collapse_codex_duplicate_exchanges(transcript.exchanges);
    Ok(transcript)
}

pub(crate) fn should_retry_codex_transcript(exchanges: &[Exchange]) -> bool {
    exchanges
        .last()
        .map(|ex| is_no_response_action(&ex.action))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transcript::{ReadOptions, TranscriptBackend, read};
    use std::fs;

    /// One rollout under `sessions/<day>/`, as Codex >= 0.152 writes it: every
    /// line carries an `ordinal`, and a fork's `session_meta` references its
    /// parent instead of copying the parent's history.
    fn write_rollout(
        sessions: &Path,
        day: &str,
        id: &str,
        fork_of: Option<(&str, u64)>,
        turns: &[(&str, &str)],
    ) -> std::path::PathBuf {
        let mut meta = json!({"session_id": id, "id": id});
        if let Some((parent, bound)) = fork_of {
            meta["forked_from_id"] = json!(parent);
            meta["forked_from_ordinal_exclusive"] = json!(bound);
        }
        let mut lines = vec![json!({"type": "session_meta", "payload": meta})];
        for (user, assistant) in turns {
            for (role, kind, text) in [
                ("user", "input_text", user),
                ("assistant", "output_text", assistant),
            ] {
                lines.push(json!({
                    "type": "response_item",
                    "payload": {"type": "message", "role": role, "content": [{"type": kind, "text": text}]}
                }));
            }
        }
        let body: String = lines
            .into_iter()
            .enumerate()
            .map(|(ordinal, mut line)| {
                line["ordinal"] = json!(ordinal);
                format!("{line}\n")
            })
            .collect();
        let dir = sessions.join(day);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("rollout-{day}-{id}.jsonl"));
        fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn test_parse_codex_fork_includes_inherited_history_through_nested_forks() {
        let dir = tempfile::tempdir().unwrap();
        let sessions = dir.path().join("sessions");
        // Ordinals: 0 session_meta, then 2 per turn. The child forks after the
        // root's first turn (bound 3), so the root's later turn must not leak in.
        write_rollout(&sessions, "d1", "root", None, &[("r1", "R1"), ("r2", "R2")]);
        write_rollout(&sessions, "d2", "child", Some(("root", 3)), &[("c1", "C1")]);
        let grandchild = write_rollout(
            &sessions,
            "d3",
            "grandchild",
            Some(("child", 3)),
            &[("g1", "G1")],
        );

        let exchanges = parse_codex_jsonl(&grandchild, 10, false).unwrap();
        let users: Vec<&str> = exchanges.iter().map(|e| e.user.as_str()).collect();
        assert_eq!(users, ["r1", "c1", "g1"]);
    }

    #[test]
    fn test_parse_codex_fork_with_missing_parent_keeps_own_turns() {
        let dir = tempfile::tempdir().unwrap();
        let sessions = dir.path().join("sessions");
        let orphan = write_rollout(
            &sessions,
            "d1",
            "orphan",
            Some(("gone", 5)),
            &[("o1", "O1")],
        );

        let exchanges = parse_codex_jsonl(&orphan, 10, false).unwrap();
        let users: Vec<&str> = exchanges.iter().map(|e| e.user.as_str()).collect();
        assert_eq!(users, ["o1"]);
    }

    #[test]
    fn test_parse_codex_prefers_response_items_over_event_msgs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout.jsonl");
        let lines = [
            json!({
                "type": "event_msg",
                "timestamp": "2026-03-27T10:00:00.100Z",
                "payload": {"type": "user_message", "message": "response user"}
            }),
            json!({
                "type": "event_msg",
                "timestamp": "2026-03-27T10:00:01.100Z",
                "payload": {"type": "agent_message", "message": "response assistant"}
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:00Z",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "response user"}]
                }
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:01Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "response assistant"}]
                }
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:02Z",
                "payload": {
                    "type": "function_call",
                    "name": "shell",
                    "call_id": "call_1",
                    "arguments": "{\"command\":\"pwd\"}"
                }
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:03Z",
                "payload": {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    // Current codex encodes output as an array of content items,
                    // not a plain string. The parser must join input_text items.
                    "output": [
                        {"type": "input_text", "text": "Chunk ID: abc"},
                        {"type": "input_image", "image_url": "data:image/png;base64,xxx"},
                        {"type": "input_text", "text": "Process exited with code 0\nFinal output:\n/work"}
                    ]
                }
            }),
            json!({
                "type": "event_msg",
                "timestamp": "2026-03-27T10:01:00Z",
                "payload": {"type": "user_message", "message": "event only user"}
            }),
            json!({
                "type": "event_msg",
                "timestamp": "2026-03-27T10:01:01Z",
                "payload": {"type": "agent_message", "message": "event only assistant"}
            }),
        ];
        fs::write(
            &path,
            lines
                .iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let exchanges = parse_codex_jsonl(&path, 10, false).unwrap();
        assert_eq!(exchanges.len(), 2);
        assert_eq!(exchanges[0].user, "response user");
        assert_eq!(exchanges[0].action, "response assistant");
        assert_eq!(exchanges[0].tools.len(), 1);
        assert_eq!(exchanges[0].tools[0].name, "Bash");
        // Array output: both input_text segments joined, image item dropped.
        assert_eq!(
            exchanges[0].tools[0].output.as_deref(),
            Some("Chunk ID: abc\nProcess exited with code 0\nFinal output:\n/work")
        );
        assert_eq!(exchanges[1].user, "event only user");
        assert_eq!(exchanges[1].action, "event only assistant");
    }

    #[test]
    fn codex_output_text_handles_string_and_array_bodies() {
        assert_eq!(
            codex_output_text(Some(&json!("plain string"))),
            "plain string"
        );
        assert_eq!(
            codex_output_text(Some(&json!([
                {"type": "input_text", "text": "a"},
                {"type": "input_text", "text": "  "},
                {"type": "input_image", "image_url": "x"},
                {"type": "input_text", "text": "b"}
            ]))),
            "a\nb"
        );
        assert_eq!(codex_output_text(None), "");
    }

    #[test]
    fn test_parse_codex_dedupes_repeated_assistant_chunks_within_exchange() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout.jsonl");
        let lines = [
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:00Z",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "analyze more code and write implementation plan"}]
                }
            }),
            json!({
                "type": "event_msg",
                "timestamp": "2026-03-27T10:00:01Z",
                "payload": {"type": "agent_message", "message": "first commentary"}
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:01Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "first commentary"}]
                }
            }),
            json!({
                "type": "event_msg",
                "timestamp": "2026-03-27T10:00:02Z",
                "payload": {"type": "agent_message", "message": "second answer"}
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:02Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "second answer"}]
                }
            }),
        ];
        fs::write(
            &path,
            lines
                .iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let exchanges = parse_codex_jsonl(&path, 10, false).unwrap();
        assert_eq!(exchanges.len(), 1);
        assert_eq!(exchanges[0].action, "first commentary\nsecond answer");
    }

    #[test]
    fn test_get_exchanges_retries_transient_codex_no_response_tail() {
        let dir = tempfile::tempdir().unwrap();
        let transcript_path = dir.path().join("rollout.jsonl");
        fs::write(
            &transcript_path,
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:00Z",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "user prompt"}]
                }
            })
            .to_string(),
        )
        .unwrap();

        let path_for_thread = transcript_path.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            fs::write(
                &path_for_thread,
                [
                    json!({
                        "type": "response_item",
                        "timestamp": "2026-03-27T10:00:00Z",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "user prompt"}]
                        }
                    }),
                    json!({
                        "type": "response_item",
                        "timestamp": "2026-03-27T10:00:01Z",
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "assistant answer"}]
                        }
                    }),
                ]
                .iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
            )
            .unwrap();
        });

        let opts = ReadOptions {
            last: 10,
            detailed: false,
            session_id: None,
            allow_codex_retry: true,
        };
        let exchanges = read(&transcript_path, TranscriptBackend::CodexJsonl, &opts)
            .unwrap()
            .exchanges;
        assert_eq!(exchanges.len(), 1);
        assert_eq!(exchanges[0].action, "assistant answer");
    }
}
