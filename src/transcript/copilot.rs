//! GitHub Copilot CLI transcript parser (.jsonl).
//!
//! Copilot stores session events under `session-state/<id>/events.jsonl` with
//! entries shaped like `{type,data,id,timestamp,parentId}`. This parser keeps
//! to the stable Phase-0 fields: user text, assistant text, and tool starts.

use std::collections::HashMap;
use std::path::Path;

use serde_json::{Value, json};

use super::shared::{
    Exchange, TokenUsage, ToolUse, Transcript, UsageCollector, capture_tool_output,
    dedup_sorted_capped, display_path, final_segment, finalize_action_text, normalize_tool_name,
    read_file_lossy, same_trimmed_text, tool_input_file, truncate_str,
};

fn data_text(data: &Value) -> String {
    data.get("text")
        .or_else(|| data.get("content"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string()
}

fn tool_name(data: &Value) -> String {
    data.get("tool_name")
        .or_else(|| data.get("toolName"))
        .or_else(|| data.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn tool_input(data: &Value) -> Value {
    data.get("tool_input")
        .or_else(|| data.get("toolInput"))
        .or_else(|| data.get("input"))
        .or_else(|| data.get("arguments"))
        .cloned()
        .unwrap_or_default()
}

fn tool_command(input: &Value) -> Option<String> {
    input
        .get("command")
        .or_else(|| input.get("cmd"))
        .or_else(|| input.get("script"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

#[cfg(test)]
pub(crate) fn parse_copilot_jsonl(
    path: &Path,
    last: usize,
    _detailed: bool,
) -> Result<Vec<Exchange>, String> {
    let mut transcript = parse_copilot_jsonl_with_usage(path, _detailed)?;
    transcript.truncate(last);
    Ok(transcript.exchanges)
}

pub(crate) fn parse_copilot_jsonl_with_usage(
    path: &Path,
    _detailed: bool,
) -> Result<Transcript, String> {
    let content = read_file_lossy(path)?;

    let mut usage = UsageCollector::default();
    let mut exchanges: Vec<Exchange> = Vec::new();
    let mut current_user = String::new();
    let mut current_action = String::new();
    let mut current_tools: Vec<ToolUse> = Vec::new();
    let mut current_files: Vec<String> = Vec::new();
    let mut current_errors: Vec<Value> = Vec::new();
    let mut current_last_was_error = false;
    let mut call_index: HashMap<String, usize> = HashMap::new();
    let mut timestamp = String::new();
    let mut position = 0usize;
    let mut in_exchange = false;
    // Where the text after the latest tool call starts in `current_action`.
    let mut final_start = 0usize;
    let mut tool_since_text = false;

    let flush = |exchanges: &mut Vec<Exchange>,
                 position: &mut usize,
                 in_exchange: &mut bool,
                 user: &mut String,
                 action: &mut String,
                 tools: &mut Vec<ToolUse>,
                 files: &mut Vec<String>,
                 errors: &mut Vec<Value>,
                 last_was_error: &mut bool,
                 call_index: &mut HashMap<String, usize>,
                 ts: &mut String,
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
            action.clear();
            files.clear();
            call_index.clear();
            *last_was_error = false;
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
        let event_type = entry.get("type").and_then(Value::as_str).unwrap_or("");
        let data = entry.get("data").unwrap_or(&entry);
        match event_type {
            "session.usage_record" => {
                if let Some(u) = data.get("usage").filter(|u| u.is_object()) {
                    let n = |key: &str| u.get(key).and_then(Value::as_u64).unwrap_or(0);
                    let tokens = TokenUsage {
                        input: n("inputTokens")
                            .saturating_sub(n("cacheReadTokens"))
                            .saturating_sub(n("cacheWriteTokens")),
                        output: n("outputTokens"),
                        cache_read: n("cacheReadTokens"),
                        cache_write: n("cacheWriteTokens"),
                        reasoning: u.get("reasoningTokens").and_then(Value::as_u64),
                    };
                    let session = usage.record(
                        tokens,
                        u.get("inputTokens").and_then(Value::as_u64),
                        None,
                        data.get("model")
                            .or_else(|| u.get("model"))
                            .and_then(Value::as_str),
                        in_exchange.then_some(exchanges.len()),
                    );
                    // Copilot persists the prompt capacity alongside each call.
                    if let Some(window) = u
                        .get("maxPromptTokens")
                        .and_then(Value::as_u64)
                        .filter(|window| *window > 0)
                    {
                        session.context_window = Some(window);
                    }
                }
            }

            "user.message" => {
                let text = data_text(data);
                if text.is_empty() {
                    continue;
                }
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
                    &mut current_errors,
                    &mut current_last_was_error,
                    &mut call_index,
                    &mut timestamp,
                    &mut final_start,
                    &mut tool_since_text,
                );
                current_user = text;
                timestamp = entry
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                in_exchange = true;
            }
            "assistant.message" => {
                let text = data_text(data);
                if !text.is_empty() {
                    if !current_action.is_empty() {
                        current_action.push_str("\n\n");
                    }
                    if std::mem::take(&mut tool_since_text) {
                        final_start = current_action.len();
                    }
                    current_action.push_str(&text);
                    in_exchange = true;
                }
            }
            "tool.execution_start" => {
                let name = tool_name(data);
                if name.is_empty() {
                    continue;
                }
                let input = tool_input(data);
                let normalized = normalize_tool_name(&name).to_string();
                tool_since_text = true;
                let file = tool_input_file(&name, &input).map(|f| display_path(f, None));
                if let Some(file) = &file {
                    current_files.push(file.clone());
                }
                if let Some(id) = data.get("toolCallId").and_then(Value::as_str) {
                    call_index.insert(id.to_string(), current_tools.len());
                }
                current_tools.push(ToolUse {
                    name: normalized,
                    is_error: false,
                    file,
                    command: tool_command(&input),
                    output: None,
                });
                in_exchange = true;
            }
            "tool.execution_complete" => {
                let id = data.get("toolCallId").and_then(Value::as_str).unwrap_or("");
                let success = data.get("success").and_then(Value::as_bool).unwrap_or(true);
                let output = data
                    .get("result")
                    .and_then(|result| {
                        result
                            .get("detailedContent")
                            .or_else(|| result.get("content"))
                    })
                    .and_then(Value::as_str)
                    .or_else(|| {
                        data.get("error")
                            .and_then(|error| error.get("message"))
                            .and_then(Value::as_str)
                    })
                    .unwrap_or("")
                    .to_string();
                if let Some(&idx) = call_index.get(id) {
                    current_tools[idx].is_error = !success;
                    current_tools[idx].output = capture_tool_output(&output);
                    if !success {
                        let tool = current_tools[idx].name.clone();
                        current_errors.push(json!({
                            "tool": tool,
                            "content": truncate_str(&output, 300),
                        }));
                    }
                    current_last_was_error = !success;
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
        &mut current_errors,
        &mut current_last_was_error,
        &mut call_index,
        &mut timestamp,
        &mut final_start,
        &mut tool_since_text,
    );

    Ok(usage.attach(exchanges))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_copilot_events_jsonl() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            concat!(
                r#"{"type":"user.message","timestamp":"t1","data":{"text":"hello"}}"#,
                "\n",
                r#"{"type":"tool.execution_start","data":{"toolCallId":"call-1","toolName":"bash","arguments":{"command":"pwd"}}}"#,
                "\n",
                r#"{"type":"tool.execution_complete","data":{"toolCallId":"call-1","success":true,"result":{"content":"/work","detailedContent":"/work\n"}}}"#,
                "\n",
                r#"{"type":"assistant.message","data":{"text":"done"}}"#,
                "\n"
            ),
        )
        .unwrap();

        let exchanges = parse_copilot_jsonl(file.path(), 10, false).unwrap();
        assert_eq!(exchanges.len(), 1);
        assert_eq!(exchanges[0].user, "hello");
        assert!(exchanges[0].action.contains("done"));
        assert_eq!(exchanges[0].tools[0].name, "Bash");
        assert_eq!(exchanges[0].tools[0].command.as_deref(), Some("pwd"));
        assert_eq!(exchanges[0].tools[0].output.as_deref(), Some("/work\n"));
    }
    #[test]
    fn normalizes_usage_without_treating_premium_units_as_dollars() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let usage = json!({"type":"session.usage_record","data":{"model":"gpt", "usage":{"inputTokens":100,"outputTokens":12,"cacheReadTokens":70,"cacheWriteTokens":5,"reasoningTokens":4,"maxPromptTokens":200,"cost":1}}});
        std::fs::write(
            f.path(),
            [
                json!({"type":"user.message","data":{"text":"first"}}),
                usage.clone(),
                usage.clone(),
                json!({"type":"user.message","data":{"text":"second"}}),
                usage,
            ]
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
        )
        .unwrap();
        let t = parse_copilot_jsonl_with_usage(f.path(), false).unwrap();
        let u = t.usage.unwrap();
        assert_eq!(u.tokens.input, 75);
        assert_eq!(u.tokens.cache_read, 210);
        assert_eq!(u.tokens.output, 36);
        assert_eq!(u.tokens.reasoning, Some(12));
        assert_eq!(u.context_tokens, Some(100));
        assert_eq!(u.context_window, Some(200));
        assert_eq!(u.cost_usd, None);
        assert_eq!(t.exchanges[0].usage.unwrap().input, 50);
        assert_eq!(
            parse_copilot_jsonl(f.path(), 1, false).unwrap()[0]
                .usage
                .unwrap()
                .input,
            25
        );
        std::fs::write(f.path(), json!({"type":"session.usage_record","data":{"usage":{"inputTokens":0,"outputTokens":0}}}).to_string()).unwrap();
        assert_eq!(
            parse_copilot_jsonl_with_usage(f.path(), false)
                .unwrap()
                .usage
                .unwrap()
                .tokens,
            TokenUsage::default()
        );
        std::fs::write(f.path(), "{}").unwrap();
        assert!(
            parse_copilot_jsonl_with_usage(f.path(), false)
                .unwrap()
                .usage
                .is_none()
        );
    }
}
