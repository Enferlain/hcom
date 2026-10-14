//! Gemini transcript parser (.json).

use std::path::Path;

use serde_json::{Value, json};

use super::shared::{
    Exchange, ToolUse, capture_tool_output, display_path, extract_gemini_user_text,
    extract_text_content, finalize_action_text, normalize_tool_name, read_file_lossy,
    tool_call_detail, tool_input_file, truncate_str,
};

fn tool_output(tool_call: &Value) -> Option<String> {
    let mut outputs = Vec::new();
    if let Some(results) = tool_call.get("result").and_then(Value::as_array) {
        for result in results {
            let Some(response) = result
                .get("functionResponse")
                .and_then(|v| v.get("response"))
            else {
                continue;
            };
            if let Some(output) = response.get("output").and_then(Value::as_str) {
                outputs.push(output.to_string());
            } else if !response.is_null() {
                outputs.push(response.to_string());
            }
        }
    }
    if outputs.is_empty() {
        tool_call
            .get("resultDisplay")
            .and_then(Value::as_str)
            .map(ToString::to_string)
    } else {
        Some(outputs.join("\n"))
    }
}

/// Parse Gemini JSON transcript.
pub(crate) fn parse_gemini_json(path: &Path, last: usize) -> Result<Vec<Exchange>, String> {
    let content = read_file_lossy(path)?;

    let mut messages: Vec<Value> = Vec::new();

    // Try parsing as a single JSON object first (old format)
    if let Ok(data) = serde_json::from_str::<Value>(&content)
        && let Some(arr) = data.get("messages").and_then(|v| v.as_array())
    {
        messages = arr.clone();
    }

    // Fallback to JSONL format
    if messages.is_empty() {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(val) = serde_json::from_str::<Value>(line)
                && val.get("type").is_some()
            {
                messages.push(val);
            }
        }
    }

    if messages.is_empty() {
        return Err("No messages found or invalid JSON".to_string());
    }

    let mut exchanges = Vec::new();
    let mut current_user = String::new();
    let mut current_action = String::new();
    let mut current_ts = String::new();
    let mut position = 0;

    let mut current_tools: Vec<ToolUse> = Vec::new();
    let mut current_files: Vec<String> = Vec::new();
    let mut current_errors: Vec<Value> = Vec::new();
    let mut current_last_was_error = false;

    for msg in &messages {
        let msg_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let ts = msg
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if msg_type == "user" {
            if !current_user.is_empty() || !current_action.is_empty() {
                position += 1;
                exchanges.push(Exchange {
                    position,
                    user: current_user.clone(),
                    action: finalize_action_text(
                        &current_action,
                        &current_tools,
                        &current_errors,
                        current_last_was_error,
                    ),
                    final_text: String::new(),
                    files: std::mem::take(&mut current_files),
                    timestamp: current_ts.clone(),
                    tools: std::mem::take(&mut current_tools),
                    edits: Vec::new(),
                    errors: std::mem::take(&mut current_errors),
                    ended_on_error: current_last_was_error,
                });
            }
            // Gemini user content can be a string or array of {text: ...} blocks.
            // Use displayContent if available (user-visible text without hook context).
            current_user = extract_gemini_user_text(msg);
            current_action = String::new();
            current_tools = Vec::new();
            current_files = Vec::new();
            current_errors = Vec::new();
            current_last_was_error = false;
            current_ts = ts;
        } else if msg_type == "gemini" || msg_type == "model" {
            let text = extract_text_content(msg);
            if !text.is_empty() {
                if !current_action.is_empty() {
                    current_action.push('\n');
                }
                current_action.push_str(&text);
            }

            // Extract tool calls
            if let Some(tool_calls) = msg.get("toolCalls").and_then(|v| v.as_array()) {
                for tc in tool_calls {
                    let raw_name = tc.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let tool_name = normalize_tool_name(raw_name);
                    let args = tc.get("args").cloned().unwrap_or(json!({}));
                    let is_err = tc
                        .get("status")
                        .and_then(|v| v.as_str())
                        .is_some_and(|status| {
                            !matches!(
                                status.to_ascii_lowercase().as_str(),
                                "ok" | "success" | "completed"
                            )
                        });
                    let output = tool_output(tc);

                    let file = tool_input_file(tool_name, &args).map(|f| display_path(f, None));
                    if let Some(f) = &file
                        && !current_files.contains(f)
                    {
                        current_files.push(f.clone());
                    }
                    let command = tool_call_detail(tool_name, &args);

                    current_tools.push(ToolUse {
                        name: tool_name.to_string(),
                        is_error: is_err,
                        file,
                        command,
                        output: output.as_deref().and_then(capture_tool_output),
                    });
                    if is_err {
                        let error_output = output.unwrap_or_default();
                        current_errors.push(json!({
                            "tool": tool_name,
                            "content": truncate_str(&error_output, 300),
                        }));
                    }
                    current_last_was_error = is_err;
                }
            }
        }
    }

    // Last exchange
    if !current_user.is_empty() || !current_action.is_empty() {
        position += 1;
        exchanges.push(Exchange {
            position,
            user: current_user,
            action: finalize_action_text(
                &current_action,
                &current_tools,
                &current_errors,
                current_last_was_error,
            ),
            final_text: String::new(),
            files: current_files,
            timestamp: current_ts,
            tools: current_tools,
            edits: Vec::new(),
            errors: current_errors,
            ended_on_error: current_last_was_error,
        });
    }

    if exchanges.len() > last {
        let start = exchanges.len() - last;
        exchanges = exchanges[start..].to_vec();
    }

    Ok(exchanges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_nested_function_response_output() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            json!({"messages": [
                {"type": "user", "content": "run pwd"},
                {"type": "gemini", "content": "done", "toolCalls": [{
                    "id": "call-1",
                    "name": "run_shell_command",
                    "args": {"command": "pwd"},
                    "status": "success",
                    "result": [{"functionResponse": {
                        "id": "call-1",
                        "name": "run_shell_command",
                        "response": {"output": "/work"}
                    }}]
                }]}
            ]})
            .to_string(),
        )
        .unwrap();

        let exchanges = parse_gemini_json(file.path(), 10).unwrap();
        assert_eq!(exchanges[0].tools[0].output.as_deref(), Some("/work"));
    }
}
