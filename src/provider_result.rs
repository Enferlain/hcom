//! Provider-specific recovery of a completed worker result from its transcript.
//!
//! The normal workflow result is an hcom `message` event. This module is the
//! fail-closed fallback used only after the exact worker generation has stopped
//! without such an event. Recovery remains bound to the workflow's unique
//! thread marker so a resumed session cannot replay an older turn.

use std::collections::HashSet;
use std::path::Path;

use serde_json::Value;

use crate::tool::Tool;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveredProviderResult {
    pub text: String,
    pub provider: String,
    pub evidence: &'static str,
}

pub(crate) fn recover_provider_result(
    tool: &str,
    transcript_path: &str,
    session_id: Option<&str>,
    thread: &str,
) -> Result<RecoveredProviderResult, String> {
    let tool = tool
        .parse::<Tool>()
        .map_err(|_| format!("provider '{tool}' does not support result recovery"))?;
    let entries = read_jsonl(Path::new(transcript_path))?;

    match tool {
        Tool::Antigravity => recover_antigravity(&entries, thread),
        Tool::Claude => recover_claude(
            &entries,
            Some(session_id.ok_or_else(|| {
                "Claude result recovery requires exact session metadata".to_string()
            })?),
            thread,
        ),
        _ => Err(format!(
            "provider '{}' does not support result recovery",
            tool.as_str()
        )),
    }
}

fn read_jsonl(path: &Path) -> Result<Vec<Value>, String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("could not read transcript {}: {error}", path.display()))?;
    let content = String::from_utf8_lossy(&bytes);
    let mut entries = Vec::new();
    for (line_no, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let entry: Value = serde_json::from_str(trimmed).map_err(|error| {
            format!(
                "transcript {} line {} is corrupt: {error}",
                path.display(),
                line_no + 1
            )
        })?;
        entries.push(entry);
    }
    if entries.is_empty() {
        return Err(format!(
            "transcript {} contains no readable records",
            path.display()
        ));
    }
    Ok(entries)
}

fn content_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.trim().to_string(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| {
                if item.get("type").and_then(Value::as_str) == Some("text") {
                    item.get("text").and_then(Value::as_str)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string(),
        Some(Value::Object(object)) => object
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string(),
        _ => String::new(),
    }
}

fn antigravity_text(entry: &Value) -> String {
    for key in [
        "content",
        "text",
        "message",
        "response",
        "plannerResponse",
        "userInput",
        "input",
    ] {
        let text = content_text(entry.get(key));
        if !text.is_empty() {
            return text;
        }
    }
    String::new()
}

fn is_envelope_thread_match(text: &str, start: usize, end: usize) -> bool {
    if !text[..start].ends_with(':') {
        return false;
    }
    let before_colon = &text[..start - 1];
    let Some(bracket_pos) = before_colon.rfind('[') else {
        return false;
    };
    let tag = &before_colon[bracket_pos + 1..];
    if !matches!(tag, "request" | "inform" | "ack" | "thread") {
        return false;
    }
    let before_bracket = &before_colon[..bracket_pos];
    let before_bracket_ok = bracket_pos == 0
        || before_bracket.ends_with(|c: char| c.is_whitespace())
        || before_bracket.ends_with("<hcom>");
    if !before_bracket_ok {
        return false;
    }

    let rest = &text[end..];
    if let Some(after_bracket) = rest.strip_prefix(']') {
        return after_bracket.is_empty()
            || after_bracket.starts_with(|c: char| c.is_whitespace())
            || after_bracket.starts_with("</hcom>");
    }

    if let Some(id_part) = rest.strip_prefix(" #")
        && let Some(bracket_end) = id_part.find(']')
    {
        let id = &id_part[..bracket_end];
        if !id.is_empty() && !id.contains(char::is_whitespace) {
            let after_bracket = &id_part[bracket_end + 1..];
            return after_bracket.is_empty()
                || after_bracket.starts_with(|c: char| c.is_whitespace())
                || after_bracket.starts_with("</hcom>");
        }
    }

    false
}

fn text_contains_thread(text: &str, thread: &str) -> bool {
    if thread.is_empty() {
        return false;
    }
    let mut offset = 0;
    while let Some(pos) = text[offset..].find(thread) {
        let start = offset + pos;
        let end = start + thread.len();

        let standalone_ok = {
            let prev_ok = start == 0
                || text[..start]
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_whitespace());
            let next_ok = end == text.len()
                || text[end..]
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_whitespace());
            prev_ok && next_ok
        };

        if standalone_ok || is_envelope_thread_match(text, start, end) {
            return true;
        }
        offset = start + thread.len().max(1);
    }
    false
}

fn command_matches_thread(tokens: &[String], thread: &str) -> bool {
    tokens
        .windows(2)
        .any(|pair| pair[0] == "--thread" && pair.get(1).is_some_and(|value| value == thread))
        || tokens
            .iter()
            .any(|token| token.strip_prefix("--thread=") == Some(thread))
}

fn command_has_inform_intent(tokens: &[String]) -> bool {
    tokens
        .windows(2)
        .any(|pair| pair[0] == "--intent" && pair.get(1).is_some_and(|value| value == "inform"))
        || tokens
            .iter()
            .any(|token| token.strip_prefix("--intent=") == Some("inform"))
}

fn extract_send_payload(command: &str, thread: &str) -> Option<String> {
    let start = command.rfind("hcom send")?;
    let tokens = shell_words::split(&command[start..]).ok()?;
    if tokens.get(1).map(String::as_str) != Some("send") {
        return None;
    }

    let separator = tokens.iter().position(|token| token == "--")?;
    let flags = &tokens[..separator];
    if !command_matches_thread(flags, thread) || !command_has_inform_intent(flags) {
        return None;
    }

    let payload = tokens[separator + 1..]
        .iter()
        .take_while(|token| !matches!(token.as_str(), "&&" | ";" | "||"))
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    (!payload.trim().is_empty()).then(|| payload.trim().to_string())
}

fn recover_antigravity(entries: &[Value], thread: &str) -> Result<RecoveredProviderResult, String> {
    let marker = entries
        .iter()
        .rposition(|entry| {
            entry.get("type").and_then(Value::as_str) == Some("USER_INPUT")
                && text_contains_thread(&antigravity_text(entry), thread)
        })
        .ok_or_else(|| "Antigravity transcript has no matching workflow turn".to_string())?;

    let has_later_user_input = entries[marker + 1..]
        .iter()
        .any(|entry| entry.get("type").and_then(Value::as_str) == Some("USER_INPUT"));
    if has_later_user_input {
        return Err(
            "Antigravity transcript contains later user turns after matching workflow turn"
                .to_string(),
        );
    }
    let attempt = &entries[marker + 1..];
    for (index, entry) in attempt.iter().enumerate().rev() {
        if entry.get("type").and_then(Value::as_str) != Some("PLANNER_RESPONSE")
            || entry.get("status").and_then(Value::as_str) != Some("DONE")
        {
            continue;
        }
        let Some(tool_calls) = entry.get("tool_calls").and_then(Value::as_array) else {
            continue;
        };
        for call in tool_calls.iter().rev() {
            let Some(command) = call
                .get("args")
                .and_then(|args| args.get("CommandLine"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            let Some(text) = extract_send_payload(command, thread) else {
                continue;
            };
            let succeeded = attempt[index + 1..]
                .iter()
                .take_while(|next| {
                    next.get("type").and_then(Value::as_str) != Some("PLANNER_RESPONSE")
                })
                .any(|next| {
                    next.get("type").and_then(Value::as_str) == Some("GENERIC") && {
                        let output = antigravity_text(next);
                        output.contains("exited with code 0")
                            && output.contains(crate::commands::send::RECIPIENT_FEEDBACK_PREFIX)
                    }
                });
            if succeeded {
                return Ok(RecoveredProviderResult {
                    text,
                    provider: Tool::Antigravity.as_str().to_string(),
                    evidence: "successful_completion_send",
                });
            }
        }
    }

    if let Some(entry) = attempt
        .iter()
        .rev()
        .find(|entry| entry.get("type").and_then(Value::as_str) == Some("PLANNER_RESPONSE"))
        && entry.get("status").and_then(Value::as_str) == Some("DONE")
        && entry
            .get("tool_calls")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty)
    {
        let text = antigravity_text(entry);
        if !text.is_empty() {
            return Ok(RecoveredProviderResult {
                text,
                provider: Tool::Antigravity.as_str().to_string(),
                evidence: "terminal_provider_response",
            });
        }
    }

    Err("Antigravity worker stopped without a recoverable final result".to_string())
}

fn claude_session_matches(entry: &Value, session_id: Option<&str>) -> bool {
    session_id
        .is_none_or(|expected| entry.get("sessionId").and_then(Value::as_str) == Some(expected))
}

fn recover_claude(
    entries: &[Value],
    session_id: Option<&str>,
    thread: &str,
) -> Result<RecoveredProviderResult, String> {
    let marker = entries
        .iter()
        .rposition(|entry| {
            claude_session_matches(entry, session_id)
                && entry.get("type").and_then(Value::as_str) == Some("user")
                && text_contains_thread(
                    &content_text(
                        entry
                            .get("message")
                            .and_then(|message| message.get("content")),
                    ),
                    thread,
                )
        })
        .ok_or_else(|| "Claude transcript has no matching workflow turn".to_string())?;

    let has_later_user_input = entries[marker + 1..].iter().any(|entry| {
        claude_session_matches(entry, session_id)
            && entry.get("type").and_then(Value::as_str) == Some("user")
            && !content_text(
                entry
                    .get("message")
                    .and_then(|message| message.get("content")),
            )
            .is_empty()
    });
    if has_later_user_input {
        return Err(
            "Claude transcript contains later user turns after matching workflow turn".to_string(),
        );
    }

    let attempt = &entries[marker + 1..];

    let successful_tool_ids: HashSet<&str> = attempt
        .iter()
        .filter(|entry| claude_session_matches(entry, session_id))
        .filter_map(|entry| {
            entry
                .get("message")
                .and_then(|message| message.get("content"))
                .and_then(Value::as_array)
        })
        .flatten()
        .filter(|block| {
            block.get("type").and_then(Value::as_str) == Some("tool_result")
                && block.get("is_error").and_then(Value::as_bool) != Some(true)
                && content_text(block.get("content"))
                    .contains(crate::commands::send::RECIPIENT_FEEDBACK_PREFIX)
        })
        .filter_map(|block| block.get("tool_use_id").and_then(Value::as_str))
        .collect();

    for entry in attempt.iter().rev() {
        if !claude_session_matches(entry, session_id)
            || entry.get("type").and_then(Value::as_str) != Some("assistant")
        {
            continue;
        }
        let Some(blocks) = entry
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for block in blocks.iter().rev() {
            if block.get("type").and_then(Value::as_str) != Some("tool_use")
                || !block
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| successful_tool_ids.contains(id))
            {
                continue;
            }
            let Some(command) = block
                .get("input")
                .and_then(|input| input.get("command"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            if let Some(text) = extract_send_payload(command, thread) {
                return Ok(RecoveredProviderResult {
                    text,
                    provider: Tool::Claude.as_str().to_string(),
                    evidence: "successful_completion_send",
                });
            }
        }
    }

    let terminal = attempt
        .iter()
        .rev()
        .find(|entry| {
            claude_session_matches(entry, session_id)
                && entry.get("type").and_then(Value::as_str) == Some("assistant")
        })
        .and_then(|entry| entry.get("message"))
        .and_then(|message| message.get("stop_reason"))
        .and_then(Value::as_str)
        == Some("end_turn");
    let text = attempt
        .iter()
        .filter(|entry| {
            claude_session_matches(entry, session_id)
                && entry.get("type").and_then(Value::as_str) == Some("assistant")
        })
        .map(|entry| {
            content_text(
                entry
                    .get("message")
                    .and_then(|message| message.get("content")),
            )
        })
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if terminal && !text.trim().is_empty() {
        return Ok(RecoveredProviderResult {
            text: text.trim().to_string(),
            provider: Tool::Claude.as_str().to_string(),
            evidence: "terminal_provider_response",
        });
    }

    Err("Claude worker stopped without a recoverable final result".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn jsonl(entries: &[Value]) -> String {
        entries
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn antigravity_recovery_requires_matching_turn_and_successful_send() {
        let entries = vec![
            json!({"type":"USER_INPUT","content":"task on thread workflow-1"}),
            json!({
                "type":"PLANNER_RESPONSE","status":"DONE",
                "tool_calls":[{"args":{"CommandLine":"hcom send @parent --thread workflow-1 --intent inform -- 'done safely'"}}]
            }),
            json!({"type":"GENERIC","content":"The command exited with code 0. Output: Sent to: parent"}),
        ];
        let result = recover_antigravity(&entries, "workflow-1").unwrap();
        assert_eq!(result.text, "done safely");
        assert_eq!(result.evidence, "successful_completion_send");
        assert!(recover_antigravity(&entries, "workflow-2").is_err());
    }

    #[test]
    fn completion_send_recovery_requires_inform_intent() {
        assert_eq!(
            extract_send_payload(
                "hcom send @parent --thread workflow-1 --intent=inform -- 'final report'",
                "workflow-1",
            ),
            Some("final report".to_string())
        );
        assert_eq!(
            extract_send_payload(
                "hcom send @parent --thread workflow-1 --intent request -- 'a question'",
                "workflow-1",
            ),
            None
        );
        assert_eq!(
            extract_send_payload(
                "hcom send @parent --thread workflow-1 -- 'ambiguous message'",
                "workflow-1",
            ),
            None
        );
    }

    #[test]
    fn antigravity_does_not_recover_a_failed_send() {
        let entries = vec![
            json!({"type":"USER_INPUT","content":"task on thread workflow-1"}),
            json!({
                "type":"PLANNER_RESPONSE","status":"DONE",
                "tool_calls":[{"args":{"CommandLine":"hcom send @parent --thread workflow-1 -- 'not delivered'"}}]
            }),
            json!({"type":"GENERIC","content":"The command exited with code 1."}),
        ];
        assert!(recover_antigravity(&entries, "workflow-1").is_err());
    }

    #[test]
    fn antigravity_does_not_replay_an_earlier_response_after_later_work() {
        let entries = vec![
            json!({"type":"USER_INPUT","content":"task on thread workflow-1"}),
            json!({"type":"PLANNER_RESPONSE","status":"DONE","content":"early answer"}),
            json!({
                "type":"PLANNER_RESPONSE","status":"DONE",
                "tool_calls":[{"args":{"CommandLine":"hcom send @parent --thread workflow-1 -- 'unfinished'"}}]
            }),
            json!({"type":"GENERIC","content":"The command exited with code 1."}),
        ];
        assert!(recover_antigravity(&entries, "workflow-1").is_err());
    }

    #[test]
    fn antigravity_recovers_only_the_terminal_provider_response() {
        let entries = vec![
            json!({"type":"USER_INPUT","content":"task on thread workflow-1"}),
            json!({
                "type":"PLANNER_RESPONSE","status":"DONE","content":"final response"
            }),
        ];
        let result = recover_antigravity(&entries, "workflow-1").unwrap();
        assert_eq!(result.text, "final response");
        assert_eq!(result.evidence, "terminal_provider_response");
    }

    #[test]
    fn claude_recovery_is_session_and_thread_scoped() {
        let entries = vec![
            json!({
                "type":"user","sessionId":"old","message":{"content":"task workflow-1"}
            }),
            json!({
                "type":"assistant","sessionId":"old",
                "message":{"stop_reason":"end_turn","content":[{"type":"text","text":"old result"}]}
            }),
            json!({
                "type":"user","sessionId":"current","message":{"content":"task workflow-1"}
            }),
            json!({
                "type":"assistant","sessionId":"current",
                "message":{"stop_reason":"end_turn","content":[{"type":"text","text":"current result"}]}
            }),
        ];
        let result = recover_claude(&entries, Some("current"), "workflow-1").unwrap();
        assert_eq!(result.text, "current result");
        assert!(recover_claude(&entries, Some("missing"), "workflow-1").is_err());
    }

    #[test]
    fn claude_does_not_treat_an_earlier_end_turn_as_a_later_partial_result() {
        let entries = vec![
            json!({
                "type":"user","sessionId":"current","message":{"content":"task workflow-1"}
            }),
            json!({
                "type":"assistant","sessionId":"current",
                "message":{"stop_reason":"end_turn","content":[{"type":"text","text":"early"}]}
            }),
            json!({
                "type":"assistant","sessionId":"current",
                "message":{"stop_reason":"tool_use","content":[{"type":"text","text":"still working"}]}
            }),
        ];
        assert!(recover_claude(&entries, Some("current"), "workflow-1").is_err());
    }

    #[test]
    fn provider_recovery_reads_a_successful_claude_completion_send() {
        let temp = tempfile::TempDir::new().unwrap();
        let transcript = temp.path().join("session.jsonl");
        let entries = vec![
            json!({
                "type":"user","sessionId":"session-a","message":{"content":"task workflow-1"}
            }),
            json!({
                "type":"assistant","sessionId":"session-a","message":{
                    "stop_reason":"tool_use",
                    "content":[{"type":"tool_use","id":"tool-1","input":{
                        "command":"hcom send @parent --thread workflow-1 --intent inform -- 'exact report'"
                    }}]
                }
            }),
            json!({
                "type":"user","sessionId":"session-a","message":{"content":[{
                    "type":"tool_result","tool_use_id":"tool-1","is_error":false,"content":"Sent to: parent"
                }]}
            }),
        ];
        std::fs::write(&transcript, jsonl(&entries)).unwrap();
        let result = recover_provider_result(
            "claude",
            transcript.to_str().unwrap(),
            Some("session-a"),
            "workflow-1",
        )
        .unwrap();
        assert_eq!(result.text, "exact report");
        assert_eq!(result.provider, "claude");
        assert!(
            recover_provider_result("claude", transcript.to_str().unwrap(), None, "workflow-1",)
                .is_err()
        );
    }

    #[test]
    fn extract_send_payload_flags_in_body_cannot_spoof_thread_or_intent() {
        let cmd = "hcom send @parent --thread other --intent request -- 'some message with --thread workflow-1 --intent inform in body'";
        assert_eq!(extract_send_payload(cmd, "workflow-1"), None);
    }

    #[test]
    fn thread_prefix_collision_does_not_match() {
        for suffix in ["10", ".2", ":sub", "/child", "+aux", "_test", "-sub"] {
            let collision_thread = format!("workflow-1{suffix}");
            let entries = vec![
                json!({"type":"USER_INPUT","content":format!("task on thread {collision_thread}")}),
                json!({
                    "type":"PLANNER_RESPONSE","status":"DONE","content":format!("result for {collision_thread}")
                }),
            ];
            assert!(
                recover_antigravity(&entries, "workflow-1").is_err(),
                "antigravity must not recover for workflow-1 when prompt only had {collision_thread}"
            );

            let claude_entries = vec![
                json!({
                    "type":"user","sessionId":"session-1","message":{"content":format!("task {collision_thread}")}
                }),
                json!({
                    "type":"assistant","sessionId":"session-1",
                    "message":{"stop_reason":"end_turn","content":[{"type":"text","text":"result"}]}
                }),
            ];
            assert!(
                recover_claude(&claude_entries, Some("session-1"), "workflow-1").is_err(),
                "claude must not recover for workflow-1 when prompt only had {collision_thread}"
            );
        }

        for prefix in ["foo:", "my-", "prefix.", "dir/", "prefix_"] {
            let collision_thread = format!("{prefix}workflow-1");
            let entries = vec![
                json!({"type":"USER_INPUT","content":format!("task on thread {collision_thread}")}),
                json!({
                    "type":"PLANNER_RESPONSE","status":"DONE","content":format!("result for {collision_thread}")
                }),
            ];
            assert!(
                recover_antigravity(&entries, "workflow-1").is_err(),
                "antigravity must not recover for workflow-1 when prompt only had {collision_thread}"
            );
        }
    }

    #[test]
    fn text_contains_thread_enforces_unambiguous_boundaries() {
        // Standalone matches bounded by whitespace or string ends
        assert!(text_contains_thread("task workflow-1 please", "workflow-1"));
        assert!(text_contains_thread("workflow-1", "workflow-1"));
        assert!(text_contains_thread("workflow-1\nnext", "workflow-1"));
        assert!(text_contains_thread(
            "task on thread workflow-1",
            "workflow-1"
        ));
        assert!(text_contains_thread(
            "multiword thread test",
            "multiword thread test"
        ));
        assert!(text_contains_thread(
            "task for multiword thread test now",
            "multiword thread test"
        ));

        // Thread containing literal punctuation matches exactly
        assert!(text_contains_thread(
            "task for workflow-1.2 now",
            "workflow-1.2"
        ));
        assert!(!text_contains_thread(
            "task for workflow-1.2.3 now",
            "workflow-1.2"
        ));

        // Reject non-whitespace suffixes
        assert!(!text_contains_thread("task workflow-10", "workflow-1"));
        assert!(!text_contains_thread("task workflow-1.2", "workflow-1"));
        assert!(!text_contains_thread("task workflow-1:child", "workflow-1"));
        assert!(!text_contains_thread("task workflow-1/sub", "workflow-1"));
        assert!(!text_contains_thread("task workflow-1+aux", "workflow-1"));
        assert!(!text_contains_thread("task workflow-1_test", "workflow-1"));
        assert!(!text_contains_thread("task workflow-1-part2", "workflow-1"));

        // Reject non-whitespace prefixes
        assert!(!text_contains_thread("task foo:workflow-1", "workflow-1"));
        assert!(!text_contains_thread("task my-workflow-1", "workflow-1"));
        assert!(!text_contains_thread(
            "task prefix.workflow-1",
            "workflow-1"
        ));
        assert!(!text_contains_thread("task dir/workflow-1", "workflow-1"));
        assert!(!text_contains_thread(
            "task prefix_workflow-1",
            "workflow-1"
        ));

        // Fail closed on ambiguous trailing punctuation or quoting rather than guessing
        assert!(!text_contains_thread("task workflow-1.", "workflow-1"));
        assert!(!text_contains_thread("task workflow-1,", "workflow-1"));
        assert!(!text_contains_thread("task workflow-1:", "workflow-1"));
        assert!(!text_contains_thread("task 'workflow-1'", "workflow-1"));
        assert!(!text_contains_thread("task \"workflow-1\"", "workflow-1"));
        assert!(!text_contains_thread("[workflow-1]", "workflow-1"));
        assert!(!text_contains_thread("--thread=workflow-1", "workflow-1"));
    }

    #[test]
    fn text_contains_thread_recognizes_production_envelopes() {
        use crate::cli_context::format_envelope_prefix;

        // Formatter-generated envelopes with reply-to id
        let env_inform = format!(
            "{} sender → recipient: do work",
            format_envelope_prefix(Some("inform"), Some("workflow-1"), Some("42"))
        );
        assert!(text_contains_thread(&env_inform, "workflow-1"));

        let env_request = format!(
            "{} sender → recipient: do work",
            format_envelope_prefix(Some("request"), Some("workflow-1"), Some("42"))
        );
        assert!(text_contains_thread(&env_request, "workflow-1"));

        let env_ack = format!(
            "{} sender → recipient: do work",
            format_envelope_prefix(Some("ack"), Some("workflow-1"), Some("42"))
        );
        assert!(text_contains_thread(&env_ack, "workflow-1"));

        let env_thread_only = format!(
            "{} sender → recipient: do work",
            format_envelope_prefix(None, Some("workflow-1"), Some("42"))
        );
        assert!(text_contains_thread(&env_thread_only, "workflow-1"));

        // Formatter-generated envelopes without reply-to id (closing bracket directly after thread)
        let env_no_reply = format!(
            "{} sender → recipient: do work",
            format_envelope_prefix(Some("inform"), Some("workflow-1"), None)
        );
        assert!(text_contains_thread(&env_no_reply, "workflow-1"));

        let env_thread_no_reply = format!(
            "{} sender → recipient: do work",
            format_envelope_prefix(None, Some("workflow-1"), None)
        );
        assert!(text_contains_thread(&env_thread_no_reply, "workflow-1"));

        // Envelopes inside <hcom> tags
        let env_hcom = format!(
            "<hcom>{} sender → recipient: do work</hcom>",
            format_envelope_prefix(Some("request"), Some("workflow-1"), Some("42"))
        );
        assert!(text_contains_thread(&env_hcom, "workflow-1"));

        // Thread containing literal punctuation formatted in envelope
        let env_punct = format!(
            "{} sender → recipient: do work",
            format_envelope_prefix(Some("inform"), Some("workflow-1.2"), Some("42"))
        );
        assert!(text_contains_thread(&env_punct, "workflow-1.2"));
        assert!(!text_contains_thread(&env_punct, "workflow-1"));

        // Suffix collisions in envelopes are rejected
        for suffix in [".2", ":child", "/sub", "+aux", "_test", "-sub"] {
            let collision_env = format!(
                "{} sender → recipient: do work",
                format_envelope_prefix(
                    Some("inform"),
                    Some(&format!("workflow-1{suffix}")),
                    Some("42")
                )
            );
            assert!(
                !text_contains_thread(&collision_env, "workflow-1"),
                "must reject collision envelope with suffix {suffix}"
            );
        }

        // Prefix collisions in envelopes are rejected
        let foreign_prefix_env = format!(
            "{} sender → recipient: do work",
            format_envelope_prefix(Some("inform"), Some("foo:workflow-1"), Some("42"))
        );
        assert!(!text_contains_thread(&foreign_prefix_env, "workflow-1"));

        // Unknown tags are rejected (only request|inform|ack|thread allowed)
        assert!(!text_contains_thread(
            "[unknown:workflow-1 #42]",
            "workflow-1"
        ));
        assert!(!text_contains_thread(
            "[custom:workflow-1 #42]",
            "workflow-1"
        ));
        assert!(!text_contains_thread("[ping:workflow-1 #42]", "workflow-1"));

        // Quoted full-envelopes are rejected (not an unquoted production delivery header)
        assert!(!text_contains_thread(
            "'[inform:workflow-1 #42]'",
            "workflow-1"
        ));
        assert!(!text_contains_thread(
            "\"[inform:workflow-1 #42]\"",
            "workflow-1"
        ));
        assert!(!text_contains_thread(
            "`[inform:workflow-1 #42]`",
            "workflow-1"
        ));
    }

    #[test]
    fn provider_recovery_matches_production_envelope_and_rejects_collisions() {
        use crate::cli_context::format_envelope_prefix;

        let prompt_envelope = format!(
            "{} caller → worker: please do workflow",
            format_envelope_prefix(Some("request"), Some("workflow-1"), Some("100"))
        );

        // Positive recovery in Antigravity
        let agy_entries = vec![
            json!({"type":"USER_INPUT","content":prompt_envelope}),
            json!({
                "type":"PLANNER_RESPONSE","status":"DONE","content":"agy result"
            }),
        ];
        let agy_res = recover_antigravity(&agy_entries, "workflow-1").unwrap();
        assert_eq!(agy_res.text, "agy result");

        // Positive recovery in Claude
        let claude_entries = vec![
            json!({
                "type":"user","sessionId":"sess-env",
                "message":{"content":prompt_envelope}
            }),
            json!({
                "type":"assistant","sessionId":"sess-env",
                "message":{"stop_reason":"end_turn","content":[{"type":"text","text":"claude result"}]}
            }),
        ];
        let claude_res = recover_claude(&claude_entries, Some("sess-env"), "workflow-1").unwrap();
        assert_eq!(claude_res.text, "claude result");

        // Negative: envelope containing workflow-1.2 rejected for workflow-1
        let collision_envelope = format!(
            "{} caller → worker: please do workflow",
            format_envelope_prefix(Some("request"), Some("workflow-1.2"), Some("100"))
        );
        let agy_collision_entries = vec![
            json!({"type":"USER_INPUT","content":collision_envelope}),
            json!({
                "type":"PLANNER_RESPONSE","status":"DONE","content":"agy result"
            }),
        ];
        assert!(recover_antigravity(&agy_collision_entries, "workflow-1").is_err());

        // Negative: foreign prefix foo:workflow-1 rejected for workflow-1
        let foreign_envelope = format!(
            "{} caller → worker: please do workflow",
            format_envelope_prefix(Some("request"), Some("foo:workflow-1"), Some("100"))
        );
        let agy_foreign_entries = vec![
            json!({"type":"USER_INPUT","content":foreign_envelope}),
            json!({
                "type":"PLANNER_RESPONSE","status":"DONE","content":"agy result"
            }),
        ];
        assert!(recover_antigravity(&agy_foreign_entries, "workflow-1").is_err());
    }

    #[test]
    fn later_unrelated_turn_fails_closed() {
        let entries = vec![
            json!({"type":"USER_INPUT","content":"task on thread workflow-1"}),
            json!({
                "type":"PLANNER_RESPONSE","status":"DONE","content":"result for workflow-1"
            }),
            json!({"type":"USER_INPUT","content":"subsequent unrelated user prompt"}),
            json!({
                "type":"PLANNER_RESPONSE","status":"DONE","content":"unrelated work"
            }),
        ];
        let err = recover_antigravity(&entries, "workflow-1").unwrap_err();
        assert!(err.contains("contains later user turns"));

        let claude_entries = vec![
            json!({
                "type":"user","sessionId":"session-1","message":{"content":"task workflow-1"}
            }),
            json!({
                "type":"assistant","sessionId":"session-1",
                "message":{"stop_reason":"end_turn","content":[{"type":"text","text":"result 1"}]}
            }),
            json!({
                "type":"user","sessionId":"session-1","message":{"content":"later user prompt"}
            }),
        ];
        let claude_err =
            recover_claude(&claude_entries, Some("session-1"), "workflow-1").unwrap_err();
        assert!(claude_err.contains("contains later user turns"));
    }

    #[test]
    fn corrupt_transcript_fails_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("corrupt.jsonl");
        std::fs::write(&path, "{\"valid\":1}\nnot valid json\n{\"valid\":2}\n").unwrap();
        let err = read_jsonl(&path).unwrap_err();
        assert!(err.contains("is corrupt"));
    }
}
