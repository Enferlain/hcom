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
    let entries: Vec<Value> = content
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
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
    if tokens.get(1).map(String::as_str) != Some("send")
        || !command_matches_thread(&tokens, thread)
        || !command_has_inform_intent(&tokens)
    {
        return None;
    }

    let separator = tokens.iter().position(|token| token == "--")?;
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
                && antigravity_text(entry).contains(thread)
        })
        .ok_or_else(|| "Antigravity transcript has no matching workflow turn".to_string())?;

    let attempt_end = entries[marker + 1..]
        .iter()
        .position(|entry| entry.get("type").and_then(Value::as_str) == Some("USER_INPUT"))
        .map_or(entries.len(), |offset| marker + 1 + offset);
    let attempt = &entries[marker + 1..attempt_end];
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
                && content_text(
                    entry
                        .get("message")
                        .and_then(|message| message.get("content")),
                )
                .contains(thread)
        })
        .ok_or_else(|| "Claude transcript has no matching workflow turn".to_string())?;
    let attempt_end = entries[marker + 1..]
        .iter()
        .position(|entry| {
            claude_session_matches(entry, session_id)
                && entry.get("type").and_then(Value::as_str) == Some("user")
                && !content_text(
                    entry
                        .get("message")
                        .and_then(|message| message.get("content")),
                )
                .is_empty()
        })
        .map_or(entries.len(), |offset| marker + 1 + offset);
    let attempt = &entries[marker + 1..attempt_end];

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
}
