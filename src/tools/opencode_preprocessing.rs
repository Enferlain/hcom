//! OpenCode launch preprocessing — sets environment variables for hcom integration.
//! Plugin management is handled separately in hooks/opencode.rs.

use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

/// OpenCode 2 hosts plugins in a shared background service that never sees
/// hcom's launch env; `--standalone` runs a private server per launch instead.
const STANDALONE_FLAG: &str = "--standalone";
/// OpenCode 2's TUI has no `--fork`; the fork happens server-side instead.
const FORK_FLAG: &str = "--fork";
const SESSION_FLAG: &str = "--session";
const SESSION_FLAGS: &[&str] = &["--session", "-s"];
/// OpenCode 2's TUI has no `--agent`/`--model`; the plugin applies them as the
/// server's defaults from these env vars.
const AGENT_FLAGS: &[&str] = &["--agent"];
const MODEL_FLAGS: &[&str] = &["--model", "-m"];
const AGENT_ENV: &str = "HCOM_OPENCODE_AGENT";
const MODEL_ENV: &str = "HCOM_OPENCODE_MODEL";
const OPENCODE_2_MAJOR: u64 = 2;

fn opencode_permission_json() -> String {
    let prefix = crate::runtime_env::build_hcom_command();
    let bash = crate::hooks::common::SAFE_HCOM_COMMANDS
        .iter()
        .map(|command| (format!("{prefix} {command}*"), serde_json::json!("allow")))
        .collect();
    serde_json::Value::Object(serde_json::Map::from_iter([(
        "bash".to_string(),
        serde_json::Value::Object(bash),
    )]))
    .to_string()
}

/// Preprocess environment variables for an OpenCode-family launch.
///
/// Sets:
/// - App-specific permission override: Auto-approve safe hcom bash commands when enabled
/// - `HCOM_NAME`: Instance name for plugin diagnostics (set before identity binding)
pub fn preprocess_opencode_env(
    env: &mut HashMap<String, String>,
    tool: &str,
    instance_name: &str,
    auto_approve: bool,
) {
    if auto_approve {
        let key = if tool == "kilo" {
            "KILO_PERMISSION"
        } else {
            "OPENCODE_PERMISSION"
        };
        env.insert(key.to_string(), opencode_permission_json());
    }
    env.insert("HCOM_NAME".to_string(), instance_name.to_string());
}

/// Preprocess OpenCode launch args for OpenCode 2+: move `--agent`/`--model` into
/// `env`, fork a `--session <id> --fork` through the server API, then add `--standalone`.
pub fn preprocess_opencode_args(
    args: &[String],
    env: &mut HashMap<String, String>,
    cwd: &Path,
) -> Result<Vec<String>> {
    preprocess_opencode_args_internal(args, env, is_opencode_2(), |id, server| {
        fork_session(id, server, cwd)
    })
}

fn preprocess_opencode_args_internal<F>(
    args: &[String],
    env: &mut HashMap<String, String>,
    is_v2: bool,
    fork_fn: F,
) -> Result<Vec<String>>
where
    F: FnOnce(&str, &[String]) -> Result<String>,
{
    if !is_v2 {
        return Ok(args.to_vec());
    }
    let (args, agent) = take_value_arg(args, AGENT_FLAGS)?;
    let (args, model) = take_value_arg(&args, MODEL_FLAGS)?;
    if let Some(ref model) = model {
        validate_model_arg(model)?;
    }
    let remote = chosen_server(&args).is_some_and(|server| server[0] != STANDALONE_FLAG);
    if remote && (agent.is_some() || model.is_some()) {
        bail!("--agent/--model cannot reach a server chosen with --server on OpenCode 2");
    }
    // Scrub any inherited agent or model before inserting new values
    env.remove(AGENT_ENV);
    env.remove(MODEL_ENV);
    if let Some(agent) = agent {
        env.insert(AGENT_ENV.to_string(), agent);
    }
    if let Some(model) = model {
        env.insert(MODEL_ENV.to_string(), model);
    }
    let server = chosen_server(&args).unwrap_or_else(|| vec![STANDALONE_FLAG.to_string()]);
    let mut forked_id = None;
    let args = fork_session_server_side(&args, |id| {
        let forked = fork_fn(id, &server)?;
        forked_id = Some(forked.clone());
        Ok(forked)
    })?;
    if let Some(id) = forked_id {
        env.insert("HCOM_RESUME_SESSION_ID".to_string(), id);
    }
    Ok(add_standalone(&args))
}

/// Removes every `--flag value` / `--flag=value` / `-fvalue` for `flags`; returns the last value.
fn take_value_arg(args: &[String], flags: &[&str]) -> Result<(Vec<String>, Option<String>)> {
    let mut kept = Vec::with_capacity(args.len());
    let mut value = None;
    let mut tokens = args.iter().peekable();
    while let Some(token) = tokens.next() {
        if flags.contains(&token.as_str()) {
            let Some(next) = tokens.next_if(|next| !next.starts_with('-')) else {
                bail!("{token} needs a value");
            };
            if next.trim().is_empty() {
                bail!("{token} needs a value");
            }
            value = Some(next.clone());
        } else if let Some(inline) = flags
            .iter()
            .find_map(|flag| token.strip_prefix(&format!("{flag}=")))
        {
            if inline.trim().is_empty() {
                bail!("{token} needs a value");
            }
            value = Some(inline.to_string());
        } else if let Some(attached) = flags.iter().find_map(|flag| {
            if flag.starts_with('-') && !flag.starts_with("--") && flag.chars().count() == 2 {
                token.strip_prefix(flag)
            } else {
                None
            }
        }) {
            if attached.trim().is_empty() || attached.starts_with('-') {
                bail!("{token} needs a value");
            }
            value = Some(attached.to_string());
        } else {
            kept.push(token.clone());
        }
    }
    Ok((kept, value))
}

fn validate_model_arg(model: &str) -> Result<()> {
    let (provider, rest) = model.split_once('/').with_context(|| {
        format!("invalid OpenCode model '{model}': expected provider/model[#variant]")
    })?;
    let (model_id, variant) = match rest.split_once('#') {
        Some((id, variant)) => (id, Some(variant)),
        None => (rest, None),
    };
    if provider.is_empty()
        || provider.contains('#')
        || model_id.is_empty()
        || model.chars().any(char::is_whitespace)
        || variant.is_some_and(|variant| variant.is_empty() || variant.contains('#'))
    {
        bail!("invalid OpenCode model '{model}': expected provider/model[#variant]");
    }
    Ok(())
}

fn validate_session_id(id: &str) -> Result<()> {
    if id.is_empty() || id.starts_with('-') || id == "." {
        bail!("invalid session id: {id}");
    }
    if id.trim() != id {
        bail!("invalid session id contains whitespace: {id}");
    }
    if id.contains('/') || id.contains('\\') || id.contains("..") {
        bail!("invalid session id contains path separators or traversal: {id}");
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        bail!("invalid session id contains invalid characters: {id}");
    }
    Ok(())
}

/// The server flags the user chose (`--standalone`, `--server <url>`), if any.
fn chosen_server(args: &[String]) -> Option<Vec<String>> {
    args.iter().enumerate().find_map(|(at, arg)| {
        if arg == STANDALONE_FLAG || arg.starts_with("--server=") {
            Some(vec![arg.clone()])
        } else if arg == "--server" {
            Some(args[at..].iter().take(2).cloned().collect())
        } else {
            None
        }
    })
}

/// Leaves the args alone when the user already chose a server.
fn add_standalone(args: &[String]) -> Vec<String> {
    let mut result = args.to_vec();
    if chosen_server(args).is_none() {
        result.insert(0, STANDALONE_FLAG.to_string());
    }
    result
}

/// Replaces `--session <id>` (or `--session=<id>`, `-s <id>`, `-s=<id>`) and `--fork`
/// with `--session <fork of id>`.
fn fork_session_server_side(
    args: &[String],
    fork: impl FnOnce(&str) -> Result<String>,
) -> Result<Vec<String>> {
    let Some(fork_at) = args.iter().position(|arg| arg == FORK_FLAG) else {
        return Ok(args.to_vec());
    };

    enum SessionArg {
        Separate {
            val_idx: usize,
            value: String,
        },
        Inline {
            flag_idx: usize,
            flag_prefix: String,
            value: String,
        },
    }

    let mut session_occurrences = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if SESSION_FLAGS.contains(&arg.as_str()) {
            if i + 1 < args.len() {
                let next = &args[i + 1];
                if next.starts_with('-') || next.trim().is_empty() {
                    bail!("{FORK_FLAG} needs {SESSION_FLAG} <id>");
                }
                session_occurrences.push(SessionArg::Separate {
                    val_idx: i + 1,
                    value: next.clone(),
                });
                i += 2;
                continue;
            } else {
                bail!("{FORK_FLAG} needs {SESSION_FLAG} <id>");
            }
        } else if let Some(flag) = SESSION_FLAGS
            .iter()
            .find(|&&f| arg.starts_with(&format!("{f}=")))
        {
            let val = arg.strip_prefix(&format!("{flag}=")).unwrap();
            if val.trim().is_empty() || val.starts_with('-') {
                bail!("{FORK_FLAG} needs {SESSION_FLAG} <id>");
            }
            session_occurrences.push(SessionArg::Inline {
                flag_idx: i,
                flag_prefix: flag.to_string(),
                value: val.to_string(),
            });
        }
        i += 1;
    }

    if session_occurrences.is_empty() {
        bail!("{FORK_FLAG} needs {SESSION_FLAG} <id>");
    }

    // Check for conflicting session flags
    let first_val = match &session_occurrences[0] {
        SessionArg::Separate { value, .. } => value.as_str(),
        SessionArg::Inline { value, .. } => value.as_str(),
    };
    for occ in &session_occurrences[1..] {
        let val = match occ {
            SessionArg::Separate { value, .. } => value.as_str(),
            SessionArg::Inline { value, .. } => value.as_str(),
        };
        if val != first_val {
            bail!("conflicting session flags in arguments");
        }
    }

    validate_session_id(first_val)?;
    let forked = fork(first_val)?;
    validate_session_id(&forked)?;

    let mut result = args.to_vec();
    for occ in &session_occurrences {
        match occ {
            SessionArg::Separate { val_idx, .. } => {
                result[*val_idx] = forked.clone();
            }
            SessionArg::Inline {
                flag_idx,
                flag_prefix,
                ..
            } => {
                result[*flag_idx] = format!("{flag_prefix}={forked}");
            }
        }
    }
    result.remove(fork_at);
    Ok(result)
}

/// Forks on `server`, the one the launched TUI connects to.
fn fork_session(session_id: &str, server: &[String], cwd: &Path) -> Result<String> {
    let output = crate::terminal::executable_command("opencode")
        .arg("api")
        .args(server)
        .arg("POST")
        .arg(format!("/api/session/{session_id}/fork"))
        .args(["--data", "{}"])
        .current_dir(cwd)
        .output()
        .context("could not run opencode api to fork the session")?;
    if !output.status.success() {
        bail!(
            "opencode could not fork session {session_id}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let response: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("opencode fork response is not JSON")?;
    let id = response["data"]["id"]
        .as_str()
        .map(str::to_string)
        .with_context(|| format!("opencode fork response has no session id: {response}"))?;
    validate_session_id(&id)?;
    Ok(id)
}

fn parse_opencode_major_version(output: &str) -> Option<u64> {
    output
        .split(|c: char| !(c.is_ascii_digit() || c == '.'))
        .find(|token| token.matches('.').count() >= 2)
        .and_then(|token| token.split('.').next()?.parse().ok())
}

fn is_opencode_2() -> bool {
    static CACHE: OnceLock<bool> = OnceLock::new();
    *CACHE.get_or_init(|| {
        let output = match crate::terminal::executable_command("opencode")
            .arg("--version")
            .output()
        {
            Ok(output) => output,
            Err(e) => {
                crate::log::log_warn(
                    "opencode",
                    "opencode.version_failed",
                    &format!("could not run opencode --version; assuming OpenCode 1: {e}"),
                );
                return false;
            }
        };
        parse_opencode_major_version(&String::from_utf8_lossy(&output.stdout))
            .is_some_and(|major| major >= OPENCODE_2_MAJOR)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_sets_permission() {
        let mut env = HashMap::new();
        preprocess_opencode_env(&mut env, "opencode", "luna", true);
        let perm = env.get("OPENCODE_PERMISSION").unwrap();
        let prefix = crate::runtime_env::build_hcom_command();
        assert!(perm.contains(&format!("{prefix} send*")));
        assert!(!perm.contains(&format!("\"{prefix} *\"")));
        assert!(!perm.contains("hcom kill"));
    }

    #[test]
    fn test_preprocess_skips_permission_when_disabled() {
        let mut env = HashMap::new();
        preprocess_opencode_env(&mut env, "opencode", "luna", false);
        assert!(!env.contains_key("OPENCODE_PERMISSION"));
    }

    #[test]
    fn test_preprocess_sets_hcom_name() {
        let mut env = HashMap::new();
        preprocess_opencode_env(&mut env, "opencode", "nova", true);
        assert_eq!(env.get("HCOM_NAME").unwrap(), "nova");
    }

    #[test]
    fn test_preprocess_overwrites_existing() {
        let mut env = HashMap::new();
        env.insert("HCOM_NAME".to_string(), "old".to_string());
        preprocess_opencode_env(&mut env, "opencode", "nova", true);
        assert_eq!(env.get("HCOM_NAME").unwrap(), "nova");
    }

    #[test]
    fn test_preprocess_kilo_sets_kilo_permission() {
        let mut env = HashMap::new();
        preprocess_opencode_env(&mut env, "kilo", "luna", true);
        assert!(env.contains_key("KILO_PERMISSION"));
        assert!(!env.contains_key("OPENCODE_PERMISSION"));
    }

    #[test]
    fn test_permission_json_is_valid() {
        let parsed: serde_json::Value =
            serde_json::from_str(&opencode_permission_json()).expect("valid JSON");
        let prefix = crate::runtime_env::build_hcom_command();
        assert!(parsed["bash"][format!("{prefix} send*")].is_string());
    }

    #[test]
    fn test_parse_opencode_major_version() {
        assert_eq!(parse_opencode_major_version("opencode v2.0.15\n"), Some(2));
        assert_eq!(parse_opencode_major_version("1.14.48"), Some(1));
        assert_eq!(parse_opencode_major_version("opencode"), None);
    }

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    #[test]
    fn test_add_standalone() {
        assert_eq!(
            add_standalone(&strings(&["--model", "a/b"])),
            ["--standalone", "--model", "a/b"]
        );
    }

    #[test]
    fn test_add_standalone_respects_chosen_server() {
        for args in [
            strings(&["--standalone"]),
            strings(&["--server", "http://x"]),
            strings(&["--server=http://x"]),
        ] {
            assert_eq!(add_standalone(&args), args);
        }
    }

    #[test]
    fn test_take_value_arg_removes_both_forms() {
        let args = strings(&["--agent", "qa", "--session", "ses_src", "-m=google/x"]);
        let (rest, agent) = take_value_arg(&args, AGENT_FLAGS).unwrap();
        assert_eq!(agent.as_deref(), Some("qa"));
        let (rest, model) = take_value_arg(&rest, MODEL_FLAGS).unwrap();
        assert_eq!(model.as_deref(), Some("google/x"));
        assert_eq!(rest, ["--session", "ses_src"]);
    }

    #[test]
    fn test_take_value_arg_attached_short() {
        let args = strings(&["-mgoogle/gemini-2.0", "--other"]);
        let (kept, model) = take_value_arg(&args, MODEL_FLAGS).unwrap();
        assert_eq!(model.as_deref(), Some("google/gemini-2.0"));
        assert_eq!(kept, strings(&["--other"]));
    }

    #[test]
    fn test_take_value_arg_absent() {
        let args = strings(&["--session", "ses_src"]);
        assert_eq!(
            take_value_arg(&args, AGENT_FLAGS).unwrap(),
            (args.clone(), None)
        );
    }

    #[test]
    fn test_take_value_arg_rejects_missing_value() {
        for args in [
            strings(&["--model", "--session", "ses_src"]),
            strings(&["--agent"]),
            strings(&["--model="]),
            strings(&["--agent="]),
            strings(&["--model", ""]),
            strings(&["--agent", " "]),
        ] {
            assert!(take_value_arg(&args, &["--model", "--agent"]).is_err());
        }
    }

    #[test]
    fn test_validate_model_arg() {
        for model in ["opencode/big-pickle", "anthropic/claude-sonnet-4-6#fast"] {
            assert!(validate_model_arg(model).is_ok());
        }
        for model in [
            "invalid",
            "/model",
            "provider/",
            "provider/model#",
            "provider/model#a#b",
            "provider#bad/model",
            "provider/model name",
        ] {
            assert!(validate_model_arg(model).is_err());
        }
    }

    #[test]
    fn test_validate_session_id() {
        for id in ["ses_123", "abc-456", "session.xyz", "01992"] {
            assert!(validate_session_id(id).is_ok());
        }
        for bad in [
            "",
            "   ",
            ".",
            " ses_1",
            "ses_1 ",
            "  ses_1  ",
            "-flag",
            "--session",
            "dir/id",
            "../escape",
            r"c:\path",
            "ses with spaces",
            "bad$char",
        ] {
            assert!(validate_session_id(bad).is_err());
        }
    }

    #[test]
    fn test_chosen_server() {
        assert_eq!(chosen_server(&strings(&["--model", "a/b"])), None);
        assert_eq!(
            chosen_server(&strings(&["--session", "s", "--server", "http://x"])),
            Some(strings(&["--server", "http://x"]))
        );
        assert_eq!(
            chosen_server(&strings(&["--server=http://x"])),
            Some(strings(&["--server=http://x"]))
        );
        assert_eq!(
            chosen_server(&strings(&["--standalone"])),
            Some(strings(&["--standalone"]))
        );
    }

    #[test]
    fn test_fork_session_server_side_continues_the_fork() {
        let args = strings(&["--model", "a/b", "--session", "ses_src", "--fork"]);
        let forked = fork_session_server_side(&args, |id| {
            assert_eq!(id, "ses_src");
            Ok("ses_copy".to_string())
        })
        .unwrap();
        assert_eq!(forked, ["--model", "a/b", "--session", "ses_copy"]);
    }

    #[test]
    fn test_fork_session_server_side_short_flag() {
        let args = strings(&["--model", "a/b", "-s", "ses_src", "--fork"]);
        let forked = fork_session_server_side(&args, |id| {
            assert_eq!(id, "ses_src");
            Ok("ses_copy".to_string())
        })
        .unwrap();
        assert_eq!(forked, ["--model", "a/b", "-s", "ses_copy"]);
    }

    #[test]
    fn test_fork_session_server_side_inline_flags() {
        let args = strings(&["--session=ses_src", "--fork"]);
        let forked = fork_session_server_side(&args, |id| {
            assert_eq!(id, "ses_src");
            Ok("ses_copy".to_string())
        })
        .unwrap();
        assert_eq!(forked, ["--session=ses_copy"]);

        let args = strings(&["-s=ses_src", "--fork"]);
        let forked = fork_session_server_side(&args, |id| {
            assert_eq!(id, "ses_src");
            Ok("ses_copy".to_string())
        })
        .unwrap();
        assert_eq!(forked, ["-s=ses_copy"]);
    }

    #[test]
    fn test_fork_session_server_side_duplicate_flags_same_value() {
        let args = strings(&["--session", "ses_src", "-s", "ses_src", "--fork"]);
        let forked = fork_session_server_side(&args, |id| {
            assert_eq!(id, "ses_src");
            Ok("ses_copy".to_string())
        })
        .unwrap();
        assert_eq!(forked, ["--session", "ses_copy", "-s", "ses_copy"]);
    }

    #[test]
    fn test_fork_session_server_side_conflicting_flags_rejected() {
        let args = strings(&["--session", "ses_1", "-s", "ses_2", "--fork"]);
        let res = fork_session_server_side(&args, |_| panic!("should not fork"));
        assert!(res.is_err());
    }

    #[test]
    fn test_fork_session_server_side_rejects_invalid_returned_session_id() {
        let args = strings(&["--session", "ses_src", "--fork"]);
        for bad_returned in ["", "--flag", "a/b", "../bad"] {
            let res = fork_session_server_side(&args, |_| Ok(bad_returned.to_string()));
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_fork_session_server_side_leaves_plain_resume() {
        let args = strings(&["--session", "ses_src"]);
        let resumed = fork_session_server_side(&args, |_| panic!("no fork requested")).unwrap();
        assert_eq!(resumed, args);
    }

    #[test]
    fn test_fork_session_server_side_needs_a_session() {
        assert!(fork_session_server_side(&strings(&["--fork"]), |_| unreachable!()).is_err());
        let failed =
            fork_session_server_side(&strings(&["--session", "ses_src", "--fork"]), |_| {
                bail!("server down")
            });
        assert!(failed.is_err());
    }

    #[test]
    fn test_fork_session_server_side_rejects_missing_or_flag_session() {
        // Must reject without calling the fork closure
        for bad in [
            strings(&["--session", "--fork"]),
            strings(&["--session", "-m", "--fork"]),
            strings(&["--session", "", "--fork"]),
            strings(&["--session", "  ", "--fork"]),
            strings(&["--session=", "--fork"]),
            strings(&["-s=", "--fork"]),
            strings(&["-s", "--fork"]),
        ] {
            assert!(
                fork_session_server_side(&bad, |_| panic!("fork should not be called")).is_err()
            );
        }
    }

    #[test]
    fn test_preprocess_opencode_args_v2_rewrites_and_binds_forked_session() {
        let mut env = HashMap::new();
        // Inherited stale agent/model should be scrubbed
        env.insert(AGENT_ENV.to_string(), "stale_agent".to_string());
        env.insert(MODEL_ENV.to_string(), "stale_model".to_string());

        let args = strings(&[
            "--agent",
            "new_agent",
            "-m",
            "provider/new_model#fast",
            "-s",
            "source_ses",
            "--fork",
        ]);

        let effective = preprocess_opencode_args_internal(&args, &mut env, true, |id, server| {
            assert_eq!(id, "source_ses");
            assert_eq!(server, &[STANDALONE_FLAG]);
            Ok("forked_ses_99".to_string())
        })
        .unwrap();

        assert_eq!(effective, strings(&["--standalone", "-s", "forked_ses_99"]));
        assert_eq!(env.get(AGENT_ENV).unwrap(), "new_agent");
        assert_eq!(env.get(MODEL_ENV).unwrap(), "provider/new_model#fast");
        assert_eq!(env.get("HCOM_RESUME_SESSION_ID").unwrap(), "forked_ses_99");
    }

    #[test]
    fn test_preprocess_opencode_args_v2_scrubs_inherited_agent_and_model() {
        let mut env = HashMap::new();
        env.insert(AGENT_ENV.to_string(), "stale_agent".to_string());
        env.insert(MODEL_ENV.to_string(), "stale_model".to_string());

        let args = strings(&["--session", "ses_normal"]);

        let effective =
            preprocess_opencode_args_internal(&args, &mut env, true, |_, _| unreachable!())
                .unwrap();

        assert_eq!(
            effective,
            strings(&["--standalone", "--session", "ses_normal"])
        );
        assert!(!env.contains_key(AGENT_ENV));
        assert!(!env.contains_key(MODEL_ENV));
        assert!(!env.contains_key("HCOM_RESUME_SESSION_ID"));
    }

    #[test]
    fn test_preprocess_opencode_args_v2_rejects_remote_model_before_fork() {
        let mut env = HashMap::new();
        let args = strings(&[
            "--server",
            "http://127.0.0.1:4096",
            "--model",
            "provider/model",
            "--session",
            "ses_1",
            "--fork",
        ]);

        let res = preprocess_opencode_args_internal(&args, &mut env, true, |_, _| {
            panic!("should not call fork")
        });
        assert!(res.is_err());
    }

    #[test]
    fn test_preprocess_opencode_args_attached_model_env_and_arg_stripping() {
        let mut env = HashMap::new();
        let args = strings(&["-manthropic/claude-3-5-sonnet", "--session", "ses_1"]);
        let effective =
            preprocess_opencode_args_internal(&args, &mut env, true, |_, _| unreachable!())
                .unwrap();
        assert_eq!(effective, strings(&["--standalone", "--session", "ses_1"]));
        assert_eq!(env.get(MODEL_ENV).unwrap(), "anthropic/claude-3-5-sonnet");
    }

    #[test]
    fn test_fork_session_server_side_rejects_source_dot_and_whitespace_before_fork() {
        for bad_source in [
            strings(&["--session", ".", "--fork"]),
            strings(&["-s", ".", "--fork"]),
            strings(&["--session=.", "--fork"]),
            strings(&["-s=.", "--fork"]),
            strings(&["--session", " ses_1", "--fork"]),
            strings(&["--session", "ses_1 ", "--fork"]),
            strings(&["--session", "  ses_1  ", "--fork"]),
            strings(&["-s", " ses_1", "--fork"]),
            strings(&["--session= ses_1", "--fork"]),
            strings(&["-s=ses_1 ", "--fork"]),
        ] {
            let res = fork_session_server_side(&bad_source, |_| {
                panic!(
                    "fork closure should NOT be called for invalid source session id: {bad_source:?}"
                )
            });
            assert!(res.is_err(), "expected error for {bad_source:?}");
        }
    }

    #[test]
    fn test_fork_session_server_side_rejects_api_dot_and_whitespace() {
        let args = strings(&["--session", "ses_valid", "--fork"]);
        for bad_api in [
            ".",
            " forked_ses",
            "forked_ses ",
            "  forked_ses  ",
            "\tforked_ses",
        ] {
            let res = fork_session_server_side(&args, |_| Ok(bad_api.to_string()));
            assert!(res.is_err(), "expected error for API returned: {bad_api:?}");
        }
    }
}
