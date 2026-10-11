//! Copilot launch preprocessing: interactive-mode validation and workspace trust.

use std::path::{Path, PathBuf};

use anyhow::Context;
use serde_json::{Value, json};

const COPILOT_PRINT_FLAGS: &[&str] = &["-p", "--prompt", "--continue"];

/// Reject one-shot/resume shortcuts that would break hcom's PTY delivery model.
pub(crate) fn validate_copilot_args(tokens: &[String]) -> Vec<String> {
    let found: Vec<&str> = COPILOT_PRINT_FLAGS
        .iter()
        .copied()
        .filter(|flag| {
            tokens
                .iter()
                .any(|token| crate::tools::launch_arg_validation::long_flag_matches(token, flag))
        })
        .collect();
    if found.is_empty() {
        return Vec::new();
    }
    vec![format!(
        "Copilot one-shot/continue mode is not supported by `hcom copilot`: {} would bypass the live interactive PTY hooks used for message delivery. Remove the flag and launch the interactive or `--headless` PTY session instead.",
        found.join(", ")
    )]
}

fn copilot_config_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("COPILOT_HOME")
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    crate::runtime_env::tool_home().join(".copilot")
}

/// Copilot stores its permanently trusted directories in the automatically
/// managed `config.json` (not `settings.json`, which only holds user prefs like
/// model/theme). See Copilot CLI docs: "Setting trusted directories".
fn config_path() -> PathBuf {
    copilot_config_dir().join("config.json")
}

/// Copilot's `config.json` is JSONC: it begins with `//` header comment lines
/// that `serde_json` cannot parse. Split off the leading comment/blank lines so
/// the remainder is valid JSON, returning the header separately so it can be
/// preserved on rewrite. Only whole lines whose first non-whitespace characters
/// are `//` are treated as comments, so JSON string values (e.g. `https://…`)
/// are never mistaken for comments.
fn split_leading_comments(content: &str) -> (String, &str) {
    let mut split_at = 0;
    for line in content.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") || trimmed.trim_end().is_empty() {
            split_at += line.len();
        } else {
            break;
        }
    }
    (content[..split_at].to_string(), &content[split_at..])
}

/// Compute updated `config.json` contents that include `folder` in
/// `trustedFolders`, preserving any leading JSONC comment header and existing
/// keys. Returns `None` when the folder is already trusted (no write needed).
fn config_with_trusted_folder(existing: &str, folder: &str) -> anyhow::Result<Option<String>> {
    let (header, body) = split_leading_comments(existing);
    let mut root: serde_json::Map<String, Value> = if body.trim().is_empty() {
        serde_json::Map::new()
    } else {
        serde_json::from_str::<Value>(body)
            .context("invalid Copilot config.json")?
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Copilot config.json must be a JSON object"))?
    };
    let trusted = root
        .entry("trustedFolders".to_string())
        .or_insert_with(|| json!([]));
    if !trusted.is_array() {
        *trusted = json!([]);
    }
    let trusted = trusted.as_array_mut().unwrap();
    if trusted.iter().any(|entry| entry.as_str() == Some(folder)) {
        return Ok(None);
    }
    trusted.push(Value::String(folder.to_string()));
    let body_out = serde_json::to_string_pretty(&Value::Object(root))?;
    let mut content = format!("{header}{body_out}");
    if !content.ends_with('\n') {
        content.push('\n');
    }
    Ok(Some(content))
}

/// Pre-seed Copilot's folder trust list for PTY launches.
pub(crate) fn ensure_copilot_workspace_trusted(workspace: &Path) -> anyhow::Result<()> {
    let normalized = workspace
        .canonicalize()
        .unwrap_or_else(|_| workspace.to_path_buf());
    let path = config_path();
    let existing = if path.exists() {
        std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read Copilot config {}", path.display()))?
    } else {
        String::new()
    };
    let normalized_str = normalized.to_string_lossy().to_string();
    let Some(updated) = config_with_trusted_folder(&existing, &normalized_str)? else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    eprintln!(
        "[hcom] Auto-approving Copilot folder trust prompt for {} (config: {})",
        normalized.display(),
        path.display()
    );
    crate::paths::atomic_write_io(&path, &updated)?;
    Ok(())
}

/// Rewrite `settings.json` so Copilot runs hcom's extension: turns on the
/// EXTENSIONS flag (not all of experimental mode) when nothing chose otherwise.
/// An explicit choice against extensions (flag false, extension mode
/// "disabled", hcom in `disabledExtensions`) is an error, not overridden.
/// Returns the new file and a description of the change, or `None` when
/// nothing needed changing.
fn settings_with_extensions(existing: &str) -> anyhow::Result<Option<(String, String)>> {
    let (header, body) = split_leading_comments(existing);
    let mut root: serde_json::Map<String, Value> = if body.trim().is_empty() {
        serde_json::Map::new()
    } else {
        serde_json::from_str::<Value>(body)
            .context("invalid Copilot settings.json")?
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Copilot settings.json must be a JSON object"))?
    };

    let extensions = root.get("extensions");
    let mut disabled_by = Vec::new();
    let flag = root
        .get("enabledFeatureFlags")
        .and_then(|flags| flags.get("EXTENSIONS"))
        .and_then(Value::as_bool);
    if flag == Some(false) {
        disabled_by.push("enabledFeatureFlags.EXTENSIONS is false");
    }
    if extensions
        .and_then(|e| e.get("mode"))
        .and_then(Value::as_str)
        == Some("disabled")
    {
        disabled_by.push("extensions.mode is \"disabled\"");
    }
    let hcom_disabled = extensions
        .and_then(|e| e.get("disabledExtensions"))
        .and_then(Value::as_array)
        .is_some_and(|ids| {
            ids.iter().filter_map(Value::as_str).any(|id| {
                id.split(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
                    .any(|part| part == "hcom")
            })
        });
    if hcom_disabled {
        disabled_by.push("hcom is in extensions.disabledExtensions");
    }
    if !disabled_by.is_empty() {
        anyhow::bail!(
            "Copilot extensions are turned off ({}); turn them back on with /extensions in Copilot",
            disabled_by.join(", ")
        );
    }

    let experimental = root.get("experimental").and_then(Value::as_bool) == Some(true);
    if flag == Some(true) || experimental {
        return Ok(None);
    }
    let flags = root
        .entry("enabledFeatureFlags".to_string())
        .or_insert_with(|| json!({}));
    if !flags.is_object() {
        *flags = json!({});
    }
    flags
        .as_object_mut()
        .unwrap()
        .insert("EXTENSIONS".to_string(), Value::Bool(true));
    let mut content = format!(
        "{header}{}",
        serde_json::to_string_pretty(&Value::Object(root))?
    );
    if !content.ends_with('\n') {
        content.push('\n');
    }
    Ok(Some((
        content,
        "enabledFeatureFlags.EXTENSIONS = true".to_string(),
    )))
}

/// Make sure Copilot will run hcom's extension (`copilot_delivery = "native"` launches),
/// saying once on stderr what was changed.
pub(crate) fn ensure_copilot_extensions_enabled(copilot_home: &Path) -> anyhow::Result<()> {
    let path = copilot_home.join("settings.json");
    let existing = match std::fs::read_to_string(&path) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read {}", path.display()));
        }
    };
    let Some((updated, change)) =
        settings_with_extensions(&existing).with_context(|| path.display().to_string())?
    else {
        return Ok(());
    };
    std::fs::create_dir_all(copilot_home)?;
    crate::paths::atomic_write_io(&path, &updated)
        .with_context(|| format!("failed to write {}", path.display()))?;
    eprintln!(
        "[hcom] Enabled Copilot CLI extensions for hcom delivery ({}: {})",
        path.display(),
        change
    );
    crate::log::log_info(
        "copilot",
        "copilot.ext_settings_enabled",
        &format!("{}: {change}", path.display()),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extensions_settings_enabled_unless_explicitly_disabled() {
        let (out, change) = settings_with_extensions("{\n  \"model\": \"auto\"\n}")
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value["enabledFeatureFlags"]["EXTENSIONS"], true);
        assert_eq!(value["model"], "auto");
        assert_eq!(change, "enabledFeatureFlags.EXTENSIONS = true");
        assert!(settings_with_extensions("").unwrap().is_some());

        for already_on in [
            r#"{"experimental": true}"#,
            r#"{"enabledFeatureFlags": {"EXTENSIONS": true}}"#,
            r#"{"experimental": true, "extensions": {"mode": "load_only", "disabledExtensions": ["mine"]}}"#,
        ] {
            assert!(settings_with_extensions(already_on).unwrap().is_none());
        }

        for (explicitly_off, reason) in [
            (
                r#"{"experimental": true, "enabledFeatureFlags": {"EXTENSIONS": false}}"#,
                "EXTENSIONS is false",
            ),
            (
                r#"{"experimental": true, "extensions": {"mode": "disabled"}}"#,
                "mode",
            ),
            (
                r#"{"experimental": true, "extensions": {"disabledExtensions": ["plugin:hcom/hcom"]}}"#,
                "disabledExtensions",
            ),
        ] {
            let err = settings_with_extensions(explicitly_off)
                .unwrap_err()
                .to_string();
            assert!(err.contains(reason), "{err}");
        }

        assert!(settings_with_extensions("{ not json").is_err());
    }

    #[test]
    fn trust_write_adds_folder_to_empty_config() {
        let out = config_with_trusted_folder("", "/ws/a").unwrap().unwrap();
        let value: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value["trustedFolders"], json!(["/ws/a"]));
    }

    #[test]
    fn trust_write_preserves_jsonc_header_and_existing_keys() {
        let existing = "// User settings belong in settings.json.\n// This file is managed automatically.\n{\n  \"model\": \"gpt-5\",\n  \"trustedFolders\": [\"/ws/a\"]\n}\n";
        let out = config_with_trusted_folder(existing, "/ws/b")
            .unwrap()
            .unwrap();
        assert!(out.starts_with("// User settings belong in settings.json.\n"));
        let (_, body) = split_leading_comments(&out);
        let value: Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["trustedFolders"], json!(["/ws/a", "/ws/b"]));
        assert_eq!(value["model"], "gpt-5");
    }

    #[test]
    fn trust_write_is_noop_when_already_trusted() {
        let existing = "{ \"trustedFolders\": [\"/ws/a\"] }";
        assert!(
            config_with_trusted_folder(existing, "/ws/a")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn split_leading_comments_does_not_eat_string_values() {
        let existing = "{\n  \"url\": \"https://example.com\"\n}";
        let (header, body) = split_leading_comments(existing);
        assert!(header.is_empty());
        assert_eq!(body, existing);
    }

    #[test]
    fn validate_copilot_args_rejects_prompt_modes() {
        let tokens: Vec<String> = ["--model", "auto", "-p", "--prompt", "--continue"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let errors = validate_copilot_args(&tokens);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("-p, --prompt, --continue"));
    }

    #[test]
    fn validate_copilot_args_rejects_equals_form_prompt() {
        let tokens = vec!["--prompt=do the thing".to_string()];
        let errors = validate_copilot_args(&tokens);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("--prompt"));
    }
}
