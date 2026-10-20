//! Explicit native hook participation.
//!
//! The normal integration path is still per-run (`hcom <tool>` injects a
//! temporary hook configuration).  This module owns the separate, opt-in
//! native path used by a user who wants an already-running Claude or Codex
//! session to participate.  All configuration helpers accept explicit fixture
//! paths so tests never touch a real profile.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use toml_edit::{DocumentMut, Item, Table, value};

use crate::db::{HcomDb, InstanceRow};
use crate::hooks::common::{
    NATIVE_CLAUDE_SESSION_ENV, NATIVE_CLAUDE_VALUE, NATIVE_CODEX_VALUE, NATIVE_PARTICIPATION_ENV,
};
use crate::paths;
use crate::shared::context::HcomContext;

/// Tools for which hcom exposes an explicit native hook installation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NativeTool {
    Claude,
    Codex,
}

impl NativeTool {
    pub const ALL: [Self; 2] = [Self::Claude, Self::Codex];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claude => NATIVE_CLAUDE_VALUE,
            Self::Codex => NATIVE_CODEX_VALUE,
        }
    }

    pub const fn marker(self) -> &'static str {
        self.as_str()
    }
}

impl fmt::Display for NativeTool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NativeTool {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            NATIVE_CLAUDE_VALUE => Ok(Self::Claude),
            NATIVE_CODEX_VALUE => Ok(Self::Codex),
            _ => Err(format!(
                "unknown native tool '{value}' (expected claude or codex)"
            )),
        }
    }
}

/// Explicit paths used by the native installer.  The command layer builds
/// this from the current environment; tests construct it under a tempdir.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeConfigPaths {
    pub claude_settings: PathBuf,
    pub codex_home: PathBuf,
}

impl NativeConfigPaths {
    pub fn from_environment() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let claude_dir = std::env::var_os("CLAUDE_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".claude"));
        let codex_home = std::env::var_os("CODEX_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".codex"));
        Self {
            claude_settings: claude_dir.join("settings.json"),
            codex_home,
        }
    }

    pub fn path_for(&self, tool: NativeTool) -> PathBuf {
        match tool {
            NativeTool::Claude => self.claude_settings.clone(),
            NativeTool::Codex => self.codex_home.join("hooks.json"),
        }
    }
}

/// Read-only status returned by add/remove/status operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeHookStatus {
    pub tool: NativeTool,
    pub path: PathBuf,
    pub installed: bool,
    pub owned_hooks: usize,
    pub expected_hooks: usize,
    /// For Codex, true means all owned hook positions have current trust state
    /// in config.toml.  Claude does not have a separate trust table.
    pub trust_configured: bool,
}

const NATIVE_RUN_SUBCOMMAND: &str = "run";

/// Build the exact hcom-owned command placed in a native profile.
///
/// The `native` path is an explicit handshake.  The router sets
/// `HCOM_NATIVE_PARTICIPATION` from this subcommand before dispatching the
/// event; no hook handler infers participation from a transcript, marker, or
/// pending database row.
pub fn native_hook_command(tool: NativeTool, event: &str) -> String {
    let mut parts = crate::runtime_env::get_hcom_prefix();
    parts.extend([
        "hooks".to_string(),
        "native".to_string(),
        NATIVE_RUN_SUBCOMMAND.to_string(),
        tool.as_str().to_string(),
        event.to_string(),
    ]);
    parts.join(" ")
}

/// Return whether this context was explicitly dispatched through an owned
/// native hook command for `tool`.
pub fn is_opted_in(ctx: &HcomContext, tool: NativeTool) -> bool {
    ctx.raw_env
        .get(NATIVE_PARTICIPATION_ENV)
        .is_some_and(|value| value == tool.marker())
}

/// Resolve an already enrolled native owner.  This is deliberately a lookup:
/// it never creates a row, adopts a pending row, or scans transcript markers.
pub fn resolve_bound_owner(db: &HcomDb, session_id: &str, tool: NativeTool) -> Option<InstanceRow> {
    let name = db.get_session_binding(session_id).ok().flatten()?;
    let row = db.get_instance_full(&name).ok().flatten()?;
    (row.tool == tool.as_str()).then_some(row)
}

/// Persist the explicit Claude native-session handshake from SessionStart.
///
/// Claude sources `CLAUDE_ENV_FILE` in later shell commands.  We append only
/// the two authoritative exports, under an exclusive file lock, and make the
/// operation idempotent.  A missing env file is an error: silently continuing
/// would make `hcom start` fall back to an unrelated adhoc identity.
pub fn persist_claude_session_opt_in(ctx: &HcomContext, session_id: &str) -> Result<()> {
    if !is_opted_in(ctx, NativeTool::Claude) {
        bail!("native Claude SessionStart lacks the hcom opt-in marker");
    }
    let env_path = ctx
        .claude_env_file
        .as_deref()
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .context("native Claude SessionStart did not provide CLAUDE_ENV_FILE")?;
    if session_id.is_empty() {
        bail!("native Claude SessionStart did not provide a session id");
    }

    let session_line = format!(
        "export {NATIVE_CLAUDE_SESSION_ENV}={}",
        shell_single_quote(session_id)
    );
    let marker_line = format!(
        "export {NATIVE_PARTICIPATION_ENV}={}",
        shell_single_quote(NATIVE_CLAUDE_VALUE)
    );

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&env_path)
        .with_context(|| format!("cannot open Claude env file {}", env_path.display()))?;
    crate::sys::fs::lock_exclusive(&file)
        .with_context(|| format!("cannot lock Claude env file {}", env_path.display()))?;

    use std::io::{Read, Seek, SeekFrom, Write};
    file.seek(SeekFrom::Start(0))?;
    let mut existing = String::new();
    file.read_to_string(&mut existing)?;
    file.seek(SeekFrom::End(0))?;
    let mut append = String::new();
    if !existing.lines().any(|line| line.trim() == marker_line) {
        append.push_str(&marker_line);
        append.push('\n');
    }
    if !existing.lines().any(|line| line.trim() == session_line) {
        append.push_str(&session_line);
        append.push('\n');
    }
    if !append.is_empty() {
        file.write_all(append.as_bytes())?;
        file.sync_data()?;
    }
    Ok(())
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

const CLAUDE_NATIVE_HOOKS: &[(&str, &str, Option<u64>)] = &[
    ("SessionStart", "SessionStart", None),
    ("UserPromptSubmit", "UserPromptSubmit", None),
    ("PreToolUse", "PreToolUse", None),
    ("PostToolUse", "PostToolUse", Some(86_400)),
    ("PostToolUseFailure", "PostToolUseFailure", None),
    ("Stop", "Stop", Some(86_400)),
    ("StopFailure", "StopFailure", None),
    ("PermissionRequest", "PermissionRequest", None),
    ("PermissionDenied", "PermissionDenied", None),
    ("SubagentStart", "SubagentStart", None),
    ("SubagentStop", "SubagentStop", Some(86_400)),
    ("Notification", "Notification", None),
    ("SessionEnd", "SessionEnd", None),
];

const CODEX_NATIVE_HOOKS: &[(&str, &str, Option<&str>)] = &[
    (
        "SessionStart",
        "SessionStart",
        Some("startup|resume|clear|fork"),
    ),
    ("UserPromptSubmit", "UserPromptSubmit", None),
    (
        "PreToolUse",
        "PreToolUse",
        Some("Bash|apply_patch|spawn_agent"),
    ),
    ("PostToolUse", "PostToolUse", None),
    ("Stop", "Stop", None),
    ("Interrupt", "Interrupt", None),
];

/// Install an explicit native hook set at the caller-selected profile paths.
pub fn install_at(tool: NativeTool, paths: &NativeConfigPaths) -> Result<NativeHookStatus> {
    match tool {
        NativeTool::Claude => install_claude_at(paths),
        NativeTool::Codex => install_codex_at(paths),
    }
}

/// Remove only the exact native declarations owned by hcom.
pub fn remove_at(tool: NativeTool, paths: &NativeConfigPaths) -> Result<NativeHookStatus> {
    match tool {
        NativeTool::Claude => remove_claude_at(paths),
        NativeTool::Codex => remove_codex_at(paths),
    }
}

/// Inspect native declarations without invoking a provider or mutating a
/// profile.  Codex trust is read from config.toml; fresh hashes are checked
/// during install through the upstream hooks/list machinery.
pub fn status_at(tool: NativeTool, paths: &NativeConfigPaths) -> Result<NativeHookStatus> {
    match tool {
        NativeTool::Claude => status_claude_at(paths),
        NativeTool::Codex => status_codex_at(paths),
    }
}

pub fn install(tool: NativeTool) -> Result<NativeHookStatus> {
    install_at(tool, &NativeConfigPaths::from_environment())
}

pub fn remove(tool: NativeTool) -> Result<NativeHookStatus> {
    remove_at(tool, &NativeConfigPaths::from_environment())
}

pub fn status(tool: NativeTool) -> Result<NativeHookStatus> {
    status_at(tool, &NativeConfigPaths::from_environment())
}

fn read_json_object(path: &Path) -> Result<(Value, bool)> {
    let source = match std::fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((serde_json::json!({}), false));
        }
        Err(error) => return Err(error).with_context(|| format!("cannot read {}", path.display())),
    };
    let value: Value = serde_json::from_str(&source)
        .with_context(|| format!("invalid JSON in {}", path.display()))?;
    if !value.is_object() {
        bail!("{} must contain a JSON object", path.display());
    }
    Ok((value, true))
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    paths::atomic_write_io(path, &serde_json::to_string_pretty(value)?)
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(())
}

fn native_command_matches(command: &str, tool: NativeTool, event: &str) -> bool {
    let words = shell_words::split(command).unwrap_or_else(|_| {
        command
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>()
    });
    let expected = [
        "hooks",
        "native",
        NATIVE_RUN_SUBCOMMAND,
        tool.as_str(),
        event,
    ];
    words
        .windows(expected.len())
        .enumerate()
        .any(|(start, window)| {
            let prefix = &words[..start];
            window
                .iter()
                .map(String::as_str)
                .eq(expected.iter().copied())
                && is_hcom_prefix(prefix)
        })
}

fn is_hcom_prefix(prefix: &[String]) -> bool {
    let is_hcom = |word: &str| {
        let name = word.rsplit(['/', '\\']).next().unwrap_or(word);
        name.strip_suffix(".exe").unwrap_or(name) == "hcom"
    };
    prefix.last().is_some_and(|word| is_hcom(word))
        && (prefix.len() == 1 || (prefix.len() == 2 && prefix[0] == "uvx"))
}

fn native_command_value(hook: &Value, tool: NativeTool, event: &str) -> bool {
    hook.get("command")
        .and_then(Value::as_str)
        .is_some_and(|command| native_command_matches(command, tool, event))
}

fn hook_group(event: &str, tool: NativeTool) -> Value {
    let matcher = match tool {
        NativeTool::Claude => CLAUDE_NATIVE_HOOKS
            .iter()
            .find(|(name, _, _)| *name == event)
            .and_then(|(_, _, _)| {
                (event == "PreToolUse").then_some("Bash|PowerShell|Agent|Task|Write|Edit")
            }),
        NativeTool::Codex => CODEX_NATIVE_HOOKS
            .iter()
            .find(|(name, _, _)| *name == event)
            .and_then(|(_, _, matcher)| *matcher),
    };
    let mut hook = serde_json::json!({
        "type": "command",
        "command": native_hook_command(tool, event),
    });
    if let Some(timeout) = match tool {
        NativeTool::Claude => CLAUDE_NATIVE_HOOKS
            .iter()
            .find(|(name, _, _)| *name == event)
            .and_then(|(_, _, timeout)| *timeout),
        NativeTool::Codex => None,
    } {
        hook["timeout"] = serde_json::json!(timeout);
    }
    let mut group = serde_json::json!({"hooks": [hook]});
    if let Some(matcher) = matcher {
        group["matcher"] = Value::String(matcher.to_string());
    }
    group
}

fn hook_events(tool: NativeTool) -> impl Iterator<Item = &'static str> {
    match tool {
        NativeTool::Claude => CLAUDE_NATIVE_HOOKS
            .iter()
            .map(|(event, _, _)| *event)
            .collect::<Vec<_>>()
            .into_iter(),
        NativeTool::Codex => CODEX_NATIVE_HOOKS
            .iter()
            .map(|(event, _, _)| *event)
            .collect::<Vec<_>>()
            .into_iter(),
    }
}

fn install_claude_at(paths: &NativeConfigPaths) -> Result<NativeHookStatus> {
    let path = &paths.claude_settings;
    let (mut settings, existed) = read_json_object(path)?;
    let original = settings.clone();
    let hooks = settings
        .as_object_mut()
        .expect("validated JSON object")
        .entry("hooks")
        .or_insert_with(|| serde_json::json!({}));
    let hooks = hooks
        .as_object_mut()
        .context("Claude settings hooks must be an object")?;

    for event in hook_events(NativeTool::Claude) {
        let entries = hooks
            .entry(event.to_string())
            .or_insert_with(|| serde_json::json!([]));
        let entries = entries
            .as_array_mut()
            .with_context(|| format!("Claude settings hooks.{event} must be an array"))?;
        if !entries.iter().any(|group| {
            group
                .get("hooks")
                .and_then(Value::as_array)
                .is_some_and(|arr| {
                    arr.iter()
                        .any(|hook| native_command_value(hook, NativeTool::Claude, event))
                })
        }) {
            entries.push(hook_group(event, NativeTool::Claude));
        }
    }

    if settings != original || !existed {
        write_json(path, &settings)?;
    }
    status_claude_value(path, settings)
}

fn remove_native_from_settings(settings: &mut Value, tool: NativeTool) -> usize {
    let Some(hooks) = settings.get_mut("hooks").and_then(Value::as_object_mut) else {
        return 0;
    };
    let mut removed = 0;
    for event in hook_events(tool) {
        let Some(groups) = hooks.get_mut(event).and_then(Value::as_array_mut) else {
            continue;
        };
        for group in groups.iter_mut() {
            if let Some(entries) = group.get_mut("hooks").and_then(Value::as_array_mut) {
                let before = entries.len();
                entries.retain(|hook| !native_command_value(hook, tool, event));
                removed += before.saturating_sub(entries.len());
            }
        }
        groups.retain(|group| {
            group
                .get("hooks")
                .and_then(Value::as_array)
                .is_some_and(|entries| !entries.is_empty())
        });
        if groups.is_empty() {
            hooks.remove(event);
        }
    }
    if hooks.is_empty() {
        settings
            .as_object_mut()
            .expect("validated JSON object")
            .remove("hooks");
    }
    removed
}

fn remove_claude_at(paths: &NativeConfigPaths) -> Result<NativeHookStatus> {
    let path = &paths.claude_settings;
    let (mut settings, existed) = read_json_object(path)?;
    if !existed {
        return status_claude_value(path, settings);
    }
    let removed = remove_native_from_settings(&mut settings, NativeTool::Claude);
    if removed > 0 {
        write_json(path, &settings)?;
    }
    status_claude_value(path, settings)
}

fn status_claude_at(paths: &NativeConfigPaths) -> Result<NativeHookStatus> {
    let (settings, _) = read_json_object(&paths.claude_settings)?;
    status_claude_value(&paths.claude_settings, settings)
}

fn status_claude_value(path: &Path, settings: Value) -> Result<NativeHookStatus> {
    let owned_hooks = hook_events(NativeTool::Claude)
        .map(|event| {
            settings
                .pointer(&format!("/hooks/{event}"))
                .and_then(Value::as_array)
                .map(|groups| {
                    groups
                        .iter()
                        .flat_map(|group| {
                            group
                                .get("hooks")
                                .and_then(Value::as_array)
                                .into_iter()
                                .flatten()
                        })
                        .filter(|hook| native_command_value(hook, NativeTool::Claude, event))
                        .count()
                })
                .unwrap_or(0)
        })
        .sum();
    let expected_hooks = CLAUDE_NATIVE_HOOKS.len();
    Ok(NativeHookStatus {
        tool: NativeTool::Claude,
        path: path.to_path_buf(),
        installed: owned_hooks == expected_hooks,
        owned_hooks,
        expected_hooks,
        trust_configured: true,
    })
}

fn ensure_hooks_feature(config: &mut DocumentMut) -> Result<()> {
    if config.get("features").is_none() {
        let mut features = Table::new();
        features["hooks"] = value(true);
        config["features"] = Item::Table(features);
    } else {
        let features = config
            .get_mut("features")
            .and_then(Item::as_table_like_mut)
            .context("Codex config features must be a table")?;
        features.insert("hooks", value(true));
    }
    Ok(())
}

fn read_codex_config(path: &Path) -> Result<(DocumentMut, bool)> {
    match std::fs::read_to_string(path) {
        Ok(source) => Ok((
            source
                .parse()
                .with_context(|| format!("invalid TOML in {}", path.display()))?,
            true,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok((DocumentMut::new(), false))
        }
        Err(error) => Err(error).with_context(|| format!("cannot read {}", path.display())),
    }
}

fn write_toml(path: &Path, document: &DocumentMut) -> Result<()> {
    paths::atomic_write_io(path, &document.to_string())
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(())
}

fn exact_codex_hook_count(hooks: &Value, event: &str) -> usize {
    hooks
        .pointer(&format!("/hooks/{event}"))
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .flat_map(|group| {
                    group
                        .get("hooks")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                })
                .filter(|hook| native_command_value(hook, NativeTool::Codex, event))
                .count()
        })
        .unwrap_or(0)
}

fn is_native_codex_handler(handler: &Value) -> bool {
    handler
        .get("command")
        .and_then(Value::as_str)
        .is_some_and(|command| {
            CODEX_NATIVE_HOOKS
                .iter()
                .any(|(event, _, _)| native_command_matches(command, NativeTool::Codex, event))
        })
}

fn add_codex_hook_declarations(hooks: &mut Value) -> Result<()> {
    let root = hooks.as_object_mut().expect("validated JSON object");
    let events = root.entry("hooks").or_insert_with(|| serde_json::json!({}));
    let events = events
        .as_object_mut()
        .context("Codex hooks.json hooks must be an object")?;
    for event in hook_events(NativeTool::Codex) {
        let groups = events
            .entry(event.to_string())
            .or_insert_with(|| serde_json::json!([]));
        let groups = groups
            .as_array_mut()
            .with_context(|| format!("Codex hooks.{event} must be an array"))?;
        if !groups.iter().any(|group| {
            group
                .get("hooks")
                .and_then(Value::as_array)
                .is_some_and(|entries| {
                    entries
                        .iter()
                        .any(|hook| native_command_value(hook, NativeTool::Codex, event))
                })
        }) {
            groups.push(hook_group(event, NativeTool::Codex));
        }
    }
    Ok(())
}

fn remove_codex_declarations(hooks: &mut Value) -> HashSet<crate::hooks::codex::HandlerPosition> {
    let before = crate::hooks::codex::handler_positions(hooks, is_native_codex_handler);
    let Some(events) = hooks.get_mut("hooks").and_then(Value::as_object_mut) else {
        return before.into_iter().collect();
    };
    for event in hook_events(NativeTool::Codex) {
        let Some(groups) = events.get_mut(event).and_then(Value::as_array_mut) else {
            continue;
        };
        for group in groups.iter_mut() {
            if let Some(entries) = group.get_mut("hooks").and_then(Value::as_array_mut) {
                entries.retain(|handler| {
                    !handler
                        .get("command")
                        .and_then(Value::as_str)
                        .is_some_and(|command| {
                            native_command_matches(command, NativeTool::Codex, event)
                        })
                });
            }
        }
        groups.retain(|group| {
            group
                .get("hooks")
                .and_then(Value::as_array)
                .is_some_and(|entries| !entries.is_empty())
        });
        if groups.is_empty() {
            events.remove(event);
        }
    }
    if events.is_empty() {
        hooks
            .as_object_mut()
            .expect("validated JSON object")
            .remove("hooks");
    }
    before.into_iter().collect()
}

fn codex_trust_for_native(
    cwd: &Path,
    codex_home: &Path,
    config: &mut DocumentMut,
) -> Result<HashMap<String, String>> {
    let entries = crate::hooks::codex::fetch_codex_hook_list_for_native(cwd, codex_home)
        .map_err(anyhow::Error::msg)?;
    let expected: HashSet<String> = CODEX_NATIVE_HOOKS
        .iter()
        .map(|(_, event, _)| native_hook_command(NativeTool::Codex, event))
        .collect();
    let mut trust = HashMap::new();
    for entry in entries {
        let (Some(key), Some(command), Some(hash)) = (entry.key, entry.command, entry.current_hash)
        else {
            continue;
        };
        if expected.contains(&command) && trust.insert(key, hash).is_some() {
            bail!("Codex hooks/list returned duplicate native hook trust entries");
        }
    }
    if trust.len() != expected.len() {
        bail!(
            "Codex hooks/list found {} of {} native hook trust entries",
            trust.len(),
            expected.len()
        );
    }
    let hooks = config
        .entry("hooks")
        .or_insert_with(|| Item::Table(Table::new()));
    let hooks = hooks
        .as_table_like_mut()
        .context("Codex config hooks must be a table")?;
    let state = hooks
        .entry("state")
        .or_insert_with(|| Item::Table(Table::new()));
    let state = state
        .as_table_like_mut()
        .context("Codex config hooks.state must be a table")?;
    for (key, hash) in &trust {
        let mut item = Table::new();
        item["trusted_hash"] = value(hash.clone());
        state.insert(key, Item::Table(item));
    }
    Ok(trust)
}

fn install_codex_at(paths: &NativeConfigPaths) -> Result<NativeHookStatus> {
    let hooks_path = paths.codex_home.join("hooks.json");
    let config_path = paths.codex_home.join("config.toml");
    let (mut hooks, hooks_existed) = read_json_object(&hooks_path)?;
    let (mut config, config_existed) = read_codex_config(&config_path)?;
    let original_hooks = hooks.clone();
    let original_config = config.to_string();
    add_codex_hook_declarations(&mut hooks)?;
    ensure_hooks_feature(&mut config)?;

    // Write both declarations only after parsing all user data.  If the
    // provider's current hooks/list cannot produce a fresh hash for every
    // exact command, restore both files and leave the profile untouched.
    write_json(&hooks_path, &hooks)?;
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    if let Err(error) = codex_trust_for_native(&cwd, &paths.codex_home, &mut config) {
        restore_file(&hooks_path, hooks_existed, &original_hooks)?;
        restore_text_file(&config_path, config_existed, &original_config)?;
        return Err(error);
    }
    if let Err(error) = write_toml(&config_path, &config) {
        restore_file(&hooks_path, hooks_existed, &original_hooks)?;
        restore_text_file(&config_path, config_existed, &original_config)?;
        return Err(error);
    }
    status_codex_at(paths)
}

fn restore_file(path: &Path, existed: bool, value: &Value) -> Result<()> {
    if existed {
        write_json(path, value)
    } else {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| format!("cannot remove {}", path.display())),
        }
    }
}

fn restore_text_file(path: &Path, existed: bool, source: &str) -> Result<()> {
    if existed {
        paths::atomic_write_io(path, source)
            .with_context(|| format!("cannot restore {}", path.display()))?;
    } else {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("cannot remove {}", path.display()));
            }
        }
    }
    Ok(())
}

fn status_codex_at(paths: &NativeConfigPaths) -> Result<NativeHookStatus> {
    let hooks_path = paths.codex_home.join("hooks.json");
    let config_path = paths.codex_home.join("config.toml");
    let (hooks, _) = read_json_object(&hooks_path)?;
    let owned_hooks = hook_events(NativeTool::Codex)
        .map(|event| exact_codex_hook_count(&hooks, event))
        .sum();
    let (config, _) = read_codex_config(&config_path)?;
    let native_positions: HashSet<_> =
        crate::hooks::codex::handler_positions(&hooks, is_native_codex_handler)
            .into_iter()
            .collect();
    let trust_configured = !native_positions.is_empty()
        && config
            .get("hooks")
            .and_then(Item::as_table_like)
            .and_then(|hooks| hooks.get("state"))
            .and_then(Item::as_table_like)
            .is_some_and(|state| {
                native_positions.iter().all(|position| {
                    state.iter().any(|(key, item)| {
                        crate::hooks::codex::hcom_hooks_json_state_position(key, &hooks_path)
                            .is_some_and(|candidate| &candidate == position)
                            && item.get("trusted_hash").and_then(Item::as_str).is_some()
                    })
                })
            });
    let expected_hooks = CODEX_NATIVE_HOOKS.len();
    Ok(NativeHookStatus {
        tool: NativeTool::Codex,
        path: hooks_path,
        installed: owned_hooks == expected_hooks,
        owned_hooks,
        expected_hooks,
        trust_configured,
    })
}

fn remove_codex_at(paths: &NativeConfigPaths) -> Result<NativeHookStatus> {
    let hooks_path = paths.codex_home.join("hooks.json");
    let config_path = paths.codex_home.join("config.toml");
    let (mut hooks, hooks_existed) = read_json_object(&hooks_path)?;
    if !hooks_existed {
        return status_codex_at(paths);
    }
    let original_hooks = hooks.clone();
    let native_positions = remove_codex_declarations(&mut hooks);
    if native_positions.is_empty() {
        return status_codex_at(paths);
    }
    let (mut config, config_existed) = read_codex_config(&config_path)?;
    let original_config = config.to_string();
    let before_user = crate::hooks::codex::handler_positions(&original_hooks, |handler| {
        !is_native_codex_handler(handler)
    });
    let after_user = crate::hooks::codex::handler_positions(&hooks, |_| true);
    let moved: HashMap<_, _> = before_user
        .into_iter()
        .zip(after_user)
        .filter(|(old, new)| old != new)
        .collect();
    if let Some(state) = config
        .get_mut("hooks")
        .and_then(Item::as_table_like_mut)
        .and_then(|hooks| hooks.get_mut("state"))
        .and_then(Item::as_table_like_mut)
    {
        let mut remove = Vec::new();
        let mut rename = Vec::new();
        for (key, _) in state.iter() {
            let Some(position) =
                crate::hooks::codex::hcom_hooks_json_state_position(key, &hooks_path)
            else {
                continue;
            };
            if native_positions.contains(&position) {
                remove.push(key.to_string());
            } else if let Some(new) = moved.get(&position) {
                rename.push((
                    key.to_string(),
                    crate::hooks::codex::rekey_state_position(key, new),
                ));
            }
        }
        for key in remove {
            state.remove(&key);
        }
        let moved_items: Vec<_> = rename
            .iter()
            .filter_map(|(old, new)| state.remove(old).map(|item| (new.clone(), item)))
            .collect();
        for (key, item) in moved_items {
            state.insert(&key, item);
        }
    }
    if let Err(error) =
        write_json(&hooks_path, &hooks).and_then(|()| write_toml(&config_path, &config))
    {
        restore_file(&hooks_path, hooks_existed, &original_hooks)?;
        restore_text_file(&config_path, config_existed, &original_config)?;
        return Err(error);
    }
    status_codex_at(paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn fixture_paths() -> (tempfile::TempDir, NativeConfigPaths) {
        let dir = tempfile::tempdir().unwrap();
        let paths = NativeConfigPaths {
            claude_settings: dir.path().join("claude/settings.json"),
            codex_home: dir.path().join("codex"),
        };
        (dir, paths)
    }

    #[test]
    fn claude_fixture_merge_is_idempotent_and_preserves_foreign_hooks() {
        let (_dir, paths) = fixture_paths();
        std::fs::create_dir_all(paths.claude_settings.parent().unwrap()).unwrap();
        std::fs::write(
            &paths.claude_settings,
            serde_json::to_string(&serde_json::json!({
                "custom": {"keep": true},
                "hooks": {"SessionStart": [{"hooks": [{"type": "command", "command": "foreign"}]}]}
            }))
            .unwrap(),
        )
        .unwrap();
        let (mut settings, _) = read_json_object(&paths.claude_settings).unwrap();
        let hooks = settings["hooks"].as_object_mut().unwrap();
        let entries = hooks["SessionStart"].as_array_mut().unwrap();
        entries.push(hook_group("SessionStart", NativeTool::Claude));
        let before = settings.clone();
        let hooks = settings["hooks"].as_object_mut().unwrap();
        let session = hooks["SessionStart"].as_array_mut().unwrap();
        assert_eq!(session.len(), 2);
        assert!(native_command_value(
            &session[1]["hooks"][0],
            NativeTool::Claude,
            "SessionStart"
        ));
        assert_eq!(before["custom"], settings["custom"]);
    }

    #[test]
    fn claude_install_and_remove_touch_only_owned_declarations() {
        let (_dir, paths) = fixture_paths();
        std::fs::create_dir_all(paths.claude_settings.parent().unwrap()).unwrap();
        std::fs::write(
            &paths.claude_settings,
            serde_json::json!({
                "custom": {"keep": true},
                "hooks": {"SessionStart": [{"hooks": [{"type": "command", "command": "foreign"}]}]}
            })
            .to_string(),
        )
        .unwrap();

        let installed = install_at(NativeTool::Claude, &paths).unwrap();
        assert!(installed.installed);
        assert_eq!(installed.owned_hooks, installed.expected_hooks);
        let once = std::fs::read_to_string(&paths.claude_settings).unwrap();
        let again = install_at(NativeTool::Claude, &paths).unwrap();
        assert!(again.installed);
        assert_eq!(
            once,
            std::fs::read_to_string(&paths.claude_settings).unwrap()
        );

        let removed = remove_at(NativeTool::Claude, &paths).unwrap();
        assert!(!removed.installed);
        let settings: Value =
            serde_json::from_str(&std::fs::read_to_string(&paths.claude_settings).unwrap())
                .unwrap();
        assert_eq!(settings["custom"]["keep"], true);
        assert_eq!(
            settings["hooks"]["SessionStart"][0]["hooks"][0]["command"],
            "foreign"
        );
    }

    #[test]
    fn native_command_matching_does_not_claim_foreign_commands() {
        let command = native_hook_command(NativeTool::Codex, "SessionStart");
        assert!(native_command_matches(
            &command,
            NativeTool::Codex,
            "SessionStart"
        ));
        assert!(!native_command_matches(
            "python /tmp/hcom-wrapper hooks native run codex SessionStart",
            NativeTool::Codex,
            "SessionStart"
        ));
        assert!(!native_command_matches(
            "hcom hooks native run codex Stop",
            NativeTool::Codex,
            "SessionStart"
        ));
    }

    #[test]
    fn claude_session_handshake_quotes_and_deduplicates_lines() {
        let (_dir, paths) = fixture_paths();
        let env_file = paths.claude_settings.parent().unwrap().join("env");
        std::fs::create_dir_all(env_file.parent().unwrap()).unwrap();
        std::fs::write(&env_file, "export OTHER=keep\n").unwrap();
        let mut env = HashMap::new();
        env.insert(
            "CLAUDE_ENV_FILE".to_string(),
            env_file.to_string_lossy().to_string(),
        );
        env.insert(
            NATIVE_PARTICIPATION_ENV.to_string(),
            NATIVE_CLAUDE_VALUE.to_string(),
        );
        let ctx = HcomContext::from_env(&env, PathBuf::from("/tmp"));
        persist_claude_session_opt_in(&ctx, "sid'quoted").unwrap();
        persist_claude_session_opt_in(&ctx, "sid'quoted").unwrap();
        let source = std::fs::read_to_string(env_file).unwrap();
        assert_eq!(source.matches(NATIVE_PARTICIPATION_ENV).count(), 1);
        assert_eq!(source.matches(NATIVE_CLAUDE_SESSION_ENV).count(), 1);
        assert!(source.contains("sid'\\''quoted"));
    }

    struct TestEnvValue {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl TestEnvValue {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            unsafe { std::env::set_var(key, value.as_ref()) };
            Self { key, previous }
        }
    }

    impl Drop for TestEnvValue {
        fn drop(&mut self) {
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn codex_hooks_list_fixture(hooks_path: &Path) -> String {
        let hooks = CODEX_NATIVE_HOOKS
            .iter()
            .enumerate()
            .map(|(index, (event, _, _))| {
                let label = match *event {
                    "SessionStart" => "session_start",
                    "UserPromptSubmit" => "user_prompt_submit",
                    "PreToolUse" => "pre_tool_use",
                    "PostToolUse" => "post_tool_use",
                    "Stop" => "stop",
                    "Interrupt" => "interrupt",
                    _ => unreachable!(),
                };
                serde_json::json!({
                    "key": format!(
                        "{}:{}:{}:0",
                        hooks_path.display(),
                        label,
                        if *event == "SessionStart" { 1 } else { 0 },
                    ),
                    "command": native_hook_command(NativeTool::Codex, event),
                    "source": "user",
                    "currentHash": format!("native-hash-{index}"),
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({"result": {"data": [{"hooks": hooks}]}}).to_string()
    }

    #[test]
    #[serial]
    fn codex_fixture_install_remove_trusts_current_hashes_and_preserves_foreign_config() {
        let (_dir, paths) = fixture_paths();
        std::fs::create_dir_all(&paths.codex_home).unwrap();
        let hooks_path = paths.codex_home.join("hooks.json");
        let config_path = paths.codex_home.join("config.toml");
        std::fs::write(
            &hooks_path,
            serde_json::json!({
                "custom": {"keep": true},
                "hooks": {
                    "SessionStart": [{"hooks": [{"type": "command", "command": "foreign"}]}]
                }
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &config_path,
            "[features]\nother = true\n\n[hooks.state]\n\"user:key\" = { trusted_hash = \"keep\" }\n",
        )
        .unwrap();
        let _fixture = TestEnvValue::set(
            "HCOM_TEST_CODEX_HOOKS_LIST_JSON",
            codex_hooks_list_fixture(&hooks_path),
        );
        let _version = TestEnvValue::set("HCOM_TEST_CODEX_CLI_VERSION", "codex-cli 0.129.0");

        let installed = install_at(NativeTool::Codex, &paths).unwrap();
        assert!(installed.installed);
        assert!(installed.trust_configured);
        assert_eq!(installed.owned_hooks, installed.expected_hooks);
        let once = std::fs::read_to_string(&hooks_path).unwrap();
        let again = install_at(NativeTool::Codex, &paths).unwrap();
        assert!(again.installed);
        assert_eq!(
            once,
            std::fs::read_to_string(&hooks_path).unwrap(),
            "reinstall must not duplicate native handlers"
        );

        let config = std::fs::read_to_string(&config_path).unwrap();
        assert!(config.contains("other = true"));
        assert!(config.contains("trusted_hash = \"keep\""));
        assert!(config.contains("trusted_hash = \"native-hash-0\""));

        let removed = remove_at(NativeTool::Codex, &paths).unwrap();
        assert!(!removed.installed);
        assert_eq!(removed.owned_hooks, 0);
        let hooks: Value =
            serde_json::from_str(&std::fs::read_to_string(&hooks_path).unwrap()).unwrap();
        assert_eq!(hooks["custom"]["keep"], true);
        assert_eq!(
            hooks["hooks"]["SessionStart"][0]["hooks"][0]["command"],
            "foreign"
        );
        let config = std::fs::read_to_string(&config_path).unwrap();
        assert!(config.contains("other = true"));
        assert!(config.contains("trusted_hash = \"keep\""));
        assert!(!config.contains("native-hash-0"));
    }
}
