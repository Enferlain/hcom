//! Kimi Code CLI hook handlers and config.toml management.
//!
//! Kimi hooks are declared in `~/.kimi-code/config.toml` under `[[hooks]]` array
//! tables. Each hook receives JSON on stdin and uses exit code / stdout for
//! results (0 = allow, 2 = block).

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde_json::{Value, json};
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table};

use crate::db::{HcomDb, InstanceRow};
use crate::hooks::{HookPayload, HookResult, common};
use crate::instance_binding;
use crate::instance_lifecycle as lifecycle;
use crate::instances;
use crate::log;
use crate::paths;
use crate::shared::context::HcomContext;
use crate::shared::{ST_ACTIVE, ST_BLOCKED, ST_LISTENING};

const HOOK_TIMEOUT_SECS: i64 = 30;
const KIMI_HOOK_COMMANDS: &[(&str, &str)] = &[
    ("SessionStart", "kimi-sessionstart"),
    ("UserPromptSubmit", "kimi-userpromptsubmit"),
    ("PreToolUse", "kimi-pretooluse"),
    ("PostToolUse", "kimi-posttooluse"),
    ("PermissionRequest", "kimi-permissionrequest"),
    ("PermissionResult", "kimi-permissionresult"),
    ("Stop", "kimi-stop"),
    ("SessionEnd", "kimi-sessionend"),
    ("SubagentStart", "kimi-subagentstart"),
    ("SubagentStop", "kimi-subagentstop"),
    ("Notification", "kimi-notification"),
];

#[derive(Debug, thiserror::Error)]
pub enum SetupError {
    #[error("existing Kimi config at {} could not be read: {source}", path.display())]
    ExistingReadFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("existing Kimi config at {} is not valid TOML: {source}", path.display())]
    ExistingParseFailed {
        path: PathBuf,
        #[source]
        source: toml_edit::TomlError,
    },
    #[error("failed to create Kimi config directory {}: {source}", path.display())]
    DirCreateFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("atomic write to {} failed: {source}", path.display())]
    AtomicWriteFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("post-write Kimi hook verification failed for {}", .0.display())]
    PostWriteVerifyFailed(PathBuf),
}

// ── Config path helpers ─────────────────────────────────────────────────

/// Kimi's data root: config.toml, sessions, credentials all live here.
/// Overridden via `KIMI_CODE_HOME` (kimi does not honor any other dir variable),
/// defaulting to `~/.kimi-code`.
fn kimi_config_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("KIMI_CODE_HOME")
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    crate::runtime_env::tool_config_root().join(".kimi-code")
}

pub fn get_kimi_settings_path() -> PathBuf {
    kimi_config_dir().join("config.toml")
}

fn build_kimi_hook_command(command: &str) -> String {
    let mut parts = crate::runtime_env::get_hcom_prefix();
    parts.push(command.to_string());
    parts.join(" ")
}

fn is_hcom_kimi_command(command: &str) -> bool {
    // Recognize the canonical command for each hook suffix under the current
    // runtime prefix AND under both install prefixes ("hcom" vs "uvx hcom").
    // Without the dual-prefix match, a re-setup launched the other way would
    // duplicate every hcom hook row instead of replacing the stale ones.
    // (An earlier `format!("{}{}", prefix.trim_end(), suffix)` also dropped
    // the separating space and matched nothing at all.)
    let trimmed = command.trim();
    KIMI_HOOK_COMMANDS.iter().any(|(_, suffix)| {
        trimmed == build_kimi_hook_command(suffix)
            || ["hcom", "uvx hcom"]
                .iter()
                .any(|prefix| trimmed == format!("{prefix} {suffix}"))
    })
}

// ── TOML manipulation ───────────────────────────────────────────────────

fn read_toml_document(path: &Path) -> Result<DocumentMut, SetupError> {
    if !path.exists() {
        return Ok(DocumentMut::new());
    }
    let content =
        std::fs::read_to_string(path).map_err(|source| SetupError::ExistingReadFailed {
            path: path.to_path_buf(),
            source,
        })?;
    content
        .parse::<DocumentMut>()
        .map_err(|source| SetupError::ExistingParseFailed {
            path: path.to_path_buf(),
            source,
        })
}

fn write_toml(path: &Path, doc: &DocumentMut) -> Result<(), SetupError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| SetupError::DirCreateFailed {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let content = doc.to_string();
    paths::atomic_write_io(path, &content).map_err(|source| SetupError::AtomicWriteFailed {
        path: path.to_path_buf(),
        source,
    })
}

fn merge_hcom_hooks(doc: &mut DocumentMut) {
    let hooks_item = doc
        .entry("hooks")
        .or_insert_with(|| Item::ArrayOfTables(ArrayOfTables::new()));

    if let Item::ArrayOfTables(arr) = hooks_item {
        let mut filtered = ArrayOfTables::new();
        for i in 0..arr.len() {
            if let Some(table) = arr.get(i) {
                let keep = table
                    .get("command")
                    .and_then(|v| v.as_str())
                    .map(|cmd| !is_hcom_kimi_command(cmd))
                    .unwrap_or(true);
                if keep {
                    filtered.push(table.clone());
                }
            }
        }
        *arr = filtered;

        for (event, command_suffix) in KIMI_HOOK_COMMANDS {
            let mut table = Table::new();
            table.insert("event", toml_edit::value(*event));
            table.insert(
                "command",
                toml_edit::value(build_kimi_hook_command(command_suffix)),
            );
            table.insert("timeout", toml_edit::value(HOOK_TIMEOUT_SECS));
            arr.push(table);
        }
    }
}

fn remove_hcom_hooks(doc: &mut DocumentMut) {
    let Some(hooks_item) = doc.get_mut("hooks") else {
        return;
    };
    let Item::ArrayOfTables(arr) = hooks_item else {
        return;
    };
    let mut filtered = ArrayOfTables::new();
    for i in 0..arr.len() {
        if let Some(table) = arr.get(i) {
            let keep = table
                .get("command")
                .and_then(|v| v.as_str())
                .map(|cmd| !is_hcom_kimi_command(cmd))
                .unwrap_or(true);
            if keep {
                filtered.push(table.clone());
            }
        }
    }
    *arr = filtered;
    if arr.is_empty() {
        doc.remove("hooks");
    }
}

// ── Permission allowlist (auto-approve hcom's own commands) ──────────────
//
// Kimi gates every tool call behind a permission check. Its `[[permission.rules]]`
// are matched top-to-bottom, first match wins (config-files.md#permission). To let
// a launched agent run its own `hcom` commands unattended — without `--yolo`, which
// would also auto-approve unrelated tools — hcom prepends `decision = "allow"`
// rules for its safe self-commands ahead of any user rules.

/// Allow-rule patterns hcom installs (current hcom command prefix).
fn kimi_permission_patterns() -> Vec<String> {
    let prefix = crate::runtime_env::build_hcom_command();
    common::SAFE_HCOM_COMMANDS
        .iter()
        .map(|command| format!("Bash({prefix} {command}*)"))
        .collect()
}

/// All patterns hcom may have written (both `hcom` and `uvx hcom` prefixes), so
/// removal/re-merge can recognize and strip stale managed rules.
fn all_kimi_permission_patterns() -> Vec<String> {
    let mut patterns = Vec::new();
    for prefix in ["hcom", "uvx hcom"] {
        for command in common::SAFE_HCOM_COMMANDS {
            patterns.push(format!("Bash({prefix} {command}*)"));
        }
    }
    patterns
}

fn is_hcom_permission_pattern(pattern: &str) -> bool {
    all_kimi_permission_patterns()
        .iter()
        .any(|managed| managed == pattern)
}

/// Get a `&mut ArrayOfTables` for `[[permission.rules]]`, creating the parent
/// `[permission]` table on demand. Returns `None` if `permission`/`rules` exist
/// but are not tables of the expected shape (leave a user's odd config alone).
fn permission_rules_mut(doc: &mut DocumentMut) -> Option<&mut ArrayOfTables> {
    let permission = doc
        .entry("permission")
        .or_insert_with(|| Item::Table(Table::new()));
    let Item::Table(permission) = permission else {
        return None;
    };
    let rules = permission
        .entry("rules")
        .or_insert_with(|| Item::ArrayOfTables(ArrayOfTables::new()));
    match rules {
        Item::ArrayOfTables(arr) => Some(arr),
        _ => None,
    }
}

fn merge_hcom_permissions(doc: &mut DocumentMut) {
    let Some(arr) = permission_rules_mut(doc) else {
        return;
    };

    // Rebuild with hcom allow-rules first (first-match-wins ordering), then the
    // user's existing non-hcom rules. This makes the merge idempotent and keeps
    // hcom's allows ahead of any broad user `ask`/`deny` on `Bash`.
    let mut rebuilt = ArrayOfTables::new();
    for pattern in kimi_permission_patterns() {
        let mut table = Table::new();
        table.insert("decision", toml_edit::value("allow"));
        table.insert("pattern", toml_edit::value(pattern));
        table.insert("reason", toml_edit::value("hcom auto-approve"));
        rebuilt.push(table);
    }
    for i in 0..arr.len() {
        if let Some(table) = arr.get(i) {
            let is_managed = table
                .get("pattern")
                .and_then(|v| v.as_str())
                .map(is_hcom_permission_pattern)
                .unwrap_or(false);
            if !is_managed {
                rebuilt.push(table.clone());
            }
        }
    }
    *arr = rebuilt;
}

fn remove_hcom_permissions(doc: &mut DocumentMut) {
    let Some(Item::Table(permission)) = doc.get_mut("permission") else {
        return;
    };
    if let Some(Item::ArrayOfTables(arr)) = permission.get_mut("rules") {
        let mut filtered = ArrayOfTables::new();
        for i in 0..arr.len() {
            if let Some(table) = arr.get(i) {
                let is_managed = table
                    .get("pattern")
                    .and_then(|v| v.as_str())
                    .map(is_hcom_permission_pattern)
                    .unwrap_or(false);
                if !is_managed {
                    filtered.push(table.clone());
                }
            }
        }
        *arr = filtered;
        if arr.is_empty() {
            permission.remove("rules");
        }
    }
    if permission.is_empty() {
        doc.remove("permission");
    }
}

fn verify_permissions_at(path: &Path) -> bool {
    let Ok(doc) = read_toml_document(path) else {
        return false;
    };
    let Some(Item::Table(permission)) = doc.get("permission") else {
        return false;
    };
    let Some(Item::ArrayOfTables(arr)) = permission.get("rules") else {
        return false;
    };
    let present: Vec<&str> = (0..arr.len())
        .filter_map(|i| arr.get(i))
        .filter(|t| t.get("decision").and_then(|v| v.as_str()) == Some("allow"))
        .filter_map(|t| t.get("pattern").and_then(|v| v.as_str()))
        .collect();
    kimi_permission_patterns()
        .iter()
        .all(|expected| present.iter().any(|p| p == expected))
}

fn verify_hooks_at(path: &Path) -> bool {
    let Ok(doc) = read_toml_document(path) else {
        return false;
    };
    let Some(Item::ArrayOfTables(arr)) = doc.get("hooks") else {
        return false;
    };
    KIMI_HOOK_COMMANDS.iter().all(|(event, command_suffix)| {
        let expected_cmd = build_kimi_hook_command(command_suffix);
        (0..arr.len()).any(|i| {
            arr.get(i).is_some_and(|table| {
                table.get("event").and_then(|v| v.as_str()) == Some(*event)
                    && table.get("command").and_then(|v| v.as_str()) == Some(&expected_cmd)
                    && table.get("timeout").and_then(|v| v.as_integer()).is_some()
            })
        })
    })
}

// ── Public setup / verify / remove ──────────────────────────────────────

pub fn remove_kimi_hooks() -> bool {
    let path = get_kimi_settings_path();
    if !path.exists() {
        return true;
    }
    match read_toml_document(&path) {
        Ok(mut doc) => {
            remove_hcom_hooks(&mut doc);
            remove_hcom_permissions(&mut doc);
            write_toml(&path, &doc).is_ok()
        }
        Err(_) => false,
    }
}

pub fn try_setup_kimi_hooks(include_permissions: bool) -> Result<(), SetupError> {
    let path = get_kimi_settings_path();
    let mut doc = read_toml_document(&path)?;
    merge_hcom_hooks(&mut doc);
    if include_permissions {
        merge_hcom_permissions(&mut doc);
    } else {
        remove_hcom_permissions(&mut doc);
    }
    write_toml(&path, &doc)?;
    if !verify_hooks_at(&path) {
        return Err(SetupError::PostWriteVerifyFailed(path.clone()));
    }
    if include_permissions && !verify_permissions_at(&path) {
        return Err(SetupError::PostWriteVerifyFailed(path));
    }
    Ok(())
}

pub fn verify_kimi_hooks_installed(check_permissions: bool) -> bool {
    let path = get_kimi_settings_path();
    verify_hooks_at(&path) && (!check_permissions || verify_permissions_at(&path))
}

// ── Instance helpers ────────────────────────────────────────────────────

fn resolve_instance(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> Option<InstanceRow> {
    instance_binding::resolve_instance_from_binding(
        db,
        payload.session_id.as_deref(),
        ctx.process_id.as_deref(),
    )
}

/// Resolve a Kimi session's transcript file.
///
/// Kimi stores each session's wire log at:
/// ```text
///   $KIMI_CODE_HOME/sessions/wd_<dir>_<hash>/<session_id>/agents/main/wire.jsonl
/// ```
/// The working-directory bucket (`wd_*`) is unknown here, so scan the buckets
/// for the one containing this session. `session_id` already carries the
/// `session_` prefix (matching the on-disk directory name).
pub fn derive_kimi_transcript_path(session_id: &str) -> Option<String> {
    let base = kimi_config_dir().join("sessions");
    if !base.exists() {
        return None;
    }
    let entries = std::fs::read_dir(&base).ok()?;
    for entry in entries.flatten() {
        let wd = entry.path();
        if !wd.is_dir() {
            continue;
        }
        let candidate = wd
            .join(session_id)
            .join("agents")
            .join("main")
            .join("wire.jsonl");
        if candidate.exists() {
            return Some(candidate.to_string_lossy().to_string());
        }
    }
    None
}

fn update_position(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload, instance_name: &str) {
    let mut updates = serde_json::Map::new();
    if let Some(session_id) = payload.session_id.as_ref().filter(|s| !s.is_empty()) {
        updates.insert("session_id".into(), Value::String(session_id.clone()));
        if let Some(tp) = derive_kimi_transcript_path(session_id) {
            updates.insert("transcript_path".into(), Value::String(tp));
        }
    }
    let cwd = payload
        .raw
        .get("cwd")
        .and_then(Value::as_str)
        .unwrap_or_else(|| ctx.cwd.to_str().unwrap_or(""));
    if !cwd.is_empty() {
        updates.insert("directory".into(), Value::String(cwd.to_string()));
    }
    if !updates.is_empty() {
        instances::update_instance_position(db, instance_name, &updates);
    }
}

// ── Hook handlers ───────────────────────────────────────────────────────

fn handle_sessionstart(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> HookResult {
    if ctx.process_id.is_none() {
        return HookResult::Allow {
            additional_context: Some(format!(
                "[hcom available - run '{} start' to participate]",
                crate::runtime_env::build_hcom_command()
            )),
            system_message: None,
            delivery_ack: None,
        };
    }

    let session_id = match payload.session_id.as_deref() {
        Some(sid) => sid,
        None => return hook_noop(),
    };

    // A hook process carries the environment of the tool that spawned it;
    // refuse to run the kimi bind path when HCOM_TOOL names another tool.
    let env_tool = ctx.raw_env.get("HCOM_TOOL").cloned();
    if env_tool.as_deref().is_some_and(|tool| tool != "kimi") {
        log::log_warn(
            "hooks",
            "kimi.sessionstart.tool_env_refused",
            &format!(
                "session_id={} process_id={:?} env_tool={:?}",
                session_id, ctx.process_id, env_tool
            ),
        );
        return hook_noop();
    }

    // Tool-checked bind: one transaction verifies every existing owner (live
    // process, canonical session, stopped-session snapshot) is kimi before
    // `bind_session_to_process` mutates anything, and creates the orphan
    // identity only when nothing foreign owns the session. A mismatch rolls
    // the whole bind back — a kimi hook must not adopt another tool's
    // identity (this also replaces the unconditional orphan fallback that
    // used to live here).
    let bind = instance_binding::bind_session_to_process_for_tool(
        db,
        session_id,
        ctx.process_id.as_deref(),
        "kimi",
        true,
    );

    log::log_info(
        "hooks",
        "kimi.sessionstart.bind",
        &format!(
            "instance={:?} session_id={} process_id={:?}",
            bind, session_id, ctx.process_id,
        ),
    );

    let instance_name = match bind {
        instance_binding::ToolCheckedBind::Bound(name) => name,
        instance_binding::ToolCheckedBind::Rejected => return hook_noop(),
        instance_binding::ToolCheckedBind::Unbound => return hook_noop(),
    };

    instance_binding::capture_and_store_launch_context(db, &instance_name);
    update_position(db, ctx, payload, &instance_name);

    lifecycle::set_status(
        db,
        &instance_name,
        ST_LISTENING,
        "start",
        Default::default(),
    );

    crate::runtime_env::set_terminal_title(&instance_name);
    crate::relay::worker::ensure_worker(true);

    // NOTE: bootstrap is intentionally NOT injected here. Kimi does not add
    // SessionStart hook output to model context (it is an observation-only
    // event), so bootstrapping happens on the first UserPromptSubmit instead
    // (see handle_userpromptsubmit).
    hook_noop()
}

fn handle_userpromptsubmit(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> HookResult {
    let instance = match resolve_instance(db, ctx, payload) {
        Some(inst) => inst,
        None => return hook_noop(),
    };
    let instance_name = &instance.name;
    update_position(db, ctx, payload, instance_name);

    // Bootstrap is delivered here, NOT at SessionStart: kimi only injects
    // UserPromptSubmit hook output into model context — SessionStart output is
    // not added to context (see kimi hooks docs). Prepend it to the first
    // delivery so a launched agent learns it's on hcom.
    //
    // KNOWN LIMITATION (kimi 0.9.0): this makes the bootstrap *visible* — kimi
    // wraps UserPromptSubmit output as a `<hook_result>` block in the turn,
    // unlike codex/claude which inject it invisibly into the system prompt.
    // Kimi has no per-instance invisible channel today: no `--system-prompt`
    // flag, no `-c` config override, no `systemPrompt` config key, and AGENTS.md
    // (its only invisible system-prompt source) loads from shared paths so it
    // can't carry a per-instance name without polluting the workspace. Revisit
    // and switch to an invisible launch-time injection (mirroring codex's
    // developer_instructions path) if a future kimi release adds a system-prompt
    // append flag or env var.
    let bootstrap =
        common::inject_bootstrap_once(db, ctx, instance_name, &instance, &instance.tool);
    let pending = common::prepare_pending_messages(db, instance_name);

    let additional_context = match (&bootstrap, &pending) {
        (Some(boot), Some(p)) => Some(format!("{boot}\n\n{}", p.formatted)),
        (Some(boot), None) => Some(boot.clone()),
        (None, Some(p)) => Some(p.formatted.clone()),
        (None, None) => None,
    };

    if let Some(additional_context) = additional_context {
        return HookResult::Allow {
            additional_context: Some(additional_context),
            system_message: None,
            delivery_ack: pending.map(|p| p.ack),
        };
    }

    hook_noop()
}

fn handle_pretooluse(db: &HcomDb, _ctx: &HcomContext, payload: &HookPayload) -> HookResult {
    let instance = match resolve_instance(db, _ctx, payload) {
        Some(inst) => inst,
        None => return hook_noop(),
    };
    let instance_name = &instance.name;

    let detail =
        crate::hooks::family::extract_tool_detail("kimi", &payload.tool_name, &payload.tool_input);
    if !detail.is_empty() {
        lifecycle::set_status(db, instance_name, ST_ACTIVE, &detail, Default::default());
    }

    hook_noop()
}

fn handle_posttooluse(_db: &HcomDb, _ctx: &HcomContext, _payload: &HookPayload) -> HookResult {
    // Kimi treats PostToolUse as observation-only; hook output is ignored and
    // must not advance the delivery cursor — silently acking here caused
    // vanish. Delivery happens at UserPromptSubmit and Stop only.
    hook_noop()
}

/// PermissionRequest (observation-only): kimi fires this just before it blocks
/// waiting for the user to approve/reject a tool call. Mark the agent `blocked`
/// so `hcom list` reflects the stall and the delivery gate (require_idle) holds
/// off injecting until the user responds. `PermissionResult` clears it.
///
/// hcom's own `[[permission.rules]]` allow-rules mean an agent's `hcom send`
/// etc. are auto-approved and never reach an `ask` — so this only fires for
/// tool calls that genuinely need a human (mirrors claude's handle_permission_request).
fn handle_permissionrequest(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> HookResult {
    let instance = match resolve_instance(db, ctx, payload) {
        Some(inst) => inst,
        None => return hook_noop(),
    };
    let instance_name = &instance.name;

    let detail =
        crate::hooks::family::extract_tool_detail("kimi", &payload.tool_name, &payload.tool_input);
    lifecycle::set_status(
        db,
        instance_name,
        ST_BLOCKED,
        "approval",
        lifecycle::StatusUpdate {
            detail: &detail,
            ..Default::default()
        },
    );

    hook_noop()
}

/// PermissionResult (observation-only): kimi fires this once approval resolves.
/// In every case the agent is still mid-turn — an approved tool now runs, a
/// declined one feeds rejection back to the model — so flip out of `blocked`
/// back to `active`. The `Stop` hook sets `listening` when the turn actually
/// ends. (PermissionResult carries no tool_input, so the tool name is the best
/// available detail; mirrors claude's PostToolUse "approved:" restore.)
///
/// The payload's `decision` (`approved`/`rejected`/`cancelled`/`error`) drives a
/// decision-aware context so a declined call isn't mislabeled as approved:
/// `approved:<tool>` only when actually approved, `denied:<tool>` otherwise.
fn handle_permissionresult(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> HookResult {
    let instance = match resolve_instance(db, ctx, payload) {
        Some(inst) => inst,
        None => return hook_noop(),
    };
    let instance_name = &instance.name;

    let decision = payload.raw.get("decision").and_then(Value::as_str);
    let verb = if decision == Some("approved") {
        "approved"
    } else {
        "denied"
    };
    lifecycle::set_status(
        db,
        instance_name,
        ST_ACTIVE,
        &format!("{verb}:{}", payload.tool_name),
        Default::default(),
    );

    hook_noop()
}

fn handle_stop(db: &HcomDb, ctx: &HcomContext, payload: &HookPayload) -> HookResult {
    let instance = match resolve_instance(db, ctx, payload) {
        Some(inst) => inst,
        None => return hook_noop(),
    };
    let instance_name = &instance.name;

    if let Some(prepared) = common::prepare_pending_messages(db, instance_name) {
        // Defer the ack to dispatch: it is committed only after the Block
        // stdout write is handed to kimi and flushed. Committing here would
        // consume the messages even if stdout never reaches the model. Leave
        // the status active — listening is only set by an empty Stop.
        return HookResult::Block {
            reason: prepared.formatted,
            delivery_ack: Some(prepared.ack),
        };
    }

    // Empty Stop: the turn really ended. Signal listening and wake the
    // delivery observers so a pending `--wait`/listen sees the agent idle.
    lifecycle::set_status(db, instance_name, ST_LISTENING, "", Default::default());
    common::notify_hook_instance_with_db(db, instance_name);
    hook_noop()
}

fn handle_sessionend(db: &HcomDb, _ctx: &HcomContext, payload: &HookPayload) -> HookResult {
    let instance = match resolve_instance(db, _ctx, payload) {
        Some(inst) => inst,
        None => return hook_noop(),
    };
    let instance_name = &instance.name;

    if instance.tool != "kimi" {
        log::log_warn(
            "hooks",
            "kimi.sessionend.tool_mismatch_ignored",
            &format!(
                "instance={} tool={} payload_session_id={:?}",
                instance_name, instance.tool, payload.session_id
            ),
        );
        return hook_noop();
    }

    if let Some(incoming) = payload.session_id.as_deref()
        && let Some(primary) = instance.session_id.as_deref()
        && incoming != primary
    {
        log::log_warn(
            "hooks",
            "kimi.sessionend.historical_ignored",
            &format!(
                "instance={} incoming={} primary={}",
                instance_name, incoming, primary
            ),
        );
        return hook_noop();
    }

    // Current Kimi emits SessionEnd for a real session close (`exit` or
    // `archive`). The process is necessarily still alive while this
    // synchronous hook runs, so PID liveness cannot identify a resumable
    // transition. A new in-process session starts before the old SessionEnd
    // and is protected by the historical-session guard above.
    common::finalize_session(db, instance_name, "sessionend", None);

    hook_noop()
}

fn handle_subagentstart(_db: &HcomDb, _ctx: &HcomContext, _payload: &HookPayload) -> HookResult {
    hook_noop()
}

fn handle_subagentstop(_db: &HcomDb, _ctx: &HcomContext, _payload: &HookPayload) -> HookResult {
    hook_noop()
}

fn handle_notification(_db: &HcomDb, _ctx: &HcomContext, _payload: &HookPayload) -> HookResult {
    // Kimi treats Notification as observation-only; hook output is ignored
    // and must not advance the delivery cursor — silently acking here caused
    // vanish. Delivery happens at UserPromptSubmit and Stop only.
    hook_noop()
}

fn hook_noop() -> HookResult {
    HookResult::Allow {
        additional_context: None,
        system_message: None,
        delivery_ack: None,
    }
}

fn get_handler(hook_name: &str) -> Option<fn(&HcomDb, &HcomContext, &HookPayload) -> HookResult> {
    match hook_name {
        "kimi-sessionstart" => Some(handle_sessionstart),
        "kimi-userpromptsubmit" => Some(handle_userpromptsubmit),
        "kimi-pretooluse" => Some(handle_pretooluse),
        "kimi-posttooluse" => Some(handle_posttooluse),
        "kimi-permissionrequest" => Some(handle_permissionrequest),
        "kimi-permissionresult" => Some(handle_permissionresult),
        "kimi-stop" => Some(handle_stop),
        "kimi-sessionend" => Some(handle_sessionend),
        "kimi-subagentstart" => Some(handle_subagentstart),
        "kimi-subagentstop" => Some(handle_subagentstop),
        "kimi-notification" => Some(handle_notification),
        _ => None,
    }
}

// ── Dispatch ────────────────────────────────────────────────────────────

/// Write one JSON line to `writer` and commit `ack` only when both the write
/// and the flush succeed, so a failed stdout leaves messages unread for the
/// next delivery attempt instead of consuming them behind a lost write.
fn commit_ack_after_write<W: std::io::Write>(
    db: &HcomDb,
    writer: &mut W,
    output: &Value,
    ack: Option<&crate::hooks::DeliveryAck>,
) {
    let mut line = output.to_string();
    line.push('\n');
    // Advance the delivery cursor only after the message is handed to kimi
    // (stdout write AND flush). Without this the PTY delivery loop never
    // observes the cursor advancing and keeps re-injecting `<hcom>`.
    if writer.write_all(line.as_bytes()).is_err() || writer.flush().is_err() {
        return;
    }
    if let Some(ack) = ack {
        common::commit_delivery_ack(db, ack);
    }
}

pub fn dispatch_kimi_hook(hook_name: &str) -> i32 {
    let start = Instant::now();

    let ctx = HcomContext::from_os();

    let mut input = Vec::new();
    if let Err(e) = std::io::stdin().read_to_end(&mut input) {
        log::log_error(
            "hooks",
            "kimi.stdin_error",
            &format!("hook={} err={}", hook_name, e),
        );
        return 0;
    }

    let raw: Value = match serde_json::from_slice(&input) {
        Ok(v) => v,
        Err(e) => {
            log::log_error(
                "hooks",
                "kimi.parse_error",
                &format!("hook={} err={}", hook_name, e),
            );
            return 0;
        }
    };

    let payload = HookPayload::from_kimi(hook_name, raw);

    // Pre-gate: skip UserPromptSubmit for non-participants
    if !ctx.is_launched && hook_name == "kimi-userpromptsubmit" {
        let sid = match payload.session_id.as_deref() {
            Some(sid) => sid,
            None => return 0,
        };
        if let Ok(db) = HcomDb::open() {
            if db.get_session_binding(sid).ok().flatten().is_none() {
                return 0;
            }
        } else {
            return 0;
        }
    }

    if !crate::paths::ensure_hcom_directories() {
        return 0;
    }

    let db = match HcomDb::open() {
        Ok(db) => db,
        Err(e) => {
            log::log_error("hooks", "kimi.db.error", &format!("{}", e));
            return 0;
        }
    };

    if !common::hook_gate_check(&ctx, &db) {
        return 0;
    }

    let handler = match get_handler(hook_name) {
        Some(h) => h,
        None => {
            log::log_error(
                "hooks",
                "kimi.dispatch.unknown",
                &format!("Unknown Kimi hook: {}", hook_name),
            );
            return 0;
        }
    };

    let result = common::dispatch_with_panic_guard(
        "kimi",
        hook_name,
        HookResult::Allow {
            additional_context: None,
            system_message: None,
            delivery_ack: None,
        },
        || handler(&db, &ctx, &payload),
    );

    let exit_code = match &result {
        HookResult::Allow { .. } => 0,
        HookResult::Block { .. } => 2,
        HookResult::UpdateInput { .. } => 0,
    };

    match result {
        HookResult::Allow {
            additional_context: Some(context),
            delivery_ack,
            ..
        } => {
            let output = json!({
                "hookSpecificOutput": {
                    "message": context,
                }
            });
            commit_ack_after_write(
                &db,
                &mut std::io::stdout().lock(),
                &output,
                delivery_ack.as_ref(),
            );
        }
        HookResult::Block {
            reason,
            delivery_ack,
        } => {
            let output = json!({
                "hookSpecificOutput": {
                    "permissionDecision": "deny",
                    "permissionDecisionReason": reason,
                }
            });
            eprintln!("{}", reason);
            commit_ack_after_write(
                &db,
                &mut std::io::stdout().lock(),
                &output,
                delivery_ack.as_ref(),
            );
        }
        _ => {}
    }

    let total_ms = start.elapsed().as_secs_f64() * 1000.0;
    log::log_info(
        "hooks",
        "kimi.dispatch.timing",
        &format!(
            "hook={} exit_code={} total_ms={:.2}",
            hook_name, exit_code, total_ms
        ),
    );

    exit_code
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::test_helpers::isolated_test_env;
    use serial_test::serial;

    #[test]
    #[serial]
    fn config_path_is_project_local_unless_explicitly_overridden() {
        let (_dir, hcom_dir, _home, _guard) = isolated_test_env();
        unsafe {
            std::env::remove_var("KIMI_CODE_HOME");
        }
        assert_eq!(
            get_kimi_settings_path(),
            hcom_dir
                .parent()
                .unwrap()
                .join(".kimi-code")
                .join("config.toml")
        );

        let explicit = hcom_dir.join("explicit-kimi");
        unsafe {
            std::env::set_var("KIMI_CODE_HOME", &explicit);
        }
        assert_eq!(get_kimi_settings_path(), explicit.join("config.toml"));
    }

    fn rules(doc: &DocumentMut) -> &ArrayOfTables {
        match doc.get("permission").and_then(|p| p.get("rules")) {
            Some(Item::ArrayOfTables(arr)) => arr,
            _ => panic!("expected [[permission.rules]]"),
        }
    }

    #[test]
    fn is_hcom_kimi_command_matches_canonical_commands() {
        // Each installed hook command must be recognized as hcom-managed, else
        // re-setup duplicates them instead of replacing them.
        for (_, suffix) in KIMI_HOOK_COMMANDS {
            let cmd = build_kimi_hook_command(suffix);
            assert!(
                is_hcom_kimi_command(&cmd),
                "should recognize installed hcom hook command: {cmd}"
            );
        }
        assert!(!is_hcom_kimi_command("echo hello"));
        assert!(!is_hcom_kimi_command("hcom send @x -- hi"));
        for (_, suffix) in KIMI_HOOK_COMMANDS {
            assert!(
                is_hcom_kimi_command(&format!("hcom {suffix}")),
                "should recognize bare hcom {suffix}"
            );
            assert!(
                is_hcom_kimi_command(&format!("uvx hcom {suffix}")),
                "should recognize uvx hcom {suffix}"
            );
        }
    }

    #[test]
    fn merge_hooks_strips_stale_uvx_prefix_rows() {
        let mut doc = DocumentMut::new();
        // Simulate a prior uvx-based install left in config.toml.
        {
            let hooks = doc
                .entry("hooks")
                .or_insert_with(|| Item::ArrayOfTables(ArrayOfTables::new()));
            let Item::ArrayOfTables(arr) = hooks else {
                panic!("hooks");
            };
            for (event, suffix) in KIMI_HOOK_COMMANDS {
                let mut table = Table::new();
                table.insert("event", toml_edit::value(*event));
                table.insert("command", toml_edit::value(format!("uvx hcom {suffix}")));
                table.insert("timeout", toml_edit::value(HOOK_TIMEOUT_SECS));
                arr.push(table);
            }
        }
        merge_hcom_hooks(&mut doc);
        let Item::ArrayOfTables(arr) = doc.get("hooks").unwrap() else {
            panic!("expected [[hooks]]");
        };
        assert_eq!(arr.len(), KIMI_HOOK_COMMANDS.len());
        for i in 0..arr.len() {
            let cmd = arr
                .get(i)
                .unwrap()
                .get("command")
                .and_then(|v| v.as_str())
                .unwrap();
            assert!(
                !cmd.starts_with("uvx "),
                "stale uvx hook must be stripped: {cmd}"
            );
            assert!(
                is_hcom_kimi_command(cmd),
                "replacement must be current-prefix hcom command: {cmd}"
            );
        }
    }

    #[test]
    fn permission_events_are_registered_and_dispatchable() {
        // Both observation-only permission events must be (a) installed into
        // config.toml via KIMI_HOOK_COMMANDS and (b) routable to a handler,
        // else the blocked-on-approval status transition never fires.
        for (event, suffix) in [
            ("PermissionRequest", "kimi-permissionrequest"),
            ("PermissionResult", "kimi-permissionresult"),
        ] {
            assert!(
                KIMI_HOOK_COMMANDS.contains(&(event, suffix)),
                "{event}/{suffix} must be in KIMI_HOOK_COMMANDS"
            );
            assert!(
                get_handler(suffix).is_some(),
                "{suffix} must resolve to a handler"
            );
        }
        // The spec's routing list (Tool::from_hook_name) must agree, or the
        // installed hook command would never reach dispatch_kimi_hook.
        let spec_names = crate::tool::Tool::Kimi.hooks();
        assert!(spec_names.contains(&"kimi-permissionrequest"));
        assert!(spec_names.contains(&"kimi-permissionresult"));
    }

    #[test]
    fn merge_hooks_is_idempotent() {
        let mut doc = DocumentMut::new();
        merge_hcom_hooks(&mut doc);
        let first = match doc.get("hooks") {
            Some(Item::ArrayOfTables(arr)) => arr.len(),
            _ => panic!("expected [[hooks]]"),
        };
        merge_hcom_hooks(&mut doc);
        let second = match doc.get("hooks") {
            Some(Item::ArrayOfTables(arr)) => arr.len(),
            _ => panic!("expected [[hooks]]"),
        };
        assert_eq!(first, KIMI_HOOK_COMMANDS.len());
        assert_eq!(
            first, second,
            "re-merging hooks must not duplicate existing hcom hooks"
        );
    }

    #[test]
    fn merge_prepends_allow_rules_for_all_safe_commands() {
        let mut doc = DocumentMut::new();
        merge_hcom_permissions(&mut doc);

        let arr = rules(&doc);
        let expected = kimi_permission_patterns();
        // Our allow-rules come first, one per safe command, all decision=allow.
        assert!(arr.len() >= expected.len());
        for (i, pat) in expected.iter().enumerate() {
            let table = arr.get(i).expect("rule present");
            assert_eq!(
                table.get("decision").and_then(|v| v.as_str()),
                Some("allow")
            );
            assert_eq!(
                table.get("pattern").and_then(|v| v.as_str()),
                Some(pat.as_str())
            );
        }
        assert!(verify_permissions_at_doc(&doc));
    }

    #[test]
    fn merge_is_idempotent_and_keeps_user_rules_after_ours() {
        let mut doc: DocumentMut = r#"
[[permission.rules]]
decision = "ask"
pattern = "Bash"
"#
        .parse()
        .unwrap();

        merge_hcom_permissions(&mut doc);
        let after_first = rules(&doc).len();
        merge_hcom_permissions(&mut doc);
        let after_second = rules(&doc).len();
        assert_eq!(
            after_first, after_second,
            "re-merging must not duplicate managed rules"
        );

        // The user's broad `ask Bash` rule survives and sits AFTER our allows,
        // so first-match-wins still auto-approves hcom commands.
        let arr = rules(&doc);
        let last = arr.get(arr.len() - 1).unwrap();
        assert_eq!(last.get("decision").and_then(|v| v.as_str()), Some("ask"));
        assert_eq!(last.get("pattern").and_then(|v| v.as_str()), Some("Bash"));
        assert!(verify_permissions_at_doc(&doc));
    }

    #[test]
    fn remove_strips_only_managed_rules() {
        let mut doc: DocumentMut = r#"
[[permission.rules]]
decision = "deny"
pattern = "Bash(rm -rf*)"
"#
        .parse()
        .unwrap();

        merge_hcom_permissions(&mut doc);
        remove_hcom_permissions(&mut doc);

        // The user's deny rule remains; managed allows are gone.
        let arr = rules(&doc);
        assert_eq!(arr.len(), 1);
        let only = arr.get(0).unwrap();
        assert_eq!(only.get("decision").and_then(|v| v.as_str()), Some("deny"));
        assert!(!verify_permissions_at_doc(&doc));
    }

    #[test]
    fn remove_drops_empty_permission_table() {
        let mut doc = DocumentMut::new();
        merge_hcom_permissions(&mut doc);
        remove_hcom_permissions(&mut doc);
        assert!(
            doc.get("permission").is_none(),
            "permission table should be removed when no rules remain"
        );
    }

    // Mirror of verify_permissions_at but against an in-memory document so the
    // tests never touch the real ~/.kimi-code/config.toml.
    fn verify_permissions_at_doc(doc: &DocumentMut) -> bool {
        let Some(Item::Table(permission)) = doc.get("permission") else {
            return false;
        };
        let Some(Item::ArrayOfTables(arr)) = permission.get("rules") else {
            return false;
        };
        let present: Vec<&str> = (0..arr.len())
            .filter_map(|i| arr.get(i))
            .filter(|t| t.get("decision").and_then(|v| v.as_str()) == Some("allow"))
            .filter_map(|t| t.get("pattern").and_then(|v| v.as_str()))
            .collect();
        kimi_permission_patterns()
            .iter()
            .all(|expected| present.iter().any(|p| p == expected))
    }

    // ── Handler-level delivery/cursor regressions ──────────────────────────

    fn make_test_db() -> (tempfile::TempDir, HcomDb) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = HcomDb::open_raw(&db_path).unwrap();
        db.init_db().unwrap();
        (dir, db)
    }

    fn ctx_with_process(process_id: &str) -> crate::shared::context::HcomContext {
        let mut env = std::collections::HashMap::new();
        env.insert("HCOM_PROCESS_ID".to_string(), process_id.to_string());
        env.insert("HCOM_TOOL".to_string(), "kimi".to_string());
        crate::shared::context::HcomContext::from_env(&env, std::env::current_dir().unwrap())
    }

    fn kimi_payload(session_id: &str, hook_name: &str) -> HookPayload {
        HookPayload {
            session_id: Some(session_id.to_string()),
            transcript_path: None,
            hook_name: hook_name.to_string(),
            tool: "kimi".to_string(),
            tool_name: String::new(),
            tool_input: serde_json::Value::Null,
            tool_result: String::new(),
            notification_type: None,
            raw: serde_json::Value::Null,
        }
    }

    fn seed_bound_kimi_instance(db: &HcomDb, name: &str, session_id: &str, process_id: &str) {
        let now = chrono::Utc::now().timestamp() as f64;
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, created_at, tool, session_id, last_event_id)
                 VALUES (?1, 'active', 'docs/x.md', ?2, 'kimi', ?3, 0)",
                rusqlite::params![name, now, session_id],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO process_bindings (process_id, session_id, instance_name, updated_at)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![process_id, session_id, name, now],
            )
            .unwrap();
        db.rebind_session(session_id, name).unwrap();
    }

    fn insert_unread_message(db: &HcomDb, from: &str, text: &str) -> i64 {
        let data = serde_json::json!({
            "from": from,
            "text": text,
            "scope": "broadcast",
        })
        .to_string();
        db.conn()
            .execute(
                "INSERT INTO events (type, timestamp, instance, data) VALUES ('message', '2026-01-01T00:00:01Z', ?1, ?2)",
                rusqlite::params![from, data],
            )
            .unwrap();
        db.conn().last_insert_rowid()
    }

    fn instance_last_event_id(db: &HcomDb, name: &str) -> i64 {
        db.conn()
            .query_row(
                "SELECT last_event_id FROM instances WHERE name = ?1",
                [name],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn stopped_event_count(db: &HcomDb, name: &str) -> i64 {
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE type = 'life' AND instance = ?1
                   AND json_extract(data, '$.action') = 'stopped'",
                [name],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn assert_allow_no_delivery(result: HookResult) {
        match result {
            HookResult::Allow {
                additional_context,
                delivery_ack,
                ..
            } => {
                assert!(additional_context.is_none());
                assert!(delivery_ack.is_none());
            }
            _ => panic!("expected Allow with no delivery"),
        }
    }

    #[test]
    fn posttooluse_with_pending_does_not_ack_or_advance_cursor() {
        let (_dir, db) = make_test_db();
        seed_bound_kimi_instance(&db, "kima", "sess-post", "pid-post");
        let message_id = insert_unread_message(&db, "homo", "secret body");

        let handler = get_handler("kimi-posttooluse").expect("kimi-posttooluse handler");
        let result = handler(
            &db,
            &ctx_with_process("pid-post"),
            &kimi_payload("sess-post", "kimi-posttooluse"),
        );
        assert_allow_no_delivery(result);

        assert_eq!(instance_last_event_id(&db, "kima"), 0);
        let unread = db.get_unread_messages("kima");
        assert_eq!(unread.len(), 1);
        assert_eq!(unread[0].event_id, Some(message_id));
    }

    #[test]
    fn notification_with_pending_does_not_ack_or_advance_cursor() {
        let (_dir, db) = make_test_db();
        seed_bound_kimi_instance(&db, "kima", "sess-notify", "pid-notify");
        let message_id = insert_unread_message(&db, "homo", "secret body");

        let handler = get_handler("kimi-notification").expect("kimi-notification handler");
        let result = handler(
            &db,
            &ctx_with_process("pid-notify"),
            &kimi_payload("sess-notify", "kimi-notification"),
        );
        assert_allow_no_delivery(result);

        assert_eq!(instance_last_event_id(&db, "kima"), 0);
        let unread = db.get_unread_messages("kima");
        assert_eq!(unread.len(), 1);
        assert_eq!(unread[0].event_id, Some(message_id));
    }

    #[test]
    fn userpromptsubmit_with_pending_delivers_and_acks() {
        let (_dir, db) = make_test_db();
        seed_bound_kimi_instance(&db, "kima", "sess-prompt", "pid-prompt");
        let message_id = insert_unread_message(&db, "homo", "deliver me");

        let handler = get_handler("kimi-userpromptsubmit").expect("kimi-userpromptsubmit handler");
        let result = handler(
            &db,
            &ctx_with_process("pid-prompt"),
            &kimi_payload("sess-prompt", "kimi-userpromptsubmit"),
        );

        let ack = match result {
            HookResult::Allow {
                additional_context,
                delivery_ack,
                ..
            } => {
                let context = additional_context.expect("expected formatted delivery context");
                assert!(context.contains("deliver me"));
                delivery_ack.expect("expected deferred delivery ack")
            }
            _ => panic!("expected Allow with delivery"),
        };

        assert_eq!(instance_last_event_id(&db, "kima"), 0);

        // Simulates dispatch writing stdout then committing the deferred ack.
        common::commit_delivery_ack(&db, &ack);

        assert_eq!(instance_last_event_id(&db, "kima"), message_id);
        let inst = db.get_instance_full("kima").unwrap().unwrap();
        assert_eq!(inst.status, ST_ACTIVE);
        assert!(inst.status_context.starts_with("deliver:"));
    }

    #[test]
    fn stop_with_pending_defers_ack_stays_active() {
        let (_dir, db) = make_test_db();
        seed_bound_kimi_instance(&db, "gire", "sess-stop-pending", "pid-stop-pending");
        let message_id = insert_unread_message(&db, "homo", "stop deliver");

        let result = handle_stop(
            &db,
            &ctx_with_process("pid-stop-pending"),
            &kimi_payload("sess-stop-pending", "kimi-stop"),
        );

        let ack = match result {
            HookResult::Block {
                reason,
                delivery_ack,
            } => {
                assert!(reason.contains("stop deliver"));
                delivery_ack.expect("expected deferred delivery ack on pending stop")
            }
            _ => panic!("expected Block with deferred delivery"),
        };

        assert_eq!(instance_last_event_id(&db, "gire"), 0);
        let inst = db.get_instance_full("gire").unwrap().unwrap();
        assert_eq!(inst.status, ST_ACTIVE);
        assert_ne!(inst.status, ST_LISTENING);

        // Simulates dispatch writing the Block stdout then committing the ack.
        common::commit_delivery_ack(&db, &ack);

        assert_eq!(instance_last_event_id(&db, "gire"), message_id);
        let inst = db.get_instance_full("gire").unwrap().unwrap();
        assert_eq!(inst.status, ST_ACTIVE);
        assert_ne!(inst.status, ST_LISTENING);
    }

    #[test]
    fn stop_without_pending_sets_listening() {
        let (_dir, db) = make_test_db();
        seed_bound_kimi_instance(&db, "gire", "sess-stop", "pid-stop");

        let result = handle_stop(
            &db,
            &ctx_with_process("pid-stop"),
            &kimi_payload("sess-stop", "kimi-stop"),
        );
        assert_eq!(result.exit_code(), 0);
        let inst = db.get_instance_full("gire").unwrap().unwrap();
        assert_eq!(inst.status, ST_LISTENING);
    }

    #[test]
    fn stop_block_ack_commits_only_after_successful_write_and_flush() {
        let (_dir, db) = make_test_db();
        seed_bound_kimi_instance(&db, "gire", "sess-flush", "pid-flush");
        let message_id = insert_unread_message(&db, "homo", "flush me");
        let prepared = common::prepare_pending_messages(&db, "gire").expect("pending delivery");
        let output = json!({
            "hookSpecificOutput": {"permissionDecision": "deny"}
        });

        struct BrokenWriter;
        impl std::io::Write for BrokenWriter {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("broken pipe"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        commit_ack_after_write(&db, &mut BrokenWriter, &output, Some(&prepared.ack));
        assert_eq!(
            instance_last_event_id(&db, "gire"),
            0,
            "failed write must preserve the unread message"
        );
        assert_eq!(db.get_unread_messages("gire").len(), 1);

        struct BrokenFlush(Vec<u8>);
        impl std::io::Write for BrokenFlush {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::Error::other("flush failed"))
            }
        }
        commit_ack_after_write(
            &db,
            &mut BrokenFlush(Vec::new()),
            &output,
            Some(&prepared.ack),
        );
        assert_eq!(
            instance_last_event_id(&db, "gire"),
            0,
            "failed flush must preserve the unread message"
        );
        assert_eq!(db.get_unread_messages("gire").len(), 1);

        let mut ok_writer = std::io::Cursor::new(Vec::new());
        commit_ack_after_write(&db, &mut ok_writer, &output, Some(&prepared.ack));
        assert_eq!(instance_last_event_id(&db, "gire"), message_id);
        assert_eq!(db.get_unread_messages("gire").len(), 0);
        let written = String::from_utf8(ok_writer.into_inner()).unwrap();
        assert!(written.ends_with('\n'));
        assert!(written.contains("permissionDecision"));
    }

    // ── Lifecycle ownership regressions ────────────────────────────────────

    #[test]
    fn sessionstart_rejects_foreign_process_owner_without_mutation() {
        let (_dir, db) = make_test_db();
        let now = chrono::Utc::now().timestamp() as f64;
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, created_at, tool, session_id)
                 VALUES ('movi', 'active', 'foreign-work', ?1, 'omp', 'sess-old')",
                rusqlite::params![now],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO process_bindings (process_id, session_id, instance_name, updated_at)
                 VALUES ('pid-foreign', 'sess-old', 'movi', ?1)",
                rusqlite::params![now],
            )
            .unwrap();
        db.rebind_session("sess-old", "movi").unwrap();

        assert_allow_no_delivery(handle_sessionstart(
            &db,
            &ctx_with_process("pid-foreign"),
            &kimi_payload("sess-new", "kimi-sessionstart"),
        ));

        let owner = db.get_instance_full("movi").unwrap().unwrap();
        assert_eq!(owner.tool, "omp");
        assert_eq!(owner.session_id.as_deref(), Some("sess-old"));
        assert_eq!(owner.status, ST_ACTIVE);
        assert_eq!(owner.status_context, "foreign-work");
        assert_eq!(db.get_session_binding("sess-new").unwrap(), None);
        assert_eq!(
            db.get_process_binding("pid-foreign").unwrap().as_deref(),
            Some("movi")
        );
    }

    #[test]
    fn sessionstart_rejects_foreign_canonical_owner_without_mutation() {
        let (_dir, db) = make_test_db();
        let now = chrono::Utc::now().timestamp() as f64;
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, created_at, tool)
                 VALUES ('kima', 'listening', 'ready_observed', ?1, 'kimi')",
                rusqlite::params![now],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO process_bindings (process_id, session_id, instance_name, updated_at)
                 VALUES ('pid-kimi', '', 'kima', ?1)",
                rusqlite::params![now],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, created_at, tool, session_id)
                 VALUES ('movi', 'inactive', 'exit:closed', ?1, 'omp', 'sess-foreign')",
                rusqlite::params![now],
            )
            .unwrap();
        db.rebind_session("sess-foreign", "movi").unwrap();

        assert_allow_no_delivery(handle_sessionstart(
            &db,
            &ctx_with_process("pid-kimi"),
            &kimi_payload("sess-foreign", "kimi-sessionstart"),
        ));

        let placeholder = db.get_instance_full("kima").unwrap().unwrap();
        assert_eq!(placeholder.session_id, None);
        assert_eq!(placeholder.status, ST_LISTENING);
        let canonical = db.get_instance_full("movi").unwrap().unwrap();
        assert_eq!(canonical.tool, "omp");
        assert_eq!(canonical.session_id.as_deref(), Some("sess-foreign"));
        assert_eq!(
            db.get_session_binding("sess-foreign").unwrap().as_deref(),
            Some("movi")
        );
        assert_eq!(
            db.get_process_binding("pid-kimi").unwrap().as_deref(),
            Some("kima")
        );
    }

    #[test]
    fn sessionstart_rejects_foreign_stopped_owner_without_mutation() {
        let (_dir, db) = make_test_db();
        let now = chrono::Utc::now().timestamp() as f64;
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, created_at, tool)
                 VALUES ('kima', 'listening', 'ready_observed', ?1, 'kimi')",
                rusqlite::params![now],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO process_bindings (process_id, session_id, instance_name, updated_at)
                 VALUES ('pid-kimi', '', 'kima', ?1)",
                rusqlite::params![now],
            )
            .unwrap();
        let stopped = serde_json::json!({
            "action": "stopped",
            "by": "session",
            "reason": "exit:closed",
            "snapshot": {
                "name": "movi",
                "session_id": "sess-stopped-foreign",
                "tool": "omp"
            }
        });
        db.log_event("life", "movi", &stopped).unwrap();

        assert_allow_no_delivery(handle_sessionstart(
            &db,
            &ctx_with_process("pid-kimi"),
            &kimi_payload("sess-stopped-foreign", "kimi-sessionstart"),
        ));

        let placeholder = db.get_instance_full("kima").unwrap().unwrap();
        assert_eq!(placeholder.session_id, None);
        assert_eq!(placeholder.status, ST_LISTENING);
        assert_eq!(
            db.get_session_binding("sess-stopped-foreign").unwrap(),
            None
        );
        assert_eq!(
            db.get_process_binding("pid-kimi").unwrap().as_deref(),
            Some("kima")
        );
        assert!(db.get_instance_full("movi").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn sessionstart_accepts_kimi_canonical_owner_and_migrates_placeholder() {
        // isolated_test_env: the accepted-bind path runs
        // ensure_worker/set_terminal_title, which must not touch the real
        // ~/.hcom (relay auto-spawn) during tests.
        let _isolated = isolated_test_env();
        let (_dir, db) = make_test_db();
        let now = chrono::Utc::now().timestamp() as f64;
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, created_at, tool)
                 VALUES ('temp', 'listening', 'ready_observed', ?1, 'kimi')",
                rusqlite::params![now],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO process_bindings (process_id, session_id, instance_name, updated_at)
                 VALUES ('pid-kimi', '', 'temp', ?1)",
                rusqlite::params![now],
            )
            .unwrap();
        db.upsert_notify_endpoint("temp", "pty", 51_234).unwrap();
        seed_bound_kimi_instance(&db, "kima", "sess-kimi", "pid-old");

        assert_allow_no_delivery(handle_sessionstart(
            &db,
            &ctx_with_process("pid-kimi"),
            &kimi_payload("sess-kimi", "kimi-sessionstart"),
        ));

        assert!(db.get_instance_full("temp").unwrap().is_none());
        let canonical = db.get_instance_full("kima").unwrap().unwrap();
        assert_eq!(canonical.tool, "kimi");
        assert_eq!(canonical.session_id.as_deref(), Some("sess-kimi"));
        assert_eq!(canonical.status, ST_LISTENING);
        assert_eq!(
            db.get_process_binding("pid-kimi").unwrap().as_deref(),
            Some("kima")
        );
        assert!(db.has_notify_endpoint_kind("kima", "pty"));
    }

    #[test]
    fn sessionend_ignores_non_kimi_tool() {
        let (_dir, db) = make_test_db();
        let now = chrono::Utc::now().timestamp() as f64;
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at, tool, session_id)
                 VALUES ('movi', 'listening', ?1, 'omp', 'sess-omp')",
                rusqlite::params![now],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO process_bindings (process_id, session_id, instance_name, updated_at)
                 VALUES ('pid-omp', 'sess-omp', 'movi', ?1)",
                rusqlite::params![now],
            )
            .unwrap();
        db.rebind_session("sess-omp", "movi").unwrap();

        let result = handle_sessionend(
            &db,
            &ctx_with_process("pid-omp"),
            &kimi_payload("sess-omp", "kimi-sessionend"),
        );
        assert_eq!(result.exit_code(), 0);
        let inst = db.get_instance_full("movi").unwrap().unwrap();
        assert_eq!(inst.status, "listening");
        assert_eq!(inst.tool, "omp");
        assert_eq!(
            db.get_process_binding("pid-omp").unwrap().as_deref(),
            Some("movi")
        );
    }

    #[test]
    fn sessionend_ignores_historical_session_id() {
        let (_dir, db) = make_test_db();
        seed_bound_kimi_instance(&db, "kima", "sess-primary", "pid-hist");

        // A SessionEnd for a session the process already switched away from
        // must not finalize the current identity.
        let result = handle_sessionend(
            &db,
            &ctx_with_process("pid-hist"),
            &kimi_payload("sess-old", "kimi-sessionend"),
        );
        assert_eq!(result.exit_code(), 0);
        let inst = db.get_instance_full("kima").unwrap().unwrap();
        assert_eq!(inst.status, "active");
        assert_eq!(
            db.get_process_binding("pid-hist").unwrap().as_deref(),
            Some("kima")
        );
    }

    #[test]
    fn sessionend_finalizes_once_even_while_pid_is_alive() {
        let (_dir, db) = make_test_db();
        let now = chrono::Utc::now().timestamp() as f64;
        let pid = std::process::id() as i64;
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, created_at, tool, session_id, pid)
                 VALUES ('kima', 'active', ?1, 'kimi', 'sess-soft', ?2)",
                rusqlite::params![now, pid],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO process_bindings (process_id, session_id, instance_name, updated_at)
                 VALUES ('pid-soft', 'sess-soft', 'kima', ?1)",
                rusqlite::params![now],
            )
            .unwrap();
        db.rebind_session("sess-soft", "kima").unwrap();

        let result = handle_sessionend(
            &db,
            &ctx_with_process("pid-soft"),
            &kimi_payload("sess-soft", "kimi-sessionend"),
        );
        assert_eq!(result.exit_code(), 0);
        assert!(db.get_instance_full("kima").unwrap().is_none());
        assert_eq!(db.get_process_binding("pid-soft").unwrap(), None);
        assert_eq!(db.get_session_binding("sess-soft").unwrap(), None);
        assert_eq!(stopped_event_count(&db, "kima"), 1);
    }
}
