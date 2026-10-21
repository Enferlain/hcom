//! `hcom hooks` command — add/remove/status for tool hooks.
//!
//!
//! Manages hook installation across every released integration. Integrations
//! without hooks are listed too, so asking about one gets an answer rather
//! than "unknown tool".

use crate::db::HcomDb;
use crate::hooks::runtime::{self, HookMode};
use crate::shared::CommandContext;
use crate::tool::Tool;

/// Parsed arguments for `hcom hooks`.
#[derive(clap::Parser, Debug)]
#[command(name = "hooks", about = "Manage tool hooks")]
pub struct HooksArgs {
    /// Subcommand and arguments (status/add/remove [tool])
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

/// Released tools, derived from the integration registry.
///
/// Keeping this as a function (rather than a parallel constant) means adding a
/// released integration automatically exposes it to status/add/remove.
pub(crate) fn hook_tools() -> Vec<Tool> {
    crate::integration_spec::ALL
        .iter()
        .filter(|spec| spec.released)
        .map(|spec| spec.tool)
        .collect()
}

fn hook_tool_names() -> Vec<&'static str> {
    hook_tools().into_iter().map(|tool| tool.as_str()).collect()
}

fn parse_hook_tool(value: &str) -> Option<Tool> {
    value
        .parse::<Tool>()
        .ok()
        .filter(|tool| tool.spec().released)
}

fn valid_hook_options() -> String {
    let mut names = hook_tool_names();
    names.push("all");
    names.join(", ")
}

/// Refresh permission state after `auto_approve` changes.
///
/// Per-run tools sync their permission-only files unconditionally (they have no
/// install to gate on). Persistent tools are skipped when their hooks aren't
/// installed, so changing one preference does not install new integrations as
/// a side effect.
pub(crate) fn refresh_installed_hook_permissions(enabled: bool) -> Vec<(&'static str, String)> {
    let mut failures = Vec::new();
    for tool in hook_tools() {
        if let Some(adapter) = runtime::adapter(tool) {
            if let Some(ensure_permissions) = adapter.ensure_permissions
                && let Err(error) = ensure_permissions(&runtime::LaunchCtx::ambient(tool, enabled))
            {
                failures.push((tool.as_str(), format!("{error:#}")));
            }
            continue;
        }
        if !tool.verify_hooks_installed(false) {
            continue;
        }
        if let Err(error) = tool.try_setup_hooks(enabled) {
            failures.push((tool.as_str(), error));
        }
    }
    failures
}

/// Get hook installation status for each persistent-mode tool.
///
/// Per-run and hookless tools have no install to check and are excluded.
fn get_tool_status() -> Vec<(Tool, bool, String)> {
    hook_tools()
        .into_iter()
        .filter(|tool| HookMode::of(*tool) == HookMode::Persistent)
        .map(|tool| {
            (
                tool,
                tool.verify_hooks_installed(false),
                tool.hooks_settings_path(),
            )
        })
        .collect()
}

/// Show hook mode and, for persistent tools, installation status. Naming
/// Hermes confirms its install through Hermes's CLI, which the plain listing
/// skips (seconds per call).
fn cmd_hooks_status(argv: &[String]) -> i32 {
    let only = match argv.first().map(|arg| (arg, parse_hook_tool(arg))) {
        None => None,
        Some((_, Some(tool))) => Some(tool),
        Some((arg, None)) => {
            eprintln!("Error: Unknown tool: {arg}");
            eprintln!("Valid options: {}", hook_tool_names().join(", "));
            return 1;
        }
    };
    if only == Some(Tool::Hermes) {
        let code = hermes_status_checked();
        print_hermes_gateways();
        return code;
    }
    let status = get_tool_status();
    for tool in hook_tools()
        .into_iter()
        .filter(|t| only.is_none_or(|o| o == *t))
    {
        let label = tool.spec().label;
        let persistent = HookMode::Persistent.label();
        match status.iter().find(|(t, _, _)| *t == tool) {
            None => println!("{label}:  {}", HookMode::of(tool).label()),
            Some((_, true, path)) => println!("{label}:  {persistent}, installed    ({path})"),
            Some((Tool::Hermes, false, path))
                if crate::hooks::hermes::install_state()
                    == crate::hooks::hermes::InstallState::Unverified =>
            {
                println!(
                    "{label}:  {persistent}, installed, unverified    ({path})\n  \
                     Hermes config changed since hcom last checked it; \
                     `hcom hooks status hermes` checks"
                )
            }
            Some((Tool::Hermes, false, path))
                if crate::hooks::hermes::install_state()
                    == crate::hooks::hermes::InstallState::Outdated =>
            {
                println!(
                    "{label}:  {persistent}, installed, outdated    ({path})\n  \
                     `hcom hooks add hermes` updates it"
                )
            }
            Some((_, false, _)) => println!("{label}:  {persistent}, not installed"),
        }
        if tool == Tool::Hermes {
            print_hermes_gateways();
        }
    }
    0
}

/// Connected gateway chats, and gateways still running an older plugin.
fn print_hermes_gateways() {
    let Ok(db) = crate::db::HcomDb::open() else {
        return;
    };
    let chats: Vec<String> = crate::hooks::hermes_bridge::bridges(&db)
        .into_iter()
        .map(|b| match b.chat.as_str() {
            "" => format!("{} ({})", b.name, b.platform),
            chat => format!("{} ({} {chat})", b.name, b.platform),
        })
        .collect();
    if !chats.is_empty() {
        println!("  gateway chats: {}", chats.join(", "));
    }
    for note in crate::hooks::hermes::stale_gateway_notes(&db) {
        println!("  {note}");
    }
}

fn hermes_status_checked() -> i32 {
    let label = Tool::Hermes.spec().label;
    let persistent = HookMode::Persistent.label();
    let path = Tool::Hermes.hooks_settings_path();
    match crate::hooks::hermes::check_enabled() {
        Ok(Some(true)) => {
            println!("{label}:  {persistent}, installed, enabled    ({path})");
            0
        }
        Ok(Some(false)) => {
            println!(
                "{label}:  {persistent}, installed, disabled in Hermes    ({path})\n  \
                 `hcom hooks add hermes` enables it"
            );
            1
        }
        Ok(None)
            if crate::hooks::hermes::install_state()
                == crate::hooks::hermes::InstallState::Outdated =>
        {
            println!(
                "{label}:  {persistent}, installed, outdated    ({path})\n  \
                 `hcom hooks add hermes` updates it"
            );
            1
        }
        Ok(None) => {
            println!("{label}:  {persistent}, not installed");
            1
        }
        Err(e) => {
            eprintln!("{label}: cannot check through Hermes: {e:#}");
            1
        }
    }
}

fn no_hooks_message(tool: Tool, action: &str) -> String {
    format!(
        "{} uses no hooks: hcom {} gets status and messages over its own connection; nothing to {action}.",
        tool.spec().label,
        tool.as_str()
    )
}

/// Add hooks for specified tool(s).
fn cmd_hooks_add(argv: &[String]) -> i32 {
    // Get auto_approve from config
    let include_permissions = crate::config::load_config_snapshot().core.auto_approve;

    // Determine which tools to install.
    let tools: Vec<Tool> = if argv.is_empty() {
        // Auto-detect current tool; outside a supported tool, operate on all.
        parse_hook_tool(detect_current_tool())
            .map(|tool| vec![tool])
            .unwrap_or_else(hook_tools)
    } else if argv[0] == "all" {
        hook_tools()
    } else if let Some(tool) = parse_hook_tool(&argv[0]) {
        vec![tool]
    } else {
        eprintln!("Error: Unknown tool: {}", argv[0]);
        eprintln!("Valid options: {}", valid_hook_options());
        return 1;
    };

    // Install hooks — propagate error detail where available
    // Outcome: "already" = was already installed, "added" = newly added, "failed" = error
    enum AddResult {
        Already,
        Added,
        Failed(Option<String>),
    }
    let mut results: Vec<(Tool, AddResult)> = Vec::new();
    // What a running Hermes gateway needs, instead of the generic restart line.
    let mut hermes_note: Option<String> = None;
    for tool in &tools {
        if HookMode::of(*tool) == HookMode::None {
            if tools.len() == 1 {
                println!("{}", no_hooks_message(*tool, "install"));
            }
            continue;
        }
        if runtime::is_per_run(*tool) {
            // Only report on an explicit request; `add all` skips silently.
            if tools.len() == 1 {
                println!(
                    "{} loads hcom's hooks per launch (hcom {}); nothing to install.",
                    tool.spec().label,
                    tool.as_str()
                );
            }
            continue;
        }
        // Hermes installs into one profile through its own CLI and hot-loads
        // into a running gateway: only on request (`hcom hooks add hermes`), or
        // by `hcom hermes` for the profile it launches.
        if *tool == Tool::Hermes && tools.len() > 1 {
            continue;
        }
        if tool.verify_hooks_installed(include_permissions) {
            results.push((*tool, AddResult::Already));
            continue;
        }
        if *tool == Tool::Hermes {
            let outcome = match crate::hooks::hermes::add_ambient() {
                Ok(note) => {
                    hermes_note = Some(note);
                    AddResult::Added
                }
                Err(e) => AddResult::Failed(Some(format!("{e:#}"))),
            };
            results.push((*tool, outcome));
            continue;
        }
        let outcome = match tool.try_setup_hooks(include_permissions) {
            Ok(()) => AddResult::Added,
            Err(msg) if msg.is_empty() => AddResult::Failed(None),
            Err(msg) => AddResult::Failed(Some(msg)),
        };
        results.push((*tool, outcome));
    }

    // Report results
    let post_status = get_tool_status();
    let mut added_count = 0;
    let mut fail_count = 0;
    for (tool, outcome) in &results {
        let path = post_status
            .iter()
            .find(|(t, _, _)| t == tool)
            .map(|(_, _, p)| p.as_str())
            .unwrap_or("");
        let name = tool.spec().label;
        match outcome {
            AddResult::Already => println!("{name} hooks already installed  ({path})"),
            AddResult::Added => {
                println!("Added {name} hooks  ({path})");
                added_count += 1;
            }
            AddResult::Failed(Some(e)) => {
                eprintln!("Failed to add {name} hooks: {e}");
                fail_count += 1;
            }
            AddResult::Failed(None) => {
                eprintln!("Failed to add {name} hooks");
                fail_count += 1;
            }
        }
    }

    match hermes_note {
        Some(note) if !note.is_empty() => println!("\n{note}"),
        Some(_) => {}
        None if added_count > 0 => {
            println!();
            if tools.len() == 1 {
                println!("Restart {} to activate hooks.", tools[0].spec().label);
            } else {
                println!("Restart the tool(s) to activate hooks.");
            }
        }
        None => {}
    }

    if fail_count > 0 { 1 } else { 0 }
}

/// Remove hooks for specified tool(s). Called from both `hcom hooks remove` and `hcom reset hooks`.
pub fn cmd_hooks_remove(argv: &[String]) -> i32 {
    // Determine which tools to remove.
    let tools: Vec<Tool> = if argv.is_empty() || (argv.len() == 1 && argv[0] == "all") {
        hook_tools()
    } else if let Some(tool) = parse_hook_tool(&argv[0]) {
        vec![tool]
    } else {
        eprintln!("Error: Unknown tool: {}", argv[0]);
        eprintln!("Valid options: {}", valid_hook_options());
        return 1;
    };

    // Always attempt removal for all paths to clean up stale hooks at old
    // paths (e.g. before an env var override was set).
    let mut fail_count = 0;
    for tool in &tools {
        if HookMode::of(*tool) == HookMode::None {
            if tools.len() == 1 {
                println!("{}", no_hooks_message(*tool, "remove"));
            }
            continue;
        }
        let name = tool.spec().label;
        match tool.remove_hooks() {
            Ok(true) if runtime::is_per_run(*tool) => {
                println!("Removed legacy {name} hooks (hcom now loads them per launch)")
            }
            Ok(true) => println!("Removed {name} hooks"),
            // Per-run tools have no global install; only mention them when asked.
            Ok(false) if runtime::is_per_run(*tool) => {
                if tools.len() == 1 {
                    println!(
                        "{name} loads hcom's hooks per launch (hcom {}); nothing installed globally.",
                        tool.as_str()
                    );
                }
            }
            Ok(false) => println!("{name} hooks already removed"),
            Err(e) if e.is_empty() => {
                eprintln!("Failed to remove {name} hooks");
                fail_count += 1;
            }
            Err(e) => {
                eprintln!("Failed to remove {name} hooks: {e}");
                fail_count += 1;
            }
        }
    }

    if fail_count > 0 { 1 } else { 0 }
}

/// Detect current AI tool from environment.
fn detect_current_tool() -> &'static str {
    crate::shared::detect_current_tool_from_env()
}

pub fn cmd_hooks(_db: &HcomDb, args: &HooksArgs, _ctx: Option<&CommandContext>) -> i32 {
    let argv = &args.args;
    if argv.is_empty() {
        // No args = show status
        return cmd_hooks_status(&[]);
    }

    let first = argv[0].as_str();

    if first == "--help" || first == "-h" {
        crate::commands::help::print_command_help("hooks");
        return 0;
    }

    let sub_argv = argv[1..].to_vec();

    match first {
        "status" => cmd_hooks_status(&sub_argv),
        "add" | "install" => cmd_hooks_add(&sub_argv),
        "remove" | "uninstall" => cmd_hooks_remove(&sub_argv),
        _ => {
            crate::commands::help::unknown_subcommand("hooks", first, &["status", "add", "remove"])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_current_tool_default() {
        // In test env, none of the AI tool vars should be set
        // (unless running inside one, which is fine — it'll detect it)
        let tool = detect_current_tool();
        let parsed = tool
            .parse::<Tool>()
            .expect("detected tool must be canonical");
        assert!(
            parsed == Tool::Adhoc || hook_tools().contains(&parsed),
            "unexpected tool: {tool}"
        );
    }
}
