//! `hcom hooks` command — add/remove/status for tool hooks.
//!
//!
//! Manages hook installation across every released hook-bearing integration.

use crate::db::HcomDb;
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

/// Released hook-bearing tools, derived from the integration registry.
///
/// Hermes's surface is the hcom-owned backend extension (installed into
/// HERMES_HOME and enabled through Hermes config — see `hooks::hermes`),
/// so it flows through the same adapter as every other tool.
pub(crate) fn hook_tools() -> Vec<Tool> {
    crate::integration_spec::ALL
        .iter()
        .filter(|spec| spec.released && !spec.hooks.names.is_empty())
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
        .filter(|tool| tool.spec().released && !tool.hooks().is_empty())
}

fn valid_hook_options() -> String {
    let mut names = hook_tool_names();
    names.push("all");
    names.join(", ")
}

/// Refresh permission state for hook integrations that are already installed.
///
/// This is used after `auto_approve` changes. It intentionally skips tools
/// without installed hooks so changing one preference does not install new
/// integrations as a side effect — and it skips Hermes entirely: the backend
/// extension has no permission block, so an auto_approve toggle must never
/// rerun its installer.
pub(crate) fn refresh_installed_hook_permissions(enabled: bool) -> Vec<(&'static str, String)> {
    let mut failures = Vec::new();
    for tool in hook_tools() {
        if tool == Tool::Hermes {
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

/// Get hook installation status for each tool.
///
/// Routes status checks through the typed hook adapter for every registry tool.
fn get_tool_status() -> Vec<(Tool, bool, String)> {
    hook_tools()
        .into_iter()
        .map(|tool| {
            (
                tool,
                tool.verify_hooks_installed(false),
                tool.hooks_settings_path(),
            )
        })
        .collect()
}

/// Show hook installation status for all tools. `hooks status hermes`
/// prints the detailed backend-extension report instead of the one-liner.
fn cmd_hooks_status(argv: &[String]) -> i32 {
    if argv.first().is_some_and(|tool| tool == "hermes") {
        return cmd_hermes_backend_status();
    }
    let status = get_tool_status();
    for (tool, installed, path) in &status {
        if *installed {
            println!("{}:  installed    ({path})", tool.spec().label);
        } else {
            println!("{}:  not installed", tool.spec().label);
        }
    }
    0
}

/// Detailed readiness report for the hcom-owned Hermes backend extension:
/// ownership, files, config enablement (via the normal readiness
/// evaluation), and the gateway-injection grant the extension needs for
/// non-CLI sessions.
fn cmd_hermes_backend_status() -> i32 {
    use crate::hooks::hermes;
    println!("Hermes backend extension (hcom-owned):");
    match hermes::backend_ownership_pub() {
        Ok(hermes::BackendOwnership::Foreign) => {
            println!(
                "  files:      foreign — NOT hcom-managed; hcom will not touch it. Back it up, remove it, then run: hcom hooks add hermes"
            );
            return 0;
        }
        Ok(hermes::BackendOwnership::Absent) => {
            println!("  files:      missing  (run: hcom hooks add hermes)");
        }
        Ok(hermes::BackendOwnership::Ours) => {
            let current = hermes::hermes_backend_files_current();
            println!(
                "  files:      present (hcom-managed){}",
                if current {
                    String::new()
                } else {
                    ", but differ from this hcom's version (hcom hooks add hermes updates them)"
                        .to_string()
                }
            );
        }
        Err(error) => println!("  files:      unreadable ({error})"),
    }
    let readiness = hermes::evaluate_hermes_plugin_readiness();
    println!(
        "  enabled:    {}",
        if readiness.ready { "yes" } else { "no" }
    );
    if !readiness.ready {
        println!("  readiness:  {}", readiness.diagnostic);
    }
    match hermes::hermes_gateway_injection_enabled() {
        Some(true) => println!("  allow_gateway_injection: granted"),
        Some(false) => println!(
            "  allow_gateway_injection: denied — non-CLI/serve sessions cannot receive injected turns; hcom hooks add hermes grants it"
        ),
        None => println!(
            "  allow_gateway_injection: not set — non-CLI/serve sessions cannot receive injected turns; hcom hooks add hermes grants it"
        ),
    }
    0
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
    // Outcome: "already" = installed, "added" = newly added, "skipped" =
    // optional CLI absent during a bulk add, "failed" = error.
    enum AddResult {
        Already,
        Added,
        SkippedUnavailable,
        Failed(Option<String>),
    }
    let mut results: Vec<(Tool, AddResult)> = Vec::new();
    for tool in &tools {
        if *tool == Tool::Hermes
            && tools.len() > 1
            && crate::terminal::which_bin("hermes").is_none()
        {
            results.push((*tool, AddResult::SkippedUnavailable));
            continue;
        }
        // A ready Hermes backend may still be an older hcom-owned copy.
        // Explicit `hooks add hermes` is also its safe update path.
        let current = *tool != Tool::Hermes || crate::hooks::hermes::hermes_backend_files_current();
        if current && tool.verify_hooks_installed(include_permissions) {
            results.push((*tool, AddResult::Already));
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
            AddResult::SkippedUnavailable => {
                println!("Skipped {name} hooks (Hermes CLI not found on PATH)");
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

    if added_count > 0 {
        println!();
        if tools.len() == 1 {
            println!("Restart {} to activate hooks.", tools[0].spec().label);
        } else {
            println!("Restart the tool(s) to activate hooks.");
        }
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

    // Check status for messaging, but always attempt removal for all paths
    // to clean up stale hooks at old paths (e.g. before env var override was set).
    let pre_status = get_tool_status();
    let mut fail_count = 0;
    for tool in &tools {
        let was_installed = pre_status
            .iter()
            .find(|(t, _, _)| t == tool)
            .map(|(_, installed, _)| *installed)
            .unwrap_or(false);
        let name = tool.spec().label;

        let ok = match tool.remove_hooks() {
            Ok(ok) => ok,
            Err(e) => {
                eprintln!("Failed to remove {name} hooks: {e}");
                fail_count += 1;
                continue;
            }
        };
        if ok {
            if was_installed {
                println!("Removed {name} hooks");
            } else {
                println!("{name} hooks already removed");
            }
        } else {
            eprintln!("Failed to remove {name} hooks");
            fail_count += 1;
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
        let options = valid_hook_options();
        println!(
            "hcom hooks - Manage tool hooks for hcom integration\n\n\
             Hooks enable automatic message delivery and status tracking. Without hooks,\n\
             you can still use hcom in ad-hoc mode (run hcom start in any ai tool).\n\n\
             Usage:\n  \
             hcom hooks                  Show hook status for all tools\n  \
             hcom hooks status           Same as above\n  \
             hcom hooks add [tool]       Add hooks ({options})\n  \
             hcom hooks remove [tool]    Remove hooks ({options})\n\n\
             Examples:\n  \
             hcom hooks add claude       Add Claude Code hooks only\n  \
             hcom hooks add              Auto-detect tool or add all\n  \
             hcom hooks remove all       Remove all hooks\n\n\
             After adding, restart the tool to activate hooks."
        );
        return 0;
    }

    let sub_argv = argv[1..].to_vec();

    match first {
        "status" => cmd_hooks_status(&sub_argv),
        "add" | "install" => cmd_hooks_add(&sub_argv),
        "remove" | "uninstall" => cmd_hooks_remove(&sub_argv),
        _ => {
            eprintln!("Error: Unknown hooks subcommand: {first}");
            eprintln!("Usage: hcom hooks [status|add|remove] [tool]");
            1
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
            parsed == Tool::Adhoc || parsed == Tool::Hermes || hook_tools().contains(&parsed),
            "unexpected tool: {tool}"
        );
    }
}
