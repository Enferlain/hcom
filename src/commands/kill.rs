//! Kill command: `hcom kill <name(s)|all|tag:X>`
//!
//!
//! Sends SIGTERM to process groups and optionally closes terminal panes.

use std::collections::HashSet;

use crate::db::HcomDb;
use crate::hooks::common::stop_instance;
use crate::identity;
use crate::log::log_info;
use crate::paths;
use crate::pidtrack;
use crate::router::GlobalFlags;
use crate::terminal;
use anyhow::{Result, bail};

/// Parsed arguments for `hcom kill`.
#[derive(clap::Parser, Debug)]
#[command(name = "kill", about = "Kill agent processes")]
pub struct KillArgs {
    /// Targets to kill (names, "all", or "tag:X")
    pub targets: Vec<String>,
    /// Print one JSON object describing every outcome
    #[arg(long)]
    pub json: bool,
}

pub struct KillTrackedResult {
    pub target: String,
    pub pid: u32,
    pub kill_result: terminal::KillResult,
    pub pane_closed: bool,
    pub pane_retry_command: Option<String>,
    pub preset_name: String,
    pub pane_id: String,
}

const EPERM_RECHECK_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

/// One kill outcome, as reported by `--json`.
#[derive(serde::Serialize, Debug)]
struct KillOutcome {
    target: String,
    /// `agent` (tracked process), `orphan` (leftover process of a stopped
    /// agent), `stopped` (already stopped; nothing to signal), `bridge` (chat
    /// disconnected), `untracked` (no process; row stopped), or `error`.
    kind: &'static str,
    pid: Option<u32>,
    /// `sent`, `already_dead`, `permission_denied`, `already_stopped`,
    /// `disconnected`, `stopped`, or `error`.
    result: &'static str,
    pane_closed: bool,
    pane_retry_command: Option<String>,
    /// Command that resumes the agent, when it can be resumed.
    resume: Option<String>,
    error: Option<String>,
}

impl KillOutcome {
    fn new(target: &str, kind: &'static str, result: &'static str) -> Self {
        Self {
            target: target.to_string(),
            kind,
            pid: None,
            result,
            pane_closed: false,
            pane_retry_command: None,
            resume: None,
            error: None,
        }
    }

    fn signalled(
        target: &str,
        kind: &'static str,
        pid: u32,
        result: terminal::KillResult,
        pane_closed: bool,
        pane_retry_command: Option<String>,
    ) -> Self {
        Self {
            pid: Some(pid),
            pane_closed,
            pane_retry_command,
            ..Self::new(target, kind, result.as_str())
        }
    }

    fn resumable(mut self, name: &str) -> Self {
        self.resume = Some(format!("hcom r {name}"));
        self
    }
}

/// Kill output. Human mode prints each line as it happens; `--json` collects
/// the outcomes and prints them as one object when the command finishes.
/// Warnings stay on stderr in both modes.
struct KillReport {
    json: bool,
    outcomes: Vec<KillOutcome>,
}

impl KillReport {
    fn new(json: bool) -> Self {
        Self {
            json,
            outcomes: Vec::new(),
        }
    }

    fn say(&self, line: impl std::fmt::Display) {
        if !self.json {
            println!("{line}");
        }
    }

    fn record(&mut self, outcome: KillOutcome) {
        self.outcomes.push(outcome);
    }

    fn record_error(&mut self, target: &str, message: String) {
        self.record(KillOutcome {
            error: Some(message),
            ..KillOutcome::new(target, "error", "error")
        });
    }

    fn finish(self, exit: i32) -> i32 {
        if self.json {
            println!(
                "{}",
                serde_json::json!({ "ok": exit == 0, "results": self.outcomes })
            );
        }
        exit
    }
}

#[derive(Clone, Copy)]
enum PaneCleanupProcessState {
    Terminated,
    AlreadyDead,
    NotTerminated,
}

impl From<terminal::KillResult> for PaneCleanupProcessState {
    fn from(result: terminal::KillResult) -> Self {
        match result {
            terminal::KillResult::Sent => Self::Terminated,
            terminal::KillResult::AlreadyDead => Self::AlreadyDead,
            terminal::KillResult::PermissionDenied => Self::NotTerminated,
        }
    }
}

fn report_incomplete_pane_cleanup(
    process_state: PaneCleanupProcessState,
    retry_command: Option<&str>,
) -> bool {
    if matches!(process_state, PaneCleanupProcessState::NotTerminated) {
        return false;
    }
    let Some(command) = retry_command else {
        return false;
    };
    let process_message = match process_state {
        PaneCleanupProcessState::Terminated => "Process terminated",
        PaneCleanupProcessState::AlreadyDead => "Process was already terminated",
        PaneCleanupProcessState::NotTerminated => unreachable!(),
    };
    eprintln!("{process_message}, but pane remains. Retry this command with approval/escalation:");
    eprintln!("{command}");
    true
}

/// Resolve who initiated the kill
fn resolve_initiator(db: &HcomDb, explicit_name: Option<&str>) -> String {
    if let Some(name) = explicit_name {
        return name.to_string();
    }
    match identity::resolve_identity(db, None, None, None, None, None) {
        Ok(id) if matches!(id.kind, crate::shared::SenderKind::Instance) => id.name,
        _ => "cli".to_string(),
    }
}

fn normalize_kill_result(
    name: &str,
    pid: u32,
    result: terminal::KillResult,
    pane_closed: bool,
) -> terminal::KillResult {
    if !matches!(result, terminal::KillResult::PermissionDenied) {
        return result;
    }

    let pid_str = pid.to_string();
    log_info(
        "kill",
        "kill.eperm",
        &format!(
            "kill(2) returned EPERM for name={} pid={}; checking if process already exited",
            name, pid
        ),
    );
    if pane_closed {
        log_info(
            "kill",
            "kill.eperm_resolved",
            &format!(
                "name={} pid={} resolved to already_dead because terminal pane closed",
                name, pid_str
            ),
        );
        return terminal::KillResult::AlreadyDead;
    }

    std::thread::sleep(EPERM_RECHECK_DELAY);
    if !pidtrack::is_alive(pid) {
        log_info(
            "kill",
            "kill.eperm_resolved",
            &format!("name={} pid={} resolved to already_dead", name, pid_str),
        );
        terminal::KillResult::AlreadyDead
    } else {
        terminal::KillResult::PermissionDenied
    }
}

pub fn kill_tracked_instance(
    db: &HcomDb,
    name: &str,
    initiator: &str,
) -> Result<KillTrackedResult, String> {
    let inst = db
        .get_instance_full(name)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Agent '{}' not found", name))?;
    let pid = inst
        .pid
        .ok_or_else(|| format!("No tracked PID for '{}'", name))? as u32;
    let is_headless = inst.background != 0;
    let (result, pane_closed, pane_retry_command, preset_name, pane_id) =
        kill_instance(db, name, pid, &inst, is_headless, initiator);
    if result != terminal::KillResult::PermissionDenied {
        stop_instance(db, name, initiator, "killed");
    }

    Ok(KillTrackedResult {
        target: name.to_string(),
        pid,
        kill_result: result,
        pane_closed,
        pane_retry_command,
        preset_name,
        pane_id,
    })
}

fn handle_remote_kill_response(
    name: &str,
    response: &serde_json::Value,
    report: &mut KillReport,
) -> Result<i32> {
    let result = &response["result"];
    let kill_result = result["kill_result"].as_str();

    // No kill_result means an RPC-level failure (e.g. timeout, protocol error).
    if kill_result.is_none() {
        crate::relay::control::require_successful_rpc_result(response.clone())
            .map_err(anyhow::Error::msg)?;
        bail!("Remote kill returned no kill_result");
    }
    let kill_result = match kill_result.unwrap() {
        "sent" => terminal::KillResult::Sent,
        "already_dead" => terminal::KillResult::AlreadyDead,
        "permission_denied" => terminal::KillResult::PermissionDenied,
        other => bail!("Remote kill failed for {name}: unexpected kill_result {other}"),
    };

    let pid = result["pid"].as_u64().unwrap_or(0);
    let pane_closed = result["pane_closed"].as_bool().unwrap_or(false);
    let preset_name = result["preset_name"].as_str().unwrap_or("");
    let pane_id = result["pane_id"].as_str().unwrap_or("");
    let pane_retry_command = result["pane_retry_command"].as_str();
    let pane_info = pane_info_str(pane_closed, preset_name, pane_id);
    let outcome = KillOutcome::signalled(
        name,
        "agent",
        u32::try_from(pid).unwrap_or(0),
        kill_result,
        pane_closed,
        pane_retry_command.map(str::to_string),
    );

    if kill_result == terminal::KillResult::PermissionDenied {
        eprintln!(
            "Permission denied to kill process group {} for '{}'",
            pid, name
        );
        report.record(outcome);
        return Ok(1);
    }

    for line in render_remote_kill_feedback(name, kill_result.as_str(), &pane_info)? {
        report.say(line);
    }
    report.record(outcome.resumable(name));
    if pane_retry_command.is_some()
        && let Some((_, device)) = crate::relay::control::split_device_suffix(name)
    {
        eprintln!("Run the pane-close retry command on remote device {device}.");
    }
    Ok(
        if report_incomplete_pane_cleanup(kill_result.into(), pane_retry_command) {
            1
        } else {
            0
        },
    )
}

fn render_remote_kill_feedback(
    name: &str,
    kill_result: &str,
    pane_info: &str,
) -> Result<Vec<String>> {
    match kill_result {
        "sent" => Ok(vec![
            format!("Sent SIGTERM to \'{}\'{}", name, pane_info),
            format!("  To resume: hcom r {}", name),
        ]),
        "already_dead" => Ok(vec![
            format!("\'{}\' had already exited{}", name, pane_info),
            format!("  To resume: hcom r {}", name),
        ]),
        other => bail!("Remote kill failed for {name}: unexpected kill_result {other}"),
    }
}

/// Run the kill command.
pub fn run(argv: &[String], flags: &GlobalFlags) -> Result<i32> {
    // Filter out global flags already consumed by the router
    let mut filtered = vec!["kill".to_string()];
    let mut skip_next = false;
    for arg in argv {
        if skip_next {
            skip_next = false;
            continue;
        }
        match arg.as_str() {
            "kill" | "--go" => continue,
            "--name" => {
                skip_next = true;
                continue;
            }
            _ => filtered.push(arg.clone()),
        }
    }

    use clap::Parser;
    let kill_args = match KillArgs::try_parse_from(&filtered) {
        Ok(a) => a,
        Err(e) => return Ok(crate::commands::help::clap_usage_error("kill", &e)),
    };

    let targets = kill_args.targets;
    if targets.is_empty() {
        return Ok(crate::commands::help::usage_error(
            "kill",
            "hcom kill needs a target: <name>, tag:<tag>, or all",
        ));
    }
    let explicit_name = flags.name.clone();

    let db = HcomDb::open()?;
    let hcom_dir = paths::hcom_dir();
    let initiator = resolve_initiator(&db, explicit_name.as_deref());
    let mut report = KillReport::new(kill_args.json);

    // A PTY that rejoined without recovery has a live row but no pid; give it
    // back so killing that row reaches the process.
    pidtrack::claim_orphans(&db, &hcom_dir);

    // If any target is "all", just kill all
    if targets.iter().any(|t| t == "all") {
        let exit = kill_all(&db, &hcom_dir, &initiator, &mut report)?;
        return Ok(report.finish(exit));
    }

    let mut worst_exit = 0;
    for target in &targets {
        let attempt = if let Some(tag) = target.strip_prefix("tag:") {
            kill_by_tag(&db, &hcom_dir, tag, &initiator, &mut report)
        } else {
            kill_single(&db, &hcom_dir, target, &initiator, &mut report)
        };
        let exit = match attempt {
            Ok(exit) => exit,
            // JSON callers get every target's outcome, so one bad target
            // doesn't discard what already happened to the others.
            Err(e) if report.json => {
                eprintln!("Error: {e}");
                report.record_error(target, e.to_string());
                1
            }
            Err(e) => return Err(e),
        };
        if exit > worst_exit {
            worst_exit = exit;
        }
    }
    Ok(report.finish(worst_exit))
}

/// Whether a tracked orphan process belongs to `target`: a base name, a
/// `tag-name` (the tag is tracked separately from the names), a process id,
/// or a PID (the TUI kills orphans by PID).
///
/// Same semantics as agent names everywhere else: the base name is the
/// identity and `tag-name` is an alias that must match the tracked tag.
fn orphan_matches(orphan: &pidtrack::OrphanProcess, target: &str) -> bool {
    orphan
        .names
        .iter()
        .any(|n| n == target || (!orphan.tag.is_empty() && format!("{}-{n}", orphan.tag) == target))
        || orphan.process_id == target
        || target.parse::<u32>().ok() == Some(orphan.pid)
}

/// Display label for an orphan: `tag-name` when tagged, else the base name.
fn orphan_label(orphan: &pidtrack::OrphanProcess) -> String {
    let name = orphan.names.first().map(String::as_str).unwrap_or("?");
    if orphan.tag.is_empty() {
        name.to_string()
    } else {
        format!("{}-{name}", orphan.tag)
    }
}

/// Report target for an orphan: its label, or its PID when it has no name.
fn orphan_target(orphan: &pidtrack::OrphanProcess) -> String {
    if orphan.names.is_empty() {
        orphan.pid.to_string()
    } else {
        orphan_label(orphan)
    }
}

/// The still-running process left behind by a specific stopped session.
pub(crate) fn find_orphan_for_session(session_id: &str) -> Option<pidtrack::OrphanProcess> {
    if session_id.is_empty() {
        return None;
    }
    pidtrack::get_orphan_processes(&paths::hcom_dir(), None)
        .into_iter()
        .find(|o| o.session_id == session_id)
}

/// Format pane close info
fn pane_info_str(pane_closed: bool, preset_name: &str, pane_id: &str) -> String {
    if pane_closed {
        if !pane_id.is_empty() {
            format!(" (closed {} pane {})", preset_name, pane_id)
        } else if !preset_name.is_empty() {
            format!(" (closed {} pane)", preset_name)
        } else {
            String::new()
        }
    } else if !preset_name.is_empty()
        && let Some(preset) = crate::config::get_merged_preset(preset_name)
        && preset.has_close(cfg!(windows))
    {
        if crate::terminal::is_zellij_merged(&preset) {
            return " (zellij pane close unconfirmed)".to_string();
        }
        format!(" (pane close failed for {})", preset_name)
    } else {
        String::new()
    }
}

/// Kill all instances.
fn kill_all(
    db: &HcomDb,
    hcom_dir: &std::path::Path,
    initiator: &str,
    report: &mut KillReport,
) -> Result<i32> {
    let instances = db.iter_instances_full()?;
    let mut killed = 0;
    let mut failed = 0;
    let mut incomplete = 0;

    // Collect active PIDs for orphan filtering
    let mut active_pids = HashSet::new();

    for inst in &instances {
        // Skip remote instances
        if inst.origin_device_id.is_some() {
            continue;
        }

        if let Some(pid) = inst.pid {
            active_pids.insert(pid as u32);
            let is_headless = inst.background != 0;
            let (result, pane_closed, pane_retry_command, preset_name, pane_id) =
                kill_instance(db, &inst.name, pid as u32, inst, is_headless, initiator);
            let pane_info = pane_info_str(pane_closed, &preset_name, &pane_id);
            match result {
                terminal::KillResult::Sent => {
                    report.say(format!("Sent SIGTERM to \'{}\'{}", inst.name, pane_info));
                    killed += 1;
                }
                terminal::KillResult::AlreadyDead => {
                    report.say(format!("\'{}\' had already exited{}", inst.name, pane_info));
                    killed += 1;
                }
                terminal::KillResult::PermissionDenied => {
                    eprintln!(
                        "Permission denied to kill process group {} for '{}'",
                        pid, inst.name
                    );
                    failed += 1;
                }
            }
            incomplete +=
                report_incomplete_pane_cleanup(result.into(), pane_retry_command.as_deref()) as i32;
            let outcome = KillOutcome::signalled(
                &inst.name,
                "agent",
                pid as u32,
                result,
                pane_closed,
                pane_retry_command,
            );
            if result != terminal::KillResult::PermissionDenied {
                stop_instance(db, &inst.name, initiator, "killed");
                report.say(format!("  To resume: hcom r {}", inst.name));
                report.record(outcome.resumable(&inst.name));
            } else {
                report.record(outcome);
            }
        } else {
            // No PID tracked — just clean up
            stop_instance(db, &inst.name, initiator, "killed");
            report.record(KillOutcome::new(&inst.name, "untracked", "stopped"));
        }
    }

    // Kill orphans too
    let orphans = pidtrack::get_orphan_processes(hcom_dir, Some(&active_pids));
    for orphan in &orphans {
        let (result, pane_closed, pane_retry_command) =
            terminal::kill_process(orphan.pid, &orphan.terminal_info());
        let names = orphan.names.join(", ");
        let pane_info = pane_info_str(pane_closed, &orphan.terminal_preset, &orphan.pane_id);
        let result = normalize_kill_result(&names, orphan.pid, result, pane_closed);
        let label = if !names.is_empty() || !pane_info.is_empty() {
            format!(" ({}{})", names, pane_info)
        } else {
            String::new()
        };
        match result {
            terminal::KillResult::Sent => {
                report.say(format!(
                    "Sent SIGTERM to orphan process {}{}",
                    orphan.pid, label
                ));
                killed += 1;
            }
            terminal::KillResult::AlreadyDead => {
                report.say(format!(
                    "Orphan process {} had already exited{}",
                    orphan.pid, label
                ));
                killed += 1;
            }
            terminal::KillResult::PermissionDenied => {
                failed += 1;
            }
        }
        incomplete +=
            report_incomplete_pane_cleanup(result.into(), pane_retry_command.as_deref()) as i32;
        if result != terminal::KillResult::PermissionDenied {
            pidtrack::remove_pid(hcom_dir, orphan.pid);
        }
        report.record(KillOutcome::signalled(
            &orphan_target(orphan),
            "orphan",
            orphan.pid,
            result,
            pane_closed,
            pane_retry_command,
        ));
    }

    if killed == 0 && failed == 0 {
        report.say("No processes with tracked PIDs found");
    } else if failed > 0 || incomplete > 0 {
        report.say(format!(
            "Killed {}, {} failed, {} with incomplete pane cleanup",
            killed, failed, incomplete
        ));
    } else {
        report.say(format!("Killed {}", killed));
    }

    Ok(if failed > 0 || incomplete > 0 { 1 } else { 0 })
}

/// Kill instances by tag.
fn kill_by_tag(
    db: &HcomDb,
    hcom_dir: &std::path::Path,
    tag: &str,
    initiator: &str,
    report: &mut KillReport,
) -> Result<i32> {
    let instances = db.iter_instances_full()?;
    let tagged: Vec<_> = instances
        .iter()
        .filter(|inst| inst.tag.as_deref() == Some(tag) && inst.origin_device_id.is_none())
        .collect();

    let mut killed = 0;
    let mut failed = 0;
    let mut incomplete = 0;

    // Kill active instances with this tag
    for inst in &tagged {
        if let Some(pid) = inst.pid {
            let is_headless = inst.background != 0;
            let (result, pane_closed, pane_retry_command, preset_name, pane_id) =
                kill_instance(db, &inst.name, pid as u32, inst, is_headless, initiator);
            let pane_info = pane_info_str(pane_closed, &preset_name, &pane_id);
            match result {
                terminal::KillResult::Sent => {
                    report.say(format!("Sent SIGTERM to \'{}\'{}", inst.name, pane_info));
                    killed += 1;
                }
                terminal::KillResult::AlreadyDead => {
                    report.say(format!("\'{}\' had already exited", inst.name));
                    killed += 1;
                }
                terminal::KillResult::PermissionDenied => {
                    eprintln!(
                        "Permission denied to kill process group {} for '{}'",
                        pid, inst.name
                    );
                    failed += 1;
                }
            }
            incomplete +=
                report_incomplete_pane_cleanup(result.into(), pane_retry_command.as_deref()) as i32;
            let outcome = KillOutcome::signalled(
                &inst.name,
                "agent",
                pid as u32,
                result,
                pane_closed,
                pane_retry_command,
            );
            if result != terminal::KillResult::PermissionDenied {
                stop_instance(db, &inst.name, initiator, "killed");
                report.record(outcome.resumable(&inst.name));
            } else {
                report.record(outcome);
            }
        } else {
            // No PID tracked — clean up DB entry
            report.say(format!(
                "No tracked process for '{}', stopping instance.",
                inst.name
            ));
            stop_instance(db, &inst.name, initiator, "killed");
            report.record(KillOutcome::new(&inst.name, "untracked", "stopped"));
        }
    }

    // Also kill orphan processes with this tag (stopped but still running)
    let active_pids: HashSet<u32> = tagged
        .iter()
        .filter_map(|i| i.pid.map(|p| p as u32))
        .collect();
    let orphans = pidtrack::get_orphan_processes(hcom_dir, Some(&active_pids));
    let tagged_orphans: Vec<_> = orphans.iter().filter(|o| o.tag == tag).collect();
    for orphan in &tagged_orphans {
        let names = orphan.names.join(", ");
        let (result, pane_closed, pane_retry_command) =
            terminal::kill_process(orphan.pid, &orphan.terminal_info());
        let result = normalize_kill_result(&names, orphan.pid, result, pane_closed);
        let pane_info = pane_info_str(pane_closed, &orphan.terminal_preset, &orphan.pane_id);
        match result {
            terminal::KillResult::Sent => {
                report.say(format!(
                    "Sent SIGTERM to leftover process of stopped agent \'{}\'{}",
                    names, pane_info
                ));
                killed += 1;
            }
            terminal::KillResult::AlreadyDead => {
                report.say(format!("\'{}\' had already exited", names));
            }
            terminal::KillResult::PermissionDenied => {
                eprintln!("Permission denied to kill process group {}", orphan.pid);
                failed += 1;
            }
        }
        incomplete +=
            report_incomplete_pane_cleanup(result.into(), pane_retry_command.as_deref()) as i32;
        if result != terminal::KillResult::PermissionDenied {
            pidtrack::remove_pid(hcom_dir, orphan.pid);
        }
        report.record(KillOutcome::signalled(
            &orphan_target(orphan),
            "orphan",
            orphan.pid,
            result,
            pane_closed,
            pane_retry_command,
        ));
    }

    if tagged.is_empty() && tagged_orphans.is_empty() {
        eprintln!("No agents with tag '{}'", tag);
        report.record_error(&format!("tag:{tag}"), format!("No agents with tag '{tag}'"));
        return Ok(1);
    }

    report.say(format!("Killed {} (tag:{})", killed, tag));
    Ok(if failed > 0 || incomplete > 0 { 1 } else { 0 })
}

/// Kill a single instance by name.
fn kill_single(
    db: &HcomDb,
    hcom_dir: &std::path::Path,
    target: &str,
    initiator: &str,
    report: &mut KillReport,
) -> Result<i32> {
    // Resolve display name
    let name = identity::resolve_display_name(db, target).unwrap_or_else(|| target.to_string());

    // A PID may name a live row: a PTY that rejoined gets its pid back on claim.
    let mut found = db.get_instance_full(&name)?;
    if found.is_none()
        && let Ok(pid) = target.parse::<i64>()
    {
        found = db
            .iter_instances_full()?
            .into_iter()
            .find(|inst| inst.pid == Some(pid));
    }
    let name = found.as_ref().map_or(name, |inst| inst.name.clone());
    let inst = match found {
        Some(inst) => inst,
        None => {
            // Check orphans
            let orphans = pidtrack::get_orphan_processes(hcom_dir, None);
            // Also match by PID number (TUI sends kill by PID for orphans)
            let matches: Vec<&pidtrack::OrphanProcess> = orphans
                .iter()
                .filter(|o| orphan_matches(o, target))
                .collect();
            // Only reachable after a base name was reused: refuse to guess which
            // incarnation's process to signal.
            if matches.len() > 1 {
                let options: Vec<String> = matches
                    .iter()
                    .map(|o| format!("{} (pid {})", orphan_label(o), o.pid))
                    .collect();
                bail!(
                    "'{target}' matches {} leftover processes: {}\n  Kill one by PID: hcom kill <pid>",
                    matches.len(),
                    options.join(", ")
                );
            }
            if let Some(orphan) = matches.first().copied() {
                let (result, pane_closed, pane_retry_command) =
                    terminal::kill_process(orphan.pid, &orphan.terminal_info());
                let result = normalize_kill_result(target, orphan.pid, result, pane_closed);
                let pane_info =
                    pane_info_str(pane_closed, &orphan.terminal_preset, &orphan.pane_id);
                match result {
                    terminal::KillResult::Sent => {
                        report.say(format!(
                            "Sent SIGTERM to leftover process of stopped agent \'{}\'{}",
                            target, pane_info
                        ));
                    }
                    terminal::KillResult::AlreadyDead => {
                        report.say(format!("\'{}\' had already exited{}", target, pane_info));
                    }
                    terminal::KillResult::PermissionDenied => {
                        eprintln!("Permission denied to kill process group {}", orphan.pid);
                        report.record(KillOutcome::signalled(
                            target,
                            "orphan",
                            orphan.pid,
                            result,
                            pane_closed,
                            pane_retry_command,
                        ));
                        return Ok(1);
                    }
                }
                pidtrack::remove_pid(hcom_dir, orphan.pid);
                let incomplete =
                    report_incomplete_pane_cleanup(result.into(), pane_retry_command.as_deref());
                report.record(KillOutcome::signalled(
                    target,
                    "orphan",
                    orphan.pid,
                    result,
                    pane_closed,
                    pane_retry_command,
                ));
                return Ok(if incomplete { 1 } else { 0 });
            }
            // The goal of a kill is already met for a stopped agent: say when and
            // by whom instead of failing, so retries don't look like errors.
            if let Some(stopped) = identity::last_stopped(db, target) {
                report.say(format!("'{target}' {}", stopped.summary()));
                let outcome = KillOutcome::new(target, "stopped", "already_stopped");
                if stopped.reason == crate::hooks::hermes_bridge::DISCONNECTED {
                    report.say(format!(
                        "  {}",
                        crate::hooks::hermes_bridge::reconnect_hint(&stopped.name)
                    ));
                    report.record(outcome);
                } else {
                    report.say(format!("  To resume: hcom r {}", stopped.display_name()));
                    report.record(outcome.resumable(&stopped.display_name()));
                }
                return Ok(0);
            }
            bail!("{}", identity::describe_missing_agent(db, target));
        }
    };

    if inst.origin_device_id.is_some() {
        if let Some((base_name, device_short_id)) =
            crate::relay::control::split_device_suffix(&name)
        {
            let result = crate::relay::control::dispatch_remote_raw(
                db,
                device_short_id,
                Some(&name),
                "kill",
                &serde_json::json!({ "target": base_name }),
                crate::relay::control::RPC_DEFAULT_TIMEOUT,
            )
            .map_err(anyhow::Error::msg)?;
            return handle_remote_kill_response(&name, &result, report);
        }
        bail!("Cannot kill remote '{name}' - missing device suffix");
    }

    // A Hermes bridge is a chat, not a process: kill disconnects it, like stop.
    if inst.pid.is_none()
        && let Some(note) = crate::hooks::hermes_bridge::disconnect(db, &name, initiator)
    {
        report.say(note);
        report.record(KillOutcome::new(&name, "bridge", "disconnected"));
        return Ok(0);
    }
    if inst.pid.is_none() {
        bail!(
            "No tracked PID for '{}' — use 'hcom stop {}' instead",
            name,
            name
        );
    }
    let kill_result = kill_tracked_instance(db, &name, initiator).map_err(anyhow::Error::msg)?;
    let pid = kill_result.pid;
    let pane_closed = kill_result.pane_closed;
    let preset_name = kill_result.preset_name;
    let pane_id = kill_result.pane_id;
    let pane_retry_command = kill_result.pane_retry_command;
    let result = kill_result.kill_result;

    let pane_info = pane_info_str(pane_closed, &preset_name, &pane_id);
    let exit = match result {
        terminal::KillResult::Sent => {
            report.say(format!("Sent SIGTERM to \'{}\'{}", name, pane_info));
            report.say(format!("  To resume: hcom r {}", name));
            0
        }
        terminal::KillResult::AlreadyDead => {
            report.say(format!("\'{}\' had already exited{}", name, pane_info));
            report.say(format!("  To resume: hcom r {}", name));
            0
        }
        terminal::KillResult::PermissionDenied => {
            eprintln!(
                "Permission denied to kill process group {} for '{}'",
                pid, name
            );
            1
        }
    };
    let incomplete = report_incomplete_pane_cleanup(result.into(), pane_retry_command.as_deref());
    let outcome =
        KillOutcome::signalled(&name, "agent", pid, result, pane_closed, pane_retry_command);
    report.record(if exit == 0 {
        outcome.resumable(&name)
    } else {
        outcome
    });
    Ok(if incomplete { 1 } else { exit })
}

fn restore_failed_kill_status(
    db: &HcomDb,
    instance: &crate::db::InstanceRow,
    initiator: &str,
    result: terminal::KillResult,
) {
    if result != terminal::KillResult::PermissionDenied {
        return;
    }
    // Restore only our pending kill marker; hooks may have updated the row
    // while we were signalling or trying to close its pane.
    if let Err(e) = db.conn().execute(
        "UPDATE instances SET status = ?, status_context = ?, status_detail = ?, status_time = ?
         WHERE name = ? AND pid = ? AND status = 'inactive'
           AND status_context = 'exit:killed' AND status_detail = ?",
        rusqlite::params![
            instance.status,
            instance.status_context,
            instance.status_detail,
            instance.status_time,
            instance.name,
            instance.pid,
            initiator,
        ],
    ) {
        log_info(
            "kill",
            "lifecycle.kill_status_restore",
            &format!("name={} err={e}", instance.name),
        );
    }
}

/// Kill a process and close its terminal pane.
/// Returns (KillResult, pane_closed, pane_retry_command, preset_name, pane_id).
fn kill_instance(
    db: &HcomDb,
    name: &str,
    pid: u32,
    instance: &crate::db::InstanceRow,
    is_headless: bool,
    initiator: &str,
) -> (terminal::KillResult, bool, Option<String>, String, String) {
    // The PTY cleanup thread may run as soon as the signal lands. Record the
    // requested reason and initiator first so it cannot turn an explicit kill
    // into a stopped snapshot with reason "closed" or by "pty".
    if let Err(e) = db.mark_killed(name, initiator) {
        log_info(
            "kill",
            "lifecycle.kill_status",
            &format!("name={name} err={e}"),
        );
    }
    // The PID now belongs to an unrelated process (reboot, crash): don't
    // signal it or resolve a pane through it. A merely dead PID falls through
    // so its group and pane are still cleaned up.
    if instance.pid_reused(pid) {
        log_info(
            "kill",
            "lifecycle.kill_pid_reused",
            &format!("name={name} pid={pid} now belongs to another process; not signalling"),
        );
        return (
            terminal::KillResult::AlreadyDead,
            false,
            None,
            String::new(),
            String::new(),
        );
    }
    // Headless instances have no terminal pane — skip pane close
    if is_headless {
        let (result, pane_closed, pane_retry_command) =
            terminal::kill_process(pid, &terminal::TerminalInfo::default());
        let result = normalize_kill_result(name, pid, result, pane_closed);
        restore_failed_kill_status(db, instance, initiator, result);
        log_info(
            "kill",
            "lifecycle.kill",
            &format!(
                "name={} pid={} result={:?} pane_closed={} headless=true",
                name, pid, result, pane_closed
            ),
        );
        return (
            result,
            pane_closed,
            pane_retry_command,
            String::new(),
            String::new(),
        );
    }

    let ti = terminal::resolve_terminal_info(
        instance.terminal_preset_effective.as_deref(),
        instance.launch_context.as_deref(),
    );

    let (result, pane_closed, pane_retry_command) = terminal::kill_process(pid, &ti);
    let result = normalize_kill_result(name, pid, result, pane_closed);
    restore_failed_kill_status(db, instance, initiator, result);

    log_info(
        "kill",
        "lifecycle.kill",
        &format!(
            "name={} pid={} result={:?} pane_closed={}",
            name, pid, result, pane_closed
        ),
    );

    (
        result,
        pane_closed,
        pane_retry_command,
        ti.preset_name.clone(),
        ti.pane_id.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denied_kill_restores_status_and_preserves_tracking() {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&dir.path().join("hcom.db")).unwrap();
        db.conn().execute(
            "INSERT INTO instances (name, created_at, pid, status, status_context, status_detail, status_time)
             VALUES ('luna', 1.0, 4242, 'listening', 'ready', 'original', 123)",
            [],
        ).unwrap();
        let original = db.get_instance_full("luna").unwrap().unwrap();
        db.mark_killed("luna", "boss").unwrap();
        restore_failed_kill_status(
            &db,
            &original,
            "boss",
            terminal::KillResult::PermissionDenied,
        );
        let restored = db.get_instance_full("luna").unwrap().unwrap();
        assert_eq!(restored.pid, original.pid);
        assert_eq!(restored.status, original.status);
        assert_eq!(restored.status_context, original.status_context);
        assert_eq!(restored.status_detail, original.status_detail);
        assert_eq!(restored.status_time, original.status_time);

        db.mark_killed("luna", "boss").unwrap();
        db.conn()
            .execute(
                "UPDATE instances SET status_context = 'new-hook' WHERE name = 'luna'",
                [],
            )
            .unwrap();
        restore_failed_kill_status(
            &db,
            &original,
            "boss",
            terminal::KillResult::PermissionDenied,
        );
        assert_eq!(
            db.get_instance_full("luna")
                .unwrap()
                .unwrap()
                .status_context,
            "new-hook"
        );
    }

    fn orphan(name: &str, tag: &str) -> pidtrack::OrphanProcess {
        pidtrack::OrphanProcess {
            pid: 4242,
            tool: "claude".into(),
            names: vec![name.into()],
            directory: String::new(),
            process_id: "proc-1".into(),
            terminal_preset: String::new(),
            pane_id: String::new(),
            terminal_id: String::new(),
            kitty_listen_on: String::new(),
            zellij_session_name: String::new(),
            herdr_socket_path: String::new(),
            wezterm_unix_socket: String::new(),
            tmux: String::new(),
            session_id: String::new(),
            notify_port: 0,
            inject_port: 0,
            tag: tag.into(),
        }
    }

    #[test]
    fn orphan_match_uses_hcom_name_semantics() {
        let untagged = orphan("tuna", "");
        let tagged = orphan("tuna", "api");
        // Base name is the identity; tag-name is an alias that must match.
        assert!(orphan_matches(&untagged, "tuna"));
        assert!(orphan_matches(&tagged, "tuna"));
        assert!(orphan_matches(&tagged, "api-tuna"));
        assert!(!orphan_matches(&untagged, "api-tuna"));
        assert!(!orphan_matches(&tagged, "web-tuna"));
        assert!(orphan_matches(&tagged, "4242"));
        assert!(orphan_matches(&tagged, "proc-1"));
        assert_eq!(orphan_label(&tagged), "api-tuna");
        assert_eq!(orphan_label(&untagged), "tuna");
    }
    use serde_json::json;

    #[test]
    fn test_kill_no_target_fails() {
        // Missing required target → clap error → exit code 1
        let flags = GlobalFlags::default();
        let argv = vec!["kill".to_string()];
        let result = run(&argv, &flags).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_kill_args_parse_single() {
        use clap::Parser;
        let args = KillArgs::try_parse_from(["kill", "myagent"]).unwrap();
        assert_eq!(args.targets, vec!["myagent"]);
    }

    #[test]
    fn test_kill_args_parse_multiple() {
        use clap::Parser;
        let args = KillArgs::try_parse_from(["kill", "nozu", "zelu"]).unwrap();
        assert_eq!(args.targets, vec!["nozu", "zelu"]);
    }

    #[test]
    fn test_kill_args_no_target_is_empty_vec() {
        use clap::Parser;
        let args = KillArgs::try_parse_from(["kill"]).unwrap();
        assert!(args.targets.is_empty());
    }

    #[test]
    fn test_normalize_permission_denied_after_pane_close_succeeds() {
        let result =
            normalize_kill_result("luna", 42, terminal::KillResult::PermissionDenied, true);
        assert_eq!(result, terminal::KillResult::AlreadyDead);
    }

    #[test]
    fn test_incomplete_cleanup_requires_terminated_process_and_retry_command() {
        assert!(report_incomplete_pane_cleanup(
            PaneCleanupProcessState::Terminated,
            Some("wezterm cli kill-pane --pane-id 123")
        ));
        assert!(!report_incomplete_pane_cleanup(
            PaneCleanupProcessState::NotTerminated,
            Some("wezterm cli kill-pane --pane-id 123")
        ));
        assert!(!report_incomplete_pane_cleanup(
            PaneCleanupProcessState::AlreadyDead,
            None
        ));
    }

    #[test]
    fn test_handle_remote_kill_response_permission_denied_returns_nonzero() {
        let result = handle_remote_kill_response(
            "luna:ABCD",
            &json!({
                "result": {
                    "pid": 42,
                    "kill_result": "permission_denied",
                    "pane_closed": false,
                    "preset_name": "",
                    "pane_id": ""
                }
            }),
            &mut KillReport::new(false),
        )
        .unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_handle_remote_kill_response_permission_denied_with_closed_pane_succeeds() {
        let result = handle_remote_kill_response(
            "luna:ABCD",
            &json!({
                "ok": true,
                "result": {
                    "pid": 42,
                    "kill_result": "already_dead",
                    "pane_closed": true,
                    "preset_name": "kitty",
                    "pane_id": "@1"
                }
            }),
            &mut KillReport::new(false),
        )
        .unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_render_remote_kill_feedback_sent_matches_cli_contract() {
        let lines =
            render_remote_kill_feedback("luna:ABCD", "sent", " (closed kitty pane @1)").unwrap();
        assert_eq!(
            lines,
            vec![
                "Sent SIGTERM to \'luna:ABCD\' (closed kitty pane @1)".to_string(),
                "  To resume: hcom r luna:ABCD".to_string(),
            ]
        );
    }

    #[test]
    fn test_render_remote_kill_feedback_already_dead_matches_cli_contract() {
        let lines = render_remote_kill_feedback("luna:ABCD", "already_dead", "").unwrap();
        assert_eq!(
            lines,
            vec![
                "\'luna:ABCD\' had already exited".to_string(),
                "  To resume: hcom r luna:ABCD".to_string(),
            ]
        );
    }

    #[test]
    fn test_handle_remote_kill_response_unknown_result_errors() {
        let err = handle_remote_kill_response(
            "luna:ABCD",
            &json!({
                "result": {
                    "pid": 42,
                    "kill_result": "mystery",
                    "pane_closed": false,
                    "preset_name": "",
                    "pane_id": ""
                }
            }),
            &mut KillReport::new(false),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unexpected kill_result mystery"));
    }
}
