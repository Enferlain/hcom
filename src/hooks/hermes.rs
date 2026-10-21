//! Hermes Agent: a Python plugin in the profile's `plugins/hcom/` plus the
//! Pi-family argv RPC (`hermes-start|status|read|beforetool|stop`).
//!
//! Hermes has no per-run plugin path, so the plugin is installed per profile
//! (`hcom hooks add hermes`, or the first `hcom hermes` in that profile) and
//! enabled with Hermes's own CLI. It loads in every process of the profile,
//! including a running gateway, and stays inert unless hcom launched that
//! process (see `hermes_plugin/__init__.py`).

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

use super::pi_family::HERMES;
use super::runtime::LaunchCtx;
use crate::tool::Tool;

pub fn dispatch_hermes_hook(hook_name: &str, argv: &[String]) -> (i32, String) {
    // Bridges run in a gateway hcom did not launch: no HCOM_PROCESS_ID gate.
    if hook_name == "hermes-bridge" {
        return super::hermes_bridge::dispatch(argv);
    }
    HERMES.dispatch(hook_name, argv)
}

pub const PLUGIN_SOURCE: &str = include_str!("../hermes_plugin/__init__.py");
const PLUGIN_MANIFEST: &str = include_str!("../hermes_plugin/plugin.yaml");
const PLUGIN_NAME: &str = "hcom";
/// `config.yaml` size and mtime when hcom last confirmed the plugin enabled
/// (and run in process). Any config change re-checks through Hermes's CLI.
const CONFIG_STAMP: &str = ".hcom-config-stamp";

/// Agent markers the plugin removes from Hermes's env so its terminal children
/// are not taken for the agent that started Hermes: every other tool's
/// detection and child-clear vars, plus session ids those agents leave behind.
/// Not hcom's own vars: the launch identity must reach terminal children.
pub fn scrub_vars() -> Vec<&'static str> {
    let mut vars = vec!["CLAUDE_CODE_SESSION_ID"];
    for spec in crate::integration_spec::ALL
        .iter()
        .filter(|s| s.tool != Tool::Hermes)
    {
        for var in crate::shared::tool_detection::tool_env_vars(spec.tool) {
            if !vars.contains(&var) {
                vars.push(var);
            }
        }
    }
    vars
}

/// How the plugin reaches this hcom from a process hcom did not start (a
/// messaging gateway under launchd/systemd has neither hcom on PATH nor this
/// shell's HCOM_DIR / HCOM_DEV_ROOT).
fn hcom_settings() -> serde_json::Value {
    let hcom = crate::terminal::which_bin("hcom")
        .or_else(|| {
            std::env::current_exe()
                .ok()
                .map(|path| path.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "hcom".to_string());
    let env: serde_json::Map<String, serde_json::Value> = ["HCOM_DIR", "HCOM_DEV_ROOT"]
        .into_iter()
        .filter_map(|key| {
            let value = std::env::var(key).ok().filter(|v| !v.is_empty())?;
            let value = if key == "HCOM_DIR" {
                crate::paths::hcom_dir().to_string_lossy().into_owned()
            } else {
                value
            };
            Some((key.to_string(), serde_json::json!(value)))
        })
        .collect();
    serde_json::json!({ "hcom": hcom, "env": env })
}

fn plugin_files() -> [(&'static str, String); 3] {
    let mut settings = hcom_settings();
    settings["scrub_vars"] = serde_json::json!(scrub_vars());
    [
        ("plugin.yaml", PLUGIN_MANIFEST.to_string()),
        ("__init__.py", PLUGIN_SOURCE.to_string()),
        ("hcom.json", format!("{settings:#}\n")),
    ]
}

// ── Gateway plugin version ──────────────────────────────────────────────

const STALE_PREFIX: &str = "hermes_gateway_stale:";
/// A gateway syncs every 20s; one silent longer than this is gone.
const STALE_FRESH_SECS: i64 = 90;

/// Short hash of the plugin source this hcom ships. A gateway's plugin
/// reports the hash of the copy it loaded on every sync.
pub fn plugin_hash() -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(PLUGIN_SOURCE.as_bytes())
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A gateway synced with `loaded` (None: a plugin older than the hash).
/// When that is not this hcom's plugin, rewrite the profile's plugin code and
/// record the gateway as stale until it runs the current one. True when the
/// code on disk is now current, so the plugin can have the gateway reload it.
pub fn note_gateway_plugin(db: &crate::db::HcomDb, home: &Path, loaded: Option<&str>) -> bool {
    let key = format!("{STALE_PREFIX}{}", home_key(home));
    if loaded == Some(plugin_hash().as_str()) {
        if db.kv_get(&key).ok().flatten().is_some() {
            let _ = db.kv_set(&key, None);
        }
        return false;
    }
    let dir = plugin_dir(home);
    let owned = is_hcom_owned(&dir).unwrap_or(false);
    if owned {
        // Code only: hcom.json carries the gateway's own hcom path and env.
        for (name, content) in plugin_files()
            .into_iter()
            .filter(|(n, _)| *n != "hcom.json")
        {
            if std::fs::read_to_string(dir.join(name)).ok().as_ref() != Some(&content)
                && let Err(e) = crate::paths::atomic_write_io(&dir.join(name), &content)
            {
                crate::log::log_warn(
                    "hooks",
                    "hermes.plugin_refresh_failed",
                    &format!("path={} err={e}", dir.join(name).display()),
                );
            }
        }
    }
    let now = crate::shared::time::now_epoch_i64().to_string();
    let _ = db.kv_set(&key, Some(&now));
    owned && code_current(&dir)
}

/// Profile homes whose running gateway has an older hcom plugin loaded.
pub fn stale_gateways(db: &crate::db::HcomDb) -> Vec<String> {
    let now = crate::shared::time::now_epoch_i64();
    db.kv_prefix(STALE_PREFIX)
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, seen)| {
            seen.parse::<i64>()
                .is_ok_and(|t| now - t < STALE_FRESH_SECS)
        })
        .map(|(key, _)| key[STALE_PREFIX.len()..].to_string())
        .collect()
}

/// One line per stale gateway, for `hcom status` and `hcom hooks`.
pub fn stale_gateway_notes(db: &crate::db::HcomDb) -> Vec<String> {
    stale_gateways(db)
        .into_iter()
        .map(|home| {
            let path = Path::new(&home);
            let restart = restart_command(path);
            // Restarting reloads the files on disk: outdated ones first.
            let update = match install_state_at(path) {
                InstallState::Outdated if path == ambient_home() => {
                    "hcom hooks add hermes, then ".to_string()
                }
                InstallState::Outdated => {
                    format!("hcom hermes -p {} (once), then ", profile_name(path))
                }
                _ => String::new(),
            };
            format!("Hermes gateway ({home}) runs an older hcom plugin: {update}{restart}")
        })
        .collect()
}

// ── Profile resolution ──────────────────────────────────────────────────

/// A profile home as one string, however it was spelled (symlinks, `..`):
/// the key for registry entries and KV markers, so every path to a profile
/// finds the same ones.
pub(crate) fn home_key(home: &Path) -> String {
    std::fs::canonicalize(home)
        .unwrap_or_else(|_| home.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

/// A profile name as Hermes canonicalizes it (`-p Work` is `work`, `Default`
/// is the root profile); None unless Hermes would accept it (PROFILE_ID_RE,
/// `^[a-z0-9][a-z0-9_-]{0,63}$`), so a name never escapes `profiles/`.
fn canonical_profile(name: &str) -> Option<String> {
    let name = name.trim().to_lowercase();
    let valid = name.len() <= 64
        && name.bytes().enumerate().all(|(i, b)| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || (i > 0 && (b == b'_' || b == b'-'))
        });
    (valid && !name.is_empty()).then_some(name)
}

/// The raw `-p NAME`, `--profile NAME` or `--profile=NAME` before any `--`.
fn raw_profile_flag(args: &[String]) -> Option<&str> {
    let mut iter = args.iter().take_while(|a| *a != "--");
    while let Some(arg) = iter.next() {
        if arg == "-p" || arg == "--profile" {
            return iter.next().map(String::as_str);
        }
        if let Some(name) = arg.strip_prefix("--profile=") {
            return Some(name);
        }
    }
    None
}

pub(crate) fn profile_flag(args: &[String]) -> Option<String> {
    raw_profile_flag(args).and_then(canonical_profile)
}

/// The profile home a launch runs in, resolved the way Hermes does at startup:
/// `-p/--profile`, else a `HERMES_HOME` that is a profile dir, else the root's
/// sticky `active_profile`.
pub fn hermes_home(ctx: &LaunchCtx) -> PathBuf {
    let env_home = ctx.path_var("HERMES_HOME");
    let env_profile = env_home
        .as_ref()
        .filter(|home| home.parent().and_then(Path::file_name) == Some("profiles".as_ref()));
    let root = match (&env_home, env_profile) {
        (Some(home), Some(_)) => home.parent().and_then(Path::parent).unwrap().to_path_buf(),
        (Some(home), None) => home.clone(),
        (None, _) => ctx.home().join(".hermes"),
    };
    let profile = profile_flag(&ctx.args).or_else(|| {
        if env_profile.is_some() {
            return None;
        }
        std::fs::read_to_string(root.join("active_profile"))
            .ok()
            .and_then(|name| canonical_profile(&name))
    });
    match profile {
        Some(name) if name != "default" => root.join("profiles").join(name),
        Some(_) => root,
        None => env_profile.cloned().unwrap_or(root),
    }
}

/// The `-p` value naming `home` for Hermes's CLI.
fn profile_name(home: &Path) -> String {
    match (home.parent().and_then(Path::file_name), home.file_name()) {
        (Some(parent), Some(name)) if parent == "profiles" => name.to_string_lossy().into_owned(),
        _ => "default".to_string(),
    }
}

pub fn plugin_dir(home: &Path) -> PathBuf {
    home.join("plugins").join(PLUGIN_NAME)
}

fn ambient_home() -> PathBuf {
    hermes_home(&LaunchCtx::ambient(Tool::Hermes, false))
}

// ── Install state ───────────────────────────────────────────────────────

/// The plugin's files match this hcom's. `with_settings: false` ignores
/// hcom.json, which records the installing shell's hcom path and env.
fn files_current(dir: &Path, with_settings: bool) -> bool {
    plugin_files()
        .iter()
        .filter(|(name, _)| with_settings || *name != "hcom.json")
        .all(|(name, content)| {
            std::fs::read_to_string(dir.join(name)).ok().as_ref() == Some(content)
        })
}

fn code_current(dir: &Path) -> bool {
    files_current(dir, false)
}

fn config_stamp(home: &Path) -> Option<String> {
    let meta = std::fs::metadata(home.join("config.yaml")).ok()?;
    let mtime = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    Some(format!("{}:{}", meta.len(), mtime.as_nanos()))
}

fn config_confirmed(home: &Path) -> bool {
    let stamp = std::fs::read_to_string(plugin_dir(home).join(CONFIG_STAMP)).ok();
    stamp.is_some() && stamp == config_stamp(home)
}

fn write_stamp(home: &Path) -> Result<()> {
    let path = plugin_dir(home).join(CONFIG_STAMP);
    if let Some(stamp) = config_stamp(home) {
        crate::paths::atomic_write_io(&path, &stamp)
            .with_context(|| format!("Cannot write {}", path.display()))?;
    }
    Ok(())
}

/// Write hcom's own config entry through Hermes's CLI. The entry cannot
/// disable the plugin, so a confirmed config stays confirmed.
fn set_own_config(home: &Path, args: &[&str]) -> Result<()> {
    let confirmed = config_confirmed(home);
    hermes(home, args)?;
    if confirmed {
        write_stamp(home)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallState {
    /// No plugin, or another plugin named hcom.
    Missing,
    /// hcom's plugin from another hcom version (or another HCOM_DIR).
    Outdated,
    /// Current files; config.yaml changed since hcom last confirmed through
    /// Hermes's CLI that the plugin is enabled.
    Unverified,
    /// Current files, enabled as of a check the config has not outdated.
    Confirmed,
}

pub fn install_state_at(home: &Path) -> InstallState {
    let dir = plugin_dir(home);
    if !code_current(&dir) {
        if is_hcom_owned(&dir).unwrap_or(false) {
            InstallState::Outdated
        } else {
            InstallState::Missing
        }
    } else if config_confirmed(home) {
        InstallState::Confirmed
    } else {
        InstallState::Unverified
    }
}

pub fn install_state() -> InstallState {
    install_state_at(&ambient_home())
}

/// True when the ambient profile has the current plugin, enabled (as of the
/// last check through Hermes's CLI).
pub fn verify_hermes_plugin_installed() -> bool {
    install_state() == InstallState::Confirmed
}

/// Ask Hermes's CLI (seconds, not milliseconds) whether the ambient profile
/// has the plugin enabled, re-stamping when it does; never enables it.
/// `None`: the plugin files are not installed.
pub fn check_enabled() -> Result<Option<bool>> {
    let home = ambient_home();
    if !code_current(&plugin_dir(&home)) {
        return Ok(None);
    }
    let on = enabled(&home)?;
    if on {
        write_stamp(&home)?;
    }
    Ok(Some(on))
}

pub fn get_hermes_plugin_path() -> PathBuf {
    plugin_dir(&ambient_home())
}

/// `hermes` acting on exactly the profile at `home`, whatever the ambient
/// HERMES_HOME: Hermes resolves `-p NAME` under HERMES_HOME's root, so both
/// are pinned (`-p default` for a root home, which would otherwise follow the
/// root's sticky profile).
fn hermes_command(home: &Path, args: &[&str]) -> Command {
    let mut cmd = Command::new("hermes");
    cmd.arg("-p").arg(profile_name(home)).args(args);
    cmd.env("HERMES_HOME", home);
    // The plugin runs in this subcommand too; it must not take it for an
    // hcom launch.
    cmd.env_remove("HCOM_LAUNCHED")
        .env_remove("HCOM_PROCESS_ID");
    cmd
}

pub(crate) fn hermes(home: &Path, args: &[&str]) -> Result<String> {
    if !home.join("config.yaml").is_file() {
        bail!("No Hermes profile at {}", home.display());
    }
    // A real `hermes` against a test's temp home installs a whole runtime there.
    if cfg!(test) {
        bail!("unit tests never run hermes ({})", args.join(" "));
    }
    let mut cmd = hermes_command(home, args);
    let rendered = format!(
        "HERMES_HOME={} hermes -p {} {}",
        home.display(),
        profile_name(home),
        args.join(" ")
    );
    run_bounded(&mut cmd, HERMES_CLI_TIMEOUT).with_context(|| format!("`{rendered}` failed"))
}

/// Hermes's CLI runs on the launch path and inside plugin RPCs: never wait
/// on a hung one forever.
const HERMES_CLI_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

fn run_bounded(cmd: &mut Command, timeout: std::time::Duration) -> Result<String> {
    use std::io::{Read, Seek};
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    let mut child = cmd
        .stdin(std::process::Stdio::null())
        .stdout(stdout.try_clone()?)
        .stderr(stderr.try_clone()?)
        .spawn()
        .context("cannot start hermes")?;
    let deadline = std::time::Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("timed out after {}s", timeout.as_secs());
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    };
    let read = |file: &mut std::fs::File| -> Result<String> {
        let mut bytes = Vec::new();
        file.rewind()?;
        file.read_to_end(&mut bytes)?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    };
    if !status.success() {
        bail!("{}", read(&mut stderr)?.trim());
    }
    read(&mut stdout)
}

/// Enable the plugin through Hermes's CLI (its locking and validation apply),
/// unless the profile runs plugins out of process: there the plugin cannot
/// keep nested Hermes runs from taking the identity, so it stays inert.
/// Returns what `hermes plugins enable` printed when it had to run.
fn configure(home: &Path) -> Result<Option<String>> {
    if enabled(home)? {
        return Ok(None);
    }
    hermes(home, &["plugins", "enable", PLUGIN_NAME]).map(Some)
}

/// Whether Hermes has the plugin enabled; an error when it would run out of
/// process.
fn enabled(home: &Path) -> Result<bool> {
    let raw = hermes(home, &["config", "get", "plugins", "--json"])?;
    // An unset section prints a notice, not JSON: nothing configured.
    let plugins: serde_json::Value = serde_json::from_str(raw.trim()).unwrap_or_default();
    check_isolation(&plugins, home)?;
    let listed = |key: &str| {
        plugins[key]
            .as_array()
            .is_some_and(|names| names.iter().any(|n| n == PLUGIN_NAME))
    };
    Ok(listed("enabled") && !listed("disabled"))
}

fn check_isolation(plugins: &serde_json::Value, home: &Path) -> Result<()> {
    match plugins["isolation"].as_str() {
        None | Some("in_process") => Ok(()),
        Some(mode) => bail!(
            "hcom needs plugins.isolation: in_process for this profile ({}), not {mode}",
            home.display()
        ),
    }
}

/// KV marker: hcom wrote this profile's injection grant (so uninstall removes it).
fn grant_key(home: &Path) -> String {
    format!("hermes_grant:{}", home_key(home))
}

/// Let the plugin start turns through `ctx.inject_message` (gateway chats).
/// Written when the first chat starts hcom, not at install: it is the opt-in.
pub(crate) fn grant_injection(home: &Path) -> Result<()> {
    let raw = hermes(home, &["config", "get", "plugins", "--json"])?;
    let plugins: serde_json::Value = serde_json::from_str(raw.trim()).unwrap_or_default();
    if plugins["entries"][PLUGIN_NAME]["allow_gateway_injection"] == true {
        return Ok(());
    }
    // Recorded first: a write interrupted after it lands is still undone.
    let db = crate::db::HcomDb::open()?;
    db.kv_set(&grant_key(home), Some("1"))?;
    set_own_config(
        home,
        &[
            "config",
            "set",
            "plugins.entries.hcom.allow_gateway_injection",
            "true",
        ],
    )
}

/// Undo what `grant_injection` wrote (never a grant the user set).
fn revoke_injection(home: &Path) -> Result<()> {
    let db = crate::db::HcomDb::open()?;
    if db.kv_get(&grant_key(home))?.is_none() {
        return Ok(());
    }
    set_own_config(
        home,
        &[
            "config",
            "unset",
            "plugins.entries.hcom.allow_gateway_injection",
        ],
    )?;
    db.kv_set(&grant_key(home), None)?;
    Ok(())
}

/// What `install_at` changed.
#[derive(Debug, Default)]
struct Installed {
    files_written: bool,
    /// Hermes's own report from enabling the plugin (whether a running
    /// gateway loaded it), when it was not enabled yet.
    enabled: Option<String>,
}

/// Install or refresh the plugin in `home`'s profile.
fn install_at(home: &Path) -> Result<Installed> {
    if !home.join("config.yaml").is_file() {
        bail!(
            "No Hermes profile at {} (run `hermes setup`, or `hermes profile create <name>`)",
            home.display()
        );
    }
    let dir = plugin_dir(home);
    let mut done = Installed::default();
    if !files_current(&dir, true) {
        done.files_written = true;
        let occupied = std::fs::read_dir(&dir).is_ok_and(|mut entries| entries.next().is_some());
        if occupied && !is_hcom_owned(&dir)? {
            bail!(
                "{} is another plugin named hcom; hcom will not overwrite it",
                dir.display()
            );
        }
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("Cannot create {}", dir.display()))?;
        for (name, content) in plugin_files() {
            crate::paths::atomic_write_io(&dir.join(name), &content)
                .with_context(|| format!("Cannot write {}", dir.join(name).display()))?;
        }
    }
    if !config_confirmed(home) {
        done.enabled = configure(home)?;
        write_stamp(home)?;
    }
    Ok(done)
}

/// Launch path: make sure the launch's profile has the current plugin.
pub fn ensure_installed(ctx: &LaunchCtx) -> Result<()> {
    if let Some(name) = raw_profile_flag(&ctx.args)
        && canonical_profile(name).is_none()
    {
        bail!("Invalid Hermes profile name {name:?} (letters, digits, _ and -)");
    }
    let home = hermes_home(ctx);
    install_at(&home).map(drop).with_context(|| {
        format!(
            "Failed to install hcom's Hermes plugin in {}",
            plugin_dir(&home).display()
        )
    })
}

/// `hcom hooks add hermes`, with what a running gateway needs to pick it up.
pub fn add_ambient() -> Result<String> {
    let home = ambient_home();
    let done = install_at(&home).with_context(|| {
        format!(
            "Failed to install hcom's Hermes plugin in {}",
            plugin_dir(&home).display()
        )
    })?;
    let restart = restart_command(&home);
    Ok(match done.enabled {
        // Enabling nudges a running gateway to load it; Hermes says whether one did.
        Some(out) => {
            let lines: Vec<&str> = out
                .lines()
                .map(str::trim)
                .filter(|l| l.to_lowercase().contains("gateway"))
                .collect();
            if lines.is_empty() {
                format!("If a Hermes gateway is running and has no /hcom: {restart}")
            } else {
                lines.join("\n")
            }
        }
        None if done.files_written => {
            "A running Hermes gateway reloads it within ~20s.".to_string()
        }
        None => String::new(),
    })
}

/// `hermes [-p NAME] gateway restart` for the profile at `home`.
fn restart_command(home: &Path) -> String {
    match profile_name(home).as_str() {
        "default" => "hermes gateway restart".to_string(),
        name => format!("hermes -p {name} gateway restart"),
    }
}

/// `hcom hooks add hermes`: the ambient profile.
pub fn try_setup_hermes_plugin() -> Result<()> {
    ensure_installed(&LaunchCtx::ambient(Tool::Hermes, false))
}

/// True when `dir` holds hcom's plugin (any version), not a user's `hcom` plugin.
fn is_hcom_owned(dir: &Path) -> std::io::Result<bool> {
    crate::hooks::runtime::file_is_hcom_owned(&dir.join("__init__.py"), |content| {
        content.starts_with("\"\"\"hcom plugin for Hermes Agent")
    })
}

/// `hcom hooks remove hermes` (and `all`, `hcom reset`): disconnect every
/// bridge and remove hcom's plugin from every profile it is in. A running
/// gateway keeps the loaded copy (inert) until it restarts.
pub fn remove_hermes_plugin() -> Result<bool> {
    let db = crate::db::HcomDb::open().ok();
    let mut removed = false;
    for home in installed_homes(db.as_ref()) {
        if let Some(db) = &db {
            super::hermes_bridge::forget_home(db, &home);
        }
        removed |= remove_from(&home)?;
    }
    Ok(removed)
}

/// Every profile hcom may have installed into: the ambient root and its
/// `profiles/*` (`hcom hermes -p NAME` installs per profile), plus any
/// profile the bridge registry names (a custom HERMES_HOME).
fn installed_homes(db: Option<&crate::db::HcomDb>) -> Vec<PathBuf> {
    let ambient = ambient_home();
    let root = match ambient.parent() {
        Some(parent) if parent.file_name() == Some("profiles".as_ref()) => {
            parent.parent().unwrap_or(parent).to_path_buf()
        }
        _ => ambient.clone(),
    };
    let mut homes = vec![root.clone()];
    if let Ok(entries) = std::fs::read_dir(root.join("profiles")) {
        homes.extend(entries.flatten().map(|e| e.path()).filter(|p| p.is_dir()));
    }
    if let Some(db) = db {
        homes.extend(
            super::hermes_bridge::bridges(db)
                .into_iter()
                .map(|b| b.home.into()),
        );
    }
    let mut seen = std::collections::HashSet::new();
    homes.retain(|home| seen.insert(home_key(home)));
    homes
}

/// Disable and delete hcom's plugin in one profile, and undo its grant.
fn remove_from(home: &Path) -> Result<bool> {
    let dir = plugin_dir(home);
    if !is_hcom_owned(&dir)? {
        return Ok(false);
    }
    if crate::terminal::which_bin("hermes").is_some() {
        for result in [
            hermes(home, &["plugins", "disable", PLUGIN_NAME]),
            revoke_injection(home).map(|()| String::new()),
        ] {
            if let Err(e) = result {
                eprintln!("Warning: {e:#}");
            }
        }
    }
    std::fs::remove_dir_all(&dir).with_context(|| format!("Cannot remove {}", dir.display()))?;
    eprintln!(
        "Removed hcom's plugin from {}. Run `{}` to unload it from a running Hermes gateway.",
        home.display(),
        restart_command(home)
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn ctx(home: &Path, env: &[(&str, &str)], args: &[&str]) -> LaunchCtx {
        let mut vars: HashMap<String, String> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        vars.insert("HOME".into(), home.to_string_lossy().into_owned());
        LaunchCtx {
            tool: Tool::Hermes,
            env: vars,
            cwd: home.to_path_buf(),
            args: args.iter().map(|s| s.to_string()).collect(),
            auto_approve: false,
        }
    }

    #[test]
    fn home_follows_profile_flag_env_and_sticky_profile() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let root = home.join(".hermes");
        std::fs::create_dir_all(&root).unwrap();

        assert_eq!(hermes_home(&ctx(home, &[], &[])), root);
        assert_eq!(
            hermes_home(&ctx(home, &[], &["-p", "work", "chat"])),
            root.join("profiles/work")
        );
        assert_eq!(hermes_home(&ctx(home, &[], &["--profile=default"])), root);
        // A flag after `--` belongs to the prompt.
        assert_eq!(hermes_home(&ctx(home, &[], &["--", "-p", "x"])), root);

        std::fs::write(root.join("active_profile"), "sticky\n").unwrap();
        assert_eq!(
            hermes_home(&ctx(home, &[], &[])),
            root.join("profiles/sticky")
        );
        assert_eq!(hermes_home(&ctx(home, &[], &["-p", "default"])), root);

        // HERMES_HOME naming a profile dir wins over the sticky profile.
        let custom = home.join("alt/profiles/one");
        let custom_s = custom.to_string_lossy().into_owned();
        assert_eq!(
            hermes_home(&ctx(home, &[("HERMES_HOME", &custom_s)], &[])),
            custom
        );
        assert_eq!(
            hermes_home(&ctx(home, &[("HERMES_HOME", &custom_s)], &["-p", "two"])),
            home.join("alt/profiles/two")
        );
        // HERMES_HOME naming a root still honours that root's sticky profile.
        let alt_root = home.join("altroot");
        std::fs::create_dir_all(&alt_root).unwrap();
        let alt_s = alt_root.to_string_lossy().into_owned();
        assert_eq!(
            hermes_home(&ctx(home, &[("HERMES_HOME", &alt_s)], &[])),
            alt_root
        );
        std::fs::write(alt_root.join("active_profile"), "b").unwrap();
        assert_eq!(
            hermes_home(&ctx(home, &[("HERMES_HOME", &alt_s)], &[])),
            alt_root.join("profiles/b")
        );

        assert_eq!(profile_name(&root.join("profiles/work")), "work");
        assert_eq!(profile_name(&root), "default");
    }

    #[test]
    fn scrub_list_covers_other_agents_but_not_hermes_or_hcom_identity() {
        let vars = scrub_vars();
        for var in [
            "CLAUDECODE",
            "CLAUDE_ENV_FILE",
            "CLAUDE_CODE_SESSION_ID",
            "CODEX_SESSION_ID",
            "CODEX_THREAD_ID",
            "GEMINI_CLI",
            "OPENCODE",
        ] {
            assert!(vars.contains(&var), "{var}");
        }
        for var in [
            "HERMES_SESSION_ID",
            "HERMES_AGENT",
            "HCOM_TOOL",
            "HCOM_PROCESS_ID",
        ] {
            assert!(!vars.contains(&var), "{var}");
        }
        let json = &plugin_files()[2].1;
        let parsed: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(parsed["scrub_vars"].as_array().unwrap().len(), vars.len());
    }

    #[test]
    #[serial_test::serial] // hcom.json follows HCOM_DIR, which serial tests change
    fn install_state_tracks_files_and_config_confirmation() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        std::fs::write(home.join("config.yaml"), "model: x\n").unwrap();
        assert_eq!(install_state_at(home), InstallState::Missing);

        let plugin = plugin_dir(home);
        std::fs::create_dir_all(&plugin).unwrap();
        for (name, content) in plugin_files() {
            std::fs::write(plugin.join(name), content).unwrap();
        }
        assert_eq!(install_state_at(home), InstallState::Unverified);
        std::fs::write(plugin.join(CONFIG_STAMP), config_stamp(home).unwrap()).unwrap();
        assert_eq!(install_state_at(home), InstallState::Confirmed);

        // A config edit (e.g. `hermes plugins disable hcom`) needs a re-check.
        std::fs::write(home.join("config.yaml"), "model: y\nplugins: {}\n").unwrap();
        assert_eq!(install_state_at(home), InstallState::Unverified);
        std::fs::write(plugin.join(CONFIG_STAMP), config_stamp(home).unwrap()).unwrap();
        assert_eq!(install_state_at(home), InstallState::Confirmed);

        // A stale plugin from an older hcom.
        std::fs::write(
            plugin.join("__init__.py"),
            "\"\"\"hcom plugin for Hermes Agent: old",
        )
        .unwrap();
        assert_eq!(install_state_at(home), InstallState::Outdated);
        assert!(is_hcom_owned(&plugin).unwrap());
        std::fs::write(plugin.join("__init__.py"), "def register(ctx): pass").unwrap();
        assert!(!is_hcom_owned(&plugin).unwrap());
        assert_eq!(install_state_at(home), InstallState::Missing);
    }

    #[test]
    fn profile_names_are_canonicalized_like_hermes() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let root = home.join(".hermes");
        std::fs::create_dir_all(&root).unwrap();
        for (args, expected) in [
            (vec!["-p", "Work"], root.join("profiles/work")),
            (vec!["--profile", " Work "], root.join("profiles/work")),
            (vec!["--profile=DEFAULT"], root.clone()),
            (vec!["-p", "Default"], root.clone()),
        ] {
            assert_eq!(hermes_home(&ctx(home, &[], &args)), expected, "{args:?}");
        }
        std::fs::write(root.join("active_profile"), " Sticky \n").unwrap();
        assert_eq!(
            hermes_home(&ctx(home, &[], &[])),
            root.join("profiles/sticky")
        );
        std::fs::write(root.join("active_profile"), "Default").unwrap();
        assert_eq!(hermes_home(&ctx(home, &[], &[])), root);

        // Names Hermes would refuse never become a path, and a launch says so.
        for bad in ["../../elsewhere", "a/b", "-x", "_x", &"a".repeat(65)] {
            assert_eq!(canonical_profile(bad), None, "{bad}");
            let err = ensure_installed(&ctx(home, &[], &["-p", bad])).unwrap_err();
            assert!(err.to_string().contains("Invalid Hermes profile"), "{err}");
        }
        assert_eq!(canonical_profile("w0rk_2-b").as_deref(), Some("w0rk_2-b"));
    }

    #[test]
    fn install_never_touches_another_plugin_named_hcom() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        std::fs::write(home.join("config.yaml"), "model: x\n").unwrap();
        let plugin = plugin_dir(home);
        std::fs::create_dir_all(&plugin).unwrap();
        std::fs::write(plugin.join("__init__.py"), "def register(ctx): pass\n").unwrap();
        std::fs::write(plugin.join("plugin.yaml"), "name: hcom\n").unwrap();
        let err = install_at(home).unwrap_err();
        assert!(
            format!("{err:#}").contains("another plugin named hcom"),
            "{err:#}"
        );
        assert_eq!(
            std::fs::read_to_string(plugin.join("__init__.py")).unwrap(),
            "def register(ctx): pass\n"
        );
        assert_eq!(
            std::fs::read_to_string(plugin.join("plugin.yaml")).unwrap(),
            "name: hcom\n"
        );
        assert!(!plugin.join("hcom.json").exists());
    }

    #[test]
    fn hermes_config_calls_are_pinned_to_the_profile_home() {
        let env = |cmd: &Command| {
            cmd.get_envs()
                .find(|(k, _)| *k == "HERMES_HOME")
                .and_then(|(_, v)| v.map(|v| v.to_os_string()))
        };
        let args = |cmd: &Command| -> Vec<String> {
            cmd.get_args()
                .map(|a| a.to_string_lossy().into_owned())
                .collect()
        };
        // Two roots with the same profile name, and a custom root's default.
        for home in [
            Path::new("/a/.hermes/profiles/work"),
            Path::new("/b/custom/profiles/work"),
            Path::new("/b/custom"),
        ] {
            let cmd = hermes_command(home, &["config", "get", "plugins"]);
            assert_eq!(env(&cmd).as_deref(), Some(home.as_os_str()), "{home:?}");
            let expected = if home.ends_with("work") {
                "work"
            } else {
                "default"
            };
            assert_eq!(args(&cmd)[..2], ["-p".to_string(), expected.to_string()]);
        }
        // A home that is not a profile is refused before any subprocess.
        let dir = tempfile::tempdir().unwrap();
        let err = grant_injection(dir.path()).unwrap_err();
        assert!(err.to_string().contains("No Hermes profile"), "{err}");
    }

    #[test]
    fn host_isolation_is_refused() {
        let home = Path::new("/h/.hermes/profiles/work");
        assert!(check_isolation(&serde_json::json!({}), home).is_ok());
        assert!(check_isolation(&serde_json::json!({"isolation": "in_process"}), home).is_ok());
        let err = check_isolation(&serde_json::json!({"isolation": "host"}), home).unwrap_err();
        assert!(
            err.to_string().contains("plugins.isolation: in_process"),
            "{err}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn remove_sweeps_every_profile_but_never_a_foreign_plugin() {
        let (_dir, _hcom_dir, home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let root = home.join(".hermes");
        let install = |home: &Path| {
            let dir = plugin_dir(home);
            std::fs::create_dir_all(&dir).unwrap();
            for (name, content) in plugin_files() {
                std::fs::write(dir.join(name), content).unwrap();
            }
        };
        // Active root, a `-p work` install, and another plugin named hcom.
        install(&root);
        install(&root.join("profiles/work"));
        let foreign = plugin_dir(&root.join("profiles/other"));
        std::fs::create_dir_all(&foreign).unwrap();
        std::fs::write(foreign.join("__init__.py"), "def register(ctx): pass").unwrap();

        assert!(remove_hermes_plugin().unwrap());
        assert!(!plugin_dir(&root).exists());
        assert!(!plugin_dir(&root.join("profiles/work")).exists());
        assert!(foreign.join("__init__.py").exists());
        assert!(!remove_hermes_plugin().unwrap(), "nothing left to remove");
    }

    #[test]
    fn install_refuses_a_missing_profile() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ctx(dir.path(), &[], &["-p", "nope"]);
        let err = ensure_installed(&ctx).unwrap_err();
        assert!(format!("{err:#}").contains("No Hermes profile"), "{err:#}");
        assert!(!plugin_dir(&hermes_home(&ctx)).exists());
    }
}
