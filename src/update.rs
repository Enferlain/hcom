//! Daily, non-blocking checks for the latest published stable release.

use crate::paths::{FLAGS_DIR, atomic_write, hcom_path};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime};

const CHECK_INTERVAL: Duration = Duration::from_secs(86400);
const RETRY_INTERVAL: Duration = Duration::from_secs(900);

pub(crate) fn flag_path() -> PathBuf {
    hcom_path(&[FLAGS_DIR, "update_check"])
}

fn parse_version(v: &str) -> Option<(u32, u32, u32)> {
    let mut parts = v.trim().strip_prefix('v').unwrap_or(v.trim()).split('.');
    let version = (
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    );
    if parts.next().is_some() {
        None
    } else {
        Some(version)
    }
}

fn is_fresh(path: &Path, interval: Duration) -> bool {
    path.metadata()
        .and_then(|m| m.modified())
        .is_ok_and(|mtime| {
            SystemTime::now()
                .duration_since(mtime)
                .unwrap_or(Duration::ZERO)
                < interval
        })
}

// Separate attempt timestamp preserves known notices on network failures,
// without spawning a checker on every subsequent CLI command.
fn claim_check(flag: &Path) -> bool {
    let attempt = flag.with_extension("attempt");
    if is_fresh(&attempt, RETRY_INTERVAL) {
        return false;
    }
    if let Some(parent) = attempt.parent()
        && fs::create_dir_all(parent).is_err()
    {
        return false;
    }
    if attempt.exists() && fs::remove_file(&attempt).is_err() {
        return false;
    }
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(attempt)
        .is_ok()
}

fn spawn_background_check(flag: &Path, current: &str) {
    if !claim_check(flag) {
        return;
    }
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let mut cmd = Command::new(exe);
    crate::sys::process::detach_session(&mut cmd);
    let _ = cmd
        .arg("__update-check")
        .arg(flag)
        .arg(current)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

pub(crate) fn run_background_check(flag: &Path, current: &str) {
    if let Some(latest) = fetch_latest_version() {
        write_update_cache(flag, current, &latest);
    }
}

fn write_update_cache(flag: &Path, current: &str, latest: &str) {
    let (Some(current), Some(remote)) = (parse_version(current), parse_version(latest)) else {
        return;
    };
    atomic_write(flag, if remote > current { latest } else { "" });
}

// Installer assets belong to published releases, not arbitrary Git tags.
fn fetch_latest_version() -> Option<String> {
    let latest = fetch_release_redirect().or_else(fetch_release_api);
    #[cfg(windows)]
    {
        latest.or_else(fetch_release_powershell)
    }
    #[cfg(not(windows))]
    latest
}

fn fetch_release_api() -> Option<String> {
    let mut cmd = Command::new("curl");
    hide_console(&mut cmd);
    let output = cmd
        .args([
            "-fsSL",
            "--max-time",
            "8",
            "-H",
            "Accept: application/vnd.github+json",
            "https://api.github.com/repos/aannoo/hcom/releases/latest",
        ])
        .stdin(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    published_version(&output.stdout)
}

// GitHub's latest-release redirect shares the installer's published-release
// semantics and remains usable when the REST API rate limit is exhausted.
fn fetch_release_redirect() -> Option<String> {
    let mut cmd = Command::new("curl");
    hide_console(&mut cmd);
    let output = cmd
        .args([
            "-fsSIL",
            "--max-time",
            "8",
            "-o",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
            "-w",
            "%{url_effective}",
            "https://github.com/aannoo/hcom/releases/latest",
        ])
        .stdin(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    version_from_release_url(String::from_utf8_lossy(&output.stdout).trim())
}

fn version_from_release_url(url: &str) -> Option<String> {
    let version = url.strip_prefix("https://github.com/aannoo/hcom/releases/tag/v")?;
    parse_version(version)?;
    Some(version.into())
}

#[cfg(windows)]
fn fetch_release_powershell() -> Option<String> {
    let mut cmd = Command::new(windows_installer_program());
    hide_console(&mut cmd);
    let output = cmd.args(["-NoProfile", "-Command", "$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12; $r=Invoke-RestMethod -TimeoutSec 8 'https://api.github.com/repos/aannoo/hcom/releases/latest'; $r | Select-Object tag_name,draft,prerelease | ConvertTo-Json -Compress"])
        .env_remove("PSModulePath").stdin(Stdio::null()).output().ok()?;
    if !output.status.success() {
        return None;
    }
    published_version(&output.stdout)
}

fn published_version(body: &[u8]) -> Option<String> {
    let release: serde_json::Value = serde_json::from_slice(body).ok()?;
    if release.get("draft").and_then(|v| v.as_bool()) == Some(true)
        || release.get("prerelease").and_then(|v| v.as_bool()) == Some(true)
    {
        return None;
    }
    let version = release
        .get("tag_name")?
        .as_str()?
        .strip_prefix('v')?
        .to_string();
    parse_version(&version)?;
    Some(version)
}

fn hide_console(cmd: &mut Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000);
    }
    #[cfg(not(windows))]
    let _ = cmd;
}

#[derive(Clone, Debug)]
pub struct UpdateInfo {
    pub current: String,
    pub latest: String,
    pub available: bool,
}

pub fn fetch_update_info() -> anyhow::Result<UpdateInfo> {
    let current = env!("CARGO_PKG_VERSION").to_string();
    let latest = fetch_latest_version().ok_or_else(|| {
        anyhow::anyhow!("Could not fetch the latest published release from GitHub")
    })?;
    let current_version = parse_version(&current)
        .ok_or_else(|| anyhow::anyhow!("Invalid current version: {current}"))?;
    let latest_version = parse_version(&latest)
        .ok_or_else(|| anyhow::anyhow!("Invalid release version: {latest}"))?;
    Ok(UpdateInfo {
        current,
        latest,
        available: latest_version > current_version,
    })
}

/// Structured argv preserves spaces in Python paths and avoids shell parsing.
#[derive(Debug)]
pub(crate) struct PackageUpdate {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: Vec<(&'static str, PathBuf)>,
    pub park_entrypoint: Option<PathBuf>,
    pub verify_exe: Option<PathBuf>,
}
impl PackageUpdate {
    pub fn command(&self) -> Command {
        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args);
        for (key, value) in &self.env {
            cmd.env(key, value);
        }
        cmd
    }

    pub fn execute(&self, mut command: Command) -> anyhow::Result<()> {
        use anyhow::Context;

        // Older uv releases copy over the Windows entrypoint in place. Park
        // that image first; running agents keep using the renamed binary.
        let parked = if let Some(entry) = self.park_entrypoint.as_ref().filter(|p| p.is_file()) {
            let parent = entry
                .parent()
                .ok_or_else(|| anyhow::anyhow!("Entrypoint has no parent"))?;
            // Prepare an unmapped copy before parking, so hcom stays available
            // to hooks while uv resolves/downloads the upgraded package.
            let copy = tempfile::NamedTempFile::new_in(parent)?;
            fs::copy(entry, copy.path())?;
            if let Some(parent) = entry.parent()
                && let Ok(entries) = fs::read_dir(parent)
            {
                for old in entries.flatten() {
                    if old
                        .file_name()
                        .to_string_lossy()
                        .starts_with("hcom.exe.update-old-")
                        && old.path().is_file()
                    {
                        let _ = fs::remove_file(old.path());
                    }
                }
            }
            let backup =
                entry.with_file_name(format!("hcom.exe.update-old-{}", uuid::Uuid::new_v4()));
            fs::rename(entry, &backup)
                .with_context(|| format!("Could not park {}", entry.display()))?;
            if let Err(error) = copy.persist(entry) {
                fs::rename(&backup, entry)?;
                return Err(error.error.into());
            }
            Some((entry, backup))
        } else {
            None
        };
        let mut result = command
            .status()
            .context("Could not run package manager")
            .and_then(|status| {
                anyhow::ensure!(
                    status.success(),
                    "Package manager failed (exit {})",
                    status.code().unwrap_or(-1)
                );
                Ok(())
            });
        if let Some((entry, backup)) = parked {
            if result.is_ok() && !entry.is_file() {
                result = Err(anyhow::anyhow!(
                    "Package manager did not recreate {}",
                    entry.display()
                ));
            }
            if result.is_ok() {
                let _ = fs::remove_file(&backup);
            } else {
                fs::rename(&backup, entry).with_context(|| {
                    format!(
                        "Could not restore the previous entrypoint; it is at {}",
                        backup.display()
                    )
                })?;
            }
        }
        result
    }
}
#[derive(Debug)]
pub(crate) enum UpdateMethod {
    Installer,
    Package(PackageUpdate),
}

pub(crate) fn update_method() -> anyhow::Result<UpdateMethod> {
    update_method_for_exe(&std::env::current_exe()?, &uv_tool_dirs())
}

fn manager_program(name: &str) -> anyhow::Result<PathBuf> {
    crate::terminal::which_bin(name)
        .map(PathBuf::from)
        .ok_or_else(|| {
            anyhow::anyhow!("This installation requires {name}, but it is not available on PATH")
        })
}

fn uv_tool_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(dir) = std::env::var_os("UV_TOOL_DIR").filter(|v| !v.is_empty()) {
        dirs.push(dir.into());
    }
    if let Some(appdata) = std::env::var_os("APPDATA") {
        dirs.push(PathBuf::from(&appdata).join("uv/tools"));
        dirs.push(PathBuf::from(appdata).join("uv/data/tools"));
    }
    if let Some(data) = std::env::var_os("XDG_DATA_HOME") {
        dirs.push(PathBuf::from(data).join("uv/tools"));
    }
    if let Some(home) = dirs::home_dir() {
        dirs.push(home.join(".local/share/uv/tools"));
    }
    if let Some(program) = crate::terminal::which_bin("uv") {
        let mut cmd = Command::new(program);
        hide_console(&mut cmd);
        if let Ok(output) = cmd.args(["tool", "dir"]).stdin(Stdio::null()).output()
            && output.status.success()
        {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path.is_empty() {
                dirs.push(path.into());
            }
        }
    }
    dirs
}

fn update_method_for_exe(exe: &Path, uv_dirs: &[PathBuf]) -> anyhow::Result<UpdateMethod> {
    let resolved = fs::canonicalize(exe).unwrap_or_else(|_| exe.to_path_buf());
    let path = resolved.to_string_lossy().replace('\\', "/");
    if path.contains("/Cellar/") {
        return Ok(UpdateMethod::Package(PackageUpdate {
            program: manager_program("brew")?,
            args: vec!["upgrade".into(), "hcom".into()],
            env: vec![],
            park_entrypoint: None,
            verify_exe: resolved
                .ancestors()
                .find(|p| p.file_name().is_some_and(|n| n == "Cellar"))
                .and_then(Path::parent)
                .map(|p| p.join("bin/hcom")),
        }));
    }
    if let Some((tool_dir, bin_dir)) = uv_owner(&resolved, uv_dirs)? {
        return Ok(UpdateMethod::Package(PackageUpdate {
            program: manager_program("uv")?,
            args: vec!["tool".into(), "upgrade".into(), "hcom".into()],
            env: vec![
                ("UV_TOOL_DIR", tool_dir),
                ("UV_TOOL_BIN_DIR", bin_dir.clone()),
            ],
            park_entrypoint: if cfg!(windows) {
                Some(bin_dir.join("hcom.exe"))
            } else {
                None
            },
            verify_exe: Some(bin_dir.join(if cfg!(windows) { "hcom.exe" } else { "hcom" })),
        }));
    }
    if let Some(owner) = pip_owner(&resolved) {
        return Ok(UpdateMethod::Package(pip_update(&owner)?));
    }
    // An unidentified Python environment must not silently become unmanaged.
    if resolved
        .parent()
        .and_then(Path::parent)
        .is_some_and(|p| p.join("pyvenv.cfg").is_file())
    {
        anyhow::bail!(
            "Could not identify the package manager owning {}; update it through its Python environment",
            exe.display()
        );
    }
    Ok(UpdateMethod::Installer)
}

fn same_path(a: &Path, b: &Path) -> bool {
    matches!((fs::canonicalize(a), fs::canonicalize(b)), (Ok(a), Ok(b)) if a == b)
}

// uv copies executables on some Windows installs. Check the receipt and bytes
// so stale metadata cannot claim a later direct installation.
fn same_binary(a: &Path, b: &Path) -> bool {
    use std::io::Read;
    if same_path(a, b) {
        return true;
    }
    let (Ok(mut a), Ok(mut b)) = (fs::File::open(a), fs::File::open(b)) else {
        return false;
    };
    if !matches!((a.metadata(), b.metadata()), (Ok(a), Ok(b)) if a.len() == b.len()) {
        return false;
    }
    let (mut left, mut right) = ([0u8; 65536], [0u8; 65536]);
    loop {
        let (Ok(n), Ok(m)) = (a.read(&mut left), b.read(&mut right)) else {
            return false;
        };
        if n != m || left[..n] != right[..m] {
            return false;
        }
        if n == 0 {
            return true;
        }
    }
}

fn uv_owner(exe: &Path, dirs: &[PathBuf]) -> anyhow::Result<Option<(PathBuf, PathBuf)>> {
    let Some(root) = exe.parent().and_then(Path::parent) else {
        return Ok(None);
    };
    let candidates = std::iter::once(root.to_path_buf()).chain(dirs.iter().map(|d| d.join("hcom")));
    for root in candidates {
        let Ok(receipt) = fs::read_to_string(root.join("uv-receipt.toml")) else {
            continue;
        };
        let Ok(receipt) = toml::from_str::<toml::Value>(&receipt) else {
            continue;
        };
        let Some(entries) = receipt
            .get("tool")
            .and_then(|v| v.get("entrypoints"))
            .and_then(|v| v.as_array())
        else {
            continue;
        };
        for entry in entries {
            if entry.get("from").and_then(|v| v.as_str()) != Some("hcom") {
                continue;
            }
            let Some(installed) = entry
                .get("install-path")
                .and_then(|v| v.as_str())
                .map(PathBuf::from)
            else {
                continue;
            };
            let Some(binary) = [root.join("Scripts/hcom.exe"), root.join("bin/hcom")]
                .into_iter()
                .find(|p| p.is_file())
            else {
                continue;
            };
            if same_path(exe, &binary) || (same_path(exe, &installed) && same_binary(exe, &binary))
            {
                if let (Some(tools), Some(bin)) = (root.parent(), installed.parent()) {
                    return Ok(Some((tools.to_path_buf(), bin.to_path_buf())));
                }
            } else if same_path(exe, &installed) {
                anyhow::bail!(
                    "The uv receipt owns {}, but it differs from the managed binary. Repair it with `uv tool install --reinstall hcom` or remove the stale uv installation",
                    exe.display()
                );
            }
        }
    }
    Ok(None)
}

#[derive(Debug)]
struct PipOwner {
    site: PathBuf,
    scripts: PathBuf,
}

fn record_path(line: &str) -> Option<String> {
    if !line.starts_with('"') {
        return Some(line.split(',').next()?.to_string());
    }
    let mut chars = line[1..].chars().peekable();
    let mut path = String::new();
    while let Some(c) = chars.next() {
        if c == '"' {
            if chars.peek() == Some(&'"') {
                chars.next();
                path.push('"');
            } else {
                return (chars.next() == Some(',')).then_some(path);
            }
        } else {
            path.push(c);
        }
    }
    None
}

fn site_owns_exe(site: &Path, exe: &Path) -> bool {
    let Ok(entries) = fs::read_dir(site) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        name.starts_with("hcom-")
            && name.ends_with(".dist-info")
            && fs::read_to_string(entry.path().join("RECORD")).is_ok_and(|record| {
                record
                    .lines()
                    .filter_map(record_path)
                    .any(|p| same_path(&site.join(p), exe))
            })
    })
}

fn pip_owner(exe: &Path) -> Option<PipOwner> {
    let scripts = exe.parent()?;
    let prefix = scripts.parent()?;
    let name = scripts.file_name()?.to_str()?;
    if name != "bin" && !name.eq_ignore_ascii_case("scripts") {
        return None;
    }
    let mut sites = vec![
        prefix.join("Lib/site-packages"),
        prefix.join("site-packages"),
    ];
    for lib in [prefix.join("lib"), prefix.join("lib64")] {
        if let Ok(entries) = fs::read_dir(lib) {
            for python in entries.flatten() {
                sites.push(python.path().join("site-packages"));
                sites.push(python.path().join("dist-packages"));
            }
        }
    }
    sites
        .into_iter()
        .find(|site| site_owns_exe(site, exe))
        .map(|site| PipOwner {
            site,
            scripts: scripts.to_path_buf(),
        })
}

const PYTHON_PATH_QUERY: &str = "import json,sys,sysconfig,site; print(json.dumps({'executable':sys.executable,'sites':[sysconfig.get_path('purelib'),sysconfig.get_path('platlib')],'user':site.getusersitepackages()}))";

fn pip_update(owner: &PipOwner) -> anyhow::Result<PackageUpdate> {
    let prefix = owner
        .scripts
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Python installation has no prefix"))?;
    // Preserve virtualenv Python symlinks; resolving them changes the environment.
    let version = owner
        .site
        .parent()
        .and_then(Path::file_name)
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_prefix("python"));
    let mut local = vec![owner.scripts.join("python.exe"), prefix.join("python.exe")];
    if let Some(version) = version {
        local.push(owner.scripts.join(format!("python{version}")));
    }
    local.extend([owner.scripts.join("python3"), owner.scripts.join("python")]);
    if (prefix.join("pyvenv.cfg").is_file()
        || same_path(&owner.site, &prefix.join("Lib/site-packages")))
        && let Some(python) = local.iter().find(|p| p.is_file())
    {
        return Ok(pip_command(python.clone(), false));
    }
    // User installs have no adjacent Python. Verify the actual interpreter's
    // install scheme matches the site whose RECORD owns this executable.
    let hint = python_version_hint(&owner.site);
    let mut probes = vec![
        "python3".to_string(),
        "python".to_string(),
        "py".to_string(),
    ];
    if let Some(hint) = &hint {
        probes.insert(0, format!("python{hint}"));
    }
    let mut candidates: Vec<(PathBuf, Option<String>)> = local
        .into_iter()
        .filter(|p| p.is_file())
        .map(|p| (p, None))
        .collect();
    for name in probes {
        if let Some(program) = crate::terminal::which_bin(&name) {
            candidates.push((
                program.into(),
                if name == "py" {
                    hint.as_ref().map(|h| format!("-{h}"))
                } else {
                    None
                },
            ));
        }
    }
    for (program, selector) in candidates {
        let mut cmd = Command::new(program);
        hide_console(&mut cmd);
        if let Some(selector) = selector {
            cmd.arg(selector);
        }
        let Ok(output) = cmd
            .args(["-c", PYTHON_PATH_QUERY])
            .stdin(Stdio::null())
            .output()
        else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let Ok(paths) = serde_json::from_slice::<serde_json::Value>(&output.stdout) else {
            continue;
        };
        let user = paths
            .get("user")
            .and_then(|v| v.as_str())
            .is_some_and(|p| same_path(Path::new(p), &owner.site));
        let normal = paths
            .get("sites")
            .and_then(|v| v.as_array())
            .is_some_and(|sites| {
                sites
                    .iter()
                    .filter_map(|v| v.as_str())
                    .any(|p| same_path(Path::new(p), &owner.site))
            });
        if (user || normal)
            && let Some(python) = paths.get("executable").and_then(|v| v.as_str())
        {
            return Ok(pip_command(python.into(), user));
        }
    }
    anyhow::bail!(
        "Could not find the Python owning {}; update hcom with that interpreter's pip",
        owner.site.display()
    )
}

fn python_version_hint(site: &Path) -> Option<String> {
    let name = site.parent()?.file_name()?.to_str()?.to_ascii_lowercase();
    let version = name.strip_prefix("python")?;
    if version.contains('.') {
        return Some(version.to_string());
    }
    let mut digits = version.chars();
    let major = digits.next()?;
    let minor: String = digits.collect();
    if major.is_ascii_digit() && !minor.is_empty() && minor.chars().all(|c| c.is_ascii_digit()) {
        Some(format!("{major}.{minor}"))
    } else {
        None
    }
}

fn pip_command(python: PathBuf, user: bool) -> PackageUpdate {
    let mut args = vec!["-m".into(), "pip".into(), "install".into(), "-U".into()];
    if user {
        args.push("--user".into());
    }
    args.push("hcom".into());
    PackageUpdate {
        program: python,
        args,
        env: vec![],
        park_entrypoint: None,
        verify_exe: None,
    }
}

pub(crate) fn windows_installer_program() -> String {
    for name in ["pwsh.exe", "powershell.exe"] {
        if let Some(program) = crate::terminal::which_bin(name) {
            let mut cmd = Command::new(&program);
            hide_console(&mut cmd);
            if cmd
                .args(["-NoProfile", "-Command", "exit 0"])
                .env_remove("PSModulePath")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success())
            {
                return program;
            }
        }
    }
    "powershell.exe".into()
}

pub fn get_update_info() -> Option<(String, &'static str)> {
    if std::env::var_os("HCOM_UPDATE_VERIFY").is_some() {
        return None;
    }
    let flag = flag_path();
    let current = env!("CARGO_PKG_VERSION");
    if !is_fresh(&flag, CHECK_INTERVAL) {
        spawn_background_check(&flag, current);
    }
    let latest = fs::read_to_string(&flag).ok()?.trim().to_string();
    if latest.is_empty() {
        return None;
    }
    if parse_version(current)? >= parse_version(&latest)? {
        atomic_write(&flag, "");
        return None;
    }
    Some((latest, "hcom update"))
}

pub(crate) fn version_reached(actual: &str, required: &str) -> bool {
    matches!((parse_version(actual), parse_version(required)), (Some(actual), Some(required)) if actual >= required)
}

pub fn get_update_notice() -> Option<String> {
    let (latest, _) = get_update_info()?;
    Some(format!("→ hcom v{latest} available — run `hcom update`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_published_releases_only() {
        assert_eq!(
            published_version(br#"{"tag_name":"v0.7.29","draft":false,"prerelease":false}"#),
            Some("0.7.29".into())
        );
        for body in [
            br#"{"tag_name":"v0.8.0-rc1"}"#.as_slice(),
            br#"{"tag_name":"v0.8.0","prerelease":true}"#,
            br#"{"tag_name":"v0.8.0","draft":true}"#,
            br#"{"tag_name":"invalid"}"#,
            b"invalid JSON",
        ] {
            assert_eq!(published_version(body), None);
        }
        assert_eq!(parse_version("1.2.3.4"), None);
        assert!(version_reached("0.7.29", "0.7.29"));
        assert!(!version_reached("0.7.28", "0.7.29"));
        assert!(!version_reached("invalid", "0.7.29"));
        assert_eq!(
            version_from_release_url("https://github.com/aannoo/hcom/releases/tag/v0.7.29"),
            Some("0.7.29".into())
        );
        assert_eq!(
            version_from_release_url("https://github.com/aannoo/hcom/releases/tag/v0.8.0-rc1"),
            None
        );
    }

    #[test]
    fn cache_preserves_notice_on_invalid_fetch_and_clears_on_valid_upgrade() {
        let tmp = tempfile::tempdir().unwrap();
        let flag = tmp.path().join("flags/update_check");
        write_update_cache(&flag, "0.7.28", "0.7.29");
        let modified = flag.metadata().unwrap().modified().unwrap();
        for latest in ["", "invalid", "0.8.0-rc1"] {
            write_update_cache(&flag, "0.7.28", latest);
            assert_eq!(fs::read_to_string(&flag).unwrap(), "0.7.29");
            assert_eq!(flag.metadata().unwrap().modified().unwrap(), modified);
        }
        assert!(claim_check(&flag));
        assert!(!claim_check(&flag));
        assert_eq!(fs::read_to_string(&flag).unwrap(), "0.7.29");
        write_update_cache(&flag, "0.7.29", "0.7.29");
        assert_eq!(fs::read_to_string(&flag).unwrap(), "");
    }

    fn pip_fixture(prefix: &Path, scripts: &str, site: &str, record: &str) -> PathBuf {
        let exe = prefix
            .join(scripts)
            .join(if scripts == "bin" { "hcom" } else { "hcom.exe" });
        let metadata = prefix.join(site).join("hcom-0.7.29.dist-info");
        fs::create_dir_all(exe.parent().unwrap()).unwrap();
        fs::create_dir_all(&metadata).unwrap();
        fs::write(&exe, b"binary").unwrap();
        fs::write(metadata.join("RECORD"), record).unwrap();
        exe
    }

    #[test]
    fn pip_targets_the_owning_venv_even_without_activation() {
        let tmp = tempfile::tempdir().unwrap();
        let prefix = tmp.path().join("arbitrary environment with spaces");
        let exe = pip_fixture(
            &prefix,
            "Scripts",
            "Lib/site-packages",
            "../../Scripts/hcom.exe,sha256=test,6\n",
        );
        let python = prefix.join("Scripts/python.exe");
        fs::write(&python, b"interpreter").unwrap();
        let UpdateMethod::Package(update) = update_method_for_exe(&exe, &[]).unwrap() else {
            panic!("not a package install")
        };
        // Canonicalizing the installed binary must not change the selected
        // interpreter or route the upgrade through an unrelated PATH pip.
        assert!(same_path(&update.program, &python));
        assert_eq!(update.args, ["-m", "pip", "install", "-U", "hcom"]);
    }

    #[test]
    fn pip_user_and_termux_records_identify_the_executable() {
        let tmp = tempfile::tempdir().unwrap();
        let user = pip_fixture(
            &tmp.path().join("Python313"),
            "Scripts",
            "site-packages",
            "../Scripts/hcom.exe,sha256=test,6\n",
        );
        assert!(pip_owner(&user).is_some());
        assert_eq!(
            python_version_hint(&tmp.path().join("Python313/site-packages")),
            Some("3.13".into())
        );
        let termux = pip_fixture(
            &tmp.path().join("usr"),
            "bin",
            "lib/python3.13/site-packages",
            "../../../bin/hcom,sha256=test,6\n",
        );
        assert!(pip_owner(&termux).is_some());
        fs::write(
            tmp.path()
                .join("Python313/site-packages/hcom-0.7.29.dist-info/RECORD"),
            "../Scripts/other.exe,sha256=test,6\n",
        )
        .unwrap();
        assert!(pip_owner(&user).is_none());
        assert_eq!(
            record_path("\"../bin/with,comma\",hash,6"),
            Some("../bin/with,comma".into())
        );
    }

    #[test]
    fn uv_receipt_recognizes_custom_directories_and_windows_copies() {
        let tmp = tempfile::tempdir().unwrap();
        let tools = tmp.path().join("custom tools");
        let root = tools.join("hcom");
        let binary = root.join("Scripts/hcom.exe");
        let installed = tmp.path().join("custom bin/hcom.exe");
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::create_dir_all(installed.parent().unwrap()).unwrap();
        fs::write(&binary, b"uv binary").unwrap();
        fs::copy(&binary, &installed).unwrap();
        fs::write(
            root.join("uv-receipt.toml"),
            format!(
                "[tool]\nentrypoints = [{{ name = 'hcom', from = 'hcom', install-path = '{}' }}]\n",
                installed.to_string_lossy().replace('\\', "/")
            ),
        )
        .unwrap();
        assert_eq!(
            uv_owner(&binary, &[]).unwrap(),
            Some((tools.clone(), installed.parent().unwrap().to_path_buf()))
        );
        assert_eq!(
            uv_owner(&installed, std::slice::from_ref(&tools)).unwrap(),
            Some((tools.clone(), installed.parent().unwrap().to_path_buf()))
        );
        fs::write(&installed, b"unrelated direct binary").unwrap();
        assert!(uv_owner(&installed, &[tools]).is_err());
    }

    #[test]
    fn unowned_python_environment_is_not_replaced_by_the_installer() {
        let tmp = tempfile::tempdir().unwrap();
        let exe = tmp.path().join("Scripts/hcom.exe");
        fs::create_dir_all(exe.parent().unwrap()).unwrap();
        fs::write(&exe, b"binary").unwrap();
        fs::write(tmp.path().join("pyvenv.cfg"), "home = unknown").unwrap();
        assert!(update_method_for_exe(&exe, &[]).is_err());
    }

    #[test]
    fn package_manager_launch_failure_restores_parked_entrypoint() {
        let tmp = tempfile::tempdir().unwrap();
        let entry = tmp.path().join("hcom.exe");
        fs::write(&entry, b"old executable").unwrap();
        let update = PackageUpdate {
            program: tmp.path().join("missing-manager.exe"),
            args: vec![],
            env: vec![],
            park_entrypoint: Some(entry.clone()),
            verify_exe: None,
        };
        assert!(update.execute(update.command()).is_err());
        assert_eq!(fs::read(&entry).unwrap(), b"old executable");
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
    }

    #[cfg(windows)]
    #[test]
    fn failed_upgrade_keeps_entrypoint_available_and_restores_partial_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let entry = tmp.path().join("hcom.exe");
        fs::write(&entry, b"old executable").unwrap();
        let update = PackageUpdate {
            program: windows_installer_program().into(),
            args: vec![],
            env: vec![],
            park_entrypoint: Some(entry.clone()),
            verify_exe: None,
        };
        let mut command = update.command();
        command.args(["-NoProfile", "-Command", "if (-not (Test-Path -LiteralPath $env:HCOM_UPDATE_TEST_ENTRY)) { exit 14 }; Set-Content -LiteralPath $env:HCOM_UPDATE_TEST_ENTRY -Value partial; exit 13"])
            .env_remove("PSModulePath").env("HCOM_UPDATE_TEST_ENTRY", &entry);
        let error = update.execute(command).unwrap_err();
        assert!(error.to_string().contains("exit 13"), "{error:#}");
        assert_eq!(fs::read(&entry).unwrap(), b"old executable");
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "creates isolated pip/uv installations and upgrades a running executable using local wheels"]
    fn live_package_managers_upgrade_running_windows_binary() {
        use std::io::BufRead;

        let python = manager_program("python").unwrap();
        let uv = manager_program("uv").unwrap();
        for manager in ["pip", "uv"] {
            let tmp = tempfile::tempdir().unwrap();
            let wheels = tmp.path().join("wheels");
            fs::create_dir_all(&wheels).unwrap();
            build_fixture_wheel(&python, &wheels, "0.0.1");
            let tools = tmp.path().join("custom tools");
            let bin = tmp.path().join("custom bin");
            let exe = if manager == "pip" {
                let venv = tmp.path().join("unactivated pip environment");
                assert!(
                    Command::new(&python)
                        .args(["-m", "venv"])
                        .arg(&venv)
                        .status()
                        .unwrap()
                        .success()
                );
                assert!(
                    Command::new(venv.join("Scripts/python.exe"))
                        .args(["-m", "pip", "install", "--no-index", "--find-links"])
                        .arg(&wheels)
                        .arg("hcom")
                        .status()
                        .unwrap()
                        .success()
                );
                venv.join("Scripts/hcom.exe")
            } else {
                assert!(
                    Command::new(&uv)
                        .args(["tool", "install", "--no-index", "--find-links"])
                        .arg(&wheels)
                        .arg("--python")
                        .arg(&python)
                        .arg("hcom")
                        .env("UV_TOOL_DIR", &tools)
                        .env("UV_TOOL_BIN_DIR", &bin)
                        .env("UV_CACHE_DIR", tmp.path().join("uv-cache"))
                        .env("UV_PYTHON_DOWNLOADS", "never")
                        .status()
                        .unwrap()
                        .success()
                );
                bin.join("hcom.exe")
            };
            build_fixture_wheel(&python, &wheels, "0.0.2");
            let UpdateMethod::Package(update) =
                update_method_for_exe(&exe, std::slice::from_ref(&tools)).unwrap()
            else {
                panic!("not a managed installation")
            };
            let mut child = Command::new(&exe)
                .args([
                    "--exact",
                    "commands::update::tests::hold_running_binary",
                    "--ignored",
                    "--nocapture",
                ])
                .env("HCOM_TEST_HOLD_BINARY", "1")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
            for line in (&mut stdout).lines() {
                if line.unwrap().contains("holding binary") {
                    break;
                }
            }
            let mut command = update.command();
            command
                .env("PIP_NO_INDEX", "1")
                .env("PIP_FIND_LINKS", &wheels)
                .env("UV_NO_INDEX", "1")
                .env("UV_FIND_LINKS", &wheels)
                .env("UV_CACHE_DIR", tmp.path().join("uv-cache"))
                .env("UV_PYTHON_DOWNLOADS", "never");
            let result = update.execute(command);
            drop(child.stdin.take());
            assert!(child.wait().unwrap().success());
            result.unwrap();
            assert!(fs::read(&exe).unwrap().ends_with(b"fixture version:0.0.2"));
            println!("{manager}: upgraded the running executable in its owning installation");
        }
    }

    #[cfg(windows)]
    fn build_fixture_wheel(python: &Path, wheels: &Path, version: &str) {
        const BUILD_WHEEL: &str = r#"
import csv, io, pathlib, sys, zipfile
payload, directory, version = sys.argv[1:]
dist = f'hcom-{version}.dist-info'
files = {
    f'hcom-{version}.data/scripts/hcom.exe': pathlib.Path(payload).read_bytes() + b'fixture version:' + version.encode(),
    f'{dist}/METADATA': f'Metadata-Version: 2.1\nName: hcom\nVersion: {version}\n'.encode(),
    f'{dist}/WHEEL': b'Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: py3-none-any\n',
}
record = io.StringIO()
writer = csv.writer(record)
for name in files:
    writer.writerow([name, '', ''])
writer.writerow([f'{dist}/RECORD', '', ''])
files[f'{dist}/RECORD'] = record.getvalue().encode()
with zipfile.ZipFile(pathlib.Path(directory) / f'hcom-{version}-py3-none-any.whl', 'w') as wheel:
    for name, content in files.items():
        wheel.writestr(name, content)
"#;
        assert!(
            Command::new(python)
                .args(["-c", BUILD_WHEEL])
                .arg(std::env::current_exe().unwrap())
                .arg(wheels)
                .arg(version)
                .status()
                .unwrap()
                .success()
        );
    }
}
