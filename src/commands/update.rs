//! `hcom update` command — check and apply updates.
//!
//! Uses the shared `fetch_update_info()` function from update.rs to get current,
//! latest, and availability in one call. Applies immediately when an update is
//! available; `--check` reports availability without applying.

use crate::db::HcomDb;
use crate::shared::CommandContext;

#[derive(clap::Parser, Debug)]
#[command(name = "update", about = "Check for and apply updates")]
pub struct UpdateArgs {
    /// Only check — print update status without applying
    #[arg(long)]
    pub check: bool,
}

fn print_dev_root_notice(db: &HcomDb) {
    if let Some((path, source)) = crate::router::resolve_effective_dev_root(db.path()) {
        println!("Using local build: {} [{}]", path.display(), source);
        println!("`hcom update` bypasses dev_root and updates the binary you invoked.");
        println!("The local checkout is not changed.");
        println!();
    }
}

pub fn cmd_update(_db: &HcomDb, args: &UpdateArgs, _ctx: Option<&CommandContext>) -> i32 {
    println!("Checking for updates...");
    print_dev_root_notice(_db);

    let info = match crate::update::fetch_update_info() {
        Ok(i) => i,
        Err(e) => {
            eprintln!("Error: {e}");
            return 1;
        }
    };

    if !info.available {
        println!("hcom v{} is up to date", info.current);
        // Clear stale "update available" cache if it existed
        let _ = crate::paths::atomic_write(&crate::update::flag_path(), "");
        return 0;
    }

    println!("Update available: v{} → v{}", info.current, info.latest);
    crate::paths::atomic_write(&crate::update::flag_path(), &info.latest);

    if args.check {
        println!("Run `hcom update` to apply.");
        return 0;
    }

    let result: anyhow::Result<()> = (|| {
        let mut exe = std::env::current_exe()?;
        match crate::update::update_method()? {
            crate::update::UpdateMethod::Package(update) => {
                println!(
                    "Running: {} {}",
                    update.program.display(),
                    update.args.join(" ")
                );
                for (key, value) in &update.env {
                    println!("  {key}={}", value.display());
                }
                update.execute(update.command())?;
                if let Some(entrypoint) = update.verify_exe {
                    exe = entrypoint;
                }
            }
            crate::update::UpdateMethod::Installer if cfg!(windows) => {
                install_windows_update(&info.latest)?
            }
            crate::update::UpdateMethod::Installer => install_unix_update(&info.latest)?,
        }
        verify_updated_binary(&exe, &info.latest)?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            // Clear the cached "update available" notice
            let _ = crate::paths::atomic_write(&crate::update::flag_path(), "");
            println!("Done. Run 'hcom --version' to confirm.");
            0
        }
        Err(e) => {
            eprintln!("Error: {e:#}");
            1
        }
    }
}

fn verify_updated_binary(exe: &std::path::Path, required: &str) -> anyhow::Result<()> {
    let state = tempfile::tempdir()?;
    let output = std::process::Command::new(exe)
        .arg("--version")
        .env("HCOM_DEV_ROOT", "")
        .env("HCOM_DIR", state.path())
        .env("HCOM_UPDATE_VERIFY", "1")
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "Could not verify the updated binary"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let actual = stdout.trim().strip_prefix("hcom ").unwrap_or("");
    anyhow::ensure!(
        crate::update::version_reached(actual, required),
        "The updater completed, but {} reports version '{}' instead of v{required} or newer. The package release may still be publishing; retry later",
        exe.display(),
        actual
    );
    Ok(())
}

/// Stage the official installer output before touching the invoked binary.
/// A running Windows executable cannot be overwritten, but can be renamed.
fn install_windows_update(version: &str) -> anyhow::Result<()> {
    use anyhow::Context;

    let dir = stage_windows_update(version)?;
    let exe = std::env::current_exe().context("Could not locate the invoked binary")?;
    replace_binary(&dir.path().join("stage/hcom.exe"), &exe)
        .with_context(|| format!("Could not replace {}", exe.display()))?;
    Ok(())
}

fn stage_windows_update(version: &str) -> anyhow::Result<tempfile::TempDir> {
    let program = crate::update::windows_installer_program();
    stage_windows_update_with_program(version, &program)
}

fn stage_windows_update_with_program(
    version: &str,
    program: &str,
) -> anyhow::Result<tempfile::TempDir> {
    use anyhow::Context;

    let dir = tempfile::tempdir().context("Could not create update staging directory")?;
    let script = dir.path().join("update.ps1");
    std::fs::write(&script, WINDOWS_UPDATE_SCRIPT)?;
    println!("Downloading hcom v{version} into staging with {program}");
    let status = std::process::Command::new(program)
        .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(&script)
        .args(["-Release", version])
        // Windows PowerShell 5.1 must rebuild its own module path when
        // launched from pwsh. Staging must ignore ambient install overrides.
        .env_remove("PSModulePath")
        .env_remove("HCOM_INSTALL_DIR")
        .env_remove("CARGO_DIST_FORCE_INSTALL_DIR")
        .env("HCOM_UNMANAGED_INSTALL", dir.path().join("stage"))
        .env("HCOM_NO_MODIFY_PATH", "1")
        .env("HCOM_DISABLE_UPDATE", "1")
        .status()
        .with_context(|| format!("Could not run Windows installer with {program}"))?;
    anyhow::ensure!(
        status.success(),
        "Windows installer failed (exit {})",
        status.code().unwrap_or(-1)
    );

    Ok(dir)
}

const WINDOWS_UPDATE_SCRIPT: &str = r#"
param([ValidatePattern('^\d+\.\d+\.\d+$')][string]$Release)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    $installer = Join-Path $PSScriptRoot 'installer.ps1'
    $url = 'https://github.com/aannoo/hcom/releases/download/v{0}/hcom-installer.ps1' -f $Release
    Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $installer
    & $installer
    if ($LASTEXITCODE) { exit $LASTEXITCODE }
} catch {
    Write-Error $_ -ErrorAction Continue
    exit 1
}
"#;

fn install_unix_update(version: &str) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let script = dir.path().join("installer.sh");
    let url =
        format!("https://github.com/aannoo/hcom/releases/download/v{version}/hcom-installer.sh");
    let status = std::process::Command::new("curl")
        .args(["-fsSL", "--max-time", "30", "-o"])
        .arg(&script)
        .arg(&url)
        .status()?;
    anyhow::ensure!(status.success(), "Could not download the hcom installer");
    let exe = std::env::current_exe()?;
    let parent = exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Binary has no parent directory"))?;
    let status = std::process::Command::new("sh")
        .arg(&script)
        .env("HCOM_INSTALL_DIR", parent)
        .env("HCOM_NO_MODIFY_PATH", "1")
        .env("HCOM_DISABLE_UPDATE", "1")
        .status()?;
    anyhow::ensure!(
        status.success(),
        "Installer failed (exit {})",
        status.code().unwrap_or(-1)
    );
    Ok(())
}

fn replace_binary(staged: &std::path::Path, target: &std::path::Path) -> std::io::Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| std::io::Error::other("binary has no parent directory"))?;
    // Copy onto the destination filesystem first. Download/copy failures leave
    // the installed executable in place, and the final rename is atomic.
    let replacement = tempfile::NamedTempFile::new_in(parent)?;
    std::fs::copy(staged, replacement.path())?;
    replacement.as_file().sync_all()?;
    let name = target
        .file_name()
        .ok_or_else(|| std::io::Error::other("binary has no filename"))?
        .to_string_lossy();
    let prefix = format!("{name}.update-old-");
    // Only remove our previous backups; mapped executables refuse deletion
    // until their processes exit, so those copies stay available to live agents.
    for entry in std::fs::read_dir(parent)?.flatten() {
        if entry.file_name().to_string_lossy().starts_with(&prefix) && entry.file_type()?.is_file()
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    let backup = parent.join(format!("{prefix}{}", uuid::Uuid::new_v4()));
    std::fs::rename(target, &backup)?;
    if let Err(error) = replacement.persist(target) {
        if let Err(rollback) = std::fs::rename(&backup, target) {
            return Err(std::io::Error::other(format!(
                "replacement failed: {}; rollback failed: {rollback}; previous binary is at {}",
                error.error,
                backup.display()
            )));
        }
        return Err(error.error);
    }
    let _ = std::fs::remove_file(backup);
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn update_args_default() {
        let args = UpdateArgs::try_parse_from(["update"]).unwrap();
        assert!(!args.check);
    }

    #[test]
    fn update_args_check_flag() {
        let args = UpdateArgs::try_parse_from(["update", "--check"]).unwrap();
        assert!(args.check);
    }

    #[test]
    fn print_dev_root_notice_is_safe_when_unset() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::db::HcomDb::open_at(&dir.path().join("hcom.db")).unwrap();
        print_dev_root_notice(&db);
    }

    #[test]
    fn binary_replacement_preserves_install_on_staging_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("hcom.exe");
        std::fs::write(&target, b"old binary").unwrap();
        assert!(replace_binary(&tmp.path().join("missing.exe"), &target).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"old binary");
    }

    #[test]
    fn binary_replacement_installs_staged_binary_and_cleans_old_backups() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("hcom.exe");
        let staged = tmp.path().join("staged.exe");
        let stale = tmp.path().join("hcom.exe.update-old-stale");
        let unrelated = tmp.path().join("other.exe");
        std::fs::write(&target, b"old binary").unwrap();
        std::fs::write(&staged, b"new binary").unwrap();
        std::fs::write(&stale, b"previous backup").unwrap();
        std::fs::write(&unrelated, b"unrelated").unwrap();
        replace_binary(&staged, &target).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new binary");
        assert!(!stale.exists());
        assert!(unrelated.exists());
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 3);
    }

    #[cfg(windows)]
    #[test]
    fn binary_replacement_works_while_old_executable_is_running() {
        use std::io::BufRead;
        use std::process::{Command, Stdio};

        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("hcom.exe");
        let staged = tmp.path().join("staged.exe");
        std::fs::copy(std::env::current_exe().unwrap(), &target).unwrap();
        std::fs::write(&staged, b"replacement").unwrap();
        let mut child = Command::new(&target)
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
        let result = replace_binary(&staged, &target);
        // Releasing stdin allows the held test process to exit and its backup
        // to become deletable. Do this before assertions to avoid orphaning it.
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success());
        result.unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"replacement");
        let backup = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("hcom.exe.update-old-")
            })
            .unwrap();
        std::fs::remove_file(backup.path()).unwrap();
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "helper subprocess for the running executable replacement test"]
    fn hold_running_binary() {
        if std::env::var_os("HCOM_TEST_HOLD_BINARY").is_none() {
            return;
        }
        println!("holding binary");
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "downloads and runs the latest official Windows installer"]
    fn windows_installer_live_smoke() {
        let version = crate::update::fetch_update_info().unwrap().latest;
        let mut programs = vec![crate::update::windows_installer_program()];
        if let Some(powershell) = crate::terminal::which_bin("powershell.exe")
            && !programs.contains(&powershell)
        {
            programs.push(powershell);
        }
        for program in programs {
            let dir = stage_windows_update_with_program(&version, &program).unwrap();
            let output = std::process::Command::new(dir.path().join("stage/hcom.exe"))
                .arg("--version")
                .env("HCOM_DIR", dir.path().join("isolated-hcom"))
                .env_remove("HCOM_DEV_ROOT")
                .output()
                .unwrap();
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8_lossy(&output.stdout).trim(),
                format!("hcom {version}")
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_wrapper_propagates_installer_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let fixture = tmp.path().join("failed-installer.ps1");
        std::fs::write(&fixture, "exit 13").unwrap();
        // Mock only the HTTP transport; execute the production wrapper and
        // its nested installer call unchanged.
        let script = WINDOWS_UPDATE_SCRIPT.replace(
            "$ProgressPreference = 'SilentlyContinue'",
            "$ProgressPreference = 'SilentlyContinue'\nfunction Invoke-WebRequest { param([switch]$UseBasicParsing, [string]$Uri, [string]$OutFile) Copy-Item -LiteralPath $env:HCOM_INSTALLER_FIXTURE -Destination $OutFile }",
        );
        let wrapper = tmp.path().join("wrapper.ps1");
        std::fs::write(&wrapper, script).unwrap();
        let status = std::process::Command::new(crate::update::windows_installer_program())
            .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
            .arg(wrapper)
            .args(["-Release", "0.0.0"])
            .env_remove("PSModulePath")
            .env("HCOM_INSTALLER_FIXTURE", fixture)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(13));
    }
}
