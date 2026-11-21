use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::Deserialize;

use super::{Fingerprint, Profile};

const TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Deserialize)]
struct Registration {
    name: String,
    endpoint: String,
    auth: String,
}

#[derive(Deserialize)]
struct GatewayInfo {
    gateway: String,
    server: String,
    status: String,
    version: String,
    compute_drivers: Vec<Driver>,
}

#[derive(Deserialize)]
struct Driver {
    name: String,
    capabilities: Capabilities,
}

#[derive(Deserialize)]
struct Capabilities {
    driver_name: String,
    driver_version: String,
}

pub(super) fn check(profile: &Profile, expected: &Fingerprint) -> Result<()> {
    let mut version = profile.base_command();
    version.arg("--version");
    let version = output(version, "version", TIMEOUT)?;
    ensure!(
        version.trim() == expected.cli_version,
        "OpenShell CLI version differs from qualification"
    );

    for (args, required) in [
        (
            &["gateway", "info", "--help"][..],
            &["--gateway", "--gateway-endpoint", "--workspace", "--output"][..],
        ),
        (
            &["sandbox", "exec", "--help"][..],
            &["--no-tty", "--no-login-shell", "--env", "--name"][..],
        ),
        (
            &["sandbox", "upload", "--help"][..],
            &["--no-git-ignore"][..],
        ),
        (&["sandbox", "download", "--help"][..], &["--workspace"][..]),
    ] {
        let mut command = profile.base_command();
        command.args(args);
        let help = output(command, args[1], TIMEOUT)?;
        for flag in required {
            ensure!(
                help.lines().any(|line| {
                    let mut columns = line.split_whitespace();
                    let Some(first) = columns.next() else {
                        return false;
                    };
                    first == *flag
                        || (first.starts_with('-')
                            && first.ends_with(',')
                            && columns.next() == Some(*flag))
                }),
                "OpenShell {} lacks required flag {flag}",
                args[1]
            );
        }
    }

    // Validate the local registration before any RPC can use its credentials.
    let mut list = profile.base_command();
    list.args(["--color", "never", "gateway", "list", "--output", "json"]);
    let registrations: Vec<Registration> =
        serde_json::from_str(&output(list, "gateway registration", TIMEOUT)?)
            .context("invalid OpenShell gateway registration output")?;
    let mut selected = registrations
        .iter()
        .filter(|record| record.name == profile.gateway);
    let record = selected
        .next()
        .context("selected OpenShell gateway is not registered")?;
    ensure!(
        selected.next().is_none(),
        "selected OpenShell gateway has duplicate registrations"
    );
    ensure!(
        record.endpoint.trim_end_matches('/') == profile.endpoint,
        "OpenShell gateway registration endpoint differs from profile"
    );
    let auth = if profile.endpoint.starts_with("https://") {
        "mtls"
    } else {
        "plaintext"
    };
    ensure!(
        record.auth == auth,
        "OpenShell gateway authentication differs from profile transport"
    );

    let mut info = profile.command();
    info.args(["gateway", "info", "--output", "json"]);
    let info: GatewayInfo =
        serde_json::from_str(&output(info, "authenticated gateway/driver", TIMEOUT)?)
            .context("invalid OpenShell gateway runtime output")?;
    ensure!(
        info.gateway == profile.gateway && info.server.trim_end_matches('/') == profile.endpoint,
        "OpenShell gateway response differs from profile"
    );
    ensure!(
        info.status == "healthy" && info.version == expected.gateway_version,
        "OpenShell gateway health or version differs from qualification"
    );
    ensure!(
        info.compute_drivers.len() == 1,
        "OpenShell gateway must report one qualified compute driver"
    );
    let driver = &info.compute_drivers[0];
    ensure!(
        driver.name == "vm"
            && driver.capabilities.driver_name == expected.driver_name
            && driver.capabilities.driver_version == expected.driver_version,
        "OpenShell VM driver differs from qualification"
    );
    let mut list = profile.command();
    list.args(["sandbox", "list", "--page-size", "1", "--output", "json"]);
    let workspace: serde_json::Value =
        serde_json::from_str(&output(list, "workspace access", TIMEOUT)?)
            .context("invalid OpenShell workspace probe output")?;
    ensure!(
        workspace
            .get("sandboxes")
            .is_some_and(serde_json::Value::is_array),
        "OpenShell workspace probe returned no sandbox list"
    );
    Ok(())
}

#[cfg(unix)]
fn output(mut command: Command, label: &str, timeout: Duration) -> Result<String> {
    use std::io::{ErrorKind, Read};
    use std::process::Stdio;
    use std::time::Instant;

    use nix::fcntl::{FcntlArg, OFlag, fcntl};

    const LIMIT: usize = 128 * 1024;

    fn nonblocking(fd: &impl std::os::fd::AsFd) -> Result<()> {
        let flags = OFlag::from_bits_truncate(fcntl(fd, FcntlArg::F_GETFL)?);
        fcntl(fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
        Ok(())
    }

    fn drain(pipe: &mut impl Read, data: &mut Vec<u8>) -> Result<bool> {
        let mut buf = [0; 8192];
        loop {
            match pipe.read(&mut buf) {
                Ok(0) => return Ok(true),
                Ok(n) => {
                    ensure!(
                        data.len() + n <= LIMIT,
                        "OpenShell probe exceeds the capture limit"
                    );
                    data.extend_from_slice(&buf[..n]);
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(false),
                Err(e) => return Err(e.into()),
            }
        }
    }

    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = crate::sys::process::spawn_detached(&mut command)
        .with_context(|| format!("start OpenShell {label} probe"))?;
    let result = (|| {
        let mut stdout = child.stdout.take().context("missing probe stdout")?;
        let mut stderr = child.stderr.take().context("missing probe stderr")?;
        nonblocking(&stdout)?;
        nonblocking(&stderr)?;
        let deadline = Instant::now() + timeout;
        let mut output = Vec::new();
        let mut diagnostics = Vec::new();
        loop {
            let out_eof = drain(&mut stdout, &mut output)?;
            let err_eof = drain(&mut stderr, &mut diagnostics)?;
            if let Some(status) = child.try_wait()?
                && out_eof
                && err_eof
            {
                // Diagnostics may contain profile details. Return the probe and
                // status without echoing stderr into an hcom event.
                ensure!(
                    status.success(),
                    "OpenShell {label} probe failed ({status})"
                );
                return Ok(String::from_utf8(output)?);
            }
            ensure!(
                Instant::now() < deadline,
                "OpenShell {label} probe timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    })();
    if result.is_err() && !matches!(child.try_wait(), Ok(Some(_))) {
        crate::sys::process::kill_child_group(&mut child);
        let _ = child.wait();
    }
    result
}

#[cfg(not(unix))]
fn output(_: Command, _: &str, _: Duration) -> Result<String> {
    anyhow::bail!("the initial OpenShell VM adapter requires a Unix host")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn probe_capture_and_wait_are_bounded() {
        let mut noisy = Command::new("/bin/sh");
        noisy.args(["-c", "while :; do printf '0123456789abcdef'; done"]);
        let error = output(noisy, "fixture", Duration::from_secs(2)).unwrap_err();
        assert!(error.to_string().contains("capture limit"));

        let mut stalled = Command::new("/bin/sh");
        stalled.args(["-c", "sleep 30"]);
        let error = output(stalled, "fixture", Duration::from_millis(80)).unwrap_err();
        assert!(error.to_string().contains("timed out"));
    }
}
