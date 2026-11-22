//! Profile-bound OpenShell CLI invocations for the Linux VM runtime.

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::directory::Worker;
use super::protocol::{MAX_ENCODED_FRAME, validate_identifier};

mod probe;

pub const ROTATION_BYTES: u64 = 2 * 1024 * 1024;
pub const CONTROL_HEADROOM_BYTES: u64 = 64 * 1024;
const MIN_INPUT_BYTES: u64 = ROTATION_BYTES + MAX_ENCODED_FRAME as u64 + CONTROL_HEADROOM_BYTES;
const ROUTE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub cli: PathBuf,
    pub gateway: String,
    pub endpoint: String,
    pub workspace: String,
    pub config_root: PathBuf,
    pub state_root: PathBuf,
    pub gateway_binary: PathBuf,
    pub driver_binary: PathBuf,
    pub gateway_config: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Fingerprint {
    pub cli_sha256: String,
    pub gateway_sha256: String,
    pub driver_sha256: String,
    pub configuration_sha256: String,
    pub gateway: String,
    pub endpoint: String,
    pub workspace: String,
    pub cli_version: String,
    pub gateway_version: String,
    pub driver_name: String,
    pub driver_version: String,
    pub vm: VmAllocation,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VmAllocation {
    pub vcpus: u8,
    pub mem_mib: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Qualification {
    pub fingerprint: Fingerprint,
    pub measured_stdin_bytes: Option<u64>,
}

pub struct Adapter {
    profile: Profile,
    cli_stamp: FileStamp,
    stdin_bytes: u64,
}

#[derive(Deserialize)]
struct SandboxRoute {
    id: String,
    name: String,
    workspace: String,
    phase: String,
}

impl Adapter {
    pub fn preflight(profile: Profile, qualification: &Qualification) -> Result<Self> {
        let stdin_bytes = qualification
            .measured_stdin_bytes
            .context("unknown OpenShell stdin allowance")?;
        ensure!(
            stdin_bytes >= MIN_INPUT_BYTES,
            "qualified OpenShell stdin allowance has insufficient headroom"
        );
        profile.validate()?;
        let cli_stamp = file_stamp(&profile.cli)?;
        let expected = &qualification.fingerprint;
        ensure!(
            profile.gateway == expected.gateway
                && profile.endpoint == expected.endpoint
                && profile.workspace == expected.workspace,
            "OpenShell qualification belongs to another gateway or workspace"
        );
        ensure!(
            hash_file(&profile.cli)? == expected.cli_sha256,
            "OpenShell CLI digest differs from qualification"
        );
        ensure!(
            hash_file(&profile.gateway_binary)? == expected.gateway_sha256,
            "OpenShell gateway digest differs from qualification"
        );
        ensure!(
            hash_file(&profile.driver_binary)? == expected.driver_sha256,
            "OpenShell driver digest differs from qualification"
        );
        let config = read_config(&profile.gateway_config)?;
        ensure!(
            hash_bytes(&config) == expected.configuration_sha256,
            "OpenShell gateway configuration differs from qualification"
        );
        let vm = vm_allocation(&config, &profile.endpoint)?;
        ensure!(
            vm == expected.vm,
            "OpenShell VM allocation differs from qualification"
        );
        ensure!(
            file_stamp(&profile.cli)? == cli_stamp,
            "OpenShell executable changed during fingerprinting"
        );
        // Gateway info reports runtime versions and driver identity. The
        // artifact hashes above cover the operator-supplied local files.
        probe::check(&profile, expected)?;
        ensure!(
            file_stamp(&profile.cli)? == cli_stamp,
            "OpenShell executable changed during preflight"
        );
        Ok(Self {
            profile,
            cli_stamp,
            stdin_bytes,
        })
    }

    pub fn measured_stdin_bytes(&self) -> u64 {
        self.stdin_bytes
    }

    /// The pinned CLI resolves an exec name again when starting the command.
    /// This check catches stale placement records before any bridge data is sent.
    pub fn verify_owned_target(&self, name: &str, worker: &Worker) -> Result<()> {
        validate_routable_name(name)?;
        ensure!(
            worker.gateway == self.profile.gateway && worker.workspace == self.profile.workspace,
            "OpenShell adapter does not match the owned gateway and workspace"
        );
        self.check_cli_stamp()?;
        let mut command = self.profile.command();
        command.args(["sandbox", "get", "--output", "json", name]);
        let response = probe::output(command, "owned sandbox lookup", ROUTE_LOOKUP_TIMEOUT)?;
        let route: SandboxRoute =
            serde_json::from_str(&response).context("invalid OpenShell sandbox lookup response")?;
        ensure!(
            route.id == worker.binding.sandbox
                && route.name == name
                && route.workspace == worker.workspace,
            "OpenShell sandbox name no longer resolves to the owned ID and workspace"
        );
        ensure!(
            route.phase == "Ready",
            "owned OpenShell sandbox is not Ready"
        );
        Ok(())
    }

    /// The lifecycle layer resolves and confirms the owned sandbox ID before
    /// supplying its current name. The argument vector is owned by the host.
    pub fn managed_exec(
        &self,
        sandbox_name: &str,
        guest_program: &str,
        args: &[OsString],
    ) -> Result<Command> {
        validate_routable_name(sandbox_name)?;
        ensure!(
            guest_program.starts_with('/') && !guest_program.chars().any(char::is_control),
            "managed OpenShell executable must be an absolute guest path"
        );
        self.check_cli_stamp()?;
        let mut command = self.profile.command();
        command
            .args([
                "sandbox",
                "exec",
                "--no-tty",
                "--no-login-shell",
                "--env",
                "BASH_ENV=",
                "--env",
                "ENV=",
                "--name",
                sandbox_name,
                "--",
                guest_program,
            ])
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok(command)
    }

    fn check_cli_stamp(&self) -> Result<()> {
        ensure!(
            file_stamp(&self.profile.cli)? == self.cli_stamp,
            "OpenShell executable changed after preflight"
        );
        Ok(())
    }
}

impl Profile {
    fn validate(&self) -> Result<()> {
        validate_arg_label(&self.gateway)?;
        validate_routable_name(&self.workspace)?;
        let authority = self
            .endpoint
            .strip_prefix("https://")
            .or_else(|| self.endpoint.strip_prefix("http://"))
            .context("OpenShell endpoint must use HTTPS or the qualified loopback fixture")?;
        ensure!(
            !authority.is_empty()
                && !authority
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control() || "/\\@?#".contains(c)),
            "OpenShell endpoint must be an authority without credentials or a URL path"
        );
        if self.endpoint.starts_with("http://") {
            ensure!(
                authority
                    .parse::<SocketAddr>()
                    .is_ok_and(|address| address.ip().is_loopback() && address.port() != 0),
                "plaintext OpenShell is limited to explicit loopback fixtures"
            );
        }
        for path in [
            &self.cli,
            &self.gateway_binary,
            &self.driver_binary,
            &self.gateway_config,
        ] {
            ensure!(
                path.is_absolute() && path.is_file(),
                "OpenShell runtime path must name an absolute regular file: {}",
                path.display()
            );
        }
        for path in [&self.config_root, &self.state_root] {
            ensure!(
                path.is_absolute() && path.is_dir(),
                "OpenShell CLI root must be an absolute directory: {}",
                path.display()
            );
        }
        Ok(())
    }

    fn base_command(&self) -> Command {
        let mut command = Command::new(&self.cli);
        command
            .env_clear()
            .env("XDG_CONFIG_HOME", &self.config_root)
            .env("XDG_CONFIG_DIRS", &self.config_root)
            .env("XDG_STATE_HOME", &self.state_root)
            .env("XDG_CACHE_HOME", &self.state_root)
            .env("OPENSHELL_TELEMETRY_ENABLED", "false")
            .env("LANG", "C")
            .current_dir(&self.state_root);
        command
    }

    fn command(&self) -> Command {
        let mut command = self.base_command();
        command.args([
            "--gateway",
            &self.gateway,
            "--gateway-endpoint",
            &self.endpoint,
            "--workspace",
            &self.workspace,
            "--color",
            "never",
        ]);
        command
    }
}

fn validate_arg_label(label: &str) -> Result<()> {
    validate_identifier(label)?;
    ensure!(
        label.as_bytes()[0].is_ascii_alphanumeric(),
        "invalid OpenShell argument name"
    );
    Ok(())
}

fn validate_routable_name(name: &str) -> Result<()> {
    let is_alnum = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    let bytes = name.as_bytes();
    ensure!(
        bytes.len() <= 19
            && bytes.first().is_some_and(is_alnum)
            && bytes.last().is_some_and(is_alnum)
            && bytes.iter().all(|b| is_alnum(b) || *b == b'-'),
        "invalid OpenShell routable name"
    );
    Ok(())
}

#[derive(PartialEq, Eq)]
struct FileStamp {
    identity: u64,
    len: u64,
    modified: std::time::SystemTime,
}

fn file_stamp(path: &Path) -> Result<FileStamp> {
    let metadata = fs::metadata(path)?;
    let identity = crate::sys::fs::file_id(path);
    ensure!(
        metadata.is_file() && identity != 0,
        "could not identify OpenShell executable"
    );
    Ok(FileStamp {
        identity,
        len: metadata.len(),
        modified: metadata.modified()?,
    })
}

fn hash_file(path: &Path) -> Result<String> {
    let mut sha = Sha256::new();
    let mut file = File::open(path)?;
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        sha.update(&buffer[..count]);
    }
    Ok(sha
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn hash_bytes(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_config(path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 1024 * 1024,
        "OpenShell gateway configuration exceeds 1 MiB"
    );
    Ok(bytes)
}

#[derive(Deserialize)]
struct GatewayConfigFile {
    openshell: GatewayConfig,
}

#[derive(Deserialize)]
struct GatewayConfig {
    gateway: GatewaySelection,
    drivers: VmDrivers,
}

#[derive(Deserialize)]
struct GatewaySelection {
    compute_driver: String,
    #[serde(default)]
    disable_tls: bool,
}

#[derive(Deserialize)]
struct VmDrivers {
    vm: VmAllocation,
}

fn vm_allocation(bytes: &[u8], endpoint: &str) -> Result<VmAllocation> {
    let config: GatewayConfigFile = toml::from_str(std::str::from_utf8(bytes)?).map_err(|_| {
        anyhow::anyhow!("OpenShell gateway configuration must pin a VM driver and allocation")
    })?;
    ensure!(
        config.openshell.gateway.compute_driver == "vm",
        "OpenShell gateway configuration selects a different driver"
    );
    ensure!(
        config.openshell.gateway.disable_tls == endpoint.starts_with("http://"),
        "OpenShell gateway transport differs from its configuration"
    );
    let vm = config.openshell.drivers.vm;
    ensure!(
        vm.vcpus > 0 && vm.mem_mib > 0,
        "OpenShell gateway VM allocation must be positive"
    );
    Ok(vm)
}

#[cfg(test)]
mod tests;
