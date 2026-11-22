#![cfg(unix)]

use super::*;
use std::os::unix::fs::PermissionsExt;

struct Fixture {
    _root: tempfile::TempDir,
    profile: Profile,
    receipt: Qualification,
}

impl Fixture {
    fn new(failure: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let cli = root.path().join("openshell");
        let driver_binary = root.path().join("driver");
        let gateway_binary = root.path().join("gateway-bin");
        let gateway_config = root.path().join("gateway.toml");
        let script = r#"#!/bin/sh
set -eu
test -z "${OPENSHELL_GATEWAY_INSECURE-}"
test -z "${BASH_ENV-}"
case "$*" in
  --version) printf 'openshell @CLI_VERSION@\n';;
  'gateway info --help')
    printf '%s\n' --gateway --gateway-endpoint --workspace --output;;
  'sandbox exec --help')
    printf '%s\n' --no-tty --no-login-shell --name @ENV_FLAG@;;
  'sandbox upload --help') printf '%s\n' --no-git-ignore;;
  'sandbox download --help') printf '%s\n' --workspace;;
  '--color never gateway list --output json')
    printf '%s' '[{"name":"pinned","endpoint":"@REGISTERED_ENDPOINT@","auth":"@AUTH@"}]';;
  '--gateway pinned --gateway-endpoint http://127.0.0.1:8791 --workspace default --color never gateway info --output json')
    @INFO_FAILURE@
    printf '%s' '{"gateway":"pinned","server":"http://127.0.0.1:8791","status":"healthy","version":"0.1.7","compute_drivers":[{"name":"@DRIVER@","capabilities":{"driver_name":"openshell-driver-@DRIVER@","driver_version":"0.1.7"}}]}';;
  '--gateway pinned --gateway-endpoint http://127.0.0.1:8791 --workspace default --color never sandbox list --page-size 1 --output json')
    @WORKSPACE_FAILURE@
    printf '%s' '{"sandboxes":[]}';;
  '--gateway pinned --gateway-endpoint http://127.0.0.1:8791 --workspace default --color never sandbox get --output json owned')
    printf '%s' '{"id":"@SANDBOX_ID@","name":"owned","workspace":"@ROUTE_WORKSPACE@","phase":"@PHASE@","policy":"fixture-private-value"}';;
  *) exit 3;;
esac
"#;
        let script = script
            .replace(
                "@SANDBOX_ID@",
                if failure == "reused-name" {
                    "replacement"
                } else {
                    "captured"
                },
            )
            .replace(
                "@ROUTE_WORKSPACE@",
                if failure == "route-workspace" {
                    "wrong"
                } else {
                    "default"
                },
            )
            .replace(
                "@PHASE@",
                if failure == "not-ready" {
                    "Stopped"
                } else {
                    "Ready"
                },
            )
            .replace(
                "@WORKSPACE_FAILURE@",
                if failure == "workspace" {
                    "exit 8"
                } else {
                    ":"
                },
            )
            .replace(
                "@ENV_FLAG@",
                if failure == "flag" {
                    "'      Overrides --env behavior'"
                } else {
                    "'  -e, --env <ENV>'"
                },
            )
            .replace(
                "@CLI_VERSION@",
                if failure == "version" {
                    "0.1.8"
                } else {
                    "0.1.7"
                },
            )
            .replace(
                "@AUTH@",
                if failure == "auth-mode" {
                    "oidc"
                } else {
                    "plaintext"
                },
            )
            .replace(
                "@INFO_FAILURE@",
                if failure == "auth" { "exit 7" } else { ":" },
            )
            .replace(
                "@DRIVER@",
                if failure == "driver" { "docker" } else { "vm" },
            )
            .replace(
                "@REGISTERED_ENDPOINT@",
                if failure == "endpoint" {
                    "http://127.0.0.1:8792"
                } else {
                    "http://127.0.0.1:8791"
                },
            );
        fs::write(&cli, script).unwrap();
        fs::set_permissions(&cli, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(&driver_binary, b"fixture driver").unwrap();
        fs::write(&gateway_binary, b"fixture gateway").unwrap();
        fs::write(
            &gateway_config,
            b"
[openshell.gateway]
compute_driver = 'vm'
disable_tls = true
[openshell.drivers.vm]
vcpus = 3
mem_mib = 2560
",
        )
        .unwrap();
        let profile = Profile {
            cli,
            gateway_binary,
            gateway_config,
            driver_binary,
            gateway: "pinned".into(),
            workspace: "default".into(),
            endpoint: "http://127.0.0.1:8791".into(),
            config_root: root.path().to_path_buf(),
            state_root: root.path().to_path_buf(),
        };
        let receipt = receipt(&profile, "0.1.7", "0.1.7", Some(4 * 1024 * 1024));
        Self {
            _root: root,
            profile,
            receipt,
        }
    }
}

#[test]
fn enrolled_bridge_verifies_the_gateway_id_workspace_and_ready_phase() {
    use crate::db::HcomDb;
    use crate::sandbox::bridge::{ExecTarget, exec};
    use crate::sandbox::controller::Owner;
    use crate::sandbox::directory::{Access, Placement};
    use crate::sandbox::store::Store;

    for (failure, expected) in [
        ("", None),
        ("reused-name", Some("no longer resolves to the owned ID")),
        (
            "route-workspace",
            Some("no longer resolves to the owned ID"),
        ),
        ("not-ready", Some("is not Ready")),
    ] {
        let fixture = Fixture::new(failure);
        let adapter = Adapter::preflight(fixture.profile.clone(), &fixture.receipt).unwrap();
        let db = HcomDb::open_at(&fixture._root.path().join("host.db")).unwrap();
        let store = Store::initialize(&db).unwrap();
        let team = store.create_team(None).unwrap();
        let worker = store
            .reserve_worker(
                &team.id,
                Access::Controller(team.external_controller.as_ref().unwrap()),
                "guest",
                crate::tool::Tool::Claude,
                Placement {
                    gateway: "pinned",
                    workspace: "default",
                    sandbox: "captured",
                    incarnation: &uuid::Uuid::new_v4().to_string(),
                },
            )
            .unwrap();
        if let Some(expected) = expected {
            let owner = Owner::try_acquire(&store).unwrap().unwrap();
            let error = exec(
                &owner,
                &adapter,
                ExecTarget {
                    sandbox_name: "owned",
                    guest_hcom: "/opt/hcom",
                    guest_manifest: "/opt/manifest",
                    startup_key: Some(&[7; 32]),
                },
                &worker.binding,
                || false,
                |_: &rusqlite::Transaction<'_>, _: &crate::sandbox::protocol::Message| {
                    anyhow::bail!("fixture must not start")
                },
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains(expected), "{error}");
            assert!(!error.contains("fixture-private-value"));
            let connections: i64 = db
                .conn()
                .query_row("SELECT count(*) FROM sandbox_bridge_epochs", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(connections, 0);
        } else {
            adapter.verify_owned_target("owned", &worker).unwrap();
            let mut moved = worker;
            moved.gateway = "another".into();
            assert!(
                adapter
                    .verify_owned_target("owned", &moved)
                    .unwrap_err()
                    .to_string()
                    .contains("owned gateway and workspace")
            );
        }
    }
}

fn receipt(
    profile: &Profile,
    gateway_version: &str,
    driver_version: &str,
    allowance: Option<u64>,
) -> Qualification {
    let configuration = read_config(&profile.gateway_config).unwrap();
    Qualification {
        fingerprint: Fingerprint {
            cli_sha256: hash_file(&profile.cli).unwrap(),
            gateway_sha256: hash_file(&profile.gateway_binary).unwrap(),
            driver_sha256: hash_file(&profile.driver_binary).unwrap(),
            configuration_sha256: hash_bytes(&configuration),
            gateway: profile.gateway.clone(),
            endpoint: profile.endpoint.clone(),
            workspace: profile.workspace.clone(),
            cli_version: "openshell 0.1.7".into(),
            gateway_version: gateway_version.into(),
            driver_name: "openshell-driver-vm".into(),
            driver_version: driver_version.into(),
            vm: vm_allocation(&configuration, &profile.endpoint).unwrap(),
        },
        measured_stdin_bytes: allowance,
    }
}

fn rejected(profile: Profile, receipt: &Qualification, expected: &str) {
    let message = Adapter::preflight(profile, receipt)
        .err()
        .expect("preflight accepted")
        .to_string();
    assert!(message.contains(expected), "{message}");
}

#[test]
fn admits_only_qualified_runtime_and_constructs_fixed_exec() {
    let fixture = Fixture::new("");
    let adapter = Adapter::preflight(fixture.profile.clone(), &fixture.receipt).unwrap();
    assert_eq!(adapter.measured_stdin_bytes(), 4 * 1024 * 1024);
    let command = adapter
        .managed_exec(
            "owned",
            "/opt/hcom/hcom",
            &["sandbox-bridge".into(), "--guest".into()],
        )
        .unwrap();
    assert_eq!(command.get_program(), fixture.profile.cli.as_os_str());
    assert_eq!(
        command.get_args().collect::<Vec<_>>(),
        [
            "--gateway",
            "pinned",
            "--gateway-endpoint",
            "http://127.0.0.1:8791",
            "--workspace",
            "default",
            "--color",
            "never",
            "sandbox",
            "exec",
            "--no-tty",
            "--no-login-shell",
            "--env",
            "BASH_ENV=",
            "--env",
            "ENV=",
            "--name",
            "owned",
            "--",
            "/opt/hcom/hcom",
            "sandbox-bridge",
            "--guest",
        ]
    );
    assert!(
        adapter
            .managed_exec("--name", "/opt/hcom/hcom", &[])
            .is_err()
    );
    assert!(adapter.managed_exec("owned", "hcom", &[]).is_err());
    fs::rename(&fixture.profile.cli, fixture._root.path().join("previous")).unwrap();
    fs::write(&fixture.profile.cli, b"substitute").unwrap();
    assert!(
        adapter
            .managed_exec("owned", "/opt/hcom/hcom", &[])
            .is_err()
    );
}

#[test]
fn rejects_missing_capabilities_wrong_auth_driver_and_registration() {
    for (failure, message) in [
        ("flag", "required flag --env"),
        ("version", "CLI version differs"),
        ("auth-mode", "authentication differs"),
        ("auth", "authenticated gateway/driver probe failed"),
        ("driver", "VM driver differs"),
        ("workspace", "workspace access probe failed"),
        ("endpoint", "registration endpoint differs"),
    ] {
        let fixture = Fixture::new(failure);
        rejected(fixture.profile, &fixture.receipt, message);
    }
}

#[test]
fn rejects_path_and_unknown_or_mismatched_qualification_before_rpc() {
    let mut fixture = Fixture::new("");
    fixture.receipt.measured_stdin_bytes = None;
    rejected(
        fixture.profile.clone(),
        &fixture.receipt,
        "unknown OpenShell stdin allowance",
    );
    fixture.receipt.measured_stdin_bytes = Some(MIN_INPUT_BYTES - 1);
    rejected(
        fixture.profile.clone(),
        &fixture.receipt,
        "insufficient headroom",
    );
    fixture.receipt.measured_stdin_bytes = Some(MIN_INPUT_BYTES);
    let mut profile = fixture.profile.clone();
    profile.cli = PathBuf::from("openshell");
    rejected(profile, &fixture.receipt, "absolute regular file");
    let mut profile = fixture.profile.clone();
    profile.workspace = "different".into();
    rejected(profile, &fixture.receipt, "another gateway or workspace");
    fixture.receipt.fingerprint.vm.mem_mib += 1;
    rejected(
        fixture.profile.clone(),
        &fixture.receipt,
        "VM allocation differs",
    );
    fs::write(&fixture.profile.gateway_config, b"changed").unwrap();
    rejected(
        fixture.profile,
        &fixture.receipt,
        "gateway configuration differs",
    );
}

#[test]
fn rejects_changed_binaries_and_mismatched_transport() {
    for (artifact, message) in [
        ("cli", "CLI digest differs"),
        ("gateway", "gateway digest differs"),
        ("driver", "driver digest differs"),
    ] {
        let fixture = Fixture::new("");
        let path = match artifact {
            "cli" => &fixture.profile.cli,
            "gateway" => &fixture.profile.gateway_binary,
            _ => &fixture.profile.driver_binary,
        };
        fs::write(path, b"different binary").unwrap();
        rejected(fixture.profile, &fixture.receipt, message);
    }

    let mut fixture = Fixture::new("");
    let config = fs::read_to_string(&fixture.profile.gateway_config)
        .unwrap()
        .replace("disable_tls = true", "disable_tls = false");
    fs::write(&fixture.profile.gateway_config, &config).unwrap();
    fixture.receipt.fingerprint.configuration_sha256 = hash_bytes(config.as_bytes());
    rejected(
        fixture.profile,
        &fixture.receipt,
        "gateway transport differs",
    );
}

#[test]
#[ignore = "requires a disposable local OpenShell VM qualification fixture"]
fn live_fixture_uses_managed_exec() {
    #[derive(Deserialize)]
    struct LiveFixture {
        profile: Profile,
        receipt: Qualification,
        sandbox: String,
    }
    let path = std::env::var_os("HCOM_OPENSHELL_VM_FIXTURE").unwrap();
    let fixture: LiveFixture = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let adapter = Adapter::preflight(fixture.profile, &fixture.receipt).unwrap();
    let result = adapter
        .managed_exec(
            &fixture.sandbox,
            "/bin/sh",
            &[
                "-c".into(),
                "test -z \"$BASH_ENV\" && test -z \"$ENV\" && printf managed".into(),
            ],
        )
        .unwrap()
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(result.stdout, b"managed");
}
