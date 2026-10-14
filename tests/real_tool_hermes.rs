//! Real Hermes Agent lifecycle: launch → idle delivery → mid-turn delivery →
//! transcript → kill → resume → kill.
//!
//! Hermes has no mock-provider mode that works without a full data root, so
//! this drives a real model in an existing, disposable Hermes profile:
//!
//!   hermes profile create hcomtest --clone --no-alias
//!   HCOM_TEST_HERMES_PROFILE=hcomtest \
//!     cargo test --test real_tool_hermes -- --ignored --nocapture --test-threads=1
//!   hermes profile delete hcomtest -y
//!
//! hcom installs its plugin into that profile (and enables it there); the test
//! disables and removes it again, even on failure. Still, never point this at
//! a profile you use: a running gateway hot-loads the plugin meanwhile.

mod support;

use serde_json::Value;
use serial_test::serial;
use std::path::PathBuf;
use std::time::Duration;
use support::{Hcom, parse_launch_names, unique_suffix};

fn test_profile() -> Option<PathBuf> {
    let name = std::env::var("HCOM_TEST_HERMES_PROFILE").ok()?;
    let home = std::env::var_os("HOME").map(PathBuf::from)?;
    let dir = home.join(".hermes").join("profiles").join(&name);
    assert!(
        dir.join("config.yaml").is_file(),
        "HCOM_TEST_HERMES_PROFILE={name}: no profile at {}",
        dir.display()
    );
    Some(dir)
}

/// Undoes the install on drop: the plugin's hcom.json points at this test's
/// temporary HCOM_DIR, which is gone afterwards.
struct PluginCleanup(PathBuf);

impl Drop for PluginCleanup {
    fn drop(&mut self) {
        let name = self.0.file_name().unwrap().to_string_lossy().into_owned();
        let _ = std::process::Command::new("hermes")
            .args(["-p", &name, "plugins", "disable", "hcom"])
            .env("HERMES_HOME", &self.0)
            .env_remove("HCOM_LAUNCHED")
            .env_remove("HCOM_PROCESS_ID")
            .output();
        let _ = std::fs::remove_dir_all(self.0.join("plugins").join("hcom"));
    }
}

/// Messages `name` sent, oldest first.
fn sent_by(h: &Hcom, name: &str) -> Vec<String> {
    let (_, stdout, _) = h.run([
        "events", "--type", "message", "--agent", name, "--last", "50",
    ]);
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|event| event["data"]["text"].as_str().map(str::to_string))
        .collect()
}

fn wait_reply(h: &Hcom, name: &str, token: &str, what: &str) {
    h.eventually(what, Duration::from_secs(180), || {
        Ok(sent_by(h, name)
            .iter()
            .any(|text| text.contains(token))
            .then_some(()))
    });
}

fn wait_listening(h: &Hcom, name: &str, what: &str) -> Value {
    h.eventually(what, Duration::from_secs(90), || {
        Ok(h.instance_json(name)?.filter(|i| {
            i["status"] == "listening" && i["session_id"].as_str().is_some_and(|s| !s.is_empty())
        }))
    })
}

#[test]
#[ignore = "drives a real model in HCOM_TEST_HERMES_PROFILE"]
#[serial]
fn real_hermes_delivery_transcript_kill_and_resume() {
    let Some(profile) = test_profile() else {
        eprintln!("SKIPPED: set HCOM_TEST_HERMES_PROFILE to a disposable Hermes profile");
        return;
    };
    let _cleanup = PluginCleanup(profile.clone());
    let h = Hcom::new();
    h.set_launch_env("HERMES_HOME", profile.to_str().expect("UTF-8 profile path"));

    let suffix = unique_suffix();
    let recipient_pid = format!("hcom-hermes-recipient-{suffix}");
    let recipient = h.start_listening_with_process_id(&recipient_pid);
    let hcom = h.shell_hcom_command();
    let send = |text: &str, to: &str| {
        let (code, out, err) = h.run_as_process(
            &recipient_pid,
            ["send", &format!("@{to}"), "--intent", "request", "--", text],
        );
        assert_eq!(code, 0, "send failed: {out} {err}");
    };

    // Launch and bind: readiness is the plugin's bind.
    let (code, stdout, stderr) = h.run([
        "hermes",
        "--headless",
        "--dir",
        h.workspace.to_str().expect("UTF-8 workspace"),
    ]);
    assert_eq!(
        code,
        0,
        "launch failed:\n{stdout}\n{stderr}\n{}",
        h.diagnostics()
    );
    let name = parse_launch_names(&stdout)
        .into_iter()
        .next()
        .expect("launched name");
    let bound = wait_listening(&h, &name, "plugin bind");
    let session_id = bound["session_id"].as_str().unwrap().to_string();
    assert!(
        profile.join("plugins/hcom/__init__.py").is_file(),
        "plugin installed into the launch's profile"
    );

    // Idle delivery: an inject that starts a turn.
    let idle = format!("HERMES_IDLE_{suffix}");
    send(
        &format!("Please reply to @{recipient} through hcom with the codeword {idle}."),
        &name,
    );
    wait_reply(&h, &name, &idle, "reply to idle delivery");
    wait_listening(&h, &name, "idle after first turn");

    // Mid-turn delivery: appended to a running tool's result.
    let mid = format!("HERMES_MIDTURN_{suffix}");
    send(
        &format!(
            "Run the shell command `sleep 15; echo slept` with your terminal tool. Then run: \
             {hcom} send @{recipient} --name {name} -- '<the codeword from any hcom message you saw meanwhile>'"
        ),
        &name,
    );
    h.eventually("turn running", Duration::from_secs(60), || {
        Ok(h.instance_json(&name)?
            .filter(|i| i["status_context"] == "tool:terminal" || i["status"] == "active")
            .map(|_| ()))
    });
    std::thread::sleep(Duration::from_secs(4));
    send(&format!("The codeword is {mid}"), &name);
    wait_reply(&h, &name, &mid, "reply carrying the mid-turn codeword");
    let (_, transcript, _) = h.run(["transcript", &name, "--detailed", "--full"]);
    assert!(
        transcript.contains("slept") && transcript.contains(&format!("The codeword is {mid}")),
        "mid-turn message rides in the tool result:\n{transcript}"
    );
    wait_listening(&h, &name, "idle after mid-turn turn");

    // Kill, then resume the same session.
    let (code, out, err) = h.run(["kill", &name]);
    assert_eq!(code, 0, "kill failed: {out} {err}");
    h.eventually("stopped", Duration::from_secs(30), || {
        Ok(h.instance_json(&name)?.is_none().then_some(()))
    });
    let (code, out, err) = h.run(["r", &name, "--headless"]);
    assert_eq!(code, 0, "resume failed:\n{out}\n{err}\n{}", h.diagnostics());
    let resumed = wait_listening(&h, &name, "resumed bind");
    assert_eq!(resumed["session_id"].as_str(), Some(session_id.as_str()));
    let recall = format!("HERMES_RESUMED_{suffix}");
    send(
        &format!(
            "Please tell @{recipient} through hcom: {recall}, followed by the idle codeword you sent first."
        ),
        &name,
    );
    h.eventually(
        "resumed reply with history",
        Duration::from_secs(180),
        || {
            Ok(sent_by(&h, &name)
                .iter()
                .any(|t| t.contains(&recall) && t.contains(&idle))
                .then_some(()))
        },
    );

    let (code, out, err) = h.run(["kill", &name]);
    assert_eq!(code, 0, "final kill failed: {out} {err}");
}
