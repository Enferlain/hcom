//! Retained behavior that upstream already supplies. Keep these tests when
//! dropping a local implementation; avoid pinning upstream's internal layout.

use crate::integration_spec::{ALL, ResumeArgs};
use crate::shared::tool_detection::{detect_tool, tool_marker_vars};
use crate::tool::Tool;
use std::collections::HashMap;

#[test]
fn hermes_registration_and_launch_resume_contract() {
    let tool: Tool = "hermes".parse().unwrap();
    assert_eq!(tool, Tool::Hermes);
    let spec = tool.spec();
    assert_eq!(ALL.iter().filter(|s| s.tool == tool).count(), 1);
    assert_eq!(spec.cli_binary, "hermes");
    assert_eq!(spec.launch.args_env, Some("HCOM_HERMES_ARGS"));
    assert_eq!(spec.launch.config_dir_env, Some("HERMES_HOME"));
    assert!(spec.gates.launch_ready_on_plugin_bind);
    assert!(matches!(
        spec.resume.as_ref().unwrap().resume,
        ResumeArgs::Flag("--resume")
    ));
}

#[test]
fn hermes_native_marker_detection_precedence_and_child_scrubbing() {
    let mut env = HashMap::from([("HERMES_SESSION_ID".into(), "session-1".into())]);
    assert_eq!(detect_tool(&env), Tool::Hermes);
    env.insert("CODEX_THREAD_ID".into(), "thread-1".into());
    assert_eq!(detect_tool(&env), Tool::Codex);
    env.remove("CODEX_THREAD_ID");
    env.insert("HERMES_SESSION_ID".into(), String::new());
    assert_eq!(detect_tool(&env), Tool::Adhoc);
    env.insert("HCOM_TOOL".into(), "hermes".into());
    assert_eq!(detect_tool(&env), Tool::Hermes);
    assert!(tool_marker_vars().contains(&"HERMES_SESSION_ID"));
}
