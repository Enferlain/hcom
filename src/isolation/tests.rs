use std::path::PathBuf;
use std::str::FromStr;

use super::error::IsolationError;
use super::id::{AttemptId, WorkflowId};
use super::plan::{
    GitPaths, GitPolicy, IsolationBackend, IsolationPlan, IsolationPlanInputs, Mount, MountMode,
    NetworkMode, compute_digest, validate_env_key, verify_profile_consistency,
};
use super::profile::IsolationProfile;

#[test]
fn test_isolation_profile_parsing_and_display() {
    // Valid profiles
    assert_eq!(
        IsolationProfile::from_str("off").unwrap(),
        IsolationProfile::Off
    );
    assert_eq!(
        IsolationProfile::from_str("workspace").unwrap(),
        IsolationProfile::Workspace
    );
    assert_eq!(
        IsolationProfile::from_str("workspace-git").unwrap(),
        IsolationProfile::WorkspaceGit
    );

    // Whitespace trimming
    assert_eq!(
        IsolationProfile::from_str("  workspace  ").unwrap(),
        IsolationProfile::Workspace
    );
    assert_eq!(
        IsolationProfile::from_str("\tworkspace-git\n").unwrap(),
        IsolationProfile::WorkspaceGit
    );

    // Display strings
    assert_eq!(IsolationProfile::Off.to_string(), "off");
    assert_eq!(IsolationProfile::Workspace.to_string(), "workspace");
    assert_eq!(IsolationProfile::WorkspaceGit.to_string(), "workspace-git");

    // Profile behavior flags
    assert!(!IsolationProfile::Off.is_isolated());
    assert!(IsolationProfile::Workspace.is_isolated());
    assert!(IsolationProfile::WorkspaceGit.is_isolated());

    assert_eq!(IsolationProfile::Off.git_policy(), GitPolicy::Unrestricted);
    assert_eq!(
        IsolationProfile::Workspace.git_policy(),
        GitPolicy::ReadOnly
    );
    assert_eq!(
        IsolationProfile::WorkspaceGit.git_policy(),
        GitPolicy::Writable
    );

    assert!(IsolationProfile::Off.allows_git_mutation());
    assert!(!IsolationProfile::Workspace.allows_git_mutation());
    assert!(IsolationProfile::WorkspaceGit.allows_git_mutation());

    // Rejection of unknown profiles
    assert!(matches!(
        IsolationProfile::from_str("strict"),
        Err(IsolationError::UnknownProfile(s)) if s == "strict"
    ));
    assert!(matches!(
        IsolationProfile::from_str("docker"),
        Err(IsolationError::UnknownProfile(s)) if s == "docker"
    ));
    assert!(matches!(
        IsolationProfile::from_str(""),
        Err(IsolationError::UnknownProfile(_))
    ));
    assert!(matches!(
        IsolationProfile::from_str("none"),
        Err(IsolationError::UnknownProfile(_))
    ));
}

#[test]
fn test_supporting_enums_parsing_and_display() {
    // GitPolicy
    assert_eq!(
        GitPolicy::from_str("read-only").unwrap(),
        GitPolicy::ReadOnly
    );
    assert_eq!(
        GitPolicy::from_str("writable").unwrap(),
        GitPolicy::Writable
    );
    assert_eq!(
        GitPolicy::from_str("unrestricted").unwrap(),
        GitPolicy::Unrestricted
    );
    assert_eq!(GitPolicy::ReadOnly.to_string(), "read-only");
    assert_eq!(GitPolicy::Writable.to_string(), "writable");
    assert_eq!(GitPolicy::Unrestricted.to_string(), "unrestricted");
    assert!(GitPolicy::from_str("invalid").is_err());

    // IsolationBackend
    assert_eq!(
        IsolationBackend::from_str("bubblewrap").unwrap(),
        IsolationBackend::Bubblewrap
    );
    assert_eq!(
        IsolationBackend::from_str("none").unwrap(),
        IsolationBackend::None
    );
    assert_eq!(IsolationBackend::Bubblewrap.to_string(), "bubblewrap");
    assert_eq!(IsolationBackend::None.to_string(), "none");
    assert!(IsolationBackend::from_str("docker").is_err());

    // NetworkMode
    assert_eq!(NetworkMode::from_str("host").unwrap(), NetworkMode::Host);
    assert_eq!(
        NetworkMode::from_str("brokered").unwrap(),
        NetworkMode::Brokered
    );
    assert_eq!(NetworkMode::None.to_string(), "none");
    assert_eq!(NetworkMode::Host.to_string(), "host");
    assert_eq!(NetworkMode::Brokered.to_string(), "brokered");
    assert!(NetworkMode::from_str("unrestricted").is_err());

    // MountMode
    assert_eq!(MountMode::from_str("ro").unwrap(), MountMode::ReadOnly);
    assert_eq!(
        MountMode::from_str("read-only").unwrap(),
        MountMode::ReadOnly
    );
    assert_eq!(MountMode::from_str("rw").unwrap(), MountMode::ReadWrite);
    assert_eq!(
        MountMode::from_str("read-write").unwrap(),
        MountMode::ReadWrite
    );
    assert_eq!(MountMode::ReadOnly.to_string(), "ro");
    assert_eq!(MountMode::ReadWrite.to_string(), "rw");
    assert!(MountMode::from_str("invalid").is_err());
}

#[test]
fn test_workflow_id_validation() {
    // Valid identifiers
    let valid = [
        "workflow-1",
        "wf_abc.0",
        "hcom-f6g.1",
        "1",
        "c9b8f2a1-4321-4f11-9234-56789abcdef0",
        "openspec-isolation-1-1-1788651670827",
        "_internal_run",
    ];
    for &id in &valid {
        let wf = WorkflowId::new(id).unwrap_or_else(|_| panic!("expected '{id}' to be valid"));
        assert_eq!(wf.as_str(), id);
        assert_eq!(wf, id);
        assert_eq!(wf.to_string(), id);
    }

    // Invalid identifiers
    let invalid = [
        "",                // empty
        ".",               // dot
        "..",              // dot-dot
        "../escape",       // path traversal
        "dir/sub",         // slash
        "dir\\sub",        // backslash
        "-flag",           // starts with hyphen
        ".hidden",         // starts with dot
        "has space",       // whitespace
        "has\tspace",      // tab
        "has\nnewline",    // newline
        "has;cmd",         // shell semicolon
        "has$var",         // shell variable
        "has|pipe",        // shell pipe
        "has&bg",          // shell background
        "has`exec`",       // backtick
        "has(paren)",      // parentheses
        "has<redirect>",   // redirect
        "has..double_dot", // double dot sequence
    ];
    for &id in &invalid {
        assert!(
            WorkflowId::new(id).is_err(),
            "expected '{id}' to be rejected as workflow ID"
        );
    }

    // Length limit: 128 chars ok, 129 chars rejected
    let ok_len = "a".repeat(128);
    assert!(WorkflowId::new(ok_len).is_ok());
    let too_long = "a".repeat(129);
    assert!(matches!(
        WorkflowId::new(too_long),
        Err(IsolationError::InvalidWorkflowId(_, reason)) if reason.contains("maximum length")
    ));
}

#[test]
fn test_attempt_id_validation() {
    // Valid attempt IDs
    let valid = ["0", "1", "42", "attempt-1", "retry_2.0", "c9b8f2a1"];
    for &id in &valid {
        let att = AttemptId::new(id).unwrap_or_else(|_| panic!("expected '{id}' to be valid"));
        assert_eq!(att.as_str(), id);
        assert_eq!(att, id);
        assert_eq!(att.to_string(), id);
    }

    // Invalid attempt IDs
    let invalid = [
        "",
        "..",
        "/1",
        "-1",
        "1/2",
        "attempt with space",
        "attempt;rm",
        ".hidden",
        "attempt..traversal",
    ];
    for &id in &invalid {
        assert!(
            AttemptId::new(id).is_err(),
            "expected '{id}' to be rejected as attempt ID"
        );
    }

    // Length limit
    let too_long = "1".repeat(129);
    assert!(matches!(
        AttemptId::new(too_long),
        Err(IsolationError::InvalidAttemptId(_, reason)) if reason.contains("maximum length")
    ));
}

#[test]
fn test_mount_and_git_paths_validation() {
    // Relative mount source rejected
    let m = Mount::read_only("relative/src", "/abs/tgt");
    assert!(matches!(
        m.validate(),
        Err(IsolationError::InvalidPath { .. })
    ));

    // Relative mount target rejected
    let m = Mount::read_write("/abs/src", "relative/tgt");
    assert!(matches!(
        m.validate(),
        Err(IsolationError::InvalidPath { .. })
    ));

    // Valid mount passes
    let m = Mount::read_only("/abs/src", "/abs/tgt");
    assert!(m.validate().is_ok());

    // Relative GitPaths git_dir rejected
    let gp = GitPaths::new(Some(PathBuf::from("relative/.git")), None);
    assert!(matches!(
        gp.validate(),
        Err(IsolationError::InvalidPath { .. })
    ));

    // Relative GitPaths common_dir rejected
    let gp = GitPaths::new(None, Some(PathBuf::from("relative/common")));
    assert!(matches!(
        gp.validate(),
        Err(IsolationError::InvalidPath { .. })
    ));

    // Valid GitPaths passes
    let gp = GitPaths::new(
        Some(PathBuf::from("/abs/.git")),
        Some(PathBuf::from("/abs/common")),
    );
    assert!(gp.validate().is_ok());

    // Builder rejects relative mount
    let res = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-1").unwrap())
        .attempt_id(AttemptId::new("att-0").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .network_mode(NetworkMode::Host)
        .runtime_path(PathBuf::from("/tmp/runtime"))
        .mount(Mount::read_only("rel/src", "/tgt"))
        .build();
    assert!(matches!(res, Err(IsolationError::InvalidPath { .. })));

    // Builder rejects relative GitPaths
    let res = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-1").unwrap())
        .attempt_id(AttemptId::new("att-0").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .network_mode(NetworkMode::Host)
        .runtime_path(PathBuf::from("/tmp/runtime"))
        .git_paths(GitPaths::new(Some(PathBuf::from("rel/.git")), None))
        .build();
    assert!(matches!(res, Err(IsolationError::InvalidPath { .. })));
}

#[test]
fn test_profile_backend_and_git_policy_consistency() {
    // 1. Off profile consistency:
    // Off must use backend None and GitPolicy Unrestricted
    assert!(
        verify_profile_consistency(
            IsolationProfile::Off,
            IsolationBackend::None,
            GitPolicy::Unrestricted
        )
        .is_ok()
    );

    // Off cannot claim Bubblewrap
    assert!(matches!(
        verify_profile_consistency(
            IsolationProfile::Off,
            IsolationBackend::Bubblewrap,
            GitPolicy::Unrestricted
        ),
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("cannot claim isolation backend")
    ));

    // Off cannot use ReadOnly or Writable
    assert!(matches!(
        verify_profile_consistency(
            IsolationProfile::Off,
            IsolationBackend::None,
            GitPolicy::ReadOnly
        ),
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("must use git policy 'unrestricted'")
    ));
    assert!(matches!(
        verify_profile_consistency(
            IsolationProfile::Off,
            IsolationBackend::None,
            GitPolicy::Writable
        ),
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("must use git policy 'unrestricted'")
    ));

    // 2. Workspace profile consistency:
    // Workspace requires Bubblewrap and ReadOnly
    assert!(
        verify_profile_consistency(
            IsolationProfile::Workspace,
            IsolationBackend::Bubblewrap,
            GitPolicy::ReadOnly
        )
        .is_ok()
    );

    // Workspace cannot use backend None
    assert!(matches!(
        verify_profile_consistency(
            IsolationProfile::Workspace,
            IsolationBackend::None,
            GitPolicy::ReadOnly
        ),
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("requires an isolation backend")
    ));

    // Workspace cannot use Writable or Unrestricted
    assert!(matches!(
        verify_profile_consistency(
            IsolationProfile::Workspace,
            IsolationBackend::Bubblewrap,
            GitPolicy::Writable
        ),
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("requires git policy 'read-only'")
    ));
    assert!(matches!(
        verify_profile_consistency(
            IsolationProfile::Workspace,
            IsolationBackend::Bubblewrap,
            GitPolicy::Unrestricted
        ),
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("requires git policy 'read-only'")
    ));

    // 3. WorkspaceGit profile consistency:
    // WorkspaceGit requires Bubblewrap and Writable
    assert!(
        verify_profile_consistency(
            IsolationProfile::WorkspaceGit,
            IsolationBackend::Bubblewrap,
            GitPolicy::Writable
        )
        .is_ok()
    );

    // WorkspaceGit cannot use backend None
    assert!(matches!(
        verify_profile_consistency(
            IsolationProfile::WorkspaceGit,
            IsolationBackend::None,
            GitPolicy::Writable
        ),
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("requires an isolation backend")
    ));

    // WorkspaceGit cannot use ReadOnly or Unrestricted
    assert!(matches!(
        verify_profile_consistency(
            IsolationProfile::WorkspaceGit,
            IsolationBackend::Bubblewrap,
            GitPolicy::ReadOnly
        ),
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("requires git policy 'writable'")
    ));
    assert!(matches!(
        verify_profile_consistency(
            IsolationProfile::WorkspaceGit,
            IsolationBackend::Bubblewrap,
            GitPolicy::Unrestricted
        ),
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("requires git policy 'writable'")
    ));
}

#[test]
fn test_serialization_round_trips() {
    // Profile serialization
    for profile in [
        IsolationProfile::Off,
        IsolationProfile::Workspace,
        IsolationProfile::WorkspaceGit,
    ] {
        let json = serde_json::to_string(&profile).unwrap();
        let decoded: IsolationProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(profile, decoded);
    }

    // WorkflowId & AttemptId serialization
    let wf = WorkflowId::new("hcom-f6g.1").unwrap();
    let wf_json = serde_json::to_string(&wf).unwrap();
    assert_eq!(wf_json, "\"hcom-f6g.1\"");
    let wf_decoded: WorkflowId = serde_json::from_str(&wf_json).unwrap();
    assert_eq!(wf, wf_decoded);

    let att = AttemptId::new("attempt-0").unwrap();
    let att_json = serde_json::to_string(&att).unwrap();
    assert_eq!(att_json, "\"attempt-0\"");
    let att_decoded: AttemptId = serde_json::from_str(&att_json).unwrap();
    assert_eq!(att, att_decoded);

    // Full IsolationPlan round trip
    let plan = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-100").unwrap())
        .attempt_id(AttemptId::new("att-1").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .git_paths(GitPaths::new(
            Some(PathBuf::from("/home/user/project/.git")),
            None,
        ))
        .mount(Mount::read_only("/usr", "/usr"))
        .mount(Mount::read_write(
            "/home/user/project",
            "/home/user/project",
        ))
        .add_env_key("PATH")
        .unwrap()
        .add_env_key("HOME")
        .unwrap()
        .network_mode(NetworkMode::Host)
        .runtime_path(PathBuf::from("/tmp/hcom_runtime/wf-100/att-1"))
        .build()
        .unwrap();

    let plan_json = serde_json::to_string_pretty(&plan).unwrap();
    let plan_decoded: IsolationPlan = serde_json::from_str(&plan_json).unwrap();

    assert_eq!(plan, plan_decoded);
    assert_eq!(plan_decoded.plan_identity(), plan.plan_identity());
    assert_eq!(plan_decoded.plan_digest(), plan.plan_identity());

    // Verify accessors
    assert_eq!(plan_decoded.profile(), IsolationProfile::Workspace);
    assert_eq!(plan_decoded.backend(), IsolationBackend::Bubblewrap);
    assert_eq!(plan_decoded.workflow_id(), "wf-100");
    assert_eq!(plan_decoded.attempt_id(), "att-1");
    assert_eq!(
        plan_decoded.workspace(),
        PathBuf::from("/home/user/project")
    );
    assert_eq!(plan_decoded.git_policy(), GitPolicy::ReadOnly);
    assert_eq!(plan_decoded.mounts().len(), 2);
    assert_eq!(plan_decoded.env_keys().len(), 2);
    assert_eq!(plan_decoded.network_mode(), NetworkMode::Host);
    assert_eq!(
        plan_decoded.runtime_path(),
        PathBuf::from("/tmp/hcom_runtime/wf-100/att-1")
    );
}

#[test]
fn test_deserialization_tamper_and_unknown_fields_detection() {
    let plan = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-100").unwrap())
        .attempt_id(AttemptId::new("att-1").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .network_mode(NetworkMode::Host)
        .runtime_path(PathBuf::from("/tmp/hcom_runtime/wf-100/att-1"))
        .build()
        .unwrap();

    let valid_json_str = serde_json::to_string(&plan).unwrap();

    // 1. Tamper with workspace path while keeping the recorded plan_identity
    let mut tampered: serde_json::Value = serde_json::from_str(&valid_json_str).unwrap();
    tampered["workspace"] = serde_json::json!("/etc/tampered_path");
    let res: Result<IsolationPlan, _> =
        serde_json::from_str(&serde_json::to_string(&tampered).unwrap());
    assert!(
        res.is_err(),
        "deserialization must fail when plan identity digest does not match modified content"
    );

    // 2. Deny unknown fields: adding an unexpected field must fail deserialization
    let mut unknown_fields: serde_json::Value = serde_json::from_str(&valid_json_str).unwrap();
    unknown_fields["malicious_extra"] = serde_json::json!("untracked_property");
    let res: Result<IsolationPlan, _> =
        serde_json::from_str(&serde_json::to_string(&unknown_fields).unwrap());
    assert!(
        res.is_err(),
        "deserialization must fail when unknown fields are present"
    );
    let err_str = res.unwrap_err().to_string();
    assert!(err_str.contains("unknown field `malicious_extra`"));

    // 3. Missing plan_identity must fail deserialization
    let mut missing_identity: serde_json::Value = serde_json::from_str(&valid_json_str).unwrap();
    missing_identity
        .as_object_mut()
        .unwrap()
        .remove("plan_identity");
    let res: Result<IsolationPlan, _> =
        serde_json::from_str(&serde_json::to_string(&missing_identity).unwrap());
    assert!(
        res.is_err(),
        "deserialization must require plan_identity to be present"
    );
    let err_str = res.unwrap_err().to_string();
    assert!(err_str.contains("missing field `plan_identity`"));
}

#[test]
fn test_secret_values_cannot_enter_isolation_plan() {
    // 1. Structural / type guarantee:
    // IsolationPlan fields are private and expose only read-only accessors.
    // There are NO setters or public struct literals for IsolationPlan.
    // IsolationPlan::env_keys() returns &BTreeSet<String> and stores ONLY variable names.

    // 2. Legitimate environment keys (including BEARER_TOKEN) MUST be accepted:
    assert!(validate_env_key("BEARER_TOKEN").is_ok());
    assert!(validate_env_key("ANTHROPIC_API_KEY").is_ok());
    assert!(validate_env_key("OPENAI_API_KEY").is_ok());
    assert!(validate_env_key("AWS_SECRET_ACCESS_KEY").is_ok());
    assert!(validate_env_key("PATH").is_ok());
    assert!(validate_env_key("HOME").is_ok());
    assert!(validate_env_key("_INTERNAL_TOKEN").is_ok());

    // 3. Rejecting key-value assignments (e.g. passing "VAR=secret_value"):
    let assignment_attempts = [
        "ANTHROPIC_API_KEY=sk-ant-api03-1234567890abcdef",
        "OPENAI_API_KEY=sk-proj-super-secret-token",
        "GITHUB_TOKEN=ghp_secretpasswordhere12345",
        "PASSWORD=secret",
        "FOO=BAR",
    ];

    for attempt in assignment_attempts {
        assert!(
            validate_env_key(attempt).is_err(),
            "environment key containing '=' must be rejected: {attempt}"
        );

        let builder_res = IsolationPlan::builder().add_env_key(attempt);
        assert!(
            builder_res.is_err(),
            "builder must reject env key containing '=': {attempt}"
        );
    }

    // 4. Rejecting invalid characters in variable names (preventing smuggled values/tokens):
    let invalid_key_attempts = [
        "sk-ant-api03-1234567890abcdef", // contains '-'
        "sk-proj-1234567890",            // contains '-'
        "Bearer my-auth-token",          // contains ' ' and '-'
        "1LEADING_DIGIT",                // starts with digit
        "has$variable",                  // contains '$'
        "has;command",                   // contains ';'
    ];

    for attempt in invalid_key_attempts {
        assert!(
            validate_env_key(attempt).is_err(),
            "invalid environment key must be rejected: {attempt}"
        );
    }

    // 5. Realistic host environment extraction:
    let host_env = [
        (
            "ANTHROPIC_API_KEY",
            "sk-ant-api03-ultra-confidential-secret-val-98765",
        ),
        ("OPENAI_API_KEY", "sk-proj-super-secret-openai-value-54321"),
        (
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ),
        ("BEARER_TOKEN", "bearer-token-val-abcdef123456"),
        ("PATH", "/usr/bin:/bin"),
        ("HOME", "/home/user"),
    ];

    // Build the plan with only the allowed variable names
    let mut builder = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-secret-test").unwrap())
        .attempt_id(AttemptId::new("att-0").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .network_mode(NetworkMode::Host)
        .runtime_path(PathBuf::from("/tmp/hcom_runtime/wf-secret-test/att-0"));

    for (key, _val) in host_env {
        builder = builder.add_env_key(key).unwrap();
    }

    let plan = builder.build().unwrap();

    // Verify only the keys are present
    assert!(plan.env_keys().contains("ANTHROPIC_API_KEY"));
    assert!(plan.env_keys().contains("OPENAI_API_KEY"));
    assert!(plan.env_keys().contains("AWS_SECRET_ACCESS_KEY"));
    assert!(plan.env_keys().contains("BEARER_TOKEN"));
    assert!(plan.env_keys().contains("PATH"));
    assert!(plan.env_keys().contains("HOME"));

    // Serialize plan to JSON string
    let serialized_plan = serde_json::to_string_pretty(&plan).unwrap();

    // PROVE that none of the secret values exist anywhere in the serialized plan!
    assert!(
        !serialized_plan.contains("sk-ant-api03-ultra-confidential-secret-val-98765"),
        "Anthropic secret value must NOT be in serialized plan"
    );
    assert!(
        !serialized_plan.contains("ultra-confidential"),
        "Secret substring must NOT be in serialized plan"
    );
    assert!(
        !serialized_plan.contains("sk-proj-super-secret-openai-value-54321"),
        "OpenAI secret value must NOT be in serialized plan"
    );
    assert!(
        !serialized_plan.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
        "AWS secret key must NOT be in serialized plan"
    );
    assert!(
        !serialized_plan.contains("bearer-token-val-abcdef123456"),
        "Bearer token value must NOT be in serialized plan"
    );

    // 6. Deserialization rejects smuggled secrets:
    let malicious_json = r#"{
        "profile": "workspace",
        "backend": "bubblewrap",
        "workflow_id": "wf-malicious",
        "attempt_id": "att-0",
        "workspace": "/home/user/project",
        "runtime_path": "/tmp/hcom_runtime/wf-malicious/att-0",
        "env_keys": ["ANTHROPIC_API_KEY=sk-ant-smuggled-secret"],
        "network_mode": "host",
        "plan_identity": "abc"
    }"#;

    let res: Result<IsolationPlan, _> = serde_json::from_str(malicious_json);
    assert!(
        res.is_err(),
        "deserialization must fail when env_keys contains an assignment with secret value"
    );
}

#[test]
fn test_post_build_mutation_and_api_safety() {
    let plan = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-immutable").unwrap())
        .attempt_id(AttemptId::new("att-0").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .network_mode(NetworkMode::Host)
        .runtime_path(PathBuf::from("/tmp/runtime"))
        .build()
        .unwrap();

    // Accessors return read-only values or clones/references
    let _workspace_ref: &std::path::Path = plan.workspace();
    let _keys_ref: &std::collections::BTreeSet<String> = plan.env_keys();
    let _mounts_ref: &[Mount] = plan.mounts();

    // Validation rejects if a plan is checked with mismatch
    assert!(plan.validate().is_ok());
}

#[test]
fn test_isolation_plan_builder_missing_required_fields() {
    // Missing network_mode
    let res = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-1").unwrap())
        .attempt_id(AttemptId::new("att-0").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .runtime_path(PathBuf::from("/tmp/runtime"))
        .build();
    assert!(matches!(
        res,
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("network_mode is required")
    ));

    // Missing profile
    let res = IsolationPlan::builder()
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-1").unwrap())
        .attempt_id(AttemptId::new("att-0").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .runtime_path(PathBuf::from("/tmp/runtime"))
        .network_mode(NetworkMode::Host)
        .build();
    assert!(matches!(
        res,
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("profile is required")
    ));

    // Missing backend
    let res = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .workflow_id(WorkflowId::new("wf-1").unwrap())
        .attempt_id(AttemptId::new("att-0").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .runtime_path(PathBuf::from("/tmp/runtime"))
        .network_mode(NetworkMode::Host)
        .build();
    assert!(matches!(
        res,
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("backend is required")
    ));

    // Missing workspace
    let res = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-1").unwrap())
        .attempt_id(AttemptId::new("att-0").unwrap())
        .runtime_path(PathBuf::from("/tmp/runtime"))
        .network_mode(NetworkMode::Host)
        .build();
    assert!(matches!(
        res,
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("workspace path is required")
    ));

    // Missing runtime_path
    let res = IsolationPlan::builder()
        .profile(IsolationProfile::Workspace)
        .backend(IsolationBackend::Bubblewrap)
        .workflow_id(WorkflowId::new("wf-1").unwrap())
        .attempt_id(AttemptId::new("att-0").unwrap())
        .workspace(PathBuf::from("/home/user/project"))
        .network_mode(NetworkMode::Host)
        .build();
    assert!(matches!(
        res,
        Err(IsolationError::InvalidPlan(msg)) if msg.contains("runtime_path is required")
    ));
}

#[test]
fn test_digest_inputs_unambiguous_encoding() {
    let wf = WorkflowId::new("wf-1").unwrap();
    let att = AttemptId::new("att-1").unwrap();
    let ws = PathBuf::from("/project");
    let gp = GitPaths::empty();
    let mounts = vec![];
    let env_keys = std::collections::BTreeSet::new();
    let rt = PathBuf::from("/runtime");

    let inputs1 = IsolationPlanInputs {
        profile: IsolationProfile::Workspace,
        backend: IsolationBackend::Bubblewrap,
        workflow_id: &wf,
        attempt_id: &att,
        workspace: &ws,
        git_paths: &gp,
        git_policy: GitPolicy::ReadOnly,
        mounts: &mounts,
        env_keys: &env_keys,
        network_mode: NetworkMode::Host,
        runtime_path: &rt,
    };

    let digest1 = compute_digest(&inputs1);

    // Changing any single field changes the digest
    let inputs2 = IsolationPlanInputs {
        network_mode: NetworkMode::Brokered,
        ..inputs1.clone()
    };
    let digest2 = compute_digest(&inputs2);
    assert_ne!(digest1, digest2);
}
