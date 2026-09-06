use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use super::error::IsolationError;
use super::id::{AttemptId, WorkflowId};
use super::profile::IsolationProfile;

/// Git metadata access policy for an isolation plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GitPolicy {
    /// Read-only access to Git metadata (diff, log, status, blame).
    ReadOnly,
    /// Writable access to Git metadata for branches owned by the worker.
    Writable,
    /// Unrestricted Git access (for unisolated/off mode).
    Unrestricted,
}

impl GitPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::Writable => "writable",
            Self::Unrestricted => "unrestricted",
        }
    }
}

impl fmt::Display for GitPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GitPolicy {
    type Err = IsolationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "read-only" => Ok(Self::ReadOnly),
            "writable" => Ok(Self::Writable),
            "unrestricted" => Ok(Self::Unrestricted),
            unknown => Err(IsolationError::InvalidPlan(format!(
                "unknown git policy '{unknown}'; expected 'read-only', 'writable', or 'unrestricted'"
            ))),
        }
    }
}

/// The execution backend responsible for isolation enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IsolationBackend {
    /// Linux Bubblewrap unprivileged container boundary.
    Bubblewrap,
    /// No isolation backend (host execution).
    None,
}

impl IsolationBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bubblewrap => "bubblewrap",
            Self::None => "none",
        }
    }
}

impl fmt::Display for IsolationBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for IsolationBackend {
    type Err = IsolationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "bubblewrap" => Ok(Self::Bubblewrap),
            "none" => Ok(Self::None),
            unknown => Err(IsolationError::InvalidPlan(format!(
                "unknown isolation backend '{unknown}'; expected 'bubblewrap' or 'none'"
            ))),
        }
    }
}

/// Effective networking mode for the isolated worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkMode {
    /// Shares host networking; functional for local development, not hardened against exfiltration.
    Host,
    /// Only configured provider and dependency endpoints are reachable through an egress broker.
    Brokered,
    /// No network access inside the sandbox.
    None,
}

impl NetworkMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Brokered => "brokered",
            Self::None => "none",
        }
    }
}

impl fmt::Display for NetworkMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NetworkMode {
    type Err = IsolationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "host" => Ok(Self::Host),
            "brokered" => Ok(Self::Brokered),
            "none" => Ok(Self::None),
            unknown => Err(IsolationError::InvalidPlan(format!(
                "unknown network mode '{unknown}'; expected 'host', 'brokered', or 'none'"
            ))),
        }
    }
}

/// Access mode for a filesystem mount entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MountMode {
    /// Read-only bind mount.
    ReadOnly,
    /// Read-write bind mount.
    ReadWrite,
}

impl MountMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnly => "ro",
            Self::ReadWrite => "rw",
        }
    }
}

impl fmt::Display for MountMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MountMode {
    type Err = IsolationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "ro" | "read-only" => Ok(Self::ReadOnly),
            "rw" | "read-write" => Ok(Self::ReadWrite),
            unknown => Err(IsolationError::InvalidPlan(format!(
                "unknown mount mode '{unknown}'; expected 'ro' or 'rw'"
            ))),
        }
    }
}

/// A declared filesystem mount entry in the isolation boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Mount {
    /// Absolute host source path.
    pub source: PathBuf,
    /// Absolute sandbox destination path.
    pub target: PathBuf,
    /// Mount access mode (read-only or read-write).
    pub mode: MountMode,
}

impl Mount {
    pub fn read_only(source: impl Into<PathBuf>, target: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(),
            target: target.into(),
            mode: MountMode::ReadOnly,
        }
    }

    pub fn read_write(source: impl Into<PathBuf>, target: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(),
            target: target.into(),
            mode: MountMode::ReadWrite,
        }
    }

    pub fn validate(&self) -> Result<(), IsolationError> {
        validate_path(&self.source, "mount source")?;
        validate_path(&self.target, "mount target")?;
        Ok(())
    }
}

/// Git metadata paths resolved for the workspace (including linked worktrees).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct GitPaths {
    /// Primary `.git` path for the repository or worktree.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_dir: Option<PathBuf>,
    /// Common Git directory (e.g. main repo `.git` in a linked worktree).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub common_dir: Option<PathBuf>,
}

impl GitPaths {
    pub fn new(git_dir: Option<PathBuf>, common_dir: Option<PathBuf>) -> Self {
        Self {
            git_dir,
            common_dir,
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn validate(&self) -> Result<(), IsolationError> {
        if let Some(ref path) = self.git_dir {
            validate_path(path, "git_dir")?;
        }
        if let Some(ref path) = self.common_dir {
            validate_path(path, "git common_dir")?;
        }
        Ok(())
    }
}

/// Validate a path to ensure it is absolute, contains no NUL bytes, and has no control characters.
fn validate_path(path: &Path, label: &'static str) -> Result<(), IsolationError> {
    if !path.is_absolute() {
        return Err(IsolationError::InvalidPath {
            path: path.to_string_lossy().to_string(),
            reason: format!("{label} must be an absolute path"),
        });
    }
    let s = path.to_string_lossy();
    if s.chars().any(|c| c.is_control() || c == '\0') {
        return Err(IsolationError::InvalidPath {
            path: s.to_string(),
            reason: format!("{label} contains control or NUL characters"),
        });
    }
    Ok(())
}

/// Validate that an environment variable key is a valid identifier and contains no secret values or assignments.
///
/// The keys-only structure combined with identifier format and '=' rejection provides the enforceable
/// secret boundary, ensuring values cannot enter the plan.
pub fn validate_env_key(key: &str) -> Result<(), IsolationError> {
    if key.is_empty() {
        return Err(IsolationError::InvalidEnvKey(
            key.to_string(),
            "environment key cannot be empty".to_string(),
        ));
    }

    if key.contains('=') {
        return Err(IsolationError::InvalidEnvKey(
            key.to_string(),
            "environment key cannot contain '=' (value assignment is forbidden; IsolationPlan only holds variable names, never values)".to_string(),
        ));
    }

    let bytes = key.as_bytes();
    let first = bytes[0];
    if !first.is_ascii_alphabetic() && first != b'_' {
        return Err(IsolationError::InvalidEnvKey(
            key.to_string(),
            "environment key must start with an ASCII letter or underscore".to_string(),
        ));
    }

    for &b in bytes {
        if !b.is_ascii_alphanumeric() && b != b'_' {
            return Err(IsolationError::InvalidEnvKey(
                key.to_string(),
                format!(
                    "environment key contains invalid character '{}'; only [a-zA-Z0-9_] are allowed",
                    b as char
                ),
            ));
        }
    }

    Ok(())
}

/// Verify exact consistency between isolation profile, backend, and Git policy.
pub(crate) fn verify_profile_consistency(
    profile: IsolationProfile,
    backend: IsolationBackend,
    git_policy: GitPolicy,
) -> Result<(), IsolationError> {
    match profile {
        IsolationProfile::Off => {
            if backend != IsolationBackend::None {
                return Err(IsolationError::InvalidPlan(format!(
                    "profile 'off' cannot claim isolation backend '{backend}'; backend must be 'none'"
                )));
            }
            if git_policy != GitPolicy::Unrestricted {
                return Err(IsolationError::InvalidPlan(format!(
                    "profile 'off' must use git policy 'unrestricted', got '{git_policy}'"
                )));
            }
        }
        IsolationProfile::Workspace => {
            if backend == IsolationBackend::None {
                return Err(IsolationError::InvalidPlan(
                    "profile 'workspace' requires an isolation backend (e.g. 'bubblewrap') and cannot use 'none'".to_string(),
                ));
            }
            if git_policy != GitPolicy::ReadOnly {
                return Err(IsolationError::InvalidPlan(format!(
                    "profile 'workspace' requires git policy 'read-only', got '{git_policy}'"
                )));
            }
        }
        IsolationProfile::WorkspaceGit => {
            if backend == IsolationBackend::None {
                return Err(IsolationError::InvalidPlan(
                    "profile 'workspace-git' requires an isolation backend (e.g. 'bubblewrap') and cannot use 'none'".to_string(),
                ));
            }
            if git_policy != GitPolicy::Writable {
                return Err(IsolationError::InvalidPlan(format!(
                    "profile 'workspace-git' requires git policy 'writable', got '{git_policy}'"
                )));
            }
        }
    }
    Ok(())
}

/// Borrowed inputs for deterministic digest calculation.
#[derive(Debug, Clone)]
pub(crate) struct IsolationPlanInputs<'a> {
    pub profile: IsolationProfile,
    pub backend: IsolationBackend,
    pub workflow_id: &'a WorkflowId,
    pub attempt_id: &'a AttemptId,
    pub workspace: &'a Path,
    pub git_paths: &'a GitPaths,
    pub git_policy: GitPolicy,
    pub mounts: &'a [Mount],
    pub env_keys: &'a BTreeSet<String>,
    pub network_mode: NetworkMode,
    pub runtime_path: &'a Path,
}

/// Unambiguous, length-prefixed field hashing helper.
fn hash_field(hasher: &mut Sha256, tag: &[u8], value: &[u8]) {
    hasher.update((tag.len() as u32).to_le_bytes());
    hasher.update(tag);
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

/// Deterministically compute the SHA-256 digest of non-secret plan fields.
///
/// Uses length-prefixed framing to guarantee collision-freedom between fields and
/// losslessly encodes raw Path bytes via `as_encoded_bytes()` so non-UTF8 paths do not alias.
pub(crate) fn compute_digest(inputs: &IsolationPlanInputs<'_>) -> String {
    let mut hasher = Sha256::new();

    hash_field(&mut hasher, b"profile", inputs.profile.as_str().as_bytes());
    hash_field(&mut hasher, b"backend", inputs.backend.as_str().as_bytes());
    hash_field(
        &mut hasher,
        b"workflow_id",
        inputs.workflow_id.as_str().as_bytes(),
    );
    hash_field(
        &mut hasher,
        b"attempt_id",
        inputs.attempt_id.as_str().as_bytes(),
    );
    hash_field(
        &mut hasher,
        b"workspace",
        inputs.workspace.as_os_str().as_encoded_bytes(),
    );

    if let Some(ref git_dir) = inputs.git_paths.git_dir {
        hash_field(
            &mut hasher,
            b"git_dir",
            git_dir.as_os_str().as_encoded_bytes(),
        );
    } else {
        hash_field(&mut hasher, b"git_dir", b"none");
    }

    if let Some(ref common_dir) = inputs.git_paths.common_dir {
        hash_field(
            &mut hasher,
            b"common_dir",
            common_dir.as_os_str().as_encoded_bytes(),
        );
    } else {
        hash_field(&mut hasher, b"common_dir", b"none");
    }

    hash_field(
        &mut hasher,
        b"git_policy",
        inputs.git_policy.as_str().as_bytes(),
    );

    // Mount order is preserved because mount order has semantic meaning in bind mounts
    hasher.update((inputs.mounts.len() as u64).to_le_bytes());
    for (i, m) in inputs.mounts.iter().enumerate() {
        let mount_tag = format!("mount:{i}");
        hash_field(&mut hasher, mount_tag.as_bytes(), b"");
        hash_field(&mut hasher, b"src", m.source.as_os_str().as_encoded_bytes());
        hash_field(&mut hasher, b"tgt", m.target.as_os_str().as_encoded_bytes());
        hash_field(&mut hasher, b"mode", m.mode.as_str().as_bytes());
    }

    // env_keys is already a BTreeSet, so sorted order is guaranteed
    hasher.update((inputs.env_keys.len() as u64).to_le_bytes());
    for key in inputs.env_keys {
        hash_field(&mut hasher, b"env_key", key.as_bytes());
    }

    hash_field(
        &mut hasher,
        b"network_mode",
        inputs.network_mode.as_str().as_bytes(),
    );
    hash_field(
        &mut hasher,
        b"runtime_path",
        inputs.runtime_path.as_os_str().as_encoded_bytes(),
    );

    let result = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for b in result {
        use std::fmt::Write;
        let _ = write!(hex, "{:02x}", b);
    }
    hex
}

/// A fully resolved, serializable, non-secret isolation plan.
///
/// This structure describes the outer containment boundary. All fields are private
/// with read-only accessors to prevent invalid state or secret injection via post-build mutation.
/// It contains validated filesystem paths, configuration enums, and environment variable names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawIsolationPlan")]
pub struct IsolationPlan {
    profile: IsolationProfile,
    backend: IsolationBackend,
    workflow_id: WorkflowId,
    attempt_id: AttemptId,
    workspace: PathBuf,
    git_paths: GitPaths,
    git_policy: GitPolicy,
    mounts: Vec<Mount>,
    env_keys: BTreeSet<String>,
    network_mode: NetworkMode,
    runtime_path: PathBuf,
    plan_identity: String,
}

impl IsolationPlan {
    /// Create a new builder for constructing an `IsolationPlan`.
    pub fn builder() -> IsolationPlanBuilder {
        IsolationPlanBuilder::default()
    }

    pub fn profile(&self) -> IsolationProfile {
        self.profile
    }

    pub fn backend(&self) -> IsolationBackend {
        self.backend
    }

    pub fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    pub fn attempt_id(&self) -> &AttemptId {
        &self.attempt_id
    }

    /// Host workspace path.
    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    /// Git metadata paths (if any).
    pub fn git_paths(&self) -> &GitPaths {
        &self.git_paths
    }

    pub fn git_policy(&self) -> GitPolicy {
        self.git_policy
    }

    pub fn mounts(&self) -> &[Mount] {
        &self.mounts
    }

    pub fn env_keys(&self) -> &BTreeSet<String> {
        &self.env_keys
    }

    pub fn network_mode(&self) -> NetworkMode {
        self.network_mode
    }

    /// Host path to private workflow/attempt runtime directory.
    pub fn runtime_path(&self) -> &Path {
        &self.runtime_path
    }

    pub fn plan_identity(&self) -> &str {
        &self.plan_identity
    }

    /// Compute the deterministic SHA-256 digest of this plan.
    pub fn plan_digest(&self) -> String {
        let inputs = IsolationPlanInputs {
            profile: self.profile,
            backend: self.backend,
            workflow_id: &self.workflow_id,
            attempt_id: &self.attempt_id,
            workspace: &self.workspace,
            git_paths: &self.git_paths,
            git_policy: self.git_policy,
            mounts: &self.mounts,
            env_keys: &self.env_keys,
            network_mode: self.network_mode,
            runtime_path: &self.runtime_path,
        };
        compute_digest(&inputs)
    }

    /// Validate all invariants of this isolation plan.
    pub fn validate(&self) -> Result<(), IsolationError> {
        validate_path(&self.workspace, "workspace")?;
        validate_path(&self.runtime_path, "runtime path")?;
        self.git_paths.validate()?;

        for mount in &self.mounts {
            mount.validate()?;
        }

        for key in &self.env_keys {
            validate_env_key(key)?;
        }

        verify_profile_consistency(self.profile, self.backend, self.git_policy)?;

        let computed = self.plan_digest();
        if self.plan_identity != computed {
            return Err(IsolationError::InvalidPlan(format!(
                "plan identity digest mismatch: recorded '{}' != computed '{}'",
                self.plan_identity, computed
            )));
        }

        Ok(())
    }
}

/// Helper struct for deserialization and validation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIsolationPlan {
    profile: IsolationProfile,
    backend: IsolationBackend,
    workflow_id: WorkflowId,
    attempt_id: AttemptId,
    workspace: PathBuf,
    #[serde(default)]
    git_paths: GitPaths,
    #[serde(default)]
    git_policy: Option<GitPolicy>,
    #[serde(default)]
    mounts: Vec<Mount>,
    #[serde(default)]
    env_keys: BTreeSet<String>,
    network_mode: NetworkMode,
    runtime_path: PathBuf,
    plan_identity: String,
}

impl TryFrom<RawIsolationPlan> for IsolationPlan {
    type Error = IsolationError;

    fn try_from(raw: RawIsolationPlan) -> Result<Self, Self::Error> {
        let git_policy = raw.git_policy.unwrap_or_else(|| raw.profile.git_policy());
        verify_profile_consistency(raw.profile, raw.backend, git_policy)?;

        let inputs = IsolationPlanInputs {
            profile: raw.profile,
            backend: raw.backend,
            workflow_id: &raw.workflow_id,
            attempt_id: &raw.attempt_id,
            workspace: &raw.workspace,
            git_paths: &raw.git_paths,
            git_policy,
            mounts: &raw.mounts,
            env_keys: &raw.env_keys,
            network_mode: raw.network_mode,
            runtime_path: &raw.runtime_path,
        };
        let computed = compute_digest(&inputs);

        if raw.plan_identity != computed {
            return Err(IsolationError::InvalidPlan(format!(
                "plan identity digest mismatch: recorded '{}' != computed '{}'",
                raw.plan_identity, computed
            )));
        }

        let plan = Self {
            profile: raw.profile,
            backend: raw.backend,
            workflow_id: raw.workflow_id,
            attempt_id: raw.attempt_id,
            workspace: raw.workspace,
            git_paths: raw.git_paths,
            git_policy,
            mounts: raw.mounts,
            env_keys: raw.env_keys,
            network_mode: raw.network_mode,
            runtime_path: raw.runtime_path,
            plan_identity: raw.plan_identity,
        };

        plan.validate()?;
        Ok(plan)
    }
}

/// Builder for constructing and validating an `IsolationPlan`.
#[derive(Debug, Clone, Default)]
pub struct IsolationPlanBuilder {
    profile: Option<IsolationProfile>,
    backend: Option<IsolationBackend>,
    workflow_id: Option<WorkflowId>,
    attempt_id: Option<AttemptId>,
    workspace: Option<PathBuf>,
    git_paths: GitPaths,
    git_policy: Option<GitPolicy>,
    mounts: Vec<Mount>,
    env_keys: BTreeSet<String>,
    network_mode: Option<NetworkMode>,
    runtime_path: Option<PathBuf>,
}

impl IsolationPlanBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn profile(mut self, profile: IsolationProfile) -> Self {
        self.profile = Some(profile);
        self
    }

    pub fn backend(mut self, backend: IsolationBackend) -> Self {
        self.backend = Some(backend);
        self
    }

    pub fn workflow_id(mut self, id: WorkflowId) -> Self {
        self.workflow_id = Some(id);
        self
    }

    pub fn attempt_id(mut self, id: AttemptId) -> Self {
        self.attempt_id = Some(id);
        self
    }

    pub fn workspace(mut self, path: impl Into<PathBuf>) -> Self {
        self.workspace = Some(path.into());
        self
    }

    pub fn git_paths(mut self, git_paths: GitPaths) -> Self {
        self.git_paths = git_paths;
        self
    }

    pub fn git_policy(mut self, policy: GitPolicy) -> Self {
        self.git_policy = Some(policy);
        self
    }

    pub fn mount(mut self, mount: Mount) -> Self {
        self.mounts.push(mount);
        self
    }

    pub fn mounts(mut self, mounts: impl IntoIterator<Item = Mount>) -> Self {
        self.mounts.extend(mounts);
        self
    }

    /// Add an environment variable key (variable name only; values are strictly forbidden).
    pub fn add_env_key(mut self, key: impl Into<String>) -> Result<Self, IsolationError> {
        let k = key.into();
        validate_env_key(&k)?;
        self.env_keys.insert(k);
        Ok(self)
    }

    /// Add multiple environment variable keys (names only).
    pub fn env_keys(
        mut self,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, IsolationError> {
        for key in keys {
            let k = key.into();
            validate_env_key(&k)?;
            self.env_keys.insert(k);
        }
        Ok(self)
    }

    pub fn network_mode(mut self, mode: NetworkMode) -> Self {
        self.network_mode = Some(mode);
        self
    }

    pub fn runtime_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.runtime_path = Some(path.into());
        self
    }

    /// Validate all fields, compute the plan identity digest, and build the `IsolationPlan`.
    pub fn build(self) -> Result<IsolationPlan, IsolationError> {
        let profile = self
            .profile
            .ok_or_else(|| IsolationError::InvalidPlan("profile is required".to_string()))?;
        let backend = self
            .backend
            .ok_or_else(|| IsolationError::InvalidPlan("backend is required".to_string()))?;
        let workflow_id = self
            .workflow_id
            .ok_or_else(|| IsolationError::InvalidPlan("workflow_id is required".to_string()))?;
        let attempt_id = self
            .attempt_id
            .ok_or_else(|| IsolationError::InvalidPlan("attempt_id is required".to_string()))?;
        let workspace = self
            .workspace
            .ok_or_else(|| IsolationError::InvalidPlan("workspace path is required".to_string()))?;
        let runtime_path = self
            .runtime_path
            .ok_or_else(|| IsolationError::InvalidPlan("runtime_path is required".to_string()))?;
        let network_mode = self
            .network_mode
            .ok_or_else(|| IsolationError::InvalidPlan("network_mode is required".to_string()))?;
        let git_policy = self.git_policy.unwrap_or_else(|| profile.git_policy());

        verify_profile_consistency(profile, backend, git_policy)?;

        let inputs = IsolationPlanInputs {
            profile,
            backend,
            workflow_id: &workflow_id,
            attempt_id: &attempt_id,
            workspace: &workspace,
            git_paths: &self.git_paths,
            git_policy,
            mounts: &self.mounts,
            env_keys: &self.env_keys,
            network_mode,
            runtime_path: &runtime_path,
        };
        let plan_identity = compute_digest(&inputs);

        let plan = IsolationPlan {
            profile,
            backend,
            workflow_id,
            attempt_id,
            workspace,
            git_paths: self.git_paths,
            git_policy,
            mounts: self.mounts,
            env_keys: self.env_keys,
            network_mode,
            runtime_path,
            plan_identity,
        };

        plan.validate()?;
        Ok(plan)
    }
}
