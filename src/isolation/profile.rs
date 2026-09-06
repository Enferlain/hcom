use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

use super::error::IsolationError;
use super::plan::GitPolicy;

/// Isolation profile representing the user's intent for worker confinement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum IsolationProfile {
    /// Host execution; explicit opt-out only.
    #[default]
    Off,
    /// Workspace writable, Git metadata read-only, private temp/cache, scoped hcom state.
    Workspace,
    /// Workspace writable, Git metadata writable, private temp/cache, scoped hcom state.
    WorkspaceGit,
}

impl IsolationProfile {
    /// Return the canonical string identifier for the profile.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Workspace => "workspace",
            Self::WorkspaceGit => "workspace-git",
        }
    }

    /// Whether this profile applies an isolation boundary (i.e. not `Off`).
    pub fn is_isolated(&self) -> bool {
        !matches!(self, Self::Off)
    }

    /// Default Git policy for this profile.
    pub fn git_policy(&self) -> GitPolicy {
        match self {
            Self::Off => GitPolicy::Unrestricted,
            Self::Workspace => GitPolicy::ReadOnly,
            Self::WorkspaceGit => GitPolicy::Writable,
        }
    }

    /// Whether this profile permits mutating Git metadata.
    pub fn allows_git_mutation(&self) -> bool {
        matches!(self, Self::Off | Self::WorkspaceGit)
    }
}

impl fmt::Display for IsolationProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for IsolationProfile {
    type Err = IsolationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "off" => Ok(Self::Off),
            "workspace" => Ok(Self::Workspace),
            "workspace-git" => Ok(Self::WorkspaceGit),
            unknown => Err(IsolationError::UnknownProfile(unknown.to_string())),
        }
    }
}

impl Serialize for IsolationProfile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for IsolationProfile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}
