use std::fmt;

/// Errors arising from isolation validation, parsing, and plan construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolationError {
    /// An unknown isolation profile was requested.
    UnknownProfile(String),

    /// A workflow ID failed validation.
    InvalidWorkflowId(String, String),

    /// An attempt ID failed validation.
    InvalidAttemptId(String, String),

    /// An environment key failed validation.
    InvalidEnvKey(String, String),

    /// A filesystem path failed validation.
    InvalidPath { path: String, reason: String },

    /// The isolation plan is missing a required field or is invalid.
    InvalidPlan(String),
}

impl std::error::Error for IsolationError {}

impl fmt::Display for IsolationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownProfile(name) => write!(
                f,
                "unknown isolation profile '{name}'; valid profiles are 'off', 'workspace', 'workspace-git'"
            ),
            Self::InvalidWorkflowId(id, reason) => {
                write!(f, "invalid workflow ID '{id}': {reason}")
            }
            Self::InvalidAttemptId(id, reason) => {
                write!(f, "invalid attempt ID '{id}': {reason}")
            }
            Self::InvalidEnvKey(key, reason) => {
                write!(f, "invalid environment key '{key}': {reason}")
            }
            Self::InvalidPath { path, reason } => {
                write!(f, "invalid path '{path}': {reason}")
            }
            Self::InvalidPlan(reason) => {
                write!(f, "invalid isolation plan: {reason}")
            }
        }
    }
}
