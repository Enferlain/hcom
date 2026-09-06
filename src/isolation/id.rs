use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use super::error::IsolationError;

fn validate_identifier(id: &str, kind: &'static str) -> Result<(), IsolationError> {
    if id.is_empty() {
        return Err(match kind {
            "workflow" => {
                IsolationError::InvalidWorkflowId(id.to_string(), "cannot be empty".to_string())
            }
            _ => IsolationError::InvalidAttemptId(id.to_string(), "cannot be empty".to_string()),
        });
    }

    if id.len() > 128 {
        return Err(match kind {
            "workflow" => IsolationError::InvalidWorkflowId(
                id.to_string(),
                "exceeds maximum length of 128 characters".to_string(),
            ),
            _ => IsolationError::InvalidAttemptId(
                id.to_string(),
                "exceeds maximum length of 128 characters".to_string(),
            ),
        });
    }

    let first = id.as_bytes()[0];
    if !first.is_ascii_alphanumeric() && first != b'_' {
        return Err(match kind {
            "workflow" => IsolationError::InvalidWorkflowId(
                id.to_string(),
                "must start with an ASCII alphanumeric character or underscore".to_string(),
            ),
            _ => IsolationError::InvalidAttemptId(
                id.to_string(),
                "must start with an ASCII alphanumeric character or underscore".to_string(),
            ),
        });
    }

    if id == "." || id == ".." || id.contains("..") {
        return Err(match kind {
            "workflow" => IsolationError::InvalidWorkflowId(
                id.to_string(),
                "path traversal sequence '..' is forbidden".to_string(),
            ),
            _ => IsolationError::InvalidAttemptId(
                id.to_string(),
                "path traversal sequence '..' is forbidden".to_string(),
            ),
        });
    }

    for &b in id.as_bytes() {
        if !b.is_ascii_alphanumeric() && b != b'-' && b != b'_' && b != b'.' {
            return Err(match kind {
                "workflow" => IsolationError::InvalidWorkflowId(
                    id.to_string(),
                    format!(
                        "forbidden character '{}'; only [a-zA-Z0-9_.-] are permitted",
                        b as char
                    ),
                ),
                _ => IsolationError::InvalidAttemptId(
                    id.to_string(),
                    format!(
                        "forbidden character '{}'; only [a-zA-Z0-9_.-] are permitted",
                        b as char
                    ),
                ),
            });
        }
    }

    Ok(())
}

/// Opaque validated workflow identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkflowId(String);

impl WorkflowId {
    /// Create and validate a new `WorkflowId`.
    pub fn new(id: impl Into<String>) -> Result<Self, IsolationError> {
        let s = id.into();
        validate_identifier(&s, "workflow")?;
        Ok(Self(s))
    }

    /// Access the underlying string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for WorkflowId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for WorkflowId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for WorkflowId {
    type Err = IsolationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for WorkflowId {
    type Error = IsolationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for WorkflowId {
    type Error = IsolationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<WorkflowId> for String {
    fn from(id: WorkflowId) -> Self {
        id.0
    }
}

impl PartialEq<&str> for WorkflowId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<str> for WorkflowId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl Serialize for WorkflowId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for WorkflowId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

/// Opaque validated attempt identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AttemptId(String);

impl AttemptId {
    /// Create and validate a new `AttemptId`.
    pub fn new(id: impl Into<String>) -> Result<Self, IsolationError> {
        let s = id.into();
        validate_identifier(&s, "attempt")?;
        Ok(Self(s))
    }

    /// Access the underlying string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for AttemptId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for AttemptId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for AttemptId {
    type Err = IsolationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for AttemptId {
    type Error = IsolationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for AttemptId {
    type Error = IsolationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<AttemptId> for String {
    fn from(id: AttemptId) -> Self {
        id.0
    }
}

impl PartialEq<&str> for AttemptId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<str> for AttemptId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl Serialize for AttemptId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AttemptId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}
