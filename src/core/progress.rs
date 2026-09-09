//! Versioned NDJSON output contract for worker observation streams.
//!
//! A worker-following event stream emits line-delimited compact records on
//! stdout. Every record is built by this module so its shape cannot drift.
//!
//! ## Compact record (version 1)
//!
//! Keys on every compact record:
//!
//! - `schema_version` (integer): the contract version defined by
//!   [`PROGRESS_SCHEMA_VERSION`].
//! - `ts` (string): event time of the observation the record was derived
//!   from; an inactivity heartbeat carries the last observed activity time.
//! - `cursor` (integer): durable event cursor / ID of the observed event; an
//!   inactivity heartbeat carries the last observed cursor.
//! - `generation` (string): exact worker generation (instance key) of the
//!   observed attempt.
//! - `activity` (object): the safe progress observation, tagged by `type`.
//! - `thread` (string, optional): workflow thread identifier, present only for
//!   thread-filtered streams.
//!
//! Activity variants (a closed union):
//!
//! - `{"type": "phase", "phase": ...}`: worker lifecycle phase, one of the
//!   [`LifecyclePhase`] names.
//! - `{"type": "file", "path": ...}`: workspace path with observed activity.
//! - `{"type": "command", "category": ...}`: allowlisted command category,
//!   one of the [`CommandCategory`] names. The contract has no field that
//!   could carry a raw command string, its arguments, or environment values.
//! - `{"type": "heartbeat", "phase": ...}`: inactivity heartbeat carrying the
//!   last known phase.
//!
//! ## Secret safety
//!
//! Raw commands, arguments, environment values, transcript text, and message
//! bodies are completely unrepresentable in this contract. No field exists to
//! carry them, and tolerant parsers discard any extra fields so sensitive
//! text cannot leak into serialized records.
//!
//! ## Compatibility policy
//!
//! - Additive changes (new activity variants, new optional keys, new lifecycle
//!   phases or command categories) may land within the current version.
//!   Consumers must ignore keys and variant names they do not recognize.
//! - Removing, renaming, or retyping a documented key, or reusing a documented
//!   variant name with different semantics is breaking and requires bumping
//!   [`PROGRESS_SCHEMA_VERSION`] together with the documentation above and
//!   the contract tests below.

// Staged contract: the classifier and stream CLI (OpenSpec
// stream-correlated-worker-progress tasks 2.x/3.x) are the consumers of this
// module. The binary target cannot see test-only usage, so dead-code analysis
// is suppressed there until the stream CLI is wired up; test builds keep the
// lint armed.
#![cfg_attr(not(test), allow(dead_code))]

use serde_json::Value;

/// Current contract version for compact worker progress records.
pub(crate) const PROGRESS_SCHEMA_VERSION: i64 = 1;

/// Closed vocabulary of worker lifecycle phases observable as progress.
///
/// The names mirror hcom's own lifecycle vocabulary (`launch_failed` recovery
/// readiness checks, the `ready` life action, and the `active`, `listening`,
/// `blocked` status values). Unknown observations are the classifier's to
/// skip, not new phases to invent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecyclePhase {
    Launching,
    Ready,
    Active,
    Listening,
    Blocked,
    Stopped,
}

/// Closed allowlisted command categories.
///
/// This enum is the only payload a command activity may carry: the projection
/// that maps commands into these categories never forwards raw command text,
/// so arguments and environment values are unrepresentable here. New
/// categories are additive within the current schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CommandCategory {
    Build,
    Test,
    Git,
    Search,
    Other,
}

/// One safe, correlated progress observation.
///
/// A closed union by construction: every variant's payload is typed, so raw
/// command strings, environment values, transcript text, and message bodies
/// have no representable field anywhere in a progress record.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ProgressActivity {
    /// The worker changed lifecycle phase.
    Phase { phase: LifecyclePhase },
    /// Activity was observed on a workspace file path.
    File { path: String },
    /// The worker began a new allowlisted command category.
    Command { category: CommandCategory },
    /// Inactivity heartbeat carrying the last known phase.
    Heartbeat { phase: LifecyclePhase },
}

/// Versioned compact NDJSON record for worker observation streams.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CompactRecord {
    pub schema_version: i64,
    pub ts: String,
    pub cursor: i64,
    pub generation: String,
    pub activity: ProgressActivity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
}

impl CompactRecord {
    pub(crate) fn new(
        cursor: i64,
        generation: impl Into<String>,
        ts: impl Into<String>,
        activity: ProgressActivity,
        thread: Option<impl Into<String>>,
    ) -> Self {
        Self {
            schema_version: PROGRESS_SCHEMA_VERSION,
            ts: ts.into(),
            cursor,
            generation: generation.into(),
            activity,
            thread: thread.map(Into::into),
        }
    }
}

/// Build one versioned compact record as a JSON [`Value`] for an observed event.
pub(crate) fn compact_record(
    cursor: i64,
    generation: &str,
    ts: &str,
    activity: &ProgressActivity,
    thread: Option<&str>,
) -> Value {
    let record = CompactRecord::new(cursor, generation, ts, activity.clone(), thread);
    serde_json::to_value(&record).expect("compact record is data-only and cannot fail to serialize")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SAMPLE_GENERATION: &str = "kuma@1000.000000";
    const SAMPLE_TS: &str = "2026-09-09T02:40:32Z";
    const SAMPLE_THREAD: &str = "glm-1788727501-105060";
    const SAMPLE_CURSOR: i64 = 42;

    fn sorted_keys(value: &Value) -> Vec<&str> {
        let mut keys: Vec<&str> = value
            .as_object()
            .expect("payload must be a JSON object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        keys
    }

    #[test]
    fn compact_record_without_thread_has_exactly_the_documented_keys() {
        let record = compact_record(
            SAMPLE_CURSOR,
            SAMPLE_GENERATION,
            SAMPLE_TS,
            &ProgressActivity::Phase {
                phase: LifecyclePhase::Ready,
            },
            None,
        );
        // Exact key set without thread: additive fields must be a deliberate contract change.
        assert_eq!(
            sorted_keys(&record),
            ["activity", "cursor", "generation", "schema_version", "ts"]
        );
        assert_eq!(record["schema_version"], PROGRESS_SCHEMA_VERSION);
        assert_eq!(record["schema_version"].as_i64(), Some(1));
        assert_eq!(record["ts"], SAMPLE_TS);
        assert_eq!(record["cursor"], SAMPLE_CURSOR);
        assert_eq!(record["generation"], SAMPLE_GENERATION);
        assert!(record.get("thread").is_none());
    }

    #[test]
    fn compact_record_with_thread_includes_optional_thread_key() {
        let record = compact_record(
            SAMPLE_CURSOR,
            SAMPLE_GENERATION,
            SAMPLE_TS,
            &ProgressActivity::Phase {
                phase: LifecyclePhase::Ready,
            },
            Some(SAMPLE_THREAD),
        );
        // Exact key set with thread present.
        assert_eq!(
            sorted_keys(&record),
            [
                "activity",
                "cursor",
                "generation",
                "schema_version",
                "thread",
                "ts"
            ]
        );
        assert_eq!(record["schema_version"], PROGRESS_SCHEMA_VERSION);
        assert_eq!(record["ts"], SAMPLE_TS);
        assert_eq!(record["cursor"], SAMPLE_CURSOR);
        assert_eq!(record["generation"], SAMPLE_GENERATION);
        assert_eq!(record["thread"], SAMPLE_THREAD);
    }

    #[test]
    fn activity_variants_serialize_to_the_pinned_safe_shapes() {
        let cases = [
            (
                ProgressActivity::Phase {
                    phase: LifecyclePhase::Launching,
                },
                json!({"type": "phase", "phase": "launching"}),
            ),
            (
                ProgressActivity::File {
                    path: "src/core/progress.rs".into(),
                },
                json!({"type": "file", "path": "src/core/progress.rs"}),
            ),
            (
                ProgressActivity::Command {
                    category: CommandCategory::Test,
                },
                json!({"type": "command", "category": "test"}),
            ),
            (
                ProgressActivity::Heartbeat {
                    phase: LifecyclePhase::Active,
                },
                json!({"type": "heartbeat", "phase": "active"}),
            ),
        ];
        for (activity, expected) in cases {
            let record = compact_record(SAMPLE_CURSOR, SAMPLE_GENERATION, "t", &activity, None);
            assert_eq!(record["activity"], expected);
        }
    }

    #[test]
    fn phase_vocabulary_is_the_closed_hcom_set() {
        let phases = [
            LifecyclePhase::Launching,
            LifecyclePhase::Ready,
            LifecyclePhase::Active,
            LifecyclePhase::Listening,
            LifecyclePhase::Blocked,
            LifecyclePhase::Stopped,
        ];
        let encoded: Vec<String> = phases
            .iter()
            .map(|phase| {
                serde_json::to_value(phase)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(
            encoded,
            [
                "launching",
                "ready",
                "active",
                "listening",
                "blocked",
                "stopped"
            ]
        );
    }

    #[test]
    fn command_categories_are_a_closed_allowlist() {
        let categories = [
            CommandCategory::Build,
            CommandCategory::Test,
            CommandCategory::Git,
            CommandCategory::Search,
            CommandCategory::Other,
        ];
        let encoded: Vec<String> = categories
            .iter()
            .map(|category| {
                serde_json::to_value(category)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(encoded, ["build", "test", "git", "search", "other"]);
    }

    #[test]
    fn command_activity_cannot_represent_raw_command_data() {
        let raw = "curl -H 'Authorization: Bearer SECRET_TOKEN' https://internal.example";
        // A category name must come from the closed allowlist; raw command
        // text is not a representable category.
        assert!(
            serde_json::from_value::<ProgressActivity>(json!(
                {"type": "command", "category": raw}
            ))
            .is_err()
        );
        // There is no activity type that could carry a raw command.
        assert!(
            serde_json::from_value::<ProgressActivity>(json!(
                {"type": "raw_command", "command": raw}
            ))
            .is_err()
        );
        // A tolerant parse drops everything outside the contract, and
        // re-serialization cannot reproduce it.
        let parsed = serde_json::from_value::<ProgressActivity>(json!(
            {"type": "command", "category": "test", "args": raw, "env": raw}
        ))
        .unwrap();
        assert_eq!(
            parsed,
            ProgressActivity::Command {
                category: CommandCategory::Test
            }
        );
        let record = compact_record(SAMPLE_CURSOR, SAMPLE_GENERATION, "t", &parsed, None);
        assert!(
            !record.to_string().contains("SECRET_TOKEN"),
            "raw command data must not appear in the serialized record"
        );
        let activity = record["activity"].clone();
        assert_eq!(sorted_keys(&activity), ["category", "type"]);
    }

    #[test]
    fn secret_bearing_fixtures_cannot_enter_compact_records() {
        // Message bodies and questions cannot be represented as progress activity.
        let secret_msg = "Please use password super_secret_1234 to decrypt credentials";
        assert!(
            serde_json::from_value::<ProgressActivity>(json!({
                "type": "message",
                "message": secret_msg,
                "body": secret_msg,
            }))
            .is_err()
        );

        // Transcript lines and tool outputs cannot be represented.
        let transcript_snippet = "API_KEY=sk-live-abcdef123456";
        assert!(
            serde_json::from_value::<ProgressActivity>(json!({
                "type": "transcript",
                "text": transcript_snippet,
            }))
            .is_err()
        );

        // Injected fields on a valid CompactRecord JSON payload are discarded on deserialization.
        let malicious_json = json!({
            "schema_version": 1,
            "ts": SAMPLE_TS,
            "cursor": 42,
            "generation": SAMPLE_GENERATION,
            "activity": {
                "type": "phase",
                "phase": "active",
                "raw_command": "aws s3 sync s3://secret-bucket /data",
                "secret_env": "AWS_SECRET_ACCESS_KEY=abcd1234efgh"
            },
            "transcript": "worker logged: confidential financial report data",
            "message_body": "worker said: my token is secret-token-xyz"
        });

        let deserialized: CompactRecord =
            serde_json::from_value(malicious_json).expect("valid outer fields deserialize");
        let serialized = serde_json::to_string(&deserialized).unwrap();

        assert!(!serialized.contains("secret-bucket"));
        assert!(!serialized.contains("AWS_SECRET_ACCESS_KEY"));
        assert!(!serialized.contains("confidential financial report"));
        assert!(!serialized.contains("secret-token-xyz"));
    }

    #[test]
    fn activity_contract_round_trips() {
        let cases = [
            ProgressActivity::Phase {
                phase: LifecyclePhase::Listening,
            },
            ProgressActivity::File {
                path: "README.md".into(),
            },
            ProgressActivity::Command {
                category: CommandCategory::Build,
            },
            ProgressActivity::Heartbeat {
                phase: LifecyclePhase::Blocked,
            },
        ];
        for activity in cases {
            let encoded = serde_json::to_value(&activity).unwrap();
            assert_eq!(
                serde_json::from_value::<ProgressActivity>(encoded).unwrap(),
                activity
            );
        }
    }

    #[test]
    fn compact_record_struct_round_trips() {
        let without_thread = CompactRecord::new(
            101,
            SAMPLE_GENERATION,
            SAMPLE_TS,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Active,
            },
            None::<&str>,
        );
        let encoded = serde_json::to_value(&without_thread).unwrap();
        let decoded: CompactRecord = serde_json::from_value(encoded).unwrap();
        assert_eq!(without_thread, decoded);

        let with_thread = CompactRecord::new(
            102,
            SAMPLE_GENERATION,
            SAMPLE_TS,
            ProgressActivity::File {
                path: "src/main.rs".into(),
            },
            Some(SAMPLE_THREAD),
        );
        let encoded_with_thread = serde_json::to_value(&with_thread).unwrap();
        let decoded_with_thread: CompactRecord =
            serde_json::from_value(encoded_with_thread).unwrap();
        assert_eq!(with_thread, decoded_with_thread);
    }

    #[test]
    fn event_cursors_serialize_exact_integer_values() {
        let test_cursors = [0, 1, 42, 999_999, i64::MAX];
        for cursor in test_cursors {
            let record = compact_record(
                cursor,
                SAMPLE_GENERATION,
                SAMPLE_TS,
                &ProgressActivity::Heartbeat {
                    phase: LifecyclePhase::Listening,
                },
                None,
            );
            assert_eq!(record["cursor"].as_i64(), Some(cursor));
        }
    }
}
