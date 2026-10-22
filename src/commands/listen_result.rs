//! Versioned JSON contract for filtered `hcom listen` results.
//!
//! A filtered listen (any `--sql` predicate, event-filter flags, or both)
//! prints exactly one JSON object describing its outcome. Both outcomes share
//! one envelope, and every result is built by this module so the match and
//! timeout shapes cannot drift apart.
//!
//! ## Version 1
//!
//! Keys present on both outcomes:
//!
//! - `schema_version` (integer): the contract version defined by
//!   [`FILTER_RESULT_SCHEMA_VERSION`].
//! - `matched` (boolean): `true` iff an event matched the filter before the
//!   deadline. This is the primary machine-readable discriminator.
//! - `notification` (string): legacy human-readable prose. Its exact wording
//!   is preserved for compatibility, but it is not a parsing surface; use the
//!   typed fields instead.
//!
//! Matched-only keys: `event_id` (integer), `type` (string), `instance`
//! (string), and `data` (the matched event's parsed data — an object for
//! normal events, `null` when the stored data is not valid JSON).
//!
//! Timeout-only keys: `reason` (string, always `"timeout"`),
//! `timeout_seconds` (number, the duration the caller requested), and
//! `effective_timeout_seconds` (number, the duration actually used; a
//! quick-check request of at most one second is clamped to 0.1s).
//!
//! ## Compatibility policy
//!
//! - Additive changes (new keys) may land within the current version.
//!   Consumers must ignore keys they do not recognize.
//! - Removing, renaming, or retyping a documented key, moving a key between
//!   the matched and timeout outcomes, or changing `matched` semantics is
//!   breaking and requires bumping [`FILTER_RESULT_SCHEMA_VERSION`] together
//!   with the documentation above and the contract tests below.
//! - Unfiltered message-mode `--json` output (`{"from", "text"}` lines) is a
//!   separate legacy shape and is intentionally outside this contract.

use serde_json::{Map, Value};

/// Current contract version for filtered-listen JSON results.
pub const FILTER_RESULT_SCHEMA_VERSION: i64 = 1;

/// Typed view of the event row a filtered listen matched.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterMatchEvent {
    /// Durable event id, also embedded in the legacy notification prose.
    pub event_id: i64,
    /// Event type (for example `status`, `life`, `message`).
    pub event_type: String,
    /// Instance the event belongs to.
    pub instance: String,
    /// Parsed event data; `null` when the stored text is not valid JSON.
    pub data: Value,
}

impl FilterMatchEvent {
    /// Build from raw event-row parts. Unparseable stored data becomes
    /// `null`, preserving the pre-contract output.
    pub fn from_row(event_id: i64, event_type: &str, instance: &str, raw_data: &str) -> Self {
        Self {
            event_id,
            event_type: event_type.to_string(),
            instance: instance.to_string(),
            data: serde_json::from_str(raw_data).unwrap_or_default(),
        }
    }
}

/// Legacy matched prose. Human-facing; not a parsing surface.
pub fn match_notification(event: &FilterMatchEvent) -> String {
    format!(
        "[Match found] #{} {}:{}",
        event.event_id, event.event_type, event.instance
    )
}

/// Legacy timeout prose. `effective_timeout_seconds` is the duration actually
/// waited, matching the pre-contract behavior of quoting the effective value.
pub fn timeout_notification(effective_timeout_seconds: f64) -> String {
    format!("[Timeout: no match after {effective_timeout_seconds}s]")
}

/// Envelope shared by both outcomes: the versioned common keys.
fn envelope(matched: bool, notification: &str) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert(
        "schema_version".into(),
        Value::from(FILTER_RESULT_SCHEMA_VERSION),
    );
    fields.insert("matched".into(), Value::from(matched));
    fields.insert("notification".into(), Value::from(notification));
    fields
}

/// Build the versioned result object for a matched filtered listen.
pub fn matched_result(event: &FilterMatchEvent) -> Value {
    let mut fields = envelope(true, &match_notification(event));
    fields.insert("event_id".into(), Value::from(event.event_id));
    fields.insert("type".into(), Value::from(event.event_type.as_str()));
    fields.insert("instance".into(), Value::from(event.instance.as_str()));
    fields.insert("data".into(), event.data.clone());
    Value::Object(fields)
}

/// Build the versioned result object for a filtered-listen timeout.
pub fn timeout_result(requested_timeout_seconds: f64, effective_timeout_seconds: f64) -> Value {
    let mut fields = envelope(false, &timeout_notification(effective_timeout_seconds));
    fields.insert("reason".into(), Value::from("timeout"));
    fields.insert(
        "timeout_seconds".into(),
        serde_json::json!(requested_timeout_seconds),
    );
    fields.insert(
        "effective_timeout_seconds".into(),
        serde_json::json!(effective_timeout_seconds),
    );
    Value::Object(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event() -> FilterMatchEvent {
        FilterMatchEvent::from_row(12, "status", "nova", r#"{"status":"active"}"#)
    }

    fn sorted_keys(value: &Value) -> Vec<&str> {
        value
            .as_object()
            .expect("result must be a JSON object")
            .keys()
            .map(String::as_str)
            .collect()
    }

    #[test]
    fn matched_result_carries_the_current_schema_version() {
        assert_eq!(
            matched_result(&sample_event())["schema_version"].as_i64(),
            Some(FILTER_RESULT_SCHEMA_VERSION)
        );
    }

    #[test]
    fn timeout_result_carries_the_current_schema_version() {
        assert_eq!(
            timeout_result(10.0, 0.1)["schema_version"].as_i64(),
            Some(FILTER_RESULT_SCHEMA_VERSION)
        );
    }

    #[test]
    fn matched_result_has_exactly_the_documented_keys_and_types() {
        let result = matched_result(&sample_event());
        // Exact key set: additive fields must be a deliberate contract change.
        assert_eq!(
            sorted_keys(&result),
            [
                "data",
                "event_id",
                "instance",
                "matched",
                "notification",
                "schema_version",
                "type"
            ]
        );
        assert_eq!(result["matched"].as_bool(), Some(true));
        assert!(result["event_id"].is_i64());
        assert!(result["notification"].is_string());
        assert!(result["type"].is_string());
        assert!(result["instance"].is_string());
        assert!(result["data"].is_object());
    }

    #[test]
    fn timeout_result_has_exactly_the_documented_keys_and_types() {
        let result = timeout_result(10.0, 0.1);
        assert_eq!(
            sorted_keys(&result),
            [
                "effective_timeout_seconds",
                "matched",
                "notification",
                "reason",
                "schema_version",
                "timeout_seconds"
            ]
        );
        assert_eq!(result["matched"].as_bool(), Some(false));
        assert_eq!(result["reason"].as_str(), Some("timeout"));
        assert_eq!(result["timeout_seconds"].as_f64(), Some(10.0));
        assert_eq!(result["effective_timeout_seconds"].as_f64(), Some(0.1));
        assert!(result["notification"].is_string());
    }

    #[test]
    fn outcomes_discriminate_on_matched_without_leaking_keys() {
        let matched = matched_result(&sample_event());
        let timed_out = timeout_result(10.0, 10.0);
        for key in ["reason", "timeout_seconds", "effective_timeout_seconds"] {
            assert!(
                matched.get(key).is_none(),
                "matched result must not carry timeout key '{key}'"
            );
        }
        for key in ["event_id", "type", "instance", "data"] {
            assert!(
                timed_out.get(key).is_none(),
                "timeout result must not carry event key '{key}'"
            );
        }
        assert_eq!(matched["matched"].as_bool(), Some(true));
        assert_eq!(timed_out["matched"].as_bool(), Some(false));
    }

    #[test]
    fn legacy_notification_prose_is_preserved() {
        assert_eq!(
            match_notification(&sample_event()),
            "[Match found] #12 status:nova"
        );
        assert_eq!(timeout_notification(10.0), "[Timeout: no match after 10s]");
        assert_eq!(
            matched_result(&sample_event())["notification"].as_str(),
            Some("[Match found] #12 status:nova")
        );
        assert_eq!(
            timeout_result(10.0, 10.0)["notification"].as_str(),
            Some("[Timeout: no match after 10s]")
        );
    }

    #[test]
    fn typed_event_fields_mirror_the_notification_reference() {
        let result = matched_result(&sample_event());
        assert_eq!(result["event_id"].as_i64(), Some(12));
        assert_eq!(result["type"].as_str(), Some("status"));
        assert_eq!(result["instance"].as_str(), Some("nova"));
        assert_eq!(result["data"], serde_json::json!({"status": "active"}));
    }

    #[test]
    fn unparseable_event_data_stays_null() {
        let event = FilterMatchEvent::from_row(3, "life", "nova", "not json");
        assert!(event.data.is_null());
        assert!(matched_result(&event)["data"].is_null());
    }
}
