//! Shared fixtures for the downstream observation contract suites.
#![allow(dead_code)]
use crate::support::Hcom;
use rusqlite::Connection;
use serde_json::Value;
pub fn db(h: &Hcom) -> Connection {
    Connection::open(h.path().join("hcom.db")).unwrap()
}

pub fn seed_test_event(h: &Hcom, event_type: &str, instance: &str, data: Value) -> i64 {
    let conn = db(h);
    conn.execute(
        "INSERT INTO events (timestamp, type, instance, data)
         VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?1, ?2, ?3)",
        rusqlite::params![event_type, instance, data.to_string()],
    )
    .unwrap();
    conn.last_insert_rowid()
}

pub fn parse_first_json_line(stdout: &str) -> Value {
    let line = stdout
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with('{'))
        .unwrap_or_else(|| panic!("expected JSON line in output:\n{stdout}"));
    serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON line '{line}': {e}"))
}
