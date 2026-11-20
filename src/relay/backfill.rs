//! Catch-up backfill for relay event gaps.
//!
//! A peer's retained snapshot carries a contiguous run of its own-origin
//! events: the retained tail below its last push plus whatever is new. A device
//! that was offline, or missed non-retained publishes, while the peer pushed
//! past that tail sees the next snapshot start above its import cursor. The
//! cursor then jumps to the newest carried id and every event in between is
//! lost without a trace: messages that were never delivered and never reported
//! as missing.
//!
//! Import records such a range as a gap. The relay worker loop asks the peer
//! for its own-origin events in the range through the existing `events` RPC and
//! imports the answer through the same namespacing path as live events,
//! deduplicated by (device, remote id). The request is sent from the worker
//! loop and the answer is picked up there too: the answer rides back inside the
//! peer's next state snapshot, which the inbound MQTT handler imports, so the
//! handler itself must never wait for it.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::db::HcomDb;
use crate::log;

use super::safe_kv_get;
#[cfg(test)]
use super::safe_kv_set;

pub(crate) const GAP_KEY_PREFIX: &str = "relay_gap_";
/// Events asked for per request. The answer travels back inside the peer's
/// state snapshot, so it has to leave room for the snapshot itself.
pub(crate) const BACKFILL_BATCH: usize = 100;
/// Byte budget asked of the peer for one answer. Peers that predate the
/// `max_bytes` parameter apply their own 96 KiB cap.
pub(crate) const BACKFILL_MAX_BYTES: usize = 32 * 1024;
/// Budget for the one-event request used when a single event is larger than
/// `BACKFILL_MAX_BYTES`.
pub(crate) const BACKFILL_WIDE_MAX_BYTES: usize = 96 * 1024;
/// Floor for the normal byte budget once answers have come back cut to fit the peer's publish.
pub(crate) const BACKFILL_MIN_BYTES: usize = 4 * 1024;
/// Seconds to wait for an answer before asking again.
pub(crate) const BACKFILL_RETRY_SECS: f64 = 30.0;
/// Requests without progress before a gap is given up (and logged).
pub(crate) const BACKFILL_MAX_ATTEMPTS: u32 = 6;
/// Gaps kept per peer. Beyond this the oldest is dropped and logged.
const MAX_GAPS_PER_DEVICE: usize = 16;

/// One missing range of a peer's event ids, both bounds exclusive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Gap {
    /// Our import cursor for the peer when the gap was seen.
    pub after: i64,
    /// The oldest id the peer's snapshot carried; narrows as batches arrive.
    pub before: i64,
    pub short_id: String,
    pub detected_at: f64,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub sent_at: f64,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub recovered: u64,
    /// Next request asks for a single event with the wide byte budget.
    #[serde(default)]
    pub wide: bool,
    /// Answers that came back cut to fit the peer's publish. Each one halves the next normal
    /// request's byte budget, down to `BACKFILL_MIN_BYTES`, so a large peer state leaves room
    /// for a smaller answer instead of every retry failing the same way until the gap is
    /// abandoned.
    #[serde(default)]
    pub shrink: u32,
}

fn gap_key(device_id: &str) -> String {
    format!("{GAP_KEY_PREFIX}{device_id}")
}

#[cfg(test)]
pub(crate) fn load_gaps(db: &HcomDb, device_id: &str) -> Vec<Gap> {
    load_gaps_checked(db, device_id).unwrap_or_default()
}

fn load_gaps_checked(db: &HcomDb, device_id: &str) -> Result<Vec<Gap>> {
    let Some(raw) = db.kv_get(&gap_key(device_id))? else {
        return Ok(Vec::new());
    };
    serde_json::from_str(&raw).map_err(|error| {
        anyhow::anyhow!("invalid relay backfill gap record for device {device_id}: {error}")
    })
}

fn save_gaps(db: &HcomDb, device_id: &str, gaps: &[Gap]) -> Result<()> {
    db.with_write_scope(|| {
        if gaps.is_empty() {
            db.kv_set(&gap_key(device_id), None)?;
        } else {
            let raw = serde_json::to_string(gaps)?;
            db.kv_set(&gap_key(device_id), Some(&raw))?;
        }
        Ok(())
    })
}

/// Forget every gap for a peer (its event ids restarted).
pub(crate) fn clear_gaps(db: &HcomDb, device_id: &str) -> Result<()> {
    db.with_write_scope(|| clear_gaps_in_scope(db, device_id))
}

pub(crate) fn clear_gaps_in_scope(db: &HcomDb, device_id: &str) -> Result<()> {
    db.kv_set(&gap_key(device_id), None)?;
    Ok(())
}

/// Record that the peer's events in (`after`, `before`) were skipped.
#[cfg(test)]
pub(crate) fn record_gap(
    db: &HcomDb,
    device_id: &str,
    short_id: &str,
    after: i64,
    before: i64,
) -> Result<()> {
    db.with_write_scope(|| record_gap_in_scope(db, device_id, short_id, after, before))
}

pub(crate) fn record_gap_in_scope(
    db: &HcomDb,
    device_id: &str,
    short_id: &str,
    after: i64,
    before: i64,
) -> Result<()> {
    if before <= after + 1 {
        return Ok(());
    }
    let mut gaps = load_gaps_checked(db, device_id)?;
    // A snapshot whose carried events were all filtered out leaves the cursor
    // where it was, so the next snapshot reports the same range again.
    if gaps.iter().any(|g| g.after <= after && before <= g.before) {
        return Ok(());
    }
    gaps.push(Gap {
        after,
        before,
        short_id: short_id.to_string(),
        detected_at: crate::shared::time::now_epoch_f64(),
        request_id: None,
        sent_at: 0.0,
        attempts: 0,
        recovered: 0,
        wide: false,
        shrink: 0,
    });
    while gaps.len() > MAX_GAPS_PER_DEVICE {
        let dropped = gaps.remove(0);
        log::log_warn(
            "relay",
            "relay.backfill_dropped",
            &format!(
                "device={} range=({},{}) reason=too_many_gaps",
                dropped.short_id, dropped.after, dropped.before
            ),
        );
    }
    save_gaps(db, device_id, &gaps)?;
    log::log_with_fields(
        "INFO",
        "relay",
        "relay.gap_detected",
        "",
        &[
            ("device", short_id),
            ("after", &after.to_string()),
            ("before", &before.to_string()),
        ],
    );
    Ok(())
}

/// Parameters for the peer's `events` RPC covering the gap. The filter is the
/// push loop's own-origin filter, so the answer is exactly what the missed
/// snapshots would have carried.
/// The normal byte budget after `shrink` cut answers: halved each time, never below the floor.
fn shrunk_budget(shrink: u32) -> usize {
    BACKFILL_MAX_BYTES
        .checked_shr(shrink)
        .unwrap_or(0)
        .max(BACKFILL_MIN_BYTES)
}

pub(crate) fn request_params(gap: &Gap) -> Value {
    let (last, max_bytes) = if gap.wide {
        (1, BACKFILL_WIDE_MAX_BYTES)
    } else {
        (BACKFILL_BATCH, shrunk_budget(gap.shrink))
    };
    json!({
        "sql": format!(
            "id > {} AND id < {} AND instance NOT LIKE '%:%' AND instance != '_device' \
             AND type NOT IN ('control', 'rpc_result') AND json_extract(data, '$._relay') IS NULL",
            gap.after, gap.before
        ),
        "last": last,
        "max_bytes": max_bytes,
    })
}

fn already_imported(db: &HcomDb, device_id: &str, remote_id: i64) -> Result<bool> {
    Ok(db
        .conn()
        .query_row(
            "SELECT 1 FROM events
             WHERE json_extract(data, '$._relay.device') = ?1
               AND json_extract(data, '$._relay.id') = ?2
             LIMIT 1",
            rusqlite::params![device_id, remote_id],
            |_| Ok(()),
        )
        .map(|_| true)
        .or_else(|error| {
            if matches!(error, rusqlite::Error::QueryReturnedNoRows) {
                Ok(false)
            } else {
                Err(error)
            }
        })?)
}

fn local_reset_ts(db: &HcomDb) -> f64 {
    safe_kv_get(db, "relay_local_reset_ts")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

#[derive(Debug, PartialEq)]
pub(crate) enum AnswerOutcome {
    /// The whole range has been answered.
    Done { imported: u64 },
    /// More events remain below the oldest one returned; the gap narrowed.
    More { imported: u64 },
}

/// Import one `events` RPC answer into the gap it was asked for.
#[cfg(test)]
pub(crate) fn apply_answer(
    db: &HcomDb,
    device_id: &str,
    own_short_id: &str,
    gap: &mut Gap,
    response: &Value,
) -> Result<AnswerOutcome, String> {
    let mut working = gap.clone();
    let result = db
        .with_write_scope(|| {
            let expected_request_id = working.request_id.clone();
            match apply_answer_in_scope(
                db,
                device_id,
                own_short_id,
                &mut working,
                expected_request_id.as_deref(),
                response,
            ) {
                Ok(outcome) => Ok(Ok(outcome)),
                Err(ApplyError::Protocol(message)) => Ok(Err(message)),
                Err(ApplyError::Persist(error)) => Err(error),
            }
        })
        .map_err(|error| error.to_string())?;
    match result {
        Ok(outcome) => {
            *gap = working;
            Ok(outcome)
        }
        Err(message) => {
            *gap = working;
            Err(message)
        }
    }
}

#[derive(Debug)]
enum ApplyError {
    Protocol(String),
    Persist(anyhow::Error),
}

fn apply_answer_in_scope(
    db: &HcomDb,
    device_id: &str,
    own_short_id: &str,
    gap: &mut Gap,
    expected_request_id: Option<&str>,
    response: &Value,
) -> std::result::Result<AnswerOutcome, ApplyError> {
    let response_device = response
        .get("_relay")
        .and_then(|relay| relay.get("device"))
        .and_then(Value::as_str);
    if response_device != Some(device_id) {
        return Err(ApplyError::Protocol(format!(
            "ignoring events answer from unexpected device (expected {device_id})"
        )));
    }
    if response.get("action").and_then(Value::as_str) != Some(super::control::rpc_action::EVENTS) {
        return Err(ApplyError::Protocol(
            "ignoring answer for an unexpected RPC action".to_string(),
        ));
    }
    if let Some(expected_request_id) = expected_request_id
        && response.get("request_id").and_then(Value::as_str) != Some(expected_request_id)
    {
        return Err(ApplyError::Protocol(
            "ignoring answer for an unexpected RPC request".to_string(),
        ));
    }

    // An answer the publishing peer had to shrink to fit its sealed payload (answer + its
    // state) has had strings inside `result.events` cut while `ok` stayed true. Importing it
    // would store partial events as if whole, and the gap would close. Refuse it,
    // and ask for less next time: the peer's state does not shrink between retries, so the
    // same budget would be cut the same way every time.
    let answer_has_cut_event = response.get("_relay_truncated").is_some()
        || response
            .get("result")
            .and_then(|result| result.get("events"))
            .and_then(Value::as_array)
            .is_some_and(|events| {
                events.iter().any(|event| {
                    event.get("_relay_truncated").is_some()
                        || event
                            .get("data")
                            .and_then(|data| data.get("_relay_truncated"))
                            .is_some()
                })
            });
    if answer_has_cut_event {
        gap.shrink = gap.shrink.saturating_add(1);
        return Err(ApplyError::Protocol(format!(
            "the answer was cut to fit the peer's publish; not importing partial events \
             (next request asks for {} bytes)",
            shrunk_budget(gap.shrink)
        )));
    }
    let result = response.get("result").cloned().unwrap_or(Value::Null);
    if !response
        .get("ok")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        let detail = result
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown remote error");
        return Err(ApplyError::Protocol(format!("peer refused: {detail}")));
    }
    let Some(events) = result.get("events").and_then(|v| v.as_array()) else {
        return Err(ApplyError::Protocol(
            "answer carried no events list".to_string(),
        ));
    };
    let truncated = result
        .get("truncated")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let asked_for = if gap.wide { 1 } else { BACKFILL_BATCH };

    if events.is_empty() && truncated {
        if gap.wide {
            return Err(ApplyError::Protocol(
                "a single event exceeds the widest answer budget".to_string(),
            ));
        }
        // The newest event in the range alone is larger than the normal budget.
        gap.wide = true;
        return Ok(AnswerOutcome::More { imported: 0 });
    }

    // A non-empty answer is evidence about this exact requested range. Do all
    // validation before inserting any row so an answer containing one bad row
    // cannot close the gap or leave a partially imported page behind. An empty
    // answer is the only valid response that proves there are no matching rows.
    for event in events {
        let Some(remote_id) = event.get("id").and_then(Value::as_i64) else {
            return Err(ApplyError::Protocol(
                "answer contained an event with a missing or invalid id".to_string(),
            ));
        };
        if remote_id <= 0 || remote_id <= gap.after || remote_id >= gap.before {
            return Err(ApplyError::Protocol(format!(
                "answer contained event id {remote_id} outside the requested range"
            )));
        }
        if event.get("type").and_then(Value::as_str) == Some("control")
            || event.get("instance").and_then(Value::as_str) == Some("_device")
        {
            return Err(ApplyError::Protocol(
                "answer contained a non-relay event".to_string(),
            ));
        }
    }

    // The events RPC returns newest-first. Insert the validated page in origin
    // order so a reply can resolve a request that is carried in the same page
    // against the receiver's freshly assigned local row id.
    let mut events_in_origin_order: Vec<&Value> = events.iter().collect();
    events_in_origin_order.sort_by_key(|event| {
        event
            .get("id")
            .and_then(Value::as_i64)
            .expect("event ids were validated above")
    });

    let reset_ts = local_reset_ts(db);
    let mut imported = 0u64;
    let mut oldest = gap.before;
    for event in events_in_origin_order {
        let remote_id = event
            .get("id")
            .and_then(Value::as_i64)
            .expect("event ids were validated above");
        oldest = oldest.min(remote_id);
        let event_ts = super::pull::event_epoch(event);
        if reset_ts > 0.0 && event_ts > 0.0 && event_ts < reset_ts {
            continue;
        }
        let already_imported =
            already_imported(db, device_id, remote_id).map_err(ApplyError::Persist)?;
        if already_imported {
            continue;
        }
        super::pull::insert_remote_event(
            db,
            device_id,
            &gap.short_id,
            remote_id,
            event,
            own_short_id,
        )
        .map_err(ApplyError::Persist)?;
        imported += 1;
    }
    gap.recovered += imported;
    gap.wide = false;

    let more_below = truncated || events.len() >= asked_for;
    if more_below && oldest > gap.after + 1 && oldest < gap.before {
        gap.before = oldest;
        return Ok(AnswerOutcome::More { imported });
    }
    Ok(AnswerOutcome::Done { imported })
}

/// Apply an answer and persist the resulting gap progress as one commit. The
/// RPC result is consumed only after every event and the gap record are durable.
/// A persistence error leaves both the original gap and the answer available
/// for a later retry.
fn apply_answer_and_persist(
    db: &HcomDb,
    device_id: &str,
    own_short_id: &str,
    gaps: &mut Vec<Gap>,
    request_id: &str,
    response: &Value,
) -> Result<std::result::Result<AnswerOutcome, String>> {
    let Some(current) = gaps.first() else {
        return Err(anyhow::anyhow!(
            "backfill answer arrived without an open gap"
        ));
    };
    let mut working = current.clone();
    working.request_id = None;
    let mut next_gaps = gaps.clone();
    let result = db.with_write_scope(|| {
        match apply_answer_in_scope(
            db,
            device_id,
            own_short_id,
            &mut working,
            Some(request_id),
            response,
        ) {
            Ok(outcome) => {
                match outcome {
                    AnswerOutcome::Done { .. } => {
                        next_gaps.remove(0);
                    }
                    AnswerOutcome::More { .. } => {
                        working.attempts = 0;
                        next_gaps[0] = working.clone();
                    }
                }
                save_gaps(db, device_id, &next_gaps)?;
                super::control::delete_rpc_result_checked_for_device(db, request_id, device_id)?;
                Ok(Ok(outcome))
            }
            Err(ApplyError::Protocol(message)) => {
                // Protocol errors contain no imported rows. Persist any
                // shrink/wide adjustment and schedule a fresh request.
                next_gaps[0] = working.clone();
                save_gaps(db, device_id, &next_gaps)?;
                super::control::delete_rpc_result_checked_for_device(db, request_id, device_id)?;
                Ok(Err(message))
            }
            Err(ApplyError::Persist(error)) => Err(error),
        }
    })?;
    *gaps = next_gaps;
    Ok(result)
}

/// What one tick did, for the caller's logging and tests.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct TickSummary {
    pub requests_sent: u32,
    pub events_imported: u64,
    pub gaps_closed: u32,
    pub gaps_abandoned: u32,
}

/// Peers with at least one open gap.
pub(crate) fn devices_with_gaps(db: &HcomDb) -> Vec<String> {
    let pattern = format!("{GAP_KEY_PREFIX}%");
    db.conn()
        .prepare("SELECT key FROM kv WHERE key LIKE ?1 AND value IS NOT NULL")
        .ok()
        .map(|mut stmt| {
            stmt.query_map(rusqlite::params![pattern], |row| row.get::<_, String>(0))
                .ok()
                .map(|rows| {
                    rows.filter_map(|r| r.ok())
                        .filter_map(|key| key.strip_prefix(GAP_KEY_PREFIX).map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

/// Advance every open gap by one step: collect an answer if one arrived,
/// otherwise (re)send the request when the peer can take it.
///
/// `send(short_id, request_id, params)` publishes an `events` RPC request and
/// reports whether MQTT accepted it. Runs on the relay worker loop.
pub(crate) fn tick(
    db: &HcomDb,
    own_short_id: &str,
    now: f64,
    send: &mut dyn FnMut(&str, &str, &Value) -> bool,
) -> TickSummary {
    let mut summary = TickSummary::default();
    for device_id in devices_with_gaps(db) {
        let mut gaps = match load_gaps_checked(db, &device_id) {
            Ok(gaps) => gaps,
            Err(error) => {
                log::log_warn(
                    "relay",
                    "relay.backfill_gap_load_failed",
                    &format!("device={device_id} error={error}"),
                );
                // A failed read is not evidence that the gap is empty. Leave
                // the durable record untouched for the next worker tick.
                continue;
            }
        };
        if gaps.is_empty() {
            if let Err(error) = clear_gaps(db, &device_id) {
                log::log_warn(
                    "relay",
                    "relay.backfill_gap_persist_failed",
                    &format!("device={} error={error}", device_id),
                );
            }
            continue;
        }
        let mut changed = false;
        // Once a pending request is persisted, the durable record is the source of truth for
        // retries. Do not let the final best-effort save below overwrite it with a stale local
        // snapshot if publishing is refused or the next write fails.
        let mut pending_request_persisted = false;

        if let Some(request_id) = gaps[0].request_id.clone() {
            if let Some(response) =
                super::control::get_rpc_result_for_device(db, &request_id, &device_id)
            {
                let prior_gap = gaps[0].clone();
                match apply_answer_and_persist(
                    db,
                    &device_id,
                    own_short_id,
                    &mut gaps,
                    &request_id,
                    &response,
                ) {
                    Ok(Ok(AnswerOutcome::Done { imported })) => {
                        changed = true;
                        summary.events_imported += imported;
                        summary.gaps_closed += 1;
                        log::log_with_fields(
                            "INFO",
                            "relay",
                            "relay.backfill_done",
                            "",
                            &[
                                ("device", &prior_gap.short_id),
                                ("after", &prior_gap.after.to_string()),
                                ("before", &prior_gap.before.to_string()),
                                ("recovered", &prior_gap.recovered.to_string()),
                            ],
                        );
                    }
                    Ok(Ok(AnswerOutcome::More { imported })) => {
                        changed = true;
                        summary.events_imported += imported;
                    }
                    Ok(Err(error)) => {
                        changed = true;
                        log::log_warn(
                            "relay",
                            "relay.backfill_answer_err",
                            &format!(
                                "device={} range=({},{}) error={}",
                                prior_gap.short_id, prior_gap.after, prior_gap.before, error
                            ),
                        );
                    }
                    Err(error) => {
                        // The response and original gap remain durable. Do
                        // not schedule a duplicate request until this write
                        // failure can be retried.
                        log::log_warn(
                            "relay",
                            "relay.backfill_persist_failed",
                            &format!(
                                "device={} range=({},{}) error={error}",
                                prior_gap.short_id, prior_gap.after, prior_gap.before
                            ),
                        );
                    }
                }
            } else if now - gaps[0].sent_at >= BACKFILL_RETRY_SECS {
                changed = true;
                gaps[0].request_id = None;
            }
        }

        if gaps.first().is_some_and(|g| g.request_id.is_none()) {
            let accepts = if gaps[0].attempts >= BACKFILL_MAX_ATTEMPTS {
                Some(false)
            } else {
                super::control::peer_accepts_action(
                    db,
                    &gaps[0].short_id,
                    super::control::rpc_action::EVENTS,
                )
            };
            match accepts {
                Some(true) => {
                    let request_id = uuid::Uuid::new_v4().to_string();
                    let params = request_params(&gaps[0]);
                    let previous_gap = gaps[0].clone();
                    gaps[0].request_id = Some(request_id.clone());
                    gaps[0].sent_at = now;
                    gaps[0].attempts += 1;
                    let short_id = gaps[0].short_id.clone();

                    // Record the request before handing it to MQTT. A full queue, a worker
                    // crash, or a publish error must leave a bounded, recoverable exchange
                    // that the timeout path can retry on the next worker tick.
                    match save_gaps(db, &device_id, &gaps) {
                        Ok(()) => {
                            pending_request_persisted = true;
                            if send(&short_id, &request_id, &params) {
                                summary.requests_sent += 1;
                            } else {
                                log::log_warn(
                                    "relay",
                                    "relay.backfill_publish_deferred",
                                    &format!(
                                        "device={} request={} retry_after={}s",
                                        short_id, request_id, BACKFILL_RETRY_SECS
                                    ),
                                );
                            }
                        }
                        Err(error) => {
                            // Keep the previous durable gap and do not publish an exchange
                            // whose request id cannot be recovered after a restart.
                            gaps[0] = previous_gap;
                            pending_request_persisted = true;
                            log::log_warn(
                                "relay",
                                "relay.backfill_request_persist_failed",
                                &format!(
                                    "device={} request={} error={error}",
                                    short_id, request_id
                                ),
                            );
                        }
                    }
                }
                Some(false) => {
                    log::log_warn(
                        "relay",
                        "relay.backfill_abandoned",
                        &format!(
                            "device={} range=({},{}) attempts={} recovered={}",
                            gaps[0].short_id,
                            gaps[0].after,
                            gaps[0].before,
                            gaps[0].attempts,
                            gaps[0].recovered
                        ),
                    );
                    summary.gaps_abandoned += 1;
                    gaps.remove(0);
                    changed = true;
                }
                // Offline or not yet synced: keep the gap and try later.
                None => {}
            }
        }

        if changed
            && !pending_request_persisted
            && let Err(error) = save_gaps(db, &device_id, &gaps)
        {
            log::log_warn(
                "relay",
                "relay.backfill_gap_persist_failed",
                &format!("device={} error={error}", device_id),
            );
        }
    }
    if summary.events_imported > 0 {
        crate::notify::wake_all(db);
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::test_helpers::isolated_test_env;
    use serial_test::serial;

    const PEER: &str = "peer-device-uuid";

    fn synced_peer(db: &HcomDb, short: &str, caps: &str) {
        safe_kv_set(db, &format!("relay_short_{short}"), Some(PEER));
        let now = crate::shared::time::now_epoch_f64();
        safe_kv_set(
            db,
            &format!("relay_sync_time_{PEER}"),
            Some(&now.to_string()),
        );
        safe_kv_set(db, &format!("relay_caps_{PEER}"), Some(caps));
    }

    fn message(id: i64, text: &str) -> Value {
        json!({
            "id": id,
            "ts": format!("2026-09-26T10:00:{:02}.000000+00:00", id % 60),
            "type": "message",
            "instance": "luna",
            "data": {"from": "luna", "text": text, "mentions": ["nova:MINE"], "scope": "mentions"},
        })
    }

    fn message_reply(id: i64, text: &str, reply_to: &str) -> Value {
        let mut event = message(id, text);
        event["data"]["reply_to"] = json!(reply_to);
        event["data"]["reply_to_local"] = json!(999_999);
        event
    }

    fn answer_from(
        device_id: &str,
        action: &str,
        request_id: &str,
        events: Vec<Value>,
        truncated: bool,
    ) -> Value {
        let mut result = json!({"events": events, "count": events.len()});
        if truncated {
            result["truncated"] = json!(true);
        }
        json!({"request_id": request_id, "action": action, "ok": true, "result": result,
               "_relay": {"device": device_id, "short": "ABCD", "id": 999}})
    }

    fn answer(request_id: &str, events: Vec<Value>, truncated: bool) -> Value {
        answer_from(PEER, "events", request_id, events, truncated)
    }

    fn imported_texts(db: &HcomDb) -> Vec<String> {
        let mut stmt = db
            .conn()
            .prepare(
                "SELECT json_extract(data, '$.text') FROM events
                 WHERE type = 'message' AND json_extract(data, '$._relay.device') = ?1
                 ORDER BY json_extract(data, '$._relay.id')",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![PEER], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    #[test]
    #[serial]
    fn record_gap_ignores_adjacent_and_repeated_ranges() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();

        record_gap(&db, PEER, "ABCD", 10, 11).unwrap();
        assert!(
            load_gaps(&db, PEER).is_empty(),
            "no id lies strictly between"
        );

        record_gap(&db, PEER, "ABCD", 10, 40).unwrap();
        record_gap(&db, PEER, "ABCD", 10, 40).unwrap();
        record_gap(&db, PEER, "ABCD", 12, 30).unwrap();
        let gaps = load_gaps(&db, PEER);
        assert_eq!(gaps.len(), 1);
        assert_eq!((gaps[0].after, gaps[0].before), (10, 40));
    }

    #[test]
    #[serial]
    fn corrupt_gap_record_is_retained_for_recovery() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        db.kv_set(&gap_key(PEER), Some("not-json")).unwrap();

        let raw_before = db.kv_get(&gap_key(PEER)).unwrap();
        assert!(load_gaps_checked(&db, PEER).is_err());
        let summary = tick(&db, "MINE", 1000.0, &mut |_, _, _| {
            panic!("a corrupt durable gap must not be treated as empty")
        });

        assert_eq!(summary, TickSummary::default());
        assert_eq!(db.kv_get(&gap_key(PEER)).unwrap(), raw_before);
    }

    #[test]
    #[serial]
    fn tick_requests_the_range_then_imports_the_answer_and_closes_the_gap() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();

        let mut sent: Vec<(String, String, Value)> = Vec::new();
        let summary = tick(&db, "MINE", 1000.0, &mut |short, req, params| {
            sent.push((short.to_string(), req.to_string(), params.clone()));
            true
        });
        assert_eq!(summary.requests_sent, 1);
        assert_eq!(sent.len(), 1);
        let (short, request_id, params) = sent.remove(0);
        assert_eq!(short, "ABCD");
        let sql = params["sql"].as_str().unwrap();
        assert!(sql.contains("id > 10 AND id < 20"), "{sql}");
        assert!(
            sql.contains("json_extract(data, '$._relay') IS NULL"),
            "{sql}"
        );

        // No answer yet and not timed out: nothing is re-sent.
        let summary = tick(&db, "MINE", 1005.0, &mut |_, _, _| {
            panic!("must not resend")
        });
        assert_eq!(summary, TickSummary::default());

        // The answer arrives inside the peer's snapshot as an imported rpc_result.
        db.log_event(
            "rpc_result",
            "_rpc",
            &answer(
                &request_id,
                vec![message(15, "second"), message(12, "first")],
                false,
            ),
        )
        .unwrap();
        let summary = tick(&db, "MINE", 1006.0, &mut |_, _, _| {
            panic!("must not resend")
        });
        assert_eq!(summary.events_imported, 2);
        assert_eq!(summary.gaps_closed, 1);
        assert!(load_gaps(&db, PEER).is_empty());
        assert_eq!(imported_texts(&db), vec!["first", "second"]);

        // Imported exactly like a live event: namespaced sender, own suffix
        // stripped from mentions so local delivery matches.
        let (from, mentions): (String, String) = db
            .conn()
            .query_row(
                "SELECT json_extract(data, '$.from'), json_extract(data, '$.mentions')
                 FROM events WHERE json_extract(data, '$._relay.id') = 12",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(from, "luna:ABCD");
        assert_eq!(mentions, r#"["nova"]"#);
        // The rendezvous row was consumed.
        assert!(super::super::control::get_rpc_result(&db, &request_id).is_none());
    }

    #[test]
    #[serial]
    fn a_full_batch_narrows_the_gap_and_asks_for_the_rest() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 1, 500).unwrap();

        let mut request_id = String::new();
        tick(&db, "MINE", 1000.0, &mut |_, req, _| {
            request_id = req.to_string();
            true
        });
        // Newest-first page of exactly BACKFILL_BATCH events: 400..=499.
        let page: Vec<Value> = (400..500)
            .rev()
            .map(|id| message(id, &format!("m{id}")))
            .collect();
        db.log_event("rpc_result", "_rpc", &answer(&request_id, page, false))
            .unwrap();

        let mut second: Option<Value> = None;
        let summary = tick(&db, "MINE", 1001.0, &mut |_, _, params| {
            second = Some(params.clone());
            true
        });
        assert_eq!(summary.events_imported, 100);
        assert_eq!(summary.gaps_closed, 0);
        let gaps = load_gaps(&db, PEER);
        assert_eq!((gaps[0].after, gaps[0].before), (1, 400));
        let sql = second.expect("follow-up request")["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(sql.contains("id > 1 AND id < 400"), "{sql}");
    }

    #[test]
    #[serial]
    fn a_repeated_answer_does_not_duplicate_events() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let mut gap = Gap {
            after: 10,
            before: 20,
            short_id: "ABCD".into(),
            detected_at: 0.0,
            request_id: None,
            sent_at: 0.0,
            attempts: 1,
            recovered: 0,
            wide: false,
            shrink: 0,
        };
        let response = answer("r1", vec![message(15, "only")], false);
        assert_eq!(
            apply_answer(&db, PEER, "MINE", &mut gap.clone(), &response),
            Ok(AnswerOutcome::Done { imported: 1 })
        );
        assert_eq!(
            apply_answer(&db, PEER, "MINE", &mut gap, &response),
            Ok(AnswerOutcome::Done { imported: 0 })
        );
        assert_eq!(imported_texts(&db), vec!["only"]);
    }

    #[test]
    #[serial]
    fn a_same_page_reply_resolves_after_origin_ordered_backfill_import() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let mut gap = Gap {
            after: 10,
            before: 20,
            short_id: "ABCD".into(),
            detected_at: 0.0,
            request_id: None,
            sent_at: 0.0,
            attempts: 1,
            recovered: 0,
            wide: false,
            shrink: 0,
        };
        // The peer returns newest-first, but the reply's canonical origin is
        // another event in the same page. Its receiver-local id must resolve
        // after the request (remote id 12) has been inserted.
        let response = answer(
            "same-page-reply",
            vec![message_reply(15, "reply", "12"), message(12, "request")],
            false,
        );
        assert_eq!(
            apply_answer(&db, PEER, "MINE", &mut gap, &response),
            Ok(AnswerOutcome::Done { imported: 2 })
        );

        let request_local_id: i64 = db
            .conn()
            .query_row(
                "SELECT id FROM events WHERE json_extract(data, '$._relay.id') = 12",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let reply_local_id: i64 = db
            .conn()
            .query_row(
                "SELECT json_extract(data, '$.reply_to_local') FROM events \
                 WHERE json_extract(data, '$._relay.id') = 15",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reply_local_id, request_local_id);
    }

    #[test]
    #[serial]
    fn an_answer_cut_in_transit_is_refused_not_imported() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let mut gap = Gap {
            after: 10,
            before: 20,
            short_id: "ABCD".into(),
            detected_at: 0.0,
            request_id: None,
            sent_at: 0.0,
            attempts: 1,
            recovered: 0,
            wide: true,
            shrink: 0,
        };
        // What push's shrink leaves when answer + state overflow the sealed payload:
        // ok stays true, strings inside result.events are cut, and the marker is added.
        let mut response = answer("r2", vec![message(15, "cut short")], false);
        response["_relay_truncated"] = json!({"level": 1, "original_bytes": 120_000});
        let outcome = apply_answer(&db, PEER, "MINE", &mut gap, &response);
        assert!(outcome.is_err(), "{outcome:?}");
        assert!(imported_texts(&db).is_empty(), "no partial event stored");
        assert_eq!(gap.recovered, 0);
    }

    #[test]
    #[serial]
    fn a_nested_cut_event_is_refused_before_partial_import() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let mut gap = Gap {
            after: 10,
            before: 20,
            short_id: "ABCD".into(),
            detected_at: 0.0,
            request_id: None,
            sent_at: 0.0,
            attempts: 1,
            recovered: 0,
            wide: true,
            shrink: 0,
        };
        let mut response = answer("r2-nested", vec![message(15, "cut short")], false);
        response["result"]["events"][0]["data"]["_relay_truncated"] =
            json!({"level": 1, "original_bytes": 120_000});

        let outcome = apply_answer(&db, PEER, "MINE", &mut gap, &response);
        assert!(outcome.is_err(), "{outcome:?}");
        assert!(imported_texts(&db).is_empty());
        assert_eq!(gap.recovered, 0);
    }

    #[test]
    #[serial]
    fn an_answer_must_match_peer_action_and_request_before_import() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let cases = [
            (
                answer_from(
                    "other-peer",
                    "events",
                    "right-request",
                    vec![message(15, "wrong-peer")],
                    false,
                ),
                15,
            ),
            (
                answer_from(
                    PEER,
                    "launch",
                    "right-request",
                    vec![message(16, "wrong-action")],
                    false,
                ),
                16,
            ),
            (
                answer_from(
                    PEER,
                    "events",
                    "wrong-request",
                    vec![message(17, "wrong-request")],
                    false,
                ),
                17,
            ),
        ];

        for (wrong, rightful_id) in cases {
            let mut gap = Gap {
                after: 10,
                before: 30,
                short_id: "ABCD".into(),
                detected_at: 0.0,
                request_id: Some("right-request".into()),
                sent_at: 0.0,
                attempts: 1,
                recovered: 0,
                wide: false,
                shrink: 0,
            };
            let original = gap.clone();
            assert!(apply_answer(&db, PEER, "MINE", &mut gap, &wrong).is_err());
            assert_eq!(gap, original, "invalid answer must not change gap state");
            let imported_before = imported_texts(&db).len();

            let rightful = answer(
                "right-request",
                vec![message(rightful_id, &format!("rightful-{rightful_id}"))],
                false,
            );
            assert_eq!(
                apply_answer(&db, PEER, "MINE", &mut gap, &rightful),
                Ok(AnswerOutcome::Done { imported: 1 })
            );
            assert_eq!(imported_texts(&db).len(), imported_before + 1);
        }
    }

    #[test]
    #[serial]
    fn a_wrong_peer_result_cannot_shadow_the_rightful_backfill_answer() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();

        let mut request_id = String::new();
        tick(&db, "MINE", 1000.0, &mut |_, req, _| {
            request_id = req.to_string();
            true
        });
        db.log_event(
            "rpc_result",
            "_rpc",
            &answer_from(
                "other-peer",
                "events",
                &request_id,
                vec![message(15, "wrong-peer")],
                false,
            ),
        )
        .unwrap();
        db.log_event(
            "rpc_result",
            "_rpc",
            &answer_from(
                PEER,
                "launch",
                &request_id,
                vec![message(15, "wrong-action")],
                false,
            ),
        )
        .unwrap();
        db.log_event(
            "rpc_result",
            "_rpc",
            &answer(&request_id, vec![message(15, "rightful")], false),
        )
        .unwrap();

        let summary = tick(&db, "MINE", 1001.0, &mut |_, _, _| {
            panic!("the rightful answer must be consumed without a retry")
        });
        assert_eq!(summary.events_imported, 1);
        assert_eq!(summary.gaps_closed, 1);
        assert_eq!(imported_texts(&db), vec!["rightful"]);
        assert!(
            super::super::control::get_rpc_result_for_device(&db, &request_id, "other-peer")
                .is_some(),
            "consuming the rightful answer must not delete another peer's row"
        );
    }

    #[test]
    #[serial]
    fn an_import_failure_rolls_back_all_events_in_one_answer() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let mut gap = Gap {
            after: 10,
            before: 20,
            short_id: "ABCD".into(),
            detected_at: 0.0,
            request_id: None,
            sent_at: 0.0,
            attempts: 1,
            recovered: 0,
            wide: false,
            shrink: 0,
        };
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_second_backfill_event BEFORE INSERT ON events
                 WHEN NEW.type = 'message'
                   AND json_extract(NEW.data, '$._relay.id') = 16
                 BEGIN SELECT RAISE(ABORT, 'injected backfill event failure'); END;",
            )
            .unwrap();
        let response = answer(
            "r2-rollback",
            vec![message(15, "first"), message(16, "second")],
            false,
        );

        assert!(apply_answer(&db, PEER, "MINE", &mut gap, &response).is_err());
        assert!(imported_texts(&db).is_empty());
        assert_eq!(gap.recovered, 0);

        db.conn()
            .execute_batch("DROP TRIGGER fail_second_backfill_event;")
            .unwrap();
        assert_eq!(
            apply_answer(&db, PEER, "MINE", &mut gap, &response),
            Ok(AnswerOutcome::Done { imported: 2 })
        );
        assert_eq!(imported_texts(&db), vec!["first", "second"]);
        assert_eq!(
            apply_answer(&db, PEER, "MINE", &mut gap, &response),
            Ok(AnswerOutcome::Done { imported: 0 })
        );
        assert_eq!(imported_texts(&db), vec!["first", "second"]);
    }

    #[test]
    #[serial]
    fn a_cut_answer_halves_the_next_budget_down_to_a_floor() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let mut gap = Gap {
            after: 10,
            before: 20,
            short_id: "ABCD".into(),
            detected_at: 0.0,
            request_id: None,
            sent_at: 0.0,
            attempts: 1,
            recovered: 0,
            wide: false,
            shrink: 0,
        };
        assert_eq!(request_params(&gap)["max_bytes"], json!(BACKFILL_MAX_BYTES));
        let mut response = answer("r3", vec![message(15, "cut short")], false);
        response["_relay_truncated"] = json!({"level": 1, "original_bytes": 60_000});
        let mut budgets = Vec::new();
        for _ in 0..BACKFILL_MAX_ATTEMPTS {
            assert!(apply_answer(&db, PEER, "MINE", &mut gap, &response).is_err());
            budgets.push(request_params(&gap)["max_bytes"].as_u64().unwrap());
        }
        // A large peer state leaves less room each time; the budget follows it down instead
        // of repeating the answer that was just cut, and never asks for less than the floor.
        assert_eq!(budgets, vec![16_384, 8_192, 4_096, 4_096, 4_096, 4_096]);
        assert!(
            imported_texts(&db).is_empty(),
            "no partial event stored on the way down"
        );
        // A gap persisted before this field existed reads back with no shrink.
        let old: Gap = serde_json::from_value(json!({
            "after": 1, "before": 5, "short_id": "ABCD", "detected_at": 0.0
        }))
        .unwrap();
        assert_eq!(old.shrink, 0);
        assert_eq!(request_params(&old)["max_bytes"], json!(BACKFILL_MAX_BYTES));
    }

    #[test]
    #[serial]
    fn an_empty_answer_closes_the_gap_without_importing() {
        // Ids between two own-origin events are often the peer's imported
        // events, which it never relays; the answer is then empty.
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();
        let mut request_id = String::new();
        tick(&db, "MINE", 1000.0, &mut |_, req, _| {
            request_id = req.to_string();
            true
        });
        db.log_event("rpc_result", "_rpc", &answer(&request_id, vec![], false))
            .unwrap();
        let summary = tick(&db, "MINE", 1001.0, &mut |_, _, _| {
            panic!("must not resend")
        });
        assert_eq!(summary.gaps_closed, 1);
        assert_eq!(summary.events_imported, 0);
        assert!(load_gaps(&db, PEER).is_empty());
    }

    #[test]
    #[serial]
    fn an_oversized_newest_event_is_fetched_alone_with_the_wide_budget() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();
        let mut request_id = String::new();
        tick(&db, "MINE", 1000.0, &mut |_, req, _| {
            request_id = req.to_string();
            true
        });
        db.log_event("rpc_result", "_rpc", &answer(&request_id, vec![], true))
            .unwrap();
        let mut next: Option<Value> = None;
        tick(&db, "MINE", 1001.0, &mut |_, _, params| {
            next = Some(params.clone());
            true
        });
        let next = next.expect("wide request");
        assert_eq!(next["last"], json!(1));
        assert_eq!(next["max_bytes"], json!(BACKFILL_WIDE_MAX_BYTES));
    }

    #[test]
    #[serial]
    fn an_unanswered_request_is_resent_then_abandoned_and_logged() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();

        let mut sends = 0;
        let mut now = 1000.0;
        for _ in 0..(BACKFILL_MAX_ATTEMPTS + 2) {
            // Keep the peer fresh so only the attempt budget ends the gap.
            let fresh = crate::shared::time::now_epoch_f64();
            safe_kv_set(
                &db,
                &format!("relay_sync_time_{PEER}"),
                Some(&fresh.to_string()),
            );
            tick(&db, "MINE", now, &mut |_, _, _| {
                sends += 1;
                true
            });
            now += BACKFILL_RETRY_SECS;
        }
        assert_eq!(sends, BACKFILL_MAX_ATTEMPTS as usize);
        assert!(
            load_gaps(&db, PEER).is_empty(),
            "abandoned after the attempt budget"
        );
    }

    #[test]
    #[serial]
    fn an_offline_peer_keeps_its_gap_without_spending_attempts() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        safe_kv_set(&db, &format!("relay_sync_time_{PEER}"), Some("1.0"));
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();

        let summary = tick(&db, "MINE", 1000.0, &mut |_, _, _| {
            panic!("peer is offline")
        });
        assert_eq!(summary, TickSummary::default());
        let gaps = load_gaps(&db, PEER);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].attempts, 0);
    }

    #[test]
    #[serial]
    fn refused_publish_retains_pending_request_until_timeout_retry() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();

        let mut sent_ids = Vec::new();
        let first = tick(&db, "MINE", 1000.0, &mut |_, request_id, _| {
            sent_ids.push(request_id.to_string());
            false
        });
        assert_eq!(first, TickSummary::default());
        assert_eq!(sent_ids.len(), 1);
        let first_id = sent_ids[0].clone();
        assert_eq!(
            load_gaps(&db, PEER)[0].request_id.as_deref(),
            Some(first_id.as_str())
        );

        let before_timeout = tick(&db, "MINE", 1001.0, &mut |_, _, _| {
            panic!("a retained refused request must wait for its bounded timeout")
        });
        assert_eq!(before_timeout, TickSummary::default());

        let retry = tick(
            &db,
            "MINE",
            1000.0 + BACKFILL_RETRY_SECS,
            &mut |_, request_id, _| {
                sent_ids.push(request_id.to_string());
                true
            },
        );
        assert_eq!(retry.requests_sent, 1);
        assert_eq!(sent_ids.len(), 2);
        assert_ne!(sent_ids[1], first_id);
        assert_eq!(load_gaps(&db, PEER)[0].attempts, 2);
    }

    #[test]
    #[serial]
    fn failed_pending_request_write_does_not_publish_or_lose_gap() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_backfill_request BEFORE INSERT ON kv
                 WHEN NEW.key = 'relay_gap_peer-device-uuid'
                 BEGIN SELECT RAISE(ABORT, 'injected request gap failure'); END;",
            )
            .unwrap();

        let mut sends = 0;
        let failed = tick(&db, "MINE", 1000.0, &mut |_, _, _| {
            sends += 1;
            true
        });
        assert_eq!(failed, TickSummary::default());
        assert_eq!(
            sends, 0,
            "a request must not publish before its durable id exists"
        );
        let gaps = load_gaps(&db, PEER);
        assert_eq!(gaps.len(), 1);
        assert!(gaps[0].request_id.is_none());

        db.conn()
            .execute_batch("DROP TRIGGER fail_backfill_request;")
            .unwrap();
        let recovered = tick(&db, "MINE", 1001.0, &mut |_, _, _| {
            sends += 1;
            true
        });
        assert_eq!(recovered.requests_sent, 1);
        assert_eq!(sends, 1);
        assert!(load_gaps(&db, PEER)[0].request_id.is_some());
    }

    #[test]
    #[serial]
    fn a_peer_without_the_events_action_is_abandoned_at_once() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["launch"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();

        let summary = tick(&db, "MINE", 1000.0, &mut |_, _, _| panic!("unsupported"));
        assert_eq!(summary.gaps_abandoned, 1);
        assert!(load_gaps(&db, PEER).is_empty());
    }

    #[test]
    #[serial]
    fn a_failed_answer_leaves_the_gap_pending_for_a_retry() {
        // A refused or malformed answer must not close the gap or count as
        // progress: the cursor jumped past these events, and the gap is the
        // only record that they are still missing. The request is re-sent with
        // a fresh id in the same tick.
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();
        let mut request_id = String::new();
        tick(&db, "MINE", 1000.0, &mut |_, req, _| {
            request_id = req.to_string();
            true
        });

        let mut refused = answer_from(PEER, "events", &request_id, vec![], false);
        refused["ok"] = json!(false);
        refused["result"] = json!({"error": "sql error: bogus"});
        db.log_event("rpc_result", "_rpc", &refused).unwrap();
        let mut retry_ids: Vec<String> = Vec::new();
        let summary = tick(&db, "MINE", 1001.0, &mut |_, req, _| {
            retry_ids.push(req.to_string());
            true
        });
        assert_eq!(
            summary,
            TickSummary {
                requests_sent: 1,
                ..Default::default()
            }
        );
        assert_eq!(retry_ids.len(), 1);
        assert_ne!(retry_ids[0], request_id, "a fresh request id is used");
        let gaps = load_gaps(&db, PEER);
        assert_eq!(gaps.len(), 1, "the gap survives a refused answer");
        assert_eq!(
            gaps[0].request_id.as_deref(),
            Some(retry_ids[0].as_str()),
            "the retry is now the pending exchange"
        );
        assert_eq!(gaps[0].recovered, 0, "nothing was imported");
        assert!(imported_texts(&db).is_empty());
    }

    #[test]
    #[serial]
    fn events_outside_the_gap_range_are_never_imported() {
        // A misbehaving peer answering with rows from another device or another
        // part of its history must not get them stored under this gap.
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let mut gap = Gap {
            after: 10,
            before: 20,
            short_id: "ABCD".into(),
            detected_at: 0.0,
            request_id: None,
            sent_at: 0.0,
            attempts: 1,
            recovered: 0,
            wide: false,
            shrink: 0,
        };
        let response = answer(
            "r4",
            vec![
                message(5, "below"),
                message(10, "at the after bound"),
                message(20, "at the before bound"),
                message(99, "above"),
            ],
            false,
        );
        assert!(apply_answer(&db, PEER, "MINE", &mut gap, &response).is_err());
        assert!(imported_texts(&db).is_empty());
        assert_eq!(gap.recovered, 0);

        let rightful = answer("r4-rightful", vec![message(15, "inside")], false);
        assert_eq!(
            apply_answer(&db, PEER, "MINE", &mut gap, &rightful),
            Ok(AnswerOutcome::Done { imported: 1 })
        );
        assert_eq!(imported_texts(&db), vec!["inside"]);
    }

    #[test]
    #[serial]
    fn failed_gap_progress_commit_keeps_answer_and_retries_exactly_once() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        synced_peer(&db, "ABCD", r#"["events"]"#);
        record_gap(&db, PEER, "ABCD", 10, 20).unwrap();

        let mut request_id = String::new();
        tick(&db, "MINE", 1000.0, &mut |_, req, _| {
            request_id = req.to_string();
            true
        });
        db.log_event(
            "rpc_result",
            "_rpc",
            &answer(&request_id, vec![message(15, "retry-once")], false),
        )
        .unwrap();
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_backfill_gap BEFORE DELETE ON kv
                 WHEN OLD.key = 'relay_gap_peer-device-uuid'
                 BEGIN SELECT RAISE(ABORT, 'injected backfill gap failure'); END;",
            )
            .unwrap();

        let first = tick(&db, "MINE", 1001.0, &mut |_, _, _| {
            panic!("a failed commit must not send a duplicate request")
        });
        assert_eq!(first, TickSummary::default());
        assert!(super::super::control::get_rpc_result(&db, &request_id).is_some());
        assert_eq!(imported_texts(&db), Vec::<String>::new());
        let pending = load_gaps(&db, PEER);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].request_id.as_deref(), Some(request_id.as_str()));

        db.conn()
            .execute_batch("DROP TRIGGER fail_backfill_gap;")
            .unwrap();
        let second = tick(&db, "MINE", 1002.0, &mut |_, _, _| {
            panic!("the retained answer should be retried before sending")
        });
        assert_eq!(second.events_imported, 1);
        assert_eq!(second.gaps_closed, 1);
        assert!(load_gaps(&db, PEER).is_empty());
        assert_eq!(imported_texts(&db), vec!["retry-once"]);
        assert!(super::super::control::get_rpc_result(&db, &request_id).is_none());

        // A third tick sees neither a gap nor a response, so the event is
        // proved exactly once rather than merely making progress.
        let third = tick(&db, "MINE", 1003.0, &mut |_, _, _| {
            panic!("closed gap must not request again")
        });
        assert_eq!(third, TickSummary::default());
        assert_eq!(imported_texts(&db), vec!["retry-once"]);
    }
}
