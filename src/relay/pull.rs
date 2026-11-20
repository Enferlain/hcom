//! Incoming message handling — route state vs control topics, apply remote state.
//!
//! Handles MQTT messages received from remote devices:
//! - State messages: upsert remote instances, import events
//! - Control messages: process stop/kill commands
//! - Authenticated null state: device gone (graceful cleanup)

use anyhow::Result;
use rusqlite::params;
use serde_json::Value;

use crate::db::HcomDb;
use crate::log;

use super::crypto;
use super::replay::ReplayGuard;
use super::{device_short_id_for_db, remember_device_short_id, safe_kv_get, safe_kv_set};

/// Crypto + replay context shared by all inbound message handlers.
pub struct InboundContext<'a> {
    pub psk: &'a [u8; 32],
    pub relay_id: &'a str,
    pub topic: &'a str,
    pub replay_guard: &'a mut ReplayGuard,
}

struct OpenedEnvelope {
    plaintext: Vec<u8>,
    ts_secs: u64,
}

enum ReplayPolicy {
    ControlFreshness,
    State { min_accepted_ts: Option<u64> },
}

fn state_ts_key(device_id: &str) -> String {
    format!("relay_state_ts_{}", device_id)
}

fn state_ts_watermark(db: &HcomDb, device_id: &str) -> Option<u64> {
    safe_kv_get(db, &state_ts_key(device_id)).and_then(|s| s.parse().ok())
}

fn record_state_ts_watermark(db: &HcomDb, device_id: &str, ts_secs: u64) {
    let current = state_ts_watermark(db, device_id).unwrap_or(0);
    if ts_secs > current {
        safe_kv_set(db, &state_ts_key(device_id), Some(&ts_secs.to_string()));
    }
}

/// Decrypt + replay-check an envelope coming off the wire. Returns the inner
/// JSON bytes ready for `serde_json::from_slice`. Errors are logged inline so
/// caller sites stay short.
fn open_envelope_for_handler(
    ctx: &mut InboundContext<'_>,
    sender_short: &str,
    payload: &[u8],
    replay_policy: ReplayPolicy,
) -> Option<OpenedEnvelope> {
    let parsed = match crypto::parse_envelope(payload) {
        Ok(p) => p,
        Err(e) => {
            log::log_warn("relay", "relay.bad_envelope", &format!("{}", e));
            return None;
        }
    };
    let plaintext = match crypto::open(ctx.psk, ctx.relay_id, ctx.topic, payload) {
        Ok(pt) => pt,
        Err(e) => {
            log::log_warn("relay", "relay.decrypt_fail", &format!("{}", e));
            return None;
        }
    };

    let now_secs = crate::shared::time::now_epoch_f64() as u64;
    let replay_result = match replay_policy {
        ReplayPolicy::ControlFreshness => {
            ctx.replay_guard
                .check(sender_short, parsed.nonce, parsed.ts_secs, now_secs)
        }
        ReplayPolicy::State { min_accepted_ts } => ctx.replay_guard.check_state(
            sender_short,
            parsed.nonce,
            parsed.ts_secs,
            now_secs,
            min_accepted_ts,
        ),
    };
    if let Err(e) = replay_result {
        log::log_warn("relay", "relay.replay", &format!("{}", e));
        return None;
    }
    if let Err(e) = ctx
        .replay_guard
        .record_nonce(sender_short, parsed.nonce, now_secs)
    {
        log::log_warn("relay", "relay.replay", &format!("{}", e));
        return None;
    }

    Some(OpenedEnvelope {
        plaintext,
        ts_secs: parsed.ts_secs,
    })
}

/// Handle an authenticated null state from a departing device.
/// Removes all instances belonging to the disconnected device.
pub fn handle_device_gone(db: &HcomDb, device_id: &str) {
    if let Err(e) = db.conn().execute(
        "DELETE FROM instances WHERE origin_device_id = ?",
        params![device_id],
    ) {
        log::log_error("relay", "relay.device_gone_err", &format!("{}", e));
        return;
    }
    let short_id = resolve_short_id(db, device_id);
    safe_kv_set(db, &format!("relay_sync_time_{}", device_id), None);
    safe_kv_set(db, &format!("relay_caps_{}", device_id), None);
    safe_kv_set(db, &format!("relay_ctrl_{}", device_id), None);
    safe_kv_set(db, &state_ts_key(device_id), None);
    if let Some(ref short) = short_id {
        safe_kv_set(db, &format!("relay_short_{}", short), None);
    }
    safe_kv_set(db, &format!("relay_uuid_short_{}", device_id), None);
    let prefix = super::device_id_prefix(device_id);
    let label = short_id.as_deref().unwrap_or(prefix);
    // The leave is fully applied above, so waiters can be woken as soon as it is logged.
    if emit_device_event(
        db,
        super::ACTION_DEVICE_LEAVE,
        label,
        prefix,
        &format!("device {} left the relay", label),
        false,
    ) {
        crate::notify::wake_all(db);
    }
    log::log_info("relay", "relay.device_gone", &format!("device={}", prefix));
}

/// Handle a control message from the control topic.
pub fn handle_control_message(
    db: &HcomDb,
    payload: &[u8],
    own_device: &str,
    ctx: &mut InboundContext<'_>,
) -> bool {
    let opened =
        match open_envelope_for_handler(ctx, "control", payload, ReplayPolicy::ControlFreshness) {
            Some(p) => p,
            None => return false,
        };

    let data: Value = match serde_json::from_slice(&opened.plaintext) {
        Ok(v) => v,
        Err(e) => {
            log::log_warn("relay", "relay.bad_payload", &format!("{}", e));
            return false;
        }
    };

    let source_device = data
        .get("from_device")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Ignore own control messages
    if source_device == own_device {
        return false;
    }

    let own_short_id = device_short_id_for_db(db, own_device);
    let events = if let Some(arr) = data.get("events").and_then(|v| v.as_array()) {
        arr.clone()
    } else if data.get("type").and_then(|v| v.as_str()) == Some("control") {
        vec![data.clone()]
    } else {
        vec![]
    };

    super::control::handle_control_events(db, &events, &own_short_id, source_device)
}

/// Handle a state message from a remote device.
pub fn handle_state_message(
    db: &HcomDb,
    device_id: &str,
    payload: &[u8],
    own_device: &str,
    ctx: &mut InboundContext<'_>,
) -> bool {
    let t0 = std::time::Instant::now();

    let watermark = state_ts_watermark(db, device_id);
    let opened = match open_envelope_for_handler(
        ctx,
        device_id,
        payload,
        ReplayPolicy::State {
            min_accepted_ts: watermark,
        },
    ) {
        Some(p) => p,
        None => return false,
    };

    let data: Value = match serde_json::from_slice(&opened.plaintext) {
        Ok(v) => v,
        Err(e) => {
            log::log_warn("relay", "relay.bad_payload", &format!("{}", e));
            return false;
        }
    };

    if data.get("state").is_some() && data["state"].is_null() {
        handle_device_gone(db, device_id);
        return false;
    }

    let state = data
        .get("state")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));
    let events = data
        .get("events")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let short_id = state
        .get("short_id")
        .and_then(|v| v.as_str())
        .unwrap_or(&device_id[..4.min(device_id.len())])
        .to_uppercase();
    let reset_ts = state
        .get("reset_ts")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    // A snapshot sealed in the same second as the newest one applied passes the watermark
    // (equal timestamps must, so a broker can re-deliver that same snapshot). If it also ends
    // below the events already imported and announces no newer reset than the one already
    // applied, it is an earlier snapshot arriving out of order, for example a stale retained
    // copy after a broker restart. Skip it before the peer's state or events are applied, so
    // it can neither roll that state back nor trip the id-regression reset in
    // import_remote_events. A snapshot announcing a newer reset is a new database generation
    // and always reaches the reset handling below, whatever second it was sealed in.
    let cached_reset = cached_reset_ts(db, device_id);
    if watermark == Some(opened.ts_secs)
        && reset_ts <= cached_reset
        && ends_below_cursor(db, device_id, &events)
    {
        log::log_info(
            "relay",
            "relay.stale_snapshot",
            &format!(
                "device={} ts={}",
                super::device_id_prefix(device_id),
                opened.ts_secs
            ),
        );
        return false;
    }

    // Check short_id collision (two different devices with same short_id)
    let cached_device = safe_kv_get(db, &format!("relay_short_{}", short_id));
    // A join or reconnect is logged here, before the peer's instances, capabilities and sync
    // time are applied below, so it is woken for at the END of this handler, not now.
    let mut lifecycle_logged = false;
    if let Some(ref cached) = cached_device {
        if cached != device_id {
            log::log_warn(
                "relay",
                "relay.collision",
                &format!(
                    "short_id={} existing={} incoming={}",
                    short_id,
                    super::device_id_prefix(cached),
                    super::device_id_prefix(device_id)
                ),
            );
            return false; // Skip to prevent data corruption
        }
        // Known device — check if it's a reconnect (was offline, now back)
        let last_sync: f64 = safe_kv_get(db, &format!("relay_sync_time_{}", device_id))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let now = crate::shared::time::now_epoch_f64();
        if last_sync > 0.0 && (now - last_sync) > super::DEVICE_STALE_SECS {
            let prefix = super::device_id_prefix(device_id);
            lifecycle_logged = emit_device_event(
                db,
                super::ACTION_DEVICE_JOIN,
                &short_id,
                prefix,
                &format!("device {} reconnected", short_id),
                true,
            );
        }
    } else {
        safe_kv_set(db, &format!("relay_short_{}", short_id), Some(device_id));
        let prefix = super::device_id_prefix(device_id);
        lifecycle_logged = emit_device_event(
            db,
            super::ACTION_DEVICE_JOIN,
            &short_id,
            prefix,
            &format!("new device {} joined the relay", short_id),
            false,
        );
    }
    remember_device_short_id(db, device_id, &short_id);
    // Cache the peer's advertised capabilities. Distinguish three states:
    //   - "null"  → peer state arrived without a `capabilities` field at all
    //               (legacy / pre-capability peer); treated as unknown by the
    //               capability check so we don't hard-block it.
    //   - "[]"    → peer explicitly advertised an empty list (e.g. remote
    //               control disabled); capability check blocks every action.
    //   - "[...]" → explicit advertisement.
    // Missing KV key means "no state received yet" and is handled separately.
    if let Some(caps) = state.get("capabilities").and_then(|v| v.as_array()) {
        let serialized = serde_json::to_string(caps).unwrap_or_else(|_| "[]".to_string());
        safe_kv_set(db, &format!("relay_caps_{}", device_id), Some(&serialized));
    } else {
        safe_kv_set(db, &format!("relay_caps_{}", device_id), Some("null"));
    }

    // Check for device reset — clean old data before importing
    if reset_ts > cached_reset {
        let reset_result = db.with_write_scope(|| {
            db.conn().execute(
                "DELETE FROM instances WHERE origin_device_id = ?",
                params![device_id],
            )?;
            db.conn().execute(
                "DELETE FROM events WHERE json_extract(data, '$._relay.device') = ?",
                params![device_id],
            )?;
            db.kv_set(
                &format!("relay_reset_{}", device_id),
                Some(&reset_ts.to_string()),
            )?;
            db.kv_set(&format!("relay_events_{}", device_id), Some("0"))?;
            super::backfill::clear_gaps_in_scope(db, device_id)?;
            Ok::<(), anyhow::Error>(())
        });
        if let Err(error) = reset_result {
            log::log_warn(
                "relay",
                "relay.reset_failed",
                &format!("device={} error={error}", device_id),
            );
            return false;
        }
        log::log_info("relay", "relay.reset", &format!("device={}", short_id));
    }

    // Get local reset timestamp for filtering stale data.
    // Check KV first, then fall back to events table.
    let mut local_reset_ts: f64 = safe_kv_get(db, "relay_local_reset_ts")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    if local_reset_ts == 0.0 {
        // Fallback: query events table for last reset event
        let ts_opt = db
            .conn()
            .query_row(
                "SELECT timestamp FROM events
             WHERE type='life' AND instance='_device'
               AND json_extract(data, '$.action')='reset'
               AND json_extract(data, '$._relay') IS NULL
             ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .ok()
            .flatten();

        if let Some(ts_str) = ts_opt {
            let ts = parse_ts(Some(&serde_json::Value::String(ts_str)));
            if ts > 0.0 {
                local_reset_ts = ts;
                // Cache in KV for future calls
                safe_kv_set(db, "relay_local_reset_ts", Some(&ts.to_string()));
            }
        }
    }

    // Upsert remote instances
    let own_short_id = device_short_id_for_db(db, own_device);
    let instances = state
        .get("instances")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    let mut seen_instances = std::collections::HashSet::new();

    for (name, inst) in &instances {
        let status_time = inst
            .get("status_time")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let status_time_i64 = status_time as i64;

        // Local reset wins: ignore remote snapshots older than our reset so
        // cleared instances don't reappear from broker-retained state.
        if local_reset_ts > 0.0 && status_time < local_reset_ts {
            continue;
        }

        let namespaced = super::add_device_suffix(name, &short_id);
        seen_instances.insert(namespaced.clone());

        let parent = inst
            .get("parent")
            .and_then(|v| v.as_str())
            .map(|p| super::add_device_suffix(p, &short_id));

        let now = crate::shared::time::now_epoch_f64();

        let _ = db.conn().execute(
            "INSERT INTO instances (
                name, origin_device_id, status, status_context, status_detail, status_time,
                parent_name, directory, transcript_path, created_at,
                session_id, parent_session_id, agent_id, wait_timeout, last_stop, tcp_mode,
                tag, tool, background
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                status = excluded.status,
                status_context = excluded.status_context, status_detail = excluded.status_detail,
                status_time = excluded.status_time,
                parent_name = excluded.parent_name,
                directory = excluded.directory, transcript_path = excluded.transcript_path,
                session_id = excluded.session_id, parent_session_id = excluded.parent_session_id,
                agent_id = excluded.agent_id, wait_timeout = excluded.wait_timeout,
                last_stop = excluded.last_stop, tcp_mode = excluded.tcp_mode,
                tag = excluded.tag, tool = excluded.tool, background = excluded.background",
            params![
                namespaced,
                device_id,
                inst.get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown"),
                inst.get("context").and_then(|v| v.as_str()).unwrap_or(""),
                inst.get("detail").and_then(|v| v.as_str()).unwrap_or(""),
                status_time_i64,
                parent,
                inst.get("directory").and_then(|v| v.as_str()),
                inst.get("transcript").and_then(|v| v.as_str()),
                now,
                Option::<String>::None,
                Option::<String>::None,
                Option::<String>::None,
                inst.get("wait_timeout")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(86400),
                inst.get("last_stop")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                inst.get("tcp_mode")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                inst.get("tag").and_then(|v| v.as_str()),
                inst.get("tool")
                    .and_then(|v| v.as_str())
                    .unwrap_or("claude"),
                inst.get("background")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            ],
        );
    }

    // Remove stale instances (no longer in remote state)
    let current_remote: Vec<String> = db
        .conn()
        .prepare("SELECT name FROM instances WHERE origin_device_id = ?")
        .ok()
        .map(|mut stmt| {
            stmt.query_map(params![device_id], |row| row.get::<_, String>(0))
                .ok()
                .map(|rows| rows.filter_map(|r| r.ok()).collect())
                .unwrap_or_default()
        })
        .unwrap_or_default();

    for name in &current_remote {
        if !seen_instances.contains(name) {
            let _ = db
                .conn()
                .execute("DELETE FROM instances WHERE name = ?", params![name]);
        }
    }

    // Handle control events in the events payload
    let should_push = super::control::handle_control_events(db, &events, &own_short_id, device_id);

    // Import remote events with dedup
    let imported_new_events = match import_remote_events(
        db,
        device_id,
        &short_id,
        &events,
        local_reset_ts,
        &own_short_id,
    ) {
        Ok(imported) => imported,
        Err(error) => {
            // Keep the peer cursor and any pending gap unchanged. The next
            // retained snapshot or backfill retry can safely replay the
            // events after the transient database failure is gone.
            log::log_warn(
                "relay",
                "relay.import_failed",
                &format!("device={} error={error}", short_id),
            );
            return false;
        }
    };

    // Update sync timestamp
    let now = crate::shared::time::now_epoch_f64();
    safe_kv_set(
        db,
        &format!("relay_sync_time_{}", device_id),
        Some(&now.to_string()),
    );

    // Update relay_device_count and relay_last_sync
    let device_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(DISTINCT origin_device_id) FROM instances \
             WHERE origin_device_id IS NOT NULL AND origin_device_id != ''",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    safe_kv_set(db, "relay_device_count", Some(&device_count.to_string()));
    safe_kv_set(db, "relay_last_sync", Some(&now.to_string()));
    record_state_ts_watermark(db, device_id, opened.ts_secs);

    let apply_ms = t0.elapsed().as_millis();
    log::log_with_fields(
        "INFO",
        "relay",
        "relay.recv",
        "",
        &[
            ("device", &short_id),
            ("events", &events.len().to_string()),
            ("instances", &instances.len().to_string()),
            ("apply_ms", &apply_ms.to_string()),
            ("payload_bytes", &payload.len().to_string()),
        ],
    );

    // A retained state snapshot arrives on every peer heartbeat. Waking every
    // local endpoint for a snapshot whose event cursor did not advance turns
    // relay liveness traffic into a permanent TCP fan-out storm on large
    // registries. Wake only when the snapshot actually changed local work. A join or
    // reconnect logged above counts as local work, and waking for it HERE, after the peer's
    // state is applied, means a waiter that acts on it finds the peer synced.
    if should_push || imported_new_events || lifecycle_logged {
        crate::notify::wake_all(db);
    }

    should_push
}

/// The reset generation already applied for a peer (0.0 when none has been seen).
fn cached_reset_ts(db: &HcomDb, device_id: &str) -> f64 {
    safe_kv_get(db, &format!("relay_reset_{}", device_id))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

/// The imported-event cursor for a peer (0 before first contact or after a reset).
fn event_cursor(db: &HcomDb, device_id: &str) -> i64 {
    safe_kv_get(db, &format!("relay_events_{}", device_id))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn event_cursor_checked(db: &HcomDb, device_id: &str) -> Result<i64> {
    Ok(db
        .kv_get(&format!("relay_events_{}", device_id))?
        .and_then(|s| s.parse().ok())
        .unwrap_or(0))
}

/// The highest event id a snapshot carries, control events excepted (0 when it carries none).
fn remote_max_event_id(events: &[Value]) -> i64 {
    events
        .iter()
        .filter(|e| e.get("type").and_then(|v| v.as_str()) != Some("control"))
        .filter_map(|e| e.get("id").and_then(|v| v.as_i64()))
        .max()
        .unwrap_or(0)
}

/// True when a snapshot's events end below what was already imported from that peer: the
/// condition import_remote_events treats as a recreated peer database.
fn ends_below_cursor(db: &HcomDb, device_id: &str, events: &[Value]) -> bool {
    let cursor = event_cursor(db, device_id);
    let remote_max_id = remote_max_event_id(events);
    cursor > 0 && remote_max_id > 0 && remote_max_id < cursor
}

/// Import remote events with cursor-based dedup.
fn import_remote_events(
    db: &HcomDb,
    device_id: &str,
    short_id: &str,
    events: &[Value],
    local_reset_ts: f64,
    own_short_id: &str,
) -> Result<bool> {
    db.with_write_scope(|| {
        let mut last_event_id = event_cursor_checked(db, device_id)?;

        // Detect ID regression (remote DB recreated without a reset event).
        if !events.is_empty() && last_event_id > 0 {
            let remote_max_id = remote_max_event_id(events);
            if remote_max_id > 0 && remote_max_id < last_event_id {
                log::log_info(
                    "relay",
                    "relay.reset",
                    &format!(
                        "device={} reason=id_regression:{}<{}",
                        short_id, remote_max_id, last_event_id
                    ),
                );
                db.conn().execute(
                    "DELETE FROM instances WHERE origin_device_id = ?",
                    params![device_id],
                )?;
                db.conn().execute(
                    "DELETE FROM events WHERE json_extract(data, '$._relay.device') = ?",
                    params![device_id],
                )?;
                last_event_id = 0;
                db.kv_set(&format!("relay_events_{}", device_id), Some("0"))?;
                super::backfill::clear_gaps_in_scope(db, device_id)?;
            }
        }

        // A snapshot carries a contiguous run of the peer's own-origin events.
        // If it starts above our cursor, record the skipped range before the
        // cursor moves. This write is in the same scope as event insertion and
        // cursor advancement, so a failed insert cannot lose the gap.
        if last_event_id > 0 {
            let oldest_carried = events
                .iter()
                .filter(|e| e.get("type").and_then(|v| v.as_str()) != Some("control"))
                .filter(|e| e.get("instance").and_then(|v| v.as_str()) != Some("_device"))
                .filter_map(|e| e.get("id").and_then(|v| v.as_i64()))
                .min();
            if let Some(oldest) = oldest_carried
                && oldest > last_event_id + 1
            {
                super::backfill::record_gap_in_scope(
                    db,
                    device_id,
                    short_id,
                    last_event_id,
                    oldest,
                )?;
            }
        }

        let mut max_event_id = last_event_id;
        for event in events {
            if event.get("type").and_then(|v| v.as_str()) == Some("control")
                || event.get("instance").and_then(|v| v.as_str()) == Some("_device")
            {
                continue;
            }
            let event_id = match event.get("id").and_then(|v| v.as_i64()) {
                Some(id) => id,
                None => {
                    log::log_warn(
                        "relay",
                        "relay.bad_event_id",
                        &format!("Skipping event with bad/missing id: {:?}", event.get("id")),
                    );
                    continue;
                }
            };
            if event_id <= last_event_id {
                continue;
            }
            let event_ts = event_epoch(event);
            if local_reset_ts > 0.0 && event_ts > 0.0 && event_ts < local_reset_ts {
                continue;
            }

            insert_remote_event(db, device_id, short_id, event_id, event, own_short_id)?;
            max_event_id = max_event_id.max(event_id);
        }

        let imported_new_events = max_event_id > last_event_id;
        if imported_new_events {
            db.kv_set(
                &format!("relay_events_{}", device_id),
                Some(&max_event_id.to_string()),
            )?;
        }
        Ok(imported_new_events)
    })
}

/// Seconds since the epoch for a relayed event's `ts` (0.0 when absent).
pub(crate) fn event_epoch(event: &Value) -> f64 {
    parse_ts(event.get("ts"))
}

/// A relayed event's `ts` as stored in the local `timestamp` column.
pub(crate) fn event_ts_string(event: &Value) -> String {
    match event.get("ts") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

/// Namespace one remote event and insert it locally. Shared by snapshot import
/// and catch-up backfill so both produce identical rows.
pub(crate) fn insert_remote_event(
    db: &HcomDb,
    device_id: &str,
    short_id: &str,
    event_id: i64,
    event: &Value,
    own_short_id: &str,
) -> Result<()> {
    let event_ts = event_epoch(event);

    // Namespace instance name
    let instance = event.get("instance").and_then(|v| v.as_str()).unwrap_or("");
    let namespaced_instance =
        if !instance.is_empty() && !instance.contains(':') && !instance.starts_with('_') {
            super::add_device_suffix(instance, short_id)
        } else {
            instance.to_string()
        };

    // Clone and namespace data fields
    let mut data = event
        .get("data")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));

    // Namespace asymmetry by design:
    // - `instance` / `from` keep the remote short_id suffix -> globally unique history
    // - `mentions` / `delivered_to` strip *our own* suffix -> local delivery still matches
    if let Some(obj) = data.as_object_mut() {
        // `reply_to_local` is a cache of the sender's SQLite row and is
        // meaningless after the event crosses a database boundary. Drop
        // it before resolving the canonical origin against this DB so a
        // stale foreign row can never cancel an unrelated local request.
        let reply_to = obj.get("reply_to").and_then(Value::as_str);
        let reply_to_local = reply_to.and_then(|reply_to| {
            resolve_imported_reply_to_local(db, reply_to, short_id, own_short_id)
        });
        obj.remove("reply_to_local");
        if let Some(reply_to_local) = reply_to_local {
            obj.insert("reply_to_local".to_string(), Value::from(reply_to_local));
        }

        // Namespace 'from' field
        if let Some(from) = obj.get("from").and_then(|v| v.as_str()).map(String::from)
            && !from.contains(':')
        {
            obj.insert(
                "from".to_string(),
                Value::String(super::add_device_suffix(&from, short_id)),
            );
        }

        // Strip own device suffix from mentions and delivered_to
        for field in &["mentions", "delivered_to"] {
            if let Some(arr) = obj.get(*field).and_then(|v| v.as_array()).cloned() {
                let fixed: Vec<Value> = arr
                    .iter()
                    .filter_map(|v| v.as_str())
                    .map(|name| Value::String(strip_device_suffix(name, own_short_id)))
                    .collect();
                obj.insert(field.to_string(), Value::Array(fixed));
            }
        }

        // Store relay origin
        obj.insert(
            "_relay".to_string(),
            serde_json::json!({
                "device": device_id,
                "short": short_id,
                "id": event_id,
            }),
        );
    }

    // Insert event
    let ts_str = event_ts_string(event);
    let event_type = event
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    db.log_event_with_ts(event_type, &namespaced_instance, &data, Some(&ts_str))?;

    // Log per-message latency for message events
    if event_type == "message" && event_ts > 0.0 {
        let now = crate::shared::time::now_epoch_f64();
        let latency_ms = ((now - event_ts) * 1000.0) as i64;
        log::log_with_fields(
            "INFO",
            "relay",
            "relay.msg_recv",
            "",
            &[
                ("device", short_id),
                ("instance", &namespaced_instance),
                ("latency_ms", &latency_ms.to_string()),
            ],
        );
    }
    Ok(())
}

/// Resolve an imported message's canonical reply origin to this database's
/// local event ID. The sender's `reply_to_local` is a foreign SQLite row and
/// must never be trusted across the relay boundary.
fn resolve_imported_reply_to_local(
    db: &HcomDb,
    reply_to: &str,
    sender_short_id: &str,
    own_short_id: &str,
) -> Option<i64> {
    let reply_to = reply_to.trim();
    if reply_to.is_empty() {
        return None;
    }

    let origin_and_short = super::control::split_device_suffix(reply_to);
    let (origin_str, origin_short) = match origin_and_short {
        Some(parts) => parts,
        None if reply_to.contains(':') => return None,
        None => (reply_to, sender_short_id),
    };
    let origin_id = origin_str.parse::<i64>().ok()?;
    if origin_id <= 0 || origin_short.is_empty() {
        return None;
    }

    if origin_short == own_short_id {
        // A canonical self-device suffix identifies the local origin row
        // directly. Require a positive message row, rather than accepting an
        // arbitrary event with the same integer ID.
        let local_id: Option<i64> = db
            .conn()
            .query_row(
                "SELECT id FROM events WHERE id = ? AND id > 0 AND type = 'message' \
                 AND json_type(data, '$._relay') IS NULL",
                params![origin_id],
                |row| row.get(0),
            )
            .ok();
        return local_id;
    }

    let mut stmt = db
        .conn()
        .prepare(
            "SELECT id FROM events WHERE type = 'message' \
             AND json_type(data, '$._relay.id') = 'integer' \
             AND json_extract(data, '$._relay.id') = ? \
             AND json_extract(data, '$._relay.short') = ? \
             LIMIT 2",
        )
        .ok()?;
    let matching_ids: Vec<i64> = stmt
        .query_map(params![origin_id, origin_short], |row| row.get(0))
        .ok()?
        .flatten()
        .collect();
    (matching_ids.len() == 1).then(|| matching_ids[0])
}

/// Reverse lookup: find short_id for a device UUID.
fn resolve_short_id(db: &HcomDb, device_id: &str) -> Option<String> {
    if let Some(short_id) = safe_kv_get(db, &format!("relay_uuid_short_{}", device_id)) {
        return Some(short_id);
    }
    if let Ok(entries) = db.kv_prefix("relay_short_") {
        for (key, val) in entries {
            if val == device_id {
                return Some(key.trim_start_matches("relay_short_").to_string());
            }
        }
    }
    None
}

/// Emit a relay device lifecycle event. The caller wakes waiters after applying
/// any associated state change.
fn emit_device_event(
    db: &HcomDb,
    action: &str,
    short_id: &str,
    device_id_prefix: &str,
    text: &str,
    reconnect: bool,
) -> bool {
    let mut data = serde_json::json!({
        "action": action,
        "short_id": short_id,
        "device_id": device_id_prefix,
        "text": text,
    });
    if reconnect {
        data["reconnect"] = serde_json::json!(true);
    }
    db.log_event("life", "", &data).is_ok()
}

/// Strip own device suffix from a name (case-insensitive).
/// e.g. "nuvi:RIVA" with own_short_id="RIVA" → "nuvi"
fn strip_device_suffix(name: &str, own_short_id: &str) -> String {
    let suffix = format!(":{}", own_short_id);
    if name.len() > suffix.len() && name[name.len() - suffix.len()..].eq_ignore_ascii_case(&suffix)
    {
        name[..name.len() - suffix.len()].to_string()
    } else {
        name.to_string()
    }
}

/// Parse timestamp (float or ISO string) to f64 epoch seconds.
fn parse_ts(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(Value::String(s)) => chrono::DateTime::parse_from_rfc3339(s)
            .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ"))
            .ok()
            .map(|dt| dt.timestamp() as f64)
            .unwrap_or(0.0),
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::test_helpers::isolated_test_env;
    use serde_json::json;
    use serial_test::serial;

    fn fixture_psk() -> [u8; 32] {
        [0x33; 32]
    }

    fn seal_for_test(payload: &serde_json::Value, topic: &str, relay_id: &str) -> Vec<u8> {
        let psk = fixture_psk();
        let bytes = serde_json::to_vec(payload).unwrap();
        let now = crate::shared::time::now_epoch_f64() as u64;
        crate::relay::crypto::seal(&psk, relay_id, topic, &bytes, now).unwrap()
    }

    fn insert_message_with_id(db: &HcomDb, id: i64, data: &serde_json::Value) {
        db.conn()
            .execute(
                "INSERT INTO events (id, timestamp, type, instance, data) \
                 VALUES (?, '2026-10-02T00:00:00Z', 'message', 'relay-test', ?)",
                params![id, data.to_string()],
            )
            .unwrap();
    }

    fn relay_message(origin_id: i64, short: &str, text: &str) -> serde_json::Value {
        json!({
            "from": format!("parent:{short}"),
            "text": text,
            "_relay": {
                "device": format!("device-{short}"),
                "short": short,
                "id": origin_id,
            },
        })
    }

    fn reply_event(
        id: i64,
        reply_to: Option<&str>,
        reply_to_local: Option<i64>,
    ) -> serde_json::Value {
        let mut data = json!({
            "from": "responder",
            "scope": "mentions",
            "delivered_to": ["requester"],
            "intent": "inform",
            "text": format!("reply-{id}"),
        });
        if let Some(reply_to) = reply_to {
            data["reply_to"] = json!(reply_to);
        }
        if let Some(reply_to_local) = reply_to_local {
            data["reply_to_local"] = json!(reply_to_local);
        }
        json!({
            "id": id,
            "type": "message",
            "instance": "responder",
            "ts": "2026-10-02T00:00:01Z",
            "data": data,
        })
    }

    fn imported_data(db: &HcomDb, origin_id: i64, device_id: &str) -> serde_json::Value {
        let data: String = db
            .conn()
            .query_row(
                "SELECT data FROM events \
                 WHERE json_extract(data, '$._relay.device') = ? \
                   AND json_extract(data, '$._relay.id') = ?",
                params![device_id, origin_id],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str(&data).unwrap()
    }

    fn add_request_watch(db: &HcomDb, request_id: i64, target: &str) {
        crate::db::subscriptions::create_request_watches(
            db,
            "requester",
            request_id,
            &[target.to_string()],
        );
    }

    #[test]
    #[serial]
    fn import_remote_reply_normalizes_origin_and_discards_foreign_cache() {
        let (_dir, hcom_dir, _home, _guard) = isolated_test_env();
        let receiver = HcomDb::open_at(&hcom_dir.join("receiver.db")).unwrap();
        let remote = HcomDb::open_at(&hcom_dir.join("remote.db")).unwrap();

        // Device BOXE sent request 42. Device WAVE imported it as local row
        // 100 and is now replying with that foreign local cache.
        insert_message_with_id(
            &receiver,
            42,
            &json!({"from": "requester", "text": "request"}),
        );
        insert_message_with_id(
            &receiver,
            100,
            &relay_message(100, "BOXE", "imported same-local-id"),
        );
        insert_message_with_id(&remote, 100, &relay_message(42, "BOXE", "imported request"));
        let foreign_local_id: i64 = remote
            .conn()
            .query_row("SELECT id FROM events WHERE id = 100", [], |row| row.get(0))
            .unwrap();

        let target = "responder:WAVE";
        add_request_watch(&receiver, 42, target);
        add_request_watch(&receiver, 100, target);

        // A same-origin ID on another device must remain distinct from the
        // local request ID 42. This is the foreign canonical lookup case.
        insert_message_with_id(
            &receiver,
            300,
            &relay_message(42, "WAVE", "same origin, other device"),
        );
        add_request_watch(&receiver, 300, target);

        let first_batch = vec![
            reply_event(200, Some("42:BOXE"), Some(foreign_local_id)),
            reply_event(201, Some("42:WAVE"), Some(foreign_local_id)),
        ];
        import_remote_events(&receiver, "device-wave", "WAVE", &first_batch, 0.0, "BOXE").unwrap();

        assert!(
            receiver
                .kv_get("events_sub:reqwatch-42-responder:WAVE")
                .unwrap()
                .is_none(),
            "canonical self-device reply must cancel the local request 42"
        );
        assert!(
            receiver
                .kv_get("events_sub:reqwatch-300-responder:WAVE")
                .unwrap()
                .is_none(),
            "foreign canonical reply must resolve the matching relay origin"
        );
        assert!(
            receiver
                .kv_get("events_sub:reqwatch-100-responder:WAVE")
                .unwrap()
                .is_some(),
            "foreign reply_to_local must not cancel unrelated local request 100"
        );

        let self_reply = imported_data(&receiver, 200, "device-wave");
        assert_eq!(self_reply["reply_to"], "42:BOXE");
        assert_eq!(self_reply["reply_to_local"], 42);
        let foreign_reply = imported_data(&receiver, 201, "device-wave");
        assert_eq!(foreign_reply["reply_to"], "42:WAVE");
        assert_eq!(foreign_reply["reply_to_local"], 300);

        // Bare legacy references use the originating sender's relay metadata,
        // never an arbitrary receiver row with the same numeric ID.
        insert_message_with_id(&receiver, 900, &relay_message(77, "WAVE", "legacy parent"));
        insert_message_with_id(
            &receiver,
            77,
            &json!({"from": "local", "text": "colliding local row"}),
        );
        add_request_watch(&receiver, 900, target);
        import_remote_events(
            &receiver,
            "device-wave",
            "WAVE",
            &[reply_event(202, Some("77"), Some(foreign_local_id))],
            0.0,
            "BOXE",
        )
        .unwrap();
        assert!(
            receiver
                .kv_get("events_sub:reqwatch-900-responder:WAVE")
                .unwrap()
                .is_none(),
            "bare legacy reply must resolve the originating sender metadata"
        );
        let legacy_reply = imported_data(&receiver, 202, "device-wave");
        assert_eq!(legacy_reply["reply_to"], "77");
        assert_eq!(legacy_reply["reply_to_local"], 900);

        // Ambiguous, malformed, unknown, and missing origins all discard the
        // foreign cache and leave the imported request row 100 active.
        insert_message_with_id(
            &receiver,
            500,
            &relay_message(42, "WAVE", "duplicate origin"),
        );
        let unresolved = vec![
            reply_event(203, Some("42:WAVE"), Some(foreign_local_id)),
            reply_event(204, Some("42:boxe"), Some(foreign_local_id)),
            reply_event(205, Some("99:WAVE"), Some(foreign_local_id)),
            reply_event(206, Some(""), Some(foreign_local_id)),
            reply_event(207, Some("100:BOXE"), Some(foreign_local_id)),
        ];
        import_remote_events(&receiver, "device-wave", "WAVE", &unresolved, 0.0, "BOXE").unwrap();
        assert!(
            receiver
                .kv_get("events_sub:reqwatch-100-responder:WAVE")
                .unwrap()
                .is_some(),
            "unresolved replies must not cancel the unrelated request"
        );
        for origin_id in 203..=207 {
            let data = imported_data(&receiver, origin_id, "device-wave");
            assert!(
                data.get("reply_to_local").is_none(),
                "unresolved origin {origin_id} must not invent reply_to_local: {data}"
            );
        }
    }

    #[test]
    #[serial]
    fn test_handle_state_message_drops_remote_unique_identity_fields() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();

        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "instances": {
                    "orla": {
                        "status": "active",
                        "context": "",
                        "detail": "",
                        "status_time": crate::shared::time::now_epoch_f64(),
                        "parent": serde_json::Value::Null,
                        "directory": "/tmp/demo-parent",
                        "transcript": "/tmp/demo-parent/transcript.jsonl",
                        "wait_timeout": 42,
                        "last_stop": 0.0,
                        "tcp_mode": false,
                        "tag": "demo",
                        "tool": "codex",
                        "background": false
                    },
                    "luna": {
                        "status": "active",
                        "context": "",
                        "detail": "",
                        "status_time": crate::shared::time::now_epoch_f64(),
                        "parent": "orla",
                        "directory": "/tmp/demo",
                        "transcript": "/tmp/demo/transcript.jsonl",
                        "wait_timeout": 42,
                        "last_stop": 0.0,
                        "tcp_mode": false,
                        "tag": "demo",
                        "tool": "codex",
                        "background": false
                    }
                }
            },
            "events": []
        });

        let topic = "relay-test/device-1234";
        let envelope = seal_for_test(&payload, topic, "relay-test");
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();
        handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        );

        let row = db
            .get_instance_full("luna:ABCD")
            .unwrap()
            .expect("remote row");
        assert_eq!(row.parent_name.as_deref(), Some("orla:ABCD"));
        assert_eq!(row.session_id, None);
        assert_eq!(row.parent_session_id, None);
        assert_eq!(row.agent_id, None);
        assert_eq!(row.tool, "codex");
    }

    #[test]
    #[serial]
    fn test_handle_state_message_caches_remote_capabilities() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();

        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "capabilities": ["launch", "resume"],
                "instances": {}
            },
            "events": []
        });

        let topic = "relay-test/device-1234";
        let envelope = seal_for_test(&payload, topic, "relay-test");
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();
        assert!(!handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));

        assert_eq!(
            safe_kv_get(&db, "relay_caps_device-1234").as_deref(),
            Some(r#"["launch","resume"]"#)
        );
    }

    #[test]
    #[serial]
    fn test_handle_state_message_caches_legacy_peer_without_capabilities() {
        // Peers that predate the `capabilities` advertisement must be cached
        // with the "null" sentinel, not "[]". The capability check in
        // relay::control reads this sentinel as `CachedCapabilities::Legacy`
        // and lets requests through optimistically so rolling upgrades don't
        // break remote actions against older peers.
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();

        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "instances": {}
            },
            "events": []
        });

        let topic = "relay-test/device-1234";
        let envelope = seal_for_test(&payload, topic, "relay-test");
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();
        assert!(!handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));

        assert_eq!(
            safe_kv_get(&db, "relay_caps_device-1234").as_deref(),
            Some("null"),
            "legacy peer (no capabilities field) must be cached as the \"null\" sentinel"
        );
    }

    #[test]
    #[serial]
    fn test_handle_state_message_accepts_sender_clock_skew_in_both_directions() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();
        let now = crate::shared::time::now_epoch_f64() as i64;

        for (device_id, short_id, offset) in [
            ("device-past", "PAST", -61_i64),
            ("device-future", "FUTR", 61_i64),
        ] {
            let payload = json!({
                "state": {
                    "short_id": short_id,
                    "reset_ts": 0.0,
                    "instances": {
                        "luna": {
                            "status": "active",
                            "context": "",
                            "detail": "",
                            "status_time": crate::shared::time::now_epoch_f64(),
                            "parent": serde_json::Value::Null,
                            "directory": "/tmp/demo",
                            "transcript": "/tmp/demo/transcript.jsonl",
                            "wait_timeout": 42,
                            "last_stop": 0.0,
                            "tcp_mode": false,
                            "tag": serde_json::Value::Null,
                            "tool": "codex",
                            "background": false
                        }
                    }
                },
                "events": []
            });
            let topic = format!("relay-test/{device_id}");
            let bytes = serde_json::to_vec(&payload).unwrap();
            let envelope = crate::relay::crypto::seal(
                &psk,
                "relay-test",
                &topic,
                &bytes,
                (now + offset) as u64,
            )
            .unwrap();

            assert!(!handle_state_message(
                &db,
                device_id,
                &envelope,
                "own-device-5678",
                &mut InboundContext {
                    psk: &psk,
                    relay_id: "relay-test",
                    topic: &topic,
                    replay_guard: &mut guard,
                },
            ));
            assert!(
                db.get_instance_full(&format!("luna:{short_id}"))
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    #[serial]
    fn test_handle_state_message_rejects_rollback_behind_watermark() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        safe_kv_set(&db, "relay_state_ts_device-1234", Some("1500"));

        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "instances": {
                    "luna": {
                        "status": "active",
                        "context": "",
                        "detail": "",
                        "status_time": crate::shared::time::now_epoch_f64(),
                        "parent": serde_json::Value::Null,
                        "directory": "/tmp/demo",
                        "transcript": "/tmp/demo/transcript.jsonl",
                        "wait_timeout": 42,
                        "last_stop": 0.0,
                        "tcp_mode": false,
                        "tag": serde_json::Value::Null,
                        "tool": "codex",
                        "background": false
                    }
                }
            },
            "events": []
        });

        let topic = "relay-test/device-1234";
        let bytes = serde_json::to_vec(&payload).unwrap();
        let envelope =
            crate::relay::crypto::seal(&fixture_psk(), "relay-test", topic, &bytes, 1000).unwrap();
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();

        assert!(!handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));

        assert!(db.get_instance_full("luna:ABCD").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_decrypt_failure_does_not_consume_replay_slot() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let topic = "relay-test/device-1234";
        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "instances": {}
            },
            "events": []
        });

        let good_envelope = seal_for_test(&payload, topic, "relay-test");
        let mut bad_psk = fixture_psk();
        bad_psk[0] ^= 0x55;
        let bad_bytes = serde_json::to_vec(&payload).unwrap();
        let bad_envelope =
            crate::relay::crypto::seal(&bad_psk, "relay-test", topic, &bad_bytes, 1234).unwrap();

        let mut guard = ReplayGuard::new(1, 600, crate::relay::replay::MAX_SKEW_SECS);
        let psk = fixture_psk();

        assert!(!handle_state_message(
            &db,
            "device-1234",
            &bad_envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));
        assert_eq!(
            guard.len(),
            0,
            "failed decrypt must not record replay nonce"
        );

        assert!(!handle_state_message(
            &db,
            "device-1234",
            &good_envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));
        assert_eq!(guard.len(), 1);
    }

    #[test]
    #[serial]
    fn test_handle_state_message_authenticated_null_state_cleans_up_device_and_watermark() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, origin_device_id, created_at) VALUES (?1, ?2, ?3)",
                rusqlite::params!["luna:ABCD", "device-1234", 1.0],
            )
            .unwrap();
        safe_kv_set(&db, "relay_state_ts_device-1234", Some("1500"));

        let payload = json!({
            "state": serde_json::Value::Null,
            "events": [],
        });
        let topic = "relay-test/device-1234";
        let bytes = serde_json::to_vec(&payload).unwrap();
        let envelope =
            crate::relay::crypto::seal(&fixture_psk(), "relay-test", topic, &bytes, 2000).unwrap();
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();

        assert!(!handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));

        assert!(db.get_instance_full("luna:ABCD").unwrap().is_none());
        assert_eq!(safe_kv_get(&db, "relay_state_ts_device-1234"), None);
    }

    fn own_event(id: i64, text: &str) -> serde_json::Value {
        json!({
            "id": id,
            "ts": "2026-09-26T10:00:00.000000+00:00",
            "type": "message",
            "instance": "luna",
            "data": {"from": "luna", "text": text, "mentions": ["nova"]},
        })
    }

    #[test]
    #[serial]
    fn a_snapshot_starting_above_the_cursor_records_the_skipped_range() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        safe_kv_set(&db, "relay_events_device-1234", Some("100"));

        let events = vec![own_event(180, "a"), own_event(190, "b")];
        assert!(import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap());

        let gaps = crate::relay::backfill::load_gaps(&db, "device-1234");
        assert_eq!(gaps.len(), 1);
        assert_eq!((gaps[0].after, gaps[0].before), (100, 180));
        assert_eq!(gaps[0].short_id, "ABCD");
        assert_eq!(
            safe_kv_get(&db, "relay_events_device-1234").as_deref(),
            Some("190")
        );
    }

    #[test]
    #[serial]
    fn an_overlapping_snapshot_records_no_gap() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        safe_kv_set(&db, "relay_events_device-1234", Some("100"));

        let events = vec![own_event(100, "seen"), own_event(150, "new")];
        assert!(import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap());
        assert!(crate::relay::backfill::load_gaps(&db, "device-1234").is_empty());
    }

    #[test]
    #[serial]
    fn a_first_contact_starts_from_the_tail_without_a_gap() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();

        let events = vec![own_event(5000, "tail")];
        assert!(import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap());
        assert!(crate::relay::backfill::load_gaps(&db, "device-1234").is_empty());
    }

    #[test]
    #[serial]
    fn an_id_regression_clears_recorded_gaps() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        safe_kv_set(&db, "relay_events_device-1234", Some("100"));
        crate::relay::backfill::record_gap(&db, "device-1234", "ABCD", 40, 90).unwrap();

        let events = vec![own_event(3, "fresh")];
        import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap();
        assert!(crate::relay::backfill::load_gaps(&db, "device-1234").is_empty());
    }

    fn apply_snapshot_at(db: &HcomDb, events: Vec<serde_json::Value>, ts_secs: u64) {
        apply_snapshot_with_reset_at(db, events, ts_secs, 0.0);
    }

    fn apply_snapshot_with_reset_at(
        db: &HcomDb,
        events: Vec<serde_json::Value>,
        ts_secs: u64,
        reset_ts: f64,
    ) {
        let topic = "relay-test/device-1234";
        let payload = json!({
            "state": {"short_id": "ABCD", "reset_ts": reset_ts, "instances": {}},
            "events": events
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let psk = fixture_psk();
        let envelope =
            crate::relay::crypto::seal(&psk, "relay-test", topic, &bytes, ts_secs).unwrap();
        let mut guard = ReplayGuard::default();
        handle_state_message(
            db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        );
    }

    fn imported_from_peer(db: &HcomDb) -> i64 {
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE json_extract(data, '$._relay.device') = 'device-1234'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    }

    #[test]
    #[serial]
    fn a_same_second_older_snapshot_is_skipped_not_treated_as_a_reset() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        apply_snapshot_at(&db, vec![own_event(10, "a"), own_event(11, "b")], 2000);
        crate::relay::backfill::record_gap(&db, "device-1234", "ABCD", 3, 9).unwrap();
        assert_eq!(imported_from_peer(&db), 2);

        apply_snapshot_at(&db, vec![own_event(5, "old")], 2000);

        assert_eq!(imported_from_peer(&db), 2);
        assert_eq!(
            safe_kv_get(&db, "relay_events_device-1234").as_deref(),
            Some("11")
        );
        assert_eq!(
            crate::relay::backfill::load_gaps(&db, "device-1234").len(),
            1
        );
    }

    #[test]
    #[serial]
    fn a_same_second_snapshot_announcing_a_newer_reset_still_resets() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        apply_snapshot_at(&db, vec![own_event(10, "a"), own_event(11, "b")], 2000);
        crate::relay::backfill::record_gap(&db, "device-1234", "ABCD", 3, 9).unwrap();

        apply_snapshot_with_reset_at(&db, vec![own_event(3, "fresh")], 2000, 1999.5);

        assert_eq!(imported_from_peer(&db), 1);
        assert_eq!(
            safe_kv_get(&db, "relay_events_device-1234").as_deref(),
            Some("3")
        );
        assert!(crate::relay::backfill::load_gaps(&db, "device-1234").is_empty());
    }

    #[test]
    #[serial]
    fn a_later_snapshot_below_the_cursor_still_resets() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        apply_snapshot_at(&db, vec![own_event(10, "a"), own_event(11, "b")], 2000);
        crate::relay::backfill::record_gap(&db, "device-1234", "ABCD", 3, 9).unwrap();

        apply_snapshot_at(&db, vec![own_event(3, "fresh")], 2001);

        assert_eq!(imported_from_peer(&db), 1);
        assert_eq!(
            safe_kv_get(&db, "relay_events_device-1234").as_deref(),
            Some("3")
        );
        assert!(crate::relay::backfill::load_gaps(&db, "device-1234").is_empty());
    }

    fn imported_count(db: &HcomDb, device_id: &str) -> i64 {
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM events \
                 WHERE json_extract(data, '$._relay.device') = ?",
                params![device_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    #[serial]
    fn failed_remote_event_insert_rolls_back_gap_and_cursor_and_retries_once() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.kv_set("relay_events_device-1234", Some("100")).unwrap();
        crate::relay::backfill::record_gap(&db, "device-1234", "ABCD", 40, 90).unwrap();

        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_remote_event BEFORE INSERT ON events
                 WHEN NEW.type = 'message'
                   AND json_extract(NEW.data, '$._relay.device') = 'device-1234'
                 BEGIN SELECT RAISE(ABORT, 'injected remote event failure'); END;",
            )
            .unwrap();
        let events = vec![own_event(180, "first"), own_event(190, "second")];
        let error =
            import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap_err();
        assert!(error.to_string().contains("injected remote event failure"));
        assert_eq!(
            safe_kv_get(&db, "relay_events_device-1234").as_deref(),
            Some("100")
        );
        assert_eq!(
            crate::relay::backfill::load_gaps(&db, "device-1234"),
            vec![crate::relay::backfill::Gap {
                after: 40,
                before: 90,
                short_id: "ABCD".to_string(),
                detected_at: crate::relay::backfill::load_gaps(&db, "device-1234")[0].detected_at,
                request_id: None,
                sent_at: 0.0,
                attempts: 0,
                recovered: 0,
                wide: false,
                shrink: 0,
            }]
        );
        assert_eq!(imported_count(&db, "device-1234"), 0);

        db.conn()
            .execute_batch("DROP TRIGGER fail_remote_event;")
            .unwrap();
        assert!(import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap());
        assert_eq!(imported_count(&db, "device-1234"), 2);
        assert_eq!(
            safe_kv_get(&db, "relay_events_device-1234").as_deref(),
            Some("190")
        );
        assert!(!import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap());
        assert_eq!(imported_count(&db, "device-1234"), 2);
    }

    #[test]
    #[serial]
    fn failed_gap_write_propagates_without_creating_partial_gap() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_gap_write BEFORE INSERT ON kv
                 WHEN NEW.key = 'relay_gap_device-1234'
                 BEGIN SELECT RAISE(ABORT, 'injected gap failure'); END;",
            )
            .unwrap();

        let error =
            crate::relay::backfill::record_gap(&db, "device-1234", "ABCD", 10, 30).unwrap_err();
        assert!(error.to_string().contains("injected gap failure"));
        assert!(crate::relay::backfill::load_gaps(&db, "device-1234").is_empty());

        db.conn()
            .execute_batch("DROP TRIGGER fail_gap_write;")
            .unwrap();
        crate::relay::backfill::record_gap(&db, "device-1234", "ABCD", 10, 30).unwrap();
        assert_eq!(
            crate::relay::backfill::load_gaps(&db, "device-1234")[0].after,
            10
        );
    }

    #[test]
    #[serial]
    fn failed_cursor_write_rolls_back_import_and_retry_is_exactly_once() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.kv_set("relay_events_device-1234", Some("100")).unwrap();
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_cursor_write BEFORE INSERT ON kv
                 WHEN NEW.key = 'relay_events_device-1234'
                 BEGIN SELECT RAISE(ABORT, 'injected cursor failure'); END;",
            )
            .unwrap();
        let events = vec![own_event(180, "only")];
        let error =
            import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap_err();
        assert!(error.to_string().contains("injected cursor failure"));
        assert_eq!(imported_count(&db, "device-1234"), 0);
        assert_eq!(
            safe_kv_get(&db, "relay_events_device-1234").as_deref(),
            Some("100")
        );
        assert!(crate::relay::backfill::load_gaps(&db, "device-1234").is_empty());

        db.conn()
            .execute_batch("DROP TRIGGER fail_cursor_write;")
            .unwrap();
        assert!(import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap());
        assert_eq!(imported_count(&db, "device-1234"), 1);
        assert!(!import_remote_events(&db, "device-1234", "ABCD", &events, 0.0, "MINE").unwrap());
        assert_eq!(imported_count(&db, "device-1234"), 1);
    }
}
