use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

#[cfg(unix)]
use super::protocol::{Activity, ObservationKey, Origin};
use super::protocol::{Binding, MessageId};
#[cfg(unix)]
use super::store::Store;
use crate::shared::time::{monotonic_seconds, now_epoch_i64};

pub(super) const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS sandbox_local_delivery (
        event_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        created_at REAL NOT NULL,
        PRIMARY KEY (event_id, name, created_at)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_remote_delivery (
        sequence INTEGER PRIMARY KEY REFERENCES sandbox_outbox(sequence) ON DELETE CASCADE
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_bridge_activity (
        route TEXT PRIMARY KEY,
        epoch INTEGER NOT NULL,
        last_seen INTEGER NOT NULL,
        monotonic_seen INTEGER NOT NULL,
        activity TEXT,
        last_sequence INTEGER NOT NULL,
        connection_state INTEGER NOT NULL CHECK (connection_state IN (0, 1, 2))
    ) STRICT;
    CREATE INDEX IF NOT EXISTS sandbox_outbox_observation
        ON sandbox_outbox(route, sequence) WHERE accepted = 1;
";
const LIVE_SECONDS: i64 = 20;

fn has_table(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?)",
        [name],
        |row| row.get(0),
    )?)
}

/// The hook output commit names the exact emitted events. Never infer
/// this evidence from the inbox cursor, wakeups, or a transport receipt.
pub(crate) fn record_local(
    tx: &Transaction<'_>,
    name: &str,
    created_at: f64,
    events: &[i64],
) -> Result<()> {
    if events.is_empty() || !has_table(tx, "sandbox_local_delivery")? {
        return Ok(());
    }
    let mut statement = tx.prepare_cached(
        "INSERT OR IGNORE INTO sandbox_local_delivery
         SELECT e.id, ?2, ?3 FROM events e
         WHERE e.id = ?1 AND e.type = 'message'
         AND CASE WHEN json_valid(e.data) THEN (
           json_type(e.data, '$._sandbox.id') = 'object'
           AND EXISTS (SELECT 1 FROM json_each(e.data, '$.delivered_to') d WHERE d.value = ?2)
           AND (json_extract(e.data, '$._sandbox.recipient_created_at') = ?3
                OR EXISTS (
                    SELECT 1 FROM json_each(e.data, '$._sandbox.recipient_created_ats') g
                    WHERE g.key = ?2 AND g.value = ?3
                )
                OR (json_type(e.data, '$._sandbox.recipient_created_at') IS NULL
                    AND json_type(e.data, '$._sandbox.recipient_created_ats') IS NULL
                    AND EXISTS (
                        SELECT 1 FROM sandbox_guest_directory d
                        WHERE d.local_name = ?2 AND d.local_created_at = ?3
                          AND json_extract(d.binding, '$.team') = json_extract(e.data, '$._sandbox.team')
                    )))
         ) ELSE 0 END",
    )?;
    for event in events {
        statement.execute(params![event, name, created_at])?;
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn observed(
    conn: &Connection,
    key: &ObservationKey,
    name: &str,
    created_at: f64,
) -> Result<bool> {
    let id = &key.id;
    Ok(conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sandbox_inbox i JOIN sandbox_local_delivery d ON d.event_id = i.event_id
             WHERE i.worker = ?1 AND i.generation = ?2 AND i.incarnation = ?3
               AND i.sequence = ?4 AND i.digest = ?5 AND d.name = ?6 AND d.created_at = ?7
         )",
        params![
            id.origin.worker,
            id.origin.generation,
            id.origin.incarnation,
            id.sequence,
            key.digest.as_slice(),
            name,
            created_at
        ],
        |row| row.get(0),
    )?)
}

#[cfg(unix)]
impl Store<'_> {
    pub(super) fn observation_query(
        &self,
        binding: &Binding,
    ) -> Result<Option<(i64, ObservationKey)>> {
        let route = serde_json::to_string(binding)?;
        let conn = self.connection()?;
        let after = conn
            .query_row(
                "SELECT last_sequence FROM sandbox_bridge_activity WHERE route = ?",
                [&route],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        let next = |after| -> Result<Option<(i64, ObservationKey)>> {
            let row = conn.query_row(
                "SELECT q.sequence, q.origin_worker, q.origin_generation,
                        COALESCE(q.forwarded_incarnation, ?3),
                        COALESCE(q.forwarded_sequence, q.sequence), q.digest
                 FROM sandbox_outbox q
                 WHERE q.route = ?1 AND q.sequence > ?2 AND q.accepted = 1
                   AND NOT EXISTS (SELECT 1 FROM sandbox_remote_delivery d WHERE d.sequence = q.sequence)
                 ORDER BY q.sequence LIMIT 1",
                params![route, after, self.incarnation()],
                |r| Ok((r.get::<_, i64>(0)?, MessageId {
                    origin: Origin { worker: r.get(1)?, generation: r.get(2)?, incarnation: r.get(3)? },
                    sequence: r.get(4)?,
                }, r.get::<_, Vec<u8>>(5)?)),
            ).optional()?;
            row.map(|(sequence, id, digest)| {
                Ok((
                    sequence,
                    ObservationKey {
                        id,
                        digest: digest
                            .try_into()
                            .map_err(|_| anyhow::anyhow!("invalid stored receipt digest"))?,
                    },
                ))
            })
            .transpose()
        };
        match next(after)? {
            None if after != 0 => next(0),
            result => Ok(result),
        }
    }

    pub(super) fn record_bridge_observation(
        &self,
        binding: &Binding,
        epoch: i64,
        requested: Option<(i64, &ObservationKey)>,
        delivered: bool,
        activity: Option<Activity>,
    ) -> Result<()> {
        let route = serde_json::to_string(binding)?;
        self.write(|tx| {
            self.check_binding(tx, binding)?;
            if delivered {
                let (sequence, key) =
                    requested.context("sandbox observation has no requested route")?;
                let id = &key.id;
                let accepted: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sandbox_outbox
                     WHERE sequence = ?1 AND route = ?2 AND digest = ?3 AND accepted = 1
                       AND origin_worker = ?4 AND origin_generation = ?5
                       AND COALESCE(forwarded_incarnation, ?6) = ?7
                       AND COALESCE(forwarded_sequence, sequence) = ?8)",
                    params![
                        sequence,
                        route,
                        key.digest.as_slice(),
                        id.origin.worker,
                        id.origin.generation,
                        self.incarnation(),
                        id.origin.incarnation,
                        id.sequence
                    ],
                    |row| row.get(0),
                )?;
                ensure!(
                    accepted,
                    "sandbox observation has no matching accepted route"
                );
                tx.execute(
                    "INSERT OR IGNORE INTO sandbox_remote_delivery VALUES (?)",
                    [sequence],
                )?;
            }
            tx.execute(
                "INSERT INTO sandbox_bridge_activity VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)
                 ON CONFLICT(route) DO UPDATE
                 SET epoch = excluded.epoch, last_seen = excluded.last_seen,
                     monotonic_seen = excluded.monotonic_seen, connection_state = 0,
                     activity = COALESCE(excluded.activity, activity),
                     last_sequence = excluded.last_sequence
                 WHERE excluded.epoch >= sandbox_bridge_activity.epoch",
                params![
                    route,
                    epoch,
                    now_epoch_i64(),
                    monotonic_seconds()?,
                    activity.map(|state| state.as_str()),
                    requested.map_or(0, |(sequence, _)| sequence)
                ],
            )?;
            Ok(())
        })
    }

    pub(super) fn finish_bridge_observation(
        &self,
        binding: &Binding,
        epoch: i64,
        rotating: bool,
    ) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "UPDATE sandbox_bridge_activity SET connection_state = ?3 WHERE route = ?1 AND epoch = ?2",
                params![serde_json::to_string(binding)?, epoch, if rotating { 2 } else { 1 }],
            )?;
            Ok(())
        })
    }
}

fn bridge_activity(conn: &Connection, route: &str, now: i64) -> Result<Value> {
    if has_table(conn, "sandbox_bridge_activity")? {
        let row = conn
            .query_row(
                "SELECT b.epoch, b.last_seen, b.monotonic_seen, b.activity, b.connection_state,
                    e.epoch = b.epoch
             FROM sandbox_bridge_activity b LEFT JOIN sandbox_bridge_epochs e ON e.route = b.route
             WHERE b.route = ?",
                [route],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, Option<bool>>(5)?.unwrap_or(false),
                    ))
                },
            )
            .optional()?;
        if let Some((epoch, seen, monotonic, activity, state, current)) = row {
            let fresh = now_epoch_i64()
                .checked_sub(seen)
                .is_some_and(|age| (0..=LIVE_SECONDS).contains(&age))
                && now
                    .checked_sub(monotonic)
                    .is_some_and(|age| (0..=LIVE_SECONDS).contains(&age));
            let connection = match (state, current, fresh) {
                (0, true, true) => "connected",
                (2, _, true) => "rotating",
                _ => "disconnected",
            };
            return Ok(json!({
                "connection": connection,
                "epoch": epoch, "last_seen": seen, "last_activity": activity
            }));
        }
    }
    Ok(json!({"connection": "unknown", "last_activity": null}))
}

pub(super) fn show(event_id: i64) -> Result<()> {
    ensure!(
        event_id > 0,
        "sandbox receipt requires a positive local event ID"
    );
    let path = crate::paths::hcom_dir().join("hcom.db");
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(std::time::Duration::from_secs(2))?;
    let tx = conn.unchecked_transaction()?;
    let view = view(&tx, event_id)?;
    println!("{}", serde_json::to_string(&view)?);
    Ok(())
}

pub(super) fn view(conn: &Connection, event_id: i64) -> Result<Value> {
    let data: String = conn
        .query_row(
            "SELECT data FROM events WHERE id = ? AND type = 'message'",
            [event_id],
            |r| r.get(0),
        )
        .optional()?
        .context("sandbox message event not found")?;
    let data: Value = serde_json::from_str(&data)?;
    let id: MessageId = serde_json::from_value(data["_sandbox"]["id"].clone())
        .context("event has no sandbox message identity")?;
    let mut routes = Vec::new();
    let now = now_epoch_i64();
    let clock = monotonic_seconds()?;
    if has_table(conn, "sandbox_outbox")? {
        let has_observed = has_table(conn, "sandbox_remote_delivery")?;
        let evidence = if has_observed {
            "EXISTS(SELECT 1 FROM sandbox_remote_delivery d WHERE d.sequence = q.sequence)"
        } else {
            "0"
        };
        let mut stmt = conn.prepare(&format!(
            "SELECT route, accepted, rejected, expires_at, {evidence}
             FROM sandbox_outbox q WHERE event_id = ? ORDER BY sequence"
        ))?;
        let mut rows = stmt.query([event_id])?;
        while let Some(row) = rows.next()? {
            let route: String = row.get(0)?;
            let binding: Binding = serde_json::from_str(&route)?;
            let transport = match (row.get::<_, i64>(1)?, row.get::<_, bool>(2)?) {
                (0, _) if row.get::<_, i64>(3)? <= now => "awaiting_expiry",
                (0, _) => "pending",
                (1, false) => "accepted",
                (2, true) => "rejected",
                (2, false) => "expired",
                _ => anyhow::bail!("invalid sandbox delivery state"),
            };
            let guest_destination = id.origin != binding.worker;
            routes.push(json!({
                "next_hop": if guest_destination { "guest" } else { "host_router" },
                "worker": binding.worker.worker,
                "generation": binding.worker.generation,
                "transport": transport,
                "agent_delivery": if row.get::<_, bool>(4)? { "observed" } else { "unknown" },
                "bridge": if guest_destination { bridge_activity(conn, &route, clock)? } else { Value::Null }
            }));
        }
    }
    let mut local = Vec::new();
    let local_evidence = has_table(conn, "sandbox_local_delivery")?;
    if let Some(names) = data["delivered_to"].as_array() {
        for name in names.iter().filter_map(Value::as_str) {
            let observed = local_evidence && conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sandbox_local_delivery WHERE event_id = ? AND name = ?)",
                params![event_id, name], |r| r.get::<_, bool>(0),
            )?;
            local.push(json!({"recipient": name, "agent_delivery": if observed { "observed" } else { "unknown" }}));
        }
    }
    Ok(
        json!({"event_id": event_id, "id": id, "queued_locally": !routes.is_empty(),
              "routes": routes, "local_recipients": local}),
    )
}
