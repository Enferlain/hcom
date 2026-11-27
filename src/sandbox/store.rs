use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, params};
use uuid::Uuid;

use crate::db::HcomDb;

use super::admission::Limits;
use super::protocol::{
    Binding, InvalidMessageText, Message, MessageId, MessagePayload, Origin, Receiver,
    validate_message_size,
};

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS sandbox_identity (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        incarnation TEXT NOT NULL,
        last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_controller_clock (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        epoch INTEGER NOT NULL CHECK (epoch > 0)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_bridge_epochs (
        route TEXT PRIMARY KEY,
        epoch INTEGER NOT NULL CHECK (epoch > 0)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_usage (
        scope TEXT NOT NULL,
        scope_key TEXT NOT NULL,
        messages INTEGER NOT NULL,
        bytes INTEGER NOT NULL,
        PRIMARY KEY (scope, scope_key)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_pacing (
        run_key TEXT PRIMARY KEY,
        started_at INTEGER NOT NULL,
        messages INTEGER NOT NULL,
        bytes INTEGER NOT NULL
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_origin_incarnations (
        worker TEXT NOT NULL,
        generation TEXT NOT NULL,
        incarnation TEXT NOT NULL,
        PRIMARY KEY (worker, generation)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_outbox (
        sequence INTEGER PRIMARY KEY,
        origin_worker TEXT NOT NULL,
        origin_generation TEXT NOT NULL,
        route TEXT NOT NULL,
        digest BLOB NOT NULL CHECK (length(digest) = 32),
        message TEXT NOT NULL,
        event_id INTEGER NOT NULL,
        wire_bytes INTEGER NOT NULL CHECK (wire_bytes > 0),
        expires_at INTEGER NOT NULL,
        accepted INTEGER NOT NULL DEFAULT 0 CHECK (accepted IN (0, 1, 2)),
        rejected INTEGER NOT NULL DEFAULT 0 CHECK (rejected IN (0, 1)),
        forwarded_sequence INTEGER CHECK (forwarded_sequence > 0),
        forwarded_incarnation TEXT
    ) STRICT;
    CREATE INDEX IF NOT EXISTS sandbox_outbox_pending
        ON sandbox_outbox (route, sequence) WHERE accepted = 0;
    CREATE INDEX IF NOT EXISTS sandbox_outbox_expiring
        ON sandbox_outbox (expires_at, sequence) WHERE accepted = 0;
    CREATE TABLE IF NOT EXISTS sandbox_inbox (
        worker TEXT NOT NULL,
        generation TEXT NOT NULL,
        incarnation TEXT NOT NULL,
        sequence INTEGER NOT NULL CHECK (sequence > 0),
        digest BLOB NOT NULL CHECK (length(digest) = 32),
        event_id INTEGER NOT NULL,
        PRIMARY KEY (worker, generation, incarnation, sequence)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_rejected_head (
        route TEXT PRIMARY KEY,
        sequence INTEGER NOT NULL CHECK (sequence > 0),
        digest BLOB NOT NULL CHECK (length(digest) = 32)
    ) STRICT;
";

pub struct Store<'db> {
    db: &'db HcomDb,
    incarnation: String,
    limits: Limits,
    bridge: Option<(String, i64)>,
}

#[derive(Clone, Copy)]
pub struct ForwardRoute<'a> {
    pub source: &'a Binding,
    pub destination: &'a Binding,
}

/// One authorized host-origin hop of an [`Store::enqueue_host_batch`] fan-out.
/// The payload carries that destination's target so every hop shares the
/// batch's wire identity while keeping its own directed digest.
#[derive(Clone)]
pub struct BatchHop {
    pub route: Binding,
    pub payload: MessagePayload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    Pending,
    AwaitingExpiry,
    Accepted,
    Expired,
    Rejected,
}

#[derive(Debug)]
pub struct ExpiredMessage {
    pub sequence: i64,
    pub event_id: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Import {
    Inserted(i64),
    Replay(i64),
    Rejected,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(super) struct UnavailableRecipient(pub &'static str);

impl<'db> Store<'db> {
    /// Called by opted-in sandbox participation, outside upstream migrations.
    pub fn initialize(db: &'db HcomDb) -> Result<Self> {
        Self::with_limits(db, Limits::default())
    }

    pub fn with_limits(db: &'db HcomDb, limits: Limits) -> Result<Self> {
        limits.validate()?;
        ensure_current_database(db)?;
        let incarnation = db.with_immediate_transaction(|tx| {
            tx.execute_batch(SCHEMA)?;
            tx.execute_batch(super::controller::SCHEMA)?;
            tx.execute_batch(super::directory::SCHEMA)?;
            tx.execute_batch(super::conversation::SCHEMA)?;
            ensure_forwarding_schema(tx)?;
            tx.execute(
                "INSERT OR IGNORE INTO sandbox_identity VALUES (1, ?, 0)",
                [Uuid::new_v4().to_string()],
            )?;
            let incarnation = tx.query_row(
                "SELECT incarnation FROM sandbox_identity WHERE singleton = 1",
                [],
                |row| row.get(0),
            )?;
            ensure_current_database(db)?;
            Ok(incarnation)
        })?;
        Ok(Self {
            db,
            incarnation,
            limits,
            bridge: None,
        })
    }

    pub(super) fn path(&self) -> &std::path::Path {
        self.db.path()
    }

    pub(super) fn allocate_epoch(&self) -> Result<i64> {
        self.write(|tx| {
            Ok(tx.query_row(
                "INSERT INTO sandbox_controller_clock VALUES (1, 1)
             ON CONFLICT (singleton) DO UPDATE SET epoch = epoch + 1 RETURNING epoch",
                [],
                |row| row.get(0),
            )?)
        })
    }

    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }

    /// Attach a connection after checking its independently configured binding.
    /// Only a strictly newer epoch may take over this route.
    pub(super) fn attach_bridge(&self, binding: &Binding, epoch: i64) -> Result<Self> {
        binding.validate()?;
        ensure!(epoch > 0, "invalid sandbox connection epoch");
        let key = serde_json::to_string(binding)?;
        self.write(|tx| {
            super::directory::check_route(tx, &key)?;
            let changed = tx.execute(
                "INSERT INTO sandbox_bridge_epochs VALUES (?, ?)
                 ON CONFLICT (route) DO UPDATE SET epoch = excluded.epoch
                 WHERE excluded.epoch > sandbox_bridge_epochs.epoch",
                params![key, epoch],
            )?;
            ensure!(changed == 1, "stale sandbox connection epoch");
            Ok(())
        })?;
        Ok(Self {
            db: self.db,
            incarnation: self.incarnation.clone(),
            limits: self.limits,
            bridge: Some((key, epoch)),
        })
    }

    pub(super) fn check_bridge(&self) -> Result<()> {
        ensure_current_database(self.db)?;
        self.check_bridge_on(self.db.conn())
    }

    fn check_bridge_on(&self, conn: &rusqlite::Connection) -> Result<()> {
        if let Some((key, epoch)) = &self.bridge {
            super::directory::check_route(conn, key)?;
            let current: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sandbox_bridge_epochs WHERE route = ? AND epoch = ?)",
                params![key, epoch],
                |row| row.get(0),
            )?;
            ensure!(current, "stale sandbox connection epoch");
        }
        Ok(())
    }

    fn check_binding(&self, conn: &rusqlite::Connection, binding: &Binding) -> Result<()> {
        let key = serde_json::to_string(binding)?;
        if let Some((attached, _)) = &self.bridge {
            ensure!(*attached == key, "sandbox connection binding mismatch");
            // write() already checked the attached generation and epoch.
            Ok(())
        } else {
            super::directory::check_route(conn, &key)
        }
    }

    /// Commit the associated event and outbox row before reporting acceptance.
    /// Size and validate the wire message against the next hop's actual binding.
    /// The event writer must defer delivery/wakes until this call succeeds.
    /// Call outside an ambient transaction; the callback uses this call's transaction.
    pub fn enqueue(
        &self,
        receiver: Receiver,
        route: &Binding,
        origin: Origin,
        payload: MessagePayload,
        now: i64,
        insert_event: impl FnOnce(&Transaction<'_>, &Message) -> Result<i64>,
    ) -> Result<Message> {
        ensure!(
            origin.incarnation == self.incarnation,
            "outbox incarnation mismatch"
        );
        self.write(|tx| {
            self.check_binding(tx, route)?;
            let sequence = self.allocate_queue_sequence(tx)?;
            let message = Message::new(MessageId { origin, sequence }, payload)?;
            let wire_bytes = validate_message_size(receiver, route, &message)?;
            self.limits.pending(tx, wire_bytes)?;
            let event_id = insert_event(tx, &message)?;
            self.limits.record(tx, route, wire_bytes, event_id, None)?;
            tx.execute(
                "INSERT INTO sandbox_outbox
                    (sequence, origin_worker, origin_generation, route, digest,
                     message, event_id, wire_bytes, expires_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    sequence,
                    message.id.origin.worker,
                    message.id.origin.generation,
                    serde_json::to_string(route)?,
                    message.digest.as_slice(),
                    serde_json::to_string(&message)?,
                    event_id,
                    wire_bytes,
                    now.saturating_add(self.limits.queue_ttl_seconds),
                ],
            )?;
            Ok(message)
        })
    }

    fn allocate_queue_sequence(&self, tx: &Transaction<'_>) -> Result<i64> {
        tx.query_row(
            "UPDATE sandbox_identity SET last_sequence = last_sequence + 1
             WHERE singleton = 1 AND incarnation = ? RETURNING last_sequence",
            [&self.incarnation],
            |row| row.get(0),
        )
        .optional()?
        .context("sandbox outbox database incarnation changed")
    }

    /// Commit a host-origin fan-out: one wire identity, one journal event, and
    /// every authorized next hop in one transaction. The first hop owns the
    /// spool sequence; later hops replay the same wire identity from forwarded
    /// rows, so each destination receives the same stable message ID. The plan
    /// callback resolves authorization and hops inside the transaction; the
    /// event writer must defer delivery/wakes until this call succeeds. Call
    /// outside an ambient transaction.
    pub fn enqueue_host_batch<T>(
        &self,
        now: i64,
        plan: impl FnOnce(&Transaction<'_>, i64) -> Result<(T, Origin, Vec<BatchHop>)>,
        insert_event: impl FnOnce(&Transaction<'_>, &T, &[Message]) -> Result<i64>,
    ) -> Result<Message> {
        self.write(|tx| {
            let wire_sequence = self.allocate_queue_sequence(tx)?;
            let (context, origin, hops) = plan(tx, wire_sequence)?;
            ensure!(
                origin.incarnation == self.incarnation,
                "outbox incarnation mismatch"
            );
            ensure!(
                hops.len() <= super::protocol::MAX_DIRECTORY_PEERS,
                "sandbox fan-out exceeds the peer bound"
            );
            let mut route_keys = Vec::with_capacity(hops.len());
            for hop in &hops {
                let key = serde_json::to_string(&hop.route)?;
                ensure!(
                    !route_keys.contains(&key),
                    "sandbox fan-out repeats a destination route"
                );
                route_keys.push(key);
            }
            // Size and digest every hop first; these checks touch no queue
            // state, so they cannot pass individually and fail in aggregate.
            let mut sized = Vec::with_capacity(hops.len());
            for hop in &hops {
                self.check_binding(tx, &hop.route)?;
                let message = Message::new(
                    MessageId {
                        origin: origin.clone(),
                        sequence: wire_sequence,
                    },
                    hop.payload.clone(),
                )?;
                let wire_bytes = validate_message_size(Receiver::Guest, &hop.route, &message)?;
                sized.push((message, wire_bytes));
            }
            let refs: Vec<Message> = sized.iter().map(|(message, _)| message.clone()).collect();
            let event_id = insert_event(tx, &context, &refs)?;
            // Pending capacity is checked as each row is inserted, so the
            // shared queue bound applies to the running batch, not per hop.
            // Any failing hop rolls the whole batch back, event included.
            for (index, (hop, (message, wire_bytes))) in hops.iter().zip(&sized).enumerate() {
                self.limits.pending(tx, *wire_bytes)?;
                self.limits
                    .record(tx, &hop.route, *wire_bytes, event_id, None)?;
                let (sequence, forwarded_sequence, forwarded_incarnation) = if index == 0 {
                    (wire_sequence, None, None)
                } else {
                    let ordinal = self.allocate_queue_sequence(tx)?;
                    (
                        ordinal,
                        Some(wire_sequence),
                        Some(origin.incarnation.as_str()),
                    )
                };
                tx.execute(
                    "INSERT INTO sandbox_outbox
                        (sequence, origin_worker, origin_generation, route, digest,
                         message, event_id, wire_bytes, expires_at,
                         forwarded_sequence, forwarded_incarnation)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        sequence,
                        message.id.origin.worker,
                        message.id.origin.generation,
                        route_keys[index],
                        message.digest.as_slice(),
                        serde_json::to_string(message)?,
                        event_id,
                        wire_bytes,
                        now.saturating_add(self.limits.queue_ttl_seconds),
                        forwarded_sequence,
                        forwarded_incarnation,
                    ],
                )?;
            }
            sized
                .into_iter()
                .next()
                .map(|(message, _)| message)
                .context("sandbox fan-out produced no hops")
        })
    }

    /// Append a host-authorized next hop inside the inbound event transaction.
    /// The queue sequence orders the host spool. The sender's wire identity
    /// and digest remain unchanged across the host relay.
    pub fn forward_in(
        &self,
        tx: &Transaction<'_>,
        route: ForwardRoute<'_>,
        message: &Message,
        event_id: i64,
        now: i64,
    ) -> Result<()> {
        ensure!(
            self.bridge.is_none() && message.id.origin.incarnation != self.incarnation,
            "sandbox forward must use the host store and a foreign origin"
        );
        let ForwardRoute {
            source,
            destination,
        } = route;
        source.validate()?;
        ensure!(
            message.id.origin == source.worker
                && source.team == destination.team
                && source != destination,
            "sandbox forward does not match an authorized team hop"
        );
        self.check_binding(tx, source)?;
        self.check_binding(tx, destination)?;
        // Validating the Deliver envelope also checks its target against this binding.
        let wire_bytes = validate_message_size(Receiver::Guest, destination, message)?;
        self.limits.pending(tx, wire_bytes)?;
        let ordinal = self.allocate_queue_sequence(tx)?;
        self.limits
            .record(tx, destination, wire_bytes, event_id, None)?;
        tx.execute(
            "INSERT INTO sandbox_outbox
                (sequence, origin_worker, origin_generation, route, digest,
                 message, event_id, wire_bytes, expires_at,
                 forwarded_sequence, forwarded_incarnation)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                ordinal,
                message.id.origin.worker,
                message.id.origin.generation,
                serde_json::to_string(destination)?,
                message.digest.as_slice(),
                serde_json::to_string(message)?,
                event_id,
                wire_bytes,
                now.saturating_add(self.limits.queue_ttl_seconds),
                message.id.sequence,
                message.id.origin.incarnation,
            ],
        )?;
        Ok(())
    }

    pub fn next_pending(&self, route: &Binding, now: i64) -> Result<Option<Message>> {
        ensure_current_database(self.db)?;
        let row = self
            .db
            .conn()
            .query_row(
                "SELECT message, digest, COALESCE(forwarded_sequence, sequence),
                        origin_worker, origin_generation, COALESCE(forwarded_incarnation, ?)
                   FROM sandbox_outbox
                  WHERE route = ? AND accepted = 0 AND expires_at > ? ORDER BY sequence LIMIT 1",
                params![self.incarnation, serde_json::to_string(route)?, now],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?;
        row.map(
            |(body, digest, sequence, worker, generation, incarnation)| {
                let message: Message = serde_json::from_str(&body)?;
                message.validate()?;
                ensure!(
                    message.id.sequence == sequence
                        && message.id.origin.worker == worker
                        && message.id.origin.generation == generation
                        && message.id.origin.incarnation == incarnation,
                    "stored sandbox outbox identity mismatch"
                );
                ensure!(
                    digest == message.digest,
                    "stored sandbox outbox digest mismatch"
                );
                Ok(message)
            },
        )
        .transpose()
    }

    /// Rediscover eligible routes after restart. Invalid stored bindings are
    /// reported per route so they do not prevent recovery of valid queues.
    pub fn pending_routes(&self, now: i64) -> Result<Vec<Result<Binding>>> {
        ensure_current_database(self.db)?;
        let mut statement = self.db.conn().prepare(
            "SELECT route, min(sequence) FROM sandbox_outbox
             WHERE accepted = 0 AND expires_at > ? GROUP BY route ORDER BY min(sequence)",
        )?;
        let mut rows = statement.query([now])?;
        let mut routes = Vec::new();
        while let Some(row) = rows.next()? {
            let sequence: i64 = row.get(1)?;
            let binding = (|| -> Result<Binding> {
                let route: String = row.get(0)?;
                let binding: Binding = serde_json::from_str(&route)?;
                binding.validate()?;
                Ok(binding)
            })();
            routes.push(
                binding
                    .with_context(|| format!("unreadable sandbox route for sequence {sequence}")),
            );
        }
        Ok(routes)
    }

    /// Only a receipt for the exact pending message and authenticated route can retire it.
    pub fn acknowledge(
        &self,
        route: &Binding,
        message: &MessageId,
        digest: &[u8; 32],
        now: i64,
    ) -> Result<()> {
        self.settle(route, message, digest, now, false)
    }

    #[cfg(unix)]
    pub(super) fn reject(
        &self,
        route: &Binding,
        message: &MessageId,
        digest: &[u8; 32],
        now: i64,
    ) -> Result<()> {
        ensure!(
            message.origin == route.worker,
            "sandbox rejection sender mismatch"
        );
        self.settle(route, message, digest, now, true)
    }

    fn settle(
        &self,
        route: &Binding,
        message: &MessageId,
        digest: &[u8; 32],
        now: i64,
        rejected: bool,
    ) -> Result<()> {
        self.write(|tx| {
            self.check_binding(tx, route)?;
            let changed = tx.execute(
                "UPDATE sandbox_outbox SET accepted = ?9, rejected = ?10
                 WHERE ((sequence = ?1 AND forwarded_sequence IS NULL AND ?2 = ?3)
                        OR (forwarded_sequence = ?1 AND forwarded_incarnation = ?3))
                   AND origin_worker = ?4 AND origin_generation = ?5
                   AND route = ?6 AND digest = ?7
                   AND ((accepted = ?9 AND rejected = ?10)
                        OR (accepted = 0 AND expires_at > ?8))",
                params![
                    message.sequence,
                    self.incarnation,
                    message.origin.incarnation,
                    message.origin.worker,
                    message.origin.generation,
                    serde_json::to_string(route)?,
                    digest.as_slice(),
                    now,
                    if rejected { 2 } else { 1 },
                    rejected,
                ],
            )?;
            ensure!(changed == 1, "receipt does not match sandbox outbox");
            Ok(())
        })
    }

    /// Persist terminal failures in bounded batches. A late receipt cannot
    /// resurrect an expired send. History quotas remain charged. Call until
    /// this returns an empty batch when draining after downtime.
    pub fn expire_pending(&self, now: i64) -> Result<Vec<ExpiredMessage>> {
        self.write(|tx| {
            let mut stmt = tx.prepare(
                "UPDATE sandbox_outbox SET accepted = 2
                 WHERE sequence IN (
                    SELECT sequence FROM sandbox_outbox WHERE accepted = 0 AND expires_at <= ?
                    ORDER BY expires_at, sequence LIMIT 64
                 ) RETURNING sequence, event_id",
            )?;
            let rows = stmt.query_map([now], |row| {
                Ok(ExpiredMessage {
                    sequence: row.get(0)?,
                    event_id: row.get(1)?,
                })
            })?;
            Ok(rows.collect::<rusqlite::Result<_>>()?)
        })
    }

    pub(super) fn expire_guest_pending_if_due(&self, now: i64) -> Result<()> {
        let due: bool = self.connection()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM sandbox_outbox WHERE accepted = 0 AND expires_at <= ?)",
            [now],
            |row| row.get(0),
        )?;
        if due {
            // Persist skipped deadlines before choosing a higher sequence. A
            // backward clock change must not make that older entry eligible again.
            while !self.expire_pending(now)?.is_empty() {}
        }
        Ok(())
    }

    /// Look up a single route's transport state. Multi-route host sends expose
    /// no aggregate state here, even after all their route receipts arrive.
    /// Retention must preserve the fan-out rows together or persist a separate
    /// fan-out record before pruning individual routes (task 3.5).
    pub fn delivery(&self, id: &MessageId, now: i64) -> Result<Option<Delivery>> {
        ensure_current_database(self.db)?;
        if id.origin.incarnation != self.incarnation {
            return Ok(None);
        }
        // A fan-out wire id maps to one row per destination. A single route's
        // receipt is not a whole-batch status, so an ambiguous identity stays
        // unknown instead of reporting the first hop's state as aggregate.
        let fan_out: bool = self.db.conn().query_row(
            "SELECT EXISTS(SELECT 1 FROM sandbox_outbox
             WHERE origin_worker = ? AND origin_generation = ?
               AND forwarded_sequence = ? AND forwarded_incarnation = ?)",
            params![
                id.origin.worker,
                id.origin.generation,
                id.sequence,
                id.origin.incarnation
            ],
            |row| row.get(0),
        )?;
        if fan_out {
            return Ok(None);
        }
        let row: Option<(i64, bool, i64)> = self
            .db
            .conn()
            .query_row(
                "SELECT accepted, rejected, expires_at FROM sandbox_outbox
                 WHERE sequence = ? AND forwarded_sequence IS NULL
                   AND origin_worker = ? AND origin_generation = ?",
                params![id.sequence, id.origin.worker, id.origin.generation],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((status, rejected, expires_at)) = row else {
            return Ok(None);
        };
        Ok(Some(match status {
            0 if expires_at > now => Delivery::Pending,
            0 => Delivery::AwaitingExpiry,
            1 => Delivery::Accepted,
            2 if rejected => Delivery::Rejected,
            2 => Delivery::Expired,
            _ => anyhow::bail!("invalid sandbox delivery state"),
        }))
    }

    /// Map a validated inbound message to its event in the same transaction.
    /// The callback runs once per stable ID, including across process restarts.
    /// Call outside an ambient transaction; the callback uses this call's transaction.
    pub fn import(
        &self,
        receiver: Receiver,
        binding: &Binding,
        message: &Message,
        now: i64,
        insert_event: impl FnOnce(&Transaction<'_>) -> Result<i64>,
    ) -> Result<Import> {
        let wire_bytes = validate_message_size(receiver, binding, message)?;
        let id = &message.id;
        let origin = &id.origin;
        self.write(|tx| {
            self.check_binding(tx, binding)?;
            tx.execute(
                "INSERT OR IGNORE INTO sandbox_origin_incarnations VALUES (?, ?, ?)",
                params![origin.worker, origin.generation, origin.incarnation],
            )?;
            let incarnation: String = tx.query_row(
                "SELECT incarnation FROM sandbox_origin_incarnations
                 WHERE worker = ? AND generation = ?",
                params![origin.worker, origin.generation],
                |row| row.get(0),
            )?;
            ensure!(
                incarnation == origin.incarnation,
                "sandbox origin incarnation changed"
            );
            let existing: Option<(Vec<u8>, i64)> = tx
                .query_row(
                    "SELECT digest, event_id FROM sandbox_inbox
                 WHERE worker = ? AND generation = ? AND incarnation = ? AND sequence = ?",
                    params![
                        origin.worker,
                        origin.generation,
                        origin.incarnation,
                        id.sequence
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((digest, event_id)) = existing {
                ensure!(
                    digest == message.digest,
                    "conflicting sandbox message replay"
                );
                return Ok(Import::Replay(event_id));
            }
            let event_id = if matches!(receiver, Receiver::Host) {
                let route = serde_json::to_string(binding)?;
                let last: Option<(i64, Vec<u8>)> = tx
                    .query_row(
                        "SELECT sequence, digest FROM sandbox_rejected_head WHERE route = ?",
                        [&route],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                if let Some((sequence, digest)) = last
                    && id.sequence <= sequence
                {
                    ensure!(
                        id.sequence == sequence && digest == message.digest,
                        "stale or conflicting sandbox rejection replay"
                    );
                    return Ok(Import::Rejected);
                }

                tx.execute_batch("SAVEPOINT sandbox_inbound_event")?;
                match insert_event(tx) {
                    Ok(event_id) => {
                        tx.execute_batch("RELEASE sandbox_inbound_event")?;
                        event_id
                    }
                    Err(error)
                        if error.is::<UnavailableRecipient>()
                            || error.is::<InvalidMessageText>() =>
                    {
                        tx.execute_batch(
                            "ROLLBACK TO sandbox_inbound_event; RELEASE sandbox_inbound_event",
                        )?;
                        self.limits.pace_guest(tx, binding, wire_bytes, now)?;
                        tx.execute(
                            "INSERT INTO sandbox_rejected_head VALUES (?, ?, ?)
                             ON CONFLICT(route) DO UPDATE
                             SET sequence = excluded.sequence, digest = excluded.digest",
                            params![route, id.sequence, message.digest.as_slice()],
                        )?;
                        return Ok(Import::Rejected);
                    }
                    Err(error) => return Err(error),
                }
            } else {
                insert_event(tx)?
            };
            let paced_at = matches!(receiver, Receiver::Host).then_some(now);
            self.limits
                .record(tx, binding, wire_bytes, event_id, paced_at)?;
            tx.execute(
                "INSERT INTO sandbox_inbox VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    origin.worker,
                    origin.generation,
                    origin.incarnation,
                    id.sequence,
                    message.digest.as_slice(),
                    event_id,
                ],
            )?;
            Ok(Import::Inserted(event_id))
        })
    }

    pub(super) fn write<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        ensure_current_database(self.db)?;
        self.db.with_immediate_transaction(|tx| {
            self.check_bridge_on(tx)?;
            let result = operation(tx)?;
            ensure_current_database(self.db)?;
            Ok(result)
        })
    }

    pub(super) fn connection(&self) -> Result<&rusqlite::Connection> {
        ensure_current_database(self.db)?;
        Ok(self.db.conn())
    }
}

fn ensure_forwarding_schema(tx: &Transaction<'_>) -> Result<()> {
    for (column, sql) in [
        (
            "forwarded_sequence",
            "ALTER TABLE sandbox_outbox ADD COLUMN forwarded_sequence INTEGER CHECK (forwarded_sequence > 0)",
        ),
        (
            "forwarded_incarnation",
            "ALTER TABLE sandbox_outbox ADD COLUMN forwarded_incarnation TEXT",
        ),
        (
            "rejected",
            "ALTER TABLE sandbox_outbox ADD COLUMN rejected INTEGER NOT NULL DEFAULT 0 CHECK (rejected IN (0, 1))",
        ),
    ] {
        let present: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('sandbox_outbox') WHERE name = ?)",
            [column],
            |row| row.get(0),
        )?;
        if !present {
            tx.execute_batch(sql)?;
        }
    }
    tx.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS sandbox_outbox_forwarded
           ON sandbox_outbox(route, origin_worker, origin_generation, forwarded_incarnation, forwarded_sequence)
           WHERE forwarded_sequence IS NOT NULL;",
    )?;
    Ok(())
}

fn ensure_current_database(db: &HcomDb) -> Result<()> {
    let file_id = crate::sys::fs::file_id(db.path());
    ensure!(
        file_id != 0 && file_id == db.file_identity(),
        "sandbox database was replaced or is unavailable; reopen before using sandbox state"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
