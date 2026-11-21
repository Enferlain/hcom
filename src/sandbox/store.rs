use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, params};
use uuid::Uuid;

use crate::db::HcomDb;

use super::admission::Limits;
use super::protocol::{
    Binding, Message, MessageId, MessagePayload, Origin, Receiver, validate_message_size,
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
        accepted INTEGER NOT NULL DEFAULT 0 CHECK (accepted IN (0, 1, 2))
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
";

pub struct Store<'db> {
    db: &'db HcomDb,
    incarnation: String,
    limits: Limits,
    bridge: Option<(String, i64)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    Pending,
    AwaitingExpiry,
    Accepted,
    Expired,
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
}

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
            let current: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sandbox_bridge_epochs WHERE route = ? AND epoch = ?)",
                params![key, epoch],
                |row| row.get(0),
            )?;
            ensure!(current, "stale sandbox connection epoch");
        }
        Ok(())
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
            let sequence = tx
                .query_row(
                    "UPDATE sandbox_identity SET last_sequence = last_sequence + 1
                     WHERE singleton = 1 AND incarnation = ? RETURNING last_sequence",
                    [&self.incarnation],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| anyhow::anyhow!("sandbox outbox database incarnation changed"))?;
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

    pub fn next_pending(&self, route: &Binding, now: i64) -> Result<Option<Message>> {
        ensure_current_database(self.db)?;
        let row = self
            .db
            .conn()
            .query_row(
                "SELECT message, digest, sequence, origin_worker, origin_generation FROM sandbox_outbox
                 WHERE route = ? AND accepted = 0 AND expires_at > ? ORDER BY sequence LIMIT 1",
                params![serde_json::to_string(route)?, now],
                |row| Ok((
                    row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?, row.get::<_, String>(4)?,
                )),
            )
            .optional()?;
        row.map(|(body, digest, sequence, worker, generation)| {
            let message: Message = serde_json::from_str(&body)?;
            message.validate()?;
            ensure!(
                message.id.sequence == sequence
                    && message.id.origin.worker == worker
                    && message.id.origin.generation == generation
                    && message.id.origin.incarnation == self.incarnation,
                "stored sandbox outbox identity mismatch"
            );
            ensure!(
                digest == message.digest,
                "stored sandbox outbox digest mismatch"
            );
            Ok(message)
        })
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
        ensure!(
            message.origin.incarnation == self.incarnation,
            "receipt incarnation mismatch"
        );
        self.write(|tx| {
            let changed = tx.execute(
                "UPDATE sandbox_outbox SET accepted = 1
                 WHERE sequence = ? AND origin_worker = ? AND origin_generation = ?
                   AND route = ? AND digest = ?
                   AND (accepted = 1 OR (accepted = 0 AND expires_at > ?))",
                params![
                    message.sequence,
                    message.origin.worker,
                    message.origin.generation,
                    serde_json::to_string(route)?,
                    digest.as_slice(),
                    now,
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

    pub fn delivery(&self, id: &MessageId, now: i64) -> Result<Option<Delivery>> {
        ensure_current_database(self.db)?;
        if id.origin.incarnation != self.incarnation {
            return Ok(None);
        }
        let row: Option<(i64, i64)> = self
            .db
            .conn()
            .query_row(
                "SELECT accepted, expires_at FROM sandbox_outbox
                 WHERE sequence = ? AND origin_worker = ? AND origin_generation = ?",
                params![id.sequence, id.origin.worker, id.origin.generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((status, expires_at)) = row else {
            return Ok(None);
        };
        Ok(Some(match status {
            0 if expires_at > now => Delivery::Pending,
            0 => Delivery::AwaitingExpiry,
            1 => Delivery::Accepted,
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
            let event_id = insert_event(tx)?;
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
