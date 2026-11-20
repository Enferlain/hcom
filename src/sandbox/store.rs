use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, params};
use uuid::Uuid;

use crate::db::HcomDb;

use super::protocol::{
    Binding, Message, MessageId, MessagePayload, Origin, Receiver, validate_message_size,
};

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS sandbox_identity (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        incarnation TEXT NOT NULL,
        last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_origin_incarnations (
        worker TEXT NOT NULL,
        generation TEXT NOT NULL,
        incarnation TEXT NOT NULL,
        PRIMARY KEY (worker, generation)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_outbox (
        sequence INTEGER PRIMARY KEY,
        route TEXT NOT NULL,
        digest BLOB NOT NULL CHECK (length(digest) = 32),
        message TEXT NOT NULL,
        event_id INTEGER NOT NULL,
        accepted INTEGER NOT NULL DEFAULT 0 CHECK (accepted IN (0, 1))
    ) STRICT;
    CREATE INDEX IF NOT EXISTS sandbox_outbox_pending
        ON sandbox_outbox (route, sequence) WHERE accepted = 0;
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
}

#[derive(Debug, PartialEq, Eq)]
pub enum Import {
    Inserted(i64),
    Replay(i64),
}

impl<'db> Store<'db> {
    /// Called by opted-in sandbox participation, outside upstream migrations.
    pub fn initialize(db: &'db HcomDb) -> Result<Self> {
        ensure_current_database(db)?;
        let incarnation = db.with_immediate_transaction(|tx| {
            tx.execute_batch(SCHEMA)?;
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
        Ok(Self { db, incarnation })
    }

    pub fn incarnation(&self) -> &str {
        &self.incarnation
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
            validate_message_size(receiver, route, &message)?;
            let event_id = insert_event(tx, &message)?;
            tx.execute(
                "INSERT INTO sandbox_outbox
                    (sequence, route, digest, message, event_id)
                 VALUES (?, ?, ?, ?, ?)",
                params![
                    sequence,
                    serde_json::to_string(route)?,
                    message.digest.as_slice(),
                    serde_json::to_string(&message)?,
                    event_id,
                ],
            )?;
            Ok(message)
        })
    }

    pub fn next_pending(&self, route: &Binding) -> Result<Option<Message>> {
        ensure_current_database(self.db)?;
        let row: Option<(String, Vec<u8>)> = self
            .db
            .conn()
            .query_row(
                "SELECT message, digest FROM sandbox_outbox
             WHERE route = ? AND accepted = 0 ORDER BY sequence LIMIT 1",
                [serde_json::to_string(route)?],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        row.map(|(body, digest)| {
            let message: Message = serde_json::from_str(&body)?;
            message.validate()?;
            ensure!(
                digest == message.digest,
                "stored sandbox outbox digest mismatch"
            );
            Ok(message)
        })
        .transpose()
    }

    /// Only a receipt for the exact pending message and authenticated route can retire it.
    pub fn acknowledge(
        &self,
        route: &Binding,
        message: &MessageId,
        digest: &[u8; 32],
    ) -> Result<()> {
        ensure!(
            message.origin.incarnation == self.incarnation,
            "receipt incarnation mismatch"
        );
        self.write(|tx| {
            let encoded = tx
                .query_row(
                    "SELECT message FROM sandbox_outbox
                 WHERE sequence = ? AND route = ? AND digest = ?",
                    params![
                        message.sequence,
                        serde_json::to_string(route)?,
                        digest.as_slice()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            let Some(encoded) = encoded else {
                anyhow::bail!("receipt does not match sandbox outbox");
            };
            let stored: Message = serde_json::from_str(&encoded)?;
            ensure!(&stored.id == message, "receipt origin mismatch");
            tx.execute(
                "UPDATE sandbox_outbox SET accepted = 1 WHERE sequence = ?",
                [message.sequence],
            )?;
            Ok(())
        })
    }

    /// Map a validated inbound message to its event in the same transaction.
    /// The callback runs once per stable ID, including across process restarts.
    /// Call outside an ambient transaction; the callback uses this call's transaction.
    pub fn import(
        &self,
        message: &Message,
        insert_event: impl FnOnce(&Transaction<'_>) -> Result<i64>,
    ) -> Result<Import> {
        message.validate()?;
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

    fn write<T>(&self, operation: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        ensure_current_database(self.db)?;
        self.db.with_immediate_transaction(|tx| {
            let result = operation(tx)?;
            ensure_current_database(self.db)?;
            Ok(result)
        })
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
