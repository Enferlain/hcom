use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::{Value, json};

use crate::db::HcomDb;
use crate::sandbox::protocol::{Binding, Message, MessageId, validate_identifier};
use crate::sandbox::store::{ForwardRoute, Store};
use crate::shared::time::now_epoch_i64;

use super::{Inbound, LocalWorker};

pub struct HostInbox<'a, 'db> {
    store: &'a Store<'db>,
    source: &'a Binding,
    inbox: Inbox<'db>,
}

impl<'a, 'db> HostInbox<'a, 'db> {
    pub fn new(db: &'db HcomDb, store: &'a Store<'db>, source: &'a Binding) -> Self {
        Self {
            store,
            source,
            inbox: Inbox::new(db, false),
        }
    }
}

impl Inbound for HostInbox<'_, '_> {
    fn insert(&mut self, tx: &Transaction<'_>, message: &Message) -> Result<i64> {
        let source_active: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sandbox_workers WHERE binding = ? AND retired = 0)",
            [serde_json::to_string(self.source)?],
            |row| row.get(0),
        )?;
        ensure!(
            source_active && message.id.origin == self.source.worker,
            "sandbox sender is not enrolled"
        );
        let target = &message.payload.target;
        let destination: Option<String> = tx
            .query_row(
                "SELECT binding FROM sandbox_workers WHERE team = ? AND name = ? AND retired = 0",
                params![self.source.team, target],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(destination) = destination {
            let destination: Binding = serde_json::from_str(&destination)?;
            let event = self
                .inbox
                .insert(tx, &self.source.team, target, false, message)?;
            self.store.forward_in(
                tx,
                ForwardRoute {
                    source: self.source,
                    destination: &destination,
                },
                message,
                event,
                now_epoch_i64(),
            )?;
            return Ok(event);
        }
        let local: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sandbox_team_hosts h JOIN instances i
               ON i.name = h.name AND i.created_at = h.created_at
             WHERE h.team = ? AND h.name = ? AND COALESCE(i.origin_device_id, '') = '')",
            params![self.source.team, target],
            |row| row.get(0),
        )?;
        ensure!(local, "sandbox message target is outside the team");
        self.inbox
            .insert(tx, &self.source.team, target, true, message)
    }

    fn committed(&mut self, event_id: i64) {
        self.inbox.publish(event_id);
    }
}

pub(super) struct GuestInbox<'a> {
    inbox: Inbox<'a>,
    binding: &'a Binding,
    worker: &'a LocalWorker,
}

impl<'a> GuestInbox<'a> {
    pub(super) fn new(
        db: &'a HcomDb,
        binding: &'a Binding,
        worker: &'a LocalWorker,
    ) -> Result<Self> {
        validate_identifier(&worker.name)?;
        ensure!(
            worker.created_at.is_finite() && worker.created_at > 0.0,
            "invalid local worker generation"
        );
        Ok(Self {
            inbox: Inbox::new(db, true),
            binding,
            worker,
        })
    }
}

impl Inbound for GuestInbox<'_> {
    fn insert(&mut self, tx: &Transaction<'_>, message: &Message) -> Result<i64> {
        ensure!(
            message.payload.target == self.binding.worker.worker,
            "sandbox delivery target mismatch"
        );
        let local: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM instances WHERE name = ? AND created_at = ?
              AND COALESCE(origin_device_id, '') = '')",
            params![self.worker.name, self.worker.created_at],
            |row| row.get(0),
        )?;
        ensure!(local, "sandbox guest inbox worker is missing");
        self.inbox
            .insert(tx, &self.binding.team, &self.worker.name, true, message)
    }

    fn committed(&mut self, event_id: i64) {
        self.inbox.publish(event_id);
    }
}

struct Inbox<'a> {
    db: &'a HcomDb,
    private_guest: bool,
    pending: Option<Publication>,
}

struct Publication {
    event_id: i64,
    sender: String,
    local: Option<String>,
    data: Value,
}

impl<'a> Inbox<'a> {
    fn new(db: &'a HcomDb, private_guest: bool) -> Self {
        Self {
            db,
            private_guest,
            pending: None,
        }
    }

    fn insert(
        &mut self,
        tx: &Transaction<'_>,
        team: &str,
        target: &str,
        wake_target: bool,
        message: &Message,
    ) -> Result<i64> {
        self.pending = None;
        let payload = &message.payload;
        crate::messages::validate_message(&payload.text).map_err(anyhow::Error::msg)?;
        ensure!(
            !payload
                .text
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t'),
            "sandbox message contains prohibited control characters"
        );
        let sender = &message.id.origin.worker;
        let mut data = json!({
            "from": sender,
            "sender_kind": "instance",
            "scope": "mentions",
            "text": payload.text,
            "mentions": if wake_target { vec![target] } else { vec![] },
            "delivered_to": if wake_target { vec![target] } else { vec![] },
            "intent": payload.intent,
            "_sandbox": {"team": team, "id": message.id},
        });
        if !wake_target {
            data["queued_to"] = json!([target]);
        } else if !self.private_guest {
            let created_at: f64 = tx.query_row(
                "SELECT created_at FROM instances WHERE name = ?",
                [target],
                |row| row.get(0),
            )?;
            data["_sandbox"]["recipient_created_at"] = json!(created_at);
        }
        if let Some(thread) = &payload.thread {
            data["thread"] = json!(thread);
        }
        if let Some(reply) = &payload.reply_to {
            let reply = reply_event(tx, team, payload.thread.as_deref(), reply)?
                .context("sandbox reply is not in this team conversation")?;
            if payload.intent == crate::sandbox::protocol::Intent::Ack {
                let parent: Option<String> = tx.query_row(
                    "SELECT json_extract(data, '$.intent') FROM events WHERE id = ?",
                    [reply],
                    |row| row.get(0),
                )?;
                ensure!(
                    !matches!(parent.as_deref(), Some("ack" | "inform")),
                    "sandbox acknowledgment cannot reply to an acknowledgment or inform"
                );
            }
            data["reply_to"] = json!(reply.to_string());
            data["reply_to_local"] = json!(reply);
        }
        ensure!(
            std::ptr::eq::<rusqlite::Connection>(&**tx, self.db.conn()),
            "sandbox inbox transaction uses another database"
        );
        let event = self.db.insert_event_row("message", sender, &data, None)?;
        self.pending = Some(Publication {
            event_id: event,
            sender: sender.clone(),
            local: wake_target.then(|| target.to_owned()),
            data,
        });
        Ok(event)
    }

    fn publish(&mut self, event_id: i64) {
        let Some(Publication {
            sender,
            local,
            data,
            ..
        }) = self
            .pending
            .take()
            .filter(|event| event.event_id == event_id)
        else {
            return;
        };
        let Some(target) = local else {
            // This host event journals a forwarded hop, with no local recipient.
            return;
        };
        if self.private_guest
            && let Some(thread) = data.get("thread").and_then(Value::as_str)
        {
            self.db
                .add_thread_memberships(thread, Some(&sender), std::slice::from_ref(&target));
        }
        self.db
            .after_event_logged(event_id, "message", &sender, &data);
        crate::notify::wake(self.db, &target, &[]);
        crate::notify::wake_observers(self.db);
    }
}

fn reply_event(
    tx: &Transaction<'_>,
    team: &str,
    thread: Option<&str>,
    id: &MessageId,
) -> Result<Option<i64>> {
    // Match the full origin and sequence and constrain the event to this team.
    Ok(tx.query_row(
        "SELECT e.id FROM events e JOIN (
            SELECT event_id FROM sandbox_inbox
             WHERE worker = ?1 AND generation = ?2 AND incarnation = ?3 AND sequence = ?4
            UNION
            SELECT event_id FROM sandbox_outbox
             WHERE origin_worker = ?1 AND origin_generation = ?2
               AND COALESCE(forwarded_incarnation, (SELECT incarnation FROM sandbox_identity WHERE singleton = 1)) = ?3
               AND COALESCE(forwarded_sequence, sequence) = ?4
         ) m ON m.event_id = e.id
         WHERE e.type = 'message' AND json_extract(e.data, '$._sandbox.team') = ?5
           AND json_extract(e.data, '$.thread') IS ?6
         ORDER BY e.id LIMIT 1",
        params![id.origin.worker, id.origin.generation, id.origin.incarnation, id.sequence, team, thread],
        |row| row.get(0),
    ).optional()?)
}

#[cfg(test)]
mod tests;
