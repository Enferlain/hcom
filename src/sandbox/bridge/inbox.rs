use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::{Value, json};

use crate::db::HcomDb;
use crate::sandbox::directory::{host_generation, host_recipient_generation};
use crate::sandbox::protocol::{
    Binding, Message, MessageId, Origin, TeamPeer, validate_identifier,
};
use crate::sandbox::store::{ForwardRoute, Store, UnavailableRecipient};
use crate::shared::time::now_epoch_i64;

use super::{Inbound, LocalWorker};

pub struct HostInbox<'a, 'db> {
    store: &'a Store<'db>,
    source: &'a Binding,
    inbox: Inbox<'db>,
}

enum Recipient<'a> {
    Guest(&'a Binding),
    Host { name: &'a str, created_at: f64 },
}

impl<'a, 'db> HostInbox<'a, 'db> {
    pub fn new(db: &'db HcomDb, store: &'a Store<'db>, source: &'a Binding) -> Self {
        Self {
            store,
            source,
            inbox: Inbox::new(db, false),
        }
    }

    fn check_reply(
        &self,
        tx: &Transaction<'_>,
        message: &Message,
        recipient: Recipient<'_>,
    ) -> Result<()> {
        let Some(parent) = &message.payload.reply_to else {
            return Ok(());
        };
        let event = reply_event(
            tx,
            &self.source.team,
            message.payload.thread.as_deref(),
            parent,
        )?
        .context("sandbox reply is not in this team conversation")?;
        let queued_for = |binding: &Binding| -> Result<bool> {
            Ok(tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sandbox_outbox WHERE event_id = ? AND route = ?)",
                params![event, serde_json::to_string(binding)?],
                |row| row.get(0),
            )?)
        };
        let matches = if parent.origin == self.source.worker {
            match recipient {
                Recipient::Guest(binding) => queued_for(binding)?,
                Recipient::Host { name, created_at } => tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM events e, json_each(e.data, '$.delivered_to') d
                     WHERE e.id = ?1 AND d.value = ?2
                       AND (json_extract(e.data, '$._sandbox.recipient_created_at') = ?3
                            OR (json_type(e.data, '$._sandbox.recipient_created_ats') = 'object'
                                AND EXISTS (
                                    SELECT 1 FROM json_each(e.data, '$._sandbox.recipient_created_ats') r
                                     WHERE r.key = ?2 AND r.value = ?3))))",
                    params![event, name, created_at],
                    |row| row.get(0),
                )?,
            }
        } else {
            let expected = match recipient {
                Recipient::Guest(binding) => binding.worker.clone(),
                Recipient::Host { name, created_at } => Origin {
                    worker: name.into(),
                    generation: host_generation(created_at),
                    incarnation: self.store.incarnation().into(),
                },
            };
            parent.origin == expected && queued_for(self.source)?
        };
        ensure!(matches, "sandbox reply is not between these participants");
        Ok(())
    }

    fn insert_broadcast(&mut self, tx: &Transaction<'_>, message: &Message) -> Result<i64> {
        ensure!(
            message.payload.target.is_empty(),
            "sandbox broadcast cannot specify a target"
        );
        ensure!(
            message.payload.target_generation.is_none(),
            "sandbox broadcast cannot specify a target generation"
        );
        ensure!(
            message.payload.thread.is_none(),
            "sandbox broadcast cannot specify a thread"
        );
        ensure!(
            message.payload.reply_to.is_none(),
            "sandbox broadcast cannot specify a reply target"
        );

        let peer_limit = (crate::sandbox::protocol::MAX_DIRECTORY_PEERS + 1) as i64;
        let mut host_stmt = tx.prepare(
            "SELECT h.name, i.created_at
             FROM sandbox_team_hosts h
             JOIN instances i ON i.name = h.name AND i.created_at = h.created_at
             WHERE h.team = ?1
               AND COALESCE(i.origin_device_id, '') = ''
               AND h.name != ?2
               AND i.status != 'stopped'
               AND i.status_context != 'launch_failed'
               AND NOT (i.status = 'inactive' AND i.status_context LIKE 'exit:%')
             ORDER BY h.name
             LIMIT ?3",
        )?;
        let host_recipients: Vec<(String, f64)> = host_stmt
            .query_map(
                params![self.source.team, self.source.worker.worker, peer_limit],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?
            .collect::<rusqlite::Result<_>>()?;
        if host_recipients.len() > crate::sandbox::protocol::MAX_DIRECTORY_PEERS {
            return Err(UnavailableRecipient("sandbox team broadcast exceeds peer limit").into());
        }

        let source_key = serde_json::to_string(self.source)?;
        let mut worker_stmt = tx.prepare(
            "SELECT w.binding
             FROM sandbox_workers w
             JOIN instances i ON i.name = w.name
               AND i.origin_device_id = 'openshell:' || w.generation
             WHERE w.team = ?1
               AND w.retired = 0
               AND w.generation != ?2
               AND w.binding != ?3
             ORDER BY w.name
             LIMIT ?4",
        )?;
        let raw_bindings: Vec<String> = worker_stmt
            .query_map(
                params![
                    self.source.team,
                    self.source.worker.generation,
                    source_key,
                    peer_limit
                ],
                |row| row.get(0),
            )?
            .collect::<rusqlite::Result<_>>()?;
        if host_recipients.len() + raw_bindings.len()
            > crate::sandbox::protocol::MAX_DIRECTORY_PEERS
        {
            return Err(UnavailableRecipient("sandbox team broadcast exceeds peer limit").into());
        }
        let mut guest_recipients = Vec::with_capacity(raw_bindings.len());
        for raw in raw_bindings {
            let destination: Binding = serde_json::from_str(&raw)?;
            ensure!(
                destination.team == self.source.team && &destination != self.source,
                "sandbox broadcast resolved invalid peer binding"
            );
            guest_recipients.push(destination);
        }
        let guest_names: Vec<String> = guest_recipients
            .iter()
            .map(|destination| destination.worker.worker.clone())
            .collect();

        let publication = self.inbox.insert_broadcast(
            tx,
            &self.source.team,
            &host_recipients,
            &guest_names,
            message,
        )?;
        let event = publication.event_id;
        let now = now_epoch_i64();
        for destination in &guest_recipients {
            self.store.forward_in(
                tx,
                ForwardRoute {
                    source: self.source,
                    destination,
                },
                message,
                event,
                now,
            )?;
        }
        self.inbox.arm(publication);
        Ok(event)
    }
}

impl Inbound for HostInbox<'_, '_> {
    fn insert(&mut self, tx: &Transaction<'_>, message: &Message) -> Result<i64> {
        self.inbox.clear();
        let source_active: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sandbox_workers WHERE binding = ? AND retired = 0)",
            [serde_json::to_string(self.source)?],
            |row| row.get(0),
        )?;
        ensure!(
            source_active && message.id.origin == self.source.worker,
            "sandbox sender is not enrolled"
        );
        ensure!(
            message.payload.conversation.is_none(),
            "guest cannot set a sandbox conversation id"
        );
        ensure!(
            message.payload.sender_routing_generation.is_none(),
            "guest cannot set a sandbox sender routing generation"
        );
        if message.payload.broadcast {
            return self.insert_broadcast(tx, message);
        }
        let target = &message.payload.target;
        let target_generation = message
            .payload
            .target_generation
            .as_deref()
            .context("sandbox guest send requires a target generation")?;
        let destination: Option<String> = tx
            .query_row(
                "SELECT binding FROM sandbox_workers WHERE team = ? AND name = ? AND retired = 0",
                params![self.source.team, target],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(destination) = destination {
            let destination: Binding = serde_json::from_str(&destination)?;
            if destination.worker.generation != target_generation {
                return Err(
                    UnavailableRecipient("sandbox message target generation is stale").into(),
                );
            }
            self.check_reply(tx, message, Recipient::Guest(&destination))?;
            let publication =
                self.inbox
                    .insert_single(tx, &self.source.team, target, false, message)?;
            let event = publication.event_id;
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
            self.inbox.arm(publication);
            return Ok(event);
        }
        let local_created_at: Option<f64> = tx
            .query_row(
                "SELECT i.created_at FROM sandbox_team_hosts h JOIN instances i
               ON i.name = h.name AND i.created_at = h.created_at
             WHERE h.team = ? AND h.name = ? AND COALESCE(i.origin_device_id, '') = ''",
                params![self.source.team, target],
                |row| row.get(0),
            )
            .optional()?;
        let local_created_at = local_created_at.ok_or(UnavailableRecipient(
            "sandbox message target is outside the team",
        ))?;
        if host_recipient_generation(tx, target, local_created_at)? != target_generation {
            return Err(UnavailableRecipient("sandbox message target generation is stale").into());
        }
        self.check_reply(
            tx,
            message,
            Recipient::Host {
                name: target,
                created_at: local_created_at,
            },
        )?;
        let publication = self
            .inbox
            .insert_single(tx, &self.source.team, target, true, message)?;
        let event = publication.event_id;
        self.inbox.arm(publication);
        Ok(event)
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

    pub(super) fn check_local_worker(&self, tx: &Transaction<'_>) -> Result<()> {
        let local: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM instances WHERE name = ? AND created_at = ?
              AND COALESCE(origin_device_id, '') = '')",
            params![self.worker.name, self.worker.created_at],
            |row| row.get(0),
        )?;
        ensure!(local, "sandbox guest inbox worker is missing");
        Ok(())
    }

    /// Retain verifiable conversation membership evidence in the import
    /// transaction. Only a host-authenticated directed message carries the
    /// opaque id; the record binds it to this route and the enrolled worker
    /// generation, and one label keeps one conversation so a later guest
    /// resolver can look it up uniquely. A repeated grant must match the
    /// retained evidence exactly; evidence never authorizes a send by itself.
    fn retain_conversation(&self, tx: &Transaction<'_>, message: &Message) -> Result<()> {
        let Some(conversation) = &message.payload.conversation else {
            return Ok(());
        };
        ensure!(
            !message.payload.broadcast,
            "sandbox conversation evidence cannot arrive on a broadcast"
        );
        let Some(label) = &message.payload.thread else {
            anyhow::bail!("sandbox conversation evidence requires a thread");
        };
        let binding = serde_json::to_string(self.binding)?;
        let generation = &self.binding.worker.generation;
        let existing: Option<(String, String, String, String)> = tx
            .query_row(
                "SELECT conversation, label, generation, binding FROM sandbox_guest_conversations
                 WHERE conversation = ? OR label = ?",
                params![conversation, label],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        match existing {
            None => {
                tx.execute(
                    "INSERT INTO sandbox_guest_conversations
                     (conversation, label, generation, binding) VALUES (?, ?, ?, ?)",
                    params![conversation, label, generation, binding],
                )?;
            }
            Some((retained_id, retained_label, retained_generation, retained_binding)) => {
                ensure!(
                    retained_id == *conversation
                        && retained_label == *label
                        && retained_generation == *generation
                        && retained_binding == binding,
                    "sandbox conversation evidence conflicts with its retained grant"
                );
            }
        }
        Ok(())
    }
}

impl Inbound for GuestInbox<'_> {
    fn insert(&mut self, tx: &Transaction<'_>, message: &Message) -> Result<i64> {
        self.inbox.clear();
        ensure!(
            message.payload.addresses(&self.binding.worker),
            "sandbox delivery target mismatch"
        );
        ensure!(
            !(message.payload.broadcast && message.id.origin == self.binding.worker),
            "sandbox broadcast cannot target its own sender"
        );
        self.check_local_worker(tx)?;
        self.retain_conversation(tx, message)?;
        let publication =
            self.inbox
                .insert_single(tx, &self.binding.team, &self.worker.name, true, message)?;
        let event = publication.event_id;
        self.inbox.arm(publication);
        Ok(event)
    }

    fn committed(&mut self, event_id: i64) {
        self.inbox.publish(event_id);
    }

    fn replace_directory(
        &mut self,
        tx: &Transaction<'_>,
        peers: Option<&[TeamPeer]>,
    ) -> Result<()> {
        self.check_local_worker(tx)?;
        let changed = tx.execute(
            "INSERT INTO sandbox_guest_directory VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT(singleton) DO UPDATE SET peers = excluded.peers
             WHERE binding = excluded.binding AND local_name = excluded.local_name
               AND local_created_at = excluded.local_created_at",
            params![
                serde_json::to_string(self.binding)?,
                self.worker.name,
                self.worker.created_at,
                serde_json::to_string(peers.unwrap_or_default())?,
            ],
        )?;
        ensure!(
            changed == 1,
            "sandbox guest directory does not match its existing enrollment"
        );
        Ok(())
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
    local_targets: Vec<String>,
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

    fn clear(&mut self) {
        self.pending = None;
    }

    fn arm(&mut self, publication: Publication) {
        self.pending = Some(publication);
    }

    fn insert_single(
        &mut self,
        tx: &Transaction<'_>,
        team: &str,
        target: &str,
        wake_target: bool,
        message: &Message,
    ) -> Result<Publication> {
        let local_targets = if wake_target {
            vec![target.to_owned()]
        } else {
            Vec::new()
        };
        let queued_to = if wake_target {
            Vec::new()
        } else {
            vec![target.to_owned()]
        };
        let mut sandbox = json!({"team": team, "id": message.id});
        if message.payload.broadcast {
            sandbox["broadcast"] = json!(true);
        }
        if let Some(conversation) = &message.payload.conversation {
            sandbox["conversation"] = json!(conversation);
        }
        if wake_target && !self.private_guest {
            let created_at: f64 = tx.query_row(
                "SELECT created_at FROM instances WHERE name = ?",
                [target],
                |row| row.get(0),
            )?;
            sandbox["recipient_created_at"] = json!(created_at);
        }
        self.insert_event(tx, team, local_targets, queued_to, sandbox, message)
    }

    fn insert_broadcast(
        &mut self,
        tx: &Transaction<'_>,
        team: &str,
        host_recipients: &[(String, f64)],
        guest_names: &[String],
        message: &Message,
    ) -> Result<Publication> {
        let host_names: Vec<String> = host_recipients
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let mut sandbox = json!({
            "team": team,
            "id": message.id,
            "broadcast": true,
        });
        if !host_recipients.is_empty() {
            let created_ats: serde_json::Map<String, Value> = host_recipients
                .iter()
                .map(|(name, created_at)| (name.clone(), json!(created_at)))
                .collect();
            sandbox["recipient_created_ats"] = Value::Object(created_ats);
        }
        self.insert_event(tx, team, host_names, guest_names.to_vec(), sandbox, message)
    }

    fn insert_event(
        &mut self,
        tx: &Transaction<'_>,
        team: &str,
        local_targets: Vec<String>,
        queued_to: Vec<String>,
        mut sandbox: Value,
        message: &Message,
    ) -> Result<Publication> {
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
        if let Some(generation) = &payload.sender_routing_generation {
            sandbox["sender_routing_generation"] = json!(generation);
        }
        let mut data = json!({
            "from": sender,
            "sender_kind": "instance",
            "scope": "mentions",
            "text": payload.text,
            "mentions": local_targets,
            "delivered_to": local_targets,
            "intent": payload.intent,
            "_sandbox": sandbox,
        });
        if !queued_to.is_empty() {
            data["queued_to"] = json!(queued_to);
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
        let event_id = self.db.insert_event_row("message", sender, &data, None)?;
        Ok(Publication {
            event_id,
            sender: sender.clone(),
            local_targets,
            data,
        })
    }

    fn publish(&mut self, event_id: i64) {
        let Some(Publication {
            sender,
            local_targets,
            data,
            ..
        }) = self
            .pending
            .take()
            .filter(|event| event.event_id == event_id)
        else {
            return;
        };
        if local_targets.is_empty() {
            // This host event journals forwarding without a local inbox delivery.
            return;
        }
        if self.private_guest
            && let Some(thread) = data.get("thread").and_then(Value::as_str)
        {
            self.db
                .add_thread_memberships(thread, Some(&sender), &local_targets);
        }
        self.db
            .after_event_logged(event_id, "message", &sender, &data);
        match local_targets.as_slice() {
            [target] => {
                crate::notify::wake(self.db, target, &[]);
                crate::notify::wake_observers(self.db);
            }
            targets => crate::notify::wake_instances_and_observers(self.db, targets),
        }
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
