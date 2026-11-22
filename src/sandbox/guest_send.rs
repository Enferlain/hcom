use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};

use crate::db::HcomDb;
use crate::messages::MessageEnvelope;
use crate::shared::time::now_epoch_i64;
use crate::shared::{SenderIdentity, SenderKind};

use super::outgoing::{Queued, wire_intent, wire_text};
use super::protocol::{Binding, Intent, MessageId, MessagePayload, Receiver, TeamPeer};
use super::store::Store;

struct Enrollment {
    binding: Binding,
    local_name: String,
    created_at: f64,
    peers: Vec<TeamPeer>,
}

pub(crate) struct Target {
    binding: Binding,
    requested: String,
    peer: TeamPeer,
}

impl Enrollment {
    fn read(conn: &Connection) -> Result<Option<Self>> {
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='sandbox_guest_directory')",
            [],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(None);
        }
        // Other sandbox stores have an empty guest enrollment table.
        let raw = conn
            .query_row(
                "SELECT binding, local_name, local_created_at, peers
                 FROM sandbox_guest_directory WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, f64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        raw.map(|(binding, local_name, created_at, peers)| {
            Ok(Self {
                binding: serde_json::from_str(&binding)?,
                local_name,
                created_at,
                peers: serde_json::from_str(&peers)?,
            })
        })
        .transpose()
    }

    fn check_sender(&self, conn: &Connection, sender: &SenderIdentity) -> Result<()> {
        let created_at = sender
            .instance_data
            .as_ref()
            .and_then(|data| data.get("created_at"))
            .and_then(Value::as_f64);
        ensure!(
            sender.kind == SenderKind::Instance
                && sender.name == self.local_name
                && created_at == Some(self.created_at),
            "sandbox sends require the enrolled guest instance"
        );
        let current: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM instances WHERE name = ?1 AND created_at = ?2
              AND COALESCE(origin_device_id, '') = '')",
            rusqlite::params![self.local_name, self.created_at],
            |row| row.get(0),
        )?;
        ensure!(current, "sandbox guest sender generation is stale");
        Ok(())
    }

    fn resolve(&self, requested: &str) -> Result<&TeamPeer> {
        super::protocol::validate_identifier(requested)?;
        let mut canonical = self
            .peers
            .iter()
            .filter(|peer| peer.name.eq_ignore_ascii_case(requested));
        if let Some(peer) = canonical.next() {
            ensure!(
                canonical.next().is_none(),
                "Ambiguous sandbox canonical name @{requested}"
            );
            return Ok(peer);
        }
        let mut matches = self.peers.iter().filter(|peer| {
            peer.alias
                .as_ref()
                .is_some_and(|alias| alias.eq_ignore_ascii_case(requested))
        });
        let peer = matches
            .next()
            .with_context(|| format!("No sandbox team peer for @{requested}"))?;
        ensure!(
            matches.next().is_none(),
            "Ambiguous sandbox target @{requested}; use a canonical target"
        );
        Ok(peer)
    }
}

pub(crate) fn resolve(
    db: &HcomDb,
    sender: &SenderIdentity,
    targets: Option<&[String]>,
) -> Result<Option<Target>> {
    let Some(enrollment) = Enrollment::read(db.conn())? else {
        return Ok(None);
    };
    enrollment.check_sender(db.conn(), sender)?;
    let Some([requested]) = targets else {
        anyhow::bail!("Target one sandbox team member explicitly");
    };
    Ok(Some(Target {
        peer: enrollment.resolve(requested)?.clone(),
        binding: enrollment.binding,
        requested: requested.clone(),
    }))
}

pub(crate) fn queue(
    db: &HcomDb,
    sender: &SenderIdentity,
    target: &Target,
    text: &str,
    envelope: &MessageEnvelope,
    reply_event: Option<i64>,
) -> Result<Queued> {
    let text = wire_text(text)?;
    let intent = wire_intent(envelope);
    ensure!(
        envelope.bundle_id.is_none(),
        "sandbox sends do not support bundles"
    );
    ensure!(
        envelope.reply_to.is_none() || reply_event.is_some(),
        "sandbox reply event was not found"
    );

    let store = Store::initialize(db)?;
    let parent = |conn: &Connection| -> Result<Option<MessageId>> {
        let Some(event_id) = reply_event else {
            return Ok(None);
        };
        let raw: String = conn.query_row(
            "SELECT data FROM events WHERE id = ?1 AND type = 'message'
             AND json_extract(data, '$._sandbox.team') = ?2 AND json_extract(data, '$.thread') IS ?3",
            rusqlite::params![event_id, target.binding.team, envelope.thread],
            |row| row.get(0),
        ).optional()?.context("sandbox reply is not in this conversation")?;
        let data: Value = serde_json::from_str(&raw)?;
        let id: MessageId = serde_json::from_value(data["_sandbox"]["id"].clone())?;
        let contains = |field: &str, name: &str| {
            data[field]
                .as_array()
                .is_some_and(|items| items.iter().any(|value| value.as_str() == Some(name)))
        };
        let current_peer = if id.origin == target.binding.worker {
            contains("queued_to", &target.peer.name)
                && data["_sandbox"]["target_generation"].as_str() == Some(&target.peer.generation)
        } else {
            let generation = if target.peer.alias.is_some() {
                Some(id.origin.generation.as_str())
            } else {
                data["_sandbox"]["sender_routing_generation"].as_str()
            };
            id.origin.worker == target.peer.name
                && contains("delivered_to", &sender.name)
                && generation == Some(&target.peer.generation)
        };
        ensure!(current_peer, "sandbox reply is not in this conversation");
        ensure!(
            intent != Intent::Ack || !matches!(data["intent"].as_str(), Some("ack" | "inform")),
            "sandbox acknowledgment cannot reply to an acknowledgment or inform"
        );
        Ok(Some(id))
    };
    let payload = MessagePayload {
        target: target.peer.name.clone(),
        target_generation: Some(target.peer.generation.clone()),
        sender_routing_generation: None,
        thread: envelope.thread.clone(),
        reply_to: parent(db.conn())?,
        intent,
        text,
    };
    let now = now_epoch_i64();
    // The private guest store has no host controller to reclaim expired slots.
    while !store.expire_pending(now)?.is_empty() {}
    let mut event_id = 0;
    store.enqueue(
        Receiver::Host,
        &target.binding,
        target.binding.worker.clone(),
        payload,
        now,
        |tx, message| {
            let current = Enrollment::read(tx)?.context("sandbox guest enrollment is missing")?;
            current.check_sender(tx, sender)?;
            ensure!(
                current.binding == target.binding
                    && current.resolve(&target.requested)? == &target.peer,
                "sandbox directory target changed during enqueue"
            );
            ensure!(
                parent(tx)? == message.payload.reply_to,
                "sandbox reply changed during enqueue"
            );
            let mut data = json!({
                "from": sender.name,
                "sender_kind": "instance",
                "scope": "mentions",
                "text": message.payload.text,
                "mentions": [],
                "delivered_to": [],
                "queued_to": [target.peer.name],
                "intent": intent,
                "_sandbox": {
                    "team": target.binding.team,
                    "id": message.id,
                    "target_generation": target.peer.generation,
                },
            });
            if let Some(thread) = &envelope.thread {
                data["thread"] = json!(thread);
            }
            if let Some(reply) = reply_event {
                data["reply_to"] = json!(reply.to_string());
                data["reply_to_local"] = json!(reply);
            }
            event_id = db.insert_event_row("message", &sender.name, &data, None)?;
            Ok(event_id)
        },
    )?;
    crate::notify::wake_observers(db);
    Ok(Queued {
        event_id,
        target: target.peer.name.clone(),
        forwarding: "pending",
        forwarding_reason: None,
    })
}
