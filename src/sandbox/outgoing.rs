use anyhow::{Context, Result, ensure};
use rusqlite::OptionalExtension;
use serde_json::json;

use crate::db::HcomDb;
use crate::messages::{MessageEnvelope, MessageIntent};
use crate::shared::SenderIdentity;

use super::controller::{Forwarding, enqueue_host};
use super::directory::{HostIdentity, Worker, check_sender, host_recipient_generation};
use super::protocol::{Intent, MessageId, MessagePayload};
use super::store::Store;

pub(crate) struct Queued {
    pub event_id: i64,
    pub target: String,
    pub forwarding: &'static str,
    pub forwarding_reason: Option<String>,
}

pub(super) fn wire_text(text: &str) -> Result<String> {
    let text = text.replace("\r\n", "\n");
    crate::messages::validate_message(&text).map_err(anyhow::Error::msg)?;
    ensure!(
        !text
            .chars()
            .any(|c| c.is_control() && c != '\n' && c != '\t'),
        "sandbox message contains prohibited control characters"
    );
    Ok(text)
}

pub(super) fn wire_intent(envelope: &MessageEnvelope) -> Intent {
    match envelope.intent.unwrap_or(MessageIntent::Inform) {
        MessageIntent::Request => Intent::Request,
        MessageIntent::Inform => Intent::Inform,
        MessageIntent::Ack => Intent::Ack,
    }
}

pub(crate) fn queue(
    db: &HcomDb,
    sender: &SenderIdentity,
    worker: &Worker,
    text: &str,
    envelope: &MessageEnvelope,
    reply_event: Option<i64>,
) -> Result<Queued> {
    let text = wire_text(text)?;
    ensure!(
        envelope.bundle_id.is_none(),
        "sandbox sends do not support host bundles"
    );
    ensure!(
        envelope.reply_to.is_none() || reply_event.is_some(),
        "sandbox reply event was not found"
    );

    let host = HostIdentity::capture(db, sender)?;
    let binding = &worker.binding;
    let target = binding.worker.worker.as_str();
    check_sender(db.conn(), &binding.team, &host)?;
    let store = Store::initialize(db)?;
    let origin = host.origin(store.incarnation());
    let route = serde_json::to_string(binding)?;
    let intent = wire_intent(envelope);
    let resolve_reply = || -> Result<Option<MessageId>> {
        let Some(event) = reply_event else {
            return Ok(None);
        };
        let row: Option<(String, Option<String>)> = db
            .conn()
            .query_row(
                "SELECT json_extract(data, '$._sandbox.id'), json_extract(data, '$.intent')
               FROM events
              WHERE id = ?1 AND type = 'message'
                AND json_extract(data, '$._sandbox.team') = ?2
                AND json_extract(data, '$.thread') IS ?3
                AND (
                    (instance = ?4 AND json_extract(data, '$._sandbox.id.origin.generation') = ?6
                     AND EXISTS (SELECT 1 FROM sandbox_outbox WHERE event_id = ?1 AND route = ?7))
                    OR
                    (instance = ?5 AND json_extract(data, '$._sandbox.id.origin.generation') = ?8
                     AND json_extract(data, '$._sandbox.id.origin.incarnation') = ?9
                     AND json_extract(data, '$._sandbox.recipient_created_at') = ?10
                     AND EXISTS (
                        SELECT 1 FROM json_each(data, '$.delivered_to') WHERE value = ?4))
                )",
                rusqlite::params![
                    event,
                    binding.team,
                    envelope.thread,
                    sender.name,
                    target,
                    origin.generation,
                    route,
                    binding.worker.generation,
                    binding.worker.incarnation,
                    host.created_at(),
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (id, parent_intent) = row.context("sandbox reply is not in this conversation")?;
        ensure!(
            intent != Intent::Ack || !matches!(parent_intent.as_deref(), Some("ack" | "inform")),
            "sandbox acknowledgment cannot reply to an acknowledgment or inform"
        );
        Ok(Some(serde_json::from_str(&id)?))
    };

    let payload = MessagePayload {
        target: target.into(),
        target_generation: Some(binding.worker.generation.clone()),
        sender_routing_generation: Some(host_recipient_generation(
            db.conn(),
            &sender.name,
            host.created_at(),
        )?),
        thread: envelope.thread.clone(),
        reply_to: resolve_reply()?,
        intent,
        text,
    };

    let mut event_id = 0;
    let queued = enqueue_host(&store, binding, origin.clone(), payload, |tx, message| {
        // The store transaction fences the destination. Recheck sender membership
        // and reply correlation on that connection before publishing the event.
        check_sender(tx, &binding.team, &host)?;
        ensure!(
            resolve_reply()? == message.payload.reply_to,
            "sandbox reply changed during enqueue"
        );
        let mut data = json!({
            "from": sender.name,
            "sender_kind": "instance",
            "scope": "mentions",
            "text": message.payload.text,
            "mentions": [],
            "delivered_to": [],
            "queued_to": [target],
            "intent": intent,
            "_sandbox": {"team": binding.team, "id": message.id},
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
    })?;

    // Wake readers of this committed journal entry without creating local
    // recipient subscriptions or request watches.
    crate::notify::wake_observers(db);
    let (forwarding, forwarding_reason) = match queued.forwarding {
        Forwarding::ControllerReady(_) => ("pending", None),
        Forwarding::Disabled => ("disabled", None),
        Forwarding::Deferred(reason) => ("deferred", Some(reason)),
    };
    Ok(Queued {
        event_id,
        target: target.to_owned(),
        forwarding,
        forwarding_reason,
    })
}
