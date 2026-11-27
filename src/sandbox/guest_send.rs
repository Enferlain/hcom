use std::fmt::Write as _;

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};

use crate::db::HcomDb;
use crate::messages::MessageEnvelope;
use crate::shared::time::now_epoch_i64;
use crate::shared::{SenderIdentity, SenderKind};

use super::outgoing::{Queued, wire_intent};
use super::protocol::{
    Binding, Intent, MessageId, MessagePayload, Outcome, Receiver, TeamPeer,
    is_prohibited_message_control, validate_message_text,
};
use super::store::Store;

struct Enrollment {
    binding: Binding,
    local_name: String,
    created_at: f64,
    peers: Vec<TeamPeer>,
}

pub(crate) enum Target {
    Direct {
        binding: Binding,
        requested: String,
        peer: TeamPeer,
    },
    Broadcast {
        binding: Binding,
    },
    /// A guest thread expansion request: the retained grant names the
    /// conversation; the host performs the fan-out.
    Expand {
        binding: Binding,
        grant: super::conversation::GuestGrant,
    },
}

impl Target {
    pub(crate) fn is_broadcast(&self) -> bool {
        matches!(self, Self::Broadcast { .. })
    }

    fn binding(&self) -> &Binding {
        match self {
            Self::Direct { binding, .. }
            | Self::Broadcast { binding }
            | Self::Expand { binding, .. } => binding,
        }
    }
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
    broadcast: bool,
    thread: Option<&str>,
    has_reply_to: bool,
) -> Result<Option<Target>> {
    let Some(enrollment) = Enrollment::read(db.conn())? else {
        return Ok(None);
    };
    enrollment.check_sender(db.conn(), sender)?;
    if broadcast {
        ensure!(
            thread.is_none() && targets.is_none_or(|t| t.is_empty()),
            "--broadcast cannot be combined with agent targets or --thread"
        );
        ensure!(
            !has_reply_to,
            "--broadcast cannot be combined with --reply-to in a sandbox"
        );
        return Ok(Some(Target::Broadcast {
            binding: enrollment.binding,
        }));
    }
    if thread.is_none() && targets.is_none_or(|t| t.is_empty()) {
        anyhow::bail!(
            "Refusing implicit broadcast: no explicit recipient was resolved.\nTarget an agent with @name, reuse a seeded --thread, or add --broadcast."
        );
    }
    // Thread-only dispatch resolves only a unique retained grant for the
    // exact enrolled worker and binding; an unknown or foreign label errors
    // here instead of falling through to any ordinary membership. Replies
    // remain explicitly directed: they still name one target below.
    if let Some(label) = thread
        && targets.is_none_or(|t| t.is_empty())
    {
        if has_reply_to {
            anyhow::bail!("Target one sandbox team member explicitly");
        }
        let grant =
            super::conversation::guest_grant_for_label(db.conn(), &enrollment.binding, label)?
                .with_context(|| format!("No sandbox thread grant for '{label}'"))?;
        return Ok(Some(Target::Expand {
            binding: enrollment.binding,
            grant,
        }));
    }
    let Some([requested]) = targets else {
        anyhow::bail!("Target one sandbox team member explicitly");
    };
    Ok(Some(Target::Direct {
        peer: enrollment.resolve(requested)?.clone(),
        binding: enrollment.binding,
        requested: requested.clone(),
    }))
}

/// Called only if normal CLI content validation failed. The queue still checks
/// sender enrollment and validates the escaped body before accepting it.
pub(crate) fn can_escape_controls(db: &HcomDb, text: &str) -> Result<bool> {
    // Preserve the raw body's required-text check. The escaped body and the
    // complete encoded frame have their own size checks before commit.
    Ok(!text.trim().is_empty()
        && text.len() <= crate::shared::MAX_MESSAGE_SIZE
        && text.chars().any(is_prohibited_message_control)
        && Enrollment::read(db.conn())?.is_some())
}

fn escape_controls(text: &str) -> (String, bool) {
    let Some((first, _)) = text
        .char_indices()
        .find(|&(_, c)| is_prohibited_message_control(c))
    else {
        return (text.to_owned(), false);
    };
    let mut escaped = String::with_capacity(text.len());
    escaped.push_str(&text[..first]);
    for c in text[first..].chars() {
        if is_prohibited_message_control(c) {
            write!(escaped, "\\u{:04x}", u32::from(c)).expect("write to String");
        } else {
            escaped.push(c);
        }
    }
    (escaped, true)
}

pub(crate) fn queue(
    db: &HcomDb,
    sender: &SenderIdentity,
    target: &Target,
    text: &str,
    envelope: &MessageEnvelope,
    reply_event: Option<i64>,
    result: Option<Outcome>,
) -> Result<Queued> {
    ensure!(!text.trim().is_empty(), "Message required");
    let (text, escaped_controls) = escape_controls(text);
    validate_message_text(&text)?;
    let intent = wire_intent(envelope);
    ensure!(
        result.is_none() || matches!(target, Target::Direct { .. }),
        "sandbox final reports require a directed request reply"
    );
    ensure!(
        envelope.bundle_id.is_none(),
        "sandbox sends do not support bundles"
    );
    ensure!(
        envelope.reply_to.is_none() || reply_event.is_some(),
        "sandbox reply event was not found"
    );
    let binding = target.binding();
    if target.is_broadcast() {
        ensure!(
            envelope.thread.is_none(),
            "--broadcast cannot be combined with agent targets or --thread"
        );
        ensure!(
            envelope.reply_to.is_none() && reply_event.is_none(),
            "--broadcast cannot be combined with --reply-to in a sandbox"
        );
    }
    if matches!(target, Target::Expand { .. }) {
        // Thread-only dispatch expands; it never carries a reply.
        ensure!(
            envelope.reply_to.is_none() && reply_event.is_none(),
            "thread expansion requests cannot combine --reply-to"
        );
    }

    let store = Store::initialize(db)?;
    let parent = |conn: &Connection| -> Result<Option<MessageId>> {
        let Some(event_id) = reply_event else {
            return Ok(None);
        };
        let Target::Direct { binding, peer, .. } = target else {
            anyhow::bail!("--broadcast cannot be combined with --reply-to in a sandbox");
        };
        let raw: String = conn.query_row(
            "SELECT data FROM events WHERE id = ?1 AND type = 'message'
             AND json_extract(data, '$._sandbox.team') = ?2 AND json_extract(data, '$.thread') IS ?3",
            rusqlite::params![event_id, binding.team, envelope.thread],
            |row| row.get(0),
        ).optional()?.context("sandbox reply is not in this conversation")?;
        let data: Value = serde_json::from_str(&raw)?;
        let id: MessageId = serde_json::from_value(data["_sandbox"]["id"].clone())?;
        let contains = |field: &str, name: &str| {
            data[field]
                .as_array()
                .is_some_and(|items| items.iter().any(|value| value.as_str() == Some(name)))
        };
        let valid_parent = if id.origin == binding.worker {
            contains("queued_to", &peer.name)
                && data["_sandbox"]["target_generation"].as_str() == Some(&peer.generation)
        } else {
            let generation = if peer.alias.is_some() {
                Some(id.origin.generation.as_str())
            } else {
                data["_sandbox"]["sender_routing_generation"].as_str()
            };
            id.origin.worker == peer.name
                && contains("delivered_to", &sender.name)
                && generation == Some(&peer.generation)
        };
        ensure!(valid_parent, "sandbox reply is not in this conversation");
        ensure!(
            result.is_none() || data["intent"].as_str() == Some("request"),
            "sandbox final report must reply to a request"
        );
        ensure!(
            intent != Intent::Ack || !matches!(data["intent"].as_str(), Some("ack" | "inform")),
            "sandbox acknowledgment cannot reply to an acknowledgment or inform"
        );
        Ok(Some(id))
    };
    let payload = match target {
        Target::Direct { peer, .. } => MessagePayload {
            assigned_result: false,
            result,
            conversation: None,
            target: peer.name.clone(),
            target_generation: Some(peer.generation.clone()),
            sender_routing_generation: None,
            broadcast: false,
            thread: envelope.thread.clone(),
            reply_to: parent(db.conn())?,
            intent,
            text,
        },
        Target::Broadcast { .. } => MessagePayload {
            assigned_result: false,
            result: None,
            conversation: None,
            target: String::new(),
            target_generation: None,
            sender_routing_generation: None,
            broadcast: true,
            thread: None,
            reply_to: None,
            intent,
            text,
        },
        Target::Expand { grant, .. } => {
            // The strictly distinct expansion shape: scoped to the retained
            // conversation, naming the label, addressing nobody.
            ensure!(
                envelope.thread.as_deref() == Some(grant.label.as_str()),
                "sandbox thread grant does not match the requested label"
            );
            MessagePayload {
                assigned_result: false,
                result: None,
                conversation: Some(grant.conversation.clone()),
                target: String::new(),
                target_generation: None,
                sender_routing_generation: None,
                broadcast: false,
                thread: Some(grant.label.clone()),
                reply_to: None,
                intent,
                text,
            }
        }
    };
    let now = now_epoch_i64();
    // The private guest store has no host controller to reclaim expired slots.
    store.expire_guest_pending_if_due(now)?;
    let mut event_id = 0;
    store.enqueue(
        Receiver::Host,
        binding,
        binding.worker.clone(),
        payload,
        now,
        |tx, message| {
            let current = Enrollment::read(tx)?.context("sandbox guest enrollment is missing")?;
            current.check_sender(tx, sender)?;
            ensure!(
                current.binding == *binding,
                "sandbox directory binding changed during enqueue"
            );
            ensure!(
                parent(tx)? == message.payload.reply_to,
                "sandbox reply changed during enqueue"
            );
            let mut data = match target {
                Target::Direct {
                    requested, peer, ..
                } => {
                    ensure!(
                        current.resolve(requested)? == peer,
                        "sandbox directory target changed during enqueue"
                    );
                    json!({
                        "from": sender.name,
                        "sender_kind": "instance",
                        "scope": "mentions",
                        "text": message.payload.text,
                        "mentions": [],
                        "delivered_to": [],
                        "queued_to": [peer.name],
                        "intent": intent,
                        "_sandbox": {
                            "team": binding.team,
                            "id": message.id,
                            "target_generation": peer.generation,
                        },
                    })
                }
                Target::Broadcast { .. } => json!({
                    "from": sender.name,
                    "sender_kind": "instance",
                    "scope": "mentions",
                    "text": message.payload.text,
                    "mentions": [],
                    "delivered_to": [],
                    "queued_to": [],
                    "intent": intent,
                    "_sandbox": {
                        "team": binding.team,
                        "id": message.id,
                        "broadcast": true,
                    },
                }),
                Target::Expand { grant, .. } => {
                    // The request is immutable: the exact retained grant must
                    // still match this route, generation, and label.
                    let retained =
                        super::conversation::guest_grant_for_label(tx, binding, &grant.label)?
                            .context("sandbox thread grant is missing")?;
                    ensure!(
                        retained.conversation == grant.conversation,
                        "sandbox thread grant changed during enqueue"
                    );
                    json!({
                        "from": sender.name,
                        "sender_kind": "instance",
                        "scope": "mentions",
                        "text": message.payload.text,
                        "mentions": [],
                        "delivered_to": [],
                        "queued_to": [],
                        "intent": intent,
                        "_sandbox": {
                            "team": binding.team,
                            "id": message.id,
                            "conversation": grant.conversation,
                        },
                    })
                }
            };
            if let Some(thread) = &envelope.thread {
                data["thread"] = json!(thread);
            }
            if let Some(reply) = reply_event {
                data["reply_to"] = json!(reply.to_string());
                data["reply_to_local"] = json!(reply);
            }
            if let Some(outcome) = message.payload.result {
                data["_sandbox"]["result"] = json!(outcome);
            }
            event_id = db.insert_event_row("message", &sender.name, &data, None)?;
            Ok(event_id)
        },
    )?;
    crate::notify::wake_observers(db);
    let (queued_to, display_target) = match target {
        Target::Direct { peer, .. } => (vec![peer.name.clone()], peer.name.clone()),
        Target::Broadcast { .. } => (Vec::new(), "team broadcast".to_string()),
        Target::Expand { grant, .. } => (Vec::new(), format!("thread '{}'", grant.label)),
    };
    Ok(Queued {
        event_id,
        queued_to,
        display_target,
        broadcast: target.is_broadcast(),
        escaped_controls,
        forwarding: "pending",
        forwarding_reason: None,
    })
}
