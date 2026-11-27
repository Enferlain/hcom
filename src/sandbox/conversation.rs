//! Host-created conversations have one host participant and explicitly seeded guests.
//! Task 6 lifecycle cleanup owns removal of retained member and conversation records.

use std::cell::RefCell;

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::db::HcomDb;
use crate::messages::MessageEnvelope;
use crate::shared::time::now_epoch_i64;
use crate::shared::{SenderIdentity, SenderKind};

use super::controller::{Forwarding, forwarding_after_commit};
use super::directory::{
    HostIdentity, Worker, check_sender, host_generation, host_recipient_generation,
};
use super::outgoing::{Queued, wire_intent, wire_text};
use super::protocol::{Binding, MessagePayload, validate_identifier};
use super::store::{BatchHop, Store};

pub(super) const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS sandbox_conversations (
        id TEXT PRIMARY KEY,
        team TEXT NOT NULL REFERENCES sandbox_teams(id),
        label TEXT NOT NULL,
        UNIQUE (team, label)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_conversation_members (
        conversation TEXT NOT NULL REFERENCES sandbox_conversations(id),
        name TEXT NOT NULL,
        generation TEXT NOT NULL,
        host_created_at REAL,
        PRIMARY KEY (conversation, name)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_guest_conversations (
        conversation TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        generation TEXT NOT NULL,
        binding TEXT NOT NULL
    ) STRICT;
    CREATE UNIQUE INDEX IF NOT EXISTS sandbox_guest_conversations_label
        ON sandbox_guest_conversations(label);
";

/// A sandbox conversation resolved for a host sender. The label scope is
/// advisory here; the queue path re-resolves it inside the write transaction.
#[derive(Debug)]
pub(crate) struct Resolved {
    pub(crate) team: String,
    pub(crate) label: String,
}

/// Resolve a thread label the sender may fan out to.
///
/// Grants are keyed by team, label, and the sender's exact participant
/// generation — never by the ordinary name-only thread index. A team member
/// without the participant grant resolves nothing and cannot distinguish a
/// hidden conversation from an absent one. Returns `None` for ordinary
/// labels so regular sends stay unchanged. Fails closed when the sender
/// holds this grant in several teams or the label collides with an ordinary
/// thread.
pub(crate) fn resolve(
    db: &HcomDb,
    sender: &SenderIdentity,
    label: &str,
) -> Result<Option<Resolved>> {
    let Some((name, created_at)) = host_member_key(sender) else {
        return Ok(None);
    };
    if validate_identifier(label).is_err() {
        return Ok(None);
    }
    let conn = db.conn();
    if !has_conversations(conn)? {
        return Ok(None);
    }
    match participant_conversations(conn, &name, created_at, label)?.as_slice() {
        [] => Ok(None),
        [team] => {
            ensure!(
                !ordinary_thread_exists(conn, label)?,
                "sandbox thread '{label}' collides with an ordinary thread; choose another label"
            );
            Ok(Some(Resolved {
                team: team.clone(),
                label: label.to_owned(),
            }))
        }
        _ => Err(anyhow::anyhow!(
            "sandbox thread '{label}' is enrolled in more than one of your teams; target a participant explicitly"
        )),
    }
}

/// Seed (or expand) the team conversation for an explicit authorized
/// host-to-guest send carrying a thread, inside the send transaction. The
/// caller gates replies out: inheriting a parent label through --reply-to
/// never creates or expands a grant.
///
/// A creating sender becomes a participant together with that guest; a
/// current participant may add same-team guests. A sender without the
/// participant grant proceeds without joining (`None`) so inherited-thread
/// replies keep working and a guessed label cannot adopt membership. A label
/// that already lives in the ordinary thread index, or that the sender holds
/// in another team, gains no conflicting grant: the explicit routing
/// proceeds, only the grant is skipped. Prior history grants nothing in
/// every case.
pub(super) fn seed_directed(
    tx: &Transaction<'_>,
    team: &str,
    label: &str,
    host: &HostIdentity,
    worker: &Worker,
) -> Result<Option<String>> {
    validate_identifier(label).map_err(anyhow::Error::msg)?;
    ensure!(
        worker.binding.team == team,
        "sandbox thread target is outside the sender's team"
    );
    let conversation = match conversation_id(tx, team, label)? {
        Some(conversation) => {
            if !is_current_participant(tx, &conversation, host)? {
                // Existing conversation the sender does not belong to: the
                // send continues as a directed reply, membership is untouched.
                return Ok(None);
            }
            conversation
        }
        None => {
            // Explicit routing never creates a grant that would collide with
            // the ordinary index or shadow the sender's other-team grant.
            if ordinary_thread_exists(tx, label)?
                || !participant_conversations(tx, host.name(), host.created_at(), label)?.is_empty()
            {
                return Ok(None);
            }
            let conversation = Uuid::new_v4().simple().to_string();
            tx.execute(
                "INSERT INTO sandbox_conversations (id, team, label) VALUES (?, ?, ?)",
                params![conversation, team, label],
            )?;
            add_host_member(tx, &conversation, host)?;
            conversation
        }
    };
    add_guest_member(tx, &conversation, worker)?;
    Ok(Some(conversation))
}

/// Queue a thread-only host send to every current guest participant of the
/// conversation. Each guest receives a directed hop sharing one stable wire
/// identity; the journal event, membership rechecks, and every outbox row
/// commit or roll back together.
pub(crate) fn queue(
    db: &HcomDb,
    sender: &SenderIdentity,
    resolved: &Resolved,
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
        envelope.reply_to.is_none() && reply_event.is_none(),
        "a thread-only sandbox send cannot combine --reply-to; reply to one participant directly"
    );
    let team = resolved.team.as_str();
    let label = resolved.label.as_str();
    validate_identifier(label).map_err(anyhow::Error::msg)?;

    let host = HostIdentity::capture(db, sender)?;
    let store = Store::initialize(db)?;
    let origin = host.origin(store.incarnation());
    let intent = wire_intent(envelope);

    let guest_targets = RefCell::new(Vec::new());
    let mut event_id = 0;

    let _message = store.enqueue_host_batch(
        now_epoch_i64(),
        |tx, _sequence| {
            // Authoritative inside this transaction: sender authorization,
            // label scope, and every participant's current generation.
            check_sender(tx, team, &host)?;
            let (conversation, hops) = resolve_fanout(tx, team, label, &host)?;
            let sender_generation = host_recipient_generation(tx, host.name(), host.created_at())?;
            let hops: Vec<BatchHop> = hops
                .into_iter()
                .map(|route| {
                    let target = route.worker.worker.clone();
                    guest_targets.borrow_mut().push(target.clone());
                    BatchHop {
                        payload: MessagePayload {
                            target,
                            target_generation: Some(route.worker.generation.clone()),
                            sender_routing_generation: Some(sender_generation.clone()),
                            broadcast: false,
                            thread: Some(label.to_owned()),
                            reply_to: None,
                            // The grant is resolved in this transaction, so
                            // every hop digests with its opaque conversation
                            // id attached.
                            conversation: Some(conversation.clone()),
                            intent,
                            text: text.clone(),
                        },
                        route,
                    }
                })
                .collect();
            Ok((conversation, origin.clone(), hops))
        },
        |_tx, conversation, messages| {
            // Every hop shares one stable wire identity for replies.
            let message = messages
                .first()
                .context("sandbox thread fan-out is empty")?;
            let data = json!({
                "from": host.name(),
                "sender_kind": "instance",
                "scope": "mentions",
                "text": message.payload.text,
                "mentions": [],
                "delivered_to": [],
                "queued_to": *guest_targets.borrow(),
                "intent": intent,
                "_sandbox": {
                    "team": team,
                    "id": message.id,
                    "conversation": conversation,
                },
                "thread": label,
            });
            event_id = db.insert_event_row("message", host.name(), &data, None)?;
            Ok(event_id)
        },
    )?;

    // Wake journal readers of this committed entry without creating local
    // recipient subscriptions or request watches, like directed guest sends.
    crate::notify::wake_observers(db);

    let (forwarding, forwarding_reason) = match forwarding_after_commit(&store) {
        Forwarding::ControllerReady(_) => ("pending", None),
        Forwarding::Disabled => ("disabled", None),
        Forwarding::Deferred(reason) => ("deferred", Some(reason)),
    };
    Ok(Queued {
        event_id,
        display_target: format!("thread '{label}'"),
        queued_to: guest_targets.into_inner(),
        broadcast: false,
        forwarding,
        forwarding_reason,
    })
}

/// Resolve the conversation and its current guest participants inside the
/// write transaction. A guest member whose canonical name moved to a new
/// generation is no longer a participant; a reused name never inherits old
/// grants.
fn resolve_fanout(
    tx: &Transaction<'_>,
    team: &str,
    label: &str,
    host: &HostIdentity,
) -> Result<(String, Vec<Binding>)> {
    ensure!(
        !ordinary_thread_exists(tx, label)?,
        "sandbox thread '{label}' collides with an ordinary thread; choose another label"
    );
    let granted = participant_conversations(tx, host.name(), host.created_at(), label)?;
    ensure!(
        !granted.is_empty(),
        "sandbox thread '{label}' is not available"
    );
    ensure!(
        granted.len() == 1 && granted[0] == team,
        "sandbox thread '{label}' is enrolled in more than one of your teams; target a participant explicitly"
    );
    let conversation =
        conversation_id(tx, team, label)?.context("sandbox conversation record is missing")?;
    check_sender(tx, team, host)?;
    ensure!(
        is_current_participant(tx, &conversation, host)?,
        "only a current sandbox thread participant can send to the thread"
    );

    let mut stmt = tx.prepare(
        "SELECT m.name, m.generation, w.binding
         FROM sandbox_conversation_members m
         JOIN sandbox_workers w ON w.name = m.name AND w.generation = m.generation
         WHERE m.conversation = ? AND m.host_created_at IS NULL AND m.name != ?
           AND w.team = ? AND w.retired = 0
         ORDER BY m.name",
    )?;
    let hops = stmt
        .query_map(params![conversation, host.name(), team], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|(name, generation, raw)| -> Result<Binding> {
            let binding: Binding = serde_json::from_str(&raw)?;
            binding.validate().map_err(anyhow::Error::msg)?;
            ensure!(
                binding.team == team
                    && binding.worker.worker == name
                    && binding.worker.generation == generation,
                "sandbox conversation member does not match its enrollment"
            );
            Ok(binding)
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        !hops.is_empty(),
        "sandbox thread '{label}' has no other current participants"
    );
    Ok((conversation, hops))
}

fn has_conversations(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema
         WHERE type = 'table' AND name = 'sandbox_conversations')",
        [],
        |row| row.get(0),
    )?)
}

/// The exact host generation of a sender, if it can hold conversation grants.
fn host_member_key(sender: &SenderIdentity) -> Option<(String, f64)> {
    let created_at = sender
        .instance_data
        .as_ref()
        .and_then(|data| data.get("created_at"))
        .and_then(Value::as_f64)?;
    (sender.kind == SenderKind::Instance).then(|| (sender.name.clone(), created_at))
}

fn conversation_id(conn: &Connection, team: &str, label: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT id FROM sandbox_conversations WHERE team = ? AND label = ?",
            params![team, label],
            |row| row.get(0),
        )
        .optional()?)
}

/// Teams whose conversation carries the sender's exact current participant
/// grant for this label. Requires the granted host generation to still be the
/// live local instance, so a replaced name resolves nothing.
fn participant_conversations(
    conn: &Connection,
    name: &str,
    created_at: f64,
    label: &str,
) -> Result<Vec<String>> {
    let mut stmt = conn.prepare_cached(
        "SELECT c.team FROM sandbox_conversations c
         JOIN sandbox_conversation_members m ON m.conversation = c.id
         JOIN instances i ON i.name = m.name AND i.created_at = m.host_created_at
         JOIN sandbox_team_hosts h ON h.team = c.team AND h.name = m.name
                                  AND h.created_at = m.host_created_at
         WHERE c.label = ?1 AND m.name = ?2 AND m.host_created_at = ?3
           AND m.host_created_at IS NOT NULL AND m.generation = ?4
           AND COALESCE(i.origin_device_id, '') = ''
         ORDER BY c.team LIMIT 2",
    )?;
    let teams = stmt
        .query_map(
            params![label, name, created_at, host_generation(created_at)],
            |row| row.get(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(teams)
}

/// The ordinary thread index is name-only and team-blind. A shared label
/// would let one send resolve into both namespaces, so the sandbox side
/// refuses it outright.
fn ordinary_thread_exists(conn: &Connection, label: &str) -> Result<bool> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM kv
         JOIN instances i ON i.name = json_extract(kv.value, '$.caller')
         WHERE kv.key LIKE 'events_sub:%'
           AND json_extract(value, '$.auto_thread_member') = 1
           AND json_extract(value, '$.thread_name') = ?)",
        params![label],
        |row| row.get(0),
    )?;
    Ok(exists)
}

fn is_current_participant(
    conn: &Connection,
    conversation: &str,
    host: &HostIdentity,
) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sandbox_conversation_members
             WHERE conversation = ? AND name = ? AND generation = ?
               AND host_created_at IS NOT NULL",
            params![
                conversation,
                host.name(),
                host_generation(host.created_at())
            ],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn add_host_member(conn: &Connection, conversation: &str, host: &HostIdentity) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO sandbox_conversation_members
         (conversation, name, generation, host_created_at) VALUES (?, ?, ?, ?)",
        params![
            conversation,
            host.name(),
            host_generation(host.created_at()),
            host.created_at()
        ],
    )?;
    Ok(())
}

/// Record the targeted guest. The explicit send always re-seeds the current
/// generation of that canonical name; a reused name does not inherit the
/// retired generation's participation.
fn add_guest_member(conn: &Connection, conversation: &str, worker: &Worker) -> Result<()> {
    let current: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sandbox_workers
         WHERE name = ? AND generation = ? AND retired = 0)",
        params![
            worker.binding.worker.worker,
            worker.binding.worker.generation
        ],
        |row| row.get(0),
    )?;
    ensure!(
        current,
        "sandbox thread target is no longer the current worker generation"
    );
    conn.execute(
        "INSERT INTO sandbox_conversation_members
         (conversation, name, generation, host_created_at) VALUES (?, ?, ?, NULL)
         ON CONFLICT(conversation, name) DO UPDATE
         SET generation = excluded.generation, host_created_at = NULL",
        params![
            conversation,
            worker.binding.worker.worker,
            worker.binding.worker.generation
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
