//! Durable final-report reservations for requests assigned by a host.

use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, params};

use super::admission::{Limits, record_bytes};
use super::protocol::{Binding, Intent, MAX_FINAL_FRAME, Message};

pub(super) const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS sandbox_result_requests (
        request TEXT NOT NULL,
        route TEXT NOT NULL,
        requester TEXT NOT NULL,
        requester_generation TEXT NOT NULL,
        thread TEXT NOT NULL,
        request_event INTEGER NOT NULL,
        final_id TEXT,
        final_event INTEGER,
        outcome TEXT,
        PRIMARY KEY (request, route)
    ) STRICT;
";

// Count the complete encoded report, the local journal projection, and the
// request/result ledger metadata. The charge stays fixed through acceptance
// and retention, even when the report is smaller.
const RESERVED_BYTES: i64 = (2 * MAX_FINAL_FRAME + 4096) as i64;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(super) struct InvalidFinal(pub &'static str);

/// Reserve before the assignment is visible, on the host at enqueue and on
/// the worker when the authenticated host request reaches its private inbox.
pub(super) fn reserve(
    tx: &Transaction<'_>,
    limits: Limits,
    binding: &Binding,
    message: &Message,
    event_id: i64,
) -> Result<()> {
    let payload = &message.payload;
    if !payload.assigned_result {
        return Ok(());
    }
    let (Intent::Request, Some(thread), Some(requester_generation)) = (
        payload.intent,
        &payload.thread,
        &payload.sender_routing_generation,
    ) else {
        anyhow::bail!("sandbox result assignment is missing request metadata");
    };
    limits.charge_history(tx, binding, RESERVED_BYTES)?;
    tx.execute(
        "INSERT INTO sandbox_result_requests
          (request, route, requester, requester_generation, thread, request_event)
         VALUES (?, ?, ?, ?, ?, ?)",
        params![
            serde_json::to_string(&message.id)?,
            serde_json::to_string(binding)?,
            message.id.origin.worker,
            requester_generation,
            thread,
            event_id,
        ],
    )?;
    Ok(())
}

fn submitted_event(
    tx: &Transaction<'_>,
    binding: &Binding,
    message: &Message,
) -> Result<Option<i64>> {
    let payload = &message.payload;
    ensure!(
        message.id.origin == binding.worker,
        InvalidFinal("sandbox final report sender is not the assigned worker")
    );
    let existing = tx
        .query_row(
            "SELECT final_event FROM sandbox_result_requests
         WHERE request = ? AND route = ? AND requester = ? AND requester_generation = ?
           AND thread = ?",
            params![
                serde_json::to_string(&payload.reply_to)?,
                serde_json::to_string(binding)?,
                payload.target,
                payload.target_generation,
                payload.thread,
            ],
            |row| row.get(0),
        )
        .optional()?;
    existing.ok_or_else(|| {
        InvalidFinal("sandbox final report has no matching request reservation").into()
    })
}

pub(super) fn check(tx: &Transaction<'_>, binding: &Binding, message: &Message) -> Result<()> {
    ensure!(
        submitted_event(tx, binding, message)?.is_none(),
        InvalidFinal("sandbox request already has an accepted final report")
    );
    Ok(())
}

/// Retain a rejected attempt in ordinary history before reusing the slot.
pub(super) fn prepare_submission(
    tx: &Transaction<'_>,
    limits: Limits,
    binding: &Binding,
    message: &Message,
) -> Result<()> {
    let Some(event_id) = submitted_event(tx, binding, message)? else {
        return Ok(());
    };
    let route = serde_json::to_string(binding)?;
    let bytes: i64 = tx
        .query_row(
            "SELECT wire_bytes FROM sandbox_outbox
         WHERE route = ? AND event_id = ? AND final_report = 1 AND rejected = 1",
            params![route, event_id],
            |row| row.get(0),
        )
        .optional()?
        .context("sandbox request already has a pending or accepted final report")?;
    limits
        .record(tx, binding, bytes, event_id, None)
        .context("cannot retain the rejected final and queue a correction")?;
    tx.execute(
        "UPDATE sandbox_result_requests SET final_id = NULL, final_event = NULL, outcome = NULL
         WHERE request = ? AND route = ?",
        params![serde_json::to_string(&message.payload.reply_to)?, route],
    )?;
    Ok(())
}

/// Called after authorization in the same write transaction. Normal inbox
/// replay is resolved before reaching this operation.
pub(super) fn settle(
    tx: &Transaction<'_>,
    binding: &Binding,
    message: &Message,
    event_id: i64,
    wire_bytes: i64,
) -> Result<()> {
    let outcome = message.payload.result.expect("final report");
    ensure!(
        record_bytes(tx, wire_bytes, event_id)? <= RESERVED_BYTES,
        InvalidFinal("sandbox final report exceeds its reservation")
    );
    let changed = tx.execute(
        "UPDATE sandbox_result_requests SET final_id = ?, final_event = ?, outcome = ?
         WHERE request = ? AND route = ? AND requester = ? AND requester_generation = ?
           AND thread = ? AND final_event IS NULL",
        params![
            serde_json::to_string(&message.id)?,
            event_id,
            outcome.as_str(),
            serde_json::to_string(&message.payload.reply_to)?,
            serde_json::to_string(binding)?,
            message.payload.target,
            message.payload.target_generation,
            message.payload.thread,
        ],
    )?;
    ensure!(changed == 1, "sandbox final report reservation changed");
    Ok(())
}
