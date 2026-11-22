use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, params};

use super::protocol::Binding;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    pub messages: i64,
    pub bytes: i64,
}

impl Budget {
    const fn new(messages: i64, bytes: i64) -> Self {
        Self { messages, bytes }
    }

    fn charge(self, used: Self, bytes: i64, scope: &str) -> Result<Self> {
        ensure!(
            used.messages < self.messages && bytes <= self.bytes.saturating_sub(used.bytes),
            "sandbox {scope} quota exhausted"
        );
        Ok(Self::new(used.messages + 1, used.bytes + bytes))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub pending: Budget,
    pub run: Budget,
    pub team: Budget,
    pub host: Budget,
    pub pace: Budget,
    pub pace_seconds: i64,
    pub queue_ttl_seconds: i64,
}

impl Default for Limits {
    fn default() -> Self {
        const MIB: i64 = 1024 * 1024;
        Self {
            pending: Budget::new(256, 16 * MIB),
            run: Budget::new(2_000, 32 * MIB),
            team: Budget::new(8_000, 128 * MIB),
            host: Budget::new(32_000, 512 * MIB),
            pace: Budget::new(32, MIB),
            pace_seconds: 10,
            queue_ttl_seconds: 3_600,
        }
    }
}

impl Limits {
    pub(super) fn validate(self) -> Result<()> {
        for budget in [self.pending, self.run, self.team, self.host, self.pace] {
            ensure!(
                budget.messages > 0 && budget.bytes > 0,
                "sandbox budgets must be positive"
            );
        }
        ensure!(
            self.pace_seconds > 0 && self.queue_ttl_seconds > 0,
            "sandbox admission durations must be positive"
        );
        Ok(())
    }

    pub(super) fn pending(self, tx: &Transaction<'_>, wire_bytes: i64) -> Result<()> {
        let used = tx.query_row(
            "SELECT count(*), COALESCE(sum(wire_bytes), 0) FROM sandbox_outbox WHERE accepted = 0",
            [],
            |row| Ok(Budget::new(row.get(0)?, row.get(1)?)),
        )?;
        self.pending.charge(used, wire_bytes, "pending queue")?;
        Ok(())
    }

    pub(super) fn record(
        self,
        tx: &Transaction<'_>,
        binding: &Binding,
        wire_bytes: i64,
        event_id: i64,
        paced_at: Option<i64>,
    ) -> Result<()> {
        // Account for the actual hcom event as well as the wire message and a
        // fixed per-record allowance for the indexes and ledger metadata.
        let event_bytes: i64 = tx.query_row(
            "SELECT length(CAST(timestamp AS BLOB)) + length(CAST(type AS BLOB)) +
                    length(CAST(instance AS BLOB)) + length(CAST(data AS BLOB))
             FROM events WHERE id = ?",
            [event_id],
            |row| row.get(0),
        )?;
        let bytes = wire_bytes.saturating_add(event_bytes).saturating_add(256);
        let run_key = serde_json::to_string(&(&binding.sandbox, &binding.worker.generation))?;
        if let Some(now) = paced_at {
            self.record_pacing(tx, &run_key, wire_bytes, now)?;
        }
        for (scope, key, limit) in [
            ("run", run_key.as_str(), self.run),
            ("team", binding.team.as_str(), self.team),
            ("host", "", self.host),
        ] {
            let used = tx
                .query_row(
                    "SELECT messages, bytes FROM sandbox_usage WHERE scope = ? AND scope_key = ?",
                    params![scope, key],
                    |row| Ok(Budget::new(row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .unwrap_or(Budget::new(0, 0));
            let charged = limit.charge(used, bytes, scope)?;
            tx.execute(
                "INSERT INTO sandbox_usage VALUES (?, ?, ?, ?)
                 ON CONFLICT (scope, scope_key) DO UPDATE SET messages = excluded.messages, bytes = excluded.bytes",
                params![scope, key, charged.messages, charged.bytes],
            )?;
        }
        Ok(())
    }

    fn record_pacing(
        self,
        tx: &Transaction<'_>,
        run_key: &str,
        bytes: i64,
        now: i64,
    ) -> Result<()> {
        let (start, used) = tx
            .query_row(
                "SELECT started_at, messages, bytes FROM sandbox_pacing WHERE run_key = ?",
                [run_key],
                |row| Ok((row.get::<_, i64>(0)?, Budget::new(row.get(1)?, row.get(2)?))),
            )
            .optional()?
            .unwrap_or((now, Budget::new(0, 0)));
        // A fixed window anchored at the first admitted message.
        // A backwards clock change preserves the previous charge.
        let (start, used) = if now.saturating_sub(start) >= self.pace_seconds {
            (now, Budget::new(0, 0))
        } else {
            (start, used)
        };
        let charged = self.pace.charge(used, bytes, "guest pacing")?;
        tx.execute(
            "INSERT INTO sandbox_pacing VALUES (?, ?, ?, ?)
             ON CONFLICT (run_key) DO UPDATE SET started_at = excluded.started_at,
                 messages = excluded.messages, bytes = excluded.bytes",
            params![run_key, start, charged.messages, charged.bytes],
        )?;
        Ok(())
    }

    pub(super) fn pace_guest(
        self,
        tx: &Transaction<'_>,
        binding: &Binding,
        wire_bytes: i64,
        now: i64,
    ) -> Result<()> {
        let run_key = serde_json::to_string(&(&binding.sandbox, &binding.worker.generation))?;
        self.record_pacing(tx, &run_key, wire_bytes, now)
    }
}
