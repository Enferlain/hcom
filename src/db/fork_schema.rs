//! Additive downstream state, kept separate from upstream's migration numbers.
use super::HcomDb;
use anyhow::{Context, Result};

impl HcomDb {
    fn has_approval_column(&self) -> Result<bool> {
        let mut statement = self.conn().prepare("PRAGMA table_info(instances)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(columns.iter().any(|name| name == "pending_tool_use_id"))
    }

    pub(super) fn ensure_fork_schema(&self) -> Result<()> {
        if self.has_approval_column()? {
            return Ok(());
        }
        self.with_write_scope(|| {
            // Recheck under the writer lock: concurrent CLI/hook processes can
            // arrive at the same upstream database before this column exists.
            if !self.has_approval_column()? {
                self.conn().execute_batch(
                    "ALTER TABLE instances ADD COLUMN pending_tool_use_id TEXT DEFAULT '';",
                )?;
            }
            Ok(())
        })
    }

    pub(super) fn has_legacy_fork_stamp(&self) -> Result<bool> {
        let version: i32 = self
            .conn()
            .query_row("PRAGMA user_version", [], |r| r.get(0))?;
        // The installed f687a9c rebuild used 19 for the approval column.
        // Upstream independently used 19 for the FTS delete repair.
        Ok(version == 19 && self.has_approval_column()?)
    }

    pub(super) fn repair_legacy_fork_fts(&self) -> Result<()> {
        let (_, sql) = super::MIGRATIONS
            .iter()
            .find(|(version, _)| *version == 19)
            .context("upstream FTS migration 19 is unavailable")?;
        self.with_write_scope(|| {
            // The supported installed fork has FTS already. Recreating an
            // absent derived table also keeps this compatibility repair from
            // preventing access to otherwise intact legacy rows/messages.
            self.conn().execute_batch(
                "CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
                    searchable, tokenize='unicode61'
                 );",
            )?;
            self.conn().execute_batch(sql)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_db() -> HcomDb {
        let db = HcomDb::open_raw(std::path::Path::new(":memory:")).unwrap();
        db.init_db().unwrap();
        db
    }

    #[test]
    fn upstream_database_gets_additive_state_without_changing_its_version() {
        let mut db = memory_db();
        db.conn().execute_batch(
            "INSERT INTO instances(name, tool, status, created_at) VALUES ('kept', 'claude', 'active', 100);
             ALTER TABLE instances DROP COLUMN pending_tool_use_id;",
        ).unwrap();
        db.ensure_schema().unwrap();
        assert!(db.has_approval_column().unwrap());
        assert!(db.get_instance_full("kept").unwrap().is_some());
        let version: i32 = db
            .conn()
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, super::super::SCHEMA_VERSION);
    }

    #[test]
    fn installed_fork_stamp_preserves_rows_and_repairs_skipped_upstream_migration() {
        let mut db = memory_db();
        db.conn()
            .execute_batch(
                "INSERT INTO instances(name, tool, status, created_at, pending_tool_use_id)
             VALUES ('kept', 'claude', 'blocked', 100, 'owning-call');
             DROP TRIGGER events_fts_delete;
             PRAGMA user_version = 19;",
            )
            .unwrap();
        let event_id = db
            .log_event(
                "message",
                "kept",
                &serde_json::json!({"text": "retained mail"}),
            )
            .unwrap();
        db.conn()
            .execute(
                "UPDATE instances SET last_event_id = ? WHERE name = 'kept'",
                [event_id],
            )
            .unwrap();
        db.ensure_schema().unwrap();
        let row = db.get_instance_full("kept").unwrap().unwrap();
        assert_eq!(row.pending_tool_use_id, "owning-call");
        assert_eq!(row.last_event_id, event_id);
        let messages = db
            .get_events_since(0, Some("message"), Some("kept"))
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["data"]["text"], "retained mail");
        let count: i64 = db.conn().query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name='events_fts_delete'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(count, 1);
        db.ensure_schema().unwrap();
        assert!(db.get_instance_full("kept").unwrap().is_some());
    }

    #[test]
    fn legacy_repair_does_not_require_the_derived_fts_table() {
        let mut db = memory_db();
        db.conn().execute_batch(
            "INSERT INTO instances(name,status,created_at,last_event_id) VALUES ('kept','active',100,7);
             DROP TABLE events_fts;
             PRAGMA user_version = 19;"
        ).unwrap();
        db.ensure_schema().unwrap();
        assert_eq!(
            db.get_instance_full("kept").unwrap().unwrap().last_event_id,
            7
        );
        let event = db
            .log_event(
                "message",
                "kept",
                &serde_json::json!({"text":"still writable"}),
            )
            .unwrap();
        assert!(event > 0);
        let indexed: i64 = db
            .conn()
            .query_row(
                "SELECT count(*) FROM events_fts WHERE events_fts MATCH 'writable'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(indexed, 1);
    }

    #[test]
    fn concurrent_extension_initializers_preserve_upstream_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let db = HcomDb::open_raw(&path).unwrap();
        db.init_db().unwrap();
        db.conn().execute_batch(
            "INSERT INTO instances(name, tool, status, created_at) VALUES ('kept', 'claude', 'active', 100);
             ALTER TABLE instances DROP COLUMN pending_tool_use_id;",
        ).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let db = HcomDb::open_raw(&path).unwrap();
                    barrier.wait();
                    db.ensure_fork_schema().unwrap();
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(db.has_approval_column().unwrap());
        assert!(db.get_instance_full("kept").unwrap().is_some());
    }
}
