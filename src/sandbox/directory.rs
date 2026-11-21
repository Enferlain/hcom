use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::db::{HcomDb, InstanceRow};
use crate::shared::time::now_epoch_f64;
use crate::shared::{SenderIdentity, SenderKind};
use crate::tool::Tool;

use super::protocol::{Binding, Origin, validate_identifier};
use super::store::Store;

const PROJECTION_PREFIX: &str = "openshell:";

pub(super) const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS sandbox_teams (
        id TEXT PRIMARY KEY,
        realm TEXT NOT NULL UNIQUE,
        owner_name TEXT,
        owner_created_at REAL,
        controller_token TEXT,
        CHECK ((owner_name IS NOT NULL AND owner_created_at IS NOT NULL AND controller_token IS NULL)
          OR (owner_name IS NULL AND owner_created_at IS NULL AND controller_token IS NOT NULL))
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_team_hosts (
        team TEXT NOT NULL REFERENCES sandbox_teams(id),
        name TEXT NOT NULL,
        created_at REAL NOT NULL,
        PRIMARY KEY (team, name, created_at)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS sandbox_workers (
        generation TEXT PRIMARY KEY,
        team TEXT NOT NULL REFERENCES sandbox_teams(id),
        alias TEXT NOT NULL,
        name TEXT NOT NULL,
        binding TEXT NOT NULL UNIQUE,
        gateway TEXT NOT NULL,
        workspace TEXT NOT NULL,
        retired INTEGER NOT NULL CHECK (retired IN (0, 1))
    ) STRICT;
    CREATE UNIQUE INDEX IF NOT EXISTS sandbox_workers_current
        ON sandbox_workers(name) WHERE retired = 0;
    CREATE INDEX IF NOT EXISTS sandbox_workers_name
        ON sandbox_workers(name);
    CREATE INDEX IF NOT EXISTS sandbox_workers_team_alias
        ON sandbox_workers(team, alias) WHERE retired = 0;
";

/// Captured from the resolved local hcom identity, then rechecked in every
/// membership transaction. A reused display name does not inherit membership.
pub struct HostIdentity {
    name: String,
    created_at: f64,
}

impl HostIdentity {
    pub fn capture(db: &HcomDb, sender: &SenderIdentity) -> Result<Self> {
        ensure!(
            sender.kind == SenderKind::Instance,
            "sandbox requester is not a host participant"
        );
        let created_at = sender
            .instance_data
            .as_ref()
            .and_then(|data| data.get("created_at"))
            .and_then(serde_json::Value::as_f64)
            .context("sandbox requester has no instance generation")?;
        let identity = Self {
            name: sender.name.clone(),
            created_at,
        };
        identity.check(db.conn())?;
        Ok(identity)
    }

    fn check(&self, connection: &Connection) -> Result<()> {
        let current: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM instances
             WHERE name = ? AND created_at = ? AND COALESCE(origin_device_id, '') = '')",
            params![self.name, self.created_at],
            |row| row.get(0),
        )?;
        ensure!(
            current && current_worker(connection, &self.name)?.is_none(),
            "sandbox requester no longer names the same local instance"
        );
        Ok(())
    }
}

/// A controller capability for a launch with no registered hcom requester.
pub struct ExternalController {
    pub team: String,
    pub token: String,
}

pub struct Team {
    pub id: String,
    pub external_controller: Option<ExternalController>,
}

#[derive(Clone, Copy)]
pub enum Access<'a> {
    Host(&'a HostIdentity),
    Controller(&'a ExternalController),
}

pub struct Placement<'a> {
    pub gateway: &'a str,
    pub workspace: &'a str,
    pub sandbox: &'a str,
    pub incarnation: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Worker {
    pub alias: String,
    pub binding: Binding,
    pub gateway: String,
    pub workspace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Location {
    Local,
    TrustedRelay(String),
    OpenShell(Worker),
}

impl Store<'_> {
    pub fn create_team(&self, requester: Option<&HostIdentity>) -> Result<Team> {
        self.write(|tx| {
            if let Some(host) = requester {
                host.check(tx)?;
            }
            let token = requester.is_none().then(|| Uuid::new_v4().to_string());
            for _ in 0..8 {
                let id = Uuid::new_v4().simple().to_string();
                let realm = &id[..8];
                let owner_name = requester.map(|host| host.name.as_str());
                let owner_created_at = requester.map(|host| host.created_at);
                let inserted = tx.execute(
                    "INSERT INTO sandbox_teams VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT(realm) DO NOTHING",
                    params![id, realm, owner_name, owner_created_at, token],
                )?;
                if inserted == 0 {
                    continue;
                }
                if let Some(host) = requester {
                    add_host(tx, &id, host)?;
                }
                return Ok(Team {
                    id: id.clone(),
                    external_controller: token.map(|token| ExternalController { team: id, token }),
                });
            }
            anyhow::bail!("could not allocate a sandbox team realm")
        })
    }

    pub fn enroll_host(&self, team: &str, access: Access<'_>, host: &HostIdentity) -> Result<()> {
        self.write(|tx| {
            authorize(tx, team, access, true)?;
            host.check(tx)?;
            add_host(tx, team, host)
        })
    }

    pub fn reserve_worker(
        &self,
        team: &str,
        access: Access<'_>,
        alias: &str,
        tool: Tool,
        placement: Placement<'_>,
    ) -> Result<Worker> {
        ensure!(
            alias.len() <= 24 && crate::identity::is_valid_base_name(alias),
            "sandbox alias must be a lowercase base instance name of at most 24 bytes"
        );
        ensure!(
            ![crate::shared::SENDER, "mention", "mentions", "name"].contains(&alias),
            "sandbox alias is reserved by hcom message routing"
        );
        validate_identifier(placement.gateway)?;
        validate_identifier(placement.workspace)?;
        self.write(|tx| {
            let realm = authorize(tx, team, access, false)?;
            let canonical = reserve_canonical(tx, alias, &realm)?;
            let generation = Uuid::new_v4().to_string();
            let binding = Binding {
                team: team.into(),
                sandbox: placement.sandbox.into(),
                worker: Origin {
                    worker: canonical.clone(),
                    generation: generation.clone(),
                    incarnation: placement.incarnation.into(),
                },
            };
            binding.validate()?;
            tx.execute(
                "INSERT INTO sandbox_workers VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
                params![
                    generation,
                    team,
                    alias,
                    canonical,
                    serde_json::to_string(&binding)?,
                    placement.gateway,
                    placement.workspace,
                ],
            )?;
            tx.execute(
                "INSERT INTO instances
                   (name, origin_device_id, tool, created_at, status, status_context, background)
                 VALUES (?, ?, ?, ?, 'inactive', 'sandbox', 1)",
                params![
                    canonical,
                    projection_id(&generation),
                    tool.as_str(),
                    now_epoch_f64(),
                ],
            )?;
            Ok(Worker {
                alias: alias.into(),
                binding,
                gateway: placement.gateway.into(),
                workspace: placement.workspace.into(),
            })
        })
    }

    pub fn retire_worker(&self, access: Access<'_>, binding: &Binding) -> Result<()> {
        self.write(|tx| {
            authorize(tx, &binding.team, access, true)?;
            let changed = tx.execute(
                "UPDATE sandbox_workers SET retired = 1 WHERE binding = ? AND retired = 0",
                [serde_json::to_string(binding)?],
            )?;
            ensure!(changed == 1, "sandbox worker generation is not active");
            tx.execute(
                "DELETE FROM instances WHERE name = ? AND origin_device_id = ?",
                params![
                    binding.worker.worker,
                    projection_id(&binding.worker.generation)
                ],
            )?;
            Ok(())
        })
    }

    pub fn resolve_worker(
        &self,
        team: &str,
        access: Access<'_>,
        name: &str,
    ) -> Result<Option<Worker>> {
        let conn = self.connection()?;
        authorize(conn, team, access, false)?;
        let mut query = conn.prepare_cached(
            "SELECT alias, binding, gateway, workspace
             FROM sandbox_workers WHERE team = ? AND retired = 0 AND (name = ? OR alias = ?)
             LIMIT 2",
        )?;
        let mut matches = query.query(params![team, name, name])?;
        let Some(first) = matches.next()? else {
            return Ok(None);
        };
        let worker = decode_worker(first)?;
        ensure!(
            matches.next()?.is_none(),
            "ambiguous sandbox alias; use a canonical name"
        );
        if worker.binding.worker.worker != name {
            let host_collision: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sandbox_team_hosts h JOIN instances i
                     ON i.name = h.name AND i.created_at = h.created_at
                     WHERE h.team = ? AND h.name = ? AND COALESCE(i.origin_device_id, '') = '')",
                params![team, name],
                |row| row.get(0),
            )?;
            ensure!(
                !host_collision,
                "ambiguous sandbox alias; use a canonical name"
            );
        }
        Ok(Some(worker))
    }
}

fn add_host(conn: &Connection, team: &str, host: &HostIdentity) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO sandbox_team_hosts VALUES (?, ?, ?)",
        params![team, host.name, host.created_at],
    )?;
    Ok(())
}

fn authorize(
    conn: &Connection,
    team: &str,
    access: Access<'_>,
    require_owner: bool,
) -> Result<String> {
    match access {
        Access::Host(host) => {
            host.check(conn)?;
            conn.query_row(
                "SELECT t.realm FROM sandbox_teams t JOIN sandbox_team_hosts h ON h.team = t.id
                 WHERE t.id = ? AND h.name = ? AND h.created_at = ?
                   AND (NOT ? OR (t.owner_name = h.name AND t.owner_created_at = h.created_at))",
                params![team, host.name, host.created_at, require_owner],
                |row| row.get(0),
            )
            .optional()?
        }
        Access::Controller(controller) => {
            ensure!(controller.team == team, "sandbox team access denied");
            conn.query_row(
                "SELECT realm FROM sandbox_teams WHERE id = ? AND controller_token = ?",
                params![team, controller.token],
                |row| row.get(0),
            )
            .optional()?
        }
    }
    .context("sandbox team access denied")
}

fn reserve_canonical(conn: &Connection, alias: &str, realm: &str) -> Result<String> {
    for index in 1..=512 {
        let name = if index == 1 {
            format!("{alias}.{realm}")
        } else {
            format!("{alias}_{index}.{realm}")
        };
        let used: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM instances WHERE name = ?)
                 OR EXISTS(SELECT 1 FROM sandbox_workers WHERE name = ? AND retired = 0)",
            params![name, name],
            |row| row.get(0),
        )?;
        if !used {
            return Ok(name);
        }
    }
    anyhow::bail!("sandbox canonical name pool exhausted")
}

fn projection_id(generation: &str) -> String {
    format!("{PROJECTION_PREFIX}{generation}")
}

pub fn location(db: &HcomDb, row: &InstanceRow) -> Result<Location> {
    let device = row.origin_device_id.as_deref();
    if let Some(worker) = current_worker(db.conn(), &row.name)? {
        let expected = projection_id(&worker.binding.worker.generation);
        ensure!(
            device == Some(expected.as_str()),
            "sandbox projection no longer matches its owned generation"
        );
        return Ok(Location::OpenShell(worker));
    }
    match device {
        None => Ok(Location::Local),
        Some(device) if device.starts_with(PROJECTION_PREFIX) => {
            anyhow::bail!("sandbox projection is no longer owned")
        }
        Some(device) => Ok(Location::TrustedRelay(device.to_owned())),
    }
}

fn current_worker(conn: &Connection, name: &str) -> Result<Option<Worker>> {
    if !has_directory(conn)? {
        return Ok(None);
    }
    let mut stmt = conn.prepare_cached(
        "SELECT alias, binding, gateway, workspace FROM sandbox_workers
         WHERE name = ? AND retired = 0",
    )?;
    let mut rows = stmt.query([name])?;
    rows.next()?.map(decode_worker).transpose()
}

fn has_directory(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'sandbox_workers')",
        [],
        |row| row.get(0),
    )?)
}

pub(crate) fn active_worker_names(db: &HcomDb) -> Result<std::collections::HashSet<String>> {
    let conn = db.conn();
    if !has_directory(conn)? {
        return Ok(Default::default());
    }
    let mut stmt = conn.prepare("SELECT name FROM sandbox_workers WHERE retired = 0")?;
    Ok(stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

fn decode_worker(row: &rusqlite::Row<'_>) -> Result<Worker> {
    let key: String = row.get(1)?;
    let binding: Binding = serde_json::from_str(&key)?;
    binding.validate()?;
    Ok(Worker {
        alias: row.get(0)?,
        binding,
        gateway: row.get(2)?,
        workspace: row.get(3)?,
    })
}

/// Keep retired generations separate from fresh reservations of the same
/// canonical name. The host caller obtains its expected binding from the
/// directory. Unregistered routes exist on guest stores and transport fixtures,
/// so this check is a fence for known bindings, not a host enrollment check.
pub(super) fn check_route(conn: &Connection, binding: &str) -> Result<()> {
    let (known, current): (bool, bool) = conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM sandbox_workers
                       WHERE name = json_extract(?1, '$.worker.worker')
                          OR generation = json_extract(?1, '$.worker.generation')),
                EXISTS(SELECT 1 FROM sandbox_workers WHERE binding = ?1 AND retired = 0)",
        )?
        .query_row([binding], |row| Ok((row.get(0)?, row.get(1)?)))?;
    ensure!(
        !known || current,
        "sandbox worker generation is retired or does not match enrollment"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        _root: tempfile::TempDir,
        db: HcomDb,
    }

    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let db = HcomDb::open_at(&root.path().join("hcom.db")).unwrap();
            Self { _root: root, db }
        }

        fn host(&self, name: &str, created: f64) -> HostIdentity {
            self.db
                .conn()
                .execute(
                    "INSERT INTO instances (name, created_at) VALUES (?, ?)",
                    params![name, created],
                )
                .unwrap();
            let sender = crate::identity::resolve_from_name(&self.db, name).unwrap();
            HostIdentity::capture(&self.db, &sender).unwrap()
        }

        fn store(&self) -> Store<'_> {
            Store::initialize(&self.db).unwrap()
        }
    }

    fn placement(sandbox: &str) -> Placement<'_> {
        Placement {
            gateway: "gateway",
            workspace: "default",
            sandbox,
            incarnation: "fedc8acb-cc62-422d-9724-54e730e64f6e",
        }
    }

    #[test]
    fn canonical_names_survive_collisions_without_claiming_foreign_or_local_rows() {
        let fixture = Fixture::new();
        let host = fixture.host("host", 1.0);
        let store = fixture.store();
        let team = store.create_team(Some(&host)).unwrap();
        let access = Access::Host(&host);
        for reserved in [crate::shared::SENDER, "mention", "mentions", "name"] {
            assert!(
                store
                    .reserve_worker(
                        &team.id,
                        access,
                        reserved,
                        Tool::Claude,
                        placement("blocked"),
                    )
                    .is_err()
            );
        }
        let blocked = format!("worker.{}", &team.id[..8]);
        fixture.db.conn().execute(
            "INSERT INTO instances (name, origin_device_id, created_at) VALUES (?, 'relay-1', 2)",
            [&blocked],
        ).unwrap();
        let a = store
            .reserve_worker(&team.id, access, "worker", Tool::Claude, placement("vm-1"))
            .unwrap();
        let b = store
            .reserve_worker(&team.id, access, "worker", Tool::Claude, placement("vm-2"))
            .unwrap();
        assert_ne!(a.binding.worker.worker, b.binding.worker.worker);
        assert_ne!(a.binding.worker.worker, blocked);
        assert!(
            store
                .resolve_worker(&team.id, access, "worker")
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );
        assert_eq!(
            store
                .resolve_worker(&team.id, access, &a.binding.worker.worker)
                .unwrap(),
            Some(a.clone())
        );
        let row = fixture
            .db
            .get_instance_full(&a.binding.worker.worker)
            .unwrap()
            .unwrap();
        assert!(row.pid.is_none() && row.session_id.is_none() && row.transcript_path.is_empty());
        let mut altered = row.clone();
        altered.origin_device_id = None;
        assert!(location(&fixture.db, &altered).is_err());
        assert_eq!(location(&fixture.db, &row).unwrap(), Location::OpenShell(a));
        let host = fixture.db.get_instance_full("host").unwrap().unwrap();
        let relay = fixture.db.get_instance_full(&blocked).unwrap().unwrap();
        assert_eq!(location(&fixture.db, &host).unwrap(), Location::Local);
        assert_eq!(
            location(&fixture.db, &relay).unwrap(),
            Location::TrustedRelay("relay-1".into())
        );
        store
            .reserve_worker(&team.id, access, "host", Tool::Claude, placement("vm-3"))
            .unwrap();
        assert!(store.resolve_worker(&team.id, access, "host").is_err());
    }

    #[test]
    fn membership_requires_host_owner_and_current_generation() {
        let fixture = Fixture::new();
        let host = fixture.host("host", 1.0);
        let member = fixture.host("member", 2.0);
        let outsider = fixture.host("outsider", 3.0);
        let store = fixture.store();
        let team = store.create_team(Some(&host)).unwrap();
        assert!(
            store
                .enroll_host(&team.id, Access::Host(&outsider), &outsider)
                .is_err()
        );
        assert!(
            store
                .reserve_worker(
                    &team.id,
                    Access::Host(&outsider),
                    "worker",
                    Tool::Codex,
                    placement("vm-1")
                )
                .is_err()
        );
        store
            .enroll_host(&team.id, Access::Host(&host), &member)
            .unwrap();
        assert!(
            store
                .enroll_host(&team.id, Access::Host(&member), &outsider)
                .is_err()
        );
        store
            .reserve_worker(
                &team.id,
                Access::Host(&member),
                "worker",
                Tool::Codex,
                placement("vm-1"),
            )
            .unwrap();
        fixture
            .db
            .conn()
            .execute(
                "UPDATE instances SET created_at = 4 WHERE name = 'member'",
                [],
            )
            .unwrap();
        assert!(
            store
                .resolve_worker(&team.id, Access::Host(&member), "worker")
                .is_err()
        );
        let new_member = HostIdentity::capture(
            &fixture.db,
            &crate::identity::resolve_from_name(&fixture.db, "member").unwrap(),
        )
        .unwrap();
        assert!(
            store
                .resolve_worker(&team.id, Access::Host(&new_member), "worker")
                .is_err()
        );
    }

    #[test]
    fn external_launcher_gets_controller_authority_without_a_host_identity() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let team = store.create_team(None).unwrap();
        let controller = team.external_controller.unwrap();
        let existing = fixture.db.iter_instances_full().unwrap();
        assert!(existing.is_empty());
        let forged = ExternalController {
            team: team.id.clone(),
            token: Uuid::new_v4().to_string(),
        };
        assert!(
            store
                .reserve_worker(
                    &team.id,
                    Access::Controller(&forged),
                    "worker",
                    Tool::Codex,
                    placement("vm-1")
                )
                .is_err()
        );
        store
            .reserve_worker(
                &team.id,
                Access::Controller(&controller),
                "worker",
                Tool::Codex,
                placement("vm-1"),
            )
            .unwrap();
        let other = store.create_team(None).unwrap();
        assert!(
            store
                .resolve_worker(&other.id, Access::Controller(&controller), "worker")
                .is_err()
        );
    }

    #[test]
    fn retiring_a_worker_fences_its_connection_when_the_name_is_reused() {
        let fixture = Fixture::new();
        let host = fixture.host("host", 1.0);
        let store = fixture.store();
        let team = store.create_team(Some(&host)).unwrap();
        let access = Access::Host(&host);
        let old = store
            .reserve_worker(&team.id, access, "worker", Tool::Codex, placement("vm-1"))
            .unwrap();
        let old_projection = fixture
            .db
            .get_instance_full(&old.binding.worker.worker)
            .unwrap()
            .unwrap();
        let live = store.attach_bridge(&old.binding, 1).unwrap();
        store.retire_worker(access, &old.binding).unwrap();
        assert!(store.retire_worker(access, &old.binding).is_err());
        let new = store
            .reserve_worker(&team.id, access, "worker", Tool::Codex, placement("vm-2"))
            .unwrap();
        assert_eq!(old.binding.worker.worker, new.binding.worker.worker);
        assert_ne!(old.binding.worker.generation, new.binding.worker.generation);
        assert!(location(&fixture.db, &old_projection).is_err());
        assert!(store.attach_bridge(&old.binding, 2).is_err());
        let mut forged_binding = new.binding.clone();
        forged_binding.worker.incarnation = Uuid::new_v4().to_string();
        assert!(store.attach_bridge(&forged_binding, 2).is_err());
        assert!(live.check_bridge().is_err());
        let stale = super::super::protocol::Message::new(
            super::super::protocol::MessageId {
                origin: old.binding.worker.clone(),
                sequence: 1,
            },
            super::super::protocol::MessagePayload {
                target: "host".into(),
                thread: None,
                reply_to: None,
                intent: super::super::protocol::Intent::Inform,
                text: "stale".into(),
            },
        )
        .unwrap();
        for importer in [&live, &store] {
            assert!(
                importer
                    .import(
                        super::super::protocol::Receiver::Host,
                        &old.binding,
                        &stale,
                        1,
                        |_| { panic!("retired guest must not import") }
                    )
                    .is_err()
            );
        }
        assert_eq!(
            store.resolve_worker(&team.id, access, "worker").unwrap(),
            Some(new)
        );
    }
}
