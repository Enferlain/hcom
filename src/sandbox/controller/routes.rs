use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::db::HcomDb;
use crate::sandbox::bridge::{self, ExecTarget, HostInbox, Session};
use crate::sandbox::directory::{Worker, current_worker};
use crate::sandbox::openshell::{Adapter, Profile, Qualification};
use crate::sandbox::protocol::Binding;

use super::{Owner, Store};

const RETRY: Duration = Duration::from_secs(2);
const MAX_RETRY: Duration = Duration::from_secs(60);
// Leave time for the controller to release its lock before stop's five-second wait.
const DRAIN: Duration = Duration::from_secs(4);
pub(super) const STEP: Duration = Duration::from_millis(5);

/// Captured by trusted provisioning. Only the controller reads the key.
#[derive(PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BridgePlan {
    pub profile: Profile,
    pub qualification: Qualification,
    pub sandbox_name: String,
    pub guest_hcom: String,
    pub guest_manifest: String,
    pub startup_key: [u8; 32],
}

impl BridgePlan {
    fn check(&self, worker: &Worker) -> Result<()> {
        ensure!(
            self.profile.gateway == worker.gateway && self.profile.workspace == worker.workspace,
            "sandbox execution plan belongs to another gateway or workspace"
        );
        ensure!(
            bridge::absolute_guest_path(&self.guest_hcom)
                && bridge::absolute_guest_path(&self.guest_manifest)
                && self.startup_key.iter().any(|&byte| byte != 0),
            "invalid sandbox execution paths or startup key"
        );
        Ok(())
    }

    fn target(&self) -> ExecTarget<'_> {
        ExecTarget {
            sandbox_name: &self.sandbox_name,
            guest_hcom: &self.guest_hcom,
            guest_manifest: &self.guest_manifest,
            startup_key: Some(&self.startup_key),
        }
    }
}

impl Store<'_> {
    /// Save the verified placement inputs once per worker generation. The
    /// existing run-work record controls when the controller starts the route.
    pub fn register_bridge(&self, worker: &Worker, plan: &BridgePlan) -> Result<()> {
        plan.check(worker)?;
        let encoded = serde_json::to_string(plan)?;
        self.write(|tx| {
            ensure!(
                current_worker(tx, &worker.binding.worker.worker)?.as_ref() == Some(worker),
                "sandbox execution plan has no active owned worker"
            );
            let generation = &worker.binding.worker.generation;
            tx.execute(
                "INSERT OR IGNORE INTO sandbox_controller_routes VALUES (?1, ?2)",
                params![generation, encoded],
            )?;
            let saved: String = tx.query_row(
                "SELECT plan FROM sandbox_controller_routes WHERE generation = ?",
                [generation],
                |row| row.get(0),
            )?;
            ensure!(
                saved == encoded,
                "sandbox execution plan already differs for this worker generation"
            );
            Ok(())
        })
    }

    fn running_bridges(&self) -> Result<Vec<String>> {
        let conn = self.connection()?;
        let mut query = conn.prepare_cached(
            "SELECT r.generation FROM sandbox_controller_routes r
             JOIN sandbox_workers w USING (generation)
             JOIN sandbox_controller_work s ON s.run_key = r.generation
             WHERE w.retired = 0 AND s.state = 'running'
             ORDER BY r.generation COLLATE BINARY",
        )?;
        Ok(query
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    fn load_running_bridge(&self, generation: &str) -> Result<Option<(Worker, BridgePlan)>> {
        let conn = self.connection()?;
        let row: Option<(String, String)> = conn
            .query_row(
                "SELECT w.name, r.plan FROM sandbox_controller_routes r
             JOIN sandbox_workers w USING (generation)
             JOIN sandbox_controller_work s ON s.run_key = r.generation
             WHERE r.generation = ? AND w.retired = 0 AND s.state = 'running'",
                [generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((name, plan)) = row else {
            return Ok(None);
        };
        let Some(worker) = current_worker(conn, &name)?
            .filter(|worker| worker.binding.worker.generation == generation)
        else {
            return Ok(None);
        };
        let plan: BridgePlan =
            serde_json::from_str(&plan).context("invalid stored sandbox execution plan")?;
        plan.check(&worker)?;
        Ok(Some((worker, plan)))
    }
}

struct Active<'store, 'db> {
    binding: Binding,
    session: Session<'store, 'db>,
    stopping: Option<Instant>,
}

struct Retry {
    until: Instant,
    delay: Duration,
}

struct Probe {
    generation: String,
    cancel: Arc<AtomicBool>,
    result: Option<JoinHandle<Result<Prepared>>>,
}

struct Prepared {
    worker: Worker,
    plan: BridgePlan,
    adapter: Arc<Adapter>,
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub(super) struct Routes<'store, 'db> {
    active: BTreeMap<String, Active<'store, 'db>>,
    adapters: BTreeMap<String, Arc<Adapter>>,
    retry: BTreeMap<String, Retry>,
    desired: Vec<String>,
    probe: Option<Probe>,
    last_attempt: Option<String>,
}

impl<'store, 'db> Routes<'store, 'db> {
    pub(super) fn refresh(&mut self, store: &Store<'_>) -> Result<()> {
        self.desired = store.running_bridges()?;
        let desired = &self.desired;
        self.active.retain(|generation, active| {
            if desired.binary_search(generation).is_ok() {
                return true;
            }
            if !active.session.ready() {
                return false;
            }
            active
                .stopping
                .get_or_insert_with(|| Instant::now() + DRAIN);
            true
        });
        self.adapters
            .retain(|generation, _| desired.binary_search(generation).is_ok());
        self.retry
            .retain(|generation, _| desired.binary_search(generation).is_ok());
        if let Some(probe) = &self.probe
            && desired.binary_search(&probe.generation).is_err()
        {
            probe.cancel.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn is_active(&self) -> bool {
        !self.active.is_empty()
    }

    pub(super) fn poll(&mut self, db: &HcomDb, owner: &'store Owner<'_, 'db>) {
        let store = owner.store();
        self.active.retain(|generation, active| {
            if active
                .stopping
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return false;
            }
            match active.session.poll(
                active.stopping.is_some(),
                &mut HostInbox::new(db, store, &active.binding),
            ) {
                Ok(None) => {
                    if active.session.ready() && active.stopping.is_none() {
                        self.retry.remove(generation);
                    }
                    true
                }
                Ok(Some(_)) => false,
                Err(error) => {
                    self.adapters.remove(generation);
                    if active.stopping.is_none() {
                        failed(&mut self.retry, generation, error);
                    }
                    false
                }
            }
        });
        if let Some(probe) = &self.probe
            && probe.result.as_ref().is_some_and(JoinHandle::is_finished)
        {
            let mut probe = self.probe.take().expect("finished probe");
            let generation = &probe.generation;
            let checked = probe
                .result
                .take()
                .expect("probe handle")
                .join()
                .map_err(|_| anyhow::anyhow!("sandbox gateway probe panicked"))
                .and_then(|result| result);
            if !probe.cancel.load(Ordering::Relaxed)
                && self.desired.binary_search(generation).is_ok()
            {
                let started = checked.and_then(
                    |Prepared {
                         worker,
                         plan,
                         adapter,
                     }| {
                        let current = store
                            .load_running_bridge(generation)?
                            .context("sandbox route is no longer running")?;
                        ensure!(
                            current.0 == worker && current.1 == plan,
                            "sandbox route changed during gateway probe"
                        );
                        let session = bridge::start_verified(
                            owner,
                            &adapter,
                            plan.target(),
                            &worker.binding,
                        )?;
                        self.adapters.insert(generation.clone(), adapter);
                        Ok(Active {
                            binding: worker.binding,
                            session,
                            stopping: None,
                        })
                    },
                );
                match started {
                    Ok(active) => {
                        self.active.insert(generation.clone(), active);
                    }
                    Err(error) => {
                        self.adapters.remove(generation);
                        failed(&mut self.retry, generation, error);
                    }
                }
            }
        }
        if self.probe.is_none() {
            let now = Instant::now();
            for generation in probe_order(&self.desired, self.last_attempt.as_deref()) {
                if self.active.contains_key(generation)
                    || self
                        .retry
                        .get(generation)
                        .is_some_and(|retry| retry.until > now)
                {
                    continue;
                }
                self.last_attempt = Some(generation.clone());
                match start_probe(store, generation, self.adapters.get(generation).cloned()) {
                    Ok(Some(probe)) => {
                        self.probe = Some(probe);
                        break;
                    }
                    Ok(None) => {}
                    Err(error) => failed(&mut self.retry, generation, error),
                }
            }
        }
    }

    pub(super) fn drain(&mut self, db: &HcomDb, store: &Store<'_>) {
        if let Some(probe) = &self.probe {
            probe.cancel.store(true, Ordering::Relaxed);
        }
        self.active.retain(|_, active| active.session.ready());
        let deadline = Instant::now() + DRAIN;
        while (!self.active.is_empty()
            || self
                .probe
                .as_ref()
                .and_then(|probe| probe.result.as_ref())
                .is_some_and(|result| !result.is_finished()))
            && Instant::now() < deadline
        {
            self.active.retain(|_, active| {
                matches!(
                    active
                        .session
                        .poll(true, &mut HostInbox::new(db, store, &active.binding)),
                    Ok(None)
                )
            });
            thread::sleep(STEP);
        }
        self.active.clear();
        if let Some(mut probe) = self.probe.take()
            && probe.result.as_ref().is_some_and(JoinHandle::is_finished)
            && let Some(result) = probe.result.take()
        {
            let _ = result.join();
        }
    }
}

fn probe_order<'a>(
    desired: &'a [String],
    last_attempt: Option<&str>,
) -> impl Iterator<Item = &'a String> + use<'a> {
    let pivot = last_attempt.map_or(0, |last| {
        desired.partition_point(|entry| entry.as_str() <= last)
    });
    desired[pivot..].iter().chain(&desired[..pivot])
}

fn start_probe(
    store: &Store<'_>,
    generation: &str,
    adapter: Option<Arc<Adapter>>,
) -> Result<Option<Probe>> {
    let Some((worker, plan)) = store.load_running_bridge(generation)? else {
        return Ok(None);
    };
    let cancel = Arc::new(AtomicBool::new(false));
    let stop = Arc::clone(&cancel);
    let result = thread::Builder::new()
        .name("sandbox-route-probe".into())
        .spawn(move || {
            let adapter = match adapter {
                Some(adapter) => adapter,
                None => Arc::new(Adapter::preflight_until(
                    plan.profile.clone(),
                    &plan.qualification,
                    Some(&stop),
                )?),
            };
            adapter.verify_owned_target_until(&plan.sandbox_name, &worker, Some(&stop))?;
            Ok(Prepared {
                worker,
                plan,
                adapter,
            })
        })?;
    Ok(Some(Probe {
        generation: generation.into(),
        cancel,
        result: Some(result),
    }))
}

fn failed(retry: &mut BTreeMap<String, Retry>, generation: &str, error: anyhow::Error) {
    let delay = retry
        .get(generation)
        .map_or(RETRY, |retry| (retry.delay * 2).min(MAX_RETRY));
    retry.insert(
        generation.into(),
        Retry {
            until: Instant::now() + delay,
            delay,
        },
    );
    crate::log::log_warn(
        "sandbox",
        "bridge.route",
        &format!("{generation}: {error:#}"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::controller::RunState;
    use crate::sandbox::directory::{Access, Placement};
    use crate::sandbox::openshell::{Fingerprint, VmAllocation};

    #[test]
    fn eligible_probes_continue_after_the_last_attempt_even_if_earlier_retries_are_due() {
        let desired = ["a".into(), "c".into(), "d".into()];
        let order = |last| {
            probe_order(&desired, last)
                .map(String::as_str)
                .collect::<Vec<_>>()
        };
        assert_eq!(order(None), ["a", "c", "d"]);
        assert_eq!(order(Some("a")), ["c", "d", "a"]);
        assert_eq!(order(Some("b")), ["c", "d", "a"]);
        assert_eq!(order(Some("d")), ["a", "c", "d"]);
    }

    #[test]
    fn controller_routes_require_an_unchanged_plan_and_running_owner_generation() {
        let root = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&root.path().join("hcom.db")).unwrap();
        let store = Store::initialize(&db).unwrap();
        let team = store.create_team(None).unwrap();
        let access = Access::Controller(team.external_controller.as_ref().unwrap());
        let worker = store
            .reserve_worker(
                &team.id,
                access,
                "worker",
                crate::tool::Tool::Claude,
                Placement {
                    gateway: "gateway",
                    workspace: "default",
                    sandbox: "vm-id",
                    incarnation: &uuid::Uuid::new_v4().to_string(),
                },
            )
            .unwrap();
        let generation = &worker.binding.worker.generation;
        let mut plan = BridgePlan {
            profile: Profile {
                gateway: "gateway".into(),
                workspace: "default".into(),
                endpoint: "http://127.0.0.1:8791".into(),
                cli: "/fixture/openshell".into(),
                gateway_binary: "/fixture/gateway".into(),
                gateway_config: "/fixture/gateway.toml".into(),
                driver_binary: "/fixture/driver".into(),
                config_root: "/fixture/config".into(),
                state_root: "/fixture/state".into(),
            },
            qualification: Qualification {
                fingerprint: Fingerprint {
                    gateway: "gateway".into(),
                    workspace: "default".into(),
                    endpoint: "http://127.0.0.1:8791".into(),
                    cli_sha256: String::new(),
                    gateway_sha256: String::new(),
                    driver_sha256: String::new(),
                    configuration_sha256: String::new(),
                    cli_version: "openshell 0.1.7".into(),
                    gateway_version: "0.1.7".into(),
                    driver_name: "openshell-driver-vm".into(),
                    driver_version: "0.1.7".into(),
                    vm: VmAllocation {
                        vcpus: 3,
                        mem_mib: 2560,
                    },
                },
                measured_stdin_bytes: Some(4 * 1024 * 1024),
            },
            sandbox_name: "fixture".into(),
            guest_hcom: "/fixture/hcom".into(),
            guest_manifest: "/fixture/manifest.json".into(),
            startup_key: [7; 32],
        };
        store.register_bridge(&worker, &plan).unwrap();
        store.register_bridge(&worker, &plan).unwrap();
        assert!(store.running_bridges().unwrap().is_empty());

        store
            .set_run_work(generation, RunState::Running, false)
            .unwrap();
        assert_eq!(
            store.running_bridges().unwrap().as_slice(),
            std::slice::from_ref(generation)
        );
        assert!(
            store
                .load_running_bridge(generation)
                .unwrap()
                .is_some_and(|saved| saved.0 == worker && saved.1 == plan)
        );
        plan.startup_key[0] = 8;
        assert!(store.register_bridge(&worker, &plan).is_err());

        store
            .set_run_work(generation, RunState::Stopped, false)
            .unwrap();
        assert!(store.load_running_bridge(generation).unwrap().is_none());
        store
            .set_run_work(generation, RunState::Running, true)
            .unwrap();
        store.retire_worker(access, &worker.binding).unwrap();
        assert!(store.running_bridges().unwrap().is_empty());
        assert_eq!(
            db.conn()
                .query_row(
                    "SELECT count(*) FROM sandbox_controller_routes",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        assert!(store.register_bridge(&worker, &plan).is_err());
        let work: (String, bool) = db
            .conn()
            .query_row(
                "SELECT state, artifact_pending FROM sandbox_controller_work WHERE run_key = ?",
                [generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(work, ("terminal".into(), true));
        assert!(
            store
                .set_run_work(generation, RunState::Running, false)
                .is_err()
        );
    }
}
