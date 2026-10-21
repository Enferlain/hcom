//! Selected-agent usage is collected off the UI thread; rendering only reads a label.
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant, SystemTime};

use crate::transcript::{self, ReadOptions, TranscriptBackend};
use crate::tui::model::{Agent, AgentStatus};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Source {
    path: PathBuf,
    session: Option<String>,
    backend: TranscriptBackend,
}

impl Source {
    fn for_agent(agent: &Agent) -> Option<Self> {
        if agent.is_remote() {
            return None;
        }
        Some(Self {
            path: agent.transcript_path.as_ref()?.into(),
            session: agent.session_id.clone(),
            backend: transcript::backend_for_tool(agent.tool.canonical()?)?,
        })
    }

    fn stamp(&self) -> Stamp {
        let mut paths = vec![self.path.clone()];
        if self.backend.is_shared_database() {
            let mut wal = self.path.as_os_str().to_os_string();
            wal.push("-wal");
            paths.push(wal.into());
        }
        paths
            .iter()
            .map(|p| {
                std::fs::metadata(p)
                    .ok()
                    .map(|m| (m.len(), m.modified().ok()))
            })
            .collect()
    }
}

type Stamp = Vec<Option<(u64, Option<SystemTime>)>>;
type UsageResult = (Source, Option<String>);
type Channels = (Sender<Source>, Receiver<UsageResult>);
struct Cached {
    source: Source,
    stamp: Stamp,
    context: Option<String>,
}

fn collect(source: &Source, cache: &mut Vec<Cached>) -> Option<String> {
    let stamp = source.stamp();
    if let Some(hit) = cache
        .iter()
        .find(|c| c.source == *source && c.stamp == stamp)
    {
        return hit.context.clone();
    }
    let context = transcript::read(
        &source.path,
        source.backend,
        &ReadOptions {
            last: 1,
            detailed: false,
            session_id: source.session.clone(),
            allow_codex_retry: false,
        },
    )
    .ok()
    .and_then(|t| t.usage)
    .map(|u| usage_label(&u));
    cache.retain(|c| c.source != *source);
    if cache.len() >= 32 {
        cache.remove(0);
    }
    cache.push(Cached {
        source: source.clone(),
        stamp,
        context: context.clone(),
    });
    context
}

fn usage_label(usage: &transcript::SessionUsage) -> String {
    match transcript::format_context(usage) {
        Some(context) => format!("ctx:{context}"),
        None => transcript::format_usage(usage),
    }
}

#[derive(Default)]
pub(crate) struct UsageWorker {
    channels: Option<Channels>,
    pending: bool,
    checked: Option<Instant>,
    selected: Option<Source>,
    /// Last label per source, so switching back shows it while it refreshes.
    results: Vec<UsageResult>,
    /// Whether a result arrived for the selected source since it was selected.
    fresh: bool,
    /// Selected agent's status; a change triggers an immediate recheck.
    status: Option<AgentStatus>,
}

/// Usage only grows while the agent works; idle rechecks just catch late writes.
fn recheck_interval(status: Option<AgentStatus>) -> Duration {
    match status {
        Some(AgentStatus::Active) => Duration::from_secs(2),
        _ => Duration::from_secs(30),
    }
}

impl UsageWorker {
    pub fn poll(&mut self, agent: Option<&Agent>) -> bool {
        let selected = agent.and_then(Source::for_agent);
        if self.selected != selected {
            self.selected = selected;
            self.checked = None;
            self.fresh = false;
        }
        let status = agent.map(|a| a.status);
        if self.status != status {
            self.status = status;
            self.checked = None;
        }
        let mut changed = false;
        if let Some((_, rx)) = &self.channels {
            while let Ok((source, context)) = rx.try_recv() {
                self.pending = false;
                if !self.fresh && self.selected.as_ref() == Some(&source) {
                    self.fresh = true;
                    changed = true;
                }
                match self.results.iter_mut().find(|(s, _)| *s == source) {
                    Some((_, old)) => {
                        changed |= *old != context;
                        *old = context;
                    }
                    None => {
                        if self.results.len() >= 32 {
                            self.results.remove(0);
                        }
                        changed = true;
                        self.results.push((source, context));
                    }
                }
            }
        }
        if !self.pending
            && self
                .checked
                .is_none_or(|t| t.elapsed() >= recheck_interval(self.status))
            && let Some(source) = self.selected.clone()
        {
            let (tx, _) = self.channels.get_or_insert_with(|| {
                let (tx, requests) = mpsc::channel::<Source>();
                let (results, rx) = mpsc::channel();
                std::thread::spawn(move || {
                    let mut cache = Vec::new();
                    while let Ok(source) = requests.recv() {
                        let context = collect(&source, &mut cache);
                        if results.send((source, context)).is_err() {
                            break;
                        }
                    }
                });
                (tx, rx)
            });
            self.pending = tx.send(source).is_ok();
            self.checked = Some(Instant::now());
        }
        changed
    }

    #[cfg(test)]
    pub fn set_context_for_test(&mut self, agent: &Agent, context: &str) {
        self.results = vec![(
            Source::for_agent(agent).unwrap(),
            Some(format!("ctx:{context}")),
        )];
        self.selected = Source::for_agent(agent);
        self.fresh = true;
    }

    #[cfg(test)]
    pub fn mark_stale_for_test(&mut self) {
        self.fresh = false;
    }

    /// Last known label, and whether it was refreshed since the agent was selected.
    pub fn context(&self, agent: &Agent) -> Option<(String, bool)> {
        let source = Source::for_agent(agent)?;
        let fresh = self.fresh && self.selected.as_ref() == Some(&source);
        self.results
            .iter()
            .find(|(s, _)| *s == source)
            .and_then(|(_, context)| context.clone())
            .map(|context| (context, fresh))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_totals_are_shown_when_context_is_unavailable() {
        let mut usage = transcript::SessionUsage {
            tokens: transcript::TokenUsage {
                input: 70_000,
                output: 4_000,
                cache_read: 1_100_000,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(usage_label(&usage), "74k tok + 1.1M cached");
        usage.context_tokens = Some(50_000);
        assert_eq!(usage_label(&usage), "ctx:50k");
    }

    #[test]
    fn cache_invalidates_file_and_session_and_caches_missing_usage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        std::fs::write(&path, "").unwrap();
        let mut source = Source {
            path: path.clone(),
            session: Some("one".into()),
            backend: TranscriptBackend::ClaudeJsonl,
        };
        let mut cache = Vec::new();
        assert_eq!(collect(&source, &mut cache), None);
        assert_eq!(collect(&source, &mut cache), None);
        assert_eq!(cache.len(), 1);
        std::fs::write(&path, r#"{"type":"assistant","message":{"id":"a","content":[],"usage":{"input_tokens":100000,"output_tokens":1}}}"#).unwrap();
        assert_eq!(collect(&source, &mut cache).as_deref(), Some("ctx:100k"));
        assert_eq!(cache.len(), 1);
        source.session = Some("two".into());
        collect(&source, &mut cache);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn sqlite_wal_changes_invalidate_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let source = Source {
            path: dir.path().join("sessions.db"),
            session: Some("one".into()),
            backend: TranscriptBackend::OpenCodeSqlite,
        };
        std::fs::write(&source.path, "db").unwrap();
        let initial = source.stamp();
        std::fs::write(dir.path().join("sessions.db-wal"), "wal").unwrap();
        let wal = source.stamp();
        assert_ne!(initial, wal);
        std::fs::write(dir.path().join("sessions.db-wal"), "wal updated").unwrap();
        assert_ne!(wal, source.stamp());
    }

    #[test]
    fn switching_back_keeps_last_label_until_refreshed() {
        let dir = tempfile::tempdir().unwrap();
        let agent = |name: &str| {
            let path = dir.path().join(format!("{name}.jsonl"));
            std::fs::write(&path, r#"{"type":"assistant","message":{"id":"a","content":[],"usage":{"input_tokens":1000,"output_tokens":1}}}"#).unwrap();
            let mut a = crate::tui::test_helpers::make_test_agent(name, 0.0);
            a.tool = crate::tool::Tool::Claude.into();
            a.transcript_path = Some(path.to_string_lossy().into());
            a
        };
        let (a, b) = (agent("a"), agent("b"));
        let mut worker = UsageWorker::default();
        let wait = |worker: &mut UsageWorker, agent: &Agent| {
            for _ in 0..200 {
                worker.poll(Some(agent));
                if worker.context(agent).is_some_and(|(_, fresh)| fresh) && !worker.pending {
                    return;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            panic!("no usage for {}", agent.name);
        };
        wait(&mut worker, &a);
        wait(&mut worker, &b);
        worker.poll(Some(&a));
        assert_eq!(worker.context(&a), Some(("ctx:1k".into(), false)));
        wait(&mut worker, &a);
        assert_eq!(worker.context(&a), Some(("ctx:1k".into(), true)));
    }

    #[test]
    fn idle_agents_wait_and_status_changes_recheck() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.jsonl");
        std::fs::write(&path, "").unwrap();
        let mut agent = crate::tui::test_helpers::make_test_agent("a", 0.0);
        agent.tool = crate::tool::Tool::Claude.into();
        agent.transcript_path = Some(path.to_string_lossy().into());
        agent.status = AgentStatus::Listening;
        let mut worker = UsageWorker::default();
        worker.poll(Some(&agent));
        assert!(worker.pending);
        while worker.pending {
            worker.poll(Some(&agent));
            std::thread::sleep(Duration::from_millis(1));
        }
        worker.poll(Some(&agent));
        assert!(!worker.pending, "idle agent rechecked too soon");
        agent.status = AgentStatus::Active;
        worker.poll(Some(&agent));
        assert!(worker.pending, "status change should recheck at once");
    }

    #[test]
    fn remote_and_missing_paths_do_not_schedule_collection() {
        let mut agent = crate::tui::test_helpers::make_test_agent("test", 0.0);
        assert!(Source::for_agent(&agent).is_none());
        agent.transcript_path = Some("/remote/session.jsonl".into());
        agent.device_name = Some("remote".into());
        let mut worker = UsageWorker::default();
        assert!(!worker.poll(Some(&agent)));
        assert!(worker.channels.is_none());
        assert!(worker.context(&agent).is_none());
    }
}
