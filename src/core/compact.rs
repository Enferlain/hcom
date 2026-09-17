//! Typed compact classifier for generation-followed event streams.
//!
//! [`crate::core::progress`] defines the closed compact-record contract; this
//! module owns the projection from raw worker events into that contract:
//!
//! - lifecycle phases are deduplicated and every real phase change emits
//!   immediately, intentionally bypassing the cadence bound (lifecycle
//!   transitions are rare, high-signal, and must never be coalesced away);
//! - file and command observations are coalesced behind one cadence window
//!   per activity kind: the first observation in a window emits and
//!   equivalent activity inside the window is coalesced away, bounding noisy
//!   traces to at most one file and one command record per window;
//! - raw shell commands are reduced to a closed allowlist of
//!   [`CommandCategory`] names before anything can serialize;
//! - file detail must pass the [`CompactFilePath`] shape boundary before it
//!   can become a file record;
//! - optional quiet heartbeats fire at most once per heartbeat interval and
//!   carry the last known phase and activity time.
//!
//! ## Secret safety and the file-path trust boundary
//!
//! The classifier is the only constructor of compact records. Raw command
//! strings and file detail enter [`CompactClassifier::observe`], and only a
//! category name or a shape-validated file path leaves it. Message bodies,
//! transcript text, arguments, and environment values have no representable
//! field in the output contract, so they cannot serialize regardless of
//! input shape.
//!
//! File detail is worker-authored free-form text. [`CompactFilePath`] bounds
//! what can pass (single line, control-free, hidden-text-free, sized, and
//! path-shaped), but hcom cannot authenticate that an accepted string really
//! names a file: bounded text that merely looks path-like still flows
//! through verbatim. That is the unavoidable trust boundary — the shape
//! bounds keep records displayable and bounded; they do not make `path` text
//! safe to trust or execute.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::core::filters::{FILE_OP_CONTEXTS, SHELL_TOOL_CONTEXT_LIST};
use crate::core::progress::{CommandCategory, CompactRecord, LifecyclePhase, ProgressActivity};

/// Minimum spacing between compact records of the same activity kind.
///
/// Phase changes bypass this cadence and emit immediately. File and command
/// observations within one cadence window coalesce to the window's first
/// record, so a noisy trace produces at most one file and one command record
/// per cadence window regardless of event volume.
pub(crate) const COMPACT_ACTIVITY_CADENCE: Duration = Duration::from_secs(5);

/// Injectable monotonic time source for deterministic classifier timing.
///
/// The stream supplies wall time; tests advance a manual clock to prove that
/// cadence and heartbeat decisions depend only on injected time.
pub(crate) type CompactClock = Arc<dyn Fn() -> Instant>;

/// Maximum accepted file-path length in bytes.
///
/// A single displayable path does not need more; the bound keeps one record
/// small no matter what the worker wrote into the event detail.
pub(crate) const COMPACT_MAX_PATH_BYTES: usize = 512;

/// Unicode format and line-separator characters that rewrite display order,
/// hide content, or break lines.
///
/// Ordinary text (including normal Unicode paths) never contains these; a
/// "path" carrying them is an injection attempt, not a path.
const HIDDEN_TEXT_CHARS: &[char] = &[
    '\u{200B}', '\u{200C}', '\u{200D}', '\u{200E}', '\u{200F}', // zero-width, LRM, RLM
    '\u{202A}', '\u{202B}', '\u{202C}', '\u{202D}', '\u{202E}', // bidi embedding/override
    '\u{2060}', '\u{2066}', '\u{2067}', '\u{2068}', '\u{2069}', // word joiner, isolates
    '\u{FEFF}', // zero-width no-break space
    '\u{061C}', // Arabic letter mark
    '\u{2028}', '\u{2029}', // line/paragraph separator
];

/// A file path accepted for compact projection.
///
/// Constructed only through [`CompactFilePath::project`], which is the typed
/// boundary between worker-authored free-form event detail and the file
/// records of the model-safe compact output. Ordinary absolute and relative
/// POSIX and Windows paths, including Unicode and spaces, pass; blank,
/// free-form prose, control-bearing, multiline, hidden-text, and oversized
/// detail is rejected (projected to no record, fail closed).
///
/// This validates shape, not truth. The pathness heuristic accepts any
/// bounded single-line text containing a path separator or a dotted
/// extension, so a worker can still push crafted text (for example prose
/// containing a slash, or `not/a/real/path.txt`) through as a "path" — hcom
/// cannot authenticate that an accepted string names a real file, and the
/// text flows into the record verbatim. That residual is the trust boundary;
/// consumers must treat `path` as display text, never as a verified
/// location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactFilePath(String);

impl CompactFilePath {
    /// Validate free-form event detail as one displayable file path.
    ///
    /// Returns `None` when the detail cannot be treated as a path: it is
    /// blank, carries no path separator or dotted extension (pathness
    /// heuristic), contains ASCII control characters (newlines, tabs, NUL,
    /// ESC), contains hidden-text or line-separator characters such as bidi
    /// overrides, zero-width joins, or U+2028/U+2029, or exceeds
    /// [`COMPACT_MAX_PATH_BYTES`] bytes.
    pub(crate) fn project(detail: &str) -> Option<Self> {
        let trimmed = detail.trim();
        if trimmed.is_empty() || trimmed.len() > COMPACT_MAX_PATH_BYTES {
            return None;
        }
        // Pathness: tool detail for file contexts is a real path in
        // practice, and real paths carry a separator or an extension.
        if !trimmed.contains(['/', '\\', '.']) {
            return None;
        }
        if trimmed.bytes().any(|byte| byte < 0x20 || byte == 0x7F) {
            return None;
        }
        if HIDDEN_TEXT_CHARS
            .iter()
            .any(|hidden| trimmed.contains(*hidden))
        {
            return None;
        }
        Some(Self(trimmed.to_string()))
    }

    /// The validated path text.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Runtime configuration for the compact projection on one stream.
pub(crate) struct CompactStreamConfig {
    /// Optional quiet-heartbeat interval; `None` disables heartbeats.
    pub(crate) heartbeat: Option<Duration>,
    /// Time source used for cadence and heartbeat decisions.
    pub(crate) clock: CompactClock,
}

/// Last observed correlated activity, used to stamp quiet heartbeats.
struct ActivityStamp {
    ts: String,
    cursor: i64,
    at: Instant,
}

/// Stateful typed projection from one worker generation's events to bounded,
/// secret-safe compact records.
///
/// The classifier is fed only events a generation follow has already accepted
/// (see `WorkerGenerationFollow` in `src/commands/events.rs`); it never
/// re-checks instance identity and never sees events from other workers.
pub(crate) struct CompactClassifier {
    generation: String,
    heartbeat: Option<Duration>,
    clock: CompactClock,
    last_phase: Option<LifecyclePhase>,
    last_activity: Option<ActivityStamp>,
    last_file_emit: Option<Instant>,
    last_command_emit: Option<Instant>,
    last_heartbeat: Option<Instant>,
}

impl CompactClassifier {
    pub(crate) fn new(generation: String, config: CompactStreamConfig) -> Self {
        Self {
            generation,
            heartbeat: config.heartbeat,
            clock: config.clock,
            last_phase: None,
            last_activity: None,
            last_file_emit: None,
            last_command_emit: None,
            last_heartbeat: None,
        }
    }

    /// Seed heartbeat state from a live worker snapshot without emitting or
    /// replaying an event at or before the caller's exclusive cursor.
    ///
    /// A follower can attach to an already-running worker after its latest
    /// status event. Without this snapshot, the classifier has no phase or
    /// activity stamp and quiet heartbeats can never begin until another
    /// durable event happens. Unknown statuses remain unseeded rather than
    /// inventing a lifecycle phase.
    pub(crate) fn seed_live_status(&mut self, cursor: i64, ts: &str, status: &str) -> bool {
        if self.heartbeat.is_none() || self.last_activity.is_some() {
            return false;
        }
        let Some(phase) = phase_from_status(Some(status)) else {
            return false;
        };
        self.last_phase = Some(phase);
        self.last_activity = Some(ActivityStamp {
            ts: ts.to_string(),
            cursor,
            at: (self.clock)(),
        });
        true
    }

    /// Project one accepted generation event into zero or more compact
    /// records.
    ///
    /// At most one phase record (only when the phase changed) plus at most
    /// one file or command record (only when that kind's cadence window has
    /// elapsed) can result. Real phase changes intentionally bypass the
    /// cadence bound: lifecycle transitions are rare, high-signal, and must
    /// never be coalesced away. Every observed event — including messages,
    /// whose bodies must never serialize — refreshes the activity stamp that
    /// quiet heartbeats report.
    pub(crate) fn observe(&mut self, event: &Value) -> Vec<CompactRecord> {
        let Some(cursor) = event.get("id").and_then(Value::as_i64) else {
            return Vec::new();
        };
        let ts = event
            .get("ts")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let now = (self.clock)();
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let data = event.get("data").unwrap_or(&Value::Null);

        let mut records = Vec::new();

        // Phase changes emit immediately and deduplicate; a real change
        // bypasses the cadence bound by design.
        if let Some(phase) = observed_phase(event_type, data)
            && self.last_phase != Some(phase)
        {
            self.last_phase = Some(phase);
            records.push(self.record(cursor, &ts, ProgressActivity::Phase { phase }));
        }

        // Tool activity coalesces behind a per-kind cadence window; only the
        // raw detail is read here, and only a validated path or an allowlisted
        // category can leave. Path detail that fails validation projects to
        // no record without consuming the cadence window.
        if event_type == "status" {
            let context = data
                .get("context")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let detail = data
                .get("detail")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim();
            if FILE_OP_CONTEXTS.contains(&context) {
                if let Some(path) = CompactFilePath::project(detail)
                    && self.file_cadence_elapsed(now)
                {
                    self.last_file_emit = Some(now);
                    records.push(self.record(
                        cursor,
                        &ts,
                        ProgressActivity::File {
                            path: path.as_str().to_string(),
                        },
                    ));
                }
            } else if !detail.is_empty()
                && SHELL_TOOL_CONTEXT_LIST.contains(&context)
                && self.command_cadence_elapsed(now)
            {
                self.last_command_emit = Some(now);
                records.push(self.record(
                    cursor,
                    &ts,
                    ProgressActivity::Command {
                        category: classify_command(detail),
                    },
                ));
            }
        }

        // Any accepted generation event is activity for heartbeat purposes,
        // even when it cannot serialize (message bodies stay excluded).
        self.last_activity = Some(ActivityStamp {
            ts,
            cursor,
            at: now,
        });
        records
    }

    /// Emit one quiet heartbeat if the heartbeat interval is configured, a
    /// phase is known, no correlated activity occurred for the interval, and
    /// the previous heartbeat is at least one interval old.
    ///
    /// Heartbeats are rate-limited: a burst of polls can produce at most one
    /// heartbeat per interval.
    pub(crate) fn poll_heartbeat(&mut self) -> Option<CompactRecord> {
        let interval = self.heartbeat?;
        let phase = self.last_phase?;
        let stamp = self.last_activity.as_ref()?;
        let now = (self.clock)();
        if now.saturating_duration_since(stamp.at) < interval {
            return None;
        }
        if let Some(last) = self.last_heartbeat
            && now.saturating_duration_since(last) < interval
        {
            return None;
        }
        self.last_heartbeat = Some(now);
        Some(self.record(
            stamp.cursor,
            &stamp.ts,
            ProgressActivity::Heartbeat { phase },
        ))
    }

    fn file_cadence_elapsed(&self, now: Instant) -> bool {
        cadence_elapsed(self.last_file_emit, now)
    }

    fn command_cadence_elapsed(&self, now: Instant) -> bool {
        cadence_elapsed(self.last_command_emit, now)
    }

    fn record(&self, cursor: i64, ts: &str, activity: ProgressActivity) -> CompactRecord {
        CompactRecord::new(cursor, &self.generation, ts, activity, None::<&str>)
    }
}

/// Whether a full cadence window separates `last_emit` from `now`.
fn cadence_elapsed(last_emit: Option<Instant>, now: Instant) -> bool {
    match last_emit {
        Some(last) => now.saturating_duration_since(last) >= COMPACT_ACTIVITY_CADENCE,
        None => true,
    }
}

/// Map an event onto the closed lifecycle-phase vocabulary.
///
/// Unknown actions and statuses are skipped, not invented: `inactive`,
/// `error`, and statuses outside hcom's worker vocabulary carry no compact
/// phase, and launch failures keep their existing wait-side handling.
fn observed_phase(event_type: &str, data: &Value) -> Option<LifecyclePhase> {
    if event_type == "life" {
        let action = data
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default();
        return match action {
            "ready" => Some(LifecyclePhase::Ready),
            "stopped" => {
                // Placeholder stops are not lifecycle boundaries.
                if data.get("placeholder").and_then(Value::as_bool) == Some(true) {
                    None
                } else if data.get("soft").and_then(Value::as_bool) == Some(true) {
                    // Provider loop-end hooks can retain a live worker for the
                    // next turn. Report that nonterminal state without telling
                    // model consumers the generation has stopped.
                    Some(LifecyclePhase::Listening)
                } else {
                    Some(LifecyclePhase::Stopped)
                }
            }
            _ => phase_from_status(data.get("status").and_then(Value::as_str)),
        };
    }
    if event_type == "status" {
        return phase_from_status(data.get("status").and_then(Value::as_str));
    }
    None
}

/// Map a status value onto the closed phase vocabulary.
fn phase_from_status(status: Option<&str>) -> Option<LifecyclePhase> {
    match status? {
        "launching" => Some(LifecyclePhase::Launching),
        "active" => Some(LifecyclePhase::Active),
        "listening" => Some(LifecyclePhase::Listening),
        "blocked" => Some(LifecyclePhase::Blocked),
        _ => None,
    }
}

/// Reduce a raw shell command to one closed allowlist category.
///
/// Only recognized leading tokens earn a specific category; everything else —
/// including network clients, REPLs, and scripts that commonly carry
/// credentials or environment values — collapses to [`CommandCategory::Other`].
/// The raw text never leaves this function.
pub(crate) fn classify_command(raw: &str) -> CommandCategory {
    let mut tokens = raw.split_whitespace();
    let Some(first) = tokens.next() else {
        return CommandCategory::Other;
    };
    match first {
        "git" => CommandCategory::Git,
        "grep" | "rg" | "ag" | "ack" | "fd" | "find" | "locate" | "whereis" => {
            CommandCategory::Search
        }
        "make" | "cmake" | "meson" | "ninja" | "bazel" | "buck" | "colcon" => {
            CommandCategory::Build
        }
        "cargo" | "go" | "npm" | "pnpm" | "yarn" | "bun" | "deno" | "mvn" | "gradle" | "uv"
        | "poetry" => {
            // Skip runner wrapper words (`npm run build`) before matching.
            let mut sub = tokens.next().unwrap_or_default();
            if sub == "run" {
                sub = tokens.next().unwrap_or_default();
            }
            match sub {
                "test" | "tests" | "t" => CommandCategory::Test,
                "build" | "check" | "clippy" | "compile" => CommandCategory::Build,
                _ => CommandCategory::Other,
            }
        }
        "pytest" | "vitest" | "jest" | "mocha" | "karma" | "playwright" | "cypress" => {
            CommandCategory::Test
        }
        _ => CommandCategory::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    const GENERATION: &str = "kuma@1000.000000";

    /// Manual clock: tests advance time explicitly, so every cadence and
    /// heartbeat decision is deterministic.
    fn manual_clock() -> (Arc<Mutex<Instant>>, CompactClock) {
        let state = Arc::new(Mutex::new(Instant::now()));
        let clock_state = Arc::clone(&state);
        (state, Arc::new(move || *clock_state.lock().unwrap()))
    }

    fn classifier(heartbeat: Option<Duration>) -> (Arc<Mutex<Instant>>, CompactClassifier) {
        let (state, clock) = manual_clock();
        (
            state,
            CompactClassifier::new(
                GENERATION.to_string(),
                CompactStreamConfig { heartbeat, clock },
            ),
        )
    }

    fn advance(state: &Arc<Mutex<Instant>>, secs: u64) {
        *state.lock().unwrap() += Duration::from_secs(secs);
    }

    fn status_event(id: i64, status: &str, context: &str, detail: &str) -> Value {
        json!({
            "id": id,
            "ts": format!("2026-09-11T00:00:{id:02}Z"),
            "type": "status",
            "instance": "kuma",
            "data": {
                "status": status,
                "context": context,
                "detail": detail,
            },
        })
    }

    fn life_event(id: i64, action: &str) -> Value {
        json!({
            "id": id,
            "ts": format!("2026-09-11T00:00:{id:02}Z"),
            "type": "life",
            "instance": "kuma",
            "data": { "action": action },
        })
    }

    fn message_event(id: i64, text: &str) -> Value {
        json!({
            "id": id,
            "ts": format!("2026-09-11T00:00:{id:02}Z"),
            "type": "message",
            "instance": "kuma",
            "data": { "text": text },
        })
    }

    fn serialized(records: &[CompactRecord]) -> String {
        records
            .iter()
            .map(|record| serde_json::to_string(record).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn phases_deduplicate_and_changes_emit_immediately() {
        let (_, mut classifier) = classifier(None);

        // The first observation of a phase emits; equivalent repeats do not.
        let records = classifier.observe(&status_event(1, "active", "", ""));
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Active
            }
        );
        assert_eq!(records[0].cursor, 1);
        assert_eq!(records[0].ts, "2026-09-11T00:00:01Z");
        assert_eq!(records[0].generation, GENERATION);

        assert!(
            classifier
                .observe(&status_event(2, "active", "", ""))
                .is_empty(),
            "an equivalent phase must be deduplicated"
        );

        // Phase changes bypass every cadence bound and emit immediately.
        let records = classifier.observe(&status_event(3, "blocked", "", ""));
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Blocked
            }
        );
        let records = classifier.observe(&status_event(4, "active", "", ""));
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Active
            }
        );
    }

    #[test]
    fn lifecycle_actions_map_onto_the_closed_phase_vocabulary() {
        let (_, mut classifier) = classifier(None);

        let records = classifier.observe(&life_event(1, "ready"));
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Ready
            }
        );
        let records = classifier.observe(&life_event(2, "stopped"));
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Stopped
            }
        );

        // A soft stop ends only the provider's current execution loop; the
        // same worker remains available for another turn.
        let soft_stop = json!({
            "id": 3,
            "ts": "t3",
            "type": "life",
            "instance": "kuma",
            "data": {
                "action": "stopped",
                "soft": true,
                "snapshot": { "name": "kuma", "created_at": 1000.0 },
            },
        });
        let records = classifier.observe(&soft_stop);
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Listening
            }
        );

        // A placeholder stop is not a lifecycle boundary.
        let placeholder = json!({
            "id": 4,
            "ts": "t4",
            "type": "life",
            "instance": "kuma",
            "data": {
                "action": "stopped",
                "placeholder": true,
                "snapshot": { "name": "kuma", "created_at": 1000.0 },
            },
        });
        assert!(classifier.observe(&placeholder).is_empty());

        // Launch failures and unknown actions carry no compact phase; a
        // launch block surfaces through its blocked status instead.
        assert!(
            classifier
                .observe(&life_event(5, "launch_failed"))
                .is_empty()
        );
        let blocked = json!({
            "id": 6,
            "ts": "t6",
            "type": "life",
            "instance": "kuma",
            "data": { "action": "launch_blocked", "status": "blocked" },
        });
        let records = classifier.observe(&blocked);
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Blocked
            }
        );
    }

    #[test]
    fn unknown_statuses_carry_no_phase() {
        let (_, mut classifier) = classifier(None);
        for status in ["inactive", "error", "weird"] {
            assert!(
                classifier
                    .observe(&status_event(1, status, "", ""))
                    .is_empty(),
                "status '{status}' must not invent a phase"
            );
        }
    }

    #[test]
    fn file_and_command_bursts_coalesce_within_one_cadence_window() {
        let (state, mut classifier) = classifier(None);

        // A noisy burst of 300 mixed events at one frozen instant: the first
        // file and first command observation emit, everything equivalent
        // inside the window coalesces away, and only phase changes add
        // records. The output count is bounded by kind, not event volume.
        let mut records = Vec::new();
        for id in 1..=100 {
            records.extend(classifier.observe(&status_event(
                id,
                "active",
                "tool:Edit",
                &format!("src/file{id}.rs"),
            )));
        }
        for id in 101..=200 {
            records.extend(classifier.observe(&status_event(
                id,
                "active",
                "tool:Bash",
                &format!("cargo build --flag-{id}"),
            )));
        }
        for id in 201..=300 {
            records.extend(classifier.observe(&status_event(
                id,
                "active",
                "tool:Read",
                &format!("docs/note{id}.md"),
            )));
        }

        assert_eq!(
            records.len(),
            3,
            "a frozen-clock burst must bound output to one phase plus one record per activity kind"
        );
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Active
            }
        );
        assert_eq!(
            records[1].activity,
            ProgressActivity::File {
                path: "src/file1.rs".into()
            }
        );
        assert_eq!(
            records[2].activity,
            ProgressActivity::Command {
                category: CommandCategory::Build
            }
        );
        // Read-only file activity counts as file activity too.
        assert!(matches!(records[1].activity, ProgressActivity::File { .. }));

        // The cadence window only rolls over when injected time advances.
        advance(&state, COMPACT_ACTIVITY_CADENCE.as_secs());
        let records = classifier.observe(&status_event(301, "active", "tool:Edit", "src/next.rs"));
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].activity,
            ProgressActivity::File {
                path: "src/next.rs".into()
            },
            "a full cadence later the next file observation must emit"
        );
    }

    #[test]
    fn cadence_windows_are_independent_per_activity_kind() {
        let (_, mut classifier) = classifier(None);

        let file = classifier.observe(&status_event(1, "active", "tool:Edit", "src/a.rs"));
        assert_eq!(file.len(), 2, "first event carries the phase and the file");
        let command = classifier.observe(&status_event(2, "active", "tool:Bash", "git status"));
        assert_eq!(
            command.len(),
            1,
            "the command kind has its own independent cadence window"
        );
        assert_eq!(
            command[0].activity,
            ProgressActivity::Command {
                category: CommandCategory::Git
            }
        );
    }

    #[test]
    fn empty_tool_detail_does_not_serialize() {
        let (_, mut classifier) = classifier(None);
        // Seed the phase so only tool records are under test.
        classifier.observe(&status_event(1, "active", "", ""));
        assert!(
            classifier
                .observe(&status_event(2, "active", "tool:Bash", ""))
                .is_empty(),
            "a tool event without detail has no path or command to project"
        );
    }

    #[test]
    fn file_path_boundary_accepts_ordinary_cross_platform_and_unicode_paths() {
        let long_unicode = format!("a/{}.rs", "深".repeat(100));
        for path in [
            "src/lib.rs",
            "./deeply/nested/module/file.ts",
            "/home/imi/Projects/hcom/Cargo.toml",
            "C:\\Users\\imi\\notes.txt",
            "\\\\server\\share\\report.md",
            "docs/my spaced file.md",
            "src/表/日本語.rs",
            "src/emoji-🌌.rs",
            ".hidden",
            long_unicode.as_str(),
        ] {
            let projected = CompactFilePath::project(path)
                .unwrap_or_else(|| panic!("ordinary path must pass the boundary: {path:?}"));
            assert_eq!(projected.as_str(), path.trim());
        }
    }

    #[test]
    fn file_path_boundary_rejects_non_path_detail() {
        let oversized = format!("src/{}", "a".repeat(COMPACT_MAX_PATH_BYTES));
        let hostile = [
            "",                                                  // empty
            "   \t  ",                                           // whitespace only
            "README",                                            // no separator or extension
            "fixed the parser bug in worker.rs\nthen ran tests", // multiline
            "path\twith\ttabs",                                  // tab control characters
            "file.rs\x00.js",                                    // NUL
            "\x1b[31mredtext\x1b[0m",                            // ANSI escape
            "src/\u{202E}gdq.rs",                                // RTL override spoofing
            "in\u{200B}visible\u{200D}path.rs",                  // zero-width joins
            "clean\u{FEFF}bom.rs",                               // zero-width no-break space
            "mark\u{061C}.rs",                                   // Arabic letter mark
            "line\u{2028}sep.rs",                                // Unicode line separator
            "para\u{2029}graph.rs",                              // Unicode paragraph separator
            "the quick brown fox jumps over the lazy dog",       // free-form prose
            "password=hunter2 token sk-live-abc",                // secret-bearing prose
            oversized.as_str(),                                  // over the byte bound
        ];
        for detail in hostile {
            assert!(
                CompactFilePath::project(detail).is_none(),
                "non-path detail must fail closed: {detail:?}"
            );
        }
    }

    #[test]
    fn file_path_boundary_bounds_shape_but_cannot_authenticate_pathness() {
        // The precise trust boundary: bounded single-line text that looks
        // path-shaped passes through verbatim even when a human can tell it
        // is not a real path. Consumers must treat `path` as display text.
        let crafted = "see/the/spec v1.2 — not a real file, token=hunter2";
        let projected =
            CompactFilePath::project(crafted).expect("path-shaped bounded text passes shape");
        assert_eq!(projected.as_str(), crafted);

        // At classifier level the same text becomes a bounded file record.
        let (_, mut classifier) = classifier(None);
        let records = classifier.observe(&status_event(1, "active", "tool:Write", crafted));
        assert_eq!(records.len(), 2, "phase + the bounded file record");
        assert_eq!(
            records[1].activity,
            ProgressActivity::File {
                path: crafted.to_string()
            }
        );
    }

    #[test]
    fn hostile_file_detail_projects_to_no_record_but_still_counts_as_activity() {
        let (state, mut classifier) = classifier(Some(Duration::from_secs(10)));

        // Seed the phase, then feed file-context events whose detail is
        // control-bearing, oversized, hidden-text, or free-form prose.
        classifier.observe(&status_event(1, "active", "", ""));
        let oversized = format!("long/{}", "x".repeat(COMPACT_MAX_PATH_BYTES));
        let hostile = [
            "src/lib.rs\n--no-preserve-root",
            "\x1b[2J\x1b[Hscreen-clear",
            oversized.as_str(),
            "src/\u{202E}spoof.rs",
            "password=hunter2 path with secrets as prose",
        ];
        for (id, detail) in hostile.iter().enumerate() {
            let id = (id + 2) as i64;
            assert!(
                classifier
                    .observe(&status_event(id, "active", "tool:Write", detail))
                    .is_empty(),
                "hostile file detail must not produce a record: {detail:?}"
            );
        }

        // Rejected detail still marks activity and keeps heartbeats honest.
        let last_id = hostile.len() as i64 + 1;
        advance(&state, 20);
        let heartbeat = classifier
            .poll_heartbeat()
            .expect("quiet past the interval");
        assert_eq!(heartbeat.cursor, last_id);
        assert_eq!(
            heartbeat.activity,
            ProgressActivity::Heartbeat {
                phase: LifecyclePhase::Active
            }
        );
    }

    #[test]
    fn command_allowlist_maps_known_runners_and_collapses_everything_else() {
        let cases = [
            ("git status", CommandCategory::Git),
            ("git push origin main", CommandCategory::Git),
            ("cargo test", CommandCategory::Test),
            ("cargo test --all", CommandCategory::Test),
            ("cargo build", CommandCategory::Build),
            ("cargo check --locked", CommandCategory::Build),
            ("cargo clippy -- -D warnings", CommandCategory::Build),
            ("cargo run", CommandCategory::Other),
            ("go test ./...", CommandCategory::Test),
            ("go build ./...", CommandCategory::Build),
            ("npm test", CommandCategory::Test),
            ("npm run build", CommandCategory::Build),
            ("npm install", CommandCategory::Other),
            ("pytest -q", CommandCategory::Test),
            ("vitest run", CommandCategory::Test),
            ("make", CommandCategory::Build),
            ("make test", CommandCategory::Build),
            ("rg TODO src/", CommandCategory::Search),
            ("grep -r secret .", CommandCategory::Search),
            ("find . -name '*.rs'", CommandCategory::Search),
            (
                "curl -H 'Authorization: Bearer tok' https://internal.example",
                CommandCategory::Other,
            ),
            (
                "AWS_SECRET_ACCESS_KEY=x ./deploy.sh",
                CommandCategory::Other,
            ),
            ("./scripts/release.sh --env prod", CommandCategory::Other),
            ("", CommandCategory::Other),
            ("   ", CommandCategory::Other),
        ];
        for (raw, expected) in cases {
            assert_eq!(classify_command(raw), expected, "command: {raw:?}");
        }
    }

    #[test]
    fn raw_command_argument_and_environment_text_never_serialize() {
        let (state, mut classifier) = classifier(None);
        let secret = "Authorization: Bearer SECRET_TOKEN";
        let env_secret = "AWS_SECRET_ACCESS_KEY=abcd1234";

        let records = classifier.observe(&status_event(
            1,
            "active",
            "tool:Bash",
            &format!("curl -H '{secret}' https://internal.example"),
        ));
        // Roll the command cadence over so the second raw command also has to
        // pass through the category projection on its own.
        advance(&state, COMPACT_ACTIVITY_CADENCE.as_secs());
        let records = [
            records,
            classifier.observe(&status_event(
                2,
                "active",
                "tool:shell",
                &format!("{env_secret} ./deploy.sh --password hunter2"),
            )),
        ]
        .concat();

        let text = serialized(&records);
        assert!(
            !text.contains("SECRET_TOKEN"),
            "raw command arguments must not appear in compact records: {text}"
        );
        assert!(
            !text.contains("AWS_SECRET_ACCESS_KEY"),
            "environment values must not appear in compact records: {text}"
        );
        assert!(
            !text.contains("hunter2") && !text.contains("deploy.sh"),
            "raw command text must not appear in compact records: {text}"
        );
        assert_eq!(records.len(), 3, "phase + two coalesced command records");
        for record in &records {
            // Only a name from the closed category allowlist may serialize.
            if let ProgressActivity::Command { category } = record.activity {
                let encoded = serde_json::to_value(category).unwrap();
                assert!(
                    ["build", "test", "git", "search", "other"]
                        .contains(&encoded.as_str().unwrap()),
                    "command payload must be an allowlisted category: {encoded}"
                );
            }
        }
    }

    #[test]
    fn message_and_unknown_events_never_serialize_but_count_as_activity() {
        let (state, mut classifier) = classifier(Some(Duration::from_secs(10)));

        // Message bodies and transcript-style payloads are unrepresentable.
        assert!(
            classifier
                .observe(&message_event(1, "the password is hunter2"))
                .is_empty()
        );
        let transcript = json!({
            "id": 2,
            "ts": "t2",
            "type": "transcript",
            "instance": "kuma",
            "data": { "text": "API_KEY=sk-live-abcdef", "output": "confidential" },
        });
        assert!(classifier.observe(&transcript).is_empty());

        // With no known phase there is nothing honest for a heartbeat to say.
        advance(&state, 60);
        assert!(
            classifier.poll_heartbeat().is_none(),
            "heartbeats must wait for a known phase"
        );

        // Once a phase is known, the message activity still delays heartbeats.
        let records = classifier.observe(&status_event(3, "active", "", ""));
        assert_eq!(records.len(), 1);
        assert!(classifier.poll_heartbeat().is_none());
        advance(&state, 5);
        assert!(
            classifier
                .observe(&message_event(4, "still working, password hunter2"))
                .is_empty()
        );
        advance(&state, 5);
        assert!(
            classifier.poll_heartbeat().is_none(),
            "a message is correlated activity and must reset the quiet interval"
        );
        advance(&state, 5);
        let heartbeat = classifier.poll_heartbeat().expect("quiet for 10s");
        assert_eq!(
            heartbeat.activity,
            ProgressActivity::Heartbeat {
                phase: LifecyclePhase::Active
            }
        );
        assert_eq!(
            heartbeat.cursor, 4,
            "heartbeats carry the last activity cursor"
        );
        assert_eq!(heartbeat.ts, "2026-09-11T00:00:04Z");
    }

    #[test]
    fn heartbeats_wait_for_the_quiet_interval_and_are_rate_limited() {
        let (state, mut classifier) = classifier(Some(Duration::from_secs(10)));

        classifier.observe(&status_event(1, "active", "", ""));
        assert!(
            classifier.poll_heartbeat().is_none(),
            "no heartbeat before the quiet interval elapses"
        );

        advance(&state, 10);
        let heartbeat = classifier.poll_heartbeat().expect("quiet for 10s");
        assert_eq!(
            heartbeat.activity,
            ProgressActivity::Heartbeat {
                phase: LifecyclePhase::Active
            }
        );
        assert_eq!(heartbeat.cursor, 1);
        assert_eq!(heartbeat.ts, "2026-09-11T00:00:01Z");

        // Rate limit: repeated polls without further elapsed time stay quiet.
        for _ in 0..10 {
            assert!(
                classifier.poll_heartbeat().is_none(),
                "heartbeats are rate-limited to one per interval"
            );
        }

        advance(&state, 10);
        assert!(
            classifier.poll_heartbeat().is_some(),
            "the next heartbeat fires one full interval later"
        );
    }

    #[test]
    fn live_status_seed_is_heartbeat_only_and_preserves_future_phase_output() {
        let (_state, mut without_heartbeat) = classifier(None);
        assert!(!without_heartbeat.seed_live_status(7, "2026-09-11T00:00:07Z", "active"));
        let records = without_heartbeat.observe(&status_event(8, "active", "", ""));
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Active
            },
            "seeding must not suppress phase output when heartbeats are disabled"
        );

        let (_state, mut unknown) = classifier(Some(Duration::from_secs(10)));
        assert!(!unknown.seed_live_status(7, "2026-09-11T00:00:07Z", "unknown"));
        assert!(unknown.poll_heartbeat().is_none());
    }

    #[test]
    fn heartbeats_report_the_last_known_phase_and_stop_at_the_boundary() {
        let (state, mut classifier) = classifier(Some(Duration::from_secs(10)));

        classifier.observe(&status_event(1, "blocked", "", ""));
        advance(&state, 10);
        let heartbeat = classifier.poll_heartbeat().expect("quiet for 10s");
        assert_eq!(
            heartbeat.activity,
            ProgressActivity::Heartbeat {
                phase: LifecyclePhase::Blocked
            },
            "a heartbeat carries the last known phase, not a live guess"
        );

        // The stop boundary is a phase change, so it emits immediately even
        // though the file/command cadence window has not rolled over.
        let records = classifier.observe(&life_event(2, "stopped"));
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].activity,
            ProgressActivity::Phase {
                phase: LifecyclePhase::Stopped
            }
        );
    }

    #[test]
    fn heartbeats_are_absent_when_not_configured() {
        let (state, mut classifier) = classifier(None);
        classifier.observe(&status_event(1, "active", "", ""));
        advance(&state, 3600);
        assert!(
            classifier.poll_heartbeat().is_none(),
            "heartbeats are optional and disabled by default"
        );
    }

    #[test]
    fn every_record_carries_the_correlation_envelope() {
        let (state, mut classifier) = classifier(Some(Duration::from_secs(1)));

        let records = classifier.observe(&status_event(1, "active", "tool:Edit", "src/a.rs"));
        assert_eq!(records.len(), 2);
        for record in &records {
            let value = serde_json::to_value(record).unwrap();
            assert_eq!(
                value["schema_version"],
                crate::core::progress::PROGRESS_SCHEMA_VERSION
            );
            assert_eq!(value["generation"], GENERATION);
            assert!(value["cursor"].as_i64().is_some());
            assert!(value["ts"].as_str().is_some());
            assert!(value.get("thread").is_none());
        }

        advance(&state, 1);
        let heartbeat = classifier.poll_heartbeat().expect("quiet for 1s");
        let value = serde_json::to_value(&heartbeat).unwrap();
        assert_eq!(value["generation"], GENERATION);
        assert_eq!(value["cursor"], 1);
    }
}
