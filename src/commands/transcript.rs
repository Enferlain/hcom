//! `hcom transcript` command — view and search agent conversation transcripts.
//!
//!
//! Supports:
//! - View transcript: `hcom transcript @instance [N | N-M] [--full] [--detailed] [--json] [--last N]`
//! - Timeline: `hcom transcript timeline [--last N] [--full] [--json]`
//! - Search: `hcom transcript search "pattern" [--live] [--all] [--limit N] [--agent TYPE]`

use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::db::HcomDb;
use crate::shared::CommandContext;
use crate::tool::Tool;
use crate::transcript::{self, Exchange, ReadOptions, format_exchanges, summarize_action};

fn run_search_tool(program: &str, args: &[&str]) -> Result<Option<std::process::Output>, String> {
    match std::process::Command::new(program).args(args).output() {
        Ok(output) if output.status.success() => Ok(Some(output)),
        Ok(output) if output.status.code() == Some(1) => Ok(None),
        Ok(output) => {
            let detail = String::from_utf8_lossy(&output.stderr);
            Err(format!(
                "{program} failed{}",
                if detail.trim().is_empty() {
                    format!(" with {}", output.status)
                } else {
                    format!(": {}", detail.trim())
                }
            ))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(format!(
            "required search tool `{program}` was not found on PATH"
        )),
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            // A bare name that only hit EACCES on an inaccessible PATH
            // directory (common on WSL with /mnt/c entries) is still "not
            // found"; an explicit path, or a real candidate we cannot
            // execute, keeps the permission error.
            let is_bare_name = Path::new(program)
                .parent()
                .is_none_or(|p| p.as_os_str().is_empty());
            if !is_bare_name
                || search_tool_candidate_on_path(program, std::env::var_os("PATH").as_deref())
            {
                Err(format!("could not run `{program}`: {err}"))
            } else {
                Err(format!(
                    "required search tool `{program}` was not found on PATH"
                ))
            }
        }
        Err(err) => Err(format!("could not run `{program}`: {err}")),
    }
}

/// Whether a PATH entry holds a file that could be `program`, so a spawn
/// PermissionDenied came from a real candidate rather than an inaccessible
/// PATH directory alone. Candidate enumeration matches `which` (PATHEXT on
/// Windows) via [`crate::terminal::which_candidates`].
fn search_tool_candidate_on_path(program: &str, path_env: Option<&std::ffi::OsStr>) -> bool {
    let Some(path_env) = path_env else {
        // execvp falls back to the default confstr PATH when PATH is unset.
        #[cfg(unix)]
        return ["/usr/bin", "/bin"]
            .iter()
            .any(|dir| is_candidate_file(&Path::new(dir).join(program)));
        #[cfg(not(unix))]
        return false;
    };
    std::env::split_paths(path_env).any(|dir| {
        // An empty PATH entry denotes the current directory (execvp semantics).
        let dir = if dir.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            dir
        };
        crate::terminal::which_candidates(&dir, program)
            .iter()
            .any(|candidate| is_candidate_file(candidate))
    })
}

/// A candidate counts only if it is a regular file, or a symlink whose target
/// cannot be resolved for lack of permission — both fail to exec with EACCES,
/// so the spawn error must be surfaced rather than downgraded to "not found".
/// Dangling symlinks (ENOENT) do not count; exec fails NotFound for them.
fn is_candidate_file(path: &Path) -> bool {
    match std::fs::metadata(path) {
        Ok(meta) => meta.is_file(),
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => path.is_symlink(),
        Err(_) => false,
    }
}

/// Parsed arguments for `hcom transcript`.
#[derive(clap::Parser, Debug)]
#[command(name = "transcript", about = "View and search transcripts")]
pub struct TranscriptArgs {
    /// Subcommand (search, timeline) or view mode
    #[command(subcommand)]
    pub subcmd: Option<TranscriptSubcmd>,

    /// Target instance name (with or without @)
    pub name: Option<String>,
    /// Exchange range (e.g., "5" or "5-10")
    pub range_positional: Option<String>,

    /// JSON output
    #[arg(long)]
    pub json: bool,
    /// Full output (no streamlining)
    #[arg(long)]
    pub full: bool,
    /// Show tool inputs/outputs, file edits, and errors
    #[arg(long)]
    pub detailed: bool,
    /// Last N exchanges (--tail is an accepted alias)
    #[arg(long, visible_alias = "tail")]
    pub last: Option<usize>,
    /// Exchange range (flag form)
    #[arg(long = "range")]
    pub range_flag: Option<String>,
}

#[derive(clap::Subcommand, Debug)]
pub enum TranscriptSubcmd {
    /// Search transcripts for a pattern
    Search(TranscriptSearchArgs),
    /// Show timeline of all agents' recent activity
    Timeline(TranscriptTimelineArgs),
}

/// Args for `hcom transcript search`.
#[derive(clap::Args, Debug)]
pub struct TranscriptSearchArgs {
    /// Search pattern (regex)
    pub pattern: String,
    /// Live-watch mode
    #[arg(long)]
    pub live: bool,
    /// Search all transcripts on disk (not just tracked instances)
    #[arg(long)]
    pub all: bool,
    /// JSON output
    #[arg(long)]
    pub json: bool,
    /// Exclude own transcript from search results
    #[arg(long)]
    pub exclude_self: bool,
    /// Max results (default: 20)
    #[arg(long, default_value = "20")]
    pub limit: usize,
    /// Filter by exact agent type (canonical name or declared alias)
    #[arg(long)]
    pub agent: Option<String>,
}

/// Args for `hcom transcript timeline`.
#[derive(clap::Args, Debug)]
pub struct TranscriptTimelineArgs {
    /// JSON output
    #[arg(long)]
    pub json: bool,
    /// Full output
    #[arg(long)]
    pub full: bool,
    /// Detailed output
    #[arg(long)]
    pub detailed: bool,
    /// Last N exchanges per agent (--tail is an accepted alias)
    #[arg(long, visible_alias = "tail")]
    pub last: Option<usize>,
}

/// Truncate a string to at most `max` bytes at a valid UTF-8 char boundary.
fn truncate_str(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Snippet width (bytes) for transcript search matches, centered on the hit.
const SEARCH_SNIPPET_WIDTH: usize = 160;

/// Build a snippet of ~`width` bytes centered on a match.
///
/// Transcript lines are whole JSON message objects (thousands of bytes), so a
/// start-anchored truncation only ever shows leading metadata (`parentUuid`…),
/// never the match. `col` is ripgrep's 1-based byte column of the match start;
/// we window around it so the matched text is actually visible, with `…`
/// markers when content is elided on either side. All slice points are snapped
/// to UTF-8 char boundaries.
fn centered_snippet(line_text: &str, col: usize, width: usize) -> String {
    if line_text.len() <= width {
        return line_text.to_string();
    }
    let match_start = col.saturating_sub(1).min(line_text.len());
    let half = width / 2;
    let mut start = match_start.saturating_sub(half);
    while start > 0 && !line_text.is_char_boundary(start) {
        start -= 1;
    }
    let mut end = (start + width).min(line_text.len());
    while end < line_text.len() && !line_text.is_char_boundary(end) {
        end += 1;
    }
    let mut out = String::new();
    if start > 0 {
        out.push('…');
    }
    out.push_str(line_text[start..end].trim());
    if end < line_text.len() {
        out.push('…');
    }
    out
}

/// Parse one search-tool output line into `(line_number, centered_snippet)`.
///
/// `has_column` is true for ripgrep run with `--column` (`LINE:COL:TEXT`) and
/// false for the `grep` fallback (`LINE:TEXT`), where we locate the pattern
/// literally to center on it and fall back to the line start otherwise.
fn parse_match_line(raw: &str, has_column: bool, pattern: &str) -> (usize, String) {
    let Some((line_str, rest)) = raw.split_once(':') else {
        return (0, centered_snippet(raw, 1, SEARCH_SNIPPET_WIDTH));
    };
    let line_num = line_str.parse::<usize>().unwrap_or(0);

    if has_column
        && let Some((col_str, text)) = rest.split_once(':')
        && let Ok(col) = col_str.parse::<usize>()
    {
        return (line_num, centered_snippet(text, col, SEARCH_SNIPPET_WIDTH));
    }

    // grep fallback (or malformed rg line): best-effort literal locate.
    let col = rest
        .to_lowercase()
        .find(&pattern.to_lowercase())
        .map(|byte| byte + 1)
        .unwrap_or(1);
    (line_num, centered_snippet(rest, col, SEARCH_SNIPPET_WIDTH))
}

// ── Transcript Path Discovery ────────────────────────────────────────────

/// Detect canonical agent type from transcript path.
pub(crate) fn detect_agent_type(path: &str) -> &'static str {
    transcript::agent_name_from_path(path)
}

fn transcript_search_key(path: &str, session_id: Option<&str>) -> String {
    format!("{path}\u{0}{}", session_id.unwrap_or(""))
}

/// Attribute a `--all` disk match to a canonical tool.
///
/// Content detection wins when it lands on a selected tool: it resolves every
/// signatured format and the one shared root (gemini/antigravity under
/// `~/.gemini`). Otherwise the file is attributed by provenance — the search
/// root it was found under — which is what classifies unsignatured sessions such
/// as pi's bare `<uuid>.jsonl` reached via a custom `PI_CODING_AGENT_SESSION_DIR`.
/// Provenance is only trusted when exactly one selected root owns the path, so
/// the shared gemini/antigravity root never guesses.
fn attribute_disk_match(
    file_path: &str,
    selected: &[Tool],
    root_owners: &[(PathBuf, Tool)],
) -> Option<Tool> {
    if let Some(detected) = transcript::detect_tool_from_path(file_path)
        && selected.contains(&detected)
    {
        return Some(detected);
    }
    let path = Path::new(file_path);
    let mut owner: Option<Tool> = None;
    for (root, tool) in root_owners {
        if selected.contains(tool) && path.starts_with(root) {
            match owner {
                None => owner = Some(*tool),
                Some(existing) if existing == *tool => {}
                Some(_) => return None, // ambiguous provenance — do not guess
            }
        }
    }
    owner
}

/// Build an appropriate error message when transcript resolution fails.
/// Uses resolve_display_name_or_stopped (which handles exact base and tag-name
/// resolution) to check if the instance exists without a transcript.
fn no_transcript_error(
    db: &HcomDb,
    name: &str,
    display_name: &str,
    device: Option<&str>,
) -> String {
    if let Some(resolved) = crate::identity::resolve_display_name_or_stopped(db, name) {
        let display_name = if display_name.is_empty() {
            &resolved
        } else {
            display_name
        };
        let command = match device {
            Some(device) => format!(
                "hcom events --remote-fetch --device {device} --participant {resolved} --type message"
            ),
            None => format!("hcom events --participant {resolved} --type message"),
        };
        format!(
            "No model transcript is registered for {display_name}.\nView transport messages with: {command}"
        )
    } else {
        format!("Agent '{display_name}' not found")
    }
}

/// Find a device-suffixed instance name ("dami:KEZE") matching a bare base
/// name ("dami") that has no exact/tag/stopped match of its own. Relay pull
/// always namespaces remote instances with a device suffix (see
/// `relay::add_device_suffix`), so a bare name typed by the user never
/// matches those rows directly — only this prefix lookup does. Without it,
/// callers fall through to a plain-name DB lookup, which still succeeds via
/// a looser `LIKE` prefix match and returns the remote device's transcript
/// path as if it were a local file.
fn resolve_remote_instance_name(db: &HcomDb, base_name: &str) -> Option<String> {
    db.conn()
        .query_row(
            "SELECT name FROM instances WHERE name LIKE ?1 ESCAPE '\\' ORDER BY status_time DESC LIMIT 1",
            rusqlite::params![format!(
                "{}:____",
                base_name.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
            )],
            |row| row.get::<_, String>(0),
        )
        .ok()
}

/// Get exchanges from a transcript file using the shared transcript module.
fn get_exchanges(
    path: &str,
    agent: &str,
    last: usize,
    detailed: bool,
    session_id: Option<&str>,
    retry_codex: bool,
) -> Result<Vec<Exchange>, String> {
    let backend = transcript::backend_from_agent_or_path(agent, path)?;
    let opts = ReadOptions {
        last,
        detailed,
        session_id: session_id.map(|s| s.to_string()),
        allow_codex_retry: retry_codex,
    };
    transcript::read(Path::new(path), backend, &opts)
}

// ── Search ───────────────────────────────────────────────────────────────

/// Correlate transcript file paths to hcom agent names via DB queries.
/// Checks instances table first, then stopped life events.
fn correlate_paths_to_hcom(
    db: &HcomDb,
    targets: &[(String, Option<String>)],
) -> std::collections::HashMap<String, String> {
    let mut result = std::collections::HashMap::new();
    let conn = db.conn();
    let target_keys: std::collections::HashSet<String> = targets
        .iter()
        .map(|(path, session_id)| transcript_search_key(path, session_id.as_deref()))
        .collect();

    // 1. Check current instances
    if let Ok(mut stmt) = conn.prepare(
        "SELECT name, transcript_path, session_id
         FROM instances
         WHERE transcript_path IS NOT NULL",
    ) && let Ok(rows) = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    }) {
        for (name, tp, session_id) in rows.flatten() {
            let key = transcript_search_key(&tp, session_id.as_deref());
            if target_keys.contains(&key) {
                result.insert(key, name);
            }
        }
    }

    // 2. Check stopped events for paths not yet matched
    if let Ok(mut stmt) = conn.prepare(
        "SELECT instance,
                json_extract(data, '$.snapshot.transcript_path') as tp,
                json_extract(data, '$.snapshot.session_id') as session_id \
         FROM events WHERE type = 'life' \
         AND json_extract(data, '$.action') = 'stopped' \
         AND json_extract(data, '$.snapshot.transcript_path') IS NOT NULL \
         ORDER BY id DESC",
    ) && let Ok(rows) = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    }) {
        for (name, tp, session_id) in rows.flatten() {
            let key = transcript_search_key(&tp, session_id.as_deref());
            if target_keys.contains(&key) && !result.contains_key(&key) {
                result.insert(key, name);
            }
        }
    }

    result
}

/// Collect transcript search results across files and databases.
pub(crate) fn collect_transcript_search_results(
    db: &HcomDb,
    args: &TranscriptSearchArgs,
    ctx: Option<&CommandContext>,
) -> Result<Vec<Value>, String> {
    let live_mode = args.live;
    let all_mode = args.all;
    let limit = args.limit;
    let agent_filter = match args.agent.as_deref() {
        Some(value) => Some(transcript::parse_tool_filter(value)?),
        None => None,
    };

    // Resolve self name for --exclude-self
    let ctx_name = if args.exclude_self {
        ctx.and_then(|c| c.identity.as_ref())
            .filter(|id| matches!(id.kind, crate::shared::SenderKind::Instance))
            .map(|id| id.name.clone())
    } else {
        None
    };

    let pattern = &args.pattern;

    // Collect transcript paths: (name, path, agent, session_id)
    let mut paths: Vec<(String, String, String, Option<String>)> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    if all_mode {
        // --all: derive file roots and database sources from canonical tools.
        let selected_tools = agent_filter
            .map(|tool| vec![tool])
            .unwrap_or_else(transcript::transcript_tools);
        let mut search_dirs: Vec<PathBuf> = Vec::new();
        // Remember which tool each search root belongs to so matches found under
        // an override root with no content signature (e.g. pi's bare
        // `<uuid>.jsonl` under a custom PI_CODING_AGENT_SESSION_DIR) can still be
        // attributed. A path can map to more than one tool — gemini and
        // antigravity share `~/.gemini` — which `attribute_disk_match` treats as
        // ambiguous and defers to content detection.
        let mut root_owners: Vec<(PathBuf, Tool)> = Vec::new();
        for tool in &selected_tools {
            for path in transcript::disk_search_roots(*tool) {
                if path.exists() {
                    if !search_dirs.contains(&path) {
                        search_dirs.push(path.clone());
                    }
                    root_owners.push((path, *tool));
                }
            }
        }
        let mut database_sources: Vec<(Tool, PathBuf)> = selected_tools
            .iter()
            .filter_map(|tool| transcript::database_search_path(*tool).map(|path| (*tool, path)))
            .collect();

        // Also search distinct profile databases of tracked Hermes instances
        // so profiles outside the caller's ambient HERMES_HOME are included and correlated.
        if selected_tools.contains(&Tool::Hermes)
            && let Ok(mut stmt) = db.conn().prepare(
                "SELECT DISTINCT transcript_path FROM instances WHERE lower(tool) = 'hermes' AND transcript_path IS NOT NULL AND transcript_path != '' \
                 UNION \
                 SELECT DISTINCT json_extract(data, '$.snapshot.transcript_path') \
                 FROM events WHERE type = 'life' AND json_extract(data, '$.action') = 'stopped' \
                 AND lower(json_extract(data, '$.snapshot.tool')) = 'hermes' \
                 AND json_extract(data, '$.snapshot.transcript_path') IS NOT NULL AND json_extract(data, '$.snapshot.transcript_path') != ''",
            )
            && let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0))
        {
            for path_str in rows.flatten() {
                let p = PathBuf::from(path_str);
                let canon_p = std::fs::canonicalize(&p).unwrap_or_else(|_| p.clone());
                if p.exists()
                    && !database_sources.iter().any(|(_, existing)| {
                        existing == &p
                            || std::fs::canonicalize(existing).unwrap_or_else(|_| existing.clone())
                                == canon_p
                    })
                {
                    database_sources.push((Tool::Hermes, p));
                }
            }
        }

        if search_dirs.is_empty() && database_sources.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 1: find matching files with rg -l (recursive, *.jsonl/*.json).
        // Avoid invoking rg without a path when only SQLite sources exist; that
        // would make it read stdin and potentially block an interactive command.
        let matching_files: Vec<String> = if search_dirs.is_empty() {
            Vec::new()
        } else {
            let mut cmd = std::process::Command::new("rg");
            cmd.args(["-l", "--glob", "*.jsonl", "--glob", "*.json", pattern]);
            for d in &search_dirs {
                cmd.arg(d);
            }
            match cmd.output() {
                Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout)
                    .lines()
                    .filter(|line| !line.is_empty())
                    .map(str::to_string)
                    .collect(),
                Ok(out) if out.status.code() == Some(1) => Vec::new(),
                Ok(out) => {
                    return Err(format!(
                        "ripgrep failed: {}",
                        String::from_utf8_lossy(&out.stderr).trim()
                    ));
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Err(
                        "transcript search --all requires `rg` (ripgrep) on PATH".to_string()
                    );
                }
                Err(err) => {
                    return Err(format!("could not run `rg`: {err}"));
                }
            }
        };

        let mut database_matches = Vec::new();
        for (tool, db_path) in &database_sources {
            if database_matches.len() >= limit {
                break;
            }
            match transcript::search_database_sessions(
                *tool,
                db_path,
                pattern,
                limit - database_matches.len(),
            ) {
                Ok(matches) => database_matches.extend(matches),
                Err(err) => {
                    return Err(err);
                }
            }
        }

        if matching_files.is_empty() && database_matches.is_empty() {
            return Ok(Vec::new());
        }

        // Correlate transcript paths/session IDs to hcom names via DB.
        let mut targets: Vec<(String, Option<String>)> = matching_files
            .iter()
            .cloned()
            .map(|path| (path, None))
            .collect();
        targets.extend(
            database_matches
                .iter()
                .filter_map(|m| m.session_id.clone().map(|sid| (m.path.clone(), Some(sid)))),
        );
        let path_to_hcom = correlate_paths_to_hcom(db, &targets);

        // Extract line-level matches from each file
        let mut results = Vec::new();
        for file_path in &matching_files {
            if results.len() >= limit {
                break;
            }
            let Some(detected_tool) =
                attribute_disk_match(file_path, &selected_tools, &root_owners)
            else {
                continue;
            };
            let agent = detected_tool.as_str();
            let hcom_name = path_to_hcom
                .get(&transcript_search_key(file_path, None))
                .cloned()
                .unwrap_or_default();

            let remaining = limit - results.len();
            let max_count = remaining.to_string();
            let out = run_search_tool(
                "rg",
                &[
                    "-n",
                    "--column",
                    "--max-count",
                    &max_count,
                    pattern,
                    file_path,
                ],
            )?;
            if let Some(out) = out {
                let stdout = String::from_utf8_lossy(&out.stdout);
                let lines: Vec<&str> = stdout.lines().collect();
                let match_count = lines.len();
                if match_count > 0 {
                    let (line_num, snippet) = parse_match_line(lines[0], true, pattern);

                    results.push(json!({
                            "hcom_name": if hcom_name.is_empty() { serde_json::Value::Null } else { json!(hcom_name) },
                            "agent": agent,
                            "path": file_path,
                            "line": line_num,
                            "text": snippet,
                            "matches": match_count,
                        }));
                }
            }
        }

        for database_match in &database_matches {
            if results.len() >= limit {
                break;
            }
            let hcom_name = path_to_hcom
                .get(&transcript_search_key(
                    &database_match.path,
                    database_match.session_id.as_deref(),
                ))
                .cloned()
                .unwrap_or_default();
            results.push(json!({
                "hcom_name": if hcom_name.is_empty() { serde_json::Value::Null } else { json!(hcom_name) },
                "agent": database_match.agent,
                "path": database_match.path,
                "line": database_match.line,
                "text": database_match.text,
                "matches": database_match.matches,
                "session_id": database_match.session_id,
                "label": database_match.label,
            }));
        }

        return Ok(results);
    } else {
        // Active instances
        if let Ok(mut stmt) = db.conn().prepare(
            "SELECT name, transcript_path, tool, session_id FROM instances WHERE transcript_path IS NOT NULL AND transcript_path != ''"
        )
            && let Ok(rows) = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            }) {
                for (name, path, tool, session_id) in rows.flatten() {
                    if let Some(filter_tool) = agent_filter
                        && transcript::tool_from_agent_or_path(&tool, &path).ok() != Some(filter_tool)
                    {
                        continue;
                    }
                    if args.exclude_self && ctx_name.as_deref() == Some(name.as_str()) { continue; }
                    seen.insert(name.clone());
                    paths.push((name, path, tool, session_id));
                }
            }

        // Stopped instances from life event snapshots (C2/C3 fix)
        if !live_mode
            && let Ok(mut stmt) = db.conn().prepare(
                "SELECT instance, json_extract(data, '$.snapshot.transcript_path'), json_extract(data, '$.snapshot.tool'), json_extract(data, '$.snapshot.session_id') \
                 FROM events WHERE type = 'life' AND json_extract(data, '$.action') = 'stopped' \
                 AND json_extract(data, '$.snapshot.transcript_path') IS NOT NULL \
                 ORDER BY id DESC"
            )
                && let Ok(rows) = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                }) {
                    for (name, path, tool, session_id) in rows.flatten() {
                        if seen.contains(&name) { continue; }
                        if let Some(filter_tool) = agent_filter
                            && transcript::tool_from_agent_or_path(&tool, &path).ok() != Some(filter_tool)
                        {
                            continue;
                        }
                        if args.exclude_self && ctx_name.as_deref() == Some(name.as_str()) { continue; }
                        seen.insert(name.clone());
                        paths.push((name, path, tool, session_id));
                    }
                }
    }

    // Search using ripgrep (with line-level matches + snippets) — hcom-tracked/live paths
    let mut results = Vec::new();
    for (name, path, agent, session_id) in &paths {
        if !Path::new(path).exists() {
            continue;
        }

        // Use rg for line-level matches with context. `--column` gives us the
        // match offset so the snippet can be centered on the hit; the grep
        // fallback has no column, so parse_match_line locates the pattern itself.
        let remaining = limit - results.len();
        let resolved_tool = agent
            .parse::<Tool>()
            .ok()
            .or_else(|| transcript::detect_tool_from_path(path));

        if resolved_tool == Some(Tool::Hermes) {
            let Some(sid) = session_id.as_deref().filter(|s| !s.trim().is_empty()) else {
                continue;
            };
            let search_match =
                transcript::hermes::search_hermes_session(Path::new(path), sid, pattern)?;
            if let Some(m) = search_match {
                // Same shape as the --all database matches: session_id and
                // label are always present so consumers can correlate Hermes
                // results across tracked and --all scopes uniformly.
                results.push(json!({
                    "hcom_name": name,
                    "agent": agent,
                    "path": path,
                    "line": 0,
                    "text": m.text,
                    "matches": m.matches,
                    "session_id": m.session_id,
                    "label": m.label,
                }));
            }
            if results.len() >= limit {
                break;
            }
            continue;
        }
        let max_count = remaining.to_string();
        let mut has_column = true;
        let output = match run_search_tool(
            "rg",
            &["-n", "--column", "--max-count", &max_count, pattern, path],
        ) {
            Ok(output) => Ok(output),
            Err(rg_err) if rg_err.contains("was not found on PATH") => {
                has_column = false;
                run_search_tool("grep", &["-n", "-m", &max_count, pattern, path]).map_err(
                    |grep_err| {
                        format!(
                            "transcript search requires `rg` or `grep` on PATH ({rg_err}; {grep_err})"
                        )
                    },
                )
            }
            Err(err) => Err(err),
        };

        let output = output?;

        if let Some(out) = output {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let lines: Vec<&str> = stdout.lines().collect();
            let match_count = lines.len();
            if match_count > 0 {
                let (line_num, snippet) = parse_match_line(lines[0], has_column, pattern);

                results.push(json!({
                    "hcom_name": name,
                    "agent": agent,
                    "path": path,
                    "line": line_num,
                    "text": snippet,
                    "matches": match_count,
                }));
            }
        }

        if results.len() >= limit {
            break;
        }
    }

    Ok(results)
}

/// Search across transcripts: `hcom transcript search "pattern" [--live] [--all] [--limit N] [--exclude-self]`
fn cmd_transcript_search(
    db: &HcomDb,
    args: &TranscriptSearchArgs,
    ctx: Option<&CommandContext>,
) -> i32 {
    let live_mode = args.live;
    let all_mode = args.all;
    let json_mode = args.json;
    let limit = args.limit;
    let pattern = &args.pattern;

    let results = match collect_transcript_search_results(db, args, ctx) {
        Ok(results) => results,
        Err(err) => {
            eprintln!("Error: {err}");
            return 1;
        }
    };

    if all_mode && results.is_empty() {
        let selected_tools = args
            .agent
            .as_deref()
            .and_then(|a| transcript::parse_tool_filter(a).ok())
            .map(|t| vec![t])
            .unwrap_or_else(transcript::transcript_tools);
        let has_dirs = selected_tools.iter().any(|t| {
            transcript::disk_search_roots(*t)
                .into_iter()
                .any(|p| p.exists())
        });
        let has_dbs = selected_tools
            .iter()
            .any(|t| transcript::database_search_path(*t).is_some());
        let has_tracked_dbs = if selected_tools.contains(&Tool::Hermes)
            && let Ok(mut stmt) = db.conn().prepare(
                "SELECT 1 FROM instances WHERE lower(tool) = 'hermes' AND transcript_path IS NOT NULL AND transcript_path != '' \
                 UNION \
                 SELECT 1 FROM events WHERE type = 'life' AND json_extract(data, '$.action') = 'stopped' \
                 AND lower(json_extract(data, '$.snapshot.tool')) = 'hermes' \
                 AND json_extract(data, '$.snapshot.transcript_path') IS NOT NULL AND json_extract(data, '$.snapshot.transcript_path') != '' LIMIT 1",
            ) {
                stmt.exists([]).unwrap_or(false)
            } else {
                false
            };
        if !has_dirs && !has_dbs && !has_tracked_dbs {
            println!("No transcript directories or databases found on disk.");
            return 0;
        }
    }

    if json_mode {
        println!(
            "{}",
            json!({"count": results.len(), "results": results, "scope": if live_mode {"live"} else if all_mode {"all"} else {"hcom"}})
        );
        return 0;
    }

    if results.is_empty() {
        println!("No matches for \"{pattern}\"");
        return 0;
    }

    if all_mode {
        println!(
            "Found matches in {} transcripts (all on disk):",
            results.len()
        );
        for r in &results {
            let path = r["path"].as_str().unwrap_or("");
            let agent = r["agent"].as_str().unwrap_or("?");
            let line = r["line"].as_u64().unwrap_or(0);
            let matches = r["matches"].as_u64().unwrap_or(0);
            let snippet = r["text"].as_str().unwrap_or("");
            let label = r["label"].as_str().unwrap_or("");
            let session_id = r["session_id"].as_str().unwrap_or("");
            let short_path = path
                .split('/')
                .rev()
                .take(3)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("/");
            let name_part = r["hcom_name"]
                .as_str()
                .map(|n| format!(" ({n})"))
                .unwrap_or_default();
            println!("  [{agent}]{name_part} .../{short_path}:{line}  ({matches} matches)");
            if !label.is_empty() || !session_id.is_empty() {
                let mut details = Vec::new();
                if !label.is_empty() {
                    details.push(label.to_string());
                }
                if !session_id.is_empty() {
                    details.push(session_id.to_string());
                }
                println!("    {}", details.join(" | "));
            }
            if !snippet.is_empty() {
                println!("    {snippet}");
            }
        }
    } else {
        let scope_label = if live_mode {
            " (live agents)"
        } else {
            " (hcom-tracked)"
        };
        let limit_hit = results.len() >= limit;
        if limit_hit {
            println!(
                "Showing {} matches (limit {}){scope_label}:\n",
                results.len(),
                limit
            );
        } else {
            println!("Found {} matches{scope_label}:\n", results.len());
        }
        for result in &results {
            let hcom_name = result
                .get("hcom_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let agent = result.get("agent").and_then(|v| v.as_str()).unwrap_or("");
            let path = result.get("path").and_then(|v| v.as_str()).unwrap_or("");
            let line = result.get("line").and_then(|v| v.as_u64()).unwrap_or(0);
            let snippet = result.get("text").and_then(|v| v.as_str()).unwrap_or("");

            let path_display = if path.len() > 60 {
                let mut start = path.len() - 57;
                while start < path.len() && !path.is_char_boundary(start) {
                    start += 1;
                }
                format!("...{}", &path[start..])
            } else {
                path.to_string()
            };

            println!("[{agent}:{hcom_name}] {path_display}:{line}");
            // Snippet is already bounded and centered on the match by
            // parse_match_line; just flatten newlines for single-line display.
            let snippet_clean = snippet.replace('\n', " ");
            println!("    {snippet_clean}\n");
        }
    }

    0
}

/// Timeline: `hcom transcript timeline [--last N] [--full] [--json]`
fn cmd_transcript_timeline(db: &HcomDb, args: &TranscriptTimelineArgs) -> i32 {
    let json_mode = args.json;
    let full_mode = args.full;
    let detailed = args.detailed;
    let last_n = args.last.unwrap_or(10);

    // Collect all transcript paths (active + stopped sessions, C3 fix)
    let mut all_entries: Vec<Value> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    // Active instances
    if let Ok(mut stmt) = db.conn().prepare(
        "SELECT name, transcript_path, tool, session_id FROM instances WHERE transcript_path IS NOT NULL AND transcript_path != ''"
    )
        && let Ok(rows) = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        }) {
            for (name, path, tool, sid) in rows.flatten() {
                seen.insert(name.clone());
                if let Ok(exchanges) =
                    get_exchanges(&path, &tool, last_n, detailed, sid.as_deref(), true)
                {
                    for ex in exchanges {
                        all_entries.push(json!({
                            "instance": name,
                            "position": ex.position,
                            "user": ex.user,
                            "action": if full_mode { ex.action.clone() } else { summarize_action(&ex.action) },
                            "timestamp": ex.timestamp,
                            "files": ex.files,
                        }));
                    }
                }
            }
        }

    // Stopped instances from life event snapshots
    if let Ok(mut stmt) = db.conn().prepare(
        "SELECT instance, json_extract(data, '$.snapshot.transcript_path'), json_extract(data, '$.snapshot.tool'), json_extract(data, '$.snapshot.session_id') FROM events WHERE type = 'life' AND json_extract(data, '$.action') = 'stopped' AND json_extract(data, '$.snapshot.transcript_path') IS NOT NULL"
    )
        && let Ok(rows) = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        }) {
            for (name, path, tool, sid) in rows.flatten() {
                if seen.contains(&name) { continue; }
                seen.insert(name.clone());
                if let Ok(exchanges) =
                    get_exchanges(&path, &tool, last_n, detailed, sid.as_deref(), true)
                {
                    for ex in exchanges {
                        all_entries.push(json!({
                            "instance": name,
                            "position": ex.position,
                            "user": ex.user,
                            "action": if full_mode { ex.action.clone() } else { summarize_action(&ex.action) },
                            "timestamp": ex.timestamp,
                            "files": ex.files,
                        }));
                    }
                }
            }
        }

    // Sort by timestamp (most recent first)
    all_entries.sort_by(|a, b| {
        let ts_a = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let ts_b = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        ts_b.cmp(ts_a) // Reverse order
    });

    // Limit
    if all_entries.len() > last_n {
        all_entries.truncate(last_n);
    }

    if json_mode {
        println!(
            "{}",
            serde_json::to_string_pretty(&all_entries).unwrap_or_default()
        );
        return 0;
    }

    if all_entries.is_empty() {
        println!("No transcript entries found");
        return 0;
    }

    //
    println!("Timeline ({} exchanges):\n", all_entries.len());
    for entry in &all_entries {
        let inst = entry.get("instance").and_then(|v| v.as_str()).unwrap_or("");
        let ts = entry
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let user = entry.get("user").and_then(|v| v.as_str()).unwrap_or("");
        let action = entry.get("action").and_then(|v| v.as_str()).unwrap_or("");
        let files = entry.get("files").and_then(|v| v.as_array());

        let ts_short = if ts.contains('T') {
            ts.get(11..16).unwrap_or("??:??")
        } else if ts.len() >= 5 {
            ts.get(..5).unwrap_or("??:??")
        } else {
            "??:??"
        };

        let user_display = if user.len() > 80 {
            format!("{}...", truncate_str(user, 77))
        } else {
            user.to_string()
        };

        println!("[{ts_short}] \"{user_display}\"");

        if full_mode {
            for action_line in action.lines().take(10) {
                println!("  {action_line}");
            }
            let line_count = action.lines().count();
            if line_count > 10 {
                println!("  ... (+{} lines)", line_count - 10);
            }
        } else {
            let action_short = summarize_action(action);
            let action_display = if action_short.len() > 100 {
                format!("{}...", truncate_str(&action_short, 97))
            } else {
                action_short
            };
            println!("  → {action_display}");
        }

        if let Some(file_arr) = files {
            let file_strs: Vec<&str> = file_arr.iter().take(5).filter_map(|v| v.as_str()).collect();
            if !file_strs.is_empty() {
                println!("  Files: {}", file_strs.join(", "));
            }
        }

        // Command line (instance reference for navigation)
        println!(
            "  hcom transcript @{inst} {}",
            entry.get("position").and_then(|v| v.as_u64()).unwrap_or(1)
        );
        println!();
    }

    0
}

// ── Main Entry Point ─────────────────────────────────────────────────────

/// Main entry point for `hcom transcript` command.
pub fn cmd_transcript(db: &HcomDb, args: &TranscriptArgs, ctx: Option<&CommandContext>) -> i32 {
    // Handle subcommands
    match &args.subcmd {
        Some(TranscriptSubcmd::Search(search_args)) => {
            return cmd_transcript_search(db, search_args, ctx);
        }
        Some(TranscriptSubcmd::Timeline(timeline_args)) => {
            return cmd_transcript_timeline(db, timeline_args);
        }
        None => {}
    }

    let json_mode = args.json;
    let full_mode = args.full;
    let detailed = args.detailed;
    let last_n = args.last.unwrap_or(10);

    if let Some(ref name) = args.name {
        let stripped = name.strip_prefix('@').unwrap_or(name);
        let resolved = crate::identity::resolve_display_name_or_stopped(db, stripped)
            .or_else(|| resolve_remote_instance_name(db, stripped))
            .unwrap_or_else(|| stripped.to_string());
        if let Some((base_name, device)) = crate::relay::control::split_device_suffix(&resolved) {
            return crate::relay::control::dispatch_remote_and_print(
                db,
                device,
                Some(&resolved),
                crate::relay::control::rpc_action::TRANSCRIPT,
                &json!({
                    "target": base_name,
                    "display_target": resolved,
                    "origin_device": device,
                    "last": last_n,
                    "range": args.range_flag.as_ref().or(args.range_positional.as_ref()),
                    "json": json_mode,
                    "full": full_mode,
                    "detailed": detailed,
                }),
                crate::relay::control::RPC_DEFAULT_TIMEOUT,
                "content",
                "No remote transcript content",
            );
        }
    }

    // Resolve target and range from positional args
    let mut target = None;
    let mut range_str: Option<String> = args.range_flag.clone();

    if let Some(ref name) = args.name {
        let stripped = name.strip_prefix('@').unwrap_or(name);
        // Check if it looks like a range (digits and hyphens)
        if stripped.chars().all(|c| c.is_ascii_digit() || c == '-')
            && stripped.chars().any(|c| c.is_ascii_digit())
        {
            if range_str.is_none() {
                range_str = Some(stripped.to_string());
            }
        } else {
            target = Some(stripped.to_string());
        }
    }

    if let Some(ref range_pos) = args.range_positional
        && range_str.is_none()
    {
        range_str = Some(range_pos.clone());
    }

    // Resolve target to transcript path
    let (instance_name, transcript_path, agent_type, session_id) = if let Some(ref name) = target {
        // Try direct match
        let resolved = resolve_instance_transcript(db, name);
        match resolved {
            Some(r) => r,
            None => {
                eprintln!("Error: {}", no_transcript_error(db, name, name, None));
                return 1;
            }
        }
    } else if let Some(id) = ctx.and_then(|c| c.identity.as_ref()) {
        // Default to self
        match resolve_instance_transcript(db, &id.name) {
            Some(r) => r,
            None => {
                eprintln!("Error: No transcript available for current instance");
                return 1;
            }
        }
    } else {
        eprintln!("Usage: hcom transcript @instance [N | N-M] [--full] [--json]");
        return 1;
    };

    // Parse range
    let (range_start, range_end) = if let Some(ref r) = range_str {
        parse_range(r)
    } else {
        (None, None)
    };

    // Get exchanges
    let effective_last = if range_start.is_some() {
        usize::MAX
    } else {
        last_n
    };
    let exchanges = match get_exchanges(
        &transcript_path,
        &agent_type,
        effective_last,
        detailed,
        session_id.as_deref(),
        true,
    ) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Error: {e}");
            return 1;
        }
    };

    // Apply range filter
    let filtered: Vec<&Exchange> = if let Some(start) = range_start {
        let end = range_end.unwrap_or(start);
        exchanges
            .iter()
            .filter(|e| e.position >= start && e.position <= end)
            .collect()
    } else {
        exchanges.iter().collect()
    };

    if json_mode {
        let json_output: Vec<Value> = filtered
            .iter()
            .map(|ex| {
                let mut obj = json!({
                    "position": ex.position,
                    "user": ex.user,
                    "action": ex.action,
                    "files": ex.files,
                    "timestamp": ex.timestamp,
                });
                if detailed {
                    obj["tools"] = json!(
                        ex.tools
                            .iter()
                            .map(|t| {
                                let mut tool = json!({
                                    "name": t.name,
                                    "is_error": t.is_error,
                                });
                                if let Some(ref f) = t.file {
                                    tool["file"] = json!(f);
                                }
                                if let Some(ref c) = t.command {
                                    tool["command"] = json!(c);
                                }
                                tool
                            })
                            .collect::<Vec<_>>()
                    );
                    obj["edits"] = json!(ex.edits);
                    obj["errors"] = json!(ex.errors);
                    obj["ended_on_error"] = json!(ex.ended_on_error);
                }
                obj
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json_output).unwrap_or_default()
        );
        return 0;
    }

    if filtered.is_empty() {
        println!("No exchanges found");
        return 0;
    }

    // Header: "Recent conversation (N exchanges, X-Y of Z) - @instance:"
    let first_pos = filtered.first().map(|e| e.position).unwrap_or(1);
    let last_pos = filtered.last().map(|e| e.position).unwrap_or(1);
    println!(
        "Recent conversation ({} exchanges, {}-{} of {}) - @{}:\n",
        filtered.len(),
        first_pos,
        last_pos,
        exchanges.len(),
        instance_name,
    );

    let owned: Vec<Exchange> = filtered.into_iter().cloned().collect();
    let formatted = format_exchanges(&owned, &instance_name, full_mode, detailed);
    println!("{formatted}");

    0
}

/// Display options for transcript rendering.
pub struct TranscriptRenderOpts<'a> {
    pub range: Option<&'a str>,
    pub last_n: usize,
    pub json_mode: bool,
    pub full_mode: bool,
    pub detailed: bool,
    pub retry_codex: bool,
}

impl Default for TranscriptRenderOpts<'_> {
    fn default() -> Self {
        Self {
            range: None,
            last_n: 10,
            json_mode: false,
            full_mode: false,
            detailed: false,
            retry_codex: true,
        }
    }
}

pub fn render_instance_transcript(
    db: &HcomDb,
    name: &str,
    last_n: usize,
) -> Result<String, String> {
    render_instance_transcript_impl(
        db,
        name,
        &TranscriptRenderOpts {
            last_n,
            ..Default::default()
        },
        name,
        None,
    )
}

pub fn render_instance_transcript_with_options_no_retry(
    db: &HcomDb,
    name: &str,
    range: Option<&str>,
    last_n: usize,
    json_mode: bool,
    full_mode: bool,
    detailed: bool,
) -> Result<String, String> {
    render_instance_transcript_impl(
        db,
        name,
        &TranscriptRenderOpts {
            range,
            last_n,
            json_mode,
            full_mode,
            detailed,
            retry_codex: false,
        },
        name,
        None,
    )
}

/// Render a remote transcript while retaining the caller's device-qualified
/// name in diagnostics.
pub fn render_remote_instance_transcript_with_options_no_retry(
    db: &HcomDb,
    name: &str,
    display_name: &str,
    device: &str,
    opts: &TranscriptRenderOpts<'_>,
) -> Result<String, String> {
    render_instance_transcript_impl(db, name, opts, display_name, Some(device))
}

pub fn render_instance_transcript_with_options(
    db: &HcomDb,
    name: &str,
    range: Option<&str>,
    last_n: usize,
    json_mode: bool,
    full_mode: bool,
    detailed: bool,
) -> Result<String, String> {
    render_instance_transcript_impl(
        db,
        name,
        &TranscriptRenderOpts {
            range,
            last_n,
            json_mode,
            full_mode,
            detailed,
            retry_codex: true,
        },
        name,
        None,
    )
}

fn render_instance_transcript_impl(
    db: &HcomDb,
    name: &str,
    opts: &TranscriptRenderOpts<'_>,
    display_name: &str,
    device: Option<&str>,
) -> Result<String, String> {
    let (instance_name, transcript_path, agent_type, session_id) =
        resolve_instance_transcript(db, name)
            .ok_or_else(|| no_transcript_error(db, name, display_name, device))?;
    let (range_start, range_end) = if let Some(r) = opts.range {
        parse_range(r)
    } else {
        (None, None)
    };
    let effective_last = if range_start.is_some() {
        usize::MAX
    } else {
        opts.last_n
    };
    let exchanges = get_exchanges(
        &transcript_path,
        &agent_type,
        effective_last,
        opts.detailed,
        session_id.as_deref(),
        opts.retry_codex,
    )
    .map_err(|e| e.to_string())?;

    let filtered: Vec<&Exchange> = if let Some(start) = range_start {
        let end = range_end.unwrap_or(start);
        exchanges
            .iter()
            .filter(|e| e.position >= start && e.position <= end)
            .collect()
    } else {
        exchanges.iter().collect()
    };

    if opts.json_mode {
        let json_output: Vec<Value> = filtered
            .iter()
            .map(|ex| {
                let mut obj = json!({
                    "position": ex.position,
                    "user": ex.user,
                    "action": ex.action,
                    "files": ex.files,
                    "timestamp": ex.timestamp,
                });
                if opts.detailed {
                    obj["tools"] = json!(
                        ex.tools
                            .iter()
                            .map(|t| {
                                let mut tool = json!({
                                    "name": t.name,
                                    "is_error": t.is_error,
                                });
                                if let Some(ref f) = t.file {
                                    tool["file"] = json!(f);
                                }
                                if let Some(ref c) = t.command {
                                    tool["command"] = json!(c);
                                }
                                tool
                            })
                            .collect::<Vec<_>>()
                    );
                    obj["edits"] = json!(ex.edits);
                    obj["errors"] = json!(ex.errors);
                    obj["ended_on_error"] = json!(ex.ended_on_error);
                }
                obj
            })
            .collect();
        return serde_json::to_string_pretty(&json_output).map_err(|e| e.to_string());
    }

    if filtered.is_empty() {
        return Ok("No exchanges found".to_string());
    }

    let first_pos = filtered.first().map(|e| e.position).unwrap_or(1);
    let last_pos = filtered.last().map(|e| e.position).unwrap_or(1);
    let owned: Vec<Exchange> = filtered.into_iter().cloned().collect();
    let formatted = format_exchanges(&owned, &instance_name, opts.full_mode, opts.detailed);
    Ok(format!(
        "Recent conversation ({} exchanges, {}-{} of {}) - @{}:\n\n{}",
        owned.len(),
        first_pos,
        last_pos,
        exchanges.len(),
        instance_name,
        formatted
    ))
}

/// Resolve instance name to (name, transcript_path, agent_type, session_id).
fn resolve_instance_transcript(
    db: &HcomDb,
    name: &str,
) -> Option<(String, String, String, Option<String>)> {
    // An exact live or stopped identity is authoritative even when it has no
    // transcript. Only infer a prefix when no exact identity exists; otherwise
    // a transcript-bearing longer name could disclose the wrong conversation.
    if let Some(exact_name) = crate::identity::resolve_display_name_or_stopped(db, name) {
        match db.get_instance_full(&exact_name) {
            Ok(Some(instance)) => {
                if !instance.transcript_path.is_empty() {
                    return Some((
                        exact_name,
                        instance.transcript_path,
                        instance.tool,
                        instance.session_id,
                    ));
                }

                // Preserve the database-backed fallback for OpenCode and Kilo
                // (and other non-Hermes tools), while Hermes always uses its
                // tracked profile path to avoid reading an unrelated ambient DB.
                if let Ok(tool) = instance.tool.parse::<Tool>()
                    && tool != Tool::Hermes
                    && let Some(db_path) = transcript::database_search_path(tool)
                {
                    return Some((
                        exact_name,
                        db_path.to_string_lossy().to_string(),
                        instance.tool,
                        instance.session_id,
                    ));
                }

                // An exact current identity without a transcript blocks stale
                // stopped data and all prefix candidates.
                return None;
            }
            Ok(None) => {}
            Err(_) => return None,
        }

        // The exact name may be represented only by a stopped life snapshot.
        // Keep its recorded tool so database-backed stopped sessions select the
        // correct parser; fall back to path detection for older snapshots.
        if let Ok((path, sid, tool)) = db.conn().query_row(
            "SELECT json_extract(data, '$.snapshot.transcript_path'), json_extract(data, '$.snapshot.session_id'), json_extract(data, '$.snapshot.tool') FROM events WHERE type = 'life' AND instance = ? AND json_extract(data, '$.action') = 'stopped' ORDER BY id DESC LIMIT 1",
            rusqlite::params![&exact_name],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, Option<String>>(2)?)),
        ) {
            let agent = tool.unwrap_or_else(|| detect_agent_type(&path).to_string());
            return Some((exact_name, path, agent, sid));
        }

        return None;
    }

    // Prefix match is literal and must be unambiguous. Escaping all LIKE
    // metacharacters prevents '_'/'%'/'\\' in a user prefix from broadening it.
    let escaped = name
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let pattern = format!("{escaped}%");

    if let Ok(mut stmt) = db.conn().prepare(
        "SELECT name, transcript_path, tool, session_id FROM instances WHERE name LIKE ?1 ESCAPE '\\' AND transcript_path IS NOT NULL AND transcript_path != '' LIMIT 2",
    ) {
        let rows_res = stmt
            .query_map(rusqlite::params![pattern], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .and_then(|mapped| mapped.collect::<rusqlite::Result<Vec<_>>>());

        if let Ok(matches) = rows_res
            && matches.len() == 1
        {
            return Some(matches.into_iter().next().unwrap());
        }
    }

    None
}

/// Parse range string "N-M" or "N".
fn parse_range(s: &str) -> (Option<usize>, Option<usize>) {
    if let Some(dash_pos) = s.find('-') {
        let start: Option<usize> = s[..dash_pos].parse().ok().filter(|&v: &usize| v >= 1);
        let end: Option<usize> = s[dash_pos + 1..].parse().ok().filter(|&v: &usize| v >= 1);
        // Validate start <= end
        if let (Some(s), Some(e)) = (start, end)
            && s > e
        {
            eprintln!("Error: invalid range '{s}-{e}' (start must be <= end)");
            return (None, None);
        }
        (start, end)
    } else {
        let pos: Option<usize> = s.parse().ok().filter(|&v: &usize| v >= 1);
        (pos, pos)
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transcript::ToolUse;
    use crate::transcript::shared::finalize_action_text;
    use std::fs;

    fn test_db() -> HcomDb {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = HcomDb::open_raw(&db_path).unwrap();
        db.init_db().unwrap();
        std::mem::forget(dir);
        db
    }

    fn insert_test_instance(db: &HcomDb, name: &str, transcript_path: &str, tool: &str) {
        let mut data = serde_json::Map::new();
        data.insert(
            "created_at".into(),
            json!(crate::shared::time::now_epoch_f64()),
        );
        data.insert("tool".into(), json!(tool));
        data.insert("transcript_path".into(), json!(transcript_path));
        db.save_instance_named(name, &data).unwrap();
    }

    #[test]
    fn known_agent_without_transcript_points_to_transport_history() {
        let db = test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, transcript_path, tool) \
                 VALUES ('pita', 100.0, '', 'adhoc')",
                [],
            )
            .unwrap();

        let error = no_transcript_error(&db, "pita", "pita", None);
        assert_eq!(
            error,
            "No model transcript is registered for pita.\n\
View transport messages with: hcom events --participant pita --type message"
        );
        assert!(!error.contains("no messages have been exchanged"));
    }

    #[test]
    fn remote_agent_without_transcript_queries_its_origin_device() {
        let db = test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, transcript_path, tool) \
                 VALUES ('pita', 100.0, '', 'adhoc')",
                [],
            )
            .unwrap();

        let error = no_transcript_error(&db, "pita", "pita:ABCD", Some("ABCD"));
        assert_eq!(
            error,
            "No model transcript is registered for pita:ABCD.\n\
View transport messages with: hcom events --remote-fetch --device ABCD --participant pita --type message"
        );
    }

    #[test]
    fn resolve_instance_transcript_literal_wildcards_and_backslash() {
        let dir = tempfile::tempdir().unwrap();
        let db = test_db();

        let testa = dir.path().join("testa1.jsonl");
        let fooa = dir.path().join("fooa1.jsonl");
        let slash = dir.path().join("esc_slash.jsonl");
        fs::write(&testa, "").unwrap();
        fs::write(&fooa, "").unwrap();
        fs::write(&slash, "").unwrap();
        insert_test_instance(&db, "testa1", testa.to_str().unwrap(), "codex");
        insert_test_instance(&db, "fooa1", fooa.to_str().unwrap(), "codex");
        insert_test_instance(&db, "esc\\1", slash.to_str().unwrap(), "codex");

        assert_eq!(
            resolve_instance_transcript(&db, "test_"),
            None,
            "literal '_' must not match 'testa1' via SQL LIKE wildcard"
        );
        assert_eq!(
            resolve_instance_transcript(&db, "foo%"),
            None,
            "literal '%' must not match 'fooa1' via SQL LIKE wildcard"
        );
        assert_eq!(
            resolve_instance_transcript(&db, "esc\\")
                .as_ref()
                .map(|(name, _, _, _)| name.as_str()),
            Some("esc\\1"),
            "literal backslash prefix must resolve its literal instance"
        );
    }

    #[test]
    fn resolve_instance_transcript_rejects_ambiguous_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let db = test_db();
        let p1 = dir.path().join("ambig_one.jsonl");
        let p2 = dir.path().join("ambig_two.jsonl");
        fs::write(&p1, "").unwrap();
        fs::write(&p2, "").unwrap();
        insert_test_instance(&db, "ambig_one", p1.to_str().unwrap(), "codex");
        insert_test_instance(&db, "ambig_two", p2.to_str().unwrap(), "codex");

        assert_eq!(
            resolve_instance_transcript(&db, "ambig_"),
            None,
            "multiple prefix candidates must fail closed"
        );
    }

    #[test]
    fn resolve_instance_transcript_exact_live_identity_blocks_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let db = test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, transcript_path, tool) \
                 VALUES ('pico', 1.0, '', 'adhoc')",
                [],
            )
            .unwrap();
        let longer = dir.path().join("pico_worker.jsonl");
        fs::write(&longer, "").unwrap();
        insert_test_instance(&db, "pico_worker", longer.to_str().unwrap(), "codex");

        assert_eq!(
            resolve_instance_transcript(&db, "pico"),
            None,
            "an exact live identity without a transcript must not borrow a prefix match"
        );
    }

    #[test]
    fn resolve_instance_transcript_exact_match_precedes_prefix_ambiguity() {
        let dir = tempfile::tempdir().unwrap();
        let db = test_db();
        let exact = dir.path().join("exact.jsonl");
        let longer = dir.path().join("exact_one.jsonl");
        fs::write(&exact, "").unwrap();
        fs::write(&longer, "").unwrap();
        insert_test_instance(&db, "exact", exact.to_str().unwrap(), "codex");
        insert_test_instance(&db, "exact_one", longer.to_str().unwrap(), "codex");

        let resolved = resolve_instance_transcript(&db, "exact").unwrap();
        assert_eq!(resolved.0, "exact");
        assert_eq!(resolved.1, exact.to_str().unwrap());
    }

    #[test]
    fn resolve_instance_transcript_exact_stopped_precedes_prefix_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let db = test_db();
        let active_one = dir.path().join("pico_one.jsonl");
        let active_two = dir.path().join("pico_two.jsonl");
        let stopped = dir.path().join("pico_stopped.jsonl");
        fs::write(&active_one, "").unwrap();
        fs::write(&active_two, "").unwrap();
        fs::write(&stopped, "").unwrap();
        insert_test_instance(&db, "pico_one", active_one.to_str().unwrap(), "codex");
        insert_test_instance(&db, "pico_two", active_two.to_str().unwrap(), "codex");
        db.conn()
            .execute(
                "INSERT INTO events (timestamp, type, instance, data) VALUES (?1, 'life', 'pico', ?2)",
                rusqlite::params![
                    "2026-03-27T10:00:00Z",
                    json!({
                        "action": "stopped",
                        "snapshot": {
                            "transcript_path": stopped.to_str().unwrap(),
                            "session_id": "sess-pico-stopped",
                            "tool": "codex"
                        }
                    })
                    .to_string()
                ],
            )
            .unwrap();

        let resolved = resolve_instance_transcript(&db, "pico").unwrap();
        assert_eq!(resolved.0, "pico");
        assert_eq!(resolved.1, stopped.to_str().unwrap());
        assert_eq!(resolved.3.as_deref(), Some("sess-pico-stopped"));
    }

    #[test]
    fn resolve_instance_transcript_stale_stopped_incarnation_does_not_override_live() {
        let dir = tempfile::tempdir().unwrap();
        let db = test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at, transcript_path, tool) \
                 VALUES ('pico', 1.0, '', 'adhoc')",
                [],
            )
            .unwrap();
        let stopped = dir.path().join("pico_stopped.jsonl");
        fs::write(&stopped, "").unwrap();
        db.conn()
            .execute(
                "INSERT INTO events (timestamp, type, instance, data) VALUES (?1, 'life', 'pico', ?2)",
                rusqlite::params![
                    "2026-03-27T10:00:00Z",
                    json!({
                        "action": "stopped",
                        "snapshot": {"transcript_path": stopped.to_str().unwrap()}
                    })
                    .to_string()
                ],
            )
            .unwrap();

        assert_eq!(
            resolve_instance_transcript(&db, "pico"),
            None,
            "a current exact identity must not inherit an older incarnation's transcript"
        );
    }

    #[test]
    fn resolve_instance_transcript_stopped_snapshot_uses_recorded_tool() {
        let dir = tempfile::tempdir().unwrap();
        let db = test_db();
        let stopped = dir.path().join("session.db");
        fs::write(&stopped, "").unwrap();
        db.conn()
            .execute(
                "INSERT INTO events (timestamp, type, instance, data) VALUES (?1, 'life', 'miso', ?2)",
                rusqlite::params![
                    "2026-03-27T10:00:00Z",
                    json!({
                        "action": "stopped",
                        "snapshot": {
                            "transcript_path": stopped.to_str().unwrap(),
                            "session_id": "sess-stopped-123",
                            "tool": "opencode"
                        }
                    })
                    .to_string()
                ],
            )
            .unwrap();

        let resolved = resolve_instance_transcript(&db, "miso").unwrap();
        assert_eq!(resolved.0, "miso");
        assert_eq!(resolved.1, stopped.to_str().unwrap());
        assert_eq!(resolved.2, "opencode");
        assert_eq!(resolved.3.as_deref(), Some("sess-stopped-123"));
    }

    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range("5"), (Some(5), Some(5)));
        assert_eq!(parse_range("3-10"), (Some(3), Some(10)));
        assert_eq!(parse_range("abc"), (None, None));
    }

    #[test]
    fn test_centered_snippet_shows_match_deep_in_long_line() {
        // A whole-JSON transcript line: the match sits far past the start, where
        // start-anchored truncation would never reach it.
        let prefix = "{\"parentUuid\":null,".repeat(40); // long metadata head
        let line = format!("{prefix}\"text\":\"NEEDLE here\"}}");
        let col = line.find("NEEDLE").unwrap() + 1; // rg is 1-based
        let snip = centered_snippet(&line, col, SEARCH_SNIPPET_WIDTH);
        assert!(snip.contains("NEEDLE"), "match must be visible: {snip}");
        assert!(snip.starts_with('…'), "elided head marked: {snip}");
        assert!(snip.len() <= SEARCH_SNIPPET_WIDTH + 8); // window + ellipses/trim slack
    }

    #[test]
    fn test_centered_snippet_short_line_returned_whole() {
        let line = "{\"text\":\"hi ping there\"}";
        let col = line.find("ping").unwrap() + 1;
        let snip = centered_snippet(line, col, SEARCH_SNIPPET_WIDTH);
        assert_eq!(snip, line, "short line kept intact, no ellipsis");
    }

    #[test]
    fn test_centered_snippet_multibyte_safe() {
        // Match adjacent to multi-byte chars; slicing must not panic and must
        // stay on char boundaries.
        let line = format!("{}émoji→NEEDLE←café{}", "🚀".repeat(60), "ü".repeat(60));
        let col = line.find("NEEDLE").unwrap() + 1;
        let snip = centered_snippet(&line, col, SEARCH_SNIPPET_WIDTH);
        assert!(snip.contains("NEEDLE"));
        assert!(std::str::from_utf8(snip.as_bytes()).is_ok());
    }

    #[test]
    fn test_parse_match_line_rg_with_column() {
        // rg --column format: LINE:COL:TEXT — COL points at the match.
        let head = "x".repeat(300);
        let text = format!("{head}FINDME{head}");
        let col = 301; // 1-based, right after the 300-char head
        let raw = format!("44:{col}:{text}");
        let (line, snip) = parse_match_line(&raw, true, "FINDME");
        assert_eq!(line, 44);
        assert!(snip.contains("FINDME"), "centered on rg column: {snip}");
    }

    #[test]
    fn test_parse_match_line_grep_fallback_locates_pattern() {
        // grep format: LINE:TEXT (no column) — pattern located case-insensitively.
        let head = "y".repeat(300);
        let text = format!("{head}findME{head}");
        let raw = format!("7:{text}");
        let (line, snip) = parse_match_line(&raw, false, "FINDME");
        assert_eq!(line, 7);
        assert!(
            snip.contains("findME"),
            "grep fallback centers on match: {snip}"
        );
    }

    #[test]
    fn test_summarize_action() {
        let short = "Hello world";
        assert_eq!(summarize_action(short), "Hello world");

        let multi = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5";
        let result = summarize_action(multi);
        assert!(result.contains("Line 1"));
        assert!(result.ends_with("..."));
    }

    #[test]
    fn test_detect_agent_type() {
        assert_eq!(
            detect_agent_type("/home/user/.claude/projects/x/transcript.jsonl"),
            "claude"
        );
        assert_eq!(
            detect_agent_type("/home/user/.gemini/tmp/project/chats/session-1-abc.json"),
            "gemini"
        );
        assert_eq!(
            detect_agent_type("/home/user/.codex/sessions/x/rollout.jsonl"),
            "codex"
        );
        assert_eq!(
            detect_agent_type("/home/user/.local/share/opencode/opencode.db"),
            "opencode"
        );
        assert_eq!(
            detect_agent_type("/home/user/.local/share/kilo/kilo.db"),
            "kilo"
        );
        assert_eq!(
            detect_agent_type("/home/user/Library/Application Support/Antigravity/session.jsonl"),
            "antigravity"
        );
        assert_eq!(
            detect_agent_type("/home/user/.copilot/session-state/abc/events.jsonl"),
            "copilot"
        );
        assert_eq!(detect_agent_type("/home/user/.hermes/state.db"), "hermes");
        assert_eq!(detect_agent_type("/tmp/state.db"), "unknown");
    }

    #[test]
    fn detect_agent_type_covers_released_integrations_with_transcript_parsers() {
        let cases = [
            ("/home/user/.claude/projects/x/transcript.jsonl", "claude"),
            (
                "/home/user/.gemini/tmp/project/chats/session-1-abc.json",
                "gemini",
            ),
            ("/home/user/.codex/sessions/x/rollout.jsonl", "codex"),
            ("/home/user/.local/share/opencode/opencode.db", "opencode"),
            ("/home/user/.local/share/kilo/kilo.db", "kilo"),
            (
                "/home/user/Library/Application Support/Antigravity/session.jsonl",
                "antigravity",
            ),
            (
                "/home/user/.cursor/projects/x/agent-transcripts/abc/abc.jsonl",
                "cursor",
            ),
            (
                "/home/user/.kimi-code/sessions/wd_x/abc123/agents/main/wire.jsonl",
                "kimi",
            ),
            (
                "/home/user/.copilot/session-state/abc/events.jsonl",
                "copilot",
            ),
            ("/home/user/.pi/agent/sessions/x/20260603_abc.jsonl", "pi"),
            ("/home/user/.omp/agent/sessions/x/20260603_abc.jsonl", "omp"),
            ("/home/user/.hermes/state.db", "hermes"),
        ];
        let expected: std::collections::HashSet<&str> =
            crate::integration_spec::released_tool_names()
                .into_iter()
                .collect();
        let actual: std::collections::HashSet<&str> = cases
            .iter()
            .map(|(path, expected_tool)| {
                let detected = detect_agent_type(path);
                assert_eq!(detected, *expected_tool);
                detected
            })
            .collect();

        assert_eq!(
            actual, expected,
            "transcript path detection cases must cover every released integration"
        );
    }

    #[test]
    fn attribute_disk_match_uses_provenance_for_unsignatured_pi_sessions() {
        let pi_root = PathBuf::from("/data/pi-sessions");
        let gem_root = PathBuf::from("/home/u/.gemini");
        let owners = vec![
            (pi_root.clone(), Tool::Pi),
            // gemini and antigravity share one root — the ambiguous case.
            (gem_root.clone(), Tool::Gemini),
            (gem_root.clone(), Tool::Antigravity),
        ];
        let selected = [Tool::Pi, Tool::Gemini, Tool::Antigravity];

        // A bare uuid.jsonl under a custom PI_CODING_AGENT_SESSION_DIR has no
        // content signature, so it is attributed by provenance.
        assert_eq!(
            attribute_disk_match("/data/pi-sessions/abc/9f8e.jsonl", &selected, &owners),
            Some(Tool::Pi)
        );
        // A signatured gemini file under the shared root resolves by content.
        assert_eq!(
            attribute_disk_match(
                "/home/u/.gemini/tmp/p/chats/session-1-x.json",
                &selected,
                &owners
            ),
            Some(Tool::Gemini)
        );
        // An unsignatured file under the shared gemini/antigravity root is
        // ambiguous by provenance and must not be guessed.
        assert_eq!(
            attribute_disk_match("/home/u/.gemini/tmp/p/notes.jsonl", &selected, &owners),
            None
        );
        // Provenance only counts roots for selected tools.
        assert_eq!(
            attribute_disk_match("/data/pi-sessions/abc/9f8e.jsonl", &[Tool::Gemini], &owners),
            None
        );
    }

    #[test]
    fn detect_agent_type_cursor_keys_on_agent_transcripts_not_dotcursor() {
        // Regression: a Claude transcript path with a LITERAL `.cursor` segment
        // (the CLAUDE_CONFIG_DIR-style vector the old `.contains(".cursor")`
        // matcher WOULD have misrouted to cursor) must detect claude. Feeds
        // resume tool detection → wrong match would break resume + parser.
        assert_eq!(
            detect_agent_type("/home/u/.claude/projects/x/.cursor/abcd.jsonl"),
            "claude"
        );
        // A real cursor transcript (the `agent-transcripts` segment) detects cursor.
        assert_eq!(
            detect_agent_type("/home/u/.cursor/projects/repo/agent-transcripts/uuid/uuid.jsonl"),
            "cursor"
        );
    }

    #[test]
    fn test_correlate_paths_to_hcom_uses_session_id_for_opencode() {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_raw(&dir.path().join("hcom.db")).unwrap();
        db.conn()
            .execute_batch(
                "CREATE TABLE instances (
                     name text,
                     transcript_path text,
                     session_id text
                 );
                 CREATE TABLE events (
                     id integer PRIMARY KEY,
                     type text,
                     instance text,
                     data text
                 );",
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, transcript_path, session_id) VALUES (?, ?, ?)",
                rusqlite::params!["luna", "/tmp/opencode.db", "ses_a"],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, transcript_path, session_id) VALUES (?, ?, ?)",
                rusqlite::params!["nova", "/tmp/opencode.db", "ses_b"],
            )
            .unwrap();

        let correlated = correlate_paths_to_hcom(
            &db,
            &[
                ("/tmp/opencode.db".to_string(), Some("ses_a".to_string())),
                ("/tmp/opencode.db".to_string(), Some("ses_b".to_string())),
                ("/tmp/file.jsonl".to_string(), None),
            ],
        );

        assert_eq!(
            correlated.get(&transcript_search_key("/tmp/opencode.db", Some("ses_a"))),
            Some(&"luna".to_string())
        );
        assert_eq!(
            correlated.get(&transcript_search_key("/tmp/opencode.db", Some("ses_b"))),
            Some(&"nova".to_string())
        );
    }

    #[test]
    fn test_correlate_paths_to_hcom_uses_session_id_for_hermes() {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_raw(&dir.path().join("hcom.db")).unwrap();
        db.conn()
            .execute_batch(
                "CREATE TABLE instances (
                     name text,
                     transcript_path text,
                     session_id text
                 );
                 CREATE TABLE events (
                     id integer PRIMARY KEY,
                     type text,
                     instance text,
                     data text
                 );",
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, transcript_path, session_id) VALUES (?, ?, ?)",
                rusqlite::params!["herm_a", "/home/user/.hermes/state.db", "ses_herm_1"],
            )
            .unwrap();

        let correlated = correlate_paths_to_hcom(
            &db,
            &[
                (
                    "/home/user/.hermes/state.db".to_string(),
                    Some("ses_herm_1".to_string()),
                ),
                (
                    "/home/user/.hermes/state.db".to_string(),
                    Some("ses_herm_2".to_string()),
                ),
            ],
        );

        assert_eq!(
            correlated.get(&transcript_search_key(
                "/home/user/.hermes/state.db",
                Some("ses_herm_1")
            )),
            Some(&"herm_a".to_string())
        );
        assert_eq!(
            correlated.get(&transcript_search_key(
                "/home/user/.hermes/state.db",
                Some("ses_herm_2")
            )),
            None
        );
    }

    #[test]
    fn test_transcript_display_for_tool_only_and_error_turns() {
        let tools = vec![ToolUse {
            name: "Bash".to_string(),
            is_error: false,
            file: None,
            command: Some("pwd".to_string()),
            output: None,
        }];
        assert_eq!(
            finalize_action_text("", &tools, &[], false),
            "(tool-only turn: Bash)"
        );

        let errors = vec![json!({"tool": "Bash", "content": "Exit code: 1"})];
        assert_eq!(
            finalize_action_text("", &tools, &errors, true),
            "(turn ended in error after using Bash)"
        );
    }

    #[test]
    fn test_render_instance_transcript_with_options_range_matches_cli_shape() {
        let dir = tempfile::tempdir().unwrap();
        let transcript_path = dir.path().join("rollout.jsonl");
        let db = test_db();
        let now = crate::shared::time::now_epoch_f64();
        let lines = [
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:00Z",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "first user"}]
                }
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:01Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "first answer"}]
                }
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:01:00Z",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "second user"}]
                }
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:01:01Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "second answer"}]
                }
            }),
        ];
        fs::write(
            &transcript_path,
            lines
                .iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let mut data = serde_json::Map::new();
        data.insert("created_at".into(), json!(now));
        data.insert("tool".into(), json!("codex"));
        data.insert(
            "transcript_path".into(),
            json!(transcript_path.to_string_lossy().to_string()),
        );
        db.save_instance_named("luna", &data).unwrap();

        let rendered = render_instance_transcript_with_options(
            &db,
            "luna",
            Some("2"),
            10,
            false,
            false,
            false,
        )
        .unwrap();

        assert!(rendered.contains("Recent conversation (1 exchanges, 2-2 of 2) - @luna:"));
        assert!(rendered.contains("second user"));
        assert!(rendered.contains("second answer"));
        assert!(!rendered.contains("first user"));
    }

    #[test]
    fn test_render_instance_transcript_with_options_json_contract() {
        let dir = tempfile::tempdir().unwrap();
        let transcript_path = dir.path().join("rollout.jsonl");
        let db = test_db();
        let now = crate::shared::time::now_epoch_f64();
        let lines = [
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:00Z",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "user prompt"}]
                }
            }),
            json!({
                "type": "response_item",
                "timestamp": "2026-03-27T10:00:01Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "assistant answer"}]
                }
            }),
        ];
        fs::write(
            &transcript_path,
            lines
                .iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let mut data = serde_json::Map::new();
        data.insert("created_at".into(), json!(now));
        data.insert("tool".into(), json!("codex"));
        data.insert(
            "transcript_path".into(),
            json!(transcript_path.to_string_lossy().to_string()),
        );
        db.save_instance_named("luna", &data).unwrap();

        let rendered =
            render_instance_transcript_with_options(&db, "luna", None, 10, true, false, false)
                .unwrap();
        let parsed: Value = serde_json::from_str(&rendered).unwrap();
        let first = parsed.as_array().unwrap().first().unwrap();

        assert_eq!(first["position"], 1);
        assert_eq!(first["user"], "user prompt");
        assert_eq!(first["action"], "assistant answer");
    }

    #[test]
    fn test_render_antigravity_transcript_user_input_and_planner_response() {
        let dir = tempfile::tempdir().unwrap();
        let transcript_path = dir.path().join("Antigravity-session.jsonl");
        let db = test_db();
        let now = crate::shared::time::now_epoch_f64();
        let lines = [
            json!({
                "type": "USER_INPUT",
                "timestamp": "2026-03-27T10:00:00Z",
                "text": "review the hook changes"
            }),
            json!({
                "type": "PLANNER_RESPONSE",
                "timestamp": "2026-03-27T10:00:01Z",
                "text": "I will inspect the Antigravity hook path."
            }),
        ];
        fs::write(
            &transcript_path,
            lines
                .iter()
                .map(serde_json::Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();

        let mut data = serde_json::Map::new();
        data.insert("created_at".into(), json!(now));
        data.insert("tool".into(), json!("antigravity"));
        data.insert(
            "transcript_path".into(),
            json!(transcript_path.to_string_lossy().to_string()),
        );
        db.save_instance_named("vibo", &data).unwrap();

        let rendered =
            render_instance_transcript_with_options(&db, "vibo", None, 10, false, false, false)
                .unwrap();

        assert!(rendered.contains("review the hook changes"));
        assert!(rendered.contains("inspect the Antigravity hook path."));
        assert!(!rendered.contains("No exchanges found"));
    }

    #[test]
    fn test_finalize_action_text_uses_final_error_state_only() {
        let tools = vec![
            ToolUse {
                name: "Edit".to_string(),
                is_error: true,
                file: Some("a.rs".to_string()),
                command: None,
                output: None,
            },
            ToolUse {
                name: "Edit".to_string(),
                is_error: false,
                file: Some("a.rs".to_string()),
                command: None,
                output: None,
            },
        ];
        let errors = vec![json!({"tool": "Edit", "content": "old failure"})];
        assert_eq!(
            finalize_action_text("", &tools, &errors, false),
            "(tool-only turn: Edit)"
        );
    }

    // ── Clap parse tests ─────────────────────────────────────────────

    use clap::Parser;

    #[test]
    fn test_transcript_view_basic() {
        let args = TranscriptArgs::try_parse_from(["transcript", "peso"]).unwrap();
        assert!(args.subcmd.is_none());
        assert_eq!(args.name.as_deref(), Some("peso"));
        assert!(!args.json);
    }

    #[test]
    fn test_transcript_view_with_flags() {
        let args = TranscriptArgs::try_parse_from([
            "transcript",
            "@peso",
            "--json",
            "--full",
            "--last",
            "5",
        ])
        .unwrap();
        assert_eq!(args.name.as_deref(), Some("@peso"));
        assert!(args.json);
        assert!(args.full);
        assert_eq!(args.last, Some(5));
    }

    #[test]
    fn test_transcript_view_range() {
        let args = TranscriptArgs::try_parse_from(["transcript", "peso", "3-10"]).unwrap();
        assert_eq!(args.name.as_deref(), Some("peso"));
        assert_eq!(args.range_positional.as_deref(), Some("3-10"));
    }

    #[test]
    fn test_transcript_view_accepts_tail_alias() {
        // Tracker 38: `hcom transcript NAME --tail N` must map to --last instead
        // of a clap rejection.
        let args = TranscriptArgs::try_parse_from(["transcript", "kuma", "--tail", "5"]).unwrap();
        assert_eq!(args.name.as_deref(), Some("kuma"));
        assert_eq!(args.last, Some(5));
    }

    #[test]
    fn test_transcript_timeline_accepts_tail_alias() {
        let args =
            TranscriptArgs::try_parse_from(["transcript", "timeline", "--tail", "3"]).unwrap();
        match args.subcmd {
            Some(TranscriptSubcmd::Timeline(ref t)) => assert_eq!(t.last, Some(3)),
            _ => panic!("expected Timeline subcommand"),
        }
    }

    #[test]
    fn test_transcript_search() {
        let args = TranscriptArgs::try_parse_from([
            "transcript",
            "search",
            "error",
            "--live",
            "--limit",
            "50",
        ])
        .unwrap();
        match args.subcmd {
            Some(TranscriptSubcmd::Search(ref s)) => {
                assert_eq!(s.pattern, "error");
                assert!(s.live);
                assert_eq!(s.limit, 50);
            }
            _ => panic!("expected Search subcommand"),
        }
    }

    #[test]
    fn test_transcript_timeline() {
        let args =
            TranscriptArgs::try_parse_from(["transcript", "timeline", "--json", "--last", "3"])
                .unwrap();
        match args.subcmd {
            Some(TranscriptSubcmd::Timeline(ref t)) => {
                assert!(t.json);
                assert_eq!(t.last, Some(3));
            }
            _ => panic!("expected Timeline subcommand"),
        }
    }

    #[test]
    fn test_transcript_rejects_bogus() {
        assert!(TranscriptArgs::try_parse_from(["transcript", "--bogus"]).is_err());
    }

    #[test]
    fn missing_search_tool_is_an_error_not_an_empty_result() {
        let err =
            run_search_tool("__hcom_definitely_missing_search_tool__", &["pattern"]).unwrap_err();
        assert!(err.contains("was not found on PATH"));
    }

    #[test]
    #[cfg(unix)]
    fn search_tool_candidate_scan_tolerates_inaccessible_path_entries() {
        use std::ffi::OsString;
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().unwrap();
        let inacc_dir = temp_dir.path().join("inaccessible");
        std::fs::create_dir(&inacc_dir).unwrap();
        std::fs::set_permissions(&inacc_dir, std::fs::Permissions::from_mode(0o000)).unwrap();

        let acc_dir = temp_dir.path().join("accessible");
        std::fs::create_dir(&acc_dir).unwrap();
        // A real candidate, but not executable.
        let bad_tool = acc_dir.join("bad_tool");
        std::fs::write(&bad_tool, b"#!/bin/sh\necho hi\n").unwrap();
        std::fs::set_permissions(&bad_tool, std::fs::Permissions::from_mode(0o644)).unwrap();
        // A directory named like a tool is not an executable candidate.
        std::fs::create_dir(acc_dir.join("sub_dir")).unwrap();

        let path_env = OsString::from(format!("{}:{}", inacc_dir.display(), acc_dir.display()));

        // A missing bare tool stays missing even though one PATH entry EACCESes.
        assert!(!search_tool_candidate_on_path(
            "__hcom_definitely_missing_search_tool__",
            Some(&path_env)
        ));
        // A real (even non-executable) candidate file is found.
        assert!(search_tool_candidate_on_path("bad_tool", Some(&path_env)));
        // A directory named like the tool does not count.
        assert!(!search_tool_candidate_on_path("sub_dir", Some(&path_env)));

        // Restore so the tempdir can be removed on hosts where deletion needs it.
        let _ = std::fs::set_permissions(&inacc_dir, std::fs::Permissions::from_mode(0o700));
    }

    #[test]
    #[cfg(unix)]
    #[serial_test::serial]
    fn missing_search_tool_with_inaccessible_path_entry_is_still_not_found() {
        use std::os::unix::fs::PermissionsExt;

        struct EnvGuard {
            original_path: Option<std::ffi::OsString>,
            inacc_dir: PathBuf,
        }

        impl Drop for EnvGuard {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(
                    &self.inacc_dir,
                    std::fs::Permissions::from_mode(0o700),
                );
                // SAFETY: serialized test restoring process env
                unsafe {
                    match &self.original_path {
                        Some(val) => std::env::set_var("PATH", val),
                        None => std::env::remove_var("PATH"),
                    }
                }
            }
        }

        let temp_dir = tempfile::tempdir().unwrap();
        // PATH shape matching the WSL failure: an inaccessible directory
        // ahead of an accessible one holding a real candidate.
        let inacc_dir = temp_dir.path().join("inaccessible");
        std::fs::create_dir(&inacc_dir).unwrap();
        std::fs::set_permissions(&inacc_dir, std::fs::Permissions::from_mode(0o000)).unwrap();

        let acc_dir = temp_dir.path().join("accessible");
        std::fs::create_dir(&acc_dir).unwrap();
        let unexec_file = acc_dir.join("test_unexec_search_tool");
        std::fs::write(&unexec_file, b"#!/bin/sh\necho hi\n").unwrap();
        std::fs::set_permissions(&unexec_file, std::fs::Permissions::from_mode(0o644)).unwrap();

        let orig_path = std::env::var_os("PATH");
        // Bound as `_guard` so it stays alive until test end and restores
        // PATH even on assertion panic.
        let _guard = EnvGuard {
            original_path: orig_path.clone(),
            inacc_dir: inacc_dir.clone(),
        };
        let test_path = match &orig_path {
            Some(p) => format!(
                "{}:{}:{}",
                inacc_dir.display(),
                acc_dir.display(),
                p.to_string_lossy()
            ),
            None => format!("{}:{}", inacc_dir.display(), acc_dir.display()),
        };
        // SAFETY: serialized test mutating process env
        unsafe {
            std::env::set_var("PATH", &test_path);
        }

        // Missing bare tool: the actionable missing-tool diagnostic, not
        // "could not run: Permission denied" leaked from the PATH entry.
        let missing_err =
            run_search_tool("__hcom_definitely_missing_search_tool__", &["pattern"]).unwrap_err();
        assert!(
            missing_err.contains("was not found on PATH"),
            "expected 'was not found on PATH', got: {missing_err}"
        );

        // Real non-executable candidate: keeps the permission error.
        let unexec_err = run_search_tool("test_unexec_search_tool", &["pattern"]).unwrap_err();
        assert!(
            unexec_err.contains("could not run `test_unexec_search_tool`"),
            "expected 'could not run `test_unexec_search_tool`', got: {unexec_err}"
        );
        assert!(
            unexec_err.contains("Permission denied") || unexec_err.contains("os error 13"),
            "expected permission error detail, got: {unexec_err}"
        );
        assert!(
            !unexec_err.contains("was not found on PATH"),
            "must not report a real candidate as missing, got: {unexec_err}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn non_executable_search_tool_at_explicit_path_keeps_permission_error() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().unwrap();
        let tool = temp_dir.path().join("hcom_unexecutable_search_tool");
        std::fs::write(&tool, b"#!/bin/sh\necho hi\n").unwrap();
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o644)).unwrap();

        // Explicit paths never do PATH search, so PermissionDenied is real.
        let err = run_search_tool(tool.to_str().unwrap(), &["pattern"]).unwrap_err();
        assert!(
            err.contains("could not run") && !err.contains("was not found on PATH"),
            "expected permission error for explicit path, got: {err}"
        );
    }

    #[test]
    fn test_tracked_hermes_search_json_includes_session_id_and_label() {
        let temp = tempfile::tempdir().unwrap();
        let db = HcomDb::open_raw(&temp.path().join("hcom.db")).unwrap();
        db.init_db().unwrap();

        let profile_dir = temp.path().join("profile1");
        std::fs::create_dir_all(&profile_dir).unwrap();
        let state_db = profile_dir.join("state.db");
        let conn = rusqlite::Connection::open(&state_db).unwrap();
        conn.execute_batch(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                tool_call_id TEXT DEFAULT '',
                tool_calls TEXT DEFAULT '',
                tool_name TEXT DEFAULT '',
                timestamp REAL DEFAULT 0.0,
                token_count INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1
            );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) \
             VALUES ('ses_p1', 'user', 'needle question in profile one', 1700000000.0)",
            [],
        )
        .unwrap();

        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) \
                 VALUES ('herm_a', 'hermes', 'ses_p1', ?, 'active', 1000.0)",
                rusqlite::params![state_db.to_str().unwrap()],
            )
            .unwrap();

        let args = TranscriptSearchArgs {
            pattern: "needle".to_string(),
            live: false,
            all: false,
            json: true,
            exclude_self: false,
            limit: 10,
            agent: None,
        };
        let results = collect_transcript_search_results(&db, &args, None).unwrap();
        assert_eq!(
            results.len(),
            1,
            "tracked Hermes match expected: {results:?}"
        );
        let entry = &results[0];
        assert_eq!(entry["hcom_name"].as_str(), Some("herm_a"));
        assert_eq!(entry["agent"].as_str(), Some("hermes"));
        // The tracked JSON carries session_id/label like the --all database
        // matches, so consumers can correlate both scopes uniformly.
        assert_eq!(entry["session_id"].as_str(), Some("ses_p1"));
        assert!(
            entry.get("label").is_some(),
            "label key must be present (null) like --all database matches"
        );
        assert!(
            entry["label"].is_null(),
            "a present label must be null, matching --all database matches"
        );
        assert!(entry["text"].as_str().unwrap().contains("needle"));
    }

    #[test]
    #[serial_test::serial]
    fn test_hermes_two_profiles_transcript_resolution_from_unrelated_caller() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let db_file = temp.path().join("hcom.db");
        let db = HcomDb::open_raw(&db_file).unwrap();
        db.init_db().unwrap();

        let profile1_dir = temp.path().join("profile1");
        let profile2_dir = temp.path().join("profile2");
        let unrelated_dir = temp.path().join("unrelated");
        std::fs::create_dir_all(&profile1_dir).unwrap();
        std::fs::create_dir_all(&profile2_dir).unwrap();
        std::fs::create_dir_all(&unrelated_dir).unwrap();

        let p1_db = profile1_dir.join("state.db");
        let p2_db = profile2_dir.join("state.db");
        let unrel_db = unrelated_dir.join("state.db");

        let make_db = |path: &Path, sid: &str, user: &str, assistant: &str| {
            let conn = rusqlite::Connection::open(path).unwrap();
            conn.execute_batch(
                "CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    tool_call_id TEXT DEFAULT '',
                    tool_calls TEXT DEFAULT '',
                    tool_name TEXT DEFAULT '',
                    timestamp REAL DEFAULT 0.0,
                    token_count INTEGER DEFAULT 0,
                    active INTEGER DEFAULT 1
                );",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', ?, 1700000000.0)",
                rusqlite::params![sid, user],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'assistant', ?, 1700000001.0)",
                rusqlite::params![sid, assistant],
            )
            .unwrap();
        };

        make_db(&p1_db, "ses_p1", "Profile 1 question", "Profile 1 answer");
        make_db(&p2_db, "ses_p2", "Profile 2 question", "Profile 2 answer");
        make_db(&unrel_db, "ses_unrel", "Ambient question", "Ambient answer");

        // Record two Hermes instances backed by different profile DBs
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES (?, 'hermes', ?, ?, 'active', 1000.0)",
                rusqlite::params!["herm_a", "ses_p1", p1_db.to_str().unwrap()],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES (?, 'hermes', ?, ?, 'active', 1000.0)",
                rusqlite::params!["herm_b", "ses_p2", p2_db.to_str().unwrap()],
            )
            .unwrap();

        // Caller runs in an unrelated ambient environment pointing to unrelated_dir
        unsafe {
            std::env::set_var("HERMES_HOME", &unrelated_dir);
        }

        // Resolving herm_a must read profile1, never ambient or profile2
        let rendered_a =
            render_instance_transcript_with_options(&db, "herm_a", None, 10, false, false, false)
                .unwrap();
        assert!(rendered_a.contains("Profile 1 question"));
        assert!(rendered_a.contains("Profile 1 answer"));
        assert!(!rendered_a.contains("Profile 2"));
        assert!(!rendered_a.contains("Ambient"));

        // Resolving herm_b must read profile2, never ambient or profile1
        let rendered_b =
            render_instance_transcript_with_options(&db, "herm_b", None, 10, false, false, false)
                .unwrap();
        assert!(rendered_b.contains("Profile 2 question"));
        assert!(rendered_b.contains("Profile 2 answer"));
        assert!(!rendered_b.contains("Profile 1"));
        assert!(!rendered_b.contains("Ambient"));

        // Missing transcript path fails safely with error, never falling back to ambient
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES (?, 'hermes', ?, ?, 'active', 1000.0)",
                rusqlite::params!["herm_missing", "ses_missing", "/nonexistent/hermes/state.db"],
            )
            .unwrap();
        let err_missing = render_instance_transcript_with_options(
            &db,
            "herm_missing",
            None,
            10,
            false,
            false,
            false,
        )
        .unwrap_err();
        assert!(err_missing.contains("Transcript not found") || err_missing.contains("not found"));
        assert!(!err_missing.contains("Ambient"));

        // Empty transcript path fails safely with no-transcript error, never falling back to ambient
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES (?, 'hermes', ?, '', 'active', 1000.0)",
                rusqlite::params!["herm_empty", "ses_empty"],
            )
            .unwrap();
        let err_empty = render_instance_transcript_with_options(
            &db,
            "herm_empty",
            None,
            10,
            false,
            false,
            false,
        )
        .unwrap_err();
        assert!(err_empty.contains("No model transcript is registered"));
        assert!(!err_empty.contains("Ambient"));
    }

    #[test]
    #[serial_test::serial]
    fn test_hermes_search_isolation_across_profiles() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let db_file = temp.path().join("hcom.db");
        let db = HcomDb::open_raw(&db_file).unwrap();
        db.init_db().unwrap();

        let profile1_dir = temp.path().join("prof1");
        let profile2_dir = temp.path().join("prof2");
        let absent_ambient_dir = temp.path().join("nonexistent_ambient");
        std::fs::create_dir_all(&profile1_dir).unwrap();
        std::fs::create_dir_all(&profile2_dir).unwrap();

        let p1_db = profile1_dir.join("state.db");
        let p2_db = profile2_dir.join("state.db");

        let make_hermes_db = |path: &Path, messages: &[(&str, &str)]| {
            let conn = rusqlite::Connection::open(path).unwrap();
            conn.execute_batch(
                "CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    tool_call_id TEXT DEFAULT '',
                    tool_calls TEXT DEFAULT '',
                    tool_name TEXT DEFAULT '',
                    timestamp REAL DEFAULT 0.0,
                    token_count INTEGER DEFAULT 0,
                    active INTEGER DEFAULT 1
                );",
            )
            .unwrap();
            for (sid, text) in messages {
                conn.execute(
                    "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', ?, 1700000000.0)",
                    rusqlite::params![sid, text],
                )
                .unwrap();
            }
        };

        make_hermes_db(&p1_db, &[("ses_1", "unique_keyword_alpha in profile one")]);
        make_hermes_db(&p2_db, &[("ses_2", "unique_keyword_beta in profile two")]);

        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES (?, 'hermes', ?, ?, 'active', 1000.0)",
                rusqlite::params!["inst_alpha", "ses_1", p1_db.to_str().unwrap()],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES (?, 'hermes', ?, ?, 'active', 1000.0)",
                rusqlite::params!["inst_beta", "ses_2", p2_db.to_str().unwrap()],
            )
            .unwrap();

        // Caller runs with absent ambient HERMES_HOME (pointing to non-existent directory)
        unsafe {
            std::env::set_var("HERMES_HOME", &absent_ambient_dir);
        }
        assert!(transcript::database_search_path(Tool::Hermes).is_none());

        // Tracked search for alpha matches only inst_alpha
        let args_alpha = TranscriptSearchArgs {
            pattern: "unique_keyword_alpha".to_string(),
            live: false,
            all: false,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let results_alpha = collect_transcript_search_results(&db, &args_alpha, None).unwrap();
        assert_eq!(results_alpha.len(), 1);
        assert_eq!(results_alpha[0]["hcom_name"], "inst_alpha");
        assert!(
            results_alpha[0]["text"]
                .as_str()
                .unwrap()
                .contains("unique_keyword_alpha")
        );

        // Tracked search for beta matches only inst_beta
        let args_beta = TranscriptSearchArgs {
            pattern: "unique_keyword_beta".to_string(),
            live: false,
            all: false,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let results_beta = collect_transcript_search_results(&db, &args_beta, None).unwrap();
        assert_eq!(results_beta.len(), 1);
        assert_eq!(results_beta[0]["hcom_name"], "inst_beta");
        assert!(
            results_beta[0]["text"]
                .as_str()
                .unwrap()
                .contains("unique_keyword_beta")
        );

        // --all search with absent ambient DB still includes and correlates both profile DBs
        let args_all = TranscriptSearchArgs {
            pattern: "unique_keyword".to_string(),
            live: false,
            all: true,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let results_all = collect_transcript_search_results(&db, &args_all, None).unwrap();
        assert_eq!(results_all.len(), 2);
        let names: Vec<&str> = results_all
            .iter()
            .filter_map(|r| r["hcom_name"].as_str())
            .collect();
        assert!(names.contains(&"inst_alpha"));
        assert!(names.contains(&"inst_beta"));
    }

    #[test]
    #[serial_test::serial]
    fn test_hermes_search_multiple_sessions_in_one_profile_and_low_limit() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let db_file = temp.path().join("hcom.db");
        let db = HcomDb::open_raw(&db_file).unwrap();
        db.init_db().unwrap();

        let profile_dir = temp.path().join("profile");
        std::fs::create_dir_all(&profile_dir).unwrap();
        let shared_db = profile_dir.join("state.db");

        let conn = rusqlite::Connection::open(&shared_db).unwrap();
        conn.execute_batch(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                tool_call_id TEXT DEFAULT '',
                tool_calls TEXT DEFAULT '',
                tool_name TEXT DEFAULT '',
                timestamp REAL DEFAULT 0.0,
                token_count INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1
            );",
        )
        .unwrap();

        // ses_first is inserted first (lower ID) with the matching keyword
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('ses_first', 'user', 'matched_keyword in first session', 1700000000.0)",
            [],
        )
        .unwrap();
        // ses_second is inserted second with the matching keyword
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('ses_second', 'user', 'matched_keyword in second session', 1700000001.0)",
            [],
        )
        .unwrap();

        // Instance herm_target is bound to ses_second (not ses_first)
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES (?, 'hermes', ?, ?, 'active', 1000.0)",
                rusqlite::params!["herm_target", "ses_second", shared_db.to_str().unwrap()],
            )
            .unwrap();

        // Instance herm_no_sid has no session ID (empty or null)
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES (?, 'hermes', '', ?, 'active', 1000.0)",
                rusqlite::params!["herm_no_sid", shared_db.to_str().unwrap()],
            )
            .unwrap();

        // Ambient HERMES_HOME points to non-existent directory
        unsafe {
            std::env::set_var("HERMES_HOME", temp.path().join("absent_ambient"));
        }

        // Search with limit 1: Must find herm_target for ses_second despite ses_first preceding it in SQLite
        let args = TranscriptSearchArgs {
            pattern: "matched_keyword".to_string(),
            live: false,
            all: false,
            limit: 1,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let results = collect_transcript_search_results(&db, &args, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["hcom_name"], "herm_target");
        assert!(
            results[0]["text"]
                .as_str()
                .unwrap()
                .contains("second session")
        );

        // Search with limit 10: herm_no_sid must be safely skipped, never misattributed
        let args_all_limit = TranscriptSearchArgs {
            pattern: "matched_keyword".to_string(),
            live: false,
            all: false,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let results_all = collect_transcript_search_results(&db, &args_all_limit, None).unwrap();
        assert_eq!(results_all.len(), 1);
        assert_eq!(results_all[0]["hcom_name"], "herm_target");
        assert!(!results_all.iter().any(|r| r["hcom_name"] == "herm_no_sid"));
    }

    #[test]
    #[serial_test::serial]
    fn test_hermes_search_stopped_snapshot_session_isolation() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let db_file = temp.path().join("hcom.db");
        let db = HcomDb::open_raw(&db_file).unwrap();
        db.init_db().unwrap();

        let profile_dir = temp.path().join("prof_stopped");
        std::fs::create_dir_all(&profile_dir).unwrap();
        let stopped_db = profile_dir.join("state.db");

        let conn = rusqlite::Connection::open(&stopped_db).unwrap();
        conn.execute_batch(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                tool_call_id TEXT DEFAULT '',
                tool_calls TEXT DEFAULT '',
                tool_name TEXT DEFAULT '',
                timestamp REAL DEFAULT 0.0,
                token_count INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1
            );",
        )
        .unwrap();

        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('ses_stopped', 'user', 'unique_stopped_phrase from stopped agent', 1700000000.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES ('ses_other', 'user', 'unique_stopped_phrase from other agent', 1700000001.0)",
            [],
        )
        .unwrap();

        // Stopped instance with snapshot.session_id
        let data_valid = serde_json::json!({
            "action": "stopped",
            "snapshot": {
                "transcript_path": stopped_db.to_str().unwrap(),
                "session_id": "ses_stopped",
                "tool": "hermes"
            }
        });
        db.conn()
            .execute(
                "INSERT INTO events (timestamp, type, instance, data) VALUES (1700000000.0, 'life', 'inst_stopped', ?)",
                rusqlite::params![data_valid.to_string()],
            )
            .unwrap();

        // Stopped instance with snapshot but no session_id
        let data_no_sid = serde_json::json!({
            "action": "stopped",
            "snapshot": {
                "transcript_path": stopped_db.to_str().unwrap(),
                "tool": "hermes"
            }
        });
        db.conn()
            .execute(
                "INSERT INTO events (timestamp, type, instance, data) VALUES (1700000001.0, 'life', 'inst_stopped_no_sid', ?)",
                rusqlite::params![data_no_sid.to_string()],
            )
            .unwrap();

        unsafe {
            std::env::set_var("HERMES_HOME", temp.path().join("absent_ambient"));
        }

        // Tracked search with live=false: inst_stopped matches ses_stopped, inst_stopped_no_sid is safely skipped
        let args = TranscriptSearchArgs {
            pattern: "unique_stopped_phrase".to_string(),
            live: false,
            all: false,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let results = collect_transcript_search_results(&db, &args, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["hcom_name"], "inst_stopped");
        assert!(
            results[0]["text"]
                .as_str()
                .unwrap()
                .contains("from stopped agent")
        );

        // Live search with live=true: stopped instance is ignored
        let args_live = TranscriptSearchArgs {
            pattern: "unique_stopped_phrase".to_string(),
            live: true,
            all: false,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let results_live = collect_transcript_search_results(&db, &args_live, None).unwrap();
        assert!(results_live.is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn test_hermes_tracked_search_propagates_invalid_regex_error() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let db_file = temp.path().join("hcom.db");
        let db = HcomDb::open_raw(&db_file).unwrap();
        db.init_db().unwrap();

        let profile_dir = temp.path().join("profile");
        std::fs::create_dir_all(&profile_dir).unwrap();
        let hermes_db = profile_dir.join("state.db");
        let conn = rusqlite::Connection::open(&hermes_db).unwrap();
        conn.execute_batch(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                active INTEGER DEFAULT 1
            );",
        )
        .unwrap();

        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES ('herm_inst', 'hermes', 'ses_test', ?, 'active', 1000.0)",
                rusqlite::params![hermes_db.to_str().unwrap()],
            )
            .unwrap();

        // Tracked search with invalid regex must propagate error, not return Ok(empty)
        let args = TranscriptSearchArgs {
            pattern: "[invalid_regex".to_string(),
            live: false,
            all: false,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let err = collect_transcript_search_results(&db, &args, None).unwrap_err();
        assert!(
            err.contains("Invalid regex"),
            "tracked search error must propagate invalid regex: {err}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_hermes_tracked_search_propagates_db_failure_error() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let db_file = temp.path().join("hcom.db");
        let db = HcomDb::open_raw(&db_file).unwrap();
        db.init_db().unwrap();

        let corrupt_db = temp.path().join("corrupt_state.db");
        std::fs::write(&corrupt_db, b"not a valid sqlite file").unwrap();

        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES ('herm_corrupt', 'hermes', 'ses_test', ?, 'active', 1000.0)",
                rusqlite::params![corrupt_db.to_str().unwrap()],
            )
            .unwrap();

        let args = TranscriptSearchArgs {
            pattern: "some_pattern".to_string(),
            live: false,
            all: false,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let err = collect_transcript_search_results(&db, &args, None).unwrap_err();
        assert!(
            err.contains("Cannot open Hermes state DB") || err.contains("Query error"),
            "tracked search error must propagate DB failure: {err}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_hermes_all_search_propagates_invalid_regex_error() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let db_file = temp.path().join("hcom.db");
        let db = HcomDb::open_raw(&db_file).unwrap();
        db.init_db().unwrap();

        let profile_dir = temp.path().join("profile");
        std::fs::create_dir_all(&profile_dir).unwrap();
        let hermes_db = profile_dir.join("state.db");
        let conn = rusqlite::Connection::open(&hermes_db).unwrap();
        conn.execute_batch(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                active INTEGER DEFAULT 1
            );",
        )
        .unwrap();

        unsafe {
            std::env::set_var("HERMES_HOME", &profile_dir);
        }

        let args = TranscriptSearchArgs {
            pattern: "[invalid_regex".to_string(),
            live: false,
            all: true,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let err = collect_transcript_search_results(&db, &args, None).unwrap_err();
        assert!(
            err.contains("Invalid regex"),
            "--all search error must propagate invalid regex: {err}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_tracked_search_non_hermes_retains_pre_change_behavior() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let db_file = temp.path().join("hcom.db");
        let db = HcomDb::open_raw(&db_file).unwrap();
        db.init_db().unwrap();

        let transcript_file = temp.path().join("session.jsonl");
        std::fs::write(
            &transcript_file,
            "{\"type\":\"message\",\"content\":\"opencode unique pattern match\"}\n",
        )
        .unwrap();

        // Instance with tool 'opencode' pointing to a flat file
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES ('open_inst', 'opencode', 'ses_oc', ?, 'active', 1000.0)",
                rusqlite::params![transcript_file.to_str().unwrap()],
            )
            .unwrap();

        // Tracked search for non-Hermes must use ripgrep on the file, retaining pre-change behavior
        let args = TranscriptSearchArgs {
            pattern: "opencode unique pattern".to_string(),
            live: false,
            all: false,
            limit: 10,
            json: true,
            agent: Some("opencode".to_string()),
            exclude_self: false,
        };
        let results = collect_transcript_search_results(&db, &args, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["hcom_name"], "open_inst");
        assert!(
            results[0]["text"]
                .as_str()
                .unwrap()
                .contains("opencode unique pattern")
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_hermes_all_search_deduplicates_symlink_or_equivalent_paths() {
        let _guard = crate::hooks::test_helpers::EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let db_file = temp.path().join("hcom.db");
        let db = HcomDb::open_raw(&db_file).unwrap();
        db.init_db().unwrap();

        let profile_dir = temp.path().join("profile");
        std::fs::create_dir_all(&profile_dir).unwrap();
        let original_db = profile_dir.join("state.db");
        let conn = rusqlite::Connection::open(&original_db).unwrap();
        conn.execute_batch(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                active INTEGER DEFAULT 1
            );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO messages (session_id, role, content) VALUES ('ses_sym', 'user', 'dedup test message')",
            [],
        )
        .unwrap();

        // Create symlink to state.db in another directory
        let symlink_dir = temp.path().join("symlink_dir");
        std::fs::create_dir_all(&symlink_dir).unwrap();
        let symlinked_db = symlink_dir.join("state.db");
        std::os::unix::fs::symlink(&original_db, &symlinked_db).unwrap();

        // Instance A points to original_db with ses_orig, Instance B points to symlinked_db with ses_sym
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES ('inst_orig', 'hermes', 'ses_orig', ?, 'active', 1000.0)",
                rusqlite::params![original_db.to_str().unwrap()],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, session_id, transcript_path, status, created_at) VALUES ('inst_sym', 'hermes', 'ses_sym', ?, 'active', 1001.0)",
                rusqlite::params![symlinked_db.to_str().unwrap()],
            )
            .unwrap();

        // Set ambient HERMES_HOME to original profile_dir
        unsafe {
            std::env::set_var("HERMES_HOME", &profile_dir);
        }

        let args = TranscriptSearchArgs {
            pattern: "dedup test message".to_string(),
            live: false,
            all: true,
            limit: 10,
            json: true,
            agent: Some("hermes".to_string()),
            exclude_self: false,
        };
        let results = collect_transcript_search_results(&db, &args, None).unwrap();
        // The DB must be searched once, not duplicated across the symlink
        assert_eq!(results.len(), 1);
    }
}
