//! `hcom send` command — send messages to hcom instances.

use std::io::{IsTerminal, Read as IoRead};

use crate::cli_context::InlineBatch;
use crate::db::HcomDb;
use crate::db::subscriptions::create_request_watches;
use crate::identity;
use crate::messages::{
    InstanceInfo, MessageEnvelope, MessageScope, compute_scope, resolve_targets,
    sender_instance_key, should_deliver_message, validate_intent, validate_message,
};
use crate::shared::{
    CommandContext, SENDER, SenderIdentity, SenderKind, is_inside_ai_tool, status_icon,
};

pub(crate) const RECIPIENT_FEEDBACK_PREFIX: &str = "Sent to:";

const SEND_AFTER_HELP: &str = "\
Target matching:
    @luna                          exact base name
    @api-luna                      exact full name
    @api-                          all local agents with exact tag 'api'
    @luna:BOXE                     exact or uniquely prefixed remote agent
  Partial local names are rejected to avoid accidental fan-out.

Inline bundle (attach structured context):
    --title <text>                 Create and attach bundle inline
    --description <text>           Bundle description (required with --title)
    --events <ids>                 Event IDs/ranges: 1,2,5-10
    --files <paths>                Comma-separated file paths
    --transcript <ranges>          Format: 3-14:normal,6:full,22-30:detailed
    --extends <id>                 Parent bundle (optional)
  See 'hcom bundle --help' for bundle details

Examples:
    hcom send @luna -- Hello there!
    hcom send @luna @nova --intent request -- Can you help?
    hcom send -- Broadcast message to everyone
    echo 'Complex message' | hcom send @luna
    hcom send @luna <<'EOF'
    Multi-line message with special chars
    EOF";

/// Parse positional arg: accept both @targets and bare text.
/// Bare text (no @ prefix) is accepted by clap and separated later in cmd_send.
fn parse_positional(s: &str) -> Result<String, String> {
    if s == "@" {
        Err("Empty target '@' is not allowed".to_string())
    } else {
        Ok(s.to_string())
    }
}

/// Parsed arguments for `hcom send`.
#[derive(clap::Parser, Debug)]
#[command(
    name = "send",
    about = "Send a message to agents",
    after_help = SEND_AFTER_HELP,
)]
pub struct SendArgs {
    /// Positional args: @targets and/or bare message text (backward compat)
    #[arg(value_parser = parse_positional)]
    pub positionals: Vec<String>,

    /// Message text (after --)
    #[arg(last = true)]
    pub message: Vec<String>,

    // ── Message source ──
    /// Read message from stdin
    #[arg(long)]
    pub stdin: bool,

    /// Read message from file
    #[arg(long)]
    pub file: Option<String>,

    /// Read message from base64-encoded string
    #[arg(long)]
    pub base64: Option<String>,

    // ── Envelope ──
    /// Message intent (request|inform|ack)
    #[arg(long)]
    pub intent: Option<String>,

    /// Reply to event ID (42 or 42:BOXE)
    #[arg(long)]
    pub reply_to: Option<String>,

    /// Threaded routing: seed recipients once, then reuse thread members
    #[arg(long)]
    pub thread: Option<String>,

    /// Explicitly allow delivery to every available agent
    #[arg(long)]
    pub broadcast: bool,

    // ── Sender ──
    /// External sender identity
    #[arg(long)]
    pub from: Option<String>,

    /// Shorthand for --from bigboss
    #[arg(short = 'b')]
    pub bigboss: bool,

    /// No output; leaves mail unread
    #[arg(long)]
    pub quiet: bool,

    /// JSON receipt; leaves mail unread
    #[arg(long)]
    pub json: bool,

    // ── Inline bundle ──
    /// Bundle title (creates inline bundle)
    #[arg(long)]
    pub title: Option<String>,

    /// Bundle description
    #[arg(long)]
    pub description: Option<String>,

    /// Bundle event IDs/ranges
    #[arg(long)]
    pub events: Option<String>,

    /// Bundle file paths (comma-separated)
    #[arg(long, rename_all = "verbatim")]
    pub files: Option<String>,

    /// Bundle transcript ranges
    #[arg(long)]
    pub transcript: Option<String>,

    /// Parent bundle ID
    #[arg(long)]
    pub extends: Option<String>,

    /// Set by router: whether `--` was present in raw argv.
    /// Clap can't distinguish "no --" from "-- with no args", so the router sets this.
    #[arg(skip)]
    pub had_separator: bool,
}

impl SendArgs {
    /// Resolve the effective --from name (--from overrides -b).
    fn sender_name(&self) -> Option<String> {
        if let Some(ref name) = self.from {
            Some(name.clone())
        } else if self.bigboss {
            Some("bigboss".to_string())
        } else {
            None
        }
    }

    /// Whether a `--` separator was present in the raw argv.
    fn has_separator(&self) -> bool {
        self.had_separator
    }

    /// Build inline bundle data from flags, or None if no bundle flags present.
    fn build_bundle_data(&self) -> Result<Option<serde_json::Value>, String> {
        let has_any = self.title.is_some()
            || self.description.is_some()
            || self.events.is_some()
            || self.files.is_some()
            || self.transcript.is_some()
            || self.extends.is_some();

        if !has_any {
            return Ok(None);
        }

        let title = self.title.as_ref().ok_or_else(|| {
            let present: Vec<&str> = [
                self.description.as_ref().map(|_| "--description"),
                self.events.as_ref().map(|_| "--events"),
                self.files.as_ref().map(|_| "--files"),
                self.transcript.as_ref().map(|_| "--transcript"),
                self.extends.as_ref().map(|_| "--extends"),
            ]
            .into_iter()
            .flatten()
            .collect();
            format!(
                "Bundle flags require --title: found {} without --title",
                present.join(", ")
            )
        })?;

        let description = self
            .description
            .as_ref()
            .ok_or("--description is required when --title is present")?;

        use crate::core::bundles::parse_csv_list;
        let events = parse_csv_list(self.events.as_deref());
        let files = parse_csv_list(self.files.as_deref());
        let transcript = parse_csv_list(self.transcript.as_deref());

        let mut bundle = serde_json::json!({
            "title": title,
            "description": description,
            "refs": {
                "events": events,
                "files": files,
                "transcript": transcript,
            }
        });

        if let Some(ref ext) = self.extends {
            bundle
                .as_object_mut()
                .unwrap()
                .insert("extends".into(), serde_json::json!(ext));
        }

        Ok(Some(bundle))
    }
}

/// Get formatted recipient feedback showing who received the message.
///
/// `thread` is set when delivery was limited to that thread's members.
fn get_recipient_feedback(db: &HcomDb, delivered_to: &[String], thread: Option<&str>) -> String {
    if delivered_to.is_empty() {
        return match thread {
            Some(thread) => {
                format!("Sent to: {SENDER} only (no other members of thread '{thread}' are active)")
            }
            None => format!("Sent to: {SENDER} only (no other agents are active)"),
        };
    }
    if delivered_to.len() > 10 {
        return format!("Sent to {} agents", delivered_to.len());
    }

    let mut parts = Vec::new();
    for name in delivered_to {
        if let Ok(Some(data)) = db.get_instance_full(name) {
            let icon = status_icon(&data.status);
            let display = identity::get_display_name(db, name);
            parts.push(format!("{icon} {display}"));
        } else {
            parts.push(format!("◌ {name}"));
        }
    }
    format!("Sent to: {}", parts.join(", "))
}

struct ResolvedDelivery {
    original_scope: MessageScope,
    effective_scope: MessageScope,
    effective_mentions: Vec<String>,
    delivered_to: Vec<String>,
    is_thread_resolved: bool,
    sandbox_target: Option<SandboxTarget>,
}

enum SandboxTarget {
    Host(crate::sandbox::directory::Worker),
    Guest(crate::sandbox::guest_send::Target),
    Thread(crate::sandbox::conversation::Resolved),
}

fn deliverable_instances(db: &HcomDb) -> Result<Vec<InstanceInfo>, String> {
    let rows = db
        .conn()
        .prepare(
            "SELECT name, tag FROM instances
             WHERE status != 'stopped'
               AND status_context != 'launch_failed'
               AND NOT (status = 'inactive' AND status_context LIKE 'exit:%')",
        )
        .map_err(|e| format!("DB error: {e}"))?
        .query_map([], |row| {
            Ok(InstanceInfo {
                name: row.get::<_, String>(0)?,
                tag: row.get::<_, Option<String>>(1)?,
            })
        })
        .map_err(|e| format!("DB error: {e}"))?
        .filter_map(|r| r.ok())
        .collect::<Vec<_>>();
    Ok(rows)
}

fn resolve_delivery(
    db: &HcomDb,
    identity: &SenderIdentity,
    message: &str,
    envelope: Option<&MessageEnvelope>,
    explicit_targets: Option<&[String]>,
    broadcast: bool,
) -> Result<ResolvedDelivery, String> {
    if let Some(target) = crate::sandbox::guest_send::resolve(
        db,
        identity,
        explicit_targets,
        broadcast,
        envelope.and_then(|env| env.thread.as_deref()),
        envelope.and_then(|env| env.reply_to.as_ref()).is_some(),
    )
    .map_err(|error| format!("{error:#}"))?
    {
        let original_scope = if target.is_broadcast() {
            MessageScope::Broadcast
        } else {
            MessageScope::Mentions
        };
        return Ok(ResolvedDelivery {
            original_scope,
            effective_scope: original_scope,
            effective_mentions: Vec::new(),
            delivered_to: Vec::new(),
            is_thread_resolved: false,
            sandbox_target: Some(SandboxTarget::Guest(target)),
        });
    }
    // Deliverable agents: exclude session-stopped (exit:*) and launch_failed placeholders.
    // Adhoc instances use inactive:tool:* between commands — still @mentionable.
    let rows = deliverable_instances(db)?;
    let sandbox_names = crate::sandbox::directory::active_worker_names(db)
        .map_err(|error| format!("Failed to read sandbox directory: {error}"))?;
    let mut canonical_targets = Vec::new();
    let mut alias_worker = None;
    let explicit_targets = if let Some(targets) =
        explicit_targets.filter(|_| !sandbox_names.is_empty())
    {
        for target in targets {
            if !crate::identity::is_valid_base_name(&target.to_ascii_lowercase()) {
                canonical_targets.push(target.clone());
                continue;
            }
            match crate::sandbox::directory::resolve_host_alias(db, identity, target)
                .map_err(|error| format!("{error:#}"))?
            {
                Some(worker) => {
                    let (ordinary, _) = resolve_targets(std::slice::from_ref(target), &rows)?;
                    if !ordinary.is_empty() {
                        return Err(format!(
                            "ambiguous sandbox alias @{target}; use @alias.realm to target a sandbox"
                        ));
                    }
                    if alias_worker
                        .as_ref()
                        .is_some_and(|previous| previous != &worker)
                    {
                        return Err("Target one sandbox worker explicitly".into());
                    }
                    canonical_targets.push(worker.binding.worker.worker.clone());
                    alias_worker = Some(worker);
                }
                None => canonical_targets.push(target.clone()),
            }
        }
        Some(canonical_targets.as_slice())
    } else {
        explicit_targets
    };

    // Compute scope and routing. Thread-only sends keep their original message
    // semantics; membership only affects the delivery target set.
    let scope_result = compute_scope(message, &rows, explicit_targets)?;
    // A thread-only sandbox send resolves through the team's conversation
    // grants, never the ordinary name-only thread index. Collisions fail
    // closed; ordinary labels fall through unchanged.
    let sandbox_thread = if scope_result.scope == MessageScope::Broadcast
        && !broadcast
        && let Some(label) = envelope.and_then(|env| env.thread.as_deref())
    {
        crate::sandbox::conversation::resolve(db, identity, label)
            .map_err(|error| format!("{error:#}"))?
    } else {
        None
    };
    let thread_delivery_members = if sandbox_thread.is_some() {
        Vec::new()
    } else if let Some(thread) = envelope.and_then(|env| env.thread.as_deref()) {
        if scope_result.scope == MessageScope::Broadcast {
            let members = db.get_thread_members(thread);
            if members.is_empty() {
                return Err(format!(
                    "Thread '{thread}' has no members. Seed it with @mentions first."
                ));
            }
            members
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let is_thread_resolved = sandbox_thread.is_some() || !thread_delivery_members.is_empty();
    let effective_scope = if is_thread_resolved {
        MessageScope::Mentions
    } else {
        scope_result.scope
    };
    let effective_mentions = if sandbox_thread.is_some() {
        Vec::new()
    } else if thread_delivery_members.is_empty() {
        scope_result.mentions.clone()
    } else {
        thread_delivery_members.clone()
    };

    let scope_data = build_scope_data(identity, effective_scope, &effective_mentions);
    let sandbox_target = if let Some(resolved) = sandbox_thread {
        Some(SandboxTarget::Thread(resolved))
    } else if alias_worker.is_some() {
        alias_worker.map(SandboxTarget::Host)
    } else if let Some(target) = effective_mentions
        .iter()
        .find(|target| sandbox_names.contains(*target))
    {
        Some(SandboxTarget::Host(
            crate::sandbox::directory::current_worker(db.conn(), target)
                .map_err(|error| format!("Failed to read sandbox worker: {error}"))?
                .ok_or_else(|| "Sandbox worker is no longer enrolled".to_string())?,
        ))
    } else {
        None
    };
    if !matches!(sandbox_target, Some(SandboxTarget::Thread(_)))
        && sandbox_target.is_some()
        && (effective_mentions.len() != 1 || is_thread_resolved)
    {
        return Err("Target one sandbox worker explicitly".into());
    }
    let delivered_to = rows
        .iter()
        .filter(|inst| {
            !sandbox_names.contains(&inst.name)
                && should_deliver_message(&scope_data, &inst.name, &identity.name).unwrap_or(false)
        })
        .map(|inst| inst.name.clone())
        .collect();

    Ok(ResolvedDelivery {
        original_scope: scope_result.scope,
        effective_scope,
        effective_mentions,
        delivered_to,
        is_thread_resolved,
        sandbox_target,
    })
}

fn build_scope_data(
    identity: &SenderIdentity,
    scope: MessageScope,
    mentions: &[String],
) -> serde_json::Value {
    let mut scope_data = serde_json::json!({
        "scope": scope.as_str(),
    });
    if !mentions.is_empty() {
        scope_data["mentions"] = serde_json::json!(mentions);
    }
    if let Some(gid) = identity.group_id() {
        scope_data["group_id"] = serde_json::json!(gid);
    }
    scope_data
}

fn print_broadcast_preview(db: &HcomDb, delivered_to: &[String]) {
    let count = delivered_to.len();
    let names: Vec<String> = delivered_to
        .iter()
        .map(|name| {
            if let Ok(Some(data)) = db.get_instance_full(name) {
                let icon = status_icon(&data.status);
                let display = identity::get_display_name(db, name);
                format!("{icon} {display}")
            } else {
                format!("◌ {name}")
            }
        })
        .collect();
    let recipient_list = if count <= 12 {
        names.join(", ")
    } else {
        format!("{} ... (+{} more)", names[..10].join(", "), count - 10)
    };

    println!("\n== BROADCAST SEND PREVIEW ==");
    println!("This message has no @targets, so it would broadcast to {count} agents.");
    println!("Recipients:\n  {recipient_list}\n");
    println!("Did you mean to send this to everyone?");
    println!("Broadcasts can wake many terminals and spend many agents' context/tools.");
    println!("\nAdd --go after send and run again to proceed:");
    println!("  hcom send --go ...\n");
}

///
/// Validates message, computes scope, logs event, notifies all instances.
/// Returns the logged event ID and delivered_to list (base names).
pub fn send_message(
    db: &HcomDb,
    identity: &SenderIdentity,
    message: &str,
    envelope: Option<&MessageEnvelope>,
    explicit_targets: Option<&[String]>,
) -> Result<(i64, Vec<String>), String> {
    validate_message(message)?;

    let delivery = resolve_delivery(db, identity, message, envelope, explicit_targets, false)?;
    if delivery.sandbox_target.is_some() {
        return Err("Sandbox targets require the sandbox send path".into());
    }
    let scope_str = delivery.effective_scope.as_str();

    // Build event data
    let mut data = serde_json::json!({
        "from": identity.name,
        "sender_kind": match identity.kind {
            SenderKind::External => "external",
            SenderKind::Instance => "instance",
            SenderKind::System => "system",
        },
        "scope": scope_str,
        "text": message,
        "delivered_to": delivery.delivered_to.clone(),
    });
    if let Some(instance_data) = identity.instance_data.as_ref()
        && let Some(instance_key) = sender_instance_key(&identity.name, instance_data)
    {
        data["sender_instance_key"] = serde_json::json!(instance_key);
    }

    // Add scope extra data (mentions)
    if !delivery.effective_mentions.is_empty() {
        data["mentions"] = serde_json::json!(delivery.effective_mentions);
    }

    if let Some(env) = envelope {
        if let Some(intent) = &env.intent {
            data["intent"] = serde_json::json!(intent.as_str());
        }
        if let Some(reply_to) = &env.reply_to {
            data["reply_to"] = serde_json::json!(reply_to);
            // Resolve to local event ID
            if let Some(local_id) = resolve_reply_to_local(db, reply_to) {
                data["reply_to_local"] = serde_json::json!(local_id);

                // Ack-on-ack loop prevention
                if env.intent.as_ref().map(|i| i.as_str()) == Some("ack")
                    && let Some(parent_intent) = get_intent_from_event(db, local_id)
                {
                    if parent_intent == "ack" {
                        return Err("Ack-on-ack loop detected. Message blocked.".to_string());
                    }
                    if parent_intent == "inform" {
                        return Err("Cannot ack an inform - informational messages don't need acknowledgment.".to_string());
                    }
                }
            }
        }
        if let Some(thread) = &env.thread {
            data["thread"] = serde_json::json!(thread);
        }
        if let Some(bundle_id) = &env.bundle_id {
            data["bundle_id"] = serde_json::json!(bundle_id);
        }
    }

    // Determine routing instance (namespace isolation)
    let routing_instance = match identity.kind {
        SenderKind::External => format!("ext_{}", identity.name),
        SenderKind::System => format!("sys_{}", identity.name),
        SenderKind::Instance => identity.name.clone(),
    };

    // Log event to DB
    let event_id = db
        .log_event("message", &routing_instance, &data)
        .map_err(|e| format!("Failed to write message to database: {e}"))?;

    // Auto-create request-watch subscriptions for targeted requests
    if let Some(env) = envelope {
        if let Some(thread) = env.thread.as_deref() {
            db.add_thread_memberships(
                thread,
                matches!(identity.kind, SenderKind::Instance).then_some(identity.name.as_str()),
                &delivery.delivered_to,
            );
        }

        if env.intent.as_ref().map(|i| i.as_str()) == Some("request")
            && matches!(identity.kind, SenderKind::Instance)
            && delivery.effective_scope == MessageScope::Mentions
        {
            create_request_watches(db, &identity.name, event_id, &delivery.delivered_to);
        }
    }

    // Wake recipient endpoints. Targeted and thread-resolved sends wake only
    // their resolved local recipients — `wake` on an instance with no registered
    // endpoints is a no-op, so stopped/unavailable names stay safe. Explicit
    // broadcasts keep the system-wide fan-out. Event observer loops
    // (events_wait, events_stream, listen_filter) are woken across all instances
    // so they see the newly logged event.
    if delivery.effective_scope == MessageScope::Broadcast {
        crate::notify::wake_all(db);
    } else {
        for recipient in &delivery.delivered_to {
            crate::notify::wake(db, recipient, &[]);
        }
        crate::notify::wake_observers(db);
    }

    // Trigger relay push so remote devices see the message immediately
    crate::relay::trigger_push();

    Ok((event_id, delivery.delivered_to))
}

/// Resolve reply_to to local event ID. Returns None if not found or ambiguous.
fn resolve_reply_to_local(db: &HcomDb, reply_to: &str) -> Option<i64> {
    let reply_to = reply_to.trim();
    if reply_to.is_empty() {
        return None;
    }

    if let Some((origin_str, device_short)) = crate::relay::control::split_device_suffix(reply_to) {
        let origin_id: i64 = origin_str.parse().ok()?;

        let mut stmt = db
            .conn()
            .prepare(
                "SELECT id FROM events WHERE type = 'message' \
                 AND json_type(data, '$._relay.id') = 'integer' \
                 AND json_extract(data, '$._relay.id') = ? \
                 AND json_extract(data, '$._relay.short') = ? \
                 LIMIT 2",
            )
            .ok()?;

        let matching_ids: Vec<i64> = stmt
            .query_map(rusqlite::params![origin_id, device_short], |row| row.get(0))
            .ok()?
            .flatten()
            .collect();

        if matching_ids.len() == 1 {
            Some(matching_ids[0])
        } else {
            None
        }
    } else {
        let local_id: i64 = reply_to.parse().ok()?;
        let exists: bool = db
            .conn()
            .query_row(
                "SELECT 1 FROM events WHERE id = ? AND type = 'message'",
                rusqlite::params![local_id],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if exists { Some(local_id) } else { None }
    }
}

/// Get thread from an event (for --reply-to thread inheritance).
fn get_thread_from_event(db: &HcomDb, event_id: i64) -> Option<String> {
    db.conn()
        .query_row(
            "SELECT json_extract(data, '$.thread') FROM events WHERE id = ?",
            rusqlite::params![event_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
}

/// Get intent from an event (for ack-on-ack prevention).
fn get_intent_from_event(db: &HcomDb, event_id: i64) -> Option<String> {
    db.conn()
        .query_row(
            "SELECT json_extract(data, '$.intent') FROM events WHERE id = ?",
            rusqlite::params![event_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
}

/// Resolve message from one of 5 source modes.
/// Returns `(message_text, stripped_name_flag)` where `stripped_name_flag` is true
/// if a trailing `--name <agent>` token was silently stripped from the message.
fn resolve_message(
    args: &SendArgs,
    auto_sender_name: Option<&str>,
) -> Result<(String, bool), String> {
    let has_separator = args.has_separator();

    // Mutual exclusivity
    let source_count = [
        args.stdin,
        args.file.is_some(),
        args.base64.is_some(),
        has_separator,
    ]
    .iter()
    .filter(|&&x| x)
    .count();
    if source_count > 1 {
        return Err("Only one of --, --stdin, --file, --base64 can be used".to_string());
    }

    // 1. -- separator
    if has_separator {
        let stripped = auto_sender_name.is_some_and(|name| {
            args.message.len() >= 2
                && args.message[args.message.len() - 2] == "--name"
                && args.message.last().is_some_and(|value| value == name)
        });
        let message_tokens = if stripped {
            &args.message[..args.message.len() - 2]
        } else {
            &args.message
        };
        let text = message_tokens.join(" ");
        if text.is_empty() {
            return Err("No message after --".to_string());
        }
        return Ok((text, stripped));
    }

    // 2. --stdin
    if args.stdin {
        return read_stdin().map(|s| (s, false));
    }

    // 3. --file
    if let Some(ref path) = args.file {
        let resolved = if std::path::Path::new(path).is_absolute() {
            std::path::PathBuf::from(path)
        } else {
            std::env::current_dir().unwrap_or_default().join(path)
        };
        return match std::fs::read_to_string(&resolved) {
            Ok(content) if !content.is_empty() => Ok((content, false)),
            Ok(_) => Err(format!("File is empty: {path}")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(format!("File not found: {path}"))
            }
            Err(e) => Err(format!("Cannot read file: {e}")),
        };
    }

    // 4. --base64
    if let Some(ref b64) = args.base64 {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|_| "Invalid base64 encoding".to_string())?;
        let s =
            String::from_utf8(bytes).map_err(|_| "Base64 decoded to invalid UTF-8".to_string())?;
        if s.is_empty() {
            return Err("Base64 decoded to empty string".to_string());
        }
        return Ok((s, false));
    }

    // 5. Auto-pipe (stdin is a pipe, no explicit source)
    if !std::io::stdin().is_terminal() {
        return read_stdin().map(|s| (s, false));
    }

    // No message source found
    let targets_str = if args.positionals.is_empty() {
        "@target".to_string()
    } else {
        args.positionals
            .iter()
            .take(3)
            .map(|t| {
                if t.starts_with('@') {
                    t.clone()
                } else {
                    format!("@{t}")
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    };
    Err(format!(
        "No message provided.\nUse: hcom send {targets_str} -- your message\n Or: echo 'msg' | hcom send {targets_str}"
    ))
}

/// Read message from stdin pipe.
fn read_stdin() -> Result<String, String> {
    let mut buf = String::new();
    if std::io::stdin().read_to_string(&mut buf).is_ok() && !buf.is_empty() {
        Ok(buf)
    } else {
        Err("No input received on stdin".to_string())
    }
}

/// Process positional args without `--` separator.
/// Matches Python messaging.py behavior:
///   - Empty → ([], None)
///   - Single arg with `@` prefix and space → backward compat: entire text is message
///   - Mix of @targets and bare text → separate targets from message
///   - Pure @targets → targets only, no message
fn process_positionals(positionals: &[String]) -> (Vec<String>, Option<String>) {
    if positionals.is_empty() {
        return (vec![], None);
    }

    // Backward compat: single arg starting with @ and containing space
    // e.g. "@luna hi" → whole thing is message (compute_scope extracts @mentions)
    if positionals.len() == 1 && positionals[0].starts_with('@') && positionals[0].contains(' ') {
        return (vec![], Some(positionals[0].clone()));
    }

    // Separate @targets from bare text
    let mut targets = Vec::new();
    let mut remaining = Vec::new();

    for arg in positionals {
        if let Some(name) = arg.strip_prefix('@') {
            targets.push(name.to_string());
        } else {
            remaining.push(arg.clone());
        }
    }

    if remaining.len() > 1 {
        // Multiple non-@ args without -- separator → error
        // Return empty message to trigger "no message" error with helpful hint
        return (targets, None);
    }

    if remaining.len() == 1 {
        return (targets, Some(remaining[0].clone()));
    }

    (targets, None)
}

/// After a failed send, say which requested targets are stopped (and when) so
/// the sender can resume them instead of guessing at typos.
fn print_stopped_target_hints(db: &HcomDb, explicit_targets: &[String], message: &str) {
    let targets = if explicit_targets.is_empty() {
        crate::shared::constants::extract_mentions(message)
    } else {
        explicit_targets.to_vec()
    };
    for target in targets {
        if let Some(stopped) = identity::last_stopped(db, &target) {
            eprintln!(
                "  @{target} {} — resume: hcom r {}",
                stopped.summary(),
                stopped.display_name()
            );
        }
    }
}

/// Reject `hcom send hi` / `hcom send luna`: a lone bare word with no `@` and
/// no `--` is as likely a target missing its `@` as a broadcast, and guessing
/// broadcast interrupts every agent. Explicit broadcasts and seeded threads
/// bypass this diagnostic.
fn ambiguous_bare_word_error(db: &HcomDb, targets: &[String], word: &str) -> Option<String> {
    if !targets.is_empty() || word.split_whitespace().nth(1).is_some() {
        return None;
    }
    let mut msg = format!(
        "Error: '{word}' is ambiguous: a target needs '@', a broadcast needs '--broadcast'"
    );
    if identity::resolve_display_name_or_stopped(db, word).is_some() {
        msg.push_str(&format!(
            "\nDid you mean @{word}? hcom send @{word} -- <message>"
        ));
    } else {
        msg.push_str(&format!(
            "\n  Direct:    hcom send @name -- {word}\n  Broadcast: hcom send --broadcast -- {word}"
        ));
    }
    Some(msg)
}

/// Outcome of validating positional args on the no-separator send path.
#[derive(Debug, PartialEq)]
enum PositionalSend {
    /// `hcom send <agent> <message words...>`: the first bare word resolved to
    /// an agent, so it acts as an @target and the rest is the message.
    Targeted { target: String, message: String },
    /// Multiple bare words that cannot form one message under the current
    /// rules (bare-word send requires `@`, `--`, or exact agent shorthand).
    Ambiguous,
    /// Arguments that pass through to `process_positionals`.
    Other,
}

fn validate_positional_send(
    db: &HcomDb,
    positionals: &[String],
    allow_bare_target: bool,
) -> PositionalSend {
    // A caller using a seeded thread or explicit broadcast wants to send words,
    // not pick an agent. Preserve their arguments for normal processing rather
    // than interpreting the first message word as an agent name.
    if !allow_bare_target {
        return PositionalSend::Other;
    }
    let bare_count = positionals
        .iter()
        .filter(|arg| !arg.starts_with('@'))
        .count();
    if bare_count < 2 {
        return PositionalSend::Other;
    }
    if let Some(first) = positionals.first().filter(|arg| !arg.starts_with('@'))
        && let Some(target) = identity::resolve_display_name(db, first)
    {
        return PositionalSend::Targeted {
            target,
            message: positionals[1..].join(" "),
        };
    }
    PositionalSend::Ambiguous
}

/// Main entry point for `hcom send` command.
///
/// Returns exit code (0 = success, 1 = error).
pub fn cmd_send(db: &HcomDb, args: &SendArgs, ctx: Option<&CommandContext>) -> i32 {
    // ── Resolve --from name ──
    let from_name = args.sender_name();

    if let Some(ref name) = from_name {
        if name.is_empty() || name.len() > 50 {
            eprintln!("Error: Name too long ({} chars, max 50)", name.len());
            return 1;
        }
        if name.contains([
            '@', '|', '&', ';', '<', '>', '`', '$', '\'', '"', '\\', '\n', '\r',
        ]) {
            eprintln!("Error: Name contains invalid characters");
            return 1;
        }
    }

    // Guard: subagents cannot use --from/-b
    if from_name.is_some() {
        let actor_from_ctx = ctx.and_then(|c| c.identity.clone());
        let actor = actor_from_ctx
            .or_else(|| identity::resolve_identity(db, None, None, None, None, None).ok());
        match actor {
            Some(ref actor) if matches!(actor.kind, SenderKind::Instance) => {
                if let Some(ref data) = actor.instance_data
                    && data
                        .get("parent_name")
                        .and_then(|v| v.as_str())
                        .is_some_and(|s| !s.is_empty())
                {
                    eprintln!("Error: Subagents cannot use --from/-b (external sender spoofing)");
                    return 1;
                }
            }
            _ => {}
        }
    }

    let explicit_name = ctx.and_then(|c| c.explicit_name.as_deref());

    // ── Validate envelope flags ──
    let mut envelope = MessageEnvelope::default();

    if let Some(ref val) = args.intent {
        let val = val.to_lowercase();
        if let Err(e) = validate_intent(&val) {
            eprintln!("Error: {e}");
            return 1;
        }
        envelope.intent = val.parse().ok();
    }

    envelope.reply_to = args.reply_to.clone();

    if let Some(ref val) = args.thread {
        if val.len() > 64 {
            eprintln!("Error: Thread name too long ({} chars, max 64)", val.len());
            return 1;
        }
        if !val
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            eprintln!("Error: Thread name must be alphanumeric with hyphens/underscores");
            return 1;
        }
        envelope.thread = Some(val.clone());
    }

    // Ack requires reply_to
    if envelope.intent.as_ref().map(|i| i.as_str()) == Some("ack") && envelope.reply_to.is_none() {
        eprintln!("Error: Intent 'ack' requires --reply-to <id>");
        eprintln!("<id> is the number in received messages like [request #id]");
        return 1;
    }

    if let Some(ref reply_to) = envelope.reply_to {
        if let Some(local_id) = resolve_reply_to_local(db, reply_to) {
            if envelope.thread.is_none()
                && let Some(parent_thread) = get_thread_from_event(db, local_id)
            {
                envelope.thread = Some(parent_thread);
            }
        } else {
            eprintln!("Error: Invalid --reply-to: event not found or not a message");
            return 1;
        }
    }

    // ── Inline bundle ──
    let bundle_data = match args.build_bundle_data() {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Error: {e}");
            return 1;
        }
    };

    // ── Process positional args: separate @targets from bare message text ──
    // Matches Python behavior in messaging.py:
    //   - Single "@name message" (with space) → entire text is message, @mention parsed by compute_scope
    //   - Non-@ args → message text (broadcast)
    //   - Pure @targets → explicit targets
    let (effective_targets, compat_message) = if !args.has_separator()
        && !args.stdin
        && args.file.is_none()
        && args.base64.is_none()
    {
        match validate_positional_send(
            db,
            &args.positionals,
            !args.broadcast && args.thread.is_none(),
        ) {
            PositionalSend::Targeted { target, message } => (vec![target], Some(message)),
            PositionalSend::Ambiguous => {
                eprintln!("Error: ambiguous send — multiple bare words cannot form one message.");
                if let Some(first) = args.positionals.first().filter(|arg| !arg.starts_with('@'))
                    && let Some(stopped) = identity::resolve_display_name_or_stopped(db, first)
                {
                    eprintln!("Did you mean @{stopped}? hcom send @{stopped} -- <message>");
                    eprintln!("Resume with: hcom r {stopped}");
                } else {
                    eprintln!(
                        "Targets require @; put the message after '--' or quote it as one argument:"
                    );
                    eprintln!("  hcom send @<agent> -- your message");
                    eprintln!("  Or: echo 'your message' | hcom send @<agent>");
                }
                return 1;
            }
            PositionalSend::Other => {
                let (targets, message) = process_positionals(&args.positionals);
                if !args.broadcast
                    && args.thread.is_none()
                    && let Some(word) = message.as_deref()
                    && let Some(err) = ambiguous_bare_word_error(db, &targets, word)
                {
                    eprintln!("{err}");
                    return 1;
                }
                (targets, message)
            }
        }
    } else {
        // With -- separator or explicit source: validate @targets
        let mut validated = Vec::new();
        for arg in &args.positionals {
            if let Some(stripped) = arg.strip_prefix('@') {
                if stripped.is_empty() {
                    eprintln!("Error: Empty target '@' is not allowed");
                    return 1;
                }
                validated.push(stripped.to_string());
            } else {
                let mut msg = format!("Error: Unexpected argument '{arg}'");
                if arg.chars().all(|c| c.is_alphabetic()) && arg.len() <= 20 {
                    msg.push_str(&format!("\nDid you mean @{arg}? Targets require @"));
                }
                eprintln!("{msg}");
                return 1;
            }
        }
        (validated, None)
    };

    if args.broadcast && (args.thread.is_some() || !effective_targets.is_empty()) {
        eprintln!("Error: --broadcast cannot be combined with agent targets or --thread");
        return 1;
    }

    // ── Resolve message ──
    let (mut message, name_was_stripped) = if let Some(msg) = compat_message {
        (msg, false)
    } else {
        let auto_sender_name = if from_name.is_none() && explicit_name.is_none() {
            ctx.and_then(|c| c.identity.as_ref())
                .filter(|id| matches!(id.kind, SenderKind::Instance))
                .map(|id| id.name.as_str())
        } else {
            None
        };
        match resolve_message(args, auto_sender_name) {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("Error: {e}");
                return 1;
            }
        }
    };

    if let Err(err) = validate_message(&message) {
        eprintln!("Error: {err}");
        return 1;
    }

    // ── Resolve sender identity ──
    let sender_identity = if let Some(ref name) = from_name {
        SenderIdentity {
            kind: SenderKind::External,
            name: name.clone(),
            instance_data: None,
            session_id: None,
        }
    } else if let Some(id) = ctx.and_then(|c| c.identity.as_ref()) {
        id.clone()
    } else if let Some(name) = explicit_name {
        match identity::resolve_identity(db, Some(name), None, None, None, None) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("Error: {e}");
                return 1;
            }
        }
    } else {
        match identity::resolve_identity(db, None, None, None, None, None) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("Error: {e}");
                return 1;
            }
        }
    };

    let targets_to_pass: Option<&[String]> =
        if args.has_separator() || !effective_targets.is_empty() || args.broadcast {
            Some(&effective_targets)
        } else {
            None
        };

    let preview_has_envelope =
        envelope.intent.is_some() || envelope.reply_to.is_some() || envelope.thread.is_some();

    let preview_delivery = match resolve_delivery(
        db,
        &sender_identity,
        &message,
        if preview_has_envelope {
            Some(&envelope)
        } else {
            None
        },
        targets_to_pass,
        args.broadcast,
    ) {
        Ok(delivery) => delivery,
        Err(e) => {
            eprintln!("Error: {e}");
            print_stopped_target_hints(db, &effective_targets, &message);
            return 1;
        }
    };

    // Self-delivery is always filtered out, so an all-self target list would
    // otherwise "succeed" with no recipients.
    if !matches!(
        preview_delivery.sandbox_target,
        Some(SandboxTarget::Guest(_))
    ) && !effective_targets.is_empty()
        && effective_targets.iter().all(|t| {
            // Routing treats bigboss:DEVICE as bigboss (device-agnostic identity).
            let base = match t.split_once(':') {
                Some((base, _)) if base == SENDER => base,
                _ => t.as_str(),
            };
            identity::resolve_display_name(db, base)
                .as_deref()
                .unwrap_or(base)
                == sender_identity.name
        })
    {
        eprintln!(
            "Error: @{} is you ({}); agents don't receive their own messages",
            effective_targets[0], sender_identity.name
        );
        return 1;
    }

    if preview_delivery.original_scope == MessageScope::Broadcast
        && !preview_delivery.is_thread_resolved
        && !args.broadcast
    {
        eprintln!("Error: Refusing implicit broadcast: no explicit recipient was resolved.");
        eprintln!("Target an agent with @name, reuse a seeded --thread, or add --broadcast.");
        return 1;
    }
    if args.broadcast
        && (preview_delivery.original_scope != MessageScope::Broadcast
            || preview_delivery.is_thread_resolved)
    {
        eprintln!("Error: --broadcast cannot be combined with agent targets or --thread");
        return 1;
    }

    if is_inside_ai_tool()
        && !ctx.map(|c| c.go).unwrap_or(false)
        && preview_delivery.original_scope == MessageScope::Broadcast
        && !preview_delivery.is_thread_resolved
        && preview_delivery.delivered_to.len() > 3
    {
        print_broadcast_preview(db, &preview_delivery.delivered_to);
        return 1;
    }

    // ── Create bundle event if inline flags provided ──
    if preview_delivery.sandbox_target.is_some() && bundle_data.is_some() {
        eprintln!("Error: Sandbox sends do not support inline bundles");
        return 1;
    }
    if let Some(mut bundle) = bundle_data {
        if let Err(e) = crate::core::bundles::validate_bundle(&mut bundle) {
            eprintln!("Error: {e}");
            return 1;
        }

        let bundle_instance = match sender_identity.kind {
            SenderKind::External => format!("ext_{}", sender_identity.name),
            SenderKind::System => format!("sys_{}", sender_identity.name),
            SenderKind::Instance => sender_identity.name.clone(),
        };

        match crate::core::bundles::create_bundle_event(
            &mut bundle,
            &bundle_instance,
            Some(&sender_identity.name),
            db,
        ) {
            Ok(bundle_id) => {
                crate::relay::worker::try_spawn_worker();
                envelope.bundle_id = Some(bundle_id.clone());

                // Append bundle summary text to message
                let refs = bundle.get("refs").cloned().unwrap_or(serde_json::json!({}));
                let events = refs
                    .get("events")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().or(v.as_i64().map(|_| "")).or(Some("")))
                            .map(|s| s.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                let files = refs
                    .get("files")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                let transcript = refs
                    .get("transcript")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| {
                                if let Some(obj) = v.as_object() {
                                    Some(format!(
                                        "{}:{}",
                                        obj.get("range").and_then(|r| r.as_str()).unwrap_or(""),
                                        obj.get("detail").and_then(|d| d.as_str()).unwrap_or("")
                                    ))
                                } else {
                                    v.as_str().map(|s| s.to_string())
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();

                let title = bundle.get("title").and_then(|v| v.as_str()).unwrap_or("");
                let description = bundle
                    .get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let extends = bundle.get("extends").and_then(|v| v.as_str());

                let mut bundle_lines = vec![
                    format!("[Bundle {bundle_id}]"),
                    format!("Title: {title}"),
                    format!("Description: {description}"),
                    "Refs:".to_string(),
                    format!("  events: {events}"),
                    format!("  files: {files}"),
                    format!("  transcript: {transcript}"),
                ];
                if let Some(ext) = extends {
                    bundle_lines.push(format!("Extends: {ext}"));
                }
                bundle_lines.push(String::new());
                bundle_lines.push("View bundle:".to_string());
                bundle_lines.push(format!("  hcom bundle cat {bundle_id}"));

                message = format!("{}\n\n{}", message.trim_end(), bundle_lines.join("\n"));
            }
            Err(e) => {
                eprintln!("Error: {e}");
                return 1;
            }
        }
    }

    // ── Send message ──
    let has_envelope = envelope.intent.is_some()
        || envelope.reply_to.is_some()
        || envelope.thread.is_some()
        || envelope.bundle_id.is_some();

    let queued = if let Some(target) = &preview_delivery.sandbox_target {
        let reply_event = envelope
            .reply_to
            .as_deref()
            .and_then(|reply| resolve_reply_to_local(db, reply));
        let result = match target {
            SandboxTarget::Host(worker) => crate::sandbox::outgoing::queue(
                db,
                &sender_identity,
                worker,
                &message,
                &envelope,
                reply_event,
            ),
            SandboxTarget::Guest(peer) => crate::sandbox::guest_send::queue(
                db,
                &sender_identity,
                peer,
                &message,
                &envelope,
                reply_event,
            ),
            SandboxTarget::Thread(resolved) => crate::sandbox::conversation::queue(
                db,
                &sender_identity,
                resolved,
                &message,
                &envelope,
                reply_event,
            ),
        };
        match result {
            Ok(queued) => Some(queued),
            Err(error) => {
                eprintln!("Error: {error}");
                return 1;
            }
        }
    } else {
        None
    };
    let (event_id, delivered_to) = if let Some(queued) = &queued {
        (queued.event_id, Vec::new())
    } else {
        match send_message(
            db,
            &sender_identity,
            &message,
            if has_envelope { Some(&envelope) } else { None },
            targets_to_pass,
        ) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Error: {e}");
                return 1;
            }
        }
    };
    // From here send owns the receiver's inline delivery: it delivers the batch
    // or deliberately leaves it unread (--quiet, --json, failed write). A send
    // that failed earlier leaves it to the router.
    crate::cli_context::claim_inline_delivery();

    // ── Feedback ──
    if args.json {
        let mut out = serde_json::json!({
            "event_id": event_id,
            "delivered_to": delivered_to,
        });
        if let Some(queued) = &queued {
            out["queued_to"] = serde_json::json!(queued.queued_to);
            if queued.broadcast {
                out["scope"] = serde_json::json!("broadcast");
            }
            out["forwarding"] = serde_json::json!(queued.forwarding);
            if let Some(reason) = &queued.forwarding_reason {
                out["forwarding_reason"] = serde_json::json!(reason);
            }
        }
        println!("{}", serde_json::to_string(&out).unwrap());
        if queued.is_none() {
            crate::relay::worker::try_spawn_worker();
        }
        return 0;
    }

    if args.quiet {
        if queued.is_none() {
            crate::relay::worker::try_spawn_worker();
        }
        return 0;
    }

    let feedback = if let Some(queued) = &queued {
        let mut feedback = format!(
            "Queued for: {} (forwarding {})",
            queued.display_target, queued.forwarding
        );
        if let Some(reason) = &queued.forwarding_reason {
            feedback.push_str(&format!("\n{reason}"));
        }
        feedback
    } else {
        get_recipient_feedback(
            db,
            &delivered_to,
            envelope
                .thread
                .as_deref()
                .filter(|_| preview_delivery.is_thread_resolved),
        )
    };

    // Use the same adhoc-only receive policy as other commands. With --from,
    // the invoking instance receives while the outgoing author stays external.
    let receiver = ctx
        .and_then(crate::cli_context::inline_receiver)
        .map(|actor| (actor, !matches!(sender_identity.kind, SenderKind::Instance)));
    let batch = receiver.and_then(|(r, _)| InlineBatch::take(db, &r.name));
    let noted_remaining = batch.as_ref().is_some_and(|b| b.remaining > 0);
    if let (Some((receiver, set_status)), Some(batch)) = (receiver, batch) {
        let subagent_names: std::collections::HashSet<String> = db
            .conn()
            .prepare("SELECT name FROM instances WHERE parent_name = ?")
            .ok()
            .map(|mut stmt| {
                stmt.query_map(rusqlite::params![&receiver.name], |row| row.get(0))
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.ok())
                    .collect()
            })
            .unwrap_or_default();
        let (sub_msgs, main_msgs): (Vec<_>, Vec<_>) = batch
            .messages
            .iter()
            .partition(|msg| subagent_names.contains(&msg.from));

        let mut output = feedback.clone();
        if !main_msgs.is_empty() {
            output.push_str(&format!(
                "\n{}",
                format_messages_for_hook(db, &main_msgs, &receiver.name)
            ));
        }
        if !sub_msgs.is_empty() {
            output.push_str(&format!(
                "\n[Subagent messages]\n{}",
                format_messages_for_hook(db, &sub_msgs, &receiver.name)
            ));
        }
        output.push('\n');
        output.push_str(&batch.remaining_note(&receiver.name));
        if let Err(e) = batch.emit(db, &receiver.name, &output, set_status) {
            if queued.is_some() {
                eprintln!("Message queued, but {e}");
            } else {
                eprintln!("Message sent, but {e}");
                crate::relay::worker::try_spawn_worker();
            }
            return 1;
        }
    } else {
        println!("{feedback}");
    }

    // ── Trailing --name hint ──
    if name_was_stripped {
        let sender_name = &sender_identity.name;
        println!();
        println!(
            "[hcom] Note: '--name {sender_name}' was stripped from the end of your message body."
        );
        println!("  Correct syntax (--name goes BEFORE --):");
        println!("    hcom send --name {sender_name} @target -- your message");
        println!("  To send '--name {sender_name}' as literal text, don't put it at the very end.");
    }

    // Messages that arrived after the batch was taken are not in the output or
    // the remaining note. Say so, or an adhoc caller won't know until its next command.
    if let Some((receiver, _)) = receiver
        && !noted_remaining
        && !db.get_unread_messages(&receiver.name).is_empty()
    {
        println!(
            "[hcom] new message(s) arrived — run: hcom listen --name {}",
            receiver.name
        );
    }

    // Show intent tip
    if let Some(ref intent) = envelope.intent
        && matches!(sender_identity.kind, SenderKind::Instance)
    {
        let tip_key = format!("send:intent:{}", intent.as_str());
        crate::core::tips::maybe_show_tip(db, &sender_identity.name, &tip_key, false);
    }

    if queued.is_none() {
        crate::relay::worker::try_spawn_worker();
    }

    0
}

/// Format messages for hook display (no ANSI).
fn format_messages_for_hook(
    db: &HcomDb,
    messages: &[&crate::db::Message],
    instance_name: &str,
) -> String {
    let recipient_display = identity::get_display_name(db, instance_name);

    if messages.len() == 1 {
        let msg = messages[0];
        let sender_display = identity::get_display_name(db, &msg.from);
        let prefix = crate::cli_context::format_envelope_prefix(
            msg.intent.as_deref(),
            msg.thread.as_deref(),
            msg.reply_id().as_deref(),
        );
        format!(
            "{prefix} {sender_display} → {recipient_display}: {}",
            msg.text
        )
    } else {
        let parts: Vec<String> = messages
            .iter()
            .map(|msg| {
                let sender_display = identity::get_display_name(db, &msg.from);
                let prefix = crate::cli_context::format_envelope_prefix(
                    msg.intent.as_deref(),
                    msg.thread.as_deref(),
                    msg.reply_id().as_deref(),
                );
                format!(
                    "{prefix} {sender_display} → {recipient_display}: {}",
                    msg.text
                )
            })
            .collect();
        format!("[{} new messages] | {}", parts.len(), parts.join(" | "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serial_test::serial;
    use std::path::PathBuf;

    type TestEnv = (
        tempfile::TempDir,
        PathBuf,
        PathBuf,
        crate::hooks::test_helpers::EnvGuard,
    );

    #[test]
    fn parse_basic_send() {
        let args = SendArgs::try_parse_from(["send", "@luna", "--", "hello", "there"]).unwrap();
        assert_eq!(args.positionals, vec!["@luna"]);
        assert_eq!(args.message, vec!["hello", "there"]);
    }

    #[test]
    fn parse_multiple_targets() {
        let args = SendArgs::try_parse_from(["send", "@luna", "@nova", "--", "hello"]).unwrap();
        assert_eq!(args.positionals, vec!["@luna", "@nova"]);
        assert_eq!(args.message, vec!["hello"]);
    }

    #[test]
    fn parse_broadcast() {
        let args = SendArgs::try_parse_from(["send", "--", "broadcast", "msg"]).unwrap();
        assert!(args.positionals.is_empty());
        assert_eq!(args.message, vec!["broadcast", "msg"]);
    }

    #[test]
    fn parse_with_intent_flag() {
        let args =
            SendArgs::try_parse_from(["send", "--intent", "request", "@luna", "--", "hello"])
                .unwrap();
        assert_eq!(args.intent.as_deref(), Some("request"));
        assert_eq!(args.positionals, vec!["@luna"]);
    }

    #[test]
    fn parse_flags_after_targets() {
        let args =
            SendArgs::try_parse_from(["send", "@luna", "--intent", "request", "--", "hello"])
                .unwrap();
        assert_eq!(args.intent.as_deref(), Some("request"));
        assert_eq!(args.positionals, vec!["@luna"]);
    }

    #[test]
    fn parse_bigboss_flag() {
        let args = SendArgs::try_parse_from(["send", "-b", "--", "hello"]).unwrap();
        assert!(args.bigboss);
        assert_eq!(args.sender_name(), Some("bigboss".to_string()));
    }

    #[test]
    fn parse_from_overrides_bigboss() {
        let args =
            SendArgs::try_parse_from(["send", "-b", "--from", "reviewer", "--", "hello"]).unwrap();
        assert_eq!(args.sender_name(), Some("reviewer".to_string()));
    }

    #[test]
    fn parse_stdin_flag() {
        let args = SendArgs::try_parse_from(["send", "--stdin", "@luna"]).unwrap();
        assert!(args.stdin);
        assert_eq!(args.positionals, vec!["@luna"]);
        assert!(args.message.is_empty());
    }

    #[test]
    fn parse_file_flag() {
        let args = SendArgs::try_parse_from(["send", "--file", "/tmp/msg.txt", "@luna"]).unwrap();
        assert_eq!(args.file.as_deref(), Some("/tmp/msg.txt"));
        assert_eq!(args.positionals, vec!["@luna"]);
    }

    #[test]
    fn parse_base64_flag() {
        let args = SendArgs::try_parse_from(["send", "--base64", "aGVsbG8=", "@luna"]).unwrap();
        assert_eq!(args.base64.as_deref(), Some("aGVsbG8="));
    }

    #[test]
    fn parse_reply_to_and_thread() {
        let args = SendArgs::try_parse_from([
            "send",
            "--reply-to",
            "42",
            "--thread",
            "pr-99",
            "@luna",
            "--",
            "hi",
        ])
        .unwrap();
        assert_eq!(args.reply_to.as_deref(), Some("42"));
        assert_eq!(args.thread.as_deref(), Some("pr-99"));
    }

    #[test]
    fn parse_quiet_flag() {
        let args = SendArgs::try_parse_from(["send", "--quiet", "-b", "--", "hi"]).unwrap();
        assert!(args.quiet);
    }

    #[test]
    fn parse_inline_bundle_flags() {
        let args = SendArgs::try_parse_from([
            "send",
            "-b",
            "--title",
            "my-bundle",
            "--description",
            "desc",
            "--events",
            "1-10",
            "--files",
            "a.py",
            "--transcript",
            "1-5:normal",
            "--",
            "msg",
        ])
        .unwrap();
        assert_eq!(args.title.as_deref(), Some("my-bundle"));
        assert_eq!(args.description.as_deref(), Some("desc"));
        assert_eq!(args.events.as_deref(), Some("1-10"));
        assert_eq!(args.files.as_deref(), Some("a.py"));
        assert_eq!(args.transcript.as_deref(), Some("1-5:normal"));
    }

    #[test]
    fn parse_no_separator_targets_only() {
        let args = SendArgs::try_parse_from(["send", "@luna"]).unwrap();
        assert_eq!(args.positionals, vec!["@luna"]);
        assert!(args.message.is_empty());
    }

    #[test]
    fn parse_compat_at_name_with_space() {
        // Backward compat: '@luna hi' as a single quoted arg
        // process_positionals treats as full message text
        let args = SendArgs::try_parse_from(["send", "@luna hi"]).unwrap();
        assert_eq!(args.positionals, vec!["@luna hi"]);
    }

    #[test]
    fn parse_bare_text_accepted() {
        // Bare text without @ is accepted by clap; cmd_send handles as message
        let args = SendArgs::try_parse_from(["send", "hello everyone"]).unwrap();
        assert_eq!(args.positionals, vec!["hello everyone"]);
    }

    #[test]
    fn parse_empty_target_rejected() {
        let result = SendArgs::try_parse_from(["send", "@", "--", "hi"]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_bare_text_with_separator_accepted() {
        // Bare text before -- is accepted by clap; validated in cmd_send
        let args = SendArgs::try_parse_from(["send", "luna", "--", "hi"]).unwrap();
        assert_eq!(args.positionals, vec!["luna"]);
        assert_eq!(args.message, vec!["hi"]);
    }

    #[test]
    fn parse_message_with_dashes() {
        let args =
            SendArgs::try_parse_from(["send", "@luna", "--", "--this", "is", "a", "message"])
                .unwrap();
        assert_eq!(args.message, vec!["--this", "is", "a", "message"]);
    }

    #[test]
    fn resolve_message_strips_redundant_trailing_auto_name_tokens() {
        let mut args =
            SendArgs::try_parse_from(["send", "@luna", "--", "message", "--name", "beru"]).unwrap();
        args.had_separator = true;

        assert_eq!(resolve_message(&args, Some("beru")).unwrap().0, "message");
    }

    #[test]
    fn resolve_message_keeps_quoted_name_suffix_inside_one_argument() {
        let mut args = SendArgs::try_parse_from([
            "send",
            "@luna",
            "--",
            "I always end commands with --name beru",
        ])
        .unwrap();
        args.had_separator = true;

        assert_eq!(
            resolve_message(&args, Some("beru")).unwrap().0,
            "I always end commands with --name beru"
        );
    }

    #[test]
    fn resolve_message_keeps_trailing_name_for_different_sender() {
        let mut args =
            SendArgs::try_parse_from(["send", "@luna", "--", "message", "--name", "nova"]).unwrap();
        args.had_separator = true;

        assert_eq!(
            resolve_message(&args, Some("beru")).unwrap().0,
            "message --name nova"
        );
    }

    #[test]
    fn parse_extends_flag() {
        let args = SendArgs::try_parse_from([
            "send",
            "-b",
            "--title",
            "t",
            "--extends",
            "abc123",
            "--",
            "msg",
        ])
        .unwrap();
        assert_eq!(args.extends.as_deref(), Some("abc123"));
    }

    // ── process_positionals tests ──

    #[test]
    fn process_empty() {
        let (targets, msg) = process_positionals(&[]);
        assert!(targets.is_empty());
        assert!(msg.is_none());
    }

    fn setup_test_db() -> (HcomDb, PathBuf, TestEnv) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        // send_message() reaches the process-global relay notification path,
        // so its ambient HCOM_DIR must live as long as the test DB.
        let env = crate::hooks::test_helpers::isolated_test_env();
        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_send_{}_{}.db",
            std::process::id(),
            test_id
        ));

        let db = HcomDb::open_at(&db_path).unwrap();
        (db, db_path, env)
    }

    fn cleanup_test_db(path: PathBuf) {
        let _ = std::fs::remove_file(&path);
        let wal = PathBuf::from(format!("{}-wal", path.display()));
        let shm = PathBuf::from(format!("{}-shm", path.display()));
        let _ = std::fs::remove_file(wal);
        let _ = std::fs::remove_file(shm);
    }

    #[test]
    fn cli_sandbox_aliases_resolve_only_for_current_members_and_reject_collisions() {
        use crate::sandbox::directory::{Access, HostIdentity, Placement};
        use crate::sandbox::store::Store;

        let root = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&root.path().join("hcom.db")).unwrap();
        db.conn()
            .execute_batch(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1), ('nova', 2);",
            )
            .unwrap();
        let owner = identity::resolve_from_name(&db, "luna").unwrap();
        let member = HostIdentity::capture(&db, &owner).unwrap();
        let outsider =
            HostIdentity::capture(&db, &identity::resolve_from_name(&db, "nova").unwrap()).unwrap();
        let store = Store::initialize(&db).unwrap();
        let team = store.create_team(Some(&member)).unwrap();
        let other = store.create_team(Some(&outsider)).unwrap();
        let reserve = |team: &str, host, sandbox| {
            store
                .reserve_worker(
                    team,
                    Access::Host(host),
                    "helper",
                    crate::tool::Tool::Claude,
                    Placement {
                        gateway: "gateway",
                        workspace: "default",
                        sandbox,
                        incarnation: "fedc8acb-cc62-422d-9724-54e730e64f6e",
                    },
                )
                .unwrap()
                .binding
        };
        let helper = reserve(&team.id, &member, "vm-a");
        let other_helper = reserve(&other.id, &outsider, "vm-b");
        let ctx = CommandContext {
            explicit_name: None,
            identity: Some(owner),
            go: true,
            identity_warning: None,
        };
        let send = |targets: &[&str]| {
            let argv = ["send", "--json"]
                .into_iter()
                .chain(targets.iter().copied())
                .chain(["--", "hello"]);
            let mut args = SendArgs::try_parse_from(argv).unwrap();
            args.had_separator = true;
            cmd_send(&db, &args, Some(&ctx))
        };
        assert_eq!(send(&["@Helper", "@nova"]), 1);
        assert_eq!(db.get_last_event_id(), 0);
        assert_eq!(send(&["@Helper"]), 0);
        let now = crate::shared::time::now_epoch_i64();
        let message = store.next_pending(&helper, now).unwrap().unwrap();
        assert_eq!(message.payload.target, helper.worker.worker);
        assert!(store.next_pending(&other_helper, now).unwrap().is_none());
        assert_eq!(send(&["@nova"]), 0);
        let delivered_to: String = db
            .conn()
            .query_row(
                "SELECT json_extract(data, '$.delivered_to') FROM events WHERE id = ?",
                [db.get_last_event_id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(delivered_to, r#"["nova"]"#);

        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('helper', 3)",
                [],
            )
            .unwrap();
        let before = db.get_last_event_id();
        assert_eq!(send(&["@helper"]), 1);
        assert_eq!(db.get_last_event_id(), before);
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'helper'", [])
            .unwrap();

        store
            .enroll_host(&other.id, Access::Host(&outsider), &member)
            .unwrap();
        assert_eq!(send(&["@helper"]), 1);
        assert_eq!(db.get_last_event_id(), before);
        assert_eq!(send(&[&format!("@{}", helper.worker.worker)]), 0);

        db.conn()
            .execute(
                "UPDATE instances SET created_at = 4 WHERE name = 'luna'",
                [],
            )
            .unwrap();
        let before = db.get_last_event_id();
        assert!(
            crate::sandbox::directory::resolve_host_alias(
                &db,
                ctx.identity.as_ref().unwrap(),
                "helper",
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(send(&["@helper"]), 1);
        assert_eq!(db.get_last_event_id(), before);
    }

    #[test]
    fn cli_queues_only_an_enrolled_single_sandbox_target() {
        use crate::sandbox::directory::{Access, HostIdentity, Placement};
        use crate::sandbox::store::Store;

        let root = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&root.path().join("hcom.db")).unwrap();
        db.conn()
            .execute_batch(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1), ('nova', 2);",
            )
            .unwrap();
        let identity = identity::resolve_from_name(&db, "luna").unwrap();
        let member = HostIdentity::capture(&db, &identity).unwrap();
        let store = Store::initialize(&db).unwrap();
        let team = store.create_team(Some(&member)).unwrap();
        let binding = store
            .reserve_worker(
                &team.id,
                Access::Host(&member),
                "sandbox",
                crate::tool::Tool::Claude,
                Placement {
                    gateway: "gateway",
                    workspace: "default",
                    sandbox: "sandbox",
                    incarnation: "fedc8acb-cc62-422d-9724-54e730e64f6e",
                },
            )
            .unwrap()
            .binding;
        let target = format!("@{}", binding.worker.worker);
        let ctx = |name| CommandContext {
            explicit_name: None,
            identity: Some(identity::resolve_from_name(&db, name).unwrap()),
            go: true,
            identity_warning: None,
        };
        let parse = |args: &[&str]| {
            let mut args = SendArgs::try_parse_from(args).unwrap();
            // The router records the separator before Clap removes it.
            args.had_separator = true;
            args
        };
        let args = parse(&[
            "send",
            "--json",
            "--intent",
            "request",
            "--thread",
            "work",
            &target,
            "--",
            "hello\r\nworld",
        ]);
        let observer = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        observer.set_nonblocking(true).unwrap();
        db.upsert_notify_endpoint(
            "luna",
            "events_stream",
            observer.local_addr().unwrap().port(),
        )
        .unwrap();
        assert_eq!(cmd_send(&db, &args, Some(&ctx("nova"))), 1);
        let mixed = parse(&["send", "--json", &target, "@nova", "--", "hello"]);
        assert_eq!(cmd_send(&db, &mixed, Some(&ctx("luna"))), 1);
        let bundle = parse(&[
            "send",
            "--json",
            "--title",
            "bundle",
            "--description",
            "context",
            &target,
            "--",
            "hello",
        ]);
        assert_eq!(cmd_send(&db, &bundle, Some(&ctx("luna"))), 1);
        assert_eq!(db.get_last_event_id(), 0);
        assert_eq!(
            observer.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(cmd_send(&db, &args, Some(&ctx("luna"))), 0);
        observer
            .accept()
            .expect("queued event wakes observers after commit");

        let event_id = db.get_last_event_id();
        let message = store
            .next_pending(&binding, crate::shared::time::now_epoch_i64())
            .unwrap()
            .unwrap();
        assert_eq!(message.payload.text, "hello\nworld");
        assert_eq!(message.payload.thread.as_deref(), Some("work"));
        let data: String = db
            .conn()
            .query_row("SELECT data FROM events WHERE id = ?", [event_id], |r| {
                r.get(0)
            })
            .unwrap();
        let data: serde_json::Value = serde_json::from_str(&data).unwrap();
        assert_eq!(
            data["queued_to"],
            serde_json::json!([binding.worker.worker])
        );
        assert_eq!(data["delivered_to"], serde_json::json!([]));
        assert_eq!(data["_sandbox"]["id"], serde_json::json!(message.id));
        assert!(
            crate::relay::push::build_push_payload(&db, "device")
                .1
                .is_empty()
        );

        let reply_id = event_id.to_string();
        let reply = parse(&[
            "send",
            "--json",
            "--reply-to",
            &reply_id,
            &target,
            "--",
            "followup",
        ]);
        assert_eq!(cmd_send(&db, &reply, Some(&ctx("luna"))), 0);
        store
            .acknowledge(
                &binding,
                &message.id,
                &message.digest,
                crate::shared::time::now_epoch_i64(),
            )
            .unwrap();
        let reply = store
            .next_pending(&binding, crate::shared::time::now_epoch_i64())
            .unwrap()
            .unwrap();
        assert_eq!(reply.payload.reply_to, Some(message.id));
        assert_eq!(reply.payload.thread.as_deref(), Some("work"));

        #[cfg(unix)]
        {
            use crate::sandbox::bridge::{HostInbox, Inbound};
            use crate::sandbox::protocol::{Intent, Message, MessageId, MessagePayload, Receiver};
            use crate::sandbox::store::Import;

            let incoming = Message::new(
                MessageId {
                    origin: binding.worker.clone(),
                    sequence: 1,
                },
                MessagePayload {
                    conversation: None,
                    target: "luna".into(),
                    target_generation: Some(
                        crate::sandbox::directory::host_recipient_generation(
                            db.conn(),
                            "luna",
                            1.0,
                        )
                        .unwrap(),
                    ),
                    sender_routing_generation: None,
                    thread: Some("work".into()),
                    reply_to: None,
                    broadcast: false,
                    intent: Intent::Request,
                    text: "from guest".into(),
                },
            )
            .unwrap();
            let mut inbox = HostInbox::new(&db, &store, &binding);
            let now = crate::shared::time::now_epoch_i64();
            let Import::Inserted(incoming_event) = store
                .import(Receiver::Host, &binding, &incoming, now, |tx| {
                    inbox.insert(tx, &incoming)
                })
                .unwrap()
            else {
                panic!("fixture did not import the guest message");
            };
            inbox.committed(incoming_event);
            let args = parse(&[
                "send",
                "--json",
                "--reply-to",
                &incoming_event.to_string(),
                &target,
                "--",
                "answer",
            ]);
            assert_eq!(cmd_send(&db, &args, Some(&ctx("luna"))), 0);
            store
                .acknowledge(&binding, &reply.id, &reply.digest, now)
                .unwrap();
            assert_eq!(
                store
                    .next_pending(&binding, now)
                    .unwrap()
                    .unwrap()
                    .payload
                    .reply_to,
                Some(incoming.id),
            );
        }

        store
            .retire_worker(Access::Host(&member), &binding)
            .unwrap();
        let replacement = store
            .reserve_worker(
                &team.id,
                Access::Host(&member),
                "sandbox",
                crate::tool::Tool::Claude,
                Placement {
                    gateway: "gateway",
                    workspace: "default",
                    sandbox: "replacement",
                    incarnation: "321741fe-a026-4bd1-be86-0099e22498c4",
                },
            )
            .unwrap()
            .binding;
        assert_eq!(replacement.worker.worker, binding.worker.worker);
        let stale_reply = parse(&[
            "send",
            "--json",
            "--reply-to",
            &reply_id,
            &target,
            "--",
            "stale",
        ]);
        let before = db.get_last_event_id();
        assert_eq!(cmd_send(&db, &stale_reply, Some(&ctx("luna"))), 1);
        assert_eq!(db.get_last_event_id(), before);
        assert!(
            store
                .next_pending(&replacement, crate::shared::time::now_epoch_i64())
                .unwrap()
                .is_none()
        );
    }

    #[serial]
    #[test]
    fn cli_thread_only_sandbox_send_fans_out_to_current_participants() {
        use crate::sandbox::directory::{Access, HostIdentity, Placement};
        use crate::sandbox::store::Store;

        let root = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&root.path().join("hcom.db")).unwrap();
        db.conn()
            .execute_batch(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1), ('nova', 2);",
            )
            .unwrap();
        let identity = identity::resolve_from_name(&db, "luna").unwrap();
        let member = HostIdentity::capture(&db, &identity).unwrap();
        let nova =
            HostIdentity::capture(&db, &identity::resolve_from_name(&db, "nova").unwrap()).unwrap();
        let store = Store::initialize(&db).unwrap();
        let team = store.create_team(Some(&member)).unwrap();
        let binding = store
            .reserve_worker(
                &team.id,
                Access::Host(&member),
                "sandbox",
                crate::tool::Tool::Claude,
                Placement {
                    gateway: "gateway",
                    workspace: "default",
                    sandbox: "sandbox",
                    incarnation: "fedc8acb-cc62-422d-9724-54e730e64f6e",
                },
            )
            .unwrap()
            .binding;
        let target = format!("@{}", binding.worker.worker);
        let ctx = |name| CommandContext {
            explicit_name: None,
            identity: Some(identity::resolve_from_name(&db, name).unwrap()),
            go: true,
            identity_warning: None,
        };
        let parse = |args: &[&str]| {
            let mut args = SendArgs::try_parse_from(args).unwrap();
            args.had_separator = true;
            args
        };
        let event_count = |db: &HcomDb| -> i64 {
            db.conn()
                .query_row(
                    "SELECT count(*) FROM events WHERE type = 'message'",
                    [],
                    |row| row.get(0),
                )
                .unwrap()
        };
        let last_event = |db: &HcomDb| -> serde_json::Value {
            let data: String = db
                .conn()
                .query_row(
                    "SELECT data FROM events WHERE type = 'message'
                     ORDER BY id DESC LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            serde_json::from_str(&data).unwrap()
        };
        let observer = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        observer.set_nonblocking(true).unwrap();
        db.upsert_notify_endpoint(
            "luna",
            "events_stream",
            observer.local_addr().unwrap().port(),
        )
        .unwrap();
        // The 50 ms best-effort wake connect can drop under load; poll the
        // listener briefly so scheduling jitter cannot fail the assertion.
        let wait_wake = |observer: &std::net::TcpListener, why: &str| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                match observer.accept() {
                    Ok(_) => break,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(std::time::Instant::now() < deadline, "{why}");
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(error) => panic!("{why}: {error}"),
                }
            }
        };

        // The explicit threaded send seeds the conversation grant.
        let seed = parse(&["send", "--json", "--thread", "work", &target, "--", "seed"]);
        assert_eq!(cmd_send(&db, &seed, Some(&ctx("luna"))), 0);
        wait_wake(&observer, "queued seed send wakes observers after commit");
        let settle = |store: &Store<'_>, binding: &crate::sandbox::protocol::Binding| {
            let now = crate::shared::time::now_epoch_i64();
            let pending = store.next_pending(binding, now).unwrap().unwrap();
            store
                .acknowledge(binding, &pending.id, &pending.digest, now)
                .unwrap();
        };
        settle(&store, &binding);
        let conversation: String = db
            .conn()
            .query_row("SELECT id FROM sandbox_conversations", [], |row| row.get(0))
            .unwrap();
        let data = last_event(&db);
        assert_eq!(
            data["_sandbox"]["conversation"],
            serde_json::json!(conversation)
        );

        // A same-team non-participant cannot adopt the grant by targeting it.
        store
            .enroll_host(&team.id, Access::Host(&member), &nova)
            .unwrap();
        let guess = parse(&["send", "--json", "--thread", "work", &target, "--", "guess"]);
        assert_eq!(cmd_send(&db, &guess, Some(&ctx("nova"))), 0);
        wait_wake(&observer, "directed non-participant send wakes observers");
        settle(&store, &binding);
        let data = last_event(&db);
        assert!(data["_sandbox"].get("conversation").is_none());
        let members: i64 = db
            .conn()
            .query_row(
                "SELECT count(*) FROM sandbox_conversation_members",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(members, 2);

        // The participant's thread-only send reaches current guests in one
        // atomic batch with the shared stable wire identity.
        let fanout = parse(&["send", "--json", "--thread", "work", "--", "fanout"]);
        assert_eq!(cmd_send(&db, &fanout, Some(&ctx("luna"))), 0);
        wait_wake(
            &observer,
            "thread-only fan-out wakes observers after commit",
        );
        let data = last_event(&db);
        assert_eq!(data["thread"], serde_json::json!("work"));
        assert_eq!(
            data["queued_to"],
            serde_json::json!([binding.worker.worker])
        );
        assert_eq!(data["mentions"], serde_json::json!([]));
        assert_eq!(data["delivered_to"], serde_json::json!([]));
        let message = store
            .next_pending(&binding, crate::shared::time::now_epoch_i64())
            .unwrap()
            .unwrap();
        assert_eq!(
            data["_sandbox"]["id"],
            serde_json::json!(message.id),
            "the queued hop replays the event's stable wire identity"
        );
        assert_eq!(message.payload.thread.as_deref(), Some("work"));
        assert_eq!(
            message.payload.target_generation.as_deref(),
            Some(binding.worker.generation.as_str())
        );
        // Sandbox thread labels never enter the ordinary membership index.
        assert!(db.get_thread_members("work").is_empty());
        assert!(
            crate::relay::push::build_push_payload(&db, "device")
                .1
                .is_empty()
        );

        // A replaced host generation resolves nothing and fails closed with
        // the ordinary unknown-thread error, without a hidden-thread oracle.
        db.conn()
            .execute(
                "UPDATE instances SET created_at = 9 WHERE name = 'luna'",
                [],
            )
            .unwrap();
        let stale = parse(&["send", "--json", "--thread", "work", "--", "stale"]);
        assert_eq!(cmd_send(&db, &stale, Some(&ctx("luna"))), 1);
        assert_eq!(event_count(&db), 3);
    }

    #[test]
    fn ordinary_broadcast_omits_sandbox_projections() {
        use crate::sandbox::directory::{Access, Placement};
        use crate::sandbox::store::Store;

        let root = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&root.path().join("hcom.db")).unwrap();
        db.conn()
            .execute_batch(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1), ('nova', 2);",
            )
            .unwrap();
        let sender = identity::resolve_from_name(&db, "luna").unwrap();
        let ordinary = resolve_delivery(&db, &sender, "hello", None, Some(&[]), false).unwrap();
        let store = Store::initialize(&db).unwrap();
        let team = store.create_team(None).unwrap();
        let controller = team.external_controller.unwrap();
        let worker = store
            .reserve_worker(
                &team.id,
                Access::Controller(&controller),
                "sandbox",
                crate::tool::Tool::Claude,
                Placement {
                    gateway: "gateway",
                    workspace: "default",
                    sandbox: "sandbox",
                    incarnation: "fedc8acb-cc62-422d-9724-54e730e64f6e",
                },
            )
            .unwrap();
        let with_sandbox = resolve_delivery(&db, &sender, "hello", None, Some(&[]), false).unwrap();
        assert_eq!(ordinary.delivered_to, ["nova"]);
        assert_eq!(with_sandbox.delivered_to, ordinary.delivered_to);
        let name = worker.binding.worker.worker;
        let before = db.get_last_event_id();
        assert!(
            send_message(
                &db,
                &sender,
                "hello",
                None,
                Some(std::slice::from_ref(&name))
            )
            .is_err()
        );
        assert!(send_message(&db, &sender, &format!("hello @{name}"), None, None).is_err());
        assert_eq!(db.get_last_event_id(), before);
        assert_eq!(
            db.send_system_message("[system]", "hello").unwrap(),
            ["luna", "nova"]
        );
        let before = db.get_last_event_id();
        assert!(
            db.send_system_message("[system]", &format!("hello @{name}"))
                .is_err()
        );
        assert_eq!(db.get_last_event_id(), before);
    }

    #[test]
    #[serial]
    fn send_message_threads_seed_and_reuse_memberships() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0), ('nova', 1000.0), ('miso', 1000.0)",
                [],
            )
            .unwrap();

        let sender = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: None,
            session_id: None,
        };
        let envelope = MessageEnvelope {
            thread: Some("debate-1".into()),
            ..Default::default()
        };

        let (_, delivered) = send_message(
            &db,
            &sender,
            "hello",
            Some(&envelope),
            Some(&["nova".to_string(), "miso".to_string()]),
        )
        .unwrap();
        assert_eq!(delivered, vec!["nova".to_string(), "miso".to_string()]);

        let members = db.get_thread_members("debate-1");
        assert_eq!(
            members,
            vec!["nova".to_string(), "miso".to_string(), "luna".to_string()]
        );

        let (_, delivered) = send_message(&db, &sender, "round 2", Some(&envelope), None).unwrap();
        assert_eq!(delivered, vec!["nova".to_string(), "miso".to_string()]);

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn send_message_thread_without_members_errors() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0)",
                [],
            )
            .unwrap();

        let sender = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: None,
            session_id: None,
        };
        let envelope = MessageEnvelope {
            thread: Some("empty-thread".into()),
            ..Default::default()
        };

        let err = send_message(&db, &sender, "hello", Some(&envelope), None).unwrap_err();
        assert!(err.contains("has no members"));

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn send_message_external_sender_does_not_auto_subscribe_to_thread() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('nova', 1000.0)",
                [],
            )
            .unwrap();

        let sender = SenderIdentity {
            kind: SenderKind::External,
            name: "bigboss".into(),
            instance_data: None,
            session_id: None,
        };
        let envelope = MessageEnvelope {
            thread: Some("ops".into()),
            ..Default::default()
        };

        let (_, delivered) = send_message(
            &db,
            &sender,
            "hello",
            Some(&envelope),
            Some(&["nova".to_string()]),
        )
        .unwrap();
        assert_eq!(delivered, vec!["nova".to_string()]);
        assert_eq!(db.get_thread_members("ops"), vec!["nova".to_string()]);

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn send_message_thread_request_creates_and_cancels_request_watch_rows() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at)
                 VALUES ('luna', 1000.0), ('nova', 1000.0), ('miso', 1000.0)",
                [],
            )
            .unwrap();

        let sender = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: None,
            session_id: None,
        };
        let seed_envelope = MessageEnvelope {
            thread: Some("ops".into()),
            ..Default::default()
        };
        send_message(
            &db,
            &sender,
            "seed",
            Some(&seed_envelope),
            Some(&["nova".to_string(), "miso".to_string()]),
        )
        .unwrap();

        let request_envelope = MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Request),
            thread: Some("ops".into()),
            ..Default::default()
        };
        let (_, delivered) =
            send_message(&db, &sender, "status?", Some(&request_envelope), None).unwrap();
        assert_eq!(delivered, vec!["nova".to_string(), "miso".to_string()]);

        let reqwatch_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reqwatch_count, 2);

        let (scope, mentions_json): (String, String) = db
            .conn()
            .query_row(
                "SELECT json_extract(data, '$.scope'), json_extract(data, '$.mentions')
                 FROM events
                 WHERE type = 'message'
                 ORDER BY id DESC
                 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(scope, "mentions");
        assert!(mentions_json.contains("nova"));

        let responder = SenderIdentity {
            kind: SenderKind::Instance,
            name: "nova".into(),
            instance_data: None,
            session_id: None,
        };
        let reply_envelope = MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Inform),
            thread: Some("ops".into()),
            ..Default::default()
        };
        let (_, delivered) =
            send_message(&db, &responder, "done", Some(&reply_envelope), None).unwrap();
        assert_eq!(delivered, vec!["luna".to_string(), "miso".to_string()]);

        let reqwatch_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reqwatch_count, 1);

        let responder = SenderIdentity {
            kind: SenderKind::Instance,
            name: "miso".into(),
            instance_data: None,
            session_id: None,
        };
        let (_, delivered) =
            send_message(&db, &responder, "done", Some(&reply_envelope), None).unwrap();
        assert_eq!(delivered, vec!["luna".to_string(), "nova".to_string()]);

        let reqwatch_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reqwatch_count, 0);

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn send_mention_excludes_inactive_instances() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, created_at)
                 VALUES ('luna', 'listening', '', 1000.0),
                        ('vine', 'inactive', 'exit:unknown', 1000.0)",
                [],
            )
            .unwrap();

        let sender = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: None,
            session_id: None,
        };

        let err =
            send_message(&db, &sender, "ping", None, Some(&["vine".to_string()])).unwrap_err();
        assert!(err.contains("@vine"), "err={err}");
        assert!(err.contains("Available:"), "err={err}");
        assert!(
            err.contains("luna"),
            "listening agent should be listed: {err}"
        );
        let available_line = err.lines().find(|l| l.contains("Available:")).unwrap_or("");
        assert!(
            !available_line.contains("vine"),
            "inactive agent must not appear in Available: {err}"
        );

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn send_mention_excludes_exit_no_tool_call_inactive() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, status, status_context, created_at)
                 VALUES ('luna', 'listening', '', 1000.0),
                        ('dove', 'inactive', 'exit:no_tool_call', 1000.0)",
                [],
            )
            .unwrap();

        let sender = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: None,
            session_id: None,
        };

        let err =
            send_message(&db, &sender, "ping", None, Some(&["dove".to_string()])).unwrap_err();
        assert!(err.contains("@dove"), "err={err}");
        let available_line = err.lines().find(|l| l.contains("Available:")).unwrap_or("");
        assert!(
            !available_line.contains("dove"),
            "soft-stopped agent must not appear in Available: {err}"
        );

        cleanup_test_db(path);
    }

    #[test]
    fn process_compat_at_with_space() {
        // "@luna hi" → full text as message, no targets
        let (targets, msg) = process_positionals(&["@luna hi".to_string()]);
        assert!(targets.is_empty());
        assert_eq!(msg.as_deref(), Some("@luna hi"));
    }

    #[test]
    fn process_bare_text() {
        // "hello everyone" → message text, no targets (broadcast)
        let (targets, msg) = process_positionals(&["hello everyone".to_string()]);
        assert!(targets.is_empty());
        assert_eq!(msg.as_deref(), Some("hello everyone"));
    }

    #[test]
    #[serial]
    fn test_ambiguous_bare_word_error_known_and_unknown() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, status, status_time, created_at)
                 VALUES ('luna', 'claude', 'active', 0, 0)",
                [],
            )
            .unwrap();

        // 1. Lone known agent -> Did you mean @luna?
        let err = ambiguous_bare_word_error(&db, &[], "luna").unwrap();
        assert!(err.contains("Error: 'luna' is ambiguous"));
        assert!(err.contains("Did you mean @luna? hcom send @luna -- <message>"));

        // 2. Lone unknown word -> direct + explicit broadcast syntax
        let err = ambiguous_bare_word_error(&db, &[], "unknownword").unwrap();
        assert!(err.contains("Error: 'unknownword' is ambiguous"));
        assert!(err.contains("Direct:    hcom send @name -- unknownword"));
        assert!(err.contains("Broadcast: hcom send --broadcast -- unknownword"));

        // 3. Multi-word string -> None
        assert!(ambiguous_bare_word_error(&db, &[], "hello world").is_none());

        // 4. Non-empty targets -> None
        assert!(ambiguous_bare_word_error(&db, &["luna".to_string()], "msg").is_none());

        cleanup_test_db(path);
    }

    #[test]
    fn process_pure_targets() {
        // "@luna" → target, no message
        let (targets, msg) = process_positionals(&["@luna".to_string()]);
        assert_eq!(targets, vec!["luna"]);
        assert!(msg.is_none());
    }

    #[test]
    fn process_target_plus_bare_text() {
        // "@luna", "hello" → target + message
        let (targets, msg) = process_positionals(&["@luna".to_string(), "hello".to_string()]);
        assert_eq!(targets, vec!["luna"]);
        assert_eq!(msg.as_deref(), Some("hello"));
    }

    fn insert_imported_remote_message(
        db: &HcomDb,
        local_id: i64,
        origin_id: serde_json::Value,
        short: &str,
        text: &str,
        thread: Option<&str>,
        intent: Option<&str>,
    ) {
        let relay_obj = serde_json::json!({
            "id": origin_id,
            "short": short,
            "device": format!("dev-uuid-{short}"),
        });
        let mut data = serde_json::json!({
            "from": format!("remote:{short}"),
            "text": text,
            "_relay": relay_obj,
        });
        if let Some(t) = thread {
            data["thread"] = serde_json::json!(t);
        }
        if let Some(i) = intent {
            data["intent"] = serde_json::json!(i);
        }
        db.conn()
            .execute(
                "INSERT INTO events (id, type, instance, timestamp, data) VALUES (?, 'message', 'remote-inst', '2026-09-04T00:00:00Z', ?)",
                rusqlite::params![local_id, data.to_string()],
            )
            .unwrap();
    }

    #[test]
    #[serial]
    fn test_resolve_reply_to_remote_different_local_id() {
        let (db, path, _env) = setup_test_db();
        insert_imported_remote_message(
            &db,
            100,
            serde_json::json!(42),
            "BOXE",
            "remote msg",
            None,
            None,
        );

        let resolved = resolve_reply_to_local(&db, "42:BOXE");
        assert_eq!(resolved, Some(100));

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_resolve_reply_to_remote_colliding_local_id() {
        let (db, path, _env) = setup_test_db();
        let local_data = serde_json::json!({ "text": "unrelated local message" });
        db.conn()
            .execute(
                "INSERT INTO events (id, type, instance, timestamp, data) VALUES (42, 'message', 'local-inst', '2026-09-04T00:00:00Z', ?)",
                [local_data.to_string()],
            )
            .unwrap();

        insert_imported_remote_message(
            &db,
            100,
            serde_json::json!(42),
            "BOXE",
            "remote msg",
            None,
            None,
        );

        // 42:BOXE must resolve to the remote event (100), never the colliding local event (42)
        assert_eq!(resolve_reply_to_local(&db, "42:BOXE"), Some(100));
        // local 42 without suffix must resolve to the local event (42)
        assert_eq!(resolve_reply_to_local(&db, "42"), Some(42));

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_resolve_reply_to_two_devices_same_origin_id() {
        let (db, path, _env) = setup_test_db();
        insert_imported_remote_message(
            &db,
            100,
            serde_json::json!(42),
            "BOXE",
            "msg 1",
            None,
            None,
        );
        insert_imported_remote_message(
            &db,
            200,
            serde_json::json!(42),
            "WAVE",
            "msg 2",
            None,
            None,
        );

        assert_eq!(resolve_reply_to_local(&db, "42:BOXE"), Some(100));
        assert_eq!(resolve_reply_to_local(&db, "42:WAVE"), Some(200));

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_resolve_reply_to_canonical_device_suffix_grammar() {
        let (db, path, _env) = setup_test_db();
        insert_imported_remote_message(
            &db,
            100,
            serde_json::json!(42),
            "BOXE",
            "remote msg",
            None,
            None,
        );

        // Exact 4-char uppercase ASCII succeeds
        assert_eq!(resolve_reply_to_local(&db, "42:BOXE"), Some(100));

        // Lowercase, mixed-case, prefixes, or non-4-char suffixes must fail closed
        assert_eq!(resolve_reply_to_local(&db, "42:boxe"), None);
        assert_eq!(resolve_reply_to_local(&db, "42:Boxe"), None);
        assert_eq!(resolve_reply_to_local(&db, "42:BO"), None);
        assert_eq!(resolve_reply_to_local(&db, "42:BOX"), None);
        assert_eq!(resolve_reply_to_local(&db, "42:BOXES"), None);
        assert_eq!(resolve_reply_to_local(&db, "42:NOPE"), None);

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_resolve_reply_to_rejects_non_integer_json_id() {
        let (db, path, _env) = setup_test_db();
        // Origin ID stored as JSON string instead of JSON integer
        insert_imported_remote_message(
            &db,
            100,
            serde_json::json!("42"),
            "BOXE",
            "malformed",
            None,
            None,
        );

        // Must fail closed because json_type is not 'integer'
        assert_eq!(resolve_reply_to_local(&db, "42:BOXE"), None);

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_resolve_reply_to_duplicate_same_short_origin_fails_closed() {
        let (db, path, _env) = setup_test_db();
        insert_imported_remote_message(
            &db,
            100,
            serde_json::json!(42),
            "BOXE",
            "msg 1",
            None,
            None,
        );
        insert_imported_remote_message(
            &db,
            101,
            serde_json::json!(42),
            "BOXE",
            "msg 2",
            None,
            None,
        );

        // Multiple rows with origin=42 and short=BOXE must fail closed
        assert_eq!(resolve_reply_to_local(&db, "42:BOXE"), None);

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_cmd_send_remote_reply_inherits_thread_at_command_boundary() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('sender', 1000.0), ('target', 1000.0)",
                [],
            )
            .unwrap();

        // Remote request at local row 100 with origin 42 and thread "thread-remote-99"
        insert_imported_remote_message(
            &db,
            100,
            serde_json::json!(42),
            "BOXE",
            "remote req",
            Some("thread-remote-99"),
            Some("request"),
        );

        // Execute send through the full cmd_send entrypoint without specifying --thread
        let mut args = SendArgs::try_parse_from([
            "send",
            "@target",
            "--from",
            "sender",
            "--intent",
            "ack",
            "--reply-to",
            "42:BOXE",
            "--",
            "pong message",
        ])
        .unwrap();
        args.had_separator = true;

        let exit_code = cmd_send(&db, &args, None);
        assert_eq!(exit_code, 0, "cmd_send should succeed");

        // Verify sent event data at command boundary
        let (reply_to, reply_to_local, thread): (Option<String>, Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT json_extract(data, '$.reply_to'), json_extract(data, '$.reply_to_local'), json_extract(data, '$.thread')
                 FROM events WHERE type = 'message' AND id != 100 ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();

        assert_eq!(reply_to.as_deref(), Some("42:BOXE"));
        assert_eq!(reply_to_local, Some(100));
        assert_eq!(thread.as_deref(), Some("thread-remote-99"));

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_cmd_send_unresolved_remote_reply_exits_one() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('sender', 1000.0), ('target', 1000.0)",
                [],
            )
            .unwrap();

        insert_imported_remote_message(
            &db,
            100,
            serde_json::json!(42),
            "BOXE",
            "remote req",
            None,
            None,
        );

        let count_before: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();

        // Unknown device
        let mut args_unknown = SendArgs::try_parse_from([
            "send",
            "@target",
            "--from",
            "sender",
            "--reply-to",
            "42:NOPE",
            "--",
            "hi",
        ])
        .unwrap();
        args_unknown.had_separator = true;
        assert_eq!(cmd_send(&db, &args_unknown, None), 1);

        // Non-canonical lowercase device
        let mut args_lower = SendArgs::try_parse_from([
            "send",
            "@target",
            "--from",
            "sender",
            "--reply-to",
            "42:boxe",
            "--",
            "hi",
        ])
        .unwrap();
        args_lower.had_separator = true;
        assert_eq!(cmd_send(&db, &args_lower, None), 1);

        // Non-existent origin ID
        let mut args_nonexistent = SendArgs::try_parse_from([
            "send",
            "@target",
            "--from",
            "sender",
            "--reply-to",
            "999:BOXE",
            "--",
            "hi",
        ])
        .unwrap();
        args_nonexistent.had_separator = true;
        assert_eq!(cmd_send(&db, &args_nonexistent, None), 1);

        let count_after: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count_before, count_after);

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_cmd_send_ambiguous_remote_reply_exits_one_and_emits_no_message() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('sender', 1000.0), ('target', 1000.0)",
                [],
            )
            .unwrap();

        // Two distinct local rows with identical origin ID (42) and short device ("BOXE")
        insert_imported_remote_message(
            &db,
            101,
            serde_json::json!(42),
            "BOXE",
            "remote msg 1",
            None,
            None,
        );
        insert_imported_remote_message(
            &db,
            102,
            serde_json::json!(42),
            "BOXE",
            "remote msg 2",
            None,
            None,
        );

        let count_before: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();

        let mut args_ambiguous = SendArgs::try_parse_from([
            "send",
            "@target",
            "--from",
            "sender",
            "--reply-to",
            "42:BOXE",
            "--",
            "hi",
        ])
        .unwrap();
        args_ambiguous.had_separator = true;

        assert_eq!(cmd_send(&db, &args_ambiguous, None), 1);

        let count_after: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count_before, count_after);

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_send_message_remote_ack_loop_prevention_with_differing_ids() {
        let (db, path, _env) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, created_at) VALUES ('luna', 1000.0)",
                [],
            )
            .unwrap();

        // 1. Remote inform: local id = 110, origin id = 10 (distinct IDs)
        insert_imported_remote_message(
            &db,
            110,
            serde_json::json!(10),
            "BOXE",
            "remote info",
            None,
            Some("inform"),
        );

        let sender = SenderIdentity {
            kind: SenderKind::Instance,
            name: "luna".into(),
            instance_data: None,
            session_id: None,
        };

        let ack_to_inform = MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Ack),
            reply_to: Some("10:BOXE".into()),
            ..Default::default()
        };

        let err = send_message(&db, &sender, "ack", Some(&ack_to_inform), None).unwrap_err();
        assert!(err.contains("Cannot ack an inform"));

        // 2. Remote ack: local id = 120, origin id = 20 (distinct IDs)
        insert_imported_remote_message(
            &db,
            120,
            serde_json::json!(20),
            "BOXE",
            "remote ack",
            None,
            Some("ack"),
        );

        let ack_to_ack = MessageEnvelope {
            intent: Some(crate::messages::MessageIntent::Ack),
            reply_to: Some("20:BOXE".into()),
            ..Default::default()
        };

        let err = send_message(&db, &sender, "ack", Some(&ack_to_ack), None).unwrap_err();
        assert!(err.contains("Ack-on-ack loop detected"));

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_resolve_reply_to_malformed_tokens_fail_closed() {
        let (db, path, _env) = setup_test_db();
        insert_imported_remote_message(
            &db,
            100,
            serde_json::json!(42),
            "BOXE",
            "remote msg",
            None,
            None,
        );

        assert_eq!(resolve_reply_to_local(&db, ""), None);
        assert_eq!(resolve_reply_to_local(&db, "   "), None);
        assert_eq!(resolve_reply_to_local(&db, "42:"), None);
        assert_eq!(resolve_reply_to_local(&db, ":BOXE"), None);
        assert_eq!(resolve_reply_to_local(&db, "42:BOXE:EXTRA"), None);
        assert_eq!(resolve_reply_to_local(&db, "abc:BOXE"), None);

        cleanup_test_db(path);
    }

    #[test]
    #[serial]
    fn test_resolve_reply_to_local_only_unchanged() {
        let (db, path, _env) = setup_test_db();
        let local_data = serde_json::json!({ "text": "local message" });
        db.conn()
            .execute(
                "INSERT INTO events (id, type, instance, timestamp, data) VALUES (42, 'message', 'local-inst', '2026-09-04T00:00:00Z', ?)",
                [local_data.to_string()],
            )
            .unwrap();

        assert_eq!(resolve_reply_to_local(&db, "42"), Some(42));
        assert_eq!(resolve_reply_to_local(&db, "999"), None);
        assert_eq!(resolve_reply_to_local(&db, "notanumber"), None);

        cleanup_test_db(path);
    }
}
