//! Hermes bridges: one messaging-gateway chat (Telegram, Discord, IRC, ...) as
//! an hcom participant.
//!
//! A human connects a chat with `/hcom start [name]` (the plugin's slash command,
//! run by the gateway inside that chat's session scope). hcom owns the
//! binding: the route is kept in the DB, so it survives gateway restarts, and
//! the bridge is an ordinary `hermes` instance bound to a process token the
//! plugin presents on every RPC. The plugin in the gateway delivers through
//! Hermes's public `inject_message`, which re-checks the chat's allowlist.
//!
//! The registry (hcom kv) is the only authority. The plugin reconciles
//! against it with `sync` when the gateway is ready and on every heartbeat.
//!
//! `hermes-bridge add|sync|session|rm|visibility` (JSON replies, exit 0).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::plugin_rpc::{self, parse_flag};
use crate::db::HcomDb;
use crate::instance_lifecycle as lifecycle;
use crate::shared::ST_LISTENING;
use crate::shared::context::HcomContext;

const KV_PREFIX: &str = "hermes_bridge:";
/// Stop reason of every bridge disconnect (`/hcom stop`, `hcom stop`, `hcom kill`).
pub const DISCONNECTED: &str = "disconnected";
/// How to bring a disconnected bridge back as the same participant (name,
/// mailbox position and session are restored from its stop).
pub fn reconnect_hint(name: &str) -> String {
    format!("To reconnect: send /hcom start {name} in that chat.")
}

/// One bridged chat.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Bridge {
    pub name: String,
    /// Process-binding token the plugin presents as HCOM_PROCESS_ID.
    pub token: String,
    /// The profile's Hermes home.
    pub home: String,
    pub session_key: String,
    pub platform: String,
    /// Display label for the chat (name or user).
    #[serde(default)]
    pub chat: String,
    /// `SessionSource`-shaped route, for re-creating an expired session.
    #[serde(default)]
    pub origin: Value,
    #[serde(default)]
    pub visibility: Visibility,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    Off,
    #[default]
    Preview,
    Full,
}

fn key(name: &str) -> String {
    format!("{KV_PREFIX}{name}")
}

pub fn bridges(db: &HcomDb) -> Vec<Bridge> {
    db.kv_prefix(KV_PREFIX)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(_, value)| serde_json::from_str(&value).ok())
        .collect()
}

fn bridge(db: &HcomDb, name: &str) -> Option<Bridge> {
    db.kv_get(&key(name))
        .ok()
        .flatten()
        .and_then(|value| serde_json::from_str(&value).ok())
}

fn save(db: &HcomDb, bridge: &Bridge) -> anyhow::Result<()> {
    db.kv_set(&key(&bridge.name), Some(&serde_json::to_string(bridge)?))
}

/// Forget a bridge: registry and (if still running) its instance, in one
/// write scope so a concurrent `sync` cannot re-activate it in between.
/// `Ok(None)`: not a bridge.
fn forget(
    db: &HcomDb,
    name: &str,
    initiated_by: &str,
    reason: &str,
) -> anyhow::Result<Option<Bridge>> {
    db.with_write_scope(|| {
        let Some(found) = bridge(db, name) else {
            return Ok(None);
        };
        db.kv_set(&key(name), None)?;
        if db.get_instance_full(name)?.is_some() {
            super::common::stop_instance(db, name, initiated_by, reason);
        }
        Ok(Some(found))
    })
}

/// `hcom stop`/`hcom kill` on a bridge: there is no process to end, and a
/// plain instance stop would be undone by the next `sync`, so both forget it.
/// Returns what happened, for the CLI to print; None when not a bridge.
pub fn disconnect(db: &HcomDb, name: &str, initiated_by: &str) -> Option<String> {
    let chat = match forget(db, name, initiated_by, DISCONNECTED) {
        Ok(chat) => chat?,
        Err(e) => return Some(format!("Could not disconnect '{name}': {e:#}")),
    };
    let label = if chat.chat.is_empty() {
        chat.platform.clone()
    } else {
        format!("{} chat {}", chat.platform, chat.chat)
    };
    Some(format!(
        "Disconnected '{name}' ({label}) from hcom. Nothing was killed: the Hermes \
         gateway and the chat keep running, other agents just can't message it.\n\
         {}",
        reconnect_hint(name)
    ))
}

/// Forget every bridge of a profile (uninstall).
pub fn forget_home(db: &HcomDb, home: &Path) {
    let home = super::hermes::home_key(home);
    for bridge in bridges(db).into_iter().filter(|b| b.home == home) {
        if let Err(e) = forget(db, &bridge.name, "system", "uninstalled") {
            eprintln!("Warning: cannot disconnect {}: {e:#}", bridge.name);
        }
    }
}

/// The bootstrap for a bridged chat: the normal hermes one, plus the hcom
/// command when the chat's shell would otherwise reach another hcom. Whether
/// the person sees incoming messages is the plugin's per-delivery concern,
/// since visibility changes at runtime.
fn bootstrap(db: &HcomDb, bridge: &Bridge) -> String {
    let env = std::collections::HashMap::from([
        ("HCOM_PROCESS_ID".to_string(), bridge.token.clone()),
        ("HCOM_LAUNCHED".to_string(), "1".to_string()),
        ("HCOM_TOOL".to_string(), "hermes".to_string()),
        (
            "HCOM_DIR".to_string(),
            crate::paths::hcom_dir().to_string_lossy().into_owned(),
        ),
    ]);
    let ctx = HcomContext::from_env(&env, PathBuf::from(&bridge.home));
    let base = crate::bootstrap::get_bootstrap(db, &ctx, &bridge.name, "hermes");
    if base.is_empty() {
        return base;
    }
    let hcom = hcom_command();
    if hcom == "hcom" {
        return base;
    }
    format!(
        "{base}\n\n[HCOM BRIDGE]\nRun hcom here as `{hcom}` (the plain `hcom` on PATH \
         talks to another hcom)."
    )
}

/// How an agent in a bridged chat must invoke this hcom: plain `hcom`, unless
/// this hcom runs with a non-default HCOM_DIR or a dev root (the gateway got
/// them from the plugin's hcom.json; the agent's login shell has neither).
fn hcom_command() -> String {
    let mut parts = Vec::new();
    if let Ok(dir) = std::env::var("HCOM_DIR")
        && !dir.is_empty()
        && Some(crate::paths::hcom_dir()) != dirs::home_dir().map(|h| h.join(".hcom"))
    {
        parts.push(format!("HCOM_DIR={}", shell_words::quote(&dir)));
    }
    if let Ok(root) = std::env::var("HCOM_DEV_ROOT")
        && !root.is_empty()
    {
        parts.push(format!("HCOM_DEV_ROOT={}", shell_words::quote(&root)));
    }
    parts.push("hcom".to_string());
    parts.join(" ")
}

/// Make `bridge.name` a live, listening instance bound to its token. Returns
/// whether the row had to be (re)created.
fn activate(db: &HcomDb, bridge: &Bridge, notify_port: Option<u16>) -> anyhow::Result<bool> {
    let name = &bridge.name;
    // A generated name arrives as a reservation placeholder: still new.
    let recreated = db
        .get_instance_full(name)?
        .is_none_or(|row| row.status == crate::instance_names::PLACEHOLDER_STATUS);
    if recreated {
        // New, or stopped while its gateway was down: keep its cursor.
        let stop = last_stop(db, name);
        let cursor = stop
            .as_ref()
            .and_then(|s| s["snapshot"]["last_event_id"].as_i64())
            .unwrap_or_else(|| db.get_last_event_id());
        crate::instance_binding::initialize_instance_in_position_file(
            db,
            name,
            None,
            None,
            None,
            None,
            Some(&Path::new(&bridge.home).join("state.db").to_string_lossy()),
            Some("hermes"),
            false,
            None,
            None,
            None,
            None,
            Some(&bridge.home),
        );
        let mut updates = serde_json::Map::new();
        updates.insert("last_event_id".into(), json!(cursor));
        crate::instances::update_instance_position(db, name, &updates);
        db.log_event(
            "life",
            name,
            &json!({"action": "started", "tool": "hermes", "name": name}),
        )?;
        db.delete_process_bindings_for_instance(name)?;
        db.set_process_binding(&bridge.token, "", name)?;
        // Back on the chat session it was on, for transcripts and status.
        if let Some(session_id) = stop
            .as_ref()
            .and_then(|s| s["snapshot"]["session_id"].as_str())
            .filter(|s| !s.is_empty())
        {
            bind_session(db, bridge, session_id)?;
        }
        lifecycle::set_status(db, name, ST_LISTENING, "start", Default::default());
    } else if db.get_process_binding(&bridge.token)?.as_deref() != Some(name.as_str()) {
        db.set_process_binding(&bridge.token, "", name)?;
    }
    mark(db, bridge);
    db.update_heartbeat(name)?;
    if let Some(port) = notify_port {
        let known = db.get_notify_endpoint_port(name, "plugin");
        if known != Some(port) {
            db.upsert_notify_endpoint(name, "plugin", port)?;
            crate::notify::wake(db, name, crate::notify::WakeKind::DELIVERY_LOOPS);
        }
    }
    Ok(recreated)
}

/// Record the chat on the instance (shown by `hcom list` as HERMES:<PLATFORM>),
/// and keep the row a Hermes one: a row an older hcom wrote without them reads
/// as `claude` with no transcript until the next sync repairs it.
fn mark(db: &HcomDb, bridge: &Bridge) {
    let context = json!({"hermes_bridge": {"platform": bridge.platform, "chat": bridge.chat}});
    let mut updates = serde_json::Map::new();
    updates.insert("launch_context".into(), json!(context.to_string()));
    updates.insert("tool".into(), json!("hermes"));
    updates.insert("directory".into(), json!(bridge.home));
    updates.insert(
        "transcript_path".into(),
        json!(Path::new(&bridge.home).join("state.db").to_string_lossy()),
    );
    crate::instances::update_instance_position(db, &bridge.name, &updates);
}

fn last_stop(db: &HcomDb, name: &str) -> Option<Value> {
    db.conn()
        .query_row(
            "SELECT data FROM events WHERE type='life' AND instance=?1 \
             AND json_extract(data, '$.action')='stopped' ORDER BY id DESC LIMIT 1",
            [name],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
}

/// What the plugin needs to serve a bridge.
fn entry(db: &HcomDb, bridge: &Bridge, recreated: bool) -> Value {
    json!({
        "name": bridge.name,
        "token": bridge.token,
        "session_key": bridge.session_key,
        "origin": bridge.origin,
        "visibility": bridge.visibility,
        "bootstrap": bootstrap(db, bridge),
        "recreated": recreated,
    })
}

fn reply(bridge: &Bridge, db: &HcomDb) -> (i32, String) {
    (0, entry(db, bridge, false).to_string())
}

/// `add --home H --session-key K --platform P [--origin JSON] [--chat C]
/// [--session-id S] [--name N]`: `/hcom start` (idempotent per chat).
fn add(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let (Some(home), Some(session_key), Some(platform)) = (
        parse_flag(argv, "--home").filter(|h| !h.is_empty()),
        parse_flag(argv, "--session-key"),
        parse_flag(argv, "--platform"),
    ) else {
        return plugin_rpc::error("Missing --home, --session-key or --platform");
    };
    let home = super::hermes::home_key(Path::new(&home));
    let find = |db: &HcomDb| {
        bridges(db)
            .into_iter()
            .find(|b| b.home == home && b.session_key == session_key)
    };
    let requested = parse_flag(argv, "--name").filter(|n| !n.is_empty());
    if let Some(name) = &requested
        && !crate::identity::is_valid_base_name(name)
    {
        return plugin_rpc::error(crate::identity::base_name_error(name));
    }
    // The bridge delivers through Hermes's public inject_message, which needs
    // the profile's injection grant: starting hcom in a chat is the opt-in.
    // Outside the write scope: it runs Hermes's CLI.
    if find(db).is_none()
        && let Err(e) = super::hermes::grant_injection(Path::new(&home))
    {
        return plugin_rpc::error(format!("{e:#}"));
    }
    // One write scope from the per-chat check to the save: concurrent starts
    // in a chat converge on one bridge, and a name is taken once.
    let added = db.with_write_scope(|| {
        if let Some(existing) = find(db) {
            activate(db, &existing, None)?;
            return Ok(Err(existing));
        }
        let name = match requested.clone() {
            Some(name) => {
                if bridge(db, &name).is_some() || db.get_instance_full(&name)?.is_some() {
                    anyhow::bail!("The name {name} is taken");
                }
                name
            }
            None => crate::instance_names::generate_unique_name(db)?,
        };
        let bridge = Bridge {
            name,
            token: format!("hermes-bridge-{}", uuid::Uuid::new_v4()),
            home: home.clone(),
            session_key: session_key.clone(),
            platform: platform.clone(),
            chat: parse_flag(argv, "--chat").unwrap_or_default(),
            origin: parse_flag(argv, "--origin")
                .and_then(|raw| serde_json::from_str(&raw).ok())
                .unwrap_or(Value::Null),
            visibility: Visibility::default(),
        };
        save(db, &bridge)?;
        activate(db, &bridge, None)?;
        Ok(Ok(bridge))
    });
    let bridge = match added {
        Ok(Ok(bridge)) => bridge,
        Ok(Err(existing)) => return reply(&existing, db),
        Err(e) => return plugin_rpc::error(format!("Cannot add bridge: {e:#}")),
    };
    if let Some(session_id) = parse_flag(argv, "--session-id").filter(|s| !s.is_empty())
        && let Err(e) = bind_session(db, &bridge, &session_id)
    {
        return plugin_rpc::error(format!("Cannot bind bridge session: {e:#}"));
    }
    crate::log::log_info(
        "hooks",
        "hermes_bridge.add",
        &format!("name={} platform={}", bridge.name, bridge.platform),
    );
    reply(&bridge, db)
}

/// `sync --home H [--notify-port P] [--ensure-grant] [--plugin-hash X]`: every bridge of the
/// profile, made live for the gateway serving it. A row that vanished (stale
/// cleanup while the gateway was down, a DB reset) is re-created with its
/// cursor and session; one stopped by `hcom kill` is forgotten. Anything not
/// in the reply has stopped: the plugin stops serving it.
fn sync(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let Some(home) = parse_flag(argv, "--home").filter(|h| !h.is_empty()) else {
        return plugin_rpc::error("Missing --home");
    };
    // Tokens go to any caller naming the profile: hcom's local trust model
    // (any local process can read the DB) makes the attachment check moot.
    let home = super::hermes::home_key(Path::new(&home));
    // At gateway start: the profile may have been reset since the start, or
    // the user removed the grant; a bridge cannot deliver without it.
    let registered: Vec<Bridge> = bridges(db).into_iter().filter(|b| b.home == home).collect();
    if !registered.is_empty()
        && argv.iter().any(|a| a == "--ensure-grant")
        && let Err(e) = super::hermes::grant_injection(Path::new(&home))
    {
        return plugin_rpc::error(format!("{e:#}"));
    }
    let reload = super::hermes::note_gateway_plugin(
        db,
        Path::new(&home),
        parse_flag(argv, "--plugin-hash").as_deref(),
    );
    let port = parse_flag(argv, "--notify-port").and_then(|p| p.parse().ok());
    let mut entries = Vec::new();
    for bridge in registered {
        // Re-checked under the write lock: a stop between the listing and
        // here must not be undone by re-activating the instance.
        let activated = db.with_write_scope(|| {
            if self::bridge(db, &bridge.name).as_ref() != Some(&bridge) {
                return Ok(None);
            }
            if db.get_instance_full(&bridge.name)?.is_none()
                && last_stop(db, &bridge.name).is_some_and(|stop| stop["reason"] == "killed")
            {
                forget(db, &bridge.name, "system", "killed")?;
                return Ok(None);
            }
            activate(db, &bridge, port).map(Some)
        });
        match activated {
            Ok(Some(recreated)) => entries.push(entry(db, &bridge, recreated)),
            Ok(None) => {}
            Err(e) => crate::log::log_warn(
                "hooks",
                "hermes_bridge.activate_failed",
                &format!("name={} err={e:#}", bridge.name),
            ),
        }
    }
    // The gateway loaded an older plugin than the one now on disk.
    (
        0,
        json!({"bridges": entries, "reload_plugin": reload}).to_string(),
    )
}

// A bridge's identity belongs to its chat route, even when the Hermes session
// was previously attached to a stopped CLI or an older bridge.
fn bind_session(db: &HcomDb, bridge: &Bridge, session_id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!session_id.is_empty(), "Empty session ID");
    db.with_write_scope(|| {
        anyhow::ensure!(
            db.get_instance_full(&bridge.name)?.is_some(),
            "Bridge is not active"
        );
        db.delete_session_bindings_for_instance(&bridge.name)?;
        db.rebind_session(session_id, &bridge.name)?;
        let mut updates = serde_json::Map::new();
        updates.insert("session_id".into(), json!(session_id));
        db.update_instance_fields(&bridge.name, &updates)?;
        db.set_process_binding(&bridge.token, session_id, &bridge.name)?;
        Ok(())
    })
}

/// `session --name N --session-id S`: the chat's gateway session (rotates on
/// /new and idle resets), for transcripts and status.
fn session(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let (Some(name), Some(session_id)) =
        (parse_flag(argv, "--name"), parse_flag(argv, "--session-id"))
    else {
        return plugin_rpc::error("Missing --name or --session-id");
    };
    let Some(bridge) = bridge(db, &name) else {
        return plugin_rpc::error(format!("{name} is not a bridge (stopped)"));
    };
    if let Err(e) = bind_session(db, &bridge, &session_id) {
        return plugin_rpc::error(format!("Cannot bind bridge session: {e:#}"));
    }
    (0, json!({"ok": true}).to_string())
}

/// `rm --name N`: `/hcom stop`.
fn rm(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let Some(name) = parse_flag(argv, "--name") else {
        return plugin_rpc::error("Missing --name");
    };
    // By name alone, like `hcom stop`: hcom's local trust model.
    match forget(db, &name, "system", DISCONNECTED) {
        Ok(Some(_)) => (0, json!({"ok": true}).to_string()),
        Ok(None) => plugin_rpc::error(format!("{name} is not a bridge")),
        Err(e) => plugin_rpc::error(format!("Cannot stop {name}: {e:#}")),
    }
}

/// `visibility --name N --mode off|preview|full`: persisted per chat.
fn visibility(db: &HcomDb, argv: &[String]) -> (i32, String) {
    let (Some(name), Some(mode)) = (parse_flag(argv, "--name"), parse_flag(argv, "--mode")) else {
        return plugin_rpc::error("Missing --name or --mode");
    };
    let visibility = match mode.as_str() {
        "off" => Visibility::Off,
        "preview" => Visibility::Preview,
        "full" => Visibility::Full,
        _ => return plugin_rpc::error("Visibility must be off, preview or full"),
    };
    match db.with_write_scope(|| {
        let mut bridge =
            bridge(db, &name).ok_or_else(|| anyhow::anyhow!("{name} is not a bridge"))?;
        bridge.visibility = visibility;
        save(db, &bridge)?;
        Ok(bridge)
    }) {
        Ok(bridge) => reply(&bridge, db),
        Err(e) => plugin_rpc::error(e),
    }
}

/// `hcom hermes-bridge <sub> ...` (argv starts with the hook name).
pub fn dispatch(argv: &[String]) -> (i32, String) {
    let ctx = HcomContext::from_os();
    crate::paths::ensure_hcom_directories_at(&ctx.hcom_dir);
    let db = match HcomDb::open() {
        Ok(db) => db,
        Err(e) => return plugin_rpc::error(format!("DB open failed: {e}")),
    };
    let args = argv.get(1..).unwrap_or_default();
    let (sub, rest) = args
        .split_first()
        .map_or(("", &[][..]), |(s, r)| (s.as_str(), r));
    if matches!(sub, "session" | "visibility")
        && let Err(reply) = plugin_rpc::require_attached(&ctx, &db, rest)
    {
        return reply;
    }
    super::common::dispatch_with_panic_guard(
        "hermes",
        "hermes-bridge",
        plugin_rpc::error("internal panic"),
        || match sub {
            "add" => add(&db, rest),
            "sync" => sync(&db, rest),
            "session" => session(&db, rest),
            "rm" => rm(&db, rest),
            "visibility" => visibility(&db, rest),
            _ => plugin_rpc::error(format!("Unknown hermes-bridge command: {sub}")),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    fn test_db() -> (tempfile::TempDir, HcomDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&dir.path().join("test.db")).unwrap();
        (dir, db)
    }

    fn sample(db: &HcomDb, home: &Path, name: &str) -> Bridge {
        let bridge = Bridge {
            name: name.into(),
            token: format!("tok-{name}"),
            home: super::super::hermes::home_key(home),
            session_key: format!("agent:work:irc:dm:{name}"),
            platform: "irc".into(),
            chat: "anno".into(),
            origin: json!({"platform": "irc", "chat_id": "anno"}),
            visibility: Visibility::Off,
        };
        save(db, &bridge).unwrap();
        bridge
    }

    #[test]
    fn visibility_is_persisted_per_bridge_and_validated() {
        let (dir, db) = test_db();
        sample(&db, dir.path(), "kiri");
        sample(&db, dir.path(), "luna");
        assert_eq!(bridge(&db, "kiri").unwrap().visibility, Visibility::Off);
        for mode in ["preview", "full", "off"] {
            let out = json_reply(visibility(&db, &sv(&["--name", "kiri", "--mode", mode])));
            assert_eq!(out["visibility"], mode);
            assert_eq!(
                serde_json::to_value(bridge(&db, "kiri").unwrap().visibility).unwrap(),
                mode
            );
            assert_eq!(bridge(&db, "luna").unwrap().visibility, Visibility::Off);
            let synced = json_reply(sync(&db, &sv(&["--home", dir.path().to_str().unwrap()])));
            assert!(
                synced["bridges"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|b| b["visibility"].as_str().is_some())
            );
        }
        assert!(
            json_reply(visibility(&db, &sv(&["--name", "kiri", "--mode", "bad"])))
                .get("error")
                .is_some()
        );
        assert_eq!(bridge(&db, "kiri").unwrap().visibility, Visibility::Off);
        assert!(
            json_reply(visibility(
                &db,
                &sv(&["--name", "missing", "--mode", "full"])
            ))
            .get("error")
            .is_some()
        );
        let mut old = serde_json::to_value(bridge(&db, "kiri").unwrap()).unwrap();
        old.as_object_mut().unwrap().remove("visibility");
        assert_eq!(
            serde_json::from_value::<Bridge>(old).unwrap().visibility,
            Visibility::Preview
        );
    }

    fn json_reply(out: (i32, String)) -> Value {
        assert_eq!(out.0, 0);
        serde_json::from_str(&out.1).unwrap()
    }

    fn sync_names(db: &HcomDb, home: &Path) -> Vec<String> {
        let out = json_reply(sync(db, &sv(&["--home", &home.to_string_lossy()])));
        out["bridges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["name"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn sync_repairs_a_bridge_row_written_as_another_tool() {
        let (dir, db) = test_db();
        sample(&db, dir.path(), "kiri");
        let home = super::super::hermes::home_key(dir.path());
        sync(&db, &sv(&["--home", &home]));
        let mut broken = serde_json::Map::new();
        broken.insert("tool".into(), json!("claude"));
        broken.insert("transcript_path".into(), json!(""));
        broken.insert("directory".into(), json!(""));
        crate::instances::update_instance_position(&db, "kiri", &broken);

        let out = json_reply(sync(&db, &sv(&["--home", &home])));
        assert_eq!(out["bridges"][0]["recreated"], false);
        let inst = db.get_instance_full("kiri").unwrap().unwrap();
        assert_eq!(inst.tool, "hermes");
        assert_eq!(inst.directory, home);
        assert_eq!(
            inst.transcript_path,
            Path::new(&home).join("state.db").to_string_lossy()
        );
    }

    #[test]
    #[serial_test::serial] // the bootstrap's hcom command reads HCOM_DIR
    fn sync_makes_listening_participants_bound_to_their_tokens() {
        let (dir, db) = test_db();
        sample(&db, dir.path(), "kiri");
        let out = json_reply(sync(
            &db,
            &sv(&[
                "--home",
                &dir.path().to_string_lossy(),
                "--notify-port",
                "4000",
            ]),
        ));
        let entry = &out["bridges"][0];
        assert_eq!(entry["name"], "kiri");
        assert_eq!(entry["token"], "tok-kiri");
        assert_eq!(entry["recreated"], true);
        let boot = entry["bootstrap"].as_str().unwrap();
        assert!(boot.contains("--name kiri"), "{boot}");
        let command = hcom_command();
        if command == "hcom" {
            assert!(!boot.contains("[HCOM BRIDGE]"), "{boot}");
        } else {
            assert!(boot.contains(&format!("`{command}`")), "{boot}");
        }
        let inst = db.get_instance_full("kiri").unwrap().unwrap();
        assert_eq!(inst.tool, "hermes");
        assert_eq!(inst.status, ST_LISTENING);
        assert!(inst.launch_context.unwrap().contains("hermes_bridge"));
        assert_eq!(
            db.get_process_binding("tok-kiri").unwrap().as_deref(),
            Some("kiri")
        );
        assert_eq!(db.get_notify_endpoint_port("kiri", "plugin"), Some(4000));
        // Other profiles' bridges are not this gateway's.
        assert!(sync_names(&db, Path::new("/elsewhere")).is_empty());
    }

    #[test]
    fn a_generated_name_starts_like_any_new_bridge() {
        let (dir, db) = test_db();
        // `/hcom start` without a name: the name arrives as a placeholder row.
        let name = crate::instance_names::generate_unique_name(&db).unwrap();
        sample(&db, dir.path(), &name);
        let out = json_reply(sync(&db, &sv(&["--home", &dir.path().to_string_lossy()])));
        assert_eq!(out["bridges"][0]["recreated"], true);
        let started: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE type='life' AND instance=?1 \
                 AND json_extract(data, '$.action')='started'",
                [&name],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(started, 1);
        assert_eq!(
            db.get_instance_full(&name).unwrap().unwrap().status,
            ST_LISTENING
        );
    }

    #[test]
    fn a_live_bridge_keeps_its_status_across_syncs() {
        let (dir, db) = test_db();
        sample(&db, dir.path(), "kiri");
        sync_names(&db, dir.path());
        lifecycle::set_status(&db, "kiri", "active", "prompt", Default::default());
        let out = json_reply(sync(&db, &sv(&["--home", &dir.path().to_string_lossy()])));
        assert_eq!(out["bridges"][0]["recreated"], false);
        assert_eq!(
            db.get_instance_full("kiri").unwrap().unwrap().status,
            "active"
        );
    }

    #[test]
    fn a_reaped_bridge_comes_back_with_its_cursor_and_session() {
        let (dir, db) = test_db();
        let bridge = sample(&db, dir.path(), "kiri");
        sync_names(&db, dir.path());
        bind_session(&db, &bridge, "g7").unwrap();
        let mut updates = serde_json::Map::new();
        updates.insert("last_event_id".into(), json!(42));
        crate::instances::update_instance_position(&db, "kiri", &updates);
        super::super::common::stop_instance(&db, "kiri", "system", "stale_cleanup");
        assert!(db.get_instance_full("kiri").unwrap().is_none());
        // The plugin's next read is refused until the next sync re-activates it.
        assert!(db.get_process_binding("tok-kiri").unwrap().is_none());
        assert_eq!(sync_names(&db, dir.path()), vec!["kiri"]);
        let inst = db.get_instance_full("kiri").unwrap().unwrap();
        assert_eq!(inst.last_event_id, 42);
        assert_eq!(inst.session_id.as_deref(), Some("g7"));
        assert_eq!(
            db.get_session_binding("g7").unwrap().as_deref(),
            Some("kiri")
        );
        assert_eq!(
            db.get_process_binding("tok-kiri").unwrap().as_deref(),
            Some("kiri")
        );
        assert_eq!(
            inst.launch_context_value("hermes_bridge", "platform")
                .as_deref(),
            Some("irc")
        );
    }

    #[test]
    fn a_killed_bridge_is_forgotten() {
        let (dir, db) = test_db();
        sample(&db, dir.path(), "kiri");
        sync_names(&db, dir.path());
        super::super::common::stop_instance(&db, "kiri", "bigboss", "killed");
        assert!(sync_names(&db, dir.path()).is_empty());
        assert!(bridge(&db, "kiri").is_none());
    }

    #[test]
    fn a_gateway_with_an_older_plugin_is_flagged_and_its_code_refreshed() {
        use super::super::hermes::{plugin_hash, stale_gateways};
        let (dir, db) = test_db();
        let home = super::super::hermes::home_key(dir.path());
        let plugin = super::super::hermes::plugin_dir(Path::new(&home));
        std::fs::create_dir_all(&plugin).unwrap();
        let old = "\"\"\"hcom plugin for Hermes Agent (old)\"\"\"\n";
        std::fs::write(plugin.join("__init__.py"), old).unwrap();
        // A plugin too old to report a hash: refreshed on disk, so reload.
        let reply = json_reply(sync(&db, &sv(&["--home", &home])));
        assert_eq!(reply["reload_plugin"], true);
        assert_eq!(stale_gateways(&db), vec![home.clone()]);
        let code = std::fs::read_to_string(plugin.join("__init__.py")).unwrap();
        assert_eq!(code, super::super::hermes::PLUGIN_SOURCE);
        // The reloaded copy reports the current hash.
        let current = sv(&["--home", &home, "--plugin-hash", &plugin_hash()]);
        assert_eq!(json_reply(sync(&db, &current))["reload_plugin"], false);
        assert!(stale_gateways(&db).is_empty());
    }

    #[test]
    fn a_foreign_plugin_is_never_reloaded_or_overwritten() {
        let (dir, db) = test_db();
        let home = super::super::hermes::home_key(dir.path());
        let plugin = super::super::hermes::plugin_dir(Path::new(&home));
        std::fs::create_dir_all(&plugin).unwrap();
        std::fs::write(plugin.join("__init__.py"), "def register(ctx): pass").unwrap();
        let reply = json_reply(sync(&db, &sv(&["--home", &home])));
        assert_eq!(reply["reload_plugin"], false);
        let code = std::fs::read_to_string(plugin.join("__init__.py")).unwrap();
        assert_eq!(code, "def register(ctx): pass");
    }

    #[test]
    fn a_cli_stop_disconnects_for_good() {
        let (dir, db) = test_db();
        sample(&db, dir.path(), "kiri");
        sync_names(&db, dir.path());
        let note = disconnect(&db, "kiri", "bigboss").unwrap();
        assert!(
            note.contains("Nothing was killed") && note.contains("/hcom start kiri in that chat"),
            "{note}"
        );
        let stop = last_stop(&db, "kiri").unwrap();
        assert_eq!(
            (stop["by"].as_str(), stop["reason"].as_str()),
            (Some("bigboss"), Some(DISCONNECTED))
        );
        // A plain instance stop would come back here; a disconnect does not.
        assert!(sync_names(&db, dir.path()).is_empty());
        assert!(disconnect(&db, "kiri", "bigboss").is_none());
    }

    #[test]
    fn stop_forgets_only_that_bridge() {
        let (dir, db) = test_db();
        sample(&db, dir.path(), "kiri");
        sample(&db, dir.path(), "sora");
        sync_names(&db, dir.path());
        assert_eq!(
            json_reply(rm(&db, &sv(&["--name", "kiri"]))),
            json!({"ok": true})
        );
        assert!(db.get_instance_full("kiri").unwrap().is_none());
        assert_eq!(last_stop(&db, "kiri").unwrap()["reason"], DISCONNECTED);
        assert_eq!(sync_names(&db, dir.path()), vec!["sora"]);
        assert!(
            json_reply(rm(&db, &sv(&["--name", "kiri"])))
                .get("error")
                .is_some()
        );
    }

    #[test]
    fn concurrent_starts_in_a_chat_converge_on_one_bridge() {
        let (dir, _db) = test_db();
        let path = dir.path().join("test.db");
        let home = super::super::hermes::home_key(dir.path());
        // An existing bridge for the chat skips the grant (which runs Hermes).
        let seed = HcomDb::open_at(&path).unwrap();
        sample(&seed, dir.path(), "kiri");
        let key = bridge(&seed, "kiri").unwrap().session_key;
        let threads: Vec<_> = (0..6)
            .map(|_| {
                let (path, home, key) = (path.clone(), home.clone(), key.clone());
                std::thread::spawn(move || {
                    let db = HcomDb::open_at(&path).unwrap();
                    let argv = sv(&["--home", &home, "--session-key", &key, "--platform", "irc"]);
                    json_reply(add(&db, &argv))["name"]
                        .as_str()
                        .unwrap()
                        .to_string()
                })
            })
            .collect();
        for t in threads {
            assert_eq!(t.join().unwrap(), "kiri");
        }
        assert_eq!(bridges(&seed).len(), 1);
        // A home spelled another way is the same profile.
        let dotted = dir.path().join(".").to_string_lossy().into_owned();
        let argv = sv(&[
            "--home",
            &dotted,
            "--session-key",
            &key,
            "--platform",
            "irc",
        ]);
        assert_eq!(json_reply(add(&seed, &argv))["name"], "kiri");
    }

    #[test]
    fn a_bridge_keeps_its_identity_when_following_a_stopped_agents_session() {
        let (dir, db) = test_db();
        let old = sample(&db, dir.path(), "memo");
        sync_names(&db, dir.path());
        bind_session(&db, &old, "shared").unwrap();
        forget(&db, "memo", "system", "test").unwrap();
        let current = sample(&db, dir.path(), "kilo");
        sync_names(&db, dir.path());
        bind_session(&db, &current, "previous").unwrap();
        let out = json_reply(session(
            &db,
            &sv(&["--name", "kilo", "--session-id", "shared"]),
        ));
        assert_eq!(out["ok"], true);
        assert!(db.get_instance_full("memo").unwrap().is_none());
        let inst = db.get_instance_full("kilo").unwrap().unwrap();
        assert_eq!(inst.session_id.as_deref(), Some("shared"));
        assert_eq!(inst.status, ST_LISTENING);
        assert_eq!(
            db.get_session_binding("shared").unwrap().as_deref(),
            Some("kilo")
        );
        assert!(db.get_session_binding("previous").unwrap().is_none());
        assert_eq!(
            db.get_process_binding("tok-kilo").unwrap().as_deref(),
            Some("kilo")
        );
        let out = json_reply(session(&db, &sv(&["--name", "kilo", "--session-id", ""])));
        assert!(out["error"].is_string());
        assert_eq!(
            db.get_instance_full("kilo")
                .unwrap()
                .unwrap()
                .session_id
                .as_deref(),
            Some("shared")
        );
    }

    #[test]
    fn a_session_rebind_keeps_the_chat_label() {
        let (dir, db) = test_db();
        sample(&db, dir.path(), "kiri");
        sync_names(&db, dir.path());
        session(&db, &sv(&["--name", "kiri", "--session-id", "g2"]));
        let inst = db.get_instance_full("kiri").unwrap().unwrap();
        assert_eq!(inst.session_id.as_deref(), Some("g2"));
        assert_eq!(
            inst.launch_context_value("hermes_bridge", "platform")
                .as_deref(),
            Some("irc")
        );
    }
}
