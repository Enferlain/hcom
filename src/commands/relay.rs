//! `hcom relay` command — cross-device sync via MQTT pub/sub.

use crate::config;
use crate::db::HcomDb;
use crate::relay::broker::BrokerProbe;
use crate::relay::token::{DecodedToken, encode_join_token};
use crate::relay::{self, DEFAULT_BROKERS};
use crate::shared::CommandContext;
use crate::shared::ansi::{FG_GRAY, FG_GREEN, FG_RED, FG_YELLOW, RESET};
use crate::shared::time::format_age;

/// Parsed arguments for `hcom relay`.
#[derive(clap::Parser, Debug)]
#[command(name = "relay", about = "Cross-device sync via MQTT")]
pub struct RelayArgs {
    /// Subcommand and arguments (new/connect/off/status + flags)
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

/// Parse --broker and --password flags from argv.
fn parse_broker_flags(argv: &[String]) -> (Option<String>, Option<String>, Vec<String>) {
    let mut broker = None;
    let mut auth_token = None;
    let mut remaining = Vec::new();
    let mut i = 0;
    while i < argv.len() {
        if argv[i] == "--broker" && i + 1 < argv.len() {
            broker = Some(argv[i + 1].clone());
            i += 2;
        } else if argv[i] == "--password" && i + 1 < argv.len() {
            auth_token = Some(argv[i + 1].clone());
            i += 2;
        } else {
            remaining.push(argv[i].clone());
            i += 1;
        }
    }
    (broker, auth_token, remaining)
}

/// Ping a broker via TCP connect (+ TLS handshake when use_tls=true).
/// Returns round-trip ms or None on failure.
fn ping_broker(host: &str, port: u16, use_tls: bool) -> Option<u32> {
    crate::relay::broker::ping_broker(host, port, use_tls).map(|ms| ms as u32)
}

/// Decode a join token. Returns the parsed structure including the optional
/// PSK; legacy tokens have `psk = None` and are rejected by `relay_connect`.
fn decode_join_token(token: &str) -> Option<DecodedToken> {
    relay::token::decode_join_token(token)
}

/// Build the "Add devices" / "current group" join token from current config,
/// or `None` if the relay isn't fully configured.
fn current_join_token(config: &crate::config::HcomConfig) -> Option<String> {
    if config.relay_id.is_empty() || config.relay.is_empty() {
        return None;
    }
    let psk = relay::load_psk(config).ok()?;
    encode_join_token(&config.relay_id, &config.relay, &psk)
}

/// Show the join token (separate command to avoid leaking in status output).
fn relay_show_token() -> i32 {
    let config = config::load_config_snapshot().core;
    match current_join_token(&config) {
        Some(token) => {
            println!("hcom relay connect {token}");
            if !config.relay_token.is_empty() {
                println!("  (also needs: --password <secret>)");
            }
            0
        }
        None => {
            eprintln!("No relay configured. Run: hcom relay new");
            1
        }
    }
}

fn validate_existing_relay_config(config: &crate::config::HcomConfig) -> Result<(), String> {
    if config.relay_id.is_empty() {
        return Err("no relay configured".to_string());
    }
    if config.relay.trim().is_empty() {
        return Err("relay broker URL is missing".to_string());
    }
    if relay::parse_broker_url(&config.relay).is_none() {
        return Err(format!("relay broker URL is invalid: {}", config.relay));
    }
    relay::load_psk(config).map(|_| ())
}

/// Format a timestamp as relative age.
fn format_time(timestamp: f64) -> String {
    if timestamp == 0.0 {
        return "never".to_string();
    }
    let now = crate::shared::time::now_epoch_f64();
    let age = (now - timestamp) as i64;
    if age <= 0 {
        return "just now".to_string();
    }
    format!("{} ago", format_age(age))
}

/// Get device short ID via FNV-1a hash
/// Auto-creates device_id file if missing (via read_device_uuid).
/// Returns "?" when the device_id file cannot be created or read — display only.
fn get_device_short_id(db: &HcomDb) -> String {
    match crate::relay::read_device_uuid() {
        Some(uuid) => crate::relay::device_short_id_for_db(db, &uuid),
        None => "?".to_string(),
    }
}

fn paint(code: &str, text: &str) -> String {
    use std::io::IsTerminal;

    let enabled = std::io::stdout().is_terminal()
        && std::env::var_os("NO_COLOR").is_none()
        && std::env::var("TERM").as_deref() != Ok("dumb");
    if enabled {
        format!("{code}{text}{RESET}")
    } else {
        text.to_string()
    }
}

fn is_public_broker(broker: &str) -> bool {
    DEFAULT_BROKERS
        .iter()
        .any(|&(h, p)| broker == format!("mqtts://{h}:{p}") || broker == format!("mqtt://{h}:{p}"))
}

/// Hint for a broker error that looks like rejected credentials on a private broker.
fn auth_hint(config: &crate::config::HcomConfig, error: Option<&str>) -> Option<&'static str> {
    let err = error.unwrap_or("").to_ascii_lowercase();
    let auth_error =
        err.contains("password") || err.contains("auth") || err.contains("not authorized");
    if !auth_error || is_public_broker(&config.relay) {
        return None;
    }
    Some(if config.relay_token.is_empty() {
        "this broker needs a password: hcom relay connect <token> --password <secret>"
    } else {
        "broker rejected the password: hcom relay connect <token> --password <secret>"
    })
}

/// Peer agents shown per device in `hcom relay` before collapsing to "+N".
const MAX_LISTED_AGENTS: usize = 5;

fn sync_time_of(sync_map: &std::collections::HashMap<String, String>, device_id: &str) -> f64 {
    sync_map
        .get(&format!("relay_sync_time_{device_id}"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

/// Show relay status.
fn relay_status(db: &HcomDb) -> i32 {
    let config = config::load_config_snapshot().core;

    if config.relay_id.is_empty() {
        println!("{}", paint(FG_GRAY, "Relay: not configured"));
        println!("Run: hcom relay new");
        return 0;
    }

    if !config.relay_enabled {
        println!("{}", paint(FG_YELLOW, "Relay: disabled"));
        println!("\nRun: hcom relay on");
        return 0;
    }

    // All status display branches off the canonical RelayHealth derivation so
    // CLI / TUI / JSON can't drift in interpretation of the underlying KV.
    let health = relay::relay_health(&config, db);
    match &health {
        relay::RelayHealth::Connected => {
            println!("Status:    {}", paint(FG_GREEN, "connected"));
        }
        relay::RelayHealth::Starting { pid } => {
            println!(
                "Status:    {} (PID {pid}, awaiting connect)",
                paint(FG_YELLOW, "starting")
            );
        }
        relay::RelayHealth::Stale { age_s, pid } => {
            println!(
                "Status:    {} — worker unresponsive (PID {pid}, {:.0}s since last heartbeat)",
                paint(FG_YELLOW, "stale"),
                age_s
            );
            println!("  Fix: hcom relay daemon restart");
        }
        relay::RelayHealth::Waiting => {
            println!(
                "Status:    {} — daemon not running (start: hcom relay daemon start)",
                paint(FG_YELLOW, "stopped")
            );
        }
        relay::RelayHealth::Error {
            reason,
            detail,
            pid,
        } => {
            println!(
                "Status:    {} — {}",
                paint(FG_RED, "error"),
                reason.clone().label(detail.as_deref(), *pid)
            );
            match reason {
                relay::RelayErrorReason::Reported => match auth_hint(&config, detail.as_deref()) {
                    Some(hint) => println!("  Hint: {hint}"),
                    None => println!("  If this persists: hcom relay daemon restart"),
                },
                relay::RelayErrorReason::StalePidfile | relay::RelayErrorReason::Ghost => {
                    println!("  Fix: hcom relay daemon restart");
                }
            }
        }
        // NotConfigured / Disabled never reach here — the early returns above
        // (config.relay_id empty / !config.relay_enabled) handled them already.
        relay::RelayHealth::NotConfigured | relay::RelayHealth::Disabled => {
            println!("Status:    {}", paint(FG_YELLOW, "waiting"));
        }
    }

    // Broker info
    if !config.relay.is_empty() {
        if let Some((host, port, use_tls)) = relay::parse_broker_url(&config.relay) {
            if let Some(ms) = ping_broker(&host, port, use_tls) {
                println!("Broker:    {} ({ms}ms)", config.relay);
            } else {
                println!("Broker:    {} (unreachable)", config.relay);
            }
        } else {
            println!("Broker:    {}", config.relay);
        }
    } else {
        println!("Broker:    auto (public fallback)");
    }

    // Queued events
    let last_push_id: i64 = db
        .kv_get("relay_last_push_id")
        .ok()
        .flatten()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let queued: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE id > ?1
             AND instance NOT LIKE '%:%'
             AND instance != '_device'
             AND json_extract(data, '$._relay') IS NULL",
            rusqlite::params![last_push_id],
            |r| r.get(0),
        )
        .unwrap_or(0);

    if queued > 0 {
        println!("Queued:    {queued} events pending");
    } else {
        println!("Queued:    up to date");
    }

    // Last push
    let last_push: f64 = db
        .kv_get("relay_last_push")
        .ok()
        .flatten()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    if last_push > 0.0 {
        println!("Last push: {}", format_time(last_push));
    } else {
        println!("Last push: never");
    }

    let own_device = crate::relay::read_device_uuid().unwrap_or_default();
    let now = crate::shared::time::now_epoch_f64();

    let mut device_to_short = std::collections::HashMap::new();
    if let Ok(entries) = db.kv_prefix("relay_short_") {
        for (key, device_id) in entries {
            if device_id == own_device {
                continue;
            }
            let short = key.strip_prefix("relay_short_").unwrap_or(&key).to_string();
            device_to_short.insert(device_id, short);
        }
    }

    let sync_map: std::collections::HashMap<String, String> = db
        .kv_prefix("relay_sync_time_")
        .unwrap_or_default()
        .into_iter()
        .collect();

    let mut agent_names: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    if let Ok(mut stmt) = db.conn().prepare(
        "SELECT origin_device_id, name FROM instances \
         WHERE origin_device_id IS NOT NULL AND origin_device_id != '' \
         ORDER BY name",
    ) && let Ok(rows) = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    }) {
        for (device_id, name) in rows.filter_map(|r| r.ok()) {
            let base = name.split(':').next().unwrap_or(&name).to_string();
            agent_names.entry(device_id).or_default().push(base);
        }
    }

    println!("\nDevice:    {}", get_device_short_id(db));
    let mut sorted_devices: Vec<_> = device_to_short.iter().collect();
    sorted_devices.sort_by(|a, b| a.1.cmp(b.1));
    // Online peers first; within each group, alphabetical by short ID.
    sorted_devices.sort_by_key(|(device_id, _)| {
        let sync_time = sync_time_of(&sync_map, device_id);
        !(sync_time > 0.0 && (now - sync_time) <= relay::DEVICE_STALE_SECS)
    });

    if sorted_devices.is_empty() {
        println!("Peers:     none yet — join from another device (token: hcom relay token)");
    } else {
        println!("Peers:");
        for (device_id, short) in sorted_devices {
            let sync_time = sync_time_of(&sync_map, device_id);
            let is_online = sync_time > 0.0 && (now - sync_time) <= relay::DEVICE_STALE_SECS;
            let state = if is_online {
                paint(FG_GREEN, "online")
            } else if sync_time > 0.0 {
                paint(FG_GRAY, &format!("seen {}", format_time(sync_time)))
            } else {
                paint(FG_GRAY, "not seen")
            };
            let agents = match agent_names.get(device_id.as_str()) {
                Some(names) if names.len() > MAX_LISTED_AGENTS => format!(
                    "{} +{}",
                    names[..MAX_LISTED_AGENTS].join(", "),
                    names.len() - MAX_LISTED_AGENTS
                ),
                Some(names) => names.join(", "),
                None if is_online => "no agents".to_string(),
                None => String::new(),
            };
            let line = format!("  {short}  {state}  {agents}");
            println!("{}", line.trim_end());
        }
    }

    // Show key fingerprint so two devices can visually verify they share the
    // same PSK without anyone leaking material.
    if let Ok(psk) = relay::load_psk(&config) {
        println!("Key:       {}", relay::crypto::fingerprint(&psk));
    } else {
        println!(
            "Key:       {} (run `hcom relay new`)",
            paint(FG_RED, "missing")
        );
    }

    println!("\nShow token: hcom relay token");

    0
}

/// Internal fast-path: wake the relay worker so queued events publish immediately.
fn relay_push() -> i32 {
    crate::relay::trigger_push();
    0
}

fn restart_relay_worker_for_config_change() -> bool {
    if crate::relay::worker::is_relay_worker_running() {
        crate::relay::worker::stop_relay_worker_blocking();
    }
    crate::relay::worker::ensure_worker()
}

/// How long `relay new/connect/on` wait for the broker handshake before
/// reporting back. Covers TLS + MQTT connect on a slow public broker.
const CONNECT_WAIT: std::time::Duration = std::time::Duration::from_secs(8);

/// Poll until the worker reports connected or a broker error, or the wait runs out.
fn await_broker_connection(db: &HcomDb) -> relay::RelayHealth {
    let config = config::load_config_snapshot().core;
    let deadline = std::time::Instant::now() + CONNECT_WAIT;
    loop {
        let health = relay::relay_health(&config, db);
        let settled = matches!(
            health,
            relay::RelayHealth::Connected | relay::RelayHealth::Error { .. }
        );
        if settled || std::time::Instant::now() >= deadline {
            return health;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Wait for the broker, then show full status. Fails on a broker error so
/// `relay on` exits like `relay new/connect` do.
fn status_after_connect(db: &HcomDb) -> i32 {
    let health = await_broker_connection(db);
    let rc = relay_status(db);
    if matches!(health, relay::RelayHealth::Error { .. }) {
        return 1;
    }
    rc
}

/// Restart the worker on new relay config and report whether it reached the
/// broker — not just whether a process started.
fn start_worker_and_report(db: &HcomDb) -> i32 {
    if !restart_relay_worker_for_config_change() && !crate::relay::worker::is_relay_worker_running()
    {
        eprintln!("\nCould not start daemon automatically. Run 'hcom relay daemon start'.");
        return 1;
    }
    match await_broker_connection(db) {
        relay::RelayHealth::Connected => {
            println!("\n{}", paint(FG_GREEN, "Connected."));
            0
        }
        relay::RelayHealth::Error {
            reason,
            detail,
            pid,
        } => {
            let config = config::load_config_snapshot().core;
            eprintln!("\nNot connected: {}", reason.label(detail.as_deref(), pid));
            if let Some(hint) = auth_hint(&config, detail.as_deref()) {
                eprintln!("  Hint: {hint}");
            }
            eprintln!("The daemon keeps retrying. Check with: hcom relay");
            1
        }
        _ => {
            println!(
                "\nDaemon running, broker not reached yet ({}s). Check with: hcom relay",
                CONNECT_WAIT.as_secs()
            );
            0
        }
    }
}

fn ensure_relay_worker_running_for_cli() -> bool {
    if crate::relay::worker::ensure_worker() {
        return true;
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if crate::relay::worker::is_relay_worker_running() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        if crate::relay::worker::ensure_worker() {
            return true;
        }
    }

    crate::relay::worker::is_relay_worker_running()
}

fn known_remote_device_shorts(db: &HcomDb) -> Vec<String> {
    let own_device = crate::relay::read_device_uuid().unwrap_or_default();
    let mut shorts = Vec::new();
    if let Ok(entries) = db.kv_prefix("relay_short_") {
        for (key, device_id) in entries {
            if device_id == own_device {
                continue;
            }
            shorts.push(key.strip_prefix("relay_short_").unwrap_or(&key).to_string());
        }
    }
    shorts.sort();
    shorts
}

fn relay_notify_off_all(db: &HcomDb, config: &crate::config::HcomConfig) {
    let peers = known_remote_device_shorts(db);
    if peers.is_empty() {
        println!("No known remote peers to notify.");
        return;
    }

    let mut sent = 0usize;
    for short in &peers {
        if relay::control::send_one_way_control_ephemeral(
            db,
            config,
            "relay_off",
            short,
            &serde_json::json!({}),
        ) {
            sent += 1;
        }
    }

    println!(
        "Best-effort relay_off sent to {sent}/{} known remote peer(s).",
        peers.len()
    );
}

fn relay_off(db: &HcomDb, argv: &[String]) -> i32 {
    let all = argv.iter().any(|a| a == "--all");
    if let Some(extra) = argv.iter().find(|a| *a != "--all") {
        return crate::commands::help::usage_error(
            "relay",
            &format!("Unexpected argument for hcom relay off: '{extra}'"),
        );
    }

    let config = config::load_config_snapshot().core;
    if config.relay_id.is_empty() {
        eprintln!("No relay configured.");
        eprintln!("Run: hcom relay new");
        return 1;
    }

    if all {
        if config.relay_enabled {
            relay_notify_off_all(db, &config);
        } else {
            println!("Relay already disabled locally; skipping remote shutdown broadcast.");
        }
    }

    match relay::control::disable_local_relay(&config, db) {
        Ok(cleared_remote_state) => {
            if cleared_remote_state {
                println!("Cleared remote state");
            }
        }
        Err(e) => {
            eprintln!("Error: {e}");
            return 1;
        }
    }

    crate::relay::worker::stop_relay_worker_blocking();
    println!("{}", paint(FG_YELLOW, "Relay: disabled"));
    println!("\nRun 'hcom relay on' to reconnect");
    0
}

/// Enable or disable relay sync.
fn relay_toggle(db: &HcomDb, enable: bool) -> i32 {
    let config = config::load_config_snapshot().core;

    if config.relay_id.is_empty() {
        eprintln!("No relay configured.");
        eprintln!("Run: hcom relay new");
        return 1;
    }

    if enable {
        if let Err(e) = validate_existing_relay_config(&config) {
            eprintln!("Error: {e}");
            return 1;
        }
        let config_path = crate::paths::config_toml_path();
        if let Ok(content) = std::fs::read_to_string(&config_path) {
            let new_content = update_toml_key(&content, "relay_enabled", "true");
            if let Err(e) = crate::config::write_config_toml_path(&config_path, &new_content) {
                eprintln!("Error: Failed to write config: {e}");
                return 1;
            }
        }
        println!("Relay enabled\n");
        if !ensure_relay_worker_running_for_cli() {
            eprintln!("Error: relay daemon could not be started");
            return 1;
        }
        status_after_connect(db)
    } else {
        relay_off(db, &[])
    }
}

/// Persist relay settings to config.toml.
/// Relay auth is cleared when no password is provided so stale credentials
/// don't poison future joins/reconfigurations.
fn render_relay_config_content(
    content: &str,
    relay_id: &str,
    broker: &str,
    auth_token: Option<&str>,
    psk_b64: &str,
) -> String {
    let mut content = update_toml_key(content, "relay_id", &format!("\"{relay_id}\""));
    content = update_toml_key(&content, "relay", &format!("\"{broker}\""));
    content = update_toml_key(&content, "relay_enabled", "true");
    content = update_toml_key(&content, "relay_psk", &format!("\"{psk_b64}\""));
    update_toml_key(
        &content,
        "relay_token",
        &format!("\"{}\"", auth_token.unwrap_or("")),
    )
}

fn persist_relay_config(
    relay_id: &str,
    broker: &str,
    auth_token: Option<&str>,
    psk: &[u8; 32],
) -> Result<(), String> {
    let config_path = crate::paths::config_toml_path();
    let content = std::fs::read_to_string(&config_path).unwrap_or_default();
    let psk_b64 = relay::encode_psk(psk);
    let content = render_relay_config_content(&content, relay_id, broker, auth_token, &psk_b64);
    crate::config::write_config_toml_path(&config_path, &content)
        .map_err(|e| format!("Failed to write config: {e}"))
}

/// Create a new relay group.
fn relay_new(db: &HcomDb, argv: &[String]) -> i32 {
    let (broker_url, auth_token, _) = parse_broker_flags(argv);

    // Ensure device_id file exists before spawning the daemon worker,
    // so both CLI and daemon use the same UUID (avoids TOCTOU race).
    if relay::read_device_uuid().is_none() {
        eprintln!("Error: failed to create device_id file");
        return 1;
    }

    let config = config::load_config_snapshot().core;

    // Show previous group token if switching
    if let Some(old_token) = current_join_token(&config) {
        println!("Current group: hcom relay connect {old_token}\n");
    }

    let relay_id = uuid::Uuid::new_v4().to_string();
    let psk = match relay::crypto::generate_psk() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error: failed to generate relay key: {e}");
            return 1;
        }
    };
    let pinned_broker = if let Some(broker) = &broker_url {
        // Private broker — test connectivity
        if let Some((host, port, use_tls)) = relay::parse_broker_url(broker) {
            println!("Testing {host}:{port}...");
            match relay::broker::ping_mqtt_broker(&host, port, use_tls, auth_token.as_deref()) {
                Ok(ms) => {
                    println!("  {host}:{port} — {ms}ms");
                    broker.clone()
                }
                Err(err) => {
                    eprintln!("  {host}:{port} — {err}");
                    eprintln!("\nBroker connection failed. Check network and credentials.");
                    return 1;
                }
            }
        } else {
            eprintln!("Invalid broker URL: {broker}");
            return 1;
        }
    } else {
        // Public broker — test all in parallel
        println!("Testing brokers...");
        let results = relay::broker::test_brokers_parallel(DEFAULT_BROKERS, auth_token.as_deref());
        let mut best = None;
        for (host, port, probe) in &results {
            match probe {
                BrokerProbe::Reachable(ms) => {
                    println!("  {host}:{port} — {ms}ms");
                    if best.is_none() {
                        best = Some(format!("mqtts://{host}:{port}"));
                    }
                }
                BrokerProbe::Failed(err) => println!("  {host}:{port} — failed ({err})"),
                BrokerProbe::Skipped => println!("  {host}:{port} — skipped"),
            }
        }
        match best {
            Some(b) => b,
            None => {
                eprintln!("\nNo broker accepted an MQTT connection. Check your network.");
                eprintln!("Or use a private broker: hcom relay new --broker mqtts://host:port");
                return 1;
            }
        }
    };

    // Clear the previous group's device state only once the new group is
    // certain to replace it: a failed probe above leaves the old config live.
    relay::clear_relay_device_state(db);

    // Save config
    if let Err(e) = persist_relay_config(&relay_id, &pinned_broker, auth_token.as_deref(), &psk) {
        eprintln!("Error: {e}");
        return 1;
    }

    // Generate join token (always v0x04 — includes relay PSK)
    if let Some(token) = encode_join_token(&relay_id, &pinned_broker, &psk) {
        println!("\nBroker: {pinned_broker}");
        println!(
            "Key:    {} (XChaCha20-Poly1305)",
            relay::crypto::fingerprint(&psk)
        );
        if auth_token.is_some() {
            println!("Password: set");
        }
        println!("\nOn other devices: hcom relay connect {token}");
        if auth_token.is_some() {
            println!("  (they will also need: --password <secret>)");
        }
    }

    start_worker_and_report(db)
}

/// Connect to relay — re-enable or join with token.
fn relay_connect(db: &HcomDb, argv: &[String]) -> i32 {
    let (broker_url, auth_token, remaining) = parse_broker_flags(argv);

    // Ensure device_id file exists before spawning the daemon worker,
    // so both CLI and daemon use the same UUID (avoids TOCTOU race).
    if relay::read_device_uuid().is_none() {
        eprintln!("Error: failed to create device_id file");
        return 1;
    }

    let token_str = remaining.first().filter(|s| !s.starts_with("-")).cloned();

    if token_str.is_none() {
        // Re-enable mode
        let config = config::load_config_snapshot().core;
        if config.relay_id.is_empty() {
            eprintln!("No relay configured.");
            eprintln!("Run: hcom relay new");
            return 1;
        }
        if let Err(e) = validate_existing_relay_config(&config) {
            eprintln!("Error: {e}");
            return 1;
        }
        if config.relay_enabled {
            println!("Relay already enabled.\n");
            if !ensure_relay_worker_running_for_cli() {
                eprintln!("Error: relay is enabled but the daemon could not be started");
                return 1;
            }
            return status_after_connect(db);
        }
        return relay_toggle(db, true);
    }

    let token_str = token_str.unwrap();

    // Decode token
    let decoded = match decode_join_token(&token_str) {
        Some(r) => r,
        None => {
            eprintln!("Invalid token.");
            return 1;
        }
    };

    let psk = match decoded.psk {
        Some(p) => p,
        None => {
            eprintln!(
                "Legacy token (v0x01/v0x02) — rejected.\n\
                 \n\
                 This hcom build requires a token that carries the relay key. Ask the source\n  \
                 device to upgrade hcom and run\n  \
                 hcom relay new\n\
                 then re-share the new token. The new token format is v0x04 (~67 chars)."
            );
            return 1;
        }
    };

    let relay_id = decoded.relay_id;
    let token_broker = decoded.broker_url;

    let effective_broker = broker_url.unwrap_or(token_broker);

    // Test broker connectivity
    let ping_ms = relay::parse_broker_url(&effective_broker)
        .and_then(|(host, port, use_tls)| ping_broker(&host, port, use_tls));

    let config = config::load_config_snapshot().core;

    // Switching groups: say so, with the way back, before dropping the old peers.
    if config.relay_id != relay_id {
        if let Some(old_token) = current_join_token(&config) {
            let old_key = relay::load_psk(&config)
                .map(|psk| relay::crypto::fingerprint(&psk))
                .unwrap_or_default();
            println!("Leaving group {old_key}. To rejoin: hcom relay connect {old_token}\n");
        }
        relay::clear_relay_device_state(db);
    }

    // Save config
    if let Err(e) = persist_relay_config(&relay_id, &effective_broker, auth_token.as_deref(), &psk)
    {
        eprintln!("Error: {e}");
        return 1;
    }

    if let Some(ms) = ping_ms {
        println!("Broker: {effective_broker} ({ms}ms)");
    } else {
        println!("Broker: {effective_broker}");
        eprintln!("  Warning: broker unreachable — check network or token");
    }
    println!("Key:    {}", relay::crypto::fingerprint(&psk));

    if auth_token.is_some() {
        println!("Password: set");
    } else {
        if !is_public_broker(&effective_broker) {
            println!("Password: not set (use --password if broker requires auth)");
        }
    }

    start_worker_and_report(db)
}

/// Update or add a key in TOML content (simple line-level editing).
/// Map flat relay field names to TOML section paths under [relay].
fn relay_toml_key(field: &str) -> (&str, &str) {
    match field {
        "relay" => ("relay", "url"),
        "relay_id" => ("relay", "id"),
        "relay_token" => ("relay", "token"),
        "relay_psk" => ("relay", "psk"),
        "relay_enabled" => ("relay", "enabled"),
        _ => panic!("unknown relay field: {field}"),
    }
}

/// Update a relay config field in config.toml using toml_edit for proper section handling.
fn update_toml_key(content: &str, field: &str, value: &str) -> String {
    let (section, key) = relay_toml_key(field);

    let mut doc = content
        .parse::<toml_edit::DocumentMut>()
        .unwrap_or_else(|_| toml_edit::DocumentMut::new());

    // Ensure section exists
    if !doc.contains_table(section) {
        doc[section] = toml_edit::Item::Table(toml_edit::Table::new());
    }

    // Parse the value appropriately
    if value == "true" || value == "false" {
        doc[section][key] = toml_edit::value(value == "true");
    } else if let Some(stripped) = value.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        doc[section][key] = toml_edit::value(stripped);
    } else {
        doc[section][key] = toml_edit::value(value);
    }

    doc.to_string()
}

pub fn cmd_relay(db: &HcomDb, args: &RelayArgs, _ctx: Option<&CommandContext>) -> i32 {
    // --name already stripped by router's extract_global_flags_full()
    let argv = &args.args;

    if argv.is_empty() {
        return relay_status(db);
    }

    let first = argv[0].as_str();

    if first == "--help" || first == "-h" {
        crate::commands::help::print_command_help("relay");
        return 0;
    }

    match first {
        "new" => relay_new(db, &argv[1..]),
        "connect" => relay_connect(db, &argv[1..]),
        "off" | "disconnect" => relay_off(db, &argv[1..]),
        "on" => relay_connect(db, &Vec::new()),
        "status" => relay_status(db),
        "token" => relay_show_token(),
        "push" => relay_push(),
        "daemon" => crate::commands::daemon::cmd_daemon(&argv[1..]),
        _ => {
            // Could be a token passed directly
            if argv[0].len() > 20 && !argv[0].starts_with('-') {
                relay_connect(db, argv)
            } else {
                crate::commands::help::unknown_subcommand(
                    "relay",
                    first,
                    &["new", "token", "connect", "on", "off", "daemon"],
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::test_helpers::isolated_test_env;
    use serial_test::serial;

    fn fake_psk() -> [u8; 32] {
        [0x44; 32]
    }

    #[test]
    fn test_encode_decode_public_broker_token() {
        let relay_id = uuid::Uuid::new_v4().to_string();
        let broker = format!("mqtts://{}:{}", DEFAULT_BROKERS[0].0, DEFAULT_BROKERS[0].1);
        let psk = fake_psk();
        let token = encode_join_token(&relay_id, &broker, &psk).unwrap();
        let decoded = decode_join_token(&token).unwrap();
        assert_eq!(decoded.relay_id, relay_id);
        assert_eq!(decoded.broker_url, broker);
        assert_eq!(decoded.psk, Some(psk));
    }

    #[test]
    fn test_encode_decode_private_broker_token() {
        let relay_id = uuid::Uuid::new_v4().to_string();
        let broker = "mqtts://my-broker.example.com:8883";
        let psk = fake_psk();
        let token = encode_join_token(&relay_id, broker, &psk).unwrap();
        let decoded = decode_join_token(&token).unwrap();
        assert_eq!(decoded.relay_id, relay_id);
        assert_eq!(decoded.broker_url, broker);
        assert_eq!(decoded.psk, Some(psk));
    }

    #[test]
    fn test_decode_invalid_token() {
        assert!(decode_join_token("not-a-token").is_none());
        assert!(decode_join_token("").is_none());
    }

    #[test]
    fn test_update_toml_key_existing() {
        let content = "[relay]\nurl = \"\"\nid = \"\"\nenabled = false\n[other]\nfoo = 1\n";
        let result = update_toml_key(content, "relay_enabled", "true");
        assert!(result.contains("enabled = true"));
        assert!(result.contains("foo = 1"));
    }

    #[test]
    fn test_update_toml_key_new() {
        let content = "[other]\nfoo = 1\n";
        let result = update_toml_key(content, "relay_enabled", "true");
        // Should create [relay] section with enabled = true
        let doc: toml_edit::DocumentMut = result.parse().unwrap();
        assert_eq!(doc["relay"]["enabled"].as_bool(), Some(true));
        assert_eq!(doc["other"]["foo"].as_integer(), Some(1));
    }

    #[test]
    fn test_parse_broker_flags() {
        let argv: Vec<String> = vec![
            "--broker".into(),
            "mqtts://host:8883".into(),
            "--password".into(),
            "secret".into(),
            "other".into(),
        ];
        let (broker, auth, remaining) = parse_broker_flags(&argv);
        assert_eq!(broker.as_deref(), Some("mqtts://host:8883"));
        assert_eq!(auth.as_deref(), Some("secret"));
        assert_eq!(remaining, vec!["other"]);
    }

    #[test]
    #[serial]
    fn test_persist_relay_config_clears_stale_token_when_password_omitted() {
        let _ = isolated_test_env();
        let psk_b64 = relay::encode_psk(&fake_psk());
        let contents = render_relay_config_content(
            "[relay]\nurl = \"mqtt://old:1883\"\nid = \"old-id\"\ntoken = \"stale-secret\"\npsk = \"old-psk\"\nenabled = true\n",
            "new-id",
            "mqtt://127.0.0.1:1",
            None,
            &psk_b64,
        );
        let doc: toml_edit::DocumentMut = contents.parse().unwrap();
        assert_eq!(doc["relay"]["id"].as_str(), Some("new-id"));
        assert_eq!(doc["relay"]["url"].as_str(), Some("mqtt://127.0.0.1:1"));
        assert_eq!(doc["relay"]["token"].as_str(), Some(""));
        assert_eq!(doc["relay"]["psk"].as_str(), Some(psk_b64.as_str()));
        assert_eq!(doc["relay"]["enabled"].as_bool(), Some(true));
    }

    #[test]
    fn test_legacy_token_rejected_in_connect_decode() {
        // A v0x01 (plaintext) token decodes to psk=None; relay_connect refuses
        // it instead of writing config.
        let decoded = decode_join_token("AaGyw9Tl9niQq83vEjRWeJAA").unwrap();
        assert!(decoded.psk.is_none());
    }

    #[test]
    #[serial]
    fn test_relay_off_all_disables_local_relay_without_peers() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let cfg = crate::config::HcomConfig {
            relay: "mqtt://127.0.0.1:1".to_string(),
            relay_id: "relay-1".to_string(),
            relay_psk: relay::encode_psk(&fake_psk()),
            relay_enabled: true,
            ..Default::default()
        };
        crate::config::save_toml_config(&cfg).unwrap();

        let db = HcomDb::open().unwrap();
        let args = RelayArgs {
            args: vec!["off".to_string(), "--all".to_string()],
        };
        assert_eq!(cmd_relay(&db, &args, None), 0);

        let updated = crate::config::HcomConfig::load(None);
        assert!(!updated.relay_enabled);
    }

    #[test]
    #[serial]
    fn test_relay_push_subcommand_exists() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let args = RelayArgs {
            args: vec!["push".to_string()],
        };
        assert_eq!(cmd_relay(&db, &args, None), 0);
    }

    #[test]
    #[serial]
    fn test_relay_on_rejects_invalid_stored_config_without_enabling() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let mut content = String::new();
        content = update_toml_key(&content, "relay_id", "\"relay-1\"");
        content = update_toml_key(&content, "relay", "\"\"");
        content = update_toml_key(&content, "relay_psk", "\"not-valid-base64\"");
        content = update_toml_key(&content, "relay_enabled", "false");
        std::fs::write(crate::paths::config_toml_path(), content).unwrap();

        let db = HcomDb::open().unwrap();
        let args = RelayArgs {
            args: vec!["on".to_string()],
        };
        assert_eq!(cmd_relay(&db, &args, None), 1);

        let updated = crate::config::HcomConfig::load(None);
        assert!(!updated.relay_enabled);
    }
}
