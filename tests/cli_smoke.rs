//! Hermetic CLI smoke tests: invoke the `hcom` binary in a temp HCOM_DIR and
//! assert exit codes + stdout shape.

mod support;

#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::time::{Duration, Instant};
use support::{Hcom, parse_hcom_marker};

#[test]
fn fixture_drop_terminates_registered_process_group() {
    #[cfg(unix)]
    let mut child = Command::new("sh")
        .args(["-c", "sleep 60"])
        .process_group(0)
        .spawn()
        .expect("spawn cleanup test process group");
    #[cfg(windows)]
    let mut child = Command::new("powershell")
        .args(["-NoProfile", "-Command", "Start-Sleep -Seconds 60"])
        .spawn()
        .expect("spawn cleanup test process group");
    let pid = i64::from(child.id());

    let h = Hcom::new();
    h.track_cleanup_pid(pid);
    assert!(
        h.process_group_alive(pid),
        "cleanup test process group did not start"
    );

    let reaper = std::thread::spawn(move || child.wait().expect("reap cleanup test process"));
    drop(h);
    let status = reaper.join().expect("cleanup reaper thread");
    assert!(
        !status.success(),
        "fixture cleanup should terminate the registered process"
    );

    let deadline = Instant::now() + Duration::from_secs(7);
    while Instant::now() < deadline && support::process_group_alive(pid) {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        !support::process_group_alive(pid),
        "fixture drop left process group {pid} alive"
    );
}

#[test]
fn help_prints_and_exits_zero() {
    let h = Hcom::new();
    let (code, stdout, _stderr) = h.run(["--help"]);
    assert_eq!(code, 0, "stdout={stdout}");
    assert!(stdout.contains("hcom"), "stdout={stdout}");
    assert!(
        stdout.contains("Commands:") || stdout.contains("Launch:"),
        "stdout={stdout}"
    );
}

#[test]
fn status_json_in_fresh_dir() {
    let h = Hcom::new();
    let (code, stdout, _stderr) = h.run(["status", "--json"]);
    assert_eq!(code, 0);
    let v: serde_json::Value =
        serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("status json: {e}\n{stdout}"));
    assert_eq!(v["hcom_dir"].as_str(), Some(h.path().to_str().unwrap()));
    assert_eq!(v["instances"]["total"], 0);
}

#[test]
fn list_json_empty() {
    let h = Hcom::new();
    let (code, stdout, _stderr) = h.run(["list", "--json"]);
    assert_eq!(code, 0);
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("list json");
    let arr = v.as_array().expect("list returns array");
    assert!(arr.is_empty(), "expected empty list, got {stdout}");
}

#[test]
fn named_list_json_includes_computed_status_metadata() {
    let h = Hcom::new();
    let name = h.start();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    db.execute(
        "UPDATE instances
         SET status = 'listening', status_time = ?1, last_stop = ?2
         WHERE name = ?3",
        rusqlite::params![now - 300, now, name],
    )
    .unwrap();
    drop(db);

    let (code, stdout, stderr) = h.run(["list", &name, "--json"]);
    assert_eq!(code, 0, "stderr={stderr}");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("named list json");
    assert!(value.get("status_context").is_some(), "stdout={stdout}");
    assert!(value.get("status_detail").is_some(), "stdout={stdout}");
    assert!(value["status_age_seconds"].is_number(), "stdout={stdout}");
    assert!(
        value["stored_status_age_seconds"].is_number(),
        "stdout={stdout}"
    );
    assert_eq!(value["status"], "listening", "stdout={stdout}");
    assert_eq!(value["status_age_seconds"], 0, "stdout={stdout}");
    assert!(
        value["stored_status_age_seconds"].as_i64().unwrap() >= 300,
        "stdout={stdout}"
    );

    let (code, full_stdout, stderr) = h.run(["list", "--json"]);
    assert_eq!(code, 0, "stderr={stderr}");
    let full: serde_json::Value = serde_json::from_str(&full_stdout).expect("full list json");
    let same_worker = full
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"] == name)
        .expect("worker in full list");
    assert_eq!(value["status"], same_worker["status"]);
    let targeted_age = value["stored_status_age_seconds"].as_i64().unwrap();
    let full_age = same_worker["stored_status_age_seconds"].as_i64().unwrap();
    assert!(
        (targeted_age - full_age).abs() <= 1,
        "targeted={targeted_age} full={full_age}"
    );
    assert!(full_age >= 300, "stdout={full_stdout}");
}

#[test]
fn list_masks_filter_wait_context() {
    let h = Hcom::new();
    let name = h.start();

    // Force status to something listable so it appears in list --json without --all
    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    db.execute(
        "UPDATE instances SET status = 'listening', status_context = 'filter-wait:4242:1700000000:7', status_time = ?2, last_stop = ?2 WHERE name = ?1",
        rusqlite::params![name, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64],
    )
    .unwrap();
    drop(db);

    // Normal JSON masks it
    let (code, stdout, _) = h.run(["list", "--json"]);
    assert_eq!(code, 0);
    let full: serde_json::Value = serde_json::from_str(&stdout).expect("full list json");
    let instance = full
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"] == name)
        .expect("worker in full list");
    assert_eq!(instance["status_context"], "event filter");

    // Verbose JSON reveals it
    let (code, stdout, _) = h.run(["list", "-v", "--json"]);
    assert_eq!(code, 0);
    let full: serde_json::Value = serde_json::from_str(&stdout).expect("full list json");
    let instance = full
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"] == name)
        .expect("worker in full list");
    assert_eq!(instance["status_context"], "filter-wait:4242:1700000000:7");

    // Single-instance normal JSON
    let (code, stdout, _) = h.run(["list", &name, "--json"]);
    assert_eq!(code, 0);
    let single: serde_json::Value = serde_json::from_str(&stdout).expect("single list json");
    assert_eq!(single["status_context"], "event filter");

    // Single-instance verbose JSON
    let (code, stdout, _) = h.run(["list", &name, "-v", "--json"]);
    assert_eq!(code, 0);
    let single: serde_json::Value = serde_json::from_str(&stdout).expect("single list json");
    assert_eq!(single["status_context"], "filter-wait:4242:1700000000:7");

    // Normal target lookup masks it
    let (code, stdout, _) = h.run(["list", &name]);
    assert_eq!(code, 0);
    assert!(stdout.contains("listening (event filter)"));
    assert!(!stdout.contains("filter-wait:4242:1700000000:7"));

    // Verbose target lookup reveals it
    let (code, stdout, _) = h.run(["list", &name, "-v"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("listening (filter-wait:4242:1700000000:7)"));

    // Custom formats receive the same normalized presentation value.
    let (code, stdout, _) = h.run(["list", "--format", "{status_context}"]);
    assert_eq!(code, 0);
    assert_eq!(stdout.trim(), "event filter");

    // Direct field extraction also masks the internal marker.
    let (code, stdout, _) = h.run(["list", &name, "status_context"]);
    assert_eq!(code, 0);
    assert_eq!(stdout.trim(), "event filter");
}

#[test]
fn events_empty_in_fresh_dir() {
    let h = Hcom::new();
    let (code, stdout, _stderr) = h.run(["events", "--last", "5"]);
    assert_eq!(code, 0);
    assert!(stdout.trim().is_empty(), "expected no events, got {stdout}");
}

#[test]
fn filtered_listen_timeout_is_structured_and_nonzero() {
    let h = Hcom::new();
    let me = h.start();

    let (code, stdout, stderr) = h.run([
        "listen",
        "--name",
        &me,
        "--timeout",
        "1",
        "--json",
        "--type",
        "life",
    ]);
    assert_eq!(code, 1, "stderr={stderr}; stdout={stdout}");
    let timeout: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|error| panic!("timeout JSON: {error}; stdout={stdout}"));
    // Contract version 1: see src/commands/listen_result.rs.
    assert_eq!(timeout["schema_version"].as_i64(), Some(1));
    assert_eq!(timeout["matched"].as_bool(), Some(false));
    assert_eq!(timeout["reason"].as_str(), Some("timeout"));
    assert_eq!(timeout["timeout_seconds"].as_f64(), Some(1.0));
    assert_eq!(timeout["effective_timeout_seconds"].as_f64(), Some(0.1));
    assert_eq!(
        timeout["notification"].as_str(),
        Some("[Timeout: no match after 0.1s]")
    );
    for absent in ["event_id", "type", "instance", "data"] {
        assert!(
            timeout.get(absent).is_none(),
            "timeout JSON must not carry matched key '{absent}': {stdout}"
        );
    }

    let (invalid_code, _, invalid_stderr) = h.run(["listen", "--name", &me, "--timeout-ok", "1"]);
    assert_eq!(invalid_code, 1);
    assert!(invalid_stderr.contains("--timeout-ok requires"));

    let (compat_code, compat_stdout, compat_stderr) = h.run([
        "listen",
        "--name",
        &me,
        "--timeout",
        "1",
        "--timeout-ok",
        "--json",
        "--type",
        "life",
    ]);
    assert_eq!(
        compat_code, 0,
        "stderr={compat_stderr}; stdout={compat_stdout}"
    );
    let compat: serde_json::Value = serde_json::from_str(compat_stdout.trim())
        .unwrap_or_else(|error| panic!("compat timeout JSON: {error}; stdout={compat_stdout}"));
    assert_eq!(compat["schema_version"].as_i64(), Some(1));
    assert_eq!(compat["matched"].as_bool(), Some(false));
    assert_eq!(compat["reason"].as_str(), Some("timeout"));
}

#[test]
fn filtered_listen_match_result_follows_versioned_schema() {
    let h = Hcom::new();
    let me = h.start();

    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    // Emit a real post-cursor status event for a second identity.
    let worker = h.start();
    let (status_code, _, status_err) = h.run([
        "pi-status",
        "--name",
        &worker,
        "--status",
        "listening",
        "--context",
        "turn:end",
    ]);
    assert_eq!(status_code, 0, "stderr={status_err}");

    let (code, stdout, stderr) = h.run([
        "listen",
        "--name",
        &me,
        "--timeout",
        "3",
        "--json",
        "--after-id",
        &cursor,
        "--type",
        "status",
        "--agent",
        &worker,
    ]);
    assert_eq!(code, 0, "stderr={stderr}; stdout={stdout}");
    let matched: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|error| panic!("match JSON: {error}; stdout={stdout}"));

    // Contract version 1: see src/commands/listen_result.rs.
    assert_eq!(matched["schema_version"].as_i64(), Some(1));
    assert_eq!(matched["matched"].as_bool(), Some(true));
    assert_eq!(matched["type"].as_str(), Some("status"));
    assert_eq!(matched["instance"].as_str(), Some(worker.as_str()));
    let event_id = matched["event_id"].as_i64().expect("event_id integer");
    assert!(event_id > cursor.parse::<i64>().unwrap_or(0));
    assert!(
        matched["data"].is_object(),
        "matched data must be the event payload: {stdout}"
    );
    // Legacy prose is preserved verbatim for older parsers.
    let expected_notification = format!("[Match found] #{event_id} status:{worker}");
    assert_eq!(
        matched["notification"].as_str(),
        Some(expected_notification.as_str())
    );
    for absent in ["reason", "timeout_seconds", "effective_timeout_seconds"] {
        assert!(
            matched.get(absent).is_none(),
            "match JSON must not carry timeout key '{absent}': {stdout}"
        );
    }
}

#[test]
fn idle_wait_ignores_transport_listen_but_accepts_provider_idle() {
    let h = Hcom::new();
    let worker = h.start();

    let (active_code, _, active_stderr) = h.run([
        "pi-status",
        "--name",
        &worker,
        "--status",
        "active",
        "--context",
        "prompt",
        "--detail",
        "working",
    ]);
    assert_eq!(active_code, 0, "stderr={active_stderr}");

    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    let (listen_code, _, listen_err) = h.run(["listen", "--name", &worker, "1"]);
    assert_eq!(listen_code, 0, "stderr={listen_err}");

    let (transport_code, transport_out, transport_err) = h.run([
        "events",
        "--wait",
        "1",
        "--after-id",
        &cursor,
        "--idle",
        &worker,
    ]);
    assert_eq!(
        transport_code, 1,
        "transport wait must not look task-idle: stderr={transport_err}; stdout={transport_out}"
    );

    let (idle_cursor_code, idle_cursor_out, idle_cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(idle_cursor_code, 0, "stderr={idle_cursor_err}");
    let idle_cursor = idle_cursor_out.trim().to_string();
    let (idle_code, _, idle_stderr) = h.run([
        "pi-status",
        "--name",
        &worker,
        "--status",
        "listening",
        "--context",
        "turn:end",
    ]);
    assert_eq!(idle_code, 0, "stderr={idle_stderr}");

    let (matched_code, matched_out, matched_err) = h.run([
        "events",
        "--wait",
        "1",
        "--after-id",
        &idle_cursor,
        "--idle",
        &worker,
    ]);
    assert_eq!(
        matched_code, 0,
        "genuine provider idle must match: stderr={matched_err}; stdout={matched_out}"
    );
}

#[test]
fn events_wait_timeout_prints_legacy_marker_and_exits_one() {
    let h = Hcom::new();
    let _worker = h.start();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    // Compatibility pin before listener extraction: a plain (uncorrelated)
    // wait that runs to its deadline must keep printing the exact legacy
    // one-line marker existing scripts grep for.
    let (code, stdout, stderr) = h.run(["events", "--wait", "1", "--after-id", &cursor]);
    assert_eq!(code, 1, "stdout={stdout} stderr={stderr}");
    assert_eq!(
        stdout.trim(),
        r#"{"timed_out":true}"#,
        "plain wait deadline output changed: stdout={stdout} stderr={stderr}"
    );
}

#[test]
fn events_wait_prints_only_the_first_matching_event_streamlined() {
    let h = Hcom::new();
    let _worker = h.start();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    for text in ["first progress report", "second progress report"] {
        let data = serde_json::json!({
            "from": "seed-worker",
            "scope": "broadcast",
            "text": text,
            "sender_kind": "instance",
            "delivered_to": ["caller"],
            "mentions": ["caller"],
            "reply_to": "12",
            "sender_instance_key": "seed-worker@1000.000000",
        });
        db.execute(
            "INSERT INTO events (timestamp, type, instance, data)
             VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'message', 'seed-worker', ?1)",
            rusqlite::params![data.to_string()],
        )
        .unwrap();
    }
    let ids: Vec<i64> = db
        .prepare("SELECT id FROM events WHERE instance = 'seed-worker' ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    drop(db);
    assert_eq!(ids.len(), 2, "seeded two message events");

    // Compatibility pin before listener extraction: one-shot wait emits the
    // FIRST matching event after the cursor in the default streamlined shape
    // and exits, never consuming past that single match.
    let (code, stdout, stderr) = h.run([
        "events",
        "--wait",
        "2",
        "--after-id",
        &cursor,
        "--type",
        "message",
    ]);
    assert_eq!(code, 0, "stdout={stdout} stderr={stderr}");

    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "wait must emit exactly one match and exit: {stdout}"
    );
    let event: serde_json::Value = serde_json::from_str(lines[0])
        .unwrap_or_else(|e| panic!("wait output must be one JSON event: {e}\n{stdout}"));
    assert_eq!(
        event["id"].as_i64(),
        Some(ids[0]),
        "the first event after the cursor must win: {stdout}"
    );
    assert_eq!(event["type"], "message");
    assert_eq!(event["instance"], "seed-worker");
    assert_eq!(event["data"]["text"], "first progress report");
    assert_eq!(
        event["ts"].as_str().map(str::len),
        Some(19),
        "streamlined wait output truncates ts to seconds: {stdout}"
    );
    for stripped in [
        "sender_kind",
        "scope",
        "delivered_to",
        "mentions",
        "reply_to",
        "sender_instance_key",
    ] {
        assert!(
            event["data"].get(stripped).is_none(),
            "streamlined wait output must drop '{stripped}': {stdout}"
        );
    }
    assert!(
        !stdout.contains("second progress report"),
        "wait must not consume past the first match: {stdout}"
    );
}

/// Seed one event row directly and return its durable ID.
fn seed_stream_event(h: &Hcom, event_type: &str, text: &str) -> i64 {
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    // Status events never become inbox messages, so an identity running the
    // command never gets a delivery block appended to its stdout.
    let data = if event_type == "status" {
        serde_json::json!({
            "status": text,
            "context": "test",
            "detail": text,
        })
    } else {
        serde_json::json!({
            "from": "seed-worker",
            "scope": "broadcast",
            "text": text,
            "sender_kind": "instance",
            "delivered_to": ["caller"],
            "mentions": ["caller"],
            "reply_to": "12",
            "sender_instance_key": "seed-worker@1000.000000",
        })
    };
    db.execute(
        "INSERT INTO events (timestamp, type, instance, data)
         VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?1, 'seed-worker', ?2)",
        rusqlite::params![event_type, data.to_string()],
    )
    .unwrap();
    db.last_insert_rowid()
}

#[test]
fn events_stream_help_documents_cursor_and_output_flags() {
    let h = Hcom::new();
    // `hcom events stream --help` renders the per-command native help for
    // `events`, which must document the stream mode alongside wait and sub.
    let (code, stdout, stderr) = h.run(["events", "stream", "--help"]);
    assert_eq!(code, 0, "stderr={stderr}");
    for needle in [
        "events stream [filters]",
        "--after-id ID",
        "--timeout SEC",
        "--full",
        "--follow NAME",
        "--compact",
        "--heartbeat SEC",
        "no filters",
        "conflicts --full/filters",
        "closed pipe",
        "secret-bearing event data",
        "only --follow --compact is",
    ] {
        assert!(
            stdout.contains(needle),
            "events help must document '{needle}': {stdout}"
        );
    }
}

#[test]
fn events_stream_rejects_query_mode_flags() {
    let h = Hcom::new();

    // The stream subcommand has no --wait of its own.
    let (code, _stdout, stderr) = h.run(["events", "stream", "--wait", "1"]);
    assert_ne!(code, 0, "stream must not accept a --wait flag");
    assert!(
        stderr.contains("unexpected argument"),
        "expected clap rejection, got: {stderr}"
    );

    // Top-level --after-id still requires --wait.
    let (code, _stdout, _stderr) = h.run(["events", "--after-id", "5", "stream"]);
    assert_ne!(
        code, 0,
        "top-level --after-id without --wait must not parse"
    );

    // Query-mode flags typed before the subcommand are rejected, not ignored.
    let (code, _stdout, stderr) = h.run(["events", "--wait", "1", "stream"]);
    assert_eq!(code, 1, "query-mode --wait before stream must fail closed");
    assert!(
        stderr.contains("query-mode"),
        "expected the query-mode flag conflict hint, got: {stderr}"
    );

    let (code, _stdout, stderr) = h.run(["events", "--type", "message", "stream"]);
    assert_eq!(code, 1, "parent-level stream filters must fail closed");
    assert!(
        stderr.contains("filters before `stream`"),
        "expected the stream-filter placement hint, got: {stderr}"
    );
}

#[test]
fn events_stream_emits_every_match_in_order_until_timeout() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    let first = seed_stream_event(&h, "message", "first progress report");
    let _noise = seed_stream_event(&h, "status", "unrelated noise");
    let second = seed_stream_event(&h, "message", "second progress report");

    let started = Instant::now();
    let (code, stdout, stderr) = h.run([
        "events",
        "stream",
        "--after-id",
        &cursor,
        "--timeout",
        "1",
        "--type",
        "message",
    ]);
    assert_eq!(code, 0, "stream timeout is a normal completion: {stderr}");
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "stream must remain active for the whole timeout window"
    );

    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "stream must emit every match, not just the first: {stdout}"
    );
    let events: Vec<serde_json::Value> = lines
        .iter()
        .map(|line| {
            serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("stream output must be JSON lines: {e}\n{stdout}"))
        })
        .collect();
    assert_eq!(
        events[0]["id"].as_i64(),
        Some(first),
        "matches must be emitted in ascending durable-ID order: {stdout}"
    );
    assert_eq!(events[1]["id"].as_i64(), Some(second));
    assert_eq!(events[0]["data"]["text"], "first progress report");
    assert_eq!(events[1]["data"]["text"], "second progress report");
    assert_eq!(
        events[0]["ts"].as_str().map(str::len),
        Some(19),
        "default stream output is the streamlined projection: {stdout}"
    );
    for stripped in ["sender_kind", "scope", "delivered_to", "reply_to"] {
        assert!(
            events[0]["data"].get(stripped).is_none(),
            "streamlined stream output must drop '{stripped}': {stdout}"
        );
    }
    assert!(
        !stdout.contains("unrelated noise"),
        "non-matching events must be filtered out: {stdout}"
    );
}

#[test]
fn events_stream_full_output_keeps_raw_event_fields() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();
    seed_stream_event(&h, "message", "raw report");

    let (code, stdout, stderr) = h.run([
        "events",
        "stream",
        "--after-id",
        &cursor,
        "--timeout",
        "1",
        "--full",
        "--type",
        "message",
    ]);
    assert_eq!(code, 0, "stderr={stderr}");
    let event: serde_json::Value = serde_json::from_str(
        stdout
            .lines()
            .next()
            .unwrap_or_else(|| panic!("no output: {stdout}")),
    )
    .unwrap_or_else(|e| panic!("stream --full output must be one JSON event: {e}\n{stdout}"));
    assert!(
        event["ts"].as_str().map(str::len) > Some(19),
        "--full must keep the untruncated timestamp: {stdout}"
    );
    assert!(
        event["data"].get("scope").is_some() && event["data"].get("sender_kind").is_some(),
        "--full must bypass streamlining: {stdout}"
    );
}

#[test]
fn events_stream_default_cursor_consumes_only_post_arm_events() {
    let h = Hcom::new();
    // Prime the temp tree so the events table exists before direct seeding.
    let (prime_code, _prime_out, prime_err) = h.run(["events", "--cursor"]);
    assert_eq!(prime_code, 0, "stderr={prime_err}");
    seed_stream_event(&h, "status", "before arming");

    // Run the stream as a registered identity so its `events_stream` notify
    // endpoint appears once armed. The listener captures the durable cursor
    // BEFORE registering the endpoint, so endpoint visibility proves the
    // default cursor was already captured — closing the arming race that
    // --after-id exists to solve.
    let process_id = "stream-default-cursor-proc";
    let name = h.start_with_process_id(process_id);
    let child = h
        .cmd()
        .env("HCOM_PROCESS_ID", process_id)
        .args(["events", "stream", "--timeout", "4", "--type", "status"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hcom events stream");

    let armed = {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let registered: i64 = db
                .query_row(
                    "SELECT COUNT(*) FROM notify_endpoints WHERE kind = 'events_stream'",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            if registered > 0 {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    assert!(
        armed,
        "identity-backed stream must register an events_stream endpoint"
    );

    let after = seed_stream_event(&h, "status", "after arming");
    let output = child.wait_with_output().expect("wait for stream exit");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stream must exit 0 at timeout: {stderr}"
    );

    let events: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| {
            serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("bad stream line {line:?}: {e}\nfull stdout: {stdout}"))
        })
        .collect();
    assert_eq!(
        events.len(),
        1,
        "default cursor must skip events durable before arming: {stdout}"
    );
    assert_eq!(events[0]["id"].as_i64(), Some(after));
    assert!(
        !stdout.contains("before arming"),
        "pre-arm events must not replay: {stdout}"
    );

    // Exit must clean up the stream's own listener registration.
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let leftover: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE kind = 'events_stream'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    assert_eq!(
        leftover, 0,
        "stream exit must remove its endpoint (owned by {name})"
    );
}

#[test]
fn events_stream_resumes_after_last_emitted_id_without_replay() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();
    let first = seed_stream_event(&h, "message", "resume one");
    let second = seed_stream_event(&h, "message", "resume two");

    let (code, stdout, stderr) = h.run([
        "events",
        "stream",
        "--after-id",
        &cursor,
        "--timeout",
        "1",
        "--type",
        "message",
    ]);
    assert_eq!(code, 0, "stderr={stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "first stream run: {stdout}");

    let third = seed_stream_event(&h, "message", "resume three");
    let (code, stdout, stderr) = h.run([
        "events",
        "stream",
        "--after-id",
        &second.to_string(),
        "--timeout",
        "1",
        "--type",
        "message",
    ]);
    assert_eq!(code, 0, "stderr={stderr}");
    let events: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).expect("stream JSON line"))
        .collect();
    assert_eq!(
        events.len(),
        1,
        "resuming after the last emitted ID must not replay consumed events: {stdout}"
    );
    assert_eq!(events[0]["id"].as_i64(), Some(third));
    assert!(!stdout.contains("resume one") && !stdout.contains("resume two"));
    assert!(
        first < second && second < third,
        "seeded IDs must be ascending for a meaningful resume assertion"
    );
}

#[test]
fn events_stream_picks_up_events_arriving_mid_stream() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    // Spawn the stream and let it arm before the event becomes durable.
    let child = h
        .cmd()
        .args([
            "events",
            "stream",
            "--after-id",
            &cursor,
            "--timeout",
            "3",
            "--type",
            "message",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hcom events stream");

    std::thread::sleep(Duration::from_millis(400));
    seed_stream_event(&h, "message", "live mid-stream update");

    let output = child.wait_with_output().expect("wait for stream exit");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stream must exit 0 at timeout: {stderr}"
    );
    assert!(
        stdout.contains("live mid-stream update"),
        "an event arriving mid-stream must be emitted by the bounded recheck: {stdout}"
    );
    assert_eq!(
        stdout.lines().count(),
        1,
        "only the mid-stream event matches: {stdout}"
    );
}

#[test]
fn events_stream_explicit_flush_emits_record_immediately() {
    use std::io::BufRead;

    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    // The record must arrive before this timeout expires; allowing the timeout
    // to terminate the process keeps the assertion portable to Windows.
    let mut child = h
        .cmd()
        .args([
            "events",
            "stream",
            "--after-id",
            &cursor,
            "--timeout",
            "3",
            "--type",
            "message",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hcom events stream");

    let stdout = child.stdout.take().expect("take stdout");
    let (tx, rx) = std::sync::mpsc::channel();

    let reader_handle = std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        let mut line = String::new();
        let res = reader.read_line(&mut line);
        tx.send((res, line)).unwrap();
    });

    std::thread::sleep(Duration::from_millis(200));
    seed_stream_event(&h, "message", "flush test payload");

    // The line must arrive immediately, well before the 3-second timeout.
    let (res, line) = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("record must be flushed immediately to stdout rather than buffered");
    assert!(res.is_ok());
    assert!(line.contains("flush test payload"));

    let status = child.wait().expect("wait for stream exit");
    assert!(status.success(), "stream must exit 0 on clean timeout");
    reader_handle.join().unwrap();
}

#[test]
fn events_stream_broken_pipe_terminates_cleanly_zero_and_cleans_up_endpoint() {
    use std::io::BufRead;

    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    let process_id = "stream-broken-pipe-proc";
    let name = h.start_with_process_id(process_id);

    // Setup extra endpoints and observed worker in instances table
    {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        db.execute(
            "INSERT INTO notify_endpoints (instance, kind, port, updated_at) VALUES (?, 'pty', 7171, 1000.0)",
            rusqlite::params![name],
        )
        .unwrap();
        db.execute(
            "INSERT INTO notify_endpoints (instance, kind, port, updated_at) VALUES ('other-worker', 'events_stream', 8181, 1000.0)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO instances (name, status, status_context, status_detail, session_id, created_at, last_stop)
             VALUES ('observed-worker', 'active', 'step-pipe', 'working', 'sess-pipe', 2000000000.0, 2000000000)",
            [],
        )
        .unwrap();
    }

    let mut child = h
        .cmd()
        .env("HCOM_PROCESS_ID", process_id)
        .args([
            "events",
            "stream",
            "--after-id",
            &cursor,
            "--timeout",
            "30",
            "--type",
            "message",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hcom events stream");

    // Wait for the stream's own endpoint to be registered
    let stream_port: u16 = {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let port: Option<u16> = db
                .query_row(
                    "SELECT port FROM notify_endpoints WHERE instance = ? AND kind = 'events_stream'",
                    rusqlite::params![name],
                    |row| row.get(0),
                )
                .ok();
            if let Some(port) = port {
                break port;
            }
            if Instant::now() >= deadline {
                panic!("events_stream endpoint failed to arm in time");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    };

    let stdout = child.stdout.take().expect("take child stdout");
    let mut reader = std::io::BufReader::new(stdout);

    // Seed first event and read it
    seed_stream_event(&h, "message", "first pipe event");
    let mut first_line = String::new();
    reader.read_line(&mut first_line).unwrap();
    assert!(first_line.contains("first pipe event"));

    // Drop the reader, closing the pipe
    drop(reader);

    // Seed second event so child attempts to write to the broken pipe
    seed_stream_event(&h, "message", "second pipe event");
    // Wake listener via TCP notification
    let _ = std::net::TcpStream::connect(format!("127.0.0.1:{stream_port}"));

    // Wait for child to exit
    let output = child.wait_with_output().expect("wait for stream exit");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "broken pipe must result in clean exit 0, got {:?}, stderr={stderr}",
        output.status
    );

    // Direct DB assertions:
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();

    // 1. events_stream endpoint for this listener must be removed
    let stream_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ? AND kind = 'events_stream'",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stream_count, 0,
        "listener's events_stream endpoint must be removed on broken pipe"
    );

    // 2. other endpoint (pty) for this listener must be preserved
    let pty_port: u16 = db
        .query_row(
            "SELECT port FROM notify_endpoints WHERE instance = ? AND kind = 'pty'",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pty_port, 7171, "listener's pty endpoint must be preserved");

    // 3. other instance's endpoint must be preserved
    let other_port: u16 = db
        .query_row(
            "SELECT port FROM notify_endpoints WHERE instance = 'other-worker' AND kind = 'events_stream'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        other_port, 8181,
        "other instance endpoint must be preserved"
    );

    // 4. observed worker in instances table must be unchanged
    let (worker_status, worker_context, worker_detail, worker_sess): (String, String, String, String) = db
        .query_row(
            "SELECT status, status_context, status_detail, session_id FROM instances WHERE name = 'observed-worker'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(worker_status, "active");
    assert_eq!(worker_context, "step-pipe");
    assert_eq!(worker_detail, "working");
    assert_eq!(worker_sess, "sess-pipe");
}

#[test]
#[cfg(unix)]
fn events_stream_sigint_interruption_terminates_cleanly_zero_and_cleans_up_endpoint() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    let process_id = "stream-sigint-proc";
    let name = h.start_with_process_id(process_id);

    // Setup extra endpoints and observed worker in instances table
    {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        db.execute(
            "INSERT INTO notify_endpoints (instance, kind, port, updated_at) VALUES (?, 'pty', 7272, 1000.0)",
            rusqlite::params![name],
        )
        .unwrap();
        db.execute(
            "INSERT INTO notify_endpoints (instance, kind, port, updated_at) VALUES ('other-worker', 'events_stream', 8282, 1000.0)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO instances (name, status, status_context, status_detail, session_id, created_at, last_stop)
             VALUES ('observed-worker', 'active', 'step-sigint', 'working', 'sess-sigint', 2000000000.0, 2000000000)",
            [],
        )
        .unwrap();
    }

    let mut child = h
        .cmd()
        .env("HCOM_PROCESS_ID", process_id)
        .args([
            "events",
            "stream",
            "--after-id",
            &cursor,
            "--timeout",
            "30",
            "--type",
            "message",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hcom events stream");

    // Wait for the stream's own endpoint to be registered
    {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let registered: i64 = db
                .query_row(
                    "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ? AND kind = 'events_stream'",
                    rusqlite::params![name],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            if registered > 0 {
                break;
            }
            if Instant::now() >= deadline {
                panic!("events_stream endpoint failed to arm in time");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    // Send SIGINT to the running stream process and prove it did not merely
    // reach the command's 30-second timeout path.
    let interrupted_at = Instant::now();
    unsafe {
        nix::libc::kill(child.id() as i32, nix::libc::SIGINT);
    }

    let status = child.wait().expect("wait for stream exit");
    assert!(
        status.success(),
        "SIGINT must result in clean exit 0, got {status:?}"
    );
    assert!(
        interrupted_at.elapsed() < Duration::from_secs(10),
        "SIGINT must terminate promptly rather than falling through to timeout"
    );

    // Direct DB assertions:
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();

    // 1. events_stream endpoint for this listener must be removed
    let stream_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ? AND kind = 'events_stream'",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stream_count, 0,
        "listener's events_stream endpoint must be removed on SIGINT"
    );

    // 2. other endpoint (pty) for this listener must be preserved
    let pty_port: u16 = db
        .query_row(
            "SELECT port FROM notify_endpoints WHERE instance = ? AND kind = 'pty'",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pty_port, 7272, "listener's pty endpoint must be preserved");

    // 3. other instance's endpoint must be preserved
    let other_port: u16 = db
        .query_row(
            "SELECT port FROM notify_endpoints WHERE instance = 'other-worker' AND kind = 'events_stream'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        other_port, 8282,
        "other instance endpoint must be preserved"
    );

    // 4. observed worker in instances table must be unchanged
    let (worker_status, worker_context, worker_detail, worker_sess): (String, String, String, String) = db
        .query_row(
            "SELECT status, status_context, status_detail, session_id FROM instances WHERE name = 'observed-worker'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(worker_status, "active");
    assert_eq!(worker_context, "step-sigint");
    assert_eq!(worker_detail, "working");
    assert_eq!(worker_sess, "sess-sigint");
}

#[test]
#[cfg(unix)]
fn events_stream_sigterm_interruption_terminates_cleanly_zero_and_cleans_up_endpoint() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    let process_id = "stream-sigterm-proc";
    let name = h.start_with_process_id(process_id);

    // Setup extra endpoints and observed worker in instances table
    {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        db.execute(
            "INSERT INTO notify_endpoints (instance, kind, port, updated_at) VALUES (?, 'pty', 7373, 1000.0)",
            rusqlite::params![name],
        )
        .unwrap();
        db.execute(
            "INSERT INTO notify_endpoints (instance, kind, port, updated_at) VALUES ('other-worker', 'events_stream', 8383, 1000.0)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO instances (name, status, status_context, status_detail, session_id, created_at, last_stop)
             VALUES ('observed-worker', 'active', 'step-sigterm', 'working', 'sess-sigterm', 2000000000.0, 2000000000)",
            [],
        )
        .unwrap();
    }

    let mut child = h
        .cmd()
        .env("HCOM_PROCESS_ID", process_id)
        .args([
            "events",
            "stream",
            "--after-id",
            &cursor,
            "--timeout",
            "30",
            "--type",
            "message",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn hcom events stream");

    // Wait for the stream's own endpoint to be registered
    {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let registered: i64 = db
                .query_row(
                    "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ? AND kind = 'events_stream'",
                    rusqlite::params![name],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            if registered > 0 {
                break;
            }
            if Instant::now() >= deadline {
                panic!("events_stream endpoint failed to arm in time");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    // Send SIGTERM to the running stream process and prove it did not merely
    // reach the command's 30-second timeout path.
    let interrupted_at = Instant::now();
    unsafe {
        nix::libc::kill(child.id() as i32, nix::libc::SIGTERM);
    }

    let status = child.wait().expect("wait for stream exit");
    assert!(
        status.success(),
        "SIGTERM must result in clean exit 0, got {status:?}"
    );
    assert!(
        interrupted_at.elapsed() < Duration::from_secs(10),
        "SIGTERM must terminate promptly rather than falling through to timeout"
    );

    // Direct DB assertions:
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();

    // 1. events_stream endpoint for this listener must be removed
    let stream_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ? AND kind = 'events_stream'",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stream_count, 0,
        "listener's events_stream endpoint must be removed on SIGTERM"
    );

    // 2. other endpoint (pty) for this listener must be preserved
    let pty_port: u16 = db
        .query_row(
            "SELECT port FROM notify_endpoints WHERE instance = ? AND kind = 'pty'",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pty_port, 7373, "listener's pty endpoint must be preserved");

    // 3. other instance's endpoint must be preserved
    let other_port: u16 = db
        .query_row(
            "SELECT port FROM notify_endpoints WHERE instance = 'other-worker' AND kind = 'events_stream'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        other_port, 8383,
        "other instance endpoint must be preserved"
    );

    // 4. observed worker in instances table must be unchanged
    let (worker_status, worker_context, worker_detail, worker_sess): (String, String, String, String) = db
        .query_row(
            "SELECT status, status_context, status_detail, session_id FROM instances WHERE name = 'observed-worker'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(worker_status, "active");
    assert_eq!(worker_context, "step-sigterm");
    assert_eq!(worker_detail, "working");
    assert_eq!(worker_sess, "sess-sigterm");
}

#[test]
fn events_stream_has_no_subscription_request_watch_or_inbox_side_effects() {
    let h = Hcom::new();
    let _worker = h.start();

    // Baseline: kv table has no subscriptions or request watches
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let sub_count_before: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:sub-%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sub_count_before, 0);

    let reqwatch_before: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reqwatch_before, 0);
    let listener_proc = "stream-no-side-effects-proc";
    let _listener = h.start_with_process_id(listener_proc);

    // Seed events: status and message
    let _first = seed_stream_event(&h, "status", "working on task");
    let _second = seed_stream_event(&h, "message", "broadcasting update");

    // Run stream with timeout starting from beginning
    let (code, stdout, stderr) = h.run_as_process(
        listener_proc,
        ["events", "stream", "--after-id", "0", "--timeout", "1"],
    );
    assert_eq!(code, 0, "stream should exit 0 on timeout: {stderr}");
    assert!(stdout.contains("working on task"));

    // Check DB state after stream: no subscriptions, no request watches, no synthetic notification messages
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let sub_count_after: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:sub-%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        sub_count_after, 0,
        "events stream must not create subscriptions in kv"
    );

    let reqwatch_after: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        reqwatch_after, 0,
        "events stream must not create request-watch rows"
    );

    let syn_msg_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM events WHERE type = 'message' AND (instance = 'hcom-events' OR \
             json_extract(data, '$.from') = 'hcom-events')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        syn_msg_count, 0,
        "events stream must not create [hcom-events] messages"
    );

    let stream_endpoints: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE kind = 'events_stream'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stream_endpoints, 0,
        "events_stream endpoints must be cleaned up"
    );
}

#[test]
fn compact_stream_coexists_with_correlated_request_reply() {
    let h = Hcom::new();
    let worker = h.start();
    let coordinator = h.start();

    let (cursor_code, cursor_out, _) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0);
    let cursor = cursor_out.trim().to_string();
    let observer_proc = "stream-observer-proc";
    let _observer = h.start_with_process_id(observer_proc);

    let stream_start = Instant::now();
    // Spawn compact stream as background child process
    let child = h
        .cmd()
        .env("HCOM_PROCESS_ID", observer_proc)
        .args([
            "events",
            "stream",
            "--after-id",
            &cursor,
            "--follow",
            &worker,
            "--compact",
            "--timeout",
            "10",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn compact stream child");

    // Wait for the stream's own endpoint to be registered
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut armed = false;
    while Instant::now() < deadline {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM notify_endpoints WHERE kind = 'events_stream'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        if count > 0 {
            armed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        armed,
        "compact stream endpoint must be registered before proceeding"
    );

    // 1. Worker reports active status
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    db.execute(
        "INSERT INTO events (timestamp, type, instance, data)
         VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'status', ?1, ?2)",
        rusqlite::params![
            worker,
            serde_json::json!({
                "status": "active",
                "context": "tool:Bash",
                "detail": "cargo test"
            })
            .to_string()
        ],
    )
    .unwrap();
    drop(db);

    // 2. Worker sends request to coordinator
    let (req_code, _, req_err) = h.run([
        "send",
        &format!("@{coordinator}"),
        "--name",
        &worker,
        "--intent",
        "request",
        "--thread",
        "cr-flow",
        "--",
        "requesting review for step 1",
    ]);
    assert_eq!(req_code, 0, "request send failed: stderr={req_err}");

    // Verify request watch was created in kv and fetch the request event ID
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let (reqwatch, req_event_id): (i64, i64) = db
        .query_row(
            "SELECT (SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'), \
                    (SELECT id FROM events WHERE type = 'message' AND instance = ?1 ORDER BY id DESC LIMIT 1)",
            rusqlite::params![worker],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(reqwatch, 1, "request watch must be created in kv");
    drop(db);

    // 2. Coordinator replies
    let (rep_code, _, rep_err) = h.run([
        "send",
        &format!("@{worker}"),
        "--name",
        &coordinator,
        "--intent",
        "inform",
        "--thread",
        "cr-flow",
        "--reply-to",
        &req_event_id.to_string(),
        "--",
        "step 1 review passed",
    ]);
    assert_eq!(rep_code, 0, "reply send failed: stderr={rep_err}");

    // Verify request watch was canceled by correlated reply
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let reqwatch_after: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:reqwatch-%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        reqwatch_after, 0,
        "request watch must be canceled by correlated reply"
    );
    drop(db);

    // 3. Worker stops cleanly
    let (stop_code, _, stop_err) = h.run(["stop", &worker]);
    assert_eq!(stop_code, 0, "stop worker failed: stderr={stop_err}");

    // Wait for the stream child process to exit cleanly at the worker's terminal stop boundary
    let output = child.wait_with_output().expect("wait on child stream");
    let stream_duration = stream_start.elapsed();
    assert_eq!(
        output.status.code(),
        Some(0),
        "compact stream child must exit 0: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stream_duration < Duration::from_secs(8),
        "compact stream must exit at worker stop boundary well before timeout (elapsed: {stream_duration:?})"
    );

    let stream_stdout = String::from_utf8_lossy(&output.stdout);

    // Verify compact stream never emits message bodies or conversational content
    assert!(
        !stream_stdout.contains("requesting review for step 1"),
        "compact stream must not emit request text: {stream_stdout}"
    );
    assert!(
        !stream_stdout.contains("step 1 review passed"),
        "compact stream must not emit reply text: {stream_stdout}"
    );
    assert!(
        !stream_stdout.contains("cr-flow"),
        "compact stream must not emit thread name: {stream_stdout}"
    );

    let stream_stderr = String::from_utf8_lossy(&output.stderr);
    let lines: Vec<&str> = stream_stdout.lines().collect();
    assert!(
        !lines.is_empty(),
        "compact stream should have emitted records: stdout={stream_stdout} stderr={stream_stderr}"
    );
    let last_record: serde_json::Value =
        serde_json::from_str(lines.last().unwrap()).expect("valid last compact JSON record");
    assert_eq!(
        last_record["activity"]["type"], "phase",
        "last compact record must be phase activity: {last_record}"
    );
    assert_eq!(
        last_record["activity"]["phase"], "stopped",
        "last compact record must be phase stopped terminal boundary: {last_record}"
    );
    for line in lines {
        let rec: serde_json::Value = serde_json::from_str(line).expect("valid compact JSON record");
        assert_eq!(rec["schema_version"], 1);
        assert!(rec["generation"].is_string());
    }
}

#[test]
#[cfg(unix)]
fn simultaneous_wait_and_stream_listeners_coexist_and_clean_up_independently() {
    use std::io::BufRead;

    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    let wait_proc = "simultaneous-wait-proc";
    let stream_proc = "simultaneous-stream-proc";
    let wait_name = h.start_with_process_id(wait_proc);
    let stream_name = h.start_with_process_id(stream_proc);

    let wait_child = h
        .cmd()
        .env("HCOM_PROCESS_ID", wait_proc)
        .args([
            "events",
            "--wait",
            "10",
            "--after-id",
            &cursor,
            "--type",
            "message",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn events --wait");

    let mut stream_child = h
        .cmd()
        .env("HCOM_PROCESS_ID", stream_proc)
        .args([
            "events",
            "stream",
            "--after-id",
            &cursor,
            "--timeout",
            "10",
            "--type",
            "message",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn events stream");

    let stream_stdout = stream_child.stdout.take().expect("take stream stdout");
    let mut stream_reader = std::io::BufReader::new(stream_stdout);

    // Wait until both endpoints are armed simultaneously
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut wait_armed = false;
    let mut stream_armed = false;
    while Instant::now() < deadline {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        let wait_count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ?1 AND kind = 'events_wait'",
                rusqlite::params![wait_name],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let stream_count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ?1 AND kind = 'events_stream'",
                rusqlite::params![stream_name],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if wait_count > 0 {
            wait_armed = true;
        }
        if stream_count > 0 {
            stream_armed = true;
        }
        if wait_armed && stream_armed {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(wait_armed, "events_wait endpoint must be armed");
    assert!(stream_armed, "events_stream endpoint must be armed");

    // Seed first event for both listeners
    seed_stream_event(&h, "message", "simultaneous match 1");

    // Wait child must terminate immediately on the first match
    let wait_output = wait_child.wait_with_output().expect("wait for wait child");
    assert_eq!(
        wait_output.status.code(),
        Some(0),
        "events --wait must exit 0: stderr={}",
        String::from_utf8_lossy(&wait_output.stderr)
    );
    let wait_stdout = String::from_utf8_lossy(&wait_output.stdout);
    assert!(
        wait_stdout.contains("simultaneous match 1"),
        "wait stdout must contain first match: {wait_stdout}"
    );

    // Stream child also emits match 1
    let mut line1 = String::new();
    stream_reader
        .read_line(&mut line1)
        .expect("read match 1 from stream");
    assert!(line1.contains("simultaneous match 1"));

    // Verify immediately: events_wait endpoint is removed, but events_stream endpoint is preserved
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let wait_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ?1 AND kind = 'events_wait'",
            rusqlite::params![wait_name],
            |r| r.get(0),
        )
        .unwrap();
    let stream_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ?1 AND kind = 'events_stream'",
            rusqlite::params![stream_name],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        wait_count, 0,
        "events_wait endpoint must be cleaned up after exit"
    );
    assert_eq!(
        stream_count, 1,
        "events_stream endpoint must remain active while stream runs"
    );
    drop(db);

    // Seed second event - stream will receive this as well
    seed_stream_event(&h, "message", "simultaneous match 2");

    // Deterministically observe second event emission from the stream before sending SIGINT
    let mut line2 = String::new();
    stream_reader
        .read_line(&mut line2)
        .expect("read match 2 from stream");
    assert!(line2.contains("simultaneous match 2"));

    // Signal stream child to interrupt cleanly (SIGINT)
    unsafe {
        nix::libc::kill(stream_child.id() as i32, nix::libc::SIGINT);
    }
    let status = stream_child.wait().expect("wait for stream child");
    assert!(
        status.success(),
        "stream child must exit 0 on SIGINT: status={status:?}"
    );

    // Verify stream endpoint is also cleaned up
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let stream_count_after: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE instance = ?1 AND kind = 'events_stream'",
            rusqlite::params![stream_name],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        stream_count_after, 0,
        "events_stream endpoint must be cleaned up after exit"
    );
}

#[test]
fn compact_stream_follow_isolates_unrelated_workers_noisy_activity_and_generation_reuse() {
    let h = Hcom::new();
    let (cursor_code, cursor_out, cursor_err) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_err}");
    let cursor = cursor_out.trim().to_string();

    let worker = h.start_with_process_id("worker-proc-1");
    let unrelated_worker = h.start_with_process_id("unrelated-proc-2");

    let observer_proc = "stream-follow-observer";
    let observer_name = h.start_with_process_id(observer_proc);

    let stream_start = Instant::now();
    let child = h
        .cmd()
        .env("HCOM_PROCESS_ID", observer_proc)
        .args([
            "events",
            "stream",
            "--after-id",
            &cursor,
            "--follow",
            &worker,
            "--compact",
            "--timeout",
            "10",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn compact stream follow child");

    // Wait for the stream's own endpoint to be registered
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut armed = false;
    while Instant::now() < deadline {
        let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM notify_endpoints WHERE kind = 'events_stream' AND instance = ?1",
                rusqlite::params![observer_name],
                |row| row.get(0),
            )
            .unwrap_or(0);
        if count > 0 {
            armed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(armed, "compact stream endpoint must be registered");

    // 1. Emit noisy repetitive activity for `worker`
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    for _ in 0..5 {
        db.execute(
            "INSERT INTO events (timestamp, type, instance, data)
             VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'status', ?1, ?2)",
            rusqlite::params![
                worker,
                serde_json::json!({
                    "status": "active",
                    "context": "tool:Bash",
                    "detail": "cargo test --noisy"
                })
                .to_string()
            ],
        )
        .unwrap();
    }

    // 2. Emit activity for `unrelated_worker`
    db.execute(
        "INSERT INTO events (timestamp, type, instance, data)
         VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'status', ?1, ?2)",
        rusqlite::params![
            unrelated_worker,
            serde_json::json!({
                "status": "active",
                "context": "tool:Grep",
                "detail": "unrelated noise detail"
            })
            .to_string()
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO events (timestamp, type, instance, data)
         VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'message', ?1, ?2)",
        rusqlite::params![
            unrelated_worker,
            serde_json::json!({
                "from": unrelated_worker,
                "scope": "direct",
                "delivered_to": ["different-instance"],
                "text": "unrelated worker secret message"
            })
            .to_string()
        ],
    )
    .unwrap();
    drop(db);

    // 3. Worker stops cleanly
    let (stop_code, _, stop_err) = h.run(["stop", &worker]);
    assert_eq!(stop_code, 0, "stop worker failed: stderr={stop_err}");

    // 4. Simulate generation reuse under the same name `worker`
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    db.execute(
        "INSERT INTO instances (name, status, status_context, status_detail, session_id, created_at, last_stop)
         VALUES (?1, 'active', 'reused-gen', 'working', 'sess-reused', 3000000000.0, NULL)",
        rusqlite::params![worker],
    )
    .unwrap();
    db.execute(
        "INSERT INTO events (timestamp, type, instance, data)
         VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'status', ?1, ?2)",
        rusqlite::params![
            worker,
            serde_json::json!({
                "status": "active",
                "context": "tool:NewGen",
                "detail": "reused generation activity"
            })
            .to_string()
        ],
    )
    .unwrap();
    drop(db);

    // Stream child must terminate at worker's stop boundary, well before the 10-second timeout
    let output = child.wait_with_output().expect("wait on child stream");
    let stream_duration = stream_start.elapsed();
    assert_eq!(
        output.status.code(),
        Some(0),
        "compact stream child must exit 0: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stream_duration < Duration::from_secs(8),
        "compact stream must exit at worker stop boundary well before timeout (elapsed: {stream_duration:?})"
    );

    let stream_stdout = String::from_utf8_lossy(&output.stdout);
    let stream_stderr = String::from_utf8_lossy(&output.stderr);
    let lines: Vec<&str> = stream_stdout.lines().collect();
    assert!(
        !lines.is_empty(),
        "compact stream should emit records: stdout={stream_stdout} stderr={stream_stderr}"
    );

    // The stream must never contain content from unrelated worker
    assert!(
        !stream_stdout.contains("unrelated noise detail"),
        "must not contain unrelated worker detail: {stream_stdout}"
    );
    assert!(
        !stream_stdout.contains("unrelated worker secret message"),
        "must not contain unrelated worker message: {stream_stdout}"
    );

    // The stream must never contain content from reused generation
    assert!(
        !stream_stdout.contains("reused generation activity"),
        "must not contain reused generation activity: {stream_stdout}"
    );

    // Terminal record must be phase 'stopped'
    let last_record: serde_json::Value =
        serde_json::from_str(lines.last().unwrap()).expect("valid last compact JSON record");
    assert_eq!(
        last_record["activity"]["type"], "phase",
        "last compact record must be phase activity: {last_record}"
    );
    assert_eq!(
        last_record["activity"]["phase"], "stopped",
        "last compact record must be phase stopped terminal boundary: {last_record}"
    );

    // Verify noisy repeated status was coalesced/deduplicated:
    // We emitted 5 identical active status events; only 1 active phase record should exist
    let active_phase_count = lines
        .iter()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|rec| rec["activity"]["type"] == "phase" && rec["activity"]["phase"] == "active")
        .count();
    assert_eq!(
        active_phase_count, 1,
        "noisy identical phases must be deduplicated to 1, found {active_phase_count}: {stream_stdout}"
    );

    // Verify endpoint cleanup
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let stream_endpoints: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE kind = 'events_stream' AND instance = ?1",
            rusqlite::params![observer_name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stream_endpoints, 0,
        "events_stream endpoint must be cleaned up on completion"
    );
}

#[test]
fn events_snapshot_default_preserves_non_streaming_exit() {
    let h = Hcom::new();
    let _worker = h.start();
    let _event = seed_stream_event(&h, "status", "test-snapshot-status");

    let (code, stdout, stderr) = h.run(["events"]);
    assert_eq!(code, 0, "stdout={stdout} stderr={stderr}");

    let lines: Vec<&str> = stdout.lines().collect();
    assert!(!lines.is_empty(), "snapshot should emit events");
    for line in lines {
        let _parsed: serde_json::Value =
            serde_json::from_str(line).expect("snapshot line must be valid JSON");
    }

    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let stream_endpoints: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE kind = 'events_stream'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stream_endpoints, 0,
        "snapshot must not create stream endpoints"
    );
}

#[test]
fn events_sub_default_preserves_subscription_without_stdout_stream() {
    let h = Hcom::new();
    let worker = h.start();

    let (code, stdout, stderr) = h.run([
        "events", "sub", "--name", &worker, "--agent", &worker, "--once",
    ]);
    assert_eq!(code, 0, "stdout={stdout} stderr={stderr}");
    assert!(stdout.contains("Subscription sub-"), "stdout={stdout}");

    // Check kv subscription exists
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let sub_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:sub-%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sub_count, 1, "sub must create a subscription row in kv");

    let stream_endpoints: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE kind = 'events_stream'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stream_endpoints, 0,
        "events sub must not create stream endpoints"
    );
}

#[test]
fn hcom_run_preserves_terminal_only_wait_output_and_no_implicit_stream() {
    let h = Hcom::new();
    let _worker = h.start();

    // Create a workflow script in $HCOM_DIR/scripts/run-wait-test.sh
    let scripts_dir = h.path().join("scripts");
    std::fs::create_dir_all(&scripts_dir).unwrap();
    let script_path = scripts_dir.join("run-wait-test.sh");
    let hcom_cmd = h.bash_hcom_command();
    let script_content = format!(
        r#"#!/bin/bash
# Test that events --wait inside a script captures only single-event wait output
OUT=$({hcom_cmd} events --wait 2 --after-id 0 --type message)
echo "OUT_CAPTURED: $OUT"
"#
    );
    std::fs::write(&script_path, script_content).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms).unwrap();
    }

    // Seed two message events
    let _first = seed_stream_event(&h, "message", "first work report");
    let _second = seed_stream_event(&h, "message", "second work report");

    // Run via `hcom run run-wait-test`
    let (code, stdout, stderr) = h.run(["run", "run-wait-test"]);
    assert_eq!(code, 0, "stdout={stdout} stderr={stderr}");

    assert!(stdout.contains("OUT_CAPTURED:"));
    assert!(stdout.contains("first work report"));
    assert!(
        !stdout.contains("second work report"),
        "wait must not stream past first match: {stdout}"
    );

    // Check no events_stream endpoint was ever registered
    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let stream_endpoints: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM notify_endpoints WHERE kind = 'events_stream'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stream_endpoints, 0, "no events_stream endpoints created");
}

#[test]
fn send_without_identity_errors_with_hint() {
    let h = Hcom::new();
    let (code, _stdout, stderr) = h.run(["send", "@nobody", "--", "hi"]);
    assert_ne!(code, 0, "send without identity must fail: stderr={stderr}");
    assert!(
        stderr.contains("identity not found"),
        "expected stable hint, got: {stderr}"
    );
}

#[test]
fn send_to_missing_agent_lists_available() {
    let h = Hcom::new();
    let me = h.start();

    let (code, _stdout, stderr) = h.run(["send", "@nope", "--name", &me, "--", "hi"]);
    assert_ne!(code, 0, "send to nonexistent must fail");
    assert!(
        stderr.contains("@nope") && stderr.contains("Available:"),
        "stderr={stderr}"
    );
}

#[test]
fn send_strips_redundant_trailing_name_from_auto_resolved_sender() {
    let h = Hcom::new();
    let recipient = h.start();
    let process_id = "send-trailing-name-process";

    let mut start = h.cmd();
    start.env("HCOM_PROCESS_ID", process_id).arg("start");
    let start_out = start.output().expect("spawn hcom start");
    let start_stdout = String::from_utf8_lossy(&start_out.stdout);
    let start_stderr = String::from_utf8_lossy(&start_out.stderr);
    assert!(
        start_out.status.success(),
        "stdout={start_stdout} stderr={start_stderr}"
    );
    let sender = parse_hcom_marker(&start_stdout).expect("sender marker");

    let mut send = h.cmd();
    send.env("HCOM_PROCESS_ID", process_id).args([
        "send",
        &format!("@{recipient}"),
        "--",
        "ack",
        "--name",
        &sender,
    ]);
    let send_out = send.output().expect("spawn hcom send");
    let send_stdout = String::from_utf8_lossy(&send_out.stdout);
    let send_stderr = String::from_utf8_lossy(&send_out.stderr);
    assert!(
        send_out.status.success(),
        "stdout={send_stdout} stderr={send_stderr}"
    );

    let (_, events, _) = h.run(["events", "--type", "message", "--last", "1"]);
    assert!(events.contains(r#""text":"ack""#), "events={events}");
    assert!(!events.contains("--name"), "events={events}");
}

#[test]
fn broadcast_requires_explicit_scope_and_go_preview_in_ai_tools() {
    let h = Hcom::new();
    let sender = h.start();
    for _ in 0..4 {
        h.start();
    }

    let mut cmd = h.cmd();
    cmd.env("CODEX_SANDBOX", "1")
        .args(["send", "--name", &sender, "moto"]);
    let out = cmd.output().expect("spawn hcom send");
    let code = out.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_ne!(
        code, 0,
        "implicit broadcast must fail closed: stdout={stdout} stderr={stderr}"
    );
    assert!(
        stderr.contains("Refusing implicit broadcast") && stderr.contains("--broadcast"),
        "stdout={stdout} stderr={stderr}"
    );

    let mut preview_cmd = h.cmd();
    preview_cmd.env("CODEX_SANDBOX", "1").args([
        "send",
        "--broadcast",
        "--name",
        &sender,
        "--",
        "explicit but not confirmed broadcast",
    ]);
    let preview_out = preview_cmd.output().expect("spawn explicit hcom broadcast");
    let preview_code = preview_out.status.code().unwrap_or(-1);
    let preview_stdout = String::from_utf8_lossy(&preview_out.stdout);
    let preview_stderr = String::from_utf8_lossy(&preview_out.stderr);
    assert_ne!(
        preview_code, 0,
        "AI-tool broadcast should preview first: stdout={preview_stdout} stderr={preview_stderr}"
    );
    assert!(
        preview_stdout.contains("BROADCAST SEND PREVIEW")
            && preview_stdout.contains("broadcast to 4 agents")
            && preview_stdout.contains("Did you mean to send this to everyone?")
            && preview_stdout.contains("hcom send --go"),
        "stdout={preview_stdout}"
    );

    let (_, events_out, _) = h.run(["events", "--type", "message", "--last", "5"]);
    assert!(
        events_out.trim().is_empty(),
        "preview must not send a message: events={events_out}"
    );

    let mut go_cmd = h.cmd();
    go_cmd.env("CODEX_SANDBOX", "1").args([
        "--go",
        "send",
        "--broadcast",
        "--name",
        &sender,
        "--",
        "confirmed broadcast",
    ]);
    let go_out = go_cmd.output().expect("spawn hcom --go send");
    let go_code = go_out.status.code().unwrap_or(-1);
    let go_stdout = String::from_utf8_lossy(&go_out.stdout);
    let go_stderr = String::from_utf8_lossy(&go_out.stderr);
    assert_eq!(go_code, 0, "stdout={go_stdout} stderr={go_stderr}");
    assert!(go_stdout.contains("Sent to:"), "stdout={go_stdout}");
}

#[test]
fn seeded_thread_allows_a_recipient_free_followup_without_broadcast() {
    let h = Hcom::new();
    let sender = h.start();
    let recipient = h.start();
    let decoy = h.start();
    let target = format!("@{recipient}");

    let (seed_code, _, seed_stderr) = h.run([
        "send",
        "--name",
        &sender,
        &target,
        "--thread",
        "seeded-thread",
        "--",
        "seed",
    ]);
    assert_eq!(seed_code, 0, "stderr={seed_stderr}");

    let (follow_code, _, follow_stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--thread",
        "seeded-thread",
        "--",
        "followup",
    ]);
    assert_eq!(follow_code, 0, "stderr={follow_stderr}");
    let (_, event, _) = h.run(["events", "--type", "message", "--last", "1", "--full"]);
    assert!(event.contains(r#""text":"followup""#), "event={event}");
    assert!(event.contains(&recipient), "event={event}");

    // A multi-word bare follow-up is still invalid without `--`; importantly,
    // a live agent name in its first message position must not silently turn
    // the failed thread follow-up into a targeted send.
    let (bare_code, _, bare_stderr) = h.run([
        "send",
        "--name",
        &sender,
        "--thread",
        "seeded-thread",
        &decoy,
        "hello",
        "world",
    ]);
    assert_ne!(bare_code, 0, "stderr={bare_stderr}");
    assert!(
        bare_stderr.contains("No input received on stdin"),
        "the established failure mode should be preserved: stderr={bare_stderr}"
    );
    let (_, bare_event, _) = h.run(["events", "--type", "message", "--last", "1", "--full"]);
    assert!(
        bare_event.contains(r#""text":"followup""#),
        "event={bare_event}"
    );
    assert!(!bare_event.contains("hello world"), "event={bare_event}");
}

#[test]
fn explicit_broadcast_does_not_retarget_bare_multiword_message() {
    let h = Hcom::new();
    let sender = h.start();
    let recipient = h.start();

    // This missing-`--` form remains invalid, but a live agent name in its
    // first message position must not silently turn it into a targeted send.
    let (code, stdout, stderr) = h.run([
        "send",
        "--broadcast",
        "--name",
        &sender,
        &recipient,
        "hello",
        "world",
    ]);
    assert_ne!(code, 0, "stdout={stdout} stderr={stderr}");
    assert!(
        stderr.contains("No input received on stdin"),
        "the established failure mode should be preserved: stderr={stderr}"
    );
    let (_, events, _) = h.run(["events", "--type", "message", "--last", "1", "--full"]);
    assert!(
        events.trim().is_empty(),
        "failed send must not deliver: {events}"
    );
}

#[test]
fn cursor_and_result_from_reject_cross_worker_completion() {
    let h = Hcom::new();
    let caller = h.start();
    let expected = h.start();
    let other = h.start();
    let caller_target = format!("@{caller}");
    let (cursor_code, cursor, cursor_stderr) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_stderr}");
    let cursor = cursor.trim().to_string();
    assert!(cursor.parse::<i64>().is_ok(), "cursor={cursor}");

    let (send_code, _, send_stderr) = h.run([
        "send",
        "--name",
        &other,
        &caller_target,
        "--intent",
        "inform",
        "--thread",
        "exact-result-thread",
        "--",
        "wrong worker",
    ]);
    assert_eq!(send_code, 0, "stderr={send_stderr}");

    let (wrong_code, wrong_out, wrong_err) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "1",
        "--after-id",
        &cursor,
        "--thread",
        "exact-result-thread",
        "--result-from",
        &expected,
    ]);
    assert_eq!(wrong_code, 1, "stdout={wrong_out} stderr={wrong_err}");
    assert!(wrong_out.contains(r#""timed_out":true"#));

    let (send_code, _, send_stderr) = h.run([
        "send",
        "--name",
        &expected,
        &caller_target,
        "--intent",
        "inform",
        "--thread",
        "exact-result-thread",
        "--",
        "exact result",
    ]);
    assert_eq!(send_code, 0, "stderr={send_stderr}");
    let (match_code, matched, match_err) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "1",
        "--after-id",
        &cursor,
        "--thread",
        "exact-result-thread",
        "--result-from",
        &expected,
        "--full",
    ]);
    assert_eq!(match_code, 0, "stdout={matched} stderr={match_err}");
    assert!(
        matched.contains(r#""text":"exact result""#),
        "matched={matched}"
    );
}

#[test]
fn result_from_returns_structured_blocker_exit_code() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();
    let (cursor_code, cursor, cursor_stderr) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_stderr}");
    let cursor = cursor.trim().to_string();

    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    db.execute(
        "UPDATE instances
         SET status = 'blocked', status_context = 'pty:approval',
             status_detail = 'Bash: cargo test'
         WHERE name = ?1",
        [&worker],
    )
    .unwrap();
    let blocker = serde_json::json!({
        "status": "blocked",
        "context": "pty:approval",
        "detail": "Bash: cargo test",
    });
    db.execute(
        "INSERT INTO events (timestamp, type, instance, data)
         VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'status', ?1, ?2)",
        rusqlite::params![worker, blocker.to_string()],
    )
    .unwrap();
    drop(db);

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "2",
        "--after-id",
        &cursor,
        "--thread",
        "blocked-cli-workflow",
        "--result-from",
        &worker,
    ]);
    assert_eq!(code, 4, "stdout={stdout} stderr={stderr}");
    let outcome: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(outcome["outcome"], "blocked");
    assert_eq!(outcome["result_blocked"], true);
    assert_eq!(outcome["worker"], worker);
    assert_eq!(outcome["thread"], "blocked-cli-workflow");
    assert_eq!(outcome["attempt_after_id"].to_string(), cursor);
    assert_eq!(outcome["context"], "pty:approval");
    assert_eq!(outcome["evidence"], "Bash: cargo test");
}

#[test]
fn result_from_returns_structured_launch_failure_exit_code() {
    let h = Hcom::new();
    let caller = h.start();
    let worker = h.start();
    let (cursor_code, cursor, cursor_stderr) = h.run(["events", "--cursor"]);
    assert_eq!(cursor_code, 0, "stderr={cursor_stderr}");
    let cursor = cursor.trim().to_string();

    let db = rusqlite::Connection::open(h.path().join("hcom.db")).unwrap();
    let failure = serde_json::json!({
        "action": "launch_failed",
        "status": "inactive",
        "context": "launch_failed",
        "reason": "exited_before_bind",
        "detail": "provider exited before readiness",
        "batch_id": "batch-cli-failure",
    });
    db.execute(
        "INSERT INTO events (timestamp, type, instance, data)
         VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'life', ?1, ?2)",
        rusqlite::params![worker, failure.to_string()],
    )
    .unwrap();
    drop(db);

    let (code, stdout, stderr) = h.run([
        "events",
        "--name",
        &caller,
        "--wait",
        "2",
        "--after-id",
        &cursor,
        "--thread",
        "failed-cli-workflow",
        "--result-from",
        &worker,
    ]);
    assert_eq!(code, 5, "stdout={stdout} stderr={stderr}");
    let outcome: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(outcome["outcome"], "launch_failed");
    assert_eq!(outcome["result_launch_failed"], true);
    assert_eq!(outcome["worker"], worker);
    assert_eq!(outcome["thread"], "failed-cli-workflow");
    assert_eq!(outcome["attempt_after_id"].to_string(), cursor);
    assert_eq!(outcome["batch_id"], "batch-cli-failure");
}

#[test]
fn start_send_events_roundtrip() {
    let h = Hcom::new();
    let sender = h.start();
    let recipient = h.start();
    assert_ne!(sender, recipient, "two starts must assign distinct names");

    let (c, stdout, stderr) = h.run([
        "send",
        &format!("@{recipient}"),
        "--name",
        &sender,
        "--",
        "hello there",
    ]);
    assert_eq!(c, 0, "stderr={stderr} stdout={stdout}");

    let (c4, events_out, _) = h.run(["events", "--last", "10"]);
    assert_eq!(c4, 0);
    let message_lines: Vec<_> = events_out
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v["type"] == "message")
        .collect();
    assert_eq!(message_lines.len(), 1, "events={events_out}");
    let msg = &message_lines[0];
    assert_eq!(msg["instance"], sender.as_str(), "attribution = sender");
    assert_eq!(msg["data"]["from"], sender.as_str());
    assert_eq!(msg["data"]["text"], "hello there");

    // Recipient/scope contract: the message event only carries from/text, so
    // we check routing via per-instance unread on `list --json`. Recipient
    // must show unread=1, sender unread=0.
    let (c5, list_out, _) = h.run(["list", "--json"]);
    assert_eq!(c5, 0);
    let list: serde_json::Value = serde_json::from_str(&list_out).expect("list json");
    let by_name: std::collections::HashMap<_, _> = list
        .as_array()
        .expect("array")
        .iter()
        .map(|v| {
            (
                v["name"].as_str().unwrap().to_string(),
                v["unread_count"].as_u64().unwrap_or(0),
            )
        })
        .collect();
    assert_eq!(
        by_name.get(&recipient).copied(),
        Some(1),
        "recipient unread; list={list_out}"
    );
    assert_eq!(
        by_name.get(&sender).copied(),
        Some(0),
        "sender unread; list={list_out}"
    );

    let (c6, listen_out, listen_err) =
        h.run(["listen", "--name", &recipient, "--timeout", "1", "--json"]);
    assert_eq!(c6, 0, "listen failed: stderr={listen_err}");
    let delivered: Vec<serde_json::Value> = listen_out
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    assert_eq!(delivered.len(), 1, "listen output={listen_out}");
    assert_eq!(delivered[0]["from"], sender.as_str());
    assert_eq!(delivered[0]["text"], "hello there");

    let (c7, list_after_listen_out, _) = h.run(["list", "--json"]);
    assert_eq!(c7, 0);
    let list_after_listen: serde_json::Value =
        serde_json::from_str(&list_after_listen_out).expect("list json after listen");
    let after_by_name: std::collections::HashMap<_, _> = list_after_listen
        .as_array()
        .expect("array")
        .iter()
        .map(|v| {
            (
                v["name"].as_str().unwrap().to_string(),
                v["unread_count"].as_u64().unwrap_or(0),
            )
        })
        .collect();
    assert_eq!(
        after_by_name.get(&recipient).copied(),
        Some(0),
        "listen should advance recipient cursor; list={list_after_listen_out}"
    );
}

#[test]
fn intent_and_reply_to_roundtrip() {
    // Wiki contract (messaging.md §Intent + event-model.md `msg_intent`/`reply_to_local`):
    // request → ack with --reply-to flattens through `events_v` so threads/replies
    // can be traced. Locks: data.intent on send, and data.reply_to_local resolved
    // from the parent event id.
    let h = Hcom::new();
    let a = h.start();
    let b = h.start();

    let (c, _, e) = h.run([
        "send",
        &format!("@{b}"),
        "--name",
        &a,
        "--intent",
        "request",
        "--",
        "ping",
    ]);
    assert_eq!(c, 0, "request send failed: stderr={e}");

    let (_, req_out, _) = h.run(["events", "--type", "message", "--from", &a, "--last", "5"]);
    let req: serde_json::Value = req_out
        .lines()
        .find_map(|l| serde_json::from_str(l).ok())
        .expect("request event present");
    assert_eq!(req["data"]["intent"], "request");
    let req_id = req["id"].as_i64().expect("event id is i64");

    let (c2, _, e2) = h.run([
        "send",
        &format!("@{a}"),
        "--name",
        &b,
        "--intent",
        "ack",
        "--reply-to",
        &req_id.to_string(),
        "--",
        "pong",
    ]);
    assert_eq!(c2, 0, "ack send failed: stderr={e2}");

    let (_, ack_out, _) = h.run(["events", "--intent", "ack", "--last", "5"]);
    let ack: serde_json::Value = ack_out
        .lines()
        .find_map(|l| serde_json::from_str(l).ok())
        .expect("ack event present");
    assert_eq!(ack["data"]["intent"], "ack");
    assert_eq!(ack["data"]["from"], b.as_str());
    assert_eq!(
        ack["data"]["reply_to_local"].as_i64(),
        Some(req_id),
        "reply_to_local must resolve to request event id; ack={ack}"
    );
}

#[test]
fn lifecycle_events_emitted_for_start_and_stop() {
    // Wiki contract (agent-lifecycle.md + event-model.md): start emits
    // life.started, stop emits life.stopped — filterable via --action.
    // events table is the lifecycle source of truth (see
    // feedback_events_are_source_of_truth memory).
    let h = Hcom::new();
    let a = h.start();

    let (c, started_out, _) = h.run([
        "events", "--action", "started", "--agent", &a, "--last", "5",
    ]);
    assert_eq!(c, 0);
    let started: Vec<serde_json::Value> = started_out
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    assert_eq!(
        started.len(),
        1,
        "expected 1 life.started for {a}, got: {started_out}"
    );
    assert_eq!(started[0]["instance"], a.as_str());
    assert_eq!(started[0]["data"]["action"], "started");

    let (cs, _, es) = h.run(["stop", &a]);
    assert_eq!(cs, 0, "stop failed: {es}");

    let (_, stopped_out, _) = h.run([
        "events", "--action", "stopped", "--agent", &a, "--last", "5",
    ]);
    let stopped: Vec<serde_json::Value> = stopped_out
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    assert_eq!(
        stopped.len(),
        1,
        "expected 1 life.stopped for {a}, got: {stopped_out}"
    );
    assert_eq!(stopped[0]["data"]["action"], "stopped");
    // Snapshot lives on the event but is streamlined out by default;
    // --full surfaces it. Rebind relies on it (see start_as_reclaims_stopped_identity).
    let (_, full_out, _) = h.run([
        "events", "--action", "stopped", "--agent", &a, "--last", "5", "--full",
    ]);
    let full: serde_json::Value = full_out
        .lines()
        .find_map(|l| serde_json::from_str(l).ok())
        .expect("stopped event under --full");
    assert!(
        full["data"]["snapshot"].is_object(),
        "stop must preserve snapshot for rebind; full={full_out}"
    );
}

#[test]
fn start_as_reclaims_stopped_identity() {
    // Wiki contract (identity.md §--as + hcom-start.md Path B): after stop,
    // `start --as <name>` rebinds the same name (no random reallocation).
    // Distinct from bare `start`, which would draw a fresh name.
    let h = Hcom::new();
    let a = h.start();

    let (cs, _, es) = h.run(["stop", &a]);
    assert_eq!(cs, 0, "stop failed: {es}");

    let (cr, stdout, stderr) = h.run(["start", "--as", &a]);
    assert_eq!(cr, 0, "start --as failed: stderr={stderr}");
    assert!(
        stdout.contains(&format!("[hcom:{a}]")),
        "reclaim marker missing; stdout={stdout}"
    );

    // Reclaimed instance is alive again under the same name.
    // (Reclaim is a quiet rebind: no new life.started event, just a logged
    // rebind.complete. The marker + a re-populated instances row is the
    // observable contract.)
    let (_, names_out, _) = h.run(["list", "--names"]);
    assert!(
        names_out.lines().any(|l| l.trim() == a),
        "list --names missing {a} after reclaim: {names_out}"
    );

    // And the stopped snapshot must still be on record — that's what made
    // the cursor-preserving rebind possible.
    let (_, full_out, _) = h.run([
        "events", "--action", "stopped", "--agent", &a, "--last", "5", "--full",
    ]);
    let snap_present = full_out
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .any(|v| v["data"]["snapshot"].is_object());
    assert!(snap_present, "stopped snapshot missing; full={full_out}");
}

#[test]
fn bigboss_send_bypasses_identity_gate() {
    // Wiki contract (messaging.md §@bigboss + reference_send_bigboss_flag memory):
    // `send -b` is sender-as-bigboss and bypasses the identity gate that
    // normally requires `--name` / a bound session. Sender_kind=external
    // distinguishes the message from instance-to-instance traffic.
    let h = Hcom::new();
    let recipient = h.start();

    // Note: no --name. -b is the sole identity signal.
    let (c, _, stderr) = h.run(["send", "-b", &format!("@{recipient}"), "--", "from above"]);
    assert_eq!(c, 0, "send -b must bypass identity gate; stderr={stderr}");
    assert!(
        !stderr.contains("identity not found"),
        "gate should not fire under -b; stderr={stderr}"
    );

    // --full bypasses streamlining so sender_kind is visible.
    let (_, events_out, _) = h.run([
        "events", "--type", "message", "--from", "bigboss", "--last", "5", "--full",
    ]);
    let msg: serde_json::Value = events_out
        .lines()
        .find_map(|l| serde_json::from_str(l).ok())
        .expect("bigboss message event");
    assert_eq!(msg["data"]["from"], "bigboss");
    assert_eq!(msg["data"]["text"], "from above");
    assert_eq!(
        msg["data"]["sender_kind"], "external",
        "bigboss must record as external sender; msg={msg}"
    );
}

#[test]
fn config_unknown_key_is_not_set() {
    let h = Hcom::new();
    let (code, stdout, _stderr) = h.run(["config", "no_such_key"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("(not set)"), "stdout={stdout}");
}

#[test]
fn unknown_command_errors() {
    let h = Hcom::new();
    let (code, _stdout, stderr) = h.run(["nonsense-not-a-command"]);
    assert_ne!(code, 0);
    assert!(!stderr.is_empty(), "expected error message on stderr");
}

#[test]
fn antigravity_e2e_hook_dispatch() {
    let h = Hcom::new();
    let transcript = tempfile::NamedTempFile::new().expect("temp transcript");
    let transcript_path = transcript.path().to_string_lossy().to_string();

    // Spawn hcom start with HCOM_PROCESS_ID to register a process binding
    let mut start_cmd = h.cmd();
    start_cmd.arg("start");
    start_cmd.env("HCOM_PROCESS_ID", "pid-agy-123");
    let start_out = start_cmd.output().expect("failed to run hcom start");
    let me = support::parse_hcom_marker(&String::from_utf8_lossy(&start_out.stdout))
        .expect("no [hcom:NAME] marker");
    let conn = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).expect("open hcom db");
    conn.execute(
        "UPDATE instances SET tool = 'antigravity' WHERE name = ?1",
        [&me],
    )
    .expect("mark fixture as Antigravity");

    // 1. Pipe PreInvocation (session start) to gemini-sessionstart.
    // This will bind the session_id "sess-agy-1" to the active instance.
    let session_start_payload = serde_json::json!({
        "conversationId": "sess-agy-1",
        "transcriptPath": transcript_path,
    });

    use std::io::Write;
    use std::process::Stdio;

    let mut cmd = h.cmd();
    cmd.args(["gemini-sessionstart"]);
    cmd.env("ANTIGRAVITY_AGENT", "1");
    cmd.env("HCOM_PROCESS_ID", "pid-agy-123");
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("failed to spawn hcom sessionstart");
    {
        let mut stdin = child.stdin.take().expect("failed to open stdin");
        stdin
            .write_all(
                serde_json::to_string(&session_start_payload)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
    }
    let out = child
        .wait_with_output()
        .expect("failed to wait sessionstart");
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let first_stdout = String::from_utf8_lossy(&out.stdout);
    let first: serde_json::Value =
        serde_json::from_str(first_stdout.trim()).expect("first sessionstart json");
    let first_context = first["injectSteps"][0]["ephemeralMessage"]
        .as_str()
        .expect("initial Antigravity bootstrap");
    assert!(first_context.contains("[HCOM SESSION]"));
    assert!(first_context.contains(&format!("[hcom:{me}]")));

    // Verify session_id binding matches in the DB via hcom list --json
    let (code, stdout, stderr) = h.run(["list", &me, "--json"]);
    assert_eq!(code, 0, "stderr={stderr}");
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("failed to parse list json");
    assert_eq!(v["session_id"].as_str(), Some("sess-agy-1"));

    // Antigravity fires this hook before every model invocation. Its ephemeral
    // bootstrap must be present after the one-shot name announcement too.
    let mut cmd = h.cmd();
    cmd.args(["gemini-sessionstart"]);
    cmd.env("ANTIGRAVITY_AGENT", "1");
    cmd.env("HCOM_PROCESS_ID", "pid-agy-123");
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("failed to spawn repeated sessionstart");
    {
        let mut stdin = child.stdin.take().expect("failed to open stdin");
        stdin
            .write_all(
                serde_json::to_string(&session_start_payload)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
    }
    let out = child
        .wait_with_output()
        .expect("failed to wait repeated sessionstart");
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let repeated_stdout = String::from_utf8_lossy(&out.stdout);
    let repeated: serde_json::Value =
        serde_json::from_str(repeated_stdout.trim()).expect("repeated sessionstart json");
    let repeated_context = repeated["injectSteps"][0]["ephemeralMessage"]
        .as_str()
        .expect("recurring Antigravity bootstrap");
    assert!(repeated_context.contains("[HCOM SESSION]"));
    assert!(repeated_context.contains(&format!("[hcom:{me}]")));

    // 2. Now pipe PreToolUse to gemini-beforetool.
    // Since the session is bound, it should resolve the instance and execute successfully.
    let before_tool_payload = serde_json::json!({
        "conversationId": "sess-agy-1",
        "transcriptPath": transcript_path,
        "toolCall": {
            "name": "run_command",
            "args": { "CommandLine": "echo hello", "Cwd": "/tmp" }
        }
    });

    let mut cmd = h.cmd();
    cmd.args(["gemini-beforetool"]);
    cmd.env("ANTIGRAVITY_AGENT", "1");
    cmd.env("HCOM_PROCESS_ID", "pid-agy-123");
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("failed to spawn hcom beforetool");
    {
        let mut stdin = child.stdin.take().expect("failed to open stdin");
        stdin
            .write_all(
                serde_json::to_string(&before_tool_payload)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
    }
    let out = child.wait_with_output().expect("failed to wait beforetool");
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).expect("beforetool json");
    assert_eq!(parsed, serde_json::json!({ "decision": "allow" }));

    // 3. AfterTool cannot inject context for Antigravity, so it must not ack delivery.
    let (send_code, _, send_stderr) = h.run([
        "send",
        &format!("@{me}"),
        "--name",
        &me,
        "--intent",
        "request",
        "--",
        "ping",
    ]);
    assert_eq!(send_code, 0, "send stderr={send_stderr}");

    let after_tool_payload = serde_json::json!({
        "conversationId": "sess-agy-1",
        "transcriptPath": transcript_path,
        "toolCall": {
            "name": "run_command",
            "args": { "CommandLine": "echo done", "Cwd": "/tmp" }
        }
    });

    let mut cmd = h.cmd();
    cmd.args(["gemini-aftertool"]);
    cmd.env("ANTIGRAVITY_AGENT", "1");
    cmd.env("HCOM_PROCESS_ID", "pid-agy-123");
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("failed to spawn hcom aftertool");
    {
        let mut stdin = child.stdin.take().expect("failed to open stdin");
        stdin
            .write_all(
                serde_json::to_string(&after_tool_payload)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
    }
    let out = child.wait_with_output().expect("failed to wait aftertool");
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let after_stdout = String::from_utf8_lossy(&out.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(after_stdout.trim()).expect("aftertool json");
    assert_eq!(parsed, serde_json::json!({}));
}

/// Pipe a JSON payload to a native cursor hook and return its parsed stdout.
///
/// Cursor hook command names route directly to `Tool::Cursor` (no shared-prefix
/// disambiguation like Antigravity's `ANTIGRAVITY_AGENT`), so the only env the
/// gate check needs is `HCOM_PROCESS_ID` to resolve the bound instance.
fn run_cursor_hook(
    h: &Hcom,
    hook: &str,
    process_id: &str,
    payload: &serde_json::Value,
) -> serde_json::Value {
    use std::io::Write;
    use std::process::Stdio;

    let mut cmd = h.cmd();
    cmd.args([hook]);
    cmd.env("HCOM_PROCESS_ID", process_id);
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().unwrap_or_else(|e| panic!("spawn {hook}: {e}"));
    {
        let mut stdin = child.stdin.take().expect("open stdin");
        stdin
            .write_all(serde_json::to_string(payload).unwrap().as_bytes())
            .unwrap();
    }
    let out = child
        .wait_with_output()
        .unwrap_or_else(|e| panic!("wait {hook}: {e}"));
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "{hook} stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("{hook} json: {e}\nstdout={stdout}"))
}

/// End-to-end cursor-agent native hook lifecycle over JSON-on-stdin.
///
/// Mirrors `antigravity_e2e_hook_dispatch`, but exercises cursor's real payload
/// shape (`conversation_id`/`tool_input`/`tool_output`) and its distinct
/// delivery contract: unlike Antigravity (whose aftertool cannot inject and
/// must return `{}`), cursor's `postToolUse` injects pending messages via
/// `additional_context` and acks delivery.
#[test]
fn cursor_e2e_hook_dispatch() {
    let h = Hcom::new();
    let transcript = tempfile::NamedTempFile::new().expect("temp transcript");
    let transcript_path = transcript.path().to_string_lossy().to_string();
    let pid = "pid-cur-123";
    let session_id = "sess-cur-1";

    // Register a process binding so the hooks can resolve an instance.
    let mut start_cmd = h.cmd();
    start_cmd.arg("start");
    start_cmd.env("HCOM_PROCESS_ID", pid);
    let start_out = start_cmd.output().expect("failed to run hcom start");
    let me = support::parse_hcom_marker(&String::from_utf8_lossy(&start_out.stdout))
        .expect("no [hcom:NAME] marker");

    // 1. sessionStart binds the conversation to the active instance. Cursor reads
    //    the id from `conversation_id` (snake_case, per the docs' common schema)
    //    and the handler always returns an `env` object.
    let session_start = run_cursor_hook(
        &h,
        "cursor-sessionstart",
        pid,
        &serde_json::json!({
            "conversation_id": session_id,
            "transcript_path": transcript_path,
            "workspace_roots": ["/tmp"],
            "is_background_agent": false,
            "composer_mode": "agent",
        }),
    );
    assert!(
        session_start.get("env").is_some(),
        "sessionStart should emit env block: {session_start}"
    );

    // Binding is visible via list --json.
    let (code, stdout, stderr) = h.run(["list", &me, "--json"]);
    assert_eq!(code, 0, "stderr={stderr}");
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("list json");
    assert_eq!(v["session_id"].as_str(), Some(session_id));

    // 2. beforeSubmitPrompt marks the instance active and must not block the
    //    prompt (`continue: true`).
    let before_submit = run_cursor_hook(
        &h,
        "cursor-beforesubmitprompt",
        pid,
        &serde_json::json!({
            "conversation_id": session_id,
            "transcript_path": transcript_path,
            "prompt": "do a thing",
        }),
    );
    assert_eq!(before_submit, serde_json::json!({ "continue": true }));

    let (code, stdout, _) = h.run(["list", &me, "--json"]);
    assert_eq!(code, 0);
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("list json");
    assert_eq!(v["status"].as_str(), Some("active"));

    // 3. preToolUse records tool status and returns an empty object.
    let pre_tool = run_cursor_hook(
        &h,
        "cursor-pretooluse",
        pid,
        &serde_json::json!({
            "conversation_id": session_id,
            "transcript_path": transcript_path,
            "tool_name": "Shell",
            "tool_input": { "command": "echo hello", "working_directory": "/tmp" },
        }),
    );
    assert_eq!(pre_tool, serde_json::json!({}));

    // 4. Queue a message, then postToolUse delivers it via additional_context.
    //    Send from an external sender (bigboss), not `me`: the DB delivery
    //    filter (`should_deliver_to`) drops any message whose `from` equals the
    //    receiver, so a self-addressed send would never be pending and the
    //    postToolUse assertion below would pass vacuously.
    let (send_code, _, send_stderr) = h.run([
        "send",
        "--from",
        "bigboss",
        &format!("@{me}"),
        "--intent",
        "request",
        "--",
        "ping",
    ]);
    assert_eq!(send_code, 0, "send stderr={send_stderr}");

    let post_tool = run_cursor_hook(
        &h,
        "cursor-posttooluse",
        pid,
        &serde_json::json!({
            "conversation_id": session_id,
            "transcript_path": transcript_path,
            "tool_name": "Shell",
            "tool_input": { "command": "echo hello" },
            "tool_output": "{\"exitCode\":0,\"stdout\":\"hello\"}",
        }),
    );
    let injected = post_tool
        .get("additional_context")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("postToolUse should inject additional_context: {post_tool}"));
    assert!(
        injected.contains("ping"),
        "delivered context should carry the message text: {injected:?}"
    );
}

/// End-to-end GitHub Copilot CLI native hook lifecycle over JSON-on-stdin.
///
/// Mirrors `cursor_e2e_hook_dispatch` but exercises copilot's real payload shape
/// (`session_id`/`tool_name`/`tool_input`/`tool_result`, Claude-style `command`
/// hooks). Copilot's `SessionStart` returns `additionalContext`/`{}` (no `env`
/// block), `PostToolUse` injects pending messages via `additionalContext` and
/// acks delivery. Reuses `run_cursor_hook` — it is a generic "pipe JSON to a
/// native hook" runner, not cursor-specific.
#[test]
fn copilot_e2e_hook_dispatch() {
    let h = Hcom::new();
    let transcript = tempfile::NamedTempFile::new().expect("temp transcript");
    let transcript_path = transcript.path().to_string_lossy().to_string();
    let pid = "pid-cop-123";
    let session_id = "sess-cop-1";

    // Register a process binding so the hooks can resolve an instance.
    let mut start_cmd = h.cmd();
    start_cmd.arg("start");
    start_cmd.env("HCOM_PROCESS_ID", pid);
    let start_out = start_cmd.output().expect("failed to run hcom start");
    let me = support::parse_hcom_marker(&String::from_utf8_lossy(&start_out.stdout))
        .expect("no [hcom:NAME] marker");

    // 1. SessionStart binds the session to the active instance.
    let _ = run_cursor_hook(
        &h,
        "copilot-sessionstart",
        pid,
        &serde_json::json!({
            "session_id": session_id,
            "transcript_path": transcript_path,
            "cwd": "/tmp",
        }),
    );

    // Binding is visible via list --json.
    let (code, stdout, stderr) = h.run(["list", &me, "--json"]);
    assert_eq!(code, 0, "stderr={stderr}");
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("list json");
    assert_eq!(v["session_id"].as_str(), Some(session_id));

    // 2. UserPromptSubmit marks the instance active and returns an empty object.
    let prompt_submit = run_cursor_hook(
        &h,
        "copilot-userpromptsubmit",
        pid,
        &serde_json::json!({
            "session_id": session_id,
            "transcript_path": transcript_path,
            "prompt": "do a thing",
        }),
    );
    assert_eq!(prompt_submit, serde_json::json!({}));

    let (code, stdout, _) = h.run(["list", &me, "--json"]);
    assert_eq!(code, 0);
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("list json");
    assert_eq!(v["status"].as_str(), Some("active"));

    // 3. PreToolUse records tool status and returns an empty object.
    let pre_tool = run_cursor_hook(
        &h,
        "copilot-pretooluse",
        pid,
        &serde_json::json!({
            "session_id": session_id,
            "transcript_path": transcript_path,
            "tool_name": "bash",
            "tool_input": { "command": "echo hello" },
        }),
    );
    assert_eq!(pre_tool, serde_json::json!({}));

    // 4. Queue a message from an external sender, then PostToolUse delivers it
    //    via additionalContext. (Self-addressed sends are dropped by the DB
    //    delivery filter, so the assertion below would pass vacuously.)
    let (send_code, _, send_stderr) = h.run([
        "send",
        "--from",
        "bigboss",
        &format!("@{me}"),
        "--intent",
        "request",
        "--",
        "ping",
    ]);
    assert_eq!(send_code, 0, "send stderr={send_stderr}");

    let post_tool = run_cursor_hook(
        &h,
        "copilot-posttooluse",
        pid,
        &serde_json::json!({
            "session_id": session_id,
            "transcript_path": transcript_path,
            "tool_name": "bash",
            "tool_input": { "command": "echo hello" },
            "tool_result": { "text_result_for_llm": "hello" },
        }),
    );
    let injected = post_tool
        .get("additionalContext")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("PostToolUse should inject additionalContext: {post_tool}"));
    assert!(
        injected.contains("ping"),
        "delivered context should carry the message text: {injected:?}"
    );
}

/// Pipe argv to a native argv-style hook and return its parsed stdout.
fn run_argv_hook(
    h: &Hcom,
    hook: &str,
    process_id: Option<&str>,
    args: &[&str],
) -> serde_json::Value {
    let mut cmd = h.cmd();
    cmd.arg(hook);
    cmd.args(args);
    if let Some(process_id) = process_id {
        cmd.env("HCOM_PROCESS_ID", process_id);
    }

    let out = cmd.output().unwrap_or_else(|e| panic!("spawn {hook}: {e}"));
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "{hook} stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("{hook} json: {e}\nstdout={stdout}"))
}

/// End-to-end Pi argv hook lifecycle.
///
/// Pi's extension invokes hcom with argv, not JSON stdin. This test mirrors the
/// native hook smoke tests above while staying hermetic: no real Pi process is
/// launched, only a fake process binding plus the `pi-*` hook commands.
#[test]
fn pi_e2e_hook_dispatch() {
    let h = Hcom::new();
    let transcript = tempfile::NamedTempFile::new().expect("temp transcript");
    let transcript_path = transcript.path().to_string_lossy().to_string();
    let pid = "pid-pi-123";
    let session_id = "sess-pi-1";

    // Register a process binding so pi-start can resolve an instance.
    let mut start_cmd = h.cmd();
    start_cmd.arg("start");
    start_cmd.env("HCOM_PROCESS_ID", pid);
    let start_out = start_cmd.output().expect("failed to run hcom start");
    let me = support::parse_hcom_marker(&String::from_utf8_lossy(&start_out.stdout))
        .expect("no [hcom:NAME] marker");

    // 1. pi-start binds the session and returns bootstrap context to the plugin.
    let cwd = h.root.path().to_string_lossy().to_string();
    let start = run_argv_hook(
        &h,
        "pi-start",
        Some(pid),
        &[
            "--session-id",
            session_id,
            "--transcript-path",
            &transcript_path,
            "--cwd",
            &cwd,
        ],
    );
    assert_eq!(start["name"].as_str(), Some(me.as_str()));
    assert_eq!(start["session_id"].as_str(), Some(session_id));
    assert!(
        start["bootstrap"]
            .as_str()
            .is_some_and(|text| text.contains(&format!("[hcom:{me}]"))),
        "pi-start should return bootstrap with the hcom marker: {start}"
    );

    let (code, stdout, stderr) = h.run(["list", &me, "--json"]);
    assert_eq!(code, 0, "stderr={stderr}");
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("list json");
    assert_eq!(v["tool"].as_str(), Some("pi"));
    assert_eq!(v["session_id"].as_str(), Some(session_id));
    assert_eq!(
        v["transcript_path"].as_str(),
        Some(transcript_path.as_str())
    );
    assert_eq!(v["directory"].as_str(), Some(cwd.as_str()));

    // 2. pi-status marks active/listening transitions.
    let status = run_argv_hook(
        &h,
        "pi-status",
        None,
        &[
            "--name",
            &me,
            "--status",
            "active",
            "--context",
            "prompt",
            "--detail",
            "working",
        ],
    );
    assert_eq!(status, serde_json::json!({ "ok": true }));
    let (code, stdout, _) = h.run(["list", &me, "--json"]);
    assert_eq!(code, 0);
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("list json");
    assert_eq!(v["status"].as_str(), Some("active"));

    // 3. pi-beforetool records tool status and allows the tool call.
    let before_tool = run_argv_hook(
        &h,
        "pi-beforetool",
        None,
        &[
            "--name",
            &me,
            "--tool",
            "bash",
            "--input-json",
            r#"{"command":"echo hello"}"#,
        ],
    );
    assert_eq!(before_tool, serde_json::json!({ "decision": "allow" }));
    let (code, stdout, _) = h.run(["events", "--agent", &me, "--type", "status", "--last", "5"]);
    assert_eq!(code, 0);
    let tool_status: serde_json::Value = stdout
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .find(|event: &serde_json::Value| event["data"]["context"] == "tool:bash")
        .unwrap_or_else(|| panic!("tool:bash status event missing: {stdout}"));
    assert!(
        tool_status["data"]["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("echo hello")),
        "tool detail should include bash command: {tool_status}"
    );

    // 4. pi-read exposes pending messages and can ack the cursor.
    let (send_code, _, send_stderr) = h.run([
        "send",
        "--from",
        "bigboss",
        &format!("@{me}"),
        "--intent",
        "request",
        "--",
        "ping",
    ]);
    assert_eq!(send_code, 0, "send stderr={send_stderr}");

    let check = h.run(["pi-read", "--name", &me, "--check"]);
    assert_eq!(check.0, 0, "pi-read --check stderr={}", check.2);
    assert_eq!(check.1.trim(), "true");

    let read = h.run(["pi-read", "--name", &me]);
    assert_eq!(read.0, 0, "pi-read stderr={}", read.2);
    let messages: serde_json::Value = serde_json::from_str(&read.1).expect("pi-read json");
    assert!(
        messages
            .as_array()
            .is_some_and(|items| items.iter().any(|m| m["message"] == "ping")),
        "pi-read should return pending ping: {messages}"
    );

    let ack = run_argv_hook(&h, "pi-read", None, &["--name", &me, "--ack"]);
    assert_eq!(ack["acked"].as_u64(), Some(1));
    let check = h.run(["pi-read", "--name", &me, "--check"]);
    assert_eq!(check.0, 0, "pi-read --check after ack stderr={}", check.2);
    assert_eq!(check.1.trim(), "false");

    // 5. pi-stop finalizes the session.
    let stop = run_argv_hook(&h, "pi-stop", None, &["--name", &me, "--reason", "done"]);
    assert_eq!(stop, serde_json::json!({ "ok": true }));
    let (code, stdout, _) = h.run([
        "events", "--agent", &me, "--action", "stopped", "--last", "5",
    ]);
    assert_eq!(code, 0);
    let stopped: serde_json::Value = stdout
        .lines()
        .find_map(|line| serde_json::from_str(line).ok())
        .unwrap_or_else(|| panic!("stopped event missing: {stdout}"));
    assert_eq!(stopped["data"]["action"].as_str(), Some("stopped"));
}

// ── Tracker 38 command-grammar retry traps ────────────────────────────────
//
// Each recorded form from the orchestration token-efficiency tracker must be
// either accepted (mapped to the canonical option) or rejected with the exact
// supported equivalent instead of a generic clap/stdin error.

#[test]
fn transcript_tail_alias_is_accepted() {
    let h = Hcom::new();
    // A fresh dir has no such agent, so reaching the "not found" error proves
    // clap parsed `--tail` (mapped to --last) instead of rejecting the flag.
    let (code, _stdout, stderr) = h.run(["transcript", "nobody", "--tail", "1"]);
    assert_ne!(code, 0, "stderr={stderr}");
    assert!(
        !stderr.contains("unexpected argument"),
        "--tail must be accepted: stderr={stderr}"
    );
    assert!(stderr.contains("not found"), "stderr={stderr}");
}

#[test]
fn transcript_canonical_last_still_works() {
    let h = Hcom::new();
    let (code, _stdout, stderr) = h.run(["transcript", "nobody", "--last", "1"]);
    assert_ne!(code, 0, "stderr={stderr}");
    assert!(stderr.contains("not found"), "stderr={stderr}");
}

#[test]
fn events_limit_alias_is_accepted() {
    let h = Hcom::new();
    let (code, stdout, stderr) = h.run(["events", "--limit", "5"]);
    assert_eq!(code, 0, "stderr={stderr}");
    assert!(stdout.trim().is_empty(), "expected no events, got {stdout}");
}

#[test]
fn events_sql_wrong_column_names_canonical_equivalent() {
    let h = Hcom::new();
    // Tracker 38: the recorded `from_agent` SQL attempt must name the public
    // column/flag instead of only the raw SQLite error. Exit stays 2.
    let (code, _stdout, stderr) = h.run(["events", "--sql", "from_agent = 'kuma'"]);
    assert_eq!(code, 2, "stderr={stderr}");
    assert!(stderr.contains("msg_from"), "stderr={stderr}");
    assert!(stderr.contains("--from"), "stderr={stderr}");
}

#[test]
fn events_sql_canonical_msg_from_is_accepted() {
    let h = Hcom::new();
    let (code, stdout, stderr) = h.run(["events", "--sql", "msg_from = 'kuma'"]);
    assert_eq!(code, 0, "stderr={stderr}");
    assert!(stdout.trim().is_empty(), "expected no events, got {stdout}");
}

#[test]
fn send_positional_agent_form_delivers() {
    let h = Hcom::new();
    let me = h.start();
    // Tracker 38: `hcom send <agent> <message words...>` (no @, no --) must
    // deliver to the resolved agent instead of failing on stdin.
    let process_id = format!("{me}-positional-send");
    let mut cmd = h.cmd();
    cmd.env("HCOM_PROCESS_ID", &process_id)
        .args(["send", "--name", &me, &me, "hello", "world"]);
    let out = cmd.output().expect("spawn hcom send");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "positional send must succeed: stdout={stdout} stderr={stderr}"
    );
    assert!(stdout.contains("Sent to:"), "stdout={stdout}");

    let (_, events, _) = h.run(["events", "--type", "message", "--last", "1"]);
    assert!(events.contains("hello world"), "events={events}");
    assert!(events.contains(&me), "events={events}");
}

#[test]
fn send_ambiguous_positional_words_error_with_canonical_syntax() {
    let h = Hcom::new();
    let me = h.start();
    // Multiple bare words that resolve to no agent must fail fast with the
    // canonical syntax — not the misleading stdin error.
    let (code, _stdout, stderr) = h.run(["send", "--name", &me, "alpha", "beta"]);
    assert_ne!(code, 0, "stderr={stderr}");
    assert!(stderr.contains("hcom send @<agent> --"), "stderr={stderr}");
    assert!(
        !stderr.contains("stdin"),
        "must not fall into the stdin path: stderr={stderr}"
    );
}
