//! Broker discovery — parallel MQTT handshake to find a working MQTT broker.
//!
//! Used by `hcom relay new` to pick the highest-priority reachable public broker.

use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Outcome of probing one broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerProbe {
    /// MQTT handshake completed in this many ms.
    Reachable(u64),
    Failed(String),
    /// Not waited for: a higher-priority broker already answered.
    Skipped,
}

/// Result of testing a single broker: (host, port, outcome).
pub type BrokerTestResult = (String, u16, BrokerProbe);

/// Connect to the first resolved address that accepts. Resolution often lists
/// IPv6 first, and on an IPv4-only network that address can never connect.
fn connect_any(addrs: &[SocketAddr]) -> Option<TcpStream> {
    addrs
        .iter()
        .find_map(|addr| TcpStream::connect_timeout(addr, CONNECT_TIMEOUT).ok())
}

/// Test a single broker via TCP+TLS handshake. Returns round-trip ms or None.
pub fn ping_broker(host: &str, port: u16, use_tls: bool) -> Option<u64> {
    let t0 = Instant::now();
    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs().ok()?.collect();
    let mut stream = connect_any(&addrs)?;

    if use_tls {
        // TCP+TLS handshake only. Verify the broker is reachable and accepts TLS.
        // Set timeouts so handshake doesn't block forever on unreachable brokers.
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .ok()?;

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let server_name: rustls::pki_types::ServerName<'static> =
            host.to_string().try_into().ok()?;
        let mut conn = rustls::ClientConnection::new(Arc::new(config), server_name).ok()?;

        // Drive TLS handshake via complete_io (handles read/write round-trips).
        // Stops after handshake — the read timeout prevents blocking on post-handshake
        // app data (MQTT brokers wait for CONNECT before sending anything).
        match conn.complete_io(&mut stream) {
            Ok(_) => {}
            Err(_) => {
                // complete_io may error with read timeout after handshake completes
                // (MQTT brokers send no app data until CONNECT). That's OK.
                if conn.is_handshaking() {
                    return None; // Failed during handshake = broker unreachable
                }
            }
        }
    }

    Some(t0.elapsed().as_millis() as u64)
}

/// Test MQTT acceptance, including TLS and credentials, without joining a group.
/// Uses the same connector, roots and overall deadline as the relay worker.
pub fn ping_mqtt_broker(
    host: &str,
    port: u16,
    use_tls: bool,
    password: Option<&str>,
) -> Result<u64, String> {
    use rumqttc::Packet;
    use rumqttc::{Client, Event, MqttOptions};

    let start = Instant::now();
    let mut options =
        MqttOptions::new(format!("hcom-probe-{}", uuid::Uuid::new_v4()), (host, port));
    options.set_keep_alive(30);
    options.set_clean_start(true);
    options.set_max_packet_size(Some(super::MQTT_MAX_PACKET_BYTES as u32));
    if use_tls {
        options.set_transport(rumqttc::Transport::tls_with_config(
            super::client::relay_tls_config(),
        ));
    }
    if let Some(password) = password.filter(|p| !p.is_empty()) {
        options.set_credentials("hcom", password.to_owned());
    }
    let (client, mut connection) = Client::builder(options).capacity(1).build();
    loop {
        let remaining = CONNECT_TIMEOUT
            .checked_sub(start.elapsed())
            .ok_or("MQTT handshake timed out")?;
        match connection.recv_timeout(remaining) {
            Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => {
                // Drive the nonblocking disconnect before dropping the connection.
                let _ = client.try_disconnect();
                let _ = connection.recv_timeout(Duration::from_millis(100));
                return Ok(start.elapsed().as_millis() as u64);
            }
            Ok(Ok(_)) => {}
            Ok(Err(err)) => return Err(err.to_string()),
            Err(err) => return Err(format!("MQTT handshake: {err:?}")),
        }
    }
}

/// Probe brokers in parallel, returning results in input (priority) order.
///
/// Returns as soon as the pick is settled — every broker ahead of the first
/// reachable one has failed — rather than waiting out slower lower-priority
/// probes, which have a five-second MQTT deadline. Those report `Skipped`.
pub fn test_brokers_parallel(
    brokers: &[(&str, u16)],
    password: Option<&str>,
) -> Vec<BrokerTestResult> {
    let password = password.map(str::to_owned);
    test_brokers_with(brokers, move |host, port, use_tls| {
        ping_mqtt_broker(host, port, use_tls, password.as_deref())
    })
}

fn test_brokers_with(
    brokers: &[(&str, u16)],
    ping: impl Fn(&str, u16, bool) -> Result<u64, String> + Clone + Send + 'static,
) -> Vec<BrokerTestResult> {
    let mut outcomes: Vec<Option<BrokerProbe>> = vec![None; brokers.len()];
    let (tx, rx) = mpsc::channel();
    for (i, (host, port)) in brokers.iter().enumerate() {
        let (tx, host, port, ping) = (tx.clone(), host.to_string(), *port, ping.clone());
        // Detached: an abandoned probe finishes (or times out) on its own.
        std::thread::spawn(move || {
            let use_tls = port == 8883 || port == 8886;
            let _ = tx.send((i, ping(&host, port, use_tls)));
        });
    }
    drop(tx);

    let settled = |outcomes: &[Option<BrokerProbe>]| {
        for outcome in outcomes {
            match outcome {
                Some(BrokerProbe::Reachable(_)) => return true,
                Some(_) => {}
                None => return false,
            }
        }
        true
    };
    while !settled(&outcomes) {
        let Ok((i, ping_ms)) = rx.recv() else { break };
        outcomes[i] = Some(ping_ms.map_or_else(BrokerProbe::Failed, BrokerProbe::Reachable));
    }

    brokers
        .iter()
        .zip(outcomes)
        .map(|((host, port), outcome)| {
            (
                host.to_string(),
                *port,
                outcome.unwrap_or(BrokerProbe::Skipped),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    /// Stand up a localhost TCP listener whose worker accepts every connection
    /// and immediately drops it (so the peer sees a clean close). For `use_tls`
    /// callers this means TLS reads fail fast on EOF instead of waiting 5s for
    /// a `192.0.2.x`-style read timeout — that's the actual unreachable shape
    /// we want to exercise, without the TOCTOU of relying on an unclaimed
    /// ephemeral port.
    ///
    /// Returns the bound port. The accepting thread and listener are leaked
    /// (process-lifetime); fine for unit tests.
    fn spawn_closing_listener() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            while let Ok((stream, _)) = listener.accept() {
                drop(stream);
            }
        });
        port
    }

    #[test]
    fn test_test_brokers_parallel_empty() {
        let results = test_brokers_parallel(&[], None);
        assert!(results.is_empty());
    }

    #[test]
    fn test_ping_broker_unreachable_tls() {
        // TLS handshake against a peer that hangs up immediately: rustls fails
        // before complete_io returns, ping_broker returns None promptly.
        let port = spawn_closing_listener();
        let result = ping_broker("127.0.0.1", port, true);
        assert!(result.is_none(), "expected None, got {result:?}");
    }

    #[test]
    fn test_test_brokers_parallel_unreachable() {
        let p1 = spawn_closing_listener();
        let p2 = spawn_closing_listener();
        let brokers = &[("127.0.0.1", p1), ("127.0.0.1", p2)];
        // Pretend these are TLS broker ports so ping_broker drives the full
        // TLS handshake path against our closing listeners.
        let results: Vec<_> = brokers
            .iter()
            .map(|(h, p)| ping_broker(h, *p, true))
            .collect();
        assert_eq!(results, [None, None]);
    }

    /// Minimal MQTT 5 peer: TCP/TLS reachability alone must not count as success.
    fn mqtt_peer(reason: u8) -> (u16, std::thread::JoinHandle<Vec<u8>>) {
        use std::io::{Read, Write};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            assert_eq!(byte[0], 0x10);
            let mut len = 0;
            let mut multiplier = 1;
            loop {
                stream.read_exact(&mut byte).unwrap();
                len += usize::from(byte[0] & 0x7f) * multiplier;
                if byte[0] & 0x80 == 0 {
                    break;
                }
                multiplier *= 128;
            }
            let mut connect = vec![0; len];
            stream.read_exact(&mut connect).unwrap();
            stream.write_all(&[0x20, 3, 0, reason, 0]).unwrap();
            if reason == 0 {
                let _ = stream.read(&mut byte);
            }
            connect
        });
        (port, handle)
    }

    #[test]
    fn mqtt_probe_requires_connack_acceptance() {
        let (port, peer) = mqtt_peer(0x89); // Server busy / throttled.
        let err = ping_mqtt_broker("127.0.0.1", port, false, None).unwrap_err();
        assert!(err.contains("ServerBusy"), "{err}");
        peer.join().unwrap();
    }

    #[test]
    fn mqtt_probe_sends_private_broker_credentials() {
        let (port, peer) = mqtt_peer(0);
        assert!(ping_mqtt_broker("127.0.0.1", port, false, Some("test-secret")).is_ok());
        let connect = peer.join().unwrap();
        assert_eq!(connect[7] & 0xc0, 0xc0); // Username + password flags.
        assert!(connect.ends_with(b"\x00\x04hcom\x00\x0btest-secret"));
    }

    #[test]
    fn discovery_skips_a_broker_that_accepts_tcp_but_rejects_mqtt() {
        let (rejected, peer_a) = mqtt_peer(0x89);
        let (accepted, peer_b) = mqtt_peer(0);
        let results =
            test_brokers_parallel(&[("127.0.0.1", rejected), ("127.0.0.1", accepted)], None);
        assert!(matches!(results[0].2, BrokerProbe::Failed(_)));
        assert!(matches!(results[1].2, BrokerProbe::Reachable(_)));
        peer_a.join().unwrap();
        peer_b.join().unwrap();
    }

    fn fake_ping(host: &str, _port: u16, _tls: bool) -> Result<u64, String> {
        // host encodes "<delay_ms>:<ok|fail>"
        let (delay, ok) = host.split_once(':').unwrap();
        std::thread::sleep(Duration::from_millis(delay.parse().unwrap()));
        if ok == "ok" {
            Ok(1)
        } else {
            Err("unreachable".into())
        }
    }

    #[test]
    fn brokers_return_once_top_priority_answers() {
        let t0 = Instant::now();
        let results = test_brokers_with(&[("10:ok", 1), ("3000:fail", 2)], fake_ping);
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "waited for slow probe"
        );
        assert_eq!(results[0].2, BrokerProbe::Reachable(1));
        assert_eq!(results[1].2, BrokerProbe::Skipped);
    }

    #[test]
    fn brokers_wait_for_higher_priority_before_picking_lower() {
        let results =
            test_brokers_with(&[("200:fail", 1), ("10:ok", 2), ("3000:ok", 3)], fake_ping);
        assert!(matches!(results[0].2, BrokerProbe::Failed(_)));
        assert_eq!(results[1].2, BrokerProbe::Reachable(1));
        assert_eq!(results[2].2, BrokerProbe::Skipped);
    }

    #[test]
    fn brokers_all_failed() {
        let results = test_brokers_with(&[("10:fail", 1), ("20:fail", 2)], fake_ping);
        assert!(
            results
                .iter()
                .all(|r| matches!(r.2, BrokerProbe::Failed(_)))
        );
    }

    #[test]
    fn connect_any_skips_unconnectable_addresses() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let good = listener.local_addr().unwrap();
        // A just-closed port refuses immediately, standing in for an
        // unreachable IPv6 address listed first.
        let dead = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let stream = connect_any(&[dead, good]).expect("falls through to the live address");
        assert_eq!(stream.peer_addr().unwrap(), good);
    }
}
