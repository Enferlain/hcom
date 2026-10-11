//! Relay join token encode/decode.
//!
//! Tokens let devices share relay configuration compactly:
//!   v0x01: public broker, plaintext   — 0x01 + 16 UUID bytes + 1 broker index → 18 bytes → ~24 char base64url
//!   v0x02: private broker, plaintext  — 0x02 + 16 UUID bytes + URL bytes → variable length base64url
//!   v0x04: compact bearer token       — 0x04 + 16 UUID bytes + 32 PSK + (1 broker idx | URL bytes)
//!
//! Only v0x04 is minted. v0x01/v0x02 still parse so that `hcom relay connect` can give a useful error
//! to a user pasting an old token. Connecting with a legacy token refuses to
//! write the local config because those formats do not carry the relay PSK;
//! the source device must run `hcom relay new` on a current build to mint a
//! v0x04 token.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use super::DEFAULT_BROKERS;

const PSK_LEN: usize = 32;

/// Result of decoding a token. `psk` is `None` for legacy v0x01/v0x02 tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedToken {
    pub relay_id: String,
    pub broker_url: String,
    pub psk: Option<[u8; PSK_LEN]>,
}

/// Encode relay_id, broker, and PSK into a compact v0x04 join token.
pub fn encode_join_token(relay_id: &str, broker_url: &str, psk: &[u8; PSK_LEN]) -> Option<String> {
    let uuid_bytes = uuid_to_bytes(relay_id)?;

    // Public broker → 1+16+32+1 = 50 bytes; private broker → variable.
    for (i, (host, port)) in DEFAULT_BROKERS.iter().enumerate() {
        let mqtts = format!("mqtts://{}:{}", host, port);
        let mqtt = format!("mqtt://{}:{}", host, port);
        if broker_url == mqtts || broker_url == mqtt {
            let mut buf = Vec::with_capacity(1 + 16 + PSK_LEN + 1);
            buf.push(0x04);
            buf.extend_from_slice(&uuid_bytes);
            buf.extend_from_slice(psk);
            buf.push(i as u8);
            return Some(URL_SAFE_NO_PAD.encode(&buf));
        }
    }

    let mut buf = Vec::with_capacity(1 + 16 + PSK_LEN + broker_url.len());
    buf.push(0x04);
    buf.extend_from_slice(&uuid_bytes);
    buf.extend_from_slice(psk);
    buf.extend_from_slice(broker_url.as_bytes());
    Some(URL_SAFE_NO_PAD.encode(&buf))
}

/// Decode a join token. Returns `None` if the token is unparseable.
pub fn decode_join_token(token: &str) -> Option<DecodedToken> {
    let padded = match token.len() % 4 {
        2 => format!("{}==", token),
        3 => format!("{}=", token),
        _ => token.to_string(),
    };
    let raw = URL_SAFE_NO_PAD.decode(padded.trim_end_matches('=')).ok()?;

    if raw.len() < 17 {
        return None;
    }

    let version = raw[0];
    let relay_id = bytes_to_uuid(&raw[1..17])?;

    match version {
        0x01 => decode_public_tail(relay_id, &raw[17..], None),
        0x02 => decode_private_tail(relay_id, &raw[17..], None),
        0x04 => {
            // 16 uuid + 32 psk already consumed at this point; remaining tail
            // is either exactly 1 byte (public broker index) or N>=2 bytes of
            // URL. The length-1 discriminator is unambiguous because no valid
            // broker URL is one byte long (`parse_broker_url` requires at
            // minimum `host:port`). Using the URL scheme as a discriminator
            // would misparse bare `host:port` tails.
            if raw.len() < 17 + PSK_LEN + 1 {
                return None;
            }
            let mut psk = [0u8; PSK_LEN];
            psk.copy_from_slice(&raw[17..17 + PSK_LEN]);
            let tail = &raw[17 + PSK_LEN..];
            if tail.len() == 1 {
                decode_public_tail(relay_id, tail, Some(psk))
            } else {
                decode_private_tail(relay_id, tail, Some(psk))
            }
        }
        _ => None,
    }
}

fn decode_public_tail(
    relay_id: String,
    tail: &[u8],
    psk: Option<[u8; PSK_LEN]>,
) -> Option<DecodedToken> {
    if tail.is_empty() {
        return None;
    }
    let idx = tail[0] as usize;
    if idx >= DEFAULT_BROKERS.len() {
        return None;
    }
    let (host, port) = DEFAULT_BROKERS[idx];
    Some(DecodedToken {
        relay_id,
        broker_url: format!("mqtts://{}:{}", host, port),
        psk,
    })
}

fn decode_private_tail(
    relay_id: String,
    tail: &[u8],
    psk: Option<[u8; PSK_LEN]>,
) -> Option<DecodedToken> {
    let broker_url = std::str::from_utf8(tail).ok()?.to_string();
    Some(DecodedToken {
        relay_id,
        broker_url,
        psk,
    })
}

/// Parse UUID string (with hyphens) to 16 bytes.
fn uuid_to_bytes(uuid_str: &str) -> Option<[u8; 16]> {
    let hex: String = uuid_str.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

/// Format 16 bytes as hyphenated UUID string.
fn bytes_to_uuid(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 16 {
        return None;
    }
    let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_psk() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(7);
        }
        k
    }

    #[test]
    fn test_uuid_roundtrip() {
        let uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let bytes = uuid_to_bytes(uuid).unwrap();
        let back = bytes_to_uuid(&bytes).unwrap();
        assert_eq!(uuid, back);
    }

    #[test]
    fn test_uuid_to_bytes_invalid() {
        assert!(uuid_to_bytes("not-a-uuid").is_none());
        assert!(uuid_to_bytes("").is_none());
    }

    #[test]
    fn test_decode_legacy_v0x01_public_broker() {
        let relay_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let uuid_bytes = uuid_to_bytes(relay_id).unwrap();

        for (i, (host, port)) in DEFAULT_BROKERS.iter().enumerate() {
            let mut buf = vec![0x01];
            buf.extend_from_slice(&uuid_bytes);
            buf.push(i as u8);
            let decoded = decode_join_token(&URL_SAFE_NO_PAD.encode(&buf)).unwrap();
            assert_eq!(decoded.relay_id, relay_id, "broker index {}", i);
            assert_eq!(
                decoded.broker_url,
                format!("mqtts://{}:{}", host, port),
                "broker index {}",
                i
            );
            assert!(decoded.psk.is_none());
        }
    }

    #[test]
    fn test_decode_invalid_token() {
        assert!(decode_join_token("").is_none());
        assert!(decode_join_token("short").is_none());
        assert!(decode_join_token("!!!invalid-base64!!!").is_none());
    }

    #[test]
    fn test_decode_v0x01_bad_broker_index() {
        let uuid_bytes = uuid_to_bytes("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let mut buf = vec![0x01];
        buf.extend_from_slice(&uuid_bytes);
        buf.push(255);
        let token = URL_SAFE_NO_PAD.encode(&buf);
        assert!(decode_join_token(&token).is_none());
    }

    #[test]
    fn test_decode_legacy_v0x01_fixture() {
        let expected_token = "AaGyw9Tl9niQq83vEjRWeJAA";
        let decoded = decode_join_token(expected_token).unwrap();
        assert_eq!(decoded.relay_id, "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        assert_eq!(decoded.broker_url, "mqtts://broker.emqx.io:8883");
        assert!(decoded.psk.is_none());
    }

    #[test]
    fn test_decode_legacy_v0x02_fixture() {
        let expected_token =
            "AqGyw9Tl9niQq83vEjRWeJBtcXR0czovL215LXByaXZhdGUtYnJva2VyLmV4YW1wbGUuY29tOjg4ODM";
        let decoded = decode_join_token(expected_token).unwrap();
        assert_eq!(decoded.relay_id, "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        assert_eq!(
            decoded.broker_url,
            "mqtts://my-private-broker.example.com:8883"
        );
        assert!(decoded.psk.is_none());
    }

    #[test]
    fn test_encode_decode_v0x04_public_broker() {
        let relay_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let psk = fake_psk();
        for (i, (host, port)) in DEFAULT_BROKERS.iter().enumerate() {
            let broker = format!("mqtts://{}:{}", host, port);
            let token = encode_join_token(relay_id, &broker, &psk).unwrap();
            let decoded = decode_join_token(&token).unwrap();
            assert_eq!(decoded.relay_id, relay_id, "broker index {}", i);
            assert_eq!(decoded.broker_url, broker);
            assert_eq!(decoded.psk, Some(psk));
        }
    }

    #[test]
    fn test_encode_decode_v0x04_private_broker() {
        let relay_id = "12345678-1234-1234-1234-123456789abc";
        let broker = "mqtts://my-private-broker.example.com:8883";
        let psk = fake_psk();

        let token = encode_join_token(relay_id, broker, &psk).unwrap();
        let decoded = decode_join_token(&token).unwrap();
        assert_eq!(decoded.relay_id, relay_id);
        assert_eq!(decoded.broker_url, broker);
        assert_eq!(decoded.psk, Some(psk));
    }

    #[test]
    fn test_v0x04_public_token_compact() {
        // 1 + 16 + 32 + 1 = 50 bytes → ceil(50*4/3) = 67 base64url chars (no pad).
        let relay_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let broker = format!("mqtts://{}:{}", DEFAULT_BROKERS[0].0, DEFAULT_BROKERS[0].1);
        let token = encode_join_token(relay_id, &broker, &fake_psk()).unwrap();
        assert_eq!(token.len(), 67);
    }

    #[test]
    fn test_v0x04_truncated_psk_rejected() {
        let uuid_bytes = uuid_to_bytes("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        // 1 version + 16 uuid + 10 bytes (less than full PSK)
        let mut buf = vec![0x04];
        buf.extend_from_slice(&uuid_bytes);
        buf.extend_from_slice(&[0x11u8; 10]);
        let token = URL_SAFE_NO_PAD.encode(&buf);
        assert!(decode_join_token(&token).is_none());
    }

    #[test]
    fn test_encode_decode_v0x04_bare_host_port_private_broker() {
        // Regression: earlier draft used `tail.starts_with("mqtt")` as the
        // public/private discriminator, which misparsed bare `host:port`
        // brokers (accepted by parse_broker_url) as a 1-byte index and then
        // returned None. The length-1 discriminator handles this correctly.
        let relay_id = "12345678-1234-1234-1234-123456789abc";
        let broker = "my-private-broker.example.com:8883";
        let psk = fake_psk();

        let token = encode_join_token(relay_id, broker, &psk).unwrap();
        let decoded = decode_join_token(&token).unwrap();
        assert_eq!(decoded.relay_id, relay_id);
        assert_eq!(decoded.broker_url, broker);
        assert_eq!(decoded.psk, Some(psk));
    }

    #[test]
    fn test_v0x04_layout_is_stable() {
        // Decode produces the exact byte sequence we expect: version || uuid ||
        // psk || broker_index. Pinned so a non-Rust implementation can verify
        // the layout without us also pinning a base64 alphabet/order quirk.
        let relay_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let mut psk = [0u8; 32];
        for (i, b) in psk.iter_mut().enumerate() {
            *b = i as u8;
        }
        let broker = format!("mqtts://{}:{}", DEFAULT_BROKERS[0].0, DEFAULT_BROKERS[0].1);
        let token = encode_join_token(relay_id, &broker, &psk).unwrap();
        let raw = URL_SAFE_NO_PAD.decode(token.as_bytes()).unwrap();

        assert_eq!(raw[0], 0x04);
        assert_eq!(&raw[1..17], &uuid_to_bytes(relay_id).unwrap());
        assert_eq!(&raw[17..49], &psk);
        assert_eq!(raw[49], 0); // broker index 0 = first DEFAULT_BROKERS entry
        assert_eq!(raw.len(), 50);
        assert_eq!(token.len(), 67);
    }
}
