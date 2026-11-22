use std::io::{Read, Write};
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};

use super::POLL_INTERVAL;
use super::wire::{try_read, try_write};
use crate::sandbox::protocol::Binding;

pub(super) const BYTES: usize = 32;
const TIMEOUT: Duration = Duration::from_secs(5);
const DOMAIN: &[u8] = b"hcom sandbox guest startup v1\0";

pub(super) struct Check {
    nonce: [u8; BYTES],
    sent: usize,
    answer: [u8; BYTES],
    received: usize,
    key: hmac::Key,
    transcript: Vec<u8>,
    deadline: Instant,
}

impl Check {
    pub(super) fn new(key: &[u8; BYTES], binding: &Binding, epoch: i64) -> Result<Self> {
        ensure!(
            key.iter().any(|&byte| byte != 0),
            "invalid sandbox startup key"
        );
        let mut nonce = [0; BYTES];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| anyhow::anyhow!("could not generate sandbox startup challenge"))?;
        Ok(Self {
            nonce,
            sent: 0,
            answer: [0; BYTES],
            received: 0,
            key: hmac::Key::new(hmac::HMAC_SHA256, key),
            transcript: transcript(&nonce, binding, epoch)?,
            deadline: Instant::now() + TIMEOUT,
        })
    }

    pub(super) fn poll(&mut self, input: &mut impl Read, output: &mut impl Write) -> Result<bool> {
        ensure!(
            Instant::now() < self.deadline,
            "sandbox guest proof timed out"
        );
        if self.sent < BYTES {
            self.sent += try_write(output, &self.nonce[self.sent..])?;
            if self.sent < BYTES {
                return Ok(false);
            }
        }
        let Some(count) = try_read(input, &mut self.answer[self.received..])? else {
            return Ok(false);
        };
        ensure!(count != 0, "sandbox guest closed before its startup proof");
        self.received += count;
        if self.received < BYTES {
            return Ok(false);
        }
        hmac::verify(&self.key, &self.transcript, &self.answer)
            .map_err(|_| anyhow::anyhow!("sandbox guest startup proof did not match"))?;
        Ok(true)
    }
}

pub(super) fn answer(
    input: &mut impl Read,
    output: &mut impl Write,
    key: &[u8; BYTES],
    binding: &Binding,
    epoch: i64,
) -> Result<()> {
    let deadline = Instant::now() + TIMEOUT;
    let mut nonce = [0; BYTES];
    let mut received = 0;
    while received < BYTES {
        ensure!(
            Instant::now() < deadline,
            "sandbox startup challenge timed out"
        );
        match try_read(input, &mut nonce[received..])? {
            Some(0) => anyhow::bail!("sandbox startup challenge disconnected"),
            Some(count) => received += count,
            None => std::thread::sleep(POLL_INTERVAL),
        }
    }
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let proof = hmac::sign(&key, &transcript(&nonce, binding, epoch)?);
    let mut sent = 0;
    while sent < BYTES {
        ensure!(
            Instant::now() < deadline,
            "sandbox guest proof write timed out"
        );
        let count = try_write(output, &proof.as_ref()[sent..])?;
        if count == 0 {
            std::thread::sleep(POLL_INTERVAL);
        }
        sent += count;
    }
    Ok(())
}

fn transcript(nonce: &[u8; BYTES], binding: &Binding, epoch: i64) -> Result<Vec<u8>> {
    let mut bytes = DOMAIN.to_vec();
    bytes.extend(nonce);
    bytes.extend(epoch.to_be_bytes());
    serde_json::to_writer(&mut bytes, binding)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_proof_waits_and_a_proof_from_another_connection_fails() {
        let key = [7; BYTES];
        let binding = Binding {
            team: "team".into(),
            sandbox: "sandbox".into(),
            worker: crate::sandbox::protocol::Origin {
                worker: "worker".into(),
                generation: "generation".into(),
                incarnation: uuid::Uuid::new_v4().to_string(),
            },
        };
        let mut check = Check::new(&key, &binding, 7).unwrap();
        let mut no_answer = ReadFn(|_: &mut [u8]| Err(std::io::ErrorKind::WouldBlock.into()));
        let mut challenge = Vec::new();
        assert!(!check.poll(&mut no_answer, &mut challenge).unwrap());
        let mut proof = Vec::new();
        answer(&mut challenge.as_slice(), &mut proof, &key, &binding, 7).unwrap();
        let mut replaced = Check::new(&key, &binding, 7).unwrap();
        assert!(
            replaced
                .poll(&mut proof.as_slice(), &mut Vec::new())
                .is_err()
        );

        for (other, epoch) in [
            (
                Binding {
                    team: "other".into(),
                    ..binding.clone()
                },
                7,
            ),
            (binding.clone(), 8),
        ] {
            let mut check = Check::new(&key, &binding, 7).unwrap();
            let mut challenge = Vec::new();
            assert!(!check.poll(&mut no_answer, &mut challenge).unwrap());
            let mut answer_bytes = Vec::new();
            answer(
                &mut challenge.as_slice(),
                &mut answer_bytes,
                &key,
                &other,
                epoch,
            )
            .unwrap();
            assert!(
                check
                    .poll(&mut answer_bytes.as_slice(), &mut Vec::new())
                    .is_err()
            );
        }
        assert!(
            !check
                .poll(&mut &proof[..BYTES / 2], &mut Vec::new())
                .unwrap()
        );
        assert!(
            check
                .poll(&mut &proof[BYTES / 2..], &mut Vec::new())
                .unwrap()
        );
    }

    struct ReadFn<F>(F);
    impl<F: FnMut(&mut [u8]) -> std::io::Result<usize>> Read for ReadFn<F> {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            (self.0)(bytes)
        }
    }
}
