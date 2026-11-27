use std::collections::HashSet;
use std::io::{ErrorKind, Read, Write};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const VERSION: u32 = 1;
pub const MAX_ENCODED_FRAME: u32 = 256 * 1024;
const HEADER_LEN: u32 = 4;
pub(super) const MAX_DIRECTORY_PEERS: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Origin {
    pub worker: String,
    pub generation: String,
    pub incarnation: String,
}

impl Origin {
    pub(super) fn validate(&self) -> Result<()> {
        validate_identifier(&self.worker)?;
        validate_identifier(&self.generation)?;
        let incarnation = Uuid::try_parse(&self.incarnation)?;
        ensure!(
            !incarnation.is_nil() && incarnation.hyphenated().to_string() == self.incarnation,
            "invalid database incarnation"
        );
        Ok(())
    }
}

pub(super) fn validate_identifier(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)),
        "invalid sandbox identifier"
    );
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(super) struct InvalidMessageText(String);

/// Validate the text independently of the wire identity and payload digest.
/// An authenticated send can receive a terminal rejection for invalid text.
pub(super) fn validate_message_text(text: &str) -> std::result::Result<(), InvalidMessageText> {
    if text
        .chars()
        .any(|c| c.is_control() && c != '\n' && c != '\t')
    {
        return Err(InvalidMessageText(
            "sandbox message contains prohibited control characters".into(),
        ));
    }
    crate::messages::validate_message(text).map_err(InvalidMessageText)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageId {
    pub origin: Origin,
    pub sequence: i64,
}

impl MessageId {
    fn validate(&self) -> Result<()> {
        self.origin.validate()?;
        ensure!(self.sequence > 0, "invalid message sequence");
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Intent {
    Request,
    Inform,
    Ack,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessagePayload {
    pub target: String,
    // Preserve the digest of retained messages written before this field was added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_generation: Option<String>,
    /// Host-authored messages carry the sender's opaque directory generation
    /// for reply correlation. Guest senders use their bound origin generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_routing_generation: Option<String>,
    /// Opaque host-issued conversation id for granted scoped threads. Only
    /// the host sets it, inside the grant transaction, before digesting; the
    /// canonical form omits it otherwise so retained digests stay stable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<String>,
    /// Broadcast frames require matching builds. Directed payload digests remain stable.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub broadcast: bool,
    pub thread: Option<String>,
    pub reply_to: Option<MessageId>,
    pub intent: Intent,
    pub text: String,
}

impl MessagePayload {
    pub(super) fn addresses(&self, target: &Origin) -> bool {
        if self.broadcast {
            self.target.is_empty() && self.target_generation.is_none()
        } else {
            self.target == target.worker
                && self
                    .target_generation
                    .as_ref()
                    .is_none_or(|generation| generation == &target.generation)
        }
    }

    /// A guest-authored thread expansion request: scoped to a conversation,
    /// naming no target and no generation at all. The single predicate for
    /// frame and inbox paths, so the shape cannot drift between them.
    pub(super) fn is_thread_expansion(&self) -> bool {
        self.conversation.as_ref().is_some_and(|_| {
            self.target.is_empty()
                && self.target_generation.is_none()
                && self.sender_routing_generation.is_none()
        })
    }

    /// A host-authored grant delivery: fully addressed with host routing.
    /// Only this form can create destination evidence.
    pub(super) fn is_conversation_delivery(&self) -> bool {
        self.conversation.as_ref().is_some_and(|_| {
            !self.target.is_empty()
                && self.target_generation.is_some()
                && self.sender_routing_generation.is_some()
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub id: MessageId,
    pub payload: MessagePayload,
    pub digest: [u8; 32],
}

impl Message {
    pub fn new(id: MessageId, payload: MessagePayload) -> Result<Self> {
        let digest = digest_payload(&payload)?;
        let message = Self {
            id,
            payload,
            digest,
        };
        message.validate()?;
        Ok(message)
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.id.validate()?;
        if let Some(reply) = &self.payload.reply_to {
            reply.validate()?;
        }
        if self.payload.broadcast {
            ensure!(
                self.payload.target.is_empty(),
                "sandbox broadcast cannot specify a target"
            );
            ensure!(
                self.payload.target_generation.is_none(),
                "sandbox broadcast cannot specify a target generation"
            );
            ensure!(
                self.payload.conversation.is_none(),
                "sandbox broadcast cannot specify a conversation"
            );
            ensure!(
                self.payload.thread.is_none(),
                "sandbox broadcast cannot specify a thread"
            );
            ensure!(
                self.payload.reply_to.is_none(),
                "sandbox broadcast cannot specify a reply target"
            );
        } else if let Some(conversation) = &self.payload.conversation {
            // A scoped payload takes exactly one of two strict forms, so a
            // frame can never blur grant delivery and expansion dispatch:
            // host grant delivery is fully addressed with host routing,
            // while a guest thread expansion request names no target and no
            // generation at all. Both always carry the thread label, and
            // neither carries a reply: replies belong to directed messages
            // whose parent they can name.
            validate_identifier(conversation)?;
            ensure!(
                self.payload.thread.is_some(),
                "sandbox conversation requires a thread"
            );
            ensure!(
                self.payload.reply_to.is_none() && self.payload.intent != Intent::Ack,
                "sandbox conversation payloads cannot carry replies"
            );
            let addressed = self.payload.is_conversation_delivery();
            let expansion = self.payload.is_thread_expansion();
            ensure!(
                addressed || expansion,
                "sandbox conversation payload mixes scoped forms"
            );
            if let Some(generation) = &self.payload.target_generation {
                validate_identifier(generation)?;
            }
            if !self.payload.target.is_empty() {
                validate_identifier(&self.payload.target)?;
            }
        } else {
            validate_identifier(&self.payload.target)?;
            if let Some(generation) = &self.payload.target_generation {
                validate_identifier(generation)?;
            }
        }
        if let Some(generation) = &self.payload.sender_routing_generation {
            validate_identifier(generation)?;
        }
        if let Some(thread) = &self.payload.thread {
            validate_identifier(thread)?;
        }
        ensure!(
            self.digest == digest_payload(&self.payload)?,
            "sandbox payload digest mismatch"
        );
        Ok(())
    }
}

fn digest_payload(payload: &MessagePayload) -> Result<[u8; 32]> {
    Ok(Sha256::digest(serde_json::to_vec(payload)?).into())
}

/// The controller chooses this binding independently of data supplied by the guest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub team: String,
    pub sandbox: String,
    pub worker: Origin,
}

impl Binding {
    pub(super) fn validate(&self) -> Result<()> {
        validate_identifier(&self.team)?;
        validate_identifier(&self.sandbox)?;
        self.worker.validate()
    }
}

/// Only the routing fields a guest needs. Host and sandbox internals stay host-side.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamPeer {
    pub name: String,
    pub alias: Option<String>,
    pub generation: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "body",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum FrameBody {
    /// Version 1 peers agree on one frame limit, including the length prefix.
    Hello {
        max_frame_bytes: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        directory: Option<Vec<TeamPeer>>,
    },
    Send(Message),
    Deliver(Message),
    Accepted {
        id: MessageId,
        digest: [u8; 32],
    },
    /// The host permanently declined this guest message.
    Rejected {
        id: MessageId,
        digest: [u8; 32],
    },
    Close,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frame {
    pub version: u32,
    pub epoch: i64,
    pub binding: Binding,
    pub body: FrameBody,
}

impl Frame {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == VERSION,
            "unsupported sandbox protocol version"
        );
        ensure!(self.epoch > 0, "invalid sandbox connection epoch");
        self.binding.validate()?;
        match &self.body {
            FrameBody::Send(message) => {
                ensure!(
                    message.id.origin == self.binding.worker,
                    "sandbox sender mismatch"
                );
                ensure!(
                    message.payload.sender_routing_generation.is_none(),
                    "guest cannot set a sandbox sender routing generation"
                );
                // Guests may carry a conversation id only as a thread
                // expansion request, never as an addressed grant claim.
                ensure!(
                    !message.payload.conversation.is_some()
                        || message.payload.is_thread_expansion(),
                    "guest thread expansion requests carry no target or generation"
                );
                message.validate()
            }
            FrameBody::Deliver(message) => {
                // A scoped expansion request is forwarded verbatim to each
                // grant-holding destination; its grant check happens in the
                // destination inbox, not the frame.
                ensure!(
                    message.payload.is_thread_expansion()
                        || message.payload.addresses(&self.binding.worker),
                    "sandbox delivery target mismatch"
                );
                message.validate()
            }
            FrameBody::Accepted { id, .. } => id.validate(),
            FrameBody::Rejected { id, .. } => {
                ensure!(
                    id.origin == self.binding.worker,
                    "sandbox rejection sender mismatch"
                );
                id.validate()
            }
            FrameBody::Hello {
                max_frame_bytes,
                directory,
            } => {
                ensure!(
                    *max_frame_bytes == MAX_ENCODED_FRAME,
                    "incompatible sandbox frame limit"
                );
                if let Some(peers) = directory {
                    ensure!(
                        peers.len() <= MAX_DIRECTORY_PEERS,
                        "sandbox directory exceeds peer limit"
                    );
                    let mut names = HashSet::new();
                    for peer in peers {
                        validate_identifier(&peer.name)?;
                        validate_identifier(&peer.generation)?;
                        if let Some(alias) = &peer.alias {
                            ensure!(
                                alias.len() <= 24 && crate::identity::is_valid_base_name(alias),
                                "invalid sandbox directory alias"
                            );
                        }
                        ensure!(
                            peer.name != self.binding.worker.worker && names.insert(&peer.name),
                            "sandbox directory contains a self or duplicate target"
                        );
                    }
                }
                Ok(())
            }
            FrameBody::Close => Ok(()),
        }
    }
}

fn encode(frame: &Frame) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(frame)?;
    ensure!(
        bytes.len() <= (MAX_ENCODED_FRAME - HEADER_LEN) as usize,
        "sandbox frame exceeds size limit"
    );
    Ok(bytes)
}

pub fn write_frame(writer: &mut impl Write, frame: &Frame) -> Result<()> {
    frame.validate()?;
    let bytes = encode(frame)?;
    writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
    writer.write_all(&bytes)?;
    Ok(())
}

pub(super) fn frame_length(header: [u8; HEADER_LEN as usize]) -> Result<usize> {
    let len = u32::from_be_bytes(header);
    ensure!(
        (1..=MAX_ENCODED_FRAME - HEADER_LEN).contains(&len),
        "invalid sandbox frame length"
    );
    Ok(len as usize)
}

pub(super) fn decode_frame(bytes: &[u8]) -> Result<Frame> {
    let frame: Frame = serde_json::from_slice(bytes)?;
    frame.validate()?;
    Ok(frame)
}

fn read_frame(reader: &mut impl Read) -> Result<Option<Frame>> {
    let mut header = [0; HEADER_LEN as usize];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    reader.read_exact(&mut header[1..])?;
    let mut bytes = vec![0; frame_length(header)?];
    reader.read_exact(&mut bytes)?;
    Ok(Some(decode_frame(&bytes)?))
}

#[derive(Clone, Copy)]
pub enum Receiver {
    Host,
    Guest,
}

pub struct Connection {
    binding: Binding,
    epoch: i64,
    receiver: Receiver,
    ready: bool,
    closed: bool,
}

impl Connection {
    pub fn new(binding: Binding, epoch: i64, receiver: Receiver) -> Result<Self> {
        binding.validate()?;
        ensure!(epoch > 0, "invalid sandbox connection epoch");
        Ok(Self {
            binding,
            epoch,
            receiver,
            ready: false,
            closed: false,
        })
    }

    /// The caller supplies the currently owned epoch for every frame. Use a
    /// blocking reader. An interrupted syscall is retried; any other error
    /// closes the stream so a partially consumed frame cannot be retried.
    pub fn receive(
        &mut self,
        reader: &mut impl Read,
        current_epoch: i64,
    ) -> Result<Option<FrameBody>> {
        let result = self.receive_inner(reader, current_epoch);
        if !matches!(&result, Ok(Some(_))) {
            self.closed = true;
        }
        result
    }

    fn receive_inner(
        &mut self,
        reader: &mut impl Read,
        current_epoch: i64,
    ) -> Result<Option<FrameBody>> {
        self.check_current(current_epoch)?;
        let Some(frame) = read_frame(reader)? else {
            return Ok(None);
        };
        Ok(Some(self.accept(frame, current_epoch)?))
    }

    fn check_current(&self, current_epoch: i64) -> Result<()> {
        ensure!(!self.closed, "sandbox connection is closed");
        ensure!(
            current_epoch == self.epoch,
            "stale sandbox connection epoch"
        );
        Ok(())
    }

    pub(super) fn accept(&mut self, frame: Frame, current_epoch: i64) -> Result<FrameBody> {
        let result = self.accept_inner(frame, current_epoch);
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    fn accept_inner(&mut self, frame: Frame, current_epoch: i64) -> Result<FrameBody> {
        self.check_current(current_epoch)?;
        ensure!(frame.epoch == self.epoch, "sandbox frame epoch mismatch");
        ensure!(
            frame.binding == self.binding,
            "sandbox connection binding mismatch"
        );
        ensure!(
            matches!(frame.body, FrameBody::Hello { .. }) != self.ready,
            "sandbox handshake out of order"
        );
        match (&self.receiver, &frame.body) {
            (Receiver::Host, FrameBody::Rejected { .. }) => {
                anyhow::bail!("guest cannot reject a sandbox send");
            }
            (
                Receiver::Host,
                FrameBody::Hello {
                    directory: Some(_), ..
                },
            ) => {
                anyhow::bail!("guest cannot publish a sandbox team directory");
            }
            (Receiver::Host, FrameBody::Deliver(_)) | (Receiver::Guest, FrameBody::Send(_)) => {
                anyhow::bail!("sandbox frame direction mismatch");
            }
            (_, FrameBody::Hello { .. }) => self.ready = true,
            (_, FrameBody::Close) => self.closed = true,
            _ => {}
        }
        Ok(frame.body)
    }
}

/// Size the longest message envelope, including the maximum decimal epoch
/// width. This sizing frame is never sent or checked against a live epoch.
pub(super) fn validate_message_size(
    receiver: Receiver,
    binding: &Binding,
    message: &Message,
) -> Result<i64> {
    let frame = Frame {
        version: VERSION,
        epoch: i64::MAX,
        binding: binding.clone(),
        body: match receiver {
            Receiver::Host => FrameBody::Send(message.clone()),
            Receiver::Guest => FrameBody::Deliver(message.clone()),
        },
    };
    frame.validate()?;
    Ok(encode(&frame)?.len() as i64 + i64::from(HEADER_LEN))
}

#[cfg(test)]
mod tests;
