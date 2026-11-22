use std::collections::VecDeque;
use std::io::{Read, Write};
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};

use crate::sandbox::openshell::ROTATION_BYTES;
use crate::sandbox::protocol::{
    Binding, Connection, Frame, FrameBody, MAX_ENCODED_FRAME, MessageId, Receiver, TeamPeer,
    VERSION, write_frame,
};
use crate::sandbox::store::{Import, Store};
use crate::shared::time::now_epoch_i64;

use super::Inbound;
use super::wire::{Input, Reader, try_write};

#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub rotation: u64,
    pub lifetime: Duration,
    pub progress: Duration,
    pub drain: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            rotation: ROTATION_BYTES,
            lifetime: Duration::from_secs(5 * 60),
            progress: Duration::from_secs(15),
            drain: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteKind {
    Hello,
    Payload,
    Receipt,
    Close,
}

struct PendingWrite {
    bytes: Vec<u8>,
    offset: usize,
    kind: WriteKind,
}

struct Outstanding {
    id: MessageId,
    digest: [u8; 32],
    sent: bool,
    deadline: Instant,
}

pub(super) struct Peer<'store, 'db> {
    store: &'store Store<'db>,
    attached: Option<Store<'db>>,
    binding: &'store Binding,
    receiver: Receiver,
    epoch: i64,
    connection: Connection,
    reader: Reader,
    writes: VecDeque<PendingWrite>,
    outstanding: Option<Outstanding>,
    incoming_credit_used: bool,
    ready: bool,
    pub remote_closed: bool,
    pub input_eof: bool,
    pub close_written: bool,
    pub draining: Option<Instant>,
    rotate: bool,
    started: Instant,
    read_deadline: Option<Instant>,
    write_deadline: Option<Instant>,
    next_queue_check: Instant,
    next_fence_check: Instant,
    input_limit: Option<u64>,
    input_bytes: u64,
    limits: Limits,
}

impl<'store, 'db> Peer<'store, 'db> {
    pub(super) fn new(
        store: &'store Store<'db>,
        binding: &'store Binding,
        epoch: i64,
        receiver: Receiver,
        input_limit: Option<u64>,
        limits: Limits,
        directory: Option<Vec<TeamPeer>>,
    ) -> Result<Self> {
        let now = Instant::now();
        let mut peer = Self {
            store,
            attached: None,
            binding,
            receiver,
            epoch,
            connection: Connection::new(binding.clone(), epoch, receiver)?,
            reader: Reader::default(),
            writes: VecDeque::new(),
            outstanding: None,
            incoming_credit_used: false,
            ready: false,
            remote_closed: false,
            input_eof: false,
            close_written: false,
            draining: None,
            rotate: false,
            started: now,
            read_deadline: None,
            write_deadline: None,
            next_queue_check: now,
            next_fence_check: now,
            input_limit,
            input_bytes: 0,
            limits,
        };
        if matches!(receiver, Receiver::Host) {
            peer.attach()?;
            peer.queue_hello(directory)?;
        }
        Ok(peer)
    }

    pub(super) fn rotating(&self) -> bool {
        self.rotate
    }

    pub(super) fn is_ready(&self) -> bool {
        self.ready
    }

    #[cfg(test)]
    pub(super) fn receipt_pending(&self) -> bool {
        self.writes
            .iter()
            .any(|write| write.kind == WriteKind::Receipt)
    }

    pub(super) fn start_clock(&mut self) {
        self.started = Instant::now();
        self.next_queue_check = self.started;
        self.write_deadline =
            (!self.writes.is_empty()).then_some(self.started + self.limits.progress);
    }

    fn attach(&mut self) -> Result<()> {
        self.attached = Some(self.store.attach_bridge(self.binding, self.epoch)?);
        Ok(())
    }

    fn encode(&self, body: FrameBody) -> Result<Vec<u8>> {
        let frame = Frame {
            version: VERSION,
            epoch: self.epoch,
            binding: self.binding.clone(),
            body,
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &frame)?;
        Ok(bytes)
    }

    fn queue(&mut self, kind: WriteKind, bytes: Vec<u8>) -> Result<()> {
        if let Some(allowance) = self.input_limit {
            self.input_bytes += bytes.len() as u64;
            ensure!(
                self.input_bytes <= allowance,
                "sandbox bridge exhausted qualified stdin allowance"
            );
        }
        if self.writes.is_empty() {
            self.write_deadline = Some(Instant::now() + self.limits.progress);
        }
        self.writes.push_back(PendingWrite {
            bytes,
            offset: 0,
            kind,
        });
        Ok(())
    }

    fn queue_hello(&mut self, directory: Option<Vec<TeamPeer>>) -> Result<()> {
        let bytes = self.encode(FrameBody::Hello {
            max_frame_bytes: MAX_ENCODED_FRAME,
            directory,
        })?;
        self.queue(WriteKind::Hello, bytes)
    }

    pub(super) fn begin_drain(&mut self, rotate: bool) -> Result<()> {
        if self.draining.is_none() {
            self.draining = Some(Instant::now() + self.limits.drain);
            self.rotate = rotate;
            let bytes = self.encode(FrameBody::Close)?;
            self.queue(WriteKind::Close, bytes)?;
        }
        Ok(())
    }

    pub(super) fn check(&mut self) -> Result<()> {
        let now = Instant::now();
        if let Some(store) = &self.attached
            && (now >= self.next_fence_check || !self.writes.is_empty())
        {
            store.check_bridge()?;
            self.next_fence_check = now + Duration::from_millis(40);
        }
        if let Some(deadline) = self.draining {
            ensure!(now < deadline, "sandbox bridge drain timed out");
        } else {
            ensure!(
                self.ready || now < self.started + self.limits.progress,
                "sandbox bridge handshake timed out"
            );
            ensure!(
                !self.read_deadline.is_some_and(|deadline| now >= deadline),
                "sandbox bridge frame read timed out"
            );
            ensure!(
                !self.write_deadline.is_some_and(|deadline| now >= deadline),
                "sandbox bridge write timed out"
            );
            ensure!(
                !self
                    .outstanding
                    .as_ref()
                    .is_some_and(|send| now >= send.deadline),
                "sandbox bridge receipt timed out"
            );
        }
        Ok(())
    }

    pub(super) fn poll_outbox(&mut self) -> Result<bool> {
        if !self.ready || self.draining.is_some() || self.input_eof {
            return Ok(false);
        }
        let now = Instant::now();
        if now >= self.started + self.limits.lifetime {
            ensure!(
                matches!(self.receiver, Receiver::Host),
                "sandbox bridge host did not rotate"
            );
            self.begin_drain(true)?;
            return Ok(true);
        }
        if self.outstanding.is_some() || now < self.next_queue_check {
            return Ok(false);
        }
        let store = self.attached.as_ref().expect("handshake attaches bridge");
        let now_epoch = now_epoch_i64();
        if matches!(self.receiver, Receiver::Guest) {
            store.expire_guest_pending_if_due(now_epoch)?;
        }
        let Some(message) = store.next_pending(self.binding, now_epoch)? else {
            self.next_queue_check = now + Duration::from_millis(40);
            return Ok(false);
        };
        let outstanding = Outstanding {
            id: message.id.clone(),
            digest: message.digest,
            sent: false,
            deadline: now + self.limits.progress,
        };
        let bytes = self.encode(match self.receiver {
            Receiver::Host => FrameBody::Deliver(message),
            Receiver::Guest => FrameBody::Send(message),
        })?;
        if self.input_limit.is_some()
            && self.input_bytes + bytes.len() as u64 > self.limits.rotation
        {
            self.begin_drain(true)?;
        } else {
            self.queue(WriteKind::Payload, bytes)?;
            self.outstanding = Some(outstanding);
        }
        Ok(true)
    }

    pub(super) fn write_step(&mut self, output: &mut impl Write) -> Result<bool> {
        let Some(write) = self.writes.front_mut() else {
            return Ok(false);
        };
        let count = try_write(output, &write.bytes[write.offset..])?;
        if count == 0 {
            return Ok(false);
        }
        write.offset += count;
        if write.offset == write.bytes.len() {
            self.write_deadline = if self.writes.len() > 1 {
                Some(Instant::now() + self.limits.progress)
            } else {
                None
            };
            match self.writes.pop_front().expect("nonempty write queue").kind {
                WriteKind::Hello => {}
                WriteKind::Payload => {
                    let outstanding = self.outstanding.as_mut().expect("queued payload");
                    outstanding.sent = true;
                    outstanding.deadline = Instant::now() + self.limits.progress;
                }
                WriteKind::Receipt => self.incoming_credit_used = false,
                WriteKind::Close => self.close_written = true,
            }
        }
        Ok(true)
    }

    pub(super) fn read_step(
        &mut self,
        input: &mut impl Read,
        import: &mut impl Inbound,
    ) -> Result<bool> {
        if self.input_eof {
            return Ok(false);
        }
        match self.reader.step(input)? {
            Input::Waiting => Ok(false),
            Input::Progress => {
                self.read_deadline
                    .get_or_insert_with(|| Instant::now() + self.limits.progress);
                Ok(true)
            }
            Input::Eof => {
                self.input_eof = true;
                Ok(true)
            }
            Input::Frame(frame) => {
                self.read_deadline = None;
                let body = self.connection.accept(*frame, self.epoch)?;
                self.handle(body, import)?;
                Ok(true)
            }
        }
    }

    fn handle(&mut self, body: FrameBody, import: &mut impl Inbound) -> Result<()> {
        if let FrameBody::Hello { directory, .. } = body {
            if matches!(self.receiver, Receiver::Guest) {
                self.attach()?;
                let store = self.attached.as_ref().expect("handshake attaches bridge");
                store.write(|tx| import.replace_directory(tx, directory.as_deref()))?;
                self.queue_hello(None)?;
            }
            self.ready = true;
            return Ok(());
        }
        let store = self.attached.as_ref().expect("handshake attaches bridge");
        match body {
            FrameBody::Send(message) | FrameBody::Deliver(message) => {
                ensure!(
                    !self.incoming_credit_used,
                    "sandbox bridge exceeded payload credit"
                );
                self.incoming_credit_used = true;
                let result = store.import(
                    self.receiver,
                    self.binding,
                    &message,
                    now_epoch_i64(),
                    |tx| import.insert(tx, &message),
                )?;
                if let Import::Inserted(event_id) = result {
                    import.committed(event_id);
                }
                // After queueing our close we still persist a peer's final
                // payload. Any undelivered receipt is recovered by replay.
                if self.draining.is_none() {
                    let receipt = match result {
                        Import::Rejected => FrameBody::Rejected {
                            id: message.id,
                            digest: message.digest,
                        },
                        Import::Inserted(_) | Import::Replay(_) => FrameBody::Accepted {
                            id: message.id,
                            digest: message.digest,
                        },
                    };
                    let bytes = self.encode(receipt)?;
                    self.queue(WriteKind::Receipt, bytes)?;
                    if self.input_limit.is_some() && self.input_bytes >= self.limits.rotation {
                        self.begin_drain(true)?;
                    }
                }
            }
            FrameBody::Accepted { ref id, ref digest }
            | FrameBody::Rejected { ref id, ref digest } => {
                ensure!(
                    self.outstanding.as_ref().is_some_and(|send| {
                        send.sent && &send.id == id && &send.digest == digest
                    }),
                    "sandbox bridge receipt does not match the sent payload"
                );
                let now = now_epoch_i64();
                if matches!(body, FrameBody::Rejected { .. }) {
                    store.reject(self.binding, id, digest, now)?;
                } else {
                    store.acknowledge(self.binding, id, digest, now)?;
                }
                self.outstanding = None;
            }
            FrameBody::Close => {
                match self.receiver {
                    Receiver::Host => ensure!(
                        self.draining.is_some(),
                        "sandbox bridge peer closed before host drain"
                    ),
                    Receiver::Guest => self.begin_drain(false)?,
                }
                self.remote_closed = true;
            }
            FrameBody::Hello { .. } => unreachable!(),
        }
        Ok(())
    }

    pub(super) fn note_eof(&mut self) {
        if self.draining.is_none() {
            self.draining = Some(Instant::now() + self.limits.drain);
        }
    }
}
