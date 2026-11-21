use std::io::{ErrorKind, Read, Write};
use std::os::fd::AsFd;

use anyhow::{Result, ensure};
use nix::fcntl::{FcntlArg, OFlag, fcntl};

use crate::sandbox::protocol::{Frame, decode_frame, frame_length};

pub(super) fn nonblocking(fd: &impl AsFd) -> Result<()> {
    let flags = OFlag::from_bits_truncate(fcntl(fd, FcntlArg::F_GETFL)?);
    fcntl(fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
    Ok(())
}

#[derive(Default)]
pub(super) struct Reader {
    header: [u8; 4],
    body: Vec<u8>,
    used: usize,
}

pub(super) enum Input {
    Waiting,
    Progress,
    Frame(Box<Frame>),
    Eof,
}

impl Reader {
    pub(super) fn step(&mut self, input: &mut impl Read) -> Result<Input> {
        let reading_header = self.body.is_empty();
        let remaining = if reading_header {
            &mut self.header[self.used..]
        } else {
            &mut self.body[self.used..]
        };
        let Some(count) = try_read(input, remaining)? else {
            return Ok(Input::Waiting);
        };
        if count == 0 {
            ensure!(reading_header && self.used == 0, "truncated sandbox frame");
            return Ok(Input::Eof);
        }
        self.used += count;
        if self.used
            != if reading_header {
                self.header.len()
            } else {
                self.body.len()
            }
        {
            return Ok(Input::Progress);
        }
        self.used = 0;
        if reading_header {
            self.body.resize(frame_length(self.header)?, 0);
            return Ok(Input::Progress);
        }
        let frame = decode_frame(&self.body)
            .map_err(|_| anyhow::anyhow!("invalid sandbox bridge frame"))?;
        self.body.clear();
        Ok(Input::Frame(Box::new(frame)))
    }
}

pub(super) fn try_read(input: &mut impl Read, buf: &mut [u8]) -> Result<Option<usize>> {
    match input.read(buf) {
        Ok(count) => Ok(Some(count)),
        Err(error)
            if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::Interrupted =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) fn try_write(output: &mut impl Write, bytes: &[u8]) -> Result<usize> {
    match output.write(bytes) {
        Ok(count) => {
            ensure!(count > 0, "sandbox bridge write made no progress");
            Ok(count)
        }
        Err(error)
            if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::Interrupted =>
        {
            Ok(0)
        }
        Err(error) => Err(error.into()),
    }
}
