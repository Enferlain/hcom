//! Low-level stdio inspection used for orphan detection.

/// Whether this process's stdin appears broken/invalid (a heuristic for
/// detecting that the launching parent is gone).
///
/// Unix: a non-blocking `poll` of fd 0 that reports `POLLERR`/`POLLNVAL` (but
/// *not* `POLLHUP`, which is the normal end of a piped payload). Windows: always
/// false — pipe close semantics differ and orphan detection there relies on
/// other signals.
pub fn stdin_appears_broken() -> bool {
    #[cfg(unix)]
    {
        let mut pfd = libc::pollfd {
            fd: 0,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: single valid pollfd, nfds=1, timeout=0 (non-blocking).
        let ret = unsafe { libc::poll(&mut pfd as *mut _, 1, 0) };
        if ret < 0 {
            return true;
        }
        (pfd.revents & (libc::POLLERR | libc::POLLNVAL)) != 0
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Whether stdout's reader has gone away (pipe read end closed, terminal hung
/// up, or fd closed), so a streaming command can exit while idle instead of
/// waiting for its next write to fail.
///
/// Unix: a non-blocking `poll` of fd 1 for `POLLOUT`, checking only
/// `POLLERR`/`POLLHUP`/`POLLNVAL`. macOS reports a pipe's closed reader only
/// when an event is requested; Linux reports `POLLERR` either way. Platforms
/// that report neither fall back to the write error. Windows: always false.
pub fn stdout_reader_gone() -> bool {
    #[cfg(unix)]
    {
        let mut pfd = libc::pollfd {
            fd: 1,
            events: libc::POLLOUT,
            revents: 0,
        };
        // SAFETY: single valid pollfd, nfds=1, timeout=0 (non-blocking).
        let ret = unsafe { libc::poll(&mut pfd as *mut _, 1, 0) };
        ret > 0 && (pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
    }
    #[cfg(not(unix))]
    {
        false
    }
}
