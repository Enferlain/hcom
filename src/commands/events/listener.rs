//! Cursor-based event queries and bounded wake endpoints.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
pub(crate) const EVENTS_WAIT_ENDPOINT_KIND: &str = "events_wait";
pub(crate) const EVENTS_STREAM_ENDPOINT_KIND: &str = "events_stream";

const LISTENER_NOTIFY_POLL_INTERVAL: Duration = Duration::from_secs(5);
const LISTENER_FALLBACK_RECHECK_INTERVAL: Duration = Duration::from_millis(500);
const STREAM_RECHECK_INTERVAL: Duration = Duration::from_millis(500);
const LISTENER_TCP_POLL_INTERVAL: Duration = Duration::from_millis(200);

pub(crate) struct EventListenerOptions<'a> {
    pub after_id: Option<i64>,
    pub instance_name: Option<&'a str>,
    pub endpoint_kind: &'a str,
}

pub(crate) struct EventListener<'a> {
    db: &'a HcomDb,
    cursor: i64,
    has_explicit_cursor: bool,
    instance_name: Option<String>,
    endpoint_kind: String,
    notify_server: Option<TcpListener>,
    notify_port: Option<u16>,
}

impl<'a> EventListener<'a> {
    pub(crate) fn new(db: &'a HcomDb, options: EventListenerOptions<'a>) -> Self {
        let has_explicit_cursor = options.after_id.is_some();
        let cursor = options.after_id.unwrap_or_else(|| db.get_last_event_id());

        let mut notify_server: Option<TcpListener> = None;
        let mut notify_port: Option<u16> = None;
        if let Some(name) = options.instance_name
            && let Ok(server) = TcpListener::bind("127.0.0.1:0")
            && let Ok(addr) = server.local_addr()
        {
            let port = addr.port();
            server.set_nonblocking(true).ok();
            if db
                .upsert_notify_endpoint(name, options.endpoint_kind, port)
                .is_ok()
            {
                notify_server = Some(server);
                notify_port = Some(port);
            }
        }

        Self {
            db,
            cursor,
            has_explicit_cursor,
            instance_name: options.instance_name.map(str::to_string),
            endpoint_kind: options.endpoint_kind.to_string(),
            notify_server,
            notify_port,
        }
    }

    pub(crate) fn cursor(&self) -> i64 {
        self.cursor
    }

    pub(crate) fn has_explicit_cursor(&self) -> bool {
        self.has_explicit_cursor
    }

    pub(crate) fn is_listening(&self) -> bool {
        self.notify_server.is_some()
    }

    pub(crate) fn query_next_events(
        &mut self,
        filter_query: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Value>, rusqlite::Error> {
        let query = format!("SELECT * FROM events_v WHERE id > ?{filter_query} ORDER BY id");
        let mut stmt = self.db.conn().prepare(&query)?;
        let mut events = Vec::new();
        let mut rows = stmt.query(rusqlite::params![self.cursor])?;
        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => break,
            };
            if let Ok(id) = row.get::<_, i64>("id") {
                self.cursor = id;
            }
            if let Ok(event) = parse_event_row(row) {
                events.push(event);
                if let Some(max) = limit
                    && events.len() >= max
                {
                    break;
                }
            }
        }
        Ok(events)
    }

    pub(crate) fn query_next_event(
        &mut self,
        filter_query: &str,
    ) -> Result<Option<Value>, rusqlite::Error> {
        let mut events = self.query_next_events(filter_query, Some(1))?;
        Ok(events.pop())
    }

    pub(crate) fn drain_new_events(
        &mut self,
        filter_query: &str,
    ) -> Result<Vec<Value>, rusqlite::Error> {
        let boundary = self.db.get_last_event_id();
        if boundary <= self.cursor {
            return Ok(Vec::new());
        }
        let query =
            format!("SELECT * FROM events_v WHERE id > ? AND id <= ?{filter_query} ORDER BY id");
        let mut stmt = self.db.conn().prepare(&query)?;
        let mut events = Vec::new();
        let mut rows = stmt.query(rusqlite::params![self.cursor, boundary])?;
        while let Some(row) = rows.next()? {
            if let Ok(event) = parse_event_row(row) {
                events.push(event);
            }
        }
        self.cursor = boundary;
        Ok(events)
    }

    pub(crate) fn bounded_recheck_duration(&self, ceiling: Option<Duration>) -> Duration {
        let base = if self.endpoint_kind == EVENTS_STREAM_ENDPOINT_KIND {
            STREAM_RECHECK_INTERVAL
        } else if self.is_listening() {
            LISTENER_NOTIFY_POLL_INTERVAL
        } else {
            LISTENER_FALLBACK_RECHECK_INTERVAL
        };
        match ceiling {
            Some(limit) => base.min(limit),
            None => base,
        }
    }

    pub(crate) fn wait_tick(&self, max_wait: Duration) -> bool {
        static DUMMY_INTERRUPT: AtomicBool = AtomicBool::new(false);
        self.wait_tick_interruptible(max_wait, &DUMMY_INTERRUPT)
    }

    pub(crate) fn wait_tick_interruptible(
        &self,
        max_wait: Duration,
        interrupted: &AtomicBool,
    ) -> bool {
        let poll_end = Instant::now() + max_wait;
        let slice = Duration::from_millis(50);
        if let Some(ref server) = self.notify_server {
            while Instant::now() < poll_end {
                if interrupted.load(Ordering::Relaxed) {
                    return false;
                }
                if let Ok((conn, _)) = server.accept() {
                    let _ = conn.shutdown(std::net::Shutdown::Both);
                    return true;
                }
                let remaining = poll_end.saturating_duration_since(Instant::now());
                std::thread::sleep(LISTENER_TCP_POLL_INTERVAL.min(remaining).min(slice));
            }
            false
        } else {
            while Instant::now() < poll_end {
                if interrupted.load(Ordering::Relaxed) {
                    return false;
                }
                let remaining = poll_end.saturating_duration_since(Instant::now());
                std::thread::sleep(remaining.min(slice));
            }
            false
        }
    }

    pub(crate) fn cleanup(&mut self) {
        if let (Some(name), Some(_port)) = (&self.instance_name, self.notify_port.take()) {
            let _ = self.db.delete_notify_endpoint(name, &self.endpoint_kind);
        }
        self.notify_server = None;
    }
}

impl Drop for EventListener<'_> {
    fn drop(&mut self) {
        self.cleanup();
    }
}
