//! Future-event waits with explicit cursor and inbox ownership boundaries.
use super::*;
use std::time::Instant;

pub(super) struct WaitOptions<'a> {
    pub(super) after_id: Option<i64>,
    pub(super) full_output: bool,
    pub(super) filters: &'a HashMap<String, Vec<String>>,
    pub(super) instance_name: Option<&'a str>,
}

pub(super) fn wait_for_event(
    db: &HcomDb,
    filter_query: &str,
    timeout: u64,
    options: WaitOptions<'_>,
) -> i32 {
    let mut listener = EventListener::new(
        db,
        EventListenerOptions {
            after_id: options.after_id,
            instance_name: options.instance_name,
            endpoint_kind: EVENTS_WAIT_ENDPOINT_KIND,
        },
    );
    let start = Instant::now();
    let mut preview_shown = false;
    loop {
        match listener.query_next_event(filter_query) {
            Ok(Some(event)) => {
                let mut output = if options.full_output {
                    event
                } else {
                    streamline_event(&event, options.filters)
                };
                if output.get("type").and_then(Value::as_str) == Some("message")
                    && let Some(data) = output.get_mut("data").and_then(Value::as_object_mut)
                    && let Some(text) = data.get("text").cloned()
                {
                    data.entry("message").or_insert(text);
                }
                println!("{}", output);
                return 0;
            }
            Ok(None) => {}
            Err(error) => {
                eprintln!("Error in SQL WHERE clause: {error}");
                return 2;
            }
        }
        if filter_query.is_empty()
            && !listener.has_explicit_cursor()
            && !preview_shown
            && let Some(name) = options.instance_name
            && !db.get_unread_messages(name).is_empty()
        {
            println!("{}", build_message_preview(db, name));
            preview_shown = true;
        }
        let remaining = Duration::from_secs(timeout).saturating_sub(start.elapsed());
        if remaining.is_zero() {
            println!("{}", json!({"timed_out": true}));
            return 1;
        }
        listener.wait_tick(listener.bounded_recheck_duration(Some(remaining)));
    }
}
