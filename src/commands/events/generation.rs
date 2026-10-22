//! Exact worker-generation identity and stop boundaries.
use super::*;
use crate::messages::sender_instance_key;
use std::collections::BTreeMap;
/// One immutable worker generation selected at a durable attempt boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WorkerGeneration {
    pub(super) exact_worker: String,
    pub(super) instance_key: String,
    pub(super) after_id: i64,
    pub(super) stop_event_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationEventDisposition {
    Ignore,
    Observe,
    ObserveAndStop,
    Terminate,
}

#[derive(Debug, Clone)]
pub(super) struct WorkerGenerationFollow {
    pub(super) generation: WorkerGeneration,
    pub(super) stopped: bool,
}

impl WorkerGenerationFollow {
    pub(super) fn classify(&mut self, event: &Value) -> GenerationEventDisposition {
        if self.stopped {
            return GenerationEventDisposition::Ignore;
        }

        let Some(event_id) = event.get("id").and_then(Value::as_i64) else {
            return GenerationEventDisposition::Ignore;
        };
        if event_id <= self.generation.after_id {
            return GenerationEventDisposition::Ignore;
        }
        if let Some(stop_id) = self.generation.stop_event_id
            && event_id > stop_id
        {
            self.stopped = true;
            return GenerationEventDisposition::Terminate;
        }

        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let instance = event
            .get("instance")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let data = event.get("data").unwrap_or(&Value::Null);

        if event_type == "life" && data.get("action").and_then(Value::as_str) == Some("stopped") {
            if data.get("placeholder").and_then(Value::as_bool) == Some(true) {
                return GenerationEventDisposition::Ignore;
            }
            if data.get("soft").and_then(Value::as_bool) == Some(true) {
                return if data
                    .get("snapshot")
                    .and_then(|snapshot| sender_instance_key(instance, snapshot))
                    .as_deref()
                    == Some(self.generation.instance_key.as_str())
                {
                    GenerationEventDisposition::Observe
                } else {
                    GenerationEventDisposition::Ignore
                };
            }
            let snapshot_key = data
                .get("snapshot")
                .and_then(|snapshot| sender_instance_key(instance, snapshot))
                .map(|key| key.to_string());
            match snapshot_key.as_deref() {
                Some(key) if key == self.generation.instance_key => {
                    self.stopped = true;
                    self.generation.stop_event_id = Some(event_id);
                    return GenerationEventDisposition::ObserveAndStop;
                }
                Some(_) => return GenerationEventDisposition::Ignore,
                None if instance.eq_ignore_ascii_case(&self.generation.exact_worker) => {
                    self.stopped = true;
                    return GenerationEventDisposition::Terminate;
                }
                None => return GenerationEventDisposition::Ignore,
            }
        }

        if event_type == "message" {
            return if data.get("sender_instance_key").and_then(Value::as_str)
                == Some(self.generation.instance_key.as_str())
            {
                GenerationEventDisposition::Observe
            } else {
                GenerationEventDisposition::Ignore
            };
        }

        if instance.eq_ignore_ascii_case(&self.generation.exact_worker) {
            GenerationEventDisposition::Observe
        } else {
            GenerationEventDisposition::Ignore
        }
    }
}

pub(super) fn discover_worker_generation(
    db: &HcomDb,
    label: &str,
    worker: &str,
    after_id: i64,
) -> Result<WorkerGeneration, String> {
    let mut generations: BTreeMap<String, WorkerGeneration> = BTreeMap::new();

    if let Some(exact_worker) = crate::identity::resolve_display_name(db, worker)
        && let Some(instance_data) = db
            .get_instance(&exact_worker)
            .map_err(|error| format!("failed to read {label} worker: {error}"))?
        && let Some(instance_key) = sender_instance_key(&exact_worker, &instance_data)
    {
        generations.insert(
            instance_key.clone(),
            WorkerGeneration {
                exact_worker,
                instance_key,
                after_id,
                stop_event_id: None,
            },
        );
    }

    let mut statement = db
        .conn()
        .prepare(
            "SELECT id, instance, data FROM events
             WHERE id > ?1 AND type = 'life'
               AND json_valid(data)
               AND json_extract(data, '$.action') = 'stopped'
               AND json_extract(data, '$.placeholder') IS NOT TRUE
             ORDER BY id",
        )
        .map_err(|error| format!("failed to inspect stopped workers: {error}"))?;
    let stopped = statement
        .query_map(rusqlite::params![after_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|error| format!("failed to inspect stopped workers: {error}"))?;
    for row in stopped {
        let (event_id, name, data) =
            row.map_err(|error| format!("failed to inspect stopped worker row: {error}"))?;
        let Ok(data) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        let soft = data.get("soft").and_then(Value::as_bool) == Some(true);
        let Some(snapshot) = data.get("snapshot") else {
            continue;
        };
        let tag = snapshot
            .get("tag")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        let display_name = tag.map_or_else(|| name.clone(), |tag| format!("{tag}-{name}"));
        if !name.eq_ignore_ascii_case(worker) && !display_name.eq_ignore_ascii_case(worker) {
            continue;
        }
        let Some(instance_key) = sender_instance_key(&name, snapshot) else {
            continue;
        };
        generations
            .entry(instance_key.clone())
            .and_modify(|generation| {
                if !soft {
                    generation.stop_event_id = Some(
                        generation
                            .stop_event_id
                            .map_or(event_id, |current| current.min(event_id)),
                    );
                }
            })
            .or_insert(WorkerGeneration {
                exact_worker: name,
                instance_key,
                after_id,
                stop_event_id: (!soft).then_some(event_id),
            });
    }

    let mut generations = generations.into_values();
    let Some(generation) = generations.next() else {
        return Err(format!(
            "{label} worker '{worker}' has no generation after the attempt cursor"
        ));
    };
    if generations.next().is_some() {
        return Err(format!(
            "{label} worker '{worker}' resolves to multiple generations after the attempt cursor"
        ));
    }
    Ok(generation)
}
