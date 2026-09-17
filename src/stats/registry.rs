//! Registry mirror of live endpoints and the per-event and per-tick
//! production of lines from it.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tracing::trace;

use super::handle::StatsEvent;
use super::line::{build_line, rfc3339_now};
use super::queue::{QueueEntry, enqueue_regular, enqueue_synthetic};
use crate::endpoint::EndpointId;
use crate::endpoint::stats::EndpointStats;

/// One row of the stats task's registry mirror — the name + stats handle
/// the task snapshots on every interval tick, plus the routable flag that
/// picks the emitted line's schema.
#[derive(Debug)]
pub(super) struct RegisteredEndpoint {
    name: String,
    stats: Arc<EndpointStats>,
    routable: bool,
}

pub(super) fn handle_event(
    registry: &mut HashMap<EndpointId, RegisteredEndpoint>,
    queue: &mut VecDeque<QueueEntry>,
    queue_capacity: usize,
    total_dropped: &mut u64,
    emit_lines: bool,
    event: StatsEvent,
) {
    match event {
        StatsEvent::Register {
            id,
            name,
            stats,
            routable,
        } => {
            trace!(%id, %name, routable, "stats: register");
            registry.insert(
                id,
                RegisteredEndpoint {
                    name,
                    stats,
                    routable,
                },
            );
        }
        StatsEvent::Finalize { id } => {
            trace!(%id, "stats: finalize");
            let Some(entry) = registry.remove(&id) else {
                return;
            };
            if !emit_lines {
                return;
            }
            let line = build_line(&entry.name, &entry.stats, rfc3339_now(), entry.routable);
            enqueue_synthetic(queue, queue_capacity, line, total_dropped);
        }
    }
}

pub(super) fn emit_interval_lines(
    registry: &HashMap<EndpointId, RegisteredEndpoint>,
    queue: &mut VecDeque<QueueEntry>,
    queue_capacity: usize,
    total_dropped: &mut u64,
) {
    if registry.is_empty() {
        return;
    }
    let ts = rfc3339_now();
    for entry in registry.values() {
        let line = build_line(&entry.name, &entry.stats, ts.clone(), entry.routable);
        enqueue_regular(queue, queue_capacity, line, total_dropped);
    }
}
