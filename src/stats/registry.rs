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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointIdAllocator;

    fn register(id: EndpointId, name: &str) -> StatsEvent {
        StatsEvent::Register {
            id,
            name: name.to_string(),
            stats: Arc::new(EndpointStats::default()),
            routable: true,
        }
    }

    #[test]
    fn finalize_of_unknown_id_is_a_no_op() {
        let mut registry = HashMap::new();
        let mut queue = VecDeque::new();
        let mut dropped = 0;
        let id = EndpointIdAllocator::new().alloc();
        handle_event(
            &mut registry,
            &mut queue,
            8,
            &mut dropped,
            true,
            StatsEvent::Finalize { id },
        );
        assert!(queue.is_empty());
        assert_eq!(dropped, 0);
    }

    #[test]
    fn interval_lines_are_regular_and_finalize_lines_synthetic() {
        let alloc = EndpointIdAllocator::new();
        let mut registry = HashMap::new();
        let mut queue = VecDeque::new();
        let mut dropped = 0;
        let first = alloc.alloc();
        let second = alloc.alloc();
        handle_event(
            &mut registry,
            &mut queue,
            8,
            &mut dropped,
            true,
            register(first, "first"),
        );
        handle_event(
            &mut registry,
            &mut queue,
            8,
            &mut dropped,
            true,
            register(second, "second"),
        );
        assert!(queue.is_empty());

        emit_interval_lines(&registry, &mut queue, 8, &mut dropped);
        assert_eq!(queue.len(), 2);
        assert!(queue.iter().all(|entry| !entry.synthetic));

        handle_event(
            &mut registry,
            &mut queue,
            8,
            &mut dropped,
            true,
            StatsEvent::Finalize { id: first },
        );
        assert_eq!(registry.len(), 1);
        let last = queue.back().expect("finalize line");
        assert!(last.synthetic);
        assert_eq!(last.line.endpoint(), "first");
        assert_eq!(dropped, 0);
    }
}
