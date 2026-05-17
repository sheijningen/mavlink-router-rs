//! Dedicated stats task scaffold.
//!
//! The stats task is one of the four top-level task roles (per-endpoint
//! reader+writer pairs, the router, and the stats task itself) introduced by
//! Phase 5a. It owns the registry mirror — `EndpointId → (name,
//! Arc<EndpointStats>)` — fed by the router over a [`StatsEvent`] channel,
//! and drops registry entries on `Finalize`. The interval-driven JSON-Lines
//! output, drop-oldest `mpsc<StatsLine>`, `BrokenPipe` handling, and
//! `stats_dropped` WARN are Phase 7 deliverables (CLAUDE.md "Stats task
//! scaffold" bullet under Phase 5a is explicit: "emits no output yet").
//!
//! The scaffold's only behavioural surface in 5a is: accept `Register` /
//! `Finalize` without blocking the router, exit cleanly on cancel, and
//! drain any in-flight events the router produced inside its own drain
//! window so the registry view of "which endpoints existed at shutdown" is
//! complete when Phase 7 starts emitting final synthetic lines.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use crate::endpoint::EndpointId;
use crate::endpoint::stats::EndpointStats;

/// One lifecycle message from the router to the stats task. Forwarded
/// fire-and-forget per CLAUDE.md — the router never awaits a stats-task
/// acknowledgement.
#[derive(Debug)]
pub enum StatsEvent {
    Register {
        id: EndpointId,
        name: String,
        stats: Arc<EndpointStats>,
    },
    Finalize {
        id: EndpointId,
    },
}

/// One row of the stats task's registry mirror. Phase 7 reads `name` and
/// `stats` when emitting JSON-Lines; today the scaffold just inserts and
/// removes rows so the registry view is complete when Phase 7 lands.
#[derive(Debug)]
#[allow(dead_code)]
struct RegisteredEndpoint {
    name: String,
    stats: Arc<EndpointStats>,
}

/// Run the stats task until the cancellation token fires. Until Phase 7
/// wires the interval timer + JSON-Lines output, this loop just maintains
/// the registry mirror so the router's fire-and-forget `StatsEvent` sends
/// do not block its hot path.
///
/// On cancel the task drains any events the router already pushed into the
/// channel (`PeerRemoved → Finalize` and shutdown-sweep `Finalize`s) before
/// returning — Phase 7 needs that drained view to emit each endpoint's
/// authoritative final stats line.
pub async fn run(mut stats_event_rx: mpsc::Receiver<StatsEvent>, cancel: CancellationToken) {
    let mut registry: HashMap<EndpointId, RegisteredEndpoint> = HashMap::new();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            ev = stats_event_rx.recv() => match ev {
                Some(ev) => handle_event(&mut registry, ev),
                None => {
                    debug!("stats_event channel closed; stats task exiting");
                    return;
                }
            },
        }
    }

    // Cancellation has fired. Drain whatever the router managed to send
    // before its own drain finished — `try_recv` is non-blocking so we
    // never wait past the cancel; any Finalize / Register the router could
    // still produce after this point is bounded by its own drain budget,
    // and the harness's per-task 2s budget keeps both tasks from lingering.
    while let Ok(ev) = stats_event_rx.try_recv() {
        handle_event(&mut registry, ev);
    }
}

fn handle_event(registry: &mut HashMap<EndpointId, RegisteredEndpoint>, ev: StatsEvent) {
    match ev {
        StatsEvent::Register { id, name, stats } => {
            trace!(%id, %name, "stats: register");
            registry.insert(id, RegisteredEndpoint { name, stats });
        }
        StatsEvent::Finalize { id } => {
            trace!(%id, "stats: finalize");
            registry.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use std::time::Duration;

    fn make_register(id: EndpointId, name: &str) -> StatsEvent {
        StatsEvent::Register {
            id,
            name: name.to_string(),
            stats: Arc::new(EndpointStats::default()),
        }
    }

    #[tokio::test]
    async fn task_exits_on_cancel_even_with_sender_alive() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(4);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run(rx, cancel.clone()));
        // Keep sender alive so the channel doesn't close; cancel must still
        // wake the select.
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("stats task did not exit on cancel")
            .expect("stats task panicked");
        drop(tx);
    }

    #[tokio::test]
    async fn task_exits_when_router_drops_sender() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(4);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run(rx, cancel));
        drop(tx);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("stats task did not exit when channel closed")
            .expect("stats task panicked");
    }

    #[tokio::test]
    async fn register_then_finalize_drains_cleanly() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run(rx, cancel.clone()));

        let alloc = EndpointIdAllocator::new();
        let id_a = alloc.alloc();
        let id_b = alloc.alloc();
        tx.send(make_register(id_a, "a")).await.expect("send a");
        tx.send(make_register(id_b, "b")).await.expect("send b");
        tx.send(StatsEvent::Finalize { id: id_a })
            .await
            .expect("send finalize a");

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");
        drop(tx);
    }

    #[tokio::test]
    async fn pending_events_drained_after_cancel() {
        // CLAUDE.md: "the stats task ... process any pending Finalizes from
        // its stats_event_rx" within its drain budget. Exercise the
        // try_recv drain branch by queueing one event between cancel and
        // task wake-up.
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let alloc = EndpointIdAllocator::new();
        let id = alloc.alloc();
        tx.send(make_register(id, "pending")).await.expect("send");
        cancel.cancel();
        // Race-free path: cancel.cancel() makes the cancelled branch ready,
        // the biased select picks it, and the post-loop try_recv catches
        // the queued register.
        let handle = tokio::spawn(run(rx, cancel));
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");
        drop(tx);
    }
}
