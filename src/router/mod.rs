//! Central router task.
//!
//! Owns the per-endpoint learn tables and a registry of every routing
//! endpoint the system currently knows about. Receives lifecycle events
//! and frames on two bounded mpscs, applies the per-destination decision
//! to each frame against every other registered endpoint's learn-set,
//! and pushes admitted frames into each destination's [`TxQueue`].
//!
//! The Phase 5a skeleton implements four behaviours from CLAUDE.md:
//!
//! - **Registry maintenance** — handle `EndpointAdded` / `PeerAdded` /
//!   `PeerRemoved`, forwarding `StatsEvent::Register` / `Finalize` to the
//!   stats task in lockstep.
//! - **Source learn** — touch the source endpoint's learn-set on every
//!   inbound frame and keep the `learn_entries` stats counter in sync.
//! - **Routing decision** — for every *other* registered endpoint, run
//!   [`decide::admit_to`] and push the frame to that destination's
//!   `TxQueue` when admitted (cheap `Bytes::clone` Arc bumps; the queue
//!   handles drop-oldest overflow and increments `dropped_tx`).
//! - **Shutdown sweep** — on cancel, write `state = Down` for every
//!   remaining registered endpoint and forward `Finalize` so the stats
//!   task can drop its registry mirror.
//!
//! Policy (filters, sniffer, dedup, groups, seq tracker) is Phase 5b and
//! lands in [`decide`] / [`dedup`] / [`group`] alongside.

pub mod decide;
pub mod dedup;
pub mod group;
pub mod learn;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use crate::endpoint::EndpointId;
use crate::endpoint::events::{EndpointEvent, PeerRemovalReason, RouterFrame};
use crate::endpoint::identity_flags::IdentityFlags;
use crate::endpoint::stats::{EndpointState, EndpointStats};
use crate::endpoint::tx_queue::TxQueue;
use crate::stats::StatsEvent;

use decide::admit_to;
use learn::LearnTable;

/// One registered routing endpoint. The router is the sole writer of the
/// fields owned here (`learn`, the registry slot); `stats` and `tx_queue`
/// are `Arc`-shared with the endpoint's reader/writer and may be observed
/// without coordination.
struct RegisteredEndpoint {
    name: String,
    is_top_level: bool,
    parent_id: Option<EndpointId>,
    tx_queue: TxQueue,
    stats: Arc<EndpointStats>,
    /// Filter / sniffer / group / seq-tracker capacities. Held here so
    /// Phase 5b can wire `passes_out_filter`, sniffer override, and the
    /// shared-learn-set group path without touching the spawner.
    #[allow(dead_code)]
    identity: IdentityFlags,
    learn: LearnTable,
    /// `false` for supervisory entries (`tcps:` / `udps:` parent listeners
    /// — they appear in stats but their `TxQueue` has no consumer, so
    /// pushing to it would just inflate `dropped_tx`). Sub-endpoints
    /// admitted via `PeerAdded` are always routable.
    routable: bool,
}

/// Bundle the spawner hands to the router task. The two `mpsc::Receiver`s
/// drive the entire data plane (lifecycle events + frames); the
/// `stats_event_tx` is fire-and-forget per CLAUDE.md ("the router does not
/// await a stats-task acknowledgement").
pub struct RouterWiring {
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    pub event_rx: mpsc::Receiver<EndpointEvent>,
    pub stats_event_tx: mpsc::Sender<StatsEvent>,
    pub cancel: CancellationToken,
}

/// Run the router task until the cancellation token fires. Biased select
/// over cancel, then events, then frames per CLAUDE.md's "Endpoint
/// registration is symmetric, and ordering is enforced by biased select"
/// decision — every available lifecycle event drains before the next
/// frame is taken, so the registry is always at least as caught up as
/// the frame stream.
pub async fn run(wiring: RouterWiring) {
    let RouterWiring {
        mut frame_rx,
        mut event_rx,
        stats_event_tx,
        cancel,
    } = wiring;

    let mut registry: HashMap<EndpointId, RegisteredEndpoint> = HashMap::new();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            Some(ev) = event_rx.recv() => {
                handle_event(&mut registry, ev, &stats_event_tx).await;
            }
            Some(fr) = frame_rx.recv() => {
                handle_frame(&mut registry, fr);
            }
            else => break,
        }
    }

    // Drain any lifecycle events the parent listeners managed to send
    // between cancel firing and the router's drop of `event_rx`. Catches
    // in-flight `PeerRemoved`s so the stats task's mirror sees the right
    // final-state for every sub-endpoint that was torn down during the
    // drain window.
    while let Ok(ev) = event_rx.try_recv() {
        handle_event(&mut registry, ev, &stats_event_tx).await;
    }

    shutdown_sweep(&mut registry, &stats_event_tx).await;
}

async fn handle_event(
    registry: &mut HashMap<EndpointId, RegisteredEndpoint>,
    ev: EndpointEvent,
    stats_event_tx: &mpsc::Sender<StatsEvent>,
) {
    match ev {
        EndpointEvent::EndpointAdded {
            id,
            name,
            tx_queue,
            stats,
            identity,
            routable,
        } => {
            let learn = LearnTable::new(identity.learn_capacity);
            let entry = RegisteredEndpoint {
                name: name.clone(),
                is_top_level: true,
                parent_id: None,
                tx_queue,
                stats: stats.clone(),
                identity,
                learn,
                routable,
            };
            trace!(%id, %name, routable, "router: endpoint added");
            registry.insert(id, entry);
            let _ = stats_event_tx
                .send(StatsEvent::Register { id, name, stats })
                .await;
        }
        EndpointEvent::PeerAdded {
            parent_id,
            child_id,
            peer_addr: _,
            name,
            tx_queue,
            stats,
            identity,
        } => {
            let learn = LearnTable::new(identity.learn_capacity);
            let entry = RegisteredEndpoint {
                name: name.clone(),
                is_top_level: false,
                parent_id: Some(parent_id),
                tx_queue,
                stats: stats.clone(),
                identity,
                learn,
                routable: true,
            };
            trace!(%child_id, %parent_id, %name, "router: peer added");
            registry.insert(child_id, entry);
            let _ = stats_event_tx
                .send(StatsEvent::Register {
                    id: child_id,
                    name,
                    stats,
                })
                .await;
        }
        EndpointEvent::PeerRemoved {
            parent_id,
            child_id,
            peer_addr: _,
            reason,
        } => {
            let final_state = match reason {
                PeerRemovalReason::Idle => EndpointState::Idle,
                PeerRemovalReason::LruEvicted
                | PeerRemovalReason::Disconnected
                | PeerRemovalReason::ListenerShutdown => EndpointState::Down,
            };
            if let Some(entry) = registry.remove(&child_id) {
                entry.stats.store_state(final_state);
                trace!(%child_id, %parent_id, ?reason, ?final_state, "router: peer removed");
            }
            let _ = stats_event_tx
                .send(StatsEvent::Finalize { id: child_id })
                .await;
        }
    }
}

fn handle_frame(registry: &mut HashMap<EndpointId, RegisteredEndpoint>, fr: RouterFrame) {
    let RouterFrame {
        endpoint_id: src_id,
        frame,
        header,
    } = fr;
    let now = Instant::now();

    {
        let Some(src_ep) = registry.get_mut(&src_id) else {
            // Should be unreachable in healthy operation per CLAUDE.md's
            // registration-before-frame invariant; surface at DEBUG so a
            // spawner-ordering regression is visible.
            debug!(%src_id, "router: frame from unknown endpoint; dropped");
            return;
        };
        if src_ep.learn.touch(header.sysid, header.compid, now) {
            src_ep
                .stats
                .learn_entries
                .store(src_ep.learn.len() as u64, Ordering::Relaxed);
        }
    }

    for (dest_id, dest_ep) in registry.iter() {
        if *dest_id == src_id {
            continue;
        }
        if !dest_ep.routable {
            continue;
        }
        if !admit_to(&header, &dest_ep.learn) {
            continue;
        }
        dest_ep.tx_queue.push(frame.clone());
    }
}

async fn shutdown_sweep(
    registry: &mut HashMap<EndpointId, RegisteredEndpoint>,
    stats_event_tx: &mpsc::Sender<StatsEvent>,
) {
    for (id, entry) in registry.drain() {
        // CLAUDE.md "On shutdown the router walks its top-level registry,
        // writes state = Down". Sub-endpoints normally exit via
        // PeerRemoved before the sweep; any survivor here is one whose
        // removal event we didn't get to before cancel fired — Down is
        // still the safe terminal value.
        entry.stats.store_state(EndpointState::Down);
        trace!(%id, name = %entry.name, top_level = entry.is_top_level, "router: shutdown finalize");
        let _ = stats_event_tx.send(StatsEvent::Finalize { id }).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use crate::endpoint::events::PeerRemovalReason;
    use crate::mavlink::frame::{ParsedHeader, Version};
    use bytes::Bytes;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    fn header(sysid: u8, compid: u8, target_system: Option<u8>) -> ParsedHeader {
        ParsedHeader {
            version: Version::V2,
            sysid,
            compid,
            msgid: 0,
            seq: 0,
            payload_len: 0,
            incompat_flags: 0,
            compat_flags: 0,
            target_system,
            target_component: None,
        }
    }

    fn fake_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 12345)
    }

    struct EndpointFixture {
        id: EndpointId,
        stats: Arc<EndpointStats>,
        tx_queue: TxQueue,
    }

    fn make_endpoint(alloc: &EndpointIdAllocator, top_level: bool) -> EndpointFixture {
        let id = alloc.alloc();
        let stats = if top_level {
            Arc::new(EndpointStats::new(EndpointState::Reconnecting))
        } else {
            Arc::new(EndpointStats::new(EndpointState::Connected))
        };
        let tx_queue = TxQueue::new(8, stats.clone());
        EndpointFixture {
            id,
            stats,
            tx_queue,
        }
    }

    fn endpoint_added(fx: &EndpointFixture, name: &str) -> EndpointEvent {
        endpoint_added_with_routable(fx, name, true)
    }

    fn endpoint_added_with_routable(
        fx: &EndpointFixture,
        name: &str,
        routable: bool,
    ) -> EndpointEvent {
        EndpointEvent::EndpointAdded {
            id: fx.id,
            name: name.to_string(),
            tx_queue: fx.tx_queue.clone(),
            stats: fx.stats.clone(),
            identity: IdentityFlags::default(),
            routable,
        }
    }

    fn peer_added(parent: EndpointId, fx: &EndpointFixture, name: &str) -> EndpointEvent {
        EndpointEvent::PeerAdded {
            parent_id: parent,
            child_id: fx.id,
            peer_addr: fake_addr(),
            name: name.to_string(),
            tx_queue: fx.tx_queue.clone(),
            stats: fx.stats.clone(),
            identity: IdentityFlags::default(),
        }
    }

    /// Make wiring with bounded channels appropriate for tests; returns
    /// the senders/receivers the test should drive plus the cancel token.
    fn make_wiring() -> (
        mpsc::Sender<RouterFrame>,
        mpsc::Sender<EndpointEvent>,
        mpsc::Receiver<StatsEvent>,
        RouterWiring,
    ) {
        let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(16);
        let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(16);
        let (stats_event_tx, stats_event_rx) = mpsc::channel::<StatsEvent>(16);
        let cancel = CancellationToken::new();
        (
            frame_tx,
            event_tx,
            stats_event_rx,
            RouterWiring {
                frame_rx,
                event_rx,
                stats_event_tx,
                cancel,
            },
        )
    }

    #[tokio::test]
    async fn endpoint_added_forwards_register() {
        let (_frame_tx, event_tx, mut stats_rx, wiring) = make_wiring();
        let cancel = wiring.cancel.clone();
        let task = tokio::spawn(run(wiring));
        let alloc = EndpointIdAllocator::new();
        let fx = make_endpoint(&alloc, true);

        event_tx
            .send(endpoint_added(&fx, "top"))
            .await
            .expect("send");
        let ev = tokio::time::timeout(Duration::from_secs(1), stats_rx.recv())
            .await
            .expect("stats event")
            .expect("channel");
        match ev {
            StatsEvent::Register { id, name, .. } => {
                assert_eq!(id, fx.id);
                assert_eq!(name, "top");
            }
            other => panic!("expected Register, got {other:?}"),
        }

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("router exit")
            .expect("router join");
    }

    #[tokio::test]
    async fn frame_routes_to_other_endpoints() {
        let (frame_tx, event_tx, _stats_rx, wiring) = make_wiring();
        let cancel = wiring.cancel.clone();
        let task = tokio::spawn(run(wiring));
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let dst = make_endpoint(&alloc, true);

        event_tx
            .send(endpoint_added(&src, "src"))
            .await
            .expect("src");
        event_tx
            .send(endpoint_added(&dst, "dst"))
            .await
            .expect("dst");

        // Give the router a moment to drain the events before sending.
        tokio::task::yield_now().await;

        let body = Bytes::from_static(b"hello");
        frame_tx
            .send(RouterFrame {
                endpoint_id: src.id,
                frame: body.clone(),
                header: header(7, 1, None),
            })
            .await
            .expect("send frame");

        // Wait for the frame to land on dst's queue.
        let popped = tokio::time::timeout(Duration::from_secs(1), dst.tx_queue.pop_or_wait())
            .await
            .expect("dst queue receive");
        assert_eq!(popped, body);
        // src must not receive its own frame.
        assert!(src.tx_queue.pop().is_none());

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("router exit")
            .expect("router join");
    }

    #[tokio::test]
    async fn frame_targeted_skips_destinations_without_learn() {
        let (frame_tx, event_tx, _stats_rx, wiring) = make_wiring();
        let cancel = wiring.cancel.clone();
        let task = tokio::spawn(run(wiring));
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let dst = make_endpoint(&alloc, true);

        event_tx
            .send(endpoint_added(&src, "src"))
            .await
            .expect("src");
        event_tx
            .send(endpoint_added(&dst, "dst"))
            .await
            .expect("dst");
        tokio::task::yield_now().await;

        // A targeted frame whose target identity has never been learned by
        // dst must not reach dst.
        frame_tx
            .send(RouterFrame {
                endpoint_id: src.id,
                frame: Bytes::from_static(b"targeted"),
                header: header(7, 1, Some(99)),
            })
            .await
            .expect("send");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(dst.tx_queue.pop().is_none());

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("router exit")
            .expect("router join");
    }

    #[tokio::test]
    async fn frame_updates_source_learn_entries_counter() {
        let (frame_tx, event_tx, _stats_rx, wiring) = make_wiring();
        let cancel = wiring.cancel.clone();
        let task = tokio::spawn(run(wiring));
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let dst = make_endpoint(&alloc, true);

        event_tx
            .send(endpoint_added(&src, "src"))
            .await
            .expect("src");
        event_tx
            .send(endpoint_added(&dst, "dst"))
            .await
            .expect("dst");
        tokio::task::yield_now().await;

        for compid in 1u8..=3u8 {
            frame_tx
                .send(RouterFrame {
                    endpoint_id: src.id,
                    frame: Bytes::from_static(b"x"),
                    header: header(7, compid, None),
                })
                .await
                .expect("send");
        }
        // Give the router time to process.
        for _ in 0..10 {
            if src.stats.learn_entries.load(Ordering::Relaxed) == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(src.stats.learn_entries.load(Ordering::Relaxed), 3);

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("router exit")
            .expect("router join");
    }

    #[tokio::test]
    async fn peer_removed_idle_writes_idle_state() {
        let (_frame_tx, event_tx, mut stats_rx, wiring) = make_wiring();
        let cancel = wiring.cancel.clone();
        let task = tokio::spawn(run(wiring));
        let alloc = EndpointIdAllocator::new();
        let parent = make_endpoint(&alloc, true);
        let child = make_endpoint(&alloc, false);

        event_tx
            .send(endpoint_added(&parent, "p"))
            .await
            .expect("parent");
        event_tx
            .send(peer_added(parent.id, &child, "c"))
            .await
            .expect("child added");
        // Drain Register events so we can assert on the next Finalize.
        let _ = stats_rx.recv().await; // parent register
        let _ = stats_rx.recv().await; // child register

        event_tx
            .send(EndpointEvent::PeerRemoved {
                parent_id: parent.id,
                child_id: child.id,
                peer_addr: fake_addr(),
                reason: PeerRemovalReason::Idle,
            })
            .await
            .expect("removed");
        let ev = tokio::time::timeout(Duration::from_secs(1), stats_rx.recv())
            .await
            .expect("finalize event")
            .expect("channel");
        match ev {
            StatsEvent::Finalize { id } => assert_eq!(id, child.id),
            other => panic!("expected Finalize, got {other:?}"),
        }
        assert_eq!(child.stats.load_state(), EndpointState::Idle);

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("router exit")
            .expect("router join");
    }

    #[tokio::test]
    async fn peer_removed_disconnected_writes_down_state() {
        let (_frame_tx, event_tx, mut stats_rx, wiring) = make_wiring();
        let cancel = wiring.cancel.clone();
        let task = tokio::spawn(run(wiring));
        let alloc = EndpointIdAllocator::new();
        let parent = make_endpoint(&alloc, true);
        let child = make_endpoint(&alloc, false);

        event_tx
            .send(endpoint_added(&parent, "p"))
            .await
            .expect("parent");
        event_tx
            .send(peer_added(parent.id, &child, "c"))
            .await
            .expect("child");
        let _ = stats_rx.recv().await;
        let _ = stats_rx.recv().await;

        event_tx
            .send(EndpointEvent::PeerRemoved {
                parent_id: parent.id,
                child_id: child.id,
                peer_addr: fake_addr(),
                reason: PeerRemovalReason::Disconnected,
            })
            .await
            .expect("removed");
        let _ = tokio::time::timeout(Duration::from_secs(1), stats_rx.recv())
            .await
            .expect("finalize")
            .expect("channel");
        assert_eq!(child.stats.load_state(), EndpointState::Down);

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("router exit")
            .expect("router join");
    }

    #[tokio::test]
    async fn shutdown_sweep_writes_down_and_finalizes_remaining() {
        let (_frame_tx, event_tx, mut stats_rx, wiring) = make_wiring();
        let cancel = wiring.cancel.clone();
        let task = tokio::spawn(run(wiring));
        let alloc = EndpointIdAllocator::new();
        let a = make_endpoint(&alloc, true);
        let b = make_endpoint(&alloc, true);

        event_tx.send(endpoint_added(&a, "a")).await.expect("a");
        event_tx.send(endpoint_added(&b, "b")).await.expect("b");
        let _ = stats_rx.recv().await; // a register
        let _ = stats_rx.recv().await; // b register

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("router exit")
            .expect("router join");

        let mut finalized = Vec::new();
        while let Ok(Some(ev)) =
            tokio::time::timeout(Duration::from_millis(100), stats_rx.recv()).await
        {
            if let StatsEvent::Finalize { id } = ev {
                finalized.push(id);
            }
        }
        finalized.sort();
        let mut expected = vec![a.id, b.id];
        expected.sort();
        assert_eq!(finalized, expected);
        assert_eq!(a.stats.load_state(), EndpointState::Down);
        assert_eq!(b.stats.load_state(), EndpointState::Down);
    }

    #[tokio::test]
    async fn non_routable_destination_does_not_receive_broadcast() {
        // CLAUDE.md: tcps/udps parent listeners get EndpointAdded for
        // stats visibility but their TxQueue has no consumer. Pushing to
        // it would inflate dropped_tx for no reason. The router must skip
        // them as routing destinations.
        let (frame_tx, event_tx, _stats_rx, wiring) = make_wiring();
        let cancel = wiring.cancel.clone();
        let task = tokio::spawn(run(wiring));
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let parent = make_endpoint(&alloc, true);

        event_tx
            .send(endpoint_added(&src, "src"))
            .await
            .expect("src");
        event_tx
            .send(endpoint_added_with_routable(&parent, "parent", false))
            .await
            .expect("parent");
        tokio::task::yield_now().await;

        // Broadcast frame from src.
        frame_tx
            .send(RouterFrame {
                endpoint_id: src.id,
                frame: Bytes::from_static(b"x"),
                header: header(7, 1, None),
            })
            .await
            .expect("send");
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            parent.tx_queue.pop().is_none(),
            "non-routable parent received a frame it shouldn't have"
        );
        assert_eq!(
            parent.stats.dropped_tx.load(Ordering::Relaxed),
            0,
            "non-routable parent's dropped_tx inflated"
        );

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("router exit")
            .expect("router join");
    }

    #[tokio::test]
    async fn frame_from_unknown_endpoint_is_dropped_safely() {
        let (frame_tx, _event_tx, _stats_rx, wiring) = make_wiring();
        let cancel = wiring.cancel.clone();
        let task = tokio::spawn(run(wiring));
        let alloc = EndpointIdAllocator::new();
        let ghost = alloc.alloc();

        frame_tx
            .send(RouterFrame {
                endpoint_id: ghost,
                frame: Bytes::from_static(b"x"),
                header: header(1, 1, None),
            })
            .await
            .expect("send");
        // Router should keep running.
        tokio::time::sleep(Duration::from_millis(50)).await;

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("router exit")
            .expect("router join");
    }
}
