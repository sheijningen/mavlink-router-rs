//! Central router task. Owns the unified endpoint registry, per-endpoint
//! learn tables, group registry, and global dedup window. Parent listeners
//! (`tcps:` / `udps:`) share the registry with routing endpoints but carry
//! `routable = None`; the per-frame dispatch loop short-circuits on that
//! at one branch per slot so parents are never iterated as destinations.

pub mod decide;
pub mod dedup;
pub mod group;
pub mod learn;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info_span, trace};

use crate::endpoint::EndpointId;
use crate::endpoint::events::{EndpointEvent, PeerRemovalReason, Routable, RouterFrame};
use crate::endpoint::identity_flags::{IdentityFlags, LEARN_CAPACITY};
use crate::endpoint::stats::{EndpointState, EndpointStats};
use crate::endpoint::tx_queue::TxQueue;
use crate::mavlink::frame::ParsedHeader;
use crate::stats::StatsEvent;

use decide::{Decision, decide as decide_for_dest};
use dedup::DedupWindow;
use group::GroupRegistry;
use learn::LearnTable;

/// One registered endpoint. Leaves (`tcpc:` / `udpc:` / `serial:`),
/// accepted `tcps:` children, and learned `udps:` peers all set `routable
/// = Some(_)`; `tcps:` / `udps:` parent listeners set `routable = None`
/// because they're never routing destinations. The router is the sole
/// writer of `routable.learn` and the registry slot; `stats` and
/// `routable.tx_queue` are `Arc`-shared with the endpoint's reader/writer
/// and may be observed without coordination.
struct RegisteredEndpoint {
    name: String,
    stats: Arc<EndpointStats>,
    /// `None` for parent listeners — they have no `TxQueue` and the hot
    /// frame-dispatch loop skips them at one branch. `Some(_)` carries the
    /// dispatch handles for every endpoint that can actually receive a
    /// frame.
    routable: Option<RoutableState>,
}

/// Per-endpoint dispatch state owned by the router for every routable
/// endpoint. The `learn` field is consulted only when the endpoint has no
/// group; group members read their effective learn table from
/// [`GroupRegistry`] keyed by `identity.group`.
struct RoutableState {
    tx_queue: TxQueue,
    /// Filter / sniffer / group flags. The router reads `filters`
    /// (out-filter evaluation), `sniffer` (decision override), and
    /// `group` (effective-learn lookup) on every dispatched frame.
    identity: IdentityFlags,
    /// Per-endpoint learn table. Unused when `identity.group.is_some()`.
    learn: LearnTable,
}

/// Bundle the spawner hands to the router task. The two `mpsc::Receiver`s
/// drive the data plane; `stats_event_tx` is fire-and-forget. `dedup_ms ==
/// 0` (the default) disables the global dedup window entirely — no
/// hashing, no allocation, no per-frame cost.
pub struct RouterWiring {
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    pub event_rx: mpsc::Receiver<EndpointEvent>,
    pub stats_event_tx: mpsc::Sender<StatsEvent>,
    pub cancel: CancellationToken,
    pub dedup_ms: u64,
    pub dedup_window_capacity: usize,
}

/// Mutable state owned by the router task: the unified endpoint registry,
/// the shared-learn-set side-table for `?group=`-tagged endpoints, the
/// global dedup window, and the fire-and-forget channel to the stats task.
/// Wrapped in a struct so `handle_event` / `handle_frame` / `shutdown_sweep`
/// become `&mut self` methods and field-disjoint borrows fall out
/// naturally (handle_frame uses this to do a single `routing.get_mut`
/// lookup per inbound frame instead of `get` + `get_mut`).
struct Router {
    routing: HashMap<EndpointId, RegisteredEndpoint>,
    groups: GroupRegistry,
    dedup: DedupWindow,
    stats_event_tx: mpsc::Sender<StatsEvent>,
}

impl Router {
    fn new(
        stats_event_tx: mpsc::Sender<StatsEvent>,
        dedup_ms: u64,
        dedup_window_capacity: usize,
    ) -> Self {
        Self {
            routing: HashMap::new(),
            groups: GroupRegistry::new(),
            dedup: DedupWindow::new(
                std::time::Duration::from_millis(dedup_ms),
                dedup_window_capacity,
            ),
            stats_event_tx,
        }
    }

    fn build_routable_state(&mut self, payload: Routable) -> RoutableState {
        if let Some(group) = &payload.identity.group {
            self.groups.join(group.clone(), LEARN_CAPACITY);
        }
        RoutableState {
            tx_queue: payload.tx_queue,
            identity: payload.identity,
            learn: LearnTable::new(LEARN_CAPACITY),
        }
    }

    async fn register_routable(
        &mut self,
        id: EndpointId,
        name: String,
        stats: Arc<EndpointStats>,
        routable_state: Option<RoutableState>,
    ) {
        let routable = routable_state.is_some();
        self.routing.insert(
            id,
            RegisteredEndpoint {
                name: name.clone(),
                stats: stats.clone(),
                routable: routable_state,
            },
        );
        let _ = self
            .stats_event_tx
            .send(StatsEvent::Register {
                id,
                name,
                stats,
                routable,
            })
            .await;
    }

    async fn handle_event(&mut self, event: EndpointEvent) {
        match event {
            EndpointEvent::EndpointAdded {
                id,
                name,
                stats,
                routable,
            } => {
                let routable_state = routable.map(|payload| self.build_routable_state(payload));
                debug!(%id, %name, routable = routable_state.is_some(), "endpoint added");
                self.register_routable(id, name, stats, routable_state)
                    .await;
            }
            EndpointEvent::PeerAdded {
                parent_id,
                child_id,
                name,
                stats,
                routable,
            } => {
                let routable_state = self.build_routable_state(routable);
                debug!(%child_id, %parent_id, %name, "peer added");
                self.register_routable(child_id, name, stats, Some(routable_state))
                    .await;
            }
            EndpointEvent::PeerRemoved {
                parent_id,
                child_id,
                reason,
            } => {
                let final_state = match reason {
                    PeerRemovalReason::Idle => EndpointState::Idle,
                    PeerRemovalReason::LruEvicted
                    | PeerRemovalReason::Disconnected
                    | PeerRemovalReason::ListenerShutdown => EndpointState::Down,
                };
                if let Some(entry) = self.routing.remove(&child_id) {
                    if let Some(routable_state) = &entry.routable
                        && let Some(group) = &routable_state.identity.group
                    {
                        self.groups.leave(group);
                    }
                    entry.stats.store_state(final_state);
                    debug!(%child_id, %parent_id, ?reason, ?final_state, "peer removed");
                }
                let _ = self
                    .stats_event_tx
                    .send(StatsEvent::Finalize { id: child_id })
                    .await;
            }
        }
    }

    fn handle_frame(&mut self, router_frame: RouterFrame) {
        let RouterFrame {
            endpoint_id: src_id,
            frame,
            header,
        } = router_frame;
        if !self.update_source_routing(src_id, &frame, &header, Instant::now()) {
            return;
        }
        self.dispatch_to_destinations(src_id, &frame, &header);
    }

    /// Validate the source endpoint, run dedup, then touch the source's
    /// effective learn-set and publish its current length to stats. Returns
    /// `false` when the frame is suppressed (unknown source, non-routable
    /// source, dedup hit, or missing group entry).
    fn update_source_routing(
        &mut self,
        src_id: EndpointId,
        frame: &Bytes,
        header: &ParsedHeader,
        now: Instant,
    ) -> bool {
        // Split-borrow: one `routing.get_mut` gates dedup AND touches the
        // per-endpoint learn-set, no second lookup or `expect` needed.
        let Router {
            routing,
            groups,
            dedup,
            stats_event_tx: _,
        } = self;

        let Some(src_entry) = routing.get_mut(&src_id) else {
            debug!(%src_id, "frame from unknown endpoint; dropped");
            return false;
        };
        let src_stats = src_entry.stats.clone();
        let Some(src_routable) = src_entry.routable.as_mut() else {
            debug!(%src_id, "frame from non-routable endpoint; dropped");
            return false;
        };

        // Dedup runs BEFORE learn so sniffer destinations see the
        // post-dedup frame set. No-op when `dedup_ms == 0`.
        if dedup.check_and_insert(frame, now) {
            src_stats.dedup_drops.fetch_add(1, Ordering::Relaxed);
            trace!(%src_id, "dedup suppressed duplicate frame");
            return false;
        }

        // Always publish learn-set size; group siblings may have inserted
        // between touches.
        let learn_len = match &src_routable.identity.group {
            Some(name) => {
                let Some(group) = groups.get_mut(name) else {
                    debug!(%src_id, ?name, "source group missing; dropped");
                    return false;
                };
                group.learn.touch(header.source, now);
                group.learn.len()
            }
            None => {
                src_routable.learn.touch(header.source, now);
                src_routable.learn.len()
            }
        };
        src_stats
            .learn_entries
            .store(learn_len as u64, Ordering::Relaxed);
        true
    }

    /// Per-destination decision: for every registered endpoint other than
    /// the source, look up the effective learn-set (per-endpoint or shared
    /// via the destination's group), run [`decide_for_dest`], and either
    /// push into the destination's TxQueue or credit `out_filter_drops`.
    /// Parent listeners (`routable == None`) are skipped at one branch.
    fn dispatch_to_destinations(&self, src_id: EndpointId, frame: &Bytes, header: &ParsedHeader) {
        for (dest_id, dest_ep) in self.routing.iter() {
            if *dest_id == src_id {
                continue;
            }
            let Some(dest_routable) = &dest_ep.routable else {
                continue;
            };
            let dest_learn = match &dest_routable.identity.group {
                Some(name) => {
                    // Single-task ownership: `groups.leave` only runs from
                    // `handle_event`, mutually exclusive with this loop.
                    let group = self.groups.get(name);
                    debug_assert!(group.is_some(), "group entry vanished mid-dispatch");
                    match group {
                        Some(group) => &group.learn,
                        None => continue,
                    }
                }
                None => &dest_routable.learn,
            };
            match decide_for_dest(header, dest_learn, &dest_routable.identity) {
                Decision::Admit => {
                    dest_routable.tx_queue.push(frame.clone());
                }
                Decision::OutFilterBlocked => {
                    // Only out-filter rejections get a counter — loop-prevent
                    // and target-mismatch are the everyday no-op rejections.
                    dest_ep
                        .stats
                        .out_filter_drops
                        .fetch_add(1, Ordering::Relaxed);
                    trace!(
                        %src_id,
                        dest_id = %dest_id,
                        msgid = header.msgid,
                        "out-filter blocked frame"
                    );
                }
                Decision::LoopBlocked | Decision::TargetMismatch => {}
            }
        }
    }

    async fn shutdown_sweep(&mut self) {
        // Sub-endpoints normally exit via PeerRemoved; any survivor here
        // missed its removal event before cancel — Down is the safe
        // terminal value.
        for (id, entry) in self.routing.drain() {
            entry.stats.store_state(EndpointState::Down);
            trace!(
                %id,
                name = %entry.name,
                routable = entry.routable.is_some(),
                "shutdown finalize"
            );
            let _ = self.stats_event_tx.send(StatsEvent::Finalize { id }).await;
        }
    }
}

/// Run the router task until the cancellation token fires. Biased select
/// over cancel, then events, then frames so every available lifecycle
/// event drains before the next frame is taken — the registry is always
/// at least as caught up as the frame stream.
pub async fn run(wiring: RouterWiring) {
    let span = info_span!("router");
    run_inner(wiring).instrument(span).await
}

async fn run_inner(wiring: RouterWiring) {
    let RouterWiring {
        mut frame_rx,
        mut event_rx,
        stats_event_tx,
        cancel,
        dedup_ms,
        dedup_window_capacity,
    } = wiring;

    let mut router = Router::new(stats_event_tx, dedup_ms, dedup_window_capacity);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            Some(event) = event_rx.recv() => {
                router.handle_event(event).await;
            }
            Some(frame) = frame_rx.recv() => {
                router.handle_frame(frame);
            }
            else => break,
        }
    }

    // Drain any lifecycle events the parent listeners managed to send
    // between cancel firing and the router's drop of `event_rx`. Catches
    // in-flight `PeerRemoved`s so the stats task's mirror sees the right
    // final-state for every sub-endpoint that was torn down during the
    // drain window.
    while let Ok(event) = event_rx.try_recv() {
        router.handle_event(event).await;
    }

    router.shutdown_sweep().await;
}

#[cfg(test)]
mod tests {
    //! Router internals exercised through direct `&mut self` method calls
    //! on a privately-constructed [`Router`]. No `tokio::spawn`, no
    //! `tokio::time::sleep` polling, no cancellation-token plumbing —
    //! every assertion runs against state that's deterministic the
    //! instant `handle_event` / `handle_frame` / `shutdown_sweep` returns.
    //! Full-pipeline coverage of the select loop, biased ordering, and
    //! cancel-driven drain lives in the integration tests under
    //! `tests/spawner_e2e.rs`, `tests/shutdown.rs`, and `tests/shutdown_soak.rs`.

    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use crate::endpoint::events::{PeerRemovalReason, Routable};
    use crate::endpoint::filters::{Filters, MsgIdRange};
    use crate::mavlink::frame::{NodeId, ParsedHeader, Version};
    use bytes::Bytes;

    fn header(sysid: u8, compid: u8, target_system: Option<u8>) -> ParsedHeader {
        ParsedHeader {
            version: Version::V2,
            source: NodeId::new(sysid, compid),
            msgid: 0,
            seq: 0,
            payload_len: 0,
            target_system,
            target_component: None,
        }
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
        endpoint_added_with_identity(fx, name, IdentityFlags::default())
    }

    fn endpoint_added_with_identity(
        fx: &EndpointFixture,
        name: &str,
        identity: IdentityFlags,
    ) -> EndpointEvent {
        EndpointEvent::EndpointAdded {
            id: fx.id,
            name: name.to_string(),
            stats: fx.stats.clone(),
            routable: Some(Routable {
                tx_queue: fx.tx_queue.clone(),
                identity,
            }),
        }
    }

    fn parent_listener_added(fx: &EndpointFixture, name: &str) -> EndpointEvent {
        EndpointEvent::EndpointAdded {
            id: fx.id,
            name: name.to_string(),
            stats: fx.stats.clone(),
            routable: None,
        }
    }

    fn peer_added(parent: EndpointId, fx: &EndpointFixture, name: &str) -> EndpointEvent {
        EndpointEvent::PeerAdded {
            parent_id: parent,
            child_id: fx.id,
            name: name.to_string(),
            stats: fx.stats.clone(),
            routable: Routable {
                tx_queue: fx.tx_queue.clone(),
                identity: IdentityFlags::default(),
            },
        }
    }

    /// Build a `Router` against an mpsc `StatsEvent` receiver sized large
    /// enough that every test-emitted event lands without blocking. Tests
    /// inspect the receiver via [`drain_stats`] after the relevant
    /// `handle_*` call.
    fn make_router(
        dedup_ms: u64,
        dedup_window_capacity: usize,
    ) -> (Router, mpsc::Receiver<StatsEvent>) {
        let (stats_event_tx, stats_event_rx) = mpsc::channel::<StatsEvent>(64);
        let router = Router::new(stats_event_tx, dedup_ms, dedup_window_capacity);
        (router, stats_event_rx)
    }

    /// Pop every currently-queued `StatsEvent` off the receiver. Safe to
    /// call after any `handle_event` / `shutdown_sweep` because those
    /// methods complete the `send().await` synchronously (capacity 64
    /// always has room in a unit test).
    fn drain_stats(rx: &mut mpsc::Receiver<StatsEvent>) -> Vec<StatsEvent> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    /// Count `Register` entries in a drained list. Tests that exercise
    /// `PeerRemoved` use this to pin the upstream `PeerAdded → Register`
    /// emission — the only place that path is currently covered.
    fn count_registers(events: &[StatsEvent]) -> usize {
        events
            .iter()
            .filter(|ev| matches!(ev, StatsEvent::Register { .. }))
            .count()
    }

    /// Find the unique `Finalize { id }` in a drained list. Panics if the
    /// drain produced zero or more than one — callers know the count
    /// from the number of `PeerRemoved` they fired.
    fn expect_one_finalize(events: &[StatsEvent]) -> EndpointId {
        let finalizes: Vec<EndpointId> = events
            .iter()
            .filter_map(|ev| match ev {
                StatsEvent::Finalize { id } => Some(*id),
                _ => None,
            })
            .collect();
        assert_eq!(
            finalizes.len(),
            1,
            "expected one Finalize, got {finalizes:?}"
        );
        finalizes[0]
    }

    #[tokio::test]
    async fn endpoint_added_forwards_register() {
        let (mut router, mut stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let fx = make_endpoint(&alloc, true);

        router.handle_event(endpoint_added(&fx, "top")).await;

        match stats_rx.try_recv().expect("Register fired") {
            StatsEvent::Register { id, name, .. } => {
                assert_eq!(id, fx.id);
                assert_eq!(name, "top");
            }
            other => panic!("expected Register, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn frame_routes_to_other_endpoints() {
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let dst = make_endpoint(&alloc, true);
        router.handle_event(endpoint_added(&src, "src")).await;
        router.handle_event(endpoint_added(&dst, "dst")).await;

        let body = Bytes::from_static(b"hello");
        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: body.clone(),
            header: header(7, 1, None),
        });

        assert_eq!(dst.tx_queue.pop(), Some(body));
        assert!(
            src.tx_queue.pop().is_none(),
            "src must not receive its own frame"
        );
    }

    #[tokio::test]
    async fn frame_targeted_skips_destinations_without_learn() {
        // A targeted frame whose target identity has never been learned
        // by dst must not reach dst.
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let dst = make_endpoint(&alloc, true);
        router.handle_event(endpoint_added(&src, "src")).await;
        router.handle_event(endpoint_added(&dst, "dst")).await;

        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: Bytes::from_static(b"targeted"),
            header: header(7, 1, Some(99)),
        });
        assert!(dst.tx_queue.pop().is_none());
    }

    #[tokio::test]
    async fn frame_updates_source_learn_entries_counter() {
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let dst = make_endpoint(&alloc, true);
        router.handle_event(endpoint_added(&src, "src")).await;
        router.handle_event(endpoint_added(&dst, "dst")).await;

        for compid in 1u8..=3u8 {
            router.handle_frame(RouterFrame {
                endpoint_id: src.id,
                frame: Bytes::from_static(b"x"),
                header: header(7, compid, None),
            });
        }
        assert_eq!(src.stats.learn_entries.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn peer_removed_idle_writes_idle_state() {
        let (mut router, mut stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let parent = make_endpoint(&alloc, true);
        let child = make_endpoint(&alloc, false);
        router.handle_event(endpoint_added(&parent, "p")).await;
        router
            .handle_event(peer_added(parent.id, &child, "c"))
            .await;
        router
            .handle_event(EndpointEvent::PeerRemoved {
                parent_id: parent.id,
                child_id: child.id,
                reason: PeerRemovalReason::Idle,
            })
            .await;

        let events = drain_stats(&mut stats_rx);
        // EndpointAdded(parent) and PeerAdded(child) each emit a Register;
        // the PeerRemoved emits the Finalize. Asserting the count pins
        // the PeerAdded → Register emission, which has no dedicated test
        // of its own.
        assert_eq!(count_registers(&events), 2);
        assert_eq!(expect_one_finalize(&events), child.id);
        assert_eq!(child.stats.load_state(), EndpointState::Idle);
    }

    #[tokio::test]
    async fn peer_removed_disconnected_writes_down_state() {
        let (mut router, mut stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let parent = make_endpoint(&alloc, true);
        let child = make_endpoint(&alloc, false);
        router.handle_event(endpoint_added(&parent, "p")).await;
        router
            .handle_event(peer_added(parent.id, &child, "c"))
            .await;
        router
            .handle_event(EndpointEvent::PeerRemoved {
                parent_id: parent.id,
                child_id: child.id,
                reason: PeerRemovalReason::Disconnected,
            })
            .await;

        let events = drain_stats(&mut stats_rx);
        assert_eq!(count_registers(&events), 2);
        assert_eq!(expect_one_finalize(&events), child.id);
        assert_eq!(child.stats.load_state(), EndpointState::Down);
    }

    #[tokio::test]
    async fn shutdown_sweep_writes_down_and_finalizes_remaining() {
        let (mut router, mut stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let first = make_endpoint(&alloc, true);
        let second = make_endpoint(&alloc, true);
        router.handle_event(endpoint_added(&first, "a")).await;
        router.handle_event(endpoint_added(&second, "b")).await;

        router.shutdown_sweep().await;

        let mut finalized: Vec<_> = drain_stats(&mut stats_rx)
            .into_iter()
            .filter_map(|ev| match ev {
                StatsEvent::Finalize { id } => Some(id),
                _ => None,
            })
            .collect();
        finalized.sort();
        let mut expected = vec![first.id, second.id];
        expected.sort();
        assert_eq!(finalized, expected);
        assert_eq!(first.stats.load_state(), EndpointState::Down);
        assert_eq!(second.stats.load_state(), EndpointState::Down);
    }

    #[tokio::test]
    async fn parent_listener_does_not_receive_broadcast() {
        // Parents register with `routable = None`; the dispatch loop
        // short-circuits at one branch. The parent's TxQueue is held only
        // by the fixture, so it must receive zero frames.
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let parent = make_endpoint(&alloc, true);
        router.handle_event(endpoint_added(&src, "src")).await;
        router
            .handle_event(parent_listener_added(&parent, "parent"))
            .await;

        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: Bytes::from_static(b"x"),
            header: header(7, 1, None),
        });

        assert!(
            parent.tx_queue.pop().is_none(),
            "parent listener received a frame it shouldn't have"
        );
        assert_eq!(
            parent.stats.dropped_tx.load(Ordering::Relaxed),
            0,
            "parent listener dropped_tx inflated"
        );
    }

    #[tokio::test]
    async fn out_filter_blocks_destination_and_increments_drop_counter() {
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let dst = make_endpoint(&alloc, true);

        let block_msgid_0 = IdentityFlags {
            filters: Filters {
                block_msgid_out: vec![MsgIdRange::single(0)],
                ..Filters::default()
            },
            ..IdentityFlags::default()
        };
        router.handle_event(endpoint_added(&src, "src")).await;
        router
            .handle_event(endpoint_added_with_identity(&dst, "dst", block_msgid_0))
            .await;

        // msgid 0 broadcast frame; dst's out-filter blocks msgid 0.
        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: Bytes::from_static(b"x"),
            header: header(7, 1, None),
        });

        assert_eq!(dst.stats.out_filter_drops.load(Ordering::Relaxed), 1);
        assert!(dst.tx_queue.pop().is_none(), "blocked frame must not queue");
    }

    #[tokio::test]
    async fn sniffer_destination_bypasses_loop_prevention_and_target_match() {
        // Two destinations, one sniffer one plain. We pre-load plain's
        // learn-set with (7, 1) by feeding a frame whose source endpoint
        // *is* plain — that's the only way `plain.learn.contains(7, 1)`
        // ever becomes true (endpoints learn from their own ingress).
        // After that, a frame from `src` whose `(srcsys, srccomp) = (7, 1)`
        // tickles loop-prevention on plain but the sniffer tap still
        // admits it.
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let tap = make_endpoint(&alloc, true);
        let plain = make_endpoint(&alloc, true);

        let sniffer = IdentityFlags {
            sniffer: true,
            ..IdentityFlags::default()
        };
        router.handle_event(endpoint_added(&src, "src")).await;
        router
            .handle_event(endpoint_added_with_identity(&tap, "tap", sniffer))
            .await;
        router.handle_event(endpoint_added(&plain, "plain")).await;

        // Pre-load plain.learn by sending a frame *from* plain with
        // source identity (7, 1). The router touches plain's learn-set;
        // src and tap admit it as a normal broadcast.
        router.handle_frame(RouterFrame {
            endpoint_id: plain.id,
            frame: Bytes::from_static(b"prime"),
            header: header(7, 1, None),
        });
        let _ = src.tx_queue.pop();
        let _ = tap.tx_queue.pop();
        assert_eq!(plain.stats.learn_entries.load(Ordering::Relaxed), 1);

        // Frame from src(7, 1) — plain rejects on loop-prevention, tap
        // admits via sniffer override.
        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: Bytes::from_static(b"echo"),
            header: header(7, 1, None),
        });
        assert_eq!(tap.tx_queue.pop(), Some(Bytes::from_static(b"echo")));
        assert!(
            plain.tx_queue.pop().is_none(),
            "loop prevention should have stopped plain from receiving"
        );

        // Targeted frame at sysid 99 (never learned). Plain rejects on
        // target-mismatch; tap admits because sniffer bypasses target-match.
        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: Bytes::from_static(b"targeted"),
            header: header(7, 1, Some(99)),
        });
        assert_eq!(tap.tx_queue.pop(), Some(Bytes::from_static(b"targeted")));
        assert!(
            plain.tx_queue.pop().is_none(),
            "target-mismatch should have stopped plain from receiving"
        );
    }

    #[tokio::test]
    async fn sniffer_destination_admits_through_out_filter_block() {
        // A frame matching the sniffer's own block_msgid_out still
        // reaches it — sniffer override skips out-filter entirely.
        // out_filter_drops must stay 0.
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let tap = make_endpoint(&alloc, true);

        let sniffer_with_block = IdentityFlags {
            sniffer: true,
            filters: Filters {
                block_msgid_out: vec![MsgIdRange::single(0)],
                ..Filters::default()
            },
            ..IdentityFlags::default()
        };
        router.handle_event(endpoint_added(&src, "src")).await;
        router
            .handle_event(endpoint_added_with_identity(
                &tap,
                "tap",
                sniffer_with_block,
            ))
            .await;

        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: Bytes::from_static(b"x"),
            header: header(7, 1, None),
        });

        assert!(
            tap.tx_queue.pop().is_some(),
            "tap received frame despite block_msgid_out"
        );
        assert_eq!(
            tap.stats.out_filter_drops.load(Ordering::Relaxed),
            0,
            "sniffer admit must not bump out_filter_drops"
        );
    }

    #[tokio::test]
    async fn group_members_share_learn_set() {
        // Two endpoints in the same group: when one of them sees an
        // inbound frame from identity (7, 1), the other's per-destination
        // decision should also treat (7, 1) as locally known — so a
        // subsequent frame from a *third* endpoint whose source is (7, 1)
        // is loop-blocked at *both* group members (redundant uplinks must
        // not silence each other).
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let lte = make_endpoint(&alloc, true);
        let rfd = make_endpoint(&alloc, true);
        let gcs = make_endpoint(&alloc, true);

        let in_group = IdentityFlags {
            group: Some(Arc::<str>::from("uplink")),
            ..IdentityFlags::default()
        };
        router
            .handle_event(endpoint_added_with_identity(&lte, "lte", in_group.clone()))
            .await;
        router
            .handle_event(endpoint_added_with_identity(&rfd, "rfd", in_group.clone()))
            .await;
        router.handle_event(endpoint_added(&gcs, "gcs")).await;

        // A frame arrives on lte from vehicle sysid 7. The group's
        // learn-set gains (7, 1); both lte and rfd reflect it.
        router.handle_frame(RouterFrame {
            endpoint_id: lte.id,
            frame: Bytes::from_static(b"telemetry"),
            header: header(7, 1, None),
        });
        // gcs is not in the group; its learn is empty, so the frame
        // admits there.
        assert!(gcs.tx_queue.pop().is_some(), "gcs got frame #1");
        // The source touch happened *before* the dispatch loop, so by
        // the time rfd's decision runs, the group learn-set already
        // contains (7, 1) — rfd is loop-blocked.
        assert!(
            rfd.tx_queue.pop().is_none(),
            "rfd must be loop-blocked because the group's learn-set already contains the source"
        );

        // learn_entries on lte reflects the group's size (1). rfd never
        // sourced a frame, so its own learn_entries hasn't been
        // republished yet.
        assert_eq!(
            lte.stats.learn_entries.load(Ordering::Relaxed),
            1,
            "lte saw the touch and stored 1"
        );

        // Send a frame *from* rfd with the same source identity (7, 1);
        // the group learn-set's touch is a refresh (no insert), but
        // rfd's learn_entries publishes the current group size.
        router.handle_frame(RouterFrame {
            endpoint_id: rfd.id,
            frame: Bytes::from_static(b"echo"),
            header: header(7, 1, None),
        });
        assert_eq!(rfd.stats.learn_entries.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn group_members_do_not_share_filters_or_stats() {
        // Group members share *only* the learn-set; filters apply
        // per-member and `out_filter_drops` is credited only to the
        // member that dropped.
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let strict = make_endpoint(&alloc, true);
        let permissive = make_endpoint(&alloc, true);

        let strict_id = IdentityFlags {
            group: Some(Arc::<str>::from("downlink")),
            filters: Filters {
                block_msgid_out: vec![MsgIdRange::single(0)],
                ..Filters::default()
            },
            ..IdentityFlags::default()
        };
        let permissive_id = IdentityFlags {
            group: Some(Arc::<str>::from("downlink")),
            ..IdentityFlags::default()
        };
        router.handle_event(endpoint_added(&src, "src")).await;
        router
            .handle_event(endpoint_added_with_identity(&strict, "strict", strict_id))
            .await;
        router
            .handle_event(endpoint_added_with_identity(
                &permissive,
                "permissive",
                permissive_id,
            ))
            .await;

        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: Bytes::from_static(b"x"),
            header: header(7, 1, None),
        });

        assert!(
            permissive.tx_queue.pop().is_some(),
            "permissive admits frame"
        );
        assert!(
            strict.tx_queue.pop().is_none(),
            "strict must drop on its own block_msgid_out, not share permissive's filter"
        );
        assert_eq!(strict.stats.out_filter_drops.load(Ordering::Relaxed), 1);
        // permissive's counter stays clean — same group, separate stats.
        assert_eq!(permissive.stats.out_filter_drops.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn dedup_disabled_admits_duplicate_frames() {
        // dedup_ms == 0 (default) — duplicate frames are admitted, the
        // dedup_drops counter stays at zero.
        let (mut router, _stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let dst = make_endpoint(&alloc, true);
        router.handle_event(endpoint_added(&src, "src")).await;
        router.handle_event(endpoint_added(&dst, "dst")).await;

        for _ in 0..3 {
            router.handle_frame(RouterFrame {
                endpoint_id: src.id,
                frame: Bytes::from_static(b"identical"),
                header: header(7, 1, None),
            });
        }
        for _ in 0..3 {
            assert!(dst.tx_queue.pop().is_some(), "dst got duplicate");
        }
        assert_eq!(src.stats.dedup_drops.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn dedup_suppresses_redundant_uplink_at_router_ingress() {
        // Redundant uplink: the global window must suppress the second
        // copy regardless of which leg it arrived on.
        let (mut router, _stats_rx) = make_router(500, 16);
        let alloc = EndpointIdAllocator::new();
        let lte = make_endpoint(&alloc, true);
        let rfd = make_endpoint(&alloc, true);
        let gcs = make_endpoint(&alloc, true);
        router.handle_event(endpoint_added(&lte, "lte")).await;
        router.handle_event(endpoint_added(&rfd, "rfd")).await;
        router.handle_event(endpoint_added(&gcs, "gcs")).await;

        // First copy arrives on the LTE leg.
        router.handle_frame(RouterFrame {
            endpoint_id: lte.id,
            frame: Bytes::from_static(b"vehicle-telemetry"),
            header: header(7, 1, None),
        });
        assert!(gcs.tx_queue.pop().is_some(), "gcs got #1");

        // Identical second copy arrives on the RFD leg — must be
        // suppressed at the dedup window and never reach gcs.
        router.handle_frame(RouterFrame {
            endpoint_id: rfd.id,
            frame: Bytes::from_static(b"vehicle-telemetry"),
            header: header(7, 1, None),
        });
        assert!(
            gcs.tx_queue.pop().is_none(),
            "duplicate frame must not reach gcs"
        );
        // `dedup_drops` is credited to the suppressed frame's source.
        assert_eq!(rfd.stats.dedup_drops.load(Ordering::Relaxed), 1);
        assert_eq!(lte.stats.dedup_drops.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn sniffer_sees_post_dedup_traffic() {
        // Ingress dedup runs before the per-destination decision, so
        // duplicate suppression also hides the second arrival from a
        // sniffer destination.
        let (mut router, _stats_rx) = make_router(500, 16);
        let alloc = EndpointIdAllocator::new();
        let lte = make_endpoint(&alloc, true);
        let rfd = make_endpoint(&alloc, true);
        let tap = make_endpoint(&alloc, true);

        let sniffer = IdentityFlags {
            sniffer: true,
            ..IdentityFlags::default()
        };
        router.handle_event(endpoint_added(&lte, "lte")).await;
        router.handle_event(endpoint_added(&rfd, "rfd")).await;
        router
            .handle_event(endpoint_added_with_identity(&tap, "tap", sniffer))
            .await;

        router.handle_frame(RouterFrame {
            endpoint_id: lte.id,
            frame: Bytes::from_static(b"telem"),
            header: header(7, 1, None),
        });
        assert!(tap.tx_queue.pop().is_some(), "tap got #1");

        router.handle_frame(RouterFrame {
            endpoint_id: rfd.id,
            frame: Bytes::from_static(b"telem"),
            header: header(7, 1, None),
        });
        assert!(
            tap.tx_queue.pop().is_none(),
            "sniffer must not see the post-dedup duplicate"
        );
    }

    #[tokio::test]
    async fn dedup_does_not_run_learn_on_suppressed_frame() {
        // Dedup runs BEFORE learn: a suppressed duplicate must not advance
        // the source's learn-set. The duplicate carries a *different*
        // header on identical bytes — if learn ran first, `learn_entries`
        // would grow to 2.
        let (mut router, _stats_rx) = make_router(500, 16);
        let alloc = EndpointIdAllocator::new();
        let src = make_endpoint(&alloc, true);
        let dst = make_endpoint(&alloc, true);
        router.handle_event(endpoint_added(&src, "src")).await;
        router.handle_event(endpoint_added(&dst, "dst")).await;

        // First arrival learns (7, 1) on src; learn_entries → 1.
        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: Bytes::from_static(b"telem"),
            header: header(7, 1, None),
        });
        assert!(dst.tx_queue.pop().is_some());
        assert_eq!(src.stats.learn_entries.load(Ordering::Relaxed), 1);

        // Duplicate bytes, different header (sysid 8). Dedup hashes the
        // bytes, so this is a hit — the (8, 1) source must not enter
        // learn.
        router.handle_frame(RouterFrame {
            endpoint_id: src.id,
            frame: Bytes::from_static(b"telem"),
            header: header(8, 1, None),
        });
        assert_eq!(
            src.stats.learn_entries.load(Ordering::Relaxed),
            1,
            "suppressed frame must not advance learn_entries"
        );
        assert_eq!(src.stats.dedup_drops.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn frame_from_unknown_endpoint_is_dropped_safely() {
        // Frame stamped with an EndpointId that was never registered:
        // the router takes the unknown-endpoint debug path and returns
        // without touching any state. We pin two observable invariants:
        // (1) a registered sibling's TxQueue stays empty (the ghost
        // frame was not forwarded to it), and (2) no stats event was
        // emitted for the ghost id (no Register / Finalize).
        let (mut router, mut stats_rx) = make_router(0, 16);
        let alloc = EndpointIdAllocator::new();
        let dst = make_endpoint(&alloc, true);
        let ghost = alloc.alloc();
        router.handle_event(endpoint_added(&dst, "dst")).await;
        // Drain the Register from dst so the post-handle_frame check
        // sees only events the ghost frame could have produced.
        let _ = drain_stats(&mut stats_rx);

        router.handle_frame(RouterFrame {
            endpoint_id: ghost,
            frame: Bytes::from_static(b"x"),
            header: header(1, 1, None),
        });

        assert!(
            dst.tx_queue.pop().is_none(),
            "dst must not receive a frame stamped with an unknown source id"
        );
        assert!(
            drain_stats(&mut stats_rx).is_empty(),
            "unknown-endpoint path must not emit stats events"
        );
    }
}
