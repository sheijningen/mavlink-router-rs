//! Central router task.
//!
//! Owns the per-endpoint learn tables and one registry keyed by
//! [`EndpointId`]. Each entry carries an optional [`RoutableState`] —
//! leaves and accepted sub-endpoints supply one; `tcps:` / `udps:` parent
//! listeners leave it `None` because they never receive frames themselves
//! (their children own real readers/writers). The hot frame-dispatch loop
//! short-circuits on `routable.is_none()` at one branch per registry slot,
//! so parents pay one predicted-not-taken branch and never reach the
//! per-destination decision. Receives lifecycle events and frames on two
//! bounded mpscs, applies the per-destination decision to each frame
//! against every other routing endpoint's learn-set, and pushes admitted
//! frames into each destination's [`TxQueue`].
//!
//! Behaviours implemented here:
//!
//! - **Registry maintenance** — handle `EndpointAdded` / `PeerAdded` /
//!   `PeerRemoved`, forwarding `StatsEvent::Register` / `Finalize` to the
//!   stats task in lockstep.
//! - **Source learn** — touch the source endpoint's learn-set on every
//!   inbound frame and keep the `learn_entries` stats counter in sync.
//! - **Routing decision** — for every *other* registered routing
//!   endpoint, run [`decide::decide`] and push the frame to that
//!   destination's `TxQueue` when admitted (cheap `Bytes::clone` Arc
//!   bumps; the queue handles drop-oldest overflow and increments
//!   `dropped_tx`). Out-filter rejections bump the destination's
//!   `out_filter_drops`; sniffer destinations bypass loop-prevent,
//!   out-filter, and target-match per CLAUDE.md.
//! - **Group registry maintenance** — endpoints declaring `?group=NAME`
//!   share a single learn-set hosted in [`GroupRegistry`]; the router
//!   joins / leaves on `EndpointAdded` / `PeerAdded` / `PeerRemoved` and
//!   routes source-learn writes and per-destination loop-prevent reads
//!   through the group's table when present.
//! - **Global dedup window** — when `dedup_ms > 0`, a single
//!   [`DedupWindow`] hashes each frame with xxh3-64 *before* learn and
//!   per-destination dispatch. A hit drops the frame and bumps the
//!   source endpoint's `dedup_drops` counter; this is what protects the
//!   redundant-uplink use case (LTE + RFD900 each deliver the same
//!   vehicle frame; the second arrival is suppressed).
//! - **Shutdown sweep** — on cancel, write `state = Down` for every
//!   remaining entry in the unified registry and forward `Finalize` so
//!   the stats task can drop its registry mirror.
//!
//! The per-source seq tracker that feeds `rx_lost_est` lives on the reader
//! side (CLAUDE.md ingress pipeline step 2); the router never touches
//! per-source seq state.

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
use crate::endpoint::identity_flags::{IdentityFlags, LEARN_CAPACITY};
use crate::endpoint::stats::{EndpointState, EndpointStats};
use crate::endpoint::tx_queue::TxQueue;
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
/// endpoint. Built from the [`Routable`] payload of `EndpointAdded` /
/// `PeerAdded` plus a fresh [`LearnTable`].
///
/// The `learn` field is *only* consulted when the endpoint has no group
/// (`identity.group.is_none()`). Group members read their effective learn
/// table from [`GroupRegistry`] keyed by `identity.group` per CLAUDE.md's
/// "Endpoint groups: members share *only* the learn-set; filters and
/// stats remain per-endpoint."
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
/// drive the entire data plane (lifecycle events + frames); the
/// `stats_event_tx` is fire-and-forget per CLAUDE.md ("the router does not
/// await a stats-task acknowledgement"). `dedup_ms == 0` (the default)
/// turns the global dedup window off — no hashing, no allocation, no
/// per-frame cost.
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

    async fn handle_event(&mut self, event: EndpointEvent) {
        match event {
            EndpointEvent::EndpointAdded {
                id,
                name,
                stats,
                routable,
            } => {
                let routable_state = routable.map(|payload| {
                    if let Some(group) = &payload.identity.group {
                        self.groups.join(group.clone(), LEARN_CAPACITY);
                    }
                    RoutableState {
                        tx_queue: payload.tx_queue,
                        identity: payload.identity,
                        learn: LearnTable::new(LEARN_CAPACITY),
                    }
                });
                let routable = routable_state.is_some();
                let entry = RegisteredEndpoint {
                    name: name.clone(),
                    stats: stats.clone(),
                    routable: routable_state,
                };
                debug!(%id, %name, routable = routable, "router: endpoint added");
                self.routing.insert(id, entry);
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
            EndpointEvent::PeerAdded {
                parent_id,
                child_id,
                peer_addr: _,
                name,
                stats,
                routable,
            } => {
                if let Some(group) = &routable.identity.group {
                    self.groups.join(group.clone(), LEARN_CAPACITY);
                }
                let entry = RegisteredEndpoint {
                    name: name.clone(),
                    stats: stats.clone(),
                    routable: Some(RoutableState {
                        tx_queue: routable.tx_queue,
                        identity: routable.identity,
                        learn: LearnTable::new(LEARN_CAPACITY),
                    }),
                };
                debug!(%child_id, %parent_id, %name, "router: peer added");
                self.routing.insert(child_id, entry);
                let _ = self
                    .stats_event_tx
                    .send(StatsEvent::Register {
                        id: child_id,
                        name,
                        stats,
                        routable: true,
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
                if let Some(entry) = self.routing.remove(&child_id) {
                    if let Some(routable_state) = &entry.routable
                        && let Some(group) = &routable_state.identity.group
                    {
                        self.groups.leave(group);
                    }
                    entry.stats.store_state(final_state);
                    debug!(%child_id, %parent_id, ?reason, ?final_state, "router: peer removed");
                }
                let _ = self
                    .stats_event_tx
                    .send(StatsEvent::Finalize { id: child_id })
                    .await;
            }
        }
    }

    fn handle_frame(&mut self, router_frame: RouterFrame) {
        // Field-disjoint split-borrow: `routing`, `groups`, and `dedup`
        // become independent `&mut` references so the same source-endpoint
        // lookup that gates dedup can ALSO touch the per-endpoint learn-set
        // when the source has no group — no second hashmap lookup, no
        // `expect` revalidating an invariant we just observed.
        let Router {
            routing,
            groups,
            dedup,
            stats_event_tx: _,
        } = self;
        let RouterFrame {
            endpoint_id: src_id,
            frame,
            header,
        } = router_frame;
        let now = Instant::now();

        // A frame from a registered-but-non-routable endpoint (a parent
        // listener) is a spawner bug — parents have no reader and cannot
        // emit a `RouterFrame` — but the same DEBUG-then-drop handles both
        // that and the genuine unknown-id case.
        let (src_stats, learn_len) = {
            let Some(src_entry) = routing.get_mut(&src_id) else {
                debug!(%src_id, "router: frame from unknown endpoint; dropped");
                return;
            };
            let src_stats = src_entry.stats.clone();
            let Some(src_routable) = src_entry.routable.as_mut() else {
                debug!(%src_id, "router: frame from non-routable endpoint; dropped");
                return;
            };

            // Dedup runs BEFORE learn and per-destination dispatch
            // (CLAUDE.md "Sniffer + dedup ordering": sniffer destinations
            // see the post-dedup frame set). Disabled when `dedup_ms == 0`
            // — `check_and_insert` is then a no-op `false`.
            if dedup.check_and_insert(&frame, now) {
                src_stats.dedup_drops.fetch_add(1, Ordering::Relaxed);
                trace!(%src_id, "router: dedup suppressed duplicate frame");
                return;
            }

            // Touch the source's effective learn-set (per-endpoint or
            // shared via a group). Always publish the current length to
            // the source's `learn_entries` — when the source's learn is a
            // group's table, other members may have inserted between our
            // touches, so the cheap unconditional store is the only way
            // every member's stats reflect the current group size.
            let learn_len = match &src_routable.identity.group {
                Some(name) => {
                    let Some(group) = groups.get_mut(name) else {
                        debug!(%src_id, ?name, "router: source group missing; dropped");
                        return;
                    };
                    group.learn.touch(header.source, now);
                    group.learn.len()
                }
                None => {
                    src_routable.learn.touch(header.source, now);
                    src_routable.learn.len()
                }
            };
            (src_stats, learn_len)
        };
        src_stats
            .learn_entries
            .store(learn_len as u64, Ordering::Relaxed);

        for (dest_id, dest_ep) in routing.iter() {
            if *dest_id == src_id {
                continue;
            }
            // Parent listeners (`routable == None`) are skipped here at
            // one branch per registry slot — the type-level distinction
            // lives on `RegisteredEndpoint.routable` rather than in a
            // separate map.
            let Some(dest_routable) = &dest_ep.routable else {
                continue;
            };
            let dest_learn = match &dest_routable.identity.group {
                Some(name) => {
                    // Single-task ownership: groups.leave is only called
                    // from handle_event, which is mutually exclusive with
                    // this iteration over routing. A registered group
                    // member always has its entry present.
                    let group = groups.get(name);
                    debug_assert!(group.is_some(), "group entry vanished mid-dispatch");
                    match group {
                        Some(group) => &group.learn,
                        None => continue,
                    }
                }
                None => &dest_routable.learn,
            };
            match decide_for_dest(&header, dest_learn, &dest_routable.identity) {
                Decision::Admit => {
                    dest_routable.tx_queue.push(frame.clone());
                }
                Decision::OutFilterBlocked => {
                    // Only out-filter rejections are credited to a counter
                    // — loop-prevent and target-mismatch are the everyday
                    // no-op rejections.
                    dest_ep
                        .stats
                        .out_filter_drops
                        .fetch_add(1, Ordering::Relaxed);
                    trace!(
                        %src_id,
                        dest_id = %dest_id,
                        msgid = header.msgid,
                        "router: out-filter blocked frame"
                    );
                }
                Decision::LoopBlocked | Decision::TargetMismatch => {}
            }
        }
    }

    async fn shutdown_sweep(&mut self) {
        // CLAUDE.md "On shutdown the router walks the unified registry,
        // writes state = Down". Sub-endpoints normally exit via
        // PeerRemoved before the sweep; any survivor here is one whose
        // removal event we didn't get to before cancel fired — Down is
        // still the safe terminal value.
        for (id, entry) in self.routing.drain() {
            entry.stats.store_state(EndpointState::Down);
            trace!(
                %id,
                name = %entry.name,
                routable = entry.routable.is_some(),
                "router: shutdown finalize"
            );
            let _ = self.stats_event_tx.send(StatsEvent::Finalize { id }).await;
        }
    }
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
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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
            peer_addr: fake_addr(),
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
                peer_addr: fake_addr(),
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
                peer_addr: fake_addr(),
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
        // CLAUDE.md: tcps/udps parent listeners register via
        // `EndpointAdded` with `routable = None`. They share the registry
        // with routing endpoints; the frame-dispatch loop short-circuits
        // on the `None` at one branch per slot. The parent's TxQueue is
        // held only by the test fixture (never handed to the router via
        // `Routable`), so it must receive zero frames despite live
        // broadcast traffic.
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
        // CLAUDE.md: "Members share *only* the learn-set; filters and
        // stats remain per-endpoint." Two group members with different
        // filters must each apply their own out-filter independently —
        // and the resulting `out_filter_drops` counter must be credited
        // only to the member that did the dropping, demonstrating
        // per-endpoint stats isolation under shared-group membership.
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
        // CLAUDE.md "redundant-uplink use case": same vehicle frame
        // arrives on two different routing endpoints (LTE + RFD900); the
        // global window suppresses the second arrival regardless of source.
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
        // dedup_drops is credited to the *source* of the suppressed
        // frame (rfd), not the lte leg that admitted the first copy.
        assert_eq!(rfd.stats.dedup_drops.load(Ordering::Relaxed), 1);
        assert_eq!(lte.stats.dedup_drops.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn sniffer_sees_post_dedup_traffic() {
        // CLAUDE.md "Sniffer + dedup ordering: ingress dedup runs before
        // the per-destination decision, so a sniffer sees the post-dedup
        // frame set." Duplicate suppression therefore hides the second
        // arrival from the sniffer too.
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
        // CLAUDE.md ingress pipeline order: dedup runs BEFORE learn. A
        // suppressed duplicate must not advance the source's learn-set.
        // The duplicate carries a *different* header (sysid 8 instead of
        // 7) on identical bytes — if dedup ran *after* learn, we'd see
        // learn_entries grow to 2; the assertion below pins ordering.
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
