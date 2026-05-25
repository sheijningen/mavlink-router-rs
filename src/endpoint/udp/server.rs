use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

use super::super::EndpointId;
use super::super::backoff::{Backoff, BindOutcome, bind_with_backoff};
use super::super::defaults::{
    DEFAULT_RECONNECT_INITIAL_MS, DEFAULT_RECONNECT_MAX_MS, DEFAULT_TX_QUEUE_FRAMES,
};
use super::super::events::{EndpointEvent, PeerRemovalReason, Routable};
use super::super::identity_flags::{IdentityFlags, SEQ_TRACKER_CAPACITY};
use super::super::peer_endpoint_name;
use super::super::seq_tracker::SeqTracker;
use super::super::session::SessionCtx;
use super::super::socket::bind_udp_dual_stack;
use super::super::spec::UdpServerEndpoint;
use super::super::stats::{EndpointState, EndpointStats, FramerCounters};
use super::super::tx_queue::TxQueue;
use super::super::wiring::ServerWiring;
use super::MAX_DATAGRAM_BYTES;
use crate::mavlink::framer::Framer;

const DEFAULT_IDLE_SECS: u64 = 60;

/// Per-listener cap on simultaneously-tracked peers (LRU-evicted by
/// last-seen). Hardcoded at the user-facing layer — 256 is well above any
/// realistic single-listener fleet size; deployments that need more peers
/// should split across listeners or processes rather than tune the cap.
/// Each admitted peer holds a `TxQueue` (~4KB at the default queue depth)
/// and a writer task. Exposed on [`UdpServerSpec`] so tests can shrink it
/// to exercise the eviction branch without 256 dummy peers.
pub const DEFAULT_PEER_CAPACITY: usize = 256;

/// Default `idle_secs` lower bound — 0 would reap every peer on the very
/// next reaper tick.
pub const MIN_IDLE_SECS: u64 = 1;

/// Default `idle_secs` upper bound (24 hours). Anything beyond a day means
/// "never reap"; if that's intentional an operator should reconsider the
/// retention model rather than push the knob through its sane range.
pub const MAX_IDLE_SECS: u64 = 86_400;
const REAP_INTERVAL: Duration = Duration::from_secs(1);

/// Per-peer state the listener task carries between packets: the child
/// routing endpoint's identity, its framer, last-seen timestamp for the LRU
/// and idle-reap policies, and the handles needed to shut down its writer.
struct PeerEntry {
    child_id: EndpointId,
    framer: Framer,
    last_seen: Instant,
    framer_counters: FramerCounters,
    seq_tracker: SeqTracker,
    stats: Arc<EndpointStats>,
    writer_cancel: CancellationToken,
}

/// Inputs that distinguish one `udps:` listener from another: where to bind,
/// what to call it, and the peer idle-reap threshold. The `reconnect_*_ms`
/// fields are always the hardcoded `tcpc:` curve (CLAUDE.md "Hardcoded
/// plumbing knobs"); the field stays on the Spec so bind-retry tests can
/// shrink the curve. `peer_capacity` stays mutable for the same reason —
/// production defaults it to [`DEFAULT_PEER_CAPACITY`], eviction tests
/// shrink it. `identity` carries the filter / sniffer / group bundle —
/// inherited by every learned peer at admission time (CLAUDE.md "Sub-
/// endpoints inherit their parent's `IdentityFlags` by clone at spawn
/// time"); the per-peer reader applies the in-filter snapshot, the router
/// applies out-filter / sniffer / group from the same bundle.
pub struct UdpServerSpec {
    pub listen_addr: SocketAddr,
    pub parent_id: EndpointId,
    pub parent_name: String,
    pub idle_secs: u64,
    pub peer_capacity: usize,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub identity: IdentityFlags,
}

impl UdpServerSpec {
    /// Build a runtime `UdpServerSpec` from the parsed `UdpServerEndpoint`,
    /// stamping the hardcoded reconnect curve and peer capacity. The spawner
    /// supplies `parent_id` and `parent_name` because the parser doesn't
    /// allocate IDs.
    pub fn from_endpoint(
        ep: UdpServerEndpoint,
        parent_id: EndpointId,
        parent_name: String,
    ) -> Self {
        Self {
            listen_addr: ep.bind_addr,
            parent_id,
            parent_name,
            idle_secs: ep.idle_secs.unwrap_or(DEFAULT_IDLE_SECS),
            peer_capacity: DEFAULT_PEER_CAPACITY,
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
            identity: ep.identity,
        }
    }
}

/// Run a `udps:` listener until the cancellation token fires. Binding is
/// retried with the shared capped-exp backoff (CLAUDE.md "Initial bind/dial
/// failure path"), so a port collision at startup logs at WARN and the
/// listener attaches as soon as the port frees. Once bound, loops over
/// `recv_from`, the idle reaper, and cancellation. Each learned peer becomes
/// a sub-routing endpoint announced via `event_tx` with its own TxQueue and
/// writer task.
pub async fn run(spec: UdpServerSpec, wiring: ServerWiring) {
    let span = info_span!("udps", name = %spec.parent_name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: UdpServerSpec, wiring: ServerWiring) {
    let mut backoff = Backoff::new(spec.reconnect_initial_ms, spec.reconnect_max_ms);

    let socket = match bind_with_backoff(&wiring.cancel, &mut backoff, spec.listen_addr, || {
        bind_udp_dual_stack(spec.listen_addr)
    })
    .await
    {
        BindOutcome::Bound(s) => Arc::new(s),
        BindOutcome::Cancelled => return,
    };
    wiring.stats.store_state(EndpointState::Connected);
    let bound_addr = socket.local_addr().unwrap_or(spec.listen_addr);
    info!(%bound_addr, parent_id = %spec.parent_id, "listening");

    let mut peers: HashMap<SocketAddr, PeerEntry> = HashMap::new();
    let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
    let mut writer_tasks: JoinSet<()> = JoinSet::new();

    let mut reaper = interval(REAP_INTERVAL);
    reaper.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // `interval(...)` fires its first tick immediately; skip it so the
    // reaper doesn't sweep before any packets have arrived.
    let _ = reaper.tick().await;

    let ctx = ListenerCtx {
        socket: socket.clone(),
        spec: &spec,
        wiring: &wiring,
    };

    loop {
        tokio::select! {
            biased;
            _ = wiring.cancel.cancelled() => {
                shutdown_all_peers(
                    &mut peers,
                    spec.parent_id,
                    &wiring.event_tx,
                    &mut writer_tasks,
                )
                .await;
                return;
            }
            _ = reaper.tick() => {
                reap_idle_peers(
                    &mut peers,
                    spec.parent_id,
                    Duration::from_secs(spec.idle_secs),
                    &wiring.event_tx,
                )
                .await;
            }
            res = socket.recv_from(&mut buf) => {
                match res {
                    Ok((bytes_read, src)) => {
                        handle_packet(&buf[..bytes_read], src, &mut peers, &mut writer_tasks, &ctx).await;
                    }
                    Err(err) => {
                        warn!(error = %err, "recv_from error");
                    }
                }
            }
        }
    }
}

/// Bundle of references the listener loop hands to its packet-handling
/// helpers, so each helper takes one parameter instead of many. Borrowed
/// for the lifetime of a single accept iteration.
struct ListenerCtx<'a> {
    socket: Arc<UdpSocket>,
    spec: &'a UdpServerSpec,
    wiring: &'a ServerWiring,
}

async fn handle_packet(
    data: &[u8],
    src: SocketAddr,
    peers: &mut HashMap<SocketAddr, PeerEntry>,
    writer_tasks: &mut JoinSet<()>,
    ctx: &ListenerCtx<'_>,
) {
    if !peers.contains_key(&src) && !admit_new_peer(src, peers, writer_tasks, ctx).await {
        return;
    }

    let Some(peer) = peers.get_mut(&src) else {
        return;
    };
    peer.last_seen = Instant::now();

    peer.framer.buffer_mut().extend_from_slice(data);
    // Filters live on the parent listener (uniform across children per
    // CLAUDE.md); drop credit goes to this peer's stats.
    let session_ctx = SessionCtx {
        endpoint_id: peer.child_id,
        stats: &peer.stats,
        frame_tx: &ctx.wiring.frame_tx,
        filters: &ctx.spec.identity.filters,
    };
    let pipeline = session_ctx
        .forward_inbound_frames(&mut peer.framer, &mut peer.seq_tracker)
        .instrument(tracing::trace_span!("udps_ingress", %src));
    if pipeline.await.is_break() {
        return;
    }
    peer.framer_counters.sync(&peer.framer, &peer.stats);
}

/// Admit a brand-new peer learned from `src`. Returns `false` when the
/// event channel is closed (router gone) so the caller drops the packet
/// without touching the existing peer table. Announces `PeerAdded` *before*
/// evicting the LRU victim: a silent failure here must not destroy real
/// state in vain.
async fn admit_new_peer(
    src: SocketAddr,
    peers: &mut HashMap<SocketAddr, PeerEntry>,
    writer_tasks: &mut JoinSet<()>,
    ctx: &ListenerCtx<'_>,
) -> bool {
    let child_id = ctx.wiring.allocator.alloc();
    let stats = Arc::new(EndpointStats::new(EndpointState::Connected));
    let tx_queue = TxQueue::new(DEFAULT_TX_QUEUE_FRAMES, stats.clone());
    let writer_cancel = ctx.wiring.cancel.child_token();
    let name = peer_endpoint_name(&ctx.spec.parent_name, src);
    let writer_span = info_span!("udps_peer", name = %name);

    if ctx
        .wiring
        .event_tx
        .send(EndpointEvent::PeerAdded {
            parent_id: ctx.spec.parent_id,
            child_id,
            peer_addr: src,
            name,
            stats: stats.clone(),
            routable: Routable {
                tx_queue: tx_queue.clone(),
                identity: ctx.spec.identity.clone(),
            },
        })
        .await
        .is_err()
    {
        debug!("event channel closed; dropping admitted peer");
        return false;
    }
    info!(parent_id = %ctx.spec.parent_id, %src, "peer added");

    if peers.len() >= ctx.spec.peer_capacity {
        evict_lru_peer(peers, ctx.spec.parent_id, &ctx.wiring.event_tx).await;
    }

    writer_tasks.spawn(
        run_peer_writer(
            ctx.socket.clone(),
            src,
            tx_queue,
            stats.clone(),
            writer_cancel.clone(),
        )
        .instrument(writer_span),
    );

    peers.insert(
        src,
        PeerEntry {
            child_id,
            framer: Framer::new(),
            last_seen: Instant::now(),
            framer_counters: FramerCounters::new(),
            seq_tracker: SeqTracker::new(SEQ_TRACKER_CAPACITY),
            stats,
            writer_cancel,
        },
    );
    true
}

async fn evict_lru_peer(
    peers: &mut HashMap<SocketAddr, PeerEntry>,
    parent_id: EndpointId,
    event_tx: &mpsc::Sender<EndpointEvent>,
) {
    let Some((&victim, _)) = peers.iter().min_by_key(|(_, e)| e.last_seen) else {
        return;
    };
    let Some(entry) = peers.remove(&victim) else {
        return;
    };
    entry.writer_cancel.cancel();
    let _ = event_tx
        .send(EndpointEvent::PeerRemoved {
            parent_id,
            child_id: entry.child_id,
            peer_addr: victim,
            reason: PeerRemovalReason::LruEvicted,
        })
        .await;
    info!(parent_id = %parent_id, %victim, "peer LRU-evicted");
}

async fn reap_idle_peers(
    peers: &mut HashMap<SocketAddr, PeerEntry>,
    parent_id: EndpointId,
    idle: Duration,
    event_tx: &mpsc::Sender<EndpointEvent>,
) {
    let now = Instant::now();
    let expired: Vec<SocketAddr> = peers
        .iter()
        .filter_map(|(addr, entry)| (now.duration_since(entry.last_seen) >= idle).then_some(*addr))
        .collect();
    for addr in expired {
        if let Some(entry) = peers.remove(&addr) {
            entry.writer_cancel.cancel();
            let _ = event_tx
                .send(EndpointEvent::PeerRemoved {
                    parent_id,
                    child_id: entry.child_id,
                    peer_addr: addr,
                    reason: PeerRemovalReason::Idle,
                })
                .await;
            info!(parent_id = %parent_id, %addr, "peer idle-reaped");
        }
    }
}

async fn shutdown_all_peers(
    peers: &mut HashMap<SocketAddr, PeerEntry>,
    parent_id: EndpointId,
    event_tx: &mpsc::Sender<EndpointEvent>,
    writer_tasks: &mut JoinSet<()>,
) {
    for (addr, entry) in peers.drain() {
        entry.writer_cancel.cancel();
        let _ = event_tx
            .send(EndpointEvent::PeerRemoved {
                parent_id,
                child_id: entry.child_id,
                peer_addr: addr,
                reason: PeerRemovalReason::ListenerShutdown,
            })
            .await;
    }
    while writer_tasks.join_next().await.is_some() {}
}

async fn run_peer_writer(
    socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    queue: TxQueue,
    stats: Arc<EndpointStats>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                queue.drain_and_discard();
                return;
            }
            frame = queue.pop_or_wait() => {
                match socket.send_to(&frame, peer_addr).await {
                    Ok(bytes_sent) => stats.add_tx_frame(bytes_sent),
                    Err(err) => {
                        stats.dropped_tx.fetch_add(1, Ordering::Relaxed);
                        warn!(error = %err, peer = %peer_addr, "send_to failed; dropping frame");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use crate::endpoint::events::RouterFrame;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn spec_defaults_when_endpoint_unset() {
        let ep = UdpServerEndpoint::default();
        let spec = UdpServerSpec::from_endpoint(ep, EndpointId(0), "n".into());
        assert_eq!(spec.idle_secs, DEFAULT_IDLE_SECS);
        assert_eq!(spec.peer_capacity, DEFAULT_PEER_CAPACITY);
        assert_eq!(spec.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(spec.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    #[test]
    fn spec_overrides_idle_secs_via_endpoint() {
        let ep = UdpServerEndpoint {
            idle_secs: Some(10),
            ..UdpServerEndpoint::default()
        };
        let spec = UdpServerSpec::from_endpoint(ep, EndpointId(0), "n".into());
        assert_eq!(spec.idle_secs, 10);
        // Reconnect curve stays at the tcpc defaults — CLAUDE.md "udps: bind-
        // retry shares the tcpc: curve, no per-listener override".
        assert_eq!(spec.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(spec.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    fn dummy_peer(child_id: EndpointId, age: Duration) -> PeerEntry {
        let stats = Arc::new(EndpointStats::default());
        PeerEntry {
            child_id,
            framer: Framer::new(),
            last_seen: Instant::now() - age,
            framer_counters: FramerCounters::new(),
            seq_tracker: SeqTracker::new(8),
            stats,
            writer_cancel: CancellationToken::new(),
        }
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, port as u8)), port)
    }

    #[tokio::test]
    async fn evict_lru_drops_oldest_peer() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut peers: HashMap<SocketAddr, PeerEntry> = HashMap::new();
        peers.insert(addr(1), dummy_peer(EndpointId(0), Duration::from_secs(30)));
        peers.insert(addr(2), dummy_peer(EndpointId(1), Duration::from_secs(60)));
        peers.insert(addr(3), dummy_peer(EndpointId(2), Duration::from_secs(5)));
        evict_lru_peer(&mut peers, EndpointId(99), &tx).await;
        assert!(!peers.contains_key(&addr(2)));
        assert!(peers.contains_key(&addr(1)));
        assert!(peers.contains_key(&addr(3)));
        let ev = rx.try_recv().unwrap();
        match ev {
            EndpointEvent::PeerRemoved {
                reason,
                peer_addr,
                child_id,
                ..
            } => {
                assert_eq!(reason, PeerRemovalReason::LruEvicted);
                assert_eq!(peer_addr, addr(2));
                assert_eq!(child_id, EndpointId(1));
            }
            other => panic!("expected PeerRemoved, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn evict_lru_no_op_on_empty_table() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut peers: HashMap<SocketAddr, PeerEntry> = HashMap::new();
        evict_lru_peer(&mut peers, EndpointId(0), &tx).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn reap_idle_removes_only_stale_peers() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut peers: HashMap<SocketAddr, PeerEntry> = HashMap::new();
        peers.insert(addr(1), dummy_peer(EndpointId(0), Duration::from_secs(120)));
        peers.insert(addr(2), dummy_peer(EndpointId(1), Duration::from_secs(5)));
        peers.insert(addr(3), dummy_peer(EndpointId(2), Duration::from_secs(120)));
        reap_idle_peers(&mut peers, EndpointId(99), Duration::from_secs(60), &tx).await;
        assert!(!peers.contains_key(&addr(1)));
        assert!(peers.contains_key(&addr(2)));
        assert!(!peers.contains_key(&addr(3)));
        let mut removed = Vec::new();
        while let Ok(EndpointEvent::PeerRemoved {
            reason, peer_addr, ..
        }) = rx.try_recv()
        {
            assert_eq!(reason, PeerRemovalReason::Idle);
            removed.push(peer_addr);
        }
        removed.sort();
        assert_eq!(removed, vec![addr(1), addr(3)]);
    }

    /// Shared admission-test fixture: a real bound socket, an allocator, a
    /// frame/event channel pair, and a `UdpServerSpec` whose `peer_capacity`
    /// is whatever the test needs. Returns the receivers alongside the
    /// owning fixture so the caller can assert on emitted lifecycle events.
    struct AdmissionFixture {
        socket: Arc<UdpSocket>,
        spec: UdpServerSpec,
        wiring: ServerWiring,
        cancel: CancellationToken,
        _frame_rx: mpsc::Receiver<RouterFrame>,
        event_rx: mpsc::Receiver<EndpointEvent>,
    }

    async fn admission_fixture(peer_capacity: usize) -> AdmissionFixture {
        let socket = Arc::new(
            UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("bind ctx socket"),
        );
        let allocator = Arc::new(EndpointIdAllocator::new());
        let parent_id = allocator.alloc();
        let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);
        let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(16);
        let cancel = CancellationToken::new();
        let spec = UdpServerSpec {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            parent_id,
            parent_name: "test".to_string(),
            idle_secs: DEFAULT_IDLE_SECS,
            peer_capacity,
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
            identity: IdentityFlags::default(),
        };
        let wiring = ServerWiring {
            allocator,
            frame_tx,
            event_tx,
            cancel: cancel.clone(),
            stats: Arc::new(EndpointStats::default()),
        };
        AdmissionFixture {
            socket,
            spec,
            wiring,
            cancel,
            _frame_rx,
            event_rx,
        }
    }

    /// Boundary test for the locked `DEFAULT_PEER_CAPACITY` invariant: when a
    /// fresh source arrives and the peer table is already at `peer_capacity`,
    /// `admit_new_peer` must (a) keep the table size at the cap, (b) evict
    /// the LRU entry by last-seen, (c) admit the new peer, and (d) emit
    /// `PeerAdded` for the newcomer followed by `PeerRemoved { LruEvicted }`
    /// for the victim. The production cap is 256 (CLAUDE.md "Hardcoded
    /// plumbing knobs"); the spec's mutable `peer_capacity` field exists so
    /// this branch can run against a small N rather than 256 dummy peers.
    #[tokio::test]
    async fn admit_new_peer_evicts_lru_when_at_capacity() {
        let mut fx = admission_fixture(3).await;
        let ctx = ListenerCtx {
            socket: fx.socket.clone(),
            spec: &fx.spec,
            wiring: &fx.wiring,
        };

        let mut peers: HashMap<SocketAddr, PeerEntry> = HashMap::new();
        // addr(2) is the LRU victim (last_seen 60s ago).
        peers.insert(addr(1), dummy_peer(EndpointId(10), Duration::from_secs(30)));
        peers.insert(addr(2), dummy_peer(EndpointId(11), Duration::from_secs(60)));
        peers.insert(addr(3), dummy_peer(EndpointId(12), Duration::from_secs(5)));
        assert_eq!(
            peers.len(),
            3,
            "pre-condition: table must be at peer_capacity before admit_new_peer"
        );
        let mut writer_tasks: JoinSet<()> = JoinSet::new();

        let admitted = admit_new_peer(addr(4), &mut peers, &mut writer_tasks, &ctx).await;
        assert!(
            admitted,
            "admission must succeed when event channel is open"
        );

        assert_eq!(peers.len(), 3, "table size should stay at peer_capacity");
        assert!(!peers.contains_key(&addr(2)), "LRU peer should be evicted");
        assert!(peers.contains_key(&addr(1)));
        assert!(peers.contains_key(&addr(3)));
        assert!(peers.contains_key(&addr(4)), "new peer should be admitted");

        match fx.event_rx.try_recv() {
            Ok(EndpointEvent::PeerAdded { peer_addr, .. }) => {
                assert_eq!(peer_addr, addr(4));
            }
            other => panic!("expected PeerAdded for addr(4), got {other:?}"),
        }
        match fx.event_rx.try_recv() {
            Ok(EndpointEvent::PeerRemoved {
                reason, peer_addr, ..
            }) => {
                assert_eq!(reason, PeerRemovalReason::LruEvicted);
                assert_eq!(peer_addr, addr(2));
            }
            other => panic!("expected PeerRemoved(LruEvicted) for addr(2), got {other:?}"),
        }
        assert!(
            fx.event_rx.try_recv().is_err(),
            "no further lifecycle events expected"
        );

        fx.cancel.cancel();
        writer_tasks.shutdown().await;
    }

    /// Regression: when the router event channel is already closed and a
    /// brand-new peer arrives at capacity, `admit_new_peer` must return
    /// `false` WITHOUT evicting an existing peer to make room. Announcing
    /// `PeerAdded` ahead of the LRU eviction guarantees we never destroy
    /// real state in vain.
    #[tokio::test]
    async fn admit_new_peer_with_closed_event_tx_does_not_evict() {
        let AdmissionFixture {
            socket,
            spec,
            wiring,
            event_rx,
            ..
        } = admission_fixture(2).await;
        // Close the event channel so PeerAdded send fails immediately.
        drop(event_rx);

        let ctx = ListenerCtx {
            socket,
            spec: &spec,
            wiring: &wiring,
        };

        let mut peers: HashMap<SocketAddr, PeerEntry> = HashMap::new();
        peers.insert(addr(1), dummy_peer(EndpointId(10), Duration::from_secs(30)));
        peers.insert(addr(2), dummy_peer(EndpointId(11), Duration::from_secs(60)));
        let mut writer_tasks: JoinSet<()> = JoinSet::new();

        let admitted = admit_new_peer(addr(3), &mut peers, &mut writer_tasks, &ctx).await;
        assert!(
            !admitted,
            "admission must fail when event channel is closed"
        );

        assert_eq!(peers.len(), 2, "no peer should have been evicted");
        assert!(peers.contains_key(&addr(1)), "addr(1) should remain");
        assert!(peers.contains_key(&addr(2)), "addr(2) should remain");
        assert!(
            writer_tasks.is_empty(),
            "no writer task should have spawned for the dropped peer"
        );
    }
}
