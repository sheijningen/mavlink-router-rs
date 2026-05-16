use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, trace, warn};

use super::super::EndpointId;
use super::super::EndpointIdAllocator;
use super::super::backoff::Backoff;
use super::super::events::{EndpointEvent, PeerRemovalReason, RouterFrame};
use super::super::identity_flags::IdentityFlags;
use super::super::peer_endpoint_name;
use super::super::socket::bind_udp_dual_stack;
use super::super::spec::UdpServerEndpoint;
use super::super::stats::{EndpointStats, FramerCounters};
use super::super::tx_queue::TxQueue;
use super::super::wait_or_cancel;
use crate::mavlink::framer::Framer;

const DEFAULT_IDLE_SECS: u64 = 60;
const DEFAULT_PEER_CAPACITY: usize = 256;
const DEFAULT_READ_BUF_BYTES: usize = 8192;
const DEFAULT_TX_QUEUE_FRAMES: usize = 256;
const DEFAULT_RECONNECT_INITIAL_MS: u64 = 250;
const DEFAULT_RECONNECT_MAX_MS: u64 = 30_000;
// Max IP datagram payload plus headroom; one `recv_from` cannot return more
// than the kernel's MTU-bounded payload, but we size the buffer to the IP
// theoretical max so a fragmented giant datagram couldn't be truncated.
const MAX_DATAGRAM_BYTES: usize = 65_536;
const REAP_INTERVAL: Duration = Duration::from_secs(1);

/// Per-listener runtime configuration. The spec parser hands us a fully-typed
/// `UdpServerEndpoint`; this struct collapses the optional knobs down to the
/// concrete values the task actually uses, substituting CLAUDE.md defaults
/// where the user left a knob unset. The `reconnect_*_ms` fields are
/// hard-coded to the `tcpc:` curve (CLAUDE.md: "`tcps:` bind-retry shares the
/// `tcpc:` curve, no per-listener override. Same reasoning applies to
/// `udps:`") — `udps:` does not expose per-listener reconnect overrides in v1.
#[derive(Debug, Clone, Copy)]
pub struct UdpServerConfig {
    pub idle_secs: u64,
    pub peer_capacity: usize,
    pub read_buf_bytes: usize,
    pub tx_queue_frames: usize,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
}

impl Default for UdpServerConfig {
    fn default() -> Self {
        Self {
            idle_secs: DEFAULT_IDLE_SECS,
            peer_capacity: DEFAULT_PEER_CAPACITY,
            read_buf_bytes: DEFAULT_READ_BUF_BYTES,
            tx_queue_frames: DEFAULT_TX_QUEUE_FRAMES,
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
        }
    }
}

impl UdpServerConfig {
    pub fn from_endpoint(ep: &UdpServerEndpoint) -> Self {
        Self {
            idle_secs: ep.idle_secs.unwrap_or(DEFAULT_IDLE_SECS),
            peer_capacity: ep.udps_peer_capacity.unwrap_or(DEFAULT_PEER_CAPACITY),
            read_buf_bytes: ep.common.read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES),
            tx_queue_frames: ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
        }
    }
}

/// Typed-empty return for `udps:` `run()`. Bind failures enter the same
/// backoff loop as `tcpc:` reconnects (CLAUDE.md "Bind/open failure at startup
/// is not fatal"), and per-peer recv errors are logged and the loop continues
/// — no terminal failure modes remain in v1. Kept as a typed return for
/// symmetry with the other endpoint modules in case a fatal case shows up.
#[derive(Debug, Error)]
pub enum UdpServerError {}

/// Per-peer state the listener task carries between packets: the child
/// routing endpoint's identity, its framer, last-seen timestamp for the LRU
/// and idle-reap policies, and the handles needed to shut down its writer.
struct PeerEntry {
    child_id: EndpointId,
    framer: Framer,
    last_seen: Instant,
    framer_counters: FramerCounters,
    stats: Arc<EndpointStats>,
    writer_cancel: CancellationToken,
}

/// Inputs that distinguish one `udps:` listener from another: where to bind,
/// what to call it, and the per-listener knobs from the query string.
/// `identity` carries the filter / sniffer / group / capacity bundle —
/// inherited by every learned peer at admission time (CLAUDE.md "Sub-
/// endpoints inherit their parent's `IdentityFlags` by clone at spawn
/// time"). Unused until Phase 5 wires it through the reader and the router.
pub struct UdpServerSpec {
    pub listen_addr: SocketAddr,
    pub parent_id: EndpointId,
    pub parent_name: String,
    pub cfg: UdpServerConfig,
    pub identity: IdentityFlags,
}

/// Shared wiring every endpoint needs: the global EndpointId allocator,
/// the reader→router frame channel, the sub-endpoint lifecycle channel,
/// and the cancellation token. `bound_addr_tx`, if set, fires once on the
/// first successful bind with the actual `local_addr()` — lets a caller
/// that requested `127.0.0.1:0` (test harnesses, future systemd-socket
/// adoption) discover the OS-assigned port.
pub struct UdpServerWiring {
    pub allocator: Arc<EndpointIdAllocator>,
    pub frame_tx: mpsc::Sender<RouterFrame>,
    pub event_tx: mpsc::Sender<EndpointEvent>,
    pub cancel: CancellationToken,
    pub bound_addr_tx: Option<oneshot::Sender<SocketAddr>>,
}

/// Run a `udps:` listener until the cancellation token fires. Binding is
/// retried with the shared capped-exp backoff (CLAUDE.md "Initial bind/dial
/// failure path"), so a port collision at startup logs at WARN and the
/// listener attaches as soon as the port frees. Once bound, loops over
/// `recv_from`, the idle reaper, and cancellation. Each learned peer becomes
/// a sub-routing endpoint announced via `event_tx` with its own TxQueue and
/// writer task.
pub async fn run(spec: UdpServerSpec, wiring: UdpServerWiring) -> Result<(), UdpServerError> {
    let span = info_span!("udps", name = %spec.parent_name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: UdpServerSpec, wiring: UdpServerWiring) -> Result<(), UdpServerError> {
    let UdpServerSpec {
        listen_addr,
        parent_id,
        parent_name,
        cfg,
        identity,
    } = spec;
    let UdpServerWiring {
        allocator,
        frame_tx,
        event_tx,
        cancel,
        bound_addr_tx,
    } = wiring;

    let mut backoff = Backoff::new(cfg.reconnect_initial_ms, cfg.reconnect_max_ms);

    let socket = loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        match bind_udp_dual_stack(listen_addr) {
            Ok(s) => break Arc::new(s),
            Err(e) => {
                warn!(error = %e, %listen_addr, "udps bind failed; retrying after backoff");
                if !wait_or_cancel(&cancel, backoff.next_delay()).await {
                    return Ok(());
                }
            }
        }
    };
    let bound_addr = socket.local_addr().unwrap_or(listen_addr);
    if let Some(tx) = bound_addr_tx {
        let _ = tx.send(bound_addr);
    }
    info!(%bound_addr, parent_id = %parent_id, "udps listening");

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
        parent_id,
        parent_name: &parent_name,
        cfg: &cfg,
        identity: &identity,
        allocator: &allocator,
        frame_tx: &frame_tx,
        event_tx: &event_tx,
        cancel: &cancel,
    };

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                shutdown_all_peers(
                    &mut peers,
                    parent_id,
                    &event_tx,
                    &mut writer_tasks,
                )
                .await;
                return Ok(());
            }
            _ = reaper.tick() => {
                reap_idle_peers(
                    &mut peers,
                    parent_id,
                    Duration::from_secs(cfg.idle_secs),
                    &event_tx,
                )
                .await;
            }
            res = socket.recv_from(&mut buf) => {
                match res {
                    Ok((n, src)) => {
                        handle_packet(&buf[..n], src, &mut peers, &mut writer_tasks, &ctx).await;
                    }
                    Err(e) => {
                        warn!(error = %e, "udps recv_from error");
                    }
                }
            }
        }
    }
}

/// Bundle of references the listener loop hands to its packet-handling
/// helpers, so each helper takes one parameter instead of seven. Borrowed
/// for the lifetime of a single accept iteration.
struct ListenerCtx<'a> {
    socket: Arc<UdpSocket>,
    parent_id: EndpointId,
    parent_name: &'a str,
    cfg: &'a UdpServerConfig,
    identity: &'a IdentityFlags,
    allocator: &'a Arc<EndpointIdAllocator>,
    frame_tx: &'a mpsc::Sender<RouterFrame>,
    event_tx: &'a mpsc::Sender<EndpointEvent>,
    cancel: &'a CancellationToken,
}

async fn handle_packet(
    data: &[u8],
    src: SocketAddr,
    peers: &mut HashMap<SocketAddr, PeerEntry>,
    writer_tasks: &mut JoinSet<()>,
    ctx: &ListenerCtx<'_>,
) {
    if !peers.contains_key(&src) {
        let child_id = ctx.allocator.alloc();
        let stats = Arc::new(EndpointStats::default());
        let tx_queue = TxQueue::new(ctx.cfg.tx_queue_frames, stats.clone());
        let writer_cancel = ctx.cancel.child_token();
        let name = peer_endpoint_name(ctx.parent_name, src);
        let writer_span = info_span!("udps_peer", name = %name);

        // Announce PeerAdded before LRU-evicting and before spawning the writer:
        // a closed router event channel here means the router is gone, and
        // destroying an existing peer's state in vain (silent PeerRemoved that
        // nobody receives) is worse than just dropping the new packet. The
        // peer inherits the parent listener's IdentityFlags by clone per
        // CLAUDE.md "Sub-endpoints inherit their parent's IdentityFlags by
        // clone at spawn time".
        if ctx
            .event_tx
            .send(EndpointEvent::PeerAdded {
                parent_id: ctx.parent_id,
                child_id,
                peer_addr: src,
                name,
                tx_queue: tx_queue.clone(),
                stats: stats.clone(),
                identity: ctx.identity.clone(),
            })
            .await
            .is_err()
        {
            debug!("udps event channel closed; dropping admitted peer");
            return;
        }
        trace!(parent_id = %ctx.parent_id, %src, "udps peer added");

        if peers.len() >= ctx.cfg.peer_capacity {
            evict_lru_peer(peers, ctx.parent_id, ctx.event_tx).await;
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

        let entry = PeerEntry {
            child_id,
            framer: Framer::with_capacity(ctx.cfg.read_buf_bytes),
            last_seen: Instant::now(),
            framer_counters: FramerCounters::new(),
            stats,
            writer_cancel,
        };
        peers.insert(src, entry);
    }

    let Some(peer) = peers.get_mut(&src) else {
        return;
    };
    peer.last_seen = Instant::now();

    peer.framer.buffer_mut().extend_from_slice(data);
    while let Some((header, frame)) = peer.framer.try_next_frame() {
        let frame_len = frame.len();
        peer.stats.add_rx_frame(frame_len);
        if ctx
            .frame_tx
            .send(RouterFrame {
                endpoint_id: peer.child_id,
                frame,
                header,
            })
            .await
            .is_err()
        {
            debug!("udps router channel closed; stopping frame forwarding");
            return;
        }
    }
    peer.framer_counters.sync(&peer.framer, &peer.stats);
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
    trace!(parent_id = %parent_id, %victim, "udps peer LRU-evicted");
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
            trace!(parent_id = %parent_id, %addr, "udps peer idle-reaped");
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
                    Ok(n) => stats.add_tx_frame(n),
                    Err(e) => {
                        warn!(error = %e, peer = %peer_addr, "udps send_to failed; dropping frame");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn config_defaults_when_endpoint_unset() {
        let ep = UdpServerEndpoint::default();
        let cfg = UdpServerConfig::from_endpoint(&ep);
        assert_eq!(cfg.idle_secs, DEFAULT_IDLE_SECS);
        assert_eq!(cfg.peer_capacity, DEFAULT_PEER_CAPACITY);
        assert_eq!(cfg.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(cfg.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
        assert_eq!(cfg.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(cfg.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    #[test]
    fn config_overrides_common_fields_but_not_reconnect_curve() {
        use crate::endpoint::spec::CommonQuery;
        let ep = UdpServerEndpoint {
            idle_secs: Some(10),
            udps_peer_capacity: Some(4),
            common: CommonQuery {
                read_buf_bytes: Some(1024),
                tx_queue_frames: Some(8),
            },
            ..UdpServerEndpoint::default()
        };
        let cfg = UdpServerConfig::from_endpoint(&ep);
        assert_eq!(cfg.idle_secs, 10);
        assert_eq!(cfg.peer_capacity, 4);
        assert_eq!(cfg.read_buf_bytes, 1024);
        assert_eq!(cfg.tx_queue_frames, 8);
        // Reconnect curve stays at the tcpc defaults — CLAUDE.md "udps: bind-
        // retry shares the tcpc: curve, no per-listener override".
        assert_eq!(cfg.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(cfg.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    fn dummy_peer(child_id: EndpointId, age: Duration) -> PeerEntry {
        let stats = Arc::new(EndpointStats::default());
        PeerEntry {
            child_id,
            framer: Framer::new(),
            last_seen: Instant::now() - age,
            framer_counters: FramerCounters::new(),
            stats,
            writer_cancel: CancellationToken::new(),
        }
    }

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, p as u8)), p)
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

    /// Regression: when the router event channel is already closed and a
    /// brand-new peer arrives at capacity, `handle_packet` must NOT evict an
    /// existing peer just to drop the new one. Reordering the announce ahead
    /// of the LRU eviction guarantees we never destroy real state in vain.
    #[tokio::test]
    async fn handle_packet_with_closed_event_tx_does_not_evict() {
        let socket = Arc::new(
            UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("bind ctx socket"),
        );
        let allocator = Arc::new(EndpointIdAllocator::new());
        let parent_id = allocator.alloc();
        let cfg = UdpServerConfig {
            peer_capacity: 2,
            ..UdpServerConfig::default()
        };
        let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);
        let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(8);
        let cancel = CancellationToken::new();
        let parent_name = "test".to_string();
        let identity = IdentityFlags::default();
        let ctx = ListenerCtx {
            socket: socket.clone(),
            parent_id,
            parent_name: &parent_name,
            cfg: &cfg,
            identity: &identity,
            allocator: &allocator,
            frame_tx: &frame_tx,
            event_tx: &event_tx,
            cancel: &cancel,
        };

        let mut peers: HashMap<SocketAddr, PeerEntry> = HashMap::new();
        peers.insert(addr(1), dummy_peer(EndpointId(10), Duration::from_secs(30)));
        peers.insert(addr(2), dummy_peer(EndpointId(11), Duration::from_secs(60)));
        let mut writer_tasks: JoinSet<()> = JoinSet::new();

        // Close the event channel so PeerAdded send fails immediately.
        drop(event_rx);

        handle_packet(&[], addr(3), &mut peers, &mut writer_tasks, &ctx).await;

        assert_eq!(peers.len(), 2, "no peer should have been evicted");
        assert!(peers.contains_key(&addr(1)), "addr(1) should remain");
        assert!(peers.contains_key(&addr(2)), "addr(2) should remain");
        assert!(
            writer_tasks.is_empty(),
            "no writer task should have spawned for the dropped peer"
        );
    }
}
