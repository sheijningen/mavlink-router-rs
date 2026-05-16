use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use super::EndpointId;
use super::EndpointIdAllocator;
use super::events::{EndpointEvent, PeerRemovalReason, RouterFrame};
use super::socket::bind_udp_dual_stack;
use super::stats::EndpointStats;
use super::tx_queue::TxQueue;
use crate::mavlink::framer::Framer;

const DEFAULT_IDLE_SECS: u64 = 60;
const DEFAULT_PEER_CAPACITY: usize = 256;
const DEFAULT_READ_BUF_BYTES: usize = 8192;
const DEFAULT_TX_QUEUE_FRAMES: usize = 256;
const MAX_DATAGRAM_BYTES: usize = 65_536;
const REAP_INTERVAL: Duration = Duration::from_secs(1);

/// Per-listener configuration sourced from the endpoint spec's query map.
#[derive(Debug, Clone, Copy)]
pub struct UdpServerConfig {
    pub idle_secs: u64,
    pub peer_capacity: usize,
    pub read_buf_bytes: usize,
    pub tx_queue_frames: usize,
}

impl Default for UdpServerConfig {
    fn default() -> Self {
        Self {
            idle_secs: DEFAULT_IDLE_SECS,
            peer_capacity: DEFAULT_PEER_CAPACITY,
            read_buf_bytes: DEFAULT_READ_BUF_BYTES,
            tx_queue_frames: DEFAULT_TX_QUEUE_FRAMES,
        }
    }
}

impl UdpServerConfig {
    pub fn from_query(q: &BTreeMap<String, String>) -> Result<Self, UdpServerError> {
        let mut cfg = Self::default();
        if let Some(v) = q.get("idle_secs") {
            cfg.idle_secs = parse_u64(v, "idle_secs")?;
            if cfg.idle_secs == 0 {
                return Err(UdpServerError::InvalidQuery(
                    "idle_secs must be > 0".to_string(),
                ));
            }
        }
        if let Some(v) = q.get("udps_peer_capacity") {
            cfg.peer_capacity = parse_usize(v, "udps_peer_capacity")?;
            if cfg.peer_capacity == 0 {
                return Err(UdpServerError::InvalidQuery(
                    "udps_peer_capacity must be > 0".to_string(),
                ));
            }
        }
        if let Some(v) = q.get("read_buf_bytes") {
            cfg.read_buf_bytes = parse_usize(v, "read_buf_bytes")?;
            if cfg.read_buf_bytes == 0 {
                return Err(UdpServerError::InvalidQuery(
                    "read_buf_bytes must be > 0".to_string(),
                ));
            }
        }
        if let Some(v) = q.get("tx_queue_frames") {
            cfg.tx_queue_frames = parse_usize(v, "tx_queue_frames")?;
            if cfg.tx_queue_frames == 0 {
                return Err(UdpServerError::InvalidQuery(
                    "tx_queue_frames must be > 0".to_string(),
                ));
            }
        }
        Ok(cfg)
    }
}

fn parse_u64(v: &str, key: &str) -> Result<u64, UdpServerError> {
    v.parse().map_err(|_| {
        UdpServerError::InvalidQuery(format!("{key} must be a non-negative integer, got '{v}'"))
    })
}

fn parse_usize(v: &str, key: &str) -> Result<usize, UdpServerError> {
    v.parse().map_err(|_| {
        UdpServerError::InvalidQuery(format!("{key} must be a non-negative integer, got '{v}'"))
    })
}

#[derive(Debug, Error)]
pub enum UdpServerError {
    #[error("invalid udps: query: {0}")]
    InvalidQuery(String),
    #[error("udps: bind {addr} failed: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}

/// Sub-endpoint name for a learned peer: `<parent>/<ip>-<port>` for IPv4 and
/// `<parent>/[<ip>]-<port>` for IPv6 (the brackets match the CLI grammar so the
/// name round-trips visually with an explicit `udps:[::]:N` spec).
pub fn peer_endpoint_name(parent_name: &str, addr: SocketAddr) -> String {
    match addr {
        SocketAddr::V4(v4) => format!("{parent_name}/{}-{}", v4.ip(), v4.port()),
        SocketAddr::V6(v6) => format!("{parent_name}/[{}]-{}", v6.ip(), v6.port()),
    }
}

struct PeerEntry {
    child_id: EndpointId,
    framer: Framer,
    last_seen: Instant,
    last_resync_total: u64,
    last_crc_total: u64,
    stats: Arc<EndpointStats>,
    writer_cancel: CancellationToken,
}

/// Inputs that distinguish one `udps:` listener from another: where to bind,
/// what to call it, and the per-listener knobs from the query string.
pub struct UdpServerSpec {
    pub listen_addr: SocketAddr,
    pub parent_id: EndpointId,
    pub parent_name: String,
    pub cfg: UdpServerConfig,
}

/// Shared wiring every endpoint needs: the global EndpointId allocator,
/// the reader→router frame channel, the sub-endpoint lifecycle channel,
/// and the cancellation token.
pub struct UdpServerWiring {
    pub allocator: Arc<EndpointIdAllocator>,
    pub frame_tx: mpsc::Sender<RouterFrame>,
    pub event_tx: mpsc::Sender<EndpointEvent>,
    pub cancel: CancellationToken,
}

/// Run a `udps:` listener until the cancellation token fires. Binds the
/// socket synchronously (errors propagated), then loops over `recv_from`,
/// the idle reaper, and cancellation. Each learned peer becomes a sub-routing
/// endpoint announced via `event_tx` with its own TxQueue and writer task.
pub async fn run(spec: UdpServerSpec, wiring: UdpServerWiring) -> Result<(), UdpServerError> {
    let UdpServerSpec {
        listen_addr,
        parent_id,
        parent_name,
        cfg,
    } = spec;
    let UdpServerWiring {
        allocator,
        frame_tx,
        event_tx,
        cancel,
    } = wiring;

    let socket =
        Arc::new(
            bind_udp_dual_stack(listen_addr).map_err(|source| UdpServerError::Bind {
                addr: listen_addr,
                source,
            })?,
        );

    let mut peers: HashMap<SocketAddr, PeerEntry> = HashMap::new();
    let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
    let mut writer_tasks: JoinSet<()> = JoinSet::new();

    let mut reaper = interval(REAP_INTERVAL);
    reaper.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let _ = reaper.tick().await; // consume the immediate first tick

    let ctx = ListenerCtx {
        socket: socket.clone(),
        parent_id,
        parent_name: &parent_name,
        cfg: &cfg,
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

struct ListenerCtx<'a> {
    socket: Arc<UdpSocket>,
    parent_id: EndpointId,
    parent_name: &'a str,
    cfg: &'a UdpServerConfig,
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
        if peers.len() >= ctx.cfg.peer_capacity {
            evict_lru_peer(peers, ctx.parent_id, ctx.event_tx).await;
        }
        let child_id = ctx.allocator.alloc();
        let stats = Arc::new(EndpointStats::new());
        let tx_queue = TxQueue::new(ctx.cfg.tx_queue_frames, stats.clone());
        let writer_cancel = ctx.cancel.child_token();
        let name = peer_endpoint_name(ctx.parent_name, src);

        writer_tasks.spawn(run_peer_writer(
            ctx.socket.clone(),
            src,
            tx_queue.clone(),
            stats.clone(),
            writer_cancel.clone(),
        ));

        let entry = PeerEntry {
            child_id,
            framer: Framer::with_capacity(ctx.cfg.read_buf_bytes),
            last_seen: Instant::now(),
            last_resync_total: 0,
            last_crc_total: 0,
            stats: stats.clone(),
            writer_cancel,
        };
        peers.insert(src, entry);

        let _ = ctx
            .event_tx
            .send(EndpointEvent::PeerAdded {
                parent_id: ctx.parent_id,
                child_id,
                peer_addr: src,
                name,
                tx_queue,
                stats,
            })
            .await;
        trace!(parent_id = %ctx.parent_id, %src, "udps peer added");
    }

    let peer = peers.get_mut(&src).expect("peer present after admit");
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
    sync_framer_counters(peer);
}

fn sync_framer_counters(peer: &mut PeerEntry) {
    let now_resync = peer.framer.resync_bytes();
    let now_crc = peer.framer.crc_errors();
    let resync_delta = now_resync.saturating_sub(peer.last_resync_total);
    let crc_delta = now_crc.saturating_sub(peer.last_crc_total);
    if resync_delta > 0 {
        peer.stats
            .resync_bytes
            .fetch_add(resync_delta, std::sync::atomic::Ordering::Relaxed);
    }
    if crc_delta > 0 {
        peer.stats
            .crc_errors
            .fetch_add(crc_delta, std::sync::atomic::Ordering::Relaxed);
    }
    peer.last_resync_total = now_resync;
    peer.last_crc_total = now_crc;
}

async fn evict_lru_peer(
    peers: &mut HashMap<SocketAddr, PeerEntry>,
    parent_id: EndpointId,
    event_tx: &mpsc::Sender<EndpointEvent>,
) {
    let Some((&victim, _)) = peers.iter().min_by_key(|(_, e)| e.last_seen) else {
        return;
    };
    let entry = peers.remove(&victim).expect("victim present");
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
        while let Some(frame) = queue.pop() {
            if cancel.is_cancelled() {
                queue.drain_and_discard();
                return;
            }
            match socket.send_to(&frame, peer_addr).await {
                Ok(n) => stats.add_tx_frame(n),
                Err(e) => {
                    warn!(error = %e, peer = %peer_addr, "udps send_to failed; dropping frame");
                }
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => {
                queue.drain_and_discard();
                return;
            }
            _ = queue.wait_for_push() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddrV4, SocketAddrV6};

    #[test]
    fn peer_name_ipv4() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 10), 14550));
        assert_eq!(peer_endpoint_name("bus", addr), "bus/192.168.1.10-14550");
    }

    #[test]
    fn peer_name_ipv6() {
        let addr = SocketAddr::V6(SocketAddrV6::new(
            "2001:db8::1".parse().unwrap(),
            14550,
            0,
            0,
        ));
        assert_eq!(peer_endpoint_name("bus", addr), "bus/[2001:db8::1]-14550");
    }

    #[test]
    fn peer_name_v4_mapped_v6_uses_v6_form() {
        // Dual-stack sockets sometimes deliver IPv4 senders as v4-mapped v6.
        // The name keeps the v6 form (bracketed) — operators looking at stats
        // can tell the difference.
        let addr = SocketAddr::V6(SocketAddrV6::new(
            Ipv4Addr::LOCALHOST.to_ipv6_mapped(),
            14550,
            0,
            0,
        ));
        let name = peer_endpoint_name("bus", addr);
        assert!(name.starts_with("bus/["), "got {name}");
        assert!(name.ends_with("]-14550"), "got {name}");
    }

    #[test]
    fn config_defaults_when_query_empty() {
        let q = BTreeMap::new();
        let cfg = UdpServerConfig::from_query(&q).unwrap();
        assert_eq!(cfg.idle_secs, DEFAULT_IDLE_SECS);
        assert_eq!(cfg.peer_capacity, DEFAULT_PEER_CAPACITY);
        assert_eq!(cfg.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(cfg.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
    }

    #[test]
    fn config_overrides_from_query() {
        let mut q = BTreeMap::new();
        q.insert("idle_secs".into(), "10".into());
        q.insert("udps_peer_capacity".into(), "4".into());
        q.insert("read_buf_bytes".into(), "1024".into());
        q.insert("tx_queue_frames".into(), "8".into());
        let cfg = UdpServerConfig::from_query(&q).unwrap();
        assert_eq!(cfg.idle_secs, 10);
        assert_eq!(cfg.peer_capacity, 4);
        assert_eq!(cfg.read_buf_bytes, 1024);
        assert_eq!(cfg.tx_queue_frames, 8);
    }

    #[test]
    fn config_zero_idle_secs_rejected() {
        let mut q = BTreeMap::new();
        q.insert("idle_secs".into(), "0".into());
        assert!(matches!(
            UdpServerConfig::from_query(&q),
            Err(UdpServerError::InvalidQuery(_))
        ));
    }

    #[test]
    fn config_zero_peer_capacity_rejected() {
        let mut q = BTreeMap::new();
        q.insert("udps_peer_capacity".into(), "0".into());
        assert!(matches!(
            UdpServerConfig::from_query(&q),
            Err(UdpServerError::InvalidQuery(_))
        ));
    }

    #[test]
    fn config_non_numeric_rejected() {
        let mut q = BTreeMap::new();
        q.insert("idle_secs".into(), "ten".into());
        assert!(matches!(
            UdpServerConfig::from_query(&q),
            Err(UdpServerError::InvalidQuery(_))
        ));
    }

    fn dummy_peer(child_id: EndpointId, age: Duration) -> PeerEntry {
        let stats = Arc::new(EndpointStats::new());
        PeerEntry {
            child_id,
            framer: Framer::new(),
            last_seen: Instant::now() - age,
            last_resync_total: 0,
            last_crc_total: 0,
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

    #[tokio::test]
    async fn sync_framer_counters_propagates_deltas() {
        // Feed deliberate garbage into a fresh framer and verify resync_bytes
        // makes it into the per-peer stats. The framer itself is unit-tested
        // separately; here we only assert plumbing.
        let mut peer = dummy_peer(EndpointId(0), Duration::ZERO);
        peer.framer.buffer_mut().extend_from_slice(&[0, 0, 0, 0]);
        // Drain (will count 4 resync bytes and return None).
        while peer.framer.try_next_frame().is_some() {}
        assert_eq!(peer.framer.resync_bytes(), 4);
        sync_framer_counters(&mut peer);
        assert_eq!(
            peer.stats
                .resync_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            4
        );
        // Second call without further framer activity is a no-op.
        sync_framer_counters(&mut peer);
        assert_eq!(
            peer.stats
                .resync_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            4
        );
    }
}
