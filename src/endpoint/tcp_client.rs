use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, lookup_host};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use super::EndpointId;
use super::backoff::Backoff;
use super::events::RouterFrame;
use super::socket::configure_tcp_stream;
use super::spec::TcpClientEndpoint;
use super::stats::EndpointStats;
use super::tx_queue::TxQueue;
use crate::mavlink::framer::Framer;

const DEFAULT_RECONNECT_INITIAL_MS: u64 = 250;
const DEFAULT_RECONNECT_MAX_MS: u64 = 30_000;
const DEFAULT_READ_BUF_BYTES: usize = 8192;
const DEFAULT_TX_QUEUE_FRAMES: usize = 256;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-endpoint runtime configuration. The spec parser hands us a fully-typed
/// `TcpClientEndpoint`; this struct collapses the optional knobs down to the
/// concrete values the task actually uses, substituting CLAUDE.md defaults
/// where the user left a knob unset.
#[derive(Debug, Clone, Copy)]
pub struct TcpClientConfig {
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub read_buf_bytes: usize,
    pub tx_queue_frames: usize,
}

impl Default for TcpClientConfig {
    fn default() -> Self {
        Self {
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
            read_buf_bytes: DEFAULT_READ_BUF_BYTES,
            tx_queue_frames: DEFAULT_TX_QUEUE_FRAMES,
        }
    }
}

impl TcpClientConfig {
    pub fn from_endpoint(ep: &TcpClientEndpoint) -> Self {
        Self {
            reconnect_initial_ms: ep
                .reconnect_initial_ms
                .unwrap_or(DEFAULT_RECONNECT_INITIAL_MS),
            reconnect_max_ms: ep.reconnect_max_ms.unwrap_or(DEFAULT_RECONNECT_MAX_MS),
            read_buf_bytes: ep.common.read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES),
            tx_queue_frames: ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
        }
    }
}

/// Inputs that distinguish one `tcpc:` endpoint from another: where to dial,
/// what to call it, and the per-endpoint knobs from the query string.
pub struct TcpClientSpec {
    pub host: String,
    pub port: u16,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub cfg: TcpClientConfig,
}

/// Shared wiring a `tcpc:` task needs. The TxQueue and stats are constructed
/// by the spawner so the router can hold its own clones before this task
/// starts running.
pub struct TcpClientWiring {
    pub frame_tx: mpsc::Sender<RouterFrame>,
    pub tx_queue: TxQueue,
    pub stats: Arc<EndpointStats>,
    pub cancel: CancellationToken,
}

/// Why a TCP session terminated — controls whether the outer loop reconnects
/// (Disconnected) or returns (Cancelled).
enum SessionOutcome {
    Cancelled,
    Disconnected,
}

/// Run a `tcpc:` endpoint until the cancellation token fires. Resolves DNS,
/// dials with capped-exponential backoff + ±20% jitter, and on every
/// successful connect drains the TxQueue (frames buffered during the outage
/// are stale — CLAUDE.md "TX queue on disconnect: drain and discard"). Each
/// session reads inbound bytes through a fresh `Framer` and writes outbound
/// frames pulled from the TxQueue.
pub async fn run(spec: TcpClientSpec, wiring: TcpClientWiring) {
    let TcpClientSpec {
        host,
        port,
        endpoint_id,
        name: _,
        cfg,
    } = spec;
    let TcpClientWiring {
        frame_tx,
        tx_queue,
        stats,
        cancel,
    } = wiring;

    let mut backoff = Backoff::new(cfg.reconnect_initial_ms, cfg.reconnect_max_ms);

    loop {
        if cancel.is_cancelled() {
            tx_queue.drain_and_discard();
            return;
        }

        let stream = match dial_with_dns(&host, port).await {
            Some(s) => s,
            None => {
                if !wait_or_cancel(&cancel, backoff.next_delay()).await {
                    tx_queue.drain_and_discard();
                    return;
                }
                continue;
            }
        };

        if let Err(e) = configure_tcp_stream(&stream) {
            warn!(error = %e, "tcpc configure_tcp_stream failed");
        }

        backoff.reset();
        let drained = tx_queue.drain_and_discard();
        if drained > 0 {
            trace!(drained, "tcpc drained stale frames before resuming");
        }

        match run_session(
            stream,
            endpoint_id,
            &stats,
            &frame_tx,
            &tx_queue,
            &cancel,
            cfg.read_buf_bytes,
        )
        .await
        {
            SessionOutcome::Cancelled => {
                tx_queue.drain_and_discard();
                return;
            }
            SessionOutcome::Disconnected => {
                continue;
            }
        }
    }
}

/// Resolve `host:port` (parsing IP literals directly so IPv6 literals don't
/// need bracket gymnastics) and dial the first resolved address, preferring
/// IPv4 on ties. Returns `None` on resolution or connect failure — the outer
/// loop retries with backoff.
async fn dial_with_dns(host: &str, port: u16) -> Option<TcpStream> {
    let resolved = resolve_to_socket_addrs(host, port).await;
    if resolved.is_empty() {
        return None;
    }
    let target = resolved[0];

    match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(target)).await {
        Ok(Ok(s)) => {
            trace!(%target, "tcpc connected");
            Some(s)
        }
        Ok(Err(e)) => {
            warn!(error = %e, %target, "tcpc connect failed");
            None
        }
        Err(_) => {
            warn!(%target, "tcpc connect timed out");
            None
        }
    }
}

async fn resolve_to_socket_addrs(host: &str, port: u16) -> Vec<SocketAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return vec![SocketAddr::new(ip, port)];
    }
    let target = format!("{host}:{port}");
    match lookup_host(target.as_str()).await {
        Ok(addrs) => {
            let mut all: Vec<SocketAddr> = addrs.collect();
            // CLAUDE.md: "prefer v4 on tie, since most MAVLink ecosystems are v4-only".
            all.sort_by_key(|sa| match sa.ip() {
                IpAddr::V4(_) => 0u8,
                IpAddr::V6(_) => 1u8,
            });
            if all.is_empty() {
                warn!(%host, "tcpc DNS resolution returned no addresses");
            }
            all
        }
        Err(e) => {
            warn!(error = %e, %host, "tcpc DNS resolution failed");
            Vec::new()
        }
    }
}

async fn run_session(
    stream: TcpStream,
    endpoint_id: EndpointId,
    stats: &Arc<EndpointStats>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    tx_queue: &TxQueue,
    cancel: &CancellationToken,
    read_buf_bytes: usize,
) -> SessionOutcome {
    let (mut rh, mut wh) = stream.into_split();
    let mut framer = Framer::with_capacity(read_buf_bytes);
    let mut last_resync_total: u64 = 0;
    let mut last_crc_total: u64 = 0;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return SessionOutcome::Cancelled,
            res = rh.read_buf(framer.buffer_mut()) => {
                match res {
                    Ok(0) => {
                        debug!("tcpc peer closed connection");
                        return SessionOutcome::Disconnected;
                    }
                    Ok(_) => {
                        while let Some((header, frame)) = framer.try_next_frame() {
                            let frame_len = frame.len();
                            stats.add_rx_frame(frame_len);
                            if frame_tx
                                .send(RouterFrame {
                                    endpoint_id,
                                    frame,
                                    header,
                                })
                                .await
                                .is_err()
                            {
                                debug!("tcpc router channel closed; ending");
                                return SessionOutcome::Cancelled;
                            }
                        }
                        sync_framer_counters(
                            &framer,
                            &mut last_resync_total,
                            &mut last_crc_total,
                            stats,
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, "tcpc read failed");
                        return SessionOutcome::Disconnected;
                    }
                }
            }
            frame = pop_or_wait(tx_queue) => {
                if let Err(e) = wh.write_all(&frame).await {
                    warn!(error = %e, "tcpc write failed");
                    return SessionOutcome::Disconnected;
                }
                stats.add_tx_frame(frame.len());
            }
        }
    }
}

async fn pop_or_wait(q: &TxQueue) -> Bytes {
    loop {
        if let Some(b) = q.pop() {
            return b;
        }
        q.wait_for_push().await;
    }
}

fn sync_framer_counters(
    framer: &Framer,
    last_resync_total: &mut u64,
    last_crc_total: &mut u64,
    stats: &Arc<EndpointStats>,
) {
    let now_resync = framer.resync_bytes();
    let now_crc = framer.crc_errors();
    let resync_delta = now_resync.saturating_sub(*last_resync_total);
    let crc_delta = now_crc.saturating_sub(*last_crc_total);
    if resync_delta > 0 {
        stats
            .resync_bytes
            .fetch_add(resync_delta, Ordering::Relaxed);
    }
    if crc_delta > 0 {
        stats.crc_errors.fetch_add(crc_delta, Ordering::Relaxed);
    }
    *last_resync_total = now_resync;
    *last_crc_total = now_crc;
}

async fn wait_or_cancel(cancel: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        _ = sleep(delay) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn config_defaults_when_endpoint_unset() {
        let ep = TcpClientEndpoint::default();
        let cfg = TcpClientConfig::from_endpoint(&ep);
        assert_eq!(cfg.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(cfg.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
        assert_eq!(cfg.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(cfg.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
    }

    #[test]
    fn config_overrides_from_endpoint() {
        use crate::endpoint::spec::CommonQuery;
        let ep = TcpClientEndpoint {
            reconnect_initial_ms: Some(50),
            reconnect_max_ms: Some(2000),
            common: CommonQuery {
                read_buf_bytes: Some(1024),
                tx_queue_frames: Some(8),
                ..CommonQuery::default()
            },
            ..TcpClientEndpoint::default()
        };
        let cfg = TcpClientConfig::from_endpoint(&ep);
        assert_eq!(cfg.reconnect_initial_ms, 50);
        assert_eq!(cfg.reconnect_max_ms, 2000);
        assert_eq!(cfg.read_buf_bytes, 1024);
        assert_eq!(cfg.tx_queue_frames, 8);
    }

    #[tokio::test]
    async fn resolve_ipv4_literal_short_circuits_dns() {
        let v = resolve_to_socket_addrs("127.0.0.1", 5760).await;
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(v[0].port(), 5760);
    }

    #[tokio::test]
    async fn resolve_ipv6_literal_short_circuits_dns() {
        let v = resolve_to_socket_addrs("::1", 5760).await;
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(v[0].port(), 5760);
    }

    #[tokio::test]
    async fn sync_framer_counters_propagates_deltas() {
        let stats = Arc::new(EndpointStats::new());
        let mut framer = Framer::new();
        framer.buffer_mut().extend_from_slice(&[0, 0, 0, 0]);
        while framer.try_next_frame().is_some() {}
        assert_eq!(framer.resync_bytes(), 4);
        let mut last_resync = 0u64;
        let mut last_crc = 0u64;
        sync_framer_counters(&framer, &mut last_resync, &mut last_crc, &stats);
        assert_eq!(stats.resync_bytes.load(Ordering::Relaxed), 4);
        sync_framer_counters(&framer, &mut last_resync, &mut last_crc, &stats);
        assert_eq!(stats.resync_bytes.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn wait_or_cancel_returns_false_when_cancelled() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let r = wait_or_cancel(&cancel, Duration::from_secs(60)).await;
        assert!(!r);
    }

    #[tokio::test]
    async fn wait_or_cancel_returns_true_after_delay() {
        let cancel = CancellationToken::new();
        let start = tokio::time::Instant::now();
        let r = wait_or_cancel(&cancel, Duration::from_millis(50)).await;
        assert!(r);
        assert!(start.elapsed() >= Duration::from_millis(50));
    }
}
