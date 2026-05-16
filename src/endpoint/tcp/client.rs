use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpStream, lookup_host};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{trace, warn};

use super::super::EndpointId;
use super::super::backoff::Backoff;
use super::super::events::RouterFrame;
use super::super::socket::configure_tcp_stream;
use super::super::spec::TcpClientEndpoint;
use super::super::stats::EndpointStats;
use super::super::tx_queue::TxQueue;
use super::session::{SessionOutcome, run_session};

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
