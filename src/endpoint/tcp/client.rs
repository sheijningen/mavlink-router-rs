use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::net::{TcpStream, lookup_host};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, trace, warn};

use super::super::EndpointId;
use super::super::backoff::Backoff;
use super::super::defaults::{
    DEFAULT_READ_BUF_BYTES, DEFAULT_RECONNECT_INITIAL_MS, DEFAULT_RECONNECT_MAX_MS,
    DEFAULT_TX_QUEUE_FRAMES,
};
use super::super::events::RouterFrame;
use super::super::identity_flags::IdentityFlags;
use super::super::session::{SessionOutcome, run_session};
use super::super::socket::configure_tcp_stream;
use super::super::spec::TcpClientEndpoint;
use super::super::stats::{EndpointState, EndpointStats};
use super::super::tx_queue::TxQueue;
use super::super::wait_or_cancel;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Typed-empty return for `tcpc:` `run()`. Connect failures enter the
/// capped-exp backoff loop, DNS failures log and retry, and disconnects are
/// handled by the session — no terminal failure modes remain in v1. Kept as
/// a typed return for symmetry with [`super::server::TcpServerError`] and
/// the UDP endpoint modules in case a fatal case shows up later.
#[derive(Debug, Error)]
pub enum TcpClientError {}

/// Inputs that distinguish one `tcpc:` endpoint from another: where to dial,
/// what to call it, and the per-endpoint knobs from the query string with
/// CLAUDE.md defaults already substituted. `identity` carries the filter /
/// sniffer / group / capacity bundle — unused today, threaded so Phase 5 can
/// wire it up without a spawner rework (CLAUDE.md "Filters, group, sniffer,
/// and learn/seq capacities travel with the `*Spec`").
pub struct TcpClientSpec {
    pub host: String,
    pub port: u16,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub read_buf_bytes: usize,
    pub tx_queue_frames: usize,
    pub identity: IdentityFlags,
}

impl TcpClientSpec {
    /// Build a runtime `TcpClientSpec` from the parsed-but-not-defaulted
    /// `TcpClientEndpoint` the CLI/TOML layer produced, substituting CLAUDE.md
    /// defaults for any unset knob. The spawner supplies `endpoint_id` and
    /// `name` because the parser doesn't allocate IDs.
    pub fn from_endpoint(ep: TcpClientEndpoint, endpoint_id: EndpointId, name: String) -> Self {
        Self {
            host: ep.host,
            port: ep.port,
            endpoint_id,
            name,
            reconnect_initial_ms: ep
                .reconnect_initial_ms
                .unwrap_or(DEFAULT_RECONNECT_INITIAL_MS),
            reconnect_max_ms: ep.reconnect_max_ms.unwrap_or(DEFAULT_RECONNECT_MAX_MS),
            read_buf_bytes: ep.common.read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES),
            tx_queue_frames: ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
            identity: ep.identity,
        }
    }
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
pub async fn run(spec: TcpClientSpec, wiring: TcpClientWiring) -> Result<(), TcpClientError> {
    let span = info_span!("tcpc", name = %spec.name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: TcpClientSpec, wiring: TcpClientWiring) -> Result<(), TcpClientError> {
    let TcpClientSpec {
        host,
        port,
        endpoint_id,
        name: _,
        reconnect_initial_ms,
        reconnect_max_ms,
        read_buf_bytes,
        tx_queue_frames: _,
        identity: _,
    } = spec;
    let TcpClientWiring {
        frame_tx,
        tx_queue,
        stats,
        cancel,
    } = wiring;

    let mut backoff = Backoff::new(reconnect_initial_ms, reconnect_max_ms);

    loop {
        if cancel.is_cancelled() {
            tx_queue.drain_and_discard();
            return Ok(());
        }

        let stream = match dial_with_dns(&host, port, &cancel).await {
            DialOutcome::Connected(s) => s,
            DialOutcome::Cancelled => {
                tx_queue.drain_and_discard();
                return Ok(());
            }
            DialOutcome::Failed => {
                if !wait_or_cancel(&cancel, backoff.next_delay()).await {
                    tx_queue.drain_and_discard();
                    return Ok(());
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
        stats.store_state(EndpointState::Connected);

        match run_session(
            stream,
            endpoint_id,
            &stats,
            &frame_tx,
            &tx_queue,
            &cancel,
            read_buf_bytes,
        )
        .await
        {
            SessionOutcome::Terminated => {
                tx_queue.drain_and_discard();
                return Ok(());
            }
            SessionOutcome::Disconnected => {
                stats.store_state(EndpointState::Reconnecting);
                continue;
            }
        }
    }
}

/// Outcome of one DNS-resolve + connect-attempt cycle. `Cancelled` is
/// surfaced as a distinct variant (not a `None` lumped together with connect
/// failure) so the caller can drain and return immediately instead of waiting
/// out the backoff sleep.
enum DialOutcome {
    Connected(TcpStream),
    Cancelled,
    Failed,
}

/// Resolve `host:port` (parsing IP literals directly so IPv6 literals don't
/// need bracket gymnastics) and try each resolved address in sorted order
/// (IPv4 first on ties), returning the first connection that succeeds. Each
/// individual connect is wrapped in a 10s timeout AND races against the
/// cancellation token so shutdown bounds at the drain budget. Returns `Failed`
/// only after every resolved address has been tried — the outer loop then
/// waits a backoff interval before re-resolving.
async fn dial_with_dns(host: &str, port: u16, cancel: &CancellationToken) -> DialOutcome {
    let resolved = resolve_to_socket_addrs(host, port).await;
    if resolved.is_empty() {
        return DialOutcome::Failed;
    }
    for target in resolved {
        match connect_one(target, cancel).await {
            DialOutcome::Connected(s) => {
                trace!(%target, "tcpc connected");
                return DialOutcome::Connected(s);
            }
            DialOutcome::Cancelled => return DialOutcome::Cancelled,
            DialOutcome::Failed => continue,
        }
    }
    DialOutcome::Failed
}

async fn connect_one(target: SocketAddr, cancel: &CancellationToken) -> DialOutcome {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => DialOutcome::Cancelled,
        res = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(target)) => match res {
            Ok(Ok(s)) => DialOutcome::Connected(s),
            Ok(Err(e)) => {
                warn!(error = %e, %target, "tcpc connect failed");
                DialOutcome::Failed
            }
            Err(_) => {
                warn!(%target, "tcpc connect timed out");
                DialOutcome::Failed
            }
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn spec_defaults_when_endpoint_unset() {
        let ep = TcpClientEndpoint::default();
        let spec = TcpClientSpec::from_endpoint(ep, EndpointId(0), "n".into());
        assert_eq!(spec.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(spec.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
        assert_eq!(spec.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(spec.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
    }

    #[test]
    fn spec_overrides_from_endpoint() {
        use crate::endpoint::spec::CommonQuery;
        let ep = TcpClientEndpoint {
            reconnect_initial_ms: Some(50),
            reconnect_max_ms: Some(2000),
            common: CommonQuery {
                read_buf_bytes: Some(1024),
                tx_queue_frames: Some(8),
            },
            ..TcpClientEndpoint::default()
        };
        let spec = TcpClientSpec::from_endpoint(ep, EndpointId(0), "n".into());
        assert_eq!(spec.reconnect_initial_ms, 50);
        assert_eq!(spec.reconnect_max_ms, 2000);
        assert_eq!(spec.read_buf_bytes, 1024);
        assert_eq!(spec.tx_queue_frames, 8);
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

    /// Cancellation set before the call must short-circuit `connect_one`
    /// without dispatching the connect syscall — proves the `cancel.cancelled()`
    /// arm of the inner `select!` is wired through.
    #[tokio::test]
    async fn connect_one_returns_cancelled_when_cancel_already_fired() {
        // Use a refused port on loopback so that, were the cancel arm broken,
        // we'd see a fast `DialOutcome::Failed` and the assert below would
        // still catch the misbehaviour.
        let target: SocketAddr = "127.0.0.1:1".parse().expect("parse target");
        let cancel = CancellationToken::new();
        cancel.cancel();
        let outcome = connect_one(target, &cancel).await;
        assert!(
            matches!(outcome, DialOutcome::Cancelled),
            "expected Cancelled when cancel.is_cancelled()"
        );
    }

    /// `connect_one` must independently report Failed/Connected back-to-back
    /// on the same `CancellationToken` — the dial loop relies on a Failed
    /// outcome being recoverable so iteration over the resolved address list
    /// can keep going.
    #[tokio::test]
    async fn connect_one_failures_and_successes_are_independent() {
        let refused: SocketAddr = "127.0.0.1:1".parse().expect("parse refused");
        let cancel = CancellationToken::new();
        let outcome_failed = connect_one(refused, &cancel).await;
        assert!(
            matches!(outcome_failed, DialOutcome::Failed),
            "expected Failed for connection-refused"
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind probe");
        let live_addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let outcome_ok = connect_one(live_addr, &cancel).await;
        assert!(
            matches!(outcome_ok, DialOutcome::Connected(_)),
            "expected Connected on live loopback"
        );
    }
}
