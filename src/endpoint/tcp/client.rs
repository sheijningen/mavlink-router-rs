use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::net::{TcpStream, lookup_host};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

use super::super::EndpointId;
use super::super::backoff::Backoff;
use super::super::defaults::{DEFAULT_RECONNECT_INITIAL_MS, DEFAULT_RECONNECT_MAX_MS};
use super::super::identity_flags::IdentityFlags;
use super::super::session::{SessionCtx, SessionOutcome, run_session};
use super::super::socket::configure_tcp_stream;
use super::super::spec::TcpClientEndpoint;
use super::super::stats::EndpointState;
use super::super::wait_or_cancel;
use super::super::wiring::ClientWiring;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Inputs that distinguish one `tcpc:` endpoint from another: where to dial,
/// what to call it, and the reconnect curve.
pub struct TcpClientSpec {
    pub host: String,
    pub port: u16,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub identity: IdentityFlags,
}

impl TcpClientSpec {
    /// Build a runtime `TcpClientSpec` from the parsed `TcpClientEndpoint`,
    /// stamping the hardcoded reconnect curve. The spawner supplies
    /// `endpoint_id` and `name` because the parser doesn't allocate IDs.
    pub fn from_endpoint(
        endpoint: TcpClientEndpoint,
        endpoint_id: EndpointId,
        name: String,
    ) -> Self {
        Self {
            host: endpoint.host,
            port: endpoint.port,
            endpoint_id,
            name,
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
            identity: endpoint.identity,
        }
    }
}

/// Run a `tcpc:` endpoint until the cancellation token fires. Resolves DNS,
/// dials with capped-exponential backoff + ±20% jitter, and on every
/// successful connect drains the TxQueue (buffered frames are stale). Each
/// session reads inbound bytes through a fresh `Framer` and writes outbound
/// frames pulled from the TxQueue.
pub async fn run(spec: TcpClientSpec, wiring: ClientWiring) {
    let span = info_span!("tcpc", name = %spec.name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: TcpClientSpec, wiring: ClientWiring) {
    let TcpClientSpec {
        host,
        port,
        endpoint_id,
        name: _,
        reconnect_initial_ms,
        reconnect_max_ms,
        identity,
    } = spec;
    let ClientWiring {
        frame_tx,
        tx_queue,
        stats,
        cancel,
    } = wiring;

    let mut backoff = Backoff::new(reconnect_initial_ms, reconnect_max_ms);

    loop {
        if cancel.is_cancelled() {
            tx_queue.drain_and_discard();
            return;
        }

        let stream = match dial_with_dns(&host, port, &cancel).await {
            DialOutcome::Connected(stream) => stream,
            DialOutcome::Cancelled => {
                tx_queue.drain_and_discard();
                return;
            }
            DialOutcome::Failed => {
                if !wait_or_cancel(&cancel, backoff.next_delay()).await {
                    tx_queue.drain_and_discard();
                    return;
                }
                continue;
            }
        };

        if let Err(err) = configure_tcp_stream(&stream) {
            warn!(error = %err, "configure_tcp_stream failed");
        }

        backoff.reset();
        let drained = tx_queue.drain_and_discard();
        if drained > 0 {
            debug!(drained, "drained stale frames before resuming");
        }
        stats.store_state(EndpointState::Connected);

        let ctx = SessionCtx {
            endpoint_id,
            stats: &stats,
            frame_tx: &frame_tx,
            filters: &identity.filters,
        };
        match run_session(stream, &ctx, &tx_queue, &cancel).await {
            SessionOutcome::Terminated => {
                let drained = tx_queue.drain_and_discard();
                if drained > 0 {
                    debug!(drained, "discarded in-flight frames on session terminate");
                }
                return;
            }
            SessionOutcome::Disconnected => {
                info!("disconnected; reconnecting");
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
            DialOutcome::Connected(stream) => {
                info!(%target, "connected");
                return DialOutcome::Connected(stream);
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
            Ok(Ok(stream)) => DialOutcome::Connected(stream),
            Ok(Err(err)) => {
                warn!(error = %err, %target, "connect failed");
                DialOutcome::Failed
            }
            Err(_) => {
                warn!(%target, "connect timed out");
                DialOutcome::Failed
            }
        },
    }
}

async fn resolve_to_socket_addrs(host: &str, port: u16) -> Vec<SocketAddr> {
    if let Ok(ip_addr) = host.parse::<IpAddr>() {
        return vec![SocketAddr::new(ip_addr, port)];
    }
    let target = format!("{host}:{port}");
    match lookup_host(target.as_str()).await {
        Ok(addrs) => {
            let mut all: Vec<SocketAddr> = addrs.collect();
            // Prefer v4 on tie — most MAVLink ecosystems are v4-only.
            all.sort_by_key(|addr| match addr.ip() {
                IpAddr::V4(_) => 0u8,
                IpAddr::V6(_) => 1u8,
            });
            if all.is_empty() {
                warn!(%host, "DNS resolution returned no addresses");
            }
            all
        }
        Err(err) => {
            warn!(error = %err, %host, "DNS resolution failed");
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
        let endpoint = TcpClientEndpoint::default();
        let spec = TcpClientSpec::from_endpoint(endpoint, EndpointId(0), "n".into());
        assert_eq!(spec.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(spec.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    #[tokio::test]
    async fn resolve_ipv4_literal_short_circuits_dns() {
        let addrs = resolve_to_socket_addrs("127.0.0.1", 5760).await;
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(addrs[0].port(), 5760);
    }

    #[tokio::test]
    async fn resolve_ipv6_literal_short_circuits_dns() {
        let addrs = resolve_to_socket_addrs("::1", 5760).await;
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addrs[0].port(), 5760);
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
