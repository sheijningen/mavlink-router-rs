use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use tokio::net::{UdpSocket, lookup_host};
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

use super::super::EndpointId;
use super::super::backoff::{Backoff, BindOutcome, bind_with_backoff};
use super::super::defaults::{
    DEFAULT_RECONNECT_INITIAL_MS, DEFAULT_RECONNECT_MAX_MS, READ_BUF_BYTES,
};
use super::super::events::RouterFrame;
use super::super::filters::Filters;
use super::super::identity_flags::{IdentityFlags, SEQ_TRACKER_CAPACITY};
use super::super::seq_tracker::SeqTracker;
use super::super::session::forward_inbound_frames;
use super::super::socket::bind_udp_dual_stack;
use super::super::spec::UdpClientEndpoint;
use super::super::stats::{EndpointState, EndpointStats, FramerCounters};
use super::super::tx_queue::TxQueue;
use crate::mavlink::framer::Framer;

const DEFAULT_LATCH_IDLE_SECS: u64 = 30;

/// `latch_idle_secs` lower bound — 0 would revert the latch on the very
/// next REVERT_TICK, defeating the latching mechanism entirely.
pub const MIN_LATCH_IDLE_SECS: u64 = 1;

/// `latch_idle_secs` upper bound (24 hours). Same rationale as
/// [`super::server::MAX_IDLE_SECS`]: a day is effectively "never revert".
pub const MAX_LATCH_IDLE_SECS: u64 = 86_400;
// Max IP datagram payload plus headroom; matches `udps:` for symmetry so
// neither side truncates an oversized inbound packet.
const MAX_DATAGRAM_BYTES: usize = 65_536;
const REVERT_TICK: Duration = Duration::from_secs(1);

/// Inputs that distinguish one `udpc:` endpoint from another: where to send,
/// what to call it, and the latch-idle threshold. The `reconnect_*_ms`
/// fields are always the hardcoded `tcpc:` curve (CLAUDE.md "Hardcoded
/// plumbing knobs"); `udpc:` reuses the curve for its local-bind retry.
/// `identity` carries the filter / sniffer / group bundle (CLAUDE.md
/// "Filters, group, sniffer travel with the `*Spec`"); the reader applies
/// the in-filter snapshot, the router applies out-filter / sniffer / group
/// from the same bundle.
pub struct UdpClientSpec {
    pub host: String,
    pub port: u16,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub latch_idle_secs: u64,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub identity: IdentityFlags,
}

impl UdpClientSpec {
    /// Build a runtime `UdpClientSpec` from the parsed-but-not-defaulted
    /// `UdpClientEndpoint` the CLI/TOML layer produced, substituting CLAUDE.md
    /// defaults for any unset knob. The spawner supplies `endpoint_id` and
    /// `name` because the parser doesn't allocate IDs. The TxQueue's depth
    /// (`tx_queue_frames`) is consumed by the spawner before the spec is
    /// built — it sizes the queue and never appears here.
    pub fn from_endpoint(
        endpoint: UdpClientEndpoint,
        endpoint_id: EndpointId,
        name: String,
    ) -> Self {
        Self {
            host: endpoint.host,
            port: endpoint.port,
            endpoint_id,
            name,
            latch_idle_secs: endpoint.latch_idle_secs.unwrap_or(DEFAULT_LATCH_IDLE_SECS),
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
            identity: endpoint.identity,
        }
    }
}

/// Shared wiring a `udpc:` task needs. The TxQueue and stats are constructed
/// by the spawner so the router can hold its own clones before this task
/// starts running.
pub struct UdpClientWiring {
    pub frame_tx: mpsc::Sender<RouterFrame>,
    pub tx_queue: TxQueue,
    pub stats: Arc<EndpointStats>,
    pub cancel: CancellationToken,
}

/// Overlay applied on top of the configured destination once an accepted
/// inbound packet has fixed the peer's exact `(ip, port)`. Cleared on idle
/// revert so the task falls back to the configured `host:port`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LatchInfo {
    addr: SocketAddr,
    last_inbound: Instant,
}

/// Where the `udpc:` task is currently sending frames and which inbound
/// sources it accepts. Held entirely inside the task; the latch overlay
/// supersedes the configured-host resolution when present.
#[derive(Debug, Clone)]
struct Destination {
    host: String,
    port: u16,
    resolved_ips: Vec<IpAddr>,
    latch: Option<LatchInfo>,
}

impl Destination {
    fn current_target(&self) -> Option<SocketAddr> {
        if let Some(latch) = &self.latch {
            return Some(latch.addr);
        }
        self.resolved_ips
            .first()
            .copied()
            .map(|ip| SocketAddr::new(ip, self.port))
    }
}

/// Outcome of evaluating one inbound packet against the current latch and
/// resolved-IP set — whether to admit it (and how to update the latch) or
/// drop it as unrelated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InboundDecision {
    /// Configured state, source IP is in `resolved_ips`: accept and latch.
    AcceptAndLatch,
    /// Already latched on this source IP: accept and refresh latch (may
    /// also pick up a new ephemeral port).
    AcceptUpdate,
    /// Source IP unknown — drop and count.
    Reject,
}

fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        ip => ip,
    }
}

fn classify_inbound(dest: &Destination, src_ip: IpAddr) -> InboundDecision {
    let src = canonical_ip(src_ip);
    if let Some(latch) = &dest.latch {
        if canonical_ip(latch.addr.ip()) == src {
            return InboundDecision::AcceptUpdate;
        }
        return InboundDecision::Reject;
    }
    if dest
        .resolved_ips
        .iter()
        .any(|&ip_addr| canonical_ip(ip_addr) == src)
    {
        InboundDecision::AcceptAndLatch
    } else {
        InboundDecision::Reject
    }
}

/// Choose a local bind address that can reach all of `resolved_ips`. If any
/// resolved IP is IPv6 we bind dual-stack `[::]:0`; otherwise plain `0.0.0.0:0`.
/// An empty input — meaning DNS has not yet succeeded — defaults to `[::]:0`
/// so we can adapt to whichever family resolves first.
fn pick_local_bind(resolved_ips: &[IpAddr]) -> SocketAddr {
    if resolved_ips.is_empty() || resolved_ips.iter().any(|ip_addr| ip_addr.is_ipv6()) {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    }
}

async fn resolve_host(host: &str, port: u16) -> Vec<IpAddr> {
    let target = format!("{host}:{port}");
    match lookup_host(target.as_str()).await {
        Ok(addrs) => {
            let ips: Vec<IpAddr> = addrs.map(|addr| addr.ip()).collect();
            if ips.is_empty() {
                warn!(%host, "udpc DNS resolution returned no addresses");
            }
            ips
        }
        Err(err) => {
            warn!(error = %err, %host, "udpc DNS resolution failed");
            Vec::new()
        }
    }
}

/// Run a `udpc:` endpoint until the cancellation token fires. Binds a local
/// socket suitable for the host's resolved family, retrying with the shared
/// capped-exp backoff on failure (CLAUDE.md "Bind/open failure at startup is
/// not fatal"); performs an initial DNS resolution (failure is non-fatal —
/// retried on first send/inbound); then loops over inbound, the revert tick,
/// the TX queue, and cancellation.
pub async fn run(spec: UdpClientSpec, wiring: UdpClientWiring) {
    let span = info_span!("udpc", name = %spec.name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: UdpClientSpec, wiring: UdpClientWiring) {
    let UdpClientSpec {
        host,
        port,
        endpoint_id,
        name: _,
        latch_idle_secs,
        reconnect_initial_ms,
        reconnect_max_ms,
        identity,
    } = spec;
    let UdpClientWiring {
        frame_tx,
        tx_queue,
        stats,
        cancel,
    } = wiring;

    let initial_ips = resolve_host(&host, port).await;
    let mut dest = Destination {
        host,
        port,
        resolved_ips: initial_ips,
        latch: None,
    };

    let mut backoff = Backoff::new(reconnect_initial_ms, reconnect_max_ms);
    let local_bind = pick_local_bind(&dest.resolved_ips);
    let socket = match bind_with_backoff(&cancel, &mut backoff, "udpc local", local_bind, || {
        bind_udp_dual_stack(local_bind)
    })
    .await
    {
        BindOutcome::Bound(socket) => socket,
        BindOutcome::Cancelled => return,
    };
    // Local bind succeeded. udpc has no transport-up/down event in the
    // socket-lifecycle sense (revert to configured-host is *not* a transport
    // event per CLAUDE.md), but the task does flip back to Reconnecting on
    // send_to errors and DNS-resolve failures that leave us with no target —
    // see `send_frame` for the recovery / failure writes.
    stats.store_state(EndpointState::Connected);

    let mut framer = Framer::with_capacity(READ_BUF_BYTES);
    let mut framer_counters = FramerCounters::new();
    let mut seq_tracker = SeqTracker::new(SEQ_TRACKER_CAPACITY);
    let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];

    let mut revert_tick = interval(REVERT_TICK);
    revert_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // `interval(...)` fires its first tick immediately; skip it so the
    // revert check doesn't run before we've had a chance to latch.
    let _ = revert_tick.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tx_queue.drain_and_discard();
                return;
            }
            _ = revert_tick.tick() => {
                check_latch_idle(&mut dest, Duration::from_secs(latch_idle_secs)).await;
            }
            res = socket.recv_from(&mut buf) => {
                match res {
                    Ok((count, src)) => {
                        handle_inbound(
                            &buf[..count],
                            src,
                            &mut dest,
                            &mut framer,
                            &mut framer_counters,
                            &mut seq_tracker,
                            endpoint_id,
                            &stats,
                            &frame_tx,
                            &identity.filters,
                        )
                        .await;
                    }
                    Err(err) => {
                        warn!(error = %err, "udpc recv_from error");
                    }
                }
            }
            frame = tx_queue.pop_or_wait() => {
                send_frame(&socket, &mut dest, frame, &stats).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_inbound(
    data: &[u8],
    src: SocketAddr,
    dest: &mut Destination,
    framer: &mut Framer,
    framer_counters: &mut FramerCounters,
    seq_tracker: &mut SeqTracker,
    endpoint_id: EndpointId,
    stats: &Arc<EndpointStats>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    filters: &Filters,
) {
    match classify_inbound(dest, src.ip()) {
        InboundDecision::Reject => {
            stats.in_filter_drops.fetch_add(1, Ordering::Relaxed);
            debug!(%src, "udpc inbound from unexpected source dropped");
            return;
        }
        InboundDecision::AcceptAndLatch => {
            dest.latch = Some(LatchInfo {
                addr: src,
                last_inbound: Instant::now(),
            });
            info!(%src, "udpc latched onto reply source");
        }
        InboundDecision::AcceptUpdate => {
            if let Some(latch) = dest.latch.as_mut() {
                latch.addr = src;
                latch.last_inbound = Instant::now();
            }
        }
    }
    // An accepted inbound proves the peer is reachable on this socket — flip
    // the state out of any prior Reconnecting (a previous send_to may have
    // failed before the peer responded over this same path). Idempotent on
    // the common already-Connected case.
    stats.store_state(EndpointState::Connected);

    framer.buffer_mut().extend_from_slice(data);
    // `in_filter_drops` here is the union counter: the same slot bumped by
    // the wrong-source-IP rejection above (CLAUDE.md "In-filter evaluation
    // in the reader task" — `in_filter_drops` is the union of all
    // ingress-side drops).
    let pipeline =
        forward_inbound_frames(framer, stats, endpoint_id, seq_tracker, filters, frame_tx)
            .instrument(tracing::trace_span!("udpc_ingress", %src));
    if pipeline.await.is_break() {
        return;
    }
    framer_counters.sync(framer, stats);
}

async fn send_frame(
    socket: &UdpSocket,
    dest: &mut Destination,
    frame: Bytes,
    stats: &Arc<EndpointStats>,
) {
    let Some(target) = dest.current_target() else {
        debug!(host = %dest.host, "udpc send skipped — no resolved address");
        let fresh = resolve_host(&dest.host, dest.port).await;
        if !fresh.is_empty() {
            dest.resolved_ips = fresh;
        }
        // CLAUDE.md: "writes Reconnecting only on send_to errors and
        // DNS-resolve failures that prevent any send at all". The frame
        // we just skipped is one such failure — if the re-resolve also
        // couldn't produce a target, surface the state to the operator.
        // The next successful send flips back to Connected below.
        if dest.current_target().is_none() {
            stats.store_state(EndpointState::Reconnecting);
        }
        return;
    };
    match socket.send_to(&frame, target).await {
        Ok(bytes_sent) => {
            stats.add_tx_frame(bytes_sent);
            // Idempotent recovery write: cheap relaxed AtomicU8 store, and
            // saves tracking "were we Reconnecting?" on the hot path.
            stats.store_state(EndpointState::Connected);
        }
        Err(err) => {
            warn!(error = %err, %target, "udpc send_to failed; re-resolving for next burst");
            let fresh = resolve_host(&dest.host, dest.port).await;
            if !fresh.is_empty() {
                dest.resolved_ips = fresh;
            }
            stats.store_state(EndpointState::Reconnecting);
        }
    }
}

async fn check_latch_idle(dest: &mut Destination, idle: Duration) {
    let Some(latch) = dest.latch else {
        return;
    };
    if Instant::now().duration_since(latch.last_inbound) < idle {
        return;
    }
    info!(%latch.addr, "udpc latch idle; reverting to configured");
    let fresh = resolve_host(&dest.host, dest.port).await;
    if !fresh.is_empty() {
        dest.resolved_ips = fresh;
    } else {
        warn!(host = %dest.host, "udpc DNS re-resolve failed on revert; keeping previous resolved IPs");
    }
    dest.latch = None;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::filters::{Filters, MsgIdRange};
    use crate::mavlink::crc::Crc16;
    use crate::mavlink::frame::STX_V1;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4};

    fn make_dest(ips: &[IpAddr], port: u16) -> Destination {
        Destination {
            host: "example".to_string(),
            port,
            resolved_ips: ips.to_vec(),
            latch: None,
        }
    }

    fn v4(octet_a: u8, octet_b: u8, octet_c: u8, octet_d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(octet_a, octet_b, octet_c, octet_d))
    }

    #[test]
    fn spec_defaults_when_endpoint_unset() {
        let endpoint = UdpClientEndpoint::default();
        let spec = UdpClientSpec::from_endpoint(endpoint, EndpointId(0), "n".into());
        assert_eq!(spec.latch_idle_secs, DEFAULT_LATCH_IDLE_SECS);
        assert_eq!(spec.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(spec.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    #[test]
    fn spec_overrides_from_endpoint() {
        let endpoint = UdpClientEndpoint {
            latch_idle_secs: Some(5),
            ..UdpClientEndpoint::default()
        };
        let spec = UdpClientSpec::from_endpoint(endpoint, EndpointId(0), "n".into());
        assert_eq!(spec.latch_idle_secs, 5);
    }

    #[test]
    fn classify_configured_matching_ip_latches() {
        let dest = make_dest(&[v4(192, 168, 1, 5)], 14550);
        assert_eq!(
            classify_inbound(&dest, v4(192, 168, 1, 5)),
            InboundDecision::AcceptAndLatch
        );
    }

    #[test]
    fn classify_configured_unrelated_ip_rejected() {
        let dest = make_dest(&[v4(192, 168, 1, 5)], 14550);
        assert_eq!(
            classify_inbound(&dest, v4(10, 0, 0, 9)),
            InboundDecision::Reject
        );
    }

    #[test]
    fn classify_configured_any_resolved_ip_latches() {
        let dest = make_dest(&[v4(192, 168, 1, 5), v4(192, 168, 1, 6)], 14550);
        assert_eq!(
            classify_inbound(&dest, v4(192, 168, 1, 6)),
            InboundDecision::AcceptAndLatch
        );
    }

    #[test]
    fn classify_latched_same_ip_accepts_update() {
        let mut dest = make_dest(&[v4(192, 168, 1, 5)], 14550);
        dest.latch = Some(LatchInfo {
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 5), 50000)),
            last_inbound: Instant::now(),
        });
        // Different port, same IP — accepted.
        assert_eq!(
            classify_inbound(&dest, v4(192, 168, 1, 5)),
            InboundDecision::AcceptUpdate
        );
    }

    #[test]
    fn classify_latched_other_ip_rejected_even_if_in_resolved() {
        let mut dest = make_dest(&[v4(192, 168, 1, 5), v4(192, 168, 1, 6)], 14550);
        dest.latch = Some(LatchInfo {
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 5), 50000)),
            last_inbound: Instant::now(),
        });
        // Even though 192.168.1.6 is in resolved_ips, we're latched on .5
        // so .6 is now Reject. The latch is a hard lock until idle revert.
        assert_eq!(
            classify_inbound(&dest, v4(192, 168, 1, 6)),
            InboundDecision::Reject
        );
    }

    #[test]
    fn canonical_ipv4_mapped_v6_collapses_to_v4() {
        let mapped = IpAddr::V6(Ipv4Addr::new(127, 0, 0, 1).to_ipv6_mapped());
        assert_eq!(canonical_ip(mapped), v4(127, 0, 0, 1));
    }

    #[test]
    fn canonical_pure_v6_unchanged() {
        let v6 = IpAddr::V6("2001:db8::1".parse().unwrap());
        assert_eq!(canonical_ip(v6), v6);
    }

    #[test]
    fn current_target_prefers_latch_over_resolved() {
        let mut dest = make_dest(&[v4(192, 168, 1, 5)], 14550);
        assert_eq!(
            dest.current_target(),
            Some(SocketAddr::new(v4(192, 168, 1, 5), 14550))
        );
        let latched = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 50000));
        dest.latch = Some(LatchInfo {
            addr: latched,
            last_inbound: Instant::now(),
        });
        assert_eq!(dest.current_target(), Some(latched));
    }

    #[test]
    fn current_target_none_when_empty_and_no_latch() {
        let dest = make_dest(&[], 14550);
        assert_eq!(dest.current_target(), None);
    }

    #[test]
    fn pick_local_bind_all_v4_uses_v4() {
        let addr = pick_local_bind(&[v4(192, 168, 1, 1), v4(10, 0, 0, 1)]);
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }

    #[test]
    fn pick_local_bind_any_v6_uses_dual_stack() {
        let v6 = IpAddr::V6("2001:db8::1".parse().unwrap());
        let addr = pick_local_bind(&[v4(192, 168, 1, 1), v6]);
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
    }

    #[test]
    fn pick_local_bind_empty_defaults_to_dual_stack() {
        let addr = pick_local_bind(&[]);
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
    }

    #[tokio::test]
    async fn check_latch_idle_reverts_after_threshold() {
        let mut dest = make_dest(&[v4(127, 0, 0, 1)], 14550);
        dest.latch = Some(LatchInfo {
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 50000)),
            last_inbound: Instant::now() - Duration::from_secs(120),
        });
        check_latch_idle(&mut dest, Duration::from_secs(30)).await;
        assert!(dest.latch.is_none());
    }

    #[tokio::test]
    async fn check_latch_idle_keeps_fresh_latch() {
        let mut dest = make_dest(&[v4(127, 0, 0, 1)], 14550);
        let latched = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 50000));
        dest.latch = Some(LatchInfo {
            addr: latched,
            last_inbound: Instant::now(),
        });
        check_latch_idle(&mut dest, Duration::from_secs(30)).await;
        assert!(dest.latch.is_some());
    }

    /// Successful send flips state to Connected (idempotent recovery path).
    #[tokio::test]
    async fn send_frame_success_writes_connected() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("recv bind");
        let receiver_port = receiver.local_addr().expect("recv addr").port();
        let sender = UdpSocket::bind("127.0.0.1:0").await.expect("send bind");
        let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
        let mut dest = make_dest(&[v4(127, 0, 0, 1)], receiver_port);
        send_frame(&sender, &mut dest, Bytes::from_static(b"hello"), &stats).await;
        assert_eq!(stats.load_state(), EndpointState::Connected);
        assert_eq!(stats.tx_frames.load(Ordering::Relaxed), 1);
    }

    /// send_to to port 0 returns EINVAL on Unix (an invalid destination);
    /// the task writes Reconnecting in response. Gated to unix because
    /// WinSock's sendto-to-port-0 behaviour isn't documented to fail
    /// synchronously.
    #[cfg(unix)]
    #[tokio::test]
    async fn send_frame_send_to_error_writes_reconnecting() {
        let sender = UdpSocket::bind("127.0.0.1:0").await.expect("send bind");
        let stats = Arc::new(EndpointStats::new(EndpointState::Connected));
        // Port 0 as a destination is invalid; send_to returns Err.
        let mut dest = make_dest(&[v4(127, 0, 0, 1)], 0);
        send_frame(&sender, &mut dest, Bytes::from_static(b"hello"), &stats).await;
        assert_eq!(stats.load_state(), EndpointState::Reconnecting);
        assert_eq!(
            stats.tx_frames.load(Ordering::Relaxed),
            0,
            "failed send must not count as tx_frame"
        );
    }

    /// No resolved IPs + DNS re-resolve also fails → Reconnecting. Uses a
    /// `.invalid` host (RFC 2606 reserved-for-non-resolution) so the
    /// lookup is guaranteed to return no addresses.
    #[tokio::test]
    async fn send_frame_no_target_after_failed_reresolve_writes_reconnecting() {
        let sender = UdpSocket::bind("127.0.0.1:0").await.expect("send bind");
        let stats = Arc::new(EndpointStats::new(EndpointState::Connected));
        let mut dest = Destination {
            host: "rmr-test-host-should-not-resolve.invalid".to_string(),
            port: 14550,
            resolved_ips: vec![],
            latch: None,
        };
        send_frame(&sender, &mut dest, Bytes::from_static(b"hello"), &stats).await;
        assert_eq!(stats.load_state(), EndpointState::Reconnecting);
    }

    /// Inverse of the previous test: if `current_target` was None *and*
    /// the fresh re-resolve produces addresses, the next call to
    /// `send_frame` will have a target. We don't write Reconnecting on
    /// that branch (we only write Reconnecting when there's still no
    /// target after the re-resolve). The state we were in before is
    /// preserved.
    #[tokio::test]
    async fn send_frame_no_target_then_reresolve_succeeds_keeps_state() {
        let sender = UdpSocket::bind("127.0.0.1:0").await.expect("send bind");
        let stats = Arc::new(EndpointStats::new(EndpointState::Connected));
        let mut dest = Destination {
            // `localhost` resolves to 127.0.0.1 (and possibly ::1) on every
            // supported OS so the re-resolve populates resolved_ips.
            host: "localhost".to_string(),
            port: 14550,
            resolved_ips: vec![],
            latch: None,
        };
        send_frame(&sender, &mut dest, Bytes::from_static(b"hello"), &stats).await;
        // The frame was skipped (no target on entry), but the re-resolve
        // succeeded so we shouldn't transition to Reconnecting.
        assert_eq!(stats.load_state(), EndpointState::Connected);
        assert!(
            !dest.resolved_ips.is_empty(),
            "re-resolve should have populated IPs"
        );
    }

    /// Recovery: a Reconnecting endpoint that sends successfully flips
    /// back to Connected. Pins the two-way transition. Unix-only because
    /// the failure leg uses port-0 send_to (see above).
    #[cfg(unix)]
    #[tokio::test]
    async fn send_frame_recovers_from_reconnecting_on_success() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("recv bind");
        let receiver_port = receiver.local_addr().expect("recv addr").port();
        let sender = UdpSocket::bind("127.0.0.1:0").await.expect("send bind");
        let stats = Arc::new(EndpointStats::new(EndpointState::Connected));

        // First: an error transitions to Reconnecting.
        let mut bad_dest = make_dest(&[v4(127, 0, 0, 1)], 0);
        send_frame(&sender, &mut bad_dest, Bytes::from_static(b"a"), &stats).await;
        assert_eq!(stats.load_state(), EndpointState::Reconnecting);

        // Then: a successful send recovers to Connected.
        let mut good_dest = make_dest(&[v4(127, 0, 0, 1)], receiver_port);
        send_frame(&sender, &mut good_dest, Bytes::from_static(b"b"), &stats).await;
        assert_eq!(stats.load_state(), EndpointState::Connected);
    }

    /// Recovery via reply path: an accepted inbound flips state out of
    /// Reconnecting regardless of whether send_to has succeeded yet.
    /// Important for the "GCS pushes traffic before we've sent" case.
    #[tokio::test]
    async fn accepted_inbound_writes_connected() {
        let mut dest = make_dest(&[v4(127, 0, 0, 1)], 14550);
        let mut framer = Framer::with_capacity(1024);
        let mut framer_counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
        let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);

        let src = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 50000));
        // Empty data so try_next_frame yields nothing; we're testing the
        // state transition that runs unconditionally after classify_inbound.
        handle_inbound(
            &[],
            src,
            &mut dest,
            &mut framer,
            &mut framer_counters,
            &mut SeqTracker::new(8),
            EndpointId(0),
            &stats,
            &frame_tx,
            &crate::endpoint::filters::Filters::default(),
        )
        .await;
        assert_eq!(stats.load_state(), EndpointState::Connected);
        assert!(dest.latch.is_some(), "expected latch to be installed");
    }

    /// Rejected inbound (unrelated source) must NOT recover state — a
    /// stranger pinging the socket doesn't prove our configured peer is
    /// reachable.
    #[tokio::test]
    async fn rejected_inbound_does_not_write_connected() {
        let mut dest = make_dest(&[v4(192, 168, 1, 5)], 14550);
        let mut framer = Framer::with_capacity(1024);
        let mut framer_counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
        let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);

        // Source IP not in resolved_ips, no latch — classify returns Reject.
        let src = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 9), 50000));
        handle_inbound(
            &[],
            src,
            &mut dest,
            &mut framer,
            &mut framer_counters,
            &mut SeqTracker::new(8),
            EndpointId(0),
            &stats,
            &frame_tx,
            &crate::endpoint::filters::Filters::default(),
        )
        .await;
        assert_eq!(stats.load_state(), EndpointState::Reconnecting);
        assert!(dest.latch.is_none());
        assert_eq!(stats.in_filter_drops.load(Ordering::Relaxed), 1);
    }

    /// Per-frame In-filter (msgid blocklist) rejects a properly-sourced
    /// frame and bumps `in_filter_drops` on the endpoint's stats. CRC + frame
    /// counters still advance — `rx_frames` reflects link rate.
    #[tokio::test]
    async fn in_filter_drops_blocked_msgid() {
        // Build a v1 HEARTBEAT (msgid 0) by hand so the test does not depend
        // on tests/common/.
        let payload = [0u8; 9];
        let mut bytes = vec![STX_V1, payload.len() as u8, 0, 1, 1, 0];
        bytes.extend_from_slice(&payload);
        let mut crc = Crc16::new();
        crc.update_slice(&bytes[1..]);
        crc.update(50);
        let crc_value = crc.finalize();
        bytes.push((crc_value & 0xFF) as u8);
        bytes.push((crc_value >> 8) as u8);

        let mut dest = make_dest(&[v4(127, 0, 0, 1)], 14550);
        let mut framer = Framer::with_capacity(1024);
        let mut framer_counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::new(EndpointState::Connected));
        let (frame_tx, mut frame_rx) = mpsc::channel::<RouterFrame>(8);

        let filters = Filters {
            block_msgid_in: vec![MsgIdRange::single(0)],
            ..Filters::default()
        };
        let src = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 50000));
        handle_inbound(
            &bytes,
            src,
            &mut dest,
            &mut framer,
            &mut framer_counters,
            &mut SeqTracker::new(8),
            EndpointId(0),
            &stats,
            &frame_tx,
            &filters,
        )
        .await;
        assert!(frame_rx.try_recv().is_err(), "frame must not reach router");
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 1);
        assert_eq!(stats.in_filter_drops.load(Ordering::Relaxed), 1);
    }
}
