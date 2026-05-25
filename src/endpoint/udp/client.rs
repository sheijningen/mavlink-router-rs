use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use tokio::net::{UdpSocket, lookup_host};
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

use super::super::EndpointId;
use super::super::backoff::{Backoff, BindOutcome, bind_with_backoff};
use super::super::defaults::{DEFAULT_RECONNECT_INITIAL_MS, DEFAULT_RECONNECT_MAX_MS};
use super::super::identity_flags::{IdentityFlags, SEQ_TRACKER_CAPACITY};
use super::super::seq_tracker::SeqTracker;
use super::super::session::SessionCtx;
use super::super::socket::bind_udp_dual_stack;
use super::super::spec::UdpClientEndpoint;
use super::super::stats::{EndpointState, EndpointStats, FramerCounters};
use super::super::wiring::ClientWiring;
use super::MAX_DATAGRAM_BYTES;
use crate::mavlink::framer::Framer;

const DEFAULT_LATCH_IDLE_SECS: u64 = 30;

/// `latch_idle_secs` lower bound — 0 would revert the latch on the very
/// next REVERT_TICK, defeating the latching mechanism entirely.
pub const MIN_LATCH_IDLE_SECS: u64 = 1;

/// `latch_idle_secs` upper bound (24 hours). Same rationale as
/// [`super::server::MAX_IDLE_SECS`]: a day is effectively "never revert".
pub const MAX_LATCH_IDLE_SECS: u64 = 86_400;
const REVERT_TICK: Duration = Duration::from_secs(1);

/// Inputs that distinguish one `udpc:` endpoint from another: where to send,
/// what to call it, and the latch-idle threshold.
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
    /// `UdpClientEndpoint`, substituting documented defaults for any unset
    /// knob. The spawner supplies `endpoint_id` and `name`.
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
                warn!(%host, "DNS resolution returned no addresses");
            }
            ips
        }
        Err(err) => {
            warn!(error = %err, %host, "DNS resolution failed");
            Vec::new()
        }
    }
}

/// Run a `udpc:` endpoint until the cancellation token fires. Binds a local
/// socket for the host's resolved family, retrying with the shared
/// capped-exp backoff on failure; initial DNS resolution failure is
/// non-fatal (retried on first send/inbound).
pub async fn run(spec: UdpClientSpec, wiring: ClientWiring) {
    let span = info_span!("udpc", name = %spec.name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: UdpClientSpec, wiring: ClientWiring) {
    let Some((socket, dest)) = bind_local_socket(&spec, &wiring.cancel).await else {
        return;
    };
    log_bind_success(&socket, &dest);
    wiring.stats.store_state(EndpointState::Connected);
    event_loop(socket, dest, spec, wiring).await;
}

/// Resolve the configured host, choose a local-bind family that can reach
/// it, and bind a UDP socket with the shared capped-exp backoff. Returns
/// `None` on cancellation.
async fn bind_local_socket(
    spec: &UdpClientSpec,
    cancel: &CancellationToken,
) -> Option<(UdpSocket, Destination)> {
    let initial_ips = resolve_host(&spec.host, spec.port).await;
    let dest = Destination {
        host: spec.host.clone(),
        port: spec.port,
        resolved_ips: initial_ips,
        latch: None,
    };

    let mut backoff = Backoff::new(spec.reconnect_initial_ms, spec.reconnect_max_ms);
    let local_bind = pick_local_bind(&dest.resolved_ips);
    match bind_with_backoff(cancel, &mut backoff, local_bind, || {
        bind_udp_dual_stack(local_bind)
    })
    .await
    {
        BindOutcome::Bound(socket) => Some((socket, dest)),
        BindOutcome::Cancelled => None,
    }
}

fn log_bind_success(socket: &UdpSocket, dest: &Destination) {
    let local_addr = socket
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|err| format!("unknown ({err})"));
    info!(
        %local_addr,
        host = %dest.host,
        port = dest.port,
        "bound and ready"
    );
}

async fn event_loop(
    socket: UdpSocket,
    mut dest: Destination,
    spec: UdpClientSpec,
    wiring: ClientWiring,
) {
    let mut framer = Framer::new();
    let mut framer_counters = FramerCounters::new();
    let mut seq_tracker = SeqTracker::new(SEQ_TRACKER_CAPACITY);
    let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];

    let mut revert_tick = interval(REVERT_TICK);
    revert_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // `interval(...)` fires its first tick immediately; consume it so the
    // revert check doesn't run before we've had a chance to latch.
    let _ = revert_tick.tick().await;

    let session_ctx = SessionCtx {
        endpoint_id: spec.endpoint_id,
        stats: &wiring.stats,
        frame_tx: &wiring.frame_tx,
        filters: &spec.identity.filters,
    };
    loop {
        tokio::select! {
            biased;
            _ = wiring.cancel.cancelled() => {
                wiring.tx_queue.drain_and_discard();
                return;
            }
            _ = revert_tick.tick() => {
                check_latch_idle(&mut dest, Duration::from_secs(spec.latch_idle_secs)).await;
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
                            &session_ctx,
                        )
                        .await;
                    }
                    Err(err) => {
                        warn!(error = %err, "recv_from error");
                    }
                }
            }
            frame = wiring.tx_queue.pop_or_wait() => {
                send_frame(&socket, &mut dest, frame, &wiring.stats).await;
            }
        }
    }
}

async fn handle_inbound(
    data: &[u8],
    src: SocketAddr,
    dest: &mut Destination,
    framer: &mut Framer,
    framer_counters: &mut FramerCounters,
    seq_tracker: &mut SeqTracker,
    session_ctx: &SessionCtx<'_>,
) {
    match classify_inbound(dest, src.ip()) {
        InboundDecision::Reject => {
            session_ctx
                .stats
                .in_filter_drops
                .fetch_add(1, Ordering::Relaxed);
            debug!(%src, "inbound from unexpected source dropped");
            return;
        }
        InboundDecision::AcceptAndLatch => {
            dest.latch = Some(LatchInfo {
                addr: src,
                last_inbound: Instant::now(),
            });
            info!(%src, "latched onto reply source");
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
    session_ctx.stats.store_state(EndpointState::Connected);

    framer.buffer_mut().extend_from_slice(data);
    // `in_filter_drops` is the union counter for every ingress-side drop;
    // the wrong-source-IP rejection above bumps the same slot.
    let pipeline = session_ctx
        .forward_inbound_frames(framer, seq_tracker)
        .instrument(tracing::trace_span!("udpc_ingress", %src));
    if pipeline.await.is_break() {
        return;
    }
    framer_counters.sync(framer, session_ctx.stats);
}

async fn send_frame(
    socket: &UdpSocket,
    dest: &mut Destination,
    frame: Bytes,
    stats: &Arc<EndpointStats>,
) {
    let Some(target) = dest.current_target() else {
        debug!(host = %dest.host, "send skipped — no resolved address");
        let fresh = resolve_host(&dest.host, dest.port).await;
        if !fresh.is_empty() {
            dest.resolved_ips = fresh;
        }
        // No target after re-resolve is a DNS-resolve failure preventing
        // any send — surface Reconnecting. Next successful send flips back.
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
            warn!(error = %err, %target, "send_to failed; re-resolving for next burst");
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
    info!(%latch.addr, "latch idle; reverting to configured");
    let fresh = resolve_host(&dest.host, dest.port).await;
    if !fresh.is_empty() {
        dest.resolved_ips = fresh;
    } else {
        warn!(host = %dest.host, "DNS re-resolve failed on revert; keeping previous resolved IPs");
    }
    dest.latch = None;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::events::RouterFrame;
    use crate::endpoint::filters::{Filters, MsgIdRange};
    use crate::mavlink::crc::Crc16;
    use crate::mavlink::frame::STX_V1;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4};
    use tokio::sync::mpsc;

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
        // 192.168.1.6 is in resolved_ips but the latch on 192.168.1.5 is a hard lock until
        // idle revert.
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
        let filters = crate::endpoint::filters::Filters::default();
        let session_ctx = SessionCtx {
            endpoint_id: EndpointId(0),
            stats: &stats,
            frame_tx: &frame_tx,
            filters: &filters,
        };

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
            &session_ctx,
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
        let filters = crate::endpoint::filters::Filters::default();
        let session_ctx = SessionCtx {
            endpoint_id: EndpointId(0),
            stats: &stats,
            frame_tx: &frame_tx,
            filters: &filters,
        };

        // Source IP not in resolved_ips, no latch — classify returns Reject.
        let src = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 9), 50000));
        handle_inbound(
            &[],
            src,
            &mut dest,
            &mut framer,
            &mut framer_counters,
            &mut SeqTracker::new(8),
            &session_ctx,
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
        let session_ctx = SessionCtx {
            endpoint_id: EndpointId(0),
            stats: &stats,
            frame_tx: &frame_tx,
            filters: &filters,
        };
        let src = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 50000));
        handle_inbound(
            &bytes,
            src,
            &mut dest,
            &mut framer,
            &mut framer_counters,
            &mut SeqTracker::new(8),
            &session_ctx,
        )
        .await;
        assert!(frame_rx.try_recv().is_err(), "frame must not reach router");
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 1);
        assert_eq!(stats.in_filter_drops.load(Ordering::Relaxed), 1);
    }
}
