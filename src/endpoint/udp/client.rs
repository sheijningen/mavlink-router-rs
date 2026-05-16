use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use thiserror::Error;
use tokio::net::{UdpSocket, lookup_host};
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info_span, trace, warn};

use super::super::EndpointId;
use super::super::backoff::Backoff;
use super::super::events::RouterFrame;
use super::super::identity_flags::IdentityFlags;
use super::super::socket::bind_udp_dual_stack;
use super::super::spec::UdpClientEndpoint;
use super::super::stats::{EndpointStats, FramerCounters};
use super::super::tx_queue::TxQueue;
use super::super::wait_or_cancel;
use crate::mavlink::framer::Framer;

const DEFAULT_LATCH_IDLE_SECS: u64 = 30;
const DEFAULT_TX_QUEUE_FRAMES: usize = 256;
const DEFAULT_READ_BUF_BYTES: usize = 8192;
const DEFAULT_RECONNECT_INITIAL_MS: u64 = 250;
const DEFAULT_RECONNECT_MAX_MS: u64 = 30_000;
// Max IP datagram payload plus headroom; matches `udps:` for symmetry so
// neither side truncates an oversized inbound packet.
const MAX_DATAGRAM_BYTES: usize = 65_536;
const REVERT_TICK: Duration = Duration::from_secs(1);

/// Per-endpoint runtime configuration. The spec parser hands us a fully-typed
/// `UdpClientEndpoint`; this struct collapses the optional knobs down to the
/// concrete values the task actually uses, substituting CLAUDE.md defaults
/// where the user left a knob unset. The `reconnect_*_ms` fields are
/// hard-coded to the `tcpc:` curve (CLAUDE.md "Bind/open failure at startup is
/// not fatal" + "Same reasoning applies to `udps:`" — `udpc:` follows the same
/// rule) — `udpc:` does not expose per-endpoint bind-retry overrides in v1.
#[derive(Debug, Clone, Copy)]
pub struct UdpClientConfig {
    pub latch_idle_secs: u64,
    pub tx_queue_frames: usize,
    pub read_buf_bytes: usize,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
}

impl Default for UdpClientConfig {
    fn default() -> Self {
        Self {
            latch_idle_secs: DEFAULT_LATCH_IDLE_SECS,
            tx_queue_frames: DEFAULT_TX_QUEUE_FRAMES,
            read_buf_bytes: DEFAULT_READ_BUF_BYTES,
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
        }
    }
}

impl UdpClientConfig {
    pub fn from_endpoint(ep: &UdpClientEndpoint) -> Self {
        Self {
            latch_idle_secs: ep.latch_idle_secs.unwrap_or(DEFAULT_LATCH_IDLE_SECS),
            tx_queue_frames: ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
            read_buf_bytes: ep.common.read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES),
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
        }
    }
}

/// Typed-empty return for `udpc:` `run()`. Local bind failures enter the same
/// backoff loop as `tcpc:` reconnects (CLAUDE.md "Bind/open failure at startup
/// is not fatal"); inbound recv errors and outbound send errors are logged and
/// the loop continues — no terminal failure modes remain in v1. Kept as a
/// typed return for symmetry with the other endpoint modules in case a fatal
/// case shows up.
#[derive(Debug, Error)]
pub enum UdpClientError {}

/// Inputs that distinguish one `udpc:` endpoint from another: where to send,
/// what to call it, and the per-endpoint knobs from the query string.
/// `identity` carries the filter / sniffer / group / capacity bundle —
/// unused today, threaded so Phase 5 can wire it up without a spawner
/// rework (CLAUDE.md "Filters, group, sniffer, and learn/seq capacities
/// travel with the `*Spec`").
pub struct UdpClientSpec {
    pub host: String,
    pub port: u16,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub cfg: UdpClientConfig,
    pub identity: IdentityFlags,
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
    if dest.resolved_ips.iter().any(|&ip| canonical_ip(ip) == src) {
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
    if resolved_ips.is_empty() || resolved_ips.iter().any(|ip| ip.is_ipv6()) {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    }
}

async fn resolve_host(host: &str, port: u16) -> Vec<IpAddr> {
    let target = format!("{host}:{port}");
    match lookup_host(target.as_str()).await {
        Ok(addrs) => {
            let ips: Vec<IpAddr> = addrs.map(|sa| sa.ip()).collect();
            if ips.is_empty() {
                warn!(%host, "udpc DNS resolution returned no addresses");
            }
            ips
        }
        Err(e) => {
            warn!(error = %e, %host, "udpc DNS resolution failed");
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
pub async fn run(spec: UdpClientSpec, wiring: UdpClientWiring) -> Result<(), UdpClientError> {
    let span = info_span!("udpc", name = %spec.name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: UdpClientSpec, wiring: UdpClientWiring) -> Result<(), UdpClientError> {
    let UdpClientSpec {
        host,
        port,
        endpoint_id,
        name: _,
        cfg,
        identity: _,
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

    let mut backoff = Backoff::new(cfg.reconnect_initial_ms, cfg.reconnect_max_ms);
    let local_bind = pick_local_bind(&dest.resolved_ips);
    let socket = loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        match bind_udp_dual_stack(local_bind) {
            Ok(s) => break s,
            Err(e) => {
                warn!(error = %e, %local_bind, "udpc local bind failed; retrying after backoff");
                if !wait_or_cancel(&cancel, backoff.next_delay()).await {
                    return Ok(());
                }
            }
        }
    };

    let mut framer = Framer::with_capacity(cfg.read_buf_bytes);
    let mut framer_counters = FramerCounters::new();
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
                return Ok(());
            }
            _ = revert_tick.tick() => {
                check_latch_idle(&mut dest, Duration::from_secs(cfg.latch_idle_secs)).await;
            }
            res = socket.recv_from(&mut buf) => {
                match res {
                    Ok((n, src)) => {
                        handle_inbound(
                            &buf[..n],
                            src,
                            &mut dest,
                            &mut framer,
                            &mut framer_counters,
                            endpoint_id,
                            &stats,
                            &frame_tx,
                        )
                        .await;
                    }
                    Err(e) => {
                        warn!(error = %e, "udpc recv_from error");
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
    endpoint_id: EndpointId,
    stats: &Arc<EndpointStats>,
    frame_tx: &mpsc::Sender<RouterFrame>,
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
            trace!(%src, "udpc latched onto reply source");
        }
        InboundDecision::AcceptUpdate => {
            if let Some(latch) = dest.latch.as_mut() {
                latch.addr = src;
                latch.last_inbound = Instant::now();
            }
        }
    }

    framer.buffer_mut().extend_from_slice(data);
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
            debug!("udpc router channel closed; stopping frame forwarding");
            return;
        }
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
        return;
    };
    match socket.send_to(&frame, target).await {
        Ok(n) => stats.add_tx_frame(n),
        Err(e) => {
            warn!(error = %e, %target, "udpc send_to failed; re-resolving for next burst");
            let fresh = resolve_host(&dest.host, dest.port).await;
            if !fresh.is_empty() {
                dest.resolved_ips = fresh;
            }
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
    debug!(%latch.addr, "udpc latch idle; reverting to configured");
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
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4};

    fn make_dest(ips: &[IpAddr], port: u16) -> Destination {
        Destination {
            host: "example".to_string(),
            port,
            resolved_ips: ips.to_vec(),
            latch: None,
        }
    }

    fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn config_defaults_when_endpoint_unset() {
        let ep = UdpClientEndpoint::default();
        let cfg = UdpClientConfig::from_endpoint(&ep);
        assert_eq!(cfg.latch_idle_secs, DEFAULT_LATCH_IDLE_SECS);
        assert_eq!(cfg.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
        assert_eq!(cfg.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(cfg.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(cfg.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    #[test]
    fn config_overrides_from_endpoint() {
        use crate::endpoint::spec::CommonQuery;
        let ep = UdpClientEndpoint {
            latch_idle_secs: Some(5),
            common: CommonQuery {
                tx_queue_frames: Some(8),
                read_buf_bytes: Some(1024),
            },
            ..UdpClientEndpoint::default()
        };
        let cfg = UdpClientConfig::from_endpoint(&ep);
        assert_eq!(cfg.latch_idle_secs, 5);
        assert_eq!(cfg.tx_queue_frames, 8);
        assert_eq!(cfg.read_buf_bytes, 1024);
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
}
