use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub mod backoff;
pub mod defaults;
pub mod events;
pub mod filters;
pub mod identity_flags;
pub mod seq_tracker;
pub mod serial;
pub mod session;
pub mod socket;
pub mod spec;
pub mod stats;
pub mod tcp;
pub mod tx_queue;
pub mod udp;

use std::net::SocketAddr;

/// Sleep for `delay` unless cancelled first. Returns `true` if the full delay
/// elapsed, `false` if the cancellation token fired. Used by every endpoint
/// task's reconnect / reopen / bind-retry sleep so shutdown is bounded by the
/// drain budget rather than the longest configured backoff.
pub async fn wait_or_cancel(cancel: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        _ = sleep(delay) => true,
    }
}

/// Sub-endpoint name for a `tcps:` accepted client or a `udps:` learned peer:
/// `<parent>/<ip>-<port>` for IPv4, `<parent>/[<ip>]-<port>` for IPv6 (the
/// brackets match the CLI grammar so the name round-trips visually with an
/// explicit `udps:[::]:N` / `tcps:[::]:N` spec).
pub fn peer_endpoint_name(parent_name: &str, addr: SocketAddr) -> String {
    match addr {
        SocketAddr::V4(v4) => format!("{parent_name}/{}-{}", v4.ip(), v4.port()),
        SocketAddr::V6(v6) => format!("{parent_name}/[{}]-{}", v6.ip(), v6.port()),
    }
}

/// Process-wide unique identifier for a routing endpoint. The router uses
/// this as a `HashMap` key for learn-sets and queue ownership; the reader
/// task stamps it on every frame so the router never has to re-map source
/// addresses. Stable for the lifetime of the routing endpoint and *not* reused
/// when a sub-endpoint (a `tcps:` child or a `udps:` peer) is torn down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EndpointId(u64);

impl fmt::Display for EndpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Allocator handed to every component that can spawn a routing endpoint
/// (configured endpoints at startup, `tcps:` accepting a client, `udps:`
/// learning a peer). Lock-free monotonic counter; not reset on reconnect.
#[derive(Debug, Default)]
pub struct EndpointIdAllocator {
    next: AtomicU64,
}

impl EndpointIdAllocator {
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
        }
    }

    pub fn alloc(&self) -> EndpointId {
        EndpointId(self.next.fetch_add(1, Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4, SocketAddrV6};

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
    fn allocator_returns_unique_ids() {
        let a = EndpointIdAllocator::new();
        let id0 = a.alloc();
        let id1 = a.alloc();
        let id2 = a.alloc();
        assert_eq!(id0.0, 0);
        assert_eq!(id1.0, 1);
        assert_eq!(id2.0, 2);
        assert_ne!(id0, id1);
    }

    #[test]
    fn endpoint_id_display() {
        let a = EndpointIdAllocator::new();
        let id = a.alloc();
        assert_eq!(format!("{id}"), "0");
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
        let r = wait_or_cancel(&cancel, Duration::from_millis(20)).await;
        assert!(r);
        assert!(start.elapsed() >= Duration::from_millis(20));
    }
}
