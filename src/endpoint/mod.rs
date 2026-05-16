use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod events;
pub mod filters;
pub mod serial;
pub mod socket;
pub mod spec;
pub mod stats;
pub mod tcp_client;
pub mod tcp_server;
pub mod tx_queue;
pub mod udp_client;
pub mod udp_server;

/// Process-wide unique identifier for a routing endpoint. The router uses
/// this as a `HashMap` key for learn-sets and queue ownership; the reader
/// task stamps it on every frame so the router never has to re-map source
/// addresses. Stable for the lifetime of the routing endpoint and *not* reused
/// when a sub-endpoint (a `tcps:` child or a `udps:` peer) is torn down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EndpointId(u64);

impl EndpointId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

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

    #[test]
    fn allocator_returns_unique_ids() {
        let a = EndpointIdAllocator::new();
        let id0 = a.alloc();
        let id1 = a.alloc();
        let id2 = a.alloc();
        assert_eq!(id0.as_u64(), 0);
        assert_eq!(id1.as_u64(), 1);
        assert_eq!(id2.as_u64(), 2);
        assert_ne!(id0, id1);
    }

    #[test]
    fn endpoint_id_display() {
        let a = EndpointIdAllocator::new();
        let id = a.alloc();
        assert_eq!(format!("{id}"), "0");
    }
}
