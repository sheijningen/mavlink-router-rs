use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;

use super::EndpointId;
use super::stats::EndpointStats;
use super::tx_queue::TxQueue;
use crate::mavlink::frame::ParsedHeader;

/// One frame on the shared reader→router channel: the routing endpoint that
/// produced it, the immutable frame body, and the header the reader already
/// parsed (so the router never re-parses).
#[derive(Debug)]
pub struct RouterFrame {
    pub endpoint_id: EndpointId,
    pub frame: Bytes,
    pub header: ParsedHeader,
}

/// Lifecycle event for a child routing endpoint — a `udps:` learned peer or
/// (in Phase 3) a `tcps:` accepted client. The router uses these to register
/// and evict its per-child learn-sets, TxQueue handles, and stats.
#[derive(Debug)]
pub enum EndpointEvent {
    PeerAdded {
        parent_id: EndpointId,
        child_id: EndpointId,
        peer_addr: SocketAddr,
        name: String,
        tx_queue: TxQueue,
        stats: Arc<EndpointStats>,
    },
    PeerRemoved {
        parent_id: EndpointId,
        child_id: EndpointId,
        peer_addr: SocketAddr,
        reason: PeerRemovalReason,
    },
}

/// Why a child routing endpoint was torn down. Surfaced in logs and (later)
/// in stats so an operator can tell idle reap apart from peer-cap pressure
/// or a parent shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRemovalReason {
    Idle,
    LruEvicted,
    ListenerShutdown,
}
