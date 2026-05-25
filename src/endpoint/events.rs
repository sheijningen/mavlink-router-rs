use std::sync::Arc;

use bytes::Bytes;

use super::EndpointId;
use super::identity_flags::IdentityFlags;
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

/// Lifecycle events of endpoints on the shared reader→router event channel.
#[derive(Debug)]
pub enum EndpointEvent {
    EndpointAdded {
        id: EndpointId,
        name: String,
        stats: Arc<EndpointStats>,
        routable: Option<Routable>,
    },
    PeerAdded {
        parent_id: EndpointId,
        child_id: EndpointId,
        name: String,
        stats: Arc<EndpointStats>,
        routable: Routable,
    },
    PeerRemoved {
        parent_id: EndpointId,
        child_id: EndpointId,
        reason: PeerRemovalReason,
    },
}

/// The dispatch handles an endpoint contributes when it is a routing
/// destination: a `TxQueue` to enqueue frames into and the `IdentityFlags`
/// the router consults for filters, sniffer, and group membership. Carried
/// on [`EndpointEvent::EndpointAdded`] for leaf top-level endpoints (`tcpc:`
/// / `udpc:` / `serial:`) and on [`EndpointEvent::PeerAdded`] for every
/// accepted `tcps:` child / learned `udps:` peer. `tcps:` / `udps:` parent
/// listeners pass `None` to `EndpointAdded` because they never receive
/// frames themselves — their children / peers register separately and supply
/// their own `Routable`.
#[derive(Debug)]
pub struct Routable {
    pub tx_queue: TxQueue,
    pub identity: IdentityFlags,
}

/// Why a child routing endpoint was torn down. Surfaced in logs so an
/// operator can tell idle reap apart from peer-cap pressure, a `tcps:`
/// client socket close, or a parent shutdown. The stats `state` field
/// collapses the four reasons into two terminal values: `Idle` → `Idle`,
/// every other reason → `Down`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRemovalReason {
    Idle,
    LruEvicted,
    Disconnected,
    ListenerShutdown,
}
