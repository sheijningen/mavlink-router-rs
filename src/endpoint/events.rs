use std::net::SocketAddr;
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

/// Lifecycle event on the shared reader→router event channel. Carries enough
/// for the router to register an endpoint into its routing tables (or drop it)
/// without ever allocating a per-endpoint handle itself — the spawner / parent
/// owns construction of `EndpointId`, `TxQueue`, `Arc<EndpointStats>`, and
/// `IdentityFlags` and announces them here.
///
/// `EndpointAdded` is emitted by the top-level spawner for every top-level
/// endpoint, leaf or listener. Leaves (`tcpc:` / `udpc:` / `serial:`) carry
/// `routable = Some(_)` so the router can dispatch frames to them; `tcps:` /
/// `udps:` parent listeners carry `routable = None` — they're configured
/// endpoints visible in stats but have no `TxQueue` (children own real
/// readers/writers) and are never iterated as routing destinations. The
/// router holds them in the same registry; the `None` short-circuits the
/// per-frame dispatch loop at one branch. `PeerAdded` / `PeerRemoved` are
/// emitted by `tcps:` listeners (per accepted client) and `udps:` listeners
/// (per learned peer). There is no top-level `EndpointRemoved` variant in
/// v1 — top-level endpoints live for the process; on shutdown the router
/// writes `state = Down` and emits one final synthetic stats line per
/// CLAUDE.md's "Endpoint registration is symmetric" decision.
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
        peer_addr: SocketAddr,
        name: String,
        tx_queue: TxQueue,
        stats: Arc<EndpointStats>,
        identity: IdentityFlags,
    },
    PeerRemoved {
        parent_id: EndpointId,
        child_id: EndpointId,
        peer_addr: SocketAddr,
        reason: PeerRemovalReason,
    },
}

/// The dispatch handles an endpoint contributes when it is a routing
/// destination. Leaf endpoints supply this; `tcps:` / `udps:` parent
/// listeners pass `None` to [`EndpointEvent::EndpointAdded`] because they
/// never receive frames themselves — their accepted children / learned peers
/// register separately via [`EndpointEvent::PeerAdded`].
#[derive(Debug)]
pub struct Routable {
    pub tx_queue: TxQueue,
    pub identity: IdentityFlags,
}

/// Why a child routing endpoint was torn down. Surfaced in logs and (later)
/// in stats so an operator can tell idle reap apart from peer-cap pressure,
/// a `tcps:` client socket close, or a parent shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRemovalReason {
    Idle,
    LruEvicted,
    Disconnected,
    ListenerShutdown,
}
