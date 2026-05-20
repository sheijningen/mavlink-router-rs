use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::EndpointIdAllocator;
use super::events::{EndpointEvent, RouterFrame};
use super::stats::EndpointStats;
use super::tx_queue::TxQueue;

/// Plumbing handed to every leaf routing endpoint — `tcpc:` / `udpc:` /
/// `serial:` top-level tasks and the per-child session a `tcps:` listener
/// spawns. The spawner builds `tx_queue` + `stats` so the router can hold
/// its own clones before the task starts running. There is no `event_tx`:
/// leaves have no sub-endpoints.
pub struct ClientWiring {
    pub frame_tx: mpsc::Sender<RouterFrame>,
    pub tx_queue: TxQueue,
    pub stats: Arc<EndpointStats>,
    pub cancel: CancellationToken,
}

/// Plumbing handed to every parent-listener routing endpoint — `tcps:` and
/// `udps:`. Carries the id allocator (for minting child IDs), the
/// reader→router frame channel, the sub-endpoint lifecycle channel, and
/// the cancellation token. `stats` is the parent's own `Arc<EndpointStats>`
/// (spawner-constructed via `EndpointStats::new(Reconnecting)`); the
/// listener writes `Connected` once bind succeeds — that transition is the
/// observable signal callers use to detect bind completion when starting
/// from `127.0.0.1:0`.
pub struct ServerWiring {
    pub allocator: Arc<EndpointIdAllocator>,
    pub frame_tx: mpsc::Sender<RouterFrame>,
    pub event_tx: mpsc::Sender<EndpointEvent>,
    pub cancel: CancellationToken,
    pub stats: Arc<EndpointStats>,
}
