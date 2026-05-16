use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use super::super::EndpointId;
use super::super::EndpointIdAllocator;
use super::super::backoff::Backoff;
use super::super::events::{EndpointEvent, PeerRemovalReason, RouterFrame};
use super::super::peer_endpoint_name;
use super::super::socket::{bind_tcp_dual_stack, configure_tcp_stream};
use super::super::spec::TcpServerEndpoint;
use super::super::stats::EndpointStats;
use super::super::tx_queue::TxQueue;
use super::session::{SessionOutcome, run_session};

const DEFAULT_READ_BUF_BYTES: usize = 8192;
const DEFAULT_TX_QUEUE_FRAMES: usize = 256;
const DEFAULT_RECONNECT_INITIAL_MS: u64 = 250;
const DEFAULT_RECONNECT_MAX_MS: u64 = 30_000;

/// Per-listener runtime configuration. The spec parser hands us a fully-typed
/// `TcpServerEndpoint`; this struct collapses the optional knobs down to the
/// concrete values the task actually uses, substituting CLAUDE.md defaults
/// where the user left a knob unset. The `reconnect_*_ms` fields are
/// hard-coded to the `tcpc:` curve (CLAUDE.md: "TCP/UDP server bind reuses
/// the `tcpc:` backoff curve") — `tcps:` does not expose per-listener
/// reconnect overrides in v1.
#[derive(Debug, Clone, Copy)]
pub struct TcpServerConfig {
    pub read_buf_bytes: usize,
    pub tx_queue_frames: usize,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
}

impl Default for TcpServerConfig {
    fn default() -> Self {
        Self {
            read_buf_bytes: DEFAULT_READ_BUF_BYTES,
            tx_queue_frames: DEFAULT_TX_QUEUE_FRAMES,
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
        }
    }
}

impl TcpServerConfig {
    pub fn from_endpoint(ep: &TcpServerEndpoint) -> Self {
        Self {
            read_buf_bytes: ep.common.read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES),
            tx_queue_frames: ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
        }
    }
}

#[derive(Debug, Error)]
pub enum TcpServerError {
    // No terminal errors in v1: bind failures enter the same backoff loop as
    // `tcpc:` reconnects, accept errors are logged and the loop continues, and
    // per-child disconnects are routine. Kept as a typed return for symmetry
    // with the other endpoint modules in case a fatal case shows up later.
}

/// Inputs that distinguish one `tcps:` listener from another: where to bind,
/// what to call it, and the per-listener knobs from the query string.
pub struct TcpServerSpec {
    pub listen_addr: SocketAddr,
    pub parent_id: EndpointId,
    pub parent_name: String,
    pub cfg: TcpServerConfig,
}

/// Shared wiring every endpoint needs: the global EndpointId allocator,
/// the reader→router frame channel, the sub-endpoint lifecycle channel,
/// and the cancellation token.
pub struct TcpServerWiring {
    pub allocator: Arc<EndpointIdAllocator>,
    pub frame_tx: mpsc::Sender<RouterFrame>,
    pub event_tx: mpsc::Sender<EndpointEvent>,
    pub cancel: CancellationToken,
}

/// Run a `tcps:` listener until the cancellation token fires. Binding is
/// retried with the shared capped-exp backoff (CLAUDE.md "Initial bind/dial
/// failure path"), so a port collision at startup logs at WARN and the
/// listener attaches as soon as the port frees. Each accepted client becomes
/// its own routing endpoint announced via `event_tx`.
pub async fn run(spec: TcpServerSpec, wiring: TcpServerWiring) -> Result<(), TcpServerError> {
    let TcpServerSpec {
        listen_addr,
        parent_id,
        parent_name,
        cfg,
    } = spec;
    let TcpServerWiring {
        allocator,
        frame_tx,
        event_tx,
        cancel,
    } = wiring;

    let mut backoff = Backoff::new(cfg.reconnect_initial_ms, cfg.reconnect_max_ms);

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }

        let listener = match bind_tcp_dual_stack(listen_addr) {
            Ok(l) => l,
            Err(e) => {
                warn!(error = %e, %listen_addr, "tcps bind failed; retrying after backoff");
                if !wait_or_cancel(&cancel, backoff.next_delay()).await {
                    return Ok(());
                }
                continue;
            }
        };
        backoff.reset();
        info!(%listen_addr, parent_id = %parent_id, "tcps listening");

        run_accept_loop(
            listener,
            parent_id,
            &parent_name,
            &cfg,
            &allocator,
            &frame_tx,
            &event_tx,
            &cancel,
        )
        .await;

        // Accept loop only returns on cancellation; we don't re-bind in normal
        // operation. (A future enhancement could detect a permanently broken
        // listener socket and re-bind, but tokio's TcpListener::accept errors
        // are per-connection in practice.)
        return Ok(());
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_accept_loop(
    listener: TcpListener,
    parent_id: EndpointId,
    parent_name: &str,
    cfg: &TcpServerConfig,
    allocator: &Arc<EndpointIdAllocator>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    event_tx: &mpsc::Sender<EndpointEvent>,
    cancel: &CancellationToken,
) {
    let mut children: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            res = listener.accept() => {
                match res {
                    Ok((stream, peer_addr)) => {
                        accept_one_client(
                            stream,
                            peer_addr,
                            parent_id,
                            parent_name,
                            cfg,
                            allocator,
                            frame_tx,
                            event_tx,
                            cancel,
                            &mut children,
                        )
                        .await;
                    }
                    Err(e) => {
                        // Per CLAUDE.md: removal of a child without killing the
                        // router. Accept errors are typically EMFILE-style
                        // per-connection failures, not listener death; log and
                        // continue.
                        warn!(error = %e, "tcps accept failed; continuing");
                    }
                }
            }
        }
    }

    // Cancellation has fired; the children share the cancel token so their
    // sessions are already unwinding. Wait for each to drain and emit its
    // final PeerRemoved event.
    while children.join_next().await.is_some() {}
}

#[allow(clippy::too_many_arguments)]
async fn accept_one_client(
    stream: TcpStream,
    peer_addr: SocketAddr,
    parent_id: EndpointId,
    parent_name: &str,
    cfg: &TcpServerConfig,
    allocator: &Arc<EndpointIdAllocator>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    event_tx: &mpsc::Sender<EndpointEvent>,
    cancel: &CancellationToken,
    children: &mut JoinSet<()>,
) {
    if let Err(e) = configure_tcp_stream(&stream) {
        warn!(error = %e, %peer_addr, "tcps configure_tcp_stream failed on accept");
    }

    let child_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::new());
    let tx_queue = TxQueue::new(cfg.tx_queue_frames, stats.clone());
    let name = peer_endpoint_name(parent_name, peer_addr);

    // Announce PeerAdded before spawning the child so the router never sees
    // a RouterFrame for an unknown EndpointId.
    if event_tx
        .send(EndpointEvent::PeerAdded {
            parent_id,
            child_id,
            peer_addr,
            name,
            tx_queue: tx_queue.clone(),
            stats: stats.clone(),
        })
        .await
        .is_err()
    {
        debug!("tcps event channel closed; dropping accepted client");
        return;
    }
    trace!(parent_id = %parent_id, %peer_addr, %child_id, "tcps client accepted");

    children.spawn(run_client_session(
        stream,
        peer_addr,
        parent_id,
        child_id,
        stats,
        frame_tx.clone(),
        tx_queue,
        event_tx.clone(),
        cancel.clone(),
        cfg.read_buf_bytes,
    ));
}

#[allow(clippy::too_many_arguments)]
async fn run_client_session(
    stream: TcpStream,
    peer_addr: SocketAddr,
    parent_id: EndpointId,
    child_id: EndpointId,
    stats: Arc<EndpointStats>,
    frame_tx: mpsc::Sender<RouterFrame>,
    tx_queue: TxQueue,
    event_tx: mpsc::Sender<EndpointEvent>,
    cancel: CancellationToken,
    read_buf_bytes: usize,
) {
    let outcome = run_session(
        stream,
        child_id,
        &stats,
        &frame_tx,
        &tx_queue,
        &cancel,
        read_buf_bytes,
    )
    .await;
    // Drain anything still queued for this client; the socket is going away.
    tx_queue.drain_and_discard();

    let reason = match outcome {
        SessionOutcome::Cancelled => PeerRemovalReason::ListenerShutdown,
        SessionOutcome::Disconnected => PeerRemovalReason::Disconnected,
    };
    let _ = event_tx
        .send(EndpointEvent::PeerRemoved {
            parent_id,
            child_id,
            peer_addr,
            reason,
        })
        .await;
    trace!(parent_id = %parent_id, %peer_addr, %child_id, ?reason, "tcps client session ended");
}

async fn wait_or_cancel(cancel: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        _ = sleep(delay) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_when_endpoint_unset() {
        let ep = TcpServerEndpoint::default();
        let cfg = TcpServerConfig::from_endpoint(&ep);
        assert_eq!(cfg.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(cfg.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
        assert_eq!(cfg.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(cfg.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    #[test]
    fn config_overrides_common_fields() {
        use crate::endpoint::spec::CommonQuery;
        let ep = TcpServerEndpoint {
            common: CommonQuery {
                read_buf_bytes: Some(1024),
                tx_queue_frames: Some(8),
                ..CommonQuery::default()
            },
            ..TcpServerEndpoint::default()
        };
        let cfg = TcpServerConfig::from_endpoint(&ep);
        assert_eq!(cfg.read_buf_bytes, 1024);
        assert_eq!(cfg.tx_queue_frames, 8);
        // Reconnect curve stays at the tcpc defaults — CLAUDE.md "TCP/UDP
        // server bind reuses the `tcpc:` backoff curve" and tcps does not
        // expose per-endpoint reconnect overrides in v1.
        assert_eq!(cfg.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(cfg.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    #[tokio::test]
    async fn wait_or_cancel_returns_false_when_cancelled() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let r = wait_or_cancel(&cancel, Duration::from_secs(60)).await;
        assert!(!r);
    }
}
