use std::net::SocketAddr;
use std::sync::Arc;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, trace, warn};

use super::super::EndpointId;
use super::super::EndpointIdAllocator;
use super::super::backoff::{Backoff, BindOutcome, bind_with_backoff};
use super::super::defaults::{
    DEFAULT_READ_BUF_BYTES, DEFAULT_RECONNECT_INITIAL_MS, DEFAULT_RECONNECT_MAX_MS,
    DEFAULT_TX_QUEUE_FRAMES,
};
use super::super::events::{EndpointEvent, PeerRemovalReason, RouterFrame};
use super::super::identity_flags::IdentityFlags;
use super::super::peer_endpoint_name;
use super::super::session::{SessionOutcome, run_session};
use super::super::socket::{bind_tcp_dual_stack, configure_tcp_stream};
use super::super::spec::TcpServerEndpoint;
use super::super::stats::{EndpointState, EndpointStats};
use super::super::tx_queue::TxQueue;

/// Typed-empty return for `tcps:` `run()`. Bind failures enter the same
/// backoff loop as `tcpc:` reconnects, accept errors are logged and the loop
/// continues, and per-child disconnects are routine — no terminal failure
/// modes remain in v1. Kept as a typed return for symmetry with the other
/// endpoint modules in case a fatal case shows up later.
#[derive(Debug, Error)]
pub enum TcpServerError {}

/// Inputs that distinguish one `tcps:` listener from another: where to bind,
/// what to call it, and the per-listener knobs from the query string with
/// CLAUDE.md defaults already substituted. The `reconnect_*_ms` fields are
/// always the `tcpc:` curve (CLAUDE.md: "TCP/UDP server bind reuses the
/// `tcpc:` backoff curve") — `tcps:` does not expose per-listener reconnect
/// overrides in v1. `identity` carries the filter / sniffer / group /
/// capacity bundle — inherited by every accepted child at admission time
/// (CLAUDE.md "Sub-endpoints inherit their parent's `IdentityFlags` by clone
/// at spawn time"). Unused until Phase 5 wires it through the reader and
/// the router.
pub struct TcpServerSpec {
    pub listen_addr: SocketAddr,
    pub parent_id: EndpointId,
    pub parent_name: String,
    pub read_buf_bytes: usize,
    pub tx_queue_frames: usize,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub identity: IdentityFlags,
}

impl TcpServerSpec {
    /// Build a runtime `TcpServerSpec` from the parsed-but-not-defaulted
    /// `TcpServerEndpoint` the CLI/TOML layer produced, substituting CLAUDE.md
    /// defaults for any unset knob. The spawner supplies `parent_id` and
    /// `parent_name` because the parser doesn't allocate IDs.
    pub fn from_endpoint(
        ep: TcpServerEndpoint,
        parent_id: EndpointId,
        parent_name: String,
    ) -> Self {
        Self {
            listen_addr: ep.bind_addr,
            parent_id,
            parent_name,
            read_buf_bytes: ep.common.read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES),
            tx_queue_frames: ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
            identity: ep.identity,
        }
    }
}

/// Shared wiring every endpoint needs: the global EndpointId allocator,
/// the reader→router frame channel, the sub-endpoint lifecycle channel,
/// and the cancellation token. `bound_addr_tx`, if set, fires once on the
/// first successful bind with the actual `local_addr()` — lets a caller
/// that requested `127.0.0.1:0` (test harnesses, future systemd-socket
/// adoption) discover the OS-assigned port. `stats` is the parent
/// listener's own `Arc<EndpointStats>` (spawner-constructed via
/// `EndpointStats::new(Reconnecting)`); the listener writes `Connected`
/// once bind succeeds.
pub struct TcpServerWiring {
    pub allocator: Arc<EndpointIdAllocator>,
    pub frame_tx: mpsc::Sender<RouterFrame>,
    pub event_tx: mpsc::Sender<EndpointEvent>,
    pub cancel: CancellationToken,
    pub bound_addr_tx: Option<oneshot::Sender<SocketAddr>>,
    pub stats: Arc<EndpointStats>,
}

/// Run a `tcps:` listener until the cancellation token fires. Binding is
/// retried with the shared capped-exp backoff (CLAUDE.md "Initial bind/dial
/// failure path"), so a port collision at startup logs at WARN and the
/// listener attaches as soon as the port frees. Each accepted client becomes
/// its own routing endpoint announced via `event_tx`.
pub async fn run(spec: TcpServerSpec, wiring: TcpServerWiring) -> Result<(), TcpServerError> {
    let span = info_span!("tcps", name = %spec.parent_name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: TcpServerSpec, wiring: TcpServerWiring) -> Result<(), TcpServerError> {
    let TcpServerSpec {
        listen_addr,
        parent_id,
        parent_name,
        read_buf_bytes,
        tx_queue_frames,
        reconnect_initial_ms,
        reconnect_max_ms,
        identity,
    } = spec;
    let TcpServerWiring {
        allocator,
        frame_tx,
        event_tx,
        cancel,
        mut bound_addr_tx,
        stats,
    } = wiring;

    let mut backoff = Backoff::new(reconnect_initial_ms, reconnect_max_ms);

    loop {
        let listener = match bind_with_backoff(&cancel, &mut backoff, "tcps", listen_addr, || {
            bind_tcp_dual_stack(listen_addr)
        })
        .await
        {
            BindOutcome::Bound(l) => l,
            BindOutcome::Cancelled => return Ok(()),
        };
        stats.store_state(EndpointState::Connected);
        let bound_addr = listener.local_addr().unwrap_or(listen_addr);
        if let Some(tx) = bound_addr_tx.take() {
            let _ = tx.send(bound_addr);
        }
        info!(%bound_addr, parent_id = %parent_id, "tcps listening");

        run_accept_loop(
            listener,
            parent_id,
            &parent_name,
            read_buf_bytes,
            tx_queue_frames,
            &identity,
            &allocator,
            &frame_tx,
            &event_tx,
            &cancel,
        )
        .await;

        // Today accept_loop only returns on cancellation, so the top-of-loop
        // cancel check terminates `run_inner` on the next iteration. Falling
        // through (rather than an explicit `return`) leaves the door open for
        // a future listener-fatal exit path to re-enter `bind_tcp_dual_stack`
        // with the same backoff curve.
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_accept_loop(
    listener: TcpListener,
    parent_id: EndpointId,
    parent_name: &str,
    read_buf_bytes: usize,
    tx_queue_frames: usize,
    identity: &IdentityFlags,
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
                            read_buf_bytes,
                            tx_queue_frames,
                            identity,
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
    read_buf_bytes: usize,
    tx_queue_frames: usize,
    identity: &IdentityFlags,
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
    // Accepting the connection IS the transport-up event, so the child lands
    // in Connected before the Arc is published to the router.
    let stats = Arc::new(EndpointStats::new(EndpointState::Connected));
    let tx_queue = TxQueue::new(tx_queue_frames, stats.clone());
    let name = peer_endpoint_name(parent_name, peer_addr);
    let child_span = info_span!("tcps_child", name = %name);

    // Announce PeerAdded before spawning the child so the router never sees
    // a RouterFrame for an unknown EndpointId. The child inherits the parent
    // listener's IdentityFlags by clone per CLAUDE.md "Sub-endpoints inherit
    // their parent's IdentityFlags by clone at spawn time".
    if event_tx
        .send(EndpointEvent::PeerAdded {
            parent_id,
            child_id,
            peer_addr,
            name,
            tx_queue: tx_queue.clone(),
            stats: stats.clone(),
            identity: identity.clone(),
        })
        .await
        .is_err()
    {
        debug!("tcps event channel closed; dropping accepted client");
        return;
    }
    trace!(parent_id = %parent_id, %peer_addr, %child_id, "tcps client accepted");

    children.spawn(
        run_client_session(
            stream,
            peer_addr,
            parent_id,
            child_id,
            stats,
            frame_tx.clone(),
            tx_queue,
            event_tx.clone(),
            cancel.clone(),
            read_buf_bytes,
        )
        .instrument(child_span),
    );
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
        SessionOutcome::Terminated => PeerRemovalReason::ListenerShutdown,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_defaults_when_endpoint_unset() {
        let ep = TcpServerEndpoint::default();
        let spec = TcpServerSpec::from_endpoint(ep, EndpointId(0), "n".into());
        assert_eq!(spec.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(spec.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
        assert_eq!(spec.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(spec.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }

    #[test]
    fn spec_overrides_common_fields() {
        use crate::endpoint::spec::CommonQuery;
        let ep = TcpServerEndpoint {
            common: CommonQuery {
                read_buf_bytes: Some(1024),
                tx_queue_frames: Some(8),
            },
            ..TcpServerEndpoint::default()
        };
        let spec = TcpServerSpec::from_endpoint(ep, EndpointId(0), "n".into());
        assert_eq!(spec.read_buf_bytes, 1024);
        assert_eq!(spec.tx_queue_frames, 8);
        // Reconnect curve stays at the tcpc defaults — CLAUDE.md "TCP/UDP
        // server bind reuses the `tcpc:` backoff curve" and tcps does not
        // expose per-endpoint reconnect overrides in v1.
        assert_eq!(spec.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(spec.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }
}
