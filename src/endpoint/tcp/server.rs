use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{Instrument, debug, info, info_span, warn};

use super::super::backoff::{Backoff, BindOutcome, bind_with_backoff};
use super::super::defaults::{
    DEFAULT_RECONNECT_INITIAL_MS, DEFAULT_RECONNECT_MAX_MS, DEFAULT_TX_QUEUE_FRAMES,
};
use super::super::events::{EndpointEvent, PeerRemovalReason, Routable};
use super::super::identity_flags::IdentityFlags;
use super::super::session::{SessionCtx, SessionOutcome, run_session};
use super::super::socket::{bind_tcp_dual_stack, configure_tcp_stream};
use super::super::spec::TcpServerEndpoint;
use super::super::stats::{EndpointState, EndpointStats};
use super::super::tx_queue::TxQueue;
use super::super::wiring::{ClientWiring, ServerWiring};
use super::super::{EndpointId, peer_endpoint_name};

/// Inputs that distinguish one `tcps:` listener from another: where to bind
/// and what to call it. Filter / sniffer / group bundle inherited by every
/// accepted child at admission time.
pub struct TcpServerSpec {
    pub listen_addr: SocketAddr,
    pub parent_id: EndpointId,
    pub parent_name: Arc<str>,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub identity: IdentityFlags,
}

impl TcpServerSpec {
    /// Build a runtime `TcpServerSpec` from the parsed `TcpServerEndpoint`,
    /// stamping the hardcoded reconnect curve. The spawner supplies
    /// `parent_id` and `parent_name` because the parser doesn't allocate IDs.
    pub fn from_endpoint(
        ep: TcpServerEndpoint,
        parent_id: EndpointId,
        parent_name: Arc<str>,
    ) -> Self {
        Self {
            listen_addr: ep.bind_addr,
            parent_id,
            parent_name,
            reconnect_initial_ms: DEFAULT_RECONNECT_INITIAL_MS,
            reconnect_max_ms: DEFAULT_RECONNECT_MAX_MS,
            identity: ep.identity,
        }
    }
}

/// Run a `tcps:` listener until the cancellation token fires. Binding is
/// retried with the shared capped-exp backoff. Each accepted client
/// becomes its own routing endpoint announced via `event_tx`.
pub async fn run(spec: TcpServerSpec, wiring: ServerWiring) {
    let span = info_span!("tcps", name = %spec.parent_name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: TcpServerSpec, wiring: ServerWiring) {
    let mut backoff = Backoff::new(spec.reconnect_initial_ms, spec.reconnect_max_ms);

    loop {
        let listener =
            match bind_with_backoff(&wiring.cancel, &mut backoff, spec.listen_addr, || {
                bind_tcp_dual_stack(spec.listen_addr)
            })
            .await
            {
                BindOutcome::Bound(l) => l,
                BindOutcome::Cancelled => return,
            };
        wiring.stats.store_state(EndpointState::Connected);
        let bound_addr = listener.local_addr().unwrap_or(spec.listen_addr);
        info!(%bound_addr, parent_id = %spec.parent_id, "listening");

        run_accept_loop(listener, &spec, &wiring).await;
    }
}

async fn run_accept_loop(listener: TcpListener, spec: &TcpServerSpec, wiring: &ServerWiring) {
    let mut children: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = wiring.cancel.cancelled() => break,
            res = listener.accept() => {
                match res {
                    Ok((stream, peer_addr)) => {
                        accept_one_client(stream, peer_addr, spec, wiring, &mut children).await;
                    }
                    Err(err) => {
                        // Accept errors are typically per-connection failures
                        // (EMFILE etc.), not listener death; log and continue.
                        warn!(error = %err, "accept failed; continuing");
                    }
                }
            }
        }
    }

    while children.join_next().await.is_some() {}
}

async fn accept_one_client(
    stream: TcpStream,
    peer_addr: SocketAddr,
    spec: &TcpServerSpec,
    wiring: &ServerWiring,
    children: &mut JoinSet<()>,
) {
    if let Err(err) = configure_tcp_stream(&stream) {
        warn!(error = %err, %peer_addr, "configure_tcp_stream failed on accept");
    }

    let child_id = wiring.allocator.alloc();
    // Accepting the connection IS the transport-up event, so the child lands
    // in Connected before the Arc is published to the router.
    let stats = Arc::new(EndpointStats::new(EndpointState::Connected));
    let tx_queue = TxQueue::new(DEFAULT_TX_QUEUE_FRAMES, stats.clone());
    let name = peer_endpoint_name(&spec.parent_name, peer_addr);
    let child_span = info_span!("tcps_child", name = %name);

    // Announce PeerAdded before spawning the child so the router never sees
    // a RouterFrame for an unknown EndpointId. The child inherits the
    // parent listener's IdentityFlags.
    if wiring
        .event_tx
        .send(EndpointEvent::PeerAdded {
            parent_id: spec.parent_id,
            child_id,
            name,
            stats: stats.clone(),
            routable: Routable {
                tx_queue: tx_queue.clone(),
                identity: spec.identity.clone(),
            },
        })
        .await
        .is_err()
    {
        debug!("event channel closed; dropping accepted client");
        return;
    }
    info!(parent_id = %spec.parent_id, %peer_addr, %child_id, "client accepted");

    let child_wiring = ClientWiring {
        frame_tx: wiring.frame_tx.clone(),
        tx_queue,
        stats,
        cancel: wiring.cancel.clone(),
    };
    children.spawn(
        run_client_session(
            stream,
            peer_addr,
            spec.parent_id,
            child_id,
            child_wiring,
            wiring.event_tx.clone(),
            spec.identity.clone(),
        )
        .instrument(child_span),
    );
}

async fn run_client_session(
    stream: TcpStream,
    peer_addr: SocketAddr,
    parent_id: EndpointId,
    child_id: EndpointId,
    wiring: ClientWiring,
    event_tx: mpsc::Sender<EndpointEvent>,
    identity: IdentityFlags,
) {
    let ClientWiring {
        frame_tx,
        tx_queue,
        stats,
        cancel,
    } = wiring;
    let ctx = SessionCtx {
        endpoint_id: child_id,
        stats: &stats,
        frame_tx: &frame_tx,
        filters: &identity.filters,
    };
    let outcome = run_session(stream, &ctx, &tx_queue, &cancel).await;
    let drained = tx_queue.drain_and_discard();
    if drained > 0 {
        debug!(%peer_addr, drained, "discarded in-flight frames on session end");
    }

    let reason = match outcome {
        SessionOutcome::Terminated => PeerRemovalReason::ListenerShutdown,
        SessionOutcome::Disconnected => PeerRemovalReason::Disconnected,
    };
    let _ = event_tx
        .send(EndpointEvent::PeerRemoved {
            parent_id,
            child_id,
            reason,
        })
        .await;
    warn!(parent_id = %parent_id, %peer_addr, %child_id, ?reason, "client session ended");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_defaults_when_endpoint_unset() {
        let ep = TcpServerEndpoint::default();
        let spec = TcpServerSpec::from_endpoint(ep, EndpointId(0), "n".into());
        assert_eq!(spec.reconnect_initial_ms, DEFAULT_RECONNECT_INITIAL_MS);
        assert_eq!(spec.reconnect_max_ms, DEFAULT_RECONNECT_MAX_MS);
    }
}
