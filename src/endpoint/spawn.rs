//! Cross-kind spawn dispatch: turn each parsed [`EndpointSpec`] into the
//! per-scheme runtime task, the per-endpoint registration event, and the
//! per-endpoint [`TxQueue`] / wiring. The router and stats tasks are spawned
//! by [`crate::run`]; everything per-endpoint goes through here.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::defaults::DEFAULT_TX_QUEUE_FRAMES;
use super::events::{EndpointEvent, Routable, RouterFrame};
use super::identity_flags::IdentityFlags;
use super::serial::SerialSpec;
use super::spec::{EndpointKind, EndpointSpec};
use super::stats::{EndpointState, EndpointStats};
use super::tcp::client::TcpClientSpec;
use super::tcp::server::TcpServerSpec;
use super::tx_queue::TxQueue;
use super::udp::client::UdpClientSpec;
use super::udp::server::UdpServerSpec;
use super::wiring::{ClientWiring, ServerWiring};
use super::{EndpointId, EndpointIdAllocator};
use crate::error::Error;

/// Spawn one task per parsed endpoint.
pub async fn spawn_endpoints(
    tasks: &mut JoinSet<()>,
    event_tx: &mpsc::Sender<EndpointEvent>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    cancel: &CancellationToken,
    specs: Vec<EndpointSpec>,
) -> Result<(), Error> {
    let allocator = Arc::new(EndpointIdAllocator::new());
    for spec in specs {
        spawn_endpoint(tasks, &allocator, event_tx, frame_tx, cancel, spec).await?;
    }
    Ok(())
}

/// Construct the endpoint's runtime handles (`EndpointId`, `Arc<EndpointStats>`,
/// per-kind `TxQueue`), announce the lifecycle event, then spawn the
/// endpoint task. The event is awaited BEFORE the spawn so the router's
///  biased select sees the registration before any frame stamped with the
/// new `EndpointId`.
async fn spawn_endpoint(
    tasks: &mut JoinSet<()>,
    allocator: &Arc<EndpointIdAllocator>,
    event_tx: &mpsc::Sender<EndpointEvent>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    cancel: &CancellationToken,
    spec: EndpointSpec,
) -> Result<(), Error> {
    let EndpointSpec { kind, name } = spec;
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
    info!(
        endpoint_id = %endpoint_id,
        scheme = kind.scheme().as_str(),
        %name,
        "spawning endpoint"
    );

    match kind {
        EndpointKind::Serial(ep) => {
            let spec = SerialSpec::from_endpoint(ep, endpoint_id, name.clone());
            let Some(tx_queue) = prepare_leaf(
                event_tx,
                endpoint_id,
                &name,
                stats.clone(),
                spec.identity.clone(),
            )
            .await
            else {
                return Ok(());
            };
            let wiring = ClientWiring {
                frame_tx: frame_tx.clone(),
                tx_queue,
                stats,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move {
                super::serial::run(spec, wiring).await;
            });
        }
        EndpointKind::TcpClient(ep) => {
            let spec = TcpClientSpec::from_endpoint(ep, endpoint_id, name.clone());
            let Some(tx_queue) = prepare_leaf(
                event_tx,
                endpoint_id,
                &name,
                stats.clone(),
                spec.identity.clone(),
            )
            .await
            else {
                return Ok(());
            };
            let wiring = ClientWiring {
                frame_tx: frame_tx.clone(),
                tx_queue,
                stats,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move {
                super::tcp::client::run(spec, wiring).await;
            });
        }
        EndpointKind::UdpClient(ep) => {
            let spec = UdpClientSpec::from_endpoint(ep, endpoint_id, name.clone());
            let Some(tx_queue) = prepare_leaf(
                event_tx,
                endpoint_id,
                &name,
                stats.clone(),
                spec.identity.clone(),
            )
            .await
            else {
                return Ok(());
            };
            let wiring = ClientWiring {
                frame_tx: frame_tx.clone(),
                tx_queue,
                stats,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move {
                super::udp::client::run(spec, wiring).await;
            });
        }
        EndpointKind::TcpServer(ep) => {
            if !prepare_parent_listener(event_tx, endpoint_id, &name, stats.clone()).await {
                return Ok(());
            }
            let spec = TcpServerSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = ServerWiring {
                allocator: allocator.clone(),
                frame_tx: frame_tx.clone(),
                event_tx: event_tx.clone(),
                cancel: cancel.clone(),
                stats,
            };
            tasks.spawn(async move {
                super::tcp::server::run(spec, wiring).await;
            });
        }
        EndpointKind::UdpServer(ep) => {
            if !prepare_parent_listener(event_tx, endpoint_id, &name, stats.clone()).await {
                return Ok(());
            }
            let spec = UdpServerSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = ServerWiring {
                allocator: allocator.clone(),
                frame_tx: frame_tx.clone(),
                event_tx: event_tx.clone(),
                cancel: cancel.clone(),
                stats,
            };
            tasks.spawn(async move {
                super::udp::server::run(spec, wiring).await;
            });
        }
    }
    Ok(())
}

/// Build the per-leaf `TxQueue`, fire `EndpointAdded` with `routable =
/// Some(_)`, and return the queue on successful registration. `None` means
/// the event channel was closed — the router has already exited and the
/// caller must skip the spawn so no frame is ever stamped with an unknown
/// `EndpointId`. The debug-level log surfaces that asymmetry without
/// aborting the rest of the spawn loop.
async fn prepare_leaf(
    event_tx: &mpsc::Sender<EndpointEvent>,
    id: EndpointId,
    name: &str,
    stats: Arc<EndpointStats>,
    identity: IdentityFlags,
) -> Option<TxQueue> {
    let tx_queue = TxQueue::new(DEFAULT_TX_QUEUE_FRAMES, stats.clone());
    if event_tx
        .send(EndpointEvent::EndpointAdded {
            id,
            name: name.to_string(),
            stats,
            routable: Some(Routable {
                tx_queue: tx_queue.clone(),
                identity,
            }),
        })
        .await
        .is_err()
    {
        debug!(
            endpoint_id = %id, %name,
            "router event channel closed during endpoint registration; skipping spawn"
        );
        return None;
    }
    Some(tx_queue)
}

/// Parent-listener counterpart of [`prepare_leaf`]. Fires `EndpointAdded`
/// with `routable = None` — parent listeners have no `TxQueue` consumer
/// (their children own real readers/writers) and the router skips them on
/// dispatch. Returns `false` if the event channel was closed.
async fn prepare_parent_listener(
    event_tx: &mpsc::Sender<EndpointEvent>,
    id: EndpointId,
    name: &str,
    stats: Arc<EndpointStats>,
) -> bool {
    if event_tx
        .send(EndpointEvent::EndpointAdded {
            id,
            name: name.to_string(),
            stats,
            routable: None,
        })
        .await
        .is_err()
    {
        debug!(
            endpoint_id = %id, %name,
            "router event channel closed during parent-listener registration; skipping spawn"
        );
        return false;
    }
    true
}
