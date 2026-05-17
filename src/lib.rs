pub mod cli;
pub mod config;
pub mod endpoint;
pub mod error;
pub mod mavlink;
pub mod router;
pub mod shutdown;
pub mod stats;

pub use error::Error;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::endpoint::EndpointId;
use crate::endpoint::EndpointIdAllocator;
use crate::endpoint::defaults::DEFAULT_TX_QUEUE_FRAMES;
use crate::endpoint::events::{EndpointEvent, RouterFrame};
use crate::endpoint::identity_flags::IdentityFlags;
use crate::endpoint::serial::{SerialSpec, SerialWiring};
use crate::endpoint::spec::{EndpointKind, EndpointSpec};
use crate::endpoint::stats::{EndpointState, EndpointStats};
use crate::endpoint::tcp::client::{TcpClientSpec, TcpClientWiring};
use crate::endpoint::tcp::server::{TcpServerSpec, TcpServerWiring};
use crate::endpoint::tx_queue::TxQueue;
use crate::endpoint::udp::client::{UdpClientSpec, UdpClientWiring};
use crate::endpoint::udp::server::{UdpServerSpec, UdpServerWiring};
use crate::router::RouterWiring;
use crate::stats::StatsEvent;

/// CLAUDE.md "Defaults" table: shared reader→router mpsc capacity. Senders
/// `await` on full — backpressure flows to readers rather than silently
/// dropping frames.
const INGRESS_QUEUE_FRAMES: usize = 1024;

/// Default udps peer table size when the spec doesn't override (matches
/// the constant `udp::server` uses internally). Used by `n_estimate` for
/// channel sizing — we never instantiate this many peers up front.
const DEFAULT_UDPS_PEER_CAPACITY: usize = 256;

/// CLAUDE.md "Stats sink architecture": `tcps_peer_budget` defaults to 64
/// per listener as a sizing hint (`tcps:` has no hard cap on accepted
/// clients in v1).
const DEFAULT_TCPS_PEER_BUDGET: usize = 64;

/// Run the rmr top-level: parse CLI specs, spawn the router + stats tasks
/// and one task per endpoint, then wait for shutdown. Each top-level
/// endpoint's `EndpointId`, `Arc<EndpointStats>`, `TxQueue`, and
/// `IdentityFlags` are constructed up front and announced via
/// `EndpointEvent::EndpointAdded` *before* the endpoint task is spawned —
/// the router's biased select then guarantees the registration is
/// processed before any frame stamped with the new `EndpointId`.
pub async fn run(cli: cli::Cli) -> Result<(), Error> {
    cli::init_tracing(cli.log_level, cli.log_format);
    let token = CancellationToken::new();
    let signal_token = token.clone();
    tokio::spawn(async move {
        shutdown::watch_for_shutdown_signal(signal_token).await;
    });
    run_with_cancel(cli, token).await
}

/// Like [`run`], but driven by a caller-supplied cancellation token and
/// without installing the signal handler. Tracing is also assumed to be
/// initialised by the caller. Used by integration tests that need to drive
/// shutdown explicitly; production code path goes through [`run`].
pub async fn run_with_cancel(cli: cli::Cli, token: CancellationToken) -> Result<(), Error> {
    if cli.config.is_some() {
        warn!("--config is accepted but not yet wired up (TOML config lands in phase 6)");
    }

    let specs = cli::parse_specs(&cli.endpoints)?;
    let endpoint_count = specs.len();

    let n_estimate = estimate_registry_size(&specs);
    let event_q_cap = (n_estimate * 2).max(64);
    let stats_q_cap = (n_estimate * 2).max(64);

    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(INGRESS_QUEUE_FRAMES);
    let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(event_q_cap);
    let (stats_event_tx, stats_event_rx) = mpsc::channel::<StatsEvent>(stats_q_cap);

    let mut tasks: JoinSet<()> = JoinSet::new();

    spawn_router(
        &mut tasks,
        frame_rx,
        event_rx,
        stats_event_tx,
        token.clone(),
    );
    spawn_stats(&mut tasks, stats_event_rx, token.clone());
    spawn_endpoints(&mut tasks, &event_tx, &frame_tx, &token, specs).await?;

    // Drop the parent senders so the router's recv() loops observe
    // end-of-input once the last endpoint task exits, not just the cancel
    // token.
    drop(event_tx);
    drop(frame_tx);

    info!(endpoint_count, "rmr started");

    token.cancelled().await;
    info!("shutdown signal received");

    shutdown::shutdown(tasks, Duration::from_secs(cli.shutdown_grace)).await;
    info!("rmr stopped");

    Ok(())
}

/// CLAUDE.md "Stats sink architecture": channel sizing formula
/// N = top-level + sum(udps_peer_capacity) + sum(tcps_peer_budget).
fn estimate_registry_size(specs: &[EndpointSpec]) -> usize {
    let mut n = specs.len();
    for s in specs {
        match &s.kind {
            EndpointKind::UdpServer(e) => {
                n = n.saturating_add(e.udps_peer_capacity.unwrap_or(DEFAULT_UDPS_PEER_CAPACITY))
            }
            EndpointKind::TcpServer(_) => n = n.saturating_add(DEFAULT_TCPS_PEER_BUDGET),
            _ => {}
        }
    }
    n
}

fn spawn_router(
    tasks: &mut JoinSet<()>,
    frame_rx: mpsc::Receiver<RouterFrame>,
    event_rx: mpsc::Receiver<EndpointEvent>,
    stats_event_tx: mpsc::Sender<StatsEvent>,
    cancel: CancellationToken,
) {
    let wiring = RouterWiring {
        frame_rx,
        event_rx,
        stats_event_tx,
        cancel,
    };
    tasks.spawn(async move {
        router::run(wiring).await;
    });
}

fn spawn_stats(
    tasks: &mut JoinSet<()>,
    stats_event_rx: mpsc::Receiver<StatsEvent>,
    cancel: CancellationToken,
) {
    tasks.spawn(async move {
        stats::run(stats_event_rx, cancel).await;
    });
}

async fn spawn_endpoints(
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
/// per-kind `TxQueue`), announce the appropriate lifecycle event to the
/// router, and spawn the endpoint task. Single `match kind` dispatch — leaf
/// arms (`tcpc:` / `udpc:` / `serial:`) build a `TxQueue` and emit
/// `EndpointAdded`; parent-listener arms (`tcps:` / `udps:`) emit
/// `ParentListenerAdded` with no `TxQueue`. The lifecycle event is awaited
/// to completion BEFORE the endpoint task is spawned so the router's biased
/// select sees the registration before any frame stamped with the new
/// `EndpointId` (CLAUDE.md "Endpoint registration is symmetric"). On a
/// closed event channel the spawn is silently skipped — the rest of the
/// router has already torn down.
async fn spawn_endpoint(
    tasks: &mut JoinSet<()>,
    allocator: &Arc<EndpointIdAllocator>,
    event_tx: &mpsc::Sender<EndpointEvent>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    cancel: &CancellationToken,
    spec: EndpointSpec,
) -> Result<(), Error> {
    let EndpointSpec {
        kind,
        name,
        explicit_name: _,
    } = spec;
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));

    match kind {
        EndpointKind::Serial(ep) => {
            let tx_queue = TxQueue::new(
                ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
                stats.clone(),
            );
            let identity = ep.identity.clone();
            if !announce_leaf(
                event_tx,
                endpoint_id,
                name.clone(),
                tx_queue.clone(),
                stats.clone(),
                identity,
            )
            .await
            {
                return Ok(());
            }
            let spec = SerialSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = SerialWiring {
                frame_tx: frame_tx.clone(),
                tx_queue,
                stats,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move {
                endpoint::serial::run(spec, wiring).await;
            });
        }
        EndpointKind::TcpClient(ep) => {
            let tx_queue = TxQueue::new(
                ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
                stats.clone(),
            );
            let identity = ep.identity.clone();
            if !announce_leaf(
                event_tx,
                endpoint_id,
                name.clone(),
                tx_queue.clone(),
                stats.clone(),
                identity,
            )
            .await
            {
                return Ok(());
            }
            let spec = TcpClientSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = TcpClientWiring {
                frame_tx: frame_tx.clone(),
                tx_queue,
                stats,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move {
                endpoint::tcp::client::run(spec, wiring).await;
            });
        }
        EndpointKind::UdpClient(ep) => {
            let tx_queue = TxQueue::new(
                ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
                stats.clone(),
            );
            let identity = ep.identity.clone();
            if !announce_leaf(
                event_tx,
                endpoint_id,
                name.clone(),
                tx_queue.clone(),
                stats.clone(),
                identity,
            )
            .await
            {
                return Ok(());
            }
            let spec = UdpClientSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = UdpClientWiring {
                frame_tx: frame_tx.clone(),
                tx_queue,
                stats,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move {
                endpoint::udp::client::run(spec, wiring).await;
            });
        }
        EndpointKind::TcpServer(ep) => {
            if !announce_parent_listener(event_tx, endpoint_id, name.clone(), stats.clone()).await {
                return Ok(());
            }
            let spec = TcpServerSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = TcpServerWiring {
                allocator: allocator.clone(),
                frame_tx: frame_tx.clone(),
                event_tx: event_tx.clone(),
                cancel: cancel.clone(),
                bound_addr_tx: None,
                stats,
            };
            tasks.spawn(async move {
                endpoint::tcp::server::run(spec, wiring).await;
            });
        }
        EndpointKind::UdpServer(ep) => {
            if !announce_parent_listener(event_tx, endpoint_id, name.clone(), stats.clone()).await {
                return Ok(());
            }
            let spec = UdpServerSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = UdpServerWiring {
                allocator: allocator.clone(),
                frame_tx: frame_tx.clone(),
                event_tx: event_tx.clone(),
                cancel: cancel.clone(),
                bound_addr_tx: None,
                stats,
            };
            tasks.spawn(async move {
                endpoint::udp::server::run(spec, wiring).await;
            });
        }
    }
    Ok(())
}

/// Fire `EndpointAdded` for a leaf routing endpoint. Returns `true` on
/// successful delivery and `false` when the channel is closed — the caller
/// then skips the spawn so the router never sees frames from an unknown
/// `EndpointId`. A closed channel here means the router has already exited;
/// the warning surfaces that asymmetry without aborting the whole spawn loop.
async fn announce_leaf(
    event_tx: &mpsc::Sender<EndpointEvent>,
    id: EndpointId,
    name: String,
    tx_queue: TxQueue,
    stats: Arc<EndpointStats>,
    identity: IdentityFlags,
) -> bool {
    if event_tx
        .send(EndpointEvent::EndpointAdded {
            id,
            name: name.clone(),
            tx_queue,
            stats,
            identity,
        })
        .await
        .is_err()
    {
        warn!(
            endpoint_id = %id, %name,
            "router event channel closed during endpoint registration; skipping spawn"
        );
        return false;
    }
    true
}

/// `ParentListenerAdded` counterpart of [`announce_leaf`]. No `TxQueue` or
/// `IdentityFlags` because parent listeners aren't routing destinations.
async fn announce_parent_listener(
    event_tx: &mpsc::Sender<EndpointEvent>,
    id: EndpointId,
    name: String,
    stats: Arc<EndpointStats>,
) -> bool {
    if event_tx
        .send(EndpointEvent::ParentListenerAdded {
            id,
            name: name.clone(),
            stats,
        })
        .await
        .is_err()
    {
        warn!(
            endpoint_id = %id, %name,
            "router event channel closed during parent-listener registration; skipping spawn"
        );
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_registry_size_counts_top_level_only_for_clients() {
        let specs = vec![
            EndpointSpec::parse("tcpc:127.0.0.1:1").unwrap(),
            EndpointSpec::parse("udpc:127.0.0.1:2").unwrap(),
            EndpointSpec::parse("serial:/dev/null:115200").unwrap(),
        ];
        assert_eq!(estimate_registry_size(&specs), 3);
    }

    #[test]
    fn estimate_registry_size_adds_peer_budgets_for_listeners() {
        let specs = vec![
            EndpointSpec::parse("udps:0.0.0.0:1").unwrap(),
            EndpointSpec::parse("tcps:0.0.0.0:2").unwrap(),
        ];
        let n = estimate_registry_size(&specs);
        assert_eq!(n, 2 + DEFAULT_UDPS_PEER_CAPACITY + DEFAULT_TCPS_PEER_BUDGET);
    }

    #[test]
    fn estimate_registry_size_uses_udps_peer_capacity_override() {
        let specs = vec![EndpointSpec::parse("udps:0.0.0.0:1?udps_peer_capacity=8").unwrap()];
        assert_eq!(estimate_registry_size(&specs), 1 + 8);
    }
}
