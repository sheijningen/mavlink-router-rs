pub mod cli;
pub mod config;
pub mod endpoint;
pub mod error;
pub mod mavlink;
pub mod router;
pub mod shutdown;
pub mod stats;

pub use error::Error;

use std::net::{IpAddr, SocketAddr};
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

fn identity_of(kind: &EndpointKind) -> IdentityFlags {
    match kind {
        EndpointKind::Serial(e) => e.identity.clone(),
        EndpointKind::TcpClient(e) => e.identity.clone(),
        EndpointKind::TcpServer(e) => e.identity.clone(),
        EndpointKind::UdpClient(e) => e.identity.clone(),
        EndpointKind::UdpServer(e) => e.identity.clone(),
    }
}

fn tx_queue_frames_of(kind: &EndpointKind) -> usize {
    let common = match kind {
        EndpointKind::Serial(e) => &e.common,
        EndpointKind::TcpClient(e) => &e.common,
        EndpointKind::TcpServer(e) => &e.common,
        EndpointKind::UdpClient(e) => &e.common,
        EndpointKind::UdpServer(e) => &e.common,
    };
    common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES)
}

/// `tcps:` / `udps:` parent listeners exist as supervisory entries: they
/// hold stats and an `EndpointId` but their `TxQueue` has no consumer
/// (children own real readers/writers). The router must skip them as
/// routing destinations, otherwise broadcast frames pile up and inflate
/// `dropped_tx` for no reason.
fn is_routable_top_level(kind: &EndpointKind) -> bool {
    !matches!(
        kind,
        EndpointKind::TcpServer(_) | EndpointKind::UdpServer(_)
    )
}

fn listen_addr_for(scheme: &'static str, host: &str, port: u16) -> Result<SocketAddr, Error> {
    let ip: IpAddr = host.parse().map_err(|_| Error::ListenHostNotAnIp {
        scheme,
        host: host.to_string(),
    })?;
    Ok(SocketAddr::new(ip, port))
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
    let identity = identity_of(&kind);
    let tx_queue_frames = tx_queue_frames_of(&kind);
    let tx_queue = TxQueue::new(tx_queue_frames, stats.clone());
    let routable = is_routable_top_level(&kind);

    // CLAUDE.md "Endpoint registration is symmetric": send EndpointAdded
    // and wait for delivery BEFORE spawning the endpoint task, so the
    // router (biased over event_rx then frame_rx) processes the
    // registration before any RouterFrame this endpoint produces.
    if event_tx
        .send(EndpointEvent::EndpointAdded {
            id: endpoint_id,
            name: name.clone(),
            tx_queue: tx_queue.clone(),
            stats: stats.clone(),
            identity,
            routable,
        })
        .await
        .is_err()
    {
        warn!(
            %endpoint_id, %name,
            "router event channel closed during endpoint registration; skipping spawn"
        );
        return Ok(());
    }

    spawn_endpoint_task(
        tasks,
        kind,
        name,
        endpoint_id,
        stats,
        tx_queue,
        frame_tx,
        event_tx,
        cancel,
        allocator,
    )
}

#[allow(clippy::too_many_arguments)]
fn spawn_endpoint_task(
    tasks: &mut JoinSet<()>,
    kind: EndpointKind,
    name: String,
    endpoint_id: EndpointId,
    stats: Arc<EndpointStats>,
    tx_queue: TxQueue,
    frame_tx: &mpsc::Sender<RouterFrame>,
    event_tx: &mpsc::Sender<EndpointEvent>,
    cancel: &CancellationToken,
    allocator: &Arc<EndpointIdAllocator>,
) -> Result<(), Error> {
    match kind {
        EndpointKind::Serial(ep) => {
            let spec = SerialSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = SerialWiring {
                frame_tx: frame_tx.clone(),
                tx_queue,
                stats,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move {
                let _ = endpoint::serial::run(spec, wiring).await;
            });
        }
        EndpointKind::TcpClient(ep) => {
            let spec = TcpClientSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = TcpClientWiring {
                frame_tx: frame_tx.clone(),
                tx_queue,
                stats,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move {
                let _ = endpoint::tcp::client::run(spec, wiring).await;
            });
        }
        EndpointKind::UdpClient(ep) => {
            let spec = UdpClientSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = UdpClientWiring {
                frame_tx: frame_tx.clone(),
                tx_queue,
                stats,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move {
                let _ = endpoint::udp::client::run(spec, wiring).await;
            });
        }
        EndpointKind::TcpServer(ep) => {
            let listen_addr = listen_addr_for("tcps", &ep.host, ep.port)?;
            let spec = TcpServerSpec::from_endpoint(ep, listen_addr, endpoint_id, name);
            let wiring = TcpServerWiring {
                allocator: allocator.clone(),
                frame_tx: frame_tx.clone(),
                event_tx: event_tx.clone(),
                cancel: cancel.clone(),
                bound_addr_tx: None,
                stats,
            };
            tasks.spawn(async move {
                let _ = endpoint::tcp::server::run(spec, wiring).await;
            });
        }
        EndpointKind::UdpServer(ep) => {
            let listen_addr = listen_addr_for("udps", &ep.host, ep.port)?;
            let spec = UdpServerSpec::from_endpoint(ep, listen_addr, endpoint_id, name);
            let wiring = UdpServerWiring {
                allocator: allocator.clone(),
                frame_tx: frame_tx.clone(),
                event_tx: event_tx.clone(),
                cancel: cancel.clone(),
                bound_addr_tx: None,
                stats,
            };
            tasks.spawn(async move {
                let _ = endpoint::udp::server::run(spec, wiring).await;
            });
        }
    }
    Ok(())
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

    #[test]
    fn listen_addr_for_accepts_ipv4_literal() {
        let addr = listen_addr_for("udps", "0.0.0.0", 14550).unwrap();
        assert_eq!(addr.port(), 14550);
        assert!(addr.is_ipv4());
    }

    #[test]
    fn listen_addr_for_accepts_ipv6_literal() {
        let addr = listen_addr_for("udps", "::", 14550).unwrap();
        assert_eq!(addr.port(), 14550);
        assert!(addr.is_ipv6());
    }

    #[test]
    fn is_routable_top_level_marks_leaves_routable() {
        for input in &[
            "tcpc:127.0.0.1:5760",
            "udpc:127.0.0.1:14550",
            "serial:/dev/null:115200",
        ] {
            let spec = EndpointSpec::parse(input).unwrap();
            assert!(
                is_routable_top_level(&spec.kind),
                "{input} should be routable"
            );
        }
    }

    #[test]
    fn is_routable_top_level_marks_listeners_non_routable() {
        for input in &["tcps:0.0.0.0:5760", "udps:0.0.0.0:14550"] {
            let spec = EndpointSpec::parse(input).unwrap();
            assert!(
                !is_routable_top_level(&spec.kind),
                "{input} should NOT be routable (parent listener)"
            );
        }
    }

    #[test]
    fn listen_addr_for_rejects_hostname() {
        match listen_addr_for("tcps", "localhost", 5760) {
            Err(Error::ListenHostNotAnIp { scheme, host }) => {
                assert_eq!(scheme, "tcps");
                assert_eq!(host, "localhost");
            }
            other => panic!("expected ListenHostNotAnIp, got {other:?}"),
        }
    }
}
