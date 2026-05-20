pub mod config;
pub mod endpoint;
pub mod error;
pub mod log;
pub mod mavlink;
pub mod parsers;
pub mod router;
pub mod shutdown;
pub mod stats;

use error::Error;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::endpoint::EndpointId;
use crate::endpoint::EndpointIdAllocator;
use crate::endpoint::defaults::{DEFAULT_DEDUP_WINDOW_CAPACITY, DEFAULT_TX_QUEUE_FRAMES};
use crate::endpoint::events::{EndpointEvent, Routable, RouterFrame};
use crate::endpoint::identity_flags::IdentityFlags;
use crate::endpoint::serial::{SerialSpec, SerialWiring};
use crate::endpoint::spec::{EndpointKind, EndpointSpec};
use crate::endpoint::stats::{EndpointState, EndpointStats};
use crate::endpoint::tcp::client::{TcpClientSpec, TcpClientWiring};
use crate::endpoint::tcp::server::{TcpServerSpec, TcpServerWiring};
use crate::endpoint::tx_queue::TxQueue;
use crate::endpoint::udp::client::{UdpClientSpec, UdpClientWiring};
use crate::endpoint::udp::server::{DEFAULT_PEER_CAPACITY, UdpServerSpec, UdpServerWiring};
use crate::router::RouterWiring;
use crate::stats::{DEFAULT_STATS_QUEUE_LINES, StatsEvent, StatsRunConfig};

/// CLAUDE.md "Defaults" table: shared reader→router mpsc capacity. Senders
/// `await` on full — backpressure flows to readers rather than silently
/// dropping frames.
const INGRESS_QUEUE_FRAMES: usize = 1024;

/// CLAUDE.md "Stats sink architecture": `tcps_peer_budget` defaults to 64
/// per listener as a sizing hint (`tcps:` has no hard cap on accepted
/// clients in v1).
const DEFAULT_TCPS_PEER_BUDGET: usize = 64;

/// Run the rmr top-level from a fully-resolved [`Config`] under a
/// caller-supplied cancellation token. Spawns the router task, the stats
/// task, and one task per endpoint, then waits for shutdown. Each top-level
/// endpoint's `EndpointId`, `Arc<EndpointStats>`, `TxQueue`, and
/// `IdentityFlags` are constructed up front and announced via
/// `EndpointEvent::EndpointAdded` *before* the endpoint task is spawned —
/// the router's biased select then guarantees the registration is processed
/// before any frame stamped with the new `EndpointId`. The caller is
/// responsible for installing the signal handler and initialising tracing.
pub async fn run(cfg: Config, token: CancellationToken) -> Result<(), Error> {
    if !cfg.skip_config_log {
        log_merged_config(&cfg);
    }

    // Exhaustive destructure: adding a Config field forces a touch here, so
    // we can't silently grow the surface without wiring the new knob into
    // the spawner.
    let Config {
        stats,
        stats_interval_secs,
        dedup_ms,
        endpoints: specs,
        log_level: _,
        log_format: _,
        skip_config_log: _,
    } = cfg;

    let endpoint_count = specs.len();

    warn_on_groups_without_dedup(&specs, dedup_ms);

    let registry_estimate = estimate_registry_size(&specs);
    let event_q_cap = (registry_estimate * 2).max(64);
    let stats_q_cap = (registry_estimate * 2).max(64);

    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(INGRESS_QUEUE_FRAMES);
    let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(event_q_cap);
    let (stats_event_tx, stats_event_rx) = mpsc::channel::<StatsEvent>(stats_q_cap);

    let mut tasks: JoinSet<()> = JoinSet::new();

    spawn_router(
        &mut tasks,
        RouterWiring {
            frame_rx,
            event_rx,
            stats_event_tx,
            cancel: token.clone(),
            dedup_ms,
            dedup_window_capacity: DEFAULT_DEDUP_WINDOW_CAPACITY,
        },
    );
    spawn_stats(
        &mut tasks,
        stats_event_rx,
        token.clone(),
        StatsRunConfig {
            enabled: stats,
            interval: std::time::Duration::from_secs(stats_interval_secs),
            queue_capacity: DEFAULT_STATS_QUEUE_LINES,
        },
    );
    spawn_endpoints(&mut tasks, &event_tx, &frame_tx, &token, specs).await?;

    // Drop the parent senders so the router's recv() loops observe
    // end-of-input once the last endpoint task exits, not just the cancel
    // token.
    drop(event_tx);
    drop(frame_tx);

    info!(endpoint_count, "rmr started");

    token.cancelled().await;
    info!("shutdown signal received");

    shutdown::shutdown(tasks, shutdown::SHUTDOWN_GRACE).await;
    info!("rmr stopped");

    Ok(())
}

/// Emit one INFO event capturing every resolved global plus the per-endpoint
/// table rendered via `Debug`, so every defaulted-in `?key=val` is visible
/// in the line. Revisit if any future config field carries a secret.
fn log_merged_config(cfg: &Config) {
    info!(
        log_level = ?cfg.log_level,
        log_format = ?cfg.log_format,
        stats = cfg.stats,
        stats_interval_secs = cfg.stats_interval_secs,
        dedup_ms = cfg.dedup_ms,
        skip_config_log = cfg.skip_config_log,
        endpoint_count = cfg.endpoints.len(),
        endpoints = ?cfg.endpoints,
        "merged config",
    );
}

/// Return every group with two or more non-sniffer members when dedup is
/// off. Sniffers are diagnostic taps, not redundant legs, so they don't
/// count. `dedup_ms > 0` short-circuits to an empty `Vec`.
fn groups_needing_dedup_warning(specs: &[EndpointSpec], dedup_ms: u64) -> Vec<(Arc<str>, usize)> {
    if dedup_ms > 0 {
        return Vec::new();
    }
    let mut by_group: HashMap<Arc<str>, usize> = HashMap::new();
    for spec in specs {
        let identity = spec.kind.identity();
        if identity.sniffer {
            continue;
        }
        let Some(name) = &identity.group else {
            continue;
        };
        *by_group.entry(name.clone()).or_insert(0) += 1;
    }
    let mut groups: Vec<(Arc<str>, usize)> = by_group
        .into_iter()
        .filter(|(_, count)| *count >= 2)
        .collect();
    groups.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    groups
}

/// A group exists to share a learn-set across redundant-uplink legs (CLAUDE.md
/// "Endpoint groups" — LTE + RFD900 sharing `?group=uplink`). When the legs
/// deliver the same vehicle frame and `--dedup-ms=0`, the duplicate fans out
/// to every destination twice. The WARN doesn't change behaviour — some
/// deployments may legitimately not need dedup — it just surfaces what is
/// almost always a misconfig.
fn warn_on_groups_without_dedup(specs: &[EndpointSpec], dedup_ms: u64) {
    for (name, count) in groups_needing_dedup_warning(specs, dedup_ms) {
        warn!(
            group = %name,
            members = count,
            "group shares a learn-set but --dedup-ms=0; if its members deliver duplicate uplink frames, set --dedup-ms (e.g. 100) to suppress them"
        );
    }
}

/// CLAUDE.md "Stats sink architecture": channel sizing formula
/// N = top-level + sum(DEFAULT_PEER_CAPACITY for each udps:) + sum(tcps_peer_budget).
fn estimate_registry_size(specs: &[EndpointSpec]) -> usize {
    let mut count = specs.len();
    for spec in specs {
        match &spec.kind {
            EndpointKind::UdpServer(_) => count = count.saturating_add(DEFAULT_PEER_CAPACITY),
            EndpointKind::TcpServer(_) => count = count.saturating_add(DEFAULT_TCPS_PEER_BUDGET),
            _ => {}
        }
    }
    count
}

fn spawn_router(tasks: &mut JoinSet<()>, wiring: RouterWiring) {
    tasks.spawn(async move {
        router::run(wiring).await;
    });
}

fn spawn_stats(
    tasks: &mut JoinSet<()>,
    stats_event_rx: mpsc::Receiver<StatsEvent>,
    cancel: CancellationToken,
    cfg: StatsRunConfig,
) {
    tasks.spawn(async move {
        stats::run(stats_event_rx, cancel, cfg, tokio::io::stdout()).await;
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
/// `EndpointAdded` with `routable = Some(_)`; parent-listener arms
/// (`tcps:` / `udps:`) emit `EndpointAdded` with `routable = None`. The
/// lifecycle event is awaited to completion BEFORE the endpoint task is
/// spawned so the router's biased select sees the registration before any
/// frame stamped with the new `EndpointId` (CLAUDE.md "Endpoint
/// registration is symmetric"). On a closed event channel the spawn is
/// silently skipped — the rest of the router has already torn down.
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
            let Some(tx_queue) = prepare_leaf(
                event_tx,
                endpoint_id,
                &name,
                stats.clone(),
                ep.common.tx_queue_frames,
                ep.identity.clone(),
            )
            .await
            else {
                return Ok(());
            };
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
            let Some(tx_queue) = prepare_leaf(
                event_tx,
                endpoint_id,
                &name,
                stats.clone(),
                ep.common.tx_queue_frames,
                ep.identity.clone(),
            )
            .await
            else {
                return Ok(());
            };
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
            let Some(tx_queue) = prepare_leaf(
                event_tx,
                endpoint_id,
                &name,
                stats.clone(),
                ep.common.tx_queue_frames,
                ep.identity.clone(),
            )
            .await
            else {
                return Ok(());
            };
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
            if !prepare_parent_listener(event_tx, endpoint_id, &name, stats.clone()).await {
                return Ok(());
            }
            let spec = TcpServerSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = TcpServerWiring {
                allocator: allocator.clone(),
                frame_tx: frame_tx.clone(),
                event_tx: event_tx.clone(),
                cancel: cancel.clone(),
                stats,
            };
            tasks.spawn(async move {
                endpoint::tcp::server::run(spec, wiring).await;
            });
        }
        EndpointKind::UdpServer(ep) => {
            if !prepare_parent_listener(event_tx, endpoint_id, &name, stats.clone()).await {
                return Ok(());
            }
            let spec = UdpServerSpec::from_endpoint(ep, endpoint_id, name);
            let wiring = UdpServerWiring {
                allocator: allocator.clone(),
                frame_tx: frame_tx.clone(),
                event_tx: event_tx.clone(),
                cancel: cancel.clone(),
                stats,
            };
            tasks.spawn(async move {
                endpoint::udp::server::run(spec, wiring).await;
            });
        }
    }
    Ok(())
}

/// Build the per-leaf `TxQueue`, fire `EndpointAdded` with `routable =
/// Some(_)`, and return the queue on successful registration. `None` means
/// the event channel was closed — the router has already exited and the
/// caller must skip the spawn so no frame is ever stamped with an unknown
/// `EndpointId`. The warning surfaces that asymmetry without aborting the
/// rest of the spawn loop.
async fn prepare_leaf(
    event_tx: &mpsc::Sender<EndpointEvent>,
    id: EndpointId,
    name: &str,
    stats: Arc<EndpointStats>,
    tx_queue_frames: Option<usize>,
    identity: IdentityFlags,
) -> Option<TxQueue> {
    let tx_queue = TxQueue::new(
        tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
        stats.clone(),
    );
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
        let count = estimate_registry_size(&specs);
        assert_eq!(count, 2 + DEFAULT_PEER_CAPACITY + DEFAULT_TCPS_PEER_BUDGET);
    }

    fn group_names(groups: &[(Arc<str>, usize)]) -> Vec<(&str, usize)> {
        groups
            .iter()
            .map(|(name, count)| (name.as_ref(), *count))
            .collect()
    }

    #[test]
    fn groups_needing_dedup_warning_ignores_lone_leaves_and_groupless() {
        let specs = vec![
            EndpointSpec::parse("tcpc:127.0.0.1:1?group=alone").unwrap(),
            EndpointSpec::parse("tcpc:127.0.0.1:2").unwrap(),
        ];
        assert!(groups_needing_dedup_warning(&specs, 0).is_empty());
    }

    #[test]
    fn groups_needing_dedup_warning_fires_on_two_leaves() {
        let specs = vec![
            EndpointSpec::parse("tcpc:127.0.0.1:1?group=uplink").unwrap(),
            EndpointSpec::parse("udpc:127.0.0.1:2?group=uplink").unwrap(),
        ];
        assert_eq!(
            group_names(&groups_needing_dedup_warning(&specs, 0)),
            vec![("uplink", 2)]
        );
    }

    #[test]
    fn groups_needing_dedup_warning_ignores_lone_listener_parent() {
        // A single `tcps:` / `udps:` listener with `?group=` declares one
        // member; the warning fires only once a second leg joins the group.
        for body in ["tcps:0.0.0.0:1?group=uplink", "udps:0.0.0.0:1?group=uplink"] {
            let specs = vec![EndpointSpec::parse(body).unwrap()];
            assert!(
                groups_needing_dedup_warning(&specs, 0).is_empty(),
                "lone listener parent should not warn: {body}"
            );
        }
    }

    #[test]
    fn groups_needing_dedup_warning_sorts_by_name() {
        let specs = vec![
            EndpointSpec::parse("tcpc:127.0.0.1:1?group=zeta").unwrap(),
            EndpointSpec::parse("tcpc:127.0.0.1:2?group=alpha").unwrap(),
            EndpointSpec::parse("udpc:127.0.0.1:3?group=zeta").unwrap(),
            EndpointSpec::parse("serial:/dev/null:115200?group=alpha").unwrap(),
        ];
        assert_eq!(
            group_names(&groups_needing_dedup_warning(&specs, 0)),
            vec![("alpha", 2), ("zeta", 2)]
        );
    }

    #[test]
    fn groups_needing_dedup_warning_excludes_sniffers_from_count() {
        // A sniffer in a group is a diagnostic tap; pairing it with one
        // real leg shouldn't fire (effective weight = 1).
        let specs = vec![
            EndpointSpec::parse("tcpc:127.0.0.1:1?group=g").unwrap(),
            EndpointSpec::parse("udpc:127.0.0.1:2?group=g&sniffer=true").unwrap(),
        ];
        assert!(groups_needing_dedup_warning(&specs, 0).is_empty());
    }

    #[test]
    fn groups_needing_dedup_warning_short_circuits_when_dedup_enabled() {
        let specs = vec![
            EndpointSpec::parse("tcpc:127.0.0.1:1?group=uplink").unwrap(),
            EndpointSpec::parse("udpc:127.0.0.1:2?group=uplink").unwrap(),
        ];
        assert!(groups_needing_dedup_warning(&specs, 100).is_empty());
    }

    #[test]
    fn groups_needing_dedup_warning_fires_on_two_listeners() {
        let specs = vec![
            EndpointSpec::parse("tcps:0.0.0.0:1?group=uplink").unwrap(),
            EndpointSpec::parse("udps:0.0.0.0:2?group=uplink").unwrap(),
        ];
        assert_eq!(
            group_names(&groups_needing_dedup_warning(&specs, 0)),
            vec![("uplink", 2)]
        );
    }
}
