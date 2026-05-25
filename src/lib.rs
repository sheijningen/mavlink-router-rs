pub mod config;
pub mod endpoint;
pub mod error;
pub mod log;
pub mod mavlink;
pub mod parsers;
pub mod router;
pub mod shutdown;
pub mod stats;

use std::collections::HashMap;
use std::sync::Arc;

use error::Error;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};

use crate::config::Config;
use crate::endpoint::defaults::DEFAULT_DEDUP_WINDOW_CAPACITY;
use crate::endpoint::events::{EndpointEvent, RouterFrame};
use crate::endpoint::spawn::spawn_endpoints;
use crate::endpoint::spec::{EndpointKind, EndpointSpec};
use crate::endpoint::udp::server::DEFAULT_PEER_CAPACITY;
use crate::router::RouterWiring;
use crate::stats::{DEFAULT_STATS_QUEUE_LINES, StatsEvent, StatsRunConfig};

/// Shared reader→router mpsc capacity. Senders `await` on full so
/// backpressure flows to readers instead of dropping frames.
const INGRESS_QUEUE_FRAMES: usize = 1024;

/// Per-listener `tcps:` peer-count sizing hint (no hard cap on accepted
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
        cfg.log_resolved();
    }

    let Config {
        stats,
        stats_interval_secs,
        dedup_ms,
        endpoints: specs,
        log_level: _,
        log_format: _,
        skip_config_log: _,
        merged: _,
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

    // Drop parent senders so the router observes end-of-input once the
    // last endpoint task exits, not just the cancel token.
    drop(event_tx);
    drop(frame_tx);

    info!(endpoint_count, "rmr started");

    token.cancelled().await;
    info!("shutdown signal received");

    shutdown::shutdown(tasks, shutdown::SHUTDOWN_GRACE).await;
    info!("rmr stopped");

    Ok(())
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

/// Groups share a learn-set across redundant-uplink legs; without dedup the
/// duplicate uplink frame fans out to every destination twice. The WARN
/// surfaces an almost-certain misconfig without changing behaviour.
fn warn_on_groups_without_dedup(specs: &[EndpointSpec], dedup_ms: u64) {
    for (name, count) in groups_needing_dedup_warning(specs, dedup_ms) {
        warn!(
            group = %name,
            members = count,
            "group shares a learn-set but --dedup-ms=0; if its members deliver duplicate uplink frames, set --dedup-ms (e.g. 100) to suppress them"
        );
    }
}

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
    let span = info_span!("router");
    tasks.spawn(
        async move {
            router::run(wiring).await;
        }
        .instrument(span),
    );
}

fn spawn_stats(
    tasks: &mut JoinSet<()>,
    stats_event_rx: mpsc::Receiver<StatsEvent>,
    cancel: CancellationToken,
    cfg: StatsRunConfig,
) {
    let span = info_span!("stats");
    tasks.spawn(
        async move {
            stats::run(stats_event_rx, cancel, cfg, tokio::io::stdout()).await;
        }
        .instrument(span),
    );
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
