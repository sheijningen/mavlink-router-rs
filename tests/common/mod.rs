//! Test fixtures shared between integration tests.

#![allow(dead_code)]

pub mod mavlink;
pub mod tcp;
pub mod udp;

use std::sync::Arc;
use std::time::Duration;

use mavlink::{Heartbeat, TestFrame};
use rmr::config::{Config, LogFormat, LogLevel};
use rmr::endpoint::EndpointId;
use rmr::endpoint::events::EndpointEvent;
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::stats::{EndpointState, EndpointStats};
use rmr::endpoint::tx_queue::TxQueue;
use rmr::parsers::cli::parse_specs;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Build a valid MAVLink v1 HEARTBEAT frame with the given seq.
pub fn build_v1_heartbeat(seq: u8) -> Vec<u8> {
    TestFrame::v1_message(&Heartbeat {
        mav_type: 2,
        autopilot: 3,
        mavlink_version: 3,
        ..Heartbeat::default()
    })
    .seq(seq)
    .build()
}

/// Build a valid MAVLink v2 HEARTBEAT frame with the given seq.
pub fn build_v2_heartbeat(seq: u8) -> Vec<u8> {
    TestFrame::v2_message(&Heartbeat {
        mav_type: 2,
        autopilot: 3,
        mavlink_version: 3,
        ..Heartbeat::default()
    })
    .seq(seq)
    .build()
}

/// Full `EndpointEvent::PeerAdded` payload, returned by `next_peer_added`
/// so callers destructure whichever fields they need.
pub struct PeerAddedPayload {
    pub parent_id: EndpointId,
    pub child_id: EndpointId,
    pub name: String,
    pub tx_queue: TxQueue,
    pub stats: Arc<EndpointStats>,
    pub identity: IdentityFlags,
}

/// Await the next `PeerAdded` event on the given channel, panicking with a
/// descriptive message on timeout, channel close, or wrong event type. Used
/// by both UDP server and TCP server integration tests — the event shape is
/// transport-agnostic.
pub async fn next_peer_added(rx: &mut mpsc::Receiver<EndpointEvent>) -> PeerAddedPayload {
    let event = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event_rx timeout waiting for PeerAdded")
        .expect("event_rx closed before PeerAdded");
    match event {
        EndpointEvent::PeerAdded {
            parent_id,
            child_id,
            name,
            stats,
            routable,
        } => PeerAddedPayload {
            parent_id,
            child_id,
            name,
            tx_queue: routable.tx_queue,
            stats,
            identity: routable.identity,
        },
        other => panic!("expected PeerAdded, got {other:?}"),
    }
}

/// Poll `stats.load_state()` until it reaches `target` or 2s elapse. The
/// state slot is shared between the endpoint task (writes Connected/
/// Reconnecting) and the router/listener (writes Idle/Down); only polling
/// can observe the transition cross-task.
pub async fn wait_for_state(stats: &Arc<EndpointStats>, target: EndpointState, label: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if stats.load_state() == target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "state never reached {target:?} ({label}); last observed: {:?}",
        stats.load_state()
    );
}

/// Trigger cancellation and await every harness task with a 3s timeout
/// each (the per-task drain budget production tasks honour, plus headroom),
/// then panic with every failure observed — a stuck task or a join panic
/// must surface as a test failure, not pass silently. The task's inner
/// `Result` value is opaque to this helper (generic over `T`) and is
/// discarded; callers whose runners can fail late should assert on it
/// themselves.
pub async fn shutdown_all<I, T>(cancel: &CancellationToken, tasks: I)
where
    I: IntoIterator<Item = JoinHandle<T>>,
{
    cancel.cancel();
    let mut failures = Vec::new();
    for (idx, task) in tasks.into_iter().enumerate() {
        match timeout(Duration::from_secs(3), task).await {
            Ok(Ok(_)) => {}
            Ok(Err(join_err)) => failures.push(format!("task #{idx}: join error: {join_err}")),
            Err(_) => failures.push(format!(
                "task #{idx}: did not drain within 3s shutdown budget"
            )),
        }
    }
    assert!(failures.is_empty(), "shutdown_all: {}", failures.join("; "));
}

/// Build a fully-validated [`Config`] from a vector of endpoint spec strings,
/// with the global knobs every `rmr::run`-driven test cares about set the
/// same way: warn-level text logs, stats off, config-dump suppressed,
/// `merged: false`. `dedup_ms` is exposed so the dedup test can flip it
/// without forking a near-identical helper; everyone else passes `0`.
///
/// Panics if any spec string fails to parse or the resulting config fails
/// cross-endpoint validation — both are programming errors in the test, not
/// behaviour under test.
pub fn config_with_endpoints(endpoints: Vec<String>, dedup_ms: u64) -> Config {
    let cfg = Config {
        log_level: LogLevel::Warn,
        log_format: LogFormat::Text,
        stats: false,
        stats_interval_secs: 5,
        dedup_ms,
        skip_config_log: true,
        endpoints: parse_specs(&endpoints).expect("test endpoint strings must parse"),
        merged: false,
    };
    cfg.validate()
        .expect("test config must pass cross-endpoint validation");
    cfg
}
