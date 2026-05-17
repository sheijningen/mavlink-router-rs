//! Test fixtures shared between integration tests.

#![allow(dead_code)]

pub mod mavlink;
pub mod tcp;
pub mod udp;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mavlink::{Heartbeat, TestFrame};
use rmr::endpoint::EndpointId;
use rmr::endpoint::events::EndpointEvent;
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::stats::{EndpointState, EndpointStats};
use rmr::endpoint::tx_queue::TxQueue;
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
    pub peer_addr: SocketAddr,
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
    let ev = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event_rx timeout waiting for PeerAdded")
        .expect("event_rx closed before PeerAdded");
    match ev {
        EndpointEvent::PeerAdded {
            parent_id,
            child_id,
            peer_addr,
            name,
            tx_queue,
            stats,
            identity,
        } => PeerAddedPayload {
            parent_id,
            child_id,
            peer_addr,
            name,
            tx_queue,
            stats,
            identity,
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

/// Trigger cancellation and await every harness task with a 3s timeout each.
/// Mirrors the per-task drain budget the production tasks honour.
pub async fn shutdown_all<I, T>(cancel: &CancellationToken, tasks: I)
where
    I: IntoIterator<Item = JoinHandle<T>>,
{
    cancel.cancel();
    for task in tasks {
        let _ = timeout(Duration::from_secs(3), task).await;
    }
}
