//! Test fixtures shared between integration tests.

#![allow(dead_code)]

pub mod mavlink;
pub mod tcp;
pub mod udp;

use std::net::SocketAddr;
use std::time::Duration;

use mavlink::{Heartbeat, TestFrame};
use rmr::endpoint::{events::EndpointEvent, tx_queue::TxQueue};
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

/// Await the next `PeerAdded` event on the given channel, panicking with a
/// descriptive message on timeout, channel close, or wrong event type. Used
/// by both UDP server and TCP server integration tests — the event shape is
/// transport-agnostic.
pub async fn next_peer_added(rx: &mut mpsc::Receiver<EndpointEvent>) -> (SocketAddr, TxQueue) {
    let ev = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event_rx timeout waiting for PeerAdded")
        .expect("event_rx closed before PeerAdded");
    match ev {
        EndpointEvent::PeerAdded {
            peer_addr,
            tx_queue,
            ..
        } => (peer_addr, tx_queue),
        other => panic!("expected PeerAdded, got {other:?}"),
    }
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
