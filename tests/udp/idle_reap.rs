//! End-to-end: a `udps:` reaps a learned peer after its `idle_secs`
//! window and announces `PeerRemovalReason::Idle` via the real reaper
//! interval.

use std::sync::Arc;
use std::time::Duration;

use rmr::endpoint::events::{EndpointEvent, PeerRemovalReason};
use rmr::endpoint::spec::UdpServerEndpoint;
use rmr::endpoint::{EndpointIdAllocator, peer_endpoint_name};
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::common;
use crate::common::udp::spawn_udps_with_endpoint;
use crate::common::{next_peer_added, shutdown_all};

#[tokio::test]
async fn udps_reaps_peer_after_idle_secs() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    // idle_secs = 1 + reap interval = 1s → reap fires ~2s after the peer is
    // last seen. Bound the overall test at ~5s.
    let endpoint = UdpServerEndpoint {
        idle_secs: Some(1),
        ..UdpServerEndpoint::default()
    };
    let mut harness = spawn_udps_with_endpoint(&allocator, cancel.clone(), "a", endpoint).await;

    // Synthetic peer sends one HEARTBEAT so the listener learns it, then stays
    // silent so the reaper can pick it up.
    let peer = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer");
    let peer_addr = peer.local_addr().expect("peer local_addr");
    let frame = common::build_v2_heartbeat(0);
    peer.send_to(&frame, harness.listen_addr)
        .await
        .expect("peer send_to listener");

    let expected_name = peer_endpoint_name("a", peer_addr);
    let added = next_peer_added(&mut harness.event_rx).await;
    assert_eq!(added.name, expected_name);
    let initial_child_id = added.child_id;

    // Drain the inbound frame so the listener task isn't backpressured on
    // frame_tx while we wait for the reaper.
    let _ = timeout(Duration::from_secs(2), harness.frame_rx.recv())
        .await
        .expect("frame_rx timeout")
        .expect("frame_rx closed");

    let removed = timeout(Duration::from_secs(5), harness.event_rx.recv())
        .await
        .expect("event_rx timeout waiting for PeerRemoved")
        .expect("event_rx closed before PeerRemoved");
    match removed {
        EndpointEvent::PeerRemoved {
            child_id, reason, ..
        } => {
            assert_eq!(child_id, initial_child_id);
            assert_eq!(reason, PeerRemovalReason::Idle);
        }
        other => panic!("expected PeerRemoved{{Idle}}, got {other:?}"),
    }

    // A second HEARTBEAT from the same socket should be treated as a *new*
    // peer (fresh learn-set, new child_id) — proves the listener is healthy
    // and the entry was fully removed, not merely flagged.
    let frame2 = common::build_v2_heartbeat(1);
    peer.send_to(&frame2, harness.listen_addr)
        .await
        .expect("peer send_to listener (re-learn)");
    let re_added = next_peer_added(&mut harness.event_rx).await;
    assert_eq!(re_added.name, expected_name);
    assert_ne!(re_added.child_id, initial_child_id);

    shutdown_all(&cancel, [harness.task]).await;
}
