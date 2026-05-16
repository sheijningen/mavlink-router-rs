//! End-to-end: a `udps:` listener reaps a learned peer after its `idle_secs`
//! window elapses with no inbound traffic, and announces the eviction on the
//! event channel with `PeerRemovalReason::Idle`.
//!
//! This is the Phase 2 checklist's "peer idle-reap test" exercising the real
//! reaper interval (not the unit-test path that calls `reap_idle_peers`
//! directly).

mod common;

use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::events::{EndpointEvent, PeerRemovalReason};
use rmr::endpoint::udp_server::UdpServerConfig;

use common::udp::spawn_udps_with_config;
use common::{next_peer_added, shutdown_all};

#[tokio::test]
async fn udps_reaps_peer_after_idle_secs() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    // idle_secs = 1 + reap interval = 1s → reap fires ~2s after the peer is
    // last seen. Bound the overall test at ~5s.
    let cfg = UdpServerConfig {
        idle_secs: 1,
        ..UdpServerConfig::default()
    };
    let mut a = spawn_udps_with_config(&allocator, cancel.clone(), "a", cfg);

    // Synthetic peer sends one HEARTBEAT so the listener learns it, then stays
    // silent so the reaper can pick it up.
    let peer = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer");
    let peer_addr = peer.local_addr().expect("peer local_addr");
    let frame = common::build_v2_heartbeat(0);
    peer.send_to(&frame, a.listen_addr)
        .await
        .expect("peer send_to listener");

    let (added_addr, _q) = next_peer_added(&mut a.event_rx).await;
    assert_eq!(added_addr, peer_addr);

    // Drain the inbound frame so the listener task isn't backpressured on
    // frame_tx while we wait for the reaper.
    let _ = timeout(Duration::from_secs(2), a.frame_rx.recv())
        .await
        .expect("frame_rx timeout")
        .expect("frame_rx closed");

    let removed = timeout(Duration::from_secs(5), a.event_rx.recv())
        .await
        .expect("event_rx timeout waiting for PeerRemoved")
        .expect("event_rx closed before PeerRemoved");
    match removed {
        EndpointEvent::PeerRemoved {
            peer_addr: addr,
            reason,
            ..
        } => {
            assert_eq!(addr, peer_addr);
            assert_eq!(reason, PeerRemovalReason::Idle);
        }
        other => panic!("expected PeerRemoved{{Idle}}, got {other:?}"),
    }

    // A second HEARTBEAT from the same socket should be treated as a *new*
    // peer (fresh learn-set, new child_id) — proves the listener is healthy
    // and the entry was fully removed, not merely flagged.
    let frame2 = common::build_v2_heartbeat(1);
    peer.send_to(&frame2, a.listen_addr)
        .await
        .expect("peer send_to listener (re-learn)");
    let (re_added_addr, _q2) = next_peer_added(&mut a.event_rx).await;
    assert_eq!(re_added_addr, peer_addr);

    shutdown_all(&cancel, [a.task]).await;
}
