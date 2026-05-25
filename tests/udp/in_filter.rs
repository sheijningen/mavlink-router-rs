//! `udps:` per-peer In-filter coverage. A blocked-msgid frame must increment
//! the peer's `in_filter_drops` and never reach the router's `frame_rx`.
//! The listener evaluates against its own `IdentityFlags` (uniform across
//! children); only the drop credit goes to the peer's stats.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::filters::{Filters, MsgIdRange};
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::spec::UdpServerEndpoint;
use rmr::endpoint::stats::EndpointState;
use rmr::endpoint::udp::server::UdpServerSpec;

use crate::common;
use crate::common::udp::{pick_free_udp_addr, spawn_udps_with_spec};
use crate::common::{next_peer_added, shutdown_all, wait_for_state};

#[tokio::test]
async fn udps_in_filter_blocks_msgid_at_listener() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    // HEARTBEAT (msgid 0) is blocklisted at ingress on this listener.
    let parent_identity = IdentityFlags {
        filters: Filters {
            block_msgid_in: vec![MsgIdRange::single(0)],
            ..Filters::default()
        },
        ..IdentityFlags::default()
    };

    let endpoint = UdpServerEndpoint {
        bind_addr: pick_free_udp_addr(),
        ..UdpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let mut spec = UdpServerSpec::from_endpoint(endpoint, parent_id, "udps-in-filter".to_string());
    spec.identity = parent_identity;
    let mut harness = spawn_udps_with_spec(&allocator, cancel.clone(), spec);
    wait_for_state(&harness.stats, EndpointState::Connected, "udps bind").await;

    let peer = UdpSocket::bind("127.0.0.1:0").await.expect("peer bind");
    let frame = common::build_v2_heartbeat(0);
    peer.send_to(&frame, harness.listen_addr)
        .await
        .expect("peer send_to listener");

    // The peer is admitted (PeerAdded fires) — the In-filter only suppresses
    // forwarding of individual frames, not admission of the routing endpoint.
    let added = next_peer_added(&mut harness.event_rx).await;

    // Poll the peer's stats until `in_filter_drops` has incremented (the
    // listener runs the framer + filter asynchronously after admission).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if added.stats.in_filter_drops.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        added.stats.in_filter_drops.load(Ordering::Relaxed),
        1,
        "peer in_filter_drops did not increment"
    );
    assert_eq!(
        added.stats.rx_frames.load(Ordering::Relaxed),
        1,
        "framer should still count rx_frames at the wire"
    );
    // The frame must never reach the router-stub.
    assert!(
        harness.frame_rx.try_recv().is_err(),
        "blocked frame leaked through to frame_rx"
    );

    shutdown_all(&cancel, std::iter::once(harness.task)).await;
}
