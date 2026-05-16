//! `udps:` learned peers must inherit a clone of the parent listener's
//! `IdentityFlags` via `PeerAdded` (CLAUDE.md "Sub-endpoints inherit their
//! parent's IdentityFlags by clone at spawn time").

use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::events::EndpointEvent;
use rmr::endpoint::filters::{Filters, MsgIdRange, U8Range};
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::udp::server::UdpServerConfig;

use crate::common;
use crate::common::shutdown_all;
use crate::common::udp::spawn_udps_at_with_config_and_identity;

#[tokio::test]
async fn udps_peer_inherits_parent_identity() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    // Parent listener with a deliberately non-default identity so every
    // field that matters (filter ranges, sniffer flag, group label,
    // capacities) is visibly different from `IdentityFlags::default()`.
    let parent_identity = IdentityFlags {
        sniffer: true,
        group: Some(Arc::from("uplink")),
        learn_capacity: 13,
        seq_tracker_capacity: 5,
        filters: Filters {
            allow_msgid_out: vec![MsgIdRange::single(0), MsgIdRange { lo: 30, hi: 40 }],
            block_src_comp_in: vec![U8Range::single(42)],
            ..Filters::default()
        },
    };

    let mut harness = spawn_udps_at_with_config_and_identity(
        &allocator,
        cancel.clone(),
        "127.0.0.1:0".parse().expect("parse listen_addr"),
        UdpServerConfig::default(),
        parent_identity.clone(),
        "udps-id",
    );
    let bound = harness
        .bound_addr_rx
        .take()
        .expect("bound_addr_rx")
        .await
        .expect("udps bound_addr_tx dropped");
    harness.listen_addr = bound;

    // Fire one heartbeat from a synthetic peer so the listener admits it
    // and emits `PeerAdded`.
    let peer = UdpSocket::bind("127.0.0.1:0").await.expect("peer bind");
    let frame = common::build_v2_heartbeat(0);
    peer.send_to(&frame, harness.listen_addr)
        .await
        .expect("peer send_to listener");

    let ev = timeout(Duration::from_secs(2), harness.event_rx.recv())
        .await
        .expect("event_rx timeout")
        .expect("event_rx closed");
    let identity = match ev {
        EndpointEvent::PeerAdded { identity, .. } => identity,
        other => panic!("expected PeerAdded, got {other:?}"),
    };
    assert_eq!(identity, parent_identity);

    shutdown_all(&cancel, std::iter::once(harness.task)).await;
}
