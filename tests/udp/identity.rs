//! `udps:` learned peers must inherit a clone of the parent listener's
//! `IdentityFlags` via `PeerAdded` (CLAUDE.md "Sub-endpoints inherit their
//! parent's IdentityFlags by clone at spawn time").

use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::filters::{Filters, MsgIdRange, U8Range};
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::spec::UdpServerEndpoint;
use rmr::endpoint::stats::EndpointState;
use rmr::endpoint::udp::server::UdpServerSpec;

use crate::common;
use crate::common::udp::{pick_free_udp_addr, spawn_udps_with_spec};
use crate::common::{next_peer_added, shutdown_all, wait_for_state};

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
        filters: Filters {
            allow_msgid_out: vec![MsgIdRange::single(0), MsgIdRange { lo: 30, hi: 40 }],
            block_src_comp_in: vec![U8Range::single(42)],
            ..Filters::default()
        },
    };

    let endpoint = UdpServerEndpoint {
        bind_addr: pick_free_udp_addr(),
        ..UdpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let mut spec = UdpServerSpec::from_endpoint(endpoint, parent_id, "udps-id".to_string());
    spec.identity = parent_identity.clone();
    let mut harness = spawn_udps_with_spec(&allocator, cancel.clone(), spec);
    wait_for_state(&harness.stats, EndpointState::Connected, "udps bind").await;

    // Fire one heartbeat from a synthetic peer so the listener admits it
    // and emits `PeerAdded`.
    let peer = UdpSocket::bind("127.0.0.1:0").await.expect("peer bind");
    let frame = common::build_v2_heartbeat(0);
    peer.send_to(&frame, harness.listen_addr)
        .await
        .expect("peer send_to listener");

    let added = next_peer_added(&mut harness.event_rx).await;
    assert_eq!(added.identity, parent_identity);

    shutdown_all(&cancel, std::iter::once(harness.task)).await;
}
