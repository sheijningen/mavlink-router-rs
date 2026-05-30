//! `udps:` learned peers must inherit a clone of the parent listener's
//! `IdentityFlags` via `PeerAdded`.

use std::sync::Arc;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::filters::{Filters, MsgIdRange, U8Range};
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::spec::UdpServerEndpoint;
use rmr::endpoint::stats::EndpointState;
use rmr::endpoint::udp::server::UdpServerSpec;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

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
    let mut spec = UdpServerSpec::from_endpoint(endpoint, parent_id, "udps-id".into());
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

#[tokio::test]
async fn udps_peer_inherits_sniffer_flag_in_isolation() {
    // The kitchen-sink test above asserts the whole `IdentityFlags` struct
    // matches, so a sniffer regression would surface as a multi-field diff.
    // This case isolates `sniffer = true` (everything else default) so a
    // future refactor that drops *only* the sniffer field during clone fails
    // here with a clear single-field message instead of swimming inside a
    // struct diff.
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let parent_identity = IdentityFlags {
        sniffer: true,
        ..IdentityFlags::default()
    };

    let endpoint = UdpServerEndpoint {
        bind_addr: pick_free_udp_addr(),
        ..UdpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let mut spec = UdpServerSpec::from_endpoint(endpoint, parent_id, "udps-sniffer".into());
    spec.identity = parent_identity.clone();
    let mut harness = spawn_udps_with_spec(&allocator, cancel.clone(), spec);
    wait_for_state(&harness.stats, EndpointState::Connected, "udps bind").await;

    let peer = UdpSocket::bind("127.0.0.1:0").await.expect("peer bind");
    let frame = common::build_v2_heartbeat(0);
    peer.send_to(&frame, harness.listen_addr)
        .await
        .expect("peer send_to listener");

    let added = next_peer_added(&mut harness.event_rx).await;
    assert!(
        added.identity.sniffer,
        "udps peer must inherit sniffer=true from parent"
    );

    shutdown_all(&cancel, std::iter::once(harness.task)).await;
}
