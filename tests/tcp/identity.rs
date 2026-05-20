//! `tcps:` children must inherit a clone of the parent listener's
//! `IdentityFlags` via `PeerAdded` (CLAUDE.md "Sub-endpoints inherit their
//! parent's IdentityFlags by clone at spawn time").

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::filters::{Filters, MsgIdRange, U8Range};
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::spec::TcpServerEndpoint;
use rmr::endpoint::stats::EndpointState;
use rmr::endpoint::tcp::server::TcpServerSpec;

use crate::common;
use crate::common::tcp::{connect_with_retry, pick_free_tcp_addr, spawn_tcps_with_spec};
use crate::common::{next_peer_added, shutdown_all, wait_for_state};

#[tokio::test]
async fn tcps_child_inherits_parent_identity() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    // Parent listener with a deliberately non-default identity so every
    // field that matters (filter ranges, sniffer flag, group label,
    // capacities) is visibly different from `IdentityFlags::default()`.
    let parent_identity = IdentityFlags {
        sniffer: true,
        group: Some(Arc::from("uplink")),
        filters: Filters {
            block_msgid_in: vec![MsgIdRange::single(33), MsgIdRange { lo: 100, hi: 150 }],
            allow_src_sys_out: vec![U8Range::single(1)],
            ..Filters::default()
        },
    };

    let endpoint = TcpServerEndpoint {
        bind_addr: pick_free_tcp_addr(),
        ..TcpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let mut spec = TcpServerSpec::from_endpoint(endpoint, parent_id, "tcps-id".to_string());
    spec.identity = parent_identity.clone();
    let mut harness = spawn_tcps_with_spec(&allocator, cancel.clone(), spec);
    wait_for_state(&harness.stats, EndpointState::Connected, "tcps bind").await;

    // Connect a synthetic peer and write one heartbeat so the listener
    // accepts the client and emits `PeerAdded`.
    let mut peer = connect_with_retry(harness.listen_addr, Duration::from_secs(3)).await;
    let frame = common::build_v2_heartbeat(0);
    peer.write_all(&frame).await.expect("peer write");

    let added = next_peer_added(&mut harness.event_rx).await;
    assert_eq!(added.identity, parent_identity);

    drop(peer);
    shutdown_all(&cancel, std::iter::once(harness.task)).await;
}

#[tokio::test]
async fn tcps_child_inherits_sniffer_flag_in_isolation() {
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

    let endpoint = TcpServerEndpoint {
        bind_addr: pick_free_tcp_addr(),
        ..TcpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let mut spec = TcpServerSpec::from_endpoint(endpoint, parent_id, "tcps-sniffer".to_string());
    spec.identity = parent_identity.clone();
    let mut harness = spawn_tcps_with_spec(&allocator, cancel.clone(), spec);
    wait_for_state(&harness.stats, EndpointState::Connected, "tcps bind").await;

    let mut peer = connect_with_retry(harness.listen_addr, Duration::from_secs(3)).await;
    let frame = common::build_v2_heartbeat(0);
    peer.write_all(&frame).await.expect("peer write");

    let added = next_peer_added(&mut harness.event_rx).await;
    assert!(
        added.identity.sniffer,
        "tcps child must inherit sniffer=true from parent"
    );

    drop(peer);
    shutdown_all(&cancel, std::iter::once(harness.task)).await;
}
