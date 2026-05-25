//! `tcps:` per-child In-filter coverage. A child accepted from a listener
//! whose `block_msgid_in` covers the frame's msgid must increment that
//! child's `in_filter_drops` and never deliver the frame to the router's
//! `frame_rx`. The child's reader runs inside
//! `run_session::forward_inbound_frames`.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::filters::{Filters, MsgIdRange};
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::spec::TcpServerEndpoint;
use rmr::endpoint::stats::EndpointState;
use rmr::endpoint::tcp::server::TcpServerSpec;

use crate::common;
use crate::common::tcp::{connect_with_retry, pick_free_tcp_addr, spawn_tcps_with_spec};
use crate::common::{next_peer_added, shutdown_all, wait_for_state};

#[tokio::test]
async fn tcps_in_filter_blocks_msgid_at_session() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let parent_identity = IdentityFlags {
        filters: Filters {
            block_msgid_in: vec![MsgIdRange::single(0)],
            ..Filters::default()
        },
        ..IdentityFlags::default()
    };

    let endpoint = TcpServerEndpoint {
        bind_addr: pick_free_tcp_addr(),
        ..TcpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let mut spec = TcpServerSpec::from_endpoint(endpoint, parent_id, "tcps-in-filter".to_string());
    spec.identity = parent_identity;
    let mut harness = spawn_tcps_with_spec(&allocator, cancel.clone(), spec);
    wait_for_state(&harness.stats, EndpointState::Connected, "tcps bind").await;

    let mut peer = connect_with_retry(harness.listen_addr, Duration::from_secs(3)).await;
    let frame = common::build_v2_heartbeat(0);
    peer.write_all(&frame).await.expect("peer write");

    let added = next_peer_added(&mut harness.event_rx).await;

    // Poll the child's stats until in_filter_drops has incremented.
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
        "child in_filter_drops did not increment"
    );
    assert_eq!(
        added.stats.rx_frames.load(Ordering::Relaxed),
        1,
        "framer should still count rx_frames at the wire"
    );
    assert!(
        harness.frame_rx.try_recv().is_err(),
        "blocked frame leaked through to frame_rx"
    );

    drop(peer);
    shutdown_all(&cancel, std::iter::once(harness.task)).await;
}
