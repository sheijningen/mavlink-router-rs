//! `tcps:` per-child seq tracker: a frame stream with a gap in `seq`
//! must bump the child's `rx_lost_est` to reflect the inferred losses.
//! The reader runs the seq tracker before In-filter so the counter
//! reflects link quality, not policy.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rmr::endpoint::EndpointIdAllocator;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::common;
use crate::common::tcp::{connect_with_retry, spawn_tcps};
use crate::common::{next_peer_added, shutdown_all};

#[tokio::test]
async fn tcps_child_seq_tracker_bumps_rx_lost_est_on_gap() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();
    let mut harness = spawn_tcps(&allocator, cancel.clone(), "tcps-seq").await;

    let mut peer = connect_with_retry(harness.listen_addr, Duration::from_secs(3)).await;
    // seq=0, seq=3 → gap=2 → +2 to rx_lost_est on the child's stats.
    let f0 = common::build_v2_heartbeat(0);
    let f3 = common::build_v2_heartbeat(3);
    peer.write_all(&f0).await.expect("peer write #0");
    peer.write_all(&f3).await.expect("peer write #3");

    let added = next_peer_added(&mut harness.event_rx).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if added.stats.rx_lost_est.load(Ordering::Relaxed) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        added.stats.rx_lost_est.load(Ordering::Relaxed),
        2,
        "child rx_lost_est should reflect the gap between seq 0 and seq 3"
    );

    drop(peer);
    shutdown_all(&cancel, std::iter::once(harness.task)).await;
}
