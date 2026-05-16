//! End-to-end TCP-server round-trip: two `tcps:` listeners on 127.0.0.1
//! exchange MAVLink frames through a test-driven router stub.
//!
//! Each listener runs as a real task with a real listening socket; the test
//! plays the role of the Phase 5 router by:
//!   - draining `frame_rx` on each side,
//!   - holding the `TxQueue` of each accepted child (announced via `event_rx`),
//!   - pushing inbound frames from A onto B's child queue and vice versa.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;

use crate::common;
use crate::common::tcp::{connect_with_retry, spawn_tcps};
use crate::common::{next_peer_added, shutdown_all};

const CONNECT_DEADLINE: Duration = Duration::from_secs(3);

async fn read_exact_with_timeout(stream: &mut TcpStream, n: usize, label: &str) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    timeout(Duration::from_secs(2), stream.read_exact(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("{label} read_exact timeout"))
        .unwrap_or_else(|e| panic!("{label} read_exact error: {e}"));
    buf
}

#[tokio::test]
async fn round_trip_between_two_tcps_listeners() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let mut a = spawn_tcps(&allocator, cancel.clone(), "a");
    let mut b = spawn_tcps(&allocator, cancel.clone(), "b");

    // Two synthetic peers — raw TcpStreams that connect to A and B respectively.
    let mut peer_a = connect_with_retry(a.listen_addr, CONNECT_DEADLINE).await;
    let mut peer_b = connect_with_retry(b.listen_addr, CONNECT_DEADLINE).await;

    // Prime: each peer sends one HEARTBEAT so both listeners accept and the
    // children emit PeerAdded with their TxQueue.
    let frame0 = common::build_v2_heartbeat(0);
    peer_a.write_all(&frame0).await.expect("peer_a write to A");
    peer_b.write_all(&frame0).await.expect("peer_b write to B");

    let (peer_a_addr, peer_a_queue_on_a) = next_peer_added(&mut a.event_rx).await;
    let (peer_b_addr, peer_b_queue_on_b) = next_peer_added(&mut b.event_rx).await;
    assert_eq!(peer_a_addr, peer_a.local_addr().unwrap());
    assert_eq!(peer_b_addr, peer_b.local_addr().unwrap());

    // Drain the initial frame from each listener's frame channel.
    let f_init_a = timeout(Duration::from_secs(2), a.frame_rx.recv())
        .await
        .expect("init A frame timeout")
        .expect("A frame_rx closed");
    assert_eq!(&f_init_a.frame[..], &frame0[..]);
    let f_init_b = timeout(Duration::from_secs(2), b.frame_rx.recv())
        .await
        .expect("init B frame timeout")
        .expect("B frame_rx closed");
    assert_eq!(&f_init_b.frame[..], &frame0[..]);

    // Route a new heartbeat from peer_a to peer_b via the stub-router.
    let frame_routed = common::build_v2_heartbeat(7);
    peer_a
        .write_all(&frame_routed)
        .await
        .expect("peer_a write routed");
    let f_a = timeout(Duration::from_secs(2), a.frame_rx.recv())
        .await
        .expect("frame from A timeout")
        .expect("A frame_rx closed");
    assert_eq!(f_a.header.seq, 7);

    let displaced = peer_b_queue_on_b.push(f_a.frame.clone());
    assert!(!displaced, "first push should not evict");
    let got = read_exact_with_timeout(&mut peer_b, frame_routed.len(), "peer_b").await;
    assert_eq!(got, frame_routed);

    // Symmetric direction: peer_b → B → routed → A → peer_a.
    let frame_routed2 = common::build_v2_heartbeat(11);
    peer_b
        .write_all(&frame_routed2)
        .await
        .expect("peer_b write routed");
    let f_b = timeout(Duration::from_secs(2), b.frame_rx.recv())
        .await
        .expect("frame from B timeout")
        .expect("B frame_rx closed");
    assert_eq!(f_b.header.seq, 11);
    let displaced = peer_a_queue_on_a.push(f_b.frame.clone());
    assert!(!displaced);
    let got = read_exact_with_timeout(&mut peer_a, frame_routed2.len(), "peer_a").await;
    assert_eq!(got, frame_routed2);

    shutdown_all(&cancel, [a.task, b.task]).await;
}

/// One `tcps:` listener accepts two concurrent clients. Each gets its own
/// child endpoint (own EndpointId, own TxQueue, own stats), and disconnecting
/// one doesn't disturb the other or the listener — CLAUDE.md "Removal on
/// disconnect without killing the router".
#[tokio::test]
async fn tcps_handles_multiple_clients_and_per_client_disconnect() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let mut a = spawn_tcps(&allocator, cancel.clone(), "a");

    let mut c1 = connect_with_retry(a.listen_addr, CONNECT_DEADLINE).await;
    let mut c2 = connect_with_retry(a.listen_addr, CONNECT_DEADLINE).await;

    let f_init = common::build_v2_heartbeat(0);
    c1.write_all(&f_init).await.expect("c1 write");
    c2.write_all(&f_init).await.expect("c2 write");

    // Two PeerAdded events, one per client. We can't assume ordering — collect both.
    let mut child_addrs = Vec::new();
    for _ in 0..2 {
        let (addr, _q) = next_peer_added(&mut a.event_rx).await;
        child_addrs.push(addr);
    }
    let c1_local = c1.local_addr().expect("c1 local_addr");
    let c2_local = c2.local_addr().expect("c2 local_addr");
    assert!(child_addrs.contains(&c1_local));
    assert!(child_addrs.contains(&c2_local));

    // Drain both initial frames.
    for _ in 0..2 {
        timeout(Duration::from_secs(2), a.frame_rx.recv())
            .await
            .expect("init frame timeout")
            .expect("A frame_rx closed");
    }

    // Drop c1 — its session ends, PeerRemoved fires. The listener stays up.
    drop(c1);
    // Look for a PeerRemoved event for c1_local.
    let mut saw_c1_removed = false;
    for _ in 0..2 {
        let ev = timeout(Duration::from_secs(2), a.event_rx.recv())
            .await
            .expect("PeerRemoved timeout")
            .expect("event_rx closed");
        if let rmr::endpoint::events::EndpointEvent::PeerRemoved { peer_addr, .. } = ev
            && peer_addr == c1_local
        {
            saw_c1_removed = true;
            break;
        }
    }
    assert!(saw_c1_removed, "PeerRemoved for dropped c1 not observed");

    // c2 is still alive — send another frame; it should arrive.
    let f_post = common::build_v2_heartbeat(5);
    c2.write_all(&f_post)
        .await
        .expect("c2 post-disconnect write");
    let f = timeout(Duration::from_secs(2), a.frame_rx.recv())
        .await
        .expect("post-disconnect frame timeout")
        .expect("A frame_rx closed");
    assert_eq!(f.header.seq, 5);

    shutdown_all(&cancel, [a.task]).await;
}
