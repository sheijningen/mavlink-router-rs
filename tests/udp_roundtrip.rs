//! End-to-end UDP-server round-trip: two `udps:` listeners on 127.0.0.1
//! exchange MAVLink frames through a test-driven router stub.
//!
//! Each listener runs as a real task with a real socket; the test plays the
//! role of the Phase 5 router by:
//!   - draining `frame_rx` on each side,
//!   - holding the `TxQueue` of each known peer (announced via `event_rx`),
//!   - pushing inbound frames from A onto B's peer queue and vice versa.

mod common;

use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;

use common::udp::spawn_udps;
use common::{next_peer_added, shutdown_all};

#[tokio::test]
async fn round_trip_between_two_udps_listeners() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let mut a = spawn_udps(&allocator, cancel.clone(), "a");
    let mut b = spawn_udps(&allocator, cancel.clone(), "b");

    // Two synthetic peers — one talks to A, one talks to B.
    let peer_a = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer_a");
    let peer_b = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer_b");

    // Prime: each peer sends one HEARTBEAT so both listeners learn a peer
    // and emit PeerAdded.
    let frame_pa = common::build_v2_heartbeat(0);
    peer_a
        .send_to(&frame_pa, a.listen_addr)
        .await
        .expect("peer_a send_to A");
    peer_b
        .send_to(&frame_pa, b.listen_addr)
        .await
        .expect("peer_b send_to B");

    // Collect the announced TxQueues.
    let (peer_a_addr, peer_a_queue_on_a) = next_peer_added(&mut a.event_rx).await;
    let (peer_b_addr, peer_b_queue_on_b) = next_peer_added(&mut b.event_rx).await;
    assert_eq!(peer_a_addr, peer_a.local_addr().unwrap());
    assert_eq!(peer_b_addr, peer_b.local_addr().unwrap());

    // Drain the initial frame from each listener's frame channel.
    let f_init_a = timeout(Duration::from_secs(2), a.frame_rx.recv())
        .await
        .expect("init A frame timeout")
        .expect("A frame_rx closed");
    assert_eq!(&f_init_a.frame[..], &frame_pa[..]);
    let f_init_b = timeout(Duration::from_secs(2), b.frame_rx.recv())
        .await
        .expect("init B frame timeout")
        .expect("B frame_rx closed");
    assert_eq!(&f_init_b.frame[..], &frame_pa[..]);

    // Send a *new* heartbeat from peer_a to A. The test stub routes it onto
    // peer_b's TxQueue on B, which causes B to forward it out to peer_b.
    let frame_routed = common::build_v2_heartbeat(7);
    peer_a
        .send_to(&frame_routed, a.listen_addr)
        .await
        .expect("peer_a send_to A (routed)");

    let f_a = timeout(Duration::from_secs(2), a.frame_rx.recv())
        .await
        .expect("frame from A timeout")
        .expect("A frame_rx closed");
    assert_eq!(f_a.header.seq, 7);
    assert_eq!(&f_a.frame[..], &frame_routed[..]);

    // Router stub: push the frame onto B's TxQueue for peer_b.
    let displaced = peer_b_queue_on_b.push(f_a.frame.clone());
    assert!(!displaced, "first push should not evict");

    // peer_b should now receive the frame at its socket.
    let mut buf = [0u8; 128];
    let (n, src) = timeout(Duration::from_secs(2), peer_b.recv_from(&mut buf))
        .await
        .expect("peer_b recv timeout")
        .expect("peer_b recv_from");
    assert_eq!(src, b.listen_addr);
    assert_eq!(&buf[..n], &frame_routed[..]);

    // Symmetric direction: peer_b → B → routed → A → peer_a.
    let frame_routed2 = common::build_v2_heartbeat(11);
    peer_b
        .send_to(&frame_routed2, b.listen_addr)
        .await
        .expect("peer_b send_to B (routed)");
    let f_b = timeout(Duration::from_secs(2), b.frame_rx.recv())
        .await
        .expect("frame from B timeout")
        .expect("B frame_rx closed");
    assert_eq!(f_b.header.seq, 11);
    let displaced = peer_a_queue_on_a.push(f_b.frame.clone());
    assert!(!displaced);
    let (n, src) = timeout(Duration::from_secs(2), peer_a.recv_from(&mut buf))
        .await
        .expect("peer_a recv timeout")
        .expect("peer_a recv_from");
    assert_eq!(src, a.listen_addr);
    assert_eq!(&buf[..n], &frame_routed2[..]);

    shutdown_all(&cancel, [a.task, b.task]).await;
}
