//! Two `udps:` listeners on 127.0.0.1 exchange MAVLink frames through a
//! test-driven router stub.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;

use crate::common;
use crate::common::udp::spawn_udps;
use crate::common::{next_peer_added, shutdown_all};

#[tokio::test]
async fn round_trip_between_two_udps_listeners() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let mut a = spawn_udps(&allocator, cancel.clone(), "a").await;
    let mut b = spawn_udps(&allocator, cancel.clone(), "b").await;

    let peer_a = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer_a");
    let peer_b = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer_b");

    let frame_pa = common::build_v2_heartbeat(0);
    peer_a
        .send_to(&frame_pa, a.listen_addr)
        .await
        .expect("peer_a send_to A");
    peer_b
        .send_to(&frame_pa, b.listen_addr)
        .await
        .expect("peer_b send_to B");

    let added_a = next_peer_added(&mut a.event_rx).await;
    let added_b = next_peer_added(&mut b.event_rx).await;
    assert_eq!(
        added_a.peer_addr,
        peer_a.local_addr().expect("peer_a local_addr")
    );
    assert_eq!(
        added_b.peer_addr,
        peer_b.local_addr().expect("peer_b local_addr")
    );
    let peer_a_queue_on_a = added_a.tx_queue;
    let peer_b_queue_on_b = added_b.tx_queue;

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

    let displaced = peer_b_queue_on_b.push(f_a.frame.clone());
    assert!(!displaced, "first push should not evict");

    let mut buf = [0u8; 128];
    let (n, src) = timeout(Duration::from_secs(2), peer_b.recv_from(&mut buf))
        .await
        .expect("peer_b recv timeout")
        .expect("peer_b recv_from");
    assert_eq!(src, b.listen_addr);
    assert_eq!(&buf[..n], &frame_routed[..]);

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
