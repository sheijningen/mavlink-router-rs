//! Two `udps:` listeners on 127.0.0.1 exchange MAVLink frames through a
//! test-driven router stub.

use std::sync::Arc;
use std::time::Duration;

use rmr::endpoint::{EndpointIdAllocator, peer_endpoint_name};
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::common;
use crate::common::udp::spawn_udps;
use crate::common::{next_peer_added, shutdown_all};

#[tokio::test]
async fn round_trip_between_two_udps_listeners() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let mut harness_a = spawn_udps(&allocator, cancel.clone(), "a").await;
    let mut harness_b = spawn_udps(&allocator, cancel.clone(), "b").await;

    let peer_a = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer_a");
    let peer_b = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer_b");

    let frame_pa = common::build_v2_heartbeat(0);
    peer_a
        .send_to(&frame_pa, harness_a.listen_addr)
        .await
        .expect("peer_a send_to A");
    peer_b
        .send_to(&frame_pa, harness_b.listen_addr)
        .await
        .expect("peer_b send_to B");

    let added_a = next_peer_added(&mut harness_a.event_rx).await;
    let added_b = next_peer_added(&mut harness_b.event_rx).await;
    assert_eq!(
        added_a.name,
        peer_endpoint_name("a", peer_a.local_addr().expect("peer_a local_addr"))
    );
    assert_eq!(
        added_b.name,
        peer_endpoint_name("b", peer_b.local_addr().expect("peer_b local_addr"))
    );
    let peer_a_queue_on_a = added_a.tx_queue;
    let peer_b_queue_on_b = added_b.tx_queue;

    let frame_init_a = timeout(Duration::from_secs(2), harness_a.frame_rx.recv())
        .await
        .expect("init A frame timeout")
        .expect("A frame_rx closed");
    assert_eq!(&frame_init_a.frame[..], &frame_pa[..]);
    let frame_init_b = timeout(Duration::from_secs(2), harness_b.frame_rx.recv())
        .await
        .expect("init B frame timeout")
        .expect("B frame_rx closed");
    assert_eq!(&frame_init_b.frame[..], &frame_pa[..]);

    let frame_routed = common::build_v2_heartbeat(7);
    peer_a
        .send_to(&frame_routed, harness_a.listen_addr)
        .await
        .expect("peer_a send_to A (routed)");

    let routed_from_a = timeout(Duration::from_secs(2), harness_a.frame_rx.recv())
        .await
        .expect("frame from A timeout")
        .expect("A frame_rx closed");
    assert_eq!(routed_from_a.header.seq, 7);
    assert_eq!(&routed_from_a.frame[..], &frame_routed[..]);

    peer_b_queue_on_b.push(routed_from_a.frame.clone());

    let mut buf = [0u8; 128];
    let (bytes_read, src) = timeout(Duration::from_secs(2), peer_b.recv_from(&mut buf))
        .await
        .expect("peer_b recv timeout")
        .expect("peer_b recv_from");
    assert_eq!(src, harness_b.listen_addr);
    assert_eq!(&buf[..bytes_read], &frame_routed[..]);

    let frame_routed2 = common::build_v2_heartbeat(11);
    peer_b
        .send_to(&frame_routed2, harness_b.listen_addr)
        .await
        .expect("peer_b send_to B (routed)");
    let routed_from_b = timeout(Duration::from_secs(2), harness_b.frame_rx.recv())
        .await
        .expect("frame from B timeout")
        .expect("B frame_rx closed");
    assert_eq!(routed_from_b.header.seq, 11);
    peer_a_queue_on_a.push(routed_from_b.frame.clone());
    let (bytes_read, src) = timeout(Duration::from_secs(2), peer_a.recv_from(&mut buf))
        .await
        .expect("peer_a recv timeout")
        .expect("peer_a recv_from");
    assert_eq!(src, harness_a.listen_addr);
    assert_eq!(&buf[..bytes_read], &frame_routed2[..]);

    shutdown_all(&cancel, [harness_a.task, harness_b.task]).await;
}
