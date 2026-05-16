//! End-to-end UDP-server round-trip: two `udps:` listeners on 127.0.0.1
//! exchange MAVLink frames through a test-driven router stub.
//!
//! Each listener runs as a real task with a real socket; the test plays the
//! role of the Phase 5 router by:
//!   - draining `frame_rx` on each side,
//!   - holding the `TxQueue` of each known peer (announced via `event_rx`),
//!   - pushing inbound frames from A onto B's peer queue and vice versa.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{
    EndpointIdAllocator,
    events::{EndpointEvent, RouterFrame},
    udp_server::{self, UdpServerConfig, UdpServerSpec, UdpServerWiring},
};
use std::sync::Arc;

fn ephemeral_localhost_port() -> u16 {
    // Bind 127.0.0.1:0, capture the kernel-assigned port, drop. Localhost
    // test runners essentially never recycle the port before our listener
    // takes it back.
    let s = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind probe");
    let port = s.local_addr().expect("local_addr").port();
    drop(s);
    port
}

async fn drain_first_event(
    rx: &mut mpsc::Receiver<EndpointEvent>,
) -> Option<(SocketAddr, rmr::endpoint::tx_queue::TxQueue)> {
    let ev = timeout(Duration::from_secs(2), rx.recv()).await.ok()??;
    match ev {
        EndpointEvent::PeerAdded {
            peer_addr,
            tx_queue,
            ..
        } => Some((peer_addr, tx_queue)),
        _ => None,
    }
}

#[tokio::test]
async fn round_trip_between_two_udps_listeners() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let port_a = ephemeral_localhost_port();
    let port_b = ephemeral_localhost_port();
    let listen_a: SocketAddr = format!("127.0.0.1:{port_a}").parse().unwrap();
    let listen_b: SocketAddr = format!("127.0.0.1:{port_b}").parse().unwrap();
    let parent_id_a = allocator.alloc();
    let parent_id_b = allocator.alloc();

    let (frame_tx_a, mut frame_rx_a) = mpsc::channel::<RouterFrame>(32);
    let (event_tx_a, mut event_rx_a) = mpsc::channel::<EndpointEvent>(32);
    let (frame_tx_b, mut frame_rx_b) = mpsc::channel::<RouterFrame>(32);
    let (event_tx_b, mut event_rx_b) = mpsc::channel::<EndpointEvent>(32);

    let a_task = {
        let allocator = allocator.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            udp_server::run(
                UdpServerSpec {
                    listen_addr: listen_a,
                    parent_id: parent_id_a,
                    parent_name: "a".to_string(),
                    cfg: UdpServerConfig::default(),
                },
                UdpServerWiring {
                    allocator,
                    frame_tx: frame_tx_a,
                    event_tx: event_tx_a,
                    cancel,
                },
            )
            .await
        })
    };
    let b_task = {
        let allocator = allocator.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            udp_server::run(
                UdpServerSpec {
                    listen_addr: listen_b,
                    parent_id: parent_id_b,
                    parent_name: "b".to_string(),
                    cfg: UdpServerConfig::default(),
                },
                UdpServerWiring {
                    allocator,
                    frame_tx: frame_tx_b,
                    event_tx: event_tx_b,
                    cancel,
                },
            )
            .await
        })
    };

    // Two synthetic peers — one talks to A, one talks to B.
    let peer_a = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer_a");
    let peer_b = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer_b");

    // Prime: each peer sends one HEARTBEAT so both listeners learn a peer
    // and emit PeerAdded.
    let frame_pa = common::build_v2_heartbeat(0);
    peer_a
        .send_to(&frame_pa, listen_a)
        .await
        .expect("peer_a send_to A");
    peer_b
        .send_to(&frame_pa, listen_b)
        .await
        .expect("peer_b send_to B");

    // Collect the announced TxQueues.
    let (peer_a_addr, peer_a_queue_on_a) = drain_first_event(&mut event_rx_a)
        .await
        .expect("A PeerAdded");
    let (peer_b_addr, peer_b_queue_on_b) = drain_first_event(&mut event_rx_b)
        .await
        .expect("B PeerAdded");
    assert_eq!(peer_a_addr, peer_a.local_addr().unwrap());
    assert_eq!(peer_b_addr, peer_b.local_addr().unwrap());

    // Drain the initial frame from each listener's frame channel.
    let f_init_a = timeout(Duration::from_secs(2), frame_rx_a.recv())
        .await
        .expect("init A frame timeout")
        .expect("A frame_rx closed");
    assert_eq!(&f_init_a.frame[..], &frame_pa[..]);
    let f_init_b = timeout(Duration::from_secs(2), frame_rx_b.recv())
        .await
        .expect("init B frame timeout")
        .expect("B frame_rx closed");
    assert_eq!(&f_init_b.frame[..], &frame_pa[..]);

    // Send a *new* heartbeat from peer_a to A. The test stub routes it onto
    // peer_b's TxQueue on B, which causes B to forward it out to peer_b.
    let frame_routed = common::build_v2_heartbeat(7);
    peer_a
        .send_to(&frame_routed, listen_a)
        .await
        .expect("peer_a send_to A (routed)");

    let f_a = timeout(Duration::from_secs(2), frame_rx_a.recv())
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
    assert_eq!(src, listen_b);
    assert_eq!(&buf[..n], &frame_routed[..]);

    // Symmetric direction: peer_b → B → routed → A → peer_a.
    let frame_routed2 = common::build_v2_heartbeat(11);
    peer_b
        .send_to(&frame_routed2, listen_b)
        .await
        .expect("peer_b send_to B (routed)");
    let f_b = timeout(Duration::from_secs(2), frame_rx_b.recv())
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
    assert_eq!(src, listen_a);
    assert_eq!(&buf[..n], &frame_routed2[..]);

    cancel.cancel();
    let _ = timeout(Duration::from_secs(3), a_task).await;
    let _ = timeout(Duration::from_secs(3), b_task).await;
}
