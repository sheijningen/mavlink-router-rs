//! Two `tcps:` listeners on 127.0.0.1 exchange MAVLink frames through a
//! test-driven router stub.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::events::{EndpointEvent, PeerRemovalReason};

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

    let mut a = spawn_tcps(&allocator, cancel.clone(), "a").await;
    let mut b = spawn_tcps(&allocator, cancel.clone(), "b").await;

    let mut peer_a = connect_with_retry(a.listen_addr, CONNECT_DEADLINE).await;
    let mut peer_b = connect_with_retry(b.listen_addr, CONNECT_DEADLINE).await;

    let frame0 = common::build_v2_heartbeat(0);
    peer_a.write_all(&frame0).await.expect("peer_a write to A");
    peer_b.write_all(&frame0).await.expect("peer_b write to B");

    let added_a = next_peer_added(&mut a.event_rx).await;
    let added_b = next_peer_added(&mut b.event_rx).await;
    assert_eq!(added_a.peer_addr, peer_a.local_addr().expect("peer_a local_addr"));
    assert_eq!(added_b.peer_addr, peer_b.local_addr().expect("peer_b local_addr"));
    let peer_a_queue_on_a = added_a.tx_queue;
    let peer_b_queue_on_b = added_b.tx_queue;

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

/// Asserts the "removal on disconnect without killing the router"
/// invariant: one child can drop without disturbing siblings or the parent.
#[tokio::test]
async fn tcps_handles_multiple_clients_and_per_client_disconnect() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let mut a = spawn_tcps(&allocator, cancel.clone(), "a").await;

    let mut c1 = connect_with_retry(a.listen_addr, CONNECT_DEADLINE).await;
    let mut c2 = connect_with_retry(a.listen_addr, CONNECT_DEADLINE).await;

    let f_init = common::build_v2_heartbeat(0);
    c1.write_all(&f_init).await.expect("c1 write");
    c2.write_all(&f_init).await.expect("c2 write");

    // PeerAdded ordering across two concurrent accepts is non-deterministic.
    let mut child_addrs = Vec::new();
    for _ in 0..2 {
        let added = next_peer_added(&mut a.event_rx).await;
        child_addrs.push(added.peer_addr);
    }
    let c1_local = c1.local_addr().expect("c1 local_addr");
    let c2_local = c2.local_addr().expect("c2 local_addr");
    assert!(child_addrs.contains(&c1_local));
    assert!(child_addrs.contains(&c2_local));

    for _ in 0..2 {
        timeout(Duration::from_secs(2), a.frame_rx.recv())
            .await
            .expect("init frame timeout")
            .expect("A frame_rx closed");
    }

    // Client-initiated close must surface as Disconnected, not ListenerShutdown.
    drop(c1);
    let mut saw_c1_removed = false;
    for _ in 0..2 {
        let ev = timeout(Duration::from_secs(2), a.event_rx.recv())
            .await
            .expect("PeerRemoved timeout")
            .expect("event_rx closed");
        if let EndpointEvent::PeerRemoved {
            peer_addr, reason, ..
        } = ev
            && peer_addr == c1_local
        {
            assert_eq!(
                reason,
                PeerRemovalReason::Disconnected,
                "client-initiated socket close must surface as Disconnected, got {reason:?}",
            );
            saw_c1_removed = true;
            break;
        }
    }
    assert!(saw_c1_removed, "PeerRemoved for dropped c1 not observed");

    let f_post = common::build_v2_heartbeat(5);
    c2.write_all(&f_post)
        .await
        .expect("c2 post-disconnect write");
    let f = timeout(Duration::from_secs(2), a.frame_rx.recv())
        .await
        .expect("post-disconnect frame timeout")
        .expect("A frame_rx closed");
    assert_eq!(f.header.seq, 5);

    // Cancel-triggered child teardown must surface as ListenerShutdown.
    cancel.cancel();
    let mut saw_c2_listener_shutdown = false;
    for _ in 0..2 {
        let ev = timeout(Duration::from_secs(3), a.event_rx.recv())
            .await
            .expect("PeerRemoved (ListenerShutdown) timeout")
            .expect("event_rx closed");
        if let EndpointEvent::PeerRemoved {
            peer_addr, reason, ..
        } = ev
            && peer_addr == c2_local
        {
            assert_eq!(
                reason,
                PeerRemovalReason::ListenerShutdown,
                "cancel-triggered child teardown must surface as ListenerShutdown, got {reason:?}",
            );
            saw_c2_listener_shutdown = true;
            break;
        }
    }
    assert!(
        saw_c2_listener_shutdown,
        "PeerRemoved{{ListenerShutdown}} for c2 not observed"
    );

    let _ = timeout(Duration::from_secs(3), a.task).await;
}
