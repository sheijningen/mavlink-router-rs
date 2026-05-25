//! Two `tcps:` listeners on 127.0.0.1 exchange MAVLink frames through a
//! test-driven router stub.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::events::{EndpointEvent, PeerRemovalReason};
use rmr::endpoint::{EndpointIdAllocator, peer_endpoint_name};

use crate::common;
use crate::common::tcp::{connect_with_retry, spawn_tcps};
use crate::common::{next_peer_added, shutdown_all};

const CONNECT_DEADLINE: Duration = Duration::from_secs(3);

async fn read_exact_with_timeout(stream: &mut TcpStream, count: usize, label: &str) -> Vec<u8> {
    let mut buf = vec![0u8; count];
    timeout(Duration::from_secs(2), stream.read_exact(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("{label} read_exact timeout"))
        .unwrap_or_else(|err| panic!("{label} read_exact error: {err}"));
    buf
}

#[tokio::test]
async fn round_trip_between_two_tcps_listeners() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let mut harness_a = spawn_tcps(&allocator, cancel.clone(), "a").await;
    let mut harness_b = spawn_tcps(&allocator, cancel.clone(), "b").await;

    let mut peer_a = connect_with_retry(harness_a.listen_addr, CONNECT_DEADLINE).await;
    let mut peer_b = connect_with_retry(harness_b.listen_addr, CONNECT_DEADLINE).await;

    let frame0 = common::build_v2_heartbeat(0);
    peer_a.write_all(&frame0).await.expect("peer_a write to A");
    peer_b.write_all(&frame0).await.expect("peer_b write to B");

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
    assert_eq!(&frame_init_a.frame[..], &frame0[..]);
    let frame_init_b = timeout(Duration::from_secs(2), harness_b.frame_rx.recv())
        .await
        .expect("init B frame timeout")
        .expect("B frame_rx closed");
    assert_eq!(&frame_init_b.frame[..], &frame0[..]);

    let frame_routed = common::build_v2_heartbeat(7);
    peer_a
        .write_all(&frame_routed)
        .await
        .expect("peer_a write routed");
    let routed_from_a = timeout(Duration::from_secs(2), harness_a.frame_rx.recv())
        .await
        .expect("frame from A timeout")
        .expect("A frame_rx closed");
    assert_eq!(routed_from_a.header.seq, 7);

    peer_b_queue_on_b.push(routed_from_a.frame.clone());
    let got = read_exact_with_timeout(&mut peer_b, frame_routed.len(), "peer_b").await;
    assert_eq!(got, frame_routed);

    let frame_routed2 = common::build_v2_heartbeat(11);
    peer_b
        .write_all(&frame_routed2)
        .await
        .expect("peer_b write routed");
    let routed_from_b = timeout(Duration::from_secs(2), harness_b.frame_rx.recv())
        .await
        .expect("frame from B timeout")
        .expect("B frame_rx closed");
    assert_eq!(routed_from_b.header.seq, 11);
    peer_a_queue_on_a.push(routed_from_b.frame.clone());
    let got = read_exact_with_timeout(&mut peer_a, frame_routed2.len(), "peer_a").await;
    assert_eq!(got, frame_routed2);

    shutdown_all(&cancel, [harness_a.task, harness_b.task]).await;
}

/// Asserts the "removal on disconnect without killing the router"
/// invariant: one child can drop without disturbing siblings or the parent.
#[tokio::test]
async fn tcps_handles_multiple_clients_and_per_client_disconnect() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let mut harness = spawn_tcps(&allocator, cancel.clone(), "a").await;

    let mut client1 = connect_with_retry(harness.listen_addr, CONNECT_DEADLINE).await;
    let mut client2 = connect_with_retry(harness.listen_addr, CONNECT_DEADLINE).await;

    let frame_init = common::build_v2_heartbeat(0);
    client1.write_all(&frame_init).await.expect("c1 write");
    client2.write_all(&frame_init).await.expect("c2 write");

    let client1_local = client1.local_addr().expect("c1 local_addr");
    let client2_local = client2.local_addr().expect("c2 local_addr");
    let client1_name = peer_endpoint_name("a", client1_local);
    let client2_name = peer_endpoint_name("a", client2_local);

    // PeerAdded ordering across two concurrent accepts is non-deterministic.
    // Capture each child's id by matching its name back to the originating
    // client so the later PeerRemoved filter can use child_id directly.
    let mut client1_child_id = None;
    let mut client2_child_id = None;
    for _ in 0..2 {
        let added = next_peer_added(&mut harness.event_rx).await;
        if added.name == client1_name {
            client1_child_id = Some(added.child_id);
        } else if added.name == client2_name {
            client2_child_id = Some(added.child_id);
        } else {
            panic!("unexpected PeerAdded name: {}", added.name);
        }
    }
    let client1_child_id = client1_child_id.expect("PeerAdded for c1 not observed");
    let client2_child_id = client2_child_id.expect("PeerAdded for c2 not observed");

    for _ in 0..2 {
        timeout(Duration::from_secs(2), harness.frame_rx.recv())
            .await
            .expect("init frame timeout")
            .expect("A frame_rx closed");
    }

    // Client-initiated close must surface as Disconnected, not ListenerShutdown.
    drop(client1);
    let mut saw_c1_removed = false;
    for _ in 0..2 {
        let event = timeout(Duration::from_secs(2), harness.event_rx.recv())
            .await
            .expect("PeerRemoved timeout")
            .expect("event_rx closed");
        if let EndpointEvent::PeerRemoved {
            child_id, reason, ..
        } = event
            && child_id == client1_child_id
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

    let frame_post = common::build_v2_heartbeat(5);
    client2
        .write_all(&frame_post)
        .await
        .expect("c2 post-disconnect write");
    let routed = timeout(Duration::from_secs(2), harness.frame_rx.recv())
        .await
        .expect("post-disconnect frame timeout")
        .expect("A frame_rx closed");
    assert_eq!(routed.header.seq, 5);

    // Cancel-triggered child teardown must surface as ListenerShutdown.
    cancel.cancel();
    let mut saw_c2_listener_shutdown = false;
    for _ in 0..2 {
        let event = timeout(Duration::from_secs(3), harness.event_rx.recv())
            .await
            .expect("PeerRemoved (ListenerShutdown) timeout")
            .expect("event_rx closed");
        if let EndpointEvent::PeerRemoved {
            child_id, reason, ..
        } = event
            && child_id == client2_child_id
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

    let _ = timeout(Duration::from_secs(3), harness.task).await;
}
