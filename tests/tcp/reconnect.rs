//! `tcpc:` endpoint task survives a mid-stream server disconnect and
//! reconnects within the backoff bound, draining any frames queued during the
//! outage rather than replaying them.

use std::sync::Arc;
use std::time::Duration;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::spec::TcpClientEndpoint;
use rmr::endpoint::stats::EndpointState;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::common;
use crate::common::tcp::spawn_tcpc_with;
use crate::common::{shutdown_all, wait_for_state};

#[tokio::test]
async fn tcpc_reconnects_after_server_disconnect() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let listen_addr = listener.local_addr().expect("local_addr");

    let mut harness = spawn_tcpc_with(
        &allocator,
        cancel.clone(),
        listen_addr,
        TcpClientEndpoint::default(),
        "c",
        |spec| {
            spec.reconnect_initial_ms = 50;
            spec.reconnect_max_ms = 500;
        },
    );

    let (mut server_stream, _peer) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("initial accept timeout")
        .expect("initial accept");
    let frame1 = common::build_v2_heartbeat(0);
    server_stream
        .write_all(&frame1)
        .await
        .expect("server write 1");

    let router_frame1 = timeout(Duration::from_secs(2), harness.frame_rx.recv())
        .await
        .expect("client frame 1 timeout")
        .expect("frame_rx closed");
    assert_eq!(&router_frame1.frame[..], &frame1[..]);

    drop(server_stream);

    let (mut server_stream2, _peer2) = timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("reconnect accept timeout")
        .expect("reconnect accept");

    let frame2 = common::build_v2_heartbeat(1);
    server_stream2
        .write_all(&frame2)
        .await
        .expect("server write 2");
    let router_frame2 = timeout(Duration::from_secs(2), harness.frame_rx.recv())
        .await
        .expect("client frame 2 timeout")
        .expect("frame_rx closed");
    assert_eq!(&router_frame2.frame[..], &frame2[..]);

    shutdown_all(&cancel, [harness.task]).await;
}

/// Stale frames queued during the outage must be drained-and-discarded on
/// reconnect, not replayed onto the new socket.
#[tokio::test]
async fn tcpc_drains_queue_on_reconnect() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let listen_addr = listener.local_addr().expect("local_addr");

    let harness = spawn_tcpc_with(
        &allocator,
        cancel.clone(),
        listen_addr,
        TcpClientEndpoint::default(),
        "c",
        |spec| {
            spec.reconnect_initial_ms = 80;
            spec.reconnect_max_ms = 500;
        },
    );

    let (server_stream, _peer) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("initial accept timeout")
        .expect("initial accept");
    wait_for_state(
        &harness.stats,
        EndpointState::Connected,
        "after initial accept",
    )
    .await;

    // Drop the listener too so the redial fails and Reconnecting is
    // observable; then wait, so frames pushed below can't be drained onto
    // the half-closed socket before the disconnect surfaces.
    drop(server_stream);
    drop(listener);
    wait_for_state(
        &harness.stats,
        EndpointState::Reconnecting,
        "after disconnect",
    )
    .await;

    let stale = common::build_v2_heartbeat(42);
    let pre_drop = harness
        .stats
        .dropped_tx
        .load(std::sync::atomic::Ordering::Relaxed);
    for _ in 0..3 {
        harness.tx_queue.push(bytes::Bytes::copy_from_slice(&stale));
    }
    assert_eq!(harness.tx_queue.len(), 3, "stale frames should be queued");

    let listener = TcpListener::bind(listen_addr).await.expect("rebind");
    let (mut server_stream2, _peer2) = timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("reconnect accept timeout")
        .expect("reconnect accept");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let post_drop = harness
        .stats
        .dropped_tx
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        post_drop >= pre_drop + 3,
        "dropped_tx did not increase by drained count: {pre_drop} → {post_drop}"
    );

    // Silence on the new socket is the assertion: no stale bytes replayed.
    let mut buf = [0u8; 64];
    let result = timeout(
        Duration::from_millis(200),
        tokio::io::AsyncReadExt::read(&mut server_stream2, &mut buf),
    )
    .await;
    match result {
        Err(_) | Ok(Ok(0)) => {}
        Ok(Ok(bytes_read)) => panic!(
            "server received {bytes_read} stale bytes after reconnect: {:?}",
            &buf[..bytes_read]
        ),
        Ok(Err(err)) => panic!("server read errored: {err}"),
    }

    shutdown_all(&cancel, [harness.task]).await;
}
