//! `tcpc:` endpoint task survives a mid-stream server disconnect and
//! reconnects within the backoff bound, draining any frames queued during the
//! outage rather than replaying them.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{EndpointIdAllocator, spec::TcpClientEndpoint};

use crate::common;
use crate::common::shutdown_all;
use crate::common::tcp::spawn_tcpc_with;

#[tokio::test]
async fn tcpc_reconnects_after_server_disconnect() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let listen_addr = listener.local_addr().expect("local_addr");

    let mut h = spawn_tcpc_with(
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

    let f1 = timeout(Duration::from_secs(2), h.frame_rx.recv())
        .await
        .expect("client frame 1 timeout")
        .expect("frame_rx closed");
    assert_eq!(&f1.frame[..], &frame1[..]);

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
    let f2 = timeout(Duration::from_secs(2), h.frame_rx.recv())
        .await
        .expect("client frame 2 timeout")
        .expect("frame_rx closed");
    assert_eq!(&f2.frame[..], &frame2[..]);

    shutdown_all(&cancel, [h.task]).await;
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

    let h = spawn_tcpc_with(
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
    drop(server_stream);

    // Push frames while tcpc is in its backoff sleep so the next connect
    // observes a non-empty queue to drain.
    let stale = common::build_v2_heartbeat(42);
    let pre_drop = h
        .stats
        .dropped_tx
        .load(std::sync::atomic::Ordering::Relaxed);
    for _ in 0..3 {
        h.tx_queue.push(bytes::Bytes::copy_from_slice(&stale));
    }
    assert_eq!(h.tx_queue.len(), 3, "stale frames should be queued");

    let (mut server_stream2, _peer2) = timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("reconnect accept timeout")
        .expect("reconnect accept");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let post_drop = h
        .stats
        .dropped_tx
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        post_drop >= pre_drop + 3,
        "dropped_tx did not increase by drained count: {pre_drop} → {post_drop}"
    );

    // Silence on the new socket is the assertion: no stale bytes replayed.
    let mut buf = [0u8; 64];
    let res = timeout(
        Duration::from_millis(200),
        tokio::io::AsyncReadExt::read(&mut server_stream2, &mut buf),
    )
    .await;
    match res {
        Err(_) | Ok(Ok(0)) => {}
        Ok(Ok(n)) => panic!(
            "server received {n} stale bytes after reconnect: {:?}",
            &buf[..n]
        ),
        Ok(Err(e)) => panic!("server read errored: {e}"),
    }

    shutdown_all(&cancel, [h.task]).await;
}
