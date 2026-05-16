//! CLAUDE.md Phase 3 integration test: "kill a TCP client mid-stream, verify
//! router stays up and reconnects within the backoff bound". The "router"
//! here is the `tcpc:` endpoint task; we drive a real server using
//! `tokio::net::TcpListener` so we control accept/close timing.

mod common;

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{EndpointIdAllocator, tcp::client::TcpClientConfig};

use common::shutdown_all;
use common::tcp::spawn_tcpc;

#[tokio::test]
async fn tcpc_reconnects_after_server_disconnect() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let listen_addr = listener.local_addr().expect("local_addr");

    // Short backoff so the test runs fast.
    let cfg = TcpClientConfig {
        reconnect_initial_ms: 50,
        reconnect_max_ms: 500,
        ..TcpClientConfig::default()
    };
    let mut h = spawn_tcpc(&allocator, cancel.clone(), listen_addr, cfg, "c");

    // Accept the first connection and send a heartbeat.
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

    // Kill the server side mid-stream by dropping the accepted stream.
    drop(server_stream);

    // tcpc should detect the disconnect, hit backoff (~50ms initial), and
    // reconnect. Accept the reconnect within a generous bound (~3s).
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

/// Frames the router pushed during the outage are stale by the time the link
/// is back. CLAUDE.md: "On reconnect — before resuming normal pop — the
/// writer drains the queue completely, incrementing `dropped_tx` by the
/// drained count, and only then begins consuming new frames."
#[tokio::test]
async fn tcpc_drains_queue_on_reconnect() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let listen_addr = listener.local_addr().expect("local_addr");

    let cfg = TcpClientConfig {
        reconnect_initial_ms: 80,
        reconnect_max_ms: 500,
        ..TcpClientConfig::default()
    };
    let h = spawn_tcpc(&allocator, cancel.clone(), listen_addr, cfg, "c");

    // First session: accept then drop to force reconnect.
    let (server_stream, _peer) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("initial accept timeout")
        .expect("initial accept");
    drop(server_stream);

    // Push frames onto the TxQueue while tcpc is in its backoff sleep. These
    // frames will be drained-and-discarded on the next successful connect —
    // the test asserts they did NOT make it onto the new socket.
    let stale = common::build_v2_heartbeat(42);
    let pre_drop = h
        .stats
        .dropped_tx
        .load(std::sync::atomic::Ordering::Relaxed);
    for _ in 0..3 {
        h.tx_queue.push(bytes::Bytes::copy_from_slice(&stale));
    }
    assert_eq!(h.tx_queue.len(), 3, "stale frames should be queued");

    // Accept the reconnect. tcpc should drain the stale frames before writing
    // anything new.
    let (mut server_stream2, _peer2) = timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("reconnect accept timeout")
        .expect("reconnect accept");

    // Wait a beat to give tcpc time to drain.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // dropped_tx should reflect the drained frames.
    let post_drop = h
        .stats
        .dropped_tx
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        post_drop >= pre_drop + 3,
        "dropped_tx did not increase by drained count: {pre_drop} → {post_drop}"
    );

    // Confirm the server side received NO stale bytes. Use a short read
    // timeout — silence is the assertion.
    let mut buf = [0u8; 64];
    let res = timeout(
        Duration::from_millis(200),
        tokio::io::AsyncReadExt::read(&mut server_stream2, &mut buf),
    )
    .await;
    match res {
        Err(_) => { /* timeout — good, no stale bytes */ }
        Ok(Ok(0)) => { /* EOF — would happen if tcpc closed the link again */ }
        Ok(Ok(n)) => panic!(
            "server received {n} stale bytes after reconnect: {:?}",
            &buf[..n]
        ),
        Ok(Err(e)) => panic!("server read errored: {e}"),
    }

    shutdown_all(&cancel, [h.task]).await;
}
