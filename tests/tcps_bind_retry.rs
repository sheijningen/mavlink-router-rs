//! CLAUDE.md Phase 3 integration test: "start a `tcps:` on a port already in
//! use, free the port, verify the listener picks it up without restart".
//!
//! Linux/macOS only: `SO_REUSEADDR` does not allow two listening sockets on
//! the same port on Unix, which is what makes this test deterministic. On
//! Windows `SO_REUSEADDR` lets the second bind succeed immediately —
//! observable port-hold behaviour there would need `SO_EXCLUSIVEADDRUSE` on
//! the probe socket, which we don't model here.

#![cfg(unix)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{EndpointIdAllocator, tcp_server::TcpServerConfig};

use common::next_peer_added;
use common::shutdown_all;
use common::tcp::{connect_with_retry, spawn_tcps_at_with_config};

#[tokio::test]
async fn tcps_attaches_when_pre_held_port_is_freed() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    // Reserve a port with a plain std listener. On Linux, RMR's tcps bind to
    // the same port will fail with EADDRINUSE while this listener is alive
    // (both sides have SO_REUSEADDR, but Linux still refuses overlapping
    // LISTEN sockets — that's the point).
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = probe.local_addr().expect("probe local_addr").port();
    let listen_addr: std::net::SocketAddr =
        format!("127.0.0.1:{port}").parse().expect("parse addr");

    // Short backoff so we don't have to wait long for tcps to attach.
    let cfg = TcpServerConfig {
        reconnect_initial_ms: 50,
        reconnect_max_ms: 250,
        ..TcpServerConfig::default()
    };
    let mut h = spawn_tcps_at_with_config(&allocator, cancel.clone(), listen_addr, cfg, "tcps");

    // Let tcps attempt at least a couple of binds. We can't directly observe
    // the retries (no telemetry hook), but if tcps panicked on the first
    // bind error, the task would have ended; the assertion below catches
    // that — and the post-free path would never succeed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !h.task.is_finished(),
        "tcps task ended early — bind failure should have been retried, not propagated"
    );

    // Free the port. tcps's next backoff iteration (≤ ~250 ms) should bind.
    drop(probe);

    // connect_with_retry tolerates the short window before tcps re-binds.
    let mut client = connect_with_retry(listen_addr, Duration::from_secs(3)).await;

    let frame = common::build_v2_heartbeat(0);
    client.write_all(&frame).await.expect("client write");
    let (_addr, _q) = next_peer_added(&mut h.event_rx).await;
    let f = timeout(Duration::from_secs(2), h.frame_rx.recv())
        .await
        .expect("frame timeout")
        .expect("frame_rx closed");
    assert_eq!(&f.frame[..], &frame[..]);

    shutdown_all(&cancel, [h.task]).await;
}
