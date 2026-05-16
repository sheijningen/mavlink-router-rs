//! CLAUDE.md Phase 3 integration test: "start a `tcps:` on a port already in
//! use, free the port, verify the listener picks it up without restart".
//!
//! Linux/macOS only: `SO_REUSEADDR` does not allow two listening sockets on
//! the same port on Unix, which is what makes this test deterministic. On
//! Windows `SO_REUSEADDR` lets the second bind succeed immediately —
//! observable port-hold behaviour there would need `SO_EXCLUSIVEADDRUSE` on
//! the probe socket, which we don't model here.

#![cfg(unix)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{EndpointIdAllocator, spec::TcpServerEndpoint, tcp::server::TcpServerSpec};

use crate::common;
use crate::common::next_peer_added;
use crate::common::shutdown_all;
use crate::common::tcp::{connect_with_retry, spawn_tcps_with_spec};

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

    // Short backoff so we don't have to wait long for tcps to attach. The
    // reconnect curve isn't exposed as a `*Endpoint` query knob (CLAUDE.md
    // "TCP/UDP server bind reuses the `tcpc:` backoff curve, no per-listener
    // override"), so we build the Spec from defaults then mutate.
    let parent_id = allocator.alloc();
    let mut spec = TcpServerSpec::from_endpoint(
        TcpServerEndpoint::default(),
        listen_addr,
        parent_id,
        "tcps".to_string(),
    );
    spec.reconnect_initial_ms = 50;
    spec.reconnect_max_ms = 250;
    let mut h = spawn_tcps_with_spec(&allocator, cancel.clone(), spec);

    // While the probe holds the port, the bound_addr oneshot stays pending.
    // Poll for early task termination without an unconditional sleep — if
    // tcps panicked on the first bind error, the task ends and the assertion
    // fires immediately; otherwise the loop exits once the deadline passes.
    let probe_deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < probe_deadline {
        assert!(
            !h.task.is_finished(),
            "tcps task ended early — bind failure should have been retried, not propagated"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Free the port. tcps's next backoff iteration (≤ ~250 ms) should bind.
    drop(probe);

    // The bound-addr oneshot fires once tcps's retry loop catches the freed
    // port — strictly tighter than waiting on a frame round-trip below.
    let bound_addr_rx = h.bound_addr_rx.take().expect("bound_addr_rx present");
    let bound_listen_addr = timeout(Duration::from_secs(3), bound_addr_rx)
        .await
        .expect("bound_addr_rx timeout")
        .expect("bound_addr_tx dropped");
    assert_eq!(bound_listen_addr, listen_addr);

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
