//! CLAUDE.md "Initial bind/dial failure path" for `udps:`: a listener whose
//! initial bind fails (port held by a previous process) enters the shared
//! capped-exp backoff loop, stays alive across attempts, and attaches as soon
//! as the port frees.
//!
//! Linux/macOS only: on these platforms two unprivileged UDP binds to the
//! exact same address fail with EADDRINUSE even with `SO_REUSEADDR` (that flag
//! lets a fresh process rebind after TIME_WAIT, but TIME_WAIT does not apply
//! to UDP — the second bind just collides). Windows behaviour with
//! `SO_REUSEADDR` differs and is out of scope here.

#![cfg(unix)]

use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::events::EndpointEvent;
use rmr::endpoint::{EndpointIdAllocator, udp::server::UdpServerConfig};

use crate::common;
use crate::common::shutdown_all;
use crate::common::udp::spawn_udps_at_with_config;

#[tokio::test]
async fn udps_attaches_when_pre_held_port_is_freed() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    // Reserve a UDP port with a plain std socket. On Linux/macOS, RMR's udps
    // bind to the same port will fail with EADDRINUSE while this socket is
    // alive (SO_REUSEADDR alone does not allow overlapping UDP binds).
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind probe");
    let port = probe.local_addr().expect("probe local_addr").port();
    let listen_addr: std::net::SocketAddr =
        format!("127.0.0.1:{port}").parse().expect("parse addr");

    // Short backoff so we don't have to wait long for udps to attach.
    let cfg = UdpServerConfig {
        reconnect_initial_ms: 50,
        reconnect_max_ms: 250,
        ..UdpServerConfig::default()
    };
    let mut h = spawn_udps_at_with_config(&allocator, cancel.clone(), listen_addr, cfg, "udps");

    // Let udps attempt at least a couple of binds. We can't directly observe
    // the retries (no telemetry hook), but if udps propagated the bind error
    // the task would have ended; the assertion below catches that — and the
    // post-free path would never succeed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !h.task.is_finished(),
        "udps task ended early — bind failure should have been retried, not propagated"
    );

    // Free the port. udps's next backoff iteration (≤ ~250 ms) should bind.
    drop(probe);

    // Drive a frame from a synthetic peer until the listener actually picks
    // up the port. Datagrams sent before the bind completes are silently
    // dropped on a connectionless socket, so retry until we observe the
    // PeerAdded event the listener emits on first inbound packet.
    let peer = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer");
    let peer_addr = peer.local_addr().expect("peer local_addr");
    let frame = common::build_v2_heartbeat(0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let added_addr = loop {
        peer.send_to(&frame, listen_addr)
            .await
            .expect("peer send_to listener");
        match timeout(Duration::from_millis(100), h.event_rx.recv()).await {
            Ok(Some(EndpointEvent::PeerAdded { peer_addr, .. })) => break peer_addr,
            Ok(Some(other)) => panic!("expected PeerAdded, got {other:?}"),
            Ok(None) => panic!("event_rx closed before listener attached"),
            Err(_) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "udps did not attach after bind retry"
                );
            }
        }
    };
    assert_eq!(added_addr, peer_addr);

    let f = timeout(Duration::from_secs(2), h.frame_rx.recv())
        .await
        .expect("frame timeout")
        .expect("frame_rx closed");
    assert_eq!(&f.frame[..], &frame[..]);

    shutdown_all(&cancel, [h.task]).await;
}
