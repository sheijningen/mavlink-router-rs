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
use rmr::endpoint::{EndpointIdAllocator, spec::UdpServerEndpoint, udp::server::UdpServerSpec};

use crate::common;
use crate::common::shutdown_all;
use crate::common::udp::spawn_udps_with_spec;

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

    // Short backoff so we don't have to wait long for udps to attach. The
    // reconnect curve isn't exposed as a `*Endpoint` query knob (CLAUDE.md
    // "udps: bind-retry shares the tcpc: curve, no per-listener override"),
    // so we build the Spec from defaults then mutate.
    let endpoint = UdpServerEndpoint {
        bind_addr: listen_addr,
        ..UdpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let mut spec = UdpServerSpec::from_endpoint(endpoint, parent_id, "udps".to_string());
    spec.reconnect_initial_ms = 50;
    spec.reconnect_max_ms = 250;
    let mut h = spawn_udps_with_spec(&allocator, cancel.clone(), spec);

    // Poll for early task termination without an unconditional sleep — if
    // udps panicked on the first bind error, the task ends and the assertion
    // fires immediately; otherwise the loop exits once the deadline passes
    // with the task still running.
    let probe_deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < probe_deadline {
        assert!(
            !h.task.is_finished(),
            "udps task ended early — bind failure should have been retried, not propagated"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Free the port. udps's next backoff iteration (≤ ~250 ms) should bind.
    drop(probe);

    // Wait for the bound-addr oneshot — strictly tighter than the
    // send-and-poll loop below.
    let bound_addr_rx = h.bound_addr_rx.take().expect("bound_addr_rx present");
    let bound_listen_addr = timeout(Duration::from_secs(3), bound_addr_rx)
        .await
        .expect("bound_addr_rx timeout")
        .expect("bound_addr_tx dropped");
    assert_eq!(bound_listen_addr, listen_addr);

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
