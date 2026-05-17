//! End-to-end integration test for the Phase 5a spawner.
//!
//! Drives `rmr::run_with_cancel` with two UDP endpoints — a `udps:` listener
//! and a `udpc:` client — and verifies that a frame injected at one is
//! forwarded by the router to the other. Exercises the full lifecycle:
//! spawner constructs channels, spawns router + stats + endpoint tasks,
//! EndpointAdded is sent before the endpoint task, the router learns the
//! source on the first inbound frame, broadcast routing reaches every
//! other endpoint, and cancel cleanly shuts everything down.

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::cli::{Cli, LogFormat, LogLevel};

fn cli_with_endpoints(endpoints: Vec<String>) -> Cli {
    Cli {
        config: None,
        log_level: LogLevel::Warn,
        log_format: LogFormat::Text,
        stats: false,
        stats_interval: 5,
        dedup_ms: 0,
        shutdown_grace: 5,
        endpoints,
    }
}

#[tokio::test]
async fn spawner_routes_udp_frame_end_to_end() {
    // Choose two free ports up front so the spec strings can name them.
    let probe_a = UdpSocket::bind("127.0.0.1:0").await.expect("probe a");
    let probe_b = UdpSocket::bind("127.0.0.1:0").await.expect("probe b");
    let addr_a: SocketAddr = probe_a.local_addr().expect("local_addr a");
    let addr_b: SocketAddr = probe_b.local_addr().expect("local_addr b");
    drop(probe_a);
    drop(probe_b);

    // rmr config:
    //   udps:127.0.0.1:<port_a> — `bus` (listens, learns peer)
    //   udpc:127.0.0.1:<port_b> — `tap` (sends to port_b, learns reply IP)
    let cli = cli_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#bus", addr_a.port()),
        format!("udpc:127.0.0.1:{}#tap", addr_b.port()),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run_with_cancel(cli, cancel).await })
    };

    // peer_to_bus plays the GCS the udpc:tap endpoint sends to — bind
    // addr_b so udpc's outbound `send_to` reaches us. injector is an
    // ephemeral client socket used to inject a frame INTO udps:bus; it
    // must NOT bind addr_a (that's where rmr's udps will listen).
    let peer_to_bus = UdpSocket::bind(addr_b).await.expect("peer_to_bus bind");
    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");

    // Wait for the spawner's endpoints to bind. UDP servers have no
    // ready-event we can synchronise on; poll with a short sleep.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send a heartbeat to udps from a known source so udps learns the peer
    // address and admits it as a routing endpoint.
    let frame = common::build_v2_heartbeat(7);
    injector
        .send_to(&frame, addr_a)
        .await
        .expect("inject into udps");

    // The frame is broadcast (HEARTBEAT carries no target field), so the
    // router routes it to every other endpoint. udpc is the only other
    // routing endpoint; it should write the bytes out to its configured
    // target — which our external peer_to_bus is bound to.
    let mut buf = vec![0u8; 256];
    let (n, _src) = timeout(Duration::from_secs(2), peer_to_bus.recv_from(&mut buf))
        .await
        .expect("frame did not arrive at udpc peer within 2s")
        .expect("recv_from");
    assert_eq!(
        &buf[..n],
        &frame[..],
        "frame bytes diverged on round-trip through the router"
    );

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}
