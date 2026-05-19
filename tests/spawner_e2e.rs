//! Drives `rmr::run_with_cancel` with a `udps:` + `udpc:` pair and verifies
//! that a frame injected at the listener reaches the client's configured peer
//! via the spawned router.

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::config::{Config, LogFormat, LogLevel};
use rmr::parsers::cli::parse_specs;

fn config_with_endpoints(endpoints: Vec<String>) -> Config {
    let cfg = Config {
        log_level: LogLevel::Warn,
        log_format: LogFormat::Text,
        stats: false,
        stats_interval_secs: 5,
        dedup_ms: 0,
        no_config_log: true,
        endpoints: parse_specs(&endpoints).expect("test endpoint strings must parse"),
    };
    cfg.validate()
        .expect("test config must pass cross-endpoint validation");
    cfg
}

#[tokio::test]
async fn spawner_routes_udp_frame_end_to_end() {
    // Probe-bind two ports so the spec strings can name them. Brief TOCTOU
    // window between drop and rmr's rebind — acceptable on loopback.
    let probe_a = UdpSocket::bind("127.0.0.1:0").await.expect("probe a");
    let probe_b = UdpSocket::bind("127.0.0.1:0").await.expect("probe b");
    let addr_a: SocketAddr = probe_a.local_addr().expect("local_addr a");
    let addr_b: SocketAddr = probe_b.local_addr().expect("local_addr b");
    drop(probe_a);
    drop(probe_b);

    let cfg = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#bus", addr_a.port()),
        format!("udpc:127.0.0.1:{}#tap", addr_b.port()),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run_with_cancel(cfg, cancel).await })
    };

    // peer_to_bus must bind addr_b so udpc's outbound `send_to` reaches it.
    // injector must NOT bind addr_a — that's where rmr's udps listens.
    let peer_to_bus = UdpSocket::bind(addr_b).await.expect("peer_to_bus bind");
    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");

    // UDP listeners have no ready-event; sleep is the only synchronisation.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let frame = common::build_v2_heartbeat(7);
    injector
        .send_to(&frame, addr_a)
        .await
        .expect("inject into udps");

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
