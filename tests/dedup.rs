//! Global dedup window end-to-end through `rmr::run_with_cancel`. Router unit
//! tests cover the algorithm against synthetic channels; this binary is the
//! transport-level proof that, with `dedup_ms > 0`, an identical frame
//! delivered by two distinct ingress endpoints is forwarded to a downstream
//! destination exactly once — the redundant-uplink scenario CLAUDE.md cites
//! as the motivating use case.

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::cli::parse_specs;
use rmr::config::{Config, LogFormat, LogLevel};

fn config_with_endpoints_and_dedup(endpoints: Vec<String>, dedup_ms: u64) -> Config {
    let cfg = Config {
        log_level: LogLevel::Warn,
        log_format: LogFormat::Text,
        stats: false,
        stats_interval: 5,
        dedup_ms,
        endpoints: parse_specs(&endpoints).expect("test endpoint strings must parse"),
    };
    cfg.validate()
        .expect("test config must pass cross-endpoint validation");
    cfg
}

fn pick_free_udp_addr() -> SocketAddr {
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("local_addr")
}

#[tokio::test]
async fn dedup_suppresses_second_copy_from_redundant_uplinks() {
    // Two redundant uplinks (udps:uplink_a, udps:uplink_b) and one
    // destination (udpc:gcs). With dedup_ms=500 the second arrival of an
    // identical frame is suppressed at the router before per-destination
    // dispatch.
    let uplink_a_addr = pick_free_udp_addr();
    let uplink_b_addr = pick_free_udp_addr();
    let gcs_addr = pick_free_udp_addr();

    let cfg = config_with_endpoints_and_dedup(
        vec![
            format!("udps:127.0.0.1:{}#uplink_a", uplink_a_addr.port()),
            format!("udps:127.0.0.1:{}#uplink_b", uplink_b_addr.port()),
            format!("udpc:127.0.0.1:{}#gcs", gcs_addr.port()),
        ],
        500,
    );

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run_with_cancel(cfg, cancel).await })
    };

    let gcs_probe = UdpSocket::bind(gcs_addr).await.expect("gcs_probe bind");
    let injector_a = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("injector_a bind");
    let injector_b = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("injector_b bind");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let frame = common::build_v2_heartbeat(42);
    injector_a
        .send_to(&frame, uplink_a_addr)
        .await
        .expect("inject via uplink_a");

    // First copy must arrive at gcs.
    let mut buf = vec![0u8; 256];
    let (n, _src) = timeout(Duration::from_secs(2), gcs_probe.recv_from(&mut buf))
        .await
        .expect("first copy did not arrive at gcs within 2s")
        .expect("gcs recv_from");
    assert_eq!(
        &buf[..n],
        &frame[..],
        "frame bytes diverged on first delivery"
    );

    // Inject the same bytes via the second uplink within the dedup window.
    injector_b
        .send_to(&frame, uplink_b_addr)
        .await
        .expect("inject via uplink_b");

    // No second copy must reach gcs — the router's dedup window drops the
    // duplicate before per-destination dispatch.
    match timeout(Duration::from_millis(400), gcs_probe.recv_from(&mut buf)).await {
        Err(_) => {} // expected: nothing arrives
        Ok(Ok((n, _))) => {
            panic!("dedup did not suppress second copy: gcs received another {n} bytes")
        }
        Ok(Err(e)) => panic!("gcs recv errored: {e}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}
