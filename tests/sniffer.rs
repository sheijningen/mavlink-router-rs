//! Sniffer end-to-end through `rmr::run`. Router unit tests cover the
//! per-destination decision's sniffer override against synthetic channels;
//! this binary is the transport-level proof that `?sniffer=true` on a real
//! `udpc:` endpoint actually admits a frame at the wire that a non-sniffer
//! destination would reject by target-match. A sniffer bypasses
//! loop-prevention, target-match, and out-filters; it sees every accepted
//! frame.

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use common::mavlink::{Ping, TestFrame};
use rmr::config::{Config, LogFormat, LogLevel};
use rmr::parsers::cli::parse_specs;

fn config_with_endpoints(endpoints: Vec<String>) -> Config {
    let cfg = Config {
        log_level: LogLevel::Warn,
        log_format: LogFormat::Text,
        stats: false,
        stats_interval_secs: 5,
        dedup_ms: 0,
        skip_config_log: true,
        endpoints: parse_specs(&endpoints).expect("test endpoint strings must parse"),
        merged: false,
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
async fn sniffer_admits_targeted_frame_that_non_sniffer_rejects() {
    // udps:source ingests frames; udpc:target is a regular destination;
    // udpc:tap is a sniffer destination. The injected PING has
    // target_system=99 — a sysid no endpoint has learned — so target-match
    // rejects at non-sniffer destinations. The sniffer override at the
    // per-destination decision admits regardless. Net result: only the
    // sniffer probe receives the frame at the wire.
    let source_addr = pick_free_udp_addr();
    let target_addr = pick_free_udp_addr();
    let tap_addr = pick_free_udp_addr();

    let cfg = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#source", source_addr.port()),
        format!("udpc:127.0.0.1:{}#target", target_addr.port()),
        format!("udpc:127.0.0.1:{}#tap?sniffer=true", tap_addr.port()),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(cfg, cancel).await })
    };

    let target_probe = UdpSocket::bind(target_addr)
        .await
        .expect("target_probe bind");
    let tap_probe = UdpSocket::bind(tap_addr).await.expect("tap_probe bind");
    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // PING with target_system=99: an unlearned sysid. Non-sniffer dests
    // reject on target-match; sniffer admits via the override.
    let frame = TestFrame::v2_message(&Ping {
        target_system: 99,
        target_component: 0,
        ..Ping::default()
    })
    .sysid(1)
    .compid(1)
    .seq(0)
    .build();
    injector
        .send_to(&frame, source_addr)
        .await
        .expect("inject ping");

    // tap (sniffer) must receive.
    let mut buf = vec![0u8; 256];
    let (n, _src) = timeout(Duration::from_secs(2), tap_probe.recv_from(&mut buf))
        .await
        .expect("sniffer did not receive targeted frame within 2s")
        .expect("tap recv_from");
    assert_eq!(
        &buf[..n],
        &frame[..],
        "frame bytes diverged on round-trip through the router"
    );

    // target (non-sniffer) must NOT receive — target_system=99 isn't in any
    // learned set.
    match timeout(Duration::from_millis(300), target_probe.recv_from(&mut buf)).await {
        Err(_) => {} // expected: nothing arrives
        Ok(Ok((n, _))) => {
            panic!("non-sniffer target unexpectedly received a frame ({n} bytes)")
        }
        Ok(Err(err)) => panic!("target recv errored: {err}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}
