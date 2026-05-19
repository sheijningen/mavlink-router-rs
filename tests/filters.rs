//! Out-filter end-to-end through `rmr::run`. Router unit tests
//! cover the per-destination decision against synthetic channels; this binary
//! is the transport-level proof that a `?block_msgid_out=` on a real `udpc:`
//! actually suppresses the frame at the wire while a non-matching msgid still
//! reaches the destination. CLAUDE.md "Phase 5b checklist" — "Per-endpoint
//! Out-filters (egress, applied per destination)".
//!
//! More advanced multi-transport fan-out belongs in Phase 6's e2e tests; this
//! file just locks the egress filter into the integration-test surface so a
//! refactor that bypassed it would surface immediately.

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
async fn out_filter_blocks_destination_msgid_at_wire() {
    // udps:source ingests frames from the injector; udpc:dst is the
    // destination with PING (msgid 4) on its out-blocklist. HEARTBEAT
    // (msgid 0) must reach dst; PING must not.
    let source_addr = pick_free_udp_addr();
    let dst_addr = pick_free_udp_addr();

    let cfg = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#source", source_addr.port()),
        format!("udpc:127.0.0.1:{}#dst?block_msgid_out=4", dst_addr.port()),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(cfg, cancel).await })
    };

    // dst_addr is the configured peer for udpc:dst; rebind a probe there so
    // the udpc writer's send_to lands somewhere we can recv on.
    let dst_probe = UdpSocket::bind(dst_addr).await.expect("dst_probe bind");
    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");

    // UDP listeners have no ready-event; sleep is the only synchronisation.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Inject a HEARTBEAT first — broadcast (no target field), msgid 0, not
    // on the blocklist. dst must receive it.
    let heartbeat = common::build_v2_heartbeat(0);
    injector
        .send_to(&heartbeat, source_addr)
        .await
        .expect("inject heartbeat");

    let mut buf = vec![0u8; 256];
    let (n, _src) = timeout(Duration::from_secs(2), dst_probe.recv_from(&mut buf))
        .await
        .expect("heartbeat did not arrive at dst within 2s")
        .expect("dst recv_from");
    assert_eq!(&buf[..n], &heartbeat[..], "heartbeat bytes diverged");

    // Now inject a PING — broadcast (target_system=0), msgid 4, on the
    // blocklist. dst must NOT receive it.
    let ping_frame = TestFrame::v2_message(&Ping {
        target_system: 0,
        target_component: 0,
        ..Ping::default()
    })
    .seq(1)
    .build();
    injector
        .send_to(&ping_frame, source_addr)
        .await
        .expect("inject ping");

    match timeout(Duration::from_millis(300), dst_probe.recv_from(&mut buf)).await {
        Err(_) => {} // expected: nothing arrives
        Ok(Ok((n, _))) => panic!(
            "blocked PING reached dst (got {n} bytes; first={:?})",
            &buf[..n.min(8)],
        ),
        Ok(Err(e)) => panic!("dst recv errored: {e}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}
