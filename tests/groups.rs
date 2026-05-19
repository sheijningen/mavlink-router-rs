//! Endpoint-group end-to-end through `rmr::run_with_cancel`. Router unit
//! tests cover [`GroupRegistry`](rmr::router::group) and the shared-learn-set
//! decision in [`rmr::router`] against synthetic channels. This binary is the
//! transport-level proof that, on real UDP endpoints:
//!   1. members of the same `?group=` share a learn-set, so a frame whose
//!      source identity was admitted via one member is loop-blocked at every
//!      other member; and
//!   2. members do **not** share filters — each member's per-endpoint
//!      out-filter applies independently.
//!
//! Maps to CLAUDE.md "Endpoint groups: declared implicitly by `?group=name`
//! / TOML `group=`; shared learned set only — filters and stats stay
//! per-endpoint." Per-endpoint stats independence is left to the router
//! unit test `group_members_do_not_share_stats` in `src/router/mod.rs` —
//! an integration-level assertion would need the stats JSON-Lines output
//! and the multi-transport e2e tests that arrive together in Phase 6; this
//! file pins the UDP-only contract so a refactor that lost the shared
//! learn-set (or leaked a filter across the group) surfaces immediately.

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use common::mavlink::{Heartbeat, Ping, TestFrame};
use rmr::config::{Config, LogFormat, LogLevel};
use rmr::parsers::cli::parse_specs;

fn config_with_endpoints(endpoints: Vec<String>) -> Config {
    let cfg = Config {
        log_level: LogLevel::Warn,
        log_format: LogFormat::Text,
        stats: false,
        stats_interval_secs: 5,
        dedup_ms: 0,
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
async fn group_members_share_learn_set_so_sibling_is_loop_blocked() {
    // udps:src and udpc:sibling are both in ?group=uplink; udpc:tap is not.
    // An injected HEARTBEAT(sysid=7) lands on src — the router touches the
    // group's learn-set with (7, 1). At per-destination dispatch:
    //   - sibling shares the group's table, sees (7, 1), and is loop-blocked.
    //   - tap has its own empty table, admits the broadcast.
    // Without the shared learn-set this same scenario would deliver the
    // frame to both sibling and tap.
    let src_addr = pick_free_udp_addr();
    let sibling_addr = pick_free_udp_addr();
    let tap_addr = pick_free_udp_addr();

    let cfg = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#src?group=uplink", src_addr.port()),
        format!(
            "udpc:127.0.0.1:{}#sibling?group=uplink",
            sibling_addr.port()
        ),
        format!("udpc:127.0.0.1:{}#tap", tap_addr.port()),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run_with_cancel(cfg, cancel).await })
    };

    let sibling_probe = UdpSocket::bind(sibling_addr)
        .await
        .expect("sibling_probe bind");
    let tap_probe = UdpSocket::bind(tap_addr).await.expect("tap_probe bind");
    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");

    // UDP listeners have no ready-event; sleep is the only synchronisation.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Broadcast HEARTBEAT (no target field) from sysid 7. The only
    // exclusion we want to demonstrate is loop-prevention via the shared
    // group learn-set — target-match must not short-circuit dispatch.
    let frame = TestFrame::v2_message(&Heartbeat {
        mav_type: 2,
        autopilot: 3,
        mavlink_version: 3,
        ..Heartbeat::default()
    })
    .sysid(7)
    .seq(0)
    .build();

    injector
        .send_to(&frame, src_addr)
        .await
        .expect("inject heartbeat");

    // tap (no group) must receive.
    let mut buf = vec![0u8; 256];
    let (n, _src) = timeout(Duration::from_secs(2), tap_probe.recv_from(&mut buf))
        .await
        .expect("tap did not receive frame within 2s")
        .expect("tap recv_from");
    assert_eq!(
        &buf[..n],
        &frame[..],
        "frame bytes diverged on tap delivery",
    );

    // sibling (in ?group=uplink) must NOT receive — the router's
    // per-destination decision sees (7, 1) in the shared learn-set and
    // loop-blocks before reaching sibling's TxQueue.
    match timeout(
        Duration::from_millis(400),
        sibling_probe.recv_from(&mut buf),
    )
    .await
    {
        Err(_) => {} // expected: nothing arrives
        Ok(Ok((n, _))) => panic!(
            "group sibling unexpectedly received a frame ({n} bytes) — shared learn-set did not loop-block"
        ),
        Ok(Err(e)) => panic!("sibling recv errored: {e}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}

#[tokio::test]
async fn group_members_do_not_share_out_filters() {
    // udps:src ingests; udpc:strict and udpc:permissive are both members of
    // ?group=g but only strict carries block_msgid_out=4. A broadcast PING
    // (msgid 4) must reach permissive and must NOT reach strict — confirming
    // that filters stay per-endpoint even when the learn-set is shared.
    let src_addr = pick_free_udp_addr();
    let strict_addr = pick_free_udp_addr();
    let permissive_addr = pick_free_udp_addr();

    let cfg = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#src", src_addr.port()),
        format!(
            "udpc:127.0.0.1:{}#strict?group=g&block_msgid_out=4",
            strict_addr.port(),
        ),
        format!(
            "udpc:127.0.0.1:{}#permissive?group=g",
            permissive_addr.port(),
        ),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run_with_cancel(cfg, cancel).await })
    };

    let strict_probe = UdpSocket::bind(strict_addr)
        .await
        .expect("strict_probe bind");
    let permissive_probe = UdpSocket::bind(permissive_addr)
        .await
        .expect("permissive_probe bind");
    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Broadcast PING (target_system=0) from a sysid that has not been
    // learned by any group member, so the loop-prevention path can't
    // confound the assertion — the only thing that can stop strict from
    // receiving is its own block_msgid_out.
    let frame = TestFrame::v2_message(&Ping {
        target_system: 0,
        target_component: 0,
        ..Ping::default()
    })
    .sysid(99)
    .seq(0)
    .build();

    injector
        .send_to(&frame, src_addr)
        .await
        .expect("inject ping");

    // permissive (no out-filter) must receive.
    let mut buf = vec![0u8; 256];
    let (n, _src) = timeout(Duration::from_secs(2), permissive_probe.recv_from(&mut buf))
        .await
        .expect("permissive did not receive frame within 2s")
        .expect("permissive recv_from");
    assert_eq!(
        &buf[..n],
        &frame[..],
        "frame bytes diverged on permissive delivery",
    );

    // strict (own block_msgid_out=4) must NOT receive — the filter is its
    // own, not shared from the group.
    match timeout(Duration::from_millis(400), strict_probe.recv_from(&mut buf)).await {
        Err(_) => {} // expected: nothing arrives
        Ok(Ok((n, _))) => {
            panic!("strict received a blocked PING ({n} bytes) — out-filter leaked across group")
        }
        Ok(Err(e)) => panic!("strict recv errored: {e}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}
