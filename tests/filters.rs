//! End-to-end: filter axes on a destination suppress matching frames at the
//! wire while non-matching ones still arrive.

#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use common::mavlink::{Ping, TestFrame};
use common::udp::pick_free_udp_addr;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn out_filter_blocks_destination_msgid_at_wire() {
    // udps:source ingests frames from the injector; udpc:dst is the
    // destination with PING (msgid 4) on its out-blocklist. HEARTBEAT
    // (msgid 0) must reach dst; PING must not.
    let source_addr = common::udp::pick_free_udp_addr();
    let dst_addr = common::udp::pick_free_udp_addr();

    let cfg = common::config_with_endpoints(
        vec![
            format!("udps:127.0.0.1:{}#source", source_addr.port()),
            format!("udpc:127.0.0.1:{}#dst?block_msgid_out=4", dst_addr.port()),
        ],
        0,
    );

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
        Ok(Err(err)) => panic!("dst recv errored: {err}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}

#[tokio::test]
async fn block_src_endpoint_out_filters_by_source_name_at_wire() {
    // Plain HEARTBEAT (no targeting fields) confirms the axis fires on
    // source-name alone, independent of header content.
    let source_addr = pick_free_udp_addr();
    let radio_addr = pick_free_udp_addr();
    let gcs_addr = pick_free_udp_addr();

    let cfg = common::config_with_endpoints(
        vec![
            format!("udps:127.0.0.1:{}#source", source_addr.port()),
            format!(
                "udpc:127.0.0.1:{}#radio?block_src_endpoint_out=source",
                radio_addr.port()
            ),
            format!("udpc:127.0.0.1:{}#gcs", gcs_addr.port()),
        ],
        0,
    );

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(cfg, cancel).await })
    };

    let radio_probe = UdpSocket::bind(radio_addr).await.expect("radio_probe bind");
    let gcs_probe = UdpSocket::bind(gcs_addr).await.expect("gcs_probe bind");
    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let heartbeat = common::build_v2_heartbeat(0);
    injector
        .send_to(&heartbeat, source_addr)
        .await
        .expect("inject heartbeat");

    let mut buf = vec![0u8; 256];
    let (n, _addr) = timeout(Duration::from_secs(2), gcs_probe.recv_from(&mut buf))
        .await
        .expect("heartbeat did not reach gcs within 2s")
        .expect("gcs recv_from");
    assert_eq!(&buf[..n], &heartbeat[..], "heartbeat bytes diverged at gcs");

    match timeout(Duration::from_millis(300), radio_probe.recv_from(&mut buf)).await {
        Err(_) => {}
        Ok(Ok((n, _))) => panic!(
            "blocked frame reached radio (got {n} bytes; first={:?})",
            &buf[..n.min(8)],
        ),
        Ok(Err(err)) => panic!("radio recv errored: {err}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}

#[tokio::test]
async fn block_src_endpoint_out_matches_parent_listener_name_for_tcps_children() {
    // Locks the design rule that a `tcps:` accepted child inherits the
    // parent's `#name` (not the composite `parent/ip-port`) as endpoint_name.
    let source_addr = common::tcp::pick_free_tcp_addr();
    let radio_addr = pick_free_udp_addr();
    let gcs_addr = pick_free_udp_addr();

    let cfg = common::config_with_endpoints(
        vec![
            format!("tcps:127.0.0.1:{}#source", source_addr.port()),
            format!(
                "udpc:127.0.0.1:{}#radio?block_src_endpoint_out=source",
                radio_addr.port()
            ),
            format!("udpc:127.0.0.1:{}#gcs", gcs_addr.port()),
        ],
        0,
    );

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(cfg, cancel).await })
    };

    let radio_probe = UdpSocket::bind(radio_addr).await.expect("radio_probe bind");
    let gcs_probe = UdpSocket::bind(gcs_addr).await.expect("gcs_probe bind");

    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut client = TcpStream::connect(source_addr)
        .await
        .expect("connect to tcps source");

    let heartbeat = common::build_v2_heartbeat(0);
    client
        .write_all(&heartbeat)
        .await
        .expect("write heartbeat to source");

    let mut buf = vec![0u8; 256];
    let (n, _addr) = timeout(Duration::from_secs(2), gcs_probe.recv_from(&mut buf))
        .await
        .expect("heartbeat did not reach gcs within 2s")
        .expect("gcs recv_from");
    assert_eq!(&buf[..n], &heartbeat[..], "heartbeat bytes diverged at gcs");

    match timeout(Duration::from_millis(300), radio_probe.recv_from(&mut buf)).await {
        Err(_) => {}
        Ok(Ok((n, _))) => {
            panic!("blocked frame reached radio (tcps child inheritance failed; got {n} bytes)")
        }
        Ok(Err(err)) => panic!("radio recv errored: {err}"),
    }

    drop(client);
    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}

#[tokio::test]
async fn allow_src_endpoint_out_restricts_by_source_name_at_wire() {
    // `other_source` is declared but unused — it's only here so the
    // cross-reference check in Config::validate has a real name to match.
    let source_addr = pick_free_udp_addr();
    let other_source_addr = pick_free_udp_addr();
    let radio_addr = pick_free_udp_addr();
    let gcs_addr = pick_free_udp_addr();

    let cfg = common::config_with_endpoints(
        vec![
            format!("udps:127.0.0.1:{}#source", source_addr.port()),
            format!("udps:127.0.0.1:{}#other_source", other_source_addr.port()),
            format!(
                "udpc:127.0.0.1:{}#radio?allow_src_endpoint_out=other_source",
                radio_addr.port()
            ),
            format!("udpc:127.0.0.1:{}#gcs", gcs_addr.port()),
        ],
        0,
    );

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(cfg, cancel).await })
    };

    let radio_probe = UdpSocket::bind(radio_addr).await.expect("radio_probe bind");
    let gcs_probe = UdpSocket::bind(gcs_addr).await.expect("gcs_probe bind");
    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let heartbeat = common::build_v2_heartbeat(0);
    injector
        .send_to(&heartbeat, source_addr)
        .await
        .expect("inject heartbeat");

    let mut buf = vec![0u8; 256];
    let (n, _addr) = timeout(Duration::from_secs(2), gcs_probe.recv_from(&mut buf))
        .await
        .expect("heartbeat did not reach gcs within 2s")
        .expect("gcs recv_from");
    assert_eq!(&buf[..n], &heartbeat[..], "heartbeat bytes diverged at gcs");

    match timeout(Duration::from_millis(300), radio_probe.recv_from(&mut buf)).await {
        Err(_) => {}
        Ok(Ok((n, _))) => {
            panic!("frame reached radio despite restrictive allow list (got {n} bytes)")
        }
        Ok(Err(err)) => panic!("radio recv errored: {err}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return within shutdown budget")
        .expect("run task panicked");
    result.expect("rmr::run errored");
}
