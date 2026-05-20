//! End-to-end routing matrix on three mixed transports. Spawns one
//! `rmr::run` instance with `udps:` + `udpc:` + `tcps:` and three
//! cooperating fake peers (one per routing endpoint). Each test injects a
//! frame at one peer and asserts which of the other peers receive it,
//! covering the three routing invariants:
//!
//! - **Loop prevention** — a frame from peer A reaches B and C but not
//!   back to A.
//! - **Broadcast fan-out** — a broadcast frame reaches everyone but the
//!   source.
//! - **Targeted routing** — a frame whose `target_system` was learned only
//!   on one endpoint reaches that endpoint and not the others.

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use common::mavlink::{Heartbeat, Ping, TestFrame};
use rmr::config::{Config, LogFormat, LogLevel};
use rmr::parsers::cli::parse_specs;

fn config_with_endpoints(endpoints: Vec<String>) -> Config {
    let config = Config {
        log_level: LogLevel::Warn,
        log_format: LogFormat::Text,
        stats: false,
        stats_interval_secs: 5,
        dedup_ms: 0,
        skip_config_log: true,
        endpoints: parse_specs(&endpoints).expect("test endpoint strings must parse"),
    };
    config
        .validate()
        .expect("test config must pass cross-endpoint validation");
    config
}

fn pick_free_udp_addr() -> SocketAddr {
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("local_addr")
}

fn pick_free_tcp_addr() -> SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("local_addr")
}

/// Drain a TCP stream into a `Vec<u8>` until we've collected at least
/// `min_bytes` or hit the timeout. Returns whatever we got — the caller
/// asserts on shape.
async fn read_tcp_at_least(
    stream: &mut TcpStream,
    min_bytes: usize,
    duration: Duration,
) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 512];
    let deadline = tokio::time::Instant::now() + duration;
    while buf.len() < min_bytes && tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(bytes_read)) => buf.extend_from_slice(&tmp[..bytes_read]),
            Ok(Err(_)) | Err(_) => break,
        }
    }
    buf
}

#[tokio::test]
async fn fanout_routes_broadcast_to_other_transports_but_not_source() {
    // Three routing endpoints mixed across transports:
    //   udps:listener   — accepts a fake peer (call it A)
    //   udpc:downlink   — sends to a fake peer (call it B) listening on B's addr
    //   tcps:gw         — accepts a fake TCP client (call it C)
    //
    // Inject a broadcast HEARTBEAT(sysid=7) from A. Assert: B receives it
    // via udpc, C receives it via the tcps connection, A does NOT receive
    // it back (loop prevention).

    let udps_addr = pick_free_udp_addr();
    let udpc_target = pick_free_udp_addr();
    let tcps_addr = pick_free_tcp_addr();

    let config = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#listener", udps_addr.port()),
        format!("udpc:127.0.0.1:{}#downlink", udpc_target.port()),
        format!("tcps:127.0.0.1:{}#gw", tcps_addr.port()),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(config, cancel).await })
    };

    // Peer B listens on udpc's target address so the router's outbound
    // send_to actually lands somewhere readable.
    let peer_b = UdpSocket::bind(udpc_target).await.expect("peer_b bind");
    // Peer A binds an arbitrary loopback port; it'll send packets *to*
    // udps_addr and read replies back on its own bound port.
    let peer_a = UdpSocket::bind("127.0.0.1:0").await.expect("peer_a bind");

    // Wait for tcps to be ready and connect peer C. Loop on connect so we
    // don't race the rmr bind/dial loop.
    let peer_c = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            match TcpStream::connect(tcps_addr).await {
                Ok(stream) => break stream,
                Err(err) => {
                    if tokio::time::Instant::now() >= deadline {
                        panic!("peer_c could not connect to tcps within 2s: {err}");
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    };
    // Brief settle so peer-admission events have flowed through the router.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Inject a broadcast HEARTBEAT(sysid=7) from peer A into the udps
    // listener. The router learns (7,1) on the udps peer, then fans out to
    // udpc (which sends to peer B) and tcps:gw (which writes to peer C).
    let frame = TestFrame::v2_message(&Heartbeat {
        mav_type: 2,
        autopilot: 3,
        mavlink_version: 3,
        ..Heartbeat::default()
    })
    .sysid(7)
    .compid(1)
    .seq(0)
    .build();

    peer_a
        .send_to(&frame, udps_addr)
        .await
        .expect("peer_a inject");

    // Peer B should receive the broadcast via udpc.
    let mut buf_b = vec![0u8; 256];
    let (bytes_read_b, _src_b) = timeout(Duration::from_secs(2), peer_b.recv_from(&mut buf_b))
        .await
        .expect("peer_b never received frame")
        .expect("peer_b recv_from");
    assert_eq!(&buf_b[..bytes_read_b], &frame[..], "peer_b got wrong bytes");

    // Peer C should receive the broadcast via tcps. We read up to one frame
    // worth of bytes from the TCP stream.
    let mut peer_c = peer_c;
    let bytes_c = read_tcp_at_least(&mut peer_c, frame.len(), Duration::from_secs(2)).await;
    assert_eq!(
        &bytes_c[..frame.len()],
        &frame[..],
        "peer_c got wrong bytes (read {} of {})",
        bytes_c.len(),
        frame.len()
    );

    // Peer A must NOT receive its own frame back (loop prevention).
    let mut buf_a = vec![0u8; 256];
    match timeout(Duration::from_millis(250), peer_a.recv_from(&mut buf_a)).await {
        Err(_) => {} // timeout = good
        Ok(Ok((bytes_read, _))) => {
            panic!(
                "peer_a received its own frame back (loop prevention failed): {bytes_read} bytes"
            )
        }
        Ok(Err(err)) => panic!("peer_a recv_from errored: {err}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return")
        .expect("rmr::run task panicked");
    result.expect("rmr::run errored");
}

#[tokio::test]
async fn fanout_routes_targeted_frame_only_to_endpoint_that_learned_target() {
    // Three routing endpoints; pre-load each non-source's learn-set with a
    // distinct (sysid, compid) by having that peer source one frame. Then
    // inject a targeted frame at the source endpoint whose `target_system`
    // matches exactly one peer's learned id; assert only that peer's
    // endpoint receives it.

    let udps_addr = pick_free_udp_addr();
    let udpc_target = pick_free_udp_addr();
    let tcps_addr = pick_free_tcp_addr();

    let config = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#listener", udps_addr.port()),
        format!("udpc:127.0.0.1:{}#downlink", udpc_target.port()),
        format!("tcps:127.0.0.1:{}#gw", tcps_addr.port()),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(config, cancel).await })
    };

    let peer_b = UdpSocket::bind(udpc_target).await.expect("peer_b bind");
    let peer_a = UdpSocket::bind("127.0.0.1:0").await.expect("peer_a bind");

    let mut peer_c = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if let Ok(stream) = TcpStream::connect(tcps_addr).await {
                break stream;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("peer_c could not connect to tcps within 2s");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Prime peer B's learn-set: udpc's destination is peer_b, but for udpc
    // to learn (sysid_b, 1) the source has to be peer_b. udpc latches on
    // reply: peer_b sends a HEARTBEAT(sysid=99) *to* the rmr-side udpc
    // socket. We have to discover udpc's source port — easiest by reading
    // the broadcast we'll send momentarily and noting the sender.
    //
    // Simpler approach: send a learning frame *from* peer B to the udpc
    // listener side. Wait — udpc doesn't listen, only sends. To prime
    // udpc's learn-set we'd need a frame whose source endpoint is udpc;
    // that only happens when peer_b sends a packet back to udpc's source
    // address. Send a no-op frame from peer_a first to give udpc a packet
    // to send (so peer_b learns udpc's source port), then have peer_b
    // reply.
    let prime_a = TestFrame::v2_message(&Heartbeat::default())
        .sysid(50)
        .compid(1)
        .seq(0)
        .build();
    peer_a
        .send_to(&prime_a, udps_addr)
        .await
        .expect("prime_a send");
    let mut tmp = vec![0u8; 256];
    let (_bytes_read, udpc_src) = timeout(Duration::from_secs(2), peer_b.recv_from(&mut tmp))
        .await
        .expect("peer_b waiting for udpc fan-out")
        .expect("peer_b recv");

    // Now peer_b sends a HEARTBEAT(sysid=20) back to udpc's source addr;
    // udpc latches its destination, and the router learns (20,1) on udpc.
    let prime_b = TestFrame::v2_message(&Heartbeat::default())
        .sysid(20)
        .compid(1)
        .seq(0)
        .build();
    peer_b
        .send_to(&prime_b, udpc_src)
        .await
        .expect("prime_b send");

    // Drain whatever fans out from prime_b so the targeted assertion below
    // sees a clean queue. The tcps peer is the only destination here (the
    // udps peer that sourced prime_a is loop-blocked).
    let _ = read_tcp_at_least(&mut peer_c, prime_b.len(), Duration::from_millis(500)).await;

    // Pre-load peer C's learn-set by having peer_c source a frame with
    // (sysid=30, compid=1). tcps:gw will learn (30,1).
    let prime_c = TestFrame::v2_message(&Heartbeat::default())
        .sysid(30)
        .compid(1)
        .seq(0)
        .build();
    use tokio::io::AsyncWriteExt;
    peer_c
        .write_all(&prime_c)
        .await
        .expect("peer_c prime write");
    // Drain everywhere prime_c fanned out (udps -> peer_a, udpc -> peer_b).
    let mut drain_buf = vec![0u8; 512];
    let _ = timeout(Duration::from_millis(500), peer_a.recv_from(&mut drain_buf)).await;
    let _ = timeout(Duration::from_millis(500), peer_b.recv_from(&mut drain_buf)).await;

    // At this point: listener:peer_a learned (50,1); udpc learned (20,1);
    // tcps:gw learned (30,1). Inject a TARGETED PING with target_system=30
    // from peer_a. Router target-match should admit only the tcps:gw
    // destination; peer_b's queue stays empty for this frame.
    let targeted = TestFrame::v2_message(&Ping {
        target_system: 30,
        target_component: 0,
        ..Ping::default()
    })
    .sysid(50)
    .compid(1)
    .seq(1)
    .build();
    peer_a
        .send_to(&targeted, udps_addr)
        .await
        .expect("targeted send");

    // peer_c (behind tcps:gw, which has (30,1) in its learn-set) must get it.
    let bytes_c = read_tcp_at_least(&mut peer_c, targeted.len(), Duration::from_secs(2)).await;
    assert!(
        bytes_c
            .windows(targeted.len())
            .any(|window| window == &targeted[..]),
        "peer_c never received the targeted frame ({} bytes observed)",
        bytes_c.len()
    );

    // peer_b (behind udpc, which learned (20,1) — not 30) must NOT get it.
    let mut maybe_b = vec![0u8; 256];
    match timeout(Duration::from_millis(300), peer_b.recv_from(&mut maybe_b)).await {
        Err(_) => {}
        Ok(Ok((bytes_read, _))) => {
            assert_ne!(
                &maybe_b[..bytes_read],
                &targeted[..],
                "peer_b received a targeted frame that wasn't for it"
            );
        }
        Ok(Err(err)) => panic!("peer_b recv errored: {err}"),
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return")
        .expect("rmr::run task panicked");
    result.expect("rmr::run errored");
}
