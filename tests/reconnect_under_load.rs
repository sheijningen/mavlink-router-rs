//! End-to-end TCP reconnect behaviour under sustained UDP ingress.
//! Verifies the locked decision "TX queue on disconnect: drain and
//! discard, never replay": when a `tcpc:` peer drops, frames in the
//! writer's queue must be discarded before the link is reestablished, so
//! the GCS on the other side never sees stale telemetry ahead of fresh
//! frames after a flap.

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Bind a TCP listener on `addr` with `SO_REUSEADDR` so the test can drop
/// it and rebind the same port without waiting out the kernel's release
/// window. `tokio::net::TcpListener::bind` does not expose `SO_REUSEADDR`
/// directly, hence the manual `socket2` dance.
fn bind_reusable_listener(addr: SocketAddr) -> TcpListener {
    let s = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).expect("socket");
    s.set_reuse_address(true).expect("set_reuse_address");
    s.set_nonblocking(true).expect("nonblocking");
    s.bind(&addr.into()).expect("bind");
    s.listen(128).expect("listen");
    TcpListener::from_std(s.into()).expect("from_std")
}

use common::mavlink::{Heartbeat, TestFrame};
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

/// Read up to `cap` bytes from `stream` with a per-read timeout. Returns
/// what was collected when either `cap` is reached or `dur` elapses without
/// new bytes.
async fn read_until_quiet(stream: &mut TcpStream, cap: usize, dur: Duration) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 512];
    while buf.len() < cap {
        match timeout(dur, stream.read(&mut tmp)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
        }
    }
    buf
}

#[tokio::test]
async fn tcpc_drains_queue_on_disconnect_then_streams_fresh_frames_after_reconnect() {
    // Topology:
    //   udps:listener    — peer A injects frames here
    //   tcpc:downlink    — connects to our fake TCP server (acts as GCS)
    //
    // Phase 1: peer A sends a "pre" batch; verify peer C (our TCP server's
    // accepted client) receives it.
    // Phase 2: drop the TCP connection on the server side. While
    // disconnected, peer A keeps sending a "during" batch.
    // Phase 3: accept the next connection; peer A sends a "post" batch.
    //          Verify that the post-reconnect stream contains the "post"
    //          frames and NONE of the "during" frames (drain-and-discard,
    //          never replay). The "pre" frames must not reappear either.

    let udps_addr = pick_free_udp_addr();
    // Bind the fake server with SO_REUSEADDR so we can rebind the same
    // port mid-test after dropping it (and the connection it accepted).
    let server = bind_reusable_listener("127.0.0.1:0".parse().unwrap());
    let server_addr: SocketAddr = server.local_addr().expect("server addr");

    let cfg = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#listener", udps_addr.port()),
        format!("tcpc:127.0.0.1:{}#downlink", server_addr.port()),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(cfg, cancel).await })
    };

    let peer_a = UdpSocket::bind("127.0.0.1:0").await.expect("peer_a bind");

    // Accept the first connection from rmr's tcpc.
    let (mut peer_c1, _peer_c1_addr) = timeout(Duration::from_secs(2), server.accept())
        .await
        .expect("first accept timed out")
        .expect("first accept");
    // Brief settle so the router has admitted the peer.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 1: pre batch. Unique source identities so we can distinguish
    // frames later — sysid 11..=14.
    let pre: Vec<Vec<u8>> = (11u8..=14u8)
        .map(|sys| {
            TestFrame::v2_message(&Heartbeat::default())
                .sysid(sys)
                .compid(1)
                .seq(sys)
                .build()
        })
        .collect();
    for f in &pre {
        peer_a.send_to(f, udps_addr).await.expect("pre send");
    }

    let bytes_pre = read_until_quiet(
        &mut peer_c1,
        pre.iter().map(|f| f.len()).sum(),
        Duration::from_millis(500),
    )
    .await;
    for f in &pre {
        assert!(
            bytes_pre.windows(f.len()).any(|w| w == &f[..]),
            "peer_c1 missing a pre-batch frame (sysid={})",
            f[5] // v2 sysid byte position
        );
    }

    // Phase 2: drop both the accepted connection AND the listener. With
    // no listener bound to `server_addr`, rmr's next `connect()` fails
    // and the tcpc enters its backoff sleep — during that window we can
    // queue frames in tcpc's TxQueue that the next reconnect MUST drain.
    drop(peer_c1);
    drop(server);
    // Sleep past one backoff floor (250 ms default + jitter) so rmr has
    // observed disconnect → failed connect → entered backoff before we
    // start queuing the "during" batch.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // While disconnected with no listener, peer A keeps sending — sysids
    // 21..=24. These frames land in the tcpc's TxQueue; on the next
    // reconnect they must be discarded, NOT replayed.
    let during: Vec<Vec<u8>> = (21u8..=24u8)
        .map(|sys| {
            TestFrame::v2_message(&Heartbeat::default())
                .sysid(sys)
                .compid(1)
                .seq(sys)
                .build()
        })
        .collect();
    for f in &during {
        peer_a.send_to(f, udps_addr).await.expect("during send");
    }

    // Phase 3: rebind the listener on the same port and wait for rmr to
    // reconnect.
    let server = bind_reusable_listener(server_addr);
    let (mut peer_c2, _peer_c2_addr) = timeout(Duration::from_secs(3), server.accept())
        .await
        .expect("second accept timed out")
        .expect("second accept");
    // Settle so the router has admitted peer_c2 and the writer's
    // drain_and_discard has wiped the during batch.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Post batch — sysids 31..=34.
    let post: Vec<Vec<u8>> = (31u8..=34u8)
        .map(|sys| {
            TestFrame::v2_message(&Heartbeat::default())
                .sysid(sys)
                .compid(1)
                .seq(sys)
                .build()
        })
        .collect();
    for f in &post {
        peer_a.send_to(f, udps_addr).await.expect("post send");
    }

    let bytes_post = read_until_quiet(
        &mut peer_c2,
        post.iter().map(|f| f.len()).sum(),
        Duration::from_millis(800),
    )
    .await;

    // The post-reconnect stream must include every "post" frame.
    for f in &post {
        assert!(
            bytes_post.windows(f.len()).any(|w| w == &f[..]),
            "peer_c2 missing a post-batch frame (sysid={}); got {} bytes total: {:?}",
            f[5],
            bytes_post.len(),
            bytes_post.iter().take(8).collect::<Vec<_>>(),
        );
    }
    // And must NOT include any "during" frame — that's the no-replay
    // guarantee. We also confirm "pre" frames don't reappear.
    for f in during.iter().chain(pre.iter()) {
        assert!(
            !bytes_post.windows(f.len()).any(|w| w == &f[..]),
            "peer_c2 received a stale frame after reconnect (sysid={}); \
             drain-and-discard contract violated",
            f[5]
        );
    }

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return")
        .expect("rmr::run task panicked");
    result.expect("rmr::run errored");
}

#[tokio::test]
async fn tcpc_survives_repeated_flaps_under_sustained_ingress() {
    // Lighter version of the above that just hammers the flap cycle and
    // asserts rmr stays up: no panic, returns cleanly on cancel. Doesn't
    // assert per-frame routing — that's covered by the test above. This
    // one watches for accidental task / FD leaks via "after N flaps, rmr
    // still accepts the next connection and still shuts down within the
    // wall-clock budget."
    let udps_addr = pick_free_udp_addr();
    let server = TcpListener::bind("127.0.0.1:0").await.expect("server bind");
    let server_addr: SocketAddr = server.local_addr().expect("server addr");

    let cfg = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#listener", udps_addr.port()),
        format!("tcpc:127.0.0.1:{}#downlink", server_addr.port()),
    ]);

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(cfg, cancel).await })
    };

    let peer_a = UdpSocket::bind("127.0.0.1:0").await.expect("peer_a bind");

    for cycle in 0..5u8 {
        // Accept one connection, send a couple frames, drop.
        let (mut stream, _) = timeout(Duration::from_secs(3), server.accept())
            .await
            .unwrap_or_else(|_| panic!("accept timed out at cycle {cycle}"))
            .expect("accept");
        for seq in 0u8..3u8 {
            let f = TestFrame::v2_message(&Heartbeat::default())
                .sysid(cycle.wrapping_add(40))
                .compid(1)
                .seq(seq)
                .build();
            peer_a.send_to(&f, udps_addr).await.expect("send");
        }
        // Let some frames arrive at the peer before dropping.
        let _ = read_until_quiet(&mut stream, 64, Duration::from_millis(200)).await;
        drop(stream);
        // Give rmr time to observe the close and enter backoff.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Final accept proves rmr's tcpc is still reconnecting after 5 flaps.
    let (mut final_stream, _) = timeout(Duration::from_secs(3), server.accept())
        .await
        .expect("final accept timed out")
        .expect("final accept");
    let f = TestFrame::v2_message(&Heartbeat::default())
        .sysid(99)
        .compid(1)
        .seq(0)
        .build();
    peer_a.send_to(&f, udps_addr).await.expect("final send");
    let bytes = read_until_quiet(&mut final_stream, f.len(), Duration::from_millis(800)).await;
    assert!(
        bytes.windows(f.len()).any(|w| w == &f[..]),
        "final reconnected stream did not deliver fresh frame ({} bytes received)",
        bytes.len()
    );

    cancel.cancel();
    let result = timeout(Duration::from_secs(6), run_handle)
        .await
        .expect("rmr::run did not return after flap soak")
        .expect("rmr::run task panicked");
    result.expect("rmr::run errored");
}

/// Binary-driven flap-cycle test that asserts the CLAUDE.md "`dropped_tx`
/// accounting" bullet at the wire level. Spawns `rmr --stats` with the
/// `tcpc:` endpoint forced to a small (`tx_queue_frames=8`) writer queue
/// so a burst of stale frames during a disconnect window overflows on the
/// router-side `force_push` path on top of the writer-side
/// `drain_and_discard` path on reconnect. After two flap cycles, parses
/// stdout JSON-Lines and asserts the tcpc endpoint's `dropped_tx` counter
/// climbed past a wide-margin floor. Unix-only — same signal-portability
/// caveat as the binary shutdown_soak case.
#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn binary_tcpc_flap_cycles_drive_dropped_tx_counter() {
    use std::io::Read;
    use std::process::{Command as StdCommand, Stdio};
    use std::time::Instant;

    use assert_cmd::Command;

    // Frame builders local to this test (the sysid/seq tagging differs
    // from the helper at the top of the file).
    fn build_heartbeat(sys: u8, seq: u8) -> Vec<u8> {
        TestFrame::v2_message(&Heartbeat::default())
            .sysid(sys)
            .compid(1)
            .seq(seq)
            .build()
    }

    async fn inject_burst(sock: &UdpSocket, sys: u8, range: std::ops::Range<u8>) {
        for seq in range {
            let _ = sock.send(&build_heartbeat(sys, seq)).await;
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener bind");
    let listen_addr: SocketAddr = listener.local_addr().expect("listen_addr");
    let udps_addr = pick_free_udp_addr();

    let bin_path = Command::cargo_bin("rmr")
        .expect("cargo_bin")
        .get_program()
        .to_os_string();

    let mut child = StdCommand::new(bin_path)
        .args([
            "--stats",
            "--stats-interval-secs=1",
            "--skip-config-log",
            "--log-level=warn",
            &format!("udps:127.0.0.1:{}#bus", udps_addr.port()),
            // Small tx_queue_frames forces router-side force_push eviction
            // on the burst during the disconnect window, on top of the
            // writer-side drain-on-disconnect drops.
            &format!(
                "tcpc:127.0.0.1:{}#client?tx_queue_frames=8",
                listen_addr.port()
            ),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rmr");

    let (mut conn, _) = timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("first accept timeout")
        .expect("first accept");

    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");
    injector
        .connect(("127.0.0.1", udps_addr.port()))
        .await
        .expect("injector connect");

    // Baseline: confirm forwarding works at all.
    inject_burst(&injector, 10, 0..10).await;
    let _ = read_until_quiet(&mut conn, 200, Duration::from_millis(300)).await;

    // -- Flap cycle 1 --
    drop(conn);
    drop(listener);
    // Burst 30 stale frames into the udps while rmr can't reach the
    // (gone) tcps listener — these queue in tcpc's TxQueue (cap 8) and
    // then get drained-and-discarded on reconnect.
    inject_burst(&injector, 99, 0..30).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let listener2 = bind_reusable_listener(listen_addr);
    let (mut conn2, _) = timeout(Duration::from_secs(3), listener2.accept())
        .await
        .expect("cycle-1 reconnect accept timeout")
        .expect("cycle-1 reconnect accept");
    tokio::time::sleep(Duration::from_millis(100)).await;
    inject_burst(&injector, 10, 100..120).await;
    let cycle1 = read_until_quiet(&mut conn2, 1024, Duration::from_millis(500)).await;
    // No sysid=99 byte at index 5 of any v2 frame should survive — the
    // simplest spot-check that none of the stale frames replayed.
    let stale_in_cycle1 = (0..cycle1.len().saturating_sub(6))
        .filter(|i| cycle1[*i] == 0xFD && cycle1[i + 5] == 99)
        .count();
    assert_eq!(
        stale_in_cycle1, 0,
        "cycle 1 leaked {stale_in_cycle1} stale frames after reconnect"
    );

    // -- Flap cycle 2 --
    drop(conn2);
    drop(listener2);
    inject_burst(&injector, 99, 50..80).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let listener3 = bind_reusable_listener(listen_addr);
    let (mut conn3, _) = timeout(Duration::from_secs(3), listener3.accept())
        .await
        .expect("cycle-2 reconnect accept timeout")
        .expect("cycle-2 reconnect accept");
    tokio::time::sleep(Duration::from_millis(100)).await;
    inject_burst(&injector, 10, 200..220).await;
    let cycle2 = read_until_quiet(&mut conn3, 1024, Duration::from_millis(500)).await;
    let stale_in_cycle2 = (0..cycle2.len().saturating_sub(6))
        .filter(|i| cycle2[*i] == 0xFD && cycle2[i + 5] == 99)
        .count();
    assert_eq!(
        stale_in_cycle2, 0,
        "cycle 2 leaked {stale_in_cycle2} stale frames after reconnect"
    );

    // Wait long enough that at least one stats interval fires after the
    // two flap cycles so a steady-state line (not just the final
    // synthetic) reflects the climbed `dropped_tx`.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let kill_status = StdCommand::new("/bin/kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("kill spawn");
    assert!(kill_status.success(), "/bin/kill -TERM failed");

    let started = Instant::now();
    let exit_status = loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => break status,
            None => {
                if started.elapsed() > Duration::from_secs(7) {
                    let _ = child.kill();
                    panic!("rmr binary did not exit within 7s of SIGTERM");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    };
    assert!(
        exit_status.success(),
        "rmr exited non-zero: {exit_status:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "shutdown took {:?}; suggests task leak across flap cycles",
        started.elapsed()
    );

    let mut out = Vec::new();
    let mut err = Vec::new();
    if let Some(mut s) = child.stdout.take() {
        let _ = s.read_to_end(&mut out);
    }
    if let Some(mut s) = child.stderr.take() {
        let _ = s.read_to_end(&mut err);
    }
    let stdout = String::from_utf8(out).expect("stdout utf8");
    let stderr = String::from_utf8(err).expect("stderr utf8");

    // Two flap cycles × 30 stale frames each = 60 inbound frames bound
    // for tcpc that never reach the wire. With tx_queue_frames=8, at
    // most 8 can survive in the queue at any moment; the rest are
    // `force_push` evictions (router-side) or drain-on-disconnect drops
    // (writer-side). A floor of 20 leaves comfortable headroom against
    // scheduling jitter while still proving the counter moved.
    let mut max_dropped_client: u64 = 0;
    for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if v["endpoint"].as_str() != Some("client") {
            continue;
        }
        if let Some(n) = v["dropped_tx"].as_u64()
            && n > max_dropped_client
        {
            max_dropped_client = n;
        }
    }
    assert!(
        max_dropped_client >= 20,
        "expected dropped_tx >= 20 on tcpc 'client' after 2 flap cycles; \
         got max={max_dropped_client}. stdout=`{stdout}` stderr=`{stderr}`"
    );
}
