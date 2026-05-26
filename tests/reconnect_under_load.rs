//! End-to-end: under sustained UDP ingress, a `tcpc:` flap discards
//! queued frames before reconnect so the peer never sees stale telemetry
//! ahead of fresh frames.

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::mavlink::{Heartbeat, TestFrame};
use common::tcp::read_until_quiet;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::{TcpListener, UdpSocket};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Bind a TCP listener on `addr` with `SO_REUSEADDR` so the test can drop
/// it and rebind the same port without waiting out the kernel's release
/// window. `tokio::net::TcpListener::bind` does not expose `SO_REUSEADDR`
/// directly, hence the manual `socket2` dance.
fn bind_reusable_listener(addr: SocketAddr) -> TcpListener {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).expect("socket");
    socket.set_reuse_address(true).expect("set_reuse_address");
    socket.set_nonblocking(true).expect("nonblocking");
    socket.bind(&addr.into()).expect("bind");
    socket.listen(128).expect("listen");
    TcpListener::from_std(socket.into()).expect("from_std")
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

    let udps_addr = common::udp::pick_free_udp_addr();
    // Bind the fake server with SO_REUSEADDR so we can rebind the same
    // port mid-test after dropping it (and the connection it accepted).
    let server = bind_reusable_listener("127.0.0.1:0".parse().unwrap());
    let server_addr: SocketAddr = server.local_addr().expect("server addr");

    let config = common::config_with_endpoints(
        vec![
            format!("udps:127.0.0.1:{}#listener", udps_addr.port()),
            format!("tcpc:127.0.0.1:{}#downlink", server_addr.port()),
        ],
        0,
    );

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(config, cancel).await })
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
    for frame in &pre {
        peer_a.send_to(frame, udps_addr).await.expect("pre send");
    }

    let bytes_pre = read_until_quiet(
        &mut peer_c1,
        pre.iter().map(|frame| frame.len()).sum(),
        Duration::from_millis(500),
    )
    .await;
    for frame in &pre {
        assert!(
            bytes_pre
                .windows(frame.len())
                .any(|window| window == &frame[..]),
            "peer_c1 missing a pre-batch frame (sysid={})",
            frame[5] // v2 sysid byte position
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
    for frame in &during {
        peer_a.send_to(frame, udps_addr).await.expect("during send");
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
    for frame in &post {
        peer_a.send_to(frame, udps_addr).await.expect("post send");
    }

    let bytes_post = read_until_quiet(
        &mut peer_c2,
        post.iter().map(|frame| frame.len()).sum(),
        Duration::from_millis(800),
    )
    .await;

    // The post-reconnect stream must include every "post" frame.
    for frame in &post {
        assert!(
            bytes_post
                .windows(frame.len())
                .any(|window| window == &frame[..]),
            "peer_c2 missing a post-batch frame (sysid={}); got {} bytes total: {:?}",
            frame[5],
            bytes_post.len(),
            bytes_post.iter().take(8).collect::<Vec<_>>(),
        );
    }
    // And must NOT include any "during" frame — that's the no-replay
    // guarantee. We also confirm "pre" frames don't reappear.
    for frame in during.iter().chain(pre.iter()) {
        assert!(
            !bytes_post
                .windows(frame.len())
                .any(|window| window == &frame[..]),
            "peer_c2 received a stale frame after reconnect (sysid={}); \
             drain-and-discard contract violated",
            frame[5]
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
    let udps_addr = common::udp::pick_free_udp_addr();
    let server = TcpListener::bind("127.0.0.1:0").await.expect("server bind");
    let server_addr: SocketAddr = server.local_addr().expect("server addr");

    let config = common::config_with_endpoints(
        vec![
            format!("udps:127.0.0.1:{}#listener", udps_addr.port()),
            format!("tcpc:127.0.0.1:{}#downlink", server_addr.port()),
        ],
        0,
    );

    let cancel = CancellationToken::new();
    let run_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move { rmr::run(config, cancel).await })
    };

    let peer_a = UdpSocket::bind("127.0.0.1:0").await.expect("peer_a bind");

    for cycle in 0..5u8 {
        // Accept one connection, send a couple frames, drop.
        let (mut stream, _) = timeout(Duration::from_secs(3), server.accept())
            .await
            .unwrap_or_else(|_| panic!("accept timed out at cycle {cycle}"))
            .expect("accept");
        for seq in 0u8..3u8 {
            let frame = TestFrame::v2_message(&Heartbeat::default())
                .sysid(cycle.wrapping_add(40))
                .compid(1)
                .seq(seq)
                .build();
            peer_a.send_to(&frame, udps_addr).await.expect("send");
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
    let frame = TestFrame::v2_message(&Heartbeat::default())
        .sysid(99)
        .compid(1)
        .seq(0)
        .build();
    peer_a.send_to(&frame, udps_addr).await.expect("final send");
    let bytes = read_until_quiet(&mut final_stream, frame.len(), Duration::from_millis(800)).await;
    assert!(
        bytes
            .windows(frame.len())
            .any(|window| window == &frame[..]),
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

/// Binary-driven flap-cycle test that asserts `dropped_tx` accounting at
/// the wire level. Spawns `rmr --stats`, drops the downstream `tcps:`
/// listener, and floods the `udps:` ingress with a >256-frame burst per
/// cycle so the `tcpc:` writer queue (at the hardcoded
/// `DEFAULT_TX_QUEUE_FRAMES = 256`) overflows on the router-side
/// `force_push` path on top of the writer-side `drain_and_discard` on
/// reconnect. After two flap cycles, parses stdout JSON-Lines and asserts
/// the tcpc endpoint's `dropped_tx` counter climbed past a wide-margin
/// floor. Unix-only — same signal-portability caveat as
/// `binary_shutdown_soak`.
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
    let udps_addr = common::udp::pick_free_udp_addr();

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
            // Default `tx_queue_frames=256`. The during-disconnect burst
            // below sends >256 frames per cycle so router-side force_push
            // eviction fires on the latter half, on top of the writer-side
            // drain-on-disconnect drops.
            &format!("tcpc:127.0.0.1:{}#client", listen_addr.port()),
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
    // Burst >256 stale frames into the udps while rmr can't reach the
    // (gone) tcps listener — the first ~256 fill tcpc's TxQueue, the
    // overflow triggers router-side force_push eviction, and whatever is
    // still in the queue at reconnect gets drained-and-discarded.
    // `inject_burst` takes Range<u8>, so use two back-to-back bursts to
    // exceed 256 (seq wraps; the router doesn't dedup here).
    inject_burst(&injector, 99, 0..255).await;
    inject_burst(&injector, 99, 0..50).await;
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
        .filter(|index| cycle1[*index] == 0xFD && cycle1[index + 5] == 99)
        .count();
    assert_eq!(
        stale_in_cycle1, 0,
        "cycle 1 leaked {stale_in_cycle1} stale frames after reconnect"
    );

    // -- Flap cycle 2 --
    drop(conn2);
    drop(listener2);
    inject_burst(&injector, 99, 0..255).await;
    inject_burst(&injector, 99, 0..50).await;
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
        .filter(|index| cycle2[*index] == 0xFD && cycle2[index + 5] == 99)
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
    if let Some(mut stdout) = child.stdout.take() {
        let _ = stdout.read_to_end(&mut out);
    }
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_end(&mut err);
    }
    let stdout = String::from_utf8(out).expect("stdout utf8");
    let stderr = String::from_utf8(err).expect("stderr utf8");

    // Two flap cycles × ~305 stale frames each = ~610 inbound frames
    // bound for tcpc that never reach the wire. With the default 256-
    // deep queue, at most 256 survive in the queue at any moment; the
    // rest hit `force_push` (router-side) or drain-on-disconnect
    // (writer-side). A floor of 20 leaves comfortable headroom against
    // scheduling jitter and any UDP-receive drops while still proving
    // the counter moved.
    let mut max_dropped_client: u64 = 0;
    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if value["endpoint"].as_str() != Some("client") {
            continue;
        }
        if let Some(count) = value["dropped_tx"].as_u64()
            && count > max_dropped_client
        {
            max_dropped_client = count;
        }
    }
    assert!(
        max_dropped_client >= 20,
        "expected dropped_tx >= 20 on tcpc 'client' after 2 flap cycles; \
         got max={max_dropped_client}. stdout=`{stdout}` stderr=`{stderr}`"
    );
}
