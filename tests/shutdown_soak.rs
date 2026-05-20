//! End-to-end shutdown drain through `rmr::run`. Cancel the router mid-run
//! with a mix of endpoints in different lifecycle states — a UDP server
//! (Connected via bind), a UDP client (Connected via local bind), and a
//! TCP client pointing at an unbound port (stuck in Reconnecting forever)
//! — and assert that:
//!
//! - every spawned task joins within the 5s wall-clock shutdown budget
//!   documented in CLAUDE.md ("Shutdown timing" locked decision),
//! - the function returns `Ok(())` (no panic propagated),
//! - the total elapsed time stays comfortably under the budget.
//!
//! A binary-driven `#[cfg(unix)]` case below spawns `rmr` as a subprocess
//! and delivers SIGTERM, exercising the same `CancellationToken`-driven
//! drain via the binary's signal handler.

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

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

/// Pick a UDP address bound to an OS-chosen port, drop it, and return the
/// address. The drop-then-reuse window is tolerable on loopback for tests.
fn pick_free_udp_addr() -> SocketAddr {
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("local_addr")
}

#[tokio::test]
async fn shutdown_drains_every_endpoint_within_budget() {
    // Mixed-state endpoints:
    // - listener: udps that successfully binds → Connected
    // - sender:   udpc dialing nowhere → Connected after local bind
    // - stuck:    tcpc dialing a port nobody is listening on → loops on
    //             reconnect forever, stays Reconnecting
    let listener = pick_free_udp_addr();
    let sender_target = pick_free_udp_addr();

    let config = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#listener", listener.port()),
        format!("udpc:127.0.0.1:{}#sender", sender_target.port()),
        // Port 1 is privileged; binding loopback to it as a client almost
        // always fails with ConnectionRefused, keeping `stuck` perpetually
        // in Reconnecting until cancel arrives.
        "tcpc:127.0.0.1:1#stuck".to_string(),
    ]);

    let cancel = CancellationToken::new();
    let cancel_for_run = cancel.clone();
    let handle = tokio::spawn(async move { rmr::run(config, cancel_for_run).await });

    // Let bind/dial loops settle so the cancel hits steady state, not
    // immediately-post-spawn pre-state.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = Instant::now();
    cancel.cancel();

    // Per CLAUDE.md "Shutdown timing": 5s overall wall-clock; the harness's
    // own per-task drain is 2s. We give the join itself 7s of headroom so a
    // flaky CI tick doesn't pretend to be a real hang; the elapsed assertion
    // below is what enforces the actual budget.
    let result = timeout(Duration::from_secs(7), handle)
        .await
        .expect("rmr::run did not return within shutdown grace + headroom")
        .expect("rmr::run task panicked");
    let elapsed = started.elapsed();
    result.expect("rmr::run returned an error");

    assert!(
        elapsed < Duration::from_secs(6),
        "shutdown took {elapsed:?}, exceeding the 5s budget (plus 1s slack)"
    );
}

#[tokio::test]
async fn shutdown_returns_immediately_when_no_endpoints_are_connected() {
    // Every endpoint is stuck in Reconnecting — drain should still complete
    // quickly because each task observes the cancel and exits its retry
    // loop. Pins the lower-bound behaviour: cancel-then-join should not
    // burn the full grace window when there's nothing to flush.
    let config = config_with_endpoints(vec![
        "tcpc:127.0.0.1:1#a".to_string(),
        "tcpc:127.0.0.1:2#b".to_string(),
        "tcpc:127.0.0.1:3#c".to_string(),
    ]);

    let cancel = CancellationToken::new();
    let cancel_for_run = cancel.clone();
    let handle = tokio::spawn(async move { rmr::run(config, cancel_for_run).await });

    tokio::time::sleep(Duration::from_millis(150)).await;
    let started = Instant::now();
    cancel.cancel();

    let result = timeout(Duration::from_secs(3), handle)
        .await
        .expect("rmr::run hung when no endpoint was connected")
        .expect("rmr::run task panicked");
    result.expect("rmr::run returned an error");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "shutdown with no live endpoints should be near-instant; got {elapsed:?}"
    );
}

#[tokio::test]
async fn shutdown_drains_with_inflight_udp_traffic() {
    // udps with a freshly-learned peer: the listener has work in flight (a
    // peer writer task that's been admitted) at the moment cancel fires.
    // The shutdown must still complete within budget.
    let listener_addr = pick_free_udp_addr();
    let probe = UdpSocket::bind("127.0.0.1:0").await.expect("probe bind");
    let peer_addr: SocketAddr = probe.local_addr().expect("probe local");

    let config = config_with_endpoints(vec![
        format!("udps:127.0.0.1:{}#listener", listener_addr.port()),
        format!("udpc:127.0.0.1:{}#fanout", peer_addr.port()),
    ]);

    let cancel = CancellationToken::new();
    let cancel_for_run = cancel.clone();
    let handle = tokio::spawn(async move { rmr::run(config, cancel_for_run).await });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send a HEARTBEAT into udps so the listener learns the peer and spawns
    // its per-peer writer task — cancel will then hit while that task is
    // active.
    let injector = UdpSocket::bind("127.0.0.1:0").await.expect("injector bind");
    injector
        .send_to(&common::build_v2_heartbeat(0), listener_addr)
        .await
        .expect("inject heartbeat");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = Instant::now();
    cancel.cancel();

    let result = timeout(Duration::from_secs(6), handle)
        .await
        .expect("rmr::run did not drain in time")
        .expect("rmr::run task panicked");
    result.expect("rmr::run returned an error");
    assert!(started.elapsed() < Duration::from_secs(5));
}

/// Binary-driven shutdown soak: spawn `rmr` as a subprocess with `--stats`,
/// send SIGTERM while it's serving endpoints, assert it exits 0 within the
/// wall-clock budget AND that every registered endpoint's last JSON-Line on
/// stdout has a terminal `state` (`down`/`idle`) — the literal CLAUDE.md
/// Phase 6 bullet "assert every registered endpoint emits its final
/// synthetic stats line with the right terminal state". Unix-only because
/// the Windows side of the signal contract has no portable analogue to
/// SIGTERM (kill-on-Windows terminates without running the in-process
/// cancellation handler).
#[cfg(unix)]
#[test]
fn binary_shutdown_emits_final_synthetic_stats_lines() {
    use assert_cmd::Command;
    use std::collections::HashMap;
    use std::io::Read;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command as StdCommand, Stdio};
    use std::thread;
    use std::time::Duration;

    let listener_addr = pick_free_udp_addr();
    let sender_target = pick_free_udp_addr();
    let endpoints = [
        // Connected after bind.
        format!("udps:127.0.0.1:{}#listener", listener_addr.port()),
        // Connected after local socket bind.
        format!("udpc:127.0.0.1:{}#sender", sender_target.port()),
        // Reconnecting forever (no listener on port 1).
        "tcpc:127.0.0.1:1#stuck".to_string(),
    ];

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
            &endpoints[0],
            &endpoints[1],
            &endpoints[2],
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rmr");

    // Let at least one stats interval fire so steady-state lines accumulate
    // and the final synthetic lines are distinguishable as "after the last
    // interval" lines.
    thread::sleep(Duration::from_millis(1_500));

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
                thread::sleep(Duration::from_millis(20));
            }
        }
    };

    assert!(
        exit_status.success(),
        "rmr exited non-zero on SIGTERM: {exit_status:?}; signal={:?}",
        exit_status.signal()
    );
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "rmr took {:?} to exit; budget is 5s plus 1s slack",
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

    // Walk every JSON-Line, recording the most-recent (state) per endpoint.
    let mut last_state: HashMap<String, String> = HashMap::new();
    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        let value: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|err| panic!("bad json on stdout: '{line}': {err}"));
        let endpoint = value["endpoint"]
            .as_str()
            .unwrap_or_else(|| panic!("line missing endpoint field: {line}"));
        let state = value["state"]
            .as_str()
            .unwrap_or_else(|| panic!("line missing state field: {line}"));
        last_state.insert(endpoint.to_string(), state.to_string());
    }
    assert!(
        !last_state.is_empty(),
        "no JSON-Lines on stdout; stderr=`{stderr}`"
    );
    for name in ["listener", "sender", "stuck"] {
        let final_state = last_state.get(name).unwrap_or_else(|| {
            panic!("no final stats line for endpoint '{name}'; got: {last_state:?}")
        });
        assert!(
            final_state == "down" || final_state == "idle",
            "endpoint '{name}' final state is '{final_state}', not down/idle; stderr=`{stderr}`"
        );
    }
}
