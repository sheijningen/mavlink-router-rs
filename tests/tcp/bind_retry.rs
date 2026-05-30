//! `tcps:` against an in-use port stays in `Reconnecting` and attaches
//! once the port frees. Unix-only — Windows `SO_REUSEADDR` lets the second
//! bind succeed immediately, which defeats the test.

#![cfg(unix)]

use std::sync::Arc;
use std::time::Duration;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::spec::TcpServerEndpoint;
use rmr::endpoint::stats::EndpointState;
use rmr::endpoint::tcp::server::TcpServerSpec;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::common;
use crate::common::tcp::{connect_with_retry, spawn_tcps_with_spec};
use crate::common::{next_peer_added, shutdown_all};

#[tokio::test]
async fn tcps_attaches_when_pre_held_port_is_freed() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    // Reserve a port with a plain std listener. On Linux, RMR's tcps bind to
    // the same port will fail with EADDRINUSE while this listener is alive
    // (both sides have SO_REUSEADDR, but Linux still refuses overlapping
    // LISTEN sockets — that's the point).
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let listen_addr = probe.local_addr().expect("probe local_addr");

    // Short backoff so we don't have to wait long for tcps to attach. The
    // reconnect curve isn't exposed as a `*Endpoint` query knob, so we
    // build the Spec from defaults then mutate.
    let endpoint = TcpServerEndpoint {
        bind_addr: listen_addr,
        ..TcpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let mut spec = TcpServerSpec::from_endpoint(endpoint, parent_id, "tcps".into());
    spec.reconnect_initial_ms = 50;
    spec.reconnect_max_ms = 250;
    let mut harness = spawn_tcps_with_spec(&allocator, cancel.clone(), spec);

    // While the probe holds the port, the listener stays in Reconnecting.
    // Poll for early task termination without an unconditional sleep — if
    // tcps panicked on the first bind error, the task ends and the assertion
    // fires immediately; otherwise the loop exits once the deadline passes.
    let probe_deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < probe_deadline {
        assert!(
            !harness.task.is_finished(),
            "tcps task ended early — bind failure should have been retried, not propagated"
        );
        assert_eq!(
            harness.stats.load_state(),
            EndpointState::Reconnecting,
            "tcps should still be retrying while the probe holds the port"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Free the port. tcps's next backoff iteration (≤ ~250 ms) should bind,
    // flipping the listener's state to Connected.
    drop(probe);

    timeout(Duration::from_secs(3), async {
        loop {
            if harness.stats.load_state() == EndpointState::Connected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("tcps did not reach Connected after probe freed the port");

    // connect_with_retry tolerates the short window before tcps re-binds.
    let mut client = connect_with_retry(listen_addr, Duration::from_secs(3)).await;

    let frame = common::build_v2_heartbeat(0);
    client.write_all(&frame).await.expect("client write");
    let _added = next_peer_added(&mut harness.event_rx).await;
    let router_frame = timeout(Duration::from_secs(2), harness.frame_rx.recv())
        .await
        .expect("frame timeout")
        .expect("frame_rx closed");
    assert_eq!(&router_frame.frame[..], &frame[..]);

    shutdown_all(&cancel, [harness.task]).await;
}
