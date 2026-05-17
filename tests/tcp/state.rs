//! Endpoint state transitions for `tcps:` and `tcpc:` (CLAUDE.md Phase 5a
//! "EndpointState on EndpointStats" decision and the split-authority rule).
//!
//! The endpoint task owns the `Connected` / `Reconnecting` writes; the
//! router owns `Idle` / `Down`. These tests prove the endpoint side fires
//! the transitions at the right edges — `Reconnecting → Connected` after
//! bind/connect succeeds, and `Connected → Reconnecting` on a `tcpc:`
//! disconnect-with-retry.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{EndpointIdAllocator, spec::TcpClientEndpoint, stats::EndpointState};

use crate::common::tcp::{spawn_tcpc, spawn_tcps};
use crate::common::{shutdown_all, wait_for_state};

#[tokio::test]
async fn tcps_listener_transitions_reconnecting_to_connected_on_bind() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();
    let h = spawn_tcps(&allocator, cancel.clone(), "p").await;
    // spawn_tcps awaits the bound_addr handshake, so by the time it returns
    // the listener has bound — state must be Connected.
    assert_eq!(h.stats.load_state(), EndpointState::Connected);
    shutdown_all(&cancel, [h.task]).await;
}

#[tokio::test]
async fn tcpc_transitions_through_connect_then_reconnect_cycle() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("local_addr");

    let endpoint = TcpClientEndpoint {
        reconnect_initial_ms: Some(50),
        reconnect_max_ms: Some(500),
        ..TcpClientEndpoint::default()
    };
    let h = spawn_tcpc(&allocator, cancel.clone(), addr, endpoint, "c");

    // Reconnecting at spawn time (harness constructs EndpointStats::new(
    // Reconnecting) just like the production spawner will).
    assert_eq!(h.stats.load_state(), EndpointState::Reconnecting);

    // Once tcpc establishes its first session, state must be Connected.
    let (server, _peer) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("initial accept timeout")
        .expect("initial accept");
    wait_for_state(&h.stats, EndpointState::Connected, "after connect").await;

    // Drop both the accepted stream AND the listener — otherwise tcpc's
    // immediate redial succeeds on the open listener within ~1ms and the
    // Reconnecting transition is invisible to the 10ms polling loop. With
    // the listener gone the dial fails (ECONNREFUSED) and tcpc settles
    // into its backoff sleep, which lives long enough to observe.
    drop(server);
    drop(listener);
    wait_for_state(&h.stats, EndpointState::Reconnecting, "after disconnect").await;

    // Bring the listener back; tcpc's next dial succeeds and state flips
    // back to Connected.
    let listener2 = TcpListener::bind(addr).await.expect("rebind");
    let (_server2, _peer2) = timeout(Duration::from_secs(3), listener2.accept())
        .await
        .expect("reconnect accept timeout")
        .expect("reconnect accept");
    wait_for_state(&h.stats, EndpointState::Connected, "after reconnect").await;

    shutdown_all(&cancel, [h.task]).await;
}
