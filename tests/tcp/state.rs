//! `tcps:`/`tcpc:` state transitions on the endpoint-owned half of
//! `EndpointStats::state`: `Reconnecting → Connected` on bind/connect and
//! `Connected → Reconnecting` on `tcpc:` disconnect.

use std::sync::Arc;
use std::time::Duration;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::spec::TcpClientEndpoint;
use rmr::endpoint::stats::EndpointState;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::common::tcp::{spawn_tcpc_with, spawn_tcps};
use crate::common::{shutdown_all, wait_for_state};

#[tokio::test]
async fn tcps_listener_transitions_reconnecting_to_connected_on_bind() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();
    let harness = spawn_tcps(&allocator, cancel.clone(), "p").await;
    // spawn_tcps awaits the bound_addr handshake, so by the time it returns
    // the listener has bound — state must be Connected.
    assert_eq!(harness.stats.load_state(), EndpointState::Connected);
    shutdown_all(&cancel, [harness.task]).await;
}

#[tokio::test]
async fn tcpc_transitions_through_connect_then_reconnect_cycle() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("local_addr");

    let harness = spawn_tcpc_with(
        &allocator,
        cancel.clone(),
        addr,
        TcpClientEndpoint::default(),
        "c",
        |spec| {
            spec.reconnect_initial_ms = 50;
            spec.reconnect_max_ms = 500;
        },
    );

    // Reconnecting at spawn time (harness constructs EndpointStats::new(
    // Reconnecting) just like the production spawner will).
    assert_eq!(harness.stats.load_state(), EndpointState::Reconnecting);

    // Once tcpc establishes its first session, state must be Connected.
    let (server, _peer) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("initial accept timeout")
        .expect("initial accept");
    wait_for_state(&harness.stats, EndpointState::Connected, "after connect").await;

    // Drop both the accepted stream AND the listener — otherwise tcpc's
    // immediate redial succeeds on the open listener within ~1ms and the
    // Reconnecting transition is invisible to the 10ms polling loop. With
    // the listener gone the dial fails (ECONNREFUSED) and tcpc settles
    // into its backoff sleep, which lives long enough to observe.
    drop(server);
    drop(listener);
    wait_for_state(
        &harness.stats,
        EndpointState::Reconnecting,
        "after disconnect",
    )
    .await;

    // Bring the listener back; tcpc's next dial succeeds and state flips
    // back to Connected.
    let listener2 = TcpListener::bind(addr).await.expect("rebind");
    let (_server2, _peer2) = timeout(Duration::from_secs(3), listener2.accept())
        .await
        .expect("reconnect accept timeout")
        .expect("reconnect accept");
    wait_for_state(&harness.stats, EndpointState::Connected, "after reconnect").await;

    shutdown_all(&cancel, [harness.task]).await;
}
