//! Endpoint state transitions for `udps:` and `udpc:` under the
//! split-authority rule on [`EndpointStats::state`].
//!
//! UDP has no transport-up/down event after the initial bind succeeds, so
//! the endpoint task writes `Connected` exactly once and never flips back
//! to `Reconnecting` on its own. Sub-endpoint state (UDP peers) is
//! initialised to `Connected` at admission (admission is the transport-up
//! event) and only the router writes `Idle` / `Down` afterwards.
//!
//! [`EndpointStats::state`]: rmr::endpoint::stats::EndpointStats::state

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{EndpointIdAllocator, spec::UdpClientEndpoint, stats::EndpointState};

use crate::common::udp::{spawn_udpc, spawn_udps};
use crate::common::{shutdown_all, wait_for_state};

#[tokio::test]
async fn udps_listener_transitions_reconnecting_to_connected_on_bind() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();
    let harness = spawn_udps(&allocator, cancel.clone(), "p").await;
    // spawn_udps awaits the bound_addr handshake, so by the time it returns
    // the listener has bound — state must be Connected.
    assert_eq!(harness.stats.load_state(), EndpointState::Connected);
    shutdown_all(&cancel, [harness.task]).await;
}

#[tokio::test]
async fn udpc_transitions_to_connected_after_local_bind() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();
    let configured: SocketAddr = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind dummy")
        .local_addr()
        .expect("local_addr");
    let harness = spawn_udpc(
        &allocator,
        cancel.clone(),
        configured,
        UdpClientEndpoint::default(),
        "c",
    );
    wait_for_state(&harness.stats, EndpointState::Connected, "after udpc bind").await;
    shutdown_all(&cancel, [harness.task]).await;
}
