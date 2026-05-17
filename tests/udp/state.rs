//! Endpoint state transitions for `udps:` and `udpc:` (CLAUDE.md Phase 5a
//! "EndpointState on EndpointStats" decision and the split-authority rule).
//!
//! UDP has no transport-up/down event after the initial bind succeeds, so
//! the endpoint task writes `Connected` exactly once and never flips back
//! to `Reconnecting` on its own. Sub-endpoint state (UDP peers) is
//! initialised to `Connected` at admission (admission is the transport-up
//! event) and only the router writes `Idle` / `Down` afterwards — see
//! CLAUDE.md.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{
    EndpointIdAllocator,
    spec::UdpClientEndpoint,
    stats::{EndpointState, EndpointStats},
};

use crate::common::shutdown_all;
use crate::common::udp::{spawn_udpc, spawn_udps};

async fn wait_for_state(stats: &Arc<EndpointStats>, target: EndpointState, label: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if stats.load_state() == target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "state never reached {target:?} ({label}); last observed: {:?}",
        stats.load_state()
    );
}

#[tokio::test]
async fn udps_listener_transitions_reconnecting_to_connected_on_bind() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();
    let h = spawn_udps(&allocator, cancel.clone(), "p").await;
    // spawn_udps awaits the bound_addr handshake, so by the time it returns
    // the listener has bound — state must be Connected.
    assert_eq!(h.stats.load_state(), EndpointState::Connected);
    shutdown_all(&cancel, [h.task]).await;
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
    let h = spawn_udpc(
        &allocator,
        cancel.clone(),
        configured,
        UdpClientEndpoint::default(),
        "c",
    );
    wait_for_state(&h.stats, EndpointState::Connected, "after udpc bind").await;
    shutdown_all(&cancel, [h.task]).await;
}
