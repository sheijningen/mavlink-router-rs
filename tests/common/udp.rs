//! Spawn helpers for `udps:` and `udpc:` integration tests. Each `spawn_*`
//! call returns a harness bundling the task handle with the channels and
//! queues the test needs to play the role of a router stub.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::events::{EndpointEvent, RouterFrame};
use rmr::endpoint::spec::{UdpClientEndpoint, UdpServerEndpoint};
use rmr::endpoint::stats::{EndpointState, EndpointStats};
use rmr::endpoint::tx_queue::TxQueue;
use rmr::endpoint::udp::client::{
    UdpClientSpec, {self as udp_client},
};
use rmr::endpoint::udp::server::{
    UdpServerSpec, {self as udp_server},
};
use rmr::endpoint::wiring::{ClientWiring, ServerWiring};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::common::wait_for_state;

/// Bundle of channels and the join handle for a spawned `udps:` listener task.
/// Tests destructure or borrow these fields to play the router-stub role.
pub struct UdpsHarness {
    /// Address the listener was asked to bind to — also the `send_to` target
    /// callers use, since `spawn_udps*` resolves `127.0.0.1:0` to a concrete
    /// port via [`pick_free_udp_addr`] before constructing the spec.
    pub listen_addr: SocketAddr,
    /// Per-frame stream from the listener (ingress as seen by the router).
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    /// Lifecycle stream announcing learned peers and idle reaps.
    pub event_rx: mpsc::Receiver<EndpointEvent>,
    /// Shared stats handle for the parent listener — tests can assert state
    /// transitions (Reconnecting → Connected on first bind). `spawn_udps*`
    /// already awaits that transition; bind-retry tests poll it explicitly.
    pub stats: Arc<EndpointStats>,
    /// Join handle of the spawned task; await after cancelling.
    pub task: JoinHandle<()>,
}

/// Bind a `std::net::UdpSocket` on `127.0.0.1:0`, capture the assigned port,
/// and drop the socket. The returned `SocketAddr` is what the test passes to
/// `udps:`/`udpc:`. The listener's `bind_with_backoff` curve absorbs the
/// (microscopic) race where another process grabs the port in the gap.
pub fn pick_free_udp_addr() -> SocketAddr {
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("probe local_addr")
}

/// Spawn a `udps:` listener bound to a probe-picked free port and await the
/// `Reconnecting → Connected` transition so the returned harness is ready
/// to receive packets.
pub async fn spawn_udps(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    name: &str,
) -> UdpsHarness {
    spawn_udps_with_endpoint(allocator, cancel, name, UdpServerEndpoint::default()).await
}

/// Like `spawn_udps` but lets the test pass a parsed `UdpServerEndpoint` with
/// `Option<…>` knob overrides — typically to shorten `idle_secs` for fast
/// idle-reap coverage.
pub async fn spawn_udps_with_endpoint(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    name: &str,
    mut endpoint: UdpServerEndpoint,
) -> UdpsHarness {
    endpoint.bind_addr = pick_free_udp_addr();
    let parent_id = allocator.alloc();
    let spec = UdpServerSpec::from_endpoint(endpoint, parent_id, name.into());
    let harness = spawn_udps_with_spec(allocator, cancel, spec);
    wait_for_state(&harness.stats, EndpointState::Connected, "udps bind").await;
    harness
}

/// Spawn a `udps:` listener with a fully-constructed `UdpServerSpec`. The
/// caller pre-allocates `parent_id` (which they place inside `spec`) and is
/// responsible for mutating any knobs that aren't reachable through the
/// parsed `UdpServerEndpoint` (e.g. the reconnect curve, which `udps:` does
/// not expose as a query override). Returns immediately without awaiting
/// bind so bind-retry tests can drive the bind path before observing
/// `Reconnecting → Connected` on the harness's `stats`.
pub fn spawn_udps_with_spec(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    spec: UdpServerSpec,
) -> UdpsHarness {
    let listen_addr = spec.listen_addr;
    let allocator = allocator.clone();
    let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(32);
    let task = {
        let stats = stats.clone();
        tokio::spawn(async move {
            udp_server::run(
                spec,
                ServerWiring {
                    allocator,
                    frame_tx,
                    event_tx,
                    cancel,
                    stats,
                },
            )
            .await
        })
    };
    UdpsHarness {
        listen_addr,
        frame_rx,
        event_rx,
        stats,
        task,
    }
}

/// Bundle of channels and the join handle for a spawned `udpc:` client task.
/// Tests destructure or borrow these fields to play the router-stub role.
pub struct UdpcHarness {
    /// Per-frame stream from the client (ingress as seen by the router).
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    /// Queue the test pushes onto to schedule egress frames.
    pub tx_queue: TxQueue,
    /// Shared stats handle — tests can assert state transitions.
    pub stats: Arc<EndpointStats>,
    /// Join handle of the spawned task; await after cancelling.
    pub task: JoinHandle<()>,
}

/// Spawn a `udpc:` client targeting `configured_addr`. The address's IP is
/// passed verbatim as the configured host (no DNS), so tests can use IPv4 or
/// IPv6 loopback interchangeably.
pub fn spawn_udpc(
    allocator: &EndpointIdAllocator,
    cancel: CancellationToken,
    configured_addr: SocketAddr,
    endpoint: UdpClientEndpoint,
    name: &str,
) -> UdpcHarness {
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
    let tx_queue = TxQueue::new(8, stats.clone());
    let endpoint = UdpClientEndpoint {
        host: configured_addr.ip().to_string(),
        port: configured_addr.port(),
        ..endpoint
    };
    let spec = UdpClientSpec::from_endpoint(endpoint, endpoint_id, name.to_string());
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let task = {
        let tx_queue = tx_queue.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            udp_client::run(
                spec,
                ClientWiring {
                    frame_tx,
                    tx_queue,
                    stats,
                    cancel,
                },
            )
            .await
        })
    };
    UdpcHarness {
        frame_rx,
        tx_queue,
        stats,
        task,
    }
}

/// Push a frame onto a `udpc:`'s TxQueue and capture the source address it
/// arrives at — i.e. the udpc task's outgoing socket address as observed at
/// the configured peer. Verifies the bytes round-tripped intact.
pub async fn udpc_send_and_capture_source(
    tx_queue: &TxQueue,
    configured: &UdpSocket,
    frame: &[u8],
) -> SocketAddr {
    tx_queue.push(bytes::Bytes::copy_from_slice(frame));
    let mut buf = [0u8; 256];
    let (bytes_read, src_addr) = timeout(Duration::from_secs(2), configured.recv_from(&mut buf))
        .await
        .expect("configured recv timeout")
        .expect("configured recv_from");
    assert_eq!(
        &buf[..bytes_read],
        frame,
        "udpc frame bytes mismatch at configured peer"
    );
    src_addr
}
