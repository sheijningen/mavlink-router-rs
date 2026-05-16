//! Spawn helpers for `udps:` and `udpc:` integration tests. Each `spawn_*`
//! call returns a harness bundling the task handle with the channels and
//! queues the test needs to play the role of a router stub.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{
    EndpointIdAllocator,
    events::{EndpointEvent, RouterFrame},
    identity_flags::IdentityFlags,
    stats::EndpointStats,
    tx_queue::TxQueue,
    udp::client::{
        self as udp_client, UdpClientConfig, UdpClientError, UdpClientSpec, UdpClientWiring,
    },
    udp::server::{
        self as udp_server, UdpServerConfig, UdpServerError, UdpServerSpec, UdpServerWiring,
    },
};

/// Bundle of channels and the join handle for a spawned `udps:` listener task.
/// Tests destructure or borrow these fields to play the router-stub role.
pub struct UdpsHarness {
    /// Address the listener actually bound to — use as the `send_to` target.
    /// Filled in by `spawn_udps*` once the OS has assigned a port; bind-retry
    /// tests that drive the spawn manually rely on `bound_addr_rx` instead.
    pub listen_addr: SocketAddr,
    /// Per-frame stream from the listener (ingress as seen by the router).
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    /// Lifecycle stream announcing learned peers and idle reaps.
    pub event_rx: mpsc::Receiver<EndpointEvent>,
    /// Resolves on the first successful bind. Already consumed by `spawn_udps*`;
    /// `spawn_udps_at_with_config` leaves it for the caller (bind-retry tests).
    pub bound_addr_rx: Option<oneshot::Receiver<SocketAddr>>,
    /// Join handle of the spawned task; await after cancelling.
    pub task: JoinHandle<Result<(), UdpServerError>>,
}

/// Spawn a `udps:` listener with default config bound to `127.0.0.1:0`; the
/// task picks an OS-assigned port and reports it back via `bound_addr_tx`,
/// closing the bind-then-drop TOCTOU window the older helper had. Awaits the
/// first successful bind so the returned harness's `listen_addr` is real.
pub async fn spawn_udps(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    name: &str,
) -> UdpsHarness {
    spawn_udps_with_config(allocator, cancel, name, UdpServerConfig::default()).await
}

/// Like `spawn_udps` but lets the test override the listener's `UdpServerConfig`
/// — typically to shorten `idle_secs` for fast idle-reap coverage.
pub async fn spawn_udps_with_config(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    name: &str,
    cfg: UdpServerConfig,
) -> UdpsHarness {
    let listen_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse listen_addr");
    let mut h = spawn_udps_at_with_config(allocator, cancel, listen_addr, cfg, name);
    let rx = h.bound_addr_rx.take().expect("bound_addr_rx present");
    h.listen_addr = rx.await.expect("udps bound_addr_tx dropped");
    h
}

/// Like `spawn_udps_with_config` but binds an explicit address — used by the
/// bind-retry test to target a pre-held port. Returns immediately with
/// `bound_addr_rx` pending so the test can drive the bind-retry path before
/// awaiting the eventual bind.
pub fn spawn_udps_at_with_config(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    listen_addr: SocketAddr,
    cfg: UdpServerConfig,
    name: &str,
) -> UdpsHarness {
    spawn_udps_at_with_config_and_identity(
        allocator,
        cancel,
        listen_addr,
        cfg,
        IdentityFlags::default(),
        name,
    )
}

/// Like `spawn_udps_at_with_config` but also lets the caller install a
/// non-default `IdentityFlags` on the parent listener. Used by the
/// inheritance test to assert each learned peer receives a clone of the
/// parent's identity via `PeerAdded`.
pub fn spawn_udps_at_with_config_and_identity(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    listen_addr: SocketAddr,
    cfg: UdpServerConfig,
    identity: IdentityFlags,
    name: &str,
) -> UdpsHarness {
    let parent_id = allocator.alloc();
    let parent_name = name.to_string();
    let allocator = allocator.clone();
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(32);
    let (bound_tx, bound_rx) = oneshot::channel::<SocketAddr>();
    let task = tokio::spawn(async move {
        udp_server::run(
            UdpServerSpec {
                listen_addr,
                parent_id,
                parent_name,
                cfg,
                identity,
            },
            UdpServerWiring {
                allocator,
                frame_tx,
                event_tx,
                cancel,
                bound_addr_tx: Some(bound_tx),
            },
        )
        .await
    });
    UdpsHarness {
        listen_addr,
        frame_rx,
        event_rx,
        bound_addr_rx: Some(bound_rx),
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
    /// Join handle of the spawned task; await after cancelling.
    pub task: JoinHandle<Result<(), UdpClientError>>,
}

/// Spawn a `udpc:` client targeting `configured_addr`. The address's IP is
/// passed verbatim as the configured host (no DNS), so tests can use IPv4 or
/// IPv6 loopback interchangeably.
pub fn spawn_udpc(
    allocator: &EndpointIdAllocator,
    cancel: CancellationToken,
    configured_addr: SocketAddr,
    cfg: UdpClientConfig,
    name: &str,
) -> UdpcHarness {
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::default());
    let tx_queue = TxQueue::new(8, stats.clone());
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let task = {
        let tx_queue = tx_queue.clone();
        let name = name.to_string();
        tokio::spawn(async move {
            udp_client::run(
                UdpClientSpec {
                    host: configured_addr.ip().to_string(),
                    port: configured_addr.port(),
                    endpoint_id,
                    name,
                    cfg,
                    identity: IdentityFlags::default(),
                },
                UdpClientWiring {
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
    let (n, src) = timeout(Duration::from_secs(2), configured.recv_from(&mut buf))
        .await
        .expect("configured recv timeout")
        .expect("configured recv_from");
    assert_eq!(
        &buf[..n],
        frame,
        "udpc frame bytes mismatch at configured peer"
    );
    src
}
