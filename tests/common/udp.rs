//! Spawn helpers for `udps:` and `udpc:` integration tests. Each `spawn_*`
//! call returns a harness bundling the task handle with the channels and
//! queues the test needs to play the role of a router stub.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{
    EndpointIdAllocator,
    events::{EndpointEvent, RouterFrame},
    stats::EndpointStats,
    tx_queue::TxQueue,
    udp_client::{self, UdpClientConfig, UdpClientError, UdpClientSpec, UdpClientWiring},
    udp_server::{self, UdpServerConfig, UdpServerError, UdpServerSpec, UdpServerWiring},
};

/// Bundle of channels and the join handle for a spawned `udps:` listener task.
/// Tests destructure or borrow these fields to play the router-stub role.
pub struct UdpsHarness {
    /// Address the listener actually bound to — use as the `send_to` target.
    pub listen_addr: SocketAddr,
    /// Per-frame stream from the listener (ingress as seen by the router).
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    /// Lifecycle stream announcing learned peers and idle reaps.
    pub event_rx: mpsc::Receiver<EndpointEvent>,
    /// Join handle of the spawned task; await after cancelling.
    pub task: JoinHandle<Result<(), UdpServerError>>,
}

/// Spawn a `udps:` listener bound to an ephemeral 127.0.0.1 port with default
/// config. Returns the harness with channels and a task handle.
pub fn spawn_udps(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    name: &str,
) -> UdpsHarness {
    let port = ephemeral_localhost_port();
    let listen_addr: SocketAddr = format!("127.0.0.1:{port}")
        .parse()
        .expect("parse listen_addr");
    let parent_id = allocator.alloc();
    let parent_name = name.to_string();
    let allocator = allocator.clone();
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(32);
    let task = tokio::spawn(async move {
        udp_server::run(
            UdpServerSpec {
                listen_addr,
                parent_id,
                parent_name,
                cfg: UdpServerConfig::default(),
            },
            UdpServerWiring {
                allocator,
                frame_tx,
                event_tx,
                cancel,
            },
        )
        .await
    });
    UdpsHarness {
        listen_addr,
        frame_rx,
        event_rx,
        task,
    }
}

/// Await the next `PeerAdded` event on the given channel, panicking with a
/// descriptive message on timeout, channel close, or wrong event type.
pub async fn next_peer_added(rx: &mut mpsc::Receiver<EndpointEvent>) -> (SocketAddr, TxQueue) {
    let ev = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event_rx timeout waiting for PeerAdded")
        .expect("event_rx closed before PeerAdded");
    match ev {
        EndpointEvent::PeerAdded {
            peer_addr,
            tx_queue,
            ..
        } => (peer_addr, tx_queue),
        other => panic!("expected PeerAdded, got {other:?}"),
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
    let stats = Arc::new(EndpointStats::new());
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

/// Trigger cancellation and await every harness task with a 3s timeout each.
/// Mirrors the per-task drain budget the production tasks honour.
pub async fn shutdown_all<I, T>(cancel: &CancellationToken, tasks: I)
where
    I: IntoIterator<Item = JoinHandle<T>>,
{
    cancel.cancel();
    for task in tasks {
        let _ = timeout(Duration::from_secs(3), task).await;
    }
}

fn ephemeral_localhost_port() -> u16 {
    let s = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind probe");
    let port = s.local_addr().expect("local_addr").port();
    drop(s);
    port
}
