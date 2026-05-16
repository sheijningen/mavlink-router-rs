//! Spawn helpers for `tcps:` and `tcpc:` integration tests. Each `spawn_*`
//! call returns a harness bundling the task handle with the channels and
//! queues the test needs to play the role of a router stub.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{
    EndpointIdAllocator,
    events::{EndpointEvent, RouterFrame},
    stats::EndpointStats,
    tcp::client::{
        self as tcp_client, TcpClientConfig, TcpClientError, TcpClientSpec, TcpClientWiring,
    },
    tcp::server::{
        self as tcp_server, TcpServerConfig, TcpServerError, TcpServerSpec, TcpServerWiring,
    },
    tx_queue::TxQueue,
};

/// Bundle of channels and the join handle for a spawned `tcps:` listener task.
pub struct TcpsHarness {
    /// Address the listener actually bound to — use as the `TcpStream::connect` target.
    pub listen_addr: SocketAddr,
    /// Per-frame stream from the listener (ingress as seen by the router).
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    /// Lifecycle stream announcing accepted clients and disconnects.
    pub event_rx: mpsc::Receiver<EndpointEvent>,
    /// Join handle of the spawned task; await after cancelling.
    pub task: JoinHandle<Result<(), TcpServerError>>,
}

/// Spawn a `tcps:` listener bound to an ephemeral 127.0.0.1 port with default
/// config. Returns the harness with channels and a task handle.
pub fn spawn_tcps(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    name: &str,
) -> TcpsHarness {
    let port = ephemeral_localhost_tcp_port();
    let listen_addr: SocketAddr = format!("127.0.0.1:{port}")
        .parse()
        .expect("parse listen_addr");
    spawn_tcps_at_with_config(
        allocator,
        cancel,
        listen_addr,
        TcpServerConfig::default(),
        name,
    )
}

/// Like `spawn_tcps` but binds an explicit address and accepts a config
/// override — used by the bind-retry test to target a pre-held port.
pub fn spawn_tcps_at_with_config(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    listen_addr: SocketAddr,
    cfg: TcpServerConfig,
    name: &str,
) -> TcpsHarness {
    let parent_id = allocator.alloc();
    let parent_name = name.to_string();
    let allocator = allocator.clone();
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(32);
    let task = tokio::spawn(async move {
        tcp_server::run(
            TcpServerSpec {
                listen_addr,
                parent_id,
                parent_name,
                cfg,
            },
            TcpServerWiring {
                allocator,
                frame_tx,
                event_tx,
                cancel,
            },
        )
        .await
    });
    TcpsHarness {
        listen_addr,
        frame_rx,
        event_rx,
        task,
    }
}

/// Bundle of channels and the join handle for a spawned `tcpc:` client task.
pub struct TcpcHarness {
    /// Per-frame stream from the client (ingress as seen by the router).
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    /// Queue the test pushes onto to schedule egress frames.
    pub tx_queue: TxQueue,
    /// Shared stats the test can inspect (e.g. `dropped_tx`).
    pub stats: Arc<EndpointStats>,
    /// Join handle of the spawned task; await after cancelling.
    pub task: JoinHandle<Result<(), TcpClientError>>,
}

/// Spawn a `tcpc:` client targeting `target_addr`. The address's IP is passed
/// verbatim as the configured host (no DNS), so tests can use IPv4 or IPv6
/// loopback interchangeably.
pub fn spawn_tcpc(
    allocator: &EndpointIdAllocator,
    cancel: CancellationToken,
    target_addr: SocketAddr,
    cfg: TcpClientConfig,
    name: &str,
) -> TcpcHarness {
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::new());
    let tx_queue = TxQueue::new(cfg.tx_queue_frames.max(8), stats.clone());
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let task = {
        let tx_queue = tx_queue.clone();
        let stats = stats.clone();
        let name = name.to_string();
        tokio::spawn(async move {
            tcp_client::run(
                TcpClientSpec {
                    host: target_addr.ip().to_string(),
                    port: target_addr.port(),
                    endpoint_id,
                    name,
                    cfg,
                },
                TcpClientWiring {
                    frame_tx,
                    tx_queue,
                    stats,
                    cancel,
                },
            )
            .await
        })
    };
    TcpcHarness {
        frame_rx,
        tx_queue,
        stats,
        task,
    }
}

/// Ask the OS for a free TCP port on 127.0.0.1 by binding then dropping.
/// There is a slight race between dropping the probe and re-binding, but
/// 127.0.0.1 ephemerals on a quiet test host almost never collide.
pub fn ephemeral_localhost_tcp_port() -> u16 {
    let s = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = s.local_addr().expect("local_addr").port();
    drop(s);
    port
}

/// `TcpStream::connect` with a short retry loop. Production `tcpc:` has its
/// own backoff; this helper exists for tests, which routinely connect within
/// a millisecond of spawning the `tcps:` listener task and must tolerate the
/// brief window before the task's bind completes.
pub async fn connect_with_retry(addr: SocketAddr, deadline: Duration) -> TcpStream {
    let start = Instant::now();
    let mut last_err: Option<std::io::Error> = None;
    while start.elapsed() < deadline {
        match TcpStream::connect(addr).await {
            Ok(s) => return s,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("connect_with_retry({addr}) gave up after {deadline:?}: {last_err:?}");
}
