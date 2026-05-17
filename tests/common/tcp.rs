//! Spawn helpers for `tcps:` and `tcpc:` integration tests. Each `spawn_*`
//! call returns a harness bundling the task handle with the channels and
//! queues the test needs to play the role of a router stub.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{
    EndpointIdAllocator,
    events::{EndpointEvent, RouterFrame},
    spec::{TcpClientEndpoint, TcpServerEndpoint},
    stats::{EndpointState, EndpointStats},
    tcp::client::{self as tcp_client, TcpClientSpec, TcpClientWiring},
    tcp::server::{self as tcp_server, TcpServerSpec, TcpServerWiring},
    tx_queue::TxQueue,
};

/// Bundle of channels and the join handle for a spawned `tcps:` listener task.
pub struct TcpsHarness {
    /// Address the listener actually bound to — use as the `TcpStream::connect`
    /// target. For tests using `spawn_tcps` this is filled in once the OS has
    /// assigned a port; bind-retry tests that drive the spawn manually rely
    /// on `bound_addr_rx` instead.
    pub listen_addr: SocketAddr,
    /// Per-frame stream from the listener (ingress as seen by the router).
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    /// Lifecycle stream announcing accepted clients and disconnects.
    pub event_rx: mpsc::Receiver<EndpointEvent>,
    /// Resolves on the first successful bind. Already consumed by `spawn_tcps`;
    /// `spawn_tcps_with_spec` leaves it for the caller (bind-retry tests).
    pub bound_addr_rx: Option<oneshot::Receiver<SocketAddr>>,
    /// Shared stats handle for the parent listener — tests can assert state
    /// transitions (Reconnecting → Connected on first bind).
    pub stats: Arc<EndpointStats>,
    /// Join handle of the spawned task; await after cancelling.
    pub task: JoinHandle<()>,
}

/// Spawn a `tcps:` listener with default config bound to `127.0.0.1:0`; the
/// task picks an OS-assigned port and reports it back via `bound_addr_tx`,
/// closing the bind-then-drop TOCTOU window the older helper had. Awaits the
/// first successful bind so the returned harness's `listen_addr` is the real
/// bound address.
pub async fn spawn_tcps(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    name: &str,
) -> TcpsHarness {
    let endpoint = TcpServerEndpoint {
        bind_addr: "127.0.0.1:0".parse().expect("parse listen_addr"),
        ..TcpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let spec = TcpServerSpec::from_endpoint(endpoint, parent_id, name.to_string());
    let mut h = spawn_tcps_with_spec(allocator, cancel, spec);
    let rx = h.bound_addr_rx.take().expect("bound_addr_rx present");
    h.listen_addr = rx.await.expect("tcps bound_addr_tx dropped");
    h
}

/// Spawn a `tcps:` listener with a fully-constructed `TcpServerSpec`. The
/// caller pre-allocates `parent_id` (which they place inside `spec`) and is
/// responsible for mutating any knobs that aren't reachable through the
/// parsed `TcpServerEndpoint` (e.g. the reconnect curve, which `tcps:` does
/// not expose as a query override). Returns immediately with `bound_addr_rx`
/// pending so bind-retry tests can drive the bind path before awaiting the
/// eventual bind.
pub fn spawn_tcps_with_spec(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    spec: TcpServerSpec,
) -> TcpsHarness {
    let listen_addr = spec.listen_addr;
    let allocator = allocator.clone();
    let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(32);
    let (bound_tx, bound_rx) = oneshot::channel::<SocketAddr>();
    let task = {
        let stats = stats.clone();
        tokio::spawn(async move {
            tcp_server::run(
                spec,
                TcpServerWiring {
                    allocator,
                    frame_tx,
                    event_tx,
                    cancel,
                    bound_addr_tx: Some(bound_tx),
                    stats,
                },
            )
            .await
        })
    };
    TcpsHarness {
        listen_addr,
        frame_rx,
        event_rx,
        bound_addr_rx: Some(bound_rx),
        stats,
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
    pub task: JoinHandle<()>,
}

/// Spawn a `tcpc:` client targeting `target_addr`, configured by a parsed
/// `TcpClientEndpoint` (the helper bolts the host/port from `target_addr`
/// on top so the test can pass `TcpClientEndpoint::default()` and reach IPv4
/// or IPv6 loopback interchangeably).
pub fn spawn_tcpc(
    allocator: &EndpointIdAllocator,
    cancel: CancellationToken,
    target_addr: SocketAddr,
    endpoint: TcpClientEndpoint,
    name: &str,
) -> TcpcHarness {
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
    let endpoint = TcpClientEndpoint {
        host: target_addr.ip().to_string(),
        port: target_addr.port(),
        ..endpoint
    };
    let spec = TcpClientSpec::from_endpoint(endpoint, endpoint_id, name.to_string());
    let tx_queue = TxQueue::new(spec.tx_queue_frames.max(8), stats.clone());
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let task = {
        let tx_queue = tx_queue.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            tcp_client::run(
                spec,
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
