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
    spec::{TcpClientEndpoint, TcpServerEndpoint},
    stats::{EndpointState, EndpointStats},
    tcp::client::{self as tcp_client, TcpClientSpec},
    tcp::server::{self as tcp_server, TcpServerSpec},
    tx_queue::TxQueue,
    wiring::{ClientWiring, ServerWiring},
};

use crate::common::wait_for_state;

/// Bundle of channels and the join handle for a spawned `tcps:` listener task.
pub struct TcpsHarness {
    /// Address the listener was asked to bind to — also the `TcpStream::connect`
    /// target callers use, since `spawn_tcps*` resolves `127.0.0.1:0` to a
    /// concrete port via [`pick_free_tcp_addr`] before constructing the spec.
    pub listen_addr: SocketAddr,
    /// Per-frame stream from the listener (ingress as seen by the router).
    pub frame_rx: mpsc::Receiver<RouterFrame>,
    /// Lifecycle stream announcing accepted clients and disconnects.
    pub event_rx: mpsc::Receiver<EndpointEvent>,
    /// Shared stats handle for the parent listener — tests can assert state
    /// transitions (Reconnecting → Connected on first bind). `spawn_tcps`
    /// already awaits that transition; bind-retry tests poll it explicitly.
    pub stats: Arc<EndpointStats>,
    /// Join handle of the spawned task; await after cancelling.
    pub task: JoinHandle<()>,
}

/// Bind a `std::net::TcpListener` on `127.0.0.1:0`, capture the assigned
/// port, and drop the listener. The returned `SocketAddr` is what the test
/// passes to `tcps:`/`tcpc:`. SO_REUSEADDR is on for `tcps:` (locked
/// decision), so the brief TIME_WAIT after the drop doesn't bite; the
/// listener's `bind_with_backoff` curve handles the (microscopic) race
/// where another process grabs the port in the gap.
pub fn pick_free_tcp_addr() -> SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("probe local_addr")
}

/// Spawn a `tcps:` listener bound to a probe-picked free port and await the
/// `Reconnecting → Connected` transition so the returned harness is ready to
/// accept connections.
pub async fn spawn_tcps(
    allocator: &Arc<EndpointIdAllocator>,
    cancel: CancellationToken,
    name: &str,
) -> TcpsHarness {
    let endpoint = TcpServerEndpoint {
        bind_addr: pick_free_tcp_addr(),
        ..TcpServerEndpoint::default()
    };
    let parent_id = allocator.alloc();
    let spec = TcpServerSpec::from_endpoint(endpoint, parent_id, name.to_string());
    let harness = spawn_tcps_with_spec(allocator, cancel, spec);
    wait_for_state(&harness.stats, EndpointState::Connected, "tcps bind").await;
    harness
}

/// Spawn a `tcps:` listener with a fully-constructed `TcpServerSpec`. The
/// caller pre-allocates `parent_id` (which they place inside `spec`) and is
/// responsible for mutating any knobs that aren't reachable through the
/// parsed `TcpServerEndpoint` (e.g. the reconnect curve, which `tcps:` does
/// not expose as a query override). Returns immediately without awaiting
/// bind so bind-retry tests can drive the bind path before observing
/// `Reconnecting → Connected` on the harness's `stats`.
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
    let task = {
        let stats = stats.clone();
        tokio::spawn(async move {
            tcp_server::run(
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
    TcpsHarness {
        listen_addr,
        frame_rx,
        event_rx,
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
    spawn_tcpc_with(allocator, cancel, target_addr, endpoint, name, |_| {})
}

/// Same as [`spawn_tcpc`] but lets the caller mutate the runtime `TcpClientSpec`
/// after defaults have been applied — used by tests that need to shrink the
/// hardcoded reconnect curve to keep test runtimes tight.
pub fn spawn_tcpc_with(
    allocator: &EndpointIdAllocator,
    cancel: CancellationToken,
    target_addr: SocketAddr,
    endpoint: TcpClientEndpoint,
    name: &str,
    tune: impl FnOnce(&mut TcpClientSpec),
) -> TcpcHarness {
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
    let endpoint = TcpClientEndpoint {
        host: target_addr.ip().to_string(),
        port: target_addr.port(),
        ..endpoint
    };
    // The spawner sizes the queue from the endpoint's `tx_queue_frames`
    // override before building the spec; mirror that pattern here so the
    // harness honors the same knob.
    let tx_queue_frames = endpoint
        .common
        .tx_queue_frames
        .unwrap_or(rmr::endpoint::defaults::DEFAULT_TX_QUEUE_FRAMES)
        .max(8);
    let mut spec = TcpClientSpec::from_endpoint(endpoint, endpoint_id, name.to_string());
    tune(&mut spec);
    let tx_queue = TxQueue::new(tx_queue_frames, stats.clone());
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(32);
    let task = {
        let tx_queue = tx_queue.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            tcp_client::run(
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
            Ok(stream) => return stream,
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("connect_with_retry({addr}) gave up after {deadline:?}: {last_err:?}");
}
