//! Integration coverage for the router writing `EndpointState::Down` on the
//! shutdown sweep with **live** endpoint/listener tasks in the loop.
//!
//! Unit tests in `src/router/mod.rs` exercise `shutdown_sweep` with fake
//! registry entries. These tests pin the system-level invariant: with a
//! real task running alongside the router and writing `Connected` on bind,
//! the final value observed in the shared `Arc<EndpointStats>` after
//! `cancel` + join is `Down` (the router's shutdown write), not a
//! lingering `Connected` (the task's last write before observing cancel).
//! That's the CLAUDE.md split-authority rule: "once an endpoint task
//! observes the cancellation token, it must not write `state` again".
//!
//! Two cases cover both sides of the unified registry: leaf top-level
//! endpoints (`EndpointAdded` with `routable = Some(_)`) and parent
//! listeners (`EndpointAdded` with `routable = None`).

#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::events::{EndpointEvent, Routable, RouterFrame};
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::spec::{TcpServerEndpoint, UdpClientEndpoint};
use rmr::endpoint::stats::{EndpointState, EndpointStats};
use rmr::endpoint::tcp::server::{self as tcp_server, TcpServerSpec, TcpServerWiring};
use rmr::endpoint::tx_queue::TxQueue;
use rmr::endpoint::udp::client::{self as udp_client, UdpClientSpec, UdpClientWiring};
use rmr::endpoint::{EndpointId, EndpointIdAllocator};
use rmr::router::{self, RouterWiring};
use rmr::stats::{self as stats_task, StatsEvent};

use crate::common::{shutdown_all, wait_for_state};

struct RouterHarness {
    cancel: CancellationToken,
    frame_tx: mpsc::Sender<RouterFrame>,
    event_tx: mpsc::Sender<EndpointEvent>,
    router_task: tokio::task::JoinHandle<()>,
    stats_task: tokio::task::JoinHandle<()>,
}

fn spawn_router() -> RouterHarness {
    let cancel = CancellationToken::new();
    let (frame_tx, frame_rx) = mpsc::channel::<RouterFrame>(64);
    let (event_tx, event_rx) = mpsc::channel::<EndpointEvent>(16);
    let (stats_event_tx, stats_event_rx) = mpsc::channel::<StatsEvent>(16);

    let router_task = {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            router::run(RouterWiring {
                frame_rx,
                event_rx,
                stats_event_tx,
                cancel,
                dedup_ms: 0,
                dedup_window_capacity: 16,
            })
            .await
        })
    };
    let stats_task = {
        let cancel = cancel.clone();
        tokio::spawn(async move { stats_task::run(stats_event_rx, cancel).await })
    };

    RouterHarness {
        cancel,
        frame_tx,
        event_tx,
        router_task,
        stats_task,
    }
}

#[tokio::test]
async fn router_writes_down_on_cancel_for_leaf_top_level_endpoint() {
    // A live target so udpc's `send_to` has somewhere to go; we don't drain
    // it. The only invariant under test is the final state value.
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr: SocketAddr = target.local_addr().expect("target local_addr");

    let h = spawn_router();
    let allocator = Arc::new(EndpointIdAllocator::new());
    let endpoint_id: EndpointId = allocator.alloc();
    let endpoint_stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
    let tx_queue = TxQueue::new(8, endpoint_stats.clone());
    let identity = IdentityFlags::default();
    let endpoint = UdpClientEndpoint {
        host: target_addr.ip().to_string(),
        port: target_addr.port(),
        ..UdpClientEndpoint::default()
    };
    let spec = UdpClientSpec::from_endpoint(endpoint, endpoint_id, "uc".to_string());

    // Announce BEFORE spawning the endpoint task — mirrors the production
    // spawner ordering enforced by the biased select on `event_rx`.
    h.event_tx
        .send(EndpointEvent::EndpointAdded {
            id: endpoint_id,
            name: "uc".to_string(),
            stats: endpoint_stats.clone(),
            routable: Some(Routable {
                tx_queue: tx_queue.clone(),
                identity: identity.clone(),
            }),
        })
        .await
        .expect("EndpointAdded send");

    let endpoint_handle = {
        let cancel = h.cancel.clone();
        let stats = endpoint_stats.clone();
        let frame_tx = h.frame_tx.clone();
        let tx_queue = tx_queue.clone();
        tokio::spawn(async move {
            udp_client::run(
                spec,
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

    // Confirms the endpoint task wrote `Connected` — only then can the
    // assertion below distinguish "router wrote Down" from "state never
    // changed since spawn".
    wait_for_state(&endpoint_stats, EndpointState::Connected, "after udpc bind").await;

    shutdown_all(&h.cancel, [endpoint_handle, h.router_task, h.stats_task]).await;

    assert_eq!(
        endpoint_stats.load_state(),
        EndpointState::Down,
        "router must have written Down on the shutdown sweep; anything else \
         means the endpoint task wrote state after observing cancel \
         (split-authority violation) or the router skipped the sweep"
    );
}

#[tokio::test]
async fn router_writes_down_on_cancel_for_parent_listener() {
    let probe = TcpListener::bind("127.0.0.1:0").await.expect("probe bind");
    let listen_addr: SocketAddr = probe.local_addr().expect("probe local_addr");
    drop(probe);

    let h = spawn_router();
    let allocator = Arc::new(EndpointIdAllocator::new());
    let parent_id: EndpointId = allocator.alloc();
    let parent_stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));

    let ep = TcpServerEndpoint {
        bind_addr: listen_addr,
        ..TcpServerEndpoint::default()
    };
    let spec = TcpServerSpec::from_endpoint(ep, parent_id, "ts".to_string());

    h.event_tx
        .send(EndpointEvent::EndpointAdded {
            id: parent_id,
            name: "ts".to_string(),
            stats: parent_stats.clone(),
            routable: None,
        })
        .await
        .expect("EndpointAdded (parent listener) send");

    // The listener forwards any PeerAdded/PeerRemoved straight to the router
    // — no client connects in this test so the channel stays empty, but
    // wiring it correctly keeps the topology faithful to production.
    let listener_handle = {
        let cancel = h.cancel.clone();
        let stats = parent_stats.clone();
        let frame_tx = h.frame_tx.clone();
        let event_tx = h.event_tx.clone();
        tokio::spawn(async move {
            tcp_server::run(
                spec,
                TcpServerWiring {
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

    wait_for_state(&parent_stats, EndpointState::Connected, "after tcps bind").await;

    shutdown_all(&h.cancel, [listener_handle, h.router_task, h.stats_task]).await;

    assert_eq!(
        parent_stats.load_state(),
        EndpointState::Down,
        "router must have written Down on the shutdown sweep for the parent \
         listener; anything else means the listener task wrote state after \
         observing cancel or the router skipped the shutdown sweep"
    );
}
