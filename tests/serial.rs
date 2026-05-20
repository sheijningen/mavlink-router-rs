//! `serial:` endpoint tests over a Unix pty pair, gated on `#![cfg(unix)]`
//! because `tokio_serial::SerialStream::pair()` is Unix-only.

#![cfg(unix)]

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use rmr::endpoint::EndpointIdAllocator;
use rmr::endpoint::events::RouterFrame;
use rmr::endpoint::filters::Filters;
use rmr::endpoint::identity_flags::IdentityFlags;
use rmr::endpoint::serial::{SerialSpec, SerialWiring, run};
use rmr::endpoint::session::{SessionOutcome, run_session};
use rmr::endpoint::spec::SerialFlowControl;
use rmr::endpoint::stats::{EndpointState, EndpointStats};
use rmr::endpoint::tx_queue::TxQueue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_serial::SerialStream;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn run_returns_when_cancelled_while_open_retrying() {
    // /dev/null is not a tty so open() fails immediately and the hot-replug
    // loop retries forever — exercises cancel-during-reopen-sleep.
    let allocator = EndpointIdAllocator::new();
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::new(EndpointState::Reconnecting));
    let tx_queue = TxQueue::new(8, stats.clone());
    let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);
    let cancel = CancellationToken::new();

    let spec = SerialSpec {
        path: "/dev/null".to_string(),
        baud: 115200,
        flow_control: SerialFlowControl::None,
        endpoint_id,
        name: "test-serial".to_string(),
        identity: IdentityFlags::default(),
    };
    let wiring = SerialWiring {
        frame_tx,
        tx_queue: tx_queue.clone(),
        stats: stats.clone(),
        cancel: cancel.clone(),
    };

    let handle = tokio::spawn(async move { run(spec, wiring).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(stats.load_state(), EndpointState::Reconnecting);
    cancel.cancel();
    timeout(Duration::from_secs(2), handle)
        .await
        .expect("serial run did not return after cancel")
        .expect("join");
}

#[tokio::test]
async fn pty_pair_round_trips_frame() {
    let (master, mut slave) = SerialStream::pair().expect("pty pair");

    let allocator = EndpointIdAllocator::new();
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::default());
    let tx_queue = TxQueue::new(8, stats.clone());
    let (frame_tx, mut frame_rx) = mpsc::channel::<RouterFrame>(8);
    let cancel = CancellationToken::new();

    let session = {
        let stats = stats.clone();
        let tx_queue = tx_queue.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            run_session(
                master,
                endpoint_id,
                &stats,
                &frame_tx,
                &tx_queue,
                &cancel,
                &Filters::default(),
            )
            .await
        })
    };

    let frame = common::build_v2_heartbeat(0);
    slave.write_all(&frame).await.expect("slave write");
    let router_frame = timeout(Duration::from_secs(2), frame_rx.recv())
        .await
        .expect("frame_rx timeout")
        .expect("frame_rx closed");
    assert_eq!(router_frame.endpoint_id, endpoint_id);
    assert_eq!(&router_frame.frame[..], &frame[..]);
    assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 1);
    assert_eq!(stats.rx_bytes.load(Ordering::Relaxed), frame.len() as u64);

    tx_queue.push(Bytes::from(frame.clone()));
    let mut buf = vec![0u8; frame.len()];
    let mut got = 0;
    while got < frame.len() {
        let bytes_read = timeout(Duration::from_secs(2), slave.read(&mut buf[got..]))
            .await
            .expect("slave read timeout")
            .expect("slave read");
        assert!(bytes_read > 0, "EOF before full frame arrived");
        got += bytes_read;
    }
    assert_eq!(buf, frame);
    assert_eq!(stats.tx_frames.load(Ordering::Relaxed), 1);

    cancel.cancel();
    let outcome = timeout(Duration::from_secs(2), session)
        .await
        .expect("session join timeout")
        .expect("session join");
    assert_eq!(outcome, SessionOutcome::Terminated);
}

#[tokio::test]
async fn session_surfaces_disconnected_on_slave_drop() {
    // Slave drop → master EOF → SessionOutcome::Disconnected is the trigger
    // run()'s outer re-open loop relies on.
    let (master, slave) = SerialStream::pair().expect("pty pair");

    let allocator = EndpointIdAllocator::new();
    let endpoint_id = allocator.alloc();
    let stats = Arc::new(EndpointStats::default());
    let tx_queue = TxQueue::new(8, stats.clone());
    let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);
    let cancel = CancellationToken::new();

    let session = {
        let stats = stats.clone();
        let tx_queue = tx_queue.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            run_session(
                master,
                endpoint_id,
                &stats,
                &frame_tx,
                &tx_queue,
                &cancel,
                &Filters::default(),
            )
            .await
        })
    };
    drop(slave);
    let outcome = timeout(Duration::from_secs(2), session)
        .await
        .expect("session join timeout")
        .expect("session join");
    assert_eq!(outcome, SessionOutcome::Disconnected);
}
