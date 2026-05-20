//! `udpc:` reply-source latching: portable cases.
//!
//! Covers:
//!   - Ephemeral-port latch: a peer replying from a different source port (same
//!     IP) causes the next outbound frame to go to the latched port, not the
//!     configured one.
//!   - Idle revert: after `latch_idle_secs` of silence the latch drops and
//!     outbound returns to the configured `host:port`.
//!
//! Unrelated-source-IP rejection is covered by unit tests in `udp/client.rs`
//! (it's not portably testable on macOS/Windows where 127.0.0.0/8 isn't all
//! loopback).

use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use rmr::endpoint::{EndpointIdAllocator, spec::UdpClientEndpoint};

use crate::common;
use crate::common::shutdown_all;
use crate::common::udp::{spawn_udpc, udpc_send_and_capture_source};

#[tokio::test]
async fn udpc_latches_onto_ephemeral_reply_port() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    // The "configured peer" socket — udpc's initial destination.
    let configured = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind configured peer");
    let configured_addr = configured.local_addr().expect("local_addr");

    let mut harness = spawn_udpc(
        &allocator,
        cancel.clone(),
        configured_addr,
        UdpClientEndpoint::default(),
        "gcs",
    );

    // Push the first frame onto udpc's TxQueue — udpc sends it to the
    // configured destination. The configured socket's recv_from learns
    // udpc's source port (which we couldn't otherwise discover).
    let frame_out = common::build_v2_heartbeat(1);
    let src_of_udpc =
        udpc_send_and_capture_source(&harness.tx_queue, &configured, &frame_out).await;

    // Now act as a GCS that replied from a different ephemeral port. Bind a
    // *new* socket on 127.0.0.1:0 and send to udpc — udpc's classify_inbound
    // sees src IP == 127.0.0.1 (matches resolved), and latches onto this
    // new (ip, port).
    let ephemeral = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let ephemeral_addr = ephemeral.local_addr().expect("ephemeral local_addr");
    assert_ne!(ephemeral_addr.port(), configured_addr.port());

    let frame_in = common::build_v2_heartbeat(2);
    ephemeral
        .send_to(&frame_in, src_of_udpc)
        .await
        .expect("ephemeral send_to udpc");

    // The inbound frame should reach udpc's frame_rx.
    let routed = timeout(Duration::from_secs(2), harness.frame_rx.recv())
        .await
        .expect("frame_rx timeout")
        .expect("frame_rx closed");
    assert_eq!(&routed.frame[..], &frame_in[..]);

    // Push another outbound frame; udpc should send to the latched
    // ephemeral_addr, not the configured one.
    let frame_out2 = common::build_v2_heartbeat(3);
    harness.tx_queue.push(frame_out2.clone().into());

    let mut buf = [0u8; 256];
    let (bytes_read, src) = timeout(Duration::from_secs(2), ephemeral.recv_from(&mut buf))
        .await
        .expect("ephemeral recv timeout")
        .expect("ephemeral recv_from");
    assert_eq!(src, src_of_udpc);
    assert_eq!(&buf[..bytes_read], &frame_out2[..]);

    shutdown_all(&cancel, [harness.task]).await;
}

#[tokio::test]
async fn udpc_reverts_to_configured_after_latch_idle() {
    let allocator = Arc::new(EndpointIdAllocator::new());
    let cancel = CancellationToken::new();

    let configured = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind configured peer");
    let configured_addr = configured.local_addr().expect("local_addr");

    // Short latch_idle_secs so the test doesn't have to wait 30s.
    let endpoint = UdpClientEndpoint {
        latch_idle_secs: Some(1),
        ..UdpClientEndpoint::default()
    };

    let mut harness = spawn_udpc(&allocator, cancel.clone(), configured_addr, endpoint, "gcs");

    // Step 1: send first outbound, learn udpc's source.
    let frame0 = common::build_v2_heartbeat(0);
    let src_of_udpc = udpc_send_and_capture_source(&harness.tx_queue, &configured, &frame0).await;

    // Step 2: latch via ephemeral source.
    let ephemeral = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let frame_in = common::build_v2_heartbeat(1);
    ephemeral
        .send_to(&frame_in, src_of_udpc)
        .await
        .expect("send");
    let _ = timeout(Duration::from_secs(2), harness.frame_rx.recv())
        .await
        .expect("frame_rx timeout")
        .expect("frame_rx closed");

    // Confirm latch by sending one outbound — should reach `ephemeral`.
    let frame_latched = common::build_v2_heartbeat(2);
    harness.tx_queue.push(frame_latched.into());
    let mut buf = [0u8; 256];
    let (_bytes_read, src) = timeout(Duration::from_secs(2), ephemeral.recv_from(&mut buf))
        .await
        .expect("ephemeral recv timeout (during latch)")
        .expect("ephemeral recv_from");
    assert_eq!(src, src_of_udpc);

    // Step 3: wait past latch_idle_secs + revert_tick interval (1s + 1s buffer).
    tokio::time::sleep(Duration::from_millis(2200)).await;

    // Step 4: push another outbound. Should now go to *configured*, not
    // ephemeral. (The revert tick has fired and the latch cleared.)
    let frame_post = common::build_v2_heartbeat(3);
    harness.tx_queue.push(frame_post.clone().into());

    let (bytes_read, src) = timeout(Duration::from_secs(2), configured.recv_from(&mut buf))
        .await
        .expect("configured recv timeout (post-revert)")
        .expect("configured recv_from");
    assert_eq!(src, src_of_udpc);
    assert_eq!(&buf[..bytes_read], &frame_post[..]);

    // And `ephemeral` should NOT have received it.
    let result = timeout(Duration::from_millis(200), ephemeral.recv_from(&mut buf)).await;
    assert!(
        result.is_err(),
        "ephemeral should not receive after revert, got {result:?}"
    );

    shutdown_all(&cancel, [harness.task]).await;
}
