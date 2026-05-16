use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info_span, warn};

use super::EndpointId;
use super::events::RouterFrame;
use super::spec::{SerialEndpoint, SerialFlowControl};
use super::stats::EndpointStats;
use super::tx_queue::TxQueue;
use crate::mavlink::framer::Framer;

const DEFAULT_SERIAL_REOPEN_MS: u64 = 1000;
const DEFAULT_READ_BUF_BYTES: usize = 8192;
const DEFAULT_TX_QUEUE_FRAMES: usize = 256;

/// Per-endpoint runtime configuration. The spec parser hands us a fully-typed
/// `SerialEndpoint`; this struct collapses the optional knobs down to the
/// concrete values the task actually uses, substituting CLAUDE.md defaults
/// where the user left a knob unset.
#[derive(Debug, Clone, Copy)]
pub struct SerialConfig {
    pub serial_reopen_ms: u64,
    pub read_buf_bytes: usize,
    pub tx_queue_frames: usize,
}

impl Default for SerialConfig {
    fn default() -> Self {
        Self {
            serial_reopen_ms: DEFAULT_SERIAL_REOPEN_MS,
            read_buf_bytes: DEFAULT_READ_BUF_BYTES,
            tx_queue_frames: DEFAULT_TX_QUEUE_FRAMES,
        }
    }
}

impl SerialConfig {
    pub fn from_endpoint(ep: &SerialEndpoint) -> Self {
        Self {
            serial_reopen_ms: ep.serial_reopen_ms.unwrap_or(DEFAULT_SERIAL_REOPEN_MS),
            read_buf_bytes: ep.common.read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES),
            tx_queue_frames: ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
        }
    }
}

/// Typed-empty return for `serial:` `run()`. Open failures and read/write
/// errors are non-terminal — Step 2 ends the session and returns `Ok(())`;
/// Step 3 will wrap the open + session in the hot-replug retry loop. Kept as
/// a typed return for symmetry with the other endpoint modules in case a
/// fatal case shows up later.
#[derive(Debug, Error)]
pub enum SerialError {}

/// Inputs that distinguish one `serial:` endpoint from another: which device
/// to open at what baud (with optional hardware flow control), what to call
/// it, and the per-endpoint knobs from the query string.
pub struct SerialSpec {
    pub path: String,
    pub baud: u32,
    pub flow_control: SerialFlowControl,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub cfg: SerialConfig,
}

/// Shared wiring a `serial:` task needs. Mirrors `TcpClientWiring` /
/// `UdpClientWiring` — the TxQueue and stats are constructed by the spawner so
/// the router can hold its own clones before this task starts running. There
/// is no `event_tx` (serial has no children) and no `bound_addr_tx` (no socket
/// to bind).
pub struct SerialWiring {
    pub frame_tx: mpsc::Sender<RouterFrame>,
    pub tx_queue: TxQueue,
    pub stats: Arc<EndpointStats>,
    pub cancel: CancellationToken,
}

/// Why a serial session terminated — controls whether the caller re-opens
/// (Disconnected) or unwinds toward shutdown (Cancelled / RouterGone). The
/// `Cancelled` and `RouterGone` variants are handled identically by callers
/// but kept distinct so tracing/logs can tell "the cancel token fired" apart
/// from "the router task exited and dropped the frame channel" during
/// debugging. Mirrors `tcp::session::SessionOutcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOutcome {
    Cancelled,
    RouterGone,
    Disconnected,
}

/// Run a `serial:` endpoint until the cancellation token fires.
///
/// Phase 4 step 2: opens the configured device once, runs the read/write
/// session until cancellation, EOF, or I/O error. Re-open recovery
/// (hot-replug) lands in step 3.
pub async fn run(spec: SerialSpec, wiring: SerialWiring) -> Result<(), SerialError> {
    let span = info_span!("serial", name = %spec.name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: SerialSpec, wiring: SerialWiring) -> Result<(), SerialError> {
    let SerialSpec {
        path,
        baud,
        flow_control,
        endpoint_id,
        name: _,
        cfg,
    } = spec;
    let SerialWiring {
        frame_tx,
        tx_queue,
        stats,
        cancel,
    } = wiring;

    let stream = match try_open(&path, baud, flow_control) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, %path, baud, "serial open failed");
            // Step 2: no retry — wait for cancellation. Step 3 wraps this in
            // a re-open loop driven by `serial_reopen_ms`.
            cancel.cancelled().await;
            tx_queue.drain_and_discard();
            return Ok(());
        }
    };

    let outcome = run_session(
        stream,
        endpoint_id,
        &stats,
        &frame_tx,
        &tx_queue,
        &cancel,
        cfg.read_buf_bytes,
    )
    .await;
    match outcome {
        SessionOutcome::Cancelled | SessionOutcome::RouterGone => {}
        SessionOutcome::Disconnected => {
            // Step 3 will swap this terminal-on-disconnect behaviour for the
            // hot-replug poll loop; for Step 2 we end the run cleanly.
            warn!(%path, "serial disconnected; exiting (step 3 adds hot-replug retry)");
        }
    }
    tx_queue.drain_and_discard();
    Ok(())
}

/// Open the serial device with the requested baud + flow-control. The other
/// line settings (8 data bits, no parity, 1 stop bit) are the `tokio_serial`
/// defaults and match the MAVLink-on-UART convention used by every supported
/// autopilot.
fn try_open(
    path: &str,
    baud: u32,
    flow_control: SerialFlowControl,
) -> Result<SerialStream, tokio_serial::Error> {
    let fc = match flow_control {
        SerialFlowControl::None => tokio_serial::FlowControl::None,
        SerialFlowControl::RtsCts => tokio_serial::FlowControl::Hardware,
    };
    tokio_serial::new(path, baud)
        .flow_control(fc)
        .open_native_async()
}

/// Read inbound bytes through a fresh `Framer` and write outbound frames from
/// the TxQueue until cancellation, EOF, or I/O error. Shape mirrors
/// `tcp::session::run_session` so behaviour stays consistent across transports
/// — same biased select, same per-frame stats accounting, same framer-counter
/// sync after each read burst.
pub async fn run_session(
    stream: SerialStream,
    endpoint_id: EndpointId,
    stats: &Arc<EndpointStats>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    tx_queue: &TxQueue,
    cancel: &CancellationToken,
    read_buf_bytes: usize,
) -> SessionOutcome {
    let (mut rh, mut wh) = tokio::io::split(stream);
    let mut framer = Framer::with_capacity(read_buf_bytes);
    let mut last_resync_total: u64 = 0;
    let mut last_crc_total: u64 = 0;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return SessionOutcome::Cancelled,
            res = rh.read_buf(framer.buffer_mut()) => {
                match res {
                    Ok(0) => {
                        debug!("serial read returned EOF (device closed)");
                        return SessionOutcome::Disconnected;
                    }
                    Ok(_) => {
                        while let Some((header, frame)) = framer.try_next_frame() {
                            let frame_len = frame.len();
                            stats.add_rx_frame(frame_len);
                            if frame_tx
                                .send(RouterFrame {
                                    endpoint_id,
                                    frame,
                                    header,
                                })
                                .await
                                .is_err()
                            {
                                debug!("serial router channel closed; ending session");
                                return SessionOutcome::RouterGone;
                            }
                        }
                        sync_framer_counters(
                            &framer,
                            &mut last_resync_total,
                            &mut last_crc_total,
                            stats,
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, "serial read failed");
                        return SessionOutcome::Disconnected;
                    }
                }
            }
            frame = pop_or_wait(tx_queue) => {
                if let Err(e) = wh.write_all(&frame).await {
                    warn!(error = %e, "serial write failed");
                    return SessionOutcome::Disconnected;
                }
                stats.add_tx_frame(frame.len());
            }
        }
    }
}

async fn pop_or_wait(q: &TxQueue) -> Bytes {
    loop {
        if let Some(b) = q.pop() {
            return b;
        }
        q.wait_for_push().await;
    }
}

fn sync_framer_counters(
    framer: &Framer,
    last_resync_total: &mut u64,
    last_crc_total: &mut u64,
    stats: &Arc<EndpointStats>,
) {
    let now_resync = framer.resync_bytes();
    let now_crc = framer.crc_errors();
    let resync_delta = now_resync.saturating_sub(*last_resync_total);
    let crc_delta = now_crc.saturating_sub(*last_crc_total);
    if resync_delta > 0 {
        stats
            .resync_bytes
            .fetch_add(resync_delta, Ordering::Relaxed);
    }
    if crc_delta > 0 {
        stats.crc_errors.fetch_add(crc_delta, Ordering::Relaxed);
    }
    *last_resync_total = now_resync;
    *last_crc_total = now_crc;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use crate::endpoint::spec::{CommonQuery, SerialEndpoint};
    use std::time::Duration;
    use tokio::time::timeout;

    #[test]
    fn config_defaults_when_endpoint_unset() {
        let ep = SerialEndpoint::default();
        let cfg = SerialConfig::from_endpoint(&ep);
        assert_eq!(cfg.serial_reopen_ms, DEFAULT_SERIAL_REOPEN_MS);
        assert_eq!(cfg.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(cfg.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
    }

    #[test]
    fn config_overrides_from_endpoint() {
        let ep = SerialEndpoint {
            serial_reopen_ms: Some(250),
            common: CommonQuery {
                read_buf_bytes: Some(1024),
                tx_queue_frames: Some(16),
                ..CommonQuery::default()
            },
            ..SerialEndpoint::default()
        };
        let cfg = SerialConfig::from_endpoint(&ep);
        assert_eq!(cfg.serial_reopen_ms, 250);
        assert_eq!(cfg.read_buf_bytes, 1024);
        assert_eq!(cfg.tx_queue_frames, 16);
    }

    #[tokio::test]
    async fn run_returns_when_cancelled_with_unopenable_path() {
        // /dev/null is not a tty so the open call fails immediately; the
        // task then waits on the cancel token. This proves the open-failure
        // arm honours cancellation cleanly.
        let allocator = EndpointIdAllocator::new();
        let endpoint_id = allocator.alloc();
        let stats = Arc::new(EndpointStats::new());
        let tx_queue = TxQueue::new(8, stats.clone());
        let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);
        let cancel = CancellationToken::new();

        let spec = SerialSpec {
            path: "/dev/null".to_string(),
            baud: 115200,
            flow_control: SerialFlowControl::None,
            endpoint_id,
            name: "test-serial".to_string(),
            cfg: SerialConfig::default(),
        };
        let wiring = SerialWiring {
            frame_tx,
            tx_queue: tx_queue.clone(),
            stats,
            cancel: cancel.clone(),
        };

        let handle = tokio::spawn(async move { run(spec, wiring).await });
        cancel.cancel();
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("serial run did not return after cancel")
            .expect("join")
            .expect("run result");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pty_pair_round_trips_frame() {
        // Use a Unix pty pair to drive both halves of the session without
        // needing real hardware: rmr's `run_session` reads/writes the master,
        // the test plays the role of the device on the slave end.
        use crate::endpoint::events::RouterFrame;
        use tokio::io::AsyncWriteExt;

        let (master, mut slave) = tokio_serial::SerialStream::pair().expect("pty pair");

        let allocator = EndpointIdAllocator::new();
        let endpoint_id = allocator.alloc();
        let stats = Arc::new(EndpointStats::new());
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
                    4096,
                )
                .await
            })
        };

        // Build a real v2 HEARTBEAT, write it from slave→master; rmr should
        // emit one RouterFrame on frame_rx with identical bytes.
        let frame = build_test_v2_heartbeat();
        slave.write_all(&frame).await.expect("slave write");
        let rf = timeout(Duration::from_secs(2), frame_rx.recv())
            .await
            .expect("frame_rx timeout")
            .expect("frame_rx closed");
        assert_eq!(rf.endpoint_id, endpoint_id);
        assert_eq!(&rf.frame[..], &frame[..]);
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 1);
        assert_eq!(stats.rx_bytes.load(Ordering::Relaxed), frame.len() as u64);

        // Symmetric: push a frame onto the TxQueue; the slave end reads it.
        tx_queue.push(Bytes::from(frame.clone()));
        let mut buf = vec![0u8; frame.len()];
        let mut got = 0;
        while got < frame.len() {
            let n = timeout(
                Duration::from_secs(2),
                tokio::io::AsyncReadExt::read(&mut slave, &mut buf[got..]),
            )
            .await
            .expect("slave read timeout")
            .expect("slave read");
            assert!(n > 0, "EOF before full frame arrived");
            got += n;
        }
        assert_eq!(buf, frame);
        assert_eq!(stats.tx_frames.load(Ordering::Relaxed), 1);

        cancel.cancel();
        let outcome = timeout(Duration::from_secs(2), session)
            .await
            .expect("session join timeout")
            .expect("session join");
        assert_eq!(outcome, SessionOutcome::Cancelled);
    }

    #[cfg(unix)]
    fn build_test_v2_heartbeat() -> Vec<u8> {
        // Hand-rolled v2 HEARTBEAT (msgid=0, payload 9 bytes), CRC computed
        // against crc_extra=50 — keeps this test independent of the test/
        // common fixtures, since those live in a separate test crate.
        use crate::mavlink::crc::Crc16;
        let payload: [u8; 9] = [0, 0, 0, 0, 2, 3, 0, 0, 3];
        let mut frame = Vec::with_capacity(12 + payload.len() + 2);
        frame.push(0xFD); // STX v2
        frame.push(payload.len() as u8); // len
        frame.push(0); // incompat_flags
        frame.push(0); // compat_flags
        frame.push(0); // seq
        frame.push(1); // sysid
        frame.push(1); // compid
        frame.push(0); // msgid LSB
        frame.push(0); // msgid mid
        frame.push(0); // msgid MSB
        frame.extend_from_slice(&payload);
        let mut crc = Crc16::new();
        crc.update_slice(&frame[1..]);
        crc.update(50); // HEARTBEAT crc_extra
        let crc = crc.finalize();
        frame.push(crc as u8);
        frame.push((crc >> 8) as u8);
        frame
    }
}
