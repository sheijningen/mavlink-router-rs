//! `serial:` endpoint.
//!
//! # Manual hardware test plan
//!
//! Phase 4 ships unit + pty-pair coverage that runs in CI, but the
//! USB-serial replug story and the hardware flow-control path cannot be
//! exercised without real hardware (CLAUDE.md "Serial framer ... where
//! available; hot-replug behaviour (manual test plan documented in repo if
//! hardware-only)"). Re-run the procedures below on the integrator's bench
//! whenever this module's open path, session loop, or `tokio-serial` pin
//! changes.
//!
//! ## 1. Loopback round-trip (USB-serial dongle with TX↔RX jumpered)
//! 1. Loop pin 2 ↔ pin 3 on a USB-serial dongle (a single jumper or a
//!    loopback plug). Note the device path (Linux: `/dev/ttyUSB0` or a
//!    `by-id/` symlink; Windows: `COM3`).
//! 2. Start rmr: `rmr serial:/dev/ttyUSB0:115200#loop --stats`
//! 3. Inject a HEARTBEAT into the device with any MAVLink-aware tool (the
//!    frame echoes back through the jumper).
//! 4. Expect the `loop` stats line to show `rx_frames` and `tx_frames`
//!    both incrementing 1:1; `crc_errors` and `resync_bytes` stay at 0.
//!
//! ## 2. Hot-replug recovery (USB-serial unplug → replug)
//! 1. Plug in a USB-serial dongle, start `rmr serial:<path>:115200`.
//! 2. Physically unplug the dongle.
//! 3. Expect either a `serial read failed` WARN or a `serial read returned
//!    EOF (device closed)` DEBUG (the specific signal depends on the
//!    driver), followed by repeated `serial open failed; retrying after
//!    serial_reopen_ms` WARNs at the configured cadence (default 1s).
//! 4. Replug the dongle. Expect the WARNs to stop and a TRACE
//!    `serial opened` line; counters resume on the next inbound frame. The
//!    TxQueue's `dropped_tx` reflects any frames buffered during the outage.
//!
//! ## 3. Hardware flow-control sanity (RTS/CTS over a full-handshake cable)
//! 1. Wire two dongles with a 7-wire cable (TX↔RX, RX↔TX, RTS↔CTS, CTS↔RTS,
//!    GND↔GND).
//! 2. Start two rmr instances, each `?flow_control=rtscts`.
//! 3. Saturate one side with frames while pausing the other (`kill -STOP`).
//! 4. Expect the sender's writes to block (paused side deasserts RTS, sender's
//!    CTS halts the write) without spinning or erroring; once the TxQueue
//!    fills, `dropped_tx` increments. Resume the paused side; counters
//!    drain.

use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info_span, trace, warn};

use super::EndpointId;
use super::events::RouterFrame;
use super::identity_flags::IdentityFlags;
use super::spec::{SerialEndpoint, SerialFlowControl};
use super::stats::{EndpointStats, FramerCounters};
use super::tx_queue::TxQueue;
use super::wait_or_cancel;
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

/// Typed-empty return for `serial:` `run()`. Open failures and disconnects
/// are non-terminal — they trip the hot-replug poll loop, so the task never
/// surfaces a fatal error to the spawner in v1. Kept as a typed return for
/// symmetry with the other endpoint modules in case a fatal case shows up
/// later.
#[derive(Debug, Error)]
pub enum SerialError {}

/// Inputs that distinguish one `serial:` endpoint from another: which device
/// to open at what baud (with optional hardware flow control), what to call
/// it, and the per-endpoint knobs from the query string. `identity` carries
/// the filter / sniffer / group / capacity bundle — unused today, threaded
/// here so Phase 5 readers and the router can consume it without a spawner
/// rework (CLAUDE.md "Filters, group, sniffer, and learn/seq capacities
/// travel with the `*Spec`").
pub struct SerialSpec {
    pub path: String,
    pub baud: u32,
    pub flow_control: SerialFlowControl,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub cfg: SerialConfig,
    pub identity: IdentityFlags,
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
/// On startup, opens the configured device at `baud` with the requested
/// flow-control. On open failure or mid-stream disconnect, polls every
/// `serial_reopen_ms` (fixed; CLAUDE.md "devices appear or they don't —
/// backoff doesn't help") until the device is reachable again or the cancel
/// token trips. The TxQueue is drained-and-discarded on every disconnect so a
/// fresh device never inherits telemetry that aged out while unplugged
/// (CLAUDE.md "TX queue on disconnect: drain and discard, never replay").
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
        identity: _,
    } = spec;
    let SerialWiring {
        frame_tx,
        tx_queue,
        stats,
        cancel,
    } = wiring;

    let reopen_delay = Duration::from_millis(cfg.serial_reopen_ms);

    loop {
        if cancel.is_cancelled() {
            tx_queue.drain_and_discard();
            return Ok(());
        }

        let stream = match open_until_cancel(&path, baud, flow_control, reopen_delay, &cancel).await
        {
            OpenOutcome::Opened(s) => s,
            OpenOutcome::Cancelled => {
                tx_queue.drain_and_discard();
                return Ok(());
            }
        };

        // Drain any frames the router queued while we were re-opening. They
        // would otherwise be stale by the time they hit the wire — the router
        // is already pushing fresh ones.
        let drained = tx_queue.drain_and_discard();
        if drained > 0 {
            trace!(drained, "serial drained stale frames before resuming");
        }

        match run_session(
            stream,
            endpoint_id,
            &stats,
            &frame_tx,
            &tx_queue,
            &cancel,
            cfg.read_buf_bytes,
        )
        .await
        {
            SessionOutcome::Cancelled | SessionOutcome::RouterGone => {
                tx_queue.drain_and_discard();
                return Ok(());
            }
            SessionOutcome::Disconnected => {
                tx_queue.drain_and_discard();
                // Sleep one reopen interval before reattempting so we don't
                // spin if the device disappeared and `open_until_cancel`
                // would succeed immediately on a zombie path.
                if !wait_or_cancel(&cancel, reopen_delay).await {
                    return Ok(());
                }
            }
        }
    }
}

/// Outcome of the hot-replug open loop — either we got a stream or the cancel
/// token tripped while waiting.
enum OpenOutcome {
    Opened(SerialStream),
    Cancelled,
}

/// Try to open the device, retrying every `reopen_delay` until success or
/// cancellation. The poll interval is *fixed* — capped-exponential backoff is
/// the wrong shape here because a missing serial device doesn't "come back
/// faster" if we wait longer.
async fn open_until_cancel(
    path: &str,
    baud: u32,
    flow_control: SerialFlowControl,
    reopen_delay: Duration,
    cancel: &CancellationToken,
) -> OpenOutcome {
    loop {
        if cancel.is_cancelled() {
            return OpenOutcome::Cancelled;
        }
        match try_open(path, baud, flow_control) {
            Ok(s) => {
                trace!(%path, baud, "serial opened");
                return OpenOutcome::Opened(s);
            }
            Err(e) => {
                warn!(error = %e, %path, baud, "serial open failed; retrying after serial_reopen_ms");
                if !wait_or_cancel(cancel, reopen_delay).await {
                    return OpenOutcome::Cancelled;
                }
            }
        }
    }
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
    let mut framer_counters = FramerCounters::new();

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
                        framer_counters.sync(&framer, stats);
                    }
                    Err(e) => {
                        warn!(error = %e, "serial read failed");
                        return SessionOutcome::Disconnected;
                    }
                }
            }
            frame = tx_queue.pop_or_wait() => {
                if let Err(e) = wh.write_all(&frame).await {
                    warn!(error = %e, "serial write failed");
                    return SessionOutcome::Disconnected;
                }
                stats.add_tx_frame(frame.len());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use crate::endpoint::spec::{CommonQuery, SerialEndpoint};
    use bytes::Bytes;
    use std::sync::atomic::Ordering;
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
            },
            ..SerialEndpoint::default()
        };
        let cfg = SerialConfig::from_endpoint(&ep);
        assert_eq!(cfg.serial_reopen_ms, 250);
        assert_eq!(cfg.read_buf_bytes, 1024);
        assert_eq!(cfg.tx_queue_frames, 16);
    }

    #[tokio::test]
    async fn run_returns_when_cancelled_while_open_retrying() {
        // /dev/null is not a tty so the open call fails immediately; the
        // hot-replug loop then sleeps `serial_reopen_ms` and retries forever
        // until cancellation. This proves the open-retry arm honours the
        // cancel token from inside its `wait_or_cancel` sleep.
        let allocator = EndpointIdAllocator::new();
        let endpoint_id = allocator.alloc();
        let stats = Arc::new(EndpointStats::default());
        let tx_queue = TxQueue::new(8, stats.clone());
        let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);
        let cancel = CancellationToken::new();

        let spec = SerialSpec {
            path: "/dev/null".to_string(),
            baud: 115200,
            flow_control: SerialFlowControl::None,
            endpoint_id,
            name: "test-serial".to_string(),
            cfg: SerialConfig {
                serial_reopen_ms: 1000,
                ..SerialConfig::default()
            },
            identity: IdentityFlags::default(),
        };
        let wiring = SerialWiring {
            frame_tx,
            tx_queue: tx_queue.clone(),
            stats,
            cancel: cancel.clone(),
        };

        let handle = tokio::spawn(async move { run(spec, wiring).await });
        // Cancel while the task is inside the reopen sleep.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("serial run did not return after cancel")
            .expect("join")
            .expect("run result");
    }

    #[tokio::test]
    async fn open_until_cancel_retries_then_yields_on_cancel() {
        // Bad path + 5ms reopen → loop retries multiple times before cancel
        // takes effect. We can't observe the attempt count directly without
        // instrumentation, but cancelling the loop after 50ms with a 5ms
        // poll proves both halves of the loop (retry + cancel-during-sleep)
        // are reachable and well-formed.
        let cancel = CancellationToken::new();
        let task = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                open_until_cancel(
                    "/this/path/definitely/does/not/exist",
                    115200,
                    SerialFlowControl::None,
                    Duration::from_millis(5),
                    &cancel,
                )
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        let outcome = timeout(Duration::from_secs(2), task)
            .await
            .expect("open_until_cancel did not return")
            .expect("join");
        assert!(matches!(outcome, OpenOutcome::Cancelled));
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
    #[tokio::test]
    async fn session_surfaces_disconnected_on_slave_drop() {
        // Dropping the slave end of a pty pair makes the master side see EOF
        // on the next read, which `run_session` must report as
        // `SessionOutcome::Disconnected`. That outcome is what triggers the
        // outer `run_inner` re-open loop — proving the trigger separately
        // from the loop keeps the assertion crisp.
        let (master, slave) = tokio_serial::SerialStream::pair().expect("pty pair");

        let allocator = EndpointIdAllocator::new();
        let endpoint_id = allocator.alloc();
        let stats = Arc::new(EndpointStats::default());
        let tx_queue = TxQueue::new(8, stats.clone());
        let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);
        let cancel = CancellationToken::new();

        // Run the session loop directly with the master we have, then assert
        // it surfaces Disconnected on slave-drop. That's the exact transition
        // run_inner's outer loop reacts to.
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
        drop(slave);
        let outcome = timeout(Duration::from_secs(2), session)
            .await
            .expect("session join timeout")
            .expect("session join");
        assert_eq!(outcome, SessionOutcome::Disconnected);
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
