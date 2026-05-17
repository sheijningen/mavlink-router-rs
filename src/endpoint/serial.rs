//! `serial:` endpoint: open the device, run the shared session loop, and
//! reopen on disconnect via a fixed `serial_reopen_ms` poll.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, trace, warn};

use super::EndpointId;
use super::defaults::{DEFAULT_READ_BUF_BYTES, DEFAULT_TX_QUEUE_FRAMES};
use super::events::RouterFrame;
use super::identity_flags::IdentityFlags;
use super::session::{SessionOutcome, run_session};
use super::spec::{SerialEndpoint, SerialFlowControl};
use super::stats::{EndpointState, EndpointStats};
use super::tx_queue::TxQueue;
use super::wait_or_cancel;

const DEFAULT_SERIAL_REOPEN_MS: u64 = 1000;

/// Inputs that distinguish one `serial:` endpoint from another: which device
/// to open at what baud (with optional hardware flow control), what to call
/// it, and the per-endpoint knobs from the query string with CLAUDE.md
/// defaults already substituted. `identity` carries the filter / sniffer /
/// group / capacity bundle — unused today, threaded here so Phase 5 readers
/// and the router can consume it without a spawner rework (CLAUDE.md
/// "Filters, group, sniffer, and learn/seq capacities travel with the
/// `*Spec`").
pub struct SerialSpec {
    pub path: String,
    pub baud: u32,
    pub flow_control: SerialFlowControl,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub serial_reopen_ms: u64,
    pub read_buf_bytes: usize,
    pub tx_queue_frames: usize,
    pub identity: IdentityFlags,
}

impl SerialSpec {
    /// Build a runtime `SerialSpec` from the parsed-but-not-defaulted
    /// `SerialEndpoint` the CLI/TOML layer produced, substituting CLAUDE.md
    /// defaults for any unset knob. The spawner supplies `endpoint_id` and
    /// `name` because the parser doesn't allocate IDs.
    pub fn from_endpoint(ep: SerialEndpoint, endpoint_id: EndpointId, name: String) -> Self {
        Self {
            path: ep.path,
            baud: ep.baud,
            flow_control: ep.flow_control,
            endpoint_id,
            name,
            serial_reopen_ms: ep.serial_reopen_ms.unwrap_or(DEFAULT_SERIAL_REOPEN_MS),
            read_buf_bytes: ep.common.read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES),
            tx_queue_frames: ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
            identity: ep.identity,
        }
    }
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

/// Run a `serial:` endpoint until the cancellation token fires.
///
/// On startup, opens the configured device at `baud` with the requested
/// flow-control. On open failure or mid-stream disconnect, polls every
/// `serial_reopen_ms` (fixed; CLAUDE.md "devices appear or they don't —
/// backoff doesn't help") until the device is reachable again or the cancel
/// token trips. The TxQueue is drained-and-discarded on every disconnect so a
/// fresh device never inherits telemetry that aged out while unplugged
/// (CLAUDE.md "TX queue on disconnect: drain and discard, never replay").
pub async fn run(spec: SerialSpec, wiring: SerialWiring) {
    let span = info_span!("serial", name = %spec.name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: SerialSpec, wiring: SerialWiring) {
    let SerialSpec {
        path,
        baud,
        flow_control,
        endpoint_id,
        name: _,
        serial_reopen_ms,
        read_buf_bytes,
        tx_queue_frames: _,
        identity: _,
    } = spec;
    let SerialWiring {
        frame_tx,
        tx_queue,
        stats,
        cancel,
    } = wiring;

    let reopen_delay = Duration::from_millis(serial_reopen_ms);

    loop {
        if cancel.is_cancelled() {
            tx_queue.drain_and_discard();
            return;
        }

        let stream = match open_until_cancel(&path, baud, flow_control, reopen_delay, &cancel).await
        {
            OpenOutcome::Opened(s) => s,
            OpenOutcome::Cancelled => {
                tx_queue.drain_and_discard();
                return;
            }
        };

        // Drain any frames the router queued while we were re-opening. They
        // would otherwise be stale by the time they hit the wire — the router
        // is already pushing fresh ones.
        let drained = tx_queue.drain_and_discard();
        if drained > 0 {
            trace!(drained, "serial drained stale frames before resuming");
        }
        stats.store_state(EndpointState::Connected);

        match run_session(
            stream,
            endpoint_id,
            &stats,
            &frame_tx,
            &tx_queue,
            &cancel,
            read_buf_bytes,
        )
        .await
        {
            SessionOutcome::Terminated => {
                tx_queue.drain_and_discard();
                return;
            }
            SessionOutcome::Disconnected => {
                tx_queue.drain_and_discard();
                stats.store_state(EndpointState::Reconnecting);
                // Sleep one reopen interval before reattempting so we don't
                // spin if the device disappeared and `open_until_cancel`
                // would succeed immediately on a zombie path.
                if !wait_or_cancel(&cancel, reopen_delay).await {
                    return;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::spec::{CommonQuery, SerialEndpoint};
    use std::time::Duration;
    use tokio::time::timeout;

    #[test]
    fn spec_defaults_when_endpoint_unset() {
        let ep = SerialEndpoint::default();
        let spec = SerialSpec::from_endpoint(ep, EndpointId(0), "n".into());
        assert_eq!(spec.serial_reopen_ms, DEFAULT_SERIAL_REOPEN_MS);
        assert_eq!(spec.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(spec.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
    }

    #[test]
    fn spec_overrides_from_endpoint() {
        let ep = SerialEndpoint {
            serial_reopen_ms: Some(250),
            common: CommonQuery {
                read_buf_bytes: Some(1024),
                tx_queue_frames: Some(16),
            },
            ..SerialEndpoint::default()
        };
        let spec = SerialSpec::from_endpoint(ep, EndpointId(0), "n".into());
        assert_eq!(spec.serial_reopen_ms, 250);
        assert_eq!(spec.read_buf_bytes, 1024);
        assert_eq!(spec.tx_queue_frames, 16);
    }

    #[tokio::test]
    async fn open_until_cancel_retries_then_yields_on_cancel() {
        // Bad path + 5ms reopen → loop retries multiple times before cancel
        // takes effect. We can't observe the attempt count directly without
        // instrumentation, but cancelling the loop after 50ms with a 5ms
        // poll proves both halves of the loop (retry + cancel-during-sleep)
        // are reachable and well-formed. Tests a private function — must
        // live next to it; the public-API equivalents are in `tests/serial.rs`.
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
}
