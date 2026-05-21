//! `serial:` endpoint: open the device, run the shared session loop, and
//! reopen on disconnect via the fixed [`REOPEN_DELAY`] poll.

use std::time::Duration;

use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

use super::EndpointId;
use super::defaults::DEFAULT_TX_QUEUE_FRAMES;
use super::identity_flags::IdentityFlags;
use super::session::{SessionOutcome, run_session};
use super::spec::{SerialEndpoint, SerialFlowControl};
use super::stats::EndpointState;
use super::wait_or_cancel;
use super::wiring::ClientWiring;

/// Fixed hot-replug poll interval. Below ~100 ms the open-retry loop spins
/// on a missing device for nothing (USB re-enumeration is on the order of
/// seconds); above ~1 s the operator-visible MTTR after a replug climbs
/// for no benefit. 1 s is the right answer for every deployment.
const REOPEN_DELAY: Duration = Duration::from_millis(1000);

/// Inputs that distinguish one `serial:` endpoint from another: which device
/// to open at what baud (with optional hardware flow control) and what to
/// call it. `tx_queue_frames` is always `DEFAULT_TX_QUEUE_FRAMES` at the
/// user-facing layer (CLAUDE.md "Hardcoded plumbing knobs"); the field stays
/// on the Spec so drop-oldest tests can shrink the queue to exercise the
/// `force_push` eviction branch without 256+ dummy frames. `identity`
/// carries the filter / sniffer / group bundle (CLAUDE.md "Filters, group,
/// sniffer travel with the `*Spec`"); the reader applies the in-filter
/// snapshot, the router applies out-filter / sniffer / group from the same
/// bundle.
pub struct SerialSpec {
    pub path: String,
    pub baud: u32,
    pub flow_control: SerialFlowControl,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub tx_queue_frames: usize,
    pub identity: IdentityFlags,
}

impl SerialSpec {
    /// Build a runtime `SerialSpec` from the parsed `SerialEndpoint`. The
    /// spawner supplies `endpoint_id` and `name` because the parser doesn't
    /// allocate IDs. `tx_queue_frames` is stamped from
    /// [`DEFAULT_TX_QUEUE_FRAMES`]; tests bypass this constructor when they
    /// need a smaller queue.
    pub fn from_endpoint(ep: SerialEndpoint, endpoint_id: EndpointId, name: String) -> Self {
        Self {
            path: ep.path,
            baud: ep.baud,
            flow_control: ep.flow_control,
            endpoint_id,
            name,
            tx_queue_frames: DEFAULT_TX_QUEUE_FRAMES,
            identity: ep.identity,
        }
    }
}

/// Run a `serial:` endpoint until the cancellation token fires.
///
/// On startup, opens the configured device at `baud` with the requested
/// flow-control. On open failure or mid-stream disconnect, polls every
/// [`REOPEN_DELAY`] (fixed; CLAUDE.md "devices appear or they don't —
/// backoff doesn't help") until the device is reachable again or the cancel
/// token trips. The TxQueue is drained-and-discarded on every disconnect so a
/// fresh device never inherits telemetry that aged out while unplugged
/// (CLAUDE.md "TX queue on disconnect: drain and discard, never replay").
pub async fn run(spec: SerialSpec, wiring: ClientWiring) {
    let span = info_span!("serial", name = %spec.name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: SerialSpec, wiring: ClientWiring) {
    let SerialSpec {
        path,
        baud,
        flow_control,
        endpoint_id,
        name: _,
        tx_queue_frames: _,
        identity,
    } = spec;
    let ClientWiring {
        frame_tx,
        tx_queue,
        stats,
        cancel,
    } = wiring;

    loop {
        if cancel.is_cancelled() {
            tx_queue.drain_and_discard();
            return;
        }

        let stream = match open_until_cancel(&path, baud, flow_control, &cancel).await {
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
            debug!(drained, "serial drained stale frames before resuming");
        }
        stats.store_state(EndpointState::Connected);

        match run_session(
            stream,
            endpoint_id,
            &stats,
            &frame_tx,
            &tx_queue,
            &cancel,
            &identity.filters,
        )
        .await
        {
            SessionOutcome::Terminated => {
                tx_queue.drain_and_discard();
                return;
            }
            SessionOutcome::Disconnected => {
                info!("serial disconnected; will retry open");
                let drained = tx_queue.drain_and_discard();
                if drained > 0 {
                    debug!(drained, "serial discarded in-flight frames on disconnect");
                }
                stats.store_state(EndpointState::Reconnecting);
                // Sleep one reopen interval before reattempting so we don't
                // spin if the device disappeared and `open_until_cancel`
                // would succeed immediately on a zombie path.
                if !wait_or_cancel(&cancel, REOPEN_DELAY).await {
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

/// Try to open the device, retrying every [`REOPEN_DELAY`] until success or
/// cancellation. The poll interval is *fixed* — capped-exponential backoff is
/// the wrong shape here because a missing serial device doesn't "come back
/// faster" if we wait longer.
async fn open_until_cancel(
    path: &str,
    baud: u32,
    flow_control: SerialFlowControl,
    cancel: &CancellationToken,
) -> OpenOutcome {
    loop {
        if cancel.is_cancelled() {
            return OpenOutcome::Cancelled;
        }
        match try_open(path, baud, flow_control) {
            Ok(stream) => {
                info!(%path, baud, "serial opened");
                return OpenOutcome::Opened(stream);
            }
            Err(err) => {
                warn!(error = %err, %path, baud, "serial open failed; retrying");
                if !wait_or_cancel(cancel, REOPEN_DELAY).await {
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
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn open_until_cancel_yields_on_cancel_during_sleep() {
        // Bad path → first `try_open` fails and the loop drops into a
        // [`REOPEN_DELAY`] sleep; cancelling mid-sleep must return
        // `OpenOutcome::Cancelled` rather than wait out the full delay.
        // Tests a private function — must live next to it; the public-API
        // equivalents are in `tests/serial.rs`.
        let cancel = CancellationToken::new();
        let task = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                open_until_cancel(
                    "/this/path/definitely/does/not/exist",
                    115200,
                    SerialFlowControl::None,
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

    #[test]
    fn try_open_runs_both_flow_control_variants_without_panicking() {
        // The mapping `SerialFlowControl::RtsCts → tokio_serial::FlowControl::
        // Hardware` is a one-line match arm whose only failure mode is "the
        // variant ends up in the wrong arm". A bad-path open errors out the
        // same way for both variants, so we only assert the call doesn't
        // panic — what matters is the branch executes.
        for fc in [SerialFlowControl::None, SerialFlowControl::RtsCts] {
            let result = try_open("/this/path/definitely/does/not/exist", 115200, fc);
            assert!(
                result.is_err(),
                "expected Err for bogus path with {fc:?}, got Ok"
            );
        }
    }
}
