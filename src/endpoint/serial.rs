//! `serial:` endpoint: open the device, run the shared session loop, and
//! reopen on disconnect via the fixed [`REOPEN_DELAY`] poll.

use std::time::Duration;

use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

use super::EndpointId;
use super::identity_flags::IdentityFlags;
use super::session::{SessionCtx, SessionOutcome, run_session};
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
/// call it. `identity` carries the filter / sniffer / group bundle.
pub struct SerialSpec {
    pub path: String,
    pub baud: u32,
    pub flow_control: SerialFlowControl,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub identity: IdentityFlags,
}

impl SerialSpec {
    /// Build a runtime `SerialSpec` from the parsed `SerialEndpoint`. The
    /// spawner supplies `endpoint_id` and `name` because the parser doesn't
    /// allocate IDs.
    pub fn from_endpoint(ep: SerialEndpoint, endpoint_id: EndpointId, name: String) -> Self {
        Self {
            path: ep.path,
            baud: ep.baud,
            flow_control: ep.flow_control,
            endpoint_id,
            name,
            identity: ep.identity,
        }
    }
}

/// Run a `serial:` endpoint until the cancellation token fires. On open
/// failure or mid-stream disconnect, polls every [`REOPEN_DELAY`] until the
/// device is reachable. The TxQueue is drained-and-discarded on every
/// disconnect so a fresh device never inherits stale telemetry.
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

        // Drain frames queued while re-opening; they're stale.
        let drained = tx_queue.drain_and_discard();
        if drained > 0 {
            debug!(drained, "drained stale frames before resuming");
        }
        stats.store_state(EndpointState::Connected);

        let ctx = SessionCtx {
            endpoint_id,
            stats: &stats,
            frame_tx: &frame_tx,
            filters: &identity.filters,
        };
        match run_session(stream, &ctx, &tx_queue, &cancel).await {
            SessionOutcome::Terminated => {
                tx_queue.drain_and_discard();
                return;
            }
            SessionOutcome::Disconnected => {
                info!("disconnected; will retry open");
                let drained = tx_queue.drain_and_discard();
                if drained > 0 {
                    debug!(drained, "discarded in-flight frames on disconnect");
                }
                stats.store_state(EndpointState::Reconnecting);
                // Sleep before reattempting so a zombie-path re-open
                // doesn't spin.
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
                info!(%path, baud, "opened");
                return OpenOutcome::Opened(stream);
            }
            Err(err) => {
                warn!(error = %err, %path, baud, "open failed; retrying");
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
        // Bad-path open errors out the same way for both variants; this
        // just exercises the match arm.
        for fc in [SerialFlowControl::None, SerialFlowControl::RtsCts] {
            let result = try_open("/this/path/definitely/does/not/exist", 115200, fc);
            assert!(
                result.is_err(),
                "expected Err for bogus path with {fc:?}, got Ok"
            );
        }
    }
}
