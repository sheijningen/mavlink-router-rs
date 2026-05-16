use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::EndpointId;
use super::events::RouterFrame;
use super::stats::{EndpointStats, FramerCounters};
use super::tx_queue::TxQueue;
use crate::mavlink::framer::Framer;

/// Why a per-connection read/write session terminated. Shared by every
/// endpoint that runs a [`TxQueue`]-fed session loop over an
/// `AsyncRead + AsyncWrite` transport (`serial:`, `tcpc:`, `tcps:` accepted
/// children).
///
/// `Cancelled` and `RouterGone` are handled identically by callers — both
/// terminate the endpoint task — but kept distinct so tracing/logs can tell
/// "the cancel token fired" apart from "the router task exited and dropped
/// the frame channel" during debugging. `Disconnected` is the recoverable
/// case: the caller drains its TxQueue, then re-opens / reconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOutcome {
    Cancelled,
    RouterGone,
    Disconnected,
}

/// Read inbound bytes through a fresh `Framer` and write outbound frames
/// from the TxQueue until cancellation, EOF, or transport I/O error. Shared
/// by `serial:` (one session per re-open), `tcpc:` (one session per
/// reconnect attempt), and `tcps:` accepted children (one session for the
/// life of the connection). The tracing span set by each caller (`serial`,
/// `tcpc`, `tcps_child`) disambiguates log lines without needing transport
/// prefixes inside this loop.
pub async fn run_session<S>(
    stream: S,
    endpoint_id: EndpointId,
    stats: &Arc<EndpointStats>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    tx_queue: &TxQueue,
    cancel: &CancellationToken,
    read_buf_bytes: usize,
) -> SessionOutcome
where
    S: AsyncRead + AsyncWrite,
{
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
                        debug!("read returned EOF (peer closed)");
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
                                debug!("router channel closed; ending session");
                                return SessionOutcome::RouterGone;
                            }
                        }
                        framer_counters.sync(&framer, stats);
                    }
                    Err(e) => {
                        warn!(error = %e, "session read failed");
                        return SessionOutcome::Disconnected;
                    }
                }
            }
            frame = tx_queue.pop_or_wait() => {
                if let Err(e) = wh.write_all(&frame).await {
                    warn!(error = %e, "session write failed");
                    return SessionOutcome::Disconnected;
                }
                stats.add_tx_frame(frame.len());
            }
        }
    }
}
