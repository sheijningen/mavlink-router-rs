use std::io;
use std::ops::ControlFlow;
use std::sync::Arc;

use bytes::Bytes;
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
/// `Terminated` is the unrecoverable case — either the cancel token fired
/// or the router channel closed — and callers wind down the endpoint task
/// on it. `Disconnected` is the recoverable case: the caller drains its
/// TxQueue, then re-opens / reconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOutcome {
    Terminated,
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
            _ = cancel.cancelled() => return SessionOutcome::Terminated,
            res = rh.read_buf(framer.buffer_mut()) => {
                if let ControlFlow::Break(outcome) = handle_read_result(
                    res,
                    &mut framer,
                    &mut framer_counters,
                    stats,
                    endpoint_id,
                    frame_tx,
                ).await {
                    return outcome;
                }
            }
            frame = tx_queue.pop_or_wait() => {
                if let ControlFlow::Break(outcome) = write_outbound_frame(&mut wh, frame, stats).await {
                    return outcome;
                }
            }
        }
    }
}

async fn handle_read_result(
    res: io::Result<usize>,
    framer: &mut Framer,
    framer_counters: &mut FramerCounters,
    stats: &EndpointStats,
    endpoint_id: EndpointId,
    frame_tx: &mpsc::Sender<RouterFrame>,
) -> ControlFlow<SessionOutcome> {
    match res {
        Ok(0) => {
            debug!("read returned EOF (peer closed)");
            return ControlFlow::Break(SessionOutcome::Disconnected);
        }
        Err(e) => {
            warn!(error = %e, "session read failed");
            return ControlFlow::Break(SessionOutcome::Disconnected);
        }
        Ok(_) => {}
    }
    forward_inbound_frames(framer, stats, endpoint_id, frame_tx).await?;
    framer_counters.sync(framer, stats);
    ControlFlow::Continue(())
}

async fn forward_inbound_frames(
    framer: &mut Framer,
    stats: &EndpointStats,
    endpoint_id: EndpointId,
    frame_tx: &mpsc::Sender<RouterFrame>,
) -> ControlFlow<SessionOutcome> {
    while let Some((header, frame)) = framer.try_next_frame() {
        stats.add_rx_frame(frame.len());
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
            return ControlFlow::Break(SessionOutcome::Terminated);
        }
    }
    ControlFlow::Continue(())
}

async fn write_outbound_frame<W: AsyncWrite + Unpin>(
    wh: &mut W,
    frame: Bytes,
    stats: &EndpointStats,
) -> ControlFlow<SessionOutcome> {
    if let Err(e) = wh.write_all(&frame).await {
        warn!(error = %e, "session write failed");
        return ControlFlow::Break(SessionOutcome::Disconnected);
    }
    stats.add_tx_frame(frame.len());
    ControlFlow::Continue(())
}
