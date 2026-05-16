use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::super::EndpointId;
use super::super::events::RouterFrame;
use super::super::session::SessionOutcome;
use super::super::stats::{EndpointStats, FramerCounters};
use super::super::tx_queue::TxQueue;
use crate::mavlink::framer::Framer;

/// Read inbound bytes through a fresh `Framer` and write outbound frames from
/// the TxQueue until cancellation, EOF, or socket I/O error. Shared by
/// `tcpc:` (one session per reconnect attempt) and the `tcps:` per-client
/// task (one session for the life of the connection).
pub async fn run_session(
    stream: TcpStream,
    endpoint_id: EndpointId,
    stats: &Arc<EndpointStats>,
    frame_tx: &mpsc::Sender<RouterFrame>,
    tx_queue: &TxQueue,
    cancel: &CancellationToken,
    read_buf_bytes: usize,
) -> SessionOutcome {
    let (mut rh, mut wh) = stream.into_split();
    let mut framer = Framer::with_capacity(read_buf_bytes);
    let mut framer_counters = FramerCounters::new();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return SessionOutcome::Cancelled,
            res = rh.read_buf(framer.buffer_mut()) => {
                match res {
                    Ok(0) => {
                        debug!("tcp peer closed connection");
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
                                debug!("tcp router channel closed; ending session");
                                return SessionOutcome::RouterGone;
                            }
                        }
                        framer_counters.sync(&framer, stats);
                    }
                    Err(e) => {
                        warn!(error = %e, "tcp read failed");
                        return SessionOutcome::Disconnected;
                    }
                }
            }
            frame = tx_queue.pop_or_wait() => {
                if let Err(e) = wh.write_all(&frame).await {
                    warn!(error = %e, "tcp write failed");
                    return SessionOutcome::Disconnected;
                }
                stats.add_tx_frame(frame.len());
            }
        }
    }
}
