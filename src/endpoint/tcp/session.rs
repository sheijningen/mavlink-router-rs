use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::super::EndpointId;
use super::super::events::RouterFrame;
use super::super::stats::EndpointStats;
use super::super::tx_queue::TxQueue;
use crate::mavlink::framer::Framer;

/// Why a TCP session terminated — controls whether the caller reconnects
/// (Disconnected) or unwinds toward shutdown (Cancelled / router-channel
/// gone).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOutcome {
    Cancelled,
    Disconnected,
}

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
    let mut last_resync_total: u64 = 0;
    let mut last_crc_total: u64 = 0;

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
                                return SessionOutcome::Cancelled;
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
                        warn!(error = %e, "tcp read failed");
                        return SessionOutcome::Disconnected;
                    }
                }
            }
            frame = pop_or_wait(tx_queue) => {
                if let Err(e) = wh.write_all(&frame).await {
                    warn!(error = %e, "tcp write failed");
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
