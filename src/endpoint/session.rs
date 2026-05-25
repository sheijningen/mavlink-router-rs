use std::io;
use std::ops::ControlFlow;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use super::EndpointId;
use super::events::RouterFrame;
use super::filters::Filters;
use super::identity_flags::SEQ_TRACKER_CAPACITY;
use super::seq_tracker::SeqTracker;
use super::stats::{EndpointStats, FramerCounters};
use super::tx_queue::TxQueue;
use crate::mavlink::framer::Framer;
use tokio::time::Instant;

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

/// Ingress-pipeline context used by every endpoint's reader-side
pub struct SessionCtx<'a> {
    pub endpoint_id: EndpointId,
    pub stats: &'a EndpointStats,
    pub frame_tx: &'a mpsc::Sender<RouterFrame>,
    pub filters: &'a Filters,
}

impl SessionCtx<'_> {
    pub(crate) async fn forward_inbound_frames(
        &self,
        framer: &mut Framer,
        seq_tracker: &mut SeqTracker,
    ) -> ControlFlow<()> {
        while let Some((header, frame)) = framer.try_next_frame() {
            self.stats.add_rx_frame(frame.len());
            let lost = seq_tracker.observe(header.source, header.seq, Instant::now());
            if lost > 0 {
                self.stats
                    .rx_lost_est
                    .fetch_add(lost as u64, Ordering::Relaxed);
            }
            if !self.filters.passes_in_filter(header.msgid, header.source) {
                self.stats.in_filter_drops.fetch_add(1, Ordering::Relaxed);
                trace!(
                    msgid = header.msgid,
                    sysid = header.source.sys,
                    compid = header.source.comp,
                    "in-filter dropped frame at ingress"
                );
                continue;
            }
            if self
                .frame_tx
                .send(RouterFrame {
                    endpoint_id: self.endpoint_id,
                    frame,
                    header,
                })
                .await
                .is_err()
            {
                debug!("router channel closed; stopping frame forwarding");
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
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
    ctx: &SessionCtx<'_>,
    tx_queue: &TxQueue,
    cancel: &CancellationToken,
) -> SessionOutcome
where
    S: AsyncRead + AsyncWrite,
{
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let mut framer = Framer::new();
    let mut framer_counters = FramerCounters::new();
    let mut seq_tracker = SeqTracker::new(SEQ_TRACKER_CAPACITY);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return SessionOutcome::Terminated,
            res = read_half.read_buf(framer.buffer_mut()) => {
                if let ControlFlow::Break(outcome) = handle_read_result(
                    ctx,
                    res,
                    &mut framer,
                    &mut framer_counters,
                    &mut seq_tracker,
                ).await {
                    return outcome;
                }
            }
            frame = tx_queue.pop_or_wait() => {
                if let ControlFlow::Break(outcome) = write_outbound_frame(&mut write_half, frame, ctx.stats).await {
                    return outcome;
                }
            }
        }
    }
}

async fn handle_read_result(
    ctx: &SessionCtx<'_>,
    res: io::Result<usize>,
    framer: &mut Framer,
    framer_counters: &mut FramerCounters,
    seq_tracker: &mut SeqTracker,
) -> ControlFlow<SessionOutcome> {
    match res {
        Ok(0) => {
            debug!("read returned EOF (peer closed)");
            return ControlFlow::Break(SessionOutcome::Disconnected);
        }
        Err(err) => {
            warn!(error = %err, "session read failed");
            return ControlFlow::Break(SessionOutcome::Disconnected);
        }
        Ok(_) => {}
    }
    if ctx
        .forward_inbound_frames(framer, seq_tracker)
        .await
        .is_break()
    {
        return ControlFlow::Break(SessionOutcome::Terminated);
    }
    framer_counters.sync(framer, ctx.stats);
    ControlFlow::Continue(())
}

async fn write_outbound_frame<W: AsyncWrite + Unpin>(
    write_half: &mut W,
    frame: Bytes,
    stats: &EndpointStats,
) -> ControlFlow<SessionOutcome> {
    if let Err(err) = write_half.write_all(&frame).await {
        warn!(error = %err, "session write failed");
        return ControlFlow::Break(SessionOutcome::Disconnected);
    }
    stats.add_tx_frame(frame.len());
    ControlFlow::Continue(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use crate::endpoint::filters::{Filters, MsgIdRange};
    use crate::mavlink::crc::Crc16;
    use crate::mavlink::frame::STX_V1;
    use bytes::BufMut;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll};

    fn no_filter() -> Filters {
        Filters::default()
    }

    fn fresh_tracker() -> SeqTracker {
        SeqTracker::new(8)
    }

    fn build_v1_heartbeat() -> Vec<u8> {
        // msgid 0 (HEARTBEAT), crc_extra 50, 9-byte payload — the smallest
        // known-msgid frame that exercises CRC-validated routing.
        let payload = [0u8; 9];
        let mut frame = vec![STX_V1, payload.len() as u8, 0, 1, 1, 0];
        frame.extend_from_slice(&payload);
        let mut crc = Crc16::new();
        crc.update_slice(&frame[1..]);
        crc.update(50);
        let crc_value = crc.finalize();
        frame.push((crc_value & 0xFF) as u8);
        frame.push((crc_value >> 8) as u8);
        frame
    }

    fn fresh_id() -> EndpointId {
        EndpointIdAllocator::new().alloc()
    }

    fn io_err() -> io::Error {
        io::Error::other("boom")
    }

    #[tokio::test]
    async fn handle_read_eof_breaks_disconnected() {
        let mut framer = Framer::with_capacity(64);
        let mut counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::default());
        let (tx, _rx) = mpsc::channel(8);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = handle_read_result(
            &ctx,
            Ok(0),
            &mut framer,
            &mut counters,
            &mut fresh_tracker(),
        )
        .await;
        assert_eq!(out, ControlFlow::Break(SessionOutcome::Disconnected));
    }

    #[tokio::test]
    async fn handle_read_err_breaks_disconnected() {
        let mut framer = Framer::with_capacity(64);
        let mut counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::default());
        let (tx, _rx) = mpsc::channel(8);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = handle_read_result(
            &ctx,
            Err(io_err()),
            &mut framer,
            &mut counters,
            &mut fresh_tracker(),
        )
        .await;
        assert_eq!(out, ControlFlow::Break(SessionOutcome::Disconnected));
    }

    #[tokio::test]
    async fn handle_read_partial_frame_continues_without_send() {
        let mut framer = Framer::with_capacity(64);
        framer.buffer_mut().put_slice(&[STX_V1, 9, 0]);
        let mut counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::default());
        let (tx, mut rx) = mpsc::channel(8);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = handle_read_result(
            &ctx,
            Ok(3),
            &mut framer,
            &mut counters,
            &mut fresh_tracker(),
        )
        .await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert!(rx.try_recv().is_err());
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn handle_read_complete_frame_forwards_and_bumps_stats() {
        let bytes = build_v1_heartbeat();
        let mut framer = Framer::with_capacity(64);
        framer.buffer_mut().put_slice(&bytes);
        let mut counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::default());
        let (tx, mut rx) = mpsc::channel(8);
        let filters = no_filter();
        let id = fresh_id();
        let ctx = SessionCtx {
            endpoint_id: id,
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = handle_read_result(
            &ctx,
            Ok(bytes.len()),
            &mut framer,
            &mut counters,
            &mut fresh_tracker(),
        )
        .await;
        assert_eq!(out, ControlFlow::Continue(()));

        let router_frame = rx.try_recv().expect("frame should be forwarded");
        assert_eq!(router_frame.endpoint_id, id);
        assert_eq!(&router_frame.frame[..], &bytes[..]);
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 1);
        assert_eq!(stats.rx_bytes.load(Ordering::Relaxed), bytes.len() as u64);
    }

    #[tokio::test]
    async fn handle_read_router_closed_breaks_terminated_but_counts_rx() {
        // rx counters bump before the send; a closed router still leaves the
        // frame visible in stats. Locking that behaviour in.
        let bytes = build_v1_heartbeat();
        let mut framer = Framer::with_capacity(64);
        framer.buffer_mut().put_slice(&bytes);
        let mut counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::default());
        let (tx, rx) = mpsc::channel(8);
        drop(rx);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = handle_read_result(
            &ctx,
            Ok(bytes.len()),
            &mut framer,
            &mut counters,
            &mut fresh_tracker(),
        )
        .await;
        assert_eq!(out, ControlFlow::Break(SessionOutcome::Terminated));
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn handle_read_syncs_framer_counters_to_stats() {
        // Four garbage bytes ahead of a valid frame produce resync_bytes=4
        // inside the framer; sync() must forward that delta to shared stats.
        let frame = build_v1_heartbeat();
        let mut buf = vec![0u8; 4];
        buf.extend_from_slice(&frame);

        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&buf);
        let mut counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::default());
        let (tx, _rx) = mpsc::channel(8);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = handle_read_result(
            &ctx,
            Ok(buf.len()),
            &mut framer,
            &mut counters,
            &mut fresh_tracker(),
        )
        .await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert_eq!(stats.resync_bytes.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn handle_read_terminated_path_skips_framer_sync() {
        // Lock in the contract: when forward_inbound_frames short-circuits
        // with Break(Terminated), the trailing framer_counters.sync() is
        // intentionally skipped — the session is winding down and the final
        // resync_bytes/crc_errors delta is allowed to die with it.
        let frame = build_v1_heartbeat();
        let mut buf = vec![0u8; 4];
        buf.extend_from_slice(&frame);

        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&buf);
        let mut counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::default());
        let (tx, rx) = mpsc::channel(8);
        drop(rx);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = handle_read_result(
            &ctx,
            Ok(buf.len()),
            &mut framer,
            &mut counters,
            &mut fresh_tracker(),
        )
        .await;
        assert_eq!(out, ControlFlow::Break(SessionOutcome::Terminated));
        assert_eq!(stats.resync_bytes.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn handle_read_corrupted_crc_propagates_to_stats() {
        let mut frame = build_v1_heartbeat();
        let last = frame.len() - 1;
        frame[last] ^= 0xFF;

        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&frame);
        let mut counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::default());
        let (tx, mut rx) = mpsc::channel(8);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = handle_read_result(
            &ctx,
            Ok(frame.len()),
            &mut framer,
            &mut counters,
            &mut fresh_tracker(),
        )
        .await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert_eq!(stats.crc_errors.load(Ordering::Relaxed), 1);
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handle_read_multiple_frames_in_one_buffer_fill_all_forward() {
        let frame = build_v1_heartbeat();
        let mut buf = frame.clone();
        buf.extend_from_slice(&frame);

        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&buf);
        let mut counters = FramerCounters::new();
        let stats = Arc::new(EndpointStats::default());
        let (tx, mut rx) = mpsc::channel(8);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = handle_read_result(
            &ctx,
            Ok(buf.len()),
            &mut framer,
            &mut counters,
            &mut fresh_tracker(),
        )
        .await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 2);
        assert_eq!(
            stats.rx_bytes.load(Ordering::Relaxed),
            2 * frame.len() as u64
        );
    }

    #[tokio::test]
    async fn forward_inbound_frames_empty_continues() {
        let mut framer = Framer::with_capacity(64);
        let stats = Arc::new(EndpointStats::default());
        let (tx, mut rx) = mpsc::channel(8);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = ctx
            .forward_inbound_frames(&mut framer, &mut fresh_tracker())
            .await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert!(rx.try_recv().is_err());
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn forward_inbound_frames_drains_all_buffered() {
        let bytes = build_v1_heartbeat();
        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&bytes);
        framer.buffer_mut().put_slice(&bytes);
        let stats = Arc::new(EndpointStats::default());
        let (tx, mut rx) = mpsc::channel(8);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = ctx
            .forward_inbound_frames(&mut framer, &mut fresh_tracker())
            .await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 2);
        assert_eq!(
            stats.rx_bytes.load(Ordering::Relaxed),
            2 * bytes.len() as u64
        );
    }

    #[tokio::test]
    async fn forward_inbound_frames_breaks_when_router_channel_closed() {
        let bytes = build_v1_heartbeat();
        let mut framer = Framer::with_capacity(64);
        framer.buffer_mut().put_slice(&bytes);
        let stats = Arc::new(EndpointStats::default());
        let (tx, rx) = mpsc::channel(8);
        drop(rx);
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = ctx
            .forward_inbound_frames(&mut framer, &mut fresh_tracker())
            .await;
        assert_eq!(out, ControlFlow::Break(()));
    }

    #[tokio::test]
    async fn in_filter_blocks_frame_and_bumps_drop_counter() {
        // A frame whose msgid matches `block_msgid_in` must not reach the
        // router and must increment `in_filter_drops`; `rx_frames` still
        // bumps because the framer admitted the frame (CLAUDE.md "Counter
        // overlap: in_filter_drops is the union of all ingress-side drops").
        let bytes = build_v1_heartbeat(); // msgid 0 (HEARTBEAT)
        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&bytes);
        let stats = Arc::new(EndpointStats::default());
        let (tx, mut rx) = mpsc::channel(8);
        let filters = Filters {
            block_msgid_in: vec![MsgIdRange::single(0)],
            ..Filters::default()
        };
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = ctx
            .forward_inbound_frames(&mut framer, &mut fresh_tracker())
            .await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert!(rx.try_recv().is_err(), "frame must not reach router");
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 1);
        assert_eq!(stats.in_filter_drops.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn in_filter_pass_lets_frame_through_without_counter_bump() {
        let bytes = build_v1_heartbeat();
        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&bytes);
        let stats = Arc::new(EndpointStats::default());
        let (tx, mut rx) = mpsc::channel(8);
        let filters = Filters {
            allow_msgid_in: vec![MsgIdRange::single(0)],
            ..Filters::default()
        };
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = ctx
            .forward_inbound_frames(&mut framer, &mut fresh_tracker())
            .await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert!(rx.try_recv().is_ok());
        assert_eq!(stats.in_filter_drops.load(Ordering::Relaxed), 0);
    }

    fn build_v1_heartbeat_with_seq(seq: u8) -> Vec<u8> {
        let payload = [0u8; 9];
        let mut frame = vec![STX_V1, payload.len() as u8, seq, 1, 1, 0];
        frame.extend_from_slice(&payload);
        let mut crc = Crc16::new();
        crc.update_slice(&frame[1..]);
        crc.update(50);
        let crc_value = crc.finalize();
        frame.push((crc_value & 0xFF) as u8);
        frame.push((crc_value >> 8) as u8);
        frame
    }

    #[tokio::test]
    async fn seq_tracker_bumps_rx_lost_est_on_small_gap() {
        let f0 = build_v1_heartbeat_with_seq(0);
        let f3 = build_v1_heartbeat_with_seq(3);
        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&f0);
        framer.buffer_mut().put_slice(&f3);
        let stats = Arc::new(EndpointStats::default());
        let (tx, _rx) = mpsc::channel(8);
        let mut tracker = fresh_tracker();
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = ctx.forward_inbound_frames(&mut framer, &mut tracker).await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 2);
        assert_eq!(stats.rx_lost_est.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn seq_tracker_runs_before_in_filter_so_blocked_frame_still_counts() {
        // CLAUDE.md "Runs before In-filter so the counter reflects link
        // quality, not policy" — In-filter blocks msgid 0, but rx_lost_est
        // still bumps by 3 (gap between seq 0 and seq 4).
        let f0 = build_v1_heartbeat_with_seq(0);
        let f4 = build_v1_heartbeat_with_seq(4);
        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&f0);
        framer.buffer_mut().put_slice(&f4);
        let stats = Arc::new(EndpointStats::default());
        let (tx, mut rx) = mpsc::channel(8);
        let filters = Filters {
            block_msgid_in: vec![MsgIdRange::single(0)],
            ..Filters::default()
        };
        let mut tracker = fresh_tracker();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let out = ctx.forward_inbound_frames(&mut framer, &mut tracker).await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert!(rx.try_recv().is_err());
        assert_eq!(stats.in_filter_drops.load(Ordering::Relaxed), 2);
        assert_eq!(stats.rx_lost_est.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn seq_tracker_consecutive_seqs_no_bump() {
        let frames: Vec<Vec<u8>> = (0u8..=4u8).map(build_v1_heartbeat_with_seq).collect();
        let mut framer = Framer::with_capacity(256);
        for frame in &frames {
            framer.buffer_mut().put_slice(frame);
        }
        let stats = Arc::new(EndpointStats::default());
        let (tx, _rx) = mpsc::channel(16);
        let mut tracker = fresh_tracker();
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let _ = ctx.forward_inbound_frames(&mut framer, &mut tracker).await;
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 5);
        assert_eq!(stats.rx_lost_est.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn seq_tracker_large_gap_treated_as_restart_no_bump() {
        let f0 = build_v1_heartbeat_with_seq(0);
        let f200 = build_v1_heartbeat_with_seq(200);
        let mut framer = Framer::with_capacity(128);
        framer.buffer_mut().put_slice(&f0);
        framer.buffer_mut().put_slice(&f200);
        let stats = Arc::new(EndpointStats::default());
        let (tx, _rx) = mpsc::channel(8);
        let mut tracker = fresh_tracker();
        let filters = no_filter();
        let ctx = SessionCtx {
            endpoint_id: fresh_id(),
            stats: &stats,
            frame_tx: &tx,
            filters: &filters,
        };

        let _ = ctx.forward_inbound_frames(&mut framer, &mut tracker).await;
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 2);
        assert_eq!(stats.rx_lost_est.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn write_outbound_frame_success_bumps_tx_stats() {
        let stats = Arc::new(EndpointStats::default());
        let mut sink = tokio::io::sink();
        let frame = Bytes::from_static(b"some bytes");

        let out = write_outbound_frame(&mut sink, frame.clone(), &stats).await;
        assert_eq!(out, ControlFlow::Continue(()));
        assert_eq!(stats.tx_frames.load(Ordering::Relaxed), 1);
        assert_eq!(stats.tx_bytes.load(Ordering::Relaxed), frame.len() as u64);
    }

    #[tokio::test]
    async fn write_outbound_frame_io_error_breaks_disconnected() {
        struct AlwaysErr;
        impl AsyncWrite for AlwaysErr {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        let stats = Arc::new(EndpointStats::default());
        let mut writer = AlwaysErr;
        let out = write_outbound_frame(&mut writer, Bytes::from_static(b"hi"), &stats).await;
        assert_eq!(out, ControlFlow::Break(SessionOutcome::Disconnected));
        assert_eq!(stats.tx_frames.load(Ordering::Relaxed), 0);
        assert_eq!(stats.tx_bytes.load(Ordering::Relaxed), 0);
    }
}
