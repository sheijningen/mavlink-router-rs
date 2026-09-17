//! Incremental stdout writer that advances one write call per step.

use std::collections::VecDeque;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::{debug, warn};

use super::queue::QueueEntry;

/// Serialized line partway to stdout.
struct InFlightLine {
    bytes: Vec<u8>,
    written: usize,
}

/// Stdout sink that advances one `write` call per step.
pub(super) struct LineWriter<W> {
    writer: W,
    in_flight: Option<InFlightLine>,
    output_failed: bool,
}

impl<W> LineWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub(super) fn new(writer: W) -> Self {
        Self {
            writer,
            in_flight: None,
            output_failed: false,
        }
    }

    pub(super) fn has_pending(&self) -> bool {
        self.in_flight.is_some()
    }

    pub(super) fn stage_next(&mut self, queue: &mut VecDeque<QueueEntry>) {
        if self.in_flight.is_some() {
            return;
        }
        if self.output_failed {
            queue.clear();
            return;
        }
        while let Some(entry) = queue.pop_front() {
            match serde_json::to_vec(&entry.line) {
                Ok(mut bytes) => {
                    bytes.push(b'\n');
                    self.in_flight = Some(InFlightLine { bytes, written: 0 });
                    return;
                }
                Err(err) => {
                    debug!(error = %err, "stats: serialize failed; dropping line");
                }
            }
        }
    }

    /// A single `write` is cancel safe, so an interrupted step leaves `written` exact.
    pub(super) async fn write_step(&mut self) {
        let Some(line) = self.in_flight.as_mut() else {
            return;
        };
        // A failed write may leave a fragment; stop rather than append to it.
        match self.writer.write(&line.bytes[line.written..]).await {
            Ok(0) => {
                warn!("stats stdout accepted no bytes; suppressing further stats output");
                self.output_failed = true;
                self.in_flight = None;
            }
            Ok(count) => {
                line.written = line.written.saturating_add(count);
                if line.written >= line.bytes.len() {
                    self.in_flight = None;
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => {
                warn!("stats stdout broken pipe; suppressing further stats output");
                self.output_failed = true;
                self.in_flight = None;
            }
            Err(err) => {
                warn!(error = %err, "stats stdout write failed; suppressing further stats output");
                self.output_failed = true;
                self.in_flight = None;
            }
        }
    }

    pub(super) async fn drain(&mut self, queue: &mut VecDeque<QueueEntry>) {
        loop {
            self.stage_next(queue);
            if !self.has_pending() {
                break;
            }
            self.write_step().await;
        }
        let _ = self.writer.flush().await;
    }
}
