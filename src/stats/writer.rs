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

/// Stdout sink; each step issues one `write` call so event intake never waits on a line.
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
        // Only a dead stdout stops output; any other error costs the current line.
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
                warn!(error = %err, "stats stdout write failed; dropping line");
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

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, duplex};

    use super::super::line::build_line;
    use super::*;
    use crate::endpoint::stats::{EndpointState, EndpointStats};

    fn queue_of(names: &[String]) -> VecDeque<QueueEntry> {
        names
            .iter()
            .map(|name| {
                let stats = EndpointStats::default();
                stats.store_state(EndpointState::Down);
                QueueEntry {
                    line: build_line(name, &stats, "ts".to_string(), true),
                    synthetic: true,
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn partial_writes_deliver_whole_lines_in_order() {
        // A 16-byte duplex splits every line into many partial writes.
        let (writer, mut reader) = duplex(16);
        let collector = tokio::spawn(async move {
            let mut out = Vec::new();
            reader.read_to_end(&mut out).await.expect("read_to_end");
            out
        });
        let names: Vec<String> = (0..20).map(|index| format!("ep{index}")).collect();
        let mut queue = queue_of(&names);
        let mut line_writer = LineWriter::new(writer);
        line_writer.drain(&mut queue).await;
        drop(line_writer);

        let out = collector.await.expect("collector panicked");
        let text = std::str::from_utf8(&out).expect("utf8");
        let delivered: Vec<String> = text
            .lines()
            .map(|line| {
                let value: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|err| panic!("corrupt line {line:?}: {err}"));
                value["endpoint"].as_str().expect("endpoint").to_string()
            })
            .collect();
        assert_eq!(delivered, names);
    }

    #[tokio::test]
    async fn failed_output_discards_the_rest_of_the_queue() {
        let (writer, reader) = duplex(16);
        drop(reader);
        let mut queue = queue_of(&["a".to_string(), "b".to_string()]);
        let mut line_writer = LineWriter::new(writer);

        line_writer.stage_next(&mut queue);
        line_writer.write_step().await;
        assert!(line_writer.output_failed);
        assert!(!line_writer.has_pending());

        line_writer.stage_next(&mut queue);
        assert!(queue.is_empty());
        assert!(!line_writer.has_pending());
    }
}
