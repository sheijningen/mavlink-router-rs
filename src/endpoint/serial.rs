use std::sync::Arc;

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use super::EndpointId;
use super::events::RouterFrame;
use super::spec::{SerialEndpoint, SerialFlowControl};
use super::stats::EndpointStats;
use super::tx_queue::TxQueue;

const DEFAULT_SERIAL_REOPEN_MS: u64 = 1000;
const DEFAULT_READ_BUF_BYTES: usize = 8192;
const DEFAULT_TX_QUEUE_FRAMES: usize = 256;

/// Per-endpoint runtime configuration. The spec parser hands us a fully-typed
/// `SerialEndpoint`; this struct collapses the optional knobs down to the
/// concrete values the task actually uses, substituting CLAUDE.md defaults
/// where the user left a knob unset.
#[derive(Debug, Clone, Copy)]
pub struct SerialConfig {
    pub serial_reopen_ms: u64,
    pub read_buf_bytes: usize,
    pub tx_queue_frames: usize,
}

impl Default for SerialConfig {
    fn default() -> Self {
        Self {
            serial_reopen_ms: DEFAULT_SERIAL_REOPEN_MS,
            read_buf_bytes: DEFAULT_READ_BUF_BYTES,
            tx_queue_frames: DEFAULT_TX_QUEUE_FRAMES,
        }
    }
}

impl SerialConfig {
    pub fn from_endpoint(ep: &SerialEndpoint) -> Self {
        Self {
            serial_reopen_ms: ep.serial_reopen_ms.unwrap_or(DEFAULT_SERIAL_REOPEN_MS),
            read_buf_bytes: ep.common.read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES),
            tx_queue_frames: ep.common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES),
        }
    }
}

/// Typed-empty return for `serial:` `run()`. Open failures enter the
/// hot-replug poll loop (CLAUDE.md "Bind/open failure at startup is not
/// fatal") and read/write errors trigger the same loop — no terminal failure
/// modes remain in v1. Kept as a typed return for symmetry with the other
/// endpoint modules in case a fatal case shows up later.
#[derive(Debug, Error)]
pub enum SerialError {}

/// Inputs that distinguish one `serial:` endpoint from another: which device
/// to open at what baud (with optional hardware flow control), what to call
/// it, and the per-endpoint knobs from the query string.
pub struct SerialSpec {
    pub path: String,
    pub baud: u32,
    pub flow_control: SerialFlowControl,
    pub endpoint_id: EndpointId,
    pub name: String,
    pub cfg: SerialConfig,
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
/// Phase 4 step 1: types-and-skeleton only — the open/read/write loop and
/// hot-replug recovery land in subsequent commits. Until then this task
/// honours cancellation cleanly so the spawner can include it in its drain
/// set without special-casing.
pub async fn run(spec: SerialSpec, wiring: SerialWiring) -> Result<(), SerialError> {
    let span = info_span!("serial", name = %spec.name);
    run_inner(spec, wiring).instrument(span).await
}

async fn run_inner(spec: SerialSpec, wiring: SerialWiring) -> Result<(), SerialError> {
    let SerialSpec { .. } = spec;
    let SerialWiring {
        frame_tx: _,
        tx_queue,
        stats: _,
        cancel,
    } = wiring;

    cancel.cancelled().await;
    tx_queue.drain_and_discard();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use crate::endpoint::spec::{CommonQuery, SerialEndpoint};
    use std::time::Duration;
    use tokio::time::timeout;

    #[test]
    fn config_defaults_when_endpoint_unset() {
        let ep = SerialEndpoint::default();
        let cfg = SerialConfig::from_endpoint(&ep);
        assert_eq!(cfg.serial_reopen_ms, DEFAULT_SERIAL_REOPEN_MS);
        assert_eq!(cfg.read_buf_bytes, DEFAULT_READ_BUF_BYTES);
        assert_eq!(cfg.tx_queue_frames, DEFAULT_TX_QUEUE_FRAMES);
    }

    #[test]
    fn config_overrides_from_endpoint() {
        let ep = SerialEndpoint {
            serial_reopen_ms: Some(250),
            common: CommonQuery {
                read_buf_bytes: Some(1024),
                tx_queue_frames: Some(16),
                ..CommonQuery::default()
            },
            ..SerialEndpoint::default()
        };
        let cfg = SerialConfig::from_endpoint(&ep);
        assert_eq!(cfg.serial_reopen_ms, 250);
        assert_eq!(cfg.read_buf_bytes, 1024);
        assert_eq!(cfg.tx_queue_frames, 16);
    }

    #[tokio::test]
    async fn run_returns_when_cancelled() {
        let allocator = EndpointIdAllocator::new();
        let endpoint_id = allocator.alloc();
        let stats = Arc::new(EndpointStats::new());
        let tx_queue = TxQueue::new(8, stats.clone());
        let (frame_tx, _frame_rx) = mpsc::channel::<RouterFrame>(8);
        let cancel = CancellationToken::new();

        let spec = SerialSpec {
            path: "/dev/null".to_string(),
            baud: 115200,
            flow_control: SerialFlowControl::None,
            endpoint_id,
            name: "test-serial".to_string(),
            cfg: SerialConfig::default(),
        };
        let wiring = SerialWiring {
            frame_tx,
            tx_queue: tx_queue.clone(),
            stats,
            cancel: cancel.clone(),
        };

        let handle = tokio::spawn(async move { run(spec, wiring).await });
        cancel.cancel();
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("serial run did not return after cancel")
            .expect("join")
            .expect("run result");
    }
}
