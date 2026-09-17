//! Router-facing side of the stats channel: the handle that owns the send
//! policy and the opaque inbox the task reads.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{Instant, timeout_at};
use tracing::warn;

use crate::endpoint::EndpointId;
use crate::endpoint::stats::EndpointStats;

/// One lifecycle message carried from a [`StatsHandle`] to the stats task.
#[derive(Debug)]
pub(crate) enum StatsEvent {
    Register {
        id: EndpointId,
        name: String,
        stats: Arc<EndpointStats>,
        /// `tcps:` / `udps:` parent listeners pass `false` — their counters
        /// are structurally zero (no frames traverse them) and the emitted
        /// JSON line carries only `ts`, `endpoint`, and `state`. Leaf
        /// endpoints and learned children/peers pass `true` and get the
        /// full counter schema.
        routable: bool,
    },
    Finalize {
        id: EndpointId,
    },
}

impl StatsEvent {
    fn id(&self) -> EndpointId {
        match self {
            Self::Register { id, .. } | Self::Finalize { id } => *id,
        }
    }
}

/// Post-cancel budget for a [`StatsHandle`] send to find channel room; stays
/// under [`super::task::POST_CANCEL_DRAIN`] so a starved stats task cannot hold the router.
const SHUTDOWN_STATS_SEND_BUDGET: Duration = Duration::from_secs(1);

/// The router's handle to the stats task, created by [`channel`]. While
/// routing a full channel drops the event; once the shutdown deadline is
/// armed, sends wait for room until it passes.
pub struct StatsHandle {
    tx: mpsc::Sender<StatsEvent>,
    shutdown_deadline: Option<Instant>,
    dropped: u64,
}

impl StatsHandle {
    pub(crate) async fn register(
        &mut self,
        id: EndpointId,
        name: String,
        stats: Arc<EndpointStats>,
        routable: bool,
    ) {
        self.forward(StatsEvent::Register {
            id,
            name,
            stats,
            routable,
        })
        .await;
    }

    pub(crate) async fn finalize(&mut self, id: EndpointId) {
        self.forward(StatsEvent::Finalize { id }).await;
    }

    #[cfg(test)]
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped
    }

    pub(crate) fn arm_shutdown_deadline(&mut self) {
        self.shutdown_deadline = Some(Instant::now() + SHUTDOWN_STATS_SEND_BUDGET);
    }

    async fn forward(&mut self, event: StatsEvent) {
        let id = event.id();
        let no_room = match self.shutdown_deadline {
            None => matches!(self.tx.try_send(event), Err(TrySendError::Full(_))),
            Some(deadline) => timeout_at(deadline, self.tx.send(event)).await.is_err(),
        };
        if !no_room {
            return;
        }
        self.dropped += 1;
        warn!(
            %id,
            dropped_total = self.dropped,
            "stats channel full; dropping lifecycle event"
        );
    }
}

/// Receiving half of the stats channel; only [`super::run`] reads it.
pub struct StatsInbox {
    pub(super) rx: mpsc::Receiver<StatsEvent>,
}

#[cfg(test)]
impl StatsInbox {
    pub(crate) fn try_recv(&mut self) -> Option<StatsEvent> {
        self.rx.try_recv().ok()
    }

    pub(crate) async fn recv(&mut self) -> Option<StatsEvent> {
        self.rx.recv().await
    }
}

/// Create the bounded channel between the router and the stats task.
pub fn channel(capacity: usize) -> (StatsHandle, StatsInbox) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        StatsHandle {
            tx,
            shutdown_deadline: None,
            dropped: 0,
        },
        StatsInbox { rx },
    )
}
