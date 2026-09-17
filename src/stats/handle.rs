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
/// under the stats task's `POST_CANCEL_DRAIN` so every send the router still
/// attempts lands while the task is receiving.
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
            shutdown = self.shutdown_deadline.is_some(),
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

#[cfg(test)]
mod tests {
    use tokio::time::timeout;

    use super::*;
    use crate::endpoint::EndpointIdAllocator;

    fn fresh_stats() -> Arc<EndpointStats> {
        Arc::new(EndpointStats::default())
    }

    #[tokio::test]
    async fn full_channel_drops_instead_of_blocking() {
        // One slot and no consumer: every event after the first must be
        // dropped, not awaited. A regression hangs, hence the timeouts.
        let (mut handle, mut inbox) = channel(1);
        let alloc = EndpointIdAllocator::new();
        let first = alloc.alloc();
        let second = alloc.alloc();
        for id in [first, second, alloc.alloc()] {
            timeout(
                Duration::from_secs(1),
                handle.register(id, "ep".to_string(), fresh_stats(), true),
            )
            .await
            .expect("register awaited the full channel");
        }
        timeout(Duration::from_secs(1), handle.finalize(second))
            .await
            .expect("finalize awaited the full channel");
        assert_eq!(handle.dropped, 3);
        assert!(matches!(inbox.try_recv(), Some(StatsEvent::Register { id, .. }) if id == first));
        assert!(inbox.try_recv().is_none());
    }

    #[tokio::test]
    async fn armed_deadline_waits_for_channel_room() {
        // One slot with a consumer that only runs while the handle awaits.
        let (mut handle, mut inbox) = channel(1);
        let alloc = EndpointIdAllocator::new();
        let ids: Vec<EndpointId> = (0..5).map(|_| alloc.alloc()).collect();
        let consumer = tokio::spawn(async move {
            let mut finalized = Vec::new();
            while let Some(StatsEvent::Finalize { id }) = inbox.recv().await {
                finalized.push(id);
            }
            finalized
        });

        handle.arm_shutdown_deadline();
        for id in &ids {
            handle.finalize(*id).await;
        }
        drop(handle);

        assert_eq!(consumer.await.expect("consumer panicked"), ids);
    }

    #[tokio::test(start_paused = true)]
    async fn armed_deadline_drops_once_it_passes() {
        let (mut handle, _inbox) = channel(1);
        let alloc = EndpointIdAllocator::new();
        handle
            .register(alloc.alloc(), "ep".to_string(), fresh_stats(), true)
            .await;

        handle.arm_shutdown_deadline();
        timeout(
            SHUTDOWN_STATS_SEND_BUDGET * 2,
            handle.finalize(alloc.alloc()),
        )
        .await
        .expect("finalize outlived the shutdown deadline");
        assert_eq!(handle.dropped, 1);
    }
}
