use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use crossbeam_queue::ArrayQueue;
use tokio::sync::Notify;

use super::stats::EndpointStats;

/// Bounded router→writer queue with drop-oldest overflow. The router pushes
/// frames as `Bytes` clones; the writer task drains with `pop_or_wait`, which
/// returns the next frame or `await`s a notify wake when the queue is empty.
/// Each push that displaces an older entry increments `dropped_tx` on the
/// shared stats.
#[derive(Clone, Debug)]
pub struct TxQueue {
    inner: Arc<ArrayQueue<Bytes>>,
    notify: Arc<Notify>,
    stats: Arc<EndpointStats>,
}

impl TxQueue {
    pub fn new(capacity: usize, stats: Arc<EndpointStats>) -> Self {
        let clamped = capacity.max(1);
        Self {
            inner: Arc::new(ArrayQueue::new(clamped)),
            notify: Arc::new(Notify::new()),
            stats,
        }
    }

    /// Insert a frame, evicting the oldest entry if at capacity. An evicted
    /// frame increments `dropped_tx` on the shared stats — callers don't need
    /// to (and shouldn't) account for overflow themselves. Always wakes the
    /// writer.
    pub fn push(&self, frame: Bytes) {
        if self.inner.force_push(frame).is_some() {
            self.stats.dropped_tx.fetch_add(1, Ordering::Relaxed);
        }
        self.notify.notify_one();
    }

    pub fn pop(&self) -> Option<Bytes> {
        self.inner.pop()
    }

    /// Block until a frame is available, then return it. Lets a writer task
    /// `select!` on `queue.pop_or_wait()` without re-implementing the
    /// pop-or-notify-wait loop in every endpoint module.
    pub async fn pop_or_wait(&self) -> Bytes {
        loop {
            if let Some(frame) = self.inner.pop() {
                return frame;
            }
            self.notify.notified().await;
        }
    }

    /// Drain everything currently queued and count the drained frames as
    /// `dropped_tx`. Called by the writer before reconnecting so a fresh link
    /// never carries telemetry that aged out during the outage.
    pub fn drain_and_discard(&self) -> usize {
        let mut count: u64 = 0;
        while self.inner.pop().is_some() {
            count += 1;
        }
        if count > 0 {
            self.stats.dropped_tx.fetch_add(count, Ordering::Relaxed);
        }
        count as usize
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_queue(capacity: usize) -> (TxQueue, Arc<EndpointStats>) {
        let stats = Arc::new(EndpointStats::default());
        (TxQueue::new(capacity, stats.clone()), stats)
    }

    #[test]
    fn push_under_capacity_no_evict() {
        let (queue, stats) = make_queue(4);
        queue.push(Bytes::from_static(b"a"));
        queue.push(Bytes::from_static(b"b"));
        assert_eq!(queue.len(), 2);
        assert_eq!(stats.dropped_tx.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn push_over_capacity_evicts_head() {
        let (queue, stats) = make_queue(2);
        queue.push(Bytes::from_static(b"a"));
        queue.push(Bytes::from_static(b"b"));
        queue.push(Bytes::from_static(b"c"));
        assert_eq!(stats.dropped_tx.load(Ordering::Relaxed), 1);
        assert_eq!(queue.pop().as_deref(), Some(b"b" as &[u8]));
        assert_eq!(queue.pop().as_deref(), Some(b"c" as &[u8]));
        assert!(queue.pop().is_none());
    }

    #[test]
    fn drain_and_discard_counts() {
        let (queue, stats) = make_queue(4);
        queue.push(Bytes::from_static(b"a"));
        queue.push(Bytes::from_static(b"b"));
        queue.push(Bytes::from_static(b"c"));
        let drained = queue.drain_and_discard();
        assert_eq!(drained, 3);
        assert!(queue.inner.is_empty());
        assert_eq!(stats.dropped_tx.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn drain_empty_increments_nothing() {
        let (queue, stats) = make_queue(4);
        assert_eq!(queue.drain_and_discard(), 0);
        assert_eq!(stats.dropped_tx.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn new_clamps_zero_capacity_to_one() {
        let stats = Arc::new(EndpointStats::default());
        let queue = TxQueue::new(0, stats);
        assert!(queue.inner.capacity() >= 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pop_or_wait_wakes_on_push() {
        let (queue, _) = make_queue(2);
        let waiter = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.pop_or_wait().await })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        queue.push(Bytes::from_static(b"x"));
        let popped = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter timed out")
            .expect("waiter task panicked");
        assert_eq!(popped.as_ref(), b"x");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aborted_waiter_does_not_break_subsequent_wakeup() {
        // Drop a registered Notify waiter, then verify a fresh waiter still
        // wakes on the next push. Guards against a regression where the
        // aborted Notified future would mishandle its permit slot.
        let (queue, _) = make_queue(2);
        let aborted = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.pop_or_wait().await })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        aborted.abort();
        let _ = aborted.await;

        let new_waiter = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.pop_or_wait().await })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        queue.push(Bytes::from_static(b"x"));
        let popped = tokio::time::timeout(Duration::from_secs(1), new_waiter)
            .await
            .expect("new waiter timed out — abort may have left Notify in a bad state")
            .expect("new waiter task panicked");
        assert_eq!(popped.as_ref(), b"x");
    }
}
