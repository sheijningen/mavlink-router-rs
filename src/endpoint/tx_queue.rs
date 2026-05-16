use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use crossbeam_queue::ArrayQueue;
use tokio::sync::Notify;

use super::stats::EndpointStats;

/// Bounded router→writer queue with drop-oldest overflow. The router pushes
/// frames as `Bytes` clones; the writer task pops in a loop and awaits
/// `wait_for_push` when empty. Each push that displaces an older entry
/// increments `dropped_tx` on the shared stats.
#[derive(Clone, Debug)]
pub struct TxQueue {
    inner: Arc<ArrayQueue<Bytes>>,
    notify: Arc<Notify>,
    stats: Arc<EndpointStats>,
}

impl TxQueue {
    pub fn new(capacity: usize, stats: Arc<EndpointStats>) -> Self {
        let cap = capacity.max(1);
        Self {
            inner: Arc::new(ArrayQueue::new(cap)),
            notify: Arc::new(Notify::new()),
            stats,
        }
    }

    /// Insert a frame, evicting the oldest entry if at capacity. Returns
    /// `true` if an older frame was displaced (also reflected in `dropped_tx`).
    /// Always wakes the writer.
    pub fn push(&self, frame: Bytes) -> bool {
        let displaced = self.inner.force_push(frame).is_some();
        if displaced {
            self.stats.dropped_tx.fetch_add(1, Ordering::Relaxed);
        }
        self.notify.notify_one();
        displaced
    }

    pub fn pop(&self) -> Option<Bytes> {
        self.inner.pop()
    }

    pub async fn wait_for_push(&self) {
        self.notify.notified().await;
    }

    /// Drain everything currently queued and count the drained frames as
    /// `dropped_tx`. Called by the writer before reconnecting so a fresh link
    /// never carries telemetry that aged out during the outage.
    pub fn drain_and_discard(&self) -> usize {
        let mut n: u64 = 0;
        while self.inner.pop().is_some() {
            n += 1;
        }
        if n > 0 {
            self.stats.dropped_tx.fetch_add(n, Ordering::Relaxed);
        }
        n as usize
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn stats(&self) -> &Arc<EndpointStats> {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make(cap: usize) -> (TxQueue, Arc<EndpointStats>) {
        let stats = Arc::new(EndpointStats::new());
        (TxQueue::new(cap, stats.clone()), stats)
    }

    #[test]
    fn push_under_capacity_no_evict() {
        let (q, stats) = make(4);
        assert!(!q.push(Bytes::from_static(b"a")));
        assert!(!q.push(Bytes::from_static(b"b")));
        assert_eq!(q.len(), 2);
        assert_eq!(stats.dropped_tx.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn push_over_capacity_evicts_head() {
        let (q, stats) = make(2);
        q.push(Bytes::from_static(b"a"));
        q.push(Bytes::from_static(b"b"));
        let displaced = q.push(Bytes::from_static(b"c"));
        assert!(displaced);
        assert_eq!(stats.dropped_tx.load(Ordering::Relaxed), 1);
        assert_eq!(q.pop().as_deref(), Some(b"b" as &[u8]));
        assert_eq!(q.pop().as_deref(), Some(b"c" as &[u8]));
        assert!(q.pop().is_none());
    }

    #[test]
    fn drain_and_discard_counts() {
        let (q, stats) = make(4);
        q.push(Bytes::from_static(b"a"));
        q.push(Bytes::from_static(b"b"));
        q.push(Bytes::from_static(b"c"));
        let drained = q.drain_and_discard();
        assert_eq!(drained, 3);
        assert!(q.is_empty());
        assert_eq!(stats.dropped_tx.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn drain_empty_increments_nothing() {
        let (q, stats) = make(4);
        assert_eq!(q.drain_and_discard(), 0);
        assert_eq!(stats.dropped_tx.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn capacity_at_least_one() {
        let stats = Arc::new(EndpointStats::new());
        let q = TxQueue::new(0, stats);
        assert!(q.capacity() >= 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_for_push_wakes_on_push() {
        let (q, _) = make(2);
        let waiter = {
            let q = q.clone();
            tokio::spawn(async move {
                q.wait_for_push().await;
                q.pop()
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        q.push(Bytes::from_static(b"x"));
        let popped = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter timed out")
            .expect("waiter task panicked");
        assert_eq!(popped.as_deref(), Some(b"x" as &[u8]));
    }
}
