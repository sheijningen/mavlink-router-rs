use std::sync::atomic::{AtomicU64, Ordering};

/// Per-endpoint cumulative-since-start counters, shared between the reader,
/// writer, and router tasks via `Arc`. Each counter is an independent atomic
/// so writers on the hot path never contend on a single lock; the router
/// snapshots all fields at each stats interval and forwards a plain-data line
/// to the dedicated stats task.
#[derive(Debug, Default)]
pub struct EndpointStats {
    pub rx_frames: AtomicU64,
    pub tx_frames: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub tx_bytes: AtomicU64,
    pub dropped_tx: AtomicU64,
    pub crc_errors: AtomicU64,
    pub resync_bytes: AtomicU64,
    pub in_filter_drops: AtomicU64,
    pub out_filter_drops: AtomicU64,
    pub dedup_drops: AtomicU64,
    pub rx_lost_est: AtomicU64,
    pub learn_entries: AtomicU64,
}

impl EndpointStats {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn add_rx_frame(&self, bytes: usize) {
        self.rx_frames.fetch_add(1, Ordering::Relaxed);
        self.rx_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_tx_frame(&self, bytes: usize) {
        self.tx_frames.fetch_add(1, Ordering::Relaxed);
        self.tx_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_rx_tx_updates_pairs() {
        let s = EndpointStats::new();
        s.add_rx_frame(12);
        s.add_rx_frame(20);
        s.add_tx_frame(7);
        assert_eq!(s.rx_frames.load(Ordering::Relaxed), 2);
        assert_eq!(s.rx_bytes.load(Ordering::Relaxed), 32);
        assert_eq!(s.tx_frames.load(Ordering::Relaxed), 1);
        assert_eq!(s.tx_bytes.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn default_is_zero() {
        let s = EndpointStats::default();
        assert_eq!(s.rx_frames.load(Ordering::Relaxed), 0);
        assert_eq!(s.dropped_tx.load(Ordering::Relaxed), 0);
        assert_eq!(s.resync_bytes.load(Ordering::Relaxed), 0);
    }
}
