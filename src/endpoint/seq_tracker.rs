//! Per-source `(sysid, compid) → last_seq` tracker on each endpoint's reader.
//! Drives `rx_lost_est` from inter-frame gaps; runs after CRC and before
//! In-filter so the counter reflects link quality, not policy.

use tokio::time::Instant;

use crate::mavlink::frame::NodeId;

/// Gap above which the tracker treats the source as restarted (a quarter of
/// the u8 sequence space; large enough to ignore bursty loss, small enough
/// that a backwards-by-N jump is correctly classified as a restart).
pub const DEFAULT_SEQ_GAP_THRESHOLD: u8 = 64;

#[derive(Debug, Clone, Copy)]
struct Entry {
    node: NodeId,
    last_seq: u8,
    last_seen: Instant,
}

/// Per-endpoint, per-source sequence-loss accounting. See module docs.
pub struct SeqTracker {
    entries: Vec<Entry>,
    capacity: usize,
    threshold: u8,
}

impl SeqTracker {
    /// Construct with the given capacity and the default gap-sanity
    /// threshold. A capacity of 0 silently clamps to 1 so the tracker is
    /// never disabled by misconfiguration.
    pub fn new(capacity: usize) -> Self {
        let clamped = capacity.max(1);
        Self {
            entries: Vec::with_capacity(clamped),
            capacity: clamped,
            threshold: DEFAULT_SEQ_GAP_THRESHOLD,
        }
    }

    /// Observe one frame's `(node, seq)` at time `now`. Returns the
    /// inferred number of lost frames between the previous and this
    /// observation — caller adds the return value to `rx_lost_est`.
    ///
    /// - Consecutive (`gap == 0`) → returns 0.
    /// - Small gap (`0 < gap < threshold`) → returns `gap as u32`.
    /// - Large gap (`gap >= threshold`) → treated as restart; returns 0,
    ///   `last_seq` reset to `seq` for the next observation.
    /// - First observation of this `node` → no prior data; returns 0 after
    ///   inserting (with possible LRU eviction).
    pub fn observe(&mut self, node: NodeId, seq: u8, now: Instant) -> u32 {
        if let Some(entry) = self.find_mut(node) {
            let gap = seq.wrapping_sub(entry.last_seq).wrapping_sub(1);
            entry.last_seq = seq;
            entry.last_seen = now;
            if gap > 0 && gap < self.threshold {
                return gap as u32;
            }
            return 0;
        }
        if self.entries.len() >= self.capacity {
            self.evict_oldest();
        }
        self.entries.push(Entry {
            node,
            last_seq: seq,
            last_seen: now,
        });
        0
    }

    fn find_mut(&mut self, node: NodeId) -> Option<&mut Entry> {
        self.entries.iter_mut().find(|entry| entry.node == node)
    }

    fn evict_oldest(&mut self) {
        let Some((index, _)) = self
            .entries
            .iter()
            .enumerate()
            .min_by_key(|(_, entry)| entry.last_seen)
        else {
            return;
        };
        self.entries.swap_remove(index);
    }

    #[cfg(test)]
    pub(crate) fn with_threshold(capacity: usize, threshold: u8) -> Self {
        let clamped = capacity.max(1);
        Self {
            entries: Vec::with_capacity(clamped),
            capacity: clamped,
            threshold,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rstest::rstest;

    use super::*;

    fn at(millis: u64) -> Instant {
        Instant::now() + Duration::from_millis(millis)
    }

    #[test]
    fn first_observation_returns_zero() {
        let mut tracker = SeqTracker::new(4);
        assert_eq!(tracker.observe(NodeId::new(1, 1), 0, at(0)), 0);
        assert_eq!(tracker.entries.len(), 1);
    }

    #[test]
    fn consecutive_seqs_return_zero_no_bump() {
        let mut tracker = SeqTracker::new(4);
        tracker.observe(NodeId::new(1, 1), 10, at(0));
        for seq in 11u8..=15u8 {
            assert_eq!(tracker.observe(NodeId::new(1, 1), seq, at(seq as u64)), 0);
        }
    }

    /// Gap classification under varied `(last_seq, current_seq, threshold)`:
    /// each case primes the tracker with `last_seq`, then observes
    /// `current_seq` and asserts the returned loss count. Covers small gap,
    /// restart-sized gap, duplicate, backward, and both wraparound branches
    /// in one matrix so a single regression in `observe` surfaces every
    /// affected case on the same `cargo test` invocation.
    #[rstest]
    #[case::small_gap(10, 13, DEFAULT_SEQ_GAP_THRESHOLD, 2)]
    #[case::large_gap_classified_as_restart(10, 200, 64, 0)]
    #[case::duplicate_seq_classified_as_restart(50, 50, 64, 0)]
    #[case::backward_seq_classified_as_restart(50, 49, 64, 0)]
    #[case::wraparound_consecutive(255, 0, DEFAULT_SEQ_GAP_THRESHOLD, 0)]
    #[case::wraparound_small_gap(254, 2, DEFAULT_SEQ_GAP_THRESHOLD, 3)]
    fn observe_returns_expected_loss(
        #[case] last_seq: u8,
        #[case] current_seq: u8,
        #[case] threshold: u8,
        #[case] expected_loss: u32,
    ) {
        let mut tracker = SeqTracker::with_threshold(4, threshold);
        tracker.observe(NodeId::new(1, 1), last_seq, at(0));
        assert_eq!(
            tracker.observe(NodeId::new(1, 1), current_seq, at(1)),
            expected_loss
        );
    }

    #[test]
    fn restart_resets_last_seq_to_current() {
        // Companion to the large-gap case above: after a restart-sized gap
        // is observed, `last_seq` must be updated to the post-restart seq —
        // not left pointing at the pre-restart value — so the next
        // consecutive observation is correctly classified.
        let mut tracker = SeqTracker::with_threshold(4, 64);
        tracker.observe(NodeId::new(1, 1), 10, at(0));
        assert_eq!(tracker.observe(NodeId::new(1, 1), 200, at(1)), 0);
        assert_eq!(tracker.observe(NodeId::new(1, 1), 201, at(2)), 0);
    }

    #[test]
    fn distinct_identities_are_independent() {
        let mut tracker = SeqTracker::new(4);
        tracker.observe(NodeId::new(1, 1), 10, at(0));
        tracker.observe(NodeId::new(2, 1), 20, at(0));
        // Each identity tracks its own last_seq.
        assert_eq!(tracker.observe(NodeId::new(1, 1), 11, at(1)), 0);
        assert_eq!(tracker.observe(NodeId::new(2, 1), 22, at(1)), 1); // one lost
    }

    #[test]
    fn lru_eviction_drops_oldest_by_last_seen() {
        let mut tracker = SeqTracker::new(2);
        tracker.observe(NodeId::new(1, 1), 0, at(0));
        tracker.observe(NodeId::new(2, 1), 0, at(10));
        // Insert a third identity — evicts (1, 1), the oldest.
        tracker.observe(NodeId::new(3, 1), 0, at(20));
        assert_eq!(tracker.entries.len(), 2);
        // Now observing (1, 1, 5) — looks like a fresh insert (no prior).
        // Returns 0 even though we'd otherwise infer a gap.
        assert_eq!(tracker.observe(NodeId::new(1, 1), 5, at(30)), 0);
        // And the second observation of (1, 1) after re-insertion behaves
        // like a normal continuation.
        assert_eq!(tracker.observe(NodeId::new(1, 1), 7, at(40)), 1);
    }

    #[test]
    fn capacity_zero_clamps_to_one() {
        let mut tracker = SeqTracker::new(0);
        assert_eq!(tracker.capacity, 1);
        tracker.observe(NodeId::new(1, 1), 0, at(0));
        // Inserting a second identity evicts the first.
        tracker.observe(NodeId::new(2, 1), 0, at(10));
        assert_eq!(tracker.entries.len(), 1);
    }
}
