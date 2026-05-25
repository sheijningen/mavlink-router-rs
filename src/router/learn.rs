//! Per-endpoint learn table.
//!
//! Each routing endpoint maintains a flat fixed-capacity table of
//! `(sysid, compid) → last_seen` populated by every valid inbound frame.
//! The router uses the table for two routing checks: loop prevention
//! (skip destinations whose learn-set contains the source identity) and
//! targeted-match (admit a targeted frame only when the destination has
//! learned the target identity).
//!
//! CLAUDE.md mandates: fixed-capacity flat structure (no `HashMap`), LRU
//! eviction by oldest `last_seen` on insert when full. Default capacity
//! 32. Endpoints in the same `?group=` share a single learn table via
//! [`super::group::GroupRegistry`]; this module's `LearnTable` is the
//! per-endpoint variant used when no group is set.

use tokio::time::Instant;

use crate::mavlink::frame::NodeId;

#[derive(Debug, Clone, Copy)]
struct LearnEntry {
    node: NodeId,
    last_seen: Instant,
}

/// Per-endpoint learned-source set.
#[derive(Debug)]
pub struct LearnTable {
    entries: Vec<LearnEntry>,
    capacity: usize,
}

impl LearnTable {
    /// Construct an empty table with the given capacity. A defaulted-down
    /// value of `0` is clamped to `1` so learning is never silently
    /// disabled — a misconfigured cap should still admit one entry.
    pub fn new(capacity: usize) -> Self {
        let clamped = capacity.max(1);
        Self {
            entries: Vec::with_capacity(clamped),
            capacity: clamped,
        }
    }

    /// Record a sighting of `node` at time `now`. Returns `true` if a new
    /// entry was inserted (so the router can sync the `learn_entries` stats
    /// counter), `false` if an existing entry was refreshed. Eviction at
    /// capacity drops the entry with the oldest `last_seen`.
    pub fn touch(&mut self, node: NodeId, now: Instant) -> bool {
        if let Some(entry) = self.find_mut(node) {
            entry.last_seen = now;
            return false;
        }
        if self.entries.len() >= self.capacity {
            self.evict_oldest();
        }
        self.entries.push(LearnEntry {
            node,
            last_seen: now,
        });
        true
    }

    /// True when `node` has been learned. Drives both the loop-prevention
    /// skip and the fully-targeted-match path.
    pub fn contains(&self, node: NodeId) -> bool {
        self.find(node).is_some()
    }

    /// True when any compid for the given sysid has been learned. Drives
    /// the half-target path (`target_comp == 0`).
    pub fn contains_sys(&self, sys: u8) -> bool {
        self.entries.iter().any(|entry| entry.node.sys == sys)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    fn find(&self, node: NodeId) -> Option<&LearnEntry> {
        self.entries.iter().find(|entry| entry.node == node)
    }

    fn find_mut(&mut self, node: NodeId) -> Option<&mut LearnEntry> {
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
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn now_plus(offset: Duration) -> Instant {
        Instant::now() + offset
    }

    #[test]
    fn capacity_zero_clamped_to_one() {
        let table = LearnTable::new(0);
        assert_eq!(table.capacity(), 1);
    }

    #[test]
    fn touch_new_returns_true_then_false() {
        let mut table = LearnTable::new(4);
        assert!(table.touch(NodeId::new(1, 1), now_plus(Duration::ZERO)));
        assert_eq!(table.len(), 1);
        assert!(!table.touch(NodeId::new(1, 1), now_plus(Duration::from_secs(1))));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn contains_distinguishes_compids() {
        let mut table = LearnTable::new(4);
        table.touch(NodeId::new(1, 1), now_plus(Duration::ZERO));
        table.touch(NodeId::new(1, 2), now_plus(Duration::ZERO));
        assert!(table.contains(NodeId::new(1, 1)));
        assert!(table.contains(NodeId::new(1, 2)));
        assert!(!table.contains(NodeId::new(1, 3)));
        assert!(!table.contains(NodeId::new(2, 1)));
    }

    #[test]
    fn contains_sys_ignores_compid() {
        let mut table = LearnTable::new(4);
        table.touch(NodeId::new(7, 199), now_plus(Duration::ZERO));
        assert!(table.contains_sys(7));
        assert!(!table.contains_sys(8));
    }

    #[test]
    fn capacity_lru_evicts_oldest_last_seen() {
        let mut table = LearnTable::new(3);
        table.touch(NodeId::new(1, 1), now_plus(Duration::from_secs(0))); // oldest
        table.touch(NodeId::new(2, 1), now_plus(Duration::from_secs(10)));
        table.touch(NodeId::new(3, 1), now_plus(Duration::from_secs(20)));
        assert_eq!(table.len(), 3);

        // Inserting a fourth identity must evict the oldest (sysid=1).
        let inserted = table.touch(NodeId::new(4, 1), now_plus(Duration::from_secs(30)));
        assert!(inserted);
        assert_eq!(table.len(), 3);
        assert!(
            !table.contains(NodeId::new(1, 1)),
            "oldest entry should have been evicted"
        );
        assert!(table.contains(NodeId::new(2, 1)));
        assert!(table.contains(NodeId::new(3, 1)));
        assert!(table.contains(NodeId::new(4, 1)));
    }

    #[test]
    fn touch_return_value_correct_after_capacity_eviction() {
        // After capacity-induced eviction, `touch` must still return `false`
        // for a refreshed survivor (entry existed) and `true` for a re-inserted
        // previously-evicted identity (fresh slot). Guards against a regression
        // where the find-then-evict-then-insert ordering returned `true`
        // unconditionally post-eviction.
        let mut table = LearnTable::new(3);
        table.touch(NodeId::new(1, 1), now_plus(Duration::from_secs(0)));
        table.touch(NodeId::new(2, 1), now_plus(Duration::from_secs(10)));
        table.touch(NodeId::new(3, 1), now_plus(Duration::from_secs(20)));
        // Refresh oldest, then trigger an eviction of (2, 1).
        table.touch(NodeId::new(1, 1), now_plus(Duration::from_secs(30)));
        table.touch(NodeId::new(4, 1), now_plus(Duration::from_secs(40)));
        assert_eq!(table.len(), 3);
        // (1, 1) survived — touch must return false.
        assert!(!table.touch(NodeId::new(1, 1), now_plus(Duration::from_secs(50))));
        // (2, 1) was evicted — re-inserting is a fresh slot, touch returns true.
        assert!(table.touch(NodeId::new(2, 1), now_plus(Duration::from_secs(60))));
    }

    #[test]
    fn refreshed_entry_survives_capacity_pressure() {
        // Touching the oldest entry must move it ahead of the others so
        // the next eviction takes someone else. This is the LRU contract.
        let mut table = LearnTable::new(3);
        table.touch(NodeId::new(1, 1), now_plus(Duration::from_secs(0)));
        table.touch(NodeId::new(2, 1), now_plus(Duration::from_secs(10)));
        table.touch(NodeId::new(3, 1), now_plus(Duration::from_secs(20)));
        // Refresh sysid=1 to "now" — it's the freshest after this.
        assert!(!table.touch(NodeId::new(1, 1), now_plus(Duration::from_secs(30))));
        // Inserting a fourth must evict sysid=2 (now the oldest), not sysid=1.
        table.touch(NodeId::new(4, 1), now_plus(Duration::from_secs(40)));
        assert!(table.contains(NodeId::new(1, 1)));
        assert!(!table.contains(NodeId::new(2, 1)));
        assert!(table.contains(NodeId::new(3, 1)));
        assert!(table.contains(NodeId::new(4, 1)));
    }

    #[test]
    fn is_empty_and_len_track_inserts() {
        let mut table = LearnTable::new(4);
        assert!(table.entries.is_empty());
        assert_eq!(table.len(), 0);
        table.touch(NodeId::new(1, 1), now_plus(Duration::ZERO));
        assert!(!table.entries.is_empty());
        assert_eq!(table.len(), 1);
    }
}
