//! Frame-hash dedup window — single global structure owned by the router.
//! Redundant uplink delivering the same frame on two different routing endpoints
//! still collides on the second arrival.
//!
//! `HashSet<u64>` for O(1) lookup + parallel FIFO
//! ring for TTL / capacity eviction. Disabled when `dedup_ms == 0`.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;
use twox_hash::XxHash3_64;

/// Frame hash + the wall-clock deadline by which it expires from the
/// window. Stored in a [`VecDeque`] to make TTL eviction at the head and
/// oldest-first eviction at capacity both O(1).
#[derive(Debug, Clone, Copy)]
struct Entry {
    hash: u64,
    deadline: Instant,
}

/// Bounded TTL-evicting frame-hash window. Disabled when constructed with
/// `ttl == Duration::ZERO`; in that state `check_and_insert` is a no-op
/// that always returns `false` and the underlying ring + set stay empty.
pub struct DedupWindow {
    ring: VecDeque<Entry>,
    set: HashSet<u64>,
    capacity: usize,
    ttl: Duration,
}

impl DedupWindow {
    /// Construct a window with the given TTL and capacity. A `ttl` of
    /// `Duration::ZERO` disables the window.
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        let clamped = capacity.max(1);
        Self {
            ring: VecDeque::with_capacity(if ttl.is_zero() { 0 } else { clamped }),
            set: HashSet::with_capacity(if ttl.is_zero() { 0 } else { clamped }),
            capacity: clamped,
            ttl,
        }
    }

    /// `true` when the window is enabled (i.e. `dedup_ms > 0`).
    #[inline]
    pub fn is_enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    /// Hash `frame` and check the window. Returns `true` if a live entry
    /// matches (drop the frame); returns `false` after inserting a fresh
    /// entry. The window is touched at most once per frame, including
    /// the TTL-expiry sweep over the head of the ring.
    ///
    /// A disabled window (`ttl == 0`) is a no-op that always returns
    /// `false` — the hot path stays at zero allocations and zero hashing.
    pub fn check_and_insert(&mut self, frame: &Bytes, now: Instant) -> bool {
        if !self.is_enabled() {
            return false;
        }
        self.expire_stale(now);
        let hash = XxHash3_64::oneshot(frame);
        if self.set.contains(&hash) {
            return true;
        }
        if self.ring.len() >= self.capacity {
            self.evict_oldest();
        }
        self.ring.push_back(Entry {
            hash,
            deadline: now + self.ttl,
        });
        self.set.insert(hash);
        false
    }

    /// Drop entries from the ring head whose deadline is at or before
    /// `now`. Constant-time amortised: each entry is admitted once and
    /// expired once.
    fn expire_stale(&mut self, now: Instant) {
        while let Some(front) = self.ring.front() {
            if front.deadline > now {
                break;
            }
            let stale = self.ring.pop_front().expect("front was Some");
            self.set.remove(&stale.hash);
        }
    }

    fn evict_oldest(&mut self) {
        let Some(victim) = self.ring.pop_front() else {
            return;
        };
        self.set.remove(&victim.hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(millis: u64) -> Instant {
        Instant::now() + Duration::from_millis(millis)
    }

    fn make_frame(bytes: &'static [u8]) -> Bytes {
        Bytes::from_static(bytes)
    }

    #[test]
    fn disabled_window_never_hits_and_stays_empty() {
        let mut window = DedupWindow::new(Duration::ZERO, 16);
        assert!(!window.is_enabled());
        for _ in 0..10 {
            assert!(!window.check_and_insert(&make_frame(b"abc"), Instant::now()));
        }
        assert_eq!(window.ring.len(), 0);
    }

    #[test]
    fn first_insert_misses_second_identical_hits() {
        let mut window = DedupWindow::new(Duration::from_millis(100), 16);
        let frame = make_frame(b"hello");
        assert!(!window.check_and_insert(&frame, at(0)));
        assert!(window.check_and_insert(&frame, at(0)));
    }

    #[test]
    fn distinct_frames_both_admitted() {
        let mut window = DedupWindow::new(Duration::from_millis(100), 16);
        assert!(!window.check_and_insert(&make_frame(b"frame-a"), at(0)));
        assert!(!window.check_and_insert(&make_frame(b"frame-b"), at(0)));
        assert_eq!(window.ring.len(), 2);
    }

    #[test]
    fn ttl_expiry_drops_old_entries_then_admits_repeat() {
        // After TTL elapses, the same frame must be admitted again.
        let mut window = DedupWindow::new(Duration::from_millis(50), 16);
        let frame = make_frame(b"telemetry");
        assert!(!window.check_and_insert(&frame, at(0)));
        // 25ms — still within window
        assert!(window.check_and_insert(&frame, at(25)));
        // 55ms — past TTL; the next call sees the entry as expired before
        // hashing the new arrival, then admits.
        assert!(!window.check_and_insert(&frame, at(55)));
    }

    #[test]
    fn capacity_oldest_evicted_when_full() {
        let mut window = DedupWindow::new(Duration::from_millis(1000), 2);
        assert!(!window.check_and_insert(&make_frame(b"a"), at(0)));
        assert!(!window.check_and_insert(&make_frame(b"b"), at(10)));
        // Inserting a third must evict the oldest ("a"). Both b and c
        // remain live; a is gone.
        assert!(!window.check_and_insert(&make_frame(b"c"), at(20)));
        assert_eq!(window.ring.len(), 2);
        // "a" was evicted, so re-inserting it is a miss again.
        assert!(!window.check_and_insert(&make_frame(b"a"), at(30)));
        assert_eq!(window.ring.len(), 2, "a's admission evicted b");
        // "b" is what got evicted by the previous insert.
        assert!(!window.check_and_insert(&make_frame(b"b"), at(40)));
    }

    #[test]
    fn ttl_expiry_releases_set_membership_for_replay() {
        // Regression guard: expired hashes must leave the HashSet too,
        // not just the ring.
        let mut window = DedupWindow::new(Duration::from_millis(10), 16);
        let frame = make_frame(b"repeat");
        assert!(!window.check_and_insert(&frame, at(0)));
        // 30ms — well past TTL.
        assert!(!window.check_and_insert(&frame, at(30)));
        assert_eq!(window.ring.len(), 1);
    }

    #[test]
    fn capacity_zero_is_clamped_to_one() {
        let mut window = DedupWindow::new(Duration::from_millis(50), 0);
        assert!(!window.check_and_insert(&make_frame(b"a"), at(0)));
        assert!(!window.check_and_insert(&make_frame(b"b"), at(0)));
        assert_eq!(window.ring.len(), 1);
    }

    #[test]
    fn hit_is_content_based_not_pointer_based() {
        // Redundant uplinks produce independent allocations of identical
        // content — dedup must collide on content, not buffer identity.
        let mut window = DedupWindow::new(Duration::from_millis(100), 16);
        let content: &[u8] = &[0xFD, 9, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2, 3, 4];
        let first = Bytes::copy_from_slice(content);
        let second = Bytes::copy_from_slice(content);
        assert_ne!(
            first.as_ptr(),
            second.as_ptr(),
            "test setup requires independent allocations"
        );
        assert!(!window.check_and_insert(&first, at(0)));
        assert!(window.check_and_insert(&second, at(0)));
    }
}
