//! Frame-hash dedup window — single global structure owned by the router.
//!
//! Per CLAUDE.md (locked decision "Dedup hash + storage"):
//!
//! - **xxh3-64** of the framed bytes (header + payload + CRC + optional
//!   signature trailer) is the key. The hash is non-cryptographic; the
//!   redundant-uplink use case it protects (LTE + RFD900 each delivering
//!   the same vehicle frame) doesn't need cryptographic strength.
//! - **Single global window** owned by the router — every source endpoint
//!   inserts into and hits the same window. That's what catches the
//!   second copy from a redundant uplink even though the two arrivals
//!   come in on different routing endpoints.
//! - **`HashSet<u64>` for O(1) membership** paired with a parallel fixed-
//!   capacity FIFO ring of `(u64 hash, Instant deadline)`. The ring
//!   drives eviction (TTL expiry from the head; oldest-first when at
//!   capacity); the set drives lookup.
//! - **TTL = `dedup_ms`** from CLI / TOML; the window is *disabled* when
//!   `dedup_ms == 0` (the default) — no allocation, no hashing on the
//!   hot path.
//! - **On hit:** drop the frame and bump `dedup_drops` on the source
//!   endpoint. Pre-learn, pre-dispatch — so a sniffer destination sees
//!   the *post-dedup* frame set (locked decision "Sniffer + dedup
//!   ordering").
//!
//! The router task is the sole owner; there is no synchronisation.

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
    /// `Duration::ZERO` disables the window (CLAUDE.md: "`dedup_ms == 0`
    /// turns dedup off").
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            ring: VecDeque::with_capacity(if ttl.is_zero() { 0 } else { cap }),
            set: HashSet::with_capacity(if ttl.is_zero() { 0 } else { cap }),
            capacity: cap,
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

    /// Current live-entry count. Used by tests; not relied on by the hot
    /// path.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
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

    fn at(ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(ms)
    }

    fn make_frame(bytes: &'static [u8]) -> Bytes {
        Bytes::from_static(bytes)
    }

    #[test]
    fn disabled_window_never_hits_and_stays_empty() {
        let mut w = DedupWindow::new(Duration::ZERO, 16);
        assert!(!w.is_enabled());
        for _ in 0..10 {
            assert!(!w.check_and_insert(&make_frame(b"abc"), Instant::now()));
        }
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn first_insert_misses_second_identical_hits() {
        let mut w = DedupWindow::new(Duration::from_millis(100), 16);
        let f = make_frame(b"hello");
        assert!(!w.check_and_insert(&f, at(0)));
        assert!(w.check_and_insert(&f, at(0)));
    }

    #[test]
    fn distinct_frames_both_admitted() {
        let mut w = DedupWindow::new(Duration::from_millis(100), 16);
        assert!(!w.check_and_insert(&make_frame(b"frame-a"), at(0)));
        assert!(!w.check_and_insert(&make_frame(b"frame-b"), at(0)));
        assert_eq!(w.len(), 2);
    }

    #[test]
    fn ttl_expiry_drops_old_entries_then_admits_repeat() {
        // After TTL elapses, the same frame must be admitted again.
        let mut w = DedupWindow::new(Duration::from_millis(50), 16);
        let f = make_frame(b"telemetry");
        assert!(!w.check_and_insert(&f, at(0)));
        // 25ms — still within window
        assert!(w.check_and_insert(&f, at(25)));
        // 55ms — past TTL; the next call sees the entry as expired before
        // hashing the new arrival, then admits.
        assert!(!w.check_and_insert(&f, at(55)));
    }

    #[test]
    fn capacity_oldest_evicted_when_full() {
        let mut w = DedupWindow::new(Duration::from_millis(1000), 2);
        assert!(!w.check_and_insert(&make_frame(b"a"), at(0)));
        assert!(!w.check_and_insert(&make_frame(b"b"), at(10)));
        // Inserting a third must evict the oldest ("a"). Both b and c
        // remain live; a is gone.
        assert!(!w.check_and_insert(&make_frame(b"c"), at(20)));
        assert_eq!(w.len(), 2);
        // "a" was evicted, so re-inserting it is a miss again.
        assert!(!w.check_and_insert(&make_frame(b"a"), at(30)));
        assert_eq!(w.len(), 2, "a's admission evicted b");
        // "b" is what got evicted by the previous insert.
        assert!(!w.check_and_insert(&make_frame(b"b"), at(40)));
    }

    #[test]
    fn ttl_expiry_releases_set_membership_for_replay() {
        // Regression guard: TTL-expired hashes must leave the HashSet, not
        // just the ring — otherwise a "replay after expiry" frame would be
        // a false positive.
        let mut w = DedupWindow::new(Duration::from_millis(10), 16);
        let f = make_frame(b"repeat");
        assert!(!w.check_and_insert(&f, at(0)));
        // 30ms — well past TTL.
        assert!(!w.check_and_insert(&f, at(30)));
        assert_eq!(w.len(), 1);
    }

    #[test]
    fn capacity_zero_is_clamped_to_one() {
        let mut w = DedupWindow::new(Duration::from_millis(50), 0);
        assert!(!w.check_and_insert(&make_frame(b"a"), at(0)));
        // Capacity is 1, so admitting "b" evicts "a".
        assert!(!w.check_and_insert(&make_frame(b"b"), at(0)));
        assert_eq!(w.len(), 1);
    }

    #[test]
    fn hit_is_content_based_not_pointer_based() {
        // The redundant-uplink use case the window exists to serve delivers
        // the same frame on two different transports — each producing its
        // own backing allocation. Dedup must collide on identical content
        // regardless of which buffer the bytes were copied from.
        let mut w = DedupWindow::new(Duration::from_millis(100), 16);
        let content: &[u8] = &[0xFD, 9, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2, 3, 4];
        let a = Bytes::copy_from_slice(content);
        let b = Bytes::copy_from_slice(content);
        assert_ne!(
            a.as_ptr(),
            b.as_ptr(),
            "test setup requires independent allocations"
        );
        assert!(!w.check_and_insert(&a, at(0)));
        assert!(w.check_and_insert(&b, at(0)));
    }
}
