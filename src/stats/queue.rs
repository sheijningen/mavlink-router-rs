//! Bounded drop-oldest line queue between production and the stdout writer.

use std::collections::VecDeque;

use tracing::warn;

use super::line::StatsLine;

/// Queued line tagged with its origin so the bypass path can preferentially
/// evict regular interval lines before touching a synthetic Finalize line.
pub(super) struct QueueEntry {
    pub(super) line: StatsLine,
    pub(super) synthetic: bool,
}

/// Regular interval-tick line: drop-oldest on overflow, increment the
/// rate-limited `stats_dropped` counter.
pub(super) fn enqueue_regular(
    queue: &mut VecDeque<QueueEntry>,
    queue_capacity: usize,
    line: StatsLine,
    total_dropped: &mut u64,
) {
    if queue_capacity == 0 {
        *total_dropped = total_dropped.saturating_add(1);
        return;
    }
    if queue.len() >= queue_capacity {
        queue.pop_front();
        *total_dropped = total_dropped.saturating_add(1);
    }
    queue.push_back(QueueEntry {
        line,
        synthetic: false,
    });
}

/// Authoritative end-state lines never silently disappear: evict the oldest
/// regular interval line to make room. If every queued entry is itself a
/// synthetic line, drop the oldest and WARN with the lost endpoint name.
pub(super) fn enqueue_synthetic(
    queue: &mut VecDeque<QueueEntry>,
    queue_capacity: usize,
    line: StatsLine,
    total_dropped: &mut u64,
) {
    if queue_capacity == 0 {
        warn!(
            endpoint = %line.endpoint(),
            "stats Finalize line dropped: queue capacity is zero"
        );
        *total_dropped = total_dropped.saturating_add(1);
        return;
    }
    if queue.len() >= queue_capacity {
        if let Some(pos) = queue.iter().position(|entry| !entry.synthetic) {
            queue.remove(pos);
            *total_dropped = total_dropped.saturating_add(1);
        } else if let Some(evicted) = queue.pop_front() {
            warn!(
                evicted_endpoint = %evicted.line.endpoint(),
                replacing_endpoint = %line.endpoint(),
                "stats Finalize line evicted by another Finalize line; terminal state lost"
            );
            *total_dropped = total_dropped.saturating_add(1);
        }
    }
    queue.push_back(QueueEntry {
        line,
        synthetic: true,
    });
}

pub(super) fn warn_on_new_drops(total_dropped: u64, last_warned: &mut u64) {
    if total_dropped > *last_warned {
        let delta = total_dropped - *last_warned;
        warn!(
            stats_dropped = total_dropped,
            delta_this_interval = delta,
            "stats queue at capacity; oldest line(s) dropped"
        );
        *last_warned = total_dropped;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::stats::EndpointStats;
    use crate::stats::line::build_line;

    fn make_line(endpoint: &str) -> StatsLine {
        build_line(endpoint, &EndpointStats::default(), "t".to_string(), true)
    }

    fn entry_endpoints(queue: &VecDeque<QueueEntry>) -> Vec<&str> {
        queue.iter().map(|entry| entry.line.endpoint()).collect()
    }

    #[test]
    fn regular_enqueue_drops_oldest_when_capacity_exceeded() {
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_regular(&mut queue, 2, make_line("a"), &mut dropped);
        enqueue_regular(&mut queue, 2, make_line("b"), &mut dropped);
        enqueue_regular(&mut queue, 2, make_line("c"), &mut dropped);
        assert_eq!(dropped, 1);
        assert_eq!(entry_endpoints(&queue), vec!["b", "c"]);
    }

    #[test]
    fn regular_enqueue_zero_capacity_drops_everything() {
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_regular(&mut queue, 0, make_line("a"), &mut dropped);
        assert_eq!(dropped, 1);
        assert!(queue.is_empty());
    }

    #[test]
    fn synthetic_enqueue_evicts_oldest_regular_first() {
        // Bypass path: a synthetic Finalize line entering a full queue must
        // displace an interval line, not another synthetic line.
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_regular(&mut queue, 3, make_line("reg-a"), &mut dropped);
        enqueue_synthetic(&mut queue, 3, make_line("syn-x"), &mut dropped);
        enqueue_regular(&mut queue, 3, make_line("reg-b"), &mut dropped);
        // Queue is now [reg-a (R), syn-x (S), reg-b (R)] at cap 3.
        enqueue_synthetic(&mut queue, 3, make_line("syn-y"), &mut dropped);
        // syn-y enters via bypass → reg-a (oldest regular) is evicted, syn-x stays.
        assert_eq!(dropped, 1);
        assert_eq!(entry_endpoints(&queue), vec!["syn-x", "reg-b", "syn-y"]);
    }

    #[test]
    fn synthetic_enqueue_falls_back_to_oldest_synthetic_when_no_regular() {
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_synthetic(&mut queue, 2, make_line("syn-a"), &mut dropped);
        enqueue_synthetic(&mut queue, 2, make_line("syn-b"), &mut dropped);
        // Queue is now [syn-a, syn-b] at cap 2 — every entry is synthetic.
        enqueue_synthetic(&mut queue, 2, make_line("syn-c"), &mut dropped);
        // syn-c displaces syn-a (oldest synthetic) and the lost endpoint is
        // named in a dedicated WARN by `enqueue_synthetic`.
        assert_eq!(dropped, 1);
        assert_eq!(entry_endpoints(&queue), vec!["syn-b", "syn-c"]);
    }

    #[test]
    fn synthetic_enqueue_zero_capacity_drops_and_warns() {
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_synthetic(&mut queue, 0, make_line("syn-a"), &mut dropped);
        assert_eq!(dropped, 1);
        assert!(queue.is_empty());
    }

    #[test]
    fn warn_on_new_drops_only_fires_when_total_advances() {
        let mut last = 0u64;
        warn_on_new_drops(0, &mut last);
        assert_eq!(last, 0);
        warn_on_new_drops(3, &mut last);
        assert_eq!(last, 3);
        // Same total → no new WARN, `last` stays put.
        warn_on_new_drops(3, &mut last);
        assert_eq!(last, 3);
        warn_on_new_drops(5, &mut last);
        assert_eq!(last, 5);
    }
}
