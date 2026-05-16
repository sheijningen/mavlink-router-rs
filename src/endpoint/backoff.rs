use std::time::Duration;

/// Capped-exponential reconnect backoff with ±20% jitter, shared by `tcpc:`
/// reconnect attempts and `tcps:` initial-bind retries (per CLAUDE.md
/// "Initial bind/dial failure path"). Each call to [`next_delay`] returns the
/// delay to wait before the next attempt (jittered) and advances the internal
/// state toward the cap; [`reset`] returns to the initial value after a
/// successful connect / bind.
///
/// The jitter PRNG is a tiny xorshift64 seeded once from the wall clock at
/// construction — sufficient to break up thundering-herd reconnects across
/// endpoints without pulling in a `rand` dependency.
///
/// [`next_delay`]: Backoff::next_delay
/// [`reset`]: Backoff::reset
#[derive(Debug)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    current: Duration,
    rng: Xorshift,
}

impl Backoff {
    pub fn new(initial_ms: u64, max_ms: u64) -> Self {
        let initial_ms = initial_ms.max(1);
        let max_ms = max_ms.max(initial_ms);
        let initial = Duration::from_millis(initial_ms);
        Self {
            initial,
            max: Duration::from_millis(max_ms),
            current: initial,
            rng: Xorshift::seeded_from_clock(),
        }
    }

    pub fn reset(&mut self) {
        self.current = self.initial;
    }

    /// Sleep duration to apply before the next attempt: the current step,
    /// scaled by a random factor in `[0.8, 1.2]`. Advances `current` toward
    /// `max` for the *following* attempt.
    pub fn next_delay(&mut self) -> Duration {
        let base_ms = self.current.as_millis() as u64;
        let jitter_range = base_ms / 5; // 20%
        let span = jitter_range * 2 + 1;
        let r = (self.rng.next() % span) as i64 - jitter_range as i64;
        let jittered_ms = (base_ms as i64 + r).max(1) as u64;
        let delay = Duration::from_millis(jittered_ms);
        self.current = (self.current.saturating_mul(2)).min(self.max);
        delay
    }

    /// The undelayed step the next call to [`next_delay`] would use as its
    /// jitter base — exposed for tests and metrics.
    #[allow(dead_code)]
    pub fn current_step(&self) -> Duration {
        self.current
    }
}

/// Minimal xorshift64 — used only to spread reconnect jitter, not for any
/// cryptographic purpose.
#[derive(Debug)]
struct Xorshift(u64);

impl Xorshift {
    fn seeded_from_clock() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0xdead_beef_cafe_babe);
        Self(nanos | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_step_matches_initial_ms() {
        let b = Backoff::new(250, 30_000);
        assert_eq!(b.current_step(), Duration::from_millis(250));
    }

    #[test]
    fn next_delay_within_jitter_band() {
        // Force a large enough base that the jitter band is non-degenerate.
        let mut b = Backoff::new(1000, 30_000);
        for _ in 0..32 {
            let d = b.next_delay();
            // Reset back so we always test the same base.
            b.reset();
            assert!(
                d >= Duration::from_millis(800) && d <= Duration::from_millis(1200),
                "delay {d:?} outside ±20% band of 1000ms"
            );
        }
    }

    #[test]
    fn next_delay_doubles_and_caps() {
        let mut b = Backoff::new(100, 800);
        let _ = b.next_delay(); // base 100 → current advances to 200
        assert_eq!(b.current_step(), Duration::from_millis(200));
        let _ = b.next_delay(); // → 400
        assert_eq!(b.current_step(), Duration::from_millis(400));
        let _ = b.next_delay(); // → 800 (max)
        assert_eq!(b.current_step(), Duration::from_millis(800));
        let _ = b.next_delay(); // → still 800
        assert_eq!(b.current_step(), Duration::from_millis(800));
    }

    #[test]
    fn reset_returns_to_initial() {
        let mut b = Backoff::new(100, 800);
        for _ in 0..10 {
            let _ = b.next_delay();
        }
        assert_eq!(b.current_step(), Duration::from_millis(800));
        b.reset();
        assert_eq!(b.current_step(), Duration::from_millis(100));
    }

    #[test]
    fn max_below_initial_is_clamped_to_initial() {
        let b = Backoff::new(500, 100);
        assert_eq!(b.current_step(), Duration::from_millis(500));
    }

    #[test]
    fn zero_initial_is_floored() {
        let b = Backoff::new(0, 30_000);
        assert_eq!(b.current_step(), Duration::from_millis(1));
    }

    #[test]
    fn xorshift_does_not_get_stuck_on_zero() {
        let mut rng = Xorshift(1);
        let mut seen_distinct = 0;
        let mut last = 0u64;
        for _ in 0..16 {
            let n = rng.next();
            assert_ne!(n, 0, "xorshift must never return 0 from non-zero state");
            if n != last {
                seen_distinct += 1;
                last = n;
            }
        }
        assert!(seen_distinct > 4, "rng seems degenerate");
    }
}
