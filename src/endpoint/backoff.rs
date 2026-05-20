use std::fmt::Display;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::wait_or_cancel;

/// Capped-exponential reconnect backoff with ±20% jitter, shared by `tcpc:`
/// reconnect attempts and `tcps:` initial-bind retries
#[derive(Debug)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    current: Duration,
    rng: Xorshift,
}

/// Outcome of [`bind_with_backoff`]. `Bound` carries the successfully-opened
/// resource (socket, listener); `Cancelled` means the cancel token tripped
/// while waiting or before the next attempt — the caller should unwind
/// without attempting further work.
pub enum BindOutcome<T> {
    Bound(T),
    Cancelled,
}

/// Drive a fallible bind/open through `Backoff`'s capped-exponential curve
/// until it succeeds or the cancel token trips. On each failure, logs at
/// WARN with `<label> bind failed; retrying after backoff` and the formatted
/// `addr` for context, then sleeps `backoff.next_delay()` against the cancel
/// token. Resets the backoff on first success so the next failure starts at
/// the floor again. Used by `tcps:` / `udps:` / `udpc:` to share one bind-
/// retry shape (CLAUDE.md "Bind/open failure at startup is not fatal").
pub async fn bind_with_backoff<T, E, F>(
    cancel: &CancellationToken,
    backoff: &mut Backoff,
    label: &str,
    addr: impl Display,
    mut bind_fn: F,
) -> BindOutcome<T>
where
    F: FnMut() -> Result<T, E>,
    E: Display,
{
    loop {
        if cancel.is_cancelled() {
            return BindOutcome::Cancelled;
        }
        match bind_fn() {
            Ok(bound) => {
                backoff.reset();
                return BindOutcome::Bound(bound);
            }
            Err(err) => {
                warn!(error = %err, addr = %addr, "{label} bind failed; retrying after backoff");
                if !wait_or_cancel(cancel, backoff.next_delay()).await {
                    return BindOutcome::Cancelled;
                }
            }
        }
    }
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

    /// Sleep duration to apply before the next attempt
    pub fn next_delay(&mut self) -> Duration {
        let base_ms = self.current.as_millis() as u64;
        let jitter_range = base_ms / 5; // 20%
        let span = jitter_range * 2 + 1;
        let jitter = (self.rng.next() % span) as i64 - jitter_range as i64;
        let jittered_ms = (base_ms as i64 + jitter).max(1) as u64;
        let delay = Duration::from_millis(jittered_ms);
        self.current = (self.current.saturating_mul(2)).min(self.max);
        delay
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
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or(0xdead_beef_cafe_babe);
        Self(nanos | 1)
    }

    fn next(&mut self) -> u64 {
        let mut state = self.0;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.0 = state;
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_step_matches_initial_ms() {
        let backoff = Backoff::new(250, 30_000);
        assert_eq!(backoff.current, Duration::from_millis(250));
    }

    #[test]
    fn next_delay_within_jitter_band() {
        // Force a large enough base that the jitter band is non-degenerate.
        let mut backoff = Backoff::new(1000, 30_000);
        for _ in 0..32 {
            let delay = backoff.next_delay();
            // Reset back so we always test the same base.
            backoff.reset();
            assert!(
                delay >= Duration::from_millis(800) && delay <= Duration::from_millis(1200),
                "delay {delay:?} outside ±20% band of 1000ms"
            );
        }
    }

    #[test]
    fn next_delay_jitter_band_holds_after_advance_and_at_cap() {
        // The initial-step jitter test re-resets between calls; this one
        // exercises the doubling and cap-saturation arithmetic by sampling
        // the band on a doubled base and on the saturated cap.
        let mut backoff = Backoff::new(1000, 4000);
        let _ = backoff.next_delay(); // current advances to 2000ms
        assert_eq!(backoff.current, Duration::from_millis(2000));
        for _ in 0..32 {
            let held = backoff.current;
            let delay = backoff.next_delay();
            assert!(
                delay >= Duration::from_millis(1600) && delay <= Duration::from_millis(2400),
                "delay {delay:?} outside ±20% of 2000ms"
            );
            backoff.current = held;
        }
        // Walk forward until saturation, then sample at the cap.
        while backoff.current < Duration::from_millis(4000) {
            let _ = backoff.next_delay();
        }
        assert_eq!(backoff.current, Duration::from_millis(4000));
        for _ in 0..32 {
            let held = backoff.current;
            let delay = backoff.next_delay();
            assert!(
                delay >= Duration::from_millis(3200) && delay <= Duration::from_millis(4800),
                "delay {delay:?} outside ±20% of 4000ms cap"
            );
            backoff.current = held;
        }
    }

    #[test]
    fn next_delay_doubles_and_caps() {
        let mut backoff = Backoff::new(100, 800);
        let _ = backoff.next_delay(); // base 100 → current advances to 200
        assert_eq!(backoff.current, Duration::from_millis(200));
        let _ = backoff.next_delay(); // → 400
        assert_eq!(backoff.current, Duration::from_millis(400));
        let _ = backoff.next_delay(); // → 800 (max)
        assert_eq!(backoff.current, Duration::from_millis(800));
        let _ = backoff.next_delay(); // → still 800
        assert_eq!(backoff.current, Duration::from_millis(800));
    }

    #[test]
    fn reset_returns_to_initial() {
        let mut backoff = Backoff::new(100, 800);
        for _ in 0..10 {
            let _ = backoff.next_delay();
        }
        assert_eq!(backoff.current, Duration::from_millis(800));
        backoff.reset();
        assert_eq!(backoff.current, Duration::from_millis(100));
    }

    #[test]
    fn max_below_initial_is_clamped_to_initial() {
        let backoff = Backoff::new(500, 100);
        assert_eq!(backoff.current, Duration::from_millis(500));
    }

    #[test]
    fn zero_initial_is_floored() {
        let backoff = Backoff::new(0, 30_000);
        assert_eq!(backoff.current, Duration::from_millis(1));
    }

    #[test]
    fn xorshift_does_not_get_stuck_on_zero() {
        let mut rng = Xorshift(1);
        let mut seen_distinct = 0;
        let mut last = 0u64;
        for _ in 0..16 {
            let value = rng.next();
            assert_ne!(value, 0, "xorshift must never return 0 from non-zero state");
            if value != last {
                seen_distinct += 1;
                last = value;
            }
        }
        assert!(seen_distinct > 4, "rng seems degenerate");
    }
}
