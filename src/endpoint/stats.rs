use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use crate::mavlink::framer::Framer;

/// User-visible health of one routing endpoint. Stored on
/// [`EndpointStats::state`] as a raw `u8` so the stats task can cheaply
/// `load → match` on a hot interval; the named enum keeps writers honest
/// about which variant they're storing. The u8 encoding is an internal
/// implementation detail — the stats JSON-Line serializes by variant name,
/// not by discriminant.
///
/// **Default is `Reconnecting`** — the safe initial state before any
/// transport event has been observed. [`EndpointStats::default`] therefore
/// lands in `Reconnecting`; every call site that needs a different initial
/// state stores it explicitly (top-level endpoints stay on `Reconnecting`
/// through their bind/dial backoff; sub-endpoint admission paths store
/// `Connected` because admission *is* the transport-up event).
///
/// **Write authority is split** (CLAUDE.md locked decision): the endpoint
/// task owns `Connected` / `Reconnecting`, the router task owns `Idle` /
/// `Down`. Once an endpoint task observes the cancellation token it must
/// not write `state` again so the router's `Down` write is guaranteed to
/// be the last write to the slot.
///
/// `Unknown` is the safe fallback [`EndpointStats::load_state`] returns
/// when the slot holds an out-of-range u8. It is not a value any writer
/// ever stores; if it surfaces in stats output it indicates a bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EndpointState {
    #[default]
    Reconnecting,
    Connected,
    Idle,
    Down,
    Unknown,
}

impl EndpointState {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Reconnecting => 0,
            Self::Connected => 1,
            Self::Idle => 2,
            Self::Down => 3,
            Self::Unknown => 4,
        }
    }

    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Reconnecting,
            1 => Self::Connected,
            2 => Self::Idle,
            3 => Self::Down,
            _ => Self::Unknown,
        }
    }
}

/// Per-endpoint cumulative-since-start counters plus a single-slot health
/// state, shared between the reader, writer, and router tasks via `Arc`.
/// Each counter is an independent atomic so writers on the hot path never
/// contend on a single lock; the router snapshots all fields at each stats
/// interval and forwards a plain-data line to the dedicated stats task. The
/// `state` slot follows the split-authority write rule documented on
/// [`EndpointState`].
#[derive(Debug)]
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
    pub state: AtomicU8,
}

impl Default for EndpointStats {
    fn default() -> Self {
        Self::new(EndpointState::default())
    }
}

impl EndpointStats {
    /// Construct an `EndpointStats` with a chosen initial state. Top-level
    /// endpoints can use [`EndpointStats::default`] (Reconnecting); sub-
    /// endpoint admission paths pass `EndpointState::Connected` because
    /// admission *is* the transport-up event.
    pub fn new(state: EndpointState) -> Self {
        Self {
            rx_frames: AtomicU64::new(0),
            tx_frames: AtomicU64::new(0),
            rx_bytes: AtomicU64::new(0),
            tx_bytes: AtomicU64::new(0),
            dropped_tx: AtomicU64::new(0),
            crc_errors: AtomicU64::new(0),
            resync_bytes: AtomicU64::new(0),
            in_filter_drops: AtomicU64::new(0),
            out_filter_drops: AtomicU64::new(0),
            dedup_drops: AtomicU64::new(0),
            rx_lost_est: AtomicU64::new(0),
            learn_entries: AtomicU64::new(0),
            state: AtomicU8::new(state.as_u8()),
        }
    }

    /// Read the current state. Cheap; intended for the stats task's interval
    /// tick. An out-of-range u8 returns [`EndpointState::Unknown`] rather
    /// than panicking — keeps the stats task alive if a future writer
    /// stores a bad value, at the cost of one visible "unknown" line in
    /// the JSON output that operators can grep for.
    #[inline]
    pub fn load_state(&self) -> EndpointState {
        EndpointState::from_u8(self.state.load(Ordering::Relaxed))
    }

    /// Overwrite the current state. The split-authority rule on
    /// [`EndpointState`] says **which** task may call this with which
    /// variant; this method does not enforce it — callers do.
    #[inline]
    pub fn store_state(&self, state: EndpointState) {
        self.state.store(state.as_u8(), Ordering::Relaxed);
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

/// Last-seen values of [`Framer::resync_bytes`] and [`Framer::crc_errors`],
/// used by callers (TCP/serial session loops, the `udps:` per-peer state)
/// to compute deltas and forward them to the shared [`EndpointStats`]. The
/// framer keeps running totals; this struct remembers what we last
/// published so each sync only adds the new bytes.
#[derive(Debug, Default)]
pub struct FramerCounters {
    last_resync_total: u64,
    last_crc_total: u64,
}

impl FramerCounters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Forward any new `resync_bytes` / `crc_errors` accumulated since the
    /// last call to the shared stats. No-op when neither has advanced.
    pub fn sync(&mut self, framer: &Framer, stats: &EndpointStats) {
        let now_resync = framer.resync_bytes();
        let now_crc = framer.crc_errors();
        let resync_delta = now_resync.saturating_sub(self.last_resync_total);
        let crc_delta = now_crc.saturating_sub(self.last_crc_total);
        if resync_delta > 0 {
            stats
                .resync_bytes
                .fetch_add(resync_delta, Ordering::Relaxed);
        }
        if crc_delta > 0 {
            stats.crc_errors.fetch_add(crc_delta, Ordering::Relaxed);
        }
        self.last_resync_total = now_resync;
        self.last_crc_total = now_crc;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_rx_tx_updates_pairs() {
        let stats = EndpointStats::default();
        stats.add_rx_frame(12);
        stats.add_rx_frame(20);
        stats.add_tx_frame(7);
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 2);
        assert_eq!(stats.rx_bytes.load(Ordering::Relaxed), 32);
        assert_eq!(stats.tx_frames.load(Ordering::Relaxed), 1);
        assert_eq!(stats.tx_bytes.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn default_is_zero_and_reconnecting() {
        let stats = EndpointStats::default();
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 0);
        assert_eq!(stats.dropped_tx.load(Ordering::Relaxed), 0);
        assert_eq!(stats.resync_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(stats.load_state(), EndpointState::Reconnecting);
    }

    #[test]
    fn as_u8_from_u8_roundtrip_every_variant() {
        for state in [
            EndpointState::Reconnecting,
            EndpointState::Connected,
            EndpointState::Idle,
            EndpointState::Down,
            EndpointState::Unknown,
        ] {
            assert_eq!(EndpointState::from_u8(state.as_u8()), state);
        }
    }

    #[test]
    fn new_with_connected_lands_in_connected() {
        let stats = EndpointStats::new(EndpointState::Connected);
        assert_eq!(stats.load_state(), EndpointState::Connected);
        // counters still zero
        assert_eq!(stats.rx_frames.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn store_load_each_variant_roundtrips() {
        let stats = EndpointStats::default();
        for state in [
            EndpointState::Connected,
            EndpointState::Reconnecting,
            EndpointState::Idle,
            EndpointState::Down,
            EndpointState::Unknown,
        ] {
            stats.store_state(state);
            assert_eq!(stats.load_state(), state);
        }
    }

    #[test]
    fn from_u8_falls_back_to_unknown_out_of_range() {
        assert_eq!(EndpointState::from_u8(0), EndpointState::Reconnecting);
        assert_eq!(EndpointState::from_u8(3), EndpointState::Down);
        assert_eq!(EndpointState::from_u8(4), EndpointState::Unknown);
        assert_eq!(EndpointState::from_u8(5), EndpointState::Unknown);
        assert_eq!(EndpointState::from_u8(255), EndpointState::Unknown);
    }

    #[test]
    fn load_state_returns_unknown_on_out_of_range_raw_value() {
        let stats = EndpointStats::default();
        // Bypass store_state to simulate a hypothetical bug; load_state must
        // return Unknown rather than panicking so the stats task survives.
        stats.state.store(99, Ordering::Relaxed);
        assert_eq!(stats.load_state(), EndpointState::Unknown);
    }

    #[test]
    fn framer_counters_propagates_deltas_then_no_ops() {
        let stats = EndpointStats::default();
        let mut framer = Framer::new();
        framer.buffer_mut().extend_from_slice(&[0, 0, 0, 0]);
        // Drain (will count 4 resync bytes and return None).
        while framer.try_next_frame().is_some() {}
        assert_eq!(framer.resync_bytes(), 4);
        let mut counters = FramerCounters::new();
        counters.sync(&framer, &stats);
        assert_eq!(stats.resync_bytes.load(Ordering::Relaxed), 4);
        // Second call without further framer activity is a no-op.
        counters.sync(&framer, &stats);
        assert_eq!(stats.resync_bytes.load(Ordering::Relaxed), 4);
    }
}
