use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use crate::mavlink::framer::Framer;

/// User-visible health of one routing endpoint. Stored on
/// [`EndpointStats::state`] as a raw `u8` so the stats task can cheaply
/// `load → match` on a hot interval; the named enum keeps writers honest
/// about which discriminant they're storing.
///
/// **Stable discriminants** (CLAUDE.md locked decision; the wire shape of the
/// stats JSON-Line depends on this mapping never changing):
/// `Connected = 0 | Reconnecting = 1 | Idle = 2 | Down = 3`.
///
/// **`Connected = 0` is load-bearing.** [`EndpointStats::default`] therefore
/// lands in state `Connected` with no explicit store, which is the right
/// initial value for sub-endpoints (UDP peers, TCP accepted clients) whose
/// admission *is* the transport-up event. Top-level endpoints that go through
/// bind/dial backoff construct via [`EndpointStats::new(Reconnecting)`] so
/// the slot is set before the `Arc` is published anywhere — closing the
/// brief default-then-store window where a concurrent stats snapshot could
/// observe `Connected` for an endpoint that hasn't bound yet.
///
/// **Write authority is split** (also a CLAUDE.md locked decision; not yet
/// wired up in code — Phase 5 enforces it):
/// the endpoint task owns `Connected` / `Reconnecting`, the router task owns
/// `Idle` / `Down`. Once an endpoint task observes the cancellation token it
/// must not write `state` again so the router's `Down` write is guaranteed
/// to be the last write to the slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EndpointState {
    Connected = 0,
    Reconnecting = 1,
    Idle = 2,
    Down = 3,
}

impl EndpointState {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    #[inline]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Connected),
            1 => Some(Self::Reconnecting),
            2 => Some(Self::Idle),
            3 => Some(Self::Down),
            _ => None,
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
    pub state: AtomicU8,
}

impl EndpointStats {
    /// Construct an `EndpointStats` with the given starting state. Top-level
    /// endpoints use this with `EndpointState::Reconnecting` so the
    /// Reconnecting state is set before the `Arc` is published anywhere; sub-
    /// endpoints typically use [`EndpointStats::default`] instead (Connected
    /// via the `Connected = 0` discriminant, no explicit store).
    pub fn new(state: EndpointState) -> Self {
        let s = Self::default();
        s.state.store(state.as_u8(), Ordering::Relaxed);
        s
    }

    /// Read the current state. Cheap; intended for the stats task's interval
    /// tick. Panics if the slot was written to an out-of-range value, which
    /// would indicate a bug in a writer (all writers go through
    /// [`EndpointStats::store_state`] or [`EndpointStats::new`]).
    #[inline]
    pub fn load_state(&self) -> EndpointState {
        let raw = self.state.load(Ordering::Relaxed);
        EndpointState::from_u8(raw)
            .unwrap_or_else(|| panic!("EndpointStats.state held an out-of-range u8: {raw}"))
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
        let s = EndpointStats::default();
        s.add_rx_frame(12);
        s.add_rx_frame(20);
        s.add_tx_frame(7);
        assert_eq!(s.rx_frames.load(Ordering::Relaxed), 2);
        assert_eq!(s.rx_bytes.load(Ordering::Relaxed), 32);
        assert_eq!(s.tx_frames.load(Ordering::Relaxed), 1);
        assert_eq!(s.tx_bytes.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn default_is_zero_and_connected() {
        let s = EndpointStats::default();
        assert_eq!(s.rx_frames.load(Ordering::Relaxed), 0);
        assert_eq!(s.dropped_tx.load(Ordering::Relaxed), 0);
        assert_eq!(s.resync_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(s.load_state(), EndpointState::Connected);
    }

    #[test]
    fn discriminants_match_doc() {
        // CLAUDE.md locked the wire-stable discriminants. If this test
        // changes, the JSON-Lines stats schema is breaking.
        assert_eq!(EndpointState::Connected as u8, 0);
        assert_eq!(EndpointState::Reconnecting as u8, 1);
        assert_eq!(EndpointState::Idle as u8, 2);
        assert_eq!(EndpointState::Down as u8, 3);
    }

    #[test]
    fn new_with_reconnecting_lands_in_reconnecting() {
        let s = EndpointStats::new(EndpointState::Reconnecting);
        assert_eq!(s.load_state(), EndpointState::Reconnecting);
        // counters still zero
        assert_eq!(s.rx_frames.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn store_load_each_variant_roundtrips() {
        let s = EndpointStats::default();
        for state in [
            EndpointState::Connected,
            EndpointState::Reconnecting,
            EndpointState::Idle,
            EndpointState::Down,
        ] {
            s.store_state(state);
            assert_eq!(s.load_state(), state);
        }
    }

    #[test]
    fn from_u8_rejects_out_of_range() {
        assert_eq!(EndpointState::from_u8(0), Some(EndpointState::Connected));
        assert_eq!(EndpointState::from_u8(3), Some(EndpointState::Down));
        assert_eq!(EndpointState::from_u8(4), None);
        assert_eq!(EndpointState::from_u8(255), None);
    }

    #[test]
    #[should_panic(expected = "out-of-range u8")]
    fn load_state_panics_on_out_of_range_raw_value() {
        let s = EndpointStats::default();
        // Bypass store_state to simulate a hypothetical bug; load_state must
        // panic rather than silently returning a junk variant.
        s.state.store(99, Ordering::Relaxed);
        let _ = s.load_state();
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
