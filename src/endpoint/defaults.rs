//! Cross-endpoint default values from CLAUDE.md's "Defaults" table.
//!
//! Every constant here is consumed by more than one endpoint type. Per-
//! endpoint exclusives (e.g. `DEFAULT_IDLE_SECS`, `DEFAULT_LATCH_IDLE_SECS`,
//! `REOPEN_DELAY`) stay in their owning module — only the truly cross-
//! cutting defaults live here so a consistency change touches one site
//! instead of five.

/// Per-endpoint `BytesMut` initial capacity (reserved after each frame
/// freeze). Applies to every transport with a framer (`serial:`, `tcpc:`,
/// `tcps:` accepted children, `udps:` per-peer, `udpc:`). Hardcoded — well
/// above the MAVLink v2 max signed frame (~280 B), so the framer's
/// `reserve` path rarely grows; tuning is overkill for an internal buffer.
pub const READ_BUF_BYTES: usize = 8192;

/// Per-endpoint writer queue depth. Drop-oldest via `force_push` on
/// overflow. Applies to every endpoint that owns a `TxQueue`.
pub const DEFAULT_TX_QUEUE_FRAMES: usize = 256;

/// `tx_queue_frames` lower bound — 0 silently clamps to 1 inside
/// `TxQueue::new`, but the parser rejects it so the operator gets a
/// concrete bounds error instead of a hidden clamp.
pub const MIN_TX_QUEUE_FRAMES: usize = 1;

/// `tx_queue_frames` upper bound. At the default frame size (~280 B
/// signed v2) a 65 K-deep queue is ~18 MiB per endpoint — already
/// well past the "you should be reading the consumer faster" zone.
pub const MAX_TX_QUEUE_FRAMES: usize = 65_536;

/// Floor of the capped-exponential reconnect curve. Applies to `tcpc:`
/// reconnects and to `tcps:` / `udps:` initial-bind retries (CLAUDE.md
/// "TCP/UDP server bind reuses the `tcpc:` backoff curve").
pub const DEFAULT_RECONNECT_INITIAL_MS: u64 = 250;

/// Ceiling of the capped-exponential reconnect curve (±20% jitter applied
/// at draw time). Applies to `tcpc:` reconnects and to `tcps:` / `udps:`
/// initial-bind retries.
pub const DEFAULT_RECONNECT_MAX_MS: u64 = 30_000;

/// Global dedup window capacity (CLAUDE.md "Defaults" table:
/// `dedup_window_capacity` default 4096). Total `(hash, deadline)`
/// entries — single window owned by the router.
pub const DEFAULT_DEDUP_WINDOW_CAPACITY: usize = 4096;
