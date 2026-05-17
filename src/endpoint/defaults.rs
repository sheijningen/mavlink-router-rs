//! Cross-endpoint default values from CLAUDE.md's "Defaults" table.
//!
//! Every constant here is a CLI/TOML knob that more than one endpoint type
//! falls back to when the user leaves it unset. Per-endpoint exclusives
//! (e.g. `DEFAULT_SERIAL_REOPEN_MS`, `DEFAULT_IDLE_SECS`) stay in their
//! owning module — only the truly cross-cutting defaults live here so a
//! consistency change touches one site instead of five.

/// Per-endpoint `BytesMut` initial capacity (reserved after each frame
/// freeze). Applies to every transport with a framer (`serial:`, `tcpc:`,
/// `tcps:` accepted children, `udps:` per-peer, `udpc:`).
pub const DEFAULT_READ_BUF_BYTES: usize = 8192;

/// `read_buf_bytes` lower bound — well above the MAVLink v2 max signed
/// frame (~280 B) so the framer's `reserve` path doesn't have to grow on
/// every read. 1 KB is a generous floor.
pub const MIN_READ_BUF_BYTES: usize = 1024;

/// `read_buf_bytes` upper bound (1 MiB). Sized to keep per-endpoint
/// memory bounded across many endpoints; beyond this the framer is no
/// longer the bottleneck.
pub const MAX_READ_BUF_BYTES: usize = 1_048_576;

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
