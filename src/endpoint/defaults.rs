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

/// Per-endpoint writer queue depth. Drop-oldest via `force_push` on
/// overflow. Applies to every endpoint that owns a `TxQueue`.
pub const DEFAULT_TX_QUEUE_FRAMES: usize = 256;

/// Floor of the capped-exponential reconnect curve. Applies to `tcpc:`
/// reconnects and to `tcps:` / `udps:` initial-bind retries (CLAUDE.md
/// "TCP/UDP server bind reuses the `tcpc:` backoff curve").
pub const DEFAULT_RECONNECT_INITIAL_MS: u64 = 250;

/// Ceiling of the capped-exponential reconnect curve (±20% jitter applied
/// at draw time). Applies to `tcpc:` reconnects and to `tcps:` / `udps:`
/// initial-bind retries.
pub const DEFAULT_RECONNECT_MAX_MS: u64 = 30_000;
