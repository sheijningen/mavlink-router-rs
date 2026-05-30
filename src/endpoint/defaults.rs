//! Cross-endpoint default values. Per-endpoint exclusives stay in their
//! owning module.

use std::time::Duration;

/// Per-endpoint `BytesMut` initial capacity. Well above the MAVLink v2 max
/// signed frame (~280 B) so the framer's `reserve` path rarely grows.
pub const READ_BUF_BYTES: usize = 8192;

/// Per-endpoint writer queue depth; drop-oldest via `force_push` on
/// overflow.
pub const DEFAULT_TX_QUEUE_FRAMES: usize = 256;

/// Floor of the capped-exponential reconnect curve.
pub const DEFAULT_RECONNECT_INITIAL_MS: u64 = 250;

/// Ceiling of the capped-exponential reconnect curve.
pub const DEFAULT_RECONNECT_MAX_MS: u64 = 30_000;

/// Total `(hash, deadline)` entries in the single global dedup window
/// owned by the router.
pub const DEFAULT_DEDUP_WINDOW_CAPACITY: usize = 4096;

/// Delay after a failed socket operation to prevent busy loops.
pub const SOCKET_ERROR_RETRY_DELAY: Duration = Duration::from_millis(10);
