//! Re-exports the compile-time MAVLink msgid table emitted by `build.rs`.
//!
//! The included file defines:
//!
//! ```ignore
//! pub(crate) const SORTED: &[(u32, MsgEntry)] = &[
//!     (0,   MsgEntry { name: "HEARTBEAT",  crc_extra: 50,  ... }),
//!     (1,   MsgEntry { name: "SYS_STATUS", crc_extra: 124, ... }),
//!     ...
//! ];
//! ```
//!
//! sorted ascending by msgid so `msgid_table::lookup` can binary-search it.
//! For each `<message>` in the parsed dialects, `build.rs` computes:
//!
//! - `crc_extra` via [`crc_extra::crc_extra_for_message`](super::crc_extra),
//!   the single source of truth shared between build- and runtime-side code.
//! - `min_payload_len` — the v1 wire size (sum of non-extension field sizes
//!   after size-descending sort), also used as the v2 zero-trim ceiling.
//! - `target_sys_offset` / `target_comp_offset` — payload offsets of the
//!   `target_system` / `target_component` fields when present, else `None`.

use super::msgid_table::MsgEntry;

include!(concat!(env!("OUT_DIR"), "/generated_msgid_table.rs"));
