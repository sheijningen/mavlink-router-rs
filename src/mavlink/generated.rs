//! Re-exports the compile-time MAVLink msgid table emitted by `build.rs`.
//!
//! The included file defines:
//!
//! ```ignore
//! pub(crate) const SORTED: &[(u32, MsgEntry)] = &[
//!     (0,   MsgEntry { crc_extra: 50,  target_sys_offset: None,    ... }),
//!     (1,   MsgEntry { crc_extra: 124, target_sys_offset: None,    ... }),
//!     ...
//! ];
//! ```
//!
//! sorted ascending by msgid so `msgid_table::lookup` can binary-search it.
//! For each `<message>` in the parsed dialects, `build.rs` computes:
//!
//! - `crc_extra` via [`crc_extra::crc_extra_for_message`](super::crc_extra),
//!   the single source of truth shared between build- and runtime-side code.
//! - `target_sys_offset` / `target_comp_offset` — payload offsets of the
//!   `target_system` / `target_component` fields when present, else `None`.

use super::msgid_table::MsgEntry;

include!(concat!(env!("OUT_DIR"), "/generated_msgid_table.rs"));
