//! Re-exports the compile-time MAVLink msgid table emitted by `build.rs`:
//! a `&[(u32, MsgEntry)]` sorted ascending by msgid for binary search.
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
use super::msgid_table::MsgEntry;

include!(concat!(env!("OUT_DIR"), "/generated_msgid_table.rs"));
