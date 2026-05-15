//! Re-exports the compile-time MAVLink msgid table emitted by `build.rs`.
//!
//! The `include!` below pulls in `$OUT_DIR/generated_msgid_table.rs`, a file
//! materialised by `build.rs` while walking the vendored dialect XML under
//! `vendor/mavlink/` (entry roots listed in the `DIALECTS` constant in
//! `build.rs`). The included file defines:
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
//!
//! A re-run is triggered by edits to any vendored XML, `crc_extra.rs`,
//! `build.rs`, or anything under `build_support/` (the `cargo:rerun-if-changed`
//! directives in `build.rs` enumerate the full set).
//!
//! Do not edit by hand: `OUT_DIR` is outside the source tree, so any manual
//! edits would be overwritten on the next build anyway.

use super::msgid_table::MsgEntry;

include!(concat!(env!("OUT_DIR"), "/generated_msgid_table.rs"));
