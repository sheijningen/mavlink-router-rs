// Build-only helpers for parsing the vendored MAVLink XML at compile time
// live under `build_support/` at the repo root, not here — they are
// `include!`d by build.rs and exercised by tests/build_support.rs.

pub(crate) mod crc;
pub(crate) mod crc_extra;
pub mod frame;
pub mod framer;
pub(crate) mod generated;
pub(crate) mod msgid_table;
