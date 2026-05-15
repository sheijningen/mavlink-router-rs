// `crc` and `crc_extra` are wired up by build.rs (consumes `crc_extra` via
// `include!`) and by the framer (consumes `crc` for frame validation). Until
// those call sites land in phase 1 steps 2 and 3, the lib build sees no use
// site, so dead_code is silenced module-wide here. Drop these `allow`s once
// the framer is in.
#[allow(dead_code)]
pub mod crc;
#[allow(dead_code)]
pub mod crc_extra;
pub mod frame;
pub mod framer;
pub mod msgid_table;
