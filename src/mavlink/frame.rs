//! MAVLink wire-format types and constants.
//!
//! "STX" (start of text) is the byte that marks the start of a MAVLink frame
//! on the wire. The byte value differs by wire version (`STX_V1` = 0xFE,
//! `STX_V2` = 0xFD) so a parser can detect the version after a single byte.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    V1,
    V2,
}

/// Start-of-frame byte for MAVLink v1.
pub const STX_V1: u8 = 0xFE;
/// Start-of-frame byte for MAVLink v2.
pub const STX_V2: u8 = 0xFD;

pub const V2_IFLAG_SIGNED: u8 = 0x01;
pub const V2_SIGNATURE_LEN: usize = 13;

pub(crate) const V1_HEADER_LEN: usize = 6;
pub(crate) const V2_HEADER_LEN: usize = 10;
pub(crate) const CRC_LEN: usize = 2;

/// Routing-relevant fields extracted from a MAVLink frame header in the reader
/// task, so the router never re-parses. Bytes the header was decoded from stay
/// in the `Bytes` payload returned alongside this struct.
#[derive(Debug, Clone, Copy)]
pub struct ParsedHeader {
    pub version: Version,
    pub sysid: u8,
    pub compid: u8,
    pub msgid: u32,
    pub seq: u8,
    pub payload_len: u8,
    pub incompat_flags: u8,
    pub compat_flags: u8,
    /// None if the msgid has no target field, the msgid is unknown to the
    /// router (no entry in the const table), or the field offset lies past
    /// the (possibly v2 zero-trimmed) payload_len — all of which are treated
    /// as broadcast by the router.
    pub target_system: Option<u8>,
    pub target_component: Option<u8>,
}

impl ParsedHeader {
    pub(crate) fn payload_start(&self) -> usize {
        match self.version {
            Version::V1 => V1_HEADER_LEN,
            Version::V2 => V2_HEADER_LEN,
        }
    }

    pub fn is_signed(&self) -> bool {
        self.version == Version::V2 && (self.incompat_flags & V2_IFLAG_SIGNED) != 0
    }
}
