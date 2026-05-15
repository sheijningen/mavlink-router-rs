#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    V1,
    V2,
}

pub const STX_V1: u8 = 0xFE;
pub const STX_V2: u8 = 0xFD;

pub const V2_IFLAG_SIGNED: u8 = 0x01;
pub const V2_SIGNATURE_LEN: usize = 13;

pub(crate) const V1_HEADER_LEN: usize = 6;
pub(crate) const V2_HEADER_LEN: usize = 10;
pub(crate) const CRC_LEN: usize = 2;

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
