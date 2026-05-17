// CRC-16-MCRF4XX (the MAVLink frame CRC): X.25 with init 0xFFFF, reflected,
// no final XOR. The build-side `crc_extra_for_message` algorithm in
// `build_support/crc_extra.rs` carries a second self-contained copy of the
// same primitive — both sides ship the published test vector (0x6F91 for
// "123456789") so any drift fails a test.

const CRC_INIT: u16 = 0xFFFF;

#[inline]
fn crc16_update(crc: &mut u16, b: u8) {
    let tmp = b ^ ((*crc & 0xFF) as u8);
    let tmp = tmp ^ (tmp << 4);
    let tmp16 = tmp as u16;
    *crc = (*crc >> 8) ^ (tmp16 << 8) ^ (tmp16 << 3) ^ (tmp16 >> 4);
}

/// Streaming CRC-16-MCRF4XX accumulator (the MAVLink frame CRC). Wraps the
/// single-byte update step so the framer can feed bytes incrementally as it
/// validates a frame.
#[derive(Debug, Clone, Copy)]
pub struct Crc16(u16);

impl Crc16 {
    #[inline]
    pub fn new() -> Self {
        Self(CRC_INIT)
    }

    #[inline]
    pub fn update(&mut self, b: u8) {
        crc16_update(&mut self.0, b);
    }

    #[inline]
    pub fn update_slice(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.update(b);
        }
    }

    #[inline]
    pub fn finalize(self) -> u16 {
        self.0
    }
}

impl Default for Crc16 {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_matches_one_shot() {
        let mut c = Crc16::new();
        c.update_slice(b"123456789");
        assert_eq!(c.finalize(), 0x6F91);
    }

    #[test]
    fn streaming_byte_by_byte() {
        let mut c = Crc16::new();
        for &b in b"123456789" {
            c.update(b);
        }
        assert_eq!(c.finalize(), 0x6F91);
    }

    #[test]
    fn empty_returns_init() {
        let c = Crc16::new();
        assert_eq!(c.finalize(), 0xFFFF);
    }
}
