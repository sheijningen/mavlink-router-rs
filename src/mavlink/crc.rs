use super::crc_extra::{CRC_INIT, crc16_update};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Crc16(u16);

impl Crc16 {
    #[inline]
    pub(crate) fn new() -> Self {
        Self(CRC_INIT)
    }

    #[inline]
    pub(crate) fn update(&mut self, b: u8) {
        crc16_update(&mut self.0, b);
    }

    #[inline]
    pub(crate) fn update_slice(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.update(b);
        }
    }

    #[inline]
    pub(crate) fn finalize(self) -> u16 {
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
