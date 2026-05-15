use bytes::{Buf, Bytes, BytesMut};

use super::crc::Crc16;
use super::frame::{
    CRC_LEN, ParsedHeader, STX_V1, STX_V2, V1_HEADER_LEN, V2_HEADER_LEN, V2_IFLAG_SIGNED,
    V2_SIGNATURE_LEN, Version,
};
use super::msgid_table::{self, MsgEntry};

const DEFAULT_BUF_CAPACITY: usize = 8192;

pub struct Framer {
    buf: BytesMut,
    resync_bytes: u64,
    crc_errors: u64,
    read_capacity: usize,
}

impl Framer {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BUF_CAPACITY)
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(cap),
            resync_bytes: 0,
            crc_errors: 0,
            read_capacity: cap,
        }
    }

    /// The inner accumulator. Caller writes raw bytes into the trailing
    /// capacity (typical pattern: `socket.read_buf(framer.buffer_mut()).await`).
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    pub fn resync_bytes(&self) -> u64 {
        self.resync_bytes
    }

    pub fn crc_errors(&self) -> u64 {
        self.crc_errors
    }

    /// Attempt to extract one complete frame. Returns None when the buffer is
    /// too short to contain a complete frame at the current scan position.
    /// Drops frames with bad CRC (counted, byte advanced) and rescans across
    /// garbage prefixes (counted) silently.
    pub fn try_next_frame(&mut self) -> Option<(ParsedHeader, Bytes)> {
        loop {
            let stx_pos = match self.buf.iter().position(|&b| b == STX_V1 || b == STX_V2) {
                Some(p) => p,
                None => {
                    self.resync_bytes = self.resync_bytes.saturating_add(self.buf.len() as u64);
                    self.buf.clear();
                    return None;
                }
            };
            if stx_pos > 0 {
                self.resync_bytes = self.resync_bytes.saturating_add(stx_pos as u64);
                self.buf.advance(stx_pos);
            }

            let stx = self.buf[0];
            let header_len = match stx {
                STX_V1 => V1_HEADER_LEN,
                STX_V2 => V2_HEADER_LEN,
                _ => unreachable!("STX scan returned non-STX byte"),
            };

            if self.buf.len() < header_len {
                return None;
            }

            let payload_len = self.buf[1] as usize;
            let (frame_len, incompat_flags) = match stx {
                STX_V1 => (V1_HEADER_LEN + payload_len + CRC_LEN, 0u8),
                STX_V2 => {
                    let iflags = self.buf[2];
                    let unknown = iflags & !V2_IFLAG_SIGNED;
                    if unknown != 0 {
                        // Frame uses incompat features RMR doesn't understand —
                        // we don't know its length and can't safely forward.
                        // Discard the STX and rescan.
                        self.resync_bytes = self.resync_bytes.saturating_add(1);
                        self.buf.advance(1);
                        continue;
                    }
                    let signed_extra = if (iflags & V2_IFLAG_SIGNED) != 0 {
                        V2_SIGNATURE_LEN
                    } else {
                        0
                    };
                    (V2_HEADER_LEN + payload_len + CRC_LEN + signed_extra, iflags)
                }
                _ => unreachable!(),
            };

            if self.buf.len() < frame_len {
                return None;
            }

            let header = parse_header(&self.buf[..frame_len], stx, incompat_flags);

            // Lookup once per frame; thread the result into both CRC and target
            // extraction so the table is hit a single time even though both
            // steps need it.
            let entry: Option<&'static MsgEntry> = msgid_table::lookup(header.msgid);

            let crc_ok = match entry {
                Some(e) => validate_crc(&self.buf[..frame_len], &header, e.crc_extra),
                None => true,
            };

            if !crc_ok {
                self.crc_errors = self.crc_errors.saturating_add(1);
                self.resync_bytes = self.resync_bytes.saturating_add(1);
                self.buf.advance(1);
                continue;
            }

            let (target_system, target_component) =
                extract_targets(&self.buf[..frame_len], &header, entry);
            let full_header = ParsedHeader {
                target_system,
                target_component,
                ..header
            };

            let frame = self.buf.split_to(frame_len).freeze();
            self.buf.reserve(self.read_capacity);
            return Some((full_header, frame));
        }
    }
}

impl Default for Framer {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_header(frame: &[u8], stx: u8, incompat_flags: u8) -> ParsedHeader {
    match stx {
        STX_V1 => ParsedHeader {
            version: Version::V1,
            payload_len: frame[1],
            seq: frame[2],
            sysid: frame[3],
            compid: frame[4],
            msgid: frame[5] as u32,
            incompat_flags: 0,
            compat_flags: 0,
            target_system: None,
            target_component: None,
        },
        STX_V2 => ParsedHeader {
            version: Version::V2,
            payload_len: frame[1],
            incompat_flags,
            compat_flags: frame[3],
            seq: frame[4],
            sysid: frame[5],
            compid: frame[6],
            msgid: u32::from_le_bytes([frame[7], frame[8], frame[9], 0]),
            target_system: None,
            target_component: None,
        },
        _ => unreachable!(),
    }
}

fn validate_crc(frame: &[u8], header: &ParsedHeader, crc_extra: u8) -> bool {
    let header_len = header.payload_start();
    let payload_end = header_len + header.payload_len as usize;
    let mut crc = Crc16::new();
    crc.update_slice(&frame[1..payload_end]);
    crc.update(crc_extra);
    let expected = u16::from_le_bytes([frame[payload_end], frame[payload_end + 1]]);
    crc.finalize() == expected
}

fn extract_targets(
    frame: &[u8],
    header: &ParsedHeader,
    entry: Option<&'static MsgEntry>,
) -> (Option<u8>, Option<u8>) {
    let Some(entry) = entry else {
        return (None, None);
    };
    let payload_start = header.payload_start();
    let payload_len = header.payload_len as usize;
    let read_at = |off: u16| -> Option<u8> {
        let off = off as usize;
        if off < payload_len {
            Some(frame[payload_start + off])
        } else {
            None
        }
    };
    (
        entry.target_sys_offset.and_then(read_at),
        entry.target_comp_offset.and_then(read_at),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mavlink::msgid_table;
    use bytes::BufMut;

    fn build_v1(msgid: u32, payload: &[u8], crc_extra: u8) -> Vec<u8> {
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.push(STX_V1);
        frame.push(payload.len() as u8);
        frame.push(0); // seq
        frame.push(1); // sysid
        frame.push(1); // compid
        frame.push(msgid as u8);
        frame.extend_from_slice(payload);
        let mut crc = Crc16::new();
        crc.update_slice(&frame[1..]);
        crc.update(crc_extra);
        let c = crc.finalize();
        frame.push((c & 0xFF) as u8);
        frame.push((c >> 8) as u8);
        frame
    }

    fn build_v2(
        msgid: u32,
        payload: &[u8],
        crc_extra: u8,
        incompat_flags: u8,
        signature: Option<&[u8; V2_SIGNATURE_LEN]>,
    ) -> Vec<u8> {
        let mut frame = Vec::with_capacity(12 + payload.len() + V2_SIGNATURE_LEN);
        frame.push(STX_V2);
        frame.push(payload.len() as u8);
        frame.push(incompat_flags);
        frame.push(0); // compat_flags
        frame.push(0); // seq
        frame.push(1); // sysid
        frame.push(1); // compid
        frame.push((msgid & 0xFF) as u8);
        frame.push(((msgid >> 8) & 0xFF) as u8);
        frame.push(((msgid >> 16) & 0xFF) as u8);
        frame.extend_from_slice(payload);
        let mut crc = Crc16::new();
        crc.update_slice(&frame[1..]);
        crc.update(crc_extra);
        let c = crc.finalize();
        frame.push((c & 0xFF) as u8);
        frame.push((c >> 8) as u8);
        if let Some(sig) = signature {
            frame.extend_from_slice(sig);
        }
        frame
    }

    fn heartbeat_payload() -> Vec<u8> {
        // custom_mode u32 + type + autopilot + base_mode + system_status + mavlink_version
        let mut p = Vec::with_capacity(9);
        p.extend_from_slice(&0u32.to_le_bytes());
        p.push(2); // MAV_TYPE_QUADROTOR
        p.push(3); // MAV_AUTOPILOT_ARDUPILOTMEGA
        p.push(0);
        p.push(0);
        p.push(3);
        p
    }

    #[test]
    fn parses_known_v1_heartbeat() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v1(0, &heartbeat_payload(), crc_extra);

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        let (header, bytes) = f.try_next_frame().expect("frame");
        assert_eq!(header.version, Version::V1);
        assert_eq!(header.msgid, 0);
        assert_eq!(header.sysid, 1);
        assert_eq!(header.payload_len, 9);
        assert_eq!(header.target_system, None);
        assert_eq!(header.target_component, None);
        assert_eq!(bytes.len(), frame.len());
        assert_eq!(&bytes[..], &frame[..]);
        assert_eq!(f.crc_errors(), 0);
        assert_eq!(f.resync_bytes(), 0);
    }

    #[test]
    fn parses_known_v2_heartbeat_unsigned() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v2(0, &heartbeat_payload(), crc_extra, 0, None);

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        let (header, bytes) = f.try_next_frame().expect("frame");
        assert_eq!(header.version, Version::V2);
        assert_eq!(header.msgid, 0);
        assert!(!header.is_signed());
        assert_eq!(bytes.len(), frame.len());
    }

    #[test]
    fn parses_signed_v2_forwards_signature_opaquely() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let sig: [u8; V2_SIGNATURE_LEN] = [0xAA; V2_SIGNATURE_LEN];
        let frame = build_v2(
            0,
            &heartbeat_payload(),
            crc_extra,
            V2_IFLAG_SIGNED,
            Some(&sig),
        );

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        let (header, bytes) = f.try_next_frame().expect("frame");
        assert!(header.is_signed());
        assert_eq!(
            bytes.len(),
            frame.len(),
            "signature must be in returned bytes"
        );
        // The trailing 13 bytes should be the signature verbatim.
        assert_eq!(&bytes[bytes.len() - V2_SIGNATURE_LEN..], &sig[..]);
        assert_eq!(f.crc_errors(), 0);
    }

    #[test]
    fn unknown_msgid_passes_without_crc_check() {
        // msgid 99_999 has no entry in the table — frame should forward with
        // arbitrary CRC bytes.
        assert!(msgid_table::lookup(99_999).is_none());
        let payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let mut frame = vec![
            STX_V2,
            payload.len() as u8,
            0, // incompat_flags
            0, // compat_flags
            0, // seq
            1, // sysid
            1, // compid
            (99_999u32 & 0xFF) as u8,
            ((99_999u32 >> 8) & 0xFF) as u8,
            ((99_999u32 >> 16) & 0xFF) as u8,
        ];
        frame.extend_from_slice(&payload);
        // Deliberately wrong CRC.
        frame.push(0x00);
        frame.push(0x00);

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        let (header, _) = f.try_next_frame().expect("frame should pass-through");
        assert_eq!(header.msgid, 99_999);
        assert_eq!(header.target_system, None);
        assert_eq!(f.crc_errors(), 0);
    }

    #[test]
    fn extracts_ping_targets_v2() {
        // PING (id 4): time_usec u64 + seq u32 + target_system u8 + target_component u8
        let crc_extra = msgid_table::lookup(4).unwrap().crc_extra;
        let mut payload = Vec::with_capacity(14);
        payload.extend_from_slice(&123u64.to_le_bytes());
        payload.extend_from_slice(&42u32.to_le_bytes());
        payload.push(7); // target_system
        payload.push(9); // target_component
        let frame = build_v2(4, &payload, crc_extra, 0, None);

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        let (header, _) = f.try_next_frame().expect("frame");
        assert_eq!(header.target_system, Some(7));
        assert_eq!(header.target_component, Some(9));
    }

    #[test]
    fn v2_zero_trim_past_target_offset_means_broadcast() {
        // PING with payload_len=12 (just time_usec + seq) — target bytes are
        // zero-trimmed off the wire. Framer must return None, not panic.
        let crc_extra = msgid_table::lookup(4).unwrap().crc_extra;
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&123u64.to_le_bytes());
        payload.extend_from_slice(&42u32.to_le_bytes());
        let frame = build_v2(4, &payload, crc_extra, 0, None);

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        let (header, _) = f.try_next_frame().expect("frame");
        assert_eq!(header.target_system, None);
        assert_eq!(header.target_component, None);
    }

    #[test]
    fn resyncs_past_garbage_prefix() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v1(0, &heartbeat_payload(), crc_extra);
        let mut input = vec![0x00, 0xFF, 0xAB, 0xCD]; // 4 bytes of garbage
        input.extend_from_slice(&frame);

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&input);
        let (header, _) = f.try_next_frame().expect("frame after resync");
        assert_eq!(header.msgid, 0);
        assert_eq!(f.resync_bytes(), 4);
    }

    #[test]
    fn corrupted_crc_drops_and_resyncs() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let mut frame = build_v1(0, &heartbeat_payload(), crc_extra);
        let last = frame.len() - 1;
        frame[last] ^= 0xFF; // flip CRC high byte

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        // The framer should drop the frame, count one CRC error, and resync.
        // Since there's no second frame after, it returns None.
        assert!(f.try_next_frame().is_none());
        assert_eq!(f.crc_errors(), 1);
        assert!(f.resync_bytes() >= 1);
    }

    #[test]
    fn partial_frame_returns_none_then_completes_when_more_bytes_arrive() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v1(0, &heartbeat_payload(), crc_extra);

        let mut f = Framer::new();
        // Feed everything except the last 3 bytes.
        f.buffer_mut().put_slice(&frame[..frame.len() - 3]);
        assert!(f.try_next_frame().is_none());
        // Then the rest.
        f.buffer_mut().put_slice(&frame[frame.len() - 3..]);
        let (header, _) = f.try_next_frame().expect("frame");
        assert_eq!(header.msgid, 0);
    }

    #[test]
    fn unknown_v2_incompat_flag_is_dropped() {
        // Set a non-IFLAG_SIGNED bit in incompat_flags. The framer cannot know
        // the true frame length, so it must skip the STX and resync.
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v2(0, &heartbeat_payload(), crc_extra, 0x02, None);

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        // No frame parsed: the STX is discarded and the rest looks like garbage.
        assert!(f.try_next_frame().is_none());
        assert!(f.resync_bytes() >= 1);
    }

    #[test]
    fn back_to_back_frames_parse() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v1(0, &heartbeat_payload(), crc_extra);

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        f.buffer_mut().put_slice(&frame);
        f.buffer_mut().put_slice(&frame);
        let mut count = 0;
        while f.try_next_frame().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
        assert_eq!(f.resync_bytes(), 0);
        assert_eq!(f.crc_errors(), 0);
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use bytes::BufMut;
    use proptest::prelude::*;

    proptest! {
        // Random bytes in: framer must not panic, must not loop forever,
        // and must terminate with all bytes either consumed as part of a
        // frame, counted as resync, or held as a partial-header tail.
        #[test]
        fn no_panic_no_infinite_loop_on_garbage(input in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let mut f = Framer::new();
            f.buffer_mut().put_slice(&input);
            let mut iterations: u64 = 0;
            while let Some((_, bytes)) = f.try_next_frame() {
                // Each returned frame consumes its bytes from the buffer.
                prop_assert!(bytes.len() >= 8); // smallest possible v1 frame
                iterations += 1;
                prop_assert!(iterations <= input.len() as u64 + 1);
            }
            let remaining = f.buffer_mut().len();
            // Whatever remains is shorter than a complete frame at the current
            // scan head (otherwise the loop would have either accepted or
            // resync'd past it).
            prop_assert!(remaining < V2_HEADER_LEN + 256 + CRC_LEN + V2_SIGNATURE_LEN);
        }

        // Garbage prefix followed by a real frame: the framer eventually
        // parses the real frame and counts the prefix as resync_bytes.
        #[test]
        fn resyncs_to_real_frame_after_garbage(
            garbage in proptest::collection::vec(any::<u8>(), 0..512)
        ) {
            // Strip STX bytes from garbage. Otherwise the framer would
            // (correctly) treat such a byte as the start of an over-long frame
            // and stall waiting for more bytes — masking the planted frame.
            // Real protocol bugs around STX-in-garbage are covered by the
            // no_panic_no_infinite_loop_on_garbage proptest above.
            let mut garbage = garbage;
            for b in garbage.iter_mut() {
                if *b == STX_V1 || *b == STX_V2 {
                    *b = 0x00;
                }
            }

            let crc_extra = crate::mavlink::msgid_table::lookup(0).unwrap().crc_extra;
            let payload = {
                let mut p = Vec::with_capacity(9);
                p.extend_from_slice(&0u32.to_le_bytes());
                p.extend_from_slice(&[2, 3, 0, 0, 3]);
                p
            };
            let mut frame = vec![STX_V1, 9, 0, 1, 1, 0];
            frame.extend_from_slice(&payload);
            let mut crc = crate::mavlink::crc::Crc16::new();
            crc.update_slice(&frame[1..]);
            crc.update(crc_extra);
            let c = crc.finalize();
            frame.push((c & 0xFF) as u8);
            frame.push((c >> 8) as u8);

            let mut input = garbage.clone();
            input.extend_from_slice(&frame);

            let mut f = Framer::new();
            f.buffer_mut().put_slice(&input);

            // Drain frames. We expect at least one to parse — the planted one.
            // Random garbage may also accidentally parse as an unknown-msgid
            // frame, so don't assert exact count.
            let mut found_heartbeat = false;
            while let Some((h, _)) = f.try_next_frame() {
                if h.msgid == 0 && h.version == Version::V1 {
                    found_heartbeat = true;
                }
            }
            prop_assert!(found_heartbeat, "planted heartbeat must parse after resync");
        }
    }
}
