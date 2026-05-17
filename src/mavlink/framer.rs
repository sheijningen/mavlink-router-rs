use bytes::{Buf, Bytes, BytesMut};

use super::crc::Crc16;
use super::frame::{
    CRC_LEN, ParsedHeader, Stx, V1_HEADER_LEN, V2_HEADER_LEN, V2_IFLAG_SIGNED, V2_SIGNATURE_LEN,
    Version,
};
use super::msgid_table::{self, MsgEntry};

const DEFAULT_BUF_CAPACITY: usize = 8192;

/// Per-endpoint MAVLink frame state machine. Callers write raw transport
/// bytes into the inner `BytesMut` via `buffer_mut()` and drain complete
/// frames via `try_next_frame()`. Garbage prefixes and CRC failures are
/// counted (`resync_bytes`, `crc_errors`) but never panic.
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
            let stx = self.align_to_stx()?;
            if self.buf.len() < stx.header_len() {
                return None;
            }
            let frame_len = match self.decide_frame_len(stx) {
                FrameLen::Ready(n) => n,
                FrameLen::UnknownIncompatFlag => {
                    // Length is unknowable, so we can't safely forward. Drop
                    // the STX byte and rescan from the next one.
                    self.discard_byte();
                    continue;
                }
            };
            if self.buf.len() < frame_len {
                return None;
            }
            if let Some(frame) = self.try_validate_and_emit(stx, frame_len) {
                return Some(frame);
            }
            // CRC failed; try_validate_and_emit already discarded one byte.
        }
    }

    /// Advance over any garbage prefix until the buffer starts at an STX byte.
    /// Returns the [`Stx`] variant now at `buf[0]`, or None if the buffer held
    /// no STX at all (in which case all of it is counted as resync and the
    /// buffer is cleared — nothing read so far can ever become a frame).
    fn align_to_stx(&mut self) -> Option<Stx> {
        let found = self
            .buf
            .iter()
            .enumerate()
            .find_map(|(i, &b)| Stx::from_byte(b).map(|s| (i, s)));
        match found {
            Some((pos, stx)) => {
                if pos > 0 {
                    self.add_resync(pos as u64);
                    self.buf.advance(pos);
                }
                Some(stx)
            }
            None => {
                self.add_resync(self.buf.len() as u64);
                self.buf.clear();
                None
            }
        }
    }

    /// Peek the header far enough to compute the full frame length. Caller
    /// must have verified that `buf` already holds the header bytes for `stx`.
    fn decide_frame_len(&self, stx: Stx) -> FrameLen {
        let payload_len = self.buf[1] as usize;
        match stx {
            Stx::V1 => FrameLen::Ready(V1_HEADER_LEN + payload_len + CRC_LEN),
            Stx::V2 => {
                let iflags = self.buf[2];
                if iflags & !V2_IFLAG_SIGNED != 0 {
                    return FrameLen::UnknownIncompatFlag;
                }
                let signed_extra = if iflags & V2_IFLAG_SIGNED != 0 {
                    V2_SIGNATURE_LEN
                } else {
                    0
                };
                FrameLen::Ready(V2_HEADER_LEN + payload_len + CRC_LEN + signed_extra)
            }
        }
    }

    /// Parse the header, validate CRC (for known msgids), and either return
    /// the frame or count a CRC failure and discard one byte for resync. The
    /// caller's loop retries on None.
    fn try_validate_and_emit(
        &mut self,
        stx: Stx,
        frame_len: usize,
    ) -> Option<(ParsedHeader, Bytes)> {
        let header = parse_header(&self.buf[..frame_len], stx);

        // Single binary search per frame; the borrow flows into both CRC and
        // target extraction so the table isn't probed twice.
        let entry: Option<&'static MsgEntry> = msgid_table::lookup(header.msgid);

        let crc_ok = match entry {
            Some(e) => validate_crc(&self.buf[..frame_len], &header, e.crc_extra),
            None => true,
        };
        if !crc_ok {
            self.crc_errors = self.crc_errors.saturating_add(1);
            self.discard_byte();
            return None;
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
        Some((full_header, frame))
    }

    #[inline]
    fn add_resync(&mut self, n: u64) {
        self.resync_bytes = self.resync_bytes.saturating_add(n);
    }

    /// Discard one byte from the front of the buffer and count it as resync.
    /// Used to break out of a stuck STX (bad incompat flag or failed CRC).
    #[inline]
    fn discard_byte(&mut self) {
        self.add_resync(1);
        self.buf.advance(1);
    }
}

enum FrameLen {
    Ready(usize),
    UnknownIncompatFlag,
}

impl Default for Framer {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_header(frame: &[u8], stx: Stx) -> ParsedHeader {
    match stx {
        Stx::V1 => ParsedHeader {
            version: Version::V1,
            payload_len: frame[1],
            seq: frame[2],
            sysid: frame[3],
            compid: frame[4],
            msgid: frame[5] as u32,
            target_system: None,
            target_component: None,
        },
        Stx::V2 => ParsedHeader {
            version: Version::V2,
            payload_len: frame[1],
            seq: frame[4],
            sysid: frame[5],
            compid: frame[6],
            msgid: u32::from_le_bytes([frame[7], frame[8], frame[9], 0]),
            target_system: None,
            target_component: None,
        },
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
    let read_at = |off: u8| -> Option<u8> {
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
    use crate::mavlink::frame::{STX_V1, STX_V2};
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
        let (_, bytes) = f.try_next_frame().expect("frame");
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

    #[test]
    fn default_constructor_is_usable() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v1(0, &heartbeat_payload(), crc_extra);
        let mut f: Framer = Framer::default();
        f.buffer_mut().put_slice(&frame);
        assert!(f.try_next_frame().is_some());
    }

    #[test]
    fn with_capacity_zero_still_parses() {
        // BytesMut::with_capacity(0) gives a buffer that must grow on first
        // put_slice; the framer must not panic on zero initial headroom.
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v1(0, &heartbeat_payload(), crc_extra);
        let mut f = Framer::with_capacity(0);
        f.buffer_mut().put_slice(&frame);
        let (h, _) = f.try_next_frame().expect("frame");
        assert_eq!(h.msgid, 0);
    }

    #[test]
    fn buffer_with_only_stx_byte_waits_without_consuming() {
        let mut f = Framer::new();
        f.buffer_mut().put_slice(&[STX_V1]);
        assert!(f.try_next_frame().is_none());
        // The STX must be retained — the framer can't decide anything yet.
        assert_eq!(f.resync_bytes(), 0);
        assert_eq!(f.buffer_mut().len(), 1);
    }

    #[test]
    fn buffer_ending_mid_v1_header_waits() {
        // V1 header is 6 bytes; feed only 3 (STX + 2). The framer should hold
        // them and return None until the header completes.
        let mut f = Framer::new();
        f.buffer_mut().put_slice(&[STX_V1, 9, 0]);
        assert!(f.try_next_frame().is_none());
        assert_eq!(f.resync_bytes(), 0);
        assert_eq!(f.buffer_mut().len(), 3);
    }

    #[test]
    fn buffer_ending_mid_v2_header_waits() {
        // V2 header is 10 bytes; feed 5. The framer must wait.
        let mut f = Framer::new();
        f.buffer_mut().put_slice(&[STX_V2, 9, 0, 0, 7]);
        assert!(f.try_next_frame().is_none());
        assert_eq!(f.resync_bytes(), 0);
        assert_eq!(f.buffer_mut().len(), 5);
    }

    #[test]
    fn large_garbage_prefix_beyond_default_buffer_is_all_counted() {
        // 12_000 STX-free bytes — larger than DEFAULT_BUF_CAPACITY (8192).
        // The whole prefix should be consumed as resync_bytes in a single call
        // (the framer clears the buffer once it finds no STX).
        let garbage = vec![0u8; 12_000];
        let mut f = Framer::new();
        f.buffer_mut().put_slice(&garbage);
        assert!(f.try_next_frame().is_none());
        assert_eq!(f.resync_bytes(), 12_000);
        assert_eq!(f.buffer_mut().len(), 0);

        // Then a real frame still parses.
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v1(0, &heartbeat_payload(), crc_extra);
        f.buffer_mut().put_slice(&frame);
        assert!(f.try_next_frame().is_some());
    }

    #[test]
    fn byte_by_byte_feeding_yields_same_frame() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v1(0, &heartbeat_payload(), crc_extra);

        let mut f = Framer::new();
        let mut parsed = 0;
        for &b in &frame {
            f.buffer_mut().put_slice(&[b]);
            if let Some((h, _)) = f.try_next_frame() {
                assert_eq!(h.msgid, 0);
                parsed += 1;
            }
        }
        assert_eq!(parsed, 1);
        assert_eq!(f.resync_bytes(), 0);
        assert_eq!(f.crc_errors(), 0);
    }

    #[test]
    fn half_target_msgid_yields_sys_some_comp_none() {
        // CHANGE_OPERATOR_CONTROL (id 5) has target_system at offset 0 but no
        // target_component. Build a frame and assert the framer reflects this.
        // Wire layout for v1: control_request u8, version u8, passkey char[25] — 27 bytes.
        let entry = msgid_table::lookup(5).expect("CHANGE_OPERATOR_CONTROL");
        assert_eq!(entry.target_sys_offset, Some(0));
        assert_eq!(entry.target_comp_offset, None);

        let mut payload = vec![0u8; 27];
        payload[0] = 42; // target_system byte
        let frame = build_v1(5, &payload, entry.crc_extra);

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&frame);
        let (h, _) = f.try_next_frame().expect("frame");
        assert_eq!(h.target_system, Some(42));
        assert_eq!(h.target_component, None);
    }

    #[test]
    fn partial_tail_remains_after_full_frames() {
        let crc_extra = msgid_table::lookup(0).unwrap().crc_extra;
        let frame = build_v1(0, &heartbeat_payload(), crc_extra);

        let mut stream = Vec::new();
        stream.extend_from_slice(&frame);
        stream.extend_from_slice(&frame);
        stream.extend_from_slice(&frame[..4]); // partial third

        let mut f = Framer::new();
        f.buffer_mut().put_slice(&stream);

        let mut count = 0;
        while f.try_next_frame().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        assert_eq!(f.buffer_mut().len(), 4);
        assert_eq!(f.resync_bytes(), 0);
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use crate::mavlink::frame::{STX_V1, STX_V2};
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
