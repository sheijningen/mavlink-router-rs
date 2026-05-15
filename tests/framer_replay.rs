// End-to-end replay test: feed a realistic byte stream (mixed v1/v2, signed,
// unknown msgid) through the public `Framer` API in chunks that don't align
// with frame boundaries, and assert each frame parses correctly.
//
// `Crc16` and `msgid_table::lookup` are crate-internal, so this file
// inlines a tiny copy of the CRC algorithm to build well-formed test
// frames. Treating the test as an outside consumer is the point.

use bytes::BufMut;
use rmr::mavlink::frame::{STX_V1, STX_V2, V2_IFLAG_SIGNED, V2_SIGNATURE_LEN, Version};
use rmr::mavlink::framer::Framer;

fn crc16_update(crc: &mut u16, b: u8) {
    let tmp = b ^ ((*crc & 0xFF) as u8);
    let tmp = tmp ^ (tmp << 4);
    let tmp16 = tmp as u16;
    *crc = (*crc >> 8) ^ (tmp16 << 8) ^ (tmp16 << 3) ^ (tmp16 >> 4);
}

fn frame_crc(after_stx: &[u8], crc_extra: u8) -> [u8; 2] {
    let mut crc = 0xFFFFu16;
    for &b in after_stx {
        crc16_update(&mut crc, b);
    }
    crc16_update(&mut crc, crc_extra);
    [(crc & 0xFF) as u8, (crc >> 8) as u8]
}

fn build_v1(msgid: u32, seq: u8, sysid: u8, compid: u8, payload: &[u8], crc_extra: u8) -> Vec<u8> {
    let mut f = Vec::with_capacity(8 + payload.len());
    f.push(STX_V1);
    f.push(payload.len() as u8);
    f.push(seq);
    f.push(sysid);
    f.push(compid);
    f.push(msgid as u8);
    f.extend_from_slice(payload);
    let crc = frame_crc(&f[1..], crc_extra);
    f.extend_from_slice(&crc);
    f
}

#[allow(clippy::too_many_arguments)]
fn build_v2(
    msgid: u32,
    seq: u8,
    sysid: u8,
    compid: u8,
    payload: &[u8],
    crc_extra: u8,
    iflags: u8,
    sig: Option<&[u8; V2_SIGNATURE_LEN]>,
) -> Vec<u8> {
    let mut f = Vec::with_capacity(12 + payload.len() + V2_SIGNATURE_LEN);
    f.push(STX_V2);
    f.push(payload.len() as u8);
    f.push(iflags);
    f.push(0); // compat_flags
    f.push(seq);
    f.push(sysid);
    f.push(compid);
    f.push((msgid & 0xFF) as u8);
    f.push(((msgid >> 8) & 0xFF) as u8);
    f.push(((msgid >> 16) & 0xFF) as u8);
    f.extend_from_slice(payload);
    let crc = frame_crc(&f[1..], crc_extra);
    f.extend_from_slice(&crc);
    if let Some(s) = sig {
        f.extend_from_slice(s);
    }
    f
}

// Published crc_extras from the MAVLink reference (also covered by unit tests):
const CRC_HEARTBEAT: u8 = 50;
const CRC_SYS_STATUS: u8 = 124;
const CRC_PING: u8 = 237;

fn heartbeat_payload(custom_mode: u32) -> Vec<u8> {
    let mut p = Vec::with_capacity(9);
    p.extend_from_slice(&custom_mode.to_le_bytes());
    p.push(2); // type
    p.push(3); // autopilot
    p.push(0); // base_mode
    p.push(4); // system_status
    p.push(3); // mavlink_version
    p
}

fn ping_payload(target_sys: u8, target_comp: u8) -> Vec<u8> {
    let mut p = Vec::with_capacity(14);
    p.extend_from_slice(&0xDEAD_BEEFu64.to_le_bytes());
    p.extend_from_slice(&0xCAFE_BABEu32.to_le_bytes());
    p.push(target_sys);
    p.push(target_comp);
    p
}

fn sys_status_payload() -> Vec<u8> {
    // 31 bytes: 3×u32 + 9×u16/i16 + 1×i8
    let mut p = Vec::with_capacity(31);
    p.extend_from_slice(&0u32.to_le_bytes());
    p.extend_from_slice(&0u32.to_le_bytes());
    p.extend_from_slice(&0u32.to_le_bytes());
    p.extend_from_slice(&100u16.to_le_bytes());
    p.extend_from_slice(&12_500u16.to_le_bytes());
    p.extend_from_slice(&50i16.to_le_bytes());
    p.push(75); // battery_remaining (i8)
    for _ in 0..6 {
        p.extend_from_slice(&0u16.to_le_bytes());
    }
    p
}

#[test]
fn capture_replay_mixed_frames_in_chunks() {
    // 1. v1 HEARTBEAT (known, no targets)
    let f1 = build_v1(0, 1, 11, 1, &heartbeat_payload(0x1111), CRC_HEARTBEAT);
    // 2. v2 unsigned SYS_STATUS (known, no targets)
    let f2 = build_v2(1, 2, 11, 1, &sys_status_payload(), CRC_SYS_STATUS, 0, None);
    // 3. v2 unsigned PING (known, targeted to (7, 9))
    let f3 = build_v2(4, 3, 11, 1, &ping_payload(7, 9), CRC_PING, 0, None);
    // 4. v2 unsigned UNKNOWN msgid — body and CRC bytes are arbitrary; framer
    //    must forward without CRC validation.
    let unknown_msgid: u32 = 0x00B0_B0B0; // 11_579_088, not in the table
    let mut f4: Vec<u8> = vec![
        STX_V2,
        4, // payload_len
        0, // iflags
        0, // cflags
        4, // seq
        11,
        1,
        (unknown_msgid & 0xFF) as u8,
        ((unknown_msgid >> 8) & 0xFF) as u8,
        ((unknown_msgid >> 16) & 0xFF) as u8,
    ];
    f4.extend_from_slice(&[0x11, 0x22, 0x33, 0x44]); // payload
    f4.extend_from_slice(&[0xFF, 0xFF]); // deliberately bogus CRC
    // 5. v2 signed HEARTBEAT (known, signed) — signature is opaque, forwarded
    //    as part of the returned Bytes.
    let sig: [u8; V2_SIGNATURE_LEN] = [
        0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
    ];
    let f5 = build_v2(
        0,
        5,
        11,
        1,
        &heartbeat_payload(0x5555),
        CRC_HEARTBEAT,
        V2_IFLAG_SIGNED,
        Some(&sig),
    );

    let mut stream = Vec::new();
    stream.extend_from_slice(&f1);
    stream.extend_from_slice(&f2);
    stream.extend_from_slice(&f3);
    stream.extend_from_slice(&f4);
    stream.extend_from_slice(&f5);

    let mut framer = Framer::new();
    let mut parsed = Vec::new();

    // Feed in 7-byte chunks — deliberately unaligned with all frame boundaries.
    for chunk in stream.chunks(7) {
        framer.buffer_mut().put_slice(chunk);
        while let Some((header, bytes)) = framer.try_next_frame() {
            parsed.push((header, bytes));
        }
    }

    assert_eq!(parsed.len(), 5, "all five frames must parse");
    assert_eq!(framer.crc_errors(), 0);
    assert_eq!(framer.resync_bytes(), 0);

    // 1. HEARTBEAT v1
    let (h, b) = &parsed[0];
    assert_eq!(h.version, Version::V1);
    assert_eq!(h.msgid, 0);
    assert_eq!(h.seq, 1);
    assert_eq!(h.target_system, None);
    assert_eq!(h.target_component, None);
    assert_eq!(b.len(), f1.len());
    assert_eq!(&b[..], &f1[..]);

    // 2. SYS_STATUS v2
    let (h, b) = &parsed[1];
    assert_eq!(h.version, Version::V2);
    assert_eq!(h.msgid, 1);
    assert_eq!(h.payload_len, 31);
    assert!(!h.is_signed());
    assert_eq!(h.target_system, None);
    assert_eq!(b.len(), f2.len());

    // 3. PING v2 with extracted targets — the "targeted" half of the
    //    "known msgid gets CRC-validated and targeted-routed" check.
    let (h, b) = &parsed[2];
    assert_eq!(h.msgid, 4);
    assert_eq!(h.target_system, Some(7));
    assert_eq!(h.target_component, Some(9));
    assert_eq!(b.len(), f3.len());

    // 4. Unknown msgid — forwarded without CRC check (we built it with a bogus
    //    CRC and the framer accepted it). target_system stays None (broadcast).
    let (h, b) = &parsed[3];
    assert_eq!(h.msgid, unknown_msgid);
    assert_eq!(h.target_system, None);
    assert_eq!(h.target_component, None);
    assert_eq!(b.len(), f4.len());

    // 5. Signed v2 HEARTBEAT — signature trailer must be at the tail of the
    //    returned Bytes, byte-for-byte identical to what we sent.
    let (h, b) = &parsed[4];
    assert!(h.is_signed());
    assert_eq!(h.msgid, 0);
    assert_eq!(b.len(), f5.len());
    assert_eq!(&b[b.len() - V2_SIGNATURE_LEN..], &sig[..]);
}

#[test]
fn corrupted_frame_in_stream_is_dropped_and_following_frame_still_parses() {
    let f_good_a = build_v1(0, 1, 11, 1, &heartbeat_payload(0xA), CRC_HEARTBEAT);
    let mut f_corrupt = build_v1(0, 2, 11, 1, &heartbeat_payload(0xB), CRC_HEARTBEAT);
    let last = f_corrupt.len() - 1;
    f_corrupt[last] ^= 0xFF; // flip high CRC byte
    let f_good_b = build_v1(0, 3, 11, 1, &heartbeat_payload(0xC), CRC_HEARTBEAT);

    let mut stream = Vec::new();
    stream.extend_from_slice(&f_good_a);
    stream.extend_from_slice(&f_corrupt);
    stream.extend_from_slice(&f_good_b);

    let mut framer = Framer::new();
    framer.buffer_mut().put_slice(&stream);

    let mut seqs = Vec::new();
    while let Some((h, _)) = framer.try_next_frame() {
        seqs.push(h.seq);
    }

    assert!(seqs.contains(&1), "first good frame must parse");
    assert!(
        seqs.contains(&3),
        "third good frame must parse despite corrupt frame in middle"
    );
    assert!(!seqs.contains(&2), "corrupt frame must not surface");
    assert!(
        framer.crc_errors() >= 1,
        "crc_errors must reflect the dropped frame"
    );
}
