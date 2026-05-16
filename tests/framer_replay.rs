//! End-to-end replay test: feed a realistic byte stream (mixed v1/v2, signed,
//! unknown msgid) through the public `Framer` API in chunks that don't align
//! with frame boundaries, and assert each frame parses correctly.

mod common;

use bytes::BufMut;
use common::mavlink::{Heartbeat, MavPayload, Ping, SysStatus, TestFrame};
use rmr::mavlink::frame::Version;
use rmr::mavlink::framer::Framer;

#[test]
fn capture_replay_mixed_frames_in_chunks() {
    let f1 = TestFrame::v1_message(&Heartbeat {
        custom_mode: 0x1111,
        mav_type: 2,
        autopilot: 3,
        system_status: 4,
        mavlink_version: 3,
        ..Heartbeat::default()
    })
    .seq(1)
    .sysid(11)
    .compid(1)
    .build();

    let f2 = TestFrame::v2_message(&SysStatus {
        load: 100,
        voltage_battery: 12_500,
        current_battery: 50,
        battery_remaining: 75,
        ..SysStatus::default()
    })
    .seq(2)
    .sysid(11)
    .compid(1)
    .build();

    let f3 = TestFrame::v2_message(&Ping {
        time_usec: 0xDEAD_BEEF,
        seq: 0xCAFE_BABE,
        target_system: 7,
        target_component: 9,
    })
    .seq(3)
    .sysid(11)
    .compid(1)
    .build();

    // Unknown msgid: framer must forward as broadcast without CRC validation.
    // Override the CRC with deliberately bogus bytes to prove the framer
    // doesn't try to validate it.
    let unknown_msgid: u32 = 0x00B0_B0B0;
    let f4 = TestFrame::v2(unknown_msgid, 0)
        .seq(4)
        .sysid(11)
        .compid(1)
        .payload([0x11u8, 0x22, 0x33, 0x44])
        .build_with_crc_override(0xFFFF);

    // Signed v2: the 13-byte signature trailer must be forwarded byte-for-byte.
    let sig: [u8; 13] = [
        0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
    ];
    let f5 = TestFrame::v2_message(&Heartbeat {
        custom_mode: 0x5555,
        mav_type: 2,
        autopilot: 3,
        system_status: 4,
        mavlink_version: 3,
        ..Heartbeat::default()
    })
    .seq(5)
    .sysid(11)
    .compid(1)
    .signed(sig)
    .build();

    let mut stream = Vec::new();
    for f in [&f1, &f2, &f3, &f4, &f5] {
        stream.extend_from_slice(f);
    }

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
    assert_eq!(h.msgid, Heartbeat::MSGID);
    assert_eq!(h.seq, 1);
    assert_eq!(h.target_system, None);
    assert_eq!(h.target_component, None);
    assert_eq!(&b[..], &f1[..]);

    // 2. SYS_STATUS v2 — known, no targets.
    let (h, b) = &parsed[1];
    assert_eq!(h.version, Version::V2);
    assert_eq!(h.msgid, SysStatus::MSGID);
    assert_eq!(h.payload_len, 31);
    assert!(!h.is_signed());
    assert_eq!(h.target_system, None);
    assert_eq!(&b[..], &f2[..]);

    // 3. PING v2 with extracted targets — "known msgid gets CRC-validated and
    //    targeted-routed."
    let (h, b) = &parsed[2];
    assert_eq!(h.msgid, Ping::MSGID);
    assert_eq!(h.target_system, Some(7));
    assert_eq!(h.target_component, Some(9));
    assert_eq!(&b[..], &f3[..]);

    // 4. Unknown msgid — forwarded with the bogus CRC, framer made no attempt
    //    to validate.
    let (h, b) = &parsed[3];
    assert_eq!(h.msgid, unknown_msgid);
    assert_eq!(h.target_system, None);
    assert_eq!(h.target_component, None);
    assert_eq!(&b[..], &f4[..]);

    // 5. Signed v2 HEARTBEAT — signature trailer at the tail, byte-for-byte.
    let (h, b) = &parsed[4];
    assert!(h.is_signed());
    assert_eq!(h.msgid, Heartbeat::MSGID);
    assert_eq!(&b[..], &f5[..]);
    assert_eq!(&b[b.len() - sig.len()..], &sig[..]);
}

#[test]
fn corrupted_frame_in_stream_is_dropped_and_following_frame_still_parses() {
    let good_a = TestFrame::v1_message(&Heartbeat {
        custom_mode: 0xA,
        ..Heartbeat::default()
    })
    .seq(1)
    .sysid(11)
    .compid(1)
    .build();

    let corrupt = TestFrame::v1_message(&Heartbeat {
        custom_mode: 0xB,
        ..Heartbeat::default()
    })
    .seq(2)
    .sysid(11)
    .compid(1)
    .build_with_corrupted_crc();

    let good_b = TestFrame::v1_message(&Heartbeat {
        custom_mode: 0xC,
        ..Heartbeat::default()
    })
    .seq(3)
    .sysid(11)
    .compid(1)
    .build();

    let mut stream = Vec::new();
    stream.extend_from_slice(&good_a);
    stream.extend_from_slice(&corrupt);
    stream.extend_from_slice(&good_b);

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
