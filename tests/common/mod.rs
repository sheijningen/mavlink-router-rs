//! Test fixtures shared between integration tests. Inlined helpers — by
//! convention this repo treats integration tests as outside consumers of the
//! public API, so the CRC and crc_extra constants are repeated here rather
//! than reaching into `pub(crate)` modules.

#![allow(dead_code)]

use rmr::mavlink::frame::{STX_V1, STX_V2};

/// HEARTBEAT (msgid 0) `crc_extra` — known constant; verified by the runtime
/// unit tests in `src/mavlink/crc_extra.rs`.
pub const HEARTBEAT_CRC_EXTRA: u8 = 50;

/// PING (msgid 4) `crc_extra` — same provenance.
pub const PING_CRC_EXTRA: u8 = 237;

fn crc16_update(crc: &mut u16, b: u8) {
    let tmp = b ^ ((*crc & 0xFF) as u8);
    let tmp = tmp ^ (tmp << 4);
    let tmp16 = tmp as u16;
    *crc = (*crc >> 8) ^ (tmp16 << 8) ^ (tmp16 << 3) ^ (tmp16 >> 4);
}

fn frame_crc(after_stx: &[u8], crc_extra: u8) -> [u8; 2] {
    let mut c: u16 = 0xFFFF;
    for &b in after_stx {
        crc16_update(&mut c, b);
    }
    crc16_update(&mut c, crc_extra);
    [(c & 0xFF) as u8, (c >> 8) as u8]
}

/// Build a valid MAVLink v1 HEARTBEAT frame with the given seq.
pub fn build_v1_heartbeat(seq: u8) -> Vec<u8> {
    let payload = heartbeat_payload();
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.push(STX_V1);
    frame.push(payload.len() as u8);
    frame.push(seq);
    frame.push(1); // sysid
    frame.push(1); // compid
    frame.push(0); // msgid (HEARTBEAT)
    frame.extend_from_slice(&payload);
    let crc = frame_crc(&frame[1..], HEARTBEAT_CRC_EXTRA);
    frame.extend_from_slice(&crc);
    frame
}

/// Build a valid MAVLink v2 HEARTBEAT frame with the given seq.
pub fn build_v2_heartbeat(seq: u8) -> Vec<u8> {
    let payload = heartbeat_payload();
    let mut frame = Vec::with_capacity(12 + payload.len());
    frame.push(STX_V2);
    frame.push(payload.len() as u8);
    frame.push(0); // incompat_flags
    frame.push(0); // compat_flags
    frame.push(seq);
    frame.push(1); // sysid
    frame.push(1); // compid
    frame.push(0); // msgid lo
    frame.push(0);
    frame.push(0);
    frame.extend_from_slice(&payload);
    let crc = frame_crc(&frame[1..], HEARTBEAT_CRC_EXTRA);
    frame.extend_from_slice(&crc);
    frame
}

fn heartbeat_payload() -> [u8; 9] {
    [0, 0, 0, 0, 2, 3, 0, 0, 3]
}
