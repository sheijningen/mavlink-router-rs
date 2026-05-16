//! Test fixtures shared between integration tests.

#![allow(dead_code)]

pub mod mavlink;
pub mod udp;

use mavlink::{Heartbeat, TestFrame};

/// Build a valid MAVLink v1 HEARTBEAT frame with the given seq.
pub fn build_v1_heartbeat(seq: u8) -> Vec<u8> {
    TestFrame::v1_message(&Heartbeat {
        mav_type: 2,
        autopilot: 3,
        mavlink_version: 3,
        ..Heartbeat::default()
    })
    .seq(seq)
    .build()
}

/// Build a valid MAVLink v2 HEARTBEAT frame with the given seq.
pub fn build_v2_heartbeat(seq: u8) -> Vec<u8> {
    TestFrame::v2_message(&Heartbeat {
        mav_type: 2,
        autopilot: 3,
        mavlink_version: 3,
        ..Heartbeat::default()
    })
    .seq(seq)
    .build()
}
