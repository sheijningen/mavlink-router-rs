//! Compile-time msgid table backing CRC validation and targeted routing.

use super::generated::SORTED;

/// One row of the build-time `SORTED` msgid table — everything the router
/// needs to validate and route a frame of this msgid without parsing the
/// payload.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MsgEntry {
    /// CRC-16-MCRF4XX seed mixed in after the framed bytes.
    pub crc_extra: u8,
    /// Payload-relative offset of the `target_system` field if this msgid
    /// carries one, else `None`. MAVLink wire payload length is u8 (≤ 255),
    /// so the offset fits in u8.
    pub target_sys_offset: Option<u8>,
    /// Payload-relative offset of the `target_component` field if this msgid
    /// carries one, else `None`. MAVLink wire payload length is u8 (≤ 255),
    /// so the offset fits in u8.
    pub target_comp_offset: Option<u8>,
}

pub(crate) fn lookup(msgid: u32) -> Option<&'static MsgEntry> {
    SORTED
        .binary_search_by_key(&msgid, |(id, _)| *id)
        .ok()
        .map(|index| &SORTED[index].1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat() {
        let entry = lookup(0).expect("HEARTBEAT (id 0) must be in the table");
        assert_eq!(entry.crc_extra, 50);
        assert_eq!(entry.target_sys_offset, None);
        assert_eq!(entry.target_comp_offset, None);
    }

    #[test]
    fn sys_status() {
        let entry = lookup(1).expect("SYS_STATUS (id 1) must be in the table");
        assert_eq!(entry.crc_extra, 124);
        assert_eq!(entry.target_sys_offset, None);
    }

    #[test]
    fn ping_target_offsets() {
        let entry = lookup(4).expect("PING (id 4) must be in the table");
        assert_eq!(entry.crc_extra, 237);
        // Wire order: time_usec (u64, 0..8), seq (u32, 8..12),
        //             target_system (u8, 12), target_component (u8, 13).
        assert_eq!(entry.target_sys_offset, Some(12));
        assert_eq!(entry.target_comp_offset, Some(13));
    }

    #[test]
    fn attitude() {
        let entry = lookup(30).expect("ATTITUDE (id 30) must be in the table");
        assert_eq!(entry.crc_extra, 39);
        assert_eq!(entry.target_sys_offset, None);
    }

    #[test]
    fn unknown_msgid_returns_none() {
        assert!(lookup(0x00FF_FFFF).is_none());
    }

    #[test]
    fn sorted_invariant() {
        let mut prev: Option<u32> = None;
        for (id, _) in SORTED {
            if let Some(prev_id) = prev {
                assert!(
                    *id > prev_id,
                    "SORTED not sorted by msgid: {prev_id} then {id}"
                );
            }
            prev = Some(*id);
        }
    }

    #[test]
    fn table_is_nonempty() {
        // common.xml alone is ~230 messages; with ardupilotmega we should be
        // well over 300. A small number means the parser dropped messages.
        assert!(
            SORTED.len() > 200,
            "msgid table too small: {}",
            SORTED.len()
        );
    }

    #[test]
    fn param_request_list_targets_at_start() {
        let entry = lookup(21).expect("PARAM_REQUEST_LIST (id 21) must be in the table");
        // Two uint8_t fields, stable size-sort preserves their declaration order.
        assert_eq!(entry.target_sys_offset, Some(0));
        assert_eq!(entry.target_comp_offset, Some(1));
    }

    #[test]
    fn command_long_targets() {
        let entry = lookup(76).expect("COMMAND_LONG (id 76) must be in the table");
        // Wire sort: 7 floats (28 bytes), command u16 (2), then the three u8s:
        // target_system (30), target_component (31), confirmation (32).
        assert_eq!(entry.target_sys_offset, Some(30));
        assert_eq!(entry.target_comp_offset, Some(31));
    }

    #[test]
    fn change_operator_control_has_sys_but_not_comp() {
        // CHANGE_OPERATOR_CONTROL (id 5) is the only standard targeted message
        // that has target_system without target_component — exercises the
        // half-target offset branch end to end.
        let entry = lookup(5).expect("CHANGE_OPERATOR_CONTROL (id 5) must be in the table");
        assert_eq!(entry.target_sys_offset, Some(0));
        assert_eq!(entry.target_comp_offset, None);
    }
}
