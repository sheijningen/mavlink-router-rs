//! Per-destination routing decision: sniffer → loop-prevention → out-filter
//! → target-match.

use super::learn::LearnTable;
use crate::endpoint::identity_flags::IdentityFlags;
use crate::mavlink::frame::{NodeId, ParsedHeader};

/// Outcome of the per-destination decision. `Admit` pushes the frame to the
/// destination's TxQueue; `OutFilterBlocked` is the one rejection that the
/// router credits to a counter (`out_filter_drops` on the destination's
/// stats); the other two rejection variants are silent (loop prevention and
/// target-miss are the everyday no-counter cases).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Admit,
    LoopBlocked,
    OutFilterBlocked,
    TargetMismatch,
}

/// Run the per-destination decision pipeline against one destination's
/// learn table and identity. See module docs for the step order; sniffer
/// short-circuits to `Admit`, loop-prevent and target-match are silent
/// rejections, out-filter is the one rejection counted in stats.
pub fn decide(
    header: &ParsedHeader,
    dest_learn: &LearnTable,
    dest_identity: &IdentityFlags,
) -> Decision {
    if dest_identity.sniffer {
        return Decision::Admit;
    }
    if dest_learn.contains(header.source) {
        return Decision::LoopBlocked;
    }
    if !dest_identity
        .filters
        .passes_out_filter(header.msgid, header.source)
    {
        return Decision::OutFilterBlocked;
    }
    if !target_match(header, dest_learn) {
        return Decision::TargetMismatch;
    }
    Decision::Admit
}

fn target_match(header: &ParsedHeader, learn: &LearnTable) -> bool {
    let Some(target_sys) = header.target_system else {
        return true;
    };
    if target_sys == 0 {
        return true;
    }
    match header.target_component {
        None | Some(0) => learn.contains_sys(target_sys),
        Some(comp) => learn.contains(NodeId::new(target_sys, comp)),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::*;
    use crate::endpoint::filters::{Filters, MsgIdRange, U8Range};
    use crate::mavlink::frame::Version;

    fn header_at(
        sysid: u8,
        compid: u8,
        target_system: Option<u8>,
        target_component: Option<u8>,
    ) -> ParsedHeader {
        ParsedHeader {
            version: Version::V2,
            source: NodeId::new(sysid, compid),
            msgid: 0,
            seq: 0,
            payload_len: 0,
            target_system,
            target_component,
        }
    }

    fn header_with_msgid(msgid: u32, sysid: u8, compid: u8) -> ParsedHeader {
        ParsedHeader {
            version: Version::V2,
            source: NodeId::new(sysid, compid),
            msgid,
            seq: 0,
            payload_len: 0,
            target_system: None,
            target_component: None,
        }
    }

    fn empty_learn() -> LearnTable {
        LearnTable::new(8)
    }

    fn learn_with(entries: &[(u8, u8)]) -> LearnTable {
        let mut table = LearnTable::new(8);
        let base = Instant::now();
        for (index, (sysid, compid)) in entries.iter().enumerate() {
            table.touch(
                NodeId::new(*sysid, *compid),
                base + Duration::from_millis(index as u64),
            );
        }
        table
    }

    fn plain_identity() -> IdentityFlags {
        IdentityFlags::default()
    }

    fn sniffer_identity() -> IdentityFlags {
        IdentityFlags {
            sniffer: true,
            ..IdentityFlags::default()
        }
    }

    fn identity_with_filters(filters: Filters) -> IdentityFlags {
        IdentityFlags {
            filters,
            ..IdentityFlags::default()
        }
    }

    #[test]
    fn broadcast_no_target_field_admitted_with_empty_learn() {
        // msgid has no target field at all (HEARTBEAT-shaped).
        let header = header_at(1, 1, None, None);
        assert_eq!(
            decide(&header, &empty_learn(), &plain_identity()),
            Decision::Admit
        );
    }

    #[test]
    fn broadcast_target_sys_zero_admitted() {
        let header = header_at(1, 1, Some(0), Some(7));
        assert_eq!(
            decide(&header, &empty_learn(), &plain_identity()),
            Decision::Admit
        );
    }

    #[test]
    fn fully_targeted_match_admitted() {
        let header = header_at(99, 99, Some(5), Some(10));
        assert_eq!(
            decide(&header, &learn_with(&[(5, 10)]), &plain_identity()),
            Decision::Admit
        );
    }

    #[test]
    fn fully_targeted_miss_rejected_as_target_mismatch() {
        let header = header_at(99, 99, Some(5), Some(10));
        assert_eq!(
            decide(&header, &learn_with(&[(5, 11), (6, 10)]), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    #[test]
    fn half_target_compid_zero_matches_any_compid() {
        let header = header_at(99, 99, Some(5), Some(0));
        assert_eq!(
            decide(&header, &learn_with(&[(5, 200)]), &plain_identity()),
            Decision::Admit
        );
        assert_eq!(
            decide(&header, &learn_with(&[(6, 200)]), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    #[test]
    fn half_target_no_compid_field_matches_any_compid() {
        // CHANGE_OPERATOR_CONTROL-shaped: only target_system carried.
        let header = header_at(99, 99, Some(5), None);
        assert_eq!(
            decide(&header, &learn_with(&[(5, 200)]), &plain_identity()),
            Decision::Admit
        );
        assert_eq!(
            decide(&header, &learn_with(&[(6, 200)]), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    #[test]
    fn loop_prevention_drops_known_source() {
        // Even a fully-targeted match must be suppressed when the source
        // identity is in the destination's learn-set.
        let header = header_at(1, 1, Some(5), Some(10));
        let learn = learn_with(&[(1, 1), (5, 10)]);
        assert_eq!(
            decide(&header, &learn, &plain_identity()),
            Decision::LoopBlocked
        );
    }

    #[test]
    fn loop_prevention_beats_broadcast() {
        let header = header_at(1, 1, Some(0), None);
        let learn = learn_with(&[(1, 1)]);
        assert_eq!(
            decide(&header, &learn, &plain_identity()),
            Decision::LoopBlocked
        );
    }

    #[test]
    fn empty_learn_rejects_fully_targeted_frame() {
        let header = header_at(99, 99, Some(5), Some(10));
        assert_eq!(
            decide(&header, &empty_learn(), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    #[test]
    fn empty_learn_rejects_half_target_compid_zero() {
        // 0-wildcard compid must not bypass the empty-learn check.
        let header = header_at(99, 99, Some(5), Some(0));
        assert_eq!(
            decide(&header, &empty_learn(), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    #[test]
    fn empty_learn_rejects_half_target_no_compid_field() {
        let header = header_at(99, 99, Some(5), None);
        assert_eq!(
            decide(&header, &empty_learn(), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    // ----- Out-filter -----

    #[test]
    fn out_filter_block_rejects_with_out_filter_blocked() {
        let header = header_with_msgid(33, 1, 1);
        let identity = identity_with_filters(Filters {
            block_msgid_out: vec![MsgIdRange::single(33)],
            ..Filters::default()
        });
        assert_eq!(
            decide(&header, &empty_learn(), &identity),
            Decision::OutFilterBlocked
        );
    }

    #[test]
    fn out_filter_block_runs_after_loop_prevent() {
        // A looped-and-blocked frame must surface as LoopBlocked so
        // out_filter_drops doesn't lie about the cause.
        let header = header_with_msgid(33, 1, 1);
        let identity = identity_with_filters(Filters {
            block_msgid_out: vec![MsgIdRange::single(33)],
            ..Filters::default()
        });
        let learn = learn_with(&[(1, 1)]);
        assert_eq!(decide(&header, &learn, &identity), Decision::LoopBlocked);
    }

    #[test]
    fn out_filter_allow_admits_listed_msgid_and_rejects_others() {
        let identity = identity_with_filters(Filters {
            allow_msgid_out: vec![MsgIdRange::single(0)],
            ..Filters::default()
        });
        let allowed = header_with_msgid(0, 1, 1);
        let blocked = header_with_msgid(1, 1, 1);
        assert_eq!(decide(&allowed, &empty_learn(), &identity), Decision::Admit);
        assert_eq!(
            decide(&blocked, &empty_learn(), &identity),
            Decision::OutFilterBlocked
        );
    }

    #[test]
    fn out_filter_src_sys_axis_independent_of_msgid() {
        let identity = identity_with_filters(Filters {
            block_src_sys_out: vec![U8Range::single(7)],
            ..Filters::default()
        });
        let header_blocked = header_with_msgid(0, 7, 1);
        let header_passes = header_with_msgid(0, 8, 1);
        assert_eq!(
            decide(&header_blocked, &empty_learn(), &identity),
            Decision::OutFilterBlocked
        );
        assert_eq!(
            decide(&header_passes, &empty_learn(), &identity),
            Decision::Admit
        );
    }

    #[test]
    fn out_filter_src_comp_axis_independent_of_msgid() {
        let identity = identity_with_filters(Filters {
            block_src_comp_out: vec![U8Range::single(9)],
            ..Filters::default()
        });
        let header_blocked = header_with_msgid(0, 1, 9);
        let header_passes = header_with_msgid(0, 1, 10);
        assert_eq!(
            decide(&header_blocked, &empty_learn(), &identity),
            Decision::OutFilterBlocked
        );
        assert_eq!(
            decide(&header_passes, &empty_learn(), &identity),
            Decision::Admit
        );
    }

    #[test]
    fn out_filter_runs_before_target_match() {
        // Out-filter rejection must surface over target-mismatch so
        // out_filter_drops captures the policy hit.
        let header = header_at(1, 1, Some(99), Some(99));
        let identity = identity_with_filters(Filters {
            block_msgid_out: vec![MsgIdRange::single(0)],
            ..Filters::default()
        });
        assert_eq!(
            decide(&header, &empty_learn(), &identity),
            Decision::OutFilterBlocked
        );
    }

    // ----- Sniffer -----

    #[test]
    fn sniffer_admits_even_when_looped() {
        let header = header_at(1, 1, None, None);
        let learn = learn_with(&[(1, 1)]);
        assert_eq!(
            decide(&header, &learn, &sniffer_identity()),
            Decision::Admit
        );
    }

    #[test]
    fn sniffer_admits_even_when_out_filter_would_block() {
        let header = header_with_msgid(33, 1, 1);
        let identity = IdentityFlags {
            sniffer: true,
            filters: Filters {
                block_msgid_out: vec![MsgIdRange::single(33)],
                ..Filters::default()
            },
            ..IdentityFlags::default()
        };
        assert_eq!(decide(&header, &empty_learn(), &identity), Decision::Admit);
    }

    #[test]
    fn sniffer_admits_even_when_target_would_mismatch() {
        // Fully-targeted frame to an identity the sniffer has never learned
        // still admits (diagnostic tap behavior).
        let header = header_at(99, 99, Some(5), Some(10));
        assert_eq!(
            decide(&header, &empty_learn(), &sniffer_identity()),
            Decision::Admit
        );
    }
}
