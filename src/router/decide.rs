//! Per-destination routing decision.
//!
//! Pure function consumed by the router task: given one parsed header, the
//! destination endpoint's learn table, and its identity (filters + sniffer
//! flag), decide whether to admit the frame and — on rejection — surface
//! the reason so the caller can bump `out_filter_drops` if and only if the
//! out-filter rejected the frame. The four decision steps documented in
//! CLAUDE.md's "Routing" section are implemented here:
//!
//! 1. Sniffer override — `sniffer = true` bypasses every other step and
//!    accepts. CLAUDE.md: "A sniffer sees every frame the router has
//!    accepted, regardless of target, loop-prevention, or out-filters."
//! 2. Loop prevention — reject if the destination has learned the source
//!    identity (the frame would loop back via a redundant link).
//! 3. Out-filter — apply the destination's `*_out` allow/block lists.
//! 4. Target match — broadcast frames go everywhere; targeted frames need
//!    the destination to have learned the target identity (fully matched
//!    on both sysid and compid, or half-matched on sysid alone when the
//!    msgid carries no compid field or the compid is the 0 wildcard).
//!
//! The caller (the router task) is responsible for excluding the source
//! endpoint from the destination loop before calling [`decide`]; every
//! other admission criterion comes from here.

use crate::endpoint::identity_flags::IdentityFlags;
use crate::mavlink::frame::ParsedHeader;

use super::learn::LearnTable;

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
    if dest_learn.contains(header.sysid, header.compid) {
        return Decision::LoopBlocked;
    }
    if !dest_identity
        .filters
        .passes_out_filter(header.msgid, header.sysid, header.compid)
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
        Some(comp) => learn.contains(target_sys, comp),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::filters::{Filters, MsgIdRange, U8Range};
    use crate::mavlink::frame::Version;
    use std::time::Duration;
    use tokio::time::Instant;

    fn header_at(
        sysid: u8,
        compid: u8,
        target_system: Option<u8>,
        target_component: Option<u8>,
    ) -> ParsedHeader {
        ParsedHeader {
            version: Version::V2,
            sysid,
            compid,
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
            sysid,
            compid,
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
        let mut t = LearnTable::new(8);
        let base = Instant::now();
        for (i, (s, c)) in entries.iter().enumerate() {
            t.touch(*s, *c, base + Duration::from_millis(i as u64));
        }
        t
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
        let h = header_at(1, 1, None, None);
        assert_eq!(
            decide(&h, &empty_learn(), &plain_identity()),
            Decision::Admit
        );
    }

    #[test]
    fn broadcast_target_sys_zero_admitted() {
        let h = header_at(1, 1, Some(0), Some(7));
        assert_eq!(
            decide(&h, &empty_learn(), &plain_identity()),
            Decision::Admit
        );
    }

    #[test]
    fn fully_targeted_match_admitted() {
        let h = header_at(99, 99, Some(5), Some(10));
        assert_eq!(
            decide(&h, &learn_with(&[(5, 10)]), &plain_identity()),
            Decision::Admit
        );
    }

    #[test]
    fn fully_targeted_miss_rejected_as_target_mismatch() {
        let h = header_at(99, 99, Some(5), Some(10));
        assert_eq!(
            decide(&h, &learn_with(&[(5, 11), (6, 10)]), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    #[test]
    fn half_target_compid_zero_matches_any_compid() {
        let h = header_at(99, 99, Some(5), Some(0));
        assert_eq!(
            decide(&h, &learn_with(&[(5, 200)]), &plain_identity()),
            Decision::Admit
        );
        assert_eq!(
            decide(&h, &learn_with(&[(6, 200)]), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    #[test]
    fn half_target_no_compid_field_matches_any_compid() {
        // CHANGE_OPERATOR_CONTROL-shaped: only target_system carried.
        let h = header_at(99, 99, Some(5), None);
        assert_eq!(
            decide(&h, &learn_with(&[(5, 200)]), &plain_identity()),
            Decision::Admit
        );
        assert_eq!(
            decide(&h, &learn_with(&[(6, 200)]), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    #[test]
    fn loop_prevention_drops_known_source() {
        // Even a fully-targeted match must be suppressed when the source
        // identity is in the destination's learn-set.
        let h = header_at(1, 1, Some(5), Some(10));
        let learn = learn_with(&[(1, 1), (5, 10)]);
        assert_eq!(decide(&h, &learn, &plain_identity()), Decision::LoopBlocked);
    }

    #[test]
    fn loop_prevention_beats_broadcast() {
        let h = header_at(1, 1, Some(0), None);
        let learn = learn_with(&[(1, 1)]);
        assert_eq!(decide(&h, &learn, &plain_identity()), Decision::LoopBlocked);
    }

    #[test]
    fn empty_learn_rejects_fully_targeted_frame() {
        let h = header_at(99, 99, Some(5), Some(10));
        assert_eq!(
            decide(&h, &empty_learn(), &plain_identity()),
            Decision::TargetMismatch
        );
    }

    // ----- Out-filter -----

    #[test]
    fn out_filter_block_rejects_with_out_filter_blocked() {
        let h = header_with_msgid(33, 1, 1);
        let id = identity_with_filters(Filters {
            block_msgid_out: vec![MsgIdRange::single(33)],
            ..Filters::default()
        });
        assert_eq!(decide(&h, &empty_learn(), &id), Decision::OutFilterBlocked);
    }

    #[test]
    fn out_filter_block_runs_after_loop_prevent() {
        // Per CLAUDE.md the out-filter is step 3; loop-prevention is step 2
        // (after sniffer). A frame that's both looped *and* blocked must
        // surface as LoopBlocked so the silent-rejection accounting wins —
        // out_filter_drops would lie about the cause.
        let h = header_with_msgid(33, 1, 1);
        let id = identity_with_filters(Filters {
            block_msgid_out: vec![MsgIdRange::single(33)],
            ..Filters::default()
        });
        let learn = learn_with(&[(1, 1)]);
        assert_eq!(decide(&h, &learn, &id), Decision::LoopBlocked);
    }

    #[test]
    fn out_filter_allow_admits_listed_msgid_and_rejects_others() {
        let id = identity_with_filters(Filters {
            allow_msgid_out: vec![MsgIdRange::single(0)],
            ..Filters::default()
        });
        let allowed = header_with_msgid(0, 1, 1);
        let blocked = header_with_msgid(1, 1, 1);
        assert_eq!(decide(&allowed, &empty_learn(), &id), Decision::Admit);
        assert_eq!(
            decide(&blocked, &empty_learn(), &id),
            Decision::OutFilterBlocked
        );
    }

    #[test]
    fn out_filter_src_sys_axis_independent_of_msgid() {
        let id = identity_with_filters(Filters {
            block_src_sys_out: vec![U8Range::single(7)],
            ..Filters::default()
        });
        let h_blocked = header_with_msgid(0, 7, 1);
        let h_passes = header_with_msgid(0, 8, 1);
        assert_eq!(
            decide(&h_blocked, &empty_learn(), &id),
            Decision::OutFilterBlocked
        );
        assert_eq!(decide(&h_passes, &empty_learn(), &id), Decision::Admit);
    }

    #[test]
    fn out_filter_src_comp_axis_independent_of_msgid() {
        let id = identity_with_filters(Filters {
            block_src_comp_out: vec![U8Range::single(9)],
            ..Filters::default()
        });
        let h_blocked = header_with_msgid(0, 1, 9);
        let h_passes = header_with_msgid(0, 1, 10);
        assert_eq!(
            decide(&h_blocked, &empty_learn(), &id),
            Decision::OutFilterBlocked
        );
        assert_eq!(decide(&h_passes, &empty_learn(), &id), Decision::Admit);
    }

    #[test]
    fn out_filter_runs_before_target_match() {
        // A frame that fails target-match AND fails out-filter must surface
        // OutFilterBlocked (step 3 runs before step 4 in the documented
        // order). Important so out_filter_drops captures the policy hit
        // rather than being swallowed by the silent target-mismatch.
        let h = header_at(1, 1, Some(99), Some(99));
        let id = identity_with_filters(Filters {
            block_msgid_out: vec![MsgIdRange::single(0)],
            ..Filters::default()
        });
        assert_eq!(decide(&h, &empty_learn(), &id), Decision::OutFilterBlocked);
    }

    // ----- Sniffer -----

    #[test]
    fn sniffer_admits_even_when_looped() {
        // CLAUDE.md: "A sniffer sees every frame the router has accepted,
        // regardless of target, loop-prevention, or out-filters."
        let h = header_at(1, 1, None, None);
        let learn = learn_with(&[(1, 1)]);
        assert_eq!(decide(&h, &learn, &sniffer_identity()), Decision::Admit);
    }

    #[test]
    fn sniffer_admits_even_when_out_filter_would_block() {
        let h = header_with_msgid(33, 1, 1);
        let id = IdentityFlags {
            sniffer: true,
            filters: Filters {
                block_msgid_out: vec![MsgIdRange::single(33)],
                ..Filters::default()
            },
            ..IdentityFlags::default()
        };
        assert_eq!(decide(&h, &empty_learn(), &id), Decision::Admit);
    }

    #[test]
    fn sniffer_admits_even_when_target_would_mismatch() {
        // Fully-targeted frame to an identity the sniffer has never learned
        // still admits (diagnostic tap behavior).
        let h = header_at(99, 99, Some(5), Some(10));
        assert_eq!(
            decide(&h, &empty_learn(), &sniffer_identity()),
            Decision::Admit
        );
    }
}
