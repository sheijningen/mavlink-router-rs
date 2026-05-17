//! Per-destination routing decision.
//!
//! Pure functions consumed by the router task: given one parsed header and
//! the destination endpoint's learn table, decide whether to admit the
//! frame. The Phase 5a skeleton implements two of the four decision steps
//! documented in CLAUDE.md's "Routing" section — loop prevention and
//! target match. Sniffer override and out-filter evaluation are Phase 5b
//! deliverables and land in this module alongside.
//!
//! The caller (the router task) is responsible for excluding the source
//! endpoint from the destination loop before calling [`admit_to`]; every
//! other admission criterion comes from here.

use crate::mavlink::frame::ParsedHeader;

use super::learn::LearnTable;

/// Decide whether a frame should be admitted to one destination. Wraps:
///
/// 1. Loop prevention — reject if the destination has already learned the
///    source identity (the frame would loop back via a redundant link).
/// 2. Target match — broadcast frames go everywhere; targeted frames need
///    the destination to have learned the target identity (fully matched
///    on both sysid and compid, or half-matched on sysid alone when the
///    msgid carries no compid field or the compid is the 0 wildcard).
///
/// Loop prevention wins on overlap: a broadcast frame that would otherwise
/// reach the destination is still suppressed if `(srcsys, srccomp)` is in
/// the destination's learn-set.
pub fn admit_to(header: &ParsedHeader, dest_learn: &LearnTable) -> bool {
    if dest_learn.contains(header.sysid, header.compid) {
        return false;
    }
    target_match(header, dest_learn)
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
            incompat_flags: 0,
            compat_flags: 0,
            target_system,
            target_component,
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

    #[test]
    fn broadcast_no_target_field_admitted_with_empty_learn() {
        // msgid has no target field at all (HEARTBEAT-shaped).
        let h = header_at(1, 1, None, None);
        assert!(admit_to(&h, &empty_learn()));
    }

    #[test]
    fn broadcast_target_sys_zero_admitted() {
        let h = header_at(1, 1, Some(0), Some(7));
        assert!(admit_to(&h, &empty_learn()));
    }

    #[test]
    fn fully_targeted_match_admitted() {
        let h = header_at(99, 99, Some(5), Some(10));
        assert!(admit_to(&h, &learn_with(&[(5, 10)])));
    }

    #[test]
    fn fully_targeted_miss_rejected() {
        let h = header_at(99, 99, Some(5), Some(10));
        assert!(!admit_to(&h, &learn_with(&[(5, 11), (6, 10)])));
    }

    #[test]
    fn half_target_compid_zero_matches_any_compid() {
        let h = header_at(99, 99, Some(5), Some(0));
        assert!(admit_to(&h, &learn_with(&[(5, 200)])));
        assert!(!admit_to(&h, &learn_with(&[(6, 200)])));
    }

    #[test]
    fn half_target_no_compid_field_matches_any_compid() {
        // CHANGE_OPERATOR_CONTROL-shaped: only target_system carried.
        let h = header_at(99, 99, Some(5), None);
        assert!(admit_to(&h, &learn_with(&[(5, 200)])));
        assert!(!admit_to(&h, &learn_with(&[(6, 200)])));
    }

    #[test]
    fn loop_prevention_drops_known_source() {
        // Even a fully-targeted match must be suppressed when the source
        // identity is in the destination's learn-set.
        let h = header_at(1, 1, Some(5), Some(10));
        let learn = learn_with(&[(1, 1), (5, 10)]);
        assert!(!admit_to(&h, &learn));
    }

    #[test]
    fn loop_prevention_beats_broadcast() {
        // Loop prevention is checked before target match; broadcast must
        // not bypass it.
        let h = header_at(1, 1, Some(0), None);
        let learn = learn_with(&[(1, 1)]);
        assert!(!admit_to(&h, &learn));
    }

    #[test]
    fn empty_learn_rejects_fully_targeted_frame() {
        // Without prior learning the destination cannot match a targeted
        // frame — it stays unrouted until the destination has heard from
        // the target identity at least once.
        let h = header_at(99, 99, Some(5), Some(10));
        assert!(!admit_to(&h, &empty_learn()));
    }
}
