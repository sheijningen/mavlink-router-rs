//! Per-endpoint filter rules and the range types they carry.

use crate::mavlink::frame::NodeId;

use super::spec::SpecError;

/// Inclusive decimal range used inside `allow_msgid_*` and `block_msgid_*`
/// filter lists. Single values parse to `lo == hi`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsgIdRange {
    pub lo: u32,
    pub hi: u32,
}

impl MsgIdRange {
    pub fn single(value: u32) -> Self {
        Self {
            lo: value,
            hi: value,
        }
    }

    pub fn contains(self, value: u32) -> bool {
        self.lo <= value && value <= self.hi
    }
}

/// Inclusive decimal range used inside `allow_src_sys_*`, `block_src_sys_*`,
/// `allow_src_comp_*`, `block_src_comp_*` filter lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct U8Range {
    pub lo: u8,
    pub hi: u8,
}

impl U8Range {
    pub fn single(value: u8) -> Self {
        Self {
            lo: value,
            hi: value,
        }
    }

    pub fn contains(self, value: u8) -> bool {
        self.lo <= value && value <= self.hi
    }
}

/// The 12 per-endpoint filter lists: `allow_*` / `block_*` on the msgid,
/// `src_sys`, and `src_comp` axes for both ingress (`*_in`) and egress
/// (`*_out`). Empty list = no restriction; blocklist wins over allowlist on
/// overlap. The per-frame decision methods [`Filters::passes_in_filter`] and
/// [`Filters::passes_out_filter`] consume this data; the parser side lives
/// in [`Filters::apply`].
///
/// Lives on [`super::identity_flags::IdentityFlags::filters`] alongside the
/// rest of the per-endpoint identity (sniffer / group / capacities). Sub-
/// endpoints inherit the parent listener's `Filters` by clone at spawn time.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Filters {
    pub allow_msgid_in: Vec<MsgIdRange>,
    pub block_msgid_in: Vec<MsgIdRange>,
    pub allow_msgid_out: Vec<MsgIdRange>,
    pub block_msgid_out: Vec<MsgIdRange>,
    pub allow_src_sys_in: Vec<U8Range>,
    pub block_src_sys_in: Vec<U8Range>,
    pub allow_src_sys_out: Vec<U8Range>,
    pub block_src_sys_out: Vec<U8Range>,
    pub allow_src_comp_in: Vec<U8Range>,
    pub block_src_comp_in: Vec<U8Range>,
    pub allow_src_comp_out: Vec<U8Range>,
    pub block_src_comp_out: Vec<U8Range>,
}

impl Filters {
    /// Sorted list of every query key this struct understands. Used by the
    /// parser's "did you mean" suggestion. Kept here so the field set and
    /// the key set don't drift.
    pub const KEYS: &'static [&'static str] = &[
        "allow_msgid_in",
        "allow_msgid_out",
        "allow_src_comp_in",
        "allow_src_comp_out",
        "allow_src_sys_in",
        "allow_src_sys_out",
        "block_msgid_in",
        "block_msgid_out",
        "block_src_comp_in",
        "block_src_comp_out",
        "block_src_sys_in",
        "block_src_sys_out",
    ];

    /// Apply one query key/value pair if it names a filter list. Returns
    /// `Ok(true)` when consumed, `Ok(false)` when the key isn't ours, or
    /// `Err` on a malformed value.
    pub fn apply(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        match key {
            "allow_msgid_in" => {
                self.allow_msgid_in = parse_msgid_ranges(value, "allow_msgid_in")?;
                Ok(true)
            }
            "block_msgid_in" => {
                self.block_msgid_in = parse_msgid_ranges(value, "block_msgid_in")?;
                Ok(true)
            }
            "allow_msgid_out" => {
                self.allow_msgid_out = parse_msgid_ranges(value, "allow_msgid_out")?;
                Ok(true)
            }
            "block_msgid_out" => {
                self.block_msgid_out = parse_msgid_ranges(value, "block_msgid_out")?;
                Ok(true)
            }
            "allow_src_sys_in" => {
                self.allow_src_sys_in = parse_u8_ranges(value, "allow_src_sys_in")?;
                Ok(true)
            }
            "block_src_sys_in" => {
                self.block_src_sys_in = parse_u8_ranges(value, "block_src_sys_in")?;
                Ok(true)
            }
            "allow_src_sys_out" => {
                self.allow_src_sys_out = parse_u8_ranges(value, "allow_src_sys_out")?;
                Ok(true)
            }
            "block_src_sys_out" => {
                self.block_src_sys_out = parse_u8_ranges(value, "block_src_sys_out")?;
                Ok(true)
            }
            "allow_src_comp_in" => {
                self.allow_src_comp_in = parse_u8_ranges(value, "allow_src_comp_in")?;
                Ok(true)
            }
            "block_src_comp_in" => {
                self.block_src_comp_in = parse_u8_ranges(value, "block_src_comp_in")?;
                Ok(true)
            }
            "allow_src_comp_out" => {
                self.allow_src_comp_out = parse_u8_ranges(value, "allow_src_comp_out")?;
                Ok(true)
            }
            "block_src_comp_out" => {
                self.block_src_comp_out = parse_u8_ranges(value, "block_src_comp_out")?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Decide whether a frame with `(msgid, src)` passes the ingress filter
    /// for this endpoint. A frame passes when every axis passes: an empty
    /// `allow_*_in` imposes no restriction; a non-empty `allow_*_in`
    /// requires the value to be in some allow range; a non-empty
    /// `block_*_in` rejects the value if it's in some block range.
    /// **Block wins on overlap** — a value that's simultaneously in an allow
    /// range and a block range is rejected.
    #[must_use]
    pub fn passes_in_filter(&self, msgid: u32, src: NodeId) -> bool {
        pass_msgid_axis(msgid, &self.allow_msgid_in, &self.block_msgid_in)
            && pass_u8_axis(src.sys, &self.allow_src_sys_in, &self.block_src_sys_in)
            && pass_u8_axis(src.comp, &self.allow_src_comp_in, &self.block_src_comp_in)
    }

    /// Decide whether a frame with `(msgid, src)` passes the egress filter
    /// for this endpoint. Same semantics as [`Filters::passes_in_filter`]
    /// applied to the `*_out` lists.
    #[must_use]
    pub fn passes_out_filter(&self, msgid: u32, src: NodeId) -> bool {
        pass_msgid_axis(msgid, &self.allow_msgid_out, &self.block_msgid_out)
            && pass_u8_axis(src.sys, &self.allow_src_sys_out, &self.block_src_sys_out)
            && pass_u8_axis(src.comp, &self.allow_src_comp_out, &self.block_src_comp_out)
    }
}

/// Per-axis decision for the msgid axis (u32 values, [`MsgIdRange`] entries).
/// Empty `allow` = no allow-restriction; non-empty `allow` requires `value` in
/// some allow range; `block` rejects on match regardless.
#[inline]
fn pass_msgid_axis(value: u32, allow: &[MsgIdRange], block: &[MsgIdRange]) -> bool {
    if !allow.is_empty() && !allow.iter().any(|range| range.contains(value)) {
        return false;
    }
    !block.iter().any(|range| range.contains(value))
}

/// Per-axis decision for the `src_sys` / `src_comp` axes (u8 values,
/// [`U8Range`] entries). Mirror of [`pass_msgid_axis`] for the smaller value
/// type; kept separate to avoid a trait shadowing the existing inherent
/// `contains` methods on the range types.
#[inline]
fn pass_u8_axis(value: u8, allow: &[U8Range], block: &[U8Range]) -> bool {
    if !allow.is_empty() && !allow.iter().any(|range| range.contains(value)) {
        return false;
    }
    !block.iter().any(|range| range.contains(value))
}

fn parse_msgid_ranges(value: &str, key: &'static str) -> Result<Vec<MsgIdRange>, SpecError> {
    let mut out = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err(SpecError::InvalidQueryValue {
                key,
                reason: format!("empty entry in '{value}'"),
            });
        }
        let (lo, hi) = parse_range_pair_u32(item, key)?;
        out.push(MsgIdRange { lo, hi });
    }
    Ok(out)
}

fn parse_u8_ranges(value: &str, key: &'static str) -> Result<Vec<U8Range>, SpecError> {
    let mut out = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err(SpecError::InvalidQueryValue {
                key,
                reason: format!("empty entry in '{value}'"),
            });
        }
        let (lo, hi) = parse_range_pair_u32(item, key)?;
        if lo > u8::MAX as u32 || hi > u8::MAX as u32 {
            return Err(SpecError::InvalidQueryValue {
                key,
                reason: format!("value out of u8 range in '{item}'"),
            });
        }
        out.push(U8Range {
            lo: lo as u8,
            hi: hi as u8,
        });
    }
    Ok(out)
}

fn parse_range_pair_u32(item: &str, key: &'static str) -> Result<(u32, u32), SpecError> {
    let (lo, hi) = if let Some((lo_text, hi_text)) = item.split_once('-') {
        let lo: u32 = lo_text
            .trim()
            .parse()
            .map_err(|_| SpecError::InvalidQueryValue {
                key,
                reason: format!("range lower bound '{lo_text}' is not a decimal integer"),
            })?;
        let hi: u32 = hi_text
            .trim()
            .parse()
            .map_err(|_| SpecError::InvalidQueryValue {
                key,
                reason: format!("range upper bound '{hi_text}' is not a decimal integer"),
            })?;
        (lo, hi)
    } else {
        let value: u32 = item.parse().map_err(|_| SpecError::InvalidQueryValue {
            key,
            reason: format!("'{item}' is not a decimal integer or 'lo-hi' range"),
        })?;
        (value, value)
    };
    if lo > hi {
        return Err(SpecError::InvalidQueryValue {
            key,
            reason: format!("range {lo}-{hi} has lo > hi"),
        });
    }
    Ok((lo, hi))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_all_empty() {
        let filters = Filters::default();
        assert!(filters.allow_msgid_in.is_empty());
        assert!(filters.block_msgid_in.is_empty());
        assert!(filters.allow_msgid_out.is_empty());
        assert!(filters.block_msgid_out.is_empty());
        assert!(filters.allow_src_sys_in.is_empty());
        assert!(filters.block_src_sys_in.is_empty());
        assert!(filters.allow_src_sys_out.is_empty());
        assert!(filters.block_src_sys_out.is_empty());
        assert!(filters.allow_src_comp_in.is_empty());
        assert!(filters.block_src_comp_in.is_empty());
        assert!(filters.allow_src_comp_out.is_empty());
        assert!(filters.block_src_comp_out.is_empty());
    }

    #[test]
    fn apply_returns_false_on_non_filter_key() {
        let mut filters = Filters::default();
        assert!(!filters.apply("sniffer", "true").unwrap());
        assert!(!filters.apply("tx_queue_frames", "8").unwrap());
        assert_eq!(filters, Filters::default());
    }

    #[test]
    fn apply_msgid_lists_parse_ranges_and_singles() {
        let mut filters = Filters::default();
        assert!(filters.apply("block_msgid_in", "33,100-150,200").unwrap());
        assert_eq!(
            filters.block_msgid_in,
            vec![
                MsgIdRange::single(33),
                MsgIdRange { lo: 100, hi: 150 },
                MsgIdRange::single(200),
            ]
        );
    }

    #[test]
    fn apply_u8_lists_reject_out_of_range() {
        let mut filters = Filters::default();
        assert!(filters.apply("allow_src_sys_in", "256").is_err());
        assert!(filters.apply("allow_src_sys_in", "100-300").is_err());
    }

    #[test]
    fn apply_rejects_inverted_range() {
        let mut filters = Filters::default();
        assert!(filters.apply("block_msgid_in", "150-100").is_err());
    }

    #[test]
    fn apply_rejects_empty_list_entry() {
        let mut filters = Filters::default();
        assert!(filters.apply("block_msgid_in", "33,,100").is_err());
    }

    #[test]
    fn keys_cover_every_filter_field_handled_by_apply() {
        for key in Filters::KEYS {
            let mut filters = Filters::default();
            let consumed = filters
                .apply(key, "1")
                .unwrap_or_else(|err| panic!("apply({key}, ..) errored: {err}"));
            assert!(
                consumed,
                "Filters::apply({key}) returned false; missing from match arm"
            );
        }
    }

    // ----- per-frame eval (passes_in_filter / passes_out_filter) -----

    #[test]
    fn default_filters_accept_everything() {
        let filters = Filters::default();
        assert!(filters.passes_in_filter(0, NodeId::new(0, 0)));
        assert!(filters.passes_in_filter(u32::MAX, NodeId::new(u8::MAX, u8::MAX)));
        assert!(filters.passes_out_filter(33, NodeId::new(1, 1)));
    }

    #[test]
    fn allow_in_restricts_to_listed_msgids() {
        let filters = Filters {
            allow_msgid_in: vec![MsgIdRange::single(0), MsgIdRange { lo: 30, hi: 40 }],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(0, NodeId::new(1, 1)));
        assert!(filters.passes_in_filter(30, NodeId::new(1, 1)));
        assert!(filters.passes_in_filter(35, NodeId::new(1, 1)));
        assert!(filters.passes_in_filter(40, NodeId::new(1, 1)));
        assert!(!filters.passes_in_filter(29, NodeId::new(1, 1)));
        assert!(!filters.passes_in_filter(41, NodeId::new(1, 1)));
        assert!(!filters.passes_in_filter(100, NodeId::new(1, 1)));
    }

    #[test]
    fn block_in_rejects_listed_msgids() {
        let filters = Filters {
            block_msgid_in: vec![MsgIdRange::single(33), MsgIdRange { lo: 100, hi: 150 }],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(0, NodeId::new(1, 1)));
        assert!(!filters.passes_in_filter(33, NodeId::new(1, 1)));
        assert!(!filters.passes_in_filter(100, NodeId::new(1, 1)));
        assert!(!filters.passes_in_filter(125, NodeId::new(1, 1)));
        assert!(!filters.passes_in_filter(150, NodeId::new(1, 1)));
        assert!(filters.passes_in_filter(151, NodeId::new(1, 1)));
    }

    #[test]
    fn block_wins_over_allow_on_overlap() {
        // CLAUDE.md: "If both are set, Block* wins on overlap."
        let filters = Filters {
            allow_msgid_in: vec![MsgIdRange { lo: 0, hi: 100 }],
            block_msgid_in: vec![MsgIdRange::single(33)],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(0, NodeId::new(1, 1)));
        assert!(filters.passes_in_filter(32, NodeId::new(1, 1)));
        assert!(!filters.passes_in_filter(33, NodeId::new(1, 1)));
        assert!(filters.passes_in_filter(34, NodeId::new(1, 1)));
        // outside allow → reject regardless of block
        assert!(!filters.passes_in_filter(101, NodeId::new(1, 1)));
    }

    #[test]
    fn src_sys_in_axis_independent_of_msgid_axis() {
        let filters = Filters {
            allow_src_sys_in: vec![U8Range::single(1)],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(0, NodeId::new(1, 0)));
        assert!(!filters.passes_in_filter(0, NodeId::new(2, 0)));
        // Block on the same axis works.
        let filters = Filters {
            block_src_sys_in: vec![U8Range::single(255)],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(0, NodeId::new(1, 0)));
        assert!(!filters.passes_in_filter(0, NodeId::new(255, 0)));
    }

    #[test]
    fn src_comp_in_axis_independent_of_other_axes() {
        let filters = Filters {
            allow_src_comp_in: vec![U8Range { lo: 1, hi: 10 }],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(0, NodeId::new(0, 1)));
        assert!(filters.passes_in_filter(0, NodeId::new(0, 10)));
        assert!(!filters.passes_in_filter(0, NodeId::new(0, 11)));
        assert!(!filters.passes_in_filter(0, NodeId::new(0, 0)));
    }

    #[test]
    fn in_filter_requires_every_axis_to_pass() {
        let filters = Filters {
            allow_msgid_in: vec![MsgIdRange::single(0)],
            allow_src_sys_in: vec![U8Range::single(1)],
            allow_src_comp_in: vec![U8Range::single(2)],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(0, NodeId::new(1, 2)));
        // any single axis miss → fail
        assert!(!filters.passes_in_filter(1, NodeId::new(1, 2)));
        assert!(!filters.passes_in_filter(0, NodeId::new(2, 2)));
        assert!(!filters.passes_in_filter(0, NodeId::new(1, 3)));
    }

    #[test]
    fn in_and_out_axes_are_independent() {
        let filters = Filters {
            block_msgid_in: vec![MsgIdRange::single(33)],
            ..Filters::default()
        };
        // _out is untouched by an _in blocklist, and vice versa.
        assert!(!filters.passes_in_filter(33, NodeId::new(1, 1)));
        assert!(filters.passes_out_filter(33, NodeId::new(1, 1)));

        let filters = Filters {
            allow_msgid_out: vec![MsgIdRange::single(0)],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(99, NodeId::new(1, 1)));
        assert!(!filters.passes_out_filter(99, NodeId::new(1, 1)));
    }

    #[test]
    fn out_filter_mirrors_in_filter_logic_on_out_lists() {
        let filters = Filters {
            allow_msgid_out: vec![MsgIdRange { lo: 30, hi: 40 }],
            block_msgid_out: vec![MsgIdRange::single(35)],
            allow_src_sys_out: vec![U8Range::single(1)],
            block_src_comp_out: vec![U8Range::single(99)],
            ..Filters::default()
        };
        assert!(filters.passes_out_filter(30, NodeId::new(1, 1)));
        assert!(filters.passes_out_filter(40, NodeId::new(1, 1)));
        assert!(!filters.passes_out_filter(35, NodeId::new(1, 1))); // block wins
        assert!(!filters.passes_out_filter(29, NodeId::new(1, 1))); // outside allow
        assert!(!filters.passes_out_filter(30, NodeId::new(2, 1))); // src_sys not allowed
        assert!(!filters.passes_out_filter(30, NodeId::new(1, 99))); // src_comp blocked
    }

    #[test]
    fn multi_range_allow_accepts_any_matching_range() {
        let filters = Filters {
            allow_msgid_in: vec![
                MsgIdRange::single(0),
                MsgIdRange { lo: 100, hi: 200 },
                MsgIdRange::single(500),
            ],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(0, NodeId::new(0, 0)));
        assert!(filters.passes_in_filter(100, NodeId::new(0, 0)));
        assert!(filters.passes_in_filter(150, NodeId::new(0, 0)));
        assert!(filters.passes_in_filter(200, NodeId::new(0, 0)));
        assert!(filters.passes_in_filter(500, NodeId::new(0, 0)));
        assert!(!filters.passes_in_filter(1, NodeId::new(0, 0)));
        assert!(!filters.passes_in_filter(201, NodeId::new(0, 0)));
        assert!(!filters.passes_in_filter(499, NodeId::new(0, 0)));
        assert!(!filters.passes_in_filter(501, NodeId::new(0, 0)));
    }

    #[test]
    fn boundary_values_u8_full_range_allow_or_block() {
        // U8Range covering the whole space behaves correctly at the ends.
        let filters = Filters {
            allow_src_sys_in: vec![U8Range { lo: 0, hi: 255 }],
            ..Filters::default()
        };
        assert!(filters.passes_in_filter(0, NodeId::new(0, 0)));
        assert!(filters.passes_in_filter(0, NodeId::new(255, 0)));

        let filters = Filters {
            block_src_comp_in: vec![U8Range { lo: 0, hi: 255 }],
            ..Filters::default()
        };
        // Every value rejected when blocklist covers the whole space.
        assert!(!filters.passes_in_filter(0, NodeId::new(0, 0)));
        assert!(!filters.passes_in_filter(0, NodeId::new(0, 128)));
        assert!(!filters.passes_in_filter(0, NodeId::new(0, 255)));
    }

    // ----- property tests -----
    //
    // CLAUDE.md "Testing strategy" requires proptest coverage for the filter
    // evaluator: "blocklist always wins over allowlist on overlap" plus the
    // surrounding axis-composition / monotonicity invariants. Properties test
    // shapes that example-based tests can't enumerate (any allow/block list,
    // any value, any range overlap).

    use proptest::collection::vec;
    use proptest::prelude::*;

    // Bias the range generators toward the small msgid space where real
    // MAVLink lives so the interesting branches (range matches value) get
    // exercised. A naive uniform `0..=u32::MAX` strategy almost never covers
    // a uniformly-random msgid, which makes monotonicity / acceptance
    // properties degenerate to their no-match branch.
    fn msgid_value() -> impl Strategy<Value = u32> {
        prop_oneof![
            // 80%: plausible MAVLink msgid space (covers the const table).
            8 => 0u32..=512,
            // 20%: full u32 range to keep edge cases reachable.
            2 => any::<u32>(),
        ]
    }

    fn msgid_range() -> impl Strategy<Value = MsgIdRange> {
        prop_oneof![
            8 => (0u32..=512, 0u32..=512),
            2 => (any::<u32>(), any::<u32>()),
        ]
        .prop_map(|(first, second)| MsgIdRange {
            lo: first.min(second),
            hi: first.max(second),
        })
    }

    fn u8_range() -> impl Strategy<Value = U8Range> {
        (0u8..=u8::MAX, 0u8..=u8::MAX).prop_map(|(first, second)| U8Range {
            lo: first.min(second),
            hi: first.max(second),
        })
    }

    fn msgid_range_around(pivot: u32) -> impl Strategy<Value = MsgIdRange> {
        (0u32..=pivot, pivot..=u32::MAX).prop_map(|(lo, hi)| MsgIdRange { lo, hi })
    }

    fn u8_range_around(pivot: u8) -> impl Strategy<Value = U8Range> {
        (0u8..=pivot, pivot..=u8::MAX).prop_map(|(lo, hi)| U8Range { lo, hi })
    }

    proptest! {
        // CLAUDE.md "blocklist always wins over allowlist on overlap": no
        // matter what allow list contains the value, a block range that also
        // covers the value rejects.
        #[test]
        fn block_wins_on_overlap_msgid(
            (msgid, allow_with_msgid, blocker) in any::<u32>().prop_flat_map(|msgid| {
                (
                    Just(msgid),
                    vec(msgid_range_around(msgid), 1..5),
                    msgid_range_around(msgid),
                )
            }),
            extra_allow in vec(msgid_range(), 0..4),
            extra_block in vec(msgid_range(), 0..4),
        ) {
            let mut allow_all = allow_with_msgid;
            allow_all.extend(extra_allow);
            let mut block_all = extra_block;
            block_all.push(blocker);
            let filters = Filters {
                allow_msgid_in: allow_all,
                block_msgid_in: block_all,
                ..Filters::default()
            };
            prop_assert!(!filters.passes_in_filter(msgid, NodeId::new(0, 0)));
        }

        #[test]
        fn block_wins_on_overlap_src_sys(
            (src_sys, allow, blocker) in any::<u8>().prop_flat_map(|src_sys| {
                (
                    Just(src_sys),
                    vec(u8_range_around(src_sys), 1..5),
                    u8_range_around(src_sys),
                )
            }),
        ) {
            let filters = Filters {
                allow_src_sys_in: allow,
                block_src_sys_in: vec![blocker],
                ..Filters::default()
            };
            prop_assert!(!filters.passes_in_filter(0, NodeId::new(src_sys, 0)));
        }

        #[test]
        fn block_wins_on_overlap_src_comp(
            (src_comp, allow, blocker) in any::<u8>().prop_flat_map(|src_comp| {
                (
                    Just(src_comp),
                    vec(u8_range_around(src_comp), 1..5),
                    u8_range_around(src_comp),
                )
            }),
        ) {
            let filters = Filters {
                allow_src_comp_in: allow,
                block_src_comp_in: vec![blocker],
                ..Filters::default()
            };
            prop_assert!(!filters.passes_in_filter(0, NodeId::new(0, src_comp)));
        }

        // Block-wins-on-overlap also applies to the egress filter — the
        // locked decision says nothing distinguishes the in/out semantics
        // beyond the list each filter reads.
        #[test]
        fn block_wins_on_overlap_msgid_out(
            (msgid, allow_with_msgid, blocker) in msgid_value().prop_flat_map(|msgid| {
                (
                    Just(msgid),
                    vec(msgid_range_around(msgid), 1..5),
                    msgid_range_around(msgid),
                )
            }),
            extra_allow in vec(msgid_range(), 0..4),
            extra_block in vec(msgid_range(), 0..4),
        ) {
            let mut allow_all = allow_with_msgid;
            allow_all.extend(extra_allow);
            let mut block_all = extra_block;
            block_all.push(blocker);
            let filters = Filters {
                allow_msgid_out: allow_all,
                block_msgid_out: block_all,
                ..Filters::default()
            };
            prop_assert!(!filters.passes_out_filter(msgid, NodeId::new(0, 0)));
        }

        // Empty allow lists impose no restriction: result equals "not in any
        // block range" across all three axes.
        #[test]
        fn empty_allow_equals_not_blocked(
            msgid in msgid_value(),
            src_sys in any::<u8>(),
            src_comp in any::<u8>(),
            block_msgid in vec(msgid_range(), 0..6),
            block_sys in vec(u8_range(), 0..6),
            block_comp in vec(u8_range(), 0..6),
        ) {
            let filters = Filters {
                block_msgid_in: block_msgid.clone(),
                block_src_sys_in: block_sys.clone(),
                block_src_comp_in: block_comp.clone(),
                ..Filters::default()
            };
            let blocked = block_msgid.iter().any(|range| range.contains(msgid))
                || block_sys.iter().any(|range| range.contains(src_sys))
                || block_comp.iter().any(|range| range.contains(src_comp));
            prop_assert_eq!(filters.passes_in_filter(msgid, NodeId::new(src_sys, src_comp)), !blocked);
        }

        // Non-empty allow: a value matched by no allow range is rejected
        // regardless of the block list.
        #[test]
        fn unlisted_in_nonempty_allow_is_rejected(
            msgid in msgid_value(),
            allow in vec(msgid_range(), 1..5),
            block in vec(msgid_range(), 0..5),
        ) {
            prop_assume!(!allow.iter().any(|range| range.contains(msgid)));
            let filters = Filters {
                allow_msgid_in: allow,
                block_msgid_in: block,
                ..Filters::default()
            };
            prop_assert!(!filters.passes_in_filter(msgid, NodeId::new(0, 0)));
        }

        // Adding a range to the blocklist can only narrow acceptance: a frame
        // already rejected stays rejected.
        #[test]
        fn block_growth_is_monotone_rejection(
            msgid in msgid_value(),
            src_sys in any::<u8>(),
            src_comp in any::<u8>(),
            allow_msgid in vec(msgid_range(), 0..4),
            block_msgid in vec(msgid_range(), 0..4),
            extra_block in msgid_range(),
        ) {
            let mut filters = Filters {
                allow_msgid_in: allow_msgid,
                block_msgid_in: block_msgid,
                ..Filters::default()
            };
            let before = filters.passes_in_filter(msgid, NodeId::new(src_sys, src_comp));
            filters.block_msgid_in.push(extra_block);
            let after = filters.passes_in_filter(msgid, NodeId::new(src_sys, src_comp));
            // Monotone: `before == false` implies `after == false`.
            prop_assert!(before || !after);
        }

        // Adding an allow range when allow is already non-empty can only
        // widen acceptance: a frame already accepted stays accepted.
        #[test]
        fn allow_growth_is_monotone_acceptance(
            msgid in msgid_value(),
            src_sys in any::<u8>(),
            src_comp in any::<u8>(),
            allow_msgid in vec(msgid_range(), 1..4),
            block_msgid in vec(msgid_range(), 0..4),
            extra_allow in msgid_range(),
        ) {
            let mut filters = Filters {
                allow_msgid_in: allow_msgid,
                block_msgid_in: block_msgid,
                ..Filters::default()
            };
            let before = filters.passes_in_filter(msgid, NodeId::new(src_sys, src_comp));
            filters.allow_msgid_in.push(extra_allow);
            let after = filters.passes_in_filter(msgid, NodeId::new(src_sys, src_comp));
            // Monotone: `before == true` implies `after == true`.
            prop_assert!(!before || after);
        }

        // In and Out lists are independent: changes to `*_out` cannot affect
        // passes_in_filter, and vice versa.
        #[test]
        fn out_lists_do_not_affect_in_filter(
            msgid in msgid_value(),
            src_sys in any::<u8>(),
            src_comp in any::<u8>(),
            allow_msgid_in in vec(msgid_range(), 0..4),
            block_msgid_in in vec(msgid_range(), 0..4),
            allow_msgid_out in vec(msgid_range(), 0..4),
            block_msgid_out in vec(msgid_range(), 0..4),
            allow_sys_out in vec(u8_range(), 0..4),
            block_sys_out in vec(u8_range(), 0..4),
            allow_comp_out in vec(u8_range(), 0..4),
            block_comp_out in vec(u8_range(), 0..4),
        ) {
            let baseline = Filters {
                allow_msgid_in: allow_msgid_in.clone(),
                block_msgid_in: block_msgid_in.clone(),
                ..Filters::default()
            };
            let with_out = Filters {
                allow_msgid_in,
                block_msgid_in,
                allow_msgid_out,
                block_msgid_out,
                allow_src_sys_out: allow_sys_out,
                block_src_sys_out: block_sys_out,
                allow_src_comp_out: allow_comp_out,
                block_src_comp_out: block_comp_out,
                ..Filters::default()
            };
            prop_assert_eq!(
                baseline.passes_in_filter(msgid, NodeId::new(src_sys, src_comp)),
                with_out.passes_in_filter(msgid, NodeId::new(src_sys, src_comp)),
            );
        }

        #[test]
        fn in_lists_do_not_affect_out_filter(
            msgid in msgid_value(),
            src_sys in any::<u8>(),
            src_comp in any::<u8>(),
            allow_msgid_out in vec(msgid_range(), 0..4),
            block_msgid_out in vec(msgid_range(), 0..4),
            allow_msgid_in in vec(msgid_range(), 0..4),
            block_msgid_in in vec(msgid_range(), 0..4),
            allow_sys_in in vec(u8_range(), 0..4),
            block_sys_in in vec(u8_range(), 0..4),
            allow_comp_in in vec(u8_range(), 0..4),
            block_comp_in in vec(u8_range(), 0..4),
        ) {
            let baseline = Filters {
                allow_msgid_out: allow_msgid_out.clone(),
                block_msgid_out: block_msgid_out.clone(),
                ..Filters::default()
            };
            let with_in = Filters {
                allow_msgid_out,
                block_msgid_out,
                allow_msgid_in,
                block_msgid_in,
                allow_src_sys_in: allow_sys_in,
                block_src_sys_in: block_sys_in,
                allow_src_comp_in: allow_comp_in,
                block_src_comp_in: block_comp_in,
                ..Filters::default()
            };
            prop_assert_eq!(
                baseline.passes_out_filter(msgid, NodeId::new(src_sys, src_comp)),
                with_in.passes_out_filter(msgid, NodeId::new(src_sys, src_comp)),
            );
        }

        // Axes compose as logical AND: the per-frame decision passes iff
        // every axis would pass on its own.
        #[test]
        fn axes_compose_as_and(
            msgid in msgid_value(),
            src_sys in any::<u8>(),
            src_comp in any::<u8>(),
            allow_msgid in vec(msgid_range(), 0..4),
            block_msgid in vec(msgid_range(), 0..4),
            allow_sys in vec(u8_range(), 0..4),
            block_sys in vec(u8_range(), 0..4),
            allow_comp in vec(u8_range(), 0..4),
            block_comp in vec(u8_range(), 0..4),
        ) {
            let filters = Filters {
                allow_msgid_in: allow_msgid.clone(),
                block_msgid_in: block_msgid.clone(),
                allow_src_sys_in: allow_sys.clone(),
                block_src_sys_in: block_sys.clone(),
                allow_src_comp_in: allow_comp.clone(),
                block_src_comp_in: block_comp.clone(),
                ..Filters::default()
            };
            let msgid_pass = pass_msgid_axis(msgid, &allow_msgid, &block_msgid);
            let sys_pass = pass_u8_axis(src_sys, &allow_sys, &block_sys);
            let comp_pass = pass_u8_axis(src_comp, &allow_comp, &block_comp);
            prop_assert_eq!(
                filters.passes_in_filter(msgid, NodeId::new(src_sys, src_comp)),
                msgid_pass && sys_pass && comp_pass,
            );
        }
    }
}
