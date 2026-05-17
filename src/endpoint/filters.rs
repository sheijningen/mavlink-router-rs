//! Per-endpoint filter rules and the range types they carry.

use super::spec::SpecError;

/// Inclusive decimal range used inside `allow_msgid_*` and `block_msgid_*`
/// filter lists. Single values parse to `lo == hi`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsgIdRange {
    pub lo: u32,
    pub hi: u32,
}

impl MsgIdRange {
    pub fn single(n: u32) -> Self {
        Self { lo: n, hi: n }
    }

    pub fn contains(self, x: u32) -> bool {
        self.lo <= x && x <= self.hi
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
    pub fn single(n: u8) -> Self {
        Self { lo: n, hi: n }
    }

    pub fn contains(self, x: u8) -> bool {
        self.lo <= x && x <= self.hi
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

    /// Decide whether a frame with `(msgid, src_sys, src_comp)` passes the
    /// ingress filter for this endpoint. A frame passes when every axis
    /// passes: an empty `allow_*_in` imposes no restriction; a non-empty
    /// `allow_*_in` requires the value to be in some allow range; a
    /// non-empty `block_*_in` rejects the value if it's in some block range.
    /// **Block wins on overlap** — a value that's simultaneously in an allow
    /// range and a block range is rejected.
    #[must_use]
    pub fn passes_in_filter(&self, msgid: u32, src_sys: u8, src_comp: u8) -> bool {
        pass_msgid_axis(msgid, &self.allow_msgid_in, &self.block_msgid_in)
            && pass_u8_axis(src_sys, &self.allow_src_sys_in, &self.block_src_sys_in)
            && pass_u8_axis(src_comp, &self.allow_src_comp_in, &self.block_src_comp_in)
    }

    /// Decide whether a frame with `(msgid, src_sys, src_comp)` passes the
    /// egress filter for this endpoint. Same semantics as
    /// [`Filters::passes_in_filter`] applied to the `*_out` lists.
    #[must_use]
    pub fn passes_out_filter(&self, msgid: u32, src_sys: u8, src_comp: u8) -> bool {
        pass_msgid_axis(msgid, &self.allow_msgid_out, &self.block_msgid_out)
            && pass_u8_axis(src_sys, &self.allow_src_sys_out, &self.block_src_sys_out)
            && pass_u8_axis(src_comp, &self.allow_src_comp_out, &self.block_src_comp_out)
    }
}

/// Per-axis decision for the msgid axis (u32 values, [`MsgIdRange`] entries).
/// Empty `allow` = no allow-restriction; non-empty `allow` requires `value` in
/// some allow range; `block` rejects on match regardless.
#[inline]
fn pass_msgid_axis(value: u32, allow: &[MsgIdRange], block: &[MsgIdRange]) -> bool {
    if !allow.is_empty() && !allow.iter().any(|r| r.contains(value)) {
        return false;
    }
    !block.iter().any(|r| r.contains(value))
}

/// Per-axis decision for the `src_sys` / `src_comp` axes (u8 values,
/// [`U8Range`] entries). Mirror of [`pass_msgid_axis`] for the smaller value
/// type; kept separate to avoid a trait shadowing the existing inherent
/// `contains` methods on the range types.
#[inline]
fn pass_u8_axis(value: u8, allow: &[U8Range], block: &[U8Range]) -> bool {
    if !allow.is_empty() && !allow.iter().any(|r| r.contains(value)) {
        return false;
    }
    !block.iter().any(|r| r.contains(value))
}

fn parse_msgid_ranges(v: &str, key: &'static str) -> Result<Vec<MsgIdRange>, SpecError> {
    let mut out = Vec::new();
    for item in v.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err(SpecError::InvalidQueryValue {
                key,
                reason: format!("empty entry in '{v}'"),
            });
        }
        let (lo, hi) = parse_range_pair_u32(item, key)?;
        out.push(MsgIdRange { lo, hi });
    }
    Ok(out)
}

fn parse_u8_ranges(v: &str, key: &'static str) -> Result<Vec<U8Range>, SpecError> {
    let mut out = Vec::new();
    for item in v.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err(SpecError::InvalidQueryValue {
                key,
                reason: format!("empty entry in '{v}'"),
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
    let (lo, hi) = if let Some((lo_s, hi_s)) = item.split_once('-') {
        let lo: u32 = lo_s
            .trim()
            .parse()
            .map_err(|_| SpecError::InvalidQueryValue {
                key,
                reason: format!("range lower bound '{lo_s}' is not a decimal integer"),
            })?;
        let hi: u32 = hi_s
            .trim()
            .parse()
            .map_err(|_| SpecError::InvalidQueryValue {
                key,
                reason: format!("range upper bound '{hi_s}' is not a decimal integer"),
            })?;
        (lo, hi)
    } else {
        let n: u32 = item.parse().map_err(|_| SpecError::InvalidQueryValue {
            key,
            reason: format!("'{item}' is not a decimal integer or 'lo-hi' range"),
        })?;
        (n, n)
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
        let f = Filters::default();
        assert!(f.allow_msgid_in.is_empty());
        assert!(f.block_msgid_in.is_empty());
        assert!(f.allow_msgid_out.is_empty());
        assert!(f.block_msgid_out.is_empty());
        assert!(f.allow_src_sys_in.is_empty());
        assert!(f.block_src_sys_in.is_empty());
        assert!(f.allow_src_sys_out.is_empty());
        assert!(f.block_src_sys_out.is_empty());
        assert!(f.allow_src_comp_in.is_empty());
        assert!(f.block_src_comp_in.is_empty());
        assert!(f.allow_src_comp_out.is_empty());
        assert!(f.block_src_comp_out.is_empty());
    }

    #[test]
    fn apply_returns_false_on_non_filter_key() {
        let mut f = Filters::default();
        assert!(!f.apply("sniffer", "true").unwrap());
        assert!(!f.apply("read_buf_bytes", "8192").unwrap());
        assert_eq!(f, Filters::default());
    }

    #[test]
    fn apply_msgid_lists_parse_ranges_and_singles() {
        let mut f = Filters::default();
        assert!(f.apply("block_msgid_in", "33,100-150,200").unwrap());
        assert_eq!(
            f.block_msgid_in,
            vec![
                MsgIdRange::single(33),
                MsgIdRange { lo: 100, hi: 150 },
                MsgIdRange::single(200),
            ]
        );
    }

    #[test]
    fn apply_u8_lists_reject_out_of_range() {
        let mut f = Filters::default();
        assert!(f.apply("allow_src_sys_in", "256").is_err());
        assert!(f.apply("allow_src_sys_in", "100-300").is_err());
    }

    #[test]
    fn apply_rejects_inverted_range() {
        let mut f = Filters::default();
        assert!(f.apply("block_msgid_in", "150-100").is_err());
    }

    #[test]
    fn apply_rejects_empty_list_entry() {
        let mut f = Filters::default();
        assert!(f.apply("block_msgid_in", "33,,100").is_err());
    }

    #[test]
    fn keys_cover_every_filter_field_handled_by_apply() {
        for k in Filters::KEYS {
            let mut f = Filters::default();
            let consumed = f
                .apply(k, "1")
                .unwrap_or_else(|e| panic!("apply({k}, ..) errored: {e}"));
            assert!(
                consumed,
                "Filters::apply({k}) returned false; missing from match arm"
            );
        }
    }

    // ----- per-frame eval (passes_in_filter / passes_out_filter) -----

    #[test]
    fn default_filters_accept_everything() {
        let f = Filters::default();
        assert!(f.passes_in_filter(0, 0, 0));
        assert!(f.passes_in_filter(u32::MAX, u8::MAX, u8::MAX));
        assert!(f.passes_out_filter(33, 1, 1));
    }

    #[test]
    fn allow_in_restricts_to_listed_msgids() {
        let f = Filters {
            allow_msgid_in: vec![MsgIdRange::single(0), MsgIdRange { lo: 30, hi: 40 }],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(0, 1, 1));
        assert!(f.passes_in_filter(30, 1, 1));
        assert!(f.passes_in_filter(35, 1, 1));
        assert!(f.passes_in_filter(40, 1, 1));
        assert!(!f.passes_in_filter(29, 1, 1));
        assert!(!f.passes_in_filter(41, 1, 1));
        assert!(!f.passes_in_filter(100, 1, 1));
    }

    #[test]
    fn block_in_rejects_listed_msgids() {
        let f = Filters {
            block_msgid_in: vec![MsgIdRange::single(33), MsgIdRange { lo: 100, hi: 150 }],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(0, 1, 1));
        assert!(!f.passes_in_filter(33, 1, 1));
        assert!(!f.passes_in_filter(100, 1, 1));
        assert!(!f.passes_in_filter(125, 1, 1));
        assert!(!f.passes_in_filter(150, 1, 1));
        assert!(f.passes_in_filter(151, 1, 1));
    }

    #[test]
    fn block_wins_over_allow_on_overlap() {
        // CLAUDE.md: "If both are set, Block* wins on overlap."
        let f = Filters {
            allow_msgid_in: vec![MsgIdRange { lo: 0, hi: 100 }],
            block_msgid_in: vec![MsgIdRange::single(33)],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(0, 1, 1));
        assert!(f.passes_in_filter(32, 1, 1));
        assert!(!f.passes_in_filter(33, 1, 1));
        assert!(f.passes_in_filter(34, 1, 1));
        // outside allow → reject regardless of block
        assert!(!f.passes_in_filter(101, 1, 1));
    }

    #[test]
    fn src_sys_in_axis_independent_of_msgid_axis() {
        let f = Filters {
            allow_src_sys_in: vec![U8Range::single(1)],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(0, 1, 0));
        assert!(!f.passes_in_filter(0, 2, 0));
        // Block on the same axis works.
        let f = Filters {
            block_src_sys_in: vec![U8Range::single(255)],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(0, 1, 0));
        assert!(!f.passes_in_filter(0, 255, 0));
    }

    #[test]
    fn src_comp_in_axis_independent_of_other_axes() {
        let f = Filters {
            allow_src_comp_in: vec![U8Range { lo: 1, hi: 10 }],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(0, 0, 1));
        assert!(f.passes_in_filter(0, 0, 10));
        assert!(!f.passes_in_filter(0, 0, 11));
        assert!(!f.passes_in_filter(0, 0, 0));
    }

    #[test]
    fn in_filter_requires_every_axis_to_pass() {
        let f = Filters {
            allow_msgid_in: vec![MsgIdRange::single(0)],
            allow_src_sys_in: vec![U8Range::single(1)],
            allow_src_comp_in: vec![U8Range::single(2)],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(0, 1, 2));
        // any single axis miss → fail
        assert!(!f.passes_in_filter(1, 1, 2));
        assert!(!f.passes_in_filter(0, 2, 2));
        assert!(!f.passes_in_filter(0, 1, 3));
    }

    #[test]
    fn in_and_out_axes_are_independent() {
        let f = Filters {
            block_msgid_in: vec![MsgIdRange::single(33)],
            ..Filters::default()
        };
        // _out is untouched by an _in blocklist, and vice versa.
        assert!(!f.passes_in_filter(33, 1, 1));
        assert!(f.passes_out_filter(33, 1, 1));

        let f = Filters {
            allow_msgid_out: vec![MsgIdRange::single(0)],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(99, 1, 1));
        assert!(!f.passes_out_filter(99, 1, 1));
    }

    #[test]
    fn out_filter_mirrors_in_filter_logic_on_out_lists() {
        let f = Filters {
            allow_msgid_out: vec![MsgIdRange { lo: 30, hi: 40 }],
            block_msgid_out: vec![MsgIdRange::single(35)],
            allow_src_sys_out: vec![U8Range::single(1)],
            block_src_comp_out: vec![U8Range::single(99)],
            ..Filters::default()
        };
        assert!(f.passes_out_filter(30, 1, 1));
        assert!(f.passes_out_filter(40, 1, 1));
        assert!(!f.passes_out_filter(35, 1, 1)); // block wins
        assert!(!f.passes_out_filter(29, 1, 1)); // outside allow
        assert!(!f.passes_out_filter(30, 2, 1)); // src_sys not allowed
        assert!(!f.passes_out_filter(30, 1, 99)); // src_comp blocked
    }

    #[test]
    fn multi_range_allow_accepts_any_matching_range() {
        let f = Filters {
            allow_msgid_in: vec![
                MsgIdRange::single(0),
                MsgIdRange { lo: 100, hi: 200 },
                MsgIdRange::single(500),
            ],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(0, 0, 0));
        assert!(f.passes_in_filter(100, 0, 0));
        assert!(f.passes_in_filter(150, 0, 0));
        assert!(f.passes_in_filter(200, 0, 0));
        assert!(f.passes_in_filter(500, 0, 0));
        assert!(!f.passes_in_filter(1, 0, 0));
        assert!(!f.passes_in_filter(201, 0, 0));
        assert!(!f.passes_in_filter(499, 0, 0));
        assert!(!f.passes_in_filter(501, 0, 0));
    }

    #[test]
    fn boundary_values_u8_full_range_allow_or_block() {
        // U8Range covering the whole space behaves correctly at the ends.
        let f = Filters {
            allow_src_sys_in: vec![U8Range { lo: 0, hi: 255 }],
            ..Filters::default()
        };
        assert!(f.passes_in_filter(0, 0, 0));
        assert!(f.passes_in_filter(0, 255, 0));

        let f = Filters {
            block_src_comp_in: vec![U8Range { lo: 0, hi: 255 }],
            ..Filters::default()
        };
        // Every value rejected when blocklist covers the whole space.
        assert!(!f.passes_in_filter(0, 0, 0));
        assert!(!f.passes_in_filter(0, 0, 128));
        assert!(!f.passes_in_filter(0, 0, 255));
    }
}
