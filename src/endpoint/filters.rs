//! Per-endpoint identity bundle and the filter-range types it carries.

use std::sync::Arc;

use super::spec::SpecError;

const DEFAULT_LEARN_CAPACITY: usize = 32;
const DEFAULT_SEQ_TRACKER_CAPACITY: usize = 32;

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
/// overlap. Phase 5 will hang the per-frame decision methods
/// (`passes_in_filter` / `passes_out_filter`) off this struct; today this is
/// the data half.
///
/// Lives on [`IdentityFlags::filters`] alongside the rest of the per-endpoint
/// identity (sniffer / group / capacities). Sub-endpoints inherit the parent
/// listener's `Filters` by clone at spawn time.
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
}

/// Per-endpoint identity bundle: filter rules, sniffer flag, optional group
/// label, and learn/seq-tracker capacities. Travels on the `*Spec` (not the
/// `*Wiring`) per CLAUDE.md's "Filters, group, sniffer, and learn/seq
/// capacities travel with the `*Spec`, not the `*Wiring`" decision — these
/// are per-endpoint identity, not shared plumbing. The parser populates
/// fields directly during query-string apply; missing knobs keep the
/// CLAUDE.md defaults baked in by [`IdentityFlags::default`]. Sub-endpoints
/// inherit a clone of the parent's `IdentityFlags` at spawn time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityFlags {
    pub filters: Filters,
    pub sniffer: bool,
    pub group: Option<Arc<str>>,
    pub learn_capacity: usize,
    pub seq_tracker_capacity: usize,
}

impl Default for IdentityFlags {
    fn default() -> Self {
        Self {
            filters: Filters::default(),
            sniffer: false,
            group: None,
            learn_capacity: DEFAULT_LEARN_CAPACITY,
            seq_tracker_capacity: DEFAULT_SEQ_TRACKER_CAPACITY,
        }
    }
}

impl IdentityFlags {
    /// Sorted list of every non-filter identity query key this struct handles
    /// directly. Filter keys live on [`Filters::KEYS`]; the parser's "did you
    /// mean" suggestion walks both. Kept here so the field set and the key
    /// set don't drift.
    pub const KEYS: &'static [&'static str] =
        &["group", "learn_capacity", "seq_tracker_capacity", "sniffer"];

    /// Apply one query key/value pair if it names an identity knob. Delegates
    /// filter keys to [`Filters::apply`]; otherwise handles the four
    /// non-filter identity keys directly. Returns `Ok(true)` when consumed,
    /// `Ok(false)` when the key isn't ours (caller falls through to plumbing
    /// / scheme-specific keys), or `Err` on a malformed value.
    pub fn apply(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        if self.filters.apply(key, value)? {
            return Ok(true);
        }
        match key {
            "sniffer" => {
                self.sniffer = parse_bool(value, "sniffer")?;
                Ok(true)
            }
            "group" => {
                self.group = Some(Arc::from(value));
                Ok(true)
            }
            "learn_capacity" => {
                self.learn_capacity = parse_usize(value, "learn_capacity")?;
                Ok(true)
            }
            "seq_tracker_capacity" => {
                self.seq_tracker_capacity = parse_usize(value, "seq_tracker_capacity")?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

fn parse_bool(v: &str, key: &'static str) -> Result<bool, SpecError> {
    match v {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(SpecError::InvalidQueryValue {
            key,
            reason: format!("expected 'true' or 'false', got '{v}'"),
        }),
    }
}

fn parse_usize(v: &str, key: &'static str) -> Result<usize, SpecError> {
    v.parse().map_err(|_| SpecError::InvalidQueryValue {
        key,
        reason: format!("expected a non-negative integer, got '{v}'"),
    })
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

    // --- Filters ---

    #[test]
    fn filters_default_is_all_empty() {
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
    fn filters_apply_returns_false_on_non_filter_key() {
        let mut f = Filters::default();
        assert!(!f.apply("sniffer", "true").unwrap());
        assert!(!f.apply("read_buf_bytes", "8192").unwrap());
        assert_eq!(f, Filters::default());
    }

    #[test]
    fn filters_apply_msgid_lists_parse_ranges_and_singles() {
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
    fn filters_apply_u8_lists_reject_out_of_range() {
        let mut f = Filters::default();
        assert!(f.apply("allow_src_sys_in", "256").is_err());
        assert!(f.apply("allow_src_sys_in", "100-300").is_err());
    }

    #[test]
    fn filters_apply_rejects_inverted_range() {
        let mut f = Filters::default();
        assert!(f.apply("block_msgid_in", "150-100").is_err());
    }

    #[test]
    fn filters_apply_rejects_empty_list_entry() {
        let mut f = Filters::default();
        assert!(f.apply("block_msgid_in", "33,,100").is_err());
    }

    #[test]
    fn filters_keys_cover_every_filter_field_handled_by_apply() {
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

    // --- IdentityFlags ---

    #[test]
    fn identity_default_is_allow_all_no_sniffer_no_group_default_capacities() {
        let f = IdentityFlags::default();
        assert_eq!(f.filters, Filters::default());
        assert!(!f.sniffer);
        assert!(f.group.is_none());
        assert_eq!(f.learn_capacity, DEFAULT_LEARN_CAPACITY);
        assert_eq!(f.seq_tracker_capacity, DEFAULT_SEQ_TRACKER_CAPACITY);
    }

    #[test]
    fn identity_apply_returns_false_on_unknown_key() {
        let mut f = IdentityFlags::default();
        assert!(!f.apply("read_buf_bytes", "8192").unwrap());
        assert_eq!(f, IdentityFlags::default());
    }

    #[test]
    fn identity_apply_sniffer() {
        let mut f = IdentityFlags::default();
        assert!(f.apply("sniffer", "true").unwrap());
        assert!(f.sniffer);
        assert!(f.apply("sniffer", "false").unwrap());
        assert!(!f.sniffer);
        assert!(f.apply("sniffer", "yes").is_err());
    }

    #[test]
    fn identity_apply_group_stores_arc_str() {
        let mut f = IdentityFlags::default();
        assert!(f.apply("group", "uplink").unwrap());
        assert_eq!(f.group.as_deref(), Some("uplink"));
        let cloned = f.clone();
        let (Some(a), Some(b)) = (f.group.as_ref(), cloned.group.as_ref()) else {
            panic!("group should be Some after clone");
        };
        assert!(Arc::ptr_eq(a, b));
    }

    #[test]
    fn identity_apply_capacities_overwrite_defaults() {
        let mut f = IdentityFlags::default();
        assert!(f.apply("learn_capacity", "8").unwrap());
        assert!(f.apply("seq_tracker_capacity", "16").unwrap());
        assert_eq!(f.learn_capacity, 8);
        assert_eq!(f.seq_tracker_capacity, 16);
    }

    #[test]
    fn identity_apply_delegates_filter_keys_to_filters() {
        // Identity::apply must route filter keys through `filters.apply`
        // so external callers (the query parser) don't have to know about
        // the split.
        let mut f = IdentityFlags::default();
        assert!(f.apply("block_msgid_in", "33,100-150").unwrap());
        assert_eq!(
            f.filters.block_msgid_in,
            vec![MsgIdRange::single(33), MsgIdRange { lo: 100, hi: 150 }]
        );
        assert!(f.apply("allow_src_sys_out", "1").unwrap());
        assert_eq!(f.filters.allow_src_sys_out, vec![U8Range::single(1)]);
    }

    #[test]
    fn identity_keys_cover_every_non_filter_field_handled_by_apply() {
        let probe_value = |key: &str| -> &'static str {
            match key {
                "sniffer" => "true",
                "group" => "x",
                _ => "1",
            }
        };
        for k in IdentityFlags::KEYS {
            let mut f = IdentityFlags::default();
            let consumed = f
                .apply(k, probe_value(k))
                .unwrap_or_else(|e| panic!("apply({k}, ..) errored: {e}"));
            assert!(
                consumed,
                "IdentityFlags::apply({k}) returned false; missing from match arm"
            );
        }
    }

    #[test]
    fn identity_and_filter_key_sets_are_disjoint() {
        for ik in IdentityFlags::KEYS {
            assert!(
                !Filters::KEYS.contains(ik),
                "key {ik} appears in both IdentityFlags::KEYS and Filters::KEYS"
            );
        }
    }
}
