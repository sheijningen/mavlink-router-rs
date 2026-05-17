//! Per-endpoint identity bundle: filter rules, sniffer flag, optional group
//! label, and learn/seq-tracker capacities. See [`IdentityFlags`].

use std::sync::Arc;

use super::filters::Filters;
use super::spec::SpecError;
use super::spec::bounds::check_usize_range;

const DEFAULT_LEARN_CAPACITY: usize = 32;
const DEFAULT_SEQ_TRACKER_CAPACITY: usize = 32;

/// `learn_capacity` lower bound — 0 silently clamps to 1 inside
/// `LearnTable::new`, but the parser rejects it so the operator gets a
/// concrete bounds error instead of a hidden clamp.
pub const MIN_LEARN_CAPACITY: usize = 1;

/// `learn_capacity` upper bound. The router uses the table for a linear
/// scan on every routed frame, so growing it past 1024 entries hurts the
/// hot path far more than it helps the rare deployment that genuinely
/// needs >1024 distinct `(sysid, compid)` pairs per endpoint.
pub const MAX_LEARN_CAPACITY: usize = 1024;

/// `seq_tracker_capacity` lower bound — 0 disables the tracker silently
/// via `LearnTable::new`'s clamp, so the parser rejects it.
pub const MIN_SEQ_TRACKER_CAPACITY: usize = 1;

/// `seq_tracker_capacity` upper bound. Same rationale as
/// [`MAX_LEARN_CAPACITY`] — sequence-loss accounting is also a linear
/// scan per-source-endpoint.
pub const MAX_SEQ_TRACKER_CAPACITY: usize = 1024;

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
                let n = parse_usize(value, "learn_capacity")?;
                self.learn_capacity =
                    check_usize_range(n, "learn_capacity", MIN_LEARN_CAPACITY, MAX_LEARN_CAPACITY)?;
                Ok(true)
            }
            "seq_tracker_capacity" => {
                let n = parse_usize(value, "seq_tracker_capacity")?;
                self.seq_tracker_capacity = check_usize_range(
                    n,
                    "seq_tracker_capacity",
                    MIN_SEQ_TRACKER_CAPACITY,
                    MAX_SEQ_TRACKER_CAPACITY,
                )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::filters::{MsgIdRange, U8Range};

    #[test]
    fn default_is_allow_all_no_sniffer_no_group_default_capacities() {
        let f = IdentityFlags::default();
        assert_eq!(f.filters, Filters::default());
        assert!(!f.sniffer);
        assert!(f.group.is_none());
        assert_eq!(f.learn_capacity, DEFAULT_LEARN_CAPACITY);
        assert_eq!(f.seq_tracker_capacity, DEFAULT_SEQ_TRACKER_CAPACITY);
    }

    #[test]
    fn apply_returns_false_on_unknown_key() {
        let mut f = IdentityFlags::default();
        assert!(!f.apply("read_buf_bytes", "8192").unwrap());
        assert_eq!(f, IdentityFlags::default());
    }

    #[test]
    fn apply_sniffer() {
        let mut f = IdentityFlags::default();
        assert!(f.apply("sniffer", "true").unwrap());
        assert!(f.sniffer);
        assert!(f.apply("sniffer", "false").unwrap());
        assert!(!f.sniffer);
        assert!(f.apply("sniffer", "yes").is_err());
    }

    #[test]
    fn apply_group_stores_arc_str() {
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
    fn apply_capacities_overwrite_defaults() {
        let mut f = IdentityFlags::default();
        assert!(f.apply("learn_capacity", "8").unwrap());
        assert!(f.apply("seq_tracker_capacity", "16").unwrap());
        assert_eq!(f.learn_capacity, 8);
        assert_eq!(f.seq_tracker_capacity, 16);
    }

    #[test]
    fn apply_delegates_filter_keys_to_filters() {
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
    fn keys_cover_every_non_filter_field_handled_by_apply() {
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
