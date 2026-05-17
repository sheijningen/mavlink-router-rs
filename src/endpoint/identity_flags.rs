//! Per-endpoint identity bundle: filter rules, sniffer flag, optional group
//! label. See [`IdentityFlags`].

use std::sync::Arc;

use super::filters::Filters;
use super::spec::SpecError;

/// Per-endpoint `(sysid, compid)` learn-table size. Hardcoded — 32 covers
/// any realistic deployment (a multi-drone endpoint sees ~5–10 distinct
/// pairs), and the router scans this LRU on every routed frame so growing
/// it costs more than it ever buys.
pub const LEARN_CAPACITY: usize = 32;

/// Per-endpoint seq-tracker LRU size. Same rationale as [`LEARN_CAPACITY`]:
/// sequence-loss accounting is per-source on a small ring, and 32 is more
/// than any real fleet needs.
pub const SEQ_TRACKER_CAPACITY: usize = 32;

/// Per-endpoint identity bundle: filter rules, sniffer flag, optional group
/// label. Travels on the `*Spec` (not the `*Wiring`) per CLAUDE.md's
/// "Filters, group, sniffer travel with the `*Spec`, not the `*Wiring`"
/// decision — these are per-endpoint identity, not shared plumbing. The
/// parser populates fields directly during query-string apply; missing
/// knobs keep the CLAUDE.md defaults baked in by [`IdentityFlags::default`].
/// Sub-endpoints inherit a clone of the parent's `IdentityFlags` at spawn
/// time.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IdentityFlags {
    pub filters: Filters,
    pub sniffer: bool,
    pub group: Option<Arc<str>>,
}

impl IdentityFlags {
    /// Sorted list of every non-filter identity query key this struct handles
    /// directly. Filter keys live on [`Filters::KEYS`]; the parser's "did you
    /// mean" suggestion walks both. Kept here so the field set and the key
    /// set don't drift.
    pub const KEYS: &'static [&'static str] = &["group", "sniffer"];

    /// Apply one query key/value pair if it names an identity knob. Delegates
    /// filter keys to [`Filters::apply`]; otherwise handles the two
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::filters::{MsgIdRange, U8Range};

    #[test]
    fn default_is_allow_all_no_sniffer_no_group() {
        let f = IdentityFlags::default();
        assert_eq!(f.filters, Filters::default());
        assert!(!f.sniffer);
        assert!(f.group.is_none());
    }

    #[test]
    fn apply_returns_false_on_unknown_key() {
        let mut f = IdentityFlags::default();
        assert!(!f.apply("tx_queue_frames", "8").unwrap());
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
