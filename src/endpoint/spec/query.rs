use std::collections::BTreeSet;

use super::super::filters::{Filters, IdentityFlags};
use super::endpoint_kinds::{
    CommonQuery, SerialEndpoint, SerialFlowControl, TcpClientEndpoint, TcpServerEndpoint,
    UdpClientEndpoint, UdpServerEndpoint,
};
use super::error::SpecError;

/// Plumbing keys handled by [`CommonQuery::apply`]. Identity-side keys live
/// on [`IdentityFlags::KEYS`] and [`Filters::KEYS`]; the "did you mean"
/// suggestion walks all three.
pub const COMMON_KEYS: &[&str] = &["read_buf_bytes", "tx_queue_frames"];

const SERIAL_EXTRA: &[&str] = &["flow_control", "serial_reopen_ms"];
const UDPS_EXTRA: &[&str] = &["idle_secs", "udps_peer_capacity"];
const UDPC_EXTRA: &[&str] = &["latch_idle_secs"];
const TCPS_EXTRA: &[&str] = &[];
const TCPC_EXTRA: &[&str] = &["reconnect_initial_ms", "reconnect_max_ms"];

fn known_keys_for(scheme: &str) -> &'static [&'static str] {
    match scheme {
        "serial" => SERIAL_EXTRA,
        "udps" => UDPS_EXTRA,
        "udpc" => UDPC_EXTRA,
        "tcps" => TCPS_EXTRA,
        "tcpc" => TCPC_EXTRA,
        _ => &[],
    }
}

/// Tokenise an `&key=value` query string into ordered pairs, rejecting empty
/// keys and duplicates. Pairs preserve insertion order so the applier can
/// report the first offending key in error messages.
pub fn parse_query_pairs(s: &str) -> Result<Vec<(String, String)>, SpecError> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    if s.is_empty() {
        return Ok(out);
    }
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').ok_or_else(|| {
            SpecError::MalformedQuery(format!("expected '<key>=<value>' (got '{pair}')"))
        })?;
        if k.is_empty() {
            return Err(SpecError::MalformedQuery(format!("empty key in '{pair}'")));
        }
        if !seen.insert(k.to_string()) {
            return Err(SpecError::DuplicateQueryKey(k.to_string()));
        }
        out.push((k.to_string(), v.to_string()));
    }
    Ok(out)
}

impl CommonQuery {
    /// Apply one key/value pair if it names a plumbing knob. Returns
    /// `Ok(true)` when the key was consumed, `Ok(false)` when it isn't a
    /// plumbing-key name (caller falls through to identity / scheme-specific
    /// handling), or `Err` on a malformed value.
    pub fn apply(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        match key {
            "read_buf_bytes" => {
                self.read_buf_bytes = Some(parse_usize(value, "read_buf_bytes")?);
                Ok(true)
            }
            "tx_queue_frames" => {
                self.tx_queue_frames = Some(parse_usize(value, "tx_queue_frames")?);
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// Per-scheme adapter that knows how to set scheme-specific knobs and then
/// falls through to [`IdentityFlags::apply`] and [`CommonQuery::apply`] for
/// shared knobs.
pub trait QueryApplier {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError>;
}

fn apply_shared(
    identity: &mut IdentityFlags,
    common: &mut CommonQuery,
    key: &str,
    value: &str,
) -> Result<bool, SpecError> {
    if identity.apply(key, value)? {
        return Ok(true);
    }
    common.apply(key, value)
}

pub struct SerialApplier<'a>(pub &'a mut SerialEndpoint);
impl QueryApplier for SerialApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        match key {
            "serial_reopen_ms" => {
                self.0.serial_reopen_ms = Some(parse_u64(value, "serial_reopen_ms")?);
                Ok(true)
            }
            "flow_control" => {
                self.0.flow_control = parse_flow_control(value)?;
                Ok(true)
            }
            _ => apply_shared(&mut self.0.identity, &mut self.0.common, key, value),
        }
    }
}

fn parse_flow_control(v: &str) -> Result<SerialFlowControl, SpecError> {
    match v {
        "none" => Ok(SerialFlowControl::None),
        "rtscts" => Ok(SerialFlowControl::RtsCts),
        _ => Err(SpecError::InvalidQueryValue {
            key: "flow_control",
            reason: format!("expected 'none' or 'rtscts', got '{v}'"),
        }),
    }
}

pub struct UdpServerApplier<'a>(pub &'a mut UdpServerEndpoint);
impl QueryApplier for UdpServerApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        match key {
            "idle_secs" => {
                self.0.idle_secs = Some(parse_u64(value, "idle_secs")?);
                Ok(true)
            }
            "udps_peer_capacity" => {
                self.0.udps_peer_capacity = Some(parse_usize(value, "udps_peer_capacity")?);
                Ok(true)
            }
            _ => apply_shared(&mut self.0.identity, &mut self.0.common, key, value),
        }
    }
}

pub struct UdpClientApplier<'a>(pub &'a mut UdpClientEndpoint);
impl QueryApplier for UdpClientApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        if key == "latch_idle_secs" {
            self.0.latch_idle_secs = Some(parse_u64(value, "latch_idle_secs")?);
            return Ok(true);
        }
        apply_shared(&mut self.0.identity, &mut self.0.common, key, value)
    }
}

pub struct TcpServerApplier<'a>(pub &'a mut TcpServerEndpoint);
impl QueryApplier for TcpServerApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        apply_shared(&mut self.0.identity, &mut self.0.common, key, value)
    }
}

pub struct TcpClientApplier<'a>(pub &'a mut TcpClientEndpoint);
impl QueryApplier for TcpClientApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        match key {
            "reconnect_initial_ms" => {
                self.0.reconnect_initial_ms = Some(parse_u64(value, "reconnect_initial_ms")?);
                Ok(true)
            }
            "reconnect_max_ms" => {
                self.0.reconnect_max_ms = Some(parse_u64(value, "reconnect_max_ms")?);
                Ok(true)
            }
            _ => apply_shared(&mut self.0.identity, &mut self.0.common, key, value),
        }
    }
}

/// Walk `pairs` and invoke the applier for each. Unknown keys produce an
/// [`SpecError::UnknownQueryKey`] with a scheme-aware did-you-mean suggestion.
pub fn apply_pairs(
    applier: &mut dyn QueryApplier,
    scheme: &'static str,
    pairs: &[(String, String)],
) -> Result<(), SpecError> {
    for (k, v) in pairs {
        let handled = applier.set(k, v)?;
        if !handled {
            return Err(SpecError::UnknownQueryKey {
                scheme,
                key: k.clone(),
                suggestion: suggest_query_key(scheme, k),
            });
        }
    }
    Ok(())
}

fn suggest_query_key(scheme: &str, unknown: &str) -> Option<&'static str> {
    let extras = known_keys_for(scheme);
    COMMON_KEYS
        .iter()
        .chain(IdentityFlags::KEYS.iter())
        .chain(Filters::KEYS.iter())
        .chain(extras.iter())
        .map(|k| (*k, levenshtein(unknown, k)))
        .filter(|(_, d)| *d <= 3)
        .min_by_key(|(_, d)| *d)
        .map(|(k, _)| k)
}

pub fn levenshtein(a: &str, b: &str) -> usize {
    let m = a.chars().count();
    let n = b.chars().count();
    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }
    let b_chars: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr: Vec<usize> = vec![0; n + 1];
    for (i, ca) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, &cb) in b_chars.iter().enumerate() {
            let cost = usize::from(ca != cb);
            let del = prev[j + 1] + 1;
            let ins = curr[j] + 1;
            let sub = prev[j] + cost;
            curr[j + 1] = del.min(ins).min(sub);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

fn parse_u64(v: &str, key: &'static str) -> Result<u64, SpecError> {
    v.parse().map_err(|_| SpecError::InvalidQueryValue {
        key,
        reason: format!("expected a non-negative integer, got '{v}'"),
    })
}

fn parse_usize(v: &str, key: &'static str) -> Result<usize, SpecError> {
    v.parse().map_err(|_| SpecError::InvalidQueryValue {
        key,
        reason: format!("expected a non-negative integer, got '{v}'"),
    })
}

#[cfg(test)]
mod tests {
    use super::{COMMON_KEYS, levenshtein};
    use crate::endpoint::filters::{Filters, IdentityFlags, MsgIdRange, U8Range};
    use crate::endpoint::spec::{
        EndpointKind, EndpointSpec, SerialEndpoint, SpecError, TcpClientEndpoint,
        UdpClientEndpoint, UdpServerEndpoint,
    };

    fn parse_ok(input: &str) -> EndpointSpec {
        EndpointSpec::parse(input).unwrap_or_else(|e| panic!("expected ok for {input:?}, got {e}"))
    }

    fn parse_err(input: &str) -> SpecError {
        EndpointSpec::parse(input).expect_err(&format!("expected err for {input:?}"))
    }

    fn as_serial(spec: &EndpointSpec) -> &SerialEndpoint {
        match &spec.kind {
            EndpointKind::Serial(e) => e,
            other => panic!("expected serial, got {other:?}"),
        }
    }

    fn as_udps(spec: &EndpointSpec) -> &UdpServerEndpoint {
        match &spec.kind {
            EndpointKind::UdpServer(e) => e,
            other => panic!("expected udps, got {other:?}"),
        }
    }

    fn as_udpc(spec: &EndpointSpec) -> &UdpClientEndpoint {
        match &spec.kind {
            EndpointKind::UdpClient(e) => e,
            other => panic!("expected udpc, got {other:?}"),
        }
    }

    fn as_tcpc(spec: &EndpointSpec) -> &TcpClientEndpoint {
        match &spec.kind {
            EndpointKind::TcpClient(e) => e,
            other => panic!("expected tcpc, got {other:?}"),
        }
    }

    // -- sniffer bool parsing --

    #[test]
    fn udps_with_sniffer_query() {
        let s = parse_ok("udps:0.0.0.0:14551#tap?sniffer=true");
        let e = as_udps(&s);
        assert_eq!(s.name, "tap");
        assert!(e.identity.sniffer);
    }

    #[test]
    fn sniffer_false_explicit() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1?sniffer=false")).clone();
        assert!(!e.identity.sniffer);
    }

    #[test]
    fn sniffer_default_is_false() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1")).clone();
        assert!(!e.identity.sniffer);
    }

    #[test]
    fn sniffer_invalid_value_rejected() {
        match parse_err("udps:0.0.0.0:1?sniffer=yes") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "sniffer"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    // -- msgid filter list parsing --

    #[test]
    fn msgid_filter_list_typed() {
        let s = parse_ok("tcpc:gcs.local:5760?block_msgid_in=33,100-150,32");
        let e = as_tcpc(&s);
        assert_eq!(
            e.identity.filters.block_msgid_in,
            vec![
                MsgIdRange::single(33),
                MsgIdRange { lo: 100, hi: 150 },
                MsgIdRange::single(32),
            ]
        );
    }

    #[test]
    fn msgid_filter_list_with_whitespace() {
        let s = parse_ok("tcpc:gcs.local:5760?allow_msgid_out=1, 2 , 3-5");
        let e = as_tcpc(&s);
        assert_eq!(e.identity.filters.allow_msgid_out.len(), 3);
        assert_eq!(
            e.identity.filters.allow_msgid_out[2],
            MsgIdRange { lo: 3, hi: 5 }
        );
    }

    #[test]
    fn msgid_filter_list_lo_gt_hi_rejected() {
        match parse_err("tcpc:gcs.local:5760?block_msgid_in=10-5") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "block_msgid_in"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn msgid_filter_list_hex_rejected() {
        match parse_err("tcpc:gcs.local:5760?block_msgid_in=0x21") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "block_msgid_in"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn msgid_filter_list_symbolic_name_rejected() {
        match parse_err("tcpc:gcs.local:5760?block_msgid_in=HEARTBEAT") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "block_msgid_in"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn msgid_filter_empty_entry_rejected() {
        match parse_err("tcpc:gcs.local:5760?block_msgid_in=1,,2") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "block_msgid_in"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    // -- u8 (src_sys / src_comp) filter list parsing --

    #[test]
    fn src_sys_filter_typed_as_u8() {
        let s = parse_ok("tcpc:gcs.local:5760?allow_src_sys_out=1,5-10,200");
        let e = as_tcpc(&s);
        assert_eq!(
            e.identity.filters.allow_src_sys_out,
            vec![
                U8Range::single(1),
                U8Range { lo: 5, hi: 10 },
                U8Range::single(200),
            ]
        );
    }

    #[test]
    fn src_sys_filter_overflow_rejected() {
        match parse_err("tcpc:gcs.local:5760?allow_src_sys_out=256") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "allow_src_sys_out"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    // -- scheme-specific knobs reach their scheme --

    #[test]
    fn serial_reopen_ms_typed() {
        let e = as_serial(&parse_ok("serial:/dev/foo:9600?serial_reopen_ms=750")).clone();
        assert_eq!(e.serial_reopen_ms, Some(750));
    }

    #[test]
    fn serial_flow_control_default_none() {
        let e = as_serial(&parse_ok("serial:/dev/foo:9600")).clone();
        assert_eq!(
            e.flow_control,
            crate::endpoint::spec::SerialFlowControl::None
        );
    }

    #[test]
    fn serial_flow_control_rtscts() {
        let e = as_serial(&parse_ok("serial:/dev/foo:9600?flow_control=rtscts")).clone();
        assert_eq!(
            e.flow_control,
            crate::endpoint::spec::SerialFlowControl::RtsCts
        );
    }

    #[test]
    fn serial_flow_control_explicit_none() {
        let e = as_serial(&parse_ok("serial:/dev/foo:9600?flow_control=none")).clone();
        assert_eq!(
            e.flow_control,
            crate::endpoint::spec::SerialFlowControl::None
        );
    }

    #[test]
    fn serial_flow_control_invalid_value_rejected() {
        match parse_err("serial:/dev/foo:9600?flow_control=hw") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "flow_control"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn serial_flow_control_rejected_on_non_serial_scheme() {
        match parse_err("udps:0.0.0.0:1?flow_control=rtscts") {
            SpecError::UnknownQueryKey { key, scheme, .. } => {
                assert_eq!(key, "flow_control");
                assert_eq!(scheme, "udps");
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn idle_secs_typed_on_udps() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:14550?idle_secs=30")).clone();
        assert_eq!(e.idle_secs, Some(30));
    }

    #[test]
    fn tcpc_with_group() {
        let s = parse_ok("tcpc:companion.local:5760#vehicle?group=uplink");
        let e = as_tcpc(&s);
        assert_eq!(e.identity.group.as_deref(), Some("uplink"));
        assert_eq!(s.name, "vehicle");
    }

    // -- scheme-specific knobs are rejected on the wrong scheme --

    #[test]
    fn udpc_specific_key_on_udps_is_unknown() {
        match parse_err("udps:0.0.0.0:14550?latch_idle_secs=15") {
            SpecError::UnknownQueryKey { key, scheme, .. } => {
                assert_eq!(key, "latch_idle_secs");
                assert_eq!(scheme, "udps");
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn udps_specific_key_on_udpc_is_unknown() {
        match parse_err("udpc:1.2.3.4:14550?idle_secs=15") {
            SpecError::UnknownQueryKey { key, scheme, .. } => {
                assert_eq!(key, "idle_secs");
                assert_eq!(scheme, "udpc");
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn tcpc_specific_key_on_tcps_is_unknown() {
        match parse_err("tcps:0.0.0.0:5760?reconnect_initial_ms=250") {
            SpecError::UnknownQueryKey { key, scheme, .. } => {
                assert_eq!(key, "reconnect_initial_ms");
                assert_eq!(scheme, "tcps");
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn serial_specific_key_on_udps_is_unknown() {
        match parse_err("udps:0.0.0.0:1?serial_reopen_ms=1000") {
            SpecError::UnknownQueryKey { key, scheme, .. } => {
                assert_eq!(key, "serial_reopen_ms");
                assert_eq!(scheme, "udps");
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    // -- did-you-mean suggestions --

    #[test]
    fn unknown_query_key_with_close_suggestion() {
        match parse_err("udps:0.0.0.0:1?snifer=true") {
            SpecError::UnknownQueryKey {
                key, suggestion, ..
            } => {
                assert_eq!(key, "snifer");
                assert_eq!(suggestion, Some("sniffer"));
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn unknown_query_key_with_near_match() {
        match parse_err("udps:0.0.0.0:1?tx_queue_frame=10") {
            SpecError::UnknownQueryKey {
                key,
                suggestion: Some("tx_queue_frames"),
                ..
            } => {
                assert_eq!(key, "tx_queue_frame");
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn unknown_query_key_no_suggestion_when_distant() {
        match parse_err("udps:0.0.0.0:1?xyz=1") {
            SpecError::UnknownQueryKey {
                key,
                suggestion: None,
                ..
            } => assert_eq!(key, "xyz"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    // -- query-string pair tokenisation --

    #[test]
    fn duplicate_query_key_fails() {
        assert!(matches!(
            parse_err("udps:0.0.0.0:1?sniffer=true&sniffer=false"),
            SpecError::DuplicateQueryKey(k) if k == "sniffer"
        ));
    }

    #[test]
    fn malformed_query_no_eq_fails() {
        assert!(matches!(
            parse_err("udps:0.0.0.0:1?just_a_key"),
            SpecError::MalformedQuery(_)
        ));
    }

    #[test]
    fn malformed_query_empty_key_fails() {
        assert!(matches!(
            parse_err("udps:0.0.0.0:1?=value"),
            SpecError::MalformedQuery(_)
        ));
    }

    #[test]
    fn empty_query_after_question_mark_ok() {
        let s = parse_ok("udps:0.0.0.0:1?");
        let e = as_udps(&s);
        assert!(e.identity.group.is_none());
        assert!(!e.identity.sniffer);
    }

    #[test]
    fn trailing_ampersand_tolerated() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1?sniffer=true&")).clone();
        assert!(e.identity.sniffer);
    }

    #[test]
    fn empty_value_for_group_ok() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1?group=")).clone();
        assert_eq!(e.identity.group.as_deref(), Some(""));
    }

    #[test]
    fn value_with_embedded_equals_kept_intact() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1?group=a=b")).clone();
        assert_eq!(e.identity.group.as_deref(), Some("a=b"));
    }

    // -- comprehensive coverage --

    #[test]
    fn all_filter_keys_accepted_on_tcpc() {
        let q = "allow_msgid_in=1&block_msgid_in=2&allow_msgid_out=3&block_msgid_out=4\
                 &allow_src_sys_in=5&block_src_sys_in=6&allow_src_sys_out=7&block_src_sys_out=8\
                 &allow_src_comp_in=9&block_src_comp_in=10&allow_src_comp_out=11&block_src_comp_out=12";
        let e = as_tcpc(&parse_ok(&format!("tcpc:x:1?{q}"))).clone();
        assert_eq!(
            e.identity.filters.allow_msgid_in,
            vec![MsgIdRange::single(1)]
        );
        assert_eq!(
            e.identity.filters.block_msgid_in,
            vec![MsgIdRange::single(2)]
        );
        assert_eq!(
            e.identity.filters.allow_msgid_out,
            vec![MsgIdRange::single(3)]
        );
        assert_eq!(
            e.identity.filters.block_msgid_out,
            vec![MsgIdRange::single(4)]
        );
        assert_eq!(
            e.identity.filters.allow_src_sys_in,
            vec![U8Range::single(5)]
        );
        assert_eq!(
            e.identity.filters.block_src_sys_in,
            vec![U8Range::single(6)]
        );
        assert_eq!(
            e.identity.filters.allow_src_sys_out,
            vec![U8Range::single(7)]
        );
        assert_eq!(
            e.identity.filters.block_src_sys_out,
            vec![U8Range::single(8)]
        );
        assert_eq!(
            e.identity.filters.allow_src_comp_in,
            vec![U8Range::single(9)]
        );
        assert_eq!(
            e.identity.filters.block_src_comp_in,
            vec![U8Range::single(10)]
        );
        assert_eq!(
            e.identity.filters.allow_src_comp_out,
            vec![U8Range::single(11)]
        );
        assert_eq!(
            e.identity.filters.block_src_comp_out,
            vec![U8Range::single(12)]
        );
    }

    #[test]
    fn no_query_leaves_identity_at_default() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1")).clone();
        assert_eq!(e.identity, IdentityFlags::default());
    }

    #[test]
    fn plumbing_keys_typed() {
        let e = as_tcpc(&parse_ok(
            "tcpc:x:1?read_buf_bytes=4096&tx_queue_frames=128",
        ))
        .clone();
        assert_eq!(e.common.read_buf_bytes, Some(4096));
        assert_eq!(e.common.tx_queue_frames, Some(128));
    }

    // -- helpers / invariants --

    #[test]
    fn levenshtein_known_pairs() {
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("snifer", "sniffer"), 1);
    }

    #[test]
    fn common_keys_sorted_and_unique() {
        let mut copy: Vec<&&str> = COMMON_KEYS.iter().collect();
        copy.sort();
        copy.dedup();
        assert_eq!(copy.len(), COMMON_KEYS.len(), "duplicates present");
        for w in COMMON_KEYS.windows(2) {
            assert!(w[0] < w[1], "not sorted: {} >= {}", w[0], w[1]);
        }
    }

    #[test]
    fn identity_keys_sorted_and_unique() {
        let mut copy: Vec<&&str> = IdentityFlags::KEYS.iter().collect();
        copy.sort();
        copy.dedup();
        assert_eq!(copy.len(), IdentityFlags::KEYS.len(), "duplicates present");
        for w in IdentityFlags::KEYS.windows(2) {
            assert!(w[0] < w[1], "not sorted: {} >= {}", w[0], w[1]);
        }
    }

    #[test]
    fn filters_keys_sorted_and_unique() {
        let mut copy: Vec<&&str> = Filters::KEYS.iter().collect();
        copy.sort();
        copy.dedup();
        assert_eq!(copy.len(), Filters::KEYS.len(), "duplicates present");
        for w in Filters::KEYS.windows(2) {
            assert!(w[0] < w[1], "not sorted: {} >= {}", w[0], w[1]);
        }
    }
}
