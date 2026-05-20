use std::collections::BTreeSet;

use super::super::defaults::{MAX_TX_QUEUE_FRAMES, MIN_TX_QUEUE_FRAMES};
use super::super::filters::Filters;
use super::super::identity_flags::IdentityFlags;
use super::super::udp::client::{MAX_LATCH_IDLE_SECS, MIN_LATCH_IDLE_SECS};
use super::super::udp::server::{MAX_IDLE_SECS, MIN_IDLE_SECS};
use super::Scheme;
use super::bounds::{check_u64_range, check_usize_range};
use super::endpoint_kinds::{
    CommonQuery, SerialEndpoint, SerialFlowControl, TcpClientEndpoint, TcpServerEndpoint,
    UdpClientEndpoint, UdpServerEndpoint,
};
use super::error::SpecError;

/// Plumbing keys handled by [`CommonQuery::apply`]. Identity-side keys live
/// on [`IdentityFlags::KEYS`] and [`Filters::KEYS`]; the "did you mean"
/// suggestion walks all three.
pub const COMMON_KEYS: &[&str] = &["tx_queue_frames"];

const SERIAL_EXTRA: &[&str] = &["flow_control"];
const UDPS_EXTRA: &[&str] = &["idle_secs"];
const UDPC_EXTRA: &[&str] = &["latch_idle_secs"];
const TCPS_EXTRA: &[&str] = &[];
const TCPC_EXTRA: &[&str] = &[];

fn known_keys_for(scheme: Scheme) -> &'static [&'static str] {
    match scheme {
        Scheme::Serial => SERIAL_EXTRA,
        Scheme::UdpServer => UDPS_EXTRA,
        Scheme::UdpClient => UDPC_EXTRA,
        Scheme::TcpServer => TCPS_EXTRA,
        Scheme::TcpClient => TCPC_EXTRA,
    }
}

/// Tokenise an `&key=value` query string into ordered pairs, rejecting empty
/// keys and duplicates. Pairs preserve insertion order so the applier can
/// report the first offending key in error messages.
pub fn parse_query_pairs(text: &str) -> Result<Vec<(String, String)>, SpecError> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    if text.is_empty() {
        return Ok(out);
    }
    for pair in text.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').ok_or_else(|| {
            SpecError::MalformedQuery(format!("expected '<key>=<value>' (got '{pair}')"))
        })?;
        if key.is_empty() {
            return Err(SpecError::MalformedQuery(format!("empty key in '{pair}'")));
        }
        if !seen.insert(key.to_string()) {
            return Err(SpecError::DuplicateQueryKey(key.to_string()));
        }
        out.push((key.to_string(), value.to_string()));
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
            "tx_queue_frames" => {
                let count = parse_usize(value, "tx_queue_frames")?;
                self.tx_queue_frames = Some(check_usize_range(
                    count,
                    "tx_queue_frames",
                    MIN_TX_QUEUE_FRAMES,
                    MAX_TX_QUEUE_FRAMES,
                )?);
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
            "flow_control" => {
                self.0.flow_control = parse_flow_control(value)?;
                Ok(true)
            }
            _ => apply_shared(&mut self.0.identity, &mut self.0.common, key, value),
        }
    }
}

fn parse_flow_control(value: &str) -> Result<SerialFlowControl, SpecError> {
    match value {
        "none" => Ok(SerialFlowControl::None),
        "rtscts" => Ok(SerialFlowControl::RtsCts),
        _ => Err(SpecError::InvalidQueryValue {
            key: "flow_control",
            reason: format!("expected 'none' or 'rtscts', got '{value}'"),
        }),
    }
}

pub struct UdpServerApplier<'a>(pub &'a mut UdpServerEndpoint);
impl QueryApplier for UdpServerApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        match key {
            "idle_secs" => {
                let secs = parse_u64(value, "idle_secs")?;
                self.0.idle_secs = Some(check_u64_range(
                    secs,
                    "idle_secs",
                    MIN_IDLE_SECS,
                    MAX_IDLE_SECS,
                )?);
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
            let secs = parse_u64(value, "latch_idle_secs")?;
            self.0.latch_idle_secs = Some(check_u64_range(
                secs,
                "latch_idle_secs",
                MIN_LATCH_IDLE_SECS,
                MAX_LATCH_IDLE_SECS,
            )?);
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
        apply_shared(&mut self.0.identity, &mut self.0.common, key, value)
    }
}

/// Walk `pairs` and invoke the applier for each. Unknown keys produce an
/// [`SpecError::UnknownQueryKey`] with a scheme-aware did-you-mean suggestion.
pub fn apply_pairs(
    applier: &mut dyn QueryApplier,
    scheme: Scheme,
    pairs: &[(String, String)],
) -> Result<(), SpecError> {
    for (key, value) in pairs {
        let handled = applier.set(key, value)?;
        if !handled {
            return Err(SpecError::UnknownQueryKey {
                scheme,
                key: key.clone(),
                suggestion: suggest_query_key(scheme, key),
            });
        }
    }
    Ok(())
}

pub(crate) fn suggest_query_key(scheme: Scheme, unknown: &str) -> Option<&'static str> {
    let extras = known_keys_for(scheme);
    COMMON_KEYS
        .iter()
        .chain(IdentityFlags::KEYS.iter())
        .chain(Filters::KEYS.iter())
        .chain(extras.iter())
        .map(|key| (*key, levenshtein(unknown, key)))
        .filter(|(_, distance)| *distance <= 3)
        .min_by_key(|(_, distance)| *distance)
        .map(|(key, _)| key)
}

pub fn levenshtein(left: &str, right: &str) -> usize {
    let left_len = left.chars().count();
    let right_len = right.chars().count();
    if left_len == 0 {
        return right_len;
    }
    if right_len == 0 {
        return left_len;
    }
    let right_chars: Vec<char> = right.chars().collect();
    let mut prev: Vec<usize> = (0..=right_len).collect();
    let mut curr: Vec<usize> = vec![0; right_len + 1];
    for (row, left_char) in left.chars().enumerate() {
        curr[0] = row + 1;
        for (col, &right_char) in right_chars.iter().enumerate() {
            let cost = usize::from(left_char != right_char);
            let del = prev[col + 1] + 1;
            let ins = curr[col] + 1;
            let sub = prev[col] + cost;
            curr[col + 1] = del.min(ins).min(sub);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[right_len]
}

pub(crate) fn parse_u64(value: &str, key: &'static str) -> Result<u64, SpecError> {
    value.parse().map_err(|_| SpecError::InvalidQueryValue {
        key,
        reason: format!("expected a non-negative integer, got '{value}'"),
    })
}

pub(crate) fn parse_usize(value: &str, key: &'static str) -> Result<usize, SpecError> {
    value.parse().map_err(|_| SpecError::InvalidQueryValue {
        key,
        reason: format!("expected a non-negative integer, got '{value}'"),
    })
}

#[cfg(test)]
mod tests {
    use super::{COMMON_KEYS, levenshtein};
    use crate::endpoint::filters::{Filters, MsgIdRange, U8Range};
    use crate::endpoint::identity_flags::IdentityFlags;
    use crate::endpoint::spec::{
        EndpointKind, EndpointSpec, Scheme, SerialEndpoint, SpecError, TcpClientEndpoint,
        UdpServerEndpoint,
    };

    fn parse_ok(input: &str) -> EndpointSpec {
        EndpointSpec::parse(input)
            .unwrap_or_else(|err| panic!("expected ok for {input:?}, got {err}"))
    }

    fn parse_err(input: &str) -> SpecError {
        EndpointSpec::parse(input).expect_err(&format!("expected err for {input:?}"))
    }

    fn as_serial(spec: &EndpointSpec) -> &SerialEndpoint {
        match &spec.kind {
            EndpointKind::Serial(endpoint) => endpoint,
            other => panic!("expected serial, got {other:?}"),
        }
    }

    fn as_udps(spec: &EndpointSpec) -> &UdpServerEndpoint {
        match &spec.kind {
            EndpointKind::UdpServer(endpoint) => endpoint,
            other => panic!("expected udps, got {other:?}"),
        }
    }

    fn as_tcpc(spec: &EndpointSpec) -> &TcpClientEndpoint {
        match &spec.kind {
            EndpointKind::TcpClient(endpoint) => endpoint,
            other => panic!("expected tcpc, got {other:?}"),
        }
    }

    // -- sniffer bool parsing --

    #[test]
    fn udps_with_sniffer_query() {
        let spec = parse_ok("udps:0.0.0.0:14551#tap?sniffer=true");
        let endpoint = as_udps(&spec);
        assert_eq!(spec.name, "tap");
        assert!(endpoint.identity.sniffer);
    }

    #[test]
    fn sniffer_false_explicit() {
        let endpoint = as_udps(&parse_ok("udps:0.0.0.0:1?sniffer=false")).clone();
        assert!(!endpoint.identity.sniffer);
    }

    #[test]
    fn sniffer_default_is_false() {
        let endpoint = as_udps(&parse_ok("udps:0.0.0.0:1")).clone();
        assert!(!endpoint.identity.sniffer);
    }

    #[test]
    fn sniffer_invalid_value_rejected() {
        match parse_err("udps:0.0.0.0:1?sniffer=yes") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "sniffer"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    // -- Parse-time bounds checks. Every numeric knob the parser accepts
    //    must reject 0 and obviously-bogus large values with a uniform
    //    `"must be in MIN..=MAX, got N"` reason so operator-visible error
    //    text is consistent. The MIN/MAX literals are hardcoded here on
    //    purpose so the wire-contract (the exact bytes an operator sees
    //    in the error) is pinned — a silent retune of the constants
    //    would otherwise pass tests. --

    /// Assert that parsing `input` fails with `InvalidQueryValue { key }`
    /// and that the error reason references both `min`/`max` (so an
    /// operator sees the valid range) and the offending `got_value`.
    fn assert_bounds_err(input: &str, key: &str, min: u64, max: u64, got_value: u64) {
        match parse_err(input) {
            SpecError::InvalidQueryValue {
                key: actual_key,
                reason,
            } => {
                assert_eq!(actual_key, key, "wrong key in error for {input}");
                assert!(
                    reason.contains(&format!("{min}..={max}")),
                    "expected '{min}..={max}' in reason; got: {reason}"
                );
                assert!(
                    reason.contains(&format!("got {got_value}")),
                    "expected 'got {got_value}' in reason; got: {reason}"
                );
            }
            other => panic!("expected InvalidQueryValue for {input}, got {other:?}"),
        }
    }

    // -- idle_secs (1..=86400) --

    #[test]
    fn idle_secs_zero_rejected() {
        assert_bounds_err("udps:0.0.0.0:1?idle_secs=0", "idle_secs", 1, 86_400, 0);
    }

    #[test]
    fn idle_secs_above_max_rejected() {
        assert_bounds_err(
            "udps:0.0.0.0:1?idle_secs=86401",
            "idle_secs",
            1,
            86_400,
            86_401,
        );
    }

    // -- latch_idle_secs (1..=86400) --

    #[test]
    fn latch_idle_secs_zero_rejected() {
        assert_bounds_err(
            "udpc:1.2.3.4:14550?latch_idle_secs=0",
            "latch_idle_secs",
            1,
            86_400,
            0,
        );
    }

    #[test]
    fn latch_idle_secs_above_max_rejected() {
        assert_bounds_err(
            "udpc:1.2.3.4:14550?latch_idle_secs=86401",
            "latch_idle_secs",
            1,
            86_400,
            86_401,
        );
    }

    // -- tx_queue_frames (1..=65536) — CommonQuery on every scheme --

    #[test]
    fn tx_queue_frames_zero_rejected() {
        assert_bounds_err(
            "tcpc:h:1?tx_queue_frames=0",
            "tx_queue_frames",
            1,
            65_536,
            0,
        );
    }

    #[test]
    fn tx_queue_frames_above_max_rejected() {
        assert_bounds_err(
            "tcpc:h:1?tx_queue_frames=65537",
            "tx_queue_frames",
            1,
            65_536,
            65_537,
        );
    }

    // -- msgid filter list parsing --

    #[test]
    fn msgid_filter_list_typed() {
        let spec = parse_ok("tcpc:gcs.local:5760?block_msgid_in=33,100-150,32");
        let endpoint = as_tcpc(&spec);
        assert_eq!(
            endpoint.identity.filters.block_msgid_in,
            vec![
                MsgIdRange::single(33),
                MsgIdRange { lo: 100, hi: 150 },
                MsgIdRange::single(32),
            ]
        );
    }

    #[test]
    fn msgid_filter_list_with_whitespace() {
        let spec = parse_ok("tcpc:gcs.local:5760?allow_msgid_out=1, 2 , 3-5");
        let endpoint = as_tcpc(&spec);
        assert_eq!(endpoint.identity.filters.allow_msgid_out.len(), 3);
        assert_eq!(
            endpoint.identity.filters.allow_msgid_out[2],
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
        let spec = parse_ok("tcpc:gcs.local:5760?allow_src_sys_out=1,5-10,200");
        let endpoint = as_tcpc(&spec);
        assert_eq!(
            endpoint.identity.filters.allow_src_sys_out,
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
    fn serial_flow_control_default_none() {
        let endpoint = as_serial(&parse_ok("serial:/dev/foo:9600")).clone();
        assert_eq!(
            endpoint.flow_control,
            crate::endpoint::spec::SerialFlowControl::None
        );
    }

    #[test]
    fn serial_flow_control_rtscts() {
        let endpoint = as_serial(&parse_ok("serial:/dev/foo:9600?flow_control=rtscts")).clone();
        assert_eq!(
            endpoint.flow_control,
            crate::endpoint::spec::SerialFlowControl::RtsCts
        );
    }

    #[test]
    fn serial_flow_control_explicit_none() {
        let endpoint = as_serial(&parse_ok("serial:/dev/foo:9600?flow_control=none")).clone();
        assert_eq!(
            endpoint.flow_control,
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
                assert_eq!(scheme, Scheme::UdpServer);
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn idle_secs_typed_on_udps() {
        let endpoint = as_udps(&parse_ok("udps:0.0.0.0:14550?idle_secs=30")).clone();
        assert_eq!(endpoint.idle_secs, Some(30));
    }

    #[test]
    fn tcpc_with_group() {
        let spec = parse_ok("tcpc:companion.local:5760#vehicle?group=uplink");
        let endpoint = as_tcpc(&spec);
        assert_eq!(endpoint.identity.group.as_deref(), Some("uplink"));
        assert_eq!(spec.name, "vehicle");
    }

    // -- scheme-specific knobs are rejected on the wrong scheme --

    #[test]
    fn udpc_specific_key_on_udps_is_unknown() {
        match parse_err("udps:0.0.0.0:14550?latch_idle_secs=15") {
            SpecError::UnknownQueryKey { key, scheme, .. } => {
                assert_eq!(key, "latch_idle_secs");
                assert_eq!(scheme, Scheme::UdpServer);
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn udps_specific_key_on_udpc_is_unknown() {
        match parse_err("udpc:1.2.3.4:14550?idle_secs=15") {
            SpecError::UnknownQueryKey { key, scheme, .. } => {
                assert_eq!(key, "idle_secs");
                assert_eq!(scheme, Scheme::UdpClient);
            }
            other => panic!("wrong error: {other:?}"),
        }
    }

    // (`serial_flow_control_rejected_on_non_serial_scheme` above already
    // covers a serial-only key rejected on `udps:`; no separate case here.)

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
    fn duplicate_filter_key_fails_no_accumulation() {
        // Repeated filter keys hit the same key-agnostic dedup; no accumulation.
        assert!(matches!(
            parse_err("udps:0.0.0.0:1?block_msgid_in=33&block_msgid_in=34"),
            SpecError::DuplicateQueryKey(k) if k == "block_msgid_in"
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
        let spec = parse_ok("udps:0.0.0.0:1?");
        let endpoint = as_udps(&spec);
        assert!(endpoint.identity.group.is_none());
        assert!(!endpoint.identity.sniffer);
    }

    #[test]
    fn trailing_ampersand_tolerated() {
        let endpoint = as_udps(&parse_ok("udps:0.0.0.0:1?sniffer=true&")).clone();
        assert!(endpoint.identity.sniffer);
    }

    #[test]
    fn empty_value_for_group_rejected() {
        let err = EndpointSpec::parse("udps:0.0.0.0:1?group=").unwrap_err();
        assert!(
            matches!(err, SpecError::InvalidQueryValue { key: "group", .. }),
            "expected InvalidQueryValue on group, got {err:?}"
        );
    }

    #[test]
    fn group_value_with_embedded_equals_rejected() {
        let err = EndpointSpec::parse("udps:0.0.0.0:1?group=a=b").unwrap_err();
        assert!(
            matches!(err, SpecError::InvalidQueryValue { key: "group", .. }),
            "expected InvalidQueryValue on group, got {err:?}"
        );
    }

    #[test]
    fn group_value_accepts_allowed_characters() {
        let endpoint = as_udps(&parse_ok("udps:0.0.0.0:1?group=Up-link_1")).clone();
        assert_eq!(endpoint.identity.group.as_deref(), Some("Up-link_1"));
    }

    // -- comprehensive coverage --

    #[test]
    fn all_filter_keys_accepted_on_tcpc() {
        let query = "allow_msgid_in=1&block_msgid_in=2&allow_msgid_out=3&block_msgid_out=4\
                 &allow_src_sys_in=5&block_src_sys_in=6&allow_src_sys_out=7&block_src_sys_out=8\
                 &allow_src_comp_in=9&block_src_comp_in=10&allow_src_comp_out=11&block_src_comp_out=12";
        let endpoint = as_tcpc(&parse_ok(&format!("tcpc:x:1?{query}"))).clone();
        assert_eq!(
            endpoint.identity.filters.allow_msgid_in,
            vec![MsgIdRange::single(1)]
        );
        assert_eq!(
            endpoint.identity.filters.block_msgid_in,
            vec![MsgIdRange::single(2)]
        );
        assert_eq!(
            endpoint.identity.filters.allow_msgid_out,
            vec![MsgIdRange::single(3)]
        );
        assert_eq!(
            endpoint.identity.filters.block_msgid_out,
            vec![MsgIdRange::single(4)]
        );
        assert_eq!(
            endpoint.identity.filters.allow_src_sys_in,
            vec![U8Range::single(5)]
        );
        assert_eq!(
            endpoint.identity.filters.block_src_sys_in,
            vec![U8Range::single(6)]
        );
        assert_eq!(
            endpoint.identity.filters.allow_src_sys_out,
            vec![U8Range::single(7)]
        );
        assert_eq!(
            endpoint.identity.filters.block_src_sys_out,
            vec![U8Range::single(8)]
        );
        assert_eq!(
            endpoint.identity.filters.allow_src_comp_in,
            vec![U8Range::single(9)]
        );
        assert_eq!(
            endpoint.identity.filters.block_src_comp_in,
            vec![U8Range::single(10)]
        );
        assert_eq!(
            endpoint.identity.filters.allow_src_comp_out,
            vec![U8Range::single(11)]
        );
        assert_eq!(
            endpoint.identity.filters.block_src_comp_out,
            vec![U8Range::single(12)]
        );
    }

    #[test]
    fn no_query_leaves_identity_at_default() {
        let endpoint = as_udps(&parse_ok("udps:0.0.0.0:1")).clone();
        assert_eq!(endpoint.identity, IdentityFlags::default());
    }

    #[test]
    fn plumbing_keys_typed() {
        let endpoint = as_tcpc(&parse_ok("tcpc:x:1?tx_queue_frames=128")).clone();
        assert_eq!(endpoint.common.tx_queue_frames, Some(128));
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
        for window in COMMON_KEYS.windows(2) {
            assert!(
                window[0] < window[1],
                "not sorted: {} >= {}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn identity_keys_sorted_and_unique() {
        let mut copy: Vec<&&str> = IdentityFlags::KEYS.iter().collect();
        copy.sort();
        copy.dedup();
        assert_eq!(copy.len(), IdentityFlags::KEYS.len(), "duplicates present");
        for window in IdentityFlags::KEYS.windows(2) {
            assert!(
                window[0] < window[1],
                "not sorted: {} >= {}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn filters_keys_sorted_and_unique() {
        let mut copy: Vec<&&str> = Filters::KEYS.iter().collect();
        copy.sort();
        copy.dedup();
        assert_eq!(copy.len(), Filters::KEYS.len(), "duplicates present");
        for window in Filters::KEYS.windows(2) {
            assert!(
                window[0] < window[1],
                "not sorted: {} >= {}",
                window[0],
                window[1]
            );
        }
    }
}
