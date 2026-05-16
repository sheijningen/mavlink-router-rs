use std::collections::BTreeSet;

use super::endpoint_kinds::{
    CommonQuery, MsgIdRange, SerialEndpoint, TcpClientEndpoint, TcpServerEndpoint, U8Range,
    UdpClientEndpoint, UdpServerEndpoint,
};
use super::error::SpecError;

// Per-scheme valid key sets. Kept sorted so the levenshtein "did you mean"
// suggestion is deterministic. Single source of truth for what each scheme
// understands.

pub const COMMON_KEYS: &[&str] = &[
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
    "group",
    "learn_capacity",
    "read_buf_bytes",
    "seq_tracker_capacity",
    "sniffer",
    "tx_queue_frames",
];

const SERIAL_EXTRA: &[&str] = &["serial_reopen_ms"];
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
    /// Apply one key/value pair if it names a common knob. Returns `Ok(true)`
    /// when the key was consumed, `Ok(false)` when it isn't a common-knob
    /// name (caller falls through to scheme-specific handling), or `Err` on
    /// a malformed value.
    pub fn apply(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        match key {
            "sniffer" => {
                self.sniffer = parse_bool(value, "sniffer")?;
                Ok(true)
            }
            "group" => {
                self.group = Some(value.to_string());
                Ok(true)
            }
            "learn_capacity" => {
                self.learn_capacity = Some(parse_usize(value, "learn_capacity")?);
                Ok(true)
            }
            "seq_tracker_capacity" => {
                self.seq_tracker_capacity = Some(parse_usize(value, "seq_tracker_capacity")?);
                Ok(true)
            }
            "read_buf_bytes" => {
                self.read_buf_bytes = Some(parse_usize(value, "read_buf_bytes")?);
                Ok(true)
            }
            "tx_queue_frames" => {
                self.tx_queue_frames = Some(parse_usize(value, "tx_queue_frames")?);
                Ok(true)
            }
            "allow_msgid_in" => {
                self.allow_msgid_in = Some(parse_msgid_ranges(value, "allow_msgid_in")?);
                Ok(true)
            }
            "block_msgid_in" => {
                self.block_msgid_in = Some(parse_msgid_ranges(value, "block_msgid_in")?);
                Ok(true)
            }
            "allow_msgid_out" => {
                self.allow_msgid_out = Some(parse_msgid_ranges(value, "allow_msgid_out")?);
                Ok(true)
            }
            "block_msgid_out" => {
                self.block_msgid_out = Some(parse_msgid_ranges(value, "block_msgid_out")?);
                Ok(true)
            }
            "allow_src_sys_in" => {
                self.allow_src_sys_in = Some(parse_u8_ranges(value, "allow_src_sys_in")?);
                Ok(true)
            }
            "block_src_sys_in" => {
                self.block_src_sys_in = Some(parse_u8_ranges(value, "block_src_sys_in")?);
                Ok(true)
            }
            "allow_src_sys_out" => {
                self.allow_src_sys_out = Some(parse_u8_ranges(value, "allow_src_sys_out")?);
                Ok(true)
            }
            "block_src_sys_out" => {
                self.block_src_sys_out = Some(parse_u8_ranges(value, "block_src_sys_out")?);
                Ok(true)
            }
            "allow_src_comp_in" => {
                self.allow_src_comp_in = Some(parse_u8_ranges(value, "allow_src_comp_in")?);
                Ok(true)
            }
            "block_src_comp_in" => {
                self.block_src_comp_in = Some(parse_u8_ranges(value, "block_src_comp_in")?);
                Ok(true)
            }
            "allow_src_comp_out" => {
                self.allow_src_comp_out = Some(parse_u8_ranges(value, "allow_src_comp_out")?);
                Ok(true)
            }
            "block_src_comp_out" => {
                self.block_src_comp_out = Some(parse_u8_ranges(value, "block_src_comp_out")?);
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// Per-scheme adapter that knows how to set scheme-specific knobs and then
/// falls through to [`CommonQuery::apply`] for the shared ones.
pub trait QueryApplier {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError>;
}

pub struct SerialApplier<'a>(pub &'a mut SerialEndpoint);
impl QueryApplier for SerialApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        if key == "serial_reopen_ms" {
            self.0.serial_reopen_ms = Some(parse_u64(value, "serial_reopen_ms")?);
            return Ok(true);
        }
        self.0.common.apply(key, value)
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
            _ => self.0.common.apply(key, value),
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
        self.0.common.apply(key, value)
    }
}

pub struct TcpServerApplier<'a>(pub &'a mut TcpServerEndpoint);
impl QueryApplier for TcpServerApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        self.0.common.apply(key, value)
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
            _ => self.0.common.apply(key, value),
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
