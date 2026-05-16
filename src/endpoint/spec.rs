use std::collections::BTreeSet;

use thiserror::Error;

/// One endpoint declaration after parsing a CLI string or TOML entry. The
/// scheme-specific configuration (address, query knobs, filter lists) lives
/// inside the [`EndpointKind`] variant rather than a flat string-keyed map so
/// the type system enforces "only knobs that make sense for this scheme are
/// reachable here."
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointSpec {
    pub kind: EndpointKind,
    /// Either the explicit `#name` fragment or an auto-name derived from the
    /// scheme + address (sanitised to satisfy the explicit-name regex).
    pub name: String,
    /// True when `name` came from `#name` rather than being auto-derived.
    pub explicit_name: bool,
}

/// Discriminator for the supported endpoint schemes. Each variant carries the
/// fully-typed config struct for that scheme — address, scheme-specific
/// knobs, and the flattened common knobs (filters, sniffer, group, routing
/// table sizes, read/tx buffer sizes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointKind {
    Serial(SerialEndpoint),
    UdpServer(UdpServerEndpoint),
    UdpClient(UdpClientEndpoint),
    TcpServer(TcpServerEndpoint),
    TcpClient(TcpClientEndpoint),
}

impl EndpointKind {
    pub fn scheme(&self) -> &'static str {
        match self {
            EndpointKind::Serial(_) => "serial",
            EndpointKind::UdpServer(_) => "udps",
            EndpointKind::UdpClient(_) => "udpc",
            EndpointKind::TcpServer(_) => "tcps",
            EndpointKind::TcpClient(_) => "tcpc",
        }
    }
}

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

/// Single source of truth for the flattened common-knob fieldset. Each
/// scheme-specific endpoint struct invokes this macro with its own
/// address + scheme-specific fields prepended; the macro tacks on the same
/// 18 common fields so they round-trip with one definition.
macro_rules! endpoint_struct {
    (
        $(#[$attr:meta])*
        $name:ident { $($field:ident : $ty:ty,)* }
    ) => {
        $(#[$attr])*
        #[derive(Debug, Clone, PartialEq, Eq, Default)]
        pub struct $name {
            $(pub $field: $ty,)*
            pub sniffer: bool,
            pub group: Option<String>,
            pub learn_capacity: Option<usize>,
            pub seq_tracker_capacity: Option<usize>,
            pub read_buf_bytes: Option<usize>,
            pub tx_queue_frames: Option<usize>,
            pub allow_msgid_in: Option<Vec<MsgIdRange>>,
            pub block_msgid_in: Option<Vec<MsgIdRange>>,
            pub allow_msgid_out: Option<Vec<MsgIdRange>>,
            pub block_msgid_out: Option<Vec<MsgIdRange>>,
            pub allow_src_sys_in: Option<Vec<U8Range>>,
            pub block_src_sys_in: Option<Vec<U8Range>>,
            pub allow_src_sys_out: Option<Vec<U8Range>>,
            pub block_src_sys_out: Option<Vec<U8Range>>,
            pub allow_src_comp_in: Option<Vec<U8Range>>,
            pub block_src_comp_in: Option<Vec<U8Range>>,
            pub allow_src_comp_out: Option<Vec<U8Range>>,
            pub block_src_comp_out: Option<Vec<U8Range>>,
        }
    };
}

endpoint_struct! {
    /// Fully-typed `serial:` endpoint config.
    SerialEndpoint {
        path: String,
        baud: u32,
        serial_reopen_ms: Option<u64>,
    }
}

endpoint_struct! {
    /// Fully-typed `udps:` endpoint config.
    UdpServerEndpoint {
        host: String,
        port: u16,
        idle_secs: Option<u64>,
        udps_peer_capacity: Option<usize>,
    }
}

endpoint_struct! {
    /// Fully-typed `udpc:` endpoint config.
    UdpClientEndpoint {
        host: String,
        port: u16,
        latch_idle_secs: Option<u64>,
    }
}

endpoint_struct! {
    /// Fully-typed `tcps:` endpoint config.
    TcpServerEndpoint {
        host: String,
        port: u16,
    }
}

endpoint_struct! {
    /// Fully-typed `tcpc:` endpoint config.
    TcpClientEndpoint {
        host: String,
        port: u16,
        reconnect_initial_ms: Option<u64>,
        reconnect_max_ms: Option<u64>,
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SpecError {
    #[error("missing scheme in '{0}'")]
    MissingScheme(String),
    #[error("unknown scheme '{0}' (valid: serial, udps, udpc, tcps, tcpc)")]
    UnknownScheme(String),
    #[error("invalid endpoint name '{0}': must match [A-Za-z0-9_-]{{1,64}}")]
    InvalidName(String),
    #[error(
        "unknown query key '{key}' for scheme '{scheme}'{}",
        fmt_suggestion(suggestion)
    )]
    UnknownQueryKey {
        scheme: &'static str,
        key: String,
        suggestion: Option<&'static str>,
    },
    #[error("malformed query string: {0}")]
    MalformedQuery(String),
    #[error("duplicate query key '{0}'")]
    DuplicateQueryKey(String),
    #[error("malformed body for scheme '{scheme}': '{body}' ({reason})")]
    MalformedBody {
        scheme: &'static str,
        body: String,
        reason: String,
    },
    #[error("invalid value for '{key}': {reason}")]
    InvalidQueryValue { key: &'static str, reason: String },
}

fn fmt_suggestion(s: &Option<&'static str>) -> String {
    s.map(|s| format!(" (did you mean '{s}'?)"))
        .unwrap_or_default()
}

// Per-scheme valid key sets. Kept sorted so the levenshtein "did you mean"
// suggestion is deterministic. Single source of truth for what each scheme
// understands.

const COMMON_KEYS: &[&str] = &[
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

impl EndpointSpec {
    pub fn parse(input: &str) -> Result<Self, SpecError> {
        let (scheme, rest) = input
            .split_once(':')
            .ok_or_else(|| SpecError::MissingScheme(input.to_string()))?;
        if scheme.is_empty() {
            return Err(SpecError::MissingScheme(input.to_string()));
        }

        let (body, explicit_name, query_str) = split_body_name_query(rest);

        if body.contains('?') {
            return Err(SpecError::MalformedQuery(format!(
                "'?' must follow '#name' (got '{body}')"
            )));
        }

        let pairs = parse_query_pairs(query_str.unwrap_or(""))?;
        let kind = parse_kind(scheme, body, &pairs)?;
        let name = match explicit_name {
            Some(n) => {
                validate_name(n)?;
                n.to_string()
            }
            None => default_name(&kind),
        };
        let explicit = explicit_name.is_some();
        Ok(Self {
            kind,
            name,
            explicit_name: explicit,
        })
    }
}

fn split_body_name_query(rest: &str) -> (&str, Option<&str>, Option<&str>) {
    if let Some((body, after_hash)) = rest.split_once('#') {
        if let Some((name, query)) = after_hash.split_once('?') {
            (body, Some(name), Some(query))
        } else {
            (body, Some(after_hash), None)
        }
    } else if let Some((body, query)) = rest.split_once('?') {
        (body, None, Some(query))
    } else {
        (rest, None, None)
    }
}

fn parse_kind(
    scheme: &str,
    body: &str,
    pairs: &[(String, String)],
) -> Result<EndpointKind, SpecError> {
    match scheme {
        "serial" => parse_serial_body(body).and_then(|(path, baud)| {
            let mut ep = SerialEndpoint {
                path,
                baud,
                ..SerialEndpoint::default()
            };
            apply_pairs(&mut SerialApplier(&mut ep), "serial", pairs)?;
            Ok(EndpointKind::Serial(ep))
        }),
        "udps" => parse_host_port(body, "udps").and_then(|(host, port)| {
            let mut ep = UdpServerEndpoint {
                host,
                port,
                ..UdpServerEndpoint::default()
            };
            apply_pairs(&mut UdpServerApplier(&mut ep), "udps", pairs)?;
            Ok(EndpointKind::UdpServer(ep))
        }),
        "udpc" => parse_host_port(body, "udpc").and_then(|(host, port)| {
            let mut ep = UdpClientEndpoint {
                host,
                port,
                ..UdpClientEndpoint::default()
            };
            apply_pairs(&mut UdpClientApplier(&mut ep), "udpc", pairs)?;
            Ok(EndpointKind::UdpClient(ep))
        }),
        "tcps" => parse_host_port(body, "tcps").and_then(|(host, port)| {
            let mut ep = TcpServerEndpoint {
                host,
                port,
                ..TcpServerEndpoint::default()
            };
            apply_pairs(&mut TcpServerApplier(&mut ep), "tcps", pairs)?;
            Ok(EndpointKind::TcpServer(ep))
        }),
        "tcpc" => parse_host_port(body, "tcpc").and_then(|(host, port)| {
            let mut ep = TcpClientEndpoint {
                host,
                port,
                ..TcpClientEndpoint::default()
            };
            apply_pairs(&mut TcpClientApplier(&mut ep), "tcpc", pairs)?;
            Ok(EndpointKind::TcpClient(ep))
        }),
        other => Err(SpecError::UnknownScheme(other.to_string())),
    }
}

/// Trait abstracting "set knob X to value V" so the per-scheme key dispatch
/// is shared between schemes. Each scheme's applier owns a mutable borrow of
/// its concrete endpoint struct and matches the keys it understands; common
/// keys are routed through the same set_* method names by macro.
trait QueryApplier {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError>;
}

macro_rules! apply_common {
    ($ep:expr, $key:expr, $value:expr) => {
        match $key {
            "sniffer" => {
                $ep.sniffer = parse_bool($value, "sniffer")?;
                Ok(true)
            }
            "group" => {
                $ep.group = Some($value.to_string());
                Ok(true)
            }
            "learn_capacity" => {
                $ep.learn_capacity = Some(parse_usize($value, "learn_capacity")?);
                Ok(true)
            }
            "seq_tracker_capacity" => {
                $ep.seq_tracker_capacity = Some(parse_usize($value, "seq_tracker_capacity")?);
                Ok(true)
            }
            "read_buf_bytes" => {
                $ep.read_buf_bytes = Some(parse_usize($value, "read_buf_bytes")?);
                Ok(true)
            }
            "tx_queue_frames" => {
                $ep.tx_queue_frames = Some(parse_usize($value, "tx_queue_frames")?);
                Ok(true)
            }
            "allow_msgid_in" => {
                $ep.allow_msgid_in = Some(parse_msgid_ranges($value, "allow_msgid_in")?);
                Ok(true)
            }
            "block_msgid_in" => {
                $ep.block_msgid_in = Some(parse_msgid_ranges($value, "block_msgid_in")?);
                Ok(true)
            }
            "allow_msgid_out" => {
                $ep.allow_msgid_out = Some(parse_msgid_ranges($value, "allow_msgid_out")?);
                Ok(true)
            }
            "block_msgid_out" => {
                $ep.block_msgid_out = Some(parse_msgid_ranges($value, "block_msgid_out")?);
                Ok(true)
            }
            "allow_src_sys_in" => {
                $ep.allow_src_sys_in = Some(parse_u8_ranges($value, "allow_src_sys_in")?);
                Ok(true)
            }
            "block_src_sys_in" => {
                $ep.block_src_sys_in = Some(parse_u8_ranges($value, "block_src_sys_in")?);
                Ok(true)
            }
            "allow_src_sys_out" => {
                $ep.allow_src_sys_out = Some(parse_u8_ranges($value, "allow_src_sys_out")?);
                Ok(true)
            }
            "block_src_sys_out" => {
                $ep.block_src_sys_out = Some(parse_u8_ranges($value, "block_src_sys_out")?);
                Ok(true)
            }
            "allow_src_comp_in" => {
                $ep.allow_src_comp_in = Some(parse_u8_ranges($value, "allow_src_comp_in")?);
                Ok(true)
            }
            "block_src_comp_in" => {
                $ep.block_src_comp_in = Some(parse_u8_ranges($value, "block_src_comp_in")?);
                Ok(true)
            }
            "allow_src_comp_out" => {
                $ep.allow_src_comp_out = Some(parse_u8_ranges($value, "allow_src_comp_out")?);
                Ok(true)
            }
            "block_src_comp_out" => {
                $ep.block_src_comp_out = Some(parse_u8_ranges($value, "block_src_comp_out")?);
                Ok(true)
            }
            _ => Ok(false),
        }
    };
}

struct SerialApplier<'a>(&'a mut SerialEndpoint);
impl QueryApplier for SerialApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        if key == "serial_reopen_ms" {
            self.0.serial_reopen_ms = Some(parse_u64(value, "serial_reopen_ms")?);
            return Ok(true);
        }
        apply_common!(self.0, key, value)
    }
}

struct UdpServerApplier<'a>(&'a mut UdpServerEndpoint);
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
            _ => apply_common!(self.0, key, value),
        }
    }
}

struct UdpClientApplier<'a>(&'a mut UdpClientEndpoint);
impl QueryApplier for UdpClientApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        if key == "latch_idle_secs" {
            self.0.latch_idle_secs = Some(parse_u64(value, "latch_idle_secs")?);
            return Ok(true);
        }
        apply_common!(self.0, key, value)
    }
}

struct TcpServerApplier<'a>(&'a mut TcpServerEndpoint);
impl QueryApplier for TcpServerApplier<'_> {
    fn set(&mut self, key: &str, value: &str) -> Result<bool, SpecError> {
        apply_common!(self.0, key, value)
    }
}

struct TcpClientApplier<'a>(&'a mut TcpClientEndpoint);
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
            _ => apply_common!(self.0, key, value),
        }
    }
}

fn apply_pairs(
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

fn parse_serial_body(body: &str) -> Result<(String, u32), SpecError> {
    if body.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme: "serial",
            body: body.to_string(),
            reason: "empty body".to_string(),
        });
    }
    let last_colon = body.rfind(':');
    let last_comma = body.rfind(',');
    let pos = match (last_colon, last_comma) {
        (Some(c), Some(m)) => Some(c.max(m)),
        (Some(c), None) => Some(c),
        (None, Some(m)) => Some(m),
        (None, None) => None,
    };
    let Some(pos) = pos else {
        return Err(SpecError::MalformedBody {
            scheme: "serial",
            body: body.to_string(),
            reason: "expected '<path>:<baud>' or '<path>,<baud>'".to_string(),
        });
    };
    let path = &body[..pos];
    let baud_str = &body[pos + 1..];
    if path.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme: "serial",
            body: body.to_string(),
            reason: "empty device path".to_string(),
        });
    }
    let baud: u32 = baud_str.parse().map_err(|_| SpecError::MalformedBody {
        scheme: "serial",
        body: body.to_string(),
        reason: format!("baud '{baud_str}' is not a valid u32"),
    })?;
    if baud == 0 {
        return Err(SpecError::MalformedBody {
            scheme: "serial",
            body: body.to_string(),
            reason: "baud must be > 0".to_string(),
        });
    }
    Ok((path.to_string(), baud))
}

fn parse_host_port(body: &str, scheme: &'static str) -> Result<(String, u16), SpecError> {
    if body.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme,
            body: body.to_string(),
            reason: "empty body".to_string(),
        });
    }
    let (host, port_str) = if let Some(rest) = body.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err(SpecError::MalformedBody {
                scheme,
                body: body.to_string(),
                reason: "unclosed '[' in IPv6 literal".to_string(),
            });
        };
        let host = &rest[..end];
        let after = &rest[end + 1..];
        let Some(port_str) = after.strip_prefix(':') else {
            return Err(SpecError::MalformedBody {
                scheme,
                body: body.to_string(),
                reason: "expected ':<port>' after ']'".to_string(),
            });
        };
        (host.to_string(), port_str.to_string())
    } else {
        let Some((host, port_str)) = body.rsplit_once(':') else {
            return Err(SpecError::MalformedBody {
                scheme,
                body: body.to_string(),
                reason: "expected '<host>:<port>'".to_string(),
            });
        };
        (host.to_string(), port_str.to_string())
    };
    if host.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme,
            body: body.to_string(),
            reason: "empty host".to_string(),
        });
    }
    let port: u16 = port_str.parse().map_err(|_| SpecError::MalformedBody {
        scheme,
        body: body.to_string(),
        reason: format!("port '{port_str}' is not a valid u16"),
    })?;
    Ok((host, port))
}

fn validate_name(s: &str) -> Result<(), SpecError> {
    if s.is_empty() || s.len() > 64 {
        return Err(SpecError::InvalidName(s.to_string()));
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(SpecError::InvalidName(s.to_string()));
    }
    Ok(())
}

fn default_name(kind: &EndpointKind) -> String {
    match kind {
        EndpointKind::Serial(e) => format!("serial-{}-{}", sanitize_for_name(&e.path), e.baud),
        EndpointKind::UdpServer(e) => format!("udps-{}-{}", sanitize_for_name(&e.host), e.port),
        EndpointKind::UdpClient(e) => format!("udpc-{}-{}", sanitize_for_name(&e.host), e.port),
        EndpointKind::TcpServer(e) => format!("tcps-{}-{}", sanitize_for_name(&e.host), e.port),
        EndpointKind::TcpClient(e) => format!("tcpc-{}-{}", sanitize_for_name(&e.host), e.port),
    }
}

fn sanitize_for_name(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn parse_query_pairs(s: &str) -> Result<Vec<(String, String)>, SpecError> {
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

fn levenshtein(a: &str, b: &str) -> usize {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(input: &str) -> EndpointSpec {
        EndpointSpec::parse(input).unwrap_or_else(|e| panic!("expected ok for {input:?}, got {e}"))
    }

    fn parse_err(input: &str) -> SpecError {
        EndpointSpec::parse(input).expect_err(&format!("expected err for {input:?}"))
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

    fn as_tcps(spec: &EndpointSpec) -> &TcpServerEndpoint {
        match &spec.kind {
            EndpointKind::TcpServer(e) => e,
            other => panic!("expected tcps, got {other:?}"),
        }
    }

    fn as_tcpc(spec: &EndpointSpec) -> &TcpClientEndpoint {
        match &spec.kind {
            EndpointKind::TcpClient(e) => e,
            other => panic!("expected tcpc, got {other:?}"),
        }
    }

    fn as_serial(spec: &EndpointSpec) -> &SerialEndpoint {
        match &spec.kind {
            EndpointKind::Serial(e) => e,
            other => panic!("expected serial, got {other:?}"),
        }
    }

    #[test]
    fn serial_colon_form() {
        let s = parse_ok("serial:/dev/ttyUSB0:921600");
        let e = as_serial(&s);
        assert_eq!(e.path, "/dev/ttyUSB0");
        assert_eq!(e.baud, 921600);
        assert_eq!(s.name, "serial-_dev_ttyUSB0-921600");
        assert!(!s.explicit_name);
    }

    #[test]
    fn serial_comma_form() {
        let s = parse_ok("serial:/dev/ttyUSB0,921600");
        let e = as_serial(&s);
        assert_eq!(e.path, "/dev/ttyUSB0");
        assert_eq!(e.baud, 921600);
    }

    #[test]
    fn serial_windows_com_colon() {
        let e = as_serial(&parse_ok("serial:COM3:115200")).clone();
        assert_eq!(e.path, "COM3");
        assert_eq!(e.baud, 115200);
    }

    #[test]
    fn serial_windows_com_comma() {
        let e = as_serial(&parse_ok("serial:COM3,115200")).clone();
        assert_eq!(e.path, "COM3");
        assert_eq!(e.baud, 115200);
    }

    #[test]
    fn serial_windows_unc_path() {
        let e = as_serial(&parse_ok(r"serial:\\.\COM10:115200")).clone();
        assert_eq!(e.path, r"\\.\COM10");
        assert_eq!(e.baud, 115200);
    }

    #[test]
    fn serial_by_id_symlink() {
        let e = as_serial(&parse_ok("serial:/dev/serial/by-id/usb-FTDI-port0:57600")).clone();
        assert_eq!(e.path, "/dev/serial/by-id/usb-FTDI-port0");
        assert_eq!(e.baud, 57600);
    }

    #[test]
    fn serial_with_explicit_name() {
        let s = parse_ok("serial:/dev/ttyUSB0:921600#vehicle");
        assert_eq!(s.name, "vehicle");
        assert!(s.explicit_name);
    }

    #[test]
    fn serial_no_separator_fails() {
        assert!(matches!(
            parse_err("serial:COM3"),
            SpecError::MalformedBody {
                scheme: "serial",
                ..
            }
        ));
    }

    #[test]
    fn serial_non_numeric_baud_fails() {
        assert!(matches!(
            parse_err("serial:/dev/foo:abc"),
            SpecError::MalformedBody {
                scheme: "serial",
                ..
            }
        ));
    }

    #[test]
    fn serial_empty_path_fails() {
        assert!(matches!(
            parse_err("serial::921600"),
            SpecError::MalformedBody {
                scheme: "serial",
                ..
            }
        ));
    }

    #[test]
    fn serial_zero_baud_fails() {
        assert!(matches!(
            parse_err("serial:/dev/foo:0"),
            SpecError::MalformedBody {
                scheme: "serial",
                ..
            }
        ));
    }

    #[test]
    fn serial_reopen_ms_typed() {
        let e = as_serial(&parse_ok("serial:/dev/foo:9600?serial_reopen_ms=750")).clone();
        assert_eq!(e.serial_reopen_ms, Some(750));
    }

    #[test]
    fn udps_ipv4() {
        let s = parse_ok("udps:0.0.0.0:14550");
        let e = as_udps(&s);
        assert_eq!(e.host, "0.0.0.0");
        assert_eq!(e.port, 14550);
        assert_eq!(s.name, "udps-0_0_0_0-14550");
    }

    #[test]
    fn udps_ipv6_dual_stack() {
        let s = parse_ok("udps:[::]:14550");
        let e = as_udps(&s);
        assert_eq!(e.host, "::");
        assert_eq!(e.port, 14550);
        assert_eq!(s.name, "udps-__-14550");
    }

    #[test]
    fn udpc_ipv4() {
        let s = parse_ok("udpc:192.168.1.5:14550");
        let e = as_udpc(&s);
        assert_eq!(e.host, "192.168.1.5");
        assert_eq!(e.port, 14550);
    }

    #[test]
    fn tcps_ipv6_bracketed() {
        let s = parse_ok("tcps:[2001:db8::1]:5760");
        let e = as_tcps(&s);
        assert_eq!(e.host, "2001:db8::1");
        assert_eq!(e.port, 5760);
    }

    #[test]
    fn tcpc_hostname_explicit_name() {
        let s = parse_ok("tcpc:companion.local:5760#vehicle");
        let e = as_tcpc(&s);
        assert_eq!(e.host, "companion.local");
        assert_eq!(e.port, 5760);
        assert_eq!(s.name, "vehicle");
    }

    #[test]
    fn tcpc_with_group() {
        let s = parse_ok("tcpc:companion.local:5760#vehicle?group=uplink");
        let e = as_tcpc(&s);
        assert_eq!(e.group.as_deref(), Some("uplink"));
        assert_eq!(s.name, "vehicle");
    }

    #[test]
    fn udps_with_sniffer_query() {
        let s = parse_ok("udps:0.0.0.0:14551#tap?sniffer=true");
        let e = as_udps(&s);
        assert_eq!(s.name, "tap");
        assert!(e.sniffer);
    }

    #[test]
    fn sniffer_false_explicit() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1?sniffer=false")).clone();
        assert!(!e.sniffer);
    }

    #[test]
    fn sniffer_default_is_false() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1")).clone();
        assert!(!e.sniffer);
    }

    #[test]
    fn sniffer_invalid_value_rejected() {
        match parse_err("udps:0.0.0.0:1?sniffer=yes") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "sniffer"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn msgid_filter_list_typed() {
        let s = parse_ok("tcpc:gcs.local:5760?block_msgid_in=33,100-150,32");
        let e = as_tcpc(&s);
        let list = e.block_msgid_in.as_deref().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0], MsgIdRange::single(33));
        assert_eq!(list[1], MsgIdRange { lo: 100, hi: 150 });
        assert_eq!(list[2], MsgIdRange::single(32));
    }

    #[test]
    fn msgid_filter_list_with_whitespace() {
        let s = parse_ok("tcpc:gcs.local:5760?allow_msgid_out=1, 2 , 3-5");
        let e = as_tcpc(&s);
        let list = e.allow_msgid_out.as_deref().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[2], MsgIdRange { lo: 3, hi: 5 });
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

    #[test]
    fn src_sys_filter_typed_as_u8() {
        let s = parse_ok("tcpc:gcs.local:5760?allow_src_sys_out=1,5-10,200");
        let e = as_tcpc(&s);
        let list = e.allow_src_sys_out.as_deref().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0], U8Range::single(1));
        assert_eq!(list[1], U8Range { lo: 5, hi: 10 });
        assert_eq!(list[2], U8Range::single(200));
    }

    #[test]
    fn src_sys_filter_overflow_rejected() {
        match parse_err("tcpc:gcs.local:5760?allow_src_sys_out=256") {
            SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "allow_src_sys_out"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn idle_secs_typed_on_udps() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:14550?idle_secs=30")).clone();
        assert_eq!(e.idle_secs, Some(30));
    }

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

    #[test]
    fn name_max_64_ok() {
        let n = "a".repeat(64);
        let s = parse_ok(&format!("udps:0.0.0.0:1#{n}"));
        assert_eq!(s.name, n);
    }

    #[test]
    fn name_too_long_fails() {
        let n = "a".repeat(65);
        assert!(matches!(
            parse_err(&format!("udps:0.0.0.0:1#{n}")),
            SpecError::InvalidName(_)
        ));
    }

    #[test]
    fn name_empty_fails() {
        assert!(matches!(
            parse_err("udps:0.0.0.0:1#"),
            SpecError::InvalidName(_)
        ));
    }

    #[test]
    fn name_with_dot_fails() {
        assert!(matches!(
            parse_err("udps:0.0.0.0:1#a.b"),
            SpecError::InvalidName(_)
        ));
    }

    #[test]
    fn name_with_slash_fails() {
        assert!(matches!(
            parse_err("udps:0.0.0.0:1#a/b"),
            SpecError::InvalidName(_)
        ));
    }

    #[test]
    fn name_with_underscore_hyphen_digit_ok() {
        let s = parse_ok("udps:0.0.0.0:1#abc_DEF-123");
        assert_eq!(s.name, "abc_DEF-123");
    }

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

    #[test]
    fn missing_scheme_fails() {
        assert!(matches!(
            parse_err("nothing-here"),
            SpecError::MissingScheme(_)
        ));
    }

    #[test]
    fn empty_scheme_fails() {
        assert!(matches!(parse_err(":body"), SpecError::MissingScheme(_)));
    }

    #[test]
    fn unknown_scheme_fails() {
        assert!(matches!(
            parse_err("http:host:80"),
            SpecError::UnknownScheme(_)
        ));
    }

    #[test]
    fn empty_input_fails() {
        assert!(matches!(parse_err(""), SpecError::MissingScheme(_)));
    }

    #[test]
    fn udps_no_port_fails() {
        assert!(matches!(
            parse_err("udps:nohost"),
            SpecError::MalformedBody { .. }
        ));
    }

    #[test]
    fn udps_bad_port_fails() {
        assert!(matches!(
            parse_err("udps:foo:abc"),
            SpecError::MalformedBody { .. }
        ));
    }

    #[test]
    fn udps_port_overflow_fails() {
        assert!(matches!(
            parse_err("udps:foo:99999"),
            SpecError::MalformedBody { .. }
        ));
    }

    #[test]
    fn udps_empty_host_fails() {
        assert!(matches!(
            parse_err("udps::14550"),
            SpecError::MalformedBody { .. }
        ));
    }

    #[test]
    fn ipv6_unclosed_fails() {
        assert!(matches!(
            parse_err("udps:[::1"),
            SpecError::MalformedBody { .. }
        ));
    }

    #[test]
    fn ipv6_no_port_after_bracket_fails() {
        assert!(matches!(
            parse_err("udps:[::1]"),
            SpecError::MalformedBody { .. }
        ));
    }

    #[test]
    fn ipv6_garbage_after_bracket_fails() {
        assert!(matches!(
            parse_err("udps:[::1]x14550"),
            SpecError::MalformedBody { .. }
        ));
    }

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
        assert!(e.group.is_none());
        assert!(!e.sniffer);
    }

    #[test]
    fn trailing_ampersand_tolerated() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1?sniffer=true&")).clone();
        assert!(e.sniffer);
    }

    #[test]
    fn empty_value_for_group_ok() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1?group=")).clone();
        assert_eq!(e.group.as_deref(), Some(""));
    }

    #[test]
    fn value_with_embedded_equals_kept_intact() {
        let e = as_udps(&parse_ok("udps:0.0.0.0:1?group=a=b")).clone();
        assert_eq!(e.group.as_deref(), Some("a=b"));
    }

    #[test]
    fn fragment_before_query_order_locked() {
        let s = parse_ok("udps:0.0.0.0:1#name?group=g");
        assert_eq!(s.name, "name");
        let e = as_udps(&s);
        assert_eq!(e.group.as_deref(), Some("g"));
    }

    #[test]
    fn query_before_fragment_rejected() {
        match parse_err("udps:0.0.0.0:1?group=val#nope") {
            SpecError::MalformedQuery(_) => {}
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn all_common_filter_keys_accepted_on_tcpc() {
        let q = "allow_msgid_in=1&block_msgid_in=2&allow_msgid_out=3&block_msgid_out=4\
                 &allow_src_sys_in=5&block_src_sys_in=6&allow_src_sys_out=7&block_src_sys_out=8\
                 &allow_src_comp_in=9&block_src_comp_in=10&allow_src_comp_out=11&block_src_comp_out=12";
        let e = as_tcpc(&parse_ok(&format!("tcpc:x:1?{q}"))).clone();
        assert!(e.allow_msgid_in.is_some());
        assert!(e.block_msgid_in.is_some());
        assert!(e.allow_msgid_out.is_some());
        assert!(e.block_msgid_out.is_some());
        assert!(e.allow_src_sys_in.is_some());
        assert!(e.block_src_sys_in.is_some());
        assert!(e.allow_src_sys_out.is_some());
        assert!(e.block_src_sys_out.is_some());
        assert!(e.allow_src_comp_in.is_some());
        assert!(e.block_src_comp_in.is_some());
        assert!(e.allow_src_comp_out.is_some());
        assert!(e.block_src_comp_out.is_some());
    }

    #[test]
    fn endpoint_kind_scheme_roundtrip() {
        for input in &[
            "serial:/dev/ttyUSB0:115200",
            "udps:0.0.0.0:1",
            "udpc:1.2.3.4:1",
            "tcps:0.0.0.0:1",
            "tcpc:host:1",
        ] {
            let s = parse_ok(input);
            assert!(input.starts_with(&format!("{}:", s.kind.scheme())));
        }
    }

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
    fn auto_names_satisfy_explicit_name_regex() {
        for input in &[
            "serial:/dev/ttyUSB0:921600",
            r"serial:\\.\COM10:115200",
            "udps:0.0.0.0:14550",
            "udps:[::]:14550",
            "tcps:[2001:db8::1]:5760",
            "tcpc:gcs.local:5760",
            "udpc:companion.local:14550",
        ] {
            let s = parse_ok(input);
            validate_name(&s.name)
                .unwrap_or_else(|e| panic!("auto-name {:?} fails name regex: {e}", s.name));
        }
    }

    #[test]
    fn sanitize_for_name_replaces_disallowed_chars() {
        assert_eq!(sanitize_for_name("0.0.0.0"), "0_0_0_0");
        assert_eq!(sanitize_for_name("/dev/ttyUSB0"), "_dev_ttyUSB0");
        assert_eq!(sanitize_for_name("::1"), "__1");
        assert_eq!(sanitize_for_name("gcs.local"), "gcs_local");
        assert_eq!(sanitize_for_name(r"\\.\COM10"), "____COM10");
        assert_eq!(sanitize_for_name("abc_DEF-123"), "abc_DEF-123");
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
}
