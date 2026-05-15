use std::collections::BTreeMap;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointSpec {
    pub kind: EndpointKind,
    pub name: String,
    pub explicit_name: bool,
    pub query: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointKind {
    Serial { path: String, baud: u32 },
    UdpServer { host: String, port: u16 },
    UdpClient { host: String, port: u16 },
    TcpServer { host: String, port: u16 },
    TcpClient { host: String, port: u16 },
}

impl EndpointKind {
    pub fn scheme(&self) -> &'static str {
        match self {
            EndpointKind::Serial { .. } => "serial",
            EndpointKind::UdpServer { .. } => "udps",
            EndpointKind::UdpClient { .. } => "udpc",
            EndpointKind::TcpServer { .. } => "tcps",
            EndpointKind::TcpClient { .. } => "tcpc",
        }
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
    #[error("unknown query key '{key}'{}", fmt_suggestion(suggestion))]
    UnknownQueryKey {
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
}

fn fmt_suggestion(s: &Option<&'static str>) -> String {
    s.map(|s| format!(" (did you mean '{s}'?)"))
        .unwrap_or_default()
}

const KNOWN_QUERY_KEYS: &[&str] = &[
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
    "idle_secs",
    "latch_idle_secs",
    "learn_capacity",
    "read_buf_bytes",
    "reconnect_initial_ms",
    "reconnect_max_ms",
    "seq_tracker_capacity",
    "serial_reopen_ms",
    "sniffer",
    "tx_queue_frames",
    "udps_peer_capacity",
];

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

        let kind = parse_body(scheme, body)?;
        let name = match explicit_name {
            Some(n) => {
                validate_name(n)?;
                n.to_string()
            }
            None => default_name(&kind),
        };
        let explicit = explicit_name.is_some();
        let query = parse_query(query_str.unwrap_or(""))?;

        Ok(Self {
            kind,
            name,
            explicit_name: explicit,
            query,
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

fn parse_body(scheme: &str, body: &str) -> Result<EndpointKind, SpecError> {
    match scheme {
        "serial" => parse_serial_body(body),
        "udps" => {
            parse_host_port(body, "udps").map(|(host, port)| EndpointKind::UdpServer { host, port })
        }
        "udpc" => {
            parse_host_port(body, "udpc").map(|(host, port)| EndpointKind::UdpClient { host, port })
        }
        "tcps" => {
            parse_host_port(body, "tcps").map(|(host, port)| EndpointKind::TcpServer { host, port })
        }
        "tcpc" => {
            parse_host_port(body, "tcpc").map(|(host, port)| EndpointKind::TcpClient { host, port })
        }
        other => Err(SpecError::UnknownScheme(other.to_string())),
    }
}

fn parse_serial_body(body: &str) -> Result<EndpointKind, SpecError> {
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
    Ok(EndpointKind::Serial {
        path: path.to_string(),
        baud,
    })
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
        EndpointKind::Serial { path, baud } => format!("serial-{}-{baud}", sanitize_for_name(path)),
        EndpointKind::UdpServer { host, port } => {
            format!("udps-{}-{port}", sanitize_for_name(host))
        }
        EndpointKind::UdpClient { host, port } => {
            format!("udpc-{}-{port}", sanitize_for_name(host))
        }
        EndpointKind::TcpServer { host, port } => {
            format!("tcps-{}-{port}", sanitize_for_name(host))
        }
        EndpointKind::TcpClient { host, port } => {
            format!("tcpc-{}-{port}", sanitize_for_name(host))
        }
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

fn parse_query(s: &str) -> Result<BTreeMap<String, String>, SpecError> {
    let mut map = BTreeMap::new();
    if s.is_empty() {
        return Ok(map);
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
        if !KNOWN_QUERY_KEYS.contains(&k) {
            return Err(SpecError::UnknownQueryKey {
                key: k.to_string(),
                suggestion: suggest_query_key(k),
            });
        }
        if map.insert(k.to_string(), v.to_string()).is_some() {
            return Err(SpecError::DuplicateQueryKey(k.to_string()));
        }
    }
    Ok(map)
}

fn suggest_query_key(unknown: &str) -> Option<&'static str> {
    KNOWN_QUERY_KEYS
        .iter()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(input: &str) -> EndpointSpec {
        EndpointSpec::parse(input).unwrap_or_else(|e| panic!("expected ok for {input:?}, got {e}"))
    }

    fn parse_err(input: &str) -> SpecError {
        EndpointSpec::parse(input).expect_err(&format!("expected err for {input:?}"))
    }

    #[test]
    fn serial_colon_form() {
        let s = parse_ok("serial:/dev/ttyUSB0:921600");
        assert_eq!(
            s.kind,
            EndpointKind::Serial {
                path: "/dev/ttyUSB0".into(),
                baud: 921600
            }
        );
        assert_eq!(s.name, "serial-_dev_ttyUSB0-921600");
        assert!(!s.explicit_name);
    }

    #[test]
    fn serial_comma_form() {
        let s = parse_ok("serial:/dev/ttyUSB0,921600");
        assert_eq!(
            s.kind,
            EndpointKind::Serial {
                path: "/dev/ttyUSB0".into(),
                baud: 921600
            }
        );
    }

    #[test]
    fn serial_windows_com_colon() {
        let s = parse_ok("serial:COM3:115200");
        assert_eq!(
            s.kind,
            EndpointKind::Serial {
                path: "COM3".into(),
                baud: 115200
            }
        );
    }

    #[test]
    fn serial_windows_com_comma() {
        let s = parse_ok("serial:COM3,115200");
        assert_eq!(
            s.kind,
            EndpointKind::Serial {
                path: "COM3".into(),
                baud: 115200
            }
        );
    }

    #[test]
    fn serial_windows_unc_path() {
        let s = parse_ok(r"serial:\\.\COM10:115200");
        assert_eq!(
            s.kind,
            EndpointKind::Serial {
                path: r"\\.\COM10".into(),
                baud: 115200
            }
        );
    }

    #[test]
    fn serial_by_id_symlink() {
        let s = parse_ok("serial:/dev/serial/by-id/usb-FTDI-port0:57600");
        assert_eq!(
            s.kind,
            EndpointKind::Serial {
                path: "/dev/serial/by-id/usb-FTDI-port0".into(),
                baud: 57600
            }
        );
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
    fn udps_ipv4() {
        let s = parse_ok("udps:0.0.0.0:14550");
        assert_eq!(
            s.kind,
            EndpointKind::UdpServer {
                host: "0.0.0.0".into(),
                port: 14550
            }
        );
        assert_eq!(s.name, "udps-0_0_0_0-14550");
    }

    #[test]
    fn udps_ipv6_dual_stack() {
        let s = parse_ok("udps:[::]:14550");
        assert_eq!(
            s.kind,
            EndpointKind::UdpServer {
                host: "::".into(),
                port: 14550
            }
        );
        assert_eq!(s.name, "udps-__-14550");
    }

    #[test]
    fn udpc_ipv4() {
        let s = parse_ok("udpc:192.168.1.5:14550");
        assert!(matches!(
            s.kind,
            EndpointKind::UdpClient { ref host, port: 14550 } if host == "192.168.1.5"
        ));
    }

    #[test]
    fn tcps_ipv6_bracketed() {
        let s = parse_ok("tcps:[2001:db8::1]:5760");
        assert_eq!(
            s.kind,
            EndpointKind::TcpServer {
                host: "2001:db8::1".into(),
                port: 5760
            }
        );
    }

    #[test]
    fn tcpc_hostname_explicit_name() {
        let s = parse_ok("tcpc:companion.local:5760#vehicle");
        assert_eq!(
            s.kind,
            EndpointKind::TcpClient {
                host: "companion.local".into(),
                port: 5760
            }
        );
        assert_eq!(s.name, "vehicle");
    }

    #[test]
    fn tcpc_with_query() {
        let s = parse_ok("tcpc:companion.local:5760#vehicle?group=uplink");
        assert_eq!(s.name, "vehicle");
        assert_eq!(s.query.get("group").map(String::as_str), Some("uplink"));
    }

    #[test]
    fn udps_with_sniffer_query() {
        let s = parse_ok("udps:0.0.0.0:14551#tap?sniffer=true");
        assert_eq!(s.name, "tap");
        assert_eq!(s.query.get("sniffer").map(String::as_str), Some("true"));
    }

    #[test]
    fn query_filter_list_stored_raw() {
        let s = parse_ok("tcpc:gcs.local:5760?block_msgid_in=33,32&allow_src_sys_out=1");
        assert_eq!(
            s.query.get("block_msgid_in").map(String::as_str),
            Some("33,32")
        );
        assert_eq!(
            s.query.get("allow_src_sys_out").map(String::as_str),
            Some("1")
        );
        assert_eq!(s.name, "tcpc-gcs_local-5760");
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
            SpecError::UnknownQueryKey { key, suggestion } => {
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
        assert!(s.query.is_empty());
    }

    #[test]
    fn trailing_ampersand_tolerated() {
        let s = parse_ok("udps:0.0.0.0:1?sniffer=true&");
        assert_eq!(s.query.get("sniffer").map(String::as_str), Some("true"));
    }

    #[test]
    fn empty_value_ok() {
        let s = parse_ok("udps:0.0.0.0:1?group=");
        assert_eq!(s.query.get("group").map(String::as_str), Some(""));
    }

    #[test]
    fn value_with_embedded_equals_kept_intact() {
        let s = parse_ok("udps:0.0.0.0:1?group=a=b");
        assert_eq!(s.query.get("group").map(String::as_str), Some("a=b"));
    }

    #[test]
    fn fragment_before_query_order_locked() {
        let s = parse_ok("udps:0.0.0.0:1#name?group=g");
        assert_eq!(s.name, "name");
        assert_eq!(s.query.get("group").map(String::as_str), Some("g"));
    }

    #[test]
    fn query_before_fragment_rejected() {
        match parse_err("udps:0.0.0.0:1?group=val#nope") {
            SpecError::MalformedQuery(_) => {}
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn all_filter_keys_are_known() {
        let q = "allow_msgid_in=1&block_msgid_in=2&allow_msgid_out=3&block_msgid_out=4\
                 &allow_src_sys_in=5&block_src_sys_in=6&allow_src_sys_out=7&block_src_sys_out=8\
                 &allow_src_comp_in=9&block_src_comp_in=10&allow_src_comp_out=11&block_src_comp_out=12";
        let s = parse_ok(&format!("tcpc:x:1?{q}"));
        assert_eq!(s.query.len(), 12);
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
    fn known_query_keys_sorted_and_unique() {
        let mut copy: Vec<&&str> = KNOWN_QUERY_KEYS.iter().collect();
        copy.sort();
        copy.dedup();
        assert_eq!(copy.len(), KNOWN_QUERY_KEYS.len(), "duplicates present");
        for w in KNOWN_QUERY_KEYS.windows(2) {
            assert!(w[0] < w[1], "not sorted: {} >= {}", w[0], w[1]);
        }
    }
}
