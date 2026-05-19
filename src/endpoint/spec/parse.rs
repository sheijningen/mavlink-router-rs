use std::net::{IpAddr, SocketAddr};

use super::Scheme;
use super::endpoint_kinds::{
    EndpointKind, SerialEndpoint, TcpClientEndpoint, TcpServerEndpoint, UdpClientEndpoint,
    UdpServerEndpoint,
};
use super::error::SpecError;
use super::query::{
    SerialApplier, TcpClientApplier, TcpServerApplier, UdpClientApplier, UdpServerApplier,
    apply_pairs,
};

/// Split the post-scheme remainder into `(body, name, query)`. `#name`
/// precedes `?query` — this ordering is locked because every example in
/// CLAUDE.md relies on it and the parser is simpler when `#` is searched
/// before `?` is parsed.
pub fn split_body_name_query(rest: &str) -> (&str, Option<&str>, Option<&str>) {
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

/// Dispatch from scheme + body + parsed query pairs to the concrete
/// `EndpointKind` variant. Each arm parses the address, defaults the
/// endpoint struct, then applies query knobs via the per-scheme applier.
/// Scheme arrives as a typed [`Scheme`] — the string→enum boundary is owned
/// by the caller ([`super::EndpointSpec::parse`] / the TOML path), so this
/// match is exhaustive and needs no `UnknownScheme` fallback.
pub fn parse_kind(
    scheme: Scheme,
    body: &str,
    pairs: &[(String, String)],
) -> Result<EndpointKind, SpecError> {
    match scheme {
        Scheme::Serial => parse_serial_body(body).and_then(|(path, baud)| {
            let mut ep = SerialEndpoint {
                path,
                baud,
                ..SerialEndpoint::default()
            };
            apply_pairs(&mut SerialApplier(&mut ep), Scheme::Serial, pairs)?;
            Ok(EndpointKind::Serial(ep))
        }),
        Scheme::UdpServer => parse_listen_addr(body, Scheme::UdpServer).and_then(|bind_addr| {
            let mut ep = UdpServerEndpoint {
                bind_addr,
                ..UdpServerEndpoint::default()
            };
            apply_pairs(&mut UdpServerApplier(&mut ep), Scheme::UdpServer, pairs)?;
            Ok(EndpointKind::UdpServer(ep))
        }),
        Scheme::UdpClient => parse_host_port(body, Scheme::UdpClient).and_then(|(host, port)| {
            let mut ep = UdpClientEndpoint {
                host,
                port,
                ..UdpClientEndpoint::default()
            };
            apply_pairs(&mut UdpClientApplier(&mut ep), Scheme::UdpClient, pairs)?;
            Ok(EndpointKind::UdpClient(ep))
        }),
        Scheme::TcpServer => parse_listen_addr(body, Scheme::TcpServer).and_then(|bind_addr| {
            let mut ep = TcpServerEndpoint {
                bind_addr,
                ..TcpServerEndpoint::default()
            };
            apply_pairs(&mut TcpServerApplier(&mut ep), Scheme::TcpServer, pairs)?;
            Ok(EndpointKind::TcpServer(ep))
        }),
        Scheme::TcpClient => parse_host_port(body, Scheme::TcpClient).and_then(|(host, port)| {
            let mut ep = TcpClientEndpoint {
                host,
                port,
                ..TcpClientEndpoint::default()
            };
            apply_pairs(&mut TcpClientApplier(&mut ep), Scheme::TcpClient, pairs)?;
            Ok(EndpointKind::TcpClient(ep))
        }),
    }
}

pub(crate) fn parse_serial_body(body: &str) -> Result<(String, u32), SpecError> {
    if body.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme: Scheme::Serial,
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
            scheme: Scheme::Serial,
            body: body.to_string(),
            reason: "expected '<path>:<baud>' or '<path>,<baud>'".to_string(),
        });
    };
    let path = &body[..pos];
    let baud_str = &body[pos + 1..];
    if path.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme: Scheme::Serial,
            body: body.to_string(),
            reason: "empty device path".to_string(),
        });
    }
    let baud: u32 = baud_str.parse().map_err(|_| SpecError::MalformedBody {
        scheme: Scheme::Serial,
        body: body.to_string(),
        reason: format!("baud '{baud_str}' is not a valid u32"),
    })?;
    if baud == 0 {
        return Err(SpecError::MalformedBody {
            scheme: Scheme::Serial,
            body: body.to_string(),
            reason: "baud must be > 0".to_string(),
        });
    }
    Ok((path.to_string(), baud))
}

/// Listen-side parser used by `tcps:` and `udps:`: same `host:port` grammar
/// as `parse_host_port`, but with an additional constraint that the host
/// must be an IP literal (CLAUDE.md "malformed addresses are fatal" — bind
/// targets are not resolved at runtime, only dial targets are).
pub(crate) fn parse_listen_addr(body: &str, scheme: Scheme) -> Result<SocketAddr, SpecError> {
    let (host, port) = parse_host_port(body, scheme)?;
    let ip: IpAddr = host.parse().map_err(|_| SpecError::MalformedBody {
        scheme,
        body: body.to_string(),
        reason: format!(
            "listen host '{host}' must be an IP literal (e.g. '0.0.0.0', '[::]', '[::1]'); hostnames are not resolved for {scheme}: endpoints"
        ),
    })?;
    Ok(SocketAddr::new(ip, port))
}

pub(crate) fn parse_host_port(body: &str, scheme: Scheme) -> Result<(String, u16), SpecError> {
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

pub fn validate_name(s: &str) -> Result<(), SpecError> {
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

pub fn default_name(kind: &EndpointKind) -> String {
    match kind {
        EndpointKind::Serial(e) => format!("serial-{}-{}", sanitize_for_name(&e.path), e.baud),
        EndpointKind::UdpServer(e) => format!(
            "udps-{}-{}",
            sanitize_for_name(&e.bind_addr.ip().to_string()),
            e.bind_addr.port()
        ),
        EndpointKind::UdpClient(e) => format!("udpc-{}-{}", sanitize_for_name(&e.host), e.port),
        EndpointKind::TcpServer(e) => format!(
            "tcps-{}-{}",
            sanitize_for_name(&e.bind_addr.ip().to_string()),
            e.bind_addr.port()
        ),
        EndpointKind::TcpClient(e) => format!("tcpc-{}-{}", sanitize_for_name(&e.host), e.port),
    }
}

pub fn sanitize_for_name(s: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::{Scheme, sanitize_for_name, validate_name};
    use crate::endpoint::spec::{
        EndpointKind, EndpointSpec, SerialEndpoint, SpecError, TcpClientEndpoint, UdpServerEndpoint,
    };
    use rstest::rstest;

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

    fn as_tcpc(spec: &EndpointSpec) -> &TcpClientEndpoint {
        match &spec.kind {
            EndpointKind::TcpClient(e) => e,
            other => panic!("expected tcpc, got {other:?}"),
        }
    }

    // -- serial: body parser --

    /// Body grammar matrix: every `(path, baud)` pair must round-trip under
    /// both `:` and `,` separators, across Unix paths, Windows COM names,
    /// UNC paths, and by-id symlinks. The auto-name derivation for the
    /// colon-form Unix path is covered separately by
    /// `auto_names_satisfy_explicit_name_regex` and
    /// `sanitize_for_name_replaces_disallowed_chars`.
    #[rstest]
    #[case::colon_form_unix("serial:/dev/ttyUSB0:921600", "/dev/ttyUSB0", 921600)]
    #[case::comma_form_unix("serial:/dev/ttyUSB0,921600", "/dev/ttyUSB0", 921600)]
    #[case::colon_form_windows_com("serial:COM3:115200", "COM3", 115200)]
    #[case::comma_form_windows_com("serial:COM3,115200", "COM3", 115200)]
    #[case::windows_unc_path(r"serial:\\.\COM10:115200", r"\\.\COM10", 115200)]
    #[case::by_id_symlink(
        "serial:/dev/serial/by-id/usb-FTDI-port0:57600",
        "/dev/serial/by-id/usb-FTDI-port0",
        57600
    )]
    fn serial_body_parses(#[case] input: &str, #[case] path: &str, #[case] baud: u32) {
        let s = parse_ok(input);
        let e = as_serial(&s);
        assert_eq!(e.path, path);
        assert_eq!(e.baud, baud);
    }

    #[test]
    fn serial_with_explicit_name() {
        let s = parse_ok("serial:/dev/ttyUSB0:921600#vehicle");
        assert_eq!(s.name, "vehicle");
    }

    /// `parse_serial_body` rejection matrix: missing separator, non-numeric
    /// baud, empty path, and zero baud all produce
    /// `SpecError::MalformedBody { scheme: Serial }`. Pinned together so a
    /// regression that flips the order of checks (e.g. parsing baud before
    /// validating the path) surfaces the affected case by name.
    #[rstest]
    #[case::no_separator("serial:COM3")]
    #[case::non_numeric_baud("serial:/dev/foo:abc")]
    #[case::empty_path("serial::921600")]
    #[case::zero_baud("serial:/dev/foo:0")]
    fn serial_body_rejects_malformed(#[case] input: &str) {
        assert!(matches!(
            parse_err(input),
            SpecError::MalformedBody {
                scheme: Scheme::Serial,
                ..
            }
        ));
    }

    // -- host:port body parser (udps/udpc/tcps/tcpc) --

    /// Listen-side address parser: IPv4 and IPv6 literals round-trip into
    /// the stored `bind_addr`. The two `udps:` cases also pin the
    /// auto-derived name, since the `udps-__-14550` form (IPv6 unspecified
    /// → `__`) is the only place the sanitiser's behaviour on `:` is
    /// exercised at the EndpointSpec layer.
    #[rstest]
    #[case::udps_ipv4("udps:0.0.0.0:14550", "0.0.0.0:14550", Some("udps-0_0_0_0-14550"))]
    #[case::udps_ipv6_dual_stack("udps:[::]:14550", "[::]:14550", Some("udps-__-14550"))]
    #[case::tcps_ipv6_bracketed("tcps:[2001:db8::1]:5760", "[2001:db8::1]:5760", None)]
    fn listen_addr_parses(
        #[case] input: &str,
        #[case] expected_addr: &str,
        #[case] expected_name: Option<&str>,
    ) {
        let s = parse_ok(input);
        let bind = match &s.kind {
            EndpointKind::UdpServer(e) => e.bind_addr,
            EndpointKind::TcpServer(e) => e.bind_addr,
            other => panic!("expected listen-side endpoint, got {other:?}"),
        };
        assert_eq!(bind.to_string(), expected_addr);
        if let Some(n) = expected_name {
            assert_eq!(s.name, n);
        }
    }

    /// Dial-side body parser: `tcpc:` and `udpc:` accept hostnames (DNS
    /// resolved at runtime); the listen-side IP-literal restriction does
    /// not apply here. The `tcpc:` hostname case with explicit `#name` is
    /// kept separate (below) to assert name propagation alongside the body.
    #[rstest]
    #[case::udpc_ipv4("udpc:192.168.1.5:14550", "192.168.1.5", 14550)]
    #[case::tcpc_hostname("tcpc:companion.local:5760", "companion.local", 5760)]
    #[case::udpc_hostname("udpc:gcs.example:14550", "gcs.example", 14550)]
    fn dial_host_port_parses(
        #[case] input: &str,
        #[case] expected_host: &str,
        #[case] expected_port: u16,
    ) {
        let s = parse_ok(input);
        let (host, port) = match &s.kind {
            EndpointKind::UdpClient(e) => (e.host.as_str(), e.port),
            EndpointKind::TcpClient(e) => (e.host.as_str(), e.port),
            other => panic!("expected dial-side endpoint, got {other:?}"),
        };
        assert_eq!(host, expected_host);
        assert_eq!(port, expected_port);
    }

    #[test]
    fn tcpc_hostname_explicit_name() {
        let s = parse_ok("tcpc:companion.local:5760#vehicle");
        let e = as_tcpc(&s);
        assert_eq!(e.host, "companion.local");
        assert_eq!(e.port, 5760);
        assert_eq!(s.name, "vehicle");
    }

    /// `host:port` rejection matrix. The first 7 cases assert
    /// `SpecError::MalformedBody` only (the structural rejections from
    /// `parse_host_port`). The last 2 also assert the `must be an IP
    /// literal` reason: CLAUDE.md "malformed addresses are fatal" + the
    /// listen-side IP-literal-only rule means a hostname must surface at
    /// parse time, with a message specific enough to be greppable, so the
    /// reason substring is part of the contract.
    #[rstest]
    #[case::udps_no_port("udps:nohost", None)]
    #[case::udps_bad_port("udps:foo:abc", None)]
    #[case::udps_port_overflow("udps:foo:99999", None)]
    #[case::udps_empty_host("udps::14550", None)]
    #[case::ipv6_unclosed("udps:[::1", None)]
    #[case::ipv6_no_port_after_bracket("udps:[::1]", None)]
    #[case::ipv6_garbage_after_bracket("udps:[::1]x14550", None)]
    #[case::tcps_hostname_must_be_ip("tcps:localhost:5760", Some("must be an IP literal"))]
    #[case::udps_hostname_must_be_ip("udps:gcs.local:14550", Some("must be an IP literal"))]
    fn host_port_body_rejects_malformed(
        #[case] input: &str,
        #[case] expected_reason_substr: Option<&str>,
    ) {
        match parse_err(input) {
            SpecError::MalformedBody { reason, .. } => {
                if let Some(needle) = expected_reason_substr {
                    assert!(
                        reason.contains(needle),
                        "reason {reason:?} should contain {needle:?}"
                    );
                }
            }
            other => panic!("expected MalformedBody for {input:?}, got {other:?}"),
        }
    }

    // -- validate_name / default_name / sanitize_for_name --

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

    // -- scheme dispatch errors --

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

    // -- split_body_name_query: fragment-before-query ordering is locked --

    #[test]
    fn fragment_before_query_order_locked() {
        let s = parse_ok("udps:0.0.0.0:1#name?group=g");
        assert_eq!(s.name, "name");
        let e = as_udps(&s);
        assert_eq!(e.identity.group.as_deref(), Some("g"));
    }

    #[test]
    fn query_before_fragment_rejected() {
        match parse_err("udps:0.0.0.0:1?group=val#nope") {
            SpecError::MalformedQuery(_) => {}
            other => panic!("wrong error: {other:?}"),
        }
    }
}
