use std::net::{IpAddr, SocketAddr};

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
pub fn parse_kind(
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
        "udps" => parse_listen_addr(body, "udps").and_then(|bind_addr| {
            let mut ep = UdpServerEndpoint {
                bind_addr,
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
        "tcps" => parse_listen_addr(body, "tcps").and_then(|bind_addr| {
            let mut ep = TcpServerEndpoint {
                bind_addr,
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

/// Listen-side parser used by `tcps:` and `udps:`: same `host:port` grammar
/// as `parse_host_port`, but with an additional constraint that the host
/// must be an IP literal (CLAUDE.md "malformed addresses are fatal" — bind
/// targets are not resolved at runtime, only dial targets are).
fn parse_listen_addr(body: &str, scheme: &'static str) -> Result<SocketAddr, SpecError> {
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
    use super::{sanitize_for_name, validate_name};
    use crate::endpoint::spec::{
        EndpointKind, EndpointSpec, SerialEndpoint, SpecError, TcpClientEndpoint,
        TcpServerEndpoint, UdpClientEndpoint, UdpServerEndpoint,
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

    // -- serial: body parser --

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

    // -- host:port body parser (udps/udpc/tcps/tcpc) --

    #[test]
    fn udps_ipv4() {
        let s = parse_ok("udps:0.0.0.0:14550");
        let e = as_udps(&s);
        assert_eq!(e.bind_addr.to_string(), "0.0.0.0:14550");
        assert_eq!(s.name, "udps-0_0_0_0-14550");
    }

    #[test]
    fn udps_ipv6_dual_stack() {
        let s = parse_ok("udps:[::]:14550");
        let e = as_udps(&s);
        assert_eq!(e.bind_addr.to_string(), "[::]:14550");
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
        assert_eq!(e.bind_addr.to_string(), "[2001:db8::1]:5760");
    }

    /// CLAUDE.md "malformed addresses are fatal" + the locked-decision rule
    /// that `tcps:`/`udps:` accept only IP literals: a hostname on the
    /// listen side must be rejected at parse time, not at spawn time.
    #[test]
    fn tcps_hostname_rejected_at_parse() {
        let err = parse_err("tcps:localhost:5760");
        match err {
            SpecError::MalformedBody { scheme, reason, .. } => {
                assert_eq!(scheme, "tcps");
                assert!(
                    reason.contains("must be an IP literal"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("expected MalformedBody, got {other:?}"),
        }
    }

    #[test]
    fn udps_hostname_rejected_at_parse() {
        let err = parse_err("udps:gcs.local:14550");
        match err {
            SpecError::MalformedBody { scheme, reason, .. } => {
                assert_eq!(scheme, "udps");
                assert!(
                    reason.contains("must be an IP literal"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("expected MalformedBody, got {other:?}"),
        }
    }

    /// `tcpc:` and `udpc:` still accept hostnames (DNS resolved at
    /// runtime); parse-time validation only applies to the listen side.
    #[test]
    fn tcpc_hostname_still_accepted() {
        let s = parse_ok("tcpc:companion.local:5760");
        let e = as_tcpc(&s);
        assert_eq!(e.host, "companion.local");
        assert_eq!(e.port, 5760);
    }

    #[test]
    fn udpc_hostname_still_accepted() {
        let s = parse_ok("udpc:gcs.example:14550");
        let e = as_udpc(&s);
        assert_eq!(e.host, "gcs.example");
        assert_eq!(e.port, 14550);
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
