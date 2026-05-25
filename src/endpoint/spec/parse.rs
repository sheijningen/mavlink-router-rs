use std::net::{IpAddr, SocketAddr};

use super::Scheme;
use super::endpoint_kinds::{
    EndpointKind, SerialEndpoint, TcpClientEndpoint, TcpServerEndpoint, UdpClientEndpoint,
    UdpServerEndpoint,
};
use super::error::SpecError;
use super::query::{SerialApplier, UdpClientApplier, UdpServerApplier, apply_pairs};

/// Split the post-scheme remainder into `(body, name, query)`. `#name`
/// precedes `?query`.
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
        Scheme::Serial => {
            let (path, baud) = parse_serial_body(body)?;
            let mut endpoint = SerialEndpoint {
                path,
                baud,
                ..SerialEndpoint::default()
            };
            apply_pairs(&mut SerialApplier(&mut endpoint), scheme, pairs)?;
            Ok(EndpointKind::Serial(endpoint))
        }
        Scheme::UdpServer => {
            let bind_addr = parse_listen_addr(body, scheme)?;
            let mut endpoint = UdpServerEndpoint {
                bind_addr,
                ..UdpServerEndpoint::default()
            };
            apply_pairs(&mut UdpServerApplier(&mut endpoint), scheme, pairs)?;
            Ok(EndpointKind::UdpServer(endpoint))
        }
        Scheme::UdpClient => {
            let (host, port) = parse_host_port(body, scheme)?;
            let mut endpoint = UdpClientEndpoint {
                host,
                port,
                ..UdpClientEndpoint::default()
            };
            apply_pairs(&mut UdpClientApplier(&mut endpoint), scheme, pairs)?;
            Ok(EndpointKind::UdpClient(endpoint))
        }
        Scheme::TcpServer => {
            let bind_addr = parse_listen_addr(body, scheme)?;
            let mut endpoint = TcpServerEndpoint {
                bind_addr,
                ..TcpServerEndpoint::default()
            };
            apply_pairs(&mut endpoint.identity, scheme, pairs)?;
            Ok(EndpointKind::TcpServer(endpoint))
        }
        Scheme::TcpClient => {
            let (host, port) = parse_host_port(body, scheme)?;
            let mut endpoint = TcpClientEndpoint {
                host,
                port,
                ..TcpClientEndpoint::default()
            };
            apply_pairs(&mut endpoint.identity, scheme, pairs)?;
            Ok(EndpointKind::TcpClient(endpoint))
        }
    }
}

const SERIAL_GRAMMAR: &str =
    "expected 'serial:<path>:<baud>' or 'serial:<path>,<baud>', e.g. 'serial:/dev/ttyUSB0:115200'";

pub(crate) fn parse_serial_body(body: &str) -> Result<(String, u32), SpecError> {
    if body.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme: Scheme::Serial,
            body: body.to_string(),
            reason: format!("empty body ({SERIAL_GRAMMAR})"),
        });
    }
    let last_colon = body.rfind(':');
    let last_comma = body.rfind(',');
    let pos = match (last_colon, last_comma) {
        (Some(colon), Some(comma)) => Some(colon.max(comma)),
        (Some(colon), None) => Some(colon),
        (None, Some(comma)) => Some(comma),
        (None, None) => None,
    };
    let Some(pos) = pos else {
        return Err(SpecError::MalformedBody {
            scheme: Scheme::Serial,
            body: body.to_string(),
            reason: format!("no ':' or ',' separator between path and baud ({SERIAL_GRAMMAR})"),
        });
    };
    let path = &body[..pos];
    let baud_str = &body[pos + 1..];
    if path.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme: Scheme::Serial,
            body: body.to_string(),
            reason: format!("empty device path ({SERIAL_GRAMMAR})"),
        });
    }
    let baud: u32 = baud_str.parse().map_err(|_| SpecError::MalformedBody {
        scheme: Scheme::Serial,
        body: body.to_string(),
        reason: format!("baud '{baud_str}' is not a valid u32 ({SERIAL_GRAMMAR})"),
    })?;
    if baud == 0 {
        return Err(SpecError::MalformedBody {
            scheme: Scheme::Serial,
            body: body.to_string(),
            reason: format!("baud must be > 0 ({SERIAL_GRAMMAR})"),
        });
    }
    Ok((path.to_string(), baud))
}

fn host_port_grammar(scheme: Scheme) -> &'static str {
    match scheme {
        Scheme::UdpServer | Scheme::TcpServer => {
            "expected '<ip>:<port>', e.g. '0.0.0.0:14550' or '[::]:5760' (hostnames are not resolved for listen-side endpoints)"
        }
        Scheme::UdpClient | Scheme::TcpClient => {
            "expected '<host>:<port>', e.g. '192.168.1.5:14550' or 'gcs.example:5760'"
        }
        Scheme::Serial => SERIAL_GRAMMAR,
    }
}

/// Listen-side parser used by `tcps:` and `udps:`: same `host:port` grammar
/// as `parse_host_port`, but the host must be an IP literal — bind targets
/// are not resolved at runtime, only dial targets are.
pub(crate) fn parse_listen_addr(body: &str, scheme: Scheme) -> Result<SocketAddr, SpecError> {
    let (host, port) = parse_host_port(body, scheme)?;
    let ip_addr: IpAddr = host.parse().map_err(|_| SpecError::MalformedBody {
        scheme,
        body: body.to_string(),
        reason: format!(
            "listen host '{host}' must be an IP literal (e.g. '0.0.0.0', '[::]', '[::1]'); hostnames are not resolved for {scheme}: endpoints"
        ),
    })?;
    Ok(SocketAddr::new(ip_addr, port))
}

pub(crate) fn parse_host_port(body: &str, scheme: Scheme) -> Result<(String, u16), SpecError> {
    let grammar = host_port_grammar(scheme);
    if body.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme,
            body: body.to_string(),
            reason: format!("empty body ({grammar})"),
        });
    }
    let (host, port_str) = if let Some(rest) = body.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err(SpecError::MalformedBody {
                scheme,
                body: body.to_string(),
                reason: format!("unclosed '[' in IPv6 literal ({grammar})"),
            });
        };
        let host = &rest[..end];
        let after = &rest[end + 1..];
        let Some(port_str) = after.strip_prefix(':') else {
            return Err(SpecError::MalformedBody {
                scheme,
                body: body.to_string(),
                reason: format!("expected ':<port>' after ']' ({grammar})"),
            });
        };
        (host.to_string(), port_str.to_string())
    } else {
        let Some((host, port_str)) = body.rsplit_once(':') else {
            return Err(SpecError::MalformedBody {
                scheme,
                body: body.to_string(),
                reason: format!("no ':' between host and port ({grammar})"),
            });
        };
        (host.to_string(), port_str.to_string())
    };
    if host.is_empty() {
        return Err(SpecError::MalformedBody {
            scheme,
            body: body.to_string(),
            reason: format!("empty host ({grammar})"),
        });
    }
    let port: u16 = port_str.parse().map_err(|_| SpecError::MalformedBody {
        scheme,
        body: body.to_string(),
        reason: format!("port '{port_str}' is not a valid u16 ({grammar})"),
    })?;
    Ok((host, port))
}

/// Shared regex check for explicit `#name` values and `?group=` values:
/// `[A-Za-z0-9_-]{1,64}`. Kept as a bool helper so callers can wrap it
/// in whichever [`SpecError`] variant fits their context.
pub fn name_matches_regex(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

pub fn validate_name(name: &str) -> Result<(), SpecError> {
    if !name_matches_regex(name) {
        return Err(SpecError::InvalidName(name.to_string()));
    }
    Ok(())
}

pub fn sanitize_for_name(text: &str) -> String {
    text.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::{Scheme, sanitize_for_name, validate_name};
    use crate::endpoint::spec::{
        EndpointKind, EndpointSpec, SerialEndpoint, SpecError, TcpClientEndpoint, UdpServerEndpoint,
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
        let spec = parse_ok(input);
        let endpoint = as_serial(&spec);
        assert_eq!(endpoint.path, path);
        assert_eq!(endpoint.baud, baud);
    }

    #[test]
    fn serial_with_explicit_name() {
        let spec = parse_ok("serial:/dev/ttyUSB0:921600#vehicle");
        assert_eq!(spec.name, "vehicle");
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
        let spec = parse_ok(input);
        let bind = match &spec.kind {
            EndpointKind::UdpServer(endpoint) => endpoint.bind_addr,
            EndpointKind::TcpServer(endpoint) => endpoint.bind_addr,
            other => panic!("expected listen-side endpoint, got {other:?}"),
        };
        assert_eq!(bind.to_string(), expected_addr);
        if let Some(name) = expected_name {
            assert_eq!(spec.name, name);
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
        let spec = parse_ok(input);
        let (host, port) = match &spec.kind {
            EndpointKind::UdpClient(endpoint) => (endpoint.host.as_str(), endpoint.port),
            EndpointKind::TcpClient(endpoint) => (endpoint.host.as_str(), endpoint.port),
            other => panic!("expected dial-side endpoint, got {other:?}"),
        };
        assert_eq!(host, expected_host);
        assert_eq!(port, expected_port);
    }

    #[test]
    fn tcpc_hostname_explicit_name() {
        let spec = parse_ok("tcpc:companion.local:5760#vehicle");
        let endpoint = as_tcpc(&spec);
        assert_eq!(endpoint.host, "companion.local");
        assert_eq!(endpoint.port, 5760);
        assert_eq!(spec.name, "vehicle");
    }

    /// `host:port` rejection matrix. The last 2 cases additionally assert
    /// the `must be an IP literal` reason substring — that text is part of
    /// the operator-visible contract.
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

    // -- validate_name / sanitize_for_name --

    #[test]
    fn name_max_64_ok() {
        let name = "a".repeat(64);
        let spec = parse_ok(&format!("udps:0.0.0.0:1#{name}"));
        assert_eq!(spec.name, name);
    }

    #[test]
    fn name_too_long_fails() {
        let name = "a".repeat(65);
        assert!(matches!(
            parse_err(&format!("udps:0.0.0.0:1#{name}")),
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
        let spec = parse_ok("udps:0.0.0.0:1#abc_DEF-123");
        assert_eq!(spec.name, "abc_DEF-123");
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
            let spec = parse_ok(input);
            validate_name(&spec.name)
                .unwrap_or_else(|err| panic!("auto-name {:?} fails name regex: {err}", spec.name));
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
        let spec = parse_ok("udps:0.0.0.0:1#name?group=g");
        assert_eq!(spec.name, "name");
        let endpoint = as_udps(&spec);
        assert_eq!(endpoint.identity.group.as_deref(), Some("g"));
    }

    #[test]
    fn query_before_fragment_rejected() {
        match parse_err("udps:0.0.0.0:1?group=val#nope") {
            SpecError::MalformedQuery(_) => {}
            other => panic!("wrong error: {other:?}"),
        }
    }
}
