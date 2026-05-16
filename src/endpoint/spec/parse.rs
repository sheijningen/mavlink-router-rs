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
        EndpointKind::UdpServer(e) => format!("udps-{}-{}", sanitize_for_name(&e.host), e.port),
        EndpointKind::UdpClient(e) => format!("udpc-{}-{}", sanitize_for_name(&e.host), e.port),
        EndpointKind::TcpServer(e) => format!("tcps-{}-{}", sanitize_for_name(&e.host), e.port),
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
