//! Endpoint-spec parsing: turn one CLI string (or one TOML entry) into a
//! fully-typed [`EndpointSpec`]. See the "Project layout" section of
//! `CLAUDE.md` for the per-file breakdown.

mod endpoint_kinds;
mod error;
mod parse;
mod query;

#[cfg(test)]
mod tests;

pub use endpoint_kinds::{
    CommonQuery, EndpointKind, MsgIdRange, SerialEndpoint, TcpClientEndpoint, TcpServerEndpoint,
    U8Range, UdpClientEndpoint, UdpServerEndpoint,
};
pub use error::SpecError;

use parse::{default_name, parse_kind, split_body_name_query, validate_name};
use query::parse_query_pairs;

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
