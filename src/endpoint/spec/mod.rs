//! Endpoint-spec parsing: turn one CLI string (or one TOML entry) into a
//! fully-typed [`EndpointSpec`]. See the "Project layout" section of
//! `CLAUDE.md` for the per-file breakdown.

use std::fmt;

pub(crate) mod bounds;
mod endpoint_kinds;
mod error;
mod parse;
mod query;

pub use endpoint_kinds::{
    CommonQuery, EndpointKind, SerialEndpoint, SerialFlowControl, TcpClientEndpoint,
    TcpServerEndpoint, UdpClientEndpoint, UdpServerEndpoint,
};
pub use error::SpecError;

/// Closed set of endpoint schemes the parser recognises. Carrying the scheme
/// as an enum past the string-tokenising boundary (CLI argv, TOML `type`
/// field) makes downstream matches exhaustive — every helper that branches on
/// scheme is type-checked against this list, with no `unreachable!` fallback
/// needed when a new variant is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scheme {
    Serial,
    UdpServer,
    UdpClient,
    TcpServer,
    TcpClient,
}

impl Scheme {
    /// CLI prefix / TOML `type` literal for this variant. Pinned strings —
    /// these appear verbatim in error messages and CLAUDE.md's CLI grammar.
    pub fn as_str(self) -> &'static str {
        match self {
            Scheme::Serial => "serial",
            Scheme::UdpServer => "udps",
            Scheme::UdpClient => "udpc",
            Scheme::TcpServer => "tcps",
            Scheme::TcpClient => "tcpc",
        }
    }

    /// String → enum at the parser boundary. `None` means the prefix isn't a
    /// known scheme; callers decide which error to raise (CLI:
    /// [`SpecError::UnknownScheme`], TOML: [`crate::error::Error::ConfigSchema`]).
    pub fn try_from_str(raw: &str) -> Option<Self> {
        match raw {
            "serial" => Some(Scheme::Serial),
            "udps" => Some(Scheme::UdpServer),
            "udpc" => Some(Scheme::UdpClient),
            "tcps" => Some(Scheme::TcpServer),
            "tcpc" => Some(Scheme::TcpClient),
            _ => None,
        }
    }

    /// String → enum on the CLI path, surfacing [`SpecError::UnknownScheme`]
    /// on a bad prefix. Used by [`EndpointSpec::parse`] and the CLI adapter
    /// in `config::EndpointEntry::from_cli_string`.
    pub fn from_cli_prefix(raw: &str) -> Result<Self, SpecError> {
        Self::try_from_str(raw).ok_or_else(|| SpecError::UnknownScheme(raw.to_string()))
    }
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

use parse::{default_name, parse_kind, validate_name};

// Re-exports for the TOML / config-side parser (CLAUDE.md "one parser for both
// CLI and TOML" locked decision). `config::EndpointEntry::from_cli_string`
// uses these to tokenise a CLI spec string into the same typed-entry shape the
// TOML deserialiser produces.
pub(crate) use parse::{
    parse_host_port, parse_listen_addr, parse_serial_body, split_body_name_query,
};
pub(crate) use query::{parse_query_pairs, parse_u64, parse_usize, suggest_query_key};

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
}

impl EndpointSpec {
    /// Parse a CLI-style spec string straight into an [`EndpointSpec`].
    /// Convenient for unit tests and for any consumer that doesn't need the
    /// typed-entry intermediate. The user-facing `cli::parse_specs` path
    /// now routes CLI argv through `config::EndpointEntry::from_cli_string`
    /// followed by `config::EndpointEntry::into_spec` so CLI and TOML inputs
    /// share a single resolution pipeline; both paths produce equivalent
    /// [`EndpointSpec`] values for the same input (pinned by
    /// `cli_path_equivalent_to_direct_endpoint_spec_parse` in `config.rs`).
    pub fn parse(input: &str) -> Result<Self, SpecError> {
        let (scheme_str, rest) = input
            .split_once(':')
            .ok_or_else(|| SpecError::MissingScheme(input.to_string()))?;
        if scheme_str.is_empty() {
            return Err(SpecError::MissingScheme(input.to_string()));
        }

        let (body, explicit_name, query_str) = split_body_name_query(rest);

        if body.contains('?') {
            return Err(SpecError::MalformedQuery(format!(
                "'?' must follow '#name' (got '{body}')"
            )));
        }

        let pairs = parse_query_pairs(query_str.unwrap_or(""))?;
        let scheme = Scheme::from_cli_prefix(scheme_str)?;
        Self::build(scheme, body, explicit_name, &pairs)
    }

    /// Construct an [`EndpointSpec`] from an already-tokenised input. Used by
    /// both [`EndpointSpec::parse`] (CLI strings) and the TOML config parser:
    /// the TOML side synthesises `body` from typed fields (`bind_addr`,
    /// `host`/`port`, `path`/`baud`) and `pairs` from typed identity / common
    /// / filter fields, then funnels through this single entry point so the
    /// "one parser for both CLI and TOML" locked decision is enforced by
    /// type-system reuse rather than by convention.
    pub fn build(
        scheme: Scheme,
        body: &str,
        explicit_name: Option<&str>,
        pairs: &[(String, String)],
    ) -> Result<Self, SpecError> {
        let kind = parse_kind(scheme, body, pairs)?;
        let name = match explicit_name {
            Some(n) => {
                validate_name(n)?;
                n.to_string()
            }
            None => default_name(&kind),
        };
        Ok(Self { kind, name })
    }
}
