//! TOML deserialisation into [`TomlConfig`] (`Option<T>` globals,
//! `[endpoint.NAME]` tables round-tripped through
//! [`crate::endpoint::spec::EndpointSpec::build`]).

use std::path::Path;

use serde::Deserialize;
use toml::Table;

use crate::config::{LogFormat, LogLevel};
use crate::endpoint::spec::{EndpointSpec, Scheme};
use crate::error::Error;

/// TOML-side input to the [`crate::config::Config::merge`] step. Globals are
/// `Option<T>` to preserve "operator omitted this key" semantics: an unset
/// `log_level` falls through to the CLI value (if set) and then to the
/// default, rather than overwriting either.
///
/// `Default` produces a `TomlConfig` with every global `None` and no
/// endpoints — convenience for tests and the "operator passed no `--config`"
/// branch of the merge orchestrator.
#[derive(Debug, Clone, Default)]
pub struct TomlConfig {
    pub log_level: Option<LogLevel>,
    pub log_format: Option<LogFormat>,
    pub stats: Option<bool>,
    pub stats_interval_secs: Option<u64>,
    pub dedup_ms: Option<u64>,
    pub skip_config_log: Option<bool>,
    pub endpoints: Vec<EndpointSpec>,
}

impl TomlConfig {
    /// Read and parse a TOML file from disk. Wraps [`TomlConfig::parse_str`]
    /// with a typed I/O error that names the offending path.
    pub fn from_path(path: &Path) -> Result<Self, Error> {
        let raw = std::fs::read_to_string(path).map_err(|err| Error::ConfigIo {
            path: path.display().to_string(),
            source: err,
        })?;
        Self::parse_str(&raw)
    }

    /// Parse a TOML string into a [`TomlConfig`]. Schema: [`TomlFile`] /
    /// [`TomlEndpoint`] with `deny_unknown_fields`; each endpoint is a
    /// `[endpoint.NAME]` table whose key is its name, and per-endpoint typed
    /// fields are validated against the chosen `type`.
    ///
    /// Named `parse_str` rather than implementing `FromStr` because the
    /// returned [`Error`] is wider than the `FromStr::Err` idiom expects.
    pub fn parse_str(text: &str) -> Result<Self, Error> {
        let file: TomlFile = toml::from_str(text).map_err(Error::ConfigParse)?;
        file.into_toml_config()
    }

    /// True if the TOML side actually provided any value worth merging.
    pub(crate) fn has_values(&self) -> bool {
        let Self {
            log_level,
            log_format,
            stats,
            stats_interval_secs,
            dedup_ms,
            skip_config_log,
            endpoints,
        } = self;
        log_level.is_some()
            || log_format.is_some()
            || stats.is_some()
            || stats_interval_secs.is_some()
            || dedup_ms.is_some()
            || skip_config_log.is_some()
            || !endpoints.is_empty()
    }
}

/// On-disk TOML schema. Mirrors the global-knob surface and accepts a table
/// of named [`TomlEndpoint`] entries. `deny_unknown_fields` so a typo'd
/// top-level key fails loudly rather than silently being ignored. The endpoint
/// table is kept as a [`Table`] so entries keep their order of appearance.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlFile {
    #[serde(default)]
    log_level: Option<LogLevel>,
    #[serde(default)]
    log_format: Option<LogFormat>,
    #[serde(default)]
    stats: Option<bool>,
    #[serde(default)]
    stats_interval_secs: Option<u64>,
    #[serde(default)]
    dedup_ms: Option<u64>,
    #[serde(default)]
    skip_config_log: Option<bool>,
    #[serde(default)]
    endpoint: Table,
}

impl TomlFile {
    fn into_toml_config(self) -> Result<TomlConfig, Error> {
        let mut endpoints = Vec::with_capacity(self.endpoint.len());
        for (name, value) in self.endpoint {
            let entry = value
                .try_into::<TomlEndpoint>()
                .map_err(Error::ConfigParse)?;
            endpoints.push(entry.into_spec(&name)?);
        }
        Ok(TomlConfig {
            log_level: self.log_level,
            log_format: self.log_format,
            stats: self.stats,
            stats_interval_secs: self.stats_interval_secs,
            dedup_ms: self.dedup_ms,
            skip_config_log: self.skip_config_log,
            endpoints,
        })
    }
}

/// One `[endpoint.NAME]` table entry; the table key is the endpoint name.
/// Flat shape (rather than an internally-tagged enum variant) because serde's
/// `deny_unknown_fields` does not compose with `#[serde(tag = ...)]`, and we
/// want a single typo'd field name to fail loudly. Scheme-conditional
/// validation (e.g. `serial` requires `path`/`baud` and must not carry
/// `bind`/`host`/`port`) runs post-parse in [`TomlEndpoint::into_spec`].
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlEndpoint {
    #[serde(rename = "type")]
    scheme: String,
    path: Option<String>,
    baud: Option<u32>,
    flow_control: Option<String>,
    bind: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    idle_secs: Option<u64>,
    latch_idle_secs: Option<u64>,
    sniffer: Option<bool>,
    group: Option<String>,
    // filters (string form only; array forms rejected at parse time)
    allow_msgid_in: Option<String>,
    block_msgid_in: Option<String>,
    allow_msgid_out: Option<String>,
    block_msgid_out: Option<String>,
    allow_src_sys_in: Option<String>,
    block_src_sys_in: Option<String>,
    allow_src_sys_out: Option<String>,
    block_src_sys_out: Option<String>,
    allow_src_comp_in: Option<String>,
    block_src_comp_in: Option<String>,
    allow_src_comp_out: Option<String>,
    block_src_comp_out: Option<String>,
    allow_src_endpoint_out: Option<String>,
    block_src_endpoint_out: Option<String>,
}

impl TomlEndpoint {
    fn into_spec(self, name: &str) -> Result<EndpointSpec, Error> {
        let scheme = scheme_from_toml_type(&self.scheme, name)?;

        // Reject wrong-scheme fields *before* synthesising the body so the
        // operator's real mistake surfaces over downstream errors.
        self.reject_disallowed_fields(name, scheme)?;
        let body = self.synthesize_body(name, scheme)?;
        let pairs = self.collect_pairs();
        EndpointSpec::build(scheme, &body, Some(name), &pairs).map_err(|source| Error::SpecInToml {
            name: name.to_string(),
            source,
        })
    }

    fn synthesize_body(&self, name: &str, scheme: Scheme) -> Result<String, Error> {
        match scheme {
            Scheme::Serial => {
                let path = self
                    .path
                    .as_ref()
                    .ok_or_else(|| missing(name, scheme, "path"))?;
                let baud = self.baud.ok_or_else(|| missing(name, scheme, "baud"))?;
                Ok(format!("{path}:{baud}"))
            }
            Scheme::UdpServer | Scheme::TcpServer => {
                let bind = self
                    .bind
                    .as_ref()
                    .ok_or_else(|| missing(name, scheme, "bind"))?;
                Ok(bind.clone())
            }
            Scheme::UdpClient | Scheme::TcpClient => {
                let host = self
                    .host
                    .as_ref()
                    .ok_or_else(|| missing(name, scheme, "host"))?;
                let port = self.port.ok_or_else(|| missing(name, scheme, "port"))?;
                // IPv6 hosts must be bracketed so `rsplit(':')` finds the
                // port colon, not a `::` inside the address.
                if host.contains(':') {
                    Ok(format!("[{host}]:{port}"))
                } else {
                    Ok(format!("{host}:{port}"))
                }
            }
        }
    }

    /// Reject scheme-specific fields that don't apply to the chosen `type`.
    /// `deny_unknown_fields` on the struct already catches truly-unknown keys;
    /// this method catches "known key, wrong scheme" — e.g. a serial entry
    /// that also carries `bind = "..."`.
    fn reject_disallowed_fields(&self, name: &str, scheme: Scheme) -> Result<(), Error> {
        let allowed: &[&str] = match scheme {
            Scheme::Serial => &["path", "baud", "flow_control"],
            Scheme::UdpServer => &["bind", "idle_secs"],
            Scheme::TcpServer => &["bind"],
            Scheme::UdpClient => &["host", "port", "latch_idle_secs"],
            Scheme::TcpClient => &["host", "port"],
        };

        let provided: [(&str, bool); 8] = [
            ("path", self.path.is_some()),
            ("baud", self.baud.is_some()),
            ("flow_control", self.flow_control.is_some()),
            ("bind", self.bind.is_some()),
            ("host", self.host.is_some()),
            ("port", self.port.is_some()),
            ("idle_secs", self.idle_secs.is_some()),
            ("latch_idle_secs", self.latch_idle_secs.is_some()),
        ];

        for (field, present) in provided {
            if present && !allowed.contains(&field) {
                let allowed_list = allowed.join(", ");
                return Err(Error::ConfigSchema {
                    name: name.to_string(),
                    reason: format!(
                        "field '{field}' is not valid for type '{scheme}' (allowed fields: {allowed_list})"
                    ),
                });
            }
        }
        Ok(())
    }

    fn collect_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        if let Some(value) = self.flow_control.as_ref() {
            pairs.push(("flow_control".to_string(), value.clone()));
        }
        if let Some(value) = self.idle_secs {
            pairs.push(("idle_secs".to_string(), value.to_string()));
        }
        if let Some(value) = self.latch_idle_secs {
            pairs.push(("latch_idle_secs".to_string(), value.to_string()));
        }
        if let Some(value) = self.sniffer {
            pairs.push(("sniffer".to_string(), value.to_string()));
        }
        if let Some(value) = self.group.as_ref() {
            pairs.push(("group".to_string(), value.clone()));
        }
        // Field name IS the query key, so `stringify!` keeps the two in lockstep
        // (same pattern as `filters.rs::Filters::apply`).
        macro_rules! push_filter_pairs {
            ($($field:ident),* $(,)?) => {
                $(
                    if let Some(value) = self.$field.as_deref() {
                        pairs.push((stringify!($field).to_string(), value.to_string()));
                    }
                )*
            };
        }
        push_filter_pairs! {
            allow_msgid_in,
            block_msgid_in,
            allow_msgid_out,
            block_msgid_out,
            allow_src_sys_in,
            block_src_sys_in,
            allow_src_sys_out,
            block_src_sys_out,
            allow_src_comp_in,
            block_src_comp_in,
            allow_src_comp_out,
            block_src_comp_out,
            allow_src_endpoint_out,
            block_src_endpoint_out,
        }
        pairs
    }
}

fn missing(name: &str, scheme: Scheme, field: &'static str) -> Error {
    Error::ConfigSchema {
        name: name.to_string(),
        reason: format!("type '{scheme}' requires field '{field}'"),
    }
}

/// Translate the TOML `type = "..."` literal into a typed [`Scheme`]. Wraps
/// [`Scheme::try_from_str`] with the `Error::ConfigSchema` shape the TOML
/// path is contracted to produce (pinned by
/// `toml_unknown_endpoint_type_rejected`).
fn scheme_from_toml_type(raw: &str, name: &str) -> Result<Scheme, Error> {
    Scheme::try_from_str(raw).ok_or_else(|| Error::ConfigSchema {
        name: name.to_string(),
        reason: format!("unknown endpoint type '{raw}' (valid: serial, udps, udpc, tcps, tcpc)"),
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::endpoint::filters::Filters;
    use crate::endpoint::spec::EndpointKind;

    #[test]
    fn empty_yields_no_endpoints() {
        let cfg = TomlConfig::parse_str("").expect("empty TOML must parse");
        assert!(cfg.endpoints.is_empty());
        assert!(cfg.log_level.is_none());
        assert!(cfg.log_format.is_none());
        assert!(cfg.stats.is_none());
        assert!(cfg.stats_interval_secs.is_none());
        assert!(cfg.dedup_ms.is_none());
    }

    #[test]
    fn globals_only() {
        let text = r#"
log_level = "debug"
log_format = "json"
stats = true
stats_interval_secs = 10
dedup_ms = 200
"#;
        let cfg = TomlConfig::parse_str(text).expect("globals-only TOML must parse");
        assert_eq!(cfg.log_level, Some(LogLevel::Debug));
        assert_eq!(cfg.log_format, Some(LogFormat::Json));
        assert_eq!(cfg.stats, Some(true));
        assert_eq!(cfg.stats_interval_secs, Some(10));
        assert_eq!(cfg.dedup_ms, Some(200));
    }

    #[test]
    fn serial_endpoint() {
        let text = r#"
[endpoint.fc]
type = "serial"
path = "/dev/ttyUSB0"
baud = 921600
flow_control = "rtscts"
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        assert_eq!(cfg.endpoints.len(), 1);
        let spec = &cfg.endpoints[0];
        assert_eq!(spec.name, "fc");
        let ep = match &spec.kind {
            EndpointKind::Serial(endpoint) => endpoint,
            other => panic!("expected serial, got {other:?}"),
        };
        assert_eq!(ep.path, "/dev/ttyUSB0");
        assert_eq!(ep.baud, 921_600);
        assert_eq!(
            ep.flow_control,
            crate::endpoint::spec::SerialFlowControl::RtsCts,
        );
    }

    #[test]
    fn udps_endpoint_with_filters() {
        let text = r#"
[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"
idle_secs = 30
sniffer = false
group = "uplink"
block_msgid_in = "33,100-150"
allow_src_sys_out = "1,5-10"
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        let spec = &cfg.endpoints[0];
        assert_eq!(spec.name, "bus");
        let ep = match &spec.kind {
            EndpointKind::UdpServer(endpoint) => endpoint,
            other => panic!("expected udps, got {other:?}"),
        };
        assert_eq!(ep.bind_addr.to_string(), "0.0.0.0:14550");
        assert_eq!(ep.idle_secs, Some(30));
        assert_eq!(ep.identity.group.as_deref(), Some("uplink"));
        assert!(!ep.identity.sniffer);
        assert_eq!(ep.identity.filters.block_msgid_in.len(), 2);
        assert_eq!(ep.identity.filters.allow_src_sys_out.len(), 2);
    }

    #[test]
    fn tcpc_endpoint_with_src_endpoint_out_filter() {
        let text = r#"
[endpoint.radio]
type = "tcpc"
host = "radio.local"
port = 5760
block_src_endpoint_out = "local_service,fc"
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        let spec = &cfg.endpoints[0];
        assert_eq!(spec.name, "radio");
        let ep = match &spec.kind {
            EndpointKind::TcpClient(endpoint) => endpoint,
            other => panic!("expected tcpc, got {other:?}"),
        };
        let names: Vec<&str> = ep
            .identity
            .filters
            .block_src_endpoint_out
            .iter()
            .map(|entry| entry.as_ref())
            .collect();
        assert_eq!(names, vec!["local_service", "fc"]);
    }

    #[test]
    fn toml_array_form_for_src_endpoint_out_rejected() {
        // Filter lists are a comma-separated string; array form must error.
        let text = r#"
[endpoint.radio]
type = "tcpc"
host = "h"
port = 1
block_src_endpoint_out = ["a", "b"]
"#;
        assert!(
            matches!(TomlConfig::parse_str(text), Err(Error::ConfigParse(_))),
            "array form must be rejected as a TOML parse error"
        );
    }

    #[test]
    fn toml_invalid_endpoint_name_in_filter_rejected() {
        let text = r#"
[endpoint.radio]
type = "tcpc"
host = "h"
port = 1
block_src_endpoint_out = "bad.name"
"#;
        let err = TomlConfig::parse_str(text).expect_err("must reject invalid name");
        let msg = err.to_string();
        assert!(
            msg.contains("block_src_endpoint_out"),
            "error must name the offending key; got: {msg}"
        );
    }

    #[test]
    fn udpc_ipv6_host_brackets_synthesized_body() {
        let text = r#"
[endpoint.tap]
type = "udpc"
host = "::1"
port = 14550
latch_idle_secs = 15
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            EndpointKind::UdpClient(endpoint) => endpoint,
            other => panic!("expected udpc, got {other:?}"),
        };
        assert_eq!(ep.host, "::1");
        assert_eq!(ep.port, 14_550);
        assert_eq!(ep.latch_idle_secs, Some(15));
    }

    #[test]
    fn tcpc_hostname() {
        let text = r#"
[endpoint.vehicle]
type = "tcpc"
host = "gcs.local"
port = 5760
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            EndpointKind::TcpClient(endpoint) => endpoint,
            other => panic!("expected tcpc, got {other:?}"),
        };
        assert_eq!(ep.host, "gcs.local");
        assert_eq!(ep.port, 5760);
        assert_eq!(cfg.endpoints[0].name, "vehicle");
    }

    #[test]
    fn tcps_endpoint() {
        let text = r#"
[endpoint.uplink]
type = "tcps"
bind = "[::]:5760"
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            EndpointKind::TcpServer(endpoint) => endpoint,
            other => panic!("expected tcps, got {other:?}"),
        };
        assert_eq!(ep.bind_addr.to_string(), "[::]:5760");
    }

    #[test]
    fn unknown_top_level_key_rejected() {
        match TomlConfig::parse_str("nonsense = true\n") {
            Err(Error::ConfigParse(_)) => {}
            other => panic!("expected ConfigParse, got {other:?}"),
        }
    }

    #[test]
    fn unknown_endpoint_field_rejected() {
        let text = r#"
[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"
totally_made_up = 1
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigParse(_)) => {}
            other => panic!("expected ConfigParse, got {other:?}"),
        }
    }

    /// Wrong-scheme field rejection matrix. Each case sets exactly one
    /// scheme-specific field on the wrong `type`; per the locked
    /// `disallowed_field_takes_priority_over_missing_field` decision the
    /// `reject_disallowed_fields` check runs before `synthesize_body`, so
    /// none of the cases need to also provide the scheme's required body
    /// fields (the existing `disallowed_field_takes_priority_over_missing_field`
    /// test pins that ordering separately). Spans every `(scheme, field)`
    /// pair the previous hand-rolled tests left uncovered: every disallowed
    /// field is rejected on at least one scheme, and every scheme has at
    /// least one disallowed-field case.
    #[rstest]
    #[case::serial_with_bind("serial", "bind", r#""0.0.0.0:1""#)]
    #[case::serial_with_idle_secs("serial", "idle_secs", "30")]
    #[case::udps_with_host("udps", "host", r#""gcs.local""#)]
    #[case::udps_with_flow_control("udps", "flow_control", r#""rtscts""#)]
    #[case::udps_with_latch_idle_secs("udps", "latch_idle_secs", "30")]
    #[case::tcps_with_host("tcps", "host", r#""gcs.local""#)]
    #[case::tcps_with_idle_secs("tcps", "idle_secs", "30")]
    #[case::udpc_with_bind("udpc", "bind", r#""0.0.0.0:1""#)]
    #[case::udpc_with_idle_secs("udpc", "idle_secs", "30")]
    #[case::tcpc_with_latch_idle_secs("tcpc", "latch_idle_secs", "30")]
    #[case::tcpc_with_flow_control("tcpc", "flow_control", r#""rtscts""#)]
    fn wrong_scheme_field_rejected(
        #[case] type_name: &str,
        #[case] wrong_field: &str,
        #[case] toml_value: &str,
    ) {
        let text = format!("[endpoint.ep]\ntype = \"{type_name}\"\n{wrong_field} = {toml_value}\n");
        match TomlConfig::parse_str(&text) {
            Err(Error::ConfigSchema { name, reason }) => {
                assert_eq!(name, "ep");
                assert!(
                    reason.contains(&format!("'{wrong_field}'")),
                    "reason `{reason}` should quote the offending field `{wrong_field}`"
                );
                assert!(
                    reason.contains(&format!("'{type_name}'")),
                    "reason `{reason}` should quote the scheme `{type_name}`"
                );
            }
            other => panic!("expected ConfigSchema for {type_name}+{wrong_field}, got {other:?}"),
        }
    }

    /// Missing-required-field rejection matrix. Each case provides the
    /// scheme plus all-but-one of its required body fields; the synthesizer
    /// must surface a `ConfigSchema` error naming the missing field. Covers
    /// every `(scheme, required_field)` pair the previous hand-rolled tests
    /// left uncovered (only `serial`-missing-`baud` and `udpc`-missing-`port`
    /// existed; now `udps`/`tcps` missing `bind`, all four `udpc`/`tcpc`
    /// missing combinations, and `serial` missing `path` are pinned).
    #[rstest]
    #[case::serial_no_path("serial", "baud = 115200", "path")]
    #[case::serial_no_baud("serial", r#"path = "/dev/x""#, "baud")]
    #[case::udps_no_bind("udps", "", "bind")]
    #[case::tcps_no_bind("tcps", "", "bind")]
    #[case::udpc_no_host("udpc", "port = 1", "host")]
    #[case::udpc_no_port("udpc", r#"host = "h""#, "port")]
    #[case::tcpc_no_host("tcpc", "port = 1", "host")]
    #[case::tcpc_no_port("tcpc", r#"host = "h""#, "port")]
    fn missing_required_field_rejected(
        #[case] type_name: &str,
        #[case] other_fields: &str,
        #[case] missing_field: &str,
    ) {
        let text = format!("[endpoint.ep]\ntype = \"{type_name}\"\n{other_fields}\n");
        match TomlConfig::parse_str(&text) {
            Err(Error::ConfigSchema { name, reason }) => {
                assert_eq!(name, "ep");
                assert!(
                    reason.contains(&format!("'{missing_field}'")),
                    "reason `{reason}` should quote the missing field `{missing_field}`"
                );
            }
            other => panic!(
                "expected ConfigSchema for {type_name} missing {missing_field}, got {other:?}"
            ),
        }
    }

    #[test]
    fn unknown_endpoint_type_rejected() {
        let text = r#"
[endpoint.mystery]
type = "carrier_pigeon"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigSchema { reason, .. }) => {
                assert!(reason.contains("unknown endpoint type"), "reason: {reason}");
            }
            other => panic!("expected ConfigSchema, got {other:?}"),
        }
    }

    #[test]
    fn filter_array_form_rejected() {
        let text = r#"
[endpoint.vehicle]
type = "tcpc"
host = "gcs.local"
port = 5760
block_msgid_in = [33, 100, 150]
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigParse(_)) => {}
            other => panic!("expected ConfigParse, got {other:?}"),
        }
    }

    #[test]
    fn propagates_spec_errors_for_invalid_query_value() {
        // Wrap `SpecError` in `SpecInToml` so the entry name + index reach
        // the operator.
        let text = r#"
[endpoint.vehicle]
type = "tcpc"
host = "gcs.local"
port = 5760
block_msgid_in = "10-5"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::SpecInToml { name, .. }) => {
                assert_eq!(name, "vehicle");
            }
            other => panic!("expected SpecInToml, got {other:?}"),
        }
    }

    #[test]
    fn table_key_becomes_endpoint_name() {
        let text = r#"
[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        assert_eq!(cfg.endpoints[0].name, "bus");
    }

    #[test]
    fn empty_quoted_endpoint_name_rejected() {
        // `[endpoint.""]` is valid TOML, so the empty name is rejected by the
        // spec validator, not the parser.
        let text = r#"
[endpoint.""]
type = "udps"
bind = "0.0.0.0:14550"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::SpecInToml { name, .. }) => assert_eq!(name, ""),
            other => panic!("expected SpecInToml for empty name, got {other:?}"),
        }
    }

    #[test]
    fn empty_bare_endpoint_name_rejected() {
        // `[endpoint.]` is a TOML syntax error (empty bare key), caught by the
        // parser before any spec validation.
        let text = r#"
[endpoint.]
type = "udps"
bind = "0.0.0.0:14550"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigParse(_)) => {}
            other => panic!("expected ConfigParse for `[endpoint.]`, got {other:?}"),
        }
    }

    #[test]
    fn name_field_inside_table_rejected() {
        // `name` is the table key now, so carrying it as a field is an unknown
        // key and must fail loudly.
        let text = r#"
[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"
name = "other"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigParse(_)) => {}
            other => panic!("expected ConfigParse, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_endpoint_name_rejected_at_parse() {
        // Duplicate table keys are a TOML parse error, so name uniqueness is
        // enforced by the format itself.
        let text = r#"
[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"

[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14551"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigParse(err)) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("bus"),
                    "error must name the duplicated key; got: {msg}"
                );
            }
            other => panic!("expected ConfigParse from duplicate key, got {other:?}"),
        }
    }

    #[test]
    fn from_path_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("rmr.toml");
        std::fs::write(
            &path,
            r#"
stats = true
[endpoint.vehicle]
type = "tcpc"
host = "gcs.local"
port = 5760
"#,
        )
        .expect("write toml");
        let cfg = TomlConfig::from_path(&path).expect("must parse");
        assert_eq!(cfg.stats, Some(true));
        assert_eq!(cfg.endpoints.len(), 1);
    }

    #[test]
    fn from_path_io_error() {
        let missing = std::path::PathBuf::from("/nonexistent/path/rmr.toml");
        match TomlConfig::from_path(&missing) {
            Err(Error::ConfigIo { path, .. }) => assert!(path.contains("nonexistent")),
            other => panic!("expected ConfigIo, got {other:?}"),
        }
    }

    #[test]
    fn invalid_explicit_name_rejected() {
        // A quoted key is valid TOML, so an invalid name reaches the spec
        // validator, not the parser.
        let text = r#"
[endpoint."has spaces"]
type = "udps"
bind = "0.0.0.0:14550"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::SpecInToml { name, .. }) => {
                assert_eq!(name, "has spaces");
            }
            other => panic!("expected SpecInToml (invalid name), got {other:?}"),
        }
    }

    #[test]
    fn serial_path_containing_colon() {
        // `/dev/serial/by-id/...` symlinks carry `:` in the path; the body
        // parser uses `rfind(':')` so only the baud-prefix colon splits.
        let text = r#"
[endpoint.fc]
type = "serial"
path = "/dev/serial/by-id/usb-FTDI:port0"
baud = 57600
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            EndpointKind::Serial(endpoint) => endpoint,
            other => panic!("expected serial, got {other:?}"),
        };
        assert_eq!(ep.path, "/dev/serial/by-id/usb-FTDI:port0");
        assert_eq!(ep.baud, 57_600);
    }

    #[test]
    fn unknown_endpoint_field_error_names_the_field() {
        // Operators grep error text to find their typo — the offending
        // key must appear at least once.
        let text = r#"
[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"
totally_made_up = 1
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigParse(err)) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("totally_made_up"),
                    "error message must name the offending field; got: {msg}"
                );
            }
            other => panic!("expected ConfigParse, got {other:?}"),
        }
    }

    #[test]
    fn multiple_endpoints_preserve_order_and_kind() {
        let text = r#"
[endpoint.fc]
type = "serial"
path = "/dev/ttyUSB0"
baud = 921600

[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"

[endpoint.tap]
type = "udpc"
host = "192.168.1.5"
port = 14550

[endpoint.uplink]
type = "tcps"
bind = "0.0.0.0:5760"

[endpoint.vehicle]
type = "tcpc"
host = "gcs.local"
port = 5760
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        assert_eq!(cfg.endpoints.len(), 5);
        let observed: Vec<(&str, &str)> = cfg
            .endpoints
            .iter()
            .map(|spec| {
                let kind = match &spec.kind {
                    EndpointKind::Serial(_) => "serial",
                    EndpointKind::UdpServer(_) => "udps",
                    EndpointKind::UdpClient(_) => "udpc",
                    EndpointKind::TcpServer(_) => "tcps",
                    EndpointKind::TcpClient(_) => "tcpc",
                };
                (spec.name.as_str(), kind)
            })
            .collect();
        assert_eq!(
            observed,
            vec![
                ("fc", "serial"),
                ("bus", "udps"),
                ("tap", "udpc"),
                ("uplink", "tcps"),
                ("vehicle", "tcpc"),
            ],
        );
    }

    #[test]
    fn duplicate_key_within_endpoint_rejected() {
        // Pin the parse-error behaviour so a future swap to a lenient
        // reader can't silently accept "last write wins".
        let text = r#"
[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"
bind = "0.0.0.0:14551"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigParse(err)) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("duplicate key"),
                    "error must mention the duplicate-key cause; got: {msg}"
                );
                assert!(
                    msg.contains("bind"),
                    "error must name the offending key; got: {msg}"
                );
            }
            other => panic!("expected ConfigParse from duplicate key, got {other:?}"),
        }
    }

    fn filter_axis_count(filters: &Filters, axis: &str) -> usize {
        match axis {
            "allow_msgid_in" => filters.allow_msgid_in.len(),
            "block_msgid_in" => filters.block_msgid_in.len(),
            "allow_msgid_out" => filters.allow_msgid_out.len(),
            "block_msgid_out" => filters.block_msgid_out.len(),
            "allow_src_sys_in" => filters.allow_src_sys_in.len(),
            "block_src_sys_in" => filters.block_src_sys_in.len(),
            "allow_src_sys_out" => filters.allow_src_sys_out.len(),
            "block_src_sys_out" => filters.block_src_sys_out.len(),
            "allow_src_comp_in" => filters.allow_src_comp_in.len(),
            "block_src_comp_in" => filters.block_src_comp_in.len(),
            "allow_src_comp_out" => filters.allow_src_comp_out.len(),
            "block_src_comp_out" => filters.block_src_comp_out.len(),
            "allow_src_endpoint_out" => filters.allow_src_endpoint_out.len(),
            "block_src_endpoint_out" => filters.block_src_endpoint_out.len(),
            other => panic!("unknown filter axis: {other}"),
        }
    }

    /// Every filter knob declared on `TomlEndpoint` must round-trip into the
    /// matching `Filters` axis: the previous tests only exercised 2 of the
    /// 12 axes (`block_msgid_in`, `allow_src_sys_out`) inside
    /// `udps_endpoint_with_filters`, so a regression in `collect_pairs` that
    /// dropped (or swapped) any of the other 10 would have gone unnoticed.
    /// Each case sets exactly one axis with a 2-range string (`"1,5-10"`,
    /// valid for both `MsgIdRange` and `U8Range`) and asserts the resulting
    /// axis Vec is length 2 — proves the knob lands on the right axis, not
    /// merely "somewhere".
    #[rstest]
    #[case("allow_msgid_in")]
    #[case("block_msgid_in")]
    #[case("allow_msgid_out")]
    #[case("block_msgid_out")]
    #[case("allow_src_sys_in")]
    #[case("block_src_sys_in")]
    #[case("allow_src_sys_out")]
    #[case("block_src_sys_out")]
    #[case("allow_src_comp_in")]
    #[case("block_src_comp_in")]
    #[case("allow_src_comp_out")]
    #[case("block_src_comp_out")]
    #[case("allow_src_endpoint_out")]
    #[case("block_src_endpoint_out")]
    fn each_filter_knob_round_trips(#[case] axis: &str) {
        let text = format!(
            "[endpoint.ep]\ntype = \"tcpc\"\nhost = \"h\"\nport = 1\n{axis} = \"1,5-10\"\n"
        );
        let cfg = TomlConfig::parse_str(&text).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            EndpointKind::TcpClient(endpoint) => endpoint,
            other => panic!("expected tcpc, got {other:?}"),
        };
        assert_eq!(
            filter_axis_count(&ep.identity.filters, axis),
            2,
            "filter knob `{axis}` did not land on its named axis"
        );
    }

    #[test]
    fn explicit_stats_false_distinguishable_from_unset() {
        // `Some(false)` and `None` flow through merge differently — the
        // explicit `false` must survive parsing.
        let cfg = TomlConfig::parse_str("stats = false\n").expect("must parse");
        assert_eq!(cfg.stats, Some(false));
    }

    #[test]
    fn sniffer_true_propagates_to_identity() {
        let text = r#"
[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"
sniffer = true
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            EndpointKind::UdpServer(endpoint) => endpoint,
            other => panic!("expected udps, got {other:?}"),
        };
        assert!(ep.identity.sniffer);
    }

    #[test]
    fn disallowed_field_takes_priority_over_missing_field() {
        // Wrong-field-for-scheme must surface over downstream
        // missing-required-field — keeps the operator's real mistake first.
        let text = r#"
[endpoint.fc]
type = "serial"
bind = "0.0.0.0:14550"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigSchema { reason, .. }) => {
                assert!(reason.contains("'bind'"), "reason: {reason}");
                assert!(reason.contains("'serial'"), "reason: {reason}");
            }
            other => panic!("expected ConfigSchema mentioning 'bind' first, got {other:?}"),
        }
    }

    #[test]
    fn config_schema_error_carries_endpoint_name() {
        let text = r#"
[endpoint.fleet-bus]
type = "udps"
bind = "0.0.0.0:14550"
host = "192.168.1.1"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::ConfigSchema { name, .. }) => {
                assert_eq!(name, "fleet-bus");
            }
            other => panic!("expected ConfigSchema, got {other:?}"),
        }
    }

    #[test]
    fn error_message_quotes_endpoint_table_and_name() {
        let text = r#"
[endpoint.bus]
type = "udps"
bind = "0.0.0.0:14550"

[endpoint.fleet-bus]
type = "udps"
host = "drone.local"
"#;
        let err = TomlConfig::parse_str(text).expect_err("must fail");
        let rendered = err.to_string();
        assert!(
            rendered.contains("[endpoint.fleet-bus]"),
            "expected the offending table locator: {rendered}"
        );
        assert!(
            rendered.contains("allowed fields:"),
            "expected allowed-fields hint: {rendered}"
        );
    }
}
