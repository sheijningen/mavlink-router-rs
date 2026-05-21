//! TOML deserialisation.
//!
//! Reads a TOML file off disk (or a string fixture in tests) and produces a
//! [`TomlConfig`] — the TOML-side view of the world, where every global is
//! `Option<T>` so the merge step in [`crate::config`] can tell "operator
//! omitted this key" from "operator wrote the default value".
//!
//! Round-tripping a `[[endpoints]]` table into an [`EndpointSpec`] happens
//! here too: the flat serde struct ([`TomlEndpoint`]) is rejected on
//! scheme-mismatched fields, then its address fields and query knobs are
//! repacked into the `(scheme, body, name, pairs)` 4-tuple that
//! [`EndpointSpec::build`] consumes. No CLI logic crosses this module.
//!
//! [`EndpointSpec`]: crate::endpoint::spec::EndpointSpec

use std::path::Path;

use serde::Deserialize;

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

    /// Parse a TOML string into a [`TomlConfig`]. The schema is documented in
    /// [`TomlFile`] / [`TomlEndpoint`] — every TOML key is a strict match
    /// against those serde structs (`deny_unknown_fields`); per-endpoint
    /// typed fields are validated against the chosen `type` (e.g. a `serial`
    /// entry must carry `path`/`baud` and must not carry `bind`/`host`/`port`).
    /// Filter knobs are accepted in the string form only
    /// (`block_msgid_in = "33,100-150"`), per CLAUDE.md.
    ///
    /// Named `parse_str` (not `from_str`) so it doesn't shadow the
    /// `std::str::FromStr` trait method; `FromStr` doesn't fit because the
    /// returned [`Error`] is wider than what the trait's idiomatic
    /// `FromStr::Err` shape allows.
    pub fn parse_str(text: &str) -> Result<Self, Error> {
        let file: TomlFile = toml::from_str(text).map_err(Error::ConfigParse)?;
        file.into_toml_config()
    }
}

/// On-disk TOML schema. Mirrors the global-knob surface and accepts an array
/// of [`TomlEndpoint`]. `deny_unknown_fields` so a typo'd top-level key fails
/// loudly rather than silently being ignored.
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
    endpoints: Vec<TomlEndpoint>,
}

impl TomlFile {
    fn into_toml_config(self) -> Result<TomlConfig, Error> {
        let mut endpoints = Vec::with_capacity(self.endpoints.len());
        for (idx, entry) in self.endpoints.into_iter().enumerate() {
            endpoints.push(entry.into_spec(idx)?);
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

/// One `[[endpoints]]` table entry. Flat shape (rather than an internally-
/// tagged enum variant) because serde's `deny_unknown_fields` does not compose
/// with `#[serde(tag = ...)]`, and we want a single typo'd field name to fail
/// loudly. Scheme-conditional validation (e.g. `serial` requires `path`/`baud`
/// and must not carry `bind`/`host`/`port`) runs post-parse in
/// [`TomlEndpoint::into_spec`].
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlEndpoint {
    #[serde(rename = "type")]
    scheme: String,
    name: Option<String>,
    // scheme-specific (only the subset for the chosen scheme is allowed)
    path: Option<String>,
    baud: Option<u32>,
    flow_control: Option<String>,
    bind: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    idle_secs: Option<u64>,
    latch_idle_secs: Option<u64>,
    // identity
    sniffer: Option<bool>,
    group: Option<String>,
    // filters (string form per CLAUDE.md — array forms are rejected at parse time)
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
}

impl TomlEndpoint {
    fn into_spec(self, index: usize) -> Result<EndpointSpec, Error> {
        let scheme = scheme_from_toml_type(&self.scheme, index)?;

        // Reject wrong-scheme fields *before* synthesising the body so that an
        // entry carrying both `path` and `bind` surfaces "field 'bind' is not
        // valid for type 'serial'" — the operator's real mistake — rather than
        // the body-synthesizer's downstream missing-`baud` complaint.
        self.reject_disallowed_fields(index, scheme)?;
        let body = self.synthesize_body(index, scheme)?;
        let pairs = self.collect_pairs();
        let name_opt = self.name.as_deref();
        EndpointSpec::build(scheme, &body, name_opt, &pairs).map_err(Error::from)
    }

    fn synthesize_body(&self, index: usize, scheme: Scheme) -> Result<String, Error> {
        match scheme {
            Scheme::Serial => {
                let path = self
                    .path
                    .as_ref()
                    .ok_or_else(|| missing(index, scheme, "path"))?;
                let baud = self.baud.ok_or_else(|| missing(index, scheme, "baud"))?;
                Ok(format!("{path}:{baud}"))
            }
            Scheme::UdpServer | Scheme::TcpServer => {
                let bind = self
                    .bind
                    .as_ref()
                    .ok_or_else(|| missing(index, scheme, "bind"))?;
                Ok(bind.clone())
            }
            Scheme::UdpClient | Scheme::TcpClient => {
                let host = self
                    .host
                    .as_ref()
                    .ok_or_else(|| missing(index, scheme, "host"))?;
                let port = self.port.ok_or_else(|| missing(index, scheme, "port"))?;
                // IPv6 literal hosts must be bracketed in the body grammar so
                // the rsplit(':') boundary lands at the port colon, not the
                // last `::` inside the address. Bracketing here is harmless for
                // IPv4 hostnames (the body parser strips brackets only when
                // present).
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
    fn reject_disallowed_fields(&self, index: usize, scheme: Scheme) -> Result<(), Error> {
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
                return Err(Error::ConfigSchema {
                    index,
                    reason: format!("field '{field}' is not valid for type '{scheme}'"),
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
        push_str_pair(&mut pairs, "allow_msgid_in", self.allow_msgid_in.as_deref());
        push_str_pair(&mut pairs, "block_msgid_in", self.block_msgid_in.as_deref());
        push_str_pair(
            &mut pairs,
            "allow_msgid_out",
            self.allow_msgid_out.as_deref(),
        );
        push_str_pair(
            &mut pairs,
            "block_msgid_out",
            self.block_msgid_out.as_deref(),
        );
        push_str_pair(
            &mut pairs,
            "allow_src_sys_in",
            self.allow_src_sys_in.as_deref(),
        );
        push_str_pair(
            &mut pairs,
            "block_src_sys_in",
            self.block_src_sys_in.as_deref(),
        );
        push_str_pair(
            &mut pairs,
            "allow_src_sys_out",
            self.allow_src_sys_out.as_deref(),
        );
        push_str_pair(
            &mut pairs,
            "block_src_sys_out",
            self.block_src_sys_out.as_deref(),
        );
        push_str_pair(
            &mut pairs,
            "allow_src_comp_in",
            self.allow_src_comp_in.as_deref(),
        );
        push_str_pair(
            &mut pairs,
            "block_src_comp_in",
            self.block_src_comp_in.as_deref(),
        );
        push_str_pair(
            &mut pairs,
            "allow_src_comp_out",
            self.allow_src_comp_out.as_deref(),
        );
        push_str_pair(
            &mut pairs,
            "block_src_comp_out",
            self.block_src_comp_out.as_deref(),
        );
        pairs
    }
}

fn push_str_pair(pairs: &mut Vec<(String, String)>, key: &str, val: Option<&str>) {
    if let Some(value) = val {
        pairs.push((key.to_string(), value.to_string()));
    }
}

fn missing(index: usize, scheme: Scheme, field: &'static str) -> Error {
    Error::ConfigSchema {
        index,
        reason: format!("type '{scheme}' requires field '{field}'"),
    }
}

/// Translate the TOML `type = "..."` literal into a typed [`Scheme`]. Wraps
/// [`Scheme::try_from_str`] with the `Error::ConfigSchema` shape the TOML
/// path is contracted to produce (pinned by
/// `toml_unknown_endpoint_type_rejected`).
fn scheme_from_toml_type(raw: &str, index: usize) -> Result<Scheme, Error> {
    Scheme::try_from_str(raw).ok_or_else(|| Error::ConfigSchema {
        index,
        reason: format!("unknown endpoint type '{raw}' (valid: serial, udps, udpc, tcps, tcpc)"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::filters::Filters;
    use crate::endpoint::spec::EndpointKind;
    use rstest::rstest;

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
[[endpoints]]
type = "serial"
path = "/dev/ttyUSB0"
baud = 921600
flow_control = "rtscts"
name = "fc"
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
[[endpoints]]
type = "udps"
bind = "0.0.0.0:14550"
name = "bus"
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
    fn udpc_ipv6_host_brackets_synthesized_body() {
        let text = r#"
[[endpoints]]
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
[[endpoints]]
type = "tcpc"
host = "gcs.local"
port = 5760
name = "vehicle"
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
[[endpoints]]
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
[[endpoints]]
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
        let text = format!("[[endpoints]]\ntype = \"{type_name}\"\n{wrong_field} = {toml_value}\n");
        match TomlConfig::parse_str(&text) {
            Err(Error::ConfigSchema { index, reason }) => {
                assert_eq!(index, 0);
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
        let text = format!("[[endpoints]]\ntype = \"{type_name}\"\n{other_fields}\n");
        match TomlConfig::parse_str(&text) {
            Err(Error::ConfigSchema { index, reason }) => {
                assert_eq!(index, 0);
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
[[endpoints]]
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
        // CLAUDE.md: "TOML accepts the string form only — array forms are a
        // parse-time error." Our serde schema types filter fields as
        // Option<String>, so an inline array fails at the toml-deserialize
        // layer.
        let text = r#"
[[endpoints]]
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
        // Invalid range (lo > hi) is caught by the existing filter parser,
        // which we reuse via EndpointSpec::build — so an out-of-shape filter
        // surfaces as a SpecError, not a ConfigSchema.
        let text = r#"
[[endpoints]]
type = "tcpc"
host = "gcs.local"
port = 5760
block_msgid_in = "10-5"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::Spec(_)) => {}
            other => panic!("expected Spec, got {other:?}"),
        }
    }

    #[test]
    fn auto_names_when_omitted() {
        let text = r#"
[[endpoints]]
type = "udps"
bind = "0.0.0.0:14550"
"#;
        let cfg = TomlConfig::parse_str(text).expect("must parse");
        assert_eq!(cfg.endpoints[0].name, "udps-0_0_0_0-14550");
    }

    #[test]
    fn from_path_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("rmr.toml");
        std::fs::write(
            &path,
            r#"
stats = true
[[endpoints]]
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
        // CLAUDE.md "`#name` validation": names outside [A-Za-z0-9_-]{1,64}
        // are fatal. TOML routes through `EndpointSpec::build` which calls
        // `validate_name`, so we should see a SpecError from there.
        let text = r#"
[[endpoints]]
type = "udps"
bind = "0.0.0.0:14550"
name = "has spaces"
"#;
        match TomlConfig::parse_str(text) {
            Err(Error::Spec(_)) => {}
            other => panic!("expected Spec (invalid name), got {other:?}"),
        }
    }

    #[test]
    fn serial_path_containing_colon() {
        // Real-world `/dev/serial/by-id/...` symlinks can carry `:` in the
        // path. The body parser uses `rfind(':')` so the last colon (before
        // the baud) wins, leaving the path intact.
        let text = r#"
[[endpoints]]
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
        // The unknown-field error text is part of the contract: operators
        // grep it to find their typo. Pin that the offending key appears in
        // the message at least once.
        let text = r#"
[[endpoints]]
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
        // Mixed-scheme TOML with several correctly-formed `[[endpoints]]`
        // entries: every entry must round-trip into the matching
        // `EndpointKind`, and the resulting `Vec<EndpointSpec>` must preserve
        // declaration order (the spawner relies on it for stable
        // `EndpointId` allocation and stats ordering).
        let text = r#"
[[endpoints]]
type = "serial"
path = "/dev/ttyUSB0"
baud = 921600
name = "fc"

[[endpoints]]
type = "udps"
bind = "0.0.0.0:14550"
name = "bus"

[[endpoints]]
type = "udpc"
host = "192.168.1.5"
port = 14550
name = "tap"

[[endpoints]]
type = "tcps"
bind = "0.0.0.0:5760"
name = "uplink"

[[endpoints]]
type = "tcpc"
host = "gcs.local"
port = 5760
name = "vehicle"
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
        // TOML disallows the same key appearing twice in the same table;
        // serde's deserializer surfaces this as a parse error before our
        // schema validation runs. Pin the behaviour so a future move to a
        // lenient TOML reader (or a swap to `toml-edit`) can't silently
        // accept "last write wins" semantics and let an operator's
        // copy-paste typo route to an unintended bind address.
        let text = r#"
[[endpoints]]
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
    fn each_filter_knob_round_trips(#[case] axis: &str) {
        let text = format!(
            "[[endpoints]]\ntype = \"tcpc\"\nhost = \"h\"\nport = 1\n{axis} = \"1,5-10\"\n"
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
        // CLAUDE.md merge: `Some(false)` falls through to CLI overrides /
        // defaults differently than `None`. Pin that an explicit
        // `stats = false` in TOML survives parsing as `Some(false)` so the
        // merge step can act on the operator's choice.
        let cfg = TomlConfig::parse_str("stats = false\n").expect("must parse");
        assert_eq!(cfg.stats, Some(false));
    }

    #[test]
    fn sniffer_true_propagates_to_identity() {
        // `udps_endpoint_with_filters` covers `sniffer = false`; without a
        // matching `sniffer = true` test, swapping the bool inside
        // `collect_pairs` (or `IdentityFlags::apply`) would only break one
        // value and pass the existing assertions.
        let text = r#"
[[endpoints]]
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
        // Regression guard for the validation-ordering decision (see
        // `into_spec`): a serial entry carrying `bind` but lacking `path`
        // must surface the wrong-field-for-scheme error, not the
        // downstream missing-path error.
        let text = r#"
[[endpoints]]
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
}
