//! Canonical input to [`crate::run`]: globals + the fully-typed endpoint
//! table that the spawner consumes. Both [`crate::cli::Cli`] and the TOML
//! parser feed into this struct — `Cli` is one input format among several,
//! not the authoritative shape.
//!
//! CLAUDE.md "Project layout" → `config.rs`: TOML schema (serde) plus the
//! eventual CLI+file merge rules. The merge rules ("TOML endpoints first then
//! CLI appended; CLI globals override TOML globals") are deferred to a later
//! Phase 6 commit; today, `--config <FILE>` and CLI endpoints are
//! mutually-exclusive — mixing them is fatal at config-resolve time.

use std::collections::HashSet;
use std::path::Path;

use serde::Deserialize;
use tracing::Level;

use crate::endpoint::spec::EndpointSpec;
use crate::error::Error;

/// CLAUDE.md "Defaults" → `stats_interval_secs` (period of stats JSON-Lines
/// output).
pub const DEFAULT_STATS_INTERVAL_SECS: u64 = 5;

/// CLAUDE.md "Defaults" → `shutdown_grace_secs` (overall wall-clock shutdown
/// budget). Per-task drain is bounded at 2 s inside this envelope.
pub const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 5;

/// CLAUDE.md "Defaults" → `dedup_ms` default (0 = dedup window disabled).
pub const DEFAULT_DEDUP_MS: u64 = 0;

/// Log verbosity, mirroring the CLAUDE.md "Global opts" enumeration. Lives in
/// the config module so TOML and CLI parsers see the same type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

/// Log output format: human-readable text (default) or structured JSON via
/// `tracing_subscriber::fmt().json()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

impl From<LogLevel> for Level {
    fn from(l: LogLevel) -> Self {
        match l {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

/// Canonical, fully-resolved runtime configuration consumed by [`crate::run`]
/// and [`crate::run_with_cancel`]. Anything that builds an `rmr` invocation
/// — `Cli`, the TOML parser, integration tests — produces a `Config`
/// and hands it to `run`.
#[derive(Debug, Clone)]
pub struct Config {
    pub log_level: LogLevel,
    pub log_format: LogFormat,
    pub stats: bool,
    pub stats_interval: u64,
    pub dedup_ms: u64,
    pub shutdown_grace: u64,
    pub endpoints: Vec<EndpointSpec>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_level: LogLevel::default(),
            log_format: LogFormat::default(),
            stats: false,
            stats_interval: DEFAULT_STATS_INTERVAL_SECS,
            dedup_ms: DEFAULT_DEDUP_MS,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE_SECS,
            endpoints: Vec::new(),
        }
    }
}

impl Config {
    /// Parse a TOML config file from disk and validate cross-endpoint
    /// invariants. See [`Config::from_toml_str`] for the schema.
    pub fn from_toml_path(path: &Path) -> Result<Self, Error> {
        let raw = std::fs::read_to_string(path).map_err(|e| Error::ConfigIo {
            path: path.display().to_string(),
            source: e,
        })?;
        Self::from_toml_str(&raw)
    }

    /// Parse a TOML config string into a [`Config`]. The schema is documented
    /// in [`ConfigFile`] / [`EndpointEntry`] — every TOML key is a strict
    /// match against those serde structs (`deny_unknown_fields`); the
    /// per-endpoint typed fields are validated against the chosen `type`
    /// (e.g. a `serial`-typed entry must carry `path`/`baud` and must not
    /// carry `bind`/`host`/`port`). Filter knobs are accepted in the
    /// string form only (`block_msgid_in = "33,100-150"`), per CLAUDE.md.
    pub fn from_toml_str(s: &str) -> Result<Self, Error> {
        let file: ConfigFile = toml::from_str(s).map_err(Error::ConfigParse)?;
        let cfg = file.into_config()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-endpoint validation: duplicate `#name` (explicit or auto-derived)
    /// across the combined endpoint table is fatal per CLAUDE.md "CLI + TOML
    /// merge" rules. The per-endpoint surface (filter ranges, address
    /// grammar, query keys) is already enforced by [`EndpointSpec::parse`];
    /// this method only catches what spans more than one entry.
    pub fn validate(&self) -> Result<(), Error> {
        let mut seen = HashSet::<&str>::new();
        for ep in &self.endpoints {
            if !seen.insert(ep.name.as_str()) {
                return Err(Error::DuplicateName(ep.name.clone()));
            }
        }
        Ok(())
    }
}

/// On-disk TOML schema, deserialised by serde. Mirrors [`Config`]'s globals
/// plus an array of [`EndpointEntry`]. `deny_unknown_fields` so a typo'd top-
/// level key fails loudly rather than silently being ignored.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    #[serde(default)]
    log_level: Option<LogLevel>,
    #[serde(default)]
    log_format: Option<LogFormat>,
    #[serde(default)]
    stats: Option<bool>,
    #[serde(default)]
    stats_interval: Option<u64>,
    #[serde(default)]
    dedup_ms: Option<u64>,
    #[serde(default)]
    shutdown_grace: Option<u64>,
    #[serde(default)]
    endpoints: Vec<EndpointEntry>,
}

impl ConfigFile {
    fn into_config(self) -> Result<Config, Error> {
        let mut endpoints = Vec::with_capacity(self.endpoints.len());
        for (idx, entry) in self.endpoints.into_iter().enumerate() {
            endpoints.push(entry.into_spec(idx)?);
        }
        let mut cfg = Config::default();
        if let Some(v) = self.log_level {
            cfg.log_level = v;
        }
        if let Some(v) = self.log_format {
            cfg.log_format = v;
        }
        if let Some(v) = self.stats {
            cfg.stats = v;
        }
        if let Some(v) = self.stats_interval {
            cfg.stats_interval = v;
        }
        if let Some(v) = self.dedup_ms {
            cfg.dedup_ms = v;
        }
        if let Some(v) = self.shutdown_grace {
            cfg.shutdown_grace = v;
        }
        cfg.endpoints = endpoints;
        Ok(cfg)
    }
}

/// One `[[endpoints]]` table entry. Flat shape (rather than an internally-
/// tagged enum variant) because serde's `deny_unknown_fields` does not
/// compose with `#[serde(tag = ...)]`, and we want a single typo'd field name
/// to fail loudly. Scheme-conditional validation (e.g. `serial` requires
/// `path`/`baud`, must not carry `bind`/`host`/`port`) runs post-parse in
/// [`EndpointEntry::into_spec`].
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointEntry {
    #[serde(rename = "type")]
    scheme: String,
    name: Option<String>,
    // common
    tx_queue_frames: Option<usize>,
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

impl EndpointEntry {
    fn into_spec(self, index: usize) -> Result<EndpointSpec, Error> {
        let scheme_static: &'static str = match self.scheme.as_str() {
            "serial" => "serial",
            "udps" => "udps",
            "udpc" => "udpc",
            "tcps" => "tcps",
            "tcpc" => "tcpc",
            other => {
                return Err(Error::ConfigSchema {
                    index,
                    reason: format!(
                        "unknown endpoint type '{other}' (valid: serial, udps, udpc, tcps, tcpc)"
                    ),
                });
            }
        };

        // Reject wrong-scheme fields *before* synthesizing the body so that
        // an entry carrying both `path` and `bind` surfaces "field 'bind' is
        // not valid for type 'serial'" — the operator's real mistake — rather
        // than the body-synthesizer's downstream missing-`baud` complaint.
        self.reject_disallowed_fields(index, scheme_static)?;
        let body = self.synthesize_body(index, scheme_static)?;
        let pairs = self.collect_pairs();
        let name_opt = self.name.as_deref();
        EndpointSpec::build(scheme_static, &body, name_opt, &pairs).map_err(Error::from)
    }

    fn synthesize_body(&self, index: usize, scheme: &'static str) -> Result<String, Error> {
        match scheme {
            "serial" => {
                let path = self
                    .path
                    .as_ref()
                    .ok_or_else(|| missing(index, scheme, "path"))?;
                let baud = self.baud.ok_or_else(|| missing(index, scheme, "baud"))?;
                Ok(format!("{path}:{baud}"))
            }
            "udps" | "tcps" => {
                let bind = self
                    .bind
                    .as_ref()
                    .ok_or_else(|| missing(index, scheme, "bind"))?;
                Ok(bind.clone())
            }
            "udpc" | "tcpc" => {
                let host = self
                    .host
                    .as_ref()
                    .ok_or_else(|| missing(index, scheme, "host"))?;
                let port = self.port.ok_or_else(|| missing(index, scheme, "port"))?;
                // IPv6 literal hosts must be bracketed in the body grammar so
                // the rsplit(':') boundary lands at the port colon, not the
                // last `::` inside the address. Bracketing here is harmless
                // for IPv4 hostnames (the body parser strips brackets only
                // when present).
                if host.contains(':') {
                    Ok(format!("[{host}]:{port}"))
                } else {
                    Ok(format!("{host}:{port}"))
                }
            }
            _ => unreachable!("scheme already validated"),
        }
    }

    /// Reject scheme-specific fields that don't apply to the chosen `type`.
    /// `deny_unknown_fields` on the struct already catches truly-unknown
    /// keys; this method catches "known key, wrong scheme" — e.g. a serial
    /// entry that also carries `bind = "..."`.
    fn reject_disallowed_fields(&self, index: usize, scheme: &'static str) -> Result<(), Error> {
        let (allowed_address_fields, allowed_extra): (&[&str], &[&str]) = match scheme {
            "serial" => (&["path", "baud"], &["flow_control"]),
            "udps" => (&["bind"], &["idle_secs"]),
            "tcps" => (&["bind"], &[]),
            "udpc" => (&["host", "port"], &["latch_idle_secs"]),
            "tcpc" => (&["host", "port"], &[]),
            _ => unreachable!(),
        };

        let deny = |present: bool, field: &'static str| -> Result<(), Error> {
            if !present {
                return Ok(());
            }
            let allowed = allowed_address_fields.contains(&field) || allowed_extra.contains(&field);
            if allowed {
                Ok(())
            } else {
                Err(Error::ConfigSchema {
                    index,
                    reason: format!("field '{field}' is not valid for type '{scheme}'"),
                })
            }
        };

        deny(self.path.is_some(), "path")?;
        deny(self.baud.is_some(), "baud")?;
        deny(self.flow_control.is_some(), "flow_control")?;
        deny(self.bind.is_some(), "bind")?;
        deny(self.host.is_some(), "host")?;
        deny(self.port.is_some(), "port")?;
        deny(self.idle_secs.is_some(), "idle_secs")?;
        deny(self.latch_idle_secs.is_some(), "latch_idle_secs")?;
        Ok(())
    }

    fn collect_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        if let Some(v) = self.tx_queue_frames {
            pairs.push(("tx_queue_frames".to_string(), v.to_string()));
        }
        if let Some(v) = self.flow_control.as_ref() {
            pairs.push(("flow_control".to_string(), v.clone()));
        }
        if let Some(v) = self.idle_secs {
            pairs.push(("idle_secs".to_string(), v.to_string()));
        }
        if let Some(v) = self.latch_idle_secs {
            pairs.push(("latch_idle_secs".to_string(), v.to_string()));
        }
        if let Some(v) = self.sniffer {
            pairs.push(("sniffer".to_string(), v.to_string()));
        }
        if let Some(v) = self.group.as_ref() {
            pairs.push(("group".to_string(), v.clone()));
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
    if let Some(v) = val {
        pairs.push((key.to_string(), v.to_string()));
    }
}

fn missing(index: usize, scheme: &'static str, field: &'static str) -> Error {
    Error::ConfigSchema {
        index,
        reason: format!("type '{scheme}' requires field '{field}'"),
    }
}

/// Install the global tracing subscriber. Called once at process startup
/// (CLAUDE.md "Lifecycle: Startup"). Tests that drive [`crate::run_with_cancel`]
/// initialise their own subscriber and bypass this entry point.
pub fn init_tracing(level: LogLevel, format: LogFormat) {
    let builder = tracing_subscriber::fmt()
        .with_max_level(Level::from(level))
        .with_writer(std::io::stderr);
    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_cli_defaults() {
        let c = Config::default();
        assert_eq!(c.log_level, LogLevel::Info);
        assert_eq!(c.log_format, LogFormat::Text);
        assert!(!c.stats);
        assert_eq!(c.stats_interval, 5);
        assert_eq!(c.dedup_ms, 0);
        assert_eq!(c.shutdown_grace, 5);
        assert!(c.endpoints.is_empty());
    }

    #[test]
    fn validate_passes_on_unique_names() {
        let c = Config {
            endpoints: vec![
                EndpointSpec::parse("udps:0.0.0.0:1#a").unwrap(),
                EndpointSpec::parse("udps:0.0.0.0:2#b").unwrap(),
            ],
            ..Config::default()
        };
        c.validate().expect("unique names must pass");
    }

    #[test]
    fn validate_detects_duplicate_explicit_names() {
        let c = Config {
            endpoints: vec![
                EndpointSpec::parse("udps:0.0.0.0:1#foo").unwrap(),
                EndpointSpec::parse("udps:0.0.0.0:2#foo").unwrap(),
            ],
            ..Config::default()
        };
        match c.validate() {
            Err(Error::DuplicateName(n)) => assert_eq!(n, "foo"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn validate_detects_duplicate_auto_names() {
        let c = Config {
            endpoints: vec![
                EndpointSpec::parse("udps:0.0.0.0:1").unwrap(),
                EndpointSpec::parse("udps:0.0.0.0:1").unwrap(),
            ],
            ..Config::default()
        };
        assert!(matches!(c.validate(), Err(Error::DuplicateName(_))));
    }

    #[test]
    fn log_level_to_tracing_level() {
        assert_eq!(Level::from(LogLevel::Trace), Level::TRACE);
        assert_eq!(Level::from(LogLevel::Debug), Level::DEBUG);
        assert_eq!(Level::from(LogLevel::Info), Level::INFO);
        assert_eq!(Level::from(LogLevel::Warn), Level::WARN);
        assert_eq!(Level::from(LogLevel::Error), Level::ERROR);
    }

    // -- TOML parser --

    #[test]
    fn toml_empty_yields_defaults() {
        let cfg = Config::from_toml_str("").expect("empty TOML must parse");
        assert_eq!(cfg.log_level, LogLevel::Info);
        assert!(cfg.endpoints.is_empty());
    }

    #[test]
    fn toml_globals_only() {
        let s = r#"
log_level = "debug"
log_format = "json"
stats = true
stats_interval = 10
dedup_ms = 200
shutdown_grace = 8
"#;
        let cfg = Config::from_toml_str(s).expect("globals-only TOML must parse");
        assert_eq!(cfg.log_level, LogLevel::Debug);
        assert_eq!(cfg.log_format, LogFormat::Json);
        assert!(cfg.stats);
        assert_eq!(cfg.stats_interval, 10);
        assert_eq!(cfg.dedup_ms, 200);
        assert_eq!(cfg.shutdown_grace, 8);
    }

    #[test]
    fn toml_serial_endpoint() {
        let s = r#"
[[endpoints]]
type = "serial"
path = "/dev/ttyUSB0"
baud = 921600
flow_control = "rtscts"
name = "fc"
tx_queue_frames = 512
"#;
        let cfg = Config::from_toml_str(s).expect("must parse");
        assert_eq!(cfg.endpoints.len(), 1);
        let spec = &cfg.endpoints[0];
        assert_eq!(spec.name, "fc");
        let ep = match &spec.kind {
            crate::endpoint::spec::EndpointKind::Serial(e) => e,
            other => panic!("expected serial, got {other:?}"),
        };
        assert_eq!(ep.path, "/dev/ttyUSB0");
        assert_eq!(ep.baud, 921_600);
        assert_eq!(
            ep.flow_control,
            crate::endpoint::spec::SerialFlowControl::RtsCts,
        );
        assert_eq!(ep.common.tx_queue_frames, Some(512));
    }

    #[test]
    fn toml_udps_endpoint_with_filters() {
        let s = r#"
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
        let cfg = Config::from_toml_str(s).expect("must parse");
        let spec = &cfg.endpoints[0];
        assert_eq!(spec.name, "bus");
        let ep = match &spec.kind {
            crate::endpoint::spec::EndpointKind::UdpServer(e) => e,
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
    fn toml_udpc_ipv6_host_brackets_synthesized_body() {
        let s = r#"
[[endpoints]]
type = "udpc"
host = "::1"
port = 14550
latch_idle_secs = 15
"#;
        let cfg = Config::from_toml_str(s).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            crate::endpoint::spec::EndpointKind::UdpClient(e) => e,
            other => panic!("expected udpc, got {other:?}"),
        };
        assert_eq!(ep.host, "::1");
        assert_eq!(ep.port, 14_550);
        assert_eq!(ep.latch_idle_secs, Some(15));
    }

    #[test]
    fn toml_tcpc_hostname() {
        let s = r#"
[[endpoints]]
type = "tcpc"
host = "gcs.local"
port = 5760
name = "vehicle"
"#;
        let cfg = Config::from_toml_str(s).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            crate::endpoint::spec::EndpointKind::TcpClient(e) => e,
            other => panic!("expected tcpc, got {other:?}"),
        };
        assert_eq!(ep.host, "gcs.local");
        assert_eq!(ep.port, 5760);
        assert_eq!(cfg.endpoints[0].name, "vehicle");
    }

    #[test]
    fn toml_tcps_endpoint() {
        let s = r#"
[[endpoints]]
type = "tcps"
bind = "[::]:5760"
"#;
        let cfg = Config::from_toml_str(s).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            crate::endpoint::spec::EndpointKind::TcpServer(e) => e,
            other => panic!("expected tcps, got {other:?}"),
        };
        assert_eq!(ep.bind_addr.to_string(), "[::]:5760");
    }

    #[test]
    fn toml_unknown_top_level_key_rejected() {
        let s = r#"
nonsense = true
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigParse(_)) => {}
            other => panic!("expected ConfigParse, got {other:?}"),
        }
    }

    #[test]
    fn toml_unknown_endpoint_field_rejected() {
        let s = r#"
[[endpoints]]
type = "udps"
bind = "0.0.0.0:14550"
totally_made_up = 1
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigParse(_)) => {}
            other => panic!("expected ConfigParse, got {other:?}"),
        }
    }

    #[test]
    fn toml_serial_with_bind_rejected() {
        let s = r#"
[[endpoints]]
type = "serial"
path = "/dev/ttyUSB0"
baud = 115200
bind = "0.0.0.0:14550"
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigSchema { index, reason }) => {
                assert_eq!(index, 0);
                assert!(reason.contains("'bind'"), "reason: {reason}");
                assert!(reason.contains("'serial'"), "reason: {reason}");
            }
            other => panic!("expected ConfigSchema, got {other:?}"),
        }
    }

    #[test]
    fn toml_udps_with_host_rejected() {
        let s = r#"
[[endpoints]]
type = "udps"
bind = "0.0.0.0:14550"
host = "gcs.local"
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigSchema { reason, .. }) => assert!(reason.contains("'host'")),
            other => panic!("expected ConfigSchema, got {other:?}"),
        }
    }

    #[test]
    fn toml_udpc_missing_port_rejected() {
        let s = r#"
[[endpoints]]
type = "udpc"
host = "gcs.local"
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigSchema { reason, .. }) => assert!(reason.contains("'port'")),
            other => panic!("expected ConfigSchema, got {other:?}"),
        }
    }

    #[test]
    fn toml_serial_missing_baud_rejected() {
        let s = r#"
[[endpoints]]
type = "serial"
path = "/dev/ttyUSB0"
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigSchema { reason, .. }) => assert!(reason.contains("'baud'")),
            other => panic!("expected ConfigSchema, got {other:?}"),
        }
    }

    #[test]
    fn toml_unknown_endpoint_type_rejected() {
        let s = r#"
[[endpoints]]
type = "carrier_pigeon"
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigSchema { reason, .. }) => {
                assert!(reason.contains("unknown endpoint type"), "reason: {reason}");
            }
            other => panic!("expected ConfigSchema, got {other:?}"),
        }
    }

    #[test]
    fn toml_filter_array_form_rejected() {
        // CLAUDE.md: "TOML accepts the string form only — array forms are a
        // parse-time error." Our serde schema types filter fields as
        // Option<String>, so an inline array fails at the toml-deserialize
        // layer.
        let s = r#"
[[endpoints]]
type = "tcpc"
host = "gcs.local"
port = 5760
block_msgid_in = [33, 100, 150]
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigParse(_)) => {}
            other => panic!("expected ConfigParse, got {other:?}"),
        }
    }

    #[test]
    fn toml_propagates_spec_errors_for_invalid_query_value() {
        // Invalid range (lo > hi) is caught by the existing filter parser,
        // which we reuse via EndpointSpec::build — so an out-of-shape filter
        // surfaces as a SpecError, not a ConfigSchema.
        let s = r#"
[[endpoints]]
type = "tcpc"
host = "gcs.local"
port = 5760
block_msgid_in = "10-5"
"#;
        match Config::from_toml_str(s) {
            Err(Error::Spec(_)) => {}
            other => panic!("expected Spec, got {other:?}"),
        }
    }

    #[test]
    fn toml_propagates_duplicate_names_via_validate() {
        let s = r#"
[[endpoints]]
type = "udps"
bind = "0.0.0.0:1"
name = "foo"

[[endpoints]]
type = "udps"
bind = "0.0.0.0:2"
name = "foo"
"#;
        match Config::from_toml_str(s) {
            Err(Error::DuplicateName(n)) => assert_eq!(n, "foo"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn toml_auto_names_when_omitted() {
        let s = r#"
[[endpoints]]
type = "udps"
bind = "0.0.0.0:14550"
"#;
        let cfg = Config::from_toml_str(s).expect("must parse");
        assert_eq!(cfg.endpoints[0].name, "udps-0_0_0_0-14550");
    }

    #[test]
    fn toml_from_path_round_trip() {
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
        let cfg = Config::from_toml_path(&path).expect("must parse");
        assert!(cfg.stats);
        assert_eq!(cfg.endpoints.len(), 1);
    }

    #[test]
    fn toml_from_path_io_error() {
        let missing = std::path::PathBuf::from("/nonexistent/path/rmr.toml");
        match Config::from_toml_path(&missing) {
            Err(Error::ConfigIo { path, .. }) => assert!(path.contains("nonexistent")),
            other => panic!("expected ConfigIo, got {other:?}"),
        }
    }

    #[test]
    fn toml_invalid_explicit_name_rejected() {
        // CLAUDE.md "`#name` validation": names outside [A-Za-z0-9_-]{1,64}
        // are fatal. TOML routes through `EndpointSpec::build` which calls
        // `validate_name`, so we should see a SpecError from there.
        let s = r#"
[[endpoints]]
type = "udps"
bind = "0.0.0.0:14550"
name = "has spaces"
"#;
        match Config::from_toml_str(s) {
            Err(Error::Spec(_)) => {}
            other => panic!("expected Spec (invalid name), got {other:?}"),
        }
    }

    #[test]
    fn toml_serial_path_containing_colon() {
        // Real-world `/dev/serial/by-id/...` symlinks can carry `:` in the
        // path. The body parser uses `rfind(':')` so the last colon (before
        // the baud) wins, leaving the path intact.
        let s = r#"
[[endpoints]]
type = "serial"
path = "/dev/serial/by-id/usb-FTDI:port0"
baud = 57600
"#;
        let cfg = Config::from_toml_str(s).expect("must parse");
        let ep = match &cfg.endpoints[0].kind {
            crate::endpoint::spec::EndpointKind::Serial(e) => e,
            other => panic!("expected serial, got {other:?}"),
        };
        assert_eq!(ep.path, "/dev/serial/by-id/usb-FTDI:port0");
        assert_eq!(ep.baud, 57_600);
    }

    #[test]
    fn toml_unknown_endpoint_field_error_names_the_field() {
        // The unknown-field error text is part of the contract: operators
        // grep it to find their typo. Pin that the offending key appears in
        // the message at least once.
        let s = r#"
[[endpoints]]
type = "udps"
bind = "0.0.0.0:14550"
totally_made_up = 1
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigParse(e)) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("totally_made_up"),
                    "error message must name the offending field; got: {msg}"
                );
            }
            other => panic!("expected ConfigParse, got {other:?}"),
        }
    }

    #[test]
    fn toml_disallowed_field_takes_priority_over_missing_field() {
        // Regression guard for the validation-ordering decision (see
        // `into_spec`): a serial entry carrying `bind` but lacking `path`
        // must surface the wrong-field-for-scheme error, not the
        // downstream missing-path error.
        let s = r#"
[[endpoints]]
type = "serial"
bind = "0.0.0.0:14550"
"#;
        match Config::from_toml_str(s) {
            Err(Error::ConfigSchema { reason, .. }) => {
                assert!(reason.contains("'bind'"), "reason: {reason}");
                assert!(reason.contains("'serial'"), "reason: {reason}");
            }
            other => panic!("expected ConfigSchema mentioning 'bind' first, got {other:?}"),
        }
    }
}
