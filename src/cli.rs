use std::path::PathBuf;

use clap::Parser;

use crate::config::{Config, DEFAULT_DEDUP_MS, DEFAULT_STATS_INTERVAL_SECS, LogFormat, LogLevel};
use crate::endpoint::spec::EndpointSpec;
use crate::error::Error;

/// Parsed command-line arguments. Built from argv via clap derive; the
/// resulting struct is converted into a [`Config`] via
/// [`Cli::try_into_config`], which is what [`crate::run`] actually consumes.
/// CLI is one input format among potentially several (TOML next) — `Config`
/// is the canonical, resolved shape.
#[derive(Parser, Debug)]
#[command(name = "rmr", version, about = "Rust MAVLink Router")]
pub struct Cli {
    /// TOML config file with endpoints and globals
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Log verbosity
    #[arg(long, value_enum, default_value_t = LogLevel::default(), value_name = "LEVEL")]
    pub log_level: LogLevel,

    /// Log format
    #[arg(long, value_enum, default_value_t = LogFormat::default(), value_name = "FMT")]
    pub log_format: LogFormat,

    /// Emit periodic per-endpoint stats (JSON-Lines on stdout)
    #[arg(long)]
    pub stats: bool,

    /// Stats output interval in seconds
    #[arg(long, default_value_t = DEFAULT_STATS_INTERVAL_SECS, value_name = "N")]
    pub stats_interval: u64,

    /// Duplicate suppression window in milliseconds (0 disables dedup)
    #[arg(long, default_value_t = DEFAULT_DEDUP_MS, value_name = "N")]
    pub dedup_ms: u64,

    /// One or more endpoint specifications (scheme:body[#name][?key=val&...])
    #[arg(value_name = "ENDPOINT", required_unless_present = "config")]
    pub endpoints: Vec<String>,
}

impl Cli {
    /// Build the canonical [`Config`] from this CLI invocation. Two paths:
    ///
    /// * `--config <FILE>` set, no CLI endpoints — load entirely from TOML
    ///   via [`Config::from_toml_path`]. CLI globals (`--log-level`, etc.)
    ///   are ignored in this mode; the TOML controls everything.
    /// * No `--config`, CLI endpoints set — parse each endpoint string into
    ///   a typed [`EndpointSpec`] and assemble a [`Config`] from CLI globals.
    ///
    /// Mixing the two ([`crate::error::Error::CliTomlMixUnsupported`]) is
    /// deferred to the CLI+TOML merge step of Phase 6. Clap's `required_unless_present`
    /// rules out the empty-and-no-config case at argv parse time.
    pub fn try_into_config(self) -> Result<Config, Error> {
        match (self.config.as_ref(), self.endpoints.is_empty()) {
            (Some(path), true) => Config::from_toml_path(path),
            (Some(_), false) => Err(Error::CliTomlMixUnsupported),
            (None, _) => self.into_config_from_args(),
        }
    }

    fn into_config_from_args(self) -> Result<Config, Error> {
        let endpoints = parse_specs(&self.endpoints)?;
        let cfg = Config {
            log_level: self.log_level,
            log_format: self.log_format,
            stats: self.stats,
            stats_interval: self.stats_interval,
            dedup_ms: self.dedup_ms,
            endpoints,
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

/// Turn a slice of CLI-style endpoint strings into typed [`EndpointSpec`]s,
/// routing each one through the same [`crate::config::EndpointEntry`] that
/// TOML deserialisation produces. CLI argv is the "thin wrapper" input format;
/// the TOML schema's typed-entry shape is the canonical intermediate — both
/// paths flow into [`crate::config::EndpointEntry::into_spec`] →
/// [`crate::endpoint::spec::EndpointSpec::build`], so a single tokenize +
/// validate + build pipeline covers every operator input. Duplicate-name
/// detection is left to [`crate::config::Config::validate`].
pub fn parse_specs(raw: &[String]) -> Result<Vec<EndpointSpec>, Error> {
    let mut specs = Vec::with_capacity(raw.len());
    for (idx, s) in raw.iter().enumerate() {
        let entry = crate::config::EndpointEntry::from_cli_string(s)?;
        specs.push(entry.into_spec(idx)?);
    }
    Ok(specs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal() {
        let cli = Cli::try_parse_from(["rmr", "udps:0.0.0.0:14550"]).unwrap();
        assert_eq!(cli.endpoints, vec!["udps:0.0.0.0:14550".to_string()]);
        assert_eq!(cli.log_level, LogLevel::Info);
        assert_eq!(cli.log_format, LogFormat::Text);
        assert!(!cli.stats);
        assert_eq!(cli.stats_interval, 5);
        assert_eq!(cli.dedup_ms, 0);
        assert!(cli.config.is_none());
    }

    #[test]
    fn parse_log_options() {
        let cli = Cli::try_parse_from([
            "rmr",
            "--log-level",
            "debug",
            "--log-format",
            "json",
            "udps:0.0.0.0:1",
        ])
        .unwrap();
        assert_eq!(cli.log_level, LogLevel::Debug);
        assert_eq!(cli.log_format, LogFormat::Json);
    }

    #[test]
    fn parse_stats_flags() {
        let cli = Cli::try_parse_from([
            "rmr",
            "--stats",
            "--stats-interval",
            "10",
            "--dedup-ms",
            "250",
            "udps:0.0.0.0:1",
        ])
        .unwrap();
        assert!(cli.stats);
        assert_eq!(cli.stats_interval, 10);
        assert_eq!(cli.dedup_ms, 250);
    }

    #[test]
    fn parse_multiple_endpoints() {
        let cli = Cli::try_parse_from([
            "rmr",
            "udps:0.0.0.0:1",
            "tcpc:host:2",
            "serial:/dev/ttyUSB0:115200",
        ])
        .unwrap();
        assert_eq!(cli.endpoints.len(), 3);
    }

    #[test]
    fn no_endpoints_no_config_fails() {
        assert!(Cli::try_parse_from(["rmr"]).is_err());
    }

    #[test]
    fn config_only_ok() {
        let cli = Cli::try_parse_from(["rmr", "--config", "rmr.toml"]).unwrap();
        assert!(cli.endpoints.is_empty());
        assert_eq!(
            cli.config
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
            Some("rmr.toml".to_string())
        );
    }

    #[test]
    fn try_into_config_builds_canonical_config() {
        let cli = Cli::try_parse_from([
            "rmr",
            "--stats",
            "--dedup-ms",
            "300",
            "udps:0.0.0.0:14550#bus",
            "tcpc:gcs.local:5760#vehicle",
        ])
        .unwrap();
        let cfg = cli.try_into_config().expect("conversion must succeed");
        assert!(cfg.stats);
        assert_eq!(cfg.dedup_ms, 300);
        assert_eq!(cfg.endpoints.len(), 2);
        assert_eq!(cfg.endpoints[0].name, "bus");
        assert_eq!(cfg.endpoints[1].name, "vehicle");
    }

    #[test]
    fn try_into_config_propagates_duplicate_name() {
        let cli = Cli::try_parse_from(["rmr", "udps:0.0.0.0:1#foo", "udps:0.0.0.0:2#foo"]).unwrap();
        match cli.try_into_config() {
            Err(Error::DuplicateName(n)) => assert_eq!(n, "foo"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn try_into_config_propagates_spec_error() {
        let cli = Cli::try_parse_from(["rmr", "bogus-not-an-endpoint"]).unwrap();
        assert!(matches!(cli.try_into_config(), Err(Error::Spec(_))));
    }

    #[test]
    fn parse_specs_ok() {
        let raw = vec![
            "udps:0.0.0.0:14550#bus".to_string(),
            "tcpc:gcs.local:5760#vehicle".to_string(),
        ];
        let specs = parse_specs(&raw).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "bus");
        assert_eq!(specs[1].name, "vehicle");
    }

    #[test]
    fn parse_specs_propagates_spec_error() {
        let raw = vec!["bogus-not-an-endpoint".to_string()];
        assert!(matches!(parse_specs(&raw), Err(Error::Spec(_))));
    }

    #[test]
    fn try_into_config_rejects_mixing_config_file_and_cli_endpoints() {
        let cli =
            Cli::try_parse_from(["rmr", "--config", "rmr.toml", "udps:0.0.0.0:14550"]).unwrap();
        match cli.try_into_config() {
            Err(Error::CliTomlMixUnsupported) => {}
            other => panic!("expected CliTomlMixUnsupported, got {other:?}"),
        }
    }
}
