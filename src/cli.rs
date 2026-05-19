use std::path::PathBuf;

use clap::Parser;

use crate::config::{Config, LogFormat, LogLevel};
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
    #[arg(long, default_value_t = crate::config::DEFAULT_STATS_INTERVAL_SECS, value_name = "N")]
    pub stats_interval: u64,

    /// Duplicate suppression window in milliseconds (0 disables dedup)
    #[arg(long, default_value_t = crate::config::DEFAULT_DEDUP_MS, value_name = "N")]
    pub dedup_ms: u64,

    /// Overall wall-clock budget for shutdown in seconds
    #[arg(long, default_value_t = crate::config::DEFAULT_SHUTDOWN_GRACE_SECS, value_name = "N")]
    pub shutdown_grace: u64,

    /// One or more endpoint specifications (scheme:body[#name][?key=val&...])
    #[arg(value_name = "ENDPOINT", required_unless_present = "config")]
    pub endpoints: Vec<String>,
}

impl Cli {
    /// Build the canonical [`Config`] from this CLI invocation. Parses every
    /// `endpoints` string into a typed [`EndpointSpec`] and runs
    /// [`Config::validate`] so cross-endpoint invariants (duplicate names)
    /// are caught before the spawner sees anything.
    ///
    /// TOML config loading is wired in the next Phase 6 commit; today
    /// `--config <FILE>` is accepted by clap but silently ignored here.
    pub fn try_into_config(self) -> Result<Config, Error> {
        let endpoints = parse_specs(&self.endpoints)?;
        let cfg = Config {
            log_level: self.log_level,
            log_format: self.log_format,
            stats: self.stats,
            stats_interval: self.stats_interval,
            dedup_ms: self.dedup_ms,
            shutdown_grace: self.shutdown_grace,
            endpoints,
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

/// Turn a slice of CLI-style endpoint strings into typed [`EndpointSpec`]s.
/// Kept on this module because the input shape (`Vec<String>` from argv) is
/// CLI-specific; TOML parsing builds `EndpointSpec`s through a different
/// path. Duplicate-name detection is intentionally left to [`Config::validate`]
/// so the rule is enforced uniformly across CLI-only, TOML-only, and
/// (eventually) merged inputs.
pub fn parse_specs(raw: &[String]) -> Result<Vec<EndpointSpec>, Error> {
    let mut specs = Vec::with_capacity(raw.len());
    for s in raw {
        specs.push(EndpointSpec::parse(s)?);
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
        assert_eq!(cli.shutdown_grace, 5);
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
            "--shutdown-grace",
            "8",
            "udps:0.0.0.0:1",
        ])
        .unwrap();
        assert!(cli.stats);
        assert_eq!(cli.stats_interval, 10);
        assert_eq!(cli.dedup_ms, 250);
        assert_eq!(cli.shutdown_grace, 8);
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
}
