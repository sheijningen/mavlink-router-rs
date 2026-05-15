use std::collections::HashSet;
use std::path::PathBuf;

use clap::Parser;
use tracing::Level;

use crate::endpoint::spec::EndpointSpec;
use crate::error::Error;

/// Parsed command-line arguments. Built from argv via clap derive; `cli.rs`'s
/// own parsers turn `endpoints: Vec<String>` into `EndpointSpec`s.
#[derive(Parser, Debug)]
#[command(name = "rmr", version, about = "Rust MAVLink Router")]
pub struct Cli {
    /// TOML config file with endpoints and globals
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Log verbosity
    #[arg(long, value_enum, default_value_t = LogLevel::Info, value_name = "LEVEL")]
    pub log_level: LogLevel,

    /// Log format
    #[arg(long, value_enum, default_value_t = LogFormat::Text, value_name = "FMT")]
    pub log_format: LogFormat,

    /// Emit periodic per-endpoint stats (JSON-Lines on stdout)
    #[arg(long)]
    pub stats: bool,

    /// Stats output interval in seconds
    #[arg(long, default_value_t = 5, value_name = "N")]
    pub stats_interval: u64,

    /// Duplicate suppression window in milliseconds (0 disables dedup)
    #[arg(long, default_value_t = 0, value_name = "N")]
    pub dedup_ms: u64,

    /// Overall wall-clock budget for shutdown in seconds
    #[arg(long, default_value_t = 5, value_name = "N")]
    pub shutdown_grace: u64,

    /// One or more endpoint specifications (scheme:body[#name][?key=val&...])
    #[arg(value_name = "ENDPOINT", required_unless_present = "config")]
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum LogFormat {
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

pub fn init_tracing(level: LogLevel, format: LogFormat) {
    let builder = tracing_subscriber::fmt()
        .with_max_level(Level::from(level))
        .with_writer(std::io::stderr);
    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
}

pub fn parse_specs(raw: &[String]) -> Result<Vec<EndpointSpec>, Error> {
    let mut specs = Vec::with_capacity(raw.len());
    let mut seen_names = HashSet::<String>::new();
    for s in raw {
        let spec = EndpointSpec::parse(s)?;
        if !seen_names.insert(spec.name.clone()) {
            return Err(Error::DuplicateName(spec.name));
        }
        specs.push(spec);
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
    fn parse_specs_detects_duplicate_explicit_names() {
        let raw = vec![
            "udps:0.0.0.0:1#foo".to_string(),
            "udps:0.0.0.0:2#foo".to_string(),
        ];
        match parse_specs(&raw) {
            Err(Error::DuplicateName(n)) => assert_eq!(n, "foo"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn parse_specs_detects_duplicate_auto_names() {
        let raw = vec!["udps:0.0.0.0:1".to_string(), "udps:0.0.0.0:1".to_string()];
        assert!(matches!(parse_specs(&raw), Err(Error::DuplicateName(_))));
    }

    #[test]
    fn parse_specs_propagates_spec_error() {
        let raw = vec!["bogus-not-an-endpoint".to_string()];
        assert!(matches!(parse_specs(&raw), Err(Error::Spec(_))));
    }

    #[test]
    fn log_level_to_tracing_level() {
        assert_eq!(Level::from(LogLevel::Trace), Level::TRACE);
        assert_eq!(Level::from(LogLevel::Debug), Level::DEBUG);
        assert_eq!(Level::from(LogLevel::Info), Level::INFO);
        assert_eq!(Level::from(LogLevel::Warn), Level::WARN);
        assert_eq!(Level::from(LogLevel::Error), Level::ERROR);
    }
}
