//! Canonical input to [`crate::run`]: globals + the fully-typed endpoint
//! table that the spawner consumes. Both [`crate::cli::Cli`] (today) and the
//! TOML parser (Phase 6's next bullet) funnel into this struct — `Cli` is one
//! input format among potentially several, not the authoritative shape.
//!
//! CLAUDE.md "Project layout" → `config.rs`: TOML schema (serde) and CLI+file
//! merge rules. Step 1 of Phase 6 is the canonical-input refactor; the TOML
//! schema follows in the next commit.

use std::collections::HashSet;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
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
/// — `Cli`, the future TOML parser, integration tests — produces a `Config`
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
}
