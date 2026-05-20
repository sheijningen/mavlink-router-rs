//! Canonical input to [`crate::run`]: the fully-resolved [`Config`] struct
//! the spawner consumes, plus the merge step that combines a TOML-derived
//! [`crate::parsers::toml::TomlConfig`] with a CLI-derived
//! [`crate::parsers::cli::CliConfig`].
//!
//! Layering:
//!
//! 1. [`crate::parsers::cli`] parses argv into a [`crate::parsers::cli::CliConfig`]
//!    with `Option<T>` globals (so "operator omitted" is distinguishable
//!    from "operator set to default").
//! 2. [`crate::parsers::toml`] parses a TOML file into a
//!    [`crate::parsers::toml::TomlConfig`] with the same shape.
//! 3. [`Config::merge`] (this module) folds the two together: CLI > TOML >
//!    defaults for globals, and for endpoints the TOML set has any
//!    name-colliding entry **wholesale replaced** by the CLI entry. The
//!    collided names are returned alongside the [`Config`] in a
//!    [`MergeOutcome`] so the caller can WARN about them (after installing
//!    the tracing subscriber, in `main`).
//! 4. [`Config::validate`] then catches duplicate names **within** a single
//!    source — cross-source dups are resolved by the merge, not reported as
//!    errors.
//!
//! Neither parser submodule references the other; both only reach into this
//! module for the shared `LogLevel` / `LogFormat` value types and the
//! defaults.

use std::collections::HashSet;

use serde::Deserialize;

use crate::endpoint::spec::EndpointSpec;
use crate::error::Error;
use crate::parsers::cli::CliConfig;
use crate::parsers::toml::TomlConfig;

/// CLAUDE.md "Defaults" → `stats_interval_secs_secs` (period of stats JSON-Lines
/// output).
pub const DEFAULT_STATS_INTERVAL_SECS: u64 = 5;

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

/// Canonical, fully-resolved runtime configuration consumed by [`crate::run`]
/// and [`crate::run`]. Produced exclusively by [`Config::merge`]
/// (or [`Config::default`] for trivial test fixtures); never deserialised
/// directly.
#[derive(Debug, Clone)]
pub struct Config {
    pub log_level: LogLevel,
    pub log_format: LogFormat,
    pub stats: bool,
    pub stats_interval_secs: u64,
    pub dedup_ms: u64,
    pub skip_config_log: bool,
    pub endpoints: Vec<EndpointSpec>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_level: LogLevel::default(),
            log_format: LogFormat::default(),
            stats: false,
            stats_interval_secs: DEFAULT_STATS_INTERVAL_SECS,
            dedup_ms: DEFAULT_DEDUP_MS,
            skip_config_log: false,
            endpoints: Vec::new(),
        }
    }
}

/// Result of [`Config::merge`]: the resolved [`Config`] plus the list of
/// endpoint `#name`s where a CLI entry replaced a same-named TOML entry,
/// reported in the order they were encountered while walking the TOML set.
/// `main` emits one WARN per name *after* installing the tracing subscriber
/// (which reads the merged log level), so the merge step can't log them
/// itself.
#[derive(Debug)]
pub struct MergeOutcome {
    pub config: Config,
    pub overridden_names: Vec<String>,
}

impl Config {
    /// Combine a TOML-derived config with a CLI-derived config into the final
    /// runtime [`Config`]. Precedence (highest to lowest): CLI globals →
    /// TOML globals → defaults. Endpoints are concatenated with TOML first
    /// (in declaration order) then CLI (in argv order); **any TOML endpoint
    /// whose `#name` collides with a CLI endpoint is wholesale replaced by
    /// the CLI version** — the entire TOML entry (filters, identity,
    /// scheme-specific knobs) is discarded. The colliding names are returned
    /// via [`MergeOutcome::overridden_names`] for the caller to WARN about.
    ///
    /// Duplicate names **within** a single source (two CLI endpoints with
    /// the same name, or two TOML endpoints with the same name) remain a
    /// fatal [`Error::DuplicateName`] — those collisions are not the merge's
    /// to resolve.
    pub fn merge(toml: Option<TomlConfig>, cli: CliConfig) -> Result<MergeOutcome, Error> {
        let toml = toml.unwrap_or_default();

        // Within-source duplicate detection runs *before* the cross-source
        // override pass — otherwise a CLI override could silently mask a
        // duplicate in TOML (or vice versa): both colliding TOML entries
        // would drop into `overridden_names` and the operator's typo would
        // be invisible. The locked decision is "within-source dups remain
        // fatal" regardless of what the other source does.
        check_unique_names(&toml.endpoints)?;
        check_unique_names(&cli.endpoints)?;

        let (endpoints, overridden_names) = merge_endpoints(toml.endpoints, cli.endpoints);

        let config = Config {
            log_level: cli.log_level.or(toml.log_level).unwrap_or_default(),
            log_format: cli.log_format.or(toml.log_format).unwrap_or_default(),
            stats: cli.stats.or(toml.stats).unwrap_or(false),
            stats_interval_secs: cli
                .stats_interval_secs
                .or(toml.stats_interval_secs)
                .unwrap_or(DEFAULT_STATS_INTERVAL_SECS),
            dedup_ms: cli.dedup_ms.or(toml.dedup_ms).unwrap_or(DEFAULT_DEDUP_MS),
            skip_config_log: cli
                .skip_config_log
                .or(toml.skip_config_log)
                .unwrap_or(false),
            endpoints,
        };
        config.validate()?;
        Ok(MergeOutcome {
            config,
            overridden_names,
        })
    }

    /// Cross-endpoint validation: any duplicate `#name` in the merged set is
    /// fatal. `Config::merge` already runs within-source duplicate detection
    /// (and cross-source collisions are resolved by the override pass), so a
    /// duplicate observed here implies a caller built `Config` directly
    /// (e.g. a test fixture) with inconsistent input — `merge`-produced
    /// configs never reach this branch via the within-source path.
    pub fn validate(&self) -> Result<(), Error> {
        check_unique_names(&self.endpoints)
    }
}

/// Concatenate TOML endpoints (declaration order) and CLI endpoints (argv
/// order), dropping any TOML entry whose `#name` collides with a CLI entry.
/// Returns the merged vector alongside the dropped names so the caller can
/// WARN about each replacement.
fn merge_endpoints(
    toml_endpoints: Vec<EndpointSpec>,
    cli_endpoints: Vec<EndpointSpec>,
) -> (Vec<EndpointSpec>, Vec<String>) {
    let cli_names: HashSet<&str> = cli_endpoints
        .iter()
        .map(|endpoint| endpoint.name.as_str())
        .collect();
    let mut overridden_names: Vec<String> = Vec::new();
    let mut endpoints: Vec<EndpointSpec> =
        Vec::with_capacity(toml_endpoints.len() + cli_endpoints.len());
    for endpoint in toml_endpoints {
        if cli_names.contains(endpoint.name.as_str()) {
            overridden_names.push(endpoint.name.clone());
            continue;
        }
        endpoints.push(endpoint);
    }
    endpoints.extend(cli_endpoints);
    (endpoints, overridden_names)
}

/// Fail with [`Error::DuplicateName`] on the first repeated `#name` in
/// `endpoints`. Shared by `Config::validate` (final-config check) and
/// `Config::merge` (per-source check before cross-source override pass).
fn check_unique_names(endpoints: &[EndpointSpec]) -> Result<(), Error> {
    let mut seen = HashSet::<&str>::new();
    for endpoint in endpoints {
        if !seen.insert(endpoint.name.as_str()) {
            return Err(Error::DuplicateName(endpoint.name.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(spec: &str) -> EndpointSpec {
        EndpointSpec::parse(spec).expect("test fixture must parse")
    }

    #[test]
    fn defaults_match_documented_defaults() {
        let config = Config::default();
        assert_eq!(config.log_level, LogLevel::Info);
        assert_eq!(config.log_format, LogFormat::Text);
        assert!(!config.stats);
        assert_eq!(config.stats_interval_secs, 5);
        assert_eq!(config.dedup_ms, 0);
        assert!(!config.skip_config_log);
        assert!(config.endpoints.is_empty());
    }

    #[test]
    fn validate_passes_on_unique_names() {
        let config = Config {
            endpoints: vec![endpoint("udps:0.0.0.0:1#a"), endpoint("udps:0.0.0.0:2#b")],
            ..Config::default()
        };
        config.validate().expect("unique names must pass");
    }

    #[test]
    fn validate_detects_duplicate_explicit_names() {
        let config = Config {
            endpoints: vec![
                endpoint("udps:0.0.0.0:1#foo"),
                endpoint("udps:0.0.0.0:2#foo"),
            ],
            ..Config::default()
        };
        match config.validate() {
            Err(Error::DuplicateName(name)) => assert_eq!(name, "foo"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn validate_detects_duplicate_auto_names() {
        let config = Config {
            endpoints: vec![endpoint("udps:0.0.0.0:1"), endpoint("udps:0.0.0.0:1")],
            ..Config::default()
        };
        assert!(matches!(config.validate(), Err(Error::DuplicateName(_))));
    }

    // -- merge: globals --

    #[test]
    fn merge_globals_cli_overrides_toml() {
        let toml = Some(TomlConfig {
            log_level: Some(LogLevel::Warn),
            log_format: Some(LogFormat::Text),
            stats: Some(false),
            stats_interval_secs: Some(7),
            dedup_ms: Some(50),
            skip_config_log: Some(false),
            endpoints: vec![],
        });
        let cli = CliConfig {
            log_level: Some(LogLevel::Debug),
            log_format: Some(LogFormat::Json),
            stats: Some(true),
            stats_interval_secs: Some(15),
            dedup_ms: Some(250),
            skip_config_log: Some(true),
            endpoints: vec![endpoint("udps:0.0.0.0:1#a")],
        };
        let outcome = Config::merge(toml, cli).expect("merge must succeed");
        let cfg = outcome.config;
        assert_eq!(cfg.log_level, LogLevel::Debug);
        assert_eq!(cfg.log_format, LogFormat::Json);
        assert!(cfg.stats);
        assert_eq!(cfg.stats_interval_secs, 15);
        assert_eq!(cfg.dedup_ms, 250);
        assert!(cfg.skip_config_log);
        assert!(outcome.overridden_names.is_empty());
    }

    #[test]
    fn merge_globals_toml_overrides_defaults_when_cli_unset() {
        let toml = Some(TomlConfig {
            log_level: Some(LogLevel::Trace),
            log_format: None,
            stats: Some(true),
            stats_interval_secs: None,
            dedup_ms: Some(99),
            skip_config_log: Some(true),
            endpoints: vec![],
        });
        let cli = CliConfig {
            endpoints: vec![endpoint("udps:0.0.0.0:1#a")],
            ..CliConfig::default()
        };
        let cfg = Config::merge(toml, cli).expect("merge must succeed").config;
        assert_eq!(cfg.log_level, LogLevel::Trace);
        assert_eq!(cfg.log_format, LogFormat::Text); // default fell through
        assert!(cfg.stats);
        assert_eq!(cfg.stats_interval_secs, DEFAULT_STATS_INTERVAL_SECS); // default fell through
        assert_eq!(cfg.dedup_ms, 99);
        assert!(cfg.skip_config_log);
    }

    #[test]
    fn merge_globals_defaults_when_neither_source_sets_them() {
        let cli = CliConfig {
            endpoints: vec![endpoint("udps:0.0.0.0:1#a")],
            ..CliConfig::default()
        };
        let cfg = Config::merge(None, cli).expect("merge must succeed").config;
        assert_eq!(cfg.log_level, LogLevel::Info);
        assert_eq!(cfg.log_format, LogFormat::Text);
        assert!(!cfg.stats);
        assert_eq!(cfg.stats_interval_secs, DEFAULT_STATS_INTERVAL_SECS);
        assert_eq!(cfg.dedup_ms, DEFAULT_DEDUP_MS);
        assert!(!cfg.skip_config_log);
    }

    #[test]
    fn merge_skip_config_log_toml_fallthrough() {
        // Operator omitted --skip-config-log on the CLI but set
        // `skip_config_log = true` in TOML — the TOML value must win.
        let toml = Some(TomlConfig {
            skip_config_log: Some(true),
            endpoints: vec![],
            ..TomlConfig::default()
        });
        let cli = CliConfig {
            endpoints: vec![endpoint("udps:0.0.0.0:1#a")],
            ..CliConfig::default()
        };
        let cfg = Config::merge(toml, cli).expect("merge must succeed").config;
        assert!(cfg.skip_config_log);
    }

    // -- merge: endpoints --

    #[test]
    fn merge_toml_only_endpoints_pass_through() {
        let toml = Some(TomlConfig {
            endpoints: vec![
                endpoint("udps:0.0.0.0:1#a"),
                endpoint("tcpc:gcs.local:5760#b"),
            ],
            ..TomlConfig::default()
        });
        let cli = CliConfig::default();
        let outcome = Config::merge(toml, cli).expect("merge must succeed");
        assert!(outcome.overridden_names.is_empty());
        let cfg = outcome.config;
        assert_eq!(cfg.endpoints.len(), 2);
        assert_eq!(cfg.endpoints[0].name, "a");
        assert_eq!(cfg.endpoints[1].name, "b");
    }

    #[test]
    fn merge_cli_only_endpoints_pass_through() {
        let cli = CliConfig {
            endpoints: vec![
                endpoint("udps:0.0.0.0:1#a"),
                endpoint("tcpc:gcs.local:5760#b"),
            ],
            ..CliConfig::default()
        };
        let outcome = Config::merge(None, cli).expect("merge must succeed");
        assert!(outcome.overridden_names.is_empty());
        let cfg = outcome.config;
        assert_eq!(cfg.endpoints.len(), 2);
        assert_eq!(cfg.endpoints[0].name, "a");
        assert_eq!(cfg.endpoints[1].name, "b");
    }

    #[test]
    fn merge_cli_endpoint_overrides_toml_same_name() {
        // TOML defines `bus` as a udps listener with a sniffer flag and a
        // group; CLI redefines `bus` as a tcpc client. The CLI version wins
        // wholesale — none of the TOML's identity/scheme knobs survive — and
        // the colliding name is reported in `overridden_names`.
        let toml = Some(TomlConfig {
            endpoints: vec![endpoint("udps:0.0.0.0:14550#bus?sniffer=true&group=uplink")],
            ..TomlConfig::default()
        });
        let cli = CliConfig {
            endpoints: vec![endpoint("tcpc:gcs.local:5760#bus")],
            ..CliConfig::default()
        };
        let outcome = Config::merge(toml, cli).expect("merge must succeed");
        assert_eq!(outcome.overridden_names, vec!["bus"]);
        let cfg = outcome.config;
        assert_eq!(cfg.endpoints.len(), 1);
        assert_eq!(cfg.endpoints[0].name, "bus");
        // The CLI version is a tcpc, not a udps — confirms TOML entry was
        // wholly replaced, not field-merged.
        assert!(matches!(
            cfg.endpoints[0].kind,
            crate::endpoint::spec::EndpointKind::TcpClient(_),
        ));
    }

    #[test]
    fn merge_cli_endpoint_override_preserves_order() {
        // TOML: [keep1, override-me, keep2]; CLI: [override-me, extra]
        // Result: TOML's "keep1" and "keep2" stay in TOML order, then CLI's
        // "override-me" and "extra" appended in CLI order. The overridden
        // TOML entry shifts from its TOML position to the CLI position;
        // operators get exactly what they typed on the CLI, where they
        // typed it.
        let toml = Some(TomlConfig {
            endpoints: vec![
                endpoint("udps:0.0.0.0:1#keep1"),
                endpoint("udps:0.0.0.0:2#override-me"),
                endpoint("udps:0.0.0.0:3#keep2"),
            ],
            ..TomlConfig::default()
        });
        let cli = CliConfig {
            endpoints: vec![
                endpoint("tcpc:host:1#override-me"),
                endpoint("tcpc:host:2#extra"),
            ],
            ..CliConfig::default()
        };
        let outcome = Config::merge(toml, cli).expect("merge must succeed");
        assert_eq!(outcome.overridden_names, vec!["override-me"]);
        let names: Vec<&str> = outcome
            .config
            .endpoints
            .iter()
            .map(|endpoint| endpoint.name.as_str())
            .collect();
        assert_eq!(names, vec!["keep1", "keep2", "override-me", "extra"]);
    }

    #[test]
    fn merge_multiple_overrides_reported_in_order() {
        let toml = Some(TomlConfig {
            endpoints: vec![
                endpoint("udps:0.0.0.0:1#a"),
                endpoint("udps:0.0.0.0:2#b"),
                endpoint("udps:0.0.0.0:3#c"),
            ],
            ..TomlConfig::default()
        });
        let cli = CliConfig {
            endpoints: vec![endpoint("tcpc:h:1#a"), endpoint("tcpc:h:2#c")],
            ..CliConfig::default()
        };
        let outcome = Config::merge(toml, cli).expect("merge must succeed");
        // Reported in TOML iteration order — that's the order the merge
        // walks the TOML endpoints to decide drops.
        assert_eq!(outcome.overridden_names, vec!["a", "c"]);
    }

    #[test]
    fn merge_duplicate_name_within_cli_is_fatal() {
        let cli = CliConfig {
            endpoints: vec![
                endpoint("udps:0.0.0.0:1#foo"),
                endpoint("udps:0.0.0.0:2#foo"),
            ],
            ..CliConfig::default()
        };
        match Config::merge(None, cli) {
            Err(Error::DuplicateName(name)) => assert_eq!(name, "foo"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn merge_duplicate_name_within_toml_is_fatal() {
        let toml = Some(TomlConfig {
            endpoints: vec![
                endpoint("udps:0.0.0.0:1#foo"),
                endpoint("udps:0.0.0.0:2#foo"),
            ],
            ..TomlConfig::default()
        });
        let cli = CliConfig::default();
        match Config::merge(toml, cli) {
            Err(Error::DuplicateName(name)) => assert_eq!(name, "foo"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn merge_within_toml_duplicate_fires_before_override_pass() {
        // Regression guard: if TOML has `#foo` twice AND CLI has `#foo`,
        // the within-TOML duplicate must be reported as a fatal error —
        // not silently masked by the cross-source override pass. The
        // operator's typo must surface as DuplicateName, not as a benign
        // override report.
        let toml = Some(TomlConfig {
            endpoints: vec![
                endpoint("udps:0.0.0.0:1#foo"),
                endpoint("udps:0.0.0.0:2#foo"),
            ],
            ..TomlConfig::default()
        });
        let cli = CliConfig {
            endpoints: vec![endpoint("tcpc:h:1#foo")],
            ..CliConfig::default()
        };
        match Config::merge(toml, cli) {
            Err(Error::DuplicateName(name)) => assert_eq!(name, "foo"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn merge_within_cli_duplicate_fires_before_override_pass() {
        // Same shape as above but the duplicate is on the CLI side. The
        // TOML's `#foo` must NOT be reported as overridden — the operator's
        // CLI invocation is itself malformed.
        let toml = Some(TomlConfig {
            endpoints: vec![endpoint("udps:0.0.0.0:1#foo")],
            ..TomlConfig::default()
        });
        let cli = CliConfig {
            endpoints: vec![endpoint("tcpc:h:1#foo"), endpoint("tcpc:h:2#foo")],
            ..CliConfig::default()
        };
        match Config::merge(toml, cli) {
            Err(Error::DuplicateName(name)) => assert_eq!(name, "foo"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn merge_no_overrides_when_no_collisions() {
        let toml = Some(TomlConfig {
            endpoints: vec![endpoint("udps:0.0.0.0:1#a")],
            ..TomlConfig::default()
        });
        let cli = CliConfig {
            endpoints: vec![endpoint("tcpc:h:1#b")],
            ..CliConfig::default()
        };
        let outcome = Config::merge(toml, cli).expect("merge must succeed");
        assert!(outcome.overridden_names.is_empty());
    }
}
