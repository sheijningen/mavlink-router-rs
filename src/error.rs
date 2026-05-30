use thiserror::Error;

use crate::endpoint::spec::SpecError;

#[derive(Debug, Error)]
pub enum Error {
    /// Spec error from a CLI endpoint argument. `index` is 0-based at the
    /// type level, 1-based in the rendered message.
    #[error(
        "invalid endpoint #{} '{arg}': {source}",
        index + 1
    )]
    SpecInArg {
        index: usize,
        arg: String,
        #[source]
        source: SpecError,
    },

    /// Spec error from a TOML `[endpoint.NAME]` entry.
    #[error("invalid endpoint [endpoint.{name}]: {source}")]
    SpecInToml {
        name: String,
        #[source]
        source: SpecError,
    },

    #[error("duplicate endpoint name '{0}'")]
    DuplicateName(String),

    #[error("failed to read config '{path}': {source}")]
    ConfigIo {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse config TOML: {0}")]
    ConfigParse(#[source] toml::de::Error),

    /// Per-endpoint schema violation surfaced by the TOML loader.
    #[error("invalid endpoint [endpoint.{name}]: {reason}")]
    ConfigSchema { name: String, reason: String },

    #[error("dedup_ms = {requested}ms exceeds maximum of {max}ms")]
    DedupMsTooLarge { requested: u64, max: u64 },

    #[error("stats_interval_secs = {requested}s is below minimum of {min}s")]
    StatsIntervalTooSmall { requested: u64, min: u64 },

    #[error(
        "endpoint '{endpoint_name}' filter {axis}: '{referenced_name}' is not a declared endpoint name"
    )]
    FilterReferencesUnknownEndpoint {
        endpoint_name: String,
        axis: &'static str,
        referenced_name: String,
    },
}
