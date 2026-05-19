use thiserror::Error;

use crate::endpoint::spec::SpecError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid endpoint specification: {0}")]
    Spec(#[from] SpecError),

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

    #[error("invalid endpoint at [[endpoints]] index {index}: {reason}")]
    ConfigSchema { index: usize, reason: String },
}
