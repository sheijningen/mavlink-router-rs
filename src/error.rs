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

    /// Spec error from a TOML `[[endpoints]]` entry.
    #[error("invalid endpoint{}: {source}", fmt_toml_locator(.index, .name.as_deref()))]
    SpecInToml {
        index: usize,
        name: Option<String>,
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
    #[error("invalid endpoint{}: {reason}", fmt_toml_locator(.index, .name.as_deref()))]
    ConfigSchema {
        index: usize,
        name: Option<String>,
        reason: String,
    },
}

fn fmt_toml_locator(index: &usize, name: Option<&str>) -> String {
    let position = index + 1;
    match name {
        Some(name) => format!(" [[endpoints]] #{position} '{name}'"),
        None => format!(" [[endpoints]] #{position}"),
    }
}
