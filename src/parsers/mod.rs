//! Input parsers — one submodule per supported source.
//!
//! Each parser owns its own deserialisation shape and produces an opaque
//! typed intermediate ([`cli::CliConfig`], [`toml::TomlConfig`]) with
//! `Option<T>` globals. The two intermediates are then fed to
//! [`crate::config::Config::merge`], which folds them into the final
//! runtime [`crate::config::Config`].
//!
//! **Architectural invariant:** neither submodule references the other.
//! Their only shared types are [`crate::config::LogLevel`] /
//! [`crate::config::LogFormat`] (the value-typed globals) and
//! [`crate::endpoint::spec::EndpointSpec`] (the parsed endpoint). All
//! cross-source merging logic — including the "CLI wholesale replaces
//! same-name TOML entry with a WARN" rule — lives in [`crate::config`],
//! not here.
//!
//! No items are re-exported at this level on purpose: writing
//! `parsers::cli::CliConfig` instead of `parsers::CliConfig` makes the
//! source of each type visible in every call site, which is what the
//! split-parsers refactor was meant to achieve.

pub mod cli;
pub mod toml;
