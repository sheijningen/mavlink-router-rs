//! Input parsers — one submodule per supported source. Each emits an
//! `Option<T>`-globals intermediate that [`crate::config::Config::merge`]
//! folds into the final runtime config.

pub mod cli;
pub mod toml;
