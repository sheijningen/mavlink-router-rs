use thiserror::Error;

use crate::endpoint::spec::SpecError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid endpoint specification: {0}")]
    Spec(#[from] SpecError),

    #[error("duplicate endpoint name '{0}'")]
    DuplicateName(String),

    #[error(
        "{scheme}: host '{host}' must be an IP literal — listeners cannot bind to a hostname (use 0.0.0.0 or [::] for any-interface)"
    )]
    ListenHostNotAnIp { scheme: &'static str, host: String },
}
