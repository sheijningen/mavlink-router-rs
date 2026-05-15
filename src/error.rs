use thiserror::Error;

use crate::endpoint::spec::SpecError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid endpoint specification: {0}")]
    Spec(#[from] SpecError),

    #[error("duplicate endpoint name '{0}'")]
    DuplicateName(String),
}
