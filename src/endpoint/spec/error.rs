use thiserror::Error;

use super::Scheme;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SpecError {
    #[error(
        "missing scheme in '{0}' (expected one of: 'serial:', 'udps:', 'udpc:', 'tcps:', 'tcpc:')"
    )]
    MissingScheme(String),
    #[error("unknown scheme '{0}' (valid: serial, udps, udpc, tcps, tcpc)")]
    UnknownScheme(String),
    #[error("invalid endpoint name '{0}': must match [A-Za-z0-9_-]{{1,64}}")]
    InvalidName(String),
    #[error(
        "unknown query key '{key}' for scheme '{scheme}'{}",
        fmt_suggestion(suggestion)
    )]
    UnknownQueryKey {
        scheme: Scheme,
        key: String,
        suggestion: Option<&'static str>,
    },
    #[error("malformed query string: {0}")]
    MalformedQuery(String),
    #[error("duplicate query key '{0}'")]
    DuplicateQueryKey(String),
    #[error("malformed body for scheme '{scheme}': '{body}' ({reason})")]
    MalformedBody {
        scheme: Scheme,
        body: String,
        reason: String,
    },
    #[error("invalid value for '{key}': {reason}")]
    InvalidQueryValue { key: &'static str, reason: String },
}

fn fmt_suggestion(suggestion: &Option<&'static str>) -> String {
    suggestion
        .map(|text| format!(" (did you mean '{text}'?)"))
        .unwrap_or_default()
}
