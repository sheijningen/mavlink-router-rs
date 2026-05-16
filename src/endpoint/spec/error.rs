use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SpecError {
    #[error("missing scheme in '{0}'")]
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
        scheme: &'static str,
        key: String,
        suggestion: Option<&'static str>,
    },
    #[error("malformed query string: {0}")]
    MalformedQuery(String),
    #[error("duplicate query key '{0}'")]
    DuplicateQueryKey(String),
    #[error("malformed body for scheme '{scheme}': '{body}' ({reason})")]
    MalformedBody {
        scheme: &'static str,
        body: String,
        reason: String,
    },
    #[error("invalid value for '{key}': {reason}")]
    InvalidQueryValue { key: &'static str, reason: String },
}

fn fmt_suggestion(s: &Option<&'static str>) -> String {
    s.map(|s| format!(" (did you mean '{s}'?)"))
        .unwrap_or_default()
}
