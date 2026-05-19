//! Tracing initialisation and the `LogLevel → tracing::Level` bridge.
//!
//! [`crate::config`] owns the parsed `LogLevel` / `LogFormat` enums (clap +
//! serde value types); this module owns the operational side — converting
//! the parsed level into the `tracing::Level` the subscriber needs and
//! installing the global subscriber at process startup.

use tracing::Level;

use crate::config::{LogFormat, LogLevel};

impl From<LogLevel> for Level {
    fn from(l: LogLevel) -> Self {
        match l {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

/// Install the global tracing subscriber. Called once at process startup
/// (CLAUDE.md "Lifecycle: Startup"). Tests that drive [`crate::run_with_cancel`]
/// initialise their own subscriber and bypass this entry point.
pub fn init_tracing(level: LogLevel, format: LogFormat) {
    let builder = tracing_subscriber::fmt()
        .with_max_level(Level::from(level))
        .with_writer(std::io::stderr);
    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_level_to_tracing_level() {
        assert_eq!(Level::from(LogLevel::Trace), Level::TRACE);
        assert_eq!(Level::from(LogLevel::Debug), Level::DEBUG);
        assert_eq!(Level::from(LogLevel::Info), Level::INFO);
        assert_eq!(Level::from(LogLevel::Warn), Level::WARN);
        assert_eq!(Level::from(LogLevel::Error), Level::ERROR);
    }
}
