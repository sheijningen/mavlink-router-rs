use std::process::ExitCode;

use clap::Parser;
use rmr::config::Config;
use rmr::log::init_tracing;
use rmr::parsers::cli::Cli;
use rmr::parsers::toml::TomlConfig;
use rmr::run;
use rmr::shutdown::watch_for_shutdown_signal;
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let toml = match cli.config.clone() {
        Some(path) => match TomlConfig::from_path(&path) {
            Ok(t) => Some(t),
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let cli_cfg = match cli.into_cli_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Merge yields override names alongside the config — they can't be
    // `tracing::warn!`d here yet because the subscriber isn't installed
    // (init_tracing reads the merged log level, which doesn't exist until
    // merge returns). Emit them in the post-init pass below so they
    // actually reach the operator.
    let (cfg, overrides) = match Config::merge(toml, cli_cfg) {
        Ok(outcome) => (outcome.config, outcome.overridden_names),
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    init_tracing(cfg.log_level, cfg.log_format);

    for name in &overrides {
        warn!(
            endpoint = %name,
            "CLI endpoint overrides TOML endpoint of the same name (TOML entry discarded entirely)",
        );
    }

    let token = CancellationToken::new();
    let signal_token = token.clone();
    tokio::spawn(async move {
        watch_for_shutdown_signal(signal_token).await;
    });

    match run(cfg, token).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
