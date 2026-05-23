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
            Ok(toml) => Some(toml),
            Err(err) => {
                eprintln!("error: {err}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let cli_cfg = match cli.into_cli_config() {
        Ok(cli_cfg) => cli_cfg,
        Err(err) => {
            eprintln!("error: {err}");
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
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

    if cfg.endpoints.is_empty() {
        eprintln!("{NO_ENDPOINT_MESSAGE}");
        return ExitCode::FAILURE;
    }

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
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

const NO_ENDPOINT_MESSAGE: &str = "\
error: no endpoint specified

Usage:
  rmr [GLOBAL OPTS] ENDPOINT [ENDPOINT ...]    # one or more endpoints on the CLI
  rmr -c rmr.toml                              # or load them from a TOML config

Run `rmr --help` for the endpoint grammar and per-scheme query keys.
";
