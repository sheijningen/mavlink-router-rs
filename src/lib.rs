pub mod cli;
pub mod config;
pub mod endpoint;
pub mod error;
pub mod mavlink;
pub mod router;
pub mod shutdown;

pub use error::Error;

use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn run(cli: cli::Cli) -> Result<(), Error> {
    cli::init_tracing(cli.log_level, cli.log_format);

    if cli.config.is_some() {
        tracing::warn!("--config is accepted but not yet wired up (TOML config lands in phase 6)");
    }

    let specs = cli::parse_specs(&cli.endpoints)?;
    let endpoint_count = specs.len();

    let token = CancellationToken::new();
    let signal_token = token.clone();
    tokio::spawn(async move {
        shutdown::watch_for_shutdown_signal(signal_token).await;
    });

    let tasks: JoinSet<()> = JoinSet::new();

    info!(
        endpoint_count,
        "rmr started — endpoint specs parsed; spawn-wiring lands in phase 5"
    );

    token.cancelled().await;
    info!("shutdown signal received");

    shutdown::shutdown(tasks, Duration::from_secs(cli.shutdown_grace)).await;
    info!("rmr stopped");

    Ok(())
}
