use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

mod api;
mod chunker;
mod config;
mod daemon;
mod peer;
mod pty;
mod signals;

#[derive(Debug, Parser)]
#[command(name = "loraterm", about, version)]
struct Cli {
    /// Path to the main configuration file.
    #[arg(long, env = "LORATERM_CONFIG", default_value = "/etc/loraterm/loraterm.toml")]
    config: PathBuf,

    /// Optional override for the whitelist file (otherwise taken from the main config).
    #[arg(long, env = "LORATERM_WHITELIST")]
    whitelist: Option<PathBuf>,

    /// Optional override for the token file (otherwise systemd LoadCredential or main config).
    #[arg(long, env = "LORATERM_TOKEN_FILE")]
    token_file: Option<PathBuf>,

    /// Dump every raw SSE frame to this file (discovery mode). Disables typed dispatch downsides
    /// — typed parsing still runs, but unknown frames are preserved verbatim in the log.
    #[arg(long)]
    sse_raw_log: Option<PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,loraterm=debug")),
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_level(true)
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("loraterm")
        .build()
        .context("failed to build tokio runtime")?;

    runtime.block_on(async move {
        daemon::run(daemon::DaemonArgs {
            config_path: cli.config,
            whitelist_override: cli.whitelist,
            token_override: cli.token_file,
            sse_raw_log: cli.sse_raw_log,
        })
        .await
    })
}
