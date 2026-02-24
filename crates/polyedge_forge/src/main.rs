mod api;
mod cli;
mod common;
mod db_sink;
mod ireland;
mod models;
mod persist;
mod tokyo;

use anyhow::Result;
use clap::Parser;

use crate::cli::{Cli, Command};
use crate::common::install_tracing;

#[tokio::main]
async fn main() -> Result<()> {
    install_tracing();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    match cli.command {
        Command::TokyoRelay(args) => tokyo::run_tokyo_relay(args).await,
        Command::IrelandRecorder(args) => ireland::run_ireland_recorder(*args).await,
    }
}
