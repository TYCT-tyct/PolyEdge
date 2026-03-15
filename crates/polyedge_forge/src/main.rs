#![recursion_limit = "512"]

mod api;
mod cli;
mod common;
mod db_sink;
mod fev1;
mod ireland;
mod market_data_exchange;
mod market_switch;
mod models;
mod persist;

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
        Command::IrelandRecorder(args) => ireland::run_ireland_recorder(*args).await,
        Command::IrelandApi(args) => ireland::run_ireland_api(*args).await,
    }
}
