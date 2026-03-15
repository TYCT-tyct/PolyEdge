mod cli;
mod common;
mod relay;

use anyhow::Result;
use clap::Parser;

use crate::cli::Cli;
use crate::common::install_tracing;

#[tokio::main]
async fn main() -> Result<()> {
    install_tracing();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    relay::run(cli).await
}
