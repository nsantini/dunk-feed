//! Binary entry point. Parses the CLI, initialises `tracing`, loads the
//! config, and dispatches the subcommand. This is the only module that uses
//! `anyhow`; every other module returns its own `thiserror` type.

mod appview;
mod cli;
mod config;
mod ingest;
mod score;

use clap::Parser;
use cli::Cli;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let lookup = |name: &str| std::env::var(name).ok();
    let config = config::load(lookup).map_err(|err| anyhow::anyhow!("config error: {err}"))?;

    let filter = EnvFilter::try_new(&config.log)
        .expect("config::load already validates DUNK_LOG with EnvFilter::try_new");
    tracing_subscriber::fmt().json().with_env_filter(filter).init();

    cli.command.run();

    Ok(())
}
