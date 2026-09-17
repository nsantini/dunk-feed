//! Binary entry point. Parses the CLI, initialises `tracing`, loads the
//! config, and dispatches the subcommand. This is the only module that uses
//! `anyhow`; every other module returns its own `thiserror` type.

mod cli;
mod config;

use clap::Parser;
use cli::Cli;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let lookup = |name: &str| std::env::var(name).ok();
    let config = match config::load(lookup) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("config error: {err}");
            std::process::exit(1);
        }
    };

    tracing_subscriber::fmt().json().with_env_filter(EnvFilter::new(config.log)).init();

    cli.command.run();

    Ok(())
}
