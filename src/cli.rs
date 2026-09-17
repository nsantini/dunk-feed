//! Command-line surface. `Cli` and `Command` derive `clap`'s parser, so
//! `--help` and usage text come free. Each subcommand is a stub here; later
//! stories replace the body without touching this shape.

use clap::{Parser, Subcommand};

/// Dunk Feed: ingests Jetstream, scores quote posts, and serves an AT
/// Protocol feed generator.
#[derive(Debug, Parser)]
#[command(name = "dunk", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// The four subcommands. Each is a stub that prints its own name and
/// returns success; later stories add the real behaviour.
#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum Command {
    /// Run the ingest, scorer and HTTP server.
    Run,
    /// Validate the configuration and exit.
    Validate,
    /// Publish the feed generator record.
    Publish,
    /// Dump the current feed to stdout.
    Dump,
}

impl Command {
    /// Prints this command's own name to stdout (BC12). Later stories
    /// replace this with the real per-command behaviour.
    pub fn run(&self) {
        println!("{}", self.name());
    }

    /// The subcommand's name, exactly as printed by `run` and as passed on
    /// the command line.
    pub fn name(&self) -> &'static str {
        match self {
            Command::Run => "run",
            Command::Validate => "validate",
            Command::Publish => "publish",
            Command::Dump => "dump",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_subcommands_exit_zero() {
        for command in [Command::Run, Command::Validate, Command::Publish, Command::Dump] {
            // `run` only prints; reaching this line without panicking is the
            // stub's whole contract, matching "exits 0" for a library call.
            command.run();
        }
    }

    #[test]
    fn each_command_prints_its_own_name() {
        assert_eq!(Command::Run.name(), "run");
        assert_eq!(Command::Validate.name(), "validate");
        assert_eq!(Command::Publish.name(), "publish");
        assert_eq!(Command::Dump.name(), "dump");
    }

    #[test]
    fn missing_subcommand_prints_usage_and_exits_non_zero() {
        let err = Cli::try_parse_from(["dunk"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand);
    }
}
