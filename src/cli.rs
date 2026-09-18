//! Command-line surface. `Cli` and `Command` derive `clap`'s parser, so
//! `--help` and usage text come free. `Publish` and `Dump` are still stubs;
//! later stories replace their bodies without touching this shape. `Run`
//! and `Validate` are real: `Run` is `dunk run`, story 06's ingest task
//! (TECH-DESIGN section 5.1), and `Validate` is `dunk validate`,
//! TECH-DESIGN section 10's phase 0 tool. Both run through [`dispatch`].

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use thiserror::Error;

use crate::appview::AppViewClient;
use crate::config::Config;
use crate::ingest::{self, IngestError};
use crate::score::{Thresholds, Weights};
use crate::validate::{self, ValidateError};

/// Dunk Feed: ingests Jetstream, scores quote posts, and serves an AT
/// Protocol feed generator.
#[derive(Debug, Parser)]
#[command(name = "dunk", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// The four subcommands. `Run`, `Publish` and `Dump` are stubs that print
/// their own name and return success; later stories add their real
/// behaviour. `Validate` runs `dunk validate`, TECH-DESIGN section 10's
/// phase 0 tool.
#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum Command {
    /// Run the ingest, scorer and HTTP server.
    Run,
    /// Score candidate dunks, seeded from `hot-classic` or `--seed-file`,
    /// and print a ranked table plus a CSV of the same rows.
    Validate {
        /// Pages of `hot-classic` to fetch (100 posts each) when no
        /// `--seed-file` is given, or it yields no candidates (BC7, BC13).
        #[arg(long, default_value_t = 3)]
        pages: u32,
        /// A file of `at://` quote post URIs, one per line, seeding
        /// candidates instead of `hot-classic` (BC6, BC7, BC8, BC9).
        #[arg(long)]
        seed_file: Option<PathBuf>,
        /// Path the same rows are written to as a CSV.
        #[arg(long, default_value = "./dunk-validate.csv")]
        csv_path: PathBuf,
    },
    /// Publish the feed generator record.
    Publish,
    /// Dump the current feed to stdout.
    Dump,
}

impl Command {
    /// Prints this command's own name to stdout (BC12). `Publish` and
    /// `Dump` have no other behaviour yet; `Run` and `Validate`'s real
    /// behaviour runs through [`dispatch`] instead, since both need
    /// `Config`, and `Run` no longer prints its own name (`dispatch` calls
    /// `ingest::run` for it directly).
    pub fn run(&self) {
        println!("{}", self.name());
    }

    /// The subcommand's name, exactly as printed by `run` and as passed on
    /// the command line.
    pub fn name(&self) -> &'static str {
        match self {
            Command::Run => "run",
            Command::Validate { .. } => "validate",
            Command::Publish => "publish",
            Command::Dump => "dump",
        }
    }
}

/// Every error a subcommand can raise, so `main.rs` has one type to catch
/// (BC34). `Publish` and `Dump` are infallible stubs and never construct
/// either variant.
#[derive(Debug, Error)]
pub enum CliError {
    #[error(transparent)]
    Validate(#[from] ValidateError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
}

/// Dispatches `command`, built from `config`. `Run` calls `ingest::run`
/// (BC33 to BC35); `Validate` builds the `AppViewClient`, `Weights` and
/// `Thresholds` `config` describes and calls `validate::run` with its own
/// flags; `Publish` and `Dump` only print their own name (`Command::run`).
/// This is the only path that can fail: `main.rs` prints the error and
/// exits 1 (BC10, BC11, BC12, BC34). Round 1 finding 6: `Validate`'s body
/// used to live in a separate `dispatch_validate`, whose only reason to
/// exist was mapping `ValidateError` to `CliError::Validate` at its call
/// site; `CliError::Validate`'s own `#[from]` does that through `?` just as
/// well, so the indirection is gone.
pub async fn dispatch(command: &Command, config: &Config) -> Result<(), CliError> {
    match command {
        Command::Run => ingest::run(config).await?,
        Command::Validate { pages, seed_file, csv_path } => {
            let client = AppViewClient::new(config).map_err(ValidateError::from)?;
            let weights = Weights::from(config);
            let thresholds = Thresholds::from(config);
            validate::run(&client, &weights, &thresholds, *pages, seed_file.as_deref(), csv_path)
                .await?;
        }
        other => other.run(),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate_command() -> Command {
        Command::Validate {
            pages: 3,
            seed_file: None,
            csv_path: PathBuf::from("./dunk-validate.csv"),
        }
    }

    #[test]
    fn stub_subcommands_exit_zero() {
        for command in [Command::Run, validate_command(), Command::Publish, Command::Dump] {
            // `run` only prints; reaching this line without panicking is the
            // stub's whole contract, matching "exits 0" for a library call.
            // `Validate`'s real behaviour is `dispatch`, not `run`, so this
            // never touches the network.
            command.run();
        }
    }

    #[test]
    fn each_command_prints_its_own_name() {
        assert_eq!(Command::Run.name(), "run");
        assert_eq!(validate_command().name(), "validate");
        assert_eq!(Command::Publish.name(), "publish");
        assert_eq!(Command::Dump.name(), "dump");
    }

    #[test]
    fn validate_flags_default() {
        let cli = Cli::try_parse_from(["dunk", "validate"]).expect("validate parses with no flags");
        match cli.command {
            Command::Validate { pages, seed_file, csv_path } => {
                assert_eq!(pages, 3);
                assert_eq!(seed_file, None);
                assert_eq!(csv_path, PathBuf::from("./dunk-validate.csv"));
            }
            other => panic!("expected Validate, got {other:?}"),
        }
    }

    #[test]
    fn validate_flags_are_parsed() {
        let cli = Cli::try_parse_from([
            "dunk",
            "validate",
            "--pages",
            "5",
            "--seed-file",
            "seeds.txt",
            "--csv-path",
            "out.csv",
        ])
        .expect("validate parses with every flag set");
        match cli.command {
            Command::Validate { pages, seed_file, csv_path } => {
                assert_eq!(pages, 5);
                assert_eq!(seed_file, Some(PathBuf::from("seeds.txt")));
                assert_eq!(csv_path, PathBuf::from("out.csv"));
            }
            other => panic!("expected Validate, got {other:?}"),
        }
    }

    #[test]
    fn missing_subcommand_prints_usage_and_exits_non_zero() {
        let err = Cli::try_parse_from(["dunk"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand);
    }

    #[tokio::test]
    async fn validate_with_zero_pages_makes_no_call() {
        // Finding 5, BC13: `pages == 0` with no `--seed-file` makes no
        // network call at all. `DUNK_APPVIEW_URL` points at a local address
        // nothing listens on, so a call that did go out would fail or hang
        // instead of returning `Ok`.
        let lookup = |name: &str| match name {
            "DUNK_HOSTNAME" => Some("feed.example.com".to_string()),
            "DUNK_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "DUNK_APPVIEW_URL" => Some("http://127.0.0.1:9".to_string()),
            _ => None,
        };
        let config = crate::config::load(lookup).expect("minimal config loads");
        let csv_path = std::env::temp_dir().join("dunk-validate-zero-pages-test.csv");
        let command = Command::Validate { pages: 0, seed_file: None, csv_path: csv_path.clone() };

        dispatch(&command, &config).await.expect("zero pages with no seed file makes no call");

        let written = std::fs::read_to_string(&csv_path).expect("csv was written");
        assert_eq!(written.lines().count(), 1); // header only, zero rows
        let _ = std::fs::remove_file(&csv_path);
    }

    // BC34, BC35: `Command::Run` with a `DUNK_DB_PATH` whose parent
    // directory does not exist never reaches the network. `Store::open`
    // fails first, `ingest::run` returns `IngestError::Store`, and
    // `dispatch` surfaces it through `CliError::Ingest` for `main.rs` to
    // catch, print and exit 1 on.
    #[tokio::test]
    async fn run_with_unreadable_db_path_is_a_cli_ingest_store_error() {
        let lookup = |name: &str| match name {
            "DUNK_HOSTNAME" => Some("feed.example.com".to_string()),
            "DUNK_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "DUNK_DB_PATH" => Some("/no/such/directory/dunk.db".to_string()),
            _ => None,
        };
        let config = crate::config::load(lookup).expect("minimal config loads");

        let result = dispatch(&Command::Run, &config).await;

        match result {
            Err(CliError::Ingest(IngestError::Store(_))) => {}
            other => panic!("expected CliError::Ingest(IngestError::Store(_)), got {other:?}"),
        }
    }
}
