//! Command-line surface. `Cli` and `Command` derive `clap`'s parser, so
//! `--help` and usage text come free. `Run`, `Validate`, `Publish` and
//! `Dump` are all real: `Run` is `upstage run`, story 06's ingest task
//! (TECH-DESIGN section 5.1); `Validate` is `upstage validate`, TECH-DESIGN
//! section 10's phase 0 tool; `Publish` is `upstage publish`, story 09,
//! TECH-DESIGN section 11.2; `Dump` is `upstage dump`, story 12, which writes
//! the tuning CSV `docs/RUNBOOK.md`'s "Tuning with `upstage dump`" section
//! describes. All four run through [`dispatch`].

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use thiserror::Error;

use crate::appview::AppViewClient;
use crate::config::Config;
use crate::dump::{self, DumpError};
use crate::graph_probe::{self, GraphProbeError};
use crate::ingest::{self, IngestError};
use crate::publish::{self, PublishError};
use crate::score::{Thresholds, Weights};
use crate::validate::{self, ValidateError};

/// Upstaged: ingests Jetstream, scores quote posts, and serves an AT
/// Protocol feed generator.
#[derive(Debug, Parser)]
#[command(name = "upstage", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// The four subcommands. `Run` is `upstage run`, story 06's ingest task
/// (TECH-DESIGN section 5.1), and `Validate` is `upstage validate`, TECH-DESIGN
/// section 10's phase 0 tool. `Publish` is `upstage publish`, story 09,
/// TECH-DESIGN section 11.2: it writes the `app.bsky.feed.generator` record.
/// `Dump` is `upstage dump`, story 12: it writes the tuning CSV `dump::run`
/// builds. All four run through [`dispatch`].
#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum Command {
    /// Run the ingest, scorer and HTTP server.
    Run,
    /// Score candidate upstages, seeded from `hot-classic` or `--seed-file`,
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
        #[arg(long, default_value = "./upstage-validate.csv")]
        csv_path: PathBuf,
    },
    /// Publish the feed generator record.
    Publish {
        /// A local image file uploaded as the feed's avatar before the
        /// record is written. `png`, `jpg` or `jpeg`, compared lowercased
        /// (BC4, BC5). Omit to publish with no avatar (BC3).
        #[arg(long)]
        avatar: Option<PathBuf>,
    },
    /// Dump every pair first seen within `--since` to a CSV at `--out`, for
    /// offline tuning (BC1 to BC21, `docs/RUNBOOK.md`'s "Tuning with `upstage
    /// dump`" section).
    Dump {
        /// The window to dump, `^[0-9]+(h|d)$`, e.g. `24h` or `7d` (BC1,
        /// BC2, BC3, BC10, BC11).
        #[arg(long, default_value = "24h")]
        since: String,
        /// Path the CSV is written to. Defaults to
        /// `./upstage-dump-<since>.csv` when omitted (`dump::run`'s own
        /// default).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Read-only, no-write: logs in with `BSKY_HANDLE`, reads the current
    /// `feed` rows, and runs a first graph build for each `--handle` in
    /// memory, printing the numbers story 04 needs to accept or reject the
    /// design (story 03 spec.md). Never run from `upstage run`.
    GraphProbe {
        /// A viewer to probe. Repeatable; not `required`, so an empty list
        /// reaches `graph_probe::preflight`'s own check and exits 1 there
        /// (story 03 spec.md BC9).
        #[arg(long = "handle")]
        handle: Vec<String>,
    },
}

#[cfg(test)]
impl Command {
    /// The subcommand's name, as passed on the command line. Test-only:
    /// every subcommand's real behaviour runs through [`dispatch`], which
    /// matches on `command` directly and never needs this name back out.
    fn name(&self) -> &'static str {
        match self {
            Command::Run => "run",
            Command::Validate { .. } => "validate",
            Command::Publish { .. } => "publish",
            Command::Dump { .. } => "dump",
            Command::GraphProbe { .. } => "graph-probe",
        }
    }
}

/// Every error a subcommand can raise, so `main.rs` has one type to catch
/// (BC34, BC8).
#[derive(Debug, Error)]
pub enum CliError {
    #[error(transparent)]
    Validate(#[from] ValidateError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Publish(#[from] PublishError),
    #[error(transparent)]
    Dump(#[from] DumpError),
    #[error(transparent)]
    GraphProbe(#[from] GraphProbeError),
}

/// Dispatches `command`, built from `config`. `Run` calls `ingest::run`
/// (BC33 to BC35); `Validate` builds the `AppViewClient`, `Weights` and
/// `Thresholds` `config` describes and calls `validate::run` with its own
/// flags; `Publish` calls `publish::run` and prints the at-URI it returns
/// (BC13); `Dump` calls `dump::run` and prints `wrote <rows> rows to
/// <path>` (BC19). This is the only path that can fail: `main.rs` prints
/// the error and exits 1 (BC10, BC11, BC12, BC34, BC8). Round 1 finding 6:
/// `Validate`'s body used to live in a separate `dispatch_validate`, whose
/// only reason to exist was mapping `ValidateError` to `CliError::Validate`
/// at its call site; `CliError::Validate`'s own `#[from]` does that through
/// `?` just as well, so the indirection is gone.
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
        Command::Publish { avatar } => {
            let uri = publish::run(config, avatar.as_deref()).await?;
            println!("{uri}");
        }
        Command::Dump { since, out } => {
            let summary = dump::run(config, since, out.clone())?;
            println!("wrote {} rows to {}", summary.rows, summary.path.display());
        }
        Command::GraphProbe { handle } => {
            let report = graph_probe::run(config, handle).await?;
            graph_probe::print_run_report(&report);
        }
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
            csv_path: PathBuf::from("./upstage-validate.csv"),
        }
    }

    fn dump_command() -> Command {
        Command::Dump { since: "24h".to_string(), out: None }
    }

    #[test]
    fn each_command_prints_its_own_name() {
        assert_eq!(Command::Run.name(), "run");
        assert_eq!(validate_command().name(), "validate");
        assert_eq!(Command::Publish { avatar: None }.name(), "publish");
        assert_eq!(dump_command().name(), "dump");
    }

    #[test]
    fn validate_flags_default() {
        let cli =
            Cli::try_parse_from(["upstage", "validate"]).expect("validate parses with no flags");
        match cli.command {
            Command::Validate { pages, seed_file, csv_path } => {
                assert_eq!(pages, 3);
                assert_eq!(seed_file, None);
                assert_eq!(csv_path, PathBuf::from("./upstage-validate.csv"));
            }
            other => panic!("expected Validate, got {other:?}"),
        }
    }

    #[test]
    fn validate_flags_are_parsed() {
        let cli = Cli::try_parse_from([
            "upstage",
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
        let err = Cli::try_parse_from(["upstage"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand);
    }

    #[test]
    fn publish_flags_default_to_no_avatar() {
        let cli =
            Cli::try_parse_from(["upstage", "publish"]).expect("publish parses with no flags");
        match cli.command {
            Command::Publish { avatar } => assert_eq!(avatar, None),
            other => panic!("expected Publish, got {other:?}"),
        }
    }

    #[test]
    fn publish_avatar_flag_is_parsed() {
        let cli = Cli::try_parse_from(["upstage", "publish", "--avatar", "avatar.png"])
            .expect("publish parses with --avatar");
        match cli.command {
            Command::Publish { avatar } => assert_eq!(avatar, Some(PathBuf::from("avatar.png"))),
            other => panic!("expected Publish, got {other:?}"),
        }
    }

    // BC1: dispatch surfaces preflight's error through CliError::Publish
    // before any network call, since no BSKY_HANDLE is set here.
    #[tokio::test]
    async fn publish_with_missing_credentials_is_a_cli_publish_error() {
        let lookup = |name: &str| match name {
            "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
            "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            _ => None,
        };
        let config = crate::config::load(lookup).expect("minimal config loads");

        let result = dispatch(&Command::Publish { avatar: None }, &config).await;

        match result {
            Err(CliError::Publish(PublishError::MissingCredentials { var })) => {
                assert_eq!(var, "BSKY_HANDLE");
            }
            other => panic!("expected CliError::Publish(MissingCredentials), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn validate_with_zero_pages_makes_no_call() {
        // Finding 5, BC13: `pages == 0` with no `--seed-file` makes no
        // network call at all. `UPSTAGE_APPVIEW_URL` points at a local address
        // nothing listens on, so a call that did go out would fail or hang
        // instead of returning `Ok`.
        let lookup = |name: &str| match name {
            "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
            "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "UPSTAGE_APPVIEW_URL" => Some("http://127.0.0.1:9".to_string()),
            _ => None,
        };
        let config = crate::config::load(lookup).expect("minimal config loads");
        let csv_path = std::env::temp_dir().join("upstage-validate-zero-pages-test.csv");
        let command = Command::Validate { pages: 0, seed_file: None, csv_path: csv_path.clone() };

        dispatch(&command, &config).await.expect("zero pages with no seed file makes no call");

        let written = std::fs::read_to_string(&csv_path).expect("csv was written");
        assert_eq!(written.lines().count(), 1); // header only, zero rows
        let _ = std::fs::remove_file(&csv_path);
    }

    // BC34, BC35: `Command::Run` with a `UPSTAGE_DB_PATH` whose parent
    // directory does not exist never reaches the network. `Store::open`
    // fails first, `ingest::run` returns `IngestError::Store`, and
    // `dispatch` surfaces it through `CliError::Ingest` for `main.rs` to
    // catch, print and exit 1 on.
    #[tokio::test]
    async fn run_with_unreadable_db_path_is_a_cli_ingest_store_error() {
        let lookup = |name: &str| match name {
            "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
            "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "UPSTAGE_DB_PATH" => Some("/no/such/directory/upstage.db".to_string()),
            // Story 11 launch flips the default to true; this test is about
            // `Store::open`, not credentials, so it keeps the old
            // switch-off behaviour explicitly rather than also setting
            // BSKY_HANDLE/BSKY_APP_PASSWORD.
            "UPSTAGE_PERSONALISE" => Some("false".to_string()),
            _ => None,
        };
        let config = crate::config::load(lookup).expect("minimal config loads");

        let result = dispatch(&Command::Run, &config).await;

        match result {
            Err(CliError::Ingest(IngestError::Store(_))) => {}
            other => panic!("expected CliError::Ingest(IngestError::Store(_)), got {other:?}"),
        }
    }

    // --- Dump (BC3, BC8, BC19) ------------------------------------------

    #[test]
    fn dump_flags_default() {
        let cli = Cli::try_parse_from(["upstage", "dump"]).expect("dump parses with no flags");
        match cli.command {
            Command::Dump { since, out } => {
                assert_eq!(since, "24h");
                assert_eq!(out, None);
            }
            other => panic!("expected Dump, got {other:?}"),
        }
    }

    #[test]
    fn dump_flags_are_parsed() {
        let cli = Cli::try_parse_from(["upstage", "dump", "--since", "7d", "--out", "out.csv"])
            .expect("dump parses with every flag set");
        match cli.command {
            Command::Dump { since, out } => {
                assert_eq!(since, "7d");
                assert_eq!(out, Some(PathBuf::from("out.csv")));
            }
            other => panic!("expected Dump, got {other:?}"),
        }
    }

    fn dump_test_config(db_path: &str) -> Config {
        let lookup = |name: &str| match name {
            "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
            "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "UPSTAGE_DB_PATH" => Some(db_path.to_string()),
            _ => None,
        };
        crate::config::load(lookup).expect("minimal config loads")
    }

    // BC8: a bad `--since` surfaces through `CliError::Dump(DumpError::BadSince)`.
    #[tokio::test]
    async fn dispatch_on_bad_since_is_a_cli_dump_bad_since_error() {
        let config = dump_test_config("/no/such/directory/upstage.db");
        let command = Command::Dump { since: "nope".to_string(), out: None };

        let result = dispatch(&command, &config).await;

        match result {
            Err(CliError::Dump(DumpError::BadSince { value })) => assert_eq!(value, "nope"),
            other => panic!("expected CliError::Dump(BadSince), got {other:?}"),
        }
    }

    // BC8: a missing `--out` parent directory surfaces through
    // `CliError::Dump(DumpError::OutDirMissing)`.
    #[tokio::test]
    async fn dispatch_on_missing_out_dir_is_a_cli_dump_out_dir_missing_error() {
        let config = dump_test_config("/no/such/directory/upstage.db");
        let out = PathBuf::from("/no/such/out/dir/dump.csv");
        let command = Command::Dump { since: "24h".to_string(), out: Some(out.clone()) };

        let result = dispatch(&command, &config).await;

        match result {
            Err(CliError::Dump(DumpError::OutDirMissing { path })) => assert_eq!(path, out),
            other => panic!("expected CliError::Dump(OutDirMissing), got {other:?}"),
        }
    }

    // BC19: over a real (empty) temporary database, `dispatch` writes the
    // CSV and prints a row count of zero, header only.
    #[tokio::test]
    async fn dispatch_over_real_database_writes_csv_and_reports_row_count() {
        let db_path =
            std::env::temp_dir().join(format!("upstage-cli-dump-test-{}.db", std::process::id()));
        let csv_path =
            std::env::temp_dir().join(format!("upstage-cli-dump-test-{}.csv", std::process::id()));
        let config = dump_test_config(&db_path.to_string_lossy());
        let command = Command::Dump { since: "24h".to_string(), out: Some(csv_path.clone()) };

        dispatch(&command, &config).await.expect("dump over a real empty database succeeds");

        let written = std::fs::read_to_string(&csv_path).expect("csv was written");
        assert_eq!(written.lines().count(), 1); // header only, zero rows

        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", db_path.display()));
        }
        let _ = std::fs::remove_file(&csv_path);
    }
}
