# 01 — Crate scaffold, config, and CLI dispatch

- **Follows**: none
- **PRD phase**: 0
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §3, §4

## Outcome

After this ships, all four gates in AGENTS.md pass on the `dunk` crate:
format, lint, test, and release build. The crate builds one binary with a
`run`, `validate`, `publish`, `dump` command dispatch. Each command is a stub
that prints its own name and exits 0. `config.rs` parses every environment
variable in TECH-DESIGN §4, applies the PRD default, and fails fast with a
clear message on a missing required variable or a malformed number. An
engineer who clones the repo has a pinned toolchain, a formatting config, a
Docker ignore file, and an `.env.example` that lists every variable, so the
first `cargo build` succeeds without guessing.

## Non-goals

- Does not implement Jetstream, the store, the scorer, or HTTP. Later stories
  add them.
- Does not implement real `validate`, `publish`, or `dump` logic. Each
  subcommand only prints its name here.
- Does not add `score.rs` or the App View client. Story 02 adds them.

## Approach

The scaffold uses `clap`'s derive API for subcommand dispatch, not a
hand-rolled argument parser, because `clap` gives free `--help` text and the
same shape later stories reuse. `config.rs` parses each variable with Rust's
`FromStr`, not a config crate, because TECH-DESIGN §3 lists no config crate
and the full variable set is twenty lines. Config errors use `thiserror`;
`main.rs` is the only place that uses `anyhow`, per AGENTS.md.

## Files in scope

| Path | Change |
|---|---|
| `Cargo.toml` | New manifest. Adds `tokio`, `clap`, `tracing`, `tracing-subscriber`, `thiserror`, `anyhow` |
| `rust-toolchain.toml` | Pins stable, pinned minor |
| `rustfmt.toml` | Repo formatting rules |
| `.dockerignore` | Excludes `target/`, `.git/`, `tests/fixtures/`, `docs/` |
| `.env.example` | Every variable from §4, with its default |
| `src/main.rs` | clap dispatch, tracing init, anyhow at the edge |
| `src/cli.rs` | `run`, `validate`, `publish`, `dump` subcommands, each a stub |
| `src/config.rs` | Parses every §4 variable, fails fast. Unit tests inline |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `DUNK_HOSTNAME` | missing | `config::load` returns `ConfigError::Missing("DUNK_HOSTNAME")`; `main.rs` prints it and exits 1 |
| BC2 | `DUNK_PUBLISHER_DID` | missing | Same path as BC1 |
| BC3 | `DUNK_K` | malformed, e.g. `"five"` | `ConfigError::Invalid{name, value, reason}`, caught in `src/config.rs` |
| BC4 | `DUNK_W_REPOST` | empty string | Treated as malformed; same error as BC3 |
| BC5 | `DUNK_FEED_RKEY` | unset | Falls back to the default `"dunks"`, no error |
| BC6 | CLI, no subcommand given | missing input | `clap` prints usage and exits non-zero |
| BC7 | `ConfigError` (new error type) | raised | Caught in `src/config.rs`; `main.rs` prints one line and exits 1, never panics |
| BC8 | Unknown environment variable, e.g. `DUNK_TYPO` | extra, unrecognised | Ignored; not an error |

## Acceptance criteria

- [ ] AC1 — All four gates pass. Checked by: `cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features && cargo build --release`
- [ ] AC2 — A missing `DUNK_HOSTNAME` fails fast with a clear message. Checked by: `cargo test config::tests::missing_required_var_fails`
- [ ] AC3 — A malformed `DUNK_K` fails fast with a clear message. Checked by: `cargo test config::tests::malformed_number_fails`
- [ ] AC4 — `.env.example` lists every variable from TECH-DESIGN §4. Checked by: reviewer reads `.env.example`
- [ ] AC5 — `dunk run` / `validate` / `publish` / `dump` each print their own name and exit 0. Checked by: `cargo test cli::tests::stub_subcommands_exit_zero`
- [ ] AC6 — `cargo test --all-features` runs at least one real test. Checked by: test runner output shows a non-zero test count

## Defaults taken

- CLI parser: `clap` with derive macros and subcommands, for free `--help`
  and a consistent shape for later stories.
- Numeric env vars parsed with `FromStr` (`f64`, `u32`), no config crate.
- Log format: `tracing_subscriber` with an `EnvFilter` built from `DUNK_LOG`,
  JSON output to stdout, matching TECH-DESIGN §13.
- Stub subcommands print their own name (e.g. `run`) to stdout and exit 0;
  no other behaviour until later stories fill them in.
- Config errors: a `thiserror` enum `ConfigError` with `Missing(&'static
  str)` and `Invalid{name, value, reason}` variants.
- `DUNK_DROP_LABELS` is parsed here as a comma-separated list into
  `Vec<String>`, trimmed, empty entries dropped. Story 10 reads it.
- `rust-toolchain.toml` pins the latest stable release at authoring time;
  bumped by hand later.

## Suggested slices

- 1.0 Toolchain and manifest — `Cargo.toml`, `rust-toolchain.toml`,
  `rustfmt.toml`, `.dockerignore`. Done when `cargo build` succeeds on an
  empty `main.rs`.
- 2.0 Config parser — `src/config.rs` with tests. Done when `cargo test
  config::tests` passes.
- 3.0 CLI dispatch — `src/cli.rs`, `src/main.rs`, `.env.example`. Done when
  all four AGENTS.md gates pass.
