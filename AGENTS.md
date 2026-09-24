# AGENTS.md

Upstaged is one Rust binary. It consumes Jetstream, scores quote posts, and
serves an AT Protocol feed generator over HTTP. Read `docs/01-TECH-DESIGN.md`
before you change code. Read `docs/01-PRD.md` for the product rules.

## Prerequisites

- Rust stable toolchain through `rustup` (`cargo`, `rustfmt`, `clippy`).
- Docker, for the container build only.

## Gates

Every gate exits non-zero on failure. Run all four before a pull request.
A test run over zero tests is red.

| Gate | Command |
|------|---------|
| Format | `cargo fmt --all -- --check` |
| Lint | `cargo clippy --all-targets --all-features -- -D warnings` |
| Test | `cargo test --all-features` |
| Build | `cargo build --release` |

## Rules

- One crate, one binary. Modules live under `src/`. No workspace until a second binary exists.
- The ingest path talks to Jetstream only. It never calls the App View or any other HTTP API. Only `appview/` talks to the App View, and its callers are the scorer (`verify`, `guards`), `validate`, `publish`, `graph/` and `graph_probe`.
- `auth/` is the only module that calls the DID resolvers (`UPSTAGE_PLC_URL`, `did:web` hosts).
- Every constant from the PRD's score table lives in `src/config.rs` and is loaded from environment variables with the PRD default.
- SQLite is the only store. Access it through `src/store/`. No raw SQL outside that module.
- Local counters are an index, not the truth. A pair is promoted only on App View counts.
- Tests that need the network are `#[ignore]` and run by hand.
- Errors use `thiserror` in library modules and `anyhow` only in `main.rs`.
- No `unwrap()` outside tests. Use `?` or `expect("reason")` with a reason.
