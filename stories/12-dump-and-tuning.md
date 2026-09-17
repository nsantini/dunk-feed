# 12 — `dunk dump` and tuning notes

- **Follows**: 07
- **PRD phase**: 4
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §10, §16

## Outcome

After this ships, `dunk dump --since 24h` writes every pair with its local
and verified counts to CSV. An operator re-fits `P`, `M`, and the weights
offline against real traffic, then changes the matching env variables and
restarts, without touching code, closing the PRD's phase 4 tuning loop.

## Non-goals

- Does not automate the re-fitting; the CSV is read by hand or in a
  spreadsheet, no notebook and no Python ships here.
- Does not change any default constant; this story only exposes the data.
- Does not add a new HTTP route; `dump` is CLI-only, like `validate`.
- Does not re-verify dumped pairs; it reads what is already in SQLite.

## Approach

`dump` reads directly from `store::pairs` and `store::counts`, joined on
URI, rather than adding a reporting table, because the existing §6 schema
already carries everything the CSV needs. `--since` parses a duration like
`24h` with a small hand-written parser, not a duration-parsing crate,
since none is listed in §3 and the format is one number and one unit
suffix.

## Files in scope

| Path | Change |
|---|---|
| `src/dump.rs` | Duration parsing, CSV writing, row assembly (new file) |
| `src/cli.rs` | Wires the `dump` subcommand, `--since`, `--out` flags |
| `src/store/pairs.rs` | Adds a read query: pairs with `first_seen_at` since a cutoff, joined with `counts` |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `--since` value | valid, e.g. `24h`, `7d` | Parsed to a cutoff timestamp; only pairs at or after it are dumped |
| BC2 | `--since` value | malformed, e.g. `nope` or a missing unit | Exits 1 with a clear message; no partial CSV written |
| BC3 | `--since` value | omitted | Defaults to `24h` |
| BC4 | `--out` path | parent directory missing | Exits 1 with a clear message, before any query runs |
| BC5 | pair with no `counts` row on one or both sides | computed row | Written with `0` for that side's counts, not an error, matching §6's note that most `O` rows never exist |
| BC6 | pair state | `candidate`, `promoted`, or `dropped` | All three states included; `state` and `drop_reason` are CSV columns, so a re-fit can filter by outcome |
| BC7 | CSV row count | zero pairs match the window | Writes a header-only CSV, exits 0, not an error |
| BC8 | `DumpError` (new error type) | raised, wraps a parse or IO failure | Caught in `src/dump.rs`; user sees one line |

## Acceptance criteria

- [ ] AC1 — `--since 24h` includes only pairs from the last 24 hours. Checked by: `cargo test dump::tests::since_filters_correctly`
- [ ] AC2 — A malformed `--since` value exits 1 before touching the store. Checked by: `cargo test dump::tests::malformed_since_fails_fast`
- [ ] AC3 — Pairs with no `counts` row dump as zero, not an error. Checked by: `cargo test dump::tests::missing_counts_defaults_to_zero`
- [ ] AC4 — The CSV includes `state` and `drop_reason` columns. Checked by: `cargo test dump::tests::csv_has_state_and_reason`
- [ ] AC5 — Zero matching pairs still produces a valid header-only CSV. Checked by: `cargo test dump::tests::empty_window_writes_header_only`
- [ ] AC6 — All four gates pass.

## Defaults taken

- CSV columns: `quote_uri, original_uri, quote_did, original_did,
  quoted_at, state, drop_reason, likes_q, reposts_q, replies_q, likes_o,
  reposts_o, replies_o, local_e_q, local_e_o, local_d`. Local `E` and `D`
  are recomputed with `score.rs`, the same functions the service uses, not
  stored separately.
- `--since` grammar: an integer followed by `h` or `d` only, matching
  TECH-DESIGN's own example (`24h`). No other units.
- CSV is written with a manual writer, comma-joined fields with `"`
  escaping for commas and quotes; no `csv` crate, since none is listed in
  §3.
- Default `--out` path: `./dunk-dump-<since>.csv` in the working
  directory.

## Suggested slices

- 1.0 `dump.rs`: duration parsing, CSV row assembly, tests. Done when
  `cargo test dump` passes.
- 2.0 CLI wiring and the `store/pairs.rs` read query. Done when all four
  gates pass.
