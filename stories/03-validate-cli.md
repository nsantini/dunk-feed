# 03 — `upstage validate` phase 0 tool

- **Follows**: 02
- **PRD phase**: 0
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §10, §12 (D5)

## Outcome

After this ships, `upstage validate` seeds candidate pairs from `hot-classic`
(or a supplied list of quote URIs), verifies them against the App View, and
prints a table sorted by rank, plus a CSV, using the exact `score.rs` the
service ships with. An engineer runs it, reads the top 30 by hand, and if
they are not funny, kills the project before writing the ingest pipeline —
TECH-DESIGN §10's and §15's phase-0 kill point, costing one afternoon
instead of a service.

## Non-goals

- Does not run continuously or serve HTTP.
- Does not write to SQLite; the store ships in story 05.
- Does not seed from popular originals, the PRD's own recipe.
  traffic-analysis §6 finding 2 shows that finds nothing (D5).
- Does not apply the follower-floor guard or any other guard; story 10.
- Does not write its own quote detection. It calls `ingest::embed::detect`
  from story 02 on each `post.record` value that `getFeed` returns.

## Approach

`validate` seeds from `getFeed(hot-classic)` quote posts, not heavily-quoted
originals, because traffic-analysis §6 found zero pairs the PRD's way and
ten of forty-seven the other way (D5). It fetches originals in batches of 25
through the same `appview` client the service uses, not a separate script,
so the formula under test is the formula that ships. Output is a printed
table and a CSV, not a database, because phase 0 needs no persistence.

## Files in scope

| Path | Change |
|---|---|
| `src/validate.rs` | Phase 0 probe: seed, fetch, score, print, CSV |
| `src/cli.rs` | Wires the `validate` subcommand to `validate::run`, with `--pages`, `--seed-file`, `--csv-path` flags |
| `tests/fixtures/hot_classic_page.json` | Recorded `getFeed(hot-classic)` page |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `Q.embed`, `app.bsky.embed.record#view`, `record.$type == #viewRecord` | quote view | Included as a candidate; original taken from `record.uri` / `record.author.did` |
| BC2 | `Q.embed`, `app.bsky.embed.recordWithMedia#view`, nested `record.record.$type == #viewRecord` | quote view with media | Same handling as BC1 |
| BC3 | `Q.embed`, images, external, none, video, or `gallery` | not a quote | Excluded, not an error |
| BC4 | `quote_did == original_did` | self quote | Excluded, printed with reason `self_quote` |
| BC5 | `getPosts` batch, a requested URI missing | quote or original gone | Pair still printed, with reason `quote_gone` or `original_gone`, not silently dropped |
| BC6 | `--seed-file` line | malformed, not a valid `at://` URI | Skipped, one warning to stderr, run continues |
| BC7 | `--seed-file` | empty file | Falls back to `hot-classic` seeding with a note to stderr |
| BC8 | table and CSV rows | ordering | Sorted by `rank` descending, ties broken by `quote_cid` ascending |
| BC9 | `--csv-path` | parent directory missing | `ValidateError::CsvWrite`, caught in `src/validate.rs`; user sees one line, exits 1 |

## Acceptance criteria

- [ ] AC1 — Non-quote embeds are excluded. Checked by: `cargo test validate::tests::excludes_non_quote_embeds`
- [ ] AC2 — Self quotes are excluded. Checked by: `cargo test validate::tests::excludes_self_quote`
- [ ] AC3 — Rows sort by rank desc, tie-break by `quote_cid` asc. Checked by: `cargo test validate::tests::sorts_by_rank_then_cid`
- [ ] AC4 — CSV output matches the printed table. Checked by: `cargo test validate::tests::csv_matches_table`
- [ ] AC5 — A malformed seed-file line is skipped with a warning, run continues. Checked by: `cargo test validate::tests::skips_malformed_seed_line`
- [ ] AC6 — All four gates pass.
- [ ] AC7 — A live run against `hot-classic` succeeds and prints a table. Checked by: run by hand: `cargo test -- --ignored validate_live_hot_classic`

## Defaults taken

- Default `--pages` is 3 (300 posts), matching §10.
- Default `--csv-path` is `./upstage-validate.csv` in the working directory.
- `--seed-file` format: one `at://` URI per line; blank lines and `#`
  comments are skipped.
- Quote-view detection is written locally in `validate.rs`, against the
  `getFeed` `postView` shape, and is not shared with later stories (see
  Non-goals).
- Table columns: bsky.app links for `Q` and `O`, `E(Q)`, `E(O)`, `D`, and
  which gate each pair passed or failed, per §10 step 4.

## Suggested slices

- 1.0 Seeding and the quote-view check, with score integration and fixture
  tests. Done when `cargo test validate::tests` passes.
- 2.0 CLI wiring, CSV writer, seed-file parsing. Done when all four gates
  pass and a live run (by hand) prints a table.
