# 05 — SQLite store, schema, writer, checkpoint

- **Follows**: 01
- **PRD phase**: 1
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §5.4, §6

## Outcome

After this ships, `store::Store` opens a SQLite file at `DUNK_DB_PATH` in
WAL mode, creates the versioned schema from TECH-DESIGN §6 if it is
missing, and runs a single writer thread that commits batched `Op`s and the
Jetstream `seq` in one transaction. A crash and restart resumes from the
committed `seq` with at most one event replayed. Every table, index, and
column in §6 exists exactly as specified, so ingest, the scorer, and HTTP
have one place to read and write data.

## Non-goals

- Does not decide what an `Op` means for a Jetstream event; the event-to-op
  mapping is story 06.
- Does not run the scorer pass, verify, or promote anything.
- Does not serve HTTP.
- Does not use an ORM or raw SQL outside `src/store/`; AGENTS.md forbids it.

## Approach

The writer is one dedicated thread with a channel, not `async` SQLite,
because `rusqlite` is synchronous and TECH-DESIGN §3 lists no async SQLite
crate. Batches commit every 500 ms or 1,000 ops, whichever comes first, with
the `seq` update in the same transaction, per §5.4, so counters and the
cursor never drift apart even on a crash. The schema is versioned with a
`schema_version` row in `meta`, applied with plain `CREATE TABLE`
statements in `store/schema.rs`, not a migration framework, because the
schema in §6 is small and stable.

## Files in scope

| Path | Change |
|---|---|
| `src/store/mod.rs` | `Store` handle, open, migrate |
| `src/store/schema.rs` | `CREATE TABLE` statements, versioned, the full §6 schema |
| `src/store/writer.rs` | Single writer thread, `Op` enum, batch commit |
| `src/store/pairs.rs` | Reads and writes on `pairs` |
| `src/store/counts.rs` | Reads and writes on `counts` |
| `src/store/feed.rs` | Reads and writes on `feed` |
| `src/store/authors.rs` | Reads and writes on `authors` |
| `src/store/meta.rs` | Reads and writes on `meta` |
| `src/store/interactions.rs` | Insert-only writes on `interactions` |
| `Cargo.toml` | Adds `rusqlite` (`bundled`) |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `pairs` table | schema | `quote_uri TEXT PK`, `quote_did`, `quote_cid`, `original_uri`, `original_did NOT NULL`, `quoted_at`, `first_seen_at INTEGER NOT NULL`, `state TEXT NOT NULL DEFAULT 'candidate'` (`candidate\|promoted\|dropped`), `drop_reason TEXT` nullable. Indexes: `pairs_original(original_uri)`, `pairs_state_seen(state, first_seen_at)` |
| BC2 | `counts` table | schema | `post_uri TEXT PK`, `likes`/`reposts`/`replies INTEGER NOT NULL DEFAULT 0`, `last_event_at INTEGER NOT NULL`, `dirty INTEGER NOT NULL DEFAULT 1`. Index: `counts_dirty(dirty) WHERE dirty = 1` |
| BC3 | `feed` table | schema | `quote_uri TEXT PK REFERENCES pairs(quote_uri)`, `quote_cid`, `quote_did`, `original_did`, `quoted_at NOT NULL`, six nullable `v_*_q`/`v_*_o` `INTEGER` count columns, `ratio REAL NOT NULL`, `rank REAL NOT NULL`, `promoted_at`, `verified_at INTEGER NOT NULL`. Index: `feed_rank(rank DESC)` |
| BC4 | `authors` table | schema | `did TEXT PK`, `followers INTEGER` nullable, `active INTEGER NOT NULL` (0 = deactivated, deleted, or taken down), `labels TEXT` nullable JSON array, `checked_at INTEGER NOT NULL` |
| BC5 | `interactions` table | schema | `received_at INTEGER NOT NULL`, `item`, `event`, `feed_context`, `req_id TEXT`; no primary key, insert-only |
| BC6 | `meta` table | schema | `key TEXT PK`, `value TEXT NOT NULL`; keys: `schema_version`, `jetstream_seq`, `zstd_dict_id`, `last_scorer_pass` |
| BC7 | writer, one batch | commit | Every `Op` in the batch and the `meta.jetstream_seq` update run in one `BEGIN ... COMMIT` transaction |
| BC8 | writer, process crash mid-batch | crash | Uncommitted ops are lost; `jetstream_seq` stays at its last committed value |
| BC9 | resume after restart | inclusive `seq` | At most one event is replayed; the caller (story 06) reconnects at `seq + 1` |
| BC10 | `InsertPair` | duplicate, same `quote_uri` twice | Idempotent; the second insert is a no-op, not an error |
| BC11 | `Incr` | no existing `counts` row for the target URI | A row is created lazily on first event, not on pair insert, matching §6's note that most `O` rows never exist |
| BC12 | `StoreError` (new error type) | raised on open, migration, or writer-channel failure | Caught in `src/store/mod.rs` or `src/store/writer.rs`; surfaced to `main.rs` as `anyhow` |
| BC13 | `DUNK_DB_PATH` | parent directory missing | `StoreError::Open`; `main.rs` exits 1 |
| BC14 | `schema_version` on open | missing | Runs the initial migration, sets it to 1 |
| BC15 | `schema_version` on open | present, equal to 1 | No-op |
| BC16 | `schema_version` on open | present, greater than 1 (a future version this build does not know) | Open fails with `StoreError::UnknownSchemaVersion` |

## Acceptance criteria

- [ ] AC1 — Every table and index from §6 exists. Checked by: `cargo test store::schema::tests::matches_design_schema`
- [ ] AC2 — The writer commits a batch and the seq in one transaction. Checked by: `cargo test store::writer::tests::seq_commits_with_batch`
- [ ] AC3 — A crash mid-batch loses uncommitted ops; the seq stays unchanged. Checked by: `cargo test store::writer::tests::partial_batch_not_committed`
- [ ] AC4 — Resume replays at most one event. Checked by: `cargo test store::writer::tests::resume_is_inclusive`
- [ ] AC5 — A duplicate `InsertPair` is idempotent. Checked by: `cargo test store::pairs::tests::insert_pair_idempotent`
- [ ] AC6 — A `counts` row is created lazily on first `Incr`. Checked by: `cargo test store::counts::tests::lazy_row_creation`
- [ ] AC7 — A newer unknown `schema_version` fails to open. Checked by: `cargo test store::mod::tests::rejects_newer_schema_version`
- [ ] AC8 — All four gates pass.

## Defaults taken

- SQLite pragmas: `journal_mode=WAL`, `synchronous=NORMAL`,
  `cache_size=-64000` (64 MB), one writer connection.
- The channel from the async ingest task to the writer thread is
  `tokio::sync::mpsc::channel` (bounded, capacity 4,096); the writer thread
  drains it with `blocking_recv()`, so no extra channel crate is needed.
- `Op` enum: `InsertPair`, `Incr{uri, likes, reposts, replies deltas}`,
  `DeletePost{uri}`, `Detach{quote_uri}`. The batch's `seq` update is
  applied by the writer after the batch's ops, not sent as its own `Op`.
- All non-live tests use an in-memory SQLite (`:memory:`), per §14.
- `schema_version` starts at 1; an unrecognised value is a hard open error,
  with no downgrade path.
- `authors.active` is stored as `INTEGER` 0 or 1, since SQLite has no
  `BOOLEAN` type.

## Suggested slices

- 1.0 `schema.rs` (the full §6 schema) and `mod.rs` open and migrate. Done
  when `cargo test store::schema` passes.
- 2.0 `writer.rs`, the `Op` enum, and batch commit with the seq in the same
  transaction. Done when `cargo test store::writer` passes.
- 3.0 `pairs.rs`, `counts.rs`, `feed.rs`, `authors.rs`, `meta.rs`,
  `interactions.rs`. Done when `cargo test store` passes and all four gates
  pass.
