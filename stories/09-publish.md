# 09 — `dunk publish`

- **Follows**: 08
- **PRD phase**: 2
- **Size**: small
- **Design**: docs/TECH-DESIGN.md §11.2

## Outcome

After this ships, `dunk publish` reads `BSKY_HANDLE`, `BSKY_APP_PASSWORD`,
`DUNK_HOSTNAME`, `DUNK_FEED_RKEY`, and an optional avatar path, creates a
session against `bsky.social`, uploads the avatar, and writes the
`app.bsky.feed.generator` record with `acceptsInteractions: true`. An
operator runs it once, and the feed appears in the Bluesky app under the
printed URL; running it again updates the same record in place.

## Non-goals

- Does not run inside `dunk run`; publish is a separate, manual step per
  §11.2.
- Does not create the account or the app password; the operator supplies
  both.
- Does not check that `DUNK_HOSTNAME` already serves
  `/.well-known/did.json`; that is the operator's responsibility first.
- Does not retry on failure; a failed call exits non-zero with the App
  View's own error.

## Approach

`publish.rs` reuses the same `reqwest::Client` shape as `appview/`, with a
session token attached, rather than a second HTTP client, so retry and
timeout behaviour stay consistent. `putRecord` is called unconditionally
rather than checked-then-written, because the lexicon guarantees it
overwrites, and an idempotent write is simpler than a read-modify-write.

## Files in scope

| Path | Change |
|---|---|
| `src/publish.rs` | Session, avatar upload, `putRecord`, prints the feed URL |
| `src/cli.rs` | Wires the `publish` subcommand to `publish::run`, adds `--avatar <path>` |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `BSKY_HANDLE` or `BSKY_APP_PASSWORD` | missing | Exits 1 with a clear message, before any network call; `run`, `validate`, and `dump` never read these two variables |
| BC2 | `--avatar` path | given but the file is missing | Exits 1 with a clear message, before any network call |
| BC3 | `--avatar` path | omitted | Record is published without an avatar field |
| BC4 | `createSession` | fails, bad handle or password | `PublishError::Auth`, caught in `src/publish.rs`; user sees the App View's error text |
| BC5 | `uploadBlob` | fails | `PublishError::Upload`, caught in `src/publish.rs`; publish aborts before `putRecord` |
| BC6 | `putRecord` | succeeds | Record written with `did: did:web:<DUNK_HOSTNAME>`, `displayName`, `description`, `avatar` (if any), `acceptsInteractions: true`, `createdAt`; prints `at://<publisher_did>/app.bsky.feed.generator/<DUNK_FEED_RKEY>` |
| BC7 | `publish` run twice, same `DUNK_FEED_RKEY` | idempotence | Second run overwrites the same record; no duplicate created |

## Acceptance criteria

- [ ] AC1 — Missing `BSKY_HANDLE`/`BSKY_APP_PASSWORD` exits 1 before any network call. Checked by: `cargo test publish::tests::missing_credentials_fails_fast`
- [ ] AC2 — A missing avatar file exits 1 before any network call. Checked by: `cargo test publish::tests::missing_avatar_fails_fast`
- [ ] AC3 — The record body matches BC6's shape. Checked by: `cargo test publish::tests::record_body_shape`
- [ ] AC4 — The printed URL matches the at-URI format. Checked by: `cargo test publish::tests::prints_at_uri`
- [ ] AC5 — All four gates pass.
- [ ] AC6 — A live publish against a real test account succeeds. Checked by: run by hand: `cargo test -- --ignored publish_live`

## Defaults taken

- `displayName` and `description` are compile-time constants in
  `publish.rs` (a short name and a one-line description), not env
  variables, since §4 lists none; changing them means editing one line.
- The avatar path is a CLI flag, `--avatar <path>`, not an env variable,
  since §4 lists none.
- The session is created against `https://bsky.social` (fixed), not
  `DUNK_APPVIEW_URL`, matching §11.2's exact wording.

## Suggested slices

- 1.0 `publish.rs` (session, avatar upload, `putRecord`, tests with
  recorded bodies) and `src/cli.rs` wiring together. Done when `cargo test
  publish` passes and all four gates pass.
