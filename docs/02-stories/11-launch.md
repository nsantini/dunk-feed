# 11 — Launch

- **Follows**: 04, 10, 10b
- **PRD story**: See only pairs from my circle; Viewers without a valid login; Limit the number of stored circles
- **Size**: small
- **Design**: docs/02-TECH-DESIGN-network-feed.md §4, §10, §15, §16
- **Flag**: `UPSTAGE_PERSONALISE` (this story flips the default to `true`)

## Release

This story is the launch. After the deploy, `UPSTAGE_PERSONALISE` is
`true` when `.env` does not set it. Viewers get the network feed. The flag
no longer hides the feature. It is now the kill switch.

Before the deploy, the operator reads the story 04 stop rule result again.
The operator also sets `BSKY_HANDLE` and `BSKY_APP_PASSWORD` in `.env`.
Without them, `upstage run` does not start.

Rollback is `UPSTAGE_PERSONALISE=false` in `.env` and a restart with
`docker compose -f <file> up -d`. No deploy is necessary. The global feed
comes back, and the graph tables stay. A revert of the pull request also
brings back the `false` default, but it needs a deploy.

## Outcome

After this ships, the network feed is on by default.
`UPSTAGE_PERSONALISE` defaults to `true`. The service refuses to start
without `BSKY_HANDLE` and `BSKY_APP_PASSWORD` when the switch is `true`. A
regression test proves that `false` still serves the `01` global feed. The
rules, the example environment file, the runbook and the `01` design all
describe the new behaviour and the kill switch.

The command that does the credential check is `upstage run`.

## Non-goals

- Does not warm up circles before launch. The Brief says each first open
  is empty.
- Does not change any graph, auth or serving logic.
- Does not change the measured defaults from story 04.

## Approach

Change one default in `config.rs`. Move the credential check that
`publish` does today into a shared config check that `run` also calls when
the switch is `true`. Remove the story 06 "worker not started" branch,
because `run` now fails first. Add a regression test that builds the same
rows with the switch `false` and compares the response with the story 01
global output. Then update the documents.

## Files in scope

| Path | Change |
|---|---|
| `src/config.rs` | `UPSTAGE_PERSONALISE` default `true` |
| `src/ingest/mod.rs` | `run` fails before any task starts when the switch is `true` and a credential is missing |
| `src/http/skeleton.rs` | Regression test for the switch `false` |
| `.env.example` | All new variables from design §4, with defaults and one comment each |
| `docs/RUNBOOK.md` | New variables (including `UPSTAGE_RESOLVER_MISSES_PER_MIN`, `UPSTAGE_GRAPH_LRU_EVICT_PER_MIN` and `UPSTAGE_GRAPH_LRU_PROTECT_MIN` from story 10b), the kill switch procedure, the `graph-probe` command, the `graph.health`, `graph.evicted`, `auth.miss_limited` and `graph.lru_refused` lines |
| `docs/01-TECH-DESIGN.md` | One pointer line in §8 and one in §11.1 to `02-TECH-DESIGN-network-feed.md` |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `UPSTAGE_PERSONALISE` | not set | `true` |
| BC2 | `upstage run` | switch `true`, `BSKY_HANDLE` or `BSKY_APP_PASSWORD` missing or blank | Exits 1 before any network call, with the variable name in the message |
| BC3 | `upstage run` | switch `false`, credentials missing | Starts. Serves the global feed |
| BC4 | `validate`, `dump` | any switch | Do not read the credentials |
| BC5 | switch `false` | same rows | Response body, cursor and headers equal the story 01 global output |
| BC6 | kill switch | `false`, then a restart | JWT not read. Worker, scheduler, resolver and metrics not started. Graph tables stay |
| BC7 | `AGENTS.md` | `appview/` and DID resolver rules | Already written by stories 03 and 05. This story checks that they are still correct |
| BC8 | `RUNBOOK.md` | kill switch | Gives the steps: set `UPSTAGE_PERSONALISE=false`, restart, check `Cache-Control: public` on a request |

## Acceptance criteria

- [ ] AC1 — The switch defaults to `true`. Checked by: `cargo test config::tests::personalise_default_true`
- [ ] AC2 — `run` with the switch `true` and a missing credential exits 1 before any network call. Checked by: `cargo test ingest::tests::run_requires_credentials_when_personalised`
- [ ] AC3 — With the switch `false`, output equals the story 01 global output. Checked by: `cargo test http::skeleton::tests::switch_off_equals_global`
- [ ] AC4 — `AGENTS.md`, `.env.example`, `docs/RUNBOOK.md` and `docs/01-TECH-DESIGN.md` hold the changes in BC7, BC8 and Files in scope. Checked by: manual review.
- [ ] AC5 — All four gates pass. Checked by: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features` and `cargo build --release`.

## Defaults taken

- The deploy after this story is the launch. Before the deploy, the
  operator reads the story 04 stop rule result again.
- `.env.example` shows `UPSTAGE_PERSONALISE=true`, with a comment that
  `false` is the kill switch.

## Suggested slices

- 1.0 Default flip, credential check, regression test. Done when AC1 to
  AC3 pass.
- 2.0 Document updates. Done when AC4 passes and all four gates pass.

## Testing steps

1. Prepare the shell. Copy `.env.example` to `.env` and fill in the
   required values. Remove any `UPSTAGE_PERSONALISE` line. Put a copy of a
   database with feed rows at `./upstage.db`. Then run:

   ```
   export $(grep -v '^#' .env | xargs)
   export UPSTAGE_DB_PATH=./upstage.db
   FEED="at://$UPSTAGE_PUBLISHER_DID/app.bsky.feed.generator/$UPSTAGE_FEED_RKEY"
   SKEL="http://localhost:3000/xrpc/app.bsky.feed.getFeedSkeleton?feed=$FEED"
   ```

   Expected: The commands exit 0. `echo $SKEL` prints the feed URL.

2. Log in as a test viewer. Use an account on `bsky.social` that follows
   some authors in the feed.

   ```
   VIEWER_HANDLE=<test viewer handle>
   VIEWER_DID=$(curl -s "https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle=$VIEWER_HANDLE" | jq -r .did)
   ACCESS=$(curl -s -X POST https://bsky.social/xrpc/com.atproto.server.createSession \
     -H 'Content-Type: application/json' \
     -d "{\"identifier\":\"$VIEWER_HANDLE\",\"password\":\"<viewer app password>\"}" | jq -r .accessJwt)
   token() { curl -s -H "Authorization: Bearer $ACCESS" \
     "https://bsky.social/xrpc/com.atproto.server.getServiceAuth?aud=${1:-did:web:$UPSTAGE_HOSTNAME}&lxm=app.bsky.feed.getFeedSkeleton&exp=$(( $(date +%s) + ${2:-1800} ))" | jq -r .token; }
   TOKEN=$(token)
   ```

   Expected: `echo $VIEWER_DID` prints a DID. `echo $TOKEN` prints three
   parts with a dot between each part.

3. Start the service without credentials and with the flag not set.

   ```
   env -u UPSTAGE_PERSONALISE -u BSKY_HANDLE -u BSKY_APP_PASSWORD cargo run --release -- run
   ```

   Expected: The service exits 1 before any network call. The message
   names `BSKY_HANDLE` or `BSKY_APP_PASSWORD`.

4. Start the service with a blank handle.

   ```
   env -u UPSTAGE_PERSONALISE BSKY_HANDLE=' ' cargo run --release -- run
   ```

   Expected: The service exits 1. The message names `BSKY_HANDLE`.

5. Start the service with credentials and with the flag not set. Send a
   request with no token.

   ```
   env -u UPSTAGE_PERSONALISE cargo run --release -- run 2>&1 | tee run.log
   # in a second shell, after one scorer pass:
   curl -si "$SKEL&limit=30"
   ```

   Expected: The service starts. The body is `{"feed":[]}`. The header
   `Cache-Control: private, no-store` is present. The switch is `true` by
   default.

6. Build the viewer's circle.

   ```
   curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"; sleep 30; curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   ```

   Expected: The second response holds the viewer's pairs. The header
   `Cache-Control: private, no-store` is present.

7. Run `dump` without credentials.

   ```
   env -u BSKY_HANDLE -u BSKY_APP_PASSWORD cargo run --release -- dump --since 1h --out dump.csv
   ```

   Expected: The command exits 0 and writes `dump.csv`. It does not ask
   for credentials.

8. Turn on the kill switch. Stop the service. Start it with the flag off
   and no credentials.

   ```
   env -u BSKY_HANDLE -u BSKY_APP_PASSWORD UPSTAGE_PERSONALISE=false \
     cargo run --release -- run 2>&1 | tee run-off.log
   # in a second shell:
   curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   sqlite3 "$UPSTAGE_DB_PATH" "SELECT count(*) FROM viewers"
   ```

   Expected: The service starts. Status 200 with the global items. The
   header `Cache-Control: public, max-age=30` is present. The `viewers`
   count is above 0, so the graph tables stay.

9. Turn off the kill switch. Restart with the flag not set and with
   credentials. Send two requests, 5 seconds apart.

   ```
   env -u UPSTAGE_PERSONALISE cargo run --release -- run 2>&1 | tee run-2.log
   # in a second shell:
   TOKEN=$(token); curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"; sleep 5; curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   ```

   Expected: The second response holds the viewer's pairs. The circles
   loaded again from SQLite.

10. Check the documents.

    ```
    grep -n UPSTAGE_PERSONALISE .env.example
    grep -nE 'graph-probe|graph.health|graph.evicted|auth.miss_limited|graph.lru_refused|UPSTAGE_PERSONALISE=false' docs/RUNBOOK.md
    grep -n 02-TECH-DESIGN-network-feed docs/01-TECH-DESIGN.md
    ```

    Expected: `.env.example` shows `UPSTAGE_PERSONALISE=true` with a
    comment about the kill switch. The runbook names each term. The `01`
    design has two pointer lines.
