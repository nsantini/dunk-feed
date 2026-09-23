# 11 — Launch

- **Follows**: 04, 10
- **PRD story**: See only pairs from my circle; Viewers without a valid login; Limit the number of stored circles
- **Size**: small
- **Design**: docs/02-TECH-DESIGN-network-feed.md §4, §10, §15, §16

## Outcome

After this ships, the network feed is on by default.
`UPSTAGE_PERSONALISE` defaults to `true`. `upstage run` refuses to start
without `BSKY_HANDLE` and `BSKY_APP_PASSWORD` when the switch is `true`. A
regression test proves that `false` still serves the `01` global feed. The
rules, the example environment file, the runbook and the `01` design all
describe the new behaviour and the kill switch.

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
| `docs/RUNBOOK.md` | New variables, the kill switch procedure, the `graph-probe` command, the `graph.health` and `graph.evicted` lines |
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
- [ ] AC5 — All four gates pass.

## Defaults taken

- The deploy after this story is the launch. Before the deploy, the
  operator reads the story 04 stop rule result again.
- `.env.example` shows `UPSTAGE_PERSONALISE=true`, with a comment that
  `false` is the kill switch.

## Suggested slices

- 1.0 Default flip, credential check, regression test. Done when AC1 to
  AC3 pass.
- 2.0 Document updates. Done when AC4 passes and all four gates pass.
