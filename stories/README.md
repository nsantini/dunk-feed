# Stories

Each file is one implementation story for the `workflow-plan` →
`workflow-slice` → `workflow-execute` plugin. The order matches
[docs/TECH-DESIGN.md](../docs/TECH-DESIGN.md) §15, the PRD's phases with
phase 0 first.

## Order and dependencies

| # | Story | PRD phase | Follows |
|---|---|---|---|
| 01 | [Crate scaffold, config, CLI](01-scaffold.md) | 0 | none |
| 02 | [Score module and App View client](02-score-and-appview.md) | 0 | 01 |
| 03 | [`upstage validate` phase 0 tool](03-validate-cli.md) | 0 | 02 |
| 04 | [Jetstream v2 client](04-jetstream-client.md) | 1 | 01 |
| 05 | [SQLite store, schema, writer](05-store.md) | 1 | 01 |
| 06 | [Ingest task](06-ingest.md) | 1 | 02, 04, 05 |
| 07 | [Scorer task](07-scorer.md) | 1 | 02, 05, 06 |
| 08 | [HTTP serving](08-http-serving.md) | 2 | 07 |
| 09 | [`upstage publish`](09-publish.md) | 2 | 08 |
| 10 | [Guards](10-guards.md) | 3 | 07 |
| 11 | [Docker, Compose, Cloudflare, runbook](11-docker-ops.md) | 2 | 08 |
| 12 | [`upstage dump` and tuning notes](12-dump-and-tuning.md) | 4 | 07 |

**Stop after story 03 and read the output.** TECH-DESIGN §15's kill point:
if the top 30 pairs from `upstage validate` are not quote posts that clearly
out-engaged their original, change the score before writing the ingest
pipeline. Tone does not matter (TECH-DESIGN §1, D10). Done 2026-09-18, the
score stood.

Run stories in number order. A story only starts once every story in its
`Follows` list is merged, because it reads or extends files those stories
created.

## Running one story through the plugin

Feed each story file to the workflow plugin, one at a time, as a free-text
task description:

```
/workflow "Implement stories/01-scaffold.md"
```

Repeat for `stories/02-score-and-appview.md`, and so on in order.

A few things to know before you run the first one:

- **Ticket**: none. A free-text argument makes the plugin set `Ticket: none`
  on its own. It will not ask.
- **Task id**: the plugin builds its own id as `<date>-<slug>`, for example
  `2026-09-18-scaffold`. You do not set this by hand.
- **No GitHub remote yet.** This repository has no remote, so
  `workflow-ship`, the step that opens a pull request, will not work until
  a remote exists. Run `workflow-plan` → `workflow-slice` →
  `workflow-execute` for each story, commit locally, and hold off on
  `workflow-ship` until a GitHub remote is added.
- **Gates**: the plugin reads AGENTS.md for the four gate commands
  (format, lint, test, build). Every story's acceptance criteria already
  name these commands, so `workflow-plan` should not need to ask.
