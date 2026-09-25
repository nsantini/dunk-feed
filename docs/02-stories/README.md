# Stories: network feed

Each file is one implementation story for the `workflow-plan` →
`workflow-slice` → `workflow-execute` plugin. The set implements
[docs/02-TECH-DESIGN-network-feed.md](../02-TECH-DESIGN-network-feed.md).
The order replaces the table in that document's §17. It splits the work
into vertical slices and puts the probe gate before the graph work.

## Order and dependencies

| # | Story | PRD story | Follows | Flag |
|---|---|---|---|---|
| 01 | [Snapshot carries authors, caps become reusable](01-snapshot-authors-and-caps.md) | none (prefactor) | none | none |
| 02 | [Shared PDS session with refresh](02-pds-session.md) | none (prefactor) | none | none |
| 03 | [Graph probe](03-graph-probe.md) | Limit the number of stored circles | 02 | none |
| 04 | [Probe gate](04-probe-gate.md) | Limit the number of stored circles | 03 | none |
| 05 | [Viewer identity behind the switch](05-viewer-identity.md) | Viewers without a valid login | none | `UPSTAGE_PERSONALISE` (default `false`) |
| 06 | ["I follow" circle end to end](06-i-follow-circle.md) | See only pairs from my circle; First open builds my circle; Scroll without repeats | 01, 04, 05 | `UPSTAGE_PERSONALISE` (default `false`) |
| 07 | ["Follows me" connections](07-follows-me.md) | See only pairs from my circle; First open builds my circle | 06 | `UPSTAGE_PERSONALISE` (default `false`) |
| 08 | [Degree 2 with a shared follows cache](08-degree-two.md) | Discover people one step out; First open builds my circle | 06 | `UPSTAGE_PERSONALISE` (default `false`) |
| 09 | [Circles stay current and bounded](09-circles-current-and-bounded.md) | My circle stays current; Limit the number of stored circles | 07, 08 | `UPSTAGE_PERSONALISE` (default `false`) |
| 10 | [Feed health metrics](10-feed-health-metrics.md) | Read feed health | 09 | `UPSTAGE_PERSONALISE` (default `false`) |
| 10b | [Launch blockers](10b-launch-blockers.md) | Viewers without a valid login; Limit the number of stored circles | 05, 09, 10 | `UPSTAGE_PERSONALISE` (default `false`) |
| 11 | [Launch](11-launch.md) | See only pairs from my circle; Viewers without a valid login | 04, 10, 10b | `UPSTAGE_PERSONALISE` (flips default to `true`) |

**Stop after story 04 and read the result.** Story 04 is the probe gate.
If the measured calls for each viewer do not fit the budget in design §7,
if the memory estimate is more than 450 MB (§13), or if `getFollows` does
not return the newest follows first, do not start story 06. Revise the
design first.

A story starts only when every story in its `Follows` list is merged.

## Rollout rule

`UPSTAGE_PERSONALISE` defaults to `false` in stories 05 to 10. Design §4
gives `true`. Story 11 flips the default. With `false`, each story merges
and deploys with the global feed unchanged. To test a story by hand, set
`UPSTAGE_PERSONALISE=true` in the local environment.

## Parallel work

- Stories 01, 02 and 05 have no dependencies. They can start at the same
  time.
- Story 03 can start when 02 is merged, while 01 and 05 continue.
- Stories 07 and 08 can run at the same time after 06. Both change
  `src/graph/queue.rs`, `src/graph/circle.rs` and `src/http/viewer.rs`.
  The second one to merge rebases on the first.

## Running one story through the plugin

Feed each story file to the workflow plugin, one at a time, as a free-text
task description:

```
/workflow "Implement docs/02-stories/01-snapshot-authors-and-caps.md"
```

Repeat for each story, in an order that respects `Follows`.

Before you run the first one:

- **Ticket**: none. A free-text argument makes the plugin set
  `Ticket: none`. It does not ask.
- **Task id**: the plugin builds its own id as `<date>-<slug>`, for
  example `2026-09-25-snapshot-authors-and-caps`.
- **Gates**: the plugin reads `AGENTS.md` for the four gate commands. Each
  story's acceptance criteria name them.
- **Story 04** has no code. Do it by hand. Do not run it through the
  plugin.
- **Network tests** are `#[ignore]`. Run them by hand with
  `cargo test -- --ignored <name>`.
