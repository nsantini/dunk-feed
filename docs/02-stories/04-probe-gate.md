# 04 — Probe gate

- **Follows**: 03
- **PRD story**: Limit the number of stored circles (sets the measured defaults)
- **Size**: small
- **Design**: docs/02-TECH-DESIGN-network-feed.md §12, §7, §13, §17

## Outcome

After this ships, the tech design has a "Measured" subsection with real
numbers from about 10 handles. It gives the measured defaults for
`UPSTAGE_MAX_VIEWERS` and `UPSTAGE_GRAPH_RPS`. The team knows if the graph
design fits the call budget and the memory limit before story 06 starts.

This is an operator story. It has no code. A person runs the probe and
records the results. The acceptance criteria are manual checks.

## Non-goals

- Does not change code or `src/config.rs`. Story 06 writes the measured
  defaults into `src/config.rs`.
- Does not tune `UPSTAGE_FOLLOWS_ME_DEPTH` or the degree-2 sample sizes,
  unless the stop rule needs it.
- Does not deploy anything. `UPSTAGE_PERSONALISE` stays `false`.

## Approach

1. Choose about 10 handles: 3 or more that follow fewer than 200 accounts,
   3 or more that follow 200 to 1,000, and 3 or more that follow more than
   1,000.
2. Run the probe on the production host, against the production SQLite
   file, in one command, so the overlap numbers are real:
   `upstage graph-probe --handle <h1> ... --handle <h10>`.
3. Record the output in a new subsection "12.1 Measured" of
   `docs/02-TECH-DESIGN-network-feed.md`. Give the date, the commit, the
   number of feed rows and one row for each handle. Do not write DIDs.
4. Compute the calls for each viewer each day: 4 degree-1 refreshes plus
   the degree-2 refill after the measured overlap.
5. Set `UPSTAGE_MAX_VIEWERS` so that the total fits in 80 % of
   `UPSTAGE_GRAPH_RPS`. Set `UPSTAGE_GRAPH_RPS` at 8 or lower.
6. Estimate process memory at that viewer count with the measured bytes
   for each circle and each shared entry.
7. Apply the stop rule.

**Stop rule.** Stop and revise the design before story 06 when one of
these is true:

- The measured calls for each viewer each day do not fit the §7 budget at
  1000 viewers.
- The estimated memory at the new `UPSTAGE_MAX_VIEWERS` is more than
  450 MB (§13).
- The `sort=latest` check shows that `getFollows` does not return the
  newest follows first.

## Files in scope

| Path | Change |
|---|---|
| `docs/02-TECH-DESIGN-network-feed.md` | New subsection 12.1 "Measured". §4 defaults for `UPSTAGE_MAX_VIEWERS` and `UPSTAGE_GRAPH_RPS`. §7 and §13 estimates replaced with measured values |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | Handle set | selection | About 10 handles across the three follow-count bands |
| BC2 | "Measured" subsection | content | For each handle: follow count band, calls and time for each step, set sizes, circle bytes. For the run: overlap, bytes saved, viewer list time on 100,000 items, circle pairs in 24 hours, discovery share, order check result |
| BC3 | Defaults | result | `UPSTAGE_MAX_VIEWERS` and `UPSTAGE_GRAPH_RPS` in §4 match the computed values |
| BC4 | Stop rule | a condition is true | Story 06 does not start. The design is revised first |
| BC5 | Privacy | the document | Handles may appear. DIDs do not |

## Acceptance criteria

- [ ] AC1 — The probe ran on about 10 handles across the three bands. Checked by: manual review of subsection 12.1.
- [ ] AC2 — Subsection 12.1 holds every field in BC2. Checked by: manual review.
- [ ] AC3 — §4 gives the measured defaults for `UPSTAGE_MAX_VIEWERS` and `UPSTAGE_GRAPH_RPS`, with the calculation. Checked by: manual review.
- [ ] AC4 — The stop rule result is written as "pass" or "stop", with the reason. Checked by: manual review.
- [ ] AC5 — All four gates pass. (No code changes, so the gates stay green.)

## Defaults taken

- The 80 % headroom in step 5 leaves room for `publish` and for retries.
  The design gives no headroom figure.
- The operator edits the tech design in this story. This is the only
  story in the set that edits it.

## Suggested slices

- 1.0 Run the probe and record subsection 12.1. Done when AC1 and AC2
  pass.
- 2.0 Compute and write the defaults and the stop rule result. Done when
  AC3 and AC4 pass.
