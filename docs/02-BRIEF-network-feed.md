# Brief: Network feed

Status: approved, 2026-09-24.
PRD: [02-PRD-network-feed.md](02-PRD-network-feed.md).

## Summary

The Upstaged feed becomes personal. The feed shows a pair only when the
viewer is connected to the original author or to the upstager. A
connection is a direct follow in either direction, or one step further
through an account that the viewer follows.

## Problem

Today every viewer gets the same global feed. Most pairs come from people
the viewer does not know. A pair is more interesting when the viewer knows
one of the authors. A pair from one step outside the viewer's follows
lets the viewer discover new people and content that is still close to
their taste.

## Rule

An author is a connected author when one or more of these statements is
true:

- The author follows the viewer.
- The viewer follows the author.
- The viewer follows an account that follows the author.

A pair passes the filter when the original author, the upstager, or both
are connected authors. The feed removes all other pairs. It does not rank
them lower.

The third statement is the only degree-2 connection. It goes in one
direction only: out from the viewer, then out again. Followers of the
viewer's followers are not connected authors, because that set grows
without limit and says little about the viewer's taste.

## Scope

In scope:

- The filter applies to the existing feed, for every subscriber. There is
  no second feed record and no global view after launch.
- The filter works only for logged-in viewers.
- Degree-2 connections through the accounts that the viewer follows.

Out of scope:

- Followers of the viewer's followers, and all other degree-2 paths.
- Degree 3 and more.
- A fallback to global pairs.
- Blocks, mutes and labels for each viewer. The App View continues to
  apply these, as [01-PRD.md](01-PRD.md) says.

## Behaviour

| Case | Result |
|------|--------|
| No JWT or a JWT that is not valid | Empty page |
| Valid JWT, graph not built yet | Empty page. The graph builds in the background. |
| Valid JWT, empty network | Empty page |
| Valid JWT, graph ready | The filtered feed |
| Fewer pairs than the floor on a slow day | Fewer items. No fallback. |

## Approach

These decisions are fixed. The tech design gives the details.

1. **"I follow them."** Get the viewer's full follows list with
   `getFollows`. A viewer who follows 500 accounts needs about 5 calls.
2. **"They follow me."** Call `getRelationships` only for the authors of
   the `UPSTAGE_FOLLOWS_ME_DEPTH` highest-ranked pairs (default 1000),
   30 authors per call. That is at most about 67 calls for each viewer,
   whatever the viewer's follower count. Deeper pairs do not get this
   check. Without this limit, the check would cover the full 7-day ranked
   list, which can be more than 1,000 calls.
3. **"I follow someone who follows them."** Degree 2 is sampled, not
   complete. Use the viewer's `UPSTAGE_D2_FOLLOWS_SAMPLE` most recent
   follows (default 100). For each of them, use their
   `UPSTAGE_D2_FOLLOWS_DEPTH` most recent follows (default 100). This is
   about 100 calls for each viewer before sharing. Some degree-2 authors
   are missed. No pair is shown in error.
4. **Shared follows cache.** An account's follows list is fetched once
   for all viewers and kept for `UPSTAGE_D2_REFRESH_AGE` (default 24
   hours). Viewers who follow the same accounts share the calls, so the
   cost for each viewer goes down as the number of viewers goes up.
5. **Authenticated calls.** Graph calls use the app-password session that
   `publish` already uses. Calls through the PDS are limited to 3,000 each
   5 minutes, about 10 each second. The public App View limit of 1 each
   second is too low: degree 1 alone costs about 3.5 calls each second at
   1000 viewers.
6. **New viewers.** The first request returns an empty page and puts the
   graph build in a queue. The build adds pairs in this order: "I follow
   them" as soon as the follows list is ready, then "they follow me", then
   degree 2. New-viewer lookups have priority over re-verification in the
   rate limiter.
7. **Module boundary.** A new `graph/` module owns the viewer graphs and
   the shared follows cache. A background task refreshes them through
   `appview/`. The HTTP handler only reads a cache. It never calls the App
   View.
8. **Filter order.** The network filter runs on the full ranked list
   before the diversity caps (one pair for each original author each day,
   one pair for each quoter in each 50 items). Then the caps apply to each
   viewer's list. The result for each viewer stays in a cache until the
   next snapshot swap.
9. **One rank.** Degree-1 and degree-2 pairs use the same global rank. No
   pair gets a bonus or a penalty for its type of connection.
10. **Refresh.** Refresh a viewer's degree-1 data when it is older than
    `UPSTAGE_GRAPH_REFRESH_AGE` (default 6 hours), and only while the
    viewer continues to send requests. Degree-2 data follows the shared
    cache age (item 4). Remove a graph after `UPSTAGE_GRAPH_IDLE_EVICT`
    (default 7 days) without requests.
11. **Cap.** Keep at most `UPSTAGE_MAX_VIEWERS` graphs (default 1000).
    When the cap is full, remove the least recently used graph and write a
    log line. That viewer gets an empty page until the next build.
12. **Privacy.** Do not write viewer DIDs to plain logs.

## Launch

Deploy with no warm-up. We do not know who the subscribers are, because
the store keeps no viewer DIDs. The first open for each subscriber is
empty. A refresh after a few seconds shows the filtered feed.

## Success metrics

| Metric | Target |
|--------|--------|
| Items per 24 hours for the median active viewer | 10 or more |
| Share of active viewers with 0 items in 24 hours | Track it. No target yet. |
| Discovery share: the part of a viewer's circle pairs that pass only through degree 2 | Track it. No target yet. |
| Distinct active viewers | Track it. This number is not known today. |

A low discovery share means degree 2 adds nothing. A high discovery
share means the feed stops being the viewer's circle. After one week of
data, set a target range, and decide if degree-2 pairs need a rank
penalty.

## Risks

- **Small feed.** The number of promoted pairs each day is not measured.
  About 1 in 12 popular quote posts qualifies. Degree 2 makes the circle
  larger, but the filter can still leave few items.
- **Budget.** The cost for each viewer is an estimate. The tech design
  must measure it and set the default for `UPSTAGE_MAX_VIEWERS` from the
  10 calls each second budget.
- **Memory.** Degree-2 sets can hold tens of thousands of DIDs for each
  viewer. The shared cache and the sample sizes limit this. The tech
  design must measure it on the target machine.
- **Sampling.** A viewer who follows many accounts gets degree 2 from
  their most recent follows only.
- **Current subscribers.** The global feed stops. Logged-out viewers and
  viewers with small networks will see an empty feed.

## Documents and rules to change

- `docs/01-PRD.md:144` says that the feed does not personalise. The new PRD
  replaces this rule.
- `docs/01-TECH-DESIGN.md:515` says that the service ignores the JWT. The
  tech design must change this.
- `docs/01-TECH-DESIGN.md` says that App View calls are not
  authenticated. Graph calls now use the app-password session.
- `AGENTS.md` names the callers of `appview/`. Add the `graph/` refresher.
- `Cache-Control: public, max-age=30` on `getFeedSkeleton` must change to
  `private`.

## Open items for the tech design

- JWT validation: DID resolution, signing keys, audience and expiry.
- Cursor stability when a viewer's graph refreshes during a scroll.
- The cost of filtering and capping for each viewer over 100k items at
  1000 viewers. Measure it.
- The calls and memory for each viewer, with and without the shared
  cache. Set the `UPSTAGE_MAX_VIEWERS` default from the result.
- The storage for graphs and the shared cache: SQLite through
  `src/store/`, or memory only.
- How the rate limiter gives priority to new-viewer lookups, and how
  authenticated graph calls and public verification calls share it.
