# Brief: Network feed

Status: approved, 2026-09-24.
PRD: [02-PRD-network-feed.md](02-PRD-network-feed.md).

## Summary

The Upstaged feed becomes personal. The feed shows a pair only when the
viewer and one of the two authors are connected. The two authors are the
original author and the upstager. A connection is a follow in either
direction.

## Problem

Today every viewer gets the same global feed. Most pairs come from people
the viewer does not know. A pair is more interesting when the viewer knows
one of the authors.

## Rule

A pair passes the filter when one or more of these statements is true:

- The viewer follows the original author.
- The viewer follows the upstager.
- The original author follows the viewer.
- The upstager follows the viewer.

The feed removes all other pairs. It does not rank them lower.

The network is degree 1 only. Degree 2 (the accounts that your follows
follow) is out of scope. Degree 1 keeps each graph refresh small and fast.

## Scope

In scope:

- The filter applies to the existing feed, for every subscriber. There is
  no second feed record and no global view after launch.
- The filter works only for logged-in viewers.

Out of scope:

- Degree 2 connections.
- A fallback to global pairs.
- Blocks, mutes and labels for each viewer. The App View continues to
  apply these, as the current PRD says.

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
   whatever the viewer's follower count. Deeper pairs pass only through
   "I follow them". Without this limit, the check would cover the full
   7-day ranked list, which can be more than 1,000 calls.
3. **New viewers.** The first request returns an empty page and puts the
   graph build in a queue. The build fetches the follows list first and
   serves "I follow them" pairs as soon as it has that list. "They follow
   me" pairs are added as the checks complete. New-viewer lookups have
   priority over re-verification in the App View rate limiter.
4. **Module boundary.** A new `graph/` module owns the viewer graphs. A
   background task refreshes them through `appview/`. The HTTP handler
   only reads a cache. It never calls the App View.
5. **Filter order.** The network filter runs on the full ranked list
   before the diversity caps (one pair for each original author each day,
   one pair for each quoter in each 50 items). Then the caps apply to each
   viewer's list. The result for each viewer stays in a cache until the
   next snapshot swap.
6. **Refresh.** Refresh a graph when it is older than 6 hours, and only
   while the viewer continues to send requests. Remove a graph after
   7 days without requests.
7. **Cap.** Keep at most `UPSTAGE_MAX_VIEWERS` graphs (default 1000).
   When the cap is full, remove the least recently used graph and write a
   log line. That viewer gets an empty page until the next build.
8. **Privacy.** Do not write viewer DIDs to plain logs.

## Launch

Deploy with no warm-up. We do not know who the subscribers are, because
the store keeps no viewer DIDs. The first open for each subscriber is
empty. A refresh after a few seconds shows the filtered feed.

## Success metrics

| Metric | Target |
|--------|--------|
| Items per 24 hours for the median active viewer | 10 or more |
| Share of active viewers with 0 items in 24 hours | Track it. No target yet. |
| Distinct active viewers | Track it. This number is not known today. |

When many viewers get 0 items, degree 1 is too narrow. That is the
evidence to add degree 2.

## Risks

- **Small feed.** The number of promoted pairs each day is not measured.
  About 1 in 12 popular quote posts qualifies. A degree-1 filter on top of
  that can leave very few items.
- **Viewer count.** The cap of 1000 is a guess. At 1 request per second,
  the App View limit can be too low for more viewers. The fixes are to
  increase `UPSTAGE_APPVIEW_RPS` or to use authenticated calls.
- **Current subscribers.** The global feed stops. Logged-out viewers and
  viewers with small networks will see an empty feed.

## Documents and rules to change

- `docs/01-PRD.md:144` says that the feed does not personalise. The new PRD
  replaces this rule.
- `docs/01-TECH-DESIGN.md:515` says that the service ignores the JWT. The
  tech design must change this.
- `AGENTS.md` names the callers of `appview/`. Add the `graph/` refresher.
- `Cache-Control: public, max-age=30` on `getFeedSkeleton` must change to
  `private`.

## Open items for the tech design

- JWT validation: DID resolution, signing keys, audience and expiry.
- Cursor stability when a viewer's graph refreshes during a scroll.
- The cost of filtering and capping for each viewer over 100k items at
  1000 viewers. Measure it.
- The storage for graphs: SQLite through `src/store/`, or memory only.
- How the rate limiter gives priority to new-viewer lookups.
