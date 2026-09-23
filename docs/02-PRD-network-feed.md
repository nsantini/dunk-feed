# PRD: Network feed

**Date**: 2026-09-24
**Status**: Draft
**Brief**: [02-BRIEF-network-feed.md](02-BRIEF-network-feed.md)
**Replaces in [01-PRD.md](01-PRD.md)**: the "Auth is optional here" rule in
the serving contract. The feed now personalises.

---

## Overview

The Upstaged feed shows each logged-in viewer only the pairs where the
original author or the upstager is connected to the viewer. See the
[Brief](02-BRIEF-network-feed.md).

## Terms

- **Pair**: a quote post and the post it quotes, as defined in
  [01-PRD.md](01-PRD.md).
- **Connected author**: an author that follows the viewer, an author
  that the viewer follows, or an author that is followed by an account
  that the viewer follows.
- **Degree-2 pair**: a circle pair that passes only through the third
  kind of connection.
- **Circle pair**: a pair where the original author, the upstager, or
  both are connected authors.
- **Active viewer**: a viewer who sent one or more feed requests in the
  last 7 days.

## User stories

### See only pairs from my circle
**As a** logged-in subscriber
**I want to** see only circle pairs in the Upstaged feed
**So that** every item involves someone in or near my network

**Acceptance criteria**:
- [ ] Each item in the feed is a circle pair for the viewer who asked
      for it.
- [ ] A pair is in the feed when the original author or the upstager
      follows the viewer, is followed by the viewer, or is followed by an
      account that the viewer follows.
- [ ] A pair is not in the feed when its only link to the viewer is an
      author who follows one of the viewer's followers.
- [ ] Two viewers with different follows get different feeds for the
      same request at the same time.
- [ ] Circle pairs keep the global rank order from
      [01-PRD.md](01-PRD.md). Degree-2 pairs get no bonus and no
      penalty.
- [ ] The diversity caps (one pair for each original author each day,
      one pair for each quoter in each 50 items) apply to the viewer's
      circle pairs, not to the global list. A circle pair is never
      dropped because a pair outside the circle used its slot.
- [ ] A circle pair that qualifies only because the author follows the
      viewer is included when it is in the 1,000 highest-ranked pairs.
      Deeper in the list, this connection is not checked.
- [ ] When the viewer has fewer circle pairs than a full page, the feed
      shows only those pairs. It never adds pairs from outside the
      circle.

**Notes**: Regression risk. This removes the global feed for every
current subscriber. The depth of 1,000 for "follows me" is set with
`UPSTAGE_FOLLOWS_ME_DEPTH`.

---

### Discover people one step out
**As a** logged-in subscriber
**I want to** see pairs from accounts that the people I follow follow
**So that** I find new people and content close to my taste

**Acceptance criteria**:
- [ ] The feed includes degree-2 pairs, at every depth of the ranked
      list.
- [ ] Degree 2 uses the viewer's 100 most recent follows, and the 100
      most recent follows of each of those accounts.
- [ ] Each degree-2 pair in the feed is correct: the viewer follows an
      account that follows one of the pair's authors. Some degree-2
      pairs can be missing. None is shown in error.
- [ ] When two viewers follow the same account, that account's follows
      list is fetched once for both of them in each 24 hours.

**Notes**: The sample sizes are set with `UPSTAGE_D2_FOLLOWS_SAMPLE` and
`UPSTAGE_D2_FOLLOWS_DEPTH`.

---

### First open builds my circle
**As a** subscriber who opens the feed for the first time after launch
**I want to** see my circle pairs quickly
**So that** the feed does not look broken

**Acceptance criteria**:
- [ ] The first request from a new viewer returns an empty page in less
      than 300 ms.
- [ ] Pairs with an author that the viewer follows are in the feed
      within 15 seconds of the first request, when the viewer follows
      1,000 accounts or fewer.
- [ ] Pairs that qualify only because an author follows the viewer are
      added after that, then degree-2 pairs, as the checks complete.
      Items that are already in the feed stay in the feed.
- [ ] A new viewer's first build starts before any re-verification work
      that is waiting.

**Notes**: There is no warm-up before launch. Each current subscriber's
first open is empty.

---

### My circle stays current
**As a** logged-in subscriber
**I want to** see changes to my follows reflected in the feed
**So that** the feed matches my network today

**Acceptance criteria**:
- [ ] After the viewer follows an account, pairs with that account are
      in the feed within 6 hours, if the viewer continues to use the
      feed.
- [ ] After the viewer unfollows an account, pairs that were connected
      only through that account leave the feed within 6 hours.
- [ ] When a new author enters the ranked list and that author follows
      the viewer, the pair is in the feed within 6 hours.
- [ ] When an account that the viewer follows starts to follow an
      author, degree-2 pairs with that author are in the feed within 30
      hours (one shared cache age plus one refresh age).
- [ ] A viewer with no requests for 7 days has no stored circle. Their
      next request is handled as a first open.

---

### Scroll without repeats
**As a** logged-in subscriber
**I want to** scroll through my feed without repeated items
**So that** the feed feels reliable

**Acceptance criteria**:
- [ ] During one scroll, the viewer never sees the same item twice, also
      when their circle refreshes during the scroll.
- [ ] During one scroll, the feed never ends before the viewer's last
      circle pair because their circle changed.
- [ ] A cursor from one viewer does not give a different viewer access
      to the first viewer's circle pairs.

**Notes**: Regression risk. Today one cursor is valid for all viewers.

---

### Viewers without a valid login
**As a** viewer who is logged out, or whose login token is not valid
**I want to** get a clean empty feed
**So that** the app does not show an error

**Acceptance criteria**:
- [ ] A request with no token returns an empty page, with no error
      status.
- [ ] A request with a token that is expired, has the wrong audience, or
      has a signature that is not valid returns an empty page, with no
      error status.
- [ ] A logged-in viewer who has no connected authors gets an empty
      page.
- [ ] No response is cached where one viewer can receive another
      viewer's feed.

**Notes**: Regression risk. Today the response is sent with
`Cache-Control: public, max-age=30`.

---

### Limit the number of stored circles
**As an** operator
**I want to** set how many circles the service keeps and how long they
last
**So that** graph traffic stays within the authenticated rate limit

**Acceptance criteria**:
- [ ] `UPSTAGE_MAX_VIEWERS` sets the maximum number of stored circles.
      The default is 1000.
- [ ] `UPSTAGE_GRAPH_REFRESH_AGE` sets the age at which a circle is
      refreshed. The default is 6 hours.
- [ ] `UPSTAGE_GRAPH_IDLE_EVICT` sets how long a circle is kept with no
      requests. The default is 7 days.
- [ ] `UPSTAGE_FOLLOWS_ME_DEPTH` sets how many of the highest-ranked
      pairs get the "follows me" check. The default is 1000.
- [ ] `UPSTAGE_D2_FOLLOWS_SAMPLE` sets how many of the viewer's most
      recent follows degree 2 uses. The default is 100.
- [ ] `UPSTAGE_D2_FOLLOWS_DEPTH` sets how many of each sampled account's
      most recent follows degree 2 uses. The default is 100.
- [ ] `UPSTAGE_D2_REFRESH_AGE` sets how long a shared follows list is
      kept. The default is 24 hours.
- [ ] When the store is full, the service removes the circle that was
      used least recently and writes one log line for each removal.
- [ ] A viewer whose circle was removed gets the first-open behaviour on
      their next request.
- [ ] A change to any of these values takes effect after a restart, with
      no code change.

---

### Read feed health
**As an** operator
**I want to** see how full the viewers' feeds are
**So that** I know if the circle is too narrow or too wide

**Acceptance criteria**:
- [ ] Each hour, the service writes one JSON log line with these
      values: the number of active viewers, the median number of circle
      pairs that entered an active viewer's list in the last 24 hours,
      the share of active viewers with 0 such pairs, the median
      discovery share (the part of a viewer's circle pairs that are
      degree-2 pairs), and the number of removed circles in that hour.
- [ ] No log line, at any level, contains a viewer DID in plain text.

---

## Edge cases and error states

- **App View not available during a refresh.** The viewer keeps their
  last complete circle. A new viewer with no circle gets an empty page
  until a build completes.
- **Partial refresh.** If a refresh fails part of the way, the viewer
  keeps their last complete circle, not a mix of the old and new data.
- **Snapshot swap during a request.** A page is built from one snapshot
  only. It never mixes items from two snapshots.
