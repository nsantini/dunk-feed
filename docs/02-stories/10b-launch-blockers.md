# 10b — Launch blockers

After this story, the service is safe to open to the public network. A
stranger cannot use a fake login to make the service fetch a page from
its own private network or from the machine it runs on. The service
checks the real network address of every outside host before it connects,
and it connects only to the address it checked. A flood of fake logins
cannot make the service call the identity directories without limit. The
service looks up a bounded number of unknown identities each minute and
drops the rest, and a real viewer who is dropped gets an empty page and
tries again on the next request. A set of valid accounts cannot push real
viewers out of the circle store at speed. A viewer who used the feed in
the last hour keeps their circle, and the service removes at most a fixed
number of old circles each minute to make room for new ones. When there is
no room, a new viewer gets an empty page until a slot opens.

- **Follows**: 05, 09, 10
- **PRD story**: Viewers without a valid login; Limit the number of stored circles
- **Size**: standard
- **Design**: docs/02-TECH-DESIGN-network-feed.md §4, §5, §6.3, §7
- **Flag**: `UPSTAGE_PERSONALISE` (default `false`)

## Release

This story merges and deploys with `UPSTAGE_PERSONALISE=false`, the
default. The served feed does not change. The resolver and the graph do
not start, so the three protections do not run. They apply each time the
switch is `true`, and there is no separate flag for them.

To turn the feature on in one environment, set `UPSTAGE_PERSONALISE=true`
in the `.env` of that environment. Then restart with `docker compose -f
<file> up -d`. The flag applies to the whole process. There is no flag for
one user.

Rollback is `UPSTAGE_PERSONALISE=false` and a restart. No deploy is
necessary. A revert of the pull request is also safe, because the story
adds no table and no migration. Do not launch (story 11) after a revert:
the three items are launch blockers.

## Outcome

After this ships, three launch blockers from the story 05 and story 09
reviews are closed. Before each `did:web` fetch, the resolver looks up the
host and fails the fetch when any address is private, loopback,
link-local or not public in another way. The HTTP client connects only to
the addresses that passed the check, so a second DNS answer cannot change
the target (DNS rebinding). The resolver sends at most
`UPSTAGE_RESOLVER_MISSES_PER_MIN` misses each minute across all DIDs. At
`UPSTAGE_MAX_VIEWERS`, LRU eviction never removes a circle with a request
in the last `UPSTAGE_GRAPH_LRU_PROTECT_MIN` minutes, and removes at most
`UPSTAGE_GRAPH_LRU_EVICT_PER_MIN` circles each minute. A first build that
finds no circle it may remove is refused, as in story 06.

## Non-goals

- Does not change the default of `UPSTAGE_PERSONALISE`. It stays `false`.
  Story 11 flips it.
- Does not check `UPSTAGE_PLC_URL`. The operator sets it, so it is
  trusted. Its format rule from story 05 stays.
- Does not validate URLs beyond the standing rule: `UPSTAGE_PDS_URL` is an
  https origin. Other URL edge cases are out of scope.
- Does not add cache admission for viewer keys. Attacker-hosted `did:web`
  documents can still push real keys out of the key cache. That is a
  separate follow-up in the program's standing rules.
- Does not give a real viewer a guaranteed slot during a flood. The miss
  limit is global. An attacker who spends the whole limit each minute
  delays real first-time viewers. The limit bounds outbound traffic. It
  does not identify the sender.
- Does not stop a patient attacker who holds many valid identities from
  filling the circle store over many hours. The eviction limit makes that
  slow: at the defaults, 1000 slots take about 17 hours to take over.
- Does not change the first-build retry rule (5 failed attempts, then a 1
  hour cooldown) or the `step_follows` cap of 100 pages.
- Does not document the new variables in `.env.example` or
  `docs/RUNBOOK.md`. Story 11 documents all new variables.

## Approach

SSRF: `auth/dns.rs` implements `reqwest::dns::Resolve`. It looks up the
host with `tokio::net::lookup_host`, fails when any address is blocked,
and returns the checked addresses to `reqwest`. `reqwest` connects only to
the addresses the resolver returns, so the checked address is the
connected address. The `did:web` fetch uses its own client with this
resolver and with `no_proxy()`, because a proxy would look up the name
itself. The `did:plc` client stays as it is. Rejected: look up the host in
`fetch` and then call `reqwest` with the URL, because `reqwest` would look
up the name a second time and a rebinding DNS server could answer
differently.

Resolver limit: `KeyCache::should_send_miss` gets a global budget of
misses for each wall-clock minute. It spends one unit only after the
per-DID checks from story 05 pass, so a DID already in flight or in
cooldown costs nothing. With no budget left, the miss is refused the same
way a full channel refuses it: no in-flight mark and no cooldown.

LRU churn: `GraphHandle::enqueue_first_build` picks the LRU victim only
from circles whose effective `last_request_at` is older than the
protection window, and it counts LRU evictions for each wall-clock minute.
With no victim or no budget, it creates no circle and queues no job.
Rejected: only a per-minute cap, because an attacker could still remove
active viewers, one each minute. Rejected: only a protection window,
because an attacker could still remove every idle circle at once and fill
the queue with first builds.

## Files in scope

| Path | Change |
|---|---|
| `src/auth/dns.rs` (new) | `is_public(IpAddr) -> bool`. `PublicOnlyResolver`, a `reqwest::dns::Resolve` over a lookup seam, so tests need no network |
| `src/auth/did.rs` | `did:web` fetches use a second client with `PublicOnlyResolver` and `no_proxy()`. `FetchError::Blocked`. Global miss budget in `KeyCache::should_send_miss`. `auth.miss_limited` log line |
| `src/auth/mod.rs` | `mod dns;`. `spawn_resolver` takes the miss budget |
| `src/graph/mod.rs` | Protection window and per-minute eviction budget in `enqueue_first_build`. `graph.lru_refused` log line. `from_store` takes the two limits |
| `src/ingest/mod.rs` | `run` passes the three new values to `spawn_resolver` and `GraphHandle::from_store`. Story 11 changes other lines of this file |
| `src/config.rs` | New keys `UPSTAGE_RESOLVER_MISSES_PER_MIN` (30), `UPSTAGE_GRAPH_LRU_EVICT_PER_MIN` (1), `UPSTAGE_GRAPH_LRU_PROTECT_MIN` (60). Story 11 changes only the `UPSTAGE_PERSONALISE` default, on other lines |
| `docs/02-TECH-DESIGN-network-feed.md` | §4: the three new rows. §5: the address check and the miss limit. §6.3: the protection window and the eviction limit |

No new crate. `reqwest::dns::Resolve` and `ClientBuilder::dns_resolver`
are part of `reqwest` 0.12 without a feature flag.

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `is_public` | IPv4 in `0.0.0.0/8`, `10/8`, `100.64/10`, `127/8`, `169.254/16`, `172.16/12`, `192.0.0/24`, `192.0.2/24`, `192.168/16`, `198.18/15`, `198.51.100/24`, `203.0.113/24`, `224/4`, `240/4` (includes `255.255.255.255`) | `false` |
| BC2 | `is_public` | IPv6 `::`, `::1`, `fc00::/7`, `fe80::/10`, `fec0::/10`, `ff00::/8`, `2001:db8::/32`, `2001::/32` (Teredo) | `false` |
| BC3 | `is_public` | IPv6 that carries an IPv4 address: `::ffff:0:0/96` (mapped), `::/96` (compatible), `64:ff9b::/96` (NAT64), `2002::/16` (6to4) | The result for the carried IPv4 address |
| BC4 | `is_public` | any other address | `true` |
| BC5 | `PublicOnlyResolver` | lookup returns one or more addresses, all public | Returns those addresses, and only those |
| BC6 | `PublicOnlyResolver` | any address not public, or no address | Error `Blocked`. No address is returned, so no connection starts |
| BC7 | `did:web` fetch | any host | The only name lookup is the one in BC5 and BC6. The client connects to an address from that lookup. The client uses no proxy |
| BC8 | `did:web` fetch | blocked | `FetchError::Blocked`. A fetch failure under story 05 BC20: one warning with kind `blocked_address`, no DID, no host, no address. For a `Miss`, the hourly per-DID cooldown starts |
| BC9 | `did:plc` fetch | any | Unchanged. The default resolver. No address check |
| BC10 | name lookup | slow | Counts against the existing 10 s DID fetch timeout. A timeout is a transport failure |
| BC11 | miss budget | fewer than `UPSTAGE_RESOLVER_MISSES_PER_MIN` misses sent in the current minute | A miss that passes the per-DID checks is sent and spends one unit |
| BC12 | miss budget | budget spent | `should_send_miss` returns `false`. No in-flight mark, no cooldown. The request gets the empty page. The next request for that DID tries again |
| BC13 | miss budget | DID in flight or in cooldown | Refused by the story 05 rule first. Spends nothing |
| BC14 | miss budget | new minute | The count goes back to 0. A minute is `now / 60` in Unix seconds |
| BC15 | miss budget | `Refetch` (stale key, signature failure) | Not counted. The story 05 hourly limit for each DID stays |
| BC16 | `auth.miss_limited` | first refused miss in a minute | One `warn` JSON line with `event: "auth.miss_limited"`. No DID. At most one line each minute |
| BC17 | LRU pick | at `UPSTAGE_MAX_VIEWERS` | The victim is the circle with the oldest effective `last_request_at` among circles older than `UPSTAGE_GRAPH_LRU_PROTECT_MIN` minutes. Any state counts, as in story 09 |
| BC18 | LRU pick | every circle has a request inside the window | Refused with reason `protected` |
| BC19 | LRU budget | `UPSTAGE_GRAPH_LRU_EVICT_PER_MIN` LRU evictions already in the current minute | Refused with reason `budget`. Idle evictions do not count |
| BC20 | refused first build | any reason | No circle created. No job queued. No cooldown set. The request gets the empty page. The next request tries again, the same as the story 06 cap refusal |
| BC21 | over the cap after a restart | several evictions needed | Each eviction spends budget. When the budget runs out, the first build is refused. The count drains to the cap at the budget rate |
| BC22 | `graph.lru_refused` | first refused first build in a minute | One `info` JSON line with `event: "graph.lru_refused"` and `reason` (`protected` or `budget`). No DID, no handle, no hash. At most one line each minute |
| BC23 | `graph.evicted` | LRU eviction | Unchanged from story 09. `evicted_1h.lru` in `graph.health` counts it |
| BC24 | `UPSTAGE_RESOLVER_MISSES_PER_MIN`, `UPSTAGE_GRAPH_LRU_EVICT_PER_MIN` | missing or empty | 30 and 1 |
| BC25 | `UPSTAGE_RESOLVER_MISSES_PER_MIN`, `UPSTAGE_GRAPH_LRU_EVICT_PER_MIN` | `0`, negative or not a number | `ConfigError::Invalid` naming the variable. Startup fails |
| BC26 | `UPSTAGE_GRAPH_LRU_PROTECT_MIN` | missing or empty | 60 |
| BC27 | `UPSTAGE_GRAPH_LRU_PROTECT_MIN` | `0` | No protection. Story 09 LRU pick, with the budget |
| BC28 | `UPSTAGE_GRAPH_LRU_PROTECT_MIN` | negative or not a number | `ConfigError::Invalid` naming the variable. Startup fails |
| BC29 | switch `false` | any | Resolver, graph and the three limits do not run. Output equals story 01 output |

## Acceptance criteria

- [ ] AC1 — `is_public` gives the right answer for each range in BC1 to BC4, with one address inside and one just outside each range. Checked by: `cargo test auth::dns::tests::blocked_ranges`
- [ ] AC2 — The resolver returns only checked addresses and fails when any address is blocked or none comes back. Checked by: `cargo test auth::dns::tests::resolve_rules`
- [ ] AC3 — A `did:web` fetch whose fake lookup returns a loopback address fails with `FetchError::Blocked`, and the lookup runs once. A `did:plc` fetch does not use the check. Checked by: `cargo test auth::did::tests::did_web_blocked`
- [ ] AC4 — A blocked `Miss` starts the hourly cooldown and logs kind `blocked_address` with no DID and no host. Checked by: `cargo test auth::did::tests::blocked_log_and_cooldown`
- [ ] AC5 — A live `did:web` fetch of a public name that resolves to `127.0.0.1` is blocked. Checked by: `cargo test --all-features -- --ignored auth::did::tests::live_loopback_host_blocked`
- [ ] AC6 — The miss budget sends N misses in a minute, refuses the next, and sends again in the next minute. Checked by: `cargo test auth::did::tests::miss_budget`
- [ ] AC7 — A DID in flight or in cooldown spends no budget. A refused miss leaves no cooldown. Refetches are not counted. Checked by: `cargo test auth::did::tests::miss_budget_rules`
- [ ] AC8 — Many refused misses in one minute write one `auth.miss_limited` line with no DID. Checked by: `cargo test auth::tests::miss_limited_log`
- [ ] AC9 — At the cap, a circle with a request inside the window is never evicted, and the oldest circle outside it is. Checked by: `cargo test graph::tests::lru_protects_recent`
- [ ] AC10 — At the cap, the budget allows N LRU evictions in a minute and refuses the next until the next minute. Checked by: `cargo test graph::tests::lru_budget`
- [ ] AC11 — A refused first build creates no circle, queues no job and sets no cooldown, and the next call after room opens succeeds. Checked by: `cargo test graph::tests::lru_refusal_retries`
- [ ] AC12 — Many refusals in one minute write one `graph.lru_refused` line with the reason and no DID. Checked by: `cargo test graph::tests::lru_refused_log`
- [ ] AC13 — The three new variables load with their defaults, and invalid values fail. Protection `0` is valid. Checked by: `cargo test config::tests::launch_blocker_limits`
- [ ] AC14 — Design §4, §5 and §6.3 describe the three rules and the three variables. Checked by: manual review.
- [ ] AC15 — All four gates pass. Checked by: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features` and `cargo build --release`.

## Defaults taken

- The address check applies to `did:web` only. `UPSTAGE_PLC_URL` is set
  by the operator and can point to a mirror on a private network (standing
  rule of 2026-09-24 names `did:web`; judgement for the PLC side).
- The block list is wider than "private, loopback and link-local". It
  holds every range that is not globally routable (BC1 to BC3), so a
  reserved or documentation range cannot reach an internal service
  (judgement, from the IANA special-purpose address registries).
- When any address of a host is blocked, the whole fetch fails. The
  resolver does not filter and continue. A public host has no reason to
  publish a private record (judgement).
- DNS rebinding is closed by the custom `reqwest` resolver: `reqwest`
  connects only to the addresses it returns, so there is no second lookup
  (engineer's brief for this story). `no_proxy()` is set on the `did:web`
  client, because a proxy from `HTTPS_PROXY` would look up the name itself
  (judgement).
- `reqwest` wraps a resolver error in its own error. The fetcher finds
  `Blocked` by walking `std::error::Error::source`. If no `Blocked` is in
  the chain, the failure is `FetchError::Transport` (judgement).
- A blocked fetch is a fetch failure under story 05 BC20. It starts the
  per-DID hourly cooldown, so a hostile host is looked up at most once an
  hour (story 05 BC9).
- `UPSTAGE_RESOLVER_MISSES_PER_MIN` is 30. Source: 30 × 60 = 1800 misses
  each hour, below the misses map cap of 2 × `UPSTAGE_MAX_VIEWERS` = 2000.
  Each failed miss stays in that map for its 1 hour cooldown, so at the
  limit a flood cannot fill the map and lock out all new DIDs (story 05
  review round 3, defect V). An operator who raises
  `UPSTAGE_MAX_VIEWERS` can raise this value with it.
- Open question, default chosen: the key cache is memory only. After a
  restart, each active viewer misses once. At 30 misses each minute, 1000
  active viewers can wait up to about 34 minutes for their first
  non-empty page. The default keeps 30 because it protects the misses map.
  The engineer can raise it if deploy recovery matters more.
- Refetches do not spend the miss budget. They exist only for DIDs with a
  cached key, story 05 already limits them to one each hour for each DID,
  and the cache holds at most 2 × `UPSTAGE_MAX_VIEWERS` DIDs (story 05
  BC11, BC14, BC16).
- A refused miss behaves like a dropped send on a full channel: no
  cooldown, and the next request tries again (story 05 BC9).
- `UPSTAGE_GRAPH_LRU_EVICT_PER_MIN` is 1. Source: design §7. A first build
  costs at most about 177 PDS calls (77 for degree 1, 100 for degree 2
  before sharing). At 1000 viewers the budget uses about 4.7 of 8 calls
  each second, which leaves about 200 calls each minute. One forced first
  build each minute fits in that. Story 04 can replace the number.
- `UPSTAGE_GRAPH_LRU_PROTECT_MIN` is 60. A Bluesky client refreshes an
  open feed within minutes, so one hour covers a session. `0` turns the
  protection off, for tests and for an operator who wants story 09
  behaviour with the budget (judgement).
- Both budgets use fixed wall-clock minutes (`now / 60`). Up to 2 × N can
  pass across a minute boundary. That is accepted for a simple, testable
  rule (judgement).
- A refused first build behaves like the story 06 cap refusal: empty
  page, no circle, no cooldown, and the next request tries again (story 09
  replaced that refusal with LRU; this story brings it back only when no
  circle may be removed).
- The existing story 09 eviction tests build the handle with protection
  `0` and a budget that does not bind, so their behaviour stays the same
  (judgement).
- `auth.miss_limited` is `warn`, because it can mean an attack.
  `graph.lru_refused` is `info`, because a full store of active viewers is
  normal at the cap. Each writes at most one line each minute (judgement,
  story 05 BC21 and story 09 BC11 for the privacy rule).
- Story 11 documents the `auth.miss_limited` and `graph.lru_refused`
  log lines and the three new variables (story 11, Files in scope).

## Suggested slices

- 1.0 SSRF: `auth/dns.rs`, the `did:web` client, `FetchError::Blocked`,
  design §5 address check. Done when AC1 to AC5 pass.
- 2.0 Global miss budget: `UPSTAGE_RESOLVER_MISSES_PER_MIN`, the budget
  in `should_send_miss`, `auth.miss_limited`, design §4 and §5. Done when
  AC6 to AC8 pass and the matching part of AC13 passes.
- 3.0 LRU churn: `UPSTAGE_GRAPH_LRU_PROTECT_MIN` and
  `UPSTAGE_GRAPH_LRU_EVICT_PER_MIN`, the window and budget in
  `enqueue_first_build`, `graph.lru_refused`, design §4 and §6.3. Done
  when AC9 to AC14 pass and all four gates pass.

## Testing steps

1. Prepare the shell. Copy `.env.example` to `.env` and fill in the
   required values. Put a copy of a database with feed rows at
   `./upstage.db`, for example a production backup. Then run:

   ```
   export $(grep -v '^#' .env | xargs)
   export UPSTAGE_DB_PATH=./upstage.db
   FEED="at://$UPSTAGE_PUBLISHER_DID/app.bsky.feed.generator/$UPSTAGE_FEED_RKEY"
   SKEL="http://localhost:3000/xrpc/app.bsky.feed.getFeedSkeleton?feed=$FEED"
   b64() { base64 | tr '+/' '-_' | tr -d '=\n'; }
   forge() {
     h=$(printf '{"alg":"ES256K","typ":"JWT"}' | b64)
     p=$(printf '{"iss":"%s","aud":"did:web:%s","exp":%s,"lxm":"app.bsky.feed.getFeedSkeleton"}' \
       "$1" "$UPSTAGE_HOSTNAME" $(( $(date +%s) + 600 )) | b64)
     s=$(head -c 64 /dev/zero | b64); echo "$h.$p.$s"
   }
   randplc() { echo "did:plc:$(LC_ALL=C tr -dc 'a-z2-7' </dev/urandom | head -c 24)"; }
   ```

   Expected: The commands exit 0. `forge did:web:example.com` prints
   three parts with a dot between each part. The signature is all zeros,
   so no such token ever verifies.

2. Log in as test viewer A. Use an account on `bsky.social` that follows
   some authors in the feed. Do the same for a second test viewer B, and
   keep its token in `TOKEN_B`.

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

   Expected: `echo $VIEWER_DID` prints a DID. `echo $TOKEN` and `echo
   $TOKEN_B` each print three parts with a dot between each part.

3. Check that the test host names resolve to blocked addresses.

   ```
   dig +short localtest.me; dig +short 10.0.0.1.nip.io
   ```

   Expected: `127.0.0.1` and `10.0.0.1`.

4. Start the service with the flag on and a small miss budget. `.env`
   must set `BSKY_HANDLE` and `BSKY_APP_PASSWORD`.

   ```
   UPSTAGE_PERSONALISE=true UPSTAGE_RESOLVER_MISSES_PER_MIN=5 \
     cargo run --release -- run 2>&1 | tee run.log
   ```

   Expected: The service starts.

5. Send fake tokens for the loopback host and the private host.

   ```
   for h in localtest.me 10.0.0.1.nip.io; do
     curl -si -H "Authorization: Bearer $(forge did:web:$h)" "$SKEL&limit=30"; done
   sleep 5; grep -c blocked_address run.log; grep -cE 'localtest|nip\.io|127\.0\.0\.1|10\.0\.0\.1' run.log
   ```

   Expected: Two empty pages: status 200, `{"feed":[]}`, `Cache-Control:
   private, no-store`. Then `2` and `0`: two warnings with kind
   `blocked_address`, and no host or address in the log.

6. Send the same loopback token again.

   ```
   curl -s -H "Authorization: Bearer $(forge did:web:localtest.me)" "$SKEL&limit=30"; sleep 5
   grep -c blocked_address run.log
   ```

   Expected: `{"feed":[]}`, then still `2`. The host is in its hourly
   cooldown, so no new lookup ran.

7. Wait for the start of a new minute. Send 20 fake tokens with random
   `did:plc` DIDs.

   ```
   sleep $(( 60 - $(date +%s) % 60 ))
   for i in $(seq 20); do curl -s -H "Authorization: Bearer $(forge $(randplc))" "$SKEL&limit=30" > /dev/null; done
   sleep 15; grep -c '"auth.miss_limited"' run.log
   ```

   Expected: `1`. One `auth.miss_limited` line for the minute. The line
   has no DID. The log has no more than 5 new resolver warnings from these
   requests.

8. In the same minute, send viewer A's first request.

   ```
   curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   ```

   Expected: The empty page. The budget for this minute is spent.

9. Wait for the next minute. Send viewer A's request two times, 30
   seconds apart.

   ```
   sleep $(( 60 - $(date +%s) % 60 ))
   curl -s -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"; sleep 30
   curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   ```

   Expected: The second response holds viewer A's pairs. The key resolved
   and the first build ran.

10. Stop the service. Start it with room for one circle and a 2 minute
    protection window. Send viewer A's request, then B's request 10
    seconds later.

    ```
    UPSTAGE_PERSONALISE=true UPSTAGE_MAX_VIEWERS=1 UPSTAGE_GRAPH_LRU_PROTECT_MIN=2 \
      cargo run --release -- run 2>&1 | tee run-lru.log
    # in a second shell:
    TOKEN=$(token); curl -s -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"; sleep 10
    curl -s -H "Authorization: Bearer $TOKEN_B" "$SKEL&limit=30"; sleep 5
    grep '"graph.lru_refused"' run-lru.log; grep -c '"graph.evicted"' run-lru.log
    sqlite3 "$UPSTAGE_DB_PATH" "SELECT count(*) FROM viewers WHERE viewer_did='$VIEWER_DID'"
    ```

    Expected: One `graph.lru_refused` line with `"reason":"protected"` and
    no DID. Then `0` eviction lines. Then `1`: viewer A keeps the circle.

11. Send no request as A for 3 minutes. Then send B's request.

    ```
    sleep 180; curl -s -H "Authorization: Bearer $TOKEN_B" "$SKEL&limit=30"; sleep 5
    grep '"graph.evicted"' run-lru.log
    sqlite3 "$UPSTAGE_DB_PATH" "SELECT count(*) FROM viewers WHERE viewer_did='$VIEWER_DID'"
    ```

    Expected: One `graph.evicted` line with `"reason":"lru"` and no DID.
    Then `0`: viewer A's circle is gone, because A was outside the window.

12. Stop the service. Start it with no protection and one eviction each
    minute. Send B's request, then A's request, then B's request, all in
    the same minute.

    ```
    UPSTAGE_PERSONALISE=true UPSTAGE_MAX_VIEWERS=1 UPSTAGE_GRAPH_LRU_PROTECT_MIN=0 \
      UPSTAGE_GRAPH_LRU_EVICT_PER_MIN=1 cargo run --release -- run 2>&1 | tee run-budget.log
    # in a second shell:
    sleep $(( 60 - $(date +%s) % 60 )); TOKEN=$(token)
    curl -s -H "Authorization: Bearer $TOKEN_B" "$SKEL&limit=30"; sleep 2
    curl -s -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"; sleep 2
    grep -c '"graph.evicted"' run-budget.log; grep '"graph.lru_refused"' run-budget.log
    ```

    Expected: B's circle from step 11 loads from SQLite at startup, so B's
    request needs no eviction and A's request evicts B: `1` eviction line.
    No refusal line yet.

13. In the same minute, send B's request. Then wait for the next minute
    and send it again.

    ```
    curl -s -H "Authorization: Bearer $TOKEN_B" "$SKEL&limit=30"; sleep 2
    grep '"graph.lru_refused"' run-budget.log
    sleep $(( 60 - $(date +%s) % 60 )); curl -s -H "Authorization: Bearer $TOKEN_B" "$SKEL&limit=30"; sleep 2
    grep -c '"graph.evicted"' run-budget.log
    ```

    Expected: One `graph.lru_refused` line with `"reason":"budget"` and no
    DID. After the new minute, `2` eviction lines: the budget came back.

14. Search all logs for the viewer DIDs.

    ```
    grep -c "$VIEWER_DID" run*.log
    ```

    Expected: 0 for each file. Do the same for viewer B.

15. Stop the service. Start it with the flag off. Send a fake loopback
    token.

    ```
    UPSTAGE_PERSONALISE=false cargo run --release -- run 2>&1 | tee run-off.log
    # in a second shell:
    curl -si -H "Authorization: Bearer $(forge did:web:localtest.me)" "$SKEL&limit=30"
    grep -cE 'blocked_address|auth.miss_limited|graph.lru_refused' run-off.log
    ```

    Expected: Status 200 with the global items and `Cache-Control: public,
    max-age=30`. Then `0`. The resolver and the graph do not start.
