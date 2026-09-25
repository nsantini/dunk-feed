# Upstaged runbook

This runbook is for an operator. It does not assume you have read the Rust
source. It covers deploy, logs, backup, upgrade, tuning, and every failure
mode in `docs/01-TECH-DESIGN.md` section 13.

## Prerequisites

You need these before you start.

- Docker and the Compose plugin, installed on the VM.
- For the host-proxy variant only: `cloudflared` and nginx, both already
  running on the host.
- A Cloudflare account, with a domain on it as an active zone. See
  "Cloudflare setup" below.
- A Bluesky account for the feed. See "Bluesky account setup" below.

## Bluesky account setup

Do this before the first deploy. It gives you the DID that `.env` needs and
the app password that `upstage publish` needs.

The account is the feed's public identity. The feed shows up under it in the
Bluesky app, with its handle and its avatar.

1. Create a Bluesky account for the feed, or pick one you already have.
2. Open Settings in the Bluesky app, then App passwords. Create one.
3. Copy the app password now. Bluesky shows it one time only.
4. Get the account DID. Run this from any machine:

   ```
   curl "https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle=<your.handle>"
   ```

   The answer is `{"did":"did:plc:..."}`. Copy that DID.
5. Keep the DID, the handle and the app password. "First deploy" step 2
   puts them in `.env`, as `UPSTAGE_PUBLISHER_DID`, `BSKY_HANDLE` and
   `BSKY_APP_PASSWORD`.

`UPSTAGE_PUBLISHER_DID` holds the DID and not the handle, because a handle can
change. The at-URI of the feed must not change.

`publish` compares the DID of the account it signs in as against
`UPSTAGE_PUBLISHER_DID`. A difference fails the command with a DID mismatch
error, and no record is written.

### The two DIDs

Upstaged uses two DIDs. They do different jobs and they are not
interchangeable.

| DID | Where it comes from | What it identifies |
|-----|--------------------|--------------------|
| `did:plc:...`, in `UPSTAGE_PUBLISHER_DID` | Your Bluesky account | The owner of the feed record |
| `did:web:<hostname>`, built from `UPSTAGE_HOSTNAME` | Your public hostname | The server that computes the feed |

The published record lives in the account's repository, and its own `did`
field holds `did:web:<hostname>`. This field is the pointer from Bluesky to
your VM. The server answers for that identity at `/.well-known/did.json`.

The feed's at-URI is
`at://<UPSTAGE_PUBLISHER_DID>/app.bsky.feed.generator/<UPSTAGE_FEED_RKEY>`.
Bluesky sends this URI in every feed request. The server serves only this one
URI and refuses every other.

## First deploy

1. Copy `.env.example` to `.env`.
2. Fill in `UPSTAGE_HOSTNAME` and `UPSTAGE_PUBLISHER_DID`. These are the only two
   required variables. `src/config.rs` rejects the container start when
   either one is empty or holds only whitespace.

   `UPSTAGE_HOSTNAME` is a hostname you choose on Cloudflare. See "Cloudflare
   setup" below for how to pick it. `UPSTAGE_PUBLISHER_DID` comes from
   "Bluesky account setup" above.

   `BSKY_HANDLE` and `BSKY_APP_PASSWORD` are always required for `upstage
   publish`. `UPSTAGE_PERSONALISE` defaults to `true` (see "The network
   feed and the kill switch" below), and with the default in place `upstage
   run` also refuses to start when either one is missing or blank. Fill in
   both from "Bluesky account setup" above before the first `up -d`.
   `TUNNEL_TOKEN` has no default and is needed only for the tunnel variant.
3. Pick one Compose file:

   | File | Use it when |
   |---|---|
   | `compose.yaml` | The host runs no tunnel yet. Compose starts one |
   | `compose.hostproxy.yaml` | The host already runs `cloudflared` and nginx |
   | `compose.proxied.yaml` | Cloudflare's proxied DNS reaches port 3000 directly |

   See "Cloudflare setup" below for all three.
4. Run `docker compose -f <file> up -d --build`.
5. Watch `/healthz` turn from 503 to 200. See "Health states" below for the
   two bodies.

All three Compose files use the same `upstage-data` volume and the same
Compose project name. Do not run two variants at once against the same
project. They share one database and one set of container names.

## Publishing the feed record

Run this once, after the container is healthy:

```
docker compose -f <file> run --rm upstage publish
```

Set `BSKY_HANDLE` and `BSKY_APP_PASSWORD` in `.env` first. `publish` needs
both. See "Bluesky account setup" above for both values.

### What publish does

`publish` writes one record and then stops. The record is one
`app.bsky.feed.generator` in your account's repository, under the record key
`UPSTAGE_FEED_RKEY`, `upstaged` by default. It goes to `https://bsky.social`, not
to the App View.

`publish` does not send posts or feed content to Bluesky. The container
serves the feed live, on every request, from your VM. What this command does
is register the feed, so that people can find it and pin it. Before it runs,
the feed does not exist for the Bluesky app.

The display name and the description are fixed in the source. Only a rebuild
changes them.

The command writes the record unconditionally, so you can run it again. A
second run replaces the record, which is how you add or change the avatar.

`ENTRYPOINT` in the image is `upstage`. `publish` replaces the default command,
so `run` does not start while this command runs.

To publish with an avatar, add `--avatar <path>`. The path must be visible
inside the container, not just on the host. Add a read-only bind mount to
the `upstage` service for the one run, for example:

```
docker compose -f <file> run --rm -v /home/you/avatar.png:/avatar.png:ro upstage publish --avatar /avatar.png
```

Pass the container path, `/avatar.png`, not the host path.

## The network feed and the kill switch

`UPSTAGE_PERSONALISE` defaults to `true`. A viewer who sends a valid
service JWT gets a feed built from their own follow graph; every other
request, and the whole feed when the switch is `false`, gets the plain
global feed from `01`.

With the switch `true`, `upstage run` reads `BSKY_HANDLE` and
`BSKY_APP_PASSWORD` before it opens the database or makes any network
call. A missing or blank value exits 1 with a message naming that
variable, and Docker's restart policy retries forever until you fix
`.env`. Set both from "Bluesky account setup" above.

These variables tune the network feed. Each lives in `.env`, with the
same "restart to apply" rule as the `01` variables above.

| Variable | Default | Meaning |
|---|---|---|
| `UPSTAGE_SERVICE_DID` | `did:web:<UPSTAGE_HOSTNAME>` | The `aud` a viewer's service JWT must name |
| `UPSTAGE_PLC_URL` | `https://plc.directory` | Base URL the `did:plc` resolver fetches a document from |
| `UPSTAGE_MAX_VIEWERS` | `1000` | Cap on stored circles. The oldest `last_request_at` is evicted first past this cap |
| `UPSTAGE_GRAPH_REFRESH_AGE_H` | `6` | Hours a Ready circle goes with no refresh before the scheduler queues one, while the viewer keeps requesting |
| `UPSTAGE_GRAPH_IDLE_EVICT_D` | `7` | Days a circle can go with no request before the scheduler evicts it |
| `UPSTAGE_FOLLOWS_ME_DEPTH` | `1000` | Ranked items the "follows me" check covers |
| `UPSTAGE_D2_FOLLOWS_SAMPLE` | `100` | Viewer's most recent follows that degree-2 discovery samples |
| `UPSTAGE_D2_FOLLOWS_DEPTH` | `100` | Each sampled account's most recent follows that degree-2 discovery reads |
| `UPSTAGE_D2_REFRESH_AGE_H` | `24` | Hours a shared follows-list cache entry stays fresh |
| `UPSTAGE_GRAPH_RPS` | `8.0` | Graph client (PDS) rate limit, requests per second |
| `UPSTAGE_PDS_URL` | `https://bsky.social` | PDS the session and every graph call go against |
| `UPSTAGE_RESOLVER_MISSES_PER_MIN` | `30` | Global cap on `did` resolver `Miss` fetches sent in one wall-clock minute. Past the cap, a resolve fails closed and the request gets the global feed |
| `UPSTAGE_GRAPH_LRU_EVICT_PER_MIN` | `1` | Most LRU circle evictions in one wall-clock minute. Past the cap, a first build that would need one more eviction is refused instead |
| `UPSTAGE_GRAPH_LRU_PROTECT_MIN` | `60` | Minutes since a circle's last request that make it immune to LRU eviction. `0` disables the protection |

### Kill switch procedure (rollback)

Use this to fall back to the plain global feed without a deploy or a
code revert. It also clears an incident where the graph subsystem itself
is the problem: the resolver, the worker, the scheduler and the metrics
loop all stop, and no viewer JWT is read.

1. Set `UPSTAGE_PERSONALISE=false` in `.env`.
2. Restart: `docker compose -f <file> up -d`.
3. Confirm the fallback with a request to the feed:

   ```
   curl -si "$SKEL&limit=30"
   ```

   Expected: `Cache-Control: public, max-age=30` in the response headers.
   That header, and not `private, no-store`, is the global feed.

The `viewers`, `viewer_follows`, `viewer_checks` and `follows_cache`
tables in SQLite are untouched by the switch. Turning the switch back to
`true` and restarting picks the circles back up; nothing needs
rebuilding from scratch unless a circle went idle past
`UPSTAGE_GRAPH_IDLE_EVICT_D` in the meantime.

### `upstage graph-probe`

An operator-run, read-only command, separate from `run`. It logs in with
`BSKY_HANDLE`, reads the current `feed` rows from SQLite, and runs one
first-build pass in memory for each handle you pass, without starting the
worker or writing anything to SQLite. Run it to size `UPSTAGE_MAX_VIEWERS`
and the call budget before raising traffic, or to sanity-check the graph
build's cost against a real account:

```
docker compose -f <file> run --rm upstage graph-probe --handle <h1> --handle <h2>
```

It prints, for each handle, the call, page and time cost of each build
step, the circle size, the overlap against the other handles you passed,
and the discovery share from degree-2 accounts. It exits 1 before any
network call if `BSKY_HANDLE` or `BSKY_APP_PASSWORD` is missing or blank,
or if you pass no `--handle`.

## Reading the logs

Upstaged logs JSON lines to stdout. `docker compose -f <file> logs -f upstage`
shows them. Three lines matter.

### `upstage: ingest stats`

Logged every 60 seconds by the ingest task. No line appears at all for the
first minute after start. If you grep for it right after `up -d` and find
nothing, that is not a fault. Fields:

| Field | Meaning |
|-------|---------|
| `events_per_s` | Jetstream commits received per second, by collection, over the window |
| `hot_set_len` | Rows currently held in the in-memory hot set |
| `ops_per_s` | Store operations applied per second, by kind, over the window |
| `gate_hit_rate` | Share of like and repost creates that passed the gate and produced an increment |
| `postgate_detaches` | Detach operations applied in the window |
| `dropped_unknown_collection` | Commits dropped because their collection is not one Upstaged tracks |
| `dropped_self_quote` | Commits dropped because a quote post quotes its own author |
| `dropped_non_post_embed` | Commits dropped because the embed is not a quote of a post |
| `channel_depth` | Events waiting in the ingest-to-writer channel at the moment this line was logged |
| `lag_s` | Seconds between now and the last commit's own timestamp, `0` before the first commit |
| `compressed` | Whether the Jetstream connection is running with zstd compression |

`events_per_s` and `ops_per_s` are logged with `tracing`'s debug
formatting, so in the JSON output each one is a quoted string holding a
Rust map literal, not a nested JSON object. A JSON query tool reads them
as strings.

### `scorer: pass complete`

Logged once per scorer pass. Fields:

| Field | Meaning |
|-------|---------|
| `selected` | Candidates the pass considered for scoring |
| `appview_calls` | Calls made to the App View during this pass |
| `profile_calls` | Author profile lookups made during this pass |
| `deferred` | Candidates left for a later pass, not yet decided |
| `promoted` | Candidates that newly met the score threshold and entered the feed |
| `demoted` | Feed rows that fell back below the score threshold and left the feed |
| `dropped` | Candidates dropped this pass, summed across every reason |
| `dropped_by_reason` | The same total, broken out by reason. See the table below |
| `expired` | Feed rows removed for age, past `UPSTAGE_FEED_TTL_D` |
| `snapshot_len` | Rows in the feed snapshot after this pass |
| `duration_ms` | Wall time the pass took, in milliseconds |

`dropped_by_reason` is logged with `tracing`'s debug formatting, so in the
JSON output it is a quoted string holding a Rust map literal, not a
nested JSON object. A JSON query tool reads it as a string.

`dropped_by_reason` is a map. Its keys are:

| Key | Meaning |
|-----|---------|
| `self_quote` | The quote post quotes its own author |
| `not_a_post` | The quoted record is not a post |
| `quote_gone` | The quote post itself no longer exists |
| `original_gone` | The quoted, original post no longer exists |
| `detached` | The quote was detached from the original |
| `blocked` | A block exists between the two authors |
| `labelled` | A label in `UPSTAGE_DROP_LABELS` was found on the quote, on the original, or on either author's profile |
| `author_inactive` | The author's account is deactivated or deleted, or the original's author carries a `!takedown` label |
| `follower_floor` | The author's follower count is under `UPSTAGE_FOLLOWER_FLOOR` |

### `scorer: guard histogram period follower distribution`

Logged once per pass, but only while the follower-floor histogram period is
open. It stops after `UPSTAGE_GUARD_HISTOGRAM_H` hours from start. The follower
floor itself still drops candidates from the first pass onward, with or
without this line. Fields: `guard_would_drop`, and six follower-count
buckets: `zero`, `one_to_99`, `hundred_to_999`, `thousand_to_9999`,
`ten_k_to_99999`, `hundred_k_plus`.

### `graph.health`

Logged once an hour by the graph metrics loop, only while
`UPSTAGE_PERSONALISE` is `true`. Fields:

| Field | Meaning |
|-------|---------|
| `active_viewers` | Viewers with a stored circle right now |
| `median_new_pairs_24h` | Median, across active viewers, of pairs each one's list gained in the last 24 hours |
| `zero_share` | Share of active viewers whose `new_pairs_24h` is `0` |
| `median_discovery_share` | Median, across viewers with at least one item, of the share of their items found only through a degree-2 account |
| `evicted_1h` | `{"idle": <n>, "lru": <n>}`, circles evicted in the last hour by each reason |
| `graph_calls_1h` | Graph client calls in the last hour, by App View method name |
| `queue_depth` | `{"first_build": <n>, "refresh": <n>, "refill": <n>}`, jobs waiting in each queue right now |

### `graph.evicted`

Logged once for each circle the scheduler or a first build removes from
memory and SQLite. One `info` line, fields `event` and `reason`. No DID,
no handle. `reason` is `idle` (no request for `UPSTAGE_GRAPH_IDLE_EVICT_D`
days) or `lru` (the store passed `UPSTAGE_MAX_VIEWERS` and this circle had
the oldest `last_request_at`).

### `auth.miss_limited`

Logged at most once a minute, `warn` level, one JSON line, `event:
"auth.miss_limited"`, no DID. Fires on the first `did:plc` or `did:web`
resolver miss refused after `UPSTAGE_RESOLVER_MISSES_PER_MIN` fetches have
already gone out in the current wall-clock minute. A refused resolve
fails closed: that request gets the global feed, not an error. Frequent
lines mean either real traffic growth or an attacker sending many unknown
DIDs; raise the budget only after you have ruled out the second.

### `graph.lru_refused`

Logged at most once a minute, `info` level, one JSON line, `event:
"graph.lru_refused"`, `reason` field, no DID, no handle, no hash. Fires
when a first build needs an LRU eviction to make room and the eviction is
refused. `reason` is `protected` (every evictable circle was inside
`UPSTAGE_GRAPH_LRU_PROTECT_MIN`) or `budget`
(`UPSTAGE_GRAPH_LRU_EVICT_PER_MIN` evictions already happened this
minute). No circle is built for that viewer this request, so they get the
empty personalised page (`{"feed":[]}`, `Cache-Control: private,
no-store`), not the global feed; the graph subsystem itself keeps
running, and their next request tries again.

## Backup

Back up the SQLite file nightly, from the host, against the named volume,
in a one-off container:

```
docker run --rm -v upstage-feed_upstage-data:/data -v /var/backups/upstage:/backup alpine/sqlite /data/upstage.db ".backup '/backup/upstage.db'"
```

Confirm the volume's real name first, with `docker volume ls`. Compose
prefixes the volume name with the project name, so it may not be exactly
`upstage-feed_upstage-data`.

This does not run as `docker compose exec upstage sqlite3 ...`. The runtime
image carries no `sqlite3` binary. Adding one would cost image size the
40 MB cap does not have to spare.

SQLite's backup API is safe to run against a live writer. The service keeps
running during the backup.

If you lose the database, you lose 30 days of feed history and nothing
else. The hot set and the counters rebuild within 48 hours.

## Upgrade

```
git pull
docker compose -f <file> up -d --build
```

The Jetstream cursor is checkpointed to SQLite. A restart under 36 hours
resumes with no gap. A restart over 36 hours loses the events in between,
because Jetstream's own retention window ends at 36 hours.

`compose.yaml` pins `cloudflared` to a specific tag, `2026.9.1` today,
instead of `latest`. This keeps an unrelated upgrade, such as an Upstaged
code change, from also pulling a new `cloudflared` release. To bump the
pin, check the current release on Docker Hub, edit the tag in
`compose.yaml`, then run `docker compose -f compose.yaml up -d --build` as
a normal upgrade.

## Tuning knobs

Each of these lives in `.env`. A change needs `docker compose -f <file> up
-d` to take effect. The binary reads the environment once, at start.

| Variable | Raising it | Lowering it |
|----------|------------|-------------|
| `UPSTAGE_P` | Requires a more popular original post before a pair can score | Lets less popular original posts score |
| `UPSTAGE_M` | Requires the quote to beat the original by a wider margin | Lets a smaller margin promote a pair |
| `UPSTAGE_W_REPOST` | Weighs each repost more heavily in the score | Weighs each repost less heavily |
| `UPSTAGE_W_REPLY` | Weighs each reply more heavily in the score | Weighs each reply less heavily |
| `UPSTAGE_K` | Smooths the score more, damping small engagement counts further | Smooths the score less |
| `UPSTAGE_FOLLOWER_FLOOR` | Requires more followers before an author's post can score. `0` disables the guard | Lets authors with fewer followers score |
| `UPSTAGE_GUARD_HISTOGRAM_H` | Keeps the follower-distribution log line running longer after start | Stops the log line sooner. `0` disables the period; the floor stays live regardless |
| `UPSTAGE_SCORER_INTERVAL_S` (default 60) | Runs the scorer pass less often, using less CPU but leaving new candidates unscored longer | Runs the pass more often, scoring candidates sooner but using more CPU. Raise `UPSTAGE_HEALTH_MAX_LAG_S` above the new value too, or `/healthz` flips unhealthy between passes |
| `UPSTAGE_REVERIFY_INTERVAL_S` (default 600) | Re-checks promoted pairs against the App View less often, using fewer App View calls but catching a block or a takedown later | Re-checks more often, catching a block or a takedown sooner but using more App View calls |
| `UPSTAGE_HEALTH_MAX_LAG_S` (default 300) | Tolerates a longer gap since the last Jetstream commit or scorer pass before `/healthz` turns 503, so a slow patch is less likely to trip your monitoring | Tolerates a shorter gap, so `/healthz` catches a stall sooner but is more likely to flip on a normal slow pass |

### Tuning with `upstage dump`

`upstage dump --since 24h --out pairs.csv` writes one CSV row for every pair
first seen in the last 24 hours, in all three states: `candidate`,
`promoted` and `dropped`. `--since` takes a number and a unit, `h` or `d`,
for example `7d` for seven days. `--out` defaults to
`./upstage-dump-<since>.csv` when you leave it out.

Each row carries the pair's local counts from `counts`, its verified
counts from `feed` when it has a `feed` row, and `E` and `D` recomputed
from those counts with the config running right now. Open the CSV in a
spreadsheet and sort by these columns to re-fit each knob:

| Knob | Env var | Sort by |
|------|---------|---------|
| `P` | `UPSTAGE_P` | The larger of `verified_e_q` and `verified_e_o`, over rows where `state` is `promoted` |
| `M` | `UPSTAGE_M` | `verified_d`, over rows where `state` is `promoted` |
| Repost weight | `UPSTAGE_W_REPOST` | `reposts_q` and `reposts_o` against `local_e_q` and `local_e_o` |
| Reply weight | `UPSTAGE_W_REPLY` | `replies_q` and `replies_o` against `local_e_q` and `local_e_o` |

After you pick new values, edit `.env` and restart the container, as the
"Tuning knobs" table above describes. No code change is needed.

A pair that was promoted and later demoted shows local counts only: its
`v_*` cells, `verified_e_q`, `verified_e_o`, `verified_d` and
`promoted_at` are empty, because demoting a pair deletes its `feed` row.

## Failure modes

Each subsection says what you see and what to do.

### One Jetstream host down

The client rotates to the next host in `UPSTAGE_JETSTREAM_URL` within one
backoff step. You see no `/healthz` change. No action needed.

### All Jetstream hosts down

Ingest backs off and retries. `/healthz` turns 503 after `lag_s` passes
`UPSTAGE_HEALTH_MAX_LAG_S`, 300 seconds by default. The feed keeps serving its
last snapshot while this happens. Check Jetstream's own status. No action
on the container is needed until Jetstream recovers.

### The App View down

No new candidates get promoted or re-verified. The feed keeps serving its
last snapshot. Check the App View's own status. No action on the container
is needed until it recovers.

### The disk full

The writer thread errors and the process exits non-zero. Docker's restart
policy starts it again. The Jetstream cursor resumes from its last
checkpoint. Free disk space on the volume, or the restart loop repeats.

### A task panicking under the task supervisor

`upstage run` supervises the ingest, scorer and HTTP tasks in one task
supervisor. A panicking task becomes a logged error, never a re-panic, so
the writer still flushes. The first task to stop flips a shutdown signal;
the others are awaited and logged; the process exits with the first error
as its exit reason. Docker's restart policy starts a fresh process. Read
the logs for the task name and the error before you restart, so you know
what to check.

### Out of memory at the 512 MB limit

Docker's `mem_limit` kills the container. `restart: unless-stopped` starts
it again. See "Memory limit" under the BC table below for detail on the
restart's gap.

## Behaviour contracts BC1 to BC6

| Contract | Condition | What you see | What you do |
|----------|-----------|---------------|--------------|
| BC1 | The `/data` volume is missing at container start | `UPSTAGE_DB_PATH` write fails fast, the process exits non-zero | Docker's restart policy retries. Check that the volume is declared and attached |
| BC2 | `/healthz` returns 503 | Docker marks the container unhealthy after 3 failed checks | This is not a restart on its own. Read the logs and the "Health states" section below, then act |
| BC3 | `/healthz` returns 200 | Docker marks the container healthy | No action |
| BC4 | `TUNNEL_TOKEN` is missing or empty, tunnel variant | `cloudflared` exits non-zero immediately | `upstage` keeps running, reachable only on the Compose network. Set `TUNNEL_TOKEN` in `.env` and restart `cloudflared` |
| BC5 | Port 3000 is unreachable from outside, proxied variant | No response from the public hostname | Check the VM firewall and Cloudflare's proxy status. This is not a container-level failure |
| BC6 | Container memory use reaches 512 MB | Docker's `mem_limit` kills the container, `restart: unless-stopped` starts it again | The Jetstream cursor makes the restart gapless within 36 hours, per the upgrade section above. No action needed unless it repeats |

## Health states

`GET /healthz` has exactly two states.

**Before the first Jetstream commit and the first scorer pass:** 503, body
`{"jetstream_lag_s":null,"last_pass_age_s":null,"snapshot_len":0}`.

**In steady state, both ages at or under `UPSTAGE_HEALTH_MAX_LAG_S`:** 200,
body `{"jetstream_lag_s":<int>,"last_pass_age_s":<int>,"snapshot_len":<int>}`.

Either age passing `UPSTAGE_HEALTH_MAX_LAG_S` (300 seconds by default) returns
the service to 503, with the same body shape and real integers, not `null`.

The first scorer pass runs at start, not one interval later, so on a fresh
volume the service normally reaches 200 within a few seconds of `up -d`.
The healthcheck's `start_period` is 120 seconds, a margin for a cold start
against an existing database, where the first pass has real work to do,
and for a slow first Jetstream commit. A check that fails inside
`start_period` does not count toward `retries`, so a generous value costs
nothing. A container still unhealthy after two minutes has a real
problem; read the logs. If you raise `UPSTAGE_SCORER_INTERVAL_S`, raise
`UPSTAGE_HEALTH_MAX_LAG_S` above it, or the container flips unhealthy between
passes. `last_pass_age_s` climbs to one full scorer interval between
passes, so a scorer interval above the threshold trips `/healthz` every
time.

## Cloudflare setup

Cloudflare terminates TLS for the feed and forwards plain HTTP to the
`upstage` container. Bluesky resolves `did:web:<hostname>` over HTTPS only,
so the hostname must answer on Cloudflare before the feed works.

### Where `UPSTAGE_HOSTNAME` comes from

Cloudflare does not give you this hostname. You choose it, as a subdomain of
a domain you own. Cloudflare then serves it.

1. Add your domain to Cloudflare as a zone, if it is not there yet.
2. At your registrar, set the nameservers to the two that Cloudflare shows.
3. Wait for the zone to reach the Active state. This can take some hours.
4. Pick a subdomain for the feed, for example `feed.example.com`.
5. Put this subdomain in `UPSTAGE_HOSTNAME` in `.env`.

The value is the bare hostname. Write no scheme, no port, no path, and no
trailing slash. Use lowercase only.

| Correct | Wrong |
|---|---|
| `feed.example.com` | `https://feed.example.com` |
| `feed.example.com` | `feed.example.com:3000` |
| `feed.example.com` | `feed.example.com/` |

The subdomain does not need a DNS record yet. Both variants below create
that record, and each one creates a different kind.

### Tunnel variant, `compose.yaml`

1. Open the Cloudflare Zero Trust dashboard. Go to Networks, then Tunnels.
2. Create a tunnel, and pick the `cloudflared` connector type.
3. Copy the tunnel token from the install command that Cloudflare shows.
4. Put the token in `TUNNEL_TOKEN` in `.env`.
5. Add a public hostname to the tunnel. Set its subdomain and domain to
   `UPSTAGE_HOSTNAME`. Leave the path empty.
6. Set the service type to HTTP and the service URL to `upstage:3000`.
7. Save the public hostname.

The token is the long string after `--token` in the install command, and not
the whole command.

`upstage:3000` is the Compose service name and its port. It is not a host
address. `cloudflared` reaches the container over the Compose network, which
is why `compose.yaml` publishes no port at all.

Step 7 creates the proxied CNAME record for the hostname. Do not also create
an A record. Two records for one name break the tunnel.

### Proxied variant, `compose.proxied.yaml`

1. Create an orange-cloud A record for `UPSTAGE_HOSTNAME`, pointing at the
   VM's public IP.
2. Set the TLS mode to Flexible. The origin, the `upstage` container, serves
   plain HTTP. Full and Full (strict) do not apply here, because there is
   no TLS certificate on the origin.
3. Restrict the VM firewall to Cloudflare's published IP ranges. Flexible
   TLS leaves the leg between Cloudflare and the VM unencrypted, so only
   Cloudflare's own IPs should reach port 3000.

CAUTION: Keep the record orange-cloud, that is proxied. A grey-cloud record
sends visitors straight to port 3000 over plain HTTP. Bluesky then cannot
resolve `did:web:<hostname>`, and the VM IP becomes public.

### Host-proxy variant, `compose.hostproxy.yaml`

When the host already runs `cloudflared` and nginx, use this variant. The
tunnel and its token stay as they are. You add one public hostname and one
nginx server block.

`compose.hostproxy.yaml` starts no `cloudflared` service. It publishes port
3000 on the loopback interface, as `127.0.0.1:3000:3000`. nginx reaches the
container over loopback. Nothing else reaches it, so the host needs no
inbound firewall rule.

The feed needs the root of its own hostname. It serves
`/.well-known/did.json` and the `/xrpc/` paths there. A path under an
existing hostname does not work.

1. Pick a subdomain for the feed. See "Where `UPSTAGE_HOSTNAME` comes from"
   above.
2. Make sure that port 3000 is free on the host:

   ```
   lsof -nP -iTCP:3000 -sTCP:LISTEN
   ```

   If another process holds the port, change the host side of the port line
   in `compose.hostproxy.yaml`, for example `127.0.0.1:3100:3000`. Keep the
   container side at 3000, or set `UPSTAGE_HTTP_ADDR` to the new port.
3. Add an nginx server block for the subdomain. The `listen` port is the
   port that the tunnel already sends traffic to:

   ```
   server {
       listen 8080;
       server_name <UPSTAGE_HOSTNAME>;

       location / {
           proxy_pass http://127.0.0.1:3000;
           proxy_set_header Host              $host;
           proxy_set_header X-Real-IP         $remote_addr;
           proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto https;
       }
   }
   ```

4. Test the nginx configuration with `nginx -t`. Then reload nginx.
5. Open the Cloudflare Zero Trust dashboard. Go to Networks, then Tunnels.
6. Open the tunnel that the host already runs. Add a public hostname.
7. Set its subdomain and domain to `UPSTAGE_HOSTNAME`. Leave the path
   empty.
8. Set the service type to HTTP. Set the service URL to `localhost:8080`,
   the nginx `listen` port.
9. Save the public hostname.

Leave `TUNNEL_TOKEN` empty. The connector on the host carries the traffic,
and `src/config.rs` never reads this variable.

Step 9 creates the proxied CNAME record for the hostname. Do not also
create an A record. Two records for one name break the tunnel.

CAUTION: Do not start a second `cloudflared` with the token of the tunnel
that the host already runs. Both connectors then serve one tunnel, and
Cloudflare divides the traffic between them. The connector that cannot
reach `upstage` answers with error 502.

### Confirming the hostname

Do this check after `up -d` and before `upstage publish`. `publish` writes
`did:web:<hostname>` into the feed record, so a wrong hostname publishes a
dead feed.

```
curl https://<UPSTAGE_HOSTNAME>/.well-known/did.json
```

The `id` field in the answer must read `did:web:<UPSTAGE_HOSTNAME>`, with the
same hostname you sent the request to. A difference means `.env` and
Cloudflare disagree.

| Result | Cause | Action |
|---|---|---|
| The `id` field holds another hostname | `UPSTAGE_HOSTNAME` in `.env` is wrong | Correct `.env`, then run `docker compose -f <file> up -d` again |
| Cloudflare error 1033 | The tunnel is not connected | Tunnel variant: read `docker compose -f compose.yaml logs -f cloudflared`. Host-proxy variant: read the logs of the host's `cloudflared` |
| A 502, tunnel variant | `cloudflared` cannot reach `upstage:3000` | Make sure that the container runs and that `/healthz` answers |
| A 502, host-proxy variant | nginx cannot reach the container, or `server_name` does not match | Run `nginx -t`. Then run `curl localhost:3000/healthz` on the host |
| Cloudflare error 521 | The proxy cannot reach port 3000 | Make sure that the firewall accepts Cloudflare's IP ranges |
| A TLS error | The record is grey-cloud, or the TLS mode is Full | Set the record to proxied, and the mode to Flexible |
| `NXDOMAIN` from the resolver | No DNS record exists for the hostname | Create the record for the variant you picked, above |

In every variant, `UPSTAGE_HOSTNAME` must match the Cloudflare hostname
exactly. It forms `did:web:<hostname>`, and the feed breaks if the two
differ.
