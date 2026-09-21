# Dunk Feed runbook

This runbook is for an operator. It does not assume you have read the Rust
source. It covers deploy, logs, backup, upgrade, tuning, and every failure
mode in `docs/TECH-DESIGN.md` section 13.

## Prerequisites

You need these before you start.

- Docker and the Compose plugin, installed on the VM.
- A Cloudflare account.
- A Bluesky account for the feed. See "Bluesky account setup" below.

## Bluesky account setup

Do this before the first deploy. It gives you the DID that `.env` needs and
the app password that `dunk publish` needs.

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
   puts them in `.env`, as `DUNK_PUBLISHER_DID`, `BSKY_HANDLE` and
   `BSKY_APP_PASSWORD`.

`DUNK_PUBLISHER_DID` holds the DID and not the handle, because a handle can
change. The at-URI of the feed must not change.

`publish` compares the DID of the account it signs in as against
`DUNK_PUBLISHER_DID`. A difference fails the command with a DID mismatch
error, and no record is written.

### The two DIDs

Dunk Feed uses two DIDs. They do different jobs and they are not
interchangeable.

| DID | Where it comes from | What it identifies |
|-----|--------------------|--------------------|
| `did:plc:...`, in `DUNK_PUBLISHER_DID` | Your Bluesky account | The owner of the feed record |
| `did:web:<hostname>`, built from `DUNK_HOSTNAME` | Your public hostname | The server that computes the feed |

The published record lives in the account's repository, and its own `did`
field holds `did:web:<hostname>`. This field is the pointer from Bluesky to
your VM. The server answers for that identity at `/.well-known/did.json`.

The feed's at-URI is
`at://<DUNK_PUBLISHER_DID>/app.bsky.feed.generator/<DUNK_FEED_RKEY>`.
Bluesky sends this URI in every feed request. The server serves only this one
URI and refuses every other.

## First deploy

1. Copy `.env.example` to `.env`.
2. Fill in `DUNK_HOSTNAME` and `DUNK_PUBLISHER_DID`. These are the only two
   required variables. `src/config.rs` rejects the container start when
   either one is empty or holds only whitespace.

   Three more variables have no default but are optional: `BSKY_HANDLE`
   and `BSKY_APP_PASSWORD`, needed only when you run `dunk publish`, and
   `TUNNEL_TOKEN`, needed only for the tunnel variant.
3. Pick one Compose file. Use `compose.yaml` for a Cloudflare Tunnel. Use
   `compose.proxied.yaml` for Cloudflare's proxied DNS with port 3000
   published. See "Cloudflare setup" below for both.
4. Run `docker compose -f <file> up -d --build`.
5. Watch `/healthz` turn from 503 to 200. See "Health states" below for the
   two bodies.

Both Compose files use the same `dunk-data` volume and the same Compose
project name. Do not run both variants at once against the same project;
they would share one database and one set of container names.

## Publishing the feed record

Run this once, after the container is healthy:

```
docker compose -f <file> run --rm dunk publish
```

Set `BSKY_HANDLE` and `BSKY_APP_PASSWORD` in `.env` first. `publish` needs
both. See "Bluesky account setup" above for both values.

### What publish does

`publish` writes one record and then stops. The record is one
`app.bsky.feed.generator` in your account's repository, under the record key
`DUNK_FEED_RKEY`, `dunks` by default. It goes to `https://bsky.social`, not
to the App View.

`publish` does not send posts or feed content to Bluesky. The container
serves the feed live, on every request, from your VM. What this command does
is register the feed, so that people can find it and pin it. Before it runs,
the feed does not exist for the Bluesky app.

The display name and the description are fixed in the source. Only a rebuild
changes them.

The command writes the record unconditionally, so you can run it again. A
second run replaces the record, which is how you add or change the avatar.

`ENTRYPOINT` in the image is `dunk`. `publish` replaces the default command,
so `run` does not start while this command runs.

To publish with an avatar, add `--avatar <path>`. The path must be visible
inside the container, not just on the host. Add a read-only bind mount to
the `dunk` service for the one run, for example:

```
docker compose -f <file> run --rm -v /home/you/avatar.png:/avatar.png:ro dunk publish --avatar /avatar.png
```

Pass the container path, `/avatar.png`, not the host path.

## Reading the logs

Dunk Feed logs JSON lines to stdout. `docker compose -f <file> logs -f dunk`
shows them. Three lines matter.

### `dunk: ingest stats`

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
| `dropped_unknown_collection` | Commits dropped because their collection is not one Dunk Feed tracks |
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
| `expired` | Feed rows removed for age, past `DUNK_FEED_TTL_D` |
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
| `labelled` | A label in `DUNK_DROP_LABELS` was found on the quote, on the original, or on either author's profile |
| `author_inactive` | The author's account is deactivated or deleted, or the original's author carries a `!takedown` label |
| `follower_floor` | The author's follower count is under `DUNK_FOLLOWER_FLOOR` |

### `scorer: guard histogram period follower distribution`

Logged once per pass, but only while the follower-floor histogram period is
open. It stops after `DUNK_GUARD_HISTOGRAM_H` hours from start. The follower
floor itself still drops candidates from the first pass onward, with or
without this line. Fields: `guard_would_drop`, and six follower-count
buckets: `zero`, `one_to_99`, `hundred_to_999`, `thousand_to_9999`,
`ten_k_to_99999`, `hundred_k_plus`.

## Backup

Back up the SQLite file nightly, from the host, against the named volume,
in a one-off container:

```
docker run --rm -v dunk-feed_dunk-data:/data -v /var/backups/dunk:/backup alpine/sqlite /data/dunk.db ".backup '/backup/dunk.db'"
```

Confirm the volume's real name first, with `docker volume ls`. Compose
prefixes the volume name with the project name, so it may not be exactly
`dunk-feed_dunk-data`.

This does not run as `docker compose exec dunk sqlite3 ...`. The runtime
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
instead of `latest`. This keeps an unrelated upgrade, such as a Dunk Feed
code change, from also pulling a new `cloudflared` release. To bump the
pin, check the current release on Docker Hub, edit the tag in
`compose.yaml`, then run `docker compose -f compose.yaml up -d --build` as
a normal upgrade.

## Tuning knobs

Each of these lives in `.env`. A change needs `docker compose -f <file> up
-d` to take effect. The binary reads the environment once, at start.

| Variable | Raising it | Lowering it |
|----------|------------|-------------|
| `DUNK_P` | Requires a more popular original post before a pair can score | Lets less popular original posts score |
| `DUNK_M` | Requires the quote to beat the original by a wider margin | Lets a smaller margin promote a pair |
| `DUNK_W_REPOST` | Weighs each repost more heavily in the score | Weighs each repost less heavily |
| `DUNK_W_REPLY` | Weighs each reply more heavily in the score | Weighs each reply less heavily |
| `DUNK_K` | Smooths the score more, damping small engagement counts further | Smooths the score less |
| `DUNK_FOLLOWER_FLOOR` | Requires more followers before an author's post can score. `0` disables the guard | Lets authors with fewer followers score |
| `DUNK_GUARD_HISTOGRAM_H` | Keeps the follower-distribution log line running longer after start | Stops the log line sooner. `0` disables the period; the floor stays live regardless |
| `DUNK_SCORER_INTERVAL_S` (default 60) | Runs the scorer pass less often, using less CPU but leaving new candidates unscored longer | Runs the pass more often, scoring candidates sooner but using more CPU. Raise `DUNK_HEALTH_MAX_LAG_S` above the new value too, or `/healthz` flips unhealthy between passes |
| `DUNK_REVERIFY_INTERVAL_S` (default 600) | Re-checks promoted pairs against the App View less often, using fewer App View calls but catching a block or a takedown later | Re-checks more often, catching a block or a takedown sooner but using more App View calls |
| `DUNK_HEALTH_MAX_LAG_S` (default 300) | Tolerates a longer gap since the last Jetstream commit or scorer pass before `/healthz` turns 503, so a slow patch is less likely to trip your monitoring | Tolerates a shorter gap, so `/healthz` catches a stall sooner but is more likely to flip on a normal slow pass |

### Tuning with `dunk dump`

`dunk dump --since 24h --out pairs.csv` writes one CSV row for every pair
first seen in the last 24 hours, in all three states: `candidate`,
`promoted` and `dropped`. `--since` takes a number and a unit, `h` or `d`,
for example `7d` for seven days. `--out` defaults to
`./dunk-dump-<since>.csv` when you leave it out.

Each row carries the pair's local counts from `counts`, its verified
counts from `feed` when it has a `feed` row, and `E` and `D` recomputed
from those counts with the config running right now. Open the CSV in a
spreadsheet and sort by these columns to re-fit each knob:

| Knob | Env var | Sort by |
|------|---------|---------|
| `P` | `DUNK_P` | The larger of `verified_e_q` and `verified_e_o`, over rows where `state` is `promoted` |
| `M` | `DUNK_M` | `verified_d`, over rows where `state` is `promoted` |
| Repost weight | `DUNK_W_REPOST` | `reposts_q` and `reposts_o` against `local_e_q` and `local_e_o` |
| Reply weight | `DUNK_W_REPLY` | `replies_q` and `replies_o` against `local_e_q` and `local_e_o` |

After you pick new values, edit `.env` and restart the container, as the
"Tuning knobs" table above describes. No code change is needed.

A pair that was promoted and later demoted shows local counts only: its
`v_*` cells, `verified_e_q`, `verified_e_o`, `verified_d` and
`promoted_at` are empty, because demoting a pair deletes its `feed` row.

## Failure modes

Each subsection says what you see and what to do.

### One Jetstream host down

The client rotates to the next host in `DUNK_JETSTREAM_URL` within one
backoff step. You see no `/healthz` change. No action needed.

### All Jetstream hosts down

Ingest backs off and retries. `/healthz` turns 503 after `lag_s` passes
`DUNK_HEALTH_MAX_LAG_S`, 300 seconds by default. The feed keeps serving its
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

`dunk run` supervises the ingest, scorer and HTTP tasks in one task
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
| BC1 | The `/data` volume is missing at container start | `DUNK_DB_PATH` write fails fast, the process exits non-zero | Docker's restart policy retries. Check that the volume is declared and attached |
| BC2 | `/healthz` returns 503 | Docker marks the container unhealthy after 3 failed checks | This is not a restart on its own. Read the logs and the "Health states" section below, then act |
| BC3 | `/healthz` returns 200 | Docker marks the container healthy | No action |
| BC4 | `TUNNEL_TOKEN` is missing or empty, tunnel variant | `cloudflared` exits non-zero immediately | `dunk` keeps running, reachable only on the Compose network. Set `TUNNEL_TOKEN` in `.env` and restart `cloudflared` |
| BC5 | Port 3000 is unreachable from outside, proxied variant | No response from the public hostname | Check the VM firewall and Cloudflare's proxy status. This is not a container-level failure |
| BC6 | Container memory use reaches 512 MB | Docker's `mem_limit` kills the container, `restart: unless-stopped` starts it again | The Jetstream cursor makes the restart gapless within 36 hours, per the upgrade section above. No action needed unless it repeats |

## Health states

`GET /healthz` has exactly two states.

**Before the first Jetstream commit and the first scorer pass:** 503, body
`{"jetstream_lag_s":null,"last_pass_age_s":null,"snapshot_len":0}`.

**In steady state, both ages at or under `DUNK_HEALTH_MAX_LAG_S`:** 200,
body `{"jetstream_lag_s":<int>,"last_pass_age_s":<int>,"snapshot_len":<int>}`.

Either age passing `DUNK_HEALTH_MAX_LAG_S` (300 seconds by default) returns
the service to 503, with the same body shape and real integers, not `null`.

The first scorer pass runs at start, not one interval later, so on a fresh
volume the service normally reaches 200 within a few seconds of `up -d`.
The healthcheck's `start_period` is 120 seconds, a margin for a cold start
against an existing database, where the first pass has real work to do,
and for a slow first Jetstream commit. A check that fails inside
`start_period` does not count toward `retries`, so a generous value costs
nothing. A container still unhealthy after two minutes has a real
problem; read the logs. If you raise `DUNK_SCORER_INTERVAL_S`, raise
`DUNK_HEALTH_MAX_LAG_S` above it, or the container flips unhealthy between
passes. `last_pass_age_s` climbs to one full scorer interval between
passes, so a scorer interval above the threshold trips `/healthz` every
time.

## Cloudflare setup

### Tunnel variant, `compose.yaml`

1. Create the tunnel in the Cloudflare dashboard.
2. Copy the tunnel token into `TUNNEL_TOKEN` in `.env`.
3. Set the tunnel's public hostname to `http://dunk:3000`. This is the
   Compose service name and port, not a host address.

### Proxied variant, `compose.proxied.yaml`

1. Create an orange-cloud A record for `DUNK_HOSTNAME`, pointing at the
   VM's public IP.
2. Set the TLS mode to Flexible. The origin, the `dunk` container, serves
   plain HTTP. Full and Full (strict) do not apply here, because there is
   no TLS certificate on the origin.
3. Restrict the VM firewall to Cloudflare's published IP ranges. Flexible
   TLS leaves the leg between Cloudflare and the VM unencrypted, so only
   Cloudflare's own IPs should reach port 3000.

In both variants, `DUNK_HOSTNAME` must match the Cloudflare hostname
exactly. It forms `did:web:<hostname>`, and the feed breaks if the two
differ.
