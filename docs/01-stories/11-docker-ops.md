# 11 — Docker, Compose, Cloudflare, runbook

- **Follows**: 08
- **PRD phase**: 2
- **Size**: standard
- **Design**: docs/01-TECH-DESIGN.md §13

## Outcome

After this ships, `docker build` produces a multi-stage image under 40 MB
that runs as non-root with a `/data` volume, and `docker compose up -d`
starts it with a healthcheck on `/healthz`, either behind a Cloudflare
Tunnel or with the port published for Cloudflare's proxied DNS.
`docs/RUNBOOK.md` tells an operator how to deploy, back up, upgrade, and
read the failure modes in §13, without opening the Rust source.

## Non-goals

- Does not automate the backup or Cloudflare Tunnel token provisioning;
  the runbook documents manual steps.
- Does not add a second binary or an orchestration layer beyond Compose.
- Does not change any application code; this story is operations only.
- Does not implement a staging environment; one VM, one Compose file per
  variant.

## Approach

The Dockerfile is multi-stage, `rust:1-bookworm` to build and
`debian:bookworm-slim` to run, because a slim runtime image with a full
build toolchain in one layer would blow past the 40 MB target. Two Compose
files ship: one with a `cloudflared` service reading `TUNNEL_TOKEN`, and
one that publishes port 3000 directly, because §13 says both work with the
same container, and the choice depends on whether the VM has a public IP
behind Cloudflare's proxy.

## Files in scope

| Path | Change |
|---|---|
| `Dockerfile` | Multi-stage build, non-root, `/data` volume |
| `compose.yaml` | Cloudflare Tunnel variant: `upstage` and `cloudflared` services |
| `compose.proxied.yaml` | Proxied-DNS variant: `upstage` only, port 3000 published |
| `docs/RUNBOOK.md` | Deploy, backup, upgrade, failure modes |
| `.env.example` | Adds `TUNNEL_TOKEN` (tunnel variant only) |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | image, `/data` volume missing at container start | edge case | `UPSTAGE_DB_PATH` write fails fast; Docker's restart policy retries; documented in RUNBOOK, not a code path |
| BC2 | healthcheck | `/healthz` returns 503 | Docker marks the container unhealthy after the configured retries; it does not restart on its own (Compose's default); an operator action, per RUNBOOK |
| BC3 | healthcheck | `/healthz` returns 200 | Container marked healthy |
| BC4 | compose, tunnel variant | `TUNNEL_TOKEN` missing | `cloudflared` exits non-zero immediately; `upstage` keeps running, reachable only on the VM's private network until the token is set |
| BC5 | compose, proxied variant | port 3000 unreachable from outside | Operator checks the VM's firewall and Cloudflare's proxy status, per RUNBOOK; not a container-level failure |
| BC6 | memory limit | container RSS approaches 512 MB | Docker's `mem_limit` kills and restarts the container; the Jetstream cursor makes the restart gapless within 36h, per §13 |
| BC7 | image size | build output | Final image under 40 MB |

## Acceptance criteria

- [ ] AC1 — `docker build .` produces an image under 40 MB. Checked by: `docker build -t upstage . && docker image inspect upstage --format='{{.Size}}'` (value under 40000000)
- [ ] AC2 — The container runs as non-root. Checked by: `docker run --rm upstage id -u` (non-zero)
- [ ] AC3 — Both Compose files validate. Checked by: `docker compose -f compose.yaml config` and `docker compose -f compose.proxied.yaml config` (both exit 0)
- [ ] AC4 — The healthcheck targets `/healthz`. Checked by: reviewer reads `compose.yaml` and `compose.proxied.yaml`
- [ ] AC5 — `docs/RUNBOOK.md` covers deploy, backup, upgrade, and every §13 failure mode. Checked by: reviewer reads `docs/RUNBOOK.md`
- [ ] AC6 — All four gates pass, confirming no application code changed.

## Defaults taken

- Base images: `rust:1-bookworm` (build stage), `debian:bookworm-slim`
  (run stage), pinned by tag, matching §13's own wording.
- Healthcheck: `curl -f http://localhost:3000/healthz`, every 30s, 3
  retries, 10s start period; `curl` is added to the runtime stage only for
  this check.
- Backup: documented in RUNBOOK as a cron entry running
  `sqlite3 /data/upstage.db ".backup /data/backup.db"` nightly, per §13; not
  automated by Compose itself.
- `compose.yaml` is the Cloudflare Tunnel variant, the more common path for
  a VM with no public IP; `compose.proxied.yaml` is the alternative.
  RUNBOOK explains when to pick each.

## Suggested slices

- 1.0 `Dockerfile` and its healthcheck. Done when `docker build -t upstage .`
  succeeds and the image is under 40 MB.
- 2.0 `compose.yaml` (tunnel) and `compose.proxied.yaml` (proxied DNS).
  Done when both `docker compose ... config` commands validate.
- 3.0 `docs/RUNBOOK.md`. Done when reviewer reads it and confirms deploy,
  backup, upgrade, and failure modes are covered.
