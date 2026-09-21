# syntax=docker/dockerfile:1

# --- Build stage -------------------------------------------------------
# Tag matches rust-toolchain.toml's pin exactly, so rustup installs no
# second toolchain at build time. It still adds the two components the
# file names, rustfmt and clippy; the build does not use them but the
# file's presence makes rustup fetch them anyway.
FROM rust:1.98.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src/ src/
RUN cargo build --release --locked

# --- Runtime stage -------------------------------------------------------
# debian:bookworm-slim, no added package. curl was measured and rejected:
# it fits under the 40 MB image cap (BC7) but leaves only 2.3 MB of
# headroom on an architecture this build has not measured, and it would
# add libcurl and its dependencies to a runtime surface a localhost health
# probe does not need. The healthcheck below uses bash's /dev/tcp instead.
FROM debian:bookworm-slim AS runtime

RUN groupadd --gid 10001 dunk \
    && useradd --uid 10001 --gid dunk --no-create-home --shell /usr/sbin/nologin dunk \
    && mkdir /data \
    && chown 10001:10001 /data

COPY --from=build /build/target/release/dunk /usr/local/bin/dunk

USER 10001:10001
VOLUME ["/data"]
EXPOSE 3000

# /healthz is 503 until both the first Jetstream commit and the first
# scorer pass. On a fresh volume that takes about 3 s. start-period is
# 120 s as a margin for a cold start against an existing database, where
# the first pass has real work to do, and for a slow first Jetstream
# commit; a check that fails inside start-period does not count toward
# retries, so a generous value costs nothing (BC12). The check sends one
# HTTP/1.1 request over /dev/tcp and greps the status line for " 200 ",
# so a 503, a refused connection and a hung socket all fail it (BC11).
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=120s \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/3000 && printf "GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n" >&3 && head -n 1 <&3 | grep -q " 200 "'

ENTRYPOINT ["dunk"]
CMD ["run"]
