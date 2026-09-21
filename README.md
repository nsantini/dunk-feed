# Dunk Feed

Dunk Feed is a custom Bluesky feed generator, written as one Rust binary. It
watches the Jetstream firehose for quote posts, scores each pair by how much
the quote out-engaged the post it quotes, verifies the winners against the
Bluesky App View, and serves the result as an AT Protocol
`getFeedSkeleton` feed.

The product rules and the score live in [PRD.md](PRD.md). The implementation
design, the file layout, and the build order live in
[docs/TECH-DESIGN.md](docs/TECH-DESIGN.md), backed by the measured traffic
numbers in [docs/traffic-analysis.md](docs/traffic-analysis.md). The work is
broken into one story per file under [stories/](stories/); read
[stories/README.md](stories/README.md) for the build order and how to run
each story through the workflow plugin. Gate commands and code rules are in
[AGENTS.md](AGENTS.md).

To build and run this project you need a stable Rust toolchain through
`rustup` (`cargo`, `rustfmt`, `clippy`), and Docker for the container build.

To deploy, back up, or upgrade a running feed, see
[docs/RUNBOOK.md](docs/RUNBOOK.md).
