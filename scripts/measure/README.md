# Measurement scripts

Python scripts used for `docs/traffic-analysis.md`. They are not part of the
service and have no gate. They need Python 3 and the `websockets` package.

```bash
python3 -m venv .venv && .venv/bin/pip install websockets
```

| Script | What it does | Run |
|---|---|---|
| `sample_jetstream.py` | Counts Jetstream v1 events by collection, embed type and quote target for N seconds. Prints JSON | `.venv/bin/python scripts/measure/sample_jetstream.py 3600 > sample.json` |
| `probe_jetstream_v2.py` | Prints two raw Jetstream v2 frames so you can see the envelope | `.venv/bin/python scripts/measure/probe_jetstream_v2.py` |
| `hot_feed_probe.py` | Reads `hot-classic` and `whats-hot`, prints like percentiles, then scores quote pairs seeded from hot posts | `.venv/bin/python scripts/measure/hot_feed_probe.py` |

Run `sample_jetstream.py` for 24 hours before you tune `P` and `M`. The
2026-09-18 sample is in `docs/measurements/`.
