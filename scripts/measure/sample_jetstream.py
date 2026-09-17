import asyncio, json, time, sys, collections
import websockets

DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 360
URL = ("wss://jetstream2.us-east.bsky.network/subscribe"
       "?wantedCollections=app.bsky.feed.post"
       "&wantedCollections=app.bsky.feed.like"
       "&wantedCollections=app.bsky.feed.repost"
       "&wantedCollections=app.bsky.feed.postgate")

c = collections.Counter()
bytes_by_col = collections.Counter()
quote_targets = collections.Counter()
embed_types = collections.Counter()
postgate = collections.Counter()
first_us = last_us = None

async def main():
    global first_us, last_us
    start = time.time()
    async with websockets.connect(URL, max_size=None) as ws:
        while time.time() - start < DURATION:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
            except asyncio.TimeoutError:
                c["timeouts"] += 1; continue
            c["events"] += 1
            c["bytes"] += len(raw)
            ev = json.loads(raw)
            us = ev.get("time_us")
            if us:
                first_us = first_us or us; last_us = us
            if ev.get("kind") != "commit":
                c["kind:" + str(ev.get("kind"))] += 1; continue
            cm = ev["commit"]; col = cm["collection"]; op = cm["operation"]
            c[f"{col}:{op}"] += 1
            bytes_by_col[col] += len(raw)
            if col == "app.bsky.feed.post" and op == "create":
                rec = cm.get("record", {})
                if rec.get("reply"): c["post:reply"] += 1
                emb = rec.get("embed") or {}
                et = emb.get("$type", "none")
                embed_types[et] += 1
                target = None
                if et == "app.bsky.embed.record":
                    target = (emb.get("record") or {}).get("uri")
                elif et == "app.bsky.embed.recordWithMedia":
                    target = ((emb.get("record") or {}).get("record") or {}).get("uri")
                if target:
                    parts = target.split("/")
                    tcol = parts[3] if len(parts) > 3 else "?"
                    quote_targets[tcol] += 1
                    if tcol == "app.bsky.feed.post":
                        c["quote:post"] += 1
                        if parts[2] == ev.get("did"): c["quote:self"] += 1
                        if rec.get("reply"): c["quote:also_reply"] += 1
            if col == "app.bsky.feed.postgate" and op in ("create", "update"):
                rec = cm.get("record", {})
                postgate["records"] += 1
                postgate["detached_uris"] += len(rec.get("detachedEmbeddingUris") or [])
                for r in rec.get("embeddingRules") or []:
                    postgate["rule:" + r.get("$type", "?").split(".")[-1]] += 1
    elapsed = time.time() - start
    out = {"elapsed_s": round(elapsed, 1), "counts": dict(c), "bytes_by_collection": dict(bytes_by_col),
           "embed_types": dict(embed_types), "quote_targets": dict(quote_targets), "postgate": dict(postgate),
           "stream_span_s": (last_us - first_us) / 1e6 if first_us and last_us else None}
    print(json.dumps(out, indent=1))

asyncio.run(main())
