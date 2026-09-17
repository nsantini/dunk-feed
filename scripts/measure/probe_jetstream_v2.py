import asyncio, json, websockets
URL = "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents?collections=app.bsky.feed.post&kinds=commit"
async def main():
    async with websockets.connect(URL, max_size=None) as ws:
        for i in range(2):
            raw = await asyncio.wait_for(ws.recv(), timeout=15)
            ev = json.loads(raw)
            p = ev.get("payload", ev)
            if "record" in p: p["record"] = {k: (v if k != "text" else "<text>") for k, v in p["record"].items() if k in ("$type","text","embed","reply")}
            print(json.dumps(ev, indent=1)[:1500]); print("----")
asyncio.run(main())
