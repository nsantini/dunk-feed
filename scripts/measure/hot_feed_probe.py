import json, urllib.request, urllib.parse, statistics, math, time
API = "https://public.api.bsky.app/xrpc/"
def get(method, **params):
    q = urllib.parse.urlencode(params, doseq=True)
    with urllib.request.urlopen(API + method + "?" + q, timeout=30) as r:
        return json.load(r)
def E(p, wr=2.0, wc=0.5):
    return p.get("likeCount",0) + wr*p.get("repostCount",0) + wc*p.get("replyCount",0)

feeds = {"hot-classic": "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/hot-classic",
         "whats-hot": "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot"}
out = {}
for name, uri in feeds.items():
    try:
        posts = []; cursor = None
        for _ in range(2):
            kw = {"feed": uri, "limit": 100}
            if cursor: kw["cursor"] = cursor
            r = get("app.bsky.feed.getFeed", **kw)
            posts += [it["post"] for it in r.get("feed", [])]
            cursor = r.get("cursor")
            if not cursor: break
        likes = sorted(p.get("likeCount",0) for p in posts)
        quotes = sorted(p.get("quoteCount",0) for p in posts)
        ages = []
        for p in posts:
            try:
                t = time.strptime(p["record"]["createdAt"][:19], "%Y-%m-%dT%H:%M:%S")
                ages.append((time.time() - time.mktime(t) + time.timezone)/3600)
            except Exception: pass
        pct = lambda a, q: a[min(len(a)-1, int(q*len(a)))] if a else None
        out[name] = {"n": len(posts), "likes_min": likes[0] if likes else None, "likes_p10": pct(likes,.1), "likes_p50": pct(likes,.5),
                     "likes_p90": pct(likes,.9), "likes_max": likes[-1] if likes else None,
                     "quotes_p50": pct(quotes,.5), "quotes_p90": pct(quotes,.9), "quotes_max": quotes[-1] if quotes else None,
                     "share_with_any_quote": round(sum(1 for q in quotes if q>0)/len(quotes),2) if quotes else None,
                     "age_h_p50": round(pct(sorted(ages),.5),1) if ages else None, "age_h_max": round(max(ages),1) if ages else None}
        out[name]["_posts"] = posts
    except Exception as e:
        out[name] = {"error": str(e)}

# mini phase-0: for the most-quoted hot posts, pull quotes and compute upstage ratios
src = out.get("hot-classic", {}).get("_posts") or out.get("whats-hot", {}).get("_posts") or []
cands = sorted(src, key=lambda p: -p.get("quoteCount",0))[:30]
pairs = []; calls = 0
for O in cands:
    if O.get("quoteCount",0) == 0: continue
    try:
        r = get("app.bsky.feed.getQuotes", uri=O["uri"], limit=100); calls += 1
    except Exception as e:
        continue
    for Q in r.get("posts", []):
        if Q["author"]["did"] == O["author"]["did"]: continue
        eo, eq = E(O), E(Q)
        D = eq / (eo + 5)
        pairs.append({"D": round(D,3), "EO": eo, "EQ": eq, "O_likes": O.get("likeCount"), "Q_likes": Q.get("likeCount"),
                      "O_quotes": O.get("quoteCount"), "Q_followers": None, "Q_uri": Q["uri"], "O_uri": O["uri"]})
pairs.sort(key=lambda x: -x["D"])
summary = {"hot_posts_checked": len([c for c in cands if c.get("quoteCount",0)>0]), "getQuotes_calls": calls, "pairs": len(pairs),
           "pairs_D_ge_1.25": sum(1 for p in pairs if p["D"]>=1.25), "pairs_D_ge_0.5": sum(1 for p in pairs if p["D"]>=0.5),
           "pairs_EQ_ge_50": sum(1 for p in pairs if p["EQ"]>=50), "top10": pairs[:10]}
for k in out:
    out[k].pop("_posts", None)
print(json.dumps({"feeds": out, "upstage_probe": summary}, indent=1))
