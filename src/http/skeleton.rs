//! `GET /xrpc/app.bsky.feed.getFeedSkeleton` (BC3 to BC11, BC17 to BC22,
//! BC32, BC33, BC53 to BC59): the paginated feed read, served from the
//! in-memory snapshot, never a store read. TECH-DESIGN section 11.1
//! (rewritten at `c4c6fbd`, story 08 slice 7.0): the snapshot handle keeps
//! the current generation and the one before it, and a cursor names both a
//! generation and an index, so three-path resolution (`resolve_start`)
//! finds the resume point in this order: (1) the cursor's generation is
//! still held and its index names the same `quote_cid` there — an exact,
//! O(1) resume, no scan; (2) otherwise scan the *current* list for that
//! `quote_cid`; (3) otherwise fall back to the rank/cid scan
//! (`page_start`) round 1's slice 6.0 built, which can repeat or drop an
//! item only when reached this way (BC39, rewritten). Path 1 turns the
//! common case — no swap since the last request — into a lookup with no
//! scan at all, well under TECH-DESIGN section 11.1's one-scan budget.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::header::CACHE_CONTROL;
use axum::http::HeaderValue;
use axum::response::{IntoResponse, Json, Response};
use serde::Serialize;
use thiserror::Error;

use crate::http::cursor::{self, CursorError};
use crate::http::AppState;
use crate::scorer::snapshot::{cmp_rank_then_cid, FeedItem, Snapshot};

/// Every way `getFeedSkeleton` can fail. Mapped to the 400 bodies in BC3,
/// BC4, BC6 by `SkeletonError`'s `IntoResponse` impl in `src/http/mod.rs`
/// (BC17).
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum SkeletonError {
    /// BC3, BC21: `feed` missing, or not the configured feed URI.
    #[error("unknown feed")]
    UnknownFeed,
    /// BC4, BC6, BC19: `limit` non-numeric, or `cursor` fails to decode.
    #[error("invalid request")]
    InvalidRequest,
}

impl From<CursorError> for SkeletonError {
    fn from(_: CursorError) -> Self {
        SkeletonError::InvalidRequest
    }
}

#[derive(Debug, Serialize)]
pub struct SkeletonResponse<'a> {
    pub feed: Vec<SkeletonItem<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Round 2 finding 7: `post` borrows straight from the snapshot `Arc` the
/// caller holds, rather than cloning every URI in a page for a response
/// that only needs to read it once and serialise it. `feed_context` still
/// owns its `String`: it is not a literal field of the snapshot, but a
/// `format!` computed fresh per response (BC9).
#[derive(Debug, Serialize)]
pub struct SkeletonItem<'a> {
    pub post: &'a str,
    #[serde(rename = "feedContext")]
    pub feed_context: String,
}

/// `at://<publisher_did>/app.bsky.feed.generator/<rkey>`, the one URI BC3
/// accepts, read straight off `HttpConfig` (BC44, BC45): the same
/// precomputed string `describe::handler` serves, never reformatted here.
fn expected_feed_uri(state: &AppState) -> &str {
    &state.cfg.feed_uri
}

/// A digit string, with an optional leading `-`, parsed leniently into
/// `i128` so a value that overflows `i64` (BC20) still parses instead of
/// erroring: the final clamp to `1..=100` never needs more range than that.
/// A digit run too long even for `i128` clamps to that type's own extreme
/// instead of erroring, since it is unambiguously "far outside the valid
/// range" either way.
fn parse_lenient_i128(raw: &str) -> Result<i128, SkeletonError> {
    if raw.is_empty() {
        return Err(SkeletonError::InvalidRequest);
    }
    let (negative, digits) = match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SkeletonError::InvalidRequest);
    }
    match digits.parse::<i128>() {
        Ok(value) => Ok(if negative { -value } else { value }),
        Err(_) => Ok(if negative { i128::MIN } else { i128::MAX }),
    }
}

/// BC4, BC5, BC18, BC19, BC20: `None` (omitted) defaults to 50 unclamped;
/// anything else must be numeric (BC4, BC19) and is then clamped to
/// `1..=100` (BC5), even when it overflows `i64` (BC20).
fn resolve_limit(raw: Option<&str>) -> Result<usize, SkeletonError> {
    let Some(raw) = raw else { return Ok(50) };
    let value = parse_lenient_i128(raw)?;
    Ok(value.clamp(1, 100) as usize)
}

/// BC6, BC22, BC52: `None` or an empty string is treated as absent (page
/// starts at the current generation's index 0); anything else must decode
/// (BC6, BC52) or the request is `InvalidRequest`.
fn resolve_cursor(raw: Option<&str>) -> Result<Option<(u64, usize, f64, String)>, SkeletonError> {
    match raw {
        None | Some("") => Ok(None),
        Some(s) => Ok(Some(cursor::decode(s)?)),
    }
}

/// Path 3 (BC55): the index into `global` the page starts at, found in one
/// linear scan of `global` (TECH-DESIGN section 11.1's budget line;
/// `caps::apply`'s cap 2 leaves `global` not totally ordered by `(rank
/// DESC, cid ASC)` over `items`, so a binary search has no defined answer
/// over it). Reached only once path 1 and path 2 (`resolve_start`) have
/// both failed to place the cursor. `cursor_value: None` (BC22, BC57)
/// always starts at 0 without scanning.
///
/// Round 2 finding 1: an exact `(rank, cid)` match always wins, wherever it
/// sits in the scan, over any earlier index that merely sorts after the
/// cursor by `cmp_rank_then_cid` — the scan never returns early on that
/// weaker signal; it only remembers the first one as a fallback and keeps
/// going. That is the whole invariant: **never go backwards, so never
/// repeat within a session.** `caps::apply`'s cap 2 can place an item
/// earlier in `global` than its raw `(rank, cid)` would otherwise sort — a
/// cap-2 deferral — so an earlier position can legitimately sort after the
/// cursor (BC37) while the cursor's own item still sits later, not yet
/// reached; returning that earlier position would replay whatever this
/// pagination session already served up to it. Scanning to completion for
/// the exact match instead means a position once returned is never
/// returned again, no matter how the deferral reordered `global` (BC37:
/// exact match at `k` always starts the next page at `k + 1`; BC38: with no
/// exact match at all, the first-sorts-after fallback stands).
///
/// The accepted cost (BC39, rewritten for slice 7.0): this path alone can
/// still repeat or drop an item, and only when a cursor is older than two
/// scorer passes — path 1 and path 2 (`resolve_start`) both cover a cursor
/// from the current or the immediately preceding generation exactly, so
/// this fallback is reached only once a cursor's own generation has fallen
/// out of both slots AND its `quote_cid` no longer appears in the current
/// list at all. In that narrow case the scan may find no exact match and no
/// legitimate fallback either (for example, cap 2 had deferred the
/// cursor's own item behind items that sort after it, and the next
/// generation dropped or re-ordered around it); the page then truncates —
/// empty, with no `cursor` in the response — rather than repeating or
/// looping. A plain refresh, a fresh request with no cursor, still serves
/// every item in the current snapshot.
fn page_start(items: &[FeedItem], cursor_value: Option<(f64, String)>, global: &[u32]) -> usize {
    let Some((rank, cid)) = cursor_value else { return 0 };
    let mut after: Option<usize> = None;
    for (i, &idx) in global.iter().enumerate() {
        let item = &items[idx as usize];
        match cmp_rank_then_cid((item.rank, &item.quote_cid), (rank, &cid)) {
            std::cmp::Ordering::Equal => return i + 1,
            std::cmp::Ordering::Greater => {
                if after.is_none() {
                    after = Some(i);
                }
            }
            std::cmp::Ordering::Less => {}
        }
    }
    after.unwrap_or(global.len())
}

/// Three-path page resolution (TECH-DESIGN section 11.1, BC53 to BC57): given
/// the two generations `SnapshotHandle::generations` returned (BC50, one
/// call per request) and the decoded cursor, picks which generation's list
/// to serve from and the index it starts at within that generation's
/// `global`. `None` (BC22, BC57) always resolves to `(current, 0)` without
/// scanning.
///
/// Path 1 (BC53, BC59): the cursor's `generation` matches `current` or
/// `previous`, and that generation's `index` is a position in `global` in
/// bounds, naming an item in `items` with the same `quote_cid` — an exact
/// O(1) resume at `index + 1` of that same generation. An out-of-bounds
/// index or a `quote_cid` mismatch at that index means the generation no
/// longer looks the way the cursor remembers it (BC59), so path 1 does not
/// apply and resolution falls through rather than trusting a stale index.
///
/// Path 2 (BC54): path 1 did not apply. Scan the *current* generation's
/// `global`, in order, for the item with the cursor's `quote_cid`; if one
/// is found, start just after its position in `global`, in the current
/// generation.
///
/// Path 3 (BC55): neither path applied. Fall back to `page_start`'s
/// rank/cid scan over the current generation's `global` — the one path
/// BC39 accepts as capable of repeating or dropping an item, reached only
/// once the cursor's generation has fallen out of both slots and its
/// `quote_cid` no longer resolves in the current list either.
fn resolve_start<'a>(
    current: &'a Snapshot,
    previous: &'a Option<Snapshot>,
    cursor_value: Option<(u64, usize, f64, String)>,
) -> (&'a Snapshot, usize) {
    let Some((cursor_generation, cursor_index, cursor_rank, cursor_cid)) = cursor_value else {
        // BC57: no cursor (or an empty one, BC22) serves the current
        // generation from index 0.
        return (current, 0);
    };

    let matched_generation: Option<&Snapshot> = if current.generation == cursor_generation {
        Some(current)
    } else {
        previous.as_ref().filter(|snap| snap.generation == cursor_generation)
    };

    if let Some(snap) = matched_generation {
        // BC59: an out-of-bounds index falls through rather than panicking.
        if let Some(&idx) = snap.global.get(cursor_index) {
            if snap.items[idx as usize].quote_cid == cursor_cid {
                return (snap, cursor_index + 1);
            }
        }
    }

    // Path 2 (BC54): one scan of the current generation's `global` by
    // `quote_cid`.
    if let Some(pos) =
        current.global.iter().position(|&idx| current.items[idx as usize].quote_cid == cursor_cid)
    {
        return (current, pos + 1);
    }

    // Path 3 (BC55, BC39): the rank/cid scan, over the current generation's
    // `global` only.
    let start = page_start(&current.items, Some((cursor_rank, cursor_cid)), &current.global);
    (current, start)
}

/// The route's pure core: given `state`, the two generations
/// `SnapshotHandle::generations` returned (already read once by the caller,
/// BC50, AC5) and the raw query params, builds the response body or a
/// `SkeletonError`. Split out from `handler` so a test can drive it without
/// a live router. `SkeletonResponse<'a>`'s items borrow from whichever
/// generation `resolve_start` picked (round 2 finding 7), so the borrow's
/// lifetime is the caller's `Snapshot`, not one local to this function.
fn build<'a>(
    state: &AppState,
    current: &'a Snapshot,
    previous: &'a Option<Snapshot>,
    params: &HashMap<String, String>,
) -> Result<SkeletonResponse<'a>, SkeletonError> {
    // BC3, BC21: `feed` missing, or a value other than the configured feed
    // URI (duplicates already resolved to the last occurrence by `Query`'s
    // `HashMap` deserialization). BC44, BC45: `state.cfg.feed_uri` is read
    // straight off `HttpConfig`, never reformatted here.
    let expected = expected_feed_uri(state);
    match params.get("feed") {
        Some(feed) if feed == expected => {}
        _ => return Err(SkeletonError::UnknownFeed),
    }

    let limit = resolve_limit(params.get("limit").map(String::as_str))?;
    let cursor_value = resolve_cursor(params.get("cursor").map(String::as_str))?;

    let (snapshot, start) = resolve_start(current, previous, cursor_value);
    let items = snapshot.items.as_slice();
    let global = snapshot.global.as_slice();
    let page: Vec<&FeedItem> =
        global.iter().skip(start).take(limit).map(|&idx| &items[idx as usize]).collect();

    // BC8, BC56: omitted once the page reaches the end of that generation's
    // `global`; otherwise names the generation it was served from and the
    // index into `global` of the last item served within that generation.
    let cursor = if start + page.len() < global.len() {
        let last_index = start + page.len() - 1;
        page.last()
            .map(|item| cursor::encode(snapshot.generation, last_index, item.rank, &item.quote_cid))
    } else {
        None
    };

    let feed = page
        .into_iter()
        .map(|item| SkeletonItem {
            post: item.quote_uri.as_str(),
            // BC9: one decimal place; a plain `f64` formats well under the
            // 2,000-char ceiling BC9 names.
            feed_context: format!("r={:.1}", item.ratio),
        })
        .collect();

    Ok(SkeletonResponse { feed, cursor })
}

/// BC10: no auth required; a present service JWT (in the `Authorization`
/// header) is never read here, so it is accepted without validation by
/// omission.
pub async fn handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // BC50, AC5: both generations are read exactly once per request, under
    // a single read lock, kept alive here for as long as `build`'s
    // borrowed response needs them.
    let (current, previous) = state.snapshot.generations();
    let mut response = match build(&state, &current, &previous, &params) {
        Ok(body) => Json(body).into_response(),
        Err(err) => err.into_response(),
    };
    // BC11, BC33: set on every response, success or 400 alike.
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("public, max-age=30"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::router;
    use crate::http::tests::{test_config, test_state};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;

    const FEED_URI: &str = "at://did:plc:abc/app.bsky.feed.generator/upstaged";

    fn item(quote_uri: &str, quote_cid: &str, rank: f64, ratio: f64) -> FeedItem {
        FeedItem {
            quote_uri: quote_uri.to_string(),
            quote_cid: quote_cid.to_string(),
            rank,
            ratio,
            quote_did: 1,
            original_did: 2,
            quoted_at: 1_700_000_000,
            promoted_at: 1_700_000_000,
        }
    }

    /// Fixture snapshots build `items` already in the shape the caps would
    /// have produced, so `global` is every index in order — no cap is
    /// exercised again by these fixtures, which are about pagination, not
    /// capping (that lives in `scorer::snapshot::caps`).
    fn state_with_items(items: Vec<FeedItem>) -> Arc<AppState> {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        let global: Vec<u32> = (0..items.len() as u32).collect();
        state.snapshot.swap(Arc::new(items), Arc::new(global));
        state
    }

    // BC3: `feed` missing.
    #[tokio::test]
    async fn missing_feed_is_unknown_feed() {
        let state = state_with_items(vec![]);
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        // BC33: Cache-Control still set on a 400.
        assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "public, max-age=30");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "UnknownFeed");
    }

    // BC3: `feed` present but wrong.
    #[tokio::test]
    async fn wrong_feed_is_unknown_feed() {
        let state = state_with_items(vec![]);
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://someone/else")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // BC21: duplicate `feed`, the last occurrence wins, then BC3 decides.
    #[tokio::test]
    async fn duplicate_feed_last_wins() {
        let state = state_with_items(vec![]);
        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://wrong&feed={FEED_URI}");
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // BC4, BC19: non-numeric and empty `limit`.
    #[tokio::test]
    async fn non_numeric_limit_is_invalid_request() {
        for limit in ["abc", ""] {
            let state = state_with_items(vec![]);
            let app = router(state);
            let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit={limit}");
            let response = app
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "limit={limit:?}");
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["error"], "InvalidRequest");
        }
    }

    // AC2, BC5, BC18, BC20: clamping, defaulting, duplicate-wins, and
    // overflow-still-numeric all at once.
    #[test]
    fn limit_clamps() {
        assert_eq!(resolve_limit(None).unwrap(), 50, "omitted defaults to 50");
        assert_eq!(resolve_limit(Some("0")).unwrap(), 1);
        assert_eq!(resolve_limit(Some("101")).unwrap(), 100);
        assert_eq!(resolve_limit(Some("-5")).unwrap(), 1);
        assert_eq!(resolve_limit(Some("50")).unwrap(), 50);
        assert_eq!(resolve_limit(Some("100")).unwrap(), 100);
        assert_eq!(resolve_limit(Some("1")).unwrap(), 1);
        // BC20: all digits, beyond i64 range, still numeric so it clamps.
        assert_eq!(resolve_limit(Some("999999999999999999999999999999")).unwrap(), 100);
        assert_eq!(resolve_limit(Some("-999999999999999999999999999999")).unwrap(), 1);
    }

    // BC6: `cursor` fails to decode.
    #[tokio::test]
    async fn bad_cursor_is_invalid_request() {
        let state = state_with_items(vec![]);
        let app = router(state);
        let uri =
            format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&cursor=not-valid!!!");
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "InvalidRequest");
    }

    // BC22: an empty `cursor` is treated as absent.
    #[tokio::test]
    async fn empty_cursor_starts_at_zero() {
        let items = vec![item("at://q/1", "cid1", 3.0, 1.0), item("at://q/2", "cid2", 2.0, 1.0)];
        let state = state_with_items(items);
        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&cursor=");
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"][0]["post"], "at://q/1");
    }

    // BC8: happy path shape, cursor omitted on the last page.
    #[tokio::test]
    async fn happy_path_returns_the_shape_and_omits_cursor_on_the_last_page() {
        let items = vec![item("at://q/1", "cid1", 3.0, 4.5)];
        let state = state_with_items(items);
        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "public, max-age=30");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"][0]["post"], "at://q/1");
        // BC9: one decimal place.
        assert_eq!(json["feed"][0]["feedContext"], "r=4.5");
        assert!(json.get("cursor").is_none(), "the last page must omit cursor");
    }

    // BC8: a page short of the full snapshot carries a `cursor`.
    #[tokio::test]
    async fn cursor_is_present_when_more_items_remain() {
        let items = vec![item("at://q/1", "cid1", 3.0, 1.0), item("at://q/2", "cid2", 2.0, 1.0)];
        let state = state_with_items(items);
        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=1");
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"].as_array().unwrap().len(), 1);
        assert!(json["cursor"].is_string());
    }

    /// The identity `global` for a fixture's `items`: every index in order,
    /// so `page_start`'s tests exercise the scan without a cap reordering
    /// anything (`deferral_snapshot` below sets up the reordered case).
    fn ids(items: &[FeedItem]) -> Vec<u32> {
        (0..items.len() as u32).collect()
    }

    /// `state.snapshot.swap` with the identity `global`, for tests that
    /// swap in a fresh generation's `items` directly and are not exercising
    /// a cap.
    fn swap_items(state: &Arc<AppState>, items: Vec<FeedItem>) {
        let global = ids(&items);
        state.snapshot.swap(Arc::new(items), Arc::new(global));
    }

    // BC7, BC32: page_start behaviour, unit-tested directly.
    #[test]
    fn page_start_exact_match_starts_after_it() {
        let items = vec![
            item("at://q/1", "cid1", 3.0, 1.0),
            item("at://q/2", "cid2", 2.0, 1.0),
            item("at://q/3", "cid3", 1.0, 1.0),
        ];
        assert_eq!(page_start(&items, Some((2.0, "cid2".to_string())), &ids(&items)), 2);
    }

    #[test]
    fn page_start_stale_cursor_starts_strictly_after() {
        // A cursor whose (rank, cid) no longer matches any item still finds
        // the first item that sorts strictly after it.
        let items = vec![item("at://q/1", "cid1", 3.0, 1.0), item("at://q/3", "cid3", 1.0, 1.0)];
        // A stale cursor at rank 2.0 sorts between the two remaining items.
        assert_eq!(page_start(&items, Some((2.0, "cidX".to_string())), &ids(&items)), 1);
    }

    #[test]
    fn page_start_cursor_past_the_end_returns_len() {
        let items = vec![item("at://q/1", "cid1", 3.0, 1.0)];
        assert_eq!(page_start(&items, Some((0.0, "zzz".to_string())), &ids(&items)), 1);
    }

    /// A snapshot holding a genuine cap-2 deferral: `cid6`'s rank (50.0) is
    /// higher than `cid3`, `cid4` and `cid5`'s, yet it sits at array index 6,
    /// after them — exactly the reviewer's own counterexample for why the
    /// rejected `min(first-after, exact + 1)` scan never terminates (round
    /// 2 finding 1, `tasks.md` 6.1). `items` is intentionally not totally
    /// ordered by `(rank DESC, cid ASC)`, matching what
    /// `snapshot::apply_cap_one_per_quoter_per_50` can actually produce.
    fn deferral_snapshot() -> Vec<FeedItem> {
        vec![
            item("at://q/0", "cid0", 100.0, 1.0),
            item("at://q/1", "cid1", 90.0, 1.0),
            item("at://q/2", "cid2", 89.0, 1.0),
            item("at://q/3", "cid3", 40.0, 1.0),
            item("at://q/4", "cid4", 39.9, 1.0),
            item("at://q/5", "cid5", 39.8, 1.0),
            item("at://q/6", "cid6", 50.0, 1.0), // deferred past lower ranks
            item("at://q/7", "cid7", 30.0, 1.0),
        ]
    }

    // BC37: the cursor's exact match at index 6 must win over the earlier
    // index 3, whose (rank, cid) sorts strictly after the cursor's value —
    // the scan must not stop at that earlier "Greater" the moment it sees
    // one; it must keep looking for the exact match. Returning index 3 here
    // would replay cid1..cid3, already served earlier in a real session.
    #[test]
    fn page_start_exact_match_wins_over_an_earlier_sorts_after_index() {
        let items = deferral_snapshot();
        assert_eq!(page_start(&items, Some((50.0, "cid6".to_string())), &ids(&items)), 7);
    }

    // 6.2: paging one item at a time over `deferral_snapshot` terminates
    // and serves every item exactly once, in array order, proving BC37's
    // "never go backwards, so never repeat within a session" invariant
    // holds across a real cap-2 deferral shape.
    #[test]
    fn page_start_pages_a_deferral_snapshot_exactly_once_and_terminates() {
        let items = deferral_snapshot();
        let global = ids(&items);
        let mut cursor: Option<(f64, String)> = None;
        let mut served = Vec::new();

        for _ in 0..=items.len() {
            let start = page_start(&items, cursor.clone(), &global);
            if start >= items.len() {
                break;
            }
            served.push(items[start].quote_cid.clone());
            cursor = Some((items[start].rank, items[start].quote_cid.clone()));
        }

        let expected: Vec<String> = items.iter().map(|i| i.quote_cid.clone()).collect();
        assert_eq!(served, expected, "every item served exactly once, in array order");
    }

    // BC39: pagination across two snapshot generations. Between two
    // requests the snapshot swaps to a new generation whose every surviving
    // item now sorts before (higher rank than) the stale cursor — as
    // happens when cap 2 has deferred the cursor's own item behind items
    // that sort after it, and a rebuild changes the composition around it.
    // No index in the new list is (rank, cid) at-or-after the stale cursor,
    // so the scan finds neither an exact match nor a fallback and returns
    // `items.len()`: a known, documented truncation, not a hole in the feed
    // (`decisions.md` carries the proof). A no-cursor refresh against the
    // same generation still serves everything.
    #[test]
    fn page_start_truncates_when_the_next_generation_sorts_entirely_before_the_stale_cursor() {
        let stale_cursor = (50.0, "cid-old".to_string());
        let next_generation = vec![
            item("at://q/a", "cid-a", 100.0, 1.0),
            item("at://q/b", "cid-b", 90.0, 1.0),
            item("at://q/c", "cid-c", 60.0, 1.0),
        ];

        let global = ids(&next_generation);
        assert_eq!(
            page_start(&next_generation, Some(stale_cursor), &global),
            next_generation.len(),
            "truncates to an empty page rather than repeating or looping"
        );
        assert_eq!(
            page_start(&next_generation, None, &global),
            0,
            "a fresh no-cursor request against the same generation serves everything"
        );
    }

    fn hundred_k_items() -> Vec<FeedItem> {
        let mut items = Vec::with_capacity(100_000);
        for i in 0..100_000i64 {
            items.push(item(
                &format!("at://q/{i}"),
                &format!("cid{i:08}"),
                100_000.0 - i as f64,
                1.0,
            ));
        }
        items
    }

    // AC5 (7.9a): path 1's O(1) lookup against a 100k-item snapshot stays
    // well under budget. `state_with_items` swaps once, so the snapshot is
    // generation 1; a cursor naming that same generation and the item's own
    // index resolves through path 1, no scan at all.
    #[tokio::test]
    async fn budget_one_scan_path_1() {
        let items = hundred_k_items();
        let state = state_with_items(items);
        let app = router(state);
        let uri = format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=50&cursor={}",
            cursor::encode(1, 50_000, 50_000.0, "cid00050000")
        );

        let start = std::time::Instant::now();
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "path 1's O(1) resume over 100k items must stay well under budget, took {elapsed:?}"
        );
    }

    // AC5 (7.9b): path 2's one-scan-by-cid against a 100k-item snapshot
    // stays well under budget. The cursor names generation 999, which
    // neither the current (1) nor the previous (0) slot holds, so path 1
    // falls through and path 2's scan for the cid runs instead.
    #[tokio::test]
    async fn budget_one_scan_path_2() {
        let items = hundred_k_items();
        let state = state_with_items(items);
        let app = router(state);
        let uri = format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=50&cursor={}",
            cursor::encode(999, 0, 50_000.0, "cid00050000")
        );

        let start = std::time::Instant::now();
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "path 2's one cid scan over 100k items must stay well under budget, took {elapsed:?}"
        );
    }

    // 7.7(a): paging `deferral_snapshot` one item at a time, entirely
    // through path 1 (no further swap happens after `state_with_items`'s
    // one swap, so every returned cursor's generation is still `current`),
    // serves every item exactly once, in array order, and terminates.
    #[tokio::test]
    async fn pages_a_deferral_snapshot_exactly_once_via_path_1() {
        let items = deferral_snapshot();
        let state = state_with_items(items.clone());
        let app = router(state);
        let mut served = Vec::new();
        let mut cursor: Option<String> = None;

        for _ in 0..=items.len() {
            let uri = match &cursor {
                Some(c) => {
                    format!(
                        "/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=1&cursor={c}"
                    )
                }
                None => format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=1"),
            };
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: Value = serde_json::from_slice(&body).unwrap();
            let feed = json["feed"].as_array().unwrap();
            if feed.is_empty() {
                break;
            }
            served.push(feed[0]["post"].as_str().unwrap().to_string());
            cursor = json.get("cursor").and_then(|c| c.as_str()).map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }

        let expected: Vec<String> = items.iter().map(|i| i.quote_uri.clone()).collect();
        assert_eq!(served, expected, "every item served exactly once, in array order, via path 1");
    }

    // 7.7(b): a cursor from generation N's page 1 still resolves through
    // path 1, exactly, after ONE swap displaces generation N into
    // `previous` — regardless of how differently the new current
    // generation is composed. Proves paging is stable across a single
    // scorer pass.
    #[tokio::test]
    async fn stable_page_across_one_swap_via_path_1() {
        let items = deferral_snapshot();
        let state = state_with_items(items.clone()); // generation 1
        let app = router(state.clone());

        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=1");
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"][0]["post"], items[0].quote_uri);
        let cursor = json["cursor"].as_str().expect("more items remain").to_string();

        // The next pass rebuilds the list from scratch, deferring the
        // items differently, and swaps generation 1 into `previous`.
        let regenerated =
            vec![item("at://q/x", "cid-x", 500.0, 1.0), item("at://q/y", "cid-y", 400.0, 1.0)];
        swap_items(&state, regenerated);

        let uri2 = format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=100&cursor={cursor}"
        );
        let response2 =
            app.oneshot(Request::builder().uri(uri2).body(Body::empty()).unwrap()).await.unwrap();
        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: Value = serde_json::from_slice(&body2).unwrap();
        let served: Vec<String> = json2["feed"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["post"].as_str().unwrap().to_string())
            .collect();

        // Path 1 resumes exactly inside generation 1's own list, items
        // 1..8, untouched by whatever the new current generation holds.
        let expected: Vec<String> = items.iter().skip(1).map(|i| i.quote_uri.clone()).collect();
        assert_eq!(served, expected, "no repeat and no hole across the swap");
    }

    // 7.8(c): the cursor's generation has fallen out of both `current` and
    // `previous` (three swaps since it was issued), but its `quote_cid`
    // still appears in the current list, so path 2 starts after it.
    #[tokio::test]
    async fn path_2_scans_current_list_by_cid_once_the_generation_falls_out() {
        let state = state_with_items(vec![item("at://q/old", "cid-old", 10.0, 1.0)]); // gen 1
        swap_items(&state, vec![item("at://q/mid", "cid-mid", 5.0, 1.0)]); // gen 2
        swap_items(
            &state,
            vec![item("at://q/old", "cid-old", 1.0, 1.0), item("at://q/new", "cid-new", 50.0, 1.0)],
        ); // gen 3: generation 1 held by neither current (3) nor previous (2)

        let app = router(state);
        let cursor = cursor::encode(1, 0, 10.0, "cid-old");
        let uri =
            format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=10&cursor={cursor}");
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let served: Vec<&str> =
            json["feed"].as_array().unwrap().iter().map(|f| f["post"].as_str().unwrap()).collect();
        assert_eq!(
            served,
            vec!["at://q/new"],
            "path 2 starts just after cid-old in the current list"
        );
    }

    // 7.8(d): the cursor's generation has fallen out, and its `quote_cid`
    // is gone from the current list too, so path 3's rank/cid scan runs.
    #[tokio::test]
    async fn path_3_rank_scan_runs_once_generation_and_cid_both_fall_out() {
        let state = state_with_items(vec![item("at://q/old", "cid-old", 10.0, 1.0)]); // gen 1
        swap_items(&state, vec![item("at://q/mid", "cid-mid", 5.0, 1.0)]); // gen 2
        swap_items(
            &state,
            vec![
                item("at://q/high", "cid-high", 20.0, 1.0),
                item("at://q/low", "cid-low", 1.0, 1.0),
            ],
        ); // gen 3: cid-old is gone entirely

        let app = router(state);
        let cursor = cursor::encode(1, 0, 10.0, "cid-old");
        let uri =
            format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=10&cursor={cursor}");
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let served: Vec<&str> =
            json["feed"].as_array().unwrap().iter().map(|f| f["post"].as_str().unwrap()).collect();
        // page_start over [cid-high(20.0), cid-low(1.0)] against the stale
        // (10.0, "cid-old"): cid-high sorts before the cursor (higher
        // rank), cid-low sorts strictly after it, so the page starts at
        // cid-low.
        assert_eq!(
            served,
            vec!["at://q/low"],
            "path 3's rank/cid scan finds the first item after it"
        );
    }

    // BC59: the cursor names the held current generation, but its `index`
    // is at or beyond that generation's own list length (the generation
    // was rebuilt shorter since the cursor was issued). Path 1 must not
    // panic on the out-of-bounds index; it falls through to path 2, which
    // still finds `cid-old` in the current list by scanning for it.
    #[test]
    fn path_1_index_out_of_bounds_falls_through_without_panicking() {
        let current = Snapshot {
            generation: 1,
            items: Arc::new(vec![item("at://q/old", "cid-old", 10.0, 1.0)]),
            global: Arc::new(vec![0]),
        };
        let previous = None;
        // Index 5 does not exist in a one-item list.
        let cursor_value = Some((1, 5, 99.0, "cid-old".to_string()));

        let (snapshot, start) = resolve_start(&current, &previous, cursor_value);

        assert_eq!(snapshot.generation, 1, "path 2 still serves the current generation");
        assert_eq!(start, 1, "path 2 found cid-old at index 0 and starts just after it");
    }

    // AC4, BC8: served bytes match the `01` capped-build oracle, byte for
    // byte, for the same input rows — a full page and a page reached via
    // cursor alike. The oracle serves its capped `Vec<FeedItem>` as a plain
    // snapshot (its own `global` is every index in order, since it is
    // already capped); the new path serves `build`'s uncapped `items` plus
    // `global`. `global_matches_v1` (`scorer::snapshot`) already proves the
    // two agree on content and order; this test proves the served
    // `getFeedSkeleton` bytes agree too.
    #[tokio::test]
    async fn global_output_unchanged() {
        use crate::score::Weights;
        use crate::scorer::snapshot::{build, build_v1_capped_oracle};
        use crate::store::feed::FeedRow;

        fn feed_row(
            quote_uri: &str,
            quote_did: &str,
            original_did: &str,
            quoted_at: i64,
        ) -> FeedRow {
            FeedRow {
                quote_uri: quote_uri.to_string(),
                quote_cid: format!("cid-{quote_uri}"),
                quote_did: quote_did.to_string(),
                original_did: original_did.to_string(),
                quoted_at,
                v_likes_q: 60,
                v_reposts_q: 0,
                v_replies_q: 0,
                v_likes_o: 0,
                v_reposts_o: 0,
                v_replies_o: 0,
                ratio: 12.0,
                rank: 0.0,
                promoted_at: quoted_at,
                verified_at: quoted_at,
            }
        }

        async fn fetch(state: Arc<AppState>, uri: String) -> (axum::http::HeaderMap, Vec<u8>) {
            let app = router(state);
            let response = app
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let headers = response.headers().clone();
            let body =
                axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap().to_vec();
            (headers, body)
        }

        let weights = Weights { repost: 3.0, reply: 5.0 };
        let now = 1_700_100_000i64;
        let k = 5.0;
        let rows: Vec<FeedRow> = (0..5)
            .map(|i| {
                feed_row(
                    &format!("at://did:plc:q{i}/app.bsky.feed.post/q"),
                    &format!("did:plc:quoter{i}"),
                    &format!("did:plc:orig{i}"),
                    now - 3600 - i,
                )
            })
            .collect();

        let oracle = build_v1_capped_oracle(rows.clone(), &weights, now, k);
        let (items, global) = build(rows, &weights, now, k);

        let oracle_state = state_with_items(oracle);
        let new_state = {
            let cfg = test_config("127.0.0.1:0");
            let state = test_state(cfg);
            state.snapshot.swap(Arc::new(items), Arc::new(global));
            state
        };

        // Full page.
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");
        let (headers_a, body_a) = fetch(oracle_state.clone(), uri.clone()).await;
        let (headers_b, body_b) = fetch(new_state.clone(), uri).await;
        assert_eq!(headers_a, headers_b, "headers must match byte for byte");
        assert_eq!(body_a, body_b, "a full page must match the 01 output byte for byte");

        // Cursor page: page 1 at limit=2, then follow the cursor for page 2.
        let uri1 = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=2");
        let (_, body1_a) = fetch(oracle_state.clone(), uri1.clone()).await;
        let (_, body1_b) = fetch(new_state.clone(), uri1).await;
        assert_eq!(body1_a, body1_b, "page 1 must match the 01 output byte for byte");
        let json1: Value = serde_json::from_slice(&body1_a).unwrap();
        let cursor = json1["cursor"].as_str().expect("more items remain").to_string();

        let uri2 =
            format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=2&cursor={cursor}");
        let (headers2_a, body2_a) = fetch(oracle_state, uri2.clone()).await;
        let (headers2_b, body2_b) = fetch(new_state, uri2).await;
        assert_eq!(headers2_a, headers2_b, "headers must match byte for byte");
        assert_eq!(body2_a, body2_b, "a cursor page must match the 01 output byte for byte");
    }
}
