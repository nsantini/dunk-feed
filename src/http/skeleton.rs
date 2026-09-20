//! `GET /xrpc/app.bsky.feed.getFeedSkeleton` (BC3 to BC11, BC17 to BC22,
//! BC32, BC33): the paginated feed read, served from the in-memory
//! snapshot with one linear scan to the cursor (TECH-DESIGN section 11.1's
//! budget line), never a store read.

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
use crate::scorer::snapshot::FeedItem;

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
pub struct SkeletonResponse {
    pub feed: Vec<SkeletonItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SkeletonItem {
    pub post: String,
    #[serde(rename = "feedContext")]
    pub feed_context: String,
}

/// `at://<publisher_did>/app.bsky.feed.generator/<rkey>`, the one URI BC3
/// accepts, shared with `describe::handler`'s own feed URI.
fn expected_feed_uri(state: &AppState) -> String {
    format!("at://{}/app.bsky.feed.generator/{}", state.cfg.publisher_did, state.cfg.feed_rkey)
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

/// BC6, BC22: `None` or an empty string is treated as absent (page starts
/// at index 0); anything else must decode (BC6) or the request is
/// `InvalidRequest`.
fn resolve_cursor(raw: Option<&str>) -> Result<Option<(f64, String)>, SkeletonError> {
    match raw {
        None | Some("") => Ok(None),
        Some(s) => Ok(Some(cursor::decode(s)?)),
    }
}

/// The index the page starts at, found in one linear scan of `items`
/// (TECH-DESIGN section 11.1's budget line; `snapshot::apply_cap_one_per_quoter_per_50`
/// leaves `items` not totally ordered by `(rank DESC, cid ASC)`, so a binary
/// search has no defined answer over it). An item whose `(rank, cid)`
/// exactly matches `cursor_value` starts the page at that item's index plus
/// one (BC32); otherwise the page starts at the first item, in list order,
/// that sorts strictly after `cursor_value` (BC7). `cursor_value: None`
/// (BC22) always starts at 0 without scanning.
fn page_start(items: &[FeedItem], cursor_value: Option<(f64, String)>) -> usize {
    let Some((rank, cid)) = cursor_value else { return 0 };
    let mut after: Option<usize> = None;
    for (i, item) in items.iter().enumerate() {
        match cursor::cmp_by_rank_then_cid((item.rank, &item.quote_cid), (rank, &cid)) {
            std::cmp::Ordering::Equal => return i + 1,
            std::cmp::Ordering::Greater => {
                if after.is_none() {
                    after = Some(i);
                }
            }
            std::cmp::Ordering::Less => {}
        }
    }
    after.unwrap_or(items.len())
}

/// The route's pure core: given `state`'s snapshot (read exactly once, BC8,
/// AC5) and the raw query params, builds the response body or a
/// `SkeletonError`. Split out from `handler` so a test can drive it without
/// a live router.
fn build(
    state: &AppState,
    params: &HashMap<String, String>,
) -> Result<SkeletonResponse, SkeletonError> {
    // BC3, BC21: `feed` missing, or a value other than the configured feed
    // URI (duplicates already resolved to the last occurrence by `Query`'s
    // `HashMap` deserialization).
    let expected = expected_feed_uri(state);
    match params.get("feed") {
        Some(feed) if *feed == expected => {}
        _ => return Err(SkeletonError::UnknownFeed),
    }

    let limit = resolve_limit(params.get("limit").map(String::as_str))?;
    let cursor_value = resolve_cursor(params.get("cursor").map(String::as_str))?;

    // BC8, AC5: the snapshot `Arc` is read exactly once per request.
    let items = state.snapshot.current();
    let start = page_start(&items, cursor_value);
    let page: Vec<&FeedItem> = items.iter().skip(start).take(limit).collect();

    // BC8: omitted once the page reaches the end of the snapshot.
    let cursor = if start + page.len() < items.len() {
        page.last().map(|item| cursor::encode(item.rank, &item.quote_cid))
    } else {
        None
    };

    let feed = page
        .into_iter()
        .map(|item| SkeletonItem {
            post: item.quote_uri.clone(),
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
///
/// No non-test caller yet: registered on `router` (`src/http/mod.rs`),
/// itself uncalled until `run` (`src/ingest/mod.rs`) starts the HTTP task.
pub async fn handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let mut response = match build(&state, &params) {
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

    const FEED_URI: &str = "at://did:plc:abc/app.bsky.feed.generator/dunks";

    fn item(quote_uri: &str, quote_cid: &str, rank: f64, ratio: f64) -> FeedItem {
        FeedItem { quote_uri: quote_uri.to_string(), quote_cid: quote_cid.to_string(), rank, ratio }
    }

    fn state_with_items(items: Vec<FeedItem>) -> Arc<AppState> {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        state.snapshot.swap(Arc::new(items));
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

    // BC7, BC32: page_start behaviour, unit-tested directly.
    #[test]
    fn page_start_exact_match_starts_after_it() {
        let items = vec![
            item("at://q/1", "cid1", 3.0, 1.0),
            item("at://q/2", "cid2", 2.0, 1.0),
            item("at://q/3", "cid3", 1.0, 1.0),
        ];
        assert_eq!(page_start(&items, Some((2.0, "cid2".to_string()))), 2);
    }

    #[test]
    fn page_start_stale_cursor_starts_strictly_after() {
        // A cursor whose (rank, cid) no longer matches any item still finds
        // the first item that sorts strictly after it.
        let items = vec![item("at://q/1", "cid1", 3.0, 1.0), item("at://q/3", "cid3", 1.0, 1.0)];
        // A stale cursor at rank 2.0 sorts between the two remaining items.
        assert_eq!(page_start(&items, Some((2.0, "cidX".to_string()))), 1);
    }

    #[test]
    fn page_start_cursor_past_the_end_returns_len() {
        let items = vec![item("at://q/1", "cid1", 3.0, 1.0)];
        assert_eq!(page_start(&items, Some((0.0, "zzz".to_string()))), 1);
    }

    // AC5: a request against a 100k-item snapshot completes in one scan,
    // well under a generous budget.
    #[tokio::test]
    async fn budget_one_scan() {
        let mut items = Vec::with_capacity(100_000);
        for i in 0..100_000i64 {
            items.push(item(
                &format!("at://q/{i}"),
                &format!("cid{i:08}"),
                100_000.0 - i as f64,
                1.0,
            ));
        }
        let state = state_with_items(items);
        let app = router(state);
        let uri = format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&limit=50&cursor={}",
            cursor::encode(50_000.0, "cid00050000")
        );

        let start = std::time::Instant::now();
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "a single scan over 100k items must stay well under budget, took {elapsed:?}"
        );
    }
}
