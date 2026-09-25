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
use axum::http::header::{AUTHORIZATION, CACHE_CONTROL};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Json, Response};
use serde::Serialize;
use thiserror::Error;

use crate::auth::ViewerDid;
use crate::graph::CircleState;
use crate::http::cursor::{self, CursorError};
use crate::http::viewer::ViewerList;
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

/// The personalised path's counterpart to `resolve_cursor` (BC17a, BC17b):
/// `None` or an empty string is absent (the page starts at index 0 of the
/// viewer's list); anything else must decode through `cursor::decode_personal`
/// (accepting either the four- or the five-field shape) or the request is
/// `InvalidRequest`.
fn resolve_personal_cursor(
    raw: Option<&str>,
) -> Result<Option<cursor::PersonalCursor>, SkeletonError> {
    match raw {
        None | Some("") => Ok(None),
        Some(s) => Ok(Some(cursor::decode_personal(s)?)),
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

/// The personalised path's counterpart to `resolve_start` (BC14 to BC17):
/// given the viewer's current list (`http::viewer::ViewerLists::list_for`,
/// always the requester's own — foreign items never enter `list.indices`,
/// so a cursor naming another viewer's item can never resolve to one,
/// BC15) and the decoded cursor, the index `list.indices` resumes at.
///
/// Path 1 (BC14): the cursor carries a `circle_version` (the five-field
/// shape, `cursor::encode_personal`'s own output) that matches `list`'s
/// own `(generation, circle_version)` exactly, and `index` names a
/// position in `list.indices` whose item has the same `quote_cid` — an
/// exact O(1) resume at `index + 1`, the same shape as the global path's
/// own path 1 (`resolve_start`). A `circle_version` of `None` (BC17a: a
/// plain four-field cursor accepted on this path) or a mismatch on
/// either field skips straight to path 2, since there is no "held list"
/// to trust an index into.
///
/// Path 2 (BC17): one scan of `list.indices` for the cursor's `quote_cid`;
/// found, the page starts just after it.
///
/// Path 3 (BC17): neither applied. `page_start`'s rank/cid scan over
/// `list.indices` — the same helper the global path's own path 3 uses,
/// since both scans are the identical `(rank DESC, cid ASC)` walk over an
/// index list into `items`, just a different index list.
fn resolve_personal_start(
    list: &ViewerList,
    items: &[FeedItem],
    cursor_value: Option<cursor::PersonalCursor>,
) -> usize {
    let Some((cursor_generation, cursor_circle_version, cursor_index, cursor_rank, cursor_cid)) =
        cursor_value
    else {
        return 0;
    };

    if let Some(circle_version) = cursor_circle_version {
        if list.generation == cursor_generation && list.circle_version == circle_version {
            if let Some(&idx) = list.indices.get(cursor_index) {
                if items[idx as usize].quote_cid == cursor_cid {
                    return cursor_index + 1;
                }
            }
        }
    }

    if let Some(pos) =
        list.indices.iter().position(|&idx| items[idx as usize].quote_cid == cursor_cid)
    {
        return pos + 1;
    }

    page_start(items, Some((cursor_rank, cursor_cid)), &list.indices)
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

/// BC2, BC13: the page every unauthenticated or rejected request gets, and
/// what `build_personal` returns for every case that carries no items —
/// no graph (`state.graph.is_none()`), no circle yet (BC4), or a circle
/// still `building_d1` (BC5, BC6a). Distinct from `empty_page` (which
/// builds the whole `Response`): this is the response *body*, since
/// `build_personal`'s caller still needs to wrap it in `Json` alongside
/// the feed's own `Cache-Control` header logic `handler` already owns.
fn empty_response<'a>() -> SkeletonResponse<'a> {
    SkeletonResponse { feed: Vec::new(), cursor: None }
}

/// The personalised route's pure core (BC4 to BC17, BC23's in-memory
/// touch), `build`'s counterpart once a viewer is verified. `current` is
/// the current generation `SnapshotHandle::generations()` returned,
/// already read once by the caller and kept alive for as long as the
/// response's borrowed items need it (round 2 finding 7, same reasoning
/// `build` itself follows). Unlike `build`, this never reads `previous`:
/// `http::viewer::ViewerLists::list_for` builds and caches every viewer's
/// list off the current generation alone (`## Approach`'s "Rejected:
/// filtering `global`" note has no bearing here — a viewer's own list is
/// never the already-capped `global`).
fn build_personal<'a>(
    state: &AppState,
    viewer: &ViewerDid,
    now: i64,
    current: &'a Snapshot,
    params: &HashMap<String, String>,
) -> Result<SkeletonResponse<'a>, SkeletonError> {
    let expected = expected_feed_uri(state);
    match params.get("feed") {
        Some(feed) if feed == expected => {}
        _ => return Err(SkeletonError::UnknownFeed),
    }

    let limit = resolve_limit(params.get("limit").map(String::as_str))?;
    let cursor_value = resolve_personal_cursor(params.get("cursor").map(String::as_str))?;

    // BC4, BC5, BC6a: no graph subsystem, no circle yet, or still building
    // all serve the empty page. `enqueue_first_build` is itself a no-op
    // for a viewer that already has a circle (building or ready) or once
    // `UPSTAGE_MAX_VIEWERS` is reached (BC6a), so calling it here never
    // risks a second job.
    let Some(graph) = &state.graph else {
        return Ok(empty_response());
    };
    let circle = match graph.get(viewer) {
        Some(circle) => circle,
        None => {
            graph.enqueue_first_build(viewer.clone(), now);
            return Ok(empty_response());
        }
    };

    // BC23 (in-memory part): every personalised request that reaches a
    // circle — building or ready — touches it; `graph::run_touch_flush`
    // (already running, story 06 slice 2.0) is what writes this through to
    // SQLite, at most once a minute, off this same in-memory record.
    graph.record_touch(viewer, now);

    if circle.state == CircleState::BuildingD1 {
        // BC5, BC8a: no step 1 circle yet, so there is nothing to filter
        // against.
        return Ok(empty_response());
    }
    // BC8: a `BuildingFm` circle is step 1's `follows` with an
    // empty `follows_me` and step 1's `circle_version` — `list_for` filters
    // and caps it exactly as it would a `Ready` circle, so this branch
    // needs no special case beyond the `BuildingD1` one above.

    let list = state.viewer_lists.list_for(viewer, &circle, current, &graph.follows_cache());
    let items = current.items.as_slice();

    let start = resolve_personal_start(&list, items, cursor_value);
    // BC11: a short list is served as-is; `.get(start..)` never reads past
    // `list.indices`'s own end, so a start beyond it just yields no items.
    let tail: &[u32] = list.indices.get(start..).unwrap_or(&[]);
    let page: Vec<&FeedItem> = tail.iter().take(limit).map(|&idx| &items[idx as usize]).collect();

    // BC11: omitted once the page reaches the end of the viewer's own
    // list; otherwise pins `list`'s `(generation, circle_version)` so the
    // next page resumes in this same list (BC14).
    let cursor = if start + page.len() < list.indices.len() {
        let last_index = start + page.len() - 1;
        page.last().map(|item| {
            cursor::encode_personal(
                list.generation,
                list.circle_version,
                last_index,
                item.rank,
                &item.quote_cid,
            )
        })
    } else {
        None
    };

    let feed = page
        .into_iter()
        .map(|item| SkeletonItem {
            post: item.quote_uri.as_str(),
            feed_context: format!("r={:.1}", item.ratio),
        })
        .collect();

    Ok(SkeletonResponse { feed, cursor })
}

/// Reads a bearer token out of `Authorization` (network-feed story 05,
/// BC2): `None` unless the header is present, its scheme matches `Bearer`
/// case-insensitively (a client that sends `bearer` per HTTP's
/// case-insensitive scheme convention must not be treated as sending no
/// token at all), and the token half, trimmed of surrounding whitespace,
/// is non-empty. The scheme and the token are split on the first ASCII
/// whitespace character, not only a space (BC2), so a tab or another
/// linear whitespace byte between them still counts.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(AUTHORIZATION)?.to_str().ok()?.trim();
    let split_at = raw.find(|c: char| c.is_ascii_whitespace())?;
    let (scheme, token) = raw.split_at(split_at);
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

/// BC2, BC13: the page every unauthenticated or rejected request gets when
/// the switch is `true` — 200, `{"feed":[]}`, no `cursor`. The lifetime
/// parameter is unconstrained (`feed` is always empty), so `'static`
/// stands in for it.
fn empty_page() -> Response {
    let body: SkeletonResponse<'static> = SkeletonResponse { feed: Vec::new(), cursor: None };
    Json(body).into_response()
}

/// BC2, BC9, BC12, BC26: runs `auth::verify` when the switch is `true`.
/// `Ok(viewer)` once a viewer is verified — network-feed story 06's
/// personalised branch (`build_personal`) is what actually reads `viewer`;
/// story 05's own `## Non-goals` (the global `build` never filters on it)
/// still holds for the switch-off path. `Err(())` for every rejection — no
/// `Authorization` header, the wrong scheme, an empty token (BC2), or any
/// `AuthError` `verify` returns (BC26, logged at `debug` with only the
/// variant name, never the DID or the token, per `auth`'s own BC21).
fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<ViewerDid, ()> {
    let auth = state.cfg.auth.as_ref().ok_or(())?;
    let token = bearer_token(headers).ok_or(())?;
    let now = crate::store::unix_now();
    match crate::auth::verify(token, now, &auth.cache, &auth.cfg, &auth.resolver_tx) {
        Ok(viewer) => Ok(viewer),
        Err(err) => {
            tracing::debug!(error = ?err, "auth: request rejected");
            Err(())
        }
    }
}

/// BC1: switch off, no `Authorization` read, output byte-identical to
/// story 01 (AC10). BC2, BC12, BC13, BC13a, BC26: switch on, `authenticate`
/// runs first, so an unauthenticated or rejected request never reaches
/// request validation and always gets the empty page; a verified request
/// runs `build_personal` (network-feed story 06, slice 4.0) instead of the
/// global `build` — BC4 to BC17, BC20 hold there, and a bad `feed`, `limit`
/// or `cursor` still gets the same 400 body shape (`SkeletonError`'s shared
/// `IntoResponse`). Either way, `Cache-Control` is set once at the end, on
/// every response: `public, max-age=30` off, `private, no-store` on (BC11,
/// BC13, BC13a, BC33).
pub async fn handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let personalise = state.cfg.personalise;

    let mut response = if personalise {
        match authenticate(&state, &headers) {
            Err(()) => empty_page(),
            Ok(viewer) => {
                let now = crate::store::unix_now();
                // BC50, AC5: the current generation is read exactly once
                // per request, kept alive here for as long as
                // `build_personal`'s borrowed response needs it.
                let (current, _previous) = state.snapshot.generations();
                match build_personal(&state, &viewer, now, &current, &params) {
                    Ok(body) => Json(body).into_response(),
                    Err(err) => err.into_response(),
                }
            }
        }
    } else {
        // BC50, AC5: both generations are read exactly once per request,
        // under a single read lock, kept alive here for as long as
        // `build`'s borrowed response needs them.
        let (current, previous) = state.snapshot.generations();
        match build(&state, &current, &previous, &params) {
            Ok(body) => Json(body).into_response(),
            Err(err) => err.into_response(),
        }
    };

    let cache_control = if personalise { "private, no-store" } else { "public, max-age=30" };
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static(cache_control));
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

    /// Like [`item`], but with caller-chosen author hashes and `quoted_at`,
    /// for the personalised-path tests (network-feed story 06, slice 4.0)
    /// that need a fixture item connected — or not — to a specific
    /// viewer's `circle.follows`.
    fn item_with_authors(
        quote_uri: &str,
        quote_cid: &str,
        quote_did: u64,
        original_did: u64,
        quoted_at: i64,
        rank: f64,
    ) -> FeedItem {
        FeedItem {
            quote_uri: quote_uri.to_string(),
            quote_cid: quote_cid.to_string(),
            rank,
            ratio: 1.0,
            quote_did,
            original_did,
            quoted_at,
            promoted_at: quoted_at,
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

    /// Config with `UPSTAGE_PERSONALISE=true`, network-feed story 05's
    /// slice 3.0 tests. `service_did` still falls back to its
    /// `did:web:feed.example.com` default (BC23); tests read it off the
    /// returned `Config` rather than hard-coding it a second time.
    fn test_config_personalised(http_addr: &str) -> crate::config::Config {
        let mut pairs: HashMap<String, String> = HashMap::new();
        pairs.insert("UPSTAGE_HOSTNAME".to_string(), "feed.example.com".to_string());
        pairs.insert("UPSTAGE_PUBLISHER_DID".to_string(), "did:plc:abc".to_string());
        pairs.insert("UPSTAGE_HTTP_ADDR".to_string(), http_addr.to_string());
        pairs.insert("UPSTAGE_PERSONALISE".to_string(), "true".to_string());
        crate::config::load(move |name| pairs.get(name).cloned())
            .expect("test config must be valid")
    }

    // BC1, AC9: switch off — `Authorization` is never read (a garbage
    // value is still accepted), and the header is always the story 01
    // `public, max-age=30`, never `private, no-store`.
    #[tokio::test]
    async fn switch_off_unchanged() {
        let items = vec![item("at://q/1", "cid1", 3.0, 4.5)];
        let state = state_with_items(items);
        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", "Bearer not-even-read")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "public, max-age=30");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"][0]["post"], "at://q/1");
    }

    #[test]
    fn bearer_token_case_insensitive_scheme() {
        // Review round 1, defect G.
        for scheme in ["Bearer", "bearer", "BEARER", "BeArEr"] {
            let mut headers = HeaderMap::new();
            headers
                .insert(AUTHORIZATION, HeaderValue::from_str(&format!("{scheme} abc123")).unwrap());
            assert_eq!(bearer_token(&headers), Some("abc123"));
        }
    }

    #[test]
    fn bearer_token_trims_surrounding_whitespace() {
        // Review round 1, defect G.
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str("  Bearer   abc123  ").unwrap());
        assert_eq!(bearer_token(&headers), Some("abc123"));
    }

    #[test]
    fn bearer_token_empty_after_trim_is_none() {
        // Review round 1, defect G: an empty token after trimming is no
        // token, not an empty-string token.
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str("Bearer    ").unwrap());
        assert_eq!(bearer_token(&headers), None);
    }

    #[test]
    fn bearer_token_splits_on_a_tab() {
        // BC2: the scheme and the token split on any ASCII whitespace, not
        // only a space.
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str("Bearer\tabc123").unwrap());
        assert_eq!(bearer_token(&headers), Some("abc123"));
    }

    // AC7, BC9: a token whose DID the cache has never held returns the
    // empty page at once, and enqueues exactly one `Miss` — no network
    // call is possible on this path, since `auth::verify` only ever does a
    // `try_send`.
    #[tokio::test]
    async fn unknown_key_empty_page() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::new(crate::auth::KeyCache::new(10)),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let state = crate::http::tests::test_state_with_auth(cfg.clone(), auth);
        let app = router(state);

        let now = crate::store::unix_now();
        let did = "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz";
        // Seeded into a throwaway cache, never the one `state` reads: the
        // token is structurally valid and signs correctly, but its DID is
        // unknown to the cache the handler actually consults.
        let token = crate::auth::seed_and_sign_for_test(
            &crate::auth::KeyCache::new(1),
            did,
            &cfg.service_did,
            now,
        );

        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "private, no-store");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"].as_array().unwrap().len(), 0);
        assert!(json.get("cursor").is_none());
        assert_eq!(
            rx.try_recv(),
            Ok(crate::auth::ResolveRequest::Miss { did: did.to_string(), token: token.clone() }),
            "a cache miss must enqueue a Miss for the resolver"
        );
    }

    // AC8: with the switch true, every response sends `private, no-store`
    // — a verified success, serving the real feed, and a rejected request,
    // serving the empty page, alike.
    #[tokio::test]
    async fn personalised_headers() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let cache = std::sync::Arc::new(crate::auth::KeyCache::new(10));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::clone(&cache),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let now = crate::store::unix_now();
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let token = crate::auth::seed_and_sign_for_test(&cache, did, &cfg.service_did, now);
        let viewer = crate::auth::ViewerDid(did.to_string());

        // A ready circle following the fixture item's quoter (`item`'s own
        // `quote_did: 1`), so the verified request below is served the
        // real, filtered feed (BC9) rather than the empty page (BC5) a
        // viewer with no ready circle would get.
        let graph = crate::graph::GraphHandle::new(10);
        graph.enqueue_first_build(viewer.clone(), now);
        let mut circle = crate::graph::Circle::new();
        circle.follows = [1u64].into_iter().collect();
        graph.insert_ready(&viewer, circle);

        let state = crate::http::tests::test_state_with_graph(cfg.clone(), auth, graph);
        swap_items(&state, vec![item("at://q/1", "cid1", 3.0, 4.5)]);
        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");

        // Verified: success, private, no-store, the real feed (BC9).
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "private, no-store");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"][0]["post"], "at://q/1");

        // Rejected: no `Authorization` at all, still private, no-store,
        // empty (BC13).
        let response2 =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response2.status(), StatusCode::OK);
        assert_eq!(response2.headers().get(CACHE_CONTROL).unwrap(), "private, no-store");
        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(json2["feed"].as_array().unwrap().len(), 0);
    }

    // AC3, BC4: a verified viewer with no circle yet gets the empty page
    // well under the 300 ms bound, with exactly one circle created in
    // memory in `building_d1` and (implicitly, via `GraphHandle`'s own
    // de-duplicated queue, `graph::tests::enqueue_first_build_creates_a_
    // building_circle_and_one_job`) one `FirstBuild` job queued.
    #[tokio::test]
    async fn first_open_empty_fast() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let cache = std::sync::Arc::new(crate::auth::KeyCache::new(10));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::clone(&cache),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let now = crate::store::unix_now();
        let did = "did:plc:firstopenaaaaaaaaaaaaaaa";
        let token = crate::auth::seed_and_sign_for_test(&cache, did, &cfg.service_did, now);

        let graph = crate::graph::GraphHandle::new(10);
        let state = crate::http::tests::test_state_with_graph(cfg.clone(), auth, graph.clone());
        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");

        let start = std::time::Instant::now();
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(elapsed < std::time::Duration::from_millis(300), "took {elapsed:?}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"].as_array().unwrap().len(), 0);
        assert!(json.get("cursor").is_none());

        let viewer = crate::auth::ViewerDid(did.to_string());
        let circle = graph.get(&viewer).expect("a circle must be created in memory");
        assert_eq!(circle.state, crate::graph::CircleState::BuildingD1);
    }

    // AC7, BC15: a cursor naming another viewer's own item and index never
    // resolves to an item outside the requester's own circle-filtered
    // list, regardless of which resolution path it falls through to.
    #[tokio::test]
    async fn foreign_cursor() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let cache = std::sync::Arc::new(crate::auth::KeyCache::new(10));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::clone(&cache),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let now = crate::store::unix_now();
        let did_b = "did:plc:viewerbaaaaaaaaaaaaaaaaa";
        let token_b = crate::auth::seed_and_sign_for_test(&cache, did_b, &cfg.service_did, now);

        let quoter_a = crate::graph::hash_did("did:plc:quoter-a");
        let quoter_b = crate::graph::hash_did("did:plc:quoter-b");
        let store = crate::store::Store::open_memory().unwrap();
        let follows_a: std::collections::HashSet<u64> = [quoter_a].into_iter().collect();
        let follows_b: std::collections::HashSet<u64> = [quoter_b].into_iter().collect();
        store
            .viewer_save_circle(
                "did:plc:vieweraaaaaaaaaaaaaaaaaa",
                "ready",
                now,
                now,
                &[],
                &follows_a,
            )
            .unwrap();
        store.viewer_save_circle(did_b, "ready", now, now, &[], &follows_b).unwrap();
        let graph = crate::graph::GraphHandle::from_store(&store, 10).unwrap();

        let state = crate::http::tests::test_state_with_graph(cfg.clone(), auth, graph);
        let item_a = item_with_authors(
            "at://q/a",
            "cid-a",
            quoter_a,
            crate::graph::hash_did("did:plc:orig-a"),
            1_700_000_000,
            10.0,
        );
        let item_b = item_with_authors(
            "at://q/b",
            "cid-b",
            quoter_b,
            crate::graph::hash_did("did:plc:orig-b"),
            1_700_000_000,
            5.0,
        );
        swap_items(&state, vec![item_a, item_b]);
        let app = router(state);

        // A cursor that names viewer A's own item, at viewer A's own
        // (generation, circle_version) — but sent as viewer B.
        let cursor_a = cursor::encode_personal(1, 1, 0, 10.0, "cid-a");
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&cursor={cursor_a}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token_b}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let served: Vec<&str> =
            json["feed"].as_array().unwrap().iter().map(|f| f["post"].as_str().unwrap()).collect();
        assert!(
            !served.contains(&"at://q/a"),
            "an item outside the requester's own list must never be served"
        );
    }

    // AC8, BC16: the circle changes (the worker swaps in a fresh circle,
    // simulated here through `GraphHandle::insert_ready`, story 06's own
    // Non-goals having no live refresh trigger yet) between two page
    // requests at the same snapshot generation. The item already served on
    // page 1 must not repeat, and the item newly visible after the change
    // must not be skipped (no early end).
    #[tokio::test]
    async fn circle_change_mid_scroll() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let cache = std::sync::Arc::new(crate::auth::KeyCache::new(10));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::clone(&cache),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let now = crate::store::unix_now();
        let did = "did:plc:scrollvieweraaaaaaaaaaaa";
        let token = crate::auth::seed_and_sign_for_test(&cache, did, &cfg.service_did, now);
        let viewer = crate::auth::ViewerDid(did.to_string());

        let quoter1 = crate::graph::hash_did("did:plc:quoter-1");
        let quoter2 = crate::graph::hash_did("did:plc:quoter-2");
        // Distinct original authors: cap 1 (one item per (original_did,
        // day)) would otherwise drop item2 outright, which would test cap
        // 1, not BC16's cursor-resolution question this test is about.
        let original1 = crate::graph::hash_did("did:plc:original-1");
        let original2 = crate::graph::hash_did("did:plc:original-2");

        let graph = crate::graph::GraphHandle::new(10);
        graph.enqueue_first_build(viewer.clone(), now);
        let mut circle1 = crate::graph::Circle::new();
        circle1.follows = [quoter1].into_iter().collect();
        graph.insert_ready(&viewer, circle1);

        let state = crate::http::tests::test_state_with_graph(cfg.clone(), auth, graph.clone());
        let item1 = item_with_authors("at://q/1", "cid1", quoter1, original1, 1_700_000_000, 10.0);
        let item2 = item_with_authors("at://q/2", "cid2", quoter2, original2, 1_700_000_000, 5.0);
        swap_items(&state, vec![item1, item2]);
        let app = router(state);

        // A cursor as if the client had already been served `cid1`, at the
        // circle's version when only `quoter1` was followed.
        let cursor_page1 = cursor::encode_personal(1, 1, 0, 10.0, "cid1");

        // The circle changes mid-scroll: a fresh save now also follows
        // `quoter2`, bumping `circle_version` to 2 at the same snapshot
        // generation (BC16's premise).
        let mut circle2 = crate::graph::Circle::new();
        circle2.follows = [quoter1, quoter2].into_iter().collect();
        graph.insert_ready(&viewer, circle2);

        let uri =
            format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&cursor={cursor_page1}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let served: Vec<&str> =
            json["feed"].as_array().unwrap().iter().map(|f| f["post"].as_str().unwrap()).collect();

        assert!(!served.contains(&"at://q/1"), "must not repeat an already-served item");
        assert_eq!(served, vec!["at://q/2"], "must not skip the newly-visible item either");
    }

    // BC5: a viewer whose circle already exists, still `building_d1`, gets
    // the empty page and no second job — the handler's "no circle" branch
    // (the only one that ever calls `enqueue_first_build`) never runs once
    // `graph.get` already returns a circle.
    #[tokio::test]
    async fn building_circle_gets_empty_page_with_no_second_job() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let cache = std::sync::Arc::new(crate::auth::KeyCache::new(10));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::clone(&cache),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let now = crate::store::unix_now();
        let did = "did:plc:buildingvieweraaaaaaaaaa";
        let token = crate::auth::seed_and_sign_for_test(&cache, did, &cfg.service_did, now);
        let viewer = crate::auth::ViewerDid(did.to_string());

        let graph = crate::graph::GraphHandle::new(10);
        graph.enqueue_first_build(viewer.clone(), now);
        let queue = graph.queue();
        assert_eq!(
            queue.try_pop(),
            Some(crate::graph::Job::FirstBuild(viewer.clone())),
            "the one job from enqueue_first_build"
        );
        // Re-push it, as `run_worker` would leave it while a real attempt
        // is in flight (the outstanding mark, not the FIFO position, is
        // what de-duplicates).
        queue.retry(crate::graph::Job::FirstBuild(viewer.clone()));

        let state = crate::http::tests::test_state_with_graph(cfg.clone(), auth, graph.clone());
        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"].as_array().unwrap().len(), 0);

        assert_eq!(
            graph.get(&viewer).map(|c| c.state),
            Some(crate::graph::CircleState::BuildingD1),
            "the circle is untouched"
        );
        assert_eq!(
            queue.try_pop(),
            Some(crate::graph::Job::FirstBuild(viewer)),
            "still exactly the one job the retry re-queued, no second job added"
        );
        assert!(queue.try_pop().is_none(), "no second job");
    }

    // AC6, BC8: a circle in `building_fm` (step 1 done, step 2 not yet)
    // serves the same list a `Ready` circle with the same `follows` would —
    // filtered and capped against step 1's `follows` and `circle_version`,
    // with an empty `follows_me` never adding or removing anything.
    #[tokio::test]
    async fn building_fm_serves_step1_list() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let cache = std::sync::Arc::new(crate::auth::KeyCache::new(10));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::clone(&cache),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let now = crate::store::unix_now();
        let did = "did:plc:buildingfmvieweraaaaaaaa";
        let token = crate::auth::seed_and_sign_for_test(&cache, did, &cfg.service_did, now);
        let viewer = crate::auth::ViewerDid(did.to_string());

        let quoter = crate::graph::hash_did("did:plc:building-fm-quoter");
        let graph = crate::graph::GraphHandle::new(10);
        let mut circle = crate::graph::Circle::new();
        circle.follows = [quoter].into_iter().collect();
        graph.swap_circle(&viewer, circle, crate::graph::CircleState::BuildingFm);

        let state = crate::http::tests::test_state_with_graph(cfg.clone(), auth, graph);
        let connected_item = item_with_authors(
            "at://q/fm-connected",
            "cid-fm-connected",
            quoter,
            crate::graph::hash_did("did:plc:fm-original"),
            1_700_000_000,
            10.0,
        );
        let unconnected_item = item_with_authors(
            "at://q/fm-unconnected",
            "cid-fm-unconnected",
            crate::graph::hash_did("did:plc:other-quoter"),
            crate::graph::hash_did("did:plc:other-original"),
            1_700_000_000,
            20.0,
        );
        swap_items(&state, vec![unconnected_item, connected_item]);

        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"].as_array().unwrap().len(), 1);
        assert_eq!(json["feed"][0]["post"], "at://q/fm-connected");
    }

    // AC4; BC11b: a circle in `building_d2` (step 2 done, step 3 not yet
    // finished) is served like a `building_fm` circle — step 1 and step 2
    // data, plus whatever degree-2 entries the shared `FollowsCache`
    // already holds for `d2_sample` at the time of the request.
    #[tokio::test]
    async fn building_d2_serves_step1_and_step2_data_plus_cache_so_far() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let cache = std::sync::Arc::new(crate::auth::KeyCache::new(10));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::clone(&cache),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let now = crate::store::unix_now();
        let did = "did:plc:buildingd2vieweraaaaaaaa";
        let token = crate::auth::seed_and_sign_for_test(&cache, did, &cfg.service_did, now);
        let viewer = crate::auth::ViewerDid(did.to_string());

        let quoter = crate::graph::hash_did("did:plc:building-d2-quoter");
        let d2_author = crate::graph::hash_did("did:plc:building-d2-degree2-author");
        let graph = crate::graph::GraphHandle::new(10);
        let mut circle = crate::graph::Circle::new();
        circle.follows = [quoter].into_iter().collect();
        circle.d2_sample = vec!["did:plc:building-d2-sampled".to_string()];
        graph.swap_circle(&viewer, circle, crate::graph::CircleState::BuildingD2);
        // Step 3 is mid-run: this one account already has a fresh entry.
        graph.follows_cache().put("did:plc:building-d2-sampled", now, vec![d2_author]);

        let state = crate::http::tests::test_state_with_graph(cfg.clone(), auth, graph);
        let step1_item = item_with_authors(
            "at://q/d2-step1",
            "cid-d2-step1",
            quoter,
            crate::graph::hash_did("did:plc:d2-step1-original"),
            1_700_000_000,
            10.0,
        );
        let degree2_item = item_with_authors(
            "at://q/d2-degree2",
            "cid-d2-degree2",
            d2_author,
            crate::graph::hash_did("did:plc:d2-degree2-original"),
            1_700_000_000,
            20.0,
        );
        let unconnected_item = item_with_authors(
            "at://q/d2-unconnected",
            "cid-d2-unconnected",
            crate::graph::hash_did("did:plc:d2-other-quoter"),
            crate::graph::hash_did("did:plc:d2-other-original"),
            1_700_000_000,
            30.0,
        );
        swap_items(&state, vec![unconnected_item, degree2_item, step1_item]);

        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let posts: Vec<&str> = json["feed"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["post"].as_str().unwrap())
            .collect();
        assert_eq!(
            posts,
            vec!["at://q/d2-degree2", "at://q/d2-step1"],
            "the step 1 item and the degree-2 item served so far both appear, in rank order"
        );
    }

    // BC6a: at `UPSTAGE_MAX_VIEWERS`, a new viewer's personalised request
    // still gets the empty page (the cap itself, and its one-per-minute
    // warning, are `graph::GraphHandle::enqueue_first_build`'s own
    // contract, proven directly in `graph::tests`).
    #[tokio::test]
    async fn at_cap_new_viewer_gets_empty_page() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let cache = std::sync::Arc::new(crate::auth::KeyCache::new(10));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::clone(&cache),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let now = crate::store::unix_now();
        let did = "did:plc:capvieweraaaaaaaaaaaaaaa";
        let token = crate::auth::seed_and_sign_for_test(&cache, did, &cfg.service_did, now);

        // A handle at its cap of one, already holding a different viewer's
        // circle.
        let graph = crate::graph::GraphHandle::new(1);
        graph.enqueue_first_build(crate::auth::ViewerDid("did:plc:someoneelse".to_string()), now);

        let state = crate::http::tests::test_state_with_graph(cfg.clone(), auth, graph);
        let app = router(state);
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"].as_array().unwrap().len(), 0);
    }

    // BC11, BC14, BC17b: a viewer's list shorter than `limit` is served
    // as-is with no `cursor` (BC11); the cursor that page would have
    // carried, had one been requested at `limit=1`, resumes exactly via
    // path 1 (BC14); and a malformed cursor on this path is the same 400
    // `InvalidRequest` shape the global path already returns (BC17b).
    #[tokio::test]
    async fn short_list_resume_and_malformed_cursor() {
        let cfg = test_config_personalised("127.0.0.1:0");
        let cache = std::sync::Arc::new(crate::auth::KeyCache::new(10));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let auth = crate::http::AuthHandle {
            cache: std::sync::Arc::clone(&cache),
            resolver_tx: tx,
            cfg: crate::auth::AuthConfig { service_did: cfg.service_did.clone() },
        };
        let now = crate::store::unix_now();
        let did = "did:plc:shortlistvieweraaaaaaaaa";
        let token = crate::auth::seed_and_sign_for_test(&cache, did, &cfg.service_did, now);
        let viewer = crate::auth::ViewerDid(did.to_string());

        let quoter = crate::graph::hash_did("did:plc:only-quoter");
        let graph = crate::graph::GraphHandle::new(10);
        graph.enqueue_first_build(viewer.clone(), now);
        let mut circle = crate::graph::Circle::new();
        circle.follows = [quoter].into_iter().collect();
        graph.insert_ready(&viewer, circle);

        let state = crate::http::tests::test_state_with_graph(cfg.clone(), auth, graph);
        let connected_item = item_with_authors(
            "at://q/only",
            "cid-only",
            quoter,
            crate::graph::hash_did("did:plc:original-only"),
            1_700_000_000,
            10.0,
        );
        let mut others = vec![connected_item];
        for i in 0..5 {
            others.push(item_with_authors(
                &format!("at://q/other-{i}"),
                &format!("cid-other-{i}"),
                crate::graph::hash_did(&format!("did:plc:other-quoter-{i}")),
                crate::graph::hash_did(&format!("did:plc:other-original-{i}")),
                1_700_000_000,
                5.0 - i as f64,
            ));
        }
        swap_items(&state, others);
        let app = router(state);

        // BC11: the viewer's list holds one item; it is served in full,
        // with no `cursor`, even though 5 other, unconnected items exist
        // in the snapshot.
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["feed"].as_array().unwrap().len(), 1);
        assert_eq!(json["feed"][0]["post"], "at://q/only");
        assert!(json.get("cursor").is_none(), "a short list carries no cursor");

        // BC14: a five-field cursor pinned exactly at this list's own
        // (generation, circle_version) and index resolves through path 1.
        // Nothing follows the one connected item, so the next page is
        // empty, not an error and not a repeat.
        let resume_cursor = cursor::encode_personal(1, 1, 0, 10.0, "cid-only");
        let uri2 =
            format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&cursor={resume_cursor}");
        let response2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri2)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX).await.unwrap();
        let json2: Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(json2["feed"].as_array().unwrap().len(), 0, "nothing follows the one item");

        // BC17b: a malformed cursor is the same 400 `InvalidRequest` shape
        // the global path already returns.
        let uri3 =
            format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={FEED_URI}&cursor=not-valid!!!");
        let response3 = app
            .oneshot(
                Request::builder()
                    .uri(uri3)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response3.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response3.headers().get(CACHE_CONTROL).unwrap(),
            "private, no-store",
            "the personalised Cache-Control still applies to a 400"
        );
        let body3 = axum::body::to_bytes(response3.into_body(), usize::MAX).await.unwrap();
        let json3: Value = serde_json::from_slice(&body3).unwrap();
        assert_eq!(json3["error"], "InvalidRequest");
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
    // `getFeedSkeleton` bytes agree too, on a fixture where both caps
    // actually fire.
    #[tokio::test]
    async fn global_output_unchanged() {
        use crate::score::Weights;
        use crate::scorer::snapshot::{build, build_v1_capped_oracle};
        use crate::store::feed::FeedRow;

        // Like `feed_row`, but with `v_likes_q` set explicitly and every
        // other count at zero, so the row's *computed* rank
        // (`recompute_ranks`, which ignores the fixture's own `rank` field)
        // is driven only by `likes`, at a shared `quoted_at` so age never
        // confounds the order.
        fn feed_row_with_likes(
            quote_uri: &str,
            quote_did: &str,
            original_did: &str,
            quoted_at: i64,
            likes: i64,
        ) -> FeedRow {
            FeedRow {
                quote_uri: quote_uri.to_string(),
                quote_cid: format!("cid-{quote_uri}"),
                quote_did: quote_did.to_string(),
                original_did: original_did.to_string(),
                quoted_at,
                v_likes_q: likes,
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

        // Rank order (descending likes, all rows at the same age):
        // cap1-high, cap1-low, repeatq-first, filler1, filler2, repeatq-again.
        // `cap1-high`/`cap1-low` share `(original_did, day)` (BC4): cap 1
        // drops `cap1-low`. `repeatq-first`/`repeatq-again` share quoter
        // `repeatq` (BC5); only 2 filler rows (well inside the 49-item
        // window) separate them and nothing follows to free the window, so
        // cap 2 defers `repeatq-again` and then drops it when the main list
        // runs out — both caps fire.
        let rows = vec![
            feed_row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/cap1-high",
                "did:plc:cap1-quoter-high",
                "did:plc:cap1-author",
                now,
                500,
            ),
            feed_row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/cap1-low",
                "did:plc:cap1-quoter-low",
                "did:plc:cap1-author",
                now,
                480,
            ),
            feed_row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/repeatq-first",
                "did:plc:repeatq",
                "did:plc:orig-first",
                now,
                460,
            ),
            feed_row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/filler1",
                "did:plc:filler-quoter1",
                "did:plc:orig-filler1",
                now,
                440,
            ),
            feed_row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/filler2",
                "did:plc:filler-quoter2",
                "did:plc:orig-filler2",
                now,
                420,
            ),
            feed_row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/repeatq-again",
                "did:plc:repeatq",
                "did:plc:orig-again",
                now,
                400,
            ),
        ];

        let oracle = build_v1_capped_oracle(rows.clone(), &weights, now, k);
        let (items, global) = build(rows, &weights, now, k);

        // Both caps must actually have fired before the byte comparison
        // below means anything.
        assert_eq!(global.len(), 4, "cap 1 drops one row, cap 2 drops another");
        let survivors: Vec<&str> =
            global.iter().map(|&i| items[i as usize].quote_uri.as_str()).collect();
        assert!(
            !survivors.contains(&"at://did:plc:q/app.bsky.feed.post/cap1-low"),
            "cap 1 must drop the lower-rank same-author-same-day row"
        );
        assert!(
            !survivors.contains(&"at://did:plc:q/app.bsky.feed.post/repeatq-again"),
            "cap 2 must drop the deferred row once the main list runs out"
        );

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
