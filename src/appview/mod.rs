//! Typed client over the App View, TECH-DESIGN section 8. Hand-written
//! `serde` structs, not `atrium-api` (section 1 rejects it for its size).
//! One `reqwest::Client`, rate limited, retried on 429 and 5xx with
//! backoff, and batched at the sizes section 8.1 gives. `validate` (story
//! 03) and the scorer (story 07) call these exact four methods instead of
//! writing their own copy.

#![allow(dead_code)] // First caller is `dunk validate`, story 03.

pub mod types;

use std::collections::HashMap;
use std::time::Duration;

use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::{self, MissedTickBehavior};

use crate::config::Config;
use types::{
    GetFeedResponse, GetPostsResponse, GetProfilesResponse, GetQuotesResponse, PostView,
    ProfileView,
};

/// The HTTP timeout every request carries, TECH-DESIGN section 8.1.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// An App View call that did not produce a usable result: every attempt
/// failed, or the body did not decode. The caller never sees a partial map
/// as success (BC10); it sees this instead. Carries the method name, the
/// HTTP status when there was one, and the number of attempts made, so
/// story 03 and story 07 can log one line (BC14). No client method panics
/// or calls `unwrap`.
#[derive(Debug, Error, PartialEq)]
pub enum AppViewError {
    #[error("appview rate must be a finite number greater than zero")]
    InvalidRate,
    #[error("{method} failed after {attempts} attempt(s), status {status:?}")]
    Failed { method: &'static str, status: Option<u16>, attempts: u32 },
}

/// What to do after a non-success response, TECH-DESIGN section 8.1's
/// "three attempts, on a fourth failure the pass logs and moves on".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    RetryAfter(Duration),
    Fail,
}

/// Decides whether to retry a failed attempt, pure. `status` is the HTTP
/// status of the failed attempt, or `None` when the attempt never produced
/// one at all: a connection error, or the 10s timeout (BC25). `attempt` is
/// the zero-based index of that attempt. A 429, a 5xx, or `None` retries
/// after 1s, 2s and 4s on attempts 0, 1 and 2 (BC9, BC25); attempt 3, the
/// fourth failure, fails (BC10). Any other 4xx fails immediately, with no
/// backoff and no second request (BC11). `retry_decision` is called only on
/// a non-success outcome; a 2xx never reaches it.
pub fn retry_decision(status: Option<u16>, attempt: u32) -> RetryDecision {
    let retryable = match status {
        None => true,
        Some(status) => status == 429 || (500..600).contains(&status),
    };
    if !retryable {
        return RetryDecision::Fail;
    }
    match attempt {
        0 => RetryDecision::RetryAfter(Duration::from_secs(1)),
        1 => RetryDecision::RetryAfter(Duration::from_secs(2)),
        2 => RetryDecision::RetryAfter(Duration::from_secs(4)),
        _ => RetryDecision::Fail,
    }
}

/// A typed client over `app.bsky.feed.getPosts`, `app.bsky.actor.getProfiles`,
/// `app.bsky.feed.getQuotes` and `app.bsky.feed.getFeed`. Unauthenticated;
/// the authenticated calls (`createSession`, `uploadBlob`, `putRecord`) are
/// story 09. One request at a time is let through the rate limiter, so two
/// concurrent callers never burst past `appview_rps`.
#[derive(Debug)]
pub struct AppViewClient {
    base_url: String,
    http: reqwest::Client,
    limiter: Mutex<time::Interval>,
}

impl AppViewClient {
    /// `getPosts` and `getProfiles` batch at 25 URIs or DIDs per request,
    /// TECH-DESIGN section 8.1. An associated constant, not a `Config`
    /// value, because section 4 lists no variable for it.
    pub const POSTS_BATCH: usize = 25;
    pub const PROFILES_BATCH: usize = 25;

    /// `getQuotes` and `getFeed` page at 100 items per call, TECH-DESIGN
    /// section 8.1.
    pub const PAGE_LIMIT: u32 = 100;

    /// Builds a client from `cfg.appview_url` and `cfg.appview_rps`. Rejects
    /// a non-positive or non-finite rate with `AppViewError::InvalidRate`
    /// (BC15) rather than building a `tokio::time::Interval` with a zero or
    /// non-finite period, which would panic.
    pub fn new(cfg: &Config) -> Result<Self, AppViewError> {
        Self::with_base_url(cfg.appview_url.clone(), cfg.appview_rps)
    }

    /// `new`'s body, taking the base URL directly so tests can point the
    /// client at an unreachable address without building a full `Config`.
    fn with_base_url(base_url: String, rps: f64) -> Result<Self, AppViewError> {
        if !rps.is_finite() || rps <= 0.0 {
            return Err(AppViewError::InvalidRate);
        }
        let period = Duration::from_secs_f64(1.0 / rps);
        let mut interval = time::interval(period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("reqwest::Client::builder with only a timeout never fails to build");
        Ok(Self { base_url, http, limiter: Mutex::new(interval) })
    }

    /// Waits for the rate limiter's next tick before a request goes out.
    async fn throttle(&self) {
        self.limiter.lock().await.tick().await;
    }

    /// Sends one GET to `path` with `query`, retrying on a retryable status
    /// or a transport error per `retry_decision` (BC25), and decodes the
    /// body as `T`. Every failure, transport-level or after the retries are
    /// spent, becomes `AppViewError::Failed` with `method`, the status when
    /// there was one, and the number of attempts made.
    async fn get_with_retry<T: DeserializeOwned>(
        &self,
        method: &'static str,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T, AppViewError> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempt: u32 = 0;
        loop {
            self.throttle().await;
            let response = match self.http.get(&url).query(query).send().await {
                Ok(response) => response,
                Err(_err) => match retry_decision(None, attempt) {
                    RetryDecision::RetryAfter(delay) => {
                        time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }
                    RetryDecision::Fail => {
                        return Err(AppViewError::Failed {
                            method,
                            status: None,
                            attempts: attempt + 1,
                        });
                    }
                },
            };
            let status = response.status();
            if status.is_success() {
                return response.json::<T>().await.map_err(|_err| AppViewError::Failed {
                    method,
                    status: Some(status.as_u16()),
                    attempts: attempt + 1,
                });
            }
            match retry_decision(Some(status.as_u16()), attempt) {
                RetryDecision::RetryAfter(delay) => {
                    time::sleep(delay).await;
                    attempt += 1;
                }
                RetryDecision::Fail => {
                    return Err(AppViewError::Failed {
                        method,
                        status: Some(status.as_u16()),
                        attempts: attempt + 1,
                    });
                }
            }
        }
    }

    /// `app.bsky.feed.getPosts`. Splits `uris` into groups of
    /// [`Self::POSTS_BATCH`] with `slice::chunks`, one request per group,
    /// and merges every group's results into one map with `HashMap::extend`
    /// (BC7). Returns an empty map, `Ok`, with no request at all when
    /// `uris` is empty (BC8). A URI absent from the response is simply
    /// absent from the map (BC12), not an error.
    pub async fn get_posts(
        &self,
        uris: &[String],
    ) -> Result<HashMap<String, PostView>, AppViewError> {
        let mut result = HashMap::new();
        if uris.is_empty() {
            return Ok(result);
        }
        for group in uris.chunks(Self::POSTS_BATCH) {
            let query: Vec<(&str, &str)> = group.iter().map(|uri| ("uris", uri.as_str())).collect();
            let response: GetPostsResponse =
                self.get_with_retry("getPosts", "/xrpc/app.bsky.feed.getPosts", &query).await?;
            result.extend(response.posts.into_iter().map(|post| (post.uri.clone(), post)));
        }
        Ok(result)
    }

    /// `app.bsky.actor.getProfiles`. Splits `dids` into groups of
    /// [`Self::PROFILES_BATCH`], one request per group, merged the same way
    /// as [`Self::get_posts`]. Empty input makes no request (BC8).
    pub async fn get_profiles(
        &self,
        dids: &[String],
    ) -> Result<HashMap<String, ProfileView>, AppViewError> {
        let mut result = HashMap::new();
        if dids.is_empty() {
            return Ok(result);
        }
        for group in dids.chunks(Self::PROFILES_BATCH) {
            let query: Vec<(&str, &str)> =
                group.iter().map(|did| ("actors", did.as_str())).collect();
            let response: GetProfilesResponse = self
                .get_with_retry("getProfiles", "/xrpc/app.bsky.actor.getProfiles", &query)
                .await?;
            result.extend(
                response.profiles.into_iter().map(|profile| (profile.did.clone(), profile)),
            );
        }
        Ok(result)
    }

    /// `app.bsky.feed.getQuotes`. One page of at most [`Self::PAGE_LIMIT`]
    /// quoting posts of `uri`, from `cursor` when given. Story 03 owns the
    /// paging loop that follows the returned cursor.
    pub async fn get_quotes(
        &self,
        uri: &str,
        cursor: Option<&str>,
    ) -> Result<GetQuotesResponse, AppViewError> {
        let limit = Self::PAGE_LIMIT.to_string();
        let mut query: Vec<(&str, &str)> = vec![("uri", uri), ("limit", &limit)];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor));
        }
        self.get_with_retry("getQuotes", "/xrpc/app.bsky.feed.getQuotes", &query).await
    }

    /// `app.bsky.feed.getFeed`. One page of at most [`Self::PAGE_LIMIT`]
    /// items of `feed`, from `cursor` when given. Story 03 seeds its walk
    /// from `hot-classic` with this method.
    pub async fn get_feed(
        &self,
        feed: &str,
        cursor: Option<&str>,
    ) -> Result<GetFeedResponse, AppViewError> {
        let limit = Self::PAGE_LIMIT.to_string();
        let mut query: Vec<(&str, &str)> = vec![("feed", feed), ("limit", &limit)];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor));
        }
        self.get_with_retry("getFeed", "/xrpc/app.bsky.feed.getFeed", &query).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{EmbedView, RecordViewInner};

    const GETPOSTS_OK: &str = include_str!("../../tests/fixtures/getposts_ok.json");
    const GETPROFILES_OK: &str = include_str!("../../tests/fixtures/getprofiles_ok.json");
    const GETPOSTS_VIEW_DETACHED: &str =
        include_str!("../../tests/fixtures/getposts_view_detached.json");
    const GETPOSTS_VIEW_BLOCKED: &str =
        include_str!("../../tests/fixtures/getposts_view_blocked.json");
    const GETPOSTS_VIEW_NOT_FOUND: &str =
        include_str!("../../tests/fixtures/getposts_view_not_found.json");
    const GETPOSTS_RECORD_WITH_MEDIA: &str =
        include_str!("../../tests/fixtures/getposts_record_with_media.json");

    #[test]
    fn retry_decision_backs_off_three_times_then_fails() {
        assert_eq!(retry_decision(Some(429), 0), RetryDecision::RetryAfter(Duration::from_secs(1)));
        assert_eq!(retry_decision(Some(429), 1), RetryDecision::RetryAfter(Duration::from_secs(2)));
        assert_eq!(retry_decision(Some(429), 2), RetryDecision::RetryAfter(Duration::from_secs(4)));
        assert_eq!(retry_decision(Some(429), 3), RetryDecision::Fail);
        assert_eq!(retry_decision(Some(503), 0), RetryDecision::RetryAfter(Duration::from_secs(1)));
        assert_eq!(retry_decision(Some(503), 3), RetryDecision::Fail);
    }

    #[test]
    fn retry_decision_fails_immediately_on_other_4xx() {
        for attempt in 0..4 {
            assert_eq!(retry_decision(Some(404), attempt), RetryDecision::Fail);
            assert_eq!(retry_decision(Some(400), attempt), RetryDecision::Fail);
        }
    }

    #[test]
    fn retry_decision_retries_a_transport_error_on_the_5xx_schedule() {
        // BC25: no HTTP status at all (`None`) is retried on the same
        // schedule as a 5xx, and fails on the fourth attempt.
        assert_eq!(retry_decision(None, 0), RetryDecision::RetryAfter(Duration::from_secs(1)));
        assert_eq!(retry_decision(None, 1), RetryDecision::RetryAfter(Duration::from_secs(2)));
        assert_eq!(retry_decision(None, 2), RetryDecision::RetryAfter(Duration::from_secs(4)));
        assert_eq!(retry_decision(None, 3), RetryDecision::Fail);
    }

    #[test]
    fn batches_over_25() {
        // BC7: more than 25 URIs split into groups, and the results of every
        // group merge into one map, via the same `HashMap::extend` code path
        // `get_posts` itself calls.
        let uris: Vec<String> = (0..60).map(|i| format!("uri-{i}")).collect();
        let groups: Vec<&[String]> = uris.chunks(AppViewClient::POSTS_BATCH).collect();
        assert_eq!(groups.len(), 3);

        let decoded: GetPostsResponse = serde_json::from_str(GETPOSTS_OK).expect("fixture decodes");
        assert_eq!(decoded.posts.len(), 2);
        let (first, second) = decoded.posts.split_at(1);

        let mut map = HashMap::new();
        map.extend(first.iter().cloned().map(|post| (post.uri.clone(), post)));
        map.extend(second.iter().cloned().map(|post| (post.uri.clone(), post)));
        assert_eq!(map.len(), 2);
    }

    #[tokio::test]
    async fn get_posts_makes_no_request_on_empty_input() {
        let client =
            AppViewClient::with_base_url("http://appview.invalid.example".to_string(), 1.0)
                .expect("positive rate builds a client");
        let result = client.get_posts(&[]).await.expect("empty input never reaches the network");
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn get_profiles_makes_no_request_on_empty_input() {
        let client =
            AppViewClient::with_base_url("http://appview.invalid.example".to_string(), 1.0)
                .expect("positive rate builds a client");
        let result = client.get_profiles(&[]).await.expect("empty input never reaches the network");
        assert!(result.is_empty());
    }

    #[test]
    fn zero_rate_is_rejected() {
        let err = AppViewClient::with_base_url("http://example.com".to_string(), 0.0).unwrap_err();
        assert_eq!(err, AppViewError::InvalidRate);
    }

    #[test]
    fn negative_rate_is_rejected() {
        let err = AppViewClient::with_base_url("http://example.com".to_string(), -1.0).unwrap_err();
        assert_eq!(err, AppViewError::InvalidRate);
    }

    #[test]
    fn nonfinite_rate_is_rejected() {
        let err =
            AppViewClient::with_base_url("http://example.com".to_string(), f64::NAN).unwrap_err();
        assert_eq!(err, AppViewError::InvalidRate);
    }

    #[test]
    fn getposts_fixture_decodes() {
        let decoded: GetPostsResponse = serde_json::from_str(GETPOSTS_OK).expect("fixture decodes");
        assert_eq!(decoded.posts.len(), 2);
        let first = &decoded.posts[0];
        assert_eq!(first.author.did, "did:plc:z72i7hdynmk6r22z27h6tvur");
        match &first.embed {
            Some(EmbedView::Record { record: RecordViewInner::ViewRecord(view) }) => {
                assert_eq!(
                    view.uri,
                    "at://did:plc:plcl43vt7d2ig7hif4zmyg6h/app.bsky.feed.post/3mv45xxdbx22k"
                );
            }
            other => panic!("expected a resolved record view, got {other:?}"),
        }
    }

    #[test]
    fn getprofiles_fixture_decodes() {
        let decoded: GetProfilesResponse =
            serde_json::from_str(GETPROFILES_OK).expect("fixture decodes");
        assert_eq!(decoded.profiles.len(), 2);
        assert_eq!(decoded.profiles[0].did, "did:plc:z72i7hdynmk6r22z27h6tvur");
    }

    #[test]
    fn missing_uri_is_absent_from_the_map() {
        let decoded: GetPostsResponse = serde_json::from_str(GETPOSTS_OK).expect("fixture decodes");
        let mut map = HashMap::new();
        map.extend(decoded.posts.into_iter().map(|post| (post.uri.clone(), post)));
        assert!(!map.contains_key("at://did:plc:doesnotexist/app.bsky.feed.post/missing"));
    }

    #[test]
    fn detached_embed_decodes_into_its_own_variant() {
        // BC22: a `record.$type` of `#viewDetached` decodes into its own
        // named variant, not the catch-all `Other`, so story 07 can tell it
        // apart from a blocked or not-found quote. `getposts_view_detached.json`
        // is hand-edited from a recorded `getPosts` body: no live post with
        // a detached quote was found to record one directly.
        let decoded: GetPostsResponse =
            serde_json::from_str(GETPOSTS_VIEW_DETACHED).expect("fixture decodes");
        let post = &decoded.posts[0];
        match &post.embed {
            Some(EmbedView::Record { record: RecordViewInner::ViewDetached { uri } }) => {
                assert_eq!(
                    uri,
                    "at://did:plc:plcl43vt7d2ig7hif4zmyg6h/app.bsky.feed.post/3mv45xxdbx22k"
                );
            }
            other => panic!("expected ViewDetached, got {other:?}"),
        }
    }

    #[test]
    fn blocked_embed_decodes_into_its_own_variant() {
        // BC22: a `record.$type` of `#viewBlocked` decodes into its own
        // named variant. `getposts_view_blocked.json` is hand-edited from a
        // recorded `getPosts` body: no live post with a blocked quote was
        // found to record one directly.
        let decoded: GetPostsResponse =
            serde_json::from_str(GETPOSTS_VIEW_BLOCKED).expect("fixture decodes");
        let post = &decoded.posts[0];
        match &post.embed {
            Some(EmbedView::Record { record: RecordViewInner::ViewBlocked { uri } }) => {
                assert_eq!(
                    uri,
                    "at://did:plc:plcl43vt7d2ig7hif4zmyg6h/app.bsky.feed.post/3mv45xxdbx22k"
                );
            }
            other => panic!("expected ViewBlocked, got {other:?}"),
        }
    }

    #[test]
    fn not_found_embed_decodes_into_its_own_variant() {
        // BC22: a `record.$type` of `#viewNotFound` decodes into its own
        // named variant. `getposts_view_not_found.json` is hand-edited from
        // a recorded `getPosts` body: no live post with a not-found quote
        // was found to record one directly.
        let decoded: GetPostsResponse =
            serde_json::from_str(GETPOSTS_VIEW_NOT_FOUND).expect("fixture decodes");
        let post = &decoded.posts[0];
        match &post.embed {
            Some(EmbedView::Record { record: RecordViewInner::ViewNotFound { uri } }) => {
                assert_eq!(
                    uri,
                    "at://did:plc:plcl43vt7d2ig7hif4zmyg6h/app.bsky.feed.post/3mv45xxdbx22k"
                );
            }
            other => panic!("expected ViewNotFound, got {other:?}"),
        }
    }

    #[test]
    fn record_with_media_decodes_through_both_hops() {
        // BC26: `app.bsky.embed.recordWithMedia#view` whose `record.record`
        // is `#viewRecord` decodes through both hops to `ViewRecord`.
        // `getposts_record_with_media.json` is hand-made: no live post with
        // this exact shape was found to record one directly.
        let decoded: GetPostsResponse =
            serde_json::from_str(GETPOSTS_RECORD_WITH_MEDIA).expect("fixture decodes");
        let post = &decoded.posts[0];
        match &post.embed {
            Some(EmbedView::RecordWithMedia {
                record: types::RecordWithMediaInner { record: RecordViewInner::ViewRecord(view) },
            }) => {
                assert_eq!(
                    view.uri,
                    "at://did:plc:plcl43vt7d2ig7hif4zmyg6h/app.bsky.feed.post/3mv45xxdbx22k"
                );
            }
            other => panic!("expected a resolved record view through both hops, got {other:?}"),
        }
    }
}

/// Live tests against `https://public.api.bsky.app`. `#[ignore]`d, so
/// `cargo test --all-features` never touches the network; run by hand with
/// `cargo test --all-features -- --ignored`.
#[cfg(test)]
mod live_tests {
    use super::*;

    fn live_client() -> AppViewClient {
        AppViewClient::with_base_url("https://public.api.bsky.app".to_string(), 3.0)
            .expect("a positive rate builds a client")
    }

    #[tokio::test]
    #[ignore]
    async fn appview_getposts_live() {
        let client = live_client();
        let uris =
            vec!["at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3mv45zmynys2l"
                .to_string()];
        let result = client.get_posts(&uris).await.expect("live getPosts round trip");
        assert!(!result.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn appview_getprofiles_live() {
        let client = live_client();
        let dids = vec!["did:plc:z72i7hdynmk6r22z27h6tvur".to_string()];
        let result = client.get_profiles(&dids).await.expect("live getProfiles round trip");
        assert!(!result.is_empty());
    }
}
