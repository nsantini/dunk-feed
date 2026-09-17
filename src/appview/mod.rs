//! Typed client over the App View, TECH-DESIGN section 8. Hand-written
//! `serde` structs, not `atrium-api` (section 1 rejects it for its size).
//! One `reqwest::Client`, rate limited, retried on 429 and 5xx with
//! backoff, and batched at the sizes section 8.1 gives. `validate` (story
//! 03) and the scorer (story 07) call these exact four methods instead of
//! writing their own copy.

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
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
pub enum AppViewError {
    #[error("appview rate must be a finite number greater than zero")]
    InvalidRate,
    #[error("{method} failed after {attempts} attempt(s), status {status:?}")]
    Failed { method: &'static str, status: Option<u16>, attempts: u32 },
}

/// What to do after a non-success response, TECH-DESIGN section 8.1's
/// "three attempts, on a fourth failure the pass logs and moves on".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
pub enum RetryDecision {
    RetryAfter(Duration),
    Fail,
}

/// Decides whether to retry a failed attempt, pure. `status` is the HTTP
/// status of the failed attempt; `attempt` is the zero-based index of that
/// attempt. A 429 or a 5xx retries after 1s, 2s and 4s on attempts 0, 1 and
/// 2 (BC9); attempt 3, the fourth failure, fails (BC10). Any other 4xx
/// fails immediately, with no backoff and no second request (BC11).
/// `retry_decision` is called only on a non-success status; a 2xx never
/// reaches it.
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
pub fn retry_decision(status: u16, attempt: u32) -> RetryDecision {
    let retryable = status == 429 || (500..600).contains(&status);
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

/// Splits `items` into groups of at most `size`, pure. An empty slice
/// yields no groups at all, so a caller that checks `items.is_empty()`
/// before calling this never needs to handle a single empty group.
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
pub fn chunk<T>(items: &[T], size: usize) -> Vec<&[T]> {
    items.chunks(size).collect()
}

/// Merges one page of `getPosts` results into `map`, keyed by `uri`. A URI
/// present in `map`'s caller's request but absent from `posts` is simply
/// never inserted (BC12); that is not this function's concern, and not an
/// error.
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
fn merge_posts(map: &mut HashMap<String, PostView>, posts: Vec<PostView>) {
    for post in posts {
        map.insert(post.uri.clone(), post);
    }
}

/// Merges one page of `getProfiles` results into `map`, keyed by `did`.
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
fn merge_profiles(map: &mut HashMap<String, ProfileView>, profiles: Vec<ProfileView>) {
    for profile in profiles {
        map.insert(profile.did.clone(), profile);
    }
}

/// A typed client over `app.bsky.feed.getPosts`, `app.bsky.actor.getProfiles`,
/// `app.bsky.feed.getQuotes` and `app.bsky.feed.getFeed`. Unauthenticated;
/// the authenticated calls (`createSession`, `uploadBlob`, `putRecord`) are
/// story 09. One request at a time is let through the rate limiter, so two
/// concurrent callers never burst past `appview_rps`.
#[derive(Debug)]
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
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
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    pub const PROFILES_BATCH: usize = 25;

    /// `getQuotes` and `getFeed` page at 100 items per call, TECH-DESIGN
    /// section 8.1.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    pub const PAGE_LIMIT: u32 = 100;

    /// Builds a client from `cfg.appview_url` and `cfg.appview_rps`. Rejects
    /// a non-positive or non-finite rate with `AppViewError::InvalidRate`
    /// (BC15) rather than building a `tokio::time::Interval` with a zero or
    /// non-finite period, which would panic.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    pub fn new(cfg: &Config) -> Result<Self, AppViewError> {
        Self::with_base_url(cfg.appview_url.clone(), cfg.appview_rps)
    }

    /// `new`'s body, taking the base URL directly so tests can point the
    /// client at an unreachable address without building a full `Config`.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
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
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    async fn throttle(&self) {
        self.limiter.lock().await.tick().await;
    }

    /// Sends one GET to `path` with `query`, retrying on a retryable status
    /// per `retry_decision`, and decodes the body as `T`. Every failure,
    /// transport-level or after the retries are spent, becomes
    /// `AppViewError::Failed` with `method`, the status when there was one,
    /// and the number of attempts made.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
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
                Err(_err) => {
                    return Err(AppViewError::Failed {
                        method,
                        status: None,
                        attempts: attempt + 1,
                    });
                }
            };
            let status = response.status();
            if status.is_success() {
                return response.json::<T>().await.map_err(|_err| AppViewError::Failed {
                    method,
                    status: Some(status.as_u16()),
                    attempts: attempt + 1,
                });
            }
            match retry_decision(status.as_u16(), attempt) {
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
    /// [`Self::POSTS_BATCH`], one request per group, and merges every
    /// group's results into one map (BC7). Returns an empty map, `Ok`, with
    /// no request at all when `uris` is empty (BC8). A URI absent from the
    /// response is simply absent from the map (BC12), not an error.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    pub async fn get_posts(
        &self,
        uris: &[String],
    ) -> Result<HashMap<String, PostView>, AppViewError> {
        let mut result = HashMap::new();
        if uris.is_empty() {
            return Ok(result);
        }
        for group in chunk(uris, Self::POSTS_BATCH) {
            let query: Vec<(&str, &str)> = group.iter().map(|uri| ("uris", uri.as_str())).collect();
            let response: GetPostsResponse =
                self.get_with_retry("getPosts", "/xrpc/app.bsky.feed.getPosts", &query).await?;
            merge_posts(&mut result, response.posts);
        }
        Ok(result)
    }

    /// `app.bsky.actor.getProfiles`. Splits `dids` into groups of
    /// [`Self::PROFILES_BATCH`], one request per group, merged the same way
    /// as [`Self::get_posts`]. Empty input makes no request (BC8).
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    pub async fn get_profiles(
        &self,
        dids: &[String],
    ) -> Result<HashMap<String, ProfileView>, AppViewError> {
        let mut result = HashMap::new();
        if dids.is_empty() {
            return Ok(result);
        }
        for group in chunk(dids, Self::PROFILES_BATCH) {
            let query: Vec<(&str, &str)> =
                group.iter().map(|did| ("actors", did.as_str())).collect();
            let response: GetProfilesResponse = self
                .get_with_retry("getProfiles", "/xrpc/app.bsky.actor.getProfiles", &query)
                .await?;
            merge_profiles(&mut result, response.profiles);
        }
        Ok(result)
    }

    /// `app.bsky.feed.getQuotes`. One page of at most [`Self::PAGE_LIMIT`]
    /// quoting posts of `uri`, from `cursor` when given. Story 03 owns the
    /// paging loop that follows the returned cursor.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
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
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
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

    #[test]
    fn retry_decision_backs_off_three_times_then_fails() {
        assert_eq!(retry_decision(429, 0), RetryDecision::RetryAfter(Duration::from_secs(1)));
        assert_eq!(retry_decision(429, 1), RetryDecision::RetryAfter(Duration::from_secs(2)));
        assert_eq!(retry_decision(429, 2), RetryDecision::RetryAfter(Duration::from_secs(4)));
        assert_eq!(retry_decision(429, 3), RetryDecision::Fail);
        assert_eq!(retry_decision(503, 0), RetryDecision::RetryAfter(Duration::from_secs(1)));
        assert_eq!(retry_decision(503, 3), RetryDecision::Fail);
    }

    #[test]
    fn retry_decision_fails_immediately_on_other_4xx() {
        for attempt in 0..4 {
            assert_eq!(retry_decision(404, attempt), RetryDecision::Fail);
            assert_eq!(retry_decision(400, attempt), RetryDecision::Fail);
        }
    }

    #[test]
    fn chunk_boundaries() {
        let empty: Vec<i32> = Vec::new();
        assert_eq!(chunk(&empty, 25).len(), 0);

        let one = vec![1];
        assert_eq!(chunk(&one, 25).len(), 1);

        let exactly_25: Vec<i32> = (0..25).collect();
        let groups = chunk(&exactly_25, 25);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 25);

        let twenty_six: Vec<i32> = (0..26).collect();
        let groups = chunk(&twenty_six, 25);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 25);
        assert_eq!(groups[1].len(), 1);

        let sixty: Vec<i32> = (0..60).collect();
        let groups = chunk(&sixty, 25);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].len(), 25);
        assert_eq!(groups[1].len(), 25);
        assert_eq!(groups[2].len(), 10);
    }

    #[test]
    fn batches_over_25() {
        // BC7: more than 25 URIs split into groups, and the results of every
        // group merge into one map, via the same `merge_posts` code path
        // `get_posts` itself calls.
        let uris: Vec<String> = (0..60).map(|i| format!("uri-{i}")).collect();
        let groups = chunk(&uris, AppViewClient::POSTS_BATCH);
        assert_eq!(groups.len(), 3);

        let decoded: GetPostsResponse = serde_json::from_str(GETPOSTS_OK).expect("fixture decodes");
        assert_eq!(decoded.posts.len(), 2);
        let (first, second) = decoded.posts.split_at(1);

        let mut map = HashMap::new();
        merge_posts(&mut map, first.to_vec());
        merge_posts(&mut map, second.to_vec());
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
        merge_posts(&mut map, decoded.posts);
        assert!(!map.contains_key("at://did:plc:doesnotexist/app.bsky.feed.post/missing"));
    }

    #[test]
    fn detached_embed_decodes_into_the_catch_all_variant() {
        // BC13: a `record.$type` of `#viewDetached` is not `#viewRecord`,
        // so it decodes into `RecordViewInner::Other`, not an error.
        // `getposts_view_detached.json` is hand-edited from a recorded
        // `getPosts` body: no live post with a detached quote was found
        // to record one directly.
        let decoded: GetPostsResponse =
            serde_json::from_str(GETPOSTS_VIEW_DETACHED).expect("fixture decodes");
        let post = &decoded.posts[0];
        match &post.embed {
            Some(EmbedView::Record { record: RecordViewInner::Other }) => {}
            other => panic!("expected the catch-all variant, got {other:?}"),
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
