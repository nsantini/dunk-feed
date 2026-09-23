//! `appview::pds::PdsClient`, TECH-DESIGN-network-feed §3 and §7: the one
//! PDS session and login path in the binary (spec.md `## Approach`).
//! `PdsTransport` is the transport seam `publish::PdsClient` used to be
//! before this story; `HttpPdsTransport` is its one `reqwest`
//! implementation. `PdsClient` holds the transport, the credentials, the
//! session in a `tokio::sync::Mutex`, and a `time::Interval` limiter at
//! `UPSTAGE_GRAPH_RPS`, the same shape `AppViewClient` uses for its own
//! rate (`src/appview/mod.rs`). Every call refreshes the session first
//! when the access token's `exp` is within five minutes, or once on an
//! `ExpiredToken` response, and retries 429s, 5xxs and transport errors
//! through `appview::retry_decision`. `get_follows` and
//! `get_relationships` (slice 2.0) and `publish`'s session, upload and
//! record calls (slice 3.0) go through the private `call` method; nothing
//! outside this module talks to `PdsTransport` directly.

#![allow(dead_code)] // First callers are slice 2.0 (`get_follows`,
                     // `get_relationships`) and slice 3.0 (`publish`).

use std::fmt;
use std::future::Future;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::{self, MissedTickBehavior};

use crate::appview::{http_client, retry_decision, RetryDecision};
use crate::config::{Config, Secret};

/// `com.atproto.server.createSession`'s nsid, used both to build the URL
/// and to label a login failure (BC1).
const LOGIN_NSID: &str = "com.atproto.server.createSession";

/// `com.atproto.server.refreshSession`'s nsid (BC2, BC4).
const REFRESH_NSID: &str = "com.atproto.server.refreshSession";

/// A call refreshes first when less than this many seconds remain before
/// the access token's `exp` (BC2).
const REFRESH_MARGIN_SECS: i64 = 5 * 60;

/// The `atproto-proxy` header value every `app.bsky.*` call carries (BC5).
const APPVIEW_PROXY: &str = "did:web:api.bsky.app#bsky_appview";

/// The longest limiter period [`PdsClient::new`] accepts, one day. A
/// `graph_rps` whose reciprocal exceeds this is rejected as
/// [`PdsError::InvalidRate`] (review round 1, finding 2): a rate that ticks
/// less than once a day is not a working rate limit, and reciprocals near
/// zero risk overflowing to a value `Duration::try_from_secs_f64` would
/// otherwise have to be trusted to catch unaided.
const MAX_LIMITER_PERIOD_SECS: f64 = 86_400.0;

/// Every way a `PdsClient` call can fail. No variant carries `accessJwt`,
/// `refreshJwt` or the app password (BC12): a session failure is
/// `PdsError::Session` with no detail from the failed attempt, and
/// `PdsError::Http` and `PdsError::Decode` carry only the nsid, the status
/// and a decode error's own message, never a header or credential.
#[derive(Debug, Error)]
pub enum PdsError {
    #[error("graph rate must be a finite number greater than zero")]
    InvalidRate,
    #[error("PDS session could not be established or refreshed")]
    Session,
    #[error("{method} failed after {attempts} attempt(s), status {status:?}")]
    Http { method: &'static str, status: Option<u16>, attempts: u32 },
    #[error("{method}: response did not decode: {reason}")]
    Decode { method: &'static str, reason: String },
    #[error("get_relationships called with {count} DIDs, more than the limit of 30")]
    TooMany { count: usize },
}

/// The two credentials a session is built from. Moved here from
/// `publish.rs` (this story's Approach): `publish` builds one of these
/// from `Config` in slice 3.0 instead of keeping its own copy. `Debug`
/// prints `[redacted]` for `app_password` because it stays inside a
/// [`Secret`] (BC12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub handle: String,
    pub app_password: Secret,
}

/// The PDS session, in memory only (BC1, BC12): never written to SQLite,
/// never logged. `exp` is the access token's own `exp` claim, read once at
/// login or refresh so [`is_near_expiry`] never has to decode the token
/// again on every call. `did` is the account's own DID, kept for a future
/// caller (`publish`'s DID mismatch check, slice 3.0); nothing in this
/// slice reads it yet.
#[derive(Debug, Clone)]
struct Session {
    did: String,
    access_jwt: Secret,
    refresh_jwt: Secret,
    exp: i64,
}

/// The two HTTP verbs every PDS call in this binary needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
}

/// A request body, or the lack of one. `Bytes` is `uploadBlob`'s shape
/// (slice 3.0): raw bytes with a fixed `Content-Type`, never JSON.
#[derive(Debug, Clone)]
pub enum RequestBody {
    None,
    Json(Value),
    Bytes { bytes: Vec<u8>, content_type: &'static str },
}

/// One transport-level response: an HTTP status and its best-effort JSON
/// body. A body that fails to decode as JSON becomes `Value::Null`, so a
/// transport implementation never has to invent an error variant for a
/// non-JSON response; the AT Protocol only ever returns JSON, so this only
/// matters for a body nothing here should have asked to decode anyway.
#[derive(Debug, Clone)]
pub struct RawResponse {
    pub status: u16,
    pub body: Value,
}

/// A transport-level failure: no HTTP response was produced at all (a
/// connection error or a timeout). Carries nothing, mirroring how
/// `AppViewClient::get_with_retry` treats a `.send().await` error: the
/// retry loop only needs to know "no status", not the underlying
/// `reqwest::Error`.
#[derive(Debug)]
pub struct TransportError;

/// The PDS surface every call in this module goes through: one method,
/// parametrised by nsid, HTTP verb, bearer token, the `atproto-proxy` flag
/// and the body (BC5, BC6). `PdsClient` is generic over this instead of
/// hard-coding `reqwest`, the same seam `publish::PdsClient` used before
/// this story (spec.md `## Approach`): a fake records every call without a
/// test HTTP server.
pub trait PdsTransport: Send + Sync {
    fn request(
        &self,
        http_method: HttpMethod,
        nsid: &'static str,
        bearer: Option<&str>,
        proxy: bool,
        query: &[(&str, &str)],
        body: RequestBody,
    ) -> impl Future<Output = Result<RawResponse, TransportError>> + Send;
}

/// The real [`PdsTransport`], one `reqwest::Client` built the same way
/// `AppViewClient` and the old `publish::HttpPdsClient` build theirs
/// ([`http_client`]). The limiter (BC7) lives one level up, in
/// `PdsClient`, not here: a transport has no notion of a rate.
#[derive(Debug)]
pub struct HttpPdsTransport {
    base_url: String,
    http: reqwest::Client,
}

impl HttpPdsTransport {
    /// Trims any trailing `/` from `base_url` once, here, so
    /// [`PdsTransport::request`]'s `format!("{}/xrpc/{}", ...)` never
    /// builds a URL with a doubled slash when `UPSTAGE_PDS_URL` is
    /// configured with a trailing slash (review round 1, finding 3).
    pub fn new(base_url: String) -> Self {
        Self { base_url: base_url.trim_end_matches('/').to_string(), http: http_client() }
    }
}

impl PdsTransport for HttpPdsTransport {
    async fn request(
        &self,
        http_method: HttpMethod,
        nsid: &'static str,
        bearer: Option<&str>,
        proxy: bool,
        query: &[(&str, &str)],
        body: RequestBody,
    ) -> Result<RawResponse, TransportError> {
        let url = format!("{}/xrpc/{}", self.base_url, nsid);
        let mut request = match http_method {
            HttpMethod::Get => self.http.get(&url),
            HttpMethod::Post => self.http.post(&url),
        };
        if !query.is_empty() {
            request = request.query(query);
        }
        if let Some(token) = bearer {
            request = request.bearer_auth(token);
        }
        if proxy {
            request = request.header("atproto-proxy", APPVIEW_PROXY);
        }
        request = match body {
            RequestBody::None => request,
            RequestBody::Json(value) => request.json(&value),
            RequestBody::Bytes { bytes, content_type } => {
                request.header(reqwest::header::CONTENT_TYPE, content_type).body(bytes)
            }
        };
        let response = request.send().await.map_err(|_err| TransportError)?;
        let status = response.status().as_u16();
        let body = response.json::<Value>().await.unwrap_or(Value::Null);
        Ok(RawResponse { status, body })
    }
}

/// App View methods (`app.bsky.*`) are proxied through the PDS with the
/// `atproto-proxy` header (BC5); repo and server methods (`com.atproto.*`)
/// are not (BC6). Computed from the nsid so a caller of `get_follows`,
/// `get_relationships`, or a future App View method never has to remember
/// to set this itself.
fn wants_proxy(nsid: &str) -> bool {
    nsid.starts_with("app.bsky.")
}

/// A completed transport round trip: a status was produced, success or
/// not. `attempts` counts every attempt made, including the ones
/// `retry_decision` retried past, so [`PdsError::Http`] reports the same
/// shape of number as `AppViewError::Failed`.
struct Attempted {
    status: u16,
    body: Value,
    attempts: u32,
}

/// Detects the AT Protocol's `ExpiredToken` XRPC error: HTTP 400 with a
/// JSON body `{"error": "ExpiredToken", ...}` (BC3; spec.md's "Defaults
/// taken" on the wire shape).
fn is_expired_token(attempted: &Attempted) -> bool {
    attempted.status == 400
        && attempted.body.get("error").and_then(Value::as_str) == Some("ExpiredToken")
}

/// Turns a completed round trip into a checked one: `Ok` only for a 2xx,
/// else [`PdsError::Http`] naming the nsid, the status and the number of
/// attempts. The shared status check behind both [`PdsClient::call_raw`]
/// (slice 3.0's `upload_blob` and `put_record` read the body themselves)
/// and [`decode_attempted`] (the graph methods, which decode straight into
/// a type).
fn check_status(nsid: &'static str, attempted: Attempted) -> Result<Attempted, PdsError> {
    if attempted.status >= 300 {
        return Err(PdsError::Http {
            method: nsid,
            status: Some(attempted.status),
            attempts: attempted.attempts,
        });
    }
    Ok(attempted)
}

/// Turns a completed round trip into a decoded value, or a [`PdsError`]
/// naming the nsid, the status and the number of attempts (a non-2xx), or
/// the decode failure's own message (a 2xx whose body is not `R`).
fn decode_attempted<R: DeserializeOwned>(
    nsid: &'static str,
    attempted: Attempted,
) -> Result<R, PdsError> {
    let attempted = check_status(nsid, attempted)?;
    serde_json::from_value(attempted.body)
        .map_err(|err| PdsError::Decode { method: nsid, reason: err.to_string() })
}

/// Reads the `exp` claim from a JWT's middle segment (BC2a): base64url
/// decoded, then parsed as JSON. A token that is not three dot-separated
/// segments, whose payload does not decode, or whose payload carries no
/// `exp` claim is treated as already expired (`0`, always more than
/// [`REFRESH_MARGIN_SECS`] in the past) rather than panicking: the PDS
/// issued the token, so this never checks the signature.
fn decode_exp(jwt: &str) -> i64 {
    let Some(payload) = jwt.split('.').nth(1) else { return 0 };
    let Ok(bytes) = URL_SAFE_NO_PAD.decode(payload) else { return 0 };
    let Ok(claims) = serde_json::from_slice::<Value>(&bytes) else { return 0 };
    claims.get("exp").and_then(Value::as_i64).unwrap_or(0)
}

/// `true` when `session`'s access token is within [`REFRESH_MARGIN_SECS`]
/// of `exp` (BC2), reading the wall clock once per check.
fn is_near_expiry(session: &Session) -> bool {
    session.exp - Utc::now().timestamp() < REFRESH_MARGIN_SECS
}

/// Builds a [`Session`] from a `createSession` or `refreshSession` 2xx
/// body. `method` labels a missing-field failure so it reads like any
/// other [`PdsError::Decode`].
fn session_from_body(method: &'static str, body: &Value) -> Result<Session, PdsError> {
    let field = |name: &'static str| -> Result<String, PdsError> {
        body.get(name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or(PdsError::Decode { method, reason: format!("response carried no {name}") })
    };
    let did = field("did")?;
    let access = field("accessJwt")?;
    let refresh = field("refreshJwt")?;
    let exp = decode_exp(&access);
    Ok(Session { did, access_jwt: Secret::new(access), refresh_jwt: Secret::new(refresh), exp })
}

/// `get_relationships` refuses more than this many DIDs in one call before
/// any network request (BC10). TECH-DESIGN-network-feed §6.2 sends
/// `others` in groups of 30; this is the AT Protocol's own limit on
/// `app.bsky.graph.getRelationships`, not a value from `Config`.
const RELATIONSHIPS_MAX: usize = 30;

/// `get_follows`'s only field of interest in each `follows` entry: the
/// subject's own DID (BC9). Every other field of the profile view is
/// ignored; `serde` drops fields not named here.
#[derive(Debug, Clone, Deserialize)]
struct FollowSubject {
    did: String,
}

/// The decoded shape of `app.bsky.graph.getFollows` (BC9, BC9a). `cursor`
/// is absent from the last page, which `serde`'s `default` turns into
/// `None` rather than a decode failure.
#[derive(Debug, Clone, Deserialize)]
struct GetFollowsResponse {
    follows: Vec<FollowSubject>,
    #[serde(default)]
    cursor: Option<String>,
}

/// One page of `get_follows`: the subject DIDs of that page, in response
/// order, and the cursor for the next page, `None` on the last page
/// (BC9, BC9a). The caller's own loop follows `cursor` until it is `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowsPage {
    pub dids: Vec<String>,
    pub cursor: Option<String>,
}

/// The decoded shape of `app.bsky.graph.getRelationships`: each entry of
/// `relationships` is left as a raw [`Value`] rather than a typed enum,
/// because the only two shapes that matter here, `#relationship` (with
/// `did` and, when present, `followedBy`) and `#notFoundActor` (with
/// neither), are told apart by which fields are present, not by matching
/// on `$type` (BC11).
#[derive(Debug, Clone, Deserialize)]
struct GetRelationshipsResponse {
    #[serde(default)]
    relationships: Vec<Value>,
}

/// The shared PDS session, TECH-DESIGN-network-feed §7: one login, a
/// limiter at `graph_rps`, and the refresh/retry rules every call goes
/// through. Generic over [`PdsTransport`] so tests use a fake that records
/// every call; `PdsClient<HttpPdsTransport>` (via [`Self::from_config`]) is
/// what `publish` (slice 3.0) and the graph methods (slice 2.0) build.
pub struct PdsClient<T: PdsTransport = HttpPdsTransport> {
    transport: T,
    credentials: Credentials,
    session: Mutex<Option<Session>>,
    limiter: Mutex<time::Interval>,
}

/// Manual `Debug`: only `credentials` is shown (already redacted by
/// [`Secret`]'s own `Debug`, BC12), and the session and limiter are left
/// out entirely rather than relying on their own types to keep redacting
/// correctly forever.
impl<T: PdsTransport> fmt::Debug for PdsClient<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PdsClient").field("credentials", &self.credentials).finish_non_exhaustive()
    }
}

impl<T: PdsTransport> PdsClient<T> {
    /// Builds a client over `transport` at `graph_rps` calls each second.
    /// Rejects a non-positive or non-finite rate with
    /// [`PdsError::InvalidRate`] (BC14 is enforced earlier, at config load;
    /// this is defence in depth for a caller that built the rate some
    /// other way, the same reason `AppViewClient::new` re-checks its own).
    /// The period is built with [`Duration::try_from_secs_f64`] rather than
    /// the panicking `Duration::from_secs_f64` (review round 1, finding 2),
    /// so a pathological reciprocal can never panic here. A rate whose
    /// period would exceed [`MAX_LIMITER_PERIOD_SECS`] — such as `1e-15`,
    /// whose reciprocal is a period of roughly 31.7 million years — is also
    /// [`PdsError::InvalidRate`]: a "rate limiter" whose next tick is that
    /// far away limits nothing, so it is rejected up front rather than
    /// built and silently never ticking.
    pub fn new(transport: T, credentials: Credentials, graph_rps: f64) -> Result<Self, PdsError> {
        if !graph_rps.is_finite() || graph_rps <= 0.0 {
            return Err(PdsError::InvalidRate);
        }
        let period_secs = 1.0 / graph_rps;
        if !period_secs.is_finite() || period_secs > MAX_LIMITER_PERIOD_SECS {
            return Err(PdsError::InvalidRate);
        }
        let period = Duration::try_from_secs_f64(period_secs).map_err(|_| PdsError::InvalidRate)?;
        let mut interval = time::interval(period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Ok(Self {
            transport,
            credentials,
            session: Mutex::new(None),
            limiter: Mutex::new(interval),
        })
    }

    /// Waits for the rate limiter's next tick. Every transport call,
    /// including a session call, goes through this first (BC7).
    async fn throttle(&self) {
        self.limiter.lock().await.tick().await;
    }

    /// `com.atproto.server.createSession` (BC1). Never proxied: session
    /// calls are always `com.atproto.server.*`.
    async fn login_raw(&self) -> Result<Session, PdsError> {
        let body = RequestBody::Json(json!({
            "identifier": self.credentials.handle,
            "password": self.credentials.app_password.expose(),
        }));
        let attempted =
            self.send_with_retry(HttpMethod::Post, LOGIN_NSID, None, false, &[], body).await?;
        if attempted.status >= 300 {
            return Err(PdsError::Http {
                method: LOGIN_NSID,
                status: Some(attempted.status),
                attempts: attempted.attempts,
            });
        }
        session_from_body(LOGIN_NSID, &attempted.body)
    }

    /// `com.atproto.server.refreshSession`, bearing the refresh token, not
    /// the access token, as its bearer (BC2, BC4): that is how the AT
    /// Protocol tells a refresh call from an ordinary one.
    async fn refresh_raw(&self, current: &Session) -> Result<Session, PdsError> {
        let attempted = self
            .send_with_retry(
                HttpMethod::Post,
                REFRESH_NSID,
                Some(current.refresh_jwt.expose()),
                false,
                &[],
                RequestBody::None,
            )
            .await?;
        if attempted.status >= 300 {
            return Err(PdsError::Http {
                method: REFRESH_NSID,
                status: Some(attempted.status),
                attempts: attempted.attempts,
            });
        }
        session_from_body(REFRESH_NSID, &attempted.body)
    }

    /// Refreshes the session already in `guard`, or logs in again once
    /// when the refresh fails (BC4): the "any call" rule, reused by both
    /// the proactive near-expiry check and the `ExpiredToken` retry (BC3).
    /// Leaves `guard` holding the new session on `Ok`; on
    /// `Err(PdsError::Session)` the old session is left in place, since
    /// nothing better replaced it.
    async fn refresh_or_relogin_locked(
        &self,
        guard: &mut tokio::sync::MutexGuard<'_, Option<Session>>,
    ) -> Result<(), PdsError> {
        let current = guard.as_ref().expect("caller holds a session already").clone();
        if let Ok(refreshed) = self.refresh_raw(&current).await {
            **guard = Some(refreshed);
            return Ok(());
        }
        match self.login_raw().await {
            Ok(session) => {
                **guard = Some(session);
                Ok(())
            }
            Err(_) => Err(PdsError::Session),
        }
    }

    /// Makes sure `self.session` holds a session whose access token is not
    /// within [`REFRESH_MARGIN_SECS`] of `exp` (BC1, BC2): logs in when
    /// there is no session yet, and refreshes (or relogs in, BC4) when
    /// there is one but it is close to expiry. `Ok(())` always leaves a
    /// session in place.
    async fn ensure_fresh_session(&self) -> Result<(), PdsError> {
        let mut guard = self.session.lock().await;
        if guard.is_none() {
            let session = self.login_raw().await?;
            *guard = Some(session);
        }
        let near = is_near_expiry(guard.as_ref().expect("just set, or already present"));
        if near {
            self.refresh_or_relogin_locked(&mut guard).await?;
        }
        Ok(())
    }

    /// Sends one call through the rate limiter, retrying a retryable
    /// failure per `appview::retry_decision` (BC7, BC8). Returns `Ok` for
    /// any outcome that produced an HTTP status, success or not, so the
    /// caller can inspect the status: an `ExpiredToken` 400 needs
    /// different handling than any other failure. Only a transport error
    /// that outlasts the retry schedule becomes `Err`.
    async fn send_with_retry(
        &self,
        http_method: HttpMethod,
        nsid: &'static str,
        bearer: Option<&str>,
        proxy: bool,
        query: &[(&str, &str)],
        body: RequestBody,
    ) -> Result<Attempted, PdsError> {
        let mut attempt: u32 = 0;
        loop {
            self.throttle().await;
            match self
                .transport
                .request(http_method, nsid, bearer, proxy, query, body.clone())
                .await
            {
                Ok(response) if response.status < 300 => {
                    return Ok(Attempted {
                        status: response.status,
                        body: response.body,
                        attempts: attempt + 1,
                    });
                }
                Ok(response) => match retry_decision(Some(response.status), attempt) {
                    RetryDecision::RetryAfter(delay) => {
                        time::sleep(delay).await;
                        attempt += 1;
                    }
                    RetryDecision::Fail => {
                        return Ok(Attempted {
                            status: response.status,
                            body: response.body,
                            attempts: attempt + 1,
                        });
                    }
                },
                Err(TransportError) => match retry_decision(None, attempt) {
                    RetryDecision::RetryAfter(delay) => {
                        time::sleep(delay).await;
                        attempt += 1;
                    }
                    RetryDecision::Fail => {
                        return Err(PdsError::Http {
                            method: nsid,
                            status: None,
                            attempts: attempt + 1,
                        });
                    }
                },
            }
        }
    }

    /// Runs one PDS call through the session, the limiter and the retry
    /// schedule (BC2, BC3, BC7, BC8): logs in on the first call, refreshes
    /// before a call when the access token's `exp` is within
    /// [`REFRESH_MARGIN_SECS`], and retries an `ExpiredToken` response once
    /// with a fresh session (BC3). `nsid` decides the `atproto-proxy`
    /// header through [`wants_proxy`] (BC5, BC6). A second `ExpiredToken`
    /// after the retry is [`PdsError::Session`] (BC3). Status-checked
    /// (`Ok` only for a 2xx) but not decoded: [`Self::call`] decodes the
    /// body into a type; [`Self::upload_blob`] and [`Self::put_record`]
    /// (slice 3.0) read the raw body or ignore it.
    async fn call_raw(
        &self,
        http_method: HttpMethod,
        nsid: &'static str,
        query: &[(&str, &str)],
        body: RequestBody,
    ) -> Result<Attempted, PdsError> {
        let proxy = wants_proxy(nsid);
        self.ensure_fresh_session().await?;
        let bearer = {
            let guard = self.session.lock().await;
            guard
                .as_ref()
                .expect("ensure_fresh_session always leaves a session")
                .access_jwt
                .expose()
                .to_string()
        };
        let attempted = self
            .send_with_retry(http_method, nsid, Some(&bearer), proxy, query, body.clone())
            .await?;
        if !is_expired_token(&attempted) {
            return check_status(nsid, attempted);
        }

        let mut guard = self.session.lock().await;
        self.refresh_or_relogin_locked(&mut guard).await?;
        let bearer = guard
            .as_ref()
            .expect("refresh_or_relogin_locked sets a session on Ok")
            .access_jwt
            .expose()
            .to_string();
        drop(guard);
        let attempted =
            self.send_with_retry(http_method, nsid, Some(&bearer), proxy, query, body).await?;
        if is_expired_token(&attempted) {
            return Err(PdsError::Session);
        }
        check_status(nsid, attempted)
    }

    /// [`Self::call_raw`], decoded into `R` (BC2, BC3, BC7, BC8).
    async fn call<R: DeserializeOwned>(
        &self,
        http_method: HttpMethod,
        nsid: &'static str,
        query: &[(&str, &str)],
        body: RequestBody,
    ) -> Result<R, PdsError> {
        let attempted = self.call_raw(http_method, nsid, query, body).await?;
        serde_json::from_value(attempted.body)
            .map_err(|err| PdsError::Decode { method: nsid, reason: err.to_string() })
    }

    /// `com.atproto.server.createSession`'s DID, for `publish`'s DID
    /// mismatch check (slice 3.0's `map_pds_error` maps a failure here to
    /// `PublishError::Auth`). Ensures a fresh session, which performs the
    /// first login for a client that has not called anything yet, and
    /// returns its `did`.
    pub async fn session_did(&self) -> Result<String, PdsError> {
        self.ensure_fresh_session().await?;
        let guard = self.session.lock().await;
        Ok(guard.as_ref().expect("ensure_fresh_session always leaves a session").did.clone())
    }

    /// `com.atproto.repo.uploadBlob`, for `publish`'s optional avatar
    /// upload (slice 3.0). Never proxied ([`wants_proxy`]: the nsid does
    /// not start with `app.bsky.`). Returns the HTTP status and the decoded
    /// JSON body rather than a typed blob, so the caller's own shape check
    /// (`publish::validate_blob`) sees the same two values the old direct
    /// `reqwest` call gave it.
    pub async fn upload_blob(
        &self,
        bytes: Vec<u8>,
        content_type: &'static str,
    ) -> Result<(u16, Value), PdsError> {
        let attempted = self
            .call_raw(
                HttpMethod::Post,
                "com.atproto.repo.uploadBlob",
                &[],
                RequestBody::Bytes { bytes, content_type },
            )
            .await?;
        Ok((attempted.status, attempted.body))
    }

    /// `com.atproto.repo.putRecord`, for `publish`'s record write (slice
    /// 3.0). The response body is discarded; only success or failure
    /// matters to a caller writing a record it never reads back.
    pub async fn put_record(&self, body: Value) -> Result<(), PdsError> {
        self.call_raw(HttpMethod::Post, "com.atproto.repo.putRecord", &[], RequestBody::Json(body))
            .await?;
        Ok(())
    }

    /// `app.bsky.graph.getFollows`, one page (BC9). `sort=latest` is
    /// always sent, matching TECH-DESIGN-network-feed §6.2's requirement
    /// that a fresh follow reaches the front of the list. `cursor` is
    /// omitted from the query when `None`, the first-page case. Returns
    /// the subject DIDs in response order and the next cursor, `None` on
    /// the last page (BC9a); a caller loops on that cursor to page to the
    /// end, story 03's probe and story 06's graph worker.
    pub async fn get_follows(
        &self,
        actor: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<FollowsPage, PdsError> {
        let limit = limit.to_string();
        let mut query: Vec<(&str, &str)> =
            vec![("actor", actor), ("limit", &limit), ("sort", "latest")];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor));
        }
        let response: GetFollowsResponse = self
            .call(HttpMethod::Get, "app.bsky.graph.getFollows", &query, RequestBody::None)
            .await?;
        Ok(FollowsPage {
            dids: response.follows.into_iter().map(|subject| subject.did).collect(),
            cursor: response.cursor,
        })
    }

    /// `app.bsky.graph.getRelationships` (BC10, BC10a, BC11). Refuses more
    /// than [`RELATIONSHIPS_MAX`] DIDs before any network call
    /// (`PdsError::TooMany`), and makes no call at all for an empty
    /// `others`. Each `other` is sent as its own `others` query parameter,
    /// the same repeated-parameter shape [`crate::appview::AppViewClient`]
    /// uses for `uris` and `actors`. Returns the DIDs whose relationship
    /// entry carries a non-null string `followedBy` field, in response
    /// order (review round 1, finding 1: a `followedBy` key present with a
    /// JSON `null` value, the shape the AT Protocol sends for a
    /// relationship it does not follow, no longer counts as followed).
    /// `notFoundActor` entries and entries without a string `followedBy`
    /// are skipped, never an error.
    pub async fn get_relationships(
        &self,
        actor: &str,
        others: &[String],
    ) -> Result<Vec<String>, PdsError> {
        if others.len() > RELATIONSHIPS_MAX {
            return Err(PdsError::TooMany { count: others.len() });
        }
        if others.is_empty() {
            return Ok(Vec::new());
        }
        let mut query: Vec<(&str, &str)> = vec![("actor", actor)];
        query.extend(others.iter().map(|other| ("others", other.as_str())));
        let response: GetRelationshipsResponse = self
            .call(HttpMethod::Get, "app.bsky.graph.getRelationships", &query, RequestBody::None)
            .await?;
        Ok(response
            .relationships
            .into_iter()
            .filter_map(|relationship| {
                let did = relationship.get("did")?.as_str()?.to_string();
                relationship.get("followedBy")?.as_str()?;
                Some(did)
            })
            .collect())
    }
}

impl PdsClient<HttpPdsTransport> {
    /// Builds the real client from `cfg.pds_url` and `cfg.graph_rps`
    /// (`UPSTAGE_PDS_URL`, `UPSTAGE_GRAPH_RPS`). `publish` (slice 3.0) and
    /// the graph methods' first caller (slice 2.0, story 03) both build
    /// their client this way.
    pub fn from_config(cfg: &Config, credentials: Credentials) -> Result<Self, PdsError> {
        Self::new(HttpPdsTransport::new(cfg.pds_url.clone()), credentials, cfg.graph_rps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex as StdMutex};

    fn creds() -> Credentials {
        Credentials {
            handle: "upstage.bsky.social".to_string(),
            app_password: Secret::new("app-pass".to_string()),
        }
    }

    fn now_unix() -> i64 {
        Utc::now().timestamp()
    }

    fn far_future_exp() -> i64 {
        now_unix() + 3600
    }

    /// A minimal, unsigned JWT carrying only the `exp` claim a real PDS
    /// token would have. The signature segment is never checked (BC2a's
    /// "Defaults taken").
    fn access_token(exp: i64) -> String {
        let header = URL_SAFE_NO_PAD.encode(b"{}");
        let payload = URL_SAFE_NO_PAD.encode(format!("{{\"exp\":{exp}}}"));
        format!("{header}.{payload}.sig")
    }

    fn session_body(did: &str, access_jwt: &str, refresh_jwt: &str) -> Value {
        json!({ "did": did, "accessJwt": access_jwt, "refreshJwt": refresh_jwt })
    }

    #[derive(Debug, Clone)]
    struct RecordedCall {
        nsid: &'static str,
        bearer: Option<String>,
        proxy: bool,
        query: Vec<(String, String)>,
    }

    /// A recording fake `PdsTransport`: `push_ok`/`push_transport_error`
    /// queue canned responses in call order, and `calls()` returns every
    /// call made so far. `Arc`-backed so a clone kept by the test can
    /// inspect it after the original is moved into a `PdsClient`.
    #[derive(Debug, Clone, Default)]
    struct FakeTransport {
        responses: Arc<StdMutex<VecDeque<Result<RawResponse, ()>>>>,
        calls: Arc<StdMutex<Vec<RecordedCall>>>,
    }

    impl FakeTransport {
        fn new() -> Self {
            Self::default()
        }

        fn push_ok(&self, status: u16, body: Value) {
            self.responses.lock().expect("lock").push_back(Ok(RawResponse { status, body }));
        }

        fn push_transport_error(&self) {
            self.responses.lock().expect("lock").push_back(Err(()));
        }

        fn calls(&self) -> Vec<RecordedCall> {
            self.calls.lock().expect("lock").clone()
        }

        fn nsids(&self) -> Vec<&'static str> {
            self.calls().iter().map(|call| call.nsid).collect()
        }
    }

    impl PdsTransport for FakeTransport {
        async fn request(
            &self,
            _http_method: HttpMethod,
            nsid: &'static str,
            bearer: Option<&str>,
            proxy: bool,
            query: &[(&str, &str)],
            _body: RequestBody,
        ) -> Result<RawResponse, TransportError> {
            self.calls.lock().expect("lock").push(RecordedCall {
                nsid,
                bearer: bearer.map(str::to_string),
                proxy,
                query: query.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            });
            match self.responses.lock().expect("lock").pop_front() {
                Some(Ok(response)) => Ok(response),
                Some(Err(())) => Err(TransportError),
                None => {
                    Ok(RawResponse { status: 500, body: json!({ "error": "no fixture queued" }) })
                }
            }
        }
    }

    #[test]
    fn http_transport_trims_trailing_slash() {
        // Review round 1, finding 3: a base URL with a trailing slash
        // builds `<base>/xrpc/<nsid>` with exactly one slash.
        let transport = HttpPdsTransport::new("https://bsky.social/".to_string());
        assert_eq!(transport.base_url, "https://bsky.social");

        let transport = HttpPdsTransport::new("https://bsky.social///".to_string());
        assert_eq!(transport.base_url, "https://bsky.social");

        let transport = HttpPdsTransport::new("https://bsky.social".to_string());
        assert_eq!(transport.base_url, "https://bsky.social");
    }

    #[test]
    fn wants_proxy_by_nsid_prefix() {
        // BC5, BC6.
        assert!(wants_proxy("app.bsky.graph.getFollows"));
        assert!(wants_proxy("app.bsky.actor.getProfile"));
        assert!(!wants_proxy("com.atproto.repo.putRecord"));
        assert!(!wants_proxy("com.atproto.server.createSession"));
    }

    #[test]
    fn zero_rate_is_rejected() {
        let err = PdsClient::new(FakeTransport::new(), creds(), 0.0).unwrap_err();
        assert!(matches!(err, PdsError::InvalidRate));
    }

    #[test]
    fn negative_rate_is_rejected() {
        let err = PdsClient::new(FakeTransport::new(), creds(), -1.0).unwrap_err();
        assert!(matches!(err, PdsError::InvalidRate));
    }

    #[test]
    fn nonfinite_rate_is_rejected() {
        let err = PdsClient::new(FakeTransport::new(), creds(), f64::NAN).unwrap_err();
        assert!(matches!(err, PdsError::InvalidRate));
    }

    #[test]
    fn vanishingly_small_rate_is_rejected_without_panic() {
        // Review round 1, finding 2: a rate whose reciprocal period is
        // wildly impractical (here, roughly 31.7 million years) is
        // `PdsError::InvalidRate`, not a panic in `Duration` construction.
        let err = PdsClient::new(FakeTransport::new(), creds(), 1e-15).unwrap_err();
        assert!(matches!(err, PdsError::InvalidRate));
    }

    #[tokio::test]
    async fn refresh_triggers() {
        // AC1, BC1, BC2: a fresh login makes no extra call, but once the
        // stored session looks like it is within five minutes of `exp`,
        // the next call refreshes first.
        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        transport.push_ok(200, json!({ "ok": true }));
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-2"),
        );
        transport.push_ok(200, json!({ "ok": true }));

        let client =
            PdsClient::new(transport.clone(), creds(), 1000.0).expect("valid rate builds a client");

        let _: Value = client
            .call(HttpMethod::Get, "com.atproto.test.opA", &[], RequestBody::None)
            .await
            .expect("first call succeeds");
        assert_eq!(transport.nsids(), vec![LOGIN_NSID, "com.atproto.test.opA"]);

        // Simulate five minutes having nearly elapsed, without waiting on
        // real or virtual time.
        {
            let mut guard = client.session.lock().await;
            guard.as_mut().expect("session set by the first call").exp = now_unix() + 60;
        }

        let _: Value = client
            .call(HttpMethod::Get, "com.atproto.test.opB", &[], RequestBody::None)
            .await
            .expect("second call succeeds");
        assert_eq!(transport.nsids()[2..], [REFRESH_NSID, "com.atproto.test.opB"]);
    }

    #[tokio::test]
    async fn relogin_once_then_fail() {
        // AC2, BC4: a refresh that fails logs in again exactly once; if
        // that also fails, the call returns `PdsError::Session`.
        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        transport.push_ok(200, json!({ "ok": true }));
        transport.push_ok(401, json!({ "error": "InvalidToken" }));
        transport.push_ok(401, json!({ "error": "AuthenticationRequired" }));

        let client =
            PdsClient::new(transport.clone(), creds(), 1000.0).expect("valid rate builds a client");

        let _: Value = client
            .call(HttpMethod::Get, "com.atproto.test.opA", &[], RequestBody::None)
            .await
            .expect("first call succeeds");

        {
            let mut guard = client.session.lock().await;
            guard.as_mut().expect("session set by the first call").exp = now_unix() + 1;
        }

        let err = client
            .call::<Value>(HttpMethod::Get, "com.atproto.test.opB", &[], RequestBody::None)
            .await
            .unwrap_err();
        assert!(matches!(err, PdsError::Session));

        assert_eq!(
            transport.nsids(),
            vec![LOGIN_NSID, "com.atproto.test.opA", REFRESH_NSID, LOGIN_NSID],
            "refresh fails, then exactly one relogin attempt, then the call stops",
        );
    }

    #[tokio::test]
    async fn proxy_header() {
        // AC3, BC5, BC6: an app.bsky.* call is sent with proxy = true, a
        // com.atproto.* call with proxy = false.
        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        transport.push_ok(200, json!({ "ok": true }));
        transport.push_ok(200, json!({ "ok": true }));

        let client =
            PdsClient::new(transport.clone(), creds(), 1000.0).expect("valid rate builds a client");
        let _: Value = client
            .call(HttpMethod::Get, "app.bsky.graph.getFollows", &[], RequestBody::None)
            .await
            .expect("app view call succeeds");
        let _: Value = client
            .call(HttpMethod::Post, "com.atproto.repo.putRecord", &[], RequestBody::None)
            .await
            .expect("repo call succeeds");

        let calls = transport.calls();
        let app_view_call = calls
            .iter()
            .find(|call| call.nsid == "app.bsky.graph.getFollows")
            .expect("call recorded");
        assert!(app_view_call.proxy);
        assert!(app_view_call.bearer.is_some());
        let repo_call = calls
            .iter()
            .find(|call| call.nsid == "com.atproto.repo.putRecord")
            .expect("call recorded");
        assert!(!repo_call.proxy);
    }

    #[tokio::test]
    async fn retries_on_5xx_and_transport_error_then_succeeds() {
        // BC7, BC8: a 5xx and a transport error each retry, and the call
        // still succeeds once a good response is queued.
        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        transport.push_ok(503, json!({ "error": "Upstream" }));
        transport.push_transport_error();
        transport.push_ok(200, json!({ "ok": true }));

        let client =
            PdsClient::new(transport.clone(), creds(), 1000.0).expect("valid rate builds a client");
        let value: Value = client
            .call(HttpMethod::Get, "com.atproto.test.opA", &[], RequestBody::None)
            .await
            .expect("retries then succeeds");
        assert_eq!(value, json!({ "ok": true }));
        assert_eq!(
            transport.nsids(),
            vec![
                LOGIN_NSID,
                "com.atproto.test.opA",
                "com.atproto.test.opA",
                "com.atproto.test.opA"
            ]
        );
    }

    #[tokio::test]
    async fn retry_exhausted_returns_http_error() {
        // BC8: four straight 5xx responses exhaust the schedule (1s, 2s,
        // 4s, then fail) and become `PdsError::Http`.
        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        for _ in 0..4 {
            transport.push_ok(500, json!({ "error": "boom" }));
        }

        let client =
            PdsClient::new(transport, creds(), 1000.0).expect("valid rate builds a client");
        let err = client
            .call::<Value>(HttpMethod::Get, "com.atproto.test.opA", &[], RequestBody::None)
            .await
            .unwrap_err();
        match err {
            PdsError::Http { method, status, attempts } => {
                assert_eq!(method, "com.atproto.test.opA");
                assert_eq!(status, Some(500));
                assert_eq!(attempts, 4);
            }
            other => panic!("expected Http, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn expired_token_refreshes_and_retries_once() {
        // BC3: an ExpiredToken response triggers one refresh and one retry
        // of the same call; a second ExpiredToken becomes `PdsError::Session`.
        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        transport.push_ok(400, json!({ "error": "ExpiredToken" }));
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-2"),
        );
        transport.push_ok(200, json!({ "ok": true }));

        let client =
            PdsClient::new(transport.clone(), creds(), 1000.0).expect("valid rate builds a client");
        let value: Value = client
            .call(HttpMethod::Get, "com.atproto.test.opA", &[], RequestBody::None)
            .await
            .expect("refreshes once and retries the call");
        assert_eq!(value, json!({ "ok": true }));
        assert_eq!(
            transport.nsids(),
            vec![LOGIN_NSID, "com.atproto.test.opA", REFRESH_NSID, "com.atproto.test.opA"]
        );
    }

    #[tokio::test]
    async fn a_second_expired_token_is_a_session_error() {
        // BC3: the retried call also answers ExpiredToken.
        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        transport.push_ok(400, json!({ "error": "ExpiredToken" }));
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-2"),
        );
        transport.push_ok(400, json!({ "error": "ExpiredToken" }));

        let client =
            PdsClient::new(transport, creds(), 1000.0).expect("valid rate builds a client");
        let err = client
            .call::<Value>(HttpMethod::Get, "com.atproto.test.opA", &[], RequestBody::None)
            .await
            .unwrap_err();
        assert!(matches!(err, PdsError::Session));
    }

    #[tokio::test]
    async fn debug_redacts_secrets() {
        // AC9, BC12: no Debug output anywhere in this module ever shows
        // the app password or a token.
        let credentials = creds();
        let printed = format!("{credentials:?}");
        assert!(!printed.contains("app-pass"));
        assert!(printed.contains("[redacted]"));

        let session = Session {
            did: "did:plc:actor".to_string(),
            access_jwt: Secret::new("access-secret".to_string()),
            refresh_jwt: Secret::new("refresh-secret".to_string()),
            exp: 0,
        };
        let printed = format!("{session:?}");
        assert!(!printed.contains("access-secret"));
        assert!(!printed.contains("refresh-secret"));
        assert!(printed.contains("[redacted]"));

        for err in [
            PdsError::InvalidRate,
            PdsError::Session,
            PdsError::Http { method: "op", status: Some(500), attempts: 3 },
            PdsError::Decode { method: "op", reason: "bad json".to_string() },
            PdsError::TooMany { count: 40 },
        ] {
            assert!(!format!("{err:?}").contains("app-pass"));
        }

        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        let client =
            PdsClient::new(transport, creds(), 1000.0).expect("valid rate builds a client");
        assert!(!format!("{client:?}").contains("app-pass"));
    }

    #[tokio::test]
    async fn get_follows_pages() {
        // AC4, BC9, BC9a: a caller loop follows the cursor to the end,
        // keeping the subject DIDs in response order across pages, and
        // stops once a page carries no cursor.
        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        transport.push_ok(
            200,
            json!({
                "follows": [{"did": "did:plc:one"}, {"did": "did:plc:two"}],
                "cursor": "page-2",
            }),
        );
        transport.push_ok(200, json!({ "follows": [{"did": "did:plc:three"}] }));

        let client =
            PdsClient::new(transport.clone(), creds(), 1000.0).expect("valid rate builds a client");

        let mut dids = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = client
                .get_follows("did:plc:viewer", 100, cursor.as_deref())
                .await
                .expect("get_follows succeeds");
            dids.extend(page.dids);
            match page.cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(
            dids,
            vec!["did:plc:one".to_string(), "did:plc:two".to_string(), "did:plc:three".to_string()]
        );

        let calls = transport.calls();
        let first_page = calls
            .iter()
            .find(|call| {
                call.nsid == "app.bsky.graph.getFollows"
                    && call.query.iter().all(|(k, _)| k != "cursor")
            })
            .expect("first page has no cursor param");
        assert!(first_page.proxy, "app.bsky.* carries the proxy header (BC5)");
        assert!(first_page.query.contains(&("actor".to_string(), "did:plc:viewer".to_string())));
        assert!(first_page.query.contains(&("limit".to_string(), "100".to_string())));
        assert!(first_page.query.contains(&("sort".to_string(), "latest".to_string())));

        let second_page = calls
            .iter()
            .find(|call| {
                call.nsid == "app.bsky.graph.getFollows"
                    && call.query.iter().any(|(k, _)| k == "cursor")
            })
            .expect("second page carries the cursor from the first page");
        assert!(second_page.query.contains(&("cursor".to_string(), "page-2".to_string())));
    }

    #[tokio::test]
    async fn get_relationships() {
        // AC5, BC10, BC10a, BC11: more than 30 DIDs is refused before any
        // network call, an empty list makes no call either, and the
        // result keeps only the DIDs whose relationship has a non-null
        // string `followedBy` (review round 1, finding 1: a `null` value
        // is not followed).
        let transport = FakeTransport::new();
        transport.push_ok(
            200,
            session_body("did:plc:actor", &access_token(far_future_exp()), "refresh-1"),
        );
        transport.push_ok(
            200,
            json!({
                "relationships": [
                    {"did": "did:plc:a", "followedBy": "at://did:plc:a/app.bsky.graph.follow/1"},
                    {"did": "did:plc:b"},
                    {"did": "did:plc:d", "followedBy": null},
                    {"$type": "app.bsky.graph.defs#notFoundActor", "actor": "did:plc:c"},
                ],
            }),
        );

        let client =
            PdsClient::new(transport.clone(), creds(), 1000.0).expect("valid rate builds a client");

        let too_many: Vec<String> = (0..31).map(|i| format!("did:plc:{i}")).collect();
        let err = client.get_relationships("did:plc:viewer", &too_many).await.unwrap_err();
        assert!(matches!(err, PdsError::TooMany { count: 31 }));
        assert!(transport.calls().is_empty(), "TooMany makes no network call");

        let empty = client
            .get_relationships("did:plc:viewer", &[])
            .await
            .expect("empty others succeeds with no call");
        assert!(empty.is_empty());
        assert!(transport.calls().is_empty(), "empty others makes no network call");

        let others = vec![
            "did:plc:a".to_string(),
            "did:plc:b".to_string(),
            "did:plc:c".to_string(),
            "did:plc:d".to_string(),
        ];
        let follows_me = client
            .get_relationships("did:plc:viewer", &others)
            .await
            .expect("get_relationships succeeds");
        assert_eq!(follows_me, vec!["did:plc:a".to_string()]);

        let call = transport
            .calls()
            .into_iter()
            .find(|call| call.nsid == "app.bsky.graph.getRelationships")
            .expect("call recorded");
        assert!(call.proxy, "app.bsky.* carries the proxy header (BC5)");
        assert!(call.query.contains(&("actor".to_string(), "did:plc:viewer".to_string())));
        for other in &others {
            assert!(call.query.contains(&("others".to_string(), other.clone())));
        }
    }
}

/// Live tests against the real PDS. `#[ignore]`d, so `cargo test
/// --all-features` never touches the network; run by hand with
/// `BSKY_HANDLE` and `BSKY_APP_PASSWORD` set:
/// `cargo test -- --ignored pds_refresh_live` (AC8).
#[cfg(test)]
mod live_tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn pds_refresh_live() {
        let lookup = |name: &str| std::env::var(name).ok();
        let cfg = crate::config::load(lookup).expect("real environment provides a valid config");
        let credentials = Credentials {
            handle: cfg.bsky_handle.clone().expect("BSKY_HANDLE set for the live test"),
            app_password: cfg
                .bsky_app_password
                .clone()
                .expect("BSKY_APP_PASSWORD set for the live test"),
        };
        let client = PdsClient::from_config(&cfg, credentials)
            .expect("a positive graph_rps builds a client");

        let _: Value = client
            .call(HttpMethod::Get, "com.atproto.server.getSession", &[], RequestBody::None)
            .await
            .expect("initial call logs in");

        // Force the stored session to look expired, so the next call must
        // refresh before it can succeed (BC2).
        {
            let mut guard = client.session.lock().await;
            guard.as_mut().expect("session set by the first call").exp = 0;
        }

        let _: Value = client
            .call(HttpMethod::Get, "com.atproto.server.getSession", &[], RequestBody::None)
            .await
            .expect("live refresh round trip");
    }
}
