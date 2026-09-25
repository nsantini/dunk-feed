//! HTTP serving for the AT Protocol feed generator, story 08. Slice 1.0
//! declared `cursor`, the pure pagination helper. Slice 2.0 added the
//! router, `AppState`, `HttpError`, and the three routes with no serving-path
//! store dependency: `did`, `describe`, and `health`. This slice (3.0) adds
//! `skeleton` and `interactions`, and the `IntoResponse` impl for
//! `SkeletonError` (BC17).

pub mod cursor;
pub mod describe;
pub mod did;
pub mod health;
pub mod interactions;
pub mod skeleton;
pub mod viewer;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use thiserror::Error;
use tokio::sync::watch;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::config::Config;
use crate::health::HealthState;
use crate::scorer::snapshot::SnapshotHandle;
use crate::store::writer::WriterHandle;

/// Every field a route handler needs from `Config`, built once at startup
/// (BC44). Carries `feed_uri` and `did_web` precomputed by their single
/// producers, `Config::feed_uri` and `Config::did_web` (BC45), so no handler
/// re-derives either string with its own `format!`.
#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub hostname: String,
    /// No handler reads this directly (BC44's precomputed `feed_uri` is what
    /// `skeleton.rs` and `describe.rs` compare and serve); carried anyway so
    /// `HttpConfig` mirrors every field `feed_uri` was built from, matching
    /// BC44's own field list.
    #[allow(dead_code)]
    pub publisher_did: String,
    /// See `publisher_did`: kept for parity with BC44's field list even
    /// though no handler reads it directly once `feed_uri` is precomputed.
    #[allow(dead_code)]
    pub feed_rkey: String,
    pub feed_uri: String,
    pub did_web: String,
    pub health_max_lag_s: u32,
    /// `UPSTAGE_PERSONALISE` (network-feed story 05, BC1, BC2, BC12,
    /// BC13): `skeleton::handler` reads `Authorization` and calls
    /// `auth::verify` only when this is `true`.
    pub personalise: bool,
    /// The DID key cache, resolver sender and `AuthConfig` `auth::verify`
    /// needs, present only when `personalise` is `true`. `HttpConfig::from`
    /// always sets this to `None`; `run` (`src/ingest/mod.rs`) attaches the
    /// real handle afterwards, only when `cfg.personalise` is `true`, so
    /// that construction never runs a DID resolver task the switch is off
    /// for. Kept on `HttpConfig` rather than as a fifth field directly on
    /// `AppState`: every existing `AppState` construction outside this
    /// slice's files (`src/http/interactions.rs`'s two hand-built states)
    /// calls `HttpConfig::from`, so adding a field here, defaulted inside
    /// that one constructor, never touches those call sites.
    pub auth: Option<AuthHandle>,
}

impl HttpConfig {
    /// The one constructor: reads `cfg` once at startup. `run`
    /// (`src/ingest/mod.rs`) calls this to build the `AppState` the HTTP
    /// task serves from, then attaches `auth` itself when the switch is on.
    pub fn from(cfg: &Config) -> Self {
        HttpConfig {
            hostname: cfg.hostname.clone(),
            publisher_did: cfg.publisher_did.clone(),
            feed_rkey: cfg.feed_rkey.clone(),
            feed_uri: cfg.feed_uri(),
            did_web: cfg.did_web(),
            health_max_lag_s: cfg.health_max_lag_s,
            personalise: cfg.personalise,
            auth: None,
        }
    }
}

/// Everything `skeleton::handler` needs to call `auth::verify` when the
/// switch is `true`: the DID key cache, the resolver task's channel
/// sender, and the config `verify` reads (`crate::auth::AuthConfig`).
/// `run` (`src/ingest/mod.rs`) builds one and attaches it to
/// `HttpConfig::auth` only when `UPSTAGE_PERSONALISE` is `true`.
#[derive(Clone)]
pub struct AuthHandle {
    pub cache: std::sync::Arc<crate::auth::KeyCache>,
    pub resolver_tx: tokio::sync::mpsc::Sender<crate::auth::ResolveRequest>,
    pub cfg: crate::auth::AuthConfig,
}

/// A manual, content-free `Debug`: `KeyCache` and the `mpsc::Sender` carry
/// no data worth printing (and the cache's keys are exactly the sort of
/// thing `auth`'s own BC21 keeps out of a log line), so this never widens
/// what a `tracing::info!(?state)`-style line could leak.
impl std::fmt::Debug for AuthHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthHandle").finish_non_exhaustive()
    }
}

/// Every dependency a route handler needs. `run` (`src/ingest/mod.rs`)
/// builds one and calls `serve` with it.
pub struct AppState {
    pub snapshot: SnapshotHandle,
    pub writer: WriterHandle,
    pub health: HealthState,
    pub cfg: HttpConfig,
    /// The viewer graph index (network-feed story 06, slice 4.0):
    /// `skeleton::handler`'s personalised branch reads it to find a
    /// viewer's circle and to enqueue a first build (BC4). `None` when
    /// `cfg.personalise` is `false`, or when it is `true` but the graph
    /// subsystem never started (`ingest::start_graph_subsystem`, BC18) —
    /// either way, a verified viewer just gets the empty page (BC5, BC6a
    /// empty-page case).
    pub graph: Option<Arc<crate::graph::GraphHandle>>,
    /// The per-viewer feed list cache (`http::viewer`, slice 3.0):
    /// `skeleton::handler`'s personalised branch calls `list_for` on every
    /// request. `Arc`-shared, not owned outright, because `run`
    /// (`src/ingest/mod.rs`, slice 4.0) also clones it into the worker's
    /// drop-lists callback (`graph::queue::DropListsFn`), which calls
    /// `drop_viewer` once a circle changes (BC7) from a task that outlives
    /// any single request and never holds this `AppState` itself.
    pub viewer_lists: Arc<crate::http::viewer::ViewerLists>,
}

/// Every way `serve` can fail. `run` (`src/ingest/mod.rs`) wraps this as
/// `IngestError::Http`; `main.rs` prints it and `upstage run` exits non-zero
/// (BC23, BC43).
#[derive(Debug, Error)]
pub enum HttpError {
    /// `UPSTAGE_HTTP_ADDR` is already taken, or otherwise unbindable.
    #[error("failed to bind {addr}: {source}")]
    Bind { addr: String, source: std::io::Error },
    /// `axum::serve` itself failed after a successful bind (BC43). `Bind`
    /// keeps its own narrower meaning: the address could not be taken in
    /// the first place.
    #[error("http server on {addr} failed: {source}")]
    Serve { addr: String, source: std::io::Error },
}

/// BC17: `SkeletonError::UnknownFeed` and `SkeletonError::InvalidRequest`
/// (`src/http/skeleton.rs`) both map to a 400 with the shape BC3, BC4 and
/// BC6 name. `src/http/skeleton.rs`'s own `handler` still adds the
/// `Cache-Control` header (BC11, BC33) after this runs, since that applies
/// to every response, not only an error one.
impl IntoResponse for skeleton::SkeletonError {
    fn into_response(self) -> Response {
        let error = match self {
            skeleton::SkeletonError::UnknownFeed => "UnknownFeed",
            skeleton::SkeletonError::InvalidRequest => "InvalidRequest",
        };
        (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": error }))).into_response()
    }
}

/// The five-second request timeout every route sits behind, applied by
/// `TimeoutLayer` in `router` rather than a per-handler deadline, so every
/// route gets it for free and none can forget it.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Builds the router over `state`: the five routes BC1 to BC15 name.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/.well-known/did.json", get(did::handler))
        .route("/xrpc/app.bsky.feed.describeFeedGenerator", get(describe::handler))
        .route("/healthz", get(health::handler))
        .route("/xrpc/app.bsky.feed.getFeedSkeleton", get(skeleton::handler))
        .route("/xrpc/app.bsky.feed.sendInteractions", post(interactions::handler))
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, REQUEST_TIMEOUT))
        .with_state(state)
}

/// Binds `cfg.http_addr` and serves `state`'s router until `shutdown_rx`
/// flips to `true`, then finishes in-flight requests before returning
/// (`axum::serve`'s graceful shutdown). `HttpError::Bind` (BC23), never a
/// panic, when the address is already taken.
///
/// `run` (`src/ingest/mod.rs`) spawns this as one of the three supervised
/// tasks.
pub async fn serve(
    cfg: &Config,
    state: Arc<AppState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), HttpError> {
    let addr: String = cfg.http_addr.clone();
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|source| HttpError::Bind { addr: addr.clone(), source })?;

    let app = router(state);
    let local_addr: SocketAddr = listener
        .local_addr()
        .unwrap_or_else(|_| "0.0.0.0:0".parse().expect("a fallback socket address always parses"));
    tracing::info!(addr = %local_addr, "http server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            loop {
                if *shutdown_rx.borrow() {
                    return;
                }
                if shutdown_rx.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        // BC43: a failure here is not a bind failure; the address was
        // already taken successfully above.
        .map_err(|source| HttpError::Serve { addr, source })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::collections::HashMap;

    /// A minimal, valid `Config`, built through `config::load` over a fixed
    /// map so every field stays in sync with `config.rs`'s own defaults and
    /// validation, rather than a hand-built struct literal drifting out of
    /// date. `UPSTAGE_PERSONALISE` is set explicitly to `false` (story 11
    /// launch flips `config::load`'s own default to `true`): every caller
    /// of this helper predates that story and was written against the
    /// switch-off, global-feed behaviour, so this keeps them all green
    /// without touching each call site. A test that needs the switch on
    /// uses its own config, like `skeleton::tests::test_config_personalised`.
    pub(crate) fn test_config(http_addr: &str) -> Config {
        let mut pairs: HashMap<String, String> = HashMap::new();
        pairs.insert("UPSTAGE_HOSTNAME".to_string(), "feed.example.com".to_string());
        pairs.insert("UPSTAGE_PUBLISHER_DID".to_string(), "did:plc:abc".to_string());
        pairs.insert("UPSTAGE_HTTP_ADDR".to_string(), http_addr.to_string());
        pairs.insert("UPSTAGE_PERSONALISE".to_string(), "false".to_string());
        crate::config::load(move |name| pairs.get(name).cloned())
            .expect("test config must be valid")
    }

    /// An `AppState` over a fresh in-memory store's writer, so route tests
    /// never touch a file on disk. The `Store` is leaked into the returned
    /// `WriterHandle`'s clone of its channel sender, so it only needs to
    /// outlive the writer thread, not this function's caller.
    pub(crate) fn test_state(cfg: Config) -> Arc<AppState> {
        let store = crate::store::Store::open_memory().expect("in-memory store must open");
        let writer = store.writer().expect("writer thread must start");
        Arc::new(AppState {
            snapshot: SnapshotHandle::new(),
            writer,
            health: HealthState::new(),
            cfg: HttpConfig::from(&cfg),
            graph: None,
            viewer_lists: Arc::new(crate::http::viewer::ViewerLists::new(
                cfg.follows_me_depth as usize,
            )),
        })
    }

    /// Like [`test_state`], but with `auth` attached to `cfg`'s
    /// `HttpConfig` (`skeleton::tests::unknown_key_empty_page` and
    /// `personalised_headers`, AC7, AC8): `cfg.personalise` must already be
    /// `true` for `auth` to matter, matching how `run`
    /// (`src/ingest/mod.rs`) only ever attaches one under the same
    /// condition.
    pub(crate) fn test_state_with_auth(cfg: Config, auth: AuthHandle) -> Arc<AppState> {
        let store = crate::store::Store::open_memory().expect("in-memory store must open");
        let writer = store.writer().expect("writer thread must start");
        let mut http_cfg = HttpConfig::from(&cfg);
        http_cfg.auth = Some(auth);
        Arc::new(AppState {
            snapshot: SnapshotHandle::new(),
            writer,
            health: HealthState::new(),
            cfg: http_cfg,
            graph: None,
            viewer_lists: Arc::new(crate::http::viewer::ViewerLists::new(
                cfg.follows_me_depth as usize,
            )),
        })
    }

    /// Like [`test_state_with_auth`], but with `graph` attached
    /// (`skeleton::tests::first_open_empty_fast`, `foreign_cursor`,
    /// `circle_change_mid_scroll`, network-feed story 06, slice 4.0): a
    /// verified viewer's personalised requests are then served through
    /// this `GraphHandle` and a fresh `ViewerLists`, exactly the shape
    /// `run` (`src/ingest/mod.rs`) attaches when the graph subsystem
    /// starts (BC18, BC19).
    pub(crate) fn test_state_with_graph(
        cfg: Config,
        auth: AuthHandle,
        graph: Arc<crate::graph::GraphHandle>,
    ) -> Arc<AppState> {
        let store = crate::store::Store::open_memory().expect("in-memory store must open");
        let writer = store.writer().expect("writer thread must start");
        let mut http_cfg = HttpConfig::from(&cfg);
        http_cfg.auth = Some(auth);
        Arc::new(AppState {
            snapshot: SnapshotHandle::new(),
            writer,
            health: HealthState::new(),
            cfg: http_cfg,
            graph: Some(graph),
            viewer_lists: Arc::new(crate::http::viewer::ViewerLists::new(
                cfg.follows_me_depth as usize,
            )),
        })
    }

    // BC23: a taken port returns `HttpError::Bind`, not a panic.
    #[tokio::test]
    async fn serve_returns_bind_error_on_a_taken_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding an ephemeral port must succeed in a test sandbox");
        let taken_addr = listener.local_addr().unwrap().to_string();

        let cfg = test_config(&taken_addr);
        let state = test_state(cfg.clone());
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let err = serve(&cfg, state, shutdown_rx).await.unwrap_err();
        match err {
            HttpError::Bind { addr, .. } => assert_eq!(addr, taken_addr),
            other => panic!("expected Bind, got {other:?}"),
        }

        drop(listener);
    }

    // BC43: `Serve` is a distinct variant from `Bind`, carrying the same two
    // fields but a different meaning — a failure after the address was
    // already taken, not a failure to take it. `run` (`src/ingest/mod.rs`)
    // wraps either as `IngestError::Http` the same way; this proves the two
    // stay tellable apart at the type and `Display` level, since exercising
    // a real post-bind `axum::serve` failure needs an OS-level fault this
    // test sandbox cannot manufacture portably.
    #[test]
    fn serve_error_is_distinct_from_bind_error() {
        let bind = HttpError::Bind {
            addr: "127.0.0.1:0".to_string(),
            source: std::io::Error::new(std::io::ErrorKind::AddrInUse, "address in use"),
        };
        let serve_err = HttpError::Serve {
            addr: "127.0.0.1:0".to_string(),
            source: std::io::Error::other("boom"),
        };
        assert!(matches!(bind, HttpError::Bind { .. }));
        assert!(matches!(serve_err, HttpError::Serve { .. }));
        assert!(bind.to_string().contains("failed to bind"));
        assert!(serve_err.to_string().contains("http server on 127.0.0.1:0 failed"));
        assert_ne!(bind.to_string(), serve_err.to_string());
    }
}
