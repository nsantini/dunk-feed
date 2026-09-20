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

/// Every dependency a route handler needs. `run` (`src/ingest/mod.rs`)
/// builds one and calls `serve` with it.
pub struct AppState {
    pub snapshot: SnapshotHandle,
    pub writer: WriterHandle,
    pub health: HealthState,
    pub cfg: Config,
}

/// Every way `serve` can fail. `run` (`src/ingest/mod.rs`) wraps this as
/// `IngestError::Http`; `main.rs` prints it and `dunk run` exits non-zero
/// (BC23).
#[derive(Debug, Error)]
pub enum HttpError {
    /// `DUNK_HTTP_ADDR` is already taken, or otherwise unbindable.
    #[error("failed to bind {addr}: {source}")]
    Bind { addr: String, source: std::io::Error },
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
        .map_err(|source| HttpError::Bind { addr, source })?;

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
    /// date.
    pub(crate) fn test_config(http_addr: &str) -> Config {
        let mut pairs: HashMap<String, String> = HashMap::new();
        pairs.insert("DUNK_HOSTNAME".to_string(), "feed.example.com".to_string());
        pairs.insert("DUNK_PUBLISHER_DID".to_string(), "did:plc:abc".to_string());
        pairs.insert("DUNK_HTTP_ADDR".to_string(), http_addr.to_string());
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
            cfg,
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
        }

        drop(listener);
    }
}
