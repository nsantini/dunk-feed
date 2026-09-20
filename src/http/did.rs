//! `GET /.well-known/did.json` (BC1): the `did:web` document a Bluesky
//! client resolves to find this feed generator's service endpoint.

use std::sync::Arc;

use axum::extract::State;
use axum::response::Json;
use serde::Serialize;

use crate::http::AppState;

#[derive(Debug, Serialize)]
pub struct DidDocument {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: String,
    pub service: Vec<Service>,
}

#[derive(Debug, Serialize)]
pub struct Service {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "serviceEndpoint")]
    pub service_endpoint: String,
}

/// Builds the document from `cfg.hostname`: `did:web:<host>` as the DID and
/// `https://<host>` as the service endpoint, per BC1.
///
/// No non-test caller yet: registered on `router` (`src/http/mod.rs`),
/// itself uncalled until `run` (slice 3.0) starts the HTTP task.
#[allow(dead_code)]
pub async fn handler(State(state): State<Arc<AppState>>) -> Json<DidDocument> {
    let host = &state.cfg.hostname;
    Json(DidDocument {
        context: vec!["https://www.w3.org/ns/did/v1".to_string()],
        id: format!("did:web:{host}"),
        service: vec![Service {
            id: "#bsky_fg".to_string(),
            kind: "BskyFeedGenerator".to_string(),
            service_endpoint: format!("https://{host}"),
        }],
    })
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::Value;
    use tower::ServiceExt;

    use crate::http::router;
    use crate::http::tests::{test_config, test_state};

    // BC1.
    #[tokio::test]
    async fn returns_the_did_document_shaped_by_bc1() {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        let app = router(state);

        let response = app
            .oneshot(Request::builder().uri("/.well-known/did.json").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["id"], "did:web:feed.example.com");
        assert_eq!(json["service"][0]["id"], "#bsky_fg");
        assert_eq!(json["service"][0]["type"], "BskyFeedGenerator");
        assert_eq!(json["service"][0]["serviceEndpoint"], "https://feed.example.com");
        assert!(json["@context"].is_array());
    }
}
