//! `GET /xrpc/app.bsky.feed.describeFeedGenerator` (BC2): tells a client
//! which feed at-URIs this generator serves.

use std::sync::Arc;

use axum::extract::State;
use axum::response::Json;
use serde::Serialize;

use crate::http::AppState;

#[derive(Debug, Serialize)]
pub struct DescribeFeedGenerator {
    pub did: String,
    pub feeds: Vec<Feed>,
}

#[derive(Debug, Serialize)]
pub struct Feed {
    pub uri: String,
}

/// `did:web:<host>` and one feed, `at://<publisher_did>/app.bsky.feed.generator/<rkey>`,
/// per BC2. Both strings come straight off `state.cfg` (BC44): neither is
/// reformatted here.
pub async fn handler(State(state): State<Arc<AppState>>) -> Json<DescribeFeedGenerator> {
    let cfg = &state.cfg;
    Json(DescribeFeedGenerator {
        did: cfg.did_web.clone(),
        feeds: vec![Feed { uri: cfg.feed_uri.clone() }],
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

    // BC2.
    #[tokio::test]
    async fn returns_the_generator_description_shaped_by_bc2() {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/xrpc/app.bsky.feed.describeFeedGenerator")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["did"], "did:web:feed.example.com");
        assert_eq!(json["feeds"][0]["uri"], "at://did:plc:abc/app.bsky.feed.generator/dunks");
    }
}
