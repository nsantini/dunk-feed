//! `POST /xrpc/app.bsky.feed.sendInteractions` (BC12, BC13, BC28, BC34,
//! BC35): appends one row per interaction event to the `interactions` table
//! through `WriterHandle::try_send`, so a stalled writer never blocks this
//! request; a full channel drops the events instead of waiting (BC28).

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::{Deserialize, Serialize};

use crate::http::AppState;
use crate::store::writer::Op;

/// The request body's one field, `interactions`. Its absence, or malformed
/// JSON, is a 400 (BC13): `handler` parses the raw body itself, rather than
/// through axum's `Json<T>` extractor, because that extractor's own
/// rejection is a 422, not the 400 BC13 asks for.
#[derive(Debug, Deserialize)]
pub struct SendInteractionsRequest {
    pub interactions: Vec<InteractionBody>,
}

/// One `app.bsky.feed.defs#interaction`. Every field is optional per the
/// lexicon; BC35 requires that an object with every field absent still
/// writes one row, with four `NULL` payload columns.
#[derive(Debug, Deserialize)]
pub struct InteractionBody {
    pub item: Option<String>,
    pub event: Option<String>,
    #[serde(rename = "feedContext")]
    pub feed_context: Option<String>,
    #[serde(rename = "reqId")]
    pub req_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SendInteractionsResponse {}

/// BC12: one `Op::Interaction` per event, sent through
/// `WriterHandle::try_send`. A full channel (`StoreError::WriterFull`) or a
/// dead writer (`StoreError::WriterGone`) both drop the event rather than
/// failing the request (BC28); the response is always 200 `{}` once the
/// body itself parsed. `interactions` present and empty (BC34) touches this
/// loop zero times and still returns 200 `{}`.
///
/// No non-test caller yet: registered on `router` (`src/http/mod.rs`),
/// itself uncalled until `run` (`src/ingest/mod.rs`) starts the HTTP task.
pub async fn handler(State(state): State<Arc<AppState>>, bytes: Bytes) -> Response {
    let body: SendInteractionsRequest = match serde_json::from_slice(&bytes) {
        Ok(body) => body,
        // BC13: not valid JSON, or missing the required `interactions`
        // field. No row is written for a body that fails here.
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "InvalidRequest" })),
            )
                .into_response()
        }
    };

    let mut dropped = 0usize;
    for interaction in body.interactions {
        let op = Op::Interaction {
            item: interaction.item,
            event: interaction.event,
            feed_context: interaction.feed_context,
            req_id: interaction.req_id,
        };
        // BC28: both `WriterFull` (the channel is at capacity) and
        // `WriterGone` (the writer thread has exited) drop the event and
        // count it; neither ever fails the request.
        if state.writer.try_send(op).is_err() {
            dropped += 1;
        }
    }
    if dropped > 0 {
        tracing::warn!(dropped, "sendInteractions: writer channel full, dropped events");
    }
    (StatusCode::OK, Json(SendInteractionsResponse {})).into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;

    use crate::http::router;
    use crate::http::tests::{test_config, test_state};

    fn post_request(body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/xrpc/app.bsky.feed.sendInteractions")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    // BC12: one interaction returns 200 `{}`. The row itself landing in
    // `interactions` through `Op::Interaction` is proven at the store layer
    // by `src/store/writer.rs`'s `batch_of_only_interactions_does_not_move_the_cursor`
    // and `src/store/interactions.rs`'s own insert tests; this test covers
    // the HTTP contract `try_send` wires up to them.
    #[tokio::test]
    async fn one_interaction_returns_empty_object() {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        let app = router(state);

        let body = serde_json::json!({
            "interactions": [
                {
                    "item": "at://did:plc:q/app.bsky.feed.post/q1",
                    "event": "app.bsky.feed.defs#requestLess",
                    "feedContext": "r=4.5",
                }
            ]
        });
        let response = app.oneshot(post_request(body)).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json, serde_json::json!({}));
    }

    // BC34: `interactions` present and empty is still 200 `{}`.
    #[tokio::test]
    async fn empty_interactions_returns_empty_object() {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        let app = router(state);

        let response =
            app.oneshot(post_request(serde_json::json!({"interactions": []}))).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json, serde_json::json!({}));
    }

    // BC35: an interaction with every field absent is still accepted (the
    // row it writes, with four NULL payload columns, is proven at the store
    // layer by `src/store/interactions.rs::insert_interaction_allows_null_payload_columns`).
    #[tokio::test]
    async fn interaction_with_every_field_absent_is_accepted() {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        let app = router(state);

        let response =
            app.oneshot(post_request(serde_json::json!({"interactions": [{}]}))).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    // BC13: malformed JSON is a 400, before the handler body ever runs.
    #[tokio::test]
    async fn malformed_json_is_bad_request() {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        let app = router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/xrpc/app.bsky.feed.sendInteractions")
            .header("content-type", "application/json")
            .body(Body::from("not json"))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // BC13: a body missing the required `interactions` field is a 400.
    #[tokio::test]
    async fn missing_interactions_field_is_bad_request() {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        let app = router(state);

        let response = app.oneshot(post_request(serde_json::json!({}))).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // Slice 4.0: `reqId` reaches `Op::Interaction` and lands in the written
    // row's `req_id` column. A file-backed store, not `test_state`'s
    // in-memory one, so the row can be read back after `flush` through a
    // second connection on the same path.
    #[tokio::test]
    async fn req_id_reaches_the_written_row() {
        let nanos =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("dunk-interactions-reqid-{nanos}.sqlite3"));
        let path_str = path.to_str().unwrap().to_string();

        let cfg = test_config("127.0.0.1:0");
        let store = crate::store::Store::open_path(&path_str).expect("file store must open");
        let writer = store.writer().expect("writer thread must start");
        let state = Arc::new(crate::http::AppState {
            snapshot: crate::scorer::snapshot::SnapshotHandle::new(),
            writer: writer.clone(),
            health: crate::health::HealthState::new(),
            cfg,
        });
        let app = router(state);

        let body = serde_json::json!({
            "interactions": [
                {
                    "item": "at://did:plc:q/app.bsky.feed.post/q1",
                    "event": "app.bsky.feed.defs#requestLess",
                    "feedContext": "r=4.5",
                    "reqId": "req-42",
                }
            ]
        });
        let response = app.oneshot(post_request(body)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        writer.flush().await.unwrap();

        let conn = rusqlite::Connection::open(&path_str).expect("reopen must succeed");
        let req_id: Option<String> = conn
            .query_row("SELECT req_id FROM interactions", [], |row| row.get(0))
            .expect("one row must exist");
        assert_eq!(req_id, Some("req-42".to_string()));

        drop(conn);
        let _ = std::fs::remove_file(&path_str);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }

    // Slice 4.0 / BC35: an interaction with every field absent, including
    // `reqId`, still writes exactly one row (the four NULL payload columns
    // are proven at the store layer by
    // `src/store/interactions.rs::insert_interaction_allows_null_payload_columns`;
    // this test covers the HTTP contract that a `reqId`-carrying
    // `InteractionBody` did not narrow that acceptance).
    #[tokio::test]
    async fn all_fields_absent_including_req_id_still_writes_one_row() {
        let nanos =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path =
            std::env::temp_dir().join(format!("dunk-interactions-reqid-absent-{nanos}.sqlite3"));
        let path_str = path.to_str().unwrap().to_string();

        let cfg = test_config("127.0.0.1:0");
        let store = crate::store::Store::open_path(&path_str).expect("file store must open");
        let writer = store.writer().expect("writer thread must start");
        let state = Arc::new(crate::http::AppState {
            snapshot: crate::scorer::snapshot::SnapshotHandle::new(),
            writer: writer.clone(),
            health: crate::health::HealthState::new(),
            cfg,
        });
        let app = router(state);

        let response =
            app.oneshot(post_request(serde_json::json!({"interactions": [{}]}))).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        writer.flush().await.unwrap();

        let conn = rusqlite::Connection::open(&path_str).expect("reopen must succeed");
        let (count, item, event, feed_context, req_id): (
            i64,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = conn
            .query_row(
                "SELECT count(*), max(item), max(event), max(feed_context), max(req_id) FROM interactions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(item, None);
        assert_eq!(event, None);
        assert_eq!(feed_context, None);
        assert_eq!(req_id, None);

        drop(conn);
        let _ = std::fs::remove_file(&path_str);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }
}
