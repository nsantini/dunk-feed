//! `GET /healthz` (BC14, BC15, BC36): reports whether the service still
//! keeps up with Jetstream and with the scorer, so a container healthcheck
//! can restart it when it does not. Reads only `HealthState`'s two atomics
//! and the snapshot's length — no store read on the serving path.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use serde::Serialize;

use crate::config::Config;
use crate::health::HealthState;
use crate::http::AppState;
use crate::store;

#[derive(Debug, Serialize, PartialEq)]
pub struct HealthBody {
    pub jetstream_lag_s: i64,
    pub last_pass_age_s: i64,
    pub snapshot_len: usize,
}

/// Pure core of the route: given `now`, decides the body and status. Split
/// out from `handler` so the 300s boundary tests below can pin `now`
/// exactly instead of racing the wall clock (BC14, BC15).
fn body_and_status(
    health: &HealthState,
    cfg: &Config,
    snapshot_len: usize,
    now: i64,
) -> (StatusCode, HealthBody) {
    let jetstream_lag_s = health.jetstream_lag_s(now);
    let last_pass_age_s = health.last_pass_age_s(now);
    let max = i64::from(cfg.health_max_lag_s);
    let status = if jetstream_lag_s <= max && last_pass_age_s <= max {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, HealthBody { jetstream_lag_s, last_pass_age_s, snapshot_len })
}

/// `snapshot.current()` is called exactly once per request (BC36).
///
/// No non-test caller yet: registered on `router` (`src/http/mod.rs`),
/// itself uncalled until `run` (slice 3.0) starts the HTTP task.
#[allow(dead_code)]
pub async fn handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let now = store::unix_now();
    let snapshot_len = state.snapshot.current().len();
    let (status, body) = body_and_status(&state.health, &state.cfg, snapshot_len, now);
    (status, Json(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::Value;
    use tower::ServiceExt;

    use crate::http::router;
    use crate::http::tests::{test_config, test_state};

    // BC14, BC36: within both thresholds, 200 with the three fields.
    #[tokio::test]
    async fn returns_200_within_thresholds() {
        let cfg = test_config("127.0.0.1:0");
        let state = test_state(cfg);
        let app = router(state);

        let response = app
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["snapshot_len"], 0);
        assert!(json["jetstream_lag_s"].is_i64());
        assert!(json["last_pass_age_s"].is_i64());
    }

    // AC4, BC14, BC15: exactly at the threshold is still 200; one second
    // past is 503, for each of the two ages independently.
    #[test]
    fn flips_at_300s() {
        let cfg = test_config("127.0.0.1:0");
        assert_eq!(cfg.health_max_lag_s, 300);

        let health = HealthState::new();
        health.set_commit_time(0);
        health.set_scorer_pass(0);

        let (status_at_300, _) = body_and_status(&health, &cfg, 0, 300);
        assert_eq!(status_at_300, StatusCode::OK, "exactly 300s is still healthy");

        let (status_at_301_jetstream, _) = body_and_status(&health, &cfg, 0, 301);
        assert_eq!(
            status_at_301_jetstream,
            StatusCode::SERVICE_UNAVAILABLE,
            "301s past the last commit flips to 503"
        );

        // Isolate the scorer age: keep the commit fresh, only the scorer
        // pass crosses the threshold.
        let health = HealthState::new();
        health.set_commit_time(1_000_000);
        health.set_scorer_pass(0);
        let (status_at_300, _) = body_and_status(&health, &cfg, 1_000_000, 300);
        assert_eq!(status_at_300, StatusCode::OK);
        let (status_at_301_scorer, _) = body_and_status(&health, &cfg, 1_000_000, 301);
        assert_eq!(
            status_at_301_scorer,
            StatusCode::SERVICE_UNAVAILABLE,
            "301s past the last scorer pass flips to 503"
        );
    }

    // BC15: the body shape is identical whether the status is 200 or 503.
    #[test]
    fn body_shape_is_the_same_on_503() {
        let cfg = test_config("127.0.0.1:0");
        let health = HealthState::new();
        health.set_commit_time(0);
        health.set_scorer_pass(0);

        let (status, body) = body_and_status(&health, &cfg, 42, 1_000);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.jetstream_lag_s, 1_000);
        assert_eq!(body.last_pass_age_s, 1_000);
        assert_eq!(body.snapshot_len, 42);
    }
}
