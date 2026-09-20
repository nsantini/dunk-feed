//! Shared liveness state for `/healthz`, story 08. Two `Arc<AtomicI64>`
//! unix-second timestamps: the last Jetstream commit the ingest loop saw,
//! and the last scorer pass that finished successfully. A top-level module,
//! not `src/http/health.rs`, because `ingest` and `scorer` both write it and
//! `http` only reads it — putting it under `src/http/` would make the
//! scorer depend on the HTTP module, the wrong direction.
//!
//! Both atomics start seeded with the process start time (BC25), so
//! `/healthz` reports a lag that grows from zero at startup and crosses the
//! `DUNK_HEALTH_MAX_LAG_S` threshold into 503 if neither task ever records a
//! pass, matching TECH-DESIGN section 13's "all hosts down" line.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use crate::store;

/// The two liveness clocks `/healthz` reads. Cloning `HealthState` clones
/// the `Arc`s, not the counters: every clone reads and writes the same
/// underlying atomics.
#[derive(Debug, Clone)]
pub struct HealthState {
    last_commit_s: Arc<AtomicI64>,
    last_scorer_pass_s: Arc<AtomicI64>,
}

/// No non-test caller yet in this slice: `src/http/mod.rs`'s `AppState`
/// holds one (constructed only in test helpers here), and the ingest loop
/// and `scorer::run` that call `set_commit_time`/`set_scorer_pass` in
/// production are wired up in slice 3.0.
impl HealthState {
    /// Seeds both atomics with the process start time (BC25). No commit and
    /// no scorer pass has happened yet, so both clocks start at "now" rather
    /// than at `0`, which would report an implausible decades-long lag.
    #[allow(dead_code)]
    pub fn new() -> Self {
        let start = store::unix_now();
        HealthState {
            last_commit_s: Arc::new(AtomicI64::new(start)),
            last_scorer_pass_s: Arc::new(AtomicI64::new(start)),
        }
    }

    /// Records `now` as the last Jetstream commit time. The ingest loop
    /// (`src/ingest/mod.rs`, slice 3.0) calls this where `Stats::record_commit`
    /// already tracks the same moment.
    #[allow(dead_code)]
    pub fn set_commit_time(&self, now: i64) {
        self.last_commit_s.store(now, Ordering::Relaxed);
    }

    /// Records `now` as the last successful scorer pass time. `scorer::run`
    /// (slice 3.0) calls this after each successful `one_pass`.
    #[allow(dead_code)]
    pub fn set_scorer_pass(&self, now: i64) {
        self.last_scorer_pass_s.store(now, Ordering::Relaxed);
    }

    /// Seconds since the last recorded Jetstream commit, clamped at `0` so a
    /// clock-skewed future timestamp never reports a negative lag (BC26).
    /// `src/http/health.rs`'s route handler is the first caller.
    #[allow(dead_code)]
    pub fn jetstream_lag_s(&self, now: i64) -> i64 {
        (now - self.last_commit_s.load(Ordering::Relaxed)).max(0)
    }

    /// Seconds since the last successful scorer pass, clamped at `0` (BC26).
    #[allow(dead_code)]
    pub fn last_pass_age_s(&self, now: i64) -> i64 {
        (now - self.last_scorer_pass_s.load(Ordering::Relaxed)).max(0)
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_seeds_both_clocks_with_the_process_start_time() {
        let before = store::unix_now();
        let state = HealthState::new();
        let after = store::unix_now();
        // Both ages are ~0 right after construction, whatever `now()`
        // landed on within this narrow window.
        assert!(state.jetstream_lag_s(after) <= after - before + 1);
        assert!(state.last_pass_age_s(after) <= after - before + 1);
    }

    #[test]
    fn set_commit_time_moves_the_jetstream_clock_only() {
        let state = HealthState::new();
        state.set_commit_time(1_000);
        assert_eq!(state.jetstream_lag_s(1_050), 50);
    }

    #[test]
    fn set_scorer_pass_moves_the_scorer_clock_only() {
        let state = HealthState::new();
        state.set_scorer_pass(2_000);
        assert_eq!(state.last_pass_age_s(2_075), 75);
    }

    #[test]
    fn lag_is_clamped_at_zero_on_clock_skew() {
        // BC26: a recorded time in the future must never yield a negative
        // lag.
        let state = HealthState::new();
        state.set_commit_time(5_000);
        state.set_scorer_pass(5_000);
        assert_eq!(state.jetstream_lag_s(4_000), 0);
        assert_eq!(state.last_pass_age_s(4_000), 0);
    }

    #[test]
    fn clone_shares_the_same_underlying_atomics() {
        let state = HealthState::new();
        let cloned = state.clone();
        cloned.set_commit_time(9_000);
        assert_eq!(state.jetstream_lag_s(9_010), 10);
    }
}
