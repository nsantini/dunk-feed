//! Shared liveness state for `/healthz`, story 08. Two `Arc<AtomicI64>`
//! unix-second timestamps: the last Jetstream commit the ingest loop saw,
//! and the last scorer pass that finished successfully. A top-level module,
//! not `src/http/health.rs`, because `ingest` and `scorer` both write it and
//! `http` only reads it — putting it under `src/http/` would make the
//! scorer depend on the HTTP module, the wrong direction.
//!
//! Round 2 finding 3 (BC41): both atomics start holding an `i64::MIN`
//! "never observed" sentinel, not the process start time. `jetstream_lag_s`
//! and `last_pass_age_s` return `None` rather than an age measured from
//! startup until the ingest loop or the scorer records a real timestamp;
//! `src/http/health.rs` (BC42) treats an unobserved clock as unhealthy, so
//! `/healthz` still flips to 503 before the first commit or pass, matching
//! TECH-DESIGN section 13's "all hosts down" line without needing a
//! plausible-looking fake age to get there.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// No commit, or no scorer pass, has been observed yet on that clock (BC41).
const NEVER_OBSERVED: i64 = i64::MIN;

/// The two liveness clocks `/healthz` reads. Cloning `HealthState` clones
/// the `Arc`s, not the counters: every clone reads and writes the same
/// underlying atomics.
#[derive(Debug, Clone)]
pub struct HealthState {
    last_commit_s: Arc<AtomicI64>,
    last_scorer_pass_s: Arc<AtomicI64>,
}

impl HealthState {
    /// Seeds both atomics with [`NEVER_OBSERVED`] (BC41): no commit and no
    /// scorer pass has happened yet, so both clocks report no age at all
    /// rather than an implausible one measured from "now".
    pub fn new() -> Self {
        HealthState {
            last_commit_s: Arc::new(AtomicI64::new(NEVER_OBSERVED)),
            last_scorer_pass_s: Arc::new(AtomicI64::new(NEVER_OBSERVED)),
        }
    }

    /// Records `now` as the last Jetstream commit time. The ingest loop
    /// (`src/ingest/mod.rs`) calls this where `Stats::record_commit`
    /// already tracks the same moment.
    pub fn set_commit_time(&self, now: i64) {
        self.last_commit_s.store(now, Ordering::Relaxed);
    }

    /// Records `now` as the last successful scorer pass time. `scorer::run`
    /// (`src/scorer/mod.rs`) calls this after each successful `one_pass`.
    pub fn set_scorer_pass(&self, now: i64) {
        self.last_scorer_pass_s.store(now, Ordering::Relaxed);
    }

    /// Seconds since the last recorded Jetstream commit, clamped at `0` so a
    /// clock-skewed future timestamp never reports a negative lag (BC26);
    /// `None` (BC41) when no commit has ever been recorded.
    /// `src/http/health.rs`'s route handler is the caller.
    pub fn jetstream_lag_s(&self, now: i64) -> Option<i64> {
        match self.last_commit_s.load(Ordering::Relaxed) {
            NEVER_OBSERVED => None,
            last => Some((now - last).max(0)),
        }
    }

    /// Seconds since the last successful scorer pass, clamped at `0`
    /// (BC26); `None` (BC41) when no pass has ever succeeded.
    pub fn last_pass_age_s(&self, now: i64) -> Option<i64> {
        match self.last_scorer_pass_s.load(Ordering::Relaxed) {
            NEVER_OBSERVED => None,
            last => Some((now - last).max(0)),
        }
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

    // BC41: a fresh state has observed neither clock yet.
    #[test]
    fn new_state_has_observed_neither_clock() {
        let state = HealthState::new();
        assert_eq!(state.jetstream_lag_s(1_000), None);
        assert_eq!(state.last_pass_age_s(1_000), None);
    }

    #[test]
    fn set_commit_time_moves_the_jetstream_clock_only() {
        let state = HealthState::new();
        state.set_commit_time(1_000);
        assert_eq!(state.jetstream_lag_s(1_050), Some(50));
        assert_eq!(state.last_pass_age_s(1_050), None, "the scorer clock is untouched");
    }

    #[test]
    fn set_scorer_pass_moves_the_scorer_clock_only() {
        let state = HealthState::new();
        state.set_scorer_pass(2_000);
        assert_eq!(state.last_pass_age_s(2_075), Some(75));
        assert_eq!(state.jetstream_lag_s(2_075), None, "the jetstream clock is untouched");
    }

    #[test]
    fn lag_is_clamped_at_zero_on_clock_skew() {
        // BC26: a recorded time in the future must never yield a negative
        // lag.
        let state = HealthState::new();
        state.set_commit_time(5_000);
        state.set_scorer_pass(5_000);
        assert_eq!(state.jetstream_lag_s(4_000), Some(0));
        assert_eq!(state.last_pass_age_s(4_000), Some(0));
    }

    #[test]
    fn clone_shares_the_same_underlying_atomics() {
        let state = HealthState::new();
        let cloned = state.clone();
        cloned.set_commit_time(9_000);
        assert_eq!(state.jetstream_lag_s(9_010), Some(10));
    }
}
