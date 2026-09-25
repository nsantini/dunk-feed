//! Feed health metrics, story 10 spec.md `## Approach`. This slice holds the
//! hourly counter types only: [`CallCounters`] counts PDS calls by nsid,
//! [`EvictCounters`] counts evictions by [`super::EvictReason`]. Both are
//! `Arc`-shared: `PdsClient` (`src/appview/pds.rs`) and `GraphHandle`
//! (`src/graph/mod.rs`) each hold one alongside the state they already touch,
//! and the hourly task (slice 2.0) reads and resets them with `take()` right
//! before it writes the `graph.health` line (BC9). `health_line` and the task
//! itself land in slice 2.0.

use std::collections::HashMap;
use std::sync::Mutex;

use super::EvictReason;

/// PDS calls made since the last [`Self::take`], keyed by nsid (BC7).
/// `PdsClient::call_raw` is this counter's one writer, incrementing once per
/// call — including session calls (`createSession`, `refreshSession`,
/// spec.md `## Defaults taken`) — at the shared choke point every method
/// routes through.
#[derive(Default)]
pub struct CallCounters {
    counts: Mutex<HashMap<&'static str, u64>>,
}

impl CallCounters {
    /// A counter with nothing recorded yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments `nsid`'s count by one.
    pub fn record(&self, nsid: &'static str) {
        let mut counts = self.counts.lock().expect("CallCounters mutex poisoned");
        *counts.entry(nsid).or_insert(0) += 1;
    }

    /// Returns the counts recorded since the last `take`, resetting every
    /// count to zero (BC9): the hourly task calls this once, right after
    /// building the fields the counts feed into.
    pub fn take(&self) -> HashMap<&'static str, u64> {
        let mut counts = self.counts.lock().expect("CallCounters mutex poisoned");
        std::mem::take(&mut *counts)
    }
}

/// Circles evicted since the last [`Self::take`], by [`EvictReason`] (BC6).
/// `GraphHandle::evict_locked` is this counter's one writer, incrementing
/// once per eviction regardless of reason.
#[derive(Default)]
pub struct EvictCounters {
    idle: Mutex<u64>,
    lru: Mutex<u64>,
}

impl EvictCounters {
    /// A counter with nothing recorded yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the count for `reason` by one.
    pub fn record(&self, reason: EvictReason) {
        let counter = match reason {
            EvictReason::Idle => &self.idle,
            EvictReason::Lru => &self.lru,
        };
        *counter.lock().expect("EvictCounters mutex poisoned") += 1;
    }

    /// Returns `(idle, lru)` counts since the last `take`, resetting both to
    /// zero (BC9).
    pub fn take(&self) -> (u64, u64) {
        let mut idle = self.idle.lock().expect("EvictCounters mutex poisoned");
        let mut lru = self.lru.lock().expect("EvictCounters mutex poisoned");
        let counts = (*idle, *lru);
        *idle = 0;
        *lru = 0;
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_counters_reset_on_take() {
        let counters = CallCounters::new();
        counters.record("app.bsky.graph.getFollows");
        counters.record("app.bsky.graph.getFollows");
        counters.record("com.atproto.server.createSession");

        let taken = counters.take();
        assert_eq!(taken.get("app.bsky.graph.getFollows"), Some(&2));
        assert_eq!(taken.get("com.atproto.server.createSession"), Some(&1));

        assert!(counters.take().is_empty(), "counts reset after take (BC9)");
    }

    #[test]
    fn evict_counters_reset_on_take() {
        let counters = EvictCounters::new();
        counters.record(EvictReason::Idle);
        counters.record(EvictReason::Idle);
        counters.record(EvictReason::Lru);

        assert_eq!(counters.take(), (2, 1));
        assert_eq!(counters.take(), (0, 0), "counts reset after take (BC9)");
    }
}
