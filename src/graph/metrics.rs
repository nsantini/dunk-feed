//! Feed health metrics, story 10 spec.md `## Approach`. [`CallCounters`]
//! counts PDS calls by nsid, [`EvictCounters`] counts evictions by
//! [`super::EvictReason`]; both are `Arc`-shared, `PdsClient`
//! (`src/appview/pds.rs`) and `GraphHandle` (`src/graph/mod.rs`) each holding
//! one alongside the state they already touch. [`health_line`] is the pure
//! function that turns one hour's [`HealthInputs`] into the `graph.health`
//! JSON line's fields, with no clock and no log of its own, so it is tested
//! on fixed inputs alone (AC1). [`run_hourly_task`] is the async task
//! `src/ingest/mod.rs`'s `start_graph_subsystem` spawns when
//! `config.personalise` is `true` (BC13): once an hour, it reads every active
//! viewer's current list through `http::viewer::health_items`, this module's
//! own counters and `JobQueue::depth`, logs one `info` line at `graph.health`
//! through [`health_line`], and resets the call and eviction counters (BC9).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::time::MissedTickBehavior;

use super::{EvictReason, GraphHandle, QueueDepth};
use crate::scorer::snapshot::SnapshotHandle;
use crate::store::unix_now;

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

/// Anything that can report and reset PDS calls by nsid for the last hour
/// (BC7, BC9): [`run_hourly_task`] is generic over this rather than naming
/// `appview::pds::PdsClient` directly, so a test fake stands in for it with
/// no network. Implemented below for every `PdsClient<T>` by delegating to
/// its own inherent `take_call_counts` (an inherent method always takes
/// priority over a trait method of the same name in method-call syntax, so
/// this never recurses into itself).
pub trait CallCountsSource {
    fn take_call_counts(&self) -> HashMap<&'static str, u64>;
}

impl<T: crate::appview::pds::PdsTransport> CallCountsSource for crate::appview::pds::PdsClient<T> {
    fn take_call_counts(&self) -> HashMap<&'static str, u64> {
        self.take_call_counts()
    }
}

/// One active viewer's contribution to the health line's medians (story 10
/// spec.md BC3, BC5): `new_pairs_24h` is the count of that viewer's current
/// list items whose `promoted_at` falls in the last 24 hours;
/// `total_items` and `degree2_only_items` are the current list's full size
/// and the count of items flagged degree-2-only
/// (`http::viewer::HealthItem`). A viewer with `total_items == 0` is left
/// out of the discovery median (BC5, spec.md `## Defaults taken`) — that
/// exclusion is [`health_line`]'s own job, not this struct's, so the
/// hourly task never has to special-case it before building
/// [`HealthInputs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewerHealth {
    pub new_pairs_24h: u64,
    pub total_items: usize,
    pub degree2_only_items: usize,
}

/// [`health_line`]'s fixed inputs for one hour (story 10 spec.md
/// `## Approach`): one [`ViewerHealth`] per active viewer, the evictions and
/// PDS calls counted since the last line (BC6, BC7), and the queue depth at
/// line time (BC8). [`run_hourly_task`] builds this fresh every hour;
/// nothing here reads a clock or a log, which is what lets [`health_line`]
/// run in a test with neither (spec.md `## Approach`).
#[derive(Debug, Clone)]
pub struct HealthInputs {
    pub viewers: Vec<ViewerHealth>,
    pub evicted: (u64, u64),
    pub graph_calls: HashMap<&'static str, u64>,
    pub queue_depth: QueueDepth,
}

/// The mean of the two middle values for an even count, the middle value
/// itself for an odd one (BC10); `0.0` for no values at all — every caller
/// here already means "nothing to report" by an empty slice, not "unknown".
fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).expect("health metric values are never NaN"));
    let n = values.len();
    if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    }
}

/// Builds the `graph.health` line's fields from `inputs` (BC1-BC10): a pure
/// function with no clock and no log, so [`run_hourly_task`] and this
/// module's own tests both go through it, and a test needs neither a clock
/// nor a subscriber to check a field (AC1). `active_viewers` is
/// `inputs.viewers.len()` (BC2): the hourly task already had to build one
/// `ViewerHealth` per active viewer to reach this call, so a separate count
/// field would only ever have to agree with that length or be wrong.
/// `median_discovery_share` (BC5) is computed over the viewers whose
/// `total_items` is at least 1 — a viewer with none is excluded outright,
/// not scored as `0.0` (spec.md `## Defaults taken`).
pub fn health_line(inputs: HealthInputs) -> Value {
    let active_viewers = inputs.viewers.len();
    let median_new_pairs_24h =
        median(inputs.viewers.iter().map(|viewer| viewer.new_pairs_24h as f64).collect());
    let zero_share = if active_viewers == 0 {
        0.0
    } else {
        let zero_count = inputs.viewers.iter().filter(|viewer| viewer.new_pairs_24h == 0).count();
        zero_count as f64 / active_viewers as f64
    };
    let discovery_shares: Vec<f64> = inputs
        .viewers
        .iter()
        .filter(|viewer| viewer.total_items > 0)
        .map(|viewer| viewer.degree2_only_items as f64 / viewer.total_items as f64)
        .collect();
    let median_discovery_share = median(discovery_shares);
    let graph_calls: serde_json::Map<String, Value> =
        inputs.graph_calls.iter().map(|(nsid, count)| (nsid.to_string(), json!(count))).collect();

    json!({
        "event": "graph.health",
        "active_viewers": active_viewers,
        "median_new_pairs_24h": median_new_pairs_24h,
        "zero_share": zero_share,
        "median_discovery_share": median_discovery_share,
        "evicted_1h": { "idle": inputs.evicted.0, "lru": inputs.evicted.1 },
        "graph_calls_1h": Value::Object(graph_calls),
        "queue_depth": {
            "first_build": inputs.queue_depth.first_build,
            "refresh": inputs.queue_depth.refresh,
            "refill": inputs.queue_depth.refill,
        },
    })
}

/// Builds one hour's [`HealthInputs`] from live state and logs the
/// `graph.health` line through [`health_line`] (BC1-BC9): reads
/// `handle`'s active viewers (BC2), each one's current list through
/// `http::viewer::health_items`, the PDS call counts off `calls`, the
/// eviction counts off `handle` and the queue depth off `handle`'s own
/// queue — the last three all `take`n or read here, right before the line
/// is built, so a concurrent writer between this call and the line being
/// logged is the only window BC9's reset can ever miss. Named fields, not
/// a single blown-in JSON blob, the same convention `Stats::emit`
/// (`src/ingest/mod.rs`) already uses for its own periodic line.
fn emit_health_line<C: CallCountsSource>(
    handle: &Arc<GraphHandle>,
    calls: &C,
    snapshot: &SnapshotHandle,
    follows_me_depth: usize,
    idle_evict_d: u32,
) {
    let now = unix_now();
    let cutoff = now - 24 * 60 * 60;
    let snap = snapshot.current();
    let follows_cache = handle.follows_cache();

    let viewers: Vec<ViewerHealth> = handle
        .active_viewers(now, idle_evict_d)
        .iter()
        .map(|viewer| {
            let items = handle
                .get(viewer)
                .map(|circle| {
                    crate::http::viewer::health_items(
                        &circle,
                        &snap,
                        follows_me_depth,
                        &follows_cache,
                    )
                })
                .unwrap_or_default();
            let new_pairs_24h =
                items.iter().filter(|item| item.promoted_at >= cutoff).count() as u64;
            let degree2_only_items = items.iter().filter(|item| item.degree2_only).count();
            ViewerHealth { new_pairs_24h, total_items: items.len(), degree2_only_items }
        })
        .collect();

    let evicted = handle.evict_counts();
    let graph_calls = calls.take_call_counts();
    let queue_depth = handle.queue().depth();

    let line = health_line(HealthInputs { viewers, evicted, graph_calls, queue_depth });
    tracing::info!(
        active_viewers = line["active_viewers"].as_u64().unwrap_or(0),
        median_new_pairs_24h = line["median_new_pairs_24h"].as_f64().unwrap_or(0.0),
        zero_share = line["zero_share"].as_f64().unwrap_or(0.0),
        median_discovery_share = line["median_discovery_share"].as_f64().unwrap_or(0.0),
        evicted_1h = %line["evicted_1h"],
        graph_calls_1h = %line["graph_calls_1h"],
        queue_depth = %line["queue_depth"],
        "graph.health"
    );
}

/// Runs forever, writing one `graph.health` line an hour (BC1): the first
/// tick of `tokio::time::interval` fires immediately, so it is skipped here
/// — the first line lands one hour after start, not at start (spec.md
/// `## Non-goals`). `src/ingest/mod.rs`'s `start_graph_subsystem` spawns
/// this only when `config.personalise` is `true` (BC13); with the flag off,
/// this function is never called and no `graph.health` line is ever
/// written.
pub async fn run_hourly_task<C: CallCountsSource + Send + Sync + 'static>(
    handle: Arc<GraphHandle>,
    calls: Arc<C>,
    snapshot: SnapshotHandle,
    follows_me_depth: usize,
    idle_evict_d: u32,
) {
    run_hourly_task_with_period(
        handle,
        calls,
        snapshot,
        follows_me_depth,
        idle_evict_d,
        Duration::from_secs(3600),
    )
    .await
}

/// [`run_hourly_task`]'s body, parameterised over the tick period: this
/// repository carries no `tokio` `test-util` feature (`Cargo.toml` is out of
/// this slice's files), so a test shrinks `period` to see a line in
/// milliseconds rather than waiting out the real hour, the same pattern
/// `graph::queue::run_worker_with_retry_delay` already uses for its own
/// retry delay.
async fn run_hourly_task_with_period<C: CallCountsSource + Send + Sync + 'static>(
    handle: Arc<GraphHandle>,
    calls: Arc<C>,
    snapshot: SnapshotHandle,
    follows_me_depth: usize,
    idle_evict_d: u32,
    period: Duration,
) {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        interval.tick().await;
        emit_health_line(&handle, calls.as_ref(), &snapshot, follows_me_depth, idle_evict_d);
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

    fn empty_inputs() -> HealthInputs {
        HealthInputs {
            viewers: Vec::new(),
            evicted: (0, 0),
            graph_calls: HashMap::new(),
            queue_depth: QueueDepth { first_build: 0, refresh: 0, refill: 0 },
        }
    }

    fn viewer(new_pairs_24h: u64, total_items: usize, degree2_only_items: usize) -> ViewerHealth {
        ViewerHealth { new_pairs_24h, total_items, degree2_only_items }
    }

    // AC1: with no active viewers, every share is `0.0`, not a division by
    // zero or a missing field.
    #[test]
    fn health_line_with_no_viewers() {
        let line = health_line(empty_inputs());

        assert_eq!(line["event"], "graph.health");
        assert_eq!(line["active_viewers"], 0);
        assert_eq!(line["median_new_pairs_24h"], 0.0);
        assert_eq!(line["zero_share"], 0.0);
        assert_eq!(line["median_discovery_share"], 0.0);
        assert_eq!(line["evicted_1h"], json!({ "idle": 0, "lru": 0 }));
        assert_eq!(line["graph_calls_1h"], json!({}));
        assert_eq!(line["queue_depth"], json!({ "first_build": 0, "refresh": 0, "refill": 0 }));
    }

    // AC1: an odd count of active viewers, each field computed from fixed
    // inputs — BC2 (the count), BC3 (the median), BC4 (the zero share),
    // BC6, BC7, BC8 (passed straight through).
    #[test]
    fn health_line_odd_count_of_viewers() {
        let inputs = HealthInputs {
            viewers: vec![viewer(0, 4, 1), viewer(5, 4, 2), viewer(3, 2, 0)],
            evicted: (2, 1),
            graph_calls: HashMap::from([("app.bsky.graph.getFollows", 4_u64)]),
            queue_depth: QueueDepth { first_build: 1, refresh: 2, refill: 3 },
        };

        let line = health_line(inputs);

        assert_eq!(line["active_viewers"], 3);
        assert_eq!(line["median_new_pairs_24h"], 3.0, "sorted [0, 3, 5], the middle value");
        assert_eq!(line["zero_share"], 1.0 / 3.0, "one of three viewers had zero new pairs");
        assert_eq!(
            line["median_discovery_share"], 0.25,
            "sorted [0.0, 0.25, 0.5], the middle value"
        );
        assert_eq!(line["evicted_1h"], json!({ "idle": 2, "lru": 1 }));
        assert_eq!(line["graph_calls_1h"]["app.bsky.graph.getFollows"], 4);
        assert_eq!(line["queue_depth"], json!({ "first_build": 1, "refresh": 2, "refill": 3 }));
    }

    // AC1, BC10: an even count of active viewers takes the mean of the two
    // middle values, for both medians.
    #[test]
    fn health_line_even_count_takes_the_mean_of_the_two_middle_values() {
        let inputs = HealthInputs {
            viewers: vec![viewer(0, 5, 1), viewer(0, 5, 2), viewer(4, 5, 3), viewer(6, 5, 4)],
            ..empty_inputs()
        };

        let line = health_line(inputs);

        assert_eq!(line["median_new_pairs_24h"], 2.0, "sorted [0, 0, 4, 6], mean of 0 and 4");
        assert_eq!(line["zero_share"], 0.5);
        assert_eq!(
            line["median_discovery_share"], 0.5,
            "sorted shares [0.2, 0.4, 0.6, 0.8], mean of 0.4 and 0.6"
        );
    }

    // AC1, BC5, spec.md `## Defaults taken`: a viewer with zero circle items
    // is left out of the discovery median entirely, not scored as `0.0`.
    #[test]
    fn health_line_excludes_viewers_with_no_circle_items_from_discovery_median() {
        let inputs =
            HealthInputs { viewers: vec![viewer(0, 0, 0), viewer(0, 2, 1)], ..empty_inputs() };

        let line = health_line(inputs);

        assert_eq!(line["active_viewers"], 2, "both viewers still count toward BC2");
        assert_eq!(
            line["median_discovery_share"], 0.5,
            "the zero-item viewer contributes no value at all, so the one real share is the median"
        );
    }

    /// A fake [`CallCountsSource`] a test can seed and read back, with no
    /// `PdsClient` or network involved.
    #[derive(Default)]
    struct FakeCallCounts {
        counts: Mutex<HashMap<&'static str, u64>>,
    }

    impl CallCountsSource for FakeCallCounts {
        fn take_call_counts(&self) -> HashMap<&'static str, u64> {
            std::mem::take(&mut *self.counts.lock().expect("FakeCallCounts mutex poisoned"))
        }
    }

    // AC3, BC9: once the hourly task has logged a line, the call counter and
    // the eviction counter are both back at zero.
    #[tokio::test]
    async fn counters_reset() {
        use crate::auth::ViewerDid;

        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:a".to_string());
        handle.enqueue_first_build(viewer.clone(), 1_000);
        handle.evict(&viewer, EvictReason::Idle);

        let calls = Arc::new(FakeCallCounts::default());
        calls
            .counts
            .lock()
            .expect("FakeCallCounts mutex poisoned")
            .insert("app.bsky.graph.getFollows", 3);

        let snapshot = SnapshotHandle::new();
        tokio::spawn(run_hourly_task_with_period(
            Arc::clone(&handle),
            Arc::clone(&calls),
            snapshot,
            0,
            7,
            Duration::from_millis(5),
        ));

        // A generous multiple of the 5 ms period, so the task has certainly
        // logged at least one line by the time this reads the counters —
        // reading `evict_counts`/`take_call_counts` any earlier, in a loop,
        // would reset them itself and prove nothing about the task.
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            handle.evict_counts(),
            (0, 0),
            "the eviction counter is reset after a line (BC9)"
        );
        assert!(
            calls.take_call_counts().is_empty(),
            "the call counter is reset after a line (BC9)"
        );
    }

    /// A `GraphSource` with no follows and no relationships, for
    /// [`no_viewer_did_in_logs`] (AC4): that test cares what the logs say,
    /// not what the circle contains, so the simplest source that lets a
    /// first build and a refresh both complete is enough — the same role
    /// `queue::tests::ManyFollowsSource` plays for `queue.rs`'s own tests.
    struct EmptySource;

    impl crate::graph::build::GraphSource for EmptySource {
        async fn get_follows(
            &self,
            _actor: &str,
            _limit: u32,
            _cursor: Option<String>,
        ) -> Result<crate::graph::build::FollowsPage, crate::appview::pds::PdsError> {
            Ok(crate::graph::build::FollowsPage { dids: Vec::new(), cursor: None })
        }

        async fn get_relationships(
            &self,
            _actor: &str,
            _others: &[String],
        ) -> Result<Vec<String>, crate::appview::pds::PdsError> {
            Ok(Vec::new())
        }
    }

    /// A `tracing_subscriber::fmt::MakeWriter` that appends every formatted
    /// event to a shared buffer, so [`no_viewer_did_in_logs`] can search
    /// everything logged for the test viewer's DID — the same pattern
    /// `ingest::tests::CapturingWriter` (`src/ingest/mod.rs`) already uses.
    #[derive(Clone)]
    struct CapturingWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("CapturingWriter mutex poisoned").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturingWriter {
        type Writer = CapturingWriter;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    // AC4, BC11: a run of first build, refresh, eviction and one health
    // line, captured at `trace` level, names the test viewer's DID nowhere.
    #[tokio::test]
    async fn no_viewer_did_in_logs() {
        let test_did = "did:plc:privacytest0001";
        let viewer = crate::auth::ViewerDid(test_did.to_string());
        let handle = GraphHandle::new(10);
        handle.enqueue_first_build(viewer.clone(), unix_now());

        let snapshot = SnapshotHandle::new();
        // Defect AE (`queue.rs`): step 2 waits at generation 0. Swap in an
        // (empty, but real) pass first so step 2 does not stall.
        snapshot.swap(Arc::new(Vec::new()), Arc::new(Vec::new()));

        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(CapturingWriter(buf.clone()))
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let worker_handle = Arc::clone(&handle);
        tokio::spawn(crate::graph::queue::run_worker(
            worker_handle,
            crate::store::Store::open_memory().expect("open in-memory store"),
            EmptySource,
            100,
            None,
            snapshot.clone(),
            10,
            100,
            24,
        ));

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(circle) = handle.get(&viewer) {
                    if circle.state == crate::graph::CircleState::Ready {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first build must reach ready within 5s");

        // A refresh of the now-ready circle: pushed onto the same worker's
        // queue, drained by the same loop that just finished the first
        // build. A generous sleep, the same margin `counters_reset` above
        // gives its own hourly task, stands in for a completion signal the
        // refresh path has no other way to expose to a test.
        handle.queue().push(crate::graph::queue::Job::Refresh(viewer.clone()));
        tokio::time::sleep(Duration::from_millis(200)).await;

        handle.evict(&viewer, EvictReason::Idle);

        let calls = FakeCallCounts::default();
        calls
            .counts
            .lock()
            .expect("FakeCallCounts mutex poisoned")
            .insert("app.bsky.graph.getFollows", 1);
        emit_health_line(&handle, &calls, &snapshot, 10, 7);

        drop(dispatch);
        let output = String::from_utf8(buf.lock().expect("CapturingWriter mutex poisoned").clone())
            .expect("captured log output must be valid UTF-8");

        assert!(!output.contains(test_did), "no line may name the viewer DID (BC11): {output}");
    }
}
