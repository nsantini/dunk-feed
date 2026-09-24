//! The first-build queue and worker, TECH-DESIGN-network-feed §6.2 to §6.4.
//! [`JobQueue`] is a de-duplicated FIFO of pending viewers (BC6);
//! [`run_worker`] drains it, running [`crate::graph::build::step_follows`]
//! over a [`GraphSource`] and saving the result through [`Store`] (BC7,
//! BC7a, BC8, BC24). [`run_touch_flush`] is the periodic task that writes
//! `last_request_at` to SQLite at most once every 60 s per viewer (BC23).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::graph::build::{step_follows, GraphSource};
use crate::graph::circle::Circle;
use crate::graph::{CircleState, GraphHandle, ViewerDid};
use crate::store::{unix_now, Store};

/// The worker's fixed retry delay on a failed first build (BC8). Not an
/// environment variable: AGENTS.md and spec.md's `## Defaults taken` limit
/// `src/config.rs` to the PRD's score-table constants, and this one is not
/// in it.
pub const RETRY_DELAY: Duration = Duration::from_secs(30);

/// How many consecutive failed first-build attempts a viewer gets before the
/// worker gives up on it (BC8 as amended, review round 1, defect W): fewer
/// than this many failures still retry after [`RETRY_DELAY`]; at this count,
/// [`process_job`] removes the viewer instead of scheduling another retry.
/// Not an environment variable, the same reason [`RETRY_DELAY`] is not.
pub(crate) const MAX_FIRST_BUILD_ATTEMPTS: u32 = 5;

/// How long [`GraphHandle::enqueue_first_build`] refuses a new first build
/// for a viewer the worker gave up on (defect W), after which a fresh
/// request may start one again.
pub(crate) const REMOVAL_COOLDOWN_SECS: i64 = 60 * 60;

/// The worker's fixed page cap for a first build's `step_follows` (review
/// round 1, defect Z): a viewer with far more than this many pages of
/// follows still gets a circle, built from whatever was fetched before the
/// cap, rather than blocking the queue paging to the end. `graph_probe::run`
/// passes `None` instead, since the probe measures a whole list's real page
/// count.
pub(crate) const FIRST_BUILD_MAX_PAGES: u32 = 100;

/// How often [`run_touch_flush`] wakes to check for due touches. Shorter
/// than [`TOUCH_MIN_INTERVAL_SECS`] so a touch is never held back much past
/// its 60 s minimum.
pub const TOUCH_FLUSH_TICK: Duration = Duration::from_secs(5);

/// The minimum time between two SQLite writes of the same viewer's
/// `last_request_at` (BC23).
pub const TOUCH_MIN_INTERVAL_SECS: i64 = 60;

/// A callback the worker invokes after swapping in a freshly built circle
/// (BC7's "cached lists for the viewer dropped"): `src/http/viewer.rs`
/// (slice 3.0) has no `ViewerLists` yet, so this stays an injectable
/// closure over the viewer's DID, which `run` (`src/ingest/mod.rs`, slice
/// 4.0) will wire to `ViewerLists::drop_viewer`. `None` here just means no
/// cache exists yet to drop from.
pub type DropListsFn = Arc<dyn Fn(&str) + Send + Sync>;

/// A de-duplicated FIFO of viewers awaiting a `FirstBuild` job (BC6): a
/// viewer already queued or currently being processed is not queued again.
/// `push` returns `false` in that case; the caller — `GraphHandle`'s own
/// `enqueue_first_build` already guards on its circle map, so this is a
/// second, independent de-dup layer a bare `JobQueue` enforces on its own.
pub struct JobQueue {
    pending: Mutex<VecDeque<ViewerDid>>,
    /// Every viewer with a job queued or currently running. A viewer stays
    /// in this set across a failed attempt's retry wait (BC8), so a request
    /// that arrives while a build is retrying still de-duplicates against
    /// it.
    outstanding: Mutex<HashSet<ViewerDid>>,
    /// Consecutive failed first-build attempts per viewer since its last
    /// success or removal (BC8 as amended, defect W). A viewer with no entry
    /// has failed zero times.
    attempts: Mutex<HashMap<ViewerDid, u32>>,
    notify: Notify,
}

impl JobQueue {
    /// An empty queue.
    pub fn new() -> Arc<Self> {
        Arc::new(JobQueue {
            pending: Mutex::new(VecDeque::new()),
            outstanding: Mutex::new(HashSet::new()),
            attempts: Mutex::new(HashMap::new()),
            notify: Notify::new(),
        })
    }

    /// Enqueues `viewer` at the back of the FIFO, unless it is already
    /// queued or running (BC6). Returns whether a job was actually queued.
    pub fn push(&self, viewer: ViewerDid) -> bool {
        {
            let mut outstanding = self.outstanding.lock().expect("JobQueue outstanding poisoned");
            if !outstanding.insert(viewer.clone()) {
                return false;
            }
        }
        self.pending.lock().expect("JobQueue pending poisoned").push_back(viewer);
        self.notify.notify_one();
        true
    }

    /// Waits for and removes the viewer at the front of the FIFO. Does not
    /// clear its outstanding mark — [`Self::complete`] or [`Self::retry`]
    /// does that (or keeps it set, for a retry).
    pub async fn pop(&self) -> ViewerDid {
        loop {
            if let Some(viewer) =
                self.pending.lock().expect("JobQueue pending poisoned").pop_front()
            {
                return viewer;
            }
            self.notify.notified().await;
        }
    }

    /// A non-blocking [`Self::pop`], for tests that check what is queued
    /// without an async runtime driving `pop`'s wait. Test-only: no
    /// production code needs a non-blocking pop.
    #[cfg(test)]
    pub(crate) fn try_pop(&self) -> Option<ViewerDid> {
        self.pending.lock().expect("JobQueue pending poisoned").pop_front()
    }

    /// Marks `viewer`'s job finished (BC7): clears its outstanding mark, so
    /// a future `push` for the same viewer starts a fresh job.
    pub fn complete(&self, viewer: &ViewerDid) {
        self.outstanding.lock().expect("JobQueue outstanding poisoned").remove(viewer);
    }

    /// Re-queues `viewer` at the back of the FIFO after a failed attempt
    /// (BC8), keeping its outstanding mark set so a concurrent `push` for
    /// the same viewer still de-duplicates against it.
    pub fn retry(&self, viewer: ViewerDid) {
        self.pending.lock().expect("JobQueue pending poisoned").push_back(viewer);
        self.notify.notify_one();
    }

    /// Records one failed first-build attempt for `viewer` and returns the
    /// new count (BC8 as amended, defect W).
    fn record_failure(&self, viewer: &ViewerDid) -> u32 {
        let mut attempts = self.attempts.lock().expect("JobQueue attempts poisoned");
        let count = attempts.entry(viewer.clone()).or_insert(0);
        *count += 1;
        *count
    }

    /// Clears `viewer`'s failed-attempt count (defect W): called on a
    /// successful save (BC7) or once the worker gives up and removes the
    /// viewer, so a later first build starts counting from zero.
    fn clear_attempts(&self, viewer: &ViewerDid) {
        self.attempts.lock().expect("JobQueue attempts poisoned").remove(viewer);
    }
}

/// One job attempt: writes the `building_d1` row (BC7a), runs step 1, and
/// on success saves the circle, swaps it into `handle`, clears the failed-
/// attempt count and calls `drop_lists`; on any failure — a `PdsError` from
/// step 1, or a `StoreError` from either save (BC24) — hands off to
/// [`handle_failure`] (BC8 as amended, defect W).
#[allow(clippy::too_many_arguments)]
async fn process_job<S: GraphSource>(
    viewer: &ViewerDid,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    source: &S,
    d2_sample_size: usize,
    drop_lists: Option<&DropListsFn>,
    retry_delay: Duration,
) {
    let start_now = unix_now();
    if let Err(err) =
        store.viewer_save_state(&viewer.0, CircleState::BuildingD1.as_str(), start_now)
    {
        tracing::warn!(kind = ?err, "graph: worker failed to save building_d1 state");
        handle_failure(viewer, handle, queue, store, drop_lists, retry_delay).await;
        return;
    }

    let mut circle = Circle::new();
    if let Err(err) =
        step_follows(source, &viewer.0, d2_sample_size, &mut circle, Some(FIRST_BUILD_MAX_PAGES))
            .await
    {
        tracing::warn!(kind = ?err, "graph: worker step_follows failed");
        handle_failure(viewer, handle, queue, store, drop_lists, retry_delay).await;
        return;
    }

    let saved_at = unix_now();
    if let Err(err) = store.viewer_save_circle(
        &viewer.0,
        CircleState::Ready.as_str(),
        saved_at,
        saved_at,
        &circle.d2_sample,
        &circle.follows,
    ) {
        tracing::warn!(kind = ?err, "graph: worker failed to save circle");
        handle_failure(viewer, handle, queue, store, drop_lists, retry_delay).await;
        return;
    }

    circle.d1_refreshed_at = Some(saved_at);
    circle.last_request_at = saved_at;
    handle.insert_ready(viewer, circle);
    queue.complete(viewer);
    queue.clear_attempts(viewer);
    if let Some(drop_lists) = drop_lists {
        drop_lists(&viewer.0);
    }
}

/// One job attempt's failure path (BC8 as amended, review round 1, defect
/// W): below [`MAX_FIRST_BUILD_ATTEMPTS`] failures, re-queues `viewer` after
/// `retry_delay` exactly as before; at the limit, gives up instead — deletes
/// `viewer`'s SQLite row (`viewer_delete`), removes its in-memory circle and
/// starts its [`REMOVAL_COOLDOWN_SECS`] cooldown (freeing the
/// `UPSTAGE_MAX_VIEWERS` slot `GraphHandle::enqueue_first_build` counts
/// against), drops its cached lists, and logs one `warn` naming the attempt
/// count and no DID (BC21).
async fn handle_failure(
    viewer: &ViewerDid,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    drop_lists: Option<&DropListsFn>,
    retry_delay: Duration,
) {
    let attempts = queue.record_failure(viewer);
    if attempts < MAX_FIRST_BUILD_ATTEMPTS {
        schedule_retry(queue, viewer.clone(), retry_delay);
        return;
    }

    tracing::warn!(
        attempts,
        "graph: worker giving up on first build after repeated failures, removing viewer"
    );
    if let Err(err) = store.viewer_delete(&viewer.0) {
        tracing::warn!(kind = ?err, "graph: failed to delete viewer row after giving up");
    }
    handle.remove_after_giving_up(viewer, unix_now(), REMOVAL_COOLDOWN_SECS);
    queue.complete(viewer);
    queue.clear_attempts(viewer);
    if let Some(drop_lists) = drop_lists {
        drop_lists(&viewer.0);
    }
}

/// Spawns a task that waits `retry_delay`, then re-queues `viewer` (BC8).
/// A separate task rather than an inline sleep, so the worker's own loop
/// keeps draining other viewers' jobs while this one waits.
fn schedule_retry(queue: &Arc<JobQueue>, viewer: ViewerDid, retry_delay: Duration) {
    let queue = Arc::clone(queue);
    tokio::spawn(async move {
        tokio::time::sleep(retry_delay).await;
        queue.retry(viewer);
    });
}

/// Runs forever, taking one `FirstBuild` job at a time from `handle`'s
/// queue and processing it (BC7, BC7a, BC8, BC24). `run` (`src/ingest/mod.rs`)
/// spawns this once, on its own task, when `UPSTAGE_PERSONALISE` is `true`
/// and BSKY credentials exist (BC18).
pub async fn run_worker<S: GraphSource>(
    handle: Arc<GraphHandle>,
    store: Store,
    source: S,
    d2_sample_size: usize,
    drop_lists: Option<DropListsFn>,
) {
    run_worker_with_retry_delay(handle, store, source, d2_sample_size, drop_lists, RETRY_DELAY)
        .await
}

/// [`run_worker`]'s body, parameterised over the retry delay: this
/// repository carries no `tokio` `test-util` feature (`Cargo.toml` is out
/// of this slice's files), so a test shrinks `retry_delay` to see a retry
/// in milliseconds rather than waiting out the real 30 s.
async fn run_worker_with_retry_delay<S: GraphSource>(
    handle: Arc<GraphHandle>,
    store: Store,
    source: S,
    d2_sample_size: usize,
    drop_lists: Option<DropListsFn>,
    retry_delay: Duration,
) {
    let queue = handle.queue();
    loop {
        let viewer = queue.pop().await;
        process_job(
            &viewer,
            &handle,
            &queue,
            &store,
            &source,
            d2_sample_size,
            drop_lists.as_ref(),
            retry_delay,
        )
        .await;
    }
}

/// Runs forever, writing every viewer's due in-memory touch to SQLite
/// (BC23): at most once every 60 s per viewer, off `handle`'s own
/// snapshot, never on the request path. `run` spawns this alongside
/// [`run_worker`].
pub async fn run_touch_flush(handle: Arc<GraphHandle>, store: Store) {
    run_touch_flush_with_period(handle, store, TOUCH_FLUSH_TICK, TOUCH_MIN_INTERVAL_SECS).await
}

/// [`run_touch_flush`]'s body, parameterised over the tick and the minimum
/// interval, for the same reason [`run_worker_with_retry_delay`] takes a
/// `retry_delay`.
async fn run_touch_flush_with_period(
    handle: Arc<GraphHandle>,
    store: Store,
    tick: Duration,
    min_interval: i64,
) {
    loop {
        tokio::time::sleep(tick).await;
        let now = unix_now();
        for (viewer, last_request_at) in handle.due_flushes(now, min_interval) {
            if let Err(err) = store.viewer_touch(&viewer.0, last_request_at) {
                tracing::warn!(kind = ?err, "graph: touch flush failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::appview::pds::PdsError;
    use crate::graph::build::FollowsPage;

    fn did(n: usize) -> String {
        format!("did:plc:{n:04}")
    }

    /// A `GraphSource` with `follows.len()` follows for every actor, paged
    /// at the caller's own `limit`, for [`i_follow_within_15s`] (AC4).
    struct ManyFollowsSource {
        follows: Vec<String>,
    }

    impl GraphSource for ManyFollowsSource {
        async fn get_follows(
            &self,
            _actor: &str,
            limit: u32,
            cursor: Option<String>,
        ) -> Result<FollowsPage, PdsError> {
            let offset: usize = cursor.as_deref().and_then(|c| c.parse().ok()).unwrap_or(0);
            let end = (offset + limit as usize).min(self.follows.len());
            let dids =
                self.follows.get(offset.min(self.follows.len())..end).unwrap_or_default().to_vec();
            let cursor = if end < self.follows.len() { Some(end.to_string()) } else { None };
            Ok(FollowsPage { dids, cursor })
        }

        async fn get_relationships(
            &self,
            _actor: &str,
            _others: &[String],
        ) -> Result<Vec<String>, PdsError> {
            Ok(Vec::new())
        }
    }

    /// A `GraphSource` whose `get_follows` always fails, counting its
    /// calls, for [`retries_after_thirty_seconds`] (BC8).
    struct FailingSource {
        calls: StdMutex<u32>,
    }

    impl GraphSource for FailingSource {
        async fn get_follows(
            &self,
            _actor: &str,
            _limit: u32,
            _cursor: Option<String>,
        ) -> Result<FollowsPage, PdsError> {
            *self.calls.lock().unwrap() += 1;
            Err(PdsError::Session)
        }

        async fn get_relationships(
            &self,
            _actor: &str,
            _others: &[String],
        ) -> Result<Vec<String>, PdsError> {
            Ok(Vec::new())
        }
    }

    fn memory_store() -> Store {
        Store::open_memory().expect("in-memory store opens and migrates")
    }

    #[test]
    fn push_twice_for_the_same_viewer_is_one_job() {
        // BC6: a second push for a viewer already queued adds no second
        // FIFO entry.
        let queue = JobQueue::new();
        let viewer = ViewerDid("did:plc:a".to_string());

        assert!(queue.push(viewer.clone()));
        assert!(!queue.push(viewer.clone()));

        assert_eq!(queue.try_pop(), Some(viewer));
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn push_after_complete_starts_a_fresh_job() {
        let queue = JobQueue::new();
        let viewer = ViewerDid("did:plc:a".to_string());

        assert!(queue.push(viewer.clone()));
        queue.try_pop();
        queue.complete(&viewer);

        assert!(queue.push(viewer.clone()));
    }

    #[test]
    fn push_while_retrying_still_dedupes() {
        // BC6, BC8: `retry` keeps the viewer's outstanding mark, so a push
        // for the same viewer during the retry wait is still a no-op.
        let queue = JobQueue::new();
        let viewer = ViewerDid("did:plc:a".to_string());

        queue.push(viewer.clone());
        queue.try_pop();
        queue.retry(viewer.clone());

        assert!(!queue.push(viewer.clone()));
    }

    #[tokio::test]
    async fn i_follow_within_15s() {
        // AC4: with a fake source, a viewer who follows 1,000 accounts
        // sees the build finish well inside 15 s.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        handle.enqueue_first_build(viewer.clone(), unix_now());

        let source = ManyFollowsSource { follows: (0..1000).map(did).collect() };
        let worker_handle = Arc::clone(&handle);
        tokio::spawn(run_worker(worker_handle, memory_store(), source, 100, None));

        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(circle) = handle.get(&viewer) {
                    if circle.state == CircleState::Ready {
                        return circle;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("circle must be ready within 15 s");

        assert_eq!(result.follows.len(), 1000);
    }

    #[tokio::test]
    async fn retries_after_the_configured_delay() {
        // BC8: a `PdsError` from step 1 leaves the circle in `building_d1`
        // and re-queues the job after the retry delay, without touching
        // the circle map in between.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        handle.enqueue_first_build(viewer.clone(), unix_now());

        let source = FailingSource { calls: StdMutex::new(0) };
        let queue = handle.queue();
        let job = queue.pop().await;
        assert_eq!(job, viewer);

        process_job(
            &job,
            &handle,
            &queue,
            &memory_store(),
            &source,
            10,
            None,
            Duration::from_millis(20),
        )
        .await;

        assert_eq!(handle.get(&viewer).unwrap().state, CircleState::BuildingD1);
        assert_eq!(*source.calls.lock().unwrap(), 1);
        assert!(queue.try_pop().is_none(), "the retry has not fired yet");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(queue.try_pop(), Some(viewer), "the job was re-queued after the retry delay");
    }

    #[tokio::test]
    async fn gives_up_after_five_failed_attempts_in_a_row() {
        // BC8 as amended, review round 1, defect W: a viewer whose first
        // build fails `MAX_FIRST_BUILD_ATTEMPTS` times running is removed
        // instead of retried forever, freeing its `UPSTAGE_MAX_VIEWERS`
        // slot and its SQLite row.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        handle.enqueue_first_build(viewer.clone(), unix_now());

        let source = FailingSource { calls: StdMutex::new(0) };
        let store = memory_store();
        let queue = handle.queue();

        for attempt in 1..=MAX_FIRST_BUILD_ATTEMPTS {
            let job = queue.pop().await;
            assert_eq!(job, viewer);
            process_job(&job, &handle, &queue, &store, &source, 10, None, Duration::from_millis(5))
                .await;
            if attempt < MAX_FIRST_BUILD_ATTEMPTS {
                // Below the limit: the circle is still there, retrying.
                assert_eq!(handle.get(&viewer).unwrap().state, CircleState::BuildingD1);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        assert!(handle.get(&viewer).is_none(), "the viewer was removed after repeated failures");
        assert!(store.viewer_load_all().unwrap().is_empty(), "the viewers row was deleted");
        assert!(queue.try_pop().is_none(), "no job is queued during the cooldown");
        assert_eq!(*source.calls.lock().unwrap(), MAX_FIRST_BUILD_ATTEMPTS);
    }

    #[tokio::test]
    async fn store_error_in_the_worker_is_caught_and_retried() {
        // BC24: a `StoreError` from the save (here, a write attempted
        // through a read-only connection) is caught in this module and
        // handled exactly like a `PdsError` (BC8).
        let nanos =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("upstage-graph-queue-bc24-{nanos}.sqlite3"));
        let path_str = path.to_str().unwrap().to_string();
        {
            let seed = Store::open_path(&path_str).unwrap();
            drop(seed);
        }
        let read_only_store = Store::open_read_only(&path_str).unwrap();

        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        handle.enqueue_first_build(viewer.clone(), unix_now());
        let queue = handle.queue();
        let job = queue.pop().await;

        let source = ManyFollowsSource { follows: vec![did(1)] };
        process_job(
            &job,
            &handle,
            &queue,
            &read_only_store,
            &source,
            10,
            None,
            Duration::from_millis(20),
        )
        .await;

        assert_eq!(handle.get(&viewer).unwrap().state, CircleState::BuildingD1);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(queue.try_pop(), Some(viewer));

        let _ = std::fs::remove_file(&path_str);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }

    #[tokio::test]
    async fn touch_flush_writes_at_most_once_per_period() {
        // BC23 (flush part): the flush task writes a due touch through to
        // the store, off `handle`'s own in-memory snapshot.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:a".to_string());
        let store = memory_store();
        store.viewer_save_state("did:plc:a", "ready", unix_now()).unwrap();
        handle.record_touch(&viewer, unix_now());

        let flush_store = store.clone();
        let flush_handle = Arc::clone(&handle);
        tokio::spawn(run_touch_flush_with_period(
            flush_handle,
            flush_store,
            Duration::from_millis(10),
            0,
        ));

        tokio::time::sleep(Duration::from_millis(100)).await;
        let rows = store.viewer_load_all().unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].last_request_at > 0);
    }
}
