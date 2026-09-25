//! The job queue and worker, TECH-DESIGN-network-feed §6.2 to §6.4, §10;
//! story 09 spec.md `## Approach`. [`Job`] is a `FirstBuild`, `Refresh` or
//! `Refill` job; [`JobQueue`] holds one de-duplicated FIFO lane per kind
//! (BC1, BC1a) and `pop` always drains the highest-priority non-empty lane
//! first. [`run_worker`] drains it, dispatching each job to its own path:
//! [`process_first_build`] runs [`crate::graph::build::step_follows`] (step
//! 1), [`crate::graph::build::step_follows_me`] (step 2) and [`run_step3`]
//! (step 3, story 08) over a [`GraphSource`], saving each step's result
//! through [`Store`] (BC3, BC3a, BC7, BC7a, BC8, BC24; story 08 BC1-BC5);
//! [`run_refresh`] (story 09) rebuilds steps 1 and 2 from scratch on a
//! `Ready` circle (BC4, BC5, BC6); [`run_refill`] (story 09) re-fetches one
//! degree-2 account's follows (BC7a, BC7b). [`run_touch_flush`] is the
//! periodic task that writes `last_request_at` to SQLite at most once every
//! 60 s per viewer (BC23) — story 09's scheduler (`graph::schedule`) folds
//! this into its own pass and replaces this task once it exists.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::graph::build::{
    fetch_account_follows, refresh, step_follows, step_follows_me, GraphSource, RankedAuthors,
};
use crate::graph::circle::Circle;
use crate::graph::{CircleState, FollowsCache, GraphHandle, ViewerDid};
use crate::scorer::snapshot::SnapshotHandle;
use crate::store::{unix_now, Store, StoreError};

/// The worker's fixed retry delay on a failed job (BC1b, BC8). Not an
/// environment variable: AGENTS.md and spec.md's `## Defaults taken` limit
/// `src/config.rs` to the PRD's score-table constants, and this one is not
/// in it.
pub const RETRY_DELAY: Duration = Duration::from_secs(30);

/// How many consecutive failed first-build attempts a viewer gets before the
/// worker gives up on it (BC8 as amended, review round 1, defect W): fewer
/// than this many failures still retry after [`RETRY_DELAY`]; at this count,
/// [`process_first_build`] removes the viewer instead of scheduling another
/// retry. Story 09's `Refresh` and `Refill` jobs have no give-up count of
/// their own (BC1b): only a first build's step 1 and step 2 use this.
pub(crate) const MAX_FIRST_BUILD_ATTEMPTS: u32 = 5;

/// How long [`GraphHandle::enqueue_first_build`] refuses a new first build
/// for a viewer the worker gave up on (defect W), after which a fresh
/// request may start one again.
pub(crate) const REMOVAL_COOLDOWN_SECS: i64 = 60 * 60;

/// The worker's fixed page cap for `step_follows`, shared by a first build
/// (review round 1, defect Z) and a refresh's own step 1 (story 09 spec.md
/// BC4): a viewer with far more than this many pages of follows still gets a
/// circle, built from whatever was fetched before the cap, rather than
/// blocking the queue paging to the end. `graph_probe::run` passes `None`
/// instead, since the probe measures a whole list's real page count.
pub(crate) const FIRST_BUILD_MAX_PAGES: u32 = 100;

/// How often [`run_touch_flush`] wakes to check for due touches. Shorter
/// than [`TOUCH_MIN_INTERVAL_SECS`] so a touch is never held back much past
/// its 60 s minimum.
pub const TOUCH_FLUSH_TICK: Duration = Duration::from_secs(5);

/// The minimum time between two SQLite writes of the same viewer's
/// `last_request_at` (BC23).
pub const TOUCH_MIN_INTERVAL_SECS: i64 = 60;

/// A callback the worker invokes after swapping in a freshly built or
/// refreshed circle, or refilling a degree-2 account's cache entry (BC7's
/// "cached lists for the viewer dropped"; story 09 BC5, BC7a): an injectable
/// closure over the viewer's DID, wired in `run` (`src/ingest/mod.rs`) to
/// `ViewerLists::drop_viewer`. `None` here just means no cache exists yet to
/// drop from.
pub type DropListsFn = Arc<dyn Fn(&str) + Send + Sync>;

/// One unit of work the worker can take off [`JobQueue`] (story 09 spec.md
/// `## Approach`): a first build for a viewer with no circle yet, a refresh
/// of a viewer's existing `Ready` circle, or a refetch of one degree-2
/// account's follows list. Each variant is its own de-duplication key (BC1a):
/// a viewer can carry a `FirstBuild` and a `Refresh` entry at once, but never
/// two of the same kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Job {
    FirstBuild(ViewerDid),
    Refresh(ViewerDid),
    Refill(String),
}

impl Job {
    /// The lane index this job queues in (BC1): 0 for `FirstBuild`, 1 for
    /// `Refresh`, 2 for `Refill`. [`JobQueue::pop`] always drains the
    /// lowest-numbered non-empty lane first.
    fn lane(&self) -> usize {
        match self {
            Job::FirstBuild(_) => 0,
            Job::Refresh(_) => 1,
            Job::Refill(_) => 2,
        }
    }
}

/// The number of lanes [`JobQueue`] holds, one per [`Job`] variant.
const LANE_COUNT: usize = 3;

/// A de-duplicated FIFO of jobs, in three priority lanes (BC1, BC1a): a job
/// already queued, running, or waiting to retry is not queued again. `push`
/// returns `false` in that case.
pub struct JobQueue {
    lanes: [Mutex<VecDeque<Job>>; LANE_COUNT],
    /// Every job queued or currently running, across all three lanes. A job
    /// stays in this set across a failed attempt's retry wait (BC1b), so a
    /// request that arrives while a job is retrying still de-duplicates
    /// against it.
    outstanding: Mutex<HashSet<Job>>,
    /// Consecutive failed step 1 attempts per viewer since its last success
    /// or removal (BC8 as amended, defect W). A viewer with no entry has
    /// failed zero times. Only a `FirstBuild` job touches this: `Refresh`
    /// and `Refill` have no give-up count (BC1b).
    attempts: Mutex<HashMap<ViewerDid, u32>>,
    /// Consecutive failed step 2 attempts per viewer since its last success
    /// or give-up (BC4b), counted separately from `attempts`: step 2 can
    /// retry and give up on its own without ever re-running step 1 or
    /// touching that count.
    step2_attempts: Mutex<HashMap<ViewerDid, u32>>,
    notify: Notify,
}

impl JobQueue {
    /// An empty queue.
    pub fn new() -> Arc<Self> {
        Arc::new(JobQueue {
            lanes: [
                Mutex::new(VecDeque::new()),
                Mutex::new(VecDeque::new()),
                Mutex::new(VecDeque::new()),
            ],
            outstanding: Mutex::new(HashSet::new()),
            attempts: Mutex::new(HashMap::new()),
            step2_attempts: Mutex::new(HashMap::new()),
            notify: Notify::new(),
        })
    }

    /// Enqueues `job` at the back of its own lane, unless it is already
    /// queued or running (BC1a). Returns whether a job was actually queued.
    pub fn push(&self, job: Job) -> bool {
        {
            let mut outstanding = self.outstanding.lock().expect("JobQueue outstanding poisoned");
            if !outstanding.insert(job.clone()) {
                return false;
            }
        }
        let lane = job.lane();
        self.lanes[lane].lock().expect("JobQueue lane poisoned").push_back(job);
        self.notify.notify_one();
        true
    }

    /// Waits for and removes the job at the front of the highest-priority
    /// non-empty lane (BC1): lane 0 (`FirstBuild`) before lane 1 (`Refresh`)
    /// before lane 2 (`Refill`), FIFO within a lane. Does not clear the
    /// job's outstanding mark — [`Self::complete`] or [`Self::retry`] does
    /// that (or keeps it set, for a retry).
    pub async fn pop(&self) -> Job {
        loop {
            if let Some(job) = self.try_pop() {
                return job;
            }
            self.notify.notified().await;
        }
    }

    /// A non-blocking [`Self::pop`], for tests that check what is queued
    /// without an async runtime driving `pop`'s wait, and for `pop`'s own
    /// loop body.
    pub(crate) fn try_pop(&self) -> Option<Job> {
        for lane in &self.lanes {
            if let Some(job) = lane.lock().expect("JobQueue lane poisoned").pop_front() {
                return Some(job);
            }
        }
        None
    }

    /// Marks `job` finished (BC7): clears its outstanding mark, so a future
    /// `push` of the same job starts a fresh one.
    pub fn complete(&self, job: &Job) {
        self.outstanding.lock().expect("JobQueue outstanding poisoned").remove(job);
    }

    /// Re-queues `job` at the back of its own lane after a failed attempt
    /// (BC1b, BC8), keeping its outstanding mark set so a concurrent `push`
    /// of the same job still de-duplicates against it.
    pub fn retry(&self, job: Job) {
        let lane = job.lane();
        self.lanes[lane].lock().expect("JobQueue lane poisoned").push_back(job);
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

    /// Records one failed step 2 attempt for `viewer` and returns the new
    /// count (BC4b), independent of [`Self::record_failure`]'s step 1
    /// count.
    fn record_step2_failure(&self, viewer: &ViewerDid) -> u32 {
        let mut attempts = self.step2_attempts.lock().expect("JobQueue step2_attempts poisoned");
        let count = attempts.entry(viewer.clone()).or_insert(0);
        *count += 1;
        *count
    }

    /// Clears `viewer`'s failed step 2 attempt count (BC4b): called on a
    /// successful step 2 save or once the worker gives up retrying it.
    fn clear_step2_attempts(&self, viewer: &ViewerDid) {
        self.step2_attempts.lock().expect("JobQueue step2_attempts poisoned").remove(viewer);
    }

    /// Clears every give-up attempt count recorded for `viewer` (story 09
    /// spec.md BC11a): called from `GraphHandle::evict`, so a viewer's next
    /// first build — after its cooldown, if a later give-up ever sets one —
    /// starts counting failures from zero rather than inheriting whatever
    /// this viewer's previous, now-evicted circle had racked up.
    pub(crate) fn forget(&self, viewer: &ViewerDid) {
        self.clear_attempts(viewer);
        self.clear_step2_attempts(viewer);
    }
}

/// BC6a, BC6b: called once a `FirstBuild` step's own existence check finds
/// `viewer`'s circle gone — evicted (`GraphHandle::evict`) while this job
/// waited its turn or ran. Completes `job` with nothing saved or swapped
/// (BC6a), then re-queues it at once if a circle already exists for
/// `viewer` again (BC6b): a request that arrived while `job`'s outstanding
/// mark was still set found `JobQueue::push` a no-op, so its new circle
/// would otherwise sit with no job until this viewer's next request
/// happened to retry it.
fn abandon_evicted_job(handle: &Arc<GraphHandle>, queue: &Arc<JobQueue>, viewer: &ViewerDid) {
    let job = Job::FirstBuild(viewer.clone());
    queue.complete(&job);
    if handle.get(viewer).is_some() {
        queue.push(job);
    }
}

/// One job's dispatch (story 09 spec.md `## Approach`): a `FirstBuild` runs
/// [`process_first_build`]'s three-step build; a `Refresh` runs
/// [`run_refresh`]; a `Refill` runs [`run_refill`].
#[allow(clippy::too_many_arguments)]
async fn process_job<S: GraphSource>(
    job: Job,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    source: &S,
    d2_sample_size: usize,
    drop_lists: Option<&DropListsFn>,
    retry_delay: Duration,
    snapshot: &SnapshotHandle,
    follows_me_depth: usize,
    d2_follows_depth: u32,
    d2_refresh_age_h: u32,
) {
    match job {
        Job::FirstBuild(viewer) => {
            process_first_build(
                &viewer,
                handle,
                queue,
                store,
                source,
                d2_sample_size,
                drop_lists,
                retry_delay,
                snapshot,
                follows_me_depth,
                d2_follows_depth,
                d2_refresh_age_h,
            )
            .await
        }
        Job::Refresh(viewer) => {
            run_refresh(
                &viewer,
                handle,
                queue,
                store,
                source,
                d2_sample_size,
                drop_lists,
                retry_delay,
                snapshot,
                follows_me_depth,
            )
            .await
        }
        Job::Refill(account) => {
            run_refill(
                &account,
                handle,
                queue,
                store,
                source,
                d2_follows_depth,
                drop_lists,
                retry_delay,
            )
            .await
        }
    }
}

/// One `FirstBuild` job attempt (story 08 BC5c): a `building_d2` circle — a
/// restart re-enqueue (BC12a) or one already swapped in by this same
/// attempt's [`run_step2`] success — runs [`run_step3`] only, no step 1 or
/// step 2. Otherwise, if the viewer's in-memory circle is still fresh
/// (`CircleState::BuildingD1`), runs step 1 first ([`run_step1`]); otherwise
/// (a `building_fm` restart re-enqueue, BC9a, or a step 2 retry, BC4a) the
/// existing in-memory circle already carries step 1's data, so step 1 is not
/// run again. Either way, [`run_step2`] runs next over whatever circle came
/// out of that first part, unless step 1 failed (`run_step1` already handed
/// off to [`handle_step1_failure`] in that case and this returns without
/// touching step 2) — [`run_step2`] itself runs step 3 when it succeeds
/// (BC5a), so a single job attempt never returns to this function partway
/// through.
#[allow(clippy::too_many_arguments)]
async fn process_first_build<S: GraphSource>(
    viewer: &ViewerDid,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    source: &S,
    d2_sample_size: usize,
    drop_lists: Option<&DropListsFn>,
    retry_delay: Duration,
    snapshot: &SnapshotHandle,
    follows_me_depth: usize,
    d2_follows_depth: u32,
    d2_refresh_age_h: u32,
) {
    let state = handle.get(viewer).map(|c| c.state).unwrap_or(CircleState::BuildingD1);

    if state == CircleState::BuildingD2 {
        run_step3(
            viewer,
            handle,
            queue,
            store,
            source,
            d2_follows_depth,
            d2_refresh_age_h,
            drop_lists,
        )
        .await;
        return;
    }

    let needs_step1 = state == CircleState::BuildingD1;

    let circle = if needs_step1 {
        match run_step1(
            viewer,
            handle,
            queue,
            store,
            source,
            d2_sample_size,
            drop_lists,
            retry_delay,
        )
        .await
        {
            Some(circle) => circle,
            None => return,
        }
    } else {
        match handle.get(viewer) {
            Some(circle) => (*circle).clone(),
            // BC6a: the viewer was removed (a concurrent give-up or
            // eviction) between the queue pop and this read; nothing left
            // to build for it. BC6b: if a fresh request already recreated
            // a circle for it, that circle still needs a job.
            None => {
                abandon_evicted_job(handle, queue, viewer);
                return;
            }
        }
    };

    run_step2(
        viewer,
        handle,
        queue,
        store,
        source,
        circle,
        snapshot,
        follows_me_depth,
        drop_lists,
        retry_delay,
        d2_follows_depth,
        d2_refresh_age_h,
    )
    .await;
}

/// Step 1 of one first-build job attempt: writes the `building_d1` row
/// (BC7a), runs [`step_follows`], and on success saves the circle as
/// `building_fm` (BC3a), swaps it into `handle`, clears the step 1
/// failed-attempt count and calls `drop_lists`. Returns the swapped-in
/// circle for [`process_first_build`] to hand to [`run_step2`]. On any
/// failure — a `PdsError` from `step_follows`, or a `StoreError` from either
/// save (BC24) — hands off to [`handle_step1_failure`] (BC8 as amended,
/// defect W) and returns `None`. Checks `handle` still holds a circle for
/// `viewer` before each of its two saves (story 09 spec.md BC6a): an
/// eviction (`GraphHandle::evict`) may have removed it while this attempt
/// waited its turn or ran, and neither save is allowed to recreate it —
/// [`abandon_evicted_job`] handles that case (and BC6b) instead, and this
/// also returns `None`.
#[allow(clippy::too_many_arguments)]
async fn run_step1<S: GraphSource>(
    viewer: &ViewerDid,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    source: &S,
    d2_sample_size: usize,
    drop_lists: Option<&DropListsFn>,
    retry_delay: Duration,
) -> Option<Circle> {
    if handle.get(viewer).is_none() {
        // BC6a: evicted before step 1 ever started saving.
        abandon_evicted_job(handle, queue, viewer);
        return None;
    }

    let start_now = unix_now();
    if let Err(err) =
        store.viewer_save_state(&viewer.0, CircleState::BuildingD1.as_str(), start_now)
    {
        tracing::warn!(kind = ?err, "graph: worker failed to save building_d1 state");
        handle_step1_failure(viewer, handle, queue, store, drop_lists, retry_delay).await;
        return None;
    }

    let mut circle = Circle::new();
    if let Err(err) =
        step_follows(source, &viewer.0, d2_sample_size, &mut circle, Some(FIRST_BUILD_MAX_PAGES))
            .await
    {
        tracing::warn!(kind = ?err, "graph: worker step_follows failed");
        handle_step1_failure(viewer, handle, queue, store, drop_lists, retry_delay).await;
        return None;
    }

    if handle.get(viewer).is_none() {
        // BC6a: evicted while `step_follows` ran.
        abandon_evicted_job(handle, queue, viewer);
        return None;
    }

    let saved_at = unix_now();
    if let Err(err) = store.viewer_save_circle(
        &viewer.0,
        CircleState::BuildingFm.as_str(),
        saved_at,
        saved_at,
        &circle.d2_sample,
        &circle.follows,
    ) {
        tracing::warn!(kind = ?err, "graph: worker failed to save circle");
        handle_step1_failure(viewer, handle, queue, store, drop_lists, retry_delay).await;
        return None;
    }

    circle.d1_refreshed_at = Some(saved_at);
    circle.last_request_at = saved_at;
    handle.swap_circle(viewer, circle.clone(), CircleState::BuildingFm);
    queue.clear_attempts(viewer);
    if let Some(drop_lists) = drop_lists {
        drop_lists(&viewer.0);
    }
    Some(circle)
}

/// Step 1's failure path (BC8 as amended, review round 1, defect W): below
/// [`MAX_FIRST_BUILD_ATTEMPTS`] failures, re-queues `viewer`'s `FirstBuild`
/// job after `retry_delay` exactly as before; at the limit, gives up instead
/// — deletes `viewer`'s SQLite row (`viewer_delete`), removes its in-memory
/// circle and starts its [`REMOVAL_COOLDOWN_SECS`] cooldown (freeing the
/// `UPSTAGE_MAX_VIEWERS` slot `GraphHandle::enqueue_first_build` counts
/// against), drops its cached lists, and logs one `warn` naming the attempt
/// count and no DID (BC21).
async fn handle_step1_failure(
    viewer: &ViewerDid,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    drop_lists: Option<&DropListsFn>,
    retry_delay: Duration,
) {
    let attempts = queue.record_failure(viewer);
    if attempts < MAX_FIRST_BUILD_ATTEMPTS {
        schedule_retry(queue, Job::FirstBuild(viewer.clone()), retry_delay);
        return;
    }

    tracing::warn!(
        attempts,
        "graph: worker giving up on first build after repeated failures, removing viewer"
    );
    // The in-memory circle and cooldown are removed and started regardless
    // of whether the SQLite row could be deleted — a viewer that keeps its
    // row past give-up must still stop occupying an `UPSTAGE_MAX_VIEWERS`
    // slot and still be blocked from a new first build during its cooldown.
    // A failed delete here is not retried (review round 3, defect AC,
    // accepted as a known limitation, defect AD): if the process restarts
    // before a later delete would have succeeded, `GraphHandle::from_store`
    // reloads the row as an ordinary `building_d1` job and re-enqueues it,
    // so the viewer gets up to `MAX_FIRST_BUILD_ATTEMPTS` more attempts
    // rather than staying stuck.
    if let Err(err) = store.viewer_delete(&viewer.0) {
        tracing::warn!(kind = ?err, "graph: failed to delete viewer row after giving up");
    }
    handle.remove_after_giving_up(viewer, unix_now(), REMOVAL_COOLDOWN_SECS);
    queue.complete(&Job::FirstBuild(viewer.clone()));
    queue.clear_attempts(viewer);
    if let Some(drop_lists) = drop_lists {
        drop_lists(&viewer.0);
    }
}

/// Step 2's candidates (BC1, BC1a): the quoter and original DIDs of the
/// first `follows_me_depth` items of `snapshot`'s current, uncapped list, in
/// rank order, read from `feed` by `quote_uri`. An item whose `quote_uri` no
/// longer has a `feed` row — demoted or dropped after this snapshot was
/// built — is silently skipped, not an error (`Store::feed_authors_by_quote_uri`
/// already drops it). `step_follows_me` itself does the rest of BC1: minus
/// the viewer, `circle.follows` and `circle.checked`, deduplicated. Shared
/// by a first build's [`run_step2`] and a refresh's [`run_refresh`] (story
/// 09).
fn step2_candidates(
    store: &Store,
    snapshot: &SnapshotHandle,
    follows_me_depth: usize,
) -> Result<Vec<RankedAuthors>, StoreError> {
    let current = snapshot.current();
    let candidate_items = &current.items[..follows_me_depth.min(current.items.len())];
    if candidate_items.is_empty() {
        return Ok(Vec::new());
    }

    let quote_uris: Vec<&str> =
        candidate_items.iter().map(|item| item.quote_uri.as_str()).collect();
    let rows = store.feed_authors_by_quote_uri(&quote_uris)?;
    let by_uri: HashMap<&str, &crate::store::feed::FeedAuthors> =
        rows.iter().map(|row| (row.quote_uri.as_str(), row)).collect();

    Ok(candidate_items
        .iter()
        .filter_map(|item| {
            by_uri.get(item.quote_uri.as_str()).map(|row| RankedAuthors {
                quote_did: row.quote_did.clone(),
                original_did: row.original_did.clone(),
            })
        })
        .collect())
}

/// Step 2 of one first-build job attempt, over `circle` (fresh from step 1,
/// or the existing in-memory circle when resuming, BC4a). Waits rather than
/// running when `snapshot`'s current generation is still `0` (review round
/// 1, defect AE): that generation is `SnapshotHandle::new`'s placeholder,
/// before the scorer's first pass has ever called `swap`, so there are no
/// ranked items to read candidates from yet. `step2_candidates` cannot tell
/// that case apart from a real pass that happened to produce an empty list
/// (BC1b), so this checks the generation itself before calling it,
/// re-queues `viewer`'s `FirstBuild` job after `retry_delay` and returns
/// without touching the step 2 failure count or the circle — it is left
/// exactly as `run_step1`'s swap or the restart load
/// (`GraphHandle::from_store`) already set it, still `building_fm`. Story
/// 08's step 3 (BC5d) never runs before this returns, since it has not run
/// yet either.
///
/// Once a real snapshot exists, reads [`step2_candidates`], runs
/// [`step_follows_me`], then always tries to save whatever `checked` and
/// `follows_me` came out of that — even a partial result from a `PdsError`
/// part way through (BC4) — as `viewer_checks` in one transaction (BC3,
/// BC3a: step 1's data is untouched either way). On a clean success, the
/// saved and swapped-in state is `building_d2`, not `ready` (story 08 BC5a):
/// the circle swaps in as [`CircleState::BuildingD2`] and [`run_step3`] runs
/// next, in this same job attempt, rather than completing the job here. A
/// `PdsError` part way through `step_follows_me` (BC4) still saves and swaps
/// in whatever was checked so far, but as `ready` (story 08 BC5b: step 3
/// does not run for this attempt) and the job is retried. The save's own
/// failure (BC4c) is handled the same as a `step_follows_me` failure, except
/// the in-memory circle keeps whatever last saved instead of swapping in
/// this attempt's (unsaved) result.
#[allow(clippy::too_many_arguments)]
async fn run_step2<S: GraphSource>(
    viewer: &ViewerDid,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    source: &S,
    mut circle: Circle,
    snapshot: &SnapshotHandle,
    follows_me_depth: usize,
    drop_lists: Option<&DropListsFn>,
    retry_delay: Duration,
    d2_follows_depth: u32,
    d2_refresh_age_h: u32,
) {
    if handle.get(viewer).is_none() {
        // Story 09 spec.md BC6a: evicted before step 2 (or its resumed
        // retry) ever started.
        abandon_evicted_job(handle, queue, viewer);
        return;
    }

    if snapshot.current().generation == 0 {
        // Defect AE: no scorer pass has swapped a real snapshot in yet.
        // Not a failure — just nothing to check against yet.
        schedule_retry(queue, Job::FirstBuild(viewer.clone()), retry_delay);
        return;
    }

    let ranked = match step2_candidates(store, snapshot, follows_me_depth) {
        Ok(ranked) => ranked,
        Err(err) => {
            tracing::warn!(kind = ?err, "graph: worker failed to read step 2 candidates");
            handle_step2_failure(viewer, queue, retry_delay, handle, store).await;
            return;
        }
    };

    let depth = ranked.len();
    let step_result = step_follows_me(source, &viewer.0, &ranked, depth, &mut circle).await;

    if handle.get(viewer).is_none() {
        // BC6a: evicted while `step_follows_me` ran, before this attempt's
        // own save.
        abandon_evicted_job(handle, queue, viewer);
        return;
    }

    // Story 08 BC5a, BC5b: a clean step 2 saves and swaps in as
    // `building_d2` (step 3 runs next); a `PdsError` part way through still
    // saves and swaps in as `ready`, so this attempt's job is retried
    // without step 3 running.
    let saved_state =
        if step_result.is_ok() { CircleState::BuildingD2 } else { CircleState::Ready };
    let now = unix_now();
    let save_result = store.viewer_save_checks(
        &viewer.0,
        saved_state.as_str(),
        now,
        &circle.checked,
        &circle.follows_me,
    );

    match (step_result, save_result) {
        (Ok(_), Ok(())) => {
            // BC5a: step 2 completed and saved cleanly, as building_d2.
            handle.swap_circle(viewer, circle, CircleState::BuildingD2);
            queue.clear_attempts(viewer);
            queue.clear_step2_attempts(viewer);
            if let Some(drop_lists) = drop_lists {
                drop_lists(&viewer.0);
            }
            run_step3(
                viewer,
                handle,
                queue,
                store,
                source,
                d2_follows_depth,
                d2_refresh_age_h,
                drop_lists,
            )
            .await;
        }
        (Err(err), Ok(())) => {
            // BC4: a `PdsError` part way through — whatever was checked
            // before the failure still saved and swaps in as `ready`, but
            // the job is still retried and step 3 does not run (BC5b).
            tracing::warn!(kind = ?err, "graph: worker step_follows_me failed");
            handle.insert_ready(viewer, circle);
            if let Some(drop_lists) = drop_lists {
                drop_lists(&viewer.0);
            }
            handle_step2_failure(viewer, queue, retry_delay, handle, store).await;
        }
        (Ok(_), Err(save_err)) => {
            // BC4c: the save itself failed. Nothing new is kept in memory;
            // the last successfully saved data (from an earlier attempt, or
            // step 1's own save) stays as-is unless give-up promotes it
            // (defect AH: never this attempt's unsaved `circle`).
            tracing::warn!(kind = ?save_err, "graph: worker failed to save step 2 checks");
            handle_step2_failure(viewer, queue, retry_delay, handle, store).await;
        }
        (Err(err), Err(save_err)) => {
            tracing::warn!(kind = ?err, "graph: worker step_follows_me failed");
            tracing::warn!(kind = ?save_err, "graph: worker failed to save partial step 2 checks");
            handle_step2_failure(viewer, queue, retry_delay, handle, store).await;
        }
    }
}

/// Step 2's failure path (BC4b as amended, review round 1 defect AF, review
/// round 2 defects AH and AI): below [`MAX_FIRST_BUILD_ATTEMPTS`] step 2
/// failures in a row, re-queues `viewer`'s `FirstBuild` job after
/// `retry_delay`, touching neither the in-memory circle nor its saved row —
/// `run_step2` already left both exactly as its own save attempt did or did
/// not change them. At the limit, stops retrying step 2 instead — but,
/// unlike [`handle_step1_failure`], the viewer already has a servable circle
/// (BC8, BC8a) — so give-up promotes whatever circle `handle` still holds
/// for `viewer` to `CircleState::Ready` (`GraphHandle::mark_ready`, defect
/// AH): never this attempt's own `circle` parameter, since a
/// `viewer_save_checks` failure (BC4c) means that value's
/// `checked`/`follows_me` never reached SQLite and must not leak into
/// memory just because the worker gives up retrying it. `mark_ready` is a
/// no-op when `handle` already has no circle for `viewer` (defect AI: e.g. a
/// concurrent step 1 give-up already removed it), and the matching
/// `viewers.state = 'ready'` write uses `Store::viewer_set_state_if_exists`,
/// a plain `UPDATE`, for the same reason — neither call recreates a viewer
/// the store or the handle no longer has. A failed write is logged with no
/// DID and left as-is, the same accepted limitation
/// `handle_step1_failure`'s own best-effort delete already documents, since
/// a restart's `GraphHandle::from_store` would simply re-enqueue a row still
/// `building_fm` and retry step 2 again rather than getting stuck.
async fn handle_step2_failure(
    viewer: &ViewerDid,
    queue: &Arc<JobQueue>,
    retry_delay: Duration,
    handle: &Arc<GraphHandle>,
    store: &Store,
) {
    let attempts = queue.record_step2_failure(viewer);
    if attempts < MAX_FIRST_BUILD_ATTEMPTS {
        schedule_retry(queue, Job::FirstBuild(viewer.clone()), retry_delay);
        return;
    }

    tracing::warn!(attempts, "graph: worker giving up on step 2 retries after repeated failures");
    handle.mark_ready(viewer);
    if let Err(err) = store.viewer_set_state_if_exists(&viewer.0, CircleState::Ready.as_str()) {
        tracing::warn!(kind = ?err, "graph: failed to save ready state after giving up on step 2");
    }
    queue.complete(&Job::FirstBuild(viewer.clone()));
    queue.clear_step2_attempts(viewer);
}

/// Step 3 of one first-build job attempt (story 08 spec.md `## Approach`,
/// BC1-BC5): reads the current in-memory circle's `d2_sample` — set by step
/// 1 and untouched since (BC10) — deduplicates it (BC3a: one account named
/// twice costs one fetch), and for each distinct account asks the shared
/// [`FollowsCache`] whether it already has a fresh entry (BC1b, BC2, BC3).
/// An account with no entry, or a stale one, is fetched through
/// [`fetch_account_follows`] at `d2_follows_depth` (BC1), saved to SQLite
/// (`Store::follows_put`) and, only on that save's success, put in the
/// shared cache (BC4a: a `StoreError` here must never let memory hold a list
/// SQLite lacks) — and, right after that put, `drop_lists` is called for
/// `viewer` (defect AJ, BC11b): a request that lands while the circle is
/// still `building_d2` rebuilds its list against every degree-2 entry made
/// fresh so far, one entry at a time, rather than only once the whole loop
/// finishes. A `PdsError` fetching one account, or a `StoreError` reading
/// its freshness or saving its fetch, is logged with no DID (BC14) and the
/// loop moves to the next account (BC4): that account keeps whatever entry
/// it had before, stale or none, and no retry job is queued for it, and
/// `drop_lists` is not called for it either — nothing new was put. Once
/// every account has been tried, saves `viewers.state = 'ready'` best
/// effort (`Store::viewer_set_state_if_exists`; a failure is logged with no
/// DID and left as-is, BC4b — a restart's `GraphHandle::from_store`
/// re-enqueues the still-`building_d2` row and runs step 3 again, skipping
/// every account step 3 already made fresh), promotes the in-memory circle
/// to `Ready` with a new `circle_version` (`GraphHandle::mark_ready`, BC5),
/// drops the viewer's cached lists once more, completes the job and clears
/// both attempt counts (BC5: step 3 itself has no retry or give-up count of
/// its own, spec.md `## Defaults taken`, but clearing here is a no-op
/// unless a stray count from an earlier attempt is still set).
#[allow(clippy::too_many_arguments)]
async fn run_step3<S: GraphSource>(
    viewer: &ViewerDid,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    source: &S,
    d2_follows_depth: u32,
    d2_refresh_age_h: u32,
    drop_lists: Option<&DropListsFn>,
) {
    let Some(circle) = handle.get(viewer) else {
        // Story 09 spec.md BC6a: evicted before step 3 ever started.
        // `mark_ready` and `viewer_set_state_if_exists` below are already
        // no-ops for a missing viewer (defect AI), but this also skips the
        // per-account fetch loop entirely rather than doing pointless work
        // for a circle nothing will ever read again.
        abandon_evicted_job(handle, queue, viewer);
        return;
    };
    let d2_sample: Vec<String> = circle.d2_sample.clone();
    let cache = handle.follows_cache();

    let mut seen: HashSet<&str> = HashSet::new();
    for account in &d2_sample {
        if !seen.insert(account.as_str()) {
            continue; // BC3a: one account named twice in d2_sample, one fetch.
        }

        let now = unix_now();
        let is_fresh = match cache.get(store, account) {
            Ok(Some((fetched_at, _))) => FollowsCache::is_fresh(now, fetched_at, d2_refresh_age_h),
            Ok(None) => false,
            Err(err) => {
                tracing::warn!(kind = ?err, "graph: step 3 failed to read a follows_cache entry");
                false
            }
        };
        if is_fresh {
            continue; // BC2: a fresh entry costs no call.
        }

        let (hashed, _calls) = match fetch_account_follows(source, account, d2_follows_depth).await
        {
            Ok(result) => result,
            Err(err) => {
                tracing::warn!(kind = ?err, "graph: step 3 failed to fetch one account's follows");
                continue; // BC4: one failed account does not stop the loop.
            }
        };

        if let Err(err) = store.follows_put(account, now, &hashed) {
            // BC4a: not put in memory, so memory never holds a list SQLite
            // lacks.
            tracing::warn!(kind = ?err, "graph: step 3 failed to save one account's follows");
            continue;
        }
        cache.put(account, now, hashed);
        // Defect AJ: drop the viewer's cached list after each entry this
        // loop puts, not only once at the end, so a request that lands
        // while the circle is still `building_d2` (BC11b) rebuilds against
        // every degree-2 entry step 3 has made fresh so far, rather than
        // waiting for the whole loop to finish.
        if let Some(drop_lists) = drop_lists {
            drop_lists(&viewer.0);
        }
    }

    if let Err(err) = store.viewer_set_state_if_exists(&viewer.0, CircleState::Ready.as_str()) {
        tracing::warn!(kind = ?err, "graph: step 3 failed to save the ready state");
    }
    handle.mark_ready(viewer);
    if let Some(drop_lists) = drop_lists {
        drop_lists(&viewer.0);
    }
    queue.complete(&Job::FirstBuild(viewer.clone()));
    queue.clear_attempts(viewer);
    queue.clear_step2_attempts(viewer);
}

/// A `Refresh` job (story 09 spec.md BC4, BC5, BC6): checks the viewer still
/// has a circle in memory (BC6a — an eviction may have removed it while this
/// job waited its turn), reads step 2's candidates off `snapshot`, waiting
/// (no failure) while its generation is still `0` (the same defect AE rule
/// [`run_step2`] applies), then runs [`refresh`] — steps 1 and 2 on a brand
/// new [`Circle`] with empty `checked` and `follows_me`, so a follow dropped
/// since the last build actually leaves the result (BC13). On success,
/// re-checks the circle still exists (BC6a), saves the new `follows`,
/// `d2_sample`, `checked`, `follows_me` and `d1_refreshed_at = now` through
/// `viewer_replace_circle` in one transaction, then swaps the result into
/// memory keeping the existing circle's `last_request_at`
/// (`GraphHandle::replace_if_present`, BC5) and drops the viewer's cached
/// lists. On any failure — reading candidates, `refresh` itself, or the
/// save — the old circle is left completely untouched in memory and SQLite,
/// and the job is retried after `retry_delay` with no give-up count (BC1b,
/// BC6).
#[allow(clippy::too_many_arguments)]
async fn run_refresh<S: GraphSource>(
    viewer: &ViewerDid,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    source: &S,
    d2_sample_size: usize,
    drop_lists: Option<&DropListsFn>,
    retry_delay: Duration,
    snapshot: &SnapshotHandle,
    follows_me_depth: usize,
) {
    if handle.get(viewer).is_none() {
        // BC6a: the circle was evicted while this job waited its turn.
        // Nothing to refresh; the job completes without saving or swapping
        // anything.
        queue.complete(&Job::Refresh(viewer.clone()));
        return;
    }

    if snapshot.current().generation == 0 {
        // Defect AE, applied to a refresh: no scorer pass has swapped a
        // real snapshot in yet. Not a failure — retry once one exists.
        schedule_retry(queue, Job::Refresh(viewer.clone()), retry_delay);
        return;
    }

    let ranked = match step2_candidates(store, snapshot, follows_me_depth) {
        Ok(ranked) => ranked,
        Err(err) => {
            tracing::warn!(kind = ?err, "graph: refresh failed to read step 2 candidates");
            schedule_retry(queue, Job::Refresh(viewer.clone()), retry_delay);
            return;
        }
    };
    let depth = ranked.len();

    let mut circle = match refresh(source, &viewer.0, d2_sample_size, &ranked, depth).await {
        Ok(circle) => circle,
        Err(err) => {
            // BC6: a `PdsError` from either of `refresh`'s steps. The old
            // circle is untouched; nothing here has saved or swapped
            // anything.
            tracing::warn!(kind = ?err, "graph: refresh failed");
            schedule_retry(queue, Job::Refresh(viewer.clone()), retry_delay);
            return;
        }
    };

    if handle.get(viewer).is_none() {
        // BC6a: evicted while `refresh` ran. Save nothing.
        queue.complete(&Job::Refresh(viewer.clone()));
        return;
    }

    let now = unix_now();
    if let Err(err) = store.viewer_replace_circle(
        &viewer.0,
        CircleState::Ready.as_str(),
        now,
        now,
        &circle.d2_sample,
        &circle.follows,
        &circle.checked,
        &circle.follows_me,
    ) {
        tracing::warn!(kind = ?err, "graph: refresh failed to save the new circle");
        schedule_retry(queue, Job::Refresh(viewer.clone()), retry_delay);
        return;
    }

    circle.d1_refreshed_at = Some(now);
    if handle.replace_if_present(viewer, circle).is_some() {
        if let Some(drop_lists) = drop_lists {
            drop_lists(&viewer.0);
        }
    }
    queue.complete(&Job::Refresh(viewer.clone()));
}

/// A `Refill` job (story 09 spec.md BC7a, BC7b): fetches `account`'s newest
/// follows through [`fetch_account_follows`] at `d2_follows_depth`, saves
/// them to SQLite (`Store::follows_put`) and, only on that save's success,
/// puts the result in the shared [`FollowsCache`] (the same BC4a ordering
/// [`run_step3`] follows), then drops the cached list of every viewer whose
/// in-memory circle's `d2_sample` names `account`
/// (`GraphHandle::viewers_naming_account`). A `PdsError` from the fetch or a
/// `StoreError` from the save leaves the cache entry — in memory and in
/// SQLite — exactly as it was, logs with no DID, and retries after
/// `retry_delay` with no give-up count (BC1b).
#[allow(clippy::too_many_arguments)]
async fn run_refill<S: GraphSource>(
    account: &str,
    handle: &Arc<GraphHandle>,
    queue: &Arc<JobQueue>,
    store: &Store,
    source: &S,
    d2_follows_depth: u32,
    drop_lists: Option<&DropListsFn>,
    retry_delay: Duration,
) {
    let now = unix_now();
    let (hashed, _calls) = match fetch_account_follows(source, account, d2_follows_depth).await {
        Ok(result) => result,
        Err(err) => {
            tracing::warn!(kind = ?err, "graph: refill failed to fetch an account's follows");
            schedule_retry(queue, Job::Refill(account.to_string()), retry_delay);
            return;
        }
    };

    if let Err(err) = store.follows_put(account, now, &hashed) {
        tracing::warn!(kind = ?err, "graph: refill failed to save an account's follows");
        schedule_retry(queue, Job::Refill(account.to_string()), retry_delay);
        return;
    }

    handle.follows_cache().put(account, now, hashed);
    let affected = handle.viewers_naming_account(account);
    if let Some(drop_lists) = drop_lists {
        for viewer in &affected {
            drop_lists(&viewer.0);
        }
    }
    queue.complete(&Job::Refill(account.to_string()));
}

/// Spawns a task that waits `retry_delay`, then re-queues `job` (BC1b, BC8).
/// A separate task rather than an inline sleep, so the worker's own loop
/// keeps draining other jobs while this one waits.
fn schedule_retry(queue: &Arc<JobQueue>, job: Job, retry_delay: Duration) {
    let queue = Arc::clone(queue);
    tokio::spawn(async move {
        tokio::time::sleep(retry_delay).await;
        queue.retry(job);
    });
}

/// Runs forever, taking one job at a time from `handle`'s queue and
/// dispatching it to its own path (BC1, BC1a; story 08 BC1-BC5; story 09
/// BC4-BC7b). `run` (`src/ingest/mod.rs`) spawns this once, on its own task,
/// when `UPSTAGE_PERSONALISE` is `true` and BSKY credentials exist (BC18).
/// `snapshot` is step 2's (and a refresh's) source of candidates (BC1) and
/// `follows_me_depth` is `cfg.follows_me_depth`, the number of the current
/// snapshot's top items considered. `d2_follows_depth` is
/// `cfg.d2_follows_depth`, step 3's and a refill's per-account fetch depth
/// (story 08 BC1), and `d2_refresh_age_h` is `cfg.d2_refresh_age_h`, step
/// 3's freshness window (story 08 BC1a).
#[allow(clippy::too_many_arguments)]
pub async fn run_worker<S: GraphSource>(
    handle: Arc<GraphHandle>,
    store: Store,
    source: S,
    d2_sample_size: usize,
    drop_lists: Option<DropListsFn>,
    snapshot: SnapshotHandle,
    follows_me_depth: usize,
    d2_follows_depth: u32,
    d2_refresh_age_h: u32,
) {
    run_worker_with_retry_delay(
        handle,
        store,
        source,
        d2_sample_size,
        drop_lists,
        RETRY_DELAY,
        snapshot,
        follows_me_depth,
        d2_follows_depth,
        d2_refresh_age_h,
    )
    .await
}

/// [`run_worker`]'s body, parameterised over the retry delay: this
/// repository carries no `tokio` `test-util` feature (`Cargo.toml` is out
/// of this slice's files), so a test shrinks `retry_delay` to see a retry
/// in milliseconds rather than waiting out the real 30 s.
#[allow(clippy::too_many_arguments)]
async fn run_worker_with_retry_delay<S: GraphSource>(
    handle: Arc<GraphHandle>,
    store: Store,
    source: S,
    d2_sample_size: usize,
    drop_lists: Option<DropListsFn>,
    retry_delay: Duration,
    snapshot: SnapshotHandle,
    follows_me_depth: usize,
    d2_follows_depth: u32,
    d2_refresh_age_h: u32,
) {
    let queue = handle.queue();
    loop {
        let job = queue.pop().await;
        process_job(
            job,
            &handle,
            &queue,
            &store,
            &source,
            d2_sample_size,
            drop_lists.as_ref(),
            retry_delay,
            &snapshot,
            follows_me_depth,
            d2_follows_depth,
            d2_refresh_age_h,
        )
        .await;
    }
}

/// Runs forever, writing every viewer's due in-memory touch to SQLite
/// (BC23): at most once every 60 s per viewer, off `handle`'s own
/// snapshot, never on the request path. `run` spawns this alongside
/// [`run_worker`]. Story 09's scheduler (`graph::schedule`) folds this same
/// flush into its own periodic pass and replaces this task once it exists.
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
pub(crate) mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::appview::pds::PdsError;
    use crate::graph::build::FollowsPage;
    use crate::graph::hash_did;
    use crate::scorer::snapshot::FeedItem;
    use crate::store::feed;
    use crate::store::writer::{Op, WriterConfig};

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
    /// calls, for [`retries_after_the_configured_delay`] (BC8).
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
    fn push_twice_for_the_same_job_is_one_job() {
        // BC1a: a second push of the same job already queued adds no
        // second FIFO entry.
        let queue = JobQueue::new();
        let viewer = ViewerDid("did:plc:a".to_string());
        let job = Job::FirstBuild(viewer);

        assert!(queue.push(job.clone()));
        assert!(!queue.push(job.clone()));

        assert_eq!(queue.try_pop(), Some(job));
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn push_after_complete_starts_a_fresh_job() {
        let queue = JobQueue::new();
        let job = Job::FirstBuild(ViewerDid("did:plc:a".to_string()));

        assert!(queue.push(job.clone()));
        queue.try_pop();
        queue.complete(&job);

        assert!(queue.push(job));
    }

    #[test]
    fn push_while_retrying_still_dedupes() {
        // BC1b: `retry` keeps the job's outstanding mark, so a push of the
        // same job during the retry wait is still a no-op.
        let queue = JobQueue::new();
        let job = Job::FirstBuild(ViewerDid("did:plc:a".to_string()));

        queue.push(job.clone());
        queue.try_pop();
        queue.retry(job.clone());

        assert!(!queue.push(job));
    }

    #[test]
    fn priority_order() {
        // BC1: with jobs in every lane, `pop` drains `FirstBuild` first,
        // then `Refresh`, then `Refill`, FIFO within a lane.
        let queue = JobQueue::new();
        let refill_a = Job::Refill("did:plc:account-a".to_string());
        let refill_b = Job::Refill("did:plc:account-b".to_string());
        let refresh_a = Job::Refresh(ViewerDid("did:plc:refresh-a".to_string()));
        let refresh_b = Job::Refresh(ViewerDid("did:plc:refresh-b".to_string()));
        let build_a = Job::FirstBuild(ViewerDid("did:plc:build-a".to_string()));
        let build_b = Job::FirstBuild(ViewerDid("did:plc:build-b".to_string()));

        // Pushed out of priority order on purpose.
        queue.push(refill_a.clone());
        queue.push(refresh_a.clone());
        queue.push(build_a.clone());
        queue.push(refill_b.clone());
        queue.push(refresh_b.clone());
        queue.push(build_b.clone());

        assert_eq!(queue.try_pop(), Some(build_a));
        assert_eq!(queue.try_pop(), Some(build_b));
        assert_eq!(queue.try_pop(), Some(refresh_a));
        assert_eq!(queue.try_pop(), Some(refresh_b));
        assert_eq!(queue.try_pop(), Some(refill_a));
        assert_eq!(queue.try_pop(), Some(refill_b));
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn first_build_jumps_refresh() {
        // AC2: a `FirstBuild` queued after a `Refresh` still pops first —
        // lane order beats arrival order.
        let queue = JobQueue::new();
        let refresh = Job::Refresh(ViewerDid("did:plc:refresh".to_string()));
        let build = Job::FirstBuild(ViewerDid("did:plc:build".to_string()));

        queue.push(refresh.clone());
        queue.push(build.clone());

        assert_eq!(queue.try_pop(), Some(build));
        assert_eq!(queue.try_pop(), Some(refresh));
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
        // Defect AE: step 2 waits at generation 0. Swap in an (empty, but
        // real) pass first so step 2 does not stall waiting for the scorer.
        let snapshot = SnapshotHandle::new();
        snapshot.swap(Arc::new(Vec::new()), Arc::new(Vec::new()));
        tokio::spawn(run_worker(
            worker_handle,
            memory_store(),
            source,
            100,
            None,
            snapshot,
            10,
            100,
            24,
        ));

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
        assert_eq!(job, Job::FirstBuild(viewer.clone()));

        process_job(
            job,
            &handle,
            &queue,
            &memory_store(),
            &source,
            10,
            None,
            Duration::from_millis(20),
            &SnapshotHandle::new(),
            10,
            100,
            24,
        )
        .await;

        assert_eq!(handle.get(&viewer).unwrap().state, CircleState::BuildingD1);
        assert_eq!(*source.calls.lock().unwrap(), 1);
        assert!(queue.try_pop().is_none(), "the retry has not fired yet");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            queue.try_pop(),
            Some(Job::FirstBuild(viewer)),
            "the job was re-queued after the retry delay"
        );
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
            assert_eq!(job, Job::FirstBuild(viewer.clone()));
            process_job(
                job,
                &handle,
                &queue,
                &store,
                &source,
                10,
                None,
                Duration::from_millis(5),
                &SnapshotHandle::new(),
                10,
                100,
                24,
            )
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
            job,
            &handle,
            &queue,
            &read_only_store,
            &source,
            10,
            None,
            Duration::from_millis(20),
            &SnapshotHandle::new(),
            10,
            100,
            24,
        )
        .await;

        assert_eq!(handle.get(&viewer).unwrap().state, CircleState::BuildingD1);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(queue.try_pop(), Some(Job::FirstBuild(viewer)));

        let _ = std::fs::remove_file(&path_str);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }

    /// A minimal `FeedItem` naming `quote_uri`, for the step 2 candidate
    /// tests below: only `quote_uri` matters to `step2_candidates`, which
    /// re-reads the author DIDs from `feed` rather than from the item
    /// itself.
    fn feed_item(quote_uri: &str) -> FeedItem {
        FeedItem {
            quote_uri: quote_uri.to_string(),
            quote_cid: "cid".to_string(),
            rank: 1.0,
            ratio: 1.0,
            quote_did: 0,
            original_did: 0,
            quoted_at: 0,
            promoted_at: 0,
        }
    }

    fn feed_row(quote_uri: &str, quote_did: &str, original_did: &str, now: i64) -> feed::FeedRow {
        feed::FeedRow {
            quote_uri: quote_uri.to_string(),
            quote_cid: format!("cid-{quote_uri}"),
            quote_did: quote_did.to_string(),
            original_did: original_did.to_string(),
            quoted_at: now,
            v_likes_q: 0,
            v_reposts_q: 0,
            v_replies_q: 0,
            v_likes_o: 0,
            v_reposts_o: 0,
            v_replies_o: 0,
            ratio: 1.0,
            rank: 1.0,
            promoted_at: now,
            verified_at: now,
        }
    }

    /// Writes a `pairs` row and promotes it to `feed`, through the writer
    /// and `Store::promote` (AGENTS.md: no raw SQL outside `src/store/`),
    /// for the step 2 candidate tests below.
    #[allow(clippy::too_many_arguments)]
    async fn seed_feed_row(
        store: &Store,
        writer: &crate::store::writer::WriterHandle,
        quote_uri: &str,
        quote_did: &str,
        original_did: &str,
        now: i64,
        seq: u64,
    ) {
        writer
            .send(Op::InsertPair {
                quote_uri: quote_uri.to_string(),
                quote_did: quote_did.to_string(),
                quote_cid: format!("cid-{quote_uri}"),
                original_uri: format!("at://{original_did}/app.bsky.feed.post/o"),
                original_did: original_did.to_string(),
                quoted_at: now,
                first_seen_at: now,
                seq,
            })
            .await
            .unwrap();
        writer.flush().await.unwrap();
        store.promote(&feed_row(quote_uri, quote_did, original_did, now)).unwrap();
    }

    fn test_writer(store: &Store) -> crate::store::writer::WriterHandle {
        store
            .writer_with(WriterConfig {
                capacity: 100,
                max_ops: 1,
                interval: Duration::from_millis(5),
            })
            .unwrap()
    }

    #[tokio::test]
    async fn step2_candidates() {
        // AC1, BC1, BC1a: the first `follows_me_depth` items' quoter and
        // original DIDs, in rank order, read from `feed` by `quote_uri`; an
        // item whose `quote_uri` no longer has a `feed` row (demoted after
        // the snapshot was built) is skipped, not an error.
        let store = memory_store();
        let writer = test_writer(&store);
        seed_feed_row(&store, &writer, "at://q/0", "did:plc:q0", "did:plc:o0", 1, 1).await;
        seed_feed_row(&store, &writer, "at://q/1", "did:plc:q1", "did:plc:o1", 1, 2).await;

        let items = vec![feed_item("at://q/0"), feed_item("at://q/missing"), feed_item("at://q/1")];
        let snapshot = SnapshotHandle::new();
        snapshot.swap(Arc::new(items), Arc::new(vec![0, 1, 2]));

        let ranked = super::step2_candidates(&store, &snapshot, 10).unwrap();

        assert_eq!(
            ranked,
            vec![
                RankedAuthors {
                    quote_did: "did:plc:q0".to_string(),
                    original_did: "did:plc:o0".to_string()
                },
                RankedAuthors {
                    quote_did: "did:plc:q1".to_string(),
                    original_did: "did:plc:o1".to_string()
                },
            ]
        );
    }

    #[tokio::test]
    async fn step2_candidates_respects_the_depth() {
        // BC1: only the first `follows_me_depth` items of the snapshot
        // contribute candidates, however many the snapshot holds.
        let store = memory_store();
        let writer = test_writer(&store);
        seed_feed_row(&store, &writer, "at://q/0", "did:plc:q0", "did:plc:o0", 1, 1).await;
        seed_feed_row(&store, &writer, "at://q/1", "did:plc:q1", "did:plc:o1", 1, 2).await;

        let items = vec![feed_item("at://q/0"), feed_item("at://q/1")];
        let snapshot = SnapshotHandle::new();
        snapshot.swap(Arc::new(items), Arc::new(vec![0, 1]));

        let ranked = super::step2_candidates(&store, &snapshot, 1).unwrap();

        assert_eq!(
            ranked,
            vec![RankedAuthors {
                quote_did: "did:plc:q0".to_string(),
                original_did: "did:plc:o0".to_string()
            }]
        );
    }

    /// A `GraphSource` whose `get_follows` succeeds (so step 1 completes)
    /// but whose `get_relationships` always fails, for
    /// [`step2_failure_keeps_step1`] (AC4, BC4).
    struct FailingRelationshipsSource {
        follows: Vec<String>,
    }

    impl GraphSource for FailingRelationshipsSource {
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
            Err(PdsError::Session)
        }
    }

    #[tokio::test]
    async fn step2_failure_keeps_step1() {
        // AC4, BC4: a `PdsError` from `get_relationships` part way through
        // step 2 keeps the circle's step 1 `follows`, saves and swaps in
        // whatever was checked before the failure with state `ready`, and
        // re-queues the job after the retry delay rather than deleting the
        // viewer.
        let store = memory_store();
        let writer = test_writer(&store);
        seed_feed_row(&store, &writer, "at://q/0", "did:plc:quoter", "did:plc:original", 1, 1)
            .await;

        let snapshot = SnapshotHandle::new();
        snapshot.swap(Arc::new(vec![feed_item("at://q/0")]), Arc::new(vec![0]));

        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        handle.enqueue_first_build(viewer.clone(), unix_now());
        let queue = handle.queue();
        let job = queue.pop().await;

        let source = FailingRelationshipsSource { follows: vec!["did:plc:known".to_string()] };
        process_job(
            job,
            &handle,
            &queue,
            &store,
            &source,
            10,
            None,
            Duration::from_millis(20),
            &snapshot,
            10,
            100,
            24,
        )
        .await;

        let circle = handle.get(&viewer).expect("circle still exists");
        assert_eq!(circle.state, CircleState::Ready);
        assert!(circle.follows.contains(&hash_did("did:plc:known")), "step 1 data is kept");
        assert!(queue.try_pop().is_none(), "the retry has not fired yet");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            queue.try_pop(),
            Some(Job::FirstBuild(viewer)),
            "the job was re-queued after the retry delay"
        );
    }

    #[tokio::test]
    async fn resumed_building_fm_waits_for_a_real_snapshot_then_checks_candidates() {
        // Defect AE: a `building_fm` row resumed before the scorer's first
        // pass has ever swapped a snapshot in must not be saved `ready`
        // with zero candidates it can never recheck. `process_first_build`
        // skips step 1 (BC9a), and step 2 sees `snapshot.current().generation
        // == 0` and re-queues instead of running — the circle stays
        // `building_fm`, and no step 2 failure is recorded. Once a real
        // (even empty) pass swaps in, the retry runs step 2 for real.
        let store = memory_store();
        let writer = test_writer(&store);
        seed_feed_row(&store, &writer, "at://q/0", "did:plc:quoter", "did:plc:original", 1, 1)
            .await;
        store
            .viewer_save_circle("did:plc:viewer", "building_fm", 1, 1, &[], &HashSet::new())
            .unwrap();

        let handle = GraphHandle::from_store(&store, 10).unwrap();
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let queue = handle.queue();
        let job = queue.pop().await;
        assert_eq!(
            job,
            Job::FirstBuild(viewer.clone()),
            "the restart load re-enqueues the building_fm row"
        );

        let snapshot = SnapshotHandle::new();
        let source = FailingRelationshipsSource { follows: Vec::new() };

        process_job(
            job,
            &handle,
            &queue,
            &store,
            &source,
            10,
            None,
            Duration::from_millis(20),
            &snapshot,
            10,
            100,
            24,
        )
        .await;

        assert_eq!(
            handle.get(&viewer).unwrap().state,
            CircleState::BuildingFm,
            "step 2 waited instead of saving ready with no candidates"
        );
        assert!(queue.try_pop().is_none(), "the retry has not fired yet");

        // Now a real pass swaps in — the next attempt checks candidates.
        snapshot.swap(Arc::new(vec![feed_item("at://q/0")]), Arc::new(vec![0]));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let job = queue.pop().await;
        assert_eq!(job, Job::FirstBuild(viewer.clone()));

        let checking_source = ManyFollowsSource { follows: Vec::new() };
        process_job(
            job,
            &handle,
            &queue,
            &store,
            &checking_source,
            10,
            None,
            Duration::from_millis(20),
            &snapshot,
            10,
            100,
            24,
        )
        .await;

        let circle = handle.get(&viewer).expect("circle still exists");
        assert_eq!(circle.state, CircleState::Ready);
        assert!(
            circle.checked.contains(&hash_did("did:plc:quoter")),
            "step 2 ran against the real snapshot's candidate"
        );
    }

    #[tokio::test]
    async fn step2_give_up_ends_ready_with_step1_data_kept() {
        // BC4b as amended, review round 1, defect AF: giving up on step 2
        // after `MAX_FIRST_BUILD_ATTEMPTS` failed saves in a row still
        // promotes the circle to `Ready` in memory, keeping whatever data
        // it already had, rather than leaving it stuck at `building_fm`
        // forever (a `ready` row is not re-enqueued by
        // `GraphHandle::from_store`, but a `building_fm` one is, so a stuck
        // `building_fm` circle would retry step 2 forever across restarts
        // too).
        let nanos =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("upstage-graph-queue-af-{nanos}.sqlite3"));
        let path_str = path.to_str().unwrap().to_string();
        {
            let seed = Store::open_path(&path_str).unwrap();
            seed.viewer_save_circle("did:plc:viewer", "building_fm", 1, 1, &[], &HashSet::new())
                .unwrap();
        }
        let read_only_store = Store::open_read_only(&path_str).unwrap();

        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let mut circle = Circle::new();
        circle.follows.insert(hash_did("did:plc:known"));
        handle.swap_circle(&viewer, circle.clone(), CircleState::BuildingFm);
        let queue = handle.queue();

        let snapshot = SnapshotHandle::new();
        snapshot.swap(Arc::new(Vec::new()), Arc::new(Vec::new()));
        let source = ManyFollowsSource { follows: Vec::new() };

        for _ in 0..MAX_FIRST_BUILD_ATTEMPTS {
            run_step2(
                &viewer,
                &handle,
                &queue,
                &read_only_store,
                &source,
                handle.get(&viewer).unwrap().as_ref().clone(),
                &snapshot,
                10,
                None,
                Duration::from_millis(5),
                100,
                24,
            )
            .await;
        }

        let result = handle.get(&viewer).expect("circle still exists");
        assert_eq!(result.state, CircleState::Ready, "give-up promotes the circle to ready");
        assert!(result.follows.contains(&hash_did("did:plc:known")), "step 1 data is kept");
        assert!(queue.try_pop().is_none(), "no further retry is queued after give-up");

        let _ = std::fs::remove_file(&path_str);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }

    #[tokio::test]
    async fn step2_give_up_never_keeps_an_unsaved_attempt() {
        // Review round 2, defect AH: five `viewer_save_checks` failures in a
        // row must not swap this attempt's own (never persisted)
        // `checked`/`follows_me` into memory just because the worker gives
        // up retrying step 2. `handle_step2_failure` promotes whatever
        // `handle` still holds — the last data that actually saved — to
        // `Ready`, so the in-memory circle ends up identical to what a fresh
        // `GraphHandle::from_store` load of the same (unmodified) file would
        // produce, just with `state` advanced to `ready`.
        let nanos =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("upstage-graph-queue-ah-{nanos}.sqlite3"));
        let path_str = path.to_str().unwrap().to_string();
        let follows: HashSet<u64> = [hash_did("did:plc:known")].into_iter().collect();
        {
            let seed = Store::open_path(&path_str).unwrap();
            seed.viewer_save_circle("did:plc:viewer", "building_fm", 1, 1, &[], &follows).unwrap();
        }
        let read_only_store = Store::open_read_only(&path_str).unwrap();

        let handle = GraphHandle::from_store(&read_only_store, 10).unwrap();
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let queue = handle.queue();
        queue.try_pop(); // drop the restart re-enqueue; this test drives run_step2 itself.

        let snapshot = SnapshotHandle::new();
        snapshot.swap(Arc::new(Vec::new()), Arc::new(Vec::new())); // BC1b: no candidates.
        let source = ManyFollowsSource { follows: Vec::new() };

        for _ in 0..MAX_FIRST_BUILD_ATTEMPTS {
            run_step2(
                &viewer,
                &handle,
                &queue,
                &read_only_store,
                &source,
                handle.get(&viewer).unwrap().as_ref().clone(),
                &snapshot,
                10,
                None,
                Duration::from_millis(5),
                100,
                24,
            )
            .await;
        }

        let result = handle.get(&viewer).expect("circle still exists");
        assert_eq!(result.state, CircleState::Ready, "give-up promotes the circle to ready");

        // A fresh load of the same, never-successfully-written-to file has
        // exactly the same follows/checked/follows_me the give-up circle
        // ended up with — nothing from the failed attempts leaked in.
        let reload_store = Store::open_read_only(&path_str).unwrap();
        let fresh = GraphHandle::from_store(&reload_store, 10).unwrap();
        let fresh_circle = fresh.get(&viewer).expect("fresh load finds the same row");
        assert_eq!(result.follows, fresh_circle.follows);
        assert_eq!(result.checked, fresh_circle.checked);
        assert_eq!(result.follows_me, fresh_circle.follows_me);

        let _ = std::fs::remove_file(&path_str);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }

    #[tokio::test]
    async fn step2_give_up_does_not_recreate_a_deleted_viewer() {
        // Review round 2, defect AI: if a concurrent step 1 give-up already
        // removed the viewer's in-memory circle and its `viewers` row before
        // step 2's own give-up runs, `handle_step2_failure` recreates
        // neither: `GraphHandle::mark_ready` is a no-op with no circle to
        // promote, and `Store::viewer_set_state_if_exists`'s plain `UPDATE`
        // matches no row.
        let store = memory_store();
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let queue = handle.queue();
        handle.enqueue_first_build(viewer.clone(), unix_now());
        handle.remove_after_giving_up(&viewer, unix_now(), 3_600);

        for _ in 0..MAX_FIRST_BUILD_ATTEMPTS {
            handle_step2_failure(&viewer, &queue, Duration::from_millis(5), &handle, &store).await;
        }

        assert!(handle.get(&viewer).is_none(), "no circle was recreated");
        assert!(store.viewer_load_all().unwrap().is_empty(), "no viewers row was created");
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

    /// A `GraphSource` for step 3 tests: `follows` holds each failing-free
    /// actor's full list, paginated at the caller's own `limit` like
    /// `FakeSource` in `build.rs`'s own tests; `fail_accounts` names the
    /// actors that return `PdsError::Session` instead of paging;
    /// `fetched` records every actor `get_follows` was actually called for,
    /// in call order, so a test can assert exactly which accounts step 3
    /// fetched — in particular, that a fresh account never appears (BC2).
    struct Step3Source {
        follows: HashMap<String, Vec<String>>,
        fail_accounts: HashSet<String>,
        fetched: StdMutex<Vec<String>>,
    }

    impl GraphSource for Step3Source {
        async fn get_follows(
            &self,
            actor: &str,
            limit: u32,
            cursor: Option<String>,
        ) -> Result<FollowsPage, PdsError> {
            self.fetched.lock().unwrap().push(actor.to_string());
            if self.fail_accounts.contains(actor) {
                return Err(PdsError::Session);
            }
            let all = self.follows.get(actor).cloned().unwrap_or_default();
            let offset: usize = cursor.as_deref().and_then(|c| c.parse().ok()).unwrap_or(0);
            let end = (offset + limit as usize).min(all.len());
            let dids = all.get(offset.min(all.len())..end).unwrap_or_default().to_vec();
            let cursor = if end < all.len() { Some(end.to_string()) } else { None };
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

    #[tokio::test]
    async fn step3_fetches_missing_only() {
        // AC1; BC1, BC2, BC4, BC4a, BC5: a fresh account (`a`) costs no
        // call; a stale one (`b`) and a missing one (`c`) are fetched and
        // saved; a failing one (`d`) is logged and skipped, leaving it with
        // no entry, and the loop still finishes the rest and promotes the
        // circle to `ready`.
        let store = memory_store();
        let real_now = unix_now();
        store.follows_put("did:plc:a", real_now, &[hash_did("did:plc:a-follow")]).unwrap();
        store.follows_put("did:plc:b", 0, &[hash_did("did:plc:stale")]).unwrap();

        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let mut circle = Circle::new();
        circle.d2_sample = vec![
            "did:plc:a".to_string(),
            "did:plc:b".to_string(),
            "did:plc:c".to_string(),
            "did:plc:d".to_string(),
        ];
        handle.swap_circle(&viewer, circle, CircleState::BuildingD2);
        let queue = handle.queue();

        let source = Step3Source {
            follows: HashMap::from([
                ("did:plc:b".to_string(), vec!["did:plc:fresh-b".to_string()]),
                ("did:plc:c".to_string(), vec!["did:plc:fresh-c".to_string()]),
            ]),
            fail_accounts: HashSet::from(["did:plc:d".to_string()]),
            fetched: StdMutex::new(Vec::new()),
        };

        run_step3(&viewer, &handle, &queue, &store, &source, 100, 24, None).await;

        let fetched = source.fetched.lock().unwrap().clone();
        assert!(!fetched.contains(&"did:plc:a".to_string()), "a fresh account costs no call");
        assert!(fetched.contains(&"did:plc:b".to_string()), "a stale account is fetched");
        assert!(fetched.contains(&"did:plc:c".to_string()), "a missing account is fetched");
        assert!(fetched.contains(&"did:plc:d".to_string()), "a failing account is still tried");

        let b_row = store.follows_get("did:plc:b").unwrap().unwrap();
        assert_eq!(b_row.follows, vec![hash_did("did:plc:fresh-b")], "b's stale entry is replaced");
        let c_row = store.follows_get("did:plc:c").unwrap().unwrap();
        assert_eq!(c_row.follows, vec![hash_did("did:plc:fresh-c")], "c's missing entry is filled");
        assert!(store.follows_get("did:plc:d").unwrap().is_none(), "d's failure leaves no entry");

        let circle = handle.get(&viewer).expect("circle still exists");
        assert_eq!(circle.state, CircleState::Ready, "step 3 completes with ready");
        assert!(queue.try_pop().is_none(), "the job completed, nothing re-queued");
    }

    #[tokio::test]
    async fn step3_drops_lists_after_each_put_not_only_at_the_end() {
        // Defect AJ, BC11b: `drop_lists` fires once per successful put (two
        // accounts fetched here) plus once more when the loop finishes, not
        // only the final call — a request arriving mid-loop must see a
        // rebuilt list against every entry made fresh so far.
        let store = memory_store();
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let mut circle = Circle::new();
        circle.d2_sample = vec!["did:plc:a".to_string(), "did:plc:b".to_string()];
        handle.swap_circle(&viewer, circle, CircleState::BuildingD2);
        let queue = handle.queue();

        let source = Step3Source {
            follows: HashMap::from([
                ("did:plc:a".to_string(), vec!["did:plc:fresh-a".to_string()]),
                ("did:plc:b".to_string(), vec!["did:plc:fresh-b".to_string()]),
            ]),
            fail_accounts: HashSet::new(),
            fetched: StdMutex::new(Vec::new()),
        };

        let dropped: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let dropped_for_closure = Arc::clone(&dropped);
        let drop_lists: DropListsFn =
            Arc::new(move |did: &str| dropped_for_closure.lock().unwrap().push(did.to_string()));

        run_step3(&viewer, &handle, &queue, &store, &source, 100, 24, Some(&drop_lists)).await;

        let calls = dropped.lock().unwrap().clone();
        assert_eq!(
            calls,
            vec![
                "did:plc:viewer".to_string(),
                "did:plc:viewer".to_string(),
                "did:plc:viewer".to_string()
            ],
            "one drop per successful put (a, b), plus one more when the loop finishes"
        );
    }

    #[tokio::test]
    async fn step3_refetches_a_future_fetched_at() {
        // Defect AK: `is_fresh` now treats a `fetched_at` later than `now`
        // as stale, so step 3 fetches that account again instead of
        // trusting a forward-skewed timestamp.
        let store = memory_store();
        let real_now = unix_now();
        store.follows_put("did:plc:a", real_now + 3_600, &[hash_did("did:plc:old")]).unwrap();

        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let mut circle = Circle::new();
        circle.d2_sample = vec!["did:plc:a".to_string()];
        handle.swap_circle(&viewer, circle, CircleState::BuildingD2);
        let queue = handle.queue();

        let source = Step3Source {
            follows: HashMap::from([("did:plc:a".to_string(), vec!["did:plc:new".to_string()])]),
            fail_accounts: HashSet::new(),
            fetched: StdMutex::new(Vec::new()),
        };

        run_step3(&viewer, &handle, &queue, &store, &source, 100, 24, None).await;

        let fetched = source.fetched.lock().unwrap().clone();
        assert!(fetched.contains(&"did:plc:a".to_string()), "a future fetched_at is refetched");
        let row = store.follows_get("did:plc:a").unwrap().unwrap();
        assert_eq!(
            row.follows,
            vec![hash_did("did:plc:new")],
            "the stale future entry is replaced"
        );
    }

    /// A `GraphSource` whose follows list can be swapped out from under it
    /// (behind a shared, thread-safe `Mutex`) after construction, for
    /// [`super::super::tests::follow_changes_after_refresh`] in `mod.rs`:
    /// the test builds a viewer once, then changes what the source reports
    /// and enqueues a `Refresh` job to see the new list land.
    pub(crate) struct ChangingFollowsSource {
        follows: StdMutex<Vec<String>>,
    }

    impl ChangingFollowsSource {
        pub(crate) fn new(follows: Vec<String>) -> Self {
            Self { follows: StdMutex::new(follows) }
        }

        pub(crate) fn set_follows(&self, follows: Vec<String>) {
            *self.follows.lock().unwrap() = follows;
        }
    }

    impl GraphSource for Arc<ChangingFollowsSource> {
        async fn get_follows(
            &self,
            _actor: &str,
            limit: u32,
            cursor: Option<String>,
        ) -> Result<FollowsPage, PdsError> {
            let all = self.follows.lock().unwrap().clone();
            let offset: usize = cursor.as_deref().and_then(|c| c.parse().ok()).unwrap_or(0);
            let end = (offset + limit as usize).min(all.len());
            let dids = all.get(offset.min(all.len())..end).unwrap_or_default().to_vec();
            let cursor = if end < all.len() { Some(end.to_string()) } else { None };
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

    #[tokio::test]
    async fn refill_success_fetches_saves_and_caches() {
        // BC7a: a `Refill` job fetches the account's follows, saves them to
        // SQLite, puts them in the shared cache, and drops the cached list
        // of every viewer whose circle names the account.
        let store = memory_store();
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let mut circle = Circle::new();
        circle.d2_sample = vec!["did:plc:account".to_string()];
        handle.swap_circle(&viewer, circle, CircleState::Ready);
        let queue = handle.queue();

        let source =
            ManyFollowsSource { follows: vec!["did:plc:fresh-a".to_string(), did(1), did(2)] };
        let dropped: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let dropped_for_closure = Arc::clone(&dropped);
        let drop_lists: DropListsFn =
            Arc::new(move |d: &str| dropped_for_closure.lock().unwrap().push(d.to_string()));

        run_refill(
            "did:plc:account",
            &handle,
            &queue,
            &store,
            &source,
            100,
            Some(&drop_lists),
            Duration::from_millis(20),
        )
        .await;

        let row = store.follows_get("did:plc:account").unwrap().unwrap();
        assert_eq!(row.follows.len(), 3, "the fetched follows are saved");
        assert_eq!(
            handle.follows_cache().degree2_set(&["did:plc:account".to_string()]).len(),
            3,
            "the fetched follows are also in the shared cache"
        );
        assert_eq!(
            dropped.lock().unwrap().clone(),
            vec!["did:plc:viewer".to_string()],
            "the viewer naming this account had its cached list dropped"
        );
        assert!(queue.try_pop().is_none(), "no retry is queued on success");
    }

    #[tokio::test]
    async fn refill_failure_leaves_the_entry_unchanged_and_retries() {
        // BC7b: a `PdsError` from the fetch leaves the cache entry — in
        // memory and in SQLite — exactly as it was, and retries after the
        // delay.
        let store = memory_store();
        store.follows_put("did:plc:account", 1, &[hash_did("did:plc:old")]).unwrap();
        let handle = GraphHandle::new(10);
        handle.follows_cache().put("did:plc:account", 1, vec![hash_did("did:plc:old")]);
        let queue = handle.queue();

        let source = FailingSource { calls: StdMutex::new(0) };
        run_refill(
            "did:plc:account",
            &handle,
            &queue,
            &store,
            &source,
            100,
            None,
            Duration::from_millis(20),
        )
        .await;

        let row = store.follows_get("did:plc:account").unwrap().unwrap();
        assert_eq!(row.follows, vec![hash_did("did:plc:old")], "the stored entry is untouched");
        assert!(queue.try_pop().is_none(), "the retry has not fired yet");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            queue.try_pop(),
            Some(Job::Refill("did:plc:account".to_string())),
            "the job was re-queued after the retry delay"
        );
    }

    #[tokio::test]
    async fn evicted_during_job_saves_nothing() {
        // AC10; story 09 spec.md BC6a: a circle evicted (`GraphHandle::
        // evict`) after its `FirstBuild` job was already popped, but before
        // that job runs, must save nothing to SQLite and swap nothing into
        // memory once the job finally executes — `run_step1`'s own
        // existence check catches this before its first save.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        handle.enqueue_first_build(viewer.clone(), unix_now());
        let queue = handle.queue();
        let job = queue.pop().await;
        assert_eq!(job, Job::FirstBuild(viewer.clone()));

        handle.evict(&viewer, crate::graph::EvictReason::Idle);

        let store = memory_store();
        let source = ManyFollowsSource { follows: vec![did(1)] };
        process_job(
            job,
            &handle,
            &queue,
            &store,
            &source,
            10,
            None,
            Duration::from_millis(20),
            &SnapshotHandle::new(),
            10,
            100,
            24,
        )
        .await;

        assert!(handle.get(&viewer).is_none(), "nothing was recreated in memory");
        assert!(store.viewer_load_all().unwrap().is_empty(), "nothing was saved to SQLite");
        assert!(
            queue.try_pop().is_none(),
            "no job was re-queued: no new circle appeared for this viewer"
        );
    }

    #[test]
    fn abandon_evicted_job_requeues_when_a_new_circle_already_exists() {
        // BC6b, a direct test of the completion helper every step's
        // eviction check hands off to: a circle already existing for
        // `viewer` by the time this runs (a request that arrived while the
        // old job's own outstanding mark was still set, so its own `push`
        // no-oped) gets the job back the moment that mark clears.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let queue = handle.queue();
        let job = Job::FirstBuild(viewer.clone());
        queue.push(job.clone());
        queue.try_pop();

        handle.enqueue_first_build(viewer.clone(), unix_now());

        abandon_evicted_job(&handle, &queue, &viewer);

        assert_eq!(queue.try_pop(), Some(job), "the new circle gets a job");
    }

    #[test]
    fn abandon_evicted_job_requeues_nothing_with_no_circle() {
        // BC6a's ordinary case: nothing recreated the circle, so nothing is
        // queued.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let queue = handle.queue();
        let job = Job::FirstBuild(viewer.clone());
        queue.push(job.clone());
        queue.try_pop();

        abandon_evicted_job(&handle, &queue, &viewer);

        assert!(queue.try_pop().is_none(), "nothing to build, nothing queued");
    }

    #[test]
    fn forget_clears_both_give_up_counters() {
        // BC11a: `GraphHandle::evict` calls `forget` so a viewer's next
        // first build starts counting failures from zero, whether it was
        // step 1's count, step 2's, or both.
        let queue = JobQueue::new();
        let viewer = ViewerDid("did:plc:viewer".to_string());

        assert_eq!(queue.record_failure(&viewer), 1);
        assert_eq!(queue.record_step2_failure(&viewer), 1);

        queue.forget(&viewer);

        assert_eq!(queue.record_failure(&viewer), 1, "step 1's count restarted at zero");
        assert_eq!(queue.record_step2_failure(&viewer), 1, "step 2's count restarted at zero");
    }
}
