//! Viewer graph, TECH-DESIGN-network-feed §6. `circle` holds the per-viewer
//! `Circle`; `build` runs the three first-build steps over a `GraphSource`
//! so `graph_probe` (story 03) and the worker (`queue`, story 06) share one
//! implementation. This module holds [`GraphHandle`], the in-memory index
//! the handler reads without an await and the worker writes into, and the
//! startup load that makes a restart pick up circles saved in SQLite
//! (BC19) instead of treating every viewer as a first open.

mod queue;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use xxhash_rust::xxh3::xxh3_64;

pub use crate::auth::ViewerDid;
use crate::store::{Store, StoreError};

pub mod build;
pub mod circle;
pub mod filter;

pub use circle::Circle;
pub use queue::{run_touch_flush, run_worker, DropListsFn, JobQueue};

/// A DID's `xxh3_64` hash, kept in place of the DID string wherever a
/// `Circle` or the connection filter only needs to compare, not print, an
/// author (spec.md BC16: a viewer DID never reaches the probe's output).
pub type DidHash = u64;

/// Hashes `did` with `xxh3_64`, a fixed-key hash (not `RandomState`), so the
/// same DID hashes to the same `DidHash` in every run (BC18): a circle
/// built in one run and compared against one built in another still lines
/// up, and a test fixture's expected hash never has to be recomputed.
pub fn hash_did(did: &str) -> DidHash {
    xxh3_64(did.as_bytes())
}

/// A `viewers.state` value (`store::viewers::ViewerRow::state`), design
/// §8. Story 07 adds `BuildingFm`; `building_d2` is still story 08's to
/// add.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CircleState {
    /// Step 1 is queued or running, or a prior attempt failed and is
    /// waiting to retry (BC7a, BC8). The default: a freshly enqueued
    /// circle starts here, before the worker has ever touched it.
    #[default]
    BuildingD1,
    /// Step 1 has saved successfully at least once and step 2
    /// (`build::step_follows_me`) is queued, running, or waiting to retry
    /// (BC3a, BC8, BC9a). `follows` and `d2_sample` are complete and safe
    /// to serve; `checked` and `follows_me` may be empty or partial.
    BuildingFm,
    /// Step 2 has saved successfully at least once, or the worker gave up
    /// retrying it without deleting the viewer (BC3, BC4b). `checked` and
    /// `follows_me` may still be partial in the give-up case.
    Ready,
}

impl CircleState {
    /// The `viewers.state` text this variant is stored and loaded as.
    pub fn as_str(&self) -> &'static str {
        match self {
            CircleState::BuildingD1 => "building_d1",
            CircleState::BuildingFm => "building_fm",
            CircleState::Ready => "ready",
        }
    }

    /// Parses a stored `viewers.state` value. Falls back to `BuildingD1`
    /// for anything but exactly `"ready"` or `"building_fm"`, rather than
    /// raising an error: an unrecognised state can only mean a build
    /// attempt was interrupted before it finished (this binary is the only
    /// writer), and treating it as still building step 1 safely
    /// re-enqueues it (BC19) instead of failing the whole startup load over
    /// one row.
    fn from_store_str(state: &str) -> Self {
        if state == CircleState::Ready.as_str() {
            CircleState::Ready
        } else if state == CircleState::BuildingFm.as_str() {
            CircleState::BuildingFm
        } else {
            CircleState::BuildingD1
        }
    }
}

/// One viewer's in-memory `last_request_at`, tracked separately from the
/// `Circle` a worker save swaps (BC23): a request touch must never wait on
/// or block the worker, so it is recorded here instead of mutating the
/// `Arc<Circle>` in place.
struct TouchEntry {
    last_request_at: i64,
    last_flushed_at: i64,
}

/// [`GraphHandle`]'s circle map and cooldown map, held behind one lock
/// (review round 2, defect AB): `enqueue_first_build`'s cooldown check and
/// circle insert, and `remove_after_giving_up`'s circle removal and
/// cooldown insert, each need to happen as a single atomic step, or a
/// request racing the worker's give-up can observe a circle during its own
/// cooldown, or a cooldown with no circle and no job. Two separate locks
/// (a `circles` `RwLock` and a `cooldowns` `Mutex`, as story 06 first
/// shipped) cannot give that guarantee: a step between them is a window a
/// concurrent caller can land in.
#[derive(Default)]
struct GraphState {
    circles: HashMap<ViewerDid, Arc<Circle>>,
    /// A viewer the worker gave up on (review round 1, defect W), mapped to
    /// the unix second [`GraphHandle::enqueue_first_build`] may start a new
    /// first build for it again. A viewer with no entry here has never been
    /// given up on.
    cooldowns: HashMap<ViewerDid, i64>,
}

/// The graph index the handler reads without an await (spec `## Approach`):
/// a `RwLock<GraphState>`, plus the de-duplicated queue
/// `enqueue_first_build` sends into and the worker drains. Cloning this
/// handle (through its `Arc`) is how the worker task, the touch-flush task
/// and the HTTP handler (slice 4.0) all share the one index.
pub struct GraphHandle {
    state: RwLock<GraphState>,
    queue: Arc<JobQueue>,
    max_viewers: usize,
    touches: Mutex<HashMap<ViewerDid, TouchEntry>>,
    /// The unix second [`Self::warn_at_cap`] last logged its one-per-minute
    /// warning at (BC6a). `None` until the first time the cap is hit.
    cap_warned_at: Mutex<Option<i64>>,
}

/// The cap warning's cooldown (BC6a): "at most once per minute".
const CAP_WARN_COOLDOWN_SECS: i64 = 60;

impl GraphHandle {
    /// An empty handle with no circles and no jobs queued, at most
    /// `max_viewers` circles held at once (BC6a).
    pub fn new(max_viewers: usize) -> Arc<Self> {
        Arc::new(GraphHandle {
            state: RwLock::new(GraphState::default()),
            queue: JobQueue::new(),
            max_viewers,
            touches: Mutex::new(HashMap::new()),
            cap_warned_at: Mutex::new(None),
        })
    }

    /// Loads every `viewers` row from `store` (BC19, BC9), builds a circle
    /// for each — `checked` and `follows_me` included (BC9) — and
    /// re-enqueues a `FirstBuild` job for any row still in `building_d1` or
    /// `building_fm`: a restart mid-build picks the job back up instead of
    /// losing it, resuming at step 2 for a `building_fm` row (BC9a) rather
    /// than repeating step 1. A `ready` row's step 2 retry, if one was
    /// pending at the moment of the restart, is not resumed (spec.md
    /// `## Defaults taken`): its data is already good enough to serve, and
    /// a future refresh (story 09) is what reruns step 2 for it. Loaded
    /// circles bypass the `UPSTAGE_MAX_VIEWERS` cap
    /// [`Self::enqueue_first_build`] enforces: they already exist in
    /// SQLite, so refusing to load one back into memory would silently
    /// drop a viewer the store already accepted.
    pub fn from_store(store: &Store, max_viewers: usize) -> Result<Arc<Self>, StoreError> {
        let handle = Self::new(max_viewers);
        let rows = store.viewer_load_all()?;
        for row in rows {
            let viewer = ViewerDid(row.viewer_did);
            let state = CircleState::from_store_str(&row.state);
            let mut circle = Circle::new();
            circle.follows = row.follows;
            circle.checked = row.checked;
            circle.follows_me = row.follows_me;
            circle.d2_sample = row.d2_sample;
            circle.last_request_at = row.last_request_at;
            circle.d1_refreshed_at = row.d1_refreshed_at;
            circle.state = state;
            circle.circle_version = if state == CircleState::BuildingD1 { 0 } else { 1 };

            handle
                .state
                .write()
                .expect("GraphHandle state lock poisoned")
                .circles
                .insert(viewer.clone(), Arc::new(circle));
            if state == CircleState::BuildingD1 || state == CircleState::BuildingFm {
                handle.queue.push(viewer);
            }
        }
        Ok(handle)
    }

    /// The queue the worker (`queue::run_worker`) drains. `Arc`-shared so
    /// the worker task can own a clone independent of this handle's own
    /// lifetime.
    pub fn queue(&self) -> Arc<JobQueue> {
        Arc::clone(&self.queue)
    }

    /// The current circle for `viewer`, if one has been created — `None`
    /// for a viewer never seen before (BC4: the handler enqueues a first
    /// build in that case). Reads the lock without an await (spec
    /// `## Approach`). No production caller yet: `src/http/viewer.rs`
    /// (slice 3.0) is the first.
    #[allow(dead_code)]
    pub fn get(&self, viewer: &ViewerDid) -> Option<Arc<Circle>> {
        self.state.read().expect("GraphHandle state lock poisoned").circles.get(viewer).cloned()
    }

    /// Enqueues a `FirstBuild` job for `viewer` unless one is already
    /// queued or running (BC4, BC5, BC6), the handle already holds
    /// `max_viewers` circles (BC6a), or `viewer` is still cooling down after
    /// a give-up (defect W). `now` is the unix second the request arrived,
    /// used only to rate-limit the cap warning. The cooldown check and the
    /// circle insert happen under one write lock (review round 2, defect
    /// AB), the same lock [`Self::remove_after_giving_up`] takes: a
    /// concurrent give-up is fully applied or not started at all when this
    /// runs, so this either sees the viewer's old circle (BC5, no new job
    /// needed) or its cooldown (no circle created), never neither.
    pub fn enqueue_first_build(&self, viewer: ViewerDid, now: i64) {
        let mut state = self.state.write().expect("GraphHandle state lock poisoned");
        if let Some(&not_before) = state.cooldowns.get(&viewer) {
            if now < not_before {
                return;
            }
        }
        if state.circles.contains_key(&viewer) {
            // BC5: a circle already exists — building or ready — so no
            // second job is queued.
            return;
        }
        if state.circles.len() >= self.max_viewers {
            drop(state);
            self.warn_at_cap(now);
            return;
        }
        state.circles.insert(viewer.clone(), Arc::new(Circle::new()));
        drop(state);
        self.queue.push(viewer);
    }

    /// BC6a: one `warn` line, naming no DID, at most once a minute.
    fn warn_at_cap(&self, now: i64) {
        let mut warned_at = self.cap_warned_at.lock().expect("GraphHandle warn mutex poisoned");
        let should_log = match *warned_at {
            Some(last) => now - last >= CAP_WARN_COOLDOWN_SECS,
            None => true,
        };
        if should_log {
            tracing::warn!(
                max_viewers = self.max_viewers,
                "graph: viewer cap reached, no circle created"
            );
            *warned_at = Some(now);
        }
    }

    /// Swaps in a freshly built circle for `viewer` at `new_state`: bumps
    /// `circle_version` past whatever the handle held before (or to 1, for
    /// a viewer with no prior entry), and installs the new `Arc`. Returns
    /// the new version, so the caller (the worker) can pass it to the
    /// drop-lists callback if it ever needs to. `graph::queue::process_job`
    /// calls this after step 1 (`CircleState::BuildingFm`, BC3a) and again
    /// after step 2 (`CircleState::Ready`, BC3, BC4).
    ///
    /// `pub(crate)`, not private: `src/http/skeleton.rs` (slice 4.0,
    /// `circle_change_mid_scroll`, BC16) is outside `graph::`'s own module
    /// tree and has no other way to simulate a worker's second save at a
    /// fixed snapshot generation, so a test is a caller besides
    /// `graph::queue::process_job`.
    pub(crate) fn swap_circle(
        &self,
        viewer: &ViewerDid,
        mut circle: Circle,
        new_state: CircleState,
    ) -> u64 {
        let mut state = self.state.write().expect("GraphHandle state lock poisoned");
        let version = state.circles.get(viewer).map(|c| c.circle_version + 1).unwrap_or(1);
        circle.state = new_state;
        circle.circle_version = version;
        state.circles.insert(viewer.clone(), Arc::new(circle));
        version
    }

    /// [`Self::swap_circle`] at `CircleState::Ready` (BC7). Kept as its own
    /// name since story 06's tests and `src/http/skeleton.rs` already call
    /// it under this name for the common "swap in a finished circle" case.
    pub(crate) fn insert_ready(&self, viewer: &ViewerDid, circle: Circle) -> u64 {
        self.swap_circle(viewer, circle, CircleState::Ready)
    }

    /// Removes `viewer`'s in-memory circle and starts a `cooldown_secs`
    /// cooldown before [`Self::enqueue_first_build`] accepts a new job for
    /// it (review round 1, defect W): the worker
    /// (`graph::queue::process_job`) calls this after
    /// `graph::queue::MAX_FIRST_BUILD_ATTEMPTS` failed attempts in a row,
    /// freeing the slot `enqueue_first_build`'s `UPSTAGE_MAX_VIEWERS` cap
    /// counts against. `pub(crate)` for the same reason
    /// [`Self::insert_ready`] is: the worker, in `graph::queue`, is the only
    /// production caller, but this module's own tests exercise it directly
    /// too.
    ///
    /// The removal and the cooldown insert happen under one write lock
    /// (review round 2, defect AB): the previous two-step version (a
    /// `circles` write lock, released, then a separate `cooldowns` lock)
    /// left a window with no circle and no cooldown yet, where a concurrent
    /// `enqueue_first_build` would insert a fresh circle with nothing to
    /// build it — `JobQueue::push` was still a no-op for that viewer, since
    /// its outstanding mark was not cleared until after this call returned,
    /// so the new circle got no job and sat stuck until the process
    /// restarted.
    pub(crate) fn remove_after_giving_up(&self, viewer: &ViewerDid, now: i64, cooldown_secs: i64) {
        let mut state = self.state.write().expect("GraphHandle state lock poisoned");
        state.circles.remove(viewer);
        state.cooldowns.insert(viewer.clone(), now + cooldown_secs);
    }

    /// Records `viewer`'s latest request time in memory (BC23). The
    /// request path (slice 4.0) calls this on every personalised request;
    /// [`Self::due_flushes`] is what the worker's touch-flush task reads to
    /// decide what to write to SQLite, and when. No production caller yet:
    /// `src/http/skeleton.rs` (slice 4.0) is the first.
    #[allow(dead_code)]
    pub fn record_touch(&self, viewer: &ViewerDid, now: i64) {
        let mut touches = self.touches.lock().expect("GraphHandle touches mutex poisoned");
        touches
            .entry(viewer.clone())
            .and_modify(|entry| entry.last_request_at = now)
            .or_insert(TouchEntry { last_request_at: now, last_flushed_at: 0 });
    }

    /// Every viewer whose in-memory touch has gone at least `min_interval`
    /// seconds without being written to SQLite (BC23), each paired with the
    /// `last_request_at` to write. Marks every entry returned as flushed at
    /// `now`, so calling this again immediately returns nothing for them
    /// until `min_interval` passes again — the write is optimistic: a
    /// caller that fails to persist a returned entry loses that flush, the
    /// same way a missed tick of any periodic flush would.
    pub fn due_flushes(&self, now: i64, min_interval: i64) -> Vec<(ViewerDid, i64)> {
        let mut touches = self.touches.lock().expect("GraphHandle touches mutex poisoned");
        let mut due = Vec::new();
        for (viewer, entry) in touches.iter_mut() {
            if now - entry.last_flushed_at >= min_interval {
                due.push((viewer.clone(), entry.last_request_at));
                entry.last_flushed_at = now;
            }
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_did_is_stable_across_calls() {
        // BC18: a fixed-key hash, so the same DID hashes to the same value
        // whether it is hashed once or many times, and whether the process
        // that hashes it is this one or a fresh one — `xxh3_64` carries no
        // per-process seed the way `std::collections::hash_map::RandomState`
        // does.
        let did = "did:plc:abc123";
        let first = hash_did(did);
        let second = hash_did(did);
        assert_eq!(first, second);
    }

    #[test]
    fn hash_did_differs_for_distinct_dids() {
        assert_ne!(hash_did("did:plc:abc"), hash_did("did:plc:def"));
    }

    #[test]
    fn enqueue_first_build_creates_a_building_circle_and_one_job() {
        // BC4: a verified viewer with no circle gets one created in memory
        // in `building_d1`, and one job queued.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:a".to_string());

        handle.enqueue_first_build(viewer.clone(), 1_700_000_000);

        let circle = handle.get(&viewer).expect("circle created");
        assert_eq!(circle.state, CircleState::BuildingD1);
    }

    #[test]
    fn enqueue_first_build_twice_is_one_job() {
        // BC5, BC6: a second enqueue for the same viewer while it is
        // already building creates no second circle and, since the queue
        // is de-duplicated, no second job.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:a".to_string());

        handle.enqueue_first_build(viewer.clone(), 1_700_000_000);
        handle.enqueue_first_build(viewer.clone(), 1_700_000_001);

        assert_eq!(handle.state.read().unwrap().circles.len(), 1);
    }

    #[test]
    fn enqueue_first_build_at_cap_creates_nothing() {
        // BC6a: at `max_viewers`, a new viewer gets no circle and no job.
        let handle = GraphHandle::new(1);
        handle.enqueue_first_build(ViewerDid("did:plc:a".to_string()), 1_700_000_000);

        let second = ViewerDid("did:plc:b".to_string());
        handle.enqueue_first_build(second.clone(), 1_700_000_000);

        assert!(handle.get(&second).is_none());
        assert_eq!(handle.state.read().unwrap().circles.len(), 1);
    }

    #[test]
    fn insert_ready_bumps_circle_version_and_sets_ready() {
        // BC7: swapping in a built circle sets `ready` and increments the
        // version past whatever was there before.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:a".to_string());
        handle.enqueue_first_build(viewer.clone(), 1_700_000_000);

        let version = handle.insert_ready(&viewer, Circle::new());
        assert_eq!(version, 1);
        let circle = handle.get(&viewer).unwrap();
        assert_eq!(circle.state, CircleState::Ready);
        assert_eq!(circle.circle_version, 1);

        let version2 = handle.insert_ready(&viewer, Circle::new());
        assert_eq!(version2, 2);
    }

    #[test]
    fn due_flushes_respects_the_minimum_interval() {
        // BC23 (flush part): a touch recorded twice inside `min_interval`
        // of the last flush yields nothing; once the interval has passed,
        // the latest `last_request_at` is returned exactly once.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:a".to_string());

        handle.record_touch(&viewer, 1_000);
        assert_eq!(handle.due_flushes(1_010, 60), vec![(viewer.clone(), 1_000)]);

        // Immediately after a flush, the same viewer is not due again.
        handle.record_touch(&viewer, 1_020);
        assert!(handle.due_flushes(1_030, 60).is_empty());

        // Once 60 s have passed since the last flush, it is due again,
        // with whatever `last_request_at` was recorded most recently.
        handle.record_touch(&viewer, 1_040);
        assert_eq!(handle.due_flushes(1_071, 60), vec![(viewer, 1_040)]);
    }

    #[test]
    fn remove_after_giving_up_frees_the_slot_immediately() {
        // Defect W: removal drops the circle right away, so a different
        // viewer can take its `UPSTAGE_MAX_VIEWERS` slot at once, even
        // while the removed viewer's own cooldown is still running.
        let handle = GraphHandle::new(1);
        let viewer = ViewerDid("did:plc:a".to_string());
        handle.enqueue_first_build(viewer.clone(), 1_000);
        assert!(handle.get(&viewer).is_some());

        handle.remove_after_giving_up(&viewer, 1_000, 3_600);
        assert!(handle.get(&viewer).is_none());

        let other = ViewerDid("did:plc:b".to_string());
        handle.enqueue_first_build(other.clone(), 1_000);
        assert!(handle.get(&other).is_some(), "the freed slot took the new viewer");
    }

    #[test]
    fn enqueue_after_giving_up_is_blocked_until_the_cooldown_expires() {
        // Defect W: `enqueue_first_build` refuses a new job for a viewer the
        // worker gave up on until its cooldown has passed, then behaves as
        // if the viewer were new.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:a".to_string());
        handle.enqueue_first_build(viewer.clone(), 1_000);
        handle.remove_after_giving_up(&viewer, 1_000, 3_600);

        handle.enqueue_first_build(viewer.clone(), 1_000 + 3_600 - 1);
        assert!(handle.get(&viewer).is_none(), "still cooling down");

        handle.enqueue_first_build(viewer.clone(), 1_000 + 3_600);
        assert!(handle.get(&viewer).is_some(), "cooldown has expired");
    }

    #[test]
    fn giveup_then_request_in_the_gap_creates_no_jobless_circle() {
        // Review round 2, findings 1 and 2 (defect AB), replayed
        // deterministically: the worker gives up on a viewer (removing its
        // circle and starting its cooldown) but has not yet cleared the
        // job's outstanding mark on the queue when a request for the same
        // viewer arrives. Before the fix, the cooldown and the circle
        // removal were two separate steps under two separate locks, so a
        // request that landed between them saw no cooldown yet, inserted a
        // fresh circle, and called `queue.push`, which silently no-opped
        // because the original job's outstanding mark was still set — the
        // new circle then sat with no job, forever. With the cooldown and
        // the circle removal now one atomic step, the request always sees
        // the cooldown and creates nothing.
        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:a".to_string());
        let queue = handle.queue();

        handle.enqueue_first_build(viewer.clone(), 1_000);
        assert_eq!(queue.try_pop(), Some(viewer.clone()), "the worker picks up the first job");

        // The worker's 5th failed attempt gives up.
        handle.remove_after_giving_up(&viewer, 1_000, 3_600);

        // A request lands before the worker's failure path clears the
        // queue's outstanding mark for this viewer.
        handle.enqueue_first_build(viewer.clone(), 1_000);

        // Only now does the worker finish its failure path.
        queue.complete(&viewer);

        assert!(handle.get(&viewer).is_none(), "no circle exists during the cooldown");
        assert!(queue.try_pop().is_none(), "no job was queued for a circle that does not exist");

        // Once the cooldown passes, a fresh request starts a real first
        // build with a job to match.
        handle.enqueue_first_build(viewer.clone(), 1_000 + 3_600);
        assert!(handle.get(&viewer).is_some());
        assert_eq!(queue.try_pop(), Some(viewer), "the new circle has a job");
    }

    #[test]
    fn enqueue_and_giveup_race_never_leaves_a_circle_without_a_job_or_during_a_cooldown() {
        // Review round 2, finding 3 (defect AB), replayed: races
        // `remove_after_giving_up` against a concurrent `enqueue_first_build`
        // for the same viewer, many times. Before the circle map and the
        // cooldown map shared one lock, a request could read the (still
        // empty) cooldown just before the give-up path inserted one, then
        // insert its own circle after the give-up path had already removed
        // the old one — leaving a circle behind during what should be its
        // cooldown. Whatever order the two racing operations actually run
        // in, that combination must never be observable.
        use std::sync::Barrier;

        for i in 0..2_000 {
            let handle = GraphHandle::new(10);
            let viewer = ViewerDid(format!("did:plc:race-{i}"));
            let now = 1_000_000 + i as i64;

            let barrier = Arc::new(Barrier::new(2));
            let giveup = {
                let handle = Arc::clone(&handle);
                let viewer = viewer.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    handle.remove_after_giving_up(&viewer, now, 3_600);
                })
            };
            let enqueue = {
                let handle = Arc::clone(&handle);
                let viewer = viewer.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    handle.enqueue_first_build(viewer, now);
                })
            };
            giveup.join().expect("give-up thread does not panic");
            enqueue.join().expect("enqueue thread does not panic");

            let state = handle.state.read().unwrap();
            let has_circle = state.circles.contains_key(&viewer);
            let cooling_down =
                state.cooldowns.get(&viewer).is_some_and(|&not_before| now < not_before);
            drop(state);
            assert!(
                !(has_circle && cooling_down),
                "iteration {i}: a circle exists during its own cooldown"
            );
        }
    }

    fn migrated_store() -> Store {
        Store::open_memory().expect("in-memory store opens and migrates")
    }

    #[test]
    fn restart_loads_circles() {
        // AC9, BC19: a `ready` row loads back as a `Circle` with its
        // follows; a `building_d1` row loads back and is re-enqueued.
        let store = migrated_store();
        let follows: std::collections::HashSet<u64> = [1_u64, 2].into_iter().collect();
        store
            .viewer_save_circle(
                "did:plc:ready",
                "ready",
                1_700_000_000,
                1_700_000_000,
                &[],
                &follows,
            )
            .unwrap();
        store.viewer_save_state("did:plc:building", "building_d1", 1_700_000_100).unwrap();

        let handle = GraphHandle::from_store(&store, 10).unwrap();

        let ready =
            handle.get(&ViewerDid("did:plc:ready".to_string())).expect("ready circle loaded");
        assert_eq!(ready.state, CircleState::Ready);
        assert_eq!(ready.follows, follows);

        let building =
            handle.get(&ViewerDid("did:plc:building".to_string())).expect("building circle loaded");
        assert_eq!(building.state, CircleState::BuildingD1);

        // The building_d1 row was re-enqueued: the queue's own FIFO has a
        // job for it.
        let queue = handle.queue();
        let popped = queue.try_pop().expect("building_d1 row was re-enqueued");
        assert_eq!(popped, ViewerDid("did:plc:building".to_string()));
    }

    #[test]
    fn restart_loads_checks() {
        // AC7, BC9, BC9a: a `building_fm` row loads its step 1 `follows`
        // plus whatever `checked`/`follows_me` it already has, at
        // `circle_version` 1 (its step 1 data is servable, BC8), and is
        // re-enqueued to resume at step 2. A `ready` row's `checked` and
        // `follows_me` load too, but a `ready` row is not re-enqueued
        // (spec.md `## Defaults taken`: a pending step 2 retry is not
        // resumed after a restart).
        let store = migrated_store();
        let follows: std::collections::HashSet<u64> = [1_u64].into_iter().collect();
        store
            .viewer_save_circle(
                "did:plc:fm",
                "building_fm",
                1_700_000_000,
                1_700_000_000,
                &[],
                &follows,
            )
            .unwrap();
        let checked: std::collections::HashSet<u64> = [10_u64, 20].into_iter().collect();
        let follows_me: std::collections::HashSet<u64> = [10_u64].into_iter().collect();
        store
            .viewer_save_checks("did:plc:fm", "building_fm", 1_700_000_050, &checked, &follows_me)
            .unwrap();

        let ready_follows: std::collections::HashSet<u64> = [2_u64].into_iter().collect();
        store
            .viewer_save_circle(
                "did:plc:ready",
                "ready",
                1_700_000_000,
                1_700_000_000,
                &[],
                &ready_follows,
            )
            .unwrap();
        let ready_checked: std::collections::HashSet<u64> = [30_u64].into_iter().collect();
        store
            .viewer_save_checks(
                "did:plc:ready",
                "ready",
                1_700_000_050,
                &ready_checked,
                &std::collections::HashSet::new(),
            )
            .unwrap();

        let handle = GraphHandle::from_store(&store, 10).unwrap();

        let fm =
            handle.get(&ViewerDid("did:plc:fm".to_string())).expect("building_fm circle loaded");
        assert_eq!(fm.state, CircleState::BuildingFm);
        assert_eq!(fm.follows, follows);
        assert_eq!(fm.checked, checked);
        assert_eq!(fm.follows_me, follows_me);
        assert_eq!(fm.circle_version, 1, "step 1 data is servable (BC8)");

        let ready =
            handle.get(&ViewerDid("did:plc:ready".to_string())).expect("ready circle loaded");
        assert_eq!(ready.checked, ready_checked);

        let queue = handle.queue();
        let popped = queue.try_pop().expect("the building_fm row was re-enqueued");
        assert_eq!(
            popped,
            ViewerDid("did:plc:fm".to_string()),
            "resumes at step 2, not the ready row"
        );
        assert!(queue.try_pop().is_none(), "the ready row is not re-enqueued");
    }
}
