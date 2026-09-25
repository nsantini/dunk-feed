//! Viewer graph, TECH-DESIGN-network-feed §6. `circle` holds the per-viewer
//! `Circle`; `build` runs the three first-build steps over a `GraphSource`
//! so `graph_probe` (story 03) and the worker (`queue`, story 06) share one
//! implementation. This module holds [`GraphHandle`], the in-memory index
//! the handler reads without an await and the worker writes into, and the
//! startup load that makes a restart pick up circles saved in SQLite
//! (BC19) instead of treating every viewer as a first open.

mod queue;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use xxhash_rust::xxh3::xxh3_64;

pub use crate::auth::ViewerDid;
use crate::store::{Store, StoreError};

pub mod build;
pub mod cache;
pub mod circle;
pub mod filter;
pub mod metrics;
mod schedule;

pub use cache::FollowsCache;
pub use circle::Circle;
#[allow(unused_imports)] // QueueDepth: no production reader yet, slice 2.0 is the first.
pub use queue::{run_worker, DropListsFn, Job, JobQueue, QueueDepth};
pub use schedule::run_scheduler;

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
/// §8. Story 07 added `BuildingFm`; story 08 (this) adds `BuildingD2`.
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
    /// Step 2 has saved successfully at least once and step 3
    /// (`graph::queue::run_step3`, story 08 spec.md BC5a, BC5c) is queued,
    /// running, or was interrupted by a restart and re-enqueued (BC12a).
    /// `follows`, `d2_sample`, `checked` and `follows_me` are all complete
    /// and safe to serve; the shared `FollowsCache` may hold no entry yet,
    /// a stale one, or the fresh one step 3 is building.
    BuildingD2,
    /// Step 2 has saved successfully at least once and step 3 has completed
    /// too, or the worker gave up retrying step 2 without deleting the
    /// viewer (BC3, BC4b, BC5). `checked` and `follows_me` may still be
    /// partial in the step 2 give-up case.
    Ready,
}

impl CircleState {
    /// The `viewers.state` text this variant is stored and loaded as.
    pub fn as_str(&self) -> &'static str {
        match self {
            CircleState::BuildingD1 => "building_d1",
            CircleState::BuildingFm => "building_fm",
            CircleState::BuildingD2 => "building_d2",
            CircleState::Ready => "ready",
        }
    }

    /// Parses a stored `viewers.state` value. Falls back to `BuildingD1`
    /// for anything but exactly `"ready"`, `"building_fm"` or
    /// `"building_d2"`, rather than raising an error: an unrecognised state
    /// can only mean a build attempt was interrupted before it finished
    /// (this binary is the only writer), and treating it as still building
    /// step 1 safely re-enqueues it (BC19) instead of failing the whole
    /// startup load over one row.
    fn from_store_str(state: &str) -> Self {
        if state == CircleState::Ready.as_str() {
            CircleState::Ready
        } else if state == CircleState::BuildingFm.as_str() {
            CircleState::BuildingFm
        } else if state == CircleState::BuildingD2.as_str() {
            CircleState::BuildingD2
        } else {
            CircleState::BuildingD1
        }
    }
}

/// Why [`GraphHandle::evict`] removed a circle (story 09 spec.md BC11): the
/// idle rule (a circle whose effective `last_request_at` has not moved in
/// `UPSTAGE_GRAPH_IDLE_EVICT_D` days, `graph::schedule`) or the
/// `UPSTAGE_MAX_VIEWERS` cap (`GraphHandle::enqueue_first_build`, BC10).
/// `as_str` is the one field the `graph.evicted` log line carries — no DID,
/// no handle, no hash (BC11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictReason {
    Idle,
    Lru,
}

impl EvictReason {
    /// The `reason` field's value on the `graph.evicted` line (BC11).
    pub fn as_str(&self) -> &'static str {
        match self {
            EvictReason::Idle => "idle",
            EvictReason::Lru => "lru",
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
    /// The one `FollowsCache` every viewer's degree-2 lookups share (story
    /// 08 spec.md `## Approach`). Step 3 (`graph::queue::run_step3`, slice
    /// 2.0) writes it; `http::viewer::build_list` (slice 3.0) reads it
    /// through [`Self::follows_cache`].
    follows_cache: Arc<FollowsCache>,
    /// A clone of the store [`Self::from_store`] loaded from, or a private
    /// in-memory one for [`Self::new`] (story 09 spec.md `## Defaults
    /// taken`): [`Self::evict`] has no other way to reach SQLite, since the
    /// resolver hook and `enqueue_first_build`'s LRU path that call it carry
    /// no `Store` of their own.
    store: Store,
    /// The worker's own drop-lists callback, registered once by
    /// [`Self::set_drop_lists`] (`src/ingest/mod.rs`'s `start_graph_
    /// subsystem`), so [`Self::evict`] can drop the evicted viewer's cached
    /// list the same way a fresh circle's own swap already does (BC11a).
    /// `None` until registered — every test handle that never calls
    /// [`Self::set_drop_lists`] just evicts with no list to drop.
    drop_lists: Mutex<Option<DropListsFn>>,
    /// Evictions in the last hour, by [`EvictReason`] (story 10 spec.md BC6):
    /// [`Self::evict_locked`] is this counter's one writer.
    /// [`Self::evict_counts`] is the hourly metrics task's reader (slice
    /// 2.0).
    evictions: Arc<metrics::EvictCounters>,
}

impl GraphHandle {
    /// An empty handle with no circles and no jobs queued, at most
    /// `max_viewers` circles held at once (story 09 spec.md BC10: LRU
    /// eviction, not refusal, once the cap is reached). Backed by its own
    /// private in-memory store, so [`Self::evict`] always has a `Store` to
    /// delete from even when no caller ever built this handle from a real
    /// one (`## Defaults taken`) — most of this module's own tests take
    /// this path and never touch SQLite otherwise. No production caller:
    /// `start_graph_subsystem` (`src/ingest/mod.rs`) always builds a handle
    /// through [`Self::from_store`] instead, so a restart picks up circles
    /// already saved in SQLite (BC19); this module's own tests, and
    /// `http::skeleton`'s, are this function's only callers.
    #[allow(dead_code)]
    pub fn new(max_viewers: usize) -> Arc<Self> {
        let store =
            Store::open_memory().expect("GraphHandle::new: in-memory store opens and migrates");
        Self::with_store(max_viewers, store)
    }

    /// [`Self::new`] and [`Self::from_store`]'s shared constructor.
    fn with_store(max_viewers: usize, store: Store) -> Arc<Self> {
        Arc::new(GraphHandle {
            state: RwLock::new(GraphState::default()),
            queue: JobQueue::new(),
            max_viewers,
            touches: Mutex::new(HashMap::new()),
            follows_cache: Arc::new(FollowsCache::new()),
            store,
            drop_lists: Mutex::new(None),
            evictions: Arc::new(metrics::EvictCounters::new()),
        })
    }

    /// The evictions counted since the last call to this method, by
    /// [`EvictReason`], reset by reading them (story 10 spec.md BC6, BC9). No
    /// production caller yet: `src/graph/metrics.rs`'s hourly task (slice
    /// 2.0) is the first.
    #[allow(dead_code)]
    pub fn evict_counts(&self) -> (u64, u64) {
        self.evictions.take()
    }

    /// Registers the callback `src/ingest/mod.rs`'s `start_graph_subsystem`
    /// also hands the worker (`graph::queue::DropListsFn`), so
    /// [`Self::evict`] can drop the evicted viewer's cached list too
    /// (BC11a). Called once, right after both the handle and the callback
    /// exist; a handle no caller ever registers one for just evicts with no
    /// list to drop — every test that builds a handle through [`Self::new`]
    /// alone.
    pub fn set_drop_lists(&self, drop_lists: DropListsFn) {
        *self.drop_lists.lock().expect("GraphHandle drop_lists mutex poisoned") = Some(drop_lists);
    }

    /// Loads every `viewers` row from `store` (BC19, BC9), builds a circle
    /// for each — `checked` and `follows_me` included (BC9) — and
    /// re-enqueues a `FirstBuild` job for any row still in `building_d1`,
    /// `building_fm` or `building_d2` (story 08 BC12a): a restart mid-build
    /// picks the job back up instead of losing it, resuming at step 2 for a
    /// `building_fm` row (BC9a) or at step 3 only for a `building_d2` row
    /// (`graph::queue::process_job`'s BC5c routing), rather than repeating
    /// earlier steps. A `ready` row's step 2 retry, if one was pending at
    /// the moment of the restart, is not resumed (spec.md `## Defaults
    /// taken`): its data is already good enough to serve, and a future
    /// refresh (story 09) is what reruns step 2 for it. Loaded circles
    /// bypass the `UPSTAGE_MAX_VIEWERS` cap [`Self::enqueue_first_build`]
    /// enforces: they already exist in SQLite, so refusing to load one back
    /// into memory would silently drop a viewer the store already accepted.
    /// If that leaves more circles in memory than the cap allows,
    /// [`Self::enqueue_first_build`]'s next call evicts as many of the oldest
    /// as it takes to get back under it (review round 1, defect AN), not
    /// just one. Also preloads the shared [`FollowsCache`] with every `follows_cache`
    /// row a loaded circle's `d2_sample` names (BC12), regardless of that
    /// circle's own state.
    pub fn from_store(store: &Store, max_viewers: usize) -> Result<Arc<Self>, StoreError> {
        let handle = Self::with_store(max_viewers, store.clone());
        let rows = store.viewer_load_all()?;
        let mut d2_accounts: std::collections::HashSet<String> = std::collections::HashSet::new();
        for row in rows {
            let viewer = ViewerDid(row.viewer_did);
            let state = CircleState::from_store_str(&row.state);
            d2_accounts.extend(row.d2_sample.iter().cloned());
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
            if state == CircleState::BuildingD1
                || state == CircleState::BuildingFm
                || state == CircleState::BuildingD2
            {
                // BC12a: a building_d2 row resumes at step 3 only
                // (`graph::queue::process_job`'s BC5c routing).
                handle.queue.push(Job::FirstBuild(viewer));
            }
        }
        // BC12: preload every follows_cache row a loaded circle's
        // d2_sample names, so the first request after a restart has
        // degree-2 items with no new fetch. A missing or malformed row is
        // skipped by `FollowsCache::preload` itself, logged with no DID.
        handle.follows_cache.preload(store, d2_accounts.iter().map(String::as_str))?;
        Ok(handle)
    }

    /// The queue the worker (`queue::run_worker`) drains. `Arc`-shared so
    /// the worker task can own a clone independent of this handle's own
    /// lifetime.
    pub fn queue(&self) -> Arc<JobQueue> {
        Arc::clone(&self.queue)
    }

    /// The [`FollowsCache`] every viewer's degree-2 lookups share.
    /// `Arc`-shared so the worker task and the HTTP handler each hold a
    /// clone independent of this handle's own lifetime, the same pattern
    /// [`Self::queue`] uses. `graph::queue::run_step3` (slice 2.0) is the
    /// first production caller; `http::viewer` (slice 3.0) is the second.
    pub fn follows_cache(&self) -> Arc<FollowsCache> {
        Arc::clone(&self.follows_cache)
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
    /// queued or running (BC4, BC5, BC6), or `viewer` is still cooling down
    /// after a give-up (defect W, BC10a). At `max_viewers` circles, the
    /// circle with the oldest effective `last_request_at`
    /// ([`Self::effective_last_request_at`]) is evicted first (story 09
    /// spec.md BC10), replacing story 06's cap refusal — this handle never
    /// again refuses a first build just because it is full. The cooldown
    /// check, the LRU pick-and-evict, and the circle insert all happen
    /// under one write lock (review round 2, defect AB; extended here to
    /// cover the eviction step too), the same lock
    /// [`Self::remove_after_giving_up`] takes: a concurrent give-up is
    /// fully applied or not started at all when this runs, so this either
    /// sees the viewer's old circle (BC5, no new job needed) or its
    /// cooldown (no circle created), never neither.
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
        // Review round 1, defect AN: a loop, not a single eviction — a
        // restart can load more than `max_viewers` circles at once
        // (`Self::from_store` bypasses the cap, spec.md `## Approach`), so
        // one eviction is not always enough to get back under it before this
        // viewer's own circle is inserted.
        while state.circles.len() >= self.max_viewers {
            let victim = state
                .circles
                .iter()
                .map(|(v, c)| (v.clone(), self.effective_last_request_at(v, c)))
                .min_by_key(|(_, effective)| *effective)
                .map(|(v, _)| v);
            match victim {
                Some(victim) => self.evict_locked(&mut state, &victim, EvictReason::Lru),
                // `max_viewers` is 0, or every circle vanished between the
                // `len()` check and here: nothing left to evict, so nothing
                // to build for this viewer either.
                None => return,
            }
        }
        let mut circle = Circle::new();
        // BC16: a freshly created circle's clock starts at the request
        // time, so it is never itself the very next LRU pick.
        circle.last_request_at = now;
        state.circles.insert(viewer.clone(), Arc::new(circle));
        drop(state);
        self.queue.push(Job::FirstBuild(viewer));
    }

    /// The later of `viewer`'s in-memory touch and `circle`'s own
    /// `last_request_at` (story 09 spec.md BC16): both the idle rule
    /// (`graph::schedule`) and the LRU pick in [`Self::enqueue_first_build`]
    /// read this instead of `Circle::last_request_at` alone, so a request
    /// that only ever records a touch — never triggering a worker save —
    /// still keeps a circle's clock current.
    fn effective_last_request_at(&self, viewer: &ViewerDid, circle: &Circle) -> i64 {
        let touches = self.touches.lock().expect("GraphHandle touches mutex poisoned");
        let touch = touches.get(viewer).map(|entry| entry.last_request_at).unwrap_or(0);
        touch.max(circle.last_request_at)
    }

    /// Removes `viewer`'s circle — idle (`graph::schedule`) or at the
    /// `UPSTAGE_MAX_VIEWERS` cap ([`Self::enqueue_first_build`], BC10) —
    /// its SQLite rows, its cached lists, its touch entry and its
    /// give-up attempt counts (BC11a). Sets no cooldown (BC12: the
    /// evicted viewer's very next request starts a new first build at
    /// once), unlike [`Self::remove_after_giving_up`]. Logs one `info`
    /// line, `graph.evicted`, naming only `reason` — no DID, no handle, no
    /// hash (BC11). `graph::schedule`'s idle rule is this method's first
    /// caller outside [`Self::enqueue_first_build`] (which uses
    /// [`Self::evict_locked`] directly, already holding the lock this
    /// method takes) and this module's own tests.
    pub(crate) fn evict(&self, viewer: &ViewerDid, reason: EvictReason) {
        let mut state = self.state.write().expect("GraphHandle state lock poisoned");
        self.evict_locked(&mut state, viewer, reason);
    }

    /// [`Self::evict`]'s body, taking an already-locked `state` rather than
    /// locking its own: [`Self::enqueue_first_build`]'s LRU pick already
    /// holds the write lock when it needs to evict, and `RwLock` is not
    /// reentrant.
    fn evict_locked(&self, state: &mut GraphState, viewer: &ViewerDid, reason: EvictReason) {
        self.evictions.record(reason);
        state.circles.remove(viewer);
        self.touches.lock().expect("GraphHandle touches mutex poisoned").remove(viewer);
        self.queue.forget(viewer);
        if let Err(err) = self.store.viewer_delete(&viewer.0) {
            // BC11a: a failed delete is logged with no DID; memory is
            // removed regardless, freeing the slot either way. A restart's
            // `Self::from_store` would simply reload the stale row and
            // treat it as an ordinary viewer, the same accepted limitation
            // `graph::queue::handle_step1_failure`'s own best-effort delete
            // already documents.
            tracing::warn!(kind = ?err, "graph: failed to delete viewer row during eviction");
        }
        if let Some(drop_lists) =
            self.drop_lists.lock().expect("GraphHandle drop_lists mutex poisoned").as_ref()
        {
            drop_lists(&viewer.0);
        }
        tracing::info!(reason = reason.as_str(), "graph.evicted");
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

    /// Promotes the circle already held for `viewer` to `CircleState::Ready`
    /// in place, bumping `circle_version` the same as [`Self::swap_circle`]
    /// (review round 2, defect AH): unlike [`Self::insert_ready`], this takes
    /// no `Circle` from the caller to swap in — it only re-saves whatever
    /// data is already in memory (the last attempt that actually saved), so
    /// a step 2 attempt's own unsaved `checked`/`follows_me` can never leak
    /// into memory as `ready` just because the worker gave up retrying it.
    /// `graph::queue::handle_step2_failure` calls this at step 2's give-up
    /// (BC4b). Does nothing, under the same write lock as the read, when
    /// `viewer` has no circle (review round 2, defect AI: e.g. a concurrent
    /// step 1 give-up already removed it) — so this can never recreate a
    /// circle for a viewer the store no longer has a row for either. Returns
    /// the new `circle_version`, or `None` when there was no circle to
    /// promote.
    pub(crate) fn mark_ready(&self, viewer: &ViewerDid) -> Option<u64> {
        let mut state = self.state.write().expect("GraphHandle state lock poisoned");
        let existing = state.circles.get(viewer)?.clone();
        let version = existing.circle_version + 1;
        let mut circle = (*existing).clone();
        circle.state = CircleState::Ready;
        circle.circle_version = version;
        state.circles.insert(viewer.clone(), Arc::new(circle));
        Some(version)
    }

    /// Swaps in `circle` for `viewer` only if the circle held for it right
    /// now still has `expected_version` and is still `CircleState::Ready`
    /// (review round 1, defect AM), checked and swapped under one write
    /// lock: an eviction may have removed it while a refresh ran (story 09
    /// spec.md BC6a), or — the case `expected_version` catches that a bare
    /// existence check cannot — the viewer may have been evicted and then
    /// re-requested, so a *different*, freshly created circle (a new first
    /// build, `BuildingD1`, `circle_version` reset) now sits under the same
    /// key. A stale refresh that started against the old circle must swap
    /// nothing over that new one. Keeps the existing circle's
    /// `last_request_at` (BC5, BC16 — a refresh must never reset the idle
    /// clock a request already advanced) and bumps `circle_version` past
    /// whatever was there. `graph::queue::run_refresh` calls this after a
    /// successful save, passing the `circle_version` it read at the start of
    /// its own run. Returns the new version, or `None` (swapping nothing)
    /// when `viewer` has no circle in memory, or the one it has no longer
    /// matches.
    pub(crate) fn replace_if_present(
        &self,
        viewer: &ViewerDid,
        mut circle: Circle,
        expected_version: u64,
    ) -> Option<u64> {
        let mut state = self.state.write().expect("GraphHandle state lock poisoned");
        let existing = state.circles.get(viewer)?;
        if existing.circle_version != expected_version || existing.state != CircleState::Ready {
            return None;
        }
        let version = existing.circle_version + 1;
        circle.last_request_at = existing.last_request_at;
        circle.state = CircleState::Ready;
        circle.circle_version = version;
        state.circles.insert(viewer.clone(), Arc::new(circle));
        Some(version)
    }

    /// The viewers whose in-memory circle's `d2_sample` names `account`
    /// (story 09 spec.md BC7a): `graph::queue::run_refill` calls this to
    /// drop each one's cached list once a fresher `follows_cache` entry for
    /// that account lands.
    pub(crate) fn viewers_naming_account(&self, account: &str) -> Vec<ViewerDid> {
        let state = self.state.read().expect("GraphHandle state lock poisoned");
        state
            .circles
            .iter()
            .filter(|(_, circle)| circle.d2_sample.iter().any(|d| d == account))
            .map(|(viewer, _)| viewer.clone())
            .collect()
    }

    /// Every account named by any circle in memory's `d2_sample`, any state
    /// (story 09 spec.md BC7, BC8, BC8a): `graph::schedule`'s refill rule
    /// reads this to find which accounts might need a fresher cache entry,
    /// and its clean-up rule reads it as the set no cache entry is ever
    /// removed for, regardless of age (BC8a).
    pub(crate) fn named_d2_accounts(&self) -> HashSet<String> {
        let state = self.state.read().expect("GraphHandle state lock poisoned");
        state.circles.values().flat_map(|circle| circle.d2_sample.iter().cloned()).collect()
    }

    /// Every viewer whose in-memory circle should be evicted as idle right
    /// now (story 09 spec.md BC9): the effective `last_request_at` (BC16)
    /// has not moved in at least `idle_evict_d` days. `graph::schedule`'s
    /// idle rule reads this, then evicts each one returned, one at a time,
    /// rather than while still holding the read lock this takes.
    pub(crate) fn idle_due(&self, now: i64, idle_evict_d: u32) -> Vec<ViewerDid> {
        let cutoff_secs = i64::from(idle_evict_d) * 86_400;
        let state = self.state.read().expect("GraphHandle state lock poisoned");
        state
            .circles
            .iter()
            .filter(|(viewer, circle)| {
                now - self.effective_last_request_at(viewer, circle) >= cutoff_secs
            })
            .map(|(viewer, _)| viewer.clone())
            .collect()
    }

    /// Every viewer whose `Ready` circle is due for a `Refresh` job right now
    /// (story 09 spec.md BC2, BC2a, BC3): `now - d1_refreshed_at` (`None`
    /// treated as `0`, BC2a, so an unset refresh time is always due) is at
    /// least `refresh_age_h` hours, and the effective `last_request_at`
    /// (BC16) is later than `d1_refreshed_at` — a `Ready` circle with no
    /// request since its last refresh gets no job (BC3), and a non-`Ready`
    /// circle (still building) never does either. `graph::schedule`'s
    /// refresh rule is the only caller.
    pub(crate) fn refresh_due(&self, now: i64, refresh_age_h: u32) -> Vec<ViewerDid> {
        let age_secs = i64::from(refresh_age_h) * 3600;
        let state = self.state.read().expect("GraphHandle state lock poisoned");
        state
            .circles
            .iter()
            .filter(|(viewer, circle)| {
                if circle.state != CircleState::Ready {
                    return false;
                }
                let d1_refreshed_at = circle.d1_refreshed_at.unwrap_or(0);
                let effective = self.effective_last_request_at(viewer, circle);
                now - d1_refreshed_at >= age_secs && effective > d1_refreshed_at
            })
            .map(|(viewer, _)| viewer.clone())
            .collect()
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
    fn enqueue_first_build_at_cap_evicts_the_lru_circle() {
        // Story 09 spec.md BC10: at `max_viewers`, a new viewer still gets
        // a circle and a job — the oldest circle is evicted to make room,
        // rather than the request being refused (story 06's old behaviour,
        // replaced here).
        let handle = GraphHandle::new(1);
        let first = ViewerDid("did:plc:a".to_string());
        handle.enqueue_first_build(first.clone(), 1_700_000_000);

        let second = ViewerDid("did:plc:b".to_string());
        handle.enqueue_first_build(second.clone(), 1_700_000_100);

        assert!(handle.get(&first).is_none(), "the old, only circle was the LRU pick");
        assert!(handle.get(&second).is_some(), "the new viewer gets a circle despite the cap");
        assert_eq!(handle.state.read().unwrap().circles.len(), 1, "the cap is never exceeded");
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
        assert_eq!(
            queue.try_pop(),
            Some(Job::FirstBuild(viewer.clone())),
            "the worker picks up the first job"
        );

        // The worker's 5th failed attempt gives up.
        handle.remove_after_giving_up(&viewer, 1_000, 3_600);

        // A request lands before the worker's failure path clears the
        // queue's outstanding mark for this viewer.
        handle.enqueue_first_build(viewer.clone(), 1_000);

        // Only now does the worker finish its failure path.
        queue.complete(&Job::FirstBuild(viewer.clone()));

        assert!(handle.get(&viewer).is_none(), "no circle exists during the cooldown");
        assert!(queue.try_pop().is_none(), "no job was queued for a circle that does not exist");

        // Once the cooldown passes, a fresh request starts a real first
        // build with a job to match.
        handle.enqueue_first_build(viewer.clone(), 1_000 + 3_600);
        assert!(handle.get(&viewer).is_some());
        assert_eq!(queue.try_pop(), Some(Job::FirstBuild(viewer)), "the new circle has a job");
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
    fn lru_eviction() {
        // Story 09 spec.md BC10, BC11a, BC16: at the cap, the circle with
        // the oldest *effective* last_request_at is evicted — the later of
        // its own touch and its `Circle::last_request_at` (BC16), not
        // creation order — and eviction removes its SQLite row, its cached
        // list and its in-memory circle together (BC11a).
        let store = migrated_store();
        let handle = GraphHandle::from_store(&store, 2).expect("empty store loads");

        let touched = ViewerDid("did:plc:touched".to_string());
        let untouched = ViewerDid("did:plc:untouched".to_string());
        handle.enqueue_first_build(touched.clone(), 1_000);
        handle.enqueue_first_build(untouched.clone(), 2_000);
        // A worker already saved something for `untouched`, so there is a
        // real SQLite row for eviction to remove.
        store.viewer_save_state(&untouched.0, "building_d1", 2_000).unwrap();

        // `touched`'s own circle is older (1_000 vs. 2_000), but a later
        // touch — never a worker save — moves its *effective* clock past
        // `untouched`'s, so `untouched` is now the older of the two despite
        // being created second.
        handle.record_touch(&touched, 5_000);

        let dropped = Arc::new(Mutex::new(Vec::new()));
        let dropped_for_closure = Arc::clone(&dropped);
        handle.set_drop_lists(Arc::new(move |did: &str| {
            dropped_for_closure.lock().unwrap().push(did.to_string());
        }));

        let third = ViewerDid("did:plc:third".to_string());
        handle.enqueue_first_build(third.clone(), 6_000);

        assert!(handle.get(&untouched).is_none(), "the older-effective circle is evicted");
        assert!(handle.get(&touched).is_some(), "the touched circle survives");
        assert!(handle.get(&third).is_some(), "the new viewer still gets a circle");
        assert_eq!(handle.state.read().unwrap().circles.len(), 2, "the cap is never exceeded");

        assert!(
            store.viewer_load_all().unwrap().iter().all(|row| row.viewer_did != untouched.0),
            "BC11a: the evicted viewer's SQLite row is gone"
        );
        assert_eq!(
            dropped.lock().unwrap().as_slice(),
            std::slice::from_ref(&untouched.0),
            "BC11a: the evicted viewer's cached list is dropped"
        );

        // BC12: no cooldown was set, so the evicted viewer's very next
        // request starts a fresh first build at once.
        handle.enqueue_first_build(untouched.clone(), 6_001);
        assert!(handle.get(&untouched).is_some(), "no cooldown blocks the evicted viewer");
    }

    #[test]
    fn lru_evicts_down_to_the_cap() {
        // Review round 1, defect AN: `Self::from_store` bypasses the cap on
        // a restart, so a handle can hold more circles than `max_viewers`
        // allows. The next first build must evict as many of the oldest as
        // it takes to land back at the cap, not just one.
        let store = migrated_store();
        for i in 0..10 {
            store.viewer_save_state(&format!("did:plc:{i:02}"), "ready", 1_000 + i as i64).unwrap();
        }
        let handle = GraphHandle::from_store(&store, 5).expect("store loads all ten circles");
        assert_eq!(
            handle.state.read().unwrap().circles.len(),
            10,
            "the cap is bypassed on a restart load"
        );

        let newcomer = ViewerDid("did:plc:new".to_string());
        handle.enqueue_first_build(newcomer.clone(), 2_000);

        assert_eq!(
            handle.state.read().unwrap().circles.len(),
            5,
            "eviction ran enough times to land back at the cap"
        );
        assert!(handle.get(&newcomer).is_some(), "the new viewer still gets a circle");
    }

    #[test]
    fn eviction() {
        // Story 09 spec.md BC9, BC9a, BC14: a restart loads circles from
        // SQLite (BC19), and the first scheduler pass right after — the same
        // one `graph::schedule::run_scheduler` fires immediately on start —
        // evicts a circle idle past `idle_evict_d` (BC9), deletes a
        // `viewers` row idle the same way but with no circle in memory at
        // all (BC9a), and leaves a fresh circle untouched.
        let store = migrated_store();
        let now = 1_800_000_000_i64;
        let idle_evict_d = 7_u32;
        let cutoff_secs = i64::from(idle_evict_d) * 86_400;

        store.viewer_save_state("did:plc:idle", "ready", now - cutoff_secs - 10).unwrap();
        // BC9a: a row idle the same way, but no circle in memory for it —
        // `from_store` only loads `building_*` rows into a pending job, so a
        // `ready` row like this one loads a circle too. To exercise the
        // no-circle case, this row is inserted directly, after the load.
        store.viewer_save_state("did:plc:orphan-row", "ready", now - cutoff_secs - 10).unwrap();

        let handle = GraphHandle::from_store(&store, 10).expect("restart loads the ready circle");
        let idle_viewer = ViewerDid("did:plc:idle".to_string());
        assert!(handle.get(&idle_viewer).is_some(), "the idle row loaded a circle");

        let fresh_viewer = ViewerDid("did:plc:fresh".to_string());
        handle.enqueue_first_build(fresh_viewer.clone(), now);
        handle.queue().try_pop(); // drain its FirstBuild job; not this test's concern.

        // BC14: this is the restart's first scheduler pass.
        schedule::pass(&handle, &store, now, 6, idle_evict_d, 24);

        assert!(handle.get(&idle_viewer).is_none(), "BC9: the idle circle is evicted");
        assert!(handle.get(&fresh_viewer).is_some(), "the fresh circle survives");
        let remaining: Vec<String> =
            store.viewer_load_all().unwrap().into_iter().map(|row| row.viewer_did).collect();
        assert!(!remaining.contains(&idle_viewer.0), "BC11a: the evicted row is gone too");
        assert!(
            !remaining.contains(&"did:plc:orphan-row".to_string()),
            "BC9a: the row with no circle in memory is deleted too"
        );
    }

    #[test]
    fn evict_counts_by_reason_and_resets() {
        // Story 10 spec.md BC6, BC9: `evict_counts` reports evictions since
        // the last call, split by reason, and resets both to zero.
        let handle = GraphHandle::new(1);
        let first = ViewerDid("did:plc:a".to_string());
        handle.enqueue_first_build(first.clone(), 1_000);
        let second = ViewerDid("did:plc:b".to_string());
        handle.enqueue_first_build(second.clone(), 2_000); // LRU-evicts `first`.

        handle.evict(&second, EvictReason::Idle);

        assert_eq!(handle.evict_counts(), (1, 1), "one idle, one lru eviction");
        assert_eq!(handle.evict_counts(), (0, 0), "counts reset after take (BC9)");
    }

    #[test]
    fn cooling_down_viewer_never_triggers_an_eviction() {
        // BC10a: a viewer still in its own give-up cooldown gets no circle
        // and no eviction happens on its behalf, even though the handle is
        // already at its cap — the cooldown check runs before the LRU pick.
        let handle = GraphHandle::new(1);
        let cooling = ViewerDid("did:plc:cooling".to_string());
        handle.enqueue_first_build(cooling.clone(), 1_000);
        handle.remove_after_giving_up(&cooling, 1_000, 3_600); // frees the slot, cools down until 4_600.

        // A different viewer takes the freed slot, filling the cap again.
        let occupant = ViewerDid("did:plc:occupant".to_string());
        handle.enqueue_first_build(occupant.clone(), 1_100);

        // `cooling` retries while still cooling down and the handle is
        // again at its cap.
        handle.enqueue_first_build(cooling.clone(), 1_500);

        assert!(handle.get(&cooling).is_none(), "still cooling down, no circle");
        assert!(
            handle.get(&occupant).is_some(),
            "the occupant is never evicted for a cooling-down request"
        );
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
        assert_eq!(popped, Job::FirstBuild(ViewerDid("did:plc:building".to_string())));
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
            Job::FirstBuild(ViewerDid("did:plc:fm".to_string())),
            "resumes at step 2, not the ready row"
        );
        assert!(queue.try_pop().is_none(), "the ready row is not re-enqueued");
    }

    #[test]
    fn restart_loads_follows_cache() {
        // AC7; BC12, BC12a: a restart preloads every `follows_cache` row a
        // loaded circle's `d2_sample` names, with no fetch, and re-enqueues
        // a `building_d2` row to resume at step 3 only
        // (`graph::queue::process_job`'s BC5c routing).
        let store = migrated_store();
        let d2_sample = vec!["did:plc:account".to_string()];
        let follows: std::collections::HashSet<u64> = [1_u64].into_iter().collect();
        store
            .viewer_save_circle(
                "did:plc:viewer",
                "building_d1",
                1_700_000_000,
                1_700_000_000,
                &d2_sample,
                &follows,
            )
            .unwrap();
        let checked: std::collections::HashSet<u64> = [10_u64].into_iter().collect();
        store
            .viewer_save_checks(
                "did:plc:viewer",
                "building_d2",
                1_700_000_050,
                &checked,
                &std::collections::HashSet::new(),
            )
            .unwrap();
        store.follows_put("did:plc:account", 1_700_000_060, &[5_u64, 6]).unwrap();

        let handle = GraphHandle::from_store(&store, 10).unwrap();

        let circle = handle.get(&ViewerDid("did:plc:viewer".to_string())).expect("circle loaded");
        assert_eq!(circle.state, CircleState::BuildingD2);
        assert_eq!(circle.d2_sample, d2_sample);
        assert_eq!(circle.circle_version, 1, "step 1 and step 2 data are servable");

        // BC12: the preload put the row in memory at `from_store` time, with
        // no fetch through this test's `store` since — `degree2_set` never
        // reads SQLite at all (BC11a), so a non-empty result here can only
        // come from the preload.
        let cached = handle.follows_cache().degree2_set(&d2_sample);
        assert_eq!(cached, std::collections::HashSet::from([5_u64, 6]));

        // BC12a: the building_d2 row is re-enqueued, to resume at step 3
        // only.
        let queue = handle.queue();
        let popped = queue.try_pop().expect("the building_d2 row was re-enqueued");
        assert_eq!(popped, Job::FirstBuild(ViewerDid("did:plc:viewer".to_string())));
    }

    #[tokio::test]
    async fn follow_changes_after_refresh() {
        // AC5; story 09 spec.md BC5, BC13: a follow dropped and a follow
        // gained since the first build both show up in the circle once a
        // `Refresh` job runs.
        use crate::graph::queue::tests::ChangingFollowsSource;
        use crate::scorer::snapshot::SnapshotHandle;

        let handle = GraphHandle::new(10);
        let viewer = ViewerDid("did:plc:viewer".to_string());
        let source = Arc::new(ChangingFollowsSource::new(vec![
            "did:plc:1".to_string(),
            "did:plc:2".to_string(),
        ]));
        handle.enqueue_first_build(viewer.clone(), crate::store::unix_now());

        let snapshot = SnapshotHandle::new();
        snapshot.swap(Arc::new(Vec::new()), Arc::new(Vec::new()));

        let store = Store::open_memory().expect("in-memory store opens and migrates");
        let worker_handle = Arc::clone(&handle);
        let worker_source = Arc::clone(&source);
        tokio::spawn(run_worker(
            worker_handle,
            store,
            worker_source,
            10,
            None,
            snapshot,
            10,
            100,
            24,
        ));

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(circle) = handle.get(&viewer) {
                    if circle.state == CircleState::Ready {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first build completes within 5 s");

        let built_version = handle.get(&viewer).unwrap().circle_version;

        // Drop did:plc:1, add did:plc:3.
        source.set_follows(vec!["did:plc:2".to_string(), "did:plc:3".to_string()]);
        handle.queue().push(Job::Refresh(viewer.clone()));

        let refreshed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(circle) = handle.get(&viewer) {
                    if circle.circle_version > built_version {
                        return circle;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refresh completes within 5 s");

        assert!(refreshed.follows.contains(&hash_did("did:plc:2")));
        assert!(refreshed.follows.contains(&hash_did("did:plc:3")), "the new follow shows up");
        assert!(!refreshed.follows.contains(&hash_did("did:plc:1")), "the dropped follow is gone");
    }
}
