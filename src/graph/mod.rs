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
/// §8. Story 06's Non-goals: no `building_fm` or `building_d2` yet — those
/// are stories 07 and 08.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CircleState {
    /// Step 1 is queued or running, or a prior attempt failed and is
    /// waiting to retry (BC7a, BC8). The default: a freshly enqueued
    /// circle starts here, before the worker has ever touched it.
    #[default]
    BuildingD1,
    /// Step 1 has saved successfully at least once (BC7).
    Ready,
}

impl CircleState {
    /// The `viewers.state` text this variant is stored and loaded as.
    pub fn as_str(&self) -> &'static str {
        match self {
            CircleState::BuildingD1 => "building_d1",
            CircleState::Ready => "ready",
        }
    }

    /// Parses a stored `viewers.state` value. Falls back to `BuildingD1`
    /// for anything but exactly `"ready"`, rather than raising an error: an
    /// unrecognised state can only mean a build attempt was interrupted
    /// before it finished (this binary is the only writer), and treating it
    /// as still building safely re-enqueues it (BC19) instead of failing
    /// the whole startup load over one row.
    fn from_store_str(state: &str) -> Self {
        if state == CircleState::Ready.as_str() {
            CircleState::Ready
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

/// The graph index the handler reads without an await (spec `## Approach`):
/// a `RwLock<HashMap<ViewerDid, Arc<Circle>>>`, plus the de-duplicated
/// queue `enqueue_first_build` sends into and the worker drains. Cloning
/// this handle (through its `Arc`) is how the worker task, the touch-flush
/// task and the HTTP handler (slice 4.0) all share the one index.
pub struct GraphHandle {
    circles: RwLock<HashMap<ViewerDid, Arc<Circle>>>,
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
            circles: RwLock::new(HashMap::new()),
            queue: JobQueue::new(),
            max_viewers,
            touches: Mutex::new(HashMap::new()),
            cap_warned_at: Mutex::new(None),
        })
    }

    /// Loads every `viewers` row from `store` (BC19), builds a circle for
    /// each, and re-enqueues a `FirstBuild` job for any row still in
    /// `building_d1` — a restart mid-build picks the job back up instead of
    /// losing it. Loaded circles bypass the `UPSTAGE_MAX_VIEWERS` cap
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
            circle.d2_sample = row.d2_sample;
            circle.last_request_at = row.last_request_at;
            circle.d1_refreshed_at = row.d1_refreshed_at;
            circle.state = state;
            circle.circle_version = if state == CircleState::Ready { 1 } else { 0 };

            handle
                .circles
                .write()
                .expect("GraphHandle circles mutex poisoned")
                .insert(viewer.clone(), Arc::new(circle));
            if state == CircleState::BuildingD1 {
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
        self.circles.read().expect("GraphHandle circles mutex poisoned").get(viewer).cloned()
    }

    /// Enqueues a `FirstBuild` job for `viewer` unless one is already
    /// queued or running (BC4, BC5, BC6) or the handle already holds
    /// `max_viewers` circles (BC6a). `now` is the unix second the request
    /// arrived, used only to rate-limit the cap warning.
    pub fn enqueue_first_build(&self, viewer: ViewerDid, now: i64) {
        let mut circles = self.circles.write().expect("GraphHandle circles mutex poisoned");
        if circles.contains_key(&viewer) {
            // BC5: a circle already exists — building or ready — so no
            // second job is queued.
            return;
        }
        if circles.len() >= self.max_viewers {
            drop(circles);
            self.warn_at_cap(now);
            return;
        }
        circles.insert(viewer.clone(), Arc::new(Circle::new()));
        drop(circles);
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

    /// Swaps in a freshly built circle for `viewer` (BC7): sets its state
    /// to `ready`, bumps `circle_version` past whatever the handle held
    /// before (or to 1, for a viewer with no prior entry), and installs the
    /// new `Arc`. Returns the new version, so the caller (the worker) can
    /// pass it to the drop-lists callback if it ever needs to.
    ///
    /// `pub(crate)`, not private: `src/http/skeleton.rs` (slice 4.0,
    /// `circle_change_mid_scroll`, BC16) is outside `graph::`'s own module
    /// tree and has no other way to simulate a worker's second save at a
    /// fixed snapshot generation — story 06 ships no refresh trigger yet
    /// (spec.md `## Non-goals`), so a test is the only caller besides
    /// `graph::queue::process_job`. A mechanical ripple, the same kind
    /// `spec.md`'s `## Defaults taken` already records for slice 2.0's
    /// one-line `src/http/skeleton.rs` edit.
    pub(crate) fn insert_ready(&self, viewer: &ViewerDid, mut circle: Circle) -> u64 {
        let mut circles = self.circles.write().expect("GraphHandle circles mutex poisoned");
        let version = circles.get(viewer).map(|c| c.circle_version + 1).unwrap_or(1);
        circle.state = CircleState::Ready;
        circle.circle_version = version;
        circles.insert(viewer.clone(), Arc::new(circle));
        version
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

        assert_eq!(handle.circles.read().unwrap().len(), 1);
    }

    #[test]
    fn enqueue_first_build_at_cap_creates_nothing() {
        // BC6a: at `max_viewers`, a new viewer gets no circle and no job.
        let handle = GraphHandle::new(1);
        handle.enqueue_first_build(ViewerDid("did:plc:a".to_string()), 1_700_000_000);

        let second = ViewerDid("did:plc:b".to_string());
        handle.enqueue_first_build(second.clone(), 1_700_000_000);

        assert!(handle.get(&second).is_none());
        assert_eq!(handle.circles.read().unwrap().len(), 1);
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
}
