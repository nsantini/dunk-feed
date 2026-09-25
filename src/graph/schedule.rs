//! The graph scheduler, TECH-DESIGN-network-feed §6.3, §6.4, §10; story 09
//! spec.md `## Approach`. [`run_scheduler`] runs [`pass`] once immediately —
//! so a restart's stale circles (BC14) are gone before the first request —
//! then every [`SCHEDULE_TICK`]. [`pass`] runs five steps in order (BC15):
//! flushing due touches to SQLite, idle eviction (both a stale circle still
//! in memory and a stale `viewers` row with none, BC9, BC9a), queueing due
//! `Refresh` jobs (BC2, BC2a, BC3), queueing due `Refill` jobs (BC7), and
//! cleaning up `follows_cache` entries no circle names any more (BC8, BC8a).
//! This one task replaces `graph::queue::run_touch_flush`: folding the flush
//! into the same pass that reads `last_request_at` for the idle rule means
//! the idle rule always sees a touch this same pass already wrote through
//! (BC15), rather than racing a separate periodic flush task.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::graph::{EvictReason, FollowsCache, GraphHandle, Job, ViewerDid};
use crate::store::{unix_now, Store};

/// How often [`run_scheduler`] wakes to run [`pass`] (design §10: "the
/// scheduler ... runs one pass at start and then every 60 s"). `tokio::time
/// ::interval`'s first tick fires immediately, so no separate call before
/// the loop is needed to satisfy BC14.
const SCHEDULE_TICK: Duration = Duration::from_secs(60);

/// Runs forever: one [`pass`] per tick of [`SCHEDULE_TICK`], the first one
/// firing as soon as this task starts (BC14). `start_graph_subsystem`
/// (`src/ingest/mod.rs`) spawns this once in place of the old
/// `graph::queue::run_touch_flush` task, only when `UPSTAGE_PERSONALISE` is
/// `true`. `refresh_age_h`, `idle_evict_d` and `d2_refresh_age_h` are
/// `cfg.graph_refresh_age_h`, `cfg.graph_idle_evict_d` and
/// `cfg.d2_refresh_age_h` respectively — the last one shared with story 08's
/// step 3 freshness window, since a `Refill` job re-fetches the same kind of
/// entry step 3 does, at the same age rule (BC7).
pub async fn run_scheduler(
    handle: Arc<GraphHandle>,
    store: Store,
    refresh_age_h: u32,
    idle_evict_d: u32,
    d2_refresh_age_h: u32,
) {
    let mut ticker = tokio::time::interval(SCHEDULE_TICK);
    loop {
        ticker.tick().await;
        pass(&handle, &store, unix_now(), refresh_age_h, idle_evict_d, d2_refresh_age_h);
    }
}

/// One scheduler pass, run at `now` (story 09 spec.md BC15's five steps, in
/// order): flush, idle eviction, refresh, refill, clean-up. Synchronous and
/// side-effecting rather than returning a plan, the same shape
/// `graph::queue::process_job`'s steps take — [`run_scheduler`] is this
/// function's only non-test caller, but taking `now` as a parameter, rather
/// than reading the clock itself, is what makes this testable without a
/// real 60 s wait.
pub(crate) fn pass(
    handle: &Arc<GraphHandle>,
    store: &Store,
    now: i64,
    refresh_age_h: u32,
    idle_evict_d: u32,
    d2_refresh_age_h: u32,
) {
    flush_touches(handle, store, now);
    evict_idle(handle, store, now, idle_evict_d);
    enqueue_refreshes(handle, now, refresh_age_h);
    enqueue_refills(handle, now, d2_refresh_age_h);
    clean_up_cache(handle, store, now, d2_refresh_age_h);
}

/// Step 1 (BC15): writes every viewer's due in-memory touch to SQLite, at
/// most one write per viewer per pass — `GraphHandle::due_flushes` marks
/// each entry it returns as flushed, so a second call inside the same pass
/// (there is none) or before the next pass's tick returns nothing for it.
/// The same write `graph::queue::run_touch_flush` used to make on its own
/// separate 5 s tick.
fn flush_touches(handle: &Arc<GraphHandle>, store: &Store, now: i64) {
    for (viewer, last_request_at) in
        handle.due_flushes(now, crate::graph::queue::TOUCH_MIN_INTERVAL_SECS)
    {
        if let Err(err) = store.viewer_touch(&viewer.0, last_request_at) {
            tracing::warn!(kind = ?err, "graph: touch flush failed");
        }
    }
}

/// Step 2 (BC9, BC9a): evicts every circle still in memory whose effective
/// `last_request_at` (BC16) has gone `idle_evict_d` days with no request —
/// through `GraphHandle::evict`, so each one logs its own `graph.evicted`
/// line (BC11) — then deletes every `viewers` row `Store::viewers_idle_since`
/// finds idle with no circle in memory at all (BC9a), silently: no circle
/// was evicted, so no `graph.evicted` line is logged for it. Runs after
/// [`flush_touches`] (BC15), so a touch this same pass just wrote is already
/// on the row `viewers_idle_since` reads.
fn evict_idle(handle: &Arc<GraphHandle>, store: &Store, now: i64, idle_evict_d: u32) {
    for viewer in handle.idle_due(now, idle_evict_d) {
        handle.evict(&viewer, EvictReason::Idle);
    }

    let cutoff = now - i64::from(idle_evict_d) * 86_400;
    match store.viewers_idle_since(cutoff) {
        Ok(idle_viewer_dids) => {
            for viewer_did in idle_viewer_dids {
                let viewer = ViewerDid(viewer_did);
                if handle.get(&viewer).is_none() {
                    if let Err(err) = store.viewer_delete(&viewer.0) {
                        tracing::warn!(
                            kind = ?err,
                            "graph: failed to delete an idle viewer row with no circle in memory"
                        );
                    }
                }
            }
        }
        Err(err) => tracing::warn!(kind = ?err, "graph: failed to read idle viewer rows"),
    }
}

/// Step 3 (BC2, BC2a, BC3): queues a `Refresh` job for every viewer
/// `GraphHandle::refresh_due` names. `JobQueue::push`'s own de-duplication
/// (BC1a) makes a second push while one is already queued, running or
/// waiting to retry a no-op, so a viewer still due at the next tick is
/// simply skipped rather than double-queued.
fn enqueue_refreshes(handle: &Arc<GraphHandle>, now: i64, refresh_age_h: u32) {
    let queue = handle.queue();
    for viewer in handle.refresh_due(now, refresh_age_h) {
        queue.push(Job::Refresh(viewer));
    }
}

/// Step 4 (BC7): queues a `Refill` job for every account any circle in
/// memory names in its `d2_sample` that has no memory entry in the shared
/// `FollowsCache`, or one at least `d2_refresh_age_h` hours old, or one
/// whose `fetched_at` is somehow later than `now` (the same forward-skew
/// rule `FollowsCache::is_fresh` applies). Reads a [`crate::graph::cache::
/// FollowsCache::snapshot`] rather than `FollowsCache::get`, so this never
/// itself reaches into SQLite (`## Defaults taken`: an account with no
/// memory entry counts as stale, even if a SQLite row for it exists) —
/// step 3 and a `Refill` job are what actually fetch and cache it.
fn enqueue_refills(handle: &Arc<GraphHandle>, now: i64, d2_refresh_age_h: u32) {
    let named = handle.named_d2_accounts();
    let cached: HashMap<String, i64> = handle.follows_cache().snapshot().into_iter().collect();
    let queue = handle.queue();
    for account in named {
        let stale = match cached.get(&account) {
            None => true,
            Some(&fetched_at) => !FollowsCache::is_fresh(now, fetched_at, d2_refresh_age_h),
        };
        if stale {
            queue.push(Job::Refill(account));
        }
    }
}

/// Step 5 (BC8, BC8a): removes every `follows_cache` entry — in memory and
/// in SQLite — that no circle in memory names any more (`GraphHandle::
/// named_d2_accounts`, the `keep` set) and whose `fetched_at` is older than
/// twice `d2_refresh_age_h` hours (the story's "older than", read the same
/// way `Store::follows_delete_older_than`'s own doc comment reads it). An
/// entry a circle still names is kept regardless of its age (BC8a).
fn clean_up_cache(handle: &Arc<GraphHandle>, store: &Store, now: i64, d2_refresh_age_h: u32) {
    let named = handle.named_d2_accounts();
    let cutoff = now - 2 * i64::from(d2_refresh_age_h) * 3600;
    let cache = handle.follows_cache();
    for (account, fetched_at) in cache.snapshot() {
        if fetched_at < cutoff && !named.contains(&account) {
            cache.remove(&account);
        }
    }
    if let Err(err) = store.follows_delete_older_than(cutoff, &named) {
        tracing::warn!(kind = ?err, "graph: follows cache clean-up failed");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::graph::circle::Circle;

    fn migrated_store() -> Store {
        Store::open_memory().expect("in-memory store opens and migrates")
    }

    #[test]
    fn refresh_rules() {
        // Story 09 spec.md BC2, BC2a, BC3: a `Ready` circle past its
        // refresh age with a request since the last refresh is due; one
        // not yet past the age, one with no request since the refresh, and
        // one still building are not. `d1_refreshed_at: None` is treated as
        // due (BC2a).
        let handle = GraphHandle::new(10);
        let now = 1_700_000_000_i64;
        let refresh_age_h = 6_u32;
        let age_secs = i64::from(refresh_age_h) * 3600;

        let due = ViewerDid("did:plc:due".to_string());
        let mut circle = Circle::new();
        circle.d1_refreshed_at = Some(now - age_secs - 10);
        circle.last_request_at = now - 5;
        handle.insert_ready(&due, circle);

        let too_young = ViewerDid("did:plc:young".to_string());
        let mut circle = Circle::new();
        circle.d1_refreshed_at = Some(now - age_secs + 100);
        circle.last_request_at = now;
        handle.insert_ready(&too_young, circle);

        let no_request = ViewerDid("did:plc:no_request".to_string());
        let mut circle = Circle::new();
        circle.d1_refreshed_at = Some(now - age_secs - 10);
        circle.last_request_at = now - age_secs - 20;
        handle.insert_ready(&no_request, circle);

        let never_refreshed = ViewerDid("did:plc:never".to_string());
        let mut circle = Circle::new();
        circle.d1_refreshed_at = None;
        circle.last_request_at = now - 1;
        handle.insert_ready(&never_refreshed, circle);

        let building = ViewerDid("did:plc:building".to_string());
        handle.enqueue_first_build(building.clone(), now);
        handle.queue().try_pop(); // drain its FirstBuild job; not this test's concern.

        let store = migrated_store();
        pass(&handle, &store, now, refresh_age_h, 7, 24);

        let mut refreshed = HashSet::new();
        while let Some(job) = handle.queue().try_pop() {
            if let Job::Refresh(viewer) = job {
                refreshed.insert(viewer);
            }
        }
        assert!(refreshed.contains(&due), "past the age, requested since: due");
        assert!(refreshed.contains(&never_refreshed), "BC2a: never refreshed is due");
        assert!(!refreshed.contains(&too_young), "not past the age yet");
        assert!(!refreshed.contains(&no_request), "BC3: no request since the last refresh");
        assert!(!refreshed.contains(&building), "still building, not Ready");
    }

    #[test]
    fn refill_and_cleanup() {
        // Story 09 spec.md BC7, BC8, BC8a: an account named by a circle in
        // memory with no cache entry or a stale one gets a Refill job; a
        // fresh, named entry does not. An unnamed, stale entry is removed
        // from memory and SQLite; a named entry survives clean-up no matter
        // its age, and an unnamed but fresh SQLite row survives too.
        let handle = GraphHandle::new(10);
        let now = 1_700_000_000_i64;
        let d2_refresh_age_h = 24_u32;
        let age_secs = i64::from(d2_refresh_age_h) * 3600;

        let viewer = ViewerDid("did:plc:viewer".to_string());
        let mut circle = Circle::new();
        circle.last_request_at = now; // recent, so the idle rule never evicts it first.
        circle.d2_sample = vec![
            "did:plc:no_entry".to_string(),
            "did:plc:stale".to_string(),
            "did:plc:fresh".to_string(),
            "did:plc:old_named".to_string(),
        ];
        handle.insert_ready(&viewer, circle);

        let cache = handle.follows_cache();
        cache.put("did:plc:stale", now - age_secs - 10, vec![1]);
        cache.put("did:plc:fresh", now - 10, vec![2]);
        cache.put("did:plc:old_named", now - 2 * age_secs - 100, vec![3]);
        // "did:plc:no_entry" has no cache entry at all (BC7).

        let store = migrated_store();
        store.follows_put("did:plc:orphan", now - 2 * age_secs - 10, &[9]).unwrap();
        store.follows_put("did:plc:orphan_fresh", now - 10, &[9]).unwrap();

        pass(&handle, &store, now, 6, 7, d2_refresh_age_h);

        let mut refills = HashSet::new();
        while let Some(job) = handle.queue().try_pop() {
            if let Job::Refill(account) = job {
                refills.insert(account);
            }
        }
        assert!(refills.contains("did:plc:no_entry"), "BC7: no cache entry is stale");
        assert!(refills.contains("did:plc:stale"), "BC7: past the refresh age is stale");
        assert!(!refills.contains("did:plc:fresh"), "a fresh entry is not stale");

        assert!(
            store.follows_get("did:plc:orphan").unwrap().is_none(),
            "BC8: unnamed and stale beyond twice the age is removed from SQLite"
        );
        assert!(
            store.follows_get("did:plc:orphan_fresh").unwrap().is_some(),
            "unnamed but fresh survives clean-up"
        );
        assert!(
            cache.snapshot().iter().any(|(account, _)| account == "did:plc:old_named"),
            "BC8a: named survives clean-up regardless of age"
        );
    }

    #[test]
    fn flush_before_idle() {
        // Story 09 spec.md BC15: the pass writes a due touch to SQLite
        // before the idle rule reads `viewers_idle_since`. A viewer with no
        // circle in memory, an old `viewers` row, but a fresh in-memory
        // touch survives: if the idle rule ran first, or read a stale row,
        // this viewer's row would be judged idle (BC9a) and deleted despite
        // the fresh touch.
        let handle = GraphHandle::new(10);
        let store = migrated_store();
        let now = 1_700_100_000_i64;
        let idle_evict_d = 7_u32;
        let cutoff_secs = i64::from(idle_evict_d) * 86_400;

        let viewer = ViewerDid("did:plc:touched_no_circle".to_string());
        store.viewer_save_state(&viewer.0, "ready", now - cutoff_secs - 100).unwrap();
        handle.record_touch(&viewer, now - 1);

        pass(&handle, &store, now, 6, idle_evict_d, 24);

        let rows = store.viewer_load_all().unwrap();
        let row = rows
            .iter()
            .find(|row| row.viewer_did == viewer.0)
            .expect("the flush updated the row before the idle rule read it, so it survives");
        assert!(row.last_request_at >= now - 1);
    }
}
