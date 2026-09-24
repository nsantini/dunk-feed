//! `FollowsCache`, TECH-DESIGN-network-feed §6.1, §6.4, §8, story 08's
//! `## Approach`. One cache, owned by [`super::GraphHandle`] and shared by
//! every viewer's step 3 (`graph::queue::run_step3`, slice 2.0): an
//! account's degree-2 follows list is the same list no matter which
//! viewer's `d2_sample` names it, so this holds it once, in memory, keyed
//! by the account DID rather than by viewer.
//!
//! `get` is the one path that ever touches SQLite from here, and only on a
//! memory miss (BC1b): the request path (`http::viewer`, slice 3.0) never
//! calls it, reading [`FollowsCache::degree2_set`] instead, which is memory
//! only (BC11a) so a handler never blocks on a SQLite read.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::store::Store;

/// One account's cached follows: when they were fetched, and the sorted,
/// deduplicated list itself. `Arc`-shared so [`FollowsCache::get`] can hand
/// a clone to a caller without copying the list, and so a concurrent
/// [`FollowsCache::put`] never mutates a list a reader already holds.
type Entry = (i64, Arc<Vec<u64>>);

/// `RwLock<HashMap<String, (i64, Arc<Vec<u64>>)>>`, per spec.md `##
/// Approach`: an account DID maps to when its follows were last fetched and
/// the follows themselves.
#[derive(Default)]
pub struct FollowsCache {
    entries: RwLock<HashMap<String, Entry>>,
}

impl FollowsCache {
    /// An empty cache, with nothing preloaded.
    pub fn new() -> Self {
        Self::default()
    }

    /// BC1a as amended (review round 1, defect AK): fresh when `0 <= now -
    /// fetched_at < refresh_age_h * 3600`. Equal to the age or older is
    /// stale. A `fetched_at` later than `now` — a clock skew this binary
    /// never itself produces, but a restart could load from an earlier
    /// crash — yields a negative difference, which is stale too, so step 3
    /// fetches that account again rather than trusting a forward-skewed
    /// timestamp indefinitely.
    pub fn is_fresh(now: i64, fetched_at: i64, refresh_age_h: u32) -> bool {
        let age = now - fetched_at;
        age >= 0 && age < i64::from(refresh_age_h) * 3600
    }

    /// The cached entry for `account_did`, or `None` when there is none
    /// anywhere. Checks memory first; on a miss, reads `follows_cache`
    /// through `store` exactly once (BC1b) and, when a row exists, puts it
    /// in memory before returning it, so a second caller within the same
    /// process never repeats the SQLite read (BC3, the shared-fetch case
    /// this module's own `shared_fetch_once` test proves). A malformed row
    /// (`StoreError::MalformedRow`, BC6a) is handled as a missing entry:
    /// logged with no DID, returned as `Ok(None)`, and nothing is put in
    /// memory for it — the caller (step 3) fetches the account again rather
    /// than getting stuck on a row it can never read back.
    pub fn get(
        &self,
        store: &Store,
        account_did: &str,
    ) -> Result<Option<Entry>, crate::store::StoreError> {
        if let Some(entry) =
            self.entries.read().expect("FollowsCache lock poisoned").get(account_did)
        {
            return Ok(Some(entry.clone()));
        }
        match store.follows_get(account_did) {
            Ok(Some(row)) => {
                let entry = self.put(account_did, row.fetched_at, row.follows);
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(crate::store::StoreError::MalformedRow { table, column }) => {
                tracing::warn!(table, column, "graph: skipping a malformed follows_cache row");
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    /// Puts `follows` in memory for `account_did` at `fetched_at`, sorted
    /// and deduplicated first (BC1: "hashes sorted and deduplicated"), and
    /// returns the stored entry. Replaces whatever was there before, the
    /// same "insert or replace" rule `store::follows_cache::follows_put`
    /// applies to the SQLite row (BC6b) — callers that also want the row
    /// saved call `Store::follows_put` themselves; this cache never writes
    /// to SQLite on its own.
    pub fn put(&self, account_did: &str, fetched_at: i64, mut follows: Vec<u64>) -> Entry {
        follows.sort_unstable();
        follows.dedup();
        let entry: Entry = (fetched_at, Arc::new(follows));
        self.entries
            .write()
            .expect("FollowsCache lock poisoned")
            .insert(account_did.to_string(), entry.clone());
        entry
    }

    /// The union of every cached follow, across the accounts in
    /// `d2_sample` that have a memory entry (BC11a: an account with none
    /// contributes nothing, and this never reads SQLite). Used only inside
    /// `http::viewer::build_list` on a list-cache miss (BC11): the result
    /// is a temporary set, never stored back in this cache, a `Circle`, or
    /// `ViewerLists`.
    pub fn degree2_set(&self, d2_sample: &[String]) -> HashSet<u64> {
        let entries = self.entries.read().expect("FollowsCache lock poisoned");
        let mut out = HashSet::new();
        for account_did in d2_sample {
            if let Some((_, follows)) = entries.get(account_did) {
                out.extend(follows.iter().copied());
            }
        }
        out
    }

    /// Loads the `follows_cache` row for each DID in `accounts` into
    /// memory, for `GraphHandle::from_store`'s restart preload (BC12): the
    /// first request after a restart has degree-2 items with no new fetch.
    /// A DID with no row, or a malformed one, is skipped exactly as
    /// [`Self::get`] would skip it, with no error returned to the caller —
    /// one bad row must not fail the whole startup load.
    pub fn preload<'a>(
        &self,
        store: &Store,
        accounts: impl Iterator<Item = &'a str>,
    ) -> Result<(), crate::store::StoreError> {
        for account_did in accounts {
            self.get(store, account_did)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn migrated_store() -> Store {
        Store::open_memory().expect("in-memory store opens and migrates")
    }

    #[test]
    fn is_fresh_boundary() {
        // BC1a as amended (defect AK): equal to the age is stale, one
        // second younger is fresh, and a future fetched_at is stale.
        assert!(!FollowsCache::is_fresh(1_000 + 3_600, 1_000, 1));
        assert!(FollowsCache::is_fresh(1_000 + 3_599, 1_000, 1));
        assert!(!FollowsCache::is_fresh(1_000, 1_000 + 10, 1));
    }

    #[test]
    fn put_sorts_and_dedups() {
        let cache = FollowsCache::new();
        let (_, follows) = cache.put("did:plc:a", 1, vec![3, 1, 2, 1]);
        assert_eq!(*follows, vec![1, 2, 3]);
    }

    #[test]
    fn get_of_an_unknown_account_with_no_store_row_is_none() {
        let store = migrated_store();
        let cache = FollowsCache::new();
        assert_eq!(cache.get(&store, "did:plc:missing").unwrap(), None);
    }

    #[test]
    fn get_reads_sqlite_once_then_serves_from_memory() {
        let store = migrated_store();
        store.follows_put("did:plc:a", 1_700_000_000, &[2, 1]).unwrap();
        let cache = FollowsCache::new();

        let (fetched_at, follows) = cache.get(&store, "did:plc:a").unwrap().unwrap();
        assert_eq!(fetched_at, 1_700_000_000);
        assert_eq!(*follows, vec![1, 2]);
    }

    // BC1b, BC3: two viewers sampling the same account within the age share
    // one fetch. Deleting the row from the store after the first `get`
    // proves the second `get` is served from memory alone.
    #[test]
    fn shared_fetch_once() {
        let store = migrated_store();
        store.follows_put("did:plc:a", 1_700_000_000, &[1, 2, 3]).unwrap();
        let cache = FollowsCache::new();

        let first = cache.get(&store, "did:plc:a").unwrap().unwrap();

        // A second store, with no row at all: the second `get` must find
        // its answer in memory, never reaching this empty store.
        let empty_store = migrated_store();
        let second = cache.get(&empty_store, "did:plc:a").unwrap().unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn degree2_set_unions_memory_entries_only() {
        let cache = FollowsCache::new();
        cache.put("did:plc:a", 1, vec![1, 2]);
        cache.put("did:plc:b", 1, vec![2, 3]);
        // "did:plc:c" has no entry (BC11a): contributes nothing.

        let set = cache.degree2_set(&[
            "did:plc:a".to_string(),
            "did:plc:b".to_string(),
            "did:plc:c".to_string(),
        ]);

        assert_eq!(set, HashSet::from([1, 2, 3]));
    }

    #[test]
    fn degree2_set_of_an_empty_sample_is_empty() {
        let cache = FollowsCache::new();
        cache.put("did:plc:a", 1, vec![1]);
        assert_eq!(cache.degree2_set(&[]), HashSet::new());
    }

    #[test]
    fn preload_loads_named_accounts_into_memory() {
        let store = migrated_store();
        store.follows_put("did:plc:a", 1, &[1, 2]).unwrap();
        store.follows_put("did:plc:b", 1, &[3]).unwrap();
        let cache = FollowsCache::new();

        cache.preload(&store, ["did:plc:a", "did:plc:b", "did:plc:missing"].into_iter()).unwrap();

        // Now backed by an empty store: proves both entries came from the
        // preload, not from a later SQLite read.
        let empty_store = migrated_store();
        assert!(cache.get(&empty_store, "did:plc:a").unwrap().is_some());
        assert!(cache.get(&empty_store, "did:plc:b").unwrap().is_some());
        assert!(cache.get(&empty_store, "did:plc:missing").unwrap().is_none());
    }
}
