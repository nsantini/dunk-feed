//! The in-memory hot set, TECH-DESIGN section 5.2 and 14. Holds the
//! `xxh3_64` hash of every URI that is either side of a live `pairs` row, so
//! the ingest task can test a like, repost, reply or postgate subject
//! against it without a database round trip. `HotSet` never touches the
//! store or the network; `rebuild_from` takes a closure so the caller
//! decides where the URIs come from (`Store::for_each_hot_uri` in
//! production, a fixed list in a test).

use std::collections::HashSet;

use xxhash_rust::xxh3::xxh3_64;

/// A set of `xxh3_64(uri.as_bytes())` hashes. Never stores the URI itself.
#[derive(Debug, Clone, Default)]
pub struct HotSet {
    hashes: HashSet<u64>,
}

impl HotSet {
    /// An empty set.
    pub fn new() -> Self {
        HotSet { hashes: HashSet::new() }
    }

    /// Inserts `uri`'s hash. Returns whether it was new.
    pub fn insert(&mut self, uri: &str) -> bool {
        self.hashes.insert(xxh3_64(uri.as_bytes()))
    }

    /// Whether `uri`'s hash is in the set.
    pub fn contains(&self, uri: &str) -> bool {
        self.hashes.contains(&xxh3_64(uri.as_bytes()))
    }

    /// Removes `uri`'s hash. Returns whether it was present.
    pub fn remove(&mut self, uri: &str) -> bool {
        self.hashes.remove(&xxh3_64(uri.as_bytes()))
    }

    /// The number of distinct hashes held.
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    /// Whether the set holds no hash.
    #[allow(dead_code)] // clippy's `len_without_is_empty` requires this method; nothing calls it yet.
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Clears the set, then calls `source` with a callback that inserts
    /// every URI it is handed. `source` is `|f| store.for_each_hot_uri(f)`
    /// in production (BC22); a test passes a closure over a fixed list
    /// instead, so this stays runnable with no store and no runtime.
    pub fn rebuild_from<F, E>(&mut self, source: F) -> Result<(), E>
    where
        F: FnOnce(&mut dyn FnMut(&str)) -> Result<(), E>,
    {
        self.hashes.clear();
        source(&mut |uri: &str| {
            self.insert(uri);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_contains() {
        let mut hot = HotSet::new();
        let uri = "at://did:plc:abc/app.bsky.feed.post/xyz";
        assert!(!hot.contains(uri));
        assert!(hot.insert(uri));
        assert!(hot.contains(uri));
    }

    #[test]
    fn remove_then_not_contains() {
        let mut hot = HotSet::new();
        let uri = "at://did:plc:abc/app.bsky.feed.post/xyz";
        hot.insert(uri);
        assert!(hot.remove(uri));
        assert!(!hot.contains(uri));
    }

    #[test]
    fn uri_never_inserted_is_not_contained() {
        let hot = HotSet::new();
        assert!(!hot.contains("at://did:plc:abc/app.bsky.feed.post/xyz"));
    }

    #[test]
    fn remove_of_absent_uri_returns_false() {
        let mut hot = HotSet::new();
        assert!(!hot.remove("at://did:plc:abc/app.bsky.feed.post/xyz"));
    }

    #[test]
    fn len_counts_distinct_uris_and_a_repeat_insert_does_not_grow_it() {
        let mut hot = HotSet::new();
        assert_eq!(hot.len(), 0);
        assert!(hot.is_empty());

        hot.insert("at://did:plc:a/app.bsky.feed.post/1");
        hot.insert("at://did:plc:b/app.bsky.feed.post/2");
        assert_eq!(hot.len(), 2);
        assert!(!hot.is_empty());

        // A repeat insert of a URI already held does not grow the set.
        assert!(!hot.insert("at://did:plc:a/app.bsky.feed.post/1"));
        assert_eq!(hot.len(), 2);
    }

    #[test]
    fn every_uri_in_the_fixture_set_hashes_to_a_distinct_u64() {
        // The URIs this story's fixtures reference (BC21): two distinct
        // URIs never collide.
        let uris = [
            "at://did:plc:abc/app.bsky.feed.post/post1",
            "at://did:plc:abc/app.bsky.feed.post/post2",
            "at://did:plc:def/app.bsky.feed.post/quote1",
            "at://did:plc:def/app.bsky.feed.post/quote2",
            "at://did:plc:ghi/app.bsky.feed.post/reply1",
            "at://did:plc:ghi/app.bsky.feed.post/reply2",
            "at://did:plc:jkl/app.bsky.feed.post/like-subject",
            "at://did:plc:jkl/app.bsky.feed.post/repost-subject",
        ];
        let mut hashes: HashSet<u64> = HashSet::new();
        for uri in uris {
            assert!(hashes.insert(xxh3_64(uri.as_bytes())), "collision for {uri}");
        }
        assert_eq!(hashes.len(), uris.len());
    }

    #[test]
    fn rebuilds_from_pairs() {
        // TECH-DESIGN section 5.2's rebuild, driven from the same
        // `pairs::for_each_hot_uri` scan `Store::for_each_hot_uri` wraps
        // (BC22): two pairs that share one original still contribute a
        // distinct hash per distinct URI. `test_support::migrated_conn`
        // stands in for `Store::open_memory` here so this stays a
        // synchronous test with no writer thread and no runtime; the
        // ingest task drives the real thing through `Store::for_each_hot_uri`.
        let conn = crate::store::test_support::migrated_conn();
        crate::store::pairs::insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/q1",
            "at://did:plc:q",
            "cid1",
            "at://did:plc:o/app.bsky.feed.post/shared",
            "at://did:plc:o",
            1,
            1,
        )
        .unwrap();
        crate::store::pairs::insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/q2",
            "at://did:plc:q",
            "cid2",
            "at://did:plc:o/app.bsky.feed.post/shared",
            "at://did:plc:o",
            2,
            2,
        )
        .unwrap();

        let mut hot = HotSet::new();
        hot.rebuild_from(|f| crate::store::pairs::for_each_hot_uri(&conn, f)).unwrap();

        // Three distinct URIs: q1, q2 and the shared original.
        assert_eq!(hot.len(), 3);
        assert!(hot.contains("at://did:plc:q/app.bsky.feed.post/q1"));
        assert!(hot.contains("at://did:plc:q/app.bsky.feed.post/q2"));
        assert!(hot.contains("at://did:plc:o/app.bsky.feed.post/shared"));
    }
}
