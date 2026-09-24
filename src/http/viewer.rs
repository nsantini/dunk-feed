//! Per-viewer feed lists, TECH-DESIGN-network-feed §9.3 and spec.md
//! `## Approach`. `list_for` filters the snapshot's uncapped `items` down
//! to the ones connected to a viewer's circle
//! (`graph::filter::connected_indices`), then applies the same two page
//! caps every list gets (`scorer::snapshot::caps::apply`) over just the
//! kept indices — never over `global`, the already-capped public list
//! (spec.md `## Approach`, Rejected): capping first and filtering after
//! would let a pair outside the circle take a cap slot from a pair inside
//! it, dropping the kept pair for good (BC10).
//!
//! `ViewerLists` caches the result keyed by `(viewer, generation,
//! circle_version)`: `src/http/skeleton.rs` (slice 4.0) reads it on every
//! personalised request and the personalised cursor pins the same three
//! fields (spec.md `## Approach`), so a page resumes in the exact list
//! that served the request before it. Only the current and the
//! immediately preceding entry are kept per viewer — the same
//! two-generation shape `scorer::snapshot::SnapshotHandle` already uses
//! for the global list — so a cursor issued just before a rebuild can
//! still resolve exactly, without holding every list a viewer has ever
//! seen (BC13).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::auth::ViewerDid;
use crate::graph::circle::Circle;
use crate::graph::filter::{connected_indices, FilterItem};
use crate::scorer::snapshot::{caps, FeedItem, Snapshot};

/// One viewer's capped list at a specific `(generation, circle_version)`:
/// indices into that generation's `Snapshot::items`, already filtered to
/// the viewer's circle and capped (BC9). Cheap to clone — an `Arc` around
/// the `Vec` — so a cache hit never copies the index list itself.
#[derive(Debug, Clone)]
pub struct ViewerList {
    pub generation: u64,
    pub circle_version: u64,
    pub indices: Arc<Vec<u32>>,
}

/// The two entries kept per viewer (BC13): `current` is the most recently
/// built list, `previous` is the one it displaced.
struct ViewerEntry {
    current: ViewerList,
    previous: Option<ViewerList>,
}

/// The per-viewer list cache. `src/http/skeleton.rs` (slice 4.0) holds one
/// of these on `AppState` alongside the `GraphHandle`, calling `list_for`
/// on every personalised request and `drop_viewer` from the worker's
/// drop-lists callback once a circle changes (BC7).
pub struct ViewerLists {
    entries: Mutex<HashMap<ViewerDid, ViewerEntry>>,
    /// `cfg.follows_me_depth` (network-feed story 07, BC6, BC6a): the
    /// index bound `build_list` passes to `connected_indices` for a
    /// `circle.follows_me` match. Set once at construction (`src/ingest/
    /// mod.rs`'s `start_graph_subsystem`), since the flag it comes from
    /// never changes for the life of the process.
    follows_me_depth: usize,
}

impl ViewerLists {
    /// An empty cache, holding no viewer's list yet, filtering every
    /// future `follows_me` match to the first `follows_me_depth` items
    /// (BC6, BC6a).
    pub fn new(follows_me_depth: usize) -> Self {
        ViewerLists { entries: Mutex::new(HashMap::new()), follows_me_depth }
    }

    /// The list for `viewer` at `snapshot`'s generation and `circle`'s
    /// current `circle_version`: an existing cache entry at exactly that
    /// key, whether it is the viewer's `current` or `previous` entry
    /// (BC13), or a freshly built one otherwise.
    ///
    /// A fresh build filters `snapshot.items` with `connected_indices`
    /// over `circle.follows` (BC9, BC12: an empty `follows` or no match
    /// yields an empty list), then runs `caps::apply` on the kept indices
    /// alone, in their already rank-ordered position within `snapshot.items`
    /// (BC10). The result becomes the viewer's new `current` entry,
    /// displacing the old `current` into `previous` — never displacing an
    /// already-cached `previous`, so an in-flight page against that older
    /// entry can still resolve after one rebuild.
    pub fn list_for(&self, viewer: &ViewerDid, circle: &Circle, snapshot: &Snapshot) -> ViewerList {
        let generation = snapshot.generation;
        let circle_version = circle.circle_version;

        let mut entries = self.entries.lock().expect("ViewerLists mutex poisoned");
        if let Some(entry) = entries.get(viewer) {
            if matches(&entry.current, generation, circle_version) {
                return entry.current.clone();
            }
            if let Some(previous) = &entry.previous {
                if matches(previous, generation, circle_version) {
                    return previous.clone();
                }
            }
        }

        let fresh = build_list(circle, snapshot, self.follows_me_depth);
        let previous = entries.remove(viewer).map(|entry| entry.current);
        entries.insert(viewer.clone(), ViewerEntry { current: fresh.clone(), previous });
        fresh
    }

    /// Drops every cached entry for `viewer` (BC7: called from the
    /// worker's drop-lists callback once a fresh circle swaps in), so the
    /// next request rebuilds against the new circle rather than serving a
    /// list filtered against the one it just replaced.
    pub fn drop_viewer(&self, viewer: &ViewerDid) {
        self.entries.lock().expect("ViewerLists mutex poisoned").remove(viewer);
    }
}

/// Whether `list` was built for exactly this `(generation, circle_version)`
/// pair (BC13).
fn matches(list: &ViewerList, generation: u64, circle_version: u64) -> bool {
    list.generation == generation && list.circle_version == circle_version
}

/// Filters `snapshot.items` to the ones connected to `circle`
/// (`connected_indices`, BC9, BC12; network-feed story 07 BC5, BC6, BC6a
/// for the `follows_me_depth` bound on a `follows_me`-only match), then
/// applies the two page caps to the kept indices alone (BC10).
/// `snapshot.items` is already rank-ordered (`scorer::snapshot::build`'s
/// `sort_by_rank`), and `connected_indices` preserves input order, so the
/// kept indices `caps::apply` receives are still in rank order — the same
/// precondition `caps::apply` already assumes for the global list.
fn build_list(circle: &Circle, snapshot: &Snapshot, follows_me_depth: usize) -> ViewerList {
    let items: &[FeedItem] = snapshot.items.as_slice();
    let filter_items: Vec<FilterItem> = items
        .iter()
        .map(|item| FilterItem { quote_did: item.quote_did, original_did: item.original_did })
        .collect();

    // No degree-2 sample yet (story 08 non-goal, spec.md `## Non-goals`).
    let d2_set: HashSet<u64> = HashSet::new();
    let kept = connected_indices(&filter_items, circle, &d2_set, follows_me_depth);
    let capped = caps::apply(items, &kept);

    ViewerList {
        generation: snapshot.generation,
        circle_version: circle.circle_version,
        indices: Arc::new(capped),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::hash_did;

    fn item(
        quote_uri: &str,
        quote_cid: &str,
        quote_did: u64,
        original_did: u64,
        quoted_at: i64,
        rank: f64,
    ) -> FeedItem {
        FeedItem {
            quote_uri: quote_uri.to_string(),
            quote_cid: quote_cid.to_string(),
            rank,
            ratio: 1.0,
            quote_did,
            original_did,
            quoted_at,
            promoted_at: quoted_at,
        }
    }

    fn snapshot(generation: u64, items: Vec<FeedItem>) -> Snapshot {
        let global: Vec<u32> = (0..items.len() as u32).collect();
        Snapshot { generation, items: Arc::new(items), global: Arc::new(global) }
    }

    fn ready_circle(follows: &[u64], circle_version: u64) -> Circle {
        let mut circle = Circle::new();
        circle.follows = follows.iter().copied().collect();
        circle.circle_version = circle_version;
        circle
    }

    /// A circle whose only connection to `follows_me` authors is through
    /// `follows_me` itself, not `follows` (network-feed story 07).
    fn circle_with_follows_me(follows: &[u64], follows_me: &[u64], circle_version: u64) -> Circle {
        let mut circle = ready_circle(follows, circle_version);
        circle.follows_me = follows_me.iter().copied().collect();
        circle
    }

    // BC12: an empty `follows` keeps nothing.
    #[test]
    fn empty_follows_yields_empty_list() {
        let circle = ready_circle(&[], 1);
        let snap = snapshot(1, vec![item("at://q/1", "cid1", 1, 2, 1_700_000_000, 3.0)]);
        let lists = ViewerLists::new(0);
        let viewer = ViewerDid("did:plc:viewer".to_string());

        let list = lists.list_for(&viewer, &circle, &snap);

        assert!(list.indices.is_empty());
    }

    // AC5, BC9, BC10: the filter runs before the caps, so a pair outside
    // the circle can never take cap 1's slot from a pair inside it. Both
    // items share `original_did` X and the same day (cap 1's key); the
    // higher-rank item A is not connected to the viewer at all, the
    // lower-rank item B is connected through its quoter. Capping first
    // (the rejected approach) would keep only A — the higher-rank row for
    // that key — and then drop it on the connectivity filter, losing B
    // for a slot A never earned a right to. Filtering first drops A
    // immediately, so cap 1 sees only B and keeps it.
    #[test]
    fn caps_after_filter() {
        let original = hash_did("did:plc:original");
        let connected_quoter = hash_did("did:plc:connected-quoter");
        let outside_quoter = hash_did("did:plc:outside-quoter");
        let day = 1_700_000_000i64;

        let item_a = item("at://q/a", "cid-a", outside_quoter, original, day, 10.0);
        let item_b = item("at://q/b", "cid-b", connected_quoter, original, day, 5.0);
        let snap = snapshot(1, vec![item_a, item_b]);

        let circle = ready_circle(&[connected_quoter], 1);
        let lists = ViewerLists::new(0);
        let viewer = ViewerDid("did:plc:viewer".to_string());

        let list = lists.list_for(&viewer, &circle, &snap);

        assert_eq!(list.indices.as_slice(), &[1], "only B, the connected pair, survives");
    }

    // AC6: a viewer whose circle connects to only one of many feed items
    // is served exactly that one item — nothing from the rest of the
    // (larger) snapshot pads the list out.
    #[test]
    fn short_list_no_fallback() {
        let connected = hash_did("did:plc:connected");
        let mut items = vec![item(
            "at://q/connected",
            "cid-connected",
            connected,
            hash_did("did:plc:other-original"),
            1_700_000_000,
            50.0,
        )];
        for i in 0..20 {
            items.push(item(
                &format!("at://q/{i}"),
                &format!("cid-unconnected-{i}"),
                hash_did(&format!("did:plc:quoter-{i}")),
                hash_did(&format!("did:plc:original-{i}")),
                1_700_000_000,
                40.0 - i as f64,
            ));
        }
        let snap = snapshot(1, items);
        let circle = ready_circle(&[connected], 1);
        let lists = ViewerLists::new(0);
        let viewer = ViewerDid("did:plc:viewer".to_string());

        let list = lists.list_for(&viewer, &circle, &snap);

        assert_eq!(list.indices.as_slice(), &[0], "only the one connected item is served");
    }

    // BC13: the cache returns the same list, by reference, for a repeated
    // call at the same (generation, circle_version) — a cache hit, not a
    // rebuild — and a fresh build at a new circle_version becomes the new
    // `current`, with the old `current` retained as `previous`.
    #[test]
    fn cache_keeps_current_and_previous_by_generation_and_circle_version() {
        let connected = hash_did("did:plc:connected");
        let snap = snapshot(
            1,
            vec![item("at://q/1", "cid1", connected, hash_did("did:plc:o"), 1_700_000_000, 1.0)],
        );
        let circle_v1 = ready_circle(&[connected], 1);
        let lists = ViewerLists::new(0);
        let viewer = ViewerDid("did:plc:viewer".to_string());

        let first = lists.list_for(&viewer, &circle_v1, &snap);
        let again = lists.list_for(&viewer, &circle_v1, &snap);
        assert!(Arc::ptr_eq(&first.indices, &again.indices), "same key must hit the cache");

        // A new circle_version (the worker swapped in a fresh circle)
        // builds a fresh entry; the old one is still reachable as
        // `previous` at its own key.
        let circle_v2 = ready_circle(&[connected], 2);
        let second = lists.list_for(&viewer, &circle_v2, &snap);
        assert_eq!(second.circle_version, 2);

        let still_v1 = lists.list_for(&viewer, &circle_v1, &snap);
        assert!(
            Arc::ptr_eq(&first.indices, &still_v1.indices),
            "the displaced entry must still resolve from `previous`"
        );
    }

    // AC2; BC5, BC6, BC6a: a `follows_me`-only match is kept while its
    // index in the uncapped snapshot is below `follows_me_depth`, and
    // dropped once the index reaches the depth. `author` is the quoter at
    // index 0 and the original at index 1 (both below depth 2, both kept)
    // and the quoter again at index 2 (at the depth bound, dropped).
    #[test]
    fn follows_me_depth() {
        let author = hash_did("did:plc:follows-me-author");
        let items = vec![
            item("at://q/0", "cid0", author, hash_did("did:plc:orig0"), 1_700_000_000, 30.0),
            item("at://q/1", "cid1", hash_did("did:plc:quoter1"), author, 1_700_000_000, 20.0),
            item("at://q/2", "cid2", author, hash_did("did:plc:orig2"), 1_700_000_000, 10.0),
        ];
        let snap = snapshot(1, items);
        let circle = circle_with_follows_me(&[], &[author], 1);
        let lists = ViewerLists::new(2);
        let viewer = ViewerDid("did:plc:viewer".to_string());

        let list = lists.list_for(&viewer, &circle, &snap);

        assert_eq!(
            list.indices.as_slice(),
            &[0, 1],
            "index 2 sits at the depth bound and is dropped"
        );
    }

    // AC3, BC7: once step 2 adds `follows_me`, the item step 1 already kept
    // through `circle.follows` is still kept, alongside the new
    // `follows_me` match.
    #[test]
    fn step2_keeps_step1_items() {
        let step1_author = hash_did("did:plc:step1-author");
        let fm_author = hash_did("did:plc:fm-author");
        let items = vec![
            item(
                "at://q/step1",
                "cid-s1",
                step1_author,
                hash_did("did:plc:orig-s1"),
                1_700_000_000,
                30.0,
            ),
            item(
                "at://q/fm",
                "cid-fm",
                fm_author,
                hash_did("did:plc:orig-fm"),
                1_700_000_000,
                20.0,
            ),
        ];
        let snap = snapshot(1, items);
        let circle = circle_with_follows_me(&[step1_author], &[fm_author], 1);
        let lists = ViewerLists::new(5);
        let viewer = ViewerDid("did:plc:viewer".to_string());

        let list = lists.list_for(&viewer, &circle, &snap);

        assert_eq!(
            list.indices.as_slice(),
            &[0, 1],
            "the step 1 item and the new follows_me item both survive"
        );
    }

    // BC7: `drop_viewer` clears both entries, so the next call rebuilds
    // rather than serving a stale list.
    #[test]
    fn drop_viewer_clears_the_cache() {
        let connected = hash_did("did:plc:connected");
        let snap = snapshot(
            1,
            vec![item("at://q/1", "cid1", connected, hash_did("did:plc:o"), 1_700_000_000, 1.0)],
        );
        let circle = ready_circle(&[connected], 1);
        let lists = ViewerLists::new(0);
        let viewer = ViewerDid("did:plc:viewer".to_string());

        let first = lists.list_for(&viewer, &circle, &snap);
        lists.drop_viewer(&viewer);
        let after_drop = lists.list_for(&viewer, &circle, &snap);

        assert!(
            !Arc::ptr_eq(&first.indices, &after_drop.indices),
            "a dropped viewer must rebuild, not hit a stale cache entry"
        );
    }
}
