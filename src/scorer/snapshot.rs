//! Feed ordering and the atomic snapshot swap, TECH-DESIGN section 7.3.
//! `build` recomputes every `feed` row's `rank` for the current age, then
//! applies the two page caps in order: one item per original author per UTC
//! day, then one item per quoting DID per 50 items. `SnapshotHandle` is the
//! shared two-generation pointer (`Snapshot`, `Inner`, TECH-DESIGN section
//! 11.1, story 08 slice 7.0) story 08's HTTP server reads through
//! `current()` and `generations()`; the scorer's own snapshot step
//! (`src/scorer/mod.rs`) builds the new list outside the lock and calls
//! `swap` only for the atomic pointer replacement (BC33 of story 07,
//! BC49).

use std::collections::HashMap;
#[cfg(test)]
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, RwLock};

use crate::graph;
use crate::score::{self, Counts, Weights};
use crate::store::feed::FeedRow;

pub mod caps;

/// One item of the ranked feed, uncapped. Pagination (story 08) reads this
/// list only, never SQLite directly.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedItem {
    pub quote_uri: String,
    pub quote_cid: String,
    pub rank: f64,
    /// `D`, the upstage ratio `recompute_ranks` computed for this row on the
    /// same pass, not `FeedRow.ratio` (the promotion-time record, left
    /// untouched). `getFeedSkeleton`'s `feedContext` (story 08, BC24) reads
    /// this field.
    pub ratio: f64,
    /// `graph::hash_did` of the quoting DID (BC1). `caps::apply`'s cap 2
    /// groups on this field; a later per-viewer filter (story 05, 06) reads
    /// it too, so a viewer DID never has to round-trip through the store
    /// again to re-derive it.
    pub quote_did: u64,
    /// `graph::hash_did` of the original post's author DID (BC1).
    /// `caps::apply`'s cap 1 groups on this field.
    pub original_did: u64,
    /// Copied from `FeedRow.quoted_at` (BC1). `caps::apply`'s cap 1 buckets
    /// this into a UTC day with `div_euclid(86_400)`.
    pub quoted_at: i64,
    /// Copied from `FeedRow.promoted_at` (BC1), for a later viewer filter
    /// (story 05, 06) that needs to know how long ago a pair promoted,
    /// without a second lookup into `feed`.
    pub promoted_at: i64,
}

/// One generation of the feed list: the items a single scorer pass produced
/// (or, for `SnapshotHandle::new`'s generation 0, the empty starting list),
/// tagged with the pass counter that built it. Story 08 slice 7.0's cursor
/// (`src/http/cursor.rs`) carries this `generation` alongside an index so a
/// page can resume from an exact position in a list the handle still holds,
/// without rescanning (BC48, BC53).
///
/// `items` is the full ranked list, uncapped (BC2); `global` is the `01`
/// caps applied to every index (BC3), the list `getFeedSkeleton` actually
/// pages over. A cursor index (`src/http/skeleton.rs`) is a position in
/// `global`, never in `items` directly, so `global[index]` is the step from
/// a served position back to the `FeedItem` it names.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub generation: u64,
    pub items: Arc<Vec<FeedItem>>,
    pub global: Arc<Vec<u32>>,
}

/// The two generations `SnapshotHandle` keeps: `current` is what `current()`
/// and a fresh, no-cursor request read; `previous` is the generation `swap`
/// just displaced, kept only so a cursor issued against it can still resolve
/// exactly (TECH-DESIGN section 11.1's path 1) until the *next* `swap`
/// displaces it in turn.
#[derive(Debug)]
struct Inner {
    current: Snapshot,
    previous: Option<Snapshot>,
}

/// The shared snapshot pointer. Cloning `SnapshotHandle` clones the `Arc`
/// around the lock, not the list: every clone reads and writes the same
/// underlying snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotHandle {
    inner: Arc<RwLock<Inner>>,
}

impl SnapshotHandle {
    /// A fresh handle whose `current()` is an empty list at generation 0,
    /// never a panic and never `None`, before the first pass ever runs
    /// (BC41 of story 07, BC48). `previous` starts `None`: there is no
    /// generation before the first.
    pub fn new() -> Self {
        SnapshotHandle {
            inner: Arc::new(RwLock::new(Inner {
                current: Snapshot {
                    generation: 0,
                    items: Arc::new(Vec::new()),
                    global: Arc::new(Vec::new()),
                },
                previous: None,
            })),
        }
    }

    /// The current generation, `items` and `global` both. Cloning the
    /// returned `Snapshot` is cheap (two `Arc` clones and a `u64`) and never
    /// blocks a concurrent `swap`. `src/http/skeleton.rs` and
    /// `src/http/health.rs` both call this.
    pub fn current(&self) -> Snapshot {
        self.inner.read().expect("snapshot lock poisoned").current.clone()
    }

    /// Replaces the snapshot with `items` and `global` under the write
    /// lock: the generation counter increments by one, the outgoing
    /// `current` becomes `previous` (displacing whatever `previous` held
    /// before), and the new pair becomes `current` at the incremented
    /// generation (BC49). Both must already be fully built: the caller (the
    /// scorer's snapshot step) does every bit of ranking and capping work
    /// outside this call, so the lock is held only for the pointer swap and
    /// a reader never observes a partially built list (BC33 of story 07).
    /// The first pass therefore produces generation 1.
    pub fn swap(&self, items: Arc<Vec<FeedItem>>, global: Arc<Vec<u32>>) {
        let mut guard = self.inner.write().expect("snapshot lock poisoned");
        let next_generation = guard.current.generation + 1;
        let outgoing = std::mem::replace(
            &mut guard.current,
            Snapshot { generation: next_generation, items, global },
        );
        guard.previous = Some(outgoing);
    }

    /// Clones both generations under a single read lock, so `current` and
    /// `previous` always come from the same instant rather than two
    /// separate reads that a concurrent `swap` could interleave between
    /// (BC50). `src/http/skeleton.rs`'s `getFeedSkeleton` calls this exactly
    /// once per request.
    pub fn generations(&self) -> (Snapshot, Option<Snapshot>) {
        let guard = self.inner.read().expect("snapshot lock poisoned");
        (guard.current.clone(), guard.previous.clone())
    }
}

impl Default for SnapshotHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Recomputes `rank` for every row from both sides' stored `v_*` counts,
/// re-derived with the current `Weights` and `k` (round 2 finding 7, BC50),
/// rather than trusting the stored `ratio` at face value: `k` can change
/// between builds, even though the verified counts themselves are frozen
/// until the next verify. `feed.ratio` itself is left untouched as the
/// promotion-time record — only the freshly recomputed `D` local to this
/// function feeds `score::rank`. `age_hours` uses the current age. A
/// `quoted_at` in the future clamps `age_hours` at `0.0` rather than handing
/// `score::rank` a negative age (BC27).
///
/// Also returns the recomputed `D` for every row, keyed by `quote_cid`
/// (unique per row), so `push_kept` can fill `FeedItem.ratio` (story 08,
/// BC24) without re-deriving `D` from counts the snapshot does not carry.
/// A `HashMap` survives the `sort_by_rank` and both caps that reorder and
/// drop rows after this function returns.
fn recompute_ranks(
    rows: &mut [FeedRow],
    weights: &Weights,
    now: i64,
    k: f64,
) -> HashMap<String, f64> {
    let mut ratios = HashMap::with_capacity(rows.len());
    for row in rows.iter_mut() {
        let counts_q = Counts {
            likes: row.v_likes_q.max(0) as u32,
            reposts: row.v_reposts_q.max(0) as u32,
            replies: row.v_replies_q.max(0) as u32,
        };
        let counts_o = Counts {
            likes: row.v_likes_o.max(0) as u32,
            reposts: row.v_reposts_o.max(0) as u32,
            replies: row.v_replies_o.max(0) as u32,
        };
        let eq = score::engagement(&counts_q, weights);
        let eo = score::engagement(&counts_o, weights);
        let d = score::ratio(eq, eo, k);
        let age_hours = ((now - row.quoted_at) as f64 / 3600.0).max(0.0);
        row.rank = score::rank(d, eq, age_hours);
        ratios.insert(row.quote_cid.clone(), d);
    }
    ratios
}

/// `rank DESC, cid ASC`, the one comparator both `sort_by_rank` and
/// `src/http/skeleton.rs`'s page scan use (BC46, round 2 finding 6): a
/// duplicate copy of this same rule, `cursor::cmp_by_rank_then_cid`, used to
/// live in `src/http/cursor.rs` and has been deleted in its favour.
pub fn cmp_rank_then_cid(a: (f64, &str), b: (f64, &str)) -> std::cmp::Ordering {
    let (rank_a, cid_a) = a;
    let (rank_b, cid_b) = b;
    rank_b.partial_cmp(&rank_a).unwrap_or(std::cmp::Ordering::Equal).then_with(|| cid_a.cmp(cid_b))
}

/// `rank DESC, quote_cid ASC` (BC26), the same tie rule `Store::feed_rows`
/// starts from, re-applied after `recompute_ranks` changes `rank`.
fn sort_by_rank(rows: &mut [FeedRow]) {
    rows.sort_by(|a, b| cmp_rank_then_cid((a.rank, &a.quote_cid), (b.rank, &b.quote_cid)));
}

/// Turns one ranked `FeedRow` into its `FeedItem` (BC1), hashing both DIDs
/// through `graph::hash_did` rather than carrying the strings forward.
/// `ratios` is `recompute_ranks`'s output, keyed by `quote_cid`, so `build`
/// can fill `FeedItem.ratio` (BC24) from the `D` this pass recomputed,
/// without re-deriving it from counts the snapshot does not carry.
fn to_feed_item(row: FeedRow, ratios: &HashMap<String, f64>) -> FeedItem {
    // `ratios` is keyed by every row `recompute_ranks` saw this pass, so a
    // row reaching here always has an entry; `unwrap_or(0.0)` is defence in
    // depth only, never expected to fire.
    let ratio = ratios.get(&row.quote_cid).copied().unwrap_or(0.0);
    let quote_did = graph::hash_did(&row.quote_did);
    let original_did = graph::hash_did(&row.original_did);
    FeedItem {
        quote_uri: row.quote_uri,
        quote_cid: row.quote_cid,
        rank: row.rank,
        ratio,
        quote_did,
        original_did,
        quoted_at: row.quoted_at,
        promoted_at: row.promoted_at,
    }
}

/// Cap 1 (the `01` oracle): one item per `(original_did, UTC day of
/// quoted_at)`, keeping the highest rank (BC28). `rows` is already sorted
/// `rank DESC, quote_cid ASC` on entry, so keeping the first row seen for
/// each key keeps the highest-rank one and breaks a rank tie on `quote_cid
/// ASC`, matching the sort. The UTC day is `quoted_at.div_euclid(86_400)`,
/// the day number since the Unix epoch in UTC; integer division by the day
/// length is exact for this grouping and needs no calendar library.
///
/// Kept only as `build_v1_capped_oracle`'s helper (`## Approach`: the old
/// string-based `build` stays as a test-only oracle) — `caps::apply` is the
/// production cap 1 now, over `u64` hashes and `u32` indices.
#[cfg(test)]
fn apply_cap_one_per_author_per_day(rows: Vec<FeedRow>) -> Vec<FeedRow> {
    let mut seen: HashSet<(String, i64)> = HashSet::with_capacity(rows.len());
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        let day = row.quoted_at.div_euclid(86_400);
        if seen.insert((row.original_did.clone(), day)) {
            kept.push(row);
        }
    }
    kept
}

/// Adds `row` to `output`, then records its quoter in the trailing window,
/// dropping the oldest entry once the window holds more than `window` DIDs.
/// `build_v1_capped_oracle`'s own helper, mirrored by `caps::push_kept` for
/// production use over indices.
#[cfg(test)]
fn push_kept(
    row: FeedRow,
    output: &mut Vec<FeedItem>,
    window_queue: &mut VecDeque<String>,
    window_counts: &mut HashMap<String, usize>,
    window: usize,
    ratios: &HashMap<String, f64>,
) {
    window_queue.push_back(row.quote_did.clone());
    *window_counts.entry(row.quote_did.clone()).or_insert(0) += 1;
    if window_queue.len() > window {
        if let Some(old) = window_queue.pop_front() {
            if let Some(count) = window_counts.get_mut(&old) {
                *count -= 1;
                if *count == 0 {
                    window_counts.remove(&old);
                }
            }
        }
    }
    // `ratios` is keyed by every row `recompute_ranks` saw this pass, so a
    // row reaching `push_kept` always has an entry; `unwrap_or(0.0)` is
    // defence in depth only, never expected to fire.
    let ratio = ratios.get(&row.quote_cid).copied().unwrap_or(0.0);
    // BC1: the author hashes are `graph::hash_did` of the DID strings, not
    // the strings themselves — `caps::apply` and a later viewer filter
    // compare `u64`, never a string.
    let quote_did = graph::hash_did(&row.quote_did);
    let original_did = graph::hash_did(&row.original_did);
    output.push(FeedItem {
        quote_uri: row.quote_uri,
        quote_cid: row.quote_cid,
        rank: row.rank,
        ratio,
        quote_did,
        original_did,
        quoted_at: row.quoted_at,
        promoted_at: row.promoted_at,
    });
}

/// Cap 2: one item per quoting DID per 50 items (BC29 to BC32). The window
/// tracks the last 49 kept quoter DIDs (BC32's boundary: the 50th back no
/// longer blocks). At each output position the deferred queue is scanned
/// first, in rank order, for the first item whose quoter has cleared the
/// window (BC30); only when none has does the next item come off the main
/// list. A main-list item whose quoter is still in the window joins the
/// back of the deferred queue instead of being output (BC29). Once the main
/// list is exhausted, every item still stuck in the deferred queue is
/// illegal for good — the window cannot shrink without a new output — so
/// the whole remainder is dropped from this snapshot (BC31).
///
/// Round 2 finding 6 (BC49): deferred rows are stored bucketed by
/// `quote_did` in `deferred: HashMap<String, VecDeque<FeedRow>>`, popped
/// front-first from the bucket that becomes legal, rather than in one flat
/// `VecDeque<FeedRow>`. `defer_order` carries the release order — one `did`
/// entry per deferred row, in the order it was deferred — so the row
/// actually released at each step, and the whole output order, are
/// identical to the previous scan-based version.
///
/// `build_v1_capped_oracle`'s own cap 2, kept only as the `01` oracle
/// `global_matches_v1` checks against; `caps::apply` is the production cap
/// 2 now.
#[cfg(test)]
fn apply_cap_one_per_quoter_per_50(
    rows: Vec<FeedRow>,
    ratios: &HashMap<String, f64>,
) -> Vec<FeedItem> {
    const WINDOW: usize = 49;
    let mut window_queue: VecDeque<String> = VecDeque::with_capacity(WINDOW);
    let mut window_counts: HashMap<String, usize> = HashMap::new();
    let mut deferred: HashMap<String, VecDeque<FeedRow>> = HashMap::new();
    let mut defer_order: VecDeque<String> = VecDeque::new();
    let mut output: Vec<FeedItem> = Vec::with_capacity(rows.len());
    let mut main_idx = 0usize;

    let is_legal =
        |did: &str, counts: &HashMap<String, usize>| counts.get(did).copied().unwrap_or(0) == 0;

    loop {
        if let Some(release_pos) = defer_order.iter().position(|did| is_legal(did, &window_counts))
        {
            let did = defer_order.remove(release_pos).expect("position just found in this deque");
            let bucket = deferred.get_mut(&did).expect("a did in defer_order has a bucket");
            let row = bucket.pop_front().expect("a did in defer_order has at least one row");
            if bucket.is_empty() {
                deferred.remove(&did);
            }
            push_kept(row, &mut output, &mut window_queue, &mut window_counts, WINDOW, ratios);
            continue;
        }

        if main_idx < rows.len() {
            let row = rows[main_idx].clone();
            main_idx += 1;
            if is_legal(&row.quote_did, &window_counts) {
                push_kept(row, &mut output, &mut window_queue, &mut window_counts, WINDOW, ratios);
            } else {
                let did = row.quote_did.clone();
                deferred.entry(did.clone()).or_default().push_back(row);
                defer_order.push_back(did);
            }
            continue;
        }

        // Main exhausted and nothing left in `deferred` is legal (BC31).
        break;
    }

    output
}

/// The full snapshot step, TECH-DESIGN section 7.3 and section 9.1:
/// recompute rank, sort, then hash every row into a `FeedItem` with no cap
/// applied (BC2) — `items` holds every row, dropping none. `global` is
/// `caps::apply` run over every index in that order (BC3), the `01` caps
/// this pass produces. Pure and synchronous, so the caller can build it
/// outside the `SnapshotHandle`'s lock (BC33). `k` reaches here from
/// `Config` (round 2 finding 7, BC50), never a literal.
pub fn build(
    mut rows: Vec<FeedRow>,
    weights: &Weights,
    now: i64,
    k: f64,
) -> (Vec<FeedItem>, Vec<u32>) {
    let ratios = recompute_ranks(&mut rows, weights, now, k);
    sort_by_rank(&mut rows);
    let items: Vec<FeedItem> = rows.into_iter().map(|row| to_feed_item(row, &ratios)).collect();
    let indices: Vec<u32> = (0..items.len() as u32).collect();
    let global = caps::apply(&items, &indices);
    (items, global)
}

/// The `01` oracle, TECH-DESIGN section 7.3 as it stood before this story:
/// recompute rank, sort, cap 1, cap 2 — all over `FeedRow` and DID strings,
/// producing the capped `Vec<FeedItem>` `build` itself used to return.
/// `#[cfg(test)]` only: `global_matches_v1` (this module) and
/// `http::skeleton::tests::global_output_unchanged` both check the new
/// `build` plus `caps::apply` against this, never the other way around
/// (`## Approach`: "the old string-based `build` stays as a `#[cfg(test)]`
/// oracle for the regression test").
#[cfg(test)]
pub(crate) fn build_v1_capped_oracle(
    mut rows: Vec<FeedRow>,
    weights: &Weights,
    now: i64,
    k: f64,
) -> Vec<FeedItem> {
    let ratios = recompute_ranks(&mut rows, weights, now, k);
    sort_by_rank(&mut rows);
    let capped_by_author = apply_cap_one_per_author_per_day(rows);
    apply_cap_one_per_quoter_per_50(capped_by_author, &ratios)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn weights() -> Weights {
        Weights { repost: 3.0, reply: 5.0 }
    }

    fn row(
        quote_uri: &str,
        quote_did: &str,
        original_did: &str,
        quoted_at: i64,
        rank: f64,
    ) -> FeedRow {
        FeedRow {
            quote_uri: quote_uri.to_string(),
            quote_cid: format!("cid-{quote_uri}"),
            quote_did: quote_did.to_string(),
            original_did: original_did.to_string(),
            quoted_at,
            v_likes_q: 60,
            v_reposts_q: 0,
            v_replies_q: 0,
            v_likes_o: 0,
            v_reposts_o: 0,
            v_replies_o: 0,
            ratio: 12.0,
            rank,
            promoted_at: quoted_at,
            verified_at: quoted_at,
        }
    }

    // BC46: the one comparator, moved here from the now-deleted
    // `cursor::cmp_by_rank_then_cid`.
    #[test]
    fn cmp_rank_then_cid_orders_rank_desc_then_cid_asc() {
        assert_eq!(cmp_rank_then_cid((2.0, "a"), (1.0, "b")), std::cmp::Ordering::Less);
        assert_eq!(cmp_rank_then_cid((1.0, "b"), (2.0, "a")), std::cmp::Ordering::Greater);
        assert_eq!(cmp_rank_then_cid((1.0, "a"), (1.0, "b")), std::cmp::Ordering::Less);
        assert_eq!(cmp_rank_then_cid((1.0, "a"), (1.0, "a")), std::cmp::Ordering::Equal);
    }

    // BC41: a fresh handle reads an empty list, never a panic and never
    // `None`.
    #[test]
    fn fresh_handle_reads_empty() {
        let handle = SnapshotHandle::new();
        assert_eq!(*handle.current().items, Vec::<FeedItem>::new());
        assert_eq!(*handle.current().global, Vec::<u32>::new());
    }

    #[test]
    fn swap_replaces_the_snapshot() {
        let handle = SnapshotHandle::new();
        let items = vec![FeedItem {
            quote_uri: "at://did:plc:q/app.bsky.feed.post/q".to_string(),
            quote_cid: "cid-q".to_string(),
            rank: 1.0,
            ratio: 4.0,
            quote_did: 1,
            original_did: 2,
            quoted_at: 1_700_000_000,
            promoted_at: 1_700_000_000,
        }];
        let global = vec![0u32];
        handle.swap(Arc::new(items.clone()), Arc::new(global.clone()));
        assert_eq!(*handle.current().items, items);
        assert_eq!(*handle.current().global, global);
    }

    // BC48: a fresh handle's current generation is 0, with no previous.
    #[test]
    fn fresh_handle_is_generation_zero_with_no_previous() {
        let handle = SnapshotHandle::new();
        let (current, previous) = handle.generations();
        assert_eq!(current.generation, 0);
        assert!(current.items.is_empty());
        assert!(previous.is_none());
    }

    // BC49: `swap` increments the generation by one each time, and the
    // outgoing `current` becomes `previous` — so the first pass produces
    // generation 1, and a second `swap` displaces generation 1 into
    // `previous` rather than losing it outright.
    #[test]
    fn swap_increments_generation_and_keeps_the_outgoing_as_previous() {
        let handle = SnapshotHandle::new();
        let gen1_items = vec![FeedItem {
            quote_uri: "at://did:plc:q/app.bsky.feed.post/1".to_string(),
            quote_cid: "cid-1".to_string(),
            rank: 1.0,
            ratio: 1.0,
            quote_did: 1,
            original_did: 2,
            quoted_at: 1_700_000_000,
            promoted_at: 1_700_000_000,
        }];
        let gen1_global = vec![0u32];
        handle.swap(Arc::new(gen1_items.clone()), Arc::new(gen1_global.clone()));
        let (current, previous) = handle.generations();
        assert_eq!(current.generation, 1, "the first pass produces generation 1");
        assert_eq!(*current.items, gen1_items);
        assert_eq!(*current.global, gen1_global);
        let previous = previous.expect("generation 0 becomes previous, even though it was empty");
        assert_eq!(previous.generation, 0);
        assert!(previous.items.is_empty());
        assert!(previous.global.is_empty());

        let gen2_items = vec![FeedItem {
            quote_uri: "at://did:plc:q/app.bsky.feed.post/2".to_string(),
            quote_cid: "cid-2".to_string(),
            rank: 2.0,
            ratio: 2.0,
            quote_did: 1,
            original_did: 2,
            quoted_at: 1_700_000_000,
            promoted_at: 1_700_000_000,
        }];
        let gen2_global = vec![0u32];
        handle.swap(Arc::new(gen2_items.clone()), Arc::new(gen2_global.clone()));
        let (current, previous) = handle.generations();
        assert_eq!(current.generation, 2);
        assert_eq!(*current.items, gen2_items);
        assert_eq!(*current.global, gen2_global);
        let previous = previous.expect("generation 1 becomes previous");
        assert_eq!(previous.generation, 1);
        assert_eq!(*previous.items, gen1_items);
        assert_eq!(*previous.global, gen1_global);
    }

    // BC50: `generations()` clones both `Arc`s under one read lock, so it
    // never observes a torn state where `current` moved on but `previous`
    // still reflects an even earlier swap.
    #[test]
    fn generations_reads_current_and_previous_together() {
        let handle = SnapshotHandle::new();
        handle.swap(Arc::new(vec![]), Arc::new(vec![]));
        handle.swap(
            Arc::new(vec![FeedItem {
                quote_uri: "at://did:plc:q/app.bsky.feed.post/only".to_string(),
                quote_cid: "cid-only".to_string(),
                rank: 5.0,
                ratio: 5.0,
                quote_did: 1,
                original_did: 2,
                quoted_at: 1_700_000_000,
                promoted_at: 1_700_000_000,
            }]),
            Arc::new(vec![0u32]),
        );
        let (current, previous) = handle.generations();
        assert_eq!(current.generation, 2);
        let previous = previous.expect("second swap leaves a previous generation");
        assert_eq!(previous.generation, 1);
        assert!(previous.items.is_empty());
    }

    // BC27: a `quoted_at` in the future never yields a negative age, so
    // `rank` stays finite and positive rather than blowing up on a
    // fractional power of a negative base.
    #[test]
    fn recompute_ranks_clamps_future_quoted_at_to_zero_age() {
        let now = 1_700_000_000;
        let mut rows = vec![row(
            "at://did:plc:q/app.bsky.feed.post/q",
            "did:plc:q",
            "did:plc:o",
            now + 3600,
            0.0,
        )];
        recompute_ranks(&mut rows, &weights(), now, 5.0);
        assert!(rows[0].rank.is_finite());
        assert!(rows[0].rank > 0.0);
    }

    // BC50: `rank` is recomputed from both sides' stored `v_*` counts with
    // the current weights and `k`, not from the stored `ratio`; `ratio`
    // itself is left untouched as the promotion-time record.
    #[test]
    fn recompute_ranks_uses_current_weights_and_k_not_stored_ratio() {
        let now = 1_700_000_000;
        let mut r = row("at://did:plc:q/app.bsky.feed.post/q", "did:plc:q", "did:plc:o", now, 0.0);
        r.ratio = 999.0; // A stale value `recompute_ranks` must not trust.
        r.v_likes_q = 100;
        r.v_likes_o = 20;
        let mut rows = vec![r];

        recompute_ranks(&mut rows, &weights(), now, 5.0);

        // D = eq / (eo + k) = 100 / (20 + 5) = 4.0, not the stale 999.0.
        let expected_rank = score::rank(4.0, 100.0, 0.0);
        assert_eq!(rows[0].rank, expected_rank);
        assert_eq!(rows[0].ratio, 999.0, "ratio itself is left untouched");
    }

    // BC24: `build`'s output `FeedItem.ratio` is the `D` this pass
    // recomputed from the row's counts, not the stale stored `FeedRow.ratio`.
    #[test]
    fn build_sets_feed_item_ratio_from_recomputed_d_not_stored_ratio() {
        let now = 1_700_000_000;
        let mut r = row("at://did:plc:q/app.bsky.feed.post/q", "did:plc:q", "did:plc:o", now, 0.0);
        r.ratio = 999.0; // Stale; `FeedItem.ratio` must not come from here.
        r.v_likes_q = 100;
        r.v_likes_o = 20;

        let (items, global) = build(vec![r], &weights(), now, 5.0);

        // D = eq / (eo + k) = 100 / (20 + 5) = 4.0.
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].ratio, 4.0);
        assert_eq!(global, vec![0]);
    }

    // AC6, BC28: two items sharing (original_did, UTC day) keep only the
    // higher-rank one.
    #[test]
    fn cap_one_per_author_per_day() {
        let day_start = 1_700_000_000i64.div_euclid(86_400) * 86_400;
        let high = row(
            "at://did:plc:q/app.bsky.feed.post/high",
            "did:plc:q1",
            "did:plc:o",
            day_start + 100,
            5.0,
        );
        let low = row(
            "at://did:plc:q/app.bsky.feed.post/low",
            "did:plc:q2",
            "did:plc:o",
            day_start + 200,
            2.0,
        );
        let other_day = row(
            "at://did:plc:q/app.bsky.feed.post/other",
            "did:plc:q3",
            "did:plc:o",
            day_start + 90_000,
            3.0,
        );
        let mut rows = vec![high.clone(), low, other_day.clone()];
        sort_by_rank(&mut rows);

        let kept = apply_cap_one_per_author_per_day(rows);

        let uris: Vec<&str> = kept.iter().map(|r| r.quote_uri.as_str()).collect();
        assert_eq!(uris.len(), 2, "the lower-rank same-day row is dropped");
        assert!(uris.contains(&high.quote_uri.as_str()));
        assert!(uris.contains(&other_day.quote_uri.as_str()));
    }

    // BC3 boundary equivalent for cap 1: a tie on rank breaks on
    // `quote_cid ASC`, the same rule the sort itself uses.
    #[test]
    fn cap_one_per_author_per_day_ties_break_on_quote_cid() {
        let mut a = row(
            "at://did:plc:q/app.bsky.feed.post/z",
            "did:plc:q1",
            "did:plc:o",
            1_700_000_000,
            5.0,
        );
        a.quote_cid = "cid-b".to_string();
        let mut b = row(
            "at://did:plc:q/app.bsky.feed.post/y",
            "did:plc:q2",
            "did:plc:o",
            1_700_000_000,
            5.0,
        );
        b.quote_cid = "cid-a".to_string();
        let mut rows = vec![a, b];
        sort_by_rank(&mut rows);

        let kept = apply_cap_one_per_author_per_day(rows);

        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].quote_cid, "cid-a");
    }

    // AC7, BC29, BC30, BC31: an item whose quoter is still in the trailing
    // window defers to the first legal position; one still illegal when the
    // main list runs out is dropped. Only 5 filler rows separate the
    // repeat, well inside the 49-item window, so the window never sheds
    // `quoter` and the deferred row is never freed before the main list
    // runs out.
    #[test]
    fn cap_one_per_quoter_per_50() {
        let quoter = "did:plc:same";
        let mut rows = vec![row(
            "at://did:plc:q/app.bsky.feed.post/1",
            quoter,
            "did:plc:o1",
            1_700_000_000,
            10.0,
        )];
        for i in 0..5 {
            rows.push(row(
                &format!("at://did:plc:q/app.bsky.feed.post/filler{i}"),
                &format!("did:plc:filler{i}"),
                "did:plc:o2",
                1_700_000_000,
                9.0 - i as f64 * 0.01,
            ));
        }
        // Shares `quoter` with the very first row, only 5 kept items later:
        // still inside the window, so it defers. Nothing follows to free
        // the window, so it never becomes legal and is dropped (BC31).
        rows.push(row(
            "at://did:plc:q/app.bsky.feed.post/blocked",
            quoter,
            "did:plc:o3",
            1_700_000_000,
            1.0,
        ));

        let kept = apply_cap_one_per_quoter_per_50(rows, &HashMap::new());

        assert_eq!(kept.len(), 6, "the still-blocked row is dropped, the other 6 rows are kept");
        assert!(!kept
            .iter()
            .any(|item| item.quote_uri == "at://did:plc:q/app.bsky.feed.post/blocked"));
    }

    // BC30: once enough new quoters have been output to push `quoter` out
    // of the 49-item window, the deferred item is re-inserted at the very
    // next output position, ahead of a lower-rank main-list item that has
    // not been reached yet, because the deferred queue is tried first.
    // `pre` (3 rows) then `again` (deferred) then exactly 46 more `post`
    // rows brings the total kept count since `quoter`'s first row to 49,
    // which is the push that evicts it from the window; `again` should then
    // come out ahead of `tail`.
    #[test]
    fn cap_one_per_quoter_per_50_reinserts_once_legal() {
        let quoter = "did:plc:same";
        let mut rows = vec![row(
            "at://did:plc:q/app.bsky.feed.post/first",
            quoter,
            "did:plc:o1",
            1_700_000_000,
            100.0,
        )];
        for i in 0..3 {
            rows.push(row(
                &format!("at://did:plc:q/app.bsky.feed.post/pre{i}"),
                &format!("did:plc:pre{i}"),
                "did:plc:o2",
                1_700_000_000,
                90.0 - i as f64,
            ));
        }
        rows.push(row(
            "at://did:plc:q/app.bsky.feed.post/again",
            quoter,
            "did:plc:o3",
            1_700_000_000,
            50.0,
        ));
        for i in 0..46 {
            rows.push(row(
                &format!("at://did:plc:q/app.bsky.feed.post/post{i}"),
                &format!("did:plc:post{i}"),
                "did:plc:o4",
                1_700_000_000,
                40.0 - i as f64 * 0.1,
            ));
        }
        rows.push(row(
            "at://did:plc:q/app.bsky.feed.post/tail",
            "did:plc:tail",
            "did:plc:o5",
            1_700_000_000,
            -100.0,
        ));

        let kept = apply_cap_one_per_quoter_per_50(rows, &HashMap::new());

        let uris: Vec<&str> = kept.iter().map(|item| item.quote_uri.as_str()).collect();
        assert_eq!(uris.len(), 52, "every row is eventually kept, none dropped");
        let again_idx =
            uris.iter().position(|u| *u == "at://did:plc:q/app.bsky.feed.post/again").unwrap();
        let tail_idx =
            uris.iter().position(|u| *u == "at://did:plc:q/app.bsky.feed.post/tail").unwrap();
        assert!(
            again_idx < tail_idx,
            "the deferred item is tried before the next unreached main-list item"
        );
    }

    // BC32: exactly 50 items separate two same-quoter items — the window is
    // the last 49 kept quoter DIDs, so the 50th-back item no longer blocks.
    #[test]
    fn cap_one_per_quoter_per_50_boundary_at_exactly_50() {
        let quoter = "did:plc:same";
        let mut rows = vec![row(
            "at://did:plc:q/app.bsky.feed.post/1",
            quoter,
            "did:plc:o1",
            1_700_000_000,
            10.0,
        )];
        for i in 0..49 {
            rows.push(row(
                &format!("at://did:plc:q/app.bsky.feed.post/filler{i}"),
                &format!("did:plc:filler{i}"),
                "did:plc:o2",
                1_700_000_000,
                9.0 - i as f64 * 0.01,
            ));
        }
        // The 50th kept item (index 50 overall, 49 items separate the two
        // same-quoter rows): outside the 49-item trailing window, so it is
        // never deferred.
        rows.push(row(
            "at://did:plc:q/app.bsky.feed.post/50th",
            quoter,
            "did:plc:o3",
            1_700_000_000,
            1.0,
        ));

        let kept = apply_cap_one_per_quoter_per_50(rows, &HashMap::new());

        assert_eq!(kept.len(), 51);
        assert_eq!(kept.last().unwrap().quote_uri, "at://did:plc:q/app.bsky.feed.post/50th");
    }

    // BC49: two rows deferred for one DID release in the same order they
    // were deferred (rank order), proving the per-DID bucket's `VecDeque`
    // pops front-first rather than, say, last-in-first-out. Releasing
    // `first` re-adds `quoter` to the window (`push_kept` always does), so
    // `second` needs the window to clear a second time before it can
    // release: 49 filler pushes evict `head`'s window entry, freeing
    // `first`; then 49 more evict the window entry `first`'s own release
    // just added, freeing `second`.
    #[test]
    fn cap_one_per_quoter_per_50_releases_many_deferred_rows_for_one_did_in_order() {
        let quoter = "did:plc:same";
        let mut rows = vec![row(
            "at://did:plc:q/app.bsky.feed.post/head",
            quoter,
            "did:plc:o0",
            1_700_000_000,
            200.0,
        )];
        for (name, rank) in [("first", 100.0), ("second", 99.0)] {
            rows.push(row(
                &format!("at://did:plc:q/app.bsky.feed.post/{name}"),
                quoter,
                "did:plc:o1",
                1_700_000_000,
                rank,
            ));
        }
        for i in 0..98 {
            rows.push(row(
                &format!("at://did:plc:q/app.bsky.feed.post/filler{i}"),
                &format!("did:plc:filler{i}"),
                "did:plc:o2",
                1_700_000_000,
                6.0 - i as f64 * 0.01,
            ));
        }

        let kept = apply_cap_one_per_quoter_per_50(rows, &HashMap::new());

        let uris: Vec<&str> = kept.iter().map(|item| item.quote_uri.as_str()).collect();
        let first_idx =
            uris.iter().position(|u| *u == "at://did:plc:q/app.bsky.feed.post/first").unwrap();
        let second_idx =
            uris.iter().position(|u| *u == "at://did:plc:q/app.bsky.feed.post/second").unwrap();
        assert!(first_idx < second_idx, "released in the order they were deferred");
    }

    // AC6 + AC7 together, and BC26: `build` recomputes rank, re-sorts, then
    // applies both caps in order.
    #[test]
    fn build_applies_rank_then_both_caps() {
        let now: i64 = 1_700_100_000;
        let day_start = now.div_euclid(86_400) * 86_400;
        let rows = vec![
            row(
                "at://did:plc:q/app.bsky.feed.post/a",
                "did:plc:q1",
                "did:plc:o",
                day_start + 10,
                0.0,
            ),
            row(
                "at://did:plc:q/app.bsky.feed.post/b",
                "did:plc:q2",
                "did:plc:o",
                day_start + 20,
                0.0,
            ),
        ];

        let (items, global) = build(rows, &weights(), now, 5.0);

        assert_eq!(items.len(), 2, "BC2: items is uncapped, every row survives");
        assert_eq!(global.len(), 1, "cap 1 drops the second same-author-same-day row from global");
    }

    /// `row`, but with `v_likes_q` set explicitly and every other count at
    /// zero, so the row's *computed* rank (`recompute_ranks`, which ignores
    /// the fixture's own `rank` field) is driven only by `likes`: with
    /// `v_likes_o == 0` and `k` fixed, both `D = eq / (eo + k)` and
    /// `log10(1 + eq)` rise with `eq`, so a higher `likes` value always
    /// computes a higher rank for rows at the same age.
    fn row_with_likes(
        quote_uri: &str,
        quote_did: &str,
        original_did: &str,
        quoted_at: i64,
        likes: i64,
    ) -> FeedRow {
        let mut r = row(quote_uri, quote_did, original_did, quoted_at, 0.0);
        r.v_likes_q = likes;
        r
    }

    /// A fixture that exercises cap 1's drop, cap 2's deferral-then-release,
    /// and a rank tie, all through *computed* rank (`recompute_ranks`
    /// overwrites the fixture's placeholder `rank` field, so `build` must
    /// see real rank differences, not literal ones): every row shares
    /// `quoted_at == day_start` and `v_likes_o == 0`, so `likes` alone fixes
    /// the sort order.
    ///
    /// - `tie-a` and `tie-b` carry identical `likes` and distinct original
    ///   and quoting DIDs, so they compute the same rank — a real tie,
    ///   broken only by `quote_cid ASC` (BC26/BC46).
    /// - `cap1-high` and `cap1-low` share `(original_did, day)` at distinct
    ///   ranks, so cap 1 (BC4) drops `cap1-low` and keeps `cap1-high`.
    /// - `cap2-first` and `cap2-again` share quoter `repeatq`. Exactly 49
    ///   other rows (`cap2-pre0..2`, 3 rows, then `cap2-post0..45`, 46
    ///   rows), each a distinct quoter, separate them — inside the 49-item
    ///   trailing window, so `cap2-again` defers, then releases the moment
    ///   the last filler row evicts `repeatq` from the window, landing
    ///   ahead of `cap2-tail` (BC5, the same shape as
    ///   `caps::tests::cap_one_per_quoter_per_50_reinserts_once_legal`).
    fn ac1_fixture(now: i64) -> Vec<FeedRow> {
        let day_start = now.div_euclid(86_400) * 86_400;
        let mut rows = vec![
            row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/tie-a",
                "did:plc:tie-quoter-a",
                "did:plc:tie-orig-a",
                day_start,
                500,
            ),
            row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/tie-b",
                "did:plc:tie-quoter-b",
                "did:plc:tie-orig-b",
                day_start,
                500,
            ),
            row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/cap1-high",
                "did:plc:cap1-quoter-high",
                "did:plc:cap1-author",
                day_start,
                400,
            ),
            row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/cap1-low",
                "did:plc:cap1-quoter-low",
                "did:plc:cap1-author",
                day_start,
                350,
            ),
            row_with_likes(
                "at://did:plc:q/app.bsky.feed.post/cap2-first",
                "did:plc:repeatq",
                "did:plc:cap2-orig-first",
                day_start,
                300,
            ),
        ];
        for i in 0..3 {
            rows.push(row_with_likes(
                &format!("at://did:plc:q/app.bsky.feed.post/cap2-pre{i}"),
                &format!("did:plc:cap2-pre-quoter{i}"),
                &format!("did:plc:cap2-pre-orig{i}"),
                day_start,
                290 - i as i64,
            ));
        }
        rows.push(row_with_likes(
            "at://did:plc:q/app.bsky.feed.post/cap2-again",
            "did:plc:repeatq",
            "did:plc:cap2-orig-again",
            day_start,
            280,
        ));
        for i in 0..46 {
            rows.push(row_with_likes(
                &format!("at://did:plc:q/app.bsky.feed.post/cap2-post{i}"),
                &format!("did:plc:cap2-post-quoter{i}"),
                &format!("did:plc:cap2-post-orig{i}"),
                day_start,
                270 - i as i64,
            ));
        }
        rows.push(row_with_likes(
            "at://did:plc:q/app.bsky.feed.post/cap2-tail",
            "did:plc:cap2-tail-quoter",
            "did:plc:cap2-tail-orig",
            day_start,
            1,
        ));
        rows
    }

    // AC1: `global` equals the old capped list, item for item, on a fixture
    // whose computed rank (not the fixture's placeholder `rank` field, which
    // `build` overwrites) produces a real cap-1 drop, a real cap-2
    // deferral-then-release, and a real rank tie.
    #[test]
    fn global_matches_v1() {
        let now: i64 = 1_700_100_000;
        let rows = ac1_fixture(now);

        let oracle = build_v1_capped_oracle(rows.clone(), &weights(), now, 5.0);
        let (items, global) = build(rows, &weights(), now, 5.0);
        let mapped: Vec<FeedItem> = global.iter().map(|&i| items[i as usize].clone()).collect();

        // These checks prove the fixture actually exercised the four
        // events its comment above claims, so the byte-for-byte equality
        // assertion below is not vacuous.
        let contains = |list: &[FeedItem], uri: &str| list.iter().any(|i| i.quote_uri == uri);
        assert!(
            !contains(&mapped, "at://did:plc:q/app.bsky.feed.post/cap1-low"),
            "cap 1 must drop the lower-rank same-author-same-day row"
        );
        assert!(
            contains(&mapped, "at://did:plc:q/app.bsky.feed.post/cap1-high"),
            "cap 1 must keep the higher-rank same-author-same-day row"
        );

        let rank_of = |uri: &str| {
            items
                .iter()
                .find(|i| i.quote_uri == uri)
                .unwrap_or_else(|| panic!("{uri} missing"))
                .rank
        };
        assert_eq!(
            rank_of("at://did:plc:q/app.bsky.feed.post/tie-a"),
            rank_of("at://did:plc:q/app.bsky.feed.post/tie-b"),
            "the tie fixture rows must compute to the same rank"
        );

        let position = |list: &[FeedItem], uri: &str| {
            list.iter().position(|i| i.quote_uri == uri).unwrap_or_else(|| panic!("{uri} missing"))
        };
        let again_pos = position(&mapped, "at://did:plc:q/app.bsky.feed.post/cap2-again");
        let last_post_pos = position(&mapped, "at://did:plc:q/app.bsky.feed.post/cap2-post45");
        let tail_pos = position(&mapped, "at://did:plc:q/app.bsky.feed.post/cap2-tail");
        assert!(
            again_pos > last_post_pos,
            "cap 2 must defer the repeat quoter behind every filler row separating its two quotes"
        );
        assert!(
            again_pos < tail_pos,
            "the deferred row is released as soon as the window frees it, ahead of the tail row"
        );

        assert_eq!(mapped, oracle);
    }
}
