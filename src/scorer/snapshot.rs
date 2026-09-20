//! Feed ordering and the atomic snapshot swap, TECH-DESIGN section 7.3.
//! `build` recomputes every `feed` row's `rank` for the current age, then
//! applies the two page caps in order: one item per original author per UTC
//! day, then one item per quoting DID per 50 items. `SnapshotHandle` is the
//! shared `Arc<RwLock<Arc<Vec<FeedItem>>>>` story 08's HTTP server (not
//! wired up yet, per this slice's `## Non-goals`) will read through
//! `current()`; the scorer's own snapshot step (`src/scorer/mod.rs`) builds
//! the new list outside the lock and calls `swap` only for the atomic
//! pointer replacement (BC33).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};

use crate::score::{self, Counts, Weights};
use crate::store::feed::FeedRow;

/// One item of the ranked, capped feed. Pagination (story 08) reads this
/// list only, never SQLite directly.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedItem {
    pub quote_uri: String,
    pub quote_cid: String,
    pub rank: f64,
    /// `D`, the dunk ratio `recompute_ranks` computed for this row on the
    /// same pass, not `FeedRow.ratio` (the promotion-time record, left
    /// untouched). `getFeedSkeleton`'s `feedContext` (story 08, BC24) reads
    /// this field.
    pub ratio: f64,
}

/// The shared snapshot pointer. Cloning `SnapshotHandle` clones the `Arc`
/// around the lock, not the list: every clone reads and writes the same
/// underlying snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotHandle {
    inner: Arc<RwLock<Arc<Vec<FeedItem>>>>,
}

impl SnapshotHandle {
    /// A fresh handle whose `current()` is an empty list, never a panic and
    /// never `None`, before the first pass ever runs (BC41).
    pub fn new() -> Self {
        SnapshotHandle { inner: Arc::new(RwLock::new(Arc::new(Vec::new()))) }
    }

    /// The current snapshot. Cloning the returned `Arc` is cheap and never
    /// blocks a concurrent `swap`. Round 2 finding 9: narrowed from a
    /// module-wide `#![allow(dead_code)]` in `src/scorer/mod.rs`. No
    /// non-test caller yet; story 08's HTTP server is the first.
    #[allow(dead_code)]
    pub fn current(&self) -> Arc<Vec<FeedItem>> {
        self.inner.read().expect("snapshot lock poisoned").clone()
    }

    /// Replaces the snapshot with `new` under the write lock. `new` must
    /// already be fully built: the caller (the scorer's snapshot step) does
    /// every bit of ranking and capping work outside this call, so the lock
    /// is held only for the pointer swap and a reader never observes a
    /// partially built list (BC33).
    pub fn swap(&self, new: Arc<Vec<FeedItem>>) {
        let mut guard = self.inner.write().expect("snapshot lock poisoned");
        *guard = new;
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

/// Cap 1: one item per `(original_did, UTC day of quoted_at)`, keeping the
/// highest rank (BC28). `rows` is already sorted `rank DESC, quote_cid ASC`
/// on entry, so keeping the first row seen for each key keeps the
/// highest-rank one and breaks a rank tie on `quote_cid ASC`, matching the
/// sort. The UTC day is `quoted_at.div_euclid(86_400)`, the day number
/// since the Unix epoch in UTC; integer division by the day length is exact
/// for this grouping and needs no calendar library.
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
    output.push(FeedItem {
        quote_uri: row.quote_uri,
        quote_cid: row.quote_cid,
        rank: row.rank,
        ratio,
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

/// The full snapshot step, TECH-DESIGN section 7.3: recompute rank, sort,
/// cap 1, cap 2. Pure and synchronous, so the caller can build it outside
/// the `SnapshotHandle`'s lock (BC33). `k` reaches here from `Config`
/// (round 2 finding 7, BC50), never a literal.
pub fn build(mut rows: Vec<FeedRow>, weights: &Weights, now: i64, k: f64) -> Vec<FeedItem> {
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
        assert_eq!(*handle.current(), Vec::<FeedItem>::new());
    }

    #[test]
    fn swap_replaces_the_snapshot() {
        let handle = SnapshotHandle::new();
        let items = vec![FeedItem {
            quote_uri: "at://did:plc:q/app.bsky.feed.post/q".to_string(),
            quote_cid: "cid-q".to_string(),
            rank: 1.0,
            ratio: 4.0,
        }];
        handle.swap(Arc::new(items.clone()));
        assert_eq!(*handle.current(), items);
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

        let items = build(vec![r], &weights(), now, 5.0);

        // D = eq / (eo + k) = 100 / (20 + 5) = 4.0.
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].ratio, 4.0);
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

        let items = build(rows, &weights(), now, 5.0);

        assert_eq!(items.len(), 1, "cap 1 drops the second same-author-same-day row");
    }
}
