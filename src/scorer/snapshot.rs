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
    /// blocks a concurrent `swap`.
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

/// Recomputes `rank` for every row from its stored `ratio` (`D`, unchanged
/// since the row's last verify) and its verified `Q` engagement, at the
/// current age. A `quoted_at` in the future clamps `age_hours` at `0.0`
/// rather than handing `score::rank` a negative age (BC27).
fn recompute_ranks(rows: &mut [FeedRow], weights: &Weights, now: i64) {
    for row in rows.iter_mut() {
        let counts_q = Counts {
            likes: row.v_likes_q.max(0) as u32,
            reposts: row.v_reposts_q.max(0) as u32,
            replies: row.v_replies_q.max(0) as u32,
        };
        let eq = score::engagement(&counts_q, weights);
        let age_hours = ((now - row.quoted_at) as f64 / 3600.0).max(0.0);
        row.rank = score::rank(row.ratio, eq, age_hours);
    }
}

/// `rank DESC, quote_cid ASC` (BC26), the same tie rule `Store::feed_rows`
/// starts from, re-applied after `recompute_ranks` changes `rank`.
fn sort_by_rank(rows: &mut [FeedRow]) {
    rows.sort_by(|a, b| {
        b.rank
            .partial_cmp(&a.rank)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.quote_cid.cmp(&b.quote_cid))
    });
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
    output.push(FeedItem { quote_uri: row.quote_uri, quote_cid: row.quote_cid, rank: row.rank });
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
fn apply_cap_one_per_quoter_per_50(rows: Vec<FeedRow>) -> Vec<FeedItem> {
    const WINDOW: usize = 49;
    let mut window_queue: VecDeque<String> = VecDeque::with_capacity(WINDOW);
    let mut window_counts: HashMap<String, usize> = HashMap::new();
    let mut deferred: VecDeque<FeedRow> = VecDeque::new();
    let mut output: Vec<FeedItem> = Vec::with_capacity(rows.len());
    let mut main_idx = 0usize;

    let is_legal =
        |did: &str, counts: &HashMap<String, usize>| counts.get(did).copied().unwrap_or(0) == 0;

    loop {
        if let Some(pos) = deferred.iter().position(|row| is_legal(&row.quote_did, &window_counts))
        {
            let row = deferred.remove(pos).expect("position just found in this deque");
            push_kept(row, &mut output, &mut window_queue, &mut window_counts, WINDOW);
            continue;
        }

        if main_idx < rows.len() {
            let row = rows[main_idx].clone();
            main_idx += 1;
            if is_legal(&row.quote_did, &window_counts) {
                push_kept(row, &mut output, &mut window_queue, &mut window_counts, WINDOW);
            } else {
                deferred.push_back(row);
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
/// the `SnapshotHandle`'s lock (BC33).
pub fn build(mut rows: Vec<FeedRow>, weights: &Weights, now: i64) -> Vec<FeedItem> {
    recompute_ranks(&mut rows, weights, now);
    sort_by_rank(&mut rows);
    let capped_by_author = apply_cap_one_per_author_per_day(rows);
    apply_cap_one_per_quoter_per_50(capped_by_author)
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
        recompute_ranks(&mut rows, &weights(), now);
        assert!(rows[0].rank.is_finite());
        assert!(rows[0].rank > 0.0);
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

        let kept = apply_cap_one_per_quoter_per_50(rows);

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

        let kept = apply_cap_one_per_quoter_per_50(rows);

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

        let kept = apply_cap_one_per_quoter_per_50(rows);

        assert_eq!(kept.len(), 51);
        assert_eq!(kept.last().unwrap().quote_uri, "at://did:plc:q/app.bsky.feed.post/50th");
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

        let items = build(rows, &weights(), now);

        assert_eq!(items.len(), 1, "cap 1 drops the second same-author-same-day row");
    }
}
