//! The two page caps, TECH-DESIGN section 9.1: one item per original author
//! per UTC day, then one item per quoting DID per 50 items. `apply` runs
//! both over a subset of `items`, given as `indices` — `snapshot::build`
//! calls it with every index (`0..len`); a later per-viewer filter (story
//! 05, 06) calls it again with a filtered subset, so the caps stay correct
//! on whatever slice of the feed a viewer is allowed to see (BC6).
//!
//! Moved here from `snapshot::build`, which used to run these caps
//! inline over `FeedRow`, keyed by DID string. `apply` compares `u64` DID
//! hashes instead (`FeedItem::original_did`, `FeedItem::quote_did`), so a
//! later viewer filter that already carries hashed circles never has to
//! round-trip through a DID string to use these caps.

use std::collections::{HashMap, HashSet, VecDeque};

use super::FeedItem;

/// Cap 1: one item per `(original_did, UTC day of quoted_at)`, keeping the
/// first index seen for each key (BC4). `indices` is assumed already in
/// rank order (`rank DESC, cid ASC`) on entry — `snapshot::build` and a
/// later per-viewer filter both provide it that way — so keeping the first
/// index seen for each key keeps the highest-rank one. The UTC day is
/// `quoted_at.div_euclid(86_400)`, the day number since the Unix epoch in
/// UTC.
fn apply_cap_one_per_author_per_day(items: &[FeedItem], indices: &[u32]) -> Vec<u32> {
    let mut seen: HashSet<(u64, i64)> = HashSet::with_capacity(indices.len());
    let mut kept = Vec::with_capacity(indices.len());
    for &idx in indices {
        let item = &items[idx as usize];
        let day = item.quoted_at.div_euclid(86_400);
        if seen.insert((item.original_did, day)) {
            kept.push(idx);
        }
    }
    kept
}

/// Adds `idx` to `output`, then records its quoter hash in the trailing
/// window, dropping the oldest entry once the window holds more than
/// `window` DIDs.
fn push_kept(
    idx: u32,
    quote_did: u64,
    output: &mut Vec<u32>,
    window_queue: &mut VecDeque<u64>,
    window_counts: &mut HashMap<u64, usize>,
    window: usize,
) {
    window_queue.push_back(quote_did);
    *window_counts.entry(quote_did).or_insert(0) += 1;
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
    output.push(idx);
}

/// Cap 2: one item per quoting DID per 50 items (BC5). The window tracks
/// the last 49 kept quoter hashes (the 50th back no longer blocks). At each
/// output position the deferred queue is scanned first, in rank order, for
/// the first item whose quoter has cleared the window; only when none has
/// does the next item come off the main list. A main-list item whose
/// quoter is still in the window joins the back of the deferred queue
/// instead of being output. Once the main list is exhausted, every item
/// still stuck in the deferred queue is illegal for good — the window
/// cannot shrink without a new output — so the whole remainder is dropped.
///
/// Deferred indices are stored bucketed by `quote_did` in
/// `deferred: HashMap<u64, VecDeque<u32>>`, popped front-first from the
/// bucket that becomes legal, rather than in one flat queue. `defer_order`
/// carries the release order — one `quote_did` entry per deferred index, in
/// the order it was deferred — so the index actually released at each step
/// matches the order the indices were first seen.
fn apply_cap_one_per_quoter_per_50(items: &[FeedItem], indices: Vec<u32>) -> Vec<u32> {
    const WINDOW: usize = 49;
    let mut window_queue: VecDeque<u64> = VecDeque::with_capacity(WINDOW);
    let mut window_counts: HashMap<u64, usize> = HashMap::new();
    let mut deferred: HashMap<u64, VecDeque<u32>> = HashMap::new();
    let mut defer_order: VecDeque<u64> = VecDeque::new();
    let mut output: Vec<u32> = Vec::with_capacity(indices.len());
    let mut main_idx = 0usize;

    let is_legal =
        |did: u64, counts: &HashMap<u64, usize>| counts.get(&did).copied().unwrap_or(0) == 0;

    loop {
        if let Some(release_pos) = defer_order.iter().position(|&did| is_legal(did, &window_counts))
        {
            let did = defer_order.remove(release_pos).expect("position just found in this deque");
            let bucket = deferred.get_mut(&did).expect("a did in defer_order has a bucket");
            let idx = bucket.pop_front().expect("a did in defer_order has at least one index");
            if bucket.is_empty() {
                deferred.remove(&did);
            }
            push_kept(idx, did, &mut output, &mut window_queue, &mut window_counts, WINDOW);
            continue;
        }

        if main_idx < indices.len() {
            let idx = indices[main_idx];
            main_idx += 1;
            let did = items[idx as usize].quote_did;
            if is_legal(did, &window_counts) {
                push_kept(idx, did, &mut output, &mut window_queue, &mut window_counts, WINDOW);
            } else {
                deferred.entry(did).or_default().push_back(idx);
                defer_order.push_back(did);
            }
            continue;
        }

        // Main exhausted and nothing left in `deferred` is legal.
        break;
    }

    output
}

/// Cap 1 then cap 2, run over `indices` into `items` (BC6). `items` supplies
/// the author hash, quoter hash and `quoted_at` each cap needs; `indices`
/// is the subset in scope, already in rank order. Every output value is a
/// value from `indices`, never duplicated, and an index outside `indices`
/// never takes a slot (BC7).
pub fn apply(items: &[FeedItem], indices: &[u32]) -> Vec<u32> {
    let capped_by_author = apply_cap_one_per_author_per_day(items, indices);
    apply_cap_one_per_quoter_per_50(items, capped_by_author)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            ratio: 0.0,
            quote_did,
            original_did,
            quoted_at,
            promoted_at: quoted_at,
        }
    }

    fn sort_by_rank(items: &mut [FeedItem]) {
        items.sort_by(|a, b| {
            super::super::cmp_rank_then_cid((a.rank, &a.quote_cid), (b.rank, &b.quote_cid))
        });
    }

    fn indices(items: &[FeedItem]) -> Vec<u32> {
        (0..items.len() as u32).collect()
    }

    // AC6, BC4: two items sharing (original_did, UTC day) keep only the
    // higher-rank one.
    #[test]
    fn cap_one_per_author_per_day() {
        let day_start = 1_700_000_000i64.div_euclid(86_400) * 86_400;
        let high = item(
            "at://did:plc:q/app.bsky.feed.post/high",
            "cid-high",
            1,
            100,
            day_start + 100,
            5.0,
        );
        let low =
            item("at://did:plc:q/app.bsky.feed.post/low", "cid-low", 2, 100, day_start + 200, 2.0);
        let other_day = item(
            "at://did:plc:q/app.bsky.feed.post/other",
            "cid-other",
            3,
            100,
            day_start + 90_000,
            3.0,
        );
        let mut items = vec![high.clone(), low, other_day.clone()];
        sort_by_rank(&mut items);
        let idx = indices(&items);

        let kept = apply_cap_one_per_author_per_day(&items, &idx);

        let uris: Vec<&str> = kept.iter().map(|&i| items[i as usize].quote_uri.as_str()).collect();
        assert_eq!(uris.len(), 2, "the lower-rank same-day row is dropped");
        assert!(uris.contains(&high.quote_uri.as_str()));
        assert!(uris.contains(&other_day.quote_uri.as_str()));
    }

    // BC4 boundary: a tie on rank breaks on `quote_cid ASC`, the same rule
    // the sort itself uses.
    #[test]
    fn cap_one_per_author_per_day_ties_break_on_quote_cid() {
        let mut a =
            item("at://did:plc:q/app.bsky.feed.post/z", "cid-b", 1, 100, 1_700_000_000, 5.0);
        a.quote_cid = "cid-b".to_string();
        let mut b =
            item("at://did:plc:q/app.bsky.feed.post/y", "cid-a", 2, 100, 1_700_000_000, 5.0);
        b.quote_cid = "cid-a".to_string();
        let mut items = vec![a, b];
        sort_by_rank(&mut items);
        let idx = indices(&items);

        let kept = apply_cap_one_per_author_per_day(&items, &idx);

        assert_eq!(kept.len(), 1);
        assert_eq!(items[kept[0] as usize].quote_cid, "cid-a");
    }

    // AC7, BC5: an item whose quoter is still in the trailing window defers
    // to the first legal position; one still illegal when the main list
    // runs out is dropped. Only 5 filler rows separate the repeat, well
    // inside the 49-item window, so the window never sheds `quoter` and the
    // deferred row is never freed before the main list runs out.
    #[test]
    fn cap_one_per_quoter_per_50() {
        let quoter = 42u64;
        let mut items = vec![item(
            "at://did:plc:q/app.bsky.feed.post/1",
            "cid-1",
            quoter,
            1,
            1_700_000_000,
            10.0,
        )];
        for i in 0..5 {
            items.push(item(
                &format!("at://did:plc:q/app.bsky.feed.post/filler{i}"),
                &format!("cid-filler{i}"),
                100 + i,
                2,
                1_700_000_000,
                9.0 - i as f64 * 0.01,
            ));
        }
        // Shares `quoter` with the very first row, only 5 kept items later:
        // still inside the window, so it defers. Nothing follows to free
        // the window, so it never becomes legal and is dropped.
        items.push(item(
            "at://did:plc:q/app.bsky.feed.post/blocked",
            "cid-blocked",
            quoter,
            3,
            1_700_000_000,
            1.0,
        ));
        let idx = indices(&items);

        let kept = apply_cap_one_per_quoter_per_50(&items, idx);

        assert_eq!(kept.len(), 6, "the still-blocked row is dropped, the other 6 rows are kept");
        assert!(!kept
            .iter()
            .any(|&i| items[i as usize].quote_uri == "at://did:plc:q/app.bsky.feed.post/blocked"));
    }

    // BC5: once enough new quoters have been output to push `quoter` out of
    // the 49-item window, the deferred item is re-inserted at the very next
    // output position, ahead of a lower-rank main-list item that has not
    // been reached yet, because the deferred queue is tried first. `pre` (3
    // rows) then `again` (deferred) then exactly 46 more `post` rows brings
    // the total kept count since `quoter`'s first row to 49, which is the
    // push that evicts it from the window; `again` should then come out
    // ahead of `tail`.
    #[test]
    fn cap_one_per_quoter_per_50_reinserts_once_legal() {
        let quoter = 42u64;
        let mut items = vec![item(
            "at://did:plc:q/app.bsky.feed.post/first",
            "cid-first",
            quoter,
            1,
            1_700_000_000,
            100.0,
        )];
        for i in 0..3 {
            items.push(item(
                &format!("at://did:plc:q/app.bsky.feed.post/pre{i}"),
                &format!("cid-pre{i}"),
                100 + i,
                2,
                1_700_000_000,
                90.0 - i as f64,
            ));
        }
        items.push(item(
            "at://did:plc:q/app.bsky.feed.post/again",
            "cid-again",
            quoter,
            3,
            1_700_000_000,
            50.0,
        ));
        for i in 0..46 {
            items.push(item(
                &format!("at://did:plc:q/app.bsky.feed.post/post{i}"),
                &format!("cid-post{i}"),
                200 + i,
                4,
                1_700_000_000,
                40.0 - i as f64 * 0.1,
            ));
        }
        items.push(item(
            "at://did:plc:q/app.bsky.feed.post/tail",
            "cid-tail",
            999,
            5,
            1_700_000_000,
            -100.0,
        ));
        let idx = indices(&items);

        let kept = apply_cap_one_per_quoter_per_50(&items, idx);

        let uris: Vec<&str> = kept.iter().map(|&i| items[i as usize].quote_uri.as_str()).collect();
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

    // BC5 boundary: exactly 50 items separate two same-quoter items — the
    // window is the last 49 kept quoter hashes, so the 50th-back item no
    // longer blocks.
    #[test]
    fn cap_one_per_quoter_per_50_boundary_at_exactly_50() {
        let quoter = 42u64;
        let mut items = vec![item(
            "at://did:plc:q/app.bsky.feed.post/1",
            "cid-1",
            quoter,
            1,
            1_700_000_000,
            10.0,
        )];
        for i in 0..49 {
            items.push(item(
                &format!("at://did:plc:q/app.bsky.feed.post/filler{i}"),
                &format!("cid-filler{i}"),
                100 + i,
                2,
                1_700_000_000,
                9.0 - i as f64 * 0.01,
            ));
        }
        // The 50th kept item (index 50 overall, 49 items separate the two
        // same-quoter rows): outside the 49-item trailing window, so it is
        // never deferred.
        items.push(item(
            "at://did:plc:q/app.bsky.feed.post/50th",
            "cid-50th",
            quoter,
            3,
            1_700_000_000,
            1.0,
        ));
        let idx = indices(&items);

        let kept = apply_cap_one_per_quoter_per_50(&items, idx);

        assert_eq!(kept.len(), 51);
        assert_eq!(
            items[*kept.last().unwrap() as usize].quote_uri,
            "at://did:plc:q/app.bsky.feed.post/50th"
        );
    }

    // BC5: two rows deferred for one DID release in the same order they
    // were deferred (rank order), proving the per-DID bucket's `VecDeque`
    // pops front-first rather than, say, last-in-first-out. Releasing
    // `first` re-adds `quoter` to the window (`push_kept` always does), so
    // `second` needs the window to clear a second time before it can
    // release: 49 filler pushes evict `head`'s window entry, freeing
    // `first`; then 49 more evict the window entry `first`'s own release
    // just added, freeing `second`.
    #[test]
    fn cap_one_per_quoter_per_50_releases_many_deferred_rows_for_one_did_in_order() {
        let quoter = 42u64;
        let mut items = vec![item(
            "at://did:plc:q/app.bsky.feed.post/head",
            "cid-head",
            quoter,
            1,
            1_700_000_000,
            200.0,
        )];
        for (name, rank) in [("first", 100.0), ("second", 99.0)] {
            items.push(item(
                &format!("at://did:plc:q/app.bsky.feed.post/{name}"),
                &format!("cid-{name}"),
                quoter,
                2,
                1_700_000_000,
                rank,
            ));
        }
        for i in 0..98 {
            items.push(item(
                &format!("at://did:plc:q/app.bsky.feed.post/filler{i}"),
                &format!("cid-filler{i}"),
                100 + i,
                3,
                1_700_000_000,
                6.0 - i as f64 * 0.01,
            ));
        }
        let idx = indices(&items);

        let kept = apply_cap_one_per_quoter_per_50(&items, idx);

        let uris: Vec<&str> = kept.iter().map(|&i| items[i as usize].quote_uri.as_str()).collect();
        let first_idx =
            uris.iter().position(|u| *u == "at://did:plc:q/app.bsky.feed.post/first").unwrap();
        let second_idx =
            uris.iter().position(|u| *u == "at://did:plc:q/app.bsky.feed.post/second").unwrap();
        assert!(first_idx < second_idx, "released in the order they were deferred");
    }

    // AC2, BC6: `apply` on a subset ignores items outside the subset. `a`
    // and `b` share `(original_did, day)`, so cap 1 would drop `b` if `a`
    // were in scope; excluding `a` from `indices` frees `b`'s slot.
    #[test]
    fn subset_frees_slots() {
        let a = item("at://did:plc:q/app.bsky.feed.post/a", "cid-a", 1, 100, 1_700_000_000, 5.0);
        let b = item("at://did:plc:q/app.bsky.feed.post/b", "cid-b", 2, 100, 1_700_000_000, 2.0);
        let items = vec![a, b];

        let full = apply(&items, &[0, 1]);
        assert_eq!(full, vec![0], "with both in scope, cap 1 keeps only the higher-rank `a`");

        let subset = apply(&items, &[1]);
        assert_eq!(subset, vec![1], "with `a` outside the subset, it never takes `b`'s slot");
    }

    // BC7: every output value is a value from `indices`, none is
    // duplicated, and an empty input gives an empty output.
    #[test]
    fn output_values_come_from_input_no_duplicates() {
        let items = vec![
            item("at://did:plc:q/app.bsky.feed.post/a", "cid-a", 1, 10, 1_700_000_000, 5.0),
            item("at://did:plc:q/app.bsky.feed.post/b", "cid-b", 2, 20, 1_700_000_100, 4.0),
            item("at://did:plc:q/app.bsky.feed.post/c", "cid-c", 3, 30, 1_700_000_200, 3.0),
        ];

        let out = apply(&items, &[0, 1, 2]);
        let mut seen = HashSet::new();
        for &idx in &out {
            assert!([0, 1, 2].contains(&idx), "output value {idx} is not from the input indices");
            assert!(seen.insert(idx), "output value {idx} is duplicated");
        }

        let empty: Vec<u32> = apply(&items, &[]);
        assert!(empty.is_empty());
    }
}
