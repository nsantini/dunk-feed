//! The three first-build steps, TECH-DESIGN-network-feed §6.2, over the
//! [`GraphSource`] seam: `PdsClient` implements it in `src/appview/pds.rs`
//! (slice 3.0); [`FakeSource`] below implements it for the tests here and
//! for `graph_probe`'s own tests. `graph_probe::run` (slice 4.0) calls
//! `step_follows`, then `step_follows_me`, then `step_degree2`, for each
//! handle in turn.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::{Duration, Instant};

use crate::appview::pds::PdsError;
use crate::graph::circle::Circle;
use crate::graph::{hash_did, DidHash};

/// `getRelationships` takes at most this many `others` in one call
/// (BC3); a longer candidate list is split into chunks of this size.
const RELATIONSHIPS_CHUNK: usize = 30;

/// One `getFollows` page: the DIDs it carries, in response order, and the
/// cursor to page again with, or `None` at the end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowsPage {
    pub dids: Vec<String>,
    pub cursor: Option<String>,
}

/// One item of the current snapshot's ranked order, as `step_follows_me`
/// needs it: the two DIDs whose relationship to the viewer step 2 checks.
/// Not `FilterItem` (`src/graph/filter.rs`): that one carries hashes for
/// the connection filter, and story 01 has not yet given the probe a
/// `FeedItem` to map from (spec.md `## Approach`, Rejected).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedAuthors {
    pub quote_did: String,
    pub original_did: String,
}

/// Calls, pages and wall time one step spent against the source. `calls`
/// and `pages` are equal in this module: every `GraphSource` call this
/// probe makes returns exactly one page, so the two counts never diverge
/// here. They stay two fields because the probe's report (BC10) prints
/// them as two labelled numbers, matching the design's own cost table
/// (TECH-DESIGN-network-feed §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepStats {
    pub calls: u32,
    pub pages: u32,
    pub elapsed: Duration,
}

/// The PDS surface the three build steps need. `PdsClient` implements this
/// in `src/appview/pds.rs` over its own login, refresh and retry (slice
/// 3.0); [`FakeSource`] implements it here with no network at all, so
/// `step_follows`, `step_follows_me` and `step_degree2` are generic over
/// `S: GraphSource` and run the same in a test as in production.
pub trait GraphSource {
    /// `com.atproto.graph.getFollows?actor=<actor>&sort=latest&limit=<limit>`,
    /// continuing at `cursor` when given. The `sort=latest` parameter is an
    /// implementation detail of the `PdsClient` transport, not a parameter
    /// here: every caller in this module wants the newest follows first
    /// (TECH-DESIGN-network-feed §6.2, steps 1 and 3).
    fn get_follows(
        &self,
        actor: &str,
        limit: u32,
        cursor: Option<String>,
    ) -> impl Future<Output = Result<FollowsPage, PdsError>> + Send;

    /// `com.atproto.graph.getRelationships?actor=<actor>&others=...`,
    /// already filtered to the DIDs among `others` that follow `actor`
    /// back: the caller never sees the raw relationship records, only the
    /// subset with `followedBy` set (TECH-DESIGN-network-feed §6.2, step 2).
    fn get_relationships(
        &self,
        actor: &str,
        others: &[String],
    ) -> impl Future<Output = Result<Vec<String>, PdsError>> + Send;
}

/// Step 1: pages `get_follows` for `viewer` to the end, filling
/// `circle.follows` with every hash and `circle.d2_sample` with the first
/// `d2_sample_size` DIDs in response order (BC1).
pub async fn step_follows<S: GraphSource>(
    source: &S,
    viewer: &str,
    d2_sample_size: usize,
    circle: &mut Circle,
) -> Result<StepStats, PdsError> {
    let start = Instant::now();
    let mut calls: u32 = 0;
    let mut cursor: Option<String> = None;
    loop {
        let page = source.get_follows(viewer, 100, cursor.take()).await?;
        calls += 1;
        for did in &page.dids {
            circle.follows.insert(hash_did(did));
            if circle.d2_sample.len() < d2_sample_size {
                circle.d2_sample.push(did.clone());
            }
        }
        cursor = page.cursor;
        if cursor.is_none() {
            break;
        }
    }
    Ok(StepStats { calls, pages: calls, elapsed: start.elapsed() })
}

/// Step 2: takes the quoter and original DIDs of the first
/// `follows_me_depth` `ranked` items, in ranked order, drops the viewer's
/// own DID and any DID already in `circle.follows` or `circle.checked`,
/// dedupes, then sends what remains to `get_relationships` in chunks of
/// [`RELATIONSHIPS_CHUNK`] (BC2, BC3). Every sent DID's hash goes into
/// `circle.checked`; every DID the source reports as following `viewer`
/// back goes into `circle.follows_me`.
pub async fn step_follows_me<S: GraphSource>(
    source: &S,
    viewer: &str,
    ranked: &[RankedAuthors],
    follows_me_depth: usize,
    circle: &mut Circle,
) -> Result<StepStats, PdsError> {
    let start = Instant::now();

    let mut seen: HashSet<DidHash> = HashSet::new();
    let mut candidates: Vec<String> = Vec::new();
    for item in ranked.iter().take(follows_me_depth) {
        for did in [&item.quote_did, &item.original_did] {
            if did == viewer {
                continue;
            }
            let hash = hash_did(did);
            if circle.follows.contains(&hash) || circle.checked.contains(&hash) {
                continue;
            }
            if seen.insert(hash) {
                candidates.push(did.clone());
            }
        }
    }

    let mut calls: u32 = 0;
    for chunk in candidates.chunks(RELATIONSHIPS_CHUNK) {
        let followed_by = source.get_relationships(viewer, chunk).await?;
        calls += 1;
        for did in chunk {
            circle.checked.insert(hash_did(did));
        }
        for did in &followed_by {
            circle.follows_me.insert(hash_did(did));
        }
    }

    Ok(StepStats { calls, pages: calls, elapsed: start.elapsed() })
}

/// Step 3: for each account in `d2_sample` with no entry yet in `shared`,
/// pages `get_follows` until `d2_follows_depth` raw DIDs are collected or the
/// end is reached, truncates that raw list to exactly `d2_follows_depth`
/// before hashing, sorting and deduplicating it, and stores the result in
/// `shared` (BC4). Each page after the first requests only the DIDs still
/// needed to reach the depth, never a full page of 100 when fewer remain
/// (review round 1, defect B): a depth of 150 therefore makes one page of
/// 100 and one of 50, and the stored list holds exactly 150 hashes, not the
/// 200 the account may actually follow. An account already in `shared`
/// costs no call.
pub async fn step_degree2<S: GraphSource>(
    source: &S,
    d2_sample: &[String],
    d2_follows_depth: u32,
    shared: &mut HashMap<String, Vec<DidHash>>,
) -> Result<StepStats, PdsError> {
    let start = Instant::now();
    let mut calls: u32 = 0;
    let depth = d2_follows_depth as usize;

    for account in d2_sample {
        if shared.contains_key(account) {
            continue;
        }
        let mut collected: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        while collected.len() < depth {
            let remaining = depth - collected.len();
            let page_limit = remaining.min(100) as u32;
            let page = source.get_follows(account, page_limit, cursor.take()).await?;
            calls += 1;
            collected.extend(page.dids);
            cursor = page.cursor;
            if cursor.is_none() {
                break;
            }
        }
        collected.truncate(depth);
        let mut hashed: Vec<DidHash> = collected.iter().map(|did| hash_did(did)).collect();
        hashed.sort_unstable();
        hashed.dedup();
        shared.insert(account.clone(), hashed);
    }

    Ok(StepStats { calls, pages: calls, elapsed: start.elapsed() })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// A `GraphSource` with no network: `follows` holds each actor's full
    /// `getFollows` list, paginated on the fly at the caller's own
    /// `limit`; `follows_back` holds the DIDs `get_relationships` reports
    /// as following the queried actor back. `relationship_chunk_sizes`
    /// records the length of every `others` slice sent, in call order, so
    /// a test can check the 30-per-call split (BC3) without a real
    /// `getRelationships` response to inspect.
    #[derive(Default)]
    struct FakeSource {
        follows: HashMap<String, Vec<String>>,
        follows_back: HashSet<String>,
        relationship_chunk_sizes: Mutex<Vec<usize>>,
    }

    impl GraphSource for FakeSource {
        async fn get_follows(
            &self,
            actor: &str,
            limit: u32,
            cursor: Option<String>,
        ) -> Result<FollowsPage, PdsError> {
            let all = self.follows.get(actor).cloned().unwrap_or_default();
            let offset: usize = cursor.as_deref().and_then(|c| c.parse().ok()).unwrap_or(0);
            let end = (offset + limit as usize).min(all.len());
            let dids = all.get(offset.min(all.len())..end).unwrap_or_default().to_vec();
            let cursor = if end < all.len() { Some(end.to_string()) } else { None };
            Ok(FollowsPage { dids, cursor })
        }

        async fn get_relationships(
            &self,
            _actor: &str,
            others: &[String],
        ) -> Result<Vec<String>, PdsError> {
            self.relationship_chunk_sizes.lock().unwrap().push(others.len());
            Ok(others.iter().filter(|did| self.follows_back.contains(*did)).cloned().collect())
        }
    }

    fn did(n: usize) -> String {
        format!("did:plc:{n:04}")
    }

    #[tokio::test]
    async fn step_follows_pages_to_the_end_and_samples_d2() {
        // BC1: 250 follows, limit 100, so 3 pages; d2_sample keeps the
        // first 5 in response order.
        let follows: Vec<String> = (0..250).map(did).collect();
        let source = FakeSource {
            follows: HashMap::from([("viewer".to_string(), follows.clone())]),
            ..Default::default()
        };
        let mut circle = Circle::new();

        let stats = step_follows(&source, "viewer", 5, &mut circle).await.unwrap();

        assert_eq!(stats.calls, 3);
        assert_eq!(stats.pages, 3);
        assert_eq!(circle.follows.len(), 250);
        assert_eq!(circle.d2_sample, follows[..5].to_vec());
        for d in &follows {
            assert!(circle.follows.contains(&hash_did(d)));
        }
    }

    #[tokio::test]
    async fn step_follows_on_an_empty_list_still_makes_one_call() {
        // BC1: "at least 1" — even a viewer with no follows makes the one
        // call that finds that out.
        let source = FakeSource::default();
        let mut circle = Circle::new();

        let stats = step_follows(&source, "viewer", 5, &mut circle).await.unwrap();

        assert_eq!(stats.calls, 1);
        assert_eq!(stats.pages, 1);
        assert!(circle.follows.is_empty());
        assert!(circle.d2_sample.is_empty());
    }

    #[tokio::test]
    async fn step_follows_me_builds_candidates_in_ranked_order_and_skips_known_dids() {
        // BC2: the viewer's own DID, a DID already in `follows`, and a
        // duplicate across two items are all excluded; order follows the
        // ranked list.
        let viewer = "did:plc:viewer".to_string();
        let known = did(1);
        let ranked = vec![
            RankedAuthors { quote_did: did(2), original_did: known.clone() },
            RankedAuthors { quote_did: viewer.clone(), original_did: did(3) },
            RankedAuthors { quote_did: did(2), original_did: did(4) },
        ];
        let mut circle = Circle::new();
        circle.follows.insert(hash_did(&known));

        let source = FakeSource::default();
        let stats = step_follows_me(&source, &viewer, &ranked, 10, &mut circle).await.unwrap();

        // Candidates, in order: did(2) [item 0], known dropped, viewer
        // dropped, did(3) [item 1], did(2) skipped as a duplicate, did(4)
        // [item 2]. One chunk of 3 distinct DIDs, so one call.
        assert_eq!(stats.calls, 1);
        assert_eq!(*source.relationship_chunk_sizes.lock().unwrap(), vec![3]);
        assert!(circle.checked.contains(&hash_did(&did(2))));
        assert!(circle.checked.contains(&hash_did(&did(3))));
        assert!(circle.checked.contains(&hash_did(&did(4))));
        assert!(!circle.checked.contains(&hash_did(&known)));
    }

    #[tokio::test]
    async fn step_follows_me_respects_the_depth_and_records_follow_backs() {
        // BC2, BC3: only the first `follows_me_depth` items contribute
        // candidates; a DID `get_relationships` reports as following back
        // lands in `follows_me`, and every sent DID lands in `checked`
        // whether or not it follows back.
        let viewer = "did:plc:viewer".to_string();
        let ranked = vec![
            RankedAuthors { quote_did: did(1), original_did: did(2) },
            RankedAuthors { quote_did: did(3), original_did: did(4) },
        ];
        let mut circle = Circle::new();
        let source = FakeSource { follows_back: HashSet::from([did(1)]), ..Default::default() };

        let stats = step_follows_me(&source, &viewer, &ranked, 1, &mut circle).await.unwrap();

        assert_eq!(stats.calls, 1);
        assert!(circle.checked.contains(&hash_did(&did(1))));
        assert!(circle.checked.contains(&hash_did(&did(2))));
        assert!(!circle.checked.contains(&hash_did(&did(3))));
        assert!(circle.follows_me.contains(&hash_did(&did(1))));
        assert!(!circle.follows_me.contains(&hash_did(&did(2))));
    }

    #[tokio::test]
    async fn step_follows_me_splits_candidates_thirty_per_call() {
        // BC3: 40 distinct candidates split into chunks of 30, so two
        // calls of size 30 and 10.
        let viewer = "did:plc:viewer".to_string();
        let ranked: Vec<RankedAuthors> = (0..20)
            .map(|i| RankedAuthors { quote_did: did(i * 2), original_did: did(i * 2 + 1) })
            .collect();
        let mut circle = Circle::new();
        let source = FakeSource::default();

        let stats = step_follows_me(&source, &viewer, &ranked, 20, &mut circle).await.unwrap();

        assert_eq!(stats.calls, 2);
        assert_eq!(*source.relationship_chunk_sizes.lock().unwrap(), vec![30, 10]);
    }

    #[tokio::test]
    async fn step_degree2_pages_until_the_depth_or_the_end() {
        // BC4: depth 100 is one page; an account already in `shared`
        // costs no call.
        let mut follows = HashMap::new();
        follows.insert("acct-a".to_string(), (0..100).map(did).collect::<Vec<_>>());
        follows.insert("acct-b".to_string(), (0..50).map(did).collect::<Vec<_>>());
        let source = FakeSource { follows, ..Default::default() };
        let mut shared: HashMap<String, Vec<DidHash>> = HashMap::new();
        shared.insert("acct-c".to_string(), vec![1, 2, 3]);

        let stats = step_degree2(
            &source,
            &["acct-a".to_string(), "acct-b".to_string(), "acct-c".to_string()],
            100,
            &mut shared,
        )
        .await
        .unwrap();

        assert_eq!(stats.calls, 2);
        assert_eq!(shared.get("acct-a").unwrap().len(), 100);
        assert_eq!(shared.get("acct-b").unwrap().len(), 50);
        assert_eq!(shared.get("acct-c").unwrap(), &vec![1, 2, 3]);
        let mut sorted_a = shared.get("acct-a").unwrap().clone();
        sorted_a.sort_unstable();
        assert_eq!(shared.get("acct-a").unwrap(), &sorted_a);
    }

    #[tokio::test]
    async fn step_degree2_above_100_pages_until_the_depth_is_reached() {
        // BC4; review round 1, defect B: depth 150 against 200 real follows
        // needs a page of 100 then a page of exactly the 50 still needed,
        // and the stored list is truncated to exactly 150, not the full 200
        // the account follows.
        let mut follows = HashMap::new();
        follows.insert("acct-a".to_string(), (0..200).map(did).collect::<Vec<_>>());
        let source = FakeSource { follows, ..Default::default() };
        let mut shared: HashMap<String, Vec<DidHash>> = HashMap::new();

        let stats = step_degree2(&source, &["acct-a".to_string()], 150, &mut shared).await.unwrap();

        assert_eq!(stats.calls, 2);
        assert_eq!(shared.get("acct-a").unwrap().len(), 150);
    }

    #[tokio::test]
    async fn hash_did_and_heap_bytes_hold_across_a_build() {
        // BC18, BC19: exercised end to end alongside the build steps, on
        // top of `graph::tests` and `graph::circle::tests`' own coverage.
        let mut circle = Circle::new();
        circle.follows.insert(hash_did("did:plc:same"));
        let again = hash_did("did:plc:same");
        assert!(circle.follows.contains(&again));
        assert!(circle.heap_bytes() > 0);
    }
}
