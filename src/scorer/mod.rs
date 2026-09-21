//! The scorer task, TECH-DESIGN section 7. One pass per tick of a
//! `tokio::time::interval` selects dirty candidate pairs, prefilters them
//! on local counts, verifies the survivors against the App View, promotes
//! or drops each on verified counts, re-verifies young promoted pairs on
//! its own timer, expires stale rows, and swaps a freshly ranked and
//! capped snapshot into a shared `Arc<RwLock<Arc<Vec<FeedItem>>>>`. Slice
//! 2.0 added `select`, `verify_all` (as `verify_step`), the guard call,
//! `promote_or_drop`, `reverify`, `expire_step`, `one_pass` and the task
//! loop `run`. Slice 3.0 added the snapshot step inside `one_pass`,
//! `last_scorer_pass`, and `ingest::run`'s wiring (`src/ingest/mod.rs`).
//! Round 2 finding 10 folds the first-verify and re-verify branches of
//! `one_pass` into one `verify_and_apply`, shared by both phases.

// Round 2 finding 9, finished in slice 6.0: the module-level
// `#![allow(dead_code)]` is gone. Story 10 (slice 3.0) gave
// `GuardResult::Drop` and `GuardResult::Defer` (`guards.rs`) their first
// caller, through `guards::check_batch`, so neither needs a local
// `#[allow(dead_code)]` any more. `SnapshotHandle::current` (story 08) and
// every other item that still needs one carries its own local
// `#[allow(dead_code)]` too.

pub mod guards;
pub mod snapshot;
pub mod verify;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, MissedTickBehavior};

use crate::appview::{AppViewClient, PostsOutcome, ProfilesOutcome};
use crate::config::Config;
use crate::health::HealthState;
use crate::score::{self, Counts, Thresholds, Weights};
use crate::store::pairs::{PairOutcome, PairWithCounts};
use crate::store::{feed, unix_now, Store, StoreError};
use guards::GuardResult;
use snapshot::SnapshotHandle;
use verify::Verdict;

/// Every way the scorer task can fail. `src/ingest/mod.rs` (slice 3.0)
/// catches this and wraps it as `IngestError::Scorer` (BC35); no other
/// module constructs it.
#[derive(Debug, Error)]
pub enum ScorerError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("a scorer blocking task panicked: {0}")]
    Task(#[from] tokio::task::JoinError),
}

/// Runs `f`, a synchronous `Store` call, on a blocking thread (the
/// `## Approach` section's rule: `rusqlite` is synchronous, so every `Store`
/// call runs inside `tokio::task::spawn_blocking`, and no `Store` call is
/// held across an `.await`). A panic inside `f` becomes `ScorerError::Task`
/// rather than propagating as a panic on the scorer's own task.
async fn blocking<T, F>(f: F) -> Result<T, ScorerError>
where
    F: FnOnce() -> Result<T, StoreError> + Send + 'static,
    T: Send + 'static,
{
    Ok(tokio::task::spawn_blocking(f).await??)
}

/// One pass's counters, TECH-DESIGN section 7.2's closing paragraph and
/// BC37. `dropped` is keyed by `DropReason::as_str()` rather than
/// `DropReason` itself, so this type needs no `Hash` impl on `DropReason`.
/// `snapshot_len` is written by `snapshot_step`, the pass's step 7.
/// `profile_calls`, `deferred`, `guard_would_drop` and `guard_histogram` are
/// story 10's, folded in by `guards::check_batch` once per verify phase and
/// read by `one_pass`'s histogram-period line (BC29). `histogram_o_dids_seen`
/// is story 10's correction round (BC31a): the `O` author DIDs already
/// sampled into `guard_histogram` this pass, private to `check_batch`, so a
/// prolific author's DID on many pairs is counted once, not once per pair.
/// It resets every pass because `one_pass` builds a fresh `PassCounters`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PassCounters {
    pub selected: usize,
    pub appview_calls: usize,
    pub promoted: usize,
    pub demoted: usize,
    pub dropped: HashMap<&'static str, usize>,
    pub expired: usize,
    pub snapshot_len: usize,
    pub duration_ms: u64,
    pub profile_calls: usize,
    pub deferred: usize,
    pub guard_would_drop: usize,
    pub guard_histogram: guards::FollowerHistogram,
    histogram_o_dids_seen: HashSet<String>,
}

/// The App View surface the scorer needs: one lenient, chunked `getPosts`
/// call. A trait rather than a base URL plus a test HTTP server, per
/// `## Approach`: the crate has no `axum` or `hyper` dependency, and adding
/// one to serve four fixtures is not worth the build cost. `AppViewClient`
/// implements it by forwarding to its own `get_posts_lenient` (round 2
/// finding 4), which does the chunking and the per-chunk failure bookkeeping
/// that used to live in `verify_step` here; tests implement it with an
/// in-memory fake.
pub trait PostSource {
    fn get_posts_lenient(&self, uris: &[String]) -> impl Future<Output = PostsOutcome> + Send;
}

impl PostSource for AppViewClient {
    fn get_posts_lenient(&self, uris: &[String]) -> impl Future<Output = PostsOutcome> + Send {
        AppViewClient::get_posts_lenient(self, uris)
    }
}

/// The App View surface story 10's guard needs: one lenient, chunked
/// `getProfiles` call, the `ProfileSource` counterpart of [`PostSource`] and
/// for the same reason (`## Approach`: no test HTTP server in this crate).
/// `AppViewClient` implements it by forwarding to its own
/// `get_profiles_lenient`; `guards::check_batch` is its caller, reached
/// through `verify_and_apply`'s `S: PostSource + ProfileSource` bound.
pub trait ProfileSource {
    fn get_profiles_lenient(&self, dids: &[String])
        -> impl Future<Output = ProfilesOutcome> + Send;
}

impl ProfileSource for AppViewClient {
    fn get_profiles_lenient(
        &self,
        dids: &[String],
    ) -> impl Future<Output = ProfilesOutcome> + Send {
        AppViewClient::get_profiles_lenient(self, dids)
    }
}

/// Step 1, TECH-DESIGN section 7.2: `dirty_candidates` within
/// `candidate_ttl_h`, local `E` and `D` computed for both sides, kept when
/// `max(E_local) >= P * prefilter_fraction` and `D_local >= M *
/// prefilter_fraction` (BC1, BC2, BC3). Round 2 finding 2: an excluded
/// pair's `dirty` flag is cleared right here, through
/// `clear_dirty_if_unchanged` (BC43, BC44), rather than an eager clear that
/// `verify_step` used to have to undo with `mark_dirty` on a failed chunk
/// (BC36's old shape). A kept pair's dirty flag is left untouched until
/// `verify_and_apply` knows whether its `getPosts` chunk actually succeeded.
/// An empty result (BC4) makes no App View call and clears no dirty flag,
/// since there was nothing to read.
async fn select_step(
    store: &Store,
    now: i64,
    cfg: &Config,
    weights: &Weights,
    thresholds: &Thresholds,
) -> Result<Vec<PairWithCounts>, ScorerError> {
    let candidate_ttl_h = i64::from(cfg.candidate_ttl_h);
    let store_read = store.clone();
    let candidates = blocking(move || store_read.dirty_candidates(now, candidate_ttl_h)).await?;

    let prefilter_p = thresholds.p * cfg.prefilter_fraction;
    let prefilter_m = thresholds.m * cfg.prefilter_fraction;

    let mut kept = Vec::with_capacity(candidates.len());
    let mut excluded_rows: Vec<(String, Counts)> = Vec::new();
    for pair in candidates {
        let eq = score::engagement(&pair.counts_q, weights);
        let eo = score::engagement(&pair.counts_o, weights);
        let d = score::ratio(eq, eo, thresholds.k);
        // BC3: both comparisons are `>=`, not `>`.
        if eq.max(eo) >= prefilter_p && d >= prefilter_m {
            kept.push(pair);
        } else {
            excluded_rows.push((pair.quote_uri.clone(), pair.counts_q));
            excluded_rows.push((pair.original_uri.clone(), pair.counts_o));
        }
    }

    if !excluded_rows.is_empty() {
        let store_clear = store.clone();
        blocking(move || store_clear.clear_dirty_if_unchanged(&excluded_rows)).await?;
    }

    Ok(kept)
}

/// Distinguishes the first verify (from `select`) from a re-verify (BC17
/// versus BC20): the same verify, guard and qualify logic runs in both, but
/// a pair that no longer qualifies on verified counts stays `candidate` the
/// first time and is demoted the second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VerifyPhase {
    First,
    Reverify,
}

/// Builds the `feed` row TECH-DESIGN section 7.2 step 4 promotes: verified
/// counts, `ratio` (`D`) and `rank` from the current age, `promoted_at` and
/// `verified_at` both `now` (`feed::promote`'s `ON CONFLICT` keeps the
/// stored `promoted_at` on a repeat promotion, so passing `now` here is
/// correct either way, BC16).
fn build_feed_row(
    verified: &verify::VerifiedPair,
    weights: &Weights,
    thresholds: &Thresholds,
    now: i64,
) -> feed::FeedRow {
    let eq = score::engagement(&verified.counts_q, weights);
    let eo = score::engagement(&verified.counts_o, weights);
    let d = score::ratio(eq, eo, thresholds.k);
    // BC27's rule applies here too: a `quoted_at` in the future never
    // yields a negative age.
    let age_hours = ((now - verified.quoted_at) as f64 / 3600.0).max(0.0);
    let rank = score::rank(d, eq, age_hours);
    feed::FeedRow {
        quote_uri: verified.quote_uri.clone(),
        quote_cid: verified.quote_cid.clone(),
        quote_did: verified.quote_did.clone(),
        original_did: verified.original_did.clone(),
        quoted_at: verified.quoted_at,
        v_likes_q: i64::from(verified.counts_q.likes),
        v_reposts_q: i64::from(verified.counts_q.reposts),
        v_replies_q: i64::from(verified.counts_q.replies),
        v_likes_o: i64::from(verified.counts_o.likes),
        v_reposts_o: i64::from(verified.counts_o.reposts),
        v_replies_o: i64::from(verified.counts_o.replies),
        ratio: d,
        rank,
        promoted_at: now,
        verified_at: now,
    }
}

/// Steps 2, 3 and 4, TECH-DESIGN section 7.2 and section 8, folded into one
/// call per verify phase (round 2 finding 10, replacing the old separate
/// `verify_step` and `promote_or_drop_step`, called once from `select` and
/// again from re-verify): dedupes `Q` and `O` URIs across `pairs`, calls
/// `PostSource::get_posts_lenient` once (BC5; it does its own chunking at
/// `AppViewClient::POSTS_BATCH`, round 2 finding 4), then for every pair
/// whose chunk succeeded runs `verify_pair`. A hard-check `Verdict::Drop`
/// (BC6 to BC15) is queued directly. A `Verdict::Continue` pair is checked
/// against `score::qualifies` right away (story 10's correction round,
/// BC39, BC40): a pair that does not qualify never reaches the guard at all,
/// costing it no `getProfiles` call, and keeps today's behaviour — stays
/// `candidate` on a first verify (BC17), demotes on a re-verify (BC20) — the
/// same as before story 10 existed. Only the qualifying subset is collected
/// and, once the loop is done, handed to `guards::check_batch` in one call,
/// which reaches the `authors` cache and the App View itself.
/// `check_batch`'s `GuardResult`s come back one per pair, in the same order,
/// and drive promote/drop/defer: a `GuardResult::Drop` drops with its reason
/// (BC17, BC18); a `Pass` always promotes, since the pair is already known to
/// qualify (BC40); a `GuardResult::Defer` (BC21) is left out of both the
/// verdicts and the dirty-flag clear, so the pair stays `candidate` and
/// dirty for the next pass to retry, the same treatment a failed `getPosts`
/// chunk already gets. Every `PairOutcome` is applied in one
/// `Store::apply_verdicts` call (BC48), and dirty is cleared on every
/// decided pair's local counts in one `Store::clear_dirty_if_unchanged` call
/// (BC43, BC44). An empty `pairs` makes no App View call at all (BC4).
#[allow(clippy::too_many_arguments)]
async fn verify_and_apply<S: PostSource + ProfileSource>(
    store: &Store,
    source: &S,
    pairs: Vec<PairWithCounts>,
    weights: &Weights,
    thresholds: &Thresholds,
    now: i64,
    phase: VerifyPhase,
    counters: &mut PassCounters,
    guard_cfg: &guards::GuardConfig,
    histogram_open: bool,
) -> Result<(), ScorerError> {
    if pairs.is_empty() {
        return Ok(());
    }

    // Round 2 finding 5's minor note: checks membership by reference first,
    // so a URI already seen (the common case once `Q` and `O` sides overlap
    // across pairs) costs one lookup, not a wasted clone that `insert`
    // would immediately drop on the duplicate path.
    let mut uris: Vec<String> = Vec::with_capacity(pairs.len() * 2);
    let mut seen: HashSet<String> = HashSet::with_capacity(pairs.len() * 2);
    for pair in &pairs {
        if !seen.contains(&pair.quote_uri) {
            seen.insert(pair.quote_uri.clone());
            uris.push(pair.quote_uri.clone());
        }
        if !seen.contains(&pair.original_uri) {
            seen.insert(pair.original_uri.clone());
            uris.push(pair.original_uri.clone());
        }
    }

    let outcome: PostsOutcome = source.get_posts_lenient(&uris).await;
    counters.appview_calls += outcome.calls;
    if !outcome.failed_uris.is_empty() {
        tracing::warn!(
            failed_uris = outcome.failed_uris.len(),
            "scorer: a getPosts chunk failed after retries; its pairs stay candidate"
        );
    }

    let mut pair_outcomes: Vec<PairOutcome> = Vec::with_capacity(pairs.len());
    let mut clear_rows: Vec<(String, Counts)> = Vec::with_capacity(pairs.len() * 2);
    // Every qualifying `Verdict::Continue` pair, alongside the (quote_uri,
    // counts_q, original_uri, counts_o)` `clear_dirty_if_unchanged` needs
    // for it once the guard decides. Kept in step with `continue_pairs` by
    // index.
    let mut continue_pairs: Vec<verify::VerifiedPair> = Vec::new();
    let mut continue_local: Vec<(String, Counts, String, Counts)> = Vec::new();

    for pair in pairs {
        if outcome.failed_uris.contains(&pair.quote_uri)
            || outcome.failed_uris.contains(&pair.original_uri)
        {
            // BC36, BC45: left out of both the clear and the verdicts, so
            // the pair stays dirty and `candidate` with no second write.
            continue;
        }

        let verdict = verify::verify_pair(
            &pair.quote_uri,
            &pair.original_uri,
            pair.quoted_at,
            &outcome.posts,
        );
        match verdict {
            Verdict::Drop(reason) => {
                clear_rows.push((pair.quote_uri.clone(), pair.counts_q));
                clear_rows.push((pair.original_uri.clone(), pair.counts_o));
                pair_outcomes.push(PairOutcome::Drop { quote_uri: pair.quote_uri.clone(), reason });
                *counters.dropped.entry(reason.as_str()).or_insert(0) += 1;
            }
            Verdict::Continue(verified) => {
                // BC39, BC40: `score::qualifies` decides right here, before
                // the guard ever runs, so a non-qualifying pair costs no
                // `getProfiles` call.
                let eq = score::engagement(&verified.counts_q, weights);
                let eo = score::engagement(&verified.counts_o, weights);
                if score::qualifies(eq, eo, thresholds) {
                    continue_local.push((
                        pair.quote_uri.clone(),
                        pair.counts_q,
                        pair.original_uri.clone(),
                        pair.counts_o,
                    ));
                    continue_pairs.push(verified);
                } else {
                    clear_rows.push((pair.quote_uri.clone(), pair.counts_q));
                    clear_rows.push((pair.original_uri.clone(), pair.counts_o));
                    if phase == VerifyPhase::Reverify {
                        pair_outcomes.push(PairOutcome::Demote { quote_uri: verified.quote_uri });
                        counters.demoted += 1;
                    }
                    // VerifyPhase::First and does not qualify: BC17, the
                    // pair simply stays `candidate`.
                }
            }
        }
    }

    let guard_results = guards::check_batch(
        store,
        source,
        guard_cfg,
        &continue_pairs,
        now,
        histogram_open,
        counters,
    )
    .await?;

    for ((verified, local), result) in
        continue_pairs.into_iter().zip(continue_local).zip(guard_results)
    {
        match result {
            GuardResult::Drop(reason) => {
                clear_rows.push((local.0, local.1));
                clear_rows.push((local.2, local.3));
                pair_outcomes
                    .push(PairOutcome::Drop { quote_uri: verified.quote_uri.clone(), reason });
                *counters.dropped.entry(reason.as_str()).or_insert(0) += 1;
            }
            GuardResult::Pass => {
                // BC40: every pair reaching the guard already qualifies.
                clear_rows.push((local.0, local.1));
                clear_rows.push((local.2, local.3));
                let row = build_feed_row(&verified, weights, thresholds, now);
                pair_outcomes.push(PairOutcome::Promote(row));
                if phase == VerifyPhase::First {
                    counters.promoted += 1;
                }
            }
            // BC21: left out of both the clear and the verdicts, exactly
            // like a failed `getPosts` chunk above, so the pair stays
            // `candidate` and dirty for the next pass to retry.
            GuardResult::Defer => {}
        }
    }

    if !pair_outcomes.is_empty() {
        let store_apply = store.clone();
        blocking(move || store_apply.apply_verdicts(&pair_outcomes)).await?;
    }
    if !clear_rows.is_empty() {
        let store_clear = store.clone();
        blocking(move || store_clear.clear_dirty_if_unchanged(&clear_rows)).await?;
    }

    Ok(())
}

/// Step 6, TECH-DESIGN section 7.2: `Store::expire`, then the non-empty
/// `evicted_uris` on `evict_tx` (BC22 to BC25). The scorer never touches
/// the `HotSet` itself; a send failure (nothing listening) is not a scorer
/// failure.
async fn expire_step(
    store: &Store,
    now: i64,
    cfg: &Config,
    evict_tx: &mpsc::UnboundedSender<Vec<String>>,
    counters: &mut PassCounters,
) -> Result<(), ScorerError> {
    let candidate_ttl_h = i64::from(cfg.candidate_ttl_h);
    let feed_ttl_d = i64::from(cfg.feed_ttl_d);
    let store_expire = store.clone();
    let report = blocking(move || store_expire.expire(now, candidate_ttl_h, feed_ttl_d)).await?;
    counters.expired = report.candidates_expired + report.feed_expired;
    if !report.evicted_uris.is_empty() {
        let _ = evict_tx.send(report.evicted_uris);
    }
    Ok(())
}

/// Step 7, TECH-DESIGN section 7.2 and 7.3: rebuild the ranked, capped
/// snapshot from the current `feed` rows and swap it in. Round 2 finding 6:
/// `feed_rows` and `snapshot::build` both run inside the same
/// `spawn_blocking` closure, since `build` is itself plain, synchronous CPU
/// work over owned data with no need to hop back onto the async task
/// between the two. `SnapshotHandle::swap` (BC33) still holds the write lock
/// only for the pointer replacement, since `build` finishes before `swap` is
/// ever called. `last_scorer_pass` is written after the swap, matching
/// TECH-DESIGN section 7.2 step 7's order (BC34). `k` reaches `build` from
/// `Config` (round 2 finding 7, BC50).
async fn snapshot_step(
    store: &Store,
    snapshot: &SnapshotHandle,
    weights: &Weights,
    now: i64,
    counters: &mut PassCounters,
    k: f64,
) -> Result<(), ScorerError> {
    let store_read = store.clone();
    let weights = *weights;
    let items = blocking(move || {
        let rows = store_read.feed_rows()?;
        Ok(snapshot::build(rows, &weights, now, k))
    })
    .await?;
    counters.snapshot_len = items.len();
    snapshot.swap(Arc::new(items));

    let store_meta = store.clone();
    let now_str = now.to_string();
    blocking(move || store_meta.meta_set("last_scorer_pass", &now_str)).await?;
    Ok(())
}

/// One full pass, TECH-DESIGN section 7.2 steps 1 to 7. `do_reverify` is
/// the caller's decision, made against `cfg.reverify_interval_s`'s own
/// timer (BC19); `one_pass` itself never reads a clock beyond the `now` it
/// is given, so it stays testable without real time passing. The histogram
/// period (story 10, revised by its correction round) is resolved once, up
/// front, so both verify phases fold the same pass's follower distribution
/// into the same `counters`; the follower floor itself is always live
/// (BC41) regardless of the period, and the histogram line it collects is
/// logged once, after both phases have run (BC29).
pub async fn one_pass<S: PostSource + ProfileSource>(
    store: &Store,
    source: &S,
    cfg: &Config,
    evict_tx: &mpsc::UnboundedSender<Vec<String>>,
    now: i64,
    do_reverify: bool,
    snapshot: &SnapshotHandle,
) -> Result<PassCounters, ScorerError> {
    let start = Instant::now();
    let mut counters = PassCounters::default();
    let weights = Weights::from(cfg);
    let thresholds = Thresholds::from(cfg);
    let guard_cfg = guards::GuardConfig::from(cfg);
    let candidate_ttl_h = i64::from(cfg.candidate_ttl_h);
    let histogram_open = guards::log_only_window(store, &guard_cfg, now).await?;

    // Steps 1 to 4: select, verify, guard, promote or drop.
    let selected = select_step(store, now, cfg, &weights, &thresholds).await?;
    counters.selected = selected.len();
    verify_and_apply(
        store,
        source,
        selected,
        &weights,
        &thresholds,
        now,
        VerifyPhase::First,
        &mut counters,
        &guard_cfg,
        histogram_open,
    )
    .await?;

    // Step 5: re-verify, on its own timer (BC19, BC21).
    if do_reverify {
        let store_promoted = store.clone();
        let promoted =
            blocking(move || store_promoted.promoted_within(now, candidate_ttl_h)).await?;
        verify_and_apply(
            store,
            source,
            promoted,
            &weights,
            &thresholds,
            now,
            VerifyPhase::Reverify,
            &mut counters,
            &guard_cfg,
            histogram_open,
        )
        .await?;
    }

    // Step 6: expire.
    expire_step(store, now, cfg, evict_tx, &mut counters).await?;

    // Step 7: snapshot.
    snapshot_step(store, snapshot, &weights, now, &mut counters, thresholds.k).await?;

    counters.duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

    // BC37: one `info` line with every counter.
    let dropped_total: usize = counters.dropped.values().sum();
    tracing::info!(
        selected = counters.selected,
        appview_calls = counters.appview_calls,
        profile_calls = counters.profile_calls,
        deferred = counters.deferred,
        promoted = counters.promoted,
        demoted = counters.demoted,
        dropped = dropped_total,
        dropped_by_reason = ?counters.dropped,
        expired = counters.expired,
        snapshot_len = counters.snapshot_len,
        duration_ms = counters.duration_ms,
        "scorer: pass complete"
    );

    // BC26, BC27, BC29: exactly one histogram line per pass, and only while
    // the histogram period is open. The floor itself is always live (BC41);
    // past the period, only the distribution line stops.
    if histogram_open {
        let h = &counters.guard_histogram;
        tracing::info!(
            guard_would_drop = counters.guard_would_drop,
            zero = h.zero,
            one_to_99 = h.one_to_99,
            hundred_to_999 = h.hundred_to_999,
            thousand_to_9999 = h.thousand_to_9999,
            ten_k_to_99999 = h.ten_k_to_99999,
            hundred_k_plus = h.hundred_k_plus,
            "scorer: guard log-only window follower distribution"
        );
    }

    Ok(counters)
}

/// The scorer task loop: one pass per tick of `cfg.scorer_interval_s`,
/// re-verifying whenever `cfg.reverify_interval_s` has elapsed since the
/// last re-verify (the first tick always re-verifies, though there is
/// nothing promoted yet to find). `shutdown_rx` flipping `true` returns
/// `Ok(())` without starting a new pass; a pass already running always
/// finishes, since a tick's `one_pass` call is awaited outside the
/// `select!` that watches for shutdown (BC38).
pub async fn run<S>(
    store: Store,
    source: S,
    cfg: Config,
    evict_tx: mpsc::UnboundedSender<Vec<String>>,
    mut shutdown_rx: watch::Receiver<bool>,
    snapshot: SnapshotHandle,
    health: HealthState,
) -> Result<(), ScorerError>
where
    S: PostSource + ProfileSource + Send + Sync + 'static,
{
    let mut ticker = interval(Duration::from_secs(u64::from(cfg.scorer_interval_s)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let reverify_interval_s = i64::from(cfg.reverify_interval_s);
    let mut last_reverify: i64 = 0;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if *shutdown_rx.borrow() {
                    return Ok(());
                }
                let now = unix_now();
                let do_reverify = last_reverify == 0 || now - last_reverify >= reverify_interval_s;
                if do_reverify {
                    last_reverify = now;
                }
                one_pass(&store, &source, &cfg, &evict_tx, now, do_reverify, &snapshot).await?;
                // BC25, BC34: recorded after the pass has fully committed
                // and swapped its snapshot in, matching TECH-DESIGN section
                // 7.2 step 7's order.
                health.set_scorer_pass(now);
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appview::types::{
        EmbedRecordViewRecord, EmbedView, PostRecord, PostView, PostViewAuthor, ProfileView,
        RecordViewInner,
    };
    use crate::store::writer::{CountField, Op, WriterConfig, WriterHandle};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn cfg(overrides: &[(&str, &str)]) -> Config {
        let mut pairs =
            vec![("DUNK_HOSTNAME", "feed.example.com"), ("DUNK_PUBLISHER_DID", "did:plc:abc")];
        pairs.extend_from_slice(overrides);
        let map: std::collections::HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        crate::config::load(move |name| map.get(name).cloned()).unwrap()
    }

    async fn test_store_with_writer() -> (Store, WriterHandle) {
        let store = Store::open_memory().unwrap();
        let writer = store
            .writer_with(WriterConfig {
                capacity: 100,
                max_ops: 1,
                interval: Duration::from_millis(5),
            })
            .unwrap();
        (store, writer)
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_pair_op(
        writer: &WriterHandle,
        quote_uri: &str,
        quote_did: &str,
        original_uri: &str,
        original_did: &str,
        quoted_at: i64,
        first_seen_at: i64,
        seq: u64,
    ) {
        writer
            .send(Op::InsertPair {
                quote_uri: quote_uri.to_string(),
                quote_did: quote_did.to_string(),
                quote_cid: format!("cid-{quote_uri}"),
                original_uri: original_uri.to_string(),
                original_did: original_did.to_string(),
                quoted_at,
                first_seen_at,
                seq,
            })
            .await
            .unwrap();
    }

    async fn incr_n(writer: &WriterHandle, uri: &str, field: CountField, n: u32, seq_start: u64) {
        for i in 0..n {
            writer
                .send(Op::Incr { post_uri: uri.to_string(), field, seq: seq_start + u64::from(i) })
                .await
                .unwrap();
        }
    }

    fn post(uri: &str, did: &str, likes: u32, embed: Option<EmbedView>) -> PostView {
        PostView {
            uri: uri.to_string(),
            cid: format!("cid-{uri}"),
            author: PostViewAuthor { did: did.to_string() },
            labels: vec![],
            record: PostRecord {
                created_at: "2026-01-01T00:00:00Z".to_string(),
                rest: serde_json::json!({}),
            },
            like_count: likes,
            repost_count: 0,
            reply_count: 0,
            embed,
        }
    }

    fn view_record_embed(uri: &str) -> EmbedView {
        EmbedView::Record {
            record: RecordViewInner::ViewRecord(EmbedRecordViewRecord { uri: uri.to_string() }),
        }
    }

    /// A fake `PostSource` and `ProfileSource` over a fixed map of posts.
    /// `failing` names URIs that make the whole `getPosts` chunk containing
    /// them fail, standing in for a call that exhausted its three retries
    /// (BC36). `ProfileSource`, story 10's addition: by default every
    /// requested DID comes back active, unlabelled and with a follower count
    /// far past any test's floor, so a test written before story 10 still
    /// promotes exactly as it did when the guard was a no-op stub;
    /// `failing_dids` opts a test into the deferred path instead (BC21).
    #[derive(Clone, Default)]
    struct FakeSource {
        posts: Arc<HashMap<String, PostView>>,
        fail_uris: Arc<HashSet<String>>,
        fail_dids: Arc<HashSet<String>>,
        calls: Arc<AtomicUsize>,
    }

    impl FakeSource {
        fn new(posts: Vec<PostView>) -> Self {
            FakeSource {
                posts: Arc::new(posts.into_iter().map(|p| (p.uri.clone(), p)).collect()),
                fail_uris: Arc::new(HashSet::new()),
                fail_dids: Arc::new(HashSet::new()),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn failing(mut self, uris: &[&str]) -> Self {
            self.fail_uris = Arc::new(uris.iter().map(|s| s.to_string()).collect());
            self
        }

        fn failing_dids(mut self, dids: &[&str]) -> Self {
            self.fail_dids = Arc::new(dids.iter().map(|s| s.to_string()).collect());
            self
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl PostSource for FakeSource {
        fn get_posts_lenient(&self, uris: &[String]) -> impl Future<Output = PostsOutcome> + Send {
            let mut outcome = PostsOutcome::default();
            for chunk in uris.chunks(AppViewClient::POSTS_BATCH) {
                self.calls.fetch_add(1, Ordering::SeqCst);
                outcome.calls += 1;
                let hit_fail = chunk.iter().any(|u| self.fail_uris.contains(u));
                if hit_fail {
                    outcome.failed_uris.extend(chunk.iter().cloned());
                } else {
                    outcome.posts.extend(
                        chunk
                            .iter()
                            .filter_map(|u| self.posts.get(u).cloned().map(|p| (u.clone(), p))),
                    );
                }
            }
            async move { outcome }
        }
    }

    impl ProfileSource for FakeSource {
        fn get_profiles_lenient(
            &self,
            dids: &[String],
        ) -> impl Future<Output = ProfilesOutcome> + Send {
            let mut outcome = ProfilesOutcome::default();
            for chunk in dids.chunks(AppViewClient::PROFILES_BATCH) {
                outcome.calls += 1;
                let hit_fail = chunk.iter().any(|d| self.fail_dids.contains(d));
                if hit_fail {
                    outcome.failed_dids.extend(chunk.iter().cloned());
                } else {
                    outcome.profiles.extend(chunk.iter().map(|d| {
                        (
                            d.clone(),
                            ProfileView {
                                did: d.clone(),
                                followers_count: 1_000_000,
                                labels: vec![],
                            },
                        )
                    }));
                }
            }
            async move { outcome }
        }
    }

    // AC1, BC1, BC2, BC3: a pair below the prefilter fraction on either
    // comparison is excluded from `select_step`'s result, but its dirty
    // flag is still cleared, the same as a kept pair's.
    #[tokio::test]
    async fn prefilter_excludes_below_fraction() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]); // P=50, M=1.25, prefilter_fraction=0.5, K=5.
        let now = 1_700_100_000;
        let first_seen_at = now - 3600;

        let below = "at://did:plc:below/app.bsky.feed.post/q";
        let below_o = "at://did:plc:belowo/app.bsky.feed.post/o";
        insert_pair_op(
            &writer,
            below,
            "did:plc:below",
            below_o,
            "did:plc:belowo",
            now,
            first_seen_at,
            1,
        )
        .await;
        incr_n(&writer, below, CountField::Likes, 5, 2).await; // E=5 < P*0.5=25.

        let kept_uri = "at://did:plc:kept/app.bsky.feed.post/q";
        let kept_o = "at://did:plc:kepto/app.bsky.feed.post/o";
        insert_pair_op(
            &writer,
            kept_uri,
            "did:plc:kept",
            kept_o,
            "did:plc:kepto",
            now,
            first_seen_at,
            100,
        )
        .await;
        incr_n(&writer, kept_uri, CountField::Likes, 60, 101).await; // E=60 >= 25, D=12 >= 0.625.

        writer.flush().await.unwrap();

        let weights = Weights::from(&cfg);
        let thresholds = Thresholds::from(&cfg);
        let kept = select_step(&store, now, &cfg, &weights, &thresholds).await.unwrap();

        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].quote_uri, kept_uri);

        // BC2 (round 2 finding 2's amendment): the excluded pair's dirty
        // flag is cleared right in `select_step`. The kept pair's flag is
        // left untouched here — it clears only once `verify_and_apply`
        // knows its `getPosts` chunk actually succeeded (BC43, BC44, BC45).
        let again = store.dirty_candidates(now, i64::from(cfg.candidate_ttl_h)).unwrap();
        assert_eq!(again.len(), 1, "only the excluded pair's dirty flag is cleared here");
        assert_eq!(again[0].quote_uri, kept_uri);
    }

    // BC16: a `Continue` verdict that qualifies and passes the guard is
    // promoted: `feed` gets a row and the pair leaves `candidate`.
    #[tokio::test]
    async fn promotes_a_pair_that_qualifies() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]);
        let now = 1_700_100_000;
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        insert_pair_op(
            &writer,
            quote_uri,
            "did:plc:quoter",
            original_uri,
            "did:plc:original",
            now,
            now - 3600,
            1,
        )
        .await;
        incr_n(&writer, quote_uri, CountField::Likes, 60, 2).await;
        writer.flush().await.unwrap();

        let source = FakeSource::new(vec![
            post(quote_uri, "did:plc:quoter", 60, Some(view_record_embed(original_uri))),
            post(original_uri, "did:plc:original", 0, None),
        ]);
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();

        let snapshot = SnapshotHandle::new();
        let counters =
            one_pass(&store, &source, &cfg, &evict_tx, now, false, &snapshot).await.unwrap();

        assert_eq!(counters.promoted, 1);
        assert_eq!(counters.profile_calls, 1, "the pass line's profile_calls reflects the fetch");
        let feed_rows = store.feed_rows().unwrap();
        assert_eq!(feed_rows.len(), 1);
        assert_eq!(feed_rows[0].quote_uri, quote_uri);
    }

    // BC17: a `Continue` verdict that does not qualify, on a first verify,
    // stays `candidate` rather than dropping. Proven indirectly: no `feed`
    // row is written and nothing is counted as dropped or promoted.
    #[tokio::test]
    async fn candidate_that_fails_to_qualify_stays_candidate() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]);
        let now = 1_700_100_000;
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        insert_pair_op(
            &writer,
            quote_uri,
            "did:plc:quoter",
            original_uri,
            "did:plc:original",
            now,
            now - 3600,
            1,
        )
        .await;
        // Local E only needs to clear the *prefilter* fraction (0.5) to
        // reach verify; the *verified* counts below still fall short of the
        // real P=50 floor.
        incr_n(&writer, quote_uri, CountField::Likes, 30, 2).await;
        writer.flush().await.unwrap();

        let source = FakeSource::new(vec![
            post(quote_uri, "did:plc:quoter", 30, Some(view_record_embed(original_uri))),
            post(original_uri, "did:plc:original", 0, None),
        ]);
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();

        let snapshot = SnapshotHandle::new();
        let counters =
            one_pass(&store, &source, &cfg, &evict_tx, now, false, &snapshot).await.unwrap();

        assert_eq!(counters.promoted, 0);
        assert!(counters.dropped.is_empty());
        assert!(store.feed_rows().unwrap().is_empty());
        // Still present and still `candidate`: a further local event marks
        // it dirty again and a later pass would find it.
        incr_n(&writer, quote_uri, CountField::Likes, 1, 1000).await;
        writer.flush().await.unwrap();
        let found = store.dirty_candidates(now, i64::from(cfg.candidate_ttl_h)).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].quote_uri, quote_uri);
    }

    /// A `feed::FeedRow` with sensible non-zero defaults, so a test that
    /// only cares about a handful of fields does not have to spell out
    /// every column.
    fn feed_row(
        quote_uri: &str,
        quote_did: &str,
        original_did: &str,
        quoted_at: i64,
        now: i64,
    ) -> feed::FeedRow {
        feed::FeedRow {
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
            rank: 1.0,
            promoted_at: now,
            verified_at: now,
        }
    }

    // AC3, BC19, BC20: re-verifying a promoted pair whose verified counts
    // no longer qualify demotes it — its `feed` row is deleted.
    #[tokio::test]
    async fn reverify_demotes() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]);
        let now = 1_700_100_000;
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";

        // Insert the pair already promoted, with counts that once
        // qualified.
        insert_pair_op(
            &writer,
            quote_uri,
            "did:plc:quoter",
            original_uri,
            "did:plc:original",
            now,
            now - 3600,
            1,
        )
        .await;
        writer.flush().await.unwrap();
        store
            .promote(&feed_row(quote_uri, "did:plc:quoter", "did:plc:original", now, now))
            .unwrap();

        // The App View now reports much lower counts: no longer qualifies.
        let source = FakeSource::new(vec![
            post(quote_uri, "did:plc:quoter", 1, Some(view_record_embed(original_uri))),
            post(original_uri, "did:plc:original", 0, None),
        ]);
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();

        let snapshot = SnapshotHandle::new();
        let counters =
            one_pass(&store, &source, &cfg, &evict_tx, now, true, &snapshot).await.unwrap();

        assert_eq!(counters.demoted, 1);
        assert!(store.feed_rows().unwrap().is_empty());
    }

    // AC4, BC21: a promoted pair whose `quoted_at` is older than
    // `candidate_ttl_h` is never selected by `promoted_within`, so a
    // re-verify pass never even calls the App View for it.
    #[tokio::test]
    async fn old_promoted_pairs_skip_reverify() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]); // candidate_ttl_h defaults to 48.
        let now = 1_700_100_000;
        let old_quoted_at = now - 100 * 3600; // Older than 48h.
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";

        insert_pair_op(
            &writer,
            quote_uri,
            "did:plc:quoter",
            original_uri,
            "did:plc:original",
            old_quoted_at,
            old_quoted_at,
            1,
        )
        .await;
        writer.flush().await.unwrap();
        store
            .promote(&feed_row(
                quote_uri,
                "did:plc:quoter",
                "did:plc:original",
                old_quoted_at,
                old_quoted_at,
            ))
            .unwrap();

        // If reverify wrongly picked this up, the fake source's low counts
        // would demote it.
        let source = FakeSource::new(vec![
            post(quote_uri, "did:plc:quoter", 1, Some(view_record_embed(original_uri))),
            post(original_uri, "did:plc:original", 0, None),
        ]);
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();

        let snapshot = SnapshotHandle::new();
        let counters =
            one_pass(&store, &source, &cfg, &evict_tx, now, true, &snapshot).await.unwrap();

        assert_eq!(counters.demoted, 0);
        assert_eq!(source.call_count(), 0, "an old promoted pair is never re-verified");
        assert_eq!(store.feed_rows().unwrap().len(), 1, "the feed row is untouched");
    }

    // AC5, BC22, BC23, BC24: expire deletes stale `candidate` pairs and
    // their orphaned `counts` rows, and sends the evicted URIs on
    // `evict_tx`.
    #[tokio::test]
    async fn expiry_removes_stale_rows() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]);
        let now = 1_700_100_000;

        let quote_uri = "at://did:plc:old/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:oldo/app.bsky.feed.post/o";
        // first_seen_at = 0 is far past the 48h candidate TTL from `now`.
        insert_pair_op(&writer, quote_uri, "did:plc:old", original_uri, "did:plc:oldo", 0, 0, 1)
            .await;
        incr_n(&writer, original_uri, CountField::Likes, 1, 2).await;
        writer.flush().await.unwrap();

        let source = FakeSource::new(vec![]);
        let (evict_tx, mut evict_rx) = mpsc::unbounded_channel();

        let snapshot = SnapshotHandle::new();
        let counters =
            one_pass(&store, &source, &cfg, &evict_tx, now, false, &snapshot).await.unwrap();

        assert_eq!(counters.expired, 1);
        let mut evicted = evict_rx.try_recv().unwrap();
        evicted.sort();
        let mut expected = vec![quote_uri.to_string(), original_uri.to_string()];
        expected.sort();
        assert_eq!(evicted, expected);

        let mut remaining = Vec::new();
        store.for_each_hot_uri(|uri| remaining.push(uri.to_string())).unwrap();
        assert!(!remaining.contains(&quote_uri.to_string()));
    }

    // BC4: an empty select makes no App View call, but the pass still
    // expires.
    #[tokio::test]
    async fn empty_select_still_expires() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]);
        let now = 1_700_100_000;
        let quote_uri = "at://did:plc:old/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:oldo/app.bsky.feed.post/o";
        insert_pair_op(&writer, quote_uri, "did:plc:old", original_uri, "did:plc:oldo", 0, 0, 1)
            .await;
        writer.flush().await.unwrap();

        let source = FakeSource::new(vec![]);
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();

        let snapshot = SnapshotHandle::new();
        let counters =
            one_pass(&store, &source, &cfg, &evict_tx, now, false, &snapshot).await.unwrap();

        assert_eq!(counters.selected, 0);
        assert_eq!(counters.appview_calls, 0);
        assert_eq!(source.call_count(), 0);
        assert_eq!(counters.expired, 1);
    }

    // BC36, BC45 (round 2 finding 2's amendment): a `getPosts` chunk failure
    // leaves every pair whose `Q` or `O` was in it `candidate`, leaves its
    // counts dirty with no second write (it was never cleared, since
    // `select_step` no longer clears a kept pair up front), and promotes
    // nothing from that chunk.
    #[tokio::test]
    async fn chunk_failure_keeps_pair_candidate_and_dirty() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]);
        let now = 1_700_100_000;
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        insert_pair_op(
            &writer,
            quote_uri,
            "did:plc:quoter",
            original_uri,
            "did:plc:original",
            now,
            now - 3600,
            1,
        )
        .await;
        incr_n(&writer, quote_uri, CountField::Likes, 60, 2).await;
        writer.flush().await.unwrap();

        let source = FakeSource::new(vec![
            post(quote_uri, "did:plc:quoter", 60, Some(view_record_embed(original_uri))),
            post(original_uri, "did:plc:original", 0, None),
        ])
        .failing(&[quote_uri]);
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();

        let snapshot = SnapshotHandle::new();
        let counters =
            one_pass(&store, &source, &cfg, &evict_tx, now, false, &snapshot).await.unwrap();

        assert_eq!(counters.promoted, 0);
        assert!(counters.dropped.is_empty());
        assert!(store.feed_rows().unwrap().is_empty());

        let found = store.dirty_candidates(now, i64::from(cfg.candidate_ttl_h)).unwrap();
        assert_eq!(found.len(), 1, "the chunk failure left the pair's counts dirty");
        assert_eq!(found[0].quote_uri, quote_uri);
    }

    // BC21: a failed `getProfiles` chunk defers the pair through the guard
    // instead of dropping or promoting it, the same "stays candidate and
    // dirty" treatment a failed `getPosts` chunk gets above.
    #[tokio::test]
    async fn profile_fetch_failure_defers_the_pair_and_leaves_it_dirty() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]);
        let now = 1_700_100_000;
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        insert_pair_op(
            &writer,
            quote_uri,
            "did:plc:quoter",
            original_uri,
            "did:plc:original",
            now,
            now - 3600,
            1,
        )
        .await;
        incr_n(&writer, quote_uri, CountField::Likes, 60, 2).await;
        writer.flush().await.unwrap();

        let source = FakeSource::new(vec![
            post(quote_uri, "did:plc:quoter", 60, Some(view_record_embed(original_uri))),
            post(original_uri, "did:plc:original", 0, None),
        ])
        .failing_dids(&["did:plc:original"]);
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();

        let snapshot = SnapshotHandle::new();
        let counters =
            one_pass(&store, &source, &cfg, &evict_tx, now, false, &snapshot).await.unwrap();

        assert_eq!(counters.promoted, 0);
        assert_eq!(counters.deferred, 1);
        assert_eq!(counters.profile_calls, 1, "the pass line's profile_calls counts the attempt");
        assert!(counters.dropped.is_empty());
        assert!(store.feed_rows().unwrap().is_empty());

        let found = store.dirty_candidates(now, i64::from(cfg.candidate_ttl_h)).unwrap();
        assert_eq!(found.len(), 1, "the deferred pair's counts are still dirty");
        assert_eq!(found[0].quote_uri, quote_uri);
    }

    // BC38: a shutdown signal already flipped before the first tick returns
    // `Ok(())` without running a pass.
    // No `start_paused`: `shutdown_rx.changed()` is already ready when
    // `run` starts (the sender fired before the call), so `select!` picks
    // it over `ticker.tick()` without waiting out a real interval.
    #[tokio::test]
    async fn run_returns_ok_without_a_new_pass_after_shutdown() {
        let store = Store::open_memory().unwrap();
        let cfg = cfg(&[]);
        let source = FakeSource::new(vec![]);
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        shutdown_tx.send(true).unwrap();

        let snapshot = SnapshotHandle::new();
        let health = HealthState::new();
        let result = run(store, source.clone(), cfg, evict_tx, shutdown_rx, snapshot, health).await;

        assert!(result.is_ok());
        assert_eq!(source.call_count(), 0, "no pass ran after shutdown");
    }

    // AC8, BC33, BC34: the snapshot handle reads empty before the first
    // pass, holds the freshly built list right after it, and
    // `last_scorer_pass` is written (after the swap, by construction: it is
    // the last thing `snapshot_step` does).
    #[tokio::test]
    async fn snapshot_swap_is_atomic() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]);
        let now = 1_700_100_000;
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        insert_pair_op(
            &writer,
            quote_uri,
            "did:plc:quoter",
            original_uri,
            "did:plc:original",
            now,
            now - 3600,
            1,
        )
        .await;
        incr_n(&writer, quote_uri, CountField::Likes, 60, 2).await;
        writer.flush().await.unwrap();

        let source = FakeSource::new(vec![
            post(quote_uri, "did:plc:quoter", 60, Some(view_record_embed(original_uri))),
            post(original_uri, "did:plc:original", 0, None),
        ]);
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();
        let snapshot = SnapshotHandle::new();

        assert!(snapshot.current().is_empty(), "BC41: empty before the first pass");

        let counters =
            one_pass(&store, &source, &cfg, &evict_tx, now, false, &snapshot).await.unwrap();

        assert_eq!(counters.snapshot_len, 1);
        let current = snapshot.current();
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].quote_uri, quote_uri);
        assert_eq!(store.meta_get("last_scorer_pass").unwrap(), Some(now.to_string()));
    }

    // AC10: a live pass against the real App View promotes the
    // TECH-DESIGN section 1 reference pair. `#[ignore]`, per `AGENTS.md`:
    // run by hand with `cargo test -- --ignored scorer_live_pass`. Local
    // counts are seeded high enough to clear the prefilter on their own;
    // what actually decides promotion here is the real App View response
    // fetched through a genuine `AppViewClient`.
    #[tokio::test]
    #[ignore]
    async fn scorer_live_pass() {
        let (store, writer) = test_store_with_writer().await;
        let cfg = cfg(&[]);
        let now = unix_now();
        let quote_uri = "at://did:plc:o7xt7svg2xtjbb4e2xqahqqc/app.bsky.feed.post/3mvxhe7uuck2n";
        let original_uri = "at://did:plc:ofzkhjyyh4kl4a35wxgmobmm/app.bsky.feed.post/3mvxb5n76u22b";
        insert_pair_op(
            &writer,
            quote_uri,
            "did:plc:o7xt7svg2xtjbb4e2xqahqqc",
            original_uri,
            "did:plc:ofzkhjyyh4kl4a35wxgmobmm",
            now - 3600,
            now,
            1,
        )
        .await;
        incr_n(&writer, quote_uri, CountField::Likes, 1000, 2).await;
        writer.flush().await.unwrap();

        let client =
            AppViewClient::new(&cfg).expect("cfg.appview_rps is validated by config::load");
        let (evict_tx, _evict_rx) = mpsc::unbounded_channel();
        let snapshot = SnapshotHandle::new();

        let counters =
            one_pass(&store, &client, &cfg, &evict_tx, now, false, &snapshot).await.unwrap();

        assert_eq!(counters.promoted, 1, "the reference pair should promote");
        let rows = store.feed_rows().unwrap();
        let row =
            rows.iter().find(|r| r.quote_uri == quote_uri).expect("the reference pair's feed row");
        assert!(
            row.v_likes_q > 0 || row.v_reposts_q > 0 || row.v_replies_q > 0,
            "verified counts must be non-zero"
        );
        assert!(row.ratio >= 1.25, "D must clear the M=1.25 margin");
        println!(
            "v_likes_q={} v_reposts_q={} v_replies_q={} v_likes_o={} v_reposts_o={} v_replies_o={} ratio={} rank={}",
            row.v_likes_q,
            row.v_reposts_q,
            row.v_replies_q,
            row.v_likes_o,
            row.v_reposts_o,
            row.v_replies_o,
            row.ratio,
            row.rank
        );
    }
}
