//! The safety guards, TECH-DESIGN section 9: follower floor, author state
//! and labels. `GuardConfig`, `GuardResult`, `decide`, `author_row_from_profile`
//! and `FollowerHistogram` are the pure pieces, each a unit test with no
//! store and no network. `check_batch` is the batch step: it reaches the
//! `authors` cache and the App View, then calls `decide` once per pair, in
//! input order. `log_only_window` resolves the histogram period's clock in
//! `meta.guard_histogram_since` and `meta.guard_histogram_floor`.
//! `verify_and_apply` (`src/scorer/mod.rs`) is the sole caller of both, and
//! only for the qualifying subset of a phase's pairs (story 10's correction
//! round, BC39, BC40).
//!
//! Story 10's correction round changed the design the story shipped with:
//! the follower floor now drops from the first pass (BC41), and
//! `DUNK_GUARD_LOG_ONLY_H`/`meta.guard_log_only_since` became
//! `DUNK_GUARD_HISTOGRAM_H`/`meta.guard_histogram_since` plus a new
//! `meta.guard_histogram_floor`: a measurement period, not a suppression
//! window. `decide` no longer takes a `log_only` flag.

use std::collections::{HashMap, HashSet};

use crate::appview::types::ProfileView;
use crate::config::Config;
use crate::scorer::verify::VerifiedPair;
use crate::scorer::{blocking, PassCounters, ProfileSource, ScorerError};
use crate::store::authors::AuthorRow;
use crate::store::{DropReason, Store};

/// The result of running every guard over one verified pair. `Drop` carries
/// the reason `Store::drop_pair` writes, the same as a hard check inside
/// `verify_pair`. `Defer` is the fourth variant: a pair whose `O` or `Q` DID
/// named a `getProfiles` chunk that failed after its retries is left
/// undecided rather than dropped or promoted, so the next pass retries it
/// (BC21).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardResult {
    Pass,
    Drop(DropReason),
    Defer,
}

/// The guard's own view of `Config`, TECH-DESIGN section 9 and section 4:
/// the follower floor, the drop-label list, the `authors` cache TTLs (active
/// and inactive) and the histogram period's length. Kept separate from
/// `Config` itself so `decide` and `check_batch` take one small,
/// guard-specific value instead of the whole binary's configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct GuardConfig {
    pub follower_floor: u32,
    pub drop_labels: Vec<String>,
    pub author_ttl_h: u32,
    pub author_inactive_ttl_h: u32,
    pub guard_histogram_h: u32,
}

impl From<&Config> for GuardConfig {
    fn from(cfg: &Config) -> Self {
        GuardConfig {
            follower_floor: cfg.follower_floor,
            drop_labels: cfg.drop_labels.clone(),
            author_ttl_h: cfg.author_ttl_h,
            author_inactive_ttl_h: cfg.author_inactive_ttl_h,
            guard_histogram_h: cfg.guard_histogram_h,
        }
    }
}

/// Whether any of `labels` is in `cfg.drop_labels`, exact and case sensitive
/// (BC13): the values the App View sends.
fn any_dropped(labels: &[String], cfg: &GuardConfig) -> bool {
    labels.iter().any(|label| cfg.drop_labels.iter().any(|dropped| dropped == label))
}

/// Decides one verified pair, pure: no store, no network. `author_o` is
/// `O`'s cache row, always present by the time a pair reaches `decide` — a
/// missing or stale row is fetched by `check_batch` before this runs, and a
/// pair whose fetch failed never reaches `decide` at all
/// (`GuardResult::Defer`, BC21). `author_q` is `Q`'s row when available
/// (BC14): its absence only narrows the label check, since the follower
/// floor and the state check are scoped to `O`'s author alone (TECH-DESIGN
/// section 9, BC7).
///
/// Order, BC15: author state, then labels, then the follower floor. A
/// `!takedown` label that also appears in `DUNK_DROP_LABELS` therefore
/// reports `author_inactive`, not `labelled`. Story 10's correction round
/// removed the `log_only` suppression this function used to take: the
/// follower floor now drops unconditionally whenever `cfg.follower_floor >
/// 0` (BC41). Whether `one_pass` also logs a histogram line is
/// `check_batch`'s and `log_only_window`'s concern, not `decide`'s.
pub fn decide(
    pair: &VerifiedPair,
    author_o: &AuthorRow,
    author_q: Option<&AuthorRow>,
    cfg: &GuardConfig,
) -> GuardResult {
    // Author state, BC5 to BC7: `O`'s row only.
    if !author_o.active {
        return GuardResult::Drop(DropReason::AuthorInactive);
    }

    // Labels, BC9 to BC14: `Q` and `O`'s own `postView.labels`, plus both
    // authors' profile labels when their rows are available.
    let author_q_labels = author_q.and_then(|row| row.labels.as_deref()).unwrap_or(&[]);
    let author_o_labels = author_o.labels.as_deref().unwrap_or(&[]);
    if any_dropped(&pair.labels_q, cfg)
        || any_dropped(&pair.labels_o, cfg)
        || any_dropped(author_q_labels, cfg)
        || any_dropped(author_o_labels, cfg)
    {
        return GuardResult::Drop(DropReason::Labelled);
    }

    // Follower floor, BC1 to BC4, BC41: the last check, live whenever
    // `follower_floor > 0`, with no suppression window any more.
    if cfg.follower_floor == 0 {
        return GuardResult::Pass; // BC3: never evaluated.
    }
    let Some(followers) = author_o.followers else {
        // BC4: the profile was fetched and simply carried no count. Unknown
        // is not evidence of being below the floor.
        tracing::debug!(did = %author_o.did, "guards: follower count unknown, passing");
        return GuardResult::Pass;
    };
    if followers < i64::from(cfg.follower_floor) {
        return GuardResult::Drop(DropReason::FollowerFloor); // BC1, BC41.
    }
    GuardResult::Pass // BC2: `<`, not `<=`.
}

/// Builds the `authors` cache row for `did` from its `getProfiles` result,
/// TECH-DESIGN section 9's author-state rule. `profile` is `None` when the
/// DID was simply absent from a successful response (BC5): the row is
/// written `active = false`, the same as a profile that carries a
/// `!takedown` label on itself (BC6). `now` becomes `checked_at`.
pub fn author_row_from_profile(profile: Option<&ProfileView>, did: &str, now: i64) -> AuthorRow {
    match profile {
        None => AuthorRow {
            did: did.to_string(),
            followers: None,
            active: false,
            labels: None,
            checked_at: now,
        },
        Some(profile) => {
            let labels: Vec<String> = profile.label_values(); // BC49
            let active = !labels.iter().any(|label| label == "!takedown");
            AuthorRow {
                did: did.to_string(),
                followers: Some(i64::from(profile.followers_count)),
                active,
                labels: Some(labels),
                checked_at: now,
            }
        }
    }
}

/// A count of `O` authors' `followers`, bucketed for the log-only window's
/// one histogram line per pass (BC29, BC30). A `followers` of `None`
/// (BC31) is never counted: the histogram measures known values only.
/// Boundaries: `0` is its own bucket, and each pair on either side of an
/// order of magnitude (`99`/`100`, `999`/`1000`, `9999`/`10000`,
/// `99999`/`100000`) falls into a different bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FollowerHistogram {
    pub zero: u32,
    pub one_to_99: u32,
    pub hundred_to_999: u32,
    pub thousand_to_9999: u32,
    pub ten_k_to_99999: u32,
    pub hundred_k_plus: u32,
}

impl FollowerHistogram {
    /// Buckets one known follower count (BC30). A negative value never
    /// reaches this in practice — `followers` is always cast up from the App
    /// View's `u32` — but is bucketed at `zero` rather than panicking or
    /// silently landing in `hundred_k_plus`.
    pub fn record(&mut self, followers: i64) {
        match followers {
            i64::MIN..=0 => self.zero += 1,
            1..=99 => self.one_to_99 += 1,
            100..=999 => self.hundred_to_999 += 1,
            1_000..=9_999 => self.thousand_to_9999 += 1,
            10_000..=99_999 => self.ten_k_to_99999 += 1,
            _ => self.hundred_k_plus += 1,
        }
    }
}

/// The batch step, TECH-DESIGN section 9's `authors` cache: dedupes `O` and
/// `Q`'s DIDs across `pairs` (BC35, item 8: checks membership by reference
/// first, so a DID already seen costs one lookup, not a clone `insert` would
/// immediately drop on the duplicate path), reads the cache in one call,
/// fetches only the missing or stale DIDs (`AuthorRow::is_fresh`, BC16,
/// BC17, BC44, BC46, BC47) in one `ProfileSource::get_profiles_lenient` call
/// (which does its own chunking at `AppViewClient::PROFILES_BATCH`, BC18),
/// writes the freshly fetched rows back in one `authors_put_many` call
/// (BC19), then calls the pure `decide` once per pair, in input order
/// (BC36). A pair naming a DID whose `getProfiles` chunk failed is never
/// decided: it becomes `GuardResult::Defer` (BC21). An empty `pairs` makes
/// no store call and no `getProfiles` call (BC34).
///
/// `counters` collects `profile_calls` (every `getProfiles` chunk attempted,
/// BC16 to BC18) and `deferred` (BC21) unconditionally. `histogram_open`
/// (`log_only_window`'s result) only gates whether this pass also folds `O`'s
/// follower distribution and the would-be-drop count into `counters`
/// (BC26, BC27, BC29 to BC31): the floor itself is always live (BC41), so
/// `guard_would_drop` simply counts pairs whose `decide` result actually was
/// `Drop(FollowerFloor)`, not a suppressed hypothetical, and the histogram
/// samples each distinct `O` author DID once per pass, not once per pair
/// (BC31a, `counters`' own `histogram_o_dids_seen`).
pub async fn check_batch<P: ProfileSource>(
    store: &Store,
    profiles: &P,
    cfg: &GuardConfig,
    pairs: &[VerifiedPair],
    now: i64,
    histogram_open: bool,
    counters: &mut PassCounters,
) -> Result<Vec<GuardResult>, ScorerError> {
    if pairs.is_empty() {
        return Ok(Vec::new()); // BC34
    }

    // BC35: `O` and `Q`'s DIDs, deduplicated before the cache read, with no
    // clone on the duplicate path (item 8).
    let mut dids: Vec<String> = Vec::with_capacity(pairs.len() * 2);
    let mut seen: HashSet<String> = HashSet::with_capacity(pairs.len() * 2);
    for pair in pairs {
        if !seen.contains(&pair.original_did) {
            seen.insert(pair.original_did.clone());
            dids.push(pair.original_did.clone());
        }
        if !seen.contains(&pair.quote_did) {
            seen.insert(pair.quote_did.clone());
            dids.push(pair.quote_did.clone());
        }
    }

    let store_read = store.clone();
    let dids_for_read = dids.clone();
    let cached = blocking(move || {
        let did_refs: Vec<&str> = dids_for_read.iter().map(String::as_str).collect();
        store_read.authors_get_many(&did_refs)
    })
    .await?;
    let mut cache: HashMap<String, AuthorRow> =
        cached.into_iter().map(|row| (row.did.clone(), row)).collect();

    // BC16, BC17, BC44, BC46, BC47: a fresh row (`AuthorRow::is_fresh`, which
    // picks the active or inactive TTL for the row's own state) needs no
    // fetch; a missing or stale row joins the fetch list.
    let ttl_active_s = i64::from(cfg.author_ttl_h) * 3600;
    let ttl_inactive_s = i64::from(cfg.author_inactive_ttl_h) * 3600;
    let stale: Vec<String> = dids
        .iter()
        .filter(|did| {
            !cache.get(*did).is_some_and(|row| row.is_fresh(now, ttl_active_s, ttl_inactive_s))
        })
        .cloned()
        .collect();

    let mut failed_dids: HashSet<String> = HashSet::new();
    if !stale.is_empty() {
        let outcome = profiles.get_profiles_lenient(&stale).await;
        counters.profile_calls += outcome.calls; // BC18
        if !outcome.failed_dids.is_empty() {
            tracing::warn!(
                failed_dids = outcome.failed_dids.len(),
                "guards: a getProfiles chunk failed after retries; its pairs are deferred"
            );
        }
        failed_dids = outcome.failed_dids;

        let mut new_rows: Vec<AuthorRow> = Vec::with_capacity(stale.len());
        for did in &stale {
            if failed_dids.contains(did) {
                continue; // BC21: no row written; the pair defers instead.
            }
            let row = author_row_from_profile(outcome.profiles.get(did), did, now);
            cache.insert(did.clone(), row.clone());
            new_rows.push(row);
        }
        if !new_rows.is_empty() {
            let store_write = store.clone();
            blocking(move || store_write.authors_put_many(&new_rows)).await?; // BC19
        }
    }

    let mut results = Vec::with_capacity(pairs.len());
    for pair in pairs {
        if failed_dids.contains(&pair.original_did) || failed_dids.contains(&pair.quote_did) {
            counters.deferred += 1;
            results.push(GuardResult::Defer);
            continue;
        }
        let author_o = cache
            .get(&pair.original_did)
            .expect("O's row is always cached or freshly fetched by this point");
        let author_q = cache.get(&pair.quote_did);

        let result = decide(pair, author_o, author_q, cfg);

        if histogram_open {
            // BC31a: one sample per distinct `O` author DID this pass, not
            // per pair.
            if let Some(followers) = author_o.followers {
                if counters.histogram_o_dids_seen.insert(pair.original_did.clone()) {
                    counters.guard_histogram.record(followers); // BC29 to BC31
                }
            }
            // BC26, BC27 (revised): the floor is always live now, so this
            // reads straight off `decide`'s own result instead of a second,
            // suppression-free call.
            if matches!(result, GuardResult::Drop(DropReason::FollowerFloor)) {
                counters.guard_would_drop += 1;
            }
        }

        results.push(result);
    }

    Ok(results)
}

/// Resolves the histogram period's state, TECH-DESIGN section 9 as revised
/// by story 10's correction round: `meta.guard_histogram_since` holds the
/// unix-second timestamp the period opened, and `meta.guard_histogram_floor`
/// the `DUNK_FOLLOWER_FLOOR` value it is measuring against. Unlike the
/// story's original design, the follower floor itself is live from the
/// first pass regardless of this period (BC41, `decide`); the period only
/// decides whether `one_pass` logs a histogram line.
///
/// `DUNK_GUARD_HISTOGRAM_H = 0` disables the period outright and writes
/// nothing (BC23). A stored floor that differs from the current
/// `DUNK_FOLLOWER_FLOOR`, or that is absent or does not parse, is treated as
/// a floor change (BC42, BC43): `since` is reset to `now` and the current
/// floor is stored, reopening the period even though a present, matching
/// `since` is otherwise never overwritten (BC24, BC25). Once the floor is
/// confirmed unchanged, a `since` that does not parse as `i64` is warned
/// about once and treated as elapsed, its stored value left alone so a
/// corrupt value can never reopen the period on its own (BC28).
pub async fn log_only_window(
    store: &Store,
    cfg: &GuardConfig,
    now: i64,
) -> Result<bool, ScorerError> {
    if cfg.guard_histogram_h == 0 {
        return Ok(false); // BC23
    }

    let store_read_floor = store.clone();
    let stored_floor = blocking(move || store_read_floor.meta_get("guard_histogram_floor")).await?;
    let floor_changed = match &stored_floor {
        None => true, // BC43: absent counts as a change.
        Some(text) => match text.parse::<u32>() {
            Ok(value) => value != cfg.follower_floor, // BC42
            Err(_) => true,                           // BC43: unparsable counts as a change.
        },
    };

    if floor_changed {
        let now_str = now.to_string();
        let floor_str = cfg.follower_floor.to_string();
        let store_write_since = store.clone();
        blocking(move || store_write_since.meta_set("guard_histogram_since", &now_str)).await?;
        let store_write_floor = store.clone();
        blocking(move || store_write_floor.meta_set("guard_histogram_floor", &floor_str)).await?;
        return Ok(true); // BC24, BC42, BC43: a freshly (re)opened period.
    }

    let store_read_since = store.clone();
    let stored_since = blocking(move || store_read_since.meta_get("guard_histogram_since")).await?;
    let since = match stored_since {
        None => {
            // The floor is unchanged but `since` is somehow missing: open
            // the period the same way an absent floor does (BC24).
            let now_str = now.to_string();
            let store_write = store.clone();
            blocking(move || store_write.meta_set("guard_histogram_since", &now_str)).await?;
            now
        }
        Some(text) => match text.parse::<i64>() {
            Ok(value) => value, // BC25
            Err(_) => {
                tracing::warn!(
                    value = %text,
                    "guards: guard_histogram_since is not a valid i64; treating the period as elapsed"
                );
                return Ok(false); // BC28
            }
        },
    };

    Ok(now - since < i64::from(cfg.guard_histogram_h) * 3600) // BC26, BC27
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::score::Counts;

    fn pair() -> VerifiedPair {
        VerifiedPair {
            quote_uri: "at://did:plc:q/app.bsky.feed.post/q".to_string(),
            quote_cid: "cid-q".to_string(),
            quote_did: "did:plc:q".to_string(),
            original_uri: "at://did:plc:o/app.bsky.feed.post/o".to_string(),
            original_did: "did:plc:o".to_string(),
            quoted_at: 0,
            counts_q: Counts::default(),
            counts_o: Counts::default(),
            labels_q: vec![],
            labels_o: vec![],
        }
    }

    const GETPROFILES_WITH_LABELS: &str =
        include_str!("../../tests/fixtures/getprofiles_with_labels.json");

    // Hand-edited from a recorded `getProfiles` body (`getprofiles_ok.json`):
    // no live profile with a `!takedown` label was found to record directly,
    // so the `labels` array on each profile was added by hand. Exercises
    // `author_row_from_profile` against the two rows the story's `## Files
    // in scope` names it for: an ordinary profile label (`spam`, stays
    // active) and a `!takedown` profile label (goes inactive, BC6).
    #[test]
    fn author_row_from_profile_reads_the_labels_fixture() {
        let decoded: crate::appview::types::GetProfilesResponse =
            serde_json::from_str(GETPROFILES_WITH_LABELS).expect("fixture decodes");
        let spam_labelled =
            decoded.profiles.iter().find(|p| p.did == "did:plc:z72i7hdynmk6r22z27h6tvur").unwrap();
        let taken_down =
            decoded.profiles.iter().find(|p| p.did == "did:plc:plcl43vt7d2ig7hif4zmyg6h").unwrap();

        let spam_row = author_row_from_profile(Some(spam_labelled), &spam_labelled.did, 0);
        assert!(spam_row.active, "a plain label does not deactivate the author");
        assert_eq!(spam_row.labels, Some(vec!["spam".to_string()]));

        let takedown_row = author_row_from_profile(Some(taken_down), &taken_down.did, 0);
        assert!(!takedown_row.active, "a !takedown label deactivates the author");
    }

    fn cfg(follower_floor: u32, drop_labels: &[&str]) -> GuardConfig {
        GuardConfig {
            follower_floor,
            drop_labels: drop_labels.iter().map(|s| s.to_string()).collect(),
            author_ttl_h: 24,
            author_inactive_ttl_h: 1,
            guard_histogram_h: 24,
        }
    }

    fn author(
        did: &str,
        followers: Option<i64>,
        active: bool,
        labels: Option<Vec<String>>,
    ) -> AuthorRow {
        author_at(did, followers, active, labels, 0)
    }

    /// Item 8: `author`'s own builder, extended with `checked_at` (append,
    /// never insert before an existing positional parameter) so the guard
    /// tests that need a specific cache age no longer hand-write an
    /// `AuthorRow` literal.
    fn author_at(
        did: &str,
        followers: Option<i64>,
        active: bool,
        labels: Option<Vec<String>>,
        checked_at: i64,
    ) -> AuthorRow {
        AuthorRow { did: did.to_string(), followers, active, labels, checked_at }
    }

    // BC1: below the floor drops.
    #[test]
    fn follower_floor_drops_below_threshold() {
        let author_o = author("did:plc:o", Some(1_999), true, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &[]));
        assert_eq!(result, GuardResult::Drop(DropReason::FollowerFloor));
    }

    // BC2: exactly at the floor passes. `<`, not `<=`.
    #[test]
    fn follower_floor_passes_at_threshold() {
        let author_o = author("did:plc:o", Some(2_000), true, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &[]));
        assert_eq!(result, GuardResult::Pass);
    }

    // BC3: a floor of 0 always passes, even far below any real count.
    #[test]
    fn follower_floor_of_zero_always_passes() {
        let author_o = author("did:plc:o", Some(0), true, None);
        let result = decide(&pair(), &author_o, None, &cfg(0, &[]));
        assert_eq!(result, GuardResult::Pass);
    }

    // BC4: an unknown follower count passes rather than counting as below.
    #[test]
    fn follower_floor_unknown_count_passes() {
        let author_o = author("did:plc:o", None, true, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &[]));
        assert_eq!(result, GuardResult::Pass);
    }

    // BC5 / BC6 rely on `author_row_from_profile` to set `active = false`;
    // `decide` itself only reads `active`. This proves the state axis: an
    // inactive `O` drops regardless of a healthy follower count.
    #[test]
    fn author_state_drops_inactive_original_author() {
        let author_o = author("did:plc:o", Some(1_000_000), false, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &[]));
        assert_eq!(result, GuardResult::Drop(DropReason::AuthorInactive));
    }

    // BC7: `Q`'s author being inactive does not matter; only `O`'s does.
    #[test]
    fn author_state_ignores_inactive_quote_author() {
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let author_q = author("did:plc:q", Some(1_000_000), false, None);
        let result = decide(&pair(), &author_o, Some(&author_q), &cfg(2_000, &[]));
        assert_eq!(result, GuardResult::Pass);
    }

    // BC9: a label on `Q`'s own `postView` in `DUNK_DROP_LABELS` drops.
    #[test]
    fn labels_drop_on_quote_post_label() {
        let mut p = pair();
        p.labels_q = vec!["porn".to_string()];
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]));
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC10: same, for `O`'s own `postView` label.
    #[test]
    fn labels_drop_on_original_post_label() {
        let mut p = pair();
        p.labels_o = vec!["porn".to_string()];
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]));
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC11: a label on `Q`'s author's `profileView` drops.
    #[test]
    fn labels_drop_on_quote_authors_profile_label() {
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let author_q = author("did:plc:q", Some(1_000_000), true, Some(vec!["spam".to_string()]));
        let result = decide(&pair(), &author_o, Some(&author_q), &cfg(2_000, &["spam"]));
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC12: same, for `O`'s author's `profileView` label.
    #[test]
    fn labels_drop_on_original_authors_profile_label() {
        let author_o = author("did:plc:o", Some(1_000_000), true, Some(vec!["spam".to_string()]));
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &["spam"]));
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC13: a label present but not in `DUNK_DROP_LABELS` passes. Matching
    // is exact and case sensitive.
    #[test]
    fn labels_not_in_drop_list_pass() {
        let mut p = pair();
        p.labels_q = vec!["Porn".to_string()]; // Different case from the configured "porn".
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]));
        assert_eq!(result, GuardResult::Pass);
    }

    // BC14: `author_q` absent simply narrows the label check to the other
    // three sources; it is not an error and does not itself drop.
    #[test]
    fn labels_author_q_absent_is_not_checked() {
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &["porn"]));
        assert_eq!(result, GuardResult::Pass);
    }

    // BC15: author state wins over labels, and labels win over the floor,
    // when more than one guard would fire.
    #[test]
    fn guard_order_state_then_labels_then_floor() {
        let mut p = pair();
        p.labels_o = vec!["porn".to_string()];
        // Inactive, labelled, and below the floor all at once.
        let author_o = author("did:plc:o", Some(1), false, None);
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]));
        assert_eq!(result, GuardResult::Drop(DropReason::AuthorInactive));

        // Active but still labelled and below the floor: labels win.
        let author_o = author("did:plc:o", Some(1), true, None);
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]));
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC41, story 10's correction round: the follower floor drops whenever
    // `follower_floor > 0`. There is no suppression window on `decide` any
    // more — the histogram period only decides whether `one_pass` also logs
    // a distribution line, in `check_batch` and `log_only_window` below.
    #[test]
    fn follower_floor_is_always_live() {
        let author_o = author("did:plc:o", Some(1), true, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &[]));
        assert_eq!(result, GuardResult::Drop(DropReason::FollowerFloor));
    }

    // BC5, BC6: `author_row_from_profile` marks a missing profile or a
    // `!takedown` label inactive; every other label leaves it active.
    #[test]
    fn author_state() {
        let now = 1_700_000_000;
        let missing = author_row_from_profile(None, "did:plc:missing", now);
        assert!(!missing.active);
        assert_eq!(missing.followers, None);
        assert_eq!(missing.checked_at, now);

        let taken_down = ProfileView {
            did: "did:plc:taken-down".to_string(),
            followers_count: 500,
            labels: vec![crate::appview::types::Label { val: "!takedown".to_string() }],
        };
        let row = author_row_from_profile(Some(&taken_down), "did:plc:taken-down", now);
        assert!(!row.active);
        assert_eq!(row.followers, Some(500));

        let healthy = ProfileView {
            did: "did:plc:healthy".to_string(),
            followers_count: 500,
            labels: vec![crate::appview::types::Label { val: "spam".to_string() }],
        };
        let row = author_row_from_profile(Some(&healthy), "did:plc:healthy", now);
        assert!(row.active);
        assert_eq!(row.labels, Some(vec!["spam".to_string()]));
    }

    // Groups the follower-floor behaviour contracts under one name, matching
    // `spec.md`'s AC1 test path (`scorer::guards::tests::follower_floor`).
    #[test]
    fn follower_floor() {
        follower_floor_drops_below_threshold();
        follower_floor_passes_at_threshold();
        follower_floor_of_zero_always_passes();
        follower_floor_unknown_count_passes();
    }

    // AC3's test path (`scorer::guards::tests::labels`).
    #[test]
    fn labels() {
        labels_drop_on_quote_post_label();
        labels_drop_on_original_post_label();
        labels_drop_on_quote_authors_profile_label();
        labels_drop_on_original_authors_profile_label();
        labels_not_in_drop_list_pass();
        labels_author_q_absent_is_not_checked();
    }

    // BC30, BC31: the six buckets and their boundaries, and `None` is
    // simply never recorded.
    #[test]
    fn histogram_buckets_boundaries() {
        let mut h = FollowerHistogram::default();
        h.record(0);
        h.record(1);
        h.record(99);
        h.record(100);
        h.record(999);
        h.record(1_000);
        h.record(9_999);
        h.record(10_000);
        h.record(99_999);
        h.record(100_000);
        assert_eq!(h.zero, 1);
        assert_eq!(h.one_to_99, 2);
        assert_eq!(h.hundred_to_999, 2);
        assert_eq!(h.thousand_to_9999, 2);
        assert_eq!(h.ten_k_to_99999, 2);
        assert_eq!(h.hundred_k_plus, 1);
    }

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::appview::{AppViewClient, ProfilesOutcome};
    use crate::store::Store;

    /// A fake `ProfileSource` over a fixed map of profiles, the
    /// `ProfileSource` counterpart of `FakeSource` in
    /// `src/scorer/mod.rs`'s tests. `failing` makes every chunk containing
    /// one of its DIDs return in `failed_dids` instead (BC21), standing in
    /// for a `getProfiles` call that exhausted its retries.
    #[derive(Clone, Default)]
    struct FakeProfiles {
        profiles: Arc<HashMap<String, ProfileView>>,
        fail_dids: Arc<HashSet<String>>,
        calls: Arc<AtomicUsize>,
    }

    impl FakeProfiles {
        fn new(profiles: Vec<ProfileView>) -> Self {
            FakeProfiles {
                profiles: Arc::new(profiles.into_iter().map(|p| (p.did.clone(), p)).collect()),
                fail_dids: Arc::new(HashSet::new()),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn failing(mut self, dids: &[&str]) -> Self {
            self.fail_dids = Arc::new(dids.iter().map(|s| s.to_string()).collect());
            self
        }
    }

    impl ProfileSource for FakeProfiles {
        fn get_profiles_lenient(
            &self,
            dids: &[String],
        ) -> impl std::future::Future<Output = ProfilesOutcome> + Send {
            let mut outcome = ProfilesOutcome::default();
            for chunk in dids.chunks(AppViewClient::PROFILES_BATCH) {
                self.calls.fetch_add(1, Ordering::SeqCst);
                outcome.calls += 1;
                let hit_fail = chunk.iter().any(|d| self.fail_dids.contains(d));
                if hit_fail {
                    outcome.failed_dids.extend(chunk.iter().cloned());
                } else {
                    outcome.profiles.extend(
                        chunk
                            .iter()
                            .filter_map(|d| self.profiles.get(d).cloned().map(|p| (d.clone(), p))),
                    );
                }
            }
            async move { outcome }
        }
    }

    // BC16: every needed DID has a fresh `authors` row, so `check_batch`
    // makes zero `getProfiles` calls.
    #[tokio::test]
    async fn cache_fresh_row_skips_the_fetch() {
        let store = Store::open_memory().unwrap();
        let now = 1_700_100_000;
        for did in ["did:plc:o", "did:plc:q"] {
            // fresh: `now - checked_at == 0 < ttl`
            store.author_put(&author_at(did, Some(5_000), true, None, now)).unwrap();
        }

        let profiles = FakeProfiles::new(vec![]);
        let guard_cfg = cfg(2_000, &[]);
        let mut counters = PassCounters::default();
        let results =
            check_batch(&store, &profiles, &guard_cfg, &[pair()], now, false, &mut counters)
                .await
                .unwrap();

        assert_eq!(counters.profile_calls, 0);
        assert_eq!(results, vec![GuardResult::Pass]);
    }

    // BC17: a stale row (and a missing one) join the fetch list, in one
    // chunk, so `check_batch` makes exactly one `getProfiles` call.
    #[tokio::test]
    async fn cache_stale_row_causes_exactly_one_call() {
        let store = Store::open_memory().unwrap();
        let now = 1_700_100_000;
        // past the 24h default TTL
        store
            .author_put(&author_at("did:plc:o", Some(5_000), true, None, now - 25 * 3600))
            .unwrap();
        // "did:plc:q" (pair()'s quote_did) has no row at all: missing joins
        // the same fetch list as stale.

        let profiles = FakeProfiles::new(vec![
            ProfileView { did: "did:plc:o".to_string(), followers_count: 5_000, labels: vec![] },
            ProfileView { did: "did:plc:q".to_string(), followers_count: 5_000, labels: vec![] },
        ]);
        let guard_cfg = cfg(2_000, &[]);
        let mut counters = PassCounters::default();
        check_batch(&store, &profiles, &guard_cfg, &[pair()], now, false, &mut counters)
            .await
            .unwrap();

        assert_eq!(counters.profile_calls, 1);
    }

    // BC18: 30 distinct DIDs need fetching, chunked at
    // `AppViewClient::PROFILES_BATCH` (25), so two `getProfiles` calls.
    #[tokio::test]
    async fn cache_thirty_dids_causes_two_calls() {
        let store = Store::open_memory().unwrap();
        let now = 1_700_100_000;
        let pairs: Vec<VerifiedPair> = (0..15)
            .map(|i| {
                let mut p = pair();
                p.quote_uri = format!("at://did:plc:q{i}/app.bsky.feed.post/q");
                p.quote_did = format!("did:plc:q{i}");
                p.original_uri = format!("at://did:plc:o{i}/app.bsky.feed.post/o");
                p.original_did = format!("did:plc:o{i}");
                p
            })
            .collect();

        let profiles = FakeProfiles::new(vec![]);
        let guard_cfg = cfg(2_000, &[]);
        let mut counters = PassCounters::default();
        check_batch(&store, &profiles, &guard_cfg, &pairs, now, false, &mut counters)
            .await
            .unwrap();

        assert_eq!(counters.profile_calls, 2);
    }

    // AC4's test path (`scorer::guards::tests::cache`) is satisfied by
    // substring match against the three `cache_*` tests above: `cargo test`
    // filters by substring, and each one's fully qualified name contains
    // `scorer::guards::tests::cache`. Unlike the synchronous BC groups above
    // (`follower_floor`, `labels`), an explicit wrapper here cannot simply
    // call each `#[tokio::test]` fn and `.await` it: that attribute turns
    // the fn into its own sync entry point, not a plain `async fn`.

    // BC34: an empty `pairs` makes no store call and no `getProfiles` call.
    #[tokio::test]
    async fn check_batch_of_empty_pairs_makes_no_calls() {
        let store = Store::open_memory().unwrap();
        let profiles = FakeProfiles::new(vec![]);
        let guard_cfg = cfg(2_000, &[]);
        let mut counters = PassCounters::default();
        let results =
            check_batch(&store, &profiles, &guard_cfg, &[], 1_700_100_000, false, &mut counters)
                .await
                .unwrap();

        assert!(results.is_empty());
        assert_eq!(counters.profile_calls, 0);
        assert_eq!(profiles.calls.load(Ordering::SeqCst), 0);
    }

    // BC35: the same DID on many pairs is fetched once, not once per pair.
    #[tokio::test]
    async fn check_batch_dedupes_shared_dids_across_pairs() {
        let store = Store::open_memory().unwrap();
        let now = 1_700_100_000;
        let mut first = pair();
        first.quote_uri = "at://did:plc:q/app.bsky.feed.post/1".to_string();
        let mut second = pair();
        second.quote_uri = "at://did:plc:q/app.bsky.feed.post/2".to_string();
        // Both share the same quote_did and original_did as `pair()`.

        let profiles = FakeProfiles::new(vec![
            ProfileView { did: "did:plc:o".to_string(), followers_count: 5_000, labels: vec![] },
            ProfileView { did: "did:plc:q".to_string(), followers_count: 5_000, labels: vec![] },
        ]);
        let guard_cfg = cfg(2_000, &[]);
        let mut counters = PassCounters::default();
        check_batch(&store, &profiles, &guard_cfg, &[first, second], now, false, &mut counters)
            .await
            .unwrap();

        assert_eq!(profiles.calls.load(Ordering::SeqCst), 1, "one getProfiles call for both pairs");
    }

    // BC21, BC36: a pair naming a DID whose chunk failed defers, and every
    // result lands at its input's position. `healthy`'s two DIDs are
    // pre-seeded as fresh cache rows so they never share `failing_pair`'s
    // chunk: a real `getProfiles` chunk fails wholesale (BC18), so this
    // proves the per-pair Defer, not just the per-chunk failure.
    #[tokio::test]
    async fn check_batch_defers_pairs_whose_profile_fetch_failed() {
        let store = Store::open_memory().unwrap();
        let now = 1_700_100_000;
        let mut healthy = pair();
        healthy.quote_uri = "at://did:plc:q/app.bsky.feed.post/healthy".to_string();
        healthy.quote_did = "did:plc:healthy-q".to_string();
        healthy.original_did = "did:plc:healthy-o".to_string();
        for did in ["did:plc:healthy-q", "did:plc:healthy-o"] {
            // fresh: never joins the fetch list
            store.author_put(&author_at(did, Some(5_000), true, None, now)).unwrap();
        }

        let mut failing_pair = pair();
        failing_pair.quote_uri = "at://did:plc:q/app.bsky.feed.post/failing".to_string();
        failing_pair.quote_did = "did:plc:failing-q".to_string();
        failing_pair.original_did = "did:plc:failing-o".to_string();

        let profiles = FakeProfiles::new(vec![]).failing(&["did:plc:failing-o"]);
        let guard_cfg = cfg(2_000, &[]);
        let mut counters = PassCounters::default();
        let results = check_batch(
            &store,
            &profiles,
            &guard_cfg,
            &[healthy, failing_pair],
            now,
            false,
            &mut counters,
        )
        .await
        .unwrap();

        assert_eq!(results, vec![GuardResult::Pass, GuardResult::Defer]);
        assert_eq!(counters.deferred, 1);
    }

    // BC26, BC27 (revised), BC29 to BC31: with the histogram period open, a
    // pair below the floor still drops — the period only decides whether the
    // line is logged, not the guard's own outcome any more — and its `O`
    // follower count still lands in the histogram, and `guard_would_drop`
    // counts the actual drop.
    #[tokio::test]
    async fn check_batch_histogram_open_still_drops_and_counts() {
        let store = Store::open_memory().unwrap();
        let now = 1_700_100_000;
        store.author_put(&author_at("did:plc:o", Some(1), true, None, now)).unwrap();
        store.author_put(&author_at("did:plc:q", Some(1), true, None, now)).unwrap();

        let profiles = FakeProfiles::new(vec![]);
        let guard_cfg = cfg(2_000, &[]);
        let mut counters = PassCounters::default();
        let results =
            check_batch(&store, &profiles, &guard_cfg, &[pair()], now, true, &mut counters)
                .await
                .unwrap();

        assert_eq!(results, vec![GuardResult::Drop(DropReason::FollowerFloor)]);
        assert_eq!(counters.guard_would_drop, 1);
        assert_eq!(counters.guard_histogram.one_to_99, 1);
    }

    // BC31a, story 10's correction round: the same `O` author DID on several
    // pairs of one pass is sampled into the histogram once, not once per
    // pair.
    #[tokio::test]
    async fn check_batch_histogram_samples_distinct_o_dids_once() {
        let store = Store::open_memory().unwrap();
        let now = 1_700_100_000;
        store.author_put(&author_at("did:plc:o", Some(1), true, None, now)).unwrap();
        store.author_put(&author_at("did:plc:q1", Some(5_000), true, None, now)).unwrap();
        store.author_put(&author_at("did:plc:q2", Some(5_000), true, None, now)).unwrap();

        let mut first = pair();
        first.quote_uri = "at://did:plc:q1/app.bsky.feed.post/1".to_string();
        first.quote_did = "did:plc:q1".to_string();
        let mut second = pair();
        second.quote_uri = "at://did:plc:q2/app.bsky.feed.post/2".to_string();
        second.quote_did = "did:plc:q2".to_string();
        // Both share the same original_did, "did:plc:o".

        let profiles = FakeProfiles::new(vec![]);
        let guard_cfg = cfg(2_000, &[]);
        let mut counters = PassCounters::default();
        check_batch(&store, &profiles, &guard_cfg, &[first, second], now, true, &mut counters)
            .await
            .unwrap();

        assert_eq!(counters.guard_histogram.one_to_99, 1, "did:plc:o counted once, not twice");
    }

    // BC23: `DUNK_GUARD_HISTOGRAM_H = 0` disables the period and writes
    // nothing to `meta`.
    #[tokio::test]
    async fn log_only_window_meta_clock_disabled_writes_nothing() {
        let store = Store::open_memory().unwrap();
        let mut guard_cfg = cfg(2_000, &[]);
        guard_cfg.guard_histogram_h = 0;

        let open = log_only_window(&store, &guard_cfg, 1_700_000_000).await.unwrap();

        assert!(!open);
        assert_eq!(store.meta_get("guard_histogram_since").unwrap(), None);
        assert_eq!(store.meta_get("guard_histogram_floor").unwrap(), None);
    }

    // BC24: an absent clock is written as `now`, `guard_histogram_floor` is
    // written as the current floor, and the period is open.
    #[tokio::test]
    async fn log_only_window_meta_clock_absent_opens_and_writes_now() {
        let store = Store::open_memory().unwrap();
        let guard_cfg = cfg(2_000, &[]); // guard_histogram_h: 24
        let now = 1_700_000_000;

        let open = log_only_window(&store, &guard_cfg, now).await.unwrap();

        assert!(open);
        assert_eq!(store.meta_get("guard_histogram_since").unwrap(), Some(now.to_string()));
        assert_eq!(store.meta_get("guard_histogram_floor").unwrap(), Some("2000".to_string()));
    }

    // BC25, BC27: a present clock, with `guard_histogram_floor` matching the
    // current floor, is never overwritten, and a restart deep inside the
    // period still reads it as open; past the period, the clock is still
    // untouched but the period reads as elapsed.
    #[tokio::test]
    async fn log_only_window_meta_clock_present_is_never_overwritten() {
        let store = Store::open_memory().unwrap();
        let guard_cfg = cfg(2_000, &[]); // guard_histogram_h: 24, follower_floor: 2000
        let opened_at = 1_700_000_000;
        store.meta_set("guard_histogram_since", &opened_at.to_string()).unwrap();
        store.meta_set("guard_histogram_floor", "2000").unwrap();

        let still_inside = log_only_window(&store, &guard_cfg, opened_at + 3_600).await.unwrap();
        assert!(still_inside);
        assert_eq!(
            store.meta_get("guard_histogram_since").unwrap(),
            Some(opened_at.to_string()),
            "a restart inside the period does not reset the clock"
        );

        let past = log_only_window(&store, &guard_cfg, opened_at + 25 * 3_600).await.unwrap();
        assert!(!past);
        assert_eq!(
            store.meta_get("guard_histogram_since").unwrap(),
            Some(opened_at.to_string()),
            "the clock itself is still untouched past the period"
        );
    }

    // BC26, BC27: the boundary itself is exclusive on the elapsed side:
    // exactly `guard_histogram_h` hours since `since` reads as elapsed, not
    // open.
    #[tokio::test]
    async fn log_only_window_boundary_at_exact_equality_is_elapsed() {
        let store = Store::open_memory().unwrap();
        let guard_cfg = cfg(2_000, &[]); // guard_histogram_h: 24
        let opened_at = 1_700_000_000;
        store.meta_set("guard_histogram_since", &opened_at.to_string()).unwrap();
        store.meta_set("guard_histogram_floor", "2000").unwrap();

        let at_boundary =
            log_only_window(&store, &guard_cfg, opened_at + 24 * 3_600).await.unwrap();
        assert!(!at_boundary, "now - since == the period length reads as elapsed, not open");

        let just_inside =
            log_only_window(&store, &guard_cfg, opened_at + 24 * 3_600 - 1).await.unwrap();
        assert!(just_inside);
    }

    // BC42, BC43, story 10's correction round: a stored floor that no longer
    // matches `DUNK_FOLLOWER_FLOOR` resets `since` to `now` and stores the
    // current floor, reopening the period even though it had already
    // elapsed.
    #[tokio::test]
    async fn log_only_window_meta_floor_change_resets_since() {
        let store = Store::open_memory().unwrap();
        let guard_cfg = cfg(2_000, &[]); // follower_floor: 2000
        let opened_at = 1_700_000_000;
        store.meta_set("guard_histogram_since", &opened_at.to_string()).unwrap();
        store.meta_set("guard_histogram_floor", "500").unwrap(); // a different floor

        let now = opened_at + 25 * 3_600; // past the old period, if it hadn't reset
        let open = log_only_window(&store, &guard_cfg, now).await.unwrap();

        assert!(open, "a floor change reopens the period even past the old window");
        assert_eq!(store.meta_get("guard_histogram_since").unwrap(), Some(now.to_string()));
        assert_eq!(store.meta_get("guard_histogram_floor").unwrap(), Some("2000".to_string()));
    }

    // BC28: a clock that does not parse as `i64` is treated as elapsed and
    // left alone, so it can never reopen the period. The floor is unchanged
    // here, so this exercises the malformed-since branch specifically, not
    // the floor-change branch.
    #[tokio::test]
    async fn log_only_window_meta_clock_malformed_is_elapsed_and_left_alone() {
        let store = Store::open_memory().unwrap();
        let guard_cfg = cfg(2_000, &[]);
        store.meta_set("guard_histogram_since", "not-a-number").unwrap();
        store.meta_set("guard_histogram_floor", "2000").unwrap();

        let open = log_only_window(&store, &guard_cfg, 1_700_000_000).await.unwrap();

        assert!(!open);
        assert_eq!(
            store.meta_get("guard_histogram_since").unwrap(),
            Some("not-a-number".to_string())
        );
    }

    // AC5's test path (`scorer::guards::tests::log_only_window`) is
    // satisfied by substring match against the `log_only_window_meta_clock_*`
    // and `log_only_window_boundary_*`/`log_only_window_meta_floor_*` tests
    // above and `check_batch_histogram_open_still_drops_and_counts`, for the
    // same reason noted above `cache_thirty_dids_causes_two_calls`.

    // AC7: a live `getProfiles` call for the reference pair's two DIDs
    // (`scorer::tests::scorer_live_pass`'s `did:plc:o7xt7svg2xtjbb4e2xqahqqc`
    // and `did:plc:ofzkhjyyh4kl4a35wxgmobmm`) plus two well-known accounts,
    // printing each DID's real follower count and the resulting histogram
    // bucket so an operator can eyeball a real distribution before setting
    // `DUNK_FOLLOWER_FLOOR`. `#[ignore]`d, per `AGENTS.md`: run by hand with
    // `cargo test --all-features -- --ignored
    // guards_live_follower_distribution`.
    #[tokio::test]
    #[ignore]
    async fn guards_live_follower_distribution() {
        let config = crate::config::load(|name| match name {
            "DUNK_HOSTNAME" => Some("feed.example.com".to_string()),
            "DUNK_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            _ => None,
        })
        .unwrap();
        let client =
            AppViewClient::new(&config).expect("cfg.appview_rps is validated by config::load");

        let dids: Vec<String> = vec![
            "did:plc:o7xt7svg2xtjbb4e2xqahqqc".to_string(), // reference pair's Q author
            "did:plc:ofzkhjyyh4kl4a35wxgmobmm".to_string(), // reference pair's O author
            "did:plc:z72i7hdynmk6r22z27h6tvur".to_string(), // bsky.app
            "did:plc:plcl43vt7d2ig7hif4zmyg6h".to_string(), // well-known account
        ];

        let outcome = client.get_profiles_lenient(&dids).await;
        assert!(
            outcome.failed_dids.is_empty(),
            "every DID should resolve against the real App View"
        );

        let mut histogram = FollowerHistogram::default();
        for did in &dids {
            let followers = outcome.profiles.get(did).map(|p| i64::from(p.followers_count));
            println!("did={did} followers={followers:?}");
            if let Some(count) = followers {
                histogram.record(count);
            }
        }
        println!("{histogram:?}");
    }
}
