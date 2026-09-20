//! The safety guards, TECH-DESIGN section 9: follower floor, author state
//! and labels. Slice 2.0 adds the pure pieces — `GuardConfig`,
//! `GuardResult::Defer`, `decide`, `author_row_from_profile` and
//! `FollowerHistogram` — and leaves `check` in place as `verify_and_apply`'s
//! guard call. Slice 3.0 replaces `check` with `check_batch`, the batch step
//! that reaches the `authors` cache and the App View and then calls `decide`
//! once per pair.

use crate::appview::types::ProfileView;
use crate::config::Config;
use crate::scorer::verify::VerifiedPair;
use crate::store::authors::AuthorRow;
use crate::store::DropReason;

/// The result of running every guard over one verified pair. `Drop` carries
/// the reason `Store::drop_pair` writes, the same as a hard check inside
/// `verify_pair`. `Defer` is slice 3.0's fourth variant: a pair whose `O` or
/// `Q` DID named a `getProfiles` chunk that failed after its retries is left
/// undecided rather than dropped or promoted, so the next pass retries it
/// (BC21).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardResult {
    Pass,
    // Constructed by `decide` (this slice) and, once wired in, by
    // `check_batch` (slice 3.0). `decide` itself has no live caller yet, so
    // this variant still needs its own attribute rather than relying on a
    // blanket module attribute.
    #[allow(dead_code)]
    Drop(DropReason),
    // No constructor yet; slice 3.0's `check_batch`, on a failed
    // `getProfiles` chunk, is the first.
    #[allow(dead_code)]
    Defer,
}

/// Always `Pass`. `verify_and_apply`'s guard call until slice 3.0 replaces
/// it with `check_batch`.
pub fn check(_pair: &VerifiedPair) -> GuardResult {
    GuardResult::Pass
}

/// The guard's own view of `Config`, TECH-DESIGN section 9 and section 4:
/// the follower floor, the drop-label list, the `authors` cache TTL and the
/// log-only window's length. Kept separate from `Config` itself so `decide`
/// and `check_batch` (slice 3.0) take one small, guard-specific value
/// instead of the whole binary's configuration.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)] // No caller yet; slice 3.0's `check_batch` is the first.
pub struct GuardConfig {
    pub follower_floor: u32,
    pub drop_labels: Vec<String>,
    pub author_ttl_h: u32,
    pub guard_log_only_h: u32,
}

impl From<&Config> for GuardConfig {
    fn from(cfg: &Config) -> Self {
        GuardConfig {
            follower_floor: cfg.follower_floor,
            drop_labels: cfg.drop_labels.clone(),
            author_ttl_h: cfg.author_ttl_h,
            guard_log_only_h: cfg.guard_log_only_h,
        }
    }
}

/// Whether any of `labels` is in `cfg.drop_labels`, exact and case sensitive
/// (BC13): the values the App View sends.
#[allow(dead_code)] // No caller yet; `decide` (below) is the first, and it has none yet either.
fn any_dropped(labels: &[String], cfg: &GuardConfig) -> bool {
    labels.iter().any(|label| cfg.drop_labels.iter().any(|dropped| dropped == label))
}

/// Decides one verified pair, pure: no store, no network. `author_o` is
/// `O`'s cache row, always present by the time a pair reaches `decide` — a
/// missing or stale row is fetched by `check_batch` (slice 3.0) before this
/// runs, and a pair whose fetch failed never reaches `decide` at all
/// (`GuardResult::Defer`, BC21). `author_q` is `Q`'s row when available
/// (BC14): its absence only narrows the label check, since the follower
/// floor and the state check are scoped to `O`'s author alone (TECH-DESIGN
/// section 9, BC7). `log_only` is the log-only window's current state
/// (`log_only_window`, slice 3.0): only the follower floor, the last and
/// only conditional check, reads it (BC15, BC26, BC27).
///
/// Order, BC15: author state, then labels, then the follower floor. A
/// `!takedown` label that also appears in `DUNK_DROP_LABELS` therefore
/// reports `author_inactive`, not `labelled`.
#[allow(dead_code)] // No caller yet; slice 3.0's `check_batch` is the first.
pub fn decide(
    pair: &VerifiedPair,
    author_o: &AuthorRow,
    author_q: Option<&AuthorRow>,
    cfg: &GuardConfig,
    log_only: bool,
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

    // Follower floor, BC1 to BC4: the last, and only conditional, check.
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
        if log_only {
            return GuardResult::Pass; // BC26: suppressed inside the window.
        }
        return GuardResult::Drop(DropReason::FollowerFloor); // BC1, BC27.
    }
    GuardResult::Pass // BC2: `<`, not `<=`.
}

/// Builds the `authors` cache row for `did` from its `getProfiles` result,
/// TECH-DESIGN section 9's author-state rule. `profile` is `None` when the
/// DID was simply absent from a successful response (BC5): the row is
/// written `active = false`, the same as a profile that carries a
/// `!takedown` label on itself (BC6). `now` becomes `checked_at`.
#[allow(dead_code)] // No caller yet; slice 3.0's `check_batch` is the first.
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
            let labels: Vec<String> =
                profile.labels.iter().map(|label| label.val.clone()).collect();
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
#[allow(dead_code)] // No caller yet; slice 3.0's `one_pass` is the first.
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
    #[allow(dead_code)] // No caller yet; slice 3.0's `check_batch` is the first.
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

    // BC40 (unchanged from the stub): `check` never inspects the pair.
    #[test]
    fn always_passes() {
        assert_eq!(check(&pair()), GuardResult::Pass);
    }

    fn cfg(follower_floor: u32, drop_labels: &[&str]) -> GuardConfig {
        GuardConfig {
            follower_floor,
            drop_labels: drop_labels.iter().map(|s| s.to_string()).collect(),
            author_ttl_h: 24,
            guard_log_only_h: 24,
        }
    }

    fn author(
        did: &str,
        followers: Option<i64>,
        active: bool,
        labels: Option<Vec<String>>,
    ) -> AuthorRow {
        AuthorRow { did: did.to_string(), followers, active, labels, checked_at: 0 }
    }

    // BC1: below the floor drops.
    #[test]
    fn follower_floor_drops_below_threshold() {
        let author_o = author("did:plc:o", Some(1_999), true, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &[]), false);
        assert_eq!(result, GuardResult::Drop(DropReason::FollowerFloor));
    }

    // BC2: exactly at the floor passes. `<`, not `<=`.
    #[test]
    fn follower_floor_passes_at_threshold() {
        let author_o = author("did:plc:o", Some(2_000), true, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &[]), false);
        assert_eq!(result, GuardResult::Pass);
    }

    // BC3: a floor of 0 always passes, even far below any real count.
    #[test]
    fn follower_floor_of_zero_always_passes() {
        let author_o = author("did:plc:o", Some(0), true, None);
        let result = decide(&pair(), &author_o, None, &cfg(0, &[]), false);
        assert_eq!(result, GuardResult::Pass);
    }

    // BC4: an unknown follower count passes rather than counting as below.
    #[test]
    fn follower_floor_unknown_count_passes() {
        let author_o = author("did:plc:o", None, true, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &[]), false);
        assert_eq!(result, GuardResult::Pass);
    }

    // BC5 / BC6 rely on `author_row_from_profile` to set `active = false`;
    // `decide` itself only reads `active`. This proves the state axis: an
    // inactive `O` drops regardless of a healthy follower count.
    #[test]
    fn author_state_drops_inactive_original_author() {
        let author_o = author("did:plc:o", Some(1_000_000), false, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &[]), false);
        assert_eq!(result, GuardResult::Drop(DropReason::AuthorInactive));
    }

    // BC7: `Q`'s author being inactive does not matter; only `O`'s does.
    #[test]
    fn author_state_ignores_inactive_quote_author() {
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let author_q = author("did:plc:q", Some(1_000_000), false, None);
        let result = decide(&pair(), &author_o, Some(&author_q), &cfg(2_000, &[]), false);
        assert_eq!(result, GuardResult::Pass);
    }

    // BC9: a label on `Q`'s own `postView` in `DUNK_DROP_LABELS` drops.
    #[test]
    fn labels_drop_on_quote_post_label() {
        let mut p = pair();
        p.labels_q = vec!["porn".to_string()];
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]), false);
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC10: same, for `O`'s own `postView` label.
    #[test]
    fn labels_drop_on_original_post_label() {
        let mut p = pair();
        p.labels_o = vec!["porn".to_string()];
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]), false);
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC11: a label on `Q`'s author's `profileView` drops.
    #[test]
    fn labels_drop_on_quote_authors_profile_label() {
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let author_q = author("did:plc:q", Some(1_000_000), true, Some(vec!["spam".to_string()]));
        let result = decide(&pair(), &author_o, Some(&author_q), &cfg(2_000, &["spam"]), false);
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC12: same, for `O`'s author's `profileView` label.
    #[test]
    fn labels_drop_on_original_authors_profile_label() {
        let author_o = author("did:plc:o", Some(1_000_000), true, Some(vec!["spam".to_string()]));
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &["spam"]), false);
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC13: a label present but not in `DUNK_DROP_LABELS` passes. Matching
    // is exact and case sensitive.
    #[test]
    fn labels_not_in_drop_list_pass() {
        let mut p = pair();
        p.labels_q = vec!["Porn".to_string()]; // Different case from the configured "porn".
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]), false);
        assert_eq!(result, GuardResult::Pass);
    }

    // BC14: `author_q` absent simply narrows the label check to the other
    // three sources; it is not an error and does not itself drop.
    #[test]
    fn labels_author_q_absent_is_not_checked() {
        let author_o = author("did:plc:o", Some(1_000_000), true, None);
        let result = decide(&pair(), &author_o, None, &cfg(2_000, &["porn"]), false);
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
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]), false);
        assert_eq!(result, GuardResult::Drop(DropReason::AuthorInactive));

        // Active but still labelled and below the floor: labels win.
        let author_o = author("did:plc:o", Some(1), true, None);
        let result = decide(&p, &author_o, None, &cfg(2_000, &["porn"]), false);
        assert_eq!(result, GuardResult::Drop(DropReason::Labelled));
    }

    // BC26 / BC27: inside the log-only window the floor passes what it
    // would otherwise drop; past it, the floor is live.
    #[test]
    fn log_only_window_suppresses_the_floor_only() {
        let author_o = author("did:plc:o", Some(1), true, None);
        let inside = decide(&pair(), &author_o, None, &cfg(2_000, &[]), true);
        assert_eq!(inside, GuardResult::Pass);
        let past = decide(&pair(), &author_o, None, &cfg(2_000, &[]), false);
        assert_eq!(past, GuardResult::Drop(DropReason::FollowerFloor));
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
}
