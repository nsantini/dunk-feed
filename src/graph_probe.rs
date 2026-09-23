//! `upstage graph-probe`, story 03 spec.md `## Outcome`: an operator-run,
//! read-only command that logs in with `BSKY_HANDLE`, reads the current
//! `feed` rows, runs the three first-build steps (`src/graph/build.rs`) for
//! each `--handle` in memory, and prints the numbers story 04 needs to
//! accept or reject the design. It writes nothing to SQLite (BC15) and never
//! starts from `upstage run`.
//!
//! Every number this module reports comes from a pure, independently
//! testable function — [`dedup_first_seen`], [`discovery_share_and_bytes_saved`],
//! [`circle_pairs_24h`], [`check_order`], [`time_filter_and_caps`] — so a
//! test checks numbers, not the text [`print_run_report`] writes (spec.md
//! `## Approach`).

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::appview::pds::{
    Credentials, HttpPdsTransport, ListFollowRecordsOutcome, PdsClient, PdsError,
};
use crate::config::Config;
use crate::graph::build::{step_degree2, step_follows, step_follows_me, RankedAuthors, StepStats};
use crate::graph::circle::Circle;
use crate::graph::filter::{connected_indices, FilterItem};
use crate::graph::{hash_did, DidHash};
use crate::publish::{self, PublishError};
use crate::score::Weights;
use crate::scorer::snapshot;
use crate::store::feed::FeedRow;
use crate::store::{Store, StoreError};

/// Every way `upstage graph-probe` can fail before or during a run.
/// `main.rs` prints this and exits 1 (through `CliError::GraphProbe`). No
/// variant carries a viewer DID (BC16): only handles, step names and
/// statuses.
#[derive(Debug, Error)]
pub enum GraphProbeError {
    #[error("missing or empty environment variable {var}")]
    MissingCredentials { var: &'static str },
    #[error("--handle is required: pass at least one --handle <handle>")]
    NoHandles,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("every handle failed; see the failure lines above")]
    AllFailed,
}

/// Keeps the first occurrence of each handle, dropping a later repeat
/// (BC9a): a handle given twice is probed once, in the order it first
/// appeared.
pub fn dedup_first_seen(handles: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(handles.len());
    for handle in handles {
        if seen.insert(handle.clone()) {
            out.push(handle.clone());
        }
    }
    out
}

/// Checks `--handle` (BC9, BC9a) before credentials (BC8), and both before
/// any network call or store open: `--handle` is not `required` in clap, so
/// an empty list reaches here rather than clap exiting 2 on its own
/// (spec.md "Defaults taken"). Reuses `publish::preflight`'s credential
/// check (`avatar_path: None`, so its avatar branch never runs) instead of
/// re-implementing the same blank/trim rule a second time.
pub fn preflight(
    cfg: &Config,
    handles: &[String],
) -> Result<(Vec<String>, Credentials), GraphProbeError> {
    let handles = dedup_first_seen(handles);
    if handles.is_empty() {
        return Err(GraphProbeError::NoHandles);
    }
    let credentials = match publish::preflight(cfg, None) {
        Ok((credentials, _avatar)) => credentials,
        Err(PublishError::MissingCredentials { var }) => {
            return Err(GraphProbeError::MissingCredentials { var })
        }
        Err(other) => {
            // `avatar_path` is `None`, so `publish::preflight`'s avatar
            // branch never runs; only `MissingCredentials` or `Ok` can
            // reach this call.
            unreachable!("preflight(cfg, None) only returns MissingCredentials or Ok: {other:?}")
        }
    };
    Ok((handles, credentials))
}

/// The share of the distinct degree-2 accounts named by two or more
/// handles' `d2_sample` (BC11), and the bytes those repeats save: for each
/// account named by more than one handle, `(handles naming it - 1) *` its
/// entry's byte size in `shared` (8 bytes per `DidHash`, matching
/// `Circle::heap_bytes`'s own per-hash accounting). Zero distinct accounts
/// gives a share of `0.0` without dividing by zero.
pub fn discovery_share_and_bytes_saved(
    d2_samples: &[Vec<String>],
    shared: &HashMap<String, Vec<DidHash>>,
) -> (f64, usize) {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for sample in d2_samples {
        let mut seen_in_this_handle: HashSet<&str> = HashSet::new();
        for account in sample {
            if seen_in_this_handle.insert(account.as_str()) {
                *counts.entry(account.as_str()).or_insert(0) += 1;
            }
        }
    }
    let distinct = counts.len();
    if distinct == 0 {
        return (0.0, 0);
    }
    let shared_accounts = counts.values().filter(|&&count| count >= 2).count();
    let share = shared_accounts as f64 / distinct as f64;
    let bytes_saved: usize = counts
        .iter()
        .filter(|(_, &count)| count >= 2)
        .map(|(account, &count)| {
            let entry_bytes = shared.get(*account).map(|dids| dids.len() * 8).unwrap_or(0);
            (count - 1) * entry_bytes
        })
        .sum();
    (share, bytes_saved)
}

/// Hashes a `feed` row's two author DIDs into a [`FilterItem`], the shape
/// `connected_indices` needs (spec.md `## Approach`, Rejected: no
/// `FeedItem` author hashes exist yet).
fn filter_item(row: &FeedRow) -> FilterItem {
    FilterItem { quote_did: hash_did(&row.quote_did), original_did: hash_did(&row.original_did) }
}

/// On the real `feed` rows, the count of kept items whose `promoted_at` is
/// within the last 24 hours, and the share of those that pass only through
/// degree 2 — connected through `d2_set`, and through neither `follows` nor
/// a `follows_me` match within `follows_me_depth` (BC13). Zero kept items in
/// that window gives a share of `0.0` without dividing by zero.
pub fn circle_pairs_24h(
    rows: &[FeedRow],
    circle: &Circle,
    d2_set: &HashSet<DidHash>,
    follows_me_depth: usize,
    now: i64,
) -> (usize, f64) {
    let items: Vec<FilterItem> = rows.iter().map(filter_item).collect();
    let kept = connected_indices(&items, circle, d2_set, follows_me_depth);

    let mut total_24h = 0usize;
    let mut degree2_only = 0usize;
    for index in kept {
        let index = index as usize;
        let row = &rows[index];
        if now - row.promoted_at > 24 * 3600 {
            continue;
        }
        total_24h += 1;
        let item = &items[index];
        let via_follows_or_follows_me = [item.quote_did, item.original_did].iter().any(|author| {
            circle.follows.contains(author)
                || (index < follows_me_depth && circle.follows_me.contains(author))
        });
        if !via_follows_or_follows_me {
            degree2_only += 1;
        }
    }
    let share = if total_24h == 0 { 0.0 } else { degree2_only as f64 / total_24h as f64 };
    (total_24h, share)
}

/// BC12: times `connected_indices` over 100,000 synthetic items built from
/// the real `feed` rows repeated in order, then times `scorer::snapshot::build`
/// (the `caps::apply` stand-in, spec.md "Defaults taken": `caps::apply` does
/// not exist until story 01 merges) on the rows the filter kept. `None` on
/// zero `feed` rows (BC12's skip case); the caller prints
/// `filter timing: skipped (no feed rows)` in that case.
pub fn time_filter_and_caps(
    rows: &[FeedRow],
    circle: &Circle,
    d2_set: &HashSet<DidHash>,
    follows_me_depth: usize,
    weights: &Weights,
    now: i64,
    k: f64,
) -> Option<(Duration, Duration)> {
    if rows.is_empty() {
        return None;
    }
    const SYNTHETIC_SIZE: usize = 100_000;
    let synthetic_rows: Vec<FeedRow> =
        (0..SYNTHETIC_SIZE).map(|i| rows[i % rows.len()].clone()).collect();
    let items: Vec<FilterItem> = synthetic_rows.iter().map(filter_item).collect();

    let filter_start = Instant::now();
    let kept = connected_indices(&items, circle, d2_set, follows_me_depth);
    let filter_elapsed = filter_start.elapsed();

    let kept_rows: Vec<FeedRow> =
        kept.into_iter().map(|index| synthetic_rows[index as usize].clone()).collect();
    let caps_start = Instant::now();
    let _ = snapshot::build(kept_rows, weights, now, k);
    let caps_elapsed = caps_start.elapsed();

    Some((filter_elapsed, caps_elapsed))
}

/// The order check's result (BC14): the first `getFollows` page matches the
/// newest `app.bsky.graph.follow` records position for position, the first
/// index where they differ, or the check was skipped, carrying the status
/// that caused the skip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderCheck {
    Match,
    Difference(usize),
    Skipped(u16),
}

/// Compares `follows_page` (the first `getFollows` page, in response order)
/// against `listed` (`list_follow_records`'s newest-first subjects) by
/// position (BC14). A length mismatch's first differing position is the
/// shorter list's length, since every earlier position matched.
pub fn check_order(follows_page: &[String], listed: &[String]) -> OrderCheck {
    let shared_len = follows_page.len().min(listed.len());
    for index in 0..shared_len {
        if follows_page[index] != listed[index] {
            return OrderCheck::Difference(index);
        }
    }
    if follows_page.len() != listed.len() {
        return OrderCheck::Difference(shared_len);
    }
    OrderCheck::Match
}

/// One handle's numbers (BC10, BC13, BC14): every field [`print_run_report`]
/// prints for that handle. Holds no viewer DID (BC16), only the handle and
/// numbers derived from it.
pub struct ProbeReport {
    pub handle: String,
    pub follows_stats: StepStats,
    pub follows_me_stats: StepStats,
    pub degree2_stats: StepStats,
    pub follows_len: usize,
    pub follows_me_len: usize,
    pub checked_len: usize,
    pub degree2_len: usize,
    pub circle_bytes: usize,
    pub order_check: OrderCheck,
    pub filter_timing: Option<(Duration, Duration)>,
    pub circle_pairs_24h: (usize, f64),
}

/// A per-handle failure (BC4a): the handle, the step it failed at, and the
/// error, never the viewer DID that step was working with (BC16).
pub struct HandleFailure {
    pub handle: String,
    pub step: &'static str,
    pub error: String,
}

/// The whole run's numbers: one [`ProbeReport`] for each handle that
/// completed, a [`HandleFailure`] for each that did not, and the
/// cross-handle discovery share and bytes saved (BC11).
pub struct RunReport {
    pub reports: Vec<ProbeReport>,
    pub failures: Vec<HandleFailure>,
    pub discovery_share: f64,
    pub discovery_bytes_saved: usize,
}

/// Runs the probe for every handle in `handles`: preflight (BC8, BC9, BC9a),
/// then a read-only store open and `feed_rows` read (BC15), then the three
/// build steps for each handle in turn, sharing one degree-2 map across all
/// handles in this run (spec.md "Defaults taken": one shared cache, design
/// §6.4). A handle whose build fails at any step is recorded as a
/// [`HandleFailure`] and the run continues with the next handle (BC4a); the
/// whole run fails only when every handle failed
/// ([`GraphProbeError::AllFailed`]).
pub async fn run(cfg: &Config, handles: &[String]) -> Result<RunReport, GraphProbeError> {
    let (handles, credentials) = preflight(cfg, handles)?;

    let store = Store::open_read_only(&cfg.db_path)?;
    let rows = store.feed_rows()?;
    let ranked: Vec<RankedAuthors> = rows
        .iter()
        .map(|row| RankedAuthors {
            quote_did: row.quote_did.clone(),
            original_did: row.original_did.clone(),
        })
        .collect();

    // `PdsClient::from_config` only fails on an out-of-range `graph_rps`,
    // already rejected at config load (`config::graph_rps_or_default`), so
    // a `Config` that reached `run` can never fail here.
    let client = match PdsClient::from_config(cfg, credentials) {
        Ok(client) => client,
        Err(err) => unreachable!("graph_rps is already validated at config load: {err}"),
    };

    let now = crate::store::unix_now();
    let weights = Weights::from(cfg);
    let k = f64::from(cfg.k);

    let mut shared_d2: HashMap<String, Vec<DidHash>> = HashMap::new();
    let mut reports = Vec::new();
    let mut failures = Vec::new();
    let mut d2_samples: Vec<Vec<String>> = Vec::new();

    for handle in &handles {
        match run_one_handle(&client, handle, &ranked, &rows, cfg, &weights, now, k, &mut shared_d2)
            .await
        {
            Ok((report, d2_sample)) => {
                d2_samples.push(d2_sample);
                reports.push(report);
            }
            Err(failure) => failures.push(failure),
        }
    }

    let (discovery_share, discovery_bytes_saved) =
        discovery_share_and_bytes_saved(&d2_samples, &shared_d2);

    if reports.is_empty() {
        return Err(GraphProbeError::AllFailed);
    }

    Ok(RunReport { reports, failures, discovery_share, discovery_bytes_saved })
}

/// One handle's full build and report (BC4a's per-step handling, BC10,
/// BC13, BC14): resolves the handle, runs the three steps against `shared`,
/// then computes the order check, filter timing and circle-pairs numbers.
/// Returns the [`HandleFailure`] for the first step that fails; `run`
/// records it and moves to the next handle.
#[allow(clippy::too_many_arguments)]
async fn run_one_handle(
    client: &PdsClient<HttpPdsTransport>,
    handle: &str,
    ranked: &[RankedAuthors],
    rows: &[FeedRow],
    cfg: &Config,
    weights: &Weights,
    now: i64,
    k: f64,
    shared_d2: &mut HashMap<String, Vec<DidHash>>,
) -> Result<(ProbeReport, Vec<String>), HandleFailure> {
    let fail = |step: &'static str, err: PdsError| HandleFailure {
        handle: handle.to_string(),
        step,
        error: err.to_string(),
    };

    let did = client.resolve_handle(handle).await.map_err(|err| fail("resolve_handle", err))?;

    let mut circle = Circle::new();
    let follows_stats = step_follows(client, &did, cfg.d2_follows_sample as usize, &mut circle)
        .await
        .map_err(|err| fail("step_follows", err))?;

    let follows_me_stats =
        step_follows_me(client, &did, ranked, cfg.follows_me_depth as usize, &mut circle)
            .await
            .map_err(|err| fail("step_follows_me", err))?;

    let degree2_stats = step_degree2(client, &circle.d2_sample, cfg.d2_follows_depth, shared_d2)
        .await
        .map_err(|err| fail("step_degree2", err))?;

    let d2_set: HashSet<DidHash> = circle
        .d2_sample
        .iter()
        .filter_map(|account| shared_d2.get(account))
        .flat_map(|dids| dids.iter().copied())
        .collect();

    let order_check = match client.get_follows(&did, 100, None).await {
        Ok(page) => match client.list_follow_records(&did, 100).await {
            Ok(ListFollowRecordsOutcome::Ok(listed)) => check_order(&page.dids, &listed),
            Ok(ListFollowRecordsOutcome::Failed(status)) => OrderCheck::Skipped(status),
            Err(_) => OrderCheck::Skipped(0),
        },
        Err(_) => OrderCheck::Skipped(0),
    };

    let filter_timing = time_filter_and_caps(
        rows,
        &circle,
        &d2_set,
        cfg.follows_me_depth as usize,
        weights,
        now,
        k,
    );
    let circle_pairs_24h =
        circle_pairs_24h(rows, &circle, &d2_set, cfg.follows_me_depth as usize, now);
    let d2_sample = circle.d2_sample.clone();

    Ok((
        ProbeReport {
            handle: handle.to_string(),
            follows_len: circle.follows.len(),
            follows_me_len: circle.follows_me.len(),
            checked_len: circle.checked.len(),
            degree2_len: d2_set.len(),
            circle_bytes: circle.heap_bytes(),
            follows_stats,
            follows_me_stats,
            degree2_stats,
            order_check,
            filter_timing,
            circle_pairs_24h,
        },
        d2_sample,
    ))
}

/// Prints `report`, one line per number (BC10, BC11, BC12, BC13, BC14),
/// naming only handles, step names and statuses (BC16) — never a viewer
/// DID. Kept separate from [`run`] so a test checks the numbers `run`
/// collects, not this function's text (spec.md `## Approach`).
pub fn print_run_report(report: &RunReport) {
    for failure in &report.failures {
        println!("{}: failed at {}: {}", failure.handle, failure.step, failure.error);
    }
    for probe in &report.reports {
        println!("{}:", probe.handle);
        println!(
            "  step_follows: {} calls, {} pages, {:?}",
            probe.follows_stats.calls, probe.follows_stats.pages, probe.follows_stats.elapsed
        );
        println!(
            "  step_follows_me: {} calls, {} pages, {:?}",
            probe.follows_me_stats.calls,
            probe.follows_me_stats.pages,
            probe.follows_me_stats.elapsed
        );
        println!(
            "  step_degree2: {} calls, {} pages, {:?}",
            probe.degree2_stats.calls, probe.degree2_stats.pages, probe.degree2_stats.elapsed
        );
        println!(
            "  follows={} follows_me={} checked={} degree2={}",
            probe.follows_len, probe.follows_me_len, probe.checked_len, probe.degree2_len
        );
        println!("  circle bytes: {}", probe.circle_bytes);
        match probe.order_check {
            OrderCheck::Match => println!("  order check: match"),
            OrderCheck::Difference(index) => println!("  order check: differs at position {index}"),
            OrderCheck::Skipped(status) => println!("  order check: skipped ({status})"),
        }
        match probe.filter_timing {
            Some((filter_elapsed, caps_elapsed)) => {
                println!("  filter timing: {filter_elapsed:?}");
                println!("  caps (snapshot::build, upper bound): {caps_elapsed:?}");
            }
            None => println!("  filter timing: skipped (no feed rows)"),
        }
        let (total_24h, share) = probe.circle_pairs_24h;
        println!("  circle pairs in 24h: {total_24h}, degree-2-only share: {share}");
    }
    println!(
        "discovery share: {}, bytes saved: {}",
        report.discovery_share, report.discovery_bytes_saved
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Secret;

    fn base_config() -> Config {
        let lookup = |name: &str| match name {
            "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
            "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            _ => None,
        };
        crate::config::load(lookup).expect("minimal config loads")
    }

    // --- dedup_first_seen ------------------------------------------------

    #[test]
    fn dedup_first_seen_keeps_first_occurrence_order() {
        let handles = vec![
            "a.bsky.social".to_string(),
            "b.bsky.social".to_string(),
            "a.bsky.social".to_string(),
        ];
        assert_eq!(dedup_first_seen(&handles), vec!["a.bsky.social", "b.bsky.social"]);
    }

    // --- preflight (BC8, BC9, BC9a) ---------------------------------------

    #[test]
    fn no_handle_fails_before_credentials() {
        // BC9: checked before credentials, even when credentials are also
        // missing.
        let cfg = base_config();
        let err = preflight(&cfg, &[]).unwrap_err();
        assert!(matches!(err, GraphProbeError::NoHandles));
    }

    #[test]
    fn missing_credentials_fails_with_a_handle_given() {
        // BC8.
        let cfg = base_config();
        let handles = vec!["someone.bsky.social".to_string()];
        let err = preflight(&cfg, &handles).unwrap_err();
        match err {
            GraphProbeError::MissingCredentials { var } => assert_eq!(var, "BSKY_HANDLE"),
            other => panic!("expected MissingCredentials, got {other:?}"),
        }
    }

    #[test]
    fn preflight_dedups_and_returns_credentials() {
        let lookup = |name: &str| match name {
            "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
            "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "BSKY_HANDLE" => Some("upstage.bsky.social".to_string()),
            "BSKY_APP_PASSWORD" => Some("secret".to_string()),
            _ => None,
        };
        let cfg = crate::config::load(lookup).expect("config loads");
        let handles = vec![
            "a.bsky.social".to_string(),
            "a.bsky.social".to_string(),
            "b.bsky.social".to_string(),
        ];

        let (deduped, credentials) = preflight(&cfg, &handles).expect("preflight succeeds");

        assert_eq!(deduped, vec!["a.bsky.social", "b.bsky.social"]);
        assert_eq!(credentials.handle, "upstage.bsky.social");
        assert_eq!(credentials.app_password, Secret::new("secret".to_string()));
    }

    // --- report_numbers (AC3, BC11, BC13) ---------------------------------

    #[test]
    fn report_numbers() {
        // BC11: two handles share two accounts out of three distinct ones;
        // the shared accounts' saved bytes are (2 - 1) * entry bytes each.
        let mut shared: HashMap<String, Vec<DidHash>> = HashMap::new();
        shared.insert("did:plc:a".to_string(), vec![1, 2, 3, 4]); // 32 bytes
        shared.insert("did:plc:b".to_string(), vec![1, 2]); // 16 bytes
        shared.insert("did:plc:c".to_string(), vec![1]); // 8 bytes, not shared

        let d2_samples = vec![
            vec!["did:plc:a".to_string(), "did:plc:b".to_string()],
            vec!["did:plc:a".to_string(), "did:plc:b".to_string(), "did:plc:c".to_string()],
        ];

        let (share, bytes_saved) = discovery_share_and_bytes_saved(&d2_samples, &shared);

        assert_eq!(share, 2.0 / 3.0);
        assert_eq!(bytes_saved, 32 + 16);
    }

    #[test]
    fn report_numbers_with_no_shared_accounts_is_zero() {
        let shared: HashMap<String, Vec<DidHash>> = HashMap::new();
        let (share, bytes_saved) = discovery_share_and_bytes_saved(&[], &shared);
        assert_eq!(share, 0.0);
        assert_eq!(bytes_saved, 0);
    }

    fn feed_row(quote_did: &str, original_did: &str, promoted_at: i64) -> FeedRow {
        FeedRow {
            quote_uri: format!("at://{quote_did}/app.bsky.feed.post/1"),
            quote_cid: format!("cid-{quote_did}"),
            quote_did: quote_did.to_string(),
            original_did: original_did.to_string(),
            quoted_at: promoted_at,
            v_likes_q: 1,
            v_reposts_q: 0,
            v_replies_q: 0,
            v_likes_o: 1,
            v_reposts_o: 0,
            v_replies_o: 0,
            ratio: 1.0,
            rank: 1.0,
            promoted_at,
            verified_at: promoted_at,
        }
    }

    #[test]
    fn circle_pairs_24h_counts_the_window_and_the_degree2_only_share() {
        let now = 1_700_000_000_i64;
        let mut circle = Circle::new();
        circle.follows.insert(hash_did("did:plc:followed"));

        let rows = vec![
            // Kept via `follows`, inside the window.
            feed_row("did:plc:followed", "did:plc:x", now - 100),
            // Kept via degree 2 only, inside the window.
            feed_row("did:plc:d2", "did:plc:y", now - 200),
            // Kept via degree 2 only, outside the 24h window.
            feed_row("did:plc:d2", "did:plc:z", now - 25 * 3600),
            // Not connected at all.
            feed_row("did:plc:nobody", "did:plc:nowhere", now - 100),
        ];
        let d2_set: HashSet<DidHash> = HashSet::from([hash_did("did:plc:d2")]);

        let (total_24h, share) = circle_pairs_24h(&rows, &circle, &d2_set, 0, now);

        assert_eq!(total_24h, 2);
        assert_eq!(share, 0.5);
    }

    #[test]
    fn circle_pairs_24h_with_no_kept_items_is_zero_share() {
        let now = 1_700_000_000_i64;
        let circle = Circle::new();
        let rows = vec![feed_row("did:plc:x", "did:plc:y", now)];

        let (total_24h, share) = circle_pairs_24h(&rows, &circle, &HashSet::new(), 0, now);

        assert_eq!(total_24h, 0);
        assert_eq!(share, 0.0);
    }

    // --- time_filter_and_caps (BC12) --------------------------------------

    #[test]
    fn filter_timing_skips_on_zero_feed_rows() {
        let circle = Circle::new();
        let weights = Weights { repost: 2.0, reply: 0.5 };
        let result =
            time_filter_and_caps(&[], &circle, &HashSet::new(), 0, &weights, 1_700_000_000, 5.0);
        assert!(result.is_none());
    }

    #[test]
    fn filter_timing_runs_over_synthetic_items() {
        let mut circle = Circle::new();
        circle.follows.insert(hash_did("did:plc:x"));
        let rows = vec![feed_row("did:plc:x", "did:plc:y", 1_700_000_000)];
        let weights = Weights { repost: 2.0, reply: 0.5 };

        let result =
            time_filter_and_caps(&rows, &circle, &HashSet::new(), 0, &weights, 1_700_000_000, 5.0);

        assert!(result.is_some());
    }

    // --- check_order (BC14) ------------------------------------------------

    #[test]
    fn check_order_matches_identical_lists() {
        let page = vec!["did:plc:a".to_string(), "did:plc:b".to_string()];
        assert_eq!(check_order(&page, &page), OrderCheck::Match);
    }

    #[test]
    fn check_order_reports_the_first_difference() {
        let page = vec!["did:plc:a".to_string(), "did:plc:b".to_string()];
        let listed = vec!["did:plc:a".to_string(), "did:plc:c".to_string()];
        assert_eq!(check_order(&page, &listed), OrderCheck::Difference(1));
    }

    #[test]
    fn check_order_reports_a_length_mismatch_at_the_shorter_length() {
        let page = vec!["did:plc:a".to_string()];
        let listed = vec!["did:plc:a".to_string(), "did:plc:b".to_string()];
        assert_eq!(check_order(&page, &listed), OrderCheck::Difference(1));
    }
}

/// Live test against a real PDS and store, `#[ignore]`d so `cargo test
/// --all-features` never touches the network or a real database (AC6). Run
/// by hand with `BSKY_HANDLE`, `BSKY_APP_PASSWORD`, `UPSTAGE_DB_PATH` and
/// `UPSTAGE_PROBE_HANDLES` (comma-separated) set:
/// `cargo test -- --ignored graph_probe_live`.
#[cfg(test)]
mod live_tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn graph_probe_live() {
        let lookup = |name: &str| std::env::var(name).ok();
        let cfg = crate::config::load(lookup).expect("real environment provides a valid config");
        let handles: Vec<String> = std::env::var("UPSTAGE_PROBE_HANDLES")
            .expect("UPSTAGE_PROBE_HANDLES set for the live test")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let report = run(&cfg, &handles).await.expect("a real run against real handles succeeds");
        print_run_report(&report);
    }
}
