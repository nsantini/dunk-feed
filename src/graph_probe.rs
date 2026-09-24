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
    #[error(
        "invalid --handle value {value:?}: must not be empty, and must not contain '@', \
         whitespace or a control character once a single leading '@' is stripped and the \
         result is trimmed"
    )]
    // Review round 2, defect I: an empty or malformed `--handle` value is
    // its own error, distinct from `NoHandles`, which now means only "zero
    // `--handle` flags were given at all". Review round 3, defect P: `value`
    // is printed with `{:?}` (`Debug`, not `Display`), so a control
    // character in a hostile `--handle` is escaped rather than written to
    // the terminal raw.
    InvalidHandle { value: String },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("every handle failed; see the failure lines above")]
    // `run` prints each failure's line through `print_failures` before
    // returning this variant (review round 1, defect A), so "above" refers
    // to stdout, not to a field this error carries.
    AllFailed,
}

/// Keeps the first occurrence of each handle, dropping a later repeat
/// (BC9a), comparing ASCII case-insensitively so `Alice.bsky.social` and
/// `alice.bsky.social` collapse to one entry (review round 1, defect F): a
/// handle given twice, in any casing, is probed once, keeping the first
/// spelling seen.
pub fn dedup_first_seen(handles: &[String]) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out = Vec::with_capacity(handles.len());
    for handle in handles {
        if seen.insert(handle.to_ascii_lowercase()) {
            out.push(handle.clone());
        }
    }
    out
}

/// Trims `raw`, strips one leading `@`, then trims again, so
/// `@ alice.bsky.social` normalizes to `alice.bsky.social` before
/// `dedup_first_seen` or any network call sees them (BC9; review round 1,
/// defect G; review round 2, defect H). The result is `Err` when it is
/// empty, or still holds an `@`, any whitespace, or a control character
/// (`char::is_control`; review round 3, defect P): only one leading `@` is
/// stripped, so `@@alice.bsky.social` (a second `@` left over), `" "`
/// (nothing left after trimming) and a value carrying an escape or other
/// control byte are all invalid rather than silently mangled or printed raw.
fn normalize_handle(raw: &str) -> Result<String, ()> {
    let trimmed = raw.trim();
    let without_at = trimmed.strip_prefix('@').unwrap_or(trimmed).trim();
    if without_at.is_empty()
        || without_at.contains('@')
        || without_at.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        Err(())
    } else {
        Ok(without_at.to_string())
    }
}

/// Checks `--handle` (BC9, BC9a) before credentials (BC8), and both before
/// any network call or store open: `--handle` is not `required` in clap, so
/// an empty list reaches here rather than clap exiting 2 on its own
/// (spec.md "Defaults taken"). Zero `--handle` flags is [`GraphProbeError::NoHandles`];
/// a `--handle` value that [`normalize_handle`] rejects is
/// [`GraphProbeError::InvalidHandle`] naming that raw value, checked before
/// credentials (review round 2, defect I — `NoHandles` no longer covers a
/// single blank or malformed handle). Reuses `publish::preflight`'s
/// credential check (`avatar_path: None`, so its avatar branch never runs)
/// instead of re-implementing the same blank/trim rule a second time.
pub fn preflight(
    cfg: &Config,
    handles: &[String],
) -> Result<(Vec<String>, Credentials), GraphProbeError> {
    if handles.is_empty() {
        return Err(GraphProbeError::NoHandles);
    }
    let mut normalized: Vec<String> = Vec::with_capacity(handles.len());
    for raw in handles {
        match normalize_handle(raw) {
            Ok(handle) => normalized.push(handle),
            Err(()) => return Err(GraphProbeError::InvalidHandle { value: raw.clone() }),
        }
    }
    let handles = dedup_first_seen(&normalized);
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

/// `rows`, reordered to match the current snapshot's ranked order: runs
/// `scorer::snapshot::build` (the same ranking and capping the served feed
/// uses, `src/scorer/snapshot.rs`, story 01) and maps each item `global`
/// names back to its own `FeedRow` by `quote_uri` (step 7.5, review round 1,
/// finding 6; spec.md "Defaults taken" and `## Answers from the engineer`;
/// BC11: `global` carries the `01` caps, so this order is unchanged from
/// before story 01). `step_follows_me`'s candidates, the connection
/// filter's `follows_me_depth` check and the circle-pairs window all read
/// "ranked order" as this order, not `Store::feed_rows`'s own row order —
/// the two can differ once the caps drop or reorder rows. A `quote_uri`
/// `build` returns that no longer matches any input row (it never invents
/// one) is silently dropped, since the caller only wants the rows that
/// survived ranking, in that order.
fn ranked_order(rows: Vec<FeedRow>, weights: &Weights, now: i64, k: f64) -> Vec<FeedRow> {
    let by_uri: HashMap<String, FeedRow> =
        rows.iter().map(|row| (row.quote_uri.clone(), row.clone())).collect();
    let (items, global) = snapshot::build(rows, weights, now, k);
    global
        .into_iter()
        .filter_map(|idx| by_uri.get(&items[idx as usize].quote_uri).cloned())
        .collect()
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

/// Why the order check (BC14) was skipped: either the `getFollows` or the
/// `listRecords` call it needs failed, or the session that either call
/// depends on could not be established or refreshed. `Status` carries a
/// failing `listRecords` or `getFollows` call's own HTTP status (BC14's
/// "skipped (`<status>`)"); `Session` carries `createSession` or
/// `refreshSession`'s status when there is one (`Some`), or `None` when the
/// session failure never reached the wire (`PdsError::Session`) — either
/// way it prints as `session` or `session (<status>)`, never bare, so a
/// session failure is never mistaken for the `listRecords`/`getFollows`
/// call itself failing (review round 2, defect K). `Transport` and
/// `Decode` name the kind of a status-less, non-session `PdsError`, so the
/// printed reason is never `0` standing in for "no status" (review round 1,
/// defect C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    Status(u16),
    Session(Option<u16>),
    Transport,
    Decode,
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SkipReason::Status(status) => write!(f, "{status}"),
            SkipReason::Session(Some(status)) => write!(f, "session ({status})"),
            SkipReason::Session(None) => write!(f, "session"),
            SkipReason::Transport => write!(f, "transport"),
            SkipReason::Decode => write!(f, "decode"),
        }
    }
}

/// `com.atproto.server.createSession`'s nsid, matching the private constant
/// of the same name in `src/appview/pds.rs`: that module is out of scope
/// for this slice (`## Files in scope` names only `graph_probe.rs` and
/// `graph/build.rs`), so [`skip_reason`] compares against this copy rather
/// than widening the slice to export the original.
const LOGIN_NSID: &str = "com.atproto.server.createSession";

/// `com.atproto.server.refreshSession`'s nsid, for the same reason as
/// [`LOGIN_NSID`].
const REFRESH_NSID: &str = "com.atproto.server.refreshSession";

/// Maps a [`PdsError`] from the order check's `getFollows` or
/// `list_follow_records` call to the [`SkipReason`] it prints (review round
/// 1, defect C; review round 2, defect K). An `Http` failure whose `method`
/// is [`LOGIN_NSID`] or [`REFRESH_NSID`] is a session failure, not a
/// `listRecords`/`getFollows` one, so it maps to `Session` carrying that
/// call's own status — distinct from a `listRecords` or `getFollows` `Http`
/// failure, which carries its status as a bare `Status` instead. Any other
/// `Http` with no status (a transport error that outlasted the retry
/// schedule) and `PdsError::TooMany` (never actually reachable here, since
/// neither call sends `others`) both read as `Transport`; `InvalidRate` is
/// unreachable too (rejected at config load and at `PdsClient::new`, never
/// returned by a call), and maps to `Transport` as the least misleading
/// fallback rather than a sixth variant nothing can construct.
fn skip_reason(err: &PdsError) -> SkipReason {
    match err {
        PdsError::Http { method, status, .. }
            if *method == LOGIN_NSID || *method == REFRESH_NSID =>
        {
            SkipReason::Session(*status)
        }
        PdsError::Http { status: Some(status), .. } => SkipReason::Status(*status),
        PdsError::Http { status: None, .. } => SkipReason::Transport,
        PdsError::Decode { .. } => SkipReason::Decode,
        PdsError::Session => SkipReason::Session(None),
        PdsError::InvalidRate | PdsError::TooMany { .. } => SkipReason::Transport,
    }
}

/// The order check's result (BC14): the first `getFollows` page matches the
/// newest `app.bsky.graph.follow` records position for position, the first
/// index where they differ, or the check was skipped, carrying the
/// [`SkipReason`] that caused the skip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderCheck {
    Match,
    Difference(usize),
    Skipped(SkipReason),
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

    // Step 7.5 (review round 1, finding 6): "ranked" is the current
    // snapshot's order, not `Store::feed_rows`'s own order. This feeds
    // `step_follows_me`'s candidates, the connection filter's depth check
    // and the circle-pairs window alike (spec.md "Defaults taken"), so it
    // is computed once, here, and passed to every one of them.
    let ranked_rows = ranked_order(rows, &weights, now, k);
    let ranked: Vec<RankedAuthors> = ranked_rows
        .iter()
        .map(|row| RankedAuthors {
            quote_did: row.quote_did.clone(),
            original_did: row.original_did.clone(),
        })
        .collect();

    let mut shared_d2: HashMap<String, Vec<DidHash>> = HashMap::new();
    let mut reports = Vec::new();
    let mut failures = Vec::new();
    let mut d2_samples: Vec<Vec<String>> = Vec::new();

    for handle in &handles {
        match run_one_handle(
            &client,
            handle,
            &ranked,
            &ranked_rows,
            cfg,
            &weights,
            now,
            k,
            &mut shared_d2,
        )
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
        // Review round 1, defect A: `cli.rs` only calls `print_run_report`
        // on `Ok`, so the all-failed path must print its own failure lines
        // through the same `print_failures` before returning the error —
        // otherwise a run where every handle failed ends in silence.
        print_failures(&failures);
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
    let follows_stats =
        step_follows(client, &did, cfg.d2_follows_sample as usize, &mut circle, None)
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
            Ok(ListFollowRecordsOutcome::Failed(status)) => {
                OrderCheck::Skipped(SkipReason::Status(status))
            }
            Err(err) => OrderCheck::Skipped(skip_reason(&err)),
        },
        Err(err) => OrderCheck::Skipped(skip_reason(&err)),
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

/// One printable line per [`HandleFailure`] (BC4a: `<handle>: failed at
/// <step>: <error>`), naming only the handle, the step and the error
/// (BC16) — never a viewer DID. The handle is printed with `{:?}` (`Debug`),
/// escaped rather than raw (BC16; review round 3, defect P), matching every
/// other place this module prints a handle: `preflight` already rejects a
/// control character before a handle reaches here, but the escaping is
/// applied uniformly rather than relied on to have happened upstream. A pure
/// function so the line format is tested directly, not through
/// [`print_failures`]'s `println!` side effect.
fn failure_lines(failures: &[HandleFailure]) -> Vec<String> {
    failures
        .iter()
        .map(|failure| {
            format!("{:?}: failed at {}: {}", failure.handle, failure.step, failure.error)
        })
        .collect()
}

/// Prints [`failure_lines`] for `failures`, one per line. Called both by
/// [`print_run_report`] (the handles that failed alongside the ones that
/// completed) and by [`run`] on the all-failed path (review round 1, defect
/// A), so a run where every handle fails still prints why before it returns
/// [`GraphProbeError::AllFailed`], instead of ending in silence.
fn print_failures(failures: &[HandleFailure]) {
    for line in failure_lines(failures) {
        println!("{line}");
    }
}

/// Prints `report`, one line per number (BC10, BC11, BC12, BC13, BC14),
/// naming only handles, step names and statuses (BC16) — never a viewer
/// DID. Kept separate from [`run`] so a test checks the numbers `run`
/// collects, not this function's text (spec.md `## Approach`).
pub fn print_run_report(report: &RunReport) {
    print_failures(&report.failures);
    for probe in &report.reports {
        // Review round 3, defect P: `{:?}` escapes the handle rather than
        // writing it raw, matching `failure_lines` above.
        println!("{:?}:", probe.handle);
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

    // --- skip_reason (BC14; review round 1, defect C) ----------------------

    #[test]
    fn skip_reason_maps_each_error_shape() {
        assert_eq!(
            skip_reason(&PdsError::Http { method: "op", status: Some(404), attempts: 1 }),
            SkipReason::Status(404)
        );
        assert_eq!(
            skip_reason(&PdsError::Http { method: "op", status: None, attempts: 3 }),
            SkipReason::Transport
        );
        assert_eq!(
            skip_reason(&PdsError::Decode { method: "op", reason: "bad json".to_string() }),
            SkipReason::Decode
        );
        assert_eq!(skip_reason(&PdsError::Session), SkipReason::Session(None));
    }

    // --- skip_reason session mapping (BC14; review round 2, defect K) ------

    #[test]
    fn skip_reason_distinguishes_session_status_from_a_bare_status() {
        // The same 401 prints differently depending on which call failed:
        // `refreshSession` is a session failure, `listRecords` is not.
        let refresh_failure =
            skip_reason(&PdsError::Http { method: REFRESH_NSID, status: Some(401), attempts: 1 });
        let list_records_failure = skip_reason(&PdsError::Http {
            method: "com.atproto.repo.listRecords",
            status: Some(401),
            attempts: 1,
        });

        assert_eq!(refresh_failure, SkipReason::Session(Some(401)));
        assert_eq!(list_records_failure, SkipReason::Status(401));
        assert_ne!(format!("{refresh_failure}"), format!("{list_records_failure}"));
        assert_eq!(format!("{refresh_failure}"), "session (401)");
        assert_eq!(format!("{list_records_failure}"), "401");
    }

    #[test]
    fn skip_reason_maps_login_status_to_session_too() {
        let failure =
            skip_reason(&PdsError::Http { method: LOGIN_NSID, status: Some(500), attempts: 1 });
        assert_eq!(failure, SkipReason::Session(Some(500)));
    }

    #[test]
    fn skip_reason_never_prints_a_bare_zero() {
        // A status-less failure must read as a word, never the number 0
        // standing in for "no status" (review round 1, defect C).
        let printed =
            format!("{}", skip_reason(&PdsError::Http { method: "op", status: None, attempts: 1 }));
        assert_ne!(printed, "0");
        assert_eq!(printed, "transport");
    }

    // --- dedup_first_seen case-insensitivity (BC9a; review round 1, defect F)

    #[test]
    fn dedup_first_seen_collapses_case_insensitively_and_keeps_first_spelling() {
        let handles = vec!["Alice.bsky.social".to_string(), "alice.bsky.social".to_string()];
        assert_eq!(dedup_first_seen(&handles), vec!["Alice.bsky.social"]);
    }

    // --- preflight normalization (BC9; review round 1, defect G) -----------

    #[test]
    fn preflight_strips_at_and_trims_whitespace() {
        let lookup = |name: &str| match name {
            "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
            "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "BSKY_HANDLE" => Some("upstage.bsky.social".to_string()),
            "BSKY_APP_PASSWORD" => Some("secret".to_string()),
            _ => None,
        };
        let cfg = crate::config::load(lookup).expect("config loads");

        let (deduped, _) =
            preflight(&cfg, &["@alice.bsky.social".to_string()]).expect("a leading @ is stripped");
        assert_eq!(deduped, vec!["alice.bsky.social"]);

        let (deduped, _) = preflight(&cfg, &[" alice.bsky.social".to_string()])
            .expect("surrounding whitespace is trimmed");
        assert_eq!(deduped, vec!["alice.bsky.social"]);
    }

    #[test]
    fn preflight_rejects_an_empty_handle_before_credentials() {
        // Review round 2, defect I: a single blank `--handle` is
        // `InvalidHandle`, not `NoHandles` — that variant now covers only
        // zero `--handle` flags. Still checked before the
        // missing-credentials check, even though this config has neither.
        let cfg = base_config();
        let err = preflight(&cfg, &["".to_string()]).unwrap_err();
        match err {
            GraphProbeError::InvalidHandle { value } => assert_eq!(value, ""),
            other => panic!("expected InvalidHandle, got {other:?}"),
        }
    }

    // --- normalize_handle (BC9; review round 2, defect H) -------------------

    #[test]
    fn normalize_handle_strips_at_then_trims_again() {
        assert_eq!(normalize_handle("@ alice.bsky.social"), Ok("alice.bsky.social".to_string()));
    }

    #[test]
    fn normalize_handle_rejects_a_second_leading_at() {
        // Only one leading `@` is stripped, so a second one left over marks
        // the handle invalid rather than being stripped too.
        assert_eq!(normalize_handle("@@alice.bsky.social"), Err(()));
    }

    #[test]
    fn normalize_handle_rejects_whitespace_only() {
        assert_eq!(normalize_handle(" "), Err(()));
    }

    #[test]
    fn normalize_handle_rejects_a_control_character() {
        // Review round 3, defect P: a raw ANSI escape sequence in a
        // `--handle` value must not survive to a network call or a printed
        // line.
        assert_eq!(normalize_handle("a\x1b[31m"), Err(()));
    }

    #[test]
    fn invalid_handle_message_escapes_a_control_character() {
        // Review round 3, defect P: `InvalidHandle`'s `Display` uses
        // `{value:?}`, so the message text holds the escaped `\u{1b}`, never
        // the raw ESC byte a terminal would interpret.
        let err = GraphProbeError::InvalidHandle { value: "a\x1b[31m".to_string() };
        let message = err.to_string();
        assert!(!message.contains('\x1b'));
        assert!(message.contains("\\u{1b}"));
    }

    // --- preflight InvalidHandle vs NoHandles (BC9; review round 2, defect I)

    #[test]
    fn preflight_flags_one_bad_handle_as_invalid_not_no_handles() {
        let cfg = base_config();
        let handles = vec!["alice.bsky.social".to_string(), " ".to_string()];

        let err = preflight(&cfg, &handles).unwrap_err();

        match err {
            GraphProbeError::InvalidHandle { value } => assert_eq!(value, " "),
            other => panic!("expected InvalidHandle, got {other:?}"),
        }
    }

    #[test]
    fn preflight_with_zero_handle_flags_is_still_no_handles() {
        let cfg = base_config();
        let err = preflight(&cfg, &[]).unwrap_err();
        assert!(matches!(err, GraphProbeError::NoHandles));
    }

    // --- print_failures (BC4a, BC16; review round 1, defect A) -------------

    #[test]
    fn failure_lines_name_the_handle_step_and_error() {
        let failures = vec![HandleFailure {
            handle: "alice.bsky.social".to_string(),
            step: "step_follows",
            error: "boom".to_string(),
        }];
        assert_eq!(
            failure_lines(&failures),
            vec!["\"alice.bsky.social\": failed at step_follows: boom".to_string()]
        );
    }

    #[test]
    fn failure_lines_escape_a_handle_instead_of_printing_it_raw() {
        // Review round 3, defect P: a handle that reaches `failure_lines`
        // carrying a control character (it should never pass `preflight`,
        // but this function does not trust that) is escaped, not written
        // raw, so it cannot inject terminal control sequences.
        let failures = vec![HandleFailure {
            handle: "a\x1b[31m".to_string(),
            step: "resolve_handle",
            error: "boom".to_string(),
        }];
        let lines = failure_lines(&failures);
        assert!(!lines[0].contains('\x1b'));
        assert!(lines[0].contains("\\u{1b}"));
    }

    // --- ranked_order (BC2, BC13; review round 1, defect D) ----------------

    #[test]
    fn ranked_order_follows_the_snapshot_not_the_stored_order() {
        // Two rows with distinct authors and days, so neither cap drops or
        // reorders either one: `snapshot::build` only reorders them by its
        // own recomputed rank. `strong` carries far more engagement than
        // `weak` (100 likes against 1, both against the same default
        // `v_likes_o`), enough to outrank `weak`'s slight recency edge
        // (`score::rank`'s age term falls off much more slowly than its
        // engagement term rises here) — so `strong` ranks first even though
        // it is stored second.
        let now = 1_700_200_000;
        let mut strong = feed_row("did:plc:strong", "did:plc:strong-o", 1_700_000_000);
        strong.v_likes_q = 100;
        let mut weak = feed_row("did:plc:weak", "did:plc:weak-o", 1_700_086_400);
        weak.v_likes_q = 1;
        let weights = Weights { repost: 2.0, reply: 0.5 };
        let k = 5.0;

        // Confirms the ranking assumption above directly through `score::rank`,
        // so the test does not rely on hand-computed numbers matching
        // `recompute_ranks`'s own formula.
        let rank_of = |row: &FeedRow| {
            let eq = crate::score::engagement(
                &crate::score::Counts { likes: row.v_likes_q as u32, reposts: 0, replies: 0 },
                &weights,
            );
            let eo = crate::score::engagement(
                &crate::score::Counts { likes: row.v_likes_o as u32, reposts: 0, replies: 0 },
                &weights,
            );
            let d = crate::score::ratio(eq, eo, k);
            let age_hours = (now - row.quoted_at) as f64 / 3600.0;
            crate::score::rank(d, eq, age_hours)
        };
        assert!(
            rank_of(&strong) > rank_of(&weak),
            "test setup: `strong` must outrank `weak` for this test to exercise the reorder"
        );

        let stored_uris = vec![weak.quote_uri.clone(), strong.quote_uri.clone()];
        let stored_order = vec![weak.clone(), strong.clone()];

        let ranked = ranked_order(stored_order, &weights, now, k);
        let ranked_uris: Vec<String> = ranked.into_iter().map(|row| row.quote_uri).collect();

        assert_eq!(ranked_uris, vec![strong.quote_uri.clone(), weak.quote_uri.clone()]);
        // Sanity: the stored order was not already the snapshot order —
        // otherwise this test would pass without exercising the fix.
        assert_ne!(stored_uris, ranked_uris);
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
