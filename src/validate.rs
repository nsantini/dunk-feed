//! `dunk validate` phase 0 tool, TECH-DESIGN section 10. Every decision here
//! is a pure function over values already in memory: seeding, the quote
//! filter, scoring, row ordering, and rendering. Only `run` touches the
//! network or the filesystem, so this module's fixture tests cover the whole
//! decision path with no network.
//!
//! `candidates` reuses `ingest::embed::detect` on the quote's own
//! `post.record`, never a second detector, and `build_rows` reuses
//! `score.rs`'s `Counts`, `engagement`, `ratio` and `rank` directly, never a
//! second formula; it reads the popularity and margin gates off the same
//! `Thresholds` `score::qualifies` uses, but separately, since the table
//! prints each gate on its own (BC17, BC18), not only their conjunction.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::appview::types::{classify_quote_embed, PostView, QuoteEmbed};
use crate::appview::{AppViewClient, AppViewError};
use crate::ingest::embed::{self, AtUri, Embed};
use crate::score::{self, Counts, Thresholds, Weights};

/// The feed seeded when no `--seed-file` is given or it yields no
/// candidates. TECH-DESIGN section 4 lists no `Config` variable for it, so
/// it is a constant here, not a field on `Config`.
pub const HOT_CLASSIC_FEED: &str =
    "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/hot-classic";

/// Why a row carries no score, printed in place of its numbers. Every
/// variant maps to the exact reason string TECH-DESIGN section 8.3 and
/// section 10, and the spec's behaviour contracts, name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// BC4: the quote and the original share one author.
    SelfQuote,
    /// BC5: the original is absent from the `getPosts` result map, and the
    /// quote's own hydrated embed gives no more specific reason. BC21: also
    /// the reason when the quote's hydrated `postView.embed` itself names
    /// the original `#viewNotFound`.
    OriginalGone,
    /// BC21: the quote's hydrated `postView.embed` names the original
    /// `#viewBlocked`.
    Blocked,
    /// BC21: the quote's hydrated `postView.embed` names the original
    /// `#viewDetached`.
    Detached,
    /// BC24: the quote's hydrated `postView.embed` inner `$type` is present
    /// but names none of `#viewRecord`, `#viewNotFound`, `#viewBlocked` or
    /// `#viewDetached`.
    NotAPost,
    /// BC9: a `--seed-file` URI absent from the `getPosts` result map.
    QuoteGone,
}

impl Reason {
    /// The exact string the table and the CSV both print for this reason.
    pub fn as_str(&self) -> &'static str {
        match self {
            Reason::SelfQuote => "self_quote",
            Reason::OriginalGone => "original_gone",
            Reason::Blocked => "blocked",
            Reason::Detached => "detached",
            Reason::NotAPost => "not_a_post",
            Reason::QuoteGone => "quote_gone",
        }
    }
}

/// A quote post whose record embeds another post, TECH-DESIGN section 10
/// step 2. `SelfQuote` is separated from `Scoreable` here, in `candidates`,
/// because BC4 needs no `getPosts` round trip at all: the original's DID is
/// already the embedded URI's own authority.
#[derive(Debug, Clone, PartialEq)]
pub enum Candidate {
    Scoreable { quote: PostView, original_uri: AtUri },
    SelfQuote { quote: PostView, original_uri: AtUri },
}

/// One printed row: a quote/original pair, its score when it has one, and
/// the reason it has none otherwise. TECH-DESIGN section 10 step 4 lists the
/// columns the table and the CSV both show: the two `bsky.app` links,
/// `E(Q)`, `E(O)`, `D`, `rank`, the popularity gate, the margin gate, and a
/// reason for an unscored row.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub quote_uri: String,
    pub quote_cid: String,
    pub original_uri: String,
    pub eq: Option<f64>,
    pub eo: Option<f64>,
    pub ratio: Option<f64>,
    pub rank: Option<f64>,
    pub popularity_pass: Option<bool>,
    pub margin_pass: Option<bool>,
    pub reason: Option<Reason>,
}

/// Filters `posts` down to quote candidates, TECH-DESIGN section 10 step 2.
/// A record whose embed is not a quote is dropped in silence (BC3), never
/// printed. A quote URI seen twice keeps its first occurrence and drops the
/// rest (BC22). A candidate whose embedded URI's own authority DID equals
/// the quote's author DID is a self quote (BC4), classified here rather than
/// after a `getPosts` round trip: the original's DID is already the URI's
/// own authority, so no fetch is needed to tell.
pub fn candidates(posts: Vec<PostView>) -> Vec<Candidate> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for quote in posts {
        let original_uri = match embed::detect(&quote.record.rest) {
            Embed::Quote { original_uri } => original_uri,
            Embed::NotAQuote => continue,
        };
        if !seen.insert(quote.uri.clone()) {
            continue;
        }
        if embed::is_self_quote(&original_uri, &quote.author.did) {
            out.push(Candidate::SelfQuote { quote, original_uri });
        } else {
            out.push(Candidate::Scoreable { quote, original_uri });
        }
    }
    out
}

/// The unscored row a `Scoreable` candidate's hydrated embed view assigns on
/// its own, before any `getPosts` lookup happens (finding 1, BC21, BC24;
/// round 2 finding 8: shares `appview::types::classify_quote_embed` with
/// `scorer::verify` instead of its own copy of section 8.2's table):
/// `#viewDetached` is `detached`, `#viewBlocked` is `blocked`, and
/// `QuoteEmbed::NotAPost` is `not_a_post`. `QuoteEmbed::NotFound`,
/// `QuoteEmbed::Normal` and `QuoteEmbed::Absent` all return `None`: the first
/// still needs the generic `original_gone` a missing map entry gives (BC5),
/// the second must still be scored, and the third (no hydrated embed at all,
/// or an outer `$type` naming neither `record#view` nor
/// `recordWithMedia#view`) falls through to the same `getPosts` lookup
/// (BC25), since none of the three tells us anything about the original on
/// its own.
fn reason_from_quote_embed(quote: &PostView) -> Option<Reason> {
    match classify_quote_embed(quote) {
        QuoteEmbed::Detached => Some(Reason::Detached),
        QuoteEmbed::Blocked => Some(Reason::Blocked),
        QuoteEmbed::NotAPost => Some(Reason::NotAPost),
        QuoteEmbed::NotFound | QuoteEmbed::Normal { .. } | QuoteEmbed::Absent => None,
    }
}

/// An unscored [`Row`] for `quote`/`original_uri`, carrying `reason`.
fn unscored_row(quote: &PostView, original_uri: &AtUri, reason: Reason) -> Row {
    Row {
        quote_uri: quote.uri.clone(),
        quote_cid: quote.cid.clone(),
        original_uri: original_uri.as_str().to_string(),
        eq: None,
        eo: None,
        ratio: None,
        rank: None,
        popularity_pass: None,
        margin_pass: None,
        reason: Some(reason),
    }
}

/// Builds one [`Row`] per candidate, TECH-DESIGN section 10 steps 3 and 4,
/// and section 8.2 and 8.3 for the embed-view check. A `SelfQuote`
/// candidate never reaches `score.rs`: it is printed with
/// [`Reason::SelfQuote`] and no score (BC4). A `Scoreable` candidate's own
/// hydrated `postView.embed` is read first, before any `getPosts` lookup
/// (finding 1): a `#viewDetached` or `#viewBlocked` original, or an inner
/// view that names neither a post nor an absence, is printed with its own
/// reason and never scored, even when `originals` already holds that URI
/// (BC21, BC24). Only `#viewNotFound`, a `#viewRecord`, or no inner view at
/// all falls through to the `originals` lookup (BC25); a miss there is
/// `original_gone` (BC5), and a hit scores through `score.rs`'s own
/// `Counts`, `engagement`, `ratio` and `rank`, with `age_hours` clamped at
/// zero (BC16) and the two gates (BC17, BC18) read off `thresholds` through
/// `score::clears_floor` and `score::clears_margin` rather than through
/// `score::qualifies`, which only returns their conjunction.
pub fn build_rows(
    candidates: Vec<Candidate>,
    originals: &HashMap<String, PostView>,
    weights: &Weights,
    thresholds: &Thresholds,
    now: DateTime<Utc>,
) -> Vec<Row> {
    candidates
        .into_iter()
        .map(|candidate| match candidate {
            Candidate::SelfQuote { quote, original_uri } => {
                unscored_row(&quote, &original_uri, Reason::SelfQuote)
            }
            Candidate::Scoreable { quote, original_uri } => {
                if let Some(reason) = reason_from_quote_embed(&quote) {
                    return unscored_row(&quote, &original_uri, reason);
                }
                match originals.get(original_uri.as_str()) {
                    Some(original) => {
                        let eq = score::engagement(&Counts::from(&quote), weights);
                        let eo = score::engagement(&Counts::from(original), weights);
                        let d = score::ratio(eq, eo, thresholds.k);
                        let age = age_hours(&quote.record.created_at, now);
                        let rank = score::rank(d, eq, age);
                        Row {
                            quote_uri: quote.uri.clone(),
                            quote_cid: quote.cid.clone(),
                            original_uri: original_uri.as_str().to_string(),
                            eq: Some(eq),
                            eo: Some(eo),
                            ratio: Some(d),
                            rank: Some(rank),
                            popularity_pass: Some(score::clears_floor(eq, eo, thresholds)),
                            margin_pass: Some(score::clears_margin(d, thresholds)),
                            reason: None,
                        }
                    }
                    None => unscored_row(&quote, &original_uri, Reason::OriginalGone),
                }
            }
        })
        .collect()
}

/// `rank`'s `age_hours` input, TECH-DESIGN section 10 step 3. Parses
/// `created_at` as RFC 3339 and returns the number of hours between it and
/// `now`. Clamped to `0.0`, with one warning line to stderr, when
/// `created_at` fails to parse or names a time after `now` (BC16): `rank`
/// requires a non-negative age, and a caller error here must never produce
/// one silently.
pub fn age_hours(created_at: &str, now: DateTime<Utc>) -> f64 {
    // Round 1 finding 7: shares `parse_rfc3339_secs` with
    // `CommitEvent::time_secs` and `ingest::quoted_at` instead of parsing
    // RFC3339 a second way here.
    let hours = match crate::jetstream::event::parse_rfc3339_secs(created_at) {
        Some(secs) => (now.timestamp() - secs) as f64 / 3600.0,
        None => {
            eprintln!("warning: quote createdAt {created_at:?} does not parse as RFC 3339");
            return 0.0;
        }
    };
    if hours < 0.0 {
        eprintln!("warning: quote createdAt {created_at:?} is in the future, clamping age to zero");
        0.0
    } else {
        hours
    }
}

/// The `bsky.app` link for an `at://` post URI (BC23):
/// `https://bsky.app/profile/<did>/post/<rkey>`. `None` when `uri` does not
/// parse as `AtUri::parse` already requires of every quote and original URI
/// this module handles, so a `None` here would signal a URI this module
/// never should have accepted in the first place.
pub fn bsky_link(uri: &str) -> Option<String> {
    let at = AtUri::parse(uri)?;
    Some(format!("https://bsky.app/profile/{}/post/{}", at.did(), at.rkey()))
}

/// Orders rows for both the printed table and the CSV, TECH-DESIGN section
/// 10 step 4. A scored row sorts before every unscored row (BC15). Among
/// scored rows, rank sorts descending (BC14). Ties, and every pair of
/// unscored rows, sort by `quote_cid` ascending (BC14, BC15).
pub fn sort_rows(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort_by(|a, b| match (a.rank, b.rank) {
        (Some(a_rank), Some(b_rank)) => b_rank
            .partial_cmp(&a_rank)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.quote_cid.cmp(&b.quote_cid)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => a.quote_cid.cmp(&b.quote_cid),
    });
    rows
}

/// `dunk validate`'s own errors. `AGENTS.md` keeps `anyhow` in `main.rs`
/// only, so every variant here carries enough context for `main.rs` to
/// print one line and exit 1 (BC10, BC11, BC12) with no further lookup.
#[derive(Debug, Error)]
pub enum ValidateError {
    /// BC10: `--seed-file` names a path that cannot be read.
    #[error("failed to read seed file {path:?}: {source}")]
    SeedFile { path: PathBuf, source: std::io::Error },
    /// BC11: `--csv-path`'s parent directory is missing, or the write is
    /// refused for any other reason.
    #[error("failed to write CSV to {path:?}: {source}")]
    CsvWrite { path: PathBuf, source: std::io::Error },
    /// BC12: `get_feed` or `get_posts` spent every retry.
    #[error("app view request failed: {0}")]
    AppView(#[from] AppViewError),
}

/// Parses `--seed-file`'s contents into quote URIs, TECH-DESIGN section 10
/// step 1. A blank line or one starting with `#` is skipped in silence
/// (BC8). A non-blank line that does not parse as `AtUri::parse` is skipped
/// with one warning line to stderr, and the run continues (BC6). An empty
/// result, whether the file was empty or every line was skipped, signals
/// the caller to fall back to `hot-classic` (BC7); this function does not
/// print that fallback note itself, since it does not know which case it is
/// until the caller decides.
pub fn parse_seed_lines(contents: &str) -> Vec<AtUri> {
    let mut out = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        match AtUri::parse(trimmed) {
            Some(uri) => out.push(uri),
            None => eprintln!(
                "warning: seed file line {trimmed:?} is not a valid at:// post URI, skipping"
            ),
        }
    }
    out
}

/// Formats an optional score field the same way for the table and the CSV:
/// two decimal places, or `-` when the row carries no score.
fn fmt_score(value: Option<f64>) -> String {
    match value {
        Some(value) => format!("{value:.2}"),
        None => "-".to_string(),
    }
}

/// Formats an optional gate result the same way for the table and the CSV:
/// `pass`, `fail`, or `-` when the row carries no score.
fn fmt_gate(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "pass",
        Some(false) => "fail",
        None => "-",
    }
}

/// One row's nine printed columns, shared by [`render_table`] and
/// [`render_csv`] so the two can never drift apart (BC19): the two
/// `bsky.app` links (BC23), `E(Q)`, `E(O)`, `D`, `rank`, the popularity gate
/// (BC17), the margin gate (BC18), and the reason for an unscored row.
fn row_fields(row: &Row) -> [String; 9] {
    let quote_link = bsky_link(&row.quote_uri).unwrap_or_else(|| row.quote_uri.clone());
    let original_link = bsky_link(&row.original_uri).unwrap_or_else(|| row.original_uri.clone());
    [
        quote_link,
        original_link,
        fmt_score(row.eq),
        fmt_score(row.eo),
        fmt_score(row.ratio),
        fmt_score(row.rank),
        fmt_gate(row.popularity_pass).to_string(),
        fmt_gate(row.margin_pass).to_string(),
        row.reason.map(|reason| reason.as_str()).unwrap_or("").to_string(),
    ]
}

/// The header, shared by [`render_table`] and [`render_csv`], naming
/// [`row_fields`]'s nine columns in order.
const COLUMN_HEADERS: [&str; 9] =
    ["quote", "original", "E(Q)", "E(O)", "D", "rank", "popularity", "margin", "reason"];

/// Renders `rows` as a table for the engineer to read by hand, TECH-DESIGN
/// section 10 step 4. One header line, then one line per row, in the order
/// `rows` is already in: the caller sorts first with [`sort_rows`].
pub fn render_table(rows: &[Row]) -> String {
    let mut out = String::new();
    out.push_str(&COLUMN_HEADERS.join("\t"));
    out.push('\n');
    for row in rows {
        out.push_str(&row_fields(row).join("\t"));
        out.push('\n');
    }
    out
}

/// Wraps `field` in `"..."` when it holds a `,`, a `"` or a newline,
/// doubling each inner `"` (BC20). Hand-written: the spec's Non-goals rule
/// out a CSV crate for one column set this small.
fn csv_quote(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Renders `rows` as CSV, TECH-DESIGN section 10 step 4. The same rows, in
/// the same order, as [`render_table`] (BC19), one header line, and no post
/// text: [`Row`] never carries any.
pub fn render_csv(rows: &[Row]) -> String {
    let mut out = String::new();
    out.push_str(&COLUMN_HEADERS.join(","));
    out.push('\n');
    for row in rows {
        let fields = row_fields(row);
        let quoted: Vec<String> = fields.iter().map(|field| csv_quote(field)).collect();
        out.push_str(&quoted.join(","));
        out.push('\n');
    }
    out
}

/// Writes [`render_csv`]'s output to `path`. A missing parent directory or
/// any other write failure becomes `ValidateError::CsvWrite` (BC11), never a
/// panic.
pub fn write_csv(rows: &[Row], path: &Path) -> Result<(), ValidateError> {
    std::fs::write(path, render_csv(rows))
        .map_err(|source| ValidateError::CsvWrite { path: path.to_path_buf(), source })
}

/// The row BC9 prints for a `--seed-file` URI absent from the `getPosts`
/// result map: no score, reason `quote_gone`. `original_uri` is left blank:
/// without the hydrated quote post, its own embed was never read, so the
/// original is unknown too.
fn quote_gone_row(uri: &AtUri) -> Row {
    Row {
        quote_uri: uri.as_str().to_string(),
        quote_cid: String::new(),
        original_uri: String::new(),
        eq: None,
        eo: None,
        ratio: None,
        rank: None,
        popularity_pass: None,
        margin_pass: None,
        reason: Some(Reason::QuoteGone),
    }
}

/// Deduplicates `items`, keeping first-seen order (finding 3, BC26): sends
/// each distinct URI to `get_posts` once, and, in the seed-file branch,
/// prints one `quote_gone` row for a URI repeated in the seed file rather
/// than one per repetition. `key` extracts the string `get_posts` matches
/// on, since neither `String` nor `AtUri` needs to implement `Hash` for its
/// own sake elsewhere in this module.
fn dedup_first_seen<T>(items: Vec<T>, key: impl Fn(&T) -> String) -> Vec<T> {
    let mut seen = HashSet::new();
    items.into_iter().filter(|item| seen.insert(key(item))).collect()
}

/// The distinct original URIs `run` must resolve via `get_posts`, in
/// first-seen order (finding 3): two quotes of one original are sent once,
/// not twice.
fn distinct_original_uris(candidates: &[Candidate]) -> Vec<String> {
    let uris: Vec<String> = candidates
        .iter()
        .filter_map(|candidate| match candidate {
            Candidate::Scoreable { original_uri, .. } => Some(original_uri.as_str().to_string()),
            Candidate::SelfQuote { .. } => None,
        })
        .collect();
    dedup_first_seen(uris, |uri| uri.clone())
}

/// Splits already-deduplicated `seed_uris` into resolved posts and
/// [`quote_gone_row`]s (BC9), TECH-DESIGN section 10 step 1. Consumes
/// `fetched` with `remove` rather than `get` plus `clone` (finding 3), since
/// each entry is used at most once. Callers pass `seed_uris` through
/// [`dedup_first_seen`] first (finding 3, BC26): a URI repeated here would
/// be reported `quote_gone` once per repetition instead of once.
fn resolve_seed_uris(
    seed_uris: &[AtUri],
    mut fetched: HashMap<String, PostView>,
) -> (Vec<PostView>, Vec<Row>) {
    let mut found = Vec::new();
    let mut gone = Vec::new();
    for uri in seed_uris {
        match fetched.remove(uri.as_str()) {
            Some(post) => found.push(post),
            None => gone.push(quote_gone_row(uri)),
        }
    }
    (found, gone)
}

/// Pages `HOT_CLASSIC_FEED` up to `pages` times, TECH-DESIGN section 10 step
/// 1 (BC13: `pages == 0` fetches nothing), stopping early when the App View
/// returns no cursor.
async fn page_hot_classic(
    client: &AppViewClient,
    pages: u32,
) -> Result<Vec<PostView>, ValidateError> {
    let mut posts = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..pages {
        let page = client.get_feed(HOT_CLASSIC_FEED, cursor.as_deref()).await?;
        posts.extend(page.feed.into_iter().map(|item| item.post));
        cursor = page.cursor;
        if cursor.is_none() {
            break;
        }
    }
    Ok(posts)
}

/// Runs the phase 0 tool end to end, TECH-DESIGN section 10. The only
/// function in this module that touches the network or the filesystem.
///
/// `seed_file`, when it yields at least one URI, seeds from those quotes
/// instead of `hot-classic`: each is fetched with one `get_posts` call, and
/// a URI absent from the result becomes a [`quote_gone_row`] (BC9), never a
/// silent drop. An empty or fully-skipped seed file falls back to paging
/// `hot-classic`, with one note to stderr (BC7). With no seed file at all,
/// `hot-classic` is paged up to `pages` times (BC13).
///
/// Every candidate's original is resolved with one more `get_posts` call,
/// batched at 25 by `client` itself. The rows are scored, sorted, printed to
/// stdout, and written to `csv_path`.
pub async fn run(
    client: &AppViewClient,
    weights: &Weights,
    thresholds: &Thresholds,
    pages: u32,
    seed_file: Option<&Path>,
    csv_path: &Path,
) -> Result<(), ValidateError> {
    let now = Utc::now();

    let seed_uris = match seed_file {
        Some(path) => {
            let contents = std::fs::read_to_string(path)
                .map_err(|source| ValidateError::SeedFile { path: path.to_path_buf(), source })?;
            let uris = parse_seed_lines(&contents);
            if uris.is_empty() {
                eprintln!(
                    "note: seed file {path:?} yielded no valid URIs, falling back to hot-classic"
                );
            }
            dedup_first_seen(uris, |uri| uri.as_str().to_string())
        }
        None => Vec::new(),
    };

    let (candidate_list, mut rows) = if seed_uris.is_empty() {
        let posts = page_hot_classic(client, pages).await?;
        (candidates(posts), Vec::new())
    } else {
        let uri_strings: Vec<String> =
            seed_uris.iter().map(|uri| uri.as_str().to_string()).collect();
        let fetched = client.get_posts(&uri_strings).await?;
        let (found, gone) = resolve_seed_uris(&seed_uris, fetched);
        (candidates(found), gone)
    };

    let original_uris = distinct_original_uris(&candidate_list);
    let originals = client.get_posts(&original_uris).await?;

    rows.extend(build_rows(candidate_list, &originals, weights, thresholds, now));
    let rows = sort_rows(rows);

    print!("{}", render_table(&rows));
    write_csv(&rows, csv_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appview::types::{EmbedView, GetFeedResponse, PostRecord, PostViewAuthor};
    use serde_json::json;

    const HOT_CLASSIC_PAGE: &str = include_str!("../tests/fixtures/hot_classic_page.json");

    /// Builds a minimal `PostView` for a test, with a record embedding
    /// `embed` (already the record's own JSON `embed` value, or `json!(null)`
    /// for none) and the hydrated `postView.embed` given directly.
    fn post(uri: &str, cid: &str, author_did: &str, embed: serde_json::Value) -> PostView {
        post_with_counts(uri, cid, author_did, embed, 10, 2, 1)
    }

    /// [`post`], with the three engagement counts given directly, so a test
    /// can drive `E(Q)` and `E(O)` past or below a threshold.
    fn post_with_counts(
        uri: &str,
        cid: &str,
        author_did: &str,
        embed: serde_json::Value,
        like_count: u32,
        repost_count: u32,
        reply_count: u32,
    ) -> PostView {
        let rest = if embed.is_null() { json!({}) } else { json!({ "embed": embed }) };
        PostView {
            uri: uri.to_string(),
            cid: cid.to_string(),
            author: PostViewAuthor { did: author_did.to_string() },
            labels: vec![],
            record: PostRecord { created_at: "2026-01-01T00:00:00Z".to_string(), rest },
            like_count,
            repost_count,
            reply_count,
            embed: None,
        }
    }

    fn quote_embed(original_uri: &str) -> serde_json::Value {
        json!({ "$type": "app.bsky.embed.record", "record": { "uri": original_uri } })
    }

    fn quote_with_media_embed(original_uri: &str) -> serde_json::Value {
        json!({
            "$type": "app.bsky.embed.recordWithMedia",
            "record": { "record": { "uri": original_uri } }
        })
    }

    fn weights() -> Weights {
        Weights { repost: 2.0, reply: 0.5 }
    }

    fn thresholds() -> Thresholds {
        Thresholds { k: 5.0, p: 50.0, m: 1.25 }
    }

    fn now() -> DateTime<Utc> {
        "2026-01-02T00:00:00Z".parse().unwrap()
    }

    /// A client that never reaches the network in these tests: building it
    /// does no I/O, and both tests that use it below only exercise paths
    /// `run` returns from before any request goes out.
    fn test_client() -> AppViewClient {
        let lookup = |name: &str| match name {
            "DUNK_HOSTNAME" => Some("feed.example.com".to_string()),
            "DUNK_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            _ => None,
        };
        let config = crate::config::load(lookup).expect("minimal config loads");
        AppViewClient::new(&config).expect("the default rate builds a client")
    }

    #[tokio::test]
    async fn unreadable_seed_file_is_a_validate_error() {
        // BC10: `run` returns `ValidateError::SeedFile` before it ever
        // reaches the network, since the seed file is read first.
        let client = test_client();
        let err = run(
            &client,
            &weights(),
            &thresholds(),
            0,
            Some(Path::new("/nonexistent/dunk-validate-seed.txt")),
            Path::new("/tmp/dunk-validate-should-not-be-written.csv"),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ValidateError::SeedFile { .. }));
    }

    #[tokio::test]
    async fn unwritable_csv_path_is_a_validate_error() {
        // BC11: with `pages == 0` and no seed file, `candidates` and
        // `get_posts(&[])` never reach the network (the client's own
        // contract for an empty URI list), so this exercises only the CSV
        // write failure.
        let client = test_client();
        let err = run(
            &client,
            &weights(),
            &thresholds(),
            0,
            None,
            Path::new("/nonexistent-dir/dunk-validate.csv"),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ValidateError::CsvWrite { .. }));
    }

    #[test]
    fn excludes_non_quote_embeds() {
        // AC1, BC3: images, external, video, gallery, a missing embed, and
        // a quote of a non-post URI are all dropped, never turned into a
        // row.
        let images = post(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid1",
            "did:plc:a",
            json!({ "$type": "app.bsky.embed.images" }),
        );
        let external = post(
            "at://did:plc:a/app.bsky.feed.post/2",
            "cid2",
            "did:plc:a",
            json!({ "$type": "app.bsky.embed.external" }),
        );
        let video = post(
            "at://did:plc:a/app.bsky.feed.post/3",
            "cid3",
            "did:plc:a",
            json!({ "$type": "app.bsky.embed.video" }),
        );
        let gallery = post(
            "at://did:plc:a/app.bsky.feed.post/4",
            "cid4",
            "did:plc:a",
            json!({ "$type": "app.bsky.embed.gallery" }),
        );
        let no_embed =
            post("at://did:plc:a/app.bsky.feed.post/5", "cid5", "did:plc:a", json!(null));
        let non_post_uri = post(
            "at://did:plc:a/app.bsky.feed.post/6",
            "cid6",
            "did:plc:a",
            quote_embed("at://did:plc:b/app.bsky.graph.list/xyz"),
        );
        let result = candidates(vec![images, external, video, gallery, no_embed, non_post_uri]);
        assert!(result.is_empty());
    }

    #[test]
    fn a_record_with_media_quote_is_scoreable() {
        // BC2: a quote with attached media is treated the same as a plain
        // quote.
        let quote = post(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid1",
            "did:plc:a",
            quote_with_media_embed("at://did:plc:b/app.bsky.feed.post/original"),
        );
        let result = candidates(vec![quote]);
        assert_eq!(result.len(), 1);
        match &result[0] {
            Candidate::Scoreable { original_uri, .. } => {
                assert_eq!(original_uri.as_str(), "at://did:plc:b/app.bsky.feed.post/original");
            }
            other => panic!("expected Scoreable, got {other:?}"),
        }
    }

    #[test]
    fn excludes_self_quote() {
        // AC2, BC4: the quote's own author DID equals the embedded URI's
        // authority, so it is a self quote, excluded from scoring.
        let quote = post(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid1",
            "did:plc:a",
            quote_embed("at://did:plc:a/app.bsky.feed.post/original"),
        );
        let result = candidates(vec![quote]);
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Candidate::SelfQuote { .. }));

        let rows = build_rows(result, &HashMap::new(), &weights(), &thresholds(), now());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reason, Some(Reason::SelfQuote));
        assert_eq!(rows[0].rank, None);
    }

    #[test]
    fn a_quote_of_another_author_is_scoreable() {
        let quote = post(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid1",
            "did:plc:a",
            quote_embed("at://did:plc:b/app.bsky.feed.post/original"),
        );
        let result = candidates(vec![quote]);
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Candidate::Scoreable { .. }));
    }

    #[test]
    fn duplicate_quote_uri_keeps_first_occurrence() {
        // BC22.
        let first = post(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid-first",
            "did:plc:a",
            quote_embed("at://did:plc:b/app.bsky.feed.post/original"),
        );
        let duplicate = post(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid-second",
            "did:plc:a",
            quote_embed("at://did:plc:b/app.bsky.feed.post/original"),
        );
        let result = candidates(vec![first, duplicate]);
        assert_eq!(result.len(), 1);
        match &result[0] {
            Candidate::Scoreable { quote, .. } => assert_eq!(quote.cid, "cid-first"),
            other => panic!("expected Scoreable, got {other:?}"),
        }
    }

    #[test]
    fn missing_original_is_reported() {
        // AC6, BC5: the original is absent from `originals`, and the
        // quote's own hydrated embed gives no more specific reason, so the
        // row prints `original_gone`, never dropped.
        let quote = post(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid1",
            "did:plc:a",
            quote_embed("at://did:plc:b/app.bsky.feed.post/original"),
        );
        let result = candidates(vec![quote]);
        let rows = build_rows(result, &HashMap::new(), &weights(), &thresholds(), now());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reason, Some(Reason::OriginalGone));
        assert_eq!(rows[0].rank, None);
    }

    /// Builds a `Scoreable` candidate whose quote's hydrated `postView.embed`
    /// is `{"$type": "app.bsky.embed.record#view", "record": {"$type":
    /// variant_json, "uri": uri}}`, for the embed-view tests below.
    fn quote_with_hydrated_variant(uri: &str, variant_json: &str) -> PostView {
        let hydrated: EmbedView = serde_json::from_value(json!({
            "$type": "app.bsky.embed.record#view",
            "record": { "$type": variant_json, "uri": uri }
        }))
        .expect("hand-built hydrated embed decodes");
        let mut quote =
            post("at://did:plc:a/app.bsky.feed.post/1", "cid1", "did:plc:a", quote_embed(uri));
        quote.embed = Some(hydrated);
        quote
    }

    #[test]
    fn missing_original_reads_the_hydrated_embed_reason() {
        // BC21: when the quote's own hydrated `postView.embed` names the
        // original `#viewNotFound`, `#viewBlocked` or `#viewDetached`, and
        // the original is absent from `originals`, `#viewNotFound` still
        // reads as the generic `original_gone`; `#viewBlocked` and
        // `#viewDetached` get their own reasons.
        let uri = "at://did:plc:b/app.bsky.feed.post/original";
        for (variant_json, expected) in [
            ("app.bsky.embed.record#viewNotFound", Reason::OriginalGone),
            ("app.bsky.embed.record#viewBlocked", Reason::Blocked),
            ("app.bsky.embed.record#viewDetached", Reason::Detached),
        ] {
            let quote = quote_with_hydrated_variant(uri, variant_json);
            let rows = candidates(vec![quote]);
            let rows = build_rows(rows, &HashMap::new(), &weights(), &thresholds(), now());
            assert_eq!(rows[0].reason, Some(expected));
        }
    }

    #[test]
    fn detached_quote_is_not_scored() {
        // AC10, finding 1: a `#viewDetached` quote is printed with reason
        // `detached` and no score, even when the original is present in
        // `originals`: the embed-view check runs before the `getPosts`
        // lookup, so a live original never rescues a detached quote.
        let uri = "at://did:plc:b/app.bsky.feed.post/original";
        let quote = quote_with_hydrated_variant(uri, "app.bsky.embed.record#viewDetached");
        let original = post(uri, "cid-o", "did:plc:b", json!(null));
        let mut originals = HashMap::new();
        originals.insert(original.uri.clone(), original);

        let rows =
            build_rows(candidates(vec![quote]), &originals, &weights(), &thresholds(), now());
        assert_eq!(rows[0].reason, Some(Reason::Detached));
        assert_eq!(rows[0].rank, None);
    }

    #[test]
    fn other_inner_view_is_not_a_post() {
        // AC11, BC24: an inner `$type` that decodes to `RecordViewInner::Other`
        // (neither `#viewRecord`, `#viewNotFound`, `#viewBlocked` nor
        // `#viewDetached`) is printed with reason `not_a_post`, no score.
        // The quote's own record embed still names a valid post URI, so
        // `candidates` classifies it `Scoreable`; only the hydrated view's
        // inner `$type` is the unexpected one.
        let uri = "at://did:plc:b/app.bsky.feed.post/original";
        let quote = quote_with_hydrated_variant(uri, "app.bsky.embed.record#viewUnexpected");
        let rows = candidates(vec![quote]);
        let rows = build_rows(rows, &HashMap::new(), &weights(), &thresholds(), now());
        assert_eq!(rows[0].reason, Some(Reason::NotAPost));
        assert_eq!(rows[0].rank, None);
    }

    #[test]
    fn a_present_original_is_scored() {
        let quote = post(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid1",
            "did:plc:a",
            quote_embed("at://did:plc:b/app.bsky.feed.post/original"),
        );
        let original =
            post("at://did:plc:b/app.bsky.feed.post/original", "cid-o", "did:plc:b", json!(null));
        let mut originals = HashMap::new();
        originals.insert(original.uri.clone(), original);

        let rows =
            build_rows(candidates(vec![quote]), &originals, &weights(), &thresholds(), now());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reason, None);
        assert!(rows[0].rank.is_some());
        assert!(rows[0].popularity_pass.is_some());
        assert!(rows[0].margin_pass.is_some());
    }

    #[test]
    fn gates_pass_when_thresholds_are_met() {
        // BC17, BC18: `max(E(O), E(Q)) >= P` and `D >= M` both read `>=`,
        // and pass when the pair clears both.
        let quote = post_with_counts(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid1",
            "did:plc:a",
            quote_embed("at://did:plc:b/app.bsky.feed.post/original"),
            200,
            0,
            0,
        );
        let original = post_with_counts(
            "at://did:plc:b/app.bsky.feed.post/original",
            "cid-o",
            "did:plc:b",
            json!(null),
            5,
            0,
            0,
        );
        let mut originals = HashMap::new();
        originals.insert(original.uri.clone(), original);

        let rows =
            build_rows(candidates(vec![quote]), &originals, &weights(), &thresholds(), now());
        assert_eq!(rows[0].eq, Some(200.0));
        assert_eq!(rows[0].eo, Some(5.0));
        assert_eq!(rows[0].popularity_pass, Some(true));
        assert_eq!(rows[0].margin_pass, Some(true));
    }

    #[test]
    fn gates_fail_when_thresholds_are_not_met() {
        // BC17, BC18: a pair below both the popularity floor and the
        // margin multiplier fails both gates.
        let quote = post_with_counts(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid1",
            "did:plc:a",
            quote_embed("at://did:plc:b/app.bsky.feed.post/original"),
            5,
            0,
            0,
        );
        let original = post_with_counts(
            "at://did:plc:b/app.bsky.feed.post/original",
            "cid-o",
            "did:plc:b",
            json!(null),
            1,
            0,
            0,
        );
        let mut originals = HashMap::new();
        originals.insert(original.uri.clone(), original);

        let rows =
            build_rows(candidates(vec![quote]), &originals, &weights(), &thresholds(), now());
        assert_eq!(rows[0].popularity_pass, Some(false));
        assert_eq!(rows[0].margin_pass, Some(false));
    }

    #[test]
    fn sorts_by_rank_then_cid() {
        // AC3, BC14, BC15: rank descending; ties, and every unscored row,
        // sort by `quote_cid` ascending.
        let scored_low = Row {
            quote_uri: "q1".into(),
            quote_cid: "b".into(),
            original_uri: "o1".into(),
            eq: Some(1.0),
            eo: Some(1.0),
            ratio: Some(1.0),
            rank: Some(1.0),
            popularity_pass: Some(true),
            margin_pass: Some(true),
            reason: None,
        };
        let scored_high = Row {
            quote_uri: "q2".into(),
            quote_cid: "a".into(),
            original_uri: "o2".into(),
            eq: Some(1.0),
            eo: Some(1.0),
            ratio: Some(1.0),
            rank: Some(5.0),
            popularity_pass: Some(true),
            margin_pass: Some(true),
            reason: None,
        };
        let tie_a = Row {
            quote_uri: "q3".into(),
            quote_cid: "z".into(),
            original_uri: "o3".into(),
            eq: Some(1.0),
            eo: Some(1.0),
            ratio: Some(1.0),
            rank: Some(2.0),
            popularity_pass: Some(true),
            margin_pass: Some(true),
            reason: None,
        };
        let tie_b = Row {
            quote_uri: "q4".into(),
            quote_cid: "y".into(),
            original_uri: "o4".into(),
            eq: Some(1.0),
            eo: Some(1.0),
            ratio: Some(1.0),
            rank: Some(2.0),
            popularity_pass: Some(true),
            margin_pass: Some(true),
            reason: None,
        };
        let unscored_a = Row {
            quote_uri: "q5".into(),
            quote_cid: "n".into(),
            original_uri: "o5".into(),
            eq: None,
            eo: None,
            ratio: None,
            rank: None,
            popularity_pass: None,
            margin_pass: None,
            reason: Some(Reason::OriginalGone),
        };
        let unscored_b = Row {
            quote_uri: "q6".into(),
            quote_cid: "m".into(),
            original_uri: "o6".into(),
            eq: None,
            eo: None,
            ratio: None,
            rank: None,
            popularity_pass: None,
            margin_pass: None,
            reason: Some(Reason::SelfQuote),
        };

        let sorted = sort_rows(vec![scored_low, unscored_a, tie_a, scored_high, unscored_b, tie_b]);

        let cids: Vec<&str> = sorted.iter().map(|row| row.quote_cid.as_str()).collect();
        // scored_high (rank 5) first, then the tie broken by quote_cid
        // ascending ("y" before "z"), then scored_low (rank 1), then the
        // two unscored rows, quote_cid ascending ("m" before "n").
        assert_eq!(cids, vec!["a", "y", "z", "b", "m", "n"]);
    }

    #[test]
    fn age_hours_is_zero_when_created_at_does_not_parse() {
        // BC16.
        assert_eq!(age_hours("not-a-timestamp", now()), 0.0);
    }

    #[test]
    fn age_hours_is_zero_when_created_at_is_in_the_future() {
        // BC16.
        assert_eq!(age_hours("2026-01-03T00:00:00Z", now()), 0.0);
    }

    #[test]
    fn age_hours_computes_the_gap_in_hours() {
        assert_eq!(age_hours("2026-01-01T12:00:00Z", now()), 12.0);
    }

    #[test]
    fn bsky_link_builds_the_profile_post_url() {
        // BC23.
        let link = bsky_link("at://did:plc:abc/app.bsky.feed.post/xyz").unwrap();
        assert_eq!(link, "https://bsky.app/profile/did:plc:abc/post/xyz");
    }

    #[test]
    fn bsky_link_is_none_for_a_malformed_uri() {
        assert_eq!(bsky_link("not-a-uri"), None);
    }

    #[test]
    fn fixture_page_yields_candidates() {
        // AC7, finding 4: `hot_classic_page.json` is a 9-post recording
        // trimmed from one real `getFeed(hot-classic)` page, kept to a
        // plain record quote, a recordWithMedia quote, a self quote (also
        // recordWithMedia), an images embed, an external embed, a video
        // embed, a gallery embed, and a post with no embed at all. Of the
        // 9, 4 are quotes: 3 `Scoreable` and 1 `SelfQuote`.
        let decoded: GetFeedResponse =
            serde_json::from_str(HOT_CLASSIC_PAGE).expect("fixture decodes as GetFeedResponse");
        let posts: Vec<PostView> = decoded.feed.into_iter().map(|item| item.post).collect();
        assert_eq!(posts.len(), 9);
        let result = candidates(posts);
        assert_eq!(result.len(), 4);
        let scoreable = result.iter().filter(|c| matches!(c, Candidate::Scoreable { .. })).count();
        let self_quote = result.iter().filter(|c| matches!(c, Candidate::SelfQuote { .. })).count();
        assert_eq!(scoreable, 3);
        assert_eq!(self_quote, 1);
    }

    #[test]
    fn dedup_first_seen_keeps_first_occurrence() {
        // Finding 3: a duplicate key keeps its first occurrence and drops
        // the rest, same order as the input.
        let result = dedup_first_seen(
            vec!["a".to_string(), "b".to_string(), "a".to_string(), "c".to_string()],
            |s| s.clone(),
        );
        assert_eq!(result, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }

    #[test]
    fn distinct_original_uris_deduplicates_two_quotes_of_one_original() {
        // Finding 3: two `Scoreable` candidates quoting the same original
        // are sent to `get_posts` as one URI, not two.
        let original_uri = AtUri::parse("at://did:plc:b/app.bsky.feed.post/original").unwrap();
        let quote_a = post(
            "at://did:plc:a/app.bsky.feed.post/1",
            "cid1",
            "did:plc:a",
            quote_embed(original_uri.as_str()),
        );
        let quote_b = post(
            "at://did:plc:c/app.bsky.feed.post/2",
            "cid2",
            "did:plc:c",
            quote_embed(original_uri.as_str()),
        );
        let list = candidates(vec![quote_a, quote_b]);
        assert_eq!(list.len(), 2);
        assert_eq!(distinct_original_uris(&list), vec![original_uri.as_str().to_string()]);
    }

    #[test]
    fn resolve_seed_uris_reports_one_quote_gone_row_for_a_repeated_missing_uri() {
        // Finding 3, BC26: a seed URI repeated in the (already deduplicated)
        // seed list is looked up once and, when missing, printed as one
        // `quote_gone` row, not one per repetition.
        let uri = AtUri::parse("at://did:plc:a/app.bsky.feed.post/missing").unwrap();
        let deduped = dedup_first_seen(vec![uri.clone(), uri.clone()], |u| u.as_str().to_string());
        assert_eq!(deduped.len(), 1);

        let (found, gone) = resolve_seed_uris(&deduped, HashMap::new());
        assert!(found.is_empty());
        assert_eq!(gone.len(), 1);
        assert_eq!(gone[0].reason, Some(Reason::QuoteGone));
    }

    #[test]
    fn resolve_seed_uris_removes_found_posts_from_fetched() {
        // Finding 3: a found URI is taken out of `fetched` with `remove`,
        // not cloned out with `get`.
        let uri = AtUri::parse("at://did:plc:a/app.bsky.feed.post/found").unwrap();
        let post = post(uri.as_str(), "cid1", "did:plc:a", json!(null));
        let mut fetched = HashMap::new();
        fetched.insert(uri.as_str().to_string(), post);

        let (found, gone) = resolve_seed_uris(&[uri], fetched);
        assert_eq!(found.len(), 1);
        assert!(gone.is_empty());
    }

    /// A minimal scored or unscored row, distinguished by `rkey`, which
    /// [`csv_matches_table`] reads back out of the rendered link to check
    /// ordering.
    fn row_stub(rkey: &str, rank: Option<f64>) -> Row {
        Row {
            quote_uri: format!("at://did:plc:a/app.bsky.feed.post/{rkey}"),
            quote_cid: rkey.to_string(),
            original_uri: "at://did:plc:b/app.bsky.feed.post/original".to_string(),
            eq: rank.map(|_| 10.0),
            eo: rank.map(|_| 5.0),
            ratio: rank,
            rank,
            popularity_pass: rank.map(|_| true),
            margin_pass: rank.map(|_| true),
            reason: if rank.is_none() { Some(Reason::OriginalGone) } else { None },
        }
    }

    #[test]
    fn skips_malformed_seed_line() {
        // AC5, BC6: a malformed line is skipped, with the rest of the file
        // still parsed.
        let input = "at://did:plc:a/app.bsky.feed.post/good\n\
                      not-a-uri\n\
                      at://did:plc:b/app.bsky.feed.post/also-good\n";
        let uris = parse_seed_lines(input);
        assert_eq!(uris.len(), 2);
        assert_eq!(uris[0].as_str(), "at://did:plc:a/app.bsky.feed.post/good");
        assert_eq!(uris[1].as_str(), "at://did:plc:b/app.bsky.feed.post/also-good");
    }

    #[test]
    fn skips_blank_and_comment_lines_in_silence() {
        // BC8.
        let input = "\n# a comment\n   \nat://did:plc:a/app.bsky.feed.post/good\n";
        let uris = parse_seed_lines(input);
        assert_eq!(uris.len(), 1);
        assert_eq!(uris[0].as_str(), "at://did:plc:a/app.bsky.feed.post/good");
    }

    #[test]
    fn empty_or_fully_skipped_seed_file_yields_no_uris() {
        // BC7: `run` reads this as the signal to fall back to `hot-classic`.
        assert!(parse_seed_lines("").is_empty());
        assert!(parse_seed_lines("# only comments\n\n   \n").is_empty());
    }

    #[test]
    fn quote_gone_row_carries_no_score() {
        // BC9: a seed URI absent from `getPosts` becomes a row with reason
        // `quote_gone`, never a silent drop.
        let uri = AtUri::parse("at://did:plc:a/app.bsky.feed.post/gone").unwrap();
        let row = quote_gone_row(&uri);
        assert_eq!(row.quote_uri, "at://did:plc:a/app.bsky.feed.post/gone");
        assert_eq!(row.reason, Some(Reason::QuoteGone));
        assert_eq!(row.rank, None);
    }

    #[test]
    fn csv_quoting_wraps_fields_that_need_it() {
        // BC20.
        assert_eq!(csv_quote("plain"), "plain");
        assert_eq!(csv_quote("has,comma"), "\"has,comma\"");
        assert_eq!(csv_quote("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(csv_quote("has\nnewline"), "\"has\nnewline\"");
    }

    #[test]
    fn csv_matches_table() {
        // AC4, BC19: the CSV's rows appear in the same order as the printed
        // table, and the CSV carries no post text (there is none to carry:
        // `Row` never holds any).
        let rows = sort_rows(vec![
            row_stub("low", Some(1.0)),
            row_stub("high", Some(5.0)),
            row_stub("unscored", None),
        ]);
        let table = render_table(&rows);
        let csv = render_csv(&rows);

        let rkey_of = |link: &str| link.rsplit('/').next().unwrap().to_string();
        let table_rkeys: Vec<String> =
            table.lines().skip(1).map(|line| rkey_of(line.split('\t').next().unwrap())).collect();
        let csv_rkeys: Vec<String> =
            csv.lines().skip(1).map(|line| rkey_of(line.split(',').next().unwrap())).collect();

        assert_eq!(table_rkeys, vec!["high", "low", "unscored"]);
        assert_eq!(csv_rkeys, table_rkeys);
    }
}

/// Live tests against `https://public.api.bsky.app`, `#[ignore]`d so
/// `cargo test --all-features` never touches the network, run by hand with
/// `cargo test --all-features -- --ignored`. Mirrors `appview`'s own live
/// tests, and the spec's own answer that `validate` needs no database: only
/// `DUNK_HOSTNAME` and `DUNK_PUBLISHER_DID` are set.
#[cfg(test)]
mod live_tests {
    use super::*;
    use crate::appview::AppViewClient;

    #[tokio::test]
    #[ignore]
    async fn validate_live_hot_classic() {
        // AC9: a live run against `hot-classic` prints a table and writes a
        // CSV with the expected header.
        let lookup = |name: &str| match name {
            "DUNK_HOSTNAME" => Some("feed.example.com".to_string()),
            "DUNK_PUBLISHER_DID" => Some("did:plc:z72i7hdynmk6r22z27h6tvur".to_string()),
            _ => None,
        };
        let config = crate::config::load(lookup).expect("minimal config loads");
        let client = AppViewClient::new(&config).expect("the default rate builds a client");
        let weights = Weights::from(&config);
        let thresholds = Thresholds::from(&config);
        let csv_path = std::env::temp_dir().join("dunk-validate-live-test.csv");

        run(&client, &weights, &thresholds, 1, None, &csv_path)
            .await
            .expect("live run against hot-classic");

        let written = std::fs::read_to_string(&csv_path).expect("csv was written");
        assert!(written.starts_with("quote,original,E(Q),E(O),D,rank,popularity,margin,reason"));
        let _ = std::fs::remove_file(&csv_path);
    }
}
