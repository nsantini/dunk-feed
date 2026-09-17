//! `dunk validate` phase 0 tool, TECH-DESIGN section 10. Every decision here
//! is a pure function over values already in memory: seeding, the quote
//! filter, scoring, and row ordering. Only `run` (wired by the next slice)
//! touches the network or the filesystem, so this module's fixture tests
//! cover the whole decision path with no network.
//!
//! `candidates` reuses `ingest::embed::detect` on the quote's own
//! `post.record`, never a second detector, and `build_rows` reuses
//! `score.rs`'s `Counts`, `engagement`, `ratio` and `rank` directly, never a
//! second formula; it reads the popularity and margin gates off the same
//! `Thresholds` `score::qualifies` uses, but separately, since the table
//! prints each gate on its own (BC17, BC18), not only their conjunction.

#![allow(dead_code)] // First caller is `dunk validate`'s CLI wiring, the next slice.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};

use crate::appview::types::{EmbedView, PostView, RecordViewInner};
use crate::ingest::embed::{self, AtUri, Embed};
use crate::score::{self, Counts, Thresholds, Weights};

/// The feed seeded when no `--seed-file` is given or it yields no
/// candidates. TECH-DESIGN section 4 lists no `Config` variable for it, so
/// it is a constant here, not a field on `Config`.
pub const HOT_CLASSIC_FEED: &str =
    "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/hot-classic";

/// Why a row carries no score, printed in place of its numbers. Every
/// variant maps to the exact reason string TECH-DESIGN section 10 and the
/// spec's behaviour contracts name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// BC4: the quote and the original share one author.
    SelfQuote,
    /// BC5: the original is absent from the `getPosts` result map, and the
    /// quote's own hydrated embed gives no more specific reason.
    OriginalGone,
    /// BC21: the quote's hydrated `postView.embed` names the original
    /// `#viewNotFound`.
    OriginalNotFound,
    /// BC21: the quote's hydrated `postView.embed` names the original
    /// `#viewBlocked`.
    OriginalBlocked,
    /// BC21: the quote's hydrated `postView.embed` names the original
    /// `#viewDetached`.
    OriginalDetached,
}

impl Reason {
    /// The exact string the table and the CSV both print for this reason.
    pub fn as_str(&self) -> &'static str {
        match self {
            Reason::SelfQuote => "self_quote",
            Reason::OriginalGone => "original_gone",
            Reason::OriginalNotFound => "original_not_found",
            Reason::OriginalBlocked => "original_blocked",
            Reason::OriginalDetached => "original_detached",
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
        if original_uri.did() == quote.author.did {
            out.push(Candidate::SelfQuote { quote, original_uri });
        } else {
            out.push(Candidate::Scoreable { quote, original_uri });
        }
    }
    out
}

/// The reason BC21 assigns when the original is absent from `originals`:
/// the quote's own hydrated `postView.embed` names why, when it can, and
/// falls back to the generic `OriginalGone` (BC5) otherwise.
fn reason_for_missing_original(quote: &PostView) -> Reason {
    let inner = match &quote.embed {
        Some(EmbedView::Record { record }) => Some(record),
        Some(EmbedView::RecordWithMedia { record }) => Some(&record.record),
        _ => None,
    };
    match inner {
        Some(RecordViewInner::ViewNotFound { .. }) => Reason::OriginalNotFound,
        Some(RecordViewInner::ViewBlocked { .. }) => Reason::OriginalBlocked,
        Some(RecordViewInner::ViewDetached { .. }) => Reason::OriginalDetached,
        _ => Reason::OriginalGone,
    }
}

/// Builds one [`Row`] per candidate, TECH-DESIGN section 10 steps 3 and 4.
/// A `SelfQuote` candidate never reaches `score.rs`: it is printed with
/// [`Reason::SelfQuote`] and no score (BC4). A `Scoreable` candidate whose
/// original is missing from `originals` is printed with the reason
/// [`reason_for_missing_original`] gives, never dropped (BC5, BC21). A
/// `Scoreable` candidate whose original is present scores through
/// `score.rs`'s own `Counts`, `engagement`, `ratio` and `rank`, with
/// `age_hours` clamped at zero (BC16) and the two gates (BC17, BC18) read
/// off `thresholds` directly rather than through `score::qualifies`, which
/// only returns their conjunction.
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
            Candidate::SelfQuote { quote, original_uri } => Row {
                quote_uri: quote.uri.clone(),
                quote_cid: quote.cid.clone(),
                original_uri: original_uri.as_str().to_string(),
                eq: None,
                eo: None,
                ratio: None,
                rank: None,
                popularity_pass: None,
                margin_pass: None,
                reason: Some(Reason::SelfQuote),
            },
            Candidate::Scoreable { quote, original_uri } => {
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
                            popularity_pass: Some(eo.max(eq) >= thresholds.p),
                            margin_pass: Some(d >= thresholds.m),
                            reason: None,
                        }
                    }
                    None => {
                        let reason = reason_for_missing_original(&quote);
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
    let parsed = DateTime::parse_from_rfc3339(created_at).map(|dt| dt.with_timezone(&Utc));
    let hours = match parsed {
        Ok(created_at) => {
            let seconds = now.signed_duration_since(created_at).num_seconds();
            seconds as f64 / 3600.0
        }
        Err(err) => {
            eprintln!("warning: quote createdAt {created_at:?} does not parse as RFC 3339: {err}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appview::types::{GetFeedResponse, PostRecord, PostViewAuthor};
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

    #[test]
    fn missing_original_reads_the_hydrated_embed_reason() {
        // BC21: when the quote's own hydrated `postView.embed` names the
        // original `#viewNotFound`, `#viewBlocked` or `#viewDetached`, that
        // reason is used instead of the generic `original_gone`.
        for (variant_json, expected) in [
            ("app.bsky.embed.record#viewNotFound", Reason::OriginalNotFound),
            ("app.bsky.embed.record#viewBlocked", Reason::OriginalBlocked),
            ("app.bsky.embed.record#viewDetached", Reason::OriginalDetached),
        ] {
            let uri = "at://did:plc:b/app.bsky.feed.post/original";
            let hydrated: EmbedView = serde_json::from_value(json!({
                "$type": "app.bsky.embed.record#view",
                "record": { "$type": variant_json, "uri": uri }
            }))
            .expect("hand-built hydrated embed decodes");
            let mut quote =
                post("at://did:plc:a/app.bsky.feed.post/1", "cid1", "did:plc:a", quote_embed(uri));
            quote.embed = Some(hydrated);
            let rows = candidates(vec![quote]);
            let rows = build_rows(rows, &HashMap::new(), &weights(), &thresholds(), now());
            assert_eq!(rows[0].reason, Some(expected));
        }
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
        // AC7: the recorded `hot_classic_page.json` fixture parses and
        // yields at least one candidate, including at least one self quote,
        // confirmed live in the page recorded for this fixture.
        let decoded: GetFeedResponse =
            serde_json::from_str(HOT_CLASSIC_PAGE).expect("fixture decodes as GetFeedResponse");
        let posts: Vec<PostView> = decoded.feed.into_iter().map(|item| item.post).collect();
        assert!(!posts.is_empty());
        let result = candidates(posts);
        assert!(!result.is_empty());
        assert!(result.iter().any(|c| matches!(c, Candidate::Scoreable { .. })));
        assert!(result.iter().any(|c| matches!(c, Candidate::SelfQuote { .. })));
    }
}
