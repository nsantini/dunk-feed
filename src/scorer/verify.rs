//! Verifies one candidate pair against a `getPosts` response, TECH-DESIGN
//! section 8.2 and 8.3. `verify_pair` is pure: it only reads the `posts`
//! map `verify_all` (slice 2.0) builds from one or more chunked
//! `get_posts` calls, and the pair's own stored identity. It never calls
//! the network itself and never touches `Store`.

use std::collections::HashMap;

use crate::appview::types::{classify_quote_embed, PostView, QuoteEmbed};
use crate::score::Counts;
use crate::store::DropReason;

/// One pair with `Q` and `O` both verified against the App View. Carries
/// the pair's stored identity (`quote_uri`, `original_uri`, `quoted_at`,
/// unchanged from the caller) plus what verification adds: each side's
/// verified `Counts` (BC27's mapping) and author DID, and `Q`'s `cid` off
/// the hydrated view. `promote_or_drop` (slice 2.0) reads this, never a
/// local `PairWithCounts`, once a pair has cleared `verify_pair`.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedPair {
    pub quote_uri: String,
    pub quote_cid: String,
    pub quote_did: String,
    pub original_uri: String,
    pub original_did: String,
    pub quoted_at: i64,
    pub counts_q: Counts,
    pub counts_o: Counts,
    /// `Q`'s own `postView.labels[].val`, story 10's label guard (BC9,
    /// BC37). Empty, not absent, when the App View sends no `labels` key:
    /// `PostView.labels` already defaults to empty.
    pub labels_q: Vec<String>,
    /// `O`'s own `postView.labels[].val`, story 10's label guard (BC10,
    /// BC37).
    pub labels_o: Vec<String>,
}

/// The result of verifying one pair, TECH-DESIGN section 8.2 and 8.3.
/// `Continue` carries what promote needs; `Drop` carries the reason
/// `Store::drop_pair` writes to `pairs.drop_reason`.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Continue(VerifiedPair),
    Drop(DropReason),
}

/// Reads `Q`'s embed off `posts`' merged `getPosts` map. `original_uri` is
/// the pair's own stored value: this function checks the embedded
/// `#viewRecord.uri` against it (BC14) rather than trusting the merged
/// map's key for `O`, since a stale `original_uri` and a genuine App View
/// response can disagree.
///
/// Order follows TECH-DESIGN section 8.2 and section 7.2 step 4 top to
/// bottom: a missing URI first (BC12, BC13), then `Q`'s embed shape (BC6 to
/// BC11), then the embedded-URI cross-check (BC14), then the self-quote
/// check (BC15). `quoted_at` is carried through unchanged into a
/// `Continue` verdict; this function never reads the clock.
pub fn verify_pair(
    quote_uri: &str,
    original_uri: &str,
    quoted_at: i64,
    posts: &HashMap<String, PostView>,
) -> Verdict {
    let Some(q) = posts.get(quote_uri) else {
        return Verdict::Drop(DropReason::QuoteGone);
    };
    let Some(o) = posts.get(original_uri) else {
        return Verdict::Drop(DropReason::OriginalGone);
    };

    let embedded_uri = match classify_quote_embed(q) {
        QuoteEmbed::Normal { uri } => uri,
        QuoteEmbed::Detached => return Verdict::Drop(DropReason::Detached),
        QuoteEmbed::Blocked => return Verdict::Drop(DropReason::Blocked),
        QuoteEmbed::NotFound => return Verdict::Drop(DropReason::OriginalGone),
        QuoteEmbed::NotAPost | QuoteEmbed::Absent => return Verdict::Drop(DropReason::NotAPost),
    };

    if embedded_uri != original_uri {
        return Verdict::Drop(DropReason::OriginalGone);
    }

    if q.author.did == o.author.did {
        return Verdict::Drop(DropReason::SelfQuote);
    }

    Verdict::Continue(VerifiedPair {
        quote_uri: q.uri.clone(),
        quote_cid: q.cid.clone(),
        quote_did: q.author.did.clone(),
        original_uri: o.uri.clone(),
        original_did: o.author.did.clone(),
        quoted_at,
        counts_q: Counts::from(q),
        counts_o: Counts::from(o),
        labels_q: q.labels.iter().map(|l| l.val.clone()).collect(),
        labels_o: o.labels.iter().map(|l| l.val.clone()).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appview::types::{
        EmbedRecordViewRecord, EmbedView, GetPostsResponse, PostRecord, PostViewAuthor,
        RecordViewInner, RecordWithMediaInner,
    };

    const GETPOSTS_NORMAL_QUOTE: &str =
        include_str!("../../tests/fixtures/getposts_normal_quote.json");
    const GETPOSTS_RECORD_WITH_MEDIA: &str =
        include_str!("../../tests/fixtures/getposts_record_with_media.json");
    const GETPOSTS_VIEW_DETACHED: &str =
        include_str!("../../tests/fixtures/getposts_view_detached.json");
    const GETPOSTS_VIEW_BLOCKED: &str =
        include_str!("../../tests/fixtures/getposts_view_blocked.json");
    const GETPOSTS_VIEW_NOT_FOUND: &str =
        include_str!("../../tests/fixtures/getposts_view_not_found.json");

    /// Builds a minimal `PostView` for the tests below that do not read
    /// from a recorded fixture: the missing-URI, cross-check and
    /// self-quote cases have no natural fixture (no live post was found in
    /// that state to record), so they are built by hand, the same pattern
    /// `score.rs`'s `counts_from_post_view` test already uses.
    fn post(uri: &str, did: &str, embed: Option<EmbedView>) -> PostView {
        PostView {
            uri: uri.to_string(),
            cid: format!("cid-{uri}"),
            author: PostViewAuthor { did: did.to_string() },
            labels: vec![],
            record: PostRecord {
                created_at: "2026-01-01T00:00:00Z".to_string(),
                rest: serde_json::json!({}),
            },
            like_count: 10,
            repost_count: 1,
            reply_count: 1,
            embed,
        }
    }

    fn map(posts: Vec<PostView>) -> HashMap<String, PostView> {
        posts.into_iter().map(|p| (p.uri.clone(), p)).collect()
    }

    fn view_record_embed(uri: &str) -> EmbedView {
        EmbedView::Record {
            record: RecordViewInner::ViewRecord(EmbedRecordViewRecord { uri: uri.to_string() }),
        }
    }

    fn posts_from_fixture(fixture: &str) -> HashMap<String, PostView> {
        let decoded: GetPostsResponse = serde_json::from_str(fixture).expect("fixture decodes");
        map(decoded.posts)
    }

    // BC6: recorded live 2026-09-21 from `public.api.bsky.app`, the
    // TECH-DESIGN section 1 reference pair — a normal
    // `app.bsky.embed.record#view` quote. Both `Q` and `O` are in the same
    // response, so this is also the happy-path end-to-end test.
    #[test]
    fn continues_on_normal_quote() {
        let posts = posts_from_fixture(GETPOSTS_NORMAL_QUOTE);
        let quote_uri = "at://did:plc:o7xt7svg2xtjbb4e2xqahqqc/app.bsky.feed.post/3mvxhe7uuck2n";
        let original_uri = "at://did:plc:ofzkhjyyh4kl4a35wxgmobmm/app.bsky.feed.post/3mvxb5n76u22b";

        let verdict = verify_pair(quote_uri, original_uri, 1_758_000_000, &posts);

        match verdict {
            Verdict::Continue(verified) => {
                assert_eq!(verified.quote_uri, quote_uri);
                assert_eq!(verified.original_uri, original_uri);
                assert_eq!(verified.quoted_at, 1_758_000_000);
                assert_eq!(verified.quote_did, "did:plc:o7xt7svg2xtjbb4e2xqahqqc");
                assert_eq!(verified.original_did, "did:plc:ofzkhjyyh4kl4a35wxgmobmm");
                assert_eq!(verified.counts_q, Counts { likes: 514, reposts: 101, replies: 6 });
                assert_eq!(verified.counts_o, Counts { likes: 125, reposts: 17, replies: 2 });
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    // BC7: `getposts_record_with_media.json` is hand-made (no live post
    // with this exact shape was found to record), a normal quote with
    // media nested under `recordWithMedia#view`. Only `Q` is in the
    // fixture, so `O` is built by hand and inserted alongside it.
    #[test]
    fn continues_on_record_with_media() {
        let mut posts = posts_from_fixture(GETPOSTS_RECORD_WITH_MEDIA);
        let quote_uri = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3mv45zmynys2l";
        let original_uri = "at://did:plc:plcl43vt7d2ig7hif4zmyg6h/app.bsky.feed.post/3mv45xxdbx22k";
        posts.insert(
            original_uri.to_string(),
            post(original_uri, "did:plc:plcl43vt7d2ig7hif4zmyg6h", None),
        );

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert!(matches!(verdict, Verdict::Continue(_)), "expected Continue, got {verdict:?}");
    }

    // BC8: `getposts_view_detached.json` is hand-edited from a recorded
    // `getPosts` body (no live detached quote was found).
    #[test]
    fn drops_detached() {
        let mut posts = posts_from_fixture(GETPOSTS_VIEW_DETACHED);
        let quote_uri = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3mv45zmynys2l";
        let original_uri = "at://did:plc:plcl43vt7d2ig7hif4zmyg6h/app.bsky.feed.post/3mv45xxdbx22k";
        posts.insert(
            original_uri.to_string(),
            post(original_uri, "did:plc:plcl43vt7d2ig7hif4zmyg6h", None),
        );

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::Detached));
    }

    // BC9: `getposts_view_blocked.json` is hand-edited from a recorded
    // `getPosts` body (no live blocked quote was found).
    #[test]
    fn drops_blocked() {
        let mut posts = posts_from_fixture(GETPOSTS_VIEW_BLOCKED);
        let quote_uri = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3mv45zmynys2l";
        let original_uri = "at://did:plc:plcl43vt7d2ig7hif4zmyg6h/app.bsky.feed.post/3mv45xxdbx22k";
        posts.insert(
            original_uri.to_string(),
            post(original_uri, "did:plc:plcl43vt7d2ig7hif4zmyg6h", None),
        );

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::Blocked));
    }

    // BC10: `getposts_view_not_found.json` is hand-edited from a recorded
    // `getPosts` body (no live not-found quote was found).
    #[test]
    fn drops_original_gone_on_view_not_found() {
        let mut posts = posts_from_fixture(GETPOSTS_VIEW_NOT_FOUND);
        let quote_uri = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3mv45zmynys2l";
        let original_uri = "at://did:plc:plcl43vt7d2ig7hif4zmyg6h/app.bsky.feed.post/3mv45xxdbx22k";
        posts.insert(
            original_uri.to_string(),
            post(original_uri, "did:plc:plcl43vt7d2ig7hif4zmyg6h", None),
        );

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::OriginalGone));
    }

    // BC7 and BC8 together: a `recordWithMedia#view` whose inner
    // `$type` is `#viewDetached` still drops `detached`, the same as the
    // plain-record case, not `not_a_post`.
    #[test]
    fn drops_detached_through_record_with_media() {
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let embed = EmbedView::RecordWithMedia {
            record: RecordWithMediaInner {
                record: RecordViewInner::ViewDetached { uri: original_uri.to_string() },
            },
        };
        let posts = map(vec![
            post(quote_uri, "did:plc:quoter", Some(embed)),
            post(original_uri, "did:plc:original", None),
        ]);

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::Detached));
    }

    // BC7 and BC9 together: a `recordWithMedia#view` whose inner `$type`
    // is `#viewBlocked` drops `blocked`.
    #[test]
    fn drops_blocked_through_record_with_media() {
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let embed = EmbedView::RecordWithMedia {
            record: RecordWithMediaInner {
                record: RecordViewInner::ViewBlocked { uri: original_uri.to_string() },
            },
        };
        let posts = map(vec![
            post(quote_uri, "did:plc:quoter", Some(embed)),
            post(original_uri, "did:plc:original", None),
        ]);

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::Blocked));
    }

    // BC11: an embed `$type` this crate does not name at all decodes into
    // `EmbedView::Other`.
    #[test]
    fn drops_not_a_post_on_other_embed_type() {
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let posts = map(vec![
            post(quote_uri, "did:plc:quoter", Some(EmbedView::Other)),
            post(original_uri, "did:plc:original", None),
        ]);

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::NotAPost));
    }

    // BC11: `embed` absent altogether is not a post quote either.
    #[test]
    fn drops_not_a_post_on_absent_embed() {
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let posts = map(vec![
            post(quote_uri, "did:plc:quoter", None),
            post(original_uri, "did:plc:original", None),
        ]);

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::NotAPost));
    }

    // BC11: the inner `record.record.$type` of a `recordWithMedia#view` can
    // be `Other` too, and drops the same way as the plain-record case.
    #[test]
    fn drops_not_a_post_on_record_with_media_other_inner() {
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let embed = EmbedView::RecordWithMedia {
            record: RecordWithMediaInner { record: RecordViewInner::Other },
        };
        let posts = map(vec![
            post(quote_uri, "did:plc:quoter", Some(embed)),
            post(original_uri, "did:plc:original", None),
        ]);

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::NotAPost));
    }

    // BC12: `Q`'s URI is simply absent from the merged `getPosts` map.
    #[test]
    fn drops_quote_gone_when_quote_uri_missing() {
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let posts = map(vec![post(original_uri, "did:plc:original", None)]);

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::QuoteGone));
    }

    // BC13: `O`'s URI is absent from the merged map, even though `Q`
    // resolved fine.
    #[test]
    fn drops_original_gone_when_original_uri_missing() {
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let posts =
            map(vec![post(quote_uri, "did:plc:quoter", Some(view_record_embed(original_uri)))]);

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::OriginalGone));
    }

    // BC14: the embedded `#viewRecord.uri` disagrees with the pair's
    // stored `original_uri` — `Q` no longer embeds what the row says it
    // does.
    #[test]
    fn drops_original_gone_when_embedded_uri_disagrees_with_stored() {
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let stored_original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let actually_embedded_uri = "at://did:plc:someone-else/app.bsky.feed.post/other";
        let posts = map(vec![
            post(quote_uri, "did:plc:quoter", Some(view_record_embed(actually_embedded_uri))),
            post(stored_original_uri, "did:plc:original", None),
            post(actually_embedded_uri, "did:plc:someone-else", None),
        ]);

        let verdict = verify_pair(quote_uri, stored_original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::OriginalGone));
    }

    // BC15: both verified author DIDs match, so `Q` quotes its own
    // author's post.
    #[test]
    fn drops_self_quote_when_authors_match() {
        let quote_uri = "at://did:plc:same/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:same/app.bsky.feed.post/o";
        let posts = map(vec![
            post(quote_uri, "did:plc:same", Some(view_record_embed(original_uri))),
            post(original_uri, "did:plc:same", None),
        ]);

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        assert_eq!(verdict, Verdict::Drop(DropReason::SelfQuote));
    }

    // A normal quote whose authors differ, built entirely by hand, checks
    // the happy path does not accidentally trip the self-quote check and
    // that `Counts::from` (BC27) reaches `VerifiedPair` unmodified.
    #[test]
    fn continues_and_carries_counts_when_hand_built() {
        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let mut q = post(quote_uri, "did:plc:quoter", Some(view_record_embed(original_uri)));
        q.like_count = 200;
        q.repost_count = 40;
        q.reply_count = 3;
        let mut o = post(original_uri, "did:plc:original", None);
        o.like_count = 60;
        o.repost_count = 5;
        o.reply_count = 1;
        let posts = map(vec![q, o]);

        let verdict = verify_pair(quote_uri, original_uri, 42, &posts);

        match verdict {
            Verdict::Continue(verified) => {
                assert_eq!(verified.counts_q, Counts { likes: 200, reposts: 40, replies: 3 });
                assert_eq!(verified.counts_o, Counts { likes: 60, reposts: 5, replies: 1 });
                assert_eq!(verified.quoted_at, 42);
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    // BC37: each side's `postView.labels[].val` lands on `VerifiedPair`
    // unmodified; a post with no `labels` key decodes to an empty vector via
    // `PostView`'s own `#[serde(default)]`, not an error, so `labels_q` and
    // `labels_o` are simply empty rather than absent.
    #[test]
    fn carries_labels_onto_verified_pair() {
        use crate::appview::types::Label;

        let quote_uri = "at://did:plc:quoter/app.bsky.feed.post/q";
        let original_uri = "at://did:plc:original/app.bsky.feed.post/o";
        let mut q = post(quote_uri, "did:plc:quoter", Some(view_record_embed(original_uri)));
        q.labels = vec![Label { val: "spam".to_string() }];
        let o = post(original_uri, "did:plc:original", None);
        let posts = map(vec![q, o]);

        let verdict = verify_pair(quote_uri, original_uri, 0, &posts);

        match verdict {
            Verdict::Continue(verified) => {
                assert_eq!(verified.labels_q, vec!["spam".to_string()]);
                assert!(verified.labels_o.is_empty());
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }
}
