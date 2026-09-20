//! `serde` types for the App View responses, TECH-DESIGN section 8.2. Only
//! the fields `verify.rs` (story 07) reads are typed here; every struct
//! ignores unknown fields instead of rejecting them, because AT Proto
//! lexicons add fields over time. `#[allow(dead_code)]` at module level: every
//! field here is read by story 07, not this story.

#![allow(dead_code)] // First caller is story 07's `verify.rs`.

use serde::Deserialize;

/// A label on a post or an actor. Only `val` matters to a guard; the App
/// View also sends `src`, `uri`, `cid` and `cts`, which no reader here needs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Label {
    pub val: String,
}

/// `postView.author`, TECH-DESIGN section 8.2's `author.did`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostViewAuthor {
    pub did: String,
}

/// `postView.record`, TECH-DESIGN section 8.2's `record.createdAt`. `rest`
/// keeps every other field of the record, `$type`, `embed`, `facets` and the
/// rest, as one JSON object: `dunk validate` (story 03) passes it straight
/// to `ingest::embed::detect`, which reads the record's own `embed`, not
/// `postView.embed`, the App View's separate hydrated view a guard walks
/// instead (story 07). `PostRecord` cannot derive `Eq`: `serde_json::Value`
/// does not implement it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostRecord {
    pub created_at: String,
    #[serde(flatten)]
    pub rest: serde_json::Value,
}

/// The embedded post inside `app.bsky.embed.record#view` or the media
/// variant, when the App View resolved it to a real post (BC13). A quote of
/// a list, a feed generator, a blocked account or a deleted post comes back
/// as one of the other `#view*` shapes, which this type does not need to
/// read: story 07 drops those without inspecting them further.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbedRecordViewRecord {
    pub uri: String,
}

/// `postView.embed`, TECH-DESIGN section 8.2's table of `Q.embed` shapes.
/// `Record` and `RecordWithMedia` carry a resolved post when the App View's
/// own `record.$type` is `#viewRecord`, and a named variant for
/// `#viewNotFound`, `#viewBlocked` and `#viewDetached` (BC22); every other
/// embed `$type` (`images`, `video`, `external`, or none at all) falls into
/// `Other`, so this type never rejects a body for an embed shape (BC13).
/// Story 07's `verify.rs` reads section 8.2's drop reasons off
/// `RecordViewInner`'s named variants and off `Other`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "$type")]
pub enum EmbedView {
    #[serde(rename = "app.bsky.embed.record#view")]
    Record { record: RecordViewInner },
    #[serde(rename = "app.bsky.embed.recordWithMedia#view")]
    RecordWithMedia { record: RecordWithMediaInner },
    #[serde(other)]
    Other,
}

/// The inner `record` of an `app.bsky.embed.record#view`. `#[serde(tag)]`
/// dispatches on the inner `$type` too, so `#viewNotFound`, `#viewBlocked`
/// and `#viewDetached` each decode into their own variant (BC22), so story
/// 07 can map each to the drop reason TECH-DESIGN section 8.2 gives it.
/// Every other inner `$type` still decodes into `Other`, never an error.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "$type")]
pub enum RecordViewInner {
    #[serde(rename = "app.bsky.embed.record#viewRecord")]
    ViewRecord(EmbedRecordViewRecord),
    #[serde(rename = "app.bsky.embed.record#viewNotFound")]
    ViewNotFound { uri: String },
    #[serde(rename = "app.bsky.embed.record#viewBlocked")]
    ViewBlocked { uri: String },
    #[serde(rename = "app.bsky.embed.record#viewDetached")]
    ViewDetached { uri: String },
    #[serde(other)]
    Other,
}

/// The inner `record` of an `app.bsky.embed.recordWithMedia#view`, which
/// nests the resolved post one level deeper, under its own `record` key.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RecordWithMediaInner {
    pub record: RecordViewInner,
}

/// `app.bsky.feed.defs#postView`, TECH-DESIGN section 8.2. Carries every
/// field a guard or the scorer reads for either side of a pair, `Q` or `O`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostView {
    pub uri: String,
    pub cid: String,
    pub author: PostViewAuthor,
    #[serde(default)]
    pub labels: Vec<Label>,
    pub record: PostRecord,
    pub like_count: u32,
    pub repost_count: u32,
    pub reply_count: u32,
    #[serde(default)]
    pub embed: Option<EmbedView>,
}

/// `app.bsky.feed.getPosts`'s response body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GetPostsResponse {
    pub posts: Vec<PostView>,
}

/// The shape of a quote post's own hydrated `postView.embed`, TECH-DESIGN
/// section 8.2's table, read once here instead of twice (round 2 finding 8):
/// `verify.rs` maps this to a `DropReason` and `validate.rs` maps it to its
/// own `Reason`. `Normal` carries the embedded post's URI, on a plain
/// `record#view` or a `recordWithMedia#view` whose inner `$type` is
/// `#viewRecord`. `Absent` (no embed at all, or an outer `$type` that is
/// neither `record#view` nor `recordWithMedia#view`) is kept distinct from
/// `NotAPost` (an inner `$type` present but none of the four named ones):
/// `validate.rs` falls through to its `getPosts`-map lookup on the first and
/// reports `not_a_post` directly on the second, so the two must stay
/// separate variants even though `verify.rs` maps both to the same
/// `DropReason::NotAPost`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuoteEmbed {
    Normal { uri: String },
    Detached,
    Blocked,
    NotFound,
    NotAPost,
    Absent,
}

/// Classifies `q`'s own hydrated `postView.embed`, TECH-DESIGN section 8.2's
/// table (BC51). Pure: reads nothing but `q.embed`.
pub fn classify_quote_embed(q: &PostView) -> QuoteEmbed {
    let inner = match &q.embed {
        Some(EmbedView::Record { record }) => record,
        Some(EmbedView::RecordWithMedia { record }) => &record.record,
        Some(EmbedView::Other) | None => return QuoteEmbed::Absent,
    };
    match inner {
        RecordViewInner::ViewRecord(view) => QuoteEmbed::Normal { uri: view.uri.clone() },
        RecordViewInner::ViewDetached { .. } => QuoteEmbed::Detached,
        RecordViewInner::ViewBlocked { .. } => QuoteEmbed::Blocked,
        RecordViewInner::ViewNotFound { .. } => QuoteEmbed::NotFound,
        RecordViewInner::Other => QuoteEmbed::NotAPost,
    }
}

#[cfg(test)]
mod classify_quote_embed_tests {
    use super::*;

    fn post(embed: Option<EmbedView>) -> PostView {
        PostView {
            uri: "at://did:plc:q/app.bsky.feed.post/q".to_string(),
            cid: "cid-q".to_string(),
            author: PostViewAuthor { did: "did:plc:q".to_string() },
            labels: vec![],
            record: PostRecord {
                created_at: "2026-01-01T00:00:00Z".to_string(),
                rest: serde_json::json!({}),
            },
            like_count: 0,
            repost_count: 0,
            reply_count: 0,
            embed,
        }
    }

    #[test]
    fn absent_embed_is_absent() {
        assert_eq!(classify_quote_embed(&post(None)), QuoteEmbed::Absent);
    }

    #[test]
    fn other_outer_type_is_absent() {
        assert_eq!(classify_quote_embed(&post(Some(EmbedView::Other))), QuoteEmbed::Absent);
    }

    #[test]
    fn other_inner_type_is_not_a_post() {
        let embed = EmbedView::Record { record: RecordViewInner::Other };
        assert_eq!(classify_quote_embed(&post(Some(embed))), QuoteEmbed::NotAPost);
    }

    #[test]
    fn view_record_is_normal() {
        let uri = "at://did:plc:o/app.bsky.feed.post/o".to_string();
        let embed = EmbedView::Record {
            record: RecordViewInner::ViewRecord(EmbedRecordViewRecord { uri: uri.clone() }),
        };
        assert_eq!(classify_quote_embed(&post(Some(embed))), QuoteEmbed::Normal { uri });
    }

    #[test]
    fn record_with_media_view_record_is_normal() {
        let uri = "at://did:plc:o/app.bsky.feed.post/o".to_string();
        let embed = EmbedView::RecordWithMedia {
            record: RecordWithMediaInner {
                record: RecordViewInner::ViewRecord(EmbedRecordViewRecord { uri: uri.clone() }),
            },
        };
        assert_eq!(classify_quote_embed(&post(Some(embed))), QuoteEmbed::Normal { uri });
    }

    #[test]
    fn view_detached_is_detached() {
        let embed = EmbedView::Record {
            record: RecordViewInner::ViewDetached { uri: "at://x".to_string() },
        };
        assert_eq!(classify_quote_embed(&post(Some(embed))), QuoteEmbed::Detached);
    }

    #[test]
    fn view_blocked_is_blocked() {
        let embed = EmbedView::Record {
            record: RecordViewInner::ViewBlocked { uri: "at://x".to_string() },
        };
        assert_eq!(classify_quote_embed(&post(Some(embed))), QuoteEmbed::Blocked);
    }

    #[test]
    fn view_not_found_is_not_found() {
        let embed = EmbedView::Record {
            record: RecordViewInner::ViewNotFound { uri: "at://x".to_string() },
        };
        assert_eq!(classify_quote_embed(&post(Some(embed))), QuoteEmbed::NotFound);
    }
}

/// `app.bsky.actor.defs#profileView`. Only the fields the follower floor and
/// author-state guards read (story 10): the DID to key the map, the follower
/// count, and the account's own labels.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileView {
    pub did: String,
    #[serde(default)]
    pub followers_count: u32,
    #[serde(default)]
    pub labels: Vec<Label>,
}

/// `app.bsky.actor.getProfiles`'s response body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GetProfilesResponse {
    pub profiles: Vec<ProfileView>,
}

/// `app.bsky.feed.getQuotes`'s response body: one page of quoting posts plus
/// the cursor for the next page. Story 03 owns the paging loop.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GetQuotesResponse {
    pub posts: Vec<PostView>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// `app.bsky.feed.defs#feedViewPost`, the element type of `getFeed`'s
/// `feed` array. Only the wrapped `post` is read; `reason` and `reply`
/// context are not needed by story 03's seed walk.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FeedViewPost {
    pub post: PostView,
}

/// `app.bsky.feed.getFeed`'s response body: one page of feed items plus the
/// cursor for the next page.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GetFeedResponse {
    pub feed: Vec<FeedViewPost>,
    #[serde(default)]
    pub cursor: Option<String>,
}
