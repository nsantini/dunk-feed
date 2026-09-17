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

/// `postView.record`, TECH-DESIGN section 8.2's `record.createdAt`. The
/// record's own `embed` is not read here; `postView.embed`, the App View's
/// hydrated view, is what a guard walks instead.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostRecord {
    pub created_at: String,
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
/// `Record` and `RecordWithMedia` carry a resolved post only when the App
/// View's own `record.$type` is `#viewRecord`; every other `record.$type`
/// (`#viewDetached`, `#viewBlocked`, `#viewNotFound`) and every other embed
/// `$type` (`images`, `video`, `external`, or none at all) falls into
/// `Other`, so this type never rejects a body for an embed shape (BC13).
/// Story 07's `verify.rs` reads section 8.2's drop reasons from `Other`,
/// not this client.
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
/// dispatches on the inner `$type` too, so a `#viewDetached`, `#viewBlocked`
/// or `#viewNotFound` decodes into `Other` rather than failing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "$type")]
pub enum RecordViewInner {
    #[serde(rename = "app.bsky.embed.record#viewRecord")]
    ViewRecord(EmbedRecordViewRecord),
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
