//! Ingest path. Never calls the App View, per `AGENTS.md`. Story 06 adds the
//! Jetstream consumer task: `hotset` holds the in-memory hot set, `embed`
//! is the pure quote detector, and this module's `translate` turns one
//! decoded [`CommitEvent`] into zero or more [`Op`] values for the writer,
//! TECH-DESIGN section 5.2. `translate` never touches the store or the
//! network, so it is unit tested with fixtures alone.

pub mod embed;
pub mod hotset;

use std::collections::HashMap;

use crate::jetstream::event::{CommitEvent, Operation};
use crate::store::writer::{CountField, Op};
use crate::store::unix_now;
use embed::{AtUri, Embed};
use hotset::HotSet;

/// The four collections the ingest task translates, TECH-DESIGN section
/// 5.1's `COLLECTIONS`. Anything else is [`Dropped::UnknownCollection`]
/// (BC18).
const POST: &str = "app.bsky.feed.post";
const LIKE: &str = "app.bsky.feed.like";
const REPOST: &str = "app.bsky.feed.repost";
const POSTGATE: &str = "app.bsky.feed.postgate";

/// How often `run_ingest` (story 06's later slice) emits the stats line
/// (BC27). A module constant, not `Config`: `AGENTS.md` reserves `Config`
/// for the PRD's score-table constants, and story 05 set the precedent with
/// the writer's own 500 ms and 1,000 ops.
const STATS_PERIOD_SECS: f64 = 60.0;

/// A change `translate` asks the caller to apply to its `HotSet`, since
/// `translate` only borrows one (BC1). `run_ingest` (story 06's later
/// slice) applies each change after every `Op` in the same `Translation`
/// has been sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HotChange {
    /// A URI that just became live: a quote's two sides (BC1, BC2).
    Insert(String),
    /// A URI that just left the hot set: a deleted post (BC9).
    Remove(String),
}

/// Why one commit produced no `Op`, for the three counters TECH-DESIGN
/// section 5.5's stats line reports. A commit can be dropped for other
/// reasons too (a gate miss, a self-quote-free reply to a cold parent), but
/// those are not counted, so they carry no `Dropped` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dropped {
    /// BC18: `commit.collection` is none of the four this task translates.
    UnknownCollection,
    /// BC4: a post quotes its own author's post.
    SelfQuote,
    /// BC3: a post embed is shaped like a quote, but the embedded record's
    /// collection is not `app.bsky.feed.post`.
    NonPostEmbed,
}

/// What `translate` returns for one commit: the `Op`s the writer should
/// receive, the `HotSet` changes the caller should apply, and, when the
/// commit produced no op for a counted reason, why. TECH-DESIGN section 5.2
/// lists at most one post-create op per commit (BC7), so `ops` holds zero or
/// one entry for every collection but `postgate`, which may hold several
/// (BC15).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Translation {
    pub ops: Vec<Op>,
    pub hot: Vec<HotChange>,
    pub dropped: Option<Dropped>,
}

/// `at://{did}/{collection}/{rkey}`, the URI a create, update or delete
/// commit refers to. Every collection this task translates builds its
/// record or subject URI the same way `AtUri::parse` expects.
fn commit_uri(commit: &CommitEvent) -> String {
    format!("at://{}/{}/{}", commit.did, commit.collection, commit.rkey)
}

/// Classifies `record.embed` for a `post` create or update, TECH-DESIGN
/// section 5.3. `embed::detect` already tells a quote of another post from
/// everything else, but it collapses "no embed", "an embed that is not
/// quote-shaped" and "a quote-shaped embed of a non-post record" into one
/// `NotAQuote`. BC3 needs the last of those three counted separately
/// (`Dropped::NonPostEmbed`), so this reads the embed's raw `uri` string
/// itself to tell them apart, rather than widening `embed::detect`'s
/// signature: `embed.rs` is `dunk validate`'s module too, and it is out of
/// this slice's files.
enum EmbedClass {
    Quote(AtUri),
    NonPostEmbed,
    None,
}

fn classify_embed(record: &serde_json::Value) -> EmbedClass {
    let Some(embed) = record.get("embed") else { return EmbedClass::None };
    let Some(kind) = embed.get("$type").and_then(|v| v.as_str()) else {
        return EmbedClass::None;
    };
    let uri = match kind {
        "app.bsky.embed.record" => embed.get("record").and_then(|r| r.get("uri")),
        "app.bsky.embed.recordWithMedia" => {
            embed.get("record").and_then(|r| r.get("record")).and_then(|r| r.get("uri"))
        }
        _ => return EmbedClass::None,
    };
    let Some(uri) = uri.and_then(|v| v.as_str()) else { return EmbedClass::None };

    match embed::detect(record) {
        Embed::Quote { original_uri } => EmbedClass::Quote(original_uri),
        Embed::NotAQuote => {
            // `embed::detect` rejected this URI. It is still quote-shaped
            // (the `$type` and a `uri` string were both there), so tell a
            // non-post collection apart from an outright malformed URI by
            // reading the collection segment ourselves.
            let collection = uri.strip_prefix("at://").and_then(|rest| rest.splitn(3, '/').nth(1));
            match collection {
                Some(collection) if collection != "app.bsky.feed.post" => EmbedClass::NonPostEmbed,
                _ => EmbedClass::None,
            }
        }
    }
}

/// `record.reply.parent.uri`, when present, TECH-DESIGN section 5.2's
/// reply row.
fn reply_parent_uri(record: &serde_json::Value) -> Option<&str> {
    record.get("reply")?.get("parent")?.get("uri")?.as_str()
}

/// `record.createdAt` parsed as RFC3339 in unix seconds; on a missing or
/// unparseable value, `commit.time`; on an unparseable `commit.time`, `now`
/// (BC20). Parsed only for `InsertPair`, since it is the only op that
/// stores `quoted_at`.
fn quoted_at(record: &serde_json::Value, commit: &CommitEvent) -> i64 {
    record
        .get("createdAt")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp())
        .or_else(|| commit.time_secs())
        .unwrap_or_else(unix_now)
}

/// Turns one `post` create or update into a `Translation`, TECH-DESIGN
/// section 5.2's `post` rows. A `record: None` commit (BC19) is treated as
/// a post with neither a quote nor a tracked reply (BC8).
fn translate_post_write(commit: &CommitEvent, hot: &HotSet) -> Translation {
    let Some(record) = &commit.record else { return Translation::default() };

    match classify_embed(record) {
        EmbedClass::Quote(original_uri) => {
            if original_uri.did() == commit.did {
                return Translation { dropped: Some(Dropped::SelfQuote), ..Default::default() };
            }
            let quote_uri = commit_uri(commit);
            let quote_cid = commit.cid.clone().unwrap_or_default();
            let original_uri_str = original_uri.as_str().to_string();
            let original_did = original_uri.did().to_string();
            let op = Op::InsertPair {
                quote_uri: quote_uri.clone(),
                quote_did: commit.did.clone(),
                quote_cid,
                original_uri: original_uri_str.clone(),
                original_did,
                quoted_at: quoted_at(record, commit),
                first_seen_at: unix_now(),
                seq: commit.seq,
            };
            Translation {
                ops: vec![op],
                hot: vec![HotChange::Insert(quote_uri), HotChange::Insert(original_uri_str)],
                dropped: None,
            }
        }
        EmbedClass::NonPostEmbed => {
            Translation { dropped: Some(Dropped::NonPostEmbed), ..Default::default() }
        }
        EmbedClass::None => match reply_parent_uri(record) {
            Some(parent_uri) if hot.contains(parent_uri) => Translation {
                ops: vec![Op::Incr {
                    post_uri: parent_uri.to_string(),
                    field: CountField::Replies,
                    seq: commit.seq,
                }],
                ..Default::default()
            },
            _ => Translation::default(),
        },
    }
}

/// A `post` delete, TECH-DESIGN section 5.2. Only a URI already in the hot
/// set produces an op and a `HotChange::Remove` (BC9); everything else is a
/// silent no-op (BC10).
fn translate_post_delete(commit: &CommitEvent, hot: &HotSet) -> Translation {
    let uri = commit_uri(commit);
    if hot.contains(&uri) {
        Translation {
            ops: vec![Op::DeletePost { uri: uri.clone(), seq: commit.seq }],
            hot: vec![HotChange::Remove(uri)],
            dropped: None,
        }
    } else {
        Translation::default()
    }
}

/// A `like` or `repost` create or update, TECH-DESIGN section 5.2. A delete
/// carries no subject (BC14) and a create or update whose subject is
/// absent, malformed, or not in the hot set is a silent gate miss (BC13):
/// neither ever produces an `Op` or a `Dropped` value, since the gate hit
/// rate is a `Stats` counter (story 06's later slice), not a `translate`
/// concern.
fn translate_gated_incr(commit: &CommitEvent, hot: &HotSet, field: CountField) -> Translation {
    if commit.operation == Operation::Delete {
        return Translation::default();
    }
    let Some(subject) =
        commit.record.as_ref().and_then(|r| r.get("subject")).and_then(|s| s.get("uri")).and_then(|v| v.as_str())
    else {
        return Translation::default();
    };
    if hot.contains(subject) {
        Translation {
            ops: vec![Op::Incr { post_uri: subject.to_string(), field, seq: commit.seq }],
            ..Default::default()
        }
    } else {
        Translation::default()
    }
}

/// A `postgate` create or update, TECH-DESIGN section 5.2. One
/// `Op::Detach` per `detachedEmbeddingUris` entry that is in the hot set,
/// in the record's order (BC15); an empty or missing list is a no-op
/// (BC16). A delete is unreachable in practice (a postgate is never
/// deleted), so it is treated the same as an empty list.
fn translate_postgate(commit: &CommitEvent, hot: &HotSet) -> Translation {
    let Some(record) = &commit.record else { return Translation::default() };
    let Some(uris) = record.get("detachedEmbeddingUris").and_then(|v| v.as_array()) else {
        return Translation::default();
    };

    let ops = uris
        .iter()
        .filter_map(|v| v.as_str())
        .filter(|uri| hot.contains(uri))
        .map(|uri| Op::Detach { quote_uri: uri.to_string(), seq: commit.seq })
        .collect::<Vec<_>>();

    Translation { ops, ..Default::default() }
}

/// Turns one decoded commit into a `Translation`, TECH-DESIGN section 5.2's
/// whole table. Pure: reads only `commit` and `hot`, touches neither the
/// store nor the network. `run_ingest` sends `translation.ops` to the
/// writer and applies `translation.hot` to its `HotSet` afterwards.
pub fn translate(commit: &CommitEvent, hot: &HotSet) -> Translation {
    match commit.collection.as_str() {
        POST => match commit.operation {
            Operation::Delete => translate_post_delete(commit, hot),
            Operation::Create | Operation::Update => translate_post_write(commit, hot),
        },
        LIKE => translate_gated_incr(commit, hot, CountField::Likes),
        REPOST => translate_gated_incr(commit, hot, CountField::Reposts),
        POSTGATE => match commit.operation {
            Operation::Delete => Translation::default(),
            Operation::Create | Operation::Update => translate_postgate(commit, hot),
        },
        _ => Translation { dropped: Some(Dropped::UnknownCollection), ..Default::default() },
    }
}

/// The `ops_per_s` key one `Op` counts under (BC27). A plain string tag
/// rather than a new field on `Op` itself, since `Op` belongs to
/// `store::writer` and already exposes everything the writer needs through
/// `seq`.
fn op_kind(op: &Op) -> &'static str {
    match op {
        Op::InsertPair { .. } => "insert_pair",
        Op::Incr { .. } => "incr",
        Op::DeletePost { .. } => "delete_post",
        Op::Detach { .. } => "detach",
        Op::Checkpoint { .. } => "checkpoint",
        Op::Interaction { .. } => "interaction",
        Op::MarkAllDirty { .. } => "mark_all_dirty",
    }
}

/// Divides every count in `counts` by `STATS_PERIOD_SECS`, for the
/// `events_per_s` and `ops_per_s` fields BC27 names.
fn per_second<K: Clone + Eq + std::hash::Hash>(counts: &HashMap<K, u64>) -> HashMap<K, f64> {
    counts.iter().map(|(k, v)| (k.clone(), *v as f64 / STATS_PERIOD_SECS)).collect()
}

/// The 60-second window's counters, TECH-DESIGN section 5.5's stats line
/// (BC27). A plain struct with `record_commit` and `emit`: no task, no
/// runtime, so a scripted sequence of calls tests `gate_hit_rate` and the
/// window reset (BC26) with neither.
#[derive(Debug, Default)]
pub struct Stats {
    events_by_collection: HashMap<String, u64>,
    ops_by_kind: HashMap<&'static str, u64>,
    gate_hits: u64,
    gate_attempts: u64,
    dropped_unknown_collection: u64,
    dropped_self_quote: u64,
    dropped_non_post_embed: u64,
    postgate_detaches: u64,
    last_commit_time: Option<i64>,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds one commit and the `Translation` `translate` returned for it
    /// into the window: the per-collection event count, the per-op-kind
    /// counts, the postgate detach count, the three drop counters, the gate
    /// hit/attempt counters (BC26), and the last commit time (BC28). A like
    /// or repost delete carries no subject, so it touches neither side of
    /// the gate (BC14); only a like or repost *create* is a gate attempt,
    /// matching BC26's literal wording.
    pub fn record_commit(&mut self, commit: &CommitEvent, translation: &Translation) {
        *self.events_by_collection.entry(commit.collection.clone()).or_insert(0) += 1;
        if let Some(t) = commit.time_secs() {
            self.last_commit_time = Some(t);
        }

        for op in &translation.ops {
            *self.ops_by_kind.entry(op_kind(op)).or_insert(0) += 1;
            if matches!(op, Op::Detach { .. }) {
                self.postgate_detaches += 1;
            }
        }

        match translation.dropped {
            Some(Dropped::UnknownCollection) => self.dropped_unknown_collection += 1,
            Some(Dropped::SelfQuote) => self.dropped_self_quote += 1,
            Some(Dropped::NonPostEmbed) => self.dropped_non_post_embed += 1,
            None => {}
        }

        let is_gated_collection = commit.collection == LIKE || commit.collection == REPOST;
        if is_gated_collection && commit.operation == Operation::Create {
            self.gate_attempts += 1;
            if translation.ops.iter().any(|op| matches!(op, Op::Incr { .. })) {
                self.gate_hits += 1;
            }
        }
    }

    /// Hits over attempts, `0.0` on an empty window (BC26).
    pub fn gate_hit_rate(&self) -> f64 {
        if self.gate_attempts == 0 {
            0.0
        } else {
            self.gate_hits as f64 / self.gate_attempts as f64
        }
    }

    /// `now` minus the last commit's time, never negative, `0` before the
    /// first commit (BC28).
    pub fn lag_s(&self) -> i64 {
        match self.last_commit_time {
            Some(t) => (unix_now() - t).max(0),
            None => 0,
        }
    }

    /// Logs one `tracing::info!` event carrying every field BC27 names,
    /// then resets every counter. `hot_set_len` and `channel_depth` are not
    /// `Stats` counters: `run_ingest` (story 06's later slice) reads them
    /// from the live `HotSet` and `WriterHandle::depth()` at the moment it
    /// calls this, the same way it supplies `compressed` from the
    /// `EventSource`.
    pub fn emit(&mut self, hot_set_len: usize, channel_depth: usize, compressed: bool) {
        tracing::info!(
            events_per_s = ?per_second(&self.events_by_collection),
            hot_set_len,
            ops_per_s = ?per_second(&self.ops_by_kind),
            gate_hit_rate = self.gate_hit_rate(),
            postgate_detaches = self.postgate_detaches,
            dropped_unknown_collection = self.dropped_unknown_collection,
            dropped_self_quote = self.dropped_self_quote,
            dropped_non_post_embed = self.dropped_non_post_embed,
            channel_depth,
            lag_s = self.lag_s(),
            compressed,
            "dunk: ingest stats"
        );
        *self = Stats::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jetstream::event::{Frame, Payload};

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR")))
            .unwrap_or_else(|err| panic!("reading fixture {name}: {err}"))
    }

    /// Decodes a Jetstream commit fixture, TECH-DESIGN section 5.1's
    /// envelope. Every fixture this module reads is a `#commit` frame,
    /// matching `jetstream::event`'s own fixture tests.
    fn commit(name: &str) -> CommitEvent {
        let raw = fixture(name);
        let frame: Frame =
            serde_json::from_str(&raw).unwrap_or_else(|err| panic!("decoding fixture {name}: {err}"));
        match frame.payload {
            Payload::Commit(commit) => commit,
            other => panic!("expected Payload::Commit for {name}, got {other:?}"),
        }
    }

    // BC1: post create, embed is a post quote (`app.bsky.embed.record`),
    // `commit.did != original.did`. Hand-made from `jetstream_commit_post.json`:
    // the embed shape kept, `record.uri`'s author changed to differ from the
    // commit's `did`.
    #[test]
    fn quote_record_produces_insert_pair() {
        let commit = commit("jetstream_commit_post_quote_record.json");
        let hot = HotSet::new();

        let translation = translate(&commit, &hot);

        assert_eq!(translation.dropped, None);
        assert_eq!(translation.ops.len(), 1);
        let quote_uri = format!("at://{}/{}/{}", commit.did, commit.collection, commit.rkey);
        match &translation.ops[0] {
            Op::InsertPair {
                quote_uri: op_quote_uri,
                quote_did,
                original_uri,
                original_did,
                seq,
                ..
            } => {
                assert_eq!(op_quote_uri, &quote_uri);
                assert_eq!(quote_did, &commit.did);
                assert_eq!(original_uri, "at://did:plc:originaldid00000000000000/app.bsky.feed.post/original0001");
                assert_eq!(original_did, "did:plc:originaldid00000000000000");
                assert_eq!(*seq, commit.seq);
            }
            other => panic!("expected Op::InsertPair, got {other:?}"),
        }
        assert_eq!(
            translation.hot,
            vec![
                HotChange::Insert(quote_uri),
                HotChange::Insert(
                    "at://did:plc:originaldid00000000000000/app.bsky.feed.post/original0001".to_string()
                ),
            ]
        );
    }

    // BC2: `app.bsky.embed.recordWithMedia`, `embed.record.record.uri` is
    // the original. Hand-made from `jetstream_commit_post.json`: the
    // original author's `did` changed so it differs from the commit's,
    // since the recorded fixture is a self quote.
    #[test]
    fn quote_with_media_produces_insert_pair() {
        let commit = commit("jetstream_commit_post_quote_with_media.json");
        let hot = HotSet::new();

        let translation = translate(&commit, &hot);

        assert_eq!(translation.dropped, None);
        assert_eq!(translation.ops.len(), 1);
        match &translation.ops[0] {
            Op::InsertPair { original_uri, original_did, .. } => {
                assert_eq!(original_uri, "at://did:plc:differentauthor00000000/app.bsky.feed.post/3mu6ks2ljsk2q");
                assert_eq!(original_did, "did:plc:differentauthor00000000");
            }
            other => panic!("expected Op::InsertPair, got {other:?}"),
        }
    }

    // BC3: embed quotes a record whose collection is not
    // `app.bsky.feed.post`. Hand-made from `jetstream_commit_post.json`:
    // the quoted record's collection changed to `app.bsky.graph.starterpack`
    // and the embed simplified to `app.bsky.embed.record`.
    #[test]
    fn quote_of_non_post_collection_is_dropped() {
        let commit = commit("jetstream_commit_post_quote_starterpack.json");
        let hot = HotSet::new();

        let translation = translate(&commit, &hot);

        assert!(translation.ops.is_empty());
        assert!(translation.hot.is_empty());
        assert_eq!(translation.dropped, Some(Dropped::NonPostEmbed));
    }

    // BC4: `commit.did == original.did`, a self quote. The recorded
    // `jetstream_commit_post.json` is exactly this case, unedited.
    #[test]
    fn self_quote_is_dropped() {
        let commit = commit("jetstream_commit_post.json");
        let hot = HotSet::new();

        let translation = translate(&commit, &hot);

        assert!(translation.ops.is_empty());
        assert!(translation.hot.is_empty());
        assert_eq!(translation.dropped, Some(Dropped::SelfQuote));
    }

    // BC5: `reply.parent.uri` is in the hot set. Hand-made from
    // `jetstream_commit_repost.json`: the `subject` replaced with a `reply`
    // whose `parent.uri` and `root.uri` are the same hot post.
    #[test]
    fn reply_to_hot_parent_increments_replies() {
        let commit = commit("jetstream_commit_post_reply.json");
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:ezay5dffpkfnjxh5yirexce2/app.bsky.feed.post/3mvqrul2wq22a");

        let translation = translate(&commit, &hot);

        assert_eq!(translation.dropped, None);
        assert!(translation.hot.is_empty());
        assert_eq!(
            translation.ops,
            vec![Op::Incr {
                post_uri: "at://did:plc:ezay5dffpkfnjxh5yirexce2/app.bsky.feed.post/3mvqrul2wq22a".to_string(),
                field: CountField::Replies,
                seq: commit.seq,
            }]
        );
    }

    // BC6: `reply.parent.uri` absent or not in the hot set. The same fixture
    // as BC5, with the parent left cold.
    #[test]
    fn reply_to_cold_parent_is_a_silent_no_op() {
        let commit = commit("jetstream_commit_post_reply.json");
        let hot = HotSet::new();

        let translation = translate(&commit, &hot);

        assert_eq!(translation, Translation::default());
    }

    // BC7: a post that is both a quote and a reply to a hot parent. The
    // quote wins: one `InsertPair`, no `Incr{Replies}`.
    #[test]
    fn quote_wins_over_reply_to_a_hot_parent() {
        let mut commit = commit("jetstream_commit_post_quote_record.json");
        let record = commit.record.as_mut().unwrap();
        record.as_object_mut().unwrap().insert(
            "reply".to_string(),
            serde_json::json!({
                "parent": {"uri": "at://did:plc:hotparent00000000000000/app.bsky.feed.post/parent0001"},
                "root": {"uri": "at://did:plc:hotparent00000000000000/app.bsky.feed.post/parent0001"},
            }),
        );
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:hotparent00000000000000/app.bsky.feed.post/parent0001");

        let translation = translate(&commit, &hot);

        assert_eq!(translation.ops.len(), 1);
        assert!(matches!(translation.ops[0], Op::InsertPair { .. }));
    }

    // BC8: neither a quote nor a tracked reply. The recorded
    // `jetstream_commit_like.json`'s shape does not apply here; instead the
    // reply fixture with a cold parent already covers this (BC6 test
    // above shares the same no-op path). This test uses the postgate
    // fixture's sibling, the plain repost fixture, edited to a bare post
    // with no embed and no reply.
    #[test]
    fn post_with_no_embed_and_no_reply_is_a_silent_no_op() {
        let mut commit = commit("jetstream_commit_post_reply.json");
        commit.record.as_mut().unwrap().as_object_mut().unwrap().remove("reply");

        let hot = HotSet::new();
        let translation = translate(&commit, &hot);

        assert_eq!(translation, Translation::default());
    }

    // BC9: post delete, the URI is in the hot set.
    #[test]
    fn post_delete_of_a_hot_uri_produces_delete_post() {
        let commit = commit("jetstream_commit_delete.json");
        let uri = format!("at://{}/{}/{}", commit.did, commit.collection, commit.rkey);
        let mut hot = HotSet::new();
        hot.insert(&uri);

        let translation = translate(&commit, &hot);

        assert_eq!(translation.ops, vec![Op::DeletePost { uri: uri.clone(), seq: commit.seq }]);
        assert_eq!(translation.hot, vec![HotChange::Remove(uri)]);
        assert_eq!(translation.dropped, None);
    }

    // BC10: post delete, the URI is not in the hot set.
    #[test]
    fn post_delete_of_a_cold_uri_is_a_silent_no_op() {
        let commit = commit("jetstream_commit_delete.json");
        let hot = HotSet::new();

        let translation = translate(&commit, &hot);

        assert_eq!(translation, Translation::default());
    }

    // BC11: like create, subject in the hot set. Counted as a gate hit
    // (story 06's `Stats`, not asserted here).
    #[test]
    fn like_of_hot_subject_increments_likes() {
        let commit = commit("jetstream_commit_like.json");
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:vvdrimbhu4kouacycafar4cs/app.bsky.feed.post/3mvqkfpsilc26");

        let translation = translate(&commit, &hot);

        assert_eq!(
            translation.ops,
            vec![Op::Incr {
                post_uri: "at://did:plc:vvdrimbhu4kouacycafar4cs/app.bsky.feed.post/3mvqkfpsilc26".to_string(),
                field: CountField::Likes,
                seq: commit.seq,
            }]
        );
    }

    // BC12: repost create, subject in the hot set.
    #[test]
    fn repost_of_hot_subject_increments_reposts() {
        let commit = commit("jetstream_commit_repost.json");
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:ezay5dffpkfnjxh5yirexce2/app.bsky.feed.post/3mvqrul2wq22a");

        let translation = translate(&commit, &hot);

        assert_eq!(
            translation.ops,
            vec![Op::Incr {
                post_uri: "at://did:plc:ezay5dffpkfnjxh5yirexce2/app.bsky.feed.post/3mvqrul2wq22a".to_string(),
                field: CountField::Reposts,
                seq: commit.seq,
            }]
        );
    }

    // BC13: like create, subject not in the hot set. The recorded
    // `jetstream_commit_like.json` with an empty hot set is exactly this
    // case (a gate miss, counted by `Stats`, not by `translate`).
    #[test]
    fn like_of_cold_subject_is_a_silent_no_op() {
        let commit = commit("jetstream_commit_like.json");
        let hot = HotSet::new();

        let translation = translate(&commit, &hot);

        assert_eq!(translation, Translation::default());
    }

    // BC14: like and repost deletes are always dropped, since the event
    // carries only an rkey, never the subject (AC2). Hand-made from
    // `jetstream_commit_like.json`: the record and cid removed and
    // `operation` changed to `delete`, matching the shape
    // `jetstream_commit_delete.json` already uses for a post delete.
    #[test]
    fn like_repost_deletes_are_dropped() {
        let like_delete = commit("jetstream_commit_like_delete.json");
        let mut hot = HotSet::new();
        // Even a hot subject cannot help: the delete carries none.
        hot.insert("at://did:plc:vvdrimbhu4kouacycafar4cs/app.bsky.feed.post/3mvqkfpsilc26");

        let translation = translate(&like_delete, &hot);
        assert_eq!(translation, Translation::default());

        let mut repost_delete = commit("jetstream_commit_repost.json");
        repost_delete.operation = Operation::Delete;
        repost_delete.record = None;
        repost_delete.cid = None;
        let translation = translate(&repost_delete, &hot);
        assert_eq!(translation, Translation::default());
    }

    // BC15: postgate create, two `detachedEmbeddingUris` entries, both in
    // the hot set, in the record's order.
    #[test]
    fn postgate_detach_produces_one_op_per_hot_entry() {
        let commit = commit("jetstream_commit_postgate_detached.json");
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote1");
        hot.insert("at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote2");

        let translation = translate(&commit, &hot);

        assert_eq!(
            translation.ops,
            vec![
                Op::Detach {
                    quote_uri: "at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote1"
                        .to_string(),
                    seq: commit.seq,
                },
                Op::Detach {
                    quote_uri: "at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote2"
                        .to_string(),
                    seq: commit.seq,
                },
            ]
        );
    }

    // BC15 continued: an entry not in the hot set is dropped silently.
    #[test]
    fn postgate_detach_skips_a_cold_entry() {
        let commit = commit("jetstream_commit_postgate_detached.json");
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote1");

        let translation = translate(&commit, &hot);

        assert_eq!(
            translation.ops,
            vec![Op::Detach {
                quote_uri: "at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote1"
                    .to_string(),
                seq: commit.seq,
            }]
        );
    }

    // BC16: an empty `detachedEmbeddingUris` is a no-op. The recorded
    // `jetstream_commit_postgate.json` carries an empty list unedited.
    #[test]
    fn postgate_with_empty_detached_list_is_a_no_op() {
        let commit = commit("jetstream_commit_postgate.json");
        let hot = HotSet::new();

        let translation = translate(&commit, &hot);

        assert_eq!(translation, Translation::default());
    }

    // BC17: `post` update is treated as a create for the embed check.
    // Hand-made from the same quote as BC1, `operation` changed to
    // `update`.
    #[test]
    fn post_update_is_treated_as_a_create_for_the_embed_check() {
        let commit = commit("jetstream_commit_post_update.json");
        assert_eq!(commit.operation, Operation::Update);
        let hot = HotSet::new();

        let translation = translate(&commit, &hot);

        assert_eq!(translation.ops.len(), 1);
        assert!(matches!(translation.ops[0], Op::InsertPair { .. }));
    }

    // BC18: an unknown collection is dropped and counted.
    #[test]
    fn unknown_collection_is_dropped() {
        let mut commit = commit("jetstream_commit_like.json");
        commit.collection = "app.bsky.feed.threadgate".to_string();

        let translation = translate(&commit, &HotSet::new());

        assert!(translation.ops.is_empty());
        assert_eq!(translation.dropped, Some(Dropped::UnknownCollection));
    }

    // BC19: `post` create or update with `record: None` is treated as BC8:
    // no op, no panic.
    #[test]
    fn post_create_with_no_record_does_not_panic() {
        let mut commit = commit("jetstream_commit_post.json");
        commit.record = None;

        let translation = translate(&commit, &HotSet::new());

        assert_eq!(translation, Translation::default());
    }

    // BC20: `quoted_at` falls back from `record.createdAt` to `commit.time`
    // to `now`, in that order.
    #[test]
    fn quoted_at_falls_back_through_created_at_then_commit_time_then_now() {
        let commit = commit("jetstream_commit_post_quote_record.json");
        let record = commit.record.as_ref().unwrap();
        let expected = chrono::DateTime::parse_from_rfc3339("2026-09-17T23:30:00.000Z").unwrap().timestamp();
        assert_eq!(quoted_at(record, &commit), expected);

        let mut no_created_at = commit.record.clone().unwrap();
        no_created_at.as_object_mut().unwrap().remove("createdAt");
        assert_eq!(quoted_at(&no_created_at, &commit), commit.time_secs().unwrap());

        let mut bad_commit = commit;
        bad_commit.time = "not-a-timestamp".to_string();
        let mut no_created_at_bad_time = bad_commit.record.clone().unwrap();
        no_created_at_bad_time.as_object_mut().unwrap().remove("createdAt");
        let before = unix_now();
        let now = quoted_at(&no_created_at_bad_time, &bad_commit);
        assert!(now >= before);
    }

    // BC26: `gate_hit_rate` counts only like and repost creates. A like
    // delete carries no subject (BC14) and touches neither counter, even
    // when its collection is `LIKE`.
    #[test]
    fn gate_hit_rate_counts_only_like_and_repost_creates() {
        let mut stats = Stats::new();
        assert_eq!(stats.gate_hit_rate(), 0.0, "an empty window is 0.0, not a division by zero");

        let like_hot = commit("jetstream_commit_like.json");
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:vvdrimbhu4kouacycafar4cs/app.bsky.feed.post/3mvqkfpsilc26");
        let translation = translate(&like_hot, &hot);
        stats.record_commit(&like_hot, &translation);

        let like_cold = commit("jetstream_commit_like.json");
        let translation = translate(&like_cold, &HotSet::new());
        stats.record_commit(&like_cold, &translation);

        let repost_hot = commit("jetstream_commit_repost.json");
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:ezay5dffpkfnjxh5yirexce2/app.bsky.feed.post/3mvqrul2wq22a");
        let translation = translate(&repost_hot, &hot);
        stats.record_commit(&repost_hot, &translation);

        let like_delete = commit("jetstream_commit_like_delete.json");
        let translation = translate(&like_delete, &HotSet::new());
        stats.record_commit(&like_delete, &translation);

        // Two hits (the hot like, the hot repost) over three attempts (the
        // delete never entered the count).
        assert!((stats.gate_hit_rate() - (2.0 / 3.0)).abs() < f64::EPSILON);
    }

    // AC6, BC27: the stats line carries every field the contract names, and
    // resets after `emit`.
    #[test]
    fn stats_line_contains_all_fields() {
        let mut stats = Stats::new();

        // A quote (`insert_pair`), a dropped self quote, a dropped non-post
        // embed, an unknown collection, and a postgate with two hot entries
        // (`detach` x2, `postgate_detaches` == 2).
        let quote = commit("jetstream_commit_post_quote_record.json");
        let translation = translate(&quote, &HotSet::new());
        assert_eq!(translation.ops.len(), 1);
        stats.record_commit(&quote, &translation);

        let self_quote = commit("jetstream_commit_post.json");
        let translation = translate(&self_quote, &HotSet::new());
        stats.record_commit(&self_quote, &translation);

        let non_post_embed = commit("jetstream_commit_post_quote_starterpack.json");
        let translation = translate(&non_post_embed, &HotSet::new());
        stats.record_commit(&non_post_embed, &translation);

        let mut unknown = commit("jetstream_commit_like.json");
        unknown.collection = "app.bsky.feed.threadgate".to_string();
        let translation = translate(&unknown, &HotSet::new());
        stats.record_commit(&unknown, &translation);

        let postgate = commit("jetstream_commit_postgate_detached.json");
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote1");
        hot.insert("at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote2");
        let translation = translate(&postgate, &hot);
        assert_eq!(translation.ops.len(), 2);
        let last_seen_time = postgate.time_secs().expect("fixture carries a parseable time");
        stats.record_commit(&postgate, &translation);

        assert_eq!(stats.dropped_self_quote, 1);
        assert_eq!(stats.dropped_non_post_embed, 1);
        assert_eq!(stats.dropped_unknown_collection, 1);
        assert_eq!(stats.postgate_detaches, 2);
        assert_eq!(stats.ops_by_kind.get("insert_pair"), Some(&1));
        assert_eq!(stats.ops_by_kind.get("detach"), Some(&2));
        assert_eq!(stats.events_by_collection.get(POST), Some(&3));
        assert_eq!(stats.events_by_collection.get("app.bsky.feed.threadgate"), Some(&1));
        assert_eq!(stats.events_by_collection.get(POSTGATE), Some(&1));

        // Per-collection rates are the raw count divided by the 60 s window.
        let rates = per_second(&stats.events_by_collection);
        assert!((rates[POST] - (3.0 / STATS_PERIOD_SECS)).abs() < f64::EPSILON);

        // `lag_s`: the postgate commit was the last one recorded.
        let expected_lag = (unix_now() - last_seen_time).max(0);
        assert!((stats.lag_s() - expected_lag).abs() <= 1, "lag_s should track the last commit's time");

        stats.emit(42, 7, true);

        // Every counter resets after the line, matching a freshly built `Stats`.
        assert_eq!(stats.events_by_collection, HashMap::new());
        assert_eq!(stats.ops_by_kind, HashMap::new());
        assert_eq!(stats.gate_hits, 0);
        assert_eq!(stats.gate_attempts, 0);
        assert_eq!(stats.dropped_unknown_collection, 0);
        assert_eq!(stats.dropped_self_quote, 0);
        assert_eq!(stats.dropped_non_post_embed, 0);
        assert_eq!(stats.postgate_detaches, 0);
        assert_eq!(stats.gate_hit_rate(), 0.0);
        assert_eq!(stats.lag_s(), 0);
    }
}
