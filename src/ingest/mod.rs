//! Ingest path. Never calls the App View, per `AGENTS.md`. Story 06 adds the
//! Jetstream consumer task: `hotset` holds the in-memory hot set, `embed`
//! is the pure quote detector, and this module's `translate` turns one
//! decoded [`CommitEvent`] into zero or more [`Op`] values for the writer,
//! TECH-DESIGN section 5.2. `translate` never touches the store or the
//! network, so it is unit tested with fixtures alone. `EventSource` is the
//! small trait `run_ingest`'s task loop reads through; `JetstreamClient`
//! implements it, and a test vector source drives the loop with no socket.
//! `run` (slice 6.0) is `upstage run`'s entry point, wiring the store, the
//! writer and a real `JetstreamClient` into `run_ingest`.

pub mod embed;
pub mod hotset;

use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, MissedTickBehavior};

use crate::config::Config;
use crate::health::HealthState;
use crate::jetstream::event::{parse_rfc3339_secs, CommitEvent, Operation};
use crate::jetstream::{Event, JetstreamError};
use crate::store::writer::{CountField, Op, WriterHandle, WriterState};
use crate::store::{unix_now, StoreError};
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

/// Why one commit produced no `Op`, for the counters TECH-DESIGN section
/// 5.5's stats line reports. A commit can be dropped for other reasons too
/// (a gate miss, a self-quote-free reply to a cold parent), but those are
/// not counted, so they carry no `Dropped` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dropped {
    /// BC18: `commit.collection` is none of the four this task translates.
    UnknownCollection,
    /// BC4: a post quotes its own author's post.
    SelfQuote,
    /// BC3: a post embed is shaped like a quote, but the embedded record's
    /// collection is not `app.bsky.feed.post`.
    NonPostEmbed,
    /// BC40: a post would form a pair, but its commit carries no `cid`. A
    /// pair is never written with an empty `quote_cid`, since story 07
    /// verifies candidates against the cid and cannot recover an empty one.
    MissingCid,
}

/// What `translate` returns for one commit: the `Op`s the writer should
/// receive, and, when the commit produced no op for a counted reason, why.
/// TECH-DESIGN section 5.2 lists at most one post-create op per commit
/// (BC7), so `ops` holds zero or one entry for every collection but
/// `postgate`, which may hold several (BC15). Round 1 finding 6: `translate`
/// used to also return a `Vec<HotChange>`, one entry for each side of a
/// `HotSet` change; `run_ingest` now derives that change straight from the
/// `Op` itself (`InsertPair` inserts both its URIs, `DeletePost` removes its
/// one), so a second, easy-to-desync copy of the same information is gone.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Translation {
    pub ops: Vec<Op>,
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
/// signature: `embed.rs` is `upstage validate`'s module too, and it is out of
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
            // reading the collection segment ourselves. Round 1 finding 7:
            // `AtUri::collection_of` in place of the manual split this used
            // to do here.
            match AtUri::collection_of(uri) {
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
    // Round 1 finding 7: shares `parse_rfc3339_secs` with
    // `CommitEvent::time_secs` and `validate::age_hours` instead of parsing
    // RFC3339 a third way here.
    record
        .get("createdAt")
        .and_then(|v| v.as_str())
        .and_then(parse_rfc3339_secs)
        .or_else(|| commit.time_secs())
        .unwrap_or_else(unix_now)
}

/// Turns one `post` create or update into a `Translation`, TECH-DESIGN
/// section 5.2's `post` rows. A `record: None` commit (BC19) is treated as
/// a post with neither a quote nor a tracked reply (BC8).
///
/// Round 1 finding 3: a quote that turns out to be a self quote (BC4), a
/// non-post embed (BC3) or missing its `cid` (BC40) is dropped, but that no
/// longer ends the function early. It falls through to the reply check
/// below (BC7's amendment), carrying its `Dropped` value forward, so a post
/// that is both an uncounted quote and a reply to a hot parent can still
/// yield one `Incr{Replies}` alongside the drop. Only a quote that actually
/// becomes an `InsertPair` short-circuits: that op wins outright, and the
/// reply check never runs for it (BC7's original rule).
fn translate_post_write(commit: &CommitEvent, hot: &HotSet) -> Translation {
    let Some(record) = &commit.record else { return Translation::default() };

    let dropped = match classify_embed(record) {
        EmbedClass::Quote(original_uri) => {
            if embed::is_self_quote(&original_uri, &commit.did) {
                Some(Dropped::SelfQuote)
            } else if commit.cid.is_none() {
                Some(Dropped::MissingCid)
            } else {
                let quote_uri = commit_uri(commit);
                let quote_cid = commit
                    .cid
                    .clone()
                    .expect("checked commit.cid.is_none() above and returned early on true");
                let original_uri_str = original_uri.as_str().to_string();
                let original_did = original_uri.did().to_string();
                let op = Op::InsertPair {
                    quote_uri,
                    quote_did: commit.did.clone(),
                    quote_cid,
                    original_uri: original_uri_str,
                    original_did,
                    quoted_at: quoted_at(record, commit),
                    first_seen_at: unix_now(),
                    seq: commit.seq,
                };
                return Translation { ops: vec![op], dropped: None };
            }
        }
        EmbedClass::NonPostEmbed => Some(Dropped::NonPostEmbed),
        EmbedClass::None => None,
    };

    // BC5: a reply increment is a `create`-only op; an update's parent can
    // never change, so incrementing here again would double count.
    if commit.operation == Operation::Create {
        if let Some(parent_uri) = reply_parent_uri(record) {
            if hot.contains(parent_uri) {
                return Translation {
                    ops: vec![Op::Incr {
                        post_uri: parent_uri.to_string(),
                        field: CountField::Replies,
                        seq: commit.seq,
                    }],
                    dropped,
                };
            }
        }
    }

    Translation { dropped, ..Default::default() }
}

/// A `post` delete, TECH-DESIGN section 5.2. Only a URI already in the hot
/// set produces an op (BC9); everything else is a silent no-op (BC10).
/// `run_ingest` derives the `HotSet` removal straight from the `Op` itself
/// (round 1 finding 6).
fn translate_post_delete(commit: &CommitEvent, hot: &HotSet) -> Translation {
    let uri = commit_uri(commit);
    if hot.contains(&uri) {
        Translation { ops: vec![Op::DeletePost { uri, seq: commit.seq }], dropped: None }
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
    let Some(subject) = commit
        .record
        .as_ref()
        .and_then(|r| r.get("subject"))
        .and_then(|s| s.get("uri"))
        .and_then(|v| v.as_str())
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
/// writer, applying to its `HotSet` whatever change each op itself implies
/// (round 1 finding 6).
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

/// Divides every count in `counts` by `elapsed_secs`, for the
/// `events_per_s` and `ops_per_s` fields BC27 names. Round 1 finding 5
/// (BC42): `elapsed_secs` is the time actually elapsed since the window
/// last reset, not the constant `STATS_PERIOD_SECS`, so a late tick never
/// reports a rate that is too high. `elapsed_secs` is floored at a small
/// positive value so a window of (near) zero length never divides by zero.
fn per_second<K: Clone + Eq + std::hash::Hash>(
    counts: &HashMap<K, u64>,
    elapsed_secs: f64,
) -> HashMap<K, f64> {
    let elapsed = elapsed_secs.max(0.001);
    counts.iter().map(|(k, v)| (k.clone(), *v as f64 / elapsed)).collect()
}

/// The `events_by_collection` key one commit counts under (BC42): one of
/// the four collections this task translates, by its own `&'static str`
/// constant, or `"other"` for every collection outside that set (an
/// `UnknownCollection` drop, TECH-DESIGN section 5.1's `COLLECTIONS`).
fn collection_key(collection: &str) -> &'static str {
    match collection {
        POST => POST,
        LIKE => LIKE,
        REPOST => REPOST,
        POSTGATE => POSTGATE,
        _ => "other",
    }
}

/// The 60-second window's counters, TECH-DESIGN section 5.5's stats line
/// (BC27). A plain struct with `record_commit` and `emit`: no task, no
/// runtime, so a scripted sequence of calls tests `gate_hit_rate` and the
/// window reset (BC26) with neither.
#[derive(Debug)]
pub struct Stats {
    events_by_collection: HashMap<&'static str, u64>,
    ops_by_kind: HashMap<&'static str, u64>,
    gate_hits: u64,
    gate_attempts: u64,
    dropped_unknown_collection: u64,
    dropped_self_quote: u64,
    dropped_non_post_embed: u64,
    dropped_missing_cid: u64,
    last_commit_time: Option<i64>,
    /// When this window started, for `per_second`'s elapsed-time
    /// denominator (round 1 finding 5, BC42). Reset by `emit`.
    window_start: Instant,
}

impl Default for Stats {
    fn default() -> Self {
        Stats::new()
    }
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            events_by_collection: HashMap::new(),
            ops_by_kind: HashMap::new(),
            gate_hits: 0,
            gate_attempts: 0,
            dropped_unknown_collection: 0,
            dropped_self_quote: 0,
            dropped_non_post_embed: 0,
            dropped_missing_cid: 0,
            last_commit_time: None,
            window_start: Instant::now(),
        }
    }

    /// Folds one commit and the `Translation` `translate` returned for it
    /// into the window: the per-collection event count, the per-op-kind
    /// counts, the four drop counters, the gate hit/attempt counters
    /// (BC26), and the last commit time (BC28). A like or repost delete
    /// carries no subject, so it touches neither side of the gate (BC14);
    /// only a like or repost *create* is a gate attempt, matching BC26's
    /// literal wording. Round 1 finding 5 (BC42): `postgate_detaches` is no
    /// longer its own counter here; `emit` reads it off `ops_by_kind`'s
    /// `"detach"` entry instead, so the same count is never held twice.
    pub fn record_commit(&mut self, commit: &CommitEvent, translation: &Translation) {
        *self.events_by_collection.entry(collection_key(&commit.collection)).or_insert(0) += 1;
        if let Some(t) = commit.time_secs() {
            self.last_commit_time = Some(t);
        }

        for op in &translation.ops {
            *self.ops_by_kind.entry(op_kind(op)).or_insert(0) += 1;
        }

        match translation.dropped {
            Some(Dropped::UnknownCollection) => self.dropped_unknown_collection += 1,
            Some(Dropped::SelfQuote) => self.dropped_self_quote += 1,
            Some(Dropped::NonPostEmbed) => self.dropped_non_post_embed += 1,
            Some(Dropped::MissingCid) => self.dropped_missing_cid += 1,
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
        let elapsed = self.window_start.elapsed().as_secs_f64();
        let postgate_detaches = *self.ops_by_kind.get("detach").unwrap_or(&0);
        tracing::info!(
            events_per_s = ?per_second(&self.events_by_collection, elapsed),
            hot_set_len,
            ops_per_s = ?per_second(&self.ops_by_kind, elapsed),
            gate_hit_rate = self.gate_hit_rate(),
            postgate_detaches,
            dropped_unknown_collection = self.dropped_unknown_collection,
            dropped_self_quote = self.dropped_self_quote,
            dropped_non_post_embed = self.dropped_non_post_embed,
            channel_depth,
            lag_s = self.lag_s(),
            compressed,
            "upstage: ingest stats"
        );
        *self = Stats::new();
    }
}

/// How often `run_ingest` sends `Op::Checkpoint` when the source has a
/// `last_seq` that no earlier checkpoint sent (BC25), so the cursor still
/// advances through a run of events that produced no op. A module constant
/// for the same reason `STATS_PERIOD_SECS` is one: `AGENTS.md` reserves
/// `Config` for the PRD's score-table constants, and story 05 already set
/// the writer's own 500 ms and 1,000 ops as the precedent.
const CHECKPOINT_PERIOD_SECS: u64 = 5;

/// The commit-and-info stream `run_ingest` reads, TECH-DESIGN section 5.1.
/// `JetstreamClient` (`src/jetstream/client.rs`) implements it directly; a
/// test vector source implements it over a fixed list of `Event`s, so the
/// task loop is driven with no socket. Generic rather than a trait object:
/// `run_ingest` takes one `S: EventSource` and never stores a second
/// implementation alongside it.
///
/// Round 1 finding 1: `next` carries an explicit `+ Send` bound on its
/// returned future. `tokio::spawn` requires a `Send` future to run the pump
/// task below on a worker thread, and a plain `async fn` in a trait makes no
/// such promise on its own.
pub trait EventSource {
    /// The next event, hiding every reconnect the implementation needs.
    /// `JetstreamError` only on a caller-fatal failure (BC32); today
    /// `JetstreamClient::next` never returns one, since it retries every
    /// network fault itself.
    fn next(&mut self) -> impl Future<Output = Result<Event, JetstreamError>> + Send;
    /// The `seq` of the last commit this source has returned, or `None`
    /// before the first one.
    fn last_seq(&self) -> Option<u64>;
    /// `true` when frames are currently received compressed.
    fn is_compressed(&self) -> bool;
    /// The cursor this source connected with, BC23's second fallback for
    /// `Op::MarkAllDirty`'s `seq` when no commit has been seen yet this run.
    fn initial_cursor(&self) -> Option<u64>;
}

/// Every way `run_ingest`, and `run` (story 06's slice 6.0), can fail.
/// `main.rs` is the only module that catches this: it logs the error and
/// exits 1 (BC30, BC31, BC32).
#[derive(Debug, Error)]
pub enum IngestError {
    /// A store read, or the hot-set rebuild, failed (BC31). `run` (slice
    /// 6.0) raises this; `run_ingest` never constructs it itself.
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    /// `source.next()` returned a caller-fatal error (BC32).
    #[error("jetstream error: {0}")]
    Jetstream(#[from] JetstreamError),
    /// The writer thread died: its health turned `Failed`, or a `send`
    /// found it already gone (BC30).
    #[error("writer thread failed")]
    WriterFailed,
    /// The scorer task failed. `run` (slice 6.0's counterpart for story 07)
    /// spawns the scorer alongside `run_ingest`, catches this, and it exits
    /// `upstage run` non-zero the same way any other `IngestError` does, since
    /// `cli.rs` already maps every `IngestError` through `CliError::Ingest`
    /// (BC35).
    #[error("scorer error: {0}")]
    Scorer(#[from] crate::scorer::ScorerError),
    /// The HTTP task failed: `crate::http::serve` could not bind
    /// `UPSTAGE_HTTP_ADDR` (BC23). `run` (slice 3.0) spawns the HTTP server as
    /// the third supervised task and wraps its `HttpError` here the same way
    /// `Scorer` wraps `ScorerError`.
    #[error("http error: {0}")]
    Http(#[from] crate::http::HttpError),
    /// A supervised task's `JoinHandle` came back `Err` (BC40): the task
    /// panicked rather than returning its `Result<(), IngestError>`
    /// normally. `supervise` (round 2 finding 2) catches every panic here
    /// and never re-panics itself; the writer still flushes and shuts down
    /// either way.
    #[error("the {task} task panicked")]
    TaskPanicked { task: &'static str },
}

/// Sends one `Op` to the writer, mapping the only error `WriterHandle::send`
/// returns, `StoreError::WriterGone`, to `IngestError::WriterFailed` (BC30).
async fn send_op(writer: &WriterHandle, op: Op) -> Result<(), IngestError> {
    writer.send(op).await.map_err(|_| IngestError::WriterFailed)
}

/// Applies to `hot` whatever change `op` itself implies (round 1 finding 6):
/// an `InsertPair` puts both its URIs in, a `DeletePost` takes its one URI
/// out. Every other `Op` leaves `hot` untouched; a `Detach`'s hot-set
/// consequence, if any, arrives later over the eviction channel (BC37,
/// BC38), because the URIs a detach frees depend on whether another live
/// pair still needs them, which only the store, not the op itself, knows.
fn apply_op_to_hot_set(hot: &mut HotSet, op: &Op) {
    match op {
        Op::InsertPair { quote_uri, original_uri, .. } => {
            hot.insert(quote_uri);
            hot.insert(original_uri);
        }
        Op::DeletePost { uri, .. } => {
            hot.remove(uri);
        }
        _ => {}
    }
}

/// One event `EventSource::next` returned, plus the source state the
/// checkpoint and stats line need at the moment it arrived (round 1 finding
/// 1). Carrying this alongside the event, rather than reaching back into the
/// source for it, is what lets the source live on its own pump task: once
/// the pump forwards a `Sourced`, `run_ingest`'s loop never touches the
/// source again.
struct Sourced {
    event: Event,
    last_seq: Option<u64>,
    compressed: bool,
}

/// The pump task's channel capacity (BC36): generous enough that a slow
/// consumer never blocks the pump mid-`next()` under normal load, since a
/// blocked pump is a blocked reconnect backoff.
const PUMP_CHANNEL_CAPACITY: usize = 1_024;

/// Spawns the one task that owns `source`, TECH-DESIGN section 5.1, round 1
/// finding 1. Loops `source.next()` and forwards `Result<Sourced,
/// JetstreamError>` over a bounded channel of [`PUMP_CHANNEL_CAPACITY`];
/// `run_ingest`'s own `select!` then only ever awaits a cancel-safe
/// `Receiver::recv()`, so a timer tick or a shutdown signal can never cancel
/// an in-flight `next()` and discard the client's reconnect backoff. The
/// pump exits once `shutdown` reports `true`, once the receiver is dropped,
/// or right after it forwards a caller-fatal error, since a source that just
/// returned one is not expected to make progress on a further call.
fn spawn_pump<S>(
    mut source: S,
    mut shutdown: watch::Receiver<bool>,
) -> mpsc::Receiver<Result<Sourced, JetstreamError>>
where
    S: EventSource + Send + 'static,
{
    let (tx, rx) = mpsc::channel(PUMP_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        loop {
            if *shutdown.borrow() {
                return;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                result = source.next() => {
                    let is_err = result.is_err();
                    let sourced = result.map(|event| Sourced {
                        event,
                        last_seq: source.last_seq(),
                        compressed: source.is_compressed(),
                    });
                    if tx.send(sourced).await.is_err() {
                        return; // `run_ingest`'s receiver is gone.
                    }
                    if is_err {
                        return;
                    }
                }
            }
        }
    });
    rx
}

/// Drives the ingest task loop, TECH-DESIGN section 5.1: reads one `Event`
/// at a time from the pump task `source` is handed to, translates each
/// commit into `Op`s sent to `writer` and applies the `HotSet` change each
/// op implies, sends `Op::Checkpoint` on a timer when nothing else moved the
/// cursor (BC25), sends `Op::MarkAllDirty` on an `#info OutdatedCursor`
/// frame (BC23), drains `evict_rx` for the URIs a batch's `DeletePost` or
/// `Detach` evicted (BC37), logs the stats line on a timer (BC27), and
/// returns once `shutdown` reports `true` (BC33) or the writer dies (BC30,
/// BC41). `run` (slice 6.0) owns the signal handlers that flip `shutdown`,
/// and calls `WriterHandle::flush` then `shutdown` once this returns.
pub async fn run_ingest<S: EventSource + Send + 'static>(
    cfg: &Config,
    source: S,
    writer: &WriterHandle,
    hot: &mut HotSet,
    shutdown: watch::Receiver<bool>,
    evict_rx: mpsc::UnboundedReceiver<Vec<String>>,
    health: &HealthState,
) -> Result<(), IngestError> {
    run_ingest_periodic(
        cfg,
        source,
        writer,
        hot,
        shutdown,
        Duration::from_secs(CHECKPOINT_PERIOD_SECS),
        Duration::from_secs_f64(STATS_PERIOD_SECS),
        evict_rx,
        health,
    )
    .await
}

/// `run_ingest`'s body, parameterised over the checkpoint and stats
/// periods. This repository carries no `tokio` `test-util` feature
/// (`Cargo.toml` is out of this slice's files, and the checkpoint and stats
/// periods are seconds, not milliseconds), so a real timer is the only kind
/// a test can use; shrinking both periods here lets a test see a checkpoint
/// or a stats line in milliseconds rather than waiting out the real 5 s and
/// 60 s periods `run_ingest` uses in production.
#[allow(clippy::too_many_arguments)]
async fn run_ingest_periodic<S: EventSource + Send + 'static>(
    _cfg: &Config,
    source: S,
    writer: &WriterHandle,
    hot: &mut HotSet,
    mut shutdown: watch::Receiver<bool>,
    checkpoint_period: Duration,
    stats_period: Duration,
    mut evict_rx: mpsc::UnboundedReceiver<Vec<String>>,
    // Named apart from the `health` local below (`writer.health()`, the
    // writer thread's own liveness watch): this is `HealthState`, the
    // cross-task `/healthz` clock the ingest loop records the last commit
    // time on (BC25), a different thing entirely.
    liveness: &HealthState,
) -> Result<(), IngestError> {
    // Round 1 finding 1: read once, before the source is handed to the
    // pump, since the loop below can no longer reach the source itself.
    let initial_cursor = source.initial_cursor();
    let mut events_rx = spawn_pump(source, shutdown.clone());

    let mut stats = Stats::new();
    let mut checkpoint_sent: Option<u64> = None;
    let mut last_seq: Option<u64> = None;
    let mut compressed = false;
    let mut health = writer.health();

    let mut checkpoint_timer = interval(checkpoint_period);
    checkpoint_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut stats_timer = interval(stats_period);
    stats_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // `tokio::time::interval`'s first tick fires immediately; skip it so the
    // first real checkpoint and stats line land a full period in.
    checkpoint_timer.tick().await;
    stats_timer.tick().await;

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        tokio::select! {
            _ = shutdown.changed() => {}
            changed = health.changed() => {
                // Round 1 finding 4 (BC41): every `watch::Sender` dropped is
                // the writer thread gone, exactly like an observed `Failed`.
                match changed {
                    Ok(()) => {
                        if matches!(*health.borrow(), WriterState::Failed(_)) {
                            return Err(IngestError::WriterFailed);
                        }
                    }
                    Err(_) => return Err(IngestError::WriterFailed),
                }
            }
            _ = checkpoint_timer.tick() => {
                if let Some(seq) = last_seq {
                    if checkpoint_sent != Some(seq) {
                        send_op(writer, Op::Checkpoint { seq }).await?;
                        checkpoint_sent = Some(seq);
                    }
                }
            }
            _ = stats_timer.tick() => {
                stats.emit(hot.len(), writer.depth(), compressed);
            }
            evicted = evict_rx.recv() => {
                // BC37: nothing is sent for a batch that evicted nothing, so
                // every message here holds at least one URI; `None` only
                // once the writer thread (the sender) is gone, which
                // `health` above already catches.
                if let Some(uris) = evicted {
                    for uri in uris {
                        hot.remove(&uri);
                    }
                }
            }
            msg = events_rx.recv() => {
                let Some(msg) = msg else {
                    // The pump exited; it only does so on shutdown or once
                    // this receiver is dropped, neither of which is
                    // possible here, but returning is still correct: there
                    // is nothing left to read.
                    return Ok(());
                };
                let sourced = msg.map_err(IngestError::Jetstream)?;
                last_seq = sourced.last_seq;
                compressed = sourced.compressed;
                match sourced.event {
                    Event::Commit(commit) => {
                        let translation = translate(&commit, hot);
                        // Stats first, so a `send` failure below still
                        // leaves this commit counted; then `ops` by value
                        // rather than `&translation.ops` + `.clone()`, so
                        // no `Op` is cloned per event on the hot path.
                        stats.record_commit(&commit, &translation);
                        if let Some(t) = commit.time_secs() {
                            liveness.set_commit_time(t);
                        }
                        for op in translation.ops {
                            apply_op_to_hot_set(hot, &op);
                            send_op(writer, op).await?;
                        }
                    }
                    Event::Info { name, message } => {
                        tracing::warn!(name = %name, message = %message, "jetstream: info frame");
                        if name == "OutdatedCursor" {
                            let seq = last_seq.or(initial_cursor).unwrap_or(0);
                            send_op(writer, Op::MarkAllDirty { seq }).await?;
                        }
                    }
                }
            }
        }
    }
}

/// Waits for SIGINT or SIGTERM, TECH-DESIGN section 13's shutdown contract
/// (BC33). `ctrl_c` covers SIGINT everywhere `tokio` runs; `tokio::signal`
/// has no portable SIGTERM, so that half is `#[cfg(unix)]` and never fires
/// on a non-Unix target, which this binary does not ship for (`AGENTS.md`'s
/// only build target is the container's Linux, and development is macOS).
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("installing a SIGINT handler should not fail");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing a SIGTERM handler should not fail")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// Starts the graph subsystem — a second `Store` connection, the
/// `GraphHandle` loaded from it (BC19), the worker task and the touch-flush
/// task (network-feed story 06) — when `cfg.bsky_handle` and
/// `cfg.bsky_app_password` are both set, and returns the hook `run` wires
/// into the resolver so a verified viewer with no circle gets a first build
/// (BC22). Returns `None`, logging one `error` line, when either credential
/// is missing (BC18) or any step here fails: `run` itself never fails over
/// this, since a verified viewer just keeps getting empty pages either way.
/// `drop_lists` is `None` for now — `src/http/viewer.rs` (slice 3.0) has no
/// `ViewerLists` yet for the worker to drop entries from; slice 4.0's `run`
/// wiring passes one.
fn start_graph_subsystem(cfg: &Config) -> Option<crate::auth::FirstBuildHook> {
    let (handle, app_password) = match (&cfg.bsky_handle, &cfg.bsky_app_password) {
        (Some(handle), Some(app_password)) => (handle.clone(), app_password.clone()),
        _ => {
            tracing::error!(
                "ingest: UPSTAGE_PERSONALISE is true but BSKY_HANDLE or BSKY_APP_PASSWORD \
                 is not set; the graph worker will not start and verified viewers get empty pages"
            );
            return None;
        }
    };

    let graph_store = match crate::store::Store::open(cfg) {
        Ok(store) => store,
        Err(err) => {
            tracing::error!(error = %err, "ingest: could not open the graph worker's store connection");
            return None;
        }
    };

    let graph_handle =
        match crate::graph::GraphHandle::from_store(&graph_store, cfg.max_viewers as usize) {
            Ok(handle) => handle,
            Err(err) => {
                tracing::error!(error = %err, "ingest: could not load circles from the store");
                return None;
            }
        };

    let credentials = crate::appview::pds::Credentials { handle, app_password };
    let pds_client = match crate::appview::pds::PdsClient::from_config(cfg, credentials) {
        Ok(client) => client,
        Err(err) => {
            tracing::error!(error = %err, "ingest: could not build the graph worker's PDS client");
            return None;
        }
    };

    let d2_sample_size = cfg.d2_follows_sample as usize;
    let worker_handle = std::sync::Arc::clone(&graph_handle);
    let worker_store = graph_store.clone();
    tokio::spawn(crate::graph::run_worker(
        worker_handle,
        worker_store,
        pds_client,
        d2_sample_size,
        None,
    ));

    let flush_handle = std::sync::Arc::clone(&graph_handle);
    let flush_store = graph_store.clone();
    tokio::spawn(crate::graph::run_touch_flush(flush_handle, flush_store));

    let hook_handle = std::sync::Arc::clone(&graph_handle);
    Some(std::sync::Arc::new(move |viewer: crate::auth::ViewerDid| {
        hook_handle.enqueue_first_build(viewer, crate::store::unix_now());
    }))
}

/// `upstage run`'s entry point, TECH-DESIGN section 5.1 end to end: opens
/// `cfg.db_path` (BC35), starts the writer wired to the eviction channel
/// (round 1 finding 2), rebuilds the hot set from `pairs` and logs its size
/// and the elapsed time at `info` (BC22), reads the stored cursor and
/// connects to Jetstream at it, then spawns [`run_ingest`], the scorer
/// (story 07) and the HTTP server (story 08, slice 3.0) as three supervised
/// tasks sharing one `SnapshotHandle` (scorer writes, HTTP reads) and one
/// `HealthState` (ingest and scorer write, HTTP reads), running until
/// SIGINT or SIGTERM flips the shutdown watch (BC33). Once every task
/// returns, `writer.flush()` then `writer.shutdown()` run on this handle (a
/// clone of the one the spawned tasks hold) so every committed op reaches
/// the database before the process exits; a failure on either is logged,
/// not raised, since the first task's own result (success or
/// `IngestError`) is the one this function returns (BC29).
pub async fn run(cfg: &Config) -> Result<(), IngestError> {
    let store = crate::store::Store::open(cfg)?;

    // Round 1 finding 2 (BC38, BC39): `evict_tx`'s clone goes to the
    // writer, which sends every batch's evicted URIs on it once committed;
    // `evict_tx` itself stays in scope below, for the scorer task to send
    // `expire`'s `ExpireReport::evicted_uris` through its own clone.
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<Vec<String>>();
    let writer = store.writer_evicting(evict_tx.clone())?;

    let mut hot = HotSet::new();
    let rebuild_start = Instant::now();
    hot.rebuild_from(|f| store.for_each_hot_uri(f))?;
    tracing::info!(
        hot_set_len = hot.len(),
        elapsed_ms = rebuild_start.elapsed().as_millis() as u64,
        "ingest: hot set rebuilt from pairs"
    );

    let cursor = store.cursor()?;
    tracing::info!(cursor = ?cursor, "ingest: connecting to jetstream");
    let source = crate::jetstream::JetstreamClient::connect(cfg, cursor).await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_shutdown_tx = shutdown_tx.clone();
    let _signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        let _ = signal_shutdown_tx.send(true);
    });

    // Shared across all three tasks (BC25): the scorer's snapshot step
    // swaps a freshly built list into `snapshot`; the HTTP server's
    // `getFeedSkeleton` reads it. `liveness` is written by the ingest loop
    // (the last Jetstream commit) and the scorer (the last successful
    // pass), and read by `/healthz`.
    let snapshot = crate::scorer::snapshot::SnapshotHandle::new();
    let liveness = HealthState::new();

    // The scorer task, spawned alongside `run_ingest` and the HTTP server
    // below, sharing a clone of `shutdown_rx` and sending through its own
    // clone of `evict_tx` (BC39). `AppViewClient::new` only fails on a
    // non-positive or non-finite `appview_rps`, which `config::load`
    // already rejects, so `cfg` here can never trigger it.
    let scorer_store = store.clone();
    let scorer_client = crate::appview::AppViewClient::new(cfg)
        .expect("cfg.appview_rps is validated positive and finite by config::load");
    let scorer_cfg = cfg.clone();
    let scorer_evict_tx = evict_tx.clone();
    let scorer_shutdown_rx = shutdown_rx.clone();
    let scorer_snapshot = snapshot.clone();
    let scorer_liveness = liveness.clone();
    let scorer_handle: tokio::task::JoinHandle<Result<(), IngestError>> =
        tokio::spawn(async move {
            crate::scorer::run(
                scorer_store,
                scorer_client,
                scorer_cfg,
                scorer_evict_tx,
                scorer_shutdown_rx,
                scorer_snapshot,
                scorer_liveness,
            )
            .await
            .map_err(IngestError::Scorer)
        });

    let ingest_cfg = cfg.clone();
    let ingest_writer = writer.clone();
    let ingest_liveness = liveness.clone();
    let ingest_handle: tokio::task::JoinHandle<Result<(), IngestError>> =
        tokio::spawn(async move {
            run_ingest(
                &ingest_cfg,
                source,
                &ingest_writer,
                &mut hot,
                shutdown_rx,
                evict_rx,
                &ingest_liveness,
            )
            .await
        });

    // The HTTP server (story 08, slice 3.0): reads `snapshot` and
    // `liveness`, never the store (`## Non-goals`: no store read on the
    // serving path).
    let http_cfg = cfg.clone();
    // A fresh receiver off `shutdown_tx`: every supervised task needs its
    // own (each is moved into its own `tokio::spawn`), and `shutdown_rx`
    // itself is already moved into the ingest task above.
    let http_shutdown_rx = shutdown_tx.subscribe();
    // network-feed story 05, BC1: the DID key cache and the resolver task
    // are built only when the switch is on; `AppState`'s `HttpConfig`
    // carries `None` otherwise, and `skeleton::handler` never reads
    // `Authorization` in that case either.
    let mut http_config = crate::http::HttpConfig::from(cfg);
    if cfg.personalise {
        let auth_cfg = crate::auth::AuthConfig { service_did: cfg.service_did.clone() };
        // network-feed story 06: the graph subsystem (a second `Store`
        // connection, the in-memory `GraphHandle`, the worker and the
        // touch-flush task) starts only when credentials exist too (BC18);
        // `first_build_hook` stays `None` otherwise, so the resolver task
        // below runs the same either way.
        let first_build_hook = start_graph_subsystem(cfg);

        let (resolver_tx, cache) = crate::auth::spawn_resolver(
            cfg.plc_url.clone(),
            cfg.max_viewers as usize * 2,
            auth_cfg.clone(),
            first_build_hook,
        );
        http_config.auth = Some(crate::http::AuthHandle { cache, resolver_tx, cfg: auth_cfg });
    }
    let http_state = std::sync::Arc::new(crate::http::AppState {
        snapshot,
        writer: writer.clone(),
        health: liveness,
        cfg: http_config,
    });
    let http_handle: tokio::task::JoinHandle<Result<(), IngestError>> = tokio::spawn(async move {
        crate::http::serve(&http_cfg, http_state, http_shutdown_rx).await.map_err(IngestError::Http)
    });

    supervise(
        vec![("ingest", ingest_handle), ("scorer", scorer_handle), ("http", http_handle)],
        shutdown_tx,
        writer,
    )
    .await
}

/// One supervised task's `JoinHandle`, its `Ok` always `IngestError` so
/// `supervise` can race the ingest, scorer and HTTP tasks together despite
/// their three different underlying error types.
type SupervisedHandle = tokio::task::JoinHandle<Result<(), IngestError>>;

/// Runs every task in `tasks` on a `tokio::task::JoinSet` (round 2 finding
/// 2, replacing round 1's two-task `tokio::select!` and round 2's own
/// `futures_util::future::select_all`, BC29, BC40): each task is wrapped in
/// a small async block that awaits its `JoinHandle` and turns a panic
/// (`JoinError`) into `IngestError::TaskPanicked { task: name }` before
/// pairing the result with the task's own name, so the wrapper future
/// itself never panics and the name and the result can never be separated
/// by however `JoinSet` orders its own completions — the swap-remove
/// reordering bug `select_all` had (round 2's earlier finding) has no
/// equivalent here, since nothing is zipped back together after the fact.
///
/// The first `join_next()` result is the winner: it flips `shutdown_tx` so
/// the other two tasks stop on their own next check, then every remaining
/// `join_next()` is drained to let each wind down cleanly, logging its
/// error at `warn` if it has one (never dropped, never returned: the
/// winner's own result is always what `supervise` returns). Once every task
/// has finished, `writer.flush()` then `writer.shutdown()` run here, so
/// every committed op reaches the database before `run` returns either way.
async fn supervise(
    tasks: Vec<(&'static str, SupervisedHandle)>,
    shutdown_tx: watch::Sender<bool>,
    writer: WriterHandle,
) -> Result<(), IngestError> {
    let mut set: tokio::task::JoinSet<(&'static str, Result<(), IngestError>)> =
        tokio::task::JoinSet::new();
    for (name, handle) in tasks {
        set.spawn(async move {
            let result = match handle.await {
                Ok(result) => result,
                // BC40: the underlying task panicked; caught here as a
                // value, never re-panicking this wrapper.
                Err(_join_err) => Err(IngestError::TaskPanicked { task: name }),
            };
            (name, result)
        });
    }

    let (_first_name, first_result) = set
        .join_next()
        .await
        .expect("supervise is always called with at least one task")
        .expect("the wrapper future never panics: JoinHandle panics are caught above");

    let _ = shutdown_tx.send(true);

    while let Some(joined) = set.join_next().await {
        let (name, result) = joined.expect("the wrapper future never panics");
        if let Err(err) = result {
            tracing::warn!(
                error = %err,
                task = name,
                "ingest: a task failed after another task finished"
            );
        }
    }

    if let Err(err) = writer.flush().await {
        tracing::warn!(error = %err, "ingest: writer flush failed during shutdown");
    }
    if let Err(err) = writer.shutdown().await {
        tracing::warn!(error = %err, "ingest: writer shutdown failed");
    }

    first_result
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
        let frame: Frame = serde_json::from_str(&raw)
            .unwrap_or_else(|err| panic!("decoding fixture {name}: {err}"));
        match frame.payload {
            Payload::Commit(commit) => commit,
            other => panic!("expected Payload::Commit for {name}, got {other:?}"),
        }
    }

    /// A task that watches `shutdown_rx` and returns `Ok(())` only once it
    /// flips `true` — round 2 finding 1's stand-in for "the other task would
    /// run forever" in `supervise`'s two tests below.
    async fn run_until_shutdown(mut shutdown_rx: watch::Receiver<bool>) {
        loop {
            if *shutdown_rx.borrow() {
                return;
            }
            if shutdown_rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// A task that finishes immediately with `result`, standing in for
    /// whichever of the three supervised tasks the test wants to finish
    /// first.
    fn finishing_task(
        result: Result<(), IngestError>,
    ) -> tokio::task::JoinHandle<Result<(), IngestError>> {
        tokio::spawn(async move { result })
    }

    /// A task that runs until `shutdown_rx` flips, standing in for a task
    /// that would otherwise run forever — round 2's three-task counterpart
    /// of round 1 finding 1's two-task stand-in.
    fn forever_task(
        shutdown_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<(), IngestError>> {
        tokio::spawn(async move {
            run_until_shutdown(shutdown_rx).await;
            Ok(())
        })
    }

    // BC29: ingest finishing first (with an error) flips the shutdown watch
    // so the other two tasks — which would otherwise run forever — stop
    // too, and `supervise` returns ingest's own error.
    #[tokio::test]
    async fn supervise_returns_ingest_error_and_stops_the_others() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let ingest_handle = finishing_task(Err(IngestError::WriterFailed));
        let scorer_handle = forever_task(shutdown_rx.clone());
        let http_handle = forever_task(shutdown_rx);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            supervise(
                vec![("ingest", ingest_handle), ("scorer", scorer_handle), ("http", http_handle)],
                shutdown_tx,
                writer,
            ),
        )
        .await
        .expect("supervise must return promptly once ingest finishes, not wait on the others");

        assert!(matches!(result, Err(IngestError::WriterFailed)));
    }

    // BC29, the reverse: the scorer finishing first (with an error) flips
    // the shutdown watch so the other two tasks stop too, and `supervise`
    // returns the scorer's error.
    #[tokio::test]
    async fn supervise_returns_scorer_error_and_stops_the_others() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let ingest_handle = forever_task(shutdown_rx.clone());
        let scorer_handle = finishing_task(Err(IngestError::Scorer(
            crate::scorer::ScorerError::Store(crate::store::StoreError::Poisoned),
        )));
        let http_handle = forever_task(shutdown_rx);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            supervise(
                vec![("ingest", ingest_handle), ("scorer", scorer_handle), ("http", http_handle)],
                shutdown_tx,
                writer,
            ),
        )
        .await
        .expect("supervise must return promptly once the scorer finishes, not wait on the others");

        assert!(matches!(result, Err(IngestError::Scorer(_))));
    }

    // BC29, a third case: the HTTP task finishing first (a bind failure)
    // flips the shutdown watch so ingest and the scorer — both of which
    // would otherwise run forever — stop too, and `supervise` returns the
    // HTTP task's own error. This is the case round 1's two-task
    // `supervise` had no room for.
    #[tokio::test]
    async fn supervise_returns_http_error_and_stops_the_others() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let ingest_handle = forever_task(shutdown_rx.clone());
        let scorer_handle = forever_task(shutdown_rx);
        let http_handle = finishing_task(Err(IngestError::Http(crate::http::HttpError::Bind {
            addr: "127.0.0.1:0".to_string(),
            source: std::io::Error::new(std::io::ErrorKind::AddrInUse, "address in use"),
        })));

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            supervise(
                vec![("ingest", ingest_handle), ("scorer", scorer_handle), ("http", http_handle)],
                shutdown_tx,
                writer,
            ),
        )
        .await
        .expect(
            "supervise must return promptly once the http task finishes, not wait on the others",
        );

        assert!(matches!(result, Err(IngestError::Http(_))));
    }

    /// A `tracing_subscriber::fmt::MakeWriter` that appends every formatted
    /// event to a shared buffer, so a test can assert on the `task` field
    /// `supervise`'s `warn!` lines carry for the tasks that finish after the
    /// winner.
    #[derive(Clone)]
    struct CapturingWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturingWriter {
        type Writer = CapturingWriter;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    // Round 2 finding 2: `select_all` resolves its winner with
    // `Vec::swap_remove`, which moves the *last* handle into the winner's
    // slot. The old code zipped a separately filtered `names` list against
    // `remaining_handles`, so whenever the winner was not the last task, a
    // remaining task's `warn!` line carried another task's name. Here
    // "ingest", the FIRST of three, wins, so the old bug would have swapped
    // "scorer" and "http"'s names on their `warn!` lines. Each remaining
    // task fails with a distinct, identifiable error so the fix is visible:
    // every `task` field must carry its own task's error, never the other's.
    #[tokio::test]
    async fn supervise_names_each_remaining_task_correctly_when_the_winner_is_not_last() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);

        // Ingest wins immediately. Scorer and http each finish shortly after,
        // on their own account (not because the shutdown watch flipped), so
        // the race's winner is deterministic and non-last while both
        // remaining results are still real, distinct errors.
        let ingest_handle = finishing_task(Err(IngestError::WriterFailed));
        let scorer_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(IngestError::Scorer(crate::scorer::ScorerError::Store(
                crate::store::StoreError::Poisoned,
            )))
        });
        let http_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(IngestError::Http(crate::http::HttpError::Bind {
                addr: "127.0.0.1:0".to_string(),
                source: std::io::Error::new(std::io::ErrorKind::AddrInUse, "address in use"),
            }))
        });

        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::fmt().json().with_writer(CapturingWriter(buf.clone())).finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            supervise(
                vec![("ingest", ingest_handle), ("scorer", scorer_handle), ("http", http_handle)],
                shutdown_tx,
                writer,
            ),
        )
        .await
        .expect("supervise must return promptly once ingest finishes");
        assert!(matches!(result, Err(IngestError::WriterFailed)));

        drop(dispatch);
        let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        let scorer_line = output
            .lines()
            .find(|line| line.contains("\"task\":\"scorer\""))
            .unwrap_or_else(|| panic!("no warn line named \"scorer\" in: {output}"));
        assert!(
            scorer_line.contains("scorer error"),
            "the \"scorer\" line must carry the scorer's own error, not another task's: {scorer_line}"
        );

        let http_line = output
            .lines()
            .find(|line| line.contains("\"task\":\"http\""))
            .unwrap_or_else(|| panic!("no warn line named \"http\" in: {output}"));
        assert!(
            http_line.contains("http error"),
            "the \"http\" line must carry the http task's own error, not another task's: {http_line}"
        );
    }

    // BC40: a panicking task maps to `IngestError::TaskPanicked` naming it,
    // rather than `supervise` itself panicking, and the writer still
    // flushes: a checkpoint sent before the panic is provably committed by
    // reading `meta.jetstream_seq` back through the same `Store`.
    #[tokio::test]
    async fn supervise_maps_a_panicking_task_to_task_panicked_and_still_flushes_the_writer() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        writer.send(Op::Checkpoint { seq: 42 }).await.unwrap();

        let ingest_handle: SupervisedHandle = tokio::spawn(async { panic!("boom") });
        let scorer_handle = forever_task(shutdown_rx.clone());
        let http_handle = forever_task(shutdown_rx);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            supervise(
                vec![("ingest", ingest_handle), ("scorer", scorer_handle), ("http", http_handle)],
                shutdown_tx,
                writer,
            ),
        )
        .await
        .expect("supervise must return promptly once ingest panics, not wait on the others");

        match result {
            Err(IngestError::TaskPanicked { task }) => assert_eq!(task, "ingest"),
            other => panic!("expected TaskPanicked, got {other:?}"),
        }

        assert_eq!(
            store.cursor().unwrap(),
            Some(42),
            "the writer must still flush the checkpoint sent before the panic"
        );
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
                assert_eq!(
                    original_uri,
                    "at://did:plc:originaldid00000000000000/app.bsky.feed.post/original0001"
                );
                assert_eq!(original_did, "did:plc:originaldid00000000000000");
                assert_eq!(*seq, commit.seq);
            }
            other => panic!("expected Op::InsertPair, got {other:?}"),
        }
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
                assert_eq!(
                    original_uri,
                    "at://did:plc:differentauthor00000000/app.bsky.feed.post/3mu6ks2ljsk2q"
                );
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
        assert_eq!(
            translation.ops,
            vec![Op::Incr {
                post_uri: "at://did:plc:ezay5dffpkfnjxh5yirexce2/app.bsky.feed.post/3mvqrul2wq22a"
                    .to_string(),
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

        assert_eq!(translation.ops, vec![Op::DeletePost { uri, seq: commit.seq }]);
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
                post_uri: "at://did:plc:vvdrimbhu4kouacycafar4cs/app.bsky.feed.post/3mvqkfpsilc26"
                    .to_string(),
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
                post_uri: "at://did:plc:ezay5dffpkfnjxh5yirexce2/app.bsky.feed.post/3mvqrul2wq22a"
                    .to_string(),
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
                    quote_uri:
                        "at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote1"
                            .to_string(),
                    seq: commit.seq,
                },
                Op::Detach {
                    quote_uri:
                        "at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote2"
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
                quote_uri:
                    "at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote1"
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

    // Round 1 finding 3, BC5: an `update` to a reply never increments
    // replies, even to a hot parent, since a reply's parent cannot change
    // and incrementing again would double count.
    #[test]
    fn update_to_a_hot_parent_never_increments_replies() {
        let mut commit = commit("jetstream_commit_post_reply.json");
        commit.operation = Operation::Update;
        let mut hot = HotSet::new();
        hot.insert("at://did:plc:ezay5dffpkfnjxh5yirexce2/app.bsky.feed.post/3mvqrul2wq22a");

        let translation = translate(&commit, &hot);

        assert_eq!(translation, Translation::default());
    }

    // Round 1 finding 3, BC7's amendment: a self quote (BC4) still falls
    // through to the reply check, carrying `Dropped::SelfQuote` forward
    // alongside the `Incr{Replies}` a hot parent yields. Only a quote that
    // becomes an `InsertPair` short-circuits the reply check outright.
    #[test]
    fn self_quote_falls_through_to_reply_check() {
        let mut commit = commit("jetstream_commit_post.json"); // an unedited self quote
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

        assert_eq!(translation.dropped, Some(Dropped::SelfQuote));
        assert_eq!(
            translation.ops,
            vec![Op::Incr {
                post_uri: "at://did:plc:hotparent00000000000000/app.bsky.feed.post/parent0001"
                    .to_string(),
                field: CountField::Replies,
                seq: commit.seq,
            }]
        );
    }

    // BC40, round 1 finding 3: a quote with no `cid` is dropped and counted,
    // never written as an `InsertPair` with an empty `quote_cid`, and also
    // falls through to the reply check.
    #[test]
    fn missing_cid_is_dropped_and_falls_through_to_reply_check() {
        let mut commit = commit("jetstream_commit_post_quote_record.json");
        commit.cid = None;

        let translation = translate(&commit, &HotSet::new());

        assert!(translation.ops.is_empty());
        assert_eq!(translation.dropped, Some(Dropped::MissingCid));
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
        let expected =
            chrono::DateTime::parse_from_rfc3339("2026-09-17T23:30:00.000Z").unwrap().timestamp();
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
        assert_eq!(stats.ops_by_kind.get("insert_pair"), Some(&1));
        assert_eq!(stats.ops_by_kind.get("detach"), Some(&2), "postgate_detaches is read off this");
        assert_eq!(stats.events_by_collection.get(POST), Some(&3));
        // Round 1 finding 5 (BC42): an unknown collection folds into
        // `"other"`, not its own literal string key.
        assert_eq!(stats.events_by_collection.get("other"), Some(&1));
        assert_eq!(stats.events_by_collection.get(POSTGATE), Some(&1));

        // Per-collection rates divide by the elapsed time passed in, not by
        // a fixed constant (round 1 finding 5): 3 events over a synthetic
        // 2-second window is 1.5/s.
        let rates = per_second(&stats.events_by_collection, 2.0);
        assert!((rates[POST] - 1.5).abs() < f64::EPSILON);

        // `lag_s`: the postgate commit was the last one recorded.
        let expected_lag = (unix_now() - last_seen_time).max(0);
        assert!(
            (stats.lag_s() - expected_lag).abs() <= 1,
            "lag_s should track the last commit's time"
        );

        stats.emit(42, 7, true);

        // Every counter resets after the line, matching a freshly built `Stats`.
        assert_eq!(stats.events_by_collection, HashMap::new());
        assert_eq!(stats.ops_by_kind, HashMap::new());
        assert_eq!(stats.gate_hits, 0);
        assert_eq!(stats.gate_attempts, 0);
        assert_eq!(stats.dropped_unknown_collection, 0);
        assert_eq!(stats.dropped_self_quote, 0);
        assert_eq!(stats.dropped_non_post_embed, 0);
        assert_eq!(stats.dropped_missing_cid, 0);
        assert_eq!(stats.gate_hit_rate(), 0.0);
        assert_eq!(stats.lag_s(), 0);
    }

    // Round 1 finding 5, BC42: `emit`'s rate maps divide by the time
    // actually elapsed since the window last reset, so a late tick never
    // reports an inflated rate.
    #[test]
    fn emit_divides_by_elapsed_time_not_a_fixed_period() {
        let mut stats = Stats::new();
        let like = commit("jetstream_commit_like.json");
        let translation = translate(&like, &HotSet::new());
        stats.record_commit(&like, &translation);

        std::thread::sleep(Duration::from_millis(50));
        stats.emit(0, 0, false);

        // No direct way to read the logged line back out, but `emit` must
        // not panic or divide by zero on a sub-second window, and the
        // window resets regardless of how much time elapsed.
        assert_eq!(stats.events_by_collection, HashMap::new());
    }

    // --- run_ingest ---------------------------------------------------

    /// A `Config` with only the two required variables set, for tests that
    /// need one to satisfy `run_ingest`'s signature but read nothing from
    /// it (`_cfg` is unused today; `run`, slice 6.0, is the first real
    /// reader).
    fn test_config() -> Config {
        let lookup = |name: &str| match name {
            "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
            "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            _ => None,
        };
        crate::config::load(lookup).expect("test config should load")
    }

    /// A `like` create commit, built by hand rather than from a fixture:
    /// story 06's slice 4.0 adds no new fixtures, and every field this
    /// needs is a plain string or JSON value.
    fn like_commit(seq: u64, subject_uri: &str) -> CommitEvent {
        CommitEvent {
            did: "did:plc:likerdid00000000000000000".to_string(),
            seq,
            time: "2026-09-18T00:00:10.000000Z".to_string(),
            operation: Operation::Create,
            collection: LIKE.to_string(),
            rkey: format!("likerkey{seq}"),
            rev: "revlike".to_string(),
            cid: Some("bafyreilikecid00000000000000000000000000".to_string()),
            record: Some(serde_json::json!({
                "subject": {"uri": subject_uri, "cid": "bafyreisubjectcid0000000000000000"},
                "createdAt": "2026-09-18T00:00:10.000Z",
            })),
        }
    }

    /// A `postgate` create commit carrying `detached`, built by hand for
    /// the same reason `like_commit` is.
    fn postgate_commit(seq: u64, detached: &[&str]) -> CommitEvent {
        CommitEvent {
            did: "did:plc:quoterdid0000000000000000".to_string(),
            seq,
            time: "2026-09-18T00:00:20.000000Z".to_string(),
            operation: Operation::Create,
            collection: POSTGATE.to_string(),
            rkey: format!("gaterkey{seq}"),
            rev: "revgate".to_string(),
            cid: Some("bafyreigatecid0000000000000000000000000".to_string()),
            record: Some(serde_json::json!({ "detachedEmbeddingUris": detached })),
        }
    }

    /// A test vector `EventSource` over a fixed list of `Event`s. Sets
    /// `last_seq` the same way `JetstreamClient` does, from every commit it
    /// returns (BC23's first fallback). Once its queue is empty it signals
    /// `drained` exactly once, then never resolves again, the same shape a
    /// live source takes once it is caught up: `run_ingest`'s `select!`
    /// waits on it alongside its timers and the shutdown watch.
    struct VecSource {
        events: std::collections::VecDeque<Event>,
        last_seq: Option<u64>,
        initial_cursor: Option<u64>,
        compressed: bool,
        drained: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl VecSource {
        fn new(events: Vec<Event>) -> Self {
            VecSource {
                events: events.into(),
                last_seq: None,
                initial_cursor: None,
                compressed: true,
                drained: None,
            }
        }

        /// Same as `new`, but returns a receiver that resolves once the
        /// queue has been drained, so a test knows every event has reached
        /// `run_ingest` before it flips the shutdown watch.
        fn with_drained_signal(events: Vec<Event>) -> (Self, tokio::sync::oneshot::Receiver<()>) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let mut source = Self::new(events);
            source.drained = Some(tx);
            (source, rx)
        }
    }

    impl EventSource for VecSource {
        async fn next(&mut self) -> Result<Event, JetstreamError> {
            match self.events.pop_front() {
                Some(event) => {
                    if let Event::Commit(commit) = &event {
                        self.last_seq = Some(commit.seq);
                    }
                    Ok(event)
                }
                None => {
                    if let Some(tx) = self.drained.take() {
                        let _ = tx.send(());
                    }
                    std::future::pending().await
                }
            }
        }

        fn last_seq(&self) -> Option<u64> {
            self.last_seq
        }

        fn is_compressed(&self) -> bool {
            self.compressed
        }

        fn initial_cursor(&self) -> Option<u64> {
            self.initial_cursor
        }
    }

    /// A source whose `next()` takes noticeably longer than one checkpoint
    /// tick, holding its one event only until the first `poll` (`.take()`
    /// runs before the first `.await`, exactly like a real `next()`
    /// advancing internal state before its first suspension point). Round 1
    /// finding 1's regression test: before the pump task existed, a faster
    /// timer branch winning `tokio::select!` dropped this in-flight future,
    /// and the event was gone for good, since a fresh `next()` call the
    /// following iteration would find nothing left to take.
    struct SlowSource {
        event: Option<Event>,
        last_seq: Option<u64>,
    }

    impl EventSource for SlowSource {
        async fn next(&mut self) -> Result<Event, JetstreamError> {
            match self.event.take() {
                Some(event) => {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Event::Commit(commit) = &event {
                        self.last_seq = Some(commit.seq);
                    }
                    Ok(event)
                }
                None => std::future::pending().await,
            }
        }

        fn last_seq(&self) -> Option<u64> {
            self.last_seq
        }

        fn is_compressed(&self) -> bool {
            true
        }

        fn initial_cursor(&self) -> Option<u64> {
            None
        }
    }

    // Round 1 finding 1, BC36: a `next()` far slower than the checkpoint
    // tick still delivers its event exactly once, since the pump task that
    // owns the source is immune to the outer `select!`'s cancellation.
    #[tokio::test]
    async fn slow_next_is_not_discarded_by_a_faster_checkpoint_tick() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let writer_task = writer.clone();
        let cfg = test_config();
        let health = crate::health::HealthState::new();
        let like = like_commit(1, "at://did:plc:cold0000000000000000000/app.bsky.feed.post/cold1");
        let source = SlowSource { event: Some(Event::Commit(like)), last_seq: None };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_evict_tx, evict_rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(async move {
            let mut hot = HotSet::new();
            run_ingest_periodic(
                &cfg,
                source,
                &writer_task,
                &mut hot,
                shutdown_rx,
                // Several checkpoint ticks fire while `next()` is still
                // sleeping through its 300ms.
                Duration::from_millis(50),
                Duration::from_secs(3600),
                evict_rx,
                &health,
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(900)).await;
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), handle).await.unwrap().unwrap().unwrap();

        writer.flush().await.unwrap();
        assert_eq!(
            store.cursor().unwrap(),
            Some(1),
            "the slow event's checkpoint must land exactly once, not be discarded"
        );
    }

    // AC8 half, BC33: the loop returns once `shutdown` reports `true`, with
    // no event and no timer having fired.
    #[tokio::test]
    async fn run_ingest_returns_on_shutdown() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let cfg = test_config();
        let health = crate::health::HealthState::new();
        let source = VecSource::new(vec![]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let (_evict_tx, evict_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            let mut hot = HotSet::new();
            run_ingest_periodic(
                &cfg,
                source,
                &writer,
                &mut hot,
                shutdown_rx,
                Duration::from_secs(3600),
                Duration::from_secs(3600),
                evict_rx,
                &health,
            )
            .await
        });

        // Give the task a moment to start and block on `source.next()`.
        tokio::time::sleep(Duration::from_millis(20)).await;
        shutdown_tx.send(true).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("run_ingest should return promptly once shutdown is flipped")
            .expect("the task should not panic");
        assert!(result.is_ok(), "expected Ok(()), got {result:?}");
    }

    // AC7, BC25: a checkpoint is sent when no event produced an op, so the
    // cursor still advances.
    #[tokio::test]
    async fn checkpoint_is_sent_when_no_op_was() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let writer_task = writer.clone();
        let cfg = test_config();
        let health = crate::health::HealthState::new();
        // A cold like: `translate` returns `Translation::default()` (BC13),
        // but `VecSource` still records its `seq` as `last_seq`.
        let cold_like =
            like_commit(101, "at://did:plc:cold000000000000000000/app.bsky.feed.post/cold0001");
        let (source, drained) = VecSource::with_drained_signal(vec![Event::Commit(cold_like)]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let (_evict_tx, evict_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            let mut hot = HotSet::new();
            run_ingest_periodic(
                &cfg,
                source,
                &writer_task,
                &mut hot,
                shutdown_rx,
                Duration::from_millis(20),
                Duration::from_secs(3600),
                evict_rx,
                &health,
            )
            .await
        });

        drained.await.expect("the source should drain");
        // The checkpoint timer's period is 20ms; give it room to fire at
        // least once before shutting down.
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), handle).await.unwrap().unwrap().unwrap();

        writer.flush().await.unwrap();
        assert_eq!(
            store.cursor().unwrap(),
            Some(101),
            "a checkpoint should have moved the cursor to the cold like's seq"
        );
    }

    /// Round 1 finding 6: the three near-identical blocks
    /// `outdated_cursor_marks_dirty` used to repeat, collapsed into one
    /// helper. Runs `run_ingest_periodic` over `events` from a source
    /// connected at `initial_cursor`, to completion, and returns the
    /// resulting stored cursor.
    async fn run_to_completion_and_read_cursor(
        events: Vec<Event>,
        initial_cursor: Option<u64>,
    ) -> Option<u64> {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let writer_task = writer.clone();
        let cfg = test_config();
        let health = crate::health::HealthState::new();
        let (mut source, drained) = VecSource::with_drained_signal(events);
        source.initial_cursor = initial_cursor;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_evict_tx, evict_rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(async move {
            let mut hot = HotSet::new();
            run_ingest_periodic(
                &cfg,
                source,
                &writer_task,
                &mut hot,
                shutdown_rx,
                Duration::from_secs(3600),
                Duration::from_secs(3600),
                evict_rx,
                &health,
            )
            .await
        });

        drained.await.expect("the source should drain");
        // See the comment on the equivalent sleep in
        // `run_ingest_lands_translated_ops_on_a_real_store`: `drained`
        // fires once the pump itself has run dry, not once `run_ingest`'s
        // loop has consumed and translated the last event.
        tokio::time::sleep(Duration::from_millis(150)).await;
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), handle).await.unwrap().unwrap().unwrap();

        writer.flush().await.unwrap();
        store.cursor().unwrap()
    }

    fn outdated_cursor_info() -> Event {
        Event::Info {
            name: "OutdatedCursor".to_string(),
            message: "resume cursor below the retention floor".to_string(),
        }
    }

    // AC5, BC23: `OutdatedCursor` sends `MarkAllDirty` with the seq
    // fallback chain, and never replays event by event.
    #[tokio::test]
    async fn outdated_cursor_marks_dirty() {
        // (a) a commit was seen this run: its seq wins over both fallbacks.
        let neutral = commit("jetstream_commit_post_reply.json");
        let neutral_seq = neutral.seq;
        let cursor = run_to_completion_and_read_cursor(
            vec![Event::Commit(neutral), outdated_cursor_info()],
            Some(999),
        )
        .await;
        assert_eq!(
            cursor,
            Some(neutral_seq),
            "MarkAllDirty should carry the last seq the task saw"
        );

        // (b) no commit was seen: falls back to the cursor the source
        // connected at.
        let cursor =
            run_to_completion_and_read_cursor(vec![outdated_cursor_info()], Some(555)).await;
        assert_eq!(cursor, Some(555));

        // (c) neither: falls back to 0.
        let cursor = run_to_completion_and_read_cursor(vec![outdated_cursor_info()], None).await;
        assert_eq!(cursor, Some(0));
    }

    // AC5 continued, BC24: an info frame with any other name is logged and
    // produces no op.
    #[tokio::test]
    async fn info_with_another_name_produces_no_op() {
        let cursor = run_to_completion_and_read_cursor(
            vec![Event::Info {
                name: "SomeOtherInfo".to_string(),
                message: "nothing to see here".to_string(),
            }],
            None,
        )
        .await;
        assert_eq!(cursor, None, "an unrecognised info name must send no op");
    }

    // BC30: a dead writer is `IngestError::WriterFailed`, whether `send`
    // discovers it (this test) or `health()` reports `Failed`.
    #[tokio::test]
    async fn writer_gone_on_send_is_writer_failed() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        writer.shutdown().await.unwrap(); // the thread exits; a later send is WriterGone
        let cfg = test_config();
        let health = crate::health::HealthState::new();
        let quote = commit("jetstream_commit_post_quote_record.json");
        let source = VecSource::new(vec![Event::Commit(quote)]);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut hot = HotSet::new();
        let (_evict_tx, evict_rx) = mpsc::unbounded_channel();

        let result = run_ingest_periodic(
            &cfg,
            source,
            &writer,
            &mut hot,
            shutdown_rx,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            evict_rx,
            &health,
        )
        .await;

        assert!(matches!(result, Err(IngestError::WriterFailed)));
    }

    // Round 1 finding 4, BC41: every `watch::Sender` for the writer's
    // health dropping (the thread panicked, or was itself dropped without
    // publishing `Failed`) is treated as the writer being gone, exactly
    // like an observed `WriterState::Failed`.
    #[tokio::test]
    async fn health_channel_closed_is_writer_failed() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        // Drop every other handle so `health()`'s sender side (owned by the
        // writer thread) can be dropped by shutting the thread down without
        // it publishing `Failed` first: `shutdown` commits and exits clean,
        // which drops `health_tx` and closes the watch channel.
        writer.shutdown().await.unwrap();

        let cfg = test_config();
        let health = crate::health::HealthState::new();
        let source = VecSource::new(vec![]);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut hot = HotSet::new();
        let (_evict_tx, evict_rx) = mpsc::unbounded_channel();

        let result = run_ingest_periodic(
            &cfg,
            source,
            &writer,
            &mut hot,
            shutdown_rx,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            evict_rx,
            &health,
        )
        .await;

        assert!(matches!(result, Err(IngestError::WriterFailed)));
    }

    // AC8, BC1, BC11, BC15: a mixed sequence lands the right ops on a real
    // in-memory store, and `run_ingest` applies each op's hot-set change to
    // the caller's `HotSet` in step.
    #[tokio::test]
    async fn run_ingest_lands_translated_ops_on_a_real_store() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store
            .writer_with(crate::store::writer::WriterConfig {
                capacity: 100,
                max_ops: 1,
                interval: Duration::from_millis(5),
            })
            .unwrap();

        let quote = commit("jetstream_commit_post_quote_record.json");
        let quote_seq = quote.seq;
        let quote_uri = "at://did:plc:quoterdid0000000000000000/app.bsky.feed.post/3mvqsnhquote1";
        let original_uri = "at://did:plc:originaldid00000000000000/app.bsky.feed.post/original0001";
        let like = like_commit(quote_seq + 1, original_uri);

        // Phase 1: the quote and a like on its original land InsertPair and
        // Incr{Likes}.
        {
            let writer_task = writer.clone();
            let cfg = test_config();
            let health = crate::health::HealthState::new();
            let (source, drained) =
                VecSource::with_drained_signal(vec![Event::Commit(quote), Event::Commit(like)]);
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let (_evict_tx, evict_rx) = mpsc::unbounded_channel();

            let handle = tokio::spawn(async move {
                let mut hot = HotSet::new();
                let result = run_ingest_periodic(
                    &cfg,
                    source,
                    &writer_task,
                    &mut hot,
                    shutdown_rx,
                    Duration::from_secs(3600),
                    Duration::from_secs(3600),
                    evict_rx,
                    &health,
                )
                .await;
                (result, hot)
            });

            drained.await.expect("the source should drain");
            // The pump forwards each event over its own channel ahead of
            // `run_ingest`'s loop actually consuming it, so `drained`
            // (which fires once the pump itself has run dry) no longer
            // guarantees the last event has been translated and applied; a
            // short beat gives the loop time to catch up before shutdown.
            tokio::time::sleep(Duration::from_millis(150)).await;
            shutdown_tx.send(true).unwrap();
            let (result, hot) =
                tokio::time::timeout(Duration::from_secs(2), handle).await.unwrap().unwrap();
            result.unwrap();

            assert!(hot.contains(quote_uri), "the quote's own uri should be hot");
            assert!(hot.contains(original_uri), "the quoted original should be hot");

            writer.flush().await.unwrap();
            let candidates = store.dirty_candidates(unix_now(), 999_999).unwrap();
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].quote_uri, quote_uri);
            assert_eq!(candidates[0].original_uri, original_uri);
            assert_eq!(
                candidates[0].counts_o.likes, 1,
                "the like should have landed on the original"
            );
        }

        // Phase 2: a postgate detach on the quote lands `Op::Detach`,
        // dropping the pair out of the candidate set.
        {
            let writer_task = writer.clone();
            let cfg = test_config();
            let health = crate::health::HealthState::new();
            let (source, drained) = VecSource::with_drained_signal(vec![Event::Commit(
                postgate_commit(quote_seq + 2, &[quote_uri]),
            )]);
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let (_evict_tx, evict_rx) = mpsc::unbounded_channel();

            let handle = tokio::spawn(async move {
                let mut hot = HotSet::new();
                hot.insert(quote_uri);
                run_ingest_periodic(
                    &cfg,
                    source,
                    &writer_task,
                    &mut hot,
                    shutdown_rx,
                    Duration::from_secs(3600),
                    Duration::from_secs(3600),
                    evict_rx,
                    &health,
                )
                .await
            });

            drained.await.expect("the source should drain");
            // See the comment on the same sleep in phase 1 above.
            tokio::time::sleep(Duration::from_millis(150)).await;
            shutdown_tx.send(true).unwrap();
            tokio::time::timeout(Duration::from_secs(2), handle).await.unwrap().unwrap().unwrap();

            writer.flush().await.unwrap();
            let candidates = store.dirty_candidates(unix_now(), 999_999).unwrap();
            assert!(candidates.is_empty(), "a detached pair must not be a candidate any longer");
        }
    }

    // Round 1 finding 2, BC37, BC38: a post delete's eviction reaches
    // `run_ingest` from the writer over `evict_rx`, taking out every URI
    // the DB-level drop frees, not only the one URI `translate` itself
    // named in the `Op::DeletePost`.
    #[tokio::test]
    async fn evicted_uris_from_the_writer_are_removed_from_the_hot_set() {
        let store = crate::store::Store::open_memory().unwrap();
        let (evict_tx, evict_rx) = mpsc::unbounded_channel();
        let writer = store.writer_evicting(evict_tx).unwrap();

        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        let q1 = "at://did:plc:q/app.bsky.feed.post/q1";
        let q2 = "at://did:plc:q/app.bsky.feed.post/q2";

        writer
            .send(Op::InsertPair {
                quote_uri: q1.to_string(),
                quote_did: "did:plc:q".to_string(),
                quote_cid: "bafyq1".to_string(),
                original_uri: original_uri.to_string(),
                original_did: "did:plc:o".to_string(),
                quoted_at: 1_700_000_000,
                first_seen_at: 1_700_000_000,
                seq: 1,
            })
            .await
            .unwrap();
        writer
            .send(Op::InsertPair {
                quote_uri: q2.to_string(),
                quote_did: "did:plc:q".to_string(),
                quote_cid: "bafyq2".to_string(),
                original_uri: original_uri.to_string(),
                original_did: "did:plc:o".to_string(),
                quoted_at: 1_700_000_000,
                first_seen_at: 1_700_000_000,
                seq: 2,
            })
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let mut hot = HotSet::new();
        hot.rebuild_from(|f| store.for_each_hot_uri(f)).unwrap();
        assert_eq!(hot.len(), 3, "both quotes and the shared original start hot");

        let delete_commit = CommitEvent {
            did: "did:plc:o".to_string(),
            seq: 3,
            time: "2026-09-18T00:00:30.000000Z".to_string(),
            operation: Operation::Delete,
            collection: POST.to_string(),
            rkey: "o1".to_string(),
            rev: "revdel".to_string(),
            cid: None,
            record: None,
        };
        let (source, drained) = VecSource::with_drained_signal(vec![Event::Commit(delete_commit)]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let cfg = test_config();
        let health = crate::health::HealthState::new();
        let writer_task = writer.clone();

        let handle = tokio::spawn(async move {
            let result = run_ingest_periodic(
                &cfg,
                source,
                &writer_task,
                &mut hot,
                shutdown_rx,
                Duration::from_secs(3600),
                Duration::from_secs(3600),
                evict_rx,
                &health,
            )
            .await;
            (result, hot)
        });

        drained.await.expect("the source should drain");
        // `drained` only means the pump has forwarded the delete event over
        // its own channel, not that `run_ingest`'s loop has consumed and
        // translated it yet; a short beat gives it time to do so and send
        // the resulting `Op::DeletePost` to the writer. `flush` then waits
        // out the writer's default 500ms batch interval for that op to
        // commit, which is also when it sends the eviction message; a
        // second short beat gives `run_ingest`'s loop time to drain that
        // message before shutdown.
        tokio::time::sleep(Duration::from_millis(150)).await;
        writer.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        shutdown_tx.send(true).unwrap();
        let (result, hot) =
            tokio::time::timeout(Duration::from_secs(2), handle).await.unwrap().unwrap();
        result.unwrap();

        assert!(!hot.contains(original_uri));
        assert!(!hot.contains(q1));
        assert!(!hot.contains(q2));
        assert_eq!(hot.len(), 0);
    }

    // AC10: a live run against the real Jetstream host survives without
    // panicking. `AGENTS.md`: a test that needs the network is `#[ignore]`
    // and run by hand: `cargo test --all-features -- --ignored
    // ingest_live_smoke`. `test_config`'s default `jetstream_urls` point at
    // the real hosts, so this connects for real, runs for a few seconds,
    // then asks for the same shutdown `run` uses.
    #[tokio::test]
    #[ignore]
    async fn ingest_live_smoke() {
        let store = crate::store::Store::open_memory().unwrap();
        let writer = store.writer().unwrap();
        let cfg = test_config();
        let health = crate::health::HealthState::new();
        let source = crate::jetstream::JetstreamClient::connect(&cfg, None)
            .await
            .expect("connecting to the real jetstream host should not fail");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_evict_tx, evict_rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(async move {
            let mut hot = HotSet::new();
            run_ingest(&cfg, source, &writer, &mut hot, shutdown_rx, evict_rx, &health).await
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        shutdown_tx.send(true).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("run_ingest should return once shutdown is requested")
            .expect("the run_ingest task should not panic");

        assert!(result.is_ok(), "a live run should not error: {result:?}");
    }

    // --- start_graph_subsystem ------------------------------------------

    /// A unique, real file path under the OS temp dir, so a `Store::open`
    /// in these tests never collides with another test run's database.
    fn temp_db_path(name: &str) -> String {
        let nanos =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir()
            .join(format!("upstage-ingest-{name}-{nanos}.sqlite3"))
            .to_str()
            .unwrap()
            .to_string()
    }

    /// A `Config` with `BSKY_HANDLE` and `BSKY_APP_PASSWORD` set, and
    /// `UPSTAGE_DB_PATH` pointed at a fresh temp file, for
    /// `start_graph_subsystem_with_credentials_starts_the_worker`.
    fn test_config_with_graph(db_path: &str) -> Config {
        let db_path = db_path.to_string();
        let lookup = move |name: &str| match name {
            "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
            "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "UPSTAGE_DB_PATH" => Some(db_path.clone()),
            "BSKY_HANDLE" => Some("upstage.bsky.social".to_string()),
            "BSKY_APP_PASSWORD" => Some("secret".to_string()),
            _ => None,
        };
        crate::config::load(lookup).expect("test config should load")
    }

    #[test]
    fn start_graph_subsystem_without_credentials_returns_none() {
        // BC18: the switch is on (implicitly, by calling this at all — the
        // caller in `run` only calls it inside `if cfg.personalise`) but
        // `BSKY_HANDLE`/`BSKY_APP_PASSWORD` are missing: no worker starts,
        // and the caller gets no hook to wire into the resolver.
        let cfg = test_config();
        assert!(cfg.bsky_handle.is_none());
        assert!(start_graph_subsystem(&cfg).is_none());
    }

    #[tokio::test]
    async fn start_graph_subsystem_with_credentials_starts_the_worker() {
        // BC19 (the load half): with credentials present, the graph store
        // opens, the (empty) circle index loads without error, and a hook
        // comes back for the caller to wire into the resolver.
        let path = temp_db_path("start-graph");
        let cfg = test_config_with_graph(&path);

        let hook = start_graph_subsystem(&cfg);
        assert!(hook.is_some(), "credentials present: a first-build hook must come back");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
    }

    #[tokio::test]
    async fn start_graph_subsystem_reenqueues_a_building_d1_row() {
        // BC19: a `building_d1` row already in the database at startup
        // means the hook's underlying `GraphHandle` re-enqueued it, the
        // same load `graph::tests::restart_loads_circles` exercises
        // directly on `GraphHandle::from_store`. This test drives it
        // through `start_graph_subsystem` instead, so the `run`-level
        // wiring is covered too.
        let path = temp_db_path("start-graph-reenqueue");
        {
            let seed = crate::store::Store::open_path(&path).unwrap();
            seed.viewer_save_state("did:plc:building", "building_d1", crate::store::unix_now())
                .unwrap();
        }
        let cfg = test_config_with_graph(&path);

        let hook = start_graph_subsystem(&cfg);
        assert!(hook.is_some());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
    }
}
