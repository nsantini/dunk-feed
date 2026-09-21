//! `pairs` table row operations, TECH-DESIGN section 6 and 5.2. Every
//! write function here takes an already-open `Connection` (or a
//! `Transaction`, which derefs to one) so `writer::commit_batch` can run all
//! of them inside one transaction. `for_each_hot_uri`, `dirty_candidates`,
//! `promoted_within`, `demote`, `drop_pair` and `expire` are the
//! synchronous reads the scorer pass (TECH-DESIGN section 7.2) needs.
//!
//! Round 1 finding 7: every dropped-state transition goes through
//! `drop_pair`, and every SQL statement here that writes a `state` column
//! uses `PairState::as_str()`, never a literal.

use rusqlite::{Connection, OptionalExtension};

use crate::score::Counts;
use crate::store::{counts, feed, DropReason, PairState, StoreError};

/// One `pairs` row with both sides' counts, saturated at `u32::MAX`
/// (BC49). Returned by `dirty_candidates` and `promoted_within`.
#[derive(Debug, Clone, PartialEq)]
pub struct PairWithCounts {
    pub quote_uri: String,
    pub quote_did: String,
    pub quote_cid: String,
    pub original_uri: String,
    pub original_did: String,
    pub quoted_at: i64,
    pub first_seen_at: i64,
    pub counts_q: Counts,
    pub counts_o: Counts,
}

/// `expire`'s result. The caller (the scorer pass) evicts `evicted_uris`
/// from the in-memory hot set (BC61).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExpireReport {
    pub evicted_uris: Vec<String>,
    pub candidates_expired: usize,
    pub feed_expired: usize,
}

/// One verdict from a scorer verify phase, ready to apply in one transaction
/// (round 2 finding 5, BC48). `Promote` carries the full `feed::FeedRow` to
/// upsert; `Drop` and `Demote` carry just what `drop_pair`/`demote` need.
#[derive(Debug, Clone, PartialEq)]
pub enum PairOutcome {
    Promote(feed::FeedRow),
    Drop { quote_uri: String, reason: DropReason },
    Demote { quote_uri: String },
}

/// Applies every `PairOutcome` in `outcomes` inside one transaction (BC48):
/// a failure partway through applies none of them. Round 2 finding 10: the
/// scorer calls this once per verify phase (first verify, re-verify) instead
/// of one blocking call per pair.
pub fn apply_verdicts(conn: &Connection, outcomes: &[PairOutcome]) -> Result<(), StoreError> {
    if outcomes.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    for outcome in outcomes {
        match outcome {
            PairOutcome::Promote(row) => feed::promote(&tx, row)?,
            PairOutcome::Drop { quote_uri, reason } => drop_pair(&tx, quote_uri, *reason)?,
            PairOutcome::Demote { quote_uri } => demote(&tx, quote_uri)?,
        }
    }
    tx.commit()?;
    Ok(())
}

/// `INSERT ... ON CONFLICT(quote_uri) DO NOTHING`, so a repeat `InsertPair`
/// for a `quote_uri` already on file is a no-op, not an error (BC10).
/// `first_seen_at` is whatever the caller passes; this function never reads
/// the clock itself.
#[allow(clippy::too_many_arguments)]
pub fn insert_pair(
    conn: &Connection,
    quote_uri: &str,
    quote_did: &str,
    quote_cid: &str,
    original_uri: &str,
    original_did: &str,
    quoted_at: i64,
    first_seen_at: i64,
) -> Result<(), StoreError> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO pairs (quote_uri, quote_did, quote_cid, original_uri, original_did, quoted_at, first_seen_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(quote_uri) DO NOTHING",
    )?;
    stmt.execute(rusqlite::params![
        quote_uri,
        quote_did,
        quote_cid,
        original_uri,
        original_did,
        quoted_at,
        first_seen_at
    ])?;
    Ok(())
}

/// True when no pair whose `state` is not `dropped` still names `uri` as
/// `quote_uri` or `original_uri` (round 1 finding 1). `expire` and
/// `delete_post` check this before a `counts` row is deleted, so a URI a
/// live pair still needs is never evicted (BC58, BC59, BC61).
fn is_orphaned(conn: &Connection, uri: &str) -> Result<bool, StoreError> {
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM pairs WHERE (quote_uri = ?1 OR original_uri = ?1) AND state != ?2",
        rusqlite::params![uri, PairState::Dropped.as_str()],
        |row| row.get(0),
    )?;
    Ok(count == 0)
}

/// A `post` delete for `uri`. If `uri` is a pair's `quote_uri`, that pair is
/// dropped through `drop_pair` with `DropReason::QuoteGone` (BC30): its
/// `feed` row is deleted and the `pairs` row stays `dropped` for `expire` to
/// remove. Every pair holding `uri` as `original_uri` is dropped the same
/// way with `DropReason::OriginalGone` (BC31). A `uri` in no pair changes
/// nothing pair-side and is not an error (BC32). `uri`'s own `counts` row is
/// deleted only when no non-dropped pair still names it on either side
/// (round 1 finding 1), which after the drops above holds unless `uri` is
/// still someone else's live original or quote.
///
/// Round 1 finding 2 (BC38): returns exactly the URIs that `for_each_hot_uri`
/// would have listed before this call and will not list after it, so
/// `run_ingest` can evict them from its in-memory `HotSet` without a second
/// scan. `uri` itself, `uri`'s own original (if `uri` is also a quote), and
/// every quote this call drops via `DropReason::OriginalGone` are the only
/// URIs whose hot-ness can change; each is checked with `is_orphaned` before
/// and after the drops, since only a URI that was live and is now orphaned
/// left the hot set.
pub fn delete_post(conn: &Connection, uri: &str) -> Result<Vec<String>, StoreError> {
    let own_original: Option<String> = conn
        .query_row("SELECT original_uri FROM pairs WHERE quote_uri = ?1", [uri], |row| row.get(0))
        .optional()?;

    // BC31: this `uri` as an original_uri.
    let quote_uris: Vec<String> = {
        let mut stmt = conn.prepare("SELECT quote_uri FROM pairs WHERE original_uri = ?1")?;
        let rows = stmt.query_map([uri], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    let mut candidates = vec![uri.to_string()];
    candidates.extend(own_original.iter().cloned());
    candidates.extend(quote_uris.iter().cloned());
    candidates.sort();
    candidates.dedup();
    let mut was_live = Vec::with_capacity(candidates.len());
    for candidate in &candidates {
        was_live.push(!is_orphaned(conn, candidate)?);
    }

    // BC30: this `uri` as a quote_uri.
    drop_pair(conn, uri, DropReason::QuoteGone)?;
    for quote_uri in &quote_uris {
        drop_pair(conn, quote_uri, DropReason::OriginalGone)?;
    }

    let mut evicted = Vec::new();
    for (candidate, was_live) in candidates.iter().zip(was_live) {
        if was_live && is_orphaned(conn, candidate)? {
            evicted.push(candidate.clone());
        }
    }

    // BC32, round 1 finding 1.
    if is_orphaned(conn, uri)? {
        counts::delete_counts(conn, uri)?;
    }
    Ok(evicted)
}

/// A `postgate` detach for `quote_uri`: `drop_pair` with `DropReason::Detached`
/// (BC33). Round 1 finding 2 (BC38): returns the same way `delete_post`
/// does, checking `quote_uri` and its `original_uri` for a live-to-orphaned
/// transition.
pub fn detach(conn: &Connection, quote_uri: &str) -> Result<Vec<String>, StoreError> {
    let original_uri: Option<String> = conn
        .query_row("SELECT original_uri FROM pairs WHERE quote_uri = ?1", [quote_uri], |row| {
            row.get(0)
        })
        .optional()?;

    let mut candidates = vec![quote_uri.to_string()];
    candidates.extend(original_uri.iter().cloned());
    candidates.sort();
    candidates.dedup();
    let mut was_live = Vec::with_capacity(candidates.len());
    for candidate in &candidates {
        was_live.push(!is_orphaned(conn, candidate)?);
    }

    drop_pair(conn, quote_uri, DropReason::Detached)?;

    let mut evicted = Vec::new();
    for (candidate, was_live) in candidates.iter().zip(was_live) {
        if was_live && is_orphaned(conn, candidate)? {
            evicted.push(candidate.clone());
        }
    }
    Ok(evicted)
}

/// Streams `quote_uri` and then `original_uri` of every pair whose `state`
/// is not `dropped` into `f` (BC45). Duplicates are not removed; the caller
/// (the scorer's hot set) holds a set. Round 1 finding 6: this streams
/// straight from the statement instead of collecting every URI into a `Vec`
/// before the caller sees the first one.
pub fn for_each_hot_uri(conn: &Connection, mut f: impl FnMut(&str)) -> Result<(), StoreError> {
    let mut stmt = conn.prepare("SELECT quote_uri, original_uri FROM pairs WHERE state != ?1")?;
    let mut rows = stmt.query(rusqlite::params![PairState::Dropped.as_str()])?;
    while let Some(row) = rows.next()? {
        let quote_uri: String = row.get(0)?;
        let original_uri: String = row.get(1)?;
        f(&quote_uri);
        f(&original_uri);
    }
    Ok(())
}

/// `candidate` pairs whose `first_seen_at` is within `ttl_h` hours of `now`
/// and where either side's `counts` row is dirty (BC46, BC48). A pair with
/// no `counts` row on either side is not returned (BC47): no row means no
/// event since the pair was inserted.
pub fn dirty_candidates(
    conn: &Connection,
    now: i64,
    ttl_h: i64,
) -> Result<Vec<PairWithCounts>, StoreError> {
    let cutoff = now - ttl_h * 3600;
    // Round 1 finding 5 (BC78): starts from `counts WHERE dirty = 1`, which
    // the `counts_dirty` partial index serves, then joins `pairs` on either
    // side, then applies the state and TTL filters. `DISTINCT` collapses
    // the join's two rows for a pair whose counts are dirty on both sides.
    let mut stmt = conn.prepare(
        "SELECT DISTINCT p.quote_uri, p.quote_did, p.quote_cid, p.original_uri, p.original_did, p.quoted_at, p.first_seen_at
         FROM counts c
         JOIN pairs p ON (c.post_uri = p.quote_uri OR c.post_uri = p.original_uri)
         WHERE c.dirty = 1
           AND p.state = ?1
           AND p.first_seen_at >= ?2",
    )?;
    let rows =
        stmt.query_map(rusqlite::params![PairState::Candidate.as_str(), cutoff], pair_columns)?;
    rows_with_counts(conn, rows)
}

/// `promoted` pairs whose `quoted_at` is at or after `now - h * 3600`
/// (BC51), with both sides' counts.
pub fn promoted_within(
    conn: &Connection,
    now: i64,
    h: i64,
) -> Result<Vec<PairWithCounts>, StoreError> {
    let cutoff = now - h * 3600;
    let mut stmt = conn.prepare(
        "SELECT quote_uri, quote_did, quote_cid, original_uri, original_did, quoted_at, first_seen_at
         FROM pairs
         WHERE state = ?1 AND quoted_at >= ?2",
    )?;
    let rows =
        stmt.query_map(rusqlite::params![PairState::Promoted.as_str(), cutoff], pair_columns)?;
    rows_with_counts(conn, rows)
}

/// Deletes the `feed` row and returns the pair to `candidate` with
/// `drop_reason = NULL` (BC55).
pub fn demote(conn: &Connection, quote_uri: &str) -> Result<(), StoreError> {
    feed::delete_feed_row(conn, quote_uri)?;
    let mut stmt = conn
        .prepare_cached("UPDATE pairs SET state = ?2, drop_reason = NULL WHERE quote_uri = ?1")?;
    stmt.execute(rusqlite::params![quote_uri, PairState::Candidate.as_str()])?;
    Ok(())
}

/// Marks the pair `dropped` with `reason` and deletes its `feed` row
/// (BC56). The pair row stays. Round 1 finding 7: every dropped-state
/// transition (`detach`, both sides of `delete_post`) goes through this one
/// function.
pub fn drop_pair(conn: &Connection, quote_uri: &str, reason: DropReason) -> Result<(), StoreError> {
    feed::delete_feed_row(conn, quote_uri)?;
    let mut stmt =
        conn.prepare_cached("UPDATE pairs SET state = ?2, drop_reason = ?3 WHERE quote_uri = ?1")?;
    stmt.execute(rusqlite::params![quote_uri, PairState::Dropped.as_str(), reason.as_str()])?;
    Ok(())
}

/// TECH-DESIGN section 7.2 step 6. Deletes `candidate` or `dropped` pairs
/// whose `first_seen_at` is older than `candidate_ttl_h` hours (BC58).
/// Deletes `feed` rows whose `promoted_at` is older than `feed_ttl_d` days,
/// and their pairs with them (BC59). A `promoted` pair inside the feed TTL
/// but past the candidate TTL is kept: it lives by the feed TTL, never the
/// candidate TTL (BC60). Round 1 finding 1: a side's `counts` row is
/// deleted, and that URI reported in `ExpireReport::evicted_uris`, only when
/// no non-dropped pair still names it on either side (BC61) — so a URI a
/// live pair still needs (a shared original whose other quote survives) is
/// never evicted. Round 1 finding 5 (BC79): every delete here is one
/// statement over the whole matching set, not one statement per row. Round 2
/// finding 3 (BC46): the whole body runs inside one
/// `conn.unchecked_transaction()`, committed at the end, so a failure
/// part-way through never leaves a pair promoted without its `feed` row, or
/// a `counts` row deleted while its `pairs` row survives.
pub fn expire(
    conn: &Connection,
    now: i64,
    candidate_ttl_h: i64,
    feed_ttl_d: i64,
) -> Result<ExpireReport, StoreError> {
    let tx = conn.unchecked_transaction()?;
    let conn = &tx;
    let mut report = ExpireReport::default();
    let mut touched_uris: std::collections::HashSet<String> = std::collections::HashSet::new();

    // BC58: candidate/dropped pairs older than the candidate TTL. `feed`
    // never holds a row for these states, so no feed delete is needed.
    let candidate_cutoff = now - candidate_ttl_h * 3600;
    let expired_candidates: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT quote_uri, original_uri FROM pairs WHERE state IN (?1, ?2) AND first_seen_at < ?3",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![
                PairState::Candidate.as_str(),
                PairState::Dropped.as_str(),
                candidate_cutoff
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    report.candidates_expired = expired_candidates.len();
    for (quote_uri, original_uri) in &expired_candidates {
        touched_uris.insert(quote_uri.clone());
        touched_uris.insert(original_uri.clone());
    }
    conn.execute(
        "DELETE FROM pairs WHERE state IN (?1, ?2) AND first_seen_at < ?3",
        rusqlite::params![
            PairState::Candidate.as_str(),
            PairState::Dropped.as_str(),
            candidate_cutoff
        ],
    )?;

    // BC59: feed rows older than the feed TTL, and their pairs. `feed` must
    // be deleted before `pairs`, because `feed.quote_uri` references
    // `pairs.quote_uri` and `foreign_keys` is on.
    let feed_cutoff = now - feed_ttl_d * 86400;
    let expired_feed: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT feed.quote_uri, pairs.original_uri
             FROM feed JOIN pairs ON pairs.quote_uri = feed.quote_uri
             WHERE feed.promoted_at < ?1",
        )?;
        let rows = stmt.query_map([feed_cutoff], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    report.feed_expired = expired_feed.len();
    for (quote_uri, original_uri) in &expired_feed {
        touched_uris.insert(quote_uri.clone());
        touched_uris.insert(original_uri.clone());
    }
    conn.execute("DELETE FROM feed WHERE promoted_at < ?1", [feed_cutoff])?;
    if !expired_feed.is_empty() {
        let quote_uris: Vec<&String> =
            expired_feed.iter().map(|(quote_uri, _)| quote_uri).collect();
        // BC39, BC50: chunked at MAX_BOUND_PARAMS, through the shared
        // `for_each_in_chunk` helper, so a large expiry never exceeds
        // SQLite's bound-parameter limit.
        crate::store::for_each_in_chunk(&quote_uris, |chunk, placeholders| {
            let sql = format!("DELETE FROM pairs WHERE quote_uri IN ({placeholders})");
            conn.execute(&sql, rusqlite::params_from_iter(chunk.iter()))?;
            Ok(())
        })?;
    }

    // Round 1 finding 1: only a URI no non-dropped pair still names is
    // evicted.
    let mut orphaned: Vec<String> = touched_uris
        .into_iter()
        .map(|uri| is_orphaned(conn, &uri).map(|orphaned| (uri, orphaned)))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter_map(|(uri, orphaned)| orphaned.then_some(uri))
        .collect();
    if !orphaned.is_empty() {
        orphaned.sort();
        // BC39, BC50: chunked at MAX_BOUND_PARAMS, the same helper the
        // `pairs` delete above uses.
        crate::store::for_each_in_chunk(&orphaned, |chunk, placeholders| {
            let sql = format!("DELETE FROM counts WHERE post_uri IN ({placeholders})");
            conn.execute(&sql, rusqlite::params_from_iter(chunk.iter()))?;
            Ok(())
        })?;
    }
    report.evicted_uris = orphaned;

    tx.commit()?;
    Ok(report)
}

/// One row of `dunk dump`'s CSV, TECH-DESIGN section 6 and `spec.md`'s
/// column order. Carries the seven `pairs` columns, `state` and
/// `drop_reason` as the raw text `pairs` stores them, both sides' local
/// counts, and the six `v_*` verified counts plus `ratio` and
/// `promoted_at` from `feed` as `None` when the pair has no `feed` row
/// (BC13) or `Some` when it does (BC14). `dump.rs` renders `E` and `D`
/// from these; this struct holds no derived value.
#[derive(Debug, Clone, PartialEq)]
pub struct DumpRow {
    pub quote_uri: String,
    pub quote_did: String,
    pub quote_cid: String,
    pub original_uri: String,
    pub original_did: String,
    pub quoted_at: i64,
    pub first_seen_at: i64,
    pub state: String,
    pub drop_reason: Option<String>,
    pub counts_q: Counts,
    pub counts_o: Counts,
    pub v_likes_q: Option<i64>,
    pub v_reposts_q: Option<i64>,
    pub v_replies_q: Option<i64>,
    pub v_likes_o: Option<i64>,
    pub v_reposts_o: Option<i64>,
    pub v_replies_o: Option<i64>,
    pub ratio: Option<f64>,
    pub promoted_at: Option<i64>,
}

/// Same rule as `counts::saturate` (BC5, BC49): a stored count never
/// exceeds `u32::MAX`, and a `NULL` from the `LEFT JOIN counts` below reads
/// as zero, matching a side with no `counts` row. Kept local rather than
/// calling `counts::saturate` because that function is private to
/// `counts.rs` and `pairs_since` already has the joined value in hand, with
/// no second query to route through `counts::counts_for`.
fn saturate_joined_count(value: Option<i64>) -> u32 {
    match value {
        Some(value) => u32::try_from(value).unwrap_or(u32::MAX),
        None => 0,
    }
}

/// Every `pairs` row, in any state, first seen at or after `cutoff`
/// (BC1, BC6, BC16, BC21): `dunk dump`'s read. `pairs LEFT JOIN counts` on
/// `quote_uri` and again on `original_uri` (aliased `cq` and `co`) attaches
/// each side's local counts, defaulting to zero when that side has no
/// `counts` row (BC5). `LEFT JOIN feed` on `quote_uri` attaches the
/// verified counts, `ratio` and `promoted_at`, `None` across the board for
/// a pair with no `feed` row (BC13, BC14). Ordered by `first_seen_at`
/// ascending (BC16), the same order for the same window every run, through
/// `prepare_cached` on the caller's connection (BC21: `Store::pairs_since`
/// passes the read-only one).
pub fn pairs_since(conn: &Connection, cutoff: i64) -> Result<Vec<DumpRow>, StoreError> {
    let mut stmt = conn.prepare_cached(
        "SELECT p.quote_uri, p.quote_did, p.quote_cid, p.original_uri, p.original_did,
                p.quoted_at, p.first_seen_at, p.state, p.drop_reason,
                cq.likes, cq.reposts, cq.replies,
                co.likes, co.reposts, co.replies,
                f.v_likes_q, f.v_reposts_q, f.v_replies_q,
                f.v_likes_o, f.v_reposts_o, f.v_replies_o,
                f.ratio, f.promoted_at
         FROM pairs p
         LEFT JOIN counts cq ON cq.post_uri = p.quote_uri
         LEFT JOIN counts co ON co.post_uri = p.original_uri
         LEFT JOIN feed f ON f.quote_uri = p.quote_uri
         WHERE p.first_seen_at >= ?1
         ORDER BY p.first_seen_at",
    )?;
    let rows = stmt.query_map(rusqlite::params![cutoff], |row| {
        let likes_q: Option<i64> = row.get(9)?;
        let reposts_q: Option<i64> = row.get(10)?;
        let replies_q: Option<i64> = row.get(11)?;
        let likes_o: Option<i64> = row.get(12)?;
        let reposts_o: Option<i64> = row.get(13)?;
        let replies_o: Option<i64> = row.get(14)?;
        Ok(DumpRow {
            quote_uri: row.get(0)?,
            quote_did: row.get(1)?,
            quote_cid: row.get(2)?,
            original_uri: row.get(3)?,
            original_did: row.get(4)?,
            quoted_at: row.get(5)?,
            first_seen_at: row.get(6)?,
            state: row.get(7)?,
            drop_reason: row.get(8)?,
            counts_q: Counts {
                likes: saturate_joined_count(likes_q),
                reposts: saturate_joined_count(reposts_q),
                replies: saturate_joined_count(replies_q),
            },
            counts_o: Counts {
                likes: saturate_joined_count(likes_o),
                reposts: saturate_joined_count(reposts_o),
                replies: saturate_joined_count(replies_o),
            },
            v_likes_q: row.get(15)?,
            v_reposts_q: row.get(16)?,
            v_replies_q: row.get(17)?,
            v_likes_o: row.get(18)?,
            v_reposts_o: row.get(19)?,
            v_replies_o: row.get(20)?,
            ratio: row.get(21)?,
            promoted_at: row.get(22)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
}

/// The seven `pairs` columns `dirty_candidates` and `promoted_within` read
/// before attaching each side's counts.
struct PairColumns {
    quote_uri: String,
    quote_did: String,
    quote_cid: String,
    original_uri: String,
    original_did: String,
    quoted_at: i64,
    first_seen_at: i64,
}

fn pair_columns(row: &rusqlite::Row<'_>) -> rusqlite::Result<PairColumns> {
    Ok(PairColumns {
        quote_uri: row.get(0)?,
        quote_did: row.get(1)?,
        quote_cid: row.get(2)?,
        original_uri: row.get(3)?,
        original_did: row.get(4)?,
        quoted_at: row.get(5)?,
        first_seen_at: row.get(6)?,
    })
}

/// Attaches both sides' counts (BC49's saturation) to each `PairColumns`
/// row produced by a `query_map` over `pair_columns`.
fn rows_with_counts(
    conn: &Connection,
    rows: impl Iterator<Item = rusqlite::Result<PairColumns>>,
) -> Result<Vec<PairWithCounts>, StoreError> {
    let mut out = Vec::new();
    for row in rows {
        let row = row?;
        let counts_q = counts::counts_for(conn, &row.quote_uri)?;
        let counts_o = counts::counts_for(conn, &row.original_uri)?;
        out.push(PairWithCounts {
            quote_uri: row.quote_uri,
            quote_did: row.quote_did,
            quote_cid: row.quote_cid,
            original_uri: row.original_uri,
            original_did: row.original_did,
            quoted_at: row.quoted_at,
            first_seen_at: row.first_seen_at,
            counts_q,
            counts_o,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::migrated_conn;

    fn insert_test_pair(conn: &Connection, quote_uri: &str, original_uri: &str) {
        insert_pair(
            conn,
            quote_uri,
            "did:plc:q",
            "bafyq",
            original_uri,
            "did:plc:o",
            1_700_000_000,
            1_700_000_000,
        )
        .unwrap();
    }

    fn insert_feed_row(conn: &Connection, quote_uri: &str) {
        conn.execute(
            "INSERT INTO feed (
                quote_uri, quote_cid, quote_did, original_did, quoted_at,
                v_likes_q, v_reposts_q, v_replies_q, v_likes_o, v_reposts_o, v_replies_o,
                ratio, rank, promoted_at, verified_at
             )
             VALUES (?1, 'bafyq', 'did:plc:q', 'did:plc:o', 1700000000, 0, 0, 0, 0, 0, 0, 1.0, 1.0, 1700000000, 1700000000)",
            [quote_uri],
        )
        .unwrap();
    }

    fn pair_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT count(*) FROM pairs", [], |row| row.get(0)).unwrap()
    }

    fn feed_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT count(*) FROM feed", [], |row| row.get(0)).unwrap()
    }

    #[test]
    fn insert_pair_idempotent() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        assert_eq!(pair_count(&conn), 1);
    }

    #[test]
    fn delete_post_when_uri_is_quote_uri() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        insert_feed_row(&conn, quote_uri);
        counts::incr(&conn, quote_uri, crate::store::writer::CountField::Likes, 1_700_000_100)
            .unwrap();

        delete_post(&conn, quote_uri).unwrap();

        // Round 1 finding 7: the pair row stays, dropped, for `expire` to
        // remove, instead of a hard delete that hid `quote_gone` from the
        // per-reason accounting.
        assert_eq!(pair_count(&conn), 1, "the pair row stays for expire to remove");
        assert_eq!(feed_count(&conn), 0);
        let (state, drop_reason): (String, Option<String>) = conn
            .query_row(
                "SELECT state, drop_reason FROM pairs WHERE quote_uri = ?1",
                [quote_uri],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "dropped");
        assert_eq!(drop_reason, Some("quote_gone".to_string()));
        assert_eq!(counts::counts_for(&conn, quote_uri).unwrap(), crate::score::Counts::default());
    }

    #[test]
    fn delete_post_keeps_a_shared_original() {
        let conn = migrated_conn();
        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        let q1 = "at://did:plc:q/app.bsky.feed.post/q1";
        let q2 = "at://did:plc:q/app.bsky.feed.post/q2";
        insert_test_pair(&conn, q1, original_uri);
        insert_test_pair(&conn, q2, original_uri);
        counts::incr(&conn, original_uri, crate::store::writer::CountField::Likes, 1_700_000_100)
            .unwrap();

        // q1 is deleted; q2 still holds a live, non-dropped reference to
        // the shared original, so the original's counts row must survive
        // (round 1 finding 1).
        delete_post(&conn, q1).unwrap();

        assert_eq!(
            counts::counts_for(&conn, original_uri).unwrap(),
            crate::score::Counts { likes: 1, reposts: 0, replies: 0 }
        );
        let q2_state: String = conn
            .query_row("SELECT state FROM pairs WHERE quote_uri = ?1", [q2], |row| row.get(0))
            .unwrap();
        assert_eq!(q2_state, "candidate", "q2's pair is untouched");
    }

    #[test]
    fn delete_post_when_uri_is_original_uri() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        insert_test_pair(&conn, quote_uri, original_uri);
        insert_feed_row(&conn, quote_uri);
        counts::incr(&conn, original_uri, crate::store::writer::CountField::Likes, 1_700_000_100)
            .unwrap();

        delete_post(&conn, original_uri).unwrap();

        assert_eq!(pair_count(&conn), 1, "the pair row stays");
        assert_eq!(feed_count(&conn), 0, "its feed row is deleted");
        let (state, drop_reason): (String, Option<String>) = conn
            .query_row(
                "SELECT state, drop_reason FROM pairs WHERE quote_uri = ?1",
                [quote_uri],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "dropped");
        assert_eq!(drop_reason, Some("original_gone".to_string()));
        assert_eq!(
            counts::counts_for(&conn, original_uri).unwrap(),
            crate::score::Counts::default()
        );
    }

    // Round 1 finding 2, BC38: an original with two quotes and no other
    // references evicts three URIs — both quotes and the original.
    #[test]
    fn delete_post_of_an_original_with_two_quotes_evicts_all_three() {
        let conn = migrated_conn();
        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        let q1 = "at://did:plc:q/app.bsky.feed.post/q1";
        let q2 = "at://did:plc:q/app.bsky.feed.post/q2";
        insert_test_pair(&conn, q1, original_uri);
        insert_test_pair(&conn, q2, original_uri);

        let mut evicted = delete_post(&conn, original_uri).unwrap();
        evicted.sort();
        assert_eq!(evicted, vec![original_uri, q1, q2]);
    }

    #[test]
    fn delete_post_keeps_a_shared_original_out_of_the_eviction_list() {
        let conn = migrated_conn();
        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        let q1 = "at://did:plc:q/app.bsky.feed.post/q1";
        let q2 = "at://did:plc:q/app.bsky.feed.post/q2";
        insert_test_pair(&conn, q1, original_uri);
        insert_test_pair(&conn, q2, original_uri);

        // q1 alone is deleted; q2 still needs the original, so only q1
        // leaves the hot set.
        let evicted = delete_post(&conn, q1).unwrap();
        assert_eq!(evicted, vec![q1]);
    }

    #[test]
    fn delete_post_when_uri_in_no_pair() {
        let conn = migrated_conn();
        let uri = "at://did:plc:x/app.bsky.feed.post/x1";
        counts::incr(&conn, uri, crate::store::writer::CountField::Likes, 1_700_000_100).unwrap();

        delete_post(&conn, uri).unwrap();

        assert_eq!(pair_count(&conn), 0);
        assert_eq!(counts::counts_for(&conn, uri).unwrap(), crate::score::Counts::default());
    }

    #[test]
    fn detach_marks_dropped_and_deletes_feed_row() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        insert_feed_row(&conn, quote_uri);

        detach(&conn, quote_uri).unwrap();

        assert_eq!(feed_count(&conn), 0);
        let (state, drop_reason): (String, Option<String>) = conn
            .query_row(
                "SELECT state, drop_reason FROM pairs WHERE quote_uri = ?1",
                [quote_uri],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "dropped");
        assert_eq!(drop_reason, Some("detached".to_string()));
        assert_eq!(pair_count(&conn), 1, "the pair row stays");
    }

    // Round 1 finding 2, BC38: detach evicts both sides of the dropped pair
    // when neither is needed elsewhere.
    #[test]
    fn detach_evicts_both_sides_with_no_other_reference() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        insert_test_pair(&conn, quote_uri, original_uri);

        let mut evicted = detach(&conn, quote_uri).unwrap();
        evicted.sort();
        assert_eq!(evicted, vec![original_uri, quote_uri]);
    }

    #[test]
    fn detach_keeps_an_original_still_live_elsewhere() {
        let conn = migrated_conn();
        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        let q1 = "at://did:plc:q/app.bsky.feed.post/q1";
        let q2 = "at://did:plc:q/app.bsky.feed.post/q2";
        insert_test_pair(&conn, q1, original_uri);
        insert_test_pair(&conn, q2, original_uri);

        let evicted = detach(&conn, q1).unwrap();
        assert_eq!(evicted, vec![q1]);
    }

    #[test]
    fn for_each_hot_uri() {
        let conn = migrated_conn();
        insert_test_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/q1",
            "at://did:plc:o/app.bsky.feed.post/o1",
        );
        insert_test_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/q2",
            "at://did:plc:o/app.bsky.feed.post/o2",
        );
        insert_test_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/q3",
            "at://did:plc:o/app.bsky.feed.post/o3",
        );
        detach(&conn, "at://did:plc:q/app.bsky.feed.post/q2").unwrap();

        let mut uris: Vec<String> = Vec::new();
        super::for_each_hot_uri(&conn, |uri| uris.push(uri.to_string())).unwrap();
        uris.sort();
        assert_eq!(
            uris,
            vec![
                "at://did:plc:o/app.bsky.feed.post/o1".to_string(),
                "at://did:plc:o/app.bsky.feed.post/o3".to_string(),
                "at://did:plc:q/app.bsky.feed.post/q1".to_string(),
                "at://did:plc:q/app.bsky.feed.post/q3".to_string(),
            ]
        );
    }

    #[test]
    fn dirty_candidates_returns_a_candidate_with_a_dirty_side() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        insert_test_pair(&conn, quote_uri, original_uri);
        counts::incr(&conn, original_uri, crate::store::writer::CountField::Likes, 1_700_000_100)
            .unwrap();

        let found = dirty_candidates(&conn, 1_700_000_200, 48).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].quote_uri, quote_uri);
        assert_eq!(found[0].counts_o.likes, 1);
    }

    #[test]
    fn dirty_candidates_skips_a_pair_with_no_counts_row_on_either_side() {
        let conn = migrated_conn();
        insert_test_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/q1",
            "at://did:plc:o/app.bsky.feed.post/o1",
        );
        let found = dirty_candidates(&conn, 1_700_000_200, 48).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn dirty_candidates_skips_a_pair_outside_the_ttl() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        insert_test_pair(&conn, quote_uri, original_uri); // first_seen_at = 1_700_000_000
        counts::incr(&conn, original_uri, crate::store::writer::CountField::Likes, 1_700_000_100)
            .unwrap();

        // now is 49h past first_seen_at, ttl is 48h.
        let found = dirty_candidates(&conn, 1_700_000_000 + 49 * 3600, 48).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn dirty_candidates_skips_a_promoted_or_dropped_pair() {
        let conn = migrated_conn();
        let promoted = "at://did:plc:q/app.bsky.feed.post/q1";
        let dropped = "at://did:plc:q/app.bsky.feed.post/q2";
        insert_test_pair(&conn, promoted, "at://did:plc:o/app.bsky.feed.post/o1");
        insert_test_pair(&conn, dropped, "at://did:plc:o/app.bsky.feed.post/o2");
        conn.execute("UPDATE pairs SET state = 'promoted' WHERE quote_uri = ?1", [promoted])
            .unwrap();
        detach(&conn, dropped).unwrap();
        for uri in [promoted, dropped] {
            counts::incr(&conn, uri, crate::store::writer::CountField::Likes, 1_700_000_100)
                .unwrap();
        }

        assert!(dirty_candidates(&conn, 1_700_000_200, 48).unwrap().is_empty());
    }

    #[test]
    fn promoted_within_returns_promoted_pairs_inside_the_window() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        conn.execute("UPDATE pairs SET state = 'promoted' WHERE quote_uri = ?1", [quote_uri])
            .unwrap();

        let found = promoted_within(&conn, 1_700_000_000 + 3600, 48).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].quote_uri, quote_uri);
    }

    #[test]
    fn promoted_within_excludes_a_candidate_pair() {
        let conn = migrated_conn();
        insert_test_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/q1",
            "at://did:plc:o/app.bsky.feed.post/o1",
        );
        assert!(promoted_within(&conn, 1_700_000_000 + 3600, 48).unwrap().is_empty());
    }

    #[test]
    fn demote_deletes_feed_row_and_returns_to_candidate() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        insert_feed_row(&conn, quote_uri);
        conn.execute(
            "UPDATE pairs SET state = 'promoted', drop_reason = 'demoted' WHERE quote_uri = ?1",
            [quote_uri],
        )
        .unwrap();

        demote(&conn, quote_uri).unwrap();

        assert_eq!(feed_count(&conn), 0);
        let (state, drop_reason): (String, Option<String>) = conn
            .query_row(
                "SELECT state, drop_reason FROM pairs WHERE quote_uri = ?1",
                [quote_uri],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "candidate");
        assert_eq!(drop_reason, None);
    }

    #[test]
    fn drop_pair_marks_dropped_with_reason_and_deletes_feed_row() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        insert_feed_row(&conn, quote_uri);

        drop_pair(&conn, quote_uri, crate::store::DropReason::FollowerFloor).unwrap();

        assert_eq!(feed_count(&conn), 0);
        let (state, drop_reason): (String, Option<String>) = conn
            .query_row(
                "SELECT state, drop_reason FROM pairs WHERE quote_uri = ?1",
                [quote_uri],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "dropped");
        assert_eq!(drop_reason, Some("follower_floor".to_string()));
        assert_eq!(pair_count(&conn), 1, "the pair row stays");
    }

    // BC48: a mixed batch of `PairOutcome`s applies all three kinds
    // atomically.
    #[test]
    fn apply_verdicts_applies_promote_drop_and_demote_in_one_transaction() {
        let conn = migrated_conn();
        let promote_uri = "at://did:plc:q/app.bsky.feed.post/promote";
        let drop_uri = "at://did:plc:q/app.bsky.feed.post/drop";
        let demote_uri = "at://did:plc:q/app.bsky.feed.post/demote";
        insert_test_pair(&conn, promote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        insert_test_pair(&conn, drop_uri, "at://did:plc:o/app.bsky.feed.post/o2");
        insert_test_pair(&conn, demote_uri, "at://did:plc:o/app.bsky.feed.post/o3");
        conn.execute("UPDATE pairs SET state = 'promoted' WHERE quote_uri = ?1", [demote_uri])
            .unwrap();
        insert_feed_row(&conn, demote_uri);

        let mut row = feed_row_template(promote_uri);
        row.quote_uri = promote_uri.to_string();
        let outcomes = vec![
            PairOutcome::Promote(row),
            PairOutcome::Drop {
                quote_uri: drop_uri.to_string(),
                reason: crate::store::DropReason::FollowerFloor,
            },
            PairOutcome::Demote { quote_uri: demote_uri.to_string() },
        ];

        apply_verdicts(&conn, &outcomes).unwrap();

        let promote_state: String = conn
            .query_row("SELECT state FROM pairs WHERE quote_uri = ?1", [promote_uri], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(promote_state, "promoted");
        let (drop_state, drop_reason): (String, Option<String>) = conn
            .query_row(
                "SELECT state, drop_reason FROM pairs WHERE quote_uri = ?1",
                [drop_uri],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(drop_state, "dropped");
        assert_eq!(drop_reason, Some("follower_floor".to_string()));
        let demote_state: String = conn
            .query_row("SELECT state FROM pairs WHERE quote_uri = ?1", [demote_uri], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(demote_state, "candidate");
        assert_eq!(feed_count(&conn), 1, "only promote_uri's feed row remains");
    }

    #[test]
    fn apply_verdicts_of_an_empty_slice_is_a_noop() {
        let conn = migrated_conn();
        apply_verdicts(&conn, &[]).unwrap();
    }

    fn feed_row_template(quote_uri: &str) -> feed::FeedRow {
        feed::FeedRow {
            quote_uri: quote_uri.to_string(),
            quote_cid: format!("cid-{quote_uri}"),
            quote_did: "did:plc:q".to_string(),
            original_did: "did:plc:o".to_string(),
            quoted_at: 1_700_000_000,
            v_likes_q: 1,
            v_reposts_q: 0,
            v_replies_q: 0,
            v_likes_o: 0,
            v_reposts_o: 0,
            v_replies_o: 0,
            ratio: 1.5,
            rank: 1.0,
            promoted_at: 1_700_000_000,
            verified_at: 1_700_000_000,
        }
    }

    #[test]
    fn expire() {
        let conn = migrated_conn();

        // A candidate past the candidate TTL: pair, counts on both sides removed.
        let old_candidate = "at://did:plc:q/app.bsky.feed.post/old-candidate";
        let old_candidate_o = "at://did:plc:o/app.bsky.feed.post/old-candidate-o";
        insert_test_pair(&conn, old_candidate, old_candidate_o);
        conn.execute("UPDATE pairs SET first_seen_at = 0 WHERE quote_uri = ?1", [old_candidate])
            .unwrap();
        counts::incr(&conn, old_candidate_o, crate::store::writer::CountField::Likes, 1).unwrap();

        // A fresh candidate inside the TTL: kept.
        let fresh_candidate = "at://did:plc:q/app.bsky.feed.post/fresh-candidate";
        insert_test_pair(&conn, fresh_candidate, "at://did:plc:o/app.bsky.feed.post/fresh-o");

        // A promoted pair whose feed row is past the feed TTL: pair, feed row,
        // and both sides' counts removed.
        let old_feed = "at://did:plc:q/app.bsky.feed.post/old-feed";
        let old_feed_o = "at://did:plc:o/app.bsky.feed.post/old-feed-o";
        insert_test_pair(&conn, old_feed, old_feed_o);
        conn.execute("UPDATE pairs SET state = 'promoted' WHERE quote_uri = ?1", [old_feed])
            .unwrap();
        insert_feed_row(&conn, old_feed);
        conn.execute("UPDATE feed SET promoted_at = 0 WHERE quote_uri = ?1", [old_feed]).unwrap();
        counts::incr(&conn, old_feed_o, crate::store::writer::CountField::Likes, 1).unwrap();

        // A promoted pair past the candidate TTL but inside the feed TTL: kept
        // (BC60 — a promoted pair lives by the feed TTL, never the candidate TTL).
        let kept_promoted = "at://did:plc:q/app.bsky.feed.post/kept-promoted";
        insert_test_pair(&conn, kept_promoted, "at://did:plc:o/app.bsky.feed.post/kept-o");
        conn.execute(
            "UPDATE pairs SET state = 'promoted', first_seen_at = 0 WHERE quote_uri = ?1",
            [kept_promoted],
        )
        .unwrap();
        insert_feed_row(&conn, kept_promoted);

        let now = 1_700_000_000;
        let report = super::expire(&conn, now, 48, 30).unwrap();

        assert_eq!(report.candidates_expired, 1);
        assert_eq!(report.feed_expired, 1);
        let mut evicted = report.evicted_uris.clone();
        evicted.sort();
        let mut expected = vec![
            old_candidate.to_string(),
            old_candidate_o.to_string(),
            old_feed.to_string(),
            old_feed_o.to_string(),
        ];
        expected.sort();
        assert_eq!(evicted, expected);

        assert!(
            counts::counts_for(&conn, old_candidate_o).unwrap() == crate::score::Counts::default()
        );
        assert!(counts::counts_for(&conn, old_feed_o).unwrap() == crate::score::Counts::default());

        let remaining: std::collections::HashSet<String> = {
            let mut stmt = conn.prepare("SELECT quote_uri FROM pairs").unwrap();
            stmt.query_map([], |row| row.get::<_, String>(0)).unwrap().map(Result::unwrap).collect()
        };
        assert!(remaining.contains(fresh_candidate), "fresh candidate is kept");
        assert!(
            remaining.contains(kept_promoted),
            "promoted pair lives by the feed TTL, not the candidate TTL"
        );
        assert!(!remaining.contains(old_candidate));
        assert!(!remaining.contains(old_feed));
        assert_eq!(feed_count(&conn), 1, "only kept_promoted's feed row remains");
    }

    #[test]
    fn expire_keeps_a_shared_original() {
        let conn = migrated_conn();
        let original_uri = "at://did:plc:o/app.bsky.feed.post/shared-o";
        let q1 = "at://did:plc:q/app.bsky.feed.post/q1";
        let q2 = "at://did:plc:q/app.bsky.feed.post/q2";
        insert_test_pair(&conn, q1, original_uri);
        insert_test_pair(&conn, q2, original_uri);
        counts::incr(&conn, original_uri, crate::store::writer::CountField::Likes, 1).unwrap();
        // Only q1 is old enough to expire; q2 keeps a live reference to the
        // shared original.
        conn.execute("UPDATE pairs SET first_seen_at = 0 WHERE quote_uri = ?1", [q1]).unwrap();

        let report = super::expire(&conn, 1_700_000_000, 48, 30).unwrap();

        assert_eq!(report.candidates_expired, 1);
        assert!(
            !report.evicted_uris.contains(&original_uri.to_string()),
            "the shared original must not be reported as evicted"
        );
        assert_eq!(
            counts::counts_for(&conn, original_uri).unwrap(),
            crate::score::Counts { likes: 1, reposts: 0, replies: 0 },
            "the shared original's counts row must survive"
        );
    }

    // BC1, BC16: the cutoff includes a row exactly on it and excludes one
    // before it, and rows come back ascending by `first_seen_at`.
    #[test]
    fn pairs_since_includes_the_cutoff_and_orders_ascending() {
        let conn = migrated_conn();
        let before = "at://did:plc:q/app.bsky.feed.post/before";
        let on_cutoff = "at://did:plc:q/app.bsky.feed.post/on-cutoff";
        let after = "at://did:plc:q/app.bsky.feed.post/after";
        insert_test_pair(&conn, before, "at://did:plc:o/app.bsky.feed.post/before-o");
        insert_test_pair(&conn, on_cutoff, "at://did:plc:o/app.bsky.feed.post/on-cutoff-o");
        insert_test_pair(&conn, after, "at://did:plc:o/app.bsky.feed.post/after-o");
        conn.execute(
            "UPDATE pairs SET first_seen_at = 1_699_999_999 WHERE quote_uri = ?1",
            [before],
        )
        .unwrap();
        conn.execute(
            "UPDATE pairs SET first_seen_at = 1_700_000_000 WHERE quote_uri = ?1",
            [on_cutoff],
        )
        .unwrap();
        conn.execute(
            "UPDATE pairs SET first_seen_at = 1_700_000_001 WHERE quote_uri = ?1",
            [after],
        )
        .unwrap();

        let found = pairs_since(&conn, 1_700_000_000).unwrap();
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].quote_uri, on_cutoff);
        assert_eq!(found[1].quote_uri, after);
    }

    // BC6: a dropped pair comes back with its `drop_reason`.
    #[test]
    fn pairs_since_returns_a_dropped_pair_with_its_reason() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        drop_pair(&conn, quote_uri, crate::store::DropReason::FollowerFloor).unwrap();

        let found = pairs_since(&conn, 0).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].state, "dropped");
        assert_eq!(found[0].drop_reason, Some("follower_floor".to_string()));
    }

    // BC13: a candidate with no `feed` row has `None` in every verified
    // column.
    #[test]
    fn pairs_since_candidate_has_no_verified_counts() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");

        let found = pairs_since(&conn, 0).unwrap();
        assert_eq!(found.len(), 1);
        let row = &found[0];
        assert_eq!(row.state, "candidate");
        assert_eq!(row.v_likes_q, None);
        assert_eq!(row.v_reposts_q, None);
        assert_eq!(row.v_replies_q, None);
        assert_eq!(row.v_likes_o, None);
        assert_eq!(row.v_reposts_o, None);
        assert_eq!(row.v_replies_o, None);
        assert_eq!(row.ratio, None);
        assert_eq!(row.promoted_at, None);
    }

    // BC14: a promoted pair has `Some` in every verified column, plus
    // `ratio` and `promoted_at`.
    #[test]
    fn pairs_since_promoted_has_verified_counts() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");
        insert_feed_row(&conn, quote_uri);
        conn.execute("UPDATE pairs SET state = 'promoted' WHERE quote_uri = ?1", [quote_uri])
            .unwrap();

        let found = pairs_since(&conn, 0).unwrap();
        assert_eq!(found.len(), 1);
        let row = &found[0];
        assert_eq!(row.state, "promoted");
        assert_eq!(row.v_likes_q, Some(0));
        assert_eq!(row.v_reposts_q, Some(0));
        assert_eq!(row.v_replies_q, Some(0));
        assert_eq!(row.v_likes_o, Some(0));
        assert_eq!(row.v_reposts_o, Some(0));
        assert_eq!(row.v_replies_o, Some(0));
        assert_eq!(row.ratio, Some(1.0));
        assert_eq!(row.promoted_at, Some(1_700_000_000));
    }

    // BC5: a pair with no `counts` row on either side reads zero, not an
    // error.
    #[test]
    fn pairs_since_missing_counts_reads_as_zero() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_test_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1");

        let found = pairs_since(&conn, 0).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].counts_q, crate::score::Counts::default());
        assert_eq!(found[0].counts_o, crate::score::Counts::default());
    }

    // BC5: a side with a `counts` row reads its real values, saturated the
    // same way `counts::counts_for` saturates.
    #[test]
    fn pairs_since_reads_local_counts_from_both_sides() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        let original_uri = "at://did:plc:o/app.bsky.feed.post/o1";
        insert_test_pair(&conn, quote_uri, original_uri);
        counts::incr(&conn, quote_uri, crate::store::writer::CountField::Likes, 1_700_000_100)
            .unwrap();
        counts::incr(&conn, original_uri, crate::store::writer::CountField::Reposts, 1_700_000_100)
            .unwrap();

        let found = pairs_since(&conn, 0).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].counts_q, crate::score::Counts { likes: 1, reposts: 0, replies: 0 });
        assert_eq!(found[0].counts_o, crate::score::Counts { likes: 0, reposts: 1, replies: 0 });
    }

    // BC39: both of `expire`'s `IN (...)` deletes chunk at
    // `MAX_BOUND_PARAMS`. An unchunked delete over this many rows would fail
    // with "too many SQL variables"; success here proves the chunking loop
    // runs for both the `pairs` delete (expired feed rows) and the `counts`
    // delete (their orphaned URIs).
    #[test]
    fn expire_chunks_deletes_past_the_bound_parameter_limit() {
        let conn = migrated_conn();
        let n = crate::store::MAX_BOUND_PARAMS + 10;
        for i in 0..n {
            let quote_uri = format!("at://did:plc:q/app.bsky.feed.post/q{i}");
            let original_uri = format!("at://did:plc:o/app.bsky.feed.post/o{i}");
            insert_test_pair(&conn, &quote_uri, &original_uri);
            conn.execute("UPDATE pairs SET state = 'promoted' WHERE quote_uri = ?1", [&quote_uri])
                .unwrap();
            insert_feed_row(&conn, &quote_uri);
            conn.execute("UPDATE feed SET promoted_at = 0 WHERE quote_uri = ?1", [&quote_uri])
                .unwrap();
        }

        let report = super::expire(&conn, 1_700_000_000, 48, 30).unwrap();

        assert_eq!(report.feed_expired, n);
        assert_eq!(feed_count(&conn), 0);
        assert_eq!(pair_count(&conn), 0);
    }
}
