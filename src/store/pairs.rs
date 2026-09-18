//! `pairs` table row operations, TECH-DESIGN section 6 and 5.2. Every
//! write function here takes an already-open `Connection` (or a
//! `Transaction`, which derefs to one) so `writer::commit_batch` can run all
//! of them inside one transaction. `hot_set_uris`, `dirty_candidates`,
//! `promoted_within`, `demote`, `drop_pair` and `expire` are the
//! synchronous reads the scorer pass (TECH-DESIGN section 7.2) needs.

use rusqlite::Connection;

use crate::score::Counts;
use crate::store::{counts, feed, DropReason, StoreError};

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
    conn.execute(
        "INSERT INTO pairs (quote_uri, quote_did, quote_cid, original_uri, original_did, quoted_at, first_seen_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(quote_uri) DO NOTHING",
        rusqlite::params![
            quote_uri,
            quote_did,
            quote_cid,
            original_uri,
            original_did,
            quoted_at,
            first_seen_at
        ],
    )?;
    Ok(())
}

/// A `post` delete for `uri`. `foreign_keys` is on and `feed.quote_uri`
/// references `pairs.quote_uri`, so if `uri` is a pair's `quote_uri` its
/// `feed` row is deleted before the `pairs` row (BC30). Every pair holding
/// `uri` as `original_uri` is instead marked `dropped` with
/// `original_gone`, and each of those pairs' `feed` rows is deleted, but the
/// pair rows themselves stay (BC31). A `uri` in no pair changes nothing
/// pair-side and is not an error (BC32); the `counts` row for `uri` is
/// deleted in every case, whether or not it exists.
pub fn delete_post(conn: &Connection, uri: &str) -> Result<(), StoreError> {
    // BC30: this `uri` as a quote_uri.
    feed::delete_feed_row(conn, uri)?;
    conn.execute("DELETE FROM pairs WHERE quote_uri = ?1", [uri])?;

    // BC31: this `uri` as an original_uri.
    let quote_uris: Vec<String> = {
        let mut stmt = conn.prepare("SELECT quote_uri FROM pairs WHERE original_uri = ?1")?;
        let rows = stmt.query_map([uri], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for quote_uri in &quote_uris {
        feed::delete_feed_row(conn, quote_uri)?;
    }
    conn.execute(
        "UPDATE pairs SET state = 'dropped', drop_reason = 'original_gone' WHERE original_uri = ?1",
        [uri],
    )?;

    // BC32: the counts row goes regardless of whether `uri` was in a pair.
    counts::delete_counts(conn, uri)?;
    Ok(())
}

/// A `postgate` detach for `quote_uri`. Marks the pair `dropped` with
/// `detached` and deletes its `feed` row; the pair row stays (BC33).
pub fn detach(conn: &Connection, quote_uri: &str) -> Result<(), StoreError> {
    feed::delete_feed_row(conn, quote_uri)?;
    conn.execute(
        "UPDATE pairs SET state = 'dropped', drop_reason = 'detached' WHERE quote_uri = ?1",
        [quote_uri],
    )?;
    Ok(())
}

/// Both URIs of every pair whose `state` is not `dropped` (BC45).
/// Duplicates are not removed; the caller (the scorer's hot set) holds a
/// set.
pub fn hot_set_uris(conn: &Connection) -> Result<impl Iterator<Item = String>, StoreError> {
    let mut stmt =
        conn.prepare("SELECT quote_uri, original_uri FROM pairs WHERE state != 'dropped'")?;
    let rows = stmt.query_map([], |row| {
        let quote_uri: String = row.get(0)?;
        let original_uri: String = row.get(1)?;
        Ok([quote_uri, original_uri])
    })?;
    let uris: Vec<String> = rows
        .collect::<Result<Vec<[String; 2]>, rusqlite::Error>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(uris.into_iter())
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
    let mut stmt = conn.prepare(
        "SELECT quote_uri, quote_did, quote_cid, original_uri, original_did, quoted_at, first_seen_at
         FROM pairs
         WHERE state = 'candidate'
           AND first_seen_at >= ?1
           AND EXISTS (
               SELECT 1 FROM counts
               WHERE (counts.post_uri = pairs.quote_uri OR counts.post_uri = pairs.original_uri)
                 AND counts.dirty = 1
           )",
    )?;
    let rows = stmt.query_map([cutoff], pair_columns)?;
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
         WHERE state = 'promoted' AND quoted_at >= ?1",
    )?;
    let rows = stmt.query_map([cutoff], pair_columns)?;
    rows_with_counts(conn, rows)
}

/// Deletes the `feed` row and returns the pair to `candidate` with
/// `drop_reason = NULL` (BC55).
pub fn demote(conn: &Connection, quote_uri: &str) -> Result<(), StoreError> {
    feed::delete_feed_row(conn, quote_uri)?;
    conn.execute(
        "UPDATE pairs SET state = 'candidate', drop_reason = NULL WHERE quote_uri = ?1",
        [quote_uri],
    )?;
    Ok(())
}

/// Marks the pair `dropped` with `reason` and deletes its `feed` row
/// (BC56). The pair row stays.
pub fn drop_pair(conn: &Connection, quote_uri: &str, reason: DropReason) -> Result<(), StoreError> {
    feed::delete_feed_row(conn, quote_uri)?;
    conn.execute(
        "UPDATE pairs SET state = 'dropped', drop_reason = ?2 WHERE quote_uri = ?1",
        rusqlite::params![quote_uri, reason.as_str()],
    )?;
    Ok(())
}

/// TECH-DESIGN section 7.2 step 6. Deletes `candidate` or `dropped` pairs
/// whose `first_seen_at` is older than `candidate_ttl_h` hours, and their
/// `counts` rows (BC58). Deletes `feed` rows whose `promoted_at` is older
/// than `feed_ttl_d` days, and their pairs and counts rows with them
/// (BC59). A `promoted` pair inside the feed TTL but past the candidate TTL
/// is kept: it lives by the feed TTL, never the candidate TTL (BC60).
pub fn expire(
    conn: &Connection,
    now: i64,
    candidate_ttl_h: i64,
    feed_ttl_d: i64,
) -> Result<ExpireReport, StoreError> {
    let mut report = ExpireReport::default();

    // BC58: candidate/dropped pairs older than the candidate TTL. `feed`
    // never holds a row for these states, so no feed delete is needed.
    let candidate_cutoff = now - candidate_ttl_h * 3600;
    let expired_candidates: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT quote_uri, original_uri FROM pairs
             WHERE state IN ('candidate', 'dropped') AND first_seen_at < ?1",
        )?;
        let rows = stmt.query_map([candidate_cutoff], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for (quote_uri, original_uri) in expired_candidates {
        conn.execute("DELETE FROM pairs WHERE quote_uri = ?1", [&quote_uri])?;
        counts::delete_counts(conn, &quote_uri)?;
        counts::delete_counts(conn, &original_uri)?;
        report.evicted_uris.push(quote_uri);
        report.evicted_uris.push(original_uri);
        report.candidates_expired += 1;
    }

    // BC59: feed rows older than the feed TTL, and their pairs.
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
    for (quote_uri, original_uri) in expired_feed {
        feed::delete_feed_row(conn, &quote_uri)?;
        conn.execute("DELETE FROM pairs WHERE quote_uri = ?1", [&quote_uri])?;
        counts::delete_counts(conn, &quote_uri)?;
        counts::delete_counts(conn, &original_uri)?;
        report.evicted_uris.push(quote_uri);
        report.evicted_uris.push(original_uri);
        report.feed_expired += 1;
    }

    Ok(report)
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
    use crate::store::schema;

    fn migrated_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        schema::migrate(&conn).unwrap();
        conn
    }

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
            "INSERT INTO feed (quote_uri, quote_cid, quote_did, original_did, quoted_at, ratio, rank, promoted_at, verified_at)
             VALUES (?1, 'bafyq', 'did:plc:q', 'did:plc:o', 1700000000, 1.0, 1.0, 1700000000, 1700000000)",
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

        assert_eq!(pair_count(&conn), 0);
        assert_eq!(feed_count(&conn), 0);
        assert_eq!(counts::counts_for(&conn, quote_uri).unwrap(), crate::score::Counts::default());
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

    #[test]
    fn hot_set_uris_yields_both_uris_of_every_non_dropped_pair() {
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
        detach(&conn, "at://did:plc:q/app.bsky.feed.post/q2").unwrap();

        let mut uris: Vec<String> = hot_set_uris(&conn).unwrap().collect();
        uris.sort();
        assert_eq!(
            uris,
            vec![
                "at://did:plc:o/app.bsky.feed.post/o1".to_string(),
                "at://did:plc:q/app.bsky.feed.post/q1".to_string(),
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
}
