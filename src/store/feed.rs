//! `feed` table row operations, TECH-DESIGN section 6. `promote` and
//! `feed_rows` are the writes and reads the scorer's promotion step
//! (section 7.2 step 4) and snapshot step (section 7.3) need.

use rusqlite::Connection;

use crate::store::{PairState, StoreError};

/// One row of the `feed` table, TECH-DESIGN section 6. The six `v_*`
/// columns are `INTEGER NOT NULL` (round 1 finding 8, BC82): section 7.2
/// step 4 promotes only on verified counts, so all six always exist by the
/// time a `FeedRow` is written, and a promotion without them is a type
/// error rather than a null row.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedRow {
    pub quote_uri: String,
    pub quote_cid: String,
    pub quote_did: String,
    pub original_did: String,
    pub quoted_at: i64,
    pub v_likes_q: i64,
    pub v_reposts_q: i64,
    pub v_replies_q: i64,
    pub v_likes_o: i64,
    pub v_reposts_o: i64,
    pub v_replies_o: i64,
    pub ratio: f64,
    pub rank: f64,
    pub promoted_at: i64,
    pub verified_at: i64,
}

/// Deletes the `feed` row for `quote_uri`, if one exists. A no-op when
/// there is none. Every caller that also touches `pairs` (`delete_post`,
/// `detach`, and slice 4.0's `demote`/`drop_pair`) deletes the `feed` row
/// first, because `feed.quote_uri` references `pairs.quote_uri` and
/// `foreign_keys` is on.
pub fn delete_feed_row(conn: &Connection, quote_uri: &str) -> Result<(), StoreError> {
    let mut stmt = conn.prepare_cached("DELETE FROM feed WHERE quote_uri = ?1")?;
    stmt.execute([quote_uri])?;
    Ok(())
}

/// Upserts `row` and sets the pair's `state = 'promoted'` with
/// `drop_reason = NULL` (BC52, BC53). On a repeat for a `quote_uri` that
/// already has a feed row, every column but `promoted_at` is replaced;
/// `promoted_at` keeps the value from the first promotion. The feed row is
/// written before the `pairs` update, so a `quote_uri` with no `pairs` row
/// fails on the foreign key and `pairs` is never touched (BC54).
pub fn promote(conn: &Connection, row: &FeedRow) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO feed (
            quote_uri, quote_cid, quote_did, original_did, quoted_at,
            v_likes_q, v_reposts_q, v_replies_q, v_likes_o, v_reposts_o, v_replies_o,
            ratio, rank, promoted_at, verified_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
         ON CONFLICT(quote_uri) DO UPDATE SET
            quote_cid = excluded.quote_cid,
            quote_did = excluded.quote_did,
            original_did = excluded.original_did,
            quoted_at = excluded.quoted_at,
            v_likes_q = excluded.v_likes_q,
            v_reposts_q = excluded.v_reposts_q,
            v_replies_q = excluded.v_replies_q,
            v_likes_o = excluded.v_likes_o,
            v_reposts_o = excluded.v_reposts_o,
            v_replies_o = excluded.v_replies_o,
            ratio = excluded.ratio,
            rank = excluded.rank,
            verified_at = excluded.verified_at",
        rusqlite::params![
            row.quote_uri,
            row.quote_cid,
            row.quote_did,
            row.original_did,
            row.quoted_at,
            row.v_likes_q,
            row.v_reposts_q,
            row.v_replies_q,
            row.v_likes_o,
            row.v_reposts_o,
            row.v_replies_o,
            row.ratio,
            row.rank,
            row.promoted_at,
            row.verified_at,
        ],
    )?;
    let mut stmt = conn
        .prepare_cached("UPDATE pairs SET state = ?2, drop_reason = NULL WHERE quote_uri = ?1")?;
    stmt.execute(rusqlite::params![row.quote_uri, PairState::Promoted.as_str()])?;
    Ok(())
}

/// A `feed` row's author DIDs, keyed by `quote_uri`, for the graph worker's
/// step 2 candidate lookup (slice 2.0, BC1). `FeedItem` (`graph::` types)
/// holds only author hashes, never DID strings, so the worker reads them
/// back from `feed` for the snapshot items it is considering.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedAuthors {
    pub quote_uri: String,
    pub quote_did: String,
    pub original_did: String,
}

/// Reads `quote_uri`, `quote_did` and `original_did` for the rows in
/// `quote_uris` that still exist in `feed` (BC1a): a `quote_uri` demoted or
/// dropped after the snapshot was built has no row, and is silently absent
/// from the result rather than an error. Chunked past `MAX_BOUND_PARAMS`
/// through `for_each_in_chunk` (BC50's rule), the same shape
/// `authors::authors_get_many` uses for its `IN (...)` read. An empty
/// `quote_uris` makes no query at all.
#[allow(dead_code)] // First caller is the worker (`graph/queue.rs`, slice 2.0).
pub fn feed_authors_by_quote_uri(
    conn: &Connection,
    quote_uris: &[&str],
) -> Result<Vec<FeedAuthors>, StoreError> {
    let mut rows = Vec::new();
    crate::store::for_each_in_chunk(quote_uris, |chunk, placeholders| {
        let sql = format!(
            "SELECT quote_uri, quote_did, original_did FROM feed WHERE quote_uri IN ({placeholders})"
        );
        let mut stmt = conn.prepare_cached(&sql)?;
        let mapped = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
            Ok(FeedAuthors {
                quote_uri: row.get(0)?,
                quote_did: row.get(1)?,
                original_did: row.get(2)?,
            })
        })?;
        for row in mapped {
            rows.push(row?);
        }
        Ok(())
    })?;
    Ok(rows)
}

/// Every `feed` row, ordered `rank DESC, quote_cid ASC` (BC62), matching
/// TECH-DESIGN section 7.3's starting order. The tie rule on `quote_cid` is
/// total, so the order is stable across runs (BC63).
pub fn feed_rows(conn: &Connection) -> Result<Vec<FeedRow>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT quote_uri, quote_cid, quote_did, original_did, quoted_at,
                v_likes_q, v_reposts_q, v_replies_q, v_likes_o, v_reposts_o, v_replies_o,
                ratio, rank, promoted_at, verified_at
         FROM feed
         ORDER BY rank DESC, quote_cid ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(FeedRow {
            quote_uri: row.get(0)?,
            quote_cid: row.get(1)?,
            quote_did: row.get(2)?,
            original_did: row.get(3)?,
            quoted_at: row.get(4)?,
            v_likes_q: row.get(5)?,
            v_reposts_q: row.get(6)?,
            v_replies_q: row.get(7)?,
            v_likes_o: row.get(8)?,
            v_reposts_o: row.get(9)?,
            v_replies_o: row.get(10)?,
            ratio: row.get(11)?,
            rank: row.get(12)?,
            promoted_at: row.get(13)?,
            verified_at: row.get(14)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::Sqlite)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::migrated_conn;

    fn insert_pair_row(conn: &Connection, quote_uri: &str) {
        crate::store::pairs::insert_pair(
            conn,
            quote_uri,
            "did:plc:q",
            "bafyq",
            "at://did:plc:o/app.bsky.feed.post/o1",
            "did:plc:o",
            1_700_000_000,
            1_700_000_000,
        )
        .unwrap();
    }

    #[test]
    fn delete_feed_row_on_a_missing_row_is_a_noop() {
        let conn = migrated_conn();
        delete_feed_row(&conn, "at://did:plc:q/app.bsky.feed.post/q1").unwrap();
    }

    #[test]
    fn delete_feed_row_removes_an_existing_row() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_pair_row(&conn, quote_uri);
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

        delete_feed_row(&conn, quote_uri).unwrap();

        let count: i64 = conn.query_row("SELECT count(*) FROM feed", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 0);
    }

    fn feed_row(quote_uri: &str, rank: f64, promoted_at: i64) -> FeedRow {
        FeedRow {
            quote_uri: quote_uri.to_string(),
            quote_cid: "bafyq".to_string(),
            quote_did: "did:plc:q".to_string(),
            original_did: "did:plc:o".to_string(),
            quoted_at: 1_700_000_000,
            v_likes_q: 1,
            v_reposts_q: 0,
            v_replies_q: 0,
            v_likes_o: 2,
            v_reposts_o: 0,
            v_replies_o: 0,
            ratio: 1.5,
            rank,
            promoted_at,
            verified_at: 1_700_000_000,
        }
    }

    #[test]
    fn promote_inserts_the_row_and_sets_pairs_promoted() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_pair_row(&conn, quote_uri);

        promote(&conn, &feed_row(quote_uri, 1.0, 1_700_000_000)).unwrap();

        let (state, drop_reason): (String, Option<String>) = conn
            .query_row(
                "SELECT state, drop_reason FROM pairs WHERE quote_uri = ?1",
                [quote_uri],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "promoted");
        assert_eq!(drop_reason, None);
        let count: i64 = conn.query_row("SELECT count(*) FROM feed", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn promote_on_an_existing_row_upserts_and_keeps_first_promoted_at() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_pair_row(&conn, quote_uri);

        promote(&conn, &feed_row(quote_uri, 1.0, 1_700_000_000)).unwrap();
        let mut second = feed_row(quote_uri, 2.5, 1_700_099_999);
        second.v_likes_q = 99;
        promote(&conn, &second).unwrap();

        let (rank, promoted_at, v_likes_q): (f64, i64, i64) = conn
            .query_row(
                "SELECT rank, promoted_at, v_likes_q FROM feed WHERE quote_uri = ?1",
                [quote_uri],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(rank, 2.5, "every column but promoted_at is replaced");
        assert_eq!(v_likes_q, 99);
        assert_eq!(promoted_at, 1_700_000_000, "promoted_at keeps its first value");
    }

    #[test]
    fn promote_without_a_pair_row_fails_on_the_foreign_key() {
        let conn = migrated_conn();
        let err = promote(
            &conn,
            &feed_row("at://did:plc:q/app.bsky.feed.post/missing", 1.0, 1_700_000_000),
        )
        .unwrap_err();
        assert!(matches!(err, StoreError::Sqlite(_)));
        let count: i64 = conn.query_row("SELECT count(*) FROM feed", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn feed_rows_orders_by_rank_desc_then_quote_cid_asc() {
        let conn = migrated_conn();
        for (quote_uri, quote_cid, rank) in [
            ("at://did:plc:q/app.bsky.feed.post/a", "cid-b", 1.0),
            ("at://did:plc:q/app.bsky.feed.post/b", "cid-a", 1.0),
            ("at://did:plc:q/app.bsky.feed.post/c", "cid-z", 5.0),
        ] {
            crate::store::pairs::insert_pair(
                &conn,
                quote_uri,
                "did:plc:q",
                quote_cid,
                "at://did:plc:o/app.bsky.feed.post/o1",
                "did:plc:o",
                1_700_000_000,
                1_700_000_000,
            )
            .unwrap();
            let mut row = feed_row(quote_uri, rank, 1_700_000_000);
            row.quote_cid = quote_cid.to_string();
            promote(&conn, &row).unwrap();
        }

        let rows = feed_rows(&conn).unwrap();
        let order: Vec<&str> = rows.iter().map(|r| r.quote_cid.as_str()).collect();
        assert_eq!(
            order,
            vec!["cid-z", "cid-a", "cid-b"],
            "rank 5.0 first, then the rank-1.0 tie broken by quote_cid"
        );
    }

    // BC1: the lookup returns the quoter and original DIDs for a `quote_uri`
    // that still has a `feed` row.
    #[test]
    fn feed_authors_by_quote_uri_returns_known_rows() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_pair_row(&conn, quote_uri);
        promote(&conn, &feed_row(quote_uri, 1.0, 1_700_000_000)).unwrap();

        let rows = feed_authors_by_quote_uri(&conn, &[quote_uri]).unwrap();
        assert_eq!(
            rows,
            vec![FeedAuthors {
                quote_uri: quote_uri.to_string(),
                quote_did: "did:plc:q".to_string(),
                original_did: "did:plc:o".to_string(),
            }]
        );
    }

    // BC1a: a `quote_uri` with no `feed` row (demoted after the snapshot was
    // built) is silently absent from the result, not an error.
    #[test]
    fn feed_authors_by_quote_uri_skips_a_missing_row() {
        let conn = migrated_conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/q1";
        insert_pair_row(&conn, quote_uri);
        promote(&conn, &feed_row(quote_uri, 1.0, 1_700_000_000)).unwrap();

        let rows = feed_authors_by_quote_uri(
            &conn,
            &[quote_uri, "at://did:plc:q/app.bsky.feed.post/missing"],
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].quote_uri, quote_uri);
    }

    #[test]
    fn feed_authors_by_quote_uri_of_empty_list_makes_no_query_and_returns_empty() {
        let conn = migrated_conn();
        assert_eq!(feed_authors_by_quote_uri(&conn, &[]).unwrap(), Vec::new());
    }

    // BC2's chunking rule (BC50): a lookup longer than `MAX_BOUND_PARAMS`
    // splits into multiple statements instead of failing with "too many SQL
    // variables".
    #[test]
    fn feed_authors_by_quote_uri_chunks_past_the_bound_parameter_limit() {
        let conn = migrated_conn();
        let n = crate::store::MAX_BOUND_PARAMS + 10;
        let mut quote_uris = Vec::with_capacity(n);
        for i in 0..n {
            let quote_uri = format!("at://did:plc:q/app.bsky.feed.post/q{i}");
            let quote_cid = format!("cid{i}");
            crate::store::pairs::insert_pair(
                &conn,
                &quote_uri,
                "did:plc:q",
                &quote_cid,
                "at://did:plc:o/app.bsky.feed.post/o1",
                "did:plc:o",
                1_700_000_000,
                1_700_000_000,
            )
            .unwrap();
            let mut row = feed_row(&quote_uri, 1.0, 1_700_000_000);
            row.quote_cid = quote_cid;
            promote(&conn, &row).unwrap();
            quote_uris.push(quote_uri);
        }

        let refs: Vec<&str> = quote_uris.iter().map(String::as_str).collect();
        let rows = feed_authors_by_quote_uri(&conn, &refs).unwrap();
        assert_eq!(rows.len(), n);
    }
}
