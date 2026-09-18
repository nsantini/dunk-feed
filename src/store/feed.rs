//! `feed` table row operations, TECH-DESIGN section 6. `promote` and
//! `feed_rows` are the writes and reads the scorer's promotion step
//! (section 7.2 step 4) and snapshot step (section 7.3) need.

use rusqlite::Connection;

use crate::store::StoreError;

/// One row of the `feed` table, TECH-DESIGN section 6. The six `v_*`
/// columns are nullable because a pair can be promoted before both sides
/// have every count type.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedRow {
    pub quote_uri: String,
    pub quote_cid: String,
    pub quote_did: String,
    pub original_did: String,
    pub quoted_at: i64,
    pub v_likes_q: Option<i64>,
    pub v_reposts_q: Option<i64>,
    pub v_replies_q: Option<i64>,
    pub v_likes_o: Option<i64>,
    pub v_reposts_o: Option<i64>,
    pub v_replies_o: Option<i64>,
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
    conn.execute("DELETE FROM feed WHERE quote_uri = ?1", [quote_uri])?;
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
    conn.execute(
        "UPDATE pairs SET state = 'promoted', drop_reason = NULL WHERE quote_uri = ?1",
        [&row.quote_uri],
    )?;
    Ok(())
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
    use crate::store::schema;

    fn migrated_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        schema::migrate(&conn).unwrap();
        conn
    }

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
            "INSERT INTO feed (quote_uri, quote_cid, quote_did, original_did, quoted_at, ratio, rank, promoted_at, verified_at)
             VALUES (?1, 'bafyq', 'did:plc:q', 'did:plc:o', 1700000000, 1.0, 1.0, 1700000000, 1700000000)",
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
            v_likes_q: Some(1),
            v_reposts_q: None,
            v_replies_q: None,
            v_likes_o: Some(2),
            v_reposts_o: None,
            v_replies_o: None,
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
        second.v_likes_q = Some(99);
        promote(&conn, &second).unwrap();

        let (rank, promoted_at, v_likes_q): (f64, i64, Option<i64>) = conn
            .query_row(
                "SELECT rank, promoted_at, v_likes_q FROM feed WHERE quote_uri = ?1",
                [quote_uri],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(rank, 2.5, "every column but promoted_at is replaced");
        assert_eq!(v_likes_q, Some(99));
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
}
