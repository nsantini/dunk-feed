//! `feed` table row operations, TECH-DESIGN section 6. This slice defines
//! `FeedRow` and `delete_feed_row` only; `promote` and `feed_rows` (the
//! writes and reads the scorer's promotion step needs) land in slice 4.0.

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
}
