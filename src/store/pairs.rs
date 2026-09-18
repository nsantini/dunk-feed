//! `pairs` table row operations, TECH-DESIGN section 6 and 5.2. Every
//! function here takes an already-open `Connection` (or a `Transaction`,
//! which derefs to one) so `writer::commit_batch` can run all of them inside
//! one transaction. The synchronous reads the scorer needs (`hot_set_uris`,
//! `dirty_candidates`, `promoted_within`, `demote`, `drop_pair`, `expire`)
//! land in slice 4.0.

use rusqlite::Connection;

use crate::store::{counts, feed, StoreError};

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
}
