//! `interactions` table row operations, TECH-DESIGN section 6. Insert-only:
//! the table has no primary key and no reader in this story. Story 08's
//! `sendInteractions` handler is the caller.

use rusqlite::Connection;

use crate::store::StoreError;

/// Appends one row. The four payload columns are nullable and stored
/// verbatim; `received_at` is whatever `now` the caller passes (BC34).
/// Round 1 finding 5: prepared through `prepare_cached`, like every other
/// per-op statement in `commit_batch`'s path.
pub fn insert_interaction(
    conn: &Connection,
    item: Option<&str>,
    event: Option<&str>,
    feed_context: Option<&str>,
    req_id: Option<&str>,
    now: i64,
) -> Result<(), StoreError> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO interactions (received_at, item, event, feed_context, req_id) VALUES (?1, ?2, ?3, ?4, ?5)",
    )?;
    stmt.execute(rusqlite::params![now, item, event, feed_context, req_id])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::migrated_conn;

    #[test]
    fn insert_interaction_appends_a_row() {
        let conn = migrated_conn();
        insert_interaction(
            &conn,
            Some("at://did:plc:q/app.bsky.feed.post/q1"),
            Some("app.bsky.feed.defs#requestLess"),
            Some("ctx"),
            Some("req-1"),
            1_700_000_000,
        )
        .unwrap();

        let (received_at, item, event, feed_context, req_id): (
            i64,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = conn
            .query_row(
                "SELECT received_at, item, event, feed_context, req_id FROM interactions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(received_at, 1_700_000_000);
        assert_eq!(item, Some("at://did:plc:q/app.bsky.feed.post/q1".to_string()));
        assert_eq!(event, Some("app.bsky.feed.defs#requestLess".to_string()));
        assert_eq!(feed_context, Some("ctx".to_string()));
        assert_eq!(req_id, Some("req-1".to_string()));
    }

    #[test]
    fn insert_interaction_allows_null_payload_columns() {
        let conn = migrated_conn();
        insert_interaction(&conn, None, None, None, None, 1_700_000_000).unwrap();

        let count: i64 =
            conn.query_row("SELECT count(*) FROM interactions", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn insert_interaction_twice_appends_two_rows() {
        let conn = migrated_conn();
        insert_interaction(&conn, None, None, None, None, 1_700_000_000).unwrap();
        insert_interaction(&conn, None, None, None, None, 1_700_000_001).unwrap();

        let count: i64 =
            conn.query_row("SELECT count(*) FROM interactions", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 2);
    }
}
