//! `interactions` table row operations, TECH-DESIGN section 6. Story 08's
//! `sendInteractions` handler is the writer. Round 2 finding 9 (BC47) adds
//! the reader: no raw SQL sits outside `src/store/` (AGENTS.md), so the
//! `sendInteractions` HTTP tests read the row back through `interactions`
//! (via `Store::interactions`) rather than opening a second `rusqlite`
//! connection of their own.

use rusqlite::Connection;

use crate::store::StoreError;

/// One `interactions` row, read back in insertion order (`rowid` order:
/// the table has no other ordering column).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractionRow {
    pub received_at: i64,
    pub item: Option<String>,
    pub event: Option<String>,
    pub feed_context: Option<String>,
    pub req_id: Option<String>,
}

/// Every `interactions` row, oldest first (BC47). No caller yet in
/// production: `Store::interactions` (`src/store/mod.rs`) is read only by
/// `src/http/interactions.rs`'s own tests today.
pub fn interactions(conn: &Connection) -> Result<Vec<InteractionRow>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT received_at, item, event, feed_context, req_id FROM interactions ORDER BY rowid",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(InteractionRow {
            received_at: row.get(0)?,
            item: row.get(1)?,
            event: row.get(2)?,
            feed_context: row.get(3)?,
            req_id: row.get(4)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

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

    // BC47: `interactions` reads every row back, oldest first.
    #[test]
    fn interactions_returns_every_row_in_insertion_order() {
        let conn = migrated_conn();
        insert_interaction(
            &conn,
            Some("at://did:plc:q/app.bsky.feed.post/a"),
            Some("app.bsky.feed.defs#requestLess"),
            None,
            None,
            100,
        )
        .unwrap();
        insert_interaction(
            &conn,
            Some("at://did:plc:q/app.bsky.feed.post/b"),
            Some("app.bsky.feed.defs#interactionSeen"),
            Some("r=4.5"),
            Some("req-2"),
            200,
        )
        .unwrap();

        let rows = interactions(&conn).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].item, Some("at://did:plc:q/app.bsky.feed.post/a".to_string()));
        assert_eq!(rows[0].received_at, 100);
        assert_eq!(rows[1].feed_context, Some("r=4.5".to_string()));
        assert_eq!(rows[1].req_id, Some("req-2".to_string()));
    }

    #[test]
    fn interactions_of_an_empty_table_is_empty() {
        let conn = migrated_conn();
        assert_eq!(interactions(&conn).unwrap(), Vec::new());
    }
}
