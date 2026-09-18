//! `counts` table row operations, TECH-DESIGN section 6. A row is created
//! lazily on the first event for a URI (BC11), never on pair insert, which
//! matches section 6's note that most `O` rows never exist.

use rusqlite::Connection;

use crate::score::Counts;
use crate::store::writer::CountField;
use crate::store::StoreError;

/// Moves exactly the one column `field` names by one, creating the row on
/// first use (BC11, BC28). Always sets `dirty = 1` and `last_event_at =
/// now`, whether the row is new or already existed. Called twice in one
/// batch for the same `post_uri` moves the column by two (BC29), because
/// each call is its own statement inside the caller's transaction.
pub fn incr(
    conn: &Connection,
    post_uri: &str,
    field: CountField,
    now: i64,
) -> Result<(), StoreError> {
    let column = match field {
        CountField::Likes => "likes",
        CountField::Reposts => "reposts",
        CountField::Replies => "replies",
    };
    let sql = format!(
        "INSERT INTO counts (post_uri, {column}, last_event_at, dirty) VALUES (?1, 1, ?2, 1)
         ON CONFLICT(post_uri) DO UPDATE SET {column} = {column} + 1, last_event_at = ?2, dirty = 1"
    );
    conn.execute(&sql, rusqlite::params![post_uri, now])?;
    Ok(())
}

/// Clears `dirty` on every URI in `post_uris`. A URI with no `counts` row is
/// skipped, not an error (BC50): the `UPDATE` simply touches zero rows.
pub fn clear_dirty(conn: &Connection, post_uris: &[&str]) -> Result<(), StoreError> {
    for post_uri in post_uris {
        conn.execute("UPDATE counts SET dirty = 0 WHERE post_uri = ?1", [post_uri])?;
    }
    Ok(())
}

/// Deletes the `counts` row for `post_uri`, if one exists. A no-op when
/// there is none.
pub fn delete_counts(conn: &Connection, post_uri: &str) -> Result<(), StoreError> {
    conn.execute("DELETE FROM counts WHERE post_uri = ?1", [post_uri])?;
    Ok(())
}

/// Reads the three counters for `post_uri` into a `score::Counts`,
/// saturating at `u32::MAX` (BC49): `Counts` is `u32` per TECH-DESIGN
/// section 7.1 and a stored count is never negative. A URI with no row
/// reads as all zeroes, matching a post that has had no event yet.
pub fn counts_for(conn: &Connection, post_uri: &str) -> Result<Counts, StoreError> {
    let row = conn.query_row(
        "SELECT likes, reposts, replies FROM counts WHERE post_uri = ?1",
        [post_uri],
        |row| {
            let likes: i64 = row.get(0)?;
            let reposts: i64 = row.get(1)?;
            let replies: i64 = row.get(2)?;
            Ok((likes, reposts, replies))
        },
    );
    match row {
        Ok((likes, reposts, replies)) => Ok(Counts {
            likes: saturate(likes),
            reposts: saturate(reposts),
            replies: saturate(replies),
        }),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(Counts::default()),
        Err(err) => Err(StoreError::Sqlite(err)),
    }
}

fn saturate(value: i64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::schema;

    fn migrated_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::migrate(&conn).unwrap();
        conn
    }

    #[test]
    fn lazy_row_creation() {
        let conn = migrated_conn();
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";
        assert_eq!(counts_for(&conn, uri).unwrap(), Counts::default());

        incr(&conn, uri, CountField::Likes, 1_700_000_000).unwrap();
        assert_eq!(counts_for(&conn, uri).unwrap(), Counts { likes: 1, reposts: 0, replies: 0 });
    }

    #[test]
    fn incr_moves_only_the_named_column() {
        let conn = migrated_conn();
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";
        incr(&conn, uri, CountField::Reposts, 1_700_000_000).unwrap();
        assert_eq!(counts_for(&conn, uri).unwrap(), Counts { likes: 0, reposts: 1, replies: 0 });
    }

    #[test]
    fn incr_same_uri_twice_moves_the_column_by_two() {
        let conn = migrated_conn();
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";
        incr(&conn, uri, CountField::Replies, 1_700_000_000).unwrap();
        incr(&conn, uri, CountField::Replies, 1_700_000_001).unwrap();
        assert_eq!(counts_for(&conn, uri).unwrap(), Counts { likes: 0, reposts: 0, replies: 2 });

        let last_event_at: i64 = conn
            .query_row("SELECT last_event_at FROM counts WHERE post_uri = ?1", [uri], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(last_event_at, 1_700_000_001);
    }

    #[test]
    fn incr_sets_dirty() {
        let conn = migrated_conn();
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";
        incr(&conn, uri, CountField::Likes, 1_700_000_000).unwrap();
        let dirty: i64 = conn
            .query_row("SELECT dirty FROM counts WHERE post_uri = ?1", [uri], |row| row.get(0))
            .unwrap();
        assert_eq!(dirty, 1);
    }

    #[test]
    fn clear_dirty_skips_a_uri_with_no_row() {
        let conn = migrated_conn();
        // No row for this URI; must not error.
        clear_dirty(&conn, &["at://did:plc:o/app.bsky.feed.post/missing"]).unwrap();
    }

    #[test]
    fn clear_dirty_clears_existing_rows() {
        let conn = migrated_conn();
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";
        incr(&conn, uri, CountField::Likes, 1_700_000_000).unwrap();
        clear_dirty(&conn, &[uri]).unwrap();
        let dirty: i64 = conn
            .query_row("SELECT dirty FROM counts WHERE post_uri = ?1", [uri], |row| row.get(0))
            .unwrap();
        assert_eq!(dirty, 0);
    }

    #[test]
    fn delete_counts_removes_the_row() {
        let conn = migrated_conn();
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";
        incr(&conn, uri, CountField::Likes, 1_700_000_000).unwrap();
        delete_counts(&conn, uri).unwrap();
        assert_eq!(counts_for(&conn, uri).unwrap(), Counts::default());
    }

    #[test]
    fn counts_for_saturates_at_u32_max() {
        let conn = migrated_conn();
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";
        conn.execute(
            "INSERT INTO counts (post_uri, likes, last_event_at, dirty) VALUES (?1, ?2, 1700000000, 1)",
            rusqlite::params![uri, (u32::MAX as i64) + 10],
        )
        .unwrap();
        assert_eq!(counts_for(&conn, uri).unwrap().likes, u32::MAX);
    }
}
