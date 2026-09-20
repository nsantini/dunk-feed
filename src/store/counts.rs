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
/// each call is its own statement inside the caller's transaction. Round 1
/// finding 5 (BC77): one of three fixed SQL strings, one per `CountField`,
/// prepared through `prepare_cached` so no batch of up to 1,000 ops
/// re-parses SQL per op.
pub fn incr(
    conn: &Connection,
    post_uri: &str,
    field: CountField,
    now: i64,
) -> Result<(), StoreError> {
    let sql = match field {
        CountField::Likes => {
            "INSERT INTO counts (post_uri, likes, last_event_at, dirty) VALUES (?1, 1, ?2, 1)
             ON CONFLICT(post_uri) DO UPDATE SET likes = likes + 1, last_event_at = ?2, dirty = 1"
        }
        CountField::Reposts => {
            "INSERT INTO counts (post_uri, reposts, last_event_at, dirty) VALUES (?1, 1, ?2, 1)
             ON CONFLICT(post_uri) DO UPDATE SET reposts = reposts + 1, last_event_at = ?2, dirty = 1"
        }
        CountField::Replies => {
            "INSERT INTO counts (post_uri, replies, last_event_at, dirty) VALUES (?1, 1, ?2, 1)
             ON CONFLICT(post_uri) DO UPDATE SET replies = replies + 1, last_event_at = ?2, dirty = 1"
        }
    };
    let mut stmt = conn.prepare_cached(sql)?;
    stmt.execute(rusqlite::params![post_uri, now])?;
    Ok(())
}

/// Clears `dirty` on every URI in `post_uris` in one set-based `UPDATE`
/// (BC50, round 1 finding 5), not one statement per URI. A URI with no
/// `counts` row is skipped, not an error: the `UPDATE` simply touches zero
/// rows for it. A `post_uris` longer than `MAX_BOUND_PARAMS` is split into
/// chunks, one `UPDATE` per chunk (BC39), so SQLite's bound-parameter limit
/// is never exceeded.
pub fn clear_dirty(conn: &Connection, post_uris: &[&str]) -> Result<(), StoreError> {
    set_dirty(conn, post_uris, 0)
}

/// Sets `dirty = 1` on every URI in `post_uris`, the reverse of
/// `clear_dirty`. Used by the scorer (BC36) to restore `dirty` for the URIs
/// of a `getPosts` chunk that failed, after `select` already cleared it on
/// read. Chunked the same way `clear_dirty` is (BC39).
pub fn mark_dirty(conn: &Connection, post_uris: &[&str]) -> Result<(), StoreError> {
    set_dirty(conn, post_uris, 1)
}

/// Shared body of `clear_dirty` and `mark_dirty`: one set-based `UPDATE` per
/// chunk of at most `MAX_BOUND_PARAMS` URIs (BC39). A URI with no `counts`
/// row is skipped, not an error.
fn set_dirty(conn: &Connection, post_uris: &[&str], dirty: i64) -> Result<(), StoreError> {
    for chunk in post_uris.chunks(crate::store::MAX_BOUND_PARAMS) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql = format!("UPDATE counts SET dirty = {dirty} WHERE post_uri IN ({placeholders})");
        conn.execute(&sql, rusqlite::params_from_iter(chunk.iter()))?;
    }
    Ok(())
}

/// Sets `dirty = 1` on every `counts` row in one statement (round 1
/// finding 2, `Op::MarkAllDirty`).
pub fn mark_all_dirty(conn: &Connection) -> Result<(), StoreError> {
    let mut stmt = conn.prepare_cached("UPDATE counts SET dirty = 1")?;
    stmt.execute([])?;
    Ok(())
}

/// Deletes the `counts` row for `post_uri`, if one exists. A no-op when
/// there is none.
pub fn delete_counts(conn: &Connection, post_uri: &str) -> Result<(), StoreError> {
    let mut stmt = conn.prepare_cached("DELETE FROM counts WHERE post_uri = ?1")?;
    stmt.execute([post_uri])?;
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
    use crate::store::test_support::migrated_conn;

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
    fn mark_dirty_sets_the_flag_on_an_existing_row() {
        let conn = migrated_conn();
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";
        incr(&conn, uri, CountField::Likes, 1_700_000_000).unwrap();
        clear_dirty(&conn, &[uri]).unwrap();

        mark_dirty(&conn, &[uri]).unwrap();

        let dirty: i64 = conn
            .query_row("SELECT dirty FROM counts WHERE post_uri = ?1", [uri], |row| row.get(0))
            .unwrap();
        assert_eq!(dirty, 1);
    }

    #[test]
    fn mark_dirty_skips_a_uri_with_no_row() {
        let conn = migrated_conn();
        // No row for this URI; must not error.
        mark_dirty(&conn, &["at://did:plc:o/app.bsky.feed.post/missing"]).unwrap();
    }

    // BC39: a URI list past SQLite's bound-parameter limit must not error.
    // An unchunked `IN (...)` over this many placeholders would fail with
    // "too many SQL variables"; success here proves the chunking loop runs.
    #[test]
    fn clear_dirty_chunks_a_list_past_the_bound_parameter_limit() {
        let conn = migrated_conn();
        let uris: Vec<String> = (0..crate::store::MAX_BOUND_PARAMS + 10)
            .map(|i| format!("at://did:plc:o/app.bsky.feed.post/o{i}"))
            .collect();
        let refs: Vec<&str> = uris.iter().map(String::as_str).collect();
        clear_dirty(&conn, &refs).unwrap();
    }

    #[test]
    fn mark_dirty_chunks_a_list_past_the_bound_parameter_limit() {
        let conn = migrated_conn();
        let uris: Vec<String> = (0..crate::store::MAX_BOUND_PARAMS + 10)
            .map(|i| format!("at://did:plc:o/app.bsky.feed.post/o{i}"))
            .collect();
        let refs: Vec<&str> = uris.iter().map(String::as_str).collect();
        mark_dirty(&conn, &refs).unwrap();
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
