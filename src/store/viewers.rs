//! `viewers` and `viewer_follows` table access, TECH-DESIGN-network-feed
//! §8. Story 06's Non-goals leave `viewer_checks` and `follows_cache`
//! without a reader or writer here; stories 07 and 08 add them. Every
//! function takes and returns plain types — the viewer DID as `&str`, the
//! circle state as a `&str`/`String`, `d2_sample` as a JSON-backed
//! `Vec<String>`, and follows as `HashSet<u64>` `DidHash` values — rather
//! than `crate::graph` types, so this module has no dependency on
//! `graph::Circle` or `graph::CircleState` (slice 2.0's concern) and
//! `graph::mod.rs` maps this module's rows onto its own types instead.

use std::collections::{HashMap, HashSet};

use rusqlite::Connection;

use crate::store::StoreError;

/// One `viewers` row, joined with its `viewer_follows` hashes. `follows` is
/// loaded as a `HashSet<u64>`: the same shape `graph::Circle::follows`
/// holds, so `graph::mod.rs`'s restart load (slice 2.0) has no set to build
/// itself.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewerRow {
    pub viewer_did: String,
    pub first_seen_at: i64,
    pub last_request_at: i64,
    pub d1_refreshed_at: Option<i64>,
    pub state: String,
    pub d2_sample: Vec<String>,
    pub follows: HashSet<u64>,
}

/// Reinterprets a `DidHash` (`u64`) as SQLite's signed 64-bit `INTEGER`
/// storage class. `as` between two integer types of the same width is a
/// lossless, reversible bit-for-bit cast (never a truncation or a
/// saturation), so `hash_as_i64` composed with `i64_as_hash` is the
/// identity for every `u64` value; design §8: "Hashes are stored, not
/// DIDs."
fn hash_as_i64(hash: u64) -> i64 {
    hash as i64
}

/// The inverse of `hash_as_i64`.
fn i64_as_hash(value: i64) -> u64 {
    value as u64
}

/// Serialises `d2_sample` to a JSON array of DID strings, the encoding
/// `viewer_load_all` parses back.
fn encode_d2_sample(d2_sample: &[String]) -> Result<String, StoreError> {
    serde_json::to_string(d2_sample)
        .map_err(|_| StoreError::MalformedRow { table: "viewers", column: "d2_sample" })
}

/// Parses a stored `d2_sample` value as a JSON array of DID strings.
/// `StoreError::MalformedRow` on anything else, the same rule
/// `authors::parse_labels` applies to `authors.labels`.
fn parse_d2_sample(text: &str) -> Result<Vec<String>, StoreError> {
    serde_json::from_str::<Vec<String>>(text)
        .map_err(|_| StoreError::MalformedRow { table: "viewers", column: "d2_sample" })
}

/// Loads every `viewers` row with its `viewer_follows` hashes, for
/// `graph::mod.rs`'s startup load (BC19, `restart_loads_circles`). Two
/// queries rather than a join, so a viewer with zero follows still gets a
/// row with an empty `follows` set instead of being silently dropped by an
/// inner join.
pub fn viewer_load_all(conn: &Connection) -> Result<Vec<ViewerRow>, StoreError> {
    let mut follows_by_viewer: HashMap<String, HashSet<u64>> = HashMap::new();
    {
        let mut stmt = conn.prepare("SELECT viewer_did, subject_hash FROM viewer_follows")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let viewer_did: String = row.get(0)?;
            let subject_hash: i64 = row.get(1)?;
            follows_by_viewer.entry(viewer_did).or_default().insert(i64_as_hash(subject_hash));
        }
    }

    let mut stmt = conn.prepare(
        "SELECT viewer_did, first_seen_at, last_request_at, d1_refreshed_at, state, d2_sample
         FROM viewers",
    )?;
    let mapped = stmt.query_map([], |row| {
        let viewer_did: String = row.get(0)?;
        let first_seen_at: i64 = row.get(1)?;
        let last_request_at: i64 = row.get(2)?;
        let d1_refreshed_at: Option<i64> = row.get(3)?;
        let state: String = row.get(4)?;
        let d2_sample: String = row.get(5)?;
        Ok((viewer_did, first_seen_at, last_request_at, d1_refreshed_at, state, d2_sample))
    })?;

    let mut out = Vec::new();
    for row in mapped {
        let (viewer_did, first_seen_at, last_request_at, d1_refreshed_at, state, d2_sample) = row?;
        let d2_sample = parse_d2_sample(&d2_sample)?;
        let follows = follows_by_viewer.remove(&viewer_did).unwrap_or_default();
        out.push(ViewerRow {
            viewer_did,
            first_seen_at,
            last_request_at,
            d1_refreshed_at,
            state,
            d2_sample,
            follows,
        });
    }
    Ok(out)
}

/// Inserts or updates a `viewers` row's `state`, leaving `d1_refreshed_at`
/// and `d2_sample` alone. On first insert, `first_seen_at` and
/// `last_request_at` are both `now` (BC7a): the worker calls this to write
/// `building_d1` before its first `PdsClient` call, so a restart mid-build
/// finds the row and re-enqueues the job (BC19) rather than losing it.
#[allow(dead_code)] // First caller is the worker (`graph/queue.rs`, slice 2.0).
pub fn viewer_save_state(
    conn: &Connection,
    viewer_did: &str,
    state: &str,
    now: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO viewers (viewer_did, first_seen_at, last_request_at, d1_refreshed_at, state, d2_sample)
         VALUES (?1, ?2, ?2, NULL, ?3, '[]')
         ON CONFLICT(viewer_did) DO UPDATE SET state = excluded.state",
        rusqlite::params![viewer_did, now, state],
    )?;
    Ok(())
}

/// Saves a completed circle build in one transaction (BC7): the `viewers`
/// row's `state`, `d1_refreshed_at` and `d2_sample`, plus a full replace of
/// `viewer_follows` for `viewer_did`. Upserts the `viewers` row rather than
/// requiring `viewer_save_state` to have run first, so a caller (a test, or
/// a future refresh path) can save a circle standalone.
#[allow(dead_code)] // First caller is the worker (`graph/queue.rs`, slice 2.0).
pub fn viewer_save_circle(
    conn: &Connection,
    viewer_did: &str,
    state: &str,
    now: i64,
    d1_refreshed_at: i64,
    d2_sample: &[String],
    follows: &HashSet<u64>,
) -> Result<(), StoreError> {
    let d2_sample_json = encode_d2_sample(d2_sample)?;
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "INSERT INTO viewers (viewer_did, first_seen_at, last_request_at, d1_refreshed_at, state, d2_sample)
         VALUES (?1, ?2, ?2, ?3, ?4, ?5)
         ON CONFLICT(viewer_did) DO UPDATE SET
            d1_refreshed_at = excluded.d1_refreshed_at,
            state = excluded.state,
            d2_sample = excluded.d2_sample",
        rusqlite::params![viewer_did, now, d1_refreshed_at, state, d2_sample_json],
    )?;
    tx.execute("DELETE FROM viewer_follows WHERE viewer_did = ?1", [viewer_did])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO viewer_follows (viewer_did, subject_hash) VALUES (?1, ?2)",
        )?;
        for hash in follows {
            stmt.execute(rusqlite::params![viewer_did, hash_as_i64(*hash)])?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Updates `last_request_at` only, for the worker's touch flush (BC23): at
/// most once every 60 s for each viewer, written by the worker task rather
/// than the request path. A no-op, not an error, when `viewer_did` has no
/// row — the flush runs off an in-memory snapshot that can race a
/// concurrent eviction.
#[allow(dead_code)] // First caller is the worker's touch flush (`graph/queue.rs`, slice 2.0).
pub fn viewer_touch(
    conn: &Connection,
    viewer_did: &str,
    last_request_at: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "UPDATE viewers SET last_request_at = ?2 WHERE viewer_did = ?1",
        rusqlite::params![viewer_did, last_request_at],
    )?;
    Ok(())
}

/// Deletes a viewer's `viewers`, `viewer_follows` and `viewer_checks` rows
/// in one transaction. Spec.md's Non-goals: no caller exists yet in this
/// story — refresh and eviction (story 09) are the first — so this is
/// exercised only by this module's own round-trip tests.
#[allow(dead_code)] // No caller until story 09's eviction.
pub fn viewer_delete(conn: &Connection, viewer_did: &str) -> Result<(), StoreError> {
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM viewer_checks WHERE viewer_did = ?1", [viewer_did])?;
    tx.execute("DELETE FROM viewer_follows WHERE viewer_did = ?1", [viewer_did])?;
    tx.execute("DELETE FROM viewers WHERE viewer_did = ?1", [viewer_did])?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::migrated_conn;

    #[test]
    fn viewer_load_all_of_an_empty_database_is_empty() {
        let conn = migrated_conn();
        assert_eq!(viewer_load_all(&conn).unwrap(), Vec::new());
    }

    #[test]
    fn viewer_save_state_then_load_all_creates_a_row_with_no_follows() {
        let conn = migrated_conn();
        viewer_save_state(&conn, "did:plc:a", "building_d1", 1_700_000_000).unwrap();

        let rows = viewer_load_all(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].viewer_did, "did:plc:a");
        assert_eq!(rows[0].first_seen_at, 1_700_000_000);
        assert_eq!(rows[0].last_request_at, 1_700_000_000);
        assert_eq!(rows[0].d1_refreshed_at, None);
        assert_eq!(rows[0].state, "building_d1");
        assert_eq!(rows[0].d2_sample, Vec::<String>::new());
        assert_eq!(rows[0].follows, HashSet::new());
    }

    // BC7a: a second `viewer_save_state` call updates `state` in place and
    // does not reset `first_seen_at` or `last_request_at` back to `now`.
    #[test]
    fn viewer_save_state_twice_keeps_the_first_seen_at() {
        let conn = migrated_conn();
        viewer_save_state(&conn, "did:plc:a", "building_d1", 1_700_000_000).unwrap();
        viewer_save_state(&conn, "did:plc:a", "building_d1", 1_700_000_500).unwrap();

        let rows = viewer_load_all(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].first_seen_at, 1_700_000_000);
        assert_eq!(rows[0].last_request_at, 1_700_000_000);
        assert_eq!(rows[0].state, "building_d1");
    }

    // BC7, AC2: `viewer_save_circle` writes the `viewers` row and every
    // `viewer_follows` hash in one call, and `viewer_load_all` reads both
    // back.
    #[test]
    fn viewer_save_circle_then_load_all_round_trips() {
        let conn = migrated_conn();
        let follows: HashSet<u64> = [1_u64, 2, u64::MAX].into_iter().collect();
        let d2_sample = vec!["did:plc:x".to_string(), "did:plc:y".to_string()];
        viewer_save_circle(
            &conn,
            "did:plc:a",
            "ready",
            1_700_000_100,
            1_700_000_100,
            &d2_sample,
            &follows,
        )
        .unwrap();

        let rows = viewer_load_all(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].viewer_did, "did:plc:a");
        assert_eq!(rows[0].state, "ready");
        assert_eq!(rows[0].d1_refreshed_at, Some(1_700_000_100));
        assert_eq!(rows[0].d2_sample, d2_sample);
        assert_eq!(rows[0].follows, follows);
    }

    // BC7: saving a circle over an existing `building_d1` row moves it to
    // `ready` and replaces the follows set in full, rather than merging it
    // with whatever `viewer_save_state` wrote at job start.
    #[test]
    fn viewer_save_circle_replaces_a_prior_building_d1_row_and_follows() {
        let conn = migrated_conn();
        viewer_save_state(&conn, "did:plc:a", "building_d1", 1_700_000_000).unwrap();

        let first: HashSet<u64> = [1_u64].into_iter().collect();
        viewer_save_circle(&conn, "did:plc:a", "ready", 1_700_000_050, 1_700_000_050, &[], &first)
            .unwrap();
        let second: HashSet<u64> = [2_u64, 3].into_iter().collect();
        viewer_save_circle(&conn, "did:plc:a", "ready", 1_700_000_100, 1_700_000_100, &[], &second)
            .unwrap();

        let rows = viewer_load_all(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, "ready");
        assert_eq!(rows[0].d1_refreshed_at, Some(1_700_000_100));
        assert_eq!(rows[0].follows, second);
    }

    #[test]
    fn viewer_touch_updates_last_request_at_only() {
        let conn = migrated_conn();
        viewer_save_state(&conn, "did:plc:a", "ready", 1_700_000_000).unwrap();

        viewer_touch(&conn, "did:plc:a", 1_700_000_999).unwrap();

        let rows = viewer_load_all(&conn).unwrap();
        assert_eq!(rows[0].last_request_at, 1_700_000_999);
        assert_eq!(rows[0].state, "ready");
    }

    #[test]
    fn viewer_touch_of_an_unknown_viewer_is_not_an_error() {
        let conn = migrated_conn();
        viewer_touch(&conn, "did:plc:missing", 1_700_000_000).unwrap();
        assert_eq!(viewer_load_all(&conn).unwrap(), Vec::new());
    }

    #[test]
    fn viewer_delete_removes_the_viewer_and_its_follows() {
        let conn = migrated_conn();
        let follows: HashSet<u64> = [1_u64, 2].into_iter().collect();
        viewer_save_circle(&conn, "did:plc:a", "ready", 1, 1, &[], &follows).unwrap();

        viewer_delete(&conn, "did:plc:a").unwrap();

        assert_eq!(viewer_load_all(&conn).unwrap(), Vec::new());
        let follows_left: i64 = conn
            .query_row(
                "SELECT count(*) FROM viewer_follows WHERE viewer_did = 'did:plc:a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(follows_left, 0);
    }

    // A hash near `u64::MAX` round-trips through the `i64` reinterpret cast
    // without truncation or an error.
    #[test]
    fn hash_round_trip_covers_the_full_u64_range() {
        for hash in [0_u64, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            assert_eq!(i64_as_hash(hash_as_i64(hash)), hash);
        }
    }

    #[test]
    fn malformed_d2_sample_is_a_malformed_row_error() {
        let conn = migrated_conn();
        conn.execute(
            "INSERT INTO viewers (viewer_did, first_seen_at, last_request_at, state, d2_sample)
             VALUES ('did:plc:a', 1, 1, 'ready', 'not-json')",
            [],
        )
        .unwrap();

        let err = viewer_load_all(&conn).unwrap_err();
        match err {
            StoreError::MalformedRow { table, column } => {
                assert_eq!(table, "viewers");
                assert_eq!(column, "d2_sample");
            }
            other => panic!("expected MalformedRow, got {other:?}"),
        }
    }
}
