//! `viewers`, `viewer_follows` and `viewer_checks` table access,
//! TECH-DESIGN-network-feed §8. Story 07 adds the `viewer_checks` reader and
//! writer this module lacked (`follows_cache` still has none; story 08 adds
//! it). Every function takes and returns plain types — the viewer DID as
//! `&str`, the circle state as a `&str`/`String`, `d2_sample` as a
//! JSON-backed `Vec<String>`, and follows/checked/follows_me as
//! `HashSet<u64>` `DidHash` values — rather than `crate::graph` types, so
//! this module has no dependency on `graph::Circle` or `graph::CircleState`
//! (slice 2.0's concern) and `graph::mod.rs` maps this module's rows onto
//! its own types instead.

use std::collections::{HashMap, HashSet};

use rusqlite::Connection;

use crate::store::StoreError;

/// One `viewers` row, joined with its `viewer_follows` and `viewer_checks`
/// hashes. `follows`, `checked` and `follows_me` are loaded as
/// `HashSet<u64>`: the same shape `graph::Circle` holds each of these in, so
/// `graph::mod.rs`'s restart load (slice 2.0, BC9) has no set to build
/// itself. `follows_me` is always a subset of `checked` (BC10).
#[derive(Debug, Clone, PartialEq)]
pub struct ViewerRow {
    pub viewer_did: String,
    pub first_seen_at: i64,
    pub last_request_at: i64,
    pub d1_refreshed_at: Option<i64>,
    pub state: String,
    pub d2_sample: Vec<String>,
    pub follows: HashSet<u64>,
    pub checked: HashSet<u64>,
    pub follows_me: HashSet<u64>,
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

/// Loads every `viewers` row with its `viewer_follows` and `viewer_checks`
/// hashes, for `graph::mod.rs`'s startup load (BC19, `restart_loads_circles`;
/// BC9, `restart_loads_checks`). Three queries rather than a join, so a
/// viewer with zero follows or zero checks still gets a row with empty sets
/// instead of being silently dropped by an inner join.
///
/// A row whose `d2_sample` fails to parse is skipped rather than failing the
/// whole load (review round 1, defect Y): this binary is the only writer, so
/// a malformed row can only be a partial write from an earlier crash, and
/// one bad row must not make `GraphHandle::from_store` lose every other
/// viewer's circle. One `warn` names the failing table and column, never the
/// viewer DID (BC21).
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

    // `checked` holds every hash the viewer has an entry for; `follows_me`
    // holds the subset whose `follows_me` flag is set (BC10).
    let mut checked_by_viewer: HashMap<String, HashSet<u64>> = HashMap::new();
    let mut follows_me_by_viewer: HashMap<String, HashSet<u64>> = HashMap::new();
    {
        let mut stmt =
            conn.prepare("SELECT viewer_did, author_hash, follows_me FROM viewer_checks")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let viewer_did: String = row.get(0)?;
            let author_hash: i64 = row.get(1)?;
            let follows_me: i64 = row.get(2)?;
            let hash = i64_as_hash(author_hash);
            checked_by_viewer.entry(viewer_did.clone()).or_default().insert(hash);
            if follows_me != 0 {
                follows_me_by_viewer.entry(viewer_did).or_default().insert(hash);
            }
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
        let d2_sample = match parse_d2_sample(&d2_sample) {
            Ok(d2_sample) => d2_sample,
            Err(err) => {
                tracing::warn!(?err, "store: skipping a malformed viewers row");
                continue;
            }
        };
        let follows = follows_by_viewer.remove(&viewer_did).unwrap_or_default();
        let checked = checked_by_viewer.remove(&viewer_did).unwrap_or_default();
        let follows_me = follows_me_by_viewer.remove(&viewer_did).unwrap_or_default();
        out.push(ViewerRow {
            viewer_did,
            first_seen_at,
            last_request_at,
            d1_refreshed_at,
            state,
            d2_sample,
            follows,
            checked,
            follows_me,
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

/// Saves step 2's result in one transaction (BC3, BC3a): the `viewers` row's
/// `state`, plus a full replace of `viewer_checks` for `viewer_did` — one row
/// per hash in `checked`, with `follows_me` set to 1 for a hash also in
/// `follows_me` and 0 otherwise (BC10). Only `UPDATE`s the `viewers` row
/// rather than upserting it, unlike `viewer_save_circle`: step 2 always runs
/// after step 1 has already inserted the row (`viewer_save_state` or
/// `viewer_save_circle`), so there is never a `viewer_did` this call needs to
/// create. A `viewer_did` with no `viewers` row updates zero rows, and then
/// fails on the `viewer_checks` insert with a foreign key `StoreError` —
/// `viewer_checks.viewer_did` references `viewers(viewer_did)` and
/// `foreign_keys` is on (review round 1, defect AG) — rather than the no-op
/// `viewer_touch` follows: `run_step2` (`graph/queue.rs`) routes that `Err`
/// to `handle_step2_failure` (BC4c) the same as any other save failure.
#[allow(dead_code)] // First caller is the worker (`graph/queue.rs`, slice 2.0).
pub fn viewer_save_checks(
    conn: &Connection,
    viewer_did: &str,
    state: &str,
    now: i64,
    checked: &HashSet<u64>,
    follows_me: &HashSet<u64>,
) -> Result<(), StoreError> {
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "UPDATE viewers SET state = ?2 WHERE viewer_did = ?1",
        rusqlite::params![viewer_did, state],
    )?;
    tx.execute("DELETE FROM viewer_checks WHERE viewer_did = ?1", [viewer_did])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO viewer_checks (viewer_did, author_hash, follows_me, checked_at)
             VALUES (?1, ?2, ?3, ?4)",
        )?;
        for hash in checked {
            let follows_me_flag = i64::from(follows_me.contains(hash));
            stmt.execute(rusqlite::params![viewer_did, hash_as_i64(*hash), follows_me_flag, now])?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Updates a `viewers` row's `state` only, and only if the row still exists
/// (review round 2, defect AI): unlike `viewer_save_state`, this is a plain
/// `UPDATE`, never an upsert, so a viewer deleted (e.g. `viewer_delete`, at
/// step 1's give-up) between the worker's last read of it and this call gets
/// no row recreated for it. `graph::queue::handle_step2_failure` calls this
/// at step 2's give-up (BC4b) instead of `viewer_save_state`, which would
/// otherwise insert a fresh `building_d1` row (its `ON CONFLICT` branch never
/// runs without a match) for a viewer the store no longer has one for. A
/// no-op, not an error, when `viewer_did` has no row.
#[allow(dead_code)] // First caller is the worker (`graph/queue.rs`, slice 5.0).
pub fn viewer_set_state_if_exists(
    conn: &Connection,
    viewer_did: &str,
    state: &str,
) -> Result<(), StoreError> {
    conn.execute(
        "UPDATE viewers SET state = ?2 WHERE viewer_did = ?1",
        rusqlite::params![viewer_did, state],
    )?;
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

    // BC10a: `viewer_delete` removes `viewer_checks` rows too, the same as
    // `viewer_follows`.
    #[test]
    fn viewer_delete_removes_viewer_checks_rows() {
        let conn = migrated_conn();
        viewer_save_circle(&conn, "did:plc:a", "building_fm", 1, 1, &[], &HashSet::new()).unwrap();
        let checked: HashSet<u64> = [10_u64, 20].into_iter().collect();
        let follows_me: HashSet<u64> = [10_u64].into_iter().collect();
        viewer_save_checks(&conn, "did:plc:a", "ready", 2, &checked, &follows_me).unwrap();

        viewer_delete(&conn, "did:plc:a").unwrap();

        let checks_left: i64 = conn
            .query_row(
                "SELECT count(*) FROM viewer_checks WHERE viewer_did = 'did:plc:a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(checks_left, 0);
    }

    // AC5: `viewer_save_checks` writes `state`, `checked` and `follows_me` in
    // one call, and `viewer_load_all` reads all three back — `follows_me` as
    // the subset of `checked` whose flag was set (BC10).
    #[test]
    fn checks_round_trip() {
        let conn = migrated_conn();
        viewer_save_circle(&conn, "did:plc:a", "building_fm", 1, 1, &[], &HashSet::new()).unwrap();
        let checked: HashSet<u64> = [10_u64, 20, 30].into_iter().collect();
        let follows_me: HashSet<u64> = [10_u64, 20].into_iter().collect();

        viewer_save_checks(&conn, "did:plc:a", "ready", 1_700_000_200, &checked, &follows_me)
            .unwrap();

        let rows = viewer_load_all(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, "ready");
        assert_eq!(rows[0].checked, checked);
        assert_eq!(rows[0].follows_me, follows_me);
    }

    // BC10: a second `viewer_save_checks` call fully replaces the prior
    // `viewer_checks` rows rather than merging with them.
    #[test]
    fn checks_round_trip_replaces_a_prior_save() {
        let conn = migrated_conn();
        viewer_save_circle(&conn, "did:plc:a", "building_fm", 1, 1, &[], &HashSet::new()).unwrap();
        let first: HashSet<u64> = [10_u64].into_iter().collect();
        viewer_save_checks(&conn, "did:plc:a", "ready", 1, &first, &first).unwrap();

        let second: HashSet<u64> = [20_u64, 30].into_iter().collect();
        let second_follows_me: HashSet<u64> = [30_u64].into_iter().collect();
        viewer_save_checks(&conn, "did:plc:a", "ready", 2, &second, &second_follows_me).unwrap();

        let rows = viewer_load_all(&conn).unwrap();
        assert_eq!(rows[0].checked, second);
        assert_eq!(rows[0].follows_me, second_follows_me);
    }

    // Review round 1, defect AG: a `viewer_did` with no `viewers` row fails
    // the `viewer_checks` insert with a foreign key `StoreError`, rather
    // than the no-op the doc used to promise.
    #[test]
    fn checks_save_with_no_viewer_row_fails() {
        let conn = migrated_conn();
        let checked: HashSet<u64> = [10_u64].into_iter().collect();

        let result =
            viewer_save_checks(&conn, "did:plc:none", "ready", 1, &checked, &HashSet::new());

        assert!(result.is_err());
    }

    // Review round 2, defect AI: `viewer_set_state_if_exists` creates no row
    // for a viewer the store has none for, unlike `viewer_save_state`'s
    // upsert.
    #[test]
    fn set_state_if_exists_of_an_unknown_viewer_creates_no_row() {
        let conn = migrated_conn();
        viewer_set_state_if_exists(&conn, "did:plc:missing", "ready").unwrap();
        assert_eq!(viewer_load_all(&conn).unwrap(), Vec::new());
    }

    // The existing-row case: the state changes, nothing else does.
    #[test]
    fn set_state_if_exists_of_a_known_viewer_updates_state_only() {
        let conn = migrated_conn();
        viewer_save_state(&conn, "did:plc:a", "building_fm", 1_700_000_000).unwrap();

        viewer_set_state_if_exists(&conn, "did:plc:a", "ready").unwrap();

        let rows = viewer_load_all(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, "ready");
        assert_eq!(rows[0].first_seen_at, 1_700_000_000);
    }

    // A hash near `u64::MAX` round-trips through the `i64` reinterpret cast
    // without truncation or an error.
    #[test]
    fn hash_round_trip_covers_the_full_u64_range() {
        for hash in [0_u64, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            assert_eq!(i64_as_hash(hash_as_i64(hash)), hash);
        }
    }

    // Review round 1, defect Y: a malformed `d2_sample` must not fail the
    // whole load — it is skipped, and every other row still comes back.
    #[test]
    fn viewer_load_all_skips_a_malformed_row_and_keeps_the_rest() {
        let conn = migrated_conn();
        viewer_save_state(&conn, "did:plc:good", "building_d1", 1_700_000_000).unwrap();
        conn.execute(
            "INSERT INTO viewers (viewer_did, first_seen_at, last_request_at, state, d2_sample)
             VALUES ('did:plc:bad', 1, 1, 'ready', 'not-json')",
            [],
        )
        .unwrap();

        let rows = viewer_load_all(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].viewer_did, "did:plc:good");
    }
}
