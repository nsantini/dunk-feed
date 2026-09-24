//! Versioned schema, TECH-DESIGN section 6 (version 1) and
//! TECH-DESIGN-network-feed §8 (version 2). `schema_version` reads
//! `meta.schema_version` without assuming the `meta` table exists yet,
//! because a brand-new database has no tables at all. `migrate` moves a
//! fresh, version-1, or version-2 database up to `CURRENT_VERSION`; a
//! version 2 migration only adds tables, so a version-1 database's rows are
//! never touched (spec.md's Outcome: "The migration to schema version 2
//! runs with either flag value and only adds tables").

use rusqlite::Connection;

use crate::store::StoreError;

/// The schema version this binary understands. `migrate` brings a database
/// up to this version; a stored version above it fails open (BC3a).
pub const CURRENT_VERSION: i64 = 2;

/// Version 1's statements, verbatim from TECH-DESIGN section 6. Order
/// matters: `feed` references `pairs`, so `pairs` is created first.
const V1_STATEMENTS: &[&str] = &[
    "CREATE TABLE pairs (
        quote_uri     TEXT PRIMARY KEY,
        quote_did     TEXT NOT NULL,
        quote_cid     TEXT NOT NULL,
        original_uri  TEXT NOT NULL,
        original_did  TEXT NOT NULL,
        quoted_at     INTEGER NOT NULL,
        first_seen_at INTEGER NOT NULL,
        state         TEXT NOT NULL DEFAULT 'candidate',
        drop_reason   TEXT
    )",
    "CREATE INDEX pairs_original ON pairs(original_uri)",
    "CREATE INDEX pairs_state_seen ON pairs(state, first_seen_at)",
    "CREATE TABLE counts (
        post_uri      TEXT PRIMARY KEY,
        likes INTEGER NOT NULL DEFAULT 0,
        reposts INTEGER NOT NULL DEFAULT 0,
        replies INTEGER NOT NULL DEFAULT 0,
        last_event_at INTEGER NOT NULL,
        dirty         INTEGER NOT NULL DEFAULT 1
    )",
    "CREATE INDEX counts_dirty ON counts(dirty) WHERE dirty = 1",
    "CREATE TABLE feed (
        quote_uri     TEXT PRIMARY KEY REFERENCES pairs(quote_uri),
        quote_cid     TEXT NOT NULL,
        quote_did     TEXT NOT NULL,
        original_did  TEXT NOT NULL,
        quoted_at     INTEGER NOT NULL,
        v_likes_q INTEGER NOT NULL, v_reposts_q INTEGER NOT NULL, v_replies_q INTEGER NOT NULL,
        v_likes_o INTEGER NOT NULL, v_reposts_o INTEGER NOT NULL, v_replies_o INTEGER NOT NULL,
        ratio         REAL NOT NULL,
        rank          REAL NOT NULL,
        promoted_at   INTEGER NOT NULL,
        verified_at   INTEGER NOT NULL
    )",
    "CREATE INDEX feed_rank ON feed(rank DESC)",
    "CREATE TABLE authors (
        did           TEXT PRIMARY KEY,
        followers     INTEGER,
        active        INTEGER NOT NULL,
        labels        TEXT,
        checked_at    INTEGER NOT NULL
    )",
    "CREATE TABLE interactions (
        received_at INTEGER NOT NULL, item TEXT, event TEXT, feed_context TEXT, req_id TEXT
    )",
    "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
];

/// Version 2's statements, TECH-DESIGN-network-feed §8. Story 06's
/// Non-goals: `viewer_checks` and `follows_cache` are created empty in this
/// story, with no reader or writer yet; `store/viewers.rs` reads and writes
/// only `viewers` and `viewer_follows`. Hashes (`subject_hash`,
/// `author_hash`) are `xxh3_64` `DidHash` values reinterpreted as `i64` for
/// SQLite's signed `INTEGER` storage (`as i64` between same-width integers
/// is a lossless bit-for-bit cast, reversed on read with `as u64`), so a
/// viewer DID never has to sit in `viewer_follows` or `viewer_checks`
/// (design §8: "Hashes are stored, not DIDs").
const V2_STATEMENTS: &[&str] = &[
    "CREATE TABLE viewers (
        viewer_did       TEXT PRIMARY KEY,
        first_seen_at    INTEGER NOT NULL,
        last_request_at  INTEGER NOT NULL,
        d1_refreshed_at  INTEGER,
        state            TEXT NOT NULL,
        d2_sample        TEXT NOT NULL DEFAULT '[]'
    )",
    "CREATE TABLE viewer_follows (
        viewer_did    TEXT NOT NULL REFERENCES viewers(viewer_did),
        subject_hash  INTEGER NOT NULL,
        PRIMARY KEY (viewer_did, subject_hash)
    )",
    "CREATE TABLE viewer_checks (
        viewer_did   TEXT NOT NULL REFERENCES viewers(viewer_did),
        author_hash  INTEGER NOT NULL,
        follows_me   INTEGER NOT NULL,
        checked_at   INTEGER NOT NULL,
        PRIMARY KEY (viewer_did, author_hash)
    )",
    "CREATE TABLE follows_cache (
        account_did  TEXT PRIMARY KEY,
        fetched_at   INTEGER NOT NULL,
        follows      BLOB NOT NULL
    )",
];

/// Reads `meta.schema_version`. `Ok(None)` both when the `meta` table does
/// not exist yet (a fresh database) and when it exists but has no such row.
/// A stored value that does not parse as an integer is `MalformedMeta`
/// rather than treated as missing (BC17): a corrupt version row must never
/// silently trigger a re-migration.
pub fn schema_version(conn: &Connection) -> Result<Option<i64>, StoreError> {
    let meta_table_exists: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'meta'",
        [],
        |row| row.get(0),
    )?;
    if meta_table_exists == 0 {
        return Ok(None);
    }
    let value = crate::store::optional(conn.query_row(
        "SELECT value FROM meta WHERE key = 'schema_version'",
        [],
        |row| row.get::<_, String>(0),
    ))?;
    match value {
        None => Ok(None),
        Some(value) => value
            .parse::<i64>()
            .map(Some)
            .map_err(|_| StoreError::MalformedMeta { key: "schema_version".to_string(), value }),
    }
}

/// Brings `conn` to `CURRENT_VERSION`. Every missing version's statements
/// and the `schema_version` write run inside one transaction (BC1, BC2,
/// BC3): a statement failing part way leaves nothing from this call landed,
/// and the version stays at what it was before the call — for a
/// version-1 database whose version-2 statements fail, version 1's tables
/// and rows are untouched, because they were committed by an earlier call.
/// A database already at `CURRENT_VERSION` is a no-op. A stored version
/// above `CURRENT_VERSION` fails without touching the database (BC3a).
pub fn migrate(conn: &Connection) -> Result<(), StoreError> {
    let found = schema_version(conn)?.unwrap_or(0);
    if found > CURRENT_VERSION {
        return Err(StoreError::SchemaTooNew {
            found: found as u64,
            supported: CURRENT_VERSION as u64,
        });
    }
    if found == CURRENT_VERSION {
        return Ok(());
    }

    let tx = conn.unchecked_transaction()?;
    if found < 1 {
        for statement in V1_STATEMENTS {
            tx.execute(statement, [])?;
        }
    }
    if found < 2 {
        for statement in V2_STATEMENTS {
            tx.execute(statement, [])?;
        }
    }
    tx.execute(
        "INSERT INTO meta (key, value) VALUES ('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [CURRENT_VERSION.to_string()],
    )?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn columns(conn: &Connection, table: &str) -> HashSet<String> {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})")).unwrap();
        stmt.query_map([], |row| row.get::<_, String>(1)).unwrap().map(Result::unwrap).collect()
    }

    fn index_names(conn: &Connection, table: &str) -> HashSet<String> {
        let mut stmt = conn.prepare(&format!("PRAGMA index_list({table})")).unwrap();
        stmt.query_map([], |row| row.get::<_, String>(1)).unwrap().map(Result::unwrap).collect()
    }

    #[test]
    fn matches_design_schema() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();

        let tables: HashSet<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        for table in [
            "pairs",
            "counts",
            "feed",
            "authors",
            "interactions",
            "meta",
            "viewers",
            "viewer_follows",
            "viewer_checks",
            "follows_cache",
            "sqlite_sequence",
        ] {
            if table == "sqlite_sequence" {
                continue;
            }
            assert!(tables.contains(table), "missing table {table}");
        }

        assert_eq!(
            columns(&conn, "pairs"),
            [
                "quote_uri",
                "quote_did",
                "quote_cid",
                "original_uri",
                "original_did",
                "quoted_at",
                "first_seen_at",
                "state",
                "drop_reason"
            ]
            .into_iter()
            .map(String::from)
            .collect()
        );
        assert!(index_names(&conn, "pairs").contains("pairs_original"));
        assert!(index_names(&conn, "pairs").contains("pairs_state_seen"));

        assert_eq!(
            columns(&conn, "counts"),
            ["post_uri", "likes", "reposts", "replies", "last_event_at", "dirty"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        assert!(index_names(&conn, "counts").contains("counts_dirty"));

        assert_eq!(
            columns(&conn, "feed"),
            [
                "quote_uri",
                "quote_cid",
                "quote_did",
                "original_did",
                "quoted_at",
                "v_likes_q",
                "v_reposts_q",
                "v_replies_q",
                "v_likes_o",
                "v_reposts_o",
                "v_replies_o",
                "ratio",
                "rank",
                "promoted_at",
                "verified_at"
            ]
            .into_iter()
            .map(String::from)
            .collect()
        );
        assert!(index_names(&conn, "feed").contains("feed_rank"));

        assert_eq!(
            columns(&conn, "authors"),
            ["did", "followers", "active", "labels", "checked_at"]
                .into_iter()
                .map(String::from)
                .collect()
        );

        assert_eq!(
            columns(&conn, "interactions"),
            ["received_at", "item", "event", "feed_context", "req_id"]
                .into_iter()
                .map(String::from)
                .collect()
        );

        assert_eq!(
            columns(&conn, "meta"),
            ["key", "value"].into_iter().map(String::from).collect()
        );

        assert_eq!(
            columns(&conn, "viewers"),
            [
                "viewer_did",
                "first_seen_at",
                "last_request_at",
                "d1_refreshed_at",
                "state",
                "d2_sample"
            ]
            .into_iter()
            .map(String::from)
            .collect()
        );

        assert_eq!(
            columns(&conn, "viewer_follows"),
            ["viewer_did", "subject_hash"].into_iter().map(String::from).collect()
        );

        assert_eq!(
            columns(&conn, "viewer_checks"),
            ["viewer_did", "author_hash", "follows_me", "checked_at"]
                .into_iter()
                .map(String::from)
                .collect()
        );

        assert_eq!(
            columns(&conn, "follows_cache"),
            ["account_did", "fetched_at", "follows"].into_iter().map(String::from).collect()
        );

        assert_eq!(schema_version(&conn).unwrap(), Some(CURRENT_VERSION));
    }

    #[test]
    fn missing_schema_version_is_none_on_a_fresh_database() {
        let conn = Connection::open_in_memory().unwrap();
        assert_eq!(schema_version(&conn).unwrap(), None);
    }

    #[test]
    fn migrate_twice_is_a_noop() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        migrate(&conn).unwrap();
        assert_eq!(schema_version(&conn).unwrap(), Some(CURRENT_VERSION));
    }

    #[test]
    fn non_numeric_schema_version_is_malformed_meta() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        conn.execute("UPDATE meta SET value = 'not-a-number' WHERE key = 'schema_version'", [])
            .unwrap();
        let err = schema_version(&conn).unwrap_err();
        match err {
            StoreError::MalformedMeta { key, value } => {
                assert_eq!(key, "schema_version");
                assert_eq!(value, "not-a-number");
            }
            other => panic!("expected MalformedMeta, got {other:?}"),
        }
    }

    #[test]
    fn a_statement_failing_part_way_rolls_back_the_whole_version() {
        let conn = Connection::open_in_memory().unwrap();
        // Pre-create `counts` with an incompatible shape so the second
        // statement in `V1_STATEMENTS` fails. The first statement
        // (`CREATE TABLE pairs`) must then not have landed either (BC21).
        conn.execute("CREATE TABLE counts (only_column TEXT)", []).unwrap();
        assert!(migrate(&conn).is_err());
        let pairs_exists: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'pairs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pairs_exists, 0, "pairs must not exist after a rolled-back migration");
        assert_eq!(schema_version(&conn).unwrap(), None);
    }

    // BC1: migrating a version-1 database adds the four version-2 tables in
    // one transaction. `meta.schema_version` becomes 2. Version 1's tables
    // and rows are unchanged.
    #[test]
    fn v2_migration_from_v1_adds_the_four_tables_and_keeps_v1_rows() {
        let conn = Connection::open_in_memory().unwrap();
        // Migrate to version 1 only, by running just the version 1
        // statements and the version write `migrate` itself would have run
        // before version 2 existed.
        let tx = conn.unchecked_transaction().unwrap();
        for statement in V1_STATEMENTS {
            tx.execute(statement, []).unwrap();
        }
        tx.execute("INSERT INTO meta (key, value) VALUES ('schema_version', '1')", []).unwrap();
        tx.commit().unwrap();
        conn.execute(
            "INSERT INTO pairs (quote_uri, quote_did, quote_cid, original_uri, original_did, quoted_at, first_seen_at)
             VALUES ('at://q/1', 'did:plc:q', 'cid1', 'at://o/1', 'did:plc:o', 1, 1)",
            [],
        )
        .unwrap();

        migrate(&conn).unwrap();

        assert_eq!(schema_version(&conn).unwrap(), Some(2));
        for table in ["viewers", "viewer_follows", "viewer_checks", "follows_cache"] {
            let exists: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 1, "missing table {table}");
        }
        let pairs_row_count: i64 =
            conn.query_row("SELECT count(*) FROM pairs", [], |row| row.get(0)).unwrap();
        assert_eq!(pairs_row_count, 1, "version 1 rows must survive the version 2 migration");
    }

    // BC2: a fresh database gets version 1 and version 2 tables in one
    // `migrate` call, ending at version 2.
    #[test]
    fn v2_migration_from_fresh_creates_version_1_and_version_2_tables() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        for table in ["pairs", "viewers", "viewer_follows", "viewer_checks", "follows_cache"] {
            let exists: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 1, "missing table {table}");
        }
        assert_eq!(schema_version(&conn).unwrap(), Some(2));
    }

    // BC3: a version 2 statement failing part way through a migration from
    // version 1 leaves nothing from that call landed. The version stays at
    // 1, the version this call started from, not `None`.
    #[test]
    fn v2_statement_failing_part_way_leaves_the_version_at_one() {
        let conn = Connection::open_in_memory().unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for statement in V1_STATEMENTS {
            tx.execute(statement, []).unwrap();
        }
        tx.execute("INSERT INTO meta (key, value) VALUES ('schema_version', '1')", []).unwrap();
        tx.commit().unwrap();
        // Pre-create `viewer_checks` with an incompatible shape so the third
        // statement in `V2_STATEMENTS` fails. `viewers` and `viewer_follows`
        // (the first two) must then not have landed either.
        conn.execute("CREATE TABLE viewer_checks (only_column TEXT)", []).unwrap();

        assert!(migrate(&conn).is_err());
        let viewers_exists: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'viewers'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(viewers_exists, 0, "viewers must not exist after a rolled-back migration");
        assert_eq!(schema_version(&conn).unwrap(), Some(1));
    }

    // BC3a: a stored version above `CURRENT_VERSION` fails open, as it did
    // when `CURRENT_VERSION` was 1.
    #[test]
    fn v2_rejects_a_stored_version_above_current() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        conn.execute("UPDATE meta SET value = '999' WHERE key = 'schema_version'", []).unwrap();

        let err = migrate(&conn).unwrap_err();
        match err {
            StoreError::SchemaTooNew { found, supported } => {
                assert_eq!(found, 999);
                assert_eq!(supported, CURRENT_VERSION as u64);
            }
            other => panic!("expected SchemaTooNew, got {other:?}"),
        }
    }
}
