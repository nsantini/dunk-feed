//! Versioned schema, TECH-DESIGN section 6. Version 1 is the whole schema;
//! spec.md's Non-goals rule out a second version and a downgrade path, so
//! `migrate` only ever moves a fresh or version-1 database to version 1.
//! `schema_version` reads `meta.schema_version` without assuming the `meta`
//! table exists yet, because a brand-new database has no tables at all.

use rusqlite::Connection;

use crate::store::StoreError;

/// The schema version this binary understands. `migrate` brings a database
/// up to this version; a stored version above it fails open (BC16).
pub const CURRENT_VERSION: i64 = 1;

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
        v_likes_q INTEGER, v_reposts_q INTEGER, v_replies_q INTEGER,
        v_likes_o INTEGER, v_reposts_o INTEGER, v_replies_o INTEGER,
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
    match conn.query_row("SELECT value FROM meta WHERE key = 'schema_version'", [], |row| {
        row.get::<_, String>(0)
    }) {
        Ok(value) => value
            .parse::<i64>()
            .map(Some)
            .map_err(|_| StoreError::MalformedMeta { key: "schema_version".to_string(), value }),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(StoreError::Sqlite(err)),
    }
}

/// Brings `conn` to `CURRENT_VERSION`. A missing version runs the version 1
/// statements and the `schema_version` write inside one transaction (BC14,
/// BC20, BC21): a statement failing part way leaves nothing landed and the
/// version unmoved. A database already at `CURRENT_VERSION` is a no-op
/// (BC15). A stored version above `CURRENT_VERSION` fails without touching
/// the database (BC16).
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
    for statement in V1_STATEMENTS {
        tx.execute(statement, [])?;
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
        for table in
            ["pairs", "counts", "feed", "authors", "interactions", "meta", "sqlite_sequence"]
        {
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
        assert_eq!(schema_version(&conn).unwrap(), Some(1));
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
}
