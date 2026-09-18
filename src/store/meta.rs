//! `meta` table access, TECH-DESIGN section 6. Keys: `schema_version`
//! (owned by `schema.rs`), `jetstream_seq`, `zstd_dict_id` and
//! `last_scorer_pass`. Every value is stored as text; `cursor` is the only
//! reader that parses one.

use rusqlite::Connection;

use crate::store::StoreError;

/// Reads one key. `Ok(None)` when the key has no row (BC67); `meta_set`
/// inserts or replaces, so there is never more than one row per key.
pub fn meta_get(conn: &Connection, key: &str) -> Result<Option<String>, StoreError> {
    crate::store::optional(
        conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| row.get(0)),
    )
}

/// Inserts or replaces one key.
pub fn meta_set(conn: &Connection, key: &str, value: &str) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

/// Reads `meta.jetstream_seq`. `Ok(None)` on a fresh database with no
/// cursor yet (BC43); the caller connects at the head. A stored value that
/// does not parse as `u64` is `MalformedMeta` (BC44), never a silent
/// restart from the head.
pub fn cursor(conn: &Connection) -> Result<Option<u64>, StoreError> {
    match meta_get(conn, "jetstream_seq")? {
        None => Ok(None),
        Some(value) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| StoreError::MalformedMeta { key: "jetstream_seq".to_string(), value }),
    }
}

/// Writes `meta.jetstream_seq`. The writer (`writer.rs`, slice 2.0) is the
/// only caller past this slice's tests.
pub fn set_cursor(conn: &Connection, seq: u64) -> Result<(), StoreError> {
    meta_set(conn, "jetstream_seq", &seq.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::migrated_conn;

    #[test]
    fn meta_get_missing_key_is_none() {
        let conn = migrated_conn();
        assert_eq!(meta_get(&conn, "zstd_dict_id").unwrap(), None);
    }

    #[test]
    fn meta_set_then_get_round_trips() {
        let conn = migrated_conn();
        meta_set(&conn, "zstd_dict_id", "abc123").unwrap();
        assert_eq!(meta_get(&conn, "zstd_dict_id").unwrap(), Some("abc123".to_string()));
    }

    #[test]
    fn meta_set_twice_replaces_the_value() {
        let conn = migrated_conn();
        meta_set(&conn, "last_scorer_pass", "1").unwrap();
        meta_set(&conn, "last_scorer_pass", "2").unwrap();
        assert_eq!(meta_get(&conn, "last_scorer_pass").unwrap(), Some("2".to_string()));
    }

    #[test]
    fn cursor_missing_is_none() {
        let conn = migrated_conn();
        assert_eq!(cursor(&conn).unwrap(), None);
    }

    #[test]
    fn set_cursor_then_cursor_round_trips() {
        let conn = migrated_conn();
        set_cursor(&conn, 42).unwrap();
        assert_eq!(cursor(&conn).unwrap(), Some(42));
    }

    #[test]
    fn non_numeric_cursor_is_malformed_meta() {
        let conn = migrated_conn();
        meta_set(&conn, "jetstream_seq", "not-a-number").unwrap();
        let err = cursor(&conn).unwrap_err();
        match err {
            StoreError::MalformedMeta { key, value } => {
                assert_eq!(key, "jetstream_seq");
                assert_eq!(value, "not-a-number");
            }
            other => panic!("expected MalformedMeta, got {other:?}"),
        }
    }
}
