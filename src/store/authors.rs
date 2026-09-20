//! `authors` table row operations, TECH-DESIGN section 6. `labels` is a
//! JSON array of strings on disk (spec.md's Non-goals rule out a
//! `serde_json::Value` column): serialised on write, parsed on read.

use rusqlite::Connection;

use crate::store::StoreError;

/// One row of the `authors` table. `active` is `false` for a deactivated,
/// deleted, or taken-down account.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthorRow {
    pub did: String,
    pub followers: Option<i64>,
    pub active: bool,
    pub labels: Option<Vec<String>>,
    pub checked_at: i64,
}

/// Reads the row for `did`. `Ok(None)` when there is none (BC64). A stored
/// `labels` value that does not parse as a JSON array of strings is
/// `StoreError::MalformedRow` (BC66): the row is never returned with a
/// silently empty label list.
pub fn author_get(conn: &Connection, did: &str) -> Result<Option<AuthorRow>, StoreError> {
    let row = crate::store::optional(conn.query_row(
        "SELECT did, followers, active, labels, checked_at FROM authors WHERE did = ?1",
        [did],
        |row| {
            let did: String = row.get(0)?;
            let followers: Option<i64> = row.get(1)?;
            let active: i64 = row.get(2)?;
            let labels: Option<String> = row.get(3)?;
            let checked_at: i64 = row.get(4)?;
            Ok((did, followers, active, labels, checked_at))
        },
    ))?;
    match row {
        None => Ok(None),
        Some((did, followers, active, labels, checked_at)) => {
            let labels = match labels {
                None => None,
                Some(text) => Some(parse_labels(&text)?),
            };
            Ok(Some(AuthorRow { did, followers, active: active != 0, labels, checked_at }))
        }
    }
}

/// Inserts or replaces the row for `row.did` (BC65). Every column is
/// replaced when the DID already has a row. `labels` is written as a JSON
/// array of strings, or `NULL` when `row.labels` is `None`.
pub fn author_put(conn: &Connection, row: &AuthorRow) -> Result<(), StoreError> {
    let labels = encode_labels(&row.labels)?;
    conn.execute(
        "INSERT INTO authors (did, followers, active, labels, checked_at) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(did) DO UPDATE SET
            followers = excluded.followers,
            active = excluded.active,
            labels = excluded.labels,
            checked_at = excluded.checked_at",
        rusqlite::params![row.did, row.followers, row.active as i64, labels, row.checked_at],
    )?;
    Ok(())
}

/// Parses a stored `labels` value as a JSON array of strings.
/// `StoreError::MalformedRow` on anything else (BC66).
fn parse_labels(text: &str) -> Result<Vec<String>, StoreError> {
    serde_json::from_str::<Vec<String>>(text)
        .map_err(|_| StoreError::MalformedRow { table: "authors", column: "labels" })
}

/// Serialises `labels` to a JSON array of strings, or `None` for a `NULL`
/// column, the same encoding `author_put` uses.
fn encode_labels(labels: &Option<Vec<String>>) -> Result<Option<String>, StoreError> {
    match labels {
        None => Ok(None),
        Some(labels) => Ok(Some(
            serde_json::to_string(labels)
                .map_err(|_| StoreError::MalformedRow { table: "authors", column: "labels" })?,
        )),
    }
}

/// Reads the rows for `dids`, skipping any DID with no row. Chunked at
/// `MAX_BOUND_PARAMS` so a batch larger than SQLite's bound-parameter limit
/// is split into multiple statements (BC20), each its own `prepare_cached`
/// call. An empty `dids` makes no query at all.
pub fn authors_get_many(conn: &Connection, dids: &[&str]) -> Result<Vec<AuthorRow>, StoreError> {
    let mut rows = Vec::new();
    for chunk in dids.chunks(crate::store::MAX_BOUND_PARAMS) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT did, followers, active, labels, checked_at FROM authors WHERE did IN ({placeholders})"
        );
        let mut stmt = conn.prepare_cached(&sql)?;
        let mapped = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
            let did: String = row.get(0)?;
            let followers: Option<i64> = row.get(1)?;
            let active: i64 = row.get(2)?;
            let labels: Option<String> = row.get(3)?;
            let checked_at: i64 = row.get(4)?;
            Ok((did, followers, active, labels, checked_at))
        })?;
        for row in mapped {
            let (did, followers, active, labels, checked_at) = row?;
            let labels = match labels {
                None => None,
                Some(text) => Some(parse_labels(&text)?),
            };
            rows.push(AuthorRow { did, followers, active: active != 0, labels, checked_at });
        }
    }
    Ok(rows)
}

/// Inserts or replaces every row in `rows` inside one
/// `conn.unchecked_transaction()`, committed at the end: a failure applies
/// none of them (BC19). A no-op, with no transaction opened, when `rows` is
/// empty.
pub fn authors_put_many(conn: &Connection, rows: &[AuthorRow]) -> Result<(), StoreError> {
    if rows.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO authors (did, followers, active, labels, checked_at) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(did) DO UPDATE SET
                followers = excluded.followers,
                active = excluded.active,
                labels = excluded.labels,
                checked_at = excluded.checked_at",
        )?;
        for row in rows {
            let labels = encode_labels(&row.labels)?;
            stmt.execute(rusqlite::params![
                row.did,
                row.followers,
                row.active as i64,
                labels,
                row.checked_at
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::migrated_conn;

    fn row(did: &str) -> AuthorRow {
        AuthorRow {
            did: did.to_string(),
            followers: Some(42),
            active: true,
            labels: Some(vec!["spam".to_string()]),
            checked_at: 1_700_000_000,
        }
    }

    #[test]
    fn author_get_missing_is_none() {
        let conn = migrated_conn();
        assert_eq!(author_get(&conn, "did:plc:missing").unwrap(), None);
    }

    #[test]
    fn author_put_then_get_round_trips() {
        let conn = migrated_conn();
        let row = row("did:plc:a");
        author_put(&conn, &row).unwrap();
        assert_eq!(author_get(&conn, "did:plc:a").unwrap(), Some(row));
    }

    #[test]
    fn author_put_with_no_labels_stores_null() {
        let conn = migrated_conn();
        let row = AuthorRow {
            did: "did:plc:a".to_string(),
            followers: None,
            active: false,
            labels: None,
            checked_at: 1_700_000_000,
        };
        author_put(&conn, &row).unwrap();
        assert_eq!(author_get(&conn, "did:plc:a").unwrap(), Some(row));
    }

    #[test]
    fn author_put_replaces_every_column() {
        let conn = migrated_conn();
        author_put(&conn, &row("did:plc:a")).unwrap();

        let updated = AuthorRow {
            did: "did:plc:a".to_string(),
            followers: Some(7),
            active: false,
            labels: Some(vec!["a".to_string(), "b".to_string()]),
            checked_at: 1_700_000_100,
        };
        author_put(&conn, &updated).unwrap();
        assert_eq!(author_get(&conn, "did:plc:a").unwrap(), Some(updated));
    }

    #[test]
    fn author_get_rejects_labels_that_are_not_a_json_array() {
        let conn = migrated_conn();
        conn.execute(
            "INSERT INTO authors (did, active, labels, checked_at) VALUES ('did:plc:a', 1, 'not-json', 1700000000)",
            [],
        )
        .unwrap();

        let err = author_get(&conn, "did:plc:a").unwrap_err();
        match err {
            StoreError::MalformedRow { table, column } => {
                assert_eq!(table, "authors");
                assert_eq!(column, "labels");
            }
            other => panic!("expected MalformedRow, got {other:?}"),
        }
    }

    #[test]
    fn authors_get_many_returns_known_rows_and_skips_unknown_dids() {
        let conn = migrated_conn();
        author_put(&conn, &row("did:plc:a")).unwrap();
        author_put(&conn, &row("did:plc:b")).unwrap();

        let mut got = authors_get_many(&conn, &["did:plc:a", "did:plc:b", "did:plc:missing"])
            .unwrap()
            .into_iter()
            .map(|row| row.did)
            .collect::<Vec<_>>();
        got.sort();
        assert_eq!(got, vec!["did:plc:a".to_string(), "did:plc:b".to_string()]);
    }

    #[test]
    fn authors_get_many_of_empty_dids_makes_no_query_and_returns_empty() {
        let conn = migrated_conn();
        assert_eq!(authors_get_many(&conn, &[]).unwrap(), Vec::new());
    }

    // BC20: an `IN (...)` read longer than `MAX_BOUND_PARAMS` is split into
    // chunks of at most that many, one statement each. Success over this many
    // distinct DIDs proves the chunking loop runs; an unchunked query would
    // fail with "too many SQL variables".
    #[test]
    fn authors_get_many_chunks_past_the_bound_parameter_limit() {
        let conn = migrated_conn();
        let n = crate::store::MAX_BOUND_PARAMS + 10;
        let dids: Vec<String> = (0..n).map(|i| format!("did:plc:{i}")).collect();
        for did in &dids {
            author_put(&conn, &row(did)).unwrap();
        }

        let refs: Vec<&str> = dids.iter().map(String::as_str).collect();
        let got = authors_get_many(&conn, &refs).unwrap();
        assert_eq!(got.len(), n);
    }

    #[test]
    fn authors_put_many_writes_every_row_in_one_transaction() {
        let conn = migrated_conn();
        let rows = vec![row("did:plc:a"), row("did:plc:b"), row("did:plc:c")];
        authors_put_many(&conn, &rows).unwrap();

        for expected in &rows {
            assert_eq!(author_get(&conn, &expected.did).unwrap().as_ref(), Some(expected));
        }
    }

    #[test]
    fn authors_put_many_replaces_an_existing_row() {
        let conn = migrated_conn();
        author_put(&conn, &row("did:plc:a")).unwrap();

        let updated = AuthorRow {
            did: "did:plc:a".to_string(),
            followers: Some(7),
            active: false,
            labels: None,
            checked_at: 1_700_000_100,
        };
        authors_put_many(&conn, &[updated.clone()]).unwrap();
        assert_eq!(author_get(&conn, "did:plc:a").unwrap(), Some(updated));
    }

    // BC19: a no-op on empty input, opening no transaction.
    #[test]
    fn authors_put_many_of_empty_rows_is_a_noop() {
        let conn = migrated_conn();
        authors_put_many(&conn, &[]).unwrap();
        assert_eq!(author_get(&conn, "did:plc:a").unwrap(), None);
    }
}
