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
    let row = conn.query_row(
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
    );
    match row {
        Ok((did, followers, active, labels, checked_at)) => {
            let labels = match labels {
                None => None,
                Some(text) => Some(parse_labels(&text)?),
            };
            Ok(Some(AuthorRow { did, followers, active: active != 0, labels, checked_at }))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(StoreError::Sqlite(err)),
    }
}

/// Inserts or replaces the row for `row.did` (BC65). Every column is
/// replaced when the DID already has a row. `labels` is written as a JSON
/// array of strings, or `NULL` when `row.labels` is `None`.
pub fn author_put(conn: &Connection, row: &AuthorRow) -> Result<(), StoreError> {
    let labels = match &row.labels {
        None => None,
        Some(labels) => Some(
            serde_json::to_string(labels)
                .map_err(|_| StoreError::MalformedRow { table: "authors", column: "labels" })?,
        ),
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::schema;

    fn migrated_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::migrate(&conn).unwrap();
        conn
    }

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
}
