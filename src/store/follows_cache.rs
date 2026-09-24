//! `follows_cache` table access, TECH-DESIGN-network-feed §6.4, §8. Story 06
//! creates the table empty, with no reader or writer (`schema.rs`'s V2
//! statements); story 08 (this module) is the first to read or write it.
//! Each row's `follows` column is a BLOB of little-endian `u64` hashes, one
//! account's degree-2 sample DIDs already hashed the same way
//! `graph::hash_did` hashes everything else this binary keeps in memory
//! (design §8: "Hashes are stored, not DIDs") — `account_did` itself is the
//! exception, kept as the plain DID text so `graph::cache::FollowsCache` can
//! key its lookups the same way `Circle::d2_sample` names accounts.

use rusqlite::Connection;

use crate::store::StoreError;

/// One `follows_cache` row: when it was fetched, and the account's follows
/// as a plain `Vec<u64>` in whatever order [`decode`] read them back in
/// (BC6: the round trip preserves order, so a sorted, deduplicated caller
/// gets a sorted, deduplicated result).
#[derive(Debug, Clone, PartialEq)]
pub struct FollowsCacheRow {
    pub fetched_at: i64,
    pub follows: Vec<u64>,
}

/// Encodes `follows` as a BLOB: each `u64` as 8 little-endian bytes, in
/// order (BC6). An empty slice encodes to an empty BLOB.
pub fn encode(follows: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(follows.len() * 8);
    for hash in follows {
        out.extend_from_slice(&hash.to_le_bytes());
    }
    out
}

/// Decodes a BLOB written by [`encode`] back into a `Vec<u64>` (BC6). A
/// length that is not a multiple of 8 is `StoreError::MalformedRow` (BC6a):
/// there is no way to recover a partial `u64` from a truncated tail, and
/// this binary is the only writer, so a bad length can only be a partial
/// write from an earlier crash.
pub fn decode(bytes: &[u8]) -> Result<Vec<u64>, StoreError> {
    if !bytes.len().is_multiple_of(8) {
        return Err(StoreError::MalformedRow { table: "follows_cache", column: "follows" });
    }
    Ok(bytes.as_chunks::<8>().0.iter().map(|chunk| u64::from_le_bytes(*chunk)).collect())
}

/// Reads the row for `account_did`. `Ok(None)` when there is none. A
/// malformed `follows` BLOB (BC6a) is returned as `Err`, not silently
/// treated as missing: `graph::cache::FollowsCache::get` handles that `Err`
/// as a missing entry (BC6a), and `GraphHandle::from_store`'s preload skips
/// it with a log naming no DID (BC12).
pub fn follows_get(
    conn: &Connection,
    account_did: &str,
) -> Result<Option<FollowsCacheRow>, StoreError> {
    let row = crate::store::optional(conn.query_row(
        "SELECT fetched_at, follows FROM follows_cache WHERE account_did = ?1",
        [account_did],
        |row| {
            let fetched_at: i64 = row.get(0)?;
            let follows: Vec<u8> = row.get(1)?;
            Ok((fetched_at, follows))
        },
    ))?;
    match row {
        None => Ok(None),
        Some((fetched_at, follows)) => {
            Ok(Some(FollowsCacheRow { fetched_at, follows: decode(&follows)? }))
        }
    }
}

/// Inserts a `follows_cache` row for `account_did`, or replaces both
/// `fetched_at` and `follows` when one already exists (BC6b).
pub fn follows_put(
    conn: &Connection,
    account_did: &str,
    fetched_at: i64,
    follows: &[u64],
) -> Result<(), StoreError> {
    let blob = encode(follows);
    conn.execute(
        "INSERT INTO follows_cache (account_did, fetched_at, follows)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(account_did) DO UPDATE SET
            fetched_at = excluded.fetched_at,
            follows = excluded.follows",
        rusqlite::params![account_did, fetched_at, blob],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::migrated_conn;

    // BC6: encode/decode round-trips a sorted list, including empty.
    #[test]
    fn encode_decode_round_trips_a_sorted_list() {
        let values: Vec<u64> = vec![1, 2, 3, u64::MAX];
        assert_eq!(decode(&encode(&values)).unwrap(), values);
    }

    #[test]
    fn encode_decode_round_trips_an_empty_list() {
        let values: Vec<u64> = Vec::new();
        assert_eq!(decode(&encode(&values)).unwrap(), values);
    }

    // BC6a: a length not a multiple of 8 is MalformedRow.
    #[test]
    fn decode_rejects_a_bad_length() {
        let err = decode(&[1, 2, 3]).unwrap_err();
        match err {
            StoreError::MalformedRow { table, column } => {
                assert_eq!(table, "follows_cache");
                assert_eq!(column, "follows");
            }
            other => panic!("expected MalformedRow, got {other:?}"),
        }
    }

    #[test]
    fn follows_get_of_an_unknown_account_is_none() {
        let conn = migrated_conn();
        assert_eq!(follows_get(&conn, "did:plc:missing").unwrap(), None);
    }

    // AC3: follows_put then follows_get round-trips fetched_at and follows.
    #[test]
    fn follows_put_then_get_round_trips() {
        let conn = migrated_conn();
        follows_put(&conn, "did:plc:a", 1_700_000_000, &[1, 2, 3]).unwrap();

        let row = follows_get(&conn, "did:plc:a").unwrap().unwrap();
        assert_eq!(row.fetched_at, 1_700_000_000);
        assert_eq!(row.follows, vec![1, 2, 3]);
    }

    // BC6b: a second follows_put for the same account replaces both
    // fetched_at and follows.
    #[test]
    fn follows_put_replaces_an_existing_row() {
        let conn = migrated_conn();
        follows_put(&conn, "did:plc:a", 1_700_000_000, &[1, 2]).unwrap();
        follows_put(&conn, "did:plc:a", 1_700_000_500, &[9]).unwrap();

        let row = follows_get(&conn, "did:plc:a").unwrap().unwrap();
        assert_eq!(row.fetched_at, 1_700_000_500);
        assert_eq!(row.follows, vec![9]);

        let count: i64 =
            conn.query_row("SELECT count(*) FROM follows_cache", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    // BC6a, via the read path: a malformed BLOB fails follows_get instead
    // of being silently treated as an empty or missing entry.
    #[test]
    fn follows_get_of_a_malformed_row_is_an_error() {
        let conn = migrated_conn();
        conn.execute(
            "INSERT INTO follows_cache (account_did, fetched_at, follows) VALUES (?1, ?2, ?3)",
            rusqlite::params!["did:plc:bad", 1, vec![1_u8, 2, 3]],
        )
        .unwrap();

        let err = follows_get(&conn, "did:plc:bad").unwrap_err();
        assert!(matches!(err, StoreError::MalformedRow { table: "follows_cache", .. }));
    }
}
