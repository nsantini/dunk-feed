//! The writer's operation type and `commit_batch`, TECH-DESIGN section 5.2
//! and 5.4. This slice adds `commit_batch`, which applies one batch of `Op`
//! inside a single transaction; the writer thread, `WriterConfig` and
//! `WriterHandle` land in slice 3.0.

use rusqlite::Connection;

use crate::store::{counts, interactions, meta, pairs, StoreError};

/// The count column one `Incr` moves, TECH-DESIGN section 5.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountField {
    Likes,
    Reposts,
    Replies,
}

/// One unit of work for the writer thread. One variant per row of
/// TECH-DESIGN section 5.2, plus `Checkpoint` (moves the cursor past an
/// event that produced no op) and `Interaction` (the row `sendInteractions`,
/// story 08, writes). Every variant but `Interaction` carries the Jetstream
/// `seq` it came from.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    InsertPair {
        quote_uri: String,
        quote_did: String,
        quote_cid: String,
        original_uri: String,
        original_did: String,
        quoted_at: i64,
        first_seen_at: i64,
        seq: u64,
    },
    Incr {
        post_uri: String,
        field: CountField,
        seq: u64,
    },
    DeletePost {
        uri: String,
        seq: u64,
    },
    Detach {
        quote_uri: String,
        seq: u64,
    },
    Checkpoint {
        seq: u64,
    },
    Interaction {
        item: Option<String>,
        event: Option<String>,
        feed_context: Option<String>,
        req_id: Option<String>,
    },
}

impl Op {
    /// The Jetstream `seq` this op carries, or `None` for `Interaction`
    /// (BC23): an interaction row has no Jetstream position.
    pub fn seq(&self) -> Option<u64> {
        match self {
            Op::InsertPair { seq, .. }
            | Op::Incr { seq, .. }
            | Op::DeletePost { seq, .. }
            | Op::Detach { seq, .. }
            | Op::Checkpoint { seq } => Some(*seq),
            Op::Interaction { .. } => None,
        }
    }
}

/// Applies every `Op` in `ops` inside one `BEGIN ... COMMIT` transaction,
/// then writes `meta.jetstream_seq` to the batch's highest `seq` in that
/// same transaction (BC7, BC24). An empty batch opens no transaction and
/// commits nothing (matches BC41's empty-batch case). A batch holding only
/// `Op::Interaction` values carries no seq, so the cursor is not written
/// (BC25); `Op::Checkpoint` writes no row of its own but still counts
/// toward the highest seq (BC27). The batch's own highest seq is written
/// unconditionally, never compared against the stored value (BC26). If any
/// op fails, the transaction is dropped without a commit and rolls back: no
/// op in the batch lands and `meta.jetstream_seq` keeps its previous value
/// (BC8, BC35).
pub fn commit_batch(conn: &Connection, ops: &[Op], now: i64) -> Result<(), StoreError> {
    if ops.is_empty() {
        return Ok(());
    }

    let tx = conn.unchecked_transaction()?;
    let mut highest_seq: Option<u64> = None;
    for op in ops {
        if let Some(seq) = op.seq() {
            highest_seq = Some(highest_seq.map_or(seq, |h| h.max(seq)));
        }
        apply_op(&tx, op, now)?;
    }
    if let Some(seq) = highest_seq {
        meta::set_cursor(&tx, seq)?;
    }
    tx.commit()?;
    Ok(())
}

fn apply_op(conn: &Connection, op: &Op, now: i64) -> Result<(), StoreError> {
    match op {
        Op::InsertPair {
            quote_uri,
            quote_did,
            quote_cid,
            original_uri,
            original_did,
            quoted_at,
            first_seen_at,
            ..
        } => pairs::insert_pair(
            conn,
            quote_uri,
            quote_did,
            quote_cid,
            original_uri,
            original_did,
            *quoted_at,
            *first_seen_at,
        ),
        Op::Incr { post_uri, field, .. } => counts::incr(conn, post_uri, *field, now),
        Op::DeletePost { uri, .. } => pairs::delete_post(conn, uri),
        Op::Detach { quote_uri, .. } => pairs::detach(conn, quote_uri),
        Op::Checkpoint { .. } => Ok(()),
        Op::Interaction { item, event, feed_context, req_id } => interactions::insert_interaction(
            conn,
            item.as_deref(),
            event.as_deref(),
            feed_context.as_deref(),
            req_id.as_deref(),
            now,
        ),
    }
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

    fn incr_op(post_uri: &str, seq: u64) -> Op {
        Op::Incr { post_uri: post_uri.to_string(), field: CountField::Likes, seq }
    }

    #[test]
    fn seq_commits_with_batch() {
        let conn = migrated_conn();
        let ops = vec![
            incr_op("at://did:plc:o/app.bsky.feed.post/o1", 1),
            incr_op("at://did:plc:o/app.bsky.feed.post/o1", 2),
        ];
        commit_batch(&conn, &ops, 1_700_000_000).unwrap();

        assert_eq!(meta::cursor(&conn).unwrap(), Some(2));
        let likes: i64 = conn
            .query_row(
                "SELECT likes FROM counts WHERE post_uri = 'at://did:plc:o/app.bsky.feed.post/o1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(likes, 2);
    }

    #[test]
    fn partial_batch_not_committed() {
        let conn = migrated_conn();
        // Break the `pairs` table so `InsertPair` fails part way through
        // the batch (BC8, BC35).
        conn.execute("DROP TABLE pairs", []).unwrap();

        let ops = vec![
            incr_op("at://did:plc:o/app.bsky.feed.post/o1", 1),
            Op::InsertPair {
                quote_uri: "at://did:plc:q/app.bsky.feed.post/q1".to_string(),
                quote_did: "did:plc:q".to_string(),
                quote_cid: "bafyq".to_string(),
                original_uri: "at://did:plc:o/app.bsky.feed.post/o1".to_string(),
                original_did: "did:plc:o".to_string(),
                quoted_at: 1_700_000_000,
                first_seen_at: 1_700_000_000,
                seq: 2,
            },
        ];
        assert!(commit_batch(&conn, &ops, 1_700_000_000).is_err());

        assert_eq!(meta::cursor(&conn).unwrap(), None, "the cursor must not move");
        let count: i64 =
            conn.query_row("SELECT count(*) FROM counts", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 0, "the Incr op must have rolled back too");
    }

    #[test]
    fn resume_is_inclusive() {
        let conn = migrated_conn();
        let ops = vec![incr_op("at://did:plc:o/app.bsky.feed.post/o1", 7)];
        commit_batch(&conn, &ops, 1_700_000_000).unwrap();

        // The stored cursor is the highest committed seq; the caller
        // resumes at seq + 1, so at most that one event is replayed.
        let stored = meta::cursor(&conn).unwrap().unwrap();
        assert_eq!(stored, 7);
        let resume_at = stored + 1;
        assert_eq!(resume_at, 8);
    }

    #[test]
    fn batch_of_only_interactions_does_not_move_the_cursor() {
        let conn = migrated_conn();
        let ops =
            vec![Op::Interaction { item: None, event: None, feed_context: None, req_id: None }];
        commit_batch(&conn, &ops, 1_700_000_000).unwrap();

        assert_eq!(meta::cursor(&conn).unwrap(), None);
        let count: i64 =
            conn.query_row("SELECT count(*) FROM interactions", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn checkpoint_writes_no_row_but_moves_the_cursor() {
        let conn = migrated_conn();
        let ops = vec![Op::Checkpoint { seq: 9 }];
        commit_batch(&conn, &ops, 1_700_000_000).unwrap();

        assert_eq!(meta::cursor(&conn).unwrap(), Some(9));
        let interactions: i64 =
            conn.query_row("SELECT count(*) FROM interactions", [], |row| row.get(0)).unwrap();
        assert_eq!(interactions, 0);
        let counts: i64 =
            conn.query_row("SELECT count(*) FROM counts", [], |row| row.get(0)).unwrap();
        assert_eq!(counts, 0);
    }

    #[test]
    fn highest_seq_in_the_batch_wins_with_no_comparison() {
        let conn = migrated_conn();
        commit_batch(&conn, &[incr_op("at://did:plc:o/app.bsky.feed.post/o1", 50)], 1_700_000_000)
            .unwrap();
        assert_eq!(meta::cursor(&conn).unwrap(), Some(50));

        // A later batch's highest seq is lower than the stored value; it is
        // still written, with no comparison against what is on file (BC26).
        commit_batch(&conn, &[incr_op("at://did:plc:o/app.bsky.feed.post/o1", 10)], 1_700_000_001)
            .unwrap();
        assert_eq!(meta::cursor(&conn).unwrap(), Some(10));
    }

    #[test]
    fn empty_batch_commits_nothing() {
        let conn = migrated_conn();
        commit_batch(&conn, &[], 1_700_000_000).unwrap();
        assert_eq!(meta::cursor(&conn).unwrap(), None);
    }

    fn insert_pair_op(seq: u64) -> Op {
        Op::InsertPair {
            quote_uri: "at://did:plc:q/app.bsky.feed.post/q1".to_string(),
            quote_did: "did:plc:q".to_string(),
            quote_cid: "bafyq".to_string(),
            original_uri: "at://did:plc:o/app.bsky.feed.post/o1".to_string(),
            original_did: "did:plc:o".to_string(),
            quoted_at: 1_700_000_000,
            first_seen_at: 1_700_000_000,
            seq,
        }
    }

    #[test]
    fn seq_is_none_only_for_interaction() {
        assert_eq!(insert_pair_op(1).seq(), Some(1));
        assert_eq!(
            Op::Incr { post_uri: "at://x".to_string(), field: CountField::Likes, seq: 2 }.seq(),
            Some(2)
        );
        assert_eq!(Op::DeletePost { uri: "at://x".to_string(), seq: 3 }.seq(), Some(3));
        assert_eq!(Op::Detach { quote_uri: "at://x".to_string(), seq: 4 }.seq(), Some(4));
        assert_eq!(Op::Checkpoint { seq: 5 }.seq(), Some(5));
        assert_eq!(
            Op::Interaction { item: None, event: None, feed_context: None, req_id: None }.seq(),
            None
        );
    }
}
