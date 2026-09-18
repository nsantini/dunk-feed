//! The writer's operation type, TECH-DESIGN section 5.2 and 5.4. This slice
//! only defines `Op` and `CountField`; the writer thread, `commit_batch`,
//! `WriterConfig` and `WriterHandle` land in slices 2.0 and 3.0.

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

#[cfg(test)]
mod tests {
    use super::*;

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
