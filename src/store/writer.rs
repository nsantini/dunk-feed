//! The writer's operation type, `commit_batch`, and the writer thread
//! itself, TECH-DESIGN section 5.2 and 5.4. `commit_batch` applies one
//! batch of `Op` inside a single transaction; `spawn` starts the one
//! dedicated `std::thread` that owns the channel's receiver, batches by
//! `WriterConfig::max_ops` or `WriterConfig::interval`, and drives every
//! commit through `commit_batch` on the connection it shares with `Store`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};

use crate::store::{counts, interactions, meta, pairs, unix_now, StoreError};

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

/// The writer's batch-boundary and channel-capacity numbers, TECH-DESIGN
/// section 5.4. These are module constants there, not `Config` fields (the
/// engineer's step 2 answer), so `WriterConfig::default()` is the only
/// place they are set outside a test, which builds a smaller config
/// through `Store::writer_with` (BC42).
#[derive(Debug, Clone, Copy)]
pub struct WriterConfig {
    /// The bounded channel's capacity. `WriterHandle::send` waits, rather
    /// than drops the op, once this many are already queued (BC37).
    pub capacity: usize,
    /// A batch closes at this many ops even if `interval` has not elapsed
    /// (BC40).
    pub max_ops: usize,
    /// A batch closes on this timer even if fewer than `max_ops` ops have
    /// arrived (BC41).
    pub interval: Duration,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig { capacity: 10_000, max_ops: 1_000, interval: Duration::from_millis(500) }
    }
}

/// One message on the writer's channel: an `Op` to apply, or a control
/// message the writer thread acknowledges only once the batch holding it
/// has committed (or, for `Shutdown`, right before the thread exits).
enum WriterMsg {
    Op(Op),
    Flush(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

/// A handle to the running writer thread. `send`, `flush` and `shutdown`
/// all go through the same bounded channel, so a `flush` queued after a
/// run of `send`s only ever acknowledges once every op ahead of it in the
/// queue has been committed (BC38): the channel is FIFO, and the writer
/// thread is single-threaded, so nothing ahead of a message in the queue
/// can still be uncommitted once that message is dispatched.
#[derive(Debug, Clone)]
pub struct WriterHandle {
    tx: mpsc::Sender<WriterMsg>,
}

impl WriterHandle {
    /// Queues one `Op`. Waits when the channel is full rather than
    /// dropping the op (BC37). `StoreError::WriterGone` once the writer
    /// thread has exited and dropped its receiver.
    pub async fn send(&self, op: Op) -> Result<(), StoreError> {
        self.tx.send(WriterMsg::Op(op)).await.map_err(|_| StoreError::WriterGone)
    }

    /// Returns once every op sent before this call is committed (BC38).
    pub async fn flush(&self) -> Result<(), StoreError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx.send(WriterMsg::Flush(ack_tx)).await.map_err(|_| StoreError::WriterGone)?;
        ack_rx.await.map_err(|_| StoreError::WriterGone)
    }

    /// Commits the pending batch, then the writer thread exits (BC39). A
    /// `send`, `flush` or `shutdown` made after this call returns
    /// `StoreError::WriterGone`, because the thread has dropped the
    /// channel's receiver.
    pub async fn shutdown(&self) -> Result<(), StoreError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx.send(WriterMsg::Shutdown(ack_tx)).await.map_err(|_| StoreError::WriterGone)?;
        ack_rx.await.map_err(|_| StoreError::WriterGone)
    }
}

/// Spawns the one writer thread over `conn`, TECH-DESIGN section 5.4:
/// sharing `Store`'s connection, rather than opening a second one, is what
/// lets a `:memory:` test see the writer's rows.
pub(crate) fn spawn(conn: Arc<Mutex<Connection>>, cfg: WriterConfig) -> WriterHandle {
    let (tx, rx) = mpsc::channel(cfg.capacity);
    std::thread::Builder::new()
        .name("dunk-store-writer".to_string())
        .spawn(move || writer_loop(conn, rx, cfg))
        .expect("failed to spawn the store writer thread");
    WriterHandle { tx }
}

/// Waits for one message, but never past `deadline`. `None` on a timeout
/// or once every `WriterHandle` has dropped its sender. Polls rather than
/// blocking indefinitely, because `tokio::sync::mpsc::Receiver` has no
/// `recv`-with-timeout outside an async context, and the writer thread is
/// a plain `std::thread`, not a runtime worker.
fn recv_before(rx: &mut mpsc::Receiver<WriterMsg>, deadline: Instant) -> Option<WriterMsg> {
    loop {
        match rx.try_recv() {
            Ok(msg) => return Some(msg),
            Err(mpsc::error::TryRecvError::Disconnected) => return None,
            Err(mpsc::error::TryRecvError::Empty) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return None;
                }
                std::thread::sleep(remaining.min(Duration::from_millis(5)));
            }
        }
    }
}

/// The writer thread's whole life: block for the first message of a batch
/// (an idle writer must not spin), collect more until `max_ops` or
/// `interval` closes the batch (BC40, BC41), commit it, acknowledge every
/// `Flush` and `Shutdown` queued inside it, and exit once a `Shutdown` has
/// been seen or every sender has dropped. A `Flush` or `Shutdown` also
/// closes the batch immediately, even with zero ops and long before
/// `interval` would: a caller waiting on `flush` or `shutdown` must not be
/// made to wait out the batch timer (BC38, BC39).
fn writer_loop(conn: Arc<Mutex<Connection>>, mut rx: mpsc::Receiver<WriterMsg>, cfg: WriterConfig) {
    loop {
        let first = match rx.blocking_recv() {
            Some(msg) => msg,
            None => return, // Every WriterHandle was dropped.
        };

        let mut ops: Vec<Op> = Vec::new();
        let mut flush_acks: Vec<oneshot::Sender<()>> = Vec::new();
        let mut shutdown_ack: Option<oneshot::Sender<()>> = None;
        let deadline = Instant::now() + cfg.interval;
        let mut close_now = false;

        match first {
            WriterMsg::Op(op) => ops.push(op),
            WriterMsg::Flush(ack) => {
                flush_acks.push(ack);
                close_now = true;
            }
            WriterMsg::Shutdown(ack) => {
                shutdown_ack = Some(ack);
                close_now = true;
            }
        }

        while !close_now && ops.len() < cfg.max_ops {
            match recv_before(&mut rx, deadline) {
                Some(WriterMsg::Op(op)) => ops.push(op),
                Some(WriterMsg::Flush(ack)) => {
                    flush_acks.push(ack);
                    close_now = true;
                }
                Some(WriterMsg::Shutdown(ack)) => {
                    shutdown_ack = Some(ack);
                    close_now = true;
                }
                None => break, // The timer elapsed, or every sender dropped.
            }
        }

        let now = unix_now();
        let commit_result = match conn.lock() {
            Ok(guard) => commit_batch(&guard, &ops, now),
            Err(_) => Err(StoreError::Poisoned),
        };

        if let Err(err) = commit_result {
            tracing::error!(error = %err, "store writer: commit_batch failed, thread exiting");
            // Dropping `flush_acks` and `shutdown_ack` without a send makes
            // every waiter's `.await` fail on a closed oneshot channel,
            // which `WriterHandle` turns into `StoreError::WriterGone`
            // (BC36). Dropping `rx` when this function returns does the
            // same for the next `send`.
            return;
        }

        for ack in flush_acks {
            let _ = ack.send(());
        }
        if let Some(ack) = shutdown_ack {
            let _ = ack.send(());
            return;
        }
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

    fn shared_conn() -> Arc<Mutex<Connection>> {
        Arc::new(Mutex::new(migrated_conn()))
    }

    fn likes(conn: &Connection, post_uri: &str) -> i64 {
        conn.query_row("SELECT likes FROM counts WHERE post_uri = ?1", [post_uri], |row| row.get(0))
            .unwrap()
    }

    #[tokio::test]
    // Holding the `std::sync::Mutex` guard across the `.await`s below is
    // the point of this test: it is what stops the writer thread (a plain
    // `std::thread`, not a runtime worker) from draining the channel, so a
    // full channel can be observed at all.
    #[allow(clippy::await_holding_lock)]
    async fn full_channel_makes_send_wait() {
        let conn = shared_conn();
        let cfg = WriterConfig { capacity: 2, max_ops: 1, interval: Duration::from_secs(30) };
        let handle = spawn(Arc::clone(&conn), cfg);
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";

        // Hold the connection so the writer thread, which pulls this first
        // op off the channel right away (`max_ops` is 1), blocks trying to
        // commit it instead of draining any more of the channel.
        let guard = conn.lock().unwrap();
        handle.send(incr_op(uri, 1)).await.unwrap();
        // Give the thread a moment to pull that op and reach the blocked
        // lock acquisition before the channel is filled below.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The channel's capacity is 2; these two fit without waiting.
        handle.send(incr_op(uri, 2)).await.unwrap();
        handle.send(incr_op(uri, 3)).await.unwrap();

        // A further send must wait rather than drop the op (BC37).
        let timed_out =
            tokio::time::timeout(Duration::from_millis(150), handle.send(incr_op(uri, 4))).await;
        assert!(timed_out.is_err(), "send must wait while the channel is full");

        drop(guard);

        // Once the lock is free the writer commits and drains the rest;
        // the op dropped by the timed-out future above is resent.
        handle.send(incr_op(uri, 4)).await.unwrap();
        handle.flush().await.unwrap();

        assert_eq!(likes(&conn.lock().unwrap(), uri), 4, "every op must have landed");
    }

    #[tokio::test]
    async fn batch_closes_at_max_ops_without_waiting_for_the_timer() {
        let conn = shared_conn();
        let cfg = WriterConfig { capacity: 100, max_ops: 5, interval: Duration::from_secs(30) };
        let handle = spawn(Arc::clone(&conn), cfg);
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";

        for seq in 1..=5u64 {
            handle.send(incr_op(uri, seq)).await.unwrap();
        }

        // If the batch had not already closed at 5 ops, this would have to
        // wait out the 30-second interval instead of returning promptly.
        tokio::time::timeout(Duration::from_secs(5), handle.flush()).await.unwrap().unwrap();

        assert_eq!(likes(&conn.lock().unwrap(), uri), 5);
    }

    #[tokio::test]
    async fn batch_closes_on_the_timer() {
        let conn = shared_conn();
        let cfg =
            WriterConfig { capacity: 100, max_ops: 1_000, interval: Duration::from_millis(50) };
        let handle = spawn(Arc::clone(&conn), cfg);
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";

        handle.send(incr_op(uri, 1)).await.unwrap();

        // Well past the 50ms interval, and nowhere near max_ops, so only
        // the timer can have closed this batch.
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(likes(&conn.lock().unwrap(), uri), 1);
    }

    #[tokio::test]
    async fn flush_returns_after_the_pending_batch_is_committed() {
        let conn = shared_conn();
        let cfg = WriterConfig { capacity: 100, max_ops: 1_000, interval: Duration::from_secs(30) };
        let handle = spawn(Arc::clone(&conn), cfg);
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";

        handle.send(incr_op(uri, 1)).await.unwrap();
        handle.flush().await.unwrap();

        assert_eq!(likes(&conn.lock().unwrap(), uri), 1);
    }

    #[tokio::test]
    async fn shutdown_commits_then_a_later_send_is_writer_gone() {
        let conn = shared_conn();
        let handle = spawn(Arc::clone(&conn), WriterConfig::default());
        let uri = "at://did:plc:o/app.bsky.feed.post/o1";

        handle.send(incr_op(uri, 1)).await.unwrap();
        handle.shutdown().await.unwrap();

        assert_eq!(likes(&conn.lock().unwrap(), uri), 1);

        let err = handle.send(incr_op(uri, 2)).await.unwrap_err();
        assert!(matches!(err, StoreError::WriterGone));
    }

    #[test]
    #[ignore]
    fn throughput_100k_incr() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("dunk-store-throughput-{nanos}.sqlite3"));
        let path_str = path.to_str().unwrap().to_string();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = crate::store::Store::open_path(&path_str).unwrap();
            let handle = store.writer();

            let start = Instant::now();
            for seq in 0..100_000u64 {
                handle
                    .send(Op::Incr {
                        post_uri: format!("at://did:plc:o/app.bsky.feed.post/o{}", seq % 1_000),
                        field: CountField::Likes,
                        seq,
                    })
                    .await
                    .unwrap();
            }
            handle.flush().await.unwrap();
            let elapsed = start.elapsed();
            handle.shutdown().await.unwrap();

            let file_size = std::fs::metadata(&path_str).map(|m| m.len()).unwrap_or(0);
            println!(
                "throughput_100k_incr: {:.0} ops/s, {file_size} bytes",
                100_000.0 / elapsed.as_secs_f64()
            );
        });

        let _ = std::fs::remove_file(&path_str);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }
}
