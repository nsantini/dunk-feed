//! SQLite store, TECH-DESIGN section 6. `Store::open` opens the database at
//! `Config.db_path` in WAL mode and creates the versioned schema when it is
//! missing. `Store::writer` starts the one writer thread that commits
//! batches of `Op`; the rest of this module exposes the synchronous reads
//! the scorer needs (slice 4.0). `rusqlite` is synchronous and TECH-DESIGN
//! section 3 forbids an async SQLite crate, so every read here blocks the
//! calling thread briefly under one shared `Mutex<Connection>` — the same
//! connection the writer thread uses, so a `:memory:` test sees the
//! writer's rows.
//!
//! No raw SQL lives outside `src/store/` (AGENTS.md); every other module
//! calls through the functions this module re-exports.

// Round 2 finding 9: narrowed from a module-wide `#![allow(dead_code)]`. The
// scorer (this story) now calls every promote/demote/expire/feed path;
// `authors` alone still awaits its first caller, story 10's guards.
#[allow(dead_code)]
pub mod authors;
pub mod counts;
pub mod feed;
pub mod interactions;
pub mod meta;
pub mod pairs;
pub mod schema;
#[cfg(test)]
pub(crate) mod test_support;
pub mod writer;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OpenFlags};
use thiserror::Error;

use crate::config::Config;

/// Turns a `rusqlite` "no such row" result into `Ok(None)`, and any other
/// error into `StoreError::Sqlite`. Used at every site that reads at most
/// one optional row: `meta::meta_get`, `authors::author_get` and
/// `schema::schema_version` (round 1 finding 9).
pub(crate) fn optional<T>(res: rusqlite::Result<T>) -> Result<Option<T>, StoreError> {
    match res {
        Ok(value) => Ok(Some(value)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(StoreError::Sqlite(err)),
    }
}

/// Every error this module raises. Constructed in `mod.rs`, `schema.rs` and
/// `writer.rs` (BC12); every variant but a writer-thread panic reaches the
/// caller. `main.rs` turns it into `anyhow` and exits 1 once story 06 calls
/// the store.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("failed to open database at {path}: {source}")]
    Open { path: String, source: rusqlite::Error },
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("schema version {found} is newer than the {supported} this binary supports")]
    SchemaTooNew { found: u64, supported: u64 },
    #[error("meta.{key} is not a valid number: {value:?}")]
    MalformedMeta { key: String, value: String },
    #[error("{table}.{column} holds a value this binary cannot parse")]
    MalformedRow { table: &'static str, column: &'static str },
    #[error("the writer thread has exited")]
    WriterGone,
    #[error("the store's connection mutex was poisoned")]
    Poisoned,
    #[error("Store::writer was already called on this store")]
    WriterAlreadyStarted,
    /// `WriterHandle::try_send` (story 08's `sendInteractions`, BC28) found
    /// the writer's bounded channel already full. The caller drops the op
    /// rather than waiting, unlike `WriterHandle::send`.
    #[error("the writer's bounded channel is full")]
    WriterFull,
}

/// A `pairs.state` value, TECH-DESIGN section 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairState {
    Candidate,
    Promoted,
    Dropped,
}

impl PairState {
    pub fn as_str(&self) -> &'static str {
        match self {
            PairState::Candidate => "candidate",
            PairState::Promoted => "promoted",
            PairState::Dropped => "dropped",
        }
    }
}

/// A `pairs.drop_reason` value, TECH-DESIGN section 8.3's ten reasons.
/// Round 2 finding 9 (BC52): no `Demoted` variant. A demote (BC20) returns a
/// pair to `candidate` with `drop_reason = NULL`; nothing ever constructs a
/// `drop_reason` of `"demoted"`, so `FromStr` no longer parses it either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    SelfQuote,
    NotAPost,
    QuoteGone,
    OriginalGone,
    Detached,
    Blocked,
    Labelled,
    AuthorInactive,
    FollowerFloor,
}

impl DropReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            DropReason::SelfQuote => "self_quote",
            DropReason::NotAPost => "not_a_post",
            DropReason::QuoteGone => "quote_gone",
            DropReason::OriginalGone => "original_gone",
            DropReason::Detached => "detached",
            DropReason::Blocked => "blocked",
            DropReason::Labelled => "labelled",
            DropReason::AuthorInactive => "author_inactive",
            DropReason::FollowerFloor => "follower_floor",
        }
    }
}

impl std::str::FromStr for DropReason {
    type Err = StoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "self_quote" => Ok(DropReason::SelfQuote),
            "not_a_post" => Ok(DropReason::NotAPost),
            "quote_gone" => Ok(DropReason::QuoteGone),
            "original_gone" => Ok(DropReason::OriginalGone),
            "detached" => Ok(DropReason::Detached),
            "blocked" => Ok(DropReason::Blocked),
            "labelled" => Ok(DropReason::Labelled),
            "author_inactive" => Ok(DropReason::AuthorInactive),
            "follower_floor" => Ok(DropReason::FollowerFloor),
            _ => Err(StoreError::MalformedRow { table: "pairs", column: "drop_reason" }),
        }
    }
}

/// Unix seconds, `i64`. Every timestamp column in section 6 is this and
/// nothing else (BC68): no string date, no millisecond value.
pub fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// SQLite's limit on the number of bound parameters in one statement
/// (`SQLITE_MAX_VARIABLE_NUMBER`'s default). Both of `pairs::expire`'s
/// `IN (...)` deletes split a URI list longer than this into chunks of at
/// most this many, one statement per chunk (BC39), so a scorer pass over a
/// large snapshot never exceeds it. `counts::clear_dirty_if_unchanged` (round
/// 2 finding 2) runs one `UPDATE` per row instead, so it never builds an
/// `IN (...)` list at all.
pub const MAX_BOUND_PARAMS: usize = 32_766;

/// Applies the pragmas TECH-DESIGN section 6 asks for, on every open
/// (BC18). On a `:memory:` path SQLite reports `journal_mode` as `memory`
/// instead of `wal`; that is accepted, not an error (BC19), and every other
/// pragma is still set as for a file.
fn apply_pragmas(conn: &Connection) -> Result<(), StoreError> {
    conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get::<_, String>(0))?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    // The negative form is KiB, so -65536 asks SQLite for a 64 MiB page
    // cache rather than 65,536 pages.
    conn.pragma_update(None, "cache_size", -65536)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "busy_timeout", 5000)?;
    Ok(())
}

/// The pragmas for the read-only connection a file-backed `Store` opens
/// alongside its writable one (BC74). `journal_mode` and `synchronous` are
/// write-mode properties already fixed by the writable connection that
/// created the file; `cache_size` (see `apply_pragmas`) and `busy_timeout`
/// are per-connection and apply here too.
fn apply_reader_pragmas(conn: &Connection) -> Result<(), StoreError> {
    conn.pragma_update(None, "cache_size", -65536)?;
    conn.pragma_update(None, "busy_timeout", 5000)?;
    Ok(())
}

/// Applies the pragmas and runs the migration on a freshly opened
/// connection. Shared by `open_path` and `open_memory` so the two
/// constructors do not each repeat it.
fn open_and_migrate(conn: &Connection) -> Result<(), StoreError> {
    apply_pragmas(conn)?;
    schema::migrate(conn)?;
    Ok(())
}

/// The SQLite store. Holds one `Arc<Mutex<Connection>>` for writes, shared
/// by the writer thread (slice 3.0), and — for a file-backed database — a
/// second `Arc<Mutex<Connection>>` opened read-only that every synchronous
/// read routes through (BC74), so a scan never waits on a commit. A
/// `:memory:` database has no second connection (BC75): a second connection
/// to `:memory:` would be a second, empty database. A caller on the tokio
/// runtime wraps a call into `Store` in `spawn_blocking` (BC76), because
/// `rusqlite` is synchronous and every method here blocks the calling
/// thread briefly.
#[derive(Debug, Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
    read_conn: Option<Arc<Mutex<Connection>>>,
    writer_started: Arc<AtomicBool>,
}

impl Store {
    /// Opens the database at `cfg.db_path`.
    pub fn open(cfg: &Config) -> Result<Self, StoreError> {
        Self::open_path(&cfg.db_path)
    }

    /// Opens a file database at `path`, applies the pragmas, and migrates
    /// it to `schema::CURRENT_VERSION`. A missing parent directory fails as
    /// `StoreError::Open` (BC13); the directory is never created. Unless
    /// `path` is `:memory:`, a second, read-only connection is opened
    /// alongside it (BC74).
    pub fn open_path(path: &str) -> Result<Self, StoreError> {
        let conn = Connection::open(path)
            .map_err(|source| StoreError::Open { path: path.to_string(), source })?;
        open_and_migrate(&conn)?;

        let read_conn = if path == ":memory:" {
            None
        } else {
            let reader = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .map_err(|source| StoreError::Open { path: path.to_string(), source })?;
            apply_reader_pragmas(&reader)?;
            Some(Arc::new(Mutex::new(reader)))
        };

        Ok(Store {
            conn: Arc::new(Mutex::new(conn)),
            read_conn,
            writer_started: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Opens an in-memory database. Tests use this so the writer thread and
    /// every read share the same connection (BC75). No production caller:
    /// `dunk run` always opens a file through `open`/`open_path`.
    #[allow(dead_code)]
    pub fn open_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        open_and_migrate(&conn)?;
        Ok(Store {
            conn: Arc::new(Mutex::new(conn)),
            read_conn: None,
            writer_started: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Locks the writable connection. `StoreError::Poisoned` if a previous
    /// holder panicked while holding it.
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
        self.conn.lock().map_err(|_| StoreError::Poisoned)
    }

    /// Locks the read-only connection when one exists (a file-backed
    /// store), or falls back to the writable connection for a `:memory:`
    /// store, which has no second connection (BC74, BC75).
    fn read_lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
        match &self.read_conn {
            Some(read_conn) => read_conn.lock().map_err(|_| StoreError::Poisoned),
            None => self.lock(),
        }
    }

    /// The stored Jetstream cursor, `meta.jetstream_seq` (BC43, BC44).
    pub fn cursor(&self) -> Result<Option<u64>, StoreError> {
        let conn = self.read_lock()?;
        meta::cursor(&conn)
    }

    /// Streams both URIs of every pair whose state is not `dropped` into
    /// `f` (BC45). Duplicates are not removed; the caller holds a set.
    pub fn for_each_hot_uri(&self, f: impl FnMut(&str)) -> Result<(), StoreError> {
        let conn = self.read_lock()?;
        pairs::for_each_hot_uri(&conn, f)
    }

    /// `candidate` pairs inside `ttl_h` hours with a dirty counts row on
    /// either side (BC46, BC47, BC48).
    pub fn dirty_candidates(
        &self,
        now: i64,
        ttl_h: i64,
    ) -> Result<Vec<pairs::PairWithCounts>, StoreError> {
        let conn = self.read_lock()?;
        pairs::dirty_candidates(&conn, now, ttl_h)
    }

    /// `promoted` pairs whose `quoted_at` is at or after `now - h * 3600`
    /// (BC51).
    pub fn promoted_within(
        &self,
        now: i64,
        h: i64,
    ) -> Result<Vec<pairs::PairWithCounts>, StoreError> {
        let conn = self.read_lock()?;
        pairs::promoted_within(&conn, now, h)
    }

    /// Upserts a `feed` row and promotes its pair (BC52, BC53, BC54). Round
    /// 2 finding 5: the scorer itself now goes through `apply_verdicts`
    /// instead, so a single promote runs inside the same transaction as any
    /// drop or demote from the same verify phase. This wrapper stays for
    /// tests that promote one row directly.
    #[allow(dead_code)]
    pub fn promote(&self, row: &feed::FeedRow) -> Result<(), StoreError> {
        let conn = self.lock()?;
        feed::promote(&conn, row)
    }

    /// Returns a pair to `candidate` and deletes its `feed` row (BC55).
    /// Round 2 finding 5: the scorer itself now goes through
    /// `apply_verdicts` instead; this wrapper stays for tests.
    #[allow(dead_code)]
    pub fn demote(&self, quote_uri: &str) -> Result<(), StoreError> {
        let conn = self.lock()?;
        pairs::demote(&conn, quote_uri)
    }

    /// Marks a pair `dropped` with `reason` and deletes its `feed` row
    /// (BC56). Round 2 finding 5: the scorer itself now goes through
    /// `apply_verdicts` instead; this wrapper stays for tests.
    #[allow(dead_code)]
    pub fn drop_pair(&self, quote_uri: &str, reason: DropReason) -> Result<(), StoreError> {
        let conn = self.lock()?;
        pairs::drop_pair(&conn, quote_uri, reason)
    }

    /// TECH-DESIGN section 7.2 step 6 (BC58, BC59, BC60, BC61).
    pub fn expire(
        &self,
        now: i64,
        candidate_ttl_h: i64,
        feed_ttl_d: i64,
    ) -> Result<pairs::ExpireReport, StoreError> {
        let conn = self.lock()?;
        pairs::expire(&conn, now, candidate_ttl_h, feed_ttl_d)
    }

    /// Every `feed` row, `rank DESC, quote_cid ASC` (BC62, BC63).
    pub fn feed_rows(&self) -> Result<Vec<feed::FeedRow>, StoreError> {
        let conn = self.read_lock()?;
        feed::feed_rows(&conn)
    }

    /// Reads one `authors` row (BC64, BC66). No caller yet; story 10's
    /// guards are the first.
    #[allow(dead_code)]
    pub fn author_get(&self, did: &str) -> Result<Option<authors::AuthorRow>, StoreError> {
        let conn = self.read_lock()?;
        authors::author_get(&conn, did)
    }

    /// Inserts or replaces one `authors` row (BC65). No caller yet; story
    /// 10's guards are the first.
    #[allow(dead_code)]
    pub fn author_put(&self, row: &authors::AuthorRow) -> Result<(), StoreError> {
        let conn = self.lock()?;
        authors::author_put(&conn, row)
    }

    /// Clears the dirty flag on each row in `rows` only when its counts
    /// still match what the caller read at select time (round 2 finding 2,
    /// BC43, BC44, BC45). A URI with no `counts` row is skipped, not an
    /// error.
    pub fn clear_dirty_if_unchanged(
        &self,
        rows: &[(String, crate::score::Counts)],
    ) -> Result<(), StoreError> {
        let conn = self.lock()?;
        counts::clear_dirty_if_unchanged(&conn, rows)
    }

    /// Applies every `PairOutcome` inside one transaction (round 2 finding
    /// 5, BC48): a failure applies none of them.
    pub fn apply_verdicts(&self, outcomes: &[pairs::PairOutcome]) -> Result<(), StoreError> {
        let conn = self.lock()?;
        pairs::apply_verdicts(&conn, outcomes)
    }

    /// Reads one `meta` key. `Ok(None)` when the key has no row (BC67). No
    /// production caller yet: `last_scorer_pass` is write-only so far.
    #[allow(dead_code)]
    pub fn meta_get(&self, key: &str) -> Result<Option<String>, StoreError> {
        let conn = self.read_lock()?;
        meta::meta_get(&conn, key)
    }

    /// Inserts or replaces one `meta` key (BC67).
    pub fn meta_set(&self, key: &str, value: &str) -> Result<(), StoreError> {
        let conn = self.lock()?;
        meta::meta_set(&conn, key, value)
    }

    /// Starts the one writer thread with `WriterConfig::default()`
    /// (BC42). No production caller: `dunk run` always uses
    /// `writer_evicting` instead, so eviction is wired in from the start.
    #[allow(dead_code)]
    pub fn writer(&self) -> Result<writer::WriterHandle, StoreError> {
        self.writer_with(writer::WriterConfig::default())
    }

    /// Starts the one writer thread with a caller-supplied config. Tests
    /// use a smaller `max_ops` and `interval` than the default so a batch
    /// closes quickly. The thread shares this `Store`'s connection, so a
    /// `:memory:` test sees the writer's rows through the same `Store`. A
    /// second call on this `Store` (or a clone of it) is
    /// `StoreError::WriterAlreadyStarted`: one store has one writer thread
    /// (BC73). No production caller: `dunk run` always uses
    /// `writer_evicting`.
    #[allow(dead_code)]
    pub fn writer_with(
        &self,
        cfg: writer::WriterConfig,
    ) -> Result<writer::WriterHandle, StoreError> {
        if self.writer_started.swap(true, Ordering::SeqCst) {
            return Err(StoreError::WriterAlreadyStarted);
        }
        Ok(writer::spawn(Arc::clone(&self.conn), cfg, None))
    }

    /// Starts the one writer thread with `WriterConfig::default()`, wired to
    /// send every batch's evicted URIs (BC38) on `evict_tx`. Round 1
    /// finding 2: the one constructor `run` (`src/ingest/mod.rs`, slice 6.0)
    /// uses; `writer()` and `writer_with()` keep their signatures and start
    /// a writer with no eviction sender at all.
    pub fn writer_evicting(
        &self,
        evict_tx: tokio::sync::mpsc::UnboundedSender<Vec<String>>,
    ) -> Result<writer::WriterHandle, StoreError> {
        if self.writer_started.swap(true, Ordering::SeqCst) {
            return Err(StoreError::WriterAlreadyStarted);
        }
        Ok(writer::spawn(Arc::clone(&self.conn), writer::WriterConfig::default(), Some(evict_tx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_memory_migrates_to_current_version() {
        let store = Store::open_memory().unwrap();
        assert_eq!(schema::schema_version(&store.lock().unwrap()).unwrap(), Some(1));
    }

    #[test]
    fn rejects_newer_schema_version() {
        let conn = Connection::open_in_memory().unwrap();
        apply_pragmas(&conn).unwrap();
        schema::migrate(&conn).unwrap();
        conn.execute("UPDATE meta SET value = '999' WHERE key = 'schema_version'", []).unwrap();

        let err = schema::migrate(&conn).unwrap_err();
        match err {
            StoreError::SchemaTooNew { found, supported } => {
                assert_eq!(found, 999);
                assert_eq!(supported, schema::CURRENT_VERSION as u64);
            }
            other => panic!("expected SchemaTooNew, got {other:?}"),
        }
    }

    #[test]
    fn missing_parent_directory_is_open_error() {
        let err = Store::open_path("/dunk-store-test-nonexistent-dir/db.sqlite3").unwrap_err();
        match err {
            StoreError::Open { path, .. } => {
                assert_eq!(path, "/dunk-store-test-nonexistent-dir/db.sqlite3");
            }
            other => panic!("expected Open, got {other:?}"),
        }
        assert!(!std::path::Path::new("/dunk-store-test-nonexistent-dir").exists());
    }

    #[test]
    fn pragmas_are_set_on_open() {
        let store = Store::open_memory().unwrap();
        let conn = store.lock().unwrap();

        let journal_mode: String =
            conn.query_row("PRAGMA journal_mode", [], |row| row.get(0)).unwrap();
        assert_eq!(journal_mode, "memory"); // BC19: accepted for :memory:.

        let synchronous: i64 = conn.query_row("PRAGMA synchronous", [], |row| row.get(0)).unwrap();
        assert_eq!(synchronous, 1); // NORMAL

        let cache_size: i64 = conn.query_row("PRAGMA cache_size", [], |row| row.get(0)).unwrap();
        assert_eq!(cache_size, -65536);

        let foreign_keys: i64 =
            conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0)).unwrap();
        assert_eq!(foreign_keys, 1);

        let busy_timeout: i64 =
            conn.query_row("PRAGMA busy_timeout", [], |row| row.get(0)).unwrap();
        assert_eq!(busy_timeout, 5000);
    }

    #[test]
    fn drop_reason_round_trips_through_str() {
        use std::str::FromStr;
        for reason in [
            DropReason::SelfQuote,
            DropReason::NotAPost,
            DropReason::QuoteGone,
            DropReason::OriginalGone,
            DropReason::Detached,
            DropReason::Blocked,
            DropReason::Labelled,
            DropReason::AuthorInactive,
            DropReason::FollowerFloor,
        ] {
            assert_eq!(DropReason::from_str(reason.as_str()).unwrap(), reason);
        }
        assert!(DropReason::from_str("bogus").is_err());
    }

    #[test]
    fn unix_now_is_a_plausible_unix_second_timestamp() {
        // 2026-01-01T00:00:00Z, a loose floor that just rules out a
        // millisecond value or a bug that returns 0.
        assert!(unix_now() > 1_767_225_600);
    }

    #[test]
    fn cursor_is_none_on_a_fresh_store() {
        let store = Store::open_memory().unwrap();
        assert_eq!(store.cursor().unwrap(), None);
    }

    #[test]
    fn clear_dirty_if_unchanged_clears_the_flag_and_skips_an_unknown_uri() {
        let store = Store::open_memory().unwrap();
        {
            let conn = store.lock().unwrap();
            counts::incr(&conn, "at://post/1", writer::CountField::Likes, 1).unwrap();
        }

        // An unknown URI alongside a known, unchanged one is skipped, not an
        // error.
        let read = crate::score::Counts { likes: 1, reposts: 0, replies: 0 };
        store
            .clear_dirty_if_unchanged(&[
                ("at://post/1".to_string(), read),
                ("at://post/unknown".to_string(), crate::score::Counts::default()),
            ])
            .unwrap();

        let conn = store.lock().unwrap();
        let dirty: i64 = conn
            .query_row("SELECT dirty FROM counts WHERE post_uri = 'at://post/1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(dirty, 0);
    }

    #[test]
    fn meta_get_of_a_missing_key_is_none() {
        let store = Store::open_memory().unwrap();
        assert_eq!(store.meta_get("zstd_dict_id").unwrap(), None);
    }

    #[test]
    fn meta_set_then_meta_get_round_trips() {
        let store = Store::open_memory().unwrap();
        store.meta_set("zstd_dict_id", "abc123").unwrap();
        assert_eq!(store.meta_get("zstd_dict_id").unwrap(), Some("abc123".to_string()));
    }

    #[test]
    fn meta_set_twice_on_one_key_replaces_the_value() {
        let store = Store::open_memory().unwrap();
        store.meta_set("last_scorer_pass", "1").unwrap();
        store.meta_set("last_scorer_pass", "2").unwrap();
        assert_eq!(store.meta_get("last_scorer_pass").unwrap(), Some("2".to_string()));
    }

    #[test]
    fn file_store_reads_through_the_read_only_connection() {
        let nanos =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("dunk-store-read-only-{nanos}.sqlite3"));
        let path_str = path.to_str().unwrap().to_string();

        let store = Store::open_path(&path_str).unwrap();
        store.meta_set("zstd_dict_id", "abc123").unwrap();

        // Hold the writable connection's lock; a read that went through it
        // instead of the separate read-only connection would deadlock here
        // (BC74).
        let write_guard = store.conn.lock().unwrap();
        assert_eq!(store.meta_get("zstd_dict_id").unwrap(), Some("abc123".to_string()));
        drop(write_guard);

        let _ = std::fs::remove_file(&path_str);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }
}
