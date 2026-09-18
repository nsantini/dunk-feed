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

#![allow(dead_code)] // First callers are the ingest task (story 06) and the scorer (story 07).

pub mod authors;
pub mod counts;
pub mod feed;
pub mod interactions;
pub mod meta;
pub mod pairs;
pub mod schema;
pub mod writer;

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use thiserror::Error;

use crate::config::Config;

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

impl std::str::FromStr for PairState {
    type Err = StoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "candidate" => Ok(PairState::Candidate),
            "promoted" => Ok(PairState::Promoted),
            "dropped" => Ok(PairState::Dropped),
            _ => Err(StoreError::MalformedRow { table: "pairs", column: "state" }),
        }
    }
}

/// A `pairs.drop_reason` value, TECH-DESIGN section 8.3's ten reasons.
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
    Demoted,
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
            DropReason::Demoted => "demoted",
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
            "demoted" => Ok(DropReason::Demoted),
            _ => Err(StoreError::MalformedRow { table: "pairs", column: "drop_reason" }),
        }
    }
}

/// Unix seconds, `i64`. Every timestamp column in section 6 is this and
/// nothing else (BC68): no string date, no millisecond value.
pub fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Applies the pragmas TECH-DESIGN section 6 asks for, on every open
/// (BC18). On a `:memory:` path SQLite reports `journal_mode` as `memory`
/// instead of `wal`; that is accepted, not an error (BC19), and every other
/// pragma is still set as for a file.
fn apply_pragmas(conn: &Connection) -> Result<(), StoreError> {
    conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get::<_, String>(0))?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "cache_size", -65536)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "busy_timeout", 5000)?;
    Ok(())
}

/// The SQLite store. Holds one `Arc<Mutex<Connection>>`, shared by the
/// writer thread (slice 3.0) and every synchronous read: a second
/// connection to `:memory:` would be a second, empty database.
#[derive(Debug, Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

impl Store {
    /// Opens the database at `cfg.db_path`.
    pub fn open(cfg: &Config) -> Result<Self, StoreError> {
        Self::open_path(&cfg.db_path)
    }

    /// Opens a file database at `path`, applies the pragmas, and migrates
    /// it to `schema::CURRENT_VERSION`. A missing parent directory fails as
    /// `StoreError::Open` (BC13); the directory is never created.
    pub fn open_path(path: &str) -> Result<Self, StoreError> {
        let conn = Connection::open(path)
            .map_err(|source| StoreError::Open { path: path.to_string(), source })?;
        Self::from_connection(conn)
    }

    /// Opens an in-memory database. Tests use this so the writer thread and
    /// every read share the same connection.
    pub fn open_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> Result<Self, StoreError> {
        apply_pragmas(&conn)?;
        schema::migrate(&conn)?;
        Ok(Store { conn: Arc::new(Mutex::new(conn)) })
    }

    /// Locks the shared connection. `StoreError::Poisoned` if a previous
    /// holder panicked while holding it.
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
        self.conn.lock().map_err(|_| StoreError::Poisoned)
    }

    /// The stored Jetstream cursor, `meta.jetstream_seq` (BC43, BC44).
    pub fn cursor(&self) -> Result<Option<u64>, StoreError> {
        let conn = self.lock()?;
        meta::cursor(&conn)
    }

    /// Both URIs of every pair whose state is not `dropped` (BC45).
    pub fn hot_set_uris(&self) -> Result<Vec<String>, StoreError> {
        let conn = self.lock()?;
        Ok(pairs::hot_set_uris(&conn)?.collect())
    }

    /// `candidate` pairs inside `ttl_h` hours with a dirty counts row on
    /// either side (BC46, BC47, BC48).
    pub fn dirty_candidates(
        &self,
        now: i64,
        ttl_h: i64,
    ) -> Result<Vec<pairs::PairWithCounts>, StoreError> {
        let conn = self.lock()?;
        pairs::dirty_candidates(&conn, now, ttl_h)
    }

    /// `promoted` pairs whose `quoted_at` is at or after `now - h * 3600`
    /// (BC51).
    pub fn promoted_within(
        &self,
        now: i64,
        h: i64,
    ) -> Result<Vec<pairs::PairWithCounts>, StoreError> {
        let conn = self.lock()?;
        pairs::promoted_within(&conn, now, h)
    }

    /// Upserts a `feed` row and promotes its pair (BC52, BC53, BC54).
    pub fn promote(&self, row: &feed::FeedRow) -> Result<(), StoreError> {
        let conn = self.lock()?;
        feed::promote(&conn, row)
    }

    /// Returns a pair to `candidate` and deletes its `feed` row (BC55).
    pub fn demote(&self, quote_uri: &str) -> Result<(), StoreError> {
        let conn = self.lock()?;
        pairs::demote(&conn, quote_uri)
    }

    /// Marks a pair `dropped` with `reason` and deletes its `feed` row
    /// (BC56).
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
        let conn = self.lock()?;
        feed::feed_rows(&conn)
    }

    /// Reads one `authors` row (BC64, BC66).
    pub fn author_get(&self, did: &str) -> Result<Option<authors::AuthorRow>, StoreError> {
        let conn = self.lock()?;
        authors::author_get(&conn, did)
    }

    /// Inserts or replaces one `authors` row (BC65).
    pub fn author_put(&self, row: &authors::AuthorRow) -> Result<(), StoreError> {
        let conn = self.lock()?;
        authors::author_put(&conn, row)
    }

    /// Clears the dirty flag on each `post_uri`'s `counts` row. A URI with
    /// no counts row is skipped, not an error (BC50).
    pub fn clear_dirty(&self, post_uris: &[&str]) -> Result<(), StoreError> {
        let conn = self.lock()?;
        counts::clear_dirty(&conn, post_uris)
    }

    /// Reads one `meta` key. `Ok(None)` when the key has no row (BC67).
    pub fn meta_get(&self, key: &str) -> Result<Option<String>, StoreError> {
        let conn = self.lock()?;
        meta::meta_get(&conn, key)
    }

    /// Inserts or replaces one `meta` key (BC67).
    pub fn meta_set(&self, key: &str, value: &str) -> Result<(), StoreError> {
        let conn = self.lock()?;
        meta::meta_set(&conn, key, value)
    }

    /// Starts the one writer thread with `WriterConfig::default()`
    /// (BC42).
    pub fn writer(&self) -> writer::WriterHandle {
        self.writer_with(writer::WriterConfig::default())
    }

    /// Starts the one writer thread with a caller-supplied config. Tests
    /// use a smaller `max_ops` and `interval` than the default so a batch
    /// closes quickly. The thread shares this `Store`'s connection, so a
    /// `:memory:` test sees the writer's rows through the same `Store`.
    pub fn writer_with(&self, cfg: writer::WriterConfig) -> writer::WriterHandle {
        writer::spawn(Arc::clone(&self.conn), cfg)
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
    fn pair_state_round_trips_through_str() {
        use std::str::FromStr;
        for state in [PairState::Candidate, PairState::Promoted, PairState::Dropped] {
            assert_eq!(PairState::from_str(state.as_str()).unwrap(), state);
        }
        assert!(PairState::from_str("bogus").is_err());
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
            DropReason::Demoted,
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
    fn clear_dirty_clears_the_flag_and_skips_an_unknown_uri() {
        let store = Store::open_memory().unwrap();
        {
            let conn = store.lock().unwrap();
            counts::incr(&conn, "at://post/1", writer::CountField::Likes, 1).unwrap();
        }

        // An unknown URI alongside a known one is skipped, not an error.
        store.clear_dirty(&["at://post/1", "at://post/unknown"]).unwrap();

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
}
