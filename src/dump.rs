//! `dunk dump`, spec `2026-09-21-dump`. Reads every pair first seen inside
//! `--since`'s window through `Store::pairs_since` (slice 1.0), renders one
//! CSV row per pair with the columns `spec.md`'s "Answers from the
//! engineer" section names, and writes the file atomically. This is the
//! tool the runbook's tuning loop reads: the operator sorts the CSV and
//! re-fits `DUNK_P`, `DUNK_M` and the two weights against real rows.
//!
//! `E` and `D` are computed here with `score::engagement` and `score::ratio`,
//! the same functions the scorer runs, so the numbers an operator fits
//! against are the numbers the service computes, not a second formula.

use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::config::Config;
use crate::score::{self, Counts, Weights};
use crate::store::pairs::DumpRow;
use crate::store::{Store, StoreError};

/// `dunk dump`'s own errors. `main.rs` prints one line and exits 1 through
/// `CliError::Dump` (BC8). `BadSince` and `OutDirMissing` carry enough
/// context for that one line with no further lookup.
#[derive(Debug, Error)]
pub enum DumpError {
    /// BC2, BC10, BC11: `--since` does not match `^[0-9]+(h|d)$`, or its
    /// multiplication to seconds overflows `u64`.
    #[error(
        "invalid --since value {value:?}: expected a number followed by h or d, e.g. 24h or 7d"
    )]
    BadSince { value: String },
    /// BC4: `--out`'s parent directory does not exist. Checked before
    /// `Store::open` runs (BC9), so a bad path is reported even when the
    /// database is fine.
    #[error("the directory for --out {path:?} does not exist")]
    OutDirMissing { path: PathBuf },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// `run`'s result. `dispatch` prints `wrote <rows> rows to <path>` (BC19).
#[derive(Debug, Clone, PartialEq)]
pub struct DumpSummary {
    pub path: PathBuf,
    pub rows: usize,
}

/// Parses `--since` against `^[0-9]+(h|d)$` by hand (the spec's Non-goals
/// rule out a duration crate) and returns the window as a count of seconds.
/// `0h` is valid and returns `0` (BC10). The multiplication from digits to
/// seconds uses `checked_mul`, so a value with more digits than fit, or one
/// whose seconds overflow `u64`, is `BadSince` rather than a wrapped value
/// (BC11). No spaces, no sign, no other unit, no uppercase suffix (BC2).
/// Splits on the `h` or `d` suffix with `strip_suffix`, never by byte index
/// (BC22): a byte-index split panics on a value whose last character is more
/// than one byte, for example `24é`.
pub fn parse_since(value: &str) -> Result<u64, DumpError> {
    let bad = || DumpError::BadSince { value: value.to_string() };
    let (digits, seconds_per_unit) = if let Some(digits) = value.strip_suffix('h') {
        (digits, 3600u64)
    } else if let Some(digits) = value.strip_suffix('d') {
        (digits, 86400u64)
    } else {
        return Err(bad());
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let n: u64 = digits.parse().map_err(|_| bad())?;
    n.checked_mul(seconds_per_unit).ok_or_else(bad)
}

/// BC4: `--out`'s parent directory must already exist. `path.parent()` on a
/// bare filename like `dunk-dump-24h.csv` returns `Some("")`, which is the
/// current directory and always exists, so that case is not rejected.
fn check_out_dir(path: &Path) -> Result<(), DumpError> {
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    if parent.as_os_str().is_empty() || parent.exists() {
        Ok(())
    } else {
        Err(DumpError::OutDirMissing { path: path.to_path_buf() })
    }
}

/// The 27 CSV column headers, in the order `spec.md`'s "Answers from the
/// engineer" section names them.
const COLUMN_HEADERS: [&str; 27] = [
    "quote_uri",
    "original_uri",
    "quote_did",
    "original_did",
    "quoted_at",
    "first_seen_at",
    "state",
    "drop_reason",
    "likes_q",
    "reposts_q",
    "replies_q",
    "likes_o",
    "reposts_o",
    "replies_o",
    "local_e_q",
    "local_e_o",
    "local_d",
    "v_likes_q",
    "v_reposts_q",
    "v_replies_q",
    "v_likes_o",
    "v_reposts_o",
    "v_replies_o",
    "verified_e_q",
    "verified_e_o",
    "verified_d",
    "promoted_at",
];

/// An empty cell for a `None`.
fn fmt_opt_i64(value: Option<i64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

/// `{:.3}` (BC15), or an empty cell for a `None`.
fn fmt_opt_score(value: Option<f64>) -> String {
    value.map(|v| format!("{v:.3}")).unwrap_or_default()
}

/// The verified `Counts` one side's three `v_*` columns describe, or `None`
/// when any of the three is missing (BC13): all six of a row's `v_*`
/// columns are set together by `feed::promote`, so a `None` likes column
/// means the whole `feed` row is absent. Called once per side with that
/// side's three columns.
fn verified_counts(
    likes: Option<i64>,
    reposts: Option<i64>,
    replies: Option<i64>,
) -> Option<Counts> {
    Some(Counts {
        likes: u32::try_from(likes?).unwrap_or(u32::MAX),
        reposts: u32::try_from(reposts?).unwrap_or(u32::MAX),
        replies: u32::try_from(replies?).unwrap_or(u32::MAX),
    })
}

/// One `DumpRow`'s 27 fields, in `COLUMN_HEADERS`'s order. `local_e_q`,
/// `local_e_o` and `local_d` come from `row`'s own local counts (BC5: a
/// missing `counts` row already reads zero by the time `pairs_since` built
/// `row`). `verified_e_q`, `verified_e_o` and `verified_d` are recomputed
/// from the six `v_*` columns with `weights` and `k`, not read from a stored
/// ratio: the value `feed` stored at promotion time was computed against
/// whatever config was running then, and an operator refitting the running
/// config needs today's formula applied to the stored counts (BC14, BC15).
/// Both sets of three cells are empty together when `row` has no `feed` row
/// (BC13).
fn row_fields(row: &DumpRow, weights: &Weights, k: f64) -> [String; 27] {
    let local_e_q = score::engagement(&row.counts_q, weights);
    let local_e_o = score::engagement(&row.counts_o, weights);
    let local_d = score::ratio(local_e_q, local_e_o, k);

    let vq = verified_counts(row.v_likes_q, row.v_reposts_q, row.v_replies_q);
    let vo = verified_counts(row.v_likes_o, row.v_reposts_o, row.v_replies_o);
    let (verified_e_q, verified_e_o, verified_d) = match (vq, vo) {
        (Some(vq), Some(vo)) => {
            let eq = score::engagement(&vq, weights);
            let eo = score::engagement(&vo, weights);
            (Some(eq), Some(eo), Some(score::ratio(eq, eo, k)))
        }
        _ => (None, None, None),
    };

    [
        row.quote_uri.clone(),
        row.original_uri.clone(),
        row.quote_did.clone(),
        row.original_did.clone(),
        row.quoted_at.to_string(),
        row.first_seen_at.to_string(),
        row.state.clone(),
        row.drop_reason.clone().unwrap_or_default(),
        row.counts_q.likes.to_string(),
        row.counts_q.reposts.to_string(),
        row.counts_q.replies.to_string(),
        row.counts_o.likes.to_string(),
        row.counts_o.reposts.to_string(),
        row.counts_o.replies.to_string(),
        format!("{local_e_q:.3}"),
        format!("{local_e_o:.3}"),
        format!("{local_d:.3}"),
        fmt_opt_i64(row.v_likes_q),
        fmt_opt_i64(row.v_reposts_q),
        fmt_opt_i64(row.v_replies_q),
        fmt_opt_i64(row.v_likes_o),
        fmt_opt_i64(row.v_reposts_o),
        fmt_opt_i64(row.v_replies_o),
        fmt_opt_score(verified_e_q),
        fmt_opt_score(verified_e_o),
        fmt_opt_score(verified_d),
        fmt_opt_i64(row.promoted_at),
    ]
}

/// One `DumpRow` rendered as a CSV line, with every field through
/// `crate::csv::quote` (BC12). Kept as a pure function, separate from the
/// streaming write below, so a test can check one row's rendering without
/// touching a file.
fn render_row_line(row: &DumpRow, weights: &Weights, k: f64) -> String {
    let fields = row_fields(row, weights, k);
    let quoted: Vec<String> = fields.iter().map(|field| crate::csv::quote(field)).collect();
    quoted.join(",")
}

/// `<path>.tmp`, the file `write_csv_atomic` writes to before the rename.
fn tmp_path_for(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

/// Streams `rows` to `path` through a `BufWriter` instead of building one
/// `String` in memory first: the header line (BC7: printed even with zero
/// data rows), then one line per `DumpRow`, in the order `rows` is already
/// in (BC16: `pairs_since` orders ascending by `first_seen_at`, then
/// `quote_uri`). No `sync_all` is needed; the write is followed by a
/// rename, not relied on to survive a crash on its own.
fn write_csv_streamed(
    path: &Path,
    rows: &[DumpRow],
    weights: &Weights,
    k: f64,
) -> Result<(), DumpError> {
    use std::io::Write;
    let file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(file);
    writeln!(writer, "{}", COLUMN_HEADERS.join(","))?;
    for row in rows {
        writeln!(writer, "{}", render_row_line(row, weights, k))?;
    }
    writer.flush()?;
    Ok(())
}

/// Writes `rows` to `<path>.tmp` and renames it onto `path` (BC17, BC18): a
/// write that fails part way through leaves no partial file at `path`,
/// since nothing is ever written there directly, and a successful run
/// leaves no `.tmp` beside it, since the rename consumes it. A failed
/// rename removes the `.tmp` it left behind before returning the error; the
/// removal is best effort, so its own failure never replaces the error the
/// caller sees (BC17).
fn write_atomic(path: &Path, rows: &[DumpRow], weights: &Weights, k: f64) -> Result<(), DumpError> {
    let tmp_path = tmp_path_for(path);
    write_csv_streamed(&tmp_path, rows, weights, k)?;
    if let Err(err) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(DumpError::Io(err));
    }
    Ok(())
}

/// Runs `dunk dump` end to end. The three failure checks run in this order
/// (BC9): `--since` is parsed first, then `--out`'s parent directory is
/// checked, then the store is opened, so a bad `--since` is always
/// `BadSince`, even when `cfg.db_path` is unreadable too. `out` defaults to
/// `./dunk-dump-<since>.csv`, `<since>` the raw flag value (BC3's default
/// lives in `cli.rs`; this default serves a caller that passes `None`
/// directly). Reads go through `Store::pairs_since`, which uses the
/// read-only connection (BC21): `dump` never writes to the database.
pub fn run(cfg: &Config, since: &str, out: Option<PathBuf>) -> Result<DumpSummary, DumpError> {
    let offset_s = parse_since(since)?;
    let out_path = out.unwrap_or_else(|| PathBuf::from(format!("./dunk-dump-{since}.csv")));
    check_out_dir(&out_path)?;

    let store = Store::open(cfg)?;
    let now = crate::store::unix_now();
    let cutoff = now.saturating_sub(offset_s.min(i64::MAX as u64) as i64);
    let rows = store.pairs_since(cutoff)?;

    let weights = Weights::from(cfg);
    let k = f64::from(cfg.k);
    let row_count = rows.len();
    write_atomic(&out_path, &rows, &weights, k)?;

    Ok(DumpSummary { path: out_path, rows: row_count })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::store::{feed, pairs, DropReason};

    // --- parse_since -------------------------------------------------

    #[test]
    fn since_filters_correctly() {
        assert_eq!(parse_since("24h").unwrap(), 24 * 3600);
        assert_eq!(parse_since("7d").unwrap(), 7 * 86400);
        assert_eq!(parse_since("0h").unwrap(), 0);
    }

    #[test]
    fn malformed_since_fails_fast() {
        for bad in [
            "nope",
            "24",
            "24H",
            " 24h",
            "1w",
            "99999999999999999999h",
            "",
            "24é",
            "2\u{ff14}h",
            "h",
        ] {
            match parse_since(bad) {
                Err(DumpError::BadSince { value }) => assert_eq!(value, bad),
                other => panic!("expected BadSince for {bad:?}, got {other:?}"),
            }
        }
    }

    // BC9: a bad `--since` is reported before the store ever opens. A
    // `DUNK_DB_PATH` naming a directory that does not exist proves it: if
    // `run` reached `Store::open` first, this would be a `StoreError`
    // wrapped in `DumpError::Store`, not `BadSince`.
    #[test]
    fn malformed_since_never_opens_the_store() {
        let cfg = test_config("/no/such/directory/dunk.db");
        let result = run(&cfg, "nope", None);
        match result {
            Err(DumpError::BadSince { value }) => assert_eq!(value, "nope"),
            other => panic!("expected BadSince, got {other:?}"),
        }
    }

    // BC4, BC9: a missing `--out` parent directory fails before the store
    // opens too.
    #[test]
    fn missing_out_dir_fails_before_store_opens() {
        let cfg = test_config("/no/such/directory/dunk.db");
        let out = PathBuf::from("/no/such/out/dir/dump.csv");
        let result = run(&cfg, "24h", Some(out.clone()));
        match result {
            Err(DumpError::OutDirMissing { path }) => assert_eq!(path, out),
            other => panic!("expected OutDirMissing, got {other:?}"),
        }
    }

    // --- test fixtures -------------------------------------------------

    static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A fresh, unique SQLite file path under the OS temp directory, and a
    /// migrated connection to it: `run` opens its own `Store` against the
    /// same path, so seeding through a second connection and reading it
    /// back through `dunk dump`'s own code path is exactly what an operator
    /// does against a real database.
    struct TestDb {
        path: PathBuf,
    }

    impl TestDb {
        fn new() -> Self {
            let id = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("dunk-dump-test-{}-{id}.db", std::process::id()));
            let conn = rusqlite::Connection::open(&path).expect("open test db");
            crate::store::schema::migrate(&conn).expect("migrate test db");
            TestDb { path }
        }

        fn conn(&self) -> rusqlite::Connection {
            rusqlite::Connection::open(&self.path).expect("reopen test db")
        }

        fn path_str(&self) -> String {
            self.path.to_string_lossy().to_string()
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
            }
        }
    }

    fn test_config(db_path: &str) -> Config {
        let lookup = |name: &str| match name {
            "DUNK_HOSTNAME" => Some("feed.example.com".to_string()),
            "DUNK_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "DUNK_DB_PATH" => Some(db_path.to_string()),
            _ => None,
        };
        crate::config::load(lookup).expect("minimal config loads")
    }

    fn temp_csv_path(name: &str) -> PathBuf {
        let id = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dunk-dump-test-{name}-{}-{id}.csv", std::process::id()))
    }

    fn insert_pair(conn: &rusqlite::Connection, quote_uri: &str, original_uri: &str, at: i64) {
        pairs::insert_pair(
            conn,
            quote_uri,
            "did:plc:q",
            "quote-cid",
            original_uri,
            "did:plc:o",
            at,
            at,
        )
        .unwrap();
    }

    fn insert_feed_row(conn: &rusqlite::Connection, quote_uri: &str) {
        feed::promote(
            conn,
            &feed::FeedRow {
                quote_uri: quote_uri.to_string(),
                quote_cid: "quote-cid".to_string(),
                quote_did: "did:plc:q".to_string(),
                original_did: "did:plc:o".to_string(),
                quoted_at: 1_700_000_000,
                v_likes_q: 5,
                v_reposts_q: 1,
                v_replies_q: 0,
                v_likes_o: 2,
                v_reposts_o: 0,
                v_replies_o: 0,
                ratio: 1.0,
                rank: 1.0,
                promoted_at: 1_700_000_000,
                verified_at: 1_700_000_000,
            },
        )
        .unwrap();
    }

    // --- run -------------------------------------------------------

    // AC1, BC1: `--since 24h` includes only pairs from the last 24 hours.
    #[test]
    fn since_filters_correctly_end_to_end() {
        let db = TestDb::new();
        let conn = db.conn();
        let now = crate::store::unix_now();
        insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/recent",
            "at://did:plc:o/app.bsky.feed.post/1",
            now - 3600,
        );
        insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/old",
            "at://did:plc:o/app.bsky.feed.post/2",
            now - 2 * 86400,
        );
        drop(conn);

        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("since");
        let summary = run(&cfg, "24h", Some(out.clone())).expect("run succeeds");

        assert_eq!(summary.rows, 1);
        let contents = std::fs::read_to_string(&out).unwrap();
        assert!(contents.contains("recent"));
        assert!(!contents.contains("/old"));
        let _ = std::fs::remove_file(&out);
    }

    // AC2: a malformed `--since` exits before touching the store, already
    // proven by `malformed_since_never_opens_the_store` above; this test
    // proves the same through the public `run` entry point with a database
    // that does exist, so a store error is not what would mask the result.
    #[test]
    fn malformed_since_fails_before_writing_anything() {
        let db = TestDb::new();
        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("malformed");

        let result = run(&cfg, "nope", Some(out.clone()));

        assert!(matches!(result, Err(DumpError::BadSince { .. })));
        assert!(!out.exists());
        assert!(!PathBuf::from(format!("{}.tmp", out.display())).exists());
    }

    // AC3, BC5: a pair with no `counts` row on either side dumps as zero,
    // not an error.
    #[test]
    fn missing_counts_defaults_to_zero() {
        let db = TestDb::new();
        let conn = db.conn();
        let now = crate::store::unix_now();
        insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/q1",
            "at://did:plc:o/app.bsky.feed.post/o1",
            now,
        );
        drop(conn);

        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("zero-counts");
        run(&cfg, "24h", Some(out.clone())).expect("run succeeds");

        let contents = std::fs::read_to_string(&out).unwrap();
        let data_line = contents.lines().nth(1).expect("one data row");
        let fields: Vec<&str> = data_line.split(',').collect();
        // likes_q, reposts_q, replies_q, likes_o, reposts_o, replies_o
        assert_eq!(&fields[8..14], ["0", "0", "0", "0", "0", "0"]);
        let _ = std::fs::remove_file(&out);
    }

    // AC4, BC6: `state` and `drop_reason` are CSV columns, and a dropped
    // pair carries its reason.
    #[test]
    fn csv_has_state_and_reason() {
        let db = TestDb::new();
        let conn = db.conn();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/dropped";
        let now = crate::store::unix_now();
        insert_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1", now);
        pairs::drop_pair(&conn, quote_uri, DropReason::FollowerFloor).unwrap();
        drop(conn);

        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("state-reason");
        run(&cfg, "24h", Some(out.clone())).expect("run succeeds");

        let contents = std::fs::read_to_string(&out).unwrap();
        let header = contents.lines().next().unwrap();
        assert!(header.contains("state"));
        assert!(header.contains("drop_reason"));
        let data_line = contents.lines().nth(1).unwrap();
        assert!(data_line.contains("dropped"));
        assert!(data_line.contains("follower_floor"));
        let _ = std::fs::remove_file(&out);
    }

    // AC5, BC7: zero matching pairs still produces a valid header-only CSV.
    #[test]
    fn empty_window_writes_header_only() {
        let db = TestDb::new();
        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("empty");

        let summary = run(&cfg, "24h", Some(out.clone())).expect("run succeeds");

        assert_eq!(summary.rows, 0);
        let contents = std::fs::read_to_string(&out).unwrap();
        assert_eq!(contents.lines().count(), 1);
        assert_eq!(contents.lines().next().unwrap(), COLUMN_HEADERS.join(","));
        let _ = std::fs::remove_file(&out);
    }

    // BC13, BC14: verified cells are empty for a candidate and filled for a
    // promoted row, recomputed from the stored `v_*` counts.
    #[test]
    fn verified_cells_empty_for_candidate_filled_for_promoted() {
        let db = TestDb::new();
        let conn = db.conn();
        let candidate = "at://did:plc:q/app.bsky.feed.post/candidate";
        let promoted = "at://did:plc:q/app.bsky.feed.post/promoted";
        let now = crate::store::unix_now();
        insert_pair(&conn, candidate, "at://did:plc:o/app.bsky.feed.post/o1", now);
        insert_pair(&conn, promoted, "at://did:plc:o/app.bsky.feed.post/o2", now);
        insert_feed_row(&conn, promoted);
        conn.execute("UPDATE pairs SET state = 'promoted' WHERE quote_uri = ?1", [promoted])
            .unwrap();
        drop(conn);

        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("verified-cells");
        run(&cfg, "24h", Some(out.clone())).expect("run succeeds");

        let contents = std::fs::read_to_string(&out).unwrap();
        let mut lines = contents.lines().skip(1);
        let candidate_line = lines.next().unwrap();
        let promoted_line = lines.next().unwrap();
        let candidate_fields: Vec<&str> = candidate_line.split(',').collect();
        let promoted_fields: Vec<&str> = promoted_line.split(',').collect();
        // v_likes_q .. promoted_at are columns 17..27 (0-indexed).
        for field in &candidate_fields[17..27] {
            assert_eq!(*field, "", "candidate row should have empty verified cells");
        }
        assert_eq!(promoted_fields[17], "5"); // v_likes_q
        assert_ne!(promoted_fields[23], ""); // verified_e_q
        assert_ne!(promoted_fields[24], ""); // verified_e_o
        assert_ne!(promoted_fields[25], ""); // verified_d
        assert_ne!(promoted_fields[26], ""); // promoted_at
        let _ = std::fs::remove_file(&out);
    }

    // BC16: rows come back ordered by `first_seen_at`, the CSV keeps that
    // order.
    #[test]
    fn rows_are_ordered_by_first_seen_at() {
        let db = TestDb::new();
        let conn = db.conn();
        insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/second",
            "at://did:plc:o/app.bsky.feed.post/o2",
            100,
        );
        insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/first",
            "at://did:plc:o/app.bsky.feed.post/o1",
            50,
        );
        drop(conn);

        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("ordering");
        run(&cfg, "1000000h", Some(out.clone())).expect("run succeeds");

        let contents = std::fs::read_to_string(&out).unwrap();
        let mut lines = contents.lines().skip(1);
        assert!(lines.next().unwrap().contains("/first"));
        assert!(lines.next().unwrap().contains("/second"));
        let _ = std::fs::remove_file(&out);
    }

    // BC17, BC18: a failed write leaves no partial CSV, and a successful
    // one leaves no `.tmp` file beside it.
    #[test]
    fn write_atomic_leaves_no_tmp_on_success_and_no_output_on_failure() {
        let weights = Weights { repost: 1.0, reply: 1.0 };
        let out = temp_csv_path("atomic-success");
        write_atomic(&out, &[], &weights, 1.0).expect("write succeeds");
        assert!(out.exists());
        assert!(!PathBuf::from(format!("{}.tmp", out.display())).exists());
        let _ = std::fs::remove_file(&out);

        // Simulate a failed write by making the `.tmp` path a directory:
        // `std::fs::File::create` on it fails with an `Io` error, and
        // `write_atomic` never reaches the rename, so `out` itself is never
        // created.
        let out = temp_csv_path("atomic-failure");
        let tmp_path = PathBuf::from(format!("{}.tmp", out.display()));
        std::fs::create_dir(&tmp_path).unwrap();
        let result = write_atomic(&out, &[], &weights, 1.0);
        assert!(matches!(result, Err(DumpError::Io(_))));
        assert!(!out.exists());
        let _ = std::fs::remove_dir(&tmp_path);
    }

    // BC17: a failed rename removes the `.tmp` it left behind, so no
    // partial file survives at either path. Making the destination path a
    // directory forces `std::fs::rename` to fail after the `.tmp` file was
    // written successfully.
    #[test]
    fn write_atomic_removes_tmp_when_rename_fails() {
        let weights = Weights { repost: 1.0, reply: 1.0 };
        let out = temp_csv_path("atomic-rename-failure");
        std::fs::create_dir(&out).unwrap();
        let tmp_path = PathBuf::from(format!("{}.tmp", out.display()));

        let result = write_atomic(&out, &[], &weights, 1.0);

        assert!(matches!(result, Err(DumpError::Io(_))));
        assert!(!tmp_path.exists(), "the .tmp file must be removed after a failed rename");
        let _ = std::fs::remove_dir(&out);
    }

    // BC2, BC11: the full `--since` table the story names.
    #[test]
    fn since_table_is_exhaustive() {
        let cases: [(&str, Option<u64>); 12] = [
            ("24h", Some(24 * 3600)),
            ("7d", Some(7 * 86400)),
            ("0h", Some(0)),
            ("nope", None),
            ("24", None),
            ("24H", None),
            (" 24h", None),
            ("1w", None),
            ("99999999999999999999h", None),
            ("24é", None),
            ("2\u{ff14}h", None),
            ("h", None),
        ];
        for (input, expected) in cases {
            match (parse_since(input), expected) {
                (Ok(got), Some(want)) => assert_eq!(got, want, "for {input:?}"),
                (Err(DumpError::BadSince { .. }), None) => {}
                (result, expected) => {
                    panic!("mismatch for {input:?}: got {result:?}, expected {expected:?}")
                }
            }
        }
    }

    // The default `--out` path is `./dunk-dump-<since>.csv` for the value
    // passed, when the caller passes `None`. This runs against the real
    // process working directory rather than switching it, since
    // `std::env::set_current_dir` is process-global and would race other
    // tests running in parallel threads; the default path itself, not
    // where it lands, is what BC and the task ask this test to prove.
    #[test]
    fn default_out_path_uses_the_since_value() {
        let db = TestDb::new();
        let cfg = test_config(&db.path_str());
        let expected = PathBuf::from("./dunk-dump-71h.csv"); // an unlikely-to-collide since value

        let summary = run(&cfg, "71h", None).expect("run succeeds");

        assert_eq!(summary.path, expected);
        assert!(expected.exists());
        let _ = std::fs::remove_file(&expected);
    }

    // The header holds the 27 columns in the exact order `spec.md`'s
    // "Answers from the engineer" section names.
    #[test]
    fn header_holds_the_27_columns_in_spec_order() {
        let expected = [
            "quote_uri",
            "original_uri",
            "quote_did",
            "original_did",
            "quoted_at",
            "first_seen_at",
            "state",
            "drop_reason",
            "likes_q",
            "reposts_q",
            "replies_q",
            "likes_o",
            "reposts_o",
            "replies_o",
            "local_e_q",
            "local_e_o",
            "local_d",
            "v_likes_q",
            "v_reposts_q",
            "v_replies_q",
            "v_likes_o",
            "v_reposts_o",
            "v_replies_o",
            "verified_e_q",
            "verified_e_o",
            "verified_d",
            "promoted_at",
        ];
        assert_eq!(COLUMN_HEADERS, expected);
        assert_eq!(COLUMN_HEADERS.len(), 27);
    }

    // BC1: a row on the cutoff second is included; the second before it is
    // not.
    #[test]
    fn row_on_cutoff_second_is_included_second_before_is_not() {
        let db = TestDb::new();
        let conn = db.conn();
        let now = crate::store::unix_now();
        let cutoff = now - 24 * 3600;
        insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/on-cutoff",
            "at://did:plc:o/app.bsky.feed.post/o1",
            cutoff,
        );
        insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/before-cutoff",
            "at://did:plc:o/app.bsky.feed.post/o2",
            cutoff - 1,
        );
        drop(conn);

        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("cutoff-boundary");
        let summary = run(&cfg, "24h", Some(out.clone())).expect("run succeeds");

        assert_eq!(summary.rows, 1);
        let contents = std::fs::read_to_string(&out).unwrap();
        assert!(contents.contains("on-cutoff"));
        assert!(!contents.contains("before-cutoff"));
        let _ = std::fs::remove_file(&out);
    }

    // BC15: `local_e_q`, `local_e_o` and `local_d` are formatted to three
    // decimals, even when the underlying value is a whole number.
    #[test]
    fn local_scores_are_formatted_to_three_decimals() {
        let db = TestDb::new();
        let conn = db.conn();
        let now = crate::store::unix_now();
        insert_pair(
            &conn,
            "at://did:plc:q/app.bsky.feed.post/decimals",
            "at://did:plc:o/app.bsky.feed.post/o1",
            now,
        );
        drop(conn);

        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("decimals");
        run(&cfg, "24h", Some(out.clone())).expect("run succeeds");

        let contents = std::fs::read_to_string(&out).unwrap();
        let data_line = contents.lines().nth(1).expect("one data row");
        let fields: Vec<&str> = data_line.split(',').collect();
        // local_e_q, local_e_o, local_d are columns 14..17 (0-indexed): with
        // no counts row on either side both engagements are 0.0, so both
        // format as "0.000", and the ratio at k=0 counts is also "0.000".
        for field in &fields[14..17] {
            assert!(field.contains('.'), "expected a decimal point in {field:?}");
            let decimals = field.split('.').nth(1).unwrap();
            assert_eq!(decimals.len(), 3, "expected three decimal digits in {field:?}");
        }
        let _ = std::fs::remove_file(&out);
    }

    // BC12: a `quote_uri` holding a comma is quoted in the rendered line.
    #[test]
    fn quote_uri_with_a_comma_is_quoted() {
        let db = TestDb::new();
        let conn = db.conn();
        let now = crate::store::unix_now();
        let quote_uri = "at://did:plc:q/app.bsky.feed.post/has,comma";
        insert_pair(&conn, quote_uri, "at://did:plc:o/app.bsky.feed.post/o1", now);
        drop(conn);

        let cfg = test_config(&db.path_str());
        let out = temp_csv_path("comma-quoted");
        run(&cfg, "24h", Some(out.clone())).expect("run succeeds");

        let contents = std::fs::read_to_string(&out).unwrap();
        let data_line = contents.lines().nth(1).expect("one data row");
        assert!(
            data_line.starts_with(&format!("\"{quote_uri}\",")),
            "expected the quote_uri field quoted, got: {data_line}"
        );
        let _ = std::fs::remove_file(&out);
    }
}
