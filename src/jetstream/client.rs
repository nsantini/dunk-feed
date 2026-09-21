//! Dictionary fetch and cache, URL build, frame decode, and the
//! `JetstreamClient` connect/reconnect/resume loop that hands a caller
//! typed [`Event`]s.
//!
//! The zstd dictionary is fetched once and its `zstd::bulk::Decompressor` is
//! built once, both cached on disk and in memory next to `cfg.db_path`,
//! because re-building a decompression context from a 65,536-byte dictionary
//! per frame would cost about 400 times a second at the measured live rate
//! (TECH-DESIGN section 5.1's "Approach"). A cold start with no cache and an
//! unreachable dictionary endpoint is not fatal: `Dictionary` is caught
//! here, logged at `warn`, and the caller connects with no
//! `zstdDictionary` parameter, so frames arrive as text (BC19). Running
//! this way is "degraded": `is_compressed()` and `dictionary_id()` say so,
//! and the fetch is retried on every reconnect and on a 10-minute timer
//! (BC39, BC40).
//!
//! `JetstreamClient::next` hides every reconnect: on a closed socket, a
//! transport error, or a failed connect (including the first one, BC31) it
//! backs off `1s, 2s, 4s, ... 60s`, rotates to the next configured host
//! (BC30), and resumes at `last_seq + 1` (TECH-DESIGN section 5.4's
//! inclusive-resume rule), so the caller only ever sees the next `Event`,
//! never the gap.

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::StreamExt;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use zstd::bulk::Decompressor;

use crate::config::Config;
use crate::jetstream::event::{Event, Frame, Payload};

/// The four collections this story cares about, TECH-DESIGN section 5.1.
/// Held in this fixed order so `subscribe_url`'s query string is stable and
/// a test can assert the whole thing.
const COLLECTIONS: [&str; 4] =
    ["app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost", "app.bsky.feed.postgate"];

/// The prefix and suffix of one dictionary cache file, `zstd-dict-<id>.bin`,
/// in the directory that holds `cfg.db_path` (BC35, BC36). There is no
/// separate pointer file: the file with the highest id wins on load.
const DICT_FILE_PREFIX: &str = "zstd-dict-";
const DICT_FILE_SUFFIX: &str = ".bin";

/// The HTTP header the dictionary endpoint returns the dictionary id on.
const DICT_ID_HEADER: &str = "x-zstd-dictionary-id";

/// The timeout every dictionary HTTP request carries (BC34), the same
/// timeout `appview/mod.rs`'s `REQUEST_TIMEOUT` uses. One `reqwest::Client`
/// built with it is kept for the life of a `JetstreamClient`, never rebuilt
/// per request.
const DICTIONARY_TIMEOUT: Duration = Duration::from_secs(10);

/// How often `next()` retries a dictionary fetch while degraded with no
/// dictionary in hand (BC39), independent of the retry that already
/// happens on every reconnect.
const DICT_RETRY_INTERVAL: Duration = Duration::from_secs(600);

/// The cap `decompress` grows its output buffer to before giving up on one
/// frame, so a genuinely corrupt frame cannot grow the buffer without
/// bound.
const MAX_DECOMPRESS_CAP: usize = 16 * 1024 * 1024;

/// Every way this module's work can fail. Every variant is caught inside
/// `JetstreamClient`'s own connect and reconnect loops, except a
/// caller-fatal configuration error; this story raises no other
/// caller-fatal variant. The caller sees one log line per skipped frame and
/// one per reconnect (BC21), never this type itself. Carries no variant
/// that nothing constructs (BC46): the earlier `Socket` variant was never
/// raised, since a socket error is logged and retried inline, so it is
/// gone.
#[derive(Debug, Error)]
pub enum JetstreamError {
    /// The dictionary was neither cached nor fetchable: no cache on disk,
    /// the fetch failed, or the response carried no `x-zstd-dictionary-id`
    /// header (BC19).
    #[error("dictionary unavailable: {0}")]
    Dictionary(String),
    /// A frame did not decode: a corrupt zstd frame (BC9) or a payload that
    /// failed its `serde` shape, such as an unrecognised `operation` (BC8).
    #[error("frame decode failed: {0}")]
    Decode(String),
    /// The configuration was bad, for example an empty host list (BC32).
    /// The only caller-fatal variant `connect` raises.
    #[error("connect failed: {0}")]
    Connect(String),
}

/// A prepared zstd dictionary: the id Jetstream tags it with, and the
/// `zstd::bulk::Decompressor` built from its bytes once at connect (or
/// dictionary retry) with `Decompressor::with_dictionary`, which copies the
/// dictionary bytes into the decompression context and needs no borrow, so a
/// frame never re-builds it (TECH-DESIGN section 5.1's "Approach").
pub struct Dictionary {
    pub id: String,
    decompressor: Decompressor<'static>,
}

impl Dictionary {
    /// Returns the directory `db_path` lives in, or the current directory
    /// when `db_path` names a bare file with no parent.
    fn cache_dir(db_path: &str) -> PathBuf {
        Path::new(db_path).parent().map(Path::to_path_buf).unwrap_or_default()
    }

    /// Loads the dictionary from disk if a cache file is present (BC17, no
    /// HTTP call), otherwise fetches it from `host`'s dictionary endpoint
    /// and writes the cache (BC18). A write failure warns and still uses
    /// the fetched bytes for this run (BC20). Returns `JetstreamError::
    /// Dictionary` when neither the cache nor a fetch produced one (BC19),
    /// which the caller catches and treats as "connect with no dictionary".
    async fn load_or_fetch(
        http: &reqwest::Client,
        cache_dir: &Path,
        host: &str,
    ) -> Result<Self, JetstreamError> {
        if let Some(dict) = Self::read_cache(cache_dir) {
            return Ok(dict);
        }
        let (id, bytes) = Self::fetch(http, host).await?;
        Self::write_cache(cache_dir, &id, &bytes);
        let decompressor = Decompressor::with_dictionary(&bytes)
            .map_err(|err| JetstreamError::Dictionary(err.to_string()))?;
        Ok(Self { decompressor, id })
    }

    /// Reads every `zstd-dict-<id>.bin` file in `dir` and keeps the one
    /// with the highest id (BC36); there is no pointer file. `None` on any
    /// miss: the directory is unreadable, holds no matching file, or the
    /// winning file is empty or unreadable (BC37), which sends the caller
    /// to `fetch` instead of failing outright.
    fn read_cache(dir: &Path) -> Option<Self> {
        let entries = std::fs::read_dir(dir).ok()?;
        let mut best: Option<(u64, PathBuf)> = None;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(id_str) =
                name.strip_prefix(DICT_FILE_PREFIX).and_then(|s| s.strip_suffix(DICT_FILE_SUFFIX))
            else {
                continue;
            };
            let Ok(id_num) = id_str.parse::<u64>() else { continue };
            if best.as_ref().is_none_or(|(current, _)| id_num > *current) {
                best = Some((id_num, entry.path()));
            }
        }
        let (id_num, path) = best?;
        let bytes = std::fs::read(&path).ok()?;
        if bytes.is_empty() {
            return None;
        }
        let decompressor = Decompressor::with_dictionary(&bytes).ok()?;
        Some(Self { decompressor, id: id_num.to_string() })
    }

    /// Writes `zstd-dict-<id>.bin` to `dir`: a `.tmp` name first, then
    /// renamed, so a reader never sees a part-written file (BC35). Every
    /// older `zstd-dict-*.bin` file is deleted after the rename succeeds
    /// (BC35). Logged at `warn` and otherwise ignored on failure (BC20): an
    /// unwritable cache directory does not fail the caller, which already
    /// has the bytes it needs for this run.
    fn write_cache(dir: &Path, id: &str, bytes: &[u8]) {
        if let Err(err) = std::fs::create_dir_all(dir) {
            tracing::warn!(error = %err, dir = %dir.display(), "jetstream: could not create dictionary cache dir");
            return;
        }
        let final_path = dir.join(format!("{DICT_FILE_PREFIX}{id}{DICT_FILE_SUFFIX}"));
        let tmp_path = dir.join(format!("{DICT_FILE_PREFIX}{id}{DICT_FILE_SUFFIX}.tmp"));
        if let Err(err) = std::fs::write(&tmp_path, bytes) {
            tracing::warn!(error = %err, "jetstream: could not write dictionary cache file");
            return;
        }
        if let Err(err) = std::fs::rename(&tmp_path, &final_path) {
            tracing::warn!(error = %err, "jetstream: could not rename dictionary cache file");
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        let keep = final_path.file_name();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let lossy = name.to_string_lossy();
            if lossy.starts_with(DICT_FILE_PREFIX)
                && lossy.ends_with(DICT_FILE_SUFFIX)
                && Some(name.as_os_str()) != keep
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    /// Deletes `dir`'s `zstd-dict-<id>.bin` cache file (BC38): called when
    /// three consecutive decompression failures discard a dictionary, so a
    /// restart does not load the same bad cache back in.
    fn delete_cache_file(dir: &Path, id: &str) {
        let _ = std::fs::remove_file(dir.join(format!("{DICT_FILE_PREFIX}{id}{DICT_FILE_SUFFIX}")));
    }

    /// Fetches the dictionary bytes and id from `host`'s HTTP dictionary
    /// endpoint over `http`, a client built once with a 10s timeout
    /// (BC34). `Dictionary` on a transport failure, a non-success status,
    /// or a response with no `x-zstd-dictionary-id` header (BC19).
    async fn fetch(
        http: &reqwest::Client,
        host: &str,
    ) -> Result<(String, Vec<u8>), JetstreamError> {
        let url = dictionary_url(host);
        let response = http
            .get(&url)
            .send()
            .await
            .map_err(|err| JetstreamError::Dictionary(format!("fetching {url}: {err}")))?;
        if !response.status().is_success() {
            return Err(JetstreamError::Dictionary(format!(
                "fetching {url}: status {}",
                response.status()
            )));
        }
        let id = response
            .headers()
            .get(DICT_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| {
                JetstreamError::Dictionary(format!(
                    "response from {url} carried no {DICT_ID_HEADER} header"
                ))
            })?;
        let bytes =
            response.bytes().await.map_err(|err| JetstreamError::Dictionary(err.to_string()))?;
        Ok((id, bytes.to_vec()))
    }
}

/// Builds the dictionary endpoint URL from a `wss://` or `ws://` Jetstream
/// host, swapping the scheme for `https://`/`http://`, TECH-DESIGN section
/// 5.1. This is the v2 `network.bsky.jetstream.getZstdDictionary` XRPC
/// endpoint, verified live on 2026-09-18; the v1 `/subscribe/zstd-dictionary`
/// path is rejected outright (TECH-DESIGN section 12, D4).
fn dictionary_url(host: &str) -> String {
    let base = host.replacen("wss://", "https://", 1).replacen("ws://", "http://", 1);
    format!("{base}/xrpc/network.bsky.jetstream.getZstdDictionary")
}

/// Builds the `subscribeEvents` URL: the four collections in
/// [`COLLECTIONS`] order as repeated `collections` parameters, `kinds=commit`,
/// `zstdDictionary=<id>` when `dict_id` is given, and `cursor=<cursor>` when
/// `cursor` is given (BC22). This is the v2 XRPC path
/// `/xrpc/network.bsky.jetstream.subscribeEvents`, verified live on
/// 2026-09-18; the v1 `/subscribe` path and its `wantedCollections`
/// parameter are rejected outright (TECH-DESIGN section 12, D4). Pure, so a
/// test can assert the whole string with no network involved.
pub fn subscribe_url(host: &str, dict_id: Option<&str>, cursor: Option<u64>) -> String {
    let mut url = format!("{host}/xrpc/network.bsky.jetstream.subscribeEvents?");
    for collection in COLLECTIONS {
        url.push_str("collections=");
        url.push_str(collection);
        url.push('&');
    }
    url.push_str("kinds=commit");
    if let Some(id) = dict_id {
        url.push_str("&zstdDictionary=");
        url.push_str(id);
    }
    if let Some(cursor) = cursor {
        url.push_str("&cursor=");
        url.push_str(&cursor.to_string());
    }
    url
}

/// Decodes one WebSocket message into a [`Frame`]. `binary` is `true` for a
/// binary message, decompressed against `dict` first (BC9); `false` for a
/// text message, parsed as JSON directly (BC23, BC45). `dict` is `None`
/// when no dictionary is in hand, in which case a binary message is parsed
/// as raw JSON bytes with no decompression, the same as a text message.
/// `JetstreamError::Decode` on a corrupt zstd frame or a payload that fails
/// its shape; `JetstreamClient` catches this, logs at `warn`, and reads the
/// next frame rather than treating it as fatal (BC8, BC9).
pub fn decode_frame(
    data: &[u8],
    binary: bool,
    dict: Option<&mut Dictionary>,
) -> Result<Frame, JetstreamError> {
    match (binary, dict) {
        (true, Some(dict)) => {
            let json = decompress(data, &mut dict.decompressor)?;
            serde_json::from_slice(&json).map_err(|err| JetstreamError::Decode(err.to_string()))
        }
        _ => serde_json::from_slice(data).map_err(|err| JetstreamError::Decode(err.to_string())),
    }
}

/// Decompresses one zstd frame against the dictionary's already-built
/// decompression context (BC44): `decompressor` is built once per
/// [`Dictionary`], in [`Dictionary::load_or_fetch`] or
/// [`Dictionary::read_cache`], so this call only ever decompresses, never
/// re-builds it. `decompress(data, cap)` starts at about four times the
/// compressed length, growing and retrying when the buffer was too small.
/// `Decode` on a malformed or corrupt frame, or on a frame that still does
/// not fit under [`MAX_DECOMPRESS_CAP`]: never a panic (BC9).
fn decompress(
    data: &[u8],
    decompressor: &mut Decompressor<'static>,
) -> Result<Vec<u8>, JetstreamError> {
    let mut cap = (data.len().saturating_mul(4)).max(4096);
    let mut last_err = None;
    while cap <= MAX_DECOMPRESS_CAP {
        match decompressor.decompress(data, cap) {
            Ok(bytes) => return Ok(bytes),
            Err(err) => {
                last_err = Some(err);
                cap = cap.saturating_mul(2);
            }
        }
    }
    Err(JetstreamError::Decode(
        last_err
            .map(|err| err.to_string())
            .unwrap_or_else(|| "decompressed frame exceeds the size cap".to_string()),
    ))
}

/// Turns one decoded [`Frame`] into the [`Event`] a caller of
/// [`JetstreamClient::next`] sees, or `None` when the loop should read the
/// next frame instead. A non-`"message"` envelope is ignored (BC1's loop
/// half); a `#commit` advances `last_seq` and returns (BC2); a `#info`
/// returns as-is, `last_seq` untouched, since an info frame carries no `seq`
/// (BC3); `#identity`, `#account`, `#sync` and `Other` are consumed
/// internally (BC4). Pure and free of I/O, so the loop it drives is
/// testable with no socket in play. `JetstreamClient::dispatch_frame` wraps
/// this to add the `last_seq` monotonicity clamp (BC43) and the pending
/// resume cursor (BC41, BC42), which both need per-connection state this
/// free function does not carry.
fn dispatch(frame: Frame, last_seq: &mut Option<u64>) -> Option<Event> {
    if frame.kind != "message" {
        return None;
    }
    match frame.payload {
        Payload::Commit(commit) => {
            *last_seq = Some(commit.seq);
            Some(Event::Commit(commit))
        }
        Payload::Info { name, message } => Some(Event::Info { name, message }),
        Payload::Identity {} | Payload::Account {} | Payload::Sync {} | Payload::Other => None,
    }
}

/// The reconnect backoff for a given attempt, TECH-DESIGN section 5.1's
/// "Approach": `1, 2, 4, 8, 16, 32` seconds, capped at `60` from attempt 6
/// onward (BC13). `checked_shl` rather than a plain `<<` so an attempt count
/// far past the cap saturates instead of overflowing or panicking.
pub fn backoff_delay(attempt: u32) -> Duration {
    let secs = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    Duration::from_secs(secs.min(60))
}

/// The cursor a reconnect (or the first connect) should resume at,
/// TECH-DESIGN section 5.4's inclusive-resume rule. `last_seq + 1` once an
/// event has been seen, never `last_seq` itself (BC14); before that, the
/// cursor `connect` was originally given, unchanged: `None` starts at the
/// head (BC15), `Some(n)` resumes at the caller's own checkpoint (BC16).
/// `JetstreamClient::reconnect` overrides this with `None` while a pending
/// resume cursor was cleared by an `OutdatedCursor` info event (BC42).
pub fn resume_cursor(last_seq: Option<u64>, initial: Option<u64>) -> Option<u64> {
    match last_seq {
        Some(seq) => Some(seq + 1),
        None => initial,
    }
}

/// `true` when `err` is a WebSocket handshake rejected with HTTP 400
/// (BC41): a `cursor` below the retention floor is the known cause.
fn is_http_400(err: &tokio_tungstenite::tungstenite::Error) -> bool {
    matches!(
        err,
        tokio_tungstenite::tungstenite::Error::Http(response) if response.status().as_u16() == 400
    )
}

/// One connect attempt against `host`. On an HTTP 400 rejection while
/// `cursor` was sent (BC41), logs the rejected cursor at `warn` and retries
/// once with no cursor, which starts the subscription at the head, before
/// giving up on this attempt.
async fn connect_socket(
    host: &str,
    dict_id: Option<&str>,
    cursor: Option<u64>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Box<tokio_tungstenite::tungstenite::Error>>
{
    let url = subscribe_url(host, dict_id, cursor);
    match tokio_tungstenite::connect_async(&url).await {
        Ok((socket, _)) => Ok(socket),
        Err(err) if cursor.is_some() && is_http_400(&err) => {
            tracing::warn!(
                cursor = ?cursor,
                "jetstream: handshake rejected with HTTP 400, retrying with no cursor"
            );
            let url = subscribe_url(host, dict_id, None);
            tokio_tungstenite::connect_async(&url).await.map(|(socket, _)| socket).map_err(Box::new)
        }
        Err(err) => Err(Box::new(err)),
    }
}

/// Connects to the next host in `hosts` (round-robin from `*host_index`),
/// loading or fetching the dictionary at each attempt (logging at `warn`
/// while degraded, BC39) and backing off between failures (BC12, BC13). A
/// failed attempt rotates to the next host before the next try, and the
/// dictionary fetch uses that same rotating host (BC30). Never returns an
/// error: this is `connect`'s and `reconnect`'s shared "never caller-fatal
/// for a network reason" loop (BC31).
async fn connect_with_rotation(
    http: &reqwest::Client,
    cache_dir: &Path,
    hosts: &[String],
    host_index: &mut usize,
    dict: &mut Option<Dictionary>,
    cursor: Option<u64>,
    attempt: &mut u32,
) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
    loop {
        if dict.is_none() {
            match Dictionary::load_or_fetch(http, cache_dir, &hosts[*host_index]).await {
                Ok(loaded) => *dict = Some(loaded),
                Err(err) => {
                    tracing::warn!(error = %err, "jetstream: degraded, connecting with no dictionary");
                }
            }
        }
        let dict_id = dict.as_ref().map(|d| d.id.as_str());
        match connect_socket(&hosts[*host_index], dict_id, cursor).await {
            Ok(socket) => return socket,
            Err(err) => {
                tracing::warn!(error = %err, host = %hosts[*host_index], "jetstream: connect failed");
                *host_index = (*host_index + 1) % hosts.len();
                let delay = backoff_delay(*attempt);
                *attempt = attempt.saturating_add(1);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// A live Jetstream v2 connection. Owns the socket, the shared HTTP client
/// and cache directory the dictionary uses, the configured host list and
/// the current rotation position, the prepared dictionary (if one is in
/// hand), the last `seq` seen, and the reconnect attempt counter. `next()`
/// is the only way a caller drives it: every reconnect, backoff, rotation
/// and resume happens inside that call, so the caller only ever sees the
/// next [`Event`].
pub struct JetstreamClient {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    http: reqwest::Client,
    cache_dir: PathBuf,
    hosts: Vec<String>,
    host_index: usize,
    dict: Option<Dictionary>,
    initial_cursor: Option<u64>,
    last_seq: Option<u64>,
    /// Set by an `OutdatedCursor` info event (BC42) and cleared by the next
    /// commit; while set, a reconnect sends no cursor at all rather than
    /// the clamped value that caused it.
    force_no_cursor: bool,
    attempt: u32,
    /// Cleared to `false` at each successful connect; set on the first
    /// event this connection returns, which is also when `attempt` resets
    /// (BC33).
    got_event_this_connection: bool,
    decompress_failures: u32,
    last_dict_retry: Instant,
    /// Logged once per connection on a `last_seq` regression (BC43), reset
    /// at each reconnect.
    warned_seq_regression: bool,
}

impl JetstreamClient {
    /// Connects to the first of `cfg.jetstream_urls`. Never caller-fatal for
    /// a network reason (BC31): a failed handshake backs off and rotates to
    /// the next configured host exactly as a reconnect does, and this call
    /// returns only once a connection succeeds. The only fatal case left is
    /// a bad configuration, here an empty host list (BC32); `config::load`
    /// already rejects that before a `Config` exists, so this is defence in
    /// depth. `cursor` is the caller's own checkpoint, used verbatim until
    /// an event is seen (BC15, BC16).
    pub async fn connect(cfg: &Config, cursor: Option<u64>) -> Result<Self, JetstreamError> {
        if cfg.jetstream_urls.is_empty() {
            return Err(JetstreamError::Connect("no jetstream hosts configured".to_string()));
        }
        let http = reqwest::Client::builder()
            .timeout(DICTIONARY_TIMEOUT)
            .build()
            .expect("reqwest::Client::builder with only a timeout never fails to build");
        let cache_dir = Dictionary::cache_dir(&cfg.db_path);
        let hosts = cfg.jetstream_urls.clone();
        let mut host_index = 0usize;
        let mut dict = None;
        let mut attempt = 0u32;
        let socket = connect_with_rotation(
            &http,
            &cache_dir,
            &hosts,
            &mut host_index,
            &mut dict,
            cursor,
            &mut attempt,
        )
        .await;
        Ok(Self {
            socket,
            http,
            cache_dir,
            hosts,
            host_index,
            dict,
            initial_cursor: cursor,
            last_seq: None,
            force_no_cursor: false,
            attempt: 0,
            got_event_this_connection: false,
            decompress_failures: 0,
            last_dict_retry: Instant::now(),
            warned_seq_regression: false,
        })
    }

    /// The `seq` of the last commit event returned, or `None` before the
    /// first one.
    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    /// `true` when a dictionary is currently in hand and frames are
    /// received compressed (BC39, BC40).
    pub fn is_compressed(&self) -> bool {
        self.dict.is_some()
    }

    /// The id of the dictionary currently in hand, or `None` while
    /// degraded (BC39, BC40).
    pub fn dictionary_id(&self) -> Option<&str> {
        self.dict.as_ref().map(|dict| dict.id.as_str())
    }

    /// Returns the next [`Event`], hiding every reconnect (BC12). A
    /// non-`"message"` envelope, `#identity`/`#account`/`#sync`/`Other`
    /// payloads, and a frame that fails to decode (BC8, BC9, logged at
    /// `warn`) all loop rather than return. A ping, pong, or close-adjacent
    /// message carries no payload and is ignored (BC24). A closed or
    /// errored socket backs off, rotates hosts and reconnects (BC12, BC30).
    /// While degraded with no dictionary, a fetch is retried on a
    /// 10-minute timer checked here, in addition to the retry every
    /// reconnect already makes (BC39). This call never gives up, so it
    /// never actually returns `Err` today, but keeps the `Result` so a
    /// future caller-fatal `JetstreamError` variant (BC21) can surface
    /// without a signature change.
    pub async fn next(&mut self) -> Result<Event, JetstreamError> {
        loop {
            if self.dict.is_none() && self.last_dict_retry.elapsed() >= DICT_RETRY_INTERVAL {
                self.retry_dictionary().await;
            }
            let event = match self.socket.next().await {
                Some(Ok(Message::Binary(data))) => self.handle_frame(&data, true),
                Some(Ok(Message::Text(data))) => self.handle_frame(data.as_bytes(), false),
                Some(Ok(
                    Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_),
                )) => {
                    None // BC24: no payload to decode.
                }
                Some(Err(err)) => {
                    tracing::warn!(error = %err, "jetstream: socket error, reconnecting");
                    self.reconnect().await;
                    None
                }
                None => {
                    tracing::warn!("jetstream: connection closed, reconnecting");
                    self.reconnect().await;
                    None
                }
            };
            if let Some(event) = event {
                if !self.got_event_this_connection {
                    // BC33: the backoff attempt counter resets only when
                    // the first event of a connection arrives, not on
                    // handshake success.
                    self.attempt = 0;
                    self.got_event_this_connection = true;
                }
                return Ok(event);
            }
        }
    }

    /// Retries the dictionary fetch while degraded (BC39, BC40), using the
    /// current host. Logged at `info` on success, `warn` on a repeat
    /// failure.
    async fn retry_dictionary(&mut self) {
        self.last_dict_retry = Instant::now();
        match Dictionary::load_or_fetch(&self.http, &self.cache_dir, &self.hosts[self.host_index])
            .await
        {
            Ok(dict) => {
                tracing::info!(id = %dict.id, "jetstream: dictionary retry succeeded");
                self.dict = Some(dict);
            }
            Err(err) => {
                tracing::warn!(error = %err, "jetstream: dictionary retry still degraded");
            }
        }
    }

    /// Decodes one message into an [`Event`], or `None` when it should be
    /// consumed internally. A decode failure warns and is otherwise
    /// swallowed (BC8, BC9): the connection stays open. Reuses
    /// [`decode_frame`] rather than re-implementing its decompress-then-
    /// parse steps; a binary message that fails while a dictionary is in
    /// use counts toward the three-consecutive-failures discard rule
    /// (BC38).
    fn handle_frame(&mut self, data: &[u8], binary: bool) -> Option<Event> {
        let used_dictionary = binary && self.dict.is_some();
        match decode_frame(data, binary, self.dict.as_mut()) {
            Ok(frame) => {
                if used_dictionary {
                    self.decompress_failures = 0;
                }
                self.dispatch_frame(frame)
            }
            Err(err) => {
                tracing::warn!(error = %err, "jetstream: skipping frame");
                if used_dictionary {
                    self.on_decompress_failure();
                }
                None
            }
        }
    }

    /// Counts one decompression failure on the current connection (BC38).
    /// At three in a row, discards the dictionary, deletes its cache file,
    /// and marks it due for an immediate retry: the next reconnect (or the
    /// 10-minute timer) fetches again, and until then the connection runs
    /// uncompressed.
    fn on_decompress_failure(&mut self) {
        self.decompress_failures += 1;
        if self.decompress_failures >= 3 {
            if let Some(dict) = self.dict.take() {
                tracing::warn!(
                    id = %dict.id,
                    "jetstream: three consecutive decompression failures, discarding dictionary"
                );
                Dictionary::delete_cache_file(&self.cache_dir, &dict.id);
            }
            self.decompress_failures = 0;
            self.last_dict_retry = Instant::now() - DICT_RETRY_INTERVAL;
        }
    }

    /// Wraps [`dispatch`] with the per-connection state it cannot itself
    /// carry: `last_seq` never moves backwards, warning once per connection
    /// on a regression rather than once per frame (BC43), and the pending
    /// resume cursor is cleared by a commit and set by an `OutdatedCursor`
    /// info event (BC41, BC42).
    fn dispatch_frame(&mut self, frame: Frame) -> Option<Event> {
        let mut candidate = self.last_seq;
        let event = dispatch(frame, &mut candidate);
        if let Some(new_seq) = candidate {
            match self.last_seq {
                Some(current) if new_seq < current => {
                    if !self.warned_seq_regression {
                        tracing::warn!(
                            current,
                            new_seq,
                            "jetstream: seq went backwards, keeping the higher value"
                        );
                        self.warned_seq_regression = true;
                    }
                }
                _ => self.last_seq = Some(new_seq),
            }
        }
        match &event {
            Some(Event::Commit(_)) => self.force_no_cursor = false,
            Some(Event::Info { name, .. }) if name == "OutdatedCursor" => {
                self.force_no_cursor = true;
            }
            _ => {}
        }
        event
    }

    /// Backs off, rotates to the next configured host, and reconnects with
    /// `cursor` computed from `resume_cursor` (BC14, BC15, BC16), or with no
    /// cursor while a pending `OutdatedCursor` clamp is in effect (BC42),
    /// retrying with the same backoff schedule until a connection succeeds
    /// (BC12, BC30). The attempt counter is carried into the new
    /// connection rather than reset here (BC33); `next()` resets it once
    /// the new connection's first event arrives.
    async fn reconnect(&mut self) {
        let cursor = if self.force_no_cursor {
            None
        } else {
            resume_cursor(self.last_seq, self.initial_cursor)
        };
        let mut attempt = self.attempt;
        self.socket = connect_with_rotation(
            &self.http,
            &self.cache_dir,
            &self.hosts,
            &mut self.host_index,
            &mut self.dict,
            cursor,
            &mut attempt,
        )
        .await;
        self.attempt = attempt;
        self.got_event_this_connection = false;
        self.decompress_failures = 0;
        self.warned_seq_regression = false;
    }
}

/// `JetstreamClient` is the ingest task's live `EventSource` (`src/ingest/
/// mod.rs`, slice 4.0). Each method just forwards to the inherent one of
/// the same name; `self.next()` etc. inside this impl resolve to those
/// inherent methods, since an inherent method always takes priority over a
/// trait method of the same name, so there is no recursion.
impl crate::ingest::EventSource for JetstreamClient {
    async fn next(&mut self) -> Result<Event, JetstreamError> {
        self.next().await
    }

    fn last_seq(&self) -> Option<u64> {
        self.last_seq()
    }

    fn is_compressed(&self) -> bool {
        self.is_compressed()
    }

    fn initial_cursor(&self) -> Option<u64> {
        self.initial_cursor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_path(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
    }

    fn fixture_bytes(name: &str) -> Vec<u8> {
        std::fs::read(fixture_path(name))
            .unwrap_or_else(|err| panic!("reading fixture {name}: {err}"))
    }

    fn recorded_dictionary() -> Dictionary {
        let bytes = fixture_bytes("jetstream_dict_20260811.bin");
        let decompressor =
            Decompressor::with_dictionary(&bytes).expect("dictionary should build a decompressor");
        Dictionary { decompressor, id: "20260811".to_string() }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("jetstream-client-test-{label}-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    fn http_client() -> reqwest::Client {
        reqwest::Client::builder().timeout(DICTIONARY_TIMEOUT).build().unwrap()
    }

    #[test]
    fn cached_dictionary_is_read_with_no_http_call() {
        // BC17: a cached dictionary is read from disk. This constructs the
        // cache directly rather than over HTTP; a `tempfile`-backed
        // `read_cache` round trip is the more direct proof.
        let tmp = temp_dir("cache-hit");
        std::fs::create_dir_all(&tmp).unwrap();
        let bytes = fixture_bytes("jetstream_dict_20260811.bin");
        Dictionary::write_cache(&tmp, "20260811", &bytes);

        let dict = Dictionary::read_cache(&tmp).expect("cache should be read back");
        assert_eq!(dict.id, "20260811");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn cache_write_keeps_only_the_newest_file() {
        // BC35, BC36: writing a new id deletes every older `zstd-dict-*.bin`
        // file, and the highest id wins on the next read.
        let tmp = temp_dir("cache-rotate");
        std::fs::create_dir_all(&tmp).unwrap();
        Dictionary::write_cache(&tmp, "1", b"one");
        Dictionary::write_cache(&tmp, "20260811", b"two");

        let names: Vec<String> = std::fs::read_dir(&tmp)
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["zstd-dict-20260811.bin"]);

        let dict = Dictionary::read_cache(&tmp).expect("cache should be read back");
        assert_eq!(dict.id, "20260811");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn empty_cache_file_is_a_miss() {
        // BC37: an empty file is treated as no cache at all.
        let tmp = temp_dir("cache-empty");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("zstd-dict-1.bin"), b"").unwrap();

        assert!(Dictionary::read_cache(&tmp).is_none());

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[tokio::test]
    async fn missing_cache_with_unreachable_host_yields_dictionary_error() {
        // BC19: no cache and a fetch failure (here, an unreachable host)
        // produces `JetstreamError::Dictionary`, never a panic and never a
        // different variant.
        let tmp = temp_dir("cache-miss");
        let http = http_client();

        match Dictionary::load_or_fetch(&http, &tmp, "wss://127.0.0.1:1").await {
            Err(JetstreamError::Dictionary(_)) => {}
            Err(other) => panic!("expected JetstreamError::Dictionary, got {other}"),
            Ok(_) => panic!("expected an error, dictionary fetch unexpectedly succeeded"),
        }
    }

    fn test_config(cache_dir: &Path, jetstream_urls: &[&str]) -> Config {
        let lookup = {
            let db_path = cache_dir.join("upstage.db").to_string_lossy().to_string();
            let urls = jetstream_urls.join(",");
            move |name: &str| match name {
                "UPSTAGE_HOSTNAME" => Some("feed.example.com".to_string()),
                "UPSTAGE_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
                "UPSTAGE_DB_PATH" => Some(db_path.clone()),
                "UPSTAGE_JETSTREAM_URL" => Some(urls.clone()),
                _ => None,
            }
        };
        crate::config::load(lookup).expect("test config should load")
    }

    #[test]
    fn subscribe_url_holds_the_four_collections_in_order() {
        // Asserted character for character against the v2 URL verified
        // live on 2026-09-18 (see spec.md's "Answers from the engineer",
        // step 7): the repeatable parameter is `collections`, never the v1
        // `wantedCollections`, and the path is the XRPC
        // `subscribeEvents` endpoint, never the v1 `/subscribe` path.
        let url = subscribe_url("wss://jetstream.us-west.bsky.network", Some("20260811"), Some(5));
        assert_eq!(
            url,
            "wss://jetstream.us-west.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents?\
             collections=app.bsky.feed.post\
             &collections=app.bsky.feed.like\
             &collections=app.bsky.feed.repost\
             &collections=app.bsky.feed.postgate\
             &kinds=commit\
             &zstdDictionary=20260811&cursor=5"
        );
    }

    #[test]
    fn subscribe_url_omits_dictionary_and_cursor_when_absent() {
        let url = subscribe_url("wss://jetstream.us-west.bsky.network", None, None);
        assert!(!url.contains("zstdDictionary"));
        assert!(!url.contains("cursor="));
        assert!(!url.contains("wantedCollections"));
    }

    #[test]
    fn dictionary_url_uses_the_v2_xrpc_path() {
        // The v2 `getZstdDictionary` XRPC endpoint, verified live on
        // 2026-09-18, reached by swapping the `wss://` scheme for
        // `https://`. Never the v1 `/subscribe/zstd-dictionary` path.
        assert_eq!(
            dictionary_url("wss://jetstream.us-west.bsky.network"),
            "https://jetstream.us-west.bsky.network/xrpc/network.bsky.jetstream.getZstdDictionary"
        );
        assert_eq!(
            dictionary_url("ws://127.0.0.1:1"),
            "http://127.0.0.1:1/xrpc/network.bsky.jetstream.getZstdDictionary"
        );
    }

    #[test]
    fn recorded_zstd_frame_decodes_and_parses_as_commit() {
        // AC6: the recorded frame decodes with the recorded dictionary and
        // parses as a Commit for app.bsky.feed.post.
        let mut dict = recorded_dictionary();
        let data = fixture_bytes("jetstream_frame.zst");

        let frame = decode_frame(&data, true, Some(&mut dict)).expect("frame should decode");
        match frame.payload {
            crate::jetstream::event::Payload::Commit(commit) => {
                assert_eq!(commit.collection, "app.bsky.feed.post");
            }
            other => panic!("expected Payload::Commit, got {other:?}"),
        }
    }

    #[test]
    fn corrupt_zstd_frame_is_skipped_not_a_panic() {
        // BC9: a malformed or corrupt zstd frame is a `Decode` error, never
        // a panic.
        let mut dict = recorded_dictionary();
        let err = decode_frame(b"not a zstd frame", true, Some(&mut dict)).unwrap_err();
        assert!(matches!(err, JetstreamError::Decode(_)));
    }

    #[test]
    fn text_message_parses_as_json_directly() {
        // BC23, BC45: a text message, with no dictionary in use, is parsed
        // with `serde_json::from_slice` directly, the same path a decoded
        // binary frame takes.
        let raw = br##"{"$type":"ping","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","did":"did:plc:abc"}}"##;
        let frame = decode_frame(raw, false, None).expect("text message should decode");
        assert_eq!(frame.kind, "ping");
    }

    #[test]
    fn ignores_non_commit_kinds() {
        // AC2: `#identity`, `#account`, `#sync`, an unknown payload type,
        // and a non-`"message"` envelope are all swallowed by `dispatch`,
        // and none of them touch `last_seq` (BC1's loop half, BC4, BC5).
        let mut last_seq = None;
        let non_commit_payloads = [
            r##"{"$type":"network.bsky.jetstream.subscribeEvents#identity","did":"did:plc:abc"}"##,
            r##"{"$type":"network.bsky.jetstream.subscribeEvents#account","did":"did:plc:abc"}"##,
            r##"{"$type":"network.bsky.jetstream.subscribeEvents#sync","did":"did:plc:abc"}"##,
            r##"{"$type":"#futurething","foo":"bar"}"##,
        ];
        for payload in non_commit_payloads {
            let raw = format!(r##"{{"$type":"message","payload":{payload}}}"##);
            let frame: Frame = serde_json::from_str(&raw).unwrap();
            assert_eq!(dispatch(frame, &mut last_seq), None);
        }
        assert_eq!(last_seq, None);

        // A non-`"message"` envelope is ignored too, even wrapped around a
        // payload kind that would otherwise be consumed internally.
        let raw = r##"{"$type":"ping","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","did":"did:plc:abc"}}"##;
        let frame: Frame = serde_json::from_str(raw).unwrap();
        assert_eq!(dispatch(frame, &mut last_seq), None);

        // A `#commit` returns and advances `last_seq`; a `#info` returns
        // too, but leaves `last_seq` untouched, since it carries no `seq`
        // (BC3).
        let raw = r##"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","did":"did:plc:abc","seq":42,"time":"2026-09-18T00:00:00.000000Z","operation":"create","collection":"app.bsky.feed.post","rkey":"abc123","rev":"rev123"}}"##;
        let frame: Frame = serde_json::from_str(raw).unwrap();
        let event = dispatch(frame, &mut last_seq).expect("commit should return an event");
        assert!(matches!(event, Event::Commit(_)));
        assert_eq!(last_seq, Some(42));

        let raw = r##"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"OutdatedCursor","message":"..."}}"##;
        let frame: Frame = serde_json::from_str(raw).unwrap();
        let event = dispatch(frame, &mut last_seq).expect("info should return an event");
        assert!(matches!(event, Event::Info { .. }));
        assert_eq!(last_seq, Some(42), "an info frame must not touch last_seq");
    }

    #[test]
    fn backoff_caps_at_60s() {
        // BC13: 1, 2, 4, 8, 16, 32, 60 seconds for attempts 0 through 6;
        // 60 again from there on, never zero, never above 60, and no
        // overflow at a large attempt.
        let expected = [1, 2, 4, 8, 16, 32, 60];
        for (attempt, &secs) in expected.iter().enumerate() {
            assert_eq!(backoff_delay(attempt as u32).as_secs(), secs);
        }
        assert_eq!(backoff_delay(7).as_secs(), 60);
        assert_eq!(backoff_delay(100).as_secs(), 60);
        assert_eq!(backoff_delay(u32::MAX).as_secs(), 60);
    }

    #[test]
    fn resume_uses_seq_plus_one() {
        // BC14, BC15, BC16.
        assert_eq!(resume_cursor(Some(5), None), Some(6));
        assert_eq!(resume_cursor(Some(5), Some(99)), Some(6));
        assert_eq!(resume_cursor(None, None), None);
        assert_eq!(resume_cursor(None, Some(42)), Some(42));
    }

    #[tokio::test]
    async fn rotates_to_the_next_host_on_a_failed_connect() {
        // BC30: two unreachable loopback hosts, connected with a fast
        // backoff. `connect_with_rotation` must not get stuck retrying the
        // first host alone.
        let http = http_client();
        let cache_dir = temp_dir("rotation");
        let hosts = vec!["ws://127.0.0.1:1".to_string(), "ws://127.0.0.1:2".to_string()];
        let mut host_index = 0usize;
        let mut dict = None;
        let mut attempt = 0u32;

        // Neither host is reachable, so run the loop by hand for two
        // iterations and check that the index actually rotated, rather
        // than driving `connect_with_rotation` to completion (it never
        // returns against two dead hosts).
        let fut = connect_with_rotation(
            &http,
            &cache_dir,
            &hosts,
            &mut host_index,
            &mut dict,
            None,
            &mut attempt,
        );
        // Give the two failing attempts a moment to run; `backoff_delay(0)`
        // is 1s and `backoff_delay(1)` is 2s, so 500ms sees the first
        // rotation without waiting for both.
        let _ = tokio::time::timeout(Duration::from_millis(500), fut).await;
        assert_eq!(host_index, 1, "the first failed attempt should rotate to the second host");

        std::fs::remove_dir_all(&cache_dir).ok();
    }

    #[test]
    fn is_http_400_matches_only_a_400_response() {
        // BC41's detection helper: sanity-checked directly since a live 400
        // handshake is not reproducible without the network.
        use tokio_tungstenite::tungstenite::http::{Response, StatusCode};
        let ok = tokio_tungstenite::tungstenite::Error::Http(
            Response::builder().status(StatusCode::OK).body(None).unwrap(),
        );
        let bad = tokio_tungstenite::tungstenite::Error::Http(
            Response::builder().status(StatusCode::BAD_REQUEST).body(None).unwrap(),
        );
        assert!(!is_http_400(&ok));
        assert!(is_http_400(&bad));
    }

    #[tokio::test]
    #[ignore = "hits the live Jetstream v2 host; run by hand"]
    async fn jetstream_live_connect() {
        // AC8: a live connection decodes real frames from all four
        // collections. The cursor is a unix-microsecond value below the
        // retention floor (spec.md step 7), which starts the subscription
        // near the current head rather than waiting on the full backlog.
        let tmp = temp_dir("live");
        let cfg = test_config(&tmp, &["wss://jetstream.us-west.bsky.network"]);
        let mut client = JetstreamClient::connect(&cfg, Some(1_600_000_000_000_000))
            .await
            .expect("live connect should succeed");

        let mut seen = std::collections::HashSet::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while seen.len() < COLLECTIONS.len() && tokio::time::Instant::now() < deadline {
            let event = tokio::time::timeout(Duration::from_secs(15), client.next())
                .await
                .expect("next() should not hang")
                .expect("next() should not error");
            if let Event::Commit(commit) = event {
                seen.insert(commit.collection);
            }
        }
        assert_eq!(seen.len(), COLLECTIONS.len(), "expected all four collections, saw {seen:?}");

        std::fs::remove_dir_all(&tmp).ok();
    }
}
