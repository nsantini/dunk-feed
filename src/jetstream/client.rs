//! Dictionary fetch and cache, URL build, and frame decode. Reconnect,
//! backoff, resume and the `JetstreamClient` struct itself land over slice
//! 3.0; this slice gives them `JetstreamError` in full and the two pure,
//! testable pieces the reconnect loop calls: `subscribe_url` and
//! `decode_frame`.
//!
//! The zstd dictionary is fetched once and cached on disk next to
//! `cfg.db_path`, because re-preparing a 65,536-byte `DecoderDictionary`
//! per frame would cost about 400 times a second at the measured live rate
//! (TECH-DESIGN section 5.1's "Approach"). A cold start with no cache and an
//! unreachable dictionary endpoint is not fatal: `Dictionary` is caught
//! here, logged at `warn`, and the caller connects with no
//! `zstdDictionary` parameter, so frames arrive as text (BC19).

use std::path::{Path, PathBuf};

use thiserror::Error;
use zstd::dict::DecoderDictionary;

use crate::config::Config;
use crate::jetstream::event::Frame;

/// The four collections this story cares about, TECH-DESIGN section 5.1.
/// Held in this fixed order so `subscribe_url`'s query string is stable and
/// a test can assert the whole thing.
const COLLECTIONS: [&str; 4] =
    ["app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost", "app.bsky.feed.postgate"];

/// The name of the dictionary id cache file, in the directory that holds
/// `cfg.db_path`.
const DICT_ID_FILE: &str = "zstd-dict-id";

/// The HTTP header the dictionary endpoint returns the dictionary id on.
const DICT_ID_HEADER: &str = "x-zstd-dictionary-id";

/// Every way this module's work can fail. Every variant is caught inside
/// `client.rs`'s own reconnect loop (slice 3.0), except a caller-fatal one;
/// this story raises no caller-fatal variant. The caller sees one log line
/// per skipped frame and one per reconnect (BC21), never this type itself.
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
    /// The initial WebSocket handshake failed.
    #[error("connect failed: {0}")]
    Connect(String),
    /// The WebSocket closed or errored after a successful connect.
    #[error("socket error: {0}")]
    Socket(String),
}

/// A prepared zstd dictionary: the id Jetstream tags it with, and the
/// `DecoderDictionary` built from its bytes once at connect so a frame
/// never re-prepares it (TECH-DESIGN section 5.1's "Approach").
pub struct Dictionary {
    pub id: String,
    prepared: DecoderDictionary<'static>,
}

impl Dictionary {
    /// Returns the directory `cfg.db_path` lives in, or the current
    /// directory when `db_path` names a bare file with no parent.
    fn cache_dir(cfg: &Config) -> PathBuf {
        Path::new(&cfg.db_path).parent().map(Path::to_path_buf).unwrap_or_default()
    }

    /// Loads the dictionary from disk if it is cached (BC17, no HTTP call),
    /// otherwise fetches it from `cfg.jetstream_url`'s dictionary endpoint
    /// and writes the cache (BC18). A write failure warns and still uses
    /// the fetched bytes for this run (BC20). Returns `Dictionary` on a
    /// cold start with a reachable host and header; returns
    /// `JetstreamError::Dictionary` when neither the cache nor a fetch
    /// produced one (BC19), which the caller catches and treats as
    /// "connect with no dictionary".
    pub async fn load_or_fetch(cfg: &Config) -> Result<Self, JetstreamError> {
        let dir = Self::cache_dir(cfg);
        if let Some(dict) = Self::read_cache(&dir) {
            return Ok(dict);
        }
        let (id, bytes) = Self::fetch(cfg).await?;
        Self::write_cache(&dir, &id, &bytes);
        Ok(Self { prepared: DecoderDictionary::copy(&bytes), id })
    }

    /// Reads `zstd-dict-id` and `zstd-<id>.dict` from `dir`. `None` on any
    /// miss: file absent, unreadable, or the two disagree, which sends the
    /// caller to `fetch` instead of failing outright.
    fn read_cache(dir: &Path) -> Option<Self> {
        let id = std::fs::read_to_string(dir.join(DICT_ID_FILE)).ok()?;
        let id = id.trim().to_string();
        if id.is_empty() {
            return None;
        }
        let bytes = std::fs::read(dir.join(format!("zstd-{id}.dict"))).ok()?;
        Some(Self { prepared: DecoderDictionary::copy(&bytes), id })
    }

    /// Writes `zstd-dict-id` and `zstd-<id>.dict` to `dir`. Logged at `warn`
    /// and otherwise ignored on failure (BC20): an unwritable cache
    /// directory does not fail the caller, which already has the bytes it
    /// needs for this run.
    fn write_cache(dir: &Path, id: &str, bytes: &[u8]) {
        if let Err(err) = std::fs::create_dir_all(dir) {
            tracing::warn!(error = %err, dir = %dir.display(), "jetstream: could not create dictionary cache dir");
            return;
        }
        if let Err(err) = std::fs::write(dir.join(format!("zstd-{id}.dict")), bytes) {
            tracing::warn!(error = %err, "jetstream: could not write dictionary cache file");
            return;
        }
        if let Err(err) = std::fs::write(dir.join(DICT_ID_FILE), id) {
            tracing::warn!(error = %err, "jetstream: could not write dictionary id cache file");
        }
    }

    /// Fetches the dictionary bytes and id from `cfg.jetstream_url`'s HTTP
    /// dictionary endpoint. `Dictionary` on a transport failure, a
    /// non-success status, or a response with no `x-zstd-dictionary-id`
    /// header (BC19).
    async fn fetch(cfg: &Config) -> Result<(String, Vec<u8>), JetstreamError> {
        let url = dictionary_url(&cfg.jetstream_url);
        let response = reqwest::get(&url)
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
/// base, swapping the scheme for `https://`/`http://`, TECH-DESIGN section
/// 5.1. This is the v2 `network.bsky.jetstream.getZstdDictionary` XRPC
/// endpoint, verified live on 2026-09-18; the v1 `/subscribe/zstd-dictionary`
/// path is rejected outright (TECH-DESIGN section 12, D4).
fn dictionary_url(jetstream_url: &str) -> String {
    let base = jetstream_url.replacen("wss://", "https://", 1).replacen("ws://", "http://", 1);
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
pub fn subscribe_url(jetstream_url: &str, dict_id: Option<&str>, cursor: Option<u64>) -> String {
    let mut url = format!("{jetstream_url}/xrpc/network.bsky.jetstream.subscribeEvents?");
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
/// text message, parsed as JSON directly, the same path a decoded binary
/// frame takes (BC23). `dict` is `None` when no dictionary is in hand, in
/// which case a binary message is treated as raw JSON bytes with no
/// decompression. `JetstreamError::Decode` on a corrupt zstd frame or a
/// payload that fails its shape; the reconnect loop (slice 3.0) catches
/// this, logs at `warn`, and reads the next frame rather than treating it
/// as fatal (BC8, BC9).
pub fn decode_frame(
    data: &[u8],
    binary: bool,
    dict: Option<&Dictionary>,
) -> Result<Frame, JetstreamError> {
    let json = if binary {
        match dict {
            Some(dict) => decompress(data, &dict.prepared)?,
            None => data.to_vec(),
        }
    } else {
        data.to_vec()
    };
    serde_json::from_slice(&json).map_err(|err| JetstreamError::Decode(err.to_string()))
}

/// Decompresses one zstd frame against a prepared dictionary. `Decode` on a
/// malformed or corrupt frame (BC9): never a panic.
fn decompress(data: &[u8], dict: &DecoderDictionary<'static>) -> Result<Vec<u8>, JetstreamError> {
    use std::io::Read;
    let mut decoder = zstd::stream::Decoder::with_prepared_dictionary(data, dict)
        .map_err(|err| JetstreamError::Decode(err.to_string()))?;
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).map_err(|err| JetstreamError::Decode(err.to_string()))?;
    Ok(out)
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
        Dictionary { prepared: DecoderDictionary::copy(&bytes), id: "20260811".to_string() }
    }

    #[test]
    fn cached_dictionary_is_read_with_no_http_call() {
        // BC17: a cached dictionary is read from disk. This constructs the
        // cache directly rather than over HTTP; a `tempfile`-backed
        // `read_cache` round trip is the more direct proof.
        let tmp = std::env::temp_dir().join(format!("jetstream-cache-test-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let bytes = fixture_bytes("jetstream_dict_20260811.bin");
        Dictionary::write_cache(&tmp, "20260811", &bytes);

        let dict = Dictionary::read_cache(&tmp).expect("cache should be read back");
        assert_eq!(dict.id, "20260811");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[tokio::test]
    async fn missing_cache_with_unreachable_host_yields_dictionary_error() {
        // BC19: no cache and a fetch failure (here, an unreachable host)
        // produces `JetstreamError::Dictionary`, never a panic and never a
        // different variant.
        let tmp =
            std::env::temp_dir().join(format!("jetstream-cache-test-miss-{}", std::process::id()));
        std::fs::remove_dir_all(&tmp).ok();
        let cfg = test_config(&tmp, "wss://127.0.0.1:1");

        match Dictionary::load_or_fetch(&cfg).await {
            Err(JetstreamError::Dictionary(_)) => {}
            Err(other) => panic!("expected JetstreamError::Dictionary, got {other}"),
            Ok(_) => panic!("expected an error, dictionary fetch unexpectedly succeeded"),
        }
    }

    fn test_config(cache_dir: &Path, jetstream_url: &str) -> Config {
        let lookup = {
            let db_path = cache_dir.join("dunk.db").to_string_lossy().to_string();
            let jetstream_url = jetstream_url.to_string();
            move |name: &str| match name {
                "DUNK_HOSTNAME" => Some("feed.example.com".to_string()),
                "DUNK_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
                "DUNK_DB_PATH" => Some(db_path.clone()),
                "DUNK_JETSTREAM_URL" => Some(jetstream_url.clone()),
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
        let dict = recorded_dictionary();
        let data = fixture_bytes("jetstream_frame.zst");

        let frame = decode_frame(&data, true, Some(&dict)).expect("frame should decode");
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
        let dict = recorded_dictionary();
        let err = decode_frame(b"not a zstd frame", true, Some(&dict)).unwrap_err();
        assert!(matches!(err, JetstreamError::Decode(_)));
    }

    #[test]
    fn text_message_parses_as_json_directly() {
        // BC23: a text message, with no dictionary in use, is parsed as
        // JSON directly, the same path a decoded binary frame takes.
        let raw = br##"{"$type":"ping","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","did":"did:plc:abc"}}"##;
        let frame = decode_frame(raw, false, None).expect("text message should decode");
        assert_eq!(frame.kind, "ping");
    }
}
