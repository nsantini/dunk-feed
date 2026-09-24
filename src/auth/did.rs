//! `KeyCache`: the DID-to-key store `auth::verify` (`mod.rs`) reads on
//! every request, and the only piece of state design §5's cache rules
//! (BC14 to BC16) describe. Every method on it is synchronous and returns
//! before yielding, so the mutex guarding it (spec `## Approach`) is
//! never held across an `.await`.
//!
//! This file also holds the resolver side: the `DidFetcher` trait and its
//! `reqwest` implementation (BC17, BC18), the DID document parse (BC19),
//! and `run_resolver`, the task that drains `ResolveRequest`s (`mod.rs`)
//! and fills the cache. `run_resolver` is generic over `DidFetcher` the
//! same way `appview::pds::PdsClient` is generic over `PdsTransport`
//! (spec `## Approach`), so its tests use DID document fixtures and never
//! the network. `src/ingest/mod.rs` (slice 3.0) builds the real
//! `HttpDidFetcher` and spawns `run_resolver` only when
//! `UPSTAGE_PERSONALISE` is `true`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::sync::mpsc;

use super::keys::PublicKey;
use super::ResolveRequest;

/// An entry older than this is stale (BC14): still used, but a refresh
/// should be enqueued.
const STALE_AFTER_SECS: i64 = 60 * 60;

/// An entry older than this is treated as missing (BC15).
const EXPIRE_AFTER_SECS: i64 = 24 * 60 * 60;

/// The refetch limiter (BC11) allows at most one refetch per DID in this
/// many seconds, so a bad signature on every request for the same DID
/// cannot make the resolver call the PLC directory or a `did:web` host on
/// every request.
const REFETCH_COOLDOWN_SECS: i64 = 60 * 60;

struct Entry {
    key: PublicKey,
    fetched_at: i64,
    last_refetch_sent: Option<i64>,
}

/// One DID's outstanding-miss bookkeeping (BC9): whether a `Miss` fetch
/// for it is already queued or running, when its last failed attempt
/// finished, and when this state was first created. `KeyCache` keeps this
/// separately from `Entry` because a DID with no cached key at all has no
/// `Entry` to hold it. `first_seen` orders pruning the same way `Entry`'s
/// `fetched_at` orders eviction for the key cache itself (BC16), but once
/// every tracked DID is in flight or cooling down there is nothing left to
/// prune, and [`KeyCache::should_send_miss`] then drops the new DID instead
/// of evicting one that is still doing useful work.
struct MissState {
    in_flight: bool,
    last_attempted: Option<i64>,
    first_seen: i64,
}

/// What a cache lookup found for a DID (BC9, BC14, BC15). `Stale` still
/// carries the key: design §5 check 6 says a stale entry is used while a
/// refresh happens in the background.
pub(super) enum Lookup {
    Fresh(PublicKey),
    Stale(PublicKey),
    Missing,
}

/// The DID key cache. `new`'s `max_entries` is `2 × UPSTAGE_MAX_VIEWERS`
/// (BC16), computed by the caller (`src/ingest/mod.rs`, slice 3.0).
pub struct KeyCache {
    max_entries: usize,
    entries: Mutex<HashMap<String, Entry>>,
    /// Review round 1, defect B: outstanding-miss bookkeeping, keyed by
    /// DID, for [`Self::should_send_miss`] and [`Self::miss_fetch_done`].
    misses: Mutex<HashMap<String, MissState>>,
}

impl KeyCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: Mutex::new(HashMap::new()),
            misses: Mutex::new(HashMap::new()),
        }
    }

    /// Looks up `did`'s key as of `now` (BC9, BC14, BC15). An entry past
    /// [`EXPIRE_AFTER_SECS`] reads as `Missing` even though the map entry
    /// is still physically present until the next [`Self::insert`] evicts
    /// it: no caller needs eviction to be eager for correctness.
    pub(super) fn get(&self, did: &str, now: i64) -> Lookup {
        let entries = self.entries.lock().expect("KeyCache mutex poisoned");
        match entries.get(did) {
            None => Lookup::Missing,
            Some(entry) => {
                let age = now - entry.fetched_at;
                if age > EXPIRE_AFTER_SECS {
                    Lookup::Missing
                } else if age > STALE_AFTER_SECS {
                    Lookup::Stale(entry.key.clone())
                } else {
                    Lookup::Fresh(entry.key.clone())
                }
            }
        }
    }

    /// Inserts or replaces `did`'s key with `now` as its fetch time
    /// (the resolver's job, slice 2.0). When inserting a DID the cache
    /// does not already hold and the cache is at `max_entries`, evicts
    /// the entry with the oldest `fetched_at` first (BC16).
    pub(super) fn insert(&self, did: String, key: PublicKey, now: i64) {
        let mut entries = self.entries.lock().expect("KeyCache mutex poisoned");
        if !entries.contains_key(&did) && entries.len() >= self.max_entries {
            if let Some(oldest_did) =
                entries.iter().min_by_key(|(_, entry)| entry.fetched_at).map(|(did, _)| did.clone())
            {
                entries.remove(&oldest_did);
            }
        }
        // Review round 1, defect A: a refresh (the DID was already
        // cached) keeps its `last_refetch_sent` rather than resetting it
        // to `None`. Without this, a resolver fetch that lands while
        // `should_refetch`'s hourly window is still open would make the
        // very next call to `should_refetch` look like the window had
        // never opened, and enqueue a second refetch inside the hour.
        let last_refetch_sent = entries.get(&did).and_then(|entry| entry.last_refetch_sent);
        entries.insert(did, Entry { key, fetched_at: now, last_refetch_sent });
    }

    /// `true` the first time this is called for `did` within a rolling
    /// hour (BC11): callers in `mod.rs` send a refetch request only when
    /// this returns `true`. Used for a stale cache hit and for a
    /// signature failure against a cached key — both cases where `did`
    /// already has an `Entry`. A DID with no cached key at all goes
    /// through [`Self::should_send_miss`] instead (review round 1, defect
    /// B), which also tracks whether a fetch for it is already in
    /// flight, since there is no `Entry` here to hold that state.
    pub(super) fn should_refetch(&self, did: &str, now: i64) -> bool {
        let mut entries = self.entries.lock().expect("KeyCache mutex poisoned");
        let Some(entry) = entries.get_mut(did) else { return true };
        match entry.last_refetch_sent {
            Some(last) if now - last < REFETCH_COOLDOWN_SECS => false,
            _ => {
                entry.last_refetch_sent = Some(now);
                true
            }
        }
    }

    /// `true` the first time this is called for a DID with no cached key,
    /// while no fetch for it is already in flight, and at least an hour
    /// since its last failed attempt finished (BC9). `verify`'s `Missing`
    /// arm (`mod.rs`) sends a `Miss` to the resolver only when this
    /// returns `true`, so a DID that never resolves — a dead PLC entry, a
    /// resolver that is down — is retried at most once an hour rather
    /// than once per request. Bounds `misses` at [`Self::max_entries`]
    /// (BC16's own cap, reused here): a DID not already tracked, arriving
    /// once the map is full, first prunes entries whose cooldown has
    /// passed and that are not in flight. When pruning frees no room —
    /// every tracked DID is still in flight or inside its cooldown, so
    /// all of them are doing useful work — this DID's `Miss` is dropped
    /// instead, the same way a full resolver channel drops one (BC9):
    /// nothing already tracked is evicted to make room for it.
    pub(super) fn should_send_miss(&self, did: &str, now: i64) -> bool {
        let mut misses = self.misses.lock().expect("KeyCache mutex poisoned");
        if !misses.contains_key(did) && misses.len() >= self.max_entries {
            Self::prune_misses(&mut misses, now);
            if misses.len() >= self.max_entries {
                return false;
            }
        }
        let state = misses.entry(did.to_string()).or_insert(MissState {
            in_flight: false,
            last_attempted: None,
            first_seen: now,
        });
        if state.in_flight {
            return false;
        }
        if let Some(last) = state.last_attempted {
            if now - last < REFETCH_COOLDOWN_SECS {
                return false;
            }
        }
        state.in_flight = true;
        true
    }

    /// Removes every entry whose cooldown has already passed and that is
    /// not in flight (BC16-style bound on `misses`): such an entry would
    /// let the very next `should_send_miss` call for it return `true`
    /// anyway, so dropping it first, before `should_send_miss` falls back
    /// to refusing the new DID, never removes bookkeeping a caller still
    /// needed.
    fn prune_misses(misses: &mut HashMap<String, MissState>, now: i64) {
        misses.retain(|_, state| {
            state.in_flight
                || match state.last_attempted {
                    Some(last) => now - last < REFETCH_COOLDOWN_SECS,
                    None => true,
                }
        });
    }

    /// Clears `did`'s in-flight mark without starting the hourly cooldown
    /// (BC9): `verify`'s `Missing` arm (`mod.rs`) calls this when
    /// `try_send` drops the `Miss` because the resolver channel was full,
    /// so the very next request for the same DID can enqueue it again at
    /// once, rather than waiting as if a real attempt had run and
    /// finished.
    pub(super) fn miss_send_dropped(&self, did: &str) {
        let mut misses = self.misses.lock().expect("KeyCache mutex poisoned");
        if let Some(state) = misses.get_mut(did) {
            state.in_flight = false;
        }
    }

    /// Marks `did`'s outstanding `Miss` fetch as finished successfully
    /// (BC9): [`run_resolver`] calls this once a fetch for it returns
    /// `Ok`, whether or not a usable key came out of the document
    /// (BC19). Removes the miss state entirely rather than starting the
    /// hourly cooldown, so a DID evicted from `entries` at the cache cap
    /// (BC16) is fetched again on its very next miss instead of waiting
    /// out a cooldown left over from before it was evicted.
    pub(super) fn miss_fetch_succeeded(&self, did: &str) {
        let mut misses = self.misses.lock().expect("KeyCache mutex poisoned");
        misses.remove(did);
    }

    /// Marks `did`'s outstanding `Miss` fetch as finished unsuccessfully
    /// (BC9, BC11): [`run_resolver`] calls this once a fetch for it
    /// returns `Err`, clearing the in-flight mark [`Self::should_send_miss`]
    /// checks and starting its hourly cooldown, so a DID that never
    /// resolves is retried at most once an hour.
    pub(super) fn miss_fetch_failed(&self, did: &str, now: i64) {
        let mut misses = self.misses.lock().expect("KeyCache mutex poisoned");
        if let Some(state) = misses.get_mut(did) {
            state.in_flight = false;
            state.last_attempted = Some(now);
        }
    }
}

/// Test-only seam for `auth::seed_and_sign_for_test` (`mod.rs`), which
/// `http::skeleton::tests::personalised_headers` calls (AC8, spec
/// `## Answers from the engineer`, step 7): [`KeyCache::insert`] above is
/// `pub(super)`, reachable only inside `auth/`, so a cross-module test
/// outside it seeds a cache through this `pub(crate)` wrapper instead of
/// widening `insert` itself.
#[cfg(test)]
pub(crate) fn insert_for_test(cache: &KeyCache, did: String, key: PublicKey, now: i64) {
    cache.insert(did, key, now);
}

/// Why a DID document fetch produced nothing usable (BC20). Carries only
/// the kind of failure, never the DID or the body, so the one warning
/// `run_resolver` logs on this can never carry a DID or a token (BC21).
#[derive(Debug)]
pub(super) enum FetchError {
    /// The request never produced an HTTP response: a connection error or
    /// a timeout.
    Transport,
    /// A response came back with a non-200 status.
    Http(u16),
    /// A 200 response whose body did not decode as JSON.
    Decode,
}

/// One DID document fetch (BC17, BC18). [`HttpDidFetcher`] is the real
/// implementation; `did::tests` implements this with canned documents so
/// `fixtures` and `rotation_refetch` never touch the network (spec
/// `## Approach`).
pub(super) trait DidFetcher: Send + Sync {
    fn fetch(&self, did: &str) -> impl Future<Output = Result<Value, FetchError>> + Send;
}

/// [`HttpDidFetcher`]'s own request timeout (review round 1, defect K):
/// the same 10 seconds `appview::http_client`'s `REQUEST_TIMEOUT` uses.
/// Not a call to that function, and not a shared constant, because
/// `REQUEST_TIMEOUT` is private to `appview/mod.rs`, which this slice's
/// `Files` list does not include; `HttpDidFetcher` also needs its own
/// `reqwest::Client` regardless, to set the redirect policy below.
const DID_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A DID document larger than this is never fully buffered (review round
/// 1, defect L): [`accumulate_capped`] fails it as [`FetchError::Decode`]
/// once the running total crosses this line, so a hostile or
/// misbehaving `did:web` host cannot grow the resolver task's memory
/// without bound. 64 KiB is generous for a handful of verification
/// methods, the only part of the document [`extract_key`] ever reads.
const MAX_DID_DOCUMENT_BYTES: usize = 64 * 1024;

/// The real [`DidFetcher`]. Builds its own `reqwest::Client`, distinct
/// from [`crate::appview::http_client`], with redirects disabled (review
/// round 1, defect K): a DID document fetch never needs one, and
/// following a redirect would hand this resolver's request to whatever
/// second origin a compromised or misconfigured host named, so a 3xx
/// response is a fetch failure ([`FetchError::Http`]) rather than
/// something this client chases on its own.
pub(super) struct HttpDidFetcher {
    http: reqwest::Client,
    /// `UPSTAGE_PLC_URL`, trailing `/` removed. `src/config.rs` (slice
    /// 3.0) will also remove it at load time (BC24); this trims again as
    /// defence in depth, the same reasoning `HttpPdsTransport::new` gives
    /// for `UPSTAGE_PDS_URL` (`appview/pds.rs`).
    plc_url: String,
}

impl HttpDidFetcher {
    pub(super) fn new(plc_url: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(DID_FETCH_TIMEOUT)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect(
                    "reqwest::Client::builder with a timeout and a redirect policy never fails to build",
                ),
            plc_url: plc_url.trim_end_matches('/').to_string(),
        }
    }
}

/// A source of raw body chunks, so [`accumulate_capped`] is testable with
/// canned chunks instead of a real HTTP response (review round 1, defect
/// L). [`reqwest::Response`] is the only production implementation.
trait ChunkSource: Send {
    fn next_chunk(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>, FetchError>> + Send;
}

impl ChunkSource for reqwest::Response {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, FetchError> {
        self.chunk()
            .await
            .map(|opt| opt.map(|bytes| bytes.to_vec()))
            .map_err(|_err| FetchError::Transport)
    }
}

/// Reads `source` to the end, failing as [`FetchError::Decode`] the
/// moment the running total would exceed [`MAX_DID_DOCUMENT_BYTES`]
/// (review round 1, defect L), instead of buffering an unbounded body
/// before ever looking at its size.
async fn accumulate_capped(mut source: impl ChunkSource) -> Result<Vec<u8>, FetchError> {
    let mut body = Vec::new();
    while let Some(chunk) = source.next_chunk().await? {
        if body.len() + chunk.len() > MAX_DID_DOCUMENT_BYTES {
            return Err(FetchError::Decode);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// BC17: did:plc goes to `plc_url/<did>`. BC18: did:web:<host> goes to
/// `https://<host>/.well-known/did.json`. `did` here always came through
/// `jwt::check_issuer` first (`mod.rs`'s `verify`), so a `did:web` value
/// never carries a port or a path. Pure and separate from
/// [`HttpDidFetcher::fetch`] so BC17 and BC18 are unit-testable without a
/// network call.
fn resolve_url(plc_url: &str, did: &str) -> String {
    match did.strip_prefix("did:web:") {
        Some(host) => format!("https://{host}/.well-known/did.json"),
        None => format!("{plc_url}/{did}"),
    }
}

impl DidFetcher for HttpDidFetcher {
    async fn fetch(&self, did: &str) -> Result<Value, FetchError> {
        let url = resolve_url(&self.plc_url, did);
        let response = self.http.get(&url).send().await.map_err(|_err| FetchError::Transport)?;
        let status = response.status().as_u16();
        if status != 200 {
            // Review round 1, defect K: with redirects disabled above, a
            // 3xx lands here as an ordinary non-200 status, so it fails
            // the same way a 4xx or 5xx does.
            return Err(FetchError::Http(status));
        }
        // Review round 1, defect L: the body is read through a capped
        // accumulator rather than `response.json`, which would buffer
        // the whole thing regardless of size.
        let body = accumulate_capped(response).await?;
        serde_json::from_slice(&body).map_err(|_err| FetchError::Decode)
    }
}

/// Reads `publicKeyMultibase` off the verification method whose `id` ends
/// `#atproto` (BC19). `None` when the document has no such method, its
/// `publicKeyMultibase` is missing or not a string, or the value does not
/// decode (`keys::decode_multibase`) — every one of those is "no usable
/// key", which `run_resolver` treats the same way: nothing is cached.
fn extract_key(document: &Value) -> Option<PublicKey> {
    let methods = document.get("verificationMethod")?.as_array()?;
    let method = methods.iter().find(|method| {
        method.get("id").and_then(Value::as_str).is_some_and(|id| id.ends_with("#atproto"))
    })?;
    let multibase = method.get("publicKeyMultibase")?.as_str()?;
    super::keys::decode_multibase(multibase)
}

/// Drains `requests` and fills `cache` from `fetcher` (BC17 to BC21).
/// Runs until the channel closes (the sender side lives in `AppState`,
/// `src/ingest/mod.rs` slice 3.0, which outlives every request). A `Miss`
/// and a `Refetch` are fetched the same way: either way the current key,
/// if any, might be stale, so the document is fetched fresh. A transport
/// or HTTP fetch failure logs one warning naming only the failure kind
/// (BC20, BC21) and caches nothing. A fetch that succeeds but yields no
/// usable key ([`extract_key`] returning `None`, BC19) also caches
/// nothing, and logs no warning of its own — an absent `#atproto` method
/// is not a transport or server failure worth a warning on every retry —
/// but for a `Miss` it is treated the same way a fetch error is (BC9,
/// BC19): the DID's outstanding miss ends unsuccessfully
/// ([`KeyCache::miss_fetch_failed`]), so a document that never carries a
/// usable key is retried at most once an hour rather than on every
/// request. For a `Miss`, the fetch's end is always reported to `cache`:
/// only a fetch that both succeeds and yields a usable key clears the
/// miss state entirely ([`KeyCache::miss_fetch_succeeded`]); every other
/// outcome starts the hourly cooldown ([`KeyCache::miss_fetch_failed`]).
pub(super) async fn run_resolver<F: DidFetcher>(
    mut requests: mpsc::Receiver<ResolveRequest>,
    fetcher: F,
    cache: Arc<KeyCache>,
) {
    while let Some(request) = requests.recv().await {
        let (did, is_miss) = match request {
            ResolveRequest::Miss(did) => (did, true),
            ResolveRequest::Refetch(did) => (did, false),
        };
        match fetcher.fetch(&did).await {
            Ok(document) => match extract_key(&document) {
                Some(key) => {
                    cache.insert(did.clone(), key, crate::store::unix_now());
                    if is_miss {
                        cache.miss_fetch_succeeded(&did);
                    }
                }
                None => {
                    // BC19: a document with no usable key caches nothing.
                    // BC9: for a `Miss`, this is a failed attempt like any
                    // other, so it starts the hourly cooldown rather than
                    // leaving the DID retryable on every request.
                    if is_miss {
                        cache.miss_fetch_failed(&did, crate::store::unix_now());
                    }
                }
            },
            Err(err) => {
                tracing::warn!(kind = ?err, "auth: did document fetch failed");
                if is_miss {
                    cache.miss_fetch_failed(&did, crate::store::unix_now());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::keys::decode_multibase;

    /// A fixed valid k256 `publicKeyMultibase` value: the multicodec
    /// prefix (`0xE7 0x01`) followed by a compressed point, base58btc
    /// encoded with the `z` prefix. `did.rs`'s tests do not sign or
    /// verify anything, so any point decodable by `keys::decode_multibase`
    /// stands in for a real key.
    fn any_key() -> PublicKey {
        let sk = k256::ecdsa::SigningKey::from_slice(&[7u8; 32]).unwrap();
        let point = sk.verifying_key().to_sec1_point(true);
        let mut bytes = vec![0xE7, 0x01];
        bytes.extend_from_slice(point.as_bytes());
        let multibase = format!("z{}", bs58::encode(bytes).into_string());
        decode_multibase(&multibase).expect("test key should decode")
    }

    #[test]
    fn cache_ages() {
        let cache = KeyCache::new(10);
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let fetched_at = 1_000_000;
        cache.insert(did.to_string(), any_key(), fetched_at);

        // BC9/fresh: read right after insert.
        assert!(matches!(cache.get(did, fetched_at), Lookup::Fresh(_)));

        // BC14: stale after 1 h, still returns the key.
        let just_stale = fetched_at + 60 * 60 + 1;
        assert!(matches!(cache.get(did, just_stale), Lookup::Stale(_)));

        // BC15: missing after 24 h.
        let expired = fetched_at + 24 * 60 * 60 + 1;
        assert!(matches!(cache.get(did, expired), Lookup::Missing));

        // A DID never inserted is always missing.
        assert!(matches!(cache.get("did:plc:neverinserted00000000", fetched_at), Lookup::Missing));
    }

    #[test]
    fn cache_cap_evicts_oldest() {
        // BC16: full at max_entries, the oldest by fetch time is removed.
        let cache = KeyCache::new(2);
        cache.insert("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".to_string(), any_key(), 100);
        cache.insert("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".to_string(), any_key(), 200);
        // Cache is now full; inserting a third evicts the oldest (the
        // first one, fetched at 100).
        cache.insert("did:plc:cccccccccccccccccccccccc".to_string(), any_key(), 300);

        assert!(matches!(cache.get("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", 300), Lookup::Missing));
        assert!(matches!(cache.get("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", 300), Lookup::Fresh(_)));
        assert!(matches!(cache.get("did:plc:cccccccccccccccccccccccc", 300), Lookup::Fresh(_)));
    }

    #[test]
    fn cache_cap_replacing_existing_does_not_evict() {
        let cache = KeyCache::new(2);
        cache.insert("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".to_string(), any_key(), 100);
        cache.insert("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".to_string(), any_key(), 200);
        // Re-inserting an existing DID is a refresh, not a new entry, so
        // it must not trigger eviction of the other one.
        cache.insert("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".to_string(), any_key(), 300);

        assert!(matches!(cache.get("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", 300), Lookup::Fresh(_)));
    }

    #[test]
    fn refetch_limiter_allows_one_per_hour() {
        // BC11: at most one refetch per DID per hour, measured from the
        // last time a refetch was actually sent (a `false` result does
        // not reset the window).
        let cache = KeyCache::new(10);
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        cache.insert(did.to_string(), any_key(), 0);

        assert!(cache.should_refetch(did, 0));
        assert!(!cache.should_refetch(did, 10));
        assert!(!cache.should_refetch(did, 3599));
        assert!(cache.should_refetch(did, 3600));
    }

    #[test]
    fn refetch_limiter_allows_unknown_did() {
        let cache = KeyCache::new(10);
        assert!(cache.should_refetch("did:plc:unknown0000000000000000", 0));
    }

    #[test]
    fn refetch_after_insert_does_not_reset_limiter() {
        // Review round 1, defect A: a resolver fetch that lands inside the
        // hourly cooldown (`insert`, the rotation case as much as a plain
        // refresh) must not reopen the window `should_refetch` is
        // tracking.
        let cache = KeyCache::new(10);
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        cache.insert(did.to_string(), any_key(), 0);

        assert!(cache.should_refetch(did, 0));
        // The resolver refetches and re-inserts at t=10, still inside the
        // hour since the refetch at t=0.
        cache.insert(did.to_string(), any_key(), 10);
        assert!(!cache.should_refetch(did, 20), "insert must not reset last_refetch_sent");
        assert!(cache.should_refetch(did, 3600));
    }

    #[test]
    fn miss_limiter_allows_one_until_marked_done() {
        // BC9: a DID with no cached key enqueues at most one outstanding
        // `Miss`, and after that fetch fails, at most one more per hour.
        let cache = KeyCache::new(10);
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

        assert!(cache.should_send_miss(did, 0));
        assert!(!cache.should_send_miss(did, 1), "a fetch for this DID is already in flight");

        cache.miss_fetch_failed(did, 10);
        assert!(!cache.should_send_miss(did, 20), "cooldown after a failed attempt");
        assert!(cache.should_send_miss(did, 3610));
    }

    #[test]
    fn miss_send_dropped_clears_in_flight_without_cooldown() {
        // BC9: a dropped send (the resolver channel was full) must not
        // be treated as a finished attempt — the next request enqueues
        // at once, with no hourly wait.
        let cache = KeyCache::new(10);
        let did = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

        assert!(cache.should_send_miss(did, 0));
        cache.miss_send_dropped(did);
        assert!(cache.should_send_miss(did, 1), "a dropped send must not start the cooldown");
    }

    #[test]
    fn miss_fetch_succeeded_clears_state_so_an_evicted_did_refetches_at_once() {
        // BC9, BC16: a successful fetch removes the miss state entirely;
        // only a failed fetch starts the hourly cooldown. This matters
        // for a DID evicted from `entries` at the cache cap (BC16): its
        // last miss attempt may have succeeded long before the
        // eviction, and it must not wait out a stale cooldown.
        let cache = KeyCache::new(10);
        let did = "did:plc:cccccccccccccccccccccccc";

        assert!(cache.should_send_miss(did, 0));
        cache.miss_fetch_succeeded(did);
        assert!(cache.should_send_miss(did, 1), "a successful fetch must not start a cooldown");

        // A failed fetch, by contrast, does start the hourly cooldown.
        cache.miss_fetch_failed(did, 1);
        assert!(!cache.should_send_miss(did, 2), "a failed fetch must start the hourly cooldown");
        assert!(cache.should_send_miss(did, 3601));
    }

    #[test]
    fn misses_map_stays_bounded_at_the_cache_cap() {
        // BC16: an attacker sending a fresh, well-formed DID on every
        // request must not grow `misses` past `max_entries`. The first
        // `max_entries` DIDs each get their one outstanding `Miss` (the
        // resolver is never run in this test, so nothing naturally clears
        // them); once the map is full and every entry is in flight,
        // pruning frees no room, so every further DID's `Miss` is dropped
        // (BC9) rather than evicting one of the four still in flight.
        let cache = KeyCache::new(4);
        for i in 0..4u32 {
            let did = format!("did:plc:{i:024}");
            assert!(cache.should_send_miss(&did, 0));
            assert_eq!(
                cache.misses.lock().unwrap().len(),
                i as usize + 1,
                "misses must never exceed max_entries"
            );
        }
        for i in 4..100u32 {
            let did = format!("did:plc:{i:024}");
            assert!(!cache.should_send_miss(&did, 0), "a full map of in-flight DIDs drops the new one");
            assert_eq!(cache.misses.lock().unwrap().len(), 4, "misses must never exceed max_entries");
        }
    }

    #[test]
    fn misses_map_full_of_in_flight_or_cooling_drops_new_did_without_evicting() {
        // BC9, BC16: cap 4, all four tracked DIDs are either still in
        // flight or inside their hourly cooldown, so pruning frees no
        // room. A fifth DID's `Miss` must be dropped, and the first four
        // must keep exactly the state they had before the fifth DID ever
        // arrived.
        let cache = KeyCache::new(4);
        let in_flight = "did:plc:000000000000000000000000";
        let cooling = "did:plc:111111111111111111111111";
        let also_in_flight = "did:plc:222222222222222222222222";
        let also_cooling = "did:plc:333333333333333333333333";

        assert!(cache.should_send_miss(in_flight, 0));
        assert!(cache.should_send_miss(also_in_flight, 0));
        assert!(cache.should_send_miss(cooling, 0));
        cache.miss_fetch_failed(cooling, 0);
        assert!(cache.should_send_miss(also_cooling, 0));
        cache.miss_fetch_failed(also_cooling, 0);

        let fifth = "did:plc:444444444444444444444444";
        assert!(!cache.should_send_miss(fifth, 1), "no room and nothing prunable: the new DID is dropped");
        assert_eq!(cache.misses.lock().unwrap().len(), 4, "nothing already tracked is evicted");

        // The four original DIDs kept exactly the state they had: the two
        // in flight still block a second send, and the two cooling down
        // are still inside their hour.
        assert!(!cache.should_send_miss(in_flight, 1), "still in flight");
        assert!(!cache.should_send_miss(also_in_flight, 1), "still in flight");
        assert!(!cache.should_send_miss(cooling, 1), "still cooling down");
        assert!(!cache.should_send_miss(also_cooling, 1), "still cooling down");
    }

    #[tokio::test]
    async fn accumulate_capped_rejects_oversized_body() {
        // Review round 1, defect L: the running total, not the final
        // buffer, is what fails once it crosses MAX_DID_DOCUMENT_BYTES,
        // so an oversized body never gets fully read into memory.
        struct FixedChunks(Vec<Vec<u8>>);
        impl ChunkSource for FixedChunks {
            async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, FetchError> {
                Ok(if self.0.is_empty() { None } else { Some(self.0.remove(0)) })
            }
        }

        let small = FixedChunks(vec![vec![0u8; 10], vec![0u8; 10]]);
        assert_eq!(accumulate_capped(small).await.unwrap().len(), 20);

        let oversized = FixedChunks(vec![vec![0u8; MAX_DID_DOCUMENT_BYTES], vec![0u8; 1]]);
        assert!(matches!(accumulate_capped(oversized).await, Err(FetchError::Decode)));
    }

    // --- resolver: DidFetcher, extract_key, run_resolver ---------------

    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    use k256::ecdsa::signature::Signer as _;
    use serde_json::json;

    use crate::auth::{verify, AuthConfig, AuthError, ViewerDid};

    /// A [`DidFetcher`] that returns one canned result, then panics if
    /// asked again: every test here sends at most one DID through the
    /// resolver, so a second call means the test built the wrong channel
    /// traffic.
    struct FakeFetcher {
        result: Mutex<Option<Result<Value, FetchError>>>,
    }

    impl FakeFetcher {
        fn once(result: Result<Value, FetchError>) -> Self {
            Self { result: Mutex::new(Some(result)) }
        }
    }

    impl DidFetcher for FakeFetcher {
        async fn fetch(&self, _did: &str) -> Result<Value, FetchError> {
            self.result.lock().unwrap().take().expect("fetch called more than once in this test")
        }
    }

    fn k256_multibase(vk: &k256::ecdsa::VerifyingKey) -> String {
        let point = vk.to_sec1_point(true);
        let mut bytes = vec![0xE7, 0x01];
        bytes.extend_from_slice(point.as_bytes());
        format!("z{}", bs58::encode(bytes).into_string())
    }

    fn p256_multibase(vk: &p256::ecdsa::VerifyingKey) -> String {
        let point = vk.to_sec1_point(true);
        let mut bytes = vec![0x80, 0x24];
        bytes.extend_from_slice(point.as_bytes());
        format!("z{}", bs58::encode(bytes).into_string())
    }

    /// A DID document with one verification method whose `id` ends
    /// `#atproto`, holding `multibase` (BC19).
    fn did_document(did: &str, multibase: &str) -> Value {
        json!({
            "id": did,
            "verificationMethod": [
                {
                    "id": format!("{did}#atproto"),
                    "type": "Multikey",
                    "controller": did,
                    "publicKeyMultibase": multibase,
                }
            ],
        })
    }

    /// Runs `run_resolver` to completion over a channel that already
    /// holds `request`, then closes the channel so the task returns.
    async fn resolve_one(request: ResolveRequest, fetcher: FakeFetcher, cache: Arc<KeyCache>) {
        let (tx, rx) = mpsc::channel(8);
        tx.try_send(request).expect("channel just built, has room for one");
        drop(tx);
        run_resolver(rx, fetcher, cache).await;
    }

    #[tokio::test]
    async fn fixtures() {
        // AC4: multibase keys decode out of did:plc and did:web fixtures.
        let k256_sk = k256::ecdsa::SigningKey::from_slice(&[1u8; 32]).unwrap();
        let plc_did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let doc = did_document(plc_did, &k256_multibase(k256_sk.verifying_key()));
        let cache = Arc::new(KeyCache::new(10));
        resolve_one(
            ResolveRequest::Miss(plc_did.to_string()),
            FakeFetcher::once(Ok(doc)),
            Arc::clone(&cache),
        )
        .await;
        assert!(matches!(cache.get(plc_did, 0), Lookup::Fresh(PublicKey::K256(_))));

        let p256_sk = p256::ecdsa::SigningKey::from_slice(&[2u8; 32]).unwrap();
        let web_did = "did:web:feed.example";
        let doc = did_document(web_did, &p256_multibase(p256_sk.verifying_key()));
        let cache = Arc::new(KeyCache::new(10));
        resolve_one(
            ResolveRequest::Miss(web_did.to_string()),
            FakeFetcher::once(Ok(doc)),
            Arc::clone(&cache),
        )
        .await;
        assert!(matches!(cache.get(web_did, 0), Lookup::Fresh(PublicKey::P256(_))));
    }

    #[tokio::test]
    async fn fetch_failure_caches_nothing() {
        // BC20: a fetch failure caches nothing (the warning it logs is
        // not asserted here; `tracing` has no test-friendly return value,
        // and BC21's "no DID, no token" is enforced by `FetchError`
        // simply never carrying either).
        let did = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        let cache = Arc::new(KeyCache::new(10));
        resolve_one(
            ResolveRequest::Miss(did.to_string()),
            FakeFetcher::once(Err(FetchError::Http(500))),
            Arc::clone(&cache),
        )
        .await;
        assert!(matches!(cache.get(did, 0), Lookup::Missing));
    }

    #[tokio::test]
    async fn miss_fetch_with_no_usable_key_starts_the_hourly_cooldown() {
        // BC9, BC19: a fetch that succeeds but whose document has no
        // usable `#atproto` key must be treated as a failed `Miss`
        // attempt, not a successful one, so it does not leave the DID
        // retryable on every request.
        //
        // `run_resolver` stamps the failed attempt with the real clock
        // (`store::unix_now()`, this file's own doc comment on
        // `run_resolver`), so this test judges the cooldown against that
        // same clock rather than an arbitrary fixed epoch.
        let did = "did:plc:ffffffffffffffffffffffff";
        let doc = json!({ "id": did, "verificationMethod": [] });
        let cache = Arc::new(KeyCache::new(10));
        let now = crate::store::unix_now();

        assert!(cache.should_send_miss(did, now), "first miss for this DID enqueues a fetch");
        resolve_one(
            ResolveRequest::Miss(did.to_string()),
            FakeFetcher::once(Ok(doc)),
            Arc::clone(&cache),
        )
        .await;

        assert!(matches!(cache.get(did, now), Lookup::Missing), "no usable key was ever cached");
        assert!(
            !cache.should_send_miss(did, now + 1),
            "a keyless document counts as a failed attempt and starts the cooldown"
        );
        assert!(
            cache.should_send_miss(did, now + 3601),
            "the cooldown ends after an hour"
        );
    }

    #[test]
    fn resolve_url_did_plc() {
        // BC17.
        assert_eq!(
            resolve_url("https://plc.directory", "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"),
            "https://plc.directory/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn resolve_url_did_web() {
        // BC18: the `plc_url` argument is not consulted at all for a
        // did:web DID.
        assert_eq!(
            resolve_url("https://plc.directory", "did:web:feed.example"),
            "https://feed.example/.well-known/did.json"
        );
    }

    #[test]
    fn extract_key_none_without_atproto_method() {
        // BC19: a document with no `#atproto` verification method caches
        // nothing.
        let doc = json!({
            "id": "did:plc:cccccccccccccccccccccccc",
            "verificationMethod": [
                {
                    "id": "did:plc:cccccccccccccccccccccccc#other",
                    "type": "Multikey",
                    "publicKeyMultibase": "zNotChecked",
                }
            ],
        });
        assert!(extract_key(&doc).is_none());
    }

    #[test]
    fn extract_key_none_when_undecodable() {
        // BC19: an `#atproto` method whose key does not decode caches
        // nothing, rather than panicking or defaulting to some key.
        let doc = did_document("did:plc:dddddddddddddddddddddddd", "not-multibase");
        assert!(extract_key(&doc).is_none());
    }

    /// Builds and signs a k256 ES256K token for `viewer_did`, `exp` 60s
    /// ahead of `now`. Mirrors `mod.rs`'s own `sign_token` test helper,
    /// duplicated here because that one is private to `mod.rs`'s test
    /// module.
    fn sign_es256k(
        sk: &k256::ecdsa::SigningKey,
        viewer_did: &str,
        service_did: &str,
        now: i64,
    ) -> String {
        let header_b64 = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K"}"#);
        let payload = format!(
            r#"{{"iss":"{viewer_did}","aud":"{service_did}","exp":{},"lxm":"app.bsky.feed.getFeedSkeleton"}}"#,
            now + 60
        );
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload);
        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig: k256::ecdsa::Signature = sk.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(sig.to_bytes());
        format!("{signing_input}.{sig_b64}")
    }

    #[tokio::test]
    async fn rotation_refetch() {
        // AC5: a token signed by a rotated key fails to verify against
        // the stale cached key, enqueues exactly one refetch (BC11), and
        // verifies once the resolver has run and installed the new key.
        //
        // `now` is the real clock, not a fixed epoch like `mod.rs`'s and
        // `jwt.rs`'s tests use: `run_resolver`'s `insert` stamps the new
        // key with `store::unix_now()` (this file's own doc comment on
        // `run_resolver`), so the final `verify` call must judge freshness
        // against that same clock, not an arbitrary one.
        let now = crate::store::unix_now();
        const SERVICE_DID: &str = "did:web:feed.example";
        let did = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";

        let old_sk = k256::ecdsa::SigningKey::from_slice(&[3u8; 32]).unwrap();
        let new_sk = k256::ecdsa::SigningKey::from_slice(&[4u8; 32]).unwrap();

        let cache = Arc::new(KeyCache::new(10));
        cache.insert(
            did.to_string(),
            decode_multibase(&k256_multibase(old_sk.verifying_key())).unwrap(),
            now,
        );
        let cfg = AuthConfig { service_did: SERVICE_DID.to_string() };
        let (tx, rx) = mpsc::channel(8);

        // Token is signed by the new key, so the stale cached (old) key
        // fails to verify it.
        let token = sign_es256k(&new_sk, did, SERVICE_DID, now);
        assert_eq!(verify(&token, now, &cache, &cfg, &tx), Err(AuthError::Signature));

        // BC11: exactly one refetch was enqueued. Dropping `tx` closes
        // the channel once that one message is drained, so `run_resolver`
        // below processes it and returns rather than waiting forever.
        drop(tx);
        let doc = did_document(did, &k256_multibase(new_sk.verifying_key()));
        run_resolver(rx, FakeFetcher::once(Ok(doc)), Arc::clone(&cache)).await;

        // The same token verifies now that the cache holds the new key.
        let (tx2, _rx2) = mpsc::channel(8);
        assert_eq!(verify(&token, now, &cache, &cfg, &tx2), Ok(ViewerDid(did.to_string())));
    }
}
