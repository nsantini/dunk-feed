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
}

impl KeyCache {
    pub fn new(max_entries: usize) -> Self {
        Self { max_entries, entries: Mutex::new(HashMap::new()) }
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
        entries.insert(did, Entry { key, fetched_at: now, last_refetch_sent: None });
    }

    /// `true` the first time this is called for `did` within a rolling
    /// hour (BC11): callers in `mod.rs` send a refetch request only when
    /// this returns `true`. A `did` the cache has never held an entry for
    /// always returns `true` — that path is BC9's `Miss`, not a refetch,
    /// but the limiter itself does not need to distinguish the two.
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

/// The real [`DidFetcher`], built on the crate's one `reqwest::Client`
/// builder ([`crate::appview::http_client`]) so the timeout and TLS
/// backend are stated once (`appview/mod.rs`'s own doc comment).
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
            http: crate::appview::http_client(),
            plc_url: plc_url.trim_end_matches('/').to_string(),
        }
    }
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
            return Err(FetchError::Http(status));
        }
        response.json::<Value>().await.map_err(|_err| FetchError::Decode)
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
/// if any, might be stale, so the document is fetched fresh. A fetch
/// failure logs one warning naming only the failure kind (BC20, BC21) and
/// caches nothing; a fetch that succeeds but yields no usable key
/// ([`extract_key`] returning `None`, BC19) caches nothing and logs
/// nothing — an absent `#atproto` method is not a transport or server
/// failure worth a warning on every retry.
pub(super) async fn run_resolver<F: DidFetcher>(
    mut requests: mpsc::Receiver<ResolveRequest>,
    fetcher: F,
    cache: Arc<KeyCache>,
) {
    while let Some(request) = requests.recv().await {
        let did = match request {
            ResolveRequest::Miss(did) | ResolveRequest::Refetch(did) => did,
        };
        match fetcher.fetch(&did).await {
            Ok(document) => {
                if let Some(key) = extract_key(&document) {
                    cache.insert(did, key, crate::store::unix_now());
                }
            }
            Err(err) => {
                tracing::warn!(kind = ?err, "auth: did document fetch failed");
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
