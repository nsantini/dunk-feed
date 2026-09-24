//! `KeyCache`: the DID-to-key store `auth::verify` (`mod.rs`) reads on
//! every request, and the only piece of state design §5's cache rules
//! (BC14 to BC16) describe. Every method here is synchronous and returns
//! before yielding, so the mutex guarding it (spec `## Approach`) is
//! never held across an `.await`.
//!
//! Slice 2.0 adds the `DidFetcher` trait, the `reqwest` fetcher, DID
//! document parsing and the resolver task that fills this cache from
//! `ResolveRequest`s (`mod.rs`). This slice ships `KeyCache` alone so
//! `auth::verify` has something to read from day one.

use std::collections::HashMap;
use std::sync::Mutex;

use super::keys::PublicKey;

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
}
