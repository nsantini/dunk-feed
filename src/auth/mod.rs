//! Viewer authentication, `docs/02-TECH-DESIGN-network-feed.md` §5.
//! `verify` is the single entry point the request path calls when
//! `UPSTAGE_PERSONALISE` is `true`: it runs the JWT structural checks
//! (`jwt.rs`), reads the DID key cache (`did.rs`), and checks the
//! signature (`keys.rs`). It never waits on the network — a cache miss or
//! a stale or failed-verify entry sends the DID to the resolver task over
//! a bounded channel with `try_send`, and a full channel just drops that
//! send, because the next request enqueues the same DID again.
//!
//! `auth/` is the only module that calls the DID resolvers
//! (`UPSTAGE_PLC_URL`, `did:web` hosts) — see `AGENTS.md`.
//!
//! First callers are `src/ingest/mod.rs` (which builds the `KeyCache` and
//! starts the resolver task) and `src/http/skeleton.rs` (which calls
//! `verify`), both slice 3.0. Slice 2.0 adds the resolver task itself in
//! `did.rs`. Until then nothing in the binary calls into this module.
#![allow(dead_code)]

mod did;
mod jwt;
mod keys;

use thiserror::Error;
use tokio::sync::mpsc;

pub use did::KeyCache;

/// Everything `verify` needs that is not the request itself:
/// `UPSTAGE_SERVICE_DID` (BC6). Built once from `Config` and cloned into
/// `AppState` (`src/ingest/mod.rs`, slice 3.0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthConfig {
    pub service_did: String,
}

/// A DID resolved and verified from a request's bearer token (BC12). Any
/// `#` fragment on the token's `iss` was already removed (BC8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewerDid(pub String);

/// A DID sent to the resolver task (`did.rs`, slice 2.0) over the bounded
/// channel. `Miss` is a DID the cache has never held a key for (BC9);
/// `Refetch` is a DID whose cached key is stale (BC14) or just failed to
/// verify a signature (BC11) — the resolver treats both the same way,
/// fetching the DID document again because the key can rotate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveRequest {
    Miss(String),
    Refetch(String),
}

/// Every way `verify` can reject a request. The handler
/// (`src/http/skeleton.rs`, slice 3.0) turns every variant into the same
/// empty 200 page (BC26); a `debug` log line names which one, and BC21
/// keeps the DID and the token out of that line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AuthError {
    #[error("malformed token")]
    Malformed,
    #[error("unsupported alg or typ")]
    Alg,
    #[error("expired")]
    Expired,
    #[error("wrong audience")]
    Audience,
    #[error("wrong lxm")]
    Method,
    #[error("bad issuer")]
    Issuer,
    #[error("key not cached")]
    KeyUnknown,
    #[error("signature did not verify")]
    Signature,
}

/// Runs design §5's checks 1 to 7 in order (BC3 to BC12). `now` is the
/// unix second the request arrived (`src/http/skeleton.rs` passes
/// `store::unix_now()`; tests pass a fixed value, so cache aging is
/// deterministic). `resolver_tx` is the resolver task's bounded channel
/// sender (`did.rs`, slice 2.0); a full channel drops the send rather
/// than blocking, because the request path never waits on it.
pub fn verify(
    token: &str,
    now: i64,
    cache: &KeyCache,
    cfg: &AuthConfig,
    resolver_tx: &mpsc::Sender<ResolveRequest>,
) -> Result<ViewerDid, AuthError> {
    let checked = jwt::check(token, now, &cfg.service_did)?;

    let key = match cache.get(&checked.viewer_did, now) {
        did::Lookup::Fresh(key) => key,
        did::Lookup::Stale(key) => {
            if cache.should_refetch(&checked.viewer_did, now) {
                let _ = resolver_tx.try_send(ResolveRequest::Refetch(checked.viewer_did.clone()));
            }
            key
        }
        did::Lookup::Missing => {
            let _ = resolver_tx.try_send(ResolveRequest::Miss(checked.viewer_did.clone()));
            return Err(AuthError::KeyUnknown);
        }
    };

    match keys::verify(&key, checked.alg, &checked.signing_input, &checked.signature) {
        Ok(()) => Ok(ViewerDid(checked.viewer_did)),
        Err(err) => {
            // BC11: one refetch per DID per hour on a signature failure
            // with a cached key, because the key can have rotated.
            if cache.should_refetch(&checked.viewer_did, now) {
                let _ = resolver_tx.try_send(ResolveRequest::Refetch(checked.viewer_did.clone()));
            }
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    use k256::ecdsa::signature::Signer as _;

    use super::*;
    use keys::PublicKey;

    const SERVICE_DID: &str = "did:web:feed.example";
    const NOW: i64 = 1_800_000_000;

    fn channel() -> (mpsc::Sender<ResolveRequest>, mpsc::Receiver<ResolveRequest>) {
        mpsc::channel(8)
    }

    fn cfg() -> AuthConfig {
        AuthConfig { service_did: SERVICE_DID.to_string() }
    }

    /// Signs a token for `viewer_did` with `signing_key`, using `alg` in
    /// the header. `exp_offset` is added to [`NOW`] for the `exp` claim,
    /// so tests can build both a valid and an expired token from the same
    /// helper.
    fn sign_token(
        viewer_did: &str,
        alg: &str,
        exp_offset: i64,
        sign: impl FnOnce(&[u8]) -> Vec<u8>,
    ) -> String {
        let header = format!(r#"{{"alg":"{alg}"}}"#);
        let payload = format!(
            r#"{{"iss":"{viewer_did}","aud":"{SERVICE_DID}","exp":{},"lxm":"app.bsky.feed.getFeedSkeleton"}}"#,
            NOW + exp_offset
        );
        let header_b64 = URL_SAFE_NO_PAD.encode(&header);
        let payload_b64 = URL_SAFE_NO_PAD.encode(&payload);
        let signing_input = format!("{header_b64}.{payload_b64}");
        let signature = sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(&signature);
        format!("{signing_input}.{sig_b64}")
    }

    /// A fixed 32-byte scalar for k256 test keys, well under the curve
    /// order, so no RNG is needed.
    const K256_SCALAR: [u8; 32] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ];

    /// A distinct fixed 32-byte scalar for p256 test keys.
    const P256_SCALAR: [u8; 32] = [
        0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e,
        0x3f, 0x40,
    ];

    fn k256_signing_key() -> k256::ecdsa::SigningKey {
        k256::ecdsa::SigningKey::from_slice(&K256_SCALAR).unwrap()
    }

    fn p256_signing_key() -> p256::ecdsa::SigningKey {
        p256::ecdsa::SigningKey::from_slice(&P256_SCALAR).unwrap()
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

    fn cache_with_k256(did: &str, vk: &k256::ecdsa::VerifyingKey, now: i64) -> KeyCache {
        let cache = KeyCache::new(10);
        let key = keys::decode_multibase(&k256_multibase(vk)).expect("k256 key should decode");
        insert_for_test(&cache, did, key, now);
        cache
    }

    fn cache_with_p256(did: &str, vk: &p256::ecdsa::VerifyingKey, now: i64) -> KeyCache {
        let cache = KeyCache::new(10);
        let key = keys::decode_multibase(&p256_multibase(vk)).expect("p256 key should decode");
        insert_for_test(&cache, did, key, now);
        cache
    }

    /// `KeyCache::insert` is `pub(super)`, so the module's own tests reach
    /// it directly rather than through a resolver that slice 2.0 has not
    /// written yet.
    fn insert_for_test(cache: &KeyCache, did: &str, key: PublicKey, now: i64) {
        cache.insert(did.to_string(), key, now);
    }

    #[test]
    fn valid_tokens() {
        // AC1: both ES256K and ES256 tokens verify end to end.
        let k256_sk = k256_signing_key();
        let k256_did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let k256_cache = cache_with_k256(k256_did, k256_sk.verifying_key(), NOW);
        let (tx, _rx) = channel();
        let token = sign_token(k256_did, "ES256K", 60, |msg| {
            let sig: k256::ecdsa::Signature = k256_sk.sign(msg);
            sig.to_bytes().to_vec()
        });
        assert_eq!(
            verify(&token, NOW, &k256_cache, &cfg(), &tx),
            Ok(ViewerDid(k256_did.to_string()))
        );

        let p256_sk = p256_signing_key();
        let p256_did = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        let p256_cache = cache_with_p256(p256_did, p256_sk.verifying_key(), NOW);
        let token = sign_token(p256_did, "ES256", 60, |msg| {
            use p256::ecdsa::signature::Signer as _;
            let sig: p256::ecdsa::Signature = p256_sk.sign(msg);
            sig.to_bytes().to_vec()
        });
        assert_eq!(
            verify(&token, NOW, &p256_cache, &cfg(), &tx),
            Ok(ViewerDid(p256_did.to_string()))
        );
    }

    #[test]
    fn rejects() {
        // AC2: expired, wrong aud, wrong lxm, bad iss, unknown alg,
        // high-S and tampered tokens each fail with the matching error.
        let sk = k256_signing_key();
        let did = "did:plc:cccccccccccccccccccccccc";
        let cache = cache_with_k256(did, sk.verifying_key(), NOW);
        let (tx, _rx) = channel();
        let sign = |msg: &[u8]| -> Vec<u8> {
            let sig: k256::ecdsa::Signature = sk.sign(msg);
            sig.to_bytes().to_vec()
        };

        // Expired.
        let expired = sign_token(did, "ES256K", -3600, sign);
        assert_eq!(verify(&expired, NOW, &cache, &cfg(), &tx), Err(AuthError::Expired));

        // Wrong aud: build by hand since `sign_token` always uses
        // `SERVICE_DID`.
        let header_b64 = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K"}"#);
        let payload_b64 = URL_SAFE_NO_PAD.encode(format!(
            r#"{{"iss":"{did}","aud":"did:web:wrong.example","exp":{}}}"#,
            NOW + 60
        ));
        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig_b64 = URL_SAFE_NO_PAD.encode(sign(signing_input.as_bytes()));
        let wrong_aud = format!("{signing_input}.{sig_b64}");
        assert_eq!(verify(&wrong_aud, NOW, &cache, &cfg(), &tx), Err(AuthError::Audience));

        // Wrong lxm.
        let payload_b64 = URL_SAFE_NO_PAD.encode(format!(
            r#"{{"iss":"{did}","aud":"{SERVICE_DID}","exp":{},"lxm":"app.bsky.feed.getTimeline"}}"#,
            NOW + 60
        ));
        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig_b64 = URL_SAFE_NO_PAD.encode(sign(signing_input.as_bytes()));
        let wrong_lxm = format!("{signing_input}.{sig_b64}");
        assert_eq!(verify(&wrong_lxm, NOW, &cache, &cfg(), &tx), Err(AuthError::Method));

        // Bad iss.
        let payload_b64 = URL_SAFE_NO_PAD.encode(format!(
            r#"{{"iss":"did:example:nope","aud":"{SERVICE_DID}","exp":{}}}"#,
            NOW + 60
        ));
        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig_b64 = URL_SAFE_NO_PAD.encode(sign(signing_input.as_bytes()));
        let bad_iss = format!("{signing_input}.{sig_b64}");
        assert_eq!(verify(&bad_iss, NOW, &cache, &cfg(), &tx), Err(AuthError::Issuer));

        // Unknown alg.
        let header_b64 = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256"}"#);
        let payload_b64 = URL_SAFE_NO_PAD
            .encode(format!(r#"{{"iss":"{did}","aud":"{SERVICE_DID}","exp":{}}}"#, NOW + 60));
        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig_b64 = URL_SAFE_NO_PAD.encode(sign(signing_input.as_bytes()));
        let unknown_alg = format!("{signing_input}.{sig_b64}");
        assert_eq!(verify(&unknown_alg, NOW, &cache, &cfg(), &tx), Err(AuthError::Alg));

        // Tampered payload: change one digit of `exp` after signing, so
        // the payload stays valid JSON and every structural check still
        // passes (`exp` is still far in the future), but the bytes that
        // were signed no longer match what is on the wire.
        let valid = sign_token(did, "ES256K", 60, sign);
        let mut parts: Vec<&str> = valid.split('.').collect();
        let payload_json = String::from_utf8(URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        let tampered_json =
            payload_json.replacen(&(NOW + 60).to_string(), &(NOW + 61).to_string(), 1);
        let tampered_payload_b64 = URL_SAFE_NO_PAD.encode(tampered_json);
        parts[1] = &tampered_payload_b64;
        let tampered = parts.join(".");
        assert_eq!(verify(&tampered, NOW, &cache, &cfg(), &tx), Err(AuthError::Signature));

        // High-S: p256 does not normalize `s` on signing, unlike k256, so
        // a handful of messages finds a naturally high-S signature (BC10
        // rejects it before a verify call, same outward error).
        let p256_sk = p256_signing_key();
        let p256_did = "did:plc:ffffffffffffffffffffffff";
        let p256_cache = cache_with_p256(p256_did, p256_sk.verifying_key(), NOW);
        let mut high_s_token = None;
        for i in 0..64u32 {
            let candidate = sign_token(p256_did, "ES256", 60 + i as i64, |msg| {
                use p256::ecdsa::signature::Signer as _;
                let sig: p256::ecdsa::Signature = p256_sk.sign(msg);
                sig.to_bytes().to_vec()
            });
            let sig_b64 = candidate.rsplit('.').next().unwrap();
            let sig_bytes = URL_SAFE_NO_PAD.decode(sig_b64).unwrap();
            let sig = p256::ecdsa::Signature::from_slice(&sig_bytes).unwrap();
            if sig.normalize_s() != sig {
                high_s_token = Some(candidate);
                break;
            }
        }
        let high_s_token = high_s_token.expect("one of 64 exp offsets should sign high-S");
        assert_eq!(verify(&high_s_token, NOW, &p256_cache, &cfg(), &tx), Err(AuthError::Signature));
    }

    #[test]
    fn unknown_key_sends_miss_and_returns_key_unknown() {
        // BC9: a DID with no cache entry returns `KeyUnknown` at once and
        // enqueues a `Miss`, without any network call on this path.
        let cache = KeyCache::new(10);
        let (tx, mut rx) = channel();
        let did = "did:plc:dddddddddddddddddddddddd";
        let token = sign_token(did, "ES256K", 60, |_msg| vec![0u8; 64]);
        assert_eq!(verify(&token, NOW, &cache, &cfg(), &tx), Err(AuthError::KeyUnknown));
        assert_eq!(rx.try_recv(), Ok(ResolveRequest::Miss(did.to_string())));
    }

    #[test]
    fn signature_failure_sends_one_refetch_per_hour() {
        // BC11: a signature failure with a cached key enqueues one
        // refetch, then no more within the hour.
        let sk = k256_signing_key();
        let other_sk = k256::ecdsa::SigningKey::from_slice(&[9u8; 32]).unwrap();
        let did = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
        let cache = cache_with_k256(did, sk.verifying_key(), NOW);
        let (tx, mut rx) = channel();
        // Signed by the wrong key, so verification fails.
        let token = sign_token(did, "ES256K", 60, |msg| {
            let sig: k256::ecdsa::Signature = other_sk.sign(msg);
            sig.to_bytes().to_vec()
        });

        assert_eq!(verify(&token, NOW, &cache, &cfg(), &tx), Err(AuthError::Signature));
        assert_eq!(rx.try_recv(), Ok(ResolveRequest::Refetch(did.to_string())));

        assert_eq!(verify(&token, NOW + 10, &cache, &cfg(), &tx), Err(AuthError::Signature));
        assert!(rx.try_recv().is_err(), "second failure within the hour must not enqueue again");
    }
}
