//! Multibase key decode and ECDSA verification for the two curves ATProto
//! signs with: k256 (secp256k1, `alg: ES256K`) and p256 (NIST P-256,
//! `alg: ES256`). `docs/02-TECH-DESIGN-network-feed.md` §5's key encoding:
//! a `did:key`-style `z`-prefixed base58btc string, a two-byte multicodec
//! prefix, then a compressed SEC1 point.

use k256::ecdsa::signature::Verifier as _;

use super::jwt::Alg;
use super::AuthError;

/// Multicodec prefix for a compressed secp256k1 public key
/// (`secp256k1-pub`, design §5).
const K256_MULTICODEC: [u8; 2] = [0xE7, 0x01];

/// Multicodec prefix for a compressed NIST P-256 public key (`p256-pub`,
/// design §5).
const P256_MULTICODEC: [u8; 2] = [0x80, 0x24];

/// A verification key decoded from a DID document's `publicKeyMultibase`,
/// tagged by the curve its bytes belong to. `did.rs`'s `KeyCache` stores
/// this; `Clone` is needed to hand a copy out of the cache's mutex guard.
#[derive(Clone)]
pub enum PublicKey {
    K256(Box<k256::ecdsa::VerifyingKey>),
    P256(Box<p256::ecdsa::VerifyingKey>),
}

/// Decodes a multibase `publicKeyMultibase` value (BC19): `z` (base58btc),
/// then the two-byte multicodec prefix, then a compressed point. Any
/// other prefix character, base58 that does not decode, an unrecognised
/// multicodec, or a point `k256`/`p256` rejects as invalid all return
/// `None` — the resolver (`did.rs`, slice 2.0) treats that as "no usable
/// key" rather than an error, per BC19.
pub(super) fn decode_multibase(value: &str) -> Option<PublicKey> {
    let body = value.strip_prefix('z')?;
    let bytes = bs58::decode(body).into_vec().ok()?;
    let (prefix, point) = bytes.split_at_checked(2)?;
    if prefix == K256_MULTICODEC {
        let key = k256::ecdsa::VerifyingKey::from_sec1_bytes(point).ok()?;
        Some(PublicKey::K256(Box::new(key)))
    } else if prefix == P256_MULTICODEC {
        let key = p256::ecdsa::VerifyingKey::from_sec1_bytes(point).ok()?;
        Some(PublicKey::P256(Box::new(key)))
    } else {
        None
    }
}

/// Verifies `signature` (raw `r || s`, 64 bytes, the JWT ECDSA
/// convention) over `message` with `key`. `alg` must match `key`'s curve,
/// or this is `AuthError::Signature` without a verify call — same as a
/// signature that fails to verify, since neither can be a signature this
/// key produced. BC10: a high-S signature is rejected before `k256` or
/// `p256` is asked to verify it.
pub(super) fn verify(
    key: &PublicKey,
    alg: Alg,
    message: &[u8],
    signature: &[u8],
) -> Result<(), AuthError> {
    match (key, alg) {
        (PublicKey::K256(vk), Alg::Es256k) => {
            let sig =
                k256::ecdsa::Signature::from_slice(signature).map_err(|_| AuthError::Signature)?;
            if sig.normalize_s() != sig {
                return Err(AuthError::Signature);
            }
            vk.verify(message, &sig).map_err(|_| AuthError::Signature)
        }
        (PublicKey::P256(vk), Alg::Es256) => {
            let sig =
                p256::ecdsa::Signature::from_slice(signature).map_err(|_| AuthError::Signature)?;
            if sig.normalize_s() != sig {
                return Err(AuthError::Signature);
            }
            vk.verify(message, &sig).map_err(|_| AuthError::Signature)
        }
        _ => Err(AuthError::Signature),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::signature::Signer as _;

    /// A fixed 32-byte scalar, well under either curve's order, so tests
    /// need no RNG: `SigningKey::from_slice` accepts any nonzero scalar
    /// less than the order, and both orders start `0xff...`.
    const TEST_SCALAR: [u8; 32] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ];

    fn k256_multibase(vk: &k256::ecdsa::VerifyingKey) -> String {
        let point = vk.to_sec1_point(true);
        let mut bytes = K256_MULTICODEC.to_vec();
        bytes.extend_from_slice(point.as_bytes());
        format!("z{}", bs58::encode(bytes).into_string())
    }

    fn p256_multibase(vk: &p256::ecdsa::VerifyingKey) -> String {
        let point = vk.to_sec1_point(true);
        let mut bytes = P256_MULTICODEC.to_vec();
        bytes.extend_from_slice(point.as_bytes());
        format!("z{}", bs58::encode(bytes).into_string())
    }

    #[test]
    fn decodes_k256_multibase() {
        let sk = k256::ecdsa::SigningKey::from_slice(&TEST_SCALAR).unwrap();
        let multibase = k256_multibase(sk.verifying_key());
        assert!(matches!(decode_multibase(&multibase), Some(PublicKey::K256(_))));
    }

    #[test]
    fn decodes_p256_multibase() {
        let sk = p256::ecdsa::SigningKey::from_slice(&TEST_SCALAR).unwrap();
        let multibase = p256_multibase(sk.verifying_key());
        assert!(matches!(decode_multibase(&multibase), Some(PublicKey::P256(_))));
    }

    #[test]
    fn rejects_unknown_multicodec() {
        let bytes = [0x00, 0x00, 0x01, 0x02, 0x03];
        let multibase = format!("z{}", bs58::encode(bytes).into_string());
        assert!(decode_multibase(&multibase).is_none());
    }

    #[test]
    fn rejects_missing_z_prefix() {
        assert!(decode_multibase("abcdef").is_none());
    }

    #[test]
    fn verifies_k256_signature() {
        let sk = k256::ecdsa::SigningKey::from_slice(&TEST_SCALAR).unwrap();
        let key = PublicKey::K256(Box::new(*sk.verifying_key()));
        let message = b"header.payload";
        let sig: k256::ecdsa::Signature = sk.sign(message);
        assert!(verify(&key, Alg::Es256k, message, &sig.to_bytes()).is_ok());
    }

    #[test]
    fn verifies_p256_signature() {
        let sk = p256::ecdsa::SigningKey::from_slice(&TEST_SCALAR).unwrap();
        let key = PublicKey::P256(Box::new(*sk.verifying_key()));
        let message = b"header.payload";
        let sig: p256::ecdsa::Signature = sk.sign(message);
        assert!(verify(&key, Alg::Es256, message, &sig.to_bytes()).is_ok());
    }

    #[test]
    fn rejects_high_s_before_verify() {
        // p256 does not normalize `s` when signing (`NORMALIZE_S = false`,
        // unlike k256's `true`), so trying a handful of messages finds a
        // naturally high-S signature without reaching into curve
        // internals. BC10 must reject it without `verify` reporting a
        // pass.
        let sk = p256::ecdsa::SigningKey::from_slice(&TEST_SCALAR).unwrap();
        let key = PublicKey::P256(Box::new(*sk.verifying_key()));
        let mut found: Option<(Vec<u8>, p256::ecdsa::Signature)> = None;
        for i in 0..64u32 {
            let message = format!("message-{i}").into_bytes();
            let sig: p256::ecdsa::Signature = sk.sign(&message);
            if sig.normalize_s() != sig {
                found = Some((message, sig));
                break;
            }
        }
        let (message, sig) = found.expect("one of 64 messages should sign high-S");
        assert_eq!(verify(&key, Alg::Es256, &message, &sig.to_bytes()), Err(AuthError::Signature));
    }
}
