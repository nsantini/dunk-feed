//! The `getFeedSkeleton` pagination cursor, pure and unit-testable without a
//! server. TECH-DESIGN section 11.1 (rewritten at `c4c6fbd`, story 08 slice
//! 7.0): a cursor is `base64url(generation ":" index ":" rank_bits_hex ":"
//! quote_cid)`, no padding. `generation` is the scorer pass counter the
//! snapshot was built in; `index` is the position of the last item served
//! within THAT generation's list. Carrying both lets
//! `src/http/skeleton.rs`'s page resolution find an exact O(1) resume point
//! (path 1) whenever the snapshot handle still holds that generation,
//! falling back to a cid scan (path 2) or the rank/cid scan this module's
//! comparator used to run alone (path 3, `snapshot::cmp_rank_then_cid`,
//! round 2 finding 6 — this module keeps no comparator of its own).
//!
//! Slice 1.0's cursor carried only `rank_bits_hex ":" quote_cid` — two
//! fields, the position implicit in list order alone. That could not
//! distinguish "the cursor's item is still exactly where it was" from "the
//! list changed shape underneath it", so every request paid a full linear
//! scan and a cap-2 deferral could still repeat or drop an item within a
//! single generation. The two extra fields turn the common case (no swap
//! since the last request) into an O(1) lookup and narrow the accepted
//! truncation (BC39, now rewritten) down to path 3 alone.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use thiserror::Error;

/// A cursor failed to decode. Mapped to `SkeletonError::InvalidRequest` by
/// `src/http/skeleton.rs` (BC6, BC52).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CursorError {
    #[error("cursor is not valid base64url")]
    Base64,
    #[error("cursor is not valid UTF-8")]
    Utf8,
    #[error("cursor does not have exactly four ':'-separated fields")]
    WrongFieldCount,
    #[error("cursor's generation field is not numeric")]
    BadGeneration,
    #[error("cursor's index field is not numeric")]
    BadIndex,
    #[error("cursor's rank field is not exactly 16 hex digits")]
    BadRankField,
    #[error("cursor's CID field is empty")]
    EmptyCid,
}

/// Encodes `generation`, `index`, `rank` and `cid` as a base64url (no
/// padding) cursor (BC51): `generation ":" index ":" rank_bits_hex ":"
/// quote_cid`. Hex on the rank's raw bits, not the float itself, keeps
/// `0.0` and `-0.0` distinct and the round trip exact. `quote_cid` never
/// itself holds a `:` (AT Protocol CIDs are base32/base58 text), so `decode`
/// can require exactly four `:`-separated fields and reject anything else.
pub fn encode(generation: u64, index: usize, rank: f64, cid: &str) -> String {
    let raw = format!("{generation}:{index}:{:016x}:{cid}", rank.to_bits());
    URL_SAFE_NO_PAD.encode(raw.as_bytes())
}

/// Decodes a cursor produced by `encode`, or fails with the specific
/// `CursorError` `src/http/skeleton.rs`'s `getFeedSkeleton` handler maps to
/// `InvalidRequest` (BC6, BC52).
///
/// Splits on `:` into exactly four fields (`WrongFieldCount` on too few or
/// too many). `quote_cid` never holds a `:`, so a genuine cursor this
/// module's own `encode` produced always lands on exactly four segments;
/// requiring the exact count, rather than only splitting the first three
/// separators and swallowing the rest into the cid, also rejects a
/// malformed cursor whose CID field was corrupted into carrying a stray
/// `:` instead of silently accepting it.
///
/// Round 2 finding 10 (carried over from slice 1.0's cursor): returns an
/// owned `String` for the CID rather than a `&str` borrowed from `cursor`.
/// A borrow would tie the result to the intermediate `text` this function
/// builds from the base64 bytes, so the caller would have to keep that
/// buffer alive alongside the returned tuple instead of getting one
/// self-contained value back. The cost is one short string clone per
/// request, well inside the 5 ms budget TECH-DESIGN section 11.1 sets for
/// the whole request.
pub fn decode(cursor: &str) -> Result<(u64, usize, f64, String), CursorError> {
    let raw = URL_SAFE_NO_PAD.decode(cursor.as_bytes()).map_err(|_| CursorError::Base64)?;
    let text = String::from_utf8(raw).map_err(|_| CursorError::Utf8)?;

    let parts: Vec<&str> = text.split(':').collect();
    let [generation_field, index_field, rank_field, cid] = parts.as_slice() else {
        return Err(CursorError::WrongFieldCount);
    };

    let generation: u64 = generation_field.parse().map_err(|_| CursorError::BadGeneration)?;
    let index: usize = index_field.parse().map_err(|_| CursorError::BadIndex)?;

    if rank_field.len() != 16 || !rank_field.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(CursorError::BadRankField);
    }
    let bits = u64::from_str_radix(rank_field, 16).map_err(|_| CursorError::BadRankField)?;
    let rank = f64::from_bits(bits);

    if cid.is_empty() {
        return Err(CursorError::EmptyCid);
    }

    Ok((generation, index, rank, cid.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(generation: u64, index: usize, rank: f64, cid: &str) {
        let encoded = encode(generation, index, rank, cid);
        let decoded = decode(&encoded).expect("round trip should decode");
        assert_eq!(decoded.0, generation);
        assert_eq!(decoded.1, index);
        assert_eq!(decoded.2.to_bits(), rank.to_bits(), "rank bits must match exactly");
        assert_eq!(decoded.3, cid);
    }

    #[test]
    fn round_trip_normal_values() {
        round_trip(7, 42, 1.5, "bafyabc123");
    }

    #[test]
    fn round_trip_zero_rank() {
        round_trip(1, 0, 0.0, "bafy-zero");
    }

    #[test]
    fn round_trip_negative_zero_rank() {
        round_trip(1, 0, -0.0, "bafy-negzero");
    }

    #[test]
    fn zero_and_negative_zero_have_distinct_bit_patterns() {
        // BC31 (carried over): -0.0 == 0.0 under IEEE 754 equality, but the
        // cursor must preserve the distinct bit patterns through the round
        // trip.
        assert_ne!(0.0_f64.to_bits(), (-0.0_f64).to_bits());
        let encoded_pos = encode(1, 0, 0.0, "cid");
        let encoded_neg = encode(1, 0, -0.0, "cid");
        assert_ne!(encoded_pos, encoded_neg);
        let decoded_pos = decode(&encoded_pos).unwrap();
        let decoded_neg = decode(&encoded_neg).unwrap();
        assert_eq!(decoded_pos.2.to_bits(), 0.0_f64.to_bits());
        assert_eq!(decoded_neg.2.to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn round_trip_negative_rank() {
        round_trip(3, 12, -42.75, "bafy-negrank");
    }

    #[test]
    fn round_trip_cid_with_base64_special_characters() {
        // Base64url uses '-' and '_'; a CID containing those characters must
        // still round-trip, since the CID sits inside the decoded payload,
        // not the outer base64 alphabet.
        round_trip(2, 5, 3.15, "bafy-abc_def-123_456");
    }

    #[test]
    fn decode_fails_not_base64() {
        assert_eq!(decode("not valid base64!!! ###"), Err(CursorError::Base64));
    }

    #[test]
    fn decode_fails_not_utf8() {
        // Raw bytes 0xFF 0xFE are invalid UTF-8; base64url-encode them so the
        // outer decode succeeds and the UTF-8 check is what fails.
        let invalid_utf8 = URL_SAFE_NO_PAD.encode([0xFFu8, 0xFE]);
        assert_eq!(decode(&invalid_utf8), Err(CursorError::Utf8));
    }

    #[test]
    fn decode_fails_too_few_fields() {
        // Only three fields: generation, index, rank, no cid separator.
        let raw = format!("1:2:{}", "0".repeat(16));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::WrongFieldCount));
    }

    #[test]
    fn decode_fails_too_many_fields() {
        let raw = format!("1:2:{}:cid:extra", "0".repeat(16));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::WrongFieldCount));
    }

    #[test]
    fn decode_fails_non_numeric_generation() {
        let raw = format!("abc:2:{}:cid", "0".repeat(16));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::BadGeneration));
    }

    #[test]
    fn decode_fails_non_numeric_index() {
        let raw = format!("1:abc:{}:cid", "0".repeat(16));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::BadIndex));
    }

    #[test]
    fn decode_fails_short_hex_field() {
        let raw = format!("1:2:{}:cid", "0".repeat(15));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::BadRankField));
    }

    #[test]
    fn decode_fails_non_hex_field() {
        let raw = format!("1:2:{}g:cid", "0".repeat(15));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::BadRankField));
    }

    #[test]
    fn decode_fails_empty_cid() {
        let raw = format!("1:2:{}:", "0".repeat(16));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::EmptyCid));
    }
}
