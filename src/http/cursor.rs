//! The `getFeedSkeleton` pagination cursor, pure and unit-testable without a
//! server. A cursor encodes the `(rank, quote_cid)` of the last item a page
//! returned so the next request can resume from it. The snapshot is not
//! totally ordered by `(rank DESC, cid ASC)` once
//! `snapshot::apply_cap_one_per_quoter_per_50` has deferred items past
//! lower-ranked ones (TECH-DESIGN section 11.1), so `src/http/skeleton.rs`'s
//! page scan finds the resume point with one linear scan, not a binary
//! search (BC16, BC30, BC31). Round 2 finding 6 (BC46): the comparator that
//! scan uses is `scorer::snapshot::cmp_rank_then_cid`, the same one
//! `sort_by_rank` uses; this module no longer keeps its own copy.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use thiserror::Error;

/// A cursor failed to decode. Mapped to `SkeletonError::InvalidRequest` by
/// slice 3.0's `src/http/skeleton.rs` (BC6, BC30).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CursorError {
    #[error("cursor is not valid base64url")]
    Base64,
    #[error("cursor is not valid UTF-8")]
    Utf8,
    #[error("cursor is missing the ':' separator after the 16 hex digits")]
    MissingSeparator,
    #[error("cursor's rank field is not exactly 16 hex digits")]
    BadRankField,
    #[error("cursor's CID field is empty")]
    EmptyCid,
}

/// Encodes `rank` and `cid` as a base64url (no padding) cursor: the rank's
/// raw bits as 16 lowercase hex digits, a `:`, then the CID verbatim. Hex on
/// the bit pattern, not the float itself, keeps `0.0` and `-0.0` distinct
/// (BC31) and the round trip exact (BC16). `src/http/skeleton.rs`'s page
/// scan is the caller.
pub fn encode(rank: f64, cid: &str) -> String {
    let raw = format!("{:016x}:{cid}", rank.to_bits());
    URL_SAFE_NO_PAD.encode(raw.as_bytes())
}

/// Decodes a cursor produced by `encode`, or fails with the specific
/// `CursorError` `src/http/skeleton.rs`'s `getFeedSkeleton` handler maps to
/// `InvalidRequest` (BC6, BC30).
///
/// Round 2 finding 10: returns an owned `String` for the CID rather than a
/// `&str` borrowed from `cursor`. A borrow would tie the result to whatever
/// buffer the caller decoded `cursor` into — here, the intermediate `text`
/// this function itself builds from the base64 bytes — so the caller would
/// have to keep that buffer alive alongside the returned tuple instead of
/// getting one self-contained value back. The cost is one short string
/// clone per request, well inside the 5 ms budget TECH-DESIGN section 11.1
/// sets for the whole request.
pub fn decode(cursor: &str) -> Result<(f64, String), CursorError> {
    let raw = URL_SAFE_NO_PAD.decode(cursor.as_bytes()).map_err(|_| CursorError::Base64)?;
    let text = String::from_utf8(raw).map_err(|_| CursorError::Utf8)?;

    // `find` returns a byte offset that always lands on a UTF-8 char
    // boundary (the ':' it matched starts there), so both slices below are
    // panic-free regardless of what bytes precede or follow it.
    let sep_idx = text.find(':').ok_or(CursorError::MissingSeparator)?;
    let rank_field = &text[..sep_idx];
    let cid = &text[sep_idx + 1..];

    if rank_field.len() != 16 || !rank_field.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(CursorError::BadRankField);
    }
    let bits = u64::from_str_radix(rank_field, 16).map_err(|_| CursorError::BadRankField)?;
    let rank = f64::from_bits(bits);

    if cid.is_empty() {
        return Err(CursorError::EmptyCid);
    }

    Ok((rank, cid.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(rank: f64, cid: &str) {
        let encoded = encode(rank, cid);
        let decoded = decode(&encoded).expect("round trip should decode");
        assert_eq!(decoded.0.to_bits(), rank.to_bits(), "rank bits must match exactly");
        assert_eq!(decoded.1, cid);
    }

    #[test]
    fn round_trip_normal_rank() {
        round_trip(1.5, "bafyabc123");
    }

    #[test]
    fn round_trip_zero() {
        round_trip(0.0, "bafy-zero");
    }

    #[test]
    fn round_trip_negative_zero() {
        round_trip(-0.0, "bafy-negzero");
    }

    #[test]
    fn zero_and_negative_zero_have_distinct_bit_patterns() {
        // BC31: -0.0 == 0.0 under IEEE 754 equality, but the cursor must
        // preserve the distinct bit patterns through the round trip.
        assert_ne!(0.0_f64.to_bits(), (-0.0_f64).to_bits());
        let encoded_pos = encode(0.0, "cid");
        let encoded_neg = encode(-0.0, "cid");
        assert_ne!(encoded_pos, encoded_neg);
        let (decoded_pos, _) = decode(&encoded_pos).unwrap();
        let (decoded_neg, _) = decode(&encoded_neg).unwrap();
        assert_eq!(decoded_pos.to_bits(), 0.0_f64.to_bits());
        assert_eq!(decoded_neg.to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn round_trip_negative_rank() {
        round_trip(-42.75, "bafy-negrank");
    }

    #[test]
    fn round_trip_cid_with_base64_special_characters() {
        // Base64url uses '-' and '_'; a CID containing those characters must
        // still round-trip, since the CID sits inside the decoded payload,
        // not the outer base64 alphabet.
        round_trip(3.15, "bafy-abc_def-123_456");
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
    fn decode_fails_no_separator() {
        let raw = "0".repeat(16);
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::MissingSeparator));
    }

    #[test]
    fn decode_fails_short_hex_field() {
        let raw = format!("{}:cid", "0".repeat(15));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::BadRankField));
    }

    #[test]
    fn decode_fails_non_hex_field() {
        let raw = format!("{}g:cid", "0".repeat(15));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::BadRankField));
    }

    #[test]
    fn decode_fails_empty_cid() {
        let raw = format!("{}:", "0".repeat(16));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode(&encoded), Err(CursorError::EmptyCid));
    }
}
