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
    let rank = parse_rank_field(rank_field)?;

    if cid.is_empty() {
        return Err(CursorError::EmptyCid);
    }

    Ok((generation, index, rank, cid.to_string()))
}

/// Parses the 16-hex-digit rank field both `decode` and `decode_personal`
/// share, so the exact-bits round trip (BC31) is implemented once.
fn parse_rank_field(rank_field: &str) -> Result<f64, CursorError> {
    if rank_field.len() != 16 || !rank_field.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(CursorError::BadRankField);
    }
    let bits = u64::from_str_radix(rank_field, 16).map_err(|_| CursorError::BadRankField)?;
    Ok(f64::from_bits(bits))
}

/// `decode_personal`'s return shape: `(generation, circle_version, index,
/// rank, cid)`. `circle_version` is `None` for a plain four-field cursor
/// (BC17a) and `Some` for the five-field `encode_personal` shape (BC14). A
/// named alias, not the bare tuple, so clippy's `type_complexity` lint
/// stays quiet and every reader — this module and `src/http/skeleton.rs`
/// alike — sees one name for the same five fields.
pub type PersonalCursor = (u64, Option<u64>, usize, f64, String);

/// Encodes the personalised cursor (spec.md `## Approach`, slice 4.0):
/// `generation ":" circle_version ":" index ":" rank_bits_hex ":"
/// quote_cid`. The fifth field, `circle_version`, pins the viewer's per-
/// generation, per-circle-version list (`http::viewer::ViewerLists`,
/// BC13), so `src/http/skeleton.rs`'s personalised path can tell "the same
/// list that served the last page" (BC14, an O(1) resume) from "the circle
/// changed since" (BC16, BC17) exactly the way `encode`/`decode`'s
/// `generation` field already tells one scorer pass from another on the
/// global path.
pub fn encode_personal(
    generation: u64,
    circle_version: u64,
    index: usize,
    rank: f64,
    cid: &str,
) -> String {
    let raw = format!("{generation}:{circle_version}:{index}:{:016x}:{cid}", rank.to_bits());
    URL_SAFE_NO_PAD.encode(raw.as_bytes())
}

/// Decodes a cursor on the personalised path (BC17a, BC17b): accepts either
/// the plain four-field `encode` shape (`circle_version` comes back `None`,
/// so BC17's cid-then-rank scan applies) or the five-field `encode_personal`
/// shape (`circle_version` comes back `Some`, enabling BC14's O(1) resume).
/// Anything but exactly four or five `:`-separated fields is
/// `WrongFieldCount`, the same error `decode`'s own field-count check
/// raises, since both are "this cursor is not shaped like anything this
/// server ever issued".
pub fn decode_personal(cursor: &str) -> Result<PersonalCursor, CursorError> {
    let raw = URL_SAFE_NO_PAD.decode(cursor.as_bytes()).map_err(|_| CursorError::Base64)?;
    let text = String::from_utf8(raw).map_err(|_| CursorError::Utf8)?;

    let parts: Vec<&str> = text.split(':').collect();
    match parts.as_slice() {
        [generation_field, index_field, rank_field, cid] => {
            let generation: u64 =
                generation_field.parse().map_err(|_| CursorError::BadGeneration)?;
            let index: usize = index_field.parse().map_err(|_| CursorError::BadIndex)?;
            let rank = parse_rank_field(rank_field)?;
            if cid.is_empty() {
                return Err(CursorError::EmptyCid);
            }
            Ok((generation, None, index, rank, cid.to_string()))
        }
        [generation_field, circle_version_field, index_field, rank_field, cid] => {
            let generation: u64 =
                generation_field.parse().map_err(|_| CursorError::BadGeneration)?;
            let circle_version: u64 =
                circle_version_field.parse().map_err(|_| CursorError::BadGeneration)?;
            let index: usize = index_field.parse().map_err(|_| CursorError::BadIndex)?;
            let rank = parse_rank_field(rank_field)?;
            if cid.is_empty() {
                return Err(CursorError::EmptyCid);
            }
            Ok((generation, Some(circle_version), index, rank, cid.to_string()))
        }
        _ => Err(CursorError::WrongFieldCount),
    }
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

    // 4.1: the five-field personalised codec round-trips, carrying
    // `circle_version` as `Some`.
    #[test]
    fn personal_round_trip() {
        let encoded = encode_personal(7, 3, 42, 1.5, "bafyabc123");
        let decoded = decode_personal(&encoded).expect("round trip should decode");
        assert_eq!(decoded, (7, Some(3), 42, 1.5, "bafyabc123".to_string()));
    }

    // BC17a: a four-field cursor (the global `encode`'s own shape) is still
    // accepted on the personalised path, with `circle_version` coming back
    // `None` so BC17's cid-then-rank scan applies.
    #[test]
    fn personal_decode_accepts_four_field_cursor() {
        let four_field = encode(7, 42, 1.5, "bafyabc123");
        let decoded = decode_personal(&four_field).expect("a plain four-field cursor must decode");
        assert_eq!(decoded, (7, None, 42, 1.5, "bafyabc123".to_string()));
    }

    // BC17b: a malformed cursor on the personalised path fails the same way
    // `decode` does on the global path.
    #[test]
    fn personal_decode_fails_on_wrong_field_count() {
        let raw = format!("1:2:{}:cid:extra:extra2", "0".repeat(16));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode_personal(&encoded), Err(CursorError::WrongFieldCount));

        let too_few = URL_SAFE_NO_PAD.encode(b"1:2:3");
        assert_eq!(decode_personal(&too_few), Err(CursorError::WrongFieldCount));
    }

    #[test]
    fn personal_decode_fails_not_base64() {
        assert_eq!(decode_personal("not valid base64!!! ###"), Err(CursorError::Base64));
    }

    #[test]
    fn personal_decode_fails_bad_circle_version() {
        let raw = format!("1:abc:2:{}:cid", "0".repeat(16));
        let encoded = URL_SAFE_NO_PAD.encode(raw.as_bytes());
        assert_eq!(decode_personal(&encoded), Err(CursorError::BadGeneration));
    }
}
