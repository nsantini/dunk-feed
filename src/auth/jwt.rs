//! Splits a bearer token into its three base64url parts and runs checks 1
//! to 5 of `docs/02-TECH-DESIGN-network-feed.md` §5: structure and `alg`
//! (BC3, BC4), `exp` (BC5), `aud` (BC6), `lxm` (BC7) and `iss` (BC8). This
//! module never looks at a key or a signature; `keys.rs` and `mod.rs`'s
//! `verify` do checks 6 and 7.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::Deserialize;

use super::AuthError;

/// The seconds of clock skew `exp` is allowed (BC5), matching design §5
/// check 2.
const CLOCK_SKEW_SECS: i64 = 30;

/// `app.bsky.feed.getFeedSkeleton`'s own nsid, the only `lxm` a service
/// token for this feed may carry (BC7).
const METHOD_NSID: &str = "app.bsky.feed.getFeedSkeleton";

/// The signing algorithm named in the JWT header (BC4). `keys::verify`
/// uses this to pick which curve's verifier runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alg {
    Es256k,
    Es256,
}

#[derive(Debug, Deserialize)]
struct Header {
    alg: String,
    #[serde(default)]
    typ: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Claims {
    #[serde(default)]
    exp: Option<i64>,
    #[serde(default)]
    aud: Option<String>,
    #[serde(default)]
    iss: Option<String>,
    #[serde(default)]
    lxm: Option<String>,
}

/// The result of checks 1 to 5: everything `mod.rs`'s `verify` needs to
/// look up a key and check the signature. `signing_input` is the exact
/// `header.payload` bytes that were signed, kept as sent (not
/// re-encoded), because a byte-for-byte mismatch would make a genuine
/// signature fail to verify.
#[derive(Debug, PartialEq)]
pub(super) struct Verified {
    pub(super) alg: Alg,
    pub(super) viewer_did: String,
    pub(super) signing_input: Vec<u8>,
    pub(super) signature: Vec<u8>,
}

/// Removes a `#fragment` from a DID or `aud` value, per BC6 and BC8. A
/// value with no `#` is returned unchanged.
fn strip_fragment(value: &str) -> &str {
    value.split('#').next().unwrap_or(value)
}

/// `did:plc:<suffix>`'s own alphabet and length (review round 1, defects
/// D and E): exactly 24 characters, each RFC4648 base32-lowercase
/// (`a`-`z`, `2`-`7`) — the encoding `did:plc` identifiers are minted
/// with. A suffix of any other length, or carrying a digit or letter
/// outside that alphabet (`0`, `1`, `8`, `9`, or uppercase), is not a
/// `did:plc` this feed could have issued a token for.
fn is_valid_plc_suffix(suffix: &str) -> bool {
    suffix.len() == 24 && suffix.chars().all(|c| matches!(c, 'a'..='z' | '2'..='7'))
}

/// `did:web:<host>`'s own shape (BC8): lower-case ASCII only, dot-separated
/// labels of 1 to 63 characters each from `[a-z0-9-]`, each starting and
/// ending with a letter or digit, the whole host at most 253 characters,
/// at least two labels, and no trailing dot. A WHATWG URL parser (the one
/// `reqwest`, and so the resolver, uses to build the fetch URL) still
/// reads a form like `0x7f.0.0.1` or a trailing dot as an IPv4 address
/// even though it fails a plain "every label is all-digits" check, so
/// this also requires the last label to be letters only, or an
/// internationalized-domain `xn--` label — no numeric or hex TLD ever
/// passes, so no IPv4 form the URL parser recognizes can pass either.
/// Rejecting upper case outright, rather than folding it, also means one
/// host can never produce two different cache or miss-limiter keys.
fn is_valid_web_host(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 || host.ends_with('.') {
        return false;
    }
    let labels: Vec<&str> = host.split('.').collect();
    if labels.len() < 2 {
        return false;
    }
    for label in &labels {
        let bytes = label.as_bytes();
        if bytes.is_empty() || bytes.len() > 63 {
            return false;
        }
        if !label.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-') {
            return false;
        }
        let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
        if !is_alnum(bytes[0]) || !is_alnum(bytes[bytes.len() - 1]) {
            return false;
        }
    }
    let last_label = labels[labels.len() - 1];
    last_label.chars().all(|c| c.is_ascii_lowercase()) || last_label.starts_with("xn--")
}

/// Checks BC8: `did` (the part before any `#`) is `did:plc:<suffix>`
/// ([`is_valid_plc_suffix`]) or `did:web:<host>` ([`is_valid_web_host`]).
fn check_issuer(iss: &str) -> Result<String, AuthError> {
    let did = strip_fragment(iss);
    if let Some(rest) = did.strip_prefix("did:plc:") {
        if !is_valid_plc_suffix(rest) {
            return Err(AuthError::Issuer);
        }
        return Ok(did.to_string());
    }
    if let Some(host) = did.strip_prefix("did:web:") {
        if !is_valid_web_host(host) {
            return Err(AuthError::Issuer);
        }
        return Ok(did.to_string());
    }
    Err(AuthError::Issuer)
}

/// Runs checks 1 to 5 of design §5 in order, so the first failure names
/// the right `AuthError` variant (BC3 to BC8). `now` is the unix second
/// the request arrived, and `service_did` is `UPSTAGE_SERVICE_DID`
/// (`AuthConfig::service_did`) with no fragment stripped yet — this
/// function strips it before comparing, matching BC6.
pub(super) fn check(token: &str, now: i64, service_did: &str) -> Result<Verified, AuthError> {
    let mut parts = token.split('.');
    let (Some(header_b64), Some(payload_b64), Some(sig_b64)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return Err(AuthError::Malformed);
    };
    if parts.next().is_some() {
        return Err(AuthError::Malformed);
    }

    let header_bytes = URL_SAFE_NO_PAD.decode(header_b64).map_err(|_| AuthError::Malformed)?;
    let header: Header = serde_json::from_slice(&header_bytes).map_err(|_| AuthError::Malformed)?;

    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).map_err(|_| AuthError::Malformed)?;
    let claims: Claims =
        serde_json::from_slice(&payload_bytes).map_err(|_| AuthError::Malformed)?;

    let signature = URL_SAFE_NO_PAD.decode(sig_b64).map_err(|_| AuthError::Malformed)?;

    // Check 1: alg and typ.
    let alg = match header.alg.as_str() {
        "ES256K" => Alg::Es256k,
        "ES256" => Alg::Es256,
        _ => return Err(AuthError::Alg),
    };
    if let Some(typ) = &header.typ {
        if typ != "JWT" {
            return Err(AuthError::Alg);
        }
    }

    // Check 2: exp, missing or more than CLOCK_SKEW_SECS in the past.
    // `saturating_add` (review round 1, defect F): an attacker-supplied
    // `exp` near `i64::MAX` must not overflow this addition and wrap
    // into looking expired, or worse, panic in a debug build.
    match claims.exp {
        Some(exp) if exp.saturating_add(CLOCK_SKEW_SECS) >= now => {}
        _ => return Err(AuthError::Expired),
    }

    // Check 3: aud, fragments stripped on both sides.
    let aud = claims.aud.as_deref().ok_or(AuthError::Audience)?;
    if strip_fragment(aud) != strip_fragment(service_did) {
        return Err(AuthError::Audience);
    }

    // Check 4: lxm, only checked when present.
    if let Some(lxm) = &claims.lxm {
        if lxm != METHOD_NSID {
            return Err(AuthError::Method);
        }
    }

    // Check 5: iss.
    let iss = claims.iss.as_deref().ok_or(AuthError::Issuer)?;
    let viewer_did = check_issuer(iss)?;

    let signing_input = format!("{header_b64}.{payload_b64}").into_bytes();

    Ok(Verified { alg, viewer_did, signing_input, signature })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a token string from raw header and payload JSON, with a
    /// dummy 64-byte signature: `check` never looks at the signature
    /// bytes, so tests here only need to control the header and payload.
    fn token(header_json: &str, payload_json: &str) -> String {
        let header = URL_SAFE_NO_PAD.encode(header_json);
        let payload = URL_SAFE_NO_PAD.encode(payload_json);
        let sig = URL_SAFE_NO_PAD.encode([0u8; 64]);
        format!("{header}.{payload}.{sig}")
    }

    const SERVICE_DID: &str = "did:web:feed.example";
    const NOW: i64 = 1_800_000_000;

    fn valid_payload(iss: &str, aud: &str) -> String {
        format!(
            r#"{{"iss":"{iss}","aud":"{aud}","exp":{},"lxm":"app.bsky.feed.getFeedSkeleton"}}"#,
            NOW + 60
        )
    }

    #[test]
    fn fragments() {
        // AC3: a `#` fragment on `aud` and on `iss` is removed before the
        // check, so a viewer's own signing key fragment on `iss`, and the
        // feed's own `#atproto`-style fragment on `aud`, do not fail the
        // comparison.
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload(
            "did:plc:abcdefghijklmnopqrstuvwx#atproto",
            &format!("{SERVICE_DID}#atproto_label_service"),
        );
        let verified = check(&token(header, &payload), NOW, SERVICE_DID).expect("should verify");
        assert_eq!(verified.viewer_did, "did:plc:abcdefghijklmnopqrstuvwx");
        assert_eq!(verified.alg, Alg::Es256k);
    }

    #[test]
    fn malformed_wrong_part_count() {
        assert_eq!(check("a.b", NOW, SERVICE_DID), Err(AuthError::Malformed));
        assert_eq!(check("a.b.c.d", NOW, SERVICE_DID), Err(AuthError::Malformed));
    }

    #[test]
    fn malformed_bad_json() {
        let header = URL_SAFE_NO_PAD.encode("not json");
        let payload = URL_SAFE_NO_PAD.encode(valid_payload("did:plc:abc", SERVICE_DID));
        let sig = URL_SAFE_NO_PAD.encode([0u8; 64]);
        assert_eq!(
            check(&format!("{header}.{payload}.{sig}"), NOW, SERVICE_DID),
            Err(AuthError::Malformed)
        );
    }

    #[test]
    fn rejects_unknown_alg() {
        let header = r#"{"alg":"HS256"}"#;
        let payload = valid_payload("did:plc:abc", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Alg));
    }

    #[test]
    fn rejects_wrong_typ() {
        let header = r#"{"alg":"ES256K","typ":"JWS"}"#;
        let payload = valid_payload("did:plc:abc", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Alg));
    }

    #[test]
    fn rejects_expired() {
        let header = r#"{"alg":"ES256K"}"#;
        let payload =
            format!(r#"{{"iss":"did:plc:abc","aud":"{SERVICE_DID}","exp":{}}}"#, NOW - 31);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Expired));
    }

    #[test]
    fn rejects_missing_exp() {
        let header = r#"{"alg":"ES256K"}"#;
        let payload = format!(r#"{{"iss":"did:plc:abc","aud":"{SERVICE_DID}"}}"#);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Expired));
    }

    #[test]
    fn accepts_exp_within_skew() {
        let header = r#"{"alg":"ES256K"}"#;
        let payload = format!(
            r#"{{"iss":"did:plc:abcdefghijklmnopqrstuvwx","aud":"{SERVICE_DID}","exp":{}}}"#,
            NOW - 30
        );
        assert!(check(&token(header, &payload), NOW, SERVICE_DID).is_ok());
    }

    #[test]
    fn rejects_wrong_audience() {
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:plc:abc", "did:web:wrong.example");
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Audience));
    }

    #[test]
    fn rejects_wrong_lxm() {
        let header = r#"{"alg":"ES256K"}"#;
        let payload = format!(
            r#"{{"iss":"did:plc:abc","aud":"{SERVICE_DID}","exp":{},"lxm":"app.bsky.feed.getTimeline"}}"#,
            NOW + 60
        );
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Method));
    }

    #[test]
    fn rejects_bad_issuer() {
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:example:abc", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
    }

    #[test]
    fn rejects_did_web_with_port() {
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:web:example.com%3A8080", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
    }

    #[test]
    fn rejects_did_web_with_path() {
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:web:example.com:users:alice", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
    }

    #[test]
    fn rejects_did_web_with_slash_query_or_userinfo() {
        // Review round 1, defects D and E: `is_valid_web_host` rejects any
        // character outside its allow-list in one pass.
        let header = r#"{"alg":"ES256K"}"#;
        for bad in
            ["did:web:example.com/path", "did:web:example.com?x=1", "did:web:user@example.com"]
        {
            let payload = valid_payload(bad, SERVICE_DID);
            assert_eq!(
                check(&token(header, &payload), NOW, SERVICE_DID),
                Err(AuthError::Issuer),
                "expected Issuer for {bad}"
            );
        }
    }

    #[test]
    fn rejects_did_web_with_lowercase_percent_port() {
        // Review round 1, defects D and E: the old check only matched the
        // uppercase `%3A` form.
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:web:example.com%3a8080", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
    }

    #[test]
    fn rejects_did_web_ip_literal() {
        // Review round 1, defects D and E: a `did:web` identifier names a
        // domain, not an address.
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:web:192.168.0.1", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
    }

    #[test]
    fn rejects_did_web_hex_or_short_ipv4_forms() {
        // BC8: a WHATWG URL parser still reads these as IPv4 addresses,
        // even though no label is all-digits.
        let header = r#"{"alg":"ES256K"}"#;
        for bad in ["did:web:0x7f.0.0.1", "did:web:0x7f.1"] {
            let payload = valid_payload(bad, SERVICE_DID);
            assert_eq!(
                check(&token(header, &payload), NOW, SERVICE_DID),
                Err(AuthError::Issuer),
                "expected Issuer for {bad}"
            );
        }
    }

    #[test]
    fn rejects_did_web_trailing_dot() {
        // BC8: a trailing dot gives an empty last label, which a URL
        // parser still resolves as the bare host.
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:web:127.0.0.1.", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
        let payload = valid_payload("did:web:example.com.", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
    }

    #[test]
    fn rejects_did_web_upper_case() {
        // BC8: upper case is rejected outright (not folded), so one host
        // can never yield two cache keys.
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:web:Example.COM", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
    }

    #[test]
    fn rejects_did_web_empty_or_edge_hyphen_labels() {
        // BC8: empty labels and a label starting with a hyphen are not
        // valid domain labels.
        let header = r#"{"alg":"ES256K"}"#;
        for bad in ["did:web:..", "did:web:-a.com", "did:web:a..b"] {
            let payload = valid_payload(bad, SERVICE_DID);
            assert_eq!(
                check(&token(header, &payload), NOW, SERVICE_DID),
                Err(AuthError::Issuer),
                "expected Issuer for {bad}"
            );
        }
    }

    #[test]
    fn rejects_plc_suffix_wrong_length() {
        // Review round 1, defects D and E: `did:plc` suffixes are exactly
        // 24 characters.
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:plc:tooshort", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
    }

    #[test]
    fn rejects_plc_suffix_invalid_chars() {
        // Review round 1, defects D and E: `0`, `1`, `8` and `9` fall
        // outside base32-lowercase, even at the right length.
        let header = r#"{"alg":"ES256K"}"#;
        let payload = valid_payload("did:plc:000000000000000000000000", SERVICE_DID);
        assert_eq!(check(&token(header, &payload), NOW, SERVICE_DID), Err(AuthError::Issuer));
    }

    #[test]
    fn exp_extremes_do_not_overflow() {
        // Review round 1, defect F: `exp.saturating_add(CLOCK_SKEW_SECS)`
        // must neither panic nor wrap at either extreme.
        let header = r#"{"alg":"ES256K"}"#;
        let iss = "did:plc:abcdefghijklmnopqrstuvwx";
        let payload_max = format!(r#"{{"iss":"{iss}","aud":"{SERVICE_DID}","exp":{}}}"#, i64::MAX);
        assert!(check(&token(header, &payload_max), NOW, SERVICE_DID).is_ok());

        let payload_min = format!(r#"{{"iss":"{iss}","aud":"{SERVICE_DID}","exp":{}}}"#, i64::MIN);
        assert_eq!(check(&token(header, &payload_min), NOW, SERVICE_DID), Err(AuthError::Expired));
    }
}
