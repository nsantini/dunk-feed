//! Configuration loaded from environment variables. Every variable is
//! documented in `docs/TECH-DESIGN.md` section 4 and mirrored in
//! `.env.example`. `load` fails fast on a missing required variable or a
//! malformed value; it never panics.

use std::str::FromStr;

use thiserror::Error;

/// A config load failure. `main.rs` prints this and exits 1. It never panics.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("missing required environment variable {0}")]
    Missing(&'static str),
    #[error("invalid value for {name}: {value:?}: {reason}")]
    Invalid { name: &'static str, value: String, reason: String },
}

/// Every environment variable from TECH-DESIGN section 4, parsed and typed.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub db_path: String,
    pub http_addr: String,
    pub hostname: String,
    pub publisher_did: String,
    pub feed_rkey: String,
    pub jetstream_url: String,
    pub appview_url: String,
    pub w_repost: f64,
    pub w_reply: f64,
    pub k: u32,
    pub p: u32,
    pub m: f64,
    pub candidate_ttl_h: u32,
    pub feed_ttl_d: u32,
    pub scorer_interval_s: u32,
    pub reverify_interval_s: u32,
    pub follower_floor: u32,
    pub drop_labels: Vec<String>,
    pub prefilter_fraction: f64,
    pub appview_rps: f64,
    pub log: String,
    pub bsky_handle: Option<String>,
    pub bsky_app_password: Option<String>,
}

/// Reads a required string variable. `Missing` if unset, and also `Missing`
/// if set to an empty or whitespace-only value (BC17): a half-filled copy of
/// `.env.example`, which ships both required variables as bare `NAME=`, must
/// fail the same way as an unset one, not pass with an empty value.
fn required(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
) -> Result<String, ConfigError> {
    match lookup(name) {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(ConfigError::Missing(name)),
    }
}

/// Reads an optional string variable, falling back to `default` when unset.
/// An unrecognised variable elsewhere in the environment (BC8) is simply
/// never looked up, so it cannot affect this or any other field.
fn string_or_default(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: &str,
) -> String {
    lookup(name).unwrap_or_else(|| default.to_string())
}

/// Reads an optional string variable with no default. `None` when unset.
fn optional(lookup: &impl Fn(&str) -> Option<String>, name: &'static str) -> Option<String> {
    lookup(name)
}

/// Parses a numeric variable with `FromStr`, falling back to `default` when
/// unset. An empty string is treated as malformed, not unset (BC4), so it
/// returns `Invalid` rather than the default.
fn number_or_default<T>(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: T,
) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match lookup(name) {
        None => Ok(default),
        Some(value) if value.is_empty() => {
            Err(ConfigError::Invalid { name, value, reason: "empty value".to_string() })
        }
        Some(value) => value.parse::<T>().map_err(|err| ConfigError::Invalid {
            name,
            value: value.clone(),
            reason: err.to_string(),
        }),
    }
}

/// Validates `DUNK_LOG` with `EnvFilter::try_new`, falling back to `default`
/// when unset (BC9). A malformed filter directive is invalid (BC14): it must
/// never degrade silently to error-only logging, so it is rejected here
/// rather than left for `tracing_subscriber` to swallow later.
fn log_filter_or_default(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: &str,
) -> Result<String, ConfigError> {
    let raw = string_or_default(lookup, name, default);
    tracing_subscriber::EnvFilter::try_new(&raw).map_err(|err| ConfigError::Invalid {
        name,
        value: raw.clone(),
        reason: err.to_string(),
    })?;
    Ok(raw)
}

/// Parses an `f64` variable via [`number_or_default`], then rejects a
/// non-finite value (BC15) or a negative one (BC16). Every `f64` field in
/// `Config` uses this instead of `number_or_default` directly, because every
/// one of them must be finite and non-negative.
fn nonneg_float_or_default(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: f64,
) -> Result<f64, ConfigError> {
    let value = number_or_default(lookup, name, default)?;
    if !value.is_finite() {
        return Err(ConfigError::Invalid {
            name,
            value: value.to_string(),
            reason: "not a finite number".to_string(),
        });
    }
    if value < 0.0 {
        return Err(ConfigError::Invalid {
            name,
            value: value.to_string(),
            reason: "must be >= 0".to_string(),
        });
    }
    Ok(value)
}

/// Splits `DUNK_DROP_LABELS` on `,`, trims each entry, and drops empty
/// entries (BC10). Falls back to `default` when unset (BC9). An empty or
/// whitespace-only value is malformed, the same rule as an empty number
/// (BC13): disabling every label guard is not a supported setting here.
fn drop_labels(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: &str,
) -> Result<Vec<String>, ConfigError> {
    let raw = string_or_default(lookup, name, default);
    if raw.trim().is_empty() {
        return Err(ConfigError::Invalid {
            name,
            value: raw,
            reason: "empty or whitespace-only value".to_string(),
        });
    }
    Ok(raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect())
}

/// Loads the config from `lookup`, a variable-name-to-value function. Tests
/// inject a closure over a fixed map; `main.rs` passes `std::env::var` turned
/// into an `Option`, so no test touches the process environment.
pub fn load(lookup: impl Fn(&str) -> Option<String>) -> Result<Config, ConfigError> {
    Ok(Config {
        db_path: string_or_default(&lookup, "DUNK_DB_PATH", "/data/dunk.db"),
        http_addr: string_or_default(&lookup, "DUNK_HTTP_ADDR", "0.0.0.0:3000"),
        hostname: required(&lookup, "DUNK_HOSTNAME")?,
        publisher_did: required(&lookup, "DUNK_PUBLISHER_DID")?,
        feed_rkey: string_or_default(&lookup, "DUNK_FEED_RKEY", "dunks"),
        jetstream_url: string_or_default(
            &lookup,
            "DUNK_JETSTREAM_URL",
            "wss://jetstream.us-east.bsky.network",
        ),
        appview_url: string_or_default(&lookup, "DUNK_APPVIEW_URL", "https://public.api.bsky.app"),
        w_repost: nonneg_float_or_default(&lookup, "DUNK_W_REPOST", 2.0)?,
        w_reply: nonneg_float_or_default(&lookup, "DUNK_W_REPLY", 0.5)?,
        k: number_or_default(&lookup, "DUNK_K", 5)?,
        p: number_or_default(&lookup, "DUNK_P", 50)?,
        m: nonneg_float_or_default(&lookup, "DUNK_M", 1.25)?,
        candidate_ttl_h: number_or_default(&lookup, "DUNK_CANDIDATE_TTL_H", 48)?,
        feed_ttl_d: number_or_default(&lookup, "DUNK_FEED_TTL_D", 30)?,
        scorer_interval_s: number_or_default(&lookup, "DUNK_SCORER_INTERVAL_S", 60)?,
        reverify_interval_s: number_or_default(&lookup, "DUNK_REVERIFY_INTERVAL_S", 600)?,
        follower_floor: number_or_default(&lookup, "DUNK_FOLLOWER_FLOOR", 2000)?,
        drop_labels: drop_labels(
            &lookup,
            "DUNK_DROP_LABELS",
            "porn,sexual,graphic-media,nudity,!hide,!warn,spam",
        )?,
        prefilter_fraction: nonneg_float_or_default(&lookup, "DUNK_PREFILTER_FRACTION", 0.5)?,
        appview_rps: nonneg_float_or_default(&lookup, "DUNK_APPVIEW_RPS", 1.0)?,
        log: log_filter_or_default(&lookup, "DUNK_LOG", "info")?,
        bsky_handle: optional(&lookup, "BSKY_HANDLE"),
        bsky_app_password: optional(&lookup, "BSKY_APP_PASSWORD"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Builds a lookup closure over a fixed map, standing in for the process
    /// environment so tests never touch it.
    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |name: &str| map.get(name).cloned()
    }

    /// The two variables every deployment must set.
    fn required_pair() -> [(&'static str, &'static str); 2] {
        [("DUNK_HOSTNAME", "feed.example.com"), ("DUNK_PUBLISHER_DID", "did:plc:abc")]
    }

    #[test]
    fn missing_required_var_fails() {
        let lookup = env(&[("DUNK_PUBLISHER_DID", "did:plc:abc")]);
        let err = load(lookup).unwrap_err();
        assert_eq!(err, ConfigError::Missing("DUNK_HOSTNAME"));
    }

    #[test]
    fn missing_publisher_did_fails() {
        let lookup = env(&[("DUNK_HOSTNAME", "feed.example.com")]);
        let err = load(lookup).unwrap_err();
        assert_eq!(err, ConfigError::Missing("DUNK_PUBLISHER_DID"));
    }

    #[test]
    fn empty_required_var_is_missing() {
        let lookup = env(&[("DUNK_HOSTNAME", ""), ("DUNK_PUBLISHER_DID", "did:plc:abc")]);
        let err = load(lookup).unwrap_err();
        assert_eq!(err, ConfigError::Missing("DUNK_HOSTNAME"));
    }

    #[test]
    fn whitespace_required_var_is_missing() {
        let lookup = env(&[("DUNK_HOSTNAME", "feed.example.com"), ("DUNK_PUBLISHER_DID", "   ")]);
        let err = load(lookup).unwrap_err();
        assert_eq!(err, ConfigError::Missing("DUNK_PUBLISHER_DID"));
    }

    #[test]
    fn malformed_number_fails() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_K", "five"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, value, .. } => {
                assert_eq!(name, "DUNK_K");
                assert_eq!(value, "five");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn empty_number_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_W_REPOST", ""));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "DUNK_W_REPOST"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn feed_rkey_falls_back_to_default() {
        let config = load(env(&required_pair())).unwrap();
        assert_eq!(config.feed_rkey, "dunks");
    }

    #[test]
    fn every_optional_variable_falls_back_to_its_default() {
        let config = load(env(&required_pair())).unwrap();
        assert_eq!(config.db_path, "/data/dunk.db");
        assert_eq!(config.http_addr, "0.0.0.0:3000");
        assert_eq!(config.jetstream_url, "wss://jetstream.us-east.bsky.network");
        assert_eq!(config.appview_url, "https://public.api.bsky.app");
        assert_eq!(config.w_repost, 2.0);
        assert_eq!(config.w_reply, 0.5);
        assert_eq!(config.k, 5);
        assert_eq!(config.p, 50);
        assert_eq!(config.m, 1.25);
        assert_eq!(config.candidate_ttl_h, 48);
        assert_eq!(config.feed_ttl_d, 30);
        assert_eq!(config.scorer_interval_s, 60);
        assert_eq!(config.reverify_interval_s, 600);
        assert_eq!(config.follower_floor, 2000);
        assert_eq!(config.prefilter_fraction, 0.5);
        assert_eq!(config.appview_rps, 1.0);
        assert_eq!(config.log, "info");
        assert_eq!(
            config.drop_labels,
            vec!["porn", "sexual", "graphic-media", "nudity", "!hide", "!warn", "spam"]
        );
    }

    #[test]
    fn unrecognised_variable_is_ignored() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_TYPO", "surprise"));
        assert!(load(env(&pairs)).is_ok());
    }

    #[test]
    fn drop_labels_splits_trims_and_drops_empty_entries() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_DROP_LABELS", " porn, , spam ,nudity"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.drop_labels, vec!["porn", "spam", "nudity"]);
    }

    #[test]
    fn empty_drop_labels_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_DROP_LABELS", "   "));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "DUNK_DROP_LABELS"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn bsky_credentials_are_optional_and_absent_by_default() {
        let config = load(env(&required_pair())).unwrap();
        assert_eq!(config.bsky_handle, None);
        assert_eq!(config.bsky_app_password, None);
    }

    #[test]
    fn bsky_credentials_are_read_when_set() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("BSKY_HANDLE", "dunk.bsky.social"));
        pairs.push(("BSKY_APP_PASSWORD", "secret"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.bsky_handle, Some("dunk.bsky.social".to_string()));
        assert_eq!(config.bsky_app_password, Some("secret".to_string()));
    }

    #[test]
    fn malformed_log_filter_fails() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_LOG", "target=notalevel"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "DUNK_LOG"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn valid_log_filter_is_accepted() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_LOG", "debug,dunk=trace"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.log, "debug,dunk=trace");
    }

    #[test]
    fn nan_weight_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_W_REPOST", "nan"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "DUNK_W_REPOST");
                assert_eq!(reason, "not a finite number");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn infinite_weight_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_APPVIEW_RPS", "inf"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "DUNK_APPVIEW_RPS");
                assert_eq!(reason, "not a finite number");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn negative_weight_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("DUNK_M", "-1.0"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "DUNK_M");
                assert_eq!(reason, "must be >= 0");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }
}
