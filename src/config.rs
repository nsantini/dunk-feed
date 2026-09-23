//! Configuration loaded from environment variables. Every variable is
//! documented in `docs/01-TECH-DESIGN.md` section 4 and mirrored in
//! `.env.example`. `load` fails fast on a missing required variable or a
//! malformed value; it never panics.

use std::fmt;
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

/// A secret read from the environment. `Debug` prints `[redacted]`, so a
/// `tracing::info!(?config)` line can never leak it to the log stream.
/// Call `expose` at the one place that sends it over the wire.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a value read from somewhere other than `load`. `upstage publish`
    /// carries the session's `accessJwt` in one of these, so a token never
    /// sits in a plain `String` that a `Debug` line could print.
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// Returns the plain value. Use only when building the request that needs it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

/// Every environment variable from TECH-DESIGN section 4, parsed and typed.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub db_path: String,
    pub http_addr: String,
    pub hostname: String,
    pub publisher_did: String,
    pub feed_rkey: String,
    pub jetstream_urls: Vec<String>,
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
    pub bsky_app_password: Option<Secret>,
    /// PDS every session, refresh and graph call goes through
    /// (`UPSTAGE_PDS_URL`, network-feed story 02). Defaults to the host
    /// `publish` used as its own fixed `BSKY_PDS_URL` before this story.
    pub pds_url: String,
    /// Calls each second `appview::pds::PdsClient`'s limiter lets through
    /// (`UPSTAGE_GRAPH_RPS`, network-feed story 02, TECH-DESIGN-network-feed
    /// §4). The PDS allows 10 each second for each IP; the default of 8
    /// leaves headroom.
    pub graph_rps: f64,
    /// `/healthz`'s lag threshold, in seconds (story 08, BC27): past this
    /// age on either `HealthState` atomic, `/healthz` returns 503.
    pub health_max_lag_s: u32,
    /// Hours the follower-floor histogram period stays open before `one_pass`
    /// stops logging its distribution line (story 10, section 9, revised by
    /// the correction round: the floor itself is live from the first pass
    /// regardless of this window, BC41). `0` disables the period outright
    /// (BC23, BC32).
    pub guard_histogram_h: u32,
    /// Hours an `authors` cache row stays fresh before the guard refetches
    /// it, for an active row or one carrying a `!takedown` label (story 10,
    /// section 9). `0` is rejected: it would refetch every DID on every pass
    /// (BC33).
    pub author_ttl_h: u32,
    /// Hours a row written `active = false` from a missing profile (no
    /// `!takedown` label) stays fresh before the guard refetches it (story
    /// 10's correction round, BC45 to BC47): shorter than `author_ttl_h`,
    /// because a deactivation is often temporary. `0` is rejected, the same
    /// rule as `author_ttl_h`.
    pub author_inactive_ttl_h: u32,
}

impl Config {
    /// `at://<publisher_did>/app.bsky.feed.generator/<feed_rkey>` (BC45):
    /// the single producer of this feed's own at-URI. `HttpConfig::from`
    /// (`src/http/mod.rs`) calls this once at startup; `upstage publish`
    /// (story 09) will too, so the format lives here and nowhere else.
    pub fn feed_uri(&self) -> String {
        format!("at://{}/app.bsky.feed.generator/{}", self.publisher_did, self.feed_rkey)
    }

    /// `did:web:<hostname>` (BC45): the single producer of this service's
    /// `did:web` identity.
    pub fn did_web(&self) -> String {
        format!("did:web:{}", self.hostname)
    }
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

/// Validates `UPSTAGE_LOG` with `EnvFilter::try_new`, falling back to `default`
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

/// Parses a `u32` variable via [`number_or_default`], then rejects `0`
/// (BC23). `UPSTAGE_K`, `UPSTAGE_SCORER_INTERVAL_S` and `UPSTAGE_REVERIFY_INTERVAL_S`
/// each divide or gate a timer period, and `0` is never a working setting.
fn positive_u32_or_default(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: u32,
) -> Result<u32, ConfigError> {
    let value = number_or_default(lookup, name, default)?;
    if value == 0 {
        return Err(ConfigError::Invalid {
            name,
            value: value.to_string(),
            reason: "must be greater than zero".to_string(),
        });
    }
    Ok(value)
}

/// Parses `UPSTAGE_PDS_URL`, falling back to `default` when unset (BC15).
/// Trims the value, then parses it with `reqwest::Url`: it must be `https`
/// (review round 1, finding 4 — a PDS session, and every App View call
/// proxied through it, carries the bearer token and the app password, so a
/// scheme downgrade to plain `http://` would send both in the clear), carry
/// a host (review round 2, defect B — a bare `https://` passed the old
/// prefix check and then broke the path once the trailing slash was
/// trimmed), and carry no userinfo, query or fragment (review round 2,
/// defects E and F — any of the three would swallow or leak past the
/// `/xrpc/<nsid>` path `HttpPdsTransport` builds, and userinfo can hold the
/// very credentials this check exists to protect). `reqwest::Url` lowercases
/// the scheme and the stored value drops a trailing `/`, so `HTTPS://` and a
/// trailing-slash input both normalise to the same base (review round 2,
/// defect C). The value is an https origin only: a path other than a bare
/// `/` is rejected too (review round 3, finding 1, and the engineer's
/// Step 7.5 answer — `HttpPdsTransport` appends `/xrpc/<nsid>` itself, so a
/// path here, like `/xrpc`, would double up), and that rejection carries its
/// own reason naming the path rule rather than the generic scheme/host/
/// userinfo/query/fragment reason above (review round 4, findings 1 to 3, and
/// the engineer's option (a) answer — the generic reason did not tell a
/// caller which rule a path violated). `ConfigError::Invalid.value` is
/// `[redacted]` for this variable, never the raw input, so a rejected
/// userinfo case never echoes its password into the error's `Display` or
/// `Debug` (review round 2, finding 13).
fn pds_url_or_default(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: &str,
) -> Result<String, ConfigError> {
    let raw = match lookup(name) {
        None => return Ok(default.to_string()),
        Some(value) => value,
    };
    let invalid_with = |reason: &str| ConfigError::Invalid {
        name,
        value: "[redacted]".to_string(),
        reason: reason.to_string(),
    };
    let invalid =
        || invalid_with("must be an https URL with a host and no userinfo, query or fragment");
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(invalid());
    }
    let url = reqwest::Url::parse(trimmed).map_err(|_| invalid())?;
    if url.scheme() != "https" {
        return Err(invalid());
    }
    if url.host_str().is_none() {
        return Err(invalid());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(invalid());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(invalid());
    }
    if !matches!(url.path(), "" | "/") {
        return Err(invalid_with("must be an https origin with no path"));
    }
    Ok(url.to_string().trim_end_matches('/').to_string())
}

/// Parses `UPSTAGE_GRAPH_RPS`, falling back to `default` when unset, on top
/// of [`positive_float_or_default`]'s zero/negative/non-finite rejection,
/// then rejects a rate outside the range
/// `appview::pds::limiter_period_for_rate` accepts (review round 2, defect
/// D): a period longer than a day, like `0.00001`'s roughly 27.8-hour
/// period, or shorter than a nanosecond, like `1e10`'s (defect A). Config
/// load and `PdsClient::new` share that one range check, so a `Config` built
/// by `load` can never fail `PdsClient::new` on its rate; `publish::run`
/// still maps a `PdsClient::from_config` failure to a `PublishError` rather
/// than `.expect()`, for a `Config` built some other way.
fn graph_rps_or_default(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: f64,
) -> Result<f64, ConfigError> {
    let value = positive_float_or_default(lookup, name, default)?;
    if crate::appview::pds::limiter_period_for_rate(value).is_err() {
        return Err(ConfigError::Invalid {
            name,
            value: value.to_string(),
            reason: "period must be between 1ns and 1 day".to_string(),
        });
    }
    Ok(value)
}

/// Parses `UPSTAGE_APPVIEW_RPS`, rejecting zero, negative and non-finite values
/// (BC24) with the same reason string as the strictly-positive integers,
/// rather than [`nonneg_float_or_default`]'s separate "not a finite number"
/// and "must be >= 0" reasons, because zero is invalid here too.
/// `AppViewClient::new` keeps its own `InvalidRate` check (BC15) as defence
/// in depth, because it also takes a rate from a caller that did not come
/// through `load`.
fn positive_float_or_default(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: f64,
) -> Result<f64, ConfigError> {
    let value = number_or_default(lookup, name, default)?;
    if !value.is_finite() || value <= 0.0 {
        return Err(ConfigError::Invalid {
            name,
            value: value.to_string(),
            reason: "must be greater than zero".to_string(),
        });
    }
    Ok(value)
}

/// Splits `UPSTAGE_JETSTREAM_URL` on `,`, trims each entry, and drops empty
/// entries (BC26). Falls back to `default` when unset (BC27). At least one
/// host is required (BC28): an empty, whitespace-only, or all-entries-empty
/// value is invalid, the same rule as `drop_labels`. Every surviving entry
/// must start with `wss://` or `ws://` (BC29); the client (`client.rs`)
/// rotates through the list on every failed connect (BC30), never
/// reconnecting to the same host alone unless only one was configured
/// (BC25).
fn jetstream_urls(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: &str,
) -> Result<Vec<String>, ConfigError> {
    let raw = string_or_default(lookup, name, default);
    let urls: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect();
    if urls.is_empty() {
        return Err(ConfigError::Invalid {
            name,
            value: raw,
            reason: "at least one host is required".to_string(),
        });
    }
    for url in &urls {
        if !(url.starts_with("wss://") || url.starts_with("ws://")) {
            return Err(ConfigError::Invalid {
                name,
                value: url.clone(),
                reason: "must start with wss:// or ws://".to_string(),
            });
        }
    }
    Ok(urls)
}

/// Splits `UPSTAGE_DROP_LABELS` on `,`, trims each entry, and drops empty
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
        db_path: string_or_default(&lookup, "UPSTAGE_DB_PATH", "/data/upstage.db"),
        http_addr: string_or_default(&lookup, "UPSTAGE_HTTP_ADDR", "0.0.0.0:3000"),
        hostname: required(&lookup, "UPSTAGE_HOSTNAME")?,
        publisher_did: required(&lookup, "UPSTAGE_PUBLISHER_DID")?,
        feed_rkey: string_or_default(&lookup, "UPSTAGE_FEED_RKEY", "upstaged"),
        jetstream_urls: jetstream_urls(
            &lookup,
            "UPSTAGE_JETSTREAM_URL",
            "wss://jetstream.us-east.bsky.network,wss://jetstream.us-west.bsky.network",
        )?,
        appview_url: string_or_default(
            &lookup,
            "UPSTAGE_APPVIEW_URL",
            "https://public.api.bsky.app",
        ),
        w_repost: nonneg_float_or_default(&lookup, "UPSTAGE_W_REPOST", 2.0)?,
        w_reply: nonneg_float_or_default(&lookup, "UPSTAGE_W_REPLY", 0.5)?,
        k: positive_u32_or_default(&lookup, "UPSTAGE_K", 5)?,
        p: number_or_default(&lookup, "UPSTAGE_P", 50)?,
        m: nonneg_float_or_default(&lookup, "UPSTAGE_M", 1.25)?,
        candidate_ttl_h: number_or_default(&lookup, "UPSTAGE_CANDIDATE_TTL_H", 48)?,
        feed_ttl_d: number_or_default(&lookup, "UPSTAGE_FEED_TTL_D", 30)?,
        scorer_interval_s: positive_u32_or_default(&lookup, "UPSTAGE_SCORER_INTERVAL_S", 60)?,
        reverify_interval_s: positive_u32_or_default(&lookup, "UPSTAGE_REVERIFY_INTERVAL_S", 600)?,
        follower_floor: number_or_default(&lookup, "UPSTAGE_FOLLOWER_FLOOR", 2000)?,
        drop_labels: drop_labels(
            &lookup,
            "UPSTAGE_DROP_LABELS",
            "porn,sexual,graphic-media,nudity,!hide,!warn,spam",
        )?,
        prefilter_fraction: nonneg_float_or_default(&lookup, "UPSTAGE_PREFILTER_FRACTION", 0.5)?,
        appview_rps: positive_float_or_default(&lookup, "UPSTAGE_APPVIEW_RPS", 1.0)?,
        log: log_filter_or_default(&lookup, "UPSTAGE_LOG", "info")?,
        bsky_handle: optional(&lookup, "BSKY_HANDLE"),
        bsky_app_password: optional(&lookup, "BSKY_APP_PASSWORD").map(Secret),
        pds_url: pds_url_or_default(&lookup, "UPSTAGE_PDS_URL", "https://bsky.social")?,
        graph_rps: graph_rps_or_default(&lookup, "UPSTAGE_GRAPH_RPS", 8.0)?,
        health_max_lag_s: positive_u32_or_default(&lookup, "UPSTAGE_HEALTH_MAX_LAG_S", 300)?,
        guard_histogram_h: number_or_default(&lookup, "UPSTAGE_GUARD_HISTOGRAM_H", 24)?,
        author_ttl_h: positive_u32_or_default(&lookup, "UPSTAGE_AUTHOR_TTL_H", 24)?,
        author_inactive_ttl_h: positive_u32_or_default(
            &lookup,
            "UPSTAGE_AUTHOR_INACTIVE_TTL_H",
            1,
        )?,
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
        [("UPSTAGE_HOSTNAME", "feed.example.com"), ("UPSTAGE_PUBLISHER_DID", "did:plc:abc")]
    }

    #[test]
    fn missing_required_var_fails() {
        let lookup = env(&[("UPSTAGE_PUBLISHER_DID", "did:plc:abc")]);
        let err = load(lookup).unwrap_err();
        assert_eq!(err, ConfigError::Missing("UPSTAGE_HOSTNAME"));
    }

    #[test]
    fn missing_publisher_did_fails() {
        let lookup = env(&[("UPSTAGE_HOSTNAME", "feed.example.com")]);
        let err = load(lookup).unwrap_err();
        assert_eq!(err, ConfigError::Missing("UPSTAGE_PUBLISHER_DID"));
    }

    #[test]
    fn empty_required_var_is_missing() {
        let lookup = env(&[("UPSTAGE_HOSTNAME", ""), ("UPSTAGE_PUBLISHER_DID", "did:plc:abc")]);
        let err = load(lookup).unwrap_err();
        assert_eq!(err, ConfigError::Missing("UPSTAGE_HOSTNAME"));
    }

    #[test]
    fn whitespace_required_var_is_missing() {
        let lookup =
            env(&[("UPSTAGE_HOSTNAME", "feed.example.com"), ("UPSTAGE_PUBLISHER_DID", "   ")]);
        let err = load(lookup).unwrap_err();
        assert_eq!(err, ConfigError::Missing("UPSTAGE_PUBLISHER_DID"));
    }

    #[test]
    fn malformed_number_fails() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_K", "five"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, value, .. } => {
                assert_eq!(name, "UPSTAGE_K");
                assert_eq!(value, "five");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn empty_number_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_W_REPOST", ""));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_W_REPOST"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn feed_rkey_falls_back_to_default() {
        let config = load(env(&required_pair())).unwrap();
        assert_eq!(config.feed_rkey, "upstaged");
    }

    #[test]
    fn every_optional_variable_falls_back_to_its_default() {
        let config = load(env(&required_pair())).unwrap();
        assert_eq!(config.db_path, "/data/upstage.db");
        assert_eq!(config.http_addr, "0.0.0.0:3000");
        assert_eq!(
            config.jetstream_urls,
            vec!["wss://jetstream.us-east.bsky.network", "wss://jetstream.us-west.bsky.network"]
        );
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
        assert_eq!(config.pds_url, "https://bsky.social");
        assert_eq!(config.graph_rps, 8.0);
        assert_eq!(config.log, "info");
        assert_eq!(config.health_max_lag_s, 300);
        assert_eq!(
            config.drop_labels,
            vec!["porn", "sexual", "graphic-media", "nudity", "!hide", "!warn", "spam"]
        );
        assert_eq!(config.guard_histogram_h, 24);
        assert_eq!(config.author_ttl_h, 24);
        assert_eq!(config.author_inactive_ttl_h, 1);
    }

    #[test]
    fn unrecognised_variable_is_ignored() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_TYPO", "surprise"));
        assert!(load(env(&pairs)).is_ok());
    }

    #[test]
    fn single_jetstream_url_is_kept_as_one_host() {
        // BC25: one configured host stays one host; the client retries that
        // same host rather than rotating.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_JETSTREAM_URL", "wss://jetstream.example.com"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.jetstream_urls, vec!["wss://jetstream.example.com"]);
    }

    #[test]
    fn jetstream_url_list_is_split_trimmed_and_kept_in_order() {
        // BC26.
        let mut pairs = required_pair().to_vec();
        pairs.push((
            "UPSTAGE_JETSTREAM_URL",
            " wss://a.example.com, ws://b.example.com ,wss://c.example.com",
        ));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(
            config.jetstream_urls,
            vec!["wss://a.example.com", "ws://b.example.com", "wss://c.example.com"]
        );
    }

    #[test]
    fn jetstream_url_default_is_two_hosts() {
        // BC27.
        let config = load(env(&required_pair())).unwrap();
        assert_eq!(
            config.jetstream_urls,
            vec!["wss://jetstream.us-east.bsky.network", "wss://jetstream.us-west.bsky.network"]
        );
    }

    #[test]
    fn empty_jetstream_url_is_invalid() {
        // BC28: at least one host is required.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_JETSTREAM_URL", "   , , "));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_JETSTREAM_URL"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn jetstream_url_bad_scheme_is_invalid() {
        // BC29: every entry must start with wss:// or ws://.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_JETSTREAM_URL", "wss://good.example.com,https://bad.example.com"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, value, .. } => {
                assert_eq!(name, "UPSTAGE_JETSTREAM_URL");
                assert_eq!(value, "https://bad.example.com");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn drop_labels_splits_trims_and_drops_empty_entries() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_DROP_LABELS", " porn, , spam ,nudity"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.drop_labels, vec!["porn", "spam", "nudity"]);
    }

    #[test]
    fn empty_drop_labels_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_DROP_LABELS", "   "));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_DROP_LABELS"),
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
        pairs.push(("BSKY_HANDLE", "upstage.bsky.social"));
        pairs.push(("BSKY_APP_PASSWORD", "secret"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.bsky_handle, Some("upstage.bsky.social".to_string()));
        assert_eq!(config.bsky_app_password.as_ref().map(Secret::expose), Some("secret"));
    }

    #[test]
    fn debug_output_never_contains_the_app_password() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("BSKY_APP_PASSWORD", "hunter2"));
        let config = load(env(&pairs)).unwrap();
        let printed = format!("{config:?}");
        assert!(!printed.contains("hunter2"));
        assert!(printed.contains("[redacted]"));
    }

    #[test]
    fn malformed_log_filter_fails() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_LOG", "target=notalevel"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_LOG"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn valid_log_filter_is_accepted() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_LOG", "debug,upstage=trace"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.log, "debug,upstage=trace");
    }

    #[test]
    fn nan_weight_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_W_REPOST", "nan"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_W_REPOST");
                assert_eq!(reason, "not a finite number");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn infinite_weight_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_W_REPOST", "inf"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_W_REPOST");
                assert_eq!(reason, "not a finite number");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn zero_k_is_invalid() {
        // BC23: UPSTAGE_K is a divisor, so 0 is never a working setting.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_K", "0"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_K");
                assert_eq!(reason, "must be greater than zero");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn zero_scorer_interval_is_invalid() {
        // BC23: UPSTAGE_SCORER_INTERVAL_S is a timer period.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_SCORER_INTERVAL_S", "0"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_SCORER_INTERVAL_S");
                assert_eq!(reason, "must be greater than zero");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn zero_reverify_interval_is_invalid() {
        // BC23: UPSTAGE_REVERIFY_INTERVAL_S is a timer period.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_REVERIFY_INTERVAL_S", "0"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_REVERIFY_INTERVAL_S");
                assert_eq!(reason, "must be greater than zero");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn zero_negative_or_nonfinite_appview_rps_is_invalid() {
        // BC24: UPSTAGE_APPVIEW_RPS rejects zero, negative and non-finite
        // values, all with the same reason string.
        for bad in ["0", "-1.0", "nan", "inf"] {
            let mut pairs = required_pair().to_vec();
            pairs.push(("UPSTAGE_APPVIEW_RPS", bad));
            let err = load(env(&pairs)).unwrap_err();
            match err {
                ConfigError::Invalid { name, reason, .. } => {
                    assert_eq!(name, "UPSTAGE_APPVIEW_RPS");
                    assert_eq!(reason, "must be greater than zero");
                }
                other => panic!("expected Invalid for {bad}, got {other:?}"),
            }
        }
    }

    #[test]
    fn zero_health_max_lag_is_invalid() {
        // BC27: UPSTAGE_HEALTH_MAX_LAG_S rejects zero, same as the other
        // positive_u32_or_default fields.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_HEALTH_MAX_LAG_S", "0"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_HEALTH_MAX_LAG_S");
                assert_eq!(reason, "must be greater than zero");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn empty_health_max_lag_is_invalid() {
        // BC27: empty is malformed, not the default.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_HEALTH_MAX_LAG_S", ""));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_HEALTH_MAX_LAG_S"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn non_numeric_health_max_lag_is_invalid() {
        // BC27.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_HEALTH_MAX_LAG_S", "soon"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_HEALTH_MAX_LAG_S"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn custom_health_max_lag_is_read() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_HEALTH_MAX_LAG_S", "120"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.health_max_lag_s, 120);
    }

    #[test]
    fn feed_uri_is_the_at_uri_of_the_generator_record() {
        // BC45.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_FEED_RKEY", "upstaged"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.feed_uri(), "at://did:plc:abc/app.bsky.feed.generator/upstaged");
    }

    #[test]
    fn did_web_is_did_web_prefixed_hostname() {
        // BC45.
        let config = load(env(&required_pair())).unwrap();
        assert_eq!(config.did_web(), "did:web:feed.example.com");
    }

    #[test]
    fn zero_guard_histogram_h_disables_the_period() {
        // BC32: 0 is a valid value, not rejected like the positive_u32 fields.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_GUARD_HISTOGRAM_H", "0"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.guard_histogram_h, 0);
    }

    #[test]
    fn malformed_guard_histogram_h_is_invalid() {
        // BC32.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_GUARD_HISTOGRAM_H", "soon"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_GUARD_HISTOGRAM_H"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn empty_guard_histogram_h_is_invalid() {
        // BC32: empty is malformed, not the default.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_GUARD_HISTOGRAM_H", ""));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_GUARD_HISTOGRAM_H"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn custom_guard_histogram_h_is_read() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_GUARD_HISTOGRAM_H", "12"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.guard_histogram_h, 12);
    }

    #[test]
    fn zero_author_ttl_h_is_invalid() {
        // BC33: a zero TTL would refetch every DID on every pass.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_AUTHOR_TTL_H", "0"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_AUTHOR_TTL_H");
                assert_eq!(reason, "must be greater than zero");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn malformed_author_ttl_h_is_invalid() {
        // BC33.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_AUTHOR_TTL_H", "soon"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_AUTHOR_TTL_H"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn empty_author_ttl_h_is_invalid() {
        // BC33: empty is malformed, not the default.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_AUTHOR_TTL_H", ""));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_AUTHOR_TTL_H"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn custom_author_ttl_h_is_read() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_AUTHOR_TTL_H", "6"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.author_ttl_h, 6);
    }

    #[test]
    fn zero_author_inactive_ttl_h_is_invalid() {
        // BC45: the same rule as UPSTAGE_AUTHOR_TTL_H.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_AUTHOR_INACTIVE_TTL_H", "0"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_AUTHOR_INACTIVE_TTL_H");
                assert_eq!(reason, "must be greater than zero");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn malformed_author_inactive_ttl_h_is_invalid() {
        // BC45.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_AUTHOR_INACTIVE_TTL_H", "soon"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_AUTHOR_INACTIVE_TTL_H"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn empty_author_inactive_ttl_h_is_invalid() {
        // BC45: empty is malformed, not the default.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_AUTHOR_INACTIVE_TTL_H", ""));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_AUTHOR_INACTIVE_TTL_H"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn custom_author_inactive_ttl_h_is_read() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_AUTHOR_INACTIVE_TTL_H", "2"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.author_inactive_ttl_h, 2);
    }

    #[test]
    fn pds_defaults() {
        // AC7 (story 02): UPSTAGE_PDS_URL and UPSTAGE_GRAPH_RPS load with
        // their defaults, and UPSTAGE_GRAPH_RPS=0 is rejected the same way
        // as UPSTAGE_APPVIEW_RPS (BC14).
        let config = load(env(&required_pair())).unwrap();
        assert_eq!(config.pds_url, "https://bsky.social");
        assert_eq!(config.graph_rps, 8.0);

        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_GRAPH_RPS", "0"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_GRAPH_RPS");
                assert_eq!(reason, "must be greater than zero");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn pds_url_rejects_non_https() {
        // Review round 1, finding 4, BC15: UPSTAGE_PDS_URL must be https,
        // and the default still loads unaffected.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_PDS_URL", "http://bsky.social"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, value, .. } => {
                assert_eq!(name, "UPSTAGE_PDS_URL");
                assert_eq!(value, "[redacted]");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn pds_url_can_be_overridden_but_not_emptied() {
        // BC15: an explicit empty value is invalid, unlike a bare unset.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_PDS_URL", "https://pds.example.com"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.pds_url, "https://pds.example.com");

        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_PDS_URL", "  "));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_PDS_URL"),
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn pds_url_rejects_bare_scheme_query_fragment_or_userinfo() {
        // Review round 2, defects B, E and F: a bare `https://` has no
        // host; a query or a fragment would swallow the `/xrpc/<nsid>`
        // path `HttpPdsTransport` builds; userinfo can hold the very
        // credentials this check exists to protect, so the userinfo case's
        // error never echoes the raw value, in `Display` or `Debug`.
        for bad in [
            "https://",
            "https://bsky.social#x",
            "https://bsky.social?x=1",
            "https://u:p@bsky.social",
        ] {
            let mut pairs = required_pair().to_vec();
            pairs.push(("UPSTAGE_PDS_URL", bad));
            let err = load(env(&pairs)).unwrap_err();
            match err {
                ConfigError::Invalid { name, value, .. } => {
                    assert_eq!(name, "UPSTAGE_PDS_URL");
                    assert_eq!(value, "[redacted]");
                }
                other => panic!("expected Invalid for {bad}, got {other:?}"),
            }
        }

        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_PDS_URL", "https://u:p@bsky.social"));
        let err = load(env(&pairs)).unwrap_err();
        let displayed = err.to_string();
        let debugged = format!("{err:?}");
        assert!(!displayed.contains("p@"), "Display leaked userinfo: {displayed}");
        assert!(!debugged.contains("p@"), "Debug leaked userinfo: {debugged}");
    }

    #[test]
    fn pds_url_normalises_case_and_trailing_slash() {
        // Review round 2, defect C: the scheme check is case-insensitive
        // (the parser lowercases it) and the value is trimmed; both forms
        // normalise to the same base with no trailing slash.
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_PDS_URL", "HTTPS://bsky.social"));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.pds_url, "https://bsky.social");

        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_PDS_URL", " https://bsky.social/ "));
        let config = load(env(&pairs)).unwrap();
        assert_eq!(config.pds_url, "https://bsky.social");
    }

    #[test]
    fn pds_url_rejects_a_path_other_than_a_bare_slash() {
        // Review round 3, finding 1, and the engineer's Step 7.5 answer:
        // UPSTAGE_PDS_URL is an https origin only. HttpPdsTransport appends
        // `/xrpc/<nsid>` itself, so any other path is rejected here. Review
        // round 4, findings 1 to 3: this rejection carries its own reason
        // naming the path rule, distinct from the generic scheme/host/
        // userinfo/query/fragment reason (correction slice 7.0).
        for bad in ["https://bsky.social/xrpc", "https://bsky.social/foo/"] {
            let mut pairs = required_pair().to_vec();
            pairs.push(("UPSTAGE_PDS_URL", bad));
            let err = load(env(&pairs)).unwrap_err();
            match err {
                ConfigError::Invalid { name, value, reason } => {
                    assert_eq!(name, "UPSTAGE_PDS_URL");
                    assert_eq!(value, "[redacted]");
                    assert!(
                        reason.contains("path"),
                        "expected reason to name the path rule, got {reason:?}"
                    );
                }
                other => panic!("expected Invalid for {bad}, got {other:?}"),
            }
        }
    }

    #[test]
    fn pds_url_accepts_bare_slash_and_a_port() {
        for good in ["https://bsky.social", "https://bsky.social/", "https://pds.example.com:8443"]
        {
            let mut pairs = required_pair().to_vec();
            pairs.push(("UPSTAGE_PDS_URL", good));
            let config = load(env(&pairs)).unwrap();
            assert_eq!(config.pds_url, good.trim_end_matches('/'));
        }
    }

    #[test]
    fn graph_rps_rejects_a_period_outside_a_nanosecond_to_a_day() {
        // Review round 2, defect D: UPSTAGE_GRAPH_RPS is bounded to the same
        // range PdsClient::new enforces, so a Config built by load can never
        // fail PdsClient::new on its rate.
        for bad in ["1e10", "0.00001"] {
            let mut pairs = required_pair().to_vec();
            pairs.push(("UPSTAGE_GRAPH_RPS", bad));
            let err = load(env(&pairs)).unwrap_err();
            match err {
                ConfigError::Invalid { name, .. } => assert_eq!(name, "UPSTAGE_GRAPH_RPS"),
                other => panic!("expected Invalid for {bad}, got {other:?}"),
            }
        }
    }

    #[test]
    fn negative_weight_is_malformed() {
        let mut pairs = required_pair().to_vec();
        pairs.push(("UPSTAGE_M", "-1.0"));
        let err = load(env(&pairs)).unwrap_err();
        match err {
            ConfigError::Invalid { name, reason, .. } => {
                assert_eq!(name, "UPSTAGE_M");
                assert_eq!(reason, "must be >= 0");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }
}
