//! `dunk publish`, TECH-DESIGN section 11.2: writes the
//! `app.bsky.feed.generator` record. A preflight reads `BSKY_HANDLE`,
//! `BSKY_APP_PASSWORD` and an optional avatar file before any network call
//! (BC1, BC2, BC3, BC4, BC5), `record_body` builds the record as a pure
//! function of `Config` and an optional uploaded blob (BC12), and
//! `publish_with` runs the three calls against [`BSKY_PDS_URL`]:
//! `createSession`, an optional `uploadBlob`, then an unconditional
//! `putRecord` (BC6 to BC10, BC13, BC14). The PDS is reached through
//! [`PdsClient`], a trait with one real `reqwest` implementation, the same
//! shape `src/scorer/mod.rs`'s `PostSource` uses over `AppViewClient`,
//! rather than an injectable base URL plus a test HTTP server: the crate has
//! no `axum`-based test-server dependency for unit tests, and the trait lets
//! a fake assert `put_record` was never called. Never run from `dunk run`
//! (TECH-DESIGN section 11.2); a separate manual step.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use thiserror::Error;

use crate::config::Config;

/// The PDS every session, upload and record write goes against. A constant,
/// not `cfg.appview_url`, matching TECH-DESIGN section 11.2: publishing is a
/// write against the operator's own PDS, not a read against the App View
/// (BC16).
pub const BSKY_PDS_URL: &str = "https://bsky.social";

/// The feed's own `displayName`, fixed per the engineer's answer (BC15): no
/// tone, sentiment or keyword wording (TECH-DESIGN D10).
pub const DISPLAY_NAME: &str = "Out-Quoted";

/// The feed's own `description`, fixed per the engineer's answer (BC15): no
/// tone, sentiment or keyword wording (TECH-DESIGN D10).
pub const DESCRIPTION: &str = "Quote posts that got more engagement than the post they quoted.";

/// The HTTP timeout every request carries, matching
/// `AppViewClient::with_base_url` (TECH-DESIGN section 8.1's budget applies
/// here too, BC10).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Every way `dunk publish` can fail. `main.rs` prints this and exits 1. No
/// variant carries `accessJwt` or `BSKY_APP_PASSWORD` (BC11): `Auth`,
/// `Upload` and `PutRecord` carry only the server's own status and body
/// text, never a request header or the credentials that built one.
#[derive(Debug, Error)]
pub enum PublishError {
    #[error("missing or empty environment variable {var}")]
    MissingCredentials { var: &'static str },
    #[error("avatar file not found: {path}")]
    AvatarNotFound { path: PathBuf },
    #[error("avatar file {path} has an unsupported extension; expected png, jpg or jpeg")]
    AvatarType { path: PathBuf },
    #[error("createSession failed, status {status}: {body}")]
    Auth { status: u16, body: String },
    #[error(
        "createSession returned did {session_did}, but DUNK_PUBLISHER_DID is {configured_did}"
    )]
    DidMismatch { session_did: String, configured_did: String },
    #[error("uploadBlob failed, status {status}: {body}")]
    Upload { status: u16, body: String },
    #[error("putRecord failed, status {status}: {body}")]
    PutRecord { status: u16, body: String },
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
}

/// The two credentials `preflight` reads, plain strings once past the check:
/// nothing downstream of `preflight` reads `BSKY_HANDLE` or
/// `BSKY_APP_PASSWORD` from `Config` again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub handle: String,
    pub app_password: String,
}

/// An avatar file read off disk during preflight, with the `Content-Type`
/// its extension maps to (BC5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Avatar {
    pub bytes: Vec<u8>,
    pub content_type: &'static str,
}

/// Reads `BSKY_HANDLE` and `BSKY_APP_PASSWORD` off `cfg`, then `avatar_path`
/// off disk, before any `reqwest::Client` is built. `BSKY_HANDLE` or
/// `BSKY_APP_PASSWORD` missing or empty after trim fails first
/// (`MissingCredentials`, BC1); trimming happens in `config::load` already,
/// so a value present here is never empty. A given `--avatar` path that does
/// not exist fails with `AvatarNotFound` (BC2); an extension other than
/// `png`, `jpg` or `jpeg`, compared lowercased, fails with `AvatarType`
/// (BC4). No `--avatar` at all returns `Ok((credentials, None))` and reads no
/// file (BC3).
pub fn preflight(
    cfg: &Config,
    avatar_path: Option<&Path>,
) -> Result<(Credentials, Option<Avatar>), PublishError> {
    let handle = cfg
        .bsky_handle
        .as_ref()
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or(PublishError::MissingCredentials { var: "BSKY_HANDLE" })?;
    let app_password = cfg
        .bsky_app_password
        .as_ref()
        .map(crate::config::Secret::expose)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or(PublishError::MissingCredentials { var: "BSKY_APP_PASSWORD" })?;
    let credentials = Credentials { handle, app_password };

    let avatar = match avatar_path {
        None => None,
        Some(path) => {
            if !path.is_file() {
                return Err(PublishError::AvatarNotFound { path: path.to_path_buf() });
            }
            let content_type = match path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(str::to_lowercase)
                .as_deref()
            {
                Some("png") => "image/png",
                Some("jpg") | Some("jpeg") => "image/jpeg",
                _ => return Err(PublishError::AvatarType { path: path.to_path_buf() }),
            };
            let bytes = std::fs::read(path)
                .map_err(|_err| PublishError::AvatarNotFound { path: path.to_path_buf() })?;
            Some(Avatar { bytes, content_type })
        }
    };

    Ok((credentials, avatar))
}

/// Builds the `putRecord` body, pure over `cfg`, an optional uploaded blob
/// value (`uploadBlob`'s own `blob` field, when an avatar was uploaded), and
/// `now` (BC12). `record.avatar` is present only when `blob` is `Some`
/// (BC3). `createdAt` is `now` formatted RFC 3339 in UTC.
pub fn record_body(
    cfg: &Config,
    session_did: &str,
    blob: Option<Value>,
    now: DateTime<Utc>,
) -> Value {
    let mut record = json!({
        "$type": "app.bsky.feed.generator",
        "did": cfg.did_web(),
        "displayName": DISPLAY_NAME,
        "description": DESCRIPTION,
        "acceptsInteractions": true,
        "createdAt": now.to_rfc3339(),
    });
    if let Some(blob) = blob {
        record["avatar"] = blob;
    }
    json!({
        "repo": session_did,
        "collection": "app.bsky.feed.generator",
        "rkey": cfg.feed_rkey,
        "record": record,
    })
}

/// One `createSession` response, the fields `publish_with` needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub did: String,
    pub access_jwt: String,
}

/// The PDS surface `dunk publish` needs: session creation, blob upload and
/// record write. A trait rather than an injectable base URL plus a test HTTP
/// server, per `## Approach` in `spec.md`: the crate has no HTTP test server
/// dependency, and `HttpPdsClient` is the one real implementation;
/// `publish_with`'s tests use an in-memory fake that can assert `put_record`
/// was never called.
pub trait PdsClient {
    fn create_session(
        &self,
        handle: &str,
        app_password: &str,
    ) -> impl Future<Output = Result<Session, PublishError>> + Send;

    fn upload_blob(
        &self,
        access_jwt: &str,
        avatar: &Avatar,
    ) -> impl Future<Output = Result<Value, PublishError>> + Send;

    fn put_record(
        &self,
        access_jwt: &str,
        body: &Value,
    ) -> impl Future<Output = Result<(), PublishError>> + Send;
}

/// The real [`PdsClient`], one `reqwest::Client` against [`BSKY_PDS_URL`].
/// Built exactly as `AppViewClient::with_base_url` builds its own: rustls by
/// Cargo feature, a 10 s timeout, so timeout behaviour matches the rest of
/// the binary. No retry (BC10): a connection error or the timeout becomes
/// `PublishError::Transport` straight from `?`.
#[derive(Debug)]
pub struct HttpPdsClient {
    base_url: String,
    http: reqwest::Client,
}

impl HttpPdsClient {
    pub fn new() -> Self {
        Self::with_base_url(BSKY_PDS_URL.to_string())
    }

    /// `new`'s body, taking the base URL directly so tests can point the
    /// client at an unreachable address without touching [`BSKY_PDS_URL`].
    fn with_base_url(base_url: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("reqwest::Client::builder with only a timeout never fails to build");
        Self { base_url, http }
    }
}

impl Default for HttpPdsClient {
    fn default() -> Self {
        Self::new()
    }
}

impl PdsClient for HttpPdsClient {
    async fn create_session(
        &self,
        handle: &str,
        app_password: &str,
    ) -> Result<Session, PublishError> {
        let url = format!("{}/xrpc/com.atproto.server.createSession", self.base_url);
        let response = self
            .http
            .post(&url)
            .json(&json!({ "identifier": handle, "password": app_password }))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(PublishError::Auth { status: status.as_u16(), body });
        }
        let body: Value = response.json().await?;
        let did = body.get("did").and_then(Value::as_str).map(str::to_string).ok_or_else(|| {
            PublishError::Auth {
                status: status.as_u16(),
                body: "createSession response carried no did".to_string(),
            }
        })?;
        let access_jwt =
            body.get("accessJwt").and_then(Value::as_str).map(str::to_string).ok_or_else(|| {
                PublishError::Auth {
                    status: status.as_u16(),
                    body: "createSession response carried no accessJwt".to_string(),
                }
            })?;
        Ok(Session { did, access_jwt })
    }

    async fn upload_blob(&self, access_jwt: &str, avatar: &Avatar) -> Result<Value, PublishError> {
        let url = format!("{}/xrpc/com.atproto.repo.uploadBlob", self.base_url);
        let response = self
            .http
            .post(&url)
            .bearer_auth(access_jwt)
            .header(reqwest::header::CONTENT_TYPE, avatar.content_type)
            .body(avatar.bytes.clone())
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(PublishError::Upload { status: status.as_u16(), body });
        }
        let body: Value = response.json().await?;
        Ok(body["blob"].clone())
    }

    async fn put_record(&self, access_jwt: &str, body: &Value) -> Result<(), PublishError> {
        let url = format!("{}/xrpc/com.atproto.repo.putRecord", self.base_url);
        let response = self.http.post(&url).bearer_auth(access_jwt).json(body).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(PublishError::PutRecord { status: status.as_u16(), body });
        }
        Ok(())
    }
}

/// The publish sequence, generic over [`PdsClient`] so tests can pass a fake
/// (BC7, BC13, BC14): `createSession`, then a DID check against
/// `cfg.publisher_did` (BC7, this catches publishing from the wrong
/// account), then an optional `uploadBlob` when `avatar` is `Some` (BC3),
/// then an unconditional `putRecord` (BC14: nothing reads the record first,
/// so a second run overwrites the same one). Returns `cfg.feed_uri()` on
/// success (BC13).
pub async fn publish_with(
    client: &impl PdsClient,
    cfg: &Config,
    credentials: &Credentials,
    avatar: Option<&Avatar>,
) -> Result<String, PublishError> {
    let session = client.create_session(&credentials.handle, &credentials.app_password).await?;
    if session.did != cfg.publisher_did {
        return Err(PublishError::DidMismatch {
            session_did: session.did,
            configured_did: cfg.publisher_did.clone(),
        });
    }

    let blob = match avatar {
        None => None,
        Some(avatar) => Some(client.upload_blob(&session.access_jwt, avatar).await?),
    };

    let body = record_body(cfg, &session.did, blob, Utc::now());
    client.put_record(&session.access_jwt, &body).await?;

    Ok(cfg.feed_uri())
}

/// `dunk publish`'s entry point. `preflight` runs first, so a missing
/// credential or a missing/unsupported avatar file stops the run before
/// [`HttpPdsClient::new`] is even built (BC1, BC2) and before any network is
/// reachable.
pub async fn run(cfg: &Config, avatar_path: Option<&Path>) -> Result<String, PublishError> {
    let (credentials, avatar) = preflight(cfg, avatar_path)?;
    let client = HttpPdsClient::new();
    publish_with(&client, cfg, &credentials, avatar.as_ref()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Mutex;

    fn required_pairs() -> Vec<(&'static str, &'static str)> {
        vec![("DUNK_HOSTNAME", "feed.example.com"), ("DUNK_PUBLISHER_DID", "did:plc:abc")]
    }

    fn config_with(extra: &[(&str, &str)]) -> Config {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(extra);
        let map: std::collections::HashMap<String, String> =
            pairs.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        crate::config::load(move |name| map.get(name).cloned()).expect("minimal config loads")
    }

    fn creds() -> Credentials {
        Credentials { handle: "dunk.bsky.social".to_string(), app_password: "app-pass".to_string() }
    }

    // --- preflight ---------------------------------------------------

    #[tokio::test]
    async fn missing_credentials_fails_fast() {
        // AC1, BC1: no BSKY_HANDLE or BSKY_APP_PASSWORD set at all. This
        // goes through `run`, not `preflight`, so it proves the ordering
        // inside `run` too: the check returns before `HttpPdsClient::new`
        // is built and before any network is reachable. A `run` that built
        // its client first would reach `BSKY_PDS_URL` here and fail with a
        // different variant, or hang for the 10 s timeout.
        let cfg = config_with(&[]);
        let err = run(&cfg, None).await.unwrap_err();
        match err {
            PublishError::MissingCredentials { var } => assert_eq!(var, "BSKY_HANDLE"),
            other => panic!("expected MissingCredentials, got {other:?}"),
        }
    }

    #[test]
    fn missing_app_password_fails() {
        // BC1: handle present, password absent.
        let cfg = config_with(&[("BSKY_HANDLE", "dunk.bsky.social")]);
        let err = preflight(&cfg, None).unwrap_err();
        match err {
            PublishError::MissingCredentials { var } => assert_eq!(var, "BSKY_APP_PASSWORD"),
            other => panic!("expected MissingCredentials, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_avatar_fails_fast() {
        // AC2, BC2: a path that names no file. Credentials are present, so
        // only the avatar check can stop this. It goes through `run`, not
        // `preflight`, for the same reason as `missing_credentials_fails_fast`:
        // it proves `run` checks the file before it builds a client.
        let cfg =
            config_with(&[("BSKY_HANDLE", "dunk.bsky.social"), ("BSKY_APP_PASSWORD", "app-pass")]);
        let path = std::env::temp_dir().join("dunk-publish-test-no-such-avatar.png");
        let _ = std::fs::remove_file(&path);
        let err = run(&cfg, Some(&path)).await.unwrap_err();
        match err {
            PublishError::AvatarNotFound { path: got } => assert_eq!(got, path),
            other => panic!("expected AvatarNotFound, got {other:?}"),
        }
    }

    #[test]
    fn preflight_rejects_missing_credentials_on_its_own() {
        // BC1: the same check at the `preflight` level, so a later refactor
        // that moves the call site still has the unit covered.
        let cfg = config_with(&[]);
        match preflight(&cfg, None).unwrap_err() {
            PublishError::MissingCredentials { var } => assert_eq!(var, "BSKY_HANDLE"),
            other => panic!("expected MissingCredentials, got {other:?}"),
        }
    }

    #[test]
    fn no_avatar_flag_reads_no_file_and_returns_none() {
        // BC3.
        let cfg =
            config_with(&[("BSKY_HANDLE", "dunk.bsky.social"), ("BSKY_APP_PASSWORD", "app-pass")]);
        let (_, avatar) = preflight(&cfg, None).expect("preflight succeeds with no avatar");
        assert_eq!(avatar, None);
    }

    #[test]
    fn unsupported_avatar_extension_fails() {
        // BC4.
        let cfg =
            config_with(&[("BSKY_HANDLE", "dunk.bsky.social"), ("BSKY_APP_PASSWORD", "app-pass")]);
        let path = std::env::temp_dir().join("dunk-publish-test-avatar.gif");
        let mut file = std::fs::File::create(&path).expect("temp file creates");
        file.write_all(b"not really a gif").expect("temp file writes");
        let err = preflight(&cfg, Some(&path)).unwrap_err();
        match err {
            PublishError::AvatarType { path: got } => assert_eq!(got, path),
            other => panic!("expected AvatarType, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn avatar_extension_maps_to_content_type_case_insensitively() {
        // BC5.
        let cfg =
            config_with(&[("BSKY_HANDLE", "dunk.bsky.social"), ("BSKY_APP_PASSWORD", "app-pass")]);
        for (ext, expected) in [
            ("png", "image/png"),
            ("PNG", "image/png"),
            ("jpg", "image/jpeg"),
            ("JPEG", "image/jpeg"),
        ] {
            let path = std::env::temp_dir().join(format!("dunk-publish-test-avatar.{ext}"));
            std::fs::write(&path, b"bytes").expect("temp file writes");
            let (_, avatar) = preflight(&cfg, Some(&path)).expect("supported extension succeeds");
            assert_eq!(avatar.expect("avatar present").content_type, expected);
            let _ = std::fs::remove_file(&path);
        }
    }

    // --- record_body ---------------------------------------------------

    #[test]
    fn record_body_shape() {
        // AC3, BC12.
        let cfg = config_with(&[("DUNK_FEED_RKEY", "dunks")]);
        let now = DateTime::parse_from_rfc3339("2026-09-21T00:00:00Z").unwrap().with_timezone(&Utc);
        let body = record_body(&cfg, "did:plc:abc", None, now);
        assert_eq!(body["repo"], "did:plc:abc");
        assert_eq!(body["collection"], "app.bsky.feed.generator");
        assert_eq!(body["rkey"], "dunks");
        let record = &body["record"];
        assert_eq!(record["$type"], "app.bsky.feed.generator");
        assert_eq!(record["did"], "did:web:feed.example.com");
        assert_eq!(record["displayName"], DISPLAY_NAME);
        assert_eq!(record["description"], DESCRIPTION);
        assert_eq!(record["acceptsInteractions"], true);
        assert_eq!(record["createdAt"], "2026-09-21T00:00:00+00:00");
        assert!(record.get("avatar").is_none());
    }

    #[test]
    fn record_body_carries_avatar_blob_only_when_uploaded() {
        // BC3, BC12.
        let cfg = config_with(&[]);
        let now = Utc::now();
        let blob = json!({"$type": "blob", "ref": {"$link": "bafy..."}, "mimeType": "image/png"});
        let body = record_body(&cfg, "did:plc:abc", Some(blob.clone()), now);
        assert_eq!(body["record"]["avatar"], blob);
    }

    // --- publish_with, via a fake PdsClient ---------------------------

    /// An in-memory fake [`PdsClient`]. Every call is counted, so a test can
    /// assert a later call in the sequence never ran (BC6, BC7, BC8).
    struct FakePds {
        session: Result<Session, PublishError>,
        upload: Result<Value, PublishError>,
        put_result: Result<(), PublishError>,
        create_session_calls: AtomicUsize,
        upload_blob_calls: AtomicUsize,
        put_record_calls: AtomicUsize,
        last_put_body: Mutex<Option<Value>>,
    }

    impl FakePds {
        fn ok(session_did: &str) -> Self {
            Self {
                session: Ok(Session {
                    did: session_did.to_string(),
                    access_jwt: "access-jwt".to_string(),
                }),
                upload: Ok(json!({"$type": "blob"})),
                put_result: Ok(()),
                create_session_calls: AtomicUsize::new(0),
                upload_blob_calls: AtomicUsize::new(0),
                put_record_calls: AtomicUsize::new(0),
                last_put_body: Mutex::new(None),
            }
        }
    }

    impl PdsClient for FakePds {
        async fn create_session(
            &self,
            _handle: &str,
            _app_password: &str,
        ) -> Result<Session, PublishError> {
            self.create_session_calls.fetch_add(1, AtomicOrdering::SeqCst);
            match &self.session {
                Ok(session) => Ok(session.clone()),
                Err(PublishError::Auth { status, body }) => {
                    Err(PublishError::Auth { status: *status, body: body.clone() })
                }
                Err(other) => panic!("unexpected fixture error: {other:?}"),
            }
        }

        async fn upload_blob(
            &self,
            _access_jwt: &str,
            _avatar: &Avatar,
        ) -> Result<Value, PublishError> {
            self.upload_blob_calls.fetch_add(1, AtomicOrdering::SeqCst);
            match &self.upload {
                Ok(value) => Ok(value.clone()),
                Err(PublishError::Upload { status, body }) => {
                    Err(PublishError::Upload { status: *status, body: body.clone() })
                }
                Err(other) => panic!("unexpected fixture error: {other:?}"),
            }
        }

        async fn put_record(&self, _access_jwt: &str, body: &Value) -> Result<(), PublishError> {
            self.put_record_calls.fetch_add(1, AtomicOrdering::SeqCst);
            *self.last_put_body.lock().expect("lock") = Some(body.clone());
            match &self.put_result {
                Ok(()) => Ok(()),
                Err(PublishError::PutRecord { status, body }) => {
                    Err(PublishError::PutRecord { status: *status, body: body.clone() })
                }
                Err(other) => panic!("unexpected fixture error: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn prints_at_uri() {
        // AC4, BC13: publish_with returns the at-URI `dispatch` prints.
        let cfg = config_with(&[("DUNK_FEED_RKEY", "dunks")]);
        let client = FakePds::ok("did:plc:abc");
        let uri = publish_with(&client, &cfg, &creds(), None).await.expect("publish succeeds");
        assert_eq!(uri, "at://did:plc:abc/app.bsky.feed.generator/dunks");
        assert_eq!(client.create_session_calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(client.upload_blob_calls.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(client.put_record_calls.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn session_did_mismatch_fails() {
        // AC5, BC7: the session's own did differs from DUNK_PUBLISHER_DID;
        // put_record never runs.
        let cfg = config_with(&[]);
        let client = FakePds::ok("did:plc:someone-else");
        let err = publish_with(&client, &cfg, &creds(), None).await.unwrap_err();
        match err {
            PublishError::DidMismatch { session_did, configured_did } => {
                assert_eq!(session_did, "did:plc:someone-else");
                assert_eq!(configured_did, "did:plc:abc");
            }
            other => panic!("expected DidMismatch, got {other:?}"),
        }
        assert_eq!(client.put_record_calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn create_session_failure_stops_before_upload_or_put_record() {
        // BC6.
        let cfg = config_with(&[]);
        let mut client = FakePds::ok("did:plc:abc");
        client.session = Err(PublishError::Auth { status: 401, body: "bad password".to_string() });
        let err = publish_with(&client, &cfg, &creds(), None).await.unwrap_err();
        match err {
            PublishError::Auth { status, .. } => assert_eq!(status, 401),
            other => panic!("expected Auth, got {other:?}"),
        }
        assert_eq!(client.upload_blob_calls.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(client.put_record_calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn upload_failure_stops_before_put_record() {
        // BC8.
        let cfg = config_with(&[]);
        let mut client = FakePds::ok("did:plc:abc");
        client.upload = Err(PublishError::Upload { status: 500, body: "server error".to_string() });
        let avatar = Avatar { bytes: vec![1, 2, 3], content_type: "image/png" };
        let err = publish_with(&client, &cfg, &creds(), Some(&avatar)).await.unwrap_err();
        match err {
            PublishError::Upload { status, .. } => assert_eq!(status, 500),
            other => panic!("expected Upload, got {other:?}"),
        }
        assert_eq!(client.put_record_calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn put_record_failure_surfaces() {
        // BC9.
        let cfg = config_with(&[]);
        let mut client = FakePds::ok("did:plc:abc");
        client.put_result =
            Err(PublishError::PutRecord { status: 400, body: "bad record".to_string() });
        let err = publish_with(&client, &cfg, &creds(), None).await.unwrap_err();
        match err {
            PublishError::PutRecord { status, .. } => assert_eq!(status, 400),
            other => panic!("expected PutRecord, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn avatar_uploaded_when_given_and_put_record_body_carries_the_blob() {
        // BC3, BC12: an avatar given means uploadBlob runs and the blob
        // lands in the putRecord body.
        let cfg = config_with(&[]);
        let client = FakePds::ok("did:plc:abc");
        let avatar = Avatar { bytes: vec![1, 2, 3], content_type: "image/png" };
        publish_with(&client, &cfg, &creds(), Some(&avatar)).await.expect("publish succeeds");
        assert_eq!(client.upload_blob_calls.load(AtomicOrdering::SeqCst), 1);
        let body = client.last_put_body.lock().expect("lock").clone().expect("body recorded");
        assert_eq!(body["record"]["avatar"], json!({"$type": "blob"}));
    }

    #[tokio::test]
    async fn second_run_overwrites_the_same_record_unconditionally() {
        // AC-adjacent, BC14: two calls to publish_with both call
        // put_record; nothing reads the record first.
        let cfg = config_with(&[("DUNK_FEED_RKEY", "dunks")]);
        let client = FakePds::ok("did:plc:abc");
        publish_with(&client, &cfg, &creds(), None).await.expect("first publish succeeds");
        publish_with(&client, &cfg, &creds(), None).await.expect("second publish succeeds");
        assert_eq!(client.put_record_calls.load(AtomicOrdering::SeqCst), 2);
    }

    #[test]
    fn debug_on_publish_error_never_contains_a_password_or_token() {
        // BC11: no variant type even has a field for either, so this guards
        // the shape rather than a redaction step.
        let err = PublishError::Auth { status: 401, body: "invalid password".to_string() };
        let printed = format!("{err:?}");
        assert!(!printed.contains("app-pass"));
        assert!(!printed.contains("access-jwt"));
    }
}

/// Live test against a real Bluesky test account. `#[ignore]`d, so
/// `cargo test --all-features` never touches the network; run by hand with
/// `BSKY_HANDLE`, `BSKY_APP_PASSWORD`, `DUNK_HOSTNAME` and
/// `DUNK_PUBLISHER_DID` set: `cargo test -- --ignored publish_live` (AC7).
#[cfg(test)]
mod live_tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn publish_live() {
        let lookup = |name: &str| std::env::var(name).ok();
        let cfg = crate::config::load(lookup).expect("real environment provides a valid config");
        let uri = run(&cfg, None).await.expect("live publish round trip");
        assert!(uri.starts_with("at://"));
    }
}
