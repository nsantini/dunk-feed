//! `upstage publish`, TECH-DESIGN section 11.2: writes the
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
//! a fake assert `put_record` was never called. Never run from `upstage run`
//! (TECH-DESIGN section 11.2); a separate manual step.

use std::future::Future;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use thiserror::Error;

use crate::appview::http_client;
use crate::config::{Config, Secret};

/// The PDS every session, upload and record write goes against. A constant,
/// not `cfg.appview_url`, matching TECH-DESIGN section 11.2: publishing is a
/// write against the operator's own PDS, not a read against the App View
/// (BC16).
pub const BSKY_PDS_URL: &str = "https://bsky.social";

/// The feed's own `displayName`, fixed per the engineer's answer (BC15): no
/// tone, sentiment or keyword wording (TECH-DESIGN D10).
pub const DISPLAY_NAME: &str = "Upstaged";

/// The feed's own `description`, fixed per the engineer's answer (BC15): no
/// tone, sentiment or keyword wording (TECH-DESIGN D10).
pub const DESCRIPTION: &str = "Quote posts that got more engagement than the post they quoted.";

/// Every way `upstage publish` can fail. `main.rs` prints this and exits 1. No
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
    #[error("avatar file {path} could not be read: {source}")]
    AvatarRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("createSession failed, status {status}: {body}")]
    Auth { status: u16, body: String },
    #[error(
        "createSession returned did {session_did}, but UPSTAGE_PUBLISHER_DID is {configured_did}"
    )]
    DidMismatch { session_did: String, configured_did: String },
    #[error("uploadBlob failed, status {status}: {body}")]
    Upload { status: u16, body: String },
    #[error("putRecord failed, status {status}: {body}")]
    PutRecord { status: u16, body: String },
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
}

/// The two credentials `preflight` reads: nothing downstream of `preflight`
/// reads `BSKY_HANDLE` or `BSKY_APP_PASSWORD` from `Config` again. The
/// password stays inside a [`Secret`], so the derived `Debug` on this struct
/// prints `[redacted]` for it; it is exposed at one place only, the
/// `createSession` request body (BC11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub handle: String,
    pub app_password: Secret,
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
/// `BSKY_APP_PASSWORD` missing, or empty once trimmed, fails first
/// (`MissingCredentials`, BC1). The trim here is the only guard:
/// `config::required` trims, but `config::optional` is a bare `lookup(name)`
/// and both `BSKY_*` variables come through `optional`, so
/// `BSKY_APP_PASSWORD="   "` reaches this function as `Some("   ")` (BC18).
/// A given `--avatar` path that does not exist fails with `AvatarNotFound`
/// (BC2); an extension other than `png`, `jpg` or `jpeg`, compared
/// lowercased, fails with `AvatarType` (BC4); a file that exists but cannot
/// be read fails with `AvatarRead`, carrying the OS error (BC19). No
/// `--avatar` at all returns `Ok((credentials, None))` and reads no file
/// (BC3).
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
        .filter(|value| !value.expose().trim().is_empty())
        .cloned()
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
                .map_err(|source| PublishError::AvatarRead { path: path.to_path_buf(), source })?;
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

/// One `createSession` response, the fields `publish_with` needs. The
/// `accessJwt` stays inside a [`Secret`], so the derived `Debug` prints
/// `[redacted]` for it and no log or panic message can carry a live token
/// (BC11). It is exposed at two places only: the `Authorization` header on
/// `uploadBlob` and the one on `putRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub did: String,
    pub access_jwt: Secret,
}

/// One `uploadBlob` response, before [`validate_blob`] has looked at it: the
/// HTTP status the PDS answered with, and the decoded JSON body. The trait
/// hands both back rather than the blob itself, so the shape check is a pure
/// function `publish_with` runs and a fake can exercise (BC17).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadResponse {
    pub status: u16,
    pub body: Value,
}

/// Checks that `uploadBlob` really returned a blob before it is written into
/// a record (BC17). `createSession` already refuses a 2xx whose body carries
/// no `did` or no `accessJwt`; this is the same check one call later. A
/// `blob` that is absent, is not an object, or is missing `$type == "blob"`,
/// `ref`, `mimeType` or `size` becomes `PublishError::Upload` naming the
/// field at fault, so a malformed upload fails here instead of writing a
/// record whose `avatar` the Bluesky app cannot render.
pub fn validate_blob(response: &UploadResponse) -> Result<Value, PublishError> {
    let fail = |reason: &str| PublishError::Upload {
        status: response.status,
        body: format!("uploadBlob returned a 2xx body that is not a blob: {reason}"),
    };

    let blob = response.body.get("blob").ok_or_else(|| fail("no `blob` field"))?;
    let object = blob.as_object().ok_or_else(|| fail("`blob` is not an object"))?;

    match object.get("$type").and_then(Value::as_str) {
        Some("blob") => {}
        Some(other) => return Err(fail(&format!("`blob.$type` is {other:?}, expected \"blob\""))),
        None => return Err(fail("`blob` has no `$type` field")),
    }
    for field in ["ref", "mimeType", "size"] {
        if !object.contains_key(field) {
            return Err(fail(&format!("`blob` has no `{field}` field")));
        }
    }

    Ok(blob.clone())
}

/// The PDS surface `upstage publish` needs: session creation, blob upload and
/// record write. A trait rather than an injectable base URL plus a test HTTP
/// server, per `## Approach` in `spec.md`: the crate has no HTTP test server
/// dependency, and `HttpPdsClient` is the one real implementation;
/// `publish_with`'s tests use an in-memory fake that can assert `put_record`
/// was never called.
pub trait PdsClient {
    fn create_session(
        &self,
        handle: &str,
        app_password: &Secret,
    ) -> impl Future<Output = Result<Session, PublishError>> + Send;

    /// Returns the status and the decoded body, not the blob. The shape
    /// check is [`validate_blob`], which `publish_with` runs, so a fake can
    /// return a 2xx body with no `blob` and the test still exercises the
    /// real check (BC17).
    fn upload_blob(
        &self,
        access_jwt: &Secret,
        avatar: &Avatar,
    ) -> impl Future<Output = Result<UploadResponse, PublishError>> + Send;

    fn put_record(
        &self,
        access_jwt: &Secret,
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
        Self { base_url, http: http_client() }
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
        app_password: &Secret,
    ) -> Result<Session, PublishError> {
        let url = format!("{}/xrpc/com.atproto.server.createSession", self.base_url);
        let response = self
            .http
            .post(&url)
            .json(&json!({ "identifier": handle, "password": app_password.expose() }))
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
        let access_jwt = body
            .get("accessJwt")
            .and_then(Value::as_str)
            .map(|value| Secret::new(value.to_string()))
            .ok_or_else(|| PublishError::Auth {
                status: status.as_u16(),
                body: "createSession response carried no accessJwt".to_string(),
            })?;
        Ok(Session { did, access_jwt })
    }

    async fn upload_blob(
        &self,
        access_jwt: &Secret,
        avatar: &Avatar,
    ) -> Result<UploadResponse, PublishError> {
        let url = format!("{}/xrpc/com.atproto.repo.uploadBlob", self.base_url);
        let response = self
            .http
            .post(&url)
            .bearer_auth(access_jwt.expose())
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
        Ok(UploadResponse { status: status.as_u16(), body })
    }

    async fn put_record(&self, access_jwt: &Secret, body: &Value) -> Result<(), PublishError> {
        let url = format!("{}/xrpc/com.atproto.repo.putRecord", self.base_url);
        let response =
            self.http.post(&url).bearer_auth(access_jwt.expose()).json(body).send().await?;
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
        Some(avatar) => {
            let response = client.upload_blob(&session.access_jwt, avatar).await?;
            Some(validate_blob(&response)?)
        }
    };

    let body = record_body(cfg, &session.did, blob, Utc::now());
    client.put_record(&session.access_jwt, &body).await?;

    Ok(cfg.feed_uri())
}

/// `upstage publish`'s entry point. `preflight` runs first, so a missing
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
        vec![("UPSTAGE_HOSTNAME", "feed.example.com"), ("UPSTAGE_PUBLISHER_DID", "did:plc:abc")]
    }

    fn config_with(extra: &[(&str, &str)]) -> Config {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(extra);
        let map: std::collections::HashMap<String, String> =
            pairs.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        crate::config::load(move |name| map.get(name).cloned()).expect("minimal config loads")
    }

    fn creds() -> Credentials {
        Credentials {
            handle: "upstage.bsky.social".to_string(),
            app_password: Secret::new("app-pass".to_string()),
        }
    }

    /// A well-formed `uploadBlob` blob: every field [`validate_blob`] insists
    /// on.
    fn good_blob() -> Value {
        json!({
            "$type": "blob",
            "ref": {"$link": "bafyreiexample"},
            "mimeType": "image/png",
            "size": 1234,
        })
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
        let cfg = config_with(&[("BSKY_HANDLE", "upstage.bsky.social")]);
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
        let cfg = config_with(&[
            ("BSKY_HANDLE", "upstage.bsky.social"),
            ("BSKY_APP_PASSWORD", "app-pass"),
        ]);
        let path = std::env::temp_dir().join("upstage-publish-test-no-such-avatar.png");
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
        let cfg = config_with(&[
            ("BSKY_HANDLE", "upstage.bsky.social"),
            ("BSKY_APP_PASSWORD", "app-pass"),
        ]);
        let (_, avatar) = preflight(&cfg, None).expect("preflight succeeds with no avatar");
        assert_eq!(avatar, None);
    }

    #[test]
    fn unsupported_avatar_extension_fails() {
        // BC4.
        let cfg = config_with(&[
            ("BSKY_HANDLE", "upstage.bsky.social"),
            ("BSKY_APP_PASSWORD", "app-pass"),
        ]);
        let path = std::env::temp_dir().join("upstage-publish-test-avatar.gif");
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
        let cfg = config_with(&[
            ("BSKY_HANDLE", "upstage.bsky.social"),
            ("BSKY_APP_PASSWORD", "app-pass"),
        ]);
        for (ext, expected) in [
            ("png", "image/png"),
            ("PNG", "image/png"),
            ("jpg", "image/jpeg"),
            ("JPEG", "image/jpeg"),
        ] {
            let path = std::env::temp_dir().join(format!("upstage-publish-test-avatar.{ext}"));
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
        let cfg = config_with(&[("UPSTAGE_FEED_RKEY", "upstaged")]);
        let now = DateTime::parse_from_rfc3339("2026-09-21T00:00:00Z").unwrap().with_timezone(&Utc);
        let body = record_body(&cfg, "did:plc:abc", None, now);
        assert_eq!(body["repo"], "did:plc:abc");
        assert_eq!(body["collection"], "app.bsky.feed.generator");
        assert_eq!(body["rkey"], "upstaged");
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
        upload: Result<UploadResponse, PublishError>,
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
                    access_jwt: Secret::new("access-jwt".to_string()),
                }),
                upload: Ok(UploadResponse { status: 200, body: json!({ "blob": good_blob() }) }),
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
            _app_password: &Secret,
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
            _access_jwt: &Secret,
            _avatar: &Avatar,
        ) -> Result<UploadResponse, PublishError> {
            self.upload_blob_calls.fetch_add(1, AtomicOrdering::SeqCst);
            match &self.upload {
                Ok(value) => Ok(value.clone()),
                Err(PublishError::Upload { status, body }) => {
                    Err(PublishError::Upload { status: *status, body: body.clone() })
                }
                Err(other) => panic!("unexpected fixture error: {other:?}"),
            }
        }

        async fn put_record(&self, _access_jwt: &Secret, body: &Value) -> Result<(), PublishError> {
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
        let cfg = config_with(&[("UPSTAGE_FEED_RKEY", "upstaged")]);
        let client = FakePds::ok("did:plc:abc");
        let uri = publish_with(&client, &cfg, &creds(), None).await.expect("publish succeeds");
        assert_eq!(uri, "at://did:plc:abc/app.bsky.feed.generator/upstaged");
        assert_eq!(client.create_session_calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(client.upload_blob_calls.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(client.put_record_calls.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn session_did_mismatch_fails() {
        // AC5, BC7: the session's own did differs from UPSTAGE_PUBLISHER_DID;
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
        assert_eq!(body["record"]["avatar"], good_blob());
    }

    #[tokio::test]
    async fn upload_blob_without_a_blob_field_fails_before_put_record() {
        // BC17: a 2xx uploadBlob body that carries no `blob` never reaches a
        // record. The fake answers 200 with an unrelated object, so only
        // `validate_blob` can stop this.
        let cfg = config_with(&[]);
        let mut client = FakePds::ok("did:plc:abc");
        client.upload = Ok(UploadResponse { status: 200, body: json!({ "ok": true }) });
        let avatar = Avatar { bytes: vec![1, 2, 3], content_type: "image/png" };

        let err = publish_with(&client, &cfg, &creds(), Some(&avatar)).await.unwrap_err();

        match err {
            PublishError::Upload { status, body } => {
                assert_eq!(status, 200);
                assert!(body.contains("no `blob` field"), "message names the field: {body}");
            }
            other => panic!("expected Upload, got {other:?}"),
        }
        assert_eq!(client.upload_blob_calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(client.put_record_calls.load(AtomicOrdering::SeqCst), 0);
        assert!(client.last_put_body.lock().expect("lock").is_none());
    }

    #[test]
    fn validate_blob_names_every_field_it_rejects() {
        // BC17: each malformed shape, and the message that names its cause.
        let cases: Vec<(Value, &str)> = vec![
            (json!({}), "no `blob` field"),
            (json!({ "blob": "not-an-object" }), "`blob` is not an object"),
            (json!({ "blob": { "ref": {}, "mimeType": "image/png", "size": 1 } }), "no `$type`"),
            (
                json!({ "blob": { "$type": "other", "ref": {}, "mimeType": "image/png", "size": 1 } }),
                "expected \"blob\"",
            ),
            (
                json!({ "blob": { "$type": "blob", "mimeType": "image/png", "size": 1 } }),
                "no `ref` field",
            ),
            (json!({ "blob": { "$type": "blob", "ref": {}, "size": 1 } }), "no `mimeType` field"),
            (
                json!({ "blob": { "$type": "blob", "ref": {}, "mimeType": "image/png" } }),
                "no `size` field",
            ),
        ];
        for (body, expected) in cases {
            let response = UploadResponse { status: 201, body: body.clone() };
            match validate_blob(&response) {
                Err(PublishError::Upload { status, body: message }) => {
                    assert_eq!(status, 201);
                    assert!(
                        message.contains(expected),
                        "{body} should report {expected}: {message}"
                    );
                }
                other => panic!("expected Upload for {body}, got {other:?}"),
            }
        }
    }

    #[test]
    fn validate_blob_accepts_a_well_formed_blob() {
        // BC17: the happy path returns the blob itself, not the envelope.
        let response = UploadResponse { status: 200, body: json!({ "blob": good_blob() }) };
        assert_eq!(validate_blob(&response).expect("valid blob"), good_blob());
    }

    #[tokio::test]
    async fn second_run_overwrites_the_same_record_unconditionally() {
        // AC-adjacent, BC14: two calls to publish_with both call
        // put_record; nothing reads the record first.
        let cfg = config_with(&[("UPSTAGE_FEED_RKEY", "upstaged")]);
        let client = FakePds::ok("did:plc:abc");
        publish_with(&client, &cfg, &creds(), None).await.expect("first publish succeeds");
        publish_with(&client, &cfg, &creds(), None).await.expect("second publish succeeds");
        assert_eq!(client.put_record_calls.load(AtomicOrdering::SeqCst), 2);
    }

    #[test]
    fn debug_on_publish_error_never_contains_a_password_or_token() {
        // BC11, three ways. No PublishError variant has a field for either
        // secret, and the two structs that do hold one keep it in `Secret`,
        // whose Debug prints `[redacted]`. A future field added as a plain
        // String fails this test.
        let err = PublishError::Auth { status: 401, body: "invalid password".to_string() };
        let printed = format!("{err:?}");
        assert!(!printed.contains("app-pass"));
        assert!(!printed.contains("access-jwt"));

        let credentials = format!("{:?}", creds());
        assert!(!credentials.contains("app-pass"), "Credentials Debug leaked: {credentials}");
        assert!(credentials.contains("[redacted]"), "Credentials Debug: {credentials}");

        let session = format!(
            "{:?}",
            Session {
                did: "did:plc:abc".to_string(),
                access_jwt: Secret::new("access-jwt".to_string()),
            }
        );
        assert!(!session.contains("access-jwt"), "Session Debug leaked: {session}");
        assert!(session.contains("[redacted]"), "Session Debug: {session}");
    }

    #[test]
    fn whitespace_app_password_fails_fast_as_missing() {
        // BC18: `config::optional` is a bare lookup and does not trim, so a
        // password of three spaces arrives here as Some("   "). The filter
        // in `preflight` is the only thing that rejects it.
        let cfg =
            config_with(&[("BSKY_HANDLE", "upstage.bsky.social"), ("BSKY_APP_PASSWORD", "   ")]);
        assert_eq!(cfg.bsky_app_password.as_ref().map(Secret::expose), Some("   "));
        match preflight(&cfg, None).unwrap_err() {
            PublishError::MissingCredentials { var } => assert_eq!(var, "BSKY_APP_PASSWORD"),
            other => panic!("expected MissingCredentials, got {other:?}"),
        }
    }

    #[test]
    fn whitespace_handle_fails_fast_as_missing() {
        // BC18, the same guard on the other variable.
        let cfg = config_with(&[("BSKY_HANDLE", "  "), ("BSKY_APP_PASSWORD", "app-pass")]);
        match preflight(&cfg, None).unwrap_err() {
            PublishError::MissingCredentials { var } => assert_eq!(var, "BSKY_HANDLE"),
            other => panic!("expected MissingCredentials, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_avatar_is_an_avatar_read_error() {
        // BC19: the file exists, so `is_file` passes and the extension is
        // fine; only the read fails. The OS error is carried, not discarded
        // into AvatarNotFound.
        use std::os::unix::fs::PermissionsExt;

        let cfg = config_with(&[
            ("BSKY_HANDLE", "upstage.bsky.social"),
            ("BSKY_APP_PASSWORD", "app-pass"),
        ]);
        let path = std::env::temp_dir().join("upstage-publish-test-unreadable-avatar.png");
        let _ = std::fs::remove_file(&path);
        let mut file = std::fs::File::create(&path).expect("create the avatar");
        file.write_all(b"not really a png").expect("write the avatar");
        drop(file);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000))
            .expect("chmod 000 the avatar");

        // Root ignores the mode bits, so the read would succeed and there
        // would be nothing to assert. Skip rather than fail.
        if std::fs::read(&path).is_ok() {
            let _ = std::fs::remove_file(&path);
            return;
        }

        let err = preflight(&cfg, Some(&path)).unwrap_err();
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        let _ = std::fs::remove_file(&path);

        match err {
            PublishError::AvatarRead { path: got, source } => {
                assert_eq!(got, path);
                assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
            }
            other => panic!("expected AvatarRead, got {other:?}"),
        }
    }
}

/// Live test against a real Bluesky test account. `#[ignore]`d, so
/// `cargo test --all-features` never touches the network; run by hand with
/// `BSKY_HANDLE`, `BSKY_APP_PASSWORD`, `UPSTAGE_HOSTNAME` and
/// `UPSTAGE_PUBLISHER_DID` set: `cargo test -- --ignored publish_live` (AC7).
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
