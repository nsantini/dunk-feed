//! `upstage publish`, TECH-DESIGN section 11.2: writes the
//! `app.bsky.feed.generator` record. A preflight reads `BSKY_HANDLE`,
//! `BSKY_APP_PASSWORD` and an optional avatar file before any network call
//! (BC1, BC2, BC3, BC4, BC5), `record_body` builds the record as a pure
//! function of `Config` and an optional uploaded blob (BC12), and
//! `publish_with` runs the three calls through [`PdsClient`]:
//! `createSession` (read back as [`PdsClient::session_did`]), an optional
//! `uploadBlob`, then an unconditional `putRecord` (BC6 to BC10, BC13,
//! BC14). `PdsClient` is `appview::pds`'s shared session client
//! (`UPSTAGE_PDS_URL`, this story's Approach): the session, refresh and
//! retry code that used to live in this module's own trait and
//! `HttpPdsClient` moved there, so this binary has exactly one PDS login
//! path. `map_pds_error` turns a `PdsError` from one of those three calls
//! into the matching [`PublishError`] variant this module already exposed
//! (BC13a), so a caller of `run` sees no shape change. Never run from
//! `upstage run` (TECH-DESIGN section 11.2); a separate manual step.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use thiserror::Error;

use crate::appview::pds::{Credentials, PdsClient, PdsError, PdsTransport};
use crate::config::Config;

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
    Transport(String),
    #[error("could not build the PDS client: {0}")]
    Config(String),
}

/// Maps a [`PdsError`] from one of `publish_with`'s three `PdsClient` calls
/// to the matching [`PublishError`] variant (BC13a). A transport
/// failure — [`PdsError::Http`] with no status, meaning every retry hit a
/// connection error or a timeout rather than an HTTP response — is always
/// `PublishError::Transport`, regardless of which call raised it. Every
/// other failure becomes `on_http`'s variant, carrying the status (`0` for
/// [`PdsError::Session`], which carries none of its own) and the error's
/// own message: enough for the user to see one line naming the call and
/// the status, without a `PdsError` variant for every `PublishError` one.
fn map_pds_error(err: PdsError, on_http: impl FnOnce(u16, String) -> PublishError) -> PublishError {
    if let PdsError::Http { status: None, .. } = &err {
        return PublishError::Transport(err.to_string());
    }
    let status = match &err {
        PdsError::Http { status: Some(status), .. } => *status,
        _ => 0,
    };
    on_http(status, err.to_string())
}

/// An avatar file read off disk during preflight, with the `Content-Type`
/// its extension maps to (BC5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Avatar {
    pub bytes: Vec<u8>,
    pub content_type: &'static str,
}

/// Reads `BSKY_HANDLE` and `BSKY_APP_PASSWORD` off `cfg` through
/// [`Config::bsky_credentials`], then `avatar_path` off disk, before any
/// `reqwest::Client` is built. `BSKY_HANDLE` or `BSKY_APP_PASSWORD` missing,
/// or empty once trimmed, fails first (`MissingCredentials`, BC1): the trim
/// rule itself lives in `Config::bsky_credentials`, shared with
/// `ingest::run` (story 11 Approach), so `BSKY_APP_PASSWORD="   "` is
/// rejected the same way here as there (BC18). A given `--avatar` path that
/// does not exist fails with `AvatarNotFound` (BC2); an extension other
/// than `png`, `jpg` or `jpeg`, compared lowercased, fails with
/// `AvatarType` (BC4); a file that exists but cannot be read fails with
/// `AvatarRead`, carrying the OS error (BC19). No `--avatar` at all returns
/// `Ok((credentials, None))` and reads no file (BC3).
pub fn preflight(
    cfg: &Config,
    avatar_path: Option<&Path>,
) -> Result<(Credentials, Option<Avatar>), PublishError> {
    let credentials =
        cfg.bsky_credentials().map_err(|var| PublishError::MissingCredentials { var })?;

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

/// The publish sequence, generic over [`PdsTransport`] so tests can pass a
/// fake (BC7, BC13, BC14): `createSession` (`PdsClient::session_did`), then
/// a DID check against `cfg.publisher_did` (BC7, this catches publishing
/// from the wrong account), then an optional `uploadBlob` when `avatar` is
/// `Some` (BC3), then an unconditional `putRecord` (BC14: nothing reads the
/// record first, so a second run overwrites the same one). `client` already
/// carries the credentials `preflight` read, so this takes no separate
/// `Credentials` argument. Returns `cfg.feed_uri()` on success (BC13).
pub async fn publish_with<T: PdsTransport>(
    client: &PdsClient<T>,
    cfg: &Config,
    avatar: Option<&Avatar>,
) -> Result<String, PublishError> {
    let session_did = client
        .session_did()
        .await
        .map_err(|err| map_pds_error(err, |status, body| PublishError::Auth { status, body }))?;
    if session_did != cfg.publisher_did {
        return Err(PublishError::DidMismatch {
            session_did,
            configured_did: cfg.publisher_did.clone(),
        });
    }

    let blob = match avatar {
        None => None,
        Some(avatar) => {
            let (status, body) =
                client.upload_blob(avatar.bytes.clone(), avatar.content_type).await.map_err(
                    |err| map_pds_error(err, |status, body| PublishError::Upload { status, body }),
                )?;
            Some(validate_blob(&UploadResponse { status, body })?)
        }
    };

    let body = record_body(cfg, &session_did, blob, Utc::now());
    client.put_record(body).await.map_err(|err| {
        map_pds_error(err, |status, body| PublishError::PutRecord { status, body })
    })?;

    Ok(cfg.feed_uri())
}

/// `upstage publish`'s entry point. `preflight` runs first, so a missing
/// credential or a missing/unsupported avatar file stops the run before a
/// [`PdsClient`] is even built (BC1, BC2) and before any network is
/// reachable. `PdsClient::from_config` only fails on a `graph_rps` outside
/// the range `config::graph_rps_or_default` already enforces at config load
/// (BC14), so this cannot fail for a `Config` `config::load` built; it is
/// mapped to `PublishError::Config` rather than `.expect()` (review round 2,
/// defect D) for a `Config` built some other way.
pub async fn run(cfg: &Config, avatar_path: Option<&Path>) -> Result<String, PublishError> {
    let (credentials, avatar) = preflight(cfg, avatar_path)?;
    let client = PdsClient::from_config(cfg, credentials)
        .map_err(|err| PublishError::Config(err.to_string()))?;
    publish_with(&client, cfg, avatar.as_ref()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex};

    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;

    use crate::appview::pds::{HttpMethod, RawResponse, RequestBody, TransportError};
    use crate::config::Secret;

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
        // inside `run` too: the check returns before `PdsClient::from_config`
        // is built and before any network is reachable. A `run` that built
        // its client first would still succeed at that step (`PdsClient`
        // does not log in until its first call), then hang for the 10 s
        // timeout on the first real network call this test never wants to
        // make.
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

    // --- publish_with, via a fake PdsTransport ------------------------

    /// A `com.atproto.server.createSession` 2xx body carrying a
    /// real-shaped, unsigned access token, the same shape
    /// `appview::pds::tests` builds: `PdsClient::ensure_fresh_session`
    /// decodes `exp` out of it, so a token without one reads as already
    /// expired.
    fn session_body(did: &str) -> Value {
        let header = URL_SAFE_NO_PAD.encode(b"{}");
        let exp = Utc::now().timestamp() + 3600;
        let payload = URL_SAFE_NO_PAD.encode(format!("{{\"exp\":{exp}}}"));
        json!({
            "did": did,
            "accessJwt": format!("{header}.{payload}.sig"),
            "refreshJwt": "refresh-jwt",
        })
    }

    /// An in-memory fake [`PdsTransport`]: one canned status and body per
    /// nsid, and a call counter for each (BC6, BC7, BC8), so a test can
    /// assert a later call in the sequence never ran. Unlike
    /// `appview::pds::tests::FakeTransport`'s response queue, each nsid
    /// always answers the same fixture: `publish_with`'s three calls are
    /// each made at most once per `publish_with`, so no test here needs a
    /// sequence. `Clone`-shareable over `Arc`, the same pattern
    /// `appview::pds::tests::FakeTransport` uses, so a test keeps a handle
    /// to inspect after the original is moved into a [`PdsClient`].
    #[derive(Clone)]
    struct FakePds {
        session: Arc<Mutex<(u16, Value)>>,
        upload: Arc<Mutex<(u16, Value)>>,
        put: Arc<Mutex<(u16, Value)>>,
        create_session_calls: Arc<AtomicUsize>,
        upload_blob_calls: Arc<AtomicUsize>,
        put_record_calls: Arc<AtomicUsize>,
        last_put_body: Arc<Mutex<Option<Value>>>,
        /// When `Some(nsid)`, every call to that nsid answers with
        /// [`TransportError`] instead of a status, for
        /// `transport_failure_maps_to_publish_transport` (BC13a).
        transport_error_nsid: Arc<Mutex<Option<&'static str>>>,
    }

    impl FakePds {
        fn ok(session_did: &str) -> Self {
            Self {
                session: Arc::new(Mutex::new((200, session_body(session_did)))),
                upload: Arc::new(Mutex::new((200, json!({ "blob": good_blob() })))),
                put: Arc::new(Mutex::new((200, json!({})))),
                create_session_calls: Arc::new(AtomicUsize::new(0)),
                upload_blob_calls: Arc::new(AtomicUsize::new(0)),
                put_record_calls: Arc::new(AtomicUsize::new(0)),
                last_put_body: Arc::new(Mutex::new(None)),
                transport_error_nsid: Arc::new(Mutex::new(None)),
            }
        }

        fn set_session(&self, status: u16, body: Value) {
            *self.session.lock().expect("lock") = (status, body);
        }

        fn set_upload(&self, status: u16, body: Value) {
            *self.upload.lock().expect("lock") = (status, body);
        }

        fn set_put(&self, status: u16, body: Value) {
            *self.put.lock().expect("lock") = (status, body);
        }

        fn fail_transport(&self, nsid: &'static str) {
            *self.transport_error_nsid.lock().expect("lock") = Some(nsid);
        }

        fn calls(&self, counter: &AtomicUsize) -> usize {
            counter.load(AtomicOrdering::SeqCst)
        }
    }

    impl PdsTransport for FakePds {
        async fn request(
            &self,
            _http_method: HttpMethod,
            nsid: &'static str,
            _bearer: Option<&str>,
            _proxy: bool,
            _query: &[(&str, &str)],
            body: RequestBody,
        ) -> Result<RawResponse, TransportError> {
            if *self.transport_error_nsid.lock().expect("lock") == Some(nsid) {
                return Err(TransportError);
            }
            match nsid {
                "com.atproto.server.createSession" => {
                    self.create_session_calls.fetch_add(1, AtomicOrdering::SeqCst);
                    let (status, body) = self.session.lock().expect("lock").clone();
                    Ok(RawResponse { status, body })
                }
                "com.atproto.repo.uploadBlob" => {
                    self.upload_blob_calls.fetch_add(1, AtomicOrdering::SeqCst);
                    let (status, body) = self.upload.lock().expect("lock").clone();
                    Ok(RawResponse { status, body })
                }
                "com.atproto.repo.putRecord" => {
                    self.put_record_calls.fetch_add(1, AtomicOrdering::SeqCst);
                    if let RequestBody::Json(value) = body {
                        *self.last_put_body.lock().expect("lock") = Some(value);
                    }
                    let (status, body) = self.put.lock().expect("lock").clone();
                    Ok(RawResponse { status, body })
                }
                other => panic!("unexpected nsid in publish test: {other}"),
            }
        }
    }

    /// Builds a [`PdsClient`] over a clone of `fake`, at a rate fast enough
    /// that the limiter never slows a test down; `fake` itself stays
    /// usable for assertions after the client is built.
    fn client_over(fake: &FakePds) -> PdsClient<FakePds> {
        PdsClient::new(fake.clone(), creds(), 1000.0).expect("valid rate builds a client")
    }

    #[tokio::test]
    async fn prints_at_uri() {
        // AC4, BC13: publish_with returns the at-URI `dispatch` prints.
        let cfg = config_with(&[("UPSTAGE_FEED_RKEY", "upstaged")]);
        let fake = FakePds::ok("did:plc:abc");
        let client = client_over(&fake);
        let uri = publish_with(&client, &cfg, None).await.expect("publish succeeds");
        assert_eq!(uri, "at://did:plc:abc/app.bsky.feed.generator/upstaged");
        assert_eq!(fake.calls(&fake.create_session_calls), 1);
        assert_eq!(fake.calls(&fake.upload_blob_calls), 0);
        assert_eq!(fake.calls(&fake.put_record_calls), 1);
    }

    #[tokio::test]
    async fn session_did_mismatch_fails() {
        // AC5, BC7: the session's own did differs from UPSTAGE_PUBLISHER_DID;
        // put_record never runs.
        let cfg = config_with(&[]);
        let fake = FakePds::ok("did:plc:someone-else");
        let client = client_over(&fake);
        let err = publish_with(&client, &cfg, None).await.unwrap_err();
        match err {
            PublishError::DidMismatch { session_did, configured_did } => {
                assert_eq!(session_did, "did:plc:someone-else");
                assert_eq!(configured_did, "did:plc:abc");
            }
            other => panic!("expected DidMismatch, got {other:?}"),
        }
        assert_eq!(fake.calls(&fake.put_record_calls), 0);
    }

    #[tokio::test]
    async fn create_session_failure_stops_before_upload_or_put_record() {
        // BC6. 401 is a non-retryable 4xx (appview::retry_decision), so this
        // fails on the first attempt.
        let fake = FakePds::ok("did:plc:abc");
        fake.set_session(401, json!({ "error": "bad password" }));
        let cfg = config_with(&[]);
        let client = client_over(&fake);
        let err = publish_with(&client, &cfg, None).await.unwrap_err();
        match err {
            PublishError::Auth { status, .. } => assert_eq!(status, 401),
            other => panic!("expected Auth, got {other:?}"),
        }
        assert_eq!(fake.calls(&fake.upload_blob_calls), 0);
        assert_eq!(fake.calls(&fake.put_record_calls), 0);
    }

    #[tokio::test]
    async fn transport_failure_maps_to_publish_transport() {
        // BC13a: a transport-level failure (every retry hit a connection
        // error, never an HTTP response) is `PublishError::Transport`, not
        // `Auth`, even though it happened on the session call. Costs the
        // same 1s+2s+4s retry schedule as the 5xx tests: `None` is always
        // retryable (appview::retry_decision).
        let fake = FakePds::ok("did:plc:abc");
        fake.fail_transport("com.atproto.server.createSession");
        let cfg = config_with(&[]);
        let client = client_over(&fake);
        let err = publish_with(&client, &cfg, None).await.unwrap_err();
        assert!(matches!(err, PublishError::Transport(_)), "expected Transport, got {err:?}");
    }

    #[tokio::test]
    async fn upload_failure_stops_before_put_record() {
        // BC8. 500 retries three times (1s, 2s, 4s) before failing, the
        // same real-wall-time cost `appview::pds::tests` already pays for
        // its own retry-exhaustion test.
        let fake = FakePds::ok("did:plc:abc");
        fake.set_upload(500, json!({ "error": "server error" }));
        let cfg = config_with(&[]);
        let client = client_over(&fake);
        let avatar = Avatar { bytes: vec![1, 2, 3], content_type: "image/png" };
        let err = publish_with(&client, &cfg, Some(&avatar)).await.unwrap_err();
        match err {
            PublishError::Upload { status, .. } => assert_eq!(status, 500),
            other => panic!("expected Upload, got {other:?}"),
        }
        assert_eq!(fake.calls(&fake.put_record_calls), 0);
    }

    #[tokio::test]
    async fn put_record_failure_surfaces() {
        // BC9. 400 is a non-retryable 4xx.
        let fake = FakePds::ok("did:plc:abc");
        fake.set_put(400, json!({ "error": "bad record" }));
        let cfg = config_with(&[]);
        let client = client_over(&fake);
        let err = publish_with(&client, &cfg, None).await.unwrap_err();
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
        let fake = FakePds::ok("did:plc:abc");
        let client = client_over(&fake);
        let avatar = Avatar { bytes: vec![1, 2, 3], content_type: "image/png" };
        publish_with(&client, &cfg, Some(&avatar)).await.expect("publish succeeds");
        assert_eq!(fake.calls(&fake.upload_blob_calls), 1);
        let body = fake.last_put_body.lock().expect("lock").clone().expect("body recorded");
        assert_eq!(body["record"]["avatar"], good_blob());
    }

    #[tokio::test]
    async fn upload_blob_without_a_blob_field_fails_before_put_record() {
        // BC17: a 2xx uploadBlob body that carries no `blob` never reaches a
        // record. The fake answers 200 with an unrelated object, so only
        // `validate_blob` can stop this.
        let fake = FakePds::ok("did:plc:abc");
        fake.set_upload(200, json!({ "ok": true }));
        let cfg = config_with(&[]);
        let client = client_over(&fake);
        let avatar = Avatar { bytes: vec![1, 2, 3], content_type: "image/png" };

        let err = publish_with(&client, &cfg, Some(&avatar)).await.unwrap_err();

        match err {
            PublishError::Upload { status, body } => {
                assert_eq!(status, 200);
                assert!(body.contains("no `blob` field"), "message names the field: {body}");
            }
            other => panic!("expected Upload, got {other:?}"),
        }
        assert_eq!(fake.calls(&fake.upload_blob_calls), 1);
        assert_eq!(fake.calls(&fake.put_record_calls), 0);
        assert!(fake.last_put_body.lock().expect("lock").is_none());
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
        let fake = FakePds::ok("did:plc:abc");
        let client = client_over(&fake);
        publish_with(&client, &cfg, None).await.expect("first publish succeeds");
        publish_with(&client, &cfg, None).await.expect("second publish succeeds");
        assert_eq!(fake.calls(&fake.put_record_calls), 2);
    }

    #[test]
    fn debug_on_publish_error_never_contains_a_password_or_token() {
        // BC11: no `PublishError` variant has a field for either secret.
        // `Credentials`'s own `Debug` redaction of `app_password` is
        // covered directly by `appview::pds::tests::debug_redacts_secrets`,
        // since `Credentials` now lives there; this test only needs to
        // cover the variants this module adds.
        let err = PublishError::Auth { status: 401, body: "invalid password".to_string() };
        let printed = format!("{err:?}");
        assert!(!printed.contains("app-pass"));
        assert!(!printed.contains("access-jwt"));

        let credentials = format!("{:?}", creds());
        assert!(!credentials.contains("app-pass"), "Credentials Debug leaked: {credentials}");
        assert!(credentials.contains("[redacted]"), "Credentials Debug: {credentials}");
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
