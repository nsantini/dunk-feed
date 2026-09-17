//! Serde types for the Jetstream v2 envelope, TECH-DESIGN section 5.1 steps
//! 3 and 4, plus the `Event` a caller of `client::JetstreamClient::next`
//! sees. Decoding a `Frame` never fails on content this module does not
//! recognise (BC1, BC5): a new envelope kind or payload kind from a future
//! Jetstream release is ignored, not an error. Only a genuinely malformed
//! shape, such as an unknown `operation` string (BC8), is a decode error,
//! and that error is `client.rs`'s to catch.
//!
//! The envelope holds exactly `$type` and `payload`: `seq`, `did`, `rkey`
//! and `rev` live inside `payload`, verified against a live capture, not on
//! the envelope as an earlier draft of this module assumed. `payload.$type`
//! is the full name Jetstream sends, `network.bsky.jetstream.subscribeEvents#commit`
//! and its siblings, not the short `#commit`.

use chrono::{DateTime, FixedOffset};
use serde::Deserialize;

/// The outer envelope of one decoded frame, `{"$type":"message","payload":{...}}`.
/// A `kind` other than `"message"` still decodes (BC1): `client.rs` reads
/// the next frame rather than treating it as an error.
#[derive(Debug, Deserialize, PartialEq)]
pub struct Frame {
    #[serde(rename = "$type")]
    pub kind: String,
    pub payload: Payload,
}

/// `payload.$type`, internally tagged with the full name Jetstream sends.
/// `Commit` and `Info` carry the data `client.rs` returns to the caller;
/// `Identity`, `Account` and `Sync` are consumed internally (BC4); `Other`
/// catches every payload kind this module does not name, including a
/// future one (BC5).
#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "$type")]
pub enum Payload {
    #[serde(rename = "network.bsky.jetstream.subscribeEvents#commit")]
    Commit(CommitEvent),
    #[serde(rename = "network.bsky.jetstream.subscribeEvents#identity")]
    Identity {},
    #[serde(rename = "network.bsky.jetstream.subscribeEvents#account")]
    Account {},
    #[serde(rename = "network.bsky.jetstream.subscribeEvents#sync")]
    Sync {},
    #[serde(rename = "network.bsky.jetstream.subscribeEvents#info")]
    Info { name: String, message: String },
    #[serde(other)]
    Other,
}

/// `payload.operation`. A finite set: an unrecognised string is a decode
/// error (BC8), unlike an unrecognised `payload.$type` (BC5), because a new
/// operation kind changes what the writer must do and is not safe to
/// ignore.
#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Create,
    Update,
    Delete,
}

/// A decoded `#commit` payload, all nine fields TECH-DESIGN section 5.1
/// step 4 lists. `record` and `cid` are `None` on a delete, which carries
/// neither (BC6). `time` is kept verbatim; `time_micros` and `time_secs`
/// parse it lazily so an unparseable value (BC11) never fails decoding
/// itself. Story 06 builds `at://{did}/{collection}/{rkey}` from `did`,
/// `collection` and `rkey`, so none of the nine may be dropped.
#[derive(Debug, Deserialize, PartialEq)]
pub struct CommitEvent {
    pub did: String,
    pub seq: u64,
    pub time: String,
    pub operation: Operation,
    pub collection: String,
    pub rkey: String,
    pub rev: String,
    pub cid: Option<String>,
    pub record: Option<serde_json::Value>,
}

impl CommitEvent {
    /// Parses `time` as RFC3339. `None` when it is not parseable (BC11).
    fn parsed_time(&self) -> Option<DateTime<FixedOffset>> {
        DateTime::parse_from_rfc3339(&self.time).ok()
    }

    /// Unix microseconds, keeping the microsecond precision Jetstream sends
    /// (BC10). `None` when `time` did not parse (BC11).
    pub fn time_micros(&self) -> Option<i64> {
        self.parsed_time().map(|dt| dt.timestamp_micros())
    }

    /// Unix seconds. `None` when `time` did not parse (BC11).
    pub fn time_secs(&self) -> Option<i64> {
        self.parsed_time().map(|dt| dt.timestamp())
    }
}

/// What `client.rs`'s `next()` returns to the caller. `Identity`, `Account`,
/// `Sync` and `Other` payloads, and a non-`"message"` envelope, are consumed
/// inside the loop and never reach this type (BC1, BC4, BC5).
#[derive(Debug, PartialEq)]
pub enum Event {
    Commit(CommitEvent),
    Info { name: String, message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR")))
            .unwrap_or_else(|err| panic!("reading fixture {name}: {err}"))
    }

    fn decode(name: &str) -> Frame {
        let raw = fixture(name);
        serde_json::from_str(&raw).unwrap_or_else(|err| panic!("decoding fixture {name}: {err}"))
    }

    #[test]
    fn commit_post_decodes_as_create() {
        let frame = decode("jetstream_commit_post.json");
        match frame.payload {
            Payload::Commit(commit) => {
                assert_eq!(commit.operation, Operation::Create);
                assert_eq!(commit.collection, "app.bsky.feed.post");
                assert!(!commit.did.is_empty());
                assert!(!commit.rkey.is_empty());
                assert!(!commit.rev.is_empty());
                assert!(commit.seq > 0);
                assert!(commit.record.is_some());
                assert!(commit.cid.is_some());
            }
            other => panic!("expected Payload::Commit, got {other:?}"),
        }
    }

    #[test]
    fn commit_like_decodes_as_create() {
        let frame = decode("jetstream_commit_like.json");
        match frame.payload {
            Payload::Commit(commit) => {
                assert_eq!(commit.operation, Operation::Create);
                assert_eq!(commit.collection, "app.bsky.feed.like");
            }
            other => panic!("expected Payload::Commit, got {other:?}"),
        }
    }

    #[test]
    fn commit_repost_decodes_as_create() {
        let frame = decode("jetstream_commit_repost.json");
        match frame.payload {
            Payload::Commit(commit) => {
                assert_eq!(commit.operation, Operation::Create);
                assert_eq!(commit.collection, "app.bsky.feed.repost");
            }
            other => panic!("expected Payload::Commit, got {other:?}"),
        }
    }

    #[test]
    fn commit_postgate_decodes_as_create() {
        let frame = decode("jetstream_commit_postgate.json");
        match frame.payload {
            Payload::Commit(commit) => {
                assert_eq!(commit.operation, Operation::Create);
                assert_eq!(commit.collection, "app.bsky.feed.postgate");
            }
            other => panic!("expected Payload::Commit, got {other:?}"),
        }
    }

    #[test]
    fn commit_delete_has_no_record_and_no_cid() {
        // BC6: a delete carries neither `record` nor `cid`.
        let frame = decode("jetstream_commit_delete.json");
        match frame.payload {
            Payload::Commit(commit) => {
                assert_eq!(commit.operation, Operation::Delete);
                assert_eq!(commit.record, None);
                assert_eq!(commit.cid, None);
                assert!(!commit.rkey.is_empty());
            }
            other => panic!("expected Payload::Commit, got {other:?}"),
        }
    }

    #[test]
    fn info_outdated_cursor_decodes() {
        let frame = decode("jetstream_info_outdated_cursor.json");
        match frame.payload {
            Payload::Info { name, .. } => assert_eq!(name, "OutdatedCursor"),
            other => panic!("expected Payload::Info, got {other:?}"),
        }
    }

    #[test]
    fn unknown_payload_type_becomes_other() {
        // BC5: an unknown `payload.$type` decodes to `Payload::Other`,
        // never a serde error.
        let raw = r##"{"$type":"message","payload":{"$type":"#futurething","foo":"bar"}}"##;
        let frame: Frame = serde_json::from_str(raw).unwrap();
        assert_eq!(frame.payload, Payload::Other);
    }

    #[test]
    fn identity_account_sync_decode_to_their_own_variants() {
        // BC4: these are consumed internally by `client.rs`, but they must
        // still decode here rather than fall through to `Other`.
        for (kind, expected) in [
            ("network.bsky.jetstream.subscribeEvents#identity", Payload::Identity {}),
            ("network.bsky.jetstream.subscribeEvents#account", Payload::Account {}),
            ("network.bsky.jetstream.subscribeEvents#sync", Payload::Sync {}),
        ] {
            let raw = format!(
                r##"{{"$type":"message","payload":{{"$type":"{kind}","did":"did:plc:abc"}}}}"##
            );
            let frame: Frame = serde_json::from_str(&raw).unwrap();
            assert_eq!(frame.payload, expected);
        }
    }

    #[test]
    fn non_message_envelope_still_decodes() {
        // BC1: an envelope `$type` other than `"message"` is not an error;
        // `client.rs` reads the next frame instead.
        let raw = r##"{"$type":"ping","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","did":"did:plc:abc"}}"##;
        let frame: Frame = serde_json::from_str(raw).unwrap();
        assert_eq!(frame.kind, "ping");
    }

    #[test]
    fn update_operation_decodes_to_its_variant() {
        // BC7: `create`, `update` and `delete` each decode to the matching
        // `Operation` variant. The fixtures cover `create` and `delete`;
        // this covers `update`.
        let raw = r##"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","did":"did:plc:abc","seq":1,"time":"2026-09-18T00:00:00.000000Z","operation":"update","collection":"app.bsky.feed.post","rkey":"abc123","rev":"rev123"}}"##;
        let frame: Frame = serde_json::from_str(raw).unwrap();
        match frame.payload {
            Payload::Commit(commit) => assert_eq!(commit.operation, Operation::Update),
            other => panic!("expected Payload::Commit, got {other:?}"),
        }
    }

    #[test]
    fn unknown_operation_is_a_decode_error() {
        // BC8: unlike an unknown payload `$type`, an unknown `operation` is
        // a finite-set violation, not forward compatibility.
        let raw = r##"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","did":"did:plc:abc","seq":1,"time":"2026-09-18T00:00:00.000000Z","operation":"upsert","collection":"app.bsky.feed.post","rkey":"abc123","rev":"rev123"}}"##;
        let result: Result<Frame, _> = serde_json::from_str(raw);
        assert!(result.is_err());
    }

    #[test]
    fn parseable_time_returns_micros_and_secs() {
        // BC10: kept verbatim, and both helpers parse it.
        let frame = decode("jetstream_commit_post.json");
        let Payload::Commit(commit) = frame.payload else { panic!("expected Payload::Commit") };
        assert!(commit.time_micros().is_some());
        assert!(commit.time_secs().is_some());
        assert_eq!(commit.time_secs(), commit.time_micros().map(|us| us.div_euclid(1_000_000)));
    }

    #[test]
    fn unparseable_time_returns_none() {
        // BC11: decoding still succeeds; only the helpers return `None`.
        let commit = CommitEvent {
            did: "did:plc:abc".to_string(),
            seq: 1,
            time: "not-a-timestamp".to_string(),
            operation: Operation::Create,
            collection: "app.bsky.feed.post".to_string(),
            rkey: "abc123".to_string(),
            rev: "rev123".to_string(),
            cid: None,
            record: None,
        };
        assert_eq!(commit.time_micros(), None);
        assert_eq!(commit.time_secs(), None);
    }
}
