//! Pure quote detection, TECH-DESIGN section 5.3. `detect` never touches the
//! network or the store; it only reads the `serde_json::Value` the ingest
//! path already holds for a `post` create. `validate` (story 03) and the
//! Jetstream consumer (story 06) call this exact function.

/// A validated `at://` URI: `at://<did>/app.bsky.feed.post/<rkey>`. The only
/// place a URI is validated in this story; the App View client (`src/appview`)
/// passes URIs through as `&str` instead.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
pub struct AtUri(String);

impl AtUri {
    /// Parses `at://<did>/app.bsky.feed.post/<rkey>`. `None` when the scheme
    /// is wrong, the authority is not a `did:`, the collection is not
    /// `app.bsky.feed.post`, or the rkey is empty. A quote of a list or a
    /// feed generator is not a dunk pair.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    pub fn parse(uri: &str) -> Option<Self> {
        let rest = uri.strip_prefix("at://")?;
        let mut parts = rest.splitn(3, '/');
        let authority = parts.next()?;
        let collection = parts.next()?;
        let rkey = parts.next()?;

        if !authority.starts_with("did:") {
            return None;
        }
        if collection != "app.bsky.feed.post" {
            return None;
        }
        if rkey.is_empty() {
            return None;
        }

        Some(AtUri(uri.to_string()))
    }

    /// The full `at://` string.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The authority segment, always a `did:`. Parsing succeeded, so this
    /// never fails.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    pub fn did(&self) -> &str {
        self.0
            .strip_prefix("at://")
            .and_then(|rest| rest.split('/').next())
            .expect("AtUri only holds a string that already parsed")
    }

    /// The rkey segment, the last path component. Parsing succeeded, so this
    /// never fails.
    #[allow(dead_code)] // First caller is `dunk validate`, story 03.
    pub fn rkey(&self) -> &str {
        self.0
            .strip_prefix("at://")
            .and_then(|rest| rest.splitn(3, '/').nth(2))
            .expect("AtUri only holds a string that already parsed")
    }
}

/// Whether a post record embeds a quote of another post, TECH-DESIGN
/// section 5.3.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
pub enum Embed {
    Quote { original_uri: AtUri },
    NotAQuote,
}

/// Reads `record.embed.$type` and, for the two quote shapes, the embedded
/// post's `uri`. Every other `$type`, including `images`, `video`,
/// `external` and `gallery`, and a missing `embed` or `$type`, is
/// `NotAQuote`. An unknown `$type` is not an error. A URI that is absent,
/// empty, not a string, or fails `AtUri::parse` is also `NotAQuote`.
#[allow(dead_code)] // First caller is `dunk validate`, story 03.
pub fn detect(record: &serde_json::Value) -> Embed {
    let Some(embed) = record.get("embed") else {
        return Embed::NotAQuote;
    };
    let Some(kind) = embed.get("$type").and_then(|v| v.as_str()) else {
        return Embed::NotAQuote;
    };

    let uri = match kind {
        "app.bsky.embed.record" => embed.get("record").and_then(|r| r.get("uri")),
        "app.bsky.embed.recordWithMedia" => {
            embed.get("record").and_then(|r| r.get("record")).and_then(|r| r.get("uri"))
        }
        _ => return Embed::NotAQuote,
    };

    let Some(uri) = uri.and_then(|v| v.as_str()) else {
        return Embed::NotAQuote;
    };

    match AtUri::parse(uri) {
        Some(original_uri) => Embed::Quote { original_uri },
        None => Embed::NotAQuote,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn record_embed_is_a_quote() {
        let record = json!({
            "embed": {
                "$type": "app.bsky.embed.record",
                "record": { "uri": "at://did:plc:abc/app.bsky.feed.post/xyz" }
            }
        });
        assert_eq!(
            detect(&record),
            Embed::Quote {
                original_uri: AtUri::parse("at://did:plc:abc/app.bsky.feed.post/xyz").unwrap()
            }
        );
    }

    #[test]
    fn record_with_media_embed_is_a_quote() {
        let record = json!({
            "embed": {
                "$type": "app.bsky.embed.recordWithMedia",
                "record": {
                    "record": { "uri": "at://did:plc:abc/app.bsky.feed.post/xyz" }
                }
            }
        });
        assert_eq!(
            detect(&record),
            Embed::Quote {
                original_uri: AtUri::parse("at://did:plc:abc/app.bsky.feed.post/xyz").unwrap()
            }
        );
    }

    #[test]
    fn images_embed_is_not_a_quote() {
        let record = json!({ "embed": { "$type": "app.bsky.embed.images" } });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn video_embed_is_not_a_quote() {
        let record = json!({ "embed": { "$type": "app.bsky.embed.video" } });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn external_embed_is_not_a_quote() {
        let record = json!({ "embed": { "$type": "app.bsky.embed.external" } });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn unknown_type_is_not_a_quote_and_not_an_error() {
        let record = json!({ "embed": { "$type": "app.bsky.embed.gallery" } });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn missing_embed_is_not_a_quote() {
        let record = json!({ "text": "no embed here" });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn missing_type_is_not_a_quote() {
        let record =
            json!({ "embed": { "record": { "uri": "at://did:plc:abc/app.bsky.feed.post/xyz" } } });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn missing_uri_is_not_a_quote() {
        let record = json!({
            "embed": { "$type": "app.bsky.embed.record", "record": {} }
        });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn malformed_uri_is_not_a_quote() {
        let record = json!({
            "embed": {
                "$type": "app.bsky.embed.record",
                "record": { "uri": "not-a-uri" }
            }
        });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn non_post_collection_is_not_a_quote() {
        let record = json!({
            "embed": {
                "$type": "app.bsky.embed.record",
                "record": { "uri": "at://did:plc:abc/app.bsky.graph.list/xyz" }
            }
        });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn handle_authority_is_not_a_quote() {
        // TECH-DESIGN section 5.3 writes at://<did>/..., so a handle-form
        // authority is not a valid pair.
        let uri = "at://alice.bsky.social/app.bsky.feed.post/xyz";
        assert_eq!(AtUri::parse(uri), None);
        let record = json!({
            "embed": {
                "$type": "app.bsky.embed.record",
                "record": { "uri": uri }
            }
        });
        assert_eq!(detect(&record), Embed::NotAQuote);
    }

    #[test]
    fn at_uri_accessors() {
        let uri = AtUri::parse("at://did:plc:abc/app.bsky.feed.post/xyz").unwrap();
        assert_eq!(uri.as_str(), "at://did:plc:abc/app.bsky.feed.post/xyz");
        assert_eq!(uri.did(), "did:plc:abc");
        assert_eq!(uri.rkey(), "xyz");
    }
}
