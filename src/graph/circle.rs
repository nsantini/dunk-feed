//! `Circle`, TECH-DESIGN-network-feed §6.1. Story 06 adds `state`,
//! `last_request_at` and `d1_refreshed_at` to the plain follows/sample data
//! the probe (story 03) needed alone: the worker (`graph/queue.rs`) owns a
//! `Circle` past its first build, and the handler's cache and cursor read
//! its `circle_version` (`spec.md` `## Approach`).

use std::collections::HashSet;

use super::{CircleState, DidHash};

/// One viewer's first-build graph. `follows`, `follows_me` and `checked`
/// hold hashes, never DID strings (BC16); `d2_sample` holds the DID
/// strings themselves, because [`crate::graph::build::step_degree2`] pages
/// `getFollows` for each one and a hash cannot be turned back into an
/// `actor` parameter.
#[derive(Debug, Clone, Default)]
pub struct Circle {
    pub follows: HashSet<DidHash>,
    /// A hash from `checked` that step 2 (`build::step_follows_me`) found
    /// following the viewer back (BC10: always a subset of `checked`).
    pub follows_me: HashSet<DidHash>,
    /// Every hash step 2 has sent to `getRelationships` for this viewer so
    /// far, whether or not it follows back (BC1a's "not in `follows` or
    /// `checked`" skip rule, BC4a's "only DIDs not in `checked`" resume
    /// rule).
    pub checked: HashSet<DidHash>,
    pub d2_sample: Vec<String>,
    /// `building_d1` while step 1 is in flight or retrying, `building_fm`
    /// once step 1 has saved and step 2 is in flight or retrying,
    /// `building_d2` once step 2 has saved successfully and step 3 is in
    /// flight or was interrupted by a restart (story 08 BC5a, BC12a),
    /// `ready` once step 3 has completed or the worker gave up retrying
    /// step 2 (BC7, BC3, BC4b, story 08 BC5).
    pub state: CircleState,
    /// The last time a request touched this viewer, unix seconds (BC23).
    /// `0` until the first request or worker save sets it.
    pub last_request_at: i64,
    /// When step 1 last saved successfully, unix seconds (BC7). `None`
    /// until the first successful save.
    pub d1_refreshed_at: Option<i64>,
    /// Incremented by one every time the worker swaps in a freshly built
    /// circle (BC7); the personalised cache and cursor key on this so a
    /// circle change never serves a stale or mismatched list (`## Approach`).
    pub circle_version: u64,
}

impl Circle {
    /// An empty circle, ready for [`crate::graph::build::step_follows`] to
    /// fill in.
    pub fn new() -> Self {
        Self::default()
    }

    /// An estimate of the heap bytes this circle holds (BC19): each
    /// `HashSet<u64>` costs `capacity() * (8 + 1)` bytes, the 8-byte key
    /// plus `hashbrown`'s one control byte for each slot, and `d2_sample`
    /// costs the length of each DID string it holds. This is an estimate
    /// for the probe's report, not an allocator measurement: it does not
    /// account for `hashbrown`'s load factor, group padding or the
    /// `Vec<String>`'s own spine, and a `String`'s heap allocation may
    /// round up past its `len()`.
    pub fn heap_bytes(&self) -> usize {
        let set_bytes = |set: &HashSet<DidHash>| set.capacity() * (8 + 1);
        let d2_bytes: usize = self.d2_sample.iter().map(|did| did.len()).sum();
        set_bytes(&self.follows) + set_bytes(&self.follows_me) + set_bytes(&self.checked) + d2_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_circle_has_zero_heap_bytes() {
        let circle = Circle::new();
        assert_eq!(circle.heap_bytes(), 0);
    }

    #[test]
    fn heap_bytes_grows_with_capacity_and_d2_sample() {
        let mut circle = Circle::new();
        circle.follows.reserve(64);
        circle.d2_sample.push("did:plc:abc".to_string());
        circle.d2_sample.push("did:plc:defg".to_string());

        let expected = circle.follows.capacity() * 9 + "did:plc:abc".len() + "did:plc:defg".len();
        assert_eq!(circle.heap_bytes(), expected);
    }
}
