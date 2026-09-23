//! `Circle`, TECH-DESIGN-network-feed §6.1, without `state` and the two
//! timestamps: the probe never schedules a refresh and never serves a
//! request, so it has no `building_*`/`ready` state machine and no
//! `last_request_at` or `d1_refreshed_at` to hold. Story 06 adds those
//! fields when the worker owns a `Circle` past its first build.

use std::collections::HashSet;

use super::DidHash;

/// One viewer's first-build graph. `follows`, `follows_me` and `checked`
/// hold hashes, never DID strings (BC16); `d2_sample` holds the DID
/// strings themselves, because [`crate::graph::build::step_degree2`] pages
/// `getFollows` for each one and a hash cannot be turned back into an
/// `actor` parameter.
#[derive(Debug, Clone, Default)]
pub struct Circle {
    pub follows: HashSet<DidHash>,
    pub follows_me: HashSet<DidHash>,
    pub checked: HashSet<DidHash>,
    pub d2_sample: Vec<String>,
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
