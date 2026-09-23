//! Viewer graph, TECH-DESIGN-network-feed §6. `circle` holds the per-viewer
//! `Circle`; `build` runs the three first-build steps over a `GraphSource`
//! so `graph_probe` (story 03) and the later worker (story 06) share one
//! implementation.

#![allow(dead_code)] // First callers are slice 2.0's `filter` module and
                     // slice 4.0's `graph_probe`.

use xxhash_rust::xxh3::xxh3_64;

pub mod build;
pub mod circle;
pub mod filter;

/// A DID's `xxh3_64` hash, kept in place of the DID string wherever a
/// `Circle` or the connection filter only needs to compare, not print, an
/// author (spec.md BC16: a viewer DID never reaches the probe's output).
pub type DidHash = u64;

/// Hashes `did` with `xxh3_64`, a fixed-key hash (not `RandomState`), so the
/// same DID hashes to the same `DidHash` in every run (BC18): a circle
/// built in one run and compared against one built in another still lines
/// up, and a test fixture's expected hash never has to be recomputed.
pub fn hash_did(did: &str) -> DidHash {
    xxh3_64(did.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_did_is_stable_across_calls() {
        // BC18: a fixed-key hash, so the same DID hashes to the same value
        // whether it is hashed once or many times, and whether the process
        // that hashes it is this one or a fresh one — `xxh3_64` carries no
        // per-process seed the way `std::collections::hash_map::RandomState`
        // does.
        let did = "did:plc:abc123";
        let first = hash_did(did);
        let second = hash_did(did);
        assert_eq!(first, second);
    }

    #[test]
    fn hash_did_differs_for_distinct_dids() {
        assert_ne!(hash_did("did:plc:abc"), hash_did("did:plc:def"));
    }
}
