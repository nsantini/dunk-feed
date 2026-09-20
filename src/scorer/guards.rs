//! Guard stub, TECH-DESIGN section 9. Story 10 replaces this pass-through
//! with the follower floor, author-state and label checks; this slice
//! always returns `Pass` (BC40). `promote_or_drop` (`src/scorer/mod.rs`)
//! calls `check` on every verified pair after `verify_pair` continues, and
//! before it evaluates `score::qualifies`.

use crate::scorer::verify::VerifiedPair;
use crate::store::DropReason;

/// The result of running every guard over one verified pair. `Drop` carries
/// the reason `Store::drop_pair` writes, the same as a hard check inside
/// `verify_pair`. Story 10 is the first caller that ever returns `Drop`;
/// this slice's `check` returns only `Pass`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardResult {
    Pass,
    // Story 10 is this variant's first constructor: the follower floor,
    // author-state and label checks it adds will build `Drop`. Until then
    // nothing constructs it, so it needs its own attribute now that
    // `src/scorer/mod.rs`'s blanket `#![allow(dead_code)]` is gone.
    #[allow(dead_code)]
    Drop(DropReason),
}

/// Always `Pass` (BC40). No call to `getProfiles`, no label read: this
/// slice's `## Non-goals` says guards are a pass-through until story 10.
pub fn check(_pair: &VerifiedPair) -> GuardResult {
    GuardResult::Pass
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::score::Counts;

    fn pair() -> VerifiedPair {
        VerifiedPair {
            quote_uri: "at://did:plc:q/app.bsky.feed.post/q".to_string(),
            quote_cid: "cid-q".to_string(),
            quote_did: "did:plc:q".to_string(),
            original_uri: "at://did:plc:o/app.bsky.feed.post/o".to_string(),
            original_did: "did:plc:o".to_string(),
            quoted_at: 0,
            counts_q: Counts::default(),
            counts_o: Counts::default(),
        }
    }

    // BC40: the stub never inspects the pair, so any pair, qualifying or
    // not, always passes.
    #[test]
    fn always_passes() {
        assert_eq!(check(&pair()), GuardResult::Pass);
    }
}
