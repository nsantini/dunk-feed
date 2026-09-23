//! The connection filter, TECH-DESIGN-network-feed §6.3: one pure function
//! over a viewer's [`Circle`] and a slice of hashed author pairs, so the
//! probe can time it on synthetic items and story 06 can call it from the
//! worker once `FeedItem` carries author hashes.

use std::collections::HashSet;

use crate::graph::circle::Circle;
use crate::graph::DidHash;

/// One ranked item's two author hashes, as the filter needs them: not
/// `FeedItem`, which has no author hashes until story 01 merges (spec.md
/// `## Approach`, Rejected).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterItem {
    pub quote_did: DidHash,
    pub original_did: DidHash,
}

/// Keeps an item's index when its quoter or original author is in
/// `circle.follows` or `d2_set` (BC5, kept at any depth), or in
/// `circle.follows_me` and the item's index is below `follows_me_depth`
/// (BC6). Returns the kept indices in ascending, input order with no
/// duplicates (BC7): an item whose two authors match through different
/// sets still appears once.
pub fn connected_indices(
    items: &[FilterItem],
    circle: &Circle,
    d2_set: &HashSet<DidHash>,
    follows_me_depth: usize,
) -> Vec<u32> {
    let mut kept = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let authors = [item.quote_did, item.original_did];
        let connected = authors.iter().any(|author| {
            circle.follows.contains(author)
                || d2_set.contains(author)
                || (index < follows_me_depth && circle.follows_me.contains(author))
        });
        if connected {
            kept.push(index as u32);
        }
    }
    kept
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(quote_did: DidHash, original_did: DidHash) -> FilterItem {
        FilterItem { quote_did, original_did }
    }

    #[test]
    fn follows_is_kept_at_any_depth() {
        // BC5: a `follows` match is kept even past `follows_me_depth`.
        let mut circle = Circle::new();
        circle.follows.insert(1);
        let items = vec![item(1, 99)];

        let kept = connected_indices(&items, &circle, &HashSet::new(), 0);

        assert_eq!(kept, vec![0]);
    }

    #[test]
    fn degree2_is_kept_at_any_depth() {
        // BC5: a degree-2 match is kept even past `follows_me_depth`.
        let circle = Circle::new();
        let d2_set = HashSet::from([2]);
        let items = vec![item(99, 2)];

        let kept = connected_indices(&items, &circle, &d2_set, 0);

        assert_eq!(kept, vec![0]);
    }

    #[test]
    fn follows_me_is_kept_only_within_the_depth() {
        // BC6: two items match through `follows_me`; only the one below
        // `follows_me_depth` (2) is kept.
        let mut circle = Circle::new();
        circle.follows_me.insert(1);
        let items = vec![item(1, 99), item(99, 99), item(1, 99)];

        let kept = connected_indices(&items, &circle, &HashSet::new(), 2);

        assert_eq!(kept, vec![0]);
    }

    #[test]
    fn follows_me_boundary_is_exclusive_of_the_depth_index() {
        // BC6: index `depth - 1` is kept, index `depth` is not.
        let mut circle = Circle::new();
        circle.follows_me.insert(1);
        let items = vec![item(1, 99), item(1, 99), item(1, 99)];

        let kept = connected_indices(&items, &circle, &HashSet::new(), 2);

        assert_eq!(kept, vec![0, 1]);
    }

    #[test]
    fn output_is_ascending_with_no_duplicate_when_both_authors_match() {
        // BC7: an item whose quoter and original both match a set still
        // appears once, and the result stays in ascending, input order.
        let mut circle = Circle::new();
        circle.follows.insert(1);
        circle.follows.insert(2);
        let items = vec![item(9, 9), item(1, 2), item(1, 9)];

        let kept = connected_indices(&items, &circle, &HashSet::new(), 0);

        assert_eq!(kept, vec![1, 2]);
    }

    #[test]
    fn unconnected_item_is_dropped() {
        let circle = Circle::new();
        let items = vec![item(1, 2)];

        let kept = connected_indices(&items, &circle, &HashSet::new(), 100);

        assert!(kept.is_empty());
    }
}
