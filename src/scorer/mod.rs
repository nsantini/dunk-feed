//! The scorer task, TECH-DESIGN section 7. One pass per tick of a
//! `tokio::time::interval` selects dirty candidate pairs, prefilters them
//! on local counts, verifies the survivors against the App View, promotes
//! or drops each on verified counts, re-verifies young promoted pairs on
//! its own timer, expires stale rows, and swaps a freshly ranked and
//! capped snapshot into a shared `Arc<RwLock<Arc<Vec<FeedItem>>>>`. This
//! slice adds only `verify`, the section 8.2 embed-view table and its drop
//! reasons; the pass loop itself (`select`, `promote_or_drop`, `one_pass`)
//! is slice 2.0, and the snapshot and cap logic is slice 3.0.

#![allow(dead_code)] // First caller is slice 2.0's `one_pass`.

pub mod verify;
