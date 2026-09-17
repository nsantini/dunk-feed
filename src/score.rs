//! Pure scoring functions, TECH-DESIGN section 7.1. Every function here is
//! pure: no I/O, no environment reads, no config lookups. `Weights` and
//! `Thresholds` carry the score constants a caller needs, and both are built
//! from `Config` by `From` impls so no literal here ever stands in for a
//! constant the PRD's score table owns. `validate` (story 03) and the scorer
//! (story 07) call these exact functions instead of writing their own copy.

#![allow(dead_code)] // First callers are `dunk validate` (story 03) and story 07's scorer.

use crate::appview::types::PostView;
use crate::config::Config;

/// The three counts one side of a pair, verified against the App View.
/// `u32` per TECH-DESIGN section 7.1; a real post cannot have a negative
/// like, repost or reply count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub likes: u32,
    pub reposts: u32,
    pub replies: u32,
}

/// Maps `PostView`'s three counts one to one (BC27), so `validate` (story
/// 03) and the scorer (story 07) share one mapping instead of each writing
/// its own. Reads three integer fields and performs no I/O, so `score.rs`
/// stays pure.
impl From<&PostView> for Counts {
    fn from(post: &PostView) -> Self {
        Counts { likes: post.like_count, reposts: post.repost_count, replies: post.reply_count }
    }
}

/// The weights `engagement` applies to a repost and a reply. Built from
/// `Config::w_repost` and `Config::w_reply`, never a literal.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Weights {
    pub repost: f64,
    pub reply: f64,
}

impl From<&Config> for Weights {
    fn from(cfg: &Config) -> Self {
        Weights { repost: cfg.w_repost, reply: cfg.w_reply }
    }
}

/// The three score thresholds `qualifies` reads. `k` and `p` are `f64` here,
/// converted once from `Config`'s `u32` fields, so no cast appears inside a
/// formula. Built from `Config::k`, `Config::p` and `Config::m`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Thresholds {
    pub k: f64,
    pub p: f64,
    pub m: f64,
}

impl From<&Config> for Thresholds {
    fn from(cfg: &Config) -> Self {
        Thresholds { k: f64::from(cfg.k), p: f64::from(cfg.p), m: cfg.m }
    }
}

/// `E(p)`: likes plus a repost weighted by `w.repost` plus a reply weighted
/// by `w.reply`. Uses the `Weights` the caller passed; never reads a default.
pub fn engagement(c: &Counts, w: &Weights) -> f64 {
    f64::from(c.likes) + w.repost * f64::from(c.reposts) + w.reply * f64::from(c.replies)
}

/// `D`: the quote's engagement over the original's, smoothed by `k` so a
/// dead original (`eo == 0.0`) never divides by zero. Finite for every finite,
/// non-negative `eq`, `eo` and `k > 0`.
pub fn ratio(eq: f64, eo: f64, k: f64) -> f64 {
    eq / (eo + k)
}

/// `max(eo, eq) >= P && D >= M`. Both boundaries are `>=`, not `>`, so a pair
/// sitting exactly on the floor or the multiplier qualifies.
pub fn qualifies(eq: f64, eo: f64, thresholds: &Thresholds) -> bool {
    let d = ratio(eq, eo, thresholds.k);
    eo.max(eq) >= thresholds.p && d >= thresholds.m
}

/// `D * log10(1 + eq) / (age_hours + 2)^1.5`. Strictly decreasing in
/// `age_hours` and strictly increasing in `eq`, at fixed `d`. `age_hours`
/// must be non-negative; a negative age is a caller error the scorer's own
/// timestamp arithmetic never produces, not a case this function guards.
pub fn rank(d: f64, eq: f64, age_hours: f64) -> f64 {
    d * (1.0 + eq).log10() / (age_hours + 2.0).powf(1.5)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thresholds(k: f64, p: f64, m: f64) -> Thresholds {
        Thresholds { k, p, m }
    }

    #[test]
    fn ratio_finite_when_original_dead() {
        let d = ratio(10.0, 0.0, 5.0);
        assert!(d.is_finite());
        assert_eq!(d, 2.0);
    }

    #[test]
    fn rank_decays_with_age() {
        let earlier = rank(2.0, 10.0, 1.0);
        let later = rank(2.0, 10.0, 5.0);
        assert!(later < earlier);
    }

    #[test]
    fn rank_rises_with_quote_engagement() {
        let low = rank(2.0, 5.0, 3.0);
        let high = rank(2.0, 50.0, 3.0);
        assert!(high > low);
    }

    #[test]
    fn qualifies_false_when_both_sides_below_floor() {
        let t = thresholds(5.0, 50.0, 1.25);
        // eo and eq both below p = 50, whatever d works out to be.
        assert!(!qualifies(10.0, 10.0, &t));
    }

    #[test]
    fn qualifies_true_on_exact_boundary() {
        // eq == p exactly, and eo chosen so ratio(eq, eo, k) == m exactly.
        let t = thresholds(5.0, 50.0, 1.25);
        let eq = 50.0;
        // d = eq / (eo + k) = m  =>  eo = eq / m - k
        let eo = eq / t.m - t.k;
        let d = ratio(eq, eo, t.k);
        assert_eq!(d, t.m);
        assert!(qualifies(eq, eo, &t));
    }

    #[test]
    fn qualifies_false_below_ratio_floor_even_above_count_floor() {
        let t = thresholds(5.0, 50.0, 1.25);
        // eq well above p, but d well below m.
        assert!(!qualifies(50.0, 1000.0, &t));
    }

    #[test]
    fn weights_come_from_config() {
        let lookup = |name: &str| match name {
            "DUNK_HOSTNAME" => Some("feed.example.com".to_string()),
            "DUNK_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "DUNK_W_REPOST" => Some("3.0".to_string()),
            "DUNK_W_REPLY" => Some("0.75".to_string()),
            _ => None,
        };
        let cfg = crate::config::load(lookup).expect("valid config");
        let weights = Weights::from(&cfg);
        assert_eq!(weights.repost, 3.0);
        assert_eq!(weights.reply, 0.75);
    }

    #[test]
    fn thresholds_come_from_config() {
        let lookup = |name: &str| match name {
            "DUNK_HOSTNAME" => Some("feed.example.com".to_string()),
            "DUNK_PUBLISHER_DID" => Some("did:plc:abc".to_string()),
            "DUNK_K" => Some("7".to_string()),
            "DUNK_P" => Some("80".to_string()),
            "DUNK_M" => Some("1.5".to_string()),
            _ => None,
        };
        let cfg = crate::config::load(lookup).expect("valid config");
        let thresholds = Thresholds::from(&cfg);
        assert_eq!(thresholds.k, 7.0);
        assert_eq!(thresholds.p, 80.0);
        assert_eq!(thresholds.m, 1.5);
    }

    #[test]
    fn zero_counts_score_zero() {
        let counts = Counts::default();
        let weights = Weights { repost: 2.0, reply: 0.5 };
        let t = thresholds(5.0, 50.0, 1.25);
        let e = engagement(&counts, &weights);
        assert_eq!(e, 0.0);
        assert_eq!(ratio(e, e, t.k), 0.0);
        assert!(!qualifies(e, e, &t));
        assert_eq!(rank(ratio(e, e, t.k), e, 0.0), 0.0);
    }

    #[test]
    fn counts_from_post_view() {
        // BC27: `Counts::from` maps `likeCount`, `repostCount` and
        // `replyCount` one to one.
        use crate::appview::types::{PostRecord, PostViewAuthor};
        let post = PostView {
            uri: "at://did:plc:abc/app.bsky.feed.post/xyz".to_string(),
            author: PostViewAuthor { did: "did:plc:abc".to_string() },
            labels: vec![],
            record: PostRecord { created_at: "2026-01-01T00:00:00Z".to_string() },
            like_count: 10,
            repost_count: 2,
            reply_count: 1,
            embed: None,
        };
        let counts = Counts::from(&post);
        assert_eq!(counts, Counts { likes: 10, reposts: 2, replies: 1 });
    }
}
