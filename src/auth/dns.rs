//! SSRF defence for `did:web` fetches (`docs/02-TECH-DESIGN-network-feed.md`
//! §5). [`is_public`] is the address rule (BC1 to BC4): every range that is
//! not globally routable is blocked, not only private, loopback and
//! link-local (spec `## Defaults taken`), because a public host has no
//! reason to publish a reserved or documentation-range record. An IPv6
//! address that carries an IPv4 address (a mapped, compatible, NAT64 or
//! 6to4 form, BC3) is judged by the carried address, so `::ffff:a.b.c.d`
//! cannot smuggle a private `a.b.c.d` past the IPv6-shaped ranges above it.
//!
//! Review round 1, defect AT widens the blocked list beyond BC1 to BC3's
//! own text with five more IANA special-purpose ranges: the IPv4
//! `192.88.99.0/24` (6to4 relay anycast), and the IPv6 `64:ff9b:1::/48`
//! (local-use NAT64, blocked whole — unlike `64:ff9b::/96` above it, this
//! prefix is not judged by a carried IPv4 address), `100::/64`
//! (discard-only), `2001::/23` (IETF protocol assignments, which holds
//! `2001::/32` Teredo, `2001:2::/48`, `2001:10::/28` and `2001:20::/28`)
//! and `3fff::/20` (IETF protocol assignments, documentation). Each is
//! blocked under the same standing rule as BC1 to BC3: not globally
//! routable, so a public host has no reason to publish one.
//!
//! The lists follow the IANA IPv4 and IPv6 special-purpose address
//! registries directly: an entry whose "Globally Reachable" column reads
//! `False` is blocked, and an entry that reads `True` is allowed even when
//! it sits inside a wider range this module otherwise blocks. Review round
//! 2, defect AV: `192.0.0.9/32` and `192.0.0.10/32` are globally reachable
//! inside the blocked `192.0.0.0/24`, and `2001:1::1/128`, `2001:1::2/128`,
//! `2001:1::3/128`, `2001:3::/32`, `2001:4:112::/48`, `2001:20::/28` and
//! `2001:30::/28` are globally reachable inside the blocked `2001::/23` —
//! the rest of `2001::/23`, including Teredo `2001::/32`, `2001:2::/48` and
//! `2001:10::/28`, stays blocked. Defect AW: `100:0:0:1::/64` (a dummy
//! prefix, not the whole `100::/64` discard-only block above it) and
//! `5f00::/16` (SRv6 SIDs) are not globally reachable and join the blocked
//! list.
//!
//! [`PublicOnlyResolver`] is a `reqwest::dns::Resolve` over a [`LookupHost`]
//! seam: production resolves through `tokio::net::lookup_host`, and the
//! module's own tests resolve through a canned [`LookupHost`], so `did.rs`'s
//! `did:web` client never touches the network in a test run. The rule is
//! all-or-nothing (BC5, BC6): every address the lookup returns must be
//! public, or the whole lookup fails as [`Blocked`] and `reqwest` connects
//! to nothing. `reqwest` connects only to the addresses this resolver
//! returns (`ClientBuilder::dns_resolver`, `did.rs`), so a second DNS answer
//! for the same name — a rebinding attack — can never change the address
//! actually connected to.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// `true` only for an address that is globally routable (BC1 to BC4): every
/// IANA special-purpose range — private, loopback, link-local, reserved,
/// documentation and multicast alike — reads as not public, and an IPv6
/// address that carries an IPv4 address (BC3) is judged by that carried
/// address.
pub(super) fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => is_public_v6(v6),
    }
}

/// BC1, review round 1 defect AT: the IPv4 special-purpose ranges from the
/// IANA registry, including `192.88.99.0/24` (6to4 relay anycast). Review
/// round 2, defect AV: `192.0.0.9/32` and `192.0.0.10/32` are the IANA
/// registry's globally reachable exceptions inside the blocked
/// `192.0.0.0/24`, checked ahead of the block list so they read as public.
fn is_public_v4(ip: Ipv4Addr) -> bool {
    let bits = u32::from(ip);
    const GLOBALLY_REACHABLE: [u32; 2] = [
        0xc0000009, // 192.0.0.9/32
        0xc000000a, // 192.0.0.10/32
    ];
    if GLOBALLY_REACHABLE.contains(&bits) {
        return true;
    }
    const RANGES: [(u32, u32); 15] = [
        (0x00000000, 8),  // 0.0.0.0/8
        (0x0a000000, 8),  // 10.0.0.0/8
        (0x64400000, 10), // 100.64.0.0/10
        (0x7f000000, 8),  // 127.0.0.0/8
        (0xa9fe0000, 16), // 169.254.0.0/16
        (0xac100000, 12), // 172.16.0.0/12
        (0xc0000000, 24), // 192.0.0.0/24
        (0xc0000200, 24), // 192.0.2.0/24
        (0xc0586300, 24), // 192.88.99.0/24 (6to4 relay anycast)
        (0xc0a80000, 16), // 192.168.0.0/16
        (0xc6120000, 15), // 198.18.0.0/15
        (0xc6336400, 24), // 198.51.100.0/24
        (0xcb007100, 24), // 203.0.113.0/24
        (0xe0000000, 4),  // 224.0.0.0/4
        (0xf0000000, 4),  // 240.0.0.0/4 (includes 255.255.255.255)
    ];
    !RANGES.iter().any(|(network, prefix)| in_v4_range(bits, *network, *prefix))
}

fn in_v4_range(bits: u32, network: u32, prefix: u32) -> bool {
    let mask = if prefix == 0 { 0 } else { u32::MAX << (32 - prefix) };
    bits & mask == network & mask
}

/// BC2: the IPv6 special-purpose ranges. `::` and `::1` are checked
/// directly, ahead of BC3's carried-IPv4 ranges (`v4_in_v6`), because both
/// also match the deprecated IPv4-compatible form (`::/96`) — checking
/// them first keeps that case reachable instead of always being caught
/// here first. Every other carried-IPv4 case (mapped, NAT64, 6to4) is
/// judged by the address it carries, ahead of the generic range checks
/// below, which were never meant for them.
fn is_public_v6(ip: Ipv6Addr) -> bool {
    if ip.is_unspecified() || ip.is_loopback() {
        return false;
    }
    if let Some(embedded) = v4_in_v6(ip) {
        return is_public_v4(embedded);
    }
    let segments = ip.segments();
    // 2001::/23 globally reachable exceptions (review round 2, defect AV),
    // checked ahead of the 2001::/23 block below so they read as public:
    // 2001:1::1/128, 2001:1::2/128 and 2001:1::3/128;
    if segments[0] == 0x2001
        && segments[1] == 0x0001
        && segments[2..7] == [0, 0, 0, 0, 0]
        && matches!(segments[7], 1..=3)
    {
        return true;
    }
    // 2001:3::/32;
    if segments[0] == 0x2001 && segments[1] == 0x0003 {
        return true;
    }
    // 2001:4:112::/48;
    if segments[0] == 0x2001 && segments[1] == 0x0004 && segments[2] == 0x0112 {
        return true;
    }
    // 2001:20::/28 and 2001:30::/28.
    if segments[0] == 0x2001 && (segments[1] & 0xfff0 == 0x0020 || segments[1] & 0xfff0 == 0x0030) {
        return true;
    }
    // 64:ff9b:1::/48: local-use NAT64. Blocked whole, unlike `64:ff9b::/96`
    // above (`v4_in_v6`): review round 1, defect AT.
    if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2] == 0x0001 {
        return false;
    }
    // 100::/64: discard-only (review round 1, defect AT).
    if segments[0] == 0x0100 && segments[1] == 0 && segments[2] == 0 && segments[3] == 0 {
        return false;
    }
    // 100:0:0:1::/64: dummy prefix, a separate /64 from 100::/64 above
    // (review round 2, defect AW).
    if segments[0] == 0x0100 && segments[1] == 0 && segments[2] == 0 && segments[3] == 1 {
        return false;
    }
    // 5f00::/16: SRv6 SIDs (review round 2, defect AW).
    if segments[0] == 0x5f00 {
        return false;
    }
    // fc00::/7: unique local.
    if segments[0] & 0xfe00 == 0xfc00 {
        return false;
    }
    // fe80::/10: link local.
    if segments[0] & 0xffc0 == 0xfe80 {
        return false;
    }
    // fec0::/10: site local (deprecated).
    if segments[0] & 0xffc0 == 0xfec0 {
        return false;
    }
    // ff00::/8: multicast.
    if segments[0] & 0xff00 == 0xff00 {
        return false;
    }
    // 2001:db8::/32: documentation.
    if segments[0] == 0x2001 && segments[1] == 0x0db8 {
        return false;
    }
    // 2001::/23: IETF protocol assignments (review round 1, defect AT),
    // which holds `2001::/32` Teredo, `2001:2::/48` and `2001:10::/28` —
    // the top 7 bits of the second segment are 0. Not the whole /23: the
    // IANA globally reachable exceptions above (defect AV) return early.
    if segments[0] == 0x2001 && segments[1] & 0xfe00 == 0 {
        return false;
    }
    // 3fff::/20: IETF protocol assignments, documentation (review round 1,
    // defect AT) — the top 4 bits of the second segment are 0.
    if segments[0] == 0x3fff && segments[1] & 0xf000 == 0 {
        return false;
    }
    true
}

/// BC3: an IPv6 address that carries an IPv4 address, extracted so
/// [`is_public_v6`] can judge it by the carried address instead. `None`
/// when `ip` carries no IPv4 address in a recognised form.
fn v4_in_v6(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    let segments = ip.segments();
    // ::ffff:0:0/96: IPv4-mapped.
    if segments[0..5] == [0, 0, 0, 0, 0] && segments[5] == 0xffff {
        return Some(Ipv4Addr::new(
            (segments[6] >> 8) as u8,
            (segments[6] & 0xff) as u8,
            (segments[7] >> 8) as u8,
            (segments[7] & 0xff) as u8,
        ));
    }
    // ::/96: IPv4-compatible (deprecated). Overlaps `::` and `::1`, which
    // `is_public_v6` also rejects directly on the embedded address (0.0.0.0
    // and 0.0.0.1 are both inside 0.0.0.0/8), so the two checks agree.
    if segments[0..6] == [0, 0, 0, 0, 0, 0] {
        return Some(Ipv4Addr::new(
            (segments[6] >> 8) as u8,
            (segments[6] & 0xff) as u8,
            (segments[7] >> 8) as u8,
            (segments[7] & 0xff) as u8,
        ));
    }
    // 64:ff9b::/96: NAT64.
    if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2..6] == [0, 0, 0, 0] {
        return Some(Ipv4Addr::new(
            (segments[6] >> 8) as u8,
            (segments[6] & 0xff) as u8,
            (segments[7] >> 8) as u8,
            (segments[7] & 0xff) as u8,
        ));
    }
    // 2002::/16: 6to4. The next 32 bits (two segments) are the address.
    if segments[0] == 0x2002 {
        return Some(Ipv4Addr::new(
            (segments[1] >> 8) as u8,
            (segments[1] & 0xff) as u8,
            (segments[2] >> 8) as u8,
            (segments[2] & 0xff) as u8,
        ));
    }
    None
}

/// BC6: the resolver's only error. Carries nothing — not the host, not the
/// address — so a blocked lookup can never put either into a log line
/// (`did.rs`'s `blocked_address` warning). `did.rs`'s `FetchError::Blocked`
/// is recovered from `reqwest`'s wrapped error by walking
/// `std::error::Error::source` for this type (spec `## Defaults taken`).
#[derive(Debug)]
pub(super) struct Blocked;

impl std::fmt::Display for Blocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("resolved address is not public")
    }
}

impl std::error::Error for Blocked {}

/// The lookup seam [`PublicOnlyResolver`] resolves a host name through:
/// [`TokioLookupHost`] in production, a canned result in this module's own
/// tests (BC5, BC6), so `resolve_rules` never touches the network.
pub(super) trait LookupHost: Send + Sync {
    fn lookup(
        &self,
        host: String,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<IpAddr>>> + Send>>;
}

/// The production [`LookupHost`]: `tokio::net::lookup_host` needs a port to
/// build a `SocketAddr` to resolve against, but the port is never used by
/// [`PublicOnlyResolver`] (BC7: `reqwest` applies the URL's own port to
/// whatever address this resolver returns), so `0` stands in for it.
struct TokioLookupHost;

impl LookupHost for TokioLookupHost {
    fn lookup(
        &self,
        host: String,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<IpAddr>>> + Send>> {
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host.as_str(), 0u16)).await?;
            Ok(addrs.map(|addr| addr.ip()).collect())
        })
    }
}

/// A `reqwest::dns::Resolve` that looks a host up through [`LookupHost`] and
/// fails the whole lookup unless every returned address is [`is_public`]
/// (BC5, BC6). `did.rs`'s `did:web` client is built with this as its
/// `dns_resolver`, so `reqwest` connects only to the addresses this
/// resolver hands back — the checked address is the connected address, and
/// a second DNS answer for the same name cannot change that (spec
/// `## Approach`, rejected alternative).
pub(super) struct PublicOnlyResolver {
    lookup: Arc<dyn LookupHost>,
}

impl PublicOnlyResolver {
    pub(super) fn new() -> Self {
        Self { lookup: Arc::new(TokioLookupHost) }
    }

    /// Test-only seam (`did.rs`'s `did_web_blocked`, `blocked_log_and_cooldown`):
    /// builds a resolver over a canned `lookup` instead of
    /// [`TokioLookupHost`], so those tests send a fixed address without a
    /// real DNS lookup.
    #[cfg(test)]
    pub(super) fn with_lookup(lookup: Arc<dyn LookupHost>) -> Self {
        Self { lookup }
    }
}

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let lookup = Arc::clone(&self.lookup);
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = lookup
                .lookup(host)
                .await
                .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;
            if addrs.is_empty() || addrs.iter().any(|ip| !is_public(*ip)) {
                return Err(Box::new(Blocked) as Box<dyn std::error::Error + Send + Sync>);
            }
            let socket_addrs: Addrs = Box::new(addrs.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(socket_addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One inside and one just outside address for each BC1 to BC3 range,
    /// plus BC4's "any other address" default.
    #[test]
    fn blocked_ranges() {
        // BC1: IPv4.
        let blocked_v4 = [
            "0.0.0.0",
            "0.255.255.255", // 0.0.0.0/8
            "10.0.0.0",
            "10.255.255.255", // 10.0.0.0/8
            "100.64.0.0",
            "100.127.255.255", // 100.64.0.0/10
            "127.0.0.0",
            "127.255.255.255", // 127.0.0.0/8
            "169.254.0.0",
            "169.254.255.255", // 169.254.0.0/16
            "172.16.0.0",
            "172.31.255.255", // 172.16.0.0/12
            "192.0.0.0",
            "192.0.0.255", // 192.0.0.0/24
            "192.0.2.0",
            "192.0.2.255", // 192.0.2.0/24
            "192.168.0.0",
            "192.168.255.255", // 192.168.0.0/16
            "198.18.0.0",
            "198.19.255.255", // 198.18.0.0/15
            "198.51.100.0",
            "198.51.100.255", // 198.51.100.0/24
            "203.0.113.0",
            "203.0.113.255", // 203.0.113.0/24
            "224.0.0.0",
            "239.255.255.255", // 224.0.0.0/4
            "240.0.0.0",
            "255.255.255.255", // 240.0.0.0/4
            "192.88.99.0",
            "192.88.99.255", // 192.88.99.0/24 (review round 1, defect AT)
        ];
        for addr in blocked_v4 {
            let ip: Ipv4Addr = addr.parse().unwrap();
            assert!(!is_public(IpAddr::V4(ip)), "{addr} must be blocked");
        }
        let just_outside_v4 = [
            "1.0.0.0",         // just past 0.0.0.0/8
            "9.255.255.255",   // just before 10.0.0.0/8
            "11.0.0.0",        // just past 10.0.0.0/8
            "100.63.255.255",  // just before 100.64.0.0/10
            "100.128.0.0",     // just past 100.64.0.0/10
            "126.255.255.255", // just before 127.0.0.0/8
            "128.0.0.0",       // just past 127.0.0.0/8
            "169.253.255.255", // just before 169.254.0.0/16
            "169.255.0.0",     // just past 169.254.0.0/16
            "172.15.255.255",  // just before 172.16.0.0/12
            "172.32.0.0",      // just past 172.16.0.0/12
            "191.255.255.255", // just before 192.0.0.0/24
            "192.0.1.255",     // just before 192.0.2.0/24, just past 192.0.0.0/24
            "192.0.3.0",       // just past 192.0.2.0/24
            "192.167.255.255", // just before 192.168.0.0/16
            "192.169.0.0",     // just past 192.168.0.0/16
            "198.17.255.255",  // just before 198.18.0.0/15
            "198.20.0.0",      // just past 198.18.0.0/15
            "198.51.99.255",   // just before 198.51.100.0/24
            "198.51.101.0",    // just past 198.51.100.0/24
            "203.0.112.255",   // just before 203.0.113.0/24
            "203.0.114.0",     // just past 203.0.113.0/24
            "223.255.255.255", // just before 224.0.0.0/4
            "8.8.8.8",         // an ordinary public address
            "192.88.98.255",   // just before 192.88.99.0/24
            "192.88.100.0",    // just past 192.88.99.0/24
        ];
        for addr in just_outside_v4 {
            let ip: Ipv4Addr = addr.parse().unwrap();
            assert!(is_public(IpAddr::V4(ip)), "{addr} must be public");
        }

        // BC2: IPv6.
        let blocked_v6 = [
            "::",
            "::1",
            "fc00::",
            "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", // fc00::/7
            "fe80::",
            "febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff", // fe80::/10
            "fec0::",
            "feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", // fec0::/10
            "ff00::",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", // ff00::/8
            "2001:db8::",
            "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff", // 2001:db8::/32
            "2001::",
            "2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff", // 2001::/23 (holds Teredo, review round 1 defect AT)
            "64:ff9b:1::",
            "64:ff9b:1:ffff:ffff:ffff:ffff:ffff", // 64:ff9b:1::/48, local-use NAT64 (defect AT)
            "100::",
            "100::ffff:ffff:ffff:ffff", // 100::/64, discard-only (defect AT)
            "100:0:0:1::",
            "100:0:0:1:ffff:ffff:ffff:ffff", // 100:0:0:1::/64, dummy prefix (defect AW)
            "5f00::",
            "5f00:ffff:ffff:ffff:ffff:ffff:ffff:ffff", // 5f00::/16, SRv6 SIDs (defect AW)
            "3fff::",
            "3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff", // 3fff::/20 (defect AT)
            "2001:2::",
            "2001:10::", // inside 2001::/23, not one of its exceptions (defect AV)
        ];
        for addr in blocked_v6 {
            let ip: Ipv6Addr = addr.parse().unwrap();
            assert!(!is_public(IpAddr::V6(ip)), "{addr} must be blocked");
        }
        // fe80::/10 and fec0::/10 are adjacent (0xfe80..0xfebf immediately
        // followed by 0xfec0..0xfeff), and fec0::/10 immediately precedes
        // ff00::/8, so there is no gap to test between any of those three.
        let just_outside_v6 = [
            "fbff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", // just before fc00::/7
            "fe00::",                                  // between fc00::/7 and fe80::/10
            "fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", // just before fe80::/10
        ];
        for addr in just_outside_v6 {
            let ip: Ipv6Addr = addr.parse().unwrap();
            assert!(is_public(IpAddr::V6(ip)), "{addr} must be public");
        }
        assert!(
            is_public(IpAddr::V6("2606:4700:4700::1111".parse().unwrap())),
            "a real public v6 address"
        );
        assert!(
            is_public(IpAddr::V6("2001:0db7:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap())),
            "just before 2001:db8::/32"
        );
        assert!(is_public(IpAddr::V6("2001:0db9::".parse().unwrap())), "just past 2001:db8::/32");
        assert!(
            is_public(IpAddr::V6("2000:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap())),
            "just before 2001::/23"
        );
        assert!(is_public(IpAddr::V6("2001:200::".parse().unwrap())), "just past 2001::/23");
        assert!(
            is_public(IpAddr::V6("64:ff9b:0:ffff:ffff:ffff:ffff:ffff".parse().unwrap())),
            "just before 64:ff9b:1::/48"
        );
        assert!(is_public(IpAddr::V6("64:ff9b:2::".parse().unwrap())), "just past 64:ff9b:1::/48");
        assert!(
            is_public(IpAddr::V6("ff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap())),
            "just before 100::/64"
        );
        assert!(
            !is_public(IpAddr::V6("100:0:0:1::".parse().unwrap())),
            "100:0:0:1::/64, a dummy prefix distinct from 100::/64 (defect AW)"
        );
        assert!(
            !is_public(IpAddr::V6("100:0:0:1:ffff:ffff:ffff:ffff".parse().unwrap())),
            "still inside 100:0:0:1::/64"
        );
        assert!(is_public(IpAddr::V6("100:0:0:2::".parse().unwrap())), "just past 100:0:0:1::/64");
        assert!(
            is_public(IpAddr::V6("5eff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap())),
            "just before 5f00::/16"
        );
        assert!(is_public(IpAddr::V6("5f01::".parse().unwrap())), "just past 5f00::/16");
        assert!(
            is_public(IpAddr::V6("3ffe:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap())),
            "just before 3fff::/20"
        );
        assert!(is_public(IpAddr::V6("3fff:1000::".parse().unwrap())), "just past 3fff::/20");
        assert!(
            !is_public(IpAddr::V6("::ffff:169.254.169.254".parse().unwrap())),
            "mapped link-local (AWS metadata service)"
        );

        // Review round 2, defect AV: IANA globally reachable exceptions
        // inside otherwise-blocked ranges.
        assert!(is_public(IpAddr::V4("192.0.0.9".parse().unwrap())), "192.0.0.9/32 exception");
        assert!(is_public(IpAddr::V4("192.0.0.10".parse().unwrap())), "192.0.0.10/32 exception");
        assert!(!is_public(IpAddr::V4("192.0.0.8".parse().unwrap())), "just before 192.0.0.9/32");
        assert!(!is_public(IpAddr::V4("192.0.0.11".parse().unwrap())), "just after 192.0.0.10/32");
        assert!(is_public(IpAddr::V6("2001:1::1".parse().unwrap())), "2001:1::1/128 exception");
        assert!(is_public(IpAddr::V6("2001:1::2".parse().unwrap())), "2001:1::2/128 exception");
        assert!(is_public(IpAddr::V6("2001:1::3".parse().unwrap())), "2001:1::3/128 exception");
        assert!(
            !is_public(IpAddr::V6("2001:1::4".parse().unwrap())),
            "just past 2001:1::3/128, still inside blocked 2001::/23"
        );
        assert!(is_public(IpAddr::V6("2001:3::".parse().unwrap())), "2001:3::/32 exception");
        assert!(
            is_public(IpAddr::V6("2001:3:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap())),
            "still inside 2001:3::/32 exception"
        );
        assert!(
            is_public(IpAddr::V6("2001:4:112::".parse().unwrap())),
            "2001:4:112::/48 exception"
        );
        assert!(
            !is_public(IpAddr::V6("2001:4:111:ffff:ffff:ffff:ffff:ffff".parse().unwrap())),
            "just before 2001:4:112::/48"
        );
        assert!(is_public(IpAddr::V6("2001:20::".parse().unwrap())), "2001:20::/28 exception");
        assert!(
            is_public(IpAddr::V6("2001:2f:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap())),
            "still inside 2001:20::/28 exception"
        );
        assert!(
            !is_public(IpAddr::V6("2001:10::".parse().unwrap())),
            "2001:10::/28, not an exception"
        );
        assert!(is_public(IpAddr::V6("2001:30::".parse().unwrap())), "2001:30::/28 exception");
        assert!(
            !is_public(IpAddr::V6("2001:40::".parse().unwrap())),
            "just past 2001:30::/28, inside blocked 2001::/23"
        );

        // BC3: IPv6 carrying an IPv4 address, judged by the carried
        // address.
        assert!(!is_public(IpAddr::V6("::ffff:10.0.0.1".parse().unwrap())), "mapped private v4");
        assert!(is_public(IpAddr::V6("::ffff:8.8.8.8".parse().unwrap())), "mapped public v4");
        assert!(!is_public(IpAddr::V6("::0.0.0.1".parse().unwrap())), "compatible private v4");
        assert!(is_public(IpAddr::V6("::8.8.8.8".parse().unwrap())), "compatible public v4");
        assert!(!is_public(IpAddr::V6("64:ff9b::10.0.0.1".parse().unwrap())), "NAT64 private v4");
        assert!(is_public(IpAddr::V6("64:ff9b::8.8.8.8".parse().unwrap())), "NAT64 public v4");
        assert!(!is_public(IpAddr::V6("2002:0a00:0001::".parse().unwrap())), "6to4 private v4");
        assert!(is_public(IpAddr::V6("2002:0808:0808::".parse().unwrap())), "6to4 public v4");

        // BC4: any other address.
        assert!(is_public(IpAddr::V4("1.1.1.1".parse().unwrap())));
    }

    struct FixedLookup(std::io::Result<Vec<IpAddr>>);

    impl LookupHost for FixedLookup {
        fn lookup(
            &self,
            _host: String,
        ) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<IpAddr>>> + Send>> {
            let result = match &self.0 {
                Ok(addrs) => Ok(addrs.clone()),
                Err(err) => Err(std::io::Error::new(err.kind(), err.to_string())),
            };
            Box::pin(async move { result })
        }
    }

    fn ip(addr: &str) -> IpAddr {
        addr.parse().unwrap()
    }

    #[tokio::test]
    async fn resolve_rules() {
        // BC5: every returned address is public, so the resolver returns
        // exactly those addresses.
        let resolver = PublicOnlyResolver::with_lookup(Arc::new(FixedLookup(Ok(vec![
            ip("8.8.8.8"),
            ip("1.1.1.1"),
        ]))));
        let name: Name = "example.com".parse().unwrap();
        let addrs: Vec<IpAddr> = resolver.resolve(name).await.unwrap().map(|s| s.ip()).collect();
        assert_eq!(addrs, vec![ip("8.8.8.8"), ip("1.1.1.1")]);

        // BC6: one address is not public, the whole lookup fails.
        let resolver = PublicOnlyResolver::with_lookup(Arc::new(FixedLookup(Ok(vec![
            ip("8.8.8.8"),
            ip("127.0.0.1"),
        ]))));
        let name: Name = "example.com".parse().unwrap();
        let err = resolver.resolve(name).await.err().expect("mixed result must be blocked");
        assert!(err.downcast_ref::<Blocked>().is_some());

        // BC6: no address at all is also blocked.
        let resolver = PublicOnlyResolver::with_lookup(Arc::new(FixedLookup(Ok(Vec::new()))));
        let name: Name = "example.com".parse().unwrap();
        let err = resolver.resolve(name).await.err().expect("empty result must be blocked");
        assert!(err.downcast_ref::<Blocked>().is_some());

        // A lookup failure (not a block) surfaces as its own error, not
        // `Blocked`.
        let resolver = PublicOnlyResolver::with_lookup(Arc::new(FixedLookup(Err(
            std::io::Error::new(std::io::ErrorKind::NotFound, "no such host"),
        ))));
        let name: Name = "example.com".parse().unwrap();
        let err = resolver.resolve(name).await.err().expect("lookup failure must propagate");
        assert!(err.downcast_ref::<Blocked>().is_none());
    }
}
