// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// The address family of an IP socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketDomain {
    /// Internet Protocol version 4.
    Ipv4,
    /// Internet Protocol version 6.
    Ipv6,
}

impl From<std::net::SocketAddr> for SocketDomain {
    fn from(address: std::net::SocketAddr) -> Self {
        match address {
            std::net::SocketAddr::V4(_) => Self::Ipv4,
            std::net::SocketAddr::V6(_) => Self::Ipv6,
        }
    }
}
