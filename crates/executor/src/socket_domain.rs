// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::SocketAddress;

/// The address family of an IP socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketDomain {
    /// Internet Protocol version 4.
    Ipv4,
    /// Internet Protocol version 6.
    Ipv6,
}

impl From<SocketAddress> for SocketDomain {
    fn from(address: SocketAddress) -> Self {
        match address {
            SocketAddress::V4(_) => Self::Ipv4,
            SocketAddress::V6(_) => Self::Ipv6,
        }
    }
}
