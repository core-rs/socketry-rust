// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{SocketDomain, SocketProtocol, SocketType};

/// Typed hints passed to the resolver. Defaults request either IP family, any socket type/protocol, and no flags.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AddressHints {
    pub domain: Option<SocketDomain>,
    pub socket_type: Option<SocketType>,
    pub protocol: Option<SocketProtocol>,
    /// With no hostname, request wildcard addresses suitable for binding; otherwise this flag has no effect. Without this flag, an absent hostname selects loopback addresses.
    pub passive: bool,
    /// Require a numeric hostname, preventing hostname lookup. Literal syntax, including IPv6 scope identifiers, follows the backend.
    pub numeric_host: bool,
    /// Ask the resolver to return families configured on the local system (`AI_ADDRCONFIG`). This is opt-in; loopback addresses do not establish a configured family on native backends.
    pub address_configured: bool,
}
