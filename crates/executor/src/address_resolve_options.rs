// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::SocketDomain;

/// Options for IP address resolution. The default returns both address families with port zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AddressResolveOptions {
    /// Numeric port attached to every result; service-name lookup is not performed.
    pub port: u16,
    /// Restrict returned addresses to this family. `None` accepts both families. This is a result filter, not a promise to restrict system resolver queries.
    pub domain: Option<SocketDomain>,
}
