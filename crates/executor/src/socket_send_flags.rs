// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Portable input flags for `socket_sendmsg`. Additional platform-specific flags belong in extension capabilities.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SocketSendFlags {
    /// Ask the OS to send directly to a local network without routing (`MSG_DONTROUTE`). Protocols may reject this or ignore it.
    pub dont_route: bool,
}
