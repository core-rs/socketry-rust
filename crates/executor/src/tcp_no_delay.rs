// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Boolean `TCP_NODELAY`: disable Nagle's algorithm when true. Valid for TCP sockets; this does not promise one packet per write or confirmation of peer receipt.
#[derive(Clone, Copy, Debug)]
pub struct TcpNoDelay;

impl crate::SocketOption for TcpNoDelay {
    type Value = bool;
}
