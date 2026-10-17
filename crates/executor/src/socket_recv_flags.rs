// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Portable input flags for `socket_recvmsg`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SocketRecvFlags {
    /// Copy queued bytes without consuming them (`MSG_PEEK`). Concurrent readers may consume them before a later receive; peeking reserves nothing.
    pub peek: bool,
}
