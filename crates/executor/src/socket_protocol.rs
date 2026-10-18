// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// An explicit IP transport protocol. Pass `None` to `socket_open` for the operating system default for the domain and type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketProtocol {
    /// Transmission Control Protocol.
    Tcp,
    /// User Datagram Protocol.
    Udp,
    /// A platform-native protocol number, preserved by resolution and forwarded unchanged to socket creation.
    Other(i32),
}
