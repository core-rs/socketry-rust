// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// The transport semantics requested when opening a socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketType {
    /// A byte stream, normally TCP.
    Stream,
    /// Datagrams, normally UDP. Connected datagram reads and writes preserve message boundaries; message/address APIs remain separate extensions.
    Datagram,
    /// Raw IP packets. Opening these sockets may require privileges.
    Raw,
    /// A platform-native socket type, preserved by resolution and forwarded unchanged to socket creation. Native values are not portable between operating systems.
    Other(i32),
}
