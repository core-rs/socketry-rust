// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::SocketAddress;
use std::io;

/// Inspect the local IP address of a socket.
pub trait SocketLocalAddress: SocketOperation {
    fn socket_local_address(&self, socket: &Self::Socket) -> io::Result<SocketAddress>;
}
