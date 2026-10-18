// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use std::{io, net::SocketAddr};

/// Inspect the local IP address of a socket.
pub trait SocketLocalAddr: SocketOperation {
    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr>;
}
