// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use std::{io, net::SocketAddr};

/// Inspect the connected peer address of a socket.
pub trait SocketPeerAddr: SocketOperation {
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr>;
}
