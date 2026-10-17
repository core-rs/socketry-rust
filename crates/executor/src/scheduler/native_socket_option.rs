// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{SocketKeepalive, SocketOption, SocketReuseAddress, TcpNoDelay};
use socket2::Socket;
use std::io;

// Native syscall mappings stay private to the backends; portable option identifiers expose no socket2 types, raw pointers, or OS constants.
pub(crate) trait NativeSocketOption: SocketOption {
    fn get(self, socket: &Socket) -> io::Result<Self::Value>;
    fn set(self, socket: &Socket, value: Self::Value) -> io::Result<()>;
}

impl NativeSocketOption for SocketReuseAddress {
    fn get(self, socket: &Socket) -> io::Result<bool> {
        socket.reuse_address()
    }
    fn set(self, socket: &Socket, value: bool) -> io::Result<()> {
        socket.set_reuse_address(value)
    }
}

impl NativeSocketOption for SocketKeepalive {
    fn get(self, socket: &Socket) -> io::Result<bool> {
        socket.keepalive()
    }
    fn set(self, socket: &Socket, value: bool) -> io::Result<()> {
        socket.set_keepalive(value)
    }
}

impl NativeSocketOption for TcpNoDelay {
    fn get(self, socket: &Socket) -> io::Result<bool> {
        socket.tcp_nodelay()
    }
    fn set(self, socket: &Socket, value: bool) -> io::Result<()> {
        socket.set_tcp_nodelay(value)
    }
}
