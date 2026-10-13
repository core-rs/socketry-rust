// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::Socket;
use std::{
    io,
    net::{TcpListener, TcpStream},
};

/// Optional interoperability with existing standard-library TCP resources.
/// Register once; the backend sets the nonblocking mode it needs. Do not change
/// that mode through another handle. Simulated schedulers need not implement it.
pub trait SocketRegistration: Socket {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket>;
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Listener>;
}
