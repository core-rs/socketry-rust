// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use async_io::Async;
use std::{io, net::SocketAddr};

#[cfg(all(feature = "io-uring", target_os = "linux"))]
use std::sync::Arc;

/// An owned nonblocking IP socket with one persistent native readiness registration. Unconnected, listening, connected, and datagram sockets use the same resource type. It is deliberately not `Clone`; backend requests privately retain ownership while in flight.
pub struct SocketDescriptor {
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    pub(crate) inner: Arc<Async<socket2::Socket>>,
    #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
    pub(crate) inner: Async<socket2::Socket>,
}

impl SocketDescriptor {
    pub(crate) fn new(socket: socket2::Socket) -> io::Result<Self> {
        Self::new_with(socket, Async::new)
    }

    fn new_with(
        socket: socket2::Socket,
        register: impl FnOnce(socket2::Socket) -> io::Result<Async<socket2::Socket>>,
    ) -> io::Result<Self> {
        let inner = register(socket)?;
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        let inner = Arc::new(inner);
        Ok(Self { inner })
    }
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    pub(crate) fn retain(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
    pub(crate) fn get_ref(&self) -> &socket2::Socket {
        self.inner.get_ref()
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref()
            .local_addr()
            .map(crate::scheduler::socket_io::address)
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref()
            .peer_addr()
            .map(crate::scheduler::socket_io::address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_errors_are_returned() {
        let socket =
            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();

        assert_eq!(
            SocketDescriptor::new_with(socket, |_| {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            })
            .err()
            .unwrap()
            .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn registered_socket_exposes_local_and_peer_addresses() {
        let socket =
            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        socket
            .bind(&socket2::SockAddr::from(
                "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            ))
            .unwrap();
        let descriptor = SocketDescriptor::new(socket).unwrap();

        assert_ne!(descriptor.local_addr().unwrap().port(), 0);
        assert_eq!(
            descriptor.peer_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
    }
}
