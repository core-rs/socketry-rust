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
        let inner = Async::new(socket)?;
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
