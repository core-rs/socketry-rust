// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use std::{io, net::SocketAddr};
use tokio::runtime::{Handle, Id};

#[cfg(unix)]
type RegisteredSocket = tokio::io::unix::AsyncFd<socket2::Socket>;

#[cfg(windows)]
type RegisteredSocket = async_io::Async<socket2::Socket>;

/// A nonblocking socket registered with the adapter's Tokio runtime.
///
/// A descriptor can represent an unconnected, bound, listening, connected, or datagram socket. It is deliberately not `Clone`; operation futures borrow it for their duration.
pub struct SocketDescriptor {
    inner: RegisteredSocket,
    runtime: Id,
}

impl SocketDescriptor {
    pub(super) fn new(runtime: &Handle, socket: socket2::Socket) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        #[cfg(unix)]
        let inner = {
            let _scope = runtime.enter();
            RegisteredSocket::new(socket)?
        };

        #[cfg(windows)]
        let inner = RegisteredSocket::new(socket)?;

        Ok(Self {
            inner,
            runtime: runtime.id(),
        })
    }

    pub(super) fn runtime(&self) -> Id {
        self.runtime
    }

    pub(super) fn get_ref(&self) -> &socket2::Socket {
        #[cfg(unix)]
        {
            self.inner.get_ref()
        }

        #[cfg(windows)]
        {
            self.inner.get_ref()
        }
    }

    pub(super) async fn io<Output, Operation>(
        &self,
        interest: Interest,
        mut operation: Operation,
    ) -> io::Result<Output>
    where
        Operation: FnMut(&socket2::Socket) -> io::Result<Output> + Send,
    {
        #[cfg(unix)]
        {
            let interest = match interest {
                Interest::Readable => tokio::io::Interest::READABLE,
                Interest::Writable => tokio::io::Interest::WRITABLE,
            };
            self.inner
                .async_io(interest, |_| operation(self.inner.get_ref()))
                .await
        }

        #[cfg(windows)]
        {
            match interest {
                Interest::Readable => self.inner.read_with(|socket| operation(socket)).await,
                Interest::Writable => self.inner.write_with(|socket| operation(socket)).await,
            }
        }
    }

    pub(super) async fn wait(&self, interest: Interest) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.io(interest, |_| Ok(())).await
        }

        #[cfg(windows)]
        {
            match interest {
                Interest::Readable => self.inner.readable().await,
                Interest::Writable => self.inner.writable().await,
            }
        }
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
