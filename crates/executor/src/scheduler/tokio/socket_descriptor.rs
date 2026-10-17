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
        Self::new_with(
            runtime,
            socket,
            |socket| socket.set_nonblocking(true),
            |runtime, socket| {
                #[cfg(unix)]
                {
                    let _scope = runtime.enter();
                    RegisteredSocket::new(socket)
                }

                #[cfg(windows)]
                {
                    let _ = runtime;
                    RegisteredSocket::new(socket)
                }
            },
        )
    }

    fn new_with(
        runtime: &Handle,
        socket: socket2::Socket,
        set_nonblocking: impl FnOnce(&socket2::Socket) -> io::Result<()>,
        register: impl FnOnce(&Handle, socket2::Socket) -> io::Result<RegisteredSocket>,
    ) -> io::Result<Self> {
        set_nonblocking(&socket)?;
        let inner = register(runtime, socket)?;

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
        // Try the syscall before waiting so invalid socket states return their
        // immediate OS error instead of waiting for readiness that cannot occur.
        let result = operation(self.get_ref());
        if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
            return result;
        }

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
            match interest {
                Interest::Readable => self.inner.readable().await.map(|_| ()),
                Interest::Writable => self.inner.writable().await.map(|_| ()),
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::{Future, poll_fn},
        pin::pin,
        task::Poll,
        time::Duration,
    };

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn socket() -> socket2::Socket {
        socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap()
    }

    #[test]
    fn nonblocking_configuration_errors_are_returned() {
        let runtime = runtime();
        let error = SocketDescriptor::new_with(
            runtime.handle(),
            socket(),
            |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
            |_, _| unreachable!("socket registration follows nonblocking configuration"),
        )
        .err()
        .unwrap();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn registration_errors_are_returned() {
        let runtime = runtime();
        let error = SocketDescriptor::new_with(
            runtime.handle(),
            socket(),
            |_| Ok(()),
            |_, _| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        )
        .err()
        .unwrap();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn invalid_accept_returns_without_waiting_for_readiness() {
        let runtime = runtime();
        let socket =
            runtime.block_on(async { SocketDescriptor::new(runtime.handle(), socket()).unwrap() });

        let result = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(1),
                socket.io(Interest::Readable, |socket| socket.accept().map(|_| ())),
            )
            .await
        });

        assert!(
            result
                .expect("invalid accept waited for readiness")
                .is_err()
        );
    }

    #[test]
    fn wait_does_not_complete_before_readiness() {
        let runtime = runtime();
        let socket =
            runtime.block_on(async { SocketDescriptor::new(runtime.handle(), socket()).unwrap() });

        runtime.block_on(async {
            let mut wait = pin!(socket.wait(Interest::Readable));
            poll_fn(|context| {
                assert!(wait.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
        });
    }
}
