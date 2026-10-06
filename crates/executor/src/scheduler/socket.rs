// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{BufferResult, Interest};
use std::future::Future;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

/// Portable socket operations, selected through the concrete implementation.
///
/// This trait is implemented by schedulers. Its associated [`Socket::Socket`]
/// and [`Socket::Listener`] types represent their registered resources.
///
/// Registrations belong to their creating implementation. Keep a socket's
/// registration across operations and worker migration. Implementations must
/// return an error when a resource belongs to an incompatible runtime instance.
///
/// Dropping an operation future abandons its result. It need not undo an I/O
/// operation already submitted to the kernel: a cancelled read can consume
/// bytes, and a cancelled write can transmit bytes. Implementations retain any
/// kernel-accessible memory until the operation finishes. Await completion when
/// the amount transferred matters. No asynchronous cleanup is promised by Drop.
pub trait Socket: Send + Sync {
    type Socket: Send + Sync;
    type Listener: Send + Sync;

    /// Register an owned socket once. The implementation sets nonblocking mode
    /// when required. Do not change its mode through another OS handle.
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket>;

    /// Register an owned listener once, with the same mode requirements.
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Listener>;

    fn connect(&self, address: SocketAddr)
    -> impl Future<Output = io::Result<Self::Socket>> + Send;

    fn accept(
        &self,
        listener: &Self::Listener,
    ) -> impl Future<Output = io::Result<(Self::Socket, SocketAddr)>> + Send;

    fn io_read(
        &self,
        socket: &Self::Socket,
        buffer: Vec<u8>,
    ) -> impl Future<Output = BufferResult> + Send;

    fn io_write(
        &self,
        socket: &Self::Socket,
        buffer: Vec<u8>,
    ) -> impl Future<Output = BufferResult> + Send;

    fn io_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
    ) -> impl Future<Output = io::Result<()>> + Send;
}
