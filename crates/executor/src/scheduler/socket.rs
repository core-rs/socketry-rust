// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferResult, Cancellation, Interest, IoBuf, IoBufMut, OperationResult};
use std::{future::Future, io, net::SocketAddr};

/// Portable TCP operations through the chosen scheduler. Resources have
/// backend-defined representations; OS resource importing is a separate
/// `SocketRegistration` capability. This initial TCP interface combines
/// creation/bind/listen; general socket types and options remain extensions.
///
/// Pre-cancelled operations do not start. Readiness cancellation stops waiting;
/// completion backends retain their buffers until the original completion.
/// Successful progress wins a racing cancellation. Dropping the waiting future
/// abandons its result and does not undo I/O already submitted to the kernel.
pub trait Socket: Send + Sync {
    type Socket: Send + Sync;
    type Listener: Send + Sync;

    fn socket_listen(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Listener>> + Send;
    fn listener_addr(&self, listener: &Self::Listener) -> io::Result<SocketAddr>;
    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr>;
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr>;
    fn connect(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Socket>> + Send;
    fn accept(
        &self,
        listener: &Self::Listener,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<(Self::Socket, SocketAddr)>> + Send;
    fn io_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
    fn io_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
    fn io_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
