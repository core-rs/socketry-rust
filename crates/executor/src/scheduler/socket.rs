// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    BufferResult, Cancellation, Interest, IoBuf, IoBufMut, OperationResult, SocketDomain,
    SocketProtocol, SocketType,
};
use std::{future::Future, io, net::SocketAddr};

/// IP socket operations through the chosen scheduler. Each backend supplies one descriptor type for unconnected, listening, and connected sockets. Operations have the same state requirements as their system calls; invalid transitions report OS errors.
///
/// Creation, binding, listening, and connecting are separate operations. All operation futures are lazy. Pre-cancellation prevents a syscall; successful progress wins a cancellation race. Cancelling or abandoning a pending connect does not roll back kernel connection state: discard that socket before trying another connection. Completion backends retain owned buffers until the original operation completes.
///
/// File and socket resources remain distinct. There is no common descriptor trait, protocol buffering, or automatic close coordination. Non-IP families and message/address APIs remain extensions.
pub trait Socket: Send + Sync {
    type Socket: Send + Sync;

    /// Open a nonblocking socket without binding, listening, or connecting. `None` selects the OS default protocol for the requested domain and type.
    fn socket_open(
        &self,
        domain: SocketDomain,
        kind: SocketType,
        protocol: Option<SocketProtocol>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Socket>> + Send;

    /// Bind an existing socket to a local address. Port zero requests an OS-selected port, available through `socket_local_addr`.
    fn socket_bind(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;

    /// Put an existing socket into listening mode using the supplied backlog. This does not create or explicitly bind the socket. OS-specific implicit binding, backlog limits, and repeated-listen behavior are preserved.
    fn socket_listen(
        &self,
        socket: &Self::Socket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;

    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr>;
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr>;

    /// Connect an existing socket, preserving any prior local binding or configuration. A cancelled connect can still complete in the kernel; it does not make the descriptor reusable for another connection attempt.
    fn socket_connect(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;

    /// Accept one connection from a listening socket, returning an owned nonblocking socket and its peer address.
    fn socket_accept(
        &self,
        socket: &Self::Socket,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<(Self::Socket, SocketAddr)>> + Send;

    /// Read once, returning the result and the original buffer. Short transfers are preserved. For datagrams, a small buffer can truncate and discard the remainder of a message.
    fn socket_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    /// Write once, returning the result and the original buffer. Stream callers handle short transfers; datagram callers submit one complete message per operation.
    fn socket_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    fn socket_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
