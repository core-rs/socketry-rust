// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    Cancellation, IoBuf, IoBufMut, OperationResult, Socket, SocketMessage, SocketRecvFlags,
    SocketRecvResult, SocketSendFlags,
};
use std::future::Future;

/// Scatter/gather message I/O with IP addresses. Generic consumers require this capability separately from `Socket`.
///
/// Operations are lazy, check cancellation before I/O, and return the original buffer owners on every result. Cancellation can race successful progress; success wins. Completion backends retain payload and native metadata until the original operation's terminal completion, including after waiter abandonment. Cancel and await outstanding operations before explicit close.
///
/// Each call performs one transfer, preserving short stream transfers and datagram boundaries. Empty buffer lists still perform a syscall, allowing empty datagram sends. Zero-capacity receives preserve OS behavior, which can differ in whether data is consumed or an address is returned; supply nonempty storage to receive empty datagrams portably. The sum of initialized buffer lengths must fit in `i32::MAX`; the OS may impose a smaller message or vector-count limit. Buffers are filled in order and are never resized. No implicit send-all, receive-exact, serialization, or protocol framing is provided.
///
/// This capability carries payload and IP addresses only. It cannot send ancillary data and supplies no receive control storage. Unix reports `MSG_CTRUNC` through the result; the Windows payload/address fallback cannot observe control metadata. Ancillary data requires a separate capability rather than silent emulation. Unsupported flags are not part of this portable interface.
/// A socket backend must explicitly implement message support:
/// ```compile_fail
/// use socketry_executor::{Cancellation, Socket, SocketMessages, SocketRecvFlags};
/// async fn receive<S: Socket>(scheduler: &S, socket: &S::Socket) {
///     scheduler.socket_recvmsg(socket, vec![vec![0; 1024]], SocketRecvFlags::default(), &Cancellation::never()).await;
/// }
/// ```
pub trait SocketMessages: Socket {
    fn socket_sendmsg<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = (OperationResult<usize>, SocketMessage<B>)> + Send;

    /// Receive one message into owned buffers. `WSAEMSGSIZE` on Windows is represented as a successful truncated receive, matching Unix; other OS errors are preserved. A zero-byte datagram is a message, whereas a zero-byte stream receive with nonempty capacity normally indicates EOF.
    fn socket_recvmsg<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffers: Vec<B>,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = (OperationResult<SocketRecvResult>, Vec<B>)> + Send;
}
