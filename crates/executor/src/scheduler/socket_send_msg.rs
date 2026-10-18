// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{Cancellation, IoBuf, OperationResult, SocketMessage, SocketSendFlags};
use std::future::Future;

/// Send one scatter/gather message through an existing socket. Buffer owners are returned on every result. See [`SocketMessages`](crate::SocketMessages) for the shared message I/O contract.
///
/// Message sending does not imply message receiving:
///
/// ```compile_fail,E0599
/// use socketry_executor::{Cancellation, SocketRecvFlags, SocketRecvMsg, SocketSendMsg};
/// async fn receive<S: SocketSendMsg>(scheduler: &S, socket: &S::Socket) {
///     scheduler.socket_recvmsg(socket, vec![vec![0; 16]], SocketRecvFlags::default(), &Cancellation::never()).await;
/// }
/// ```
pub trait SocketSendMsg: SocketOperation {
    fn socket_sendmsg<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = (OperationResult<usize>, SocketMessage<B>)> + Send;
}
