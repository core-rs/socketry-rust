// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{Cancellation, OperationResult, SocketAddress};
use std::future::Future;

/// Accept one connection from a listening socket.
pub trait SocketAccept: SocketOperation {
    /// Accept one connection from a listening socket, returning an owned nonblocking socket and its peer address.
    fn socket_accept(
        &self,
        socket: &Self::Socket,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<(Self::Socket, SocketAddress)>> + Send;
}
