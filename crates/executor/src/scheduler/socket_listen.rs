// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{Cancellation, OperationResult};
use std::future::Future;

/// Put an existing socket into listening mode.
pub trait SocketListen: SocketOperation {
    /// Put an existing socket into listening mode using the supplied backlog. This does not create or explicitly bind the socket. OS-specific implicit binding, backlog limits, and repeated-listen behavior are preserved.
    fn socket_listen(
        &self,
        socket: &Self::Socket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
