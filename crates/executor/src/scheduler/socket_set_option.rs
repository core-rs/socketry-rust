// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{Cancellation, OperationResult, SocketOption};
use std::future::Future;

/// Set a typed socket option. See [`SocketOptions`](crate::SocketOptions) for the shared option contract; the opposite operation is not required.
pub trait SocketSetOption<O: SocketOption>: SocketOperation {
    /// Set the option and wait for syscall completion. This borrows the descriptor and does not change its ownership.
    fn socket_set_option(
        &self,
        socket: &Self::Socket,
        option: O,
        value: O::Value,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
