// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{Cancellation, OperationResult, SocketAddress};
use std::future::Future;

/// Bind an existing socket to a local IP address.
pub trait SocketBind: SocketOperation {
    /// Bind an existing socket to a local address. Port zero requests an OS-selected port, available through `socket_local_address`.
    fn socket_bind(
        &self,
        socket: &Self::Socket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
