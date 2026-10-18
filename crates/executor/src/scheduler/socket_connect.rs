// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{Cancellation, OperationResult};
use std::{future::Future, net::SocketAddr};

/// Connect an existing socket to a peer IP address.
pub trait SocketConnect: SocketOperation {
    /// Connect an existing socket, preserving any prior local binding or configuration. A cancelled connect can still complete in the kernel; it does not make the descriptor reusable for another connection attempt.
    fn socket_connect(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
