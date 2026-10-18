// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{Cancellation, Interest, OperationResult};
use std::future::Future;

/// Wait for socket readiness. Readiness is a hint; a following operation can still need to wait.
pub trait SocketWait: SocketOperation {
    fn socket_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
