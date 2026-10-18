// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{Cancellation, OperationResult, SocketDomain, SocketProtocol, SocketType};
use std::future::Future;

/// Create an unbound IP socket through the scheduler.
pub trait SocketOpen: SocketOperation {
    /// Open a nonblocking socket without binding, listening, or connecting. `None` selects the OS default protocol for the requested domain and type.
    fn socket_open(
        &self,
        domain: SocketDomain,
        kind: SocketType,
        protocol: Option<SocketProtocol>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Socket>> + Send;
}
