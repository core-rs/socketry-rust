// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{Cancellation, OperationResult, SocketOption};
use std::future::Future;

/// Read a typed socket option. See [`SocketOptions`](crate::SocketOptions) for the shared option contract; the opposite operation is not required.
///
/// A getter-only adapter needs neither a setter nor any core socket operations:
///
/// ```
/// use socketry_executor::{Cancellation, OperationResult, SocketGetOption, SocketOperation, SocketOption};
/// struct Inspector<S>(S);
/// impl<S: SocketOperation> SocketOperation for Inspector<S> {
///     type Socket = S::Socket;
/// }
/// impl<S: SocketGetOption<O>, O: SocketOption> SocketGetOption<O> for Inspector<S> {
///     async fn socket_get_option(&self, socket: &Self::Socket, option: O, cancellation: &Cancellation) -> OperationResult<O::Value> {
///         self.0.socket_get_option(socket, option, cancellation).await
///     }
/// }
/// ```
///
/// Reading an option does not grant permission to set it:
///
/// ```compile_fail,E0599
/// use socketry_executor::{Cancellation, SocketGetOption, SocketSetOption, TcpNoDelay};
/// async fn configure<S: SocketGetOption<TcpNoDelay>>(scheduler: &S, socket: &S::Socket) {
///     scheduler.socket_set_option(socket, TcpNoDelay, true, &Cancellation::never()).await;
/// }
/// ```
pub trait SocketGetOption<O: SocketOption>: SocketOperation {
    /// Query the option's current value, preserving OS errors.
    fn socket_get_option(
        &self,
        socket: &Self::Socket,
        option: O,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<O::Value>> + Send;
}
