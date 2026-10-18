// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{SocketGetOption, SocketSetOption};
use crate::SocketOption;

/// Convenience aggregate of `SocketGetOption<O>` and `SocketSetOption<O>`, automatically implemented for schedulers providing both operations for an option. Generic consumers require only the options they use, such as `S: SocketOptions<TcpNoDelay>`.
///
/// An absent implementation is a compile-time error. Socket type, protocol, state, and OS policy can still reject an operation at runtime; preserve those errors. Options retain their OS semantics, including address reuse and inheritance on accepted sockets. Read-only and write-only controls implement only their respective operation trait. Differently shaped controls can use separate extension traits.
///
/// Both futures are lazy and check cancellation before the syscall. Once executed, the syscall's result wins concurrent cancellation; dropping an unpolled setter changes nothing. Await setters before operations that depend on them. There is no extra serialization between concurrent option operations, and get-followed-by-set is not atomic.
///
/// Option values are checked at compile time:
/// ```compile_fail
/// use socketry_executor::{Cancellation, SocketOptions, TcpNoDelay};
/// async fn configure<S: SocketOptions<TcpNoDelay>>(scheduler: &S, socket: &S::Socket) {
///     scheduler.socket_set_option(socket, TcpNoDelay, 1_u32, &Cancellation::never()).await;
/// }
/// ```
/// Merely supporting sockets does not imply support for every option:
/// ```compile_fail
/// use socketry_executor::{Cancellation, SocketScheduler, SocketSetOption, TcpNoDelay};
/// async fn configure<S: SocketScheduler>(scheduler: &S, socket: &S::Socket) {
///     scheduler.socket_set_option(socket, TcpNoDelay, true, &Cancellation::never()).await;
/// }
/// ```
pub trait SocketOptions<O: SocketOption>: SocketGetOption<O> + SocketSetOption<O> {}

impl<S: ?Sized, O: SocketOption> SocketOptions<O> for S where
    S: SocketGetOption<O> + SocketSetOption<O>
{
}
