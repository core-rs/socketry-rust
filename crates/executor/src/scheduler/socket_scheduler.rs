// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{
    SocketAccept, SocketBind, SocketClose, SocketConnect, SocketListen, SocketLocalAddress,
    SocketOpen, SocketPeerAddress, SocketRead, SocketShutdown, SocketWait, SocketWrite,
};

/// Convenience aggregate for the core socket operations provided by a scheduler.
///
/// Implemented automatically for schedulers supplying every core socket capability. Consumers can instead require individual traits such as `SocketRead + SocketWrite`. Message I/O, typed options, and standard-library resource registration remain optional.
///
/// Implement `SocketOperation` to select the resource type, then each operation trait to supply its methods. Import individual operation traits for concrete method calls; a generic `S: SocketScheduler` bound includes every core method. See `SocketOperation` for the shared resource and cancellation contract.
///
/// Standard-library resource import remains an explicit optional capability:
///
/// ```compile_fail,E0599
/// use socketry_executor::{SocketRegistration, SocketScheduler};
/// fn register<S: SocketScheduler>(scheduler: &S, socket: std::net::TcpStream) {
///     scheduler.register_socket(socket);
/// }
/// ```
pub trait SocketScheduler:
    SocketClose
    + SocketShutdown
    + SocketOpen
    + SocketBind
    + SocketListen
    + SocketLocalAddress
    + SocketPeerAddress
    + SocketConnect
    + SocketAccept
    + SocketRead
    + SocketWrite
    + SocketWait
{
}

impl<S: ?Sized> SocketScheduler for S where
    S: SocketClose
        + SocketShutdown
        + SocketOpen
        + SocketBind
        + SocketListen
        + SocketLocalAddress
        + SocketPeerAddress
        + SocketConnect
        + SocketAccept
        + SocketRead
        + SocketWrite
        + SocketWait
{
}
