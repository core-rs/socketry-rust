// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{BufferResult, Cancellation, IoBufMut};
use std::future::Future;

/// Read once from an existing socket, preserving short transfers and datagram boundaries.
///
/// A consumer can read an existing connection without opening, accepting, writing, or closing sockets:
///
/// ```
/// use socketry_executor::{BufferResult, Cancellation, SocketRead};
/// async fn read_chunk<S: SocketRead>(scheduler: &S, socket: &S::Socket) -> BufferResult {
///     scheduler.socket_read(socket, vec![0; 16], &Cancellation::never()).await
/// }
/// ```
///
/// Writing needs an explicit additional capability, even when its trait is imported:
///
/// ```compile_fail,E0599
/// use socketry_executor::{Cancellation, SocketRead, SocketWrite};
/// async fn write<S: SocketRead>(scheduler: &S, socket: &S::Socket) {
///     scheduler.socket_write(socket, vec![1], &Cancellation::never()).await;
/// }
/// ```
pub trait SocketRead: SocketOperation {
    /// Read once, returning the result and the original buffer. Short transfers are preserved. For datagrams, a small buffer can truncate and discard the remainder of a message.
    fn socket_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
}
