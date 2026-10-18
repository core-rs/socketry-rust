// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use crate::{BufferResult, Cancellation, IoBuf};
use std::future::Future;

/// Write once to an existing socket, preserving short transfers and datagram boundaries.
pub trait SocketWrite: SocketOperation {
    /// Write once, returning the result and the original buffer. Stream callers handle short transfers; datagram callers submit one complete message per operation.
    fn socket_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
}
