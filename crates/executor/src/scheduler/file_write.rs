// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use crate::{BufferResult, Cancellation, IoBuf};
use std::future::Future;

/// Write at a file resource's shared cursor. Successful writes advance it by the transferred byte count. Await cursor operations sequentially when ordering matters: concurrent operations have no portable ordering or non-overlap guarantee. Dropping a submitted future cannot roll back transferred bytes or cursor changes.
///
/// Cancellation before execution prevents I/O. Once a blocking call starts, completion can win cancellation; the backend retains the resource and buffer until access ends, including after the waiting future is dropped. Buffers are returned on both success and failure.
pub trait FileWrite: FileOperation {
    /// Write once at the shared cursor, returning a possibly short count and the original buffer. This does not guarantee durability.
    fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
}
