// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use crate::{BufferResult, Cancellation, IoBufMut};
use std::future::Future;

/// Read from a file resource's shared cursor. Successful reads advance it by the transferred byte count. Await cursor operations sequentially when ordering matters: concurrent operations have no portable ordering or non-overlap guarantee. Dropping a submitted future cannot roll back transferred bytes or cursor changes.
///
/// Cancellation before execution prevents I/O. Once a blocking call starts, completion can win cancellation; the backend retains the resource and buffer until access ends, including after the waiting future is dropped. Buffers are returned on both success and failure.
///
/// A read-only consumer needs no creation, writing, metadata, or close capability:
///
/// ```
/// use socketry_executor::{BufferResult, Cancellation, FileRead};
/// async fn read_chunk<S: FileRead>(scheduler: &S, file: &S::File) -> BufferResult {
///     scheduler.file_read(file, vec![0; 16], &Cancellation::never()).await
/// }
/// ```
///
/// Writing requires an explicit additional bound, even with the trait in scope:
///
/// ```compile_fail,E0599
/// use socketry_executor::{Cancellation, FileRead, FileWrite};
/// async fn write<S: FileRead>(scheduler: &S, file: &S::File) {
///     scheduler.file_write(file, vec![1], &Cancellation::never()).await;
/// }
/// ```
pub trait FileRead: FileOperation {
    /// Read once from the shared cursor, returning a possibly short count and the original buffer.
    fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
}
