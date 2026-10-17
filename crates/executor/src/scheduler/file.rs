// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferResult, Cancellation, FileOptions, IoBuf, IoBufMut, OperationResult};
use std::{future::Future, io::SeekFrom, path::Path};

/// Regular-file creation, shared-cursor operations, and explicit-offset operations through the chosen scheduler. The backend retains resources used by accepted operations even when the waiting future is destroyed.
///
/// `file_read` and `file_write` use and advance the resource's shared cursor by the transferred byte count. Await operations sequentially when ordering matters: concurrent operations have no portable ordering or non-overlap guarantee. Dropping a submitted future does not roll back transferred bytes or cursor changes.
///
/// Explicit offsets must fit in `i64`. On Unix, positioned operations leave the shared cursor unchanged; the current Windows fallback uses `std`'s `seek_read`/`seek_write` and changes it. Cancellation before execution prevents I/O. Once a blocking call starts, completion can win cancellation; buffers are returned only after access ends.
pub trait File: Send + Sync {
    type File: Send + Sync;

    fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::File>> + Send;

    /// Reposition the shared cursor and return its resulting byte offset. `SeekFrom::Current(0)` queries the position; seeking past EOF does not extend the file. Absolute offsets must fit in `i64`; invalid positions and OS limits return errors.
    ///
    /// Await cursor operations sequentially when ordering matters. A seek followed by a read or write is not atomic with respect to other users of the resource. Cancellation before execution prevents seeking; once started, the actual result is preserved, and dropping the future does not undo the seek.
    fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<u64>> + Send;

    /// Read once from the shared cursor, returning a possibly short count and the original buffer.
    fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    /// Write once at the shared cursor, returning a possibly short count and the original buffer. This does not guarantee durability.
    fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
    fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
}
