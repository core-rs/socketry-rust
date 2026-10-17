// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferResult, Cancellation, FileOptions, IoBuf, IoBufMut, OperationResult, SyncMode};
use std::{future::Future, io::SeekFrom, path::Path};

/// Regular-file creation, synchronization, shared-cursor operations, and explicit-offset operations through the chosen scheduler. The backend retains resources used by accepted operations even when the waiting future is destroyed.
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

    /// Consume an exclusively owned handle, wait for bounded admission and OS close completion, and report admission or close errors. This future is lazy and takes no cancellation token. Close does not synchronize file contents and is never retried after an error.
    ///
    /// Finish all operations using the file first. Requesting cancellation or dropping an operation future alone does not establish that backend access has ended. Once admitted, close runs even if this future is dropped. Before admission, dropping the future releases the handle through ordinary destruction.
    ///
    /// # Panics
    /// Backends may panic if an abandoned operation still retains the file. Close never interrupts or joins other operations.
    fn file_close(&self, file: Self::File) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Consume an exclusively owned handle and wait only for bounded admission. The scheduler then owns completion even if this future is dropped. Errors report admission failure, not OS close failure; await `file_close` when execution errors matter.
    ///
    /// The ownership, laziness, and outstanding-operation preconditions are the same as [`File::file_close`]. Graceful scheduler shutdown drains accepted close jobs.
    fn file_close_submit(
        &self,
        file: Self::File,
    ) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Set the logical file length, discarding bytes when shrinking and filling an extension with zeros. The file must be open for writing. Lengths must fit in `i64`; filesystem limits and other OS errors are reported. The shared cursor is unchanged, even when it ends up past EOF.
    ///
    /// The future is lazy. Cancellation before dispatch prevents submission; blocking workers check again before execution. Once submitted, cancellation is best effort and successful completion wins a concurrent request. The backend retains the file until access ends, and dropping a waiter cannot undo a completed resize.
    ///
    /// Await reads, writes, seeks, and truncation sequentially when ordering matters. This operation does not guarantee disk-space reservation or durability; use `file_sync` after completion when synchronization is needed.
    fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;

    /// Synchronize file contents and selected metadata, waiting for OS completion and reporting errors. This leaves the shared cursor unchanged. Await preceding writes first; this call does not order concurrent operations or flush application buffers.
    ///
    /// The future is lazy. Cancellation before dispatch prevents submission; blocking workers check again before execution. Once submitted, cancellation is best effort, and a successful completion wins a concurrent cancellation request. Dropping the future cannot undo synchronization; the backend retains the file until access ends.
    ///
    /// Durability follows the platform, filesystem, and device guarantees. `SyncMode::Data` may perform full synchronization. Synchronizing a file does not synchronize its parent directory entry.
    fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;

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
