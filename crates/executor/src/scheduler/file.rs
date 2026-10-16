// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferResult, Cancellation, FileOptions, IoBuf, IoBufMut, OperationResult};
use std::{future::Future, path::Path};

/// Regular-file creation and positioned operations through the chosen scheduler.
/// The backend chooses its resource representation and retains resources used
/// by accepted operations even when the waiting future is destroyed.
///
/// Offsets must fit in i64. Unix leaves the shared cursor unchanged; the current
/// Windows fallback uses std seek_read/seek_write and changes that cursor.
/// Cancellation before execution prevents I/O. Once a blocking call starts,
/// completion can win cancellation; buffers are returned only after access ends.
pub trait File: Send + Sync {
    type File: Send + Sync;

    fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::File>> + Send;
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
