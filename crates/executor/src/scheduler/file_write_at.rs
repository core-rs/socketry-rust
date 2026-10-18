// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use crate::{BufferResult, Cancellation, IoBuf};
use std::future::Future;

/// Write at an explicit byte offset, which must fit in `i64`. Unix implementations leave the shared cursor unchanged; the current Windows fallback uses `std`'s `seek_write` and changes it.
///
/// Cancellation before execution prevents I/O. Once a blocking call starts, completion can win cancellation; the backend retains the resource and buffer until access ends, including after the waiting future is dropped. Buffers are returned on both success and failure.
pub trait FileWriteAt: FileOperation {
    fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
}
