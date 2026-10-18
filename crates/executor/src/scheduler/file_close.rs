// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use std::future::Future;

/// Close exclusively owned file resources with completion or admission-only semantics.
pub trait FileClose: FileOperation {
    /// Consume an exclusively owned handle, wait for bounded admission and OS close completion, and report admission or close errors. This future is lazy and takes no cancellation token. Close does not synchronize file contents and is never retried after an error.
    ///
    /// Finish all operations using the file first. Requesting cancellation or dropping an operation future alone does not establish that backend access has ended. Once admitted, close runs even if this future is dropped. Before admission, dropping the future releases the handle through ordinary destruction.
    ///
    /// # Panics
    /// Backends may panic if an abandoned operation still retains the file. Close never interrupts or joins other operations.
    fn file_close(&self, file: Self::File) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Consume an exclusively owned handle and wait only for bounded admission. The scheduler then owns completion even if this future is dropped. Errors report admission failure, not OS close failure; await `file_close` when execution errors matter.
    ///
    /// The ownership, laziness, and outstanding-operation preconditions are the same as [`FileClose::file_close`]. Graceful scheduler shutdown drains accepted close jobs.
    fn file_close_submit(
        &self,
        file: Self::File,
    ) -> impl Future<Output = std::io::Result<()>> + Send;
}
