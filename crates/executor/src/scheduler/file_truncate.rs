// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use crate::{Cancellation, OperationResult};
use std::future::Future;

/// Change the logical length of an already-open file resource.
pub trait FileTruncate: FileOperation {
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
}
