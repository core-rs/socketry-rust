// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::File;
use crate::{AllocationMode, Cancellation, OperationResult};
use std::future::Future;

/// Optional file-space reservation capability, with Linux `fallocate` semantics for ordinary allocation and `KEEP_SIZE`.
///
/// Native and Tokio implementations currently provide this capability on Linux. Other targets omit the implementation, allowing generic consumers to require support at compile time. Filesystem and per-file support still vary at runtime; their errors are preserved. Allocation is not emulated by resizing or writing zeros.
pub trait FileAllocate: File {
    /// Reserve backing space for `[offset, offset + length)` on a writable file. `Extend` grows the logical length when necessary; `KeepSize` preserves it. Both modes preserve existing contents and the shared cursor. Subsequent writes in the reserved range have the underlying filesystem's allocation guarantee against running out of space.
    ///
    /// Length must be positive and the range end must fit in `i64`; invalid ranges return `InvalidInput`. Filesystem limits and errors, including unsupported allocation, are reported. This does not unshare copy-on-write extents, guarantee durability, or order concurrent file operations. Await related operations sequentially and use `file_sync` for synchronization.
    ///
    /// The future is lazy. Cancellation before dispatch prevents submission, and blocking workers check again before execution. Started blocking operations preserve their actual result; native cancellation is best effort, with successful completion winning concurrent cancellation. The backend retains the file until access ends even if the waiting future is dropped. Errors or cancellation do not promise rollback of partial filesystem changes.
    fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
