// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use crate::{Cancellation, OperationResult, SyncMode};
use std::future::Future;

/// Synchronize file contents and metadata through the chosen scheduler.
pub trait FileSync: FileOperation {
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
}
