// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use crate::{Cancellation, FileMetadata, OperationResult};
use std::future::Future;

/// Query owned metadata for an already-open file resource.
pub trait FileStat: FileOperation {
    /// Owned metadata representation chosen by this implementation.
    type Metadata: FileMetadata;

    /// Query the already-open resource and return owned metadata without moving its cursor. The result is an observation, not an atomic snapshot: await preceding modifications when ordering matters. Path lookup and symlink-following policy belong to resource creation, not this operation.
    ///
    /// The future is lazy. Cancellation before dispatch prevents submission; blocking workers check again before execution. Started blocking calls preserve their actual result. Ring cancellation is best effort and waits for original completion. The backend retains the file until access ends, even if the waiting future is dropped.
    fn file_stat(
        &self,
        file: &Self::File,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Metadata>> + Send;
}
