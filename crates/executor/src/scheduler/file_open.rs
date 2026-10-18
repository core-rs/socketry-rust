// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use crate::{Cancellation, FileOptions, OperationResult};
use std::{future::Future, path::Path};

/// Create file resources through the chosen scheduler.
pub trait FileOpen: FileOperation {
    fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::File>> + Send;
}
