// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use crate::{Cancellation, OperationResult};
use std::{future::Future, io::SeekFrom};

/// Inspect or reposition a file resource's shared cursor.
pub trait FileSeek: FileOperation {
    /// Reposition the shared cursor and return its resulting byte offset. `SeekFrom::Current(0)` queries the position; seeking past EOF does not extend the file. Absolute offsets must fit in `i64`; invalid positions and OS limits return errors.
    ///
    /// Await cursor operations sequentially when ordering matters. A seek followed by a read or write is not atomic with respect to other users of the resource. Cancellation before execution prevents seeking; once started, the actual result is preserved, and dropping the future does not undo the seek.
    fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<u64>> + Send;
}
