// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::BufferResult;
use std::fs::File as StdFile;
use std::future::Future;
use std::sync::Arc;

/// A scheduler's positioned file operations.
///
/// This trait is implemented by schedulers. File resources are [`std::fs::File`];
/// alias that type as `StdFile` when importing both names.
///
/// A regular file does not support a universal
/// readiness fallback, so implementations use native completion or a blocking
/// pool. Use ordinary files opened without append mode, not pipes. Offsets
/// must fit in i64. The Unix implementation leaves the shared cursor unchanged;
/// the Windows blocking fallback updates it, as std's seek_read/seek_write do.
///
/// Buffers and the file remain owned by an in-flight operation even if the
/// waiting future is dropped. A write can still complete after cancellation.
pub trait File: Send + Sync {
    fn file_read_at(
        &self,
        file: Arc<StdFile>,
        buffer: Vec<u8>,
        offset: u64,
    ) -> impl Future<Output = BufferResult> + Send;

    fn file_write_at(
        &self,
        file: Arc<StdFile>,
        buffer: Vec<u8>,
        offset: u64,
    ) -> impl Future<Output = BufferResult> + Send;
}
