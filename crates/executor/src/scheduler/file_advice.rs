// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::FileOperation;
use crate::AdviceMode;
use std::{future::Future, io};

/// Optional file advice capability, following Linux `posix_fadvise` semantics.
///
/// Native and Tokio implementations currently provide this capability on Linux. Other targets omit the implementation so consumers can require it at compile time. The OS may ignore advice; success promises neither prefetch completion nor eviction. Filesystem-specific errors are preserved.
///
/// An advice-only adapter does not need to forward the core file operations:
///
/// ```
/// use socketry_executor::{AdviceMode, FileAdvice, FileOperation};
/// struct AdviceOnly<S>(S);
/// impl<S: FileAdvice> FileOperation for AdviceOnly<S> {
///     type File = S::File;
/// }
/// impl<S: FileAdvice> FileAdvice for AdviceOnly<S> {
///     async fn file_advise(&self, file: &Self::File, offset: u64, length: u64, advice: AdviceMode) -> std::io::Result<()> {
///         self.0.file_advise(file, offset, length, advice).await
///     }
///     async fn file_advise_submit(&self, file: &Self::File, offset: u64, length: u64, advice: AdviceMode) -> std::io::Result<()> {
///         self.0.file_advise_submit(file, offset, length, advice).await
///     }
/// }
/// ```
pub trait FileAdvice: FileOperation {
    /// Apply an access or cache hint and await OS completion, reporting admission and execution errors. A zero length means from `offset` through EOF; otherwise the range is `[offset, offset + length)`. The offset and range end must fit in `i64`, or admission fails with `InvalidInput`.
    ///
    /// Access-policy hints may affect the whole open file description rather than only the supplied range.
    ///
    /// The future is lazy and has no cancellation input. Polling waits for bounded admission, then retains the file until execution ends. After admission the job survives waiter abandonment and is drained by graceful scheduler shutdown. Await this form before explicitly closing the descriptor; advice preserves contents, size, and cursor, but does not order other operations or guarantee durability.
    fn file_advise(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: AdviceMode,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Admit the same operation as `file_advise`, returning before execution necessarily starts or finishes. Only validation and admission errors are reported; execution errors are discarded. This is an alternative to `file_advise`, not a second step after it.
    ///
    /// Constructing an unpolled future submits nothing; dropping a future still waiting for capacity abandons admission. Once accepted, the scheduler retains the file and completes the job even if the caller drops its descriptor. Explicit close requires completed work, so use `file_advise` when that completion boundary is needed.
    fn file_advise_submit(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: AdviceMode,
    ) -> impl Future<Output = io::Result<()>> + Send;
}
