// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{
    FileClose, FileOpen, FileRead, FileReadAt, FileSeek, FileStat, FileSync, FileTruncate,
    FileWrite, FileWriteAt,
};

/// Convenience aggregate for the core file operations provided by a scheduler.
///
/// This trait is implemented automatically for schedulers providing every core operation. Consumers can instead require individual capabilities such as `FileRead + FileWrite`; optional `FileAllocate` and `FileAdvice` remain outside this aggregate.
///
/// Implement `FileOperation` to select the resource type, `FileStat` to select its metadata representation, and each operation trait to provide its methods. Import the operation traits for method calls on concrete scheduler values; a generic `S: FileScheduler` bound includes all core methods.
///
/// Optional platform capabilities are not implied by this aggregate:
///
/// ```compile_fail,E0599
/// use socketry_executor::{AdviceMode, FileScheduler, FileAdvice};
/// async fn advise<S: FileScheduler>(scheduler: &S, file: &S::File) {
///     scheduler.file_advise(file, 0, 0, AdviceMode::Normal).await;
/// }
/// ```
///
/// ```compile_fail,E0599
/// use socketry_executor::{AllocationMode, Cancellation, FileScheduler, FileAllocate};
/// async fn allocate<S: FileScheduler>(scheduler: &S, file: &S::File) {
///     scheduler.file_allocate(file, 0, 1, AllocationMode::Extend, &Cancellation::never()).await;
/// }
/// ```
pub trait FileScheduler:
    FileOpen
    + FileClose
    + FileStat
    + FileTruncate
    + FileSync
    + FileSeek
    + FileRead
    + FileWrite
    + FileReadAt
    + FileWriteAt
{
}

impl<S: ?Sized> FileScheduler for S where
    S: FileOpen
        + FileClose
        + FileStat
        + FileTruncate
        + FileSync
        + FileSeek
        + FileRead
        + FileWrite
        + FileReadAt
        + FileWriteAt
{
}
