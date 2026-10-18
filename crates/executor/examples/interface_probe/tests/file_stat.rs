// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Cancellation, FileMetadata, FileOptions, FileType, OperationError};
use socketry_executor::{FileClose, FileOpen, FileSeek, FileStat, FileTruncate};
use std::{io::SeekFrom, path::Path};

static NEVER: Cancellation = Cancellation::never();

pub async fn check<S: FileClose + FileOpen + FileSeek + FileStat + FileTruncate>(
    scheduler: &S,
    root: &Path,
) {
    let path = root.join("stat");
    let file = scheduler
        .file_open(&path, FileOptions::CREATE, &NEVER)
        .await
        .unwrap();
    let initial = scheduler.file_stat(&file, &NEVER).await.unwrap();
    assert_eq!(initial.file_type(), Some(FileType::File));
    assert_eq!(initial.size(), Some(0));
    // Reading timestamps must work for both native and simulated metadata, including absent values. Availability and update policy are platform/filesystem-dependent.
    let _ = (initial.accessed(), initial.modified(), initial.created());
    scheduler.file_truncate(&file, 23, &NEVER).await.unwrap();
    scheduler
        .file_seek(&file, SeekFrom::Start(7), &NEVER)
        .await
        .unwrap();
    let resized = scheduler.file_stat(&file, &NEVER).await.unwrap();
    assert_eq!(resized.size(), Some(23));
    assert_eq!(initial.size(), Some(0));
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Current(0), &NEVER)
            .await
            .unwrap(),
        7
    );
    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        scheduler.file_stat(&file, &cancelled).await,
        Err(OperationError::Cancelled(_))
    ));
    // An unpolled query is inert; a completed query releases all backend references, permitting immediate exclusive close.
    drop(scheduler.file_stat(&file, &NEVER));
    scheduler.file_close(file).await.unwrap();
    let reader = scheduler
        .file_open(&path, FileOptions::READ, &NEVER)
        .await
        .unwrap();
    let observed = scheduler.file_stat(&reader, &NEVER).await.unwrap();
    scheduler.file_close(reader).await.unwrap();
    // Metadata owns its result, not a borrow of the descriptor, and querying does not require write access.
    assert_eq!(observed.size(), Some(23));
    assert_eq!(observed.file_type(), Some(FileType::File));
}
