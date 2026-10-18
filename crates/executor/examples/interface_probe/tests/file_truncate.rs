// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Cancellation, FileOptions, IoBuf, OperationError, SyncMode};
use socketry_executor::{FileOpen, FileRead, FileSeek, FileSync, FileTruncate, FileWrite};
use std::{
    io::{self, SeekFrom},
    path::Path,
};

static NEVER: Cancellation = Cancellation::never();

async fn position<S: FileSeek>(scheduler: &S, file: &S::File) -> u64 {
    scheduler
        .file_seek(file, SeekFrom::Current(0), &NEVER)
        .await
        .unwrap()
}

async fn contents<S: FileOpen + FileRead>(scheduler: &S, path: &Path) -> Vec<u8> {
    let reader = scheduler
        .file_open(path, FileOptions::READ, &NEVER)
        .await
        .unwrap();
    let mut bytes = Vec::new();
    loop {
        let (result, buffer) = scheduler.file_read(&reader, vec![0; 16], &NEVER).await;
        let count = result.unwrap();
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    bytes
}

pub async fn check<S: FileOpen + FileRead + FileSeek + FileSync + FileTruncate + FileWrite>(
    scheduler: &S,
    root: &Path,
) {
    let path = root.join("truncate");
    let file = scheduler
        .file_open(
            &path,
            FileOptions {
                read: true,
                ..FileOptions::CREATE
            },
            &NEVER,
        )
        .await
        .unwrap();
    scheduler.file_truncate(&file, 0, &NEVER).await.unwrap();
    let mut buffer = b"abcdef".to_vec();
    let mut offset = 0;
    while offset < buffer.len() {
        let (result, returned) = scheduler
            .file_write(&file, buffer.slice(offset..), &NEVER)
            .await;
        let count = result.unwrap();
        assert!(count > 0);
        offset += count;
        buffer = returned.into_inner();
    }
    assert_eq!(position(scheduler, &file).await, 6);

    scheduler.file_truncate(&file, 3, &NEVER).await.unwrap();
    assert_eq!(position(scheduler, &file).await, 6);
    assert_eq!(
        scheduler.file_read(&file, vec![1], &NEVER).await.0.unwrap(),
        0
    );
    assert_eq!(contents(scheduler, &path).await, b"abc");

    // Shrinking and re-extending must not expose the discarded suffix.
    scheduler.file_truncate(&file, 8, &NEVER).await.unwrap();
    assert_eq!(position(scheduler, &file).await, 6);
    let (result, buffer) = scheduler.file_read(&file, vec![255], &NEVER).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, [0]);
    assert_eq!(contents(scheduler, &path).await, b"abc\0\0\0\0\0");
    scheduler
        .file_seek(&file, SeekFrom::Start(1), &NEVER)
        .await
        .unwrap();
    scheduler.file_truncate(&file, 8, &NEVER).await.unwrap();
    assert_eq!(position(scheduler, &file).await, 1);
    let (result, buffer) = scheduler.file_read(&file, vec![0], &NEVER).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, b"b");

    scheduler.file_truncate(&file, 0, &NEVER).await.unwrap();
    assert_eq!(position(scheduler, &file).await, 2);
    assert_eq!(contents(scheduler, &path).await, b"");
    let (result, _) = scheduler.file_write(&file, b"Z".to_vec(), &NEVER).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(contents(scheduler, &path).await, b"\0\0Z");

    for length in [i64::MAX as u64 + 1, u64::MAX] {
        assert_eq!(
            scheduler
                .file_truncate(&file, length, &NEVER)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(position(scheduler, &file).await, 3);
        assert_eq!(contents(scheduler, &path).await, b"\0\0Z");
    }
    drop(scheduler.file_truncate(&file, 0, &NEVER));
    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        scheduler.file_truncate(&file, 0, &cancelled).await,
        Err(OperationError::Cancelled(_))
    ));
    let stopping = Cancellation::new();
    let unpolled = scheduler.file_truncate(&file, 0, &stopping);
    stopping.cancel();
    assert!(matches!(unpolled.await, Err(OperationError::Cancelled(_))));
    assert_eq!(position(scheduler, &file).await, 3);
    assert_eq!(contents(scheduler, &path).await, b"\0\0Z");

    let reader = scheduler
        .file_open(&path, FileOptions::READ, &NEVER)
        .await
        .unwrap();
    // Write permission is required even when the requested length is unchanged.
    for length in [0, 3, 5] {
        assert!(matches!(
            scheduler.file_truncate(&reader, length, &NEVER).await,
            Err(OperationError::Io(_))
        ));
        assert_eq!(position(scheduler, &reader).await, 0);
        assert_eq!(contents(scheduler, &path).await, b"\0\0Z");
    }
    let writer = scheduler
        .file_open(
            &path,
            FileOptions {
                write: true,
                ..Default::default()
            },
            &NEVER,
        )
        .await
        .unwrap();
    scheduler.file_truncate(&writer, 5, &NEVER).await.unwrap();
    assert_eq!(position(scheduler, &writer).await, 0);
    assert_eq!(position(scheduler, &file).await, 3);
    assert_eq!(contents(scheduler, &path).await, b"\0\0Z\0\0");
    scheduler
        .file_sync(&writer, SyncMode::All, &NEVER)
        .await
        .unwrap();
}
