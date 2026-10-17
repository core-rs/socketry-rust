// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{
    AllocationMode, Cancellation, File, FileAllocate, FileOptions, OperationError,
};
use std::{
    io::{self, SeekFrom},
    path::Path,
};

static NEVER: Cancellation = Cancellation::never();

async fn contents<S: File>(scheduler: &S, path: &Path) -> Vec<u8> {
    let reader = scheduler
        .file_open(path, FileOptions::READ, &NEVER)
        .await
        .unwrap();
    let mut bytes = Vec::new();
    loop {
        let (result, buffer) = scheduler.file_read(&reader, vec![255; 16], &NEVER).await;
        let count = result.unwrap();
        if count == 0 {
            return bytes;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

pub async fn check<S: FileAllocate>(scheduler: &S, root: &Path) {
    let path = root.join("allocate");
    let file = scheduler
        .file_open(&path, FileOptions::CREATE, &NEVER)
        .await
        .unwrap();
    assert_eq!(AllocationMode::default(), AllocationMode::Extend);
    scheduler
        .file_seek(&file, SeekFrom::Start(32), &NEVER)
        .await
        .unwrap();
    scheduler
        .file_allocate(&file, 4, 8, AllocationMode::KeepSize, &NEVER)
        .await
        .unwrap();
    assert_eq!(contents(scheduler, &path).await, b"");
    scheduler
        .file_allocate(&file, 4, 8, AllocationMode::Extend, &NEVER)
        .await
        .unwrap();
    assert_eq!(contents(scheduler, &path).await, [0; 12]);
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Current(0), &NEVER)
            .await
            .unwrap(),
        32
    );

    scheduler
        .file_seek(&file, SeekFrom::Start(3), &NEVER)
        .await
        .unwrap();
    assert_eq!(
        scheduler
            .file_write(&file, b"X".to_vec(), &NEVER)
            .await
            .0
            .unwrap(),
        1
    );
    let expected = b"\0\0\0X\0\0\0\0\0\0\0\0";
    for mode in [AllocationMode::Extend, AllocationMode::KeepSize] {
        // Already allocated ranges and ranges wholly inside the file preserve data and never shrink it.
        scheduler
            .file_allocate(&file, 2, 4, mode, &NEVER)
            .await
            .unwrap();
        assert_eq!(contents(scheduler, &path).await, expected);
        for (offset, length) in [
            (0, 0),
            (i64::MAX as u64, 1),
            (0, i64::MAX as u64 + 1),
            (u64::MAX, 1),
        ] {
            assert_eq!(
                scheduler
                    .file_allocate(&file, offset, length, mode, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        drop(scheduler.file_allocate(&file, 0, 64, mode, &NEVER));
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(matches!(
            scheduler
                .file_allocate(&file, 0, 64, mode, &cancelled)
                .await,
            Err(OperationError::Cancelled(_))
        ));
        let stopping = Cancellation::new();
        let future = scheduler.file_allocate(&file, 0, 64, mode, &stopping);
        stopping.cancel();
        assert!(matches!(future.await, Err(OperationError::Cancelled(_))));
        assert_eq!(contents(scheduler, &path).await, expected);
        assert_eq!(
            scheduler
                .file_seek(&file, SeekFrom::Current(0), &NEVER)
                .await
                .unwrap(),
            4
        );

        let reader = scheduler
            .file_open(&path, FileOptions::READ, &NEVER)
            .await
            .unwrap();
        assert!(matches!(
            scheduler.file_allocate(&reader, 0, 4, mode, &NEVER).await,
            Err(OperationError::Io(_))
        ));
        assert_eq!(contents(scheduler, &path).await, expected);
    }
    scheduler
        .file_allocate(&file, 4, 12, AllocationMode::KeepSize, &NEVER)
        .await
        .unwrap();
    assert_eq!(contents(scheduler, &path).await, expected);
    scheduler
        .file_allocate(&file, 4, 12, AllocationMode::Extend, &NEVER)
        .await
        .unwrap();
    assert_eq!(
        contents(scheduler, &path).await,
        b"\0\0\0X\0\0\0\0\0\0\0\0\0\0\0\0"
    );
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Current(0), &NEVER)
            .await
            .unwrap(),
        4
    );
}
