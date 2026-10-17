// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Cancellation, File, FileOptions, IoBuf, OperationError, SyncMode};
use std::{io::SeekFrom, path::Path};

static NEVER: Cancellation = Cancellation::never();

pub async fn check<S: File>(scheduler: &S, root: &Path) {
    let path = root.join("sync");
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
    let cancelled = Cancellation::new();
    cancelled.cancel();
    for mode in [SyncMode::All, SyncMode::Data] {
        // Empty files can be synchronized, and completed writes can be synchronized repeatedly.
        scheduler.file_sync(&file, mode, &NEVER).await.unwrap();
        scheduler
            .file_seek(&file, SeekFrom::Start(0), &NEVER)
            .await
            .unwrap();
        let mut buffer = b"contents".to_vec();
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
        scheduler.file_sync(&file, mode, &NEVER).await.unwrap();
        scheduler.file_sync(&file, mode, &NEVER).await.unwrap();
        assert_eq!(
            scheduler
                .file_seek(&file, SeekFrom::Current(0), &NEVER)
                .await
                .unwrap(),
            8
        );
        assert!(matches!(
            scheduler.file_sync(&file, mode, &cancelled).await,
            Err(OperationError::Cancelled(_))
        ));
        drop(scheduler.file_sync(&file, mode, &NEVER));
        assert_eq!(
            scheduler
                .file_seek(&file, SeekFrom::Current(0), &NEVER)
                .await
                .unwrap(),
            8
        );
        scheduler
            .file_seek(&file, SeekFrom::Start(0), &NEVER)
            .await
            .unwrap();
        let mut actual = Vec::new();
        loop {
            let (result, buffer) = scheduler.file_read(&file, vec![0; 16], &NEVER).await;
            let count = result.unwrap();
            if count == 0 {
                break;
            }
            actual.extend_from_slice(&buffer[..count]);
        }
        assert_eq!(actual, b"contents");
    }
}
