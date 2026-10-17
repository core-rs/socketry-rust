// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Cancellation, File, FileOptions, IoBuf, OperationError};
use std::{io::SeekFrom, path::Path};

static NEVER: Cancellation = Cancellation::never();

async fn write_all<S: File>(scheduler: &S, file: &S::File, bytes: &[u8]) {
    let mut buffer = bytes.to_vec();
    let allocation = buffer.as_ptr();
    let mut offset = 0;
    while offset < bytes.len() {
        let (result, returned) = scheduler
            .file_write(file, buffer.slice(offset..), &NEVER)
            .await;
        let count = result.unwrap();
        assert!(count > 0 && count <= bytes.len() - offset);
        offset += count;
        buffer = returned.into_inner();
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(buffer, bytes);
    }
}

async fn read_all<S: File>(scheduler: &S, file: &S::File) -> Vec<u8> {
    let mut output = Vec::new();
    let mut buffer = vec![255; 6].into_boxed_slice();
    let allocation = buffer.as_ptr();
    loop {
        buffer.fill(255);
        let (result, returned) = scheduler.file_read(file, buffer.slice(1..5), &NEVER).await;
        let count = result.unwrap();
        assert!(count <= 4);
        buffer = returned.into_inner();
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(buffer[0], 255);
        assert!(buffer[1 + count..].iter().all(|byte| *byte == 255));
        if count == 0 {
            break;
        }
        output.extend_from_slice(&buffer[1..1 + count]);
    }
    output
}

pub async fn check<S: File>(scheduler: &S, root: &Path) {
    check_seek(scheduler, root).await;
    let path = root.join("stateful");
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
    write_all(scheduler, &file, b"abc").await;
    drop(scheduler.file_write(&file, b"unpolled".to_vec(), &NEVER));

    let cancelled = Cancellation::new();
    cancelled.cancel();
    for write in [false, true] {
        let buffer = vec![17; 4];
        let allocation = buffer.as_ptr();
        let (result, buffer) = if write {
            scheduler.file_write(&file, buffer, &cancelled).await
        } else {
            scheduler.file_read(&file, buffer, &cancelled).await
        };
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(buffer, vec![17; 4]);
    }
    // Neither pre-cancelled nor empty operations move the cursor.
    assert_eq!(
        scheduler
            .file_read(&file, Vec::new(), &NEVER)
            .await
            .0
            .unwrap(),
        0
    );
    assert_eq!(
        scheduler
            .file_write(&file, Vec::new(), &NEVER)
            .await
            .0
            .unwrap(),
        0
    );
    write_all(scheduler, &file, b"def").await;
    assert!(read_all(scheduler, &file).await.is_empty());

    let reader = scheduler
        .file_open(&path, FileOptions::READ, &NEVER)
        .await
        .unwrap();
    assert_eq!(read_all(scheduler, &reader).await, b"abcdef");
    let reader = scheduler
        .file_open(&path, FileOptions::READ, &NEVER)
        .await
        .unwrap();
    // A fresh open has its own cursor, and an access error preserves the buffer and cursor.
    let buffer = vec![7];
    let allocation = buffer.as_ptr();
    let (result, returned) = scheduler.file_write(&reader, buffer, &NEVER).await;
    assert!(matches!(result, Err(OperationError::Io(_))));
    assert_eq!(returned.as_ptr(), allocation);
    assert_eq!(returned, [7]);
    assert_eq!(read_all(scheduler, &reader).await, b"abcdef");

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
    let buffer = vec![8];
    let allocation = buffer.as_ptr();
    let (result, returned) = scheduler.file_read(&writer, buffer, &NEVER).await;
    assert!(matches!(result, Err(OperationError::Io(_))));
    assert_eq!(returned.as_ptr(), allocation);
    assert_eq!(returned, [8]);
    write_all(scheduler, &writer, b"AB").await;

    let mixed = scheduler
        .file_open(
            &path,
            FileOptions {
                read: true,
                write: true,
                ..Default::default()
            },
            &NEVER,
        )
        .await
        .unwrap();
    for expected in b"AB" {
        drop(scheduler.file_read(&mixed, vec![0], &NEVER));
        let (result, buffer) = scheduler.file_read(&mixed, vec![0], &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, [*expected]);
    }
    write_all(scheduler, &mixed, b"XY").await;
    assert_eq!(read_all(scheduler, &mixed).await, b"ef");
    let reader = scheduler
        .file_open(&path, FileOptions::READ, &NEVER)
        .await
        .unwrap();
    assert_eq!(read_all(scheduler, &reader).await, b"ABXYef");

    #[cfg(unix)]
    {
        // Explicit offsets preserve the shared cursor on Unix. The Windows fallback documents different behavior.
        let file = scheduler
            .file_open(
                &path,
                FileOptions {
                    read: true,
                    write: true,
                    ..Default::default()
                },
                &NEVER,
            )
            .await
            .unwrap();
        let (result, buffer) = scheduler.file_read_at(&file, vec![0], 5, &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, b"f");
        let (result, buffer) = scheduler.file_read(&file, buffer, &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, b"A");
        let (result, _) = scheduler
            .file_write_at(&file, b"!".to_vec(), 4, &NEVER)
            .await;
        assert_eq!(result.unwrap(), 1);
        let (result, buffer) = scheduler.file_read(&file, buffer, &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, b"B");
    }
}

async fn check_seek<S: File>(scheduler: &S, root: &Path) {
    let path = root.join("seek");
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
    write_all(scheduler, &file, b"abcdef").await;
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Current(0), &NEVER)
            .await
            .unwrap(),
        6
    );
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Start(2), &NEVER)
            .await
            .unwrap(),
        2
    );
    let (result, buffer) = scheduler.file_read(&file, vec![0], &NEVER).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, b"c");
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Current(2), &NEVER)
            .await
            .unwrap(),
        5
    );
    assert_eq!(read_all(scheduler, &file).await, b"f");
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::End(-3), &NEVER)
            .await
            .unwrap(),
        3
    );
    write_all(scheduler, &file, b"X").await;
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Current(-2), &NEVER)
            .await
            .unwrap(),
        2
    );
    assert_eq!(read_all(scheduler, &file).await, b"cXef");

    // Seeking past EOF leaves the length unchanged; a subsequent write fills the gap with zero bytes.
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::End(4), &NEVER)
            .await
            .unwrap(),
        10
    );
    assert!(read_all(scheduler, &file).await.is_empty());
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::End(0), &NEVER)
            .await
            .unwrap(),
        6
    );
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Start(10), &NEVER)
            .await
            .unwrap(),
        10
    );
    write_all(scheduler, &file, b"Z").await;
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Current(0), &NEVER)
            .await
            .unwrap(),
        11
    );
    scheduler
        .file_seek(&file, SeekFrom::Start(2), &NEVER)
        .await
        .unwrap();

    // Invalid absolute positions, negative results, and relative overflow preserve the cursor.
    for position in [
        SeekFrom::Start(i64::MAX as u64 + 1),
        SeekFrom::Start(u64::MAX),
        SeekFrom::Current(i64::MIN),
        SeekFrom::Current(i64::MAX),
        SeekFrom::End(-12),
        SeekFrom::End(i64::MAX),
    ] {
        assert!(matches!(
            scheduler.file_seek(&file, position, &NEVER).await,
            Err(OperationError::Io(_))
        ));
        assert_eq!(
            scheduler
                .file_seek(&file, SeekFrom::Current(0), &NEVER)
                .await
                .unwrap(),
            2
        );
    }

    let cancelled = Cancellation::new();
    cancelled.cancel();
    for position in [SeekFrom::Start(0), SeekFrom::Current(0), SeekFrom::End(0)] {
        drop(scheduler.file_seek(&file, position, &NEVER));
        assert!(matches!(
            scheduler.file_seek(&file, position, &cancelled).await,
            Err(OperationError::Cancelled(_))
        ));
    }
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Current(0), &NEVER)
            .await
            .unwrap(),
        2
    );

    // A new read-only open can seek independently; a write-only open can seek too.
    let reader = scheduler
        .file_open(&path, FileOptions::READ, &NEVER)
        .await
        .unwrap();
    assert_eq!(
        scheduler
            .file_seek(&reader, SeekFrom::Current(0), &NEVER)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        scheduler
            .file_seek(&reader, SeekFrom::End(-1), &NEVER)
            .await
            .unwrap(),
        10
    );
    assert_eq!(read_all(scheduler, &reader).await, b"Z");
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Current(0), &NEVER)
            .await
            .unwrap(),
        2
    );
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
    assert_eq!(
        scheduler
            .file_seek(&writer, SeekFrom::Start(1), &NEVER)
            .await
            .unwrap(),
        1
    );
    write_all(scheduler, &writer, b"B").await;
    assert_eq!(
        scheduler
            .file_seek(&file, SeekFrom::Start(0), &NEVER)
            .await
            .unwrap(),
        0
    );
    assert_eq!(read_all(scheduler, &file).await, b"aBcXef\0\0\0\0Z");
}
